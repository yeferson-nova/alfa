from dataclasses import dataclass
from typing import Any

import pytest

from builders import DETAIL_SQL, case_data, global_config, parameters
from certification_job.application.empty_result import EmptyResultConfirmer
from certification_job.application.generate_certification import Dependencies, GenerateCertification
from certification_job.application.ports import LogEvent
from certification_job.domain.errors import ConfigurationError, DataError, PlatformError
from certification_job.domain.models import ProbeResult, QueryDetails
from fakes import (
    FakeConfigs,
    FakeNotifications,
    FakeObjects,
    FakeQueries,
    FixedClock,
    MemoryLog,
    RecordingSleep,
    SequentialIds,
)

BOM = b"\xef\xbb\xbf"
UNLOAD_FILES = {
    ("bucket-assets", "athena/unload/exec-1/part-0"): b"1|2\n",
    ("bucket-assets", "athena/unload/exec-1/x-manifest.csv"): b"ignored",
    ("bucket-assets", "athena/unload/exec-2/part-1"): b"3|4\n",
    ("bucket-assets", "athena/unload/exec-2/part-0"): b"5|6\n",
}


@dataclass
class Scenario:
    use_case: GenerateCertification
    configs: FakeConfigs
    queries: FakeQueries
    objects: FakeObjects
    notifications: FakeNotifications
    log: MemoryLog
    sleep: RecordingSleep


def scenario(
    case: Any = None, files: Any = None, queries: Any = None, visible_after: int = 1, **changes: Any
) -> Scenario:
    params = parameters(**changes)
    configs = FakeConfigs({(params.config_bucket, params.case_key): case if case is not None else case_data()})
    queries = queries or FakeQueries()
    objects = FakeObjects(UNLOAD_FILES if files is None else files)
    notifications = FakeNotifications(visible_after)
    log, sleep = MemoryLog(), RecordingSleep()
    dependencies = Dependencies(configs, queries, objects, notifications, FixedClock(), SequentialIds(), sleep, log)
    use_case = GenerateCertification(params, global_config(), dependencies)
    return Scenario(use_case, configs, queries, objects, notifications, log, sleep)


def test_generates_one_file_and_one_item_per_detail_query_in_name_order():
    current = scenario()
    result = current.use_case.run()

    assert result.cert_code == "202606-20261111-0930"
    assert [exported.file_name for exported in result.files] == [
        "202606-detail_1-20261111-093015.csv",
        "202606-detail_2-20261111-093015.csv",
    ]
    assert [database for _, database in current.queries.runs] == ["db-default", "db_dev"]
    sql, _ = current.queries.runs[0]
    assert "p = '202606'" in sql
    assert "TO 's3://bucket-assets/athena/unload/exec-1/'" in sql


def test_report_files_get_the_configured_headers_bom_and_content_type():
    current = scenario()
    current.use_case.run()

    first, second = current.objects.writes
    assert first[:2] == ("bucket-reports", "folderA/202606-detail_1-20261111-093015.csv")
    assert first[2] == BOM + "Columna A|b\n1|2\n".encode()
    assert first[3] == "text/csv; charset=utf-8"
    assert second[2] == BOM + "Columna A|b\n5|6\n3|4\n".encode()


def test_items_keep_the_deployed_contract_and_add_the_flow_ids():
    current = scenario(origin_execution_ids=("o-1", "o-2"))
    current.use_case.run()

    table, item = current.notifications.items[0]
    assert table == "table-notifications"
    assert item.pop("s3_url").startswith("https://sso.example/start/#/console?account_id=ACCOUNT&role_name=ROLE")
    assert item == {
        "useCaseId": "caseA",
        "executionId": "exec-1",
        "status": "PROCESSED",
        "period": "202606",
        "emailType": "cert",
        "queryName": "detail#1",
        "userEmail": "SYSTEM_GLUE_JOB",
        "nameFile": "202606-detail_1-20261111-093015.csv",
        "creationDate": "2026-11-11T09:30:15-05:00",
        "configBucket": "bucket-config",
        "configKey": "cases/caseA.json",
        "globalConfigKey": "global.json",
        "regeneracionSinDatosNuevos": False,
        "sinDatosPeriodo": False,
        "certCode": "202606-20261111-0930",
        "ejecucionId": "primasArl-202606-flow",
        "ejecucionesOrigen": ["o-1", "o-2"],
    }


def test_marks_every_item_of_the_cert_code_as_pending():
    current = scenario()
    current.use_case.run()

    table, keys, values = current.notifications.pending[0]
    assert table == "table-notifications"
    assert [key.execution_id for key in keys] == ["exec-1", "exec-2"]
    assert values == {
        "status": "PENDING",
        "pendingDate": "2026-11-11T09:30:15-05:00",
        "configBucket": "bucket-config",
        "configKey": "cases/caseA.json",
        "globalConfigKey": "global.json",
    }
    assert current.notifications.searches[0] == ("table-notifications", "index-cert", "202606-20261111-0930", "caseA")


def test_logs_carry_the_trace_fields_and_never_the_sql():
    current = scenario()
    current.use_case.run()

    assert current.log.events() == [
        LogEvent.JOB_START,
        LogEvent.CONFIG_LOADED,
        LogEvent.CERT_CODE_GENERATED,
        LogEvent.QUERY_EXECUTED,
        LogEvent.FILE_WRITTEN,
        LogEvent.ITEM_REGISTERED,
        LogEvent.QUERY_EXECUTED,
        LogEvent.FILE_WRITTEN,
        LogEvent.ITEM_REGISTERED,
        LogEvent.PENDING_MARKED,
        LogEvent.JOB_END,
    ]
    executed = current.log.fields(LogEvent.QUERY_EXECUTED)
    assert executed["ejecucion_id"] == "primasArl-202606-flow"
    assert executed["request_id"] == "jr_1"
    assert executed["cert_code"] == "202606-20261111-0930"
    assert executed["use_case_id"] == "caseA"
    assert executed["periodo_negocio"] == "202606"
    assert (executed["id_consulta"], executed["bytes_escaneados"]) == ("q-1", 100)
    assert all("UNLOAD" not in str(value) for _, _, fields in current.log.records for value in fields.values())


def test_missing_flow_id_is_logged_and_never_invented():
    current = scenario(flow_execution_id=None)
    current.use_case.run()

    assert (LogEvent.FLOW_ID_MISSING, "WARN") in [(event, level) for event, level, _ in current.log.records]
    assert all("ejecucionId" not in item and "ejecucionesOrigen" not in item for _, item in current.notifications.items)


def test_regeneration_uses_the_unique_cert_code_and_marks_the_items():
    current = scenario(regeneration_without_new_data=True)
    result = current.use_case.run()

    assert result.cert_code == "202606-20261111-093015-suffix"
    assert all(item["regeneracionSinDatosNuevos"] is True for _, item in current.notifications.items)


def test_non_certification_cases_have_no_cert_code_and_are_not_marked_pending():
    current = scenario(case=case_data(emailType="report"))
    result = current.use_case.run()

    assert result.cert_code is None
    assert all("certCode" not in item for _, item in current.notifications.items)
    assert current.notifications.pending == []


def test_zero_data_rows_mark_the_period_without_data():
    files = {
        ("bucket-assets", "athena/unload/exec-1/part-0"): b"\n",
        ("bucket-assets", "athena/unload/exec-2/p"): b"1|2\n",
    }
    current = scenario(files=files)
    result = current.use_case.run()

    assert [(exported.data_rows, exported.no_data) for exported in result.files] == [(0, True), (1, False)]
    assert [item["sinDatosPeriodo"] for _, item in current.notifications.items] == [True, False]


def test_empty_unload_is_confirmed_before_writing_a_header_only_file():
    current = scenario(files={("bucket-assets", "manifest.csv"): b""}, case=case_data(email={}))
    result = current.use_case.run()

    assert all(exported.no_data for exported in result.files)
    assert current.objects.writes[0][2] == BOM + b"a,b\r\n"
    probe_sql, probe_database = current.queries.runs[1]
    assert probe_sql.startswith("SELECT * FROM (\nSELECT a, b FROM t WHERE p = '202606'")
    assert probe_database == "db-default"
    assert LogEvent.NO_DATA_CONFIRMED in current.log.events()


def test_empty_unload_with_configured_headers_keeps_the_deployed_double_header():
    current = scenario(files={("bucket-assets", "manifest.csv"): b""})
    current.use_case.run()

    assert current.objects.writes[0][2] == BOM + b"Columna A|b\na,b\r\n"


def test_reserva_case_with_suffixed_table_names_gets_no_headers():
    case = case_data(useCaseId="reserva", email={"tableVars": {"t": {"queryName": "detail#1", "columns": {"a": {}}}}})
    current = scenario(case=case)
    result = current.use_case.run()

    assert current.objects.writes[0][2] == BOM + b"1|2\n"
    assert result.files[0].data_rows is None
    assert current.objects.writes[0][1].startswith("folderReserva/")


def test_use_case_without_report_folder_fails_before_running_queries():
    current = scenario(case=case_data(useCaseId="otro"))
    with pytest.raises(ConfigurationError) as error:
        current.use_case.run()
    assert error.value.code == "CER-CFG-003"
    assert current.queries.runs == []


def test_query_without_database_fails():
    case = case_data()
    del case["athena"]
    current = scenario(case=case)
    with pytest.raises(ConfigurationError) as error:
        current.use_case.run()
    assert error.value.code == "CER-CFG-006"


def test_query_with_an_unknown_variable_fails_before_reaching_athena():
    case = case_data(queries={"detail": [{"queryName": "d", "query": DETAIL_SQL + " AND x = '$otro'"}]})
    current = scenario(case=case)
    with pytest.raises(ConfigurationError) as error:
        current.use_case.run()
    assert error.value.code == "CER-CFG-004"
    assert current.queries.runs == []


def test_waits_until_the_cert_code_is_visible_in_the_index():
    current = scenario(visible_after=3)
    current.use_case.run()

    assert current.sleep.calls == [2.0, 2.0]
    retries = [fields["intento"] for event, _, fields in current.log.records if event == LogEvent.INDEX_NOT_VISIBLE]
    assert retries == [1, 2]


def test_fails_as_recoverable_when_the_cert_code_never_appears_in_the_index():
    current = scenario(visible_after=99)
    with pytest.raises(PlatformError) as error:
        current.use_case.run()
    assert error.value.code == "CER-PLA-006"
    assert error.value.recoverable is True
    assert current.sleep.calls == [2.0, 2.0]


def _confirmer(details=None, probe=None, manifest=b""):
    queries = FakeQueries(probe=probe, details=details)
    objects = FakeObjects({("bucket-assets", "manifest.csv"): manifest})
    return EmptyResultConfirmer(queries, objects), queries


SQL = "UNLOAD (SELECT 1) TO 's3://bucket-assets/athena/unload/e/'"
EXPECTED = "s3://bucket-assets/athena/unload/e/"


def test_confirmer_returns_the_column_header_when_everything_is_empty():
    confirmer, queries = _confirmer()
    assert confirmer.confirm("db", SQL, "q-0", EXPECTED) == b"a,b\r\n"
    assert queries.runs == [("SELECT * FROM (\nSELECT 1\n) AS niif_sin_datos LIMIT 1", "db")]


@pytest.mark.parametrize(
    "details, probe, manifest, sql, reason",
    [
        (QueryDetails("FAILED", "s3://bucket-assets/manifest.csv"), None, b"", SQL, "fallida"),
        (QueryDetails("SUCCEEDED", None), None, b"", SQL, "Sin manifiesto"),
        (None, None, b"s3://x/part-0\n", SQL, "Manifiesto no vacio"),
        (None, None, b"", SQL.replace("/e/", "/otro/"), "Destino UNLOAD distinto"),
        (None, ProbeResult(2, False, ("a",)), b"", SQL, "encontro datos"),
        (None, ProbeResult(1, True, ("a",)), b"", SQL, "encontro datos"),
        (None, ProbeResult(1, False, ()), b"", SQL, "Sin metadatos"),
    ],
)
def test_confirmer_rejects_any_doubt(details, probe, manifest, sql, reason):
    confirmer, _ = _confirmer(details, probe, manifest)
    with pytest.raises(DataError) as error:
        confirmer.confirm("db", sql, "q-0", EXPECTED)
    assert error.value.code == "CER-DAT-001"
    assert reason in error.value.message
