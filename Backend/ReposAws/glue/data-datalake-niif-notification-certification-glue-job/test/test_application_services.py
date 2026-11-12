from datetime import timedelta

import pytest

from builders import build_composite_record
from fakes import (
    InMemoryEventLogger,
    FakeQueryRunner,
    SequentialTokenGenerator,
    FixedClock,
    FakeCompositeRepository,
    FakeLinkRepository,
)
from certification_notifier.application.composite import CompositeService
from certification_notifier.application.queries import QueryService
from certification_notifier.application.links import LinkService
from certification_notifier.application.ports import LogEvent
from certification_notifier.domain.errors import (
    ConcurrencyError,
    ConfigurationError,
    QueryFailedError,
    PlatformError,
)
from certification_notifier.domain.models import (
    CompositeConfig,
    Query,
    QueryEnvironment,
    LinkPolicy,
    QueryResult,
)

QUERY_ENVIRONMENT = QueryEnvironment(catalog="catalog", output_location="s3://b/salida/")
VARIABLES = {"period": "202610", "env": "dev"}


def _query(name="q", sql="SELECT ${period}", base="db_${env}"):
    return Query(name=name, database=base, sql=sql)


def test_link_service_saves_link_with_expiration_and_returns_short_url():
    repository = FakeLinkRepository()
    clock = FixedClock()
    policy = LinkPolicy("https://redir.example/r", "table", 7, 8)
    logger = InMemoryEventLogger()
    service = LinkService(repository, clock, SequentialTokenGenerator(), logger, policy, "casoA", "C1")

    url = service.create("CERTIFICATE", "https://largo.example/x")

    table, link = repository.saved[0]
    assert url == "https://redir.example/r?t=00000001"
    assert table == "table"
    assert link.token == "00000001"
    assert link.url == "https://largo.example/x"
    assert (link.use_case_id, link.cert_code, link.action) == ("casoA", "C1", "CERTIFICATE")
    assert link.created_at == clock.local.isoformat()
    assert link.expires_at == int((clock.local + timedelta(days=7)).timestamp())
    assert logger.names() == [LogEvent.LINK_CREATED]


def test_query_service_runs_with_variables_and_joins_blocks():
    runner = FakeQueryRunner(
        {
            "SELECT 202610": QueryResult(("a",), (("1",),)),
            "SELECT otro": QueryResult(("b",), (("2",),)),
        }
    )
    service = QueryService(runner, InMemoryEventLogger())

    text = service.tables([_query(), _query("q2", "SELECT otro")], VARIABLES, QUERY_ENVIRONMENT).text

    assert text == "a: 1\n\nb: 2"
    assert runner.calls[0] == ("SELECT 202610", "db_dev", QUERY_ENVIRONMENT)


def test_query_service_without_queries_returns_empty_text():
    assert QueryService(FakeQueryRunner({}), InMemoryEventLogger()).tables([], VARIABLES, QUERY_ENVIRONMENT).text == ""


def test_query_service_turns_failed_query_into_text_and_logs_it():
    logger = InMemoryEventLogger()
    runner = FakeQueryRunner({"SELECT 202610": QueryFailedError("NTF-PLA-002", "fallo")})

    text = QueryService(runner, logger).tables([_query()], VARIABLES, QUERY_ENVIRONMENT).text

    assert text == "Error ejecutando consulta"
    event, level, context = logger.events[0]
    assert (event, level, context["codigo"]) == (LogEvent.QUERY_FAILED, "ERROR", "NTF-PLA-002")


def test_query_service_does_not_hide_non_query_errors():
    runner = FakeQueryRunner({"SELECT 202610": PlatformError("NTF-PLA-005", "tiempo")})
    with pytest.raises(PlatformError):
        QueryService(runner, InMemoryEventLogger()).tables([_query()], VARIABLES, QUERY_ENVIRONMENT)


def test_availability_without_queries_returns_none():
    assert QueryService(FakeQueryRunner({}), InMemoryEventLogger()).availability([], VARIABLES, QUERY_ENVIRONMENT) is None


def test_availability_reports_members_without_data():
    runner = FakeQueryRunner(
        {
            "SELECT 202610": QueryResult(("registros",), (("0",),)),
            "SELECT otro": QueryResult(("registros",), (("3",),)),
        }
    )
    service = QueryService(runner, InMemoryEventLogger())

    notice = service.availability(
        [_query("A"), _query("B", "SELECT otro")], VARIABLES, QUERY_ENVIRONMENT
    )

    assert "A: no se encontraron datos activos para el periodo 202610." in notice
    assert "B:" not in notice


def test_availability_with_all_members_with_data_is_empty():
    runner = FakeQueryRunner({"SELECT 202610": QueryResult(("registros",), (("1",),))})
    assert QueryService(runner, InMemoryEventLogger()).availability([_query()], VARIABLES, QUERY_ENVIRONMENT) == ""


def test_availability_does_not_read_technical_failure_as_zero():
    runner = FakeQueryRunner({"SELECT 202610": QueryFailedError("NTF-PLA-002", "fallo")})
    with pytest.raises(QueryFailedError):
        QueryService(runner, InMemoryEventLogger()).availability([_query()], VARIABLES, QUERY_ENVIRONMENT)


def test_availability_rejects_results_outside_contract():
    runner = FakeQueryRunner({"SELECT 202610": QueryResult(("x",), (("1",),))})
    with pytest.raises(ConfigurationError):
        QueryService(runner, InMemoryEventLogger()).availability([_query()], VARIABLES, QUERY_ENVIRONMENT)


COMPOSITE_CONFIG_DATA = CompositeConfig("table", "grupo", "_CONTROL", ("m1", "m2"))


def _composite_records(control_status="NOTIFICATION_IN_PROGRESS", **control_changes):
    control = build_composite_record(member_id="_CONTROL", status=control_status, **control_changes)
    return {
        ("table", "grupo#202610", "_CONTROL"): control,
        ("table", "grupo#202610", "m1"): build_composite_record(member_id="m1"),
        ("table", "grupo#202610", "m2"): build_composite_record(member_id="m2"),
    }


def _composite_service(records):
    repository = FakeCompositeRepository(records)
    return CompositeService(repository, FixedClock(), InMemoryEventLogger()), repository


def test_composite_not_prepared_without_config_or_on_regeneration():
    service, _ = _composite_service({})
    assert service.prepare(None, "202610", "C1", False) is None
    assert service.prepare(COMPOSITE_CONFIG_DATA, "202610", "C1", True) is None


def test_composite_prepares_reservation_with_control_and_members():
    service, _ = _composite_service(_composite_records())

    reservation = service.prepare(COMPOSITE_CONFIG_DATA, "202610", "C1", False)

    assert reservation.composite_id == "grupo#202610"
    assert reservation.already_completed is False
    assert [member.member_id for member in reservation.members] == ["m1", "m2"]
    assert reservation.control.member_id == "_CONTROL"


def test_composite_completed_for_same_cert_code_loads_no_members():
    records = _composite_records("NOTIFICATION_COMPLETED", cert_code="C1")
    service, _ = _composite_service(records)

    reservation = service.prepare(COMPOSITE_CONFIG_DATA, "202610", "C1", False)

    assert reservation.already_completed is True
    assert reservation.members == ()


def test_composite_with_member_not_ready_fails():
    records = _composite_records()
    records[("table", "grupo#202610", "m2")] = build_composite_record(member_id="m2", status="CONSUMED")
    service, _ = _composite_service(records)
    with pytest.raises(ConcurrencyError):
        service.prepare(COMPOSITE_CONFIG_DATA, "202610", "C1", False)


def test_composite_without_control_fails():
    service, _ = _composite_service({})
    with pytest.raises(ConcurrencyError):
        service.prepare(COMPOSITE_CONFIG_DATA, "202610", "C1", False)


def test_close_does_nothing_without_reservation_or_if_completed():
    records = _composite_records("NOTIFICATION_COMPLETED", cert_code="C1")
    service, repository = _composite_service(records)
    reservation = service.prepare(COMPOSITE_CONFIG_DATA, "202610", "C1", False)

    service.close(None, "id")
    service.close(reservation, "id")

    assert repository.consumptions == []


def test_close_consumes_members_with_identifier_and_utc_date():
    service, repository = _composite_service(_composite_records())
    reservation = service.prepare(COMPOSITE_CONFIG_DATA, "202610", "C1", False)

    service.close(reservation, "id-1,id-2")

    consumed, message_id, occurred_at = repository.consumptions[0]
    assert consumed is reservation
    assert message_id == "id-1,id-2"
    assert occurred_at == FixedClock().utc.isoformat()


def test_close_without_identifier_does_not_consume_and_fails():
    service, repository = _composite_service(_composite_records())
    reservation = service.prepare(COMPOSITE_CONFIG_DATA, "202610", "C1", False)

    with pytest.raises(PlatformError) as error:
        service.close(reservation, "")

    assert error.value.code == "NTF-PLA-003"
    assert repository.consumptions == []
