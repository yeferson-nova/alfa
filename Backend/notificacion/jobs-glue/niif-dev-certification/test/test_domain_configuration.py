import pytest

from builders import case_data, global_data
from certification_job.domain.configuration import read_case_config, read_global_config
from certification_job.domain.errors import ConfigurationError


def test_global_config_reads_every_key():
    config = read_global_config(global_data())
    assert (config.environment, config.catalog, config.timezone) == ("dev", "AwsDataCatalog", "America/Bogota")
    assert (config.assets_bucket, config.reports_bucket) == ("bucket-assets", "bucket-reports")
    assert (config.notification_table, config.cert_code_index) == ("table-notifications", "index-cert")
    assert (config.athena_output_path, config.athena_unload_path) == ("athena/out", "athena/unload")
    assert config.console.base_url == "https://sso.example/start/#/console"
    console = config.console
    assert (console.account_id, console.role_name, console.region) == ("ACCOUNT", "ROLE", "region-1")
    assert config.export.write_bom is True
    assert config.export.field_delimiter == "|"
    settings = config.certification
    assert (settings.processed_status, settings.pending_status) == ("PROCESSED", "PENDING")
    assert settings.default_user_email == "SYSTEM_GLUE_JOB"
    assert settings.report_folders == {"caseA": "folderA", "reserva": "folderReserva"}
    assert (settings.athena_wait.poll_seconds, settings.athena_wait.max_wait_seconds) == (2.0, 10.0)
    assert (settings.index_visibility.attempts, settings.index_visibility.wait_seconds) == (3, 2.0)


@pytest.mark.parametrize(
    "path",
    [
        ("environment",),
        ("catalog",),
        ("timezone",),
        ("buckets", "assets"),
        ("buckets", "reports"),
        ("dynamodb", "notification_table"),
        ("dynamodb", "cert_code_index"),
        ("athena", "output_path"),
        ("athena", "unload_path"),
        ("console_url", "base_url"),
        ("console_url", "account_id"),
        ("console_url", "role_name"),
        ("console_url", "region"),
        ("s3_export", "csv_content_type"),
        ("s3_export", "csv_encoding_bom"),
        ("s3_export", "field_delimiter"),
        ("certification", "default_user_email"),
        ("certification", "processed_status"),
        ("certification", "pending_status"),
        ("certification", "report_folders"),
        ("certification", "athena_poll_seconds"),
        ("certification", "athena_max_wait_seconds"),
        ("certification", "index_visibility_attempts"),
        ("certification", "index_visibility_wait_seconds"),
    ],
)
def test_global_config_has_no_default_values(path):
    data = global_data()
    parent = data
    for key in path[:-1]:
        parent = parent[key]
    del parent[path[-1]]
    with pytest.raises(ConfigurationError) as error:
        read_global_config(data)
    assert error.value.code == "CER-CFG-001"
    assert ".".join(path) in error.value.message


@pytest.mark.parametrize(
    "section, key, value",
    [
        ("certification", "athena_poll_seconds", 0),
        ("certification", "athena_poll_seconds", "2"),
        ("certification", "athena_max_wait_seconds", 1),
        ("certification", "index_visibility_attempts", 1.5),
        ("certification", "index_visibility_attempts", True),
        ("certification", "index_visibility_wait_seconds", -1),
        ("certification", "report_folders", {}),
        ("certification", "report_folders", ["a"]),
        ("certification", "report_folders", {"caseA": " "}),
        ("certification", "processed_status", " "),
        ("s3_export", "csv_encoding_bom", "true"),
        ("s3_export", "field_delimiter", "||"),
    ],
)
def test_global_config_rejects_invalid_values(section, key, value):
    data = global_data()
    data[section][key] = value
    with pytest.raises(ConfigurationError) as error:
        read_global_config(data)
    assert error.value.code == "CER-CFG-001"


def test_global_config_rejects_a_document_that_is_not_an_object():
    with pytest.raises(ConfigurationError):
        read_global_config([])


def test_case_config_sorts_detail_queries_and_reads_headers():
    case = read_case_config(case_data())
    assert case.use_case_id == "caseA"
    assert case.email_type == "cert"
    assert case.default_database == "db-default"
    assert [query.name for query in case.detail_queries] == ["detail#1", "detail#2"]
    assert case.detail_queries[0].database is None
    assert case.detail_queries[1].database == "db_${env}"
    detail, total = case.header_specs
    assert (detail.query_name, detail.headers, detail.export_disabled) == ("detail", ("Columna A", "b"), False)
    assert (total.query_name, total.headers) == ("total", ("x",))


def test_case_config_without_table_vars_or_default_database_is_valid():
    data = case_data(email={"subject": "s"})
    del data["athena"]
    case = read_case_config(data)
    assert case.header_specs == ()
    assert case.default_database is None


def test_case_config_skips_table_vars_without_query_name_and_marks_export_false():
    data = case_data(
        email={
            "tableVars": {
                "loose": "texto",
                "noName": {"columns": {"a": {}}},
                "hidden": {"queryName": "detail", "export": False, "columns": {"a": {}}},
                "empty": {"queryName": "total"},
            }
        }
    )
    hidden, empty = read_case_config(data).header_specs
    assert hidden.export_disabled is True
    assert empty.headers == ()


@pytest.mark.parametrize(
    "changes",
    [
        {"useCaseId": " "},
        {"emailType": None},
        {"email": {"tableVars": []}},
        {"email": {"tableVars": {"t": {"queryName": "detail", "columns": ["a"]}}}},
    ],
)
def test_case_config_rejects_invalid_values(changes):
    with pytest.raises(ConfigurationError) as error:
        read_case_config(case_data(**changes))
    assert error.value.code == "CER-CFG-001"


@pytest.mark.parametrize("queries", [{}, {"detail": []}, {"detail": "x"}, "x"])
def test_case_config_requires_detail_queries(queries):
    with pytest.raises(ConfigurationError) as error:
        read_case_config(case_data(queries=queries))
    assert error.value.code == "CER-CFG-002"


@pytest.mark.parametrize("query", ["texto", {"query": "SELECT 1"}, {"queryName": "d"}])
def test_case_config_rejects_invalid_detail_queries(query):
    with pytest.raises(ConfigurationError):
        read_case_config(case_data(queries={"detail": [query]}))


def test_case_config_must_be_an_object():
    with pytest.raises(ConfigurationError):
        read_case_config(["x"])
