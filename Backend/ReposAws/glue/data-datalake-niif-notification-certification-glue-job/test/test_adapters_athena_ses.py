import boto3
import pytest
from botocore.stub import Stubber

from certification_notifier.adapters.outbound.athena import AthenaQueryRunner
from certification_notifier.adapters.outbound.ses import SesNotifier
from certification_notifier.domain.errors import (
    QueryFailedError,
    AccessDeniedError,
    PlatformError,
)
from certification_notifier.domain.models import QueryEnvironment, EmailMessage, QueryResult

QUERY_ENVIRONMENT = QueryEnvironment(catalog="catalog", output_location="s3://bucket/salida/")


class FakeSleep:
    def __init__(self) -> None:
        self.seconds: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.seconds.append(seconds)


def _athena(interval=1.0, maximum=10.0):
    client = boto3.client("athena", region_name="us-east-1")
    fake_sleep = FakeSleep()
    return AthenaQueryRunner(client, fake_sleep, interval, maximum), Stubber(client), fake_sleep


def _expect_start(stub):
    stub.add_response(
        "start_query_execution",
        {"QueryExecutionId": "q1"},
        {
            "QueryString": "SELECT 1",
            "QueryExecutionContext": {"Catalog": "catalog", "Database": "db"},
            "ResultConfiguration": {"OutputLocation": "s3://bucket/salida/"},
        },
    )


def _status(stub, name, reason=None):
    status = {"State": name}
    if reason:
        status["StateChangeReason"] = reason
    stub.add_response("get_query_execution", {"QueryExecution": {"Status": status}}, {"QueryExecutionId": "q1"})


def _rows(*rows):
    return [{"Data": [{"VarCharValue": value} for value in row]} for row in rows]


def test_athena_waits_until_query_ends_and_reads_result():
    runner, stub, fake_sleep = _athena()
    _expect_start(stub)
    _status(stub, "QUEUED")
    _status(stub, "RUNNING")
    _status(stub, "SUCCEEDED")
    stub.add_response(
        "get_query_results",
        {"ResultSet": {"Rows": _rows(("a", "b"), ("1", "2"), ("3", "4"))}},
        {"QueryExecutionId": "q1"},
    )
    with stub:
        result = runner.run("SELECT 1", "db", QUERY_ENVIRONMENT)

    assert result == QueryResult(("a", "b"), (("1", "2"), ("3", "4")))
    assert fake_sleep.seconds == [1.0, 1.0]


def test_athena_reads_all_pages_and_takes_headers_from_first():
    runner, stub, _ = _athena()
    _expect_start(stub)
    _status(stub, "SUCCEEDED")
    stub.add_response(
        "get_query_results",
        {"ResultSet": {"Rows": _rows(("a",), ("1",))}, "NextToken": "pagina2"},
        {"QueryExecutionId": "q1"},
    )
    stub.add_response(
        "get_query_results",
        {"ResultSet": {"Rows": _rows(("2",), ("3",))}},
        {"QueryExecutionId": "q1", "NextToken": "pagina2"},
    )
    with stub:
        result = runner.run("SELECT 1", "db", QUERY_ENVIRONMENT)

    assert result == QueryResult(("a",), (("1",), ("2",), ("3",)))


def test_athena_headers_only_returns_result_without_rows():
    runner, stub, _ = _athena()
    _expect_start(stub)
    _status(stub, "SUCCEEDED")
    stub.add_response("get_query_results", {"ResultSet": {"Rows": _rows(("a",))}}, {"QueryExecutionId": "q1"})
    with stub:
        assert runner.run("SELECT 1", "db", QUERY_ENVIRONMENT) == QueryResult(("a",), ())


def test_athena_without_rows_returns_empty_headers():
    runner, stub, _ = _athena()
    _expect_start(stub)
    _status(stub, "SUCCEEDED")
    stub.add_response("get_query_results", {"ResultSet": {"Rows": []}}, {"QueryExecutionId": "q1"})
    with stub:
        assert runner.run("SELECT 1", "db", QUERY_ENVIRONMENT) == QueryResult((), ())


def test_athena_fills_empty_cells_with_empty_text():
    runner, stub, _ = _athena()
    _expect_start(stub)
    _status(stub, "SUCCEEDED")
    rows = [{"Data": [{"VarCharValue": "a"}]}, {"Data": [{}]}]
    stub.add_response("get_query_results", {"ResultSet": {"Rows": rows}}, {"QueryExecutionId": "q1"})
    with stub:
        assert runner.run("SELECT 1", "db", QUERY_ENVIRONMENT).rows == (("",),)


@pytest.mark.parametrize("status", ["FAILED", "CANCELLED"])
def test_athena_failed_or_cancelled_query_reports_reason(status):
    runner, stub, _ = _athena()
    _expect_start(stub)
    _status(stub, status, "tabla inexistente")
    with stub, pytest.raises(QueryFailedError) as error:
        runner.run("SELECT 1", "db", QUERY_ENVIRONMENT)
    assert error.value.code == "NTF-PLA-002"
    assert "tabla inexistente" in error.value.message
    assert status in error.value.message


def test_athena_exhausts_the_max_wait_time():
    runner, stub, fake_sleep = _athena(interval=1.0, maximum=2.0)
    _expect_start(stub)
    _status(stub, "RUNNING")
    _status(stub, "RUNNING")
    with stub, pytest.raises(PlatformError) as error:
        runner.run("SELECT 1", "db", QUERY_ENVIRONMENT)
    assert error.value.code == "NTF-PLA-005"
    assert not isinstance(error.value, QueryFailedError)
    assert len(fake_sleep.seconds) == 2


def test_athena_translates_access_errors():
    runner, stub, _ = _athena()
    stub.add_client_error("start_query_execution", service_error_code="AccessDeniedException")
    with stub, pytest.raises(AccessDeniedError):
        runner.run("SELECT 1", "db", QUERY_ENVIRONMENT)


def _ses(configuration_set=None):
    client = boto3.client("sesv2", region_name="us-east-1")
    return SesNotifier(client, "remitente@example.com", configuration_set), Stubber(client)


def _request(**extra):
    base = {
        "FromEmailAddress": "remitente@example.com",
        "Destination": {"ToAddresses": ["destino@example.com"]},
        "Content": {
            "Simple": {
                "Subject": {"Data": "Asunto ñ", "Charset": "UTF-8"},
                "Body": {"Text": {"Data": "Texto", "Charset": "UTF-8"}},
            }
        },
    }
    base.update(extra)
    return base


def test_ses_sends_text_email_to_one_recipient():
    notifier, stub = _ses()
    stub.add_response("send_email", {"MessageId": "mid-1"}, _request())
    with stub:
        assert notifier.send(EmailMessage("Asunto ñ", "Texto"), "destino@example.com") == "mid-1"


def test_ses_includes_tags_configuration_set_and_html_body():
    notifier, stub = _ses("conjunto")
    request = _request(
        EmailTags=[{"Name": "useCaseId", "Value": "casoA"}],
        ConfigurationSetName="conjunto",
    )
    request["Content"]["Simple"]["Body"]["Html"] = {"Data": "<p>Texto</p>", "Charset": "UTF-8"}
    stub.add_response("send_email", {"MessageId": "mid-2"}, request)
    message = EmailMessage("Asunto ñ", "Texto", html="<p>Texto</p>", tags={"useCaseId": "casoA"})
    with stub:
        assert notifier.send(message, "destino@example.com") == "mid-2"


def test_ses_without_message_id_returns_empty_text():
    notifier, stub = _ses()
    stub.add_response("send_email", {}, _request())
    with stub:
        assert notifier.send(EmailMessage("Asunto ñ", "Texto"), "destino@example.com") == ""


@pytest.mark.parametrize(
    ("code", "error_class", "error_code"),
    [
        ("MessageRejected", PlatformError, "NTF-PLA-004"),
        ("TooManyRequestsException", PlatformError, "NTF-PLA-001"),
        ("AccessDeniedException", AccessDeniedError, "NTF-PER-001"),
    ],
)
def test_ses_translates_service_rejections(code, error_class, error_code):
    notifier, stub = _ses()
    stub.add_client_error("send_email", service_error_code=code)
    with stub, pytest.raises(error_class) as error:
        notifier.send(EmailMessage("Asunto ñ", "Texto"), "destino@example.com")
    assert error.value.code == error_code
