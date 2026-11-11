import io
import json
from datetime import timezone

import boto3
import pytest
from botocore.exceptions import BotoCoreError, ClientError
from botocore.response import StreamingBody
from botocore.stub import ANY, Stubber
from niif_observability import StructuredLogger

from certification_job.adapters.outbound.athena import AthenaQueryRunner
from certification_job.adapters.outbound.aws_errors import translate, translating
from certification_job.adapters.outbound.dynamodb_notifications import DynamoNotificationRepository, serialize
from certification_job.adapters.outbound.s3_store import S3ObjectStore, normalize_key
from certification_job.adapters.outbound.system import JsonEventLog, SystemClock, UuidGenerator
from certification_job.application.ports import LogEvent
from certification_job.domain.errors import AccessDeniedError, ConfigurationError, PlatformError
from certification_job.domain.models import NotificationKey, WaitPolicy
from fakes import RecordingSleep

REGION = "us-east-1"


def _client(service):
    return boto3.client(service, region_name=REGION)


def _error(code):
    return ClientError({"Error": {"Code": code, "Message": "m"}}, "Operation")


@pytest.mark.parametrize(
    "code, error_type, expected_code, recoverable",
    [
        ("AccessDenied", AccessDeniedError, "CER-PER-001", False),
        ("NoSuchKey", ConfigurationError, "CER-CFG-007", False),
        ("ThrottlingException", PlatformError, "CER-PLA-001", True),
        ("InternalError", PlatformError, "CER-PLA-004", False),
    ],
)
def test_translate_classifies_aws_errors(code, error_type, expected_code, recoverable):
    error = translate(_error(code), "S3 get_object")
    assert isinstance(error, error_type)
    assert (error.code, error.recoverable) == (expected_code, recoverable)
    assert code in error.message


def test_translating_wraps_botocore_errors_as_recoverable():
    with pytest.raises(PlatformError) as error:
        with translating("S3"):
            raise BotoCoreError()
    assert (error.value.code, error.value.recoverable) == ("CER-PLA-001", True)


def _runner(client, poll=1.0, maximum=2.0, sleep=None):
    return AthenaQueryRunner(client, sleep or RecordingSleep(), "Catalog", "s3://b/out/", WaitPolicy(poll, maximum))


def _execution(state, scanned=None, manifest=None, reason=None):
    status = {"State": state}
    if reason:
        status["StateChangeReason"] = reason
    statistics = {}
    if scanned is not None:
        statistics["DataScannedInBytes"] = scanned
    if manifest:
        statistics["DataManifestLocation"] = manifest
    return {"QueryExecution": {"QueryExecutionId": "q-1", "Status": status, "Statistics": statistics}}


def test_athena_runs_waits_and_returns_bytes_scanned():
    client, sleep = _client("athena"), RecordingSleep()
    with Stubber(client) as stub:
        stub.add_response(
            "start_query_execution",
            {"QueryExecutionId": "q-1"},
            {
                "QueryString": "SQL",
                "QueryExecutionContext": {"Catalog": "Catalog", "Database": "db"},
                "ResultConfiguration": {"OutputLocation": "s3://b/out/"},
            },
        )
        stub.add_response("get_query_execution", _execution("RUNNING"), {"QueryExecutionId": "q-1"})
        stub.add_response("get_query_execution", _execution("SUCCEEDED", scanned=42), {"QueryExecutionId": "q-1"})
        execution = _runner(client, sleep=sleep).run("SQL", "db")
    assert (execution.query_execution_id, execution.bytes_scanned) == ("q-1", 42)
    assert sleep.calls == [1.0]


def test_athena_failed_query_raises_platform_error():
    client = _client("athena")
    with Stubber(client) as stub:
        stub.add_response("start_query_execution", {"QueryExecutionId": "q-1"}, None)
        stub.add_response("get_query_execution", _execution("FAILED", reason="syntax"), None)
        with pytest.raises(PlatformError) as error:
            _runner(client).run("SQL", "db")
    assert error.value.code == "CER-PLA-002"
    assert "syntax" in error.value.message


@pytest.mark.parametrize("stop_fails, outcome", [(False, "cancelada"), (True, "no se pudo cancelar")])
def test_athena_timeout_cancels_the_query_and_is_recoverable(stop_fails, outcome):
    client = _client("athena")
    with Stubber(client) as stub:
        stub.add_response("start_query_execution", {"QueryExecutionId": "q-1"}, None)
        stub.add_response("get_query_execution", _execution("RUNNING"), None)
        stub.add_response("get_query_execution", _execution("QUEUED"), None)
        if stop_fails:
            stub.add_client_error("stop_query_execution", "AccessDeniedException")
        else:
            stub.add_response("stop_query_execution", {}, {"QueryExecutionId": "q-1"})
        with pytest.raises(PlatformError) as error:
            _runner(client).run("SQL", "db")
    assert (error.value.code, error.value.recoverable) == ("CER-PLA-005", True)
    assert outcome in error.value.message


def test_athena_describe_and_probe():
    client = _client("athena")
    probe_response = {
        "ResultSet": {
            "Rows": [{"Data": [{"VarCharValue": "a"}]}],
            "ResultSetMetadata": {"ColumnInfo": [{"Name": "a", "Type": "varchar"}]},
        },
        "NextToken": "t",
    }
    with Stubber(client) as stub:
        stub.add_response("get_query_execution", _execution("SUCCEEDED", manifest="s3://b/m.csv"), None)
        stub.add_response("get_query_results", probe_response, {"QueryExecutionId": "q-1", "MaxResults": 2})
        details = _runner(client).describe("q-1")
        probe = _runner(client).probe("q-1")
    assert (details.state, details.manifest_location) == ("SUCCEEDED", "s3://b/m.csv")
    assert (probe.row_count, probe.has_more, probe.column_names) == (1, True, ("a",))


def _body(content: bytes) -> StreamingBody:
    return StreamingBody(io.BytesIO(content), len(content))


def test_normalize_key_accepts_own_bucket_uris_and_rejects_others():
    assert normalize_key("b", " s3://b/configs/x.json ") == "configs/x.json"
    assert normalize_key("b", "/configs/x.json") == "configs/x.json"
    with pytest.raises(ConfigurationError) as error:
        normalize_key("b", "s3://otro/x.json")
    assert error.value.code == "CER-CFG-005"


def test_s3_load_json_read_write_and_list():
    client = _client("s3")
    with Stubber(client) as stub:
        document = _body(json.dumps({"a": 1}).encode())
        stub.add_response("get_object", {"Body": document}, {"Bucket": "b", "Key": "k.json"})
        stub.add_response("get_object", {"Body": _body(b"{no json")}, None)
        first_page = {"Contents": [{"Key": "p/1"}], "IsTruncated": True, "NextContinuationToken": "n"}
        stub.add_response("list_objects_v2", first_page, None)
        stub.add_response("list_objects_v2", {"IsTruncated": False}, None)
        stub.add_response(
            "put_object", {}, {"Bucket": "b", "Key": "k.csv", "Body": b"x", "ContentType": "text/csv"}
        )
        store = S3ObjectStore(client)
        assert store.load_json("b", "s3://b/k.json") == {"a": 1}
        with pytest.raises(ConfigurationError) as error:
            store.load_json("b", "k.json")
        assert store.list_keys("b", "p/") == ["p/1"]
        store.write("b", "k.csv", b"x", "text/csv")
    assert error.value.code == "CER-CFG-001"


def test_s3_errors_are_translated():
    client = _client("s3")
    with Stubber(client) as stub:
        stub.add_client_error("get_object", "AccessDenied")
        with pytest.raises(AccessDeniedError):
            S3ObjectStore(client).read("b", "k")


def test_dynamodb_put_serializes_booleans_and_lists():
    client = _client("dynamodb")
    item = {"useCaseId": "c", "executionId": "e", "sinDatosPeriodo": False, "ejecucionesOrigen": ["o1"]}
    with Stubber(client) as stub:
        stub.add_response("put_item", {}, {"TableName": "tabla", "Item": serialize(item)})
        DynamoNotificationRepository(client).put("tabla", item)
    assert serialize(item)["sinDatosPeriodo"] == {"BOOL": False}
    assert serialize(item)["ejecucionesOrigen"] == {"L": [{"S": "o1"}]}


def test_dynamodb_find_by_cert_code_paginates_and_filters_by_use_case():
    client = _client("dynamodb")
    first = {
        "Items": [
            serialize({"useCaseId": "c", "executionId": "e1"}),
            serialize({"useCaseId": "otro", "executionId": "e2"}),
        ],
        "LastEvaluatedKey": serialize({"useCaseId": "c", "executionId": "e1"}),
    }
    second = {"Items": [serialize({"useCaseId": "c", "executionId": "e3"}), serialize({"useCaseId": "c"})]}
    expected = {
        "TableName": "tabla",
        "IndexName": "indice",
        "KeyConditionExpression": "certCode = :cert",
        "ExpressionAttributeValues": {":cert": {"S": "CERT"}},
    }
    with Stubber(client) as stub:
        stub.add_response("query", first, expected)
        stub.add_response("query", second, {**expected, "ExclusiveStartKey": ANY})
        keys = DynamoNotificationRepository(client).find_by_cert_code("tabla", "indice", "CERT", "c")
    assert keys == (NotificationKey("c", "e1"), NotificationKey("c", "e3"))


def test_dynamodb_mark_pending_updates_each_key():
    client = _client("dynamodb")
    values = {"status": "PENDING", "pendingDate": "d", "configBucket": "b", "configKey": "k", "globalConfigKey": "g"}
    with Stubber(client) as stub:
        stub.add_response(
            "update_item",
            {},
            {
                "TableName": "tabla",
                "Key": {"useCaseId": {"S": "c"}, "executionId": {"S": "e1"}},
                "UpdateExpression": ANY,
                "ExpressionAttributeNames": {"#st": "status"},
                "ExpressionAttributeValues": {f":{name}": {"S": value} for name, value in values.items()},
            },
        )
        DynamoNotificationRepository(client).mark_pending("tabla", [NotificationKey("c", "e1")], values)


def test_system_clock_ids_and_json_log():
    clock = SystemClock("America/Bogota")
    assert clock.now_local().utcoffset().total_seconds() == -5 * 3600
    assert clock.now_utc().tzinfo == timezone.utc
    with pytest.raises(ConfigurationError) as error:
        SystemClock("Mundo/Imaginario")
    assert "timezone" in error.value.message
    ids = UuidGenerator()
    assert len(ids.execution_id()) == 36 and ids.execution_id() != ids.execution_id()
    assert len(ids.unique_suffix()) == 32
    lines: list[str] = []
    log = JsonEventLog(StructuredLogger("componente", lines.append))
    log.bind(ejecucion_id="flujo-1")
    log.record(LogEvent.JOB_END, archivos_generados=2)
    line = json.loads(lines[0])
    assert (line["evento"], line["ejecucion_id"], line["archivos_generados"]) == ("CER_FIN_JOB", "flujo-1", 2)
    assert line["mensaje"] == "Fin del job de certificación"
