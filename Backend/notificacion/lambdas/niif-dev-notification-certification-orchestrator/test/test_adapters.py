import io
import json
from datetime import datetime

import boto3
import pytest
from botocore.exceptions import EndpointConnectionError
from botocore.stub import ANY, Stubber
from builders import image

from certification_orchestrator.adapters.outbound.aws_errors import translating
from certification_orchestrator.adapters.outbound.dynamodb_notifications import DynamoNotificationRepository
from certification_orchestrator.adapters.outbound.glue_jobs import GlueJobLauncher
from certification_orchestrator.adapters.outbound.s3_store import S3ObjectStore, normalize_key
from certification_orchestrator.adapters.outbound.ses_alerts import SesAlertSender
from certification_orchestrator.adapters.outbound.system import JsonEventLog, SystemClock
from certification_orchestrator.application.ports import LogEvent
from certification_orchestrator.domain.errors import (
    AccessDeniedError,
    ConcurrencyError,
    ConfigurationError,
    PlatformError,
)
from certification_orchestrator.domain.models import Alert, AlertSettings, LockAcquisition, LockRequest, RecordKey
from niif_observability import StructuredLogger

KEY = RecordKey("primasArl", "exec-1")


def _client(service):
    client = boto3.client(service, region_name="us-east-1")
    return client, Stubber(client)


def test_s3_reads_json_and_normalizes_full_uris():
    client, stub = _client("s3")
    stub.add_response(
        "get_object", {"Body": io.BytesIO(json.dumps({"a": 1}).encode())}, {"Bucket": "cfg", "Key": "c/g.json"}
    )
    with stub:
        assert S3ObjectStore(client).load_json("cfg", "s3://cfg/c/g.json") == {"a": 1}


def test_s3_invalid_json_is_a_configuration_error():
    client, stub = _client("s3")
    stub.add_response("get_object", {"Body": io.BytesIO(b"{")}, {"Bucket": "cfg", "Key": "g.json"})
    with stub, pytest.raises(ConfigurationError):
        S3ObjectStore(client).load_json("cfg", "g.json")


def test_s3_key_from_another_bucket_is_rejected():
    with pytest.raises(ConfigurationError):
        normalize_key("cfg", "s3://otro/g.json")


@pytest.mark.parametrize("code, expected", [("404", False), ("NoSuchKey", False)])
def test_s3_exists_treats_not_found_as_missing(code, expected):
    client, stub = _client("s3")
    stub.add_client_error("head_object", service_error_code=code, http_status_code=404)
    with stub:
        assert S3ObjectStore(client).exists("b", "k") is expected


def test_s3_exists_true_when_head_succeeds():
    client, stub = _client("s3")
    stub.add_response("head_object", {}, {"Bucket": "b", "Key": "k"})
    with stub:
        assert S3ObjectStore(client).exists("b", "k") is True


def test_s3_access_denied_is_not_read_as_missing():
    client, stub = _client("s3")
    stub.add_client_error("head_object", service_error_code="403", http_status_code=403)
    with stub, pytest.raises(AccessDeniedError):
        S3ObjectStore(client).exists("b", "k")


def test_s3_exists_network_error_is_recoverable():
    class Broken:
        def head_object(self, **_):
            raise EndpointConnectionError(endpoint_url="x")

    with pytest.raises(PlatformError) as error:
        S3ObjectStore(Broken()).exists("b", "k")
    assert error.value.recoverable is True


def test_s3_copy_and_delete():
    client, stub = _client("s3")
    stub.add_response(
        "copy_object", {}, {"Bucket": "t", "Key": "tk", "CopySource": {"Bucket": "s", "Key": "sk"}}
    )
    stub.add_response("delete_object", {}, {"Bucket": "s", "Key": "sk"})
    with stub:
        store = S3ObjectStore(client)
        store.copy("s", "sk", "t", "tk")
        store.delete("s", "sk")
    stub.assert_no_pending_responses()


def _dynamo():
    return _client("dynamodb")


def test_dynamo_finds_keys_by_cert_code_across_pages():
    client, stub = _dynamo()
    page = {"Items": [image(executionId="e1")], "LastEvaluatedKey": {"useCaseId": {"S": "x"}}}
    stub.add_response("query", page, {
        "TableName": "tabla", "IndexName": "indice", "KeyConditionExpression": "certCode = :cert",
        "ExpressionAttributeValues": {":cert": {"S": "CERT-1"}},
    })
    stub.add_response("query", {"Items": [image(executionId="e2")]}, {
        "TableName": "tabla", "IndexName": "indice", "KeyConditionExpression": ANY,
        "ExpressionAttributeValues": ANY, "ExclusiveStartKey": {"useCaseId": {"S": "x"}},
    })
    with stub:
        keys = DynamoNotificationRepository(client).find_keys_by_cert_code("tabla", "indice", "CERT-1")
    assert [key.execution_id for key in keys] == ["e1", "e2"]


def test_dynamo_finds_pending_before_a_date():
    client, stub = _dynamo()
    stub.add_response("query", {"Items": [image(sendAttempts=2)]}, {
        "TableName": "tabla", "IndexName": "pendientes",
        "KeyConditionExpression": "#st = :status AND pendingDate < :before",
        "ExpressionAttributeNames": {"#st": "status"},
        "ExpressionAttributeValues": {":status": {"S": "PENDING"}, ":before": {"S": "2026"}},
    })
    with stub:
        records = DynamoNotificationRepository(client).find_pending_before("tabla", "pendientes", "PENDING", "2026")
    assert records[0].lock.attempts == 2


def _acquisition(current=None, status=None):
    return LockAcquisition((LockRequest(KEY, current),), "tok", "fecha", status)


def test_dynamo_lock_without_previous_token_requires_absence():
    client, stub = _dynamo()
    expected = {"TransactItems": [{"Update": {
        "TableName": "tabla",
        "Key": {"useCaseId": {"S": "primasArl"}, "executionId": {"S": "exec-1"}},
        "UpdateExpression": "SET sendLock = :token, sendLockDate = :date ADD sendAttempts :one",
        "ConditionExpression": "attribute_not_exists(sendLock)",
        "ExpressionAttributeValues": {":token": {"S": "tok"}, ":date": {"S": "fecha"}, ":one": {"N": "1"}},
    }}]}
    stub.add_response("transact_write_items", {}, expected)
    with stub:
        assert DynamoNotificationRepository(client).acquire_lock("tabla", _acquisition()) is True


def test_dynamo_lock_takeover_checks_token_and_status():
    client, stub = _dynamo()
    stub.add_response("transact_write_items", {}, {"TransactItems": [{"Update": {
        "TableName": "tabla", "Key": ANY, "UpdateExpression": ANY,
        "ConditionExpression": "sendLock = :current AND #st = :status",
        "ExpressionAttributeNames": {"#st": "status"},
        "ExpressionAttributeValues": ANY,
    }}]})
    with stub:
        assert DynamoNotificationRepository(client).acquire_lock("tabla", _acquisition("viejo", "PENDING")) is True


def test_dynamo_lock_condition_failure_means_taken():
    client, stub = _dynamo()
    stub.add_client_error(
        "transact_write_items",
        service_error_code="TransactionCanceledException",
        response_meta={},
        modeled_fields={"CancellationReasons": [{"Code": "None"}, {"Code": "ConditionalCheckFailed"}]},
    )
    with stub:
        assert DynamoNotificationRepository(client).acquire_lock("tabla", _acquisition()) is False


@pytest.mark.parametrize(
    "code, error_type",
    [("TransactionConflictException", ConcurrencyError), ("TransactionCanceledException", PlatformError)],
)
def test_dynamo_lock_other_failures_raise(code, error_type):
    client, stub = _dynamo()
    stub.add_client_error("transact_write_items", service_error_code=code)
    with stub, pytest.raises(error_type):
        DynamoNotificationRepository(client).acquire_lock("tabla", _acquisition())


def test_dynamo_release_and_status_change():
    client, stub = _dynamo()
    stub.add_response("update_item", {}, {
        "TableName": "tabla", "Key": ANY, "UpdateExpression": "REMOVE sendLock, sendLockDate",
        "ConditionExpression": "sendLock = :token", "ExpressionAttributeValues": {":token": {"S": "tok"}},
    })
    stub.add_response("update_item", {}, {
        "TableName": "tabla", "Key": ANY, "UpdateExpression": "SET #st = :new",
        "ConditionExpression": "#st = :expected", "ExpressionAttributeNames": {"#st": "status"},
        "ExpressionAttributeValues": {":new": {"S": "TRANSPORT"}, ":expected": {"S": "CERTIFICATE"}},
    })
    stub.add_client_error("update_item", service_error_code="ConditionalCheckFailedException")
    with stub:
        repository = DynamoNotificationRepository(client)
        repository.release_lock("tabla", [KEY], "tok")
        assert repository.change_status("tabla", KEY, "CERTIFICATE", "TRANSPORT") is True
        assert repository.change_status("tabla", KEY, "CERTIFICATE", "TRANSPORT") is False


def test_dynamo_status_change_other_errors_raise():
    client, stub = _dynamo()
    stub.add_client_error("update_item", service_error_code="AccessDeniedException")
    with stub, pytest.raises(AccessDeniedError):
        DynamoNotificationRepository(client).change_status("tabla", KEY, "A", "B")


def test_dynamo_marks_alerted():
    client, stub = _dynamo()
    stub.add_response("update_item", {}, {
        "TableName": "tabla", "Key": ANY, "UpdateExpression": "SET sendAlertDate = :date",
        "ExpressionAttributeValues": {":date": {"S": "hoy"}},
    })
    with stub:
        DynamoNotificationRepository(client).mark_alerted("tabla", [KEY], "hoy")
    stub.assert_no_pending_responses()


def test_glue_starts_the_job():
    client, stub = _client("glue")
    stub.add_response("start_job_run", {"JobRunId": "jr_1"}, {"JobName": "job", "Arguments": {"--a": "1"}})
    with stub:
        assert GlueJobLauncher(client).start("job", {"--a": "1"}) == "jr_1"


@pytest.mark.parametrize(
    "code, error_type, recoverable",
    [
        ("ConcurrentRunsExceededException", PlatformError, True),
        ("EntityNotFoundException", ConfigurationError, False),
        ("ValidationException", PlatformError, False),
    ],
)
def test_glue_errors_are_classified(code, error_type, recoverable):
    client, stub = _client("glue")
    stub.add_client_error("start_job_run", service_error_code=code)
    with stub, pytest.raises(error_type) as error:
        GlueJobLauncher(client).start("job", {})
    assert error.value.recoverable is recoverable


@pytest.mark.parametrize("configuration_set", ["conjunto", None])
def test_ses_sends_the_alert(configuration_set):
    client, stub = _client("sesv2")
    expected = {
        "FromEmailAddress": "r@x.test",
        "Destination": {"ToAddresses": ["s@x.test"]},
        "Content": {"Simple": {
            "Subject": {"Data": "asunto", "Charset": "UTF-8"},
            "Body": {"Text": {"Data": "cuerpo", "Charset": "UTF-8"}},
        }},
    }
    if configuration_set:
        expected["ConfigurationSetName"] = configuration_set
    stub.add_response("send_email", {"MessageId": "m"}, expected)
    with stub:
        settings = AlertSettings("r@x.test", configuration_set, ("s@x.test",))
        SesAlertSender(client).send(settings, Alert("asunto", "cuerpo"))


def test_translating_wraps_network_errors():
    with pytest.raises(PlatformError) as error, translating("op"):
        raise EndpointConnectionError(endpoint_url="x")
    assert error.value.recoverable is True


def test_system_clock_uses_the_configured_zone():
    now = SystemClock().now_local("America/Bogota")
    assert now.utcoffset().total_seconds() == -5 * 3600
    assert SystemClock.now_utc().tzinfo is not None
    with pytest.raises(ConfigurationError):
        SystemClock().now_local("Zona/Inexistente")


def test_json_event_log_writes_spanish_messages():
    lines = []
    log = JsonEventLog(StructuredLogger("comp", lines.append, lambda: datetime(2026, 1, 1)))
    log.bind(cert_code="C")
    log.record(LogEvent.LOCK_TAKEN, level="WARN", extra=1)
    line = json.loads(lines[0])
    assert line["evento"] == "ORQ_CANDADO_OCUPADO" and line["cert_code"] == "C" and line["nivel"] == "WARN"
