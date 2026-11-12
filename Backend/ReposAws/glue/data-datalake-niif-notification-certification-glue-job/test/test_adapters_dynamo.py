import boto3
import pytest
from botocore.stub import Stubber

from builders import build_record, build_composite_record
from certification_notifier.adapters.outbound.dynamo_composite import DynamoCompositeRepository
from certification_notifier.adapters.outbound.dynamo_links import DynamoLinkRepository
from certification_notifier.adapters.outbound.dynamo_notifications import DynamoNotificationRepository
from certification_notifier.adapters.outbound.dynamo_types import deserialize, serialize
from certification_notifier.domain.errors import ConcurrencyError, ConfigurationError
from certification_notifier.domain.models import ShortLink, SentEmail, CompositeReservation


def _client():
    client = boto3.client("dynamodb", region_name="us-east-1")
    return client, Stubber(client)


def _item(**extra):
    base = {
        "useCaseId": {"S": "casoA"},
        "executionId": {"S": "ej1"},
        "period": {"S": "202610"},
        "status": {"S": "PENDING"},
        "userEmail": {"S": "usuario@example.com"},
        "queryName": {"S": "detail#a"},
        "nameFile": {"S": "a.csv"},
        "s3_url": {"S": "https://s3.example/a.csv"},
        "sinDatosPeriodo": {"BOOL": True},
    }
    base.update(extra)
    return base


def _expected_query(**extra):
    base = {
        "TableName": "table",
        "IndexName": "index_name",
        "KeyConditionExpression": "certCode = :cert",
        "ExpressionAttributeValues": {":cert": {"S": "CERT1"}},
    }
    base.update(extra)
    return base


def test_serialize_and_deserialize_are_inverse():
    values = {"text": "a", "entero": 3, "bandera": True, "lista": ["x", "y"]}
    assert deserialize(serialize(values))["text"] == "a"
    assert serialize({"n": 3}) == {"n": {"N": "3"}}


def test_find_by_cert_code_filters_use_case_and_maps_fields():
    client, stub = _client()
    stub.add_response(
        "query",
        {"Items": [_item(), _item(useCaseId={"S": "otro"}, executionId={"S": "ej9"})]},
        _expected_query(),
    )
    with stub:
        records = DynamoNotificationRepository(client).find_by_cert_code("table", "index_name", "CERT1", "casoA")

    assert records == (
        build_record(
            status="PENDING",
            user_email="usuario@example.com",
            s3_url="https://s3.example/a.csv",
            no_data_for_period=True,
        ),
    )


def test_find_by_cert_code_paginates_until_results_end():
    client, stub = _client()
    item_key = {"useCaseId": {"S": "casoA"}, "executionId": {"S": "ej1"}}
    stub.add_response("query", {"Items": [_item()], "LastEvaluatedKey": item_key}, _expected_query())
    stub.add_response(
        "query",
        {"Items": [_item(executionId={"S": "ej2"})]},
        _expected_query(ExclusiveStartKey=item_key),
    )
    with stub:
        records = DynamoNotificationRepository(client).find_by_cert_code("table", "index_name", "CERT1", "casoA")

    assert [record.execution_id for record in records] == ["ej1", "ej2"]


def test_find_by_cert_code_uses_defaults_for_optional_fields():
    client, stub = _client()
    minimum = {"useCaseId": {"S": "casoA"}, "executionId": {"S": "ej1"}, "period": {"S": "202610"}}
    stub.add_response("query", {"Items": [minimum]}, _expected_query())
    with stub:
        (record,) = DynamoNotificationRepository(client).find_by_cert_code("table", "index_name", "CERT1", "casoA")

    assert (record.status, record.file_name, record.no_data_for_period) == ("", "", False)


def test_find_by_cert_code_rejects_records_without_required_fields():
    client, stub = _client()
    without_period = {"useCaseId": {"S": "casoA"}, "executionId": {"S": "ej1"}}
    stub.add_response("query", {"Items": [without_period]}, _expected_query())
    with stub, pytest.raises(ConfigurationError) as error:
        DynamoNotificationRepository(client).find_by_cert_code("table", "index_name", "CERT1", "casoA")
    assert "period" in error.value.message


def test_mark_sent_updates_each_record_with_send_data():
    client, stub = _client()
    expected = {
        "TableName": "table",
        "Key": {"useCaseId": {"S": "casoA"}, "executionId": {"S": "ej1"}},
        "UpdateExpression": (
            "SET #st = :s, sentDate = :d, notificationChannel = :ch, "
            "sesMessageId = :mid, emailDestination = :dest"
        ),
        "ExpressionAttributeNames": {"#st": "status"},
        "ExpressionAttributeValues": {
            ":s": {"S": "SENT"},
            ":d": {"S": "2026-11-04T10:00:00-05:00"},
            ":ch": {"S": "SES_EMAIL"},
            ":mid": {"S": "m1,m2"},
            ":dest": {"L": [{"S": "a@example.com"}, {"S": "b@example.com"}]},
        },
    }
    second = {**expected, "Key": {"useCaseId": {"S": "casoA"}, "executionId": {"S": "ej2"}}}
    stub.add_response("update_item", {}, expected)
    stub.add_response("update_item", {}, second)
    sent_emails = [SentEmail("a@example.com", "m1"), SentEmail("b@example.com", "m2")]
    with stub:
        DynamoNotificationRepository(client).mark_sent(
            "table",
            [build_record(), build_record(execution_id="ej2")],
            "SENT",
            "SES_EMAIL",
            sent_emails,
            "2026-11-04T10:00:00-05:00",
        )
    stub.assert_no_pending_responses()


def test_mark_sent_translates_aws_errors():
    client, stub = _client()
    stub.add_client_error("update_item", service_error_code="AccessDeniedException")
    with stub, pytest.raises(Exception) as error:
        DynamoNotificationRepository(client).mark_sent(
            "table", [build_record()], "SENT", "SES_EMAIL", [SentEmail("a@x.com", "m")], "f"
        )
    assert error.value.code == "NTF-PER-001"


def _link():
    return ShortLink("tk000001", "https://largo.example/x", "casoA", "CERT1", "CERTIFICATE", "2026-11-04T10:00:00", 1790000000)


def test_save_link_writes_item_only_if_token_is_new():
    client, stub = _client()
    stub.add_response(
        "put_item",
        {},
        {
            "TableName": "links",
            "Item": {
                "token": {"S": "tk000001"},
                "url": {"S": "https://largo.example/x"},
                "useCaseId": {"S": "casoA"},
                "certCode": {"S": "CERT1"},
                "action": {"S": "CERTIFICATE"},
                "creationDate": {"S": "2026-11-04T10:00:00"},
                "expiresAt": {"N": "1790000000"},
            },
            "ConditionExpression": "attribute_not_exists(#tk)",
            "ExpressionAttributeNames": {"#tk": "token"},
        },
    )
    with stub:
        DynamoLinkRepository(client).save("links", _link())
    stub.assert_no_pending_responses()


def test_save_link_with_repeated_token_fails_as_concurrency():
    client, stub = _client()
    stub.add_client_error("put_item", service_error_code="ConditionalCheckFailedException")
    with stub, pytest.raises(ConcurrencyError):
        DynamoLinkRepository(client).save("links", _link())


def _composite_key(member: str) -> dict:
    return {"compositeId": {"S": "grupo#202610"}, "memberId": {"S": member}}


def test_get_member_maps_item_keeping_original_types():
    client, stub = _client()
    item = {
        "status": {"S": "READY_TO_NOTIFY"},
        "period": {"S": "202610"},
        "sourceExecutionId": {"S": "origen-1"},
        "loteCargueId": {"N": "7"},
        "updatedAt": {"S": "2026-11-04T10:00:00"},
    }
    stub.add_response(
        "get_item",
        {"Item": item},
        {"TableName": "t", "Key": _composite_key("m1"), "ConsistentRead": True},
    )
    with stub:
        record = DynamoCompositeRepository(client).get("t", "grupo#202610", "m1")

    assert record.status == "READY_TO_NOTIFY"
    assert record.batch_id == 7
    assert record.cert_code == ""


def test_get_missing_member_returns_none():
    client, stub = _client()
    stub.add_response("get_item", {}, {"TableName": "t", "Key": _composite_key("m1"), "ConsistentRead": True})
    with stub:
        assert DynamoCompositeRepository(client).get("t", "grupo#202610", "m1") is None


def _reservation():
    return CompositeReservation(
        table="t",
        composite_id="grupo#202610",
        cert_code="CERT1",
        control=build_composite_record(member_id="_CONTROL", status="NOTIFICATION_IN_PROGRESS"),
        members=(build_composite_record(member_id="m1"),),
        already_completed=False,
    )


def test_consume_updates_members_and_control_in_one_transaction():
    client, stub = _client()
    member = {
        "Update": {
            "TableName": "t",
            "Key": _composite_key("m1"),
            "UpdateExpression": (
                "SET #st = :consumed, notificationReleased = :yes, completedAt = :now, "
                "updatedAt = :now, certCode = :cert, sesMessageId = :mid"
            ),
            "ConditionExpression": (
                "#st = :ready AND sourceExecutionId = :exec AND loteCargueId = :lot AND updatedAt = :previous"
            ),
            "ExpressionAttributeNames": {"#st": "status"},
            "ExpressionAttributeValues": {
                ":consumed": {"S": "CONSUMED"},
                ":yes": {"BOOL": True},
                ":now": {"S": "2026-11-04T15:00:00+00:00"},
                ":cert": {"S": "CERT1"},
                ":mid": {"S": "m1,m2"},
                ":ready": {"S": "READY_TO_NOTIFY"},
                ":exec": {"S": "origen-1"},
                ":lot": {"S": "lote-1"},
                ":previous": {"S": "2026-11-04T10:00:00"},
            },
        }
    }
    control = {
        "Update": {
            "TableName": "t",
            "Key": _composite_key("_CONTROL"),
            "UpdateExpression": (
                "SET #st = :done, completedAt = :now, updatedAt = :now, certCode = :cert, sesMessageId = :mid"
            ),
            "ConditionExpression": "#st = :progress AND updatedAt = :previous",
            "ExpressionAttributeNames": {"#st": "status"},
            "ExpressionAttributeValues": {
                ":done": {"S": "NOTIFICATION_COMPLETED"},
                ":now": {"S": "2026-11-04T15:00:00+00:00"},
                ":cert": {"S": "CERT1"},
                ":mid": {"S": "m1,m2"},
                ":progress": {"S": "NOTIFICATION_IN_PROGRESS"},
                ":previous": {"S": "2026-11-04T10:00:00"},
            },
        }
    }
    stub.add_response("transact_write_items", {}, {"TransactItems": [member, control]})
    with stub:
        DynamoCompositeRepository(client).consume(_reservation(), "m1,m2", "2026-11-04T15:00:00+00:00")
    stub.assert_no_pending_responses()


def test_consume_with_failed_condition_fails_as_concurrency():
    client, stub = _client()
    stub.add_client_error("transact_write_items", service_error_code="TransactionCanceledException")
    with stub, pytest.raises(ConcurrencyError):
        DynamoCompositeRepository(client).consume(_reservation(), "m1", "occurred_at")
