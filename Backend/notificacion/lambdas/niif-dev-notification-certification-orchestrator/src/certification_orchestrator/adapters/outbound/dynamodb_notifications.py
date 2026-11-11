from typing import Any, Iterator, Mapping, Sequence

from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError

from certification_orchestrator.adapters.outbound.aws_errors import error_code, translate, translating
from certification_orchestrator.domain.models import LockAcquisition, LockRequest, NotificationRecord, RecordKey
from certification_orchestrator.domain.records import (
    CERT_CODE,
    EXECUTION_ID,
    PENDING_DATE,
    SEND_ALERT_DATE,
    SEND_ATTEMPTS,
    SEND_LOCK,
    SEND_LOCK_DATE,
    STATUS,
    USE_CASE_ID,
    record_from_image,
    record_key,
)

SERIALIZER = TypeSerializer()
CONDITION_FAILED = "ConditionalCheckFailedException"
TRANSACTION_CANCELED = "TransactionCanceledException"
CONDITION_FAILED_REASON = "ConditionalCheckFailed"
LOCK_UPDATE = f"SET {SEND_LOCK} = :token, {SEND_LOCK_DATE} = :date ADD {SEND_ATTEMPTS} :one"
RELEASE_UPDATE = f"REMOVE {SEND_LOCK}, {SEND_LOCK_DATE}"


def serialize(values: Mapping[str, Any]) -> dict[str, Any]:
    return {name: SERIALIZER.serialize(value) for name, value in values.items()}


def key_of(key: RecordKey) -> dict[str, Any]:
    return serialize({USE_CASE_ID: key.use_case_id, EXECUTION_ID: key.execution_id})


def _canceled_by_condition(error: ClientError) -> bool:
    if error_code(error) != TRANSACTION_CANCELED:
        return False
    reasons = error.response.get("CancellationReasons") or []
    return any(reason.get("Code") == CONDITION_FAILED_REASON for reason in reasons)


def _lock_update(table: str, request: LockRequest, acquisition: LockAcquisition) -> dict[str, Any]:
    values: dict[str, Any] = {":token": acquisition.token, ":date": acquisition.date, ":one": 1}
    names: dict[str, str] = {}
    if request.current_token is None:
        condition = f"attribute_not_exists({SEND_LOCK})"
    else:
        condition = f"{SEND_LOCK} = :current"
        values[":current"] = request.current_token
    if acquisition.required_status is not None:
        condition += " AND #st = :status"
        names["#st"] = STATUS
        values[":status"] = acquisition.required_status
    update: dict[str, Any] = {
        "TableName": table,
        "Key": key_of(request.key),
        "UpdateExpression": LOCK_UPDATE,
        "ConditionExpression": condition,
        "ExpressionAttributeValues": serialize(values),
    }
    if names:
        update["ExpressionAttributeNames"] = names
    return {"Update": update}


class DynamoNotificationRepository:
    def __init__(self, client: Any) -> None:
        self._client = client

    def find_keys_by_cert_code(self, table: str, index: str, cert_code: str) -> Sequence[RecordKey]:
        arguments = {
            "TableName": table,
            "IndexName": index,
            "KeyConditionExpression": f"{CERT_CODE} = :cert",
            "ExpressionAttributeValues": serialize({":cert": cert_code}),
        }
        return tuple(record_key(item) for item in self._query(arguments))

    def find_pending_before(
        self, table: str, index: str, status: str, before: str
    ) -> Sequence[NotificationRecord]:
        arguments = {
            "TableName": table,
            "IndexName": index,
            "KeyConditionExpression": f"#st = :status AND {PENDING_DATE} < :before",
            "ExpressionAttributeNames": {"#st": STATUS},
            "ExpressionAttributeValues": serialize({":status": status, ":before": before}),
        }
        return tuple(record_from_image(item) for item in self._query(arguments))

    def acquire_lock(self, table: str, acquisition: LockAcquisition) -> bool:
        items = [_lock_update(table, request, acquisition) for request in acquisition.requests]
        operation = f"DynamoDB transact_write_items {table}"
        try:
            self._client.transact_write_items(TransactItems=items)
        except ClientError as error:
            if _canceled_by_condition(error):
                return False
            raise translate(error, operation) from error
        return True

    def release_lock(self, table: str, keys: Sequence[RecordKey], token: str) -> None:
        for key in keys:
            self._conditional_update(
                table,
                key,
                RELEASE_UPDATE,
                f"{SEND_LOCK} = :token",
                {"values": serialize({":token": token})},
            )

    def change_status(self, table: str, key: RecordKey, expected: str, new: str) -> bool:
        return self._conditional_update(
            table,
            key,
            "SET #st = :new",
            "#st = :expected",
            {"names": {"#st": STATUS}, "values": serialize({":new": new, ":expected": expected})},
        )

    def mark_alerted(self, table: str, keys: Sequence[RecordKey], date: str) -> None:
        for key in keys:
            with translating(f"DynamoDB update_item {table}"):
                self._client.update_item(
                    TableName=table,
                    Key=key_of(key),
                    UpdateExpression=f"SET {SEND_ALERT_DATE} = :date",
                    ExpressionAttributeValues=serialize({":date": date}),
                )

    def _conditional_update(
        self, table: str, key: RecordKey, update: str, condition: str, expression: Mapping[str, Any]
    ) -> bool:
        arguments: dict[str, Any] = {
            "TableName": table,
            "Key": key_of(key),
            "UpdateExpression": update,
            "ConditionExpression": condition,
            "ExpressionAttributeValues": expression["values"],
        }
        if "names" in expression:
            arguments["ExpressionAttributeNames"] = expression["names"]
        try:
            self._client.update_item(**arguments)
        except ClientError as error:
            if error_code(error) == CONDITION_FAILED:
                return False
            raise translate(error, f"DynamoDB update_item {table}") from error
        return True

    def _query(self, arguments: dict[str, Any]) -> Iterator[Mapping[str, Any]]:
        request = dict(arguments)
        while True:
            with translating(f"DynamoDB query {request['TableName']}/{request['IndexName']}"):
                response = self._client.query(**request)
            yield from response.get("Items", [])
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                return
            request["ExclusiveStartKey"] = last_key
