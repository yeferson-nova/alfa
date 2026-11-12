from typing import Any

from certification_notifier.adapters.outbound.dynamo_types import deserialize, serialize
from certification_notifier.adapters.outbound.aws_errors import translating
from certification_notifier.domain.constants import (
    STATUS_CONSUMED,
    STATUS_READY_TO_NOTIFY,
    STATUS_NOTIFICATION_COMPLETED,
    STATUS_NOTIFICATION_IN_PROGRESS,
)
from certification_notifier.domain.models import CompositeRecord, CompositeReservation

CONSUME_MEMBER = (
    "SET #st = :consumed, notificationReleased = :yes, completedAt = :now, updatedAt = :now, "
    "certCode = :cert, sesMessageId = :mid"
)
MEMBER_CONDITION = "#st = :ready AND sourceExecutionId = :exec AND loteCargueId = :lot AND updatedAt = :previous"
COMPLETE_CONTROL = (
    "SET #st = :done, completedAt = :now, updatedAt = :now, certCode = :cert, sesMessageId = :mid"
)
CONTROL_CONDITION = "#st = :progress AND updatedAt = :previous"
STATUS_ATTRIBUTE_NAMES = {"#st": "status"}


class DynamoCompositeRepository:
    def __init__(self, client: Any) -> None:
        self._client = client

    def get(self, table: str, composite_id: str, member_id: str) -> CompositeRecord | None:
        with translating(f"DynamoDB get_item {table}"):
            response = self._client.get_item(
                TableName=table,
                Key=serialize({"compositeId": composite_id, "memberId": member_id}),
                ConsistentRead=True,
            )
        item = response.get("Item")
        return None if item is None else self._to_record(member_id, deserialize(item))

    def consume(self, reservation: CompositeReservation, message_id: str, occurred_at: str) -> None:
        transactions = [self._consume_member(reservation, member, message_id, occurred_at) for member in reservation.members]
        transactions.append(self._complete_control(reservation, message_id, occurred_at))
        with translating(f"DynamoDB transact_write_items {reservation.table}"):
            self._client.transact_write_items(TransactItems=transactions)

    @staticmethod
    def _to_record(member_id: str, item: dict[str, Any]) -> CompositeRecord:
        return CompositeRecord(
            member_id=member_id,
            status=str(item.get("status", "")),
            period=item.get("period"),
            cert_code=str(item.get("certCode", "")),
            source_execution_id=item.get("sourceExecutionId"),
            batch_id=item.get("loteCargueId"),
            updated_at=item.get("updatedAt"),
        )

    @staticmethod
    def _consume_member(
        reservation: CompositeReservation, member: CompositeRecord, message_id: str, occurred_at: str
    ) -> dict[str, Any]:
        return {
            "Update": {
                "TableName": reservation.table,
                "Key": serialize({"compositeId": reservation.composite_id, "memberId": member.member_id}),
                "UpdateExpression": CONSUME_MEMBER,
                "ConditionExpression": MEMBER_CONDITION,
                "ExpressionAttributeNames": STATUS_ATTRIBUTE_NAMES,
                "ExpressionAttributeValues": serialize(
                    {
                        ":consumed": STATUS_CONSUMED,
                        ":yes": True,
                        ":now": occurred_at,
                        ":cert": reservation.cert_code,
                        ":mid": message_id,
                        ":ready": STATUS_READY_TO_NOTIFY,
                        ":exec": member.source_execution_id,
                        ":lot": member.batch_id,
                        ":previous": member.updated_at,
                    }
                ),
            }
        }

    @staticmethod
    def _complete_control(reservation: CompositeReservation, message_id: str, occurred_at: str) -> dict[str, Any]:
        return {
            "Update": {
                "TableName": reservation.table,
                "Key": serialize({"compositeId": reservation.composite_id, "memberId": reservation.control.member_id}),
                "UpdateExpression": COMPLETE_CONTROL,
                "ConditionExpression": CONTROL_CONDITION,
                "ExpressionAttributeNames": STATUS_ATTRIBUTE_NAMES,
                "ExpressionAttributeValues": serialize(
                    {
                        ":done": STATUS_NOTIFICATION_COMPLETED,
                        ":now": occurred_at,
                        ":cert": reservation.cert_code,
                        ":mid": message_id,
                        ":progress": STATUS_NOTIFICATION_IN_PROGRESS,
                        ":previous": reservation.control.updated_at,
                    }
                ),
            }
        }
