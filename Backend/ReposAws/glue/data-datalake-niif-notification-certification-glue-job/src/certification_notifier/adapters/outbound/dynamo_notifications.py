from typing import Any, Mapping, Sequence

from certification_notifier.adapters.outbound.dynamo_types import deserialize, serialize
from certification_notifier.adapters.outbound.aws_errors import translating
from certification_notifier.domain.errors import ConfigurationError
from certification_notifier.domain.models import SentEmail, NotificationRecord

IDS_SEPARATOR = ","
UPDATE_SENT = (
    "SET #st = :s, sentDate = :d, notificationChannel = :ch, sesMessageId = :mid, emailDestination = :dest"
)


class DynamoNotificationRepository:
    def __init__(self, client: Any) -> None:
        self._client = client

    def find_by_cert_code(
        self, table: str, index_name: str, cert_code: str, use_case_id: str
    ) -> tuple[NotificationRecord, ...]:
        items = self._query_items(table, index_name, cert_code)
        return tuple(self._to_record(item) for item in items if item.get("useCaseId") == use_case_id)

    def mark_sent(
        self,
        table: str,
        records: Sequence[NotificationRecord],
        status: str,
        channel: str,
        sent_emails: Sequence[SentEmail],
        sent_at: str,
    ) -> None:
        values = serialize(
            {
                ":s": status,
                ":d": sent_at,
                ":ch": channel,
                ":mid": IDS_SEPARATOR.join(sent_email.message_id for sent_email in sent_emails),
                ":dest": [sent_email.recipient for sent_email in sent_emails],
            }
        )
        for record in records:
            with translating(f"DynamoDB update_item {table}"):
                self._client.update_item(
                    TableName=table,
                    Key=serialize({"useCaseId": record.use_case_id, "executionId": record.execution_id}),
                    UpdateExpression=UPDATE_SENT,
                    ExpressionAttributeNames={"#st": "status"},
                    ExpressionAttributeValues=values,
                )

    def _query_items(self, table: str, index_name: str, cert_code: str) -> list[dict[str, Any]]:
        arguments: dict[str, Any] = {
            "TableName": table,
            "IndexName": index_name,
            "KeyConditionExpression": "certCode = :cert",
            "ExpressionAttributeValues": serialize({":cert": cert_code}),
        }
        items: list[dict[str, Any]] = []
        while True:
            with translating(f"DynamoDB query {table}/{index_name}"):
                response = self._client.query(**arguments)
            items.extend(deserialize(item) for item in response.get("Items", []))
            following = response.get("LastEvaluatedKey")
            if not following:
                return items
            arguments["ExclusiveStartKey"] = following

    @staticmethod
    def _required(item: Mapping[str, Any], key: str) -> str:
        if key not in item:
            raise ConfigurationError("NTF-CFG-001", f"Registro de notificacion sin el campo {key}")
        return str(item[key])

    def _to_record(self, item: Mapping[str, Any]) -> NotificationRecord:
        return NotificationRecord(
            use_case_id=self._required(item, "useCaseId"),
            execution_id=self._required(item, "executionId"),
            period=self._required(item, "period"),
            status=str(item.get("status", "")),
            user_email=str(item.get("userEmail", "")),
            query_name=str(item.get("queryName", "")),
            file_name=str(item.get("nameFile", "")),
            s3_url=str(item.get("s3_url", "")),
            no_data_for_period=item.get("sinDatosPeriodo") is True,
        )
