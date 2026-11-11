from typing import Any, Mapping, Sequence

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from certification_job.adapters.outbound.aws_errors import translating
from certification_job.domain.models import NotificationKey

SERIALIZER = TypeSerializer()
DESERIALIZER = TypeDeserializer()
PENDING_UPDATE = (
    "SET #st = :status, pendingDate = :pendingDate, configBucket = :configBucket, "
    "configKey = :configKey, globalConfigKey = :globalConfigKey"
)


def serialize(values: Mapping[str, Any]) -> dict[str, Any]:
    return {name: SERIALIZER.serialize(value) for name, value in values.items()}


def deserialize(item: Mapping[str, Any]) -> dict[str, Any]:
    return {name: DESERIALIZER.deserialize(value) for name, value in item.items()}


class DynamoNotificationRepository:
    def __init__(self, client: Any) -> None:
        self._client = client

    def put(self, table: str, item: Mapping[str, Any]) -> None:
        with translating(f"DynamoDB put_item {table}"):
            self._client.put_item(TableName=table, Item=serialize(item))

    def find_by_cert_code(
        self, table: str, index: str, cert_code: str, use_case_id: str
    ) -> Sequence[NotificationKey]:
        return tuple(
            NotificationKey(use_case_id=str(item["useCaseId"]), execution_id=str(item["executionId"]))
            for item in self._query(table, index, cert_code)
            if item.get("useCaseId") == use_case_id and "executionId" in item
        )

    def mark_pending(self, table: str, keys: Sequence[NotificationKey], values: Mapping[str, str]) -> None:
        attribute_values = serialize({f":{name}": value for name, value in values.items()})
        for key in keys:
            with translating(f"DynamoDB update_item {table}"):
                self._client.update_item(
                    TableName=table,
                    Key=serialize({"useCaseId": key.use_case_id, "executionId": key.execution_id}),
                    UpdateExpression=PENDING_UPDATE,
                    ExpressionAttributeNames={"#st": "status"},
                    ExpressionAttributeValues=attribute_values,
                )

    def _query(self, table: str, index: str, cert_code: str) -> list[dict[str, Any]]:
        arguments: dict[str, Any] = {
            "TableName": table,
            "IndexName": index,
            "KeyConditionExpression": "certCode = :cert",
            "ExpressionAttributeValues": serialize({":cert": cert_code}),
        }
        items: list[dict[str, Any]] = []
        while True:
            with translating(f"DynamoDB query {table}/{index}"):
                response = self._client.query(**arguments)
            items.extend(deserialize(item) for item in response.get("Items", []))
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                return items
            arguments["ExclusiveStartKey"] = last_key
