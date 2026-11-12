from typing import Any

from certification_notifier.adapters.outbound.dynamo_types import serialize
from certification_notifier.adapters.outbound.aws_errors import translating
from certification_notifier.domain.models import ShortLink

NEW_TOKEN_CONDITION = "attribute_not_exists(#tk)"


class DynamoLinkRepository:
    def __init__(self, client: Any) -> None:
        self._client = client

    def save(self, table: str, link: ShortLink) -> None:
        item = serialize(
            {
                "token": link.token,
                "url": link.url,
                "useCaseId": link.use_case_id,
                "certCode": link.cert_code,
                "action": link.action,
                "creationDate": link.created_at,
                "expiresAt": link.expires_at,
            }
        )
        with translating(f"DynamoDB put_item {table}"):
            self._client.put_item(
                TableName=table,
                Item=item,
                ConditionExpression=NEW_TOKEN_CONDITION,
                ExpressionAttributeNames={"#tk": "token"},
            )
