import json
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from certification_orchestrator.adapters.outbound.aws_errors import error_code, translate, translating
from certification_orchestrator.domain.errors import ConfigurationError, PlatformError

ENCODING = "utf-8"
S3_SCHEME = "s3://"
MISSING_OBJECT_CODES = frozenset({"404", "NoSuchKey", "NotFound"})


def normalize_key(bucket: str, key: str) -> str:
    cleaned = key.strip()
    own_prefix = f"{S3_SCHEME}{bucket}/"
    if cleaned.startswith(own_prefix):
        cleaned = cleaned[len(own_prefix):]
    if cleaned.startswith(S3_SCHEME):
        raise ConfigurationError("ORQ-CFG-005", f"La ruta {key} no pertenece al bucket {bucket}")
    return cleaned.lstrip("/")


class S3ObjectStore:
    def __init__(self, client: Any) -> None:
        self._client = client

    def load_json(self, bucket: str, key: str) -> Any:
        normalized = normalize_key(bucket, key)
        with translating(f"S3 get_object {bucket}"):
            body = self._client.get_object(Bucket=bucket, Key=normalized)["Body"].read()
        try:
            return json.loads(body.decode(ENCODING))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ConfigurationError("ORQ-CFG-001", f"JSON invalido en {normalized}: {error}") from error

    def exists(self, bucket: str, key: str) -> bool:
        operation = f"S3 head_object {bucket}"
        try:
            self._client.head_object(Bucket=bucket, Key=key)
        except ClientError as error:
            if error_code(error) in MISSING_OBJECT_CODES:
                return False
            raise translate(error, operation) from error
        except BotoCoreError as error:
            raise PlatformError("ORQ-PLA-001", f"{operation}: {error}", recoverable=True) from error
        return True

    def copy(self, source_bucket: str, source_key: str, target_bucket: str, target_key: str) -> None:
        with translating(f"S3 copy_object {source_bucket} -> {target_bucket}"):
            self._client.copy_object(
                Bucket=target_bucket,
                Key=target_key,
                CopySource={"Bucket": source_bucket, "Key": source_key},
            )

    def delete(self, bucket: str, key: str) -> None:
        with translating(f"S3 delete_object {bucket}"):
            self._client.delete_object(Bucket=bucket, Key=key)
