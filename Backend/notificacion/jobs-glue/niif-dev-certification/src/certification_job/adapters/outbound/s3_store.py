import json
from typing import Any

from certification_job.adapters.outbound.aws_errors import translating
from certification_job.domain.errors import ConfigurationError
from certification_job.domain.naming import S3_SCHEME

ENCODING = "utf-8"


def normalize_key(bucket: str, key: str) -> str:
    cleaned = key.strip()
    own_prefix = f"{S3_SCHEME}{bucket}/"
    if cleaned.startswith(own_prefix):
        cleaned = cleaned[len(own_prefix):]
    if cleaned.startswith(S3_SCHEME):
        raise ConfigurationError("CER-CFG-005", f"La ruta {key} no pertenece al bucket {bucket}")
    return cleaned.lstrip("/")


class S3ObjectStore:
    def __init__(self, client: Any) -> None:
        self._client = client

    def load_json(self, bucket: str, key: str) -> Any:
        normalized = normalize_key(bucket, key)
        try:
            return json.loads(self.read(bucket, normalized).decode(ENCODING))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ConfigurationError("CER-CFG-001", f"JSON invalido en {normalized}: {error}") from error

    def read(self, bucket: str, key: str) -> bytes:
        with translating(f"S3 get_object {bucket}"):
            return self._client.get_object(Bucket=bucket, Key=key)["Body"].read()

    def list_keys(self, bucket: str, prefix: str) -> list[str]:
        keys: list[str] = []
        with translating(f"S3 list_objects_v2 {bucket}"):
            for page in self._client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
                keys.extend(item["Key"] for item in page.get("Contents", []))
        return keys

    def write(self, bucket: str, key: str, body: bytes, content_type: str) -> None:
        with translating(f"S3 put_object {bucket}"):
            self._client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
