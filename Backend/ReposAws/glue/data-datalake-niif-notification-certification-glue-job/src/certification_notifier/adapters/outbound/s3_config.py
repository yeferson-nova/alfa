import json
from typing import Any, Mapping

from certification_notifier.adapters.outbound.aws_errors import translating
from certification_notifier.domain.constants import ENCODING
from certification_notifier.domain.errors import ConfigurationError
from certification_notifier.domain.paths import normalize_key


class S3ConfigRepository:
    def __init__(self, client: Any) -> None:
        self._client = client

    def load_json(self, bucket: str, key: str) -> Mapping[str, Any]:
        normalized_key = normalize_key(bucket, key)
        content = self._read(bucket, normalized_key)
        return self._parse(content, f"{bucket}/{normalized_key}")

    def load_text(self, bucket: str, key: str) -> str:
        return self._read(bucket, normalize_key(bucket, key))

    def _read(self, bucket: str, key: str) -> str:
        with translating(f"S3 get_object {bucket}/{key}"):
            response = self._client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read().decode(ENCODING)

    @staticmethod
    def _parse(content: str, source: str) -> Mapping[str, Any]:
        try:
            data = json.loads(content)
        except json.JSONDecodeError as error:
            raise ConfigurationError("NTF-CFG-001", f"JSON invalido en {source}") from error
        if not isinstance(data, dict):
            raise ConfigurationError("NTF-CFG-001", f"El JSON debe ser un objeto en {source}")
        return data
