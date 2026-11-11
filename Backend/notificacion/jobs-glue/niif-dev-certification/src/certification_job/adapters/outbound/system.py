import uuid
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from niif_observability import StructuredLogger

from certification_job.application.ports import EVENT_MESSAGES, LogEvent
from certification_job.domain.errors import ConfigurationError


class SystemClock:
    def __init__(self, timezone_name: str) -> None:
        try:
            self._zone = ZoneInfo(timezone_name)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ConfigurationError("CER-CFG-001", f"Zona horaria invalida en timezone: {timezone_name}") from error

    def now_local(self) -> datetime:
        return datetime.now(self._zone)

    @staticmethod
    def now_utc() -> datetime:
        return datetime.now(timezone.utc)


class UuidGenerator:
    def execution_id(self) -> str:
        return str(uuid.uuid4())

    def unique_suffix(self) -> str:
        return uuid.uuid4().hex


class JsonEventLog:
    def __init__(self, logger: StructuredLogger) -> None:
        self._logger = logger

    def bind(self, **fields: Any) -> None:
        self._logger.bind(**fields)

    def record(self, event: LogEvent, level: str = "INFO", **fields: Any) -> None:
        self._logger.log(event.value, EVENT_MESSAGES[event], level, **fields)
