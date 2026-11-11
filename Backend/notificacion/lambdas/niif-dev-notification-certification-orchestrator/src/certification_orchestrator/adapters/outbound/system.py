from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from niif_observability import StructuredLogger

from certification_orchestrator.application.ports import EVENT_MESSAGES, LogEvent
from certification_orchestrator.domain.errors import ConfigurationError


class SystemClock:
    def now_local(self, timezone_name: str) -> datetime:
        try:
            zone = ZoneInfo(timezone_name)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ConfigurationError("ORQ-CFG-001", f"Zona horaria invalida en timezone: {timezone_name}") from error
        return datetime.now(zone)

    @staticmethod
    def now_utc() -> datetime:
        return datetime.now(timezone.utc)


class JsonEventLog:
    def __init__(self, logger: StructuredLogger) -> None:
        self._logger = logger

    def bind(self, **fields: Any) -> None:
        self._logger.bind(**fields)

    def record(self, event: LogEvent, level: str = "INFO", **fields: Any) -> None:
        self._logger.log(event.value, EVENT_MESSAGES[event], level, **fields)
