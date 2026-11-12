import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from certification_notifier.domain.errors import InputError


class SystemClock:
    def __init__(self, timezone_name: str) -> None:
        try:
            self._zone = ZoneInfo(timezone_name)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise InputError("NTF-ENT-003", f"Zona horaria invalida: {timezone_name}") from error

    def local_now(self) -> datetime:
        return datetime.now(self._zone)

    def utc_now(self) -> datetime:
        return datetime.now(timezone.utc)


class UuidTokenGenerator:
    def generate(self, length: int) -> str:
        return uuid.uuid4().hex[:length]
