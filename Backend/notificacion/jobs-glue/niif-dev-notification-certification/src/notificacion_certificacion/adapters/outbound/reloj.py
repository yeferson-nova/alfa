import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from notificacion_certificacion.domain.errores import ErrorConfiguracion


class RelojSistema:
    def __init__(self, zona_horaria: str) -> None:
        try:
            self._zona = ZoneInfo(zona_horaria)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ErrorConfiguracion("NTF-CFG-001", f"Zona horaria invalida en timezone: {zona_horaria}") from error

    def ahora_local(self) -> datetime:
        return datetime.now(self._zona)

    def ahora_utc(self) -> datetime:
        return datetime.now(timezone.utc)


class GeneradorTokensUuid:
    def generar(self, longitud: int) -> str:
        return uuid.uuid4().hex[:longitud]
