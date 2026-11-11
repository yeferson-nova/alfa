from typing import Any

from niif_observability import StructuredLogger

from notificacion_certificacion.application.puertos import MENSAJES_EVENTO, EventoLog

CAMPO_MENSAJE = "mensaje"
CAMPO_DETALLE = "detalle"


class BitacoraJson:
    def __init__(self, registrador: StructuredLogger) -> None:
        self._registrador = registrador

    def fijar_contexto(self, **contexto: Any) -> None:
        self._registrador.bind(**contexto)

    def registrar(self, evento: EventoLog, nivel: str = "INFO", **contexto: Any) -> None:
        if CAMPO_MENSAJE in contexto:
            contexto[CAMPO_DETALLE] = contexto.pop(CAMPO_MENSAJE)
        self._registrador.log(evento.value, MENSAJES_EVENTO[evento], nivel, **contexto)
