import json
from typing import Any, Callable

from notificacion_certificacion.aplicacion.puertos import EventoLog, Reloj


class BitacoraJson:
    def __init__(
        self,
        reloj: Reloj,
        id_ejecucion: str,
        componente: str,
        escribir: Callable[[str], None],
    ) -> None:
        self._reloj = reloj
        self._id_ejecucion = id_ejecucion
        self._componente = componente
        self._escribir = escribir

    def registrar(self, evento: EventoLog, nivel: str = "INFO", **contexto: Any) -> None:
        linea = {
            "marcaTiempo": self._reloj.ahora_utc().isoformat(),
            "nivel": nivel,
            "evento": evento.value,
            "funcion": self._componente,
            "ejecucion_id": self._id_ejecucion,
            **contexto,
        }
        self._escribir(json.dumps(linea, ensure_ascii=False, default=str))
