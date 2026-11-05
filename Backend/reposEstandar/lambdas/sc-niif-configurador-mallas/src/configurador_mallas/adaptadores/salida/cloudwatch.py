"""Adaptador de observabilidad: logs JSON y métricas EMF en CloudWatch.

Una línea JSON por evento, siempre con ``ejecucion_id`` (HU-GI-05 y HU-GI-07). Los
eventos salen del catálogo cerrado del puerto (``EventoLog``). Las métricas usan el
formato EMF: CloudWatch las extrae del log sin llamadas adicionales a la API.
"""
import json
import logging
import sys
import time
from datetime import datetime, timezone

from ...aplicacion.puertos import EventoLog, Metrica

NOMBRE_LOGGER = "configurador_mallas"
SIN_VALOR = "NO_DISPONIBLE"


class _FormatoJson(logging.Formatter):
    def format(self, record):
        cuerpo = record.msg if isinstance(record.msg, dict) else {"mensaje": record.getMessage()}
        base = {
            "marcaTiempo": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "nivel": record.levelname,
        }
        return json.dumps({**base, **cuerpo}, ensure_ascii=False, default=str)


def crear_logger(nivel, salida=None):
    """Logger propio con salida JSON a stdout (CloudWatch Logs)."""
    logger = logging.getLogger(NOMBRE_LOGGER)
    logger.handlers.clear()
    manejador = logging.StreamHandler(salida or sys.stdout)
    manejador.setFormatter(_FormatoJson())
    logger.addHandler(manejador)
    logger.setLevel(nivel)
    logger.propagate = False
    return logger


class Observador:
    """Registra eventos, errores y métricas con el contexto de la invocación."""

    def __init__(self, logger, namespace, funcion, reloj=time.time):
        self._logger = logger
        self._namespace = namespace
        self._reloj = reloj
        self._contexto = {"funcion": funcion}

    def usar_namespace(self, namespace):
        self._namespace = namespace

    def con_contexto(self, **campos):
        self._contexto.update({k: v for k, v in campos.items() if v is not None})

    def evento(self, evento, **campos):
        self._logger.info({"evento": evento.value, **self._contexto, **campos})

    def error(self, error):
        self._logger.error({"evento": EventoLog.CORRIDA_FALLIDA.value, **self._contexto,
                            "error": error.to_dict()})
        self.metrica(Metrica.CORRIDA_FALLIDA, 1, "Count",
                     Dominio=error.clasificacion.dominio)

    def metrica(self, metrica, valor, unidad, **dimensiones_extra):
        if not self._namespace:  # solo ocurre si falló la lectura del entorno
            return
        dimensiones = {
            "Funcion": str(self._contexto["funcion"]),
            "Malla": str(self._contexto.get("malla", SIN_VALOR)),
            **{k: str(v) for k, v in dimensiones_extra.items()},
        }
        self._logger.info({
            "_aws": {
                "Timestamp": int(self._reloj() * 1000),
                "CloudWatchMetrics": [{
                    "Namespace": self._namespace,
                    "Dimensions": [list(dimensiones)],
                    "Metrics": [{"Name": metrica.value, "Unit": unidad}],
                }],
            },
            **dimensiones,
            metrica.value: valor,
            "ejecucion_id": self._contexto.get("ejecucion_id", SIN_VALOR),
        })
