import sys
import time
from functools import partial
from typing import Callable, Sequence

from niif_observability import StructuredLogger

from notificacion_certificacion.application.notificar_certificacion import ResultadoJob
from notificacion_certificacion.application.puertos import MENSAJES_EVENTO, EventoLog
from notificacion_certificacion.config.ajustes import leer_parametros
from notificacion_certificacion.config.composicion import COMPONENTE, Contexto, construir_contexto
from notificacion_certificacion.domain.errores import ErrorNotificacion, ErrorPlataforma
from notificacion_certificacion.domain.modelos import ParametrosEjecucion

CODIGO_ERROR_NO_CLASIFICADO = "NTF-PLA-999"
MILISEGUNDOS_POR_SEGUNDO = 1000


def _escribir(linea: str) -> None:
    print(linea, flush=True)


def _clasificar(error: Exception) -> ErrorNotificacion:
    if isinstance(error, ErrorNotificacion):
        return error
    return ErrorPlataforma(CODIGO_ERROR_NO_CLASIFICADO, str(error))


def _detalle(error: ErrorNotificacion) -> dict:
    datos = error.como_diccionario()
    datos["detalle"] = datos.pop("mensaje")
    return datos


def _registrar_error_de_arranque(escribir: Callable[[str], None], error: Exception) -> None:
    registrador = StructuredLogger(COMPONENTE, escribir)
    registrador.log(EventoLog.ERROR_JOB.value, MENSAJES_EVENTO[EventoLog.ERROR_JOB], "ERROR", **_detalle(_clasificar(error)))


def _resumir(contexto: Contexto, exitoso: bool, inicio: float) -> None:
    caso = contexto.caso_de_uso
    contexto.bitacora.registrar(
        EventoLog.RESUMEN_JOB,
        job_exitoso=1 if exitoso else 0,
        job_fallido=0 if exitoso else 1,
        correos_enviados=caso.enviados,
        correos_fallidos=caso.fallidos,
        duracion_ms=round((time.monotonic() - inicio) * MILISEGUNDOS_POR_SEGUNDO),
    )


def ejecutar(
    argv: Sequence[str] | None = None,
    leer: Callable[[Sequence[str]], ParametrosEjecucion] = leer_parametros,
    construir: Callable[[ParametrosEjecucion], Contexto] = construir_contexto,
    escribir: Callable[[str], None] = _escribir,
) -> ResultadoJob:
    inicio = time.monotonic()
    argumentos = sys.argv[1:] if argv is None else argv
    try:
        contexto = construir(leer(argumentos))
    except Exception as error:
        _registrar_error_de_arranque(escribir, error)
        raise
    resumir = partial(_resumir, contexto, inicio=inicio)
    try:
        resultado = contexto.caso_de_uso.ejecutar()
    except Exception as error:
        contexto.bitacora.registrar(EventoLog.ERROR_JOB, nivel="ERROR", **_clasificar(error).como_diccionario())
        resumir(exitoso=False)
        raise
    resumir(exitoso=True)
    return resultado
