import sys
from typing import Callable, Sequence

from notificacion_certificacion.aplicacion.notificar_certificacion import ResultadoJob
from notificacion_certificacion.aplicacion.puertos import EventoLog
from notificacion_certificacion.configuracion.ajustes import leer_parametros
from notificacion_certificacion.configuracion.composicion import Contexto, construir_contexto
from notificacion_certificacion.dominio.errores import ErrorNotificacion, ErrorPlataforma
from notificacion_certificacion.dominio.modelos import ParametrosEjecucion

CODIGO_ERROR_NO_CLASIFICADO = "NTF-PLA-999"


def ejecutar(
    argv: Sequence[str] | None = None,
    leer: Callable[[Sequence[str]], ParametrosEjecucion] = leer_parametros,
    construir: Callable[[ParametrosEjecucion], Contexto] = construir_contexto,
) -> ResultadoJob:
    argumentos = sys.argv[1:] if argv is None else argv
    contexto = construir(leer(argumentos))
    try:
        return contexto.caso_de_uso.ejecutar()
    except ErrorNotificacion as error:
        contexto.bitacora.registrar(EventoLog.ERROR_JOB, nivel="ERROR", **error.como_diccionario())
        raise
    except Exception as error:
        clasificado = ErrorPlataforma(CODIGO_ERROR_NO_CLASIFICADO, str(error))
        contexto.bitacora.registrar(EventoLog.ERROR_JOB, nivel="ERROR", **clasificado.como_diccionario())
        raise
