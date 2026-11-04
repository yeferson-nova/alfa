"""Adaptador de entrada (AWS Lambda).

Handler: ``configurador_mallas.adaptadores.entrada.lambda_handler.lambda_handler``.

Solo traduce evento → caso de uso → respuesta, y registra en CloudWatch cualquier
error con su clasificación. Los errores se vuelven a lanzar para que Lambda los
cuente (métrica ``Errors``) y aplique su política de reintentos y destinos.
"""
import time

from ...aplicacion.puertos import Metrica
from ...configuracion import ajustes as config
from ...configuracion import composicion
from ...dominio.errores import ErrorConfigurador, ErrorPlataforma
from ...dominio.modelos import Evento, Invocacion

# Se reutilizan entre invocaciones del mismo contenedor (arranque en frío).
_estado = {"ajustes": None, "servicio": None, "logger": None}
fabrica_servicio = composicion.crear_servicio


def lambda_handler(event, context):
    inicio = time.monotonic()
    observador = composicion.crear_observador(_logger(), context.function_name)
    observador.con_contexto(request_id=context.aws_request_id)
    try:
        ajustes = _ajustes()
        observador.usar_namespace(ajustes.metricas_namespace)
        servicio = _servicio(ajustes, context)
        evento = Evento.desde_dict(event, ajustes.malla_por_defecto)
        resultado = servicio.ejecutar(
            evento,
            Invocacion(context.aws_request_id, context.get_remaining_time_in_millis()),
            observador,
        )
    except ErrorConfigurador as error:
        observador.error(error)
        raise
    except Exception as exc:
        error = ErrorPlataforma("Error no clasificado", codigo="CFG-PLT-000",
                                detalle={"tipo": type(exc).__name__})
        observador.error(error)
        raise error from exc
    finally:
        observador.metrica(Metrica.DURACION, round((time.monotonic() - inicio) * 1000),
                           "Milliseconds")
    return resultado.to_dict()


def _logger():
    if _estado["logger"] is None:
        try:
            nivel = config.nivel_log()
        except ErrorConfigurador:
            nivel = config.NIVEL_LOG_POR_DEFECTO  # el error se informa al leer los ajustes
        _estado["logger"] = composicion.crear_logger(nivel)
    return _estado["logger"]


def _ajustes():
    if _estado["ajustes"] is None:
        _estado["ajustes"] = config.Ajustes.desde_entorno()
    return _estado["ajustes"]


def _servicio(ajustes, context):
    if _estado["servicio"] is None:
        _estado["servicio"] = fabrica_servicio(ajustes, context)
    return _estado["servicio"]


def reiniciar():
    """Olvida el estado del contenedor (solo para pruebas)."""
    _estado.update({"ajustes": None, "servicio": None, "logger": None})
