"""Punto de entrada de Lambda. Solo traduce evento -> servicio -> respuesta."""
from . import composicion
from .modelos import Evento

_servicio = None  # se crea una vez por contenedor (arranque en frío)


def lambda_handler(event, context):
    global _servicio
    if _servicio is None:
        _servicio = composicion.crear_configurador()
    resultado = _servicio.ejecutar(Evento.desde_dict(event), context.aws_request_id)
    return resultado.to_dict()
