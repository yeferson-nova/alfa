from dataclasses import dataclass
from typing import Any

from constructores import (
    CONFIG_GLOBAL,
    RESPUESTAS_POR_DEFECTO,
    construir_parametros,
    construir_registro,
    copiar_config_caso,
)
from fakes import (
    BitacoraEnMemoria,
    ConsultorFalso,
    GeneradorSecuencial,
    NotificadorFalso,
    RelojFijo,
    RepositorioCompuestoFalso,
    RepositorioConfiguracionFalso,
    RepositorioEnlacesFalso,
    RepositorioNotificacionesFalso,
)
from notificacion_certificacion.application.compuesto import ServicioCompuesto
from notificacion_certificacion.application.consultas import ServicioConsultas
from notificacion_certificacion.application.enlaces import ServicioEnlaces
from notificacion_certificacion.application.notificar_certificacion import Dependencias, NotificarCertificacion
from notificacion_certificacion.domain.configuracion import leer_configuracion_global


@dataclass
class Escenario:
    caso: NotificarCertificacion
    configuracion: RepositorioConfiguracionFalso
    notificaciones: RepositorioNotificacionesFalso
    enlaces: RepositorioEnlacesFalso
    consultor: ConsultorFalso
    notificador: NotificadorFalso
    compuesto: RepositorioCompuestoFalso
    bitacora: BitacoraEnMemoria
    reloj: RelojFijo


def armar_escenario(
    parametros: Any = None,
    config_caso: Any = None,
    registros: Any = None,
    respuestas: Any = None,
    compuesto: Any = None,
    notificador: Any = None,
    plantillas: Any = None,
    config_global: Any = None,
) -> Escenario:
    parametros = parametros or construir_parametros()
    config_caso = config_caso if config_caso is not None else copiar_config_caso()
    configuracion_global = leer_configuracion_global(config_global if config_global is not None else CONFIG_GLOBAL)
    documentos = {(parametros.bucket_configuracion, parametros.clave_caso): config_caso}
    configuracion = RepositorioConfiguracionFalso(documentos, plantillas)
    notificaciones = RepositorioNotificacionesFalso(
        registros if registros is not None else [construir_registro()]
    )
    enlaces = RepositorioEnlacesFalso()
    consultor = ConsultorFalso(respuestas if respuestas is not None else dict(RESPUESTAS_POR_DEFECTO))
    notificador = notificador or NotificadorFalso()
    repositorio_compuesto = compuesto or RepositorioCompuestoFalso()
    bitacora = BitacoraEnMemoria()
    reloj = RelojFijo()
    politica = configuracion_global.notificacion.enlaces
    dependencias = Dependencias(
        configuracion=configuracion,
        notificaciones=notificaciones,
        notificador=notificador,
        consultas=ServicioConsultas(consultor, bitacora),
        enlaces=ServicioEnlaces(
            enlaces, reloj, GeneradorSecuencial(), bitacora, politica, parametros.caso_uso, parametros.cert_code
        ),
        compuesto=ServicioCompuesto(repositorio_compuesto, reloj, bitacora),
        reloj=reloj,
        bitacora=bitacora,
    )
    return Escenario(
        caso=NotificarCertificacion(parametros, configuracion_global, dependencias),
        configuracion=configuracion,
        notificaciones=notificaciones,
        enlaces=enlaces,
        consultor=consultor,
        notificador=notificador,
        compuesto=repositorio_compuesto,
        bitacora=bitacora,
        reloj=reloj,
    )
