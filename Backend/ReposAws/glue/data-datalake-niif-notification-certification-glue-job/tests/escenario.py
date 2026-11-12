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
from notificacion_certificacion.aplicacion.compuesto import ServicioCompuesto
from notificacion_certificacion.aplicacion.consultas import ServicioConsultas
from notificacion_certificacion.aplicacion.enlaces import ServicioEnlaces
from notificacion_certificacion.aplicacion.notificar_certificacion import Dependencias, NotificarCertificacion
from notificacion_certificacion.dominio.modelos import PoliticaEnlaces


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
) -> Escenario:
    parametros = parametros or construir_parametros()
    config_caso = config_caso if config_caso is not None else copiar_config_caso()
    documentos = {
        (parametros.bucket_configuracion, parametros.clave_global): CONFIG_GLOBAL,
        (parametros.bucket_configuracion, parametros.clave_caso): config_caso,
    }
    bucket_activos = CONFIG_GLOBAL["buckets"]["assets"]
    documentos.update({(bucket_activos, clave): texto for clave, texto in (plantillas or {}).items()})
    configuracion = RepositorioConfiguracionFalso(documentos)
    notificaciones = RepositorioNotificacionesFalso(
        registros if registros is not None else [construir_registro()]
    )
    enlaces = RepositorioEnlacesFalso()
    consultor = ConsultorFalso(respuestas if respuestas is not None else dict(RESPUESTAS_POR_DEFECTO))
    notificador = notificador or NotificadorFalso()
    repositorio_compuesto = compuesto or RepositorioCompuestoFalso()
    bitacora = BitacoraEnMemoria()
    reloj = RelojFijo()
    politica = PoliticaEnlaces(
        url_base=parametros.url_redireccion,
        tabla=parametros.tabla_enlaces,
        dias_vigencia=parametros.dias_vigencia_enlace,
        longitud_token=parametros.longitud_token_enlace,
    )
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
        caso=NotificarCertificacion(parametros, dependencias),
        configuracion=configuracion,
        notificaciones=notificaciones,
        enlaces=enlaces,
        consultor=consultor,
        notificador=notificador,
        compuesto=repositorio_compuesto,
        bitacora=bitacora,
        reloj=reloj,
    )
