import time
from dataclasses import dataclass
from functools import partial

import boto3

from notificacion_certificacion.adaptadores.salida.athena import ConsultorAthena
from notificacion_certificacion.adaptadores.salida.bitacora_json import BitacoraJson
from notificacion_certificacion.adaptadores.salida.dynamo_compuesto import RepositorioCompuestoDynamo
from notificacion_certificacion.adaptadores.salida.dynamo_enlaces import RepositorioEnlacesDynamo
from notificacion_certificacion.adaptadores.salida.dynamo_notificaciones import RepositorioNotificacionesDynamo
from notificacion_certificacion.adaptadores.salida.reloj import GeneradorTokensUuid, RelojSistema
from notificacion_certificacion.adaptadores.salida.s3_configuracion import RepositorioConfiguracionS3
from notificacion_certificacion.adaptadores.salida.ses import NotificadorSes
from notificacion_certificacion.aplicacion.compuesto import ServicioCompuesto
from notificacion_certificacion.aplicacion.consultas import ServicioConsultas
from notificacion_certificacion.aplicacion.enlaces import ServicioEnlaces
from notificacion_certificacion.aplicacion.notificar_certificacion import Dependencias, NotificarCertificacion
from notificacion_certificacion.aplicacion.puertos import Bitacora
from notificacion_certificacion.dominio.modelos import ParametrosEjecucion, PoliticaEnlaces

COMPONENTE = "notificacion_certificacion"


@dataclass(frozen=True)
class Contexto:
    caso_de_uso: NotificarCertificacion
    bitacora: Bitacora


def construir_contexto(parametros: ParametrosEjecucion) -> Contexto:
    sesion = boto3.session.Session()
    reloj = RelojSistema(parametros.zona_horaria)
    bitacora = BitacoraJson(reloj, parametros.id_ejecucion, COMPONENTE, partial(print, flush=True))
    dynamodb = sesion.client("dynamodb")
    politica = PoliticaEnlaces(
        url_base=parametros.url_redireccion,
        tabla=parametros.tabla_enlaces,
        dias_vigencia=parametros.dias_vigencia_enlace,
        longitud_token=parametros.longitud_token_enlace,
    )
    consultor = ConsultorAthena(
        sesion.client("athena"),
        time.sleep,
        parametros.segundos_entre_consultas,
        parametros.segundos_maximos_consulta,
    )
    dependencias = Dependencias(
        configuracion=RepositorioConfiguracionS3(sesion.client("s3")),
        notificaciones=RepositorioNotificacionesDynamo(dynamodb),
        notificador=NotificadorSes(
            sesion.client("sesv2"), parametros.remitente, parametros.conjunto_configuracion
        ),
        consultas=ServicioConsultas(consultor, bitacora),
        enlaces=ServicioEnlaces(
            RepositorioEnlacesDynamo(dynamodb),
            reloj,
            GeneradorTokensUuid(),
            bitacora,
            politica,
            parametros.caso_uso,
            parametros.cert_code,
        ),
        compuesto=ServicioCompuesto(RepositorioCompuestoDynamo(dynamodb), reloj, bitacora),
        reloj=reloj,
        bitacora=bitacora,
    )
    return Contexto(caso_de_uso=NotificarCertificacion(parametros, dependencias), bitacora=bitacora)
