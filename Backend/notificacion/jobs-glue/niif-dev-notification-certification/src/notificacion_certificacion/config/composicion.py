import time
from dataclasses import dataclass
from functools import partial
from typing import Any

import boto3
from niif_observability import StructuredLogger

from notificacion_certificacion.adapters.outbound.athena import ConsultorAthena
from notificacion_certificacion.adapters.outbound.bitacora_json import BitacoraJson
from notificacion_certificacion.adapters.outbound.dynamo_compuesto import RepositorioCompuestoDynamo
from notificacion_certificacion.adapters.outbound.dynamo_enlaces import RepositorioEnlacesDynamo
from notificacion_certificacion.adapters.outbound.dynamo_notificaciones import RepositorioNotificacionesDynamo
from notificacion_certificacion.adapters.outbound.reloj import GeneradorTokensUuid, RelojSistema
from notificacion_certificacion.adapters.outbound.s3_configuracion import RepositorioConfiguracionS3
from notificacion_certificacion.adapters.outbound.ses import NotificadorSes
from notificacion_certificacion.application.compuesto import ServicioCompuesto
from notificacion_certificacion.application.consultas import ServicioConsultas
from notificacion_certificacion.application.enlaces import ServicioEnlaces
from notificacion_certificacion.application.notificar_certificacion import Dependencias, NotificarCertificacion
from notificacion_certificacion.application.puertos import Bitacora
from notificacion_certificacion.domain.configuracion import leer_configuracion_global
from notificacion_certificacion.domain.modelos import AjustesNotificacion, ConfiguracionGlobal, ParametrosEjecucion

COMPONENTE = "notificacion_certificacion"


@dataclass(frozen=True)
class Contexto:
    caso_de_uso: NotificarCertificacion
    bitacora: Bitacora
    caso_uso: str = ""


def _consultor(sesion: Any, ajustes: AjustesNotificacion) -> ConsultorAthena:
    return ConsultorAthena(
        sesion.client("athena"),
        time.sleep,
        ajustes.segundos_entre_consultas,
        ajustes.segundos_maximos_consulta,
    )


def _cargar_global(repositorio: RepositorioConfiguracionS3, parametros: ParametrosEjecucion) -> ConfiguracionGlobal:
    return leer_configuracion_global(repositorio.cargar_json(parametros.bucket_configuracion, parametros.clave_global))


def construir_contexto(parametros: ParametrosEjecucion) -> Contexto:
    sesion = boto3.session.Session()
    repositorio = RepositorioConfiguracionS3(sesion.client("s3"))
    configuracion_global = _cargar_global(repositorio, parametros)
    ajustes = configuracion_global.notificacion
    reloj = RelojSistema(configuracion_global.zona_horaria)
    bitacora = BitacoraJson(StructuredLogger(COMPONENTE, partial(print, flush=True), reloj.ahora_utc))
    dynamodb = sesion.client("dynamodb")
    dependencias = Dependencias(
        configuracion=repositorio,
        notificaciones=RepositorioNotificacionesDynamo(dynamodb),
        notificador=NotificadorSes(sesion.client("sesv2"), ajustes.remitente, ajustes.conjunto_configuracion),
        consultas=ServicioConsultas(_consultor(sesion, ajustes), bitacora),
        enlaces=ServicioEnlaces(
            RepositorioEnlacesDynamo(dynamodb),
            reloj,
            GeneradorTokensUuid(),
            bitacora,
            ajustes.enlaces,
            parametros.caso_uso,
            parametros.cert_code,
        ),
        compuesto=ServicioCompuesto(RepositorioCompuestoDynamo(dynamodb), reloj, bitacora),
        reloj=reloj,
        bitacora=bitacora,
    )
    return Contexto(
        caso_de_uso=NotificarCertificacion(parametros, configuracion_global, dependencias),
        bitacora=bitacora,
        caso_uso=parametros.caso_uso,
    )
