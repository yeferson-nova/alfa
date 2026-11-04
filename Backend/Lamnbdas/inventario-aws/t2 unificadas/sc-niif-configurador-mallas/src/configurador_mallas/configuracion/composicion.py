"""Raíz de composición: único lugar que conecta los puertos con sus adaptadores.

El adaptador de entrada (handler) pide aquí el caso de uso y el observador; así
no depende de los adaptadores de salida.
"""
import time

from ..adaptadores.salida import cloudwatch
from ..aplicacion.configurar_malla import ConfiguradorMallas
from ..aplicacion.idempotencia import ControlIdempotencia
from ..dominio.errores import ErrorConfiguracion


def crear_logger(nivel):
    return cloudwatch.crear_logger(nivel)


def crear_observador(logger, funcion):
    """Observador sin namespace: se fija cuando se leen los ajustes."""
    return cloudwatch.Observador(logger, None, funcion)


def partes_arn(arn_funcion):
    """(partición, región, cuenta) a partir del ARN de la propia función."""
    partes = (arn_funcion or "").split(":")
    if len(partes) < 7 or partes[0] != "arn" or partes[2] != "lambda":
        raise ErrorConfiguracion("ARN de la función inválido", codigo="CFG-CNF-001",
                                 detalle={"arn": arn_funcion})
    return partes[1], partes[3], partes[4]


def crear_servicio(ajustes, contexto_lambda):
    import boto3  # aquí, para que las pruebas del núcleo no lo necesiten
    from botocore.config import Config

    from ..adaptadores.salida.dynamo import (DynamoCnf, DynamoEjecuciones, DynamoIdempotencia,
                                     DynamoLectura)
    from ..adaptadores.salida.s3 import AlmacenS3, cargar_catalogo
    from ..adaptadores.salida.stepfunctions import OrquestadorStepFunctions

    particion, region, cuenta = partes_arn(contexto_lambda.invoked_function_arn)
    config = Config(region_name=ajustes.region, retries={"mode": "standard"})
    dynamo = boto3.client("dynamodb", config=config)
    almacen = AlmacenS3(boto3.client("s3", config=config))

    return ConfiguradorMallas(
        catalogo=cargar_catalogo(almacen, ajustes.catalogo_uri),
        cnf=DynamoCnf(dynamo, ajustes.cnf_table),
        prs=DynamoLectura(dynamo, ajustes.prs_table),
        programaciones=DynamoLectura(dynamo, ajustes.prg_table),
        almacen=almacen,
        orquestador=OrquestadorStepFunctions(boto3.client("stepfunctions", config=config),
                                             particion, region, cuenta),
        ejecuciones=DynamoEjecuciones(dynamo, ajustes.ejc_table),
        idempotencia=ControlIdempotencia(DynamoIdempotencia(dynamo, ajustes.idempotencia_table),
                                         ajustes.idempotencia_ttl_segundos, time.time),
        reloj=time.time,
    )
