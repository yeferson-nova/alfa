import json
import logging
import os
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.environ["TABLE_NAME"]
table = boto3.resource("dynamodb").Table(TABLE_NAME)

dynamodb = boto3.client("dynamodb")
stepfunctions = boto3.client("stepfunctions")
serializer = TypeSerializer()
deserializer = TypeDeserializer()


def ahora_utc():
    return datetime.now(timezone.utc).isoformat()


def serializar_item(item):
    return {
        clave: serializer.serialize(valor)
        for clave, valor in item.items()
        if valor is not None
    }


def deserializar_item(item):
    return {
        clave: deserializer.deserialize(valor)
        for clave, valor in item.items()
    }


def validar_identificador(valor, campo):
    if not valor:
        raise ValueError(f"{campo} es obligatorio.")

    if not re.fullmatch(r"[A-Za-z0-9._#-]+", valor):
        raise ValueError(
            f"{campo} contiene caracteres no permitidos."
        )


def obtener_item(pk, sk):
    respuesta = dynamodb.get_item(
        TableName=TABLE_NAME,
        Key={
            "PK": {"S": pk},
            "SK": {"S": sk}
        },
        ConsistentRead=True
    )

    item = respuesta.get("Item")

    if not item:
        return None

    return deserializar_item(item)


def obtener_extraccion(id_automatizacion):
    pk = f"EJC#AUTOMATIZACION#{id_automatizacion}"

    extraccion = obtener_item(
        pk=pk,
        sk="EXTRACCION"
    )

    if not extraccion:
        raise ValueError(
            "No existe EJC.EXTRACCION para la automatizacion "
            f"{id_automatizacion}."
        )

    if extraccion.get("estado") != "CONFIGURACION_GENERADA":
        raise ValueError(
            "La extraccion no esta lista para ejecucion. "
            f"Estado actual: {extraccion.get('estado')}"
        )

    if not extraccion.get("rutaConfigExtraccion"):
        raise ValueError(
            "La extraccion no tiene rutaConfigExtraccion."
        )

    if not extraccion.get("rutaConfigValidacion"):
        raise ValueError(
            "La extraccion no tiene rutaConfigValidacion."
        )

    return extraccion


def identificadores(id_automatizacion):
    id_lote = f"LOTE#{id_automatizacion}"
    id_ejecucion = f"EJ#{id_automatizacion}"

    return id_lote, id_ejecucion


def construir_registros(extraccion):
    id_automatizacion = extraccion["idAutomatizacion"]
    id_lote, id_ejecucion = identificadores(
        id_automatizacion
    )

    fecha_hora = ahora_utc()
    fecha = fecha_hora[:10]
    hora = fecha_hora[11:19]

    lote = {
        "PK": f"EJC#LOTE_CARGUE#{id_lote}",
        "SK": "METADATA",
        "tipoRegistro": "EJC_LOTE_CARGUE",
        "idLoteCargue": id_lote,
        "idAutomatizacion": id_automatizacion,
        "idExtraccion": extraccion["idExtraccion"],
        "fecha": fecha,
        "hora": hora,
        "fechaHora": fecha_hora,
        "estado": "CREADO",
        "activo": True
    }

    ejecucion = {
        "PK": f"EJC#AUTOMATIZACION#{id_automatizacion}",
        "SK": "EJECUCION",
        "tipoRegistro": "EJC_EJECUCION",
        "idEjecucion": id_ejecucion,
        "idAutomatizacion": id_automatizacion,
        "idProgramacion": extraccion.get(
            "idProgramacion"
        ),
        "idExtraccion": extraccion["idExtraccion"],
        "idLoteCargue": id_lote,
        "malla": extraccion["malla"],
        "periodoNegocio": extraccion["periodoNegocio"],
        "tipoEvento": extraccion["tipoEvento"],
        "modoEjecucion": extraccion["modoEjecucion"],
        "estado": "LISTA_PARA_EJECUTAR",
        "rutaConfigExtraccion": (
            extraccion["rutaConfigExtraccion"]
        ),
        "rutaConfigValidacion": (
            extraccion["rutaConfigValidacion"]
        ),
        "rutaParametrosExtraccion": (
            extraccion["rutaParametrosExtraccion"]
        ),
        "fechaHoraInicio": fecha_hora,
        "activo": True
    }

    return lote, ejecucion


def registrar_transaccion(lote, ejecucion):
    token = ejecucion["idAutomatizacion"][:36]

    try:
        dynamodb.transact_write_items(
            ClientRequestToken=token,
            TransactItems=[
                {
                    "Put": {
                        "TableName": TABLE_NAME,
                        "Item": serializar_item(lote),
                        "ConditionExpression": (
                            "attribute_not_exists(PK) "
                            "AND attribute_not_exists(SK)"
                        )
                    }
                },
                {
                    "Put": {
                        "TableName": TABLE_NAME,
                        "Item": serializar_item(ejecucion),
                        "ConditionExpression": (
                            "attribute_not_exists(PK) "
                            "AND attribute_not_exists(SK)"
                        )
                    }
                }
            ]
        )

        logger.info(
            "Ejecucion y lote registrados: %s / %s",
            ejecucion["idEjecucion"],
            lote["idLoteCargue"]
        )

        return lote, ejecucion, False

    except ClientError as error:
        codigo = error.response.get("Error", {}).get("Code")

        if codigo not in (
            "TransactionCanceledException",
            "IdempotentParameterMismatchException"
        ):
            raise

        existente_ejecucion = obtener_item(
            pk=ejecucion["PK"],
            sk=ejecucion["SK"]
        )

        existente_lote = obtener_item(
            pk=lote["PK"],
            sk=lote["SK"]
        )

        if not existente_ejecucion or not existente_lote:
            raise

        if (
            existente_ejecucion.get("idExtraccion")
            != ejecucion["idExtraccion"]
            or existente_ejecucion.get("idLoteCargue")
            != lote["idLoteCargue"]
        ):
            raise ValueError(
                "La automatizacion ya tiene una ejecucion "
                "diferente."
            )

        logger.info(
            "Ejecucion existente validada: %s",
            existente_ejecucion["idEjecucion"]
        )

        return (
            existente_lote,
            existente_ejecucion,
            True
        )



def separar_s3_uri(uri):
    parsed = urlparse(uri)

    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(
            f"rutaParametrosExtraccion no es una URI S3 valida: {uri}"
        )

    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    if not key:
        raise ValueError(
            "rutaParametrosExtraccion no contiene una key S3."
        )

    return bucket, key



def construir_nombre_ejecucion(id_automatizacion):
    nombre = re.sub(
        r"[^A-Za-z0-9_-]",
        "-",
        f"gestor-{id_automatizacion.lower()}"
    )

    return nombre[:80]



def resolver_step_function_arn(malla, version=1):
    codigo_malla = str(malla).strip().upper()
    codigo_malla = re.sub(
        r"[^A-Z0-9]+",
        "_",
        codigo_malla
    ).strip("_")

    clave = {
        "PK": f"CNF#MALLA#{codigo_malla}",
        "SK": f"VERSION#{version:03d}"
    }

    respuesta = table.get_item(
        Key=clave,
        ConsistentRead=True
    )

    catalogo = respuesta.get("Item")

    if catalogo:
        if catalogo.get("estado") != "ACTIVA":
            raise ValueError(
                f"Catálogo inactivo para {codigo_malla}"
            )

        step_arn = catalogo.get("stepFunctionArn")

        if not step_arn:
            raise ValueError(
                f"Falta stepFunctionArn para {codigo_malla}"
            )

        logger.info(
            "Step resuelta desde catálogo: %s",
            step_arn
        )

        return step_arn, "CATALOGO"

    raise ValueError(
        "No existe catalogo tecnico activo "
        f"para {codigo_malla} VERSION#{version:03d}"
    )


def iniciar_step(
    ejecucion,
    parametros_bucket,
    parametros_key,
    step_function_arn
):
    nombre = construir_nombre_ejecucion(
        ejecucion["idAutomatizacion"]
    )

    input_step = {
        "estado": ejecucion["estado"],
        "idProgramacion": ejecucion.get("idProgramacion"),
        "idAutomatizacion": ejecucion["idAutomatizacion"],
        "idExtraccion": ejecucion["idExtraccion"],
        "idEjecucion": ejecucion["idEjecucion"],
        "idLoteCargue": ejecucion["idLoteCargue"],
        "malla": ejecucion["malla"],
        "periodoNegocio": ejecucion["periodoNegocio"],
        "tipoEvento": ejecucion["tipoEvento"],
        "modoEjecucion": ejecucion["modoEjecucion"],
        "rutaConfigExtraccion": (
            ejecucion["rutaConfigExtraccion"]
        ),
        "rutaConfigValidacion": (
            ejecucion["rutaConfigValidacion"]
        ),
        "rutaParametrosExtraccion": (
            ejecucion["rutaParametrosExtraccion"]
        ),
        "parametrosBucket": parametros_bucket,
        "parametrosKey": parametros_key
    }

    try:
        respuesta = stepfunctions.start_execution(
            stateMachineArn=step_function_arn,
            name=nombre,
            input=json.dumps(
                input_step,
                ensure_ascii=False
            )
        )

        execution_arn = respuesta["executionArn"]
        existente = False

    except ClientError as error:
        codigo = error.response.get("Error", {}).get("Code")

        if codigo != "ExecutionAlreadyExists":
            raise

        execution_arn = (
            step_function_arn.replace(
                ":stateMachine:",
                ":execution:"
            )
            + ":"
            + nombre
        )
        existente = True

    try:
        dynamodb.update_item(
            TableName=TABLE_NAME,
            Key={
                "PK": {"S": ejecucion["PK"]},
                "SK": {"S": ejecucion["SK"]}
            },
            UpdateExpression=(
                "SET #estado = :estado, "
                "executionArn = :arn, "
                "nombreEjecucionStep = :nombre, "
                "fechaInicioStep = if_not_exists("
                "fechaInicioStep, :fecha)"
            ),
            ExpressionAttributeNames={
                "#estado": "estado"
            },
            ExpressionAttributeValues={
                ":estado": {"S": "EN_EJECUCION"},
                ":arn": {"S": execution_arn},
                ":nombre": {"S": nombre},
                ":fecha": {"S": ahora_utc()},
                ":exitosa": {"S": "EXITOSA"},
                ":fallida": {"S": "FALLIDA"}
            },
            ConditionExpression=(
                "attribute_exists(PK) "
                "AND attribute_exists(SK) "
                "AND #estado <> :exitosa "
                "AND #estado <> :fallida"
            )
        )
    except ClientError as error:
        codigo_error = error.response.get(
            "Error",
            {}
        ).get("Code")

        if codigo_error != "ConditionalCheckFailedException":
            raise

        logger.info(
            "Estado terminal protegido para %s. "
            "No se revierte a EN_EJECUCION.",
            ejecucion["idEjecucion"]
        )

    return execution_arn, nombre, existente


def lambda_handler(event, context):
    logger.info(
        "Entrada: %s",
        json.dumps(event, ensure_ascii=False)
    )

    id_automatizacion = event["idAutomatizacion"]
    validar_identificador(
        id_automatizacion,
        "idAutomatizacion"
    )

    extraccion = obtener_extraccion(
        id_automatizacion
    )

    step_function_arn, origen_step = (
        resolver_step_function_arn(
            extraccion["malla"]
        )
    )

    lote, ejecucion = construir_registros(
        extraccion
    )

    lote, ejecucion, ejecucion_existente = (
        registrar_transaccion(
            lote=lote,
            ejecucion=ejecucion
        )
    )

    parametros_bucket, parametros_key = separar_s3_uri(
        ejecucion["rutaParametrosExtraccion"]
    )

    execution_arn, nombre_step, step_existente = iniciar_step(
        ejecucion=ejecucion,
        parametros_bucket=parametros_bucket,
        parametros_key=parametros_key,
        step_function_arn=step_function_arn
    )

    resultado = {
        "estado": "EN_EJECUCION",
        "ejecucionExistente": ejecucion_existente,
        "idProgramacion": ejecucion.get(
            "idProgramacion"
        ),
        "idAutomatizacion": id_automatizacion,
        "idExtraccion": ejecucion["idExtraccion"],
        "idEjecucion": ejecucion["idEjecucion"],
        "idLoteCargue": ejecucion["idLoteCargue"],
        "malla": ejecucion["malla"],
        "periodoNegocio": ejecucion[
            "periodoNegocio"
        ],
        "tipoEvento": ejecucion["tipoEvento"],
        "modoEjecucion": ejecucion["modoEjecucion"],
        "rutaConfigExtraccion": ejecucion[
            "rutaConfigExtraccion"
        ],
        "rutaConfigValidacion": ejecucion[
            "rutaConfigValidacion"
        ],
        "rutaParametrosExtraccion": ejecucion[
            "rutaParametrosExtraccion"
        ],
        "parametrosBucket": parametros_bucket,
        "parametrosKey": parametros_key,
        "stepFunctionIniciada": True,
        "stepFunctionArn": step_function_arn,
        "origenStepFunction": origen_step,
        "stepFunctionExistente": step_existente,
        "nombreEjecucionStep": nombre_step,
        "executionArn": execution_arn
    }

    logger.info(
        "Resultado: %s",
        json.dumps(resultado, ensure_ascii=False)
    )

    return resultado
