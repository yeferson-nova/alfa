import json
import logging
import os
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.environ["TABLE_NAME"]
EXPECTED_STEP_ARN = os.environ["STEP_ARN"]

dynamodb = boto3.client("dynamodb")
stepfunctions = boto3.client("stepfunctions")
s3 = boto3.client("s3")

ESTADOS_TERMINALES = {
    "SUCCEEDED",
    "FAILED",
    "TIMED_OUT",
    "ABORTED",
}


def texto_fecha(valor):
    if valor is None:
        return datetime.now(timezone.utc).isoformat()
    if hasattr(valor, "isoformat"):
        return valor.isoformat()
    return str(valor)


def separar_s3_uri(uri):
    parsed = urlparse(uri)

    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"URI S3 invalida: {uri}")

    key = parsed.path.lstrip("/")

    if not key:
        raise ValueError(f"URI S3 sin key: {uri}")

    return parsed.netloc, key


def leer_json_s3(uri):
    bucket, key = separar_s3_uri(uri)

    respuesta = s3.get_object(
        Bucket=bucket,
        Key=key,
    )

    return json.loads(
        respuesta["Body"].read().decode("utf-8")
    )


def obtener_resultado(uri, estado_step):
    if estado_step != "SUCCEEDED":
        return estado_step

    try:
        parametros = leer_json_s3(uri)

        return (
            parametros.get("estado_flujo_extraccion")
            or parametros.get("estado_flujo")
            or parametros.get("estado")
            or "SUCCESS"
        )

    except ClientError as error:
        codigo = error.response.get(
            "Error", {}
        ).get("Code")

        if codigo in {
            "NoSuchKey",
            "AccessDenied",
            "403",
            "404",
        }:
            logger.warning(
                "No se pudieron leer parametros: %s",
                codigo,
            )
            return "SUCCESS"

        raise


def actualizar_ejecucion(
    id_automatizacion,
    estado,
    resultado,
    fecha_fin,
    execution_arn,
    error=None,
    causa=None,
):
    nombres = {
        "#estado": "estado",
    }

    valores = {
        ":estado": {"S": estado},
        ":resultado": {"S": resultado},
        ":fecha": {"S": fecha_fin},
        ":arn": {"S": execution_arn},
    }

    expresion = (
        "SET #estado = :estado, "
        "resultadoFlujo = :resultado, "
        "fechaFinStep = :fecha, "
        "executionArn = :arn"
    )

    if error:
        expresion += ", errorStep = :error"
        valores[":error"] = {"S": str(error)}

    if causa:
        expresion += ", causaStep = :causa"
        valores[":causa"] = {
            "S": str(causa)[:4000]
        }

    if not error and not causa:
        expresion += " REMOVE errorStep, causaStep"

    dynamodb.update_item(
        TableName=TABLE_NAME,
        Key={
            "PK": {
                "S": (
                    "EJC#AUTOMATIZACION#"
                    + id_automatizacion
                )
            },
            "SK": {"S": "EJECUCION"},
        },
        UpdateExpression=expresion,
        ExpressionAttributeNames=nombres,
        ExpressionAttributeValues=valores,
        ConditionExpression=(
            "attribute_exists(PK) "
            "AND attribute_exists(SK)"
        ),
    )


def actualizar_lote(
    id_lote,
    estado,
    resultado,
    fecha_fin,
):
    dynamodb.update_item(
        TableName=TABLE_NAME,
        Key={
            "PK": {
                "S": "EJC#LOTE_CARGUE#" + id_lote
            },
            "SK": {"S": "METADATA"},
        },
        UpdateExpression=(
            "SET #estado = :estado, "
            "resultadoFlujo = :resultado, "
            "fechaHoraFin = :fecha"
        ),
        ExpressionAttributeNames={
            "#estado": "estado",
        },
        ExpressionAttributeValues={
            ":estado": {"S": estado},
            ":resultado": {"S": resultado},
            ":fecha": {"S": fecha_fin},
        },
        ConditionExpression=(
            "attribute_exists(PK) "
            "AND attribute_exists(SK)"
        ),
    )


def lambda_handler(event, context):
    logger.info(
        "Evento recibido: %s",
        json.dumps(event, ensure_ascii=False),
    )

    detail = event.get("detail", {})

    execution_arn = detail.get("executionArn")
    state_machine_arn = detail.get("stateMachineArn")
    event_status = detail.get("status")

    if not execution_arn:
        raise ValueError(
            "Falta detail.executionArn."
        )

    if state_machine_arn != EXPECTED_STEP_ARN:
        raise ValueError(
            "La Step Function no es la esperada."
        )

    if event_status not in ESTADOS_TERMINALES:
        return {
            "estado": "IGNORADO",
            "statusEvento": event_status,
            "executionArn": execution_arn,
        }

    ejecucion = stepfunctions.describe_execution(
        executionArn=execution_arn
    )

    estado_step = ejecucion["status"]

    if estado_step not in ESTADOS_TERMINALES:
        return {
            "estado": "PENDIENTE",
            "statusReal": estado_step,
            "executionArn": execution_arn,
        }

    entrada = json.loads(
        ejecucion.get("input") or "{}"
    )

    id_automatizacion = entrada[
        "idAutomatizacion"
    ]
    id_lote = entrada["idLoteCargue"]
    ruta_parametros = entrada[
        "rutaParametrosExtraccion"
    ]

    fecha_fin = texto_fecha(
        ejecucion.get("stopDate")
    )

    error = ejecucion.get("error")
    causa = ejecucion.get("cause")

    resultado = obtener_resultado(
        ruta_parametros,
        estado_step,
    )

    if estado_step == "SUCCEEDED":
        estado_ejecucion = "EXITOSA"
        estado_lote = "COMPLETADO"
    else:
        estado_ejecucion = "FALLIDA"
        estado_lote = "FALLIDO"

    actualizar_ejecucion(
        id_automatizacion=id_automatizacion,
        estado=estado_ejecucion,
        resultado=resultado,
        fecha_fin=fecha_fin,
        execution_arn=execution_arn,
        error=error,
        causa=causa,
    )

    actualizar_lote(
        id_lote=id_lote,
        estado=estado_lote,
        resultado=resultado,
        fecha_fin=fecha_fin,
    )

    respuesta = {
        "estado": "FINALIZACION_REGISTRADA",
        "statusStep": estado_step,
        "estadoEjecucion": estado_ejecucion,
        "estadoLote": estado_lote,
        "resultadoFlujo": resultado,
        "idAutomatizacion": id_automatizacion,
        "idLoteCargue": id_lote,
        "executionArn": execution_arn,
        "fechaFin": fecha_fin,
    }

    logger.info(
        "Finalizacion: %s",
        json.dumps(respuesta, ensure_ascii=False),
    )

    return respuesta
