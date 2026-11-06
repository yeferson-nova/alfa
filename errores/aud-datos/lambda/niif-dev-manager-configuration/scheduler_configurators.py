import json
import logging
import os
from datetime import datetime, timezone

import boto3


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

REGION = os.environ["AWS_REGION"]
ACCOUNT_ID = os.environ["ACCOUNT_ID"]
PRG_TABLE = os.environ["PRG_TABLE"]
SCHEDULER_ROLE_ARN = os.environ["SCHEDULER_ROLE_ARN"]
SCHEDULE_GROUP = os.environ["SCHEDULE_GROUP"]

DYNAMODB = boto3.resource(
    "dynamodb",
    region_name=REGION,
)

SCHEDULER = boto3.client(
    "scheduler",
    region_name=REGION,
)

TABLE = DYNAMODB.Table(PRG_TABLE)

CONFIGURADORES = json.loads(os.environ["CONFIGURADORES_JSON"])


class ProgramacionError(Exception):
    pass


def obtener_programacion(codigo_malla):
    respuesta = TABLE.get_item(
        Key={
            "PK": f"PROGRAMACION#{codigo_malla}",
            "SK": "METADATA",
        },
        ConsistentRead=True,
    )

    item = respuesta.get("Item")

    if not item:
        raise ProgramacionError(
            "No existe programacion para "
            f"{codigo_malla}"
        )

    return item


def validar_programacion(
    codigo_malla,
    item,
):
    if codigo_malla not in CONFIGURADORES:
        raise ProgramacionError(
            f"Malla no soportada: {codigo_malla}"
        )

    requeridos = [
        "scheduleName",
        "scheduleExpression",
        "scheduleTimezone",
        "periodoNegocio",
    ]

    faltantes = [
        campo
        for campo in requeridos
        if not item.get(campo)
    ]

    if faltantes:
        raise ProgramacionError(
            "Faltan campos: "
            + ", ".join(faltantes)
        )

    if (
        item["scheduleExpression"]
        == "PENDIENTE"
    ):
        raise ProgramacionError(
            "scheduleExpression esta pendiente"
        )

    if item["periodoNegocio"] == "PENDIENTE":
        raise ProgramacionError(
            "periodoNegocio esta pendiente"
        )


def construir_target(
    codigo_malla,
    item,
):
    nombre_lambda = CONFIGURADORES[
        codigo_malla
    ]

    arn_lambda = (
        f"arn:aws:lambda:{REGION}:"
        f"{ACCOUNT_ID}:function:"
        f"{nombre_lambda}"
    )

    payload = {
        "codigoMalla": codigo_malla,
        "publicar": True,
        "ejecutar": True,
        "periodoNegocio": str(
            item["periodoNegocio"]
        ),
        "modoEjecucion": item.get(
            "modoEjecucion",
            "NORMAL"
        ),
        "origen": "PROGRAMACION_GESTOR",
    }

    return {
        "Arn": arn_lambda,
        "RoleArn": SCHEDULER_ROLE_ARN,
        "Input": json.dumps(
            payload,
            ensure_ascii=False,
        ),
        "RetryPolicy": {
            "MaximumEventAgeInSeconds": 3600,
            "MaximumRetryAttempts": 2,
        },
    }


def construir_parametros_schedule(
    codigo_malla,
    item,
):
    expresion = item[
        "scheduleExpression"
    ]

    accion_final = (
        "DELETE"
        if expresion.startswith("at(")
        else "NONE"
    )

    return {
        "Name": item["scheduleName"],
        "GroupName": SCHEDULE_GROUP,
        "ScheduleExpression": expresion,
        "ScheduleExpressionTimezone": (
            item["scheduleTimezone"]
        ),
        "FlexibleTimeWindow": {
            "Mode": "OFF",
        },
        "State": str(
            item.get(
                "scheduleState",
                "DISABLED",
            )
        ).upper(),
        "ActionAfterCompletion": accion_final,
        "Description": (
            "Programacion Gestor NIIF "
            f"{codigo_malla}"
        ),
        "Target": construir_target(
            codigo_malla,
            item,
        ),
    }


def existe_schedule(nombre):
    try:
        SCHEDULER.get_schedule(
            Name=nombre,
            GroupName=SCHEDULE_GROUP,
        )

        return True

    except (
        SCHEDULER.exceptions
        .ResourceNotFoundException
    ):
        return False


def actualizar_estado_programacion(
    codigo_malla,
    estado,
):
    TABLE.update_item(
        Key={
            "PK": (
                f"PROGRAMACION#{codigo_malla}"
            ),
            "SK": "METADATA",
        },
        UpdateExpression=(
            "SET scheduleEstado = :estado, "
            "fechaActualizacion = :fecha"
        ),
        ExpressionAttributeValues={
            ":estado": estado,
            ":fecha": datetime.now(
                timezone.utc
            ).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        },
    )


def sincronizar_schedule(
    codigo_malla,
    item,
):
    parametros = (
        construir_parametros_schedule(
            codigo_malla,
            item,
        )
    )

    nombre = parametros["Name"]

    if existe_schedule(nombre):
        SCHEDULER.update_schedule(
            **parametros
        )

        operacion = "ACTUALIZADO"

    else:
        SCHEDULER.create_schedule(
            **parametros
        )

        operacion = "CREADO"

    actualizar_estado_programacion(
        codigo_malla,
        "PROGRAMADO",
    )

    return operacion


def construir_resultado(
    codigo_malla,
    accion,
    item,
):
    return {
        "estado": "VALIDADO",
        "accion": accion,
        "codigoMalla": codigo_malla,
        "programacionPK": (
            f"PROGRAMACION#{codigo_malla}"
        ),
        "scheduleName": item[
            "scheduleName"
        ],
        "scheduleExpression": item[
            "scheduleExpression"
        ],
        "scheduleTimezone": item[
            "scheduleTimezone"
        ],
        "periodoNegocio": str(
            item["periodoNegocio"]
        ),
        "configurador": CONFIGURADORES[
            codigo_malla
        ],
        "activo": bool(
            item.get("activo", False)
        ),
    }


def procesar_evento_directo(
    event,
    context,
):
    codigo_malla = str(
        event.get(
            "codigoMalla",
            "",
        )
    ).strip().upper()

    accion = str(
        event.get(
            "accion",
            "VALIDAR",
        )
    ).strip().upper()

    if not codigo_malla:
        raise ProgramacionError(
            "codigoMalla es obligatorio"
        )

    if accion not in {
        "VALIDAR",
        "SINCRONIZAR",
    }:
        raise ProgramacionError(
            f"Accion no soportada: {accion}"
        )

    item = obtener_programacion(
        codigo_malla
    )

    resultado = construir_resultado(
        codigo_malla,
        accion,
        item,
    )

    if accion == "VALIDAR":
        faltantes = [
            campo
            for campo in [
                "scheduleName",
                "scheduleExpression",
                "scheduleTimezone",
                "periodoNegocio",
            ]
            if not item.get(campo)
        ]

        resultado["camposFaltantes"] = (
            faltantes
        )

        resultado[
            "listaParaSincronizar"
        ] = (
            bool(item.get("activo", False))
            and not faltantes
            and item.get(
                "scheduleExpression"
            ) != "PENDIENTE"
            and item.get(
                "periodoNegocio"
            ) != "PENDIENTE"
        )

    if accion == "SINCRONIZAR":
        validar_programacion(
            codigo_malla,
            item,
        )

        if not item.get("activo", False):
            raise ProgramacionError(
                "La programacion esta inactiva"
            )

        resultado["operacion"] = (
            sincronizar_schedule(
                codigo_malla,
                item,
            )
        )

        resultado["estado"] = (
            "PROGRAMADO"
        )

    LOGGER.info(
        "Resultado: %s",
        json.dumps(
            resultado,
            ensure_ascii=False,
        ),
    )

    return resultado


def deserializar_imagen(imagen):
    from boto3.dynamodb.types import TypeDeserializer

    deserializador = TypeDeserializer()

    return {
        clave: deserializador.deserialize(valor)
        for clave, valor in imagen.items()
    }


def procesar_registro_stream(registro):
    nombre_evento = registro.get(
        "eventName",
        "",
    )

    if nombre_evento not in {
        "INSERT",
        "MODIFY",
    }:
        return {
            "estado": "OMITIDO",
            "motivo": (
                f"Evento no procesable: "
                f"{nombre_evento}"
            ),
        }

    dynamodb = registro.get(
        "dynamodb",
        {},
    )

    imagen = dynamodb.get(
        "NewImage",
        {},
    )

    if not imagen:
        return {
            "estado": "OMITIDO",
            "motivo": "NewImage inexistente",
        }

    item = deserializar_imagen(
        imagen
    )

    codigo_malla = str(
        item.get(
            "codigoMalla",
            "",
        )
    ).strip().upper()

    if not codigo_malla:
        return {
            "estado": "OMITIDO",
            "motivo": "codigoMalla inexistente",
        }

    if not item.get("activo", False):
        return {
            "estado": "OMITIDO",
            "codigoMalla": codigo_malla,
            "motivo": "Programacion inactiva",
        }

    if (
        item.get("scheduleEstado")
        != "PENDIENTE"
    ):
        return {
            "estado": "OMITIDO",
            "codigoMalla": codigo_malla,
            "motivo": (
                "scheduleEstado diferente "
                "de PENDIENTE"
            ),
        }

    validar_programacion(
        codigo_malla,
        item,
    )

    operacion = sincronizar_schedule(
        codigo_malla,
        item,
    )

    return {
        "estado": "PROGRAMADO",
        "codigoMalla": codigo_malla,
        "operacion": operacion,
        "scheduleName": item[
            "scheduleName"
        ],
    }


def procesar_evento_stream(event):
    resultados = []

    for registro in event.get(
        "Records",
        [],
    ):
        resultados.append(
            procesar_registro_stream(
                registro
            )
        )

    respuesta = {
        "estado": "PROCESADO_STREAM",
        "registrosRecibidos": len(
            event.get("Records", [])
        ),
        "resultados": resultados,
    }

    LOGGER.info(
        "Resultado Stream: %s",
        json.dumps(
            respuesta,
            ensure_ascii=False,
        ),
    )

    return respuesta


def lambda_handler(event, context):
    if isinstance(
        event,
        dict,
    ) and isinstance(
        event.get("Records"),
        list,
    ):
        return procesar_evento_stream(
            event
        )

    return procesar_evento_directo(
        event,
        context,
    )

