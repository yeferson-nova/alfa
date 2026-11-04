import json
import logging
import os
import re
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.environ["TABLE_NAME"]
SCHEDULE_GROUP = os.environ["SCHEDULE_GROUP"]
SCHEDULER_ROLE_ARN = os.environ["SCHEDULER_ROLE_ARN"]
TARGET_FUNCTION_ARN = os.environ["TARGET_FUNCTION_ARN"]

table = boto3.resource("dynamodb").Table(TABLE_NAME)
scheduler = boto3.client("scheduler")
deserializer = TypeDeserializer()


def deserializar(imagen):
    return {
        clave: deserializer.deserialize(valor)
        for clave, valor in imagen.items()
    }


def ahora_utc():
    return datetime.now(timezone.utc).isoformat()


def sumar_meses(fecha, meses):
    indice = fecha.month - 1 + meses
    anio = fecha.year + indice // 12
    mes = indice % 12 + 1

    dias = [
        31,
        29 if (
            anio % 400 == 0
            or (anio % 4 == 0 and anio % 100 != 0)
        ) else 28,
        31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    ]

    return fecha.replace(
        year=anio,
        month=mes,
        day=min(fecha.day, dias[mes - 1])
    )


def calcular_fechas(item):
    fecha_inicio = datetime.fromisoformat(item["fechaInicio"])
    frecuencia = str(item.get("frecuencia", "")).upper()
    intervalo = int(item.get("intervalo", 1))
    repeticiones = int(item.get("repeticiones", 1))

    if frecuencia != "MES":
        raise ValueError("La POC solo soporta frecuencia MES.")

    if intervalo < 1:
        raise ValueError("intervalo debe ser mayor o igual a 1.")

    if repeticiones < 1 or repeticiones > 100:
        raise ValueError("repeticiones debe estar entre 1 y 100.")

    return [
        sumar_meses(fecha_inicio, numero * intervalo)
        for numero in range(repeticiones)
    ]


def construir_nombre(id_programacion, indice):
    nombre = f"niif-dev-{id_programacion.lower()}-{indice:03d}"
    return re.sub(r"[^a-z0-9-_]", "-", nombre)[:64]


def crear_schedule(item, fecha, indice, total):
    id_programacion = item["idProgramacion"]
    id_automatizacion = f"{id_programacion}-{indice:03d}"
    nombre = construir_nombre(id_programacion, indice)

    payload = {
        "idProgramacion": id_programacion,
        "idAutomatizacion": id_automatizacion,
        "malla": item.get("malla"),
        "periodoNegocio": item.get("periodoNegocio"),
        "tipoEvento": item.get("tipoEvento"),
        "fechaEjecucion": fecha.isoformat()
    }

    parametros = {
        "Name": nombre,
        "GroupName": SCHEDULE_GROUP,
        "ScheduleExpression": (
            f"at({fecha.replace(tzinfo=None).strftime('%Y-%m-%dT%H:%M:%S')})"
        ),
        "ScheduleExpressionTimezone": "America/Bogota",
        "FlexibleTimeWindow": {
            "Mode": "OFF"
        },
        "Target": {
            "Arn": TARGET_FUNCTION_ARN,
            "RoleArn": SCHEDULER_ROLE_ARN,
            "Input": json.dumps(payload, ensure_ascii=False)
        },
        "State": "ENABLED",
        "ActionAfterCompletion": "DELETE",
        "Description": (
            f"NIIF17 {id_programacion} {indice}/{total}"
        )
    }

    try:
        respuesta = scheduler.create_schedule(**parametros)
        arn = respuesta["ScheduleArn"]
        logger.info("Schedule creado: %s", nombre)

    except ClientError as error:
        codigo = error.response.get("Error", {}).get("Code")

        if codigo != "ConflictException":
            raise

        existente = scheduler.get_schedule(
            Name=nombre,
            GroupName=SCHEDULE_GROUP
        )

        arn = existente["Arn"]
        logger.info("Schedule existente validado: %s", nombre)

    return nombre, arn


def guardar_automatizacion(item, fecha, indice, total):
    nombre, arn = crear_schedule(
        item=item,
        fecha=fecha,
        indice=indice,
        total=total
    )

    table.put_item(
        Item={
            "PK": item["PK"],
            "SK": f"AUTOMATIZACION#{indice:03d}",
            "tipoRegistro": "AUTOMATIZACION",
            "idAutomatizacion": (
                f"{item['idProgramacion']}-{indice:03d}"
            ),
            "idProgramacion": item["idProgramacion"],
            "malla": item.get("malla"),
            "periodoNegocio": item.get("periodoNegocio"),
            "tipoEvento": item.get("tipoEvento"),
            "numeroEjecucion": Decimal(indice),
            "totalEjecuciones": Decimal(total),
            "fechaEjecucion": fecha.isoformat(),
            "estado": "PROGRAMADA",
            "schedulerName": nombre,
            "schedulerGroup": SCHEDULE_GROUP,
            "schedulerArn": arn,
            "activo": True,
            "fechaCreacion": ahora_utc()
        }
    )


def actualizar_estado(pk, estado, mensaje):
    table.update_item(
        Key={
            "PK": pk,
            "SK": "METADATA"
        },
        UpdateExpression=(
            "SET #estado = :estado, "
            "fechaProcesamiento = :fecha, "
            "mensajeProcesamiento = :mensaje"
        ),
        ExpressionAttributeNames={
            "#estado": "estado"
        },
        ExpressionAttributeValues={
            ":estado": estado,
            ":fecha": ahora_utc(),
            ":mensaje": mensaje
        }
    )


def procesar(item):
    actualizar_estado(
        item["PK"],
        "GENERANDO_AUTOMATIZACIONES",
        "Calculando fechas y creando schedules"
    )

    fechas = calcular_fechas(item)

    for indice, fecha in enumerate(fechas, start=1):
        guardar_automatizacion(
            item=item,
            fecha=fecha,
            indice=indice,
            total=len(fechas)
        )

    actualizar_estado(
        item["PK"],
        "PROGRAMADA",
        f"{len(fechas)} schedules creados"
    )


def lambda_handler(event, context):
    fallos = []
    procesados = 0
    ignorados = 0

    for record in event.get("Records", []):
        event_id = record.get("eventID", "sin-id")
        item = {}

        try:
            if record.get("eventName") not in ("INSERT", "MODIFY"):
                ignorados += 1
                continue

            imagen = record.get("dynamodb", {}).get("NewImage", {})

            if not imagen:
                ignorados += 1
                continue

            item = deserializar(imagen)

            es_programacion = (
                item.get("PK", "").startswith("PROGRAMACION#")
                and item.get("SK") == "METADATA"
                and item.get("estado") == "PENDIENTE"
            )

            if not es_programacion:
                ignorados += 1
                continue

            procesar(item)
            procesados += 1

        except Exception as error:
            logger.exception("Error en evento %s", event_id)

            fallos.append({
                "itemIdentifier": event_id
            })

            if item.get("PK"):
                try:
                    actualizar_estado(
                        item["PK"],
                        "ERROR_PROGRAMACION",
                        str(error)
                    )
                except Exception:
                    logger.exception(
                        "No fue posible registrar el error."
                    )

    resultado = {
        "procesados": procesados,
        "ignorados": ignorados,
        "fallidos": len(fallos),
        "batchItemFailures": fallos
    }

    logger.info("Resultado: %s", json.dumps(resultado))
    return resultado
