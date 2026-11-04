import os
import boto3
import logging

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

REPORT_BUCKET = os.environ.get("REPORT_BUCKET")
NOTIFICATIONS_TABLE = os.environ.get("NOTIFICATIONS_TABLE")


def get_str(img, key):
    if not img:
        return None

    val = img.get(key)

    if isinstance(val, dict):
        return val.get("S")

    return None


def object_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")

        if error_code in ("404", "NoSuchKey", "NotFound"):
            return False

        raise


def get_items_by_certcode(cert_code, use_case_id):
    table = dynamodb.Table(NOTIFICATIONS_TABLE)

    response = table.query(
        IndexName="certCode",
        KeyConditionExpression=Key("certCode").eq(cert_code)
    )

    items = response.get("Items", [])

    return [
        item for item in items
        if item.get("useCaseId") == use_case_id
    ]


def move_to_no_certificados(use_case_id, name_file):
    source_key = f"{use_case_id}/{name_file}"
    dest_key = f"{use_case_id}/NO-CERTIFICADOS/{name_file}"

    if object_exists(REPORT_BUCKET, dest_key):
        logger.info(f"Archivo ya existe en destino: {dest_key}")

        if object_exists(REPORT_BUCKET, source_key):
            s3.delete_object(Bucket=REPORT_BUCKET, Key=source_key)
            logger.info(f"Origen eliminado porque ya existía destino: {source_key}")

        return

    if not object_exists(REPORT_BUCKET, source_key):
        logger.warning(f"Archivo origen no existe: {source_key}")
        return

    s3.copy_object(
        Bucket=REPORT_BUCKET,
        CopySource={
            "Bucket": REPORT_BUCKET,
            "Key": source_key
        },
        Key=dest_key
    )

    logger.info(f"Copiado a NO-CERTIFICADOS: {dest_key}")

    s3.delete_object(
        Bucket=REPORT_BUCKET,
        Key=source_key
    )

    logger.info(f"Origen eliminado: {source_key}")


def update_status_no_transport(use_case_id, execution_id):
    table = dynamodb.Table(NOTIFICATIONS_TABLE)

    try:
        table.update_item(
            Key={
                "useCaseId": use_case_id,
                "executionId": execution_id
            },
            UpdateExpression="SET #st = :newStatus",
            ConditionExpression="#st <> :newStatus",
            ExpressionAttributeNames={
                "#st": "status"
            },
            ExpressionAttributeValues={
                ":newStatus": "NO_TRANSPORT"
            }
        )

        logger.info(f"Registro {execution_id} marcado como NO_TRANSPORT")

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")

        if error_code == "ConditionalCheckFailedException":
            logger.info(
                f"Registro {execution_id} ya estaba en NO_TRANSPORT. "
                "Se ignora de forma idempotente."
            )
            return

        raise


def process_no_certificate(cert_code, use_case_id):
    items = get_items_by_certcode(cert_code, use_case_id)

    if not items:
        logger.warning(
            f"No se encontraron registros para useCaseId={use_case_id}, "
            f"certCode={cert_code}"
        )
        return

    for item in items:
        execution_id = item.get("executionId")
        name_file = item.get("nameFile")
        current_status = item.get("status")

        if not execution_id:
            logger.warning(f"Registro sin executionId: {item}")
            continue

        if not name_file:
            logger.warning(f"Registro sin nameFile: {execution_id}")
            continue

        if current_status == "NO_TRANSPORT":
            logger.info(
                f"Registro {execution_id} ya está en NO_TRANSPORT. "
                "Se valida S3 y se continúa."
            )

        move_to_no_certificados(use_case_id, name_file)
        update_status_no_transport(use_case_id, execution_id)


def lambda_handler(event, context):
    for record in event.get("Records", []):
        try:
            if record.get("eventName") != "MODIFY":
                continue

            new_image = record.get("dynamodb", {}).get("NewImage", {})
            old_image = record.get("dynamodb", {}).get("OldImage", {})

            new_status = get_str(new_image, "status")
            old_status = get_str(old_image, "status")
            query_name = get_str(new_image, "queryName")

            # Solo se procesa la transición hacia NO_CERTIFICATE.
            if new_status != "NO_CERTIFICATE":
                logger.info(f"Estado ignorado: {new_status}")
                continue

            # Evita reprocesar la misma transición.
            if old_status == "NO_CERTIFICATE":
                logger.info(
                    "Evento ignorado porque old_status ya era NO_CERTIFICATE"
                )
                continue

            # Evita doble procesamiento cuando hay varios detail#n.
            # Solo detail#1 dispara el proceso completo por certCode.
            if query_name != "detail#1":
                logger.info(
                    f"Evento NO_CERTIFICATE ignorado para queryName={query_name}. "
                    "Solo procesa detail#1."
                )
                continue

            use_case_id = get_str(new_image, "useCaseId")
            cert_code = get_str(new_image, "certCode")

            if not use_case_id or not cert_code:
                logger.warning("Faltan useCaseId o certCode")
                continue

            logger.info(
                f"Procesando NO_CERTIFICATE -> NO_TRANSPORT "
                f"useCaseId={use_case_id}, certCode={cert_code}"
            )

            process_no_certificate(cert_code, use_case_id)

        except Exception as e:
            logger.exception(f"Error procesando stream: {e}")

    return {
        "status": "success"
    }