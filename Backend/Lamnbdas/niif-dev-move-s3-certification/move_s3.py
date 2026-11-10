import os
import boto3
import logging
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)


from botocore.exceptions import ClientError

NIIF_CARPETAS = {'Previsional': 'Previsional', 'auxilioFunerario': 'auxiliosFunerario', 'auxiliosFunerario': 'auxiliosFunerario', 'primasArl': 'primasArl', 'primasRentasEndosos': 'primasRentasEndosos', 'reservaMatematicaRvi': 'reservaMatematicaRviArp', 'reservaMatematicaRviArp': 'reservaMatematicaRviArp', 'primasPCR': 'primasPCR'}

def niif_carpeta(use_case_id):
    if use_case_id not in NIIF_CARPETAS:
        raise ValueError(
            "useCaseId sin carpeta autorizada: " + str(use_case_id)
        )
    return NIIF_CARPETAS[use_case_id]

def niif_origen(s3, bucket, use_case_id, name_file):
    oficial = niif_carpeta(use_case_id)
    candidatos = list(dict.fromkeys([
        f"{oficial}/{name_file}",
        f"{use_case_id}/{name_file}",
    ]))
    for key in candidatos:
        try:
            s3.head_object(Bucket=bucket, Key=key)
            return key
        except ClientError as error:
            status = error.response.get(
                "ResponseMetadata", {}
            ).get("HTTPStatusCode")
            if status != 404:
                raise
    raise FileNotFoundError(
        "Archivo no encontrado en rutas oficial/historica: "
        + str(candidatos)
    )

def get_s3():
    return boto3.client(
        "s3",
        region_name=os.environ.get("AWS_REGION", "us-east-1")
    )

def get_dynamodb():
    return boto3.resource(
        "dynamodb",
        region_name=os.environ.get("AWS_REGION", "us-east-1")
    )

REPORT_BUCKET = os.environ.get("REPORT_BUCKET")
NOTIFICATIONS_TABLE = os.environ.get("NOTIFICATIONS_TABLE")

def get_str(img, key):
    if not img:
        return None
    val = img.get(key)
    return val.get("S") if isinstance(val, dict) else None


def object_exists(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False

		
def get_items_by_certcode(dynamodb, cert_code):
    table = dynamodb.Table(NOTIFICATIONS_TABLE)

    response = table.query(
        IndexName="certCode-index",
        KeyConditionExpression=Key("certCode").eq(cert_code)
    )

    return response.get("Items", [])

def move_to_no_certificados(s3, use_case_id, name_file):
    source_key = niif_origen(s3, REPORT_BUCKET, use_case_id, name_file)
    dest_key = f"{niif_carpeta(use_case_id)}/NO-CERTIFICADOS/{name_file}"

    if object_exists(s3, REPORT_BUCKET, dest_key):
        logger.info(f"Archivo ya existe en destino: {dest_key}")
        return

    if not object_exists(s3, REPORT_BUCKET, source_key):
        logger.warning(f"Archivo origen no existe: {source_key}")
        return

    # Copiar
    s3.copy_object(
        Bucket=REPORT_BUCKET,
        CopySource={"Bucket": REPORT_BUCKET, "Key": source_key},
        Key=dest_key
    )

    logger.info(f"Copiado a NO-CERTIFICADOS: {dest_key}")

    # Borrar origen
    s3.delete_object(Bucket=REPORT_BUCKET, Key=source_key)
    logger.info(f"Origen eliminado: {source_key}")

def update_status_no_transport(dynamodb, use_case_id, execution_id):
    table = dynamodb.Table(NOTIFICATIONS_TABLE)

    table.update_item(
        Key={
            "useCaseId": use_case_id,
            "executionId": execution_id
        },
        UpdateExpression="SET #st = :newStatus",
        ConditionExpression="#st <> :currentStatus",
        ExpressionAttributeNames={
            "#st": "status"
        },
        ExpressionAttributeValues={
            ":newStatus": "NO_TRANSPORT",
            ":currentStatus": "NO_TRANSPORT"
        }
    )
    
def process_no_certificate(dynamodb, s3, cert_code, use_case_id):
    items = get_items_by_certcode(dynamodb, cert_code)

    for item in items:
        execution_id = item["executionId"]
        name_file = item.get("nameFile")

        if not name_file:
            logger.warning(f"Registro sin nameFile: {execution_id}")
            continue

        move_to_no_certificados(s3, use_case_id, name_file)
        update_status_no_transport(dynamodb, use_case_id, execution_id)

        logger.info(f"Registro {execution_id} marcado como NO_TRANSPORT")

def lambda_handler(event, context):
    dynamodb = get_dynamodb()
    s3 = get_s3()
    for record in event.get("Records", []):
        try:
            if record.get("eventName") != "MODIFY":
                continue

            new_image = record["dynamodb"].get("NewImage", {})
            old_image = record["dynamodb"].get("OldImage", {})

            new_status = get_str(new_image, "status")
            old_status = get_str(old_image, "status")

            # SOLO transición válida
            if new_status != "NO_CERTIFICATE" or old_status == "NO_CERTIFICATE":
                continue

            use_case_id = get_str(new_image, "useCaseId")
            cert_code = get_str(new_image, "certCode")

            if not use_case_id or not cert_code:
                logger.warning("Faltan useCaseId o certCode")
                continue

            logger.info(
                f"Procesando NO_CERTIFICATE → NO_TRANSPORT "
                f"useCaseId={use_case_id}, certCode={cert_code}"
            )

            process_no_certificate(dynamodb, s3, cert_code, use_case_id)

        except Exception as e:
            logger.exception(f"Error procesando stream: {e}")

    return {"status": "success"}
