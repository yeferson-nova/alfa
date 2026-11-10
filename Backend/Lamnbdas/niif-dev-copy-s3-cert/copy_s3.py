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
TRANSPORT_BUCKET = os.environ.get("TRANSPORT_BUCKET")
NOTIFICATIONS_TABLE = os.environ.get("NOTIFICATIONS_TABLE")


def get_str(img, key):
    if not img:
        return None
    val = img.get(key)
    return val.get("S") if isinstance(val, dict) else None


def get_items_by_certcode(dynamodb, table_name, cert_code):
    table = dynamodb.Table(table_name)

    response = table.query(
        IndexName="certCode-index",
        KeyConditionExpression=Key("certCode").eq(cert_code)
    )

    return response.get("Items", [])


def copy_s3_file(s3, source_bucket, source_key, dest_bucket, dest_key):
    logger.info(f"Copiando s3://{source_bucket}/{source_key} → s3://{dest_bucket}/{dest_key}")

    s3.copy_object(
        Bucket=dest_bucket,
        CopySource={"Bucket": source_bucket, "Key": source_key},
        Key=dest_key
    )


def update_status_transport(dynamodb, table_name, use_case_id, execution_id):
    table = dynamodb.Table(table_name)

    table.update_item(
        Key={
            "useCaseId": use_case_id,
            "executionId": execution_id
        },
        UpdateExpression="SET #st = :newStatus",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={":newStatus": "TRANSPORT"}
    )


def process_certcode(dynamodb, s3, cert_code, use_case_id):
    items = get_items_by_certcode(dynamodb, NOTIFICATIONS_TABLE, cert_code)

    for item in items:
        execution_id = item["executionId"]
        name_file = item.get("nameFile")

        if not name_file:
            logger.warning(f"Registro sin nameFile: {execution_id}")
            continue

        source_key = niif_origen(s3, REPORT_BUCKET, use_case_id, name_file)
        dest_key = f"export/{niif_carpeta(use_case_id)}/{name_file}"

        copy_s3_file(
            s3,
            REPORT_BUCKET,
            source_key,
            TRANSPORT_BUCKET,
            dest_key
        )

        update_status_transport(
            dynamodb,
            NOTIFICATIONS_TABLE,
            use_case_id,
            execution_id
        )

        logger.info(f"Archivo {name_file} procesado y marcado como TRANSPORT")


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

            # Solo transición CERTIFICATE
            if new_status != "CERTIFICATE" or old_status == "CERTIFICATE":
                continue

            use_case_id = get_str(new_image, "useCaseId")
            cert_code = get_str(new_image, "certCode")

            if not use_case_id or not cert_code:
                logger.warning("Faltan useCaseId o certCode")
                continue

            logger.info(f"Procesando CERTIFICATE → TRANSPORT para {use_case_id} / {cert_code}")

            process_certcode(dynamodb, s3, cert_code, use_case_id)

        except Exception as e:
            logger.exception(f"Error procesando stream: {e}")

    return {"status": "success"}