import os
import json
import boto3
from datetime import datetime, timedelta, timezone
import logging

logger = logging.getLogger()
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logger.setLevel(log_level)

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("s3transfer").setLevel(logging.WARNING)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

dynamodb = boto3.resource('dynamodb')

APPROVERS_DYNAMODB_TABLE = os.environ.get("APPROVERS_DYNAMODB_TABLE")
CERTIFICATION_DYNAMODB_TABLE = os.environ.get("CERTIFICATION_DYNAMODB_TABLE")

COLOMBIA_TZ = timezone(timedelta(hours=-5))

def lambda_handler(event, _context):
    try:
        claims = event["requestContext"]["authorizer"]["claims"]
        user_email = claims.get("email")

        if not user_email:
            return _response(401, {"message": "No se encontró el email del usuario autenticado."})

        path = event.get("path", "")
        if path.endswith("/certificar"):
            cert_status = "CERTIFICADO"
        elif path.endswith("/no_certificar"):
            cert_status = "NO_CERTIFICADO"
        else:
            return _response(400, {"message": f"Ruta no válida: {path}"})

        body = json.loads(event.get("body", "{}"))
        use_case_id = body.get("useCaseId")
        period = body.get("period")
        cert_code = body.get("certCode")

        if not use_case_id or not period or not cert_code:
            return _response(400, {"message": "Faltan parámetros obligatorios: useCaseId, period y certCode"})

        approver_table = dynamodb.Table(APPROVERS_DYNAMODB_TABLE)
        approver_item = approver_table.get_item(Key={"useCaseId": use_case_id})
        if "Item" not in approver_item:
            return _response(403, {"message": f"No hay aprobadores registrados para el useCaseId {use_case_id}"})

        approvers = approver_item["Item"].get("emails", [])
        if user_email not in approvers:
            return _response(403, {"message": f"El usuario {user_email} no está autorizado para aprobar este caso."})

        certification_table = dynamodb.Table(CERTIFICATION_DYNAMODB_TABLE)

        # Buscar si existe el registro
        cert_item_response = certification_table.get_item(Key={"useCaseId": use_case_id, "certCode": cert_code})
        if "Item" not in cert_item_response:
            return _response(404, {"message": f"No existe el caso de certificación con certCode {cert_code} para el useCaseId {use_case_id}"})

        cert_item = cert_item_response["Item"]

        # Verificar si ya tiene un certStatus asignado
        existing_cert_status = cert_item.get("certStatus", "")
        if existing_cert_status:
            return _response(409, {
                "message": "Este caso ya fue procesado previamente.",
                "currentStatus": existing_cert_status,
                "processedBy": cert_item.get("userEmail", "Desconocido")
            })

        now_col = datetime.now(COLOMBIA_TZ)
        create_date_str = now_col.isoformat()

        # Actualizar el registro existente
        certification_table.update_item(
            Key={
                "useCaseId": use_case_id,
                "certCode": cert_code
            },
            UpdateExpression="SET creationDate = :cd, certStatus = :cs, userEmail = :ue",
            ExpressionAttributeValues={
                ":cd": create_date_str,
                ":cs": cert_status,
                ":ue": user_email
            }
        )

        return _response(200, {
            "message": "Actualización de certificación realizada correctamente",
            "updatedFields": {
                "creationDate": create_date_str,
                "certStatus": cert_status,
                "userEmail": user_email
            }
        })

    except Exception as e:
        logger.exception("Error en Lambda")
        return _response(500, {"message": "Error interno", "error": str(e)})

def _response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body)
    }
