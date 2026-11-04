import json
import os
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

COLOMBIA_TZ = ZoneInfo("America/Bogota")

NOTIFICATIONS_TABLE = os.environ.get("NOTIFICATIONS_TABLE")
APPROVERS_LOG_TABLE = os.environ.get("APPROVERS_LOG_TABLE")

CONFIG_BUCKET = os.environ.get("CONFIG_BUCKET")
GLOBAL_CONFIG_KEY = os.environ.get("GLOBAL_CONFIG_KEY")


# ===================== HELPERS =====================

def _response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, ensure_ascii=False)
    }


def cargar_json_s3(bucket, key):
    logger.info(f"[CONFIG] Leyendo JSON desde s3://{bucket}/{key}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def obtener_config_caso(use_case_id):
    if not CONFIG_BUCKET:
        raise RuntimeError("CONFIG_BUCKET no está configurado en variables de entorno")

    if not GLOBAL_CONFIG_KEY:
        raise RuntimeError("GLOBAL_CONFIG_KEY no está configurado en variables de entorno")

    global_cfg = cargar_json_s3(CONFIG_BUCKET, GLOBAL_CONFIG_KEY)

    use_cases_base = global_cfg["paths"]["use_cases_base"]
    case_key = f"{use_cases_base}/{use_case_id}.json"

    case_cfg = cargar_json_s3(CONFIG_BUCKET, case_key)

    if case_cfg.get("useCaseId") != use_case_id:
        raise RuntimeError(
            f"El useCaseId del JSON ({case_cfg.get('useCaseId')}) "
            f"no coincide con {use_case_id}"
        )

    return case_cfg


def obtener_approvers_desde_json(use_case_id):
    case_cfg = obtener_config_caso(use_case_id)

    approvers_cfg = case_cfg.get("approvers", {})

    approvers = approvers_cfg.get("users", [])
    required = int(approvers_cfg.get("required", 1))

    if not approvers:
        raise RuntimeError(f"No hay approvers.users configurados para {use_case_id}")

    if required < 1:
        required = 1

    if required > len(approvers):
        raise RuntimeError(
            f"approvers.required={required} es mayor que users={len(approvers)} "
            f"para {use_case_id}"
        )

    return approvers, required


def resolve_final_status(cert_status, pending_approvals):
    if pending_approvals and int(pending_approvals) > 0:
        return "PENDING_APPROVERS"
    return cert_status


def build_update_expressions(cert_status, final_status, now_col, user_email):
    expr_attr_names = {"#status": "status"}

    expr_attr_values = {
        ":cd": now_col,
        ":cs": final_status,
        ":ue": user_email,
        ":certificate": "CERTIFICATE"
    }

    update_expression = "SET creationDate = :cd, #status = :cs, userEmail = :ue"

    if cert_status == "NO_CERTIFICATE":
        condition_expression = "attribute_not_exists(#status) OR #status <> :certificate"
    else:
        expr_attr_values[":transport"] = "TRANSPORT"
        condition_expression = (
            "attribute_not_exists(#status) OR "
            "(#status <> :certificate AND #status <> :transport)"
        )

    return update_expression, condition_expression, expr_attr_names, expr_attr_values


# ===================== AUDIT =====================

def process_approval_audit(
    dynamodb_resource,
    APPROVERS_LOG_TABLE,
    use_case_id,
    cert_code,
    email,
    cert_status,
    total_required
):
    now = datetime.now(COLOMBIA_TZ).isoformat()

    log_table = dynamodb_resource.Table(APPROVERS_LOG_TABLE)

    existing = log_table.get_item(
        Key={"useCaseId": use_case_id, "certCode": cert_code}
    ).get("Item")

    # Si no existe auditoría previa
    if not existing:
        if cert_status == "NO_CERTIFICATE":
            pending = 0
            status = "CANCELED"
        else:
            pending = max(total_required - 1, 0)
            status = "CERTIFIED" if pending == 0 else "PENDING"

        log_table.put_item(Item={
            "useCaseId": use_case_id,
            "certCode": cert_code,
            "pendingApprovals": pending,
            "status": status,
            "emails": {
                email: {"decision": cert_status, "decisionDate": now}
            },
            "lastUpdate": now
        })

        return {"pendingApprovals": pending}

    # Si el usuario ya decidió antes, no duplicar
    if email in existing.get("emails", {}):
        return {"pendingApprovals": existing.get("pendingApprovals", 0)}

    # Si alguien no certifica, cancela inmediatamente
    if cert_status == "NO_CERTIFICATE":
        pending = 0
        status = "CANCELED"
    else:
        pending = max(int(existing.get("pendingApprovals", 0)) - 1, 0)
        status = "CERTIFIED" if pending == 0 else "PENDING"

    log_table.update_item(
        Key={"useCaseId": use_case_id, "certCode": cert_code},
        UpdateExpression="SET pendingApprovals=:pa,#st=:st,emails.#em=:em,lastUpdate=:lu",
        ExpressionAttributeNames={"#st": "status", "#em": email},
        ExpressionAttributeValues={
            ":pa": pending,
            ":st": status,
            ":em": {"decision": cert_status, "decisionDate": now},
            ":lu": now
        }
    )

    return {"pendingApprovals": pending}


# ===================== HANDLER =====================

def lambda_handler(event, _context):
    try:
        claims = event["requestContext"]["authorizer"]["claims"]
        logger.info(f"[AUTH] Claims completos: {json.dumps(claims)}")

        user_email = claims.get("email")
        logger.info(f"[AUTH] Email autenticado por Cognito: {user_email}")

        if not user_email:
            return _response(401, {"message": "Usuario no autenticado"})

        path = event.get("path", "")

        cert_status = (
            "CERTIFICATE" if path.endswith("/certificar")
            else "NO_CERTIFICATE" if path.endswith("/no_certificar")
            else None
        )

        if not cert_status:
            return _response(400, {"message": "Ruta no válida"})

        body = json.loads(event.get("body", "{}"))

        use_case_id = body.get("useCaseId")
        cert_code = body.get("certCode")

        if not use_case_id or not cert_code:
            return _response(400, {"message": "Parámetros incompletos"})

        # ===================== NUEVO: APPROVERS DESDE JSON =====================
        approvers, total_required = obtener_approvers_desde_json(use_case_id)

        logger.info(f"[AUTH] Approvers JSON para {use_case_id}: {approvers}")
        logger.info(f"[AUTH] Aprobaciones requeridas: {total_required}")

        if user_email not in approvers:
            return _response(403, {
                "message": "Usuario no autorizado",
                "userEmail": user_email
            })

        table = dynamodb.Table(NOTIFICATIONS_TABLE)

        items = table.query(
            IndexName="certCode",
            KeyConditionExpression=Key("certCode").eq(cert_code)
        ).get("Items", [])

        items_to_update = [
            i for i in items
            if i["useCaseId"] == use_case_id
        ]

        if not items_to_update:
            return _response(404, {"message": "Registros no encontrados"})

        if items_to_update[0].get("status") in (
            "CERTIFICATE",
            "NO_CERTIFICATE",
            "TRANSPORT",
            "NO_TRANSPORT"
        ):
            return _response(409, {"message": "Caso ya procesado"})

        audit_result = process_approval_audit(
            dynamodb,
            APPROVERS_LOG_TABLE,
            use_case_id,
            cert_code,
            user_email,
            cert_status,
            total_required
        )

        final_status = resolve_final_status(
            cert_status,
            audit_result.get("pendingApprovals", 0)
        )

        update_expr, cond_expr, expr_names, expr_vals = build_update_expressions(
            cert_status,
            final_status,
            datetime.now(COLOMBIA_TZ).isoformat(),
            user_email
        )

        for item in items_to_update:
            table.update_item(
                Key={
                    "useCaseId": use_case_id,
                    "executionId": item["executionId"]
                },
                UpdateExpression=update_expr,
                ConditionExpression=cond_expr,
                ExpressionAttributeNames=expr_names,
                ExpressionAttributeValues=expr_vals
            )

        return _response(200, {
            "message": "Actualización correcta",
            "decision": cert_status,
            "finalStatus": final_status,
            "pendingApprovals": audit_result.get("pendingApprovals", 0)
        })

    except Exception as e:
        logger.exception("Error en Lambda")
        return _response(500, {"message": "Error interno", "error": str(e)})