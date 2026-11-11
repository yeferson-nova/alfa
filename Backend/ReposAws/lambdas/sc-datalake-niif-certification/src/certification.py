import json
import os
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key


logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

COLOMBIA_TZ = ZoneInfo("America/Bogota")


# ============================================================
# VARIABLES DE ENTORNO
# ============================================================

NOTIFICATIONS_TABLE = os.environ.get(
    "NOTIFICATIONS_TABLE",
    "niif-qa-notification-certification"
)

APPROVERS_LOG_TABLE = os.environ.get(
    "APPROVERS_LOG_TABLE",
    "niif-qa-approval-audit-log"
)

CONFIG_BUCKET = os.environ.get(
    "CONFIG_BUCKET",
    "niif-qa-assets"
)

GLOBAL_CONFIG_KEY = os.environ.get(
    "GLOBAL_CONFIG_KEY",
    "configs/certification/json_global_notif_cert.json"
)

CONFIG_KEY = os.environ.get(
    "CONFIG_KEY",
    "primas-arl/configs/certification/primasArl.json"
)


# ============================================================
# HELPERS
# ============================================================

def formatear_fecha_sin_microsegundos(fecha):
    """
    Convierte fechas tipo:
    2026-06-05T16:26:25.227409-05:00
    a:
    2026-06-05T16:26:25
    """
    if not fecha:
        return "No disponible"

    fecha = str(fecha)

    if "." in fecha:
        fecha = fecha.split(".", 1)[0]

    if len(fecha) >= 19:
        return fecha[:19]

    return fecha


def json_default(obj):
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST"
        },
        "body": json.dumps(body, ensure_ascii=False, default=json_default)
    }


def normalizar_s3_key(bucket, key):
    key = str(key or "").strip()

    prefix = f"s3://{bucket}/"

    if key.startswith(prefix):
        key = key.replace(prefix, "", 1)

    if key.startswith("s3://"):
        raise ValueError(
            f"La ruta S3 no pertenece al bucket configurado. "
            f"bucket={bucket}, key={key}"
        )

    return key.lstrip("/")


def cargar_json_s3(bucket, key):
    key = normalizar_s3_key(bucket, key)

    logger.info(f"[CONFIG] Leyendo JSON desde s3://{bucket}/{key}")

    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def obtener_config_caso(use_case_id, config_bucket=None, config_key=None):
    bucket = config_bucket or CONFIG_BUCKET

    if not bucket:
        raise RuntimeError("CONFIG_BUCKET no está configurado")

    if not GLOBAL_CONFIG_KEY:
        raise RuntimeError("GLOBAL_CONFIG_KEY no está configurado")

    global_cfg = cargar_json_s3(bucket, GLOBAL_CONFIG_KEY)

    # ========================================================
    # Nuevo modelo:
    # Si Dynamo trae configKey/configBucket, se usa ese JSON.
    # Si no viene configKey, queda fallback legacy.
    # ========================================================
    if config_key:
        case_cfg = cargar_json_s3(bucket, config_key)
    elif CONFIG_KEY:
        case_cfg = cargar_json_s3(bucket, CONFIG_KEY)
    else:
        use_cases_base = global_cfg["paths"]["use_cases_base"]
        case_key = f"{use_cases_base}/{use_case_id}.json"
        case_cfg = cargar_json_s3(bucket, case_key)

    if case_cfg.get("useCaseId") != use_case_id:
        raise RuntimeError(
            f"El useCaseId del JSON ({case_cfg.get('useCaseId')}) "
            f"no coincide con {use_case_id}"
        )

    return case_cfg


def obtener_approvers_desde_json(use_case_id, config_bucket=None, config_key=None):
    case_cfg = obtener_config_caso(use_case_id, config_bucket, config_key)

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
    expr_attr_names = {
        "#status": "status"
    }

    expr_attr_values = {
        ":cd": now_col,
        ":cs": final_status,
        ":ue": user_email,
        ":certificate": "CERTIFICATE"
    }

    update_expression = "SET certificationDate = :cd, #status = :cs, userEmail = :ue"

    if cert_status == "NO_CERTIFICATE":
        condition_expression = (
            "attribute_not_exists(#status) OR #status <> :certificate"
        )
    else:
        expr_attr_values[":transport"] = "TRANSPORT"
        condition_expression = (
            "attribute_not_exists(#status) OR "
            "(#status <> :certificate AND #status <> :transport)"
        )

    return update_expression, condition_expression, expr_attr_names, expr_attr_values


# ============================================================
# AUDITORÍA APPROVERS
# ============================================================

def process_approval_audit(
    dynamodb_resource,
    approvers_log_table_name,
    use_case_id,
    cert_code,
    email,
    cert_status,
    total_required
):
    now = datetime.now(COLOMBIA_TZ).isoformat()

    log_table = dynamodb_resource.Table(approvers_log_table_name)

    existing = log_table.get_item(
        Key={
            "useCaseId": use_case_id,
            "certCode": cert_code
        }
    ).get("Item")

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
                email: {
                    "decision": cert_status,
                    "decisionDate": now
                }
            },
            "lastUpdate": now
        })

        return {
            "pendingApprovals": pending
        }

    if email in existing.get("emails", {}):
        existing_decision_obj = existing.get("emails", {}).get(email, {}) or {}
        existing_decision = existing_decision_obj.get("decision")
        existing_decision_date = existing_decision_obj.get("decisionDate")

        return {
            "alreadyVoted": True,
            "existingDecision": existing_decision,
            "existingDecisionDate": existing_decision_date,
            "pendingApprovals": existing.get("pendingApprovals", 0),
            "auditStatus": existing.get("status")
        }

    if cert_status == "NO_CERTIFICATE":
        pending = 0
        status = "CANCELED"
    else:
        pending = max(int(existing.get("pendingApprovals", 0)) - 1, 0)
        status = "CERTIFIED" if pending == 0 else "PENDING"

    log_table.update_item(
        Key={
            "useCaseId": use_case_id,
            "certCode": cert_code
        },
        UpdateExpression="SET pendingApprovals=:pa, #st=:st, emails.#em=:em, lastUpdate=:lu",
        ExpressionAttributeNames={
            "#st": "status",
            "#em": email
        },
        ExpressionAttributeValues={
            ":pa": pending,
            ":st": status,
            ":em": {
                "decision": cert_status,
                "decisionDate": now
            },
            ":lu": now
        }
    )

    return {
        "pendingApprovals": pending
    }


# ============================================================
# HANDLER
# ============================================================

def lambda_handler(event, _context):
    try:
        logger.info("[EVENT] %s", json.dumps(event, default=str))

        claims = event.get("requestContext", {}).get("authorizer", {}).get("claims", {})
        logger.info("[AUTH] Claims completos: %s", json.dumps(claims, default=str))

        user_email = claims.get("email")
        logger.info("[AUTH] Email autenticado por Cognito: %s", user_email)

        if not user_email:
            return _response(401, {
                "message": "Usuario no autenticado"
            })

        path = event.get("path", "")

        cert_status = (
            "CERTIFICATE" if path.endswith("/certify") or path.endswith("/certificar")
            else "NO_CERTIFICATE" if path.endswith("/not_certify") or path.endswith("/no_certificar")
            else None
        )

        if not cert_status:
            return _response(400, {
                "message": "Ruta no válida",
                "path": path
            })

        body_raw = event.get("body") or "{}"

        if event.get("isBase64Encoded"):
            import base64
            body_raw = base64.b64decode(body_raw).decode("utf-8")

        body = json.loads(body_raw)

        use_case_id = body.get("useCaseId")
        cert_code = body.get("certCode")
        period = body.get("period")

        logger.info(
            "[REQUEST] useCaseId=%s, certCode=%s, period=%s, decision=%s",
            use_case_id,
            cert_code,
            period,
            cert_status
        )

        if not use_case_id or not cert_code:
            return _response(400, {
                "message": "Parámetros incompletos",
                "required": [
                    "useCaseId",
                    "certCode"
                ]
            })

        table = dynamodb.Table(NOTIFICATIONS_TABLE)

        items = table.query(
            IndexName="certCode-index",
            KeyConditionExpression=Key("certCode").eq(cert_code)
        ).get("Items", [])

        items_to_update = [
            item for item in items
            if item.get("useCaseId") == use_case_id
        ]

        logger.info(
            "[DYNAMO] Registros encontrados para certCode=%s, useCaseId=%s: %s",
            cert_code,
            use_case_id,
            len(items_to_update)
        )

        if not items_to_update:
            return _response(404, {
                "message": "Registros no encontrados",
                "useCaseId": use_case_id,
                "certCode": cert_code
            })

        notification_item = items_to_update[0]
        config_bucket = notification_item.get("configBucket") or CONFIG_BUCKET
        config_key = notification_item.get("configKey") or CONFIG_KEY

        logger.info(
            "[CONFIG] JSON asociado a certificación: bucket=%s, key=%s",
            config_bucket,
            config_key
        )

        approvers, total_required = obtener_approvers_desde_json(
            use_case_id,
            config_bucket=config_bucket,
            config_key=config_key
        )

        logger.info("[AUTH] Approvers JSON para %s: %s", use_case_id, approvers)
        logger.info("[AUTH] Aprobaciones requeridas: %s", total_required)

        if user_email not in approvers:
            return _response(403, {
                "message": "Usuario no autorizado",
            })

        current_item = items_to_update[0]
        current_status = current_item.get("status")

        if current_status in (
            "CERTIFICATE",
            "NO_CERTIFICATE",
            "TRANSPORT",
            "NO_TRANSPORT"
        ):
            processed_by = (
                current_item.get("processedBy")
                or current_item.get("decisionBy")
                or current_item.get("approvedBy")
                or current_item.get("certifiedBy")
                or current_item.get("lastModifiedBy")
                or current_item.get("userEmail")
                or "No disponible"
            )

            processed_date = (
                current_item.get("processedDate")
                or current_item.get("decisionDate")
                or current_item.get("approvedDate")
                or current_item.get("certifiedDate")
                or current_item.get("lastModifiedDate")
                or current_item.get("sentDate")
                or "No disponible"
            )

            processed_date = formatear_fecha_sin_microsegundos(processed_date)

            return _response(409, {
                "message": "Caso ya procesado anteriormente",
                "status": current_status,
                "processedBy": processed_by,
                "processedDate": processed_date,
                "useCaseId": use_case_id,
                "certCode": cert_code
            })

        audit_result = process_approval_audit(
            dynamodb,
            APPROVERS_LOG_TABLE,
            use_case_id,
            cert_code,
            user_email,
            cert_status,
            total_required
        )

        if audit_result.get("alreadyVoted"):
            audit_status = audit_result.get("auditStatus")
            existing_decision = audit_result.get("existingDecision")
            pending_approvals = audit_result.get("pendingApprovals", 0)

            if audit_status == "CERTIFIED":
                current_final_status = "CERTIFICATE"
            elif audit_status == "CANCELED":
                current_final_status = "NO_CERTIFICATE"
            elif audit_status == "PENDING":
                current_final_status = "PENDING_APPROVERS"
            else:
                current_final_status = resolve_final_status(
                    existing_decision,
                    pending_approvals
                )

            return _response(200, {
                "message": "El usuario ya registró una decisión para este caso",
                "existingDecision": existing_decision,
                "existingDecisionDate": audit_result.get("existingDecisionDate"),
                "requestedDecision": cert_status,
                "finalStatus": current_final_status,
                "pendingApprovals": pending_approvals,
                "updatedItems": 0,
                "processedBy": user_email,
                "useCaseId": use_case_id,
                "certCode": cert_code
            })

        final_status = resolve_final_status(
            cert_status,
            audit_result.get("pendingApprovals", 0)
        )

        now_iso = datetime.now(COLOMBIA_TZ).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")

        update_expr, cond_expr, expr_names, expr_vals = build_update_expressions(
            cert_status,
            final_status,
            now_iso,
            user_email
        )

        # QA: guardar quién procesó el caso y cuándo,
        # para mostrarlo si alguien intenta certificar/no certificar nuevamente.
        expr_names["#processedBy"] = "processedBy"
        expr_names["#processedDate"] = "processedDate"
        expr_names["#decision"] = "decision"

        expr_vals[":processedBy"] = user_email
        expr_vals[":processedDate"] = now_iso
        expr_vals[":decision"] = cert_status

        update_expr = (
            update_expr
            + ", #processedBy=:processedBy"
            + ", #processedDate=:processedDate"
            + ", #decision=:decision"
        )

        updated = 0

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

            updated += 1

        return _response(200, {
            "message": "Actualización correcta",
            "decision": cert_status,
            "finalStatus": final_status,
            "pendingApprovals": audit_result.get("pendingApprovals", 0),
            "updatedItems": updated,
            "processedBy": user_email,
            "useCaseId": use_case_id,
            "certCode": cert_code
        })

    except Exception as e:
        logger.exception("Error en Lambda de certificación")
        return _response(500, {
            "message": "Error interno",
            "error": str(e)
        })
