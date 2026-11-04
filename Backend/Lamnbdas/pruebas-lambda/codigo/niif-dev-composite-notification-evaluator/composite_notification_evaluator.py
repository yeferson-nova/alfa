import os
import json
import boto3
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
glue = boto3.client("glue")

TABLE_NAME = os.environ["TABLE_NAME"]
CONFIG_BUCKET = os.environ["CONFIG_BUCKET"]
COMPOSITE_GROUPS_KEY = os.environ["COMPOSITE_GROUPS_KEY"]
GLOBAL_CONFIG_KEY = os.environ["GLOBAL_CONFIG_KEY"]

GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
REDIRECT_BASE_URL = os.environ["REDIRECT_BASE_URL"]
SHORT_LINKS_TABLE = os.environ["SHORT_LINKS_TABLE"]

table = dynamodb.Table(TABLE_NAME)


def now():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def unmarshall_image(image):
    from boto3.dynamodb.types import TypeDeserializer
    deserializer = TypeDeserializer()
    return {k: deserializer.deserialize(v) for k, v in image.items()}


def load_composite_groups():
    obj = s3.get_object(Bucket=CONFIG_BUCKET, Key=COMPOSITE_GROUPS_KEY)
    return json.loads(obj["Body"].read().decode("utf-8"))


def get_group_id_from_composite_id(composite_id):
    parts = composite_id.split("#")
    if len(parts) < 3:
        raise ValueError(f"compositeId inválido: {composite_id}")
    return "#".join(parts[:3])


def find_group(groups_cfg, group_id):
    for group in groups_cfg.get("groups", []):
        if group.get("grupoId") == group_id and group.get("status", "ACTIVE") == "ACTIVE":
            return group
    return None


def query_members(composite_id):
    resp = table.query(
        KeyConditionExpression=Key("compositeId").eq(composite_id)
    )
    return [
        item for item in resp.get("Items", [])
        if item.get("memberId") != "_CONTROL"
    ]


def all_members_ready(expected_members, current_items):
    by_member = {i.get("memberId"): i for i in current_items}

    for member in expected_members:
        use_case_id = member.get("useCaseId")
        item = by_member.get(use_case_id)

        if not item:
            return False

        if item.get("status") != "READY_TO_NOTIFY":
            return False

    return True


def create_control_lock(
    composite_id,
    group_id,
    group_name,
    timeout_minutes=30
):
    control_key = {
        "compositeId": composite_id,
        "memberId": "_CONTROL"
    }

    response = table.get_item(
        Key=control_key,
        ConsistentRead=True
    )

    control = response.get("Item")
    current_time = datetime.now()

    if control:
        status = control.get("status")
        updated_at = (
            control.get("updatedAt")
            or control.get("createdAt")
        )

        lock_expired = False

        if status == "NOTIFICATION_IN_PROGRESS":
            try:
                lock_time = datetime.strptime(
                    updated_at,
                    "%Y-%m-%dT%H:%M:%S"
                )

                lock_expired = (
                    current_time - lock_time
                    >= timedelta(minutes=timeout_minutes)
                )
            except Exception:
                lock_expired = True

            if not lock_expired:
                print(
                    f"[SKIP] Lock activo para {composite_id}. "
                    f"updatedAt={updated_at}"
                )
                return False

            print(
                f"[LOCK_EXPIRED] Recuperando lock vencido "
                f"para {composite_id}. updatedAt={updated_at}"
            )

        elif status == "NOTIFICATION_COMPLETED":
            print(
                f"[NEW_EXECUTION] Reemplazando control completado "
                f"para {composite_id}"
            )

        else:
            print(
                f"[LOCK_RECOVERY] Reemplazando control "
                f"status={status} para {composite_id}"
            )

        table.delete_item(
            Key=control_key
        )

    timestamp = now()

    try:
        table.put_item(
            Item={
                "compositeId": composite_id,
                "memberId": "_CONTROL",
                "grupoId": group_id,
                "nombreGrupo": group_name,
                "status": "NOTIFICATION_IN_PROGRESS",
                "createdAt": timestamp,
                "updatedAt": timestamp
            },
            ConditionExpression=(
                "attribute_not_exists(compositeId)"
            )
        )

        print(
            f"[LOCK_CREATED] Control adquirido "
            f"para {composite_id}"
        )
        return True

    except ClientError as error:
        if (
            error.response["Error"]["Code"]
            == "ConditionalCheckFailedException"
        ):
            print(
                f"[SKIP] Otro proceso adquirio el lock "
                f"para {composite_id}"
            )
            return False

        raise


def start_composite_job(
    composite_id,
    group_id,
    group_name,
    period,
    lote_cargue_id,
    notification_config_bucket,
    notification_config_key,
    notification_use_case_id
):
    # En esta implementación el composite funciona como compuerta.
    # Cuando todos los miembros están READY_TO_NOTIFY, se dispara el Job 1 normal:
    # niif-{env}-certification, usando el JSON compuesto.
    args = {
        "--CONFIG_BUCKET": str(notification_config_bucket),
        "--CONFIG_KEY": str(notification_config_key),
        "--GLOBAL_CONFIG_KEY": GLOBAL_CONFIG_KEY,
        "--PERIOD": str(period),
        "--ENVIRONMENT": '{env}',
        "--LOG_LEVEL": "INFO",
        "--LOTE_CARGUE_ID": str(lote_cargue_id),
        "--COMPOSITE_MODE": "true",
        "--COMPOSITE_ID": str(composite_id),
        "--COMPOSITE_GROUP_ID": str(group_id),
        "--COMPOSITE_GROUP_NAME": str(group_name),
        "--NOTIFICATION_USE_CASE_ID": str(notification_use_case_id or "")
    }

    print("[START_JOB1_NORMAL]", GLUE_JOB_NAME)
    print("[START_JOB1_NORMAL_ARGS]", json.dumps(args, ensure_ascii=False))

    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments=args
    )

    return response["JobRunId"]


def update_control_with_job_run(composite_id, job_run_id):
    table.update_item(
        Key={
            "compositeId": composite_id,
            "memberId": "_CONTROL"
        },
        UpdateExpression=(
            "SET glueJobRunId = :j, "
            "updatedAt = :u"
        ),
        ExpressionAttributeValues={
            ":j": job_run_id,
            ":u": now()
        }
    )


def mark_members_in_progress(composite_id, items, job_run_id):
    for item in items:
        table.update_item(
            Key={
                "compositeId": composite_id,
                "memberId": item["memberId"]
            },
            UpdateExpression=(
                "SET #st = :s, "
                "notificationReleased = :r, "
                "glueJobRunId = :j, "
                "updatedAt = :u"
            ),
            ExpressionAttributeNames={
                "#st": "status"
            },
            ExpressionAttributeValues={
                ":s": "NOTIFICATION_IN_PROGRESS",
                ":r": False,
                ":j": job_run_id,
                ":u": now()
            }
        )


def process_ready_event(item):
    if item.get("status") != "READY_TO_NOTIFY":
        return

    composite_id = item["compositeId"]
    group_id = item.get("grupoId") or get_group_id_from_composite_id(composite_id)

    groups_cfg = load_composite_groups()
    group = find_group(groups_cfg, group_id)

    if not group:
        print(f"[NORMAL/NO-GROUP] No existe grupo activo para {group_id}")
        return

    expected_members = group.get("members", [])
    group_name = group.get("nombreGrupo", group_id)

    notification_config_bucket = group.get("notificationConfigBucket", CONFIG_BUCKET)
    notification_config_key = group.get("notificationConfigKey")
    notification_use_case_id = group.get("notificationUseCaseId")

    if not notification_config_key:
        raise ValueError(
            f"Grupo {group_id} no tiene notificationConfigKey configurado en {COMPOSITE_GROUPS_KEY}"
        )

    current_items = query_members(composite_id)

    if not all_members_ready(expected_members, current_items):
        print(f"[WAIT] Aún faltan miembros READY para {composite_id}")
        return

    if not create_control_lock(composite_id, group_id, group_name):
        return

    period = item.get("period")
    lote_cargue_id = item.get("loteCargueId")

    job_run_id = start_composite_job(
        composite_id=composite_id,
        group_id=group_id,
        group_name=group_name,
        period=period,
        lote_cargue_id=lote_cargue_id,
        notification_config_bucket=notification_config_bucket,
        notification_config_key=notification_config_key,
        notification_use_case_id=notification_use_case_id
    )

    update_control_with_job_run(composite_id, job_run_id)
    mark_members_in_progress(composite_id, current_items, job_run_id)

    print(f"[OK] Job compuesto disparado. compositeId={composite_id}, jobRunId={job_run_id}")




def find_group_for_use_case(groups_cfg, use_case_id):
    for group in groups_cfg.get("groups", []):
        if group.get("status", "ACTIVE") != "ACTIVE":
            continue

        for member in group.get("members", []):
            if member.get("useCaseId") == use_case_id:
                return group

    return None


def upsert_member_ready(
    composite_id,
    group_id,
    group_name,
    use_case_id,
    period,
    lote_cargue_id,
    config_bucket,
    config_key,
    source_execution_id
):
    table.update_item(
        Key={
            "compositeId": composite_id,
            "memberId": use_case_id
        },
        UpdateExpression=(
            "SET grupoId = :g, "
            "nombreGrupo = :gn, "
            "useCaseId = :u, "
            "#st = :s, "
            "period = :p, "
            "loteCargueId = :l, "
            "configBucket = :cb, "
            "configKey = :ck, "
            "globalConfigKey = :gck, "
            "notificationReleased = :nr, "
            "sourceExecutionId = :sei, "
            "readyAt = if_not_exists(readyAt, :now), "
            "updatedAt = :now"
        ),
        ExpressionAttributeNames={
            "#st": "status"
        },
        ExpressionAttributeValues={
            ":g": group_id,
            ":gn": group_name,
            ":u": use_case_id,
            ":s": "READY_TO_NOTIFY",
            ":p": str(period),
            ":l": str(lote_cargue_id),
            ":cb": str(config_bucket),
            ":ck": str(config_key),
            ":gck": GLOBAL_CONFIG_KEY,
            ":nr": False,
            ":sei": str(source_execution_id or ""),
            ":now": now()
        }
    )


def resolve_notification_event(event):
    use_case_id = event.get("useCaseId")
    period = event.get("period")
    lote_cargue_id = event.get("loteCargueId", "NO_LOTE")
    config_bucket = event.get("configBucket", CONFIG_BUCKET)
    config_key = event.get("configKey")
    source_execution_id = event.get("sourceExecutionId", "")

    if not use_case_id:
        raise ValueError("Falta useCaseId en evento RESOLVE_NOTIFICATION")
    if not period:
        raise ValueError("Falta period en evento RESOLVE_NOTIFICATION")
    if not config_key:
        raise ValueError("Falta configKey en evento RESOLVE_NOTIFICATION")

    groups_cfg = load_composite_groups()
    group = find_group_for_use_case(groups_cfg, use_case_id)

    if event.get("regeneracionSinDatosNuevos", False) is True:
        if group:
            config_bucket = group.get("notificationConfigBucket", CONFIG_BUCKET)
            config_key = group.get("notificationConfigKey")
            use_case_id = group.get("notificationUseCaseId")
        if not config_key or not use_case_id:
            raise ValueError("Configuracion de regeneracion incompleta")
        return {
            "notificationMode": "REGENERACION_SIN_DATOS_NUEVOS",
            "shouldStartCertification": True,
            "regeneracionSinDatosNuevos": True,
            "useCaseId": use_case_id,
            "period": str(period),
            "loteCargueId": str(lote_cargue_id),
            "configBucket": str(config_bucket),
            "configKey": str(config_key)
        }


    # Caso simple: no está en composite_groups.json
    if not group:
        return {
            "notificationMode": "SIMPLE",
            "shouldStartCertification": True,
            "useCaseId": use_case_id,
            "period": str(period),
            "loteCargueId": str(lote_cargue_id),
            "configBucket": str(config_bucket),
            "configKey": str(config_key)
        }

    group_id = group["grupoId"]
    group_name = group.get("nombreGrupo", group_id)
    composite_id = f"{group_id}#{period}"

    upsert_member_ready(
        composite_id=composite_id,
        group_id=group_id,
        group_name=group_name,
        use_case_id=use_case_id,
        period=period,
        lote_cargue_id=lote_cargue_id,
        config_bucket=config_bucket,
        config_key=config_key,
        source_execution_id=source_execution_id
    )

    expected_members = group.get("members", [])
    current_items = query_members(composite_id)

    if not all_members_ready(expected_members, current_items):
        return {
            "notificationMode": "COMPOSITE_WAITING",
            "shouldStartCertification": False,
            "useCaseId": use_case_id,
            "period": str(period),
            "loteCargueId": str(lote_cargue_id),
            "compositeId": composite_id,
            "compositeGroupId": group_id,
            "compositeGroupName": group_name
        }

    # Lock para evitar doble disparo si dos miembros terminan casi al mismo tiempo
    if not create_control_lock(composite_id, group_id, group_name):
        return {
            "notificationMode": "COMPOSITE_ALREADY_LOCKED",
            "shouldStartCertification": False,
            "useCaseId": use_case_id,
            "period": str(period),
            "loteCargueId": str(lote_cargue_id),
            "compositeId": composite_id,
            "compositeGroupId": group_id,
            "compositeGroupName": group_name
        }

    notification_config_bucket = group.get("notificationConfigBucket", CONFIG_BUCKET)
    notification_config_key = group.get("notificationConfigKey")
    notification_use_case_id = group.get("notificationUseCaseId")

    if not notification_config_key:
        raise ValueError(
            f"Grupo {group_id} no tiene notificationConfigKey configurado en {COMPOSITE_GROUPS_KEY}"
        )

    return {
        "notificationMode": "COMPOSITE_READY",
        "shouldStartCertification": True,
        "useCaseId": notification_use_case_id,
        "period": str(period),
        "loteCargueId": str(lote_cargue_id),
        "configBucket": str(notification_config_bucket),
        "configKey": str(notification_config_key),
        "compositeId": composite_id,
        "compositeGroupId": group_id,
        "compositeGroupName": group_name
    }



def lambda_handler(event, context):
    print(json.dumps(event, default=str))

    # Modo invocado desde Step Functions
    if event.get("mode") == "RESOLVE_NOTIFICATION":
        result = resolve_notification_event(event)
        print("[RESOLVE_NOTIFICATION_RESULT]", json.dumps(result, ensure_ascii=False))
        return result

    # Eventos DynamoDB Stream:
    # En este nuevo diseño la Step Function decide y ejecuta Job 1.
    # El stream queda ignorado para evitar disparos dobles desde Lambda.
    if "Records" in event:
        print("[STREAM_SKIP] Evento DynamoDB Stream ignorado en modo router.")
        return {
            "statusCode": 200,
            "body": "STREAM_SKIPPED"
        }

    return {
        "statusCode": 200,
        "body": "OK"
    }
