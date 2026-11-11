import sys
import json
import time
import calendar
import logging
import html
import uuid

from datetime import datetime, timedelta
from string import Template
from zoneinfo import ZoneInfo

import boto3
from boto3.dynamodb.conditions import Key
from awsglue.utils import getResolvedOptions


# =================================================
# LOGGING
# =================================================
import logging

logger = logging.getLogger(__name__)


def log(msg):
    print(msg, flush=True)
    logger.info(msg)


# ============================================================
# ARGUMENTOS GLUE
# ============================================================

args = getResolvedOptions(sys.argv, [
    "useCaseId",
    "certCode",
    "emailType",
    "CONFIG_BUCKET",
    "CONFIG_KEY",
    "GLOBAL_CONFIG_KEY",
    "SNS_TOPIC_ARN",
    "REDIRECT_BASE_URL",
    "SHORT_LINKS_TABLE"
])

useCaseId = args["useCaseId"]
certCode = args["certCode"]
emailType = args["emailType"].lower()

CONFIG_BUCKET = args["CONFIG_BUCKET"]
CONFIG_KEY = args["CONFIG_KEY"]
GLOBAL_CONFIG_KEY = args["GLOBAL_CONFIG_KEY"]

SNS_TOPIC_ARN = args["SNS_TOPIC_ARN"]
REDIRECT_BASE_URL = args["REDIRECT_BASE_URL"]
SHORT_LINKS_TABLE = args["SHORT_LINKS_TABLE"]

# Marca opcional: el flujo normal conserva false.
REGENERACION_SIN_DATOS_NUEVOS = False
if "--REGENERACION_SIN_DATOS_NUEVOS" in sys.argv:
    _marca = getResolvedOptions(
        sys.argv, ["REGENERACION_SIN_DATOS_NUEVOS"]
    )["REGENERACION_SIN_DATOS_NUEVOS"].lower()
    if _marca not in ("true", "false"):
        raise ValueError("Marca de regeneracion invalida")
    REGENERACION_SIN_DATOS_NUEVOS = _marca == "true"



# ============================================================
# CLIENTES AWS
# ============================================================

s3 = boto3.client("s3")
sns = boto3.client("sns")

def asegurar_filtros_sns_por_recipient(topic_arn):
    """
    Asegura que cada suscripción email confirmada del topic tenga FilterPolicy:
    {"recipient": ["endpoint_email"]}

    Esto permite que el JSON controle destinatarios:
    - El script publica mensajes con MessageAttributes.recipient
    - SNS entrega solo a la suscripción cuyo filtro coincide.
    """
    import json

    log("[SNS] Validando/configurando FilterPolicy por recipient en suscripciones confirmadas")

    next_token = None
    total = 0
    actualizadas = 0
    omitidas = 0

    while True:
        kwargs = {"TopicArn": topic_arn}
        if next_token:
            kwargs["NextToken"] = next_token

        resp = sns.list_subscriptions_by_topic(**kwargs)

        for sub in resp.get("Subscriptions", []):
            protocol = (sub.get("Protocol") or "").lower()
            endpoint = (sub.get("Endpoint") or "").strip().lower()
            sub_arn = sub.get("SubscriptionArn")

            if protocol != "email":
                omitidas += 1
                continue

            if not endpoint:
                omitidas += 1
                continue

            if not sub_arn or sub_arn == "PendingConfirmation":
                log(f"[SNS] Suscripción pendiente/no confirmada, se omite endpoint={endpoint}")
                omitidas += 1
                continue

            total += 1

            desired_policy_obj = {"recipient": [endpoint]}
            desired_policy = json.dumps(desired_policy_obj, separators=(",", ":"))

            try:
                attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn).get("Attributes", {})
                current_policy_raw = attrs.get("FilterPolicy")

                same_policy = False
                if current_policy_raw:
                    try:
                        same_policy = json.loads(current_policy_raw) == desired_policy_obj
                    except Exception:
                        same_policy = False

                if not same_policy:
                    sns.set_subscription_attributes(
                        SubscriptionArn=sub_arn,
                        AttributeName="FilterPolicy",
                        AttributeValue=desired_policy
                    )
                    actualizadas += 1
                    log(f"[SNS] FilterPolicy configurado endpoint={endpoint}")
                else:
                    log(f"[SNS] FilterPolicy OK endpoint={endpoint}")

            except Exception as e:
                log(f"[SNS] ERROR configurando FilterPolicy endpoint={endpoint}: {e}")
                raise

        next_token = resp.get("NextToken")
        if not next_token:
            break

    log(f"[SNS] FilterPolicy validado. confirmadas={total}, actualizadas={actualizadas}, omitidas={omitidas}")


athena = boto3.client("athena")
dynamodb = boto3.resource("dynamodb")
short_table = dynamodb.Table(SHORT_LINKS_TABLE)


# ============================================================
# HELPERS CONFIG
# ============================================================

def normalizar_s3_key(bucket: str, key: str) -> str:
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


def cargar_json(bucket, key):
    key = normalizar_s3_key(bucket, key)

    log(f"[CONFIG] Leyendo JSON desde s3://{bucket}/{key}")

    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def limpiar_url(url):
    url = str(url or "")

    for _ in range(3):
        nuevo = html.unescape(url)
        if nuevo == url:
            break
        url = nuevo

    return url.strip()


def parametrizar(texto, variables, unescape=False):
    if texto is None:
        return texto

    texto = str(texto)

    if unescape:
        texto = limpiar_url(texto)

    return Template(texto).safe_substitute(variables)


def periodo_format(period):
    y, m = int(period[:4]), int(period[4:6])

    return (
        f"{datetime(y, m, 1):%d/%m/%Y} - "
        f"{datetime(y, m, calendar.monthrange(y, m)[1]):%d/%m/%Y}"
    )


# ============================================================
# DYNAMO
# ============================================================

def obtener_regs(table_name):
    table = dynamodb.Table(table_name)

    log(
        f"[DYNAMO] Consultando tabla={table_name}, "
        f"IndexName=certCode, certCode={certCode}"
    )

    response = table.query(
        IndexName="certCode-index",
        KeyConditionExpression=Key("certCode").eq(certCode)
    )

    items = [
        item for item in response.get("Items", [])
        if item.get("useCaseId") == useCaseId
    ]

    log(f"[DYNAMO] Registros encontrados para useCaseId={useCaseId}: {len(items)}")

    return items


def crear_short_link(action, long_url):
    token = str(uuid.uuid4())[:8]

    now = datetime.now(ZoneInfo("America/Bogota"))
    expires = int((now + timedelta(days=7)).timestamp())

    short_table.put_item(Item={
        "token": token,
        "url": long_url,
        "useCaseId": useCaseId,
        "certCode": certCode,
        "action": action,
        "creationDate": now.isoformat(),
        "expiresAt": expires
    })

    short_url = f"{REDIRECT_BASE_URL}?t={token}"

    log(f"[SHORT-LINK] action={action}, token={token}")
    log(f"[SHORT-LINK] shortUrl={short_url}")

    return short_url


def marcar_sent(table_name, items, sent_status, response, destinations):
    table = dynamodb.Table(table_name)

    now = datetime.now(ZoneInfo("America/Bogota")).isoformat()
    message_id = response.get("MessageId", "")

    for item in items:
        table.update_item(
            Key={
                "useCaseId": item["useCaseId"],
                "executionId": item["executionId"]
            },
            UpdateExpression=(
                "SET #st = :s, "
                "sentDate = :d, "
                "notificationChannel = :ch, "
                "snsMessageId = :mid, "
                "emailDestination = :dest"
            ),
            ExpressionAttributeNames={
                "#st": "status"
            },
            ExpressionAttributeValues={
                ":s": sent_status,
                ":d": now,
                ":ch": "SNS_EMAIL",
                ":mid": message_id,
                ":dest": destinations
            }
        )

        log(
            f"[DYNAMO] Registro marcado como {sent_status}. "
            f"useCaseId={item['useCaseId']}, "
            f"executionId={item['executionId']}, "
            f"snsMessageId={message_id}"
        )


# ============================================================
# ATHENA
# ============================================================

def ejecutar_athena(database, query, variables, athena_output, catalog):
    db = parametrizar(database, variables, unescape=True)
    q = parametrizar(query, variables, unescape=True)

    log(f"[ATHENA] DB={db}")
    log(f"[ATHENA] Query final:\n{q}")

    response = athena.start_query_execution(
        QueryString=q,
        QueryExecutionContext={
            "Catalog": catalog,
            "Database": db
        },
        ResultConfiguration={
            "OutputLocation": athena_output
        }
    )

    qid = response["QueryExecutionId"]
    log(f"[ATHENA] QueryExecutionId={qid}")

    while True:
        execution = athena.get_query_execution(QueryExecutionId=qid)
        status = execution["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            log(f"[ATHENA] Query SUCCEEDED: {qid}")
            break

        if status in ["FAILED", "CANCELLED"]:
            reason = execution["QueryExecution"]["Status"].get("StateChangeReason", "")
            log(f"[ATHENA] Query {status}: {qid}. Reason={reason}")
            return "Error ejecutando consulta"

        time.sleep(1)

    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]

    if len(rows) <= 1:
        return "No hay datos"

    headers = [
        col.get("VarCharValue", "")
        for col in rows[0]["Data"]
    ]

    lines = []

    for row in rows[1:]:
        values = [
            col.get("VarCharValue", "")
            for col in row.get("Data", [])
        ]

        line = " | ".join(
            f"{headers[i]}: {values[i] if i < len(values) else ''}"
            for i in range(len(headers))
        )

        lines.append(line)

    return "\n".join(lines)


def generar_tablas(config, tipo, base_vars, athena_output, catalog):
    salida = ""
    queries = config.get("queries", {}).get(tipo, [])

    log(f"[TABLES] tipo={tipo}, queries={len(queries)}")

    for q in sorted(queries, key=lambda x: x.get("queryName", "")):
        resultado = ejecutar_athena(
            q["database"],
            q["query"],
            base_vars,
            athena_output,
            catalog
        )

        salida += resultado
        salida += "\n\n"

    return salida.strip()


# ============================================================
# MAIN
# ============================================================

def aviso_sin_datos_por_miembro(case_cfg, base, athena_output, catalog):
    """Consulta Gold por miembro; un fallo tecnico nunca se interpreta como cero.
    Se ejecuta antes de crear enlaces o enviar SNS. No modifica certificacion.
    Los conteos reflejan Gold al preparar el correo, no una instantanea del CSV.
    """
    import re
    miembros = case_cfg.get("email", {}).get("dataPresence", [])
    if not miembros:
        return None
    if not isinstance(miembros, list):
        raise ValueError("email.dataPresence debe ser una lista")
    vacios = []
    vistos = set()
    for miembro in miembros:
        nombre = miembro.get("name")
        if not isinstance(nombre, str) or not nombre.strip() or nombre in vistos:
            raise ValueError("Nombre de miembro ausente o duplicado en dataPresence")
        vistos.add(nombre)
        resultado = ejecutar_athena(
            miembro["database"], miembro["query"], base, athena_output, catalog
        )
        # ejecutar_athena devuelve una fila formateada. Exigir el contrato exacto.
        match = re.fullmatch(r"registros: ([0-9]+)", str(resultado).strip())
        if not match:
            raise RuntimeError("No se pudo confirmar disponibilidad de " + nombre)
        if int(match.group(1)) == 0:
            vacios.append(nombre)
    if not vacios:
        return ""
    periodo = str(base["period"])
    lineas = ["Disponibilidad por caso de uso:"]
    lineas.extend(nombre + ": no se encontraron datos activos para el periodo " + periodo + "." for nombre in vacios)
    if len(vacios) == len(miembros):
        lineas.append("Ninguno de los casos del compuesto tiene datos activos para el periodo solicitado.")
    else:
        lineas.append("El archivo y los resumenes incluyen los datos disponibles de los demas casos.")
    return "\n".join(lineas)


# CIERRE_COMPUESTO_CONSUMED_V1

def preparar_cierre_compuesto(case_cfg, period, cert_code, regeneracion=False):
    """Captura referencias antes del envio. No consume ni cambia estados."""
    cfg = case_cfg.get('compositeNotification') or {}
    if regeneracion or not cfg.get('enabled', False):
        return None
    table_name = cfg['tableName']
    composite_id = cfg['groupId'] + '#' + str(period)
    control_id = cfg.get('controlMemberId', '_CONTROL')
    member_ids = cfg['memberIds']
    if not member_ids or len(set(member_ids)) != len(member_ids) or control_id in member_ids:
        raise ValueError('Miembros del compuesto invalidos')
    table = boto3.resource('dynamodb').Table(table_name)
    control = table.get_item(Key={'compositeId': composite_id, 'memberId': control_id}, ConsistentRead=True).get('Item', {})
    if control.get('status') == 'NOTIFICATION_COMPLETED':
        if control.get('certCode') == cert_code:
            return {'alreadyCompleted': True}
        raise RuntimeError('Control ya completado para otra solicitud; no se reenvia')
    if control.get('status') != 'NOTIFICATION_IN_PROGRESS' or not control.get('updatedAt'):
        raise RuntimeError('Control no esta reservado para notificacion')
    members = []
    for member_id in member_ids:
        member = table.get_item(Key={'compositeId': composite_id, 'memberId': member_id}, ConsistentRead=True).get('Item', {})
        if member.get('status') != 'READY_TO_NOTIFY' or str(member.get('period')) != str(period):
            raise RuntimeError('Miembro no listo: ' + member_id)
        if not all(member.get(k) for k in ('sourceExecutionId', 'loteCargueId', 'updatedAt')):
            raise RuntimeError('Miembro sin identidad completa: ' + member_id)
        members.append(member)
    return {'tableName': table_name, 'compositeId': composite_id,
            'control': control, 'members': members, 'certCode': cert_code}


def cerrar_compuesto_consumed(snapshot, sns_response):
    """Solo despues de SNS y marcar_sent. Transaccion condicional todo o nada.
    SNS aceptado no demuestra entrega en bandeja. Si falla este cierre tras SNS,
    NO reenviar automaticamente: recuperar solo el cierre con los MessageId.
    """
    if not snapshot or snapshot.get('alreadyCompleted'):
        return
    if not sns_response.get('MessageId'):
        raise RuntimeError('SNS no devolvio MessageId; no se consume')
    from boto3.dynamodb.types import TypeSerializer
    from datetime import timezone
    serializer = TypeSerializer()
    encode = lambda obj: {k: serializer.serialize(v) for k, v in obj.items()}
    now = datetime.now(timezone.utc).isoformat()
    table_name = snapshot['tableName']
    cid = snapshot['compositeId']
    transactions = []
    for member in snapshot['members']:
        transactions.append({'Update': {
            'TableName': table_name,
            'Key': encode({'compositeId': cid, 'memberId': member['memberId']}),
            'UpdateExpression': 'SET #st = :consumed, notificationReleased = :yes, completedAt = :now, updatedAt = :now, certCode = :cert, snsMessageId = :mid',
            'ConditionExpression': '#st = :ready AND sourceExecutionId = :exec AND loteCargueId = :lot AND updatedAt = :previous',
            'ExpressionAttributeNames': {'#st': 'status'},
            'ExpressionAttributeValues': encode({
                ':consumed': 'CONSUMED', ':yes': True, ':now': now,
                ':cert': snapshot['certCode'], ':mid': sns_response['MessageId'],
                ':ready': 'READY_TO_NOTIFY', ':exec': member['sourceExecutionId'],
                ':lot': member['loteCargueId'], ':previous': member['updatedAt']})
        }})
    control = snapshot['control']
    transactions.append({'Update': {
        'TableName': table_name,
        'Key': encode({'compositeId': cid, 'memberId': control['memberId']}),
        'UpdateExpression': 'SET #st = :done, completedAt = :now, updatedAt = :now, certCode = :cert, snsMessageId = :mid',
        'ConditionExpression': '#st = :progress AND updatedAt = :previous',
        'ExpressionAttributeNames': {'#st': 'status'},
        'ExpressionAttributeValues': encode({':done': 'NOTIFICATION_COMPLETED',
            ':now': now, ':cert': snapshot['certCode'], ':mid': sns_response['MessageId'],
            ':progress': 'NOTIFICATION_IN_PROGRESS', ':previous': control['updatedAt']})
    }})
    boto3.client('dynamodb').transact_write_items(TransactItems=transactions)
    log('[COMPOSITE] Miembros CONSUMED y control NOTIFICATION_COMPLETED. certCode=' + snapshot['certCode'])


def main():
    log('============================================================')
    log(f'[ARGS] useCaseId={useCaseId}')
    log(f'[ARGS] certCode={certCode}')
    log(f'[ARGS] emailType={emailType}')
    log(f'[ARGS] CONFIG_BUCKET={CONFIG_BUCKET}')
    log(f'[ARGS] CONFIG_KEY={CONFIG_KEY}')
    log(f'[ARGS] GLOBAL_CONFIG_KEY={GLOBAL_CONFIG_KEY}')
    log(f'[ARGS] SNS_TOPIC_ARN={SNS_TOPIC_ARN}')
    log(f'[ARGS] REDIRECT_BASE_URL={REDIRECT_BASE_URL}')
    log(f'[ARGS] SHORT_LINKS_TABLE={SHORT_LINKS_TABLE}')
    log('============================================================')
    global_cfg = cargar_json(CONFIG_BUCKET, GLOBAL_CONFIG_KEY)
    case_cfg = cargar_json(CONFIG_BUCKET, CONFIG_KEY)
    if case_cfg.get('useCaseId') != useCaseId:
        raise RuntimeError(f"El useCaseId del JSON no coincide. JSON={case_cfg.get('useCaseId')} ARG={useCaseId}")
    email_cfg = case_cfg['email']
    assets_bucket = global_cfg['buckets']['assets']
    notification_table = global_cfg['dynamodb']['notification_table']
    catalog = global_cfg.get('catalog', 'AwsDataCatalog')
    env = global_cfg['environment']
    athena_output = f"s3://{assets_bucket}/{global_cfg['athena']['output_path']}/"
    sent_status = global_cfg['certification'].get('sent_status', 'SENT')
    items = obtener_regs(notification_table)
    if not items:
        log(f'[WARN] No hay registros asociados al certCode={certCode} para useCaseId={useCaseId}. Posible evento viejo o ejecución con argumentos anteriores. Se finaliza sin enviar notificación.')
        return
    first = items[0]
    cierre_snapshot = preparar_cierre_compuesto(case_cfg, first['period'], certCode, REGENERACION_SIN_DATOS_NUEVOS)
    if cierre_snapshot and cierre_snapshot.get('alreadyCompleted'):
        log('[COMPOSITE] Solicitud ya completada; no se reenvia')
        return
    base = {'env': env, 'period': first['period'], 'periodRange': periodo_format(first['period']), 'useCaseId': useCaseId, 'useCaseName': email_cfg.get('stringVars', {}).get('useCaseName', useCaseId), 'certCode': certCode, 'userEmail': first.get('userEmail', ''), 'executionId': first['executionId']}
    string_vars = {key: parametrizar(value, base, unescape=False) for key, value in email_cfg.get('stringVars', {}).items()}
    final_vars = {**base, **string_vars}
    subject = Template(email_cfg['subject']).safe_substitute(final_vars)
    subject_sns = subject.replace('\n', ' ').replace('\r', ' ')[:100]
    table_summary = generar_tablas(case_cfg, 'summary', base, athena_output, catalog)
    table_total = generar_tablas(case_cfg, 'total', base, athena_output, catalog)
    aviso_miembros = aviso_sin_datos_por_miembro(case_cfg, base, athena_output, catalog)
    certificar_link_largo = limpiar_url(final_vars.get('certificarLink', ''))
    nocertificar_link_largo = limpiar_url(final_vars.get('nocertificarLink', ''))
    certificar_link = crear_short_link('CERTIFICATE', certificar_link_largo)
    nocertificar_link = crear_short_link('NO_CERTIFICATE', nocertificar_link_largo)
    archivos = []
    for item in sorted(items, key=lambda x: x.get('queryName', '')):
        if item.get('queryName', '').startswith('detail#'):
            s3_url_larga = limpiar_url(item.get('s3_url', ''))
            s3_url_corta = crear_short_link('DOWNLOAD', s3_url_larga)
            archivos.append(f"- {item.get('nameFile', 'Archivo CSV')}\n  {s3_url_corta}")
    archivos_txt = '\n'.join(archivos) if archivos else 'No se encontraron archivos detalle.'
    mensaje_sns = f"\nSolicitud de certificación NIIF 17\n\nCaso de uso:\n{final_vars.get('useCaseName', useCaseId)}\n\nResumen:\n{(table_summary if table_summary else 'No hay resumen configurado.')}\n\nTotales:\n{(table_total if table_total else 'No hay totales configurados.')}\n\nArchivos generados:\n{archivos_txt}\n\nCertificar:\n{certificar_link}\n\nNo certificar:\n{nocertificar_link}\n\nEste mensaje fue generado automáticamente por el flujo NIIF 17.\n"
    if aviso_miembros is not None:
        if aviso_miembros:
            mensaje_sns = aviso_miembros + '\n\n' + mensaje_sns
    else:
        vacios = [x for x in items if x.get('sinDatosPeriodo') is True]
        if vacios:
            if len(vacios) == len(items):
                aviso = 'No se encontraron datos procesados para el periodo seleccionado: ' + str(first['period']) + '.'
            else:
                aviso = 'No se encontraron datos procesados para el periodo ' + str(first['period']) + ' en las consultas: ' + ', '.join((str(x.get('queryName', '')) for x in vacios)) + '. Las demas consultas conservan sus datos.'
            mensaje_sns = aviso + '\n\n' + mensaje_sns
    if REGENERACION_SIN_DATOS_NUEVOS:
        subject_sns = ('[REGENERADO] ' + subject_sns)[:100]
        mensaje_sns = 'CASO DE USO REEJECUTADO SIN REGISTROS NUEVOS\nArchivos regenerados desde Gold existente del período.\nSilver y Gold no se ejecutaron nuevamente.\nNo se verificaron cambios en registros ya procesados.\n\n' + mensaje_sns
    log('==== INICIO ENVIO SNS ====')
    log(f'[SNS] TopicArn={SNS_TOPIC_ARN}')
    log(f'[SNS] Subject={subject_sns}')
    log(f'[SNS] Certificar corto={certificar_link}')
    log(f'[SNS] No certificar corto={nocertificar_link}')
    destinations = [str(d).strip().lower() for d in email_cfg.get('destinations', []) if str(d).strip()]
    if not destinations:
        raise ValueError('No hay destinatarios configurados en email.destinations')
    asegurar_filtros_sns_por_recipient(SNS_TOPIC_ARN)
    responses = []
    for dest in destinations:
        log(f'[SNS] Enviando mensaje filtrado para recipient={dest}')
        resp = sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject_sns, Message=mensaje_sns, MessageAttributes={'recipient': {'DataType': 'String', 'StringValue': dest}, 'useCaseId': {'DataType': 'String', 'StringValue': str(useCaseId)}, 'emailType': {'DataType': 'String', 'StringValue': str(emailType)}})
        if not resp.get('MessageId'):
            raise RuntimeError('SNS no confirmo MessageId; no se consumen miembros')
        responses.append({'destination': dest, 'messageId': resp.get('MessageId')})
        log(f"[SNS] MessageId={resp.get('MessageId')} recipient={dest}")
    response = {'MessageId': ','.join([r.get('messageId', '') for r in responses]), 'Responses': responses}
    log('==== FIN ENVIO SNS ====')
    marcar_sent(notification_table, items, sent_status, response, destinations)
    log('[END] MENSAJE SNS ENVIADO Y REGISTROS MARCADOS COMO SENT')
    cerrar_compuesto_consumed(cierre_snapshot, response)
    log('============================================================')


if __name__ == "__main__":
    main()
# SIN_DATOS_CORREO_V1
