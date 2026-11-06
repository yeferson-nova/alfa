from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import sys
import json
import boto3
from botocore.config import Config
import logging
import io
import gzip
import time
import calendar
import uuid
import html

from string import Template
from datetime import datetime
from boto3.dynamodb.conditions import Key
from zoneinfo import ZoneInfo


# =================================================
# LOGGING
# =================================================
import logging

logger = logging.getLogger(__name__)


# ============================================================
# ARGUMENTOS GLUE
# ============================================================

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "CONFIG_BUCKET",
    "CONFIG_KEY",
    "GLOBAL_CONFIG_KEY",
    "PERIOD"
])

JOB_NAME = args["JOB_NAME"]
CONFIG_BUCKET = args["CONFIG_BUCKET"]
CONFIG_KEY = args["CONFIG_KEY"]
GLOBAL_CONFIG_KEY = args["GLOBAL_CONFIG_KEY"]
PERIOD = args["PERIOD"]

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
# INICIALIZACION GLUE
# ============================================================

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(JOB_NAME, args)


# ============================================================
# CLIENTES AWS
# ============================================================

s3 = boto3.client(
    "s3",
    region_name=boto3.session.Session().region_name,
    config=Config(signature_version="s3v4")
)


# ==========================
# _NIIF_HEADERS_GENERIC_PATCH
# Agrega encabezados a CSV finales usando email.tableVars.*.columns del JSON.
# ==========================
_CASE_CONFIG_FOR_HEADERS = None
_ORIGINAL_S3_PUT_OBJECT = s3.put_object

def _niif_decode_body(body):
    if body is None:
        return "", False, False

    if isinstance(body, bytes):
        has_bom = body.startswith(b"\xef\xbb\xbf")
        return body.decode("utf-8-sig"), True, has_bom

    if isinstance(body, str):
        has_bom = body.startswith("\ufeff")
        return body.lstrip("\ufeff"), False, has_bom

    return body, False, False


def _niif_encode_body(text, was_bytes, had_bom):
    if not isinstance(text, str):
        return text

    if had_bom:
        text = "\ufeff" + text.lstrip("\ufeff")

    if was_bytes:
        return text.encode("utf-8")

    return text


def _niif_query_group_from_s3_key(key):
    """
    Ejemplo nameFile:
      202602-detail_1-20260624-223432.csv -> detail
      202606-total_1-...                  -> total
    """
    if not key:
        return None

    filename = key.split("/")[-1]

    parts = filename.split("-")
    if len(parts) < 2:
        return None

    token = parts[1]  # detail_1, total_1, summary_1
    return token.split("_")[0] if "_" in token else token


def _niif_headers_for_group(query_group):
    """
    Busca en email.tableVars una tabla cuyo queryName sea igual al grupo.
    Ejemplo:
      tableDetail.queryName = detail
      tableTotal.queryName  = total
    Solo agrega encabezados para export=true o cuando exista columns.
    """
    global _CASE_CONFIG_FOR_HEADERS

    if not _CASE_CONFIG_FOR_HEADERS or not query_group:
        return None

    table_vars = (
        _CASE_CONFIG_FOR_HEADERS
        .get("email", {})
        .get("tableVars", {})
    )

    for _, table_cfg in table_vars.items():
        if table_cfg.get("queryName") != query_group:
            continue

        columns = table_cfg.get("columns", {})
        if not columns:
            return None

        # Para evitar meter encabezados a bloques que no son archivos exportables,
        # se prioriza export=true, pero si no existe export y sí columns, también aplica.
        if table_cfg.get("export") is False:
            return None

        headers = []
        for col_key, meta in columns.items():
            if isinstance(meta, dict):
                headers.append(str(meta.get("name") or col_key))
            else:
                headers.append(str(col_key))

        return headers

    return None


def _niif_add_headers_to_csv_body(body, key):
    query_group = _niif_query_group_from_s3_key(key)
    headers = _niif_headers_for_group(query_group)

    if not headers:
        return body

    text, was_bytes, had_bom = _niif_decode_body(body)

    if not isinstance(text, str):
        return body

    delimiter = "|"
    header_line = delimiter.join(headers)

    # Si el body está vacío, solo deja encabezado.
    if not text.strip():
        new_text = header_line + "\n"
        return _niif_encode_body(new_text, was_bytes, had_bom)

    clean_text = text.lstrip("\ufeff")
    first_line = clean_text.splitlines()[0] if clean_text.splitlines() else ""

    # Idempotencia: si ya tiene el encabezado, no lo duplica.
    if first_line.strip() == header_line.strip():
        return body

    new_text = header_line + "\n" + clean_text
    return _niif_encode_body(new_text, was_bytes, had_bom)


def _niif_put_object_with_headers(**kwargs):
    """
    Intercepta escrituras al bucket de reportes y agrega headers a CSVs.
    """
    try:
        bucket = kwargs.get("Bucket")
        key = kwargs.get("Key")

        if (
            REPORT_BUCKET
            and bucket == REPORT_BUCKET
            and key
            and str(key).lower().endswith(".csv")
            and "Body" in kwargs
        ):
            kwargs["Body"] = _niif_add_headers_to_csv_body(kwargs.get("Body"), key)

    except Exception as e:
        logger.warning(f"[HEADERS] No se pudieron agregar encabezados automáticamente: {e}")

    return _ORIGINAL_S3_PUT_OBJECT(**kwargs)


s3.put_object = _niif_put_object_with_headers

# ==========================
# Fin _NIIF_HEADERS_GENERIC_PATCH
# ==========================

athena = boto3.client("athena")
dynamodb = boto3.resource("dynamodb")


# ============================================================
# VARIABLES GLOBALES CARGADAS DESDE JSON GLOBAL
# ============================================================

ENVIRONMENT = None
REGION = None
TIMEZONE = None
GLUE_CATALOG = None

ASSETS_BUCKET = None
REPORT_BUCKET = None
NOTIFICATION_DYNAMODB_TABLE = None

ATHENA_OUTPUT_PATH = None
ATHENA_UNLOAD_PATH = None
ATHENA_OUTPUT_LOCATION = None

CONSOLE_BASE_URL = None
ACCOUNT_ID = None
ROLE_NAME = None
CONSOLE_REGION = None

DEFAULT_USER_EMAIL = None
STATUS_PROCESSED = None
STATUS_PENDING = None

CSV_CONTENT_TYPE = None
CSV_ENCODING_BOM = True


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


def cargar_json_s3(bucket: str, key: str) -> dict:
    key = normalizar_s3_key(bucket, key)

    logger.info(f"[CONFIG] Leyendo JSON desde s3://{bucket}/{key}")

    obj = s3.get_object(Bucket=bucket, Key=key)
    contenido = obj["Body"].read().decode("utf-8")

    return json.loads(contenido)


def cargar_configs():
    global ENVIRONMENT
    global REGION
    global TIMEZONE
    global GLUE_CATALOG

    global ASSETS_BUCKET
    global REPORT_BUCKET
    global NOTIFICATION_DYNAMODB_TABLE

    global ATHENA_OUTPUT_PATH
    global ATHENA_UNLOAD_PATH
    global ATHENA_OUTPUT_LOCATION

    global CONSOLE_BASE_URL
    global ACCOUNT_ID
    global ROLE_NAME
    global CONSOLE_REGION

    global DEFAULT_USER_EMAIL
    global STATUS_PROCESSED
    global STATUS_PENDING

    global CSV_CONTENT_TYPE
    global CSV_ENCODING_BOM

    global_config = cargar_json_s3(CONFIG_BUCKET, GLOBAL_CONFIG_KEY)
    global _CASE_CONFIG_FOR_HEADERS
    case_config = cargar_json_s3(CONFIG_BUCKET, CONFIG_KEY)
    _CASE_CONFIG_FOR_HEADERS = case_config

    ENVIRONMENT = global_config["environment"]
    REGION = global_config.get("region", s3.meta.region_name)
    TIMEZONE = global_config.get("timezone", "America/Bogota")
    GLUE_CATALOG = global_config.get("catalog", "AwsDataCatalog")

    ASSETS_BUCKET = global_config["buckets"]["assets"]
    REPORT_BUCKET = global_config["buckets"]["reports"]

    NOTIFICATION_DYNAMODB_TABLE = global_config["dynamodb"]["notification_table"]

    ATHENA_OUTPUT_PATH = global_config["athena"]["output_path"]
    ATHENA_UNLOAD_PATH = global_config["athena"]["unload_path"]
    ATHENA_OUTPUT_LOCATION = f"s3://{ASSETS_BUCKET}/{ATHENA_OUTPUT_PATH}/"

    CONSOLE_BASE_URL = global_config["console_url"]["base_url"]
    ACCOUNT_ID = global_config["console_url"]["account_id"]
    ROLE_NAME = global_config["console_url"]["role_name"]
    CONSOLE_REGION = global_config["console_url"].get("region", REGION)

    DEFAULT_USER_EMAIL = global_config["certification"].get(
        "default_user_email",
        "SYSTEM_GLUE_JOB"
    )

    STATUS_PROCESSED = global_config["certification"].get(
        "processed_status",
        "PROCESSED"
    )

    STATUS_PENDING = global_config["certification"].get(
        "pending_status",
        "PENDING"
    )

    CSV_CONTENT_TYPE = global_config["s3_export"].get(
        "csv_content_type",
        "text/csv; charset=utf-8"
    )

    CSV_ENCODING_BOM = global_config["s3_export"].get(
        "csv_encoding_bom",
        True
    )

    validar_case_config(case_config)

    return global_config, case_config


def validar_case_config(config: dict):
    use_case_id = config.get("useCaseId")
    email_type = config.get("emailType")

    if not use_case_id:
        raise ValueError("El JSON del caso no tiene useCaseId")

    if not email_type:
        raise ValueError("El JSON del caso no tiene emailType")

    if not config.get("queries", {}).get("detail"):
        raise ValueError(
            f"El caso {use_case_id} no tiene queries detail configurados"
        )


# ============================================================
# HELPERS GENERALES
# ============================================================

def tz():
    return ZoneInfo(TIMEZONE or "America/Bogota")


def parametrizar_query(texto: str, variables: dict) -> str:
    if texto is None:
        return texto

    texto = html.unescape(str(texto))
    return Template(texto).safe_substitute(variables)


def parametrizar_texto(texto: str, variables: dict) -> str:
    if texto is None:
        return texto

    return Template(str(texto)).safe_substitute(variables)


def parametrizar_query_dict(data, variables):
    if isinstance(data, dict):
        return {
            k: parametrizar_query_dict(v, variables)
            for k, v in data.items()
        }

    if isinstance(data, list):
        return [
            parametrizar_query_dict(v, variables)
            for v in data
        ]

    if isinstance(data, str):
        return parametrizar_query(data, variables)

    return data


def formatear_fecha(period: str) -> str:
    try:
        year = int(period[:4])
        month = int(period[4:6])

        first_day = datetime(year, month, 1).strftime("%d/%m/%Y")
        last_day_num = calendar.monthrange(year, month)[1]
        last_day = datetime(year, month, last_day_num).strftime("%d/%m/%Y")

        return f"{first_day} - {last_day}"

    except Exception as e:
        logger.error(f"Error formateando periodo {period}: {str(e)}")
        raise


def obtener_queries_detail(config: dict) -> list:
    detail_queries = config.get("queries", {}).get("detail", [])

    return sorted(
        detail_queries,
        key=lambda x: x.get("queryName", "")
    )


def generar_cert_code(period: str, email_type: str):
    if str(email_type).lower() != "cert":
        return None

    now = datetime.now(tz())

    if REGENERACION_SIN_DATOS_NUEVOS:
        return f"{period}-{now.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex}"
    return f"{period}-{now.strftime('%Y%m%d-%H%M')}"


# ============================================================
# ATHENA
# ============================================================

def athena_run_raw(database: str, query: str) -> str:
    try:
        logger.info(f"[ATHENA] Database: {database}")
        logger.info(f"[ATHENA] Query final:\n{query}")

        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                "Catalog": GLUE_CATALOG,
                "Database": database
            },
            ResultConfiguration={
                "OutputLocation": ATHENA_OUTPUT_LOCATION
            }
        )

        athena_execution_id = response["QueryExecutionId"]

        logger.info(f"[ATHENA] QueryExecutionId={athena_execution_id}")

        while True:
            execution = athena.get_query_execution(
                QueryExecutionId=athena_execution_id
            )

            status = execution["QueryExecution"]["Status"]["State"]

            if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break

            time.sleep(2)

        if status != "SUCCEEDED":
            reason = execution["QueryExecution"]["Status"].get(
                "StateChangeReason",
                ""
            )

            logger.error(f"[ATHENA] Estado: {status}")
            logger.error(f"[ATHENA] Motivo: {reason}")
            logger.error(f"[ATHENA] Query fallida:\n{query}")

            raise RuntimeError(
                f"Athena falló con estado {status}: {reason}"
            )

        logger.info(
            f"[ATHENA] Query ejecutada correctamente: {athena_execution_id}"
        )

        return athena_execution_id

    except Exception:
        logger.exception("[ATHENA] Error ejecutando query")
        raise


# ============================================================
# S3 EXPORT
# ============================================================

def construir_url_consola_s3(
    use_case_id,
    name_file,
    expiracion=604800
):
    from urllib.parse import quote

    bucket = REPORT_BUCKET
    key = f"{use_case_id}/{name_file}"

    console_base = f"https://{CONSOLE_REGION}.console.aws.amazon.com/"
    sso_base = "https:" + "//ssoath-lz.awsapps.com/start/#/console"

    destination = (
        console_base
        + f"s3/object/{bucket}"
        + f"?region={CONSOLE_REGION}"
        + f"&prefix={quote(key, safe='/')}"
    )

    return (
        sso_base
        + f"?account_id={ACCOUNT_ID}"
        + f"&role_name={ROLE_NAME}"
        + f"&destination={quote(destination, safe='')}"
    )

def exportar_csv_raw(use_case_id: str, query_name: str, execution_id: str, period: str, database=None, query=None, athena_id=None):
    try:
        empty_csv = b''
        _RESULTADOS_SIN_DATOS[execution_id] = False
        unload_prefix = f'{ATHENA_UNLOAD_PATH}/{execution_id}/'
        ts = datetime.now(tz()).strftime('%Y%m%d-%H%M%S')
        query_name_norm = query_name.replace('#', '_').replace(' ', '_')
        name_file = f'{period}-{query_name_norm}-{ts}.csv'
        carpetas_certificacion = {'Previsional': 'Previsional', 'auxilioFunerario': 'auxiliosFunerario', 'auxiliosFunerario': 'auxiliosFunerario', 'primasArl': 'primasArl', 'primasRentasEndosos': 'primasRentasEndosos', 'reservaMatematicaRvi': 'reservaMatematicaRviArp', 'reservaMatematicaRviArp': 'reservaMatematicaRviArp', 'primasPCR': 'primasPCR'}
        if use_case_id not in carpetas_certificacion:
            raise ValueError('useCaseId sin carpeta autorizada: ' + str(use_case_id))
        dest_key = f'{carpetas_certificacion[use_case_id]}/{name_file}'
        logger.info(f'[S3] Buscando archivos UNLOAD en s3://{ASSETS_BUCKET}/{unload_prefix}')
        paginator = s3.get_paginator('list_objects_v2')
        data_keys = []
        for page in paginator.paginate(Bucket=ASSETS_BUCKET, Prefix=unload_prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('/'):
                    continue
                lower_key = key.lower()
                if 'manifest' in lower_key or 'metadata' in lower_key:
                    continue
                data_keys.append(key)
        if not data_keys:
            empty_csv = _csv_sin_datos_confirmado(database, query, athena_id, f's3://{ASSETS_BUCKET}/{unload_prefix}')
            _RESULTADOS_SIN_DATOS[execution_id] = True
        data_keys.sort()
        out_csv = io.BytesIO()
        if CSV_ENCODING_BOM:
            out_csv.write(b'\xef\xbb\xbf')
        out_csv.write(empty_csv)
        for key in data_keys:
            logger.info(f'[S3] Leyendo fragmento: s3://{ASSETS_BUCKET}/{key}')
            obj = s3.get_object(Bucket=ASSETS_BUCKET, Key=key)
            chunk = obj['Body'].read()
            if len(chunk) >= 2 and chunk[0] == 31 and (chunk[1] == 139):
                chunk = gzip.decompress(chunk)
            out_csv.write(chunk)
        s3.put_object(Bucket=REPORT_BUCKET, Key=dest_key, Body=out_csv.getvalue(), ContentType=CSV_CONTENT_TYPE)
        s3_url_reference = f's3://{REPORT_BUCKET}/{dest_key}'
        csv_url = construir_url_consola_s3(carpetas_certificacion[use_case_id], name_file)
        logger.info(f'[S3] CSV final generado: {s3_url_reference}')
        logger.info(f'[S3] URL consola: {csv_url}')
        return (dest_key, csv_url, name_file)
    except Exception:
        logger.exception('[S3] Error exportando CSV final')
        raise


# ============================================================
# DYNAMO RUNTIME
# ============================================================

def registrar_notification_item(notify_table, use_case_id: str, execution_id: str, cert_code: str, period: str, email_type: str, query_name: str, s3_url: str, name_file: str):
    now = datetime.now(tz()).isoformat()
    item = {'useCaseId': use_case_id, 'executionId': execution_id, 's3_url': s3_url, 'status': STATUS_PROCESSED, 'period': period, 'emailType': email_type, 'queryName': query_name, 'userEmail': DEFAULT_USER_EMAIL, 'nameFile': name_file, 'creationDate': now, 'configBucket': CONFIG_BUCKET, 'configKey': CONFIG_KEY, 'globalConfigKey': GLOBAL_CONFIG_KEY}
    if cert_code:
        item['certCode'] = cert_code
    item['regeneracionSinDatosNuevos'] = REGENERACION_SIN_DATOS_NUEVOS
    item['sinDatosPeriodo'] = _RESULTADOS_SIN_DATOS.get(execution_id, False)
    notify_table.put_item(Item=item)
    logger.info(f'[DYNAMO] Registro {STATUS_PROCESSED} creado. useCaseId={use_case_id}, executionId={execution_id}, queryName={query_name}, certCode={cert_code}, configBucket={CONFIG_BUCKET}, configKey={CONFIG_KEY}, globalConfigKey={GLOBAL_CONFIG_KEY}')


def marcar_certcode_pendiente(
    notification_table_name: str,
    use_case_id: str,
    cert_code: str,
    max_intentos: int = 6,
    espera_segundos: int = 2
):
    table = dynamodb.Table(notification_table_name)

    items = []

    for intento in range(1, max_intentos + 1):
        response = table.query(
            IndexName="certCode-index",
            KeyConditionExpression=Key("certCode").eq(cert_code)
        )

        items = [
            item
            for item in response.get("Items", [])
            if item.get("useCaseId") == use_case_id
        ]

        if items:
            logger.info(
                f"[DYNAMO] certCode visible en certCode-index. "
                f"useCaseId={use_case_id}, certCode={cert_code}, "
                f"intento={intento}/{max_intentos}, items={len(items)}"
            )
            break

        if intento < max_intentos:
            logger.warning(
                f"[DYNAMO] certCode todavía no visible en certCode-index. "
                f"useCaseId={use_case_id}, certCode={cert_code}, "
                f"intento={intento}/{max_intentos}. "
                f"Reintentando en {espera_segundos} segundos."
            )
            time.sleep(espera_segundos)

    if not items:
        raise RuntimeError(
            f"No se encontraron items para useCaseId={use_case_id}, "
            f"certCode={cert_code} después de {max_intentos} intentos"
        )

    now = datetime.now(tz()).isoformat()
    actualizados = 0

    for item in items:
        table.update_item(
            Key={
                "useCaseId": item["useCaseId"],
                "executionId": item["executionId"]
            },
            UpdateExpression=(
                "SET #st = :pending, "
                "pendingDate = :pendingDate, "
                "configBucket = :configBucket, "
                "configKey = :configKey, "
                "globalConfigKey = :globalConfigKey"
            ),
            ExpressionAttributeNames={
                "#st": "status"
            },
            ExpressionAttributeValues={
                ":pending": STATUS_PENDING,
                ":pendingDate": now,
                ":configBucket": CONFIG_BUCKET,
                ":configKey": CONFIG_KEY,
                ":globalConfigKey": GLOBAL_CONFIG_KEY
            }
        )

        actualizados += 1

    logger.info(
        f"[DYNAMO] Registros marcados como {STATUS_PENDING}: {actualizados}. "
        f"useCaseId={use_case_id}, certCode={cert_code}, "
        f"configBucket={CONFIG_BUCKET}, "
        f"configKey={CONFIG_KEY}, "
        f"globalConfigKey={GLOBAL_CONFIG_KEY}"
    )


# ============================================================
# PROCESO PRINCIPAL
# ============================================================

def generar_archivos_desde_json(config: dict, general_params: dict):
    notify_table = dynamodb.Table(NOTIFICATION_DYNAMODB_TABLE)
    detail_queries = obtener_queries_detail(config)
    if not detail_queries:
        raise RuntimeError(f"No existen queries detail para useCaseId={general_params['useCaseId']}")
    cert_code = generar_cert_code(general_params['period'], general_params['emailType'])
    logger.info(f'[CERT] certCode generado: {cert_code}')
    for query_data in detail_queries:
        execution_id = str(uuid.uuid4())
        query_name = query_data['queryName']
        variables = {**general_params, 'execution_id': execution_id, 'assets_bucket': ASSETS_BUCKET, 'report_bucket': REPORT_BUCKET, 'env': ENVIRONMENT, 'unload_path': ATHENA_UNLOAD_PATH}
        query_data_p = parametrizar_query_dict(query_data, variables)
        database = query_data_p.get('database') or config.get('athena', {}).get('defaultDatabase')
        if not database:
            raise RuntimeError(f'No se encontró database para query {query_name}')
        query = query_data_p['query']
        athena_id = athena_run_raw(database, query)
        dest_key, final_url, name_file = exportar_csv_raw(use_case_id=general_params['useCaseId'], query_name=query_name, execution_id=execution_id, period=general_params['period'], database=database, query=query, athena_id=athena_id)
        registrar_notification_item(notify_table=notify_table, use_case_id=general_params['useCaseId'], execution_id=execution_id, cert_code=cert_code, period=general_params['period'], email_type=general_params['emailType'], query_name=query_name, s3_url=final_url, name_file=name_file)
        logger.info(f'[CERT] Query procesada correctamente: {query_name}. Archivo={name_file}')
    if cert_code:
        marcar_certcode_pendiente(notification_table_name=NOTIFICATION_DYNAMODB_TABLE, use_case_id=general_params['useCaseId'], cert_code=cert_code)
    return cert_code


# ============================================================
# MAIN
# ============================================================

def main():
    try:
        global_config, config = cargar_configs()

        use_case_id = config["useCaseId"]
        email_type = config["emailType"].lower()
        period_range = formatear_fecha(PERIOD)

        logger.info(
            f"[START] Job={JOB_NAME}, useCaseId={use_case_id}, "
            f"period={PERIOD}, emailType={email_type}, env={ENVIRONMENT}, "
            f"CONFIG_BUCKET={CONFIG_BUCKET}, "
            f"CONFIG_KEY={CONFIG_KEY}, "
            f"GLOBAL_CONFIG_KEY={GLOBAL_CONFIG_KEY}"
        )

        general_params = {
            "useCaseId": use_case_id,
            "period": PERIOD,
            "periodRange": period_range,
            "env": ENVIRONMENT,
            "emailType": email_type
        }

        cert_code = generar_archivos_desde_json(config, general_params)

        logger.info(
            f"[END] Proceso finalizado correctamente. "
            f"useCaseId={use_case_id}, certCode={cert_code}"
        )

        job.commit()

    except Exception:
        logger.exception("[ERROR] Falló el job de certificación")
        job.commit()
        raise



# SIN_DATOS_V1: la marca corresponde al resultado de cada consulta, no a regeneracion.
_RESULTADOS_SIN_DATOS = {}

def _csv_sin_datos_confirmado(database, query, athena_id, expected_uri):
    import csv
    import io
    import re
    from urllib.parse import urlparse
    execution = athena.get_query_execution(QueryExecutionId=athena_id)['QueryExecution']
    if execution['Status']['State'] != 'SUCCEEDED':
        raise RuntimeError('No se admite resultado sin datos de una consulta fallida')
    manifest = execution.get('Statistics', {}).get('DataManifestLocation')
    if not manifest:
        raise RuntimeError('Sin manifiesto UNLOAD: no se puede confirmar resultado vacio')
    location = urlparse(manifest)
    raw = s3.get_object(Bucket=location.netloc, Key=location.path.lstrip('/'))['Body'].read()
    if raw.strip():
        raise RuntimeError('Manifiesto no vacio pero faltan archivos UNLOAD; conservar error tecnico')
    opening = re.match(r'\s*UNLOAD\s*\(', query, flags=re.I)
    if not opening:
        raise RuntimeError('Formato UNLOAD no reconocido; no se modifica el resultado')
    # Lexer: no contar parentesis dentro de literales, identificadores o comentarios.
    token = re.compile(
        '\'(?:\'\'|[^\'])*\'|\\"(?:\\"\\"|[^\\"])*\\"|--[^\\n]*(?:\\n|$)|/\\*.*?\\*/|[^\'\\"()/\\-]+|.',
        re.S
    )
    depth = 1
    closing = None
    for match in token.finditer(query, opening.end()):
        if match.group() == '(':
            depth += 1
        elif match.group() == ')':
            depth -= 1
            if depth == 0:
                closing = match.start()
                break
    if closing is None:
        raise RuntimeError('UNLOAD sin cierre reconocido')
    target = re.match(r"\s*TO\s+'((?:''|[^'])*)'", query[closing+1:], flags=re.I)
    if not target or target.group(1).replace("''", "'") != expected_uri:
        raise RuntimeError('Destino UNLOAD distinto del prefijo buscado')
    inner = query[opening.end():closing].strip()
    probe_id = athena_run_raw(database, 'SELECT * FROM (\n' + inner + '\n) AS niif_sin_datos LIMIT 1')
    result = athena.get_query_results(QueryExecutionId=probe_id, MaxResults=2)
    rows = result['ResultSet'].get('Rows', [])
    if len(rows) > 1 or result.get('NextToken'):
        raise RuntimeError('La comprobacion encontro datos; no se crea archivo vacio')
    columns = result['ResultSet'].get('ResultSetMetadata', {}).get('ColumnInfo', [])
    if not columns:
        raise RuntimeError('Sin metadatos de columnas; no se genera CSV')
    buffer = io.StringIO(newline='')
    csv.writer(buffer).writerow([c['Name'] for c in columns])
    return buffer.getvalue().encode('utf-8')

if __name__ == "__main__":
    main()
