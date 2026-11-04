import os
import boto3
import json
import logging
import time
import calendar
import pandas as pd
import botocore.exceptions
from io import StringIO
from string import Template
from datetime import datetime
from boto3.dynamodb.conditions import Key
from datetime import timezone
from zoneinfo import ZoneInfo

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

aws_region = os.getenv('AWS_REGION', 'us-east-1')
session = boto3.session.Session(region_name=aws_region)

dynamodb = session.resource("dynamodb")
athena = session.client("athena")
s3 = session.client("s3")
ses = session.client("ses")

ENVIRONMENT = os.environ["ENVIRONMENT"]
ASSETS_BUCKET = os.environ["ASSETS_BUCKET"]
REPORT_BUCKET = os.getenv("REPORT_BUCKET")
EMAIL_DYNAMODB_TABLE = os.environ["EMAIL_DYNAMODB_TABLE"]
QUERY_DYNAMODB_TABLE = os.environ["QUERY_DYNAMODB_TABLE"]
CERTIFICATION_DYNAMODB_TABLE = os.environ["CERTIFICATION_DYNAMODB_TABLE"]

GLUE_CATALOG = "AwsDataCatalog"

ATHENA_OUTPUT_PATH = "athena/notifications"
ATHENA_OUTPUT_LOCATION = f"s3://{ASSETS_BUCKET}/{ATHENA_OUTPUT_PATH}"

def parametrizar_dict(original_dict, variables):
    resultado = {}
    for key, value in original_dict.items():
        if isinstance(value, str) and "$" in value:
            resultado[key] = Template(value).safe_substitute(variables)
        else:
            resultado[key] = value
    return resultado

def formatear_fecha(period):
    """
    Dado un período en formato 'yyyymm', retorna una tupla con la fecha del primer y último día del mes.
    """
    try:
        year = int(period[:4])
        month = int(period[4:6])

        firstDay = datetime(year, month, 1).strftime("%d/%m/%Y")

        lastDayNum = calendar.monthrange(year, month)[1]
        lastDay = datetime(year, month, lastDayNum).strftime("%d/%m/%Y")

        periodRange = f"{firstDay} - {lastDay}"

        return periodRange

    except Exception as e:
        logger.error(f"Error formateando el periodo: {str(e)}", exc_info=True)
        raise

def consulta_dynamodb(tableName, primaryKey, primaryValue, sortKey, sortValue):
    """Realizar consulta de item de tabla dynamodb."""
    try:
        logger.info(f"ℹ️ Realizando consulta en tabla dynamodb: {tableName}.")

        table = dynamodb.Table(tableName)
        response = table.query(
            KeyConditionExpression=Key(primaryKey).eq(primaryValue) & Key(sortKey).eq(sortValue)
        )

        items = response.get("Items", [])

        if len(items) == 0:
            logger.warning(f"No se encontró ningún item con primaryKey: {primaryKey}, sortKey: {primaryValue}")
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'No se encontró ningún item con esos valores'})
            }

        if len(items) > 1:
            logger.warning(f"Se encontraron múltiples items para primaryKey: {primaryKey}, sortKey: {primaryValue}")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Se encontraron múltiples items, lo cual no es esperado'})
            }

        return items[0]

    except Exception as e:
        logger.error(f"Error ejecutando consulta en Dymanodb: {str(e)}", exc_info=True)
        raise

def consulta_athena(database, query):
    """Realizar consulta en catalogo de glue con athena."""
    try:
        logger.info(f"ℹ️ Realizando consulta en athena.")
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                "Catalog": GLUE_CATALOG,
                "Database": database},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
        )

        query_execution_id = response['QueryExecutionId']
        logger.info(f"ℹ️ Consulta lanzada: {query_execution_id}")

        while True:
            estado = athena.get_query_execution(QueryExecutionId=query_execution_id)
            estado_consulta = estado['QueryExecution']['Status']['State']

            if estado_consulta in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break

            time.sleep(1)

        if estado_consulta != 'SUCCEEDED':
            raise Exception(f"Consulta fallida: {estado_consulta}")

        key = f"{ATHENA_OUTPUT_PATH}/{query_execution_id}.csv"
        csv_obj = s3.get_object(Bucket=ASSETS_BUCKET, Key=key)
        body = csv_obj['Body'].read().decode('utf-8')

        df = pd.read_csv(StringIO(body))
        return df

    except Exception as e:
        logger.error(f"Error ejecutando consulta en Athena: {str(e)}", exc_info=True)
        raise

def formatear_columnas_df(df, definiciones_columnas):
    """
    Aplica formato a columnas de un DataFrame según definiciones recibidas.
    Soporta: money, percent, date.
    """
    df_copy = df.copy()

    for col, tipo in definiciones_columnas.items():
        if col not in df_copy.columns:
            logger.warning(f"⚠️ Columna {col} no existe en el resultado de Athena.")
            continue

        if tipo == "money":
            df_copy[col] = df_copy[col].apply(
                lambda x: f"$ {int(round(float(x))):,}".replace(",", ".") if pd.notnull(x) else ""
            )
        elif tipo == "percent":
            df_copy[col] = df_copy[col].apply(
                lambda x: f"{round(float(x)*100, 2)}%" if pd.notnull(x) else ""
            )
        elif tipo == "date":
            df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce').dt.strftime('%d/%m/%Y')
        else:
            logger.warning(f"⚠️ Tipo de dato '{tipo}' no soportado para columna {col}")

    return df_copy

def generar_tablas(tablesDict, tableParams, generalParams):
    """Create downloadable or formatted HTML table variables using Athena queries and metadata definitions."""
    results = {}

    for varName, tableInfo in tablesDict.items():
        if isinstance(tableInfo, str):
            queryName = tableInfo
            columnDefinitions = {}
            asText = False
            exportEnabled = False
        elif isinstance(tableInfo, dict):
            queryName = tableInfo.get("queryName")
            asText = tableInfo.get("asText", False)
            exportEnabled = tableInfo.get("export", False)
            columnDefinitions = {
                col: {
                    "name": props.get("name"),
                    "type": props.get("type")
                }
                for col, props in tableInfo.get("columns", {}).items()
            }
        else:
            logger.warning(f"⚠️ Invalid structure for table '{varName}'")
            results[varName] = f"<p>Invalid structure for table '{varName}'.</p>"
            continue

        queryData = consulta_dynamodb(
            tableParams["tableName"],
            tableParams["primaryKey"],
            tableParams["primaryValue"],
            tableParams["sortKey"],
            queryName,
        )

        if not queryData:
            logger.warning(f"No query found for {queryName}")
            results[varName] = f"<p>No query found for {queryName}.</p>"
            continue

        queryData = parametrizar_dict(queryData, generalParams)
        df = consulta_athena(queryData.get("database"), queryData.get("query"))

        if df.empty:
            logger.warning(f"⚠️ Query {queryName} returned no results.")
            results[varName] = f"<p>Query {queryName} returned no results.</p>"
            continue

        expectedColumns = set(columnDefinitions.keys())
        presentColumns = set(df.columns)
        missingColumns = expectedColumns - presentColumns

        if missingColumns:
            logger.warning(f"⚠️ Missing columns in {queryName}: {', '.join(missingColumns)}")
            results[varName] = f"<p>Missing columns in query {queryName}: {', '.join(missingColumns)}</p>"
            continue

        typeMap = {col: defs["type"] for col, defs in columnDefinitions.items() if "type" in defs}
        df = formatear_columnas_df(df, typeMap)

        renameMap = {col: defs["name"] for col, defs in columnDefinitions.items() if "name" in defs}
        df.rename(columns=renameMap, inplace=True)

        if exportEnabled:
            try:
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                s3Key = f"exports/{tableParams["primaryValue"]}/{queryName}/{timestamp}.xlsx"

                tempFilePath = "/tmp/temp.xlsx"
                df.to_excel(tempFilePath, index=False, sheet_name="Sheet1")

                with open(tempFilePath, "rb") as fileData:
                    s3.upload_fileobj(fileData, REPORT_BUCKET, s3Key)

              #  presignedUrl = s3.generate_presigned_url(
               #     ClientMethod="get_object",
               #     Params={"Bucket": REPORT_BUCKET, "Key": s3Key},
               #     ExpiresIn=3600
               # )

                    presignedUrl =  """https://ssoath-lz.awsapps.com/start/#/console?account_id=533267035849&role_name=ALFA-DEV-DATALAKE-ADM&destination=https%3A%2F%2Fus-east-1.console.aws.amazon.com%2Fs3%2Fbuckets%2Falfa-datalake-dev-niif-con%3Fprefix%3Dexports%252FrentasVitalicias%252Fdetail%252F%26region%3Dus-east-1%26bucketType%3Dgeneral%26tab%3Dobjects"""
                    results[varName] = f'<p><a href="{presignedUrl}" target="_blank">📥 Download Excel</a></p>'
                    continue

               

            except Exception as e:
                logger.error(f"❌ Failed to export table {varName} to Excel: {e}")
                results[varName] = "<p>Error generating Excel export.</p>"
                continue

        if asText:
            if df.shape[0] == 1:
                row = df.iloc[0]
                text = "<br>".join([
                    f"<strong>{col}</strong>: {row[col]}"
                    for col in df.columns
                    if pd.notna(row[col]) and str(row[col]).strip() != ""
                ])
                results[varName] = f"<p>{text}</p>"
            else:
                logger.warning(f"⚠️ Table '{varName}' cannot be shown as text: incorrect dimensions.")
                results[varName] = (
                    "<p>Cannot render as plain text because the table does not have the required dimensions.</p>"
                )
        else:
            results[varName] = df.to_html(index=False, border=0, classes='dataframe', escape=False)

    return results



def obtener_template_desde_s3(bucket, key):
    """Descarga el template HTML desde S3 y lo devuelve como string."""
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8")

def lambda_handler(event, _context):
    required_params = ["useCaseId", "period", "emailType"]

    missing_params = [param for param in required_params if param not in event or event[param] is None]
    if missing_params:
        logger.error(f"Faltan los siguientes parámetros requeridos: {', '.join(missing_params)}")
        return {"statusCode": 400, "message": f"Faltan parámetros: {', '.join(missing_params)}"}

    useCaseId = str(event["useCaseId"])
    emailType = str(event["emailType"]).lower()
    period = str(event["period"])

    try:
        periodRange = formatear_fecha(period)
        params = {"useCaseId": useCaseId, "period": period, "periodRange": periodRange, "env": ENVIRONMENT}

        certRequired = (emailType == "cert")
        certCode = None
        now = None

        if certRequired:
            now = datetime.now(ZoneInfo("America/Bogota"))
            current_datetime = now.strftime("%Y%m%d-%H%M")
            certCode = f"{period}-{current_datetime}"
            params["certCode"] = certCode

        emailData = consulta_dynamodb(EMAIL_DYNAMODB_TABLE, "useCaseId", useCaseId, "emailType", emailType)
        emailData = parametrizar_dict(emailData, params)
        logger.debug(f"🐞 Parametros de envío de correo: {emailData}")

        queryTable = {
            "tableName": QUERY_DYNAMODB_TABLE,
            "primaryKey": "useCaseId",
            "primaryValue": useCaseId,
            "sortKey": "queryName",
        }

        tables = generar_tablas(emailData.get("tableVars"), queryTable, params)

        template_html = obtener_template_desde_s3(ASSETS_BUCKET, emailData.get("templatePath"))
        template = Template(template_html)

        stringVars_parametrizado = parametrizar_dict(emailData.get("stringVars", {}), params)
        allParams = {**params, **tables, **stringVars_parametrizado}

        subject = emailData.get("subject")
        source = emailData.get("sender")
        destinations = emailData.get('destinations')
        body_html = template.safe_substitute(allParams)

        logger.debug(f"🐞 Cuerpo del correo:\n{body_html}")

        response = ses.send_email(
            Source=source,
            Destination={"ToAddresses": destinations},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Html": {"Data": body_html}},
            },
            ReplyToAddresses=["no-reply@tudominio.com"]
        )

        logger.info(f"✅ Correo enviado exitosamente a {destinations}")

        if certRequired:
            certification_item = {
                "useCaseId": useCaseId,
                "certCode": certCode,
                "certStatus": "",
                "creationDate": now.isoformat(),
                "period": period,
                "userEmail": ""
            }
            logger.info(f"ℹ️ Insertando registro de certificación: {certification_item}")

            table = dynamodb.Table(CERTIFICATION_DYNAMODB_TABLE)
            table.put_item(Item=certification_item)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'Status': 'Enviado',
                'Subject': subject,
                'Destination': destinations,
            }, ensure_ascii=False, default=str)
        }

    except botocore.exceptions.BotoCoreError as e:
        logger.error(f"Error en boto3: {str(e)}")
        return {"status": "error", "message": f"Error en AWS SDK: {str(e)}"}

    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}", exc_info=True)
        return {"status": "error", "message": f"Error inesperado: {str(e)}"}
