import json
import boto3
import os
import logging
import time
from botocore.exceptions import ClientError

# Logger
logger = logging.getLogger()
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logger.setLevel(log_level)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Clientes AWS
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
athena = boto3.client('athena', region_name=AWS_REGION)
s3 = boto3.client('s3', region_name=AWS_REGION)

# Variables entorno
TABLE_CERTIFICATIONS = os.getenv('DYNAMODB_CERT_TABLE', 'datalake-dev-use-case-certification')
BUCKET_NAME = os.getenv('BUCKET_NAME', 'alfa-datalake-dev-landing-prophet')
S3_PREFIX = 'landing/prophets/'
OUTPUT_ATHENA_PREFIX = 'athena-query-results/'

cert_table = dynamodb.Table(TABLE_CERTIFICATIONS)

def wait_query_execution(athena_client, query_execution_id):
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']

        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            if status == 'FAILED':
                reason = response['QueryExecution']['Status'].get('StateChangeReason', 'No reason provided')
                raise Exception(f"Athena query failed: {reason}")
            elif status == 'CANCELLED':
                raise Exception("Athena query was cancelled")
            else:
                return True
        else:
            time.sleep(1)

def lambda_handler(event, context):
    logger.info(f"Evento recibido: {json.dumps(event)}")

    use_case_id = event.get('useCaseId')
    cert_code = event.get('certCode')

    if not use_case_id or not cert_code:
        return {
            "statusCode": 400,
            "valid": False,
            "message": "Faltan parámetros obligatorios: useCaseId y certCode"
        }

    try:
        # Paso 1: Obtener period de DynamoDB
        cert_response = cert_table.get_item(Key={'useCaseId': use_case_id, 'certCode': cert_code})
        cert_item = cert_response.get('Item')
        if not cert_item:
            return {
                "statusCode": 404,
                "valid": False,
                "message": "No se encontró la certificación"
            }
        period = cert_item.get('period')
        if not period:
            return {
                "statusCode": 500,
                "valid": False,
                "message": "El ítem de certificación no contiene el campo 'period'"
            }

        # Paso 2: Armar y ejecutar consulta Athena con el period dinámico
        sql_query = f"""
            SELECT numero_de_poliza,
    periodo_de_expedicion_de_la_poliza,
    origen_de_la_renta,
    valor_auxilio_funerario_pagado,
    mes_de_pago
            FROM "datalake-dev-gold-catalog-db"."niif_auxilios_funerarios_gold" 
            WHERE "mes_de_pago" = '$period'
        """

        logger.info(f"Ejecutando consulta Athena: {sql_query}")

        response = athena.start_query_execution(
            QueryString=sql_query,
            QueryExecutionContext={'Database': 'datalake-dev-gold-catalog-db'},
            ResultConfiguration={'OutputLocation': f's3://{BUCKET_NAME}/{OUTPUT_ATHENA_PREFIX}'}
        )

        query_execution_id = response['QueryExecutionId']

        # Esperar que termine
        wait_query_execution(athena, query_execution_id)

        # Paso 3: Obtener resultados (puedes cambiar esto para guardar el CSV directamente)
        result_response = athena.get_query_results(QueryExecutionId=query_execution_id)

        # Convertir resultados a texto plano (CSV o tabulado)
        rows = result_response['ResultSet']['Rows']
        headers = [col['VarCharValue'] for col in rows[0]['Data']]
        data_lines = []
        for row in rows[1:]:
            data = [col.get('VarCharValue', '') for col in row['Data']]
            data_lines.append(' '.join(data))
        csv_content = ' '.join(headers) + '\n' + '\n'.join(data_lines)

        # Paso 4: Guardar archivo en S3
        file_name = f"{use_case_id}-{period}.csv"
        s3_key = f"{S3_PREFIX}{file_name}"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=csv_content.encode('utf-8'),
            ContentType='text/csv'
        )

        logger.info(f"Archivo {s3_key} subido correctamente al bucket {BUCKET_NAME}")

        return {
            "statusCode": 200,
            "valid": True,
            "message": "Consulta Athena ejecutada y archivo generado exitosamente.",
            "s3File": f"s3://{BUCKET_NAME}/{s3_key}"
        }

    except ClientError as e:
        logger.error(f"Error de AWS: {e}")
        return {
            "statusCode": 500,
            "valid": False,
            "message": f"Error de AWS: {e.response['Error']['Message']}"
        }
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        return {
            "statusCode": 500,
            "valid": False,
            "message": f"Error inesperado: {str(e)}"
        }
