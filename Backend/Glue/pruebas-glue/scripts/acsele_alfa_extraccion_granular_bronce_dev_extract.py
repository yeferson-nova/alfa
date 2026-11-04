"""
Script that loads bronce phase and uploads extracted data to user-defined S3 location in Parquet format sequentially
"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.ExtractHelper import ExtractHelper
from commons.LogsHelper import LogsHelper
from pyspark.sql import functions as F

import sys, traceback, boto3, json, os, shutil

# ========================== USER-DEFINED S3 PATH ==========================
USER_DEFINED_S3_PATH = "s3://alfa-datalake-dev-bronce/ACSELE/ACSELE_ALFA/FINANCIALPLANDCO_PRUEBAS"  # <-- Modifica esta línea con la ruta de S3 deseada
# ========================== SET ARGS AND CONFIG COMMONS UTILS ==========================

logs = LogsHelper()
logger = logs.logger_conf()

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "DB_SCHEMA",
        "DB_NAME",
        "BUCKET_NAME",
        "BUCKET_NAME_ASSETS",
        "CONNECT_NAME",
        "PERIOD",
        "CERTIFICATE",
    ]
)

DB_SCHEMA = args["DB_SCHEMA"]
DB_NAME = args["DB_NAME"]
BUCKET_NAME = args["BUCKET_NAME"]
BUCKET_NAME_ASSETS = args["BUCKET_NAME_ASSETS"]
CONNECT_NAME = args["CONNECT_NAME"]
PERIOD = args["PERIOD"]
CERTIFICATE = args["CERTIFICATE"]

LOCAL_TEMP_PATH = "/tmp/extracted_data/"

# Crear carpeta temporal si no existe
if not os.path.exists(LOCAL_TEMP_PATH):
    os.makedirs(LOCAL_TEMP_PATH)

logger.info(f"Period: {PERIOD}")
logger.info(f"Schema: {DB_SCHEMA}")
logger.info(f"Connect: {CONNECT_NAME}")
logger.info(f"Bucket_name: {BUCKET_NAME}")
logger.info(f"User Defined S3 Path: {USER_DEFINED_S3_PATH}")

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3_client = boto3.client("s3")

extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME, DB_NAME)

query = f"SELECT * FROM ACSELE_ALFA.policydco"
#( select * from ACSELE_ALFA.thirdpartymovementpolicy where PK >= 5643141670) WHERE PK <= 5787224785"
logger.info(f"Executing query")

table_info = {
    "table": "POLICYDCO",
    "fields": [
        "AGREGATEDOBJECTID",
        "OPERATIONPK",
        "DCOID",
        "STATEID",
        "INITIALDATE",
        "FINISHDATE",
        "TIME_STAMP",
        "AUDITDATE",
        "EVENT",
        "IDDCOEVENT",
        "AGREGATEDPARENTID",
        "SUB_STATUS",
        "POD_AUTOMATICCOLLECTION",
        "STATUS",
        "POD_VALIDITY"
    ],
    "partition_column": "-",
    "logic": query
}

try:
    extract_helper.execute_data(
        table_info['table'], 
        table_info['partition_column'],  
        table_info['logic'], 
        connectname= CONNECT_NAME)
    logger.info(f"Successfully Extracted: Query executed")
except Exception as e:
    logger.error(f"ERROR WITH INGEST: {e}")
        
