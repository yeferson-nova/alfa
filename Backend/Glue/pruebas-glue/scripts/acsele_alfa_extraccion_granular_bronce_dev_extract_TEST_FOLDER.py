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
 
# Lista de valores límite para las consultas
limit_values = [5643141670,5787224785,5931458400,6075688869,6219855111,6364005787,6508091483,6652134718,6796206604]


 
# Generar consultas dinámicamente según la cantidad de elementos en la lista
for i, value in enumerate(limit_values):
    if i == 0:
        # Primer elemento: traer todo lo menor o igual al valor
        query = f"SELECT * FROM ( select * from ACSELE_ALFA.thirdpartymovementpolicy where PK >= 5499039332) WHERE PK <= {value}"
        logger.info(f"Executing first query: {query}")
    else:
        # Elementos intermedios y último elemento: traer lo que esté entre el anterior y el actual
        prev_value = limit_values[i - 1]
        query = f"SELECT * FROM  ( select * from ACSELE_ALFA.thirdpartymovementpolicy where PK >= 5499039332) WHERE PK  > {prev_value} AND PK <= {value}"
        logger.info(f"Executing intermediate query between {prev_value} and {value}: {query}")
 
    # Configuración de la tabla para la consulta actual
    table_info = {
    "table": "THIRDPARTYMOVEMENTPOLICY_DATA1",
    "fields": [
        "PK",
        "POLICYID",
        "OPERATIONPK",
        "THIRDPARTY",
        "ROL_ID",
        "DESCPROPERTY",
        "CONCEPT",
        "AMMOUNT",
        "DATEOPERATION",
        "DATEUSERECIPENT",
        "DUEDATE",
        "STATUS",
        "ISMANUAL",
        "POST",
        "IDMOVEMENTPARENT",
        "TMP_TYPE",
        "PGC_CATEGORY",
        "ISVISIBLE",
        "TMP_VISIBILITYINDOCUMENTS"
    ],
    "partition_column": "-",
    "logic": query
}
 
    # Ejecución de la consulta principal
    try:
        extract_helper.execute_data(
                            f"{table_info['table']}_{i}",
                            table_info['partition_column'],  
                            table_info['logic'],
                            connectname= CONNECT_NAME)
        logger.info(f"Successfully Extracted: Query {i + 1} executed")
    except Exception as e:
        logger.error(f"ERROR WITH INGEST: {e}")
 
# Consulta adicional para todo lo superior al último valor
last_value = limit_values[-1]
query = f"SELECT * FROM (select * from ACSELE_ALFA.thirdpartymovementpolicy where PK >= 5499039332) WHERE PK > {last_value}"
logger.info(f"Executing final overflow query: {query}")
 
# Configuración de la tabla para la última consulta
table_info = {
    "table": "THIRDPARTYMOVEMENTPOLICY_DATA1",
    "fields": [
        "PK",
        "POLICYID",
        "OPERATIONPK",
        "THIRDPARTY",
        "ROL_ID",
        "DESCPROPERTY",
        "CONCEPT",
        "AMMOUNT",
        "DATEOPERATION",
        "DATEUSERECIPENT",
        "DUEDATE",
        "STATUS",
        "ISMANUAL",
        "POST",
        "IDMOVEMENTPARENT",
        "TMP_TYPE",
        "PGC_CATEGORY",
        "ISVISIBLE",
        "TMP_VISIBILITYINDOCUMENTS"
    ],
    "partition_column": "-",
    "logic": query
}
 
# Ejecución de la última consulta (superior al último valor)
try:
    extract_helper.execute_data(
        f"{table_info['table']}_last",
        table_info['partition_column'],
        table_info['logic'],
        connectname= CONNECT_NAME)
       
    logger.info(f"Successfully Extracted: Final overflow query executed")
   
except Exception as e:
    logger.error(f"ERROR WITH INGEST: {e}")