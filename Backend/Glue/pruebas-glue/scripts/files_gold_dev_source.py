# -----------------------------
# Simple Parquet File Copy Job
# -----------------------------
import boto3
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, sum
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from datetime import datetime
import sys, traceback, boto3, json, tempfile, zipfile
from urllib.parse import urlparse
from commons.ExtractHelper import ExtractHelper
from commons.LogsHelper import LogsHelper

###################################################################
######## SET ARGS AND CONFIG COMMONS UTILS ########################
###################################################################
logs = LogsHelper()
logger = logs.logger_conf()

args = getResolvedOptions(
    sys.argv, 
        [
        'JOB_NAME',
        'BUCKET_SOURCE',                # Bucket source
        'BUCKET_NAME',                  # Bucket targe
        'FILES',                        # Files to processing
        'FILES_TOTAL',
        'DB_NAME',
        'DB_SCHEMA',
        'DB_CATALOG_GLUE',
        'LOAD_MODE'
        ]
    )
    

BUCKET_SOURCE = args['BUCKET_SOURCE']
BUCKET_NAME = args['BUCKET_NAME']
DB_CATALOG_GLUE = args['DB_CATALOG_GLUE']
DB_NAME = args['DB_NAME']
DB_SCHEMA = args['DB_SCHEMA']
LOAD_MODE = args['LOAD_MODE']
# Parse FILES from job parameters (JSON string to Python list)
files = json.loads(args['FILES'])
files_total = json.loads(args['FILES_TOTAL'])
        

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3_client = boto3.client("s3")

extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME, DB_NAME)



    
def archivo_cesion_previsionales_calculados(df): 
    
    df_total_aggregated = df.groupBy(
                df["MES_DE_CIERRE"],
                df["FECHA_CARGA"],
                df["ID_LOTE_CARGUE"]
                ).agg(
                    sum(col("REVERSION")).alias("TOTAL_REVERSION"),
                    sum(col("REAL")).alias("TOTAL_REAL"),
                    sum(col("CAUSACION")).alias("TOTAL_CAUSACION"),
                    sum(col("PRIMA_EMITIDA")).alias("TOTAL_PRIMA_EMITIDA"),
                    sum(col("VALOR_PRIMA_CEDIDA")).alias("TOTAL_VALOR_PRIMA_CEDIDA")
                )
    df_total = df_total_aggregated.select(
        df_total_aggregated['TOTAL_REVERSION'],
        df_total_aggregated['TOTAL_REAL'],
        df_total_aggregated['TOTAL_CAUSACION'],
        df_total_aggregated['TOTAL_PRIMA_EMITIDA'],
        df_total_aggregated['TOTAL_VALOR_PRIMA_CEDIDA'],
        df_total_aggregated['ID_LOTE_CARGUE'],
        df_total_aggregated['FECHA_CARGA'].alias('fecha_carga')
        )
    
    return df_total
    
    
load = 'truncate'
partition_columns =  ["fecha_carga"]

for case_use, file in files.items():
    
    # Load the table data as a DataFrame
    table_name = f"{DB_NAME.lower()}_{file.lower()}"
    
    logger.info(table_name)
    
    df_from_bronce_catalog = glueContext.create_dynamic_frame.from_catalog(
            database=f"{DB_CATALOG_GLUE}",
            table_name=table_name
        ).toDF()
    
    for current_file in [file, files_total.get(case_use)]:
        if current_file is None:
            continue 
        
        logger.info(f"Processing file: {current_file}")
        
        key_gold = f"GOLD/NIIF/{current_file}_GOLD/"
        
        
        try:
            df_from_bronce = eval(f"{current_file.lower()}(df_from_bronce_catalog)")
            logger.info(f"✔ Se aplicó la función: {current_file.lower()}")
            
        except NameError:
            df_from_bronce = df_from_bronce_catalog
            logger.info(f"⚠ No existe función {current_file.lower()}, se usará el DataFrame original")
        
        last_partition = df_from_bronce.selectExpr("max(fecha_carga) as max_fecha").first()[0]
    
        logger.info(f"The last_partition is : {last_partition}")
        
        
        #if LOAD_MODE == 'truncate': 
        if load == 'truncate':
            
            s3_prefix = key_gold
            df_to_gold = df_from_bronce
            
            #delete all data 
            extract_helper.delete_s3_folder(BUCKET_NAME, key_gold)
            
        else: 
            
            #filter to last_partition
            df_to_gold = df_from_bronce.filter(col("fecha_carga") == last_partition )
            
            s3_prefix = key_gold
            
            
        #Save the transformed DataFrame to S3
        frame_save = DynamicFrame.fromDF(df_to_gold, glueContext, "frame_save_dynamic")
        
        #Save the data to s3
        s3_path_gold = f"s3://{BUCKET_NAME}/{key_gold}"
        
        logger.info(f"Saving data to: {s3_path_gold}")
        
        
        extract_helper.save_files_s3(frame_save, s3_path_gold, partition_columns)


# End of job
job.commit()

