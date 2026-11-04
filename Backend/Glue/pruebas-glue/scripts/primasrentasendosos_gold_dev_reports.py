"""Script that load gold phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim, date_format, lit, max
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, DecimalType

import sys, traceback, boto3, json

###################################################################
######## SET ARGS AND CONFIG COMMONS UTILS ########################
###################################################################

logs = LogsHelper()
logger = logs.logger_conf()

args = getResolvedOptions(
    sys.argv, 
        [
        'JOB_NAME',
        'BUCKET_NAME',
        'DB_NAME',
        'DB_SCHEMA',
        'BUCKET_NAME_ASSETS',
        'PERIOD',
        'CERTIFICATE',
        'DB_CATALOG_GLUE',
        'TABLE_NAME_GOLD'
        ]
    )
BUCKET_NAME = args['BUCKET_NAME']
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
DB_NAME = args['DB_NAME']
DB_SCHEMA = args['DB_SCHEMA']
PERIOD = args['PERIOD']
CERTIFICATE = args['CERTIFICATE']
DB_CATALOG_GLUE = args['DB_CATALOG_GLUE']
TABLE_NAME_GOLD = args['TABLE_NAME_GOLD']

logger.info(f"Period: {PERIOD}")
logger.info(f"DB_CATALOG_GLUE_SILVER: {DB_CATALOG_GLUE}")

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME, DB_NAME)

###################################################################
##################### Init stage gold #############################
###################################################################

try:
        
    
    # Define the schema for the  Final DataFrame
    schema_primas_rentas_gold = StructType([
        StructField("Valor_prima", IntegerType(), True),  # Numeric
        StructField("Tipo_de_renta", StringType(), True),  # AAAAMM (Date format stored as String)
        StructField("Fecha_expedicion", StringType(), True),  # String with specific categories
        StructField("Periodo_point", FloatType(), True),  # Numeric
        StructField("Tipo_proceso", StringType(), True),
        StructField("Unidad_de_cuenta", StringType(), True),# AAAAMM (Date format stored as String)
        StructField("PERIODO_PARTITION", StringType(), True)  # AAAAMM (Date format stored as String)
    ])
    
    #Read tables from silver schema general
    #df_acu_personas_naturales = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ACU_PERSONAS_NATURALES").toDF()
    df_cgr_origenes = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_ORIGENES").toDF()
    #df_cgr_tipos_identificaciones = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_TIPOS_IDENTIFICACIONES").toDF()
    
    #Read tables from silver schema prv
    df_prv_endosos_point = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_ENDOSOS_POINT").toDF()
    df_prv_rentas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_RENTAS").toDF()
    #df_prv_saldos_matematica = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_SALDOS_MATEMATICA").toDF()
    
    
    ###################################################################
    ############ Init Businees Logic CAMPOS_ANEXO1 ####################
    ###################################################################
    
    logger.info(f"Writing to {TABLE_NAME_GOLD} with period = {PERIOD}")
    logger.info(f"Make logic to CAMPOS_ANEXO1 ")

    #-----------------------------------------------------------------#
    #---------------------- Join Tables ------------------------------#
    #-----------------------------------------------------------------#
    df_result = df_prv_rentas.alias("r").join(
        df_prv_endosos_point.alias("e"), col("r.nid_rentas") == col("e.nid_rentas")
    ).join(
        df_cgr_origenes.alias("o"), col("r.cod_origen") == col("o.cod_origen")
    )
    #------------------------ start apply logic ----------------------#
    ################ SELECT FIELDS ##################
    
    df_result = df_result.select(
              F.col("r.nid_rentas").alias("nid_renta")
            , F.col("r.ramo_point").alias("ramo_point")
            , F.date_format("r.fec_expedicion", "yyyyMM").alias("periodo_expedicion_renta")
            , F.col("r.fec_expedicion").alias("fecha_expedicion_renta")
            , F.date_format("r.fec_expedicion", "yyyyMM").alias("periodo_expedicion_endoso")
            , F.col("e.fec_expedicion").alias("fecha_expedicion_endoso")
            , F.date_format("e.fec_envio_point", "yyyyMM").alias("periodo_envio_point_endoso")
            , F.col("e.fec_envio_point").alias("fecha_envio_point_endoso")
            , F.col("e.vlr_total").alias("vlr_endoso")
            , F.col("e.vlr_total_recaudo").alias("vlr_recaudo_endoso")
            , F.col("e.nid_endoso_reversion").alias("reversion_endoso")
            , F.col("e.nid_endoso_reversado").alias("reversado_endoso")
            , F.col("e.bln_proceso")
            , F.col("o.nombre").alias("tipo_renta")
            , F.col("e.cod_tipo_endoso").alias("cod_tipo_endoso")
        )

    df_result = df_result.select(
        F.col("nid_renta").alias("NO_RENTA"),
        F.col("ramo_point").alias("RAMO_POINT"),
        F.col("periodo_expedicion_renta").alias("PER_EXP_RENTA"),
        F.col("fecha_expedicion_renta").cast("date").alias("FEC_EXP_RENTA"),
        F.date_format("fecha_expedicion_endoso", "yyyyMM").alias("PER_EXP_ENDOSO"),
        F.col("fecha_expedicion_endoso").cast("date").alias("FEC_EXP_ENDOSO"),
        F.col("periodo_envio_point_endoso").alias("PER_POINT_ENDOSO"),
        F.col("fecha_envio_point_endoso").cast("date").alias("FEC_POINT_ENDOSO"),
        F.col("vlr_endoso").cast(DecimalType(38,7)).alias("VLR_ENDOSO"),
        F.col("vlr_recaudo_endoso").cast(DecimalType(38,7)).alias("VLR_RECAUDO_ENDOSO"),
        F.col("reversion_endoso").alias("NID_REVERSION_ENDOSO"),
        F.col("reversado_endoso").alias("NID_REVERSADO_ENDOSO"),
        F.col("bln_proceso").alias("BLN_PROCESO"),
        F.col("tipo_renta").alias("TIPO_RENTA"),
        F.lit("Endoso").alias("TIPO_PROCESO_ENDOSO"),
        F.col("cod_tipo_endoso").alias("COD_TIPO_ENDOSO")
    )
    ###################################################################
    ##################### Save files Data #############################
    ###################################################################
    
    partition_columns = ['PER_EXP_ENDOSO']
    
    # Save the transformed DataFrame to S3
    frame_save = DynamicFrame.fromDF(df_result, glueContext, "frame_save_dynamic")
    
    s3_prefix = f"GOLD/NIIF/{TABLE_NAME_GOLD}/"
                
    s3_path = f"s3://{BUCKET_NAME}/{s3_prefix}"
    
    logger.info(f"Truncate & Saving data to: {s3_path}")
        
    extract_helper.delete_s3_folder(BUCKET_NAME, s3_prefix)
    
    extract_helper.save_files_s3(frame_save, s3_path, partition_columns)
    
except Exception as e:
    print(traceback.format_exc())
    logger.error(f"An error occurred: {e}")
    
