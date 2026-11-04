"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim

from datetime import datetime
from dateutil.relativedelta import relativedelta

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
        'DB_CATALOG_GLUE'
        ]
    )
BUCKET_NAME = args['BUCKET_NAME']
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
DB_NAME = args['DB_NAME']
DB_SCHEMA = args['DB_SCHEMA']
PERIOD = args['PERIOD']
CERTIFICATE = args['CERTIFICATE']
DB_CATALOG_GLUE = args['DB_CATALOG_GLUE']


logger.info(f"Period: {PERIOD}")
logger.info(f"DB_CATALOG_GLUE_BRONCE: {DB_CATALOG_GLUE}")

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
#################### Init stage silver ############################
###################################################################

try:
    # Define the dictionary of tables and their respective partition columns.
    tables_case_use = {
    "ANP_DETALLE_SOLICITUDES_GIRO": {
        "partition_columns": ["periodo_partition"],
        "schema": "GENERAL",
        "transform": [
            lambda df: df.select(
                col("NID_DETALLE_SOLICITUD_GIRO").alias("NID_DETALLE_SOLICITUD_GIRO"),
                col("NID_PERIODO_SOLICITUD_GIRO").alias("NID_PERIODO_SOLICITUD_GIRO"),
                trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
                col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
                col("VLR_ORIGINAL").alias("VLR_ORIGINAL"),
                col("VLR_APROXIMADO").alias("VLR_APROXIMADO"),
                col("VLR_SALARIO_BASICO").alias("VLR_SALARIO_BASICO"),
                col("VLR_INGRESO_BASE_COTIZ").alias("VLR_INGRESO_BASE_COTIZ"),
                col("periodo_partition").alias("periodo_partition")
            ),
            lambda df: df.dropDuplicates()
        ]
    },
    "ANP_DISTRIBUCION_VIG_PENSION": {
        "partition_columns": ["fec_inicial_partition"],
        "schema": "GENERAL",
        "transform": [
            lambda df: df.select(
                col("NID_DISTRIBUCION_VIGENCIA").alias("NID_DISTRIBUCION_VIGENCIA"),
                col("NID_VERSION_PENSION").alias("NID_VERSION_PENSION"),
                col("NID_PENSION").alias("NID_PENSION"),
                col("FEC_INICIAL").alias("FEC_INICIAL"),
                col("FEC_FINAL").alias("FEC_FINAL"),
                trim(col("COD_RAZON_CAMBIO")).alias("COD_RAZON_CAMBIO"),
                trim(col("COMENTARIOS")).alias("COMENTARIOS"),
                trim(col("USUARIO_CREACION")).alias("USUARIO_CREACION"),
                col("fec_inicial_partition").alias("fec_inicial_partition")
            ),
            lambda df: df.dropDuplicates()
        ]
    },
    "ANP_PENSIONADO_NOVEDAD": {
        "partition_columns": ["fec_ingreso_partition"],
        "schema": "GENERAL",
        "transform": [
            lambda df: df.select(
                col("NID_DISTRIBUCION_VIGENCIA").alias("NID_DISTRIBUCION_VIGENCIA"),
                trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
                col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
                trim(col("COD_CONCEPTO")).alias("COD_CONCEPTO"),
                col("PCJ_PENSION").alias("PCJ_PENSION"),
                col("VLR_FIJO").alias("VLR_FIJO"),
                col("FEC_FINAL").alias("FEC_FINAL"),
                col("FEC_INICIAL").alias("FEC_INICIAL"),
                trim(col("BLN_FIJA")).alias("BLN_FIJA"),
                col("VLR_TOTAL").alias("VLR_TOTAL"),
                col("FEC_INGRESO").alias("FEC_INGRESO"),
                col("FEC_MODIFICACION").alias("FEC_MODIFICACION"),
                trim(col("COD_TIPO_IDENTIFICACION_TER")).alias("COD_TIPO_IDENTIFICACION_TER"),
                col("NID_NUMERO_IDENTIFICACION_TER").alias("NID_NUMERO_IDENTIFICACION_TER"),
                trim(col("COD_EPS")).alias("COD_EPS"),
                trim(col("PERIODO_INICIAL")).alias("PERIODO_INICIAL"),
                trim(col("PERIODO_FINAL")).alias("PERIODO_FINAL"),
                trim(col("COD_SUCURSAL_GIRO")).alias("COD_SUCURSAL_GIRO"),
                trim(col("FEC_INHABILITACION")).alias("FEC_INHABILITACION"),
                col("NID_CONSECUTIVO_TER").alias("NID_CONSECUTIVO_TER"),
                col("NUM_DIAS_LIQUIDADOS").alias("NUM_DIAS_LIQUIDADOS"),
                col("VALOR_UPC").alias("VALOR_UPC"),
                trim(col("BLN_VALOR_UPC")).alias("BLN_VALOR_UPC"),
                trim(col("COD_FORMA_PAGO")).alias("COD_FORMA_PAGO"),
                trim(col("BLN_EPS_OBLIGATORIA")).alias("BLN_EPS_OBLIGATORIA"),
                trim(col("COMENTARIO_INHABILITACION")).alias("COMENTARIO_INHABILITACION"),
                trim(col("COD_CAUSA_INHABILITACION")).alias("COD_CAUSA_INHABILITACION"),
                trim(col("COD_CAUSA_ACTIVACION")).alias("COD_CAUSA_ACTIVACION"),
                trim(col("USUARIO_MODIFICACION")).alias("USUARIO_MODIFICACION"),
                trim(col("ESTADO_NOVEDAD")).alias("ESTADO_NOVEDAD"),
                trim(col("BLN_MESADA_ADCNL")).alias("BLN_MESADA_ADCNL"),
                col("fec_ingreso_partition").alias("fec_ingreso_partition")
            ),
            lambda df: df.dropDuplicates()  # Deduplication
        ]
    },
    "ANP_PENSIONES": {
        "partition_columns": ["FEC_EXPEDICION_PARTITION"],
        "schema": "GENERAL",
        "transform": [
            lambda df: df.select(
                    col("NID_PENSION").alias("NID_PENSION"),
                    col("NID_PROCEDENCIA").alias("NID_PROCEDENCIA"),
                    col("VLR_PENSION_FINAL").alias("VLR_PENSION_FINAL"),
                    col("VLR_PENSION_INICIAL").alias("VLR_PENSION_INICIAL"),                    
                    trim(col("COD_PROCEDENCIA")).alias("COD_PROCEDENCIA"),
                    trim(col("COD_ORIGEN")).alias("COD_ORIGEN"),
                    trim(col("BLN_DEFINITIVO")).alias("BLN_DEFINITIVO"),
                    trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
                    col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),                                        
                    trim(col("COD_SUCURSAL_ORIGEN")).alias("COD_SUCURSAL_ORIGEN"),
                    col("NID_SINIESTRO").alias("NID_SINIESTRO"),
                    col("FEC_EXPEDICION").alias("FEC_EXPEDICION"),
                    col("NUM_MESADAS").alias("NUM_MESADAS"),                    
                    col("FEC_EXPEDICION_PARTITION").alias("FEC_EXPEDICION_PARTITION")
            ),
            lambda df: df.dropDuplicates()
        ]
    },
    "ANP_PERIODO_SOLICITUDES_GIRO": {
        "partition_columns": ["periodo_partition"],
        "schema": "GENERAL",
        "transform": [
            lambda df: df.select(
                    col("NUM_SOLICITUD_ANP").alias("NUM_SOLICITUD_ANP"),                         
                    col("PERIODO").alias("PERIODO"),                                             
                    trim(col("COD_SUCURSAL_GIRO")).alias("COD_SUCURSAL_GIRO"),                   
                    trim(col("COD_SUCURSAL_ORIGEN")).alias("COD_SUCURSAL_ORIGEN"),               
                    trim(col("COD_CONCEPTO_POINT")).alias("COD_CONCEPTO_POINT"),                 
                    trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),       
                    col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),         
                    col("VLR_BRUTO").alias("VLR_BRUTO"),                                         
                    col("VLR_NETO").alias("VLR_NETO"),                                           
                    col("FEC_GENERACION_ANP").alias("FEC_GENERACION_ANP"),                       
                    col("NUM_SOLICITUD_TES").alias("NUM_SOLICITUD_TES"),                         
                    col("FEC_MODIFICACION_ANP").alias("FEC_MODIFICACION_ANP"),                   
                    col("FEC_ENVIO_TES").alias("FEC_ENVIO_TES"),                                 
                    col("FEC_PROCESADO_TES").alias("FEC_PROCESADO_TES"),                         
                    col("FEC_MARCA_ANULACION").alias("FEC_MARCA_ANULACION"),                     
                    col("FEC_ANULACION_TES").alias("FEC_ANULACION_TES"),                         
                    col("FEC_ENVIO_ANULACION").alias("FEC_ENVIO_ANULACION"),                     
                    trim(col("BLN_PROCESADO_POINT")).alias("BLN_PROCESADO_POINT"),               
                    trim(col("BLN_ANULADO_POINT")).alias("BLN_ANULADO_POINT"),                   
                    trim(col("COD_CONCEPTO_ANP")).alias("COD_CONCEPTO_ANP"),                     
                    col("NID_PERIODO_SOLICITUD_GIRO").alias("NID_PERIODO_SOLICITUD_GIRO"),       
                    trim(col("COD_FORMA_PAGO")).alias("COD_FORMA_PAGO"),                         
                    trim(col("BLN_EJECUCION_ARCHIVO_EPS")).alias("BLN_EJECUCION_ARCHIVO_EPS"),   
                    col("NID_CONSECUTIVO").alias("NID_CONSECUTIVO"),                             
                    col("NUM_PROCESO_NOMINA").alias("NUM_PROCESO_NOMINA"),                       
                    trim(col("BLN_SUSPENDIDO")).alias("BLN_SUSPENDIDO"),                         
                    trim(col("USUARIO_ANULACION")).alias("USUARIO_ANULACION"),                   
                    trim(col("COD_TIPO_OPERACION")).alias("COD_TIPO_OPERACION"),                 
                    col("NID_SOLICITUD_ANULADA").alias("NID_SOLICITUD_ANULADA"),                 
                    col("NID_SOLICITUD_ANULACION").alias("NID_SOLICITUD_ANULACION"),             
                    trim(col("COD_CAUSA_ANULACION")).alias("COD_CAUSA_ANULACION"),               
                    trim(col("USUARIO_GENERACION")).alias("USUARIO_GENERACION"),                 
                    trim(col("USUARIO_SINCRONIZACION")).alias("USUARIO_SINCRONIZACION"),         
                    trim(col("BLN_GENERADO_ARCHIVO_PAGO")).alias("BLN_GENERADO_ARCHIVO_PAGO"),   
                    col("FEC_GENERADO_ARCHIVO_PAGO").alias("FEC_GENERADO_ARCHIVO_PAGO"),         
                    trim(col("COD_PROCEDENCIA")).alias("COD_PROCEDENCIA"),                       
                    trim(col("BLN_PAGO_EXTRAORDINARIO")).alias("BLN_PAGO_EXTRAORDINARIO"),       
                    col("FEC_MODIF_BLN_PAGO_EXTRAORD").alias("FEC_MODIF_BLN_PAGO_EXTRAORD"),     
                    col("BLN_MODIFICADO_PAGO_EXTRAORD").alias("BLN_MODIFICADO_PAGO_EXTRAORD"),
                    col("periodo_partition").alias("periodo_partition")
            ),
            lambda df: df.dropDuplicates()
        ]
    },
    "CGR_CONCEPTOS": {
        "partition_columns": [],
        "schema": "GENERAL",
        "transform": [
            lambda df: df.select(
                    trim(col("COD_CONCEPTO")).alias("COD_CONCEPTO"),                      
                    trim(col("NOMBRE_CONCEPTO")).alias("NOMBRE_CONCEPTO"),               
                    trim(col("COD_TIPO_CONCEPTO")).alias("COD_TIPO_CONCEPTO"),           
                    trim(col("BLN_PREDETERMINADA")).alias("BLN_PREDETERMINADA"),         
                    trim(col("BLN_INFORMATIVA")).alias("BLN_INFORMATIVA"),               
                    col("VLR_FIJO").alias("VLR_FIJO"),                                   
                    col("PCJ_PENSION").alias("PCJ_PENSION"),                             
                    trim(col("BLN_AGRUPAR_NIT")).alias("BLN_AGRUPAR_NIT"),               
                    trim(col("COD_APROXIMACION_VALORES")).alias("COD_APROXIMACION_VALORES"),
                    trim(col("BLN_PORCENTAJE")).alias("BLN_PORCENTAJE"),                  
                    trim(col("BLN_VALOR_FIJO")).alias("BLN_VALOR_FIJO"),                  
                    trim(col("BLN_VALOR_TOTAL")).alias("BLN_VALOR_TOTAL"),                
                    trim(col("BLN_VALOR_BASE")).alias("BLN_VALOR_BASE"),                  
                    trim(col("COD_APROXIMACION_BASE")).alias("COD_APROXIMACION_BASE"),    
                    col("PCJ_VLR_BASE").alias("PCJ_VLR_BASE"),                            
                    trim(col("BLN_NETO")).alias("BLN_NETO"),                              
                    trim(col("BLN_LEY")).alias("BLN_LEY"),                                
                    trim(col("BLN_PRIMA")).alias("BLN_PRIMA")                             
            ),
            lambda df: df.dropDuplicates()
        ]
    },
    "CGR_ORIGENES": {
        "partition_columns": [],
        "schema": "GENERAL",
        "transform": [
            lambda df: df.select(
                trim(col("ABREVIATURA")).alias("ABREVIATURA"),
                trim(col("COD_ORIGEN")).alias("COD_ORIGEN"),
                trim(col("COD_ORIGEN_SUPER")).alias("COD_ORIGEN_SUPER"),
                trim(col("NOMBRE")).alias("NOMBRE")
            ),
            lambda df: df.dropDuplicates()
        ]
    }
    }
    
    ###################################################################
    #################### save stage silver ############################
    ###################################################################
    
    
    for table, config in tables_case_use.items():
        
        logger.info(f"Transform data from table : {table}")
        
        partition_columns = config["partition_columns"]
        transform_functions = config["transform"]
        schema = config["schema"]

        # Load the table data as a DataFrame
        table_name = f"{DB_NAME}_{schema}_{table}"
        df_from_bronce = glueContext.create_dynamic_frame.from_catalog(
            database=f"{DB_CATALOG_GLUE}",
            table_name=table_name
        ).toDF()
        
        #Initialize of df to transform
        df_to_silver = df_from_bronce
        
        # Apply each transformation in sequence
        for transform_function in transform_functions:
            df_to_silver = transform_function(df_to_silver)
        
        
        # Save the transformed DataFrame to S3
        frame_save = DynamicFrame.fromDF(df_to_silver, glueContext, "frame_save_dynamic")
        
        
        s3_prefix = f"{DB_NAME}/{DB_SCHEMA}/SILVER_{table}/"
            
        s3_path = f"s3://{BUCKET_NAME}/{s3_prefix}"
            
        logger.info(f"Truncate & Saving data to: {s3_path}")
        
        extract_helper.delete_s3_folder(BUCKET_NAME, s3_prefix)
        
        extract_helper.save_files_s3(frame_save, s3_path, partition_columns)

except Exception as e:
    logger.error(f"An error occurred: {e}")
    