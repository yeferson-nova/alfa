"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim
from pyspark.sql.functions import regexp_replace

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
        "ACU_PERSONAS_NATURALES": {
            "partition_columns": [],
            "schema": "GENERAL",
            "transform": 
        [
            lambda df: df.select(
				trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
				col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
				trim(col("PRIMER_APELLIDO")).alias("PRIMER_APELLIDO"),
				trim(col("SEGUNDO_APELLIDO")).alias("SEGUNDO_APELLIDO"),
				trim(col("PRIMER_NOMBRE")).alias("PRIMER_NOMBRE"),
				trim(col("SEGUNDO_NOMBRE")).alias("SEGUNDO_NOMBRE"),
				col("FEC_NACIMIENTO").alias("FEC_NACIMIENTO"),
				trim(col("GENERO")).alias("GENERO"),
				trim(col("COD_ESTADO_CIVIL")).alias("COD_ESTADO_CIVIL"),
				col("PIN_POINT").alias("PIN_POINT"),
				col("FEC_MUERTE").alias("FEC_MUERTE"),
				col("FEC_REGISTRO").alias("FEC_REGISTRO"),
				col("VLR_NIVEL_INGRESOS").alias("VLR_NIVEL_INGRESOS"),
				trim(col("CARGO")).alias("CARGO"),
				trim(col("COD_OCUPACION")).alias("COD_OCUPACION"),
				trim(col("COD_ACTIVIDAD_ECONOMICA_CIIU")).alias("COD_ACTIVIDAD_ECONOMICA_CIIU"),
				trim(col("COD_DEPARTAMENTO_NACIMIENTO")).alias("COD_DEPARTAMENTO_NACIMIENTO"),
				trim(col("COD_CIUDAD_NACIMIENTO")).alias("COD_CIUDAD_NACIMIENTO"),
				trim(col("COD_TRATAMIENTO")).alias("COD_TRATAMIENTO"),
				trim(col("COD_ESTADO")).alias("COD_ESTADO"),
				trim(col("COD_TIPO_SANGUINEO")).alias("COD_TIPO_SANGUINEO"),
				trim(col("COD_EPS_ULTIMA")).alias("COD_EPS_ULTIMA"),
				trim(col("COD_NIVEL_EDUCATIVO")).alias("COD_NIVEL_EDUCATIVO"),
				trim(col("COD_PROFESION")).alias("COD_PROFESION"),
				trim(col("COD_ENTIDAD_EDUCATIVA")).alias("COD_ENTIDAD_EDUCATIVA"),
				trim(col("COD_ESTADO_SALUD")).alias("COD_ESTADO_SALUD"),
				trim(col("OBSERVACIONES")).alias("OBSERVACIONES"),
				trim(col("COD_ACTIVIDAD_ECONOMICA_SGSS")).alias("COD_ACTIVIDAD_ECONOMICA_SGSS"),
				trim(col("COD_AFP_ULTIMA")).alias("COD_AFP_ULTIMA"),
				col("FEC_MODIFICACION_MUERTE").alias("FEC_MODIFICACION_MUERTE"),
				trim(col("NID_NUIP")).alias("NID_NUIP"),
				trim(col("USUARIO_CREACION")).alias("USUARIO_CREACION"),
				trim(col("USUARIO_MODIFICACION")).alias("USUARIO_MODIFICACION"),
				col("FEC_MODIFICACION").alias("FEC_MODIFICACION"),
				trim(col("COD_OCUPACION_CIUO_88")).alias("COD_OCUPACION_CIUO_88"),
				trim(col("COD_ARP")).alias("COD_ARP"),
				col("FEC_EXPEDICION_DOCUMENTO").alias("FEC_EXPEDICION_DOCUMENTO"),
				trim(col("COD_CIUDAD_EXPEDICION_DOCU")).alias("COD_CIUDAD_EXPEDICION_DOCU"),
				trim(col("COD_DEPARTAMENTO_EXPE_DOCU")).alias("COD_DEPARTAMENTO_EXPE_DOCU"),
				trim(col("BLN_RESIDENTE_EXTERIOR")).alias("BLN_RESIDENTE_EXTERIOR"),
				col("FEC_INGRESO_AFP").alias("FEC_INGRESO_AFP"),
				trim(col("CONDICION_INVALIDEZ")).alias("CONDICION_INVALIDEZ"),
				trim(col("NACIONALIDAD")).alias("NACIONALIDAD"),
				col("NUM_CUENTA_AFP").alias("NUM_CUENTA_AFP"),
				col("VLR_SALARIO_BASE_AFP").alias("VLR_SALARIO_BASE_AFP"),
				col("ULT_PERIODO_COT_AFP").alias("ULT_PERIODO_COT_AFP"),
				trim(col("COD_COMENT_FEC_MUERTE")).alias("COD_COMENT_FEC_MUERTE"),
				trim(col("ORIGEN_CAMBIO")).alias("ORIGEN_CAMBIO")
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "CGR_EQUIVALENCIA_TIPOS_ID": {
            "partition_columns": [],
            "schema": "GENERAL",
            "transform": 
        [
            lambda df: df.select(
				trim(col("COD_EQUIVALENCIA_TIPOS_ID")).alias("COD_EQUIVALENCIA_TIPOS_ID"),
				trim(col("COD_TIPO_DOCUMENTO")).alias("COD_TIPO_DOCUMENTO"),
				trim(col("COD_TIPO_DOCUMENTO_AFP")).alias("COD_TIPO_DOCUMENTO_AFP"),
				trim(col("ABREVIATURA_TIPO_ID_AFP")).alias("ABREVIATURA_TIPO_ID_AFP"),
				trim(col("COD_TIPO_DOCUMENTO_ACCESS")).alias("COD_TIPO_DOCUMENTO_ACCESS"),
				trim(col("COD_TIPO_POINT")).alias("COD_TIPO_POINT"),
				trim(col("COD_TIPO_ID_BONOS")).alias("COD_TIPO_ID_BONOS"),
				trim(col("COD_TIPO_TESORERIA")).alias("COD_TIPO_TESORERIA"),
				trim(col("COD_TIPO_SUPER")).alias("COD_TIPO_SUPER"),
				trim(col("COD_TIPO_BBVA")).alias("COD_TIPO_BBVA")
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "CGR_ESTADOS_PERSONAS": {
            "partition_columns": [],
            "schema": "GENERAL",
            "transform": 
        [
            lambda df: df.select(
				trim(col("COD_ESTADO_PERSONA")).alias("COD_ESTADO_PERSONA"),
				trim(col("NOMBRE")).alias("NOMBRE"),
				trim(col("ABREVIATURA")).alias("ABREVIATURA")
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "CGR_PARENTESCOS": {
            "partition_columns": [],
            "schema": "GENERAL",
            "transform": 
        [
            lambda df: df.select(
				trim(col("COD_PARENTESCO")).alias("COD_PARENTESCO"),
				trim(col("NOMBRE")).alias("NOMBRE")
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "ANP_IPC": {
            "partition_columns": [],
            "schema": "GENERAL",
            "transform": 
        [
            lambda df: df.select(
				col("ANIO").cast("int").alias("ANIO"),
				col("PCJ_IPC").cast("decimal(5,2)").alias("PCJ_IPC")
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "RVA_RESERVA_MATEMATICA_TRANSIC": {
            "partition_columns": [],
            "schema": "GENERAL",
            "transform": 
        [
            lambda df: df.select(
				col("RAMO").cast("string").alias("RAMO"),
				col("NID_SINIESTRO_RENTA").cast("bigint").alias("NID_SINIESTRO_RENTA"),
				col("VLR_RESERVA_RV89").cast("decimal(14,2)").alias("VLR_RESERVA_RV89"),
				col("VLR_SALDO_RESERVA").cast("decimal(38,10)").alias("VLR_SALDO_RESERVA"),
				col("VLR_RESERVA_RV08").cast("decimal(14,2)").alias("VLR_RESERVA_RV08"),
				col("PCJ_VARIACION_RESERVA").cast("decimal(8,5)").alias("PCJ_VARIACION_RESERVA"),
				col("DIFERENCIA_PCJ").cast("decimal(8,5)").alias("DIFERENCIA_PCJ"),
				col("PCJ_DELTA").cast("decimal(13,10)").alias("PCJ_DELTA")
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "CGR_ORIGENES": {
            "partition_columns": [],
            "schema": "GENERAL",
            "transform": 
        [
            lambda df: df.select(
				col("COD_ORIGEN").cast("string").alias("COD_ORIGEN"),
				col("NOMBRE").cast("string").alias("NOMBRE"),
				col("ABREVIATURA").cast("string").alias("ABREVIATURA"),
				col("COD_ORIGEN_SUPER").cast("string").alias("COD_ORIGEN_SUPER")
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "PGR_SALARIOS_MINIMOS": {
            "partition_columns": [],
            "schema": "GENERAL",
            "transform": 
        [
            lambda df: df.select(
                col("ANO").alias("ANO"),
                col("MES").alias("MES"),
                col("VLR_SALARIO_MINIMO").alias("VLR_SALARIO_MINIMO")
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "ANP_PENSIONES": {
            "partition_columns": ["fec_expedicion_partition"],
            "schema": "GENERAL",
            "transform": [
                lambda df: df.select(
				col("NID_PENSION").cast("int").alias("NID_PENSION"),
				col("NID_PROCEDENCIA").cast("int").alias("NID_PROCEDENCIA"),
				col("VLR_PENSION_INICIAL").cast("decimal(14,2)").alias("VLR_PENSION_INICIAL"),
				col("VLR_PENSION_FINAL").cast("decimal(14,2)").alias("VLR_PENSION_FINAL"),
				trim(col("COD_PROCEDENCIA")).alias("COD_PROCEDENCIA"),
				trim(col("COD_ORIGEN")).alias("COD_ORIGEN"),
				trim(col("BLN_DEFINITIVO")).alias("BLN_DEFINITIVO"),
				trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
				col("NID_NUMERO_IDENTIFICACION").cast("bigint").alias("NID_NUMERO_IDENTIFICACION"),
				trim(col("COD_SUCURSAL_ORIGEN")).alias("COD_SUCURSAL_ORIGEN"),
				col("NID_SINIESTRO").cast("bigint").alias("NID_SINIESTRO"),
				col("FEC_EXPEDICION").alias("FEC_EXPEDICION"),
				col("NUM_MESADAS").cast("int").alias("NUM_MESADAS"),
                col("fec_expedicion_partition").alias("fec_expedicion_partition")
                ),
            lambda df: df.dropDuplicates()
            ]
        },
        "ANP_PENSIONADO_NOVEDAD": {
            "partition_columns": ["fec_ingreso_partition"],
            "schema": "GENERAL",
            "transform": 
        [
            lambda df: df.select(
			col("NID_DISTRIBUCION_VIGENCIA").cast("int").alias("NID_DISTRIBUCION_VIGENCIA"),
				trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
				col("NID_NUMERO_IDENTIFICACION").cast("bigint").alias("NID_NUMERO_IDENTIFICACION"),
				trim(col("COD_CONCEPTO")).alias("COD_CONCEPTO"),
				col("PCJ_PENSION").cast("decimal(5,2)").alias("PCJ_PENSION"),
				col("VLR_FIJO").cast("decimal(14,2)").alias("VLR_FIJO"),
				col("FEC_FINAL").alias("FEC_FINAL"),
				col("FEC_INICIAL").alias("FEC_INICIAL"),
				trim(col("BLN_FIJA")).alias("BLN_FIJA"),
				col("VLR_TOTAL").cast("decimal(14,2)").alias("VLR_TOTAL"),
				col("FEC_INGRESO").alias("FEC_INGRESO"),
				col("FEC_MODIFICACION").alias("FEC_MODIFICACION"),
				trim(col("COD_TIPO_IDENTIFICACION_TER")).alias("COD_TIPO_IDENTIFICACION_TER"),
				col("NID_NUMERO_IDENTIFICACION_TER").cast("bigint").alias("NID_NUMERO_IDENTIFICACION_TER"),
				trim(col("COD_EPS")).alias("COD_EPS"),
				trim(col("PERIODO_INICIAL")).alias("PERIODO_INICIAL"),
				trim(col("PERIODO_FINAL")).alias("PERIODO_FINAL"),
				trim(col("COD_SUCURSAL_GIRO")).alias("COD_SUCURSAL_GIRO"),
				col("FEC_INHABILITACION").alias("FEC_INHABILITACION"),
				col("NID_CONSECUTIVO_TER").cast("int").alias("NID_CONSECUTIVO_TER"),
				col("NUM_DIAS_LIQUIDADOS").cast("int").alias("NUM_DIAS_LIQUIDADOS"),
				col("VALOR_UPC").cast("decimal(14,2)").alias("VALOR_UPC"),
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
            lambda df: df.dropDuplicates()
        ]
        },
        "ANP_DISTRIBUCION_VIG_PENSION": {
            "partition_columns": ["fec_inicial_partition"],
            "schema": "GENERAL",
            "transform": [
                lambda df: df.select(
				col("NID_DISTRIBUCION_VIGENCIA").cast("int").alias("NID_DISTRIBUCION_VIGENCIA"),
				col("NID_VERSION_PENSION").cast("int").alias("NID_VERSION_PENSION"),
				col("NID_PENSION").cast("int").alias("NID_PENSION"),
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
        "ANP_MOVIMIENTOS_PAGO": {
            "partition_columns": ["FEC_INICIAL_PARTITION"],
            "schema": "GENERAL",
            "transform": 
        [
            lambda df: df.select(
				col("PERIODO_PAGO").alias("PERIODO_PAGO"),
				col("NID_DISTRIBUCION_VIGENCIA").alias("NID_DISTRIBUCION_VIGENCIA"),
				trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
				col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
				trim(col("COD_CONCEPTO")).alias("COD_CONCEPTO"),
				col("VLR_CONCEPTO").alias("VLR_CONCEPTO"),
				col("FEC_PAGO").alias("FEC_PAGO"),
				col("FEC_INICIAL").alias("FEC_INICIAL"),
				col("FEC_FINAL").alias("FEC_FINAL"),
				col("NUM_DIAS_LIQ_CONCEPTO").alias("NUM_DIAS_LIQ_CONCEPTO"),
				trim(col("COD_TIPO_IDENTIFICACION_TUTOR")).alias("COD_TIPO_IDENTIFICACION_TUTOR"),
				col("NID_NUMERO_IDENTIFICACION_TUTO").alias("NID_NUMERO_IDENTIFICACION_TUTO"),
				trim(col("COD_SUCURSAL_GIRO")).alias("COD_SUCURSAL_GIRO"),
				col("FEC_GENERACION_NOMINA").alias("FEC_GENERACION_NOMINA"),
				col("NID_NUMERO_SOLICITUD_GIRO").alias("NID_NUMERO_SOLICITUD_GIRO"),
				trim(col("COD_FORMA_PAGO")).alias("COD_FORMA_PAGO"),
				col("VLR_BASE").alias("VLR_BASE"),
				col("NID_CONSECUTIVO_TUTOR").alias("NID_CONSECUTIVO_TUTOR"),
				col("COD_MISCELANEO").alias("COD_MISCELANEO"),
				trim(col("BLN_VALOR_UPC")).alias("BLN_VALOR_UPC"),
				col("FEC_INHABILITACION").alias("FEC_INHABILITACION"),
				trim(col("COMENTARIO_INHABILITACION")).alias("COMENTARIO_INHABILITACION"),
				trim(col("BLN_RECHAZADO_OCCIRED")).alias("BLN_RECHAZADO_OCCIRED"),
				col("FEC_HABILITACION").alias("FEC_HABILITACION"),
				trim(col("USUARIO_HABILITACION")).alias("USUARIO_HABILITACION"),
				col("COD_ESTADO_MOVIMIENTO").alias("COD_ESTADO_MOVIMIENTO"),
				col("IDN_PAGO_MASIVO").alias("IDN_PAGO_MASIVO"),
				col("FEC_MODIFICACION").alias("FEC_MODIFICACION"),
				col("FEC_INICIAL_PARTITION").alias("FEC_INICIAL_PARTITION")
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
    