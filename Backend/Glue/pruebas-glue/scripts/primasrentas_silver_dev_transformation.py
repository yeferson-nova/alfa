"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim
from awsglue.transforms import ResolveChoice

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
        ###########
        ### SRC ###
        ###########
        "ANP_DISTRIBUCION_VIG_PENSION": {
            "partition_columns": ["FEC_INICIAL_PARTITION"],
            "schema": "GENERAL",
            "transform": [
                lambda df: df.select(
                    col("FEC_FINAL").alias("FEC_FINAL"),
                    col("FEC_INICIAL").alias("FEC_INICIAL"),
                    col("NID_DISTRIBUCION_VIGENCIA").alias("NID_DISTRIBUCION_VIGENCIA"),
                    col("NID_PENSION").alias("NID_PENSION"),
                    col("NID_VERSION_PENSION").alias("NID_VERSION_PENSION"),
                    trim(col("COD_RAZON_CAMBIO")).alias("COD_RAZON_CAMBIO"),
                    trim(col("COMENTARIOS")).alias("COMENTARIOS"),
                    trim(col("USUARIO_CREACION")).alias("USUARIO_CREACION"),
                    col("FEC_INICIAL_PARTITION").alias("FEC_INICIAL_PARTITION")
                ),
                lambda df: df.dropDuplicates()
            ]
        },
        
        "ANP_MOVIMIENTOS_PAGO": {
            "partition_columns": ["FEC_INICIAL_PARTITION"],
            "schema": "GENERAL",
            "transform": [
                lambda df: df.select(
                    trim(col("COD_CONCEPTO")).alias("COD_CONCEPTO"),
                    trim(col("COD_FORMA_PAGO")).alias("COD_FORMA_PAGO"),
                    trim(col("COD_SUCURSAL_GIRO")).alias("COD_SUCURSAL_GIRO"),
                    trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
                    trim(col("COD_TIPO_IDENTIFICACION_TUTOR")).alias("COD_TIPO_IDENTIFICACION_TUTOR"),
                    trim(col("COMENTARIO_INHABILITACION")).alias("COMENTARIO_INHABILITACION"),
                    trim(col("USUARIO_HABILITACION")).alias("USUARIO_HABILITACION"),
                    col("BLN_RECHAZADO_OCCIRED").alias("BLN_RECHAZADO_OCCIRED"),
                    col("BLN_VALOR_UPC").alias("BLN_VALOR_UPC"),
                    col("COD_ESTADO_MOVIMIENTO").alias("COD_ESTADO_MOVIMIENTO"),
                    col("COD_MISCELANEO").alias("COD_MISCELANEO"),
                    col("FEC_FINAL").alias("FEC_FINAL"),
                    col("FEC_GENERACION_NOMINA").alias("FEC_GENERACION_NOMINA"),
                    col("FEC_HABILITACION").alias("FEC_HABILITACION"),
                    col("FEC_INHABILITACION").alias("FEC_INHABILITACION"),
                    col("FEC_INICIAL").alias("FEC_INICIAL"),
                    col("FEC_MODIFICACION").alias("FEC_MODIFICACION"),
                    col("FEC_PAGO").alias("FEC_PAGO"),
                    col("IDN_PAGO_MASIVO").alias("IDN_PAGO_MASIVO"),
                    col("NID_CONSECUTIVO_TUTOR").alias("NID_CONSECUTIVO_TUTOR"),
                    col("NID_DISTRIBUCION_VIGENCIA").alias("NID_DISTRIBUCION_VIGENCIA"),
                    col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
                    col("NID_NUMERO_IDENTIFICACION_TUTO").alias("NID_NUMERO_IDENTIFICACION_TUTO"),
                    col("NID_NUMERO_SOLICITUD_GIRO").alias("NID_NUMERO_SOLICITUD_GIRO"),
                    col("NUM_DIAS_LIQ_CONCEPTO").alias("NUM_DIAS_LIQ_CONCEPTO"),
                    col("PERIODO_PAGO").alias("PERIODO_PAGO"),
                    col("VLR_BASE").alias("VLR_BASE"),
                    col("VLR_CONCEPTO").alias("VLR_CONCEPTO"),
                    col("FEC_INICIAL_PARTITION").alias("FEC_INICIAL_PARTITION")
                ),
                lambda df: df.dropDuplicates()
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
        },
        "PRV_ENDOSOS_POINT": {
            "partition_columns": ["FEC_PROCESO_POINT_PARTITION"],
            "schema": "PRV",
            "transform": [
                lambda df: df.select(
                    trim(col("COD_TIPO_OPERACION")).alias("COD_TIPO_OPERACION"),
                    trim(col("HORA_DESDE")).alias("HORA_DESDE"),
                    trim(col("HORA_HASTA")).alias("HORA_HASTA"),
                    trim(col("MODULO")).alias("MODULO"),
                    trim(col("OBJETO")).alias("OBJETO"),
                    trim(col("PERIODO")).alias("PERIODO"),
                    trim(col("PERIODO_POINT")).alias("PERIODO_POINT"),
                    col("BLN_PROCESADO_RECAUDOS").alias("BLN_PROCESADO_RECAUDOS"),
                    col("BLN_PROCESO").alias("BLN_PROCESO"),
                    col("COD_RAZON_MODIFICACION").alias("COD_RAZON_MODIFICACION"),
                    col("COD_TIPO_ENDOSO").alias("COD_TIPO_ENDOSO"),
                    col("FEC_CREACION").alias("FEC_CREACION"),
                    col("FEC_ENVIO_POINT").alias("FEC_ENVIO_POINT"),
                    col("FEC_EXPEDICION").alias("FEC_EXPEDICION"),
                    col("FEC_FINAL").alias("FEC_FINAL"),
                    col("FEC_INICIAL").alias("FEC_INICIAL"),
                    col("FEC_PROCESO_POINT").alias("FEC_PROCESO_POINT"),
                    col("FEC_ULTIMA_ACTUALIZACION").alias("FEC_ULTIMA_ACTUALIZACION"),
                    col("NID_ENDOSO_POINT").alias("NID_ENDOSO_POINT"),
                    col("NID_ENDOSO_REVERSADO").alias("NID_ENDOSO_REVERSADO"),
                    col("NID_ENDOSO_REVERSION").alias("NID_ENDOSO_REVERSION"),
                    col("NID_POLIZA_POINT").alias("NID_POLIZA_POINT"),
                    col("NID_RENTAS").alias("NID_RENTAS"),
                    col("NUM_ENDOSO_POINT").alias("NUM_ENDOSO_POINT"),
                    col("PCJ_GASTOS").alias("PCJ_GASTOS"),
                    col("PCJ_IVA").alias("PCJ_IVA"),
                    col("VLR_ASEGURADO").alias("VLR_ASEGURADO"),
                    col("VLR_GASTOS").alias("VLR_GASTOS"),
                    col("VLR_IVA").alias("VLR_IVA"),
                    col("VLR_PRIMA").alias("VLR_PRIMA"),
                    col("VLR_TOTAL").alias("VLR_TOTAL"),
                    col("VLR_TOTAL_RECAUDO").alias("VLR_TOTAL_RECAUDO"),
                    col("FEC_PROCESO_POINT_PARTITION").alias("FEC_PROCESO_POINT_PARTITION")                    
                ),
                lambda df: df.dropDuplicates()
            ]
        },
        "PRV_RENTAS": {
            "partition_columns": ["FEC_EXPEDICION_PARTITION"],
            "schema": "PRV",
            "transform": [
                lambda df: df.select(
                    col("BLN_ACTUALIZADO_POINT").alias("BLN_ACTUALIZADO_POINT"),
                    col("BLN_POLIZA_EMITIDA").alias("BLN_POLIZA_EMITIDA"),
                    col("BLN_PROCESADO_POINT").alias("BLN_PROCESADO_POINT"),
                    col("BLN_TIENE_DERECHO").alias("BLN_TIENE_DERECHO"),
                    col("COD_CAUSA_ORIGEN_COMUN").alias("COD_CAUSA_ORIGEN_COMUN"),
                    col("FEC_ACUMULADO_PAGOS").alias("FEC_ACUMULADO_PAGOS"),
                    col("FEC_CAUSACION").alias("FEC_CAUSACION"),
                    col("FEC_DOCSOPTEJUD").alias("FEC_DOCSOPTEJUD"),
                    col("FEC_ENVIO_POINT").alias("FEC_ENVIO_POINT"),
                    col("FEC_ESTRUCTURACION_INVALIDEZ").alias("FEC_ESTRUCTURACION_INVALIDEZ"),
                    col("FEC_EXPEDICION").alias("FEC_EXPEDICION"),
                    col("FEC_INVALIDACION_REGISTRO").alias("FEC_INVALIDACION_REGISTRO"),
                    col("FEC_PAGO").alias("FEC_PAGO"),
                    col("FEC_PROCESO_POINT").alias("FEC_PROCESO_POINT"),
                    col("FEC_RECONOCIMIENTO_PENSION").alias("FEC_RECONOCIMIENTO_PENSION"),
                    col("FEC_REHABILITACION").alias("FEC_REHABILITACION"),
                    col("FEC_RESOLUCION").alias("FEC_RESOLUCION"),
                    col("FEC_ULTIMA_MOD").alias("FEC_ULTIMA_MOD"),
                    col("FEC_ULT_INCREMENTO_PENSION").alias("FEC_ULT_INCREMENTO_PENSION"),
                    col("FEC_VIGENCIA_INICIAL").alias("FEC_VIGENCIA_INICIAL"),
                    col("NID_AFILIADO").alias("NID_AFILIADO"),
                    col("NID_COTIZACION").alias("NID_COTIZACION"),
                    col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
                    col("NID_RECUPERACION").alias("NID_RECUPERACION"),
                    col("NID_RENTA_MIGRACION").alias("NID_RENTA_MIGRACION"),
                    col("NID_RENTAS").alias("NID_RENTAS"),
                    col("NID_SIMULACION").alias("NID_SIMULACION"),
                    col("NID_SINIESTRO").alias("NID_SINIESTRO"),
                    col("NUM_DIAS_TRABAJADOS").alias("NUM_DIAS_TRABAJADOS"),
                    col("NUM_ENDOSO_ULT").alias("NUM_ENDOSO_ULT"),
                    col("NUM_MESADAS").alias("NUM_MESADAS"),
                    col("NUM_RELIQUIDACION").alias("NUM_RELIQUIDACION"),
                    col("PC_AD").alias("PC_AD"),
                    col("PC_AD_EXP").alias("PC_AD_EXP"),
                    col("PCJ_ADMINISTRACION").alias("PCJ_ADMINISTRACION"),
                    col("PCJ_GASTOS_EXP").alias("PCJ_GASTOS_EXP"),
                    col("PCJ_INTERES_RESERVA").alias("PCJ_INTERES_RESERVA"),
                    col("PCJ_INTERES_TECNICO").alias("PCJ_INTERES_TECNICO"),
                    col("PCJ_PCL_ACTUAL").alias("PCJ_PCL_ACTUAL"),
                    col("PCJ_PCL_INICIAL").alias("PCJ_PCL_INICIAL"),
                    col("PCJ_PENSION").alias("PCJ_PENSION"),
                    col("PCJ_ULT_INCREMENTO_PENSION").alias("PCJ_ULT_INCREMENTO_PENSION"),
                    col("PC_J12").alias("PC_J12"),
                    col("PT_TK").alias("PT_TK"),
                    col("VLR_ACUMULADO_PAGOS").alias("VLR_ACUMULADO_PAGOS"),
                    col("VLR_BONO_PENSIONAL").alias("VLR_BONO_PENSIONAL"),
                    col("VLR_CAPITAL_VAR").alias("VLR_CAPITAL_VAR"),
                    col("VLR_CUENTA_INDIVIDUAL").alias("VLR_CUENTA_INDIVIDUAL"),
                    col("VLR_EXCESO_SMMLV_IPC").alias("VLR_EXCESO_SMMLV_IPC"),
                    col("VLR_EXCESO_SMMLV_IPC_EXP").alias("VLR_EXCESO_SMMLV_IPC_EXP"),
                    col("VLR_GASTOS_ADM_FIJOS").alias("VLR_GASTOS_ADM_FIJOS"),
                    col("VLR_GASTOS_EXP_FIJOS").alias("VLR_GASTOS_EXP_FIJOS"),
                    col("VLR_INDEMNIZACION_SEGURO").alias("VLR_INDEMNIZACION_SEGURO"),
                    col("VLR_OTROS_APORTES").alias("VLR_OTROS_APORTES"),
                    col("VLR_PAGADO_BONO").alias("VLR_PAGADO_BONO"),
                    col("VLR_PAGADO_GARANTIA").alias("VLR_PAGADO_GARANTIA"),
                    col("VLR_PAGADO_SUMA").alias("VLR_PAGADO_SUMA"),
                    col("VLR_PENSION_ACTUAL").alias("VLR_PENSION_ACTUAL"),
                    col("VLR_PENSION_INICIAL").alias("VLR_PENSION_INICIAL"),
                    col("VLR_PRIMA_CUENTA_INDIVIDUAL").alias("VLR_PRIMA_CUENTA_INDIVIDUAL"),
                    col("VLR_PRIMA_UNICA").alias("VLR_PRIMA_UNICA"),
                    col("VLR_PRIMA_UNICA_BONO").alias("VLR_PRIMA_UNICA_BONO"),
                    col("VLR_PRIMA_UNICA_GARANTIA").alias("VLR_PRIMA_UNICA_GARANTIA"),
                    col("VLR_PRIMA_UNICA_OTROS_APORTES").alias("VLR_PRIMA_UNICA_OTROS_APORTES"),
                    col("VLR_PRIMA_UNICA_SUMA").alias("VLR_PRIMA_UNICA_SUMA"),
                    col("VLR_SALARIO_BASE").alias("VLR_SALARIO_BASE"),
                    trim(col("COD_AFP")).alias("COD_AFP"),
                    trim(col("COD_CATEGORIA_RENTA")).alias("COD_CATEGORIA_RENTA"),
                    trim(col("COD_CAUSA_RETIRO_SUPER")).alias("COD_CAUSA_RETIRO_SUPER"),
                    trim(col("COD_CIUDAD")).alias("COD_CIUDAD"),
                    trim(col("COD_CLASE_RENTA")).alias("COD_CLASE_RENTA"),
                    trim(col("COD_COMPANIA")).alias("COD_COMPANIA"),
                    trim(col("COD_DEPARTAMENTO")).alias("COD_DEPARTAMENTO"),
                    trim(col("COD_ESTADO_PERSONA")).alias("COD_ESTADO_PERSONA"),
                    trim(col("COD_FACTOR_SEGURIDAD")).alias("COD_FACTOR_SEGURIDAD"),
                    trim(col("COD_GRADO_INVALIDEZ")).alias("COD_GRADO_INVALIDEZ"),
                    trim(col("COD_MODALIDAD_RENTA")).alias("COD_MODALIDAD_RENTA"),
                    trim(col("COD_OPCION_CALCULO")).alias("COD_OPCION_CALCULO"),
                    trim(col("COD_ORIGEN")).alias("COD_ORIGEN"),
                    trim(col("COD_ORIGEN_INI")).alias("COD_ORIGEN_INI"),
                    trim(col("COD_ORIGEN_SIN")).alias("COD_ORIGEN_SIN"),
                    trim(col("COD_SUCURSAL_ALFA")).alias("COD_SUCURSAL_ALFA"),
                    trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
                    trim(col("COD_TIPO_OPERACION")).alias("COD_TIPO_OPERACION"),
                    trim(col("EDO")).alias("EDO"),
                    trim(col("ENDOSO_POINT")).alias("ENDOSO_POINT"),
                    trim(col("LUGAR_EXPEDICION")).alias("LUGAR_EXPEDICION"),
                    trim(col("LUGAR_PAGO")).alias("LUGAR_PAGO"),
                    trim(col("MODULO_POINT")).alias("MODULO_POINT"),
                    trim(col("NID_POLIZA_POINT")).alias("NID_POLIZA_POINT"),
                    trim(col("NUM_RESOLUCION")).alias("NUM_RESOLUCION"),
                    trim(col("OBSERVACIONES_FALLOJUD")).alias("OBSERVACIONES_FALLOJUD"),
                    trim(col("RAMO_POINT")).alias("RAMO_POINT"),
                    trim(col("RESULTADO_EJECUCION_CALCULO")).alias("RESULTADO_EJECUCION_CALCULO"),
                    trim(col("SIMBOLO_POINT")).alias("SIMBOLO_POINT"),
                    trim(col("USUARIO_CREACION")).alias("USUARIO_CREACION"),
                    trim(col("USUARIO_SINCRONIZACION")).alias("USUARIO_SINCRONIZACION"),
                    col("FEC_EXPEDICION_PARTITION").alias("FEC_EXPEDICION_PARTITION")
                ),
                lambda df: df.dropDuplicates()
            ]
           },
        "PRV_SALDOS_MATEMATICA": {
            "partition_columns": ["FEC_CORTE_CALCULO_PARTITION"],
            "schema": "PRV",
            "transform": [
                lambda df: df.select(
                    col("BLN_CASO_TRANSICION").alias("BLN_CASO_TRANSICION"),
                    col("FEC_CORTE_CALCULO").alias("FEC_CORTE_CALCULO"),
                    col("MAX").alias("MAX"),
                    col("MES_DIFERIDO_TRANSICION").alias("MES_DIFERIDO_TRANSICION"),
                    col("NID_CALCULO").alias("NID_CALCULO"),
                    col("NID_RENTAS").alias("NID_RENTAS"),
                    col("PCJ_APLICADO_TRANSICION").alias("PCJ_APLICADO_TRANSICION"),
                    col("PERIODO").alias("PERIODO"),
                    trim(col("PERIODO_INGRESO")).alias("PERIODO_INGRESO"),
                    col("VLR_DIFERENCIA_RESOLUCION").alias("VLR_DIFERENCIA_RESOLUCION"),
                    col("VLR_PENSION_REAL").alias("VLR_PENSION_REAL"),
                    col("VLR_RESERVA_CALCULADA").alias("VLR_RESERVA_CALCULADA"),
                    col("VLR_RESERVA_CONTABLE").alias("VLR_RESERVA_CONTABLE"),
                    col("FEC_CORTE_CALCULO_PARTITION").alias("FEC_CORTE_CALCULO_PARTITION")
                ),
                lambda df: df.dropDuplicates()
            ]
               }
                ################
                ### FIN  SRC ###
                ################

           ###################
           #### FIN Renta ####
           ###################
       
    }

    ###################################################################
    #################### save stage silver ############################
    ###################################################################
    
    
    for table, config in tables_case_use.items():
        
        partition_columns = config["partition_columns"]
        transform_functions = config["transform"]
        schema = config["schema"]
        
        if schema in DB_SCHEMA:
        
            logger.info(f"Transform data from table : {table}")
            
            # Load the table data as a DataFrame
            table_name = f"{DB_NAME}_{schema}_{table}"
            logger.info(f"Transform data from table : {table_name}")
            df_from_bronce = glueContext.create_dynamic_frame.from_catalog(
                database=f"{DB_CATALOG_GLUE}",
                table_name=table_name
            ).toDF()
            
            #Initialize of df to transform
            df_to_silver = df_from_bronce
            
            # Apply each transformation in sequence
            for transform_function in transform_functions:
                df_to_silver = transform_function(df_to_silver)
            
            frame_save = DynamicFrame.fromDF(df_to_silver, glueContext, "frame_save_dynamic")
            
        
            s3_prefix = f"{DB_NAME}/{schema}/SILVER_{table}/"
                
            s3_path = f"s3://{BUCKET_NAME}/{s3_prefix}"
                
            logger.info(f"Truncate & Saving data to: {s3_path}")
            
            extract_helper.delete_s3_folder(BUCKET_NAME, s3_prefix)
            extract_helper.save_files_s3(frame_save, s3_path, partition_columns)
           
except Exception as e:
    print(traceback.format_exc())
    logger.error(f"An error occurred: {e}")
    