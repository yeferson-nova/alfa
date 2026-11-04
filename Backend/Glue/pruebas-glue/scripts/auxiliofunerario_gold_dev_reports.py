"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

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
    schema_auxilios_gold = StructType([
        StructField("Numero_de_poliza", IntegerType(), True),  # Numeric
        StructField("Periodo_de_expedicion_de_la_poliza", StringType(), True),  # AAAAMM (Date format stored as String)
        StructField("Origen_de_la_renta", StringType(), True),  # String with specific categories
        StructField("Valor_auxilio_funerario_pagado", FloatType(), True),  # Numeric
        StructField("Mes_de_pago", StringType(), True),  # AAAAMM (Date format stored as String)
        StructField("PERIODO_PARTITION", StringType(), True)  # AAAAMM (Date format stored as String)
    ])
    
    
    #Read tables from silver
    df_anp_detalle_solicitudes_giro = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_DETALLE_SOLICITUDES_GIRO").toDF()
    df_anp_distribucion_vig_pension = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_DISTRIBUCION_VIG_PENSION").toDF()
    df_anp_pensionado_novedad = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PENSIONADO_NOVEDAD").toDF()
    df_anp_pensiones = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PENSIONES").toDF()
    df_anp_periodo_solicitudes_giro = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PERIODO_SOLICITUDES_GIRO").toDF()
    df_cgr_conceptos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_CONCEPTOS").toDF()
    df_cgr_origenes = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_ORIGENES").toDF() 
    
    
    ###################################################################
    ################### Init Businees Logic ###########################
    ###################################################################
    
    logger.info(f"Writing to {TABLE_NAME_GOLD} with period = {PERIOD}")
    
    
    #-----------------------------------------------------------------#
    #------------- subquerie anp_pensionado_novedad ------------------#
    #-----------------------------------------------------------------#
    
    #join anp_pensionado_novedad ->  anp_distribucion_vig_pension -> anp_pensiones -> cgr_origenes
    
    df_join_anp_pensionado = df_anp_pensionado_novedad.join(
        df_anp_distribucion_vig_pension,
        df_anp_pensionado_novedad["nid_distribucion_vigencia"] == df_anp_distribucion_vig_pension["nid_distribucion_vigencia"],
        "inner"
    )
    df_join_anp_pensionado = df_join_anp_pensionado.join(
        df_anp_pensiones,
        df_anp_distribucion_vig_pension["nid_pension"] == df_anp_pensiones["nid_pension"],
        "inner"
    )
    
    df_join_anp_pensionado = df_join_anp_pensionado.join(
        df_cgr_origenes,
        df_anp_pensiones["cod_origen"] == df_cgr_origenes["cod_origen"],
        "inner"
    )
    
    #select 
    df_select_subquerie_anp_pensionado_novedad = df_join_anp_pensionado.select(
        df_anp_pensionado_novedad["nid_numero_identificacion"],
        df_anp_pensionado_novedad["cod_tipo_identificacion"],
        df_anp_pensiones["nid_procedencia"],
        df_anp_pensiones["fec_expedicion"],
        df_cgr_origenes["nombre"],
        df_anp_pensionado_novedad["vlr_total"],
        df_anp_pensionado_novedad["cod_concepto"]
    )
    
    #-----------------------------------------------------------------#
    #-------- subquerie anp_periodo_solicitudes_giro -----------------#
    #-----------------------------------------------------------------#
    
    #join anp_periodo_solicitudes_giro ->  cgr_conceptos -> anp_detalle_solicitudes_giro
    
    
    df_join_anp_periodo_solicitudes_giro = df_anp_periodo_solicitudes_giro.join(
        df_cgr_conceptos,
        df_anp_periodo_solicitudes_giro["cod_concepto_anp"] == df_cgr_conceptos["cod_concepto"],
        "inner"
    )
    df_join_anp_periodo_solicitudes_giro = df_join_anp_periodo_solicitudes_giro.join(
        df_anp_detalle_solicitudes_giro,
        df_anp_periodo_solicitudes_giro["nid_periodo_solicitud_giro"] == df_anp_detalle_solicitudes_giro["nid_periodo_solicitud_giro"],
        "inner"
    )
    
    #select 
    df_select_subquerie_anp_periodo_solicitudes_giro = df_join_anp_periodo_solicitudes_giro.select(
        df_anp_detalle_solicitudes_giro["nid_numero_identificacion"],
        df_anp_detalle_solicitudes_giro["cod_tipo_identificacion"],
        df_anp_periodo_solicitudes_giro["num_solicitud_tes"],
        df_cgr_conceptos["nombre_concepto"],
        df_anp_periodo_solicitudes_giro["fec_envio_tes"],
        df_anp_periodo_solicitudes_giro["bln_anulado_point"]
    )

    #-----------------------------------------------------------------#
    #--------------------- Inner across subqueries -------------------#
    #-----------------------------------------------------------------#    
    
    
    #join df_select_subquerie_anp_pensionado_novedad ->  df_select_subquerie_anp_periodo_solicitudes_giro 
    
    df_join_final_across_subqueries = df_select_subquerie_anp_pensionado_novedad.join(
        df_select_subquerie_anp_periodo_solicitudes_giro,
        (df_select_subquerie_anp_pensionado_novedad["nid_numero_identificacion"] == df_select_subquerie_anp_periodo_solicitudes_giro["nid_numero_identificacion"]) &
        (df_select_subquerie_anp_pensionado_novedad["cod_tipo_identificacion"] == df_select_subquerie_anp_periodo_solicitudes_giro["cod_tipo_identificacion"]),
        "inner" 
    )
    
    
    
    #-----------------------------------------------------------------#
    #--------------------- Select Final ------------------------------#
    #-----------------------------------------------------------------#
    
    df_auxilios_funerarios_query = df_join_final_across_subqueries.select(
        df_select_subquerie_anp_pensionado_novedad["nid_numero_identificacion"].alias("id_afiliado"),
        df_select_subquerie_anp_pensionado_novedad["cod_tipo_identificacion"].alias("tipo_identificacion_afiliado"),
        df_select_subquerie_anp_periodo_solicitudes_giro["num_solicitud_tes"],
        df_select_subquerie_anp_periodo_solicitudes_giro["nombre_concepto"],
        df_select_subquerie_anp_pensionado_novedad["nid_procedencia"].alias("numero_poliza"),
        df_select_subquerie_anp_pensionado_novedad["fec_expedicion"],
        df_select_subquerie_anp_pensionado_novedad["nombre"].alias("origen"),
        df_select_subquerie_anp_pensionado_novedad["vlr_total"].alias("valor_total"),
        df_select_subquerie_anp_pensionado_novedad["cod_concepto"],
        df_select_subquerie_anp_periodo_solicitudes_giro["bln_anulado_point"],
        df_select_subquerie_anp_periodo_solicitudes_giro["fec_envio_tes"]
    ).distinct()  # Ensure DISTINCT rows
    
    #-----------------------------------------------------------------#
    #----------------------- add partition ---------------------------#
    #-----------------------------------------------------------------#

    #Add partition to table 
    df_auxilios_funerarios_query = df_auxilios_funerarios_query.withColumn("PERIODO_PARTITION", date_format(df_auxilios_funerarios_query["fec_expedicion"], "yyyy-MM"))
    

    #Select final dataframe for gold stage
    df_auxilios_gold = df_auxilios_funerarios_query.select(
        df_auxilios_funerarios_query["id_afiliado"],
        df_auxilios_funerarios_query["tipo_identificacion_afiliado"],
        df_auxilios_funerarios_query["num_solicitud_tes"],
        df_auxilios_funerarios_query["nombre_concepto"],
        df_auxilios_funerarios_query["numero_poliza"],
        df_auxilios_funerarios_query["fec_expedicion"].cast(DateType()),
        df_auxilios_funerarios_query["origen"],
        df_auxilios_funerarios_query["valor_total"],
        df_auxilios_funerarios_query["cod_concepto"],
        df_auxilios_funerarios_query["bln_anulado_point"],
        df_auxilios_funerarios_query["fec_envio_tes"].cast(DateType()),
        df_auxilios_funerarios_query["PERIODO_PARTITION"].alias("PERIODO_PARTITION")
    )
    
    ###################################################################
    ##################### Save files Data #############################
    ###################################################################
    
    partition_columns = ["PERIODO_PARTITION"]
    
    # Save the transformed DataFrame to S3
    frame_save = DynamicFrame.fromDF(df_auxilios_gold, glueContext, "frame_save_dynamic")
    
    s3_prefix = f"GOLD/NIIF/{TABLE_NAME_GOLD}/"
                
    s3_path = f"s3://{BUCKET_NAME}/{s3_prefix}"
    
    logger.info(f"Truncate & Saving data to: {s3_path}")
        
    extract_helper.delete_s3_folder(BUCKET_NAME, s3_prefix)
    
    extract_helper.save_files_s3(frame_save, s3_path, partition_columns)
        
except Exception as e:
    logger.error(f"An error occurred: {e}")
    