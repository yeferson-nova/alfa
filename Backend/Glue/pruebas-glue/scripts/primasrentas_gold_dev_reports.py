"""Script that load gold phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim, date_format, lit, max
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
    df_acu_personas_naturales = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ACU_PERSONAS_NATURALES").toDF()
    df_cgr_origenes = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_ORIGENES").toDF()
    df_cgr_tipos_identificaciones = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_TIPOS_IDENTIFICACIONES").toDF()
    
    #Read tables from silver schema prv
    df_prv_endosos_point = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_ENDOSOS_POINT").toDF()
    df_prv_rentas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_RENTAS").toDF()
    df_prv_saldos_matematica = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_SALDOS_MATEMATICA").toDF()
    
    
    ###################################################################
    ############ Init Businees Logic CAMPOS_ANEXO1 ####################
    ###################################################################
    
    logger.info(f"Writing to {TABLE_NAME_GOLD} with period = {PERIOD}")
    logger.info(f"Make logic to CAMPOS_ANEXO1 ")

    #-----------------------------------------------------------------#
    #---------------------- Join Tables ------------------------------#
    #-----------------------------------------------------------------#
    #join prv_rentas ->  prv_saldos_matematica -> acu_personas_naturales -> cgr_origenes
    
    # Join prv_rentas with prv_saldos_matematica (LEFT JOIN)
    df_join_prv_rentas = df_prv_rentas.join(
        df_prv_saldos_matematica,
        df_prv_rentas["nid_rentas"] == df_prv_saldos_matematica["nid_rentas"],
        "left"
    )
    
    # Join result with acu_personas_naturales (INNER JOIN)
    df_join_prv_rentas = df_join_prv_rentas.join(
        df_acu_personas_naturales,
        (df_prv_rentas["cod_tipo_identificacion"] == df_acu_personas_naturales["cod_tipo_identificacion"]) &
        (df_prv_rentas["nid_numero_identificacion"] == df_acu_personas_naturales["nid_numero_identificacion"]),
        "inner"
    )
    
    # Join result with cgr_origenes (INNER JOIN)
    df_join_prv_rentas = df_join_prv_rentas.join(
        df_cgr_origenes,
        df_prv_rentas["cod_origen"] == df_cgr_origenes["cod_origen"],
        "inner"
    )
    
    # filter where fec_invalidacion_registro is null
    df_join_prv_rentas_filtered = df_join_prv_rentas.filter(
        df_prv_rentas["fec_invalidacion_registro"].isNull()
    )
    
    #Make aggregated for max id nid_calculo groupBy 
    df_join_prv_rentas_aggregated = (
        df_join_prv_rentas_filtered.groupBy(
            df_prv_saldos_matematica["nid_rentas"],
            df_prv_rentas["fec_expedicion"],
            df_prv_rentas["fec_envio_point"],
            df_cgr_origenes["nombre"],
            df_prv_rentas["vlr_prima_unica"]
    ).agg(
        max(col("nid_calculo")).alias("max_nid_calculo"))  # Take max id with groupBy
    )
    
    # filter where agg max_nid_calculo is not null
    df_join_prv_rentas_aggregated_filtered = df_join_prv_rentas_aggregated.filter(
        df_join_prv_rentas_aggregated['max_nid_calculo'].isNotNull()
    )
    
    
    # Select DISTINCT rows with required columns
    df_campos_anexos_1 = df_join_prv_rentas_aggregated_filtered.select(
        df_prv_saldos_matematica["nid_rentas"],
        df_join_prv_rentas_aggregated["max_nid_calculo"].alias("nid_calculo"),
        df_prv_rentas["fec_expedicion"],
        df_prv_rentas["fec_envio_point"],
        df_cgr_origenes["nombre"].alias("tipo_renta"),
        df_prv_rentas["vlr_prima_unica"]
    ).distinct()  # Ensure distinct rows
    
    #-----------------------------------------------------------------#
    #----------------------- Transforms ------------------------------#
    #-----------------------------------------------------------------#
    
    #Add cols tipo_proceso, y unidad__de_cuenta    
    df_campos_anexos_1 = df_campos_anexos_1.withColumn("Tipo_proceso", lit("Renta nueva"))\
        .withColumn("Unidad_de_cuenta", lit(""))
    
    #Select final dataframe for gold stage
    df_campos_anexos_1 = df_campos_anexos_1.select(
        df_campos_anexos_1["nid_rentas"].alias("Numero_de_renta"),          # NID_RENTAS from PRV_SALDOS_MATEMATICA
        df_campos_anexos_1["vlr_prima_unica"].alias("Valor_prima"),      # vlr_prima_unica from PRV_RENTAS
        df_campos_anexos_1["tipo_renta"].alias("Tipo_de_renta"),             # Nombre from CGR_ORIGENES (joined via PRV_RENTAS.cod_origen)
        df_campos_anexos_1["fec_expedicion"].alias("Fecha_expedicion"),  # FEC_EXPEDICION from PRV_RENTAS
        df_campos_anexos_1["fec_envio_point"].alias("Periodo_point"),    # FEC_ENVIO_POINT from PRV_RENTAS
        df_campos_anexos_1["Tipo_proceso"].alias("Tipo_proceso"),        # Tipo_proceso from N/A
        df_campos_anexos_1["Unidad_de_cuenta"].alias("Unidad_de_cuenta") # Unidad_de_cuenta from N/A
    )
    
    
    
    ###################################################################
    ############ Init Businees Logic CAMPOS_ANEXO2 ####################
    ###################################################################
    logger.info(f"Make logic to CAMPOS_ANEXO2 ")
    
    #-----------------------------------------------------------------#
    #------------------------ SubQuery -------------------------------#
    #-----------------------------------------------------------------#
    
    df_filter_prv_endosos_point = df_prv_endosos_point.filter(
        (df_prv_endosos_point["bln_proceso"] == "S" ) &                                         # Filter for specific concept bln_proceso
        (df_prv_endosos_point["vlr_total_recaudo"] != df_prv_endosos_point["vlr_total"] ) &     # Compare columns for vlr_total_recaudo & vlr_total
        (df_prv_endosos_point["nid_endoso_reversion"].isNull()) &                               # Ensure nid_endoso_reversion is null
        (df_prv_endosos_point["nid_endoso_reversado"].isNull())                                 # Ensure nid_endoso_reversado is null
    )
    
    
   #-----------------------------------------------------------------#
   #---------------------- Join Tables ------------------------------#
   #-----------------------------------------------------------------#
   # Subquery -> prv_rentas -> cgr_origenes
    
    df_join_prv_endosos_point = df_filter_prv_endosos_point.join(
        df_prv_rentas,
        df_filter_prv_endosos_point["nid_rentas"] == df_prv_rentas["nid_rentas"],
        "left"
    )
    
    df_join_prv_endosos_point = df_join_prv_endosos_point.join(
        df_cgr_origenes,
        df_prv_rentas["cod_origen"] == df_cgr_origenes["cod_origen"],
        "left"
    )
        
    # Select rows with required columns
    df_campos_anexos_2 = df_join_prv_endosos_point.select(
        df_filter_prv_endosos_point["nid_rentas"],
        df_filter_prv_endosos_point["vlr_total"],
        df_cgr_origenes["nombre"],
        df_filter_prv_endosos_point["fec_expedicion"],
        df_filter_prv_endosos_point["fec_envio_point"],
    )
    
    #-----------------------------------------------------------------#
    #----------------------- Transforms ------------------------------#
    #-----------------------------------------------------------------#
    
    #Add cols tipo_proceso, y unidad__de_cuenta    
    df_campos_anexos_2 = df_campos_anexos_2.withColumn("Tipo_proceso", lit("Endoso"))\
        .withColumn("Unidad_de_cuenta", lit(""))
        
    #Select final dataframe for gold stage
    df_campos_anexos_2 = df_campos_anexos_2.select(
        df_campos_anexos_2["nid_rentas"].alias("Numero_de_renta"),          # NID_RENTAS from PRV_ENDOSOS_POINT
        df_campos_anexos_2["vlr_total"].alias("Valor_prima"),            # vlr_prima_unica from PRV_ENDOSOS_POINT
        df_campos_anexos_2["nombre"].alias("Tipo_de_renta"),             # Nombre from CGR_ORIGENES (joined via PRV_RENTAS.cod_origen)
        df_campos_anexos_2["fec_expedicion"].alias("Fecha_expedicion"),  # FEC_EXPEDICION from PRV_ENDOSOS_POINT
        df_campos_anexos_2["fec_envio_point"].alias("Periodo_point"),    # FEC_ENVIO_POINT from PRV_ENDOSOS_POINT
        df_campos_anexos_2["Tipo_proceso"].alias("Tipo_proceso"),        # Tipo_proceso from N/A
        df_campos_anexos_2["Unidad_de_cuenta"].alias("Unidad_de_cuenta") # Unidad_de_cuenta from N/A
    )
    
    ###################################################################
    ########### UNION TABLE CAMPOS_ANEXO1 & CAMPOS_ANEXO2 #############
    ###################################################################
    
    df_primas_rentas_query = df_campos_anexos_1.union(df_campos_anexos_2)
    
    #-----------------------------------------------------------------#
    #----------------------- Transforms ------------------------------#
    #-----------------------------------------------------------------#
    
    #Add partition to table 
    df_primas_rentas_query = df_primas_rentas_query.withColumn("PERIODO_PARTITION", date_format(df_primas_rentas_query["Periodo_point"], "yyyy-MM"))
    
    #Add date AAAAMM
    
    #modify dates for table 
    df_primas_rentas_query = df_primas_rentas_query.withColumn("Periodo_point", date_format(df_primas_rentas_query["Periodo_point"], "yyyyMM"))
    
    
    #Select final dataframe for gold stage
    df_primas_rentas_gold = df_primas_rentas_query.select(
        df_primas_rentas_query["Numero_de_renta"].alias("Numero_de_renta"),
        df_primas_rentas_query["Valor_prima"].alias("Valor_prima"),
        df_primas_rentas_query["Tipo_de_renta"].alias("Tipo_de_renta"),
        df_primas_rentas_query["Fecha_expedicion"].cast(DateType()).alias("Fecha_expedicion"),
        df_primas_rentas_query["Periodo_point"].alias("Periodo_point"),
        df_primas_rentas_query["Tipo_proceso"].alias("Tipo_proceso"),
        df_primas_rentas_query["Unidad_de_cuenta"].alias("Unidad_de_cuenta"),
        df_primas_rentas_query["PERIODO_PARTITION"].alias("PERIODO_PARTITION")
    )
    
    
    ###################################################################
    ##################### Save files Data #############################
    ###################################################################
    
    partition_columns = ["PERIODO_PARTITION"]
    
    # Save the transformed DataFrame to S3
    frame_save = DynamicFrame.fromDF(df_primas_rentas_gold, glueContext, "frame_save_dynamic")
    
    s3_prefix = f"GOLD/NIIF/{TABLE_NAME_GOLD}/"
                
    s3_path = f"s3://{BUCKET_NAME}/{s3_prefix}"
    
    logger.info(f"Truncate & Saving data to: {s3_path}")
        
    extract_helper.delete_s3_folder(BUCKET_NAME, s3_prefix)
    
    extract_helper.save_files_s3(frame_save, s3_path, partition_columns)
        
except Exception as e:
    logger.error(f"An error occurred: {e}")
    