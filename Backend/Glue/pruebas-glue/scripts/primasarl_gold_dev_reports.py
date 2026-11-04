"""Script that load gold phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim, date_format, lit, max, to_date, sum, when, concat_ws, date_format, broadcast
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
PERIOD = json.loads(args['PERIOD'])

BUCKET_NAME = args['BUCKET_NAME']
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
DB_NAME = args['DB_NAME']
DB_SCHEMA = args['DB_SCHEMA']
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
    schema_primas_arl_gold = StructType([
        StructField("Nid_poliza", IntegerType(), True),  # Numeric
        StructField("Vlr_prima", FloatType(), True),  # AAAAMM (Date format stored as String)
        StructField("Fec_expedicion", DateType(), True),  # String with specific categories
        StructField("Per_expedicion", StringType(), True),
        StructField("Fec_proceso_point", DateType(), True),  # Numeric
        StructField("Per_proceso_point", StringType(), True),
        StructField("Unidad_de_cuenta", StringType(), True),# AAAAMM (Date format stored as String)
        StructField("PERIODO_PARTITION", StringType(), True)  # AAAAMM (Date format stored as String)
    ])

    #Read tables from silver schema arpprod
    df_gar_tipos_endosos_arp = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_GAR_TIPOS_ENDOSOS_ARP").toDF()
    df_hist_datos_invalidez_muerte = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_HIST_DATOS_INVALIDEZ_MUERTE").toDF()
    df_par_afiliaciones = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_PAR_AFILIACIONES").toDF()
    df_par_endosos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_PAR_ENDOSOS").toDF()
    df_par_polizas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_PAR_POLIZAS").toDF()
    df_par_trabajadores = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_PAR_TRABAJADORES").toDF()
    df_sar_siniestros = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_SINIESTROS").toDF()
    
    #Read tables from silver schema general
    
    df_acu_personas_juridicas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ACU_PERSONAS_JURIDICAS").toDF()
    df_acu_personas_naturales = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ACU_PERSONAS_NATURALES").toDF()
    df_cgr_sucursales_alfa = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_SUCURSALES_ALFA").toDF()
    df_cgr_tipos_endosos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_TIPOS_ENDOSOS").toDF()
    df_rva_reserva_matematica_transic = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_RVA_RESERVA_MATEMATICA_TRANSIC").toDF()
    
    logger.info(f"Read tables from source bronce")
    
    #-----------------------------------------------------------------#
    #-------------------- filter when applied ------------------------#
    #-----------------------------------------------------------------#
    
    if PERIOD != 0:
        logger.info(f"Create report with periods {PERIOD}")
        df_par_endosos = df_par_endosos.filter(df_par_endosos["PERIODO_POINT"].isin(PERIOD))
        
    else:
        logger.info(f"Create full report")
        df_par_endosos = df_par_endosos
        
    ###################################################################
    ############## Init Businees Logic Primas Arl #####################
    ###################################################################
    
    #-----------------------------------------------------------------#
    #-------------------- subquerie PAR_ENDOSOS ----------------------#
    #-----------------------------------------------------------------#
    
    #join par_endosos ->  cgr_tipos_endosos -> gar_tipos_endosos_arp -> par_polizas -> cgr_sucursales_alfa -> par_afiliaciones -> acu_personas_juridicas -> acu_personas_naturales -> par_trabajadores -> sar_siniestros -> hist_datos_invalidez_muerte -> rva_reserva_matematica_transic
    
    df_join_par_endosos = df_par_endosos.join(
        broadcast(df_cgr_tipos_endosos),
        df_par_endosos["cod_tipo_endoso"] == df_cgr_tipos_endosos["cod_tipo_endoso"],
        "inner"
    )
    df_join_par_endosos = df_join_par_endosos.join(
        broadcast(df_gar_tipos_endosos_arp),
        df_par_endosos["cod_tipo_endoso_arp"] == df_gar_tipos_endosos_arp["cod_tipo_endoso_arp"],
        "inner"
    )
    
    df_join_par_endosos = df_join_par_endosos.join(
        df_par_polizas,
        (df_par_endosos["cod_sucursal_alfa"] == df_par_polizas["cod_sucursal_alfa"]) & (df_par_endosos["nid_poliza"] == df_par_polizas["nid_poliza"]) & (df_par_endosos["modulo"] == df_par_polizas["modulo"]),
        "inner"
    )
    
    df_join_par_endosos = df_join_par_endosos.join(
        broadcast(df_cgr_sucursales_alfa),
        df_par_endosos["cod_sucursal_alfa"] == df_cgr_sucursales_alfa["cod_sucursal_alfa"],
        "inner"
    )
    
    df_join_par_endosos = df_join_par_endosos.join(
        df_par_afiliaciones,
        df_par_endosos["nid_poliza"] == df_par_afiliaciones["nid_afiliacion"],
        "inner"
    )
    
    df_join_par_endosos = df_join_par_endosos.join(
        df_acu_personas_juridicas,
        (df_par_afiliaciones["cod_tipo_identificacion"] == df_acu_personas_juridicas["cod_tipo_identificacion"]) & (df_par_afiliaciones["nid_numero_identificacion"] == df_acu_personas_juridicas["nid_numero_identificacion"]) ,
        "left"
    )
    
    
    df_join_par_endosos = df_join_par_endosos.join(
        df_acu_personas_naturales,
        (df_par_afiliaciones["cod_tipo_identificacion"] == df_acu_personas_naturales["cod_tipo_identificacion"]) & (df_par_afiliaciones["nid_numero_identificacion"] == df_acu_personas_naturales["nid_numero_identificacion"]) ,
        "left"
    )
    
    
    df_join_par_endosos = df_join_par_endosos.join(
        df_par_trabajadores,
        df_par_endosos["nid_poliza"] == df_par_trabajadores["nid_afiliacion"],
        "left"
    )
    
    
    df_join_par_endosos = df_join_par_endosos.join(
        df_sar_siniestros,
        df_sar_siniestros["nid_trabajador"] == df_par_trabajadores["nid_trabajador"],
        "left"
    )
    
    
    df_join_par_endosos = df_join_par_endosos.join(
        broadcast(df_hist_datos_invalidez_muerte),
        df_sar_siniestros["nid_siniestro"] == df_hist_datos_invalidez_muerte["nid_siniestro"],
        "left"
    )
    
    df_join_par_endosos = df_join_par_endosos.join(
        df_rva_reserva_matematica_transic,
        df_sar_siniestros["nid_siniestro"] == df_rva_reserva_matematica_transic["nid_siniestro_renta"],
        "left"
    )
    
    
    #select subquerie PAR_ENDOSOS

    df_select_subquerie_par_endosos = df_join_par_endosos.select (
        df_par_endosos["nid_poliza"],
        df_par_endosos["nid_endoso"],
        df_par_endosos["vlr_prima"],
        df_par_endosos["fec_expedicion"],
        df_par_endosos["periodo"].alias("periodo_expedicion"),
        df_par_endosos["fec_proceso_point"],
        df_par_endosos["periodo_point"],
        df_par_endosos["cod_tipo_endoso"],
        df_par_endosos["cod_tipo_endoso_arp"],
        df_par_endosos["cod_sucursal_alfa"],
        df_cgr_sucursales_alfa["nombre"].alias("nom_sucursal_alfa"),
        df_par_endosos["modulo"],
        df_par_afiliaciones["nid_afiliacion"],
        df_par_afiliaciones["cod_tipo_identificacion"],
        df_par_afiliaciones["nid_numero_identificacion"],
        df_acu_personas_juridicas["cod_tipo_identificacion"].alias("apj_cod_tipo_identificacion"),
        df_acu_personas_juridicas["nid_numero_identificacion"].alias("apj_nid_numero_identificacion"),
        df_acu_personas_juridicas["nombre_comercial"],
        df_acu_personas_juridicas["razon_social"],
        df_acu_personas_naturales["cod_tipo_identificacion"].alias("apn_cod_tipo_identificacion"),
        df_acu_personas_naturales["nid_numero_identificacion"].alias("apn_nid_numero_identificacion"),
        df_acu_personas_naturales["primer_apellido"],
        df_acu_personas_naturales["segundo_apellido"],
        df_acu_personas_naturales["primer_nombre"],
        df_acu_personas_naturales["segundo_nombre"],
        when(df_acu_personas_juridicas["cod_tipo_identificacion"].isNotNull() & df_acu_personas_naturales["cod_tipo_identificacion"].isNotNull(), 1)
            .otherwise(0)
            .alias("val_razon_natural"),
        df_par_trabajadores["nid_trabajador"],
        df_sar_siniestros["nid_siniestro"]
    ).distinct()
    
    
    df_primas_arl_query = df_select_subquerie_par_endosos.select (
        df_select_subquerie_par_endosos["nid_poliza"],
        df_select_subquerie_par_endosos["nid_endoso"],
        df_select_subquerie_par_endosos["vlr_prima"],
        df_select_subquerie_par_endosos["fec_expedicion"],
        df_select_subquerie_par_endosos["periodo_expedicion"],
        df_select_subquerie_par_endosos["fec_proceso_point"],
        df_select_subquerie_par_endosos["periodo_point"],
        df_select_subquerie_par_endosos["cod_tipo_endoso"],
        df_select_subquerie_par_endosos["cod_tipo_endoso_arp"],
        df_select_subquerie_par_endosos["cod_sucursal_alfa"],
        df_select_subquerie_par_endosos["nom_sucursal_alfa"],
        df_select_subquerie_par_endosos["modulo"],
        df_select_subquerie_par_endosos["nid_afiliacion"],
        df_select_subquerie_par_endosos["cod_tipo_identificacion"],
        df_select_subquerie_par_endosos["nid_numero_identificacion"],
        
        when(df_select_subquerie_par_endosos["APJ_COD_TIPO_IDENTIFICACION"].isNull(),df_select_subquerie_par_endosos["APN_COD_TIPO_IDENTIFICACION"])
        .otherwise(df_select_subquerie_par_endosos["APJ_COD_TIPO_IDENTIFICACION"])
        .alias("cod_tipo_identificacion_tercero"),
        
        when(df_select_subquerie_par_endosos["APJ_NID_NUMERO_IDENTIFICACION"].isNull(),df_select_subquerie_par_endosos["APN_NID_NUMERO_IDENTIFICACION"])
        .otherwise(df_select_subquerie_par_endosos["APJ_NID_NUMERO_IDENTIFICACION"])
        .alias("nid_numero_identificacion_tercero"),
        
        when(df_select_subquerie_par_endosos["apj_cod_tipo_identificacion"].isNotNull() & df_select_subquerie_par_endosos["apj_nid_numero_identificacion"].isNotNull() & df_select_subquerie_par_endosos["nombre_comercial"].isNotNull() & (df_select_subquerie_par_endosos["nombre_comercial"] == df_select_subquerie_par_endosos["razon_social"]),df_select_subquerie_par_endosos["nombre_comercial"])
        .when(df_select_subquerie_par_endosos["apj_cod_tipo_identificacion"].isNotNull() & df_select_subquerie_par_endosos["apj_nid_numero_identificacion"].isNotNull() & df_select_subquerie_par_endosos["nombre_comercial"].isNull() ,df_select_subquerie_par_endosos["razon_social"])
        .when(df_select_subquerie_par_endosos["apj_cod_tipo_identificacion"].isNotNull() & df_select_subquerie_par_endosos["apj_nid_numero_identificacion"].isNotNull() & df_select_subquerie_par_endosos["nombre_comercial"].isNotNull() & (df_select_subquerie_par_endosos["nombre_comercial"] != df_select_subquerie_par_endosos["razon_social"]),df_select_subquerie_par_endosos["razon_social"])
        .otherwise(concat_ws(" ", 
            df_select_subquerie_par_endosos["primer_apellido"], 
            df_select_subquerie_par_endosos["segundo_apellido"], 
            df_select_subquerie_par_endosos["primer_nombre"], 
            df_select_subquerie_par_endosos["segundo_nombre"] ))
        .alias("nom_tercero"),    
            
        df_select_subquerie_par_endosos["val_razon_natural"],
        df_select_subquerie_par_endosos["nid_trabajador"],
        df_select_subquerie_par_endosos["nid_siniestro"]
    ).distinct()

    df_primas_arl_query = df_primas_arl_query.withColumn("PERIODO_PARTITION",date_format(to_date(col("periodo_point"), "yyyyMM"), "yyyy-MM"))
    
    df_primas_arl_gold = df_primas_arl_query.select(
    
        df_primas_arl_query["nid_poliza"],
        df_primas_arl_query["nid_endoso"],
        df_primas_arl_query["vlr_prima"],
        df_primas_arl_query["fec_expedicion"].cast(DateType()),
        df_primas_arl_query["periodo_expedicion"].alias("per_expedicion"),
        df_primas_arl_query["fec_proceso_point"].cast(DateType()),
        df_primas_arl_query["periodo_point"].alias("per_proceso_point"),
        df_primas_arl_query["cod_tipo_endoso"],
        df_primas_arl_query["cod_tipo_endoso_arp"],
        df_primas_arl_query["cod_sucursal_alfa"],
        df_primas_arl_query["nom_sucursal_alfa"],
        df_primas_arl_query["modulo"],
        df_primas_arl_query["nid_afiliacion"],
        df_primas_arl_query["cod_tipo_identificacion_tercero"],
        df_primas_arl_query["nid_numero_identificacion_tercero"],
        df_primas_arl_query["nom_tercero"],
        df_primas_arl_query["nid_trabajador"],
        df_primas_arl_query["nid_siniestro"],
        lit(None).cast(IntegerType()).alias("id_lote_cargue"),
        df_primas_arl_query["PERIODO_PARTITION"]
    )

    ###################################################################
    ##################### Save files Data #############################
    ###################################################################
    
    partition_columns = ["PERIODO_PARTITION"]
    
    distinct_partitions = [row[0] for row in df_primas_arl_gold.select(f"{partition_columns[0]}").distinct().collect()]
    
    # Save the transformed DataFrame to S3
    frame_save = DynamicFrame.fromDF(df_primas_arl_gold, glueContext, "frame_save_dynamic")
    
    s3_prefix = f"GOLD/NIIF/{TABLE_NAME_GOLD}/"
                
    s3_path = f"s3://{BUCKET_NAME}/{s3_prefix}"
    
    
    for period in distinct_partitions:
        specific_partition_prefix = f"{s3_prefix}{partition_columns[0]}={period}/"
        logger.info(f"Truncate data for partitions {period} to: {specific_partition_prefix}")
        extract_helper.delete_s3_folder(BUCKET_NAME, specific_partition_prefix)
        
    logger.info(f"Saving data to: {s3_path}")
    
    try:
        extract_helper.save_files_s3(frame_save, s3_path, partition_columns)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        
    logger.info(f"Truncate and save complete. Data written to: {s3_path}")
    
except Exception as e:
    logger.error(f"An error occurred: {e}")
    