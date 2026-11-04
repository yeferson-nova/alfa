"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.transforms import *
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality
from commons.LogsHelper import LogsHelper
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, current_timestamp, current_date
from pyspark.sql.window import Window
#import botocore

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
        'DB_SCHEMA',
        'DB_NAME',
        'DB_CATALOG_GLUE',
        'BUCKET_NAME',
        'BUCKET_NAME_ASSETS',
        'CONNECT_NAME', 
        'PERIOD',
        'CERTIFICATE'
        ]
    )
DB_SCHEMA = args['DB_SCHEMA']
DB_NAME = args['DB_NAME']
DB_CATALOG_GLUE = args['DB_CATALOG_GLUE']
BUCKET_NAME = args['BUCKET_NAME']
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
CONNECT_NAME = args['CONNECT_NAME']
PERIOD = args['PERIOD']
CERTIFICATE = args['CERTIFICATE']
PATH_JSON = f"support_phase_files/calidad/{DB_NAME.lower()}_{DB_SCHEMA.lower()}_rules_tables.json" ##cambiar ruta y apuntar al nuevo archivo


logger.info(f"Period: {PERIOD}")
logger.info(f"Period: {DB_CATALOG_GLUE}")
logger.info(f"Schema: {DB_SCHEMA}")
logger.info(f"Connect: {CONNECT_NAME}")
logger.info(f"Bucket_name: {BUCKET_NAME}")
logger.info(f"Json_file: s3://{BUCKET_NAME_ASSETS}/{PATH_JSON}")

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session#
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")



def glue_table_exists_safe(database, table_name):
    try:
        spark.table(f"{database}.{table_name}")
        return True
    except Exception:
        return False

###################################################################
################# consultar catalogo  #############################
###################################################################

##tipo activo
df_activo = glueContext.create_dynamic_frame.from_catalog(
                        database="alfa-datalake-catalogo-bronce",  
                        table_name="tbl_activo"
                    ).toDF()
##tipo dato
df_tipodat = glueContext.create_dynamic_frame.from_catalog(
                        database="alfa-datalake-catalogo-bronce",  
                        table_name="tbl_tipo_dato"
                    ).toDF()                    
#atributo
df_atributo = glueContext.create_dynamic_frame.from_catalog(
                        database="alfa-datalake-catalogo-bronce",  
                        table_name="tbl_atributo"
                    ).toDF()
#dominio
df_dominio = glueContext.create_dynamic_frame.from_catalog(
                        database="alfa-datalake-catalogo-bronce",  
                        table_name="tbl_dominio"
                    ).toDF()  
#tbl_regla
df_regla = glueContext.create_dynamic_frame.from_catalog(
                        database="alfa-datalake-catalogo-bronce",  
                        table_name="tbl_regla"
                    ).toDF()     
#tbl_umbral
df_umbral = glueContext.create_dynamic_frame.from_catalog(
                        database="alfa-datalake-catalogo-bronce",  
                        table_name="tbl_umbral"
                    ).toDF()                       
#tbl_capa
df_capa = glueContext.create_dynamic_frame.from_catalog(
                        database="alfa-datalake-catalogo-bronce",  
                        table_name="tbl_capa"
                    ).toDF()   

##dim calendario
df_calendario = glueContext.create_dynamic_frame.from_catalog(
                        database="alfa-datalake-dev-gold",  
                        table_name="gold_caract_clientes_dim_dim_calendario_gold"
                    ).toDF()                    

try:      
                        ###################################################################
                        ########################## PROCESO DIM_CALENDARIO   ###############
                        ###################################################################
                        
    
    df_tbl_calendario = (
        df_calendario
            .withColumn(
                "fecha_formada",
                F.to_date(
                    F.concat_ws(
                        "-",
                        F.col("anio_t").cast("string"),
                        F.lpad(F.col("mes_t").cast("string"), 2, "0"),
                        F.lpad(F.col("dia_mes_t").cast("string"), 2, "0")
                    ),
                    "yyyy-MM-dd"
                )
            )
            .select(
                F.col("fecha_formada"),
                F.col("anio_t").alias("año"),
                F.col("mes_t").alias("mes"),
                F.col("dia_mes_t").alias("dia")
            )
    )

    
    df_tbl_calendario.show(20, truncate=False)
    logger.info("Mostradas primeras 20 filas de df_tbl_calendario")

    
    dim_exists = glue_table_exists_safe(  
        "alfa-datalake-calidad-gold",
        "dim_calendario"
    )
    
    if not dim_exists:
        df_merge = df_tbl_calendario.select(
            col("fecha_formada").alias("fecha"),
            "año",
            "mes",
            "dia"
        )
    
    else:
        df_dim_calendario = spark.table(
        "alfa-datalake-calidad-gold.dim_calendario"
        )
    
        df_merge = (
            df_dim_calendario.alias("dimcal")
            .join(
                df_tbl_calendario.alias("tblcal"),
                col("dimcal.fecha") == col("tblcal.fecha_formada"),
                "full_outer"
            )
            .select(
                F.coalesce(col("tblcal.fecha_formada"), col("dimcal.fecha")).alias("fecha"),
                F.coalesce(col("tblcal.año"), col("dimcal.año")).alias("año"),
                F.coalesce(col("tblcal.mes"), col("dimcal.mes")).alias("mes"),
                F.coalesce(col("tblcal.dia"), col("dimcal.dia")).alias("dia")
            )
        )
        
        
    window_fecha = Window.orderBy("fecha")
    window_periodo = Window.orderBy("periodo")
    window_id = Window.orderBy("fecha")


    df_final = (
        df_merge
            .withColumn("fecha_corta", F.to_date("fecha"))
            .withColumn("periodo", F.date_format("fecha", "yyyyMM"))
            .withColumn("id", F.row_number().over(window_fecha))
            .withColumn("id_periodo", F.dense_rank().over(window_periodo))
            .withColumn("load_dt", F.current_date())
            .select(
                "id",
                "id_periodo",
                "fecha",
                "fecha_corta",
                "periodo",
                "año",
                "mes",
                "dia",
                "load_dt"
            )
    )


    df_final.printSchema()
    df_final.show(20, truncate=False)
    
    
    output_path = "s3://alfa-datalake-dev-gold/GOLD/CALIDAD_CARGA_GOLD/dim_calendario/"
    
    df_final.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    logger.info(f"DIM_CALENDARIO cargada correctamente en {output_path}")


     
except Exception as e:
    logger.error(f"ERROR WITH Evaluate error ATRIBUTO: {traceback.format_exc()}")  
    
