"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality
from commons.LogsHelper import LogsHelper
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, current_timestamp, current_date
from pyspark.sql.window import Window

import sys, traceback, boto3, json

logs = LogsHelper()
logger = logs.logger_conf()

args = getResolvedOptions(
    sys.argv, 
        [
        'JOB_NAME',
        'DB_NAME_CATALOG',        
        'DB_NAME_ALFA_DATALAKE', 
        'BUCKET_NAME_GOLD',
        'DB_NAME_CAL_GOLD'
        ]
    )
DB_NAME_CATALOG = args['DB_NAME_CATALOG']
DB_NAME_ALFA_DATALAKE = args['DB_NAME_ALFA_DATALAKE']
BUCKET_NAME_GOLD = args['BUCKET_NAME_GOLD']
DB_NAME_CAL_GOLD = args['DB_NAME_CAL_GOLD']

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
                        database=f"{DB_NAME_CATALOG}",  
                        table_name="tbl_activo"
                    ).toDF()                

try:      

    

                        ###################################################################
                        ########################## PROCESO DIM_ACTIVO    ##################
                        ###################################################################
                        
    
    
    df_tbl_activo=(
    df_activo.select(
        F.col("id_activo"),            		
        F.col("nombre_fisico"),        		
        F.col("fuente_origen"),        		
        F.col("retencion_meses"),    		
        F.col("id_estado"),            		
        F.col("id_dominio"),         		
        F.col("nombre_activo_funcional"),   
        F.col("id_tipo"),        
        F.col("tipo_tabla") #provisional para dev
    ))     #tbl 1 datos bronce  
    
    df_tbl_activo.show(20, truncate=False)
    logger.info("Mostradas primeras 20 filas de df_tbl_activo")

    
    dim_exists = glue_table_exists_safe(  
        f"{DB_NAME_CAL_GOLD}",
        "dim_activo"
    )
   
   
    if not dim_exists:
        logger.info("DIM_ACTIVO no existe → carga inicial")
    
        df_merge = (
            df_tbl_activo
                .select(
                    "id_activo",
                    "nombre_fisico",        	
                    "fuente_origen" ,       	
                    "retencion_meses",      	
                    "id_estado",            	
                    "id_dominio",         	
                    "nombre_activo_funcional",
                    "id_tipo",
                    "tipo_tabla", #provisional para dev
                    F.current_date().alias("load_dt")
                )
        )
    else:
        logger.info("DIM_ACTIVO existe → merge catálogo vs dimensión")
        df_dim_activo = glueContext.create_dynamic_frame.from_catalog(
            database=f"{DB_NAME_CAL_GOLD}",
            table_name="dim_activo"
        ).toDF()
    
        df_merge = (
        df_dim_activo.alias("dimact")
        .join(df_tbl_activo.alias("tblact"),
              col("dimact.id") == col("tblact.id_activo"),
              "full_outer")
        .select(
            coalesce(col("tblact.id_activo"),     col("dimact.id")).alias("id"),
            coalesce(col("tblact.nombre_fisico"), col("dimact.nombre_fisico")).alias("nombre_fisico"),
            coalesce(col("tblact.fuente_origen"),   col("dimact.fuente_origen")).alias("fuente_origen"),
            coalesce(col("tblact.retencion_meses"), col("dimact.retencion_meses")).alias("retencion_meses"),
            coalesce(col("tblact.id_estado"),     col("dimact.id_estado")).alias("id_estado"),
            coalesce(col("tblact.id_dominio"), col("dimact.id_dominio")).alias("id_dominio"),                    #id dominio ponerla en catalogo de categoria a dominio
            coalesce(col("tblact.nombre_activo_funcional"), col("dimact.nombre_fisica")).alias("nombre_activo_funcional"),      #cambiar a nombre_activo_funcional en el catalogo y en la dim nom act fun
            coalesce(col("tblact.id_tipo"), col("dimact.tipo de tabla")).alias("tipo_de_tabla"),                   #tipo de tabla se debe poner en cat bronce y la dimension
            coalesce(col("tblact.tipo_de_tabla"), col("dimact.tipo de tabla")).alias("tipo_de_tabla"),                   #
            
            F.to_date(F.current_timestamp()).alias("load_dt")
        )
    )
    
    df_merge.printSchema()
    df_merge.show(20, truncate=False)
    
    output_path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CALIDAD_CARGA_GOLD/dim_activo/"
        
    df_merge.write \
            .mode("overwrite") \
            .parquet(output_path)
        
    logger.info(f"DIM_DOMINIO cargada correctamente en {output_path}")
    
    
    
    
except Exception as e:
    logger.error(f"ERROR WITH Evaluate error ATRIBUTO: {traceback.format_exc()}")  
    
