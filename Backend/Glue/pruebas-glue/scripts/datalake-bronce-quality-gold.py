"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, current_timestamp
import sys, traceback, boto3, json
from pyspark.sql.functions import lit
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logs = LogsHelper()
logger = logs.logger_conf()



args = getResolvedOptions(
    sys.argv, 
        [
        'DB_NAME_CAL_SIL',
        'DB_NAME_CAL_GOLD',
        'BUCKET_NAME_GOLD',
        ]
    )
DB_NAME_CAL_SIL = args['DB_NAME_CAL_SIL']
DB_NAME_CAL_GOLD = args['DB_NAME_CAL_GOLD']
BUCKET_NAME_GOLD = args['BUCKET_NAME_GOLD']


    ###################################################################
    ################# cruce con fct-siver y fct-gold ##################
    ###################################################################

try:
    ##dim alfa-datalake-dev-cal-silver
    fct_silver = glueContext.create_dynamic_frame.from_catalog(
                            database=f"{DB_NAME_CAL_SIL}",  
                            table_name="calidad"
                        ).toDF()
    fct_silver.show(20, truncate=False)
    logger.info("Mostradas primeras 20 filas de fct calidad silver")   
    
    ########################################################################
    ################# consultar dimensiones ################################
    ######################################################################## 
                  
    # consultar dimension calendario
    df_dim_calendario =  glueContext.create_dynamic_frame.from_catalog(
                            database=f"{DB_NAME_CAL_GOLD}",  
                            table_name="dim_calendario"
                        ).toDF()
    df_dim_calendario.show(20, truncate=False)
    logger.info("Mostradas primeras 20 filas de fct calendario silver")           
    
    ########################################################################
    ################# tratamiento dimensiones ##############################
    ######################################################################## 

    df_dim_cal_key = (
        df_dim_calendario
        .select(
            col("fecha_corta"),
            col("id").alias("id_fecha"),
            col("id_periodo")
        )
    )
    # relacion dimension calendario, silver 
    fct_gold_pre = (
        fct_silver
        .join(
            df_dim_cal_key,
            on="fecha_corta",        
            how="left"
        )
    )

    
    # Claves críticas no nulas
    fct_silver.filter(F.col("id_activo").isNull()).count()
    fct_silver.filter(F.col("id_atributo").isNull()).count()
    fct_silver.filter(F.col("id_umbral").isNull()).count()


    fct_calidad = (
        fct_gold_pre
        .select(
            col("id_fecha").alias("id"),
            "id_periodo",
            col("fecha_corta").alias("fecha"),
            "id_activo",
            "id_atributo",
            "id_dominio",
            "id_capa",
            lit(1).cast("int").alias("id_regla"), 
            "id_umbral",
            "valor_referencia",
            "valor_resultado"
        )
    )
    
    fct_calidad.filter(col("id").isNull()).count()
    fct_calidad.filter(col("id_periodo").isNull()).count()

    fct_calidad.printSchema()
    df_preview = fct_calidad
    log_str = df_preview._jdf.showString(20, 0, False)
    logger.info(f"Preview primeras 20 filas de fct_calidad:\n{log_str}")
    
    
    output_path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CALIDAD_CARGA_GOLD/fct_calidad/"
    (fct_calidad.write.mode("overwrite").parquet(output_path))
    logger.info(f"fct_calidad guardado correctamente en {output_path}")




except Exception as e:
    logger.error(f"ERROR WITH Evaluate error ATRIBUTO: {traceback.format_exc()}")  
                    

