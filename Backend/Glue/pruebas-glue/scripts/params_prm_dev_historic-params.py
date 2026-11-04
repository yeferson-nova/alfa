"""Script that load gold phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import col, trim, date_format, lit, max
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from datetime import datetime
import sys, traceback, boto3, json, ast

from commons3.ExtractHelper import ExtractHelper
from commons3.LogsHelper import LogsHelper
from commons3.ParameterBuilder import ParameterBuilder

###################################################################
######## SET ARGS AND CONFIG COMMONS UTILS ########################
###################################################################

logs = LogsHelper()
logger = logs.logger_conf()


args = getResolvedOptions(
    sys.argv, 
        [
        'JOB_NAME',
        'TABLES_PARAMS',
        'FUNCTION_TO_EVALUATE',
        'VAL_PERIOD', 
        'DB_SCHEMA',
        'DB_NAME',
        'PERIOD',
        'CERTIFICATE'
        ]
)

FUNCTION_TO_EVALUATE = json.loads(args['FUNCTION_TO_EVALUATE'])
tables_params = json.loads(args['TABLES_PARAMS'])

VAL_PERIOD = args['VAL_PERIOD']
DB_NAME = args['DB_NAME']
DB_SCHEMA = args['DB_SCHEMA']
PERIOD = args['PERIOD']
CERTIFICATE = args['CERTIFICATE']


#Read params from aux lib
builder = ParameterBuilder(table_names=tables_params )
params = builder.process_and_build_all(max_workers=10)
json_params = json.loads(params)

# Call function to process and group data
unified_directories = builder.group_and_unify_structures(json_params, 'directory')
unified_historic = builder.group_and_unify_structures(json_params, 'historic')

expected_historic = {key: unified_historic[key] for key in FUNCTION_TO_EVALUATE}

DB_CATALOG_GLUE_BRONCE = unified_directories['bronce_bdd']
DB_CATALOG_GLUE_PRM = unified_directories['prm_bdd']
BUCKET_PRM = unified_directories['prm']

if VAL_PERIOD == '1':
    VAL_PERIOD = True
if VAL_PERIOD == '0':
    VAL_PERIOD = False

logger.info(f"Catalog_bronce: {DB_CATALOG_GLUE_BRONCE}")
logger.info(f"Catalog_prm: {DB_CATALOG_GLUE_PRM}")
logger.info(f"Bucket_prm: {BUCKET_PRM}")
logger.info(f"Tables for func {FUNCTION_TO_EVALUATE}: tables and info : {expected_historic}")


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

extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_PRM, DB_NAME)

def obtain_last_date(fecha = None):
    """
    This Function help us to obtein the last cut date.
    """
    if fecha is None:
        fecha = datetime.now()
    if PERIOD == '0':    
        primer_dia_mes_actual = datetime(fecha.year, fecha.month, 1)
    if PERIOD != '0':
        year = int(PERIOD[0:4])
        month = int(PERIOD[4:6])
        primer_dia_mes_actual = datetime(year, month, 1)
    return primer_dia_mes_actual
    
    
def save_files(df, s3_prefix):
    """Function to use to save a DF"""
    try:
        frame_save = DynamicFrame.fromDF(df, glueContext, "frame_save_dynamic")
        s3_path = f"s3://{BUCKET_PRM}/{s3_prefix}"
        logger.info(f"Truncate & Saving data to: {s3_path}")
        extract_helper.delete_s3_folder(BUCKET_PRM, s3_prefix)
        extract_helper.save_files_s3(frame_save, s3_path, partition_columns)
        logger.info(f"Success to save in {s3_path}")
    except Exception as e:
        logger.error(f"Failed to save in {s3_path} because : {e}")    


try:
    #-----------------------------------------------------------------#
    #--------- Load Necesary Tables (Bronce & gold) ------------------#
    #-----------------------------------------------------------------#
    for function, tables in expected_historic.items():
        logger.info(f"Start with logic on {function}")
        if isinstance(tables, dict):
            tables = [tables]        
        for table in tables:
            execution_date = F.current_date()
            period_date = obtain_last_date()
            period_date = period_date.strftime('%Y-%m-%d')
            period_date_val = datetime.strptime(period_date, '%Y-%m-%d').strftime('%Y%m')        
            table_source = table['tabla_source']
            activo_inactivo = ast.literal_eval(table['activo'])
            inactivo = activo_inactivo['inactivo']
            activo = activo_inactivo['activo']
            periodo = json.loads(table['periodo']) if table['periodo'] else 'N/A'     
            pivote = json.loads(table['pivote']) if table['pivote'] else 'N/A'   
            inactivar = json.loads(table['inactivar']) if table['inactivar'] else 'N/A'  
            cols_info = json.loads(table['info']) if table['info'] else 'N/A' 
            logger.info(f"Start with load {table_source} from bronze")
            df_result = None
            df_bronce = None
            df_prm = None
            
            try:
                df_bronce = glueContext.create_dynamic_frame.from_catalog(database=DB_CATALOG_GLUE_BRONCE, table_name=f"files_{table_source}").toDF()
                logger.info(f"the table {table_source} is loaded from bronze")
            except Exception as e:
                df_bronce = None
                logger.error(f"the table {table_source} bronze dont have data: {e}")
    
            logger.info(f"Start with load {table_source} from prm")
            if df_bronce != None:            
                try:
                    df_prm = glueContext.create_dynamic_frame.from_catalog(database=DB_CATALOG_GLUE_PRM, table_name=f"niif_prm_{table_source}").toDF()
                    if df_prm.count() < 1:
                        df_prm = None
                    logger.info(f"the table {table_source} is loaded from prm")
                except Exception as e:
                    df_prm = None
                    logger.error(f"the table {table_source} prm dont have data, dont loaded: {e}")
                                    
                # The correct logic assumes you compare distinct variables:
                
                #-----------------------------------------------------------------#
                #------------------- Logic function 1 ----------------------------#
                #-----------------------------------------------------------------#
                
                if function == 'Funcionalidad_1':
                    # Logic for Functionality 1
                    logger.info(f"Executing logic for {function}") 
                    
                    max_period_new = df_bronce.agg(F.max(periodo[0])).collect()[0][0]
                    df_bronce_filter = df_bronce.filter(F.col(periodo[0]) == max_period_new)
                    
                    if df_prm != None:
                        window_spec = Window.partitionBy(pivote[0])
                        df_prm_max_per = df_prm.withColumn(f"{periodo[0]}_MAX", F.max(periodo[0]).over(window_spec))
                        df_prm_max_per_filter = df_prm_max_per.filter(F.col(periodo[0]) == F.col(f"{periodo[0]}_MAX")).drop(f"{periodo[0]}_MAX")
                        df_prm_max_per_filter_diff = df_prm_max_per.filter(F.col(periodo[0]) != F.col(f"{periodo[0]}_MAX")).drop(f"{periodo[0]}_MAX")
                    if df_prm != None:
                        df_prm.cache()
                        
                    df_bronce = df_bronce.withColumn(
                        f"{periodo[0]}_DATE",
                        F.to_date(F.concat_ws("-", F.col(periodo[0]).substr(1,4), F.col(periodo[0]).substr(5,2), F.lit("01")), "yyyy-MM-dd")
                    )
                                    
                    if df_prm == None:                    
                        logger.info(f"This is the first execution of {function}")
                        df_bronce = df_bronce.withColumn(
                                "PER_FEC_INI_VIG" , F.trunc(F.col(f"{periodo[0]}_DATE"), "month")
                            ).withColumn(
                                "PER_FEC_FIN_VIG", F.last_day(F.col(f"{periodo[0]}_DATE"))    
                            ).withColumn(
                                "FEC_INI_VIG_REG", F.current_date()
                            )
    
                        w = Window.partitionBy(pivote[0]).orderBy(F.desc(periodo[0]))
                        
                        df_result = df_bronce.withColumn(
                            f"max_{periodo[0]}", F.max(periodo[0]).over(Window.partitionBy(pivote[0]))
                            ).withColumn(
                                "FEC_FIN_VIG_REG",
                                F.when(F.col(periodo[0]) == F.col(f"max_{periodo[0]}"), F.lit(None).cast("date")).otherwise(execution_date)
                            ).withColumn(
                                "ACTIVO", 
                                F.when(F.col(periodo[0]) == F.col(f"max_{periodo[0]}"), F.lit(activo)).otherwise(F.lit(inactivo))
                            ).withColumn(
                                "FEC_INI_VIG_REG",
                                F.when(F.col(periodo[0]) == F.col(f"max_{periodo[0]}"), F.date_add(F.col("FEC_INI_VIG_REG"), 1)).otherwise(F.col("FEC_INI_VIG_REG"))
                            ).drop(f"max_{periodo[0]}","parition_0","parition_1","parition_2")
    
                        df_result = df_result.select(periodo[0], "PER_FEC_INI_VIG", "PER_FEC_FIN_VIG", pivote[0], "IPC", "TASA_TECNICA", "FEC_INI_VIG_REG", "FEC_FIN_VIG_REG", "ACTIVO")   
                        df_result = df_result.distinct()
                        
                        s3_prefix = f"NIIF/PRM_{table_source.upper()}/"
                        partition_columns = [periodo[0]]
                        try:
                            save_files(df_result, s3_prefix)
                        except Exception as e:                        
                            logger.error(f"Failed to save table to {s3_prefix}. Error: {e}")
                            
                        logger.info(f"End First Execution Functionality {function} table {table_source}")
                    
                    if df_prm != None:   
                        logger.info(f"This is the incremental execution of {function}")
                        period_val = df_prm_max_per_filter.select(periodo[0]).distinct().collect()[0][0]
                        if (period_date_val == period_val) or VAL_PERIOD == True:
                            
                            df_join = df_prm_max_per_filter.alias("ant").join(
                                df_bronce_filter.alias("new"),
                                on = pivote[0],
                                how = "outer"
                            )
                            
                            cond_new = F.col(f"ant.{pivote[0]}").isNull()
                            cond_change = F.col(f"ant.{pivote[0]}").isNotNull() & F.col(f"new.{pivote[0]}").isNotNull()
                            cond_with_out_changes = F.col(f"new.{pivote[0]}").isNull()  
                            
                            expresiones_fijas_news = [                            
                                F.coalesce(F.col(f"new.{periodo[0]}"), F.lit(None)).alias(periodo[0]),
                                F.col(f"new.{pivote[0]}").alias(pivote[0]),
                                F.lit(activo).alias("ACTIVO"),
                                (execution_date + F.expr("INTERVAL 1 DAY")).alias("FEC_INI_VIG_REG"),
                                F.lit(None).cast("date").alias("FEC_FIN_VIG_REG"),
                                F.trunc(F.to_date(F.concat_ws("-", F.col(f"new.{periodo[0]}").substr(1,4), F.col(f"new.{periodo[0]}").substr(5,2), F.lit("01")), "yyyy-MM-dd"), "month").alias("PER_FEC_INI_VIG"),
                                F.last_day(F.to_date(F.concat_ws("-", F.col(f"new.{periodo[0]}").substr(1,4), F.col(f"new.{periodo[0]}").substr(5,2), F.lit("01")), "yyyy-MM-dd")).alias("PER_FEC_FIN_VIG")                        
                            ]
                            expresiones_dinamicas_news = [F.col(f"new.{col_name}") for col_name in cols_info]
                            select_df_news = expresiones_fijas_news + expresiones_dinamicas_news
                            df_news = df_join.filter(cond_new & (F.col(f"new.{inactivar[0]}") == inactivo)).select(*select_df_news)
    
                            
                            expresiones_fijas_ant_update = [
                                F.col(f"ant.{periodo[0]}").alias(periodo[0]),
                                F.col(f"ant.{pivote[0]}").alias(pivote[0]),
                                F.lit(inactivo).alias("ACTIVO"),
                                F.col("ant.FEC_INI_VIG_REG"),
                                F.lit(execution_date).alias("FEC_FIN_VIG_REG"),
                                F.col("ant.PER_FEC_INI_VIG"),
                                F.col("ant.PER_FEC_FIN_VIG")
                            ]
                            expresiones_dinamicas_ant_update = [F.col(f"ant.{col_name}") for col_name in cols_info]
                            select_df_ant_update = expresiones_fijas_ant_update + expresiones_dinamicas_ant_update                        
                            df_ant_update = df_join.filter(cond_change & (F.col(f"new.{inactivar[0]}") == activo)).select(*select_df_ant_update)                        
                            
                            
                            expresiones_fijas_change_new = [                            
                                F.col(f"new.{periodo[0]}").alias(periodo[0]),
                                F.col(pivote[0]).alias(pivote[0]),                                                        
                                F.lit(activo).alias("ACTIVO"),
                                (execution_date + F.expr("INTERVAL 1 DAY")).alias("FEC_INI_VIG_REG"), 
                                F.lit(None).cast("date").alias("FEC_FIN_VIG_REG"),     
                                F.trunc(F.to_date(F.concat_ws("-", F.col(f"new.{periodo[0]}").substr(1,4), F.col(f"new.{periodo[0]}").substr(5,2), F.lit("01")), "yyyy-MM-dd"), "month").alias("PER_FEC_INI_VIG"),
                                F.last_day(F.to_date(F.concat_ws("-", F.col(f"new.{periodo[0]}").substr(1,4), F.col(f"new.{periodo[0]}").substr(5,2), F.lit("01")), "yyyy-MM-dd")).alias("PER_FEC_FIN_VIG")
                            ]
    
                            select_df_change_new = expresiones_fijas_change_new + expresiones_dinamicas_news
                            
                            df_change_new = df_join.filter(cond_change & (F.col(f"new.{inactivar[0]}") == inactivo)).select(*select_df_change_new)
    
                                                    
                            expresiones_fijas_with_out_change = [                
                                F.col(f"ant.{periodo[0]}").alias(periodo[0]),
                                F.col(f"ant.{pivote[0]}").alias(pivote[0]),
                                F.col("ant.ACTIVO").alias("ACTIVO"),
                                F.col("ant.FEC_INI_VIG_REG").alias("FEC_INI_VIG_REG"), 
                                F.col("ant.FEC_FIN_VIG_REG").alias("FEC_FIN_VIG_REG"),                             
                                F.col("ant.PER_FEC_INI_VIG").alias("PER_FEC_INI_VIG"),
                                F.col("ant.PER_FEC_FIN_VIG").alias("PER_FEC_FIN_VIG"),
                            ]
    
                            select_df_with_out_change = expresiones_fijas_with_out_change + expresiones_dinamicas_ant_update
    
                            # Aplicación
                            df_with_out_change = df_join.filter(cond_with_out_changes).select(*select_df_with_out_change)      
                            
                            df_result = df_news.unionByName(df_ant_update).unionByName(df_change_new).unionByName(df_with_out_change).unionByName(df_prm_max_per_filter_diff)
                            df_result = df_result.distinct()
                            
                            s3_prefix = f"NIIF/PRM_{table_source.upper()}/"
                            partition_columns = [periodo[0]]                        
                            try:
                                save_files(df_result, s3_prefix)
                            except Exception as e:                        
                                logger.error(f"Failed to save table to {s3_prefix}. Error: {e}")
    
                            logger.info(f"End Incremental Execution Functionality {function} table {table_source}")                        
                            
                #-----------------------------------------------------------------#
                #------------------- Logic function 2 ----------------------------#
                #-----------------------------------------------------------------#                            
                        
                elif function == 'Funcionalidad_2':
                    # Logic for Functionality 2
                    logger.info(f"Executing logic {function}") 
                    
                    if df_bronce != None:    
                        max_df_bronce_new = df_bronce.agg(F.max(periodo[0])).collect()[0][0]
                        df_bronce = df_bronce.filter(F.col(periodo[0]) == max_df_bronce_new)
                    
                    if df_prm == None:
                        df_bronce = df_bronce.withColumnRenamed(f"{periodo[0]}#0",periodo[0])
                        df_result = df_bronce.withColumn(
                            "FECHA_FIN",
                            F.lit(None).cast(StringType())
                        ).withColumn(
                            "ACTIVO",
                            F.lit(activo)
                        ).withColumn(
                            "FEC_CARGUE",
                            F.lit(execution_date)
                        )          
                        df_result = df_result.distinct()                                        
                        
                        s3_prefix = f"NIIF/PRM_{table_source.upper()}/"
                        partition_columns = [periodo[0]]                    
                        try:
                            save_files(df_result, s3_prefix)
                        except Exception as e:                        
                            logger.error(f"Failed to save table to {s3_prefix}. Error: {e}")
    
                        logger.info(f"End first Execution Functionality {function} table {table_source}")     
                        
                    if df_prm !=None:
                        
                        period_val = df_prm.select(periodo[0]).distinct().collect()[0][0]
                        period_val = datetime.strptime(period_val, '%Y-%m-%d').strftime('%Y%m')
                        if (period_date_val == period_val) or VAL_PERIOD == True:
                            df_prm = df_prm.withColumn(
                                    "ACTIVO",
                                    F.lit(inactivo)
                                ).withColumn(
                                    "FECHA_FIN",
                                    F.date_sub(F.current_date(), 1)
                                )
                            
                            df_result = df_bronce.withColumn(
                                    "FECHA_FIN",
                                    F.lit(None)
                                ).withColumn(
                                    "ACTIVO",
                                    F.lit(activo)
                                ).withColumn(
                                    "FEC_CARGUE",
                                    F.lit(execution_date)
                                )
    
                            df_result = df_result.unionByName(df_prm)
                            df_result = df_result.distinct()
    
                            s3_prefix = f"NIIF/PRM_{table_source.upper()}/"
                            partition_columns = [periodo[0]]                    
                            try:
                                save_files(df_result, s3_prefix)
                            except Exception as e:                        
                                logger.error(f"Failed to save table to {s3_prefix}. Error: {e}")
    
                            logger.info(f"End Incremental Execution Functionality {function} table {table_source}")                                                                                                           
                            
                #-----------------------------------------------------------------#
                #------------------- Logic function 3 ----------------------------#
                #-----------------------------------------------------------------#    
                
                elif function == 'Funcionalidad_3':
                    # Logic for Functionality 3
                    logger.info(f"Executing logic {function}")
                    
                    if df_bronce != None and df_bronce.count() > 1:
                        max_df_bronce_fec_ini = df_bronce.agg(F.max(periodo[0])).collect()[0][0]
                        df_bronce = df_bronce.filter(F.col(periodo[0]) == max_df_bronce_fec_ini)
                    
                    if df_prm != None:
                        max_df_prm_fec_ini = df_prm.agg(F.max("FEC_INI_VIG")).collect()[0][0]
                        if df_prm.select("FEC_INI_VIG").distinct().count() > 1:
                            df_prm_anteriores = df_prm.filter(~(F.col("FEC_INI_VIG") == max_df_prm_fec_ini))
                            df_prm = df_prm.filter(F.col("FEC_INI_VIG") == max_df_prm_fec_ini)
                        else:
                            df_prm = df_prm.filter((F.col("FEC_INI_VIG") == max_df_prm_fec_ini))
                            df_prm_anteriores = df_prm.filter((F.col("ACTIVO") == "3"))
                            
                    if df_prm == None:
                        df_result = df_bronce.withColumn(
                                "ACTIVO",
                                F.lit(activo)
                            ).withColumn(
                                "FEC_CARGUE",
                                F.lit(F.current_date())
                            ).withColumn(
                                "FEC_FIN_VIG",
                                F.lit(None).cast(StringType())
                            ).withColumn(
                                "FEC_INI_VIG",
                                F.col(periodo[0])
                            )
    
                        df_result = df_result.drop(inactivar[0],periodo[0])  
                        df_result = df_result.distinct()                    
                        
                        s3_prefix = f"NIIF/PRM_{table_source.upper()}/"
                        partition_columns = ["FEC_CARGUE"]                    
                        try:
                            save_files(df_result, s3_prefix)
                        except Exception as e:                        
                            logger.error(f"Failed to save table to {s3_prefix}. Error: {e}")
    
                        logger.info(f"End first Execution Functionality {function} table {table_source}")                       
                        
                    if df_prm != None:  
                        
                        df_bronce_inactivar = df_bronce.filter(F.col(inactivar[0]) == '1')
                        df_bronce = df_bronce.filter(F.col(inactivar[0]) == '0')    
                        
                        df_result_new = df_bronce.withColumn(
                                "ACTIVO",
                                F.lit(activo)
                            ).withColumn(
                                "FEC_CARGUE",
                                F.lit(F.current_date())
                            ).withColumn(
                                "FEC_FIN_VIG",
                                F.lit(None).cast(StringType())
                            ).withColumn(
                                "FEC_INI_VIG",
                                F.col(periodo[0])
                            )                    
                        
                        df_ant_des = df_result_new.alias("new").join(df_prm.alias("ant"),
                            F.col(f"ant.{pivote[0]}") == F.col(f"new.{pivote[0]}"), "inner"
                        )
                            
                        df_ant_des = df_ant_des.withColumn(
                            "ACTIVO_new",
                                F.lit(str(inactivo))
                            ).withColumn(
                            "FEC_FIN_VIG_new",
                                F.date_trunc("month", F.add_months(F.current_date(), 1))
                            )
                        df_ant_des = df_ant_des.select("ant.*","ACTIVO_new","FEC_FIN_VIG_new")
                        df_ant_des = df_ant_des.drop(F.col("ant.ACTIVO"),F.col("ant.FEC_FIN_VIG"))
                        df_ant_des = df_ant_des.withColumnRenamed("ACTIVO_new","ACTIVO").withColumnRenamed("FEC_FIN_VIG_new","FEC_FIN_VIG")              
                        
                        df_result_new = df_result_new.filter(F.col(inactivar[0]) == '0').drop(inactivar[0],periodo[0])
                        df_ant_sin_cambios = df_prm.join(df_result_new, on=pivote[0], how="left_anti")                    
                    
                        df_result_new = df_result_new.unionByName(df_ant_des).unionByName(df_prm_anteriores).unionByName(df_ant_sin_cambios)
                        df_result_new_inct = df_result_new.alias("New").join(df_bronce_inactivar.alias("Inc"), on=pivote[0], how = "inner").select("New.*")
                        df_result_new = df_result_new.alias("New").join(df_bronce_inactivar.alias("Inc"), on=pivote[0], how = "left_anti").select("New.*")       
                        
                        
                        df_result_new_inct = df_result_new_inct.withColumn(
                            "ACTIVO_new",
                                F.lit(str(inactivo))
                            ).withColumn(
                            "FEC_FIN_VIG_new",
                                F.date_trunc("month", F.add_months(F.current_date(), 1))
                            )
                        
                        df_result_new_inct = df_result_new_inct.select("New.*","ACTIVO_new","FEC_FIN_VIG_new")
                        df_result_new_inct = df_result_new_inct.drop(F.col("New.ACTIVO"),F.col("New.FEC_FIN_VIG"))
                        df_result_new_inct = df_result_new_inct.withColumnRenamed("ACTIVO_new","ACTIVO").withColumnRenamed("FEC_FIN_VIG_new","FEC_FIN_VIG")
    
                        df_result_new = df_result_new.unionByName(df_result_new_inct)
                        df_result_new.drop(F.col("NO_REGISTRO"))
                        df_result = df_result_new.distinct()
                        
                        
                        s3_prefix = f"NIIF/PRM_{table_source.upper()}/"
                        partition_columns = ["FEC_CARGUE"]                    
                        try:
                            save_files(df_result, s3_prefix)
                        except Exception as e:                        
                            logger.error(f"Failed to save table to {s3_prefix}. Error: {e}")
    
                        logger.info(f"End Incremental Execution Functionality {function} table {table_source}") 
                                                        
                #-----------------------------------------------------------------#
                #------------------- Logic function 4 ----------------------------#
                #-----------------------------------------------------------------#
                                                        
                elif function == 'Funcionalidad_4':
                    # Logic for Functionality 3
                    logger.info(f"Executing logic {function}")                
                    
                    if df_bronce != None and df_bronce.count() > 1:
                        max_df_bronce_fec_ini = df_bronce.agg(F.max("FECHA_CARGA")).collect()[0][0]
                        df_bronce = df_bronce.filter(F.col("FECHA_CARGA") == max_df_bronce_fec_ini)
                        
                    df_prm_anteriores = None                
                    if df_prm != None:
                        logger.info(periodo)
                        logger.info(df_prm)
                        max_df_prm_fec_ini = df_prm.agg(F.max("FEC_INI_VIG")).collect()[0][0]
                        if df_prm.select("FEC_INI_VIG").distinct().count() > 1:
                            df_prm_anteriores = df_prm.filter(~(F.col("FEC_INI_VIG") == max_df_prm_fec_ini))
                            df_prm = df_prm.filter(F.col("FEC_INI_VIG") == max_df_prm_fec_ini)
                        else:
                            df_prm = df_prm.filter((F.col("FEC_INI_VIG") == max_df_prm_fec_ini))
                            df_prm_anteriores = df_prm.filter((F.col("ACTIVO") == "3"))                                                                                        
                            
                    
                    if df_prm == None:
                        
                        df_result = df_bronce.withColumn(
                            "ACTIVO",
                            F.lit(activo)
                        ).withColumn(
                            "FEC_FIN_VIG",
                            F.lit(None).cast(StringType())
                        ).withColumn(
                            "FEC_INI_VIG",
                            F.current_date()
                        )
    
                        df_result = df_result.drop(inactivar[0])
    
                        df_result = df_result.distinct()
                        
                        s3_prefix = f"NIIF/PRM_{table_source.upper()}/"
                        partition_columns = ["FEC_INI_VIG"]               
                        
                        try:
                            save_files(df_result, s3_prefix)
                        except Exception as e:                        
                            logger.error(f"Failed to save table to {s3_prefix}. Error: {e}")
    
                        logger.info(f"End first Execution Functionality {function} table {table_source}")     
                                        
                    if df_prm != None and df_bronce != None:
                        
                        df_bronce_inactivar = df_bronce.filter(F.col(inactivar[0]) == '1')
                        df_bronce = df_bronce.filter(F.col(inactivar[0]) == '0')
    
                        df_result_new = df_bronce.withColumn(
                                "ACTIVO",
                                F.lit(activo)
                            ).withColumn(
                                "FEC_FIN_VIG",
                                F.lit(None).cast(StringType())
                            ).withColumn(
                                "FEC_INI_VIG",
                                F.current_date()
                            )
                        
                        df_ant_des = df_result_new.alias("new").join(df_prm.alias("ant"),
                                    F.col(f"ant.{pivote[0]}") == F.col(f"new.{pivote[0]}"), "inner"
                                )#Inactivar los que tengan datos
                        df_ant_des = df_ant_des.withColumn(
                            "ACTIVO_new",
                                F.lit(str(inactivo))
                            ).withColumn(
                            "FEC_FIN_VIG_new",
                                F.date_trunc("month", F.add_months(F.current_date(), 1))
                            )
                        df_ant_des = df_ant_des.select("ant.*","ACTIVO_new","FEC_FIN_VIG_new")
                        df_ant_des = df_ant_des.drop(F.col("ant.ACTIVO"),F.col("ant.FEC_FIN_VIG"))
                        df_ant_des = df_ant_des.withColumnRenamed("ACTIVO_new","ACTIVO").withColumnRenamed("FEC_FIN_VIG_new","FEC_FIN_VIG")
    
                        df_result_new = df_result_new.filter(F.col(inactivar[0]) == '0').drop(inactivar[0])
                        df_ant_sin_cambios = df_prm.join(df_result_new, on=pivote[0], how="left_anti")
                        df_result_new = df_result_new.unionByName(df_ant_des).unionByName(df_prm_anteriores).unionByName(df_ant_sin_cambios)
                        df_result_new_inct = df_result_new.alias("New").join(df_bronce_inactivar.alias("Inc"), on=pivote[0], how = "inner").select("New.*")
                        df_result_new = df_result_new.alias("New").join(df_bronce_inactivar.alias("Inc"), on=pivote[0], how = "left_anti").select("New.*")   
                        
                        df_result_new_inct = df_result_new_inct.withColumn(
                            "ACTIVO_new",
                                F.lit(str(inactivo))
                            ).withColumn(
                            "FEC_FIN_VIG_new",
                                F.date_trunc("month", F.add_months(F.current_date(), 1))
                            )
                        df_result_new_inct = df_result_new_inct.select("New.*","ACTIVO_new","FEC_FIN_VIG_new")
                        df_result_new_inct = df_result_new_inct.drop(F.col("New.ACTIVO"),F.col("New.FEC_FIN_VIG"))
                        df_result_new_inct = df_result_new_inct.withColumnRenamed("ACTIVO_new","ACTIVO").withColumnRenamed("FEC_FIN_VIG_new","FEC_FIN_VIG")                                        
    
                        df_result_new = df_result_new.unionByName(df_result_new_inct)
                        df_result_new.drop(F.col("NO_REGISTRO"))
                        df_result = df_result_new.distinct()                                        
                   
                        s3_prefix = f"NIIF/PRM_{table_source.upper()}/"
                        partition_columns = ["FEC_INI_VIG"]              
                        try:
                            save_files(df_result, s3_prefix)
                        except Exception as e:                        
                            logger.error(f"Failed to save table to {s3_prefix}. Error: {e}")
    
                        logger.info(f"End Incremental Execution Functionality {function} table {table_source}") 
    
                else:                
                    logger.info(f"Function '{function}' is not configured")

except Exception as e:
    logger.info(traceback.format_exc())
    logger.error(f"An error occurred: {e}")













