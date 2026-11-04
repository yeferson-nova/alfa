"""Script that load gold phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim, date_format, lit, max
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
        'DB_CATALOG_GLUE_FILE',
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
DB_CATALOG_GLUE_FILE = args['DB_CATALOG_GLUE_FILE']
TABLE_NAME_GOLD = args['TABLE_NAME_GOLD']

logger.info(f"Period: {PERIOD}")

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

def load_json_from_s3(bucket: str, key: str) -> dict:
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    return json.loads(content)

def save_files(df, s3_prefix):
    """Function to use to save a DF"""
    try:
        frame_save = DynamicFrame.fromDF(df, glueContext, "frame_save_dynamic")
        s3_path = f"s3://{BUCKET_NAME}/{s3_prefix}"
        logger.info(f"Truncate & Saving data to: {s3_path}")
        extract_helper.delete_s3_folder(BUCKET_NAME, s3_prefix)
        extract_helper.save_files_s3(frame_save, s3_path, partition_columns)
        print(f"Success to save in {s3_path}")
    except Exception as e:
        print(f"Failed to save in {s3_path} beacusa : {e}")
        
try:
        
    
    # Define the schema for the  Final DataFrame
    data = load_json_from_s3("alfa-datalake-dev-assets","support_phase_files/niif/params_prm_json.json")
    #Read tables from silver schema general
    parametros_actuariales_ipc = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_FILE}", table_name="files_ipctasatecnica").toDF()
    parametros_actuariales_ipc_actuales = None
    try:
        parametros_actuariales_ipc_actuales = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="niif_ipctasatecnica_prm_gold").toDF()
        if parametros_actuariales_ipc_actuales.count() < 1:
            parametros_actuariales_ipc_actuales = None
    except Exception as e:
        parametros_actuariales_ipc_actuales = None
    
    ###################################################################
    ############ Init Businees Logic CAMPOS_ANEXO1 ####################
    ###################################################################
    
    logger.info(f"Writing to {TABLE_NAME_GOLD} with period = {PERIOD}")
    logger.info(f"Make logic to CAMPOS_ANEXO1 ")

    #-----------------------------------------------------------------#
    #---------------------- Join Tables ------------------------------#
    #-----------------------------------------------------------------#
    
    #---- inicio joins, query principal ----#
    ###################
    ### Apply logic ###
    ###################
    
    ## get max PERIOD ###
    max_period_new = parametros_actuariales_ipc.agg(F.max("PERIODO")).collect()[0][0]
    if parametros_actuariales_ipc_actuales != None:
        max_period_current = parametros_actuariales_ipc_actuales.agg(F.max("PERIODO")).collect()[0][0]
    #####################
    
    ####### Filter Dfs ######

    parametros_actuariales_ipc_filter = parametros_actuariales_ipc.filter(F.col("PERIODO") == max_period_new)
    if parametros_actuariales_ipc_actuales != None:
        window_spec = Window.partitionBy("T")
        df_param_act_ipc_actuales_max_per = parametros_actuariales_ipc_actuales.withColumn("PERIODO_MAX", F.max("PERIODO").over(window_spec))
        parametros_actuariales_ipc_actuales_filter = df_param_act_ipc_actuales_max_per.filter(F.col("periodo") == F.col("periodo_max")).drop("periodo_max")
        parametros_actuariales_ipc_actuales_filter_diff = df_param_act_ipc_actuales_max_per.filter(F.col("periodo") != F.col("periodo_max")).drop("periodo_max")
        
    
    #########################
    
    if parametros_actuariales_ipc_actuales != None:
        parametros_actuariales_ipc_actuales.cache()
    
    print(data)
    data = data['PRM']
    for funtionality_data in data:
        functionality = funtionality_data.get("funtionality", {})
        print(functionality)
        if functionality == "One":
            execution_date = F.current_date()
            hom_fields = funtionality_data.get("validate_fields", {})
            for field, hom in hom_fields.items():
                parametros_actuariales_ipc.withColumnRenamed(
                    field, hom
                )
            parametros_actuariales_ipc = parametros_actuariales_ipc.withColumn(
                    "PERIODO_DATE",
                    F.to_date(F.concat_ws("-", F.col("PERIODO").substr(1,4), F.col("PERIODO").substr(5,2), F.lit("01")), "yyyy-MM-dd")
                )
            active_inactive = funtionality_data.get("active-inactive", {})
            for active, inactive in active_inactive.items():
                active_value = active
                inactive_value = inactive
            if parametros_actuariales_ipc_actuales == None:
                parametros_actuariales_ipc = parametros_actuariales_ipc.withColumn(
                        "PER_FEC_INI_VIG" , F.trunc(F.col("PERIODO_DATE"), "month")
                    ).withColumn(
                        "PER_FEC_FIN_VIG", F.last_day(F.col("PERIODO_DATE"))    
                    ).withColumn(
                        "FEC_INI_VIG_REG", F.current_date()
                    )
                    
                w = Window.partitionBy("T").orderBy(F.desc("PERIODO"))
                df_result = parametros_actuariales_ipc.withColumn(
                    "max_periodo", F.max("PERIODO").over(Window.partitionBy("T"))
                    ).withColumn(
                        "FEC_FIN_VIG_REG",
                        F.when(F.col("PERIODO") == F.col("max_periodo"), F.lit(None).cast("date")).otherwise(execution_date)
                    ).withColumn(
                        "ACTIVO", 
                        F.when(F.col("PERIODO") == F.col("max_periodo"), F.lit(active)).otherwise(F.lit(inactive))
                    ).withColumn(
                        "FEC_INI_VIG_REG",
                        F.when(F.col("PERIODO") == F.col("max_periodo"), F.date_add(F.col("FEC_INI_VIG_REG"), 1)).otherwise(F.col("FEC_INI_VIG_REG"))
                    ).drop("max_periodo","parition_0","parition_1","parition_2")
                
                df_result = df_result.select("PERIODO", "PER_FEC_INI_VIG", "PER_FEC_FIN_VIG", "T", "IPC", "TASA_TECNICA", "FEC_INI_VIG_REG", "FEC_FIN_VIG_REG", "ACTIVO")
            
            ###################################################################
            ##################### Save files Data #############################
            ###################################################################
                partition_col = funtionality_data.get("partition", {})
                partition_columns = [partition_col]
                table_name = funtionality_data.get("TB_NAME", {})
                s3_prefix = f"GOLD/NIIF/{table_name}_TEST/"
                df_result = df_result.distinct()
                print(df_result.show(1))
            # Save the transformed DataFrame to S3
                save_files(df_result, s3_prefix)
            else:
                ## join entre los dfs ##
                df_join = parametros_actuariales_ipc_actuales_filter.alias("ant").join(
                    parametros_actuariales_ipc_filter.alias("new"),
                    on = "T",
                    how = "outer"
                )
                ## Condiciones ##
                
                cond_new = F.col("ant.T").isNull()
                cond_change = F.col("ant.T").isNotNull() & F.col("new.T").isNotNull()
                cond_with_out_changes = F.col("new.T").isNull()
                
                ## New register ##
                df_news = df_join.filter(cond_new & (F.col("new.INACTIVAR") == "0")).select(
                        F.coalesce(F.col("new.periodo"), F.lit(None)).alias("PERIODO"),
                        F.col("new.T").alias("T"),
                        F.lit(1).alias("ACTIVO"),
                        F.col("new.IPC"),
                        F.col("new.TASA_TECNICA"),
                        (execution_date + F.expr("INTERVAL 1 DAY")).alias("FEC_INI_VIG_REG"),
                        F.lit(None).cast("date").alias("FEC_FIN_VIG_REG"),
                        F.trunc(F.to_date(F.concat_ws("-", F.col("new.periodo").substr(1,4), F.col("new.periodo").substr(5,2), F.lit("01")), "yyyy-MM-dd"), "month").alias("PER_FEC_INI_VIG"),
                        F.last_day(F.to_date(F.concat_ws("-", F.col("new.periodo").substr(1,4), F.col("new.periodo").substr(5,2), F.lit("01")), "yyyy-MM-dd")).alias("PER_FEC_FIN_VIG"),
                        F.lit("uno").alias("source")
                    )
                ## update register ##
                
                df_ant_update = df_join.filter(cond_change & (F.col("new.INACTIVAR") == "1")).select(
                        F.col("ant.periodo").alias("PERIODO"),
                        F.col("ant.T").alias("T"),
                        F.lit(0).alias("ACTIVO"),
                        F.col("ant.IPC"),
                        F.col("ant.TASA_TECNICA"),
                        F.col("ant.FEC_INI_VIG_REG"),
                        F.lit(execution_date).alias("FEC_FIN_VIG_REG"),
                        F.col("ant.PER_FEC_INI_VIG"),
                        F.col("ant.PER_FEC_FIN_VIG"),
                        F.lit("dos").alias("source")
                    )
                    
                ## create new register for the same ID ##
                
                df_change_new = df_join.filter(cond_change & (F.col("new.INACTIVAR") == "0")).select(
                        F.coalesce(F.col("new.PERIODO"), F.lit(None)).alias("PERIODO"),
                        F.col("new.T").alias("T"),
                        F.lit(1).alias("ACTIVO"),
                        F.col("new.IPC"),
                        F.col("new.TASA_TECNICA"),
                        (execution_date + F.expr("INTERVAL 1 DAY")).alias("FEC_INI_VIG_REG"),
                        F.lit(None).cast("date").alias("FEC_FIN_VIG_REG"),
                        F.trunc(F.to_date(F.concat_ws("-", F.col("new.periodo").substr(1,4), F.col("new.periodo").substr(5,2), F.lit("01")), "yyyy-MM-dd"), "month").alias("PER_FEC_INI_VIG"),
                        F.last_day(F.to_date(F.concat_ws("-", F.col("new.periodo").substr(1,4), F.col("new.periodo").substr(5,2), F.lit("01")), "yyyy-MM-dd")).alias("PER_FEC_FIN_VIG"),
                        F.lit("tres").alias("source")
                    )
                    
                ## register without change ##
                
                df_with_out_change = df_join.filter(cond_with_out_changes).select(
                        F.col("ant.periodo").alias("PERIODO"),
                        F.col("ant.T").alias("T"),
                        F.col("ant.ACTIVO"),
                        F.col("ant.IPC"),
                        F.col("ant.TASA_TECNICA"),
                        F.col("ant.FEC_INI_VIG_REG"),
                        F.col("ant.FEC_FIN_VIG_REG"),
                        F.col("ant.PER_FEC_INI_VIG"),
                        F.col("ant.PER_FEC_FIN_VIG"),
                        F.lit("cuatro").alias("source")
                    )
                    
                ## union dfs ###
                
                print(df_news.printSchema())
                print(df_ant_update.printSchema())
                print(df_change_new.printSchema())
                print(df_with_out_change.printSchema())
                print(parametros_actuariales_ipc_actuales_filter_diff.printSchema())
                parametros_actuariales_ipc_actuales_filter_diff = parametros_actuariales_ipc_actuales_filter_diff.withColumn(
                    "source",
                    F.lit("anterior")
                    )
                df_result = df_news.unionByName(df_ant_update).unionByName(df_change_new).unionByName(df_with_out_change).unionByName(parametros_actuariales_ipc_actuales_filter_diff)
                
                print(df_result.count())
                print(df_result.filter(F.col("T") == "55").show())
                print(df_result.filter(F.col("T") == "119").show())
                
                partition_col = funtionality_data.get("partition", {})
                partition_columns = [partition_col]
                table_name = funtionality_data.get("TB_NAME", {})
                s3_prefix = f"GOLD/NIIF/{table_name}/"
                df_result = df_result.distinct()
            # Save the transformed DataFrame to S3
                print(df_result.show(1))
                #save_files(df_result, s3_prefix)
    
except Exception as e:
    print(traceback.format_exc())
    logger.error(f"An error occurred: {e}")
    