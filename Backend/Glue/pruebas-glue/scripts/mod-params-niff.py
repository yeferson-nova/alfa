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
from datetime import datetime

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
        'TABLE_NAME_GOLD',
        'VAL_PERIOD',
        'PARAMS'
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
VAL_PERIOD = args['VAL_PERIOD']
PARAMS = args['PARAMS']

PARAMS_TO_EXCECUTE = PARAMS.split(',')

if VAL_PERIOD == '1':
    VAL_PERIOD = True
if VAL_PERIOD == '0':
    VAL_PERIOD = False
        
logger.info(f"Se cargaron las variables correctamente")

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
    
    #-----------------------------------------------------------------#
    #--------------------- Load Necesary Variables -------------------#
    #-----------------------------------------------------------------#
    data = load_json_from_s3(BUCKET_NAME_ASSETS,"support_phase_files/niif/params_prm_json.json")
    schema_source = data['schema_source']
    schema_target = data['schema_target']
    activo = data['activo']
    inactivo = data['inactivo']
    logger.info(f"End Load Necesary Varuables")
    period_date = obtain_last_date()
    TB_BRONCE_PROC_ONE = data['TB_BRONCE_PROC_ONE']
    TB_BRONCE_PROC_TWO_PL = data['TB_BRONCE_PROC_TWO_PL']
    TB_BRONCE_PROC_TWO_G = data['TB_BRONCE_PROC_TWO_G']
    TB_GOLD_PROC_ONE = data['TB_GOLD_PROC_ONE']
    TB_GOLD_PROC_TWO_PL = data['TB_GOLD_PROC_TWO_PL']
    TB_GOLD_PROC_TWO_G = data['TB_GOLD_PROC_TWO_G']
    TB_BRONCE_PROC_THREE_D = data['TB_BRONCE_PROC_THREE_D']
    TB_BRONCE_PROC_THREE_FC = data['TB_BRONCE_PROC_THREE_FC']
    TB_GOLD_PROC_THREE_D = data['TB_GOLD_PROC_THREE_D']
    TB_GOLD_PROC_THREE_FC = data['TB_GOLD_PROC_THREE_FC']
    data = data['PRM']
    period_date = period_date.strftime('%Y-%m-%d')
    period_date_val = datetime.strptime(period_date, '%Y-%m-%d').strftime('%Y%m')
    #-----------------------------------------------------------------#
    #--------------- Load Necesary Tables (Bronce) -------------------#
    #-----------------------------------------------------------------#

    # ----------- Func #1 -------------#

    parametros_actuariales_ipc = None
    try:
        parametros_actuariales_ipc = glueContext.create_dynamic_frame.from_catalog(database=f"{schema_source}", table_name=f"{TB_BRONCE_PROC_ONE}").toDF()
    except Exception as e:
        parametros_actuariales_ipc = None
        logger.info(f"la tabla parametros act bronce no tiene data: {e}")
    parametros_actuariales_ipc_actuales = None

    # ----------- Func #2 -------------#

    df_prima_liquidez = None
    try:
        df_prima_liquidez = glueContext.create_dynamic_frame.from_catalog(database=f"{schema_source}", table_name=f"{TB_BRONCE_PROC_TWO_PL}").toDF()
    except Exception as e:
        df_prima_liquidez = None
        logger.info(f"la tabla prima l bronce no tiene data: {e}")
    df_prima_liquidez_actuales = None
    df_gastos = None
    try:
        df_gastos = glueContext.create_dynamic_frame.from_catalog(database=f"{schema_source}", table_name=f"{TB_BRONCE_PROC_TWO_G}").toDF()
    except Exception as e:
        df_gastos = None
        logger.info(f"la tabla gastos bronce no tiene data: {e}")
    df_gastos_actuales = None
    
    # ----------- Func #3 -------------#
    
    df_deslizamiento = None
    try:
        #df_deslizamiento = glueContext.create_dynamic_frame.from_catalog(database=f"{schema_source}", table_name=f"niff_{TB_BRONCE_PROC_THREE_D}").toDF()
        df_deslizamiento = glueContext.create_dynamic_frame.from_catalog(database=f"{schema_source}", table_name=TB_BRONCE_PROC_THREE_D).toDF()
        if df_deslizamiento.count() < 1:
            df_deslizamiento = None
        df_deslizamiento.cache()
    except Exception as e:
        df_deslizamiento = None
        logger.info(f"la tabla Deslizamiento bronce no tiene data: {e}")
    
    df_factor_cambio = None
    try:
        #df_factor_cambio = glueContext.create_dynamic_frame.from_catalog(database=f"{schema_source}", table_name=f"niff_{TB_BRONCE_PROC_THREE_FC}").toDF()
        df_factor_cambio = glueContext.create_dynamic_frame.from_catalog(database=f"{schema_source}", table_name=TB_BRONCE_PROC_THREE_FC).toDF()
        if df_factor_cambio.count() < 1:
            df_factor_cambio = None
        df_factor_cambio.cache()
    except Exception as e:
        logger.info(f"la tabla factor cambio bronce no tiene data: {e}")
        df_factor_cambio = None
    logger.info(f"End Load Bronce Tables")
    
    #-----------------------------------------------------------------#
    #---------------- Load Necesary Tables (Gold) --------------------#
    #-----------------------------------------------------------------#
    try:
        parametros_actuariales_ipc_actuales = glueContext.create_dynamic_frame.from_catalog(database=f"{schema_target}", table_name=f"niff_{TB_GOLD_PROC_ONE}").toDF()
        if parametros_actuariales_ipc_actuales.count() < 1:
            parametros_actuariales_ipc_actuales = None
    except Exception as e:
        parametros_actuariales_ipc_actuales = None
    
    try:
        df_prima_liquidez_actuales = glueContext.create_dynamic_frame.from_catalog(database=f"{schema_target}", table_name=f"niff_{TB_GOLD_PROC_TWO_PL}").toDF()
        if df_prima_liquidez_actuales.count() < 1:
            df_prima_liquidez_actuales = None
        df_prima_liquidez_actuales.cache()
    except Exception as e:
        df_prima_liquidez_actuales = None
    
    try:
        df_gastos_actuales = glueContext.create_dynamic_frame.from_catalog(database=f"{schema_target}", table_name=f"niff_{TB_BRONCE_PROC_TWO_G}").toDF()
        if df_gastos_actuales.count() < 1:
            df_gastos_actuales = None
        df_gastos_actuales.cache()
    except Exception as e:
        df_gastos_actuales = None

    try:
        #df_deslizamiento_actuales = glueContext.create_dynamic_frame.from_catalog(database=f"{schema_target}", table_name=f"niff_{TB_GOLD_PROC_THREE_D}").toDF()
        df_deslizamiento_actuales = glueContext.create_dynamic_frame.from_catalog(database=f"{schema_target}", table_name=f"{TB_GOLD_PROC_THREE_D}").toDF()
        if df_deslizamiento_actuales.count() < 1:
            df_deslizamiento_actuales = None
        df_deslizamiento_actuales.cache()
    except Exception as e:
        df_deslizamiento_actuales = None
        
    try:
        #df_factor_cambio_actuales = glueContext.create_dynamic_frame.from_catalog(database=f"{schema_target}", table_name=f"niff_{TB_GOLD_PROC_THREE_FC}").toDF()
        df_factor_cambio_actuales = glueContext.create_dynamic_frame.from_catalog(database=f"{schema_target}", table_name=f"{TB_GOLD_PROC_THREE_FC}").toDF()
        if df_factor_cambio_actuales.count() < 1:
            df_factor_cambio_actuales = None
        df_factor_cambio_actuales.cache()
    except Exception as e:
        df_factor_cambio_actuales = None
    
    logger.info(f"End Load Gold Tables")
    ###################################################################
    ############ Init Businees Logic functionality ####################
    ###################################################################

    #------------- Define max Period of Bronce Df´s ------------------#

    #----- funct # 1 -----#

    if parametros_actuariales_ipc != None:
        max_period_new = parametros_actuariales_ipc.agg(F.max("PERIODO")).collect()[0][0]
        parametros_actuariales_ipc_filter = parametros_actuariales_ipc.filter(F.col("PERIODO") == max_period_new)
    
    if parametros_actuariales_ipc_actuales != None:
        window_spec = Window.partitionBy("T")
        df_param_act_ipc_actuales_max_per = parametros_actuariales_ipc_actuales.withColumn("PERIODO_MAX", F.max("PERIODO").over(window_spec))
        parametros_actuariales_ipc_actuales_filter = df_param_act_ipc_actuales_max_per.filter(F.col("periodo") == F.col("periodo_max")).drop("periodo_max")
        parametros_actuariales_ipc_actuales_filter_diff = df_param_act_ipc_actuales_max_per.filter(F.col("periodo") != F.col("periodo_max")).drop("periodo_max")
    if parametros_actuariales_ipc_actuales != None:
        parametros_actuariales_ipc_actuales.cache()    
        
    #----- funct # 2 -----#

    if df_prima_liquidez != None:    
        max_prima_liquidez_new = df_prima_liquidez.agg(F.max("FECHA_INICIO")).collect()[0][0]
        df_prima_liquidez = df_prima_liquidez.filter(F.col("FECHA_INICIO") == max_prima_liquidez_new)
    if df_gastos != None:
        max_prima_gastos_new = df_gastos.agg(F.max("FECHA_INICIO")).collect()[0][0]
        df_gastos = df_gastos.filter(F.col("FECHA_INICIO") == max_prima_gastos_new)
    logger.info(f"End max Period of Bronce Df´s")
    
    #----- funct # 2 -----#

    if df_deslizamiento != None:
        max_fec_ini = df_deslizamiento.agg(F.max("FECHA_INICIO")).collect()[0][0]
        df_deslizamiento = df_deslizamiento.filter(F.col("FECHA_INICIO") == max_fec_ini)
        print("antes de filtrar max deslizamiento")
        print(df_deslizamiento.count())
    
    if df_factor_cambio != None:
        max_fec_ini = df_factor_cambio.agg(F.max("FECHA_INICIO")).collect()[0][0]
        df_factor_cambio = df_factor_cambio.filter(F.col("FECHA_INICIO") == max_fec_ini)

    if df_deslizamiento_actuales != None:
        max_fec_ini = df_deslizamiento_actuales.agg(F.max("FEC_INI_VIG")).collect()[0][0]
        print("antes del filtro deslizamiento AQUI")
        print(df_deslizamiento_actuales.select("FEC_INI_VIG").distinct().count())
        if df_deslizamiento_actuales.select("FEC_INI_VIG").distinct().count() > 1:
            print("entre aqui")
            df_deslizamiento_anteriores = df_deslizamiento_actuales.filter(~(F.col("FEC_INI_VIG") == max_fec_ini))
            df_deslizamiento_actuales = df_deslizamiento_actuales.filter(F.col("FEC_INI_VIG") == max_fec_ini)
        else:
            df_deslizamiento_actuales = df_deslizamiento_actuales.filter((F.col("FEC_INI_VIG") == max_fec_ini))
            df_deslizamiento_anteriores = df_deslizamiento_actuales.filter((F.col("ACTIVO") == "3"))
    
    if df_factor_cambio_actuales != None:
        max_fec_ini = df_factor_cambio_actuales.agg(F.max("FEC_INI_VIG")).collect()[0][0]
        df_factor_cambio_anteriores = df_factor_cambio_actuales.filter(~(F.col("FEC_INI_VIG") == max_fec_ini))
        df_factor_cambio_actuales = df_factor_cambio_actuales.filter(F.col("FEC_INI_VIG") == max_fec_ini)
        if df_factor_cambio_actuales.select("FEC_INI_VIG").distinct().count() > 1:
            print("entre aqui")
            df_factor_cambio_anteriores = df_factor_cambio_actuales.filter(~(F.col("FEC_INI_VIG") == max_fec_ini))
            df_factor_cambio_actuales = df_factor_cambio_actuales.filter(F.col("FEC_INI_VIG") == max_fec_ini)
        else:
            df_factor_cambio_actuales = df_factor_cambio_actuales.filter((F.col("FEC_INI_VIG") == max_fec_ini))
            df_factor_cambio_anteriores = df_factor_cambio_actuales.filter((F.col("ACTIVO") == "3"))
    
    logger.info(f"End filter for last Period (bronce)")
 
    ###################
    ### Apply logic ###
    ###################
    
    for funtionality_data in data:
        functionality = funtionality_data.get("funtionality", {})
        print(f"Functionality to execute is {functionality}")
        execution_date = F.current_date()
        if functionality == "One" and parametros_actuariales_ipc != None and 'ONE' in PARAMS_TO_EXCECUTE:
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
            
            #--- First Execution ---#
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
                        F.when(F.col("PERIODO") == F.col("max_periodo"), F.lit(activo)).otherwise(F.lit(inactivo))
                    ).withColumn(
                        "FEC_INI_VIG_REG",
                        F.when(F.col("PERIODO") == F.col("max_periodo"), F.date_add(F.col("FEC_INI_VIG_REG"), 1)).otherwise(F.col("FEC_INI_VIG_REG"))
                    ).drop("max_periodo","parition_0","parition_1","parition_2")
                
                df_result = df_result.select("PERIODO", "PER_FEC_INI_VIG", "PER_FEC_FIN_VIG", "T", "IPC", "TASA_TECNICA", "FEC_INI_VIG_REG", "FEC_FIN_VIG_REG", "ACTIVO")
            
            #------------------ Save files Data ---------------------#
                partition_col = funtionality_data.get("partition", {})
                partition_columns = [partition_col]
                s3_prefix = f"GOLD/NIIF/{TB_GOLD_PROC_ONE.upper()}/"
                df_result = df_result.distinct()
                save_files(df_result, s3_prefix)
                logger.info(f"End Fist Execution Functionality One")
            #----- Incremental Excecution Functionality ONE -----#
            if parametros_actuariales_ipc_actuales != None:
                
                period_val = parametros_actuariales_ipc_actuales_filter.select("PERIODO").distinct().collect()[0][0]
                if (period_date_val == period_val) or VAL_PERIOD == True:
                    df_join = parametros_actuariales_ipc_actuales_filter.alias("ant").join(
                        parametros_actuariales_ipc_filter.alias("new"),
                        on = "T",
                        how = "outer"
                    )
                    #--- Conditions ---#
                    
                    cond_new = F.col("ant.T").isNull()
                    cond_change = F.col("ant.T").isNotNull() & F.col("new.T").isNotNull()
                    cond_with_out_changes = F.col("new.T").isNull()
                    
                    #--- New register ---#
                    df_news = df_join.filter(cond_new & (F.col("new.INACTIVAR") == inactivo)).select(
                            F.coalesce(F.col("new.periodo"), F.lit(None)).alias("PERIODO"),
                            F.col("new.T").alias("T"),
                            F.lit(activo).alias("ACTIVO"),
                            F.col("new.IPC"),
                            F.col("new.TASA_TECNICA"),
                            (execution_date + F.expr("INTERVAL 1 DAY")).alias("FEC_INI_VIG_REG"),
                            F.lit(None).cast("date").alias("FEC_FIN_VIG_REG"),
                            F.trunc(F.to_date(F.concat_ws("-", F.col("new.periodo").substr(1,4), F.col("new.periodo").substr(5,2), F.lit("01")), "yyyy-MM-dd"), "month").alias("PER_FEC_INI_VIG"),
                            F.last_day(F.to_date(F.concat_ws("-", F.col("new.periodo").substr(1,4), F.col("new.periodo").substr(5,2), F.lit("01")), "yyyy-MM-dd")).alias("PER_FEC_FIN_VIG")
                        )
                    
                    #--- update register ---#
                    df_ant_update = df_join.filter(cond_change & (F.col("new.INACTIVAR") == activo)).select(
                            F.col("ant.periodo").alias("PERIODO"),
                            F.col("ant.T").alias("T"),
                            F.lit(inactivo).alias("ACTIVO"),
                            F.col("ant.IPC"),
                            F.col("ant.TASA_TECNICA"),
                            F.col("ant.FEC_INI_VIG_REG"),
                            F.lit(execution_date).alias("FEC_FIN_VIG_REG"),
                            F.col("ant.PER_FEC_INI_VIG"),
                            F.col("ant.PER_FEC_FIN_VIG")
                        )
                        
                    #--- create new register for the same ID ---#
                    df_change_new = df_join.filter(cond_change & (F.col("new.INACTIVAR") == inactivo)).select(
                            F.coalesce(F.col("new.PERIODO"), F.lit(None)).alias("PERIODO"),
                            F.col("new.T").alias("T"),
                            F.lit(activo).alias("ACTIVO"),
                            F.col("new.IPC"),
                            F.col("new.TASA_TECNICA"),
                            (execution_date + F.expr("INTERVAL 1 DAY")).alias("FEC_INI_VIG_REG"),
                            F.lit(None).cast("date").alias("FEC_FIN_VIG_REG"),
                            F.trunc(F.to_date(F.concat_ws("-", F.col("new.periodo").substr(1,4), F.col("new.periodo").substr(5,2), F.lit("01")), "yyyy-MM-dd"), "month").alias("PER_FEC_INI_VIG"),
                            F.last_day(F.to_date(F.concat_ws("-", F.col("new.periodo").substr(1,4), F.col("new.periodo").substr(5,2), F.lit("01")), "yyyy-MM-dd")).alias("PER_FEC_FIN_VIG")
                        )
                        
                    #--- register without change ---#
                    df_with_out_change = df_join.filter(cond_with_out_changes).select(
                            F.col("ant.periodo").alias("PERIODO"),
                            F.col("ant.T").alias("T"),
                            F.col("ant.ACTIVO"),
                            F.col("ant.IPC"),
                            F.col("ant.TASA_TECNICA"),
                            F.col("ant.FEC_INI_VIG_REG"),
                            F.col("ant.FEC_FIN_VIG_REG"),
                            F.col("ant.PER_FEC_INI_VIG"),
                            F.col("ant.PER_FEC_FIN_VIG")
                        )
                        
                    #--- union dfs ---#
                    df_result = df_news.unionByName(df_ant_update).unionByName(df_change_new).unionByName(df_with_out_change).unionByName(parametros_actuariales_ipc_actuales_filter_diff)
                    
                    #------------------ Save files Data ---------------------#
                    partition_col = funtionality_data.get("partition", {})
                    partition_columns = [partition_col]
                    s3_prefix = f"GOLD/NIIF/{TB_GOLD_PROC_ONE.upper()}/"
                    df_result = df_result.distinct()
                    save_files(df_result, s3_prefix)
                    
                    logger.info(f"End Incremental Execution Functionality One")

        if functionality == "Two" and (df_prima_liquidez != None and df_gastos != None) and 'TWO' in PARAMS_TO_EXCECUTE:
            print("entre al fun 2")
            if df_prima_liquidez_actuales == None:
                print("wnreew al fun 2")
                df_prima_liquidez = df_prima_liquidez.withColumnRenamed("FECHA_INICIO#0","FECHA_INICIO")
                df_result = df_prima_liquidez.withColumn(
                        "FECHA_FIN",
                        F.lit(None).cast(StringType())
                    ).withColumn(
                        "ACTIVO",
                        F.lit(activo)
                    ).withColumn(
                        "FEC_CARGUE",
                        F.lit(execution_date)
                    )
                
                #------------------ Save files Data ---------------------#
                partition_col = funtionality_data.get("partition", {})
                partition_columns = [partition_col]
                s3_prefix = f"GOLD/NIIF/{TB_GOLD_PROC_TWO_PL.upper()}/"
                df_result = df_result.distinct()
                print(df_result.printSchema())
                save_files(df_result, s3_prefix)
                logger.info(f"End Fist Execution Functionality Two Primas")
            if df_prima_liquidez_actuales != None:
                
                period_val = df_prima_liquidez_actuales.select("FECHA_INICIO").distinct().collect()[0][0]
                print(type(period_val))
                period_val = datetime.strptime(period_val, '%Y-%m-%d %H:%M:%S').strftime('%Y%m')
                print(VAL_PERIOD)
                print(f"Print primas {period_date_val} == {period_val}")
                if (period_date_val == period_val) or VAL_PERIOD == True:
                    print("Entre")
                    df_prima_liquidez_actuales = df_prima_liquidez_actuales.withColumn(
                            "ACTIVO",
                            F.lit(inactivo)
                        ).withColumn(
                            "FECHA_FIN",
                            F.date_sub(F.current_date(), 1)
                        )
                    df_result = df_result.withColumn(
                            "FECHA_FIN",
                            F.lit(None)
                        ).withColumn(
                            "ACTIVO",
                            F.lit(activo)
                        ).withColumn(
                            "FEC_CARGUE",
                            F.lit(execution_date)
                        )
    
                    df_result = df_result.unionByName(df_prima_liquidez_actuales)
                    
                    #------------------ Save files Data ---------------------#
                    partition_col = funtionality_data.get("partition", {})
                    partition_columns = [partition_col]
                    s3_prefix = f"GOLD/NIIF/{TB_GOLD_PROC_TWO_PL.upper()}/"
                    df_result = df_result.distinct()
                    print(df_result.printSchema())
                    save_files(df_result, s3_prefix)
                    logger.info(f"End Incremental Execution Functionality Two Primas")
            if df_gastos_actuales == None:
                df_gastos = df_gastos.withColumnRenamed("FECHA_INICIO#0","FECHA_INICIO")
                df_gastos = df_gastos.withColumn(
                        "FECHA_FIN",
                        F.lit(None).cast(StringType())
                    ).withColumn(
                        "ACTIVO",
                        F.lit(activo)
                    ).withColumn(
                        "FEC_CARGUE",
                        F.lit(execution_date)
                    )
                
                #------------------ Save files Data ---------------------#
                partition_col = funtionality_data.get("partition", {})
                partition_columns = [partition_col]
                s3_prefix = f"GOLD/NIIF/{TB_GOLD_PROC_TWO_G.upper()}/"
                df_result = df_gastos.distinct()
                print(df_result.printSchema())
                save_files(df_result, s3_prefix)
                logger.info(f"End Fist Execution Functionality Two Gastos")
            if df_gastos_actuales != None:
                
                period_val = df_gastos_actuales.select("FECHA_INICIO").distinct().collect()[0][0]
                period_val = datetime.strptime(period_val, '%Y-%m-%d %H:%M:%S').strftime('%Y%m')
                print(f"Print gastos {period_date_val} == {period_val}")
                if (period_date_val == period_val) or VAL_PERIOD == True:
                    
                    df_gastos_actuales = df_gastos_actuales.withColumn(
                            "ACTIVO",
                            F.lit(inactivo)
                        ).withColumn(
                            "FECHA_FIN",
                            F.date_sub(F.current_date(), 1)
                        )
                    df_result = df_result.withColumn(
                            "FECHA_FIN",
                            F.lit(None)
                        ).withColumn(
                            "ACTIVO",
                            F.lit(activo)
                        ).withColumn(
                            "FEC_CARGUE",
                            F.lit(execution_date)
                        )
                    
                    df_result = df_result.unionByName(df_gastos_actuales)
                    
                    #------------------ Save files Data ---------------------#
                    partition_col = funtionality_data.get("partition", {})
                    partition_columns = [partition_col]
                    s3_prefix = f"GOLD/NIIF/{TB_GOLD_PROC_TWO_G.upper()}/"
                    df_result = df_result.distinct()
                    print(df_result.printSchema())
                    save_files(df_result, s3_prefix)
                    logger.info(f"End Incremental Execution Functionality Two Gastos")
    
        if functionality == "Three" and (df_deslizamiento != None and df_factor_cambio != None) and 'THREE' in PARAMS_TO_EXCECUTE:
            print("Entre tres")
            #---- first charge -------#
            if df_deslizamiento_actuales == None or df_deslizamiento_actuales.count() == 0:
                df_result = df_deslizamiento.withColumn(
                        "ACTIVO",
                        F.lit(1)
                    ).withColumn(
                        "FEC_CARGUE",
                        F.lit(F.current_date())
                    ).withColumn(
                        "FEC_FIN_VIG",
                        F.lit(None).cast(StringType())
                    ).withColumn(
                        "FEC_INI_VIG",
                        F.col("FECHA_INICIO")
                        #F.date_trunc("month", F.add_months(F.current_date(), 1))
                    )
                    
                df_result = df_result.drop("INACTIVAR","FECHA_INICIO")
                
                partition_col = funtionality_data.get("partition", {})
                partition_columns = [partition_col]
                s3_prefix = f"GOLD/NIIF/{TB_GOLD_PROC_THREE_D.upper()}/"
                df_result = df_result.distinct()
                print(df_result.printSchema())
                save_files(df_result, s3_prefix)
                logger.info(f"End Fist Execution Functionality Three Deslizamiento")
            
            print(df_deslizamiento_actuales)
            print("#----------------------#")
            print(df_deslizamiento)
            if df_deslizamiento_actuales != None and df_deslizamiento != None and df_deslizamiento_actuales.count() > 0:

                df_deslizamiento_inactivar = df_deslizamiento.filter(F.col("INACTIVAR") == '1')
                df_deslizamiento = df_deslizamiento.filter(F.col("INACTIVAR") == '0')
                
                df_result_new = df_deslizamiento.withColumn(
                        "ACTIVO",
                        F.lit(1)
                    ).withColumn(
                        "FEC_CARGUE",
                        F.lit(F.current_date())
                    ).withColumn(
                        "FEC_FIN_VIG",
                        F.lit(None).cast(StringType())
                    ).withColumn(
                        "FEC_INI_VIG",
                        F.col("FECHA_INICIO")
                    ) ##GUARDO LOS NUEVOS
                
                df_ant_des = df_result_new.alias("new").join(df_deslizamiento_actuales.alias("ant"),
                    F.col("ant.T") == F.col("new.T"), "inner"
                )#Inactivar los que tengan datos
                df_ant_des = df_ant_des.withColumn(
                    "ACTIVO_new",
                        F.lit("0")
                    ).withColumn(
                    "FEC_FIN_VIG_new",
                        F.date_trunc("month", F.add_months(F.current_date(), 1))
                    )
                df_ant_des = df_ant_des.select("ant.*","ACTIVO_new","FEC_FIN_VIG_new")
                df_ant_des = df_ant_des.drop(F.col("ant.ACTIVO"),F.col("ant.FEC_FIN_VIG"))
                df_ant_des = df_ant_des.withColumnRenamed("ACTIVO_new","ACTIVO").withColumnRenamed("FEC_FIN_VIG_new","FEC_FIN_VIG")
                
                df_result_new = df_result_new.filter(F.col("INACTIVAR") == '0').drop("INACTIVAR","FECHA_INICIO") #solo nuevos, los 1 no son nuevos solo reporte apra inactivar
                df_ant_sin_cambios = df_deslizamiento_actuales.join(df_result_new, on="T", how="left_anti")
                
                df_result_new = df_result_new.unionByName(df_ant_des).unionByName(df_deslizamiento_anteriores).unionByName(df_ant_sin_cambios)
                df_result_new_inct = df_result_new.alias("New").join(df_deslizamiento_inactivar.alias("Inc"), on="T", how = "inner").select("New.*")
                df_result_new = df_result_new.alias("New").join(df_deslizamiento_inactivar.alias("Inc"), on="T", how = "left_anti").select("New.*")
                
                df_result_new_inct = df_result_new_inct.withColumn(
                    "ACTIVO_new",
                        F.lit("0")
                    ).withColumn(
                    "FEC_FIN_VIG_new",
                        F.date_trunc("month", F.add_months(F.current_date(), 1))
                    )
                df_result_new_inct = df_result_new_inct.select("New.*","ACTIVO_new","FEC_FIN_VIG_new")
                df_result_new_inct = df_result_new_inct.drop(F.col("New.ACTIVO"),F.col("New.FEC_FIN_VIG"))
                df_result_new_inct = df_result_new_inct.withColumnRenamed("ACTIVO_new","ACTIVO").withColumnRenamed("FEC_FIN_VIG_new","FEC_FIN_VIG")
                
                df_result_new = df_result_new.unionByName(df_result_new_inct)
                
                partition_col = funtionality_data.get("partition", {})
                partition_columns = [partition_col]
                s3_prefix = f"GOLD/NIIF/{TB_GOLD_PROC_THREE_D.upper()}/"
                df_result = df_result_new.distinct()
                save_files(df_result, s3_prefix)
                logger.info(f"End Fist Execution Functionality Three Deslizamiento II")
                
            if df_factor_cambio_actuales == None:
                df_result = df_factor_cambio.withColumn(
                        "ACTIVO",
                        F.lit(1)
                    ).withColumn(
                        "FEC_CARGUE",
                        F.lit(F.current_date())
                    ).withColumn(
                        "FEC_FIN_VIG",
                        F.lit(None).cast(StringType())
                    ).withColumn(
                        "FEC_INI_VIG",
                        F.date_trunc("month", F.add_months(F.current_date(), 1))
                    )
                    
                df_result = df_result.drop("INACTIVAR")
                
                partition_col = funtionality_data.get("partition", {})
                partition_columns = [partition_col]
                s3_prefix = f"GOLD/NIIF/{TB_GOLD_PROC_THREE_FC.upper()}/"
                df_result = df_result.distinct()
                print(df_result.printSchema())
                #save_files(df_result, s3_prefix)
                logger.info(f"End Fist Execution Functionality Three Deslizamiento")
            
            if df_factor_cambio_actuales != None and df_factor_cambio != None and df_factor_cambio_actuales.count() > 1000:
               
                df_factor_cambio_inactivar = df_factor_cambio.filter(F.col("INACTIVAR") == '1')
                df_factor_cambio = df_factor_cambio.filter(F.col("INACTIVAR") == '0')
                
                df_result_new = df_factor_cambio.withColumn(
                        "ACTIVO",
                        F.lit(1)
                    ).withColumn(
                        "FEC_CARGUE",
                        F.lit(F.current_date())
                    ).withColumn(
                        "FEC_FIN_VIG",
                        F.lit(None).cast(StringType())
                    ).withColumn(
                        "FEC_INI_VIG",
                        F.col("FECHA_INICIO")
                    ) ##GUARDO LOS NUEVOS
                
                df_ant_des = df_result_new.alias("new").join(df_factor_cambio_actuales.alias("ant"),
                    F.col("ant.T") == F.col("new.T"), "inner"
                )#Inactivar los que tengan datos
                df_ant_des = df_ant_des.withColumn(
                    "ACTIVO_new",
                        F.lit("0")
                    ).withColumn(
                    "FEC_FIN_VIG_new",
                        F.date_trunc("month", F.add_months(F.current_date(), 1))
                    )
                df_ant_des = df_ant_des.select("ant.*","ACTIVO_new","FEC_FIN_VIG_new")
                df_ant_des = df_ant_des.drop(F.col("ant.ACTIVO"),F.col("ant.FEC_FIN_VIG"))
                df_ant_des = df_ant_des.withColumnRenamed("ACTIVO_new","ACTIVO").withColumnRenamed("FEC_FIN_VIG_new","FEC_FIN_VIG")
                
                df_result_new = df_result_new.filter(F.col("INACTIVAR") == '0').drop("INACTIVAR","FECHA_INICIO") #solo nuevos, los 1 no son nuevos solo reporte apra inactivar
                df_ant_sin_cambios = df_factor_cambio_actuales.join(df_result_new, on="T", how="left_anti")
                
                df_result_new = df_result_new.unionByName(df_ant_des).unionByName(df_factor_cambio_anteriores).unionByName(df_ant_sin_cambios)
                df_result_new_inct = df_result_new.alias("New").join(df_factor_cambio_inactivar.alias("Inc"), on="T", how = "inner").select("New.*")
                df_result_new = df_result_new.alias("New").join(df_factor_cambio_inactivar.alias("Inc"), on="T", how = "left_anti").select("New.*")
                
                df_result_new_inct = df_result_new_inct.withColumn(
                    "ACTIVO_new",
                        F.lit("0")
                    ).withColumn(
                    "FEC_FIN_VIG_new",
                        F.date_trunc("month", F.add_months(F.current_date(), 1))
                    )
                df_result_new_inct = df_result_new_inct.select("New.*","ACTIVO_new","FEC_FIN_VIG_new")
                df_result_new_inct = df_result_new_inct.drop(F.col("New.ACTIVO"),F.col("New.FEC_FIN_VIG"))
                df_result_new_inct = df_result_new_inct.withColumnRenamed("ACTIVO_new","ACTIVO").withColumnRenamed("FEC_FIN_VIG_new","FEC_FIN_VIG")
                
                df_result_new = df_result_new.unionByName(df_result_new_inct)
                
                partition_col = funtionality_data.get("partition", {})
                partition_columns = [partition_col]
                s3_prefix = f"GOLD/NIIF/{TB_GOLD_PROC_THREE_FC.upper()}/"
                df_result = df_result_new.distinct()
                save_files(df_result, s3_prefix)
                logger.info(f"End Fist Execution Functionality Three Factor Cambio II")
                
except Exception as e:
    print(traceback.format_exc())
    logger.error(f"An error occurred: {e}")
    sys.exit(1)