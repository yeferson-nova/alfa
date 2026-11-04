"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.transforms import SelectFromCollection 
from commons.LogsHelper import LogsHelper
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, current_timestamp
from pyspark.sql.window import Window
import sys, traceback, boto3, json


###################################################################
######## SET ARGS AND CONFIG COMMONS UTILS ########################
###################################################################

logs = LogsHelper()
logger = logs.logger_conf()

args = getResolvedOptions(
    sys.argv, 
        [
        'DB_NAME',    
        'JOB_NAME',
        'DB_SCHEMA',
        'DB_CATALOG_CALIDAD',
        'BUCKET_NAME',
        'BUCKET_NAME_ASSETS',
        'BUCKET_NAME_SIL'
        ]
    )
DB_SCHEMA = args['DB_SCHEMA']
DB_CATALOG_CALIDAD = args['DB_CATALOG_CALIDAD']
BUCKET_NAME = args['BUCKET_NAME']
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
BUCKET_NAME_SIL = args['BUCKET_NAME_SIL']
DB_NAME = args['DB_NAME']
PATH_JSON = f"support_phase_files/calidad/{DB_NAME.lower()}_{DB_SCHEMA.lower()}_rules_tables.json" ##cambiar ruta y apuntar al nuevo archivo




logger.info(f"Schema: {DB_SCHEMA}")
logger.info(f"Bucket_name: {BUCKET_NAME}")
logger.info(f"Bucket_name_silver: {BUCKET_NAME_SIL}")
logger.info(f"Json_file: s3://{BUCKET_NAME_ASSETS}/{PATH_JSON}")

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

###################################################################
############## VALIDATE FILE IN S3 & READ JSON ####################
###################################################################
 
try:
    # Check if the file exists before attempting to read it
    s3_client.head_object(Bucket=BUCKET_NAME_ASSETS, Key=PATH_JSON)
    logger.info(f"The file {PATH_JSON} exists in S3. Proceeding with the reading.")

    # If the file exists, proceed with the reading
    response = s3_client.get_object(Bucket=BUCKET_NAME_ASSETS, Key=PATH_JSON)
    catalog_tables = json.loads(response["Body"].read().decode("utf-8"))  
    logger.info("JSON file successfully loaded.")

except s3_client.exceptions.NoSuchKey:
    logger.error(f"ERROR: The file {PATH_JSON} does not exist in the bucket {BUCKET_NAME_ASSETS}.")
    sys.exit(1)  # Stop execution if the file does not exist 

except Exception as e:
    logger.exception(f"Unexpected error accessing the JSON in S3: {traceback.format_exc()}")  ##se cambio logger error por logger Exception
    sys.exit(1)

###################################################################
################# constantes mas usadas############################
###################################################################


COL_C_ID_ACTIVO = "c.id_activo"
COL_C_NOMBRE_FISICO = "c.nombre_fisico"
COL_C_ID_DOMINIO = "c.id_dominio"
COL_C_ID_CAPA = "c.id_capa"

COL_A_ID_ACTIVO = "a.id_activo"
COL_A_ID_ATRIBUTO = "a.id_atributo"
COL_A_NOMBRE_CAMPO = "a.nombre_campo"

COL_D_ID = "d.id"
COL_D_ID_UMBRAL = "d.id_umbral"


###################################################################
################# consultar catalogo  #############################
###################################################################

##tipo activo
df_activo = glueContext.create_dynamic_frame.from_catalog(
                        database=f"{DB_CATALOG_CALIDAD}",  
                        table_name="tbl_activo"
                    ).toDF()
#atributo                    
df_atributo = glueContext.create_dynamic_frame.from_catalog(
                        database=f"{DB_CATALOG_CALIDAD}",  
                        table_name="tbl_atributo"
                    ).toDF()
#dominio
df_dominio = glueContext.create_dynamic_frame.from_catalog(
                        database=f"{DB_CATALOG_CALIDAD}",  
                        table_name="tbl_dominio"
                    ).toDF()  


df_catalogo = (
    df_activo.alias("c")
    .join(
        df_atributo.alias("a"),
        F.col(COL_C_ID_ACTIVO) == F.col(COL_A_ID_ACTIVO),
        "inner"
    )
    .join(
        df_dominio.alias("d"),
        F.col(COL_C_ID_DOMINIO) == F.col(COL_D_ID),
        "left"
    )
    .select(
        F.col(COL_C_ID_ACTIVO),
        F.col(COL_C_NOMBRE_FISICO),
        F.col(COL_C_ID_DOMINIO),
        F.col(COL_C_ID_CAPA),
        F.col(COL_A_ID_ATRIBUTO),
        F.col(COL_A_NOMBRE_CAMPO),
        F.col(COL_D_ID_UMBRAL)
    )
)


logger.info(f"Contenido de catalogo (primeras 20 filas UMBRAL):\n{df_catalogo.show(20)}")  # nuevo



###################################################################
################# ITERATE FOR EACH TABLE ##########################
###################################################################


# Dictionary to store EvaluateDataQualityMultiframe for each table

for schema in catalog_tables:
    if schema == DB_SCHEMA:  
        for table_info in catalog_tables[schema]:
            if table_info['status']:
                logger.info(f"Tables for ingest: {table_info['db']}.{table_info['table']}")
                logger.info(f"TABLA QUE BUSCA:\n{table_info['table']}")
                try:
                    
                    frame_to_evaluate = glueContext.create_dynamic_frame.from_catalog(
                        database=f"{table_info['db']}",  
                        table_name=f"{table_info['table']}"
                    )

                    frame_df = (
                        frame_to_evaluate
                        .toDF()
                        .withColumn("_row_id", F.monotonically_increasing_id())
                    )
                    
                    
                    frame_with_id_dyf = DynamicFrame.fromDF(
                        frame_df,
                        glueContext,
                        "frame_with_id_dyf"
                    )

                    # Construir dinámicamente el conjunto de reglas
                    rules_str = "\n".join(table_info['rules'])
                    logger.info(f"MUESTRAME ESTAS REGLAS:\n{rules_str}")
                    
                    ##Validacion
                    df = frame_to_evaluate.toDF()
                    df.printSchema()
                    log_str = df._jdf.showString(20, 0, False)  # filas, columnas, truncar
                    logger.info(f"Preview de FRAME TO EVALUATE {table_info['table']}:\n{log_str}")

                    EvaluateDataQuality_ruleset = "Rules = [\n  " + ",\n  ".join(table_info['rules']) + "\n]"
                    
                    logger.info(f"EvaluateDataQualityMultiframe creado correctamente:\n{EvaluateDataQuality_ruleset}")
                    print(f"Son los datos a val: {EvaluateDataQuality_ruleset}")

                    EvaluateDataQualityMultiframe = EvaluateDataQuality().process_rows(
                        frame=frame_with_id_dyf,
                        ruleset=EvaluateDataQuality_ruleset,
                        publishing_options={
                            "dataQualityEvaluationContext": f"DataQuality_{table_info['table']}",
                            "enableDataQualityCloudWatchMetrics": False,
                            "enableDataQualityResultsPublishing": False,
                        },
                        additional_options={"observations.scope": "ALL"}, # "observations.scope": "ALL"
                    )  
                    originalData_dyf = SelectFromCollection.apply(
                        dfc=EvaluateDataQualityMultiframe,
                        key="originalData"
                    )
                    df_original = originalData_dyf.toDF()
                    df_original.select("_row_id").show(5)  
                    logger.info(f"MUESTRAME ESTE DATAFRAME ORIGINAL:\n{df_original}")
                    
                    logger.info(f"EvaluateDataQualityMultiframe creado correctamente:\n{EvaluateDataQualityMultiframe.keys()}")

                    rowLevelOutcomes  = SelectFromCollection.apply(
                        dfc=EvaluateDataQualityMultiframe,
                        key="rowLevelOutcomes",
                    )
                    logger.info("Successfully Evaluate Data Quality")
                    
                    
                    frame_df = frame_to_evaluate.toDF().withColumn("_row_id", F.monotonically_increasing_id()).cache() 
                    
                    
                    frame_with_id_dyf = DynamicFrame.fromDF(
                        frame_df,
                        glueContext,
                        "frame_with_id_dyf"
                    )

                        
                        
                    total_rows = frame_df.count()     
                    
                    df_total_rows = (    spark.createDataFrame([(table_info['table'], total_rows)],["table_name", "rowsDF"]))

                    
                    df_row_outcomes = rowLevelOutcomes.toDF()
                        
                    
                    df_rule_outcomes = (
                        df_row_outcomes
                        .withColumn("fecha_ejecucion", F.current_date())
                        .withColumn("db_name", F.lit(table_info['db']))
                        .withColumn("schema_name", F.lit(table_info['schema']))
                        .withColumn("table_name", F.lit(table_info['table']))
                        .withColumn("rowsDF", F.lit(total_rows))
                    )


                    
                    df_calidad_bronce = rowLevelOutcomes.toDF()

                    logger.info(f"Contenido de catalogo (primeras 20 filas):\n{df_calidad_bronce.show(20)}")  # count
                    ###################################################################
                    ########################## PONER EN S3 ############################
                    ###################################################################

                    output_path = f"s3://{BUCKET_NAME}/calidad/dq_results_rows/tbl_DQDL_result_rows/"
                    df_calidad_bronce.write.mode("append").parquet(output_path)
                    logger.info(f"Data Quality Results saved to BRONCE: {output_path}")

                        
                    
                    #Evaluacion de nuevas reglas 
                    
                    df_pass = (
                        df_rule_outcomes
                        .select(
                            "_row_id",  
                            "fecha_ejecucion",
                            "table_name",
                            "rowsDF",
                            F.posexplode("dataqualityrulespass").alias("id_regla", "rule_text")
                        )
                        .withColumn("resultado_regla", F.lit("PASSED"))
                    )
                    print(f" data pass {df_pass.show()}")
                    df_fail = (
                        df_rule_outcomes
                        .select(
                            "_row_id",   
                            "fecha_ejecucion",
                            "table_name",
                            "rowsDF",
                            F.posexplode("dataqualityrulesfail").alias("id_regla", "rule_text")
                        )
                        .withColumn("resultado_regla", F.lit("FAILED"))
                    )    
                    print(f" data fail {df_fail.show()}")
                    df_rules_all = df_pass.unionByName(df_fail)  #aqui unimos los dos dataframes
                    
                    #parseo de los campos 
                    pattern = r'^\s*(\w+)\s+"([^"]+)"\s*(<>|<=|>=|<|>|!=|=)\s*(NULL|null|".*?"|\S+)\s*$'
                    
                    df_rules_struct = (
                        df_rules_all
                        .withColumn("rule",     F.regexp_extract(F.col("rule_text"), pattern, 1))
                        .withColumn("atributo", F.regexp_extract(F.col("rule_text"), pattern, 2))
                        .withColumn("operator", F.regexp_extract(F.col("rule_text"), pattern, 3))
                        .withColumn("value_raw",F.regexp_extract(F.col("rule_text"), pattern, 4))
                        .withColumn("value",    F.when(F.lower(F.col("value_raw")) == "null",   F.lit(None)).otherwise(F.regexp_replace(F.col("value_raw"), r'^"(.*)"$', r'\1'))).drop("value_raw")
                    )
                    
                    df_failed_rows = (
                        df_rules_struct
                        .filter(F.col("resultado_regla") == "FAILED")
                        .select(
                            "fecha_ejecucion",
                            "table_name",
                            "atributo",
                            "_row_id"
                        )
                        .dropDuplicates(["fecha_ejecucion", "table_name", "atributo", "_row_id"])
                    )
            
                    
                   
                    
                    
                    df_failed_count = (
                        df_failed_rows
                        .groupBy(
                            "fecha_ejecucion",
                            "table_name",
                            "atributo"
                        )
                        .count()
                        .withColumnRenamed("count", "filas_fallidas")
                    )

                    df_metricas = (
                        df_failed_count
                        .withColumn("valor_referencia", F.lit(total_rows))
                        .withColumn(
                            "valor_resultado",
                            F.lit(total_rows) - F.col("filas_fallidas")
                        )
                    )

                    df_passed_only = (
                        df_rules_struct
                        .filter(F.col("resultado_regla") == "PASSED")
                        .select(
                            "fecha_ejecucion",
                            "table_name",
                            "atributo"
                        )
                        .dropDuplicates(["fecha_ejecucion", "table_name", "atributo"])
                        .withColumn("filas_fallidas", F.lit(0))
                        .withColumn("valor_referencia", F.lit(total_rows))
                        .withColumn("valor_resultado", F.lit(total_rows))
                    )

                    
                    df_final_metricas = df_metricas.unionByName(df_passed_only)
                    
                    
                    df_metricas_por_atributo = (
                        df_final_metricas
                        .groupBy(
                            "fecha_ejecucion",
                            "table_name",
                            "atributo"
                        )
                        .agg(
                            F.max("valor_referencia").alias("valor_referencia"),
                            F.min("valor_resultado").alias("valor_resultado")
                        )
                    )
                    
                    log_str = (
                        df_metricas_por_atributo
                        .select("atributo", "valor_referencia", "valor_resultado")
                        .orderBy("atributo")
                        ._jdf
                        .showString(20, 0, False)
                    )
                    
                    logger.info(
                        f"""
                        VALIDACIÓN MÉTRICAS POR ATRIBUTO
                        --------------------------------
                        {log_str}
                        """
                    )

                    
                    result = (
                        df_catalogo.alias("c")
                        .join(
                            df_metricas_por_atributo .alias("d"),
                            on=[
                                F.col("d.table_name") == F.col("c.nombre_fisico"),
                                F.col("d.atributo") == F.col("c.nombre_campo"),
                            ],
                            how="inner"
                        )
                        .select(
                            F.col("d.fecha_ejecucion"),
                            F.col(COL_C_ID_ACTIVO),
                            F.col(COL_C_NOMBRE_FISICO),
                            F.col(COL_C_ID_DOMINIO),
                            F.col(COL_C_ID_CAPA),
                            F.col("c.id_atributo"),   
                            F.col("c.id_umbral"),   
                            F.col("d.atributo"), 
                            F.col("d.valor_referencia"),
                            F.col("d.valor_resultado")

                        )

                    )
                    
                    
                    result_final = (  ## Agregando el periodo para capa silver
                        result
                        
                        .withColumn("fecha_ejecucion",F.col("fecha_ejecucion").cast("timestamp"))
                        .withColumn("fecha_corta", F.to_date(F.col("fecha_ejecucion")))
                        .withColumn("periodo", F.date_format(F.col("fecha_ejecucion"), "yyyyMM"))
                    )

                    
                    ## poner id_atributo.
                    result_final.show(truncate=False)
                    logger.info(f"Contenido de bronce (primeras 20 filas UMBRAL FINAL ENSERIO):\n{result.show(20)}")  # count
                    
                    # Mostrar en CloudWatch 
                    # Esto evita que logger.info reciba 'None' de df.show()
                    log_str = result_final._jdf.showString(20, 0, False)  # nuevo
                    logger.info(f"Contenido de silver (primeras 20 filas UMBRAL FINAL MILTON):\n{log_str}")  # result
                    
                    
                    result_final = result_final.withColumn(
                        "table_name",
                        F.lit(table_info["table"])
                    )

                    
                    ###################################################################
                    ########################## PONER EN S3 -- Silver ##################
                    ###################################################################

                    (
                        result_final
                        .withColumn("table_name", F.lit(table_info["table"]))  
                        .write
                        .mode("append")                                        
                        .parquet(f"s3://{BUCKET_NAME_SIL}/calidad/")
                    )
                    
                    logger.info(
                        f"data Quality Results appended for table {table_info['table']}")

                   # ------------------------------------------------------------------

                except Exception as e:
                    logger.error(f"ERROR WITH Evaluate Data Quality: {traceback.format_exc()}")  

    else:
        logger.info("NOT TABLE FOR Evaluate Data Quality")

