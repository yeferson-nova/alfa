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
################# consultar catalogo  #############################
###################################################################

##tipo activo
df_activo = glueContext.create_dynamic_frame.from_catalog(
                        database="alfa-datalake-catalogo-bronce",  
                        table_name="tbl_activo"
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


df_catalogo = (df_activo.alias("c").join(   ##nuevo codigo
    df_atributo.alias("a"),
    F.col("c.id_activo") == F.col("a.id_activo"),
    "inner"
    )
).join(
        df_dominio.alias("d"),
        F.col("c.id_dominio") == F.col("d.id_categoria"),
    "left"
    ).select(
        F.col("c.id_activo"),
        F.col("c.nombre_fisico"),
        F.col("c.id_dominio"),
        F.col("c.id_capa"),
        F.col("a.id_atributo"),
        F.col("a.nombre_campo"),
        F.col("d.id_umbral")
    )                                ##nuevo codigo

logger.info(f"Contenido de catalogo (primeras 20 filas):\n{df_catalogo.show(20)}")  # nuevo


#valor_objetivo = "acsele_acsele_alfa_direccion"
#df_eq = frame_to_catalog.filter(F.col("nombre_fisico") == valor_objetivo



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
                    
                    #ruleset=r'''Rules = [ ColumnValues "DESCRIPCION" != NULL ]'''

                    # Ejecutar evaluación a nivel row 
                    EvaluateDataQualityMultiframe = EvaluateDataQuality().process_rows(
                        frame=frame_to_evaluate,
                        ruleset=EvaluateDataQuality_ruleset,
                        publishing_options={
                            "dataQualityEvaluationContext": f"DataQuality_{table_info['table']}",
                            "enableDataQualityCloudWatchMetrics": True,
                            "enableDataQualityResultsPublishing": True,
                        },
                        additional_options={"observations.scope": "ALL"}, # "observations.scope": "ALL"
                    )  
                    originalData_dyf = SelectFromCollection.apply(
                        dfc=EvaluateDataQualityMultiframe,
                        key="originalData"
                    )
                    df_original = originalData_dyf.toDF()
                    logger.info(f"MUESTRAME ESTE DATAFRAME ORIGINAL:\n{df_original}")
                    
                    logger.info(f"EvaluateDataQualityMultiframe creado correctamente:\n{EvaluateDataQualityMultiframe.keys()}")
                    
                    # ['rowLevelOutcomes ', 'rowLevelOutcomes', 'originalData']
                    
                    # Ejecutar evaluación a nivel dataset
                    rowLevelOutcomes  = SelectFromCollection.apply(
                        dfc=EvaluateDataQualityMultiframe,
                        key="rowLevelOutcomes",
                        #transformation_ctx="rowLevelOutcomes ", ## genera un ciclo 
                    )
                    logger.info("Successfully Evaluate Data Quality")
                    #logger.info(f"AQUI ESTA EL ROWLEVEL OUTCOMES :\n{rowLevelOutcomes}")
                  
                    frame_df = frame_to_evaluate.toDF().cache()   # evita recomputar al hacer count
                    total_rows = frame_df.count()  


                    # Convertir el DynamicFrame a DataFrame --> Se almacena en bronce --> DB --> 
                    df_rule_outcomes = (   ##Agregar fecha al ruler outcomes
                        rowLevelOutcomes .toDF().select(
                               "dataqualityrulespass",   # array<string>
                               "dataqualityrulesfail",
                                "dataqualityrulesskip",
                               "dataqualityevaluationresult"   # array<string
                        )
                        .withColumn("fecha_ejecucion", F.current_date())
                        .withColumn("db_name", F.lit(table_info['db']))
                        .withColumn("schema_name", F.lit(table_info['schema']))
                        .withColumn("table_name", F.lit(table_info['table']))
                        .withColumn("rowsDF", F.lit(total_rows))
                        #.withColumn("id_proceso", 1) #id_jobs_ejecución
                        )  ###hasta este punto OK
                    
                    
                    df_calidad_bronce = df_rule_outcomes.distinct()

                    logger.info(f"Contenido de catalogo (primeras 20 filas):\n{df_calidad_bronce.show(20)}")  # count
                    ###################################################################
                    ########################## PONER EN S3 ############################
                    ###################################################################

                    output_path = f"s3://{BUCKET_NAME}/calidad/dq_results_rows/tbl_DQDL_result_rows/"
                    df_calidad_bronce.write.mode("append").parquet(output_path)
                    logger.info(f"Data Quality Results saved to BRONCE: {output_path}")

                        
                    ###################### 
                    df_base = (
                           df_rule_outcomes
                           .filter(F.col("dataqualityevaluationresult") == "Passed")
                           .select(
                               "fecha_ejecucion",
                               "table_name",
                               "dataqualityrulespass",   # array<string>
                               "rowsDF"
                           )
                       )
                    
                    df_rules = (
                        df_base
                        .select(
                            "fecha_ejecucion",
                            "table_name",
                            "rowsDF",
                            F.posexplode("dataqualityrulespass").alias("id_regla", "rule_text")  # rule_text: string
                        )
                    )
                    

                    pattern = r'^\s*(\w+)\s+"([^"]+)"\s*(<>|<=|>=|<|>|!=|=)\s*(NULL|null|".*?"|\S+)\s*$'
                    
                    df_parsed = (
                        df_rules
                        .withColumn("rule",        F.regexp_extract(F.col("rule_text"), pattern, 1))  # p.ej., ColumnValues
                        .withColumn("atributo",    F.regexp_extract(F.col("rule_text"), pattern, 2))  # p.ej., DESCRIPCION
                        .withColumn("operator",    F.regexp_extract(F.col("rule_text"), pattern, 3))  # p.ej., !=
                        .withColumn("value_raw",   F.regexp_extract(F.col("rule_text"), pattern, 4))  # p.ej., NULL o "ABC"
                        .withColumn("value",       F.when(F.lower(F.col("value_raw")) == "null", F.lit(None)).otherwise(F.regexp_replace(F.col("value_raw"), r'^"(.*)"$', r'\1'))).drop("value_raw")
                                )

                    df_count = (
                        df_parsed
                        .groupBy(
                            "fecha_ejecucion",
                            "table_name",
                            "rowsDF",
                            "id_regla",      # posición de la regla en el array (conserva orden)
                            "rule",
                            "atributo",
                            "operator",
                            "value"
                        )
                        .agg(F.count(F.lit(1)).alias("count"))
                    )
                    df_count.show(20, truncate=False)
                    logger.info("Mostradas primeras 20 filas de DF_COUNT")
                    
                    result = (
                        df_catalogo.alias("c")
                        .join(
                            df_count.alias("d"),
                            on=[
                                F.col("d.table_name") == F.col("c.nombre_fisico"),
                                F.col("d.atributo") == F.col("c.nombre_campo"),
                            ],
                            how="inner"
                        )
                        .select(
                            F.col("d.fecha_ejecucion"),
                            F.col("c.id_activo"),
                            F.col("c.nombre_fisico"),
                            F.col("c.id_dominio"),
                            F.col("c.id_capa"),
                            F.col("c.id_atributo"),
                            F.col("c.id_umbral"),
                            F.col("d.atributo"), 
                            F.lit(1).alias("id_regla"),
                            F.concat_ws("_", F.col("d.operator"), F.col("d.value")).alias("regla"), 
                            F.col("d.rowsDF").alias("valor_referencia"), 
                            F.col("d.count").alias("valor_resultado")
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
                    logger.info(f"Contenido de bronce (primeras 20 filas):\n{df_count.show(20)}")  # count
                    
                    # Mostrar en CloudWatch 
                    # Esto evita que logger.info reciba 'None' de df.show()
                    log_str = result_final._jdf.showString(20, 0, False)  # nuevo
                    logger.info(f"Contenido de silver (primeras 20 filas):\n{log_str}")  # result
                    
                    
                    ###################################################################
                    ########################## PONER EN S3 -- Silver ##################
                    ###################################################################
                    
                    
                    output_path = f"s3://alfa-datalake-dev-silver/calidad/tbl_medicion_calidad/"
                    result_final.distinct().write.mode("overwrite").parquet(output_path)   ##antes habia append tener en cuenta esto
                    logger.info(f"Data Quality Results saved to Silver: {output_path}")
                   # ------------------------------------------------------------------

                except Exception as e:
                    logger.error(f"ERROR WITH Evaluate Data Quality: {traceback.format_exc()}")  

    else:
        logger.info("NOT TABLE FOR Evaluate Data Quality")

