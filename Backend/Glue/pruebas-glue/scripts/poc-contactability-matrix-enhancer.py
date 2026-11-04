#------ AWS GLUE ---------#
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
#-------SPARK--------------#
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
#-----CUSTOM IMPORTS -----------#
from s3_handler import S3Handler
from utils import Utils
#------PYTHON IMPORTS -------#
import os
import sys
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
utils = Utils()
STG_PATH = "s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_silver/staging_definitions/"
# procesador cardiff
print("======CARDIFF=======")
# 01. Search Path
staging_path = utils.search_json_by_db_name(s3_path=STG_PATH, db_name="CARDIFF")
# 02. Load Json with data
response = utils._read_json_from_s3(s3_target_path=staging_path)
# 03. Search s3_path
s3_table_path = response['tables_processed']['CUSTOMER'].get("s3_path","")
print(s3_table_path)
response.__class__
# procesador alfprd
def _get_json_s3_paths() -> tuple:
    """
    Get the json with the response of each path
    """
    # 01. Search Path
    alfaprod_path = utils.search_json_by_db_name(s3_path=STG_PATH, db_name = "ALFAPROD")
    alfprd_path = utils.search_json_by_db_name(s3_path=STG_PATH db_name = "ALFPRD")
    # 02. Load Json with data
    alfaprod_response = utils._read_json_from_s3(s3_target_path=staging_path)
    alfprd_response = utils._read_json_from_s3(s3_target_path=staging_path)
    return alfaprod_response, alfprd_response




print("======ALFAPROD=======")
# 01. Search Path
staging_path = utils.search_json_by_db_name(s3_path=STG_PATH, db_name="CARDIFF")
# 02. Load Json with data
response = utils._read_json_from_s3(s3_target_path=staging_path)
# 03. Search s3_path
s3_table_path = response['tables_processed']['CUSTOMER'].get("s3_path","")
print(s3_table_path)
BUCKET_SILVER = 'alfa-datalake-dev-silver'
utils = Utils()
s3_prefix = f's3://{BUCKET_SILVER}/CLIENTES/'
df_silver = utils.read_full_parquet(spark, s3_target_path=os.path.join(s3_prefix,'DATOS_UBICACION_Y_CONTACTABILIDAD','CLIENTES_DATOS_UBICACION_Y_CONTACTABILIDAD_SILVER'))
df_concordance_matrix = utils.read_full_parquet(spark, 
                                                s3_target_path=os.path.join(s3_prefix,'DATOS_UBICACION_Y_CONTACTABILIDAD','CLIENTES_DATOS_UBICACION_Y_CONTACTABILIDAD_MATRIX_PIVOTED'))
print(f"CLIENTES_DATOS_UBICACION_CONTACTABILIDAD_SILVER: {df_silver.count()}")
print(f"CLIENTES_DATOS_UBICACION_CONTACTABILIDAD_SILVER_MATRIX_PIVOTED: {df_concordance_matrix.count()}")
from pyspark.sql.functions import broadcast, coalesce, when, lit, col, concat
from pyspark.sql.window import Window
# 1. Preparar los dataframes para el cruce
df_join_matrix = df_concordance_matrix
df_join_silver = df_silver
# 2. Realizar la columna PK en df_join_silver
df_join_silver = df_join_silver.withColumn("PK", 
                                           concat(col("cod_tipo_identificacion"),lit("-"),col("numero_documento")
                                                 )
                                          )
# 3. Seleccionar solo las columnas que aparecen en la matrix pivoteada
# y eliminar las que no sean del negocio
exclude_cols  = ['partition_0','partition_1','partition_2','pk_hash_partition','partition_date', 'created_at']
selected_cols = [col for col in df_join_matrix.columns if col not in exclude_cols]
# 4. Filtrar ambos dataframes con las columnas seleccionadas
df_join_silver_filter = df_join_silver[selected_cols]
df_join_matrix_filter = df_join_matrix[selected_cols]
# 5. Realizar Join de silver con la concordance matrix
df_join_silver_only = df_join_matrix_filter.select("PK").join(#rigth join
        df_join_silver_filter,
        on=["PK"],
        how="right",
    ).withColumn("best_record", lit(False))
# 6. Agregar best_record = True en matrix
df_join_matrix_filter = df_join_matrix_filter.withColumn("best_record", lit(True))
# 7. Realizar la union entre la información extraida del join_silver con la matrix
df_final_contactabilidad = df_join_matrix_filter.unionByName(df_join_silver_only)
# 8. Contar cuántas veces aparece cada PK y seleccionar las que aparecen exactamente dos veces
df_counts = df_final_contactabilidad.groupBy("PK").agg(count("*").alias("count"))
df_pk_twice = df_counts.filter(col("count") == 2).select("PK")
# 9. Crear ventana para ordenar por best_record (True primero)
window_spec = Window.partitionBy("PK").orderBy(col("best_record").desc())
df_with_rownum = df_final_contactabilidad.withColumn("row_num", row_number().over(window_spec))
# 10. Filtrar según estas reglas
#    10.1 Si PK aparece exactamente dos veces → dejar solo el row_num == 1
#    10.2 PK no está duplicada dos veces → dejar todos

#    10.1 Si PK aparece exactamente dos veces → dejar solo el row_num == 1
df_twice_filtered = df_with_rownum.join(df_pk_twice, on="PK", how="inner").filter(col("row_num") == 1)
#    10.2 PK no está duplicada dos veces → dejar todos
df_rest = df_with_rownum.join(df_pk_twice, on="PK", how="left_anti")
# 11. Unir ambos resultados
df_final_cleaned = df_twice_filtered.unionByName(df_rest).drop("row_num")
df_final_cleaned.count()
df_final_cleaned
# 8. Validacion de Cantidades de Clientes Únicos
clientes_unicos_matrix = df_join_silver.select("PK").distinct().count()
clientes_unicos_silver = df_concordance_matrix.select("PK").distinct().count()
clientes_unicos_df_final = df_final_contactabilidad.select("PK").distinct().count()
print(f"Clientes Silver: {df_silver.count()}")
print(f"Clientes Matrix: {df_concordance_matrix.count()}")
print(f"Clientes Df Final: {df_final_contactabilidad.count()}")
print(f"Clientes Únicos Matrix: {clientes_unicos_matrix}")
print(f"Clientes Únicos Silver: {clientes_unicos_silver}")
print(f"Clientes Únicos Df Final: {clientes_unicos_df_final}")
df_final_contactabilidad.count()
df_final_contactabilidad.filter(col("PK") == "1-10059411").show(truncate=False)
df_final_cleaned.filter(col("PK") == "1-10059411").show(truncate=False)
df_final_cleaned.filter(col("PK") == "1-1000000478").show(truncate=False)
df_final_contactabilidad.filter(col("PK") == "1-1000000478").show(truncate=False)
job.commit()