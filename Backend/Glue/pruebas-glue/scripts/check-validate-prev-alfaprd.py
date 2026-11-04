import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import pandas as pd
import time
from io import StringIO
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
print(f"Delta Extensions: {spark.conf.get('spark.sql.extensions')}")
# Spark Libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, coalesce, when, substring, 
    max as spark_max, sum as spark_sum, count,
    row_number, desc, concat, to_date, date_format, current_date,
    add_months, lpad, upper, trim
)
from pyspark.sql.types import DecimalType
from decimal import Decimal
from datetime import datetime
from pyspark.sql.window import Window
# Custom Libraries
from logger_config import logger
from utils import Utils

spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
utils_instance = Utils()
BUCKET_ASSETS = "alfa-datalake-dev-clientes-assets"
PATH_CLIENTES_ARL_GOLD = f"s3://{BUCKET_ASSETS}/phase_bronze/XLSX/ALFAPRD/ARL/clientes_gold_arl_202504.xlsx"
PATH_CLIENTES_ARL_USERS = f"s3://{BUCKET_ASSETS}/test/Active_Customers_202504_Users_ARL.xlsx"
PATH_TIPOS_DOC = f"s3://{BUCKET_ASSETS}/test/tipos_doc.xlsx"
df_arl_1 = utils_instance.read_excel_from_s3(PATH_CLIENTES_ARL_USERS,'asegurados')
df_arl_2 = utils_instance.read_excel_from_s3(PATH_CLIENTES_ARL_GOLD,'ARL')
df_arl_users = spark.createDataFrame(df_arl_1)
df_arl_alfaprd = spark.createDataFrame(df_arl_2)
df_tipos_doc = utils_instance.read_excel_from_s3(PATH_TIPOS_DOC,'datos')
df_tp_doc = spark.createDataFrame(df_tipos_doc)
count_arl_users = df_arl_users.count()
count_arl_alfaprd = df_arl_alfaprd.count()
missing_arl_alfaprd = count_arl_users - count_arl_alfaprd
print(f'Totales\nARL Usuarios: {count_arl_users:,}\nARL Alfaprd QA: {count_arl_alfaprd:,}\nFaltantes ARL QA: {missing_arl_alfaprd:,}')
# PySpark — limpiar nombres de columnas a snake_case sin espacios ni caracteres especiales

import re

def clean_column(col_name):
    col_name = col_name.lower()
    col_name = re.sub(r"[áàäâ]", "a", col_name)
    col_name = re.sub(r"[éèëê]", "e", col_name)
    col_name = re.sub(r"[íìïî]", "i", col_name)
    col_name = re.sub(r"[óòöô]", "o", col_name)
    col_name = re.sub(r"[úùüû]", "u", col_name)
    col_name = re.sub(r"ñ", "n", col_name)
    col_name = re.sub(r"[^a-z0-9]+", "_", col_name)
    col_name = re.sub(r"(^_|_$)", "", col_name)
    return col_name 
df_clean_arl_users = df_arl_users.select([
col(f"`{c}`").alias(clean_column(c)) for c in df_arl_users.columns
])
df_clean_arl_gold = df_arl_alfaprd.select([
col(f"`{c}`").alias(clean_column(c)) for c in df_arl_alfaprd.columns
])
df_tp_doc = df_tp_doc.withColumn(
    "tipo_de_documento",
     upper(trim(col("tipo_de_documento")))
)
df_tp_doc = df_tp_doc.select(col("id"), col("codigo"), col("tipo_de_documento"))
df_tp_doc.show(5)
# cross with new columns
df_arl_users_filter = df_clean_arl_users.join(
    df_tp_doc.select("tipo_de_documento", "codigo"),
    df_clean_arl_users["abreviatura_tipo_id"] == df_tp_doc["tipo_de_documento"],
    how="inner"
  )
df_arl_users_filter = df_arl_users_filter.select(
    col("tipo_de_documento"),
    col("codigo"),
    col("no_identificacion"),
    col("no_afiliacion")
)
df_arl_users_filter = df_arl_users_filter.withColumn(
    "numero_poliza",
    concat(
        col("no_afiliacion"),lit("-"),col("codigo"),lit("-"),col("no_identificacion"))
)
df_arl_users_filter.show(5,truncate=False)
df_clean_arl_gold.show(5,truncate=False)
df_missing = df_arl_users_filter.alias("t1").join(
    df_clean_arl_gold.alias("t2"),
    col("t1.numero_poliza") == col("t2.numero_poliza"),
    how="left_anti"
)
count_missing = df_missing.count()
print(f'Total Faltantes: {count_missing:,}')
dups_users = df_users_norm.groupBy("key").count().filter("count > 1").count()
dups_alfaprd = df_alfaprd_norm.groupBy("key").count().filter("count > 1").count()
print(f'Dups ALFAPRD: {dups_alfaprd:,}\nDups Users: {dups_users:,}')
df_match = df_users_norm.join(
    df_alfaprd_norm.select("key").distinct(),
    on="key",
    how="inner"
)

df_match.count()
df_missing = df_users_norm.alias("t1").join(
    df_alfaprd_norm.alias("t2"),
    col("t1.key") == col("t2.key"),
    how="left_anti")
count_missing = df_missing.count()
print(f'Total Faltantes: {count_missing:,}')
df_missing.printSchema()
df_pandas = df_missing.toPandas()
#df_pandas.to_excel('s3://alfa-datalake-dev-clientes-assets/phase_bronze/XLSX/ALFAPRD/ARL/Missing-ARL-QA.xlsx',index=False)
df_pandas.to_excel('s3://alfa-datalake-dev-clientes-assets/phase_bronze/XLSX/ALFAPRD/ARL/Missing-ARL-GOLD-240226-1.xlsx',index=False)
df_missing
BUCKET_ASSETS = "alfa-datalake-dev-clientes-assets"
PATH_CLIENTES_RENTAS_GOLD = f"s3://{BUCKET_ASSETS}/phase_bronze/XLSX/ALFAPRD/RENTAS/Rentas_Datalake_202506.xlsx"
PATH_CLIENTES_RENTAS_USERS = f"s3://{BUCKET_ASSETS}/phase_bronze/XLSX/ALFAPRD/RENTAS/Active_Customers_Rentas_Users_202506.xlsx"
PATH_TIPOS_DOC = f"s3://{BUCKET_ASSETS}/test/tipos_doc.xlsx"
df_rentas_1 = utils_instance.read_excel_from_s3(PATH_CLIENTES_RENTAS_USERS, 'Datos')
df_rentas_2 = utils_instance.read_excel_from_s3(PATH_CLIENTES_RENTAS_GOLD, 'rentas')
df_rentas_users = spark.createDataFrame(df_rentas_1)
df_rentas_gold = spark.createDataFrame(df_rentas_2)
# PySpark — limpiar nombres de columnas a snake_case sin espacios ni caracteres especiales

import re

def clean_column(col_name):
    col_name = col_name.lower()
    col_name = re.sub(r"[áàäâ]", "a", col_name)
    col_name = re.sub(r"[éèëê]", "e", col_name)
    col_name = re.sub(r"[íìïî]", "i", col_name)
    col_name = re.sub(r"[óòöô]", "o", col_name)
    col_name = re.sub(r"[úùüû]", "u", col_name)
    col_name = re.sub(r"ñ", "n", col_name)
    col_name = re.sub(r"[^a-z0-9]+", "_", col_name)
    col_name = re.sub(r"(^_|_$)", "", col_name)
    return col_name 
df_clean_rentas_users = df_rentas_users.select([
col(f"`{c}`").alias(clean_column(c)) for c in df_rentas_users.columns
])
df_clean_rentas_gold = df_rentas_gold.select([
col(f"`{c}`").alias(clean_column(c)) for c in df_rentas_gold.columns
])
df_clean_rentas_users = df_clean_rentas_users.filter(col("ramo") == "40")
# CROSS POLIZA | FIND MISSING INFO
df_missing = df_clean_rentas_users.join(
    df_clean_rentas_gold.select("numero_poliza").distinct(),
    df_clean_rentas_users["no_poliza"] == df_clean_rentas_gold["numero_poliza"],
    how="left_anti"
)
count_missing = df_missing.count()
print(f'Total Faltantes: {count_missing:,}')
df_pandas = df_missing.toPandas()
df_pandas.to_excel('s3://alfa-datalake-dev-clientes-assets/phase_bronze/XLSX/ALFAPRD/RENTAS/Missing-Rentas-QA-250226.xlsx',index=False)
# Spark Libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, coalesce, when, substring, 
    max as spark_max, sum as spark_sum, count,
    row_number, desc, concat, to_date, date_format, current_date,
    add_months, lpad
)
from pyspark.sql.types import DecimalType
from decimal import Decimal
from datetime import datetime
from pyspark.sql.window import Window
# Custom Libraries
from logger_config import logger
from utils import Utils

spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
utils_instance = Utils()
BUCKET_SILVER = "alfa-datalake-dev-silver"
PATH_CLIENTES = f"s3://{BUCKET_SILVER}/POLIZAS_PRODUCTO/ALFAPRD/ALFAPRD_CLIENTES_STG/"
PATH_POLIZAS = f"s3://{BUCKET_SILVER}/POLIZAS_PRODUCTO/ALFAPRD/ALFAPRD_POLIZAS_STG/"
# Read and Filter by Period 202507
df_clientes = utils_instance.read_full_delta(spark, PATH_CLIENTES)
df_cl_prv_tierra = df_clientes.filter(
    (col("origen") == "PREVISIONAL") & 
    (col("yyyy") == "2025") & 
    (col("mm") == "07") &
    (col("rol") == "ASEGURADO")
)
df_cl_prv_plus = df_clientes.filter(
    (col("origen") == "PREVISIONAL_PLUS") & 
    (col("yyyy") == "2025") & 
    (col("mm") == "07") &
    (col("rol") == "ASEGURADO")
)
#ℹ️ INFO [2026-02-14 18:19:20] [PrevisionalesMVP2SilverProcessor] :   PLUS ASEGURADOS: 507
#ℹ️ INFO [2026-02-14 18:19:20] [PrevisionalesMVP2SilverProcessor] :   TIERRA ASEGURADOS: 4,878,761
plus_count = df_cl_prv_plus.count()
tierra_count = df_cl_prv_tierra.count()
print(f'Asegurados Plus: {plus_count:,}')
print(f'Asegurados Tierra: {tierra_count:,}')
df_en_ambos = df_cl_prv_plus.join(
                df_cl_prv_tierra.select(
                    "tipo_identificacion", 
                    "numero_identificacion"
                ).distinct(),
                ["tipo_identificacion", "numero_identificacion"],
                "inner"
            )
count_ambos = df_en_ambos.count()
print(f'Asegurados en ambos: {count_ambos}')
# Apply rule

from utils import Utils
utils_instance = Utils()
path = 's3://alfa-datalake-dev-clientes-assets/phase_bronze/RESERVA_MATEMATICA_ARL_Y_RENTAS_VITALICAS_202506.xlsx'
df_xl = utils_instance.read_excel_from_s3(path, "Datos")
df_rentas = spark.createDataFrame(df_xl)
# Spark Libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, coalesce, when, substring, 
    max as spark_max, sum as spark_sum, count,
    row_number, desc, concat, to_date, date_format, current_date,
    add_months, lpad
)
from pyspark.sql.types import DecimalType
from decimal import Decimal
from datetime import datetime
from pyspark.sql.window import Window
# Custom Libraries
from logger_config import logger
from utils import Utils

spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
df_rentas_filter = df_rentas.filter(col("Ramo") == "40")
df_rentas_filter = (df_rentas_filter
      .withColumnRenamed("Fec. Corte", "fecha_corte")
      .withColumnRenamed("Ramo", "ramo")
      .withColumnRenamed("No. Póliza", "poliza")
      .withColumnRenamed("No. Siniestro o renta", "siniestro")
      .withColumnRenamed("Total Reserva Estados Financieros", "valor_asegurado")
      .withColumnRenamed("Tipo Identificación", "tipo_documento")
      .withColumnRenamed("No. Identificación", "numero_documento")
)
 
df_rentas_final = df_rentas_filter.select(
col("fecha_corte"),
    col("ramo"),
    col("poliza"),
    col("siniestro"),
    col("valor_asegurado"),
    col("tipo_documento"),
    col("numero_documento")
)
df_rentas_final = df_rentas_final.withColumn(
    "periodo",
    date_format(col("fecha_corte"), "yyyyMM")
)
df_rentas_final.show(5, truncate=False)
# group by policies
df_rentas_final.groupBy("periodo").count().show()
PATH_POLIZAS = 's3://alfa-datalake-dev-silver/POLIZAS_PRODUCTO/ALFAPRD/POLIZAS_ALFAPRD_SILVER/'
df_polizas_prv = utils_instance.read_full_delta(spark, PATH_POLIZAS)
# Filter by periodo
df_polizas_rentas = df_polizas_prv.filter(col("origen") == "rentas")
df_polizas_rentas = df_polizas_rentas.withColumn(
    "periodo",
    concat(col("yyyy"), col("mm"))
)
df_polizas_rentas.select(col("periodo")).show(5)
df_polizas_rentas_silver = df_polizas_rentas.filter(col("periodo") == "202506")
df_polizas_rentas_silver.groupBy("periodo").count().show()
df_polizas_rentas_silver.printSchema()
# Cruce

df_polizas_faltantes = df_rentas_final.join(
                df_polizas_rentas_silver,
                df_rentas_final["poliza"] == df_polizas_rentas_silver["numero_poliza"],
                "left_anti"
 )
df_polizas_faltantes.count()
# Spark Libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, coalesce, when, substring, 
    max as spark_max, sum as spark_sum, count,
    row_number, desc, concat, to_date, date_format, current_date,
    add_months, lpad
)
from pyspark.sql.types import DecimalType
from decimal import Decimal
from datetime import datetime
from pyspark.sql.window import Window
# Custom Libraries
from logger_config import logger
from utils import Utils

spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
utils_instance = Utils()
BUCKET = 'alfa-datalake-dev-clientes-assets'
PREFIXES = ['valor_asegurado_vw_gold_siniestros',
            'valor_asegurado_vw_gold_siniestros_reserva',
            'valor_prima_vw_silver']
PATH_CLIENTES = 's3://alfa-datalake-dev-silver/POLIZAS_PRODUCTO/ALFAPRD/CLIENTES_ALFAPRD_SILVER/'
PATH_POLIZAS = 's3://alfa-datalake-dev-silver/POLIZAS_PRODUCTO/ALFAPRD/POLIZAS_ALFAPRD_SILVER/'
athena_output_locations = [
f's3://{BUCKET}/{PREFIXES[0]}',
f's3://{BUCKET}/{PREFIXES[1]}',
f's3://{BUCKET}/{PREFIXES[2]}',
]
df_clientes_prev = utils_instance.read_full_delta(spark, PATH_CLIENTES)
df_polizas_prv = utils_instance.read_full_delta(spark, PATH_POLIZAS)
# Filter Polizas - PRV
df_polizas_prv = df_polizas_prv.filter(col('origen') == "previsional")
count = df_polizas_prv.count()
logger.info(f'Polizas PREVISIONAL loaded: {count:,}')
def get_df_previsional_siniestros(spark, output_location: str) -> DataFrame:
   """
   Load Previsional siniestros data from Gold layer Athena view.
   Executes the query defined in query_previsional_polizas.sql against
   Gold layer tables that are not available in Bronze.
   Args:
       output_location (str): S3 path for Athena query results
   Returns:
       DataFrame: Previsional siniestros aggregated data
   """
   query = """
   SELECT
       T1.id_fuente_fk,
       T1.id_poliza_pk,
       T2.unidad_de_negocio,
       "Fecha de Transaccion",
       '1' AS "Tipo_Identificacion",
       CEDULA AS "Numero_Documento_Asegurado",
       T1.id_tipo_transaccion_fk,
       T3.tipo_de_transaccion,
       count(id_siniestro_fk) AS Nro_Siniestros,
       sum(valor_transaccion) AS valor_transaccion
   FROM "alfa-datalake-dev-gold"."gold_caract_siniestros_fact_movimiento_view" T1
   INNER JOIN "alfa-datalake-dev-gold"."gold_caract_siniestros_fact_poliza_pf" T2
       ON T1.id_poliza_pk = T2.id_poliza_pk
   INNER JOIN "alfa-datalake-dev-gold"."gold_caract_siniestros_dim_tipo_transaccion" T3
       ON CAST(T1.id_tipo_transaccion_fk AS INT) = T3.id_tipo_transaccion_pk
       AND T1.id_fuente_fk = '3'
   WHERE UPPER(T3.tipo_de_transaccion) = 'PAGO'
   GROUP BY "Fecha de Transaccion",
       T1.id_fuente_fk,
       T1.id_poliza_pk,
       T2.unidad_de_negocio,
       CEDULA,
       T1.id_tipo_transaccion_fk,
       T3.tipo_de_transaccion
   """
   logger.info("Loading Previsional siniestros from Gold layer", "AlfaprdStagingPolizas")
   df = utils_instance.execute_athena_query_to_dataframe(
       spark=spark,
       query=query,
       database="alfa-datalake-dev-gold",
       output_location=output_location
   )
   df = df.withColumn(
         "valor_transaccion",
         col("valor_transaccion").cast(DecimalType(18, 2))
       ) \
       .withColumn(
            "pk_client",
           concat(
                            col("id_poliza_pk"),
                            lit("-"),
                            col("Tipo_Identificacion"),
                            lit("-"),
                            col("Numero_Documento_Asegurado")
           ) )
   return df
def get_df_previsional_reserva_sn(spark, output_location: str) -> DataFrame:
   """
   Load Previsional siniestros data from Gold layer Athena view.
   Executes the query defined in query_previsional_polizas.sql against
   Gold layer tables that are not available in Bronze.
   Args:
       output_location (str): S3 path for Athena query results
   Returns:
       DataFrame: Previsional siniestros aggregated data
   """
   query = """
   WITH query_reserva AS (
    SELECT
        ano,
        mes,
        estado,
        SUM(monto) AS sum_monto,
        id_siniestro_fk
    FROM "alfa-datalake-dev-gold"."gold_caract_siniestros_fact_estado_siniestro"
    WHERE id_fuente_fk = '3'
    AND estado = 'En Reserva'
    GROUP BY ano, mes, estado, id_siniestro_fk
    )
    SELECT T1.sum_monto,
        T1.id_siniestro_fk,
        '1' AS "tipo_identificacion_asegurado",
        T2.cedula AS "numero_documento_asegurado"
    FROM query_reserva T1
    INNER JOIN "alfa-datalake-dev-gold"."gold_caract_siniestros_fact_siniestros_pf" T2
    ON T1.id_siniestro_fk = T2.id_siniestro_pk
   """
   logger.info("Loading Previsional siniestros from Gold layer", "AlfaprdStagingPolizas")
   df = utils_instance.execute_athena_query_to_dataframe(
       spark=spark,
       query=query,
       database="alfa-datalake-dev-gold",
       output_location=output_location
   )
   df = df.withColumn(
         "valor_transaccion",
         col("sum_monto").cast(DecimalType(18, 2))
       ) \
       .withColumn(
            "pk_client",
           concat(
                            col("id_siniestro_fk"),
                            lit("-"),
                            col("tipo_identificacion_asegurado"),
                            lit("-"),
                            col("numero_documento_asegurado")
           ) )
   return df
df_previsional_siniestros = get_df_previsional_siniestros(spark, athena_output_locations[0])
df_previsional_reservas_sn = get_df_previsional_reserva_sn(spark, athena_output_locations[1])
df_siniestros_pagos = df_previsional_siniestros \
                .withColumnRenamed("pk_client", "pk_pagos") \
                .withColumnRenamed("valor_transaccion", "valor_pagos") \
                .withColumnRenamed("Nro_Siniestros", "nro_siniestros_pagos")
df_with_pagos = df_polizas_prv.join(
                df_siniestros_pagos,
                df_polizas_prv["numero_poliza"] == df_siniestros_pagos["pk_pagos"],
                "inner"
            )
count = df_with_pagos.count()
logger.info(f'Join PAGOS | df_polizas.numero_poliza =  df_siniestros_pagos.pk_pagos loaded: {count:,}')
df_reserva = df_previsional_reservas_sn \
                .withColumnRenamed("pk_client", "pk_reserva") \
                .withColumnRenamed("valor_transaccion", "valor_reserva")
df_reserva.show(5, truncate=False)
df_previsional_siniestros
df_previsional_reservas_sn
df_polizas_prv
job.commit()