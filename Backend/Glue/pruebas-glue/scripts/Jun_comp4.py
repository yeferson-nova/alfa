

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from commons.ExtractHelper import ExtractHelper
from pyspark.sql import functions as F
from pyspark.sql.functions import col

import boto3
import datetime

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")
job = Job(glueContext)
ExtractHelper = ExtractHelper(glueContext, spark, "-", "-")
print(10 * "*" + "Librerías Cargadas Correctamente" + "*" * 10)
from commons.IncrementalCharge import IncrementalCharge
IncrementalCharge = IncrementalCharge(glueContext, spark, "", "")
path_source = f"s3://alfa-datalake-dev-bronce/POINT/FILES/POINT-FILES-"#revisar path
table_in_proccess = f"CIERRESGENERALES"
df_data_modify = IncrementalCharge.charge_tmp_tables_with_partition(path_source, table_in_proccess, 'Gold')
df_data_modify.select("FECHA_TRANSACCION").distinct().show()
df_modify_date = df_data_modify.withColumn(
    "FECHA_TRANSACCION",
    F.to_date("FECHA_TRANSACCION", "d/M/yyyy")
)
df_data_modify.filter(~F.isnan(F.col("FECHA_TRANSACCION"))).select("FECHA_TRANSACCION").show(2)
df_data_modify.filter(F.year("FECHA_TRANSACCION")==2021).select(F.month("FECHA_TRANSACCION")).distinct().show()##VALIDACION
df_data_modify.count()
anio = 2021
mes = 4
df_unmodify = df_modify_date.filter(
    (F.col("FECHA_TRANSACCION").isNull())|(~((F.year("FECHA_TRANSACCION") == anio) & (F.month("FECHA_TRANSACCION") == mes)))
)
df_unmodify.count()
df_modify = df_modify_date.filter(
    ((F.year("FECHA_TRANSACCION") == anio) & (F.month("FECHA_TRANSACCION") == mes))
)
df_modify.count()
df.filter(F.col("FECHA_TRANSACCION").isNull()).count()
df_data_modify.count()
df.count()
df_unmodify.cache()
partition_array_modify = df_modify.select("partition_date").distinct().collect()
partition_array_unmodify = df_unmodify.select("partition_date").distinct().collect()
BUCKET_NAME_BRONCE = "alfa-datalake-dev-bronce"
path_source = f"s3://{BUCKET_NAME_BRONCE}/POINT/FILES/POINT-FILES-"#revisar path
table = f"CIERRESGENERALES"
for i in partition_array_modify:
    IncrementalCharge.delete_data(path_source, table, i["partition_date"], BUCKET_NAME_BRONCE)
for i in partition_array_unmodify:
    IncrementalCharge.save_files_modify(df_unmodify, path_source, f"{table}", i["partition_date"])
job.commit()