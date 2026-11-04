

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
import pandas as pd
from pydantic_settings import BaseSettings
from pandas_profiling import ProfileReport
import boto3, datetime, json
from io import BytesIO
from commons.ExtractHelper import ExtractHelper
import warnings

s3_client = boto3.client("s3")

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

ExtractHelper = ExtractHelper(glueContext, spark, "-", "-")


spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")


pip install pydeequ



INPUT_S3_PATH = "s3://alfa-datalake-dev-bronce/ACSELE/ACSELE_ALFA/POLICYDCO/"
OUTPUT_S3_PATH = "s3://alfa-bucket-dev-wi/parquet/"

print(f"Cargando datos desde: {INPUT_S3_PATH}")
print(f"Guardando resultados en: {OUTPUT_S3_PATH}")

try:
    df = spark.read.parquet(INPUT_S3_PATH)
except Exception as e:
    print(f"Error al cargar el archivo Parquet: {e}")
    sys.exit(1)

# Mostrar el esquema y algunas filas para verificación
print("Esquema del DataFrame:")
df.printSchema()
df.count()



print("Iniciando el perfilamiento de datos con PyDeequ...")

profiler_result = ColumnProfilerRunner(spark) \
    .onData(df) \
    .run()

job.commit()