"""Script that load bronce phase"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.ExtractHelper import ExtractHelper
from commons.IncrementalCharge import IncrementalCharge
from commons.TransformationHelper import TransformationHelper
from commons.GoldTransformation import GoldTransformation
from commons.FechaObjHelper import FechaObjHelper
from commons.ClaimHelper import ClaimHelper
from commons.DocumentoHelper import DocumentoHelper
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import compt3.src.utils.common.df_method as df_mtd
from step_gold.orchestrator_gold import main
from pyspark.sql.window import Window
import datetime
import traceback
import boto3
import time

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','ANIO_PARAM','MES_PARAM','BUCKET_NAME_BRONCE'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")
spark.conf.set("spark.sql.files.ignoreCorruptFiles","true")
spark.conf.set("spark.hadoop.fs.s3a.experimental.input.fadvise","sequential")

s3_client = boto3.client("s3")

TransformationHelper = TransformationHelper()
ExtractHelper = ExtractHelper(glueContext, spark, "", "")
IncrementalCharge = IncrementalCharge(glueContext, spark, "", "")
GoldTransformation = GoldTransformation(glueContext, spark, 'None')
FechaObj_helper = FechaObjHelper(glueContext, spark)
ClaimHelper = ClaimHelper(glueContext, spark)
DocumentoHelper = DocumentoHelper(glueContext, spark)

BUCKET_NAME_BRONCE = args['BUCKET_NAME_BRONCE']
ANIO_PARAM = args['ANIO_PARAM']
MES_PARAM = args['MES_PARAM']

path_source = f"s3://{BUCKET_NAME_BRONCE}/POINT/FILES/POINT-FILES-"#revisar path
path_source = f"s3://alfa-datalake-dev-silver/POINT/TRANSFORMATION/POINT_SILVER_"

#tables = ["CIERRESGENERALES","CIERREVIDA"]
tables = ["CIERREVIDA"]

for table in tables:
    
    print(f"tabla ha ejecutar {table}")
    
    df_data_modify = IncrementalCharge.charge_tmp_tables_with_partition(path_source, table, 'Gold')
    
    df_modify_date = df_data_modify.withColumn(
        "FECHA_TRANSACCION",
        F.to_date("FECHA_TRANSACCION", "d/M/yyyy")
    )
    
    df_unmodify = df_modify_date.filter(
        (F.col("FECHA_TRANSACCION").isNull())|(~((F.year("FECHA_TRANSACCION") == ANIO_PARAM) & (F.month("FECHA_TRANSACCION") == MES_PARAM)))
    )
    #df_unmodify.count()
    df_modify = df_modify_date.filter(
        ((F.year("FECHA_TRANSACCION") == ANIO_PARAM) & (F.month("FECHA_TRANSACCION") == MES_PARAM))
    )
    #df_modify.count()
    df_unmodify.cache()
    
    partition_array_modify = df_modify.select("partition_date").distinct().collect()
    partition_array_unmodify = df_unmodify.select("partition_date").distinct().collect()
    for i in partition_array_modify:
        IncrementalCharge.delete_data(path_source, table, i["partition_date"], BUCKET_NAME_BRONCE)
    for i in partition_array_unmodify:
        IncrementalCharge.save_files_modify(df_unmodify, path_source, f"{table}", i["partition_date"])
        
    

job.commit()