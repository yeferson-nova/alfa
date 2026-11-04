
from pyspark.sql import functions as F
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
from commons.LogsHelper import LogsHelper

logs = LogsHelper()
logger = logs.logger_conf()


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


try:
    #df = spark.table("alfa-datalake-dev-gold.gold_caract_clientes_dim_dim_ciudad")
    df = spark.table("`alfa-datalake-dev-gold`.`gold_caract_clientes_dim_dim_ciudad`")
    
    exprs = [F.count("*").alias("total_rows")]
    
    for c in df.columns:
        exprs.append(F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"{c}_nulls"))
        exprs.append(F.sum(F.when(F.col(c).isNotNull(), 1).otherwise(0)).alias(f"{c}_not_nulls"))
    
    df.select(exprs).show(truncate=False)
except Exception as e:
    logger.error(f"ERROR WITH Evaluate error ATRIBUTO: {traceback.format_exc()}")  