
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
--aditional-python
import pandas as pd
fields = {
                'NUMERO_SINIESTRO':str, 'IdMun':str
            }
df = pd.read_excel("s3://alfa-datalake-dev-assets/valores_guia/valores_guia.xlsx",sheet_name = 'X',
                                  dtype=fields
                                  )
df_spark = spark.createDataFrame(df)
df_spark.repartition(2).write.mode("overwrite").parquet("s3://alfa-datalake-dev-assets/valores_guia/data/")
job.commit()