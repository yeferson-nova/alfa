

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from commons.ExtractHelper import ExtractHelper
from pyspark.sql import functions as F

import boto3
import datetime

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")
job = Job(glueContext)
ExtractHelper = ExtractHelper(glueContext, spark, "-", "-")
from commons.StepGold import StepGold
from commons.IncrementalCharge import IncrementalCharge
from pyspark.sql.types import IntegerType
import sys
import io
 
IncrementalCharge = IncrementalCharge(glueContext, spark, "-", "-")
StepGold = StepGold(glueContext, spark)
path = f"s3://alfa-datalake-dev-assets/support_phase_files/procces_ola1.xlsx"
sheet = "gold_phase"
df_tables = ExtractHelper.extract_excel_data(path, sheet)
sheet = "gold_tables"
df_tmp_tables = ExtractHelper.extract_excel_data(path, sheet)
 
for row in df_tmp_tables.collect():
    table = row["Table"].upper()
    print(table)
    if row["Phase"].upper() == "SILVER":
        path_source =f"s3://alfa-datalake-dev-silver/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}_SILVER_"
    else:
        path_source =f"s3://alfa-datalake-dev-gold/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-CARACT_SINIESTROS-"
    IncrementalCharge.charge_tmp_tables(path_source, table, 'Gold')
import nltk
nltk.download('stopwords', quiet=True)
#nltk.download('punkt', quiet=True)
from nltk.corpus import stopwords
#from pyspark.sql.functions import stopwords
#from pyspark.sql.types import udf, col, ArrayType, StringType
from nltk.tokenize import word_tokenize
stop_words = set(stopwords.words('spanish'))
print(stop_words)
CR = spark.table("global_temp.rsv_comentario_reserva")
CR.select("ID_COMENTARIO_RESERVA","comentario_reserva").orderBy("ID_COMENTARIO_RESERVA").show(truncate=False)
_CR = CR
def remove_stopwords(text):
    if text is None:
        return None
    #words = word_tokenize(text.lower())
    words = text.lower().split()
    filter_words = [p for p in words if p.isalpha() and p not in stop_words]
    return ' '.join(filter_words)
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
delete_udf = udf(remove_stopwords, StringType())
_CR = _CR.withColumn('comentario_reserva', delete_udf('comentario_reserva'))
_CR.select("ID_COMENTARIO_RESERVA","comentario_reserva").orderBy("ID_COMENTARIO_RESERVA").show(truncate=False)
job.commit()