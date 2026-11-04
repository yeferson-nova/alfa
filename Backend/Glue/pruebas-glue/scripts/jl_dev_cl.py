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
import pandas as pd
from commons.ExtractHelper import ExtractHelper

ExtractHelper = ExtractHelper(glueContext, spark, "TIERRA", "alfa-datalake-dev-bronce")
path = "s3://alfa-datalake-dev-assets/data_siniestros_julio/"
sheet="Hoja1"
#data = pd.read_excel(ruta, engine = "openpyxl", sheet_name = "Hoja1")
from DayliCommons import DayliCommons
df = ExtractHelper.extract_excel_data_point(path="s3://alfa-datalake-dev-assets/data_siniestros_julio/Reservas por siniestro a julio 2022.xlsx",sheet="Hoja1")
df.count()
df.coalesce(1).write.mode("overwrite").parquet("s3://alfa-datalake-dev-assets/data_siniestros_julio/")
dayli_commons = DayliCommons(
    glueContext,
    spark)
date = dayli_commons.day_list('01012025','01022025')
print(date)
query = """WITH CONTEXTOP /*+MATERIALIZE*/ AS (
    SELECT id, TIME_STAMP FROM ACSELE_ALFA.CONTEXTOPERATION
                    WHERE TIME_STAMP == TO_DATE(REPLACE,'DD/MM/YYYY')
                    and status = 2
                    order by 1
)
select /*+PARALLEL(3)*/
COB.*, COR.TIME_STAMP AS TIME_STAMP_COR
from ACSELE_ALFA.COBERTURA COB
inner join ACSELE_ALFA.COVERAGEDCO CDCO
    on COB.PK = CDCO.DCOID
INNER JOIN CONTEXTOP COR
     ON CDCO.OPERATIONPK = COR.ID;"""
query = query.replace("REPLACE", '1/1/2025')
print(query)
partition_column = 'TIME_STAMP_COR'
table_name = 'COBERTURA'
import sys
from datetime import datetime
from extract_helper import ExtractHelper
extract_helper = ExtractHelper(
    glueContext,
    spark,
    'alfa-datalake-dev-clientes-assets',
    'alfa-test-data-origin-v1',
    'COREPROD',
    'ACSELE_ALFA',
    'datalake-dev-acsele-dev-connection',
    's3://alfa-datalake-dev-clientes-assets/config/table_definition_extraction_clients.xlsx',
    's3://alfa-datalake-dev-clientes-assets/phase_bronze/definitions/clients_table_definitions'
)
print(f"Delta Extensions: {spark.conf.get('spark.sql.extensions')}")
df_result = extract_helper.read_from_database(
                        table_name=table_name,
                        partition_column=TIME_STAMP_COR if partition_column != 'NA' else '',
                        sql_script=query,
                        connectname='datalake-dev-acsele-dev-connection',
                        primary_key_column='PK',
                        table_info=table_info
                    )
job.commit()