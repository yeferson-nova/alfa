import sys
import traceback
import pandas as pd
from awsglue.utils import getResolvedOptions
import compt3.src.utils.common.df_method as df_mtd
#from commons.UrincterrdeudoresHelper import UrincterrdeudoresHelper
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from commons.FechaObjHelper import FechaObjHelper
from awsglue import DynamicFrame
from datetime import datetime
from commons.ExtractHelper import ExtractHelper
from commons.ClaimHelper import ClaimHelper

from commons.EstadoSiniestroHelperA import EstadoSiniestroHelperA
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField


from pyspark.sql import functions as F
from pyspark.sql.functions import lower,when
from step_gold.orchestrator_gold import main
from compt3.orchestrator.orchestrator_gold_dane import orchestrator_gold_dane
import re
from pyspark.sql.window import Window

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME_SILVER', 'BUCKET_NAME_GOLD','NM_BUCKET_ASSETS','DATE'])


NM_BUCKET_ASSETS = args['NM_BUCKET_ASSETS']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
BUCKET_NAME_SILVER = args['BUCKET_NAME_SILVER']
BUCKET_NAME_GOLD = args['BUCKET_NAME_GOLD']
DATE = args['DATE']

ExtractHelper = ExtractHelper(glueContext, spark, "-", "-")
#EstadoSiniestroHelperA = EstadoSiniestroHelperA(glueContext, spark)
#Urincterrdeudores_helper = UrincterrdeudoresHelper(glueContext, spark)
#ClaimHelper = ClaimHelper(glueContext, spark)
FechaObj_helper = FechaObjHelper(glueContext, spark)

path = f"s3://{NM_BUCKET_ASSETS}/support_phase_files/componente5/procces_ola0.xlsx"
sheet = "gold_phase"
df_tables = ExtractHelper.extract_excel_data(path, sheet)
sheet = "gold_tables"
df_tmp_tables = ExtractHelper.extract_excel_data(path, sheet)

if DATE == 'None':
    current_date = datetime.now()
    day = current_date.day
    month = current_date.month
    year = current_date.year 
else:
    year = DATE.split("-")[0]
    month = DATE.split("-")[1]
    day = DATE.split("-")[2]

def parse_schema(schema_str):
    fields = re.split(r',\s*(?=(?:[^()]*\([^()]*\))*[^()]*$)', schema_str)
    schema = []
    type_mapping = {
        "STRING": StringType,
        "INTEGER": IntegerType,
        "BOOLEAN": BooleanType,
        "FLOAT": FloatType,
        "DOUBLE": DoubleType
    }

    for field in fields:
        field = field.strip()
        if ":" not in field:
            print(f"Formato inválido en el esquema: {field}")
            continue
        field_name, field_type = map(str.strip, field.split(":", 1))
        field_type = field_type.upper()  

        try:
            if field_type.startswith("DECIMAL"):
                precision, scale = map(int, re.findall(r'\d+', field_type))
                schema.append(StructField(field_name, DecimalType(precision, scale), True))
            elif field_type in type_mapping:
                schema.append(StructField(field_name, type_mapping[field_type](), True))
            else:
                print(f"Tipo de dato desconocido para el campo {field_name}: {field_type}")
        except Exception as e:
            print(f"Error al procesar el esquema para el campo {field}: {e}")

    return StructType(schema)


def charge_tmp_tables(path, table)->None:
    fields = "*"
    table = table.upper()
    path = f"{path}{table}/"
    df = extract_parquet_data_gold(path, fields)
    df.createOrReplaceGlobalTempView(table)
    print(table)
    
def extract_parquet_data_gold(path, fields):
    """
    Function to extract data from parquets to df

    arg:
        path: The ubication of patquet files.
        fields: Fields that we need return by the function.
    return:
        df: Return df with the data of parquet files.
    """
    parquet_data = glueContext.create_dynamic_frame.from_options(
        format_options={},
        connection_type="s3",
        format="parquet",
        connection_options=
        {
            "paths": 
                [f"{path}{year}/{month}/{day}"],
            "recurse": True
        },
        transformation_ctx="AmazonS3"
    )
    df = parquet_data.toDF()
    return df

for row in df_tmp_tables.collect():
        table = row["Table"].upper()
        path_source =f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}_SILVER_"
        print(path_source+table)
        charge_tmp_tables(path_source, table)
        print(f"tabla cargada{table}-ruta-{path_source+table}")

test = 1
if test == 1:
    for row in df_tables.collect():
        if row['Status'] == '1':
            sql = row['Logic']
            df = spark.sql(
                sql.upper()
            )
            table = row['Table'].upper()
            #df_causa = EstadoSiniestroHelperA.create_estado_siniestro(df) 
            path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-{table}/"
            ExtractHelper.save_files(df,path)
    
    
    