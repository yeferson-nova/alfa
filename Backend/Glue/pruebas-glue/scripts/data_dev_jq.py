import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import openpyxl
import traceback
import boto3
import pandas as pd
import re
import unicodedata

from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def search_in_s3(v_bucket, v_prefix, v_delimiter="empty"):
    """
    This function hel us to find forder o files in s3 path
    """
    if v_delimiter != "empty":
        response = s3_client.list_objects_v2(
            Bucket = v_bucket,
            Prefix = v_prefix,
            Delimiter = v_delimiter
        )
    else:
        response = s3_client.list_objects_v2(
            Bucket = v_bucket,
            Prefix = v_prefix
        )
        
    return response

def load_csv(path, delimiter=","):
    """
    Load CSV file from S3
    """
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "UTF-8")
        .option("delimiter", delimiter)
        .csv(path)
    )

def load_txt(path, delimiter="|"):
    """
    Load TXT file from S3
    """
    return (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("encoding", "UTF-8")
            .option("delimiter", delimiter)
            .csv(path)    
        )


def load_xlsx(path):
    """
    Load XLSX file from S3
    """
    df_pandas = pd.read_excel(
        path,
        engine="openpyxl",
        dtype=str
        )
    df_pandas = df_pandas.fillna("")
    return spark.createDataFrame(df_pandas)

def save_df(df, path):
    """
    Function to save df in parquet format on s3
    """
    df = df.toDF(*[
            clean_col(col)
            for col in df.columns
        ]
    )
    df.write.mode("overwrite").parquet(path)

def clean_col(name):
    """
    Funtion to eliminate special character from column name.
    """
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = re.sub(r"[^a-zA-Z0-9]", "_", name)
    name = name.strip("_")
    
    return name.lower()
    
try:
    args = getResolvedOptions(
        sys.argv, 
            [
            'BUCKET_NAME',
            'SOURCE_PATH'
        ]
    )
    BUCKET_NAME = args['BUCKET_NAME']
    SOURCE_PATH = args['SOURCE_PATH']
    
    s3_client = boto3.client("s3")
    
    fecha_hoy = datetime.now().strftime("%Y-%m-%d")
    
    print(fecha_hoy)
    # ---------------- List Files ----------------- #
    folder_path = SOURCE_PATH #+"/"+fecha_hoy+"/"
    
    response = search_in_s3(v_bucket=BUCKET_NAME, v_prefix=folder_path, v_delimiter="/")
    
    available_files = {}
    
    for folder in response.get("CommonPrefixes", []):
        response_folder = search_in_s3(BUCKET_NAME, folder['Prefix'])
        for obj in response_folder.get("Contents", []):
            if "." in obj["Key"]:
                file_name = obj["Key"].split("/")[-1]
                ext_file = file_name.split(".")[-1]
                txt = folder['Prefix'].replace(folder_path,"")
                print(folder['Prefix']+file_name)
                print(txt.split("/")[0])
                print(file_name)
                print(ext_file)
                if ext_file == 'csv':
                    df = load_csv("s3://"+BUCKET_NAME+"/"+folder['Prefix']+file_name, ",")
                    ##save_path = "s3://" + BUCKET_NAME + "/output/" + txt.split("/")[0] + "/" + fecha_hoy
                    file_base = file_name.rsplit(".", 1)[0]
                    save_path = (
                        f"s3://{BUCKET_NAME}/data_dev/output/"
                        f"{txt.split('/')[0]}/"
                        f"{fecha_hoy}/"
                        f"{file_base}"
                    )
                    print("Filas:", df.count())
                    print("Columnas:", len(df.columns))
                    save_df(df, save_path)
                    
                if ext_file == 'xlsx':
                    df = load_xlsx("s3://"+BUCKET_NAME+"/"+folder['Prefix']+file_name)
                    ##save_path = "s3://" + BUCKET_NAME + "/output/" + txt.split("/")[0] + "/" + fecha_hoy
                    file_base = file_name.rsplit(".", 1)[0]
                    save_path = (
                        f"s3://{BUCKET_NAME}/data_dev/output/"
                        f"{txt.split('/')[0]}/"
                        f"{fecha_hoy}/"
                        f"{file_base}"
                    )
                    print("Filas:", df.count())
                    print("Columnas:", len(df.columns))
                    save_df(df, save_path)
                    
    # ---------------- ---------- ----------------- #
except Exception as e:
        print(traceback.format_exc())
        raise e
job.commit()