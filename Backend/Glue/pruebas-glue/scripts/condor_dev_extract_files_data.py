import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled","true")


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

def load_csv(path):
    """
    Load CSV file from S3
    """
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "UTF-8")
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
    df.coalesce(1).write.mode("overwrite").parquet(path)

def clean_col(name):
    """
    Funtion to eliminate special character from column name.
    """
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = re.sub(r"[^a-zA-Z0-9]", "_", name)
    name = name.strip("_")
    
    return name.lower()
    
import boto3

def agregar_path_a_crawler(crawler_name, nuevo_s3_path, region_name='us-east-1'):
    client = boto3.client('glue', region_name=region_name)
    
    # 1. Obtener la configuración actual del crawler
    response = client.get_crawler(Name=crawler_name)
    crawler = response['Crawler']
    
    # 2. Extraer los targets de S3 actuales
    targets = crawler.get('Targets', {})
    s3_targets = targets.get('S3Targets', [])
    
    # 3. Verificar si el path ya está registrado
    paths_existentes = [t['Path'] for t in s3_targets]
    if nuevo_s3_path in paths_existentes:
        print(f"El path '{nuevo_s3_path}' ya se encuentra configurado en el crawler '{crawler_name}'.")
        return
    
    # 4. Agregar el nuevo path a la lista
    s3_targets.append({'Path': nuevo_s3_path})
    targets['S3Targets'] = s3_targets
    
    # 5. Actualizar el crawler conservando los demás tipos de targets si existen
    client.update_crawler(
        Name=crawler_name,
        Targets=targets
    )
    print(f"Path '{nuevo_s3_path}' agregado exitosamente al crawler '{crawler_name}'.")

# Ejemplo de uso:
# agregar_path_a_crawler('mi-crawler-de-datos', 's3://mi-bucket/nueva-ruta/')

try:
    args = getResolvedOptions(
        sys.argv, 
            [
            'BUCKET_NAME',
            'SOURCE_PATH',
            'BUCKET_NAME_OUT',
            'SAVE_PATH',
            'CRAWLER_DB'
        ]
    )
    BUCKET_NAME = args['BUCKET_NAME']
    BUCKET_NAME_OUT = args['BUCKET_NAME_OUT']
    SOURCE_PATH = args['SOURCE_PATH']
    SAVE_PATH = args['SAVE_PATH']
    CRAWLER_DB = args['CRAWLER_DB']
    
    s3_client = boto3.client("s3")
    
    fecha_hoy = datetime.now().strftime("%Y-%m-%d")
    
    print(fecha_hoy)
    # ---------------- List Files ----------------- #
    folder_path = SOURCE_PATH+"/"+fecha_hoy+"/"
    
    response = search_in_s3(v_bucket=BUCKET_NAME, v_prefix=folder_path, v_delimiter="/")
    print(f"ruta:{BUCKET_NAME}: {folder_path}")
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
                if ext_file == 'txt':
                    df = load_txt("s3://"+BUCKET_NAME+"/"+folder['Prefix']+file_name, ";")
                    save_path = "s3://" + BUCKET_NAME_OUT + "/"+SAVE_PATH+"/" + txt.split("/")[0] + "/" + fecha_hoy
                    save_df(df, save_path)
                    crawler_path = "s3://" + BUCKET_NAME_OUT + "/"+SAVE_PATH+"/" + txt.split("/")[0] + "/"
                    agregar_path_a_crawler(CRAWLER_DB, crawler_path)
                    
                if ext_file == 'xlsx':
                    df = load_xlsx("s3://"+BUCKET_NAME+"/"+folder['Prefix']+file_name)
                    save_path = "s3://" + BUCKET_NAME_OUT + "/"+SAVE_PATH+"/" + txt.split("/")[0] + "/" + fecha_hoy
                    save_df(df, save_path)
                    crawler_path = "s3://" + BUCKET_NAME_OUT + "/"+SAVE_PATH+"/" + txt.split("/")[0] + "/"
                    agregar_path_a_crawler(CRAWLER_DB, crawler_path)
                
                if ext_file == 'csv':
                    df = load_csv("s3://"+BUCKET_NAME+"/"+folder['Prefix']+file_name)
                    save_path = "s3://" + BUCKET_NAME_OUT + "/"+SAVE_PATH+"/" + txt.split("/")[0] + "/" + fecha_hoy
                    save_df(df, save_path)
                    crawler_path = "s3://" + BUCKET_NAME_OUT + "/"+SAVE_PATH+"/" + txt.split("/")[0] + "/"
                    agregar_path_a_crawler(CRAWLER_DB, crawler_path)
                    
    # ---------------- ---------- ----------------- #
except Exception as e:
        print(traceback.format_exc())
        raise e
job.commit()