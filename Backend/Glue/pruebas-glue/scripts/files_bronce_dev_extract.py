import sys
import logging
import pandas as pd
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from commons.LogsHelper import LogsHelper

import sys, traceback, boto3, json

###################################################################
######## SET ARGS AND CONFIG COMMONS UTILS ########################
###################################################################

logs = LogsHelper()
logger = logs.logger_conf()

args = getResolvedOptions(
    sys.argv, 
        [
        'JOB_NAME',
        'DB_SCHEMA',
        'DB_NAME',
        'BUCKET_NAME',
        'BUCKET_NAME_ASSETS',
        'BUCKET_NAME_LANDING',
        'CONNECT_NAME', 
        'PERIOD',
        'CERTIFICATE',
        'PATH_JSON'
        ]
    )
DB_SCHEMA = args['DB_SCHEMA']
DB_NAME = args['DB_NAME']
BUCKET_NAME = args['BUCKET_NAME']
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
BUCKET_NAME_LANDING = args['BUCKET_NAME_LANDING']
CONNECT_NAME = args['CONNECT_NAME']
PERIOD = args['PERIOD']
CERTIFICATE = args['CERTIFICATE']
PATH_JSON = args['PATH_JSON']

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3_client = boto3.client("s3")

###################################################################
############## VALIDATE FILE IN S3 & READ JSON ####################
###################################################################


try:
    # Check if the file exists before attempting to read it
    s3_client.head_object(Bucket=BUCKET_NAME_ASSETS, Key=PATH_JSON)
    logger.info(f"The file {PATH_JSON} exists in S3. Proceeding with the reading.")

    # If the file exists, proceed with the reading
    response = s3_client.get_object(Bucket=BUCKET_NAME_ASSETS, Key=PATH_JSON)
    catalog_files = json.loads(response["Body"].read().decode("utf-8"))   # .read() is used here
    logger.info("JSON file successfully loaded.")

except s3_client.exceptions.NoSuchKey:
    logger.error(f"ERROR: The file {PATH_JSON} does not exist in the bucket {BUCKET_NAME_ASSETS}.")
    sys.exit(1)  # Stop execution if the file does not exist

except Exception as e:
    logger.error(f"Unexpected error accessing the JSON in S3: {traceback.format_exc()}")
    sys.exit(1)
    
###################################################################
####################### ITERATE FILES #############################
###################################################################

# Iterate over the files in the JSON catalog
for file_info in catalog_files:

    try:
        
        filename = file_info["filename"]
        db = file_info["db"]
        schema = file_info["schema"]
        table = file_info["table"]

        # Build input and output paths
        s3_key = f'NIIF_{db}/{filename}'
        input_path = f's3://{BUCKET_NAME_LANDING}/{s3_key}'
        output_path = f's3://{BUCKET_NAME}/{db}/{schema}/{table}/'
        

        logger.info(f'Read file from {input_path}')
    
        # Read the Excel file with pandas
        df_pandas = pd.read_excel(input_path)
    
        # Convert all columns to string in pandas to avoid type conflicts
        df_pandas = df_pandas.astype(str)
    
        # Convert the pandas DataFrame to a Spark DataFrame
        df_spark = spark.createDataFrame(df_pandas)
    
        logger.info('File successfully read with pandas and converted to Spark')
    
        # Show the schema of the DataFrame
        df_spark.printSchema()
    
        # Save the DataFrame in Parquet format to the output path
        logger.info(f'Saving the file in Parquet format to {output_path}')
        df_spark.write.mode("overwrite").parquet(output_path)
        logger.info('File successfully saved in Parquet format')
    
    except Exception as e:
        logger.error(f'Error process the file: {str(e)}')
        continue

job.commit()
