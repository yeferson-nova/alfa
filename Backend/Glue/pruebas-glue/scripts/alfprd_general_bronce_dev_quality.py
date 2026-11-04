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
        'DB_CATALOG_GLUE',
        'BUCKET_NAME',
        'BUCKET_NAME_ASSETS',
        'CONNECT_NAME', 
        'PERIOD',
        'CERTIFICATE'
        ]
    )
DB_SCHEMA = args['DB_SCHEMA']
DB_NAME = args['DB_NAME']
DB_CATALOG_GLUE = args['DB_CATALOG_GLUE']
BUCKET_NAME = args['BUCKET_NAME']
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
CONNECT_NAME = args['CONNECT_NAME']
PERIOD = args['PERIOD']
CERTIFICATE = args['CERTIFICATE']

PATH_JSON = f"support_phase_files/niif/{DB_NAME.lower()}_{DB_SCHEMA.lower()}_rules_tables.json"


logger.info(f"Period: {PERIOD}")
logger.info(f"Period: {DB_CATALOG_GLUE}")
logger.info(f"Schema: {DB_SCHEMA}")
logger.info(f"Connect: {CONNECT_NAME}")
logger.info(f"Bucket_name: {BUCKET_NAME}")
logger.info(f"Json_file: s3://{BUCKET_NAME_ASSETS}/{PATH_JSON}")

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

###################################################################
############## VALIDATE FILE IN S3 & READ JSON ####################
###################################################################
 
try:
    # Check if the file exists before attempting to read it
    s3_client.head_object(Bucket=BUCKET_NAME_ASSETS, Key=PATH_JSON)
    logger.info(f"The file {PATH_JSON} exists in S3. Proceeding with the reading.")

    # If the file exists, proceed with the reading
    response = s3_client.get_object(Bucket=BUCKET_NAME_ASSETS, Key=PATH_JSON)
    catalog_tables = json.loads(response["Body"].read().decode("utf-8"))  # Se usa .read()
    logger.info("JSON file successfully loaded.")

except s3_client.exceptions.NoSuchKey:
    logger.error(f"ERROR: The file {PATH_JSON} does not exist in the bucket {BUCKET_NAME_ASSETS}.")
    sys.exit(1)  # Stop execution if the file does not exist

except Exception as e:
    logger.error(f"Unexpected error accessing the JSON in S3: {traceback.format_exc()}")
    sys.exit(1)

###################################################################
################# ITERATE FOR EACH TABLE ##########################
###################################################################



# Dictionary to store EvaluateDataQualityMultiframe for each table


for schema in catalog_tables:
    if schema in DB_SCHEMA:
        for table_info in catalog_tables[schema]:
                if table_info['status']:
                    logger.info(f"Tables for ingest: {schema}.{table_info['table']}")
                    try:
                        
                        frame_to_evaluate = glueContext.create_dynamic_frame.from_catalog(f"{DB_CATALOG_GLUE}", table_name=f"{DB_NAME}_{schema}_{table_info['table']}")
                        
                       
                        # Construir dinámicamente el conjunto de reglas
                        rules_str = "\n".join(table_info['rules'])
                        
                        EvaluateDataQuality_ruleset = f"""
                            Rules = [
                                {rules_str}
                            ]
                        """ 

                        
                        EvaluateDataQualityMultiframe= EvaluateDataQuality().process_rows(
                            frame=frame_to_evaluate,
                            ruleset=EvaluateDataQuality_ruleset,
                            publishing_options={
                                "dataQualityEvaluationContext": f"DataQuality_{table_info['table']}",
                                "enableDataQualityMetrics": True,
                                "enableDataQualityResultsPublishing": True,
                            },
                            additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
                        )      
                        
                        logger.info(EvaluateDataQualityMultiframe)
                        
                        ruleOutcomes = SelectFromCollection.apply(
                            dfc=EvaluateDataQualityMultiframe,
                            key="ruleOutcomes",
                            transformation_ctx="ruleOutcomes",
                        )
                        
                        logger.info("Successfully Evaluate Data Quality ")
                    except Exception as e:
                        logger.info(f"ERROR WITH Evaluate Data Quality: {e}")
    else:
        logger.info("NOT TABLE FOR Evaluate Data Quality")
        
        