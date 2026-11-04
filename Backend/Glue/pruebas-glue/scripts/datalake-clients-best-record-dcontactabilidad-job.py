#------ AWS GLUE ---------#
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
#from awsglue import DynamicFrame
from awsglue.job import Job
#-------SPARK--------------#
from pyspark.context import SparkContext
#-----CUSTOM IMPORTS -----------#
from concordance_matrix_orchestrator import ConcordanceMatrixOrchestrator
from logger_config import logger
#------PYTHON IMPORTS -------#
import os
import sys
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 
                                    'BUCKET_SILVER',
                                    'S3_PATH_PRIORITY_SOURCES',
                                    'S3_PATH_INPUT',
                                    'SELECTED_COLUMNS',
                                    'PREFIX_BUCKET',
                                    'JOB_PROCESS_ID'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Envars
BUCKET_SILVER = args['BUCKET_SILVER']
S3_PATH_PRIORITY_SOURCES = args['S3_PATH_PRIORITY_SOURCES']
S3_PATH_INPUT = args['S3_PATH_INPUT']
MAIN_FOLDER = "CLIENTES"
SELECTED_COLUMNS = args['SELECTED_COLUMNS']
PREFIX_BUCKET = args['PREFIX_BUCKET']
ID_PROCESS = args['JOB_PROCESS_ID']
#################################### START JOB ###########################
start_time = datetime.now()
logger.info(f"Job {args['JOB_NAME']} start at {start_time.strftime('%Y-%m-%d %H:%M:%S')} with PID: {args['JOB_PROCESS_ID']}")

selected_columns = SELECTED_COLUMNS.strip().split(',')
# Con lectura desde S3
orchestrator = ConcordanceMatrixOrchestrator(
    spark_session=spark,
    glue_context=glueContext,
    bucket_silver=BUCKET_SILVER,
    s3_path_priority_sources=S3_PATH_PRIORITY_SOURCES,
    selected_columns=selected_columns,
    s3_path_input=S3_PATH_INPUT,
    main_folder="CLIENTES",
    prefix_bucket=PREFIX_BUCKET
)

response = orchestrator.execute_concordance_matrix_processing()

job.commit()