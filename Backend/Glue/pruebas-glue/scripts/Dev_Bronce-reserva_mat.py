"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.ExtractHelper import ExtractHelper
from pyspark.sql import functions as F

import sys, traceback, boto3, logging

###################################################################
########################## SET ARGS ###############################
###################################################################
MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt= DATETIME_FORMAT)
logger = logging.getLogger('log message')
logger.setLevel(logging.INFO)

args = getResolvedOptions(
    sys.argv, 
        [
        'JOB_NAME',
        'DB_SCHEMA',
        'BUCKET_NAME',
        'BUCKET_NAME_ASSETS',
        'CONNECT_NAME', 
        'CONNECT_NAME_ASC',
        'CONNECT_NAME_QA'
        ]
    )
DB_SCHEMA = args['DB_SCHEMA']
BUCKET_NAME = args['BUCKET_NAME']
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
CONNECT_NAME = args['CONNECT_NAME']
CONNECT_NAME_ASC = args['CONNECT_NAME_ASC']
CONNECT_NAME_QA = args['CONNECT_NAME_QA']


logger.info(f"Schema: {DB_SCHEMA}")
logger.info(f"Connect: {CONNECT_NAME}")
logger.info(f"Bucket_name: {BUCKET_NAME}")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3_client = boto3.client("s3")

simple_execution_acsele = {
        "useConnectionProperties": "true",
        "dbtable": f"ACSELE_ALFA.CLAIM",
        "connectionName": CONNECT_NAME_ASC
}

simple_execution_general = {
        "useConnectionProperties": "true",
        "dbtable": f"{DB_SCHEMA}.PGR_SALARIOS_MINIMOS",
        "connectionName": CONNECT_NAME_QA
    }
    
simple_execution_arpprod = {
        "useConnectionProperties": "true",
        "dbtable": f"ARPPROD.SAR_BENEFICIARIOS_SINIESTROS",
        "connectionName": CONNECT_NAME
    }
        
simple_execution_prv = {
        "useConnectionProperties": "true",
        "dbtable": f"PRV.PRV_RENTAS",
        "connectionName": CONNECT_NAME
    }
    

my_tabla = glueContext.create_dynamic_frame.from_options(
connection_type = "oracle",
connection_options = simple_execution_general,
    transformation_ctx = "MyTabla"
)


my_tabla.printSchema()
my_tabla.toDF().show(100)