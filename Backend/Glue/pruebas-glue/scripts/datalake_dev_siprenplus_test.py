import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Oracle SQL
OracleSQL_node1747922989538 = glueContext.create_dynamic_frame.from_options(
    connection_type = "oracle",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "SINIESTROS.SNR_DATO_BASICO",
        "connectionName": "Jdbc_sipren_vpn",
    },
    transformation_ctx = "OracleSQL_node1747922989538"
)

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=OracleSQL_node1747922989538, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1747921257259", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1747923042930 = glueContext.write_dynamic_frame.from_options(frame=OracleSQL_node1747922989538, connection_type="s3", format="glueparquet", connection_options={"path": "s3://alfa-bronce-test-jl/SIPREN_PLUS/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1747923042930")

job.commit()