import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import pandas as pd
from pyspark.sql.types import StringType, StructField, StructType
import openpyxl
import et_xmlfile
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1749652517834_df = pd.read_excel("s3://alfa-datalake-dev-langding/NIIF_ULAES/TEST_QUALITY/Formato_ULAES_PSI_Q.xlsx")
AmazonS3_node1749652517834_schema = StructType([StructField(column_name, StringType(), True) for column_name in AmazonS3_node1749652517834_df.columns])
AmazonS3_node1749652517834_df = spark.createDataFrame(AmazonS3_node1749652517834_df, schema=AmazonS3_node1749652517834_schema)
AmazonS3_node1749652517834 = DynamicFrame.fromDF(AmazonS3_node1749652517834_df, glueContext, "AmazonS3_node1749652517834")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1749652557346_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
    #verificar_espacio_atributo
    ColumnValues "COMPANIA" matches "[a-zA-Z]*"
    ]

"""

EvaluateDataQuality_node1749652557346 = EvaluateDataQuality().process_rows(frame=AmazonS3_node1749652517834, ruleset=EvaluateDataQuality_node1749652557346_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749652557346", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node ruleOutcomes
ruleOutcomes_node1749655815407 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1749652557346, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1749655815407")

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1749655803264 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1749652557346, key="rowLevelOutcomes", transformation_ctx="rowLevelOutcomes_node1749655803264")

job.commit()