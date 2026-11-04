import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

query = """
SELECT ID, STATUS, TIME_STAMP
FROM ACSELE_ALFA.CONTEXTOPERATION
WHERE ROWNUM <= 1
"""

dyf = glue_context.create_dynamic_frame.from_options(
    connection_type="oracle",
    connection_options={
        "connectionName": "datalake-dev-acsele-sop-connection",
        "useConnectionProperties": "true",
        "dbtable": "ACSELE_ALFA.CONTEXTOPERATION",
        "sampleQuery": query,
        "oracle.net.CONNECT_TIMEOUT": "30000",
        "oracle.jdbc.ReadTimeout": "60000",
        "fetchsize": "10"
    }
)

df = dyf.toDF()

print("ACSELE_SOP_DEV_CONEXION_OK", flush=True)
print("ACSELE_SOP_DEV_TOTAL={}".format(df.count()), flush=True)

job.commit()
