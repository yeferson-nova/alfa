
import sys
import json
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
contexto = GlueContext(SparkContext.getOrCreate())
job = Job(contexto)
job.init(args["JOB_NAME"], args)

conexion = contexto.extract_jdbc_conf(
    connection_name="datalake-dev-alfaniif-pre-connection"
)

sql = """
SELECT
    COUNT(*) AS TOTAL_MAYO,
    COUNT(CASE WHEN COD_ESTADO_RESERVA = 1 THEN 1 END)
        AS TOTAL_MAYO_ESTADO_1
FROM ARPPROD.SAR_DATOS_INVALIDEZ_MUERTE
WHERE FEC_APROBACION_PENSION >= DATE '2026-05-01'
  AND FEC_APROBACION_PENSION < DATE '2026-06-01'
"""

url = conexion.get("fullUrl") or conexion.get("url")
if not url:
    raise RuntimeError("La conexion no devolvio URL JDBC.")

df = (
    contexto.spark_session.read.format("jdbc")
    .option("url", url)
    .option("driver", "oracle.jdbc.OracleDriver")
    .option("user", conexion["user"])
    .option("password", conexion["password"])
    .option("dbtable", "(" + sql.strip() + ") DIAG_ARP")
    .option("fetchsize", "1")
    .load()
)

fila = df.collect()[0]
resultado = {k: int(v) if v is not None else None
             for k, v in fila.asDict().items()}

print("RESULTADO_ORACLE_ARP_202605=" + json.dumps(resultado), flush=True)
job.commit()
