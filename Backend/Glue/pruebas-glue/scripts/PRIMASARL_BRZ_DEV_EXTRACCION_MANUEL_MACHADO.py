import sys
import logging
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, monotonically_increasing_id

# -------------------------------------------------
# LOGGING
# -------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------------------------
# PARAMETROS
# -------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BUCKET_NAME", "CONNECT_NAME"]
)

BASE_PATH = args["BUCKET_NAME"]
CONNECT_NAME = args["CONNECT_NAME"]

# -------------------------------------------------
# CONTEXTO GLUE
# -------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
lote_cargue_id = int(datetime.now().timestamp())

logger.info(f"LOTE_CARGUE_ID generado: {lote_cargue_id}")
logger.info("Extracción SIN límite de registros")
logger.info(f"Ruta base Bronze: {BASE_PATH}")

# -------------------------------------------------
# TABLAS
# -------------------------------------------------
tablas = [
    ("ARPPROD", "GAR_TIPOS_ENDOSOS_ARP"),
    ("ARPPROD", "PAR_POLIZAS"),
    ("ARPPROD", "PAR_ENDOSOS"),
    ("ARPPROD", "PAR_AFILIACIONES"),
    ("GENERAL", "CGR_TIPOS_ENDOSOS"),
    ("GENERAL", "CGR_SUCURSALES_ALFA"),
    ("GENERAL", "ACU_PERSONAS_JURIDICAS"),
    ("GENERAL", "ACU_PERSONAS_NATURALES")
]

# -------------------------------------------------
# EXTRACCION ORACLE -> BRONZE (FULL)
# -------------------------------------------------
for schema, table in tablas:
    logger.info(f"===== INICIANDO {schema}.{table} =====")

    try:
        dbtable_name = f"{schema}.{table}"
        logger.info(f"dbtable: {dbtable_name}")

        dyf = glueContext.create_dynamic_frame.from_options(
            connection_type="oracle",
            connection_options={
                "connectionName": CONNECT_NAME,
                "useConnectionProperties": "true",
                "dbtable": dbtable_name,
                "oracle.net.CONNECT_TIMEOUT": "30000",
                "oracle.jdbc.ReadTimeout": "30000"
            }
        )

        df = dyf.toDF()

        logger.info(f"Schema leído de {schema}.{table}:")
        df.printSchema()

        df = (
            df
            .withColumn("NRO_FILA", monotonically_increasing_id().cast("bigint"))
            .withColumn("LOTE_CARGUE_ID", lit(lote_cargue_id).cast("bigint"))
        )

        target_path = f"{BASE_PATH}{schema.lower()}/{table.lower()}/"
        logger.info(f"Escribiendo en: {target_path}")

        (
            df.write
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .parquet(target_path)
        )

        logger.info(f"===== OK {schema}.{table} =====")

    except Exception:
        logger.error(f"===== ERROR EN {schema}.{table} =====", exc_info=True)
        raise

# -------------------------------------------------
# FIN
# -------------------------------------------------
logger.info("=== FIN JOB BRONZE PRIMAS ARL (FULL) ===")
job.commit()