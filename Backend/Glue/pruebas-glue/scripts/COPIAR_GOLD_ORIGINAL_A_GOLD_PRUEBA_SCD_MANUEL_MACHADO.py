import sys
import logging

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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
    [
        "JOB_NAME",
        "S3_GLD_SRC_PATH",
        "S3_GLD_TEST_PATH"
    ]
)

def ensure_trailing_slash(path: str) -> str:
    return path if path.endswith("/") else path + "/"

gld_src_path = ensure_trailing_slash(args["S3_GLD_SRC_PATH"])
gld_test_path = ensure_trailing_slash(args["S3_GLD_TEST_PATH"])

# -------------------------------------------------
# CONTEXTO GLUE
# -------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# -------------------------------------------------
# CONFIG SPARK 3 - PARQUET / FECHAS
# -------------------------------------------------
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info(f"=== INICIO JOB COPIA GOLD ===")
logger.info(f"GOLD ORIGEN: {gld_src_path}")
logger.info(f"GOLD PRUEBA: {gld_test_path}")

try:
    # -----------------------------------------
    # LECTURA GOLD ORIGINAL
    # -----------------------------------------
    logger.info("Leyendo dataset Gold original")
    df_gld = spark.read.parquet(gld_src_path)

    logger.info("Schema leído del Gold original:")
    df_gld.printSchema()

    count_gld = df_gld.count()
    logger.info(f"Registros leídos desde Gold original: {count_gld}")

    # -----------------------------------------
    # ESCRITURA GOLD PRUEBA
    # -----------------------------------------
    logger.info("Escribiendo dataset en Gold de prueba")

    (
        df_gld.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .parquet(gld_test_path)
    )

    logger.info("=== COPIA GOLD ORIGINAL -> GOLD PRUEBA FINALIZADA OK ===")

except Exception:
    logger.error("=== ERROR EN COPIA GOLD ORIGINAL -> GOLD PRUEBA ===", exc_info=True)
    raise

job.commit()