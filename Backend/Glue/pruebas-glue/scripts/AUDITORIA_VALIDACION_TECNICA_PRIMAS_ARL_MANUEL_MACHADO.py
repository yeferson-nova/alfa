import sys
import logging

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count, lit

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
    ["JOB_NAME", "S3_VAL_PATH"]
)

val_path = args["S3_VAL_PATH"].rstrip("/")

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

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------
# LECTURA RESULTADOS
# -------------------------------------------------
logger.info("Leyendo resultados de validación técnica origen")

df_detalle = spark.read.parquet(f"{val_path}/detalle/")
df_resumen = spark.read.parquet(f"{val_path}/resumen/")
df_errores = spark.read.parquet(f"{val_path}/errores/")

# -------------------------------------------------
# ATRIBUTOS ESPERADOS (13 columnas ORIGEN del MappingGLD)
# -------------------------------------------------
expected_attrs = [
    "NID_POLIZA",
    "NID_ENDOSO",
    "VLR_PRIMA",
    "FEC_EXPEDICION",
    "PER_EXPEDICION",
    "FEC_PROCESO_POINT",
    "PER_PROCESO_POINT",
    "COD_TIPO_ENDOSO",
    "COD_TIPO_ENDOSO_ARP",
    "COD_SUCURSAL_ALFA",
    "NOM_SUCURSAL_ALFA",
    "MODULO",
    "NID_AFILIACION"
]

actual_attrs = [r["ATRIBUTO"] for r in df_detalle.select("ATRIBUTO").distinct().collect()]
actual_attrs_sorted = sorted(actual_attrs)
expected_attrs_sorted = sorted(expected_attrs)

logger.info(f"Atributos distintos encontrados: {len(actual_attrs_sorted)}")
logger.info(f"Atributos esperados: {expected_attrs_sorted}")
logger.info(f"Atributos encontrados: {actual_attrs_sorted}")

# -------------------------------------------------
# VALIDACIONES DE AUDITORIA
# -------------------------------------------------
if len(actual_attrs_sorted) != 13:
    raise Exception(f"Se esperaban 13 atributos y se encontraron {len(actual_attrs_sorted)}")

if actual_attrs_sorted != expected_attrs_sorted:
    raise Exception(
        f"Los atributos encontrados no coinciden con los esperados. "
        f"Esperados={expected_attrs_sorted}, Encontrados={actual_attrs_sorted}"
    )

# -------------------------------------------------
# METRICAS GENERALES
# -------------------------------------------------
total_detalle = df_detalle.count()
total_resumen = df_resumen.count()
total_errores = df_errores.count()

logger.info(f"Total filas detalle: {total_detalle}")
logger.info(f"Total filas resumen: {total_resumen}")
logger.info(f"Total filas errores: {total_errores}")

# -------------------------------------------------
# DISTRIBUCION POR TABLA ORIGEN Y ATRIBUTO
# -------------------------------------------------
logger.info("Distribución por TABLA_ORIGEN y ATRIBUTO")
df_detalle.groupBy("TABLA_ORIGEN", "ATRIBUTO") \
    .agg(count(lit(1)).alias("CANTIDAD")) \
    .orderBy("TABLA_ORIGEN", "ATRIBUTO") \
    .show(100, False)

# -------------------------------------------------
# ERRORES POR ATRIBUTO
# -------------------------------------------------
logger.info("Errores por atributo")
df_detalle.groupBy("ATRIBUTO", "VAL_RESULTADO_ATRIBUTO") \
    .agg(count(lit(1)).alias("CANTIDAD")) \
    .orderBy("ATRIBUTO", "VAL_RESULTADO_ATRIBUTO") \
    .show(100, False)

# -------------------------------------------------
# RESUMEN FINAL DE VALIDACION TECNICA
# -------------------------------------------------
logger.info("Distribución de IND_VALIDACION_TECNICA")
df_resumen.groupBy("IND_VALIDACION_TECNICA") \
    .agg(count(lit(1)).alias("CANTIDAD")) \
    .orderBy("IND_VALIDACION_TECNICA") \
    .show(10, False)

logger.info("Auditoría de validación técnica origen finalizada correctamente")
job.commit()