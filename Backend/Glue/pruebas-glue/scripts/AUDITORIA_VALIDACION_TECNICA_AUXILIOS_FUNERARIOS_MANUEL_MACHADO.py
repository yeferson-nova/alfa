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

def ensure_trailing_slash(path: str) -> str:
    return path if path.endswith("/") else path + "/"

val_path = ensure_trailing_slash(args["S3_VAL_PATH"])

# -------------------------------------------------
# CONTEXTO GLUE
# -------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Compatibilidad Spark 3 / Parquet
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info(f"=== INICIO AUDITORIA VALIDACION TECNICA: {args['JOB_NAME']} ===")
logger.info(f"VALIDACION PATH BASE: {val_path}")

# -------------------------------------------------
# LECTURA RESULTADOS
# -------------------------------------------------
logger.info("Leyendo resultados de validación técnica")

df_detalle = spark.read.parquet(f"{val_path}detalle/")
df_resumen = spark.read.parquet(f"{val_path}resumen/")
df_errores = spark.read.parquet(f"{val_path}errores/")

# -------------------------------------------------
# ATRIBUTOS ESPERADOS
# SOLO COLUMNAS ORIGEN del MappingGLD
# -------------------------------------------------
expected_attrs = [
    "NUM_SOLICITUD_TESORERIA",
    "NUM_POLIZA",
    "TIPO_IDENTIFICACION_AFILIADO",
    "NUM_IDENTIFICACION_AFILIADO",
    "FEC_EXPEDICION_PENSION",
    "VLR_TOTAL",
    "FEC_ENVIO_TESORERIA",
    "COD_CONCEPTO",
    "NOM_CONCEPTO",
    "COD_ORIGEN",
    "DSC_ORIGEN"
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
if len(actual_attrs_sorted) != len(expected_attrs_sorted):
    raise Exception(
        f"Se esperaban {len(expected_attrs_sorted)} atributos y se encontraron {len(actual_attrs_sorted)}"
    )

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
(
    df_detalle
    .groupBy("TABLA_ORIGEN", "ATRIBUTO")
    .agg(count(lit(1)).alias("CANTIDAD"))
    .orderBy("TABLA_ORIGEN", "ATRIBUTO")
    .show(100, False)
)

# -------------------------------------------------
# RESULTADO 0/1 POR ATRIBUTO
# -------------------------------------------------
logger.info("Distribución de VAL_RESULTADO_ATRIBUTO por atributo")
(
    df_detalle
    .groupBy("ATRIBUTO", "VAL_RESULTADO_ATRIBUTO")
    .agg(count(lit(1)).alias("CANTIDAD"))
    .orderBy("ATRIBUTO", "VAL_RESULTADO_ATRIBUTO")
    .show(100, False)
)

# -------------------------------------------------
# RESUMEN FINAL DE VALIDACION
# -------------------------------------------------
logger.info("Distribución de IND_VALIDACION_TECNICA")
(
    df_resumen
    .groupBy("IND_VALIDACION_TECNICA")
    .agg(count(lit(1)).alias("CANTIDAD"))
    .orderBy("IND_VALIDACION_TECNICA")
    .show(10, False)
)

logger.info("=== FIN AUDITORIA VALIDACION TECNICA AUXILIOS FUNERARIOS ===")
job.commit()