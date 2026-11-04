import sys
import json
import logging
import boto3
from urllib.parse import urlparse

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    col,
    lit,
    sha2,
    concat_ws,
    coalesce,
    current_timestamp
)
from pyspark.sql.types import IntegerType, TimestampType

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
    ["JOB_NAME", "RUTA_CONFIG_SCD"]
)

ruta_config_scd = args["RUTA_CONFIG_SCD"]

# -------------------------------------------------
# HELPERS
# -------------------------------------------------
def cargar_json_s3(ruta_s3: str) -> dict:
    parsed = urlparse(ruta_s3)
    s3 = boto3.client("s3")
    obj = s3.get_object(
        Bucket=parsed.netloc,
        Key=parsed.path.lstrip("/")
    )
    return json.loads(obj["Body"].read().decode("utf-8"))

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
# CARGAR CONFIGURACION
# -------------------------------------------------
configuracion = cargar_json_s3(ruta_config_scd)

nombre_caso = configuracion["nombre_caso"]
ruta_silver = configuracion["entrada"]["ruta_silver_preparada"]
ruta_gold = configuracion["salida"]["ruta_gold"]

COLUMNAS_LLAVE = configuracion["llaves"]["primaria"]
COLUMNAS_APERTURA = configuracion["llaves"]["apertura"]
COLUMNAS_CAMBIO = configuracion["columnas_cambio"]["normales"]
COLUMNAS_CAMBIO_ALERTA = configuracion["columnas_cambio"]["alerta"]
COLUMNAS_FIJAS = configuracion["columnas_fijas"]

COLUMNAS_HASH = COLUMNAS_APERTURA + COLUMNAS_CAMBIO + COLUMNAS_CAMBIO_ALERTA

logger.info(f"Iniciando SCD estándar DEV para el caso: {nombre_caso}")
logger.info(f"Leyendo Silver desde: {ruta_silver}")
logger.info(f"Escribiendo Gold en: {ruta_gold}")

# -------------------------------------------------
# LECTURA SILVER
# -------------------------------------------------
df_entrada = spark.read.parquet(ruta_silver)

total_filas_silver = df_entrada.count()
logger.info(f"Total filas Silver preparada: {total_filas_silver}")

# -------------------------------------------------
# PROTECCION DEV: SILVER VACIO
# -------------------------------------------------
if total_filas_silver == 0:
    logger.warning(
        "Silver preparada vacía. "
        "No se ejecuta lógica SCD para evitar apagar histórico Gold."
    )
    job.commit()
    sys.exit(0)

# -------------------------------------------------
# HASH DE NEGOCIO
# -------------------------------------------------
def hash_negocio():
    return sha2(
        concat_ws(
            "||",
            *[coalesce(col(c).cast("string"), lit("")) for c in COLUMNAS_HASH]
        ),
        256
    )

ahora = current_timestamp().cast(TimestampType())

# -------------------------------------------------
# LEER GOLD EXISTENTE
# -------------------------------------------------
try:
    df_gold = spark.read.parquet(ruta_gold).cache()
    df_gold.count()
    existe_gold = True
    logger.info("Gold existente encontrado")
except Exception:
    existe_gold = False
    logger.info("Primera ejecución: no existe Gold")

# -------------------------------------------------
# PRIMERA EJECUCION
# -------------------------------------------------
if not existe_gold:
    df_salida = (
        df_entrada
        .withColumn("FEC_INI_VIG_REG", ahora)
        .withColumn("FEC_FIN_VIG_REG", lit(None).cast(TimestampType()))
        .withColumn("VRS_REG", lit(1).cast(IntegerType()))
        .withColumn("ACT_REG", lit(1).cast(IntegerType()))
    )

    df_salida.write.mode("overwrite").parquet(ruta_gold)

# -------------------------------------------------
# CONSOLIDACION SCD
# -------------------------------------------------
else:
    df_activos = df_gold.filter(col("ACT_REG") == 1)
    df_inactivos = df_gold.filter(col("ACT_REG") == 0)

    df_e = df_entrada.withColumn("HASH", hash_negocio())
    df_c = df_activos.withColumn("HASH", hash_negocio())

    j = df_e.alias("e").join(df_c.alias("c"), COLUMNAS_LLAVE, "full")

    df_insertar = (
        j.filter(col(f"c.{COLUMNAS_LLAVE[0]}").isNull())
        .select("e.*")
        .withColumn("FEC_INI_VIG_REG", ahora)
        .withColumn("FEC_FIN_VIG_REG", lit(None).cast(TimestampType()))
        .withColumn("VRS_REG", lit(1).cast(IntegerType()))
        .withColumn("ACT_REG", lit(1).cast(IntegerType()))
    )

    df_apagar = (
        j.filter(col(f"e.{COLUMNAS_LLAVE[0]}").isNull())
        .select("c.*")
        .withColumn("FEC_FIN_VIG_REG", ahora)
        .withColumn("ACT_REG", lit(0).cast(IntegerType()))
    )

    df_sin_cambio = (
        j.filter(
            col(f"e.{COLUMNAS_LLAVE[0]}").isNotNull() &
            col(f"c.{COLUMNAS_LLAVE[0]}").isNotNull() &
            (col("e.HASH") == col("c.HASH"))
        )
        .select("c.*")
    )

    df_cerrar = (
        j.filter(
            col(f"e.{COLUMNAS_LLAVE[0]}").isNotNull() &
            col(f"c.{COLUMNAS_LLAVE[0]}").isNotNull() &
            (col("e.HASH") != col("c.HASH"))
        )
        .select("c.*")
        .withColumn("FEC_FIN_VIG_REG", ahora)
        .withColumn("ACT_REG", lit(0).cast(IntegerType()))
    )

    df_nueva_version = (
        j.filter(
            col(f"e.{COLUMNAS_LLAVE[0]}").isNotNull() &
            col(f"c.{COLUMNAS_LLAVE[0]}").isNotNull() &
            (col("e.HASH") != col("c.HASH"))
        )
        .select("e.*", col("c.VRS_REG").alias("VRS_ANT"))
        .withColumn("FEC_INI_VIG_REG", ahora)
        .withColumn("FEC_FIN_VIG_REG", lit(None).cast(TimestampType()))
        .withColumn("VRS_REG", (col("VRS_ANT") + 1).cast(IntegerType()))
        .withColumn("ACT_REG", lit(1).cast(IntegerType()))
        .drop("VRS_ANT")
    )

    df_final = (
        df_inactivos
        .unionByName(df_sin_cambio)
        .unionByName(df_apagar)
        .unionByName(df_cerrar)
        .unionByName(df_insertar)
        .unionByName(df_nueva_version)
    )

    df_final.write.mode("overwrite").parquet(ruta_gold)

# -------------------------------------------------
# FIN
# -------------------------------------------------
job.commit()
logger.info(f"SCD DEV finalizado correctamente para el caso: {nombre_caso}")
