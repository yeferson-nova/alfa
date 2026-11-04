import sys
import logging

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    col,
    lit,
    concat_ws,
    sha2,
    current_timestamp,
    coalesce,
    trim,
    when
)

from pyspark.sql.types import (
    LongType,
    DecimalType,
    IntegerType,
    TimestampType,
    StringType
)

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
        "S3_SLV_PATH",
        "S3_GLD_PATH"
    ]
)

slv_path = args["S3_SLV_PATH"].rstrip("/")
gld_path = args["S3_GLD_PATH"].rstrip("/")

# -------------------------------------------------
# CONTEXTO GLUE
# -------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# -------------------------------------------------
# CONFIG SPARK 3
# -------------------------------------------------
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info(f"=== INICIO JOB GOLD: {args['JOB_NAME']} ===")
logger.info(f"SILVER PATH BASE: {slv_path}")
logger.info(f"GOLD PATH BASE: {gld_path}")

# -------------------------------------------------
# LECTURA SILVER
# -------------------------------------------------
logger.info("Leyendo tablas Silver requeridas para PRIMAS_ARL")

df_par_endosos = spark.read.parquet(f"{slv_path}/arpprod/par_endosos/")
df_par_afiliaciones = spark.read.parquet(f"{slv_path}/arpprod/par_afiliaciones/")
df_sucursales = spark.read.parquet(f"{slv_path}/general/cgr_sucursales_alfa/")
df_personas_juridicas = spark.read.parquet(f"{slv_path}/general/acu_personas_juridicas/")
df_personas_naturales = spark.read.parquet(f"{slv_path}/general/acu_personas_naturales/")

# -------------------------------------------------
# HELPERS – NOMBRE DEL TERCERO
# -------------------------------------------------
nom_juridico = (
    when(
        trim(col("pj.NOMBRE_COMERCIAL")).isNull() | (trim(col("pj.NOMBRE_COMERCIAL")) == ""),
        trim(col("pj.RAZON_SOCIAL"))
    )
    .when(
        trim(col("pj.NOMBRE_COMERCIAL")) == trim(col("pj.RAZON_SOCIAL")),
        trim(col("pj.NOMBRE_COMERCIAL"))
    )
    .otherwise(trim(col("pj.RAZON_SOCIAL")))
)

nom_natural = concat_ws(
    " ",
    trim(col("pn.PRIMER_APELLIDO")),
    trim(col("pn.SEGUNDO_APELLIDO")),
    trim(col("pn.PRIMER_NOMBRE")),
    trim(col("pn.SEGUNDO_NOMBRE"))
)

nom_tercero = when(
    col("pj.COD_TIPO_IDENTIFICACION").isNotNull() &
    col("pj.NID_NUMERO_IDENTIFICACION").isNotNull(),
    nom_juridico
).otherwise(nom_natural)

# -------------------------------------------------
# DATASET BASE GOLD – PRIMAS_ARL
# -------------------------------------------------
logger.info("Construyendo dataset base PRIMAS_ARL")

df_entrada = (
    df_par_endosos.alias("pe")

    .join(
        df_sucursales.alias("cs"),
        col("pe.COD_SUCURSAL_ALFA") == col("cs.COD_SUCURSAL_ALFA"),
        "left"
    )

    # LEFT JOIN para no perder primas
    .join(
        df_par_afiliaciones.alias("pa"),
        col("pe.NID_POLIZA") == col("pa.NID_AFILIACION"),
        "left"
    )

    .join(
        df_personas_juridicas.alias("pj"),
        (col("pa.COD_TIPO_IDENTIFICACION") == col("pj.COD_TIPO_IDENTIFICACION")) &
        (col("pa.NID_NUMERO_IDENTIFICACION") == col("pj.NID_NUMERO_IDENTIFICACION")) &
        (col("pa.NID_CONSECUTIVO") == col("pj.NID_CONSECUTIVO")),
        "left"
    )

    .join(
        df_personas_naturales.alias("pn"),
        (col("pa.COD_TIPO_IDENTIFICACION") == col("pn.COD_TIPO_IDENTIFICACION")) &
        (col("pa.NID_NUMERO_IDENTIFICACION") == col("pn.NID_NUMERO_IDENTIFICACION")),
        "left"
    )

    .select(
        col("pe.NID_POLIZA").cast(LongType()).alias("NID_POLIZA"),
        col("pe.NID_ENDOSO").cast(LongType()).alias("NID_ENDOSO"),
        col("pe.VLR_PRIMA").cast(DecimalType(17, 2)).alias("VLR_PRIMA"),
        col("pe.FEC_EXPEDICION").cast("date").alias("FEC_EXPEDICION"),
        col("pe.PERIODO").cast(StringType()).alias("PER_EXPEDICION"),
        col("pe.FEC_PROCESO_POINT").cast("date").alias("FEC_PROCESO_POINT"),
        col("pe.PERIODO_POINT").cast(StringType()).alias("PER_PROCESO_POINT"),
        col("pe.COD_TIPO_ENDOSO").cast(LongType()).alias("COD_TIPO_ENDOSO"),
        col("pe.COD_TIPO_ENDOSO_ARP").cast(StringType()).alias("COD_TIPO_ENDOSO_ARP"),
        col("pe.COD_SUCURSAL_ALFA").cast(StringType()).alias("COD_SUCURSAL_ALFA"),
        col("cs.NOMBRE").cast(StringType()).alias("NOM_SUCURSAL_ALFA"),
        col("pe.MODULO").cast(StringType()).alias("MODULO"),
        col("pa.NID_AFILIACION").cast(LongType()).alias("NID_AFILIACION"),
        col("pa.COD_TIPO_IDENTIFICACION").cast(StringType()).alias("COD_TIPO_IDENTIFICACION_TERCERO"),
        col("pa.NID_NUMERO_IDENTIFICACION").cast(LongType()).alias("NID_NUMERO_IDENTIFICACION_TERCERO"),
        nom_tercero.cast(StringType()).alias("NOM_TERCERO"),
        col("pe.LOTE_CARGUE_ID").cast(LongType()).alias("LOTE_CARGUE_ID")
    )
    .filter(
        col("NID_POLIZA").isNotNull() &
        col("NID_ENDOSO").isNotNull()
    )
)

logger.info(f"Registros dataset base: {df_entrada.count()}")

# -------------------------------------------------
# DEFINICIONES SCD TIPO 2
# -------------------------------------------------
KEY_COLS = ["NID_POLIZA", "NID_ENDOSO"]

BUSINESS_COMPARE_COLS = [
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
    "NID_AFILIACION",
    "COD_TIPO_IDENTIFICACION_TERCERO",
    "NID_NUMERO_IDENTIFICACION_TERCERO",
    "NOM_TERCERO"
]

ADMIN_COLS = [
    "LOTE_CARGUE_ID",
    "FEC_INI_VIG_REG",
    "FEC_FIN_VIG_REG",
    "VRS_REG",
    "ACT_REG"
]

FINAL_COLS = KEY_COLS + BUSINESS_COMPARE_COLS + ADMIN_COLS

def hash_negocio():
    return sha2(
        concat_ws(
            "||",
            *[coalesce(col(c).cast("string"), lit("")) for c in BUSINESS_COMPARE_COLS]
        ),
        256
    )

# -------------------------------------------------
# LEER GOLD EXISTENTE
# -------------------------------------------------
logger.info("Intentando leer Gold existente")

required_gold_cols = FINAL_COLS

try:
    df_gold = spark.read.parquet(gld_path).cache()
    df_gold.count()

    missing_cols = [c for c in required_gold_cols if c not in df_gold.columns]

    if missing_cols:
        logger.warning(
            f"Gold existente inválido. Faltan columnas: {missing_cols}. "
            "Se tratará como primera ejecución."
        )
        existe_gold = False
    else:
        existe_gold = True
        logger.info("Gold existente encontrado y válido")

except Exception:
    existe_gold = False
    logger.info("No existe Gold previo (primera ejecución)")

now_ts = current_timestamp().cast(TimestampType())

# -------------------------------------------------
# PRIMERA EJECUCION
# -------------------------------------------------
if not existe_gold:
    logger.info("Primera ejecución: insertando todo")

    df_salida = (
        df_entrada
        .withColumn("FEC_INI_VIG_REG", now_ts)
        .withColumn("FEC_FIN_VIG_REG", lit(None).cast(TimestampType()))
        .withColumn("VRS_REG", lit(1).cast(LongType()))
        .withColumn("ACT_REG", lit(1).cast(IntegerType()))
        .select(*FINAL_COLS)
    )

    (
        df_salida
        .write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .parquet(gld_path)
    )

# -------------------------------------------------
# CONSOLIDACION SCD TIPO 2
# -------------------------------------------------
else:
    logger.info("Ejecutando consolidación SCD Tipo 2")

    df_gold_base = df_gold.select(*FINAL_COLS)

    df_act = df_gold_base.filter(col("ACT_REG") == 1)
    df_inact = df_gold_base.filter(col("ACT_REG") == 0)

    df_e = df_entrada.withColumn("HASH", hash_negocio())
    df_c = df_act.withColumn("HASH", hash_negocio())

    join_cond = (
        (col("e.NID_POLIZA") == col("c.NID_POLIZA")) &
        (col("e.NID_ENDOSO") == col("c.NID_ENDOSO"))
    )

    j = df_e.alias("e").join(df_c.alias("c"), join_cond, "fullouter")

    e_cols = [col(f"e.{c}").alias(c) for c in df_entrada.columns]
    c_final_cols = [col(f"c.{c}").alias(c) for c in FINAL_COLS]

    # Nuevos registros: existen en entrada y no en gold activo
    df_insert = (
        j.filter(
            col("e.NID_POLIZA").isNotNull() &
            col("e.NID_ENDOSO").isNotNull() &
            col("c.NID_POLIZA").isNull() &
            col("c.NID_ENDOSO").isNull()
        )
        .select(*e_cols)
        .withColumn("FEC_INI_VIG_REG", now_ts)
        .withColumn("FEC_FIN_VIG_REG", lit(None).cast(TimestampType()))
        .withColumn("VRS_REG", lit(1).cast(LongType()))
        .withColumn("ACT_REG", lit(1).cast(IntegerType()))
        .select(*FINAL_COLS)
    )

    # Registros que estaban activos y ya no vienen en entrada
    df_off_missing = (
        j.filter(
            col("e.NID_POLIZA").isNull() &
            col("e.NID_ENDOSO").isNull() &
            col("c.NID_POLIZA").isNotNull() &
            col("c.NID_ENDOSO").isNotNull()
        )
        .select(*c_final_cols)
        .withColumn("FEC_FIN_VIG_REG", now_ts)
        .withColumn("ACT_REG", lit(0).cast(IntegerType()))
        .select(*FINAL_COLS)
    )

    # Sin cambios: conservar activo actual
    df_same = (
        j.filter(
            col("e.NID_POLIZA").isNotNull() &
            col("e.NID_ENDOSO").isNotNull() &
            col("c.NID_POLIZA").isNotNull() &
            col("c.NID_ENDOSO").isNotNull() &
            (col("e.HASH") == col("c.HASH"))
        )
        .select(*c_final_cols)
        .select(*FINAL_COLS)
    )

    # Cambiados: cerrar versión anterior
    df_chg_off = (
        j.filter(
            col("e.NID_POLIZA").isNotNull() &
            col("e.NID_ENDOSO").isNotNull() &
            col("c.NID_POLIZA").isNotNull() &
            col("c.NID_ENDOSO").isNotNull() &
            (col("e.HASH") != col("c.HASH"))
        )
        .select(*c_final_cols)
        .withColumn("FEC_FIN_VIG_REG", now_ts)
        .withColumn("ACT_REG", lit(0).cast(IntegerType()))
        .select(*FINAL_COLS)
    )

    # Cambiados: insertar nueva versión
    df_chg_ins = (
        j.filter(
            col("e.NID_POLIZA").isNotNull() &
            col("e.NID_ENDOSO").isNotNull() &
            col("c.NID_POLIZA").isNotNull() &
            col("c.NID_ENDOSO").isNotNull() &
            (col("e.HASH") != col("c.HASH"))
        )
        .select(
            *e_cols,
            col("c.VRS_REG").alias("V_PREV")
        )
        .withColumn("FEC_INI_VIG_REG", now_ts)
        .withColumn("FEC_FIN_VIG_REG", lit(None).cast(TimestampType()))
        .withColumn("VRS_REG", (col("V_PREV") + lit(1)).cast(LongType()))
        .withColumn("ACT_REG", lit(1).cast(IntegerType()))
        .drop("V_PREV")
        .select(*FINAL_COLS)
    )

    df_final = (
        df_inact
        .select(*FINAL_COLS)
        .unionByName(df_same.select(*FINAL_COLS))
        .unionByName(df_off_missing.select(*FINAL_COLS))
        .unionByName(df_chg_off.select(*FINAL_COLS))
        .unionByName(df_insert.select(*FINAL_COLS))
        .unionByName(df_chg_ins.select(*FINAL_COLS))
    )

    (
        df_final
        .write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .parquet(gld_path)
    )

job.commit()
logger.info("Job GOLD PRIMAS_ARL finalizado correctamente")