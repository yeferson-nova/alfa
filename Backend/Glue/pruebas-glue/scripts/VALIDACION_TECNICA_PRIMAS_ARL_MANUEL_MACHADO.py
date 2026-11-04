import sys
import logging

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    when,
    length,
    concat_ws,
    sum as spark_sum
)
from pyspark.sql.types import DecimalType

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
    ["JOB_NAME", "S3_SLV_PATH", "S3_VAL_PATH"]
)

slv_path = args["S3_SLV_PATH"].rstrip("/")
val_path = args["S3_VAL_PATH"].rstrip("/")

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

# -------------------------------------------------
# REGLAS TECNICAS
# Derivadas de MappingGLD (qué validar) + SistemasSRC (cómo validar)
# -------------------------------------------------
RULES = [
    # DEST_COL, SRC_SCHEMA, SRC_TABLE, SRC_COL, SRC_KEY_COLS, EXPECTED_TYPE, LENGTH, PRECISION, SCALE, NULLABLE
    {
        "dest_col": "NID_POLIZA",
        "src_schema": "arpprod",
        "src_table": "par_endosos",
        "src_col": "NID_POLIZA",
        "src_key_cols": ["NID_POLIZA", "NID_ENDOSO"],
        "type": "NUMBER",
        "length": 22,
        "precision": 10,
        "scale": 0,
        "nullable": "N"
    },
    {
        "dest_col": "NID_ENDOSO",
        "src_schema": "arpprod",
        "src_table": "par_endosos",
        "src_col": "NID_ENDOSO",
        "src_key_cols": ["NID_POLIZA", "NID_ENDOSO"],
        "type": "NUMBER",
        "length": 22,
        "precision": 10,
        "scale": 0,
        "nullable": "N"
    },
    {
        "dest_col": "VLR_PRIMA",
        "src_schema": "arpprod",
        "src_table": "par_endosos",
        "src_col": "VLR_PRIMA",
        "src_key_cols": ["NID_POLIZA", "NID_ENDOSO"],
        "type": "NUMBER",
        "length": 22,
        "precision": 17,
        "scale": 2,
        "nullable": "N"
    },
    {
        "dest_col": "FEC_EXPEDICION",
        "src_schema": "arpprod",
        "src_table": "par_endosos",
        "src_col": "FEC_EXPEDICION",
        "src_key_cols": ["NID_POLIZA", "NID_ENDOSO"],
        "type": "DATE",
        "length": 7,
        "precision": None,
        "scale": None,
        "nullable": "N"
    },
    {
        "dest_col": "PER_EXPEDICION",
        "src_schema": "arpprod",
        "src_table": "par_endosos",
        "src_col": "PERIODO",
        "src_key_cols": ["NID_POLIZA", "NID_ENDOSO"],
        "type": "VARCHAR2",
        "length": 6,
        "precision": None,
        "scale": None,
        "nullable": "Y"
    },
    {
        "dest_col": "FEC_PROCESO_POINT",
        "src_schema": "arpprod",
        "src_table": "par_endosos",
        "src_col": "FEC_PROCESO_POINT",
        "src_key_cols": ["NID_POLIZA", "NID_ENDOSO"],
        "type": "DATE",
        "length": 7,
        "precision": None,
        "scale": None,
        "nullable": "Y"
    },
    {
        "dest_col": "PER_PROCESO_POINT",
        "src_schema": "arpprod",
        "src_table": "par_endosos",
        "src_col": "PERIODO_POINT",
        "src_key_cols": ["NID_POLIZA", "NID_ENDOSO"],
        "type": "VARCHAR2",
        "length": 6,
        "precision": None,
        "scale": None,
        "nullable": "Y"
    },
    {
        "dest_col": "COD_TIPO_ENDOSO",
        "src_schema": "arpprod",
        "src_table": "par_endosos",
        "src_col": "COD_TIPO_ENDOSO",
        "src_key_cols": ["NID_POLIZA", "NID_ENDOSO"],
        "type": "NUMBER",
        "length": 22,
        "precision": 10,
        "scale": 0,
        "nullable": "N"
    },
    {
        "dest_col": "COD_TIPO_ENDOSO_ARP",
        "src_schema": "arpprod",
        "src_table": "par_endosos",
        "src_col": "COD_TIPO_ENDOSO_ARP",
        "src_key_cols": ["NID_POLIZA", "NID_ENDOSO"],
        "type": "VARCHAR2",
        "length": 3,
        "precision": None,
        "scale": None,
        "nullable": "N"
    },
    {
        "dest_col": "COD_SUCURSAL_ALFA",
        "src_schema": "arpprod",
        "src_table": "par_endosos",
        "src_col": "COD_SUCURSAL_ALFA",
        "src_key_cols": ["NID_POLIZA", "NID_ENDOSO"],
        "type": "VARCHAR2",
        "length": 3,
        "precision": None,
        "scale": None,
        "nullable": "N"
    },
    {
        "dest_col": "MODULO",
        "src_schema": "arpprod",
        "src_table": "par_endosos",
        "src_col": "MODULO",
        "src_key_cols": ["NID_POLIZA", "NID_ENDOSO"],
        "type": "VARCHAR2",
        "length": 2,
        "precision": None,
        "scale": None,
        "nullable": "N"
    },
    {
        "dest_col": "NOM_SUCURSAL_ALFA",
        "src_schema": "general",
        "src_table": "cgr_sucursales_alfa",
        "src_col": "NOMBRE",
        "src_key_cols": ["COD_SUCURSAL_ALFA"],
        "type": "VARCHAR2",
        "length": 50,
        "precision": None,
        "scale": None,
        "nullable": "N"
    },
    {
        "dest_col": "NID_AFILIACION",
        "src_schema": "arpprod",
        "src_table": "par_afiliaciones",
        "src_col": "NID_AFILIACION",
        "src_key_cols": ["NID_AFILIACION"],
        "type": "NUMBER",
        "length": 22,
        "precision": 10,
        "scale": 0,
        "nullable": "N"
    }
]

# -------------------------------------------------
# LECTURA DE TABLAS FUENTE EN SLV
# -------------------------------------------------
logger.info("Leyendo tablas Silver origen para validación técnica")

df_par_endosos = spark.read.parquet(f"{slv_path}/arpprod/par_endosos/")
df_cgr_sucursales = spark.read.parquet(f"{slv_path}/general/cgr_sucursales_alfa/")
df_par_afiliaciones = spark.read.parquet(f"{slv_path}/arpprod/par_afiliaciones/")

TABLE_MAP = {
    ("arpprod", "par_endosos"): df_par_endosos,
    ("general", "cgr_sucursales_alfa"): df_cgr_sucursales,
    ("arpprod", "par_afiliaciones"): df_par_afiliaciones
}

# -------------------------------------------------
# HELPERS
# -------------------------------------------------
def get_dtype(df: DataFrame, col_name: str) -> str:
    dtypes = dict(df.dtypes)
    return dtypes.get(col_name)

def schema_type_ok(dtype_str: str, expected_type: str) -> int:
    if dtype_str is None:
        return 0

    dtype_str = dtype_str.lower()

    if expected_type == "NUMBER":
        # Acepta decimal e enteros de Spark
        if dtype_str.startswith("decimal") or dtype_str in ("bigint", "int", "smallint", "tinyint"):
            return 1
        return 0

    if expected_type == "VARCHAR2":
        return 1 if dtype_str == "string" else 0

    if expected_type == "DATE":
        # Oracle DATE usualmente llega como timestamp/date en Spark
        return 1 if dtype_str in ("date", "timestamp") else 0

    return 0

def validate_nullability(col_name: str, nullable: str):
    if nullable == "N":
        return when(col(col_name).isNotNull(), lit(1)).otherwise(lit(0))
    return lit(1)

def validate_length(col_name: str, max_len: int):
    return when(col(col_name).isNull(), lit(1)).otherwise(
        when(length(col(col_name).cast("string")) <= max_len, lit(1)).otherwise(lit(0))
    )

def validate_number(col_name: str, precision: int, scale: int):
    return when(col(col_name).isNull(), lit(1)).otherwise(
        when(col(col_name).cast(DecimalType(precision, scale)).isNotNull(), lit(1)).otherwise(lit(0))
    )

def validate_date(col_name: str):
    return when(col(col_name).isNull(), lit(1)).otherwise(
        when(col(col_name).cast("date").isNotNull(), lit(1)).otherwise(lit(0))
    )

def build_llave(df: DataFrame, key_cols):
    parts = [col(c).cast("string") for c in key_cols]
    return concat_ws(";", *parts)

def build_rule_df(rule):
    src_df = TABLE_MAP[(rule["src_schema"], rule["src_table"])]
    src_col = rule["src_col"]
    key_cols = rule["src_key_cols"]

    actual_dtype = get_dtype(src_df, src_col)
    schema_ok = schema_type_ok(actual_dtype, rule["type"])

    df = (
        src_df
        .select(
            *[col(k) for k in key_cols],
            col(src_col).alias("_VAL")
        )
        .withColumn("TABLA_ORIGEN", lit(f"{rule['src_schema']}.{rule['src_table']}"))
        .withColumn("COLUMNA_ORIGEN", lit(src_col))
        .withColumn("COLUMNA_DESTINO", lit(rule["dest_col"]))
        .withColumn("LLAVE_REGISTRO", build_llave(src_df, key_cols))
        .withColumn("ATRIBUTO", lit(rule["dest_col"]))
        .withColumn("VALOR", col("_VAL").cast("string"))
        .withColumn("VAL_NULO_DATO", validate_nullability("_VAL", rule["nullable"]))
    )

    if rule["type"] == "VARCHAR2":
        df = (
            df
            .withColumn("VAL_TIPO_DATO", lit(schema_ok))
            .withColumn("VAL_LONGITUD_DATO", validate_length("_VAL", rule["length"]))
            .withColumn("VAL_PRECISION_DATO", lit(1))
            .withColumn("VAL_ESCALA_DATO", lit(1))
        )

    elif rule["type"] == "NUMBER":
        df = (
            df
            .withColumn("VAL_TIPO_DATO", lit(schema_ok))
            .withColumn("VAL_LONGITUD_DATO", lit(1))
            .withColumn("VAL_PRECISION_DATO", validate_number("_VAL", rule["precision"], rule["scale"]))
            .withColumn("VAL_ESCALA_DATO", validate_number("_VAL", rule["precision"], rule["scale"]))
        )

    elif rule["type"] == "DATE":
        df = (
            df
            .withColumn("VAL_TIPO_DATO", lit(schema_ok))
            .withColumn("VAL_LONGITUD_DATO", lit(1))
            .withColumn("VAL_PRECISION_DATO", lit(1))
            .withColumn("VAL_ESCALA_DATO", lit(1))
        )
        df = df.withColumn(
            "VAL_TIPO_DATO",
            when(col("_VAL").isNull(), lit(1)).otherwise(
                when(col("_VAL").cast("date").isNotNull(), lit(schema_ok)).otherwise(lit(0))
            )
        )

    key_projection = [col(k).cast("string").alias(k) for k in key_cols]

    return (
        df
        .select(
            "TABLA_ORIGEN",
            "COLUMNA_ORIGEN",
            "COLUMNA_DESTINO",
            "ATRIBUTO",
            "LLAVE_REGISTRO",
            *key_projection,
            "VALOR",
            "VAL_NULO_DATO",
            "VAL_TIPO_DATO",
            "VAL_LONGITUD_DATO",
            "VAL_PRECISION_DATO",
            "VAL_ESCALA_DATO"
        )
        .withColumn(
            "VAL_RESULTADO_ATRIBUTO",
            when(
                (col("VAL_TIPO_DATO") == 1) &
                (col("VAL_LONGITUD_DATO") == 1) &
                (col("VAL_PRECISION_DATO") == 1) &
                (col("VAL_ESCALA_DATO") == 1) &
                (col("VAL_NULO_DATO") == 1),
                lit(1)
            ).otherwise(lit(0))
        )
    )

# -------------------------------------------------
# EJECUCION VALIDACIONES
# -------------------------------------------------
logger.info("Aplicando validaciones técnicas sobre atributos fuente")

df_detalle = None
for rule in RULES:
    df_rule = build_rule_df(rule)
    df_detalle = df_rule if df_detalle is None else df_detalle.unionByName(df_rule, allowMissingColumns=True)

# -------------------------------------------------
# RESUMEN
# -------------------------------------------------
df_resumen = (
    df_detalle
    .groupBy("TABLA_ORIGEN", "LLAVE_REGISTRO")
    .agg(
        spark_sum(col("VAL_RESULTADO_ATRIBUTO")).alias("TOTAL_REGLAS_OK"),
        spark_sum(when(col("VAL_RESULTADO_ATRIBUTO") == 0, 1).otherwise(0)).alias("TOTAL_REGLAS_ERROR")
    )
    .withColumn(
        "IND_VALIDACION_TECNICA",
        when(col("TOTAL_REGLAS_ERROR") == 0, lit(1)).otherwise(lit(0))
    )
)

# -------------------------------------------------
# ESCRITURA
# -------------------------------------------------
logger.info("Escribiendo resultados de validación técnica")

(
    df_detalle.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .parquet(f"{val_path}/detalle/")
)

(
    df_resumen.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .parquet(f"{val_path}/resumen/")
)

(
    df_detalle
    .filter(col("VAL_RESULTADO_ATRIBUTO") == 0)
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .parquet(f"{val_path}/errores/")
)

logger.info("Validación técnica origen finalizada correctamente")

# -------------------------------------------------
# QUALITY GATE
# -------------------------------------------------
errores_count = (
    df_detalle
    .filter(col("VAL_RESULTADO_ATRIBUTO") == 0)
    .count()
)

logger.info(f"Total errores técnicos encontrados: {errores_count}")

if errores_count > 0:
    raise Exception(
        f"Validación técnica fallida. "
        f"Se encontraron {errores_count} errores. "
        f"Revisar salida en {val_path}/errores/"
    )

logger.info("Quality gate OK: no se encontraron errores técnicos.")

job.commit()