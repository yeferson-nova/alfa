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
    [
        "JOB_NAME",
        "S3_BRZ_PATH",
        "S3_VAL_PATH"
    ]
)

def ensure_trailing_slash(path: str) -> str:
    return path if path.endswith("/") else path + "/"

brz_path = ensure_trailing_slash(args["S3_BRZ_PATH"])
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

logger.info(f"=== INICIO JOB VALIDACION TECNICA ORIGEN: {args['JOB_NAME']} ===")
logger.info(f"BRONZE PATH BASE: {brz_path}")
logger.info(f"VALIDACION PATH BASE: {val_path}")

# -------------------------------------------------
# REGLAS TECNICAS
# SOLO COLUMNAS ORIGEN del MappingGLD
# -------------------------------------------------
RULES = [
    {
        "dest_col": "NUM_SOLICITUD_TESORERIA",
        "src_schema": "general",
        "src_table": "anp_periodo_solicitudes_giro",
        "src_col": "NUM_SOLICITUD_TES",
        "src_key_cols": ["NID_PERIODO_SOLICITUD_GIRO"],
        "type": "NUMBER",
        "length": 22,
        "precision": 10,
        "scale": 0,
        "nullable": "Y"
    },
    {
        "dest_col": "NUM_POLIZA",
        "src_schema": "general",
        "src_table": "anp_pensiones",
        "src_col": "NID_PROCEDENCIA",
        "src_key_cols": ["NID_PENSION"],
        "type": "NUMBER",
        "length": 22,
        "precision": 10,
        "scale": 0,
        "nullable": "N"
    },
    {
        "dest_col": "TIPO_IDENTIFICACION_AFILIADO",
        "src_schema": "general",
        "src_table": "anp_pensionado_novedad",
        "src_col": "COD_TIPO_IDENTIFICACION",
        "src_key_cols": [
            "NID_DISTRIBUCION_VIGENCIA",
            "COD_TIPO_IDENTIFICACION",
            "NID_NUMERO_IDENTIFICACION",
            "COD_CONCEPTO",
            "FEC_INICIAL"
        ],
        "type": "VARCHAR2",
        "length": 3,
        "precision": None,
        "scale": None,
        "nullable": "N"
    },
    {
        "dest_col": "NUM_IDENTIFICACION_AFILIADO",
        "src_schema": "general",
        "src_table": "anp_pensionado_novedad",
        "src_col": "NID_NUMERO_IDENTIFICACION",
        "src_key_cols": [
            "NID_DISTRIBUCION_VIGENCIA",
            "COD_TIPO_IDENTIFICACION",
            "NID_NUMERO_IDENTIFICACION",
            "COD_CONCEPTO",
            "FEC_INICIAL"
        ],
        "type": "NUMBER",
        "length": 22,
        "precision": 15,
        "scale": 0,
        "nullable": "N"
    },
    {
        "dest_col": "FEC_EXPEDICION_PENSION",
        "src_schema": "general",
        "src_table": "anp_pensiones",
        "src_col": "FEC_EXPEDICION",
        "src_key_cols": ["NID_PENSION"],
        "type": "DATE",
        "length": 7,
        "precision": None,
        "scale": None,
        "nullable": "Y"
    },
    {
        "dest_col": "VLR_TOTAL",
        "src_schema": "general",
        "src_table": "anp_pensionado_novedad",
        "src_col": "VLR_TOTAL",
        "src_key_cols": [
            "NID_DISTRIBUCION_VIGENCIA",
            "COD_TIPO_IDENTIFICACION",
            "NID_NUMERO_IDENTIFICACION",
            "COD_CONCEPTO",
            "FEC_INICIAL"
        ],
        "type": "NUMBER",
        "length": 22,
        "precision": 14,
        "scale": 2,
        "nullable": "N"
    },
    {
        "dest_col": "FEC_ENVIO_TESORERIA",
        "src_schema": "general",
        "src_table": "anp_periodo_solicitudes_giro",
        "src_col": "FEC_ENVIO_TES",
        "src_key_cols": ["NID_PERIODO_SOLICITUD_GIRO"],
        "type": "DATE",
        "length": 7,
        "precision": None,
        "scale": None,
        "nullable": "Y"
    },
    {
        "dest_col": "COD_CONCEPTO",
        "src_schema": "general",
        "src_table": "cgr_conceptos",
        "src_col": "COD_CONCEPTO",
        "src_key_cols": ["COD_CONCEPTO"],
        "type": "VARCHAR2",
        "length": 3,
        "precision": None,
        "scale": None,
        "nullable": "N"
    },
    {
        "dest_col": "NOM_CONCEPTO",
        "src_schema": "general",
        "src_table": "cgr_conceptos",
        "src_col": "NOMBRE_CONCEPTO",
        "src_key_cols": ["COD_CONCEPTO"],
        "type": "VARCHAR2",
        "length": 30,
        "precision": None,
        "scale": None,
        "nullable": "N"
    },
    {
        "dest_col": "COD_ORIGEN",
        "src_schema": "general",
        "src_table": "cgr_origenes",
        "src_col": "COD_ORIGEN",
        "src_key_cols": ["COD_ORIGEN"],
        "type": "VARCHAR2",
        "length": 3,
        "precision": None,
        "scale": None,
        "nullable": "N"
    },
    {
        "dest_col": "DSC_ORIGEN",
        "src_schema": "general",
        "src_table": "cgr_origenes",
        "src_col": "NOMBRE",
        "src_key_cols": ["COD_ORIGEN"],
        "type": "VARCHAR2",
        "length": 50,
        "precision": None,
        "scale": None,
        "nullable": "Y"
    }
]

# -------------------------------------------------
# LECTURA BRONZE
# -------------------------------------------------
logger.info("Leyendo tablas Bronze necesarias para validación técnica")

df_periodo_giro = spark.read.parquet(f"{brz_path}general/anp_periodo_solicitudes_giro/")
df_pensiones = spark.read.parquet(f"{brz_path}general/anp_pensiones/")
df_pensionado_novedad = spark.read.parquet(f"{brz_path}general/anp_pensionado_novedad/")
df_conceptos = spark.read.parquet(f"{brz_path}general/cgr_conceptos/")
df_origenes = spark.read.parquet(f"{brz_path}general/cgr_origenes/")

TABLE_MAP = {
    ("general", "anp_periodo_solicitudes_giro"): df_periodo_giro,
    ("general", "anp_pensiones"): df_pensiones,
    ("general", "anp_pensionado_novedad"): df_pensionado_novedad,
    ("general", "cgr_conceptos"): df_conceptos,
    ("general", "cgr_origenes"): df_origenes
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
        if dtype_str.startswith("decimal") or dtype_str in ("bigint", "int", "smallint", "tinyint"):
            return 1
        return 0

    if expected_type == "VARCHAR2":
        return 1 if dtype_str == "string" else 0

    if expected_type == "DATE":
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

def build_llave(key_cols):
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
        .withColumn("LLAVE_REGISTRO", build_llave(key_cols))
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
            .withColumn(
                "VAL_TIPO_DATO",
                when(col("_VAL").isNull(), lit(1)).otherwise(
                    when(col("_VAL").cast("date").isNotNull(), lit(schema_ok)).otherwise(lit(0))
                )
            )
            .withColumn("VAL_LONGITUD_DATO", lit(1))
            .withColumn("VAL_PRECISION_DATO", lit(1))
            .withColumn("VAL_ESCALA_DATO", lit(1))
        )

    return (
        df
        .select(
            "TABLA_ORIGEN",
            "COLUMNA_ORIGEN",
            "COLUMNA_DESTINO",
            "ATRIBUTO",
            "LLAVE_REGISTRO",
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
logger.info("Aplicando validaciones técnicas sobre columnas ORIGEN")

df_detalle = None
for rule in RULES:
    logger.info(
        f"Validando {rule['dest_col']} desde "
        f"{rule['src_schema']}.{rule['src_table']}.{rule['src_col']}"
    )
    df_rule = build_rule_df(rule)
    df_detalle = df_rule if df_detalle is None else df_detalle.unionByName(df_rule)

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
    .parquet(f"{val_path}detalle/")
)

(
    df_resumen.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .parquet(f"{val_path}resumen/")
)

(
    df_detalle
    .filter(col("VAL_RESULTADO_ATRIBUTO") == 0)
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .parquet(f"{val_path}errores/")
)

logger.info(f"Total filas detalle: {df_detalle.count()}")
logger.info(f"Total filas resumen: {df_resumen.count()}")
logger.info(f"Total filas errores: {df_detalle.filter(col('VAL_RESULTADO_ATRIBUTO') == 0).count()}")

logger.info("=== FIN JOB VALIDACION TECNICA AUXILIOS FUNERARIOS ===")
job.commit()