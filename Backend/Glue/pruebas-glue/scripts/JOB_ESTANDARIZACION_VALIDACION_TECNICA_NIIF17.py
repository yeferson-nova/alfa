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
    trim,
    length,
    when,
    regexp_replace,
    regexp_extract,
    min as spark_min,
    current_timestamp
)
from pyspark.sql.types import IntegerType

# -------------------------------------------------
# LOGGING
# -------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------------------------
# HELPERS
# -------------------------------------------------
def ensure_trailing_slash(path: str) -> str:
    return path if path.endswith("/") else path + "/"

def cargar_json_s3(ruta_s3: str) -> dict:
    parsed = urlparse(ruta_s3)
    s3 = boto3.client("s3")
    obj = s3.get_object(
        Bucket=parsed.netloc,
        Key=parsed.path.lstrip("/")
    )
    return json.loads(obj["Body"].read().decode("utf-8"))

# -------------------------------------------------
# PARAMETROS
# -------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "RUTA_CONFIG_VALIDACION"])
ruta_config = args["RUTA_CONFIG_VALIDACION"]

# -------------------------------------------------
# CONTEXTO GLUE
# -------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
config = cargar_json_s3(ruta_config)

nombre_caso = config.get("nombre_caso", "sin_nombre")
ruta_entrada = config["entrada"]["ruta_dataset"]
ruta_salida = ensure_trailing_slash(config["salida"]["ruta_base"])
reglas = config["reglas"]

if not reglas:
    raise ValueError("La configuración no contiene reglas de validación.")

logger.info(f"Nombre caso: {nombre_caso}")
logger.info(f"Ruta entrada: {ruta_entrada}")
logger.info(f"Ruta salida: {ruta_salida}")
logger.info(f"Cantidad de reglas: {len(reglas)}")

# -------------------------------------------------
# LEER DATASET
# -------------------------------------------------
df = spark.read.parquet(ruta_entrada).cache()

total_filas = df.count()
logger.info(f"Total filas dataset: {total_filas}")

columnas_df = set(df.columns)
columnas_reglas = [r["columna"] for r in reglas]
columnas_faltantes = [c for c in columnas_reglas if c not in columnas_df]

if columnas_faltantes:
    raise ValueError(
        f"Las siguientes columnas definidas en reglas no existen en el dataset: {columnas_faltantes}"
    )

# -------------------------------------------------
# HELPERS DE VALIDACION
# -------------------------------------------------
def expr_validacion_nulo(nombre_columna: str, permite_nulo: bool):
    if permite_nulo:
        return lit(1)
    return when(col(nombre_columna).isNull(), lit(0)).otherwise(lit(1))

def expr_validacion_tipo(nombre_columna: str, tipo: str):
    valor_str = trim(col(nombre_columna).cast("string"))
    tipo_norm = (tipo or "").upper()

    if tipo_norm == "STRING":
        return when(col(nombre_columna).isNull(), lit(1)).otherwise(lit(1))

    elif tipo_norm == "INTEGER":
        return when(
            col(nombre_columna).isNull(),
            lit(1)
        ).otherwise(
            when(valor_str.rlike(r"^-?\d+$"), lit(1)).otherwise(lit(0))
        )

    elif tipo_norm == "DECIMAL":
        return when(
            col(nombre_columna).isNull(),
            lit(1)
        ).otherwise(
            when(valor_str.rlike(r"^-?\d+(\.\d+)?$"), lit(1)).otherwise(lit(0))
        )

    else:
        raise ValueError(f"Tipo no soportado para la columna '{nombre_columna}': {tipo}")

def expr_validacion_longitud(nombre_columna: str, longitud_maxima):
    if longitud_maxima is None:
        return lit(1)

    return when(
        col(nombre_columna).isNull(),
        lit(1)
    ).otherwise(
        when(length(col(nombre_columna).cast("string")) <= lit(int(longitud_maxima)), lit(1)).otherwise(lit(0))
    )

def expr_validacion_precision_escala(nombre_columna: str, tipo: str, precision, escala):
    tipo_norm = (tipo or "").upper()

    if tipo_norm != "DECIMAL":
        return lit(1)

    if precision is None and escala is None:
        return lit(1)

    valor_str = trim(col(nombre_columna).cast("string"))

    # validar que sea decimal
    es_decimal = valor_str.rlike(r"^-?\d+(\.\d+)?$")

    # quitar signo
    valor_abs = regexp_replace(valor_str, r"^-", "")

    # parte entera
    parte_entera = regexp_extract(valor_abs, r"^(\d+)", 1)

    # parte decimal
    parte_decimal = regexp_extract(valor_abs, r"^\d+\.(\d+)$", 1)

    longitud_entera = length(parte_entera)
    longitud_decimal = length(parte_decimal)

    # total digitos = enteros + decimales
    total_digitos = longitud_entera + longitud_decimal

    condicion = es_decimal

    if precision is not None:
        condicion = condicion & (total_digitos <= lit(int(precision)))

    if escala is not None:
        condicion = condicion & (longitud_decimal <= lit(int(escala)))

    return when(
        col(nombre_columna).isNull(),
        lit(1)
    ).otherwise(
        when(condicion, lit(1)).otherwise(lit(0))
    )

# -------------------------------------------------
# SI EL DATASET ESTA VACIO
# -------------------------------------------------
if total_filas == 0:
    logger.warning("El dataset está vacío. Se generará salida controlada.")

    resultados = []
    for regla in reglas:
        resultados.append(
            (
                nombre_caso,
                regla["columna"],
                None,               # sin resultado porque no hay datos
                ruta_entrada
            )
        )

    df_final = spark.createDataFrame(
        resultados,
        schema=["nombre_caso", "columna", "resultado", "ruta_dataset"]
    ).withColumn("fecha_validacion", current_timestamp())

# -------------------------------------------------
# SI EL DATASET TIENE DATOS
# -------------------------------------------------
else:
    resultados = []

    for regla in reglas:
        columna = regla["columna"]
        tipo = regla.get("tipo", "STRING")
        nulo = regla.get("nulo", True)
        longitud_max = regla.get("longitud_maxima")
        precision = regla.get("precision")
        escala = regla.get("escala")

        logger.info(
            f"Validando columna={columna}, tipo={tipo}, nulo={nulo}, "
            f"longitud_maxima={longitud_max}, precision={precision}, escala={escala}"
        )

        expr_nulo = expr_validacion_nulo(columna, nulo)
        expr_tipo = expr_validacion_tipo(columna, tipo)
        expr_longitud = expr_validacion_longitud(columna, longitud_max)
        expr_precision_escala = expr_validacion_precision_escala(columna, tipo, precision, escala)

        df_resultado = df.select(
            expr_nulo.alias("val_nulo"),
            expr_tipo.alias("val_tipo"),
            expr_longitud.alias("val_longitud"),
            expr_precision_escala.alias("val_precision_escala")
        ).withColumn(
            "resultado_fila",
            (
                col("val_nulo") *
                col("val_tipo") *
                col("val_longitud") *
                col("val_precision_escala")
            ).cast(IntegerType())
        )

        resultado_columna = (
            df_resultado
            .agg(spark_min("resultado_fila").alias("resultado"))
            .withColumn("nombre_caso", lit(nombre_caso))
            .withColumn("columna", lit(columna))
            .withColumn("ruta_dataset", lit(ruta_entrada))
            .withColumn("fecha_validacion", current_timestamp())
            .select(
                "nombre_caso",
                "columna",
                "resultado",
                "ruta_dataset",
                "fecha_validacion"
            )
        )

        resultados.append(resultado_columna)

    df_final = resultados[0]
    for df_r in resultados[1:]:
        df_final = df_final.unionByName(df_r)

# -------------------------------------------------
# ESCRIBIR SALIDA UNICA
# -------------------------------------------------
df_final.write.mode("overwrite").parquet(f"{ruta_salida}resultado_columna/")

# -------------------------------------------------
# FIN
# -------------------------------------------------
df.unpersist()
job.commit()

logger.info("Validación técnica simplificada finalizada correctamente")
logger.info(f"Salida resultado_columna: {ruta_salida}resultado_columna/")