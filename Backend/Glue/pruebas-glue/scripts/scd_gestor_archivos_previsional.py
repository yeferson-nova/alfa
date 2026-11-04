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
    current_timestamp,
    date_format,
    concat,
    to_timestamp,
    to_date
)

from pyspark.sql.types import (
    IntegerType,
    LongType,
    TimestampType,
    DecimalType
)


# -------------------------------------------------
# LOGGING
# -------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# -------------------------------------------------
# PARÁMETROS
# -------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RUTA_CONFIG_SCD",
        "FECHA_CARGA",
        "LOTE_CARGUE_ID"
    ]
)

ruta_config_scd = args["RUTA_CONFIG_SCD"]
fecha_carga = args["FECHA_CARGA"]
lote_cargue_id = int(args["LOTE_CARGUE_ID"])


# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def asegurar_slash(ruta):
    return ruta if ruta.endswith("/") else ruta + "/"


def cargar_json_s3(ruta_s3):
    parsed = urlparse(ruta_s3)

    s3 = boto3.client("s3")

    obj = s3.get_object(
        Bucket=parsed.netloc,
        Key=parsed.path.lstrip("/")
    )

    return json.loads(
        obj["Body"].read().decode("utf-8")
    )


def aplicar_hash(df, columnas_hash):
    return df.withColumn(
        "HASH",
        sha2(
            concat_ws(
                "||",
                *[
                    coalesce(
                        col(nombre).cast("string"),
                        lit("")
                    )
                    for nombre in columnas_hash
                ]
            ),
            256
        )
    )


def parse_decimal_type(tipo):
    contenido = (
        tipo.lower()
        .replace("decimal", "")
        .replace("(", "")
        .replace(")", "")
        .strip()
    )

    precision, scale = [
        int(value.strip())
        for value in contenido.split(",")
    ]

    return DecimalType(precision, scale)


def cast_column(df, column_name, transformation):
    tipo = str(
        transformation.get(
            "tipo_destino",
            "string"
        )
    ).strip().lower()

    source = col(column_name)

    if tipo in ("int", "integer"):
        return df.withColumn(
            column_name,
            source.cast(IntegerType())
        )

    if tipo in ("bigint", "long"):
        return df.withColumn(
            column_name,
            source.cast(LongType())
        )

    if tipo.startswith("decimal("):
        return df.withColumn(
            column_name,
            source.cast(
                parse_decimal_type(tipo)
            )
        )

    if tipo in (
        "decimal",
        "double",
        "float",
        "numeric"
    ):
        return df.withColumn(
            column_name,
            source.cast(DecimalType(38, 7))
        )

    if tipo == "date":
        formato = transformation.get("formato")

        if formato:
            return df.withColumn(
                column_name,
                to_date(source, formato)
            )

        return df.withColumn(
            column_name,
            source.cast("date")
        )

    if tipo in ("timestamp", "datetime"):
        hora = transformation.get("hora")
        formato = transformation.get(
            "formato",
            "yyyy-MM-dd HH:mm:ss"
        )

        if hora:
            return df.withColumn(
                column_name,
                to_timestamp(
                    concat(
                        date_format(
                            source,
                            "yyyy-MM-dd"
                        ),
                        lit(f" {hora}")
                    ),
                    "yyyy-MM-dd HH:mm:ss"
                )
            )

        return df.withColumn(
            column_name,
            to_timestamp(source, formato)
        )

    if tipo == "boolean":
        return df.withColumn(
            column_name,
            source.cast("boolean")
        )

    return df.withColumn(
        column_name,
        source.cast("string")
    )


def apply_transformations(
    df,
    transformations,
    execution_values
):
    result = df

    for column_name, transformation in (
        transformations.items()
    ):
        column_name = column_name.upper()

        source_type = str(
            transformation.get(
                "origen",
                "columna"
            )
        ).lower()

        if source_type == "parametro":
            parameter_name = transformation.get(
                "parametro"
            )

            if parameter_name not in execution_values:
                raise ValueError(
                    "No existe el parámetro requerido "
                    f"{parameter_name} para {column_name}."
                )

            result = result.withColumn(
                column_name,
                lit(
                    execution_values[
                        parameter_name
                    ]
                )
            )

        result = cast_column(
            result,
            column_name,
            transformation
        )

    return result


def validar_columnas(df, columnas_requeridas, contexto):
    disponibles = {
        nombre.upper()
        for nombre in df.columns
    }

    faltantes = [
        nombre
        for nombre in columnas_requeridas
        if nombre.upper() not in disponibles
    ]

    if faltantes:
        raise ValueError(
            f"Columnas faltantes en {contexto}: "
            + ", ".join(faltantes)
        )


# -------------------------------------------------
# CONTEXTO GLUE
# -------------------------------------------------

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

spark.conf.set(
    "spark.sql.parquet.datetimeRebaseModeInWrite",
    "LEGACY"
)

spark.conf.set(
    "spark.sql.parquet.int96RebaseModeInWrite",
    "LEGACY"
)

spark.conf.set(
    "spark.sql.parquet.datetimeRebaseModeInRead",
    "LEGACY"
)

spark.conf.set(
    "spark.sql.parquet.int96RebaseModeInRead",
    "LEGACY"
)

spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic"
)

spark.conf.set(
    "spark.sql.caseSensitive",
    "false"
)

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# -------------------------------------------------
# CONFIGURACIÓN
# -------------------------------------------------

config = cargar_json_s3(ruta_config_scd)

nombre_caso = config["nombre_caso"]

database_bronze = config["entrada"]["database"]
tabla_bronze = config["entrada"]["tabla"]
columna_particion = config["entrada"]["columna_particion"]

ruta_gold = asegurar_slash(
    config["salida"]["ruta_gold"]
)

columnas_primarias = config["llaves"]["primaria"]
columnas_apertura = config["llaves"].get(
    "apertura",
    []
)

columnas_cambio = config["columnas_cambio"]["normales"]
columnas_alerta = config["columnas_cambio"].get(
    "alerta",
    []
)

columnas_hash = columnas_cambio + columnas_alerta
columnas_union = columnas_primarias + columnas_apertura

if not columnas_union:
    raise ValueError(
        "La configuración SCD no contiene llaves."
    )


# -------------------------------------------------
# COLUMNAS DEL MODELO GOLD
# -------------------------------------------------

columnas_negocio = [
    nombre.upper()
    for nombre in config["columnas_negocio"]
]

columnas_administrativas = [
    nombre.upper()
    for nombre in config.get(
        "columnas_administrativas",
        [
            "FEC_INI_VIG_REG",
            "FEC_FIN_VIG_REG",
            "VRS_REG",
            "ACT_REG"
        ]
    )
]

columnas_gold = (
    columnas_negocio
    + columnas_administrativas
)

transformaciones = {
    nombre.upper(): configuracion
    for nombre, configuracion
    in config.get(
        "transformaciones",
        {}
    ).items()
}

scd_config = config.get("scd", {}) or {}

columna_inicio = scd_config.get(
    "columna_inicio_vigencia",
    "FEC_INI_VIG_REG"
).upper()

columna_fin = scd_config.get(
    "columna_fin_vigencia",
    "FEC_FIN_VIG_REG"
).upper()

columna_version = scd_config.get(
    "columna_version",
    "VRS_REG"
).upper()

columna_activo = scd_config.get(
    "columna_activo",
    "ACT_REG"
).upper()

cerrar_ausentes = bool(
    scd_config.get(
        "cerrar_ausentes",
        True
    )
)


# -------------------------------------------------
# INFORMACIÓN DE EJECUCIÓN
# -------------------------------------------------

logger.info(
    "=== INICIO JOB GOLD SCD GENERICO ==="
)

logger.info(f"Caso                  : {nombre_caso}")
logger.info(f"Database Bronze       : {database_bronze}")
logger.info(f"Tabla Bronze          : {tabla_bronze}")
logger.info(f"Fecha carga           : {fecha_carga}")
logger.info(f"Lote carga            : {lote_cargue_id}")
logger.info(f"Ruta Gold             : {ruta_gold}")
logger.info(f"Llave primaria        : {columnas_primarias}")
logger.info(f"Llave apertura        : {columnas_apertura}")
logger.info(f"Llave de comparación  : {columnas_union}")
logger.info(f"Columnas hash         : {columnas_hash}")


# -------------------------------------------------
# LECTURA BRONZE
# -------------------------------------------------

glue_client = boto3.client("glue")

table_response = glue_client.get_table(
    DatabaseName=database_bronze,
    Name=tabla_bronze
)

ruta_bronze = asegurar_slash(
    table_response[
        "Table"
    ][
        "StorageDescriptor"
    ][
        "Location"
    ]
)

ruta_particion = (
    f"{ruta_bronze}"
    f"{columna_particion}={fecha_carga}/"
)

logger.info(
    "Lectura directa partición Bronze: "
    f"{ruta_particion}"
)

df_bronze = (
    spark.read
    .parquet(ruta_particion)
)

df_bronze = df_bronze.select(
    *[
        col(nombre).alias(nombre.upper())
        for nombre in df_bronze.columns
    ]
)

validar_columnas(
    df_bronze,
    columnas_negocio,
    "partición Bronze"
)

df_entrada = (
    df_bronze
    .select(
        *[
            col(nombre)
            for nombre in columnas_negocio
        ]
    )
)


# -------------------------------------------------
# PREPARACIÓN DE TIPOS
# -------------------------------------------------

execution_values = {
    "FECHA_CARGA": fecha_carga,
    "LOTE_CARGUE_ID": lote_cargue_id
}

df_entrada = apply_transformations(
    df_entrada,
    transformaciones,
    execution_values
).cache()

total_entrada = df_entrada.count()

logger.info(
    f"Total registros entrada Bronze: {total_entrada}"
)

if total_entrada == 0:
    raise ValueError(
        "No existen registros Bronze para "
        f"fecha_carga={fecha_carga}"
    )


# -------------------------------------------------
# VALIDAR DUPLICADOS EN LLAVES
# -------------------------------------------------

duplicados = (
    df_entrada
    .groupBy(*columnas_union)
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicados > 0:
    raise ValueError(
        "La entrada contiene llaves duplicadas para: "
        + ", ".join(columnas_union)
    )


# -------------------------------------------------
# EJECUCIÓN SCD
# -------------------------------------------------

ahora = current_timestamp().cast(TimestampType())

try:
    df_gold_actual = (
        spark.read
        .parquet(ruta_gold)
        .select(*columnas_gold)
        .cache()
    )

    total_gold_anterior = df_gold_actual.count()
    existe_gold = True

    logger.info(
        "Gold existente encontrado. "
        f"Registros: {total_gold_anterior}"
    )

except Exception as error:
    df_gold_actual = None
    total_gold_anterior = 0
    existe_gold = False

    logger.warning(
        "No existe Gold previo. "
        "Se realizará la primera carga. "
        f"Detalle: {str(error)}"
    )


# -------------------------------------------------
# PRIMERA EJECUCIÓN
# -------------------------------------------------

if not existe_gold:
    df_final = (
        df_entrada
        .withColumn(
            columna_inicio,
            ahora
        )
        .withColumn(
            columna_fin,
            lit(None).cast(TimestampType())
        )
        .withColumn(
            columna_version,
            lit(1).cast(IntegerType())
        )
        .withColumn(
            columna_activo,
            lit(1).cast(IntegerType())
        )
        .select(*columnas_gold)
        .cache()
    )

    total_insertados = df_final.count()
    total_sin_cambio = 0
    total_apagados = 0
    total_cerrados = 0
    total_nuevas_versiones = 0


# -------------------------------------------------
# EJECUCIONES POSTERIORES
# -------------------------------------------------

else:
    df_historico = (
        df_gold_actual
        .filter(col(columna_activo) == 0)
    )

    df_activos = (
        df_gold_actual
        .filter(col(columna_activo) == 1)
    )

    # ---------------------------------------------
    # Limitar el SCD a las aperturas presentes
    # en la carga actual.
    # ---------------------------------------------

    if columnas_apertura:
        df_aperturas = (
            df_entrada
            .select(*columnas_apertura)
            .distinct()
        )

        df_activos_alcance = (
            df_activos
            .join(
                df_aperturas,
                columnas_apertura,
                "left_semi"
            )
        )

        df_activos_fuera_alcance = (
            df_activos
            .join(
                df_aperturas,
                columnas_apertura,
                "left_anti"
            )
        )

    else:
        df_activos_alcance = df_activos
        df_activos_fuera_alcance = (
            df_activos.limit(0)
        )

    duplicados_gold = (
        df_activos_alcance
        .groupBy(*columnas_union)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    if duplicados_gold > 0:
        raise ValueError(
            "Gold contiene más de un registro activo "
            "para la misma llave: "
            + ", ".join(columnas_union)
        )

    df_e = (
        aplicar_hash(
            df_entrada,
            columnas_hash
        )
        .withColumn(
            "_ENTRADA_PRESENTE",
            lit(1)
        )
        .alias("e")
    )

    df_c = (
        aplicar_hash(
            df_activos_alcance,
            columnas_hash
        )
        .withColumn(
            "_GOLD_PRESENTE",
            lit(1)
        )
        .alias("c")
    )

    condicion_union = [
        col(f"e.{llave}") == col(f"c.{llave}")
        for llave in columnas_union
    ]

    comparacion = df_e.join(
        df_c,
        condicion_union,
        "full"
    )

    # ---------------------------------------------
    # Registros nuevos
    # ---------------------------------------------

    df_insertar = (
        comparacion
        .filter(
            col("e._ENTRADA_PRESENTE").isNotNull()
            & col("c._GOLD_PRESENTE").isNull()
        )
        .select("e.*")
        .drop("HASH", "_ENTRADA_PRESENTE")
        .withColumn(
            columna_inicio,
            ahora
        )
        .withColumn(
            columna_fin,
            lit(None).cast(TimestampType())
        )
        .withColumn(
            columna_version,
            lit(1).cast(IntegerType())
        )
        .withColumn(
            columna_activo,
            lit(1).cast(IntegerType())
        )
        .select(*columnas_gold)
    )

    # ---------------------------------------------
    # Registros ausentes dentro de la apertura
    # ---------------------------------------------

    df_apagar_base = (
        comparacion
        .filter(
            col("e._ENTRADA_PRESENTE").isNull()
            & col("c._GOLD_PRESENTE").isNotNull()
        )
    )

    if cerrar_ausentes:
        df_apagar_origen = df_apagar_base
    else:
        df_apagar_origen = (
            df_apagar_base.limit(0)
        )

    df_apagar = (
        df_apagar_origen
        .select("c.*")
        .drop("HASH", "_GOLD_PRESENTE")
        .withColumn(
            columna_fin,
            ahora
        )
        .withColumn(
            columna_activo,
            lit(0).cast(IntegerType())
        )
        .select(*columnas_gold)
    )

    # ---------------------------------------------
    # Registros sin cambios
    # ---------------------------------------------

    df_sin_cambio = (
        comparacion
        .filter(
            col("e._ENTRADA_PRESENTE").isNotNull()
            & col("c._GOLD_PRESENTE").isNotNull()
            & (col("e.HASH") == col("c.HASH"))
        )
        .select("c.*")
        .drop("HASH", "_GOLD_PRESENTE")
        .select(*columnas_gold)
    )

    # ---------------------------------------------
    # Cerrar versión anterior
    # ---------------------------------------------

    df_cerrar = (
        comparacion
        .filter(
            col("e._ENTRADA_PRESENTE").isNotNull()
            & col("c._GOLD_PRESENTE").isNotNull()
            & (col("e.HASH") != col("c.HASH"))
        )
        .select("c.*")
        .drop("HASH", "_GOLD_PRESENTE")
        .withColumn(
            columna_fin,
            ahora
        )
        .withColumn(
            columna_activo,
            lit(0).cast(IntegerType())
        )
        .select(*columnas_gold)
    )

    # ---------------------------------------------
    # Crear nueva versión
    # ---------------------------------------------

    df_nueva_version = (
        comparacion
        .filter(
            col("e._ENTRADA_PRESENTE").isNotNull()
            & col("c._GOLD_PRESENTE").isNotNull()
            & (col("e.HASH") != col("c.HASH"))
        )
        .select(
            "e.*",
            col("c.VRS_REG").alias("VRS_ANT")
        )
        .drop("HASH", "_ENTRADA_PRESENTE")
        .withColumn(
            columna_inicio,
            ahora
        )
        .withColumn(
            columna_fin,
            lit(None).cast(TimestampType())
        )
        .withColumn(
            columna_version,
            (
                col("VRS_ANT") + lit(1)
            ).cast(IntegerType())
        )
        .withColumn(
            columna_activo,
            lit(1).cast(IntegerType())
        )
        .drop("VRS_ANT")
        .select(*columnas_gold)
    )

    total_insertados = df_insertar.count()
    total_sin_cambio = df_sin_cambio.count()
    total_apagados = df_apagar.count()
    total_cerrados = df_cerrar.count()
    total_nuevas_versiones = (
        df_nueva_version.count()
    )

    df_final = (
        df_historico
        .unionByName(df_activos_fuera_alcance)
        .unionByName(df_sin_cambio)
        .unionByName(df_apagar)
        .unionByName(df_cerrar)
        .unionByName(df_insertar)
        .unionByName(df_nueva_version)
        .select(*columnas_gold)
        .cache()
    )


# -------------------------------------------------
# ESCRITURA GOLD
# -------------------------------------------------

total_gold_final = df_final.count()

logger.info(f"Insertados nuevos          : {total_insertados}")
logger.info(f"Sin cambios               : {total_sin_cambio}")
logger.info(f"Apagados por ausencia     : {total_apagados}")
logger.info(f"Versiones cerradas        : {total_cerrados}")
logger.info(
    f"Nuevas versiones          : {total_nuevas_versiones}"
)
logger.info(f"Total Gold final          : {total_gold_final}")

(
    df_final.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(ruta_gold)
)

logger.info(
    "=== FIN JOB GOLD SCD GENERICO ==="
)

job.commit()
