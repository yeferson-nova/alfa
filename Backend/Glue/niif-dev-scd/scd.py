import sys
import json
import logging
import boto3
from urllib.parse import urlparse
from datetime import datetime

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
ARGUMENTOS_OPCIONALES = ("PERMITIR_APAGADO_MASIVO", "UMBRAL_APAGADO_PCT")
UMBRAL_APAGADO_PCT_DEFECTO = 20.0

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RUTA_CONFIG_SCD",
        "RUTA_PARAMETROS_EXTRACCION",
    ] + [o for o in ARGUMENTOS_OPCIONALES if f"--{o}" in sys.argv]
)


ruta_config_scd = args["RUTA_CONFIG_SCD"]
ruta_parametros_extraccion = args["RUTA_PARAMETROS_EXTRACCION"]

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

def guardar_json_s3(ruta_s3: str, contenido: dict):
    parsed = urlparse(ruta_s3)
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=parsed.netloc,
        Key=parsed.path.lstrip("/"),
        Body=json.dumps(contenido, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

def inicializar_gold_vacio(spark, df_entrada, ruta_gold):
    """Solo primera carga vacia. Nunca sobrescribe una ruta existente.

    Si la ruta existe pero es ilegible, propaga el error; no asume ausencia.
    El crawler del flujo debe catalogar el Parquet de cero filas generado.
    Requiere la misma exclusion de escritores que el Job original.
    """
    path = spark._jvm.org.apache.hadoop.fs.Path(ruta_gold)
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if filesystem.exists(path):
        # Validar legibilidad antes de conservar. No contar toda la tabla.
        spark.read.parquet(ruta_gold).schema
        return False

    df_vacio = (
        df_entrada.limit(0)
        .withColumn("FEC_INI_VIG_REG", lit(None).cast(TimestampType()))
        .withColumn("FEC_FIN_VIG_REG", lit(None).cast(TimestampType()))
        .withColumn("VRS_REG", lit(None).cast(IntegerType()))
        .withColumn("ACT_REG", lit(None).cast(IntegerType()))
    )
    # Un fichero Parquet con footer y esquema, sin registros ficticios.
    df_vacio.coalesce(1).write.mode("errorifexists").option("compression", "snappy").parquet(ruta_gold)
    verificado = spark.read.parquet(ruta_gold)
    if verificado.limit(1).count() != 0:
        raise ValueError("INICIALIZACION_GOLD_VACIO_CON_REGISTROS")
    if verificado.schema != df_vacio.schema:
        # Parquet puede ampliar nulabilidad al leer; comparar nombres y tipos.
        esperado = [(f.name, f.dataType) for f in df_vacio.schema.fields]
        obtenido = [(f.name, f.dataType) for f in verificado.schema.fields]
        if esperado != obtenido:
            raise ValueError("ESQUEMA_GOLD_VACIO_INCOMPATIBLE")
    return True


# -------------------------------------------------
# FRENO DE APAGADO MASIVO
# El SCD apaga por ausencia lo que no viene en la entrada. Con una entrada parcial
# (carga incremental) apagaria casi todo Gold. Se detiene antes de escribir.
# -------------------------------------------------
class FrenoApagadoMasivo(Exception):
    pass


def leer_umbral_pct(valor) -> float:
    try:
        umbral = float(valor)
    except (TypeError, ValueError):
        raise ValueError(f"UMBRAL_APAGADO_PCT invalido: {valor!r}")
    if not 0 <= umbral <= 100:
        raise ValueError(f"UMBRAL_APAGADO_PCT fuera de 0 a 100: {umbral}")
    return umbral


def evaluar_freno_apagado(total_apagados: int, total_gold_activos: int, umbral_pct: float, permitir_masivo: bool) -> float:
    if total_gold_activos <= 0:
        return 0.0
    porcentaje = 100.0 * total_apagados / total_gold_activos
    if porcentaje > umbral_pct and not permitir_masivo:
        raise FrenoApagadoMasivo(
            f"SCD_APAGADO_MASIVO: se apagarian {total_apagados} de {total_gold_activos} "
            f"registros activos de Gold ({porcentaje:.2f}% > umbral {umbral_pct}%). "
            f"Gold no se modifico."
        )
    return porcentaje


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
spark.conf.set("spark.sql.caseSensitive", "false")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------
# CARGA CONFIGURACION
# -------------------------------------------------
cfg = cargar_json_s3(ruta_config_scd)
parametros_extraccion = cargar_json_s3(ruta_parametros_extraccion)

nombre_caso = cfg["nombre_caso"]
umbral_apagado_pct = leer_umbral_pct(
    cfg.get(
        "umbral_apagado_pct",
        args.get("UMBRAL_APAGADO_PCT", UMBRAL_APAGADO_PCT_DEFECTO),
    )
)
permitir_apagado_masivo = str(args.get("PERMITIR_APAGADO_MASIVO", "false")).lower() == "true"
ruta_gold = ensure_trailing_slash(cfg["salida"]["ruta_gold"])

columnas_llave = cfg["llaves"]["primaria"]
columnas_apertura = cfg["llaves"].get("apertura", [])
columnas_cambio = cfg["columnas_cambio"]["normales"]
columnas_cambio_alerta = cfg["columnas_cambio"].get("alerta", [])

columnas_hash = columnas_cambio + columnas_cambio_alerta
join_keys = columnas_llave + columnas_apertura

lote_cargue_id = parametros_extraccion.get("lote_cargue_id")
estado_validacion = parametros_extraccion.get("estado_validacion")
estado_remediacion = parametros_extraccion.get("estado_remediacion")
estado_disponibilizacion_silver = parametros_extraccion.get("estado_disponibilizacion_silver")
estado_preparacion_silver = parametros_extraccion.get("estado_preparacion_silver")
ruta_silver_preparada = parametros_extraccion.get("ruta_silver_preparada")
requiere_revision_analitica = parametros_extraccion.get("requiere_revision_analitica", False)
estado_hallazgos_validacion = parametros_extraccion.get("estado_hallazgos_validacion")

if lote_cargue_id is None:
    raise ValueError("El JSON de parametros_extraccion no contiene lote_cargue_id.")

# -------------------------------------------------
# NUEVA REGLA:
# La validación puede quedar SUCCESS o FAILED.
# FAILED significa hallazgos de datos, no fallo técnico.
# -------------------------------------------------
if estado_validacion not in ["SUCCESS", "FAILED"]:
    raise ValueError(
        f"Estado de validación no soportado para Gold SCD. estado_validacion={estado_validacion}"
    )

if estado_validacion == "FAILED":
    logger.warning(
        "La validación técnica tiene hallazgos, pero Gold SCD continúa porque "
        "remediación, disponibilización Silver y preparación Silver están en SUCCESS."
    )

if estado_remediacion != "SUCCESS":
    raise ValueError(
        f"Remediación no está en SUCCESS. estado_remediacion={estado_remediacion}"
    )

if estado_disponibilizacion_silver != "SUCCESS":
    raise ValueError(
        f"Disponibilización Silver no está en SUCCESS. "
        f"estado_disponibilizacion_silver={estado_disponibilizacion_silver}"
    )

if estado_preparacion_silver != "SUCCESS":
    raise ValueError(
        f"Preparación Silver no está en SUCCESS. "
        f"estado_preparacion_silver={estado_preparacion_silver}"
    )

if not ruta_silver_preparada:
    raise ValueError("El JSON de parametros_extraccion no contiene ruta_silver_preparada.")

ruta_silver_preparada = ensure_trailing_slash(ruta_silver_preparada)

logger.info("=== INICIO JOB SCD GOLD ESTANDAR NIIF ===")
logger.info(f"Caso de uso                  : {nombre_caso}")
logger.info(f"Ruta Silver preparada        : {ruta_silver_preparada}")
logger.info(f"Ruta Gold                    : {ruta_gold}")
logger.info(f"Ruta parametros extraccion   : {ruta_parametros_extraccion}")
logger.info(f"LOTE_CARGUE_ID               : {lote_cargue_id}")
logger.info(f"Estado validacion            : {estado_validacion}")
logger.info(f"Estado remediacion           : {estado_remediacion}")
logger.info(f"Estado disponibilizacion     : {estado_disponibilizacion_silver}")
logger.info(f"Estado preparacion silver    : {estado_preparacion_silver}")
logger.info(f"Estado hallazgos validacion  : {estado_hallazgos_validacion}")
logger.info(f"Requiere revision analitica  : {requiere_revision_analitica}")
logger.info(f"Columnas llave               : {join_keys}")
logger.info(f"Columnas hash                : {columnas_hash}")

# -------------------------------------------------
# LECTURA SILVER PREPARADA
# Silver preparada es la iteracion actual generada por el job Silver especifico.
# -------------------------------------------------
# La ruta Silver preparada representa la foto completa del caso.
# LOTE_CARGUE_ID conserva la procedencia original de cada registro.
# No se filtra por el lote de la ejecución actual porque la foto puede
# estar conformada por varios lotes reutilizados durante la preparación.
df_entrada = (
    spark.read.parquet(ruta_silver_preparada)
    .cache()
)

total_entrada = df_entrada.count()

logger.info(f"Total registros entrada Silver preparada: {total_entrada}")

# -------------------------------------------------
# HASH DE NEGOCIO
# -------------------------------------------------
def aplicar_hash(df):
    return df.withColumn(
        "HASH",
        sha2(
            concat_ws(
                "||",
                *[coalesce(col(c).cast("string"), lit("")) for c in columnas_hash]
            ),
            256
        )
    )

ahora = current_timestamp().cast(TimestampType())

# -------------------------------------------------
# EJECUCION SCD
# -------------------------------------------------
if total_entrada == 0:
    gold_vacio_inicializado = inicializar_gold_vacio(spark, df_entrada, ruta_gold)
    logger.info("Entrada vacia. Gold inicializado=%s; Gold existente nunca se sobrescribe.", gold_vacio_inicializado)

    parametros_extraccion["estado_gold_scd"] = "SKIPPED"
    parametros_extraccion["estado_consolidacion_gold"] = "SKIPPED"
    parametros_extraccion["motivo_gold_scd"] = (
        "Silver preparada vacia. Gold inicializado sin filas para catalogacion."
        if gold_vacio_inicializado else
        "Silver preparada vacia. Gold existente conservado sin cambios."
    )
    parametros_extraccion["ruta_gold_scd"] = ruta_gold
    parametros_extraccion["gold_vacio_inicializado"] = gold_vacio_inicializado
    parametros_extraccion["fecha_consolidacion_gold"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    parametros_extraccion["metricas_gold_scd"] = {
        "total_entrada_silver_preparada": total_entrada,
        "total_gold_anterior": 0 if gold_vacio_inicializado else None,
        "total_gold_final": 0 if gold_vacio_inicializado else None,
        "total_insertados_nuevos": 0,
        "total_registros_sin_cambio": 0,
        "total_registros_apagados_por_ausencia": 0,
        "total_versiones_cerradas_por_cambio": 0,
        "total_nuevas_versiones_por_cambio": 0
    }

    guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

    logger.info("=== FIN JOB SCD GOLD ESTANDAR NIIF ===")
    logger.info("SCD sin movimientos por entrada vacia; ruta Gold disponible para el crawler.")

else:
    # -------------------------------------------------
    # LEER GOLD EXISTENTE
    # -------------------------------------------------
    try:
        df_gold = spark.read.parquet(ruta_gold).cache()
        total_gold_actual = df_gold.count()
        existe_gold = True
        logger.info(f"Gold existente encontrado. Registros actuales: {total_gold_actual}")
    except Exception as e:
        existe_gold = False
        total_gold_actual = 0
        logger.warning(
            f"No existe Gold previo o no pudo leerse. "
            f"Se asumirá primera ejecución. Detalle: {str(e)}"
        )

    # -------------------------------------------------
    # PRIMERA EJECUCION
    # -------------------------------------------------
    if not existe_gold:
        logger.info("Primera ejecución Gold. Se insertan todos los registros como activos.")

        df_salida = (
            df_entrada
            .withColumn("FEC_INI_VIG_REG", ahora)
            .withColumn("FEC_FIN_VIG_REG", lit(None).cast(TimestampType()))
            .withColumn("VRS_REG", lit(1).cast(IntegerType()))
            .withColumn("ACT_REG", lit(1).cast(IntegerType()))
            .cache()
        )

        total_gold_final = df_salida.count()

        (
            df_salida.write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(ruta_gold)
        )

        total_insertados = total_gold_final
        total_sin_cambio = 0
        total_cerrados_cambio = 0
        total_nuevas_versiones = 0
        total_apagados = 0

    # -------------------------------------------------
    # SCD TIPO 2
    # -------------------------------------------------
    else:
        logger.info("Ejecutando SCD Tipo 2 sobre Gold existente.")

        columnas_gold_base = [c for c in df_gold.columns if c != "HASH"]

        df_gold_act = df_gold.filter(col("ACT_REG") == 1).cache()
        df_gold_hist = df_gold.filter(col("ACT_REG") == 0).select(*columnas_gold_base).cache()

        total_gold_act = df_gold_act.count()
        total_gold_hist = df_gold_hist.count()

        logger.info(f"Gold activos actuales       : {total_gold_act}")
        logger.info(f"Gold históricos actuales    : {total_gold_hist}")

        df_e = aplicar_hash(df_entrada).alias("e")
        df_c = aplicar_hash(df_gold_act).alias("c")

        join_cond = [col(f"e.{k}") == col(f"c.{k}") for k in join_keys]

        j = df_e.join(df_c, join_cond, "full")

        llave_ref = columnas_llave[0]

        # -------------------------------------------------
        # NUEVOS: existen en entrada, no existen activos en Gold
        # -------------------------------------------------
        df_insertar = (
            j.filter(col(f"c.{llave_ref}").isNull())
            .select("e.*")
            .drop("HASH")
            .withColumn("FEC_INI_VIG_REG", ahora)
            .withColumn("FEC_FIN_VIG_REG", lit(None).cast(TimestampType()))
            .withColumn("VRS_REG", lit(1).cast(IntegerType()))
            .withColumn("ACT_REG", lit(1).cast(IntegerType()))
        )

        # -------------------------------------------------
        # APAGADOS: existen activos en Gold, ya no vienen en la foto actual
        # -------------------------------------------------
        df_apagar = (
            j.filter(col(f"e.{llave_ref}").isNull())
            .select("c.*")
            .drop("HASH")
            .withColumn("FEC_FIN_VIG_REG", ahora)
            .withColumn("ACT_REG", lit(0).cast(IntegerType()))
            .select(*columnas_gold_base)
        )

        # -------------------------------------------------
        # SIN CAMBIO: existe en ambos y hash igual
        # -------------------------------------------------
        df_sin_cambio = (
            j.filter(
                col(f"e.{llave_ref}").isNotNull() &
                col(f"c.{llave_ref}").isNotNull() &
                (col("e.HASH") == col("c.HASH"))
            )
            .select("c.*")
            .drop("HASH")
            .select(*columnas_gold_base)
        )

        # -------------------------------------------------
        # CERRAR VERSION ANTERIOR: existe en ambos, pero hash cambió
        # -------------------------------------------------
        df_cerrar = (
            j.filter(
                col(f"e.{llave_ref}").isNotNull() &
                col(f"c.{llave_ref}").isNotNull() &
                (col("e.HASH") != col("c.HASH"))
            )
            .select("c.*")
            .drop("HASH")
            .withColumn("FEC_FIN_VIG_REG", ahora)
            .withColumn("ACT_REG", lit(0).cast(IntegerType()))
            .select(*columnas_gold_base)
        )

        # -------------------------------------------------
        # NUEVA VERSION: versión nueva activa de registros modificados
        # -------------------------------------------------
        df_nueva_version = (
            j.filter(
                col(f"e.{llave_ref}").isNotNull() &
                col(f"c.{llave_ref}").isNotNull() &
                (col("e.HASH") != col("c.HASH"))
            )
            .select("e.*", col("c.VRS_REG").alias("VRS_ANT"))
            .drop("HASH")
            .withColumn("FEC_INI_VIG_REG", ahora)
            .withColumn("FEC_FIN_VIG_REG", lit(None).cast(TimestampType()))
            .withColumn("VRS_REG", (col("VRS_ANT") + 1).cast(IntegerType()))
            .withColumn("ACT_REG", lit(1).cast(IntegerType()))
            .drop("VRS_ANT")
        )

        total_insertados = df_insertar.count()
        total_apagados = df_apagar.count()
        total_sin_cambio = df_sin_cambio.count()
        total_cerrados_cambio = df_cerrar.count()
        total_nuevas_versiones = df_nueva_version.count()

        logger.info(f"Registros nuevos insertados      : {total_insertados}")
        logger.info(f"Registros apagados por ausencia  : {total_apagados}")
        logger.info(f"Registros sin cambio             : {total_sin_cambio}")
        logger.info(f"Versiones cerradas por cambio    : {total_cerrados_cambio}")
        logger.info(f"Nuevas versiones por cambio      : {total_nuevas_versiones}")

        try:
            porcentaje_apagado = evaluar_freno_apagado(
                total_apagados, total_gold_act, umbral_apagado_pct, permitir_apagado_masivo
            )
        except FrenoApagadoMasivo as e:
            logger.error(str(e))
            parametros_extraccion["estado_gold_scd"] = "FAILED"
            parametros_extraccion["estado_consolidacion_gold"] = "FAILED"
            parametros_extraccion["motivo_gold_scd"] = str(e)
            guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)
            raise
        logger.info(f"Porcentaje apagado por ausencia  : {porcentaje_apagado:.2f}% (umbral {umbral_apagado_pct}%)")

        df_final = (
            df_gold_hist
            .unionByName(df_sin_cambio)
            .unionByName(df_apagar)
            .unionByName(df_cerrar)
            .unionByName(df_insertar)
            .unionByName(df_nueva_version)
            .cache()
        )

        total_gold_final = df_final.count()

        logger.info(f"Total registros Gold final reconstruido: {total_gold_final}")

        # Se escribe la tabla Gold completa reconstruida,
        # conservando histórico lógico SCD.
        (
            df_final.write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(ruta_gold)
        )

    # -------------------------------------------------
    # ACTUALIZAR PARAMETROS_EXTRACCION
    # -------------------------------------------------
    parametros_extraccion["estado_gold_scd"] = "SUCCESS"
    parametros_extraccion.pop("motivo_gold_scd", None)
    parametros_extraccion["estado_consolidacion_gold"] = "SUCCESS"
    parametros_extraccion["ruta_gold_scd"] = ruta_gold
    parametros_extraccion["modo_escritura_gold_scd"] = "overwrite_controlado"
    parametros_extraccion["gold_scd_historico"] = True
    parametros_extraccion["fecha_consolidacion_gold"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    parametros_extraccion["gold_scd"] = {
        "estado": "SUCCESS",
        "nombre_caso": nombre_caso,
        "ruta_gold": ruta_gold,
        "modo_escritura": "overwrite_controlado",
        "historico_scd": True,
        "lote_cargue_id": lote_cargue_id,
        "estado_validacion": estado_validacion,
        "estado_hallazgos_validacion": estado_hallazgos_validacion,
        "requiere_revision_analitica": requiere_revision_analitica,
        "fecha_consolidacion": parametros_extraccion["fecha_consolidacion_gold"]
    }

    parametros_extraccion["metricas_gold_scd"] = {
        "total_entrada_silver_preparada": total_entrada,
        "total_gold_anterior": total_gold_actual,
        "total_gold_final": total_gold_final,
        "total_insertados_nuevos": total_insertados,
        "total_registros_sin_cambio": total_sin_cambio,
        "total_registros_apagados_por_ausencia": total_apagados,
        "total_versiones_cerradas_por_cambio": total_cerrados_cambio,
        "total_nuevas_versiones_por_cambio": total_nuevas_versiones
    }

    guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

    logger.info("parametros-extraccion.json actualizado con estado_gold_scd SUCCESS.")
    logger.info("=== FIN JOB SCD GOLD ESTANDAR NIIF ===")
    logger.info(f"SCD Gold finalizado correctamente para el caso: {nombre_caso}")

# -------------------------------------------------
# FIN
# -------------------------------------------------
job.commit()