import sys
import json
import logging
import boto3
from urllib.parse import urlparse
from datetime import datetime, timedelta

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    col,
    lit,
    lower,
    when,
    date_format,
    collect_set,
    struct,
    size
)
from pyspark.sql.types import (
    LongType,
    IntegerType,
    DecimalType,
    StringType
)

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


def guardar_json_s3(ruta_s3: str, contenido: dict):
    parsed = urlparse(ruta_s3)
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=parsed.netloc,
        Key=parsed.path.lstrip("/"),
        Body=json.dumps(contenido, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json"
    )


def normalizar_tabla_relativa(tabla_relativa: str) -> str:
    return str(tabla_relativa).lower().strip().strip("/")


def obtener_segmento_preparacion(parametros_extraccion: dict, tabla_relativa: str):
    tabla_relativa_norm = normalizar_tabla_relativa(tabla_relativa)

    for segmento in parametros_extraccion.get("segmentos_preparacion", []):
        if normalizar_tabla_relativa(segmento.get("tabla_relativa")) == tabla_relativa_norm:
            return segmento

    return None


def obtener_lotes_preparacion(
    parametros_extraccion: dict,
    tabla_relativa: str,
    dataset: dict,
    lote_actual
):
    """
    Nuevo estándar:
    1. Si existe segmentos_preparacion para la tabla, usar lotes_segmento de ahí.
    2. Si no existe, pero el dataset viene segmentado, usar dataset.lotes_segmento.
    3. Si no aplica segmentación, usar lote_actual.

    Esto permite preparar correctamente cuando una tabla compartida
    tiene universo compuesto por varios lotes.
    """
    if "_lotes_lectura_fijados" in dataset:
        return list(dataset["_lotes_lectura_fijados"])
    segmento = obtener_segmento_preparacion(
        parametros_extraccion=parametros_extraccion,
        tabla_relativa=tabla_relativa
    )

    if segmento:
        lotes = segmento.get("lotes_segmento") or []
        if lotes:
            return sorted([int(x) for x in lotes if x is not None])

    segmentada = bool(dataset.get("segmentacion_horizontal_habilitada", False))

    if segmentada:
        lotes = dataset.get("lotes_segmento") or [lote_actual]
    else:
        lotes = [lote_actual]

    return sorted([int(x) for x in lotes if x is not None])


def leer_silver_base(spark, ruta, parametros_extraccion, tabla_relativa, dataset, lote_actual, nombre_tabla):
    # Patron RVI/ARP: segmentos declarados o maximo lote fisico por tabla.
    # Seleccion fijada para esta invocacion; no es un snapshot transaccional.
    from pyspark.sql.functions import col, max as spark_max
    df = spark.read.parquet(ruta)
    nombres = [c for c in df.columns if c.upper() == "LOTE_CARGUE_ID"]
    if len(nombres) != 1:
        raise ValueError("COLUMNA_LOTE_AUSENTE_O_AMBIGUA: " + tabla_relativa)
    lote_col = col(nombres[0]).cast("bigint")
    segmentada = bool(dataset.get("segmentacion_horizontal_habilitada", False))
    if segmentada:
        # No sustituir un segmento ausente por el ultimo lote.
        dataset.pop("_lotes_lectura_fijados", None)
        lotes = obtener_lotes_preparacion(parametros_extraccion, tabla_relativa, dataset, lote_actual)
        criterio = "LOTES_SEGMENTO"
    else:
        ultimo = df.agg(spark_max(lote_col).alias("ultimo")).first()["ultimo"]
        lotes = [] if ultimo is None else [int(ultimo)]
        criterio = "MAXIMO_LOTE_FISICO"
    dataset["_lotes_lectura_fijados"] = list(lotes)
    parametros_extraccion.setdefault("lotes_usados_preparacion_silver", {})[tabla_relativa] = list(lotes)
    logger.info("Leyendo %s. segmentada=%s criterio=%s lotes_preparacion=%s", nombre_tabla, segmentada, criterio, lotes)
    return df.filter(lote_col.isin(lotes))


# -------------------------------------------------
# PARAMETROS
# -------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_SLV_PREP_PATH",
        "RUTA_PARAMETROS_EXTRACCION"
    ]
)

slv_prep = ensure_trailing_slash(args["S3_SLV_PREP_PATH"])
ruta_parametros_extraccion = args["RUTA_PARAMETROS_EXTRACCION"]

logger.info(f"SILVER PREP PATH           : {slv_prep}")
logger.info(f"RUTA PARAMETROS EXTRACCION : {ruta_parametros_extraccion}")


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
# CARGA PARAMETROS_EXTRACCION
# -------------------------------------------------
parametros_extraccion = cargar_json_s3(ruta_parametros_extraccion)

lote_cargue_id = parametros_extraccion.get("lote_cargue_id")
estado_validacion = parametros_extraccion.get("estado_validacion")
estado_remediacion = parametros_extraccion.get("estado_remediacion")
estado_disponibilizacion_silver = parametros_extraccion.get("estado_disponibilizacion_silver")
requiere_revision_analitica = parametros_extraccion.get("requiere_revision_analitica", False)
estado_hallazgos_validacion = parametros_extraccion.get("estado_hallazgos_validacion")
datasets_extraidos = parametros_extraccion.get("datasets_extraidos", [])

if lote_cargue_id is None:
    raise ValueError("El JSON de parametros_extraccion no contiene lote_cargue_id.")

if estado_validacion not in ["SUCCESS", "FAILED"]:
    raise ValueError(
        f"Estado de validación no soportado para Silver. estado_validacion={estado_validacion}"
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

if not datasets_extraidos:
    raise ValueError("El JSON de parametros_extraccion no contiene datasets_extraidos.")

if estado_validacion == "FAILED":
    logger.warning(
        "La validación técnica tiene hallazgos, pero el flujo continúa porque "
        "estado_remediacion y estado_disponibilizacion_silver están en SUCCESS."
    )

mapa_parametros = {
    normalizar_tabla_relativa(d["tabla_relativa"]): d
    for d in datasets_extraidos
}

tablas_requeridas = [
    "prv/prv_rentas",
    "prv/prv_endosos_point",
    "general/cgr_origenes",
    "general/cgr_tipos_endosos"
]

for tabla_req in tablas_requeridas:
    if tabla_req not in mapa_parametros:
        raise ValueError(f"La tabla requerida '{tabla_req}' no aparece en parametros_extraccion.")

logger.info(f"LOTE_CARGUE_ID                   : {lote_cargue_id}")
logger.info(f"ESTADO VALIDACION                : {estado_validacion}")
logger.info(f"ESTADO REMEDIACION               : {estado_remediacion}")
logger.info(f"ESTADO DISPONIBILIZACION SILVER  : {estado_disponibilizacion_silver}")
logger.info(f"ESTADO HALLAZGOS VALIDACION      : {estado_hallazgos_validacion}")
logger.info(f"REQUIERE REVISION ANALITICA      : {requiere_revision_analitica}")


# -------------------------------------------------
# DATASETS PARAMETROS
# -------------------------------------------------
ds_rentas = mapa_parametros["prv/prv_rentas"]
ds_endosos = mapa_parametros["prv/prv_endosos_point"]
ds_origenes = mapa_parametros["general/cgr_origenes"]
ds_tipos_endoso = mapa_parametros["general/cgr_tipos_endosos"]


# -------------------------------------------------
# RUTAS SILVER BASE
# Estas rutas ya deben haber sido disponibilizadas por remediación.
# -------------------------------------------------
ruta_rentas = ensure_trailing_slash(ds_rentas["ruta_dataset_destino"])
ruta_endosos = ensure_trailing_slash(ds_endosos["ruta_dataset_destino"])
ruta_origenes = ensure_trailing_slash(ds_origenes["ruta_dataset_destino"])
ruta_tipos_endoso = ensure_trailing_slash(ds_tipos_endoso["ruta_dataset_destino"])

logger.info(f"Ruta PRV_RENTAS        : {ruta_rentas}")
logger.info(f"Ruta PRV_ENDOSOS_POINT : {ruta_endosos}")
logger.info(f"Ruta CGR_ORIGENES      : {ruta_origenes}")
logger.info(f"Ruta CGR_TIPOS_ENDOSOS : {ruta_tipos_endoso}")


# -------------------------------------------------
# LECTURA DESDE SILVER BASE
# Nuevo estándar:
# - Primero usa segmentos_preparacion.lotes_segmento.
# - Fallback a dataset.lotes_segmento.
# - Fallback final a lote_cargue_id.
# -------------------------------------------------
df_rentas = leer_silver_base(
    spark=spark,
    ruta=ruta_rentas,
    parametros_extraccion=parametros_extraccion,
    tabla_relativa="prv/prv_rentas",
    dataset=ds_rentas,
    lote_actual=lote_cargue_id,
    nombre_tabla="prv/prv_rentas"
)

df_endosos = leer_silver_base(
    spark=spark,
    ruta=ruta_endosos,
    parametros_extraccion=parametros_extraccion,
    tabla_relativa="prv/prv_endosos_point",
    dataset=ds_endosos,
    lote_actual=lote_cargue_id,
    nombre_tabla="prv/prv_endosos_point"
)

df_origenes = leer_silver_base(
    spark=spark,
    ruta=ruta_origenes,
    parametros_extraccion=parametros_extraccion,
    tabla_relativa="general/cgr_origenes",
    dataset=ds_origenes,
    lote_actual=lote_cargue_id,
    nombre_tabla="general/cgr_origenes"
)

df_tipos_endoso = leer_silver_base(
    spark=spark,
    ruta=ruta_tipos_endoso,
    parametros_extraccion=parametros_extraccion,
    tabla_relativa="general/cgr_tipos_endosos",
    dataset=ds_tipos_endoso,
    lote_actual=lote_cargue_id,
    nombre_tabla="general/cgr_tipos_endosos"
)


# -------------------------------------------------
# VALIDACION DE INSUMOS
# -------------------------------------------------
total_rentas = df_rentas.count()
total_endosos = df_endosos.count()
total_origenes = df_origenes.count()
total_tipos_endoso = df_tipos_endoso.count()

logger.info(f"Registros PRV_RENTAS preparación        : {total_rentas}")
logger.info(f"Registros PRV_ENDOSOS_POINT preparación : {total_endosos}")
logger.info(f"Registros CGR_ORIGENES preparación      : {total_origenes}")
logger.info(f"Registros CGR_TIPOS_ENDOSOS preparación : {total_tipos_endoso}")

if total_rentas == 0:
    raise ValueError(
        "No hay registros en Silver base para prv/prv_rentas "
        "usando los lotes de preparación."
    )

if total_endosos == 0:
    raise ValueError(
        "No hay registros en Silver base para prv/prv_endosos_point "
        "usando los lotes de preparación."
    )


# -------------------------------------------------
# PREPARACION SILVER
# -------------------------------------------------
periodo_preparacion = parametros_extraccion.get("periodo_negocio")
if not isinstance(periodo_preparacion, str) or len(periodo_preparacion) != 6 or not periodo_preparacion.isdigit():
    raise ValueError("periodo_negocio debe ser texto YYYYMM")
inicio_periodo = datetime.strptime(periodo_preparacion, "%Y%m")
siguiente_periodo = datetime(
    inicio_periodo.year + (inicio_periodo.month == 12),
    1 if inicio_periodo.month == 12 else inicio_periodo.month + 1, 1
)
fin_periodo_sql = siguiente_periodo - timedelta(seconds=1)
logger.info("Periodo preparacion=%s; FEC_EXPEDICION entre %s y %s", periodo_preparacion, inicio_periodo, fin_periodo_sql)

# Seleccion mensual en preparacion, no en extraccion.
df_base = (
    df_endosos.alias("e")
    .filter(col("e.FEC_EXPEDICION").between(lit(inicio_periodo), lit(fin_periodo_sql)))
    .join(
        df_rentas.alias("r"),
        col("e.NID_RENTAS") == col("r.NID_RENTAS"),
        "inner"
    )
    .join(
        df_origenes.alias("o"),
        col("r.COD_ORIGEN") == col("o.COD_ORIGEN"),
        "inner"
    )
    .join(
        df_tipos_endoso.alias("t"),
        col("e.COD_TIPO_ENDOSO") == col("t.COD_TIPO_ENDOSO"),
        "inner"
    )
)


# -------------------------------------------------
# ATRIBUTOS PREPARADOS
# -------------------------------------------------
df_preparado = (
    df_base
    .withColumn("BLN_PROCESO_L", lower(col("e.BLN_PROCESO")))
    .withColumn("RAMO_POINT_N", col("r.RAMO_POINT").cast(IntegerType()))
    .withColumn("PER_EXPEDICION_RENTA", date_format(col("r.FEC_EXPEDICION"), "yyyyMM"))
    .withColumn("PER_EXPEDICION_ENDOSO", date_format(col("e.FEC_EXPEDICION"), "yyyyMM"))
    .withColumn("PER_ENVIO_POINT_ENDOSO", date_format(col("e.FEC_ENVIO_POINT"), "yyyyMM"))
    .withColumn(
        "COD_DVLCN_RNTGR",
        when(
            (col("BLN_PROCESO_L") == "s") &
            col("e.NID_ENDOSO_REVERSION").isNull() &
            col("e.NID_ENDOSO_REVERSADO").isNull() &
            (col("e.VLR_TOTAL") != col("e.VLR_TOTAL_RECAUDO")),
            lit(1)
        ).when(
            (col("BLN_PROCESO_L") == "s") &
            col("e.NID_ENDOSO_REVERSION").isNull() &
            col("e.NID_ENDOSO_REVERSADO").isNull() &
            (col("e.VLR_TOTAL") == col("e.VLR_TOTAL_RECAUDO")) &
            (col("e.COD_TIPO_ENDOSO") == lit(2)),
            lit(2)
        ).otherwise(lit(0))
    )
    .withColumn(
        "DSC_DVLCN_RNTGR",
        when(col("COD_DVLCN_RNTGR") == 1, lit("Devolucion"))
        .when(col("COD_DVLCN_RNTGR") == 2, lit("Reintegro"))
        .otherwise(lit("Ninguno"))
    )
)


# -------------------------------------------------
# FILTRO FUNCIONAL
# SOLO DEVOLUCION / REINTEGRO
# -------------------------------------------------
df_preparado = df_preparado.filter(col("COD_DVLCN_RNTGR").isin(1, 2))


# -------------------------------------------------
# SELECCION FINAL SILVER PREPARADA
# -------------------------------------------------
df_silver = (
    df_preparado
    .select(
        col("r.NID_RENTAS").cast(LongType()).alias("NID_RENTA"),
        col("e.NID_ENDOSO_POINT").cast(LongType()).alias("NID_ENDOSO"),
        col("r.RAMO_POINT").cast(StringType()).alias("RAMO_POINT_T"),
        col("RAMO_POINT_N").cast(IntegerType()).alias("RAMO_POINT_N"),
        col("r.FEC_EXPEDICION").cast("timestamp").alias("FEC_EXPEDICION_RENTA"),
        col("PER_EXPEDICION_RENTA").cast(StringType()).alias("PER_EXPEDICION_RENTA"),
        col("e.FEC_EXPEDICION").cast("timestamp").alias("FEC_EXPEDICION_ENDOSO"),
        col("PER_EXPEDICION_ENDOSO").cast(StringType()).alias("PER_EXPEDICION_ENDOSO"),
        col("e.FEC_ENVIO_POINT").cast("timestamp").alias("FEC_ENVIO_POINT_ENDOSO"),
        col("PER_ENVIO_POINT_ENDOSO").cast(StringType()).alias("PER_ENVIO_POINT_ENDOSO"),
        col("e.VLR_TOTAL").alias("VLR_ENDOSO"),
        col("e.VLR_TOTAL_RECAUDO").alias("VLR_RECAUDO"),
        col("e.NID_ENDOSO_REVERSION").cast(LongType()).alias("NID_ENDOSO_REVERSION"),
        col("e.NID_ENDOSO_REVERSADO").cast(LongType()).alias("NID_ENDOSO_REVERSADO"),
        col("e.BLN_PROCESO").cast(StringType()).alias("BLN_PROCESO"),
        col("BLN_PROCESO_L").cast(StringType()).alias("BLN_PROCESO_L"),
        col("r.COD_ORIGEN").cast(LongType()).alias("COD_TIPO_RENTA"),
        col("o.NOMBRE").cast(StringType()).alias("DSC_TIPO_RENTA"),
        col("e.COD_TIPO_ENDOSO").cast(LongType()).alias("COD_TIPO_ENDOSO"),
        col("t.NOMBRE_TIPO").cast(StringType()).alias("DSC_TIPO_ENDOSO"),
        col("COD_DVLCN_RNTGR").cast(IntegerType()).alias("COD_DVLCN_RNTGR"),
        col("DSC_DVLCN_RNTGR").cast(StringType()).alias("DSC_DVLCN_RNTGR"),
        col("e.LOTE_CARGUE_ID").cast(LongType()).alias("LOTE_CARGUE_ID")
    )

)

# El GROUP BY del SQL no contiene NID_ENDOSO ni LOTE_CARGUE_ID.
# Conservar los atributos funcionales y verificar que el identificador tecnico
# sea univoco por grupo. No elegir min/first de forma arbitraria para SCD.
columnas_funcionales = [c for c in df_silver.columns if c not in ("NID_ENDOSO", "LOTE_CARGUE_ID")]
columnas_salida = df_silver.columns
agrupado = df_silver.groupBy(*columnas_funcionales).agg(
    collect_set(struct(col("NID_ENDOSO"), col("LOTE_CARGUE_ID"))).alias("_REFERENCIAS")
)
if agrupado.filter(size(col("_REFERENCIAS")) != 1).limit(1).count():
    raise ValueError(
        "GRUPO_SQL_CON_VARIAS_REFERENCIAS: el SQL agrupa endosos/lotes distintos; "
        "no se publica una identidad SCD arbitraria. Revisar granularidad."
    )
df_silver = (
    agrupado
    .withColumn("NID_ENDOSO", col("_REFERENCIAS")[0]["NID_ENDOSO"])
    .withColumn("LOTE_CARGUE_ID", col("_REFERENCIAS")[0]["LOTE_CARGUE_ID"])
    .drop("_REFERENCIAS")
    .select(*columnas_salida)
)

total_silver = df_silver.count()

logger.info(f"Total registros Silver preparada Primas Endosos: {total_silver}")


# -------------------------------------------------
# ESCRITURA SILVER PREPARADA
# Silver preparada representa la foto actual lista para Gold.
# El histórico de cambios se conserva en Gold SCD.
# -------------------------------------------------
target_path = slv_prep

logger.info(f"Escribiendo Silver preparada en modo overwrite: {target_path}")

(
    df_silver
    .write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(target_path)
)


# -------------------------------------------------
# ACTUALIZAR PARAMETROS_EXTRACCION
# -------------------------------------------------
fecha_preparacion = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

lotes_usados_preparacion = {
    "prv/prv_rentas": obtener_lotes_preparacion(
        parametros_extraccion,
        "prv/prv_rentas",
        ds_rentas,
        lote_cargue_id
    ),
    "prv/prv_endosos_point": obtener_lotes_preparacion(
        parametros_extraccion,
        "prv/prv_endosos_point",
        ds_endosos,
        lote_cargue_id
    ),
    "general/cgr_origenes": obtener_lotes_preparacion(
        parametros_extraccion,
        "general/cgr_origenes",
        ds_origenes,
        lote_cargue_id
    ),
    "general/cgr_tipos_endosos": obtener_lotes_preparacion(
        parametros_extraccion,
        "general/cgr_tipos_endosos",
        ds_tipos_endoso,
        lote_cargue_id
    )
}

parametros_extraccion["estado_preparacion_silver"] = "SUCCESS"
parametros_extraccion["ruta_silver_preparada"] = target_path
parametros_extraccion["modo_escritura_silver_preparada"] = "overwrite"
parametros_extraccion["silver_preparada_historica"] = False
parametros_extraccion["total_registros_silver_preparada"] = total_silver
parametros_extraccion["fecha_preparacion_silver"] = fecha_preparacion
parametros_extraccion["lotes_usados_preparacion_silver"] = lotes_usados_preparacion

parametros_extraccion["silver_preparacion"] = {
    "estado": "SUCCESS",
    "nombre_caso": "primas_endosos",
    "ruta_silver_preparada": target_path,
    "modo_escritura": "overwrite",
    "total_registros": total_silver,
    "lote_cargue_id": lote_cargue_id,
    "lotes_usados_preparacion": lotes_usados_preparacion,
    "estado_validacion": estado_validacion,
    "requiere_revision_analitica": requiere_revision_analitica,
    "estado_hallazgos_validacion": estado_hallazgos_validacion,
    "fecha_preparacion": fecha_preparacion
}

guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

logger.info("parametros_extraccion.json actualizado con estado_preparacion_silver SUCCESS.")
logger.info("=== FIN SILVER PREPARADA PRIMAS ENDOSOS ===")

job.commit()