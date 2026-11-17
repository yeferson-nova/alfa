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
    concat,
    substring,
    to_date,
    last_day,
    date_format,
    when,
    max as spark_max
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

    Esto permite que primas_rentas prepare correctamente el universo cuando:
    - una tabla ya fue extraída por otro caso de uso,
    - la extracción actual quedó SKIPPED,
    - o el universo funcional se compone de varios lotes.
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
    "general/anp_movimientos_pago",
    "general/anp_distribucion_vig_pension",
    "general/anp_pensiones",
    "prv/prv_rentas",
    "prv/prv_saldos_matematica",
    "general/cgr_origenes",
    "general/cgr_conceptos"
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
ds_movimientos_pago = mapa_parametros["general/anp_movimientos_pago"]
ds_distribucion_vig = mapa_parametros["general/anp_distribucion_vig_pension"]
ds_pensiones = mapa_parametros["general/anp_pensiones"]
ds_rentas = mapa_parametros["prv/prv_rentas"]
ds_saldos_matematica = mapa_parametros["prv/prv_saldos_matematica"]
ds_origenes = mapa_parametros["general/cgr_origenes"]
ds_conceptos = mapa_parametros["general/cgr_conceptos"]


# -------------------------------------------------
# RUTAS SILVER BASE
# Estas rutas ya deben haber sido disponibilizadas por remediación.
# -------------------------------------------------
ruta_movimientos_pago = ensure_trailing_slash(ds_movimientos_pago["ruta_dataset_destino"])
ruta_distribucion_vig = ensure_trailing_slash(ds_distribucion_vig["ruta_dataset_destino"])
ruta_pensiones = ensure_trailing_slash(ds_pensiones["ruta_dataset_destino"])
ruta_rentas = ensure_trailing_slash(ds_rentas["ruta_dataset_destino"])
ruta_saldos_matematica = ensure_trailing_slash(ds_saldos_matematica["ruta_dataset_destino"])
ruta_origenes = ensure_trailing_slash(ds_origenes["ruta_dataset_destino"])
ruta_conceptos = ensure_trailing_slash(ds_conceptos["ruta_dataset_destino"])

logger.info(f"Ruta ANP_MOVIMIENTOS_PAGO         : {ruta_movimientos_pago}")
logger.info(f"Ruta ANP_DISTRIBUCION_VIG_PENSION : {ruta_distribucion_vig}")
logger.info(f"Ruta ANP_PENSIONES                : {ruta_pensiones}")
logger.info(f"Ruta PRV_RENTAS                   : {ruta_rentas}")
logger.info(f"Ruta PRV_SALDOS_MATEMATICA        : {ruta_saldos_matematica}")
logger.info(f"Ruta CGR_ORIGENES                 : {ruta_origenes}")
logger.info(f"Ruta CGR_CONCEPTOS                : {ruta_conceptos}")


# -------------------------------------------------
# LECTURA DESDE SILVER BASE
# Nuevo estándar:
# - Primero usa segmentos_preparacion.lotes_segmento.
# - Fallback a dataset.lotes_segmento.
# - Fallback final a lote_cargue_id.
# -------------------------------------------------
df_movimientos_pago = leer_silver_base(
    spark=spark,
    ruta=ruta_movimientos_pago,
    parametros_extraccion=parametros_extraccion,
    tabla_relativa="general/anp_movimientos_pago",
    dataset=ds_movimientos_pago,
    lote_actual=lote_cargue_id,
    nombre_tabla="general/anp_movimientos_pago"
)

df_distribucion_vig = leer_silver_base(
    spark=spark,
    ruta=ruta_distribucion_vig,
    parametros_extraccion=parametros_extraccion,
    tabla_relativa="general/anp_distribucion_vig_pension",
    dataset=ds_distribucion_vig,
    lote_actual=lote_cargue_id,
    nombre_tabla="general/anp_distribucion_vig_pension"
)

df_pensiones = leer_silver_base(
    spark=spark,
    ruta=ruta_pensiones,
    parametros_extraccion=parametros_extraccion,
    tabla_relativa="general/anp_pensiones",
    dataset=ds_pensiones,
    lote_actual=lote_cargue_id,
    nombre_tabla="general/anp_pensiones"
)

df_rentas = leer_silver_base(
    spark=spark,
    ruta=ruta_rentas,
    parametros_extraccion=parametros_extraccion,
    tabla_relativa="prv/prv_rentas",
    dataset=ds_rentas,
    lote_actual=lote_cargue_id,
    nombre_tabla="prv/prv_rentas"
)

df_saldos_matematica = leer_silver_base(
    spark=spark,
    ruta=ruta_saldos_matematica,
    parametros_extraccion=parametros_extraccion,
    tabla_relativa="prv/prv_saldos_matematica",
    dataset=ds_saldos_matematica,
    lote_actual=lote_cargue_id,
    nombre_tabla="prv/prv_saldos_matematica"
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

df_conceptos = leer_silver_base(
    spark=spark,
    ruta=ruta_conceptos,
    parametros_extraccion=parametros_extraccion,
    tabla_relativa="general/cgr_conceptos",
    dataset=ds_conceptos,
    lote_actual=lote_cargue_id,
    nombre_tabla="general/cgr_conceptos"
)


# -------------------------------------------------
# VALIDACION DE INSUMOS
# -------------------------------------------------
total_movimientos_pago = df_movimientos_pago.count()
total_distribucion_vig = df_distribucion_vig.count()
total_pensiones = df_pensiones.count()
total_rentas = df_rentas.count()
total_saldos_matematica = df_saldos_matematica.count()
total_origenes = df_origenes.count()
total_conceptos = df_conceptos.count()

logger.info(f"Registros ANP_MOVIMIENTOS_PAGO preparación         : {total_movimientos_pago}")
logger.info(f"Registros ANP_DISTRIBUCION_VIG_PENSION preparación : {total_distribucion_vig}")
logger.info(f"Registros ANP_PENSIONES preparación                : {total_pensiones}")
logger.info(f"Registros PRV_RENTAS preparación                   : {total_rentas}")
logger.info(f"Registros PRV_SALDOS_MATEMATICA preparación        : {total_saldos_matematica}")
logger.info(f"Registros CGR_ORIGENES preparación                 : {total_origenes}")
logger.info(f"Registros CGR_CONCEPTOS preparación                : {total_conceptos}")

if total_movimientos_pago == 0:
    raise ValueError(
        "No hay registros en Silver base para general/anp_movimientos_pago "
        "usando los lotes de preparación."
    )

if total_distribucion_vig == 0:
    raise ValueError(
        "No hay registros en Silver base para general/anp_distribucion_vig_pension "
        "usando los lotes de preparación."
    )

if total_pensiones == 0:
    raise ValueError(
        "No hay registros en Silver base para general/anp_pensiones "
        "usando los lotes de preparación."
    )

if total_rentas == 0:
    raise ValueError(
        "No hay registros en Silver base para prv/prv_rentas "
        "usando los lotes de preparación."
    )

if total_saldos_matematica == 0:
    raise ValueError(
        "No hay registros en Silver base para prv/prv_saldos_matematica "
        "usando los lotes de preparación."
    )


# -------------------------------------------------
# PREPARACION SILVER - PRIMAS RENTAS
# -------------------------------------------------
# El SQL funcional filtra FEC_INICIAL, no PERIODO_PAGO.
# El periodo procede del JSON publicado desde PRG; no se consulta PRG otra vez.
periodo_preparacion = parametros_extraccion.get("periodo_negocio")
if not isinstance(periodo_preparacion, str) or len(periodo_preparacion) != 6 or not periodo_preparacion.isdigit():
    raise ValueError("periodo_negocio debe ser texto YYYYMM")
inicio_periodo = datetime.strptime(periodo_preparacion, "%Y%m")
siguiente_periodo = datetime(
    inicio_periodo.year + (inicio_periodo.month == 12),
    1 if inicio_periodo.month == 12 else inicio_periodo.month + 1,
    1
)
# Mismo extremo inclusivo 23:59:59 que el SQL recibido.
fin_periodo_sql = siguiente_periodo - timedelta(seconds=1)
logger.info("Periodo preparacion=%s; FEC_INICIAL entre %s y %s", periodo_preparacion, inicio_periodo, fin_periodo_sql)

df_mp = (
    df_movimientos_pago.alias("mp")
    .filter(col("mp.FEC_INICIAL").between(lit(inicio_periodo), lit(fin_periodo_sql)))
    .withColumn(
        "FECHA_INICIO_PERIODO_PAGO_RENTA",
        to_date(
            concat(
                substring(col("mp.PERIODO_PAGO").cast(StringType()), 1, 4),
                lit("-"),
                substring(col("mp.PERIODO_PAGO").cast(StringType()), 5, 2),
                lit("-01")
            ),
            "yyyy-MM-dd"
        )
    )
    .withColumn(
        "FECHA_FIN_PERIODO_PAGO_RENTA",
        last_day(col("FECHA_INICIO_PERIODO_PAGO_RENTA"))
    )
)

df_base = (
    df_mp.alias("mp")
    .join(
        df_distribucion_vig.alias("dv"),
        col("mp.NID_DISTRIBUCION_VIGENCIA") == col("dv.NID_DISTRIBUCION_VIGENCIA"),
        "inner"
    )
    .join(
        df_pensiones.alias("p"),
        col("dv.NID_PENSION") == col("p.NID_PENSION"),
        "inner"
    )
    .join(
        df_rentas.alias("r"),
        (col("p.NID_PROCEDENCIA") == col("r.NID_RENTAS")) &
        (col("mp.FECHA_FIN_PERIODO_PAGO_RENTA") >= col("r.FEC_EXPEDICION")),
        "inner"
    )
    .join(
        df_saldos_matematica.alias("sm"),
        (col("r.NID_RENTAS") == col("sm.NID_RENTAS")) &
        (col("mp.FECHA_FIN_PERIODO_PAGO_RENTA") >= col("sm.FEC_CORTE_CALCULO")),
        "inner"
    )
    .join(
        df_origenes.alias("o"),
        col("r.COD_ORIGEN") == col("o.COD_ORIGEN"),
        "inner"
    )
    .join(
        df_conceptos.alias("c"),
        col("mp.COD_CONCEPTO") == col("c.COD_CONCEPTO"),
        "inner"
    )
)


# -------------------------------------------------
# AGRUPACION FUNCIONAL
# Equivalente a prep_rentas_rentas_7:
# max(NID_CALCULO) y max(FEC_CORTE_CALCULO) por renta y atributos.
# -------------------------------------------------
df_agrupado = (
    df_base
    .groupBy(
        col("r.NID_RENTAS").alias("NID_RENTA"),
        col("r.FEC_EXPEDICION").alias("FEC_EXPEDICION_RENTA"),
        col("r.FEC_INVALIDACION_REGISTRO").alias("FEC_INVALIDACION_REGISTRO"),
        col("r.FEC_ENVIO_POINT").alias("FEC_POINT_RENTA"),
        col("r.COD_ORIGEN").alias("COD_TIPO_RENTA"),
        col("o.NOMBRE").alias("DSC_TIPO_RENTA"),
        col("r.VLR_PRIMA_UNICA").alias("VLR_RENTA"),
        col("mp.COD_CONCEPTO").alias("COD_CONCEPTO_PAGO_RENTA"),
        col("c.NOMBRE_CONCEPTO").alias("DSC_CONCEPTO_PAGO_RENTA"),
        col("mp.PERIODO_PAGO").alias("PER_PAGO_RENTA"),
        col("mp.FECHA_INICIO_PERIODO_PAGO_RENTA").alias("FEC_INI_PER_PAGO_RENTA"),
        col("mp.FECHA_FIN_PERIODO_PAGO_RENTA").alias("FEC_FIN_PER_PAGO_RENTA"),
        col("mp.LOTE_CARGUE_ID").alias("LOTE_CARGUE_ID")
    )
    .agg(
        spark_max(col("sm.NID_CALCULO")).alias("NID_CALCULO"),
        spark_max(col("sm.FEC_CORTE_CALCULO")).alias("FEC_CALCULO")
    )
)


# -------------------------------------------------
# SELECCION FINAL SILVER PREPARADA
# -------------------------------------------------
df_silver = (
    df_agrupado
    .filter(col("COD_CONCEPTO_PAGO_RENTA").cast(LongType()) == lit(1))
    .withColumn(
        "VAL_REG_INVALIDO_NULL",
        when(col("FEC_INVALIDACION_REGISTRO").isNotNull(), lit(1)).otherwise(lit(0))
    )
    .withColumn(
        "VAL_REG_INVALIDO",
        when(
            (col("FEC_INVALIDACION_REGISTRO") < col("FEC_INI_PER_PAGO_RENTA")) &
            (col("VAL_REG_INVALIDO_NULL") == lit(1)),
            lit(1)
        ).otherwise(lit(0))
    )
    .select(
        col("NID_RENTA").cast(LongType()).alias("NID_RENTA"),
        col("NID_CALCULO").cast(LongType()).alias("NID_CALCULO"),
        col("FEC_CALCULO").cast("date").alias("FEC_CALCULO"),
        date_format(col("FEC_CALCULO"), "yyyyMM").cast(StringType()).alias("PER_CALCULO"),
        col("FEC_EXPEDICION_RENTA").cast("date").alias("FEC_EXPEDICION_RENTA"),
        date_format(col("FEC_EXPEDICION_RENTA"), "yyyyMM").cast(StringType()).alias("PER_EXPEDICION_RENTA"),
        col("FEC_INVALIDACION_REGISTRO").cast("date").alias("FEC_INVALIDACION_REGISTRO"),
        date_format(col("FEC_INVALIDACION_REGISTRO"), "yyyyMM").cast(StringType()).alias("PER_INVALIDACION_REGISTRO"),
        col("FEC_POINT_RENTA").cast("date").alias("FEC_POINT_RENTA"),
        date_format(col("FEC_POINT_RENTA"), "yyyyMM").cast(StringType()).alias("PER_POINT_RENTA"),
        col("COD_TIPO_RENTA").cast(LongType()).alias("COD_TIPO_RENTA"),
        col("DSC_TIPO_RENTA").cast(StringType()).alias("DSC_TIPO_RENTA"),
        col("VLR_RENTA").cast(DecimalType(17, 2)).alias("VLR_RENTA"),
        col("COD_CONCEPTO_PAGO_RENTA").cast(LongType()).alias("COD_CONCEPTO_PAGO_RENTA"),
        col("DSC_CONCEPTO_PAGO_RENTA").cast(StringType()).alias("DSC_CONCEPTO_PAGO_RENTA"),
        col("PER_PAGO_RENTA").cast(StringType()).alias("PER_PAGO_RENTA"),
        col("FEC_INI_PER_PAGO_RENTA").cast("date").alias("FEC_INI_PER_PAGO_RENTA"),
        col("FEC_FIN_PER_PAGO_RENTA").cast("date").alias("FEC_FIN_PER_PAGO_RENTA"),
        col("VAL_REG_INVALIDO_NULL").cast(IntegerType()).alias("VAL_REG_INVALIDO_NULL"),
        col("VAL_REG_INVALIDO").cast(IntegerType()).alias("VAL_REG_INVALIDO"),
        col("LOTE_CARGUE_ID").cast(IntegerType()).alias("LOTE_CARGUE_ID")
    )

)

total_silver = df_silver.count()

logger.info(f"Total registros Silver preparada Primas Rentas: {total_silver}")


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
    "general/anp_movimientos_pago": obtener_lotes_preparacion(
        parametros_extraccion,
        "general/anp_movimientos_pago",
        ds_movimientos_pago,
        lote_cargue_id
    ),
    "general/anp_distribucion_vig_pension": obtener_lotes_preparacion(
        parametros_extraccion,
        "general/anp_distribucion_vig_pension",
        ds_distribucion_vig,
        lote_cargue_id
    ),
    "general/anp_pensiones": obtener_lotes_preparacion(
        parametros_extraccion,
        "general/anp_pensiones",
        ds_pensiones,
        lote_cargue_id
    ),
    "prv/prv_rentas": obtener_lotes_preparacion(
        parametros_extraccion,
        "prv/prv_rentas",
        ds_rentas,
        lote_cargue_id
    ),
    "prv/prv_saldos_matematica": obtener_lotes_preparacion(
        parametros_extraccion,
        "prv/prv_saldos_matematica",
        ds_saldos_matematica,
        lote_cargue_id
    ),
    "general/cgr_origenes": obtener_lotes_preparacion(
        parametros_extraccion,
        "general/cgr_origenes",
        ds_origenes,
        lote_cargue_id
    ),
    "general/cgr_conceptos": obtener_lotes_preparacion(
        parametros_extraccion,
        "general/cgr_conceptos",
        ds_conceptos,
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
    "nombre_caso": "primas_rentas",
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
logger.info("=== FIN SILVER PREPARADA PRIMAS RENTAS ===")

job.commit()