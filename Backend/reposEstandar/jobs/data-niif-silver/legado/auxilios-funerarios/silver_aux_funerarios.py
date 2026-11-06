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

from pyspark.sql.functions import col, lit, date_format
from pyspark.sql.types import LongType, DecimalType, IntegerType, StringType

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
    parametros_extraccion, tabla_relativa, dataset, lote_actual
):
    objetivo = normalizar_tabla_relativa(tabla_relativa)

    def convertir(valor):
        if valor is None:
            return []
        if not isinstance(valor, (list, tuple, set)):
            valor = [valor]
        return sorted(set(int(x) for x in valor if x is not None))

    referencias = parametros_extraccion.get(
        "lotes_usados_preparacion_silver", {}
    ) or {}

    for tabla, valor in referencias.items():
        if normalizar_tabla_relativa(tabla) == objetivo:
            lotes = convertir(valor)
            if lotes:
                return lotes

    lotes = convertir(dataset.get("lotes_preparacion_silver"))
    if lotes:
        return lotes

    segmento = obtener_segmento_preparacion(
        parametros_extraccion, tabla_relativa
    )
    if segmento:
        lotes = convertir(segmento.get("lotes_segmento"))
        if lotes:
            return lotes

    sh = dataset.get("segmentacion_horizontal") or {}
    segmentada = bool(
        dataset.get("segmentacion_horizontal_habilitada")
        or sh.get("habilitada")
    )

    if segmentada:
        lotes = convertir(
            dataset.get("lotes_segmento") or sh.get("lotes_segmento")
        )
        if lotes:
            return lotes

    lotes = convertir(dataset.get("lote_cargue_id_dataset"))
    if lotes:
        return lotes

    lotes = convertir(lote_actual)
    if not lotes:
        raise RuntimeError(
            "Sin lotes de preparacion para " + tabla_relativa
        )
    return lotes


def leer_silver_base(
    spark, ruta, parametros_extraccion, tabla_relativa,
    dataset, lote_actual, nombre_tabla
):
    lotes = obtener_lotes_preparacion(
        parametros_extraccion, tabla_relativa, dataset, lote_actual
    )

    logger.info(
        f"Leyendo {nombre_tabla}: ruta={ruta}, lotes={lotes}"
    )

    df = spark.read.parquet(ruta)
    columnas = [
        c for c in df.columns if c.upper() == "LOTE_CARGUE_ID"
    ]
    if len(columnas) != 1:
        raise RuntimeError(
            f"{nombre_tabla}: columna LOTE_CARGUE_ID ausente o ambigua"
        )

    filtrado = df.filter(
        col(columnas[0]).cast("bigint").isin(lotes)
    )

    sh = dataset.get("segmentacion_horizontal") or {}
    extraidos = int(dataset.get("total_registros") or 0)
    excluidos = int(
        dataset.get("registros_excluidos_por_segmento")
        or sh.get("registros_excluidos_por_segmento")
        or 0
    )

    # Permite entradas legitimamente vacias.
    # No permite ocultar la ausencia de filas esperadas.
    if (extraidos > 0 or excluidos > 0) and not filtrado.take(1):
        raise RuntimeError(
            f"{nombre_tabla}: no hay filas en Silver para "
            f"los lotes referenciados {lotes}. "
            f"Extraidos={extraidos}, excluidos={excluidos}. "
            "No se sustituye por el ultimo lote."
        )

    return filtrado


# -------------------------------------------------
# PARAMETROS DEL JOB
# -------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_SLV_PREP_PATH",
        "RUTA_PARAMETROS_EXTRACCION",
        "FEC_DESDE",
        "FEC_HASTA"
    ]
)

slv_prep = ensure_trailing_slash(args["S3_SLV_PREP_PATH"])
ruta_parametros_extraccion = args["RUTA_PARAMETROS_EXTRACCION"]
fec_desde = args["FEC_DESDE"]
fec_hasta = args["FEC_HASTA"]

logger.info(f"SILVER PREP PATH           : {slv_prep}")
logger.info(f"RUTA PARAMETROS EXTRACCION : {ruta_parametros_extraccion}")
logger.info(f"FEC_DESDE                  : {fec_desde}")
logger.info(f"FEC_HASTA                  : {fec_hasta}")


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
    "general/anp_periodo_solicitudes_giro",
    "general/anp_detalle_solicitudes_giro",
    "general/anp_pensionado_novedad",
    "general/anp_distribucion_vig_pension",
    "general/anp_pensiones",
    "general/cgr_conceptos",
    "general/cgr_origenes"
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
# CONFIGURACION, CARGA Y VALIDACION DE INSUMOS
# -------------------------------------------------

configuracion_insumos = {
    "periodo_giro": {
        "tabla": "general/anp_periodo_solicitudes_giro",
        "etiqueta": "ANP_PERIODO_SOLICITUDES_GIRO"
    },
    "detalle_giro": {
        "tabla": "general/anp_detalle_solicitudes_giro",
        "etiqueta": "ANP_DETALLE_SOLICITUDES_GIRO"
    },
    "pensionado_novedad": {
        "tabla": "general/anp_pensionado_novedad",
        "etiqueta": "ANP_PENSIONADO_NOVEDAD"
    },
    "distribucion_vig": {
        "tabla": "general/anp_distribucion_vig_pension",
        "etiqueta": "ANP_DISTRIBUCION_VIG_PENSION"
    },
    "pensiones": {
        "tabla": "general/anp_pensiones",
        "etiqueta": "ANP_PENSIONES"
    },
    "conceptos": {
        "tabla": "general/cgr_conceptos",
        "etiqueta": "CGR_CONCEPTOS"
    },
    "origenes": {
        "tabla": "general/cgr_origenes",
        "etiqueta": "CGR_ORIGENES"
    }
}

datasets_insumos = {}
dataframes_insumos = {}
totales_insumos = {}

for nombre_insumo, configuracion in configuracion_insumos.items():
    tabla_relativa = configuracion["tabla"]
    etiqueta = configuracion["etiqueta"]

    dataset_insumo = mapa_parametros[tabla_relativa]
    ruta_insumo = ensure_trailing_slash(
        dataset_insumo["ruta_dataset_destino"]
    )

    dataframe_insumo = leer_silver_base(
        spark=spark,
        ruta=ruta_insumo,
        parametros_extraccion=parametros_extraccion,
        tabla_relativa=tabla_relativa,
        dataset=dataset_insumo,
        lote_actual=lote_cargue_id,
        nombre_tabla=tabla_relativa
    )

    total_insumo = dataframe_insumo.count()

    datasets_insumos[nombre_insumo] = dataset_insumo
    dataframes_insumos[nombre_insumo] = dataframe_insumo
    totales_insumos[nombre_insumo] = total_insumo

    logger.info("Ruta %s : %s", etiqueta, ruta_insumo)
    logger.info("Registros %s : %s", etiqueta, total_insumo)

ds_periodo_giro = datasets_insumos["periodo_giro"]
ds_detalle_giro = datasets_insumos["detalle_giro"]
ds_pensionado_novedad = datasets_insumos["pensionado_novedad"]
ds_distribucion_vig = datasets_insumos["distribucion_vig"]
ds_pensiones = datasets_insumos["pensiones"]
ds_conceptos = datasets_insumos["conceptos"]
ds_origenes = datasets_insumos["origenes"]

df_periodo_giro = dataframes_insumos["periodo_giro"]
df_detalle_giro = dataframes_insumos["detalle_giro"]
df_pensionado_novedad = dataframes_insumos["pensionado_novedad"]
df_distribucion_vig = dataframes_insumos["distribucion_vig"]
df_pensiones = dataframes_insumos["pensiones"]
df_conceptos = dataframes_insumos["conceptos"]
df_origenes = dataframes_insumos["origenes"]

total_periodo_giro = totales_insumos["periodo_giro"]

if total_periodo_giro == 0:
    lotes_periodo_giro = obtener_lotes_preparacion(
        parametros_extraccion=parametros_extraccion,
        tabla_relativa=configuracion_insumos[
            "periodo_giro"
        ]["tabla"],
        dataset=ds_periodo_giro,
        lote_actual=lote_cargue_id
    )

    raise ValueError(
        "No hay registros en Silver base para "
        "ANP_PERIODO_SOLICITUDES_GIRO usando los lotes "
        f"de preparación: {lotes_periodo_giro}."
    )


# -------------------------------------------------
# PREPARACION SILVER - AUXILIOS FUNERARIOS
# -------------------------------------------------
df_preparado = (
    df_periodo_giro.alias("pg")
    .filter(
        (col("pg.FEC_ENVIO_TES").cast("date") >= lit(fec_desde).cast("date")) &
        (col("pg.FEC_ENVIO_TES").cast("date") <= lit(fec_hasta).cast("date"))
    )
    .join(
        df_detalle_giro.alias("dg"),
        col("pg.NID_PERIODO_SOLICITUD_GIRO") == col("dg.NID_PERIODO_SOLICITUD_GIRO"),
        "inner"
    )
    .join(
        df_conceptos.alias("cc"),
        col("pg.COD_CONCEPTO_ANP") == col("cc.COD_CONCEPTO"),
        "inner"
    )
    .join(
        df_pensionado_novedad.alias("pn"),
        (col("dg.COD_TIPO_IDENTIFICACION") == col("pn.COD_TIPO_IDENTIFICACION")) &
        (col("dg.NID_NUMERO_IDENTIFICACION") == col("pn.NID_NUMERO_IDENTIFICACION")),
        "inner"
    )
    .join(
        df_distribucion_vig.alias("dv"),
        col("pn.NID_DISTRIBUCION_VIGENCIA") == col("dv.NID_DISTRIBUCION_VIGENCIA"),
        "inner"
    )
    .join(
        df_pensiones.alias("p"),
        col("dv.NID_PENSION") == col("p.NID_PENSION"),
        "inner"
    )
    .join(
        df_origenes.alias("o"),
        col("p.COD_ORIGEN") == col("o.COD_ORIGEN"),
        "inner"
    )
    .filter(col("pn.COD_CONCEPTO") == lit("28"))
    .filter(col("pg.BLN_ANULADO_POINT") == lit("N"))
    .filter(col("pg.NUM_SOLICITUD_TES").isNotNull())
    .filter(col("pg.NUM_SOLICITUD_TES") > 0)
    .filter(col("cc.COD_CONCEPTO") == col("pn.COD_CONCEPTO"))
    .select(
        col("pg.NUM_SOLICITUD_TES").cast(LongType()).alias("NUM_SOLICITUD_TESORERIA"),
        col("p.NID_PROCEDENCIA").cast(LongType()).alias("NUM_POLIZA"),
        col("pn.COD_TIPO_IDENTIFICACION").cast(StringType()).alias("TIPO_IDENTIFICACION_AFILIADO"),
        col("pn.NID_NUMERO_IDENTIFICACION").cast(LongType()).alias("NUM_IDENTIFICACION_AFILIADO"),
        col("p.FEC_EXPEDICION").cast("date").alias("FEC_EXPEDICION_PENSION"),
        date_format(col("p.FEC_EXPEDICION"), "yyyyMM").alias("PER_EXPEDICION_PENSION"),
        col("pn.VLR_TOTAL").cast(DecimalType(14, 2)).alias("VLR_TOTAL"),
        col("pg.FEC_ENVIO_TES").cast("date").alias("FEC_ENVIO_TESORERIA"),
        date_format(col("pg.FEC_ENVIO_TES"), "yyyyMM").alias("PER_ENVIO_TESORERIA"),
        col("cc.COD_CONCEPTO").cast(StringType()).alias("COD_CONCEPTO"),
        col("cc.NOMBRE_CONCEPTO").cast(StringType()).alias("NOM_CONCEPTO"),
        col("o.COD_ORIGEN").cast(StringType()).alias("COD_ORIGEN"),
        col("o.NOMBRE").cast(StringType()).alias("DSC_ORIGEN"),
        col("pg.LOTE_CARGUE_ID").cast(IntegerType()).alias("LOTE_CARGUE_ID")
    )
    .dropDuplicates()
)

total_preparado = df_preparado.count()

logger.info(f"Total registros Silver preparada Auxilios Funerarios: {total_preparado}")


# -------------------------------------------------
# ESCRITURA SILVER PREPARADA
# Silver preparada representa la foto actual lista para Gold.
# El histórico de cambios se conserva en Gold SCD.
# -------------------------------------------------
target_path = slv_prep

logger.info(f"Escribiendo Silver preparada en modo overwrite: {target_path}")

(
    df_preparado
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
    configuracion["tabla"]: obtener_lotes_preparacion(
        parametros_extraccion=parametros_extraccion,
        tabla_relativa=configuracion["tabla"],
        dataset=datasets_insumos[nombre_insumo],
        lote_actual=lote_cargue_id
    )
    for nombre_insumo, configuracion
    in configuracion_insumos.items()
}

parametros_extraccion["estado_preparacion_silver"] = "SUCCESS"
parametros_extraccion["ruta_silver_preparada"] = target_path
parametros_extraccion["modo_escritura_silver_preparada"] = "overwrite"
parametros_extraccion["silver_preparada_historica"] = False
parametros_extraccion["total_registros_silver_preparada"] = total_preparado
parametros_extraccion["fecha_preparacion_silver"] = fecha_preparacion
parametros_extraccion["lotes_usados_preparacion_silver"] = lotes_usados_preparacion

parametros_extraccion["silver_preparacion"] = {
    "estado": "SUCCESS",
    "nombre_caso": "auxilios_funerarios",
    "ruta_silver_preparada": target_path,
    "modo_escritura": "overwrite",
    "total_registros": total_preparado,
    "lote_cargue_id": lote_cargue_id,
    "lotes_usados_preparacion": lotes_usados_preparacion,
    "estado_validacion": estado_validacion,
    "requiere_revision_analitica": requiere_revision_analitica,
    "estado_hallazgos_validacion": estado_hallazgos_validacion,
    "fecha_preparacion": fecha_preparacion
}

guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

logger.info("parametros_extraccion.json actualizado con estado_preparacion_silver SUCCESS.")
logger.info("=== FIN SILVER PREPARADA AUXILIOS FUNERARIOS ===")

job.commit()