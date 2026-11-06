# ================================================================
# RESERVA MATEMATICA ARP
# ================================================================


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

from pyspark.sql import Window
from pyspark.sql.functions import (
    col, lit, lower, upper, trim, concat_ws, concat, when, to_date, last_day,
    current_timestamp,
    date_format, add_months, months_between, first, dense_rank,
    row_number, desc, asc, round as spark_round, least, floor, coalesce,
    countDistinct, max as spark_max, substring
)
from pyspark.sql.types import LongType, IntegerType, DecimalType, StringType, DateType, StructType

# =================================================
# LOGGING
# =================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =================================================
# HELPERS GENERALES
# =================================================
def ensure_trailing_slash(path: str) -> str:
    return path if path.endswith("/") else path + "/"


def parse_s3_uri(ruta_s3: str):
    parsed = urlparse(ruta_s3)
    return parsed.netloc, parsed.path.lstrip("/")


def cargar_json_s3(ruta_s3: str) -> dict:
    bucket, key = parse_s3_uri(ruta_s3)
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def guardar_json_s3(ruta_s3: str, contenido: dict):
    bucket, key = parse_s3_uri(ruta_s3)
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
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


def obtener_lotes_preparacion(parametros_extraccion: dict, tabla_relativa: str, dataset: dict, lote_actual):
    """Devuelve lotes solo para la tabla pivote segmentada.

    Las dependencias no segmentadas se resuelven como snapshot fisico vigente.
    """
    segmento = obtener_segmento_preparacion(parametros_extraccion, tabla_relativa)
    if segmento and bool(dataset.get("segmentacion_horizontal_habilitada", False)):
        lotes = segmento.get("lotes_segmento") or dataset.get("lotes_segmento") or [lote_actual]
        return sorted({int(x) for x in lotes if x is not None})

    if bool(dataset.get("segmentacion_horizontal_habilitada", False)):
        lotes = dataset.get("lotes_segmento") or [lote_actual]
        return sorted({int(x) for x in lotes if x is not None})

    return []


def leer_silver_base(spark, ruta: str, parametros_extraccion: dict, tabla_relativa: str, dataset: dict, lote_actual, nombre_tabla: str):
    segmentada = bool(dataset.get("segmentacion_horizontal_habilitada", False))
    lotes_preparacion = obtener_lotes_preparacion(
        parametros_extraccion=parametros_extraccion,
        tabla_relativa=tabla_relativa,
        dataset=dataset,
        lote_actual=lote_actual
    )

    df = spark.read.parquet(ensure_trailing_slash(ruta))
    cols_lower = {c.lower(): c for c in df.columns}

    if "lote_cargue_id" not in cols_lower:
        raise ValueError(f"{nombre_tabla} no tiene columna LOTE_CARGUE_ID. Ruta={ruta}")

    lote_col = cols_lower["lote_cargue_id"]

    if segmentada:
        logger.info(
            f"Leyendo {nombre_tabla}. modo=SEGMENTOS_DEL_PERIODO "
            f"lotes_preparacion={lotes_preparacion}"
        )
        return df.filter(col(lote_col).cast("bigint").isin(lotes_preparacion))

    lote_vigente = (
        df.select(col(lote_col).cast("bigint").alias("lote"))
        .where(col("lote").isNotNull())
        .agg(spark_max("lote").alias("lote_vigente"))
        .first()["lote_vigente"]
    )

    if lote_vigente is None:
        raise ValueError(f"{nombre_tabla} no tiene un snapshot fisico vigente. Ruta={ruta}")

    logger.info(
        f"Leyendo {nombre_tabla}. modo=SNAPSHOT_VIGENTE "
        f"lote_vigente={lote_vigente}"
    )
    return df.filter(col(lote_col).cast("bigint") == lit(int(lote_vigente)))


def materializar(df, ruta: str, nombre: str):
    ruta = ensure_trailing_slash(ruta)
    logger.info(f"Materializando {nombre} en: {ruta}")

    # Protección contra columnas duplicadas antes de escribir Parquet.
    cols_originales = df.columns
    vistos = set()
    columnas_unicas = []
    nuevos_nombres = []

    for idx, c in enumerate(cols_originales):
        key = c.lower()
        if key in vistos:
            nuevo = f"__dup_{idx}_{c}"
            logger.warning(f"Columna duplicada detectada en {nombre}: {c}. Se renombra temporalmente como {nuevo} y se descarta.")
            nuevos_nombres.append(nuevo)
        else:
            vistos.add(key)
            columnas_unicas.append(c)
            nuevos_nombres.append(c)

    if len(columnas_unicas) != len(cols_originales):
        df = df.toDF(*nuevos_nombres).select(*columnas_unicas)

    df.write.mode("overwrite").option("compression", "snappy").parquet(ruta)
    return spark.read.parquet(ruta)


# =================================================
# PARAMETROS GLUE
# =================================================
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RUTA_PARAMETROS_EXTRACCION",
        "S3_SLV_PREP_PATH",
        "S3_TMP_PREP_PATH",
        "PER_CORTE"
    ]
)

ruta_parametros_extraccion = args["RUTA_PARAMETROS_EXTRACCION"]
slv_prep_base = ensure_trailing_slash(args["S3_SLV_PREP_PATH"])
tmp_prep_base = ensure_trailing_slash(args["S3_TMP_PREP_PATH"])
per_corte = str(args["PER_CORTE"]).strip()

if not per_corte.isdigit() or len(per_corte) != 6:
    raise ValueError(f"PER_CORTE debe tener formato yyyymm. Valor recibido: {per_corte}")

per_corte_int = int(per_corte)
anio_corte_int = int(per_corte[:4])

slv_prep_target = f"{slv_prep_base}corte_{per_corte}/"
tmp_target = f"{tmp_prep_base}corte_{per_corte}/"

logger.info("=== INICIO SILVER PREPARACION RESERVA MATEMATICA ARP V2.4 ===")
logger.info(f"PER_CORTE                   : {per_corte}")
logger.info(f"S3_SLV_PREP_PATH base       : {slv_prep_base}")
logger.info(f"S3_SLV_PREP_PATH corte      : {slv_prep_target}")
logger.info(f"S3_TMP_PREP_PATH corte      : {tmp_target}")
logger.info(f"RUTA_PARAMETROS_EXTRACCION  : {ruta_parametros_extraccion}")


# =================================================
# CONTEXTO GLUE/SPARK
# =================================================
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


# =================================================
# CARGA PARAMETROS_EXTRACCION Y VALIDACION DE ESTADOS
# =================================================
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
    raise ValueError(f"Estado de validación no soportado para Silver: {estado_validacion}")

if estado_remediacion != "SUCCESS":
    raise ValueError(f"Remediación no está en SUCCESS. estado_remediacion={estado_remediacion}")

if estado_disponibilizacion_silver != "SUCCESS":
    raise ValueError(
        f"Disponibilización Silver no está en SUCCESS. "
        f"estado_disponibilizacion_silver={estado_disponibilizacion_silver}"
    )

if not datasets_extraidos:
    raise ValueError("El JSON de parametros_extraccion no contiene datasets_extraidos.")

if estado_validacion == "FAILED":
    logger.warning(
        "Validación técnica tiene hallazgos, pero el flujo continúa porque "
        "remediación y disponibilización Silver están en SUCCESS."
    )

mapa_parametros = {
    normalizar_tabla_relativa(d["tabla_relativa"]): d
    for d in datasets_extraidos
}

TABLAS_REQUERIDAS = [
    "arpprod/sar_datos_invalidez_muerte",
    "arpprod/sar_siniestros",
    "arpprod/par_trabajadores",
    "arpprod/sar_auditoria_medica",
    "arpprod/ind_datos_reservas_siniestros",
    "arpprod/hist_datos_invalidez_muerte",
    "arpprod/sar_calculos_actuariales",
    "arpprod/hist_sar_pcl_pension",
    "arpprod/sar_reserva_pcj_amortizacion",
    "arpprod/sar_beneficiarios_siniestros",
    "general/acu_personas_naturales",
    "general/cgr_equivalencia_tipos_id",
    "general/anp_ipc",
    "general/cgr_origenes",
    "general/rva_reserva_matematica_transic",
    "general/pgr_salarios_minimos",
    "general/cgr_estados_personas",
    "general/cgr_parentescos"
]

for tabla_req in TABLAS_REQUERIDAS:
    if tabla_req not in mapa_parametros:
        raise ValueError(f"La tabla requerida '{tabla_req}' no aparece en parametros_extraccion.")


def ds(tabla_relativa):
    return mapa_parametros[normalizar_tabla_relativa(tabla_relativa)]


def ruta_destino(tabla_relativa):
    return ensure_trailing_slash(ds(tabla_relativa)["ruta_dataset_destino"])


# =================================================
# ENTRADA FUNCIONAL VACIA: SALIDA PREPARADA VACIA TIPADA
# =================================================
pivote_arp = ds("arpprod/sar_datos_invalidez_muerte")
entrada_funcional_vacia_confirmada = (
    bool(parametros_extraccion.get("entrada_funcional_vacia", False))
    and pivote_arp.get("total_registros") == 0
    and not bool(pivote_arp.get("exclusion_aplicada", False))
)

if entrada_funcional_vacia_confirmada:
    logger.info(
        "ARP sin entrada funcional para el corte. "
        "Se genera Silver preparada vacia con esquema historico validado."
    )
    schema_vacio = StructType.fromJson(json.loads('{"type": "struct", "fields": [{"name": "PER_CORTE", "type": "integer", "nullable": true, "metadata": {}}, {"name": "FEC_INI_PER_CORTE", "type": "date", "nullable": true, "metadata": {}}, {"name": "FEC_FIN_PER_CORTE", "type": "date", "nullable": true, "metadata": {}}, {"name": "ANI_CORTE", "type": "integer", "nullable": true, "metadata": {}}, {"name": "RAMO", "type": "integer", "nullable": true, "metadata": {}}, {"name": "UNIDAD", "type": "integer", "nullable": true, "metadata": {}}, {"name": "EQUIVALENCIA", "type": "string", "nullable": true, "metadata": {}}, {"name": "NRO_POLIZA", "type": "long", "nullable": true, "metadata": {}}, {"name": "FEC_EXP_POLIZA", "type": "date", "nullable": true, "metadata": {}}, {"name": "PER_EXP_POLIZA", "type": "integer", "nullable": true, "metadata": {}}, {"name": "FEC_INI_PER_EXP_POLIZA", "type": "date", "nullable": true, "metadata": {}}, {"name": "FEC_FIN_PER_EXP_POLIZA", "type": "date", "nullable": true, "metadata": {}}, {"name": "ANI_EXP_POLIZA", "type": "integer", "nullable": true, "metadata": {}}, {"name": "FEC_VIG_INICIAL", "type": "date", "nullable": true, "metadata": {}}, {"name": "NRO_SINIESTRO", "type": "long", "nullable": true, "metadata": {}}, {"name": "FEC_CAU_DERECHO", "type": "date", "nullable": true, "metadata": {}}, {"name": "PER_CAU_DERECHO", "type": "integer", "nullable": true, "metadata": {}}, {"name": "FEC_INI_PER_CAU_DERECHO", "type": "date", "nullable": true, "metadata": {}}, {"name": "FEC_FIN_PER_CAU_DERECHO", "type": "date", "nullable": true, "metadata": {}}, {"name": "COD_ORG_PENSION", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_ORG_PENSION", "type": "string", "nullable": true, "metadata": {}}, {"name": "PRT_AFILIADO", "type": "integer", "nullable": true, "metadata": {}}, {"name": "COD_ORG_TIP_IDT_AFILIADO", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_TIP_IDT_AFILIADO", "type": "string", "nullable": true, "metadata": {}}, {"name": "NRO_DOC_IDT_AFILIADO", "type": "long", "nullable": true, "metadata": {}}, {"name": "GEN_AFILIADO", "type": "string", "nullable": true, "metadata": {}}, {"name": "SEX_AFILIADO", "type": "integer", "nullable": true, "metadata": {}}, {"name": "PMR_APE_AFILIADO", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_APE_AFILIADO", "type": "string", "nullable": true, "metadata": {}}, {"name": "PMR_NOM_AFILIADO", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_NOM_AFILIADO", "type": "string", "nullable": true, "metadata": {}}, {"name": "NOM_CPT_AFILIADO", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_NAC_AFILIADO", "type": "date", "nullable": true, "metadata": {}}, {"name": "FEC_MRT_AFILIADO", "type": "date", "nullable": true, "metadata": {}}, {"name": "ETD_AFILIADO", "type": "string", "nullable": true, "metadata": {}}, {"name": "EST_AFILIADO", "type": "string", "nullable": true, "metadata": {}}, {"name": "NRO_HJS_TTL_AFILIADO", "type": "integer", "nullable": true, "metadata": {}}, {"name": "NRO_HJS_INV_AFILIADO", "type": "integer", "nullable": true, "metadata": {}}, {"name": "IGR_BSE_LIQ_AFILIADO", "type": "decimal(18,2)", "nullable": true, "metadata": {}}, {"name": "MSD_AFILIADO", "type": "decimal(18,2)", "nullable": true, "metadata": {}}, {"name": "NRO_MSD_AFILIADO", "type": "integer", "nullable": true, "metadata": {}}, {"name": "RETROACTIVO", "type": "string", "nullable": true, "metadata": {}}, {"name": "GRD_INV_INICIAL", "type": "decimal(5,2)", "nullable": true, "metadata": {}}, {"name": "FEC_ULT_MOD", "type": "date", "nullable": true, "metadata": {}}, {"name": "GRD_INV_ACTUAL", "type": "decimal(5,2)", "nullable": true, "metadata": {}}, {"name": "GASTOS", "type": "decimal(5,2)", "nullable": true, "metadata": {}}, {"name": "COD_ORG_PRT_BENEFICIARIO_1", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_ORG_PRT_BENEFICIARIO_1", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_PRT_BENEFICIARIO_1", "type": "integer", "nullable": true, "metadata": {}}, {"name": "DSC_PRT_BENEFICIARIO_1", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_ORG_TIP_IDT_BENEFICIARIO_1", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_TIP_IDT_BENEFICIARIO_1", "type": "string", "nullable": true, "metadata": {}}, {"name": "NRO_DOC_IDT_BENEFICIARIO_1", "type": "decimal(15,0)", "nullable": true, "metadata": {}}, {"name": "GEN_BENEFICIARIO_1", "type": "string", "nullable": true, "metadata": {}}, {"name": "SEX_BENEFICIARIO_1", "type": "integer", "nullable": true, "metadata": {}}, {"name": "PMR_APE_BENEFICIARIO_1", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_APE_BENEFICIARIO_1", "type": "string", "nullable": true, "metadata": {}}, {"name": "PMR_NOM_BENEFICIARIO_1", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_NOM_BENEFICIARIO_1", "type": "string", "nullable": true, "metadata": {}}, {"name": "NOM_CPT_BENEFICIARIO_1", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_NAC_BENEFICIARIO_1", "type": "timestamp", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_CNY_CMP_1", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_HIJO_1", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "TMP_BNF_BENEFICIARIO_1", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "CNT_HJS_BENEFICIARIO_1", "type": "decimal(2,0)", "nullable": true, "metadata": {}}, {"name": "ETD_BENEFICIARIO_1", "type": "integer", "nullable": true, "metadata": {}}, {"name": "COD_EST_BENEFICIARIO_1", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_EST_BENEFICIARIO_1", "type": "string", "nullable": true, "metadata": {}}, {"name": "CON_DER_BENEFICIARIO_1", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_PDD_DER_BENEFICIARIO_1", "type": "date", "nullable": true, "metadata": {}}, {"name": "COD_ORG_PRT_BENEFICIARIO_2", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_ORG_PRT_BENEFICIARIO_2", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_PRT_BENEFICIARIO_2", "type": "integer", "nullable": true, "metadata": {}}, {"name": "DSC_PRT_BENEFICIARIO_2", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_ORG_TIP_IDT_BENEFICIARIO_2", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_TIP_IDT_BENEFICIARIO_2", "type": "string", "nullable": true, "metadata": {}}, {"name": "NRO_DOC_IDT_BENEFICIARIO_2", "type": "decimal(15,0)", "nullable": true, "metadata": {}}, {"name": "GEN_BENEFICIARIO_2", "type": "string", "nullable": true, "metadata": {}}, {"name": "SEX_BENEFICIARIO_2", "type": "integer", "nullable": true, "metadata": {}}, {"name": "PMR_APE_BENEFICIARIO_2", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_APE_BENEFICIARIO_2", "type": "string", "nullable": true, "metadata": {}}, {"name": "PMR_NOM_BENEFICIARIO_2", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_NOM_BENEFICIARIO_2", "type": "string", "nullable": true, "metadata": {}}, {"name": "NOM_CPT_BENEFICIARIO_2", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_NAC_BENEFICIARIO_2", "type": "timestamp", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_CNY_CMP_2", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_HIJO_2", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "TMP_BNF_BENEFICIARIO_2", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "CNT_HJS_BENEFICIARIO_2", "type": "decimal(2,0)", "nullable": true, "metadata": {}}, {"name": "ETD_BENEFICIARIO_2", "type": "integer", "nullable": true, "metadata": {}}, {"name": "COD_EST_BENEFICIARIO_2", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_EST_BENEFICIARIO_2", "type": "string", "nullable": true, "metadata": {}}, {"name": "CON_DER_BENEFICIARIO_2", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_PDD_DER_BENEFICIARIO_2", "type": "date", "nullable": true, "metadata": {}}, {"name": "COD_ORG_PRT_BENEFICIARIO_3", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_ORG_PRT_BENEFICIARIO_3", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_PRT_BENEFICIARIO_3", "type": "integer", "nullable": true, "metadata": {}}, {"name": "DSC_PRT_BENEFICIARIO_3", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_ORG_TIP_IDT_BENEFICIARIO_3", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_TIP_IDT_BENEFICIARIO_3", "type": "string", "nullable": true, "metadata": {}}, {"name": "NRO_DOC_IDT_BENEFICIARIO_3", "type": "decimal(15,0)", "nullable": true, "metadata": {}}, {"name": "GEN_BENEFICIARIO_3", "type": "string", "nullable": true, "metadata": {}}, {"name": "SEX_BENEFICIARIO_3", "type": "integer", "nullable": true, "metadata": {}}, {"name": "PMR_APE_BENEFICIARIO_3", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_APE_BENEFICIARIO_3", "type": "string", "nullable": true, "metadata": {}}, {"name": "PMR_NOM_BENEFICIARIO_3", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_NOM_BENEFICIARIO_3", "type": "string", "nullable": true, "metadata": {}}, {"name": "NOM_CPT_BENEFICIARIO_3", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_NAC_BENEFICIARIO_3", "type": "timestamp", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_CNY_CMP_3", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_HIJO_3", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "TMP_BNF_BENEFICIARIO_3", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "CNT_HJS_BENEFICIARIO_3", "type": "decimal(2,0)", "nullable": true, "metadata": {}}, {"name": "ETD_BENEFICIARIO_3", "type": "integer", "nullable": true, "metadata": {}}, {"name": "COD_EST_BENEFICIARIO_3", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_EST_BENEFICIARIO_3", "type": "string", "nullable": true, "metadata": {}}, {"name": "CON_DER_BENEFICIARIO_3", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_PDD_DER_BENEFICIARIO_3", "type": "date", "nullable": true, "metadata": {}}, {"name": "COD_ORG_PRT_BENEFICIARIO_4", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_ORG_PRT_BENEFICIARIO_4", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_PRT_BENEFICIARIO_4", "type": "integer", "nullable": true, "metadata": {}}, {"name": "DSC_PRT_BENEFICIARIO_4", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_ORG_TIP_IDT_BENEFICIARIO_4", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_TIP_IDT_BENEFICIARIO_4", "type": "string", "nullable": true, "metadata": {}}, {"name": "NRO_DOC_IDT_BENEFICIARIO_4", "type": "decimal(15,0)", "nullable": true, "metadata": {}}, {"name": "GEN_BENEFICIARIO_4", "type": "string", "nullable": true, "metadata": {}}, {"name": "SEX_BENEFICIARIO_4", "type": "integer", "nullable": true, "metadata": {}}, {"name": "PMR_APE_BENEFICIARIO_4", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_APE_BENEFICIARIO_4", "type": "string", "nullable": true, "metadata": {}}, {"name": "PMR_NOM_BENEFICIARIO_4", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_NOM_BENEFICIARIO_4", "type": "string", "nullable": true, "metadata": {}}, {"name": "NOM_CPT_BENEFICIARIO_4", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_NAC_BENEFICIARIO_4", "type": "timestamp", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_CNY_CMP_4", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_HIJO_4", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "TMP_BNF_BENEFICIARIO_4", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "CNT_HJS_BENEFICIARIO_4", "type": "decimal(2,0)", "nullable": true, "metadata": {}}, {"name": "ETD_BENEFICIARIO_4", "type": "integer", "nullable": true, "metadata": {}}, {"name": "COD_EST_BENEFICIARIO_4", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_EST_BENEFICIARIO_4", "type": "string", "nullable": true, "metadata": {}}, {"name": "CON_DER_BENEFICIARIO_4", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_PDD_DER_BENEFICIARIO_4", "type": "date", "nullable": true, "metadata": {}}, {"name": "COD_ORG_PRT_BENEFICIARIO_5", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_ORG_PRT_BENEFICIARIO_5", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_PRT_BENEFICIARIO_5", "type": "integer", "nullable": true, "metadata": {}}, {"name": "DSC_PRT_BENEFICIARIO_5", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_ORG_TIP_IDT_BENEFICIARIO_5", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_TIP_IDT_BENEFICIARIO_5", "type": "string", "nullable": true, "metadata": {}}, {"name": "NRO_DOC_IDT_BENEFICIARIO_5", "type": "decimal(15,0)", "nullable": true, "metadata": {}}, {"name": "GEN_BENEFICIARIO_5", "type": "string", "nullable": true, "metadata": {}}, {"name": "SEX_BENEFICIARIO_5", "type": "integer", "nullable": true, "metadata": {}}, {"name": "PMR_APE_BENEFICIARIO_5", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_APE_BENEFICIARIO_5", "type": "string", "nullable": true, "metadata": {}}, {"name": "PMR_NOM_BENEFICIARIO_5", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_NOM_BENEFICIARIO_5", "type": "string", "nullable": true, "metadata": {}}, {"name": "NOM_CPT_BENEFICIARIO_5", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_NAC_BENEFICIARIO_5", "type": "timestamp", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_CNY_CMP_5", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_HIJO_5", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "TMP_BNF_BENEFICIARIO_5", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "CNT_HJS_BENEFICIARIO_5", "type": "decimal(2,0)", "nullable": true, "metadata": {}}, {"name": "ETD_BENEFICIARIO_5", "type": "integer", "nullable": true, "metadata": {}}, {"name": "COD_EST_BENEFICIARIO_5", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_EST_BENEFICIARIO_5", "type": "string", "nullable": true, "metadata": {}}, {"name": "CON_DER_BENEFICIARIO_5", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_PDD_DER_BENEFICIARIO_5", "type": "date", "nullable": true, "metadata": {}}, {"name": "COD_ORG_PRT_BENEFICIARIO_6", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_ORG_PRT_BENEFICIARIO_6", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_PRT_BENEFICIARIO_6", "type": "integer", "nullable": true, "metadata": {}}, {"name": "DSC_PRT_BENEFICIARIO_6", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_ORG_TIP_IDT_BENEFICIARIO_6", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_TIP_IDT_BENEFICIARIO_6", "type": "string", "nullable": true, "metadata": {}}, {"name": "NRO_DOC_IDT_BENEFICIARIO_6", "type": "decimal(15,0)", "nullable": true, "metadata": {}}, {"name": "GEN_BENEFICIARIO_6", "type": "string", "nullable": true, "metadata": {}}, {"name": "SEX_BENEFICIARIO_6", "type": "integer", "nullable": true, "metadata": {}}, {"name": "PMR_APE_BENEFICIARIO_6", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_APE_BENEFICIARIO_6", "type": "string", "nullable": true, "metadata": {}}, {"name": "PMR_NOM_BENEFICIARIO_6", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_NOM_BENEFICIARIO_6", "type": "string", "nullable": true, "metadata": {}}, {"name": "NOM_CPT_BENEFICIARIO_6", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_NAC_BENEFICIARIO_6", "type": "timestamp", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_CNY_CMP_6", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_HIJO_6", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "TMP_BNF_BENEFICIARIO_6", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "CNT_HJS_BENEFICIARIO_6", "type": "decimal(2,0)", "nullable": true, "metadata": {}}, {"name": "ETD_BENEFICIARIO_6", "type": "integer", "nullable": true, "metadata": {}}, {"name": "COD_EST_BENEFICIARIO_6", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_EST_BENEFICIARIO_6", "type": "string", "nullable": true, "metadata": {}}, {"name": "CON_DER_BENEFICIARIO_6", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_PDD_DER_BENEFICIARIO_6", "type": "date", "nullable": true, "metadata": {}}, {"name": "COD_ORG_PRT_BENEFICIARIO_7", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_ORG_PRT_BENEFICIARIO_7", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_PRT_BENEFICIARIO_7", "type": "integer", "nullable": true, "metadata": {}}, {"name": "DSC_PRT_BENEFICIARIO_7", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_ORG_TIP_IDT_BENEFICIARIO_7", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_TIP_IDT_BENEFICIARIO_7", "type": "string", "nullable": true, "metadata": {}}, {"name": "NRO_DOC_IDT_BENEFICIARIO_7", "type": "decimal(15,0)", "nullable": true, "metadata": {}}, {"name": "GEN_BENEFICIARIO_7", "type": "string", "nullable": true, "metadata": {}}, {"name": "SEX_BENEFICIARIO_7", "type": "integer", "nullable": true, "metadata": {}}, {"name": "PMR_APE_BENEFICIARIO_7", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_APE_BENEFICIARIO_7", "type": "string", "nullable": true, "metadata": {}}, {"name": "PMR_NOM_BENEFICIARIO_7", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_NOM_BENEFICIARIO_7", "type": "string", "nullable": true, "metadata": {}}, {"name": "NOM_CPT_BENEFICIARIO_7", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_NAC_BENEFICIARIO_7", "type": "timestamp", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_CNY_CMP_7", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_HIJO_7", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "TMP_BNF_BENEFICIARIO_7", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "CNT_HJS_BENEFICIARIO_7", "type": "decimal(2,0)", "nullable": true, "metadata": {}}, {"name": "ETD_BENEFICIARIO_7", "type": "integer", "nullable": true, "metadata": {}}, {"name": "COD_EST_BENEFICIARIO_7", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_EST_BENEFICIARIO_7", "type": "string", "nullable": true, "metadata": {}}, {"name": "CON_DER_BENEFICIARIO_7", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_PDD_DER_BENEFICIARIO_7", "type": "date", "nullable": true, "metadata": {}}, {"name": "COD_ORG_PRT_BENEFICIARIO_8", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_ORG_PRT_BENEFICIARIO_8", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_PRT_BENEFICIARIO_8", "type": "integer", "nullable": true, "metadata": {}}, {"name": "DSC_PRT_BENEFICIARIO_8", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_ORG_TIP_IDT_BENEFICIARIO_8", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_TIP_IDT_BENEFICIARIO_8", "type": "string", "nullable": true, "metadata": {}}, {"name": "NRO_DOC_IDT_BENEFICIARIO_8", "type": "decimal(15,0)", "nullable": true, "metadata": {}}, {"name": "GEN_BENEFICIARIO_8", "type": "string", "nullable": true, "metadata": {}}, {"name": "SEX_BENEFICIARIO_8", "type": "integer", "nullable": true, "metadata": {}}, {"name": "PMR_APE_BENEFICIARIO_8", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_APE_BENEFICIARIO_8", "type": "string", "nullable": true, "metadata": {}}, {"name": "PMR_NOM_BENEFICIARIO_8", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_NOM_BENEFICIARIO_8", "type": "string", "nullable": true, "metadata": {}}, {"name": "NOM_CPT_BENEFICIARIO_8", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_NAC_BENEFICIARIO_8", "type": "timestamp", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_CNY_CMP_8", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_HIJO_8", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "TMP_BNF_BENEFICIARIO_8", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "CNT_HJS_BENEFICIARIO_8", "type": "decimal(2,0)", "nullable": true, "metadata": {}}, {"name": "ETD_BENEFICIARIO_8", "type": "integer", "nullable": true, "metadata": {}}, {"name": "COD_EST_BENEFICIARIO_8", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_EST_BENEFICIARIO_8", "type": "string", "nullable": true, "metadata": {}}, {"name": "CON_DER_BENEFICIARIO_8", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_PDD_DER_BENEFICIARIO_8", "type": "date", "nullable": true, "metadata": {}}, {"name": "COD_ORG_PRT_BENEFICIARIO_9", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_ORG_PRT_BENEFICIARIO_9", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_PRT_BENEFICIARIO_9", "type": "integer", "nullable": true, "metadata": {}}, {"name": "DSC_PRT_BENEFICIARIO_9", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_ORG_TIP_IDT_BENEFICIARIO_9", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_TIP_IDT_BENEFICIARIO_9", "type": "string", "nullable": true, "metadata": {}}, {"name": "NRO_DOC_IDT_BENEFICIARIO_9", "type": "decimal(15,0)", "nullable": true, "metadata": {}}, {"name": "GEN_BENEFICIARIO_9", "type": "string", "nullable": true, "metadata": {}}, {"name": "SEX_BENEFICIARIO_9", "type": "integer", "nullable": true, "metadata": {}}, {"name": "PMR_APE_BENEFICIARIO_9", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_APE_BENEFICIARIO_9", "type": "string", "nullable": true, "metadata": {}}, {"name": "PMR_NOM_BENEFICIARIO_9", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_NOM_BENEFICIARIO_9", "type": "string", "nullable": true, "metadata": {}}, {"name": "NOM_CPT_BENEFICIARIO_9", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_NAC_BENEFICIARIO_9", "type": "timestamp", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_CNY_CMP_9", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_HIJO_9", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "TMP_BNF_BENEFICIARIO_9", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "CNT_HJS_BENEFICIARIO_9", "type": "decimal(2,0)", "nullable": true, "metadata": {}}, {"name": "ETD_BENEFICIARIO_9", "type": "integer", "nullable": true, "metadata": {}}, {"name": "COD_EST_BENEFICIARIO_9", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_EST_BENEFICIARIO_9", "type": "string", "nullable": true, "metadata": {}}, {"name": "CON_DER_BENEFICIARIO_9", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_PDD_DER_BENEFICIARIO_9", "type": "date", "nullable": true, "metadata": {}}, {"name": "COD_ORG_PRT_BENEFICIARIO_10", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_ORG_PRT_BENEFICIARIO_10", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_PRT_BENEFICIARIO_10", "type": "integer", "nullable": true, "metadata": {}}, {"name": "DSC_PRT_BENEFICIARIO_10", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_ORG_TIP_IDT_BENEFICIARIO_10", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_TIP_IDT_BENEFICIARIO_10", "type": "string", "nullable": true, "metadata": {}}, {"name": "NRO_DOC_IDT_BENEFICIARIO_10", "type": "decimal(15,0)", "nullable": true, "metadata": {}}, {"name": "GEN_BENEFICIARIO_10", "type": "string", "nullable": true, "metadata": {}}, {"name": "SEX_BENEFICIARIO_10", "type": "integer", "nullable": true, "metadata": {}}, {"name": "PMR_APE_BENEFICIARIO_10", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_APE_BENEFICIARIO_10", "type": "string", "nullable": true, "metadata": {}}, {"name": "PMR_NOM_BENEFICIARIO_10", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_NOM_BENEFICIARIO_10", "type": "string", "nullable": true, "metadata": {}}, {"name": "NOM_CPT_BENEFICIARIO_10", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_NAC_BENEFICIARIO_10", "type": "timestamp", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_CNY_CMP_10", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_HIJO_10", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "TMP_BNF_BENEFICIARIO_10", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "CNT_HJS_BENEFICIARIO_10", "type": "decimal(2,0)", "nullable": true, "metadata": {}}, {"name": "ETD_BENEFICIARIO_10", "type": "integer", "nullable": true, "metadata": {}}, {"name": "COD_EST_BENEFICIARIO_10", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_EST_BENEFICIARIO_10", "type": "string", "nullable": true, "metadata": {}}, {"name": "CON_DER_BENEFICIARIO_10", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_PDD_DER_BENEFICIARIO_10", "type": "date", "nullable": true, "metadata": {}}, {"name": "COD_ORG_PRT_BENEFICIARIO_11", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_ORG_PRT_BENEFICIARIO_11", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_PRT_BENEFICIARIO_11", "type": "integer", "nullable": true, "metadata": {}}, {"name": "DSC_PRT_BENEFICIARIO_11", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_ORG_TIP_IDT_BENEFICIARIO_11", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_TIP_IDT_BENEFICIARIO_11", "type": "string", "nullable": true, "metadata": {}}, {"name": "NRO_DOC_IDT_BENEFICIARIO_11", "type": "decimal(15,0)", "nullable": true, "metadata": {}}, {"name": "GEN_BENEFICIARIO_11", "type": "string", "nullable": true, "metadata": {}}, {"name": "SEX_BENEFICIARIO_11", "type": "integer", "nullable": true, "metadata": {}}, {"name": "PMR_APE_BENEFICIARIO_11", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_APE_BENEFICIARIO_11", "type": "string", "nullable": true, "metadata": {}}, {"name": "PMR_NOM_BENEFICIARIO_11", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_NOM_BENEFICIARIO_11", "type": "string", "nullable": true, "metadata": {}}, {"name": "NOM_CPT_BENEFICIARIO_11", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_NAC_BENEFICIARIO_11", "type": "timestamp", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_CNY_CMP_11", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_HIJO_11", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "TMP_BNF_BENEFICIARIO_11", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "CNT_HJS_BENEFICIARIO_11", "type": "decimal(2,0)", "nullable": true, "metadata": {}}, {"name": "ETD_BENEFICIARIO_11", "type": "integer", "nullable": true, "metadata": {}}, {"name": "COD_EST_BENEFICIARIO_11", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_EST_BENEFICIARIO_11", "type": "string", "nullable": true, "metadata": {}}, {"name": "CON_DER_BENEFICIARIO_11", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_PDD_DER_BENEFICIARIO_11", "type": "date", "nullable": true, "metadata": {}}, {"name": "COD_ORG_PRT_BENEFICIARIO_12", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_ORG_PRT_BENEFICIARIO_12", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_PRT_BENEFICIARIO_12", "type": "integer", "nullable": true, "metadata": {}}, {"name": "DSC_PRT_BENEFICIARIO_12", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_ORG_TIP_IDT_BENEFICIARIO_12", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_TIP_IDT_BENEFICIARIO_12", "type": "string", "nullable": true, "metadata": {}}, {"name": "NRO_DOC_IDT_BENEFICIARIO_12", "type": "decimal(15,0)", "nullable": true, "metadata": {}}, {"name": "GEN_BENEFICIARIO_12", "type": "string", "nullable": true, "metadata": {}}, {"name": "SEX_BENEFICIARIO_12", "type": "integer", "nullable": true, "metadata": {}}, {"name": "PMR_APE_BENEFICIARIO_12", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_APE_BENEFICIARIO_12", "type": "string", "nullable": true, "metadata": {}}, {"name": "PMR_NOM_BENEFICIARIO_12", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_NOM_BENEFICIARIO_12", "type": "string", "nullable": true, "metadata": {}}, {"name": "NOM_CPT_BENEFICIARIO_12", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_NAC_BENEFICIARIO_12", "type": "timestamp", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_CNY_CMP_12", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_HIJO_12", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "TMP_BNF_BENEFICIARIO_12", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "CNT_HJS_BENEFICIARIO_12", "type": "decimal(2,0)", "nullable": true, "metadata": {}}, {"name": "ETD_BENEFICIARIO_12", "type": "integer", "nullable": true, "metadata": {}}, {"name": "COD_EST_BENEFICIARIO_12", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_EST_BENEFICIARIO_12", "type": "string", "nullable": true, "metadata": {}}, {"name": "CON_DER_BENEFICIARIO_12", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_PDD_DER_BENEFICIARIO_12", "type": "date", "nullable": true, "metadata": {}}, {"name": "COD_ORG_PRT_BENEFICIARIO_13", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_ORG_PRT_BENEFICIARIO_13", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_PRT_BENEFICIARIO_13", "type": "integer", "nullable": true, "metadata": {}}, {"name": "DSC_PRT_BENEFICIARIO_13", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_ORG_TIP_IDT_BENEFICIARIO_13", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_TIP_IDT_BENEFICIARIO_13", "type": "string", "nullable": true, "metadata": {}}, {"name": "NRO_DOC_IDT_BENEFICIARIO_13", "type": "decimal(15,0)", "nullable": true, "metadata": {}}, {"name": "GEN_BENEFICIARIO_13", "type": "string", "nullable": true, "metadata": {}}, {"name": "SEX_BENEFICIARIO_13", "type": "integer", "nullable": true, "metadata": {}}, {"name": "PMR_APE_BENEFICIARIO_13", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_APE_BENEFICIARIO_13", "type": "string", "nullable": true, "metadata": {}}, {"name": "PMR_NOM_BENEFICIARIO_13", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_NOM_BENEFICIARIO_13", "type": "string", "nullable": true, "metadata": {}}, {"name": "NOM_CPT_BENEFICIARIO_13", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_NAC_BENEFICIARIO_13", "type": "timestamp", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_CNY_CMP_13", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_HIJO_13", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "TMP_BNF_BENEFICIARIO_13", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "CNT_HJS_BENEFICIARIO_13", "type": "decimal(2,0)", "nullable": true, "metadata": {}}, {"name": "ETD_BENEFICIARIO_13", "type": "integer", "nullable": true, "metadata": {}}, {"name": "COD_EST_BENEFICIARIO_13", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_EST_BENEFICIARIO_13", "type": "string", "nullable": true, "metadata": {}}, {"name": "CON_DER_BENEFICIARIO_13", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_PDD_DER_BENEFICIARIO_13", "type": "date", "nullable": true, "metadata": {}}, {"name": "COD_ORG_PRT_BENEFICIARIO_14", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_ORG_PRT_BENEFICIARIO_14", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_PRT_BENEFICIARIO_14", "type": "integer", "nullable": true, "metadata": {}}, {"name": "DSC_PRT_BENEFICIARIO_14", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_ORG_TIP_IDT_BENEFICIARIO_14", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_TIP_IDT_BENEFICIARIO_14", "type": "string", "nullable": true, "metadata": {}}, {"name": "NRO_DOC_IDT_BENEFICIARIO_14", "type": "decimal(15,0)", "nullable": true, "metadata": {}}, {"name": "GEN_BENEFICIARIO_14", "type": "string", "nullable": true, "metadata": {}}, {"name": "SEX_BENEFICIARIO_14", "type": "integer", "nullable": true, "metadata": {}}, {"name": "PMR_APE_BENEFICIARIO_14", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_APE_BENEFICIARIO_14", "type": "string", "nullable": true, "metadata": {}}, {"name": "PMR_NOM_BENEFICIARIO_14", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_NOM_BENEFICIARIO_14", "type": "string", "nullable": true, "metadata": {}}, {"name": "NOM_CPT_BENEFICIARIO_14", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_NAC_BENEFICIARIO_14", "type": "timestamp", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_CNY_CMP_14", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_HIJO_14", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "TMP_BNF_BENEFICIARIO_14", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "CNT_HJS_BENEFICIARIO_14", "type": "decimal(2,0)", "nullable": true, "metadata": {}}, {"name": "ETD_BENEFICIARIO_14", "type": "integer", "nullable": true, "metadata": {}}, {"name": "COD_EST_BENEFICIARIO_14", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_EST_BENEFICIARIO_14", "type": "string", "nullable": true, "metadata": {}}, {"name": "CON_DER_BENEFICIARIO_14", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_PDD_DER_BENEFICIARIO_14", "type": "date", "nullable": true, "metadata": {}}, {"name": "COD_ORG_PRT_BENEFICIARIO_15", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_ORG_PRT_BENEFICIARIO_15", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_PRT_BENEFICIARIO_15", "type": "integer", "nullable": true, "metadata": {}}, {"name": "DSC_PRT_BENEFICIARIO_15", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_ORG_TIP_IDT_BENEFICIARIO_15", "type": "string", "nullable": true, "metadata": {}}, {"name": "COD_TIP_IDT_BENEFICIARIO_15", "type": "string", "nullable": true, "metadata": {}}, {"name": "NRO_DOC_IDT_BENEFICIARIO_15", "type": "decimal(15,0)", "nullable": true, "metadata": {}}, {"name": "GEN_BENEFICIARIO_15", "type": "string", "nullable": true, "metadata": {}}, {"name": "SEX_BENEFICIARIO_15", "type": "integer", "nullable": true, "metadata": {}}, {"name": "PMR_APE_BENEFICIARIO_15", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_APE_BENEFICIARIO_15", "type": "string", "nullable": true, "metadata": {}}, {"name": "PMR_NOM_BENEFICIARIO_15", "type": "string", "nullable": true, "metadata": {}}, {"name": "SGN_NOM_BENEFICIARIO_15", "type": "string", "nullable": true, "metadata": {}}, {"name": "NOM_CPT_BENEFICIARIO_15", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_NAC_BENEFICIARIO_15", "type": "timestamp", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_CNY_CMP_15", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "EDD_BEN_HIJO_15", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "TMP_BNF_BENEFICIARIO_15", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "CNT_HJS_BENEFICIARIO_15", "type": "decimal(2,0)", "nullable": true, "metadata": {}}, {"name": "ETD_BENEFICIARIO_15", "type": "integer", "nullable": true, "metadata": {}}, {"name": "COD_EST_BENEFICIARIO_15", "type": "string", "nullable": true, "metadata": {}}, {"name": "DSC_EST_BENEFICIARIO_15", "type": "string", "nullable": true, "metadata": {}}, {"name": "CON_DER_BENEFICIARIO_15", "type": "string", "nullable": true, "metadata": {}}, {"name": "FEC_PDD_DER_BENEFICIARIO_15", "type": "date", "nullable": true, "metadata": {}}, {"name": "NRO_RAD_NOT_TEC", "type": "long", "nullable": true, "metadata": {}}, {"name": "INT_TEC_PRIMA", "type": "decimal(18,6)", "nullable": true, "metadata": {}}, {"name": "TAS_CREC_PRIMA", "type": "decimal(5,2)", "nullable": true, "metadata": {}}, {"name": "GST_PRIMA", "type": "decimal(18,6)", "nullable": true, "metadata": {}}, {"name": "PRIMA", "type": "decimal(18,6)", "nullable": true, "metadata": {}}, {"name": "FEC_INI_RNT_DIFERIDA", "type": "date", "nullable": true, "metadata": {}}, {"name": "VLR_MSD_BNF_PER_DIFERIDA", "type": "decimal(18,6)", "nullable": true, "metadata": {}}, {"name": "VLR_AUX_FUNERARIO", "type": "decimal(16,0)", "nullable": true, "metadata": {}}, {"name": "OBSERVACIONES", "type": "string", "nullable": true, "metadata": {}}, {"name": "VLR_RSV_RV89", "type": "decimal(14,2)", "nullable": true, "metadata": {}}, {"name": "VLR_RSV_RV08", "type": "decimal(14,2)", "nullable": true, "metadata": {}}, {"name": "PAR_0", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "PARMA", "type": "decimal(38,2)", "nullable": true, "metadata": {}}, {"name": "POR_EXC_PARMA", "type": "decimal(18,6)", "nullable": true, "metadata": {}}, {"name": "POR_AMT_ACUM", "type": "decimal(38,0)", "nullable": true, "metadata": {}}, {"name": "FEC_AVI_SINIESTRO", "type": "date", "nullable": true, "metadata": {}}, {"name": "FAR_VALUE", "type": "decimal(18,6)", "nullable": true, "metadata": {}}, {"name": "INS_MECANISMO", "type": "integer", "nullable": true, "metadata": {}}, {"name": "FEC_INS_MECANISMO", "type": "date", "nullable": true, "metadata": {}}, {"name": "PRM_DESLIZAMIENTO", "type": "decimal(5,2)", "nullable": true, "metadata": {}}, {"name": "TAS_FAC_SEGURIDAD", "type": "decimal(38,4)", "nullable": true, "metadata": {}}, {"name": "VLR_COBERTURA", "type": "decimal(18,6)", "nullable": true, "metadata": {}}, {"name": "ITR_TEC_NIIF4", "type": "decimal(18,6)", "nullable": true, "metadata": {}}, {"name": "NID_AUDITORIA_MEDICA", "type": "long", "nullable": true, "metadata": {}}, {"name": "NID_CALCULO", "type": "long", "nullable": true, "metadata": {}}, {"name": "NID_CALCULO_2", "type": "long", "nullable": true, "metadata": {}}, {"name": "NID_SINIESTRO_RESERVA_PCJ", "type": "long", "nullable": true, "metadata": {}}, {"name": "NID_HIST_DATOS_INV_MTE_INI", "type": "long", "nullable": true, "metadata": {}}, {"name": "NID_HIST_DATOS_INV_MTE_ACT", "type": "long", "nullable": true, "metadata": {}}, {"name": "FEC_INI_VIG_REG", "type": "timestamp", "nullable": true, "metadata": {}}, {"name": "FEC_FIN_VIG_REG", "type": "timestamp", "nullable": true, "metadata": {}}, {"name": "VRS_REG", "type": "integer", "nullable": true, "metadata": {}}, {"name": "ACT_REG", "type": "integer", "nullable": true, "metadata": {}}, {"name": "LOTE_CARGUE_ID", "type": "long", "nullable": true, "metadata": {}}]}'))
    df_preparado_vacio = spark.createDataFrame([], schema_vacio)
    (
        df_preparado_vacio.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(slv_prep_target)
    )

    fecha_preparacion = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    lotes_usados_preparacion = {
        tabla: [] for tabla in TABLAS_REQUERIDAS
    }
    parametros_extraccion["criterio_preparacion_silver"] = {
        "tabla_pivote_segmentada": "arpprod/sar_datos_invalidez_muerte",
        "pivote": "ENTRADA_FUNCIONAL_VACIA_CONFIRMADA",
        "dependencias": "NO_LEIDAS_POR_ENTRADA_FUNCIONAL_VACIA",
        "lote_cargue_id": "TRAZABILIDAD_NO_LLAVE_DE_ARTICULACION"
    }
    parametros_extraccion["estado_preparacion_silver"] = "SUCCESS"
    parametros_extraccion["ruta_silver_preparada"] = slv_prep_target
    parametros_extraccion["ruta_silver_preparada_base"] = slv_prep_base
    parametros_extraccion["ruta_tmp_preparacion_silver"] = tmp_target
    parametros_extraccion["modo_escritura_silver_preparada"] = "overwrite_por_per_corte"
    parametros_extraccion["silver_preparada_historica"] = False
    parametros_extraccion["total_registros_silver_preparada"] = 0
    parametros_extraccion["fecha_preparacion_silver"] = fecha_preparacion
    parametros_extraccion["ultimo_per_corte_preparado"] = per_corte
    parametros_extraccion["lotes_usados_preparacion_silver"] = lotes_usados_preparacion
    parametros_extraccion.setdefault("silver_preparaciones", {})
    parametros_extraccion["silver_preparaciones"][per_corte] = {
        "estado": "SUCCESS",
        "nombre_caso": "reserva_matematica_arp",
        "per_corte": per_corte,
        "ruta_silver_preparada": slv_prep_target,
        "ruta_silver_preparada_base": slv_prep_base,
        "ruta_tmp_preparacion": tmp_target,
        "modo_escritura": "overwrite_por_per_corte",
        "total_registros": 0,
        "lote_cargue_id": lote_cargue_id,
        "lotes_usados_preparacion": lotes_usados_preparacion,
        "estado_validacion": estado_validacion,
        "requiere_revision_analitica": requiere_revision_analitica,
        "estado_hallazgos_validacion": estado_hallazgos_validacion,
        "fecha_preparacion": fecha_preparacion,
        "observacion": "Silver preparada ARP vacia por entrada funcional vacia confirmada."
    }
    parametros_extraccion["silver_preparacion"] = (
        parametros_extraccion["silver_preparaciones"][per_corte]
    )
    guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)
    logger.info("Silver preparada ARP vacia publicada con esquema y estado SUCCESS.")
    job.commit()

else:
    # El procesamiento normal solo se ejecuta cuando la entrada no está
    # funcionalmente vacía.
    # =================================================
    logger.info("Leyendo 18 datasets desde Silver base.")

    sar_datos = leer_silver_base(spark, ruta_destino("arpprod/sar_datos_invalidez_muerte"), parametros_extraccion, "arpprod/sar_datos_invalidez_muerte", ds("arpprod/sar_datos_invalidez_muerte"), lote_cargue_id, "SAR_DATOS_INVALIDEZ_MUERTE")
    sar_siniestros = leer_silver_base(spark, ruta_destino("arpprod/sar_siniestros"), parametros_extraccion, "arpprod/sar_siniestros", ds("arpprod/sar_siniestros"), lote_cargue_id, "SAR_SINIESTROS")
    par_trabajadores = leer_silver_base(spark, ruta_destino("arpprod/par_trabajadores"), parametros_extraccion, "arpprod/par_trabajadores", ds("arpprod/par_trabajadores"), lote_cargue_id, "PAR_TRABAJADORES")
    sar_auditoria = leer_silver_base(spark, ruta_destino("arpprod/sar_auditoria_medica"), parametros_extraccion, "arpprod/sar_auditoria_medica", ds("arpprod/sar_auditoria_medica"), lote_cargue_id, "SAR_AUDITORIA_MEDICA")
    ind_reservas = leer_silver_base(spark, ruta_destino("arpprod/ind_datos_reservas_siniestros"), parametros_extraccion, "arpprod/ind_datos_reservas_siniestros", ds("arpprod/ind_datos_reservas_siniestros"), lote_cargue_id, "IND_DATOS_RESERVAS_SINIESTROS")
    hist_datos = leer_silver_base(spark, ruta_destino("arpprod/hist_datos_invalidez_muerte"), parametros_extraccion, "arpprod/hist_datos_invalidez_muerte", ds("arpprod/hist_datos_invalidez_muerte"), lote_cargue_id, "HIST_DATOS_INVALIDEZ_MUERTE")
    sar_calculos = leer_silver_base(spark, ruta_destino("arpprod/sar_calculos_actuariales"), parametros_extraccion, "arpprod/sar_calculos_actuariales", ds("arpprod/sar_calculos_actuariales"), lote_cargue_id, "SAR_CALCULOS_ACTUARIALES")
    hist_pcl = leer_silver_base(spark, ruta_destino("arpprod/hist_sar_pcl_pension"), parametros_extraccion, "arpprod/hist_sar_pcl_pension", ds("arpprod/hist_sar_pcl_pension"), lote_cargue_id, "HIST_SAR_PCL_PENSION")
    amortizacion = leer_silver_base(spark, ruta_destino("arpprod/sar_reserva_pcj_amortizacion"), parametros_extraccion, "arpprod/sar_reserva_pcj_amortizacion", ds("arpprod/sar_reserva_pcj_amortizacion"), lote_cargue_id, "SAR_RESERVA_PCJ_AMORTIZACION")
    benef_siniestros = leer_silver_base(spark, ruta_destino("arpprod/sar_beneficiarios_siniestros"), parametros_extraccion, "arpprod/sar_beneficiarios_siniestros", ds("arpprod/sar_beneficiarios_siniestros"), lote_cargue_id, "SAR_BENEFICIARIOS_SINIESTROS")
    personas = leer_silver_base(spark, ruta_destino("general/acu_personas_naturales"), parametros_extraccion, "general/acu_personas_naturales", ds("general/acu_personas_naturales"), lote_cargue_id, "ACU_PERSONAS_NATURALES")
    equiv_tipos = leer_silver_base(spark, ruta_destino("general/cgr_equivalencia_tipos_id"), parametros_extraccion, "general/cgr_equivalencia_tipos_id", ds("general/cgr_equivalencia_tipos_id"), lote_cargue_id, "CGR_EQUIVALENCIA_TIPOS_ID")
    ipc = leer_silver_base(spark, ruta_destino("general/anp_ipc"), parametros_extraccion, "general/anp_ipc", ds("general/anp_ipc"), lote_cargue_id, "ANP_IPC")
    origenes = leer_silver_base(spark, ruta_destino("general/cgr_origenes"), parametros_extraccion, "general/cgr_origenes", ds("general/cgr_origenes"), lote_cargue_id, "CGR_ORIGENES")
    rva_transic = leer_silver_base(spark, ruta_destino("general/rva_reserva_matematica_transic"), parametros_extraccion, "general/rva_reserva_matematica_transic", ds("general/rva_reserva_matematica_transic"), lote_cargue_id, "RVA_RESERVA_MATEMATICA_TRANSIC")
    salarios = leer_silver_base(spark, ruta_destino("general/pgr_salarios_minimos"), parametros_extraccion, "general/pgr_salarios_minimos", ds("general/pgr_salarios_minimos"), lote_cargue_id, "PGR_SALARIOS_MINIMOS")
    estados_personas = leer_silver_base(spark, ruta_destino("general/cgr_estados_personas"), parametros_extraccion, "general/cgr_estados_personas", ds("general/cgr_estados_personas"), lote_cargue_id, "CGR_ESTADOS_PERSONAS")
    parentescos = leer_silver_base(spark, ruta_destino("general/cgr_parentescos"), parametros_extraccion, "general/cgr_parentescos", ds("general/cgr_parentescos"), lote_cargue_id, "CGR_PARENTESCOS")


    # =================================================
    # BASE PRINCIPAL rm_arp_0 a rm_arp_8
    # =================================================
    fec_ini_per_corte_expr = to_date(lit(f"{per_corte}01"), "yyyyMMdd")
    fec_fin_per_corte_expr = last_day(fec_ini_per_corte_expr)

    logger.info("Construyendo base principal Reserva Matemática ARP.")

    # rm_arp_2: universo principal y marca histórico vigente
    hd_w_latest = Window.partitionBy(col("hd.NID_SINIESTRO"), col("hd.NID_AUDITORIA_MEDICA")).orderBy(desc(col("hd.FEC_REGISTRA")), desc(col("hd.NID_HIST_DATOS_INV_MTE")))
    hd_w_first = Window.partitionBy(col("hd.NID_SINIESTRO"), col("hd.NID_AUDITORIA_MEDICA")).orderBy(asc(col("hd.FEC_REGISTRA")), asc(col("hd.NID_HIST_DATOS_INV_MTE")))

    rm_arp_2 = (
        sar_datos.alias("i")
        .join(sar_siniestros.alias("s"), col("i.NID_SINIESTRO") == col("s.NID_SINIESTRO"), "inner")
        .join(par_trabajadores.alias("t"), col("s.NID_TRABAJADOR") == col("t.NID_TRABAJADOR"), "inner")
        .join(
            sar_auditoria.alias("a"),
            (col("s.NID_SINIESTRO") == col("a.NID_SINIESTRO")) &
            (col("i.NID_AUDITORIA_MEDICA") == col("a.NID_AUDITORIA_MEDICA")),
            "inner"
        )
        .join(equiv_tipos.alias("e"), col("t.COD_TIPO_IDENTIFICACION") == col("e.COD_TIPO_DOCUMENTO"), "inner")
        .join(
            personas.alias("apn"),
            (col("t.NID_NUMERO_IDENTIFICACION") == col("apn.NID_NUMERO_IDENTIFICACION")) &
            (col("t.COD_TIPO_IDENTIFICACION") == col("apn.COD_TIPO_IDENTIFICACION")),
            "inner"
        )
        .join(ind_reservas.alias("idr"), col("s.NID_SINIESTRO") == col("idr.NID_SINIESTRO"), "inner")
        .join(
            hist_datos.alias("hd"),
            (col("i.NID_SINIESTRO") == col("hd.NID_SINIESTRO")) &
            (col("i.NID_AUDITORIA_MEDICA") == col("hd.NID_AUDITORIA_MEDICA")) &
            (col("hd.FEC_APROBACION_PENSION") <= fec_fin_per_corte_expr) &
            (col("hd.FEC_REGISTRA") <= fec_fin_per_corte_expr),
            "inner"
        )
        .filter(
            (col("i.FEC_APROBACION_PENSION") <= fec_fin_per_corte_expr) &
            (col("a.COD_CONCEPTO_MEDICO_AM").isin("4", "5", 4, 5)) &
            (col("i.COD_ESTADO_RESERVA").cast("string") == lit("1"))
        )
        .select(
            col("t.NID_AFILIACION").alias("no_poliza"),
            col("s.NID_SINIESTRO").alias("no_siniestro"),
            col("s.FEC_SINIESTRO").alias("fecha_causacion_derecho"),
            date_format(col("s.FEC_SINIESTRO"), "yyyyMM").cast("int").alias("periodo_causacion_derecho"),
            to_date(concat(date_format(col("s.FEC_SINIESTRO"), "yyyyMM"), lit("01")), "yyyyMMdd").alias("fecha_inicio_periodo_causacion_derecho"),
            last_day(to_date(concat(date_format(col("s.FEC_SINIESTRO"), "yyyyMM"), lit("01")), "yyyyMMdd")).alias("fecha_fin_periodo_causacion_derecho"),
            col("t.COD_TIPO_IDENTIFICACION").alias("cod_tipo_identificacion"),
            col("e.COD_TIPO_SUPER").alias("tipo_identif"),
            col("t.NID_NUMERO_IDENTIFICACION").alias("no_identif"),
            col("apn.GENERO").alias("genero"),
            lower(col("apn.GENERO")).alias("genero_l"),
            col("apn.FEC_NACIMIENTO").alias("fecha_nacimiento"),
            col("apn.PRIMER_APELLIDO").alias("primer_apellido"),
            trim(col("apn.PRIMER_APELLIDO")).alias("primer_apellido_t"),
            col("apn.SEGUNDO_APELLIDO").alias("segundo_apellido"),
            trim(col("apn.SEGUNDO_APELLIDO")).alias("segundo_apellido_t"),
            col("apn.PRIMER_NOMBRE").alias("primer_nombre"),
            trim(col("apn.PRIMER_NOMBRE")).alias("primer_nombre_t"),
            col("apn.SEGUNDO_NOMBRE").alias("segundo_nombre"),
            trim(col("apn.SEGUNDO_NOMBRE")).alias("segundo_nombre_t"),
            col("idr.VRL_IBL_MANUAL").alias("ingreso_base_liquidacion"),
            col("apn.FEC_MUERTE").alias("fecha_muerte_afiliado"),
            col("a.COD_CONCEPTO_MEDICO_AM").alias("cod_concepto_medico_am"),
            col("a.NID_AUDITORIA_MEDICA").alias("nid_auditoria_medica"),
            lit(39).alias("ramo"),
            col("i.FEC_APROBACION_PENSION").alias("vig_inicial"),
            col("i.VLR_PENSION").alias("mesada"),
            col("i.FEC_APROBACION_PENSION").alias("fecha_exped_poliza"),
            col("i.COD_ESTADO_RESERVA").alias("cod_estado_reserva"),
            date_format(col("i.FEC_APROBACION_PENSION"), "yyyy").cast("int").alias("anio_exped_poliza"),
            date_format(col("i.FEC_APROBACION_PENSION"), "yyyyMM").cast("int").alias("per_exped_poliza"),
            to_date(date_format(col("i.FEC_APROBACION_PENSION"), "yyyyMMdd"), "yyyyMMdd").alias("fec_ini_per_exp_poliza"),
            last_day(col("i.FEC_APROBACION_PENSION")).alias("fec_fin_per_exp_poliza"),
            col("hd.NID_HIST_DATOS_INV_MTE").alias("nid_hist_datos_inv_mte"),
            col("hd.FEC_REGISTRA").alias("fec_registra"),
            col("hd.FEC_APROBACION_PENSION").alias("fec_aprobacion_pension"),
            col("hd.FEC_DICTAMEN_INVMTE").alias("fec_dictamen_invmte"),
            col("hd.VLR_PENSION").alias("vlr_pension"),
            first(col("hd.NID_HIST_DATOS_INV_MTE")).over(hd_w_latest).alias("nid_hist_datos_inv_mte_h"),
            first(col("hd.NID_HIST_DATOS_INV_MTE")).over(hd_w_first).alias("nid_hist_datos_inv_mte_m")
        )
    )

    rm_arp_3 = (
        rm_arp_2.alias("a")
        .join(ipc.alias("ipc"), col("a.anio_exped_poliza") == col("ipc.ANIO"), "left")
        .join(
            hist_datos.alias("hdm"),
            (col("a.no_siniestro") == col("hdm.NID_SINIESTRO")) &
            (col("a.nid_auditoria_medica") == col("hdm.NID_AUDITORIA_MEDICA")) &
            (col("a.nid_hist_datos_inv_mte_m") == col("hdm.NID_HIST_DATOS_INV_MTE")),
            "inner"
        )
        .filter(col("a.nid_hist_datos_inv_mte") == col("a.nid_hist_datos_inv_mte_h"))
        .select(
            lit(39).alias("ramo"),
            lit(1).alias("unidad"),
            lit(None).cast(StringType()).alias("equivalencia"),
            lit(1).alias("parent"),
            lit(None).cast(StringType()).alias("estudios"),
            lit(None).cast(StringType()).alias("retroactivo"),
            lit(2011028468000000).cast(LongType()).alias("no_rad_not_tec"),
            lit(None).cast(DecimalType(18, 6)).alias("interes_tec_prima"),
            lit(None).cast(DecimalType(18, 6)).alias("gastos_prima"),
            lit(None).cast(DecimalType(18, 6)).alias("prima"),
            lit(None).cast(DateType()).alias("fecha_inicial_renta_diferida"),
            lit(None).cast(DecimalType(18, 6)).alias("valor_mesada_benef_perio_difer"),
            lit(None).cast(StringType()).alias("observaciones"),
            lit(None).cast(DecimalType(18, 6)).alias("porcentaje_exc_parma"),
            lit(None).cast(DateType()).alias("fec_aviso_siniestro"),
            lit(None).cast(DecimalType(18, 6)).alias("fair_value"),
            lit(0).alias("inscripcion_mecanismo"),
            lit(None).cast(DateType()).alias("fecha_inscripcion"),
            lit(None).cast(DecimalType(18, 6)).alias("valor_cobertura"),
            lit(None).cast(DecimalType(18, 6)).alias("interes_tecnico_niif4"),
            col("a.*"),
            when(lower(col("a.genero_l")) == lit("m"), lit(1)).when(lower(col("a.genero_l")) == lit("f"), lit(2)).otherwise(lit(None)).alias("sexo"),
            trim(concat_ws(" ", col("a.primer_apellido_t"), col("a.segundo_apellido_t"), col("a.primer_nombre_t"), col("a.segundo_nombre_t"))).alias("nombre"),
            when(col("a.cod_concepto_medico_am").cast("int") == 4, lit(2)).when(col("a.cod_concepto_medico_am").cast("int") == 5, lit(1)).otherwise(lit(0)).alias("cod_concepto_medico_am_h"),
            when(col("a.mesada") == col("a.vlr_pension"), col("a.mesada")).otherwise(col("a.vlr_pension")).alias("mesada_h"),
            col("ipc.PCJ_IPC").alias("tasa_crec_prima"),
            lit(per_corte_int).alias("per_corte"),
            fec_ini_per_corte_expr.alias("fec_ini_per_corte"),
            fec_fin_per_corte_expr.alias("fec_fin_per_corte"),
            lit(anio_corte_int).alias("anio_corte"),
            col("hdm.FEC_REGISTRA").alias("fec_registra_m"),
            col("hdm.FEC_APROBACION_PENSION").alias("fec_aprobacion_pension_m"),
            col("hdm.FEC_DICTAMEN_INVMTE").alias("fec_dictamen_invmte_m"),
            col("hdm.VLR_PENSION").alias("vlr_pension_m")
        )
        .drop("mesada")
        .withColumnRenamed("mesada_h", "mesada")
    )


    # escoger el calculo principal.
    rm_arp_4 = (
        rm_arp_3.alias("r")
        .join(
            rva_transic.alias("rm"),
            col("r.no_siniestro") == col("rm.NID_SINIESTRO_RENTA"),
            "left"
        )
        .join(
            sar_calculos.alias("cal"),
            (col("r.no_siniestro") == col("cal.NID_SINIESTRO")) &
            (to_date(col("cal.FEC_CALCULO")) >= add_months(col("r.fec_ini_per_corte"), -1)) &
            (to_date(col("cal.FEC_CALCULO")) <= col("r.fec_fin_per_corte")),
            "left"
        )
        .select(
            col("r.*"),
            col("cal.COD_ORIGEN").alias("cod_origen_calculo"),
            when(
                col("r.fecha_muerte_afiliado").isNotNull() |
                (col("r.cod_concepto_medico_am_h") == lit(1)),
                lit(3)
            )
            .when(col("r.cod_concepto_medico_am_h") == lit(2), lit(2))
            .otherwise(lit(1))
            .alias("estado"),
            col("rm.VLR_RESERVA_RV89").alias("valor_reserva_rv89"),
            col("rm.VLR_RESERVA_RV08").alias("valor_reserva_rv08"),
            col("rm.RAMO").alias("ramo_reserva"),
            col("cal.NID_CALCULO").alias("nid_calculo"),
            col("cal.FEC_CALCULO").alias("fec_calculo"),
            col("cal.PCJ_ADMON").alias("gastos"),
            col("cal.PCJ_DESLIZAMIENTO_SM").alias("parametro_deslizamiento"),
            col("cal.PCJ_FACTOR_SEGURIDAD").alias("tasa_fac_seg")
        )
        .dropDuplicates()
    )

    # periodo anterior a cierre del periodo actual.
    w_calculo_principal = (
        Window.partitionBy("no_siniestro")
        .orderBy(desc(col("nid_calculo")), desc(col("fec_calculo")))
    )
    rm_arp_4 = (
        rm_arp_4
        .withColumn("_rn_calculo_principal", row_number().over(w_calculo_principal))
        .filter(col("_rn_calculo_principal") == lit(1))
        .drop("_rn_calculo_principal")
    )

    # homologar una sola vez contra CGR_ORIGENES.
    rm_arp_4 = (
        rm_arp_4
        .withColumn(
            "cod_origen_definitivo",
            when(
                (
                    (last_day(to_date(current_timestamp())) > col("fec_fin_per_corte")) |
                    (col("fecha_muerte_afiliado") >= col("fec_fin_per_corte"))
                ) & col("cod_origen_calculo").isNotNull(),
                col("cod_origen_calculo")
            ).otherwise(col("cod_concepto_medico_am_h"))
        )
        .alias("r")
        .join(
            origenes.alias("org"),
            col("r.cod_origen_definitivo").cast("string") ==
            col("org.COD_ORIGEN").cast("string"),
            "left"
        )
        .select(
            col("r.*"),
            col("org.COD_ORIGEN_SUPER").alias("origen_pension"),
            col("org.NOMBRE").alias("d_origen_pension")
        )
    )


    # No se usa current_timestamp() para evitar resultados no deterministas.
    w_calculo_ipc = (
        Window.partitionBy(col("r.no_siniestro"))
        .orderBy(desc(col("cal2.FEC_CALCULO")), desc(col("cal2.NID_CALCULO")))
    )
    rm_arp_4 = (
        rm_arp_4.alias("r")
        .join(
            sar_calculos.alias("cal2"),
            (col("r.no_siniestro") == col("cal2.NID_SINIESTRO")) &
            (to_date(col("cal2.FEC_CALCULO")) <= col("r.fec_fin_per_corte")),
            "left"
        )
        .select(
            col("r.*"),
            col("cal2.NID_CALCULO").alias("nid_calculo_2"),
            col("cal2.PCJ_TK").alias("pcj_tk_calculo"),
            row_number().over(w_calculo_ipc).alias("_rn_calculo_ipc")
        )
        .filter(col("_rn_calculo_ipc") == lit(1))
        .drop("_rn_calculo_ipc")
        .withColumn(
            "tasa_crec_prima",
            coalesce(col("tasa_crec_prima"), col("pcj_tk_calculo"))
        )
    )

    w_pcl_asc = Window.partitionBy(col("p.NID_SINIESTRO")).orderBy(asc(col("p.FEC_REGISTRA")))
    w_pcl_desc = Window.partitionBy(col("p.NID_SINIESTRO")).orderBy(desc(col("p.FEC_REGISTRA")))
    rm_arp_5 = (
        hist_pcl.alias("p")
        .join(sar_auditoria.alias("a"), col("p.NID_SINIESTRO") == col("a.NID_SINIESTRO"), "inner")
        .filter(col("a.COD_CONCEPTO_MEDICO_AM").cast("int") == 4)
        .select(
            col("p.NID_SINIESTRO").alias("nid_siniestro"),
            first(col("p.PCJ_PCL")).over(w_pcl_asc).alias("primero"),
            first(col("p.PCJ_PCL")).over(w_pcl_desc).alias("ultimo"),
            first(col("p.FEC_REGISTRA")).over(w_pcl_desc).alias("ultima_modificacion")
        )
        .dropDuplicates(["nid_siniestro", "primero", "ultimo", "ultima_modificacion"])
    )

    w_calc = Window.partitionBy(col("r.no_siniestro")).orderBy(desc(col("r.nid_calculo")), desc(col("r.fec_calculo")))
    rm_arp_6 = (
        rm_arp_4.alias("r")
        .join(rm_arp_5.alias("pcl"), col("r.no_siniestro") == col("pcl.nid_siniestro"), "left")
        .join(salarios.alias("sm"), (col("r.anio_corte") == col("sm.ANO")) & (col("sm.MES") == lit(1)), "left")
        .join(salarios.alias("sn"), (col("r.anio_exped_poliza") == col("sn.ANO")) & (col("sn.MES") == lit(1)), "left")
        .join(amortizacion.alias("amo"), col("r.no_siniestro") == col("amo.NID_SINIESTRO"), "left")
        .select(
            col("r.*"),
            when(col("pcl.nid_siniestro").isNull(), lit(None))
                .when(col("pcl.primero") == col("pcl.ultimo"), lit(None))
                .otherwise(col("pcl.primero")).alias("grado_invalidez_inicial"),
            when(col("pcl.nid_siniestro").isNull(), lit(None))
                .when(col("pcl.primero") == col("pcl.ultimo"), lit(None))
                .otherwise(col("pcl.ultima_modificacion")).alias("fecha_ultima_mod"),
            when(col("pcl.nid_siniestro").isNull(), lit(None))
                .otherwise(col("pcl.ultimo")).alias("grado_invalidez_actual"),
            first(col("r.nid_calculo")).over(w_calc).alias("no_calculo_1"),
            col("r.nid_calculo").alias("no_calculo"),
            col("r.fec_calculo").alias("fecha_calculo"),
            col("sm.ANO").alias("ano_salario_minimo"),
            col("sm.MES").alias("mes_salario_minimo"),
            col("sm.VLR_SALARIO_MINIMO").alias("vlr_salario_minimo"),
            col("sn.ANO").alias("ano_salario_minimo_sn"),
            col("sn.MES").alias("mes_salario_minimo_sn"),
            col("sn.VLR_SALARIO_MINIMO").alias("vlr_salario_minimo_sn"),
            col("amo.NID_SINIESTRO").alias("nid_siniestro_reserva_pcj")
        )
    )

    # El origen ya fue calculado con la fecha original en rm_arp_4.
    rm_arp_6 = rm_arp_6.withColumn(
        "fecha_muerte_afiliado",
        when(
            col("fecha_muerte_afiliado") >= col("fec_fin_per_corte"),
            lit(None).cast(DateType())
        ).otherwise(col("fecha_muerte_afiliado"))
    )

    rm_arp_7 = (
        rm_arp_6
        .withColumn(
            "no_mesadas",
            when(col("fec_dictamen_invmte") <= to_date(lit("20050725"), "yyyyMMdd"), lit(14))
            .when(
                (col("fec_dictamen_invmte") >= to_date(lit("20050726"), "yyyyMMdd")) &
                (col("fec_dictamen_invmte") <= to_date(lit("20110731"), "yyyyMMdd")),
                when(col("vlr_pension_m") <= (col("vlr_salario_minimo_sn") * lit(3)), lit(14)).otherwise(lit(13))
            )
            .otherwise(lit(13))
        )
        .withColumn(
            "valor_auxilio_funerario",
            when(col("origen_pension").cast("int").isin(1, 3),
                 when(col("mesada") <= (col("vlr_salario_minimo") * lit(5)), spark_round(col("vlr_salario_minimo") * lit(5), 0))
                 .when(col("mesada") <= (col("vlr_salario_minimo") * lit(10)), spark_round(col("mesada"), 0))
                 .otherwise(spark_round(col("vlr_salario_minimo") * lit(10), 0))
            ).otherwise(lit(None))
        )
        .withColumn(
            "par_0",
            when((col("nid_siniestro_reserva_pcj").isNotNull()) & (col("ramo_reserva") == lit("ARP")),
                 spark_round((col("valor_reserva_rv89") / col("valor_reserva_rv08")) * lit(100), 2))
        )
        .withColumn(
            "par_0",
            when(
                (col("par_0") >= lit(0)) & (col("par_0") < lit(100)),
                col("par_0")
            ).otherwise(lit(None).cast(DecimalType(18, 2)))
        )
        .withColumn(
            "parma",
            when((col("nid_siniestro_reserva_pcj").isNotNull()) & (col("ramo_reserva") == lit("ARP")),
                 spark_round((lit(100) - spark_round((col("valor_reserva_rv89") / col("valor_reserva_rv08")) * lit(100), 2)) / lit(240), 10))
        )
        .withColumn(
            "por_amort_acum",
            when((col("nid_siniestro_reserva_pcj").isNotNull()) & (col("ramo_reserva") == lit("ARP")),
                 least(
                     spark_round(
                         spark_round((col("valor_reserva_rv89") / col("valor_reserva_rv08")) * lit(100), 2) +
                         (spark_round((lit(100) - spark_round((col("valor_reserva_rv89") / col("valor_reserva_rv08")) * lit(100), 2)) / lit(240), 10) *
                          (floor(months_between(col("fec_fin_per_corte"), to_date(lit("20180401"), "yyyyMMdd"))) + lit(91)))
                     ),
                     lit(100)
                 ))
        )
        .filter(col("no_calculo") == col("no_calculo_1"))
    )

    rm_arp_8 = rm_arp_7.dropDuplicates()
    rm_arp_8 = materializar(rm_arp_8, f"{tmp_target}base_principal/", "rm_arp_8_base_principal")
    logger.info(f"Registros base principal: {rm_arp_8.count()}")


    # =================================================
    # BENEFICIARIOS NORMALIZADOS
    # =================================================
    logger.info("Construyendo beneficiarios normalizados y pivoteados.")

    rm_arp_ben_1 = (
        rm_arp_8.alias("r")
        .join(benef_siniestros.alias("b"), col("r.no_siniestro") == col("b.NID_SINIESTRO"), "inner")
        .join(equiv_tipos.alias("e"), col("e.COD_TIPO_DOCUMENTO") == col("b.COD_TIPO_IDENTIFICACION"), "inner")
        .join(
            personas.alias("apn"),
            (col("apn.NID_NUMERO_IDENTIFICACION") == col("b.NID_NUMERO_IDENTIFICACION")) &
            (col("apn.COD_TIPO_IDENTIFICACION") == col("b.COD_TIPO_IDENTIFICACION")),
            "inner"
        )
        .join(estados_personas.alias("ep"), col("b.COD_ESTADO_PERSONA") == col("ep.COD_ESTADO_PERSONA"), "inner")
        .join(parentescos.alias("pa"), col("b.COD_PARENTESCO") == col("pa.COD_PARENTESCO"), "inner")
        .select(
            col("r.no_siniestro").alias("nid_rentas"),
            col("b.COD_PARENTESCO").alias("cod_parentesco"),
            col("pa.NOMBRE").alias("dsc_parentesco"),
            col("b.COD_TIPO_IDENTIFICACION").alias("cod_tipo_identificacion"),
            col("e.COD_TIPO_SUPER").alias("tipo_identif"),
            col("b.NID_NUMERO_IDENTIFICACION").alias("no_identif"),
            col("apn.GENERO").alias("genero"),
            lower(col("apn.GENERO")).alias("genero_l"),
            col("r.fecha_muerte_afiliado").alias("fecha_muerte_afiliado"),
            col("apn.FEC_NACIMIENTO").alias("fecha_nacimiento"),
            col("apn.PRIMER_APELLIDO").alias("primer_apellido"),
            trim(col("apn.PRIMER_APELLIDO")).alias("primer_apellido_t"),
            col("apn.SEGUNDO_APELLIDO").alias("segundo_apellido"),
            trim(col("apn.SEGUNDO_APELLIDO")).alias("segundo_apellido_t"),
            col("apn.PRIMER_NOMBRE").alias("primer_nombre"),
            trim(col("apn.PRIMER_NOMBRE")).alias("primer_nombre_t"),
            col("apn.SEGUNDO_NOMBRE").alias("segundo_nombre"),
            trim(col("apn.SEGUNDO_NOMBRE")).alias("segundo_nombre_t"),
            ((months_between(last_day(col("r.fecha_muerte_afiliado")), last_day(col("apn.FEC_NACIMIENTO"))) / lit(12)) - when((date_format(col("r.fecha_muerte_afiliado"), "MM") == date_format(col("apn.FEC_NACIMIENTO"), "MM")) & (date_format(col("r.fecha_muerte_afiliado"), "dd") < date_format(col("apn.FEC_NACIMIENTO"), "dd")), lit(1)).otherwise(lit(0))).alias("edad_beneficiario_cony_com"),
            (months_between(col("r.fec_fin_per_corte"), last_day(col("apn.FEC_NACIMIENTO"))) / lit(12)).alias("edad_beneficiario_hijo"),
            (months_between(col("r.fec_fin_per_corte"), last_day(col("r.fecha_muerte_afiliado"))) / lit(12)).alias("tmp_bnf_beneficiario"),
            col("b.CANT_HIJOS").alias("cant_hijos"),
            col("b.COD_ESTADO_PERSONA").alias("cod_estado"),
            col("ep.NOMBRE").alias("dsc_estado"),
            col("b.BLN_CON_DERECHO").alias("con_derecho"),
            lower(col("b.BLN_CON_DERECHO")).alias("con_derecho_l"),
            col("b.FEC_PERDIDA_DERECHO").cast(DateType()).alias("fec_perdida_derecho")
        )
    )

    ben = (
        rm_arp_ben_1
        .withColumn(
            "cod_parent_beneficiario_h_1",
            when(col("cod_parentesco") == lit("1"), lit(5))
            .when(col("cod_parentesco") == lit("2"), lit(6))
            .when(col("cod_parentesco").isin("3", "10"), lit(4))
            .when(col("cod_parentesco") == lit("4"), lit(3))
            .when(col("cod_parentesco") == lit("6"), lit(8))
            .otherwise(lit(0))
        )
        .withColumn(
            "dsc_parent_beneficiario_h_1",
            when(col("cod_parentesco") == lit("1"), lit("padre"))
            .when(col("cod_parentesco") == lit("2"), lit("madre"))
            .when(col("cod_parentesco").isin("3", "10"), lit("cónyugue o compañera permanente"))
            .when(col("cod_parentesco") == lit("4"), lit("hijo"))
            .when(col("cod_parentesco") == lit("6"), lit("hermano"))
            .otherwise(lit("descartado"))
        )
        .withColumn("sexo_beneficiario", when(col("genero_l") == lit("m"), lit(1)).when(col("genero_l") == lit("f"), lit(2)))
        .withColumn("nombre", concat_ws(" ", col("primer_apellido_t"), col("segundo_apellido_t"), col("primer_nombre_t"), col("segundo_nombre_t")))
        .withColumn("estudios", when((col("cod_parentesco") == lit("4")) & (col("cod_estado") == lit("1")) & (col("edad_beneficiario_hijo") >= lit(18)), lit(1)))
    )

    ben = (
        ben
        .withColumn(
            "cod_parent_beneficiario_h_2",
            when((col("cod_parent_beneficiario_h_1") == lit(5)) & (col("con_derecho_l") == lit("s")), lit(5))
            .when((col("cod_parent_beneficiario_h_1") == lit(6)) & (col("con_derecho_l") == lit("s")), lit(6))
            .when((col("cod_parent_beneficiario_h_1") == lit(4)) & (col("con_derecho_l") == lit("s")), lit(4))
            .when((col("cod_parent_beneficiario_h_1") == lit(3)) & (col("con_derecho_l") == lit("s")), lit(3))
            .when((col("cod_parent_beneficiario_h_1") == lit(8)) & (col("con_derecho_l") == lit("s")), lit(8))
            .otherwise(lit(0))
        )
        .withColumn(
            "dsc_parent_beneficiario_h_2",
            when((col("cod_parent_beneficiario_h_1") == lit(5)) & (col("con_derecho_l") == lit("s")), lit("padre"))
            .when((col("cod_parent_beneficiario_h_1") == lit(6)) & (col("con_derecho_l") == lit("s")), lit("madre"))
            .when((col("cod_parent_beneficiario_h_1") == lit(4)) & (col("con_derecho_l") == lit("s")), lit("cónyugue o compañera permanente"))
            .when((col("cod_parent_beneficiario_h_1") == lit(3)) & (col("con_derecho_l") == lit("s")), lit("hijo"))
            .when((col("cod_parent_beneficiario_h_1") == lit(8)) & (col("con_derecho_l") == lit("s")), lit("hermano"))
            .otherwise(lit("descartado"))
        )
    )

    ben = (
        ben
        .withColumn(
            "cod_parent_beneficiario_h_3",
            when(col("cod_parent_beneficiario_h_2") == lit(5), lit(5))
            .when(col("cod_parent_beneficiario_h_2") == lit(6), lit(6))
            .when(
                (col("cod_parent_beneficiario_h_2") == lit(4))
                & (col("edad_beneficiario_cony_com") < lit(30))
                & (col("cant_hijos") == lit(0))
                & (col("cod_estado") == lit("1"))
                & (col("fecha_muerte_afiliado") >= to_date(lit("20030129"), "yyyyMMdd")),
                lit(7)
            )
            .when(col("cod_parent_beneficiario_h_2") == lit(4), lit(4))
            .when((col("cod_parent_beneficiario_h_2") == lit(3)) & (col("cod_estado") == lit("2")), lit(2))
            .when(col("cod_parent_beneficiario_h_2") == lit(3), lit(3))
            .when((col("cod_parent_beneficiario_h_2") == lit(8)) & (col("cod_estado") == lit("2")), lit(8))
            .otherwise(lit(0))
        )
        .withColumn(
            "dsc_parent_beneficiario_h_3",
            when(col("cod_parent_beneficiario_h_2") == lit(5), lit("padre"))
            .when(col("cod_parent_beneficiario_h_2") == lit(6), lit("madre"))
            .when(
                (col("cod_parent_beneficiario_h_2") == lit(4))
                & (col("edad_beneficiario_cony_com") < lit(30))
                & (col("cant_hijos") == lit(0))
                & (col("cod_estado") == lit("1"))
                & (col("fecha_muerte_afiliado") >= to_date(lit("20030129"), "yyyyMMdd")),
                lit("conyugue compañera temporal")
            )
            .when(col("cod_parent_beneficiario_h_2") == lit(4), lit("cónyugue o compañera permanente"))
            .when((col("cod_parent_beneficiario_h_2") == lit(3)) & (col("cod_estado") == lit("2")), lit("hijo invalido"))
            .when(col("cod_parent_beneficiario_h_2") == lit(3), lit("hijo valido"))
            .when((col("cod_parent_beneficiario_h_2") == lit(8)) & (col("cod_estado") == lit("2")), lit("hermano invalido"))
            .otherwise(lit("descartado"))
        )
        .withColumn("edad_beneficiario_cny_cmp", when((col("cod_parent_beneficiario_h_1") == lit(4)) | (col("cod_parent_beneficiario_h_3") == lit(7)), col("edad_beneficiario_cony_com")).otherwise(lit(0)).cast(DecimalType(38, 2)))
        .withColumn("edad_beneficiario_hijo", when((col("cod_parent_beneficiario_h_1") == lit(2)) | (col("cod_parent_beneficiario_h_1") == lit(3)), col("edad_beneficiario_hijo")).otherwise(lit(0)).cast(DecimalType(38, 2)))
        .withColumn("tmp_bnf_beneficiario", when((col("cod_parent_beneficiario_h_1") == lit(4)) | (col("cod_parent_beneficiario_h_3") == lit(7)), col("tmp_bnf_beneficiario")).otherwise(lit(0)).cast(DecimalType(38, 2)))
    )

    w_ben = Window.partitionBy("nid_rentas").orderBy(col("cod_parentesco"), desc(col("fecha_nacimiento")), col("tipo_identif"), col("no_identif"), desc(col("con_derecho")))
    rm_arp_ben_8 = (
        ben
        .withColumn("num_ben", dense_rank().over(w_ben))
        .filter(col("cod_parent_beneficiario_h_3") != lit(0))
        .select(
            "num_ben", "nid_rentas", "cod_parentesco", "dsc_parentesco",
            col("cod_parent_beneficiario_h_3").alias("cod_parent_beneficiario"),
            col("dsc_parent_beneficiario_h_3").alias("dsc_parent_beneficiario"),
            "cod_tipo_identificacion", "tipo_identif", "no_identif", "genero", "sexo_beneficiario",
            "fecha_muerte_afiliado", "fecha_nacimiento", "primer_apellido", "segundo_apellido",
            "primer_nombre", "segundo_nombre", "nombre", "edad_beneficiario_cny_cmp",
            "edad_beneficiario_hijo", "tmp_bnf_beneficiario", "cant_hijos", "estudios",
            "cod_estado", "dsc_estado", "con_derecho", "fec_perdida_derecho"
        )
    )

    rm_arp_ben_8 = materializar(rm_arp_ben_8, f"{tmp_target}beneficiarios_normalizados/", "rm_arp_ben_8_normalizados")
    logger.info(f"Registros beneficiarios normalizados: {rm_arp_ben_8.count()}")

    # No hijos
    no_hijos = (
        benef_siniestros
        .filter(
            (col("COD_PARENTESCO").cast("string") == lit("4")) &
            (upper(trim(col("BLN_CON_DERECHO").cast("string"))) == lit("S"))
        )
        .groupBy(col("NID_SINIESTRO").alias("nid_rentas"))
        .agg(
            countDistinct(col("NID_BENEFICIARIO").cast("string")).alias("no_hijos")
        )
        .withColumn(
            "no_hijos",
            when(col("no_hijos") == 0, lit(None)).otherwise(col("no_hijos"))
        )
    )

    no_hijos_invalidos = (
        benef_siniestros
        .filter(
            (col("COD_PARENTESCO").cast("string") == lit("4")) &
            (col("COD_ESTADO_PERSONA").cast("string") == lit("2")) &
            (upper(trim(col("BLN_CON_DERECHO").cast("string"))) == lit("S"))
        )
        .groupBy(col("NID_SINIESTRO").alias("nid_rentas"))
        .agg(
            countDistinct(col("NID_BENEFICIARIO").cast("string")).alias("no_hijos_invalidos")
        )
        .withColumn(
            "no_hijos_invalidos",
            when(col("no_hijos_invalidos") == 0, lit(None)).otherwise(col("no_hijos_invalidos"))
        )
    )

    campos_benef = [
        "cod_parentesco", "dsc_parentesco", "cod_parent_beneficiario", "dsc_parent_beneficiario",
        "cod_tipo_identificacion", "tipo_identif", "no_identif", "genero", "sexo_beneficiario",
        "fecha_muerte_afiliado", "fecha_nacimiento", "primer_apellido", "segundo_apellido",
        "primer_nombre", "segundo_nombre", "nombre", "edad_beneficiario_cny_cmp", "edad_beneficiario_hijo",
        "tmp_bnf_beneficiario", "cant_hijos", "estudios", "cod_estado", "dsc_estado", "con_derecho",
        "fec_perdida_derecho"
    ]

    aggs = []
    for i in range(1, 16):
        for campo in campos_benef:
            aggs.append(spark_max(when(col("num_ben") == lit(i), col(campo))).alias(f"{campo}_{i}"))

    benef_pivot = rm_arp_ben_8.groupBy("nid_rentas").agg(*aggs)
    benef_pivot = materializar(benef_pivot, f"{tmp_target}beneficiarios_pivoteados/", "beneficiarios_pivoteados")
    logger.info(f"Registros beneficiarios pivoteados: {benef_pivot.count()}")


    # =================================================
    # BLOQUE E: SALIDA FINAL rm_arp_9 a rm_arp_12
    # =================================================
    logger.info("Construyendo salida final Silver preparada.")

    r = rm_arp_8.alias("r")
    bp = benef_pivot.alias("bp")
    hj = no_hijos.alias("hj")
    hi = no_hijos_invalidos.alias("hi")

    joined = (
        r
        .join(bp, col("r.no_siniestro") == col("bp.nid_rentas"), "left")
        .join(hj, col("r.no_siniestro") == col("hj.nid_rentas"), "left")
        .join(hi, col("r.no_siniestro") == col("hi.nid_rentas"), "left")
    )

    select_cols = [
        col("r.per_corte").cast(IntegerType()).alias("PER_CORTE"),
        col("r.fec_ini_per_corte").cast(DateType()).alias("FEC_INI_PER_CORTE"),
        col("r.fec_fin_per_corte").cast(DateType()).alias("FEC_FIN_PER_CORTE"),
        col("r.anio_corte").cast(IntegerType()).alias("ANI_CORTE"),
        col("r.ramo").cast(IntegerType()).alias("RAMO"),
        col("r.unidad").cast(IntegerType()).alias("UNIDAD"),
        col("r.equivalencia").alias("EQUIVALENCIA"),
        col("r.no_poliza").cast(LongType()).alias("NRO_POLIZA"),
        col("r.fecha_exped_poliza").cast(DateType()).alias("FEC_EXP_POLIZA"),
        col("r.per_exped_poliza").cast(IntegerType()).alias("PER_EXP_POLIZA"),
        col("r.fec_ini_per_exp_poliza").cast(DateType()).alias("FEC_INI_PER_EXP_POLIZA"),
        col("r.fec_fin_per_exp_poliza").cast(DateType()).alias("FEC_FIN_PER_EXP_POLIZA"),
        col("r.anio_exped_poliza").cast(IntegerType()).alias("ANI_EXP_POLIZA"),
        col("r.vig_inicial").cast(DateType()).alias("FEC_VIG_INICIAL"),
        col("r.no_siniestro").cast(LongType()).alias("NRO_SINIESTRO"),
        col("r.fecha_causacion_derecho").cast(DateType()).alias("FEC_CAU_DERECHO"),
        col("r.periodo_causacion_derecho").cast(IntegerType()).alias("PER_CAU_DERECHO"),
        col("r.fecha_inicio_periodo_causacion_derecho").cast(DateType()).alias("FEC_INI_PER_CAU_DERECHO"),
        col("r.fecha_fin_periodo_causacion_derecho").cast(DateType()).alias("FEC_FIN_PER_CAU_DERECHO"),
        col("r.origen_pension").alias("COD_ORG_PENSION"),
        col("r.d_origen_pension").alias("DSC_ORG_PENSION"),
        col("r.parent").alias("PRT_AFILIADO"),
        col("r.cod_tipo_identificacion").alias("COD_ORG_TIP_IDT_AFILIADO"),
        col("r.tipo_identif").alias("COD_TIP_IDT_AFILIADO"),
        col("r.no_identif").cast(LongType()).alias("NRO_DOC_IDT_AFILIADO"),
        col("r.genero").alias("GEN_AFILIADO"),
        col("r.sexo").cast(IntegerType()).alias("SEX_AFILIADO"),
        col("r.primer_apellido").alias("PMR_APE_AFILIADO"),
        col("r.segundo_apellido").alias("SGN_APE_AFILIADO"),
        col("r.primer_nombre").alias("PMR_NOM_AFILIADO"),
        col("r.segundo_nombre").alias("SGN_NOM_AFILIADO"),
        col("r.nombre").alias("NOM_CPT_AFILIADO"),
        col("r.fecha_nacimiento").cast(DateType()).alias("FEC_NAC_AFILIADO"),
        col("r.fecha_muerte_afiliado").cast(DateType()).alias("FEC_MRT_AFILIADO"),
        col("r.estudios").alias("ETD_AFILIADO"),
        col("r.estado").cast(StringType()).alias("EST_AFILIADO"),
        col("hj.no_hijos").cast(IntegerType()).alias("NRO_HJS_TTL_AFILIADO"),
        col("hi.no_hijos_invalidos").cast(IntegerType()).alias("NRO_HJS_INV_AFILIADO"),
        col("r.ingreso_base_liquidacion").cast(DecimalType(18, 2)).alias("IGR_BSE_LIQ_AFILIADO"),
        col("r.mesada").cast(DecimalType(18, 2)).alias("MSD_AFILIADO"),
        col("r.no_mesadas").cast(IntegerType()).alias("NRO_MSD_AFILIADO"),
        col("r.retroactivo").alias("RETROACTIVO"),
        col("r.grado_invalidez_inicial").alias("GRD_INV_INICIAL"),
        col("r.fecha_ultima_mod").cast(DateType()).alias("FEC_ULT_MOD"),
        col("r.grado_invalidez_actual").alias("GRD_INV_ACTUAL"),
        col("r.gastos").alias("GASTOS")
    ]

    alias_benef = {
        "cod_parentesco": "COD_ORG_PRT_BENEFICIARIO",
        "dsc_parentesco": "DSC_ORG_PRT_BENEFICIARIO",
        "cod_parent_beneficiario": "COD_PRT_BENEFICIARIO",
        "dsc_parent_beneficiario": "DSC_PRT_BENEFICIARIO",
        "cod_tipo_identificacion": "COD_ORG_TIP_IDT_BENEFICIARIO",
        "tipo_identif": "COD_TIP_IDT_BENEFICIARIO",
        "no_identif": "NRO_DOC_IDT_BENEFICIARIO",
        "genero": "GEN_BENEFICIARIO",
        "sexo_beneficiario": "SEX_BENEFICIARIO",
        "primer_apellido": "PMR_APE_BENEFICIARIO",
        "segundo_apellido": "SGN_APE_BENEFICIARIO",
        "primer_nombre": "PMR_NOM_BENEFICIARIO",
        "segundo_nombre": "SGN_NOM_BENEFICIARIO",
        "nombre": "NOM_CPT_BENEFICIARIO",
        "fecha_nacimiento": "FEC_NAC_BENEFICIARIO",
        "edad_beneficiario_cny_cmp": "EDD_BEN_CNY_CMP",
        "edad_beneficiario_hijo": "EDD_BEN_HIJO",
        "tmp_bnf_beneficiario": "TMP_BNF_BENEFICIARIO",
        "cant_hijos": "CNT_HJS_BENEFICIARIO",
        "estudios": "ETD_BENEFICIARIO",
        "cod_estado": "COD_EST_BENEFICIARIO",
        "dsc_estado": "DSC_EST_BENEFICIARIO",
        "con_derecho": "CON_DER_BENEFICIARIO",
        "fec_perdida_derecho": "FEC_PDD_DER_BENEFICIARIO"
    }

    for i in range(1, 16):
        for campo, alias_base in alias_benef.items():
            select_cols.append(col(f"bp.{campo}_{i}").alias(f"{alias_base}_{i}"))

    select_cols.extend([
        col("r.no_rad_not_tec").cast(LongType()).alias("NRO_RAD_NOT_TEC"),
        col("r.interes_tec_prima").alias("INT_TEC_PRIMA"),
        col("r.tasa_crec_prima").alias("TAS_CREC_PRIMA"),
        col("r.gastos_prima").alias("GST_PRIMA"),
        col("r.prima").alias("PRIMA"),
        col("r.fecha_inicial_renta_diferida").cast(DateType()).alias("FEC_INI_RNT_DIFERIDA"),
        col("r.valor_mesada_benef_perio_difer").alias("VLR_MSD_BNF_PER_DIFERIDA"),
        when(col("r.valor_auxilio_funerario") == lit(0), lit(None)).otherwise(col("r.valor_auxilio_funerario")).alias("VLR_AUX_FUNERARIO"),
        col("r.observaciones").alias("OBSERVACIONES"),
        col("r.valor_reserva_rv89").alias("VLR_RSV_RV89"),
        col("r.valor_reserva_rv08").alias("VLR_RSV_RV08"),
        spark_round(col("r.par_0"), 2).cast(DecimalType(38, 2)).alias("PAR_0"),
        spark_round(col("r.parma"), 2).cast(DecimalType(38, 2)).alias("PARMA"),
        col("r.porcentaje_exc_parma").alias("POR_EXC_PARMA"),
        spark_round(col("r.por_amort_acum"), 0).cast(DecimalType(38, 0)).alias("POR_AMT_ACUM"),
        col("r.fec_aviso_siniestro").cast(DateType()).alias("FEC_AVI_SINIESTRO"),
        lit(None).cast(DecimalType(18, 6)).alias("FAR_VALUE"),
        col("r.inscripcion_mecanismo").alias("INS_MECANISMO"),
        col("r.fecha_inscripcion").cast(DateType()).alias("FEC_INS_MECANISMO"),
        col("r.parametro_deslizamiento").alias("PRM_DESLIZAMIENTO"),
        spark_round(col("r.tasa_fac_seg") / lit(100), 4).cast(DecimalType(38, 4)).alias("TAS_FAC_SEGURIDAD"),
        col("r.valor_cobertura").alias("VLR_COBERTURA"),
        lit(None).cast(DecimalType(18, 6)).alias("ITR_TEC_NIIF4"),
        col("r.nid_auditoria_medica").cast(LongType()).alias("NID_AUDITORIA_MEDICA"),
        col("r.nid_calculo").cast(LongType()).alias("NID_CALCULO"),
        col("r.nid_calculo_2").cast(LongType()).alias("NID_CALCULO_2"),
        col("r.nid_siniestro_reserva_pcj").cast(LongType()).alias("NID_SINIESTRO_RESERVA_PCJ"),
        col("r.nid_hist_datos_inv_mte_m").cast(LongType()).alias("NID_HIST_DATOS_INV_MTE_INI"),
        col("r.nid_hist_datos_inv_mte_h").cast(LongType()).alias("NID_HIST_DATOS_INV_MTE_ACT"),
        current_timestamp().alias("FEC_INI_VIG_REG"),
        lit(None).cast("timestamp").alias("FEC_FIN_VIG_REG"),
        lit(1).cast(IntegerType()).alias("VRS_REG"),
        lit(1).cast(IntegerType()).alias("ACT_REG"),
        lit(lote_cargue_id).cast(LongType()).alias("LOTE_CARGUE_ID")
    ])

    df_preparado = joined.select(*select_cols).dropDuplicates(["PER_CORTE", "RAMO", "NRO_POLIZA", "NRO_SINIESTRO"])

    total_preparado = df_preparado.count()
    if total_preparado == 0:
        raise ValueError("Silver preparada ARP termino con 0 registros para PER_CORTE=" + per_corte)
    logger.info(f"Total registros Silver preparada PER_CORTE={per_corte}: {total_preparado}")

    # =================================================
    # ESCRITURA OVERWRITE POR PER_CORTE
    # =================================================
    logger.info(f"Escribiendo Silver preparada en overwrite por corte: {slv_prep_target}")
    df_preparado.write.mode("overwrite").option("compression", "snappy").parquet(slv_prep_target)


    # =================================================
    # ACTUALIZAR PARAMETROS_EXTRACCION
    # =================================================
    fecha_preparacion = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    lotes_usados_preparacion = {
        tabla: (
            obtener_lotes_preparacion(parametros_extraccion, tabla, ds(tabla), lote_cargue_id)
            if bool(ds(tabla).get("segmentacion_horizontal_habilitada", False))
            else "SNAPSHOT_VIGENTE"
        )
        for tabla in TABLAS_REQUERIDAS
    }
    parametros_extraccion["criterio_preparacion_silver"] = {
        "tabla_pivote_segmentada": "arpprod/sar_datos_invalidez_muerte",
        "pivote": "LOTES_SEGMENTO_MAS_PERIODO_CORTE",
        "dependencias": "SNAPSHOT_FISICO_VIGENTE",
        "lote_cargue_id": "TRAZABILIDAD_NO_LLAVE_DE_ARTICULACION"
    }

    parametros_extraccion["estado_preparacion_silver"] = "SUCCESS"
    parametros_extraccion["ruta_silver_preparada"] = slv_prep_target
    parametros_extraccion["ruta_silver_preparada_base"] = slv_prep_base
    parametros_extraccion["ruta_tmp_preparacion_silver"] = tmp_target
    parametros_extraccion["modo_escritura_silver_preparada"] = "overwrite_por_per_corte"
    parametros_extraccion["silver_preparada_historica"] = False
    parametros_extraccion["total_registros_silver_preparada"] = total_preparado
    parametros_extraccion["fecha_preparacion_silver"] = fecha_preparacion
    parametros_extraccion["ultimo_per_corte_preparado"] = per_corte
    parametros_extraccion["lotes_usados_preparacion_silver"] = lotes_usados_preparacion

    parametros_extraccion.setdefault("silver_preparaciones", {})
    parametros_extraccion["silver_preparaciones"][per_corte] = {
        "estado": "SUCCESS",
        "nombre_caso": "reserva_matematica_arp",
        "per_corte": per_corte,
        "ruta_silver_preparada": slv_prep_target,
        "ruta_silver_preparada_base": slv_prep_base,
        "ruta_tmp_preparacion": tmp_target,
        "modo_escritura": "overwrite_por_per_corte",
        "total_registros": total_preparado,
        "lote_cargue_id": lote_cargue_id,
        "lotes_usados_preparacion": lotes_usados_preparacion,
        "estado_validacion": estado_validacion,
        "requiere_revision_analitica": requiere_revision_analitica,
        "estado_hallazgos_validacion": estado_hallazgos_validacion,
        "fecha_preparacion": fecha_preparacion,
        "observacion": "Silver preparada ARP v2.4 con ajuste de orden y condiciones en la clasificacion de beneficiarios, alineado con la consulta Oracle V2.4 validada funcionalmente."
    }

    parametros_extraccion["silver_preparacion"] = parametros_extraccion["silver_preparaciones"][per_corte]

    guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

    logger.info("parametros_extraccion.json actualizado con estado_preparacion_silver SUCCESS.")
    logger.info("=== FIN SILVER PREPARADA RESERVA MATEMATICA ARP V2.4 ===")

    job.commit()
