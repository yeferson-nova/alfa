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
    col, lit, lower, trim, concat_ws, concat, when, to_date, last_day,
    current_timestamp, date_format, add_months, months_between,
    first, dense_rank, desc, asc, round as spark_round, least, floor,
    coalesce, countDistinct, max as spark_max, sum as spark_sum
)
from pyspark.sql.types import (
    LongType, IntegerType, DecimalType, StringType, DateType
)

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

    cols_originales = df.columns
    vistos = set()
    columnas_unicas = []
    nuevos_nombres = []

    for idx, c in enumerate(cols_originales):
        key = c.lower()
        if key in vistos:
            nuevo = f"__dup_{idx}_{c}"
            logger.warning(
                f"Columna duplicada detectada en {nombre}: {c}. "
                f"Se renombra temporalmente como {nuevo} y se descarta."
            )
            nuevos_nombres.append(nuevo)
        else:
            vistos.add(key)
            columnas_unicas.append(c)
            nuevos_nombres.append(c)

    if len(columnas_unicas) != len(cols_originales):
        df = df.toDF(*nuevos_nombres).select(*columnas_unicas)

    df.write.mode("overwrite").option("compression", "snappy").parquet(ruta)
    return spark.read.parquet(ruta)


def safe_div(num_col, den_col):
    return when(
        den_col.isNotNull() & (den_col != lit(0)),
        num_col / den_col
    ).otherwise(lit(None))


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

logger.info("=== INICIO SILVER PREPARACION RESERVA MATEMATICA RVI V2.3 ===")
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
    "general/anp_movimientos_pago",
    "general/anp_distribucion_vig_pension",
    "general/anp_pensiones",
    "general/anp_pensionado_novedad",
    "general/cgr_conceptos",
    "prv/prv_rentas",
    "prv/prv_rentas_notas",
    "prv/crv_calculos_2",
    "prv/prv_rentas_aprobadas_obp",
    "prv/prv_endosos_point",
    "prv/prv_beneficiarios",
    "prv/spr_siniestros",
    "general/acu_personas_naturales",
    "general/cgr_estados_personas",
    "general/cgr_origenes",
    "general/cgr_equivalencia_tipos_id",
    "general/cgr_parentescos",
    "general/pgr_salarios_minimos",
    "general/anp_ipc",
    "general/rva_reserva_matematica_transic"
]

for tabla_req in TABLAS_REQUERIDAS:
    if tabla_req not in mapa_parametros:
        raise ValueError(f"La tabla requerida '{tabla_req}' no aparece en parametros_extraccion.")


def ds(tabla_relativa):
    return mapa_parametros[normalizar_tabla_relativa(tabla_relativa)]


def ruta_destino(tabla_relativa):
    return ensure_trailing_slash(ds(tabla_relativa)["ruta_dataset_destino"])


# =================================================
# LECTURA DE SILVER BASE
# =================================================
logger.info("Leyendo 20 datasets desde Silver base.")

anp_mov_pago = leer_silver_base(spark, ruta_destino("general/anp_movimientos_pago"), parametros_extraccion, "general/anp_movimientos_pago", ds("general/anp_movimientos_pago"), lote_cargue_id, "ANP_MOVIMIENTOS_PAGO")
anp_dist = leer_silver_base(spark, ruta_destino("general/anp_distribucion_vig_pension"), parametros_extraccion, "general/anp_distribucion_vig_pension", ds("general/anp_distribucion_vig_pension"), lote_cargue_id, "ANP_DISTRIBUCION_VIG_PENSION")
anp_pensiones = leer_silver_base(spark, ruta_destino("general/anp_pensiones"), parametros_extraccion, "general/anp_pensiones", ds("general/anp_pensiones"), lote_cargue_id, "ANP_PENSIONES")
anp_novedad = leer_silver_base(spark, ruta_destino("general/anp_pensionado_novedad"), parametros_extraccion, "general/anp_pensionado_novedad", ds("general/anp_pensionado_novedad"), lote_cargue_id, "ANP_PENSIONADO_NOVEDAD")
cgr_conceptos = leer_silver_base(spark, ruta_destino("general/cgr_conceptos"), parametros_extraccion, "general/cgr_conceptos", ds("general/cgr_conceptos"), lote_cargue_id, "CGR_CONCEPTOS")
prv_rentas = leer_silver_base(spark, ruta_destino("prv/prv_rentas"), parametros_extraccion, "prv/prv_rentas", ds("prv/prv_rentas"), lote_cargue_id, "PRV_RENTAS")
prv_rentas_notas = leer_silver_base(spark, ruta_destino("prv/prv_rentas_notas"), parametros_extraccion, "prv/prv_rentas_notas", ds("prv/prv_rentas_notas"), lote_cargue_id, "PRV_RENTAS_NOTAS")
crv_calculos_2 = leer_silver_base(spark, ruta_destino("prv/crv_calculos_2"), parametros_extraccion, "prv/crv_calculos_2", ds("prv/crv_calculos_2"), lote_cargue_id, "CRV_CALCULOS_2")
prv_rentas_aprobadas_obp = leer_silver_base(spark, ruta_destino("prv/prv_rentas_aprobadas_obp"), parametros_extraccion, "prv/prv_rentas_aprobadas_obp", ds("prv/prv_rentas_aprobadas_obp"), lote_cargue_id, "PRV_RENTAS_APROBADAS_OBP")
prv_endosos_point = leer_silver_base(spark, ruta_destino("prv/prv_endosos_point"), parametros_extraccion, "prv/prv_endosos_point", ds("prv/prv_endosos_point"), lote_cargue_id, "PRV_ENDOSOS_POINT")
prv_beneficiarios = leer_silver_base(spark, ruta_destino("prv/prv_beneficiarios"), parametros_extraccion, "prv/prv_beneficiarios", ds("prv/prv_beneficiarios"), lote_cargue_id, "PRV_BENEFICIARIOS")
spr_siniestros = leer_silver_base(spark, ruta_destino("prv/spr_siniestros"), parametros_extraccion, "prv/spr_siniestros", ds("prv/spr_siniestros"), lote_cargue_id, "SPR_SINIESTROS")
personas = leer_silver_base(spark, ruta_destino("general/acu_personas_naturales"), parametros_extraccion, "general/acu_personas_naturales", ds("general/acu_personas_naturales"), lote_cargue_id, "ACU_PERSONAS_NATURALES")
estados_personas = leer_silver_base(spark, ruta_destino("general/cgr_estados_personas"), parametros_extraccion, "general/cgr_estados_personas", ds("general/cgr_estados_personas"), lote_cargue_id, "CGR_ESTADOS_PERSONAS")
origenes = leer_silver_base(spark, ruta_destino("general/cgr_origenes"), parametros_extraccion, "general/cgr_origenes", ds("general/cgr_origenes"), lote_cargue_id, "CGR_ORIGENES")
equiv_tipos = leer_silver_base(spark, ruta_destino("general/cgr_equivalencia_tipos_id"), parametros_extraccion, "general/cgr_equivalencia_tipos_id", ds("general/cgr_equivalencia_tipos_id"), lote_cargue_id, "CGR_EQUIVALENCIA_TIPOS_ID")
parentescos = leer_silver_base(spark, ruta_destino("general/cgr_parentescos"), parametros_extraccion, "general/cgr_parentescos", ds("general/cgr_parentescos"), lote_cargue_id, "CGR_PARENTESCOS")
salarios = leer_silver_base(spark, ruta_destino("general/pgr_salarios_minimos"), parametros_extraccion, "general/pgr_salarios_minimos", ds("general/pgr_salarios_minimos"), lote_cargue_id, "PGR_SALARIOS_MINIMOS")
ipc = leer_silver_base(spark, ruta_destino("general/anp_ipc"), parametros_extraccion, "general/anp_ipc", ds("general/anp_ipc"), lote_cargue_id, "ANP_IPC")
rva_transic = leer_silver_base(spark, ruta_destino("general/rva_reserva_matematica_transic"), parametros_extraccion, "general/rva_reserva_matematica_transic", ds("general/rva_reserva_matematica_transic"), lote_cargue_id, "RVA_RESERVA_MATEMATICA_TRANSIC")


# =================================================
# FECHAS PARAMETRIZADAS
# =================================================
fec_ini_per_corte_expr = to_date(lit(f"{per_corte}01"), "yyyyMMdd")
fec_fin_per_corte_expr = last_day(fec_ini_per_corte_expr)


# =================================================
# CUERPO PRINCIPAL RVI

# Equivalente rm_rvi_0 a rm_rvi_10
# =================================================
logger.info("Construyendo cuerpo principal Reserva Matemática RVI.")

rm_rvi_1 = (
    anp_mov_pago.alias("m")
    .join(cgr_conceptos.alias("cp"), col("m.COD_CONCEPTO") == col("cp.COD_CONCEPTO"), "inner")
    .join(anp_dist.alias("d"), col("m.NID_DISTRIBUCION_VIGENCIA") == col("d.NID_DISTRIBUCION_VIGENCIA"), "inner")
    .join(anp_pensiones.alias("p"), col("d.NID_PENSION") == col("p.NID_PENSION"), "inner")
    .join(
        prv_rentas.alias("r"),
        (col("p.NID_PROCEDENCIA") == col("r.NID_RENTAS")) &
        (fec_fin_per_corte_expr >= col("r.FEC_EXPEDICION")),
        "inner"
    )
    .join(origenes.alias("op"), col("r.COD_ORIGEN") == col("op.COD_ORIGEN"), "inner")
    .join(
        anp_novedad.alias("nv"),
        (col("d.NID_DISTRIBUCION_VIGENCIA") == col("nv.NID_DISTRIBUCION_VIGENCIA")) &
        (fec_fin_per_corte_expr > col("nv.FEC_INGRESO")) &
        (col("nv.COD_CONCEPTO").cast("string") == lit("1")),
        "inner"
    )
    .join(cgr_conceptos.alias("cnv"), col("nv.COD_CONCEPTO") == col("cnv.COD_CONCEPTO"), "inner")
    .join(
        personas.alias("apn"),
        (col("r.COD_TIPO_IDENTIFICACION") == col("apn.COD_TIPO_IDENTIFICACION")) &
        (col("r.NID_NUMERO_IDENTIFICACION") == col("apn.NID_NUMERO_IDENTIFICACION")),
        "inner"
    )
    .join(
        estados_personas.alias("ep"),
        col("apn.COD_ESTADO").cast("string")
        == col("ep.COD_ESTADO_PERSONA").cast("string"),
        "left"
    )
    .join(prv_rentas_notas.alias("nt"), col("r.NID_RENTAS") == col("nt.NID_RENTAS"), "inner")
    .join(equiv_tipos.alias("eq"), col("r.COD_TIPO_IDENTIFICACION") == col("eq.COD_TIPO_DOCUMENTO"), "inner")
    .filter(
        (col("m.FEC_INICIAL").between(fec_ini_per_corte_expr, fec_fin_per_corte_expr)) &
        (col("m.COD_CONCEPTO").cast("string") == lit("1"))
    )
    .select(
        lit(per_corte_int).cast(IntegerType()).alias("per_corte"),
        fec_ini_per_corte_expr.alias("fec_ini_per_corte"),
        fec_fin_per_corte_expr.alias("fec_fin_per_corte"),
        date_format(col("m.FEC_INICIAL"), "yyyyMM").cast(IntegerType()).alias("per_pago"),
        to_date(concat(date_format(col("m.FEC_INICIAL"), "yyyyMM"), lit("01")), "yyyyMMdd").alias("fec_ini_per_pago"),
        last_day(col("m.FEC_INICIAL")).alias("fec_fin_per_pago"),
        date_format(col("m.FEC_INICIAL"), "yyyy").cast(IntegerType()).alias("ani_per_pago"),
        to_date(
            concat(
                date_format(
                    add_months(col("m.FEC_INICIAL"), -1),
                    "yyyyMM"
                ),
                lit("01")
            ),
            "yyyyMMdd"
        ).alias("fec_ini_per_pago_ant"),
        last_day(add_months(col("m.FEC_INICIAL"), -1)).alias("fec_fin_per_pago_ant"),
        last_day(add_months(col("m.FEC_INICIAL"), 1)).alias("fec_fin_per_pago_pst"),
        col("m.COD_CONCEPTO").alias("cod_cpt_pago"),
        col("cp.NOMBRE_CONCEPTO").alias("dsc_cpt_pago"),
        lit(None).cast(DecimalType(18, 6)).alias("int_tec_niif4"),
        lit(None).cast(DecimalType(18, 6)).alias("fair_value"),
        col("r.NID_RENTAS").alias("nro_poliza"),
        col("r.NID_SINIESTRO").alias("nro_siniestro"),
col("r.NID_SINIESTRO").alias("nro_siniestro_original"),
        col("r.FEC_EXPEDICION").alias("fec_exp_renta"),
        date_format(col("r.FEC_EXPEDICION"), "yyyyMM").cast(IntegerType()).alias("per_exp_renta"),
        to_date(concat(date_format(col("r.FEC_EXPEDICION"), "yyyyMM"), lit("01")), "yyyyMMdd").alias("fec_ini_per_exp_renta"),
        last_day(col("r.FEC_EXPEDICION")).alias("fec_fin_per_exp_renta"),
        date_format(col("r.FEC_EXPEDICION"), "yyyy").cast(IntegerType()).alias("ani_per_exp_renta"),
        col("r.VLR_SALARIO_BASE").alias("ing_bas_liquidacion"),
        col("r.VLR_PRIMA_UNICA").alias("vlr_prima_unica"),
        col("r.VLR_CUENTA_INDIVIDUAL").alias("vlr_cuenta_individual"),
        col("r.VLR_BONO_PENSIONAL").alias("vlr_bono_pensional"),
        col("r.VLR_PENSION_INICIAL").alias("vlr_pension_inicial"),
        col("r.VLR_PENSION_ACTUAL").alias("mesada"),
        col("r.NUM_MESADAS").alias("nro_mesadas"),
        col("r.COD_ORIGEN").alias("cod_org_pension"),
col("r.COD_ORIGEN").alias("cod_org_pension_original"),
        col("op.NOMBRE").alias("dsc_org_pension"),
        col("r.COD_AFP").alias("cod_afp"),
        col("nv.FEC_INGRESO").alias("fec_ing_novedad"),
        col("nv.COD_CONCEPTO").alias("cod_cpt_novedad_spn"),
        col("cnv.NOMBRE_CONCEPTO").alias("dsc_cpt_novedad_spn"),
        coalesce(col("nv.VLR_FIJO"), lit(0)).alias("vlr_aux_funerario_spn"),
        col("m.NID_DISTRIBUCION_VIGENCIA").alias("nid_distribucion_vigencia_spn"),
        lit(1).cast(IntegerType()).alias("unidad"),
        lit(None).cast(StringType()).alias("equivalencia"),
        lit(1).cast(IntegerType()).alias("par_afiliado"),
        lit(None).cast(StringType()).alias("est_afiliado"),
        lit(None).cast(StringType()).alias("retroactivo"),
        lit(None).cast(DateType()).alias("fec_ini_rnt_diferida"),
        lit(None).cast(DecimalType(18, 6)).alias("vlr_msd_bnf_per_diferido"),
        lit(None).cast(DecimalType(18, 6)).alias("por_exc_parma"),
        lit(None).cast(DateType()).alias("fec_avi_siniestro"),
        col("r.COD_TIPO_IDENTIFICACION").alias("cod_org_tip_idt_afiliado"),
        col("eq.COD_TIPO_SUPER").alias("cod_tip_idt_afiliado"),
        col("r.NID_NUMERO_IDENTIFICACION").alias("nid_num_idt_afiliado"),
        col("apn.PRIMER_APELLIDO").alias("pmr_ape_afiliado"),
        trim(col("apn.PRIMER_APELLIDO")).alias("pmr_ape_afiliado_t"),
        col("apn.SEGUNDO_APELLIDO").alias("sgn_ape_afiliado"),
        trim(col("apn.SEGUNDO_APELLIDO")).alias("sgn_ape_afiliado_t"),
        col("apn.PRIMER_NOMBRE").alias("pmr_nom_afiliado"),
        trim(col("apn.PRIMER_NOMBRE")).alias("pmr_nom_afiliado_t"),
        col("apn.SEGUNDO_NOMBRE").alias("sgn_nom_afiliado"),
        trim(col("apn.SEGUNDO_NOMBRE")).alias("sgn_nom_afiliado_t"),
        col("apn.FEC_NACIMIENTO").alias("fec_nac_afiliado"),
        col("apn.GENERO").alias("gen_afiliado"),
        lower(col("apn.GENERO")).alias("gen_afiliado_l"),
        col("apn.FEC_MUERTE").alias("fec_mrt_afiliado"),
        col("apn.FEC_MUERTE").alias("fec_mrt_afiliado_original"),
        col("apn.COD_ESTADO").alias("cod_etd_afiliado"),
        col("ep.NOMBRE").alias("dsc_etd_afiliado"),
        col("nt.COD_RAMO_SFC").alias("ramo"),
        col("nt.NUM_RADICADO_NOTA").alias("nro_rad_not_tec"),
        col("r.FEC_CAUSACION").alias("fec_csc_derecho"),
        col("r.PCJ_PCL_INICIAL").alias("grd_inv_inicial"),
        col("r.FEC_ULTIMA_MOD").alias("fec_ult_modificacion"),
        col("r.PCJ_PCL_ACTUAL").alias("grd_inv_actual"),
        col("r.PC_AD_EXP").alias("gastos"),
        when(
            col("r.FEC_EXPEDICION") < to_date(lit("20200110"), "yyyyMMdd"),
            col("r.PCJ_INTERES_TECNICO")
        ).otherwise(col("r.PCJ_INTERES_RESERVA") * lit(100)).alias("int_tec_prima")
    )
    .dropDuplicates()
)


# Auxilio funerario concepto 28
rm_rvi_pxf = (
    rm_rvi_1.alias("r")
    .join(anp_pensiones.alias("p"), col("r.nro_poliza") == col("p.NID_PROCEDENCIA"), "inner")
    .join(anp_dist.alias("d"), col("p.NID_PENSION") == col("d.NID_PENSION"), "inner")
    .join(
        anp_novedad.alias("nv"),
        (col("d.NID_DISTRIBUCION_VIGENCIA") == col("nv.NID_DISTRIBUCION_VIGENCIA")) &
        (col("r.fec_fin_per_corte") > col("nv.FEC_INGRESO")) &
        (col("nv.COD_CONCEPTO").cast("string") == lit("28")),
        "inner"
    )
    .join(cgr_conceptos.alias("c"), col("nv.COD_CONCEPTO") == col("c.COD_CONCEPTO"), "inner")
    .groupBy(
        col("r.nro_poliza").alias("nro_poliza"),
        col("nv.COD_CONCEPTO").alias("cod_cpt_novedad_pxf"),
        col("c.NOMBRE_CONCEPTO").alias("dsc_cpt_novedad_pxf")
    )
    .agg(spark_max(coalesce(col("nv.VLR_FIJO"), lit(0))).alias("vlr_aux_funerario_pxf"))
)

rm_rvi_5 = (
    rm_rvi_1.alias("r")
    .join(rm_rvi_pxf.alias("pxf"), col("r.nro_poliza") == col("pxf.nro_poliza"), "left")
    .join(spr_siniestros.alias("s"), col("r.nro_siniestro") == col("s.NID_SINIESTRO"), "left")
    .select(
        col("r.*"),
        col("s.COD_ESTADO_SINIESTRO").alias("cod_etd_siniestro"),
        col("pxf.cod_cpt_novedad_pxf"),
        col("pxf.dsc_cpt_novedad_pxf"),
        col("pxf.vlr_aux_funerario_pxf"),
        trim(concat_ws(" ", col("r.pmr_ape_afiliado_t"), col("r.sgn_ape_afiliado_t"), col("r.pmr_nom_afiliado_t"), col("r.sgn_nom_afiliado_t"))).alias("nom_cpt_afiliado"),
        when(lower(col("r.gen_afiliado_l")) == lit("m"), lit(1))
            .when(lower(col("r.gen_afiliado_l")) == lit("f"), lit(2))
            .otherwise(lit(None)).alias("sex_afiliado")
    )
)



# V2.3: primer cruce de CRV_CALCULOS_2 para valores actuariales principales.
# Replica FIRST_VALUE + WHERE del SQL y conserva la granularidad de las filas base.
w_calculo = (
    Window.partitionBy(col("r.nro_poliza"))
    .orderBy(desc(col("cal.NID_CALCULO")), desc(col("cal.FEC_CORTE_CALCULO")))
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)

rm_rvi_5_1 = (
    rm_rvi_5.alias("r")
    .join(
        crv_calculos_2.alias("cal"),
        (col("r.nro_poliza") == col("cal.NID_RENTAS")) &
        col("cal.FEC_CORTE_CALCULO").between(
            col("r.fec_ini_per_pago_ant"),
            col("r.fec_fin_per_pago")
        ),
        "left"
    )
    .select(
        col("r.*"),
        col("cal.NID_CALCULO").alias("nro_calculo"),
        col("cal.PCJ_TASA_ADMINISTRACION").alias("gst_calculo"),
        col("cal.PCJ_FACTOR_SEGURIDAD").alias("tas_fac_seg_calculo"),
        (col("cal.VLR_EXCESO_SMMLV_IPC") * lit(100)).alias("ptr_dzt_calculo"),
        col("cal.FEC_CORTE_CALCULO").alias("fec_calculo"),
        col("cal.COD_ORIGEN").alias("cod_origen_calculo"),
        col("cal.VLR_PENSION_REAL").alias("vlr_pension_real_calculo"),
        col("cal.COD_ESTADO_TRABAJADOR").alias("cod_estado_trabajador_calculo"),
        first(col("cal.NID_CALCULO"), ignorenulls=True).over(w_calculo).alias("nro_calculo_h")
    )
    .filter(
        col("nro_calculo").isNotNull() &
        (col("nro_calculo") == col("nro_calculo_h"))
    )
    .drop("nro_calculo_h")
)


# V2.3: todo valor proveniente del calculo aplica la misma regla historica.
condicion_calculo_historico = (
    (last_day(to_date(current_timestamp())) > col("fec_fin_per_pago")) |
    (col("fec_mrt_afiliado") >= col("fec_fin_per_pago"))
)

rm_rvi_5_2 = (
    rm_rvi_5_1
    .withColumn(
        "mesada",
        when(condicion_calculo_historico & col("vlr_pension_real_calculo").isNotNull(),
             col("vlr_pension_real_calculo")).otherwise(col("mesada"))
    )
    .withColumn(
        "cod_org_pension",
        when(condicion_calculo_historico & col("cod_origen_calculo").isNotNull(),
             col("cod_origen_calculo")).otherwise(col("cod_org_pension"))
    )
    .withColumn(
        "cod_etd_afiliado",
        when(condicion_calculo_historico & col("cod_estado_trabajador_calculo").isNotNull(),
             col("cod_estado_trabajador_calculo")).otherwise(col("cod_etd_afiliado"))
    )
)

# Descripciones alineadas con los codigos ya ajustados.
rm_rvi_5_3 = (
    rm_rvi_5_2.drop("dsc_org_pension", "dsc_etd_afiliado").alias("r")
    .join(origenes.alias("org"),
          col("r.cod_org_pension").cast("string") == col("org.COD_ORIGEN").cast("string"), "left")
    .join(estados_personas.alias("est"),
          col("r.cod_etd_afiliado").cast("string") == col("est.COD_ESTADO_PERSONA").cast("string"), "left")
    .select(col("r.*"), col("org.NOMBRE").alias("dsc_org_pension"),
            col("est.NOMBRE").alias("dsc_etd_afiliado"))
)

# V2.3: segundo cruce para IPC historico, incluyendo periodo posterior.
# NID_CALCULO_2 identifica el calculo exacto usado para la trazabilidad del IPC.
w_calculo_ipc = (
    Window.partitionBy(col("r.nro_poliza"))
    .orderBy(desc(col("cal2.NID_CALCULO")), desc(col("cal2.FEC_CORTE_CALCULO")))
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)
rm_rvi_5_4 = (
    rm_rvi_5_3.alias("r")
    .join(
        crv_calculos_2.alias("cal2"),
        (col("r.nro_poliza") == col("cal2.NID_RENTAS")) &
        col("cal2.FEC_CORTE_CALCULO").between(
            col("r.fec_ini_per_pago_ant"),
            col("r.fec_fin_per_pago_pst")
        ),
        "left"
    )
    .select(
        col("r.*"),
        col("cal2.NID_CALCULO").alias("nid_calculo_2"),
        col("cal2.PCJ_TASA_INFLACION").alias("pcj_tasa_inflacion_calculo"),
        first(col("cal2.NID_CALCULO"), ignorenulls=True).over(w_calculo_ipc).alias("nid_calculo_2_h")
    )
    .filter(
        col("nid_calculo_2").isNotNull() &
        (col("nid_calculo_2") == col("nid_calculo_2_h"))
    )
    .drop("nid_calculo_2_h")
)


# La fecha original ya se utilizo en las reglas historicas. Para salida, una
# muerte del periodo actual o posterior no se presenta como efectiva.
rm_rvi_5_4 = rm_rvi_5_4.withColumn(
    "fec_mrt_afiliado",
    when(col("fec_mrt_afiliado") >= col("fec_fin_per_pago"),
         lit(None).cast(DateType())).otherwise(col("fec_mrt_afiliado"))
)

rm_rvi_6 = (
    rm_rvi_5_4.alias("r")
    .join(salarios.alias("sm"), (col("r.ani_per_pago") == col("sm.ANO")) & (col("sm.MES") == lit(1)), "inner")
    .join(ipc.alias("ipc"), col("r.ani_per_exp_renta") == col("ipc.ANIO"), "left")
    .withColumn(
        "cod_nov_org_pension",
        when(
            col("r.fec_mrt_afiliado").isNotNull(),
            when(
                col("r.cod_org_pension").cast("int") == lit(1),
                when((to_date(col("r.fec_mrt_afiliado")) >= to_date(col("r.fec_exp_renta"))) & col("r.nro_siniestro").isNull(), lit(5))
                .when((to_date(col("r.fec_mrt_afiliado")) >= to_date(col("r.fec_exp_renta"))) & col("r.nro_siniestro").isNotNull(), lit(4))
                .when(to_date(col("r.fec_mrt_afiliado")) < to_date(col("r.fec_exp_renta")), lit(2))
            )
            .when(col("r.cod_org_pension").cast("int") == lit(2), lit(1))
            .when(col("r.cod_org_pension").cast("int") == lit(3), lit(3))
        ).otherwise(
            when(
                col("r.nro_siniestro").isNotNull(),
                when(col("r.cod_org_pension").cast("int") == lit(2), lit(1))
                .when(col("r.cod_etd_siniestro").cast("int") == lit(4), lit(3))
                .when(col("r.cod_etd_siniestro").cast("int") != lit(4), lit(1))
                .otherwise(lit(1))
            )
            .when(
                col("r.nro_siniestro").isNull() &
                (col("r.cod_afp").cast("int") != lit(1)) &
                (col("r.cod_org_pension").cast("int") == lit(2)),
                lit(1)
            )
            .when(col("r.nro_siniestro").isNull() & (col("r.cod_org_pension").cast("int") == lit(2)), lit(1))
            .when(col("r.nro_siniestro").isNull() & (col("r.cod_org_pension").cast("int") == lit(3)), lit(3))
        )
    )
    .select(
        col("r.*"),
        when(col("r.ani_per_exp_renta") == lit(anio_corte_int), col("r.pcj_tasa_inflacion_calculo"))
            .otherwise(col("ipc.PCJ_IPC")).alias("tsa_crc_prima"),
        col("cod_nov_org_pension"),
        col("sm.VLR_SALARIO_MINIMO").alias("vlr_slr_minimo")
    )
)


rm_rvi_7 = (
    rm_rvi_6.alias("r")
    .join(
        origenes.alias("o"),
        col("r.cod_nov_org_pension").cast("string") == col("o.COD_ORIGEN").cast("string"),
        "inner"
    )
    .select(
        col("r.*"),
        col("o.NOMBRE").alias("dsc_nov_org_pension"),
        when(
            col("r.cod_nov_org_pension").cast("int").isin(1, 3),
            when(
                col("r.mesada") <= (col("r.vlr_slr_minimo") * lit(5)),
                col("r.vlr_slr_minimo") * lit(5)
            ).when(
                (col("r.mesada") > (col("r.vlr_slr_minimo") * lit(5))) &
                (col("r.mesada") <= (col("r.vlr_slr_minimo") * lit(10))),
                col("r.mesada")
            ).otherwise(col("r.vlr_slr_minimo") * lit(10))
        ).otherwise(
            when(
                col("r.cod_cpt_novedad_pxf").cast("string") == lit("28"),
                col("r.vlr_aux_funerario_pxf")
            ).otherwise(lit(0))
        ).alias("vlr_aux_funerario")
    )
)



joined_rvi_8 = (
    rm_rvi_7.alias("r")
    .join(prv_rentas_aprobadas_obp.alias("ao"), col("r.nro_poliza") == col("ao.NID_RENTAS"), "left")
    .join(
        rva_transic.alias("tr"),
        (col("r.nro_poliza") == col("tr.NID_SINIESTRO_RENTA")) &
        (col("tr.RAMO") == lit("RVI")),
        "left"
    )
    .join(
        prv_endosos_point.alias("ep"),
        (col("r.nro_poliza") == col("ep.NID_RENTAS")) &
        (col("r.fec_fin_per_corte") >= col("ep.FEC_PROCESO_POINT")),
        "left"
    )
)

base_group_cols = [col(f"r.{c}").alias(c) for c in rm_rvi_7.columns]

rm_rvi_8 = (
    joined_rvi_8
    .groupBy(*base_group_cols,
             col("ao.PARAMETRO_DESLIZAMIENTO").alias("prt_dzt_aprobada"),
             col("ao.FECHA_INSCRIPCION").alias("fec_isc_aprobada"),
             coalesce(col("ao.INSCRIPCION_MECANISMO"), lit("0")).alias("inc_mecanismo"),
             col("ao.FECHA_INSCRIPCION").alias("fec_inc_mecanismo"),
             col("ao.COBERTURA").alias("vlr_cobertura"),
             col("tr.VLR_RESERVA_RV89").alias("vlr_rsv_rv89"),
             col("tr.VLR_RESERVA_RV08").alias("vlr_rsv_rv08"))
    .agg(spark_sum(col("ep.VLR_PRIMA")).alias("prima"))
)


rm_rvi_10 = (
    rm_rvi_8
    .withColumn("gastos_final", coalesce(col("gst_calculo"), col("gastos")))
    .withColumn(
        "par_0_calculado",
        spark_round(safe_div(col("vlr_rsv_rv89"), col("vlr_rsv_rv08")) * lit(100), 7)
    )
    .withColumn(
        "par_0",
        when(
            (col("par_0_calculado") >= lit(0)) &
            (col("par_0_calculado") < lit(100)),
            col("par_0_calculado")
        ).otherwise(lit(None).cast(DecimalType(18, 7)))
    )
    .drop("par_0_calculado")
    .withColumn(
        "parma",
        when(
            col("par_0").isNotNull(),
            spark_round(
                (
                    lit(100)
                    - (
                        safe_div(
                            col("vlr_rsv_rv89"),
                            col("vlr_rsv_rv08")
                        ) * lit(100)
                    )
                ) / lit(240),
                7
            )
        ).otherwise(
            lit(None).cast(DecimalType(18, 7))
        )
    )
    .withColumn(
        "ptr_dzt",
        when(col("ptr_dzt_calculo").isNotNull(), col("ptr_dzt_calculo"))
        .otherwise(col("prt_dzt_aprobada"))
    )
    .withColumn(
        "por_mrt_acumulado",
        spark_round(
            when(
                (safe_div(col("vlr_rsv_rv89"), col("vlr_rsv_rv08")) * lit(100)) +
                (((lit(100) - (safe_div(col("vlr_rsv_rv89"), col("vlr_rsv_rv08")) * lit(100))) / lit(240)) *
                 (floor(months_between(col("fec_fin_per_corte"), to_date(lit("201804"), "yyyyMM"))) + lit(91))) > lit(100),
                lit(100)
            ).otherwise(
                (safe_div(col("vlr_rsv_rv89"), col("vlr_rsv_rv08")) * lit(100)) +
                (((lit(100) - (safe_div(col("vlr_rsv_rv89"), col("vlr_rsv_rv08")) * lit(100))) / lit(240)) *
                 (floor(months_between(col("fec_fin_per_corte"), to_date(lit("201804"), "yyyyMM"))) + lit(91)))
            ),
            7
        )
    )
    .withColumn(
        "observaciones",
        when(col("fec_mrt_afiliado").isNotNull() & col("ptr_dzt").isNotNull(),
             concat(lit("fma "), col("fec_mrt_afiliado").cast("string"), lit(" deslizamient 0"), col("ptr_dzt").cast("string")))
        .when(col("fec_mrt_afiliado").isNotNull(),
              concat(lit("fma "), col("fec_mrt_afiliado").cast("string")))
        .when(col("ptr_dzt").isNotNull(),
              concat(lit("deslizamient 0"), col("ptr_dzt").cast("string")))
        .otherwise(lit(None))
    )
)


rm_rvi_10 = materializar(rm_rvi_10, f"{tmp_target}base_principal/", "rm_rvi_10_base_principal")
logger.info(f"Registros base principal RVI: {rm_rvi_10.count()}")


# =================================================
# BENEFICIARIOS NORMALIZADOS Y PIVOTEADOS
# =================================================
logger.info("Construyendo beneficiarios RVI normalizados y pivoteados.")

ben_1 = (
    rm_rvi_10.alias("r")
    .join(prv_beneficiarios.alias("b"), col("r.nro_poliza") == col("b.NID_RENTAS"), "inner")
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
        col("r.nro_poliza").alias("nid_rentas"),
        col("b.COD_PARENTESCO").alias("cod_parentesco"),
        col("pa.NOMBRE").alias("dsc_parentesco"),
        col("b.COD_TIPO_IDENTIFICACION").alias("cod_tipo_identificacion"),
        col("e.COD_TIPO_SUPER").alias("tipo_identif"),
        col("b.NID_NUMERO_IDENTIFICACION").alias("no_identif"),
        col("apn.GENERO").alias("genero"),
        lower(col("apn.GENERO")).alias("genero_l"),
        col("r.fec_mrt_afiliado").alias("fecha_muerte_afiliado"),
        col("apn.FEC_NACIMIENTO").alias("fecha_nacimiento"),
        col("apn.PRIMER_APELLIDO").alias("primer_apellido"),
        trim(col("apn.PRIMER_APELLIDO")).alias("primer_apellido_t"),
        col("apn.SEGUNDO_APELLIDO").alias("segundo_apellido"),
        trim(col("apn.SEGUNDO_APELLIDO")).alias("segundo_apellido_t"),
        col("apn.PRIMER_NOMBRE").alias("primer_nombre"),
        trim(col("apn.PRIMER_NOMBRE")).alias("primer_nombre_t"),
        col("apn.SEGUNDO_NOMBRE").alias("segundo_nombre"),
        trim(col("apn.SEGUNDO_NOMBRE")).alias("segundo_nombre_t"),
        (spark_round(months_between(last_day(col("r.fec_mrt_afiliado")), last_day(col("apn.FEC_NACIMIENTO"))) / lit(12), 7) - when((date_format(col("r.fec_mrt_afiliado"), "MM") == date_format(col("apn.FEC_NACIMIENTO"), "MM")) & (date_format(col("r.fec_mrt_afiliado"), "dd") < date_format(col("apn.FEC_NACIMIENTO"), "dd")), lit(1)).otherwise(lit(0))).alias("edad_beneficiario_cony_com"),
        spark_round(months_between(col("r.fec_fin_per_corte"), last_day(col("apn.FEC_NACIMIENTO"))) / lit(12), 7).alias("edad_beneficiario_hijo"),
        spark_round(months_between(col("r.fec_fin_per_corte"), last_day(col("r.fec_mrt_afiliado"))) / lit(12), 7).alias("tmp_bnf_beneficiario"),
        col("b.CANT_HIJOS").alias("cant_hijos"),
        col("b.COD_ESTADO_PERSONA").alias("cod_estado"),
        col("ep.NOMBRE").alias("dsc_estado"),
        col("b.BLN_ES_BENEFICIARIO").alias("es_beneficiario"),
        lower(col("b.BLN_ES_BENEFICIARIO")).alias("es_beneficiario_l"),
        col("b.BLN_CON_DERECHO").alias("con_derecho"),
        lower(col("b.BLN_CON_DERECHO")).alias("con_derecho_l"),
        col("b.FEC_PERDIDA_DERECHO").cast(DateType()).alias("fec_perdida_derecho")
    )
)

ben = (
    ben_1
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
    .withColumn("nombre", trim(concat_ws(" ", col("primer_apellido_t"), col("segundo_apellido_t"), col("primer_nombre_t"), col("segundo_nombre_t"))))
    .withColumn("estudios", when((col("cod_parentesco") == lit("4")) & (col("cod_estado") == lit("1")) & (col("edad_beneficiario_hijo") >= lit(18)), lit(1)))
)

ben = (
    ben
    .withColumn(
        "cod_parent_beneficiario_h_2",
        when((col("cod_parent_beneficiario_h_1") == lit(5)) & (col("es_beneficiario_l") == lit("s")) & (col("con_derecho_l") == lit("s")), lit(5))
        .when((col("cod_parent_beneficiario_h_1") == lit(6)) & (col("es_beneficiario_l") == lit("s")) & (col("con_derecho_l") == lit("s")), lit(6))
        .when((col("cod_parent_beneficiario_h_1") == lit(4)) & (col("es_beneficiario_l") == lit("s")) & (col("con_derecho_l") == lit("s")), lit(4))
        .when((col("cod_parent_beneficiario_h_1") == lit(3)) & (col("es_beneficiario_l") == lit("s")) & (col("con_derecho_l") == lit("s")), lit(3))
        .when((col("cod_parent_beneficiario_h_1") == lit(8)) & (col("es_beneficiario_l") == lit("s")) & (col("con_derecho_l") == lit("s")), lit(8))
        .otherwise(lit(0))
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
            & (to_date(col("fecha_muerte_afiliado")) < add_months(to_date(col("fecha_nacimiento")), 360))
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
            & (to_date(col("fecha_muerte_afiliado")) < add_months(to_date(col("fecha_nacimiento")), 360))
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

w_ben = Window.partitionBy("nid_rentas").orderBy(
    col("cod_parentesco"),
    desc(col("fecha_nacimiento")),
    col("tipo_identif"),
    col("no_identif"),
    desc(col("con_derecho"))
)

rm_rvi_ben_8 = (
    ben
    .filter(col("cod_parent_beneficiario_h_3") != lit(0))
    .withColumn("num_ben", dense_rank().over(w_ben))
    .select(
        "num_ben", "nid_rentas", "cod_parentesco", "dsc_parentesco",
        col("cod_parent_beneficiario_h_3").alias("cod_parent_beneficiario"),
        col("dsc_parent_beneficiario_h_3").alias("dsc_parent_beneficiario"),
        "cod_tipo_identificacion", "tipo_identif", "no_identif", "genero", "sexo_beneficiario",
        "fecha_muerte_afiliado", "fecha_nacimiento", "primer_apellido", "segundo_apellido",
        "primer_nombre", "segundo_nombre", "nombre", "edad_beneficiario_cny_cmp",
        "edad_beneficiario_hijo", "tmp_bnf_beneficiario", "cant_hijos", "estudios",
        "cod_estado", "dsc_estado", "es_beneficiario", "con_derecho",
        "fec_perdida_derecho"
    )
)

rm_rvi_ben_8 = materializar(rm_rvi_ben_8, f"{tmp_target}beneficiarios_normalizados/", "rm_rvi_ben_8_normalizados")
logger.info(f"Registros beneficiarios normalizados RVI: {rm_rvi_ben_8.count()}")

# AJUSTE JULI 202606: conteos desde ben_1, igual que rm_rvi_12/rm_rvi_13.
no_hijos = (
    ben_1
    .filter(
        (col("cod_parentesco").cast("string") == lit("4")) &
        (lower(trim(col("es_beneficiario").cast("string"))) == lit("s")) &
        (lower(trim(col("con_derecho").cast("string"))) == lit("s"))
    )
    .groupBy("nid_rentas")
    .agg(
        countDistinct(
            concat_ws(
                "|",
                col("tipo_identif").cast("string"),
                col("no_identif").cast("string")
            )
        ).alias("nro_hjs_ttl_afiliado")
    )
)

no_hijos_invalidos = (
    ben_1
    .filter(
        (col("cod_parentesco").cast("string") == lit("4")) &
        (col("cod_estado").cast("string") == lit("2")) &
        (lower(trim(col("es_beneficiario").cast("string"))) == lit("s")) &
        (lower(trim(col("con_derecho").cast("string"))) == lit("s"))
    )
    .groupBy("nid_rentas")
    .agg(
        countDistinct(
            concat_ws(
                "|",
                col("tipo_identif").cast("string"),
                col("no_identif").cast("string")
            )
        ).alias("nro_hjs_inv_afiliado")
    )
)

campos_benef = [
    "cod_parentesco", "dsc_parentesco", "cod_parent_beneficiario", "dsc_parent_beneficiario",
    "cod_tipo_identificacion", "tipo_identif", "no_identif", "genero", "sexo_beneficiario",
    "fecha_nacimiento", "primer_apellido", "segundo_apellido",
    "primer_nombre", "segundo_nombre", "nombre", "edad_beneficiario_cny_cmp",
    "edad_beneficiario_hijo", "tmp_bnf_beneficiario", "cant_hijos", "estudios",
    "cod_estado", "dsc_estado", "es_beneficiario", "con_derecho", "fec_perdida_derecho"
]

aggs = []
for i in range(1, 16):
    for campo in campos_benef:
        aggs.append(spark_max(when(col("num_ben") == lit(i), col(campo))).alias(f"{campo}_{i}"))

benef_pivot = rm_rvi_ben_8.groupBy("nid_rentas").agg(*aggs)
benef_pivot = materializar(benef_pivot, f"{tmp_target}beneficiarios_pivoteados/", "beneficiarios_pivoteados")
logger.info(f"Registros beneficiarios pivoteados RVI: {benef_pivot.count()}")


# =================================================
# SALIDA FINAL SILVER PREPARADA
# =================================================
logger.info("Construyendo salida final Silver preparada RVI.")

r = rm_rvi_10.alias("r")
bp = benef_pivot.alias("bp")
hj = no_hijos.alias("hj")
hi = no_hijos_invalidos.alias("hi")

joined = (
    r
    .join(bp, col("r.nro_poliza") == col("bp.nid_rentas"), "left")
    .join(hj, col("r.nro_poliza") == col("hj.nid_rentas"), "left")
    .join(hi, col("r.nro_poliza") == col("hi.nid_rentas"), "left")
)

select_cols = [
    col("r.per_corte").cast(IntegerType()).alias("PER_CORTE"),
    col("r.fec_ini_per_corte").cast(DateType()).alias("FEC_INI_PER_CORTE"),
    col("r.fec_fin_per_corte").cast(DateType()).alias("FEC_FIN_PER_CORTE"),
    col("r.per_pago").cast(IntegerType()).alias("PER_PAGO"),
    col("r.fec_ini_per_pago").cast(DateType()).alias("FEC_INI_PER_PAGO"),
    col("r.fec_fin_per_pago").cast(DateType()).alias("FEC_FIN_PER_PAGO"),
    col("r.ani_per_pago").cast(IntegerType()).alias("ANI_PER_PAGO"),
    col("r.cod_cpt_pago").alias("COD_CPT_PAGO"),
    col("r.dsc_cpt_pago").alias("DSC_CPT_PAGO"),
    col("r.ramo").alias("RAMO"),
    col("r.unidad").cast(IntegerType()).alias("UNIDAD"),
    col("r.equivalencia").alias("EQUIVALENCIA"),
    col("r.nro_poliza").cast(LongType()).alias("NRO_POLIZA"),
    col("r.fec_exp_renta").cast(DateType()).alias("FEC_EXP_POLIZA"),
    col("r.per_exp_renta").cast(IntegerType()).alias("PER_EXP_POLIZA"),
    col("r.fec_ini_per_exp_renta").cast(DateType()).alias("FEC_INI_PER_EXP_POLIZA"),
    col("r.fec_fin_per_exp_renta").cast(DateType()).alias("FEC_FIN_PER_EXP_POLIZA"),
    col("r.ani_per_exp_renta").cast(IntegerType()).alias("ANI_EXP_POLIZA"),
    col("r.nro_poliza").cast(LongType()).alias("NRO_SINIESTRO"),
    col("r.cod_etd_siniestro").alias("COD_ETD_SINIESTRO"),
    col("r.fec_exp_renta").cast(DateType()).alias("FEC_VIG_INICIAL"),
    col("r.fec_csc_derecho").cast(DateType()).alias("FEC_CAU_DERECHO"),
    col("r.cod_nov_org_pension").cast(StringType()).alias("COD_ORG_PENSION"),
    col("r.dsc_nov_org_pension").alias("DSC_ORG_PENSION"),
    col("r.cod_nov_org_pension").alias("COD_NOV_ORG_PENSION"),
    col("r.dsc_nov_org_pension").alias("DSC_NOV_ORG_PENSION"),
    col("r.par_afiliado").alias("PRT_AFILIADO"),
    col("r.cod_org_tip_idt_afiliado").alias("COD_ORG_TIP_IDT_AFILIADO"),
    col("r.cod_tip_idt_afiliado").alias("COD_TIP_IDT_AFILIADO"),
    col("r.nid_num_idt_afiliado").cast(LongType()).alias("NRO_DOC_IDT_AFILIADO"),
    col("r.pmr_ape_afiliado").alias("PMR_APE_AFILIADO"),
    col("r.sgn_ape_afiliado").alias("SGN_APE_AFILIADO"),
    col("r.pmr_nom_afiliado").alias("PMR_NOM_AFILIADO"),
    col("r.sgn_nom_afiliado").alias("SGN_NOM_AFILIADO"),
    col("r.nom_cpt_afiliado").alias("NOM_CPT_AFILIADO"),
    col("r.gen_afiliado").alias("GEN_AFILIADO"),
    col("r.sex_afiliado").cast(IntegerType()).alias("SEX_AFILIADO"),
    col("r.fec_nac_afiliado").cast(DateType()).alias("FEC_NAC_AFILIADO"),
    col("r.est_afiliado").alias("ETD_AFILIADO"),
    col("r.cod_etd_afiliado").alias("COD_EST_AFILIADO"),
    col("r.dsc_etd_afiliado").alias("DSC_EST_AFILIADO"),
    col("r.fec_mrt_afiliado").cast(DateType()).alias("FEC_MRT_AFILIADO"),
    col("hj.nro_hjs_ttl_afiliado").cast(IntegerType()).alias("NRO_HJS_TTL_AFILIADO"),
    col("hi.nro_hjs_inv_afiliado").cast(IntegerType()).alias("NRO_HJS_INV_AFILIADO"),
    col("r.ing_bas_liquidacion").cast(DecimalType(18, 2)).alias("IGR_BSE_LIQ_AFILIADO"),
    col("r.mesada").cast(DecimalType(18, 2)).alias("MSD_AFILIADO"),
    col("r.nro_mesadas").cast(IntegerType()).alias("NRO_MSD_AFILIADO"),
    col("r.retroactivo").alias("RETROACTIVO"),
    col("r.grd_inv_inicial").alias("GRD_INV_INICIAL"),
    col("r.fec_ult_modificacion").cast(DateType()).alias("FEC_ULT_MOD"),
    col("r.grd_inv_actual").alias("GRD_INV_ACTUAL"),
    col("r.gastos_final").alias("GASTOS")
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
    "es_beneficiario": "ES_BENEFICIARIO",
    "con_derecho": "CON_DER_BENEFICIARIO",
    "fec_perdida_derecho": "FEC_PDD_DER_BENEFICIARIO"
}

for i in range(1, 16):
    for campo, alias_base in alias_benef.items():
        select_cols.append(col(f"bp.{campo}_{i}").alias(f"{alias_base}_{i}"))

select_cols.extend([
    col("r.nro_rad_not_tec").cast(LongType()).alias("NRO_RAD_NOT_TEC"),
    col("r.int_tec_prima").alias("INT_TEC_PRIMA"),
    col("r.tsa_crc_prima").alias("TAS_CREC_PRIMA"),
    col("r.gst_calculo").alias("GST_PRIMA"),
    col("r.prima").alias("PRIMA"),
    col("r.fec_ini_rnt_diferida").cast(DateType()).alias("FEC_INI_RNT_DIFERIDA"),
    col("r.vlr_msd_bnf_per_diferido").alias("VLR_MSD_BNF_PER_DIFERIDA"),
    when(col("r.vlr_aux_funerario") == lit(0), lit(None)).otherwise(col("r.vlr_aux_funerario")).alias("VLR_AUX_FUNERARIO"),
    col("r.observaciones").alias("OBSERVACIONES"),
    col("r.vlr_rsv_rv08").alias("VLR_RSV_RV08"),
    col("r.vlr_rsv_rv89").alias("VLR_RSV_RV89"),
    spark_round(col("r.par_0"), 2).cast(DecimalType(38, 2)).alias("PAR_0"),
    spark_round(col("r.parma"), 2).cast(DecimalType(38, 2)).alias("PARMA"),
    col("r.por_exc_parma").alias("POR_EXC_PARMA"),
    spark_round(col("r.por_mrt_acumulado"), 2).cast(DecimalType(38, 2)).alias("POR_AMT_ACUM"),
    col("r.fec_avi_siniestro").cast(DateType()).alias("FEC_AVI_SINIESTRO"),
    col("r.fair_value").alias("FAR_VALUE"),
    col("r.inc_mecanismo").alias("INS_MECANISMO"),
    col("r.fec_inc_mecanismo").cast(DateType()).alias("FEC_INS_MECANISMO"),
    col("r.ptr_dzt").alias("PRM_DESLIZAMIENTO"),
    spark_round(col("r.tas_fac_seg_calculo") / lit(100), 4).cast(DecimalType(38, 4)).alias("TAS_FAC_SEGURIDAD"),
    col("r.vlr_cobertura").alias("VLR_COBERTURA"),
    col("r.int_tec_niif4").alias("ITR_TEC_NIIF4"),
    col("r.nro_calculo").cast(LongType()).alias("NID_CALCULO"),
    col("r.nid_calculo_2").cast(LongType()).alias("NID_CALCULO_2"),
    col("r.nid_distribucion_vigencia_spn").alias("NID_DISTRIBUCION_VIGENCIA"),
    current_timestamp().alias("FEC_INI_VIG_REG"),
    lit(None).cast("timestamp").alias("FEC_FIN_VIG_REG"),
    lit(1).cast(IntegerType()).alias("VRS_REG"),
    lit(1).cast(IntegerType()).alias("ACT_REG"),
    lit(lote_cargue_id).cast(LongType()).alias("LOTE_CARGUE_ID")
])

df_preparado = (
    joined
    .select(*select_cols)
    .dropDuplicates(["PER_CORTE", "NRO_POLIZA", "PER_PAGO", "COD_CPT_PAGO"])
)


total_preparado = df_preparado.count()
if total_preparado == 0:
    raise ValueError("Silver preparada RVI termino con 0 registros para PER_CORTE=" + per_corte)
logger.info(f"Total registros Silver preparada RVI PER_CORTE={per_corte}: {total_preparado}")


# =================================================
# ESCRITURA OVERWRITE POR PER_CORTE
# =================================================
logger.info(f"Escribiendo Silver preparada RVI en overwrite por corte: {slv_prep_target}")

(
    df_preparado
    .write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(slv_prep_target)
)


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
    "tabla_pivote_segmentada": "general/anp_movimientos_pago",
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
    "nombre_caso": "reserva_matematica_rvi",
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
    "observacion": "Silver preparada RVI v2.3. Doble cruce CRV_CALCULOS_2, NID_CALCULO_2 trazable, IPC LEFT JOIN, regla historica comun, hijos por tipo+numero y formatos finales."
}

parametros_extraccion["silver_preparacion"] = parametros_extraccion["silver_preparaciones"][per_corte]

guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

logger.info("parametros_extraccion.json actualizado con estado_preparacion_silver SUCCESS.")
logger.info("=== FIN SILVER PREPARADA RESERVA MATEMATICA RVI V2.3 ===")

job.commit()
