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
    concat_ws,
    trim,
    when,
    date_format
)
from pyspark.sql.types import (
    LongType,
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
        f"El JSON de parametros_extraccion no tiene remediación exitosa. "
        f"estado_remediacion={estado_remediacion}"
    )

if estado_disponibilizacion_silver != "SUCCESS":
    raise ValueError(
        f"El JSON de parametros_extraccion no tiene disponibilización exitosa en Silver. "
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
    "arpprod/par_endosos",
    "arpprod/par_afiliaciones",
    "general/cgr_sucursales_alfa",
    "general/acu_personas_juridicas",
    "general/acu_personas_naturales"
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
ds_par_endosos = mapa_parametros["arpprod/par_endosos"]
ds_par_afiliaciones = mapa_parametros["arpprod/par_afiliaciones"]
ds_sucursales = mapa_parametros["general/cgr_sucursales_alfa"]
ds_personas_juridicas = mapa_parametros["general/acu_personas_juridicas"]
ds_personas_naturales = mapa_parametros["general/acu_personas_naturales"]


# -------------------------------------------------
# RUTAS SILVER BASE
# Estas rutas ya deben haber sido creadas por remediación.
# -------------------------------------------------
ruta_par_endosos = ensure_trailing_slash(ds_par_endosos["ruta_dataset_destino"])
ruta_par_afiliaciones = ensure_trailing_slash(ds_par_afiliaciones["ruta_dataset_destino"])
ruta_sucursales = ensure_trailing_slash(ds_sucursales["ruta_dataset_destino"])
ruta_personas_juridicas = ensure_trailing_slash(ds_personas_juridicas["ruta_dataset_destino"])
ruta_personas_naturales = ensure_trailing_slash(ds_personas_naturales["ruta_dataset_destino"])

logger.info(f"Ruta PAR_ENDOSOS            : {ruta_par_endosos}")
logger.info(f"Ruta PAR_AFILIACIONES       : {ruta_par_afiliaciones}")
logger.info(f"Ruta CGR_SUCURSALES_ALFA    : {ruta_sucursales}")
logger.info(f"Ruta ACU_PERSONAS_JURIDICAS : {ruta_personas_juridicas}")
logger.info(f"Ruta ACU_PERSONAS_NATURALES : {ruta_personas_naturales}")


# -------------------------------------------------
# LECTURA DESDE SILVER BASE
# Nuevo estándar:
# - Primero usa segmentos_preparacion.lotes_segmento.
# - Fallback a dataset.lotes_segmento.
# - Fallback final a lote_cargue_id.
# -------------------------------------------------
df_par_endosos = leer_silver_base(
    spark=spark,
    ruta=ruta_par_endosos,
    parametros_extraccion=parametros_extraccion,
    tabla_relativa="arpprod/par_endosos",
    dataset=ds_par_endosos,
    lote_actual=lote_cargue_id,
    nombre_tabla="arpprod/par_endosos"
)

df_par_afiliaciones = leer_silver_base(
    spark=spark,
    ruta=ruta_par_afiliaciones,
    parametros_extraccion=parametros_extraccion,
    tabla_relativa="arpprod/par_afiliaciones",
    dataset=ds_par_afiliaciones,
    lote_actual=lote_cargue_id,
    nombre_tabla="arpprod/par_afiliaciones"
)

df_sucursales = leer_silver_base(
    spark=spark,
    ruta=ruta_sucursales,
    parametros_extraccion=parametros_extraccion,
    tabla_relativa="general/cgr_sucursales_alfa",
    dataset=ds_sucursales,
    lote_actual=lote_cargue_id,
    nombre_tabla="general/cgr_sucursales_alfa"
)

df_personas_juridicas = leer_silver_base(
    spark=spark,
    ruta=ruta_personas_juridicas,
    parametros_extraccion=parametros_extraccion,
    tabla_relativa="general/acu_personas_juridicas",
    dataset=ds_personas_juridicas,
    lote_actual=lote_cargue_id,
    nombre_tabla="general/acu_personas_juridicas"
)

df_personas_naturales = leer_silver_base(
    spark=spark,
    ruta=ruta_personas_naturales,
    parametros_extraccion=parametros_extraccion,
    tabla_relativa="general/acu_personas_naturales",
    dataset=ds_personas_naturales,
    lote_actual=lote_cargue_id,
    nombre_tabla="general/acu_personas_naturales"
)


# -------------------------------------------------
# VALIDACION DE INSUMOS SILVER
# -------------------------------------------------
total_par_endosos = df_par_endosos.count()
total_par_afiliaciones = df_par_afiliaciones.count()
total_sucursales = df_sucursales.count()
total_personas_juridicas = df_personas_juridicas.count()
total_personas_naturales = df_personas_naturales.count()

logger.info(f"Registros PAR_ENDOSOS preparación            : {total_par_endosos}")
logger.info(f"Registros PAR_AFILIACIONES preparación       : {total_par_afiliaciones}")
logger.info(f"Registros CGR_SUCURSALES_ALFA preparación    : {total_sucursales}")
logger.info(f"Registros ACU_PERSONAS_JURIDICAS preparación : {total_personas_juridicas}")
logger.info(f"Registros ACU_PERSONAS_NATURALES preparación : {total_personas_naturales}")

if total_par_endosos == 0:
    raise ValueError(
        "No hay registros en Silver base para arpprod/par_endosos "
        "usando los lotes de preparación."
    )


# -------------------------------------------------
# NOMBRE TERCERO
# -------------------------------------------------
nom_juridico = when(
    trim(col("pj.NOMBRE_COMERCIAL")).isNull() | (trim(col("pj.NOMBRE_COMERCIAL")) == ""),
    trim(col("pj.RAZON_SOCIAL"))
).otherwise(trim(col("pj.NOMBRE_COMERCIAL")))

nom_natural = concat_ws(
    " ",
    trim(col("pn.PRIMER_APELLIDO")),
    trim(col("pn.SEGUNDO_APELLIDO")),
    trim(col("pn.PRIMER_NOMBRE")),
    trim(col("pn.SEGUNDO_NOMBRE"))
)

nom_tercero = when(
    col("pj.COD_TIPO_IDENTIFICACION").isNotNull(),
    nom_juridico
).otherwise(nom_natural)


# -------------------------------------------------
# DATASET PREPARADO PRIMAS ARL
# -------------------------------------------------
df_preparado = (
    df_par_endosos.alias("pe")
    .join(
        df_sucursales.alias("cs"),
        col("pe.COD_SUCURSAL_ALFA") == col("cs.COD_SUCURSAL_ALFA"),
        "left"
    )
    .join(
        df_par_afiliaciones.alias("pa"),
        col("pe.NID_POLIZA") == col("pa.NID_AFILIACION"),
        "left"
    )
    .join(
        df_personas_juridicas.alias("pj"),
        (col("pa.COD_TIPO_IDENTIFICACION") == col("pj.COD_TIPO_IDENTIFICACION")) &
        (col("pa.NID_NUMERO_IDENTIFICACION") == col("pj.NID_NUMERO_IDENTIFICACION")),
        "left"
    )
    .join(
        df_personas_naturales.alias("pn"),
        (col("pa.COD_TIPO_IDENTIFICACION") == col("pn.COD_TIPO_IDENTIFICACION")) &
        (col("pa.NID_NUMERO_IDENTIFICACION") == col("pn.NID_NUMERO_IDENTIFICACION")),
        "left"
    )
    .select(
        col("pe.NID_POLIZA").cast(LongType()).alias("NID_POLIZA"),
        col("pe.NID_ENDOSO").cast(LongType()).alias("NID_ENDOSO"),
        col("pe.VLR_PRIMA").cast(DecimalType(17, 2)).alias("VLR_PRIMA"),
        col("pe.FEC_EXPEDICION").cast("date").alias("FEC_EXPEDICION"),
        date_format(col("pe.FEC_EXPEDICION"), "yyyyMM").alias("PER_EXPEDICION"),
        col("pe.FEC_PROCESO_POINT").cast("date").alias("FEC_PROCESO_POINT"),
        date_format(col("pe.FEC_PROCESO_POINT"), "yyyyMM").alias("PER_PROCESO_POINT"),
        col("pe.COD_TIPO_ENDOSO").cast(LongType()).alias("COD_TIPO_ENDOSO"),
        col("pe.COD_TIPO_ENDOSO_ARP").cast(StringType()).alias("COD_TIPO_ENDOSO_ARP"),
        col("pe.COD_SUCURSAL_ALFA").cast(StringType()).alias("COD_SUCURSAL_ALFA"),
        col("cs.NOMBRE").cast(StringType()).alias("NOM_SUCURSAL_ALFA"),
        col("pe.MODULO").cast(StringType()).alias("MODULO"),
        col("pa.NID_AFILIACION").cast(LongType()).alias("NID_AFILIACION"),
        col("pa.COD_TIPO_IDENTIFICACION").cast(StringType()).alias("COD_TIPO_IDENTIFICACION_TERCERO"),
        col("pa.NID_NUMERO_IDENTIFICACION").cast(LongType()).alias("NID_NRO_IDENTIFICACION_TERCERO"),
        nom_tercero.cast(StringType()).alias("NOM_TERCERO"),
        col("pe.LOTE_CARGUE_ID").cast(LongType()).alias("LOTE_CARGUE_ID")
    )
    .dropDuplicates(["NID_POLIZA", "NID_ENDOSO"])
)

total_preparado = df_preparado.count()

logger.info(f"Total registros Silver preparada: {total_preparado}")


# -------------------------------------------------
# ESCRITURA SILVER PREPARADA
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
    "arpprod/par_endosos": obtener_lotes_preparacion(
        parametros_extraccion,
        "arpprod/par_endosos",
        ds_par_endosos,
        lote_cargue_id
    ),
    "arpprod/par_afiliaciones": obtener_lotes_preparacion(
        parametros_extraccion,
        "arpprod/par_afiliaciones",
        ds_par_afiliaciones,
        lote_cargue_id
    ),
    "general/cgr_sucursales_alfa": obtener_lotes_preparacion(
        parametros_extraccion,
        "general/cgr_sucursales_alfa",
        ds_sucursales,
        lote_cargue_id
    ),
    "general/acu_personas_juridicas": obtener_lotes_preparacion(
        parametros_extraccion,
        "general/acu_personas_juridicas",
        ds_personas_juridicas,
        lote_cargue_id
    ),
    "general/acu_personas_naturales": obtener_lotes_preparacion(
        parametros_extraccion,
        "general/acu_personas_naturales",
        ds_personas_naturales,
        lote_cargue_id
    )
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
    "nombre_caso": "primas_arl",
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
logger.info("=== FIN SILVER PREPARADA PRIMAS ARL ===")

job.commit()