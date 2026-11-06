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
from pyspark.sql.functions import col, lit

# -------------------------------------------------
# LOGGING
# -------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def _lotes_requeridos_dataset(parametros, item, lote_actual):
    tabla = str(item.get("tabla_relativa", "")).strip().lower()
    referencias = parametros.get("lotes_usados_preparacion_silver") or {}

    candidatos = []
    for nombre, lotes in referencias.items():
        if str(nombre).strip().lower() == tabla:
            candidatos.append(lotes)

    candidatos.append(item.get("lotes_preparacion_silver"))

    for segmento in parametros.get("segmentos_preparacion", []):
        if str(segmento.get("tabla_relativa", "")).strip().lower() == tabla:
            candidatos.append(segmento.get("lotes_segmento"))

    candidatos.extend([
        item.get("lotes_segmento"),
        (item.get("segmentacion_horizontal") or {}).get("lotes_segmento"),
        item.get("lote_cargue_id_dataset"),
        lote_actual,
    ])

    for valor in candidatos:
        if valor is None:
            continue
        if not isinstance(valor, (list, tuple, set)):
            valor = [valor]
        lotes = sorted(set(int(x) for x in valor if x is not None))
        if lotes:
            return lotes

    raise ValueError("Sin lotes requeridos para " + tabla)


def _leer_lotes_disponibles(spark, ruta, lotes):
    import boto3
    from urllib.parse import urlparse
    from pyspark.sql.functions import col

    uri = urlparse(ruta)
    if uri.scheme != "s3":
        raise ValueError("Ruta no S3: " + ruta)

    s3 = boto3.client("s3")
    hay_parquet = False
    for pagina in s3.get_paginator("list_objects_v2").paginate(
        Bucket=uri.netloc,
        Prefix=uri.path.lstrip("/").rstrip("/") + "/",
    ):
        if any(
            obj["Key"].lower().endswith(".parquet")
            and obj.get("Size", 0) > 0
            for obj in pagina.get("Contents", [])
        ):
            hay_parquet = True
            break

    if not hay_parquet:
        return None, set()

    # Los errores de permisos, lectura o esquema no se ocultan.
    df = spark.read.parquet(ruta)
    columnas = [c for c in df.columns if c.lower() == "lote_cargue_id"]
    if len(columnas) != 1:
        raise ValueError("Columna de lote ausente o ambigua: " + ruta)

    columna = columnas[0]
    presentes = {
        int(fila[0])
        for fila in (
            df.select(col(columna).cast("bigint").alias("_lote"))
            .filter(col("_lote").isin(lotes))
            .distinct()
            .collect()
        )
    }
    return df, presentes

def ensure_trailing_slash(path: str) -> str:
    return path if path.endswith("/") else path + "/"


def parse_s3_uri(ruta_s3: str):
    parsed = urlparse(ruta_s3)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return bucket, key


def cargar_json_s3(ruta_s3: str) -> dict:
    bucket, key = parse_s3_uri(ruta_s3)
    s3 = boto3.client("s3")

    obj = s3.get_object(
        Bucket=bucket,
        Key=key
    )

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


def obtener_columna_real(df, nombre_columna: str):
    mapa = {c.lower(): c for c in df.columns}
    return mapa.get(nombre_columna.lower())


def debe_disponibilizar_dataset(item: dict) -> bool:
    """Disponibiliza datos nuevos o un snapshot reutilizado que falta en Silver."""
    if item.get("estado") != "SUCCESS":
        return False

    if item.get("requiere_disponibilizacion_silver") is not True:
        return False

    modo = str(item.get("modo_disponibilidad") or "").upper()
    escritura = str(item.get("escritura_bronze") or "").upper()

    if modo == "PENDIENTE_DESDE_BRONZE" and escritura == "REUTILIZADA":
        return True

    if escritura == "SKIPPED":
        return False

    return item.get("datos_nuevos") is not False


def resolver_modo_escritura_silver(item: dict) -> str:
    """
    Reglas:
    - Si hubo exclusión y aun así hay datos nuevos, es incremental: append.
    - En primera carga o tabla no segmentada con datos: overwrite.
    """
    exclusion_aplicada = bool(item.get("exclusion_aplicada", False))
    datos_nuevos = bool(item.get("datos_nuevos", False))
    escritura_bronze = item.get("escritura_bronze")

    if str(item.get("modo_disponibilidad") or "").upper() == "PENDIENTE_DESDE_BRONZE":
        return "overwrite"

    if exclusion_aplicada and datos_nuevos and escritura_bronze == "SUCCESS":
        return "append"

    return "overwrite"


def registrar_dataset_disponibilizacion(
    tabla_relativa,
    ruta_origen,
    ruta_destino,
    modo_escritura,
    lote_cargue_id,
    total_registros,
    estado,
    fecha_proceso,
    motivo=None
):
    registro = {
        "tabla_relativa": tabla_relativa,
        "ruta_origen_bronze": ruta_origen,
        "ruta_destino_silver": ruta_destino,
        "modo_escritura": modo_escritura,
        "lote_cargue_id": lote_cargue_id,
        "total_registros_copiados": total_registros,
        "estado": estado,
        "fecha_disponibilizacion": fecha_proceso
    }

    if motivo:
        registro["motivo"] = motivo

    return registro


# -------------------------------------------------
# PARAMETROS GLUE
# -------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RUTA_PARAMETROS_EXTRACCION",
        "RUTA_ORIGEN_REMEDIACION",
        "RUTA_DESTINO_REMEDIACION"
    ]
)

ruta_parametros_extraccion = args["RUTA_PARAMETROS_EXTRACCION"]
ruta_origen_remediacion = ensure_trailing_slash(args["RUTA_ORIGEN_REMEDIACION"])
ruta_destino_remediacion = ensure_trailing_slash(args["RUTA_DESTINO_REMEDIACION"])


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
spark.conf.set("spark.sql.caseSensitive", "false")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info("=== INICIO JOB REMEDIACION ESTANDAR NIIF ===")
logger.info(f"Ruta parametros extracción   : {ruta_parametros_extraccion}")
logger.info(f"Ruta origen remediación      : {ruta_origen_remediacion}")
logger.info(f"Ruta destino remediación     : {ruta_destino_remediacion}")


# -------------------------------------------------
# LEE PARAMETROS EXTRACCION
# -------------------------------------------------
parametros_extraccion = cargar_json_s3(ruta_parametros_extraccion)

nombre_caso = parametros_extraccion.get("nombre_caso", "NO_INFORMADO")
lote_cargue_id = parametros_extraccion.get("lote_cargue_id")
estado_extraccion = parametros_extraccion.get("estado_extraccion")
estado_validacion = parametros_extraccion.get("estado_validacion")
datasets_extraidos = parametros_extraccion.get("datasets_extraidos", [])

if lote_cargue_id is None:
    raise ValueError("No se encontró lote_cargue_id en parametros_extraccion.json")

if estado_extraccion != "SUCCESS":
    raise ValueError(
        f"La extracción no está en SUCCESS. estado_extraccion={estado_extraccion}"
    )

if estado_validacion not in ["SUCCESS", "FAILED"]:
    raise ValueError(
        f"Estado de validación no soportado para remediación: {estado_validacion}"
    )

if not datasets_extraidos:
    raise ValueError("No se encontraron datasets_extraidos en parametros_extraccion.json")

logger.info(f"Nombre caso                  : {nombre_caso}")
logger.info(f"Lote cargue id               : {lote_cargue_id}")
logger.info(f"Estado extracción            : {estado_extraccion}")
logger.info(f"Estado validación            : {estado_validacion}")
logger.info(f"Datasets extraídos           : {len(datasets_extraidos)}")

fecha_proceso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
fecha_particion = datetime.now().strftime("%Y%m%d_%H%M%S")


# =================================================
# PASO 1: DISPONIBILIZACION CONTROLADA
# Bronze -> Silver base
# =================================================
logger.info("Se inicia disponibilización controlada Bronze -> Silver base.")
logger.info("Nuevo estándar: no sobrescribir Silver base para datasets SKIPPED o sin datos nuevos.")

datasets_disponibilizados = []
datasets_copiados = []
datasets_omitidos = []

for item in datasets_extraidos:
    tabla_relativa = item.get("tabla_relativa")
    ruta_origen_raw = item.get("ruta_dataset_origen")
    ruta_destino_raw = item.get("ruta_dataset_destino")

    if not tabla_relativa:
        raise ValueError(f"Dataset sin tabla_relativa: {item}")

    if not ruta_origen_raw:
        raise ValueError(f"Dataset {tabla_relativa} sin ruta_dataset_origen")

    if not ruta_destino_raw:
        raise ValueError(f"Dataset {tabla_relativa} sin ruta_dataset_destino")

    ruta_origen = ensure_trailing_slash(ruta_origen_raw)
    ruta_destino = ensure_trailing_slash(ruta_destino_raw)

    logger.info("-------------------------------------------------")
    logger.info(f"Evaluando tabla                 : {tabla_relativa}")
    logger.info(f"Origen Bronze                   : {ruta_origen}")
    logger.info(f"Destino Silver                  : {ruta_destino}")
    logger.info(f"estado dataset                  : {item.get('estado')}")
    logger.info(f"escritura_bronze                : {item.get('escritura_bronze')}")
    logger.info(f"datos_nuevos                    : {item.get('datos_nuevos')}")
    logger.info(f"requiere_disponibilizacion      : {item.get('requiere_disponibilizacion_silver')}")
    logger.info(f"exclusion_aplicada              : {item.get('exclusion_aplicada')}")
    logger.info(f"lotes_segmento                  : {item.get('lotes_segmento')}")



    # Recuperar solo referencias reutilizadas o segmentadas.
    recuperacion = (
        bool(item.get("exclusion_aplicada"))
        or str(item.get("escritura_bronze") or "").upper()
           == "REUTILIZADA"
        or str(item.get("modo_disponibilidad") or "").upper()
           in ("PENDIENTE_DESDE_BRONZE", "REUTILIZADA_COMPARTIDA")
    )

    if recuperacion:
        # Reconciliar los lotes requeridos, independientemente de datos_nuevos.
        item = dict(item)
        lotes_requeridos = _lotes_requeridos_dataset(
            parametros_extraccion, item, lote_cargue_id
        )
    
        _, presentes_silver = _leer_lotes_disponibles(
            spark, ruta_destino, lotes_requeridos
        )
        lotes_faltantes = sorted(set(lotes_requeridos) - presentes_silver)
        lotes_copiar = []
    
        if item.get("estado") == "SUCCESS" and lotes_faltantes:
            _, presentes_bronze = _leer_lotes_disponibles(
                spark, ruta_origen, lotes_faltantes
            )
            ausentes = sorted(set(lotes_faltantes) - presentes_bronze)
    
            sh = item.get("segmentacion_horizontal") or {}
            espera_filas = (
                int(item.get("total_registros") or 0) > 0
                or int(
                    item.get("registros_excluidos_por_segmento")
                    or sh.get("registros_excluidos_por_segmento")
                    or 0
                ) > 0
                or any(int(x) != int(lote_cargue_id) for x in lotes_faltantes)
            )
    
            if ausentes and espera_filas:
                raise ValueError(
                    f"{tabla_relativa}: lotes requeridos ausentes "
                    f"en Bronze y Silver: {ausentes}"
                )
    
            lotes_copiar = sorted(presentes_bronze)
    
        item["requiere_disponibilizacion_silver"] = bool(lotes_copiar)
        if lotes_copiar:
            item["modo_disponibilidad"] = "PENDIENTE_DESDE_BRONZE"
            item["escritura_bronze"] = "REUTILIZADA"
    
        logger.info(
            f"{tabla_relativa}: requeridos={lotes_requeridos}, "
            f"presentes_silver={sorted(presentes_silver)}, "
            f"copiar_desde_bronze={lotes_copiar}"
        )
    else:
        lote_normal = item.get("lote_cargue_id_dataset") or lote_cargue_id
        lotes_copiar = [int(lote_normal)]

    # Recuperacion acotada: no registrar varios lotes como uno solo.
    if recuperacion and len(lotes_copiar) > 1:
        raise ValueError(
            f"{tabla_relativa}: recuperacion de varios lotes pendiente "
            f"de tratamiento individual: {lotes_copiar}"
        )

    if not debe_disponibilizar_dataset(item):
        motivo = (
            "DATASET_NO_REQUIERE_DISPONIBILIZACION_O_SKIPPED"
        )

        logger.info(
            f"Se omite disponibilización de {tabla_relativa}. "
            f"Motivo: {motivo}"
        )

        registro = registrar_dataset_disponibilizacion(
            tabla_relativa=tabla_relativa,
            ruta_origen=ruta_origen,
            ruta_destino=ruta_destino,
            modo_escritura="SKIPPED",
            lote_cargue_id=lote_cargue_id,
            total_registros=0,
            estado="SKIPPED",
            fecha_proceso=fecha_proceso,
            motivo=motivo
        )

        datasets_disponibilizados.append(registro)
        datasets_omitidos.append(registro)
        continue

    modo_escritura_silver = (
        "append" if recuperacion
        else resolver_modo_escritura_silver(item)
    )

    logger.info(f"Modo escritura Silver calculado : {modo_escritura_silver}")

    lote_dataset = (
        lotes_copiar[0] if recuperacion
        else item.get("lote_cargue_id_dataset") or lote_cargue_id
    )

    df_dataset = (
        spark.read.parquet(ruta_origen)
        .filter(
            col("LOTE_CARGUE_ID").cast("bigint").isin(lotes_copiar)
        )
    )

    total_registros = df_dataset.count()

    logger.info(f"Registros a copiar              : {total_registros}")

    if total_registros == 0:
        motivo = "SIN_REGISTROS_EN_BRONZE_PARA_LOTE_ACTUAL_NO_SE_SOBRESCRIBE_SILVER"

        logger.warning(
            f"No hay registros para lote {lote_dataset} en tabla {tabla_relativa}. "
            "No se escribe en Silver para evitar borrar universo previo."
        )

        registro = registrar_dataset_disponibilizacion(
            tabla_relativa=tabla_relativa,
            ruta_origen=ruta_origen,
            ruta_destino=ruta_destino,
            modo_escritura="SKIPPED",
            lote_cargue_id=lote_dataset,
            total_registros=0,
            estado="SKIPPED_SIN_REGISTROS",
            fecha_proceso=fecha_proceso,
            motivo=motivo
        )

        datasets_disponibilizados.append(registro)
        datasets_omitidos.append(registro)
        continue

    logger.info(
        f"Escribiendo Silver base para {tabla_relativa} "
        f"modo={modo_escritura_silver}"
    )

    (
        df_dataset
        .write
        .mode(modo_escritura_silver)
        .option("compression", "snappy")
        .parquet(ruta_destino)
    )

    registro = registrar_dataset_disponibilizacion(
        tabla_relativa=tabla_relativa,
        ruta_origen=ruta_origen,
        ruta_destino=ruta_destino,
        modo_escritura=modo_escritura_silver,
        lote_cargue_id=lote_dataset,
        total_registros=total_registros,
        estado="SUCCESS",
        fecha_proceso=fecha_proceso
    )

    datasets_disponibilizados.append(registro)
    datasets_copiados.append(registro)

logger.info("Disponibilización controlada Bronze -> Silver base finalizada.")
logger.info(f"Datasets copiados correctamente : {len(datasets_copiados)}")
logger.info(f"Datasets omitidos/skipped       : {len(datasets_omitidos)}")


# =================================================
# PASO 2: SI VALIDACION FAILED, COPIAR ERRORES
# =================================================
remediacion_errores = None

if estado_validacion == "FAILED":

    logger.warning("Validación técnica con hallazgos. Se copiarán errores a histórico de remediación.")

    ruta_errores_validacion = (
        parametros_extraccion.get("ruta_validacion_tecnica")
        or ruta_origen_remediacion
    )
    ruta_errores_validacion = ensure_trailing_slash(ruta_errores_validacion)

    logger.info(f"Ruta errores validación a leer: {ruta_errores_validacion}")

    try:
        df_errores = spark.read.parquet(ruta_errores_validacion)
    except Exception as e:
        logger.error("No fue posible leer la ruta origen de remediación.", exc_info=True)
        raise ValueError(
            f"No fue posible leer la ruta origen de remediación: {ruta_errores_validacion}"
        ) from e

    col_lote_real = obtener_columna_real(df_errores, "lote_cargue_id")

    if col_lote_real is None:
        raise ValueError("La tabla de errores no contiene la columna lote_cargue_id.")

    df_lote = df_errores.filter(
        col(col_lote_real).cast("string") == str(lote_cargue_id)
    )

    if col_lote_real != "lote_cargue_id":
        df_lote = df_lote.withColumnRenamed(col_lote_real, "lote_cargue_id")

    total_errores_lote = df_lote.count()

    logger.info(f"Total errores del lote       : {total_errores_lote}")

    if total_errores_lote == 0:
        logger.warning("Validación está FAILED, pero no se encontraron errores para el lote actual.")

        remediacion_errores = {
            "estado": "SIN_ERRORES_ENCONTRADOS",
            "tipo": "COPIA_ERRORES_VALIDACION_TECNICA",
            "modo": "HISTORICO",
            "lote_cargue_id": lote_cargue_id,
            "ruta_origen": ruta_errores_validacion,
            "ruta_destino_base": ruta_destino_remediacion,
            "total_registros_copiados": 0,
            "fecha_remediacion": fecha_proceso
        }

    else:
        df_salida = (
            df_lote
            .withColumn("fecha_copia_remediacion", lit(fecha_particion))
            .withColumn("fecha_copia_remediacion_ts", lit(fecha_proceso))
            .withColumn("estado_remediacion", lit("PENDIENTE_ANALITICA"))
            .withColumn("tipo_remediacion", lit("COPIA_ERRORES_VALIDACION_TECNICA"))
        )

        ruta_destino_lote = (
            f"{ruta_destino_remediacion}"
            f"lote_cargue_id={lote_cargue_id}/"
            f"fecha_copia_remediacion={fecha_particion}/"
        )

        logger.info(f"Ruta destino histórica       : {ruta_destino_lote}")
        logger.info("Escribiendo tabla de errores filtrada por lote en ruta histórica de remediación.")

        (
            df_salida
            .write
            .mode("append")
            .option("compression", "snappy")
            .parquet(ruta_destino_lote)
        )

        logger.info(f"Tabla de remediación escrita en: {ruta_destino_lote}")

        remediacion_errores = {
            "estado": "PENDIENTE_ANALITICA",
            "tipo": "COPIA_ERRORES_VALIDACION_TECNICA",
            "modo": "HISTORICO",
            "modo_escritura_remediacion": "append",
            "lote_cargue_id": lote_cargue_id,
            "ruta_origen": ruta_errores_validacion,
            "ruta_destino_base": ruta_destino_remediacion,
            "ruta_destino_historica": ruta_destino_lote,
            "total_registros_copiados": total_errores_lote,
            "fecha_remediacion": fecha_proceso,
            "fecha_copia_remediacion_particion": fecha_particion
        }

elif estado_validacion == "SUCCESS":
    logger.info("Validación técnica SUCCESS. No se requiere copia de errores a histórico.")

else:
    raise ValueError(
        f"Estado de validación no soportado para remediación: {estado_validacion}"
    )


# =================================================
# PASO 3: ACTUALIZAR PARAMETROS_EXTRACCION
# =================================================
if estado_validacion == "SUCCESS":
    estado_remediacion_detalle = "SUCCESS_SIN_HALLAZGOS"
    requiere_revision_analitica = False
    estado_hallazgos_validacion = "SIN_HALLAZGOS"
else:
    estado_remediacion_detalle = "SUCCESS_CON_HALLAZGOS_VALIDACION"
    requiere_revision_analitica = True
    estado_hallazgos_validacion = "CON_HALLAZGOS"

parametros_extraccion["remediacion"] = {
    "estado": estado_remediacion_detalle,
    "tipo": "DISPONIBILIZACION_Y_TRAZABILIDAD_VALIDACION",
    "modo": "CONTINUA_FLUJO",
    "lote_cargue_id": lote_cargue_id,
    "estado_validacion": estado_validacion,
    "requiere_revision_analitica": requiere_revision_analitica,
    "fecha_remediacion": fecha_proceso,
    "disponibilizacion_silver": {
        "estado": "SUCCESS",
        "tipo": "DISPONIBILIZACION_BRONZE_A_SILVER",
        "modo_escritura_silver_base": "CONTROLADO_POR_DATASET",
        "total_datasets_evaluados": len(datasets_disponibilizados),
        "total_datasets_copiados": len(datasets_copiados),
        "total_datasets_omitidos": len(datasets_omitidos),
        "datasets": datasets_disponibilizados
    },
    "errores_validacion": remediacion_errores
}

# -------------------------------------------------
# CAMPOS DE CONTROL PARA COMPONENTES SIGUIENTES
# -------------------------------------------------
parametros_extraccion["estado_remediacion"] = "SUCCESS"
parametros_extraccion["estado_disponibilizacion_silver"] = "SUCCESS"
parametros_extraccion["datasets_disponibilizados_silver"] = datasets_disponibilizados
parametros_extraccion["datasets_copiados_silver"] = datasets_copiados
parametros_extraccion["datasets_omitidos_silver"] = datasets_omitidos
parametros_extraccion["requiere_revision_analitica"] = requiere_revision_analitica
parametros_extraccion["validacion_tecnica_detiene_flujo"] = False
parametros_extraccion["estado_hallazgos_validacion"] = estado_hallazgos_validacion
parametros_extraccion["fecha_remediacion"] = fecha_proceso
parametros_extraccion["modo_flujo_post_validacion"] = "CONTINUA_A_SILVER_Y_GOLD"
parametros_extraccion["modo_escritura_silver_base"] = "CONTROLADO_POR_DATASET"

if estado_validacion == "FAILED":
    parametros_extraccion["ruta_remediacion_historica"] = (
        remediacion_errores.get("ruta_destino_historica")
        if remediacion_errores
        else None
    )
    parametros_extraccion["total_registros_remediacion"] = (
        remediacion_errores.get("total_registros_copiados", 0)
        if remediacion_errores
        else 0
    )
else:
    parametros_extraccion["ruta_remediacion_historica"] = None
    parametros_extraccion["total_registros_remediacion"] = 0

guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

logger.info("parametros_extraccion.json actualizado correctamente.")
logger.info(f"estado_remediacion              : {parametros_extraccion['estado_remediacion']}")
logger.info(f"estado_disponibilizacion_silver : {parametros_extraccion['estado_disponibilizacion_silver']}")
logger.info(f"estado_hallazgos_validacion     : {parametros_extraccion['estado_hallazgos_validacion']}")
logger.info(f"requiere_revision_analitica     : {parametros_extraccion['requiere_revision_analitica']}")
logger.info(f"modo_flujo_post_validacion      : {parametros_extraccion['modo_flujo_post_validacion']}")
logger.info(f"datasets copiados silver        : {len(datasets_copiados)}")
logger.info(f"datasets omitidos silver        : {len(datasets_omitidos)}")
logger.info("=== FIN JOB REMEDIACION ESTANDAR NIIF ===")

job.commit()