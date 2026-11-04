import sys
import json
import logging
import boto3
from urllib.parse import urlparse
from datetime import datetime
from functools import reduce

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, ltrim, rtrim, coalesce

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


def normalizar_nombre_para_particion(valor: str) -> str:
    return str(valor).replace("/", "_").replace(" ", "_").lower()


def debe_disponibilizar_dataset(item: dict) -> bool:
    """
    Nuevo estándar:
    Solo se disponibilizan datasets que realmente tuvieron escritura Bronze
    y que extracción marcó como requeridos para Silver base.
    """
    if item.get("requiere_disponibilizacion_silver") is False:
        return False

    if item.get("escritura_bronze") == "SKIPPED":
        return False

    if item.get("datos_nuevos") is False:
        return False

    if item.get("estado") != "SUCCESS":
        return False

    return True


def resolver_modo_escritura_silver(item: dict) -> str:
    """
    Reglas:
    - Si hubo exclusión y aun así hay datos nuevos, es incremental: append.
    - En primera carga o tabla no segmentada con datos: overwrite.
    """
    exclusion_aplicada = bool(item.get("exclusion_aplicada", False))
    datos_nuevos = bool(item.get("datos_nuevos", False))
    escritura_bronze = item.get("escritura_bronze")

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
    motivo=None,
    remediacion_datos=None
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

    if remediacion_datos is not None:
        registro["remediacion_datos"] = remediacion_datos

    return registro


def cargar_config_remediacion(ruta_config_remediacion: str):
    if not ruta_config_remediacion:
        logger.info("No se recibió RUTA_CONFIG_REMEDIACION. Remediación de datos deshabilitada.")
        return {
            "remediacion_habilitada": False,
            "reglas": [],
            "trazabilidad": {"habilitada": False}
        }

    cfg = cargar_json_s3(ruta_config_remediacion)
    logger.info(f"Config remediación cargada desde: {ruta_config_remediacion}")
    logger.info(f"Remediación habilitada: {cfg.get('remediacion_habilitada')}")
    logger.info(f"Total grupos de reglas: {len(cfg.get('reglas', []))}")
    return cfg


def reglas_aplicables_para_tabla(config_remediacion: dict, tabla_relativa: str):
    reglas = []
    for regla in config_remediacion.get("reglas", []):
        tabla_cfg = regla.get("tabla_relativa", "*")
        if tabla_cfg == "*" or str(tabla_cfg).lower() == str(tabla_relativa).lower():
            reglas.append(regla)
    return reglas


def aplicar_regla_remediacion(df: DataFrame, columna_real: str, regla: str) -> DataFrame:
    if regla == "TRIM_ESPACIOS_INICIO":
        return df.withColumn(columna_real, ltrim(col(columna_real)))

    if regla == "TRIM_ESPACIOS_FIN":
        return df.withColumn(columna_real, rtrim(col(columna_real)))

    raise ValueError(f"Regla de remediación no soportada: {regla}")


def construir_trazabilidad_regla(
    df_con_antes: DataFrame,
    nombre_caso: str,
    tabla_relativa: str,
    lote_cargue_id,
    atributo_configurado: str,
    columna_real: str,
    regla_aplicada: str,
    fecha_proceso: str,
    orden_regla: int
):
    return (
        df_con_antes
        .filter(
            coalesce(col("__valor_antes_remediacion__").cast("string"), lit("__NULL__"))
            != coalesce(col(columna_real).cast("string"), lit("__NULL__"))
        )
        .select(
            lit(nombre_caso).alias("nombre_caso"),
            lit(tabla_relativa).alias("tabla_relativa"),
            lit(str(lote_cargue_id)).alias("lote_cargue_id"),
            lit(atributo_configurado).alias("atributo"),
            lit(columna_real).alias("atributo_real"),
            lit(regla_aplicada).alias("regla_aplicada"),
            lit(orden_regla).alias("orden_regla"),
            col("__valor_antes_remediacion__").cast("string").alias("valor_antes"),
            col(columna_real).cast("string").alias("valor_despues"),
            lit(fecha_proceso).alias("fecha_remediacion")
        )
    )


def aplicar_remediacion_configurada(
    df_dataset: DataFrame,
    config_remediacion: dict,
    nombre_caso: str,
    tabla_relativa: str,
    lote_cargue_id,
    fecha_proceso: str,
    fecha_particion: str,
    ruta_destino_silver: str
):
    if not config_remediacion.get("remediacion_habilitada", False):
        return df_dataset, {
            "aplicada": False,
            "motivo": "REMEDIACION_DESHABILITADA",
            "total_valores_remediados": 0,
            "columnas_evaluadas": [],
            "columnas_remediadas": [],
            "ruta_trazabilidad": None
        }

    reglas_tabla = reglas_aplicables_para_tabla(config_remediacion, tabla_relativa)
    if not reglas_tabla:
        return df_dataset, {
            "aplicada": False,
            "motivo": "SIN_REGLAS_APLICABLES_PARA_TABLA",
            "total_valores_remediados": 0,
            "columnas_evaluadas": [],
            "columnas_remediadas": [],
            "ruta_trazabilidad": None
        }

    trazabilidad_cfg = config_remediacion.get("trazabilidad", {})
    trazabilidad_habilitada = bool(trazabilidad_cfg.get("habilitada", False))
    ruta_trazabilidad_base = ensure_trailing_slash(trazabilidad_cfg.get("ruta_base", "")) if trazabilidad_habilitada else None

    df_actual = df_dataset
    trazas = []
    columnas_evaluadas = []
    columnas_remediadas = set()
    total_valores_remediados = 0
    reglas_ejecutadas = []

    for regla_grupo in reglas_tabla:
        columnas_cfg = regla_grupo.get("columnas", [])
        reglas_remediacion = regla_grupo.get("reglas_remediacion", [])
        aplicar_si_columna_existe = regla_grupo.get("aplicar_si_columna_existe", True)

        for atributo in columnas_cfg:
            columna_real = obtener_columna_real(df_actual, atributo)
            columnas_evaluadas.append(atributo)

            if columna_real is None:
                mensaje = f"Columna {atributo} no existe en {tabla_relativa}."
                if aplicar_si_columna_existe:
                    logger.info(mensaje + " Se omite por aplicar_si_columna_existe=true.")
                    continue
                raise ValueError(mensaje)

            for idx, regla in enumerate(reglas_remediacion, start=1):
                logger.info(f"Aplicando remediación tabla={tabla_relativa}, columna={columna_real}, regla={regla}")

                df_con_antes = df_actual.withColumn("__valor_antes_remediacion__", col(columna_real).cast("string"))
                df_remediado = aplicar_regla_remediacion(df_con_antes, columna_real, regla)

                df_traza = construir_trazabilidad_regla(
                    df_con_antes=df_remediado,
                    nombre_caso=nombre_caso,
                    tabla_relativa=tabla_relativa,
                    lote_cargue_id=lote_cargue_id,
                    atributo_configurado=atributo,
                    columna_real=columna_real,
                    regla_aplicada=regla,
                    fecha_proceso=fecha_proceso,
                    orden_regla=idx
                ).cache()

                total_cambios_regla = df_traza.count()
                logger.info(
                    f"Cambios detectados tabla={tabla_relativa}, columna={columna_real}, "
                    f"regla={regla}: {total_cambios_regla}"
                )

                if total_cambios_regla > 0:
                    trazas.append(df_traza)
                    columnas_remediadas.add(atributo)
                    total_valores_remediados += total_cambios_regla
                    reglas_ejecutadas.append({
                        "tabla_relativa": tabla_relativa,
                        "atributo": atributo,
                        "atributo_real": columna_real,
                        "regla_aplicada": regla,
                        "total_valores_remediados": total_cambios_regla
                    })

                df_actual = df_remediado.drop("__valor_antes_remediacion__")

    ruta_trazabilidad_lote = None
    if trazabilidad_habilitada and trazas:
        if not ruta_trazabilidad_base:
            raise ValueError("La trazabilidad está habilitada, pero no se informó trazabilidad.ruta_base en el JSON.")

        df_trazabilidad = reduce(lambda a, b: a.unionByName(b), trazas)
        tabla_particion = normalizar_nombre_para_particion(tabla_relativa)
        ruta_trazabilidad_lote = (
            f"{ruta_trazabilidad_base}"
            f"lote_cargue_id={lote_cargue_id}/"
            f"fecha_remediacion={fecha_particion}/"
            f"tabla_relativa={tabla_particion}/"
        )

        logger.info(f"Escribiendo trazabilidad remediación en: {ruta_trazabilidad_lote}")
        (
            df_trazabilidad
            .write
            .mode(trazabilidad_cfg.get("modo_escritura", "append"))
            .option("compression", "snappy")
            .parquet(ruta_trazabilidad_lote)
        )

    return df_actual, {
        "aplicada": total_valores_remediados > 0,
        "tipo": "REMEDIACION_DATOS_CONFIGURADA",
        "tabla_relativa": tabla_relativa,
        "total_valores_remediados": total_valores_remediados,
        "columnas_evaluadas": columnas_evaluadas,
        "columnas_remediadas": sorted(list(columnas_remediadas)),
        "reglas_ejecutadas": reglas_ejecutadas,
        "ruta_trazabilidad": ruta_trazabilidad_lote,
        "ruta_destino_silver": ruta_destino_silver
    }


# -------------------------------------------------
# PARAMETROS GLUE
# -------------------------------------------------
parametros_requeridos = [
    "JOB_NAME",
    "RUTA_PARAMETROS_EXTRACCION",
    "RUTA_ORIGEN_REMEDIACION",
    "RUTA_DESTINO_REMEDIACION"
]

# Permite compatibilidad hacia atrás: si no llega RUTA_CONFIG_REMEDIACION, el job copia como antes.
if "--RUTA_CONFIG_REMEDIACION" in sys.argv:
    parametros_requeridos.append("RUTA_CONFIG_REMEDIACION")

args = getResolvedOptions(sys.argv, parametros_requeridos)

ruta_parametros_extraccion = args["RUTA_PARAMETROS_EXTRACCION"]
ruta_origen_remediacion = ensure_trailing_slash(args["RUTA_ORIGEN_REMEDIACION"])
ruta_destino_remediacion = ensure_trailing_slash(args["RUTA_DESTINO_REMEDIACION"])
ruta_config_remediacion = args.get("RUTA_CONFIG_REMEDIACION")


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
logger.info(f"Ruta config remediación      : {ruta_config_remediacion}")


# -------------------------------------------------
# LEE PARAMETROS EXTRACCION Y CONFIG REMEDIACION
# -------------------------------------------------
parametros_extraccion = cargar_json_s3(ruta_parametros_extraccion)
config_remediacion = cargar_config_remediacion(ruta_config_remediacion)

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
# Bronze -> Silver base + remediación configurada
# =================================================
logger.info("Se inicia disponibilización controlada Bronze -> Silver base.")
logger.info("Nuevo estándar: no sobrescribir Silver base para datasets SKIPPED o sin datos nuevos.")


datasets_disponibilizados = []
datasets_copiados = []
datasets_omitidos = []
datasets_remediados = []
total_valores_remediados_global = 0

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

    if not debe_disponibilizar_dataset(item):
        motivo = "DATASET_NO_REQUIERE_DISPONIBILIZACION_O_SKIPPED"

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

    modo_escritura_silver = resolver_modo_escritura_silver(item)

    logger.info(f"Modo escritura Silver calculado : {modo_escritura_silver}")

    df_dataset = (
        spark.read.parquet(ruta_origen)
        .filter(col("LOTE_CARGUE_ID").cast("bigint") == lit(lote_cargue_id).cast("bigint"))
    )

    total_registros = df_dataset.count()

    logger.info(f"Registros a copiar              : {total_registros}")

    if total_registros == 0:
        motivo = "SIN_REGISTROS_EN_BRONZE_PARA_LOTE_ACTUAL_NO_SE_SOBRESCRIBE_SILVER"

        logger.warning(
            f"No hay registros para lote {lote_cargue_id} en tabla {tabla_relativa}. "
            "No se escribe en Silver para evitar borrar universo previo."
        )

        registro = registrar_dataset_disponibilizacion(
            tabla_relativa=tabla_relativa,
            ruta_origen=ruta_origen,
            ruta_destino=ruta_destino,
            modo_escritura="SKIPPED",
            lote_cargue_id=lote_cargue_id,
            total_registros=0,
            estado="SKIPPED_SIN_REGISTROS",
            fecha_proceso=fecha_proceso,
            motivo=motivo
        )

        datasets_disponibilizados.append(registro)
        datasets_omitidos.append(registro)
        continue

    # -------------------------------------------------
    # NUEVO: Remediación de datos configurada por JSON
    # Se aplica antes de escribir en Silver base.
    # -------------------------------------------------
    df_dataset, resumen_remediacion_datos = aplicar_remediacion_configurada(
        df_dataset=df_dataset,
        config_remediacion=config_remediacion,
        nombre_caso=nombre_caso,
        tabla_relativa=tabla_relativa,
        lote_cargue_id=lote_cargue_id,
        fecha_proceso=fecha_proceso,
        fecha_particion=fecha_particion,
        ruta_destino_silver=ruta_destino
    )

    if resumen_remediacion_datos.get("total_valores_remediados", 0) > 0:
        datasets_remediados.append(resumen_remediacion_datos)
        total_valores_remediados_global += resumen_remediacion_datos.get("total_valores_remediados", 0)

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
        lote_cargue_id=lote_cargue_id,
        total_registros=total_registros,
        estado="SUCCESS",
        fecha_proceso=fecha_proceso,
        remediacion_datos=resumen_remediacion_datos
    )

    datasets_disponibilizados.append(registro)
    datasets_copiados.append(registro)

logger.info("Disponibilización controlada Bronze -> Silver base finalizada.")
logger.info(f"Datasets copiados correctamente : {len(datasets_copiados)}")
logger.info(f"Datasets omitidos/skipped       : {len(datasets_omitidos)}")
logger.info(f"Datasets con remediación datos  : {len(datasets_remediados)}")
logger.info(f"Total valores remediados        : {total_valores_remediados_global}")


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

remediacion_datos_resumen = {
    "estado": "SUCCESS",
    "tipo": "REMEDIACION_DATOS_CONFIGURADA",
    "config_habilitada": bool(config_remediacion.get("remediacion_habilitada", False)),
    "ruta_config_remediacion": ruta_config_remediacion,
    "total_datasets_remediados": len(datasets_remediados),
    "total_valores_remediados": total_valores_remediados_global,
    "datasets": datasets_remediados
}

parametros_extraccion["remediacion"] = {
    "estado": estado_remediacion_detalle,
    "tipo": "DISPONIBILIZACION_TRAZABILIDAD_Y_REMEDIACION_CONFIGURADA",
    "modo": "CONTINUA_FLUJO",
    "lote_cargue_id": lote_cargue_id,
    "estado_validacion": estado_validacion,
    "requiere_revision_analitica": requiere_revision_analitica,
    "fecha_remediacion": fecha_proceso,
    "remediacion_datos": remediacion_datos_resumen,
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

# Nuevos campos de control para remediación real de datos
parametros_extraccion["remediacion_configurada_habilitada"] = bool(config_remediacion.get("remediacion_habilitada", False))
parametros_extraccion["ruta_config_remediacion"] = ruta_config_remediacion
parametros_extraccion["remediacion_datos_aplicada"] = total_valores_remediados_global > 0
parametros_extraccion["total_valores_remediados"] = total_valores_remediados_global
parametros_extraccion["datasets_remediados"] = datasets_remediados

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
logger.info(f"datasets remediados             : {len(datasets_remediados)}")
logger.info(f"total valores remediados        : {total_valores_remediados_global}")
logger.info("=== FIN JOB REMEDIACION ESTANDAR NIIF ===")

job.commit()
