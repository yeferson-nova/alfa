import sys
import json
import logging
import boto3
from urllib.parse import urlparse
from datetime import datetime

from botocore.exceptions import ClientError

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import lit, monotonically_increasing_id

# -------------------------------------------------
# LOGGING
# -------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------------------------
# HELPERS S3 / JSON
# -------------------------------------------------
def ensure_trailing_slash(path: str) -> str:
    return path if path.endswith("/") else path + "/"

def parse_s3_uri(ruta_s3: str):
    parsed = urlparse(ruta_s3)
    return parsed.netloc, parsed.path.lstrip("/")

def existe_s3(ruta_s3: str) -> bool:
    bucket, key = parse_s3_uri(ruta_s3)
    s3 = boto3.client("s3")

    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ["404", "NoSuchKey", "NotFound"]:
            return False
        raise

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

# -------------------------------------------------
# HELPERS EXTRACCION
# -------------------------------------------------
def construir_dbtable(schema: str, tabla: str, usar_schema_en_dbtable: bool = True) -> str:
    """
    Construye el nombre de tabla para lectura Oracle.

    Si usar_schema_en_dbtable = True:
        ARPPROD.TABLA

    Si usar_schema_en_dbtable = False:
        TABLA

    Esto permite soportar conexiones Oracle donde el usuario ya tiene
    un schema por defecto o sinónimos configurados.
    """
    if usar_schema_en_dbtable:
        return f"{schema}.{tabla}"

    return tabla

def construir_query_where(tabla_cfg: dict, usar_schema_en_dbtable: bool = True) -> str:
    """
    Construye query Oracle cuando existe WHERE.
    No usa ORDER BY.
    """
    schema = tabla_cfg["schema"]
    tabla = tabla_cfg["tabla"]

    dbtable = construir_dbtable(
        schema=schema,
        tabla=tabla,
        usar_schema_en_dbtable=usar_schema_en_dbtable
    )

    where = tabla_cfg.get("where")

    query = f"SELECT * FROM {dbtable}"

    if where:
        query = f"{query} WHERE {where}"

    return query

def normalizar_tabla_relativa(tabla_cfg: dict) -> str:
    schema = tabla_cfg["schema"]
    tabla = tabla_cfg["tabla"]

    tabla_relativa = tabla_cfg.get(
        "tabla_relativa",
        f"{schema.lower()}/{tabla.lower()}"
    )

    return tabla_relativa.lower().strip("/")

# -------------------------------------------------
# CHECKPOINTS
# -------------------------------------------------
def crear_checkpoint_base():
    return {
        "estado": "IN_PROGRESS",
        "ciclo_actual": None,
        "segmento_actual": None,
        "tabla_actual": None,
        "ultimo_ciclo_ok": None,
        "ultimo_segmento_ok": None,
        "ultima_tabla_ok": None,
        "tablas_exitosas": [],
        "detalle": []
    }

def actualizar_checkpoint(
    parametros_extraccion: dict,
    estado: str,
    ciclo: dict = None,
    segmento: dict = None,
    tabla: dict = None,
    mensaje: str = None
):
    checkpoint = parametros_extraccion.get(
        "checkpoint_extraccion",
        crear_checkpoint_base()
    )

    checkpoint["estado"] = estado

    if ciclo:
        checkpoint["ciclo_actual"] = {
            "orden": ciclo.get("orden"),
            "nombre": ciclo.get("nombre")
        }

    if segmento:
        checkpoint["segmento_actual"] = {
            "orden": segmento.get("orden"),
            "nombre": segmento.get("nombre"),
            "forma": segmento.get("forma")
        }

    if tabla:
        checkpoint["tabla_actual"] = {
            "orden": tabla.get("orden"),
            "schema": tabla.get("schema"),
            "tabla": tabla.get("tabla"),
            "tabla_relativa": normalizar_tabla_relativa(tabla)
        }

    checkpoint.setdefault("detalle", []).append({
        "fecha": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "estado": estado,
        "mensaje": mensaje
    })

    parametros_extraccion["checkpoint_extraccion"] = checkpoint

def registrar_ok_tabla(parametros_extraccion: dict, tabla_cfg: dict):
    checkpoint = parametros_extraccion.get(
        "checkpoint_extraccion",
        crear_checkpoint_base()
    )

    tabla_relativa = normalizar_tabla_relativa(tabla_cfg)

    checkpoint["ultima_tabla_ok"] = {
        "orden": tabla_cfg.get("orden"),
        "schema": tabla_cfg.get("schema"),
        "tabla": tabla_cfg.get("tabla"),
        "tabla_relativa": tabla_relativa
    }

    tablas_exitosas = set(checkpoint.get("tablas_exitosas", []))
    tablas_exitosas.add(tabla_relativa)

    checkpoint["tablas_exitosas"] = sorted(list(tablas_exitosas))

    parametros_extraccion["checkpoint_extraccion"] = checkpoint

def registrar_ok_segmento(parametros_extraccion: dict, segmento: dict):
    checkpoint = parametros_extraccion.get(
        "checkpoint_extraccion",
        crear_checkpoint_base()
    )

    checkpoint["ultimo_segmento_ok"] = {
        "orden": segmento.get("orden"),
        "nombre": segmento.get("nombre"),
        "forma": segmento.get("forma")
    }

    parametros_extraccion["checkpoint_extraccion"] = checkpoint

def registrar_ok_ciclo(parametros_extraccion: dict, ciclo: dict):
    checkpoint = parametros_extraccion.get(
        "checkpoint_extraccion",
        crear_checkpoint_base()
    )

    checkpoint["ultimo_ciclo_ok"] = {
        "orden": ciclo.get("orden"),
        "nombre": ciclo.get("nombre")
    }

    parametros_extraccion["checkpoint_extraccion"] = checkpoint

def obtener_tablas_exitosas(parametros_extraccion: dict):
    tablas = set()

    for item in parametros_extraccion.get("datasets_extraidos", []):
        if item.get("estado") == "SUCCESS":
            if item.get("tabla_relativa"):
                tablas.add(item["tabla_relativa"].lower())

    checkpoint = parametros_extraccion.get("checkpoint_extraccion", {})

    for t in checkpoint.get("tablas_exitosas", []):
        tablas.add(t.lower())

    return tablas

# -------------------------------------------------
# PARAMETROS GLUE
# -------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RUTA_CONFIG_EXTRACCION"
    ]
)

ruta_config_extraccion = args["RUTA_CONFIG_EXTRACCION"]

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
# CARGA CONFIG EXTRACCION
# -------------------------------------------------
config = cargar_json_s3(ruta_config_extraccion)

nombre_caso = config["nombre_caso"]

fuente = config["fuente"]
connection_name = fuente["connection_name"]
usar_schema_en_dbtable = bool(fuente.get("usar_schema_en_dbtable", True))

rutas = config["rutas"]
s3_bronze_base = ensure_trailing_slash(rutas["s3_bronze_base"])
s3_silver_base = ensure_trailing_slash(rutas["s3_silver_base"])
ruta_parametros_extraccion = rutas["ruta_parametros_extraccion"]

control_extraccion = config.get("control_extraccion", {})
limite_unidades = control_extraccion.get("limite_unidades_extraccion")
calcular_unidades = control_extraccion.get("calcular_unidades_extraccion", True)
accion_si_supera_limite = control_extraccion.get("accion_si_supera_limite", "ADVERTIR").upper()
permitir_reanudacion = control_extraccion.get("permitir_reanudacion", True)
usar_checkpoints = control_extraccion.get("usar_checkpoints", True)

salida_runtime = config.get("salida_runtime", {})
estructura_dataset = salida_runtime.get("estructura_dataset", {})
checkpoint_cfg = salida_runtime.get("checkpoint_extraccion", {})

agregar_nro_registro = estructura_dataset.get("agregar_nro_registro", True)
agregar_lote_cargue_id = estructura_dataset.get("agregar_lote_cargue_id", True)
modo_escritura = estructura_dataset.get("modo_escritura", "overwrite").lower()
compresion = estructura_dataset.get("compresion", "snappy")

if modo_escritura not in ["overwrite", "append"]:
    raise ValueError(
        f"Modo de escritura no soportado: {modo_escritura}. "
        "Valores válidos: overwrite, append"
    )

saltar_tablas_exitosas = checkpoint_cfg.get(
    "saltar_tablas_exitosas_en_reanudacion",
    True
)

ciclos = sorted(
    config.get("ciclos", []),
    key=lambda x: x.get("orden", 0)
)

if not ciclos:
    raise ValueError("El JSON de extracción no contiene ciclos.")

if accion_si_supera_limite not in ["ADVERTIR", "FALLAR"]:
    raise ValueError(
        f"accion_si_supera_limite no soportada: {accion_si_supera_limite}. "
        "Valores válidos: ADVERTIR, FALLAR"
    )

# -------------------------------------------------
# FECHA / LOTE / REANUDACION
# -------------------------------------------------
now = datetime.now()
timestamp_ingesta = int(now.timestamp())
fecha_ejecucion = now.strftime("%Y-%m-%dT%H:%M:%S")

reanudar_ejecucion = False

if permitir_reanudacion and existe_s3(ruta_parametros_extraccion):
    parametros_existentes = cargar_json_s3(ruta_parametros_extraccion)
    estado_anterior = parametros_existentes.get("estado_extraccion")

    if estado_anterior in ["FAILED", "IN_PROGRESS"]:
        parametros_extraccion = parametros_existentes
        timestamp_ingesta = parametros_extraccion["lote_cargue_id"]
        reanudar_ejecucion = True

        parametros_extraccion["estado_extraccion"] = "IN_PROGRESS"
        parametros_extraccion["fecha_reanudacion"] = fecha_ejecucion

        logger.info(
            "Se detectó ejecución previa fallida/en progreso. "
            "Se reanudará el lote existente."
        )
    else:
        parametros_extraccion = None
else:
    parametros_extraccion = None

if parametros_extraccion is None:
    parametros_extraccion = {
        "execution_id": f"bronze_{nombre_caso}_{timestamp_ingesta}",
        "nombre_caso": nombre_caso,
        "lote_cargue_id": timestamp_ingesta,
        "fecha_ejecucion": fecha_ejecucion,
        "estado_extraccion": "IN_PROGRESS",
        "ruta_origen_base": s3_bronze_base,
        "ruta_destino_base": s3_silver_base,
        "ruta_config_extraccion": ruta_config_extraccion,
        "datasets_extraidos": [],
        "checkpoint_extraccion": crear_checkpoint_base(),
        "reanudacion": {
            "habilitada": permitir_reanudacion,
            "es_reanudacion": False
        }
    }
else:
    parametros_extraccion.setdefault("datasets_extraidos", [])
    parametros_extraccion.setdefault("checkpoint_extraccion", crear_checkpoint_base())

    parametros_extraccion["reanudacion"] = {
        "habilitada": permitir_reanudacion,
        "es_reanudacion": reanudar_ejecucion,
        "fecha_reanudacion": fecha_ejecucion if reanudar_ejecucion else None
    }

logger.info(f"Nombre caso                  : {nombre_caso}")
logger.info(f"Lote cargue id               : {timestamp_ingesta}")
logger.info(f"Ruta config extracción       : {ruta_config_extraccion}")
logger.info(f"Ruta Bronze base             : {s3_bronze_base}")
logger.info(f"Ruta Silver base             : {s3_silver_base}")
logger.info(f"Ruta parametros extracción   : {ruta_parametros_extraccion}")
logger.info(f"Connection name              : {connection_name}")
logger.info(f"Usar schema en dbtable       : {usar_schema_en_dbtable}")
logger.info(f"Modo escritura               : {modo_escritura}")
logger.info(f"Límite unidades extracción   : {limite_unidades}")
logger.info(f"Acción si supera límite      : {accion_si_supera_limite}")
logger.info(f"Permitir reanudación         : {permitir_reanudacion}")
logger.info(f"Usar checkpoints             : {usar_checkpoints}")
logger.info(f"Es reanudación               : {reanudar_ejecucion}")

guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

# -------------------------------------------------
# EXTRACCION POR CICLOS / SEGMENTOS / TABLAS
# -------------------------------------------------
try:
    tablas_exitosas = obtener_tablas_exitosas(parametros_extraccion)

    for ciclo in ciclos:
        logger.info("=================================================")
        logger.info(f"INICIANDO CICLO {ciclo.get('orden')} - {ciclo.get('nombre')}")
        logger.info("=================================================")

        fecha_inicio_ciclo = datetime.now()

        if usar_checkpoints:
            actualizar_checkpoint(
                parametros_extraccion,
                estado="CICLO_IN_PROGRESS",
                ciclo=ciclo,
                mensaje=f"Iniciando ciclo {ciclo.get('nombre')}"
            )
            guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

        segmentos = sorted(
            ciclo.get("segmentos", []),
            key=lambda x: x.get("orden", 0)
        )

        if not segmentos:
            raise ValueError(f"El ciclo {ciclo.get('nombre')} no contiene segmentos.")

        for segmento in segmentos:
            forma = segmento.get("forma", "CASCADA").upper()

            logger.info("---------------------------------------------")
            logger.info(f"INICIANDO SEGMENTO {segmento.get('orden')} - {segmento.get('nombre')}")
            logger.info(f"FORMA CONFIGURADA: {forma}")
            logger.info("---------------------------------------------")

            fecha_inicio_segmento = datetime.now()

            if forma != "CASCADA":
                logger.warning(
                    f"Forma {forma} configurada, pero esta versión ejecuta todo en CASCADA."
                )

            if usar_checkpoints:
                actualizar_checkpoint(
                    parametros_extraccion,
                    estado="SEGMENTO_IN_PROGRESS",
                    ciclo=ciclo,
                    segmento=segmento,
                    mensaje=f"Iniciando segmento {segmento.get('nombre')}"
                )
                guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

            tablas = sorted(
                segmento.get("tablas", []),
                key=lambda x: x.get("orden", 0)
            )

            if not tablas:
                raise ValueError(f"El segmento {segmento.get('nombre')} no contiene tablas.")

            for tabla_cfg in tablas:
                schema = tabla_cfg["schema"]
                tabla = tabla_cfg["tabla"]
                tabla_relativa = normalizar_tabla_relativa(tabla_cfg)

                segmentacion_horizontal = tabla_cfg.get("segmentacion_horizontal", {})
                dependencia = tabla_cfg.get("dependencia")

                if segmentacion_horizontal.get("habilitada", False):
                    logger.warning(
                        f"{tabla_relativa} tiene segmentación horizontal habilitada, "
                        "pero esta versión todavía no la ejecuta. Se extrae tabla completa."
                    )

                if dependencia:
                    logger.info(
                        f"{tabla_relativa} tiene dependencia documentada: {dependencia}. "
                        "Esta versión no filtra por dependencia; solo la registra como metadata."
                    )

                if (
                    reanudar_ejecucion
                    and saltar_tablas_exitosas
                    and tabla_relativa in tablas_exitosas
                ):
                    logger.info(
                        f"Saltando tabla ya extraída exitosamente por reanudación: {tabla_relativa}"
                    )
                    continue

                logger.info(f"===== INICIANDO EXTRACCION {schema}.{tabla} =====")

                fecha_inicio_tabla_dt = datetime.now()
                fecha_inicio_tabla = fecha_inicio_tabla_dt.strftime("%Y-%m-%dT%H:%M:%S")

                if usar_checkpoints:
                    actualizar_checkpoint(
                        parametros_extraccion,
                        estado="TABLA_IN_PROGRESS",
                        ciclo=ciclo,
                        segmento=segmento,
                        tabla=tabla_cfg,
                        mensaje=f"Extrayendo {schema}.{tabla}"
                    )
                    guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

                dbtable_name = construir_dbtable(
                    schema=schema,
                    tabla=tabla,
                    usar_schema_en_dbtable=usar_schema_en_dbtable
                )

                where = tabla_cfg.get("where")

                # -------------------------------------------------
                # EXTRACCION ORACLE
                # -------------------------------------------------
                if where:
                    query = construir_query_where(
                        tabla_cfg=tabla_cfg,
                        usar_schema_en_dbtable=usar_schema_en_dbtable
                    )

                    logger.info(f"Usando sampleQuery con WHERE para {schema}.{tabla}")
                    logger.info(f"dbtable usado: {dbtable_name}")
                    logger.info(f"Query: {query}")

                    dyf = glueContext.create_dynamic_frame.from_options(
                        connection_type="oracle",
                        connection_options={
                            "connectionName": connection_name,
                            "useConnectionProperties": "true",
                            "dbtable": dbtable_name,
                            "sampleQuery": query,
                            "oracle.net.CONNECT_TIMEOUT": "30000",
                            "oracle.jdbc.ReadTimeout": "30000"
                        }
                    )
                else:
                    logger.info(f"Usando dbtable directo para {schema}.{tabla}")
                    logger.info(f"dbtable usado: {dbtable_name}")

                    dyf = glueContext.create_dynamic_frame.from_options(
                        connection_type="oracle",
                        connection_options={
                            "connectionName": connection_name,
                            "useConnectionProperties": "true",
                            "dbtable": dbtable_name,
                            "oracle.net.CONNECT_TIMEOUT": "30000",
                            "oracle.jdbc.ReadTimeout": "30000"
                        }
                    )

                df = dyf.toDF()

                total_registros = df.count()
                total_columnas = len(df.columns)

                if calcular_unidades:
                    unidades_extraccion = total_registros * total_columnas
                else:
                    unidades_extraccion = None

                supera_limite_unidades = False

                if (
                    limite_unidades is not None
                    and unidades_extraccion is not None
                    and unidades_extraccion > int(limite_unidades)
                ):
                    supera_limite_unidades = True

                    mensaje_limite = (
                        f"{schema}.{tabla} supera límite de unidades "
                        f"({unidades_extraccion} > {limite_unidades})."
                    )

                    if accion_si_supera_limite == "FALLAR":
                        raise ValueError(
                            f"{mensaje_limite} accion_si_supera_limite=FALLAR"
                        )

                    logger.warning(
                        f"{mensaje_limite} accion_si_supera_limite=ADVERTIR. "
                        "La extracción continuará."
                    )

                logger.info(f"{schema}.{tabla}: registros leídos = {total_registros}")
                logger.info(f"{schema}.{tabla}: columnas = {total_columnas}")
                logger.info(f"{schema}.{tabla}: unidades extracción = {unidades_extraccion}")
                logger.info(f"{schema}.{tabla}: supera límite = {supera_limite_unidades}")

                df_brz = df

                if agregar_nro_registro:
                    df_brz = df_brz.withColumn(
                        "NRO_REGISTRO",
                        monotonically_increasing_id().cast("bigint")
                    )

                if agregar_lote_cargue_id:
                    df_brz = df_brz.withColumn(
                        "LOTE_CARGUE_ID",
                        lit(timestamp_ingesta).cast("bigint")
                    )

                target_path = f"{s3_bronze_base}{tabla_relativa}/"
                silver_target_path = f"{s3_silver_base}{tabla_relativa}/"

                logger.info(f"Escribiendo Bronze: {target_path}")

                (
                    df_brz.write
                    .mode(modo_escritura)
                    .option("compression", compresion)
                    .parquet(target_path)
                )

                fecha_fin_tabla_dt = datetime.now()
                fecha_fin_tabla = fecha_fin_tabla_dt.strftime("%Y-%m-%dT%H:%M:%S")
                duracion_segundos = int(
                    (fecha_fin_tabla_dt - fecha_inicio_tabla_dt).total_seconds()
                )

                registro_dataset = {
                    "estado": "SUCCESS",
                    "esquema": schema.lower(),
                    "tabla": tabla.lower(),
                    "tabla_relativa": tabla_relativa,
                    "ruta_dataset_origen": target_path,
                    "ruta_dataset_destino": silver_target_path,
                    "ciclo_orden": ciclo.get("orden"),
                    "ciclo_nombre": ciclo.get("nombre"),
                    "segmento_orden": segmento.get("orden"),
                    "segmento_nombre": segmento.get("nombre"),
                    "forma": forma,
                    "tabla_orden": tabla_cfg.get("orden"),
                    "tipo_tabla": tabla_cfg.get("tipo_tabla"),
                    "dependencia": dependencia,
                    "segmentacion_horizontal_habilitada": segmentacion_horizontal.get("habilitada", False),
                    "total_registros": total_registros,
                    "total_columnas": total_columnas,
                    "unidades_extraccion": unidades_extraccion,
                    "limite_unidades_extraccion": limite_unidades,
                    "supera_limite_unidades": supera_limite_unidades,
                    "accion_si_supera_limite": accion_si_supera_limite,
                    "fecha_inicio_extraccion": fecha_inicio_tabla,
                    "fecha_fin_extraccion": fecha_fin_tabla,
                    "duracion_segundos": duracion_segundos,
                    "dbtable_oracle": dbtable_name,
                    "usar_schema_en_dbtable": usar_schema_en_dbtable
                }

                parametros_extraccion["datasets_extraidos"].append(registro_dataset)

                if usar_checkpoints:
                    registrar_ok_tabla(parametros_extraccion, tabla_cfg)

                    actualizar_checkpoint(
                        parametros_extraccion,
                        estado="TABLA_SUCCESS",
                        ciclo=ciclo,
                        segmento=segmento,
                        tabla=tabla_cfg,
                        mensaje=f"OK {schema}.{tabla}"
                    )

                guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

                tablas_exitosas.add(tabla_relativa)

                logger.info(f"===== OK {schema}.{tabla} =====")

            fecha_fin_segmento = datetime.now()
            duracion_segmento = int(
                (fecha_fin_segmento - fecha_inicio_segmento).total_seconds()
            )

            registrar_ok_segmento(parametros_extraccion, segmento)

            if usar_checkpoints:
                actualizar_checkpoint(
                    parametros_extraccion,
                    estado="SEGMENTO_SUCCESS",
                    ciclo=ciclo,
                    segmento=segmento,
                    mensaje=(
                        f"OK segmento {segmento.get('nombre')} "
                        f"duracion_segundos={duracion_segmento}"
                    )
                )
                guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

            logger.info(f"===== OK SEGMENTO {segmento.get('nombre')} =====")

        fecha_fin_ciclo = datetime.now()
        duracion_ciclo = int(
            (fecha_fin_ciclo - fecha_inicio_ciclo).total_seconds()
        )

        registrar_ok_ciclo(parametros_extraccion, ciclo)

        if usar_checkpoints:
            actualizar_checkpoint(
                parametros_extraccion,
                estado="CICLO_SUCCESS",
                ciclo=ciclo,
                mensaje=(
                    f"OK ciclo {ciclo.get('nombre')} "
                    f"duracion_segundos={duracion_ciclo}"
                )
            )
            guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

        logger.info(f"===== OK CICLO {ciclo.get('nombre')} =====")

except Exception as e:
    logger.error("ERROR EN EXTRACCION ESTANDAR", exc_info=True)

    parametros_extraccion["estado_extraccion"] = "FAILED"
    parametros_extraccion["error_extraccion"] = str(e)
    parametros_extraccion["fecha_error_extraccion"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    if usar_checkpoints:
        actualizar_checkpoint(
            parametros_extraccion,
            estado="FAILED",
            mensaje=str(e)
        )

    guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

    job.commit()
    raise

# -------------------------------------------------
# CIERRE EXITOSO
# -------------------------------------------------
parametros_extraccion["estado_extraccion"] = "SUCCESS"
parametros_extraccion["fecha_fin_extraccion"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

if usar_checkpoints:
    actualizar_checkpoint(
        parametros_extraccion,
        estado="SUCCESS",
        mensaje="Extracción finalizada correctamente"
    )

guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

logger.info("=== FIN JOB EXTRACCION ESTANDAR NIIF ===")
logger.info(f"PARAMETROS_EXTRACCION guardado en: {ruta_parametros_extraccion}")

job.commit()