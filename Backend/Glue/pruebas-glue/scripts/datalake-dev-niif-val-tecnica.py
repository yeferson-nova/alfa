import sys
import json
import logging
import boto3
from urllib.parse import urlparse
from py4j.protocol import Py4JJavaError
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    length,
    when,
    regexp_replace,
    regexp_extract,
    current_timestamp,
    to_date
)

from pyspark.sql.types import (
    LongType,
    StringType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    ShortType,
    ByteType
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


def limpiar_prefijo_s3(ruta_s3: str):
    parsed = urlparse(ensure_trailing_slash(ruta_s3))
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    objetos = []

    for pagina in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in pagina.get("Contents", []):
            objetos.append({"Key": obj["Key"]})

            if len(objetos) == 1000:
                s3.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": objetos}
                )
                objetos = []

    if objetos:
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": objetos}
        )


def resolver_columna_opcional(df, nombre_columna: str):
    mapa = {c.lower(): c for c in df.columns}
    return mapa.get(nombre_columna.lower())


def nombre_real_columna(df, nombre_columna: str) -> str:
    mapa = {c.lower(): c for c in df.columns}
    clave = nombre_columna.lower()

    if clave not in mapa:
        raise ValueError(
            f"Columna '{nombre_columna}' no existe. Columnas disponibles: {df.columns}"
        )

    return mapa[clave]


def es_tipo_numerico(data_type):
    return isinstance(
        data_type,
        (
            DecimalType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            ShortType,
            ByteType
        )
    )


# -------------------------------------------------
# PARAMETROS GLUE
# -------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RUTA_CONFIG_VALIDACION"
    ]
)

ruta_config = args["RUTA_CONFIG_VALIDACION"]

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
config = cargar_json_s3(ruta_config)

nombre_caso = config.get("nombre_caso", "sin_nombre")
datasets_cfg = config.get("datasets", [])
ruta_salida = ensure_trailing_slash(config["salida"]["ruta_base"])
ruta_parametros_extraccion = config["parametros_extraccion"]["ruta"]

if not datasets_cfg:
    raise ValueError("El JSON no contiene datasets para validar.")

# -------------------------------------------------
# CARGA PARAMETROS_EXTRACCION
# -------------------------------------------------
parametros_extraccion = cargar_json_s3(ruta_parametros_extraccion)

if parametros_extraccion.get("estado_extraccion") != "SUCCESS":
    raise ValueError(
        f"El JSON de parametros_extraccion no está en SUCCESS: {ruta_parametros_extraccion}"
    )

lote_cargue_id = parametros_extraccion.get("lote_cargue_id")
datasets_extraidos = parametros_extraccion.get("datasets_extraidos", [])

if lote_cargue_id is None:
    raise ValueError("El JSON de parametros_extraccion no contiene lote_cargue_id.")

if not datasets_extraidos:
    raise ValueError("El JSON de parametros_extraccion no contiene datasets_extraidos.")

mapa_parametros = {
    d["tabla_relativa"]: d
    for d in datasets_extraidos
}

logger.info(f"Nombre caso                : {nombre_caso}")
logger.info(f"Lote a validar             : {lote_cargue_id}")
logger.info(f"Cantidad datasets cfg      : {len(datasets_cfg)}")
logger.info(f"Ruta salida errores        : {ruta_salida}")
logger.info(f"Ruta parametros extraccion : {ruta_parametros_extraccion}")

# -------------------------------------------------
# CONDICIONES DE FALLO
# -------------------------------------------------
def condicion_fallo_tipo(nombre_columna: str, tipo: str, data_type):
    tipo_norm = (tipo or "").upper()

    if tipo_norm == "STRING":
        return lit(False)

    if tipo_norm == "INTEGER":
        if es_tipo_numerico(data_type):
            return lit(False)

        return (
            col(nombre_columna).isNotNull()
            & (~trim(col(nombre_columna).cast("string")).rlike(r"^-?\d+$"))
        )

    if tipo_norm == "DECIMAL":
        if es_tipo_numerico(data_type):
            return lit(False)

        return (
            col(nombre_columna).isNotNull()
            & (~trim(col(nombre_columna).cast("string")).rlike(r"^-?\d+(\.\d+)?$"))
        )

    if tipo_norm == "DATE":
        return (
            col(nombre_columna).isNotNull()
            & to_date(col(nombre_columna)).isNull()
        )

    raise ValueError(f"Tipo no soportado: {tipo}")


def condicion_fallo_precision_escala(nombre_columna: str, tipo: str, precision, escala, data_type):
    if (tipo or "").upper() != "DECIMAL":
        return lit(False)

    if precision is None and escala is None:
        return lit(False)

    if precision is None:
        return lit(False)

    if escala is None:
        escala = 0

    # Si Spark ya leyó la columna como numérica, evitamos validar con regex sobre string,
    # porque Spark puede mostrar números grandes en notación científica: 1.03013E7.
    if es_tipo_numerico(data_type):
        tipo_decimal = DecimalType(int(precision), int(escala))

        return (
            col(nombre_columna).isNotNull()
            & col(nombre_columna).cast(tipo_decimal).isNull()
        )

    valor_str = trim(col(nombre_columna).cast("string"))
    es_decimal = valor_str.rlike(r"^-?\d+(\.\d+)?$")

    valor_abs = regexp_replace(valor_str, r"^-", "")
    parte_entera = regexp_extract(valor_abs, r"^(\d+)", 1)
    parte_decimal = regexp_extract(valor_abs, r"^\d+\.(\d+)$", 1)

    longitud_entera = length(parte_entera)
    longitud_decimal = length(parte_decimal)
    total_digitos = longitud_entera + longitud_decimal

    condicion = es_decimal

    if precision is not None:
        condicion = condicion & (total_digitos <= lit(int(precision)))

    if escala is not None:
        condicion = condicion & (longitud_decimal <= lit(int(escala)))

    return col(nombre_columna).isNotNull() & (~condicion)


# -------------------------------------------------
# CREAR DETALLE DE FALLOS
# -------------------------------------------------
def crear_detalle_fallos(
    df,
    nombre_caso,
    tabla,
    columna,
    lote_cargue_id,
    regla,
    condicion_fallo,
    valor_esperado,
    motivo_fallo
):
    nro_registro_col = resolver_columna_opcional(df, "NRO_REGISTRO")

    if nro_registro_col:
        num_fila_expr = col(nro_registro_col).cast(StringType()).alias("num_fila")
    else:
        num_fila_expr = lit(None).cast(StringType()).alias("num_fila")

    columna_real = nombre_real_columna(df, columna)

    return (
        df
        .filter(condicion_fallo)
        .select(
            lit(nombre_caso).alias("nombre_caso"),
            lit(tabla).alias("tabla"),
            lit(columna).alias("columna"),
            lit(regla).alias("regla"),
            num_fila_expr,
            when(
                col(columna_real).isNull(),
                lit("<NULL>")
            ).otherwise(
                col(columna_real).cast(StringType())
            ).alias("valor_incorrecto"),
            lit(valor_esperado).alias("valor_esperado"),
            lit(motivo_fallo).alias("motivo_fallo"),
            lit(lote_cargue_id).cast(LongType()).alias("lote_cargue_id"),
            current_timestamp().alias("fecha_validacion")
        )
    )

def crear_error_sin_filas(
    nombre_caso,
    tabla,
    columna,
    lote_cargue_id):
    schema = StructType([
        StructField("nombre_caso", StringType(), True),
        StructField("tabla", StringType(), True),
        StructField("columna", StringType(), True),
        StructField("regla", StringType(), True),
        StructField("num_fila", StringType(), True),
        StructField("valor_incorrecto", StringType(), True),
        StructField("valor_esperado", StringType(), True),
        StructField("motivo_fallo", StringType(), True),
        StructField("lote_cargue_id", LongType(), True)
    ])

    data = [(
        nombre_caso,
        tabla,
        columna,
        "SIN_FILAS",
        None,
        "<SIN_FILAS>",
        "Dataset con filas para validar",
        "No existen filas para el lote indicado",
        int(lote_cargue_id)
    )]

    df = spark.createDataFrame(data, schema)

    return df.withColumn("fecha_validacion", current_timestamp())


# -------------------------------------------------
# VALIDACION MULTI-DATASET
# -------------------------------------------------
resultados_fallos = []

for dataset_cfg in datasets_cfg:
    tabla_relativa = dataset_cfg["tabla_relativa"]
    reglas = dataset_cfg.get("reglas", [])

    if tabla_relativa not in mapa_parametros:
        raise ValueError(f"La tabla '{tabla_relativa}' no aparece en parametros_extraccion.")

    if not reglas:
        logger.warning(f"Tabla sin reglas: {tabla_relativa}")
        continue

    ruta_dataset = ensure_trailing_slash(mapa_parametros[tabla_relativa]["ruta_dataset_origen"])

    logger.info(f"Validando tabla : {tabla_relativa}")
    logger.info(f"Ruta dataset    : {ruta_dataset}")

    # ---------------------------------------------
    # VALIDAR SI EL DATASET TIENE ARCHIVOS
    # ---------------------------------------------
    from py4j.protocol import Py4JJavaError
    
    try:
        df = spark.read.parquet(ruta_dataset)
    except Exception as e:
        logger.warning(f"No se pudo leer dataset (posiblemente vacío): {ruta_dataset}")
        
        for regla_cfg in reglas:
            resultados_fallos.append(
                crear_error_sin_filas(
                    nombre_caso,
                    tabla_relativa,
                    regla_cfg["columna"],
                    lote_cargue_id
                )
            )
        continue
    
    
    # ---------------------------------------------
    # FILTRO POR LOTE
    # ---------------------------------------------
    if "lote_cargue_id" not in [c.lower() for c in df.columns]:
        logger.warning(f"{tabla_relativa} no tiene LOTE_CARGUE_ID. Se omite.")
        continue
    
    lote_col = nombre_real_columna(df, "LOTE_CARGUE_ID")
    
    df = df.filter(col(lote_col) == lit(lote_cargue_id)).cache()
    
    total_filas = df.count()
    logger.info(f"Filas leidas lote: {total_filas}")
    
    
    # ---------------------------------------------
    # SI NO HAY DATOS (Fase 3)
    # ---------------------------------------------
    if total_filas == 0:
        logger.warning(f"{tabla_relativa} no tiene datos para el lote {lote_cargue_id}")
    
        for regla_cfg in reglas:
            resultados_fallos.append(
                crear_error_sin_filas(
                    nombre_caso,
                    tabla_relativa,
                    regla_cfg["columna"],
                    lote_cargue_id
                )
            )
    
        df.unpersist()
        continue
    
    
    # ---------------------------------------------
    # VALIDACIONES NORMALES
    # ---------------------------------------------
    columnas_df_lower = {c.lower() for c in df.columns}
    
    for regla_cfg in reglas:
        columna_cfg = regla_cfg["columna"]
        tipo = regla_cfg.get("tipo", "STRING")
        permite_nulo = regla_cfg.get("nulo", True)
        longitud_maxima = regla_cfg.get("longitud_maxima")
        precision = regla_cfg.get("precision")
        escala = regla_cfg.get("escala")
    
        if columna_cfg.lower() not in columnas_df_lower:
            raise ValueError(
                f"Columna '{columna_cfg}' no existe en dataset {ruta_dataset}"
            )
    
        columna_real = nombre_real_columna(df, columna_cfg)
        data_type_columna = df.schema[columna_real].dataType
    
        # ----------------
        # NULO
        # ----------------
        if not permite_nulo:
            resultados_fallos.append(
                crear_detalle_fallos(
                    df=df,
                    nombre_caso=nombre_caso,
                    tabla=tabla_relativa,
                    columna=columna_cfg,
                    lote_cargue_id=lote_cargue_id,
                    regla="NULO",
                    condicion_fallo=col(columna_real).isNull(),
                    valor_esperado="NOT NULL",
                    motivo_fallo="La columna no debe ser nula"
                )
            )
    
        # ----------------
        # TIPO
        # ----------------
        resultados_fallos.append(
            crear_detalle_fallos(
                df=df,
                nombre_caso=nombre_caso,
                tabla=tabla_relativa,
                columna=columna_cfg,
                lote_cargue_id=lote_cargue_id,
                regla="TIPO",
                condicion_fallo=condicion_fallo_tipo(
                    columna_real,
                    tipo,
                    data_type_columna
                ),
                valor_esperado=tipo,
                motivo_fallo=f"El valor no cumple el tipo esperado {tipo}"
            )
        )
    
        # ----------------
        # LONGITUD
        # ----------------
        if longitud_maxima is not None:
            resultados_fallos.append(
                crear_detalle_fallos(
                    df=df,
                    nombre_caso=nombre_caso,
                    tabla=tabla_relativa,
                    columna=columna_cfg,
                    lote_cargue_id=lote_cargue_id,
                    regla="LONGITUD",
                    condicion_fallo=(
                        col(columna_real).isNotNull()
                        & (length(trim(col(columna_real).cast("string"))) > lit(int(longitud_maxima)))
                    ),
                    valor_esperado=f"Longitud <= {longitud_maxima}",
                    motivo_fallo="La longitud excede el máximo permitido"
                )
            )
    
        # ----------------
        # PRECISION / ESCALA
        # ----------------
        if (tipo or "").upper() == "DECIMAL" and (precision is not None or escala is not None):
            resultados_fallos.append(
                crear_detalle_fallos(
                    df=df,
                    nombre_caso=nombre_caso,
                    tabla=tabla_relativa,
                    columna=columna_cfg,
                    lote_cargue_id=lote_cargue_id,
                    regla="PRECISION_ESCALA",
                    condicion_fallo=condicion_fallo_precision_escala(
                        columna_real,
                        tipo,
                        precision,
                        escala,
                        data_type_columna
                    ),
                    valor_esperado=f"DECIMAL({precision},{escala})",
                    motivo_fallo="El valor no cumple precisión/escala decimal"
                )
            )
    
    df.unpersist()
# -------------------------------------------------
# UNION FINAL DE FALLOS
# -------------------------------------------------
if resultados_fallos:
    df_final = resultados_fallos[0]

    for d in resultados_fallos[1:]:
        df_final = df_final.unionByName(d)

    total_fallos = df_final.count()
else:
    df_final = None
    total_fallos = 0

logger.info(f"Total registros fallidos encontrados: {total_fallos}")

# -------------------------------------------------
# SI HAY FALLOS
# Validación técnica NO detiene el flujo.
# Deja errores para remediación y termina Glue como SUCCESS técnico.
# -------------------------------------------------
if total_fallos > 0:
    logger.info(f"Escribiendo errores actuales de validación en: {ruta_salida}")

    (
        df_final
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(ruta_salida)
    )

    parametros_extraccion["estado_validacion"] = "FAILED"
    parametros_extraccion["ruta_validacion_tecnica"] = ruta_salida
    parametros_extraccion["total_errores_validacion"] = total_fallos
    parametros_extraccion["validacion_tecnica_detiene_flujo"] = False
    parametros_extraccion["estado_hallazgos_validacion"] = "CON_HALLAZGOS"
    parametros_extraccion["requiere_revision_analitica"] = True
    parametros_extraccion["modo_flujo_post_validacion"] = "CONTINUA_A_REMEDIACION_SILVER_Y_GOLD"

    guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

    logger.warning("Validación técnica encontró hallazgos.")
    logger.warning("El flujo continuará hacia remediación.")

# -------------------------------------------------
# SI NO HAY FALLOS
# Se limpia la ruta de errores para no dejar errores viejos.
# -------------------------------------------------
else:
    logger.info(f"No hay errores. Limpiando ruta de validación técnica: {ruta_salida}")

    limpiar_prefijo_s3(ruta_salida)

    parametros_extraccion["estado_validacion"] = "SUCCESS"
    parametros_extraccion["total_errores_validacion"] = 0
    parametros_extraccion["ruta_validacion_tecnica"] = None
    parametros_extraccion["validacion_tecnica_detiene_flujo"] = False
    parametros_extraccion["estado_hallazgos_validacion"] = "SIN_HALLAZGOS"
    parametros_extraccion["requiere_revision_analitica"] = False
    parametros_extraccion["modo_flujo_post_validacion"] = "CONTINUA_A_SILVER_Y_GOLD"

    guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

    logger.info("Validación técnica: SUCCESS")
    logger.info("No se generó tabla de errores.")
    logger.info("Ruta de errores limpiada correctamente.")

logger.info("=== FIN VALIDACION TECNICA ESTANDAR NIIF ===")
job.commit()