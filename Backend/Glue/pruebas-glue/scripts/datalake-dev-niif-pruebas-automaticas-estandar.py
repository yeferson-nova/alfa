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

from pyspark.sql.functions import col, lit, when

# -------------------------------------------------
# LOGGING
# -------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------------------------
# HELPERS S3 / JSON
# -------------------------------------------------
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

def parse_s3_uri(ruta_s3: str):
    parsed = urlparse(ruta_s3)
    return parsed.netloc, parsed.path.lstrip("/")

def listar_objetos_s3(bucket: str, prefix: str):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    keys = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return keys

def borrar_prefijo_s3(bucket: str, prefix: str):
    s3 = boto3.client("s3")
    keys = listar_objetos_s3(bucket, prefix)

    if not keys:
        logger.info(f"No hay objetos para borrar en s3://{bucket}/{prefix}")
        return

    # borrar en bloques de 1000
    for i in range(0, len(keys), 1000):
        bloque = keys[i:i+1000]
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in bloque]}
        )

    logger.info(f"Objetos borrados en s3://{bucket}/{prefix}: {len(keys)}")

def copiar_prefijo_s3(bucket_src: str, prefix_src: str, bucket_dst: str, prefix_dst: str):
    s3 = boto3.client("s3")
    keys = listar_objetos_s3(bucket_src, prefix_src)

    if not keys:
        raise ValueError(f"No hay objetos en la ruta temporal s3://{bucket_src}/{prefix_src}")

    for k in keys:
        # conservar el sufijo relativo
        sufijo = k[len(prefix_src):]
        destino = prefix_dst + sufijo

        s3.copy_object(
            Bucket=bucket_dst,
            CopySource={"Bucket": bucket_src, "Key": k},
            Key=destino
        )

    logger.info(f"Objetos copiados de s3://{bucket_src}/{prefix_src} a s3://{bucket_dst}/{prefix_dst}: {len(keys)}")

def resolver_columna(df, nombre_buscado: str) -> str:
    mapa = {c.lower(): c for c in df.columns}
    clave = nombre_buscado.lower()
    if clave not in mapa:
        raise ValueError(
            f"No existe la columna '{nombre_buscado}' en el DataFrame. Columnas: {df.columns}"
        )
    return mapa[clave]

# -------------------------------------------------
# PARAMETROS
# -------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RUTA_PARAMETROS_EXTRACCION",
        "RUTA_JSON_PRUEBAS"
    ]
)

ruta_parametros_extraccion = args["RUTA_PARAMETROS_EXTRACCION"]
ruta_json_pruebas = args["RUTA_JSON_PRUEBAS"]

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
# CARGA RUNTIME Y JSON DE PRUEBAS
# -------------------------------------------------
parametros_extraccion = cargar_json_s3(ruta_parametros_extraccion)
qa_cfg = cargar_json_s3(ruta_json_pruebas)

lote_actual = parametros_extraccion.get("lote_cargue_id")
datasets_extraidos = parametros_extraccion.get("datasets_extraidos", [])

if lote_actual is None:
    raise ValueError("parametros-extraccion.json no contiene lote_cargue_id.")

if not datasets_extraidos:
    raise ValueError("parametros-extraccion.json no contiene datasets_extraidos.")

tabla_objetivo = qa_cfg["tabla_objetivo"].lower()
llave_objetivo = qa_cfg["llave"].lower()
registros = qa_cfg.get("registros", [])

if not registros:
    raise ValueError("El JSON de pruebas no contiene 'registros'.")

mapa_datasets = {
    d["tabla_relativa"].lower(): d
    for d in datasets_extraidos
}

# Buscar tabla objetivo dentro de datasets_extraidos
candidatos = [
    k for k in mapa_datasets.keys()
    if k.endswith(f"/{tabla_objetivo}") or k == tabla_objetivo
]

if not candidatos:
    raise ValueError(
        f"No se encontró la tabla objetivo '{tabla_objetivo}' dentro de parametros-extraccion.json"
    )

tabla_relativa_objetivo = candidatos[0]
ruta_tabla_objetivo = mapa_datasets[tabla_relativa_objetivo]["ruta_dataset_origen"]

logger.info(f"Lote actual                : {lote_actual}")
logger.info(f"Tabla objetivo             : {tabla_objetivo}")
logger.info(f"Llave objetivo             : {llave_objetivo}")
logger.info(f"Ruta tabla objetivo        : {ruta_tabla_objetivo}")
logger.info(f"Registros a modificar      : {len(registros)}")
logger.info(f"Ruta parametros extraccion : {ruta_parametros_extraccion}")
logger.info(f"Ruta json pruebas          : {ruta_json_pruebas}")

# -------------------------------------------------
# LEER TABLA COMPLETA Y SEPARAR LOTE ACTUAL
# -------------------------------------------------
df_original = spark.read.parquet(ruta_tabla_objetivo)

col_lote = resolver_columna(df_original, "lote_cargue_id")
col_llave = resolver_columna(df_original, llave_objetivo)

mapa_cols = {c.lower(): c for c in df_original.columns}

df_otros_lotes = df_original.filter(col(col_lote) != lit(lote_actual))
df_lote_actual = df_original.filter(col(col_lote) == lit(lote_actual))

if df_lote_actual.count() == 0:
    raise ValueError(
        f"La tabla {tabla_objetivo} no tiene registros para el lote actual {lote_actual}"
    )

# -------------------------------------------------
# UPDATE PARCIAL SOBRE EL LOTE ACTUAL
# Solo cambia columnas presentes en el JSON.
# Si una columna viene con null en el JSON, se actualiza a null.
# -------------------------------------------------
df_mod = df_lote_actual

for reg in registros:
    mapa_reg = {k.lower(): v for k, v in reg.items()}

    if llave_objetivo not in mapa_reg:
        raise ValueError(
            f"Uno de los registros del JSON no contiene la llave '{llave_objetivo}'"
        )

    valor_llave = mapa_reg[llave_objetivo]
    condicion = col(col_llave) == lit(valor_llave)

    existe = df_mod.filter(condicion).limit(1).count() > 0
    if not existe:
        logger.warning(
            f"No se encontró registro con {llave_objetivo}={valor_llave} en el lote actual. Se omite."
        )
        continue

    for campo_json, valor_nuevo in mapa_reg.items():
        if campo_json == llave_objetivo:
            continue

        if campo_json not in mapa_cols:
            logger.warning(
                f"La columna '{campo_json}' no existe en la tabla {tabla_objetivo}. Se omite."
            )
            continue

        nombre_real = mapa_cols[campo_json]
        tipo_destino = df_original.schema[nombre_real].dataType

        df_mod = df_mod.withColumn(
            nombre_real,
            when(condicion, lit(valor_nuevo).cast(tipo_destino)).otherwise(col(nombre_real))
        )

# -------------------------------------------------
# RECONSTRUIR TABLA COMPLETA
# -------------------------------------------------
df_final = df_otros_lotes.unionByName(df_mod, allowMissingColumns=True)

# -------------------------------------------------
# ESCRITURA SEGURA CON RUTA TEMPORAL
# -------------------------------------------------
ruta_original = ruta_tabla_objetivo
ruta_tmp = ruta_original.rstrip("/") + "_tmp_qa/"

bucket_original, prefix_original = parse_s3_uri(ruta_original)
bucket_tmp, prefix_tmp = parse_s3_uri(ruta_tmp)

logger.info(f"Escribiendo tabla temporal: {ruta_tmp}")

(
    df_final.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(ruta_tmp)
)

logger.info("Reemplazando ruta original con contenido temporal...")

# 1) borrar original
borrar_prefijo_s3(bucket_original, prefix_original)

# 2) copiar temporal a original
copiar_prefijo_s3(bucket_tmp, prefix_tmp, bucket_original, prefix_original)

# 3) borrar temporal
borrar_prefijo_s3(bucket_tmp, prefix_tmp)

logger.info(f"Ruta original actualizada: {ruta_original}")

# -------------------------------------------------
# ACTUALIZAR PARAMETROS_EXTRACCION
# -------------------------------------------------
parametros_extraccion["estado_extraccion"] = "SUCCESS"
parametros_extraccion["qa_mutacion_aplicada"] = True
parametros_extraccion["qa_mutacion_fecha"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
parametros_extraccion["qa_mutacion_tabla_objetivo"] = tabla_relativa_objetivo
parametros_extraccion["qa_mutacion_llave"] = llave_objetivo
parametros_extraccion["qa_mutacion_total_registros"] = len(registros)

guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)

logger.info("=== FIN JOB INTERMEDIO QA POST-BRONZE ===")
logger.info(f"Se modificó el lote actual: {lote_actual}")
logger.info(f"Tabla modificada: {tabla_relativa_objetivo}")
logger.info(f"parametros-extraccion actualizado en: {ruta_parametros_extraccion}")

job.commit()