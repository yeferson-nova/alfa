import sys
import json
import boto3
from urllib.parse import urlparse

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col


# =============================
# INIT
# =============================
args = getResolvedOptions(sys.argv, ["JOB_NAME", "PARAMETROS_JSON"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3 = boto3.client("s3")


# =============================
# UTILS
# =============================
def parse_s3(path):
    p = urlparse(path)
    return p.netloc, p.path.lstrip("/")


def leer_json(ruta):
    bucket, key = parse_s3(ruta)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def normalizar(df):
    return df.toDF(*[c.lower() for c in df.columns])


def json_to_df(lista):
    if not lista:
        return None

    json_lines = [json.dumps(x, ensure_ascii=False) for x in lista]
    df = spark.read.json(sc.parallelize(json_lines))
    return normalizar(df)


# =============================
# MAIN
# =============================
def ejecutar(cfg):

    ruta = cfg["ruta_bronze"].rstrip("/") + "/"

    print("PRIMAS ARL QA")
    print(f"Ruta: {ruta}")

    # 1. Leer Bronze
    df_bronze = spark.read.parquet(ruta)
    df_bronze = normalizar(df_bronze)

    # FIX: materializar para evitar error de overwrite
    df_bronze = df_bronze.cache()
    df_bronze.count()

    print(f"Registros Bronze: {df_bronze.count()}")

    updates, inserts, deletes = [], [], []

    # 2. Separar mutaciones
    for m in cfg["mutaciones"]:
        if m["accion"] == "UPDATE":
            updates.append(m["registro"])
        elif m["accion"] == "INSERT":
            inserts.append(m["registro"])
        elif m["accion"] == "DELETE":
            deletes.append(m["llave"])

    # 3. Construir DataFrames
    df_update = json_to_df(updates)
    df_insert = json_to_df(inserts)
    df_delete = json_to_df(deletes)

    # 4. UPSERT
    df_upsert = None
    if df_update is not None and df_insert is not None:
        df_upsert = df_update.unionByName(df_insert)
    elif df_update is not None:
        df_upsert = df_update
    elif df_insert is not None:
        df_upsert = df_insert

    # 5. Llaves
    df_keys = None
    if df_update is not None and df_delete is not None:
        df_keys = df_update.select("nro_registro").unionByName(df_delete)
    elif df_update is not None:
        df_keys = df_update.select("nro_registro")
    elif df_delete is not None:
        df_keys = df_delete

    # 6. Eliminar
    if df_keys is not None:
        print("Eliminando registros existentes...")
        df_final = df_bronze.join(df_keys, on="nro_registro", how="left_anti")
    else:
        df_final = df_bronze

    # 7. Insertar
    if df_upsert is not None:
        print("Insertando registros...")
        df_final = df_final.unionByName(df_upsert)

    print(f"Registros finales: {df_final.count()}")

    # FIX: materializar antes de overwrite
    df_final = df_final.cache()
    df_final.count()

    # 8. Overwrite directo
    print("Sobrescribiendo Bronze directamente...")

    df_final.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(ruta)

    print("Proceso completado correctamente")


# =============================
# MAIN
# =============================
def main():
    cfg = leer_json(args["PARAMETROS_JSON"])
    ejecutar(cfg)
    job.commit()


main()