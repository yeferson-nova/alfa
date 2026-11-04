
import os
import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F, types as T

# ========= CONFIGURACIÓN =========
# Región donde está tu Glue Data Catalog (ajústala)
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Dónde quieres escribir el inventario:
# Ejemplo: s3://mi-bucket-admin/reportes/buckets_y_tablas/
OUTPUT_S3 = os.getenv("OUTPUT_S3", "s3://alfa-bucket-dev-wi/inventarios/buckets_y_tablas/")

# ¿Quieres incluir buckets que NO tengan tablas del Catalog?
INCLUDE_EMPTY_BUCKETS = True  # True = incluye fila "sin tablas" para esos buckets

# ========== UTILIDADES ==========
def get_bucket_from_s3_path(s3_path: str) -> str:
    try:
        parsed = urlparse(s3_path)
        return parsed.netloc or ""
    except Exception:
        return ""

def list_all_buckets(s3_client):
    """
    Retorna lista de nombres de buckets S3 (global por cuenta).
    """
    try:
        resp = s3_client.list_buckets()
        return [b["Name"] for b in resp.get("Buckets", [])]
    except ClientError as e:
        print("❌ Error al listar buckets S3 (s3:ListAllMyBuckets requerido).")
        print(e)
        return []

def list_all_tables(glue_client):
    """
    Recorre TODO el Glue Data Catalog en AWS_REGION y devuelve lista de
    dicts con: table_schema, table_name, location, bucket (si aplica).
    Usa paginación para DB y Tablas.
    """
    rows = []

    # Paginador de bases
    db_pag = glue_client.get_paginator("get_databases")
    for db_page in db_pag.paginate():
        for db in db_page.get("DatabaseList", []):
            db_name = db["Name"]

            # Paginador de tablas por DB
            try:
                tbl_pag = glue_client.get_paginator("get_tables")
                for tbl_page in tbl_pag.paginate(DatabaseName=db_name):
                    for t in tbl_page.get("TableList", []):
                        tname = t["Name"]
                        storage_desc = t.get("StorageDescriptor", {}) or {}
                        location = storage_desc.get("Location", "") or ""
                        bucket = ""
                        if location.startswith("s3://"):
                            bucket = get_bucket_from_s3_path(location)
                        rows.append({
                            "table_schema": db_name,
                            "table_name": tname,
                            "location": location if location else "Sin ubicación/No aplica",
                            "bucket": f"s3://{bucket}/" if bucket else "No aplica/No disponible"
                        })
            except ClientError as e:
                # Puede fallar por permisos/LF; seguimos con otras DB
                print(f"⚠️ No se pudieron listar tablas de DB '{db_name}': {e}")
                continue

    return rows

def main():
    # Inicializa Spark/Glue
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    print("▶️ Preparando inventario Buckets ↔ Tablas (Glue Catalog)")
    print(f"   Región Catalog: {AWS_REGION}")
    print(f"   Salida S3:      {OUTPUT_S3}")
    print(f"   Incluir buckets sin tablas: {INCLUDE_EMPTY_BUCKETS}")

    s3 = boto3.client("s3")  # buckets son globales
    glue = boto3.client("glue", region_name=AWS_REGION)

    # 1) Buckets de la cuenta
    all_buckets = list_all_buckets(s3)
    print(f"📦 Buckets S3 encontrados: {len(all_buckets)}")

    # 2) Tablas del Catalog (en la región)
    table_rows = list_all_tables(glue)
    print(f"🗂️  Tablas del Catalog encontradas: {len(table_rows)}")

    # 3) DataFrame de tablas (bucket puede ser "No aplica/No disponible")
    df_tables = spark.createDataFrame(table_rows) if table_rows else spark.createDataFrame(
        [{"table_schema": "—", "table_name": "—", "location": "—", "bucket": "No aplica/No disponible"}],
        schema=T.StructType([
            T.StructField("table_schema", T.StringType(), True),
            T.StructField("table_name", T.StringType(), True),
            T.StructField("location", T.StringType(), True),
            T.StructField("bucket", T.StringType(), True),
        ])
    )

    # 4) Normalizamos bucket a solo nombre (para cruzar contra S3) y volvemos a mostrar como s3://.../ en salida
    to_bucket_name = F.udf(lambda s: s.replace("s3://", "").strip("/"), T.StringType())

    df_tables_norm = df_tables.withColumn(
        "bucket_name",
        F.when(F.col("bucket").startswith("s3://"), to_bucket_name(F.col("bucket"))).otherwise(F.lit(""))
    )

    # 5) Armamos DF con buckets S3 (uno por fila)
    df_buckets = spark.createDataFrame([{"bucket_name": b} for b in all_buckets])

    # 6) Left join: todos los buckets y al lado las tablas que apuntan a ese bucket
    df_join = (
        df_buckets.join(df_tables_norm, on="bucket_name", how="left")
        .withColumn(
            "bucket",
            F.when(F.col("bucket_name") != "", F.concat(F.lit("s3://"), F.col("bucket_name"), F.lit("/")))
             .otherwise(F.lit("s3://(desconocido)/"))
        )
        .drop("bucket_name")
    )

    # 7) Si NO quieres buckets sin tablas, filtramos nulos
    if not INCLUDE_EMPTY_BUCKETS:
        df_join = df_join.filter(F.col("table_name").isNotNull())

    # 8) Para buckets sin tablas, llenamos columnas de tabla con un marcador
    df_output = (
        df_join
        .withColumn("table_schema", F.coalesce(F.col("table_schema"), F.lit("(sin tablas)")))
        .withColumn("table_name",   F.coalesce(F.col("table_name"),   F.lit("(sin tablas)")))
        .withColumn("location",     F.coalesce(F.col("location"),     F.lit("(sin tablas)")))
        .select("bucket", "table_schema", "table_name", "location")
        .orderBy(F.col("bucket"), F.col("table_schema"), F.col("table_name"))
    )

    print("📋 Vista previa del inventario:")
    df_output.show(50, truncate=False)

    # 9) Escribimos CSV y Parquet
    (
        df_output.coalesce(1)
                 .write.mode("overwrite")
                 .option("header", True)
                 .csv(OUTPUT_S3.rstrip("/") + "/csv/")
    )
    (
        df_output.write.mode("overwrite")
                 .parquet(OUTPUT_S3.rstrip("/") + "/parquet/")
    )

    print("✅ Inventario generado:")
    print(f"   CSV:     {OUTPUT_S3.rstrip('/')}/csv/")
    print(f"   Parquet: {OUTPUT_S3.rstrip('/')}/parquet/")

if __name__ == "__main__":
    main()
