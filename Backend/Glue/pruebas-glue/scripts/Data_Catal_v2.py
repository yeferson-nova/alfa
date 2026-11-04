import os
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row
from pyspark.sql.functions import trim, col

# -----------------------------
# Parámetros
# -----------------------------
AWS_REGION = "us-east-1" #Parametrizar
#DB_FILTRO = "alfa-catalog-dev-bronce"  # filtra una DB (opcional)
DB_FILTRO = None
OUTPUT_S3 = os.getenv("OUTPUT_S3", "s3://alfa-bucket-dev-wi/inventarios/buckets_y_tablas/") #Parametrizar
DELIMITER = "|"                        # como en tu código actual #Parametrizar
FORMATO_SALIDA = "csv"                 # "csv" o "parquet" #Parametrizar
REPARTICIONES = 1 #Parametrizar



# -----------------------------
# Inicializaciones
# -----------------------------
spark = SparkSession.builder.getOrCreate()
glue = boto3.client("glue", region_name=AWS_REGION)

# Esquema explícito (todo como string en tu caso)
schema = StructType([
    StructField("table_schema",  StringType(), True),
    StructField("table_name",    StringType(), True),
    StructField("fuente_origen", StringType(), True),
    StructField("nombre_campo",  StringType(), True),
    StructField("tipo_atributo", StringType(), True),
])

# -----------------------------
# 1) Extraer metadatos de Glue → lista de Rows
# -----------------------------
rows = []

try:
    db_paginator  = glue.get_paginator("get_databases")
    tbl_paginator = glue.get_paginator("get_tables")

    for db_page in db_paginator.paginate():
        for db in db_page.get("DatabaseList", []):
            db_name = db.get("Name")

            # Filtro opcional por una DB específica
            if DB_FILTRO and db_name != DB_FILTRO:
                continue

            for tbl_page in tbl_paginator.paginate(DatabaseName=db_name):
                #iteramos por cada tabla
                for tbl in tbl_page.get("TableList", []):
                    table_name = tbl.get("Name")
                    
                    #creamos sd para info de s3 y columnas
                    sd = tbl.get("StorageDescriptor", {}) or {}
                    fuente_origen = sd.get("Location", "")
                    columnas = sd.get("Columns", []) or []
                    
                    #obtener las particiones
                    pk = tbl.get("PartitionKeys", {}) or {}
                    
                    #iteramos sobre las columnas y creamos registro
                    for col_def in columnas:
                        atributo = col_def.get("Name")
                        tipo_atributo = col_def.get("Type")

                        # Asegura strings y evita None
                        rows.append(Row(
                            table_schema  = str(db_name or ""),
                            table_name    = str(table_name or ""),
                            fuente_origen = str(fuente_origen or ""),
                            nombre_campo  = str(atributo or ""),
                            tipo_atributo = str(tipo_atributo or "")
                        ))
                    
                    #iteramos sobre las particiones
                    for par in pk:
                        atributo = par.get("Name")
                        tipo_atributo = par.get("Type")
                        
                        # Asegura strings y evita None
                        rows.append(Row(
                            table_schema  = str(db_name or ""),
                            table_name    = str(table_name or ""),
                            fuente_origen = str(fuente_origen or ""),
                            nombre_campo  = str(atributo or ""),
                            tipo_atributo = str(tipo_atributo or "")
                        ))
                        
except (BotoCoreError, ClientError) as e:
    raise RuntimeError(f"Error consultando Glue: {e}")

# -----------------------------
# 2) Crear DataFrame Spark directamente
# -----------------------------
df_spark = spark.createDataFrame(rows, schema=schema)

# Limpieza mínima (recorta espacios)
df_spark = (df_spark
            .withColumn("table_schema",  trim(col("table_schema")))
            .withColumn("table_name",    trim(col("table_name")))
            .withColumn("fuente_origen", trim(col("fuente_origen")))
            .withColumn("nombre_campo",  trim(col("nombre_campo")))
            .withColumn("tipo_atributo", trim(col("tipo_atributo"))))

df_spark.show(10, truncate=False)
df_spark.printSchema()

# -----------------------------
# 3) Escritura en S3
# -----------------------------
if FORMATO_SALIDA.lower() == "csv":
    # CSV con encabezado y delimitador "|"
    (df_spark
        .coalesce(REPARTICIONES)     # 1 archivo (para sets pequeños). Para grandes, usa repartition.
        .write
        .mode("overwrite")
        .option("header", True)
        .option("delimiter", DELIMITER)
        .csv(OUTPUT_S3))
else:
    # Parquet recomendado (tipos, compresión, mejor para Athena)
    (df_spark
        .repartition(REPARTICIONES)
        .write
        .mode("overwrite")
        .parquet(OUTPUT_S3))


df_spark.show()
job.commit()