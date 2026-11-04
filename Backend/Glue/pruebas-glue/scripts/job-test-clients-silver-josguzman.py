# glue config

# add python files

#%extra_py_files s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/scripts/utils.zip,s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/scripts/schemas_silver.zip,s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/scripts/commons.zip,s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/scripts/silver_staging.zip
 
 
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from utils import Utils
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from commons.ExtractHelper import ExtractHelper
from pyspark.sql import functions as F
 
import boto3
import datetime
 
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

spark = glueContext.spark_session
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")
job = Job(glueContext)
ExtractHelper = ExtractHelper(glueContext, spark, "-", "-")

utils = Utils()

ruta = "s3://alfa-datalake-dev-silver/POLIZAS_PRODUCTO/ACSELE/POLIZAS_PRODUCTO_ACSELE_STG/yyyy=2025/mm=05/"
df_f = spark.read.format("parquet").load(ruta)
df_f.columns
from pyspark.sql import functions as F
df_valores =df_f.filter(F.col("nro_poliza").isin('0000352932', '0000352933')).select('valor_asegurado')

import re
import unicodedata
import datetime
import math
from typing import List, Optional, Any
from pyspark.sql import functions as F, Window
from pyspark.sql.functions import (
    split, to_date, date_format, col, regexp_replace, trim, when, isnan, isnull,
    upper, lower, regexp_extract, length, abs as spark_abs, ceil, coalesce,
    translate, ltrim, rtrim, current_date, datediff, year, month, dayofmonth, initcap, lpad ,last_day, expr,concat
)
from pyspark.sql.types import DecimalType
def limpiar_valor_prima( prima_col):
    """
    Limpia y homologa el valor de la prima según reglas de negocio.

    Reglas aplicadas:
    - Eliminación de espacios
    - Eliminación de caracteres distintos a números (excepto separadores , y .)
    - Eliminación del separador de miles (coma)
    - Conversión a decimal con hasta 4 decimales
    - Separador de decimales: '.'

    Parameters:
        prima_col: Spark column con el valor de prima

    Returns:
        Spark column con el valor limpio como decimal(18,4)
    """
    return when(
        prima_col.isNull(),
        None
    ).otherwise(
        when(
            trim(prima_col) == "",
            None
        ).otherwise(
            regexp_replace(
                regexp_replace(
                    trim(prima_col),
                    r"[^0-9.eE\-+]",
                    ""
                ),
                ",",
                ""
            ).cast(DecimalType(18, 4))
        )
    )
df_valores_limpio = df_valores.withColumn("valor_asegurado", limpiar_valor_prima(F.col("valor_asegurado")))
df_valores_limpio.show()

df = (spark.read.format("csv")
      .option("header", "true")        # el archivo tiene cabecera
      .option("delimiter", "|")        # delimitador pipe
      .option("inferSchema", "true")   # infiere tipos (rápido, pero puede equivocarse)
      .option("quote", '"')            # maneja campos entre comillas si existen
      .option("escape", "\\")          # para comillas escapadas y caracteres especiales
      .option("multiLine", "false")    # cambia a true si hay saltos de línea dentro de campos
      .option("mode", "PERMISSIVE")    # o DROPMALFORMED/FAILFAST
      .option("encoding", "UTF-8")     # ajusta si es Latin-1/ISO-8859-1
      .load(ruta))

df.show(5,0)
from pyspark.sql import functions as F
keys = ["CUSTOMER_NO"]

dupes = (df
    .groupBy(*keys)
    .agg(F.count(F.lit(1)).alias("cnt"))
    .filter(F.col("cnt") > 1)           # solo llaves repetidas
)

dupes.show(truncate=False)
dupes.count()

df = df.dropDuplicates(keys)
from pyspark.sql import functions as F
keys = ["CUSTOMER_NO"]

dupes = (df
    .groupBy(*keys)
    .agg(F.count(F.lit(1)).alias("cnt"))
    .filter(F.col("cnt") > 1)           # solo llaves repetidas
)

dupes.show(truncate=False)
dupes.count()


df.coalesce(1).write.mode("overwrite").option("header","true").option("delimiter","|").csv("s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_bronze/cardiff_full_deduplicado/")

path = f"s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_bronze/test_data/dim_periodo.xlsx"
sheet = "Datos"
df_tmp_tables = ExtractHelper.extract_excel_data(path, sheet)
df_tmp_tables.show(2)
from pyspark.sql import functions as F
from pyspark.sql import Window
# --- 2) Asegurar tipo fecha ---
df = df_tmp_tables.withColumn("Fecha", F.to_date("Fecha"))

# --- 3) Opcional: rango de años ---
df = df.filter((F.year("Fecha") >= 1900) & (F.year("Fecha") <= 2979))

# --- 4) Llaves de año/mes ---
df = df.withColumn("Año", F.year("Fecha")).withColumn("Mes", F.month("Fecha"))

# --- 5) Elegir la primera fila por (Año, Mes) según la fecha ---
w = Window.partitionBy("Año", "Mes").orderBy(F.col("Fecha").asc())
primer_fila_mes = (
    df.withColumn("rn", F.row_number().over(w))
      .filter(F.col("rn") == 1)
      .drop("rn")
)
primer_fila_mes.show(2)
df = primer_fila_mes.withColumn("Periodo", F.date_format(F.col("Fecha"), "yyyyMM"))
df_sorted = df.orderBy(F.col("Fecha").asc())
df_sorted.show(5)
df_pd=df_sorted.toPandas()
import pandas as pd
output_file = "s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_bronze/test_data/primer_dia_por_mes.xlsx" 
writer = pd.ExcelWriter(output_file, engine="openpyxl")
df_pd.to_excel("s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_bronze/test_data/dim_periodo.xlsx", sheet_name="Datos", index=False)
df_sorted.coalesce(1).write.format("csv").option("header",True).mode("overwrite").save("s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_bronze/test_data/solo_una_fecha.csv")

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
sc = SparkContext.getOrCreate()

                                  
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")
job = Job(glueContext)
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import to_date,to_timestamp, when, col,lit,udf
from pyspark.sql.types import StringType
import sys
from datetime import datetime
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, isnan, when, count, lower, trim, regexp_replace, to_date, year, month, dayofmonth,  current_date,last_day, concat
from awsglue.dynamicframe import DynamicFrame
# Custom libraries
from cardiff_staging_processor import CardiffStagingProcessor
from alfaprd_staging_processor import AlfprdStagingProcessor
from acsele_staging_processor import AcseleStagingProcessor
from servialfa_staging_processor import ServiAlfaStagingProcessor
from data_harmonization_manager import DataHarmonizationManager
from transformer_proccessor import DataCleanupProcessor
from dimension_handler import SCDDimensionHandler
from source_catalog_schema import SourceDataGenerator
from utils import Utils
from s3_handler import S3Handler
from cleanup import CleanUp



import pandas as pd
from pathlib import Path

# === Parámetros: ajusta según tu archivo ===
input_file = "s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_bronze/test_data/dim_periodo.xlsx"   # Ruta del Excel de entrada
output_file = "primer_dia_por_mes.xlsx"  # Nombre del Excel de salida
nombre_columna_fecha = "Fecha"        # Cambia si tu columna se llama distinto
hojas_a_procesar = None               # None = todas las hojas; o lista: ["Hoja1", "Hoja2"]

# === Carga del Excel ===
# engine="openpyxl" para .xlsx; para .xls usa engine="xlrd"
xlsx = pd.ExcelFile(input_file, engine="openpyxl")
sheet_names = xlsx.sheet_names if hojas_a_procesar is None else hojas_a_procesar

writer = pd.ExcelWriter(output_file, engine="openpyxl")

for sheet in sheet_names:
    df = pd.read_excel(xlsx, sheet_name=sheet)
    
    # Validación de columna de fecha
    if nombre_columna_fecha not in df.columns:
        print(f"[AVISO] En la hoja '{sheet}' no se encontró la columna '{nombre_columna_fecha}'. Se omite.")
        continue

    # Convertir a tipo datetime (manejo robusto de formatos)
    df[nombre_columna_fecha] = pd.to_datetime(df[nombre_columna_fecha], errors="coerce")

    # Quitar filas sin fecha válida
    df_valid = df.dropna(subset=[nombre_columna_fecha]).copy()

    # Ordenar por fecha ascendente (importante para quedarse con la primera fila por mes)
    df_valid = df_valid.sort_values(by=nombre_columna_fecha)

    # Crear llaves de año-mes para agrupar
    df_valid["Año"] = df_valid[nombre_columna_fecha].dt.year
    df_valid["Mes"] = df_valid[nombre_columna_fecha].dt.month

    # Tomar la primera fila de cada (Año, Mes)
    primer_dia_mes = (
        df_valid.groupby(["Año", "Mes"], as_index=False)
        .first()  # como está ordenado ascendente, la primera fila será el primer día del mes
    )

    # Opcional: si quieres que la columna fecha sea exactamente el primer día de cada mes,
    # independientemente del dato original:
    # primer_dia_mes[nombre_columna_fecha] = primer_dia_mes[nombre_columna_fecha].dt.to_period('M').dt.to_timestamp()

    # Reordenar columnas opcional (Fecha primero)
    cols = [nombre_columna_fecha] + [c for c in primer_dia_mes.columns if c not in [nombre_columna_fecha]]
    primer_dia_mes = primer_dia_mes[cols]

    # Exportar a hoja con mismo nombre
    primer_dia_mes.to_excel(writer, sheet_name=sheet, index=False)

writer.close()

print(f"Archivo generado: {Path(output_file).resolve()}")


from pyspark.sql import functions as F
from pyspark.sql import Window

# --- 1) Leer Excel como antes ---
df = (
    spark.read.format("com.crealytics.spark.excel")
    .option("header", True)
    .option("inferSchema", True)
    .load("s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_bronze/test_data/dim_periodo.xlsx")
)

# --- 2) Asegurar tipo fecha ---
df = df.withColumn("Fecha", F.to_date("Fecha"))

# --- 3) Opcional: rango de años ---
df = df.filter((F.year("Fecha") >= 1900) & (F.year("Fecha") <= 2979))

# --- 4) Llaves de año/mes ---
df = df.withColumn("Año", F.year("Fecha")).withColumn("Mes", F.month("Fecha"))

# --- 5) Elegir la primera fila por (Año, Mes) según la fecha ---
w = Window.partitionBy("Año", "Mes").orderBy(F.col("Fecha").asc())
primer_fila_mes = (
    df.withColumn("rn", F.row_number().over(w))
      .filter(F.col("rn") == 1)
      .drop("rn")
)

# --- 6) (Opcional) Forzar la columna Fecha al inicio del mes (día 1 exacto) ---
# Si necesitas que sea siempre día 1 aunque la fila original no lo sea:
# primer_fila_mes = primer_fila_mes.withColumn("Fecha", F.date_trunc("month", F.col("Fecha")).cast("date"))

# --- 7) Escribir a Excel ---
(
    primer_fila_mes.coalesce(1)
    .write.format("com.crealytics.spark.excel")
    .option("header", True)
    .option("dataAddress", "'PrimerDiaMes'!A1")
    .mode("overwrite")
    .save("s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_bronze/test_data/primer_dia_por_mes.xlsx")
)



from datetime import datetime

# Obtener el año actual
anio_actual = datetime.now().year
print(anio_actual)


# Glue Python Shell job
import sys
import time
import threading
import traceback
from typing import Iterable, List, Dict
from awsglue.utils import getResolvedOptions
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

# ====== Parámetros esperados ======
# --src_bucket: bucket origen
# --src_prefix: prefijo origen sin slash final (ej: "origen")
# --dst_bucket: bucket destino
# --dst_prefix: prefijo destino sin slash final (ej: "destino")
# --src_year  : año a mover (ej: "2024")
# --months    : "all" o lista "01,02,03"
# --dry_run   : "true"/"false" (recomiendo true primero)
# --dst_kms_key_id : (opcional) ARN/ID de KMS para cifrado en destino
# --threads   : (opcional) número de hilos para paralelizar (ej: "10")

#args = getResolvedOptions(sys.argv, [
#    'src_bucket','src_prefix','dst_bucket','dst_prefix','src_year','months','dry_run'
#])

# Opcionales con valor por defecto
dst_kms_key_id = None
threads = 5
#if '--dst_kms_key_id' in sys.argv:
#    dst_kms_key_id = getResolvedOptions(sys.argv, ['dst_kms_key_id'])['dst_kms_key_id']
#if '--threads' in sys.argv:
#    threads = int(getResolvedOptions(sys.argv, ['threads'])['threads'])

src_bucket = "alfa-datalake-dev-gold"
src_prefix = args['src_prefix']              # ej: "origen"
dst_bucket = "alfa-datalake-dev-gold"
#dst_prefix =              # ej: "destino"
src_year = 2025                 # ej: "2024"
months = 12                      # "all" o "01,02,03"
dry_run = 'true'

# Config de cliente con reintentos
cfg = Config(retries={'max_attempts': 10, 'mode': 'standard'})
s3 = boto3.client('s3', config=cfg)

def build_source_prefixes(src_prefix: str, year: str, months: str) -> List[str]:
    if months.lower() == "all":
        month_list = [str(m).zfill(2) for m in range(1,13)]
    else:
        month_list = [m.strip() for m in months.split(',')]
    # Ajusta aquí si tu estructura es distinta:
    # Ejemplo actual: origen/year=YYYY/month=MM/
    return [f"{src_prefix}/year={year}/month={m}/" for m in month_list]

def list_keys(bucket: str, prefix: str) -> Iterable[Dict]:
    """Itera objetos de un prefijo con paginación."""
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            yield obj

def copy_object(copy_source: Dict, dst_bucket: str, dst_key: str, size_bytes: int):
    """Copia con soporte SSE-KMS opcional y valida tamaño."""
    put_args = {'Bucket': dst_bucket, 'Key': dst_key}
    if dst_kms_key_id:
        put_args.update({
            'ServerSideEncryption': 'aws:kms',
            'SSEKMSKeyId': dst_kms_key_id
        })
    # Nota: s3.copy encapsula copy_object; para objetos >5GB, S3 hace multipart copy automáticamente
    s3.copy(copy_source, **put_args)

    # Validar existencia y tamaño
    head = s3.head_object(Bucket=dst_bucket, Key=dst_key)
    if head['ContentLength'] != size_bytes:
        raise RuntimeError(f"Tamaño no coincide para {dst_key}: esperado {size_bytes}, got {head['ContentLength']}")

def delete_source(bucket: str, key: str):
    s3.delete_object(Bucket=bucket, Key=key)

def worker(job_queue: List[Dict], stats: Dict):
    """Hilo consumidor que procesa items del job_queue."""
    while True:
        try:
            item = None
            # sección crítica simple
            try:
                item = job_queue.pop()
            except IndexError:
                return

            key = item['Key']
            size = item['Size']
            spref = item['SourcePrefix']

            # Sufijo relativo al prefijo origen
            # Si tu estructura es distinta, ajusta el cálculo del dst_key aquí:
            # Ejemplo destino: destino/year=YYYY/month=MM/<resto>
            suffix = key.split(spref, 1)[1] if key.startswith(spref) else key
            # El primer segmento del suffix suele ser el archivo o subcarpeta; mantenemos jerarquía
            dst_key = f"{dst_prefix}/year={src_year}/month={spref.split('month=')[1].split('/')[0]}/{suffix}"
            #s3://alfa-datalake-dev-gold/GOLD/CLIENTES_GOLD/GOLD-CARACT_CLIENTES-FACT_POLIZA_EMISION/
            msg = f"MOVER: s3://{src_bucket}/{key}  ->  s3://{dst_bucket}/{dst_key}"
            print(msg)

            if not dry_run:
                copy_source = {'Bucket': src_bucket, 'Key': key}
                copy_object(copy_source, dst_bucket, dst_key, size)
                delete_source(src_bucket, key)

            stats['moved'] += 1
            stats['bytes'] += size
        except ClientError as ce:
            stats['errors'].append((item, f"ClientError: {ce}"))
            traceback.print_exc()
        except Exception as e:
            stats['errors'].append((item, f"Error: {e}"))
            traceback.print_exc()

def main():
    source_prefixes = build_source_prefixes(src_prefix, src_year, months)
    pending_jobs: List[Dict] = []
    total_source_bytes = 0

    # Enumerar objetos de cada prefijo
    for spref in source_prefixes:
        print(f"Listando: s3://{src_bucket}/{spref}")
        for obj in list_keys(src_bucket, spref):
            pending_jobs.append({'Key': obj['Key'], 'Size': obj['Size'], 'SourcePrefix': spref})
            total_source_bytes += obj['Size']

    if not pending_jobs:
        print(f"No se encontraron objetos para year={src_year} en {src_bucket}/{src_prefix}")
        return

    print(f"Total objetos a procesar: {len(pending_jobs)} | Total bytes: {total_source_bytes}")

    # Lanzar hilos
    stats = {'moved': 0, 'bytes': 0, 'errors': []}
    workers = []
    start = time.time()

    # Usamos copia de la lista como stack LIFO; los hilos harán pop()
    job_stack = pending_jobs.copy()

    for i in range(threads):
        t = threading.Thread(target=worker, args=(job_stack, stats), daemon=True)
        workers.append(t)
        t.start()

    for t in workers:
        t.join()

    elapsed = time.time() - start
    print(f"\n==== Resumen ====")
    print(f"Dry run       : {dry_run}")
    print(f"Objetos movidos: {stats['moved']}/{len(pending_jobs)}")
    print(f"Bytes movidos  : {stats['bytes']} (de {total_source_bytes})")
    print(f"Tiempo (s)     : {int(elapsed)}")
    if stats['errors']:
        print(f"Errores ({len(stats['errors'])}):")
        for item, err in stats['errors']:
            print(f"- {item['Key']}: {err}")

if __name__ == "__main__":
    main()

s3_input = "s3://tu-bucket/ruta/archivo.txt"
s3_output = "s3://tu-bucket/ruta/output/"
# Lee la primera línea para detectar delimitador
first_line = spark.read.text(s3_input).first()[0]
# Detecta el delimitador más común
delimiters = ["|", "\t", ";", ","]
delimiter = max(delimiters, key=lambda d: first_line.count(d))
print(f"Delimitador detectado: {repr(delimiter)}")
# Lee con el delimitador detectado
df = spark.read \
   .option("header", "true") \
   .option("inferSchema", "true") \
   .option("delimiter", delimiter) \
   .csv(s3_input)
df.write.mode("overwrite").parquet(s3_output)
df = spark.sql("SELECT * FROM alfa-datalake-dev-gold.gold_caract_clientes_datos_completos_view")
df_view = glueContext.create_dynamic_frame.from_catalog(database="alfa-datalake-dev-gold",table_name="gold_caract_clientes_datos_completos_view").toDF()
df = utils.read_full_parquet(spark, "s3://alfa-datalake-dev-gold/GOLD/CLIENTES_GOLD/sample_30pct/")
df.count()
df = spark.read.parquet("s3://alfa-datalake-dev-gold/GOLD/CLIENTES_GOLD/sample_30pct/")
df.count()
df.printSchema()
def only_numbers( text_col):
        """
        Extract only numbers using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Column with only numbers
        """
        return when(text_col.isNull(), None).otherwise(
            regexp_replace(text_col, "[^0-9]", "")
        )
def remove_extra_spaces( text_col):
        """
        Remove extra spaces using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Column without extra spaces
        """
        return when(text_col.isNull(), None).otherwise(
            trim(regexp_replace(text_col, "\\s+", " "))
        )
def clean_phone_number( text_col):
        """
        Clean phone number using native Spark functions.
    
        Parameters:
            text_col: Spark column
    
        Returns:
            Cleaned phone number column
        """
        text_col_str = text_col.cast("string")  
        only_digits = only_numbers(text_col_str)
        regex = r'^(\d)\1+$'
        
        return when(text_col.isNull(), None).otherwise(
            when(regexp_extract(only_digits, regex, 0) != "", "No Registra").otherwise(when(length(only_digits) > 10,
                 F.substring(only_digits, 1, 10)
            ).otherwise(
                remove_extra_spaces(only_digits)
            ))
        )

data = [
    ("1111111",),
    ("222",),
    ("1234567",),
    ("999999",),
    ("55555",),
    ("321321",),
    ("0000000",),
    ("987654",)
]

# Crear DataFrame
df = spark.createDataFrame(data, ["numero_telefono"])

def clean_email(email_col):
    """
    Clean email using native Spark functions, preserving valid structure.
    Parameters:
        email_col: Spark column
    Returns:
        Cleaned email column or None if invalid
    """
    cleaned = lower(trim(email_col))
 
 
    cleaned = regexp_replace(cleaned, r'^[\.]+|[\.]+$', '') 
    cleaned = regexp_replace(cleaned, r'[\"\,\:\;\<\>\(\)\[\]\{\}\|]', '')  
    cleaned = regexp_replace(cleaned, r'\s+', '')  
    cleaned = regexp_replace(cleaned, r'\.@', '@')  
    cleaned = regexp_replace(cleaned, r'@+', '@')   
    cleaned = regexp_replace(cleaned, r'\.\.', '.')  
 
    
    return when(email_col.isNull(), None) \
        .when(email_col.rlike("^[0-9]+$"), None) \
        .when(cleaned.rlike(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'), cleaned) \
        .otherwise(None)
def clean_email(email_col):
        """
        Clean email using native Spark functions.

        Parameters:
            email_col: Spark column

        Returns:
            Cleaned email column or None if invalid
        """
        cleaned = lower(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    trim(email_col),  # elimina espacios al inicio y final
                                    "^[\\.]+|[\\.]+$", ""  # elimina puntos al inicio y final
                                ),
                                "[\"\\,\\:\\;\\&lt;&gt;\\(\\)\\[\\]\\{\\}\\|]", ""  # elimina caracteres especiales
                            ),
                            "\\.@", "@"  # corrige errores con punto antes de @
                        ),
                        "@+", "@"  # corrige múltiples arrobas
                    ),
                    "\\.\\.", "."  # corrige puntos dobles
                ),
                "\\s+", ""  # elimina espacios internos si los hay
            )
        )
        return when(email_col.isNull(), None) \
            .when(email_col.rlike("^[0-9]+$"), None) \
            .otherwise(cleaned)
def adjust_invalid_date_2(date_col):
        """
        Adjust invalid dates using native Spark functions.
        """
        lastDate = last_day(to_date(concat(year(date_col), F.lit("-"), month(date_col), F.lit("-1"))))

        return when(date_col.isNull(), None).otherwise(
            when(dayofmonth(date_col) > lastDate.cast("int"),
                to_date(F.concat(year(date_col), F.lit("-"), month(date_col), F.lit(lastDate)), "yyyy-M-d")
            ).otherwise(date_col)
        )    
utils = Utils()
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")
df_clean = utils.read_full_parquet(spark, "s3://alfa-datalake-dev-silver/CLIENTES/DATOS_SOCIODEMOGRAFICOS/CLIENTES_DATOS_SOCIODEMOGRAFICOS_STG/")
df_unico =df_clean.filter(F.col("numero_documento").isin("1032408138"))
df_unico.show()
def adjust_invalid_date(date_col):
        """
        Adjust invalid dates using native Spark functions.
        """
        return when(date_col.isNull(), None).otherwise(
            when(dayofmonth(date_col) > 28,
                to_date(F.concat(year(date_col), F.lit("-"), month(date_col), F.lit("-28")), "yyyy-M-d")
            ).otherwise(date_col)
        )
df_cl = df_unico.withColumn("fecha_nacimiento_limpio", adjust_invalid_date(col("fecha_nacimiento")))
df_cl.select("fecha_nacimiento","fecha_nacimiento_limpio").show(truncate=False)
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""================================================================================
                    Cleanup Utility - AWS Glue (Native Spark Functions)
================================================================================

Description:
    Utility class with cleanup functions using native Spark functions instead of UDFs.

Author:         Jose Guzman Moreno [jose.guzman@gft.com]
Created:        2025-07-21
Last Updated:   2025-08-10
Version:        2.0.0 - Native Spark Implementation
Environment:    Development

Dependencies:   re, unicodedata, datetime, math

Class:          CleanUp

================================================================================
"""

import re
import unicodedata
import datetime
import math
from logger_config import logger
from typing import List, Optional, Any
from pyspark.sql import functions as F, Window
from pyspark.sql.functions import (
    split, to_date, date_format, col, regexp_replace, trim, when, isnan, isnull,
    upper, lower, regexp_extract, length, abs as spark_abs, ceil, coalesce,
    translate, ltrim, rtrim, current_date, datediff, year, month, dayofmonth, initcap, lpad
)

class CleanUp:

    def __init__(self):
        """
          init  .

        Returns:
            type: Description of return value.
        """

    def round_up_number(self, value_col):
        """
        Approximate number to the largest number using native Spark functions.

        Parameters:
            value_col: Spark column

        Returns:
            Spark column with ceiled values
        """
        return when(value_col.isNull(), None).otherwise(ceil(value_col.cast("double")))

    def calculate_age(self, birth_date_col):
        """
        Calculate age using native Spark functions.

        Parameters:
            birth_date_col: Spark column with birth dates

        Returns:
            Spark column with calculated age
        """
        return when(birth_date_col.isNull(), None).otherwise(
            year(current_date()) - year(birth_date_col) -
            when((month(current_date()) < month(birth_date_col)) |
                 ((month(current_date()) == month(birth_date_col)) &
                  (dayofmonth(current_date()) < dayofmonth(birth_date_col))), 1).otherwise(0)
        )

    def concatenate_if_applicable(self, *args):
        """
        Concatenate columns if applicable using native Spark functions.

        Parameters:
            *args: Spark columns

        Returns:
            Concatenated string column
        """
        non_null_cols = [when(arg.isNull() | (arg == ""), "").otherwise(arg) for arg in args]
        return trim(regexp_replace(F.concat_ws(" ", *non_null_cols), r"\s+", " "))

    def to_lowercase(self, text_col):
        """
        Convert to lowercase using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Lowercase column
        """
        return when(text_col.isNull(), None).otherwise(lower(text_col))

    def to_uppercase(self, text_col):
        """
        Convert to uppercase using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Uppercase column
        """
        return when(text_col.isNull(), None).otherwise(upper(text_col))

    def absolute_value(self, value_col):
        """
        Absolute value using native Spark functions.

        Parameters:
            value_col: Spark column

        Returns:
            Absolute value column
        """
        return when(value_col.isNull(), None).otherwise(spark_abs(value_col.cast("double")))

    def adjust_day_month_out_of_range(self, date_col):
        """
        Adjust day and month to be within valid ranges using native Spark functions.
        """
        return when(date_col.isNull(), None).otherwise(
            to_date(
                F.concat(
                    year(date_col),
                    F.lit("-"),
                    when(month(date_col) > 12, 12)
                    .when(month(date_col) < 1, 1)
                    .otherwise(month(date_col)),
                    F.lit("-"),
                    when(dayofmonth(date_col) > 28, 28)
                    .when(dayofmonth(date_col) < 1, 1)
                    .otherwise(dayofmonth(date_col))
                ),
                "yyyy-M-d"
            )
        )

    def only_numbers(self, text_col):
        """
        Extract only numbers using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Column with only numbers
        """
        return when(text_col.isNull(), None).otherwise(
            regexp_replace(text_col, "[^0-9]", "")
        )

    def only_alphanumeric(self, text_col):
        """
        Extract only alphanumeric characters using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Column with only alphanumeric characters
        """
        return when(text_col.isNull(), None).otherwise(
            regexp_replace(text_col, "[^a-zA-Z0-9]", "")
        )

    def remove_special_characters(self, text_col):
        """
        Remove special characters using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Column without special characters
        """
        return when(text_col.isNull(), None).otherwise(
            regexp_replace(
                regexp_replace(text_col, "[\x00-\x1F\x7F-\x9F]", ""),
                "[^\\w\\s\\u00C0-\\u017F]", ""
            )
        )

    def clean_stopwords(self, text_col):
        """
        Clean stopwords using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Column without stopwords
        """
        stopwords_pattern = "\\b(de|la|el|los|las|un|una|unos|unas|del|al|y|o|en|por|para|con|sin|sobre|entre|hacia|hasta|desde|que)\\b"
        return when(text_col.isNull(), None).otherwise(
            trim(regexp_replace(text_col, stopwords_pattern, " "))
        )

    def clean_stopwords_col(self, col):
        """
        Clean stopwords for columns using native Spark functions.

        Parameters:
            col: Spark column

        Returns:
            Cleaned column
        """
        stopwords_pattern = "\\b(de|la|el|los|las|un|una|unos|unas|del|al|y|o|en|por|para|con|sin|sobre|entre|hacia|hasta|desde|que)\\b"
        cleaned = regexp_replace(col, stopwords_pattern, " ")
        cleaned = regexp_replace(cleaned, "\\s+", " ")
        cleaned = trim(cleaned)
        return cleaned

    def normalize_columns(self, df, column_name=None):
        """
        Normalizes text in a specific column or in all string-type columns.
        """
        if column_name:
            return df.withColumn(column_name, self.clean_stopwords_col(col(column_name)))
        else:
            string_cols = [c for c, t in df.dtypes if t == "string"]
            for c in string_cols:
                df = df.withColumn(c, self.clean_stopwords_col(col(c)))
            return df

    def remove_nit_verification_digit(self, nit_col):
        """
        Remove check digit using native Spark functions.

        Parameters:
            nit_col: Spark column

        Returns:
            Column without verification digit
        """
        return when(nit_col.isNull(), None).otherwise(
            when(nit_col.contains("-"),
                 regexp_extract(nit_col, "^([^-]+)", 1)
            ).otherwise(nit_col)
        )

    def remove_extra_spaces(self, text_col):
        """
        Remove extra spaces using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Column without extra spaces
        """
        return when(text_col.isNull(), None).otherwise(
            trim(regexp_replace(text_col, "\\s+", " "))
        )

    def remove_letters_if_document(self, text_col, doc_type_col):
        """
        Remove letters if document type is applicable using native Spark functions.

        Parameters:
            text_col: Spark column
            doc_type_col: Spark column with document type

        Returns:
            Processed column
        """
        return when(text_col.isNull(), None).otherwise(
            when(doc_type_col.isin(["CC", "TI", "RC", "CE", "NIT"]),
                 regexp_replace(upper(text_col), "[A-Za-z]", "")
            ).otherwise(text_col)
        )

    def clean_email(self, email_col):
        """
        Clean email using native Spark functions.

        Parameters:
            email_col: Spark column

        Returns:
            Cleaned email column or None if invalid
        """
        cleaned = lower(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    trim(email_col),  # elimina espacios al inicio y final
                                    "^[\\.]+|[\\.]+$", ""  # elimina puntos al inicio y final
                                ),
                                "[\"\\,\\:\\;\\&lt;&gt;\\(\\)\\[\\]\\{\\}\\|]", ""  # elimina caracteres especiales
                            ),
                            "\\.@", "@"  # corrige errores con punto antes de @
                        ),
                        "@+", "@"  # corrige múltiples arrobas
                    ),
                    "\\.\\.", "."  # corrige puntos dobles
                ),
                "\\s+", ""  # elimina espacios internos si los hay
            )
        )
        return when(email_col.isNull(), None) \
            .when(email_col.rlike("^[0-9]+$"), None) \
            .otherwise(cleaned)
    def remove_accents(self, text_col):
        """
        Remove accents using native Spark functions.
        Mapeo corregido para evitar errores de correspondencia.
    
        Parameters:
        text_col: Spark column
    
        Returns:
        Column without common accents
        """
        # Separemos claramente minúsculas y mayúsculas para evitar errores
    
        # Minúsculas con acento (23 caracteres)
        accent_lower = "áàâäéèêëíìîïóòôöúùûüñç"
        normal_lower = "aaaaeeeeiiiioooouuuunc"
    
        # Mayúsculas con acento (23 caracteres)
        accent_upper = "ÁÀÂÄÉÈÊËÍÌÎÏÓÒÔÖÚÙÛÜÑÇ"
        normal_upper = "AAAAEEEEIIIIOOOOUUUUNC"
    
        # Concatenamos para el mapeo completo
        accent_map = accent_lower + accent_upper
        normal_map = normal_lower + normal_upper
    
        return when(text_col.isNull(), None).otherwise(
            translate(text_col, accent_map, normal_map)
        )


    def remove_text_special_characters(self, text_col):
        """
        Remove special characters and accents using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Cleaned column
        """
        return when(text_col.isNull(), None).otherwise(
            self.remove_accents(self.remove_special_characters(text_col))
        )

    def replace_multiple_spaces(self, text_col):
        """
        Replace multiple spaces using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Column with single spaces
        """
        return when(text_col.isNull(), None).otherwise(
            regexp_replace(text_col, "\\s+", " ")
        )

    def select_minimum_date(self, date_list, format="%d/%m/%Y"):
        """
        This function requires complex logic that's better handled in the calling code.
        Keeping original implementation note for reference.
        """
        # This would need to be implemented at the DataFrame level with multiple columns
        pass

    def validate_date(self, date_col):
        """
        Validate date using native Spark functions.

        Parameters:
            date_col: Spark column

        Returns:
            Valid date column
        """
        return when(date_col.isNull(), None).otherwise(
            when((year(date_col) >= 1900) & (date_col < current_date()), date_col)
        )

    def adjust_invalid_date(self, date_col):
        """
        Adjust invalid dates using native Spark functions.
        """
        return when(date_col.isNull(), None).otherwise(
            when(dayofmonth(date_col) > 28,
                to_date(F.concat(year(date_col), F.lit("-"), month(date_col), F.lit("-28")), "yyyy-M-d")
            ).otherwise(date_col)
        )

    def change_invalid_date_to_null(self, date_col):
        """
        Convert invalid dates to null using native Spark functions.
        """
        return when(date_col.isNull(), None).otherwise(
            when((year(date_col) >= 1900) & (date_col < current_date()), date_col)
        )

    def clean_id_type(self, text_col):
        """
        Clean ID type using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Cleaned ID type column
        """
        return when(text_col.isNull(), None).otherwise(
            self.remove_special_characters(
                self.remove_extra_spaces(
                    self.to_lowercase(
                        self.remove_accents(
                            self.clean_stopwords(text_col)
                        )
                    )
                )
            )
        )

    def clean_id_number(self, text_col, doc_type_col):
        """
        Clean ID number using native Spark functions.

        Parameters:
            text_col: Spark column
            doc_type_col: Spark column

        Returns:
            Cleaned ID number column
        """
        return when(text_col.isNull(), None).otherwise(
            self.remove_letters_if_document(
                self.remove_nit_verification_digit(
                    self.remove_extra_spaces(
                        self.only_alphanumeric(text_col)
                    )
                ),
                doc_type_col
            )
        )

    def clean_names(self, text_col):
        """
        Clean names using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Cleaned names column
        """
        return when(text_col.isNull(), None).otherwise(
            self.remove_extra_spaces(
                when(regexp_extract(text_col, "[a-zA-Z]", 0) != "",
                     regexp_replace(
                         self.remove_accents(
                             self.to_uppercase(
                                 self.remove_special_characters(text_col)
                             )
                         ), "[0-9]", ""
                     )
                ).otherwise(
                    self.remove_accents(
                        self.to_uppercase(
                            self.remove_special_characters(text_col)
                        )
                    )
                )
            )
        )

    def clean_full_name(self, text_col):
        """
        Clean full name using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Cleaned full name column
        """
        return when(text_col.isNull(), None).otherwise(
            trim(self.replace_multiple_spaces(text_col))
        )

    def clean_business_name(self, text_col):
        """
        Clean business name using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Cleaned business name column
        """
        return when(text_col.isNull(), None).otherwise(
            trim(
                self.replace_multiple_spaces(
                    self.remove_accents(
                        self.to_uppercase(text_col)
                    )
                )
            )
        )

    def clean_date(self, date_col):
        """
        Clean date using native Spark functions.

        Parameters:
            date_col: Spark column

        Returns:
            Cleaned date column
        """
        return when(date_col.isNull(), None).otherwise(
            self.change_invalid_date_to_null(
                self.adjust_invalid_date(
                    self.adjust_day_month_out_of_range(date_col)
                )
            )
        )

    def clean_date_no_null(self, date_col):
        """
        Clean date without nulls using native Spark functions.

        Parameters:
            date_col: Spark column

        Returns:
            Cleaned date column
        """
        return when(date_col.isNull(), None).otherwise(
            self.adjust_invalid_date(
                self.adjust_day_month_out_of_range(
                    self.remove_extra_spaces(
                        self.remove_special_characters(
                            date_format(date_col, "dd/MM/yyyy")
                        )
                    )
                )
            )
        )

 
    def clean_city(self, text_col):
        """
        Clean city using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Cleaned city column
        """
        # Regex para detectar números enteros o decimales tipo .0
        integer_regex = r'^\d+(\.0+)?$'

        return when(text_col.isNull(), None).otherwise(
            when(trim(text_col) == "", text_col).otherwise(
                when(regexp_extract(text_col, integer_regex, 0) != "",
                     # Elimina los decimales tipo .0 antes de convertir
                     lpad(regexp_replace(text_col, r'\.0+$', '').cast("int").cast("string"), 5, "0")
                ).otherwise(
                    self.remove_extra_spaces(
                        self.remove_accents(
                            self.to_uppercase(
                                self.remove_special_characters(text_col)
                            )
                        )
                    )
                )
            )
        )
    def clean_country(self, text_col):
        """
        Clean country using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Cleaned country column
        """
        return when(text_col.isNull(), None).otherwise(
            self.remove_extra_spaces(
                self.remove_accents(
                    self.to_lowercase(
                        self.remove_special_characters(text_col)
                    )
                )
            )
        )

    def clean_city_SA(self, text_col):
        """
        Clean country using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Cleaned country column
        """
        return when(text_col.isNull(), None).otherwise(initcap(trim(text_col))
        )    

    def clean_text_lower(self, text_col):
        """
        Clean text and convert to lowercase using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Cleaned lowercase text column
        """
        return when(text_col.isNull(), None).otherwise(
            when(trim(text_col) == "", text_col).otherwise(
                trim(
                    regexp_replace(
                        regexp_replace(
                            self.to_lowercase(self.remove_accents(
                                regexp_replace(text_col, "[\x00-\x1F\x7F-\x9F]", "")
                            )), "[^\\w\\s\\u00C0-\\u017F]", ""
                        ), "\\s+", " "
                    )
                ).cast("string")
            )
        ).cast("string")

    def clean_nationality_numeric_code(self,text_col):
        """
        Limpia códigos numéricos de nacionalidad:
        - Elimina espacios
        - Elimina caracteres no numéricos excepto el signo negativo
        - Convierte a string
        - Mantiene valores como -1 y 0

        Parámetros:
            text_col: columna de Spark

        Retorna:
            Columna limpia como string
        """
        return regexp_replace(trim(text_col.cast("string")), "[^0-9-]", "")

    def clean_address(self, text_col):
        """
        Clean address using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Cleaned address column
        """
        return when(text_col.isNull(), None).otherwise(
            trim(
                self.remove_extra_spaces(
                    self.remove_accents(
                        self.to_uppercase(
                            self.remove_special_characters(text_col)
                        )
                    )
                )
            )
        )

    def clean_gender(self, text_col):
        """
        Clean gender using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Cleaned gender column
        """
        return when(text_col.isNull(), None).otherwise(
            self.remove_extra_spaces(
                self.remove_accents(
                    self.to_lowercase(
                        self.remove_special_characters(text_col)
                    )
                )
            )
        )

    def clean_email_address(self, email_col):
        """
        Clean email address using native Spark functions.

        Parameters:
            email_col: Spark column

        Returns:
            Cleaned email column
        """
        return when(email_col.isNull(), None).otherwise(
            self.clean_email(email_col)
        )

    def clean_phone_number(self, text_col):
        """
        Clean phone number using native Spark functions.
    
        Parameters:
            text_col: Spark column
    
        Returns:
            Cleaned phone number column
        """
        text_col_str = text_col.cast("string")  
        only_digits = self.only_numbers(text_col_str)
    
        return when(text_col.isNull(), None).otherwise(
            when(length(only_digits) > 10,
                 F.substring(only_digits, 1, 10)
            ).otherwise(
                self.remove_extra_spaces(only_digits)
            )
        )
    def clean_extension(self, text_col):
        """
        Clean extension using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Cleaned extension column
        """
        return when(text_col.isNull(), None).otherwise(
            self.only_numbers(text_col)
        )

    def clean_children_count(self, value_col):
        """
        Clean children count using native Spark functions.

        Parameters:
            value_col: Spark column

        Returns:
            Cleaned children count column
        """
        return when(value_col.isNull(), None).otherwise(
            self.round_up_number(
                self.absolute_value(
                    self.only_numbers(value_col).cast("double")
                )
            )
        )

    def clean_company_nit(self, text_col, doc_type_col):
        """
        Clean company NIT using native Spark functions.

        Parameters:
            text_col: Spark column
            doc_type_col: Spark column

        Returns:
            Cleaned company NIT column
        """
        return when(text_col.isNull(), None).otherwise(
            self.remove_letters_if_document(
                self.remove_extra_spaces(
                    regexp_replace(text_col, "[^a-zA-Z0-9\\-_]", "")
                ),
                doc_type_col
            )
        )

    def clean_company_name(self, text_col):
        """
        Clean company name using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Cleaned company name column
        """
        return when(text_col.isNull(), None).otherwise(
            self.remove_extra_spaces(
                self.remove_accents(
                    self.to_uppercase(text_col)
                )
            )
        )

    def clean_company_position(self, text_col):
        """
        Clean company position using native Spark functions.

        Parameters:
            text_col: Spark column

        Returns:
            Cleaned company position column
        """
        return when(text_col.isNull(), None).otherwise(
            self.remove_extra_spaces(
                self.remove_accents(text_col)
            )
        )

    def clean_company_entry_date(self, date_col):
        """
        Clean company entry date using native Spark functions.

        Parameters:
            date_col: Spark column

        Returns:
            Cleaned date column
        """
        return self.clean_date(date_col)

    def clean_phone_number_by_type(self, number_col, phone_type_col):
        """
        Clean phone number by type using native Spark functions.

        Parameters:
            number_col: Spark column
            phone_type_col: Spark column

        Returns:
            Cleaned phone number column
        """
        return when(number_col.isNull(), None).otherwise(
            F.substr(
                regexp_replace(trim(number_col.cast("string")), "[^0-9]", ""),
                1, 10
            )
        )

clean = CleanUp()
df_original = df_unico.withColumn("departamento", clean.clean_city(col("departamento"))) 

df_unico.show()
df_original.show()
df_basic_data_STG_clean = df_basic_data_STG_clean.withColumn("cod_tipo_identificacion", F.trim(F.col("cod_tipo_identificacion")))
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
 
# Crear la sesión de Spark
spark = SparkSession.builder.appName("CrearDataFrame").getOrCreate()
 
# Definir el esquema del DataFrame
schema = StructType([
    StructField("tabla", StringType(), True),
    StructField("campo_referencia", StringType(), True),
    StructField("valor_referencia", StringType(), True),
    StructField("campo_homologacion", StringType(), True),
    StructField("valor_homologacion", StringType(), True),
    StructField("cod_fuente_fk", StringType(), True)
])
 
# Datos
data = [

    ("clientes_datos_basicos_stg", "cod_tipo_identificacion", "13.0", "cod_tipo_identificacion", "1", "1"),
    ("clientes_datos_basicos_stg", "cod_tipo_identificacion", "registro civil", "cod_tipo_identificacion", "12", "3"),
    ("clientes_datos_basicos_stg", "cod_tipo_identificacion", "12", "cod_tipo_identificacion", "6", "2"),
    ("clientes_datos_basicos_stg", "tipo_empresa", "privada", "tipo_empresa", "2", ""),
    ("clientes_datos_ubicacion_y_contactabilidad_stg", "departamento", "bogota", "tipo_empresa", "2", ""),
    ("clientes_datos_sociodemograficos_stg", "genero", "M", "genero", "1", ""),
    ("clientes_datos_sociodemograficos_stg", "genero", "m", "genero", "1", "")

]
 
# Crear el DataFrame
df_homologaciones = spark.createDataFrame(data, schema)
 
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
                    Homologaciones Manager - Data Harmonization Engine
================================================================================
Description:
    OPTIMIZED VERSION - Specialized class for data homologation and remediation.
    Handles large-scale data transformations (12M+ records) efficiently.
    Designed for deterministic behavior without expensive operations.
   
    VERSION 4.0 - Added source filtering capability (cod_fuente_fk)
   
Author:         Maria Cardona (Original) / Optimized Version
Created:        2025-08-10
Last Updated:   2025-01-24 - Added cod_fuente_fk filtering
Version:        4.0.0 - Performance optimized with source filtering
Environment:    Production
Dependencies:   pyspark, logging
Class:          DataHarmonizationManager
================================================================================
"""
 
## ----- SPARK LIBRARIES ------------------ ###
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
 
## ----- PYTHON LIBRARIES ------------------ ###
import logging
from typing import Dict, List, Set, Optional
import time
 
 
class DataHarmonizationManager:
    """
    OPTIMIZED Manager class for large-scale data homologation and remediation.
   
    KEY OPTIMIZATIONS:
    1. No count() or show() operations
    2. Broadcast joins for small rule sets
    3. Column-based operations instead of row-by-row
    4. Lazy evaluation maximized
    5. Deterministic transformations without IDs
    6. Source-based filtering with cod_fuente_fk
    """
   
    def __init__(self, debug_mode: bool = False):
        """
        Initialize HomologacionesManager.
       
        Args:
            debug_mode: Enable minimal logging (no counts or shows)
        """
        self.debug_mode = debug_mode
        self.processed_fields = []
        self.start_time = None
        logging.info("DataHarmonizationManager v4.0 initialized (WITH SOURCE FILTERING)")
   
    def _log_safely(self, message: str, level: str = "info"):
        """Log messages without triggering actions on DataFrames."""
        if self.debug_mode:
            if level == "info":
                logging.info(message)
            elif level == "debug":
                logging.debug(message)
            elif level == "warning":
                logging.warning(message)
            elif level == "error":
                logging.error(message)
   
    def _prepare_rules_broadcast(self, df_homologaciones: DataFrame,
                                table: str, campo: str, tipo: str,
                                source_column: Optional[str] = None) -> DataFrame:
        """
        Prepare and broadcast rules for efficient joining.
        Rules are typically small and can be broadcast.
       
        Args:
            df_homologaciones: DataFrame with homologation rules
            table: Table name
            campo: Field to process
            tipo: 'homologacion' or 'remediacion'
            source_column: Source column name if exists in data (e.g., 'cod_fuente_fk')
       
        Returns:
            Filtered and prepared rules DataFrame
        """
        # Filter and clean rules WITHOUT triggering actions
        df_rules = df_homologaciones.filter(
            (F.lower(F.trim(F.col("tabla"))) == table.lower()) &
            (F.lower(F.trim(F.col("campo_referencia"))) == campo.lower()) &
            F.col("valor_referencia").isNotNull() &
            F.col("valor_homologacion").isNotNull()
        )
       
        # Select columns including cod_fuente_fk if it exists
        columns_to_select = [
            F.upper(F.trim(F.col("valor_referencia"))).alias("valor_ref_normalizado"),
            F.col("valor_homologacion").alias("valor_nuevo"),
            F.col("campo_homologacion").alias("campo_destino")
        ]
       
        # Add cod_fuente_fk if it exists in the rules table
        if "cod_fuente_fk" in df_homologaciones.columns:
            columns_to_select.append(F.col("cod_fuente_fk").alias("source_filter"))
            self._log_safely(f"cod_fuente_fk found in rules table for field '{campo}'", "debug")
       
        df_rules = df_rules.select(*columns_to_select).distinct()
       
        # Determine if it's homologation or remediation
        if tipo == "homologacion":
            df_rules = df_rules.filter(
                F.lower(F.col("campo_destino")) == campo.lower()
            )
        else:  # remediacion
            df_rules = df_rules.filter(
                F.lower(F.col("campo_destino")) != campo.lower()
            )
       
        # Broadcast hint for small datasets (rules are typically small)
        df_rules = F.broadcast(df_rules)
       
        return df_rules
   
    def _create_mapping_expr_with_source(self, df_rules_collected: List, campo: str,
                                        source_column: Optional[str], tipo: str) -> F.Column:
        """
        Create a CASE WHEN expression for efficient mapping without joins,
        considering source filtering with proper empty string handling.
        """
        if not df_rules_collected:
            return F.col(campo)
       
        # Build CASE WHEN expression
        case_expr = F.col(campo)
       
        for rule in df_rules_collected:
            valor_ref = rule["valor_ref_normalizado"]
            valor_nuevo = rule["valor_nuevo"]
           
            # Base condition for matching (case-insensitive, trimmed)
            condition = F.upper(F.trim(F.col(campo))) == F.lit(valor_ref)
           
            # CORRECCIÓN: Manejo mejorado para None Y strings vacíos
            if source_column and "source_filter" in rule:
                source_value = rule["source_filter"]
               
                # Verificar si es regla universal (None, "", o string con solo espacios)
                is_universal_rule = (
                    source_value is None or
                    source_value == "" or
                    (isinstance(source_value, str) and source_value.strip() == "")
                )
               
                if not is_universal_rule:
                    # Solo añadir condición de fuente si NO es regla universal
                    source_condition = F.col(source_column) == F.lit(source_value)
                    condition = condition & source_condition
                    self._log_safely(f"Source-specific rule: {source_column} == {source_value}", "debug")
                else:
                    self._log_safely(f"Universal rule (applies to all sources) for value: {valor_ref}", "debug")
           
            case_expr = F.when(condition, F.lit(valor_nuevo)).otherwise(case_expr)
       
        return case_expr
 
    def homologacion_optimizada(self, df: DataFrame, campo: str,
                            df_rules: DataFrame, tipo: str,
                            source_column: Optional[str] = None) -> DataFrame:
        """
        Optimized homologation with proper NULL/empty cod_fuente_fk handling.
        """
        try:
            # Collect rules ONLY if small enough (rules are typically <1000 rows)
            rules_collected = df_rules.limit(10000).collect()
           
            if not rules_collected:
                self._log_safely(f"No rules found for field '{campo}'", "debug")
                return df
           
            # Get destination field from first rule
            campo_destino = rules_collected[0]["campo_destino"].lower() if rules_collected else campo
           
            # Check if we have source filtering rules
            has_source_filter = any("source_filter" in rule and rule["source_filter"] is not None
                                for rule in rules_collected)
           
            if has_source_filter and source_column:
                self._log_safely(f"Processing with source filtering on column '{source_column}'", "info")
           
            if tipo == "homologacion":
                # Direct replacement using CASE WHEN (more efficient than JOIN for small rule sets)
                if len(rules_collected) <= 100:
                    # For small rule sets, use CASE WHEN
                    mapping_expr = self._create_mapping_expr_with_source(
                        rules_collected, campo, source_column, tipo
                    )
                    df = df.withColumn(campo, mapping_expr)
                else:
                    # For larger rule sets, use broadcast join
                    df = df.withColumn("__temp_match__", F.upper(F.trim(F.col(campo))))
                   
                    # Build join condition
                    join_condition = F.col("__temp_match__") == F.col("valor_ref_normalizado")
                   
                    # CORRECCIÓN: Add source filtering to join with empty string handling
                    if source_column and "source_filter" in df_rules.columns:
                        # CORRECCIÓN: Incluir strings vacíos como reglas universales
                        source_join = (
                            F.col("source_filter").isNull() |                    # None
                            (F.col("source_filter") == F.lit("")) |              # String vacío
                            (F.trim(F.col("source_filter")) == F.lit("")) |      # Solo espacios
                            (F.col(source_column) == F.col("source_filter"))     # Match específico
                        )
                        join_condition = join_condition & source_join
                   
                    df = df.join(
                        df_rules,
                        join_condition,
                        "left"
                    )
                   
                    df = df.withColumn(
                        campo,
                        F.coalesce(F.col("valor_nuevo"), F.col(campo))
                    )
                   
                    # Clean up temporary columns
                    cols_to_drop = ["__temp_match__", "valor_ref_normalizado",
                                "valor_nuevo", "campo_destino"]
                    if "source_filter" in df.columns:
                        cols_to_drop.append("source_filter")
                    df = df.drop(*cols_to_drop)
               
                self._log_safely(f"Applied homologation to field '{campo}'", "info")
               
            else:  # remediacion
                # Ensure destination field exists
                if campo_destino not in [c.lower() for c in df.columns]:
                    df = df.withColumn(campo_destino, F.lit(None).cast(StringType()))
                    self._log_safely(f"Created new field '{campo_destino}' for remediation", "debug")
               
                # Find actual column name (case-sensitive)
                actual_campo_destino = None
                for col in df.columns:
                    if col.lower() == campo_destino:
                        actual_campo_destino = col
                        break
               
                if not actual_campo_destino:
                    actual_campo_destino = campo_destino
               
                # Apply remediation
                if len(rules_collected) <= 100:
                    # Build mapping for remediation with improved source filtering
                    case_expr = F.col(actual_campo_destino)
                    for rule in rules_collected:
                        valor_ref = rule["valor_ref_normalizado"]
                        valor_nuevo = rule["valor_nuevo"]
                        condition = F.upper(F.trim(F.col(campo))) == F.lit(valor_ref)
                       
                        # CORRECCIÓN: Add source filtering for remediation with empty string handling
                        if source_column and "source_filter" in rule:
                            source_value = rule["source_filter"]
                           
                            # Verificar si es regla universal (None, "", o string con solo espacios)
                            is_universal_rule = (
                                source_value is None or
                                source_value == "" or
                                (isinstance(source_value, str) and source_value.strip() == "")
                            )
                           
                            if not is_universal_rule:
                                # Solo añadir condición de fuente si NO es regla universal
                                source_condition = F.col(source_column) == F.lit(source_value)
                                condition = condition & source_condition
                       
                        case_expr = F.when(condition, F.lit(valor_nuevo)).otherwise(case_expr)
                   
                    df = df.withColumn(actual_campo_destino, case_expr)
                else:
                    # Use broadcast join for larger rule sets
                    df = df.withColumn("__temp_match__", F.upper(F.trim(F.col(campo))))
                   
                    # Build join condition
                    join_condition = F.col("__temp_match__") == F.col("valor_ref_normalizado")
                   
                    # CORRECCIÓN: Add source filtering to join for remediation with empty string handling
                    if source_column and "source_filter" in df_rules.columns:
                        source_join = (
                            F.col("source_filter").isNull() |                    # None
                            (F.col("source_filter") == F.lit("")) |              # String vacío
                            (F.trim(F.col("source_filter")) == F.lit("")) |      # Solo espacios
                            (F.col(source_column) == F.col("source_filter"))     # Match específico
                        )
                        join_condition = join_condition & source_join
                   
                    df = df.join(
                        df_rules,
                        join_condition,
                        "left"
                    )
                   
                    df = df.withColumn(
                        actual_campo_destino,
                        F.coalesce(F.col("valor_nuevo"), F.col(actual_campo_destino))
                    )
                   
                    # Clean up temporary columns
                    cols_to_drop = ["__temp_match__", "valor_ref_normalizado",
                                "valor_nuevo", "campo_destino"]
                    if "source_filter" in df.columns:
                        cols_to_drop.append("source_filter")
                    df = df.drop(*cols_to_drop)
               
                self._log_safely(f"Applied remediation from '{campo}' to '{actual_campo_destino}'", "info")
           
            return df
           
        except Exception as e:
            self._log_safely(f"Error in homologacion_optimizada: {str(e)}", "error")
            return df
   
    def homologacion_i(self, table: str, df_homologaciones_dos: DataFrame,
                      df_homologaciones_principal: DataFrame, df: DataFrame,
                      tipo: str, source_column: Optional[str] = None) -> DataFrame:
        """
        Main orchestrator optimized for large datasets with source filtering.
       
        Args:
            table: Table name to process
            df_homologaciones_dos: Secondary homologation rules
            df_homologaciones_principal: Primary transformation rules
            df: DataFrame to transform (can be 12M+ records)
            tipo: 'homologacion' or 'remediacion'
            source_column: Column name in df for source filtering (e.g., 'cod_fuente_fk')
       
        Returns:
            Transformed DataFrame (lazy evaluation maintained)
        """
        try:
            self.start_time = time.time()
            table = table.lower().strip()
           
            self._log_safely(f"Starting {tipo} for table '{table}'", "info")
           
            # Check if source column exists in main DataFrame
            if source_column and source_column not in df.columns:
                self._log_safely(
                    f"Warning: source column '{source_column}' not found in DataFrame. "
                    f"Proceeding without source filtering.", "warning"
                )
                source_column = None
           
            # Get unique fields to process from rules
            campos_procesar = (
                df_homologaciones_principal
                .filter(
                    (F.lower(F.trim(F.col("tabla"))) == table) &
                    F.col("campo_referencia").isNotNull()
                )
                .select(F.lower(F.trim(F.col("campo_referencia"))).alias("campo"))
                .distinct()
                .collect()
            )
           
            if not campos_procesar:
                self._log_safely(f"No fields to process for table '{table}'", "warning")
                return df
           
            # Get DataFrame columns (lowercase for comparison)
            df_columns_lower = [col.lower() for col in df.columns]
            campos_to_process = [row["campo"] for row in campos_procesar]
           
            self._log_safely(
                f"Fields to process: {campos_to_process[:10]}{'...' if len(campos_to_process) > 10 else ''}",
                "info"
            )
           
            # Process each field
            processed_count = 0
            for campo in campos_to_process:
                if campo in df_columns_lower:
                    # Get actual column name preserving case
                    actual_column = df.columns[df_columns_lower.index(campo)]
                   
                    # Prepare broadcast rules for this field
                    df_rules = self._prepare_rules_broadcast(
                        df_homologaciones_principal, table, campo, tipo, source_column
                    )
                   
                    # Apply transformation with source filtering
                    df = self.homologacion_optimizada(
                        df, actual_column, df_rules, tipo, source_column
                    )
                    processed_count += 1
                   
                else:
                    self._log_safely(f"Field '{campo}' not found in DataFrame", "debug")
           
            elapsed_time = time.time() - self.start_time
            self._log_safely(
                f"Completed {tipo} for table '{table}': "
                f"processed {processed_count} fields in {elapsed_time:.2f} seconds",
                "info"
            )
           
            return df
           
        except Exception as e:
            self._log_safely(f"Error in homologacion_i: {str(e)}", "error")
            return df
   
    def apply_multiple_harmonizations(self, df: DataFrame,
                                     harmonizations: List[Dict],
                                     table: str,
                                     source_column: Optional[str] = None) -> DataFrame:
        """
        Apply multiple harmonizations efficiently in sequence.
       
        Args:
            df: DataFrame to process
            harmonizations: List of dicts with 'rules' and 'tipo' keys
            table: Table name
            source_column: Column name in df for source filtering
       
        Returns:
            Transformed DataFrame
        """
        for idx, harm in enumerate(harmonizations):
            self._log_safely(f"Applying harmonization {idx + 1}/{len(harmonizations)}", "info")
           
            df = self.homologacion_i(
                table=table,
                df_homologaciones_dos=harm['rules'],
                df_homologaciones_principal=harm['rules'],
                df=df,
                tipo=harm['tipo'],
                source_column=source_column
            )
       
        return df
 
 
# Utility functions for validation without triggering actions
def create_validation_sample(df: DataFrame, sample_fraction: float = 0.0001) -> DataFrame:
    """
    Create a small sample for validation without processing entire dataset.
   
    Args:
        df: Large DataFrame
        sample_fraction: Fraction to sample (0.0001 = 0.01% for 12M records = 1200 records)
   
    Returns:
        Small sample for validation
    """
    return df.sample(withReplacement=False, fraction=sample_fraction, seed=42)
 
 
def validate_rules_format(df_rules: DataFrame, check_source: bool = False) -> bool:
    """
    Validate rules DataFrame format without triggering expensive operations.
   
    Args:
        df_rules: Rules DataFrame
        check_source: Whether to check for cod_fuente_fk column
   
    Returns:
        Boolean indicating if format is valid
    """
    required_columns = {"tabla", "campo_referencia", "campo_homologacion",
                       "valor_referencia", "valor_homologacion"}
   
    if check_source:
        required_columns.add("cod_fuente_fk")
   
    actual_columns = set(df_rules.columns)
   
    if not required_columns.issubset(actual_columns):
        missing = required_columns - actual_columns
        logging.error(f"Missing required columns in rules: {missing}")
        return False
   
    return True
 
 
def optimize_dataframe_for_joins(df: DataFrame, join_columns: List[str]) -> DataFrame:
    """
    Optimize DataFrame for upcoming joins by repartitioning if needed.
   
    Args:
        df: DataFrame to optimize
        join_columns: Columns that will be used for joining
   
    Returns:
        Optimized DataFrame
    """
    # Only repartition if explicitly needed
    # Don't trigger this automatically as it can be expensive
   
    # Option 1: Repartition by join columns (commented out by default)
    # df = df.repartition(*[F.col(c) for c in join_columns])
   
    # Option 2: Just return as-is and let Spark optimize
    return df
 
 
    return result_df    
homol = DataHarmonizationManager()
                    
df = homol.homologacion_i(
    "clientes_datos_sociodemograficos_stg",
    df_homologaciones,
    df_homologaciones,
    df_unico,
    "homologacion",
    "cod_fuente_fk"
)



df.show()
df_homologado.show()
df.filter(F.col("cod_tipo_identificacion") != F.trim(F.col("cod_tipo_identificacion"))).show()
bucket_silver = 'alfa-datalake-dev-silver'
BUCKET_BRONZE = 'alfa-datalake-dev-bronce'
BUCKET_GOLD = 'alfa-datalake-dev-gold'
def load_homologation_data(s3_homologation_path):
    """Load homologation data."""
    cleanup = CleanUp()
    df_homologaciones = spark.read.csv(s3_homologation_path, header=True, inferSchema=True, sep=";")
    df_cod_tipo = df_homologaciones.filter(col("reference_field") == "cod_tipo_identificacion")
    df_otros = df_homologaciones.filter(col("reference_field") != "cod_tipo_identificacion")
    df_cod_tipo = cleanup.normalize_columns(df_cod_tipo, "reference_value")
    return df_cod_tipo.unionByName(df_otros)
df_homologaciones = spark.read.csv("s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_silver/definitions/Homologaciones-clientes-test-v.csv", header=True, inferSchema=True, sep=";")
arch = df_homologaciones.filter(col("reference_field") == "genero").show(truncate=False)
print(f"archivo homologaciones {arch}")

df_clean = utils.read_full_parquet(spark, "s3://alfa-datalake-dev-silver/CLIENTES/DATOS_BASICOS/CLIENTES_DATOS_BASICOS_STG/")
df_clean= df_clean.select("primer_nombre","segundo_nombre","primer_apellido","segundo_apellido","nombre_completo","cod_fuente_fk").filter(F.col("numero_documento").isin("1075297262","52854480", "1012400955", "1112494204"))
 
def split_name(df, campo_origen="primer_nombre"):
        """
        Función optimizada con cache y count para forzar evaluación
        en DataFrames grandes sin usar checkpoint
        """
 
        es_fuente_uno = (F.col("cod_fuente_fk") == 1)
 
        # Normalización del campo de origen
        df = df.withColumn(
            f"{campo_origen}_limpio",
            F.upper(
                F.trim(
                    F.regexp_replace(
                        F.regexp_replace(F.col(campo_origen), r"\s+", " "),
                        r"[^\w\sáéíóúÁÉÍÓÚñÑüÜ.-]",
                        ""
                    )
                )
            )
        )
 
        # Condiciones de procesamiento
        debe_procesar_primer_nombre = (
            (F.col("cod_fuente_fk") == 1) &
            (F.col(campo_origen).isNotNull()) &
            (F.length(F.trim(F.col(campo_origen))) > 0) &
            (F.size(F.split(F.col(f"{campo_origen}_limpio"), " ")) >= 2)
        )
 
        debe_procesar_nombre_completo = (
            (F.col("cod_fuente_fk") == 1) &
            (
                (F.col(campo_origen).isNull()) |
                (F.length(F.trim(F.col(campo_origen))) == 0) |
                (F.size(F.split(F.col(f"{campo_origen}_limpio"), " ")) < 2)
            ) &
            (F.col("nombre_completo").isNotNull()) &
            (F.length(F.trim(F.col("nombre_completo"))) > 0) &
            (F.size(F.split(F.upper(F.trim(F.col("nombre_completo"))), " ")) >= 2)
        )
 
        # Preparación de datos con cache inmediato
        df = df.withColumn(
            "fuente_texto",
            F.when(debe_procesar_nombre_completo, F.upper(F.trim(F.col("nombre_completo"))))
            .otherwise(F.col(f"{campo_origen}_limpio"))
        ).withColumn(
            "partes_array", F.split(F.col("fuente_texto"), " ")
        ).withColumn(
            "num_partes", F.size(F.col("partes_array"))
        ).withColumn(
            "debe_procesar", debe_procesar_primer_nombre | debe_procesar_nombre_completo
        )
 
        # *** FORZAR EVALUACIÓN CON CACHE ***
        df = df.cache()
        df.count()  # Materializa el DataFrame
 
        # *** ESTRATEGIA: Procesar todos los campos en UNA SOLA operación withColumn ***
        df = df.select(
            "*",
            # PRIMER NOMBRE
            F.when(F.col("debe_procesar"), F.col("partes_array")[0])
            .otherwise(F.col("primer_nombre")).alias("primer_nombre_new"),
       
            # SEGUNDO NOMBRE  
            F.when(F.col("debe_procesar") & (F.col("num_partes") == 2), F.lit(None).cast("string"))
            .when(F.col("debe_procesar") & (F.col("num_partes") >= 3), F.col("partes_array")[1])
            .otherwise(F.col("segundo_nombre")).alias("segundo_nombre_new"),
       
            # PRIMER APELLIDO
            F.when(F.col("debe_procesar") & (F.col("num_partes") == 2), F.col("partes_array")[1])
            .when(F.col("debe_procesar") & (F.col("num_partes") == 3), F.col("partes_array")[2])
            .when(F.col("debe_procesar") & (F.col("num_partes") >= 4), F.col("partes_array")[2])
            .otherwise(F.col("primer_apellido")).alias("primer_apellido_new"),
       
            # SEGUNDO APELLIDO
            F.when(F.col("debe_procesar") & (F.col("num_partes") <= 3), F.lit(None).cast("string"))
            .when(F.col("debe_procesar") & (F.col("num_partes") >= 4), F.col("partes_array")[3])
            .otherwise(F.col("segundo_apellido")).alias("segundo_apellido_new")
        )
 
        # *** FORZAR EVALUACIÓN OTRA VEZ ***
        df = df.cache()
        df.count()
 
        # Reemplazar campos originales
        df = df.drop("primer_nombre", "segundo_nombre", "primer_apellido", "segundo_apellido") \
            .withColumnRenamed("primer_nombre_new", "primer_nombre") \
            .withColumnRenamed("segundo_nombre_new", "segundo_nombre") \
            .withColumnRenamed("primer_apellido_new", "primer_apellido") \
            .withColumnRenamed("segundo_apellido_new", "segundo_apellido")
 
        # Generar nombre completo
        df = df.withColumn(
            "nombre_completo_generado",
            F.trim(
                F.concat_ws(
                    " ",
                    F.when((F.col("primer_nombre").isNotNull()) & (F.length(F.trim(F.col("primer_nombre"))) > 0),
                        F.trim(F.col("primer_nombre"))),
                    F.when((F.col("segundo_nombre").isNotNull()) & (F.length(F.trim(F.col("segundo_nombre"))) > 0),
                        F.trim(F.col("segundo_nombre"))),
                    F.when((F.col("primer_apellido").isNotNull()) & (F.length(F.trim(F.col("primer_apellido"))) > 0),
                        F.trim(F.col("primer_apellido"))),
                    F.when((F.col("segundo_apellido").isNotNull()) & (F.length(F.trim(F.col("segundo_apellido"))) > 0),
                        F.trim(F.col("segundo_apellido")))
                )
            )
        )
 
        # Actualización final de nombre completo
        df = df.withColumn(
            "nombre_completo",
            F.when(
                es_fuente_uno & (
                    (F.col("nombre_completo").isNull()) | (F.length(F.trim(F.col("nombre_completo"))) == 0)
                ),
                F.col("nombre_completo_generado")
            ).when(
                es_fuente_uno & (
                    F.size(F.split(F.trim(F.col("nombre_completo_generado")), " ")) >
                    F.size(F.split(F.trim(F.col("nombre_completo")), " "))
                ),
                F.col("nombre_completo_generado")
            ).when(
                es_fuente_uno & (
                    F.length(F.trim(F.col("nombre_completo_generado"))) >
                    (F.length(F.trim(F.col("nombre_completo"))) + 10)
                ),
                F.col("nombre_completo_generado")
            ).otherwise(
                F.when(es_fuente_uno, F.upper(F.trim(F.col("nombre_completo"))))
                .otherwise(F.col("nombre_completo"))
            )
        )
 
        # Limpiar columnas temporales
        df = df.drop(
            f"{campo_origen}_limpio",
            "fuente_texto",
            "partes_array",
            "num_partes",
            "debe_procesar",
            "nombre_completo_generado"
        )
 
        # Cache final y forzar evaluación
        df = df.cache()
        df.count()
   
        return df
 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
 
# Crear sesión de Spark
spark = SparkSession.builder \
   .appName("EjemplosSplitName") \
   .getOrCreate()
 
# Definir esquema
schema = StructType([
   StructField("id", IntegerType(), True),
   StructField("cod_fuente_fk", IntegerType(), True),
   StructField("primer_nombre", StringType(), True),
   StructField("segundo_nombre", StringType(), True),
   StructField("primer_apellido", StringType(), True),
   StructField("segundo_apellido", StringType(), True),
   StructField("nombre_completo", StringType(), True),
   StructField("descripcion_caso", StringType(), True)
])
 
# Datos de ejemplo ANTES del procesamiento
data_antes = [
   # Caso 1: Todo en primer_nombre (2 palabras)
   (1, 1, "Juan Perez", None, None, None, None, "2 palabras en primer_nombre"),
  
   # Caso 2: Todo en primer_nombre (3 palabras)
   (2, 1, "Juan Carlos Perez", None, None, None, None, "3 palabras en primer_nombre"),
  
   # Caso 3: Todo en primer_nombre (4 palabras)
   (3, 1, "Maria Antonia Bermudez Cardona", None, None, None, None, "4 palabras en primer_nombre"),
  
   # Caso 4: Todo en primer_nombre (5+ palabras)
   (4, 1, "Maria Del Carmen Perez Gonzalez", None, None, None, None, "5 palabras en primer_nombre"),
  
   # Caso 5: Solo nombre_completo (3 palabras)
   (5, 1, None, None, None, None, "Ana Sofia Rodriguez", "3 palabras en nombre_completo"),
  
   # Caso 6: Solo nombre_completo (4 palabras)
   (6, 1, None, None, None, None, "Luis Fernando Garcia Lopez", "4 palabras en nombre_completo"),
  
   # Caso 7: Datos parciales - NO se procesa
   (7, 1, "Carlos", None, "Martinez", None, "Carlos Martinez", "Datos parciales - NO procesa"),
  
   # Caso 8: Ya tiene todos los campos - NO se procesa
   (8, 1, "Pedro", "Pablo", "Ruiz", "Diaz", "Pedro Pablo Ruiz Diaz", "Datos completos - NO procesa"),
  
   # Caso 9: Fuente diferente - NO se procesa
   (9, 2, "Roberto Sanchez Martinez", None, None, None, None, "Fuente 2 - NO procesa"),
  
   # Caso 10: Solo 1 palabra - NO se procesa
   (10, 1, "Pedro", None, None, None, None, "1 palabra - NO procesa"),
  
   # Caso 11: Nombre con preposiciones (problema)
   (11, 1, "Juan De La Rosa", None, None, None, None, "Apellido con preposición"),
  
   # Caso 12: Nombre compuesto común (problema)
   (12, 1, "Maria Jose Rodriguez Perez", None, None, None, None, "Nombre compuesto común")
]
 
# Crear DataFrame
df_antes = spark.createDataFrame(data_antes, schema)
cleanup = CleanUp()
df_clean=df_clean.withColumn("primer_nombre", cleanup.clean_names(col("primer_nombre")))
df_clean.show(truncate=False)
split_name(df_clean).show(truncate=False)
df_clean.show()

from pyspark.sql.functions import lower, trim

# Aplicar lower y trim al campo 'genero'
df_clean = df_clean.withColumn("genero", trim(lower(df_clean["genero"])))

df_clean.show()
harmonization_manager = DataHarmonizationManager()
df_harmonized = harmonization_manager.apply_harmonization(
    table_name="clientes_datos_sociodemograficos_stg",
    df_homologaciones_dos=df_homologaciones,
    df_homologaciones_principal=df_homologaciones,
    df=df_clean,
    tipo="harmonization"
)
def populate_gender_when_null(df):
        """
        populate the gender field when it is null or does not have an expected value.
 
        Parameters:
            df
 
        Returns:
            df.
        """
        default_value = "9"
   
        result_df = df.withColumn(
            "genero",
            F.when(
                (F.col("genero").isin("1", "2","3","4","9")),
                F.col("genero")  # conservar si es válido
            ).otherwise(
                F.when(
                    F.length(F.col("numero_documento")) <= 8,
                    F.when(
                        (F.col("numero_documento").cast("int").between(10000, 19999999)) |
                        (F.col("numero_documento").cast("int").between(70000000, 79999999)),
                        "2"
                    ).when(
                        F.col("numero_documento").cast("int").between(20000000, 69999999),
                        "1"
                    ).otherwise(default_value)
                ).otherwise(default_value)
            )
        )
   
        return result_df
 
test = populate_gender_when_null(df_harmonized)
test.show()
df_harmonized.show()
df_harmonized
df_harmonized.printSchema()
BUCKET_SILVER = 'alfa-datalake-dev-silver'
BUCKET_BRONZE = 'alfa-datalake-dev-bronce'
BUCKET_GOLD = 'alfa-datalake-dev-gold'
from s3_handler import S3Handler
s3_handler = S3Handler(BUCKET_GOLD, glueContext)
from utils import Utils
utils = Utils()
from awsglue import DynamicFrame
from pyspark.sql.functions import col, split, size, when, expr, concat_ws,trim

def split_names_last_names(df, columna_nombre="nombre", columna_apellido="apellido"):
    """
    Divide la columna de nombres en 'primer_nombre' y 'segundo_nombre',
    respetando nombres compuestos como 'María José', 'Juan Pablo', 'José Luis', 'de la Cruz'.

    Divide la columna de apellidos en 'primer_apellido' y 'segundo_apellido',
    respetando apellidos compuestos como 'de la Rosa' tanto en primera como segunda posición.
    """

    # Separar nombres
    df = df.withColumn("nombre_tokens", split(trim(col(columna_nombre)), " "))

    # Detectar primer nombre compuesto
    df = df.withColumn(
        f"primer_{columna_nombre}",
        when(
            expr("size(nombre_tokens) >= 3 AND nombre_tokens[0] IN ('de', 'del','la')"),
            concat_ws(" ", col("nombre_tokens")[0], col("nombre_tokens")[1], col("nombre_tokens")[2])
        ).when(
            expr("size(nombre_tokens) >= 2 AND nombre_tokens[0] IN ('de', 'del','la')"),
            concat_ws(" ", col("nombre_tokens")[0], col("nombre_tokens")[1])
        ).otherwise(col("nombre_tokens")[0])
    )

    # Detectar segundo nombre compuesto
    df = df.withColumn(
        f"segundo_{columna_nombre}",
        when(
            expr("size(nombre_tokens) >= 5 AND nombre_tokens[3] IN ('de', 'del','la')"),
            concat_ws(" ", col("nombre_tokens")[3], col("nombre_tokens")[4])
        ).when(
            expr("size(nombre_tokens) >= 4 AND nombre_tokens[2] IN ('de', 'del','la')"),
            concat_ws(" ", col("nombre_tokens")[2], col("nombre_tokens")[3])
        ).when(
            expr("size(nombre_tokens) > 2"),
            col("nombre_tokens")[2]
        ).when(
            expr("size(nombre_tokens) == 2"),
            col("nombre_tokens")[1]
        ).otherwise(None)
    )

    # Separar apellidos
    df = df.withColumn("apellido_tokens", split(trim(col(columna_apellido)), " "))

    # Detectar primer apellido compuesto
    df = df.withColumn(
        f"primer_{columna_apellido}",
        when(
            expr("size(apellido_tokens) >= 3 AND apellido_tokens[0] IN ('de', 'del','la')"),
            concat_ws(" ", col("apellido_tokens")[0], col("apellido_tokens")[1], col("apellido_tokens")[2])
        ).when(
            expr("size(apellido_tokens) >= 2 AND apellido_tokens[0] IN ('de', 'del','la')"),
            concat_ws(" ", col("apellido_tokens")[0], col("apellido_tokens")[1])
        ).otherwise(col("apellido_tokens")[0])
    )

    # Detectar segundo apellido compuesto
    df = df.withColumn(
        f"segundo_{columna_apellido}",
        when(
            expr("size(apellido_tokens) >= 5 AND apellido_tokens[3] IN ('de', 'del','la')"),
            concat_ws(" ", col("apellido_tokens")[3], col("apellido_tokens")[4])
        ).when(
            expr("size(apellido_tokens) >= 4 AND apellido_tokens[2] IN ('de', 'del','la')"),
            concat_ws(" ", col("apellido_tokens")[2], col("apellido_tokens")[3])
        ).when(
            expr("size(apellido_tokens) > 2"),
            col("apellido_tokens")[2]
        ).when(
            expr("size(apellido_tokens) == 2"),
            col("apellido_tokens")[1]
        ).otherwise(None)
    )

    # Eliminar columnas temporales
    df = df.drop("nombre_tokens", "apellido_tokens")

    return df

from pyspark.sql.functions import col, split, size, when, expr, concat_ws, lower, trim

def split_names_last_names(df, columna_nombre="nombre", columna_apellido="apellido"):
    """
    Divide nombres y apellidos en primer y segundo componente, respetando partículas como 'de', 'del', 'la', etc.
    """

    # Normalizar espacios
    df = df.withColumn(columna_nombre, regexp_replace(trim(col(columna_nombre)), r'\s+', ' '))
    df = df.withColumn(columna_apellido, regexp_replace(trim(col(columna_apellido)), r'\s+', ' '))

    # Tokenizar apellidos
    df = df.withColumn("apellido_tokens", split(col(columna_apellido), " "))
    df = df.withColumn("apellido_size", size(col("apellido_tokens")))

    # Detectar tamaño del primer apellido compuesto
    df = df.withColumn(
        "primer_apellido_size",
        when(
            (col("apellido_size") >= 3) &
            (lower(col("apellido_tokens")[0]) == "de") &
            (lower(col("apellido_tokens")[1]).isin("la", "las", "los")),
            3
        ).when(
            (col("apellido_size") >= 2) &
            (lower(col("apellido_tokens")[0]).isin("de", "del", "la", "las", "los", "san", "santa")),
            2
        ).otherwise(1)
    )

    # Extraer primer apellido
    df = df.withColumn(
        f"primer_{columna_apellido}",
        concat_ws(" ", *[col("apellido_tokens")[i] for i in range(3)])
    ).withColumn(
        f"primer_{columna_apellido}",
        expr(f"trim(split({f'primer_{columna_apellido}'}, ' ')[0:{f'primer_apellido_size'}])")
    )

    # Extraer segundo apellido si hay más palabras
    df = df.withColumn(
        f"segundo_{columna_apellido}",
        when(
            col("apellido_size") > col("primer_apellido_size"),
            concat_ws(" ", *[
                when(col("apellido_size") > i, col("apellido_tokens")[i]).otherwise("")
                for i in range(3, 6)
            ])
        ).otherwise(None)
    )

    # Tokenizar nombres
    df = df.withColumn("nombre_tokens", split(col(columna_nombre), " "))
    df = df.withColumn("nombre_size", size(col("nombre_tokens")))

    # Detectar tamaño del primer nombre compuesto
    df = df.withColumn(
        "primer_nombre_size",
        when(
            (col("nombre_size") >= 3) &
            (lower(col("nombre_tokens")[0]) == "de") &
            (lower(col("nombre_tokens")[1]).isin("la", "las", "los")),
            3
        ).when(
            (col("nombre_size") >= 2) &
            (lower(col("nombre_tokens")[0]).isin("de", "del", "la", "las", "los", "san", "santa")),
            2
        ).otherwise(1)
    )

    # Extraer primer nombre
    df = df.withColumn(
        f"primer_{columna_nombre}",
        concat_ws(" ", *[col("nombre_tokens")[i] for i in range(3)])
    ).withColumn(
        f"primer_{columna_nombre}",
        expr(f"trim(split({f'primer_{columna_nombre}'}, ' ')[0:{f'primer_nombre_size'}])")
    )

    # Extraer segundo nombre si hay más palabras
    df = df.withColumn(
        f"segundo_{columna_nombre}",
        when(
            col("nombre_size") > col("primer_nombre_size"),
            concat_ws(" ", *[
                when(col("nombre_size") > i, col("nombre_tokens")[i]).otherwise("")
                for i in range(3, 6)
            ])
        ).otherwise(None)
    )

    # Limpiar columnas temporales
    df = df.drop("apellido_tokens", "apellido_size", "primer_apellido_size",
                 "nombre_tokens", "nombre_size", "primer_nombre_size")

    return df
from pyspark.sql.functions import regexp_replace, trim, lower

def split_names_last_names(df, columna_nombre="nombre", columna_apellido="apellido"):
    """
    Split nombres y apellidos de forma genérica manejando partículas comunes.
    No usa listas hardcodeadas, detecta patrones de forma genérica.
    """

 
    df = df.withColumn(columna_nombre, regexp_replace(trim(col(columna_nombre)), r'\s+', ' '))
    df = df.withColumn(columna_apellido, regexp_replace(trim(col(columna_apellido)), r'\s+', ' '))

 
    df = df.withColumn("apellido_array", split(col(columna_apellido), " "))
    df = df.withColumn("apellido_size", size(col("apellido_array")))
    df = df.withColumn(
        f"primer_{columna_apellido}",
        when(
            (col("apellido_size") >= 3) & 
            (lower(col("apellido_array")[0]) == "de") & 
            (lower(col("apellido_array")[1]).isin("la", "las", "los")),
            concat_ws(" ", col("apellido_array")[0], col("apellido_array")[1], col("apellido_array")[2])
        )
        .when(
            (col("apellido_size") >= 2) & 
            (lower(col("apellido_array")[0]).isin("de", "del")),
            concat_ws(" ", col("apellido_array")[0], col("apellido_array")[1])
        )
 
        .when(
            (col("apellido_size") >= 2) & 
            (lower(col("apellido_array")[0]).isin("la", "las", "los", "san", "santa")),
            concat_ws(" ", col("apellido_array")[0], col("apellido_array")[1])
        )
        .otherwise(col("apellido_array")[0])
    )

 
    df = df.withColumn(
        "primer_apellido_palabras",
        when(
            (col("apellido_size") >= 3) & 
            (lower(col("apellido_array")[0]) == "de") & 
            (lower(col("apellido_array")[1]).isin("la", "las", "los")),
            3
        )
        .when(
            (col("apellido_size") >= 2) & 
            (lower(col("apellido_array")[0]).isin("de", "del", "la", "las", "los", "san", "santa")),
            2
        )
        .otherwise(1)
    )
    df = df.withColumn(
        f"segundo_{columna_apellido}",
        when(
            col("apellido_size") > col("primer_apellido_palabras"),
            concat_ws(" ", *[
                when(
                    col("primer_apellido_palabras") == 3,
                    col("apellido_array")[i]
                )
                .when(
                    col("primer_apellido_palabras") == 2,
                    col("apellido_array")[i-1]
                )
                .otherwise(
                    col("apellido_array")[i-2]
                )
                for i in range(3, 10)
            ])
        )
        .otherwise(None)
    )

    df = df.withColumn(
        f"segundo_{columna_apellido}",
        when(col("primer_apellido_palabras") == 1,
             when(col("apellido_size") > 1,
                  concat_ws(" ", *[col("apellido_array")[i] for i in range(1, 10)]))
             .otherwise(None))
        .when(col("primer_apellido_palabras") == 2,
              when(col("apellido_size") > 2,
                   concat_ws(" ", *[col("apellido_array")[i] for i in range(2, 10)]))
              .otherwise(None))
        .when(col("primer_apellido_palabras") == 3,
              when(col("apellido_size") > 3,
                   concat_ws(" ", *[col("apellido_array")[i] for i in range(3, 10)]))
              .otherwise(None))
        .otherwise(None)
    )
    # ============== PROCESAR NOMBRES ==============
    # Para nombres, sin lista hardcodeada, dividimos de forma más simple
    # Estrategia: Si hay 2 palabras, cada una es un nombre
    # Si hay 3+, las primeras 2 pueden ser nombre compuesto si la segunda es corta (de, del, la)
    df = df.withColumn("nombre_array", split(col(columna_nombre), " "))
    df = df.withColumn("nombre_size", size(col("nombre_array")))
    df = df.withColumn(
        f"primer_{columna_nombre}",
        when(
            (col("nombre_size") >= 3) & 
            (lower(col("nombre_array")[1]).isin("de", "del", "la", "las", "los")),
            concat_ws(" ", col("nombre_array")[0], col("nombre_array")[1])
        )
 
        .when(col("nombre_size") == 2,
              col("nombre_array")[0])
 
        .otherwise(col("nombre_array")[0])
    )
    df = df.withColumn(
        f"segundo_{columna_nombre}",
 
        when(
            (col("nombre_size") >= 3) & 
            (lower(col("nombre_array")[1]).isin("de", "del", "la", "las", "los")),
            concat_ws(" ", *[col("nombre_array")[i] for i in range(2, 10)])
        )
        .when(col("nombre_size") == 2,
              col("nombre_array")[1])
 
        .when(col("nombre_size") > 2,
              concat_ws(" ", *[col("nombre_array")[i] for i in range(1, 10)])
        )
        .otherwise(None)
    )

 
    df = df.drop("apellido_array", "apellido_size", "nombre_array", "nombre_size", "primer_apellido_palabras")
    for col_name in [f"primer_{columna_nombre}", f"segundo_{columna_nombre}", 
                     f"primer_{columna_apellido}", f"segundo_{columna_apellido}"]:
        df = df.withColumn(
            col_name,
            when((trim(col(col_name)) == "") | (col(col_name) == ""), None)
            .otherwise(col(col_name))
        )
    return df

df_input = spark.createDataFrame([
            {"nombre": "Juan Carlos", "apellido": "de la Rosa"},
            {"nombre": "Ana", "apellido": "Pérez"},
            {"nombre": "Luis Alberto", "apellido": "del Valle Gómez"},
            {"nombre": "María", "apellido": "García Márquez"},
            {"nombre": "Pedro", "apellido": "de los Santos"},
            {"nombre": "Laura", "apellido": "de la cruz Rodríguez"},
            {"nombre": "Carlos del pilar", "apellido": "Gómez de la Rosa"},
            {"nombre": "de la cruz Sofía", "apellido": "Martínez del Río"},
            {"nombre": "José", "apellido": "López"},
            {"nombre": "Elena", "apellido": "Ramírez de la Vega"},
        ])

split_names_last_names(df_input).show(truncate=False)

from pyspark.sql.functions import split, size, when, col

def separar_nombres(df, columna_nombre="nombre",columna_apellido="apellido"):
    """
    Separa la columna 'nombre' en 'primer_nombre' y 'segundo_nombre'.
    Si solo hay un nombre, 'segundo_nombre' será null.
    """
    nombres_split = split(col(columna_nombre), " ")

    df = df.withColumn(f"primer_{columna_nombre}", nombres_split.getItem(0)) \
           .withColumn(f"segundo_{columna_nombre}", when(size(nombres_split) > 1, nombres_split.getItem(1)).otherwise(None))
    
    apellido_split = split(col(columna_apellido), " ")

    df = df.withColumn(f"primer_{columna_nombre}", apellido_split.getItem(0)) \
           .withColumn(f"segundo_{columna_nombre}", when(size(apellido_split) > 1, apellido_split.getItem(1)).otherwise(None))

    return df

servialfa_path = "s3://alfa-datalake-dev-bronce/SERVIALFA/FILES/"

df_servialfa = utils.read_full_parquet(spark, servialfa_path)
df_splited = separar_nombres(df_servialfa)

df_splited.select("nombre","primer_nombre","segundo_nombre","apellido","primer_apellido","segundo_apellido").show(4,0)
def _save_to_gold(df, table_name):
        """Save DataFrame to Gold layer."""
        try:
            # Build Gold path
            gold_schema = "CLIENTES_GOLD"
            gold_path = f's3://{BUCKET_GOLD}/GOLD/{gold_schema}/GOLD-CARACT_CLIENTES-FACT_{table_name.upper()}/'
           
            final_count = df.count()
            #self.logger.info(f"Saving {final_count:,} records to Gold: {gold_path}")
            print(f"Saving {final_count:,} records to Gold: {gold_path}")
           
            # Convert to DynamicFrame
            dynamic_frame = DynamicFrame.fromDF(df, glueContext, f"{table_name}_gold")
           
            # Save to S3
            s3_handler.save_files_to_s3(
                df=dynamic_frame,
                s3_target_path=gold_path,
                format="parquet",
                mode="overwrite"
            )
           
            saved_count = dynamic_frame.count()
            #self.logger.info(f"Successfully saved {saved_count:,} records to Gold layer")
            print(f"Successfully saved {saved_count:,} records to Gold layer")
        except Exception as e:
            #self.logger.error(f"Error saving to Gold layer: {str(e)}")
            print(f"Error saving to Gold layer: {str(e)}")
            raise
def _convert_spark_row_to_dict(obj):
        """Convert Spark Row objects to regular dictionaries."""
        from pyspark.sql.types import Row
       
        if isinstance(obj, Row):
            return {k: _convert_spark_row_to_dict(v) for k, v in obj.asDict().items()}
        elif hasattr(obj, 'asDict'):
            return {k: _convert_spark_row_to_dict(v) for k, v in obj.asDict().items()}
        elif isinstance(obj, list):
            return [_convert_spark_row_to_dict(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: _convert_spark_row_to_dict(v) for k, v in obj.items()}
        else:
            return obj
def load_config(s3_config_path):
        """Load Gold configuration from S3."""
        try:
            print(f"Loading Gold configuration from: {s3_config_path}")
           
            df = spark.read.option("multiline", "true").json(s3_config_path)
            config_row = df.collect()[0]
            config = config_row.asDict()
           
            # Convert Row objects to dictionaries
            config = _convert_spark_row_to_dict(config)
           
            print("Gold configuration loaded successfully")
            return config
           
        except Exception as e:
            print(f"Error loading Gold config: {str(e)}")
            raise
            
genero_path = "s3://alfa-datalake-dev-gold/GOLD/CLIENTES_GOLD/GOLD-CARACT_CLIENTES-DIM_DIM_GENERO_GOLD/"

df_genero = utils.read_full_parquet(spark, genero_path)
def populate_company_name(self, df):
    """
    populate the age field calculated based on the birthdate.
    Parameters:
        df
    Returns:
        df.
    """
    default_value = None
    
    result_df = df.withColumn(
         "Nombre_Empresa",
        F.when(
            (F.col("Cod_Tipo_Identificacion").isin("1","2","4","5","6","9","10","11","90","91","92")),
            default_value
        ).when(F.col("Cod_Tipo_Identificacion").isin("3","7","8","99"),
            col("nombre_completo")
        ).otherwise(default_value)
    )  
    return result_df   
genero_path = "s3://alfa-datalake-dev-gold/GOLD/CLIENTES_GOLD/GOLD-CARACT_CLIENTES-DIM_DIM_GENERO_GOLD/"

df_genero = utils.read_full_parquet(spark, genero_path)
def apply_column_selection( df, column_config, missing_allowed):
    """Apply column selection based on configuration."""
    try:
        #if not column_config:
        #    print("No column selection specified, using all columns")
        #    return df
       
        selection_type = column_config.get('type', 'select')
        columns = column_config.get('columns', [])
        #columns = column_config
        #selection_type = 'select'
        
        if selection_type == 'select' and columns:
            print(f"Selecting specific columns: {columns}")
            # Validate that columns exist
            available_columns = df.columns
            missing_columns = [col for col in columns if col not in available_columns]
            if missing_columns:
                print(f"Missing columns will be skipped: {missing_columns}")
                columns = [col for col in columns if col in available_columns]
           
            df_selected = df.select(*columns)
            if missing_allowed:
                for column in missing_columns:
                    print(f"creating empty column: {column}")
                    df_selected = df_selected.withColumn(column, lit(None))
        elif selection_type == 'exclude' and columns:
            print(f"Excluding columns: {columns}")
            remaining_columns = [col for col in df.columns if col not in columns]
            df_selected = df.select(*remaining_columns)
           
        else:
            print("Invalid column selection config, using all columns")
            df_selected = df
       
        selected_count = len(df_selected.columns)
        print(f"Column selection result: {selected_count} columns")
       
        return df_selected
       
    except Exception as e:
        print(f"Error applying column selection: {str(e)}")
        raise
def cross_code_with_id(df1, df2,column_code_df1, column_code_df2: str = "codigo", column_id: str = "id", column_fact_id: str = "id_cliente"):
    """
    Cross df1 with df2 using the code column and return only the ID renamed as id_{code_column}.

    Parameters:
    - df1: DataFrame with the codes.
    - df2: DataFrame with the codes and IDs.
    - code_column: name of the column with the code.
    - id_column: name of the column with the ID.

    Returns:
    - DataFrame with a single column: id_{code_column}.
    """
    from pyspark.sql import functions as F
    try: 
        if ("estado" in df2.columns ):
            df2 = df2.filter(F.col("estado") =='A')
        else:
            df2 = df2

        if column_code_df2 not in df2.columns:
            print(f"Column '{column_code_df2}' not found in df2")
            return df1
        if column_code_df1 not in df1.columns :
            print(f"Column '{column_code_df1}' not found in df1")
            return df1
        if  column_id not in df2.columns :
            print(f"Column '{column_id}' not found in df2")
            return df1
        else :
            df_result = df1.join(
            df2.select(col(column_code_df2), col(column_id)),
            df1[column_code_df1] == df2[column_code_df2],
            how="left"
            )
            df_result = df_result.drop(column_fact_id).withColumnRenamed(column_id, column_fact_id)
            print(f"column {column_id} renamed to {column_fact_id}")
           
            return df_result
        
    except Exception as e:
        print(f"Error crossing code with id  {str(e)}")
        raise    

config_path = "s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_gold/gold_config-facts.json"
config = load_config(config_path)
table_config = config.get('gold_configs', {})

clients_fact_config = table_config.get('clientes_fact', {})

contact_fact_config = table_config.get('contact_fact', {})

location_fact_config = table_config.get('location_fact', {})



basic_path = f"{base_path}/CLIENTES/DATOS_BASICOS/CLIENTES_DATOS_BASICOS_MATRIX_PIVOTED/"
            
socio_path = f"{base_path}/CLIENTES/DATOS_SOCIODEMOGRAFICOS/CLIENTES_DATOS_SOCIODEMOGRAFICOS_MATRIX_PIVOTED/"
df_basic = utils.read_full_parquet(spark, basic_path)
from pyspark.sql.functions import split
from pyspark.sql.functions import monotonically_increasing_id

base_path = f's3://{BUCKET_SILVER}'
            
basic_path = f"{base_path}/CLIENTES/DATOS_BASICOS/CLIENTES_DATOS_BASICOS_MATRIX_PIVOTED/"
            
socio_path = f"{base_path}/CLIENTES/DATOS_SOCIODEMOGRAFICOS/CLIENTES_DATOS_SOCIODEMOGRAFICOS_MATRIX_PIVOTED/"
df_basic = utils.read_full_parquet(spark, basic_path)
            
df_socio = utils.read_full_parquet(spark, socio_path)
            
df_client = df_basic.join(df_socio.drop("created_at"),on="pk",how="left")
            
df_client = df_client.withColumn("Cod_Tipo_Identificacion", split(df_client["pk"], "-")[0]).withColumn("Numero_Documento", split(df_client["pk"], "-")[1])

print("antes de cruzar")

print(df_client.filter(col("genero").isNotNull()).count())
            
df_client = cross_code_with_id(df_client, df_genero,"genero", "codigo", "id", "Id_Genero")


print("despues de cruzar")

print(df_client.filter(col("Id_Genero").isNotNull()).count())

def _create_fact_client (config):
        
        from pyspark.sql.functions import split
        from pyspark.sql.functions import monotonically_increasing_id
        try:
            base_path = f's3://{BUCKET_SILVER}'
            
            basic_path = f"{base_path}/CLIENTES/DATOS_BASICOS/CLIENTES_DATOS_BASICOS_MATRIX_PIVOTED/"
            
            socio_path = f"{base_path}/CLIENTES/DATOS_SOCIODEMOGRAFICOS/CLIENTES_DATOS_SOCIODEMOGRAFICOS_MATRIX_PIVOTED/"
            df_basic = utils.read_full_parquet(spark, basic_path)
            
            df_socio = utils.read_full_parquet(spark, socio_path)
            
            df_client = df_basic.join(df_socio.drop("created_at"),on="pk",how="left")
            
            df_client = df_client.withColumn("Cod_Tipo_Identificacion", split(df_client["pk"], "-")[0]).withColumn("Numero_Documento", split(df_client["pk"], "-")[1])
            
            
            print("antes de cruzar")
            df_client.printSchema()
            
            dims = config.get('dims', {})
            
            for dim in dims:
                print(f"crossing dimension ID for {dim}")
                dimen = dims[dim]
                dim_path = dimen['dim_path']
                print(f"dimention path: {dim_path}}}")
                df1 = utils.read_full_parquet(spark, dim_path)
                df_client = cross_code_with_id(df_client, df1,dimen['column_fact'], dimen['column_dim'], dimen['column_id'], dimen['column_fact_id'])
            
            df_client.printSchema()
            column_selection = config.get('column_selection',{})
            
            df_client = df_client.withColumn("Id", monotonically_increasing_id())
            
            df_client = apply_column_selection(df_client,column_selection, "true")
            
            _save_to_gold(df_client,"Cliente")
            
        except Exception as e:
            #self.logger.error(f"Error saving to Gold layer: {str(e)}")
            print(f"Error saving to Gold layer: {str(e)}")
            raise
def select_fact_client (df_client,config):
    
    column_selection = config.get('column_selection',{})
    #column = ["Id","Cod_Tipo_Identificacion","Numero_Documento","primer_nombre", "segundo_nombre","primer_apellido", "segundo_apellido","nombre_completo","Fecha_Expedicion_Documento","Id_Ciudad_Expedicion_Documento","Fecha_Vinculacion_Alfa","Id_estado","Id_Tipo_Persona","Fecha_Nacimiento","Id_Ciudad_Nacimiento","nacionalidad","Id_Genero","Id_Estado_Civil","Id_Tipo_Empresa","Id_Nivel_Estudios","Id_Profesion","Id_Ocupacion","Id_Actividad_Economica","Numero_Hijos","Id_Tipo_Sociedad","Tipo_Identificacion_Empresa","Nit_Empresa","Nombre_Empresa","Cargo_Empresa","fecha_ingreso_empresa","Activo","Fecha_Inicio_Activacion","Fecha_Fin_Activacion"]
    
    df_client = apply_column_selection(df_client,column_selection, "true")
    
    df_client.printSchema()
    
    df_client.show(2)                     
    #_save_to_gold(df_client,"Cliente")
def select_fact_client (config):
    from pyspark.sql.functions import split
    from pyspark.sql.functions import monotonically_increasing_id
    base_path = f's3://{BUCKET_SILVER}'
            
    basic_path = f"{base_path}/{config.get('silver_basic_data_path', {})}"
            
    socio_path = f"{base_path}/{config.get('silver_socio_data_path', {})}"
    
    df_basic = utils.read_full_parquet(spark, basic_path)
            
    df_socio = utils.read_full_parquet(spark, socio_path)
            
    df_client = df_basic.join(df_socio.drop("created_at"),on="pk",how="left")
    
    df_client = df_client.withColumn("Id", monotonically_increasing_id())
    
    df_client = df_client.withColumn("Cod_Tipo_Identificacion", split(df_client["pk"], "-")[0]).withColumn("Numero_Documento", split(df_client["pk"], "-")[1])
    
    df_client = df_client.select(
                         col("Id"),
                         col("pk"),
                         col("Cod_Tipo_Identificacion").alias("Id_Tipo_Documento"),
                         col("Numero_Documento"),
                         col("primer_nombre"),
                         col("segundo_nombre"),
                         col("primer_apellido"),
                         col("segundo_apellido"),
                         col("nombre_completo"),
                         lit("").alias("Fecha_Expedicion_Documento"),
                         col("ciudad_expedicion_documento").alias("Id_Ciudad_Expedicion_Documento"),
                         lit("").alias("Fecha_Vinculacion_Alfa"),
                         lit("").alias("Id_estado"),
                         lit("").alias("Id_Tipo_Persona"),
                         col("fecha_nacimiento"),
                         col("ciudad_nacimiento").alias("Id_Ciudad_Nacimiento"),
                         col("nacionalidad"),
                         col("genero").alias("Id_Genero"),
                         col("cod_estado_civil").alias("Id_Estado_Civil"),
                         lit("").alias("Id_Tipo_Empresa"),
                         col("nivel_estudios").alias("Id_Nivel_Estudios"),
                         col("profesion").alias("Id_Profesion"),
                         col("ocupacion").alias("Id_Ocupacion"),
                         col("cod_actividad_economica").alias("Id_Actividad_Economica"),
                         lit("").alias("Numero_Hijos"),
                         lit("").alias("Id_Tipo_Sociedad"),
                         col("tipo_id_empresa").alias("Tipo_Identificacion_Empresa"),
                         col("nit_empresa").alias("Nit_Empresa"),
                         lit("").alias("Nombre_Empresa"),
                         col("cod_cargo_empresa").alias("Cargo_Empresa"),
                         col("fecha_ingreso_empresa"),
                         lit("").alias("Activo"),
                         lit("").alias("Fecha_Inicio_Activacion"),
                         lit("").alias("Fecha_Fin_Activacion")
                         )
    
    _save_to_gold(df_client,"Cliente")
def _create_fact_contact (config):
            from pyspark.sql.functions import monotonically_increasing_id
            base_path = f's3://{BUCKET_SILVER}'
            
            contact_path = f"{base_path}/{config.get('silver_location_contact_path', {})}"
            
            
            df_contact = utils.read_full_parquet(spark, contact_path)
            
            df_contact.printSchema()
            
            dims = config.get('dims', {})
            
            for dim in dims:
                print(f"crossing dimension ID for {dim}")
                dimen = dims[dim]
                dim_path = dimen['dim_path']
                print(f"dimention path: {dim_path}}}")
                df1 = utils.read_full_parquet(spark, dim_path)
                df_contact = cruzar_codigo_con_id(df_contact,df1 ,dimen['column_fact'], dimen['column_dim'], dimen['column_id'], dimen['column_fact_id'])
            
            column_selection = config.get('column_selection',{})
            
            df_contact =  df_contact.withColumn("Id", monotonically_increasing_id())
            
            df_contact = apply_column_selection(df_contact,column_selection, "true")
            
            _save_to_gold(df_contact,"Contactabilidad")
column = ["Id","pk","tipo_direccion","direccion","barrio","ciudad","extension","indicativo","pais","created_at"]
            df_contact = df_contact.select(
                col("Id"),
                col("pk").alias("Id_Cliente"),
                lit("").alias("Tipo_Contacto"),
                col("Indicativo"),
                lit("").alias("Numero_Telefono"),
                col("Extension"),
                col("Correo_Electronico"),
                lit("").alias("Fecha_Inicio_Activacion"),
                lit("").alias("Fecha_Fin_Activacion"),
                lit("").alias("Activo"),
                lit("").alias("Id_Cargo")
            )
 |-- PK: string (nullable = true)
 |-- barrio: string (nullable = true)
 |-- ciudad: string (nullable = true)
 |-- correo_electronico: string (nullable = true)
 |-- departamento: string (nullable = true)
 |-- direccion: string (nullable = true)
 |-- extension: string (nullable = true)
 |-- indicativo: string (nullable = true)
 |-- pais: string (nullable = true)
 |-- tipo_direccion: string (nullable = true)
 |-- created_at: timestamp (nullable = true)
def _create_fact_location(config):
            from pyspark.sql.functions import monotonically_increasing_id
            
            
            base_path = f's3://{BUCKET_SILVER}'
            
            contact_path = f"{base_path}/{config.get('silver_location_contact_path', {})}"
            
            df_location = utils.read_full_parquet(spark, contact_path)
            
            
            dims = config.get('dims', {})
            
            for dim in dims:
                print(f"crossing dimension ID for {dim}")
                dimen = dims[dim]
                dim_path = dimen['dim_path']
                print(f"dimention path: {dim_path}}}")
                df1 = utils.read_full_parquet(spark, dim_path)
                df_location = cruzar_codigo_con_id(df_location, df1,dimen['column_fact'], dimen['column_dim'], dimen['column_id'], dimen['column_fact_id'])
            
            column_selection = config.get('column_selection',{})
            
            df_location =  df_location.withColumn("Id", monotonically_increasing_id())
            
            df_location = apply_column_selection(df_location,column_selection, "true")
            _save_to_gold(df_location,"Ubicacion")
df_client = _create_fact_client(clients_fact_config)

df_client.printSchema()

df_contact = _create_fact_contact(contact_fact_config)

df_contact.printSchema()
df_location = _create_fact_location(location_fact_config)

df_location.printSchema()
select_fact_client(clients_fact_config)
_create_fact_contact()
_create_fact_location()
from s3_handler import S3Handler
from utils import Utils
import os

BUCKET_SILVER = 'alfa-datalake-dev-silver'


utils = Utils()
s3_prefix = f's3://{BUCKET_SILVER}/CLIENTES/'
# load tmp data
datos_basicos_stg = utils.read_full_parquet(spark, s3_target_path=os.path.join(s3_prefix,'DATOS_BASICOS'))
datos_sociodemo_stg = utils.read_full_parquet(spark,s3_target_path=os.path.join(s3_prefix,'DATOS_SOCIODEMOGRAFICOS'))
datos_contactabilidad_stg = utils.read_full_parquet(spark,s3_target_path=os.path.join(s3_prefix,'DATOS_UBICACION_Y_CONTACTABILIDAD'))
from apply_cleanup import ApplyCleanup
from apply_cleanup import ApplyCleanup
cleanup = ApplyCleanup(spark,glueContext)

df_DB= cleanup.clean_basic_data(datos_basicos_stg)
df_DB_limpio = cleanup.select_basic_data(df_DB)
 
df_DS= cleanup.clean_sociodemographic_data(datos_sociodemo_stg)
df_DS_limpio = cleanup.select_sociodemographic_data(df_DS)
 
df_DC= cleanup.clean_contact_data(datos_contactabilidad_stg)
df_DC_limpio = cleanup.select_contact_data(df_DC)
df_DB_limpio.show(2, 0)
df_DS_limpio.show(2,0)
df_DC_limpio.show(2,0)
cleanup = Apply_cleanup(spark,glueContext)
df= cleanup.clean_basic_data(datos_basicos_stg)
df.show(2,0)
df_limpio = cleanup.select_basic_data(df)
df_limpio.show(2, 0)

from pyspark.sql.functions import col, sum, count, when

def calcular_porcentaje_nulidad(df):
    total_registros = df.count()
    resultados = []

    for columna in df.columns:
        valores_null = df.filter(col(columna).isNull()).count()
        porcentaje_null = round((valores_null * 100.0) / total_registros, 2)
        resultados.append((columna, valores_null, porcentaje_null))

    # Convertir resultados a DataFrame
    schema = ["columna", "valores_null", "porcentaje_null"]
    df_resultado = spark.createDataFrame(resultados, schema)
    return df_resultado




def cargar_datos_desde_catalogo(glueContext, database, table_name, campos_seleccionados):
    """
    Carga datos desde el catálogo de Glue y selecciona campos específicos.

    Parámetros:
    - glueContext: contexto de Glue.
    - database (str): nombre de la base de datos en el catálogo.
    - table_name (str): nombre de la tabla en el catálogo.
    - campos_seleccionados (list): lista de nombres de campos a seleccionar.

    Retorna:
    - DynamicFrame con los campos seleccionados.
    """
    df = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table_name
    )

    df_seleccionado = df.select_fields(campos_seleccionados).toDF()
    #df_seleccionado.show(2,0)
    return df_seleccionado
# ENV VARS
#SELECT * FROM "datalake-dev-bronce-catalog-db"."acsele_acsele_alfa_direccion" LIMIT 10;
#DATABASE_NAME = 'datalake-dev-bronce-catalog-db'
#TABLE_NAME = 'acsele_acsele_alfa_oavidaap'

df_prethirdparty = cargar_datos_desde_catalogo(
glueContext,
"datalake-dev-bronce-catalog-db",
"acsele_acsele_alfa_prethirdparty",
["PK", "STATIC", "TIPODOCUMENTOINPUT", "DOCUMENTOIDENTIDADINPUT", "NOMBREPREDETERMINADOINPUT",
 "FECHAINCLUSIONINPUT", "STATUS", "EMAILINPUT", "INDICATIVOOFICINAVALUE", "TELEFONOOFICINAVALUE", "TELEFONOOFICINACOMPLETINPUT"])

df_personajuridica = cargar_datos_desde_catalogo(
glueContext,
"datalake-dev-bronce-catalog-db",
"acsele_acsele_alfa_personajuridica",
["PK", "STATIC", "NOMBRECONTACTOINPUT", "RAZONSOCIALINPUT", "TIPOEMPRESAVALUE", "TIPOEMPRESAINPUT",
 "ACTIVIDADECONOMICAVALUE", "TIPOSOCIEDADINPUT", "EXTENSIONDOSINPUT", "TELEFONOOFICINADOSINPUT"])

"""df_personanatural = cargar_datos_desde_catalogo(
glueContext,
"datalake-dev-bronce-catalog-db",
"acsele_acsele_alfa_personanatural",
["PK", "STATIC", "NOMBREINPUT", "SEGUNDONOMBREINPUT", "APELLIDOINPUT", "APELLIDOMATERNOINPUT",
 "ESTADOCIVILINPUT", "GRADOINSTITUCIONINPUT", "GRADOINSTITUCIONVALUE", "OCUPACIONINPUT",
 "INDICATIVOFIJOINPUT", "TELEFONOCELULARINPUT", "TELEFONOFIJOCOMPLETOINPUT", "TELEFONOINPUT"])"""

df_personanatural = cargar_datos_desde_catalogo(
glueContext,
"datalake-dev-bronce-catalog-db",
"acsele_acsele_alfa_personanatural",
["PK", "STATIC", "NOMBREINPUT", "SEGUNDONOMBREINPUT", "APELLIDOINPUT", "APELLIDOMATERNOINPUT", "GRADOINSTITUCIONINPUT", "GRADOINSTITUCIONVALUE", "OCUPACIONINPUT",
 "INDICATIVOFIJOINPUT", "TELEFONOCELULARINPUT", "TELEFONOFIJOCOMPLETOINPUT", "TELEFONOINPUT"])

df_aseguradovida = cargar_datos_desde_catalogo(
glueContext,
"datalake-dev-bronce-catalog-db",
"acsele_acsele_alfa_aseguradovida",
["PK", "STATIC", "FECHANACIMIENTOINPUT", "CIUDADINPUT", "CIUDADVALUE",
 "DEPARTAMENTOINPUT", "DEPARTAMENTOVALUE", "SEXOINPUT"])

df_oavidaap = cargar_datos_desde_catalogo(
glueContext,
"datalake-dev-bronce-catalog-db",
"acsele_acsele_alfa_oavidaap",
["PK", "STATIC", "ESTADOCIVILINPUT", "HIJOSINPUT"])

df_pomihogarseguro = cargar_datos_desde_catalogo(
glueContext,
"datalake-dev-bronce-catalog-db",
"acsele_acsele_alfa_pomihogarseguro",
["PK", "STATIC", "IDEMPLEADORINPUT", "EMPLEADORINPUT"])

df_direccion = cargar_datos_desde_catalogo(
glueContext,
"datalake-dev-bronce-catalog-db",
"acsele_acsele_alfa_direccion",
["PK", "STATIC", "TIPODIRECCIONTERCEROINPUT", "DIRECCIONINPUT", "BARRIOINPUT",
 "CIUDADINPUT", "CIUDADVALUE", "DEPARTAMENTOINPUT", "DEPARTAMENTOVALUE", "PAISINPUT", "PAISVALUE"])

df_thirdparty_role = glueContext.create_dynamic_frame.from_catalog(database='datalake-dev-bronce-catalog-db',table_name='acsele_acsele_alfa_thirdpartyrole').toDF()    
          
df_aseguradovida.show(1,0)
df_thirdparty_role.show(1,0)
# ================================================================================
# acsele - PERSONAS NATURALES - DATOS BÁSICOS
# Query convertida a lógica Spark pura
# ================================================================================

df_result = df_prethirdparty.join(df_personanatural, on="STATIC", how="inner")

df_acsele_pn_datos_basicos = df_result.select(
    col("TIPODOCUMENTOINPUT").alias("Cod_Tipo_Identificacion"),
    col("DOCUMENTOIDENTIDADINPUT").alias("Numero_Documento"),
    col("NOMBREINPUT").alias("Primer_Nombre"),
    col("SEGUNDONOMBREINPUT").alias("SEGUNDO_NOMBRE"),
    col("APELLIDOINPUT").alias("Primer_Apellido"),
    col("APELLIDOMATERNOINPUT").alias("Segundo_Apellido"),
    col("NOMBREPREDETERMINADOINPUT").alias("Nombre_Completo"),
    col("FECHAINCLUSIONINPUT").alias("Fecha_Vinculacion"),
    col("STATUS").alias("Cod_Estado")
)

#df_acsele_pn_datos_basicos.show(2,0)

# ================================================================================
# acsele - PERSONAS NATURALES - DATOS DEMOGRÁFICOS 
# ================================================================================

df_result = df_prethirdparty.join(df_personanatural, on="STATIC", how="inner").join(df_oavidaap.drop("STATIC"), df_prethirdparty.PK == df_oavidaap.PK, how="left").join(df_pomihogarseguro.drop("STATIC"),df_prethirdparty.PK == df_pomihogarseguro.PK,"left")
df_temp= df_aseguradovida.join(df_thirdparty_role,df_aseguradovida.PK == df_thirdparty_role.IDDCOROLE,how="inner")
df_result= df_result.join(df_temp,df_result.STATIC == df_temp.THIRDPARTYID , how="left")
#df_result.show(2,0)
df_acsele_pn_datos_demograficos = df_result.select(
    col("TIPODOCUMENTOINPUT").alias("Cod_Tipo_Identificacion"),
    col("DOCUMENTOIDENTIDADINPUT").alias("Numero_Documento"),
    col("FECHANACIMIENTOINPUT").alias("Fecha_Nacimiento"),
    col("CIUDADINPUT").alias("Cod_Ciudad_Nacimiento"),
    col("CIUDADVALUE").alias("Ciudad_Nacimiento"),
    col("DEPARTAMENTOINPUT").alias("Cod_Departamento_Nacimiento"),
    col("DEPARTAMENTOVALUE").alias("Departamento_Nacimiento"),
    col("SEXOINPUT").alias("Genero"),
    col("ESTADOCIVILINPUT").alias("Cod_Estado_Civil"),
    col("GRADOINSTITUCIONINPUT").alias("Cod_Nivel_Estudios"),
    col("GRADOINSTITUCIONVALUE").alias("Nivel_Estudios"),
    col("OCUPACIONINPUT").alias("Cod_Ocupacion"),
    col("HIJOSINPUT").alias("Numero_Hijos"),
    col("IDEMPLEADORINPUT").alias("Tipo_Identificacion_Empresa"),
    col("EMPLEADORINPUT").alias("NIT_Empresa")
)
#df_acsele_pn_datos_demograficos.show(2,0)
# ================================================================================
# acsele - PERSONAS NATURALES - DATOS CONTACTABILIDAD
# ================================================================================

df_result = df_prethirdparty.join(df_personanatural, on="STATIC", how="inner").join(df_direccion.drop("STATIC"), df_prethirdparty.PK == df_direccion.PK, how="left")


df_acsele_pn_datos_contactabilidad = df_result.select(
    col("TIPODOCUMENTOINPUT").alias("Cod_Tipo_Identificacion"),
    col("DOCUMENTOIDENTIDADINPUT").alias("Numero_Documento"),
    col("EMAILINPUT").alias("Correo_Electronico"),
    col("TIPODIRECCIONTERCEROINPUT").alias("Tipo_Direccion"),
    col("DIRECCIONINPUT").alias("Direccion"),
    col("BARRIOINPUT").alias("Barrio"),
    col("CIUDADINPUT").alias("Cod_Ciudad"),
    col("CIUDADVALUE").alias("Ciudad"),
    col("DEPARTAMENTOINPUT").alias("Cod_Departamento"),
    col("DEPARTAMENTOVALUE").alias("Departamento"),
    col("PAISINPUT").alias("Cod_Pais"),
    col("PAISVALUE").alias("Pais"),
    col("INDICATIVOFIJOINPUT").alias("Indicativo"),
    col("INDICATIVOOFICINAVALUE").alias("Indicativo_value"),
    col("TELEFONOCELULARINPUT").alias("Numero_Telefono_1"),
    col("TELEFONOFIJOCOMPLETOINPUT").alias("Numero_Telefono_2"),
    col("TELEFONOINPUT").alias("Numero_Telefono_3"),
    col("TELEFONOOFICINAVALUE").alias("Numero_Telefono_4"),
    col("TELEFONOOFICINACOMPLETINPUT").alias("Numero_Telefono_5")
)

#df_acsele_pn_datos_contactabilidad.show(2,0)

df_personajuridica.show(1,0)
# ================================================================================
# acsele - PERSONAS JURIDICAS - DATOS BÁSICOS
# Query convertida a lógica Spark pura
# ================================================================================

df_result = df_prethirdparty.join(df_personajuridica, on="STATIC", how="inner")
df_acsele_pj_datos_basicos = df_result.select(
    col("TIPODOCUMENTOINPUT").alias("Cod_Tipo_Identificacion"),
    col("DOCUMENTOIDENTIDADINPUT").alias("Numero_Documento"),
    col("NOMBRECONTACTOINPUT").alias("Primer_Nombre"),
    col("NOMBREPREDETERMINADOINPUT").alias("Nombre_Completo_Persona"),
    col("RAZONSOCIALINPUT").alias("Nombre_Completo_Empresa"),
    col("RAZONSOCIALINPUT").alias("Nombre_Completo"),
    col("FECHAINCLUSIONINPUT").alias("Fecha_Vinculacion"),
    col("STATUS").alias("Cod_Estado"),
    col("TIPOEMPRESAVALUE").alias("Cod_Tipo_Empresa"),
    col("TIPOEMPRESAINPUT").alias("Tipo_Empresa")
)

#df_acsele_pj_datos_basicos.show(2,0)

# ================================================================================
# acsele - PERSONAS JURIDICAS - DATOS DEMOGRÁFICOS 
# ================================================================================

df_result = df_prethirdparty.join(df_personajuridica, on="STATIC", how="inner").join(df_oavidaap.drop("STATIC"), df_prethirdparty.PK == df_oavidaap.PK, how="left").join(df_pomihogarseguro.drop("STATIC"),df_prethirdparty.PK == df_pomihogarseguro.PK,"left")
df_temp= df_aseguradovida.join(df_thirdparty_role,df_aseguradovida.PK == df_thirdparty_role.IDDCOROLE,how="inner")
df_result= df_result.join(df_temp,df_result.STATIC == df_temp.THIRDPARTYID , how="left")
#df_result.show(2,0)
df_acsele_pj_datos_demograficos = df_result.select(
    col("TIPODOCUMENTOINPUT").alias("Cod_Tipo_Identificacion"),
    col("DOCUMENTOIDENTIDADINPUT").alias("Numero_Documento"),
    col("FECHANACIMIENTOINPUT").alias("Fecha_Nacimiento"),
    col("CIUDADINPUT").alias("Cod_Ciudad_Nacimiento"),
    col("CIUDADVALUE").alias("Ciudad_Nacimiento"),
    col("DEPARTAMENTOINPUT").alias("Cod_Departamento_Nacimiento"),
    col("DEPARTAMENTOVALUE").alias("Departamento_Nacimiento"),
    col("SEXOINPUT").alias("Genero"),
    col("ESTADOCIVILINPUT").alias("Cod_Estado_Civil"),
    col("ACTIVIDADECONOMICAVALUE").alias("Cod_Actividad_Economica"),
    col("HIJOSINPUT").alias("Numero_Hijos"),
    col("TIPOSOCIEDADINPUT").alias("Cod_Tipo_Sociedad"),
    col("IDEMPLEADORINPUT").alias("Tipo_Identificacion_Empresa"),
    col("IDEMPLEADORINPUT").alias("NIT_Empresa")
)

#df_acsele_pj_datos_demograficos.show(2,0)
# ================================================================================
# acsele - PERSONAS JURIDICAS - DATOS CONTACTABILIDAD
# ================================================================================

df_result = df_prethirdparty.join(df_personajuridica, on="STATIC", how="inner").join(df_direccion.drop("STATIC"), df_prethirdparty.PK == df_direccion.PK, how="left")


df_acsele_pj_datos_contactabilidad = df_result.select(
    col("TIPODOCUMENTOINPUT").alias("Cod_Tipo_Identificacion"),
    col("DOCUMENTOIDENTIDADINPUT").alias("Numero_Documento"),
    col("EMAILINPUT").alias("Correo_Electronico"),
    col("TIPODIRECCIONTERCEROINPUT").alias("Tipo_Direccion"),
    col("DIRECCIONINPUT").alias("Direccion"),
    col("BARRIOINPUT").alias("Barrio"),
    col("CIUDADINPUT").alias("Cod_Ciudad"),
    col("CIUDADVALUE").alias("Ciudad"),
    col("DEPARTAMENTOINPUT").alias("Cod_Departamento"),
    col("DEPARTAMENTOVALUE").alias("Departamento"),
    col("PAISINPUT").alias("Cod_Pais"),
    col("PAISVALUE").alias("Pais"),
    col("EXTENSIONDOSINPUT").alias("Indicativo_Oficina"),
    col("INDICATIVOOFICINAVALUE").alias("Indicativo_Telefono"),
    col("TELEFONOOFICINADOSINPUT").alias("Telefono_Oficina_DOS"),
    col("TELEFONOOFICINAVALUE").alias("Telefono_Oficina"),
    col("TELEFONOOFICINACOMPLETINPUT").alias("Telefono_Oficina_Completo")
)

#df_acsele_pj_datos_contactabilidad.show(2,0)

df_pn_demograficos_renamed = df_acsele_pn_datos_demograficos.select(
    *[col(c).alias(f"{c}_demografico") if c not in ["Cod_Tipo_Identificacion", "Numero_Documento"] 
      else col(c) for c in df_acsele_pn_datos_demograficos.columns]
)

# Renombrar columnas conflictivas en contactabilidad PN  
df_pn_contactabilidad_renamed = df_acsele_pn_datos_contactabilidad.select(
    *[col(c).alias(f"{c}_contacto") if c not in ["Cod_Tipo_Identificacion", "Numero_Documento"] 
      else col(c) for c in df_acsele_pn_datos_contactabilidad.columns]
)

df_acsele_pn_completo = df_acsele_pn_datos_basicos \
    .join(
        df_pn_demograficos_renamed, 
        ["Cod_Tipo_Identificacion", "Numero_Documento"], 
        "left"
    ) \
    .join(
        df_pn_contactabilidad_renamed, 
        ["Cod_Tipo_Identificacion", "Numero_Documento"], 
        "left"
    )

df_pj_demograficos_renamed = df_acsele_pj_datos_demograficos.select(
    *[col(c).alias(f"{c}_demografico") if c not in ["Cod_Tipo_Identificacion", "Numero_Documento"] 
      else col(c) for c in df_acsele_pj_datos_demograficos.columns]
)

# Renombrar columnas conflictivas en contactabilidad PN  
df_pj_contactabilidad_renamed = df_acsele_pj_datos_contactabilidad.select(
    *[col(c).alias(f"{c}_contacto") if c not in ["Cod_Tipo_Identificacion", "Numero_Documento"] 
      else col(c) for c in df_acsele_pj_datos_contactabilidad.columns]
)

df_acsele_pj_completo = df_acsele_pj_datos_basicos \
    .join(
        df_pj_demograficos_renamed, 
        ["Cod_Tipo_Identificacion", "Numero_Documento"], 
        "left"
    ) \
    .join(
        df_pj_contactabilidad_renamed, 
        ["Cod_Tipo_Identificacion", "Numero_Documento"], 
        "left"
    )

print("🔄 Uniendo Personas Naturales + Personas Jurídicas...")
df_macro_acsele_temp = df_acsele_pn_completo.unionByName(
    df_acsele_pj_completo, 
    allowMissingColumns=True
)
df_macro_acsele = df_macro_acsele_temp.withColumn("COD_FUENTE", lit(1))

count_pn = df_acsele_pn_completo.count()
count_pj = df_acsele_pj_completo.count()
count_total = df_macro_acsele.count()

print(f"✅ Personas Naturales consolidadas: {count_pn:,} registros")
print(f"✅ Personas Jurídicas consolidadas: {count_pj:,} registros")
print(f"🎯 DF Macro ALFAPRD total: {count_total:,} registros")
df_macro_acsele.columns

# ================================================================================
# ESQUEMA DATOS BÁSICOS - MAPEO EXACTO ORIGEN → DESTINO
# ================================================================================

from pyspark.sql.functions import col, lit, coalesce, concat_ws, when, current_date
from pyspark.sql.functions import monotonically_increasing_id

print("📊 Creando esquema DATOS BÁSICOS con mapeo exacto...")

# MAPEO EXACTO SEGÚN TU ESPECIFICACIÓN
df_acsele_datos_basicos_schema = df_macro_acsele.select(
    # === PK (único campo NOT NULL) ===
    monotonically_increasing_id().alias("customer_dbasicos_id"),
    
    # === MAPEO 1: COD_TIPO_IDENTIFICACION → Cod_Tipo_Identificacion ===
    col("Cod_Tipo_Identificacion").alias("Cod_Tipo_Identificacion"),
    
    # === MAPEO 2: NID_NUMERO_IDENTIFICACION → Numero_Documento ===
    col("Numero_Documento").cast("string").alias("Numero_Documento"),
    
    # === MAPEO 3: PRIMER_NOMBRE → Primer_Nombre ===
    col("Primer_Nombre").alias("Primer_Nombre"),
    
    # === MAPEO 4: SEGUNDO_NOMBRE → Segundo_Nombre ===
    col("SEGUNDO_NOMBRE").alias("Segundo_Nombre"),
    
    # === MAPEO 5: PRIMER_APELLIDO → Primer_Apellido ===
    col("Primer_Apellido").alias("Primer_Apellido"),
    
    # === MAPEO 6: SEGUNDO_APELLIDO → Segundo_Apellido ===
    col("Segundo_Apellido").alias("Segundo_Apellido"),
    
    # === MAPEO 7: CONSTRUIR Nombre_Completo ===
    # Para PN: concatenar nombres y apellidos
    # Para PJ: usar RAZON_SOCIAL
    coalesce(
        when(col("PRIMER_NOMBRE").isNotNull(),col("Nombre_Completo")
        ),
        col("Nombre_Completo")  # Para PJ (de tu DF con sufijo)
    ).alias("Nombre_Completo"),
    
    # === MAPEO 8: FEC_EXPEDICION_DOCUMENT → Fecha_Expedición_Documento ===
    lit("").alias("Fecha_Expedición_Documento"),
    
    # === MAPEO 9: COD_CIUDAD_EXPEDICION_DOCU → Ciudad_Expedición_Documento ===
    lit("").alias("Ciudad_Expedición_Documento"),
    
    # === MAPEO 10: FEC_REGISTRO → Fecha_Vinculacion ===
    col("Fecha_Vinculacion").alias("Fecha_Vinculacion"),
    
    # === MAPEO 11: COD_ESTADO → Cod_Estado ===
    col("Cod_Estado").cast("int").alias("Cod_Estado"),
    # === MAPEO 12: Cod_Tipo_Persona → Cod_Tipo_Persona ===
    lit("").alias("Cod_Tipo_Persona"),   
    # === MAPEO 13: COD_TIPO_EMPRESA → Cod_Tipo_Empresa ===
    col("Cod_Tipo_Empresa").alias("Cod_Tipo_Empresa"),
    
    # === MAPEO 14: NOMBRE (tipos_empresa) → Tipo_Empresa ===
    col("Tipo_Empresa").alias("Tipo_Empresa"),
    

    # === METADATOS ===
    col("COD_FUENTE").alias("cod_fuente_fk"),
    current_date().alias("fecha_cargue"),
    lit("GLUE_JOB_ACSELE_SILVER").alias("job_cargue_id")
)

# Verificar resultado
count_datos_basicos = df_acsele_datos_basicos_schema.count()
print(f"✅ Esquema DATOS BÁSICOS (mapeo exacto): {count_datos_basicos:,} registros")

# Mostrar mapeo aplicado
print(f"\n📋 Mapeo aplicado:")
mapeos = [
    ("COD_TIPO_IDENTIFICACION", "Cod_Tipo_Identificacion"),
    ("NID_NUMERO_IDENTIFICACION", "Numero_Documento"),
    ("PRIMER_NOMBRE", "Primer_Nombre"),
    ("SEGUNDO_NOMBRE", "Segundo_Nombre"),
    ("PRIMER_APELLIDO", "Primer_Apellido"),
    ("SEGUNDO_APELLIDO", "Segundo_Apellido"),
    ("PRIMER_NOMBRE+APELLIDOS/razon_social", "Nombre_Completo"),
    ("FEC_EXPEDICION_DOCUMENTO", "Fecha_Expedición_Documento"),
    ("COD_CIUDAD_EXPEDICION_DOCU", "Ciudad_Expedicion_Documento"),
    ("FEC_REGISTRO", "Fecha_Vinculacion"),
    ("COD_ESTADO", "Cod_Estado"),
    ("cod_tipo_empresa", "Cod_Tipo_Empresa"),
    ("tipo_empresa", "Tipo_Empresa")
]

for i, (origen, destino) in enumerate(mapeos, 1):
    print(f"  {i:2d}. {origen} → {destino}")

# Mostrar muestra
print(f"\n📊 Muestra de datos:")
df_acsele_datos_basicos_schema.show(3, truncate=False)

print("🎯 ¡Esquema DATOS BÁSICOS con mapeo exacto completado!")
BUCKET_SILVER = 'alfa-datalake-dev-silver'
BUCKET_BRONZE = 'alfa-datalake-dev-bronce'
from s3_handler import S3Handler
s3_handler = S3Handler(BUCKET_SILVER, glueContext)
from awsglue import DynamicFrame
df_dynamic = DynamicFrame.fromDF(df_acsele_datos_basicos_schema, glueContext, "fuentes_clientes")
s3_handler = S3Handler(BUCKET_SILVER, glueContext)
s3_prefix, s3_target_path = s3_handler.generate_s3_paths('CLIENTES', 'DATOS_BASICOS', 'STG')
response = s3_handler.save_files_to_s3(df_dynamic, s3_target_path, format="parquet", mode="append")
from pyspark.sql.functions import col, lit, coalesce, concat_ws, when, current_date
from pyspark.sql.functions import monotonically_increasing_id
df_acsele_datos_Sociodemográficos_schema = df_macro_acsele.select(
    # === PK (único campo NOT NULL) ===
    monotonically_increasing_id().alias("customer_dsociodemograficos_id"),
    
    # === MAPEO 1: COD_TIPO_IDENTIFICACION → Cod_Tipo_Identificacion ===
    col("Cod_Tipo_Identificacion").alias("Cod_Tipo_Identificacion"),
    
    # === MAPEO 2: NID_NUMERO_IDENTIFICACION → Numero_Documento ===
    col("Numero_Documento").cast("string").alias("Numero_Documento"),
    
    # === MAPEO 3: PRIMER_NOMBRE → Primer_Nombre ===
    col("Fecha_Nacimiento_demografico").alias("Fecha_Nacimiento"),
    
    # === MAPEO 4: SEGUNDO_NOMBRE → Segundo_Nombre ===
    lit("").alias("Edad"),
    
    # === MAPEO 5: PRIMER_APELLIDO → Primer_Apellido ===
    col("Cod_Ciudad_Nacimiento_demografico").alias("Cod_Ciudad_Nacimiento"),
    
    # === MAPEO 6: SEGUNDO_APELLIDO → Segundo_Apellido ===
    col("Ciudad_Nacimiento_demografico").alias("Ciudad_Nacimiento"),
    
    col("Cod_Departamento_Nacimiento_demografico").alias("Cod_Departamento_Nacimiento"),
    
    col("Departamento_Nacimiento_demografico").alias("Departamento_Nacimiento"),
    
    lit("").alias("Cod_Pais_Nacimiento"),
    
    lit("").alias("Pais_Nacimiento"),
    
    lit("").alias("Nacionalidad"),
    
    col("Genero_demografico").alias("Genero"),
    
    col("Cod_Estado_Civil_demografico").alias("Cod_Estado_Civil"),
    
    col("Cod_Nivel_Estudios_demografico").alias("Cod_Nivel_Estudios"),
    
    col("Nivel_Estudios_demografico").alias("Nivel_Estudios"),
    
    lit("").alias("Cod_Profesion"),
    
    lit("").alias("Profesion"),
    
    col("Cod_Ocupacion_demografico").alias("Cod_Ocupacion"),
    
    lit("").alias("Ocupacion"),
    
    col("Cod_Actividad_Economica_demografico").alias("Cod_Actividad_Economica"),
    
    col("Numero_Hijos_demografico").alias("Numero_Hijos"),
    
    col("Cod_Tipo_Sociedad_demografico").alias("Cod_Tipo_Sociedad"),
    
    col("Tipo_Identificacion_Empresa_demografico").alias("Tipo_Identificacion_Empresa"),
    
    col("NIT_Empresa_demografico").alias("NIT_Empresa"),
    
    lit("").alias("Id_Afiliacion"),
    
    lit("").alias("Cod_Cargo_Empresa"),
    
    lit("").alias("Fecha_Ingreso_Empresa"),
    

    # === METADATOS ===
    col("COD_FUENTE").alias("cod_fuente_fk"),
    current_date().alias("fecha_cargue"),
    lit("GLUE_JOB_ACSELE_SILVER").alias("job_cargue_id")
)

df_acsele_datos_Sociodemográficos_schema.show(3, truncate=False)

from awsglue import DynamicFrame
df_dynamic = DynamicFrame.fromDF(df_acsele_datos_Sociodemográficos_schema, glueContext, "fuentes_clientes")
s3_handler = S3Handler(BUCKET_SILVER, glueContext)
s3_prefix, s3_target_path = s3_handler.generate_s3_paths('CLIENTES', 'DATOS_SOCIODEMOGRAFICOS', 'STG')
response = s3_handler.save_files_to_s3(df_dynamic, s3_target_path, format="parquet", mode="overwrite")
from pyspark.sql.functions import col, lit, coalesce, concat_ws, when, current_date
from pyspark.sql.functions import monotonically_increasing_id
df_acsele_datos_Contactabilidad_schema = df_macro_acsele.select(
    # === PK (único campo NOT NULL) ===
    monotonically_increasing_id().alias("customer_dcontactabilidad_id"),
    
    # === MAPEO 1: COD_TIPO_IDENTIFICACION → Cod_Tipo_Identificacion ===
    col("Cod_Tipo_Identificacion").alias("Cod_Tipo_Identificacion"),
    
    # === MAPEO 2: NID_NUMERO_IDENTIFICACION → Numero_Documento ===
    col("Numero_Documento").cast("string").alias("Numero_Documento"),
    
    # === MAPEO 3: PRIMER_NOMBRE → Primer_Nombre ===
    col("Correo_Electronico_contacto").alias("Correo_Electronico"),
    
    # === MAPEO 4: SEGUNDO_NOMBRE → Segundo_Nombre ===
    col("Tipo_Direccion_contacto").alias("Tipo_Direccion"),
    
    # === MAPEO 5: PRIMER_APELLIDO → Primer_Apellido ===
    col("Direccion_contacto").alias("Direccion"),
    
    # === MAPEO 6: SEGUNDO_APELLIDO → Segundo_Apellido ===
    col("Barrio_contacto").alias("Barrio"),
    
    col("Cod_Ciudad_contacto").alias("Cod_Ciudad"),
    
    col("Ciudad_contacto").alias("Ciudad"),
    
    col("Cod_Departamento_contacto").alias("Cod_Departamento"),
    
    col("Departamento_contacto").alias("Departamento"),
    
    col("Cod_Pais_contacto").alias("Cod_Pais"),
    
    col("Pais_contacto").alias("Pais"),
    
    lit("").alias("Tipo_Telefono"),
    
    coalesce(
        col("Indicativo_contacto"),
        col("Indicativo_value_contacto")  # Para PJ
    ).alias("Indicativo"),
    
    coalesce(
        col("Numero_Telefono_1_contacto"),col("Numero_Telefono_2_contacto"),col("Numero_Telefono_3_contacto"),col("Numero_Telefono_4_contacto"),col("Numero_Telefono_5_contacto"),
        col("Telefono_Oficina_contacto"),col("Telefono_Oficina_DOS_contacto")
    ).alias("Numero_Telefono"),
    
    lit("").alias("Extension"),

    # === METADATOS ===
    col("COD_FUENTE").alias("cod_fuente_fk"),
    current_date().alias("fecha_cargue"),
    lit("GLUE_JOB_ACSELE_SILVER").alias("job_cargue_id")
)

df_acsele_datos_Sociodemográficos_schema.show(3, truncate=False)
from awsglue import DynamicFrame
df_dynamic = DynamicFrame.fromDF(df_acsele_datos_Sociodemográficos_schema, glueContext, "fuentes_clientes")
s3_handler = S3Handler(BUCKET_SILVER, glueContext)
s3_prefix, s3_target_path = s3_handler.generate_s3_paths('CLIENTES', 'DATOS_UBICACION_Y_CONTACTABILIDAD', 'STG')
response = s3_handler.save_files_to_s3(df_dynamic, s3_target_path, format="parquet", mode="overwrite")
Cod_Tipo_Identificacion
Numero_Documento
Fecha_Nacimiento
Edad
Cod_Ciudad_Nacimiento
Ciudad_Nacimiento
Cod_Departamento_Nacimiento
Departamento_Nacimiento
Cod_Pais_Nacimiento
Pais_Nacimiento
Nacionalidad
Genero
Cod_Estado_Civil
Cod_Nivel_Estudios
Nivel_Estudios
Cod_Profesion
Profesion
Cod_Ocupacion
Ocupacion
Cod_Actividad_Economica
Numero_Hijos
Cod_Tipo_Sociedad
Tipo_Identificacion_Empresa
NIT_Empresa
Id_Afiliacion
Cod_Cargo_Empresa
Fecha_Ingreso_Empresa
Cod_Fuente
Fecha_Actualización/cargue
Id_cargue
def read_full_parquet(s3_target_path:str = None, recursive_file_lookup:str = "true",
                          merge_schema:str = "false", path_glob_filter:str = "*.parquet"):
    return spark.read.option("recursiveFileLookup", recursive_file_lookup)\
        .option("mergeSchema", merge_schema)\
        .option("pathGlobFilter", path_glob_filter)\
        .parquet(s3_target_path)

# ================================================================================
# Carga de tablas STG (DB,DS,DC)
# ================================================================================



S3_PATH_DATOS_BASICOS_STG = f's3://alfa-datalake-dev-silver/CLIENTES/DATOS_BASICOS/CLIENTES_DATOS_BASICOS_STG/'
S3_PATH_DATOS_SOCIODEMOGRAFICOS_STG = f's3://alfa-datalake-dev-silver/CLIENTES/DATOS_SOCIODEMOGRAFICOS/CLIENTES_DATOS_SOCIODEMOGRAFICOS_STG/'
S3_PATH_DATOS_CONTACTABILIDAD_STG = f's3://alfa-datalake-dev-silver/CLIENTES/DATOS_UBICACION_Y_CONTACTABILIDAD/CLIENTES_DATOS_UBICACION_Y_CONTACTABILIDAD_STG/'


df_datos_basicos_STG = read_full_parquet(s3_target_path=S3_PATH_DATOS_BASICOS_STG)
df_datos_sociodemografico_STG = read_full_parquet(s3_target_path=S3_PATH_DATOS_SOCIODEMOGRAFICOS_STG)
df_datos_contactabilidad_STG = read_full_parquet(s3_target_path=S3_PATH_DATOS_CONTACTABILIDAD_STG)
df_datos_contactabilidad_STG.printSchema()
df_datos_basicos_STG.show(2,0)
df_datos_sociodemografico_STG.show(2,0)
df_datos_contactabilidad_STG.show(2,0)
# ================================================================================
# Funciones de limpieza 
# ================================================================================
import re 
import unicodedata

def limpiar_y_formatear(texto):
    if not texto:
        return ""
    #quitar tildes
    texto=unicodedata.normalize('NFD', texto).encode('ASCII', 'ignore').decode('utf-8')
    
    #dejar solo letras y espacios
    texto = re.sub(r'[^a-zA-Z\s]', '', texto)
    
    texto = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', texto)
    
    texto = ' '.join(texto.split()))
    
    return texto.capitalize()

limpiar_udf
import re
import unicodedata
import datetime
import math

def aproximar_numero_mayor(valor):
    try:
        return math.ceil(float(valor))
    except:
        return None

def calcular_edad(fecha_nacimiento, formato="%d/%m/%Y"):
    try:
        nacimiento = datetime.datetime.strptime(fecha_nacimiento, formato)
        hoy = datetime.datetime.today()
        edad = hoy.year - nacimiento.year - ((hoy.month, hoy.day) < (nacimiento.month, nacimiento.day))
        return edad
    except:
        return None

def concatenar_si_aplica(*args):
    return ' '.join([str(arg) for arg in args if arg])

def convertir_minusculas(texto):
    return texto.lower() if texto else texto

def convertir_mayusculas(texto):
    return texto.upper() if texto else texto

def valor_absoluto(valor):
    try:
        return abs(float(valor))
    except:
        return None

def ajustar_dia_mes_fuera_rango(fecha_str):
    try:
        dia, mes, anio = map(int, fecha_str.split('/'))
        dia = min(max(dia, 1), 31)
        mes = min(max(mes, 1), 12)
        return f"{dia:02d}/{mes:02d}/{anio}"
    except:
        return fecha_str

def solo_numeros(texto):
    return re.sub(r'[^0-9]', '', texto)

def solo_alfanumerico(texto):
    return re.sub(r'[^a-zA-Z0-9]', '', texto)

def eliminar_caracteres_especiales(texto):
    texto = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', texto)
    texto = re.sub(r'[^\w\s]', '', texto)
    return texto

def eliminar_digito_verificacion_nit(nit):
    return nit.split('-')[0] if '-' in nit else nit

def eliminar_espacios_varios(texto):
    return ' '.join(texto.split())

def eliminar_letras_si_documento(texto, tipo_doc):
    tipos = ['CC', 'TI', 'RC', 'CE', 'NIT']
    return re.sub(r'[A-Za-z]', '', texto.upper()) if tipo_doc in tipos else texto

def limpiar_correo(correo):
    correo = re.sub(r'[\\"\\,\\:\\;\\<\\>\\(\\)\\[\\]\\{\\}\\|]', '', correo)
    correo = eliminar_tildes(correo)
    correo = correo.replace('..', '.')
    correo = correo.strip('.')
    correo = re.sub(r'\.@', '@', correo)
    correo = re.sub(r'@+', '@', correo)
    return correo.lower()

def eliminar_tildes(texto):
    if not texto:
        return texto
    texto = unicodedata.normalize('NFD', texto)
    texto = texto.encode('ASCII', 'ignore').decode('utf-8')
    return texto

def eliminar_textos_caracteres_especiales(texto):
    texto = eliminar_caracteres_especiales(texto)
    texto = eliminar_tildes(texto)
    return texto

def homologar_ciudad_departamento(nombre, referencia_dict, tipo='ciudad'):
    return referencia_dict.get(nombre.upper(), '09999' if tipo == 'ciudad' else '00')

def obtener_nombre_desde_codigo(codigo, referencia_dict):
    return referencia_dict.get(codigo, '')

def reemplazar_multiples_espacios(texto):
    return re.sub(r'\s+', ' ', texto)

def seleccionar_fecha_minima(lista_fechas, formato="%d/%m/%Y"):
    fechas = []
    for fecha in lista_fechas:
        try:
            fechas.append(datetime.datetime.strptime(fecha, formato))
        except:
            continue
    return min(fechas).strftime(formato) if fechas else None

def validar_fecha(fecha_str, formato="%d/%m/%Y"):
    try:
        fecha = datetime.datetime.strptime(fecha_str, formato)
        if fecha.year < 1900 or fecha >= datetime.datetime.today():
            return None
        return fecha_str
    except:
        return None

def ajustar_fecha_invalida(fecha_str, formato="%d/%m/%Y"):
    try:
        dia, mes, anio = map(int, fecha_str.split('/'))
        mes = min(max(mes, 1), 12)
        ultimo_dia = (datetime.date(anio, mes % 12 + 1, 1) - datetime.timedelta(days=1)).day
        dia = min(dia, ultimo_dia)
        return f"{dia:02d}/{mes:02d}/{anio}"
    except:
        return fecha_str

def cambiar_fecha_invalida_a_null(fecha_str, formato="%d/%m/%Y"):
    try:
        fecha = datetime.datetime.strptime(fecha_str, formato)
        if fecha.year < 1900 or fecha >= datetime.datetime.today():
            return None
        return fecha_str
    except:
        return None

def limpiar_tipo_identificacion(texto):
    
    if texto is None:
            return None  

    texto = eliminar_tildes(texto)
    texto = convertir_minusculas(texto)
    texto = eliminar_espacios_varios(texto)
    texto = eliminar_caracteres_especiales(texto)
    return texto

def limpiar_numero_identificacion(texto, tipo_doc):
    if texto is None:
            return None 
    texto = solo_alfanumerico(texto)
    texto = eliminar_espacios_varios(texto)
    texto = eliminar_digito_verificacion_nit(texto)
    texto = eliminar_letras_si_documento(texto, tipo_doc)
    return texto

def limpiar_nombres(texto):
    if texto is None:
            return None 
    texto = eliminar_caracteres_especiales(texto)
    texto = convertir_mayusculas(texto)
    texto = eliminar_tildes(texto)
    texto = re.sub(r'[0-9]', '', texto) if re.search(r'[a-zA-Z]', texto) else texto
    texto = eliminar_espacios_varios(texto)
    return texto

def limpiar_nombre_completo(texto):
    if texto is None:
            return None 
    texto = reemplazar_multiples_espacios(texto)
    texto = texto.strip()
    return texto

def limpiar_razon_social(texto):
    if texto is None:
            return None 
    texto = convertir_mayusculas(texto)
    texto = eliminar_tildes(texto)
    texto = reemplazar_multiples_espacios(texto)
    texto = texto.strip()
    return texto

def limpiar_fecha(fecha_str):
    if fecha_str is None:
            return None 
    fecha_str = eliminar_caracteres_especiales(fecha_str.strftime("%d/%m/%Y"))
    fecha_str = eliminar_espacios_varios(fecha_str)
    fecha_str = ajustar_dia_mes_fuera_rango(fecha_str)
    fecha_str = ajustar_fecha_invalida(fecha_str)
    fecha_str = cambiar_fecha_invalida_a_null(fecha_str)
    return fecha_str

def limpiar_fecha_no_null(fecha_str):
    if fecha_str is None:
            return None 
    fecha_str = eliminar_caracteres_especiales(fecha_str.strftime("%d/%m/%Y"))
    fecha_str = eliminar_espacios_varios(fecha_str)
    fecha_str = ajustar_dia_mes_fuera_rango(fecha_str)
    fecha_str = ajustar_fecha_invalida(fecha_str)
    return fecha_str

def limpiar_ciudad(texto):
    if texto is None:
            return None 
    texto = eliminar_caracteres_especiales(texto)
    texto = convertir_mayusculas(texto)
    texto = eliminar_tildes(texto)
    texto = eliminar_espacios_varios(texto)
    return texto

def limpiar_pais(texto):
    if texto is None:
            return None 
    texto = eliminar_caracteres_especiales(texto)
    texto = convertir_minusculas(texto)
    texto = eliminar_tildes(texto)
    texto = eliminar_espacios_varios(texto)
    return texto

def limpiar_texto_min(texto):
    if texto is None:
            return None 
    texto = eliminar_caracteres_especiales(texto)
    texto = convertir_minusculas(texto)
    texto = eliminar_tildes(texto)
    texto = eliminar_espacios_varios(texto)
    return texto

def limpiar_direccion(texto):
    if texto is None:
            return None 
    texto = eliminar_caracteres_especiales(texto)
    texto = convertir_mayusculas(texto)
    texto = eliminar_tildes(texto)
    texto = eliminar_espacios_varios(texto)
    texto = texto.strip()
    return texto

def limpiar_genero(texto):
    if texto is None:
            return None 
    texto = eliminar_caracteres_especiales(texto)
    texto = convertir_minusculas(texto)
    texto = eliminar_tildes(texto)
    texto = eliminar_espacios_varios(texto)
    return texto

def limpiar_correo_electronico(correo):
    if correo is None:
            return None 
    return limpiar_correo(correo)

def limpiar_direccion(texto):
    if texto is None:
            return None
    
    texto = eliminar_caracteres_especiales(texto)
    texto = convertir_mayusculas(texto)
    texto = eliminar_tildes(texto)
    texto = eliminar_espacios_varios(texto)
    return texto

def limpiar_numero_telefonico(texto):
    if texto is None:
            return None
    texto = solo_numeros(texto)
    texto = eliminar_espacios_varios(texto)
    return texto[:10] if len(texto) > 10 else texto

def limpiar_extension(texto):
    if texto is None:
            return None
    return solo_numeros(texto)

def limpiar_numero_hijos(valor):
    valor = solo_numeros(valor)
    valor = valor_absoluto(valor)
    valor = aproximar_numero_mayor(valor)
    return valor

def limpiar_nit_empresa(texto, tipo_doc):
    if texto is None:
            return None
    texto = re.sub(r'[^a-zA-Z0-9\\-_]', '', texto)
    texto = eliminar_espacios_varios(texto)
    texto = eliminar_letras_si_documento(texto, tipo_doc)
    return texto

def limpiar_nombre_empresa(texto):
    if texto is None:
            return None
    texto = convertir_mayusculas(texto)
    texto = eliminar_tildes(texto)
    texto = eliminar_espacios_varios(texto)
    return texto

def limpiar_cargo_empresa(texto):
    if texto is None:
            return None
    texto = eliminar_tildes(texto)
    texto = eliminar_espacios_varios(texto)
    return texto

def limpiar_fecha_ingreso_empresa(fecha_str):
    
    return limpiar_fecha(fecha_str)


def limpiar_numero_telefono(numero, tipo_telefono):
    if numero is None:
        return None

    # Eliminar espacios y caracteres no numéricos
    numero_limpio = re.sub(r'[^0-9]', '', str(numero).strip())

    # Ajustar longitud según tipo de teléfono
    
    numero_limpio = numero_limpio[:10]  # máximo 10 dígitos

limpiar_tipo_identificacion_udf = udf(limpiar_tipo_identificacion, StringType())

limpiar_numero_identificacion_udf = udf(limpiar_numero_identificacion, StringType())

limpiar_nombres_udf = udf(limpiar_nombres, StringType())

limpiar_nombre_completo_udf = udf(limpiar_nombre_completo, StringType())

limpiar_razon_social_udf = udf(limpiar_razon_social, StringType())

limpiar_fecha_udf = udf(limpiar_fecha, StringType())

limpiar_fecha_no_null_udf = udf(limpiar_fecha_no_null, StringType())

limpiar_ciudad_udf = udf(limpiar_ciudad, StringType())

limpiar_pais_udf = udf(limpiar_pais, StringType())

limpiar_genero_udf = udf(limpiar_genero, StringType())

limpiar_texto_min_udf=udf(limpiar_texto_min, StringType())

limpiar_correo_electronico_udf = udf(limpiar_correo_electronico, StringType())

limpiar_direccion_udf = udf(limpiar_direccion, StringType())

limpiar_numero_telefonico_udf = udf(limpiar_numero_telefonico, StringType())

limpiar_extension_udf = udf(limpiar_extension, StringType())

limpiar_numero_hijos_udf = udf(limpiar_numero_hijos, StringType())

limpiar_nit_empresa_udf = udf(limpiar_nit_empresa, StringType())

limpiar_nombre_empresa_udf = udf(limpiar_nombre_empresa, StringType())

limpiar_cargo_empresa_udf = udf(limpiar_cargo_empresa, StringType())

limpiar_fecha_ingreso_empresa_udf = udf(limpiar_fecha_ingreso_empresa, StringType())

limpiar_direccion_udf = udf(limpiar_direccion, StringType())

limpiar_numero_telefono_udf = udf(limpiar_numero_telefono, StringType())

# Crear DataFrame de ejemplo
data = [("CC", "123-456", "José", "Luis", "Pérez", "Gómez", "José Luis Pérez Gómez", "31/04/2022", "NAT", "Bogotá", "30/02/2020", "ACT", "EMP", "Privada", "SRC01", "15/08/2023", "JOB123", "CUST001")]
columns = ["cod_tipo_identificacion", "numero_documento", "primer_nombre", "segundo_nombre", "primer_apellido", "segundo_apellido", "nombre_completo", "fecha_vinculacion", "cod_tipo_persona", "ciudad_expedicion_documento", "fecha_expedicion_documento", "cod_estado", "cod_tipo_empresa", "tipo_empresa", "cod_fuente_fk", "fecha_cargue", "job_cargue_id", "customer_dbasicos_id"]
df = spark.createDataFrame(data, columns)

# Aplicar UDFs
df_limpio = df \
    .withColumn("cod_tipo_identificacion_limpio", limpiar_tipo_identificacion_udf(col("cod_tipo_identificacion"))) \
    .withColumn("numero_documento_limpio", limpiar_numero_identificacion_udf(col("numero_documento"),col("cod_tipo_identificacion"))) \
    .withColumn("primer_nombre_limpio", limpiar_nombres_udf(col("primer_nombre"))) \
    .withColumn("segundo_nombre_limpio", limpiar_nombres_udf(col("segundo_nombre"))) \
    .withColumn("primer_apellido_limpio", limpiar_nombres_udf(col("primer_apellido"))) \
    .withColumn("segundo_apellido_limpio", limpiar_nombres_udf(col("segundo_apellido"))) \
    .withColumn("nombre_completo_limpio", limpiar_nombre_completo_udf(col("nombre_completo"))) \
    .withColumn("fecha_vinculacion_limpia", limpiar_fecha_udf(col("fecha_vinculacion"))) \
    .withColumn("ciudad_expedicion_documento_limpia", limpiar_ciudad_udf(col("ciudad_expedicion_documento"))) \
    .withColumn("fecha_expedicion_documento_limpia", limpiar_fecha_udf(col("fecha_expedicion_documento"))) \
    .withColumn("fecha_cargue_limpia", limpiar_fecha_udf(col("fecha_cargue")))



df_datos_basicos_STG_limpio = df_datos_basicos_STG \
    .withColumn("cod_tipo_identificacion_limpio", limpiar_tipo_identificacion_udf(col("cod_tipo_identificacion"))) \
    .withColumn("numero_documento_limpio", limpiar_numero_identificacion_udf(col("numero_documento"),col("cod_tipo_identificacion"))) \
    .withColumn("primer_nombre_limpio", limpiar_nombres_udf(col("primer_nombre"))) \
    .withColumn("segundo_nombre_limpio", limpiar_nombres_udf(col("segundo_nombre"))) \
    .withColumn("primer_apellido_limpio", limpiar_nombres_udf(col("primer_apellido"))) \
    .withColumn("segundo_apellido_limpio", limpiar_nombres_udf(col("segundo_apellido"))) \
    .withColumn("nombre_completo_limpio", limpiar_nombre_completo_udf(col("nombre_completo"))) \
    .withColumn("fecha_vinculacion_limpia", limpiar_fecha_udf(col("fecha_vinculacion"))) \
    .withColumn("ciudad_expedicion_documento_limpia", limpiar_ciudad_udf(col("ciudad_expedicion_documento"))) \
    .withColumn("fecha_expedicion_documento_limpia", limpiar_fecha_udf(col("fecha_expedicion_documento"))) \
    .withColumn("tipo_empresa_limpio", limpiar_tipo_identificacion_udf(col("tipo_empresa"))) \
    .withColumn("fecha_cargue_limpia", limpiar_fecha_udf(col("fecha_cargue")))

df_datos_basicos_STG_limpio.printSchema()



df_datos_basicos_STG_limpio= df_datos_basicos_STG_limpio.select(
    # === PK (único campo NOT NULL) ===
    
    # === MAPEO 1: COD_TIPO_IDENTIFICACION 
    col("cod_tipo_identificacion_limpio").alias("Cod_Tipo_Identificacion"),
    
    # === MAPEO 2: NID_NUMERO_IDENTIFICACION 
    col("numero_documento_limpio").cast("string").alias("Numero_Documento"),
    
    # === MAPEO 3: PRIMER_NOMBRE 
    col("primer_nombre_limpio").alias("Primer_Nombre"),
    
    # === MAPEO 4: SEGUNDO_NOMBRE 
    col("segundo_nombre_limpio").alias("Segundo_Nombre"),
    
    # === MAPEO 5: PRIMER_APELLIDO
    col("primer_apellido_limpio").alias("Primer_Apellido"),
    
    # === MAPEO 6: SEGUNDO_APELLIDO 
    col("segundo_apellido_limpio").alias("Segundo_Apellido"),
    
    # === MAPEO 7: Nombre_Completo
   
    col("nombre_completo_limpio").alias("Nombre_Completo"),
    
    # === MAPEO 8: FEC_EXPEDICION_DOCUMENT → Fecha_Expedición_Documento ===
    col("fecha_expedicion_documento_limpia").alias("Fecha_Expedición_Documento"),
    
    # === MAPEO 9: COD_CIUDAD_EXPEDICION_DOCU → Ciudad_Expedición_Documento ===
    col("ciudad_expedicion_documento_limpia").alias("Ciudad_Expedición_Documento"),
    
    # === MAPEO 10: FEC_REGISTRO → Fecha_Vinculacion ===
    col("fecha_vinculacion_limpia").alias("Fecha_Vinculacion"),
    
    # === MAPEO 11: COD_ESTADO → Cod_Estado ===
    col("cod_estado").alias("Cod_Estado"),
    # === MAPEO 12: Cod_Tipo_Persona → Cod_Tipo_Persona ===
    col("cod_tipo_persona").alias("Cod_Tipo_Persona"),   
    # === MAPEO 13: COD_TIPO_EMPRESA → Cod_Tipo_Empresa ===
    col("Cod_Tipo_Empresa").alias("Cod_Tipo_Empresa"),
    
    # === MAPEO 14: NOMBRE (tipos_empresa) → Tipo_Empresa ===
    col("tipo_empresa_limpio").alias("Tipo_Empresa"),
    
    
    # === METADATOS ===
    col("cod_fuente_fk"),
    
    col("job_cargue_id"),
    
    col("fecha_cargue"),

    col("customer_dbasicos_id"))
df_datos_basicos_STG_limpio.printSchema()
df_datos_sociodemografico_STG_limpio = df_datos_sociodemografico_STG \
    .withColumn("cod_tipo_identificacion_limpio", limpiar_tipo_identificacion_udf(col("cod_tipo_identificacion"))) \
    .withColumn("numero_documento_limpio", limpiar_numero_identificacion_udf(col("numero_documento"), col("cod_tipo_identificacion"))) \
    .withColumn("fecha_nacimiento_limpia", limpiar_fecha_udf(col("fecha_nacimiento"))) \
    .withColumn("ciudad_nacimiento_limpia", limpiar_ciudad_udf(col("ciudad_nacimiento"))) \
    .withColumn("departamento_nacimiento_limpio", limpiar_ciudad_udf(col("departamento_nacimiento"))) \
    .withColumn("pais_nacimiento_limpio", limpiar_texto_min_udf(col("pais_nacimiento"))) \
    .withColumn("nacionalidad_limpia", limpiar_texto_min_udf(col("nacionalidad"))) \
    .withColumn("genero_limpio", limpiar_texto_min_udf(col("genero"))) \
    .withColumn("estado_civil_limpio", limpiar_texto_min_udf(col("cod_estado_civil"))) \
    .withColumn("nivel_estudios_limpio", limpiar_texto_min_udf(col("nivel_estudios"))) \
    .withColumn("profesion_limpia", limpiar_texto_min_udf(col("profesion"))) \
    .withColumn("ocupacion_limpia", limpiar_texto_min_udf(col("ocupacion"))) \
    .withColumn("actividad_economica_limpia", limpiar_texto_min_udf(col("cod_actividad_economica"))) \
    .withColumn("tipo_sociedad_limpia", limpiar_texto_min_udf(col("cod_tipo_sociedad"))) \
    .withColumn("tipo_id_empresa_limpio", limpiar_texto_min_udf(col("tipo_id_empresa"))) \
    .withColumn("nit_empresa_limpio", limpiar_nit_empresa_udf(col("nit_empresa"), col("tipo_id_empresa"))) \
    .withColumn("ciudad_expedicion_documento_limpia", limpiar_ciudad_udf(col("ciudad_expedicion_documento"))) \
    .withColumn("fecha_expedicion_documento_limpia", limpiar_fecha_no_null_udf(col("fecha_expedicion_documento"))) \
    .withColumn("tipo_empresa_limpio", limpiar_texto_min_udf(col("tipo_empresa"))) 
df_datos_sociodemografico_STG_limpio=df_datos_sociodemografico_STG_limpio.select(
    # === PK (único campo NOT NULL) ===
    
    # === MAPEO 1: COD_TIPO_IDENTIFICACION 
    col("cod_tipo_identificacion_limpio").alias("Cod_Tipo_Identificacion"),
    
    # === MAPEO 2: NID_NUMERO_IDENTIFICACION 
    col("numero_documento_limpio").cast("string").alias("Numero_Documento"),
    
    # === MAPEO 3: PRIMER_NOMBRE 
    col("fecha_nacimiento_limpia").alias("Fecha_Nacimiento"),
    
    # === MAPEO 4: SEGUNDO_NOMBRE 
    col("ciudad_nacimiento_limpia").alias("Ciudad_Nacimiento"),
    
    # === MAPEO 5: PRIMER_APELLIDO
    col("departamento_nacimiento_limpio").alias("Departamento_Nacimiento"),
    
    # === MAPEO 6: SEGUNDO_APELLIDO 
    col("pais_nacimiento_limpio").alias("Pais_Nacimiento"),
    
    # === MAPEO 7: Nombre_Completo
   
    col("nacionalidad_limpia").alias("Nacionalidad"),
    
    # === MAPEO 8: FEC_EXPEDICION_DOCUMENT → Fecha_Expedición_Documento ===
    col("genero_limpio").alias("Genero"),
    
    # === MAPEO 9: COD_CIUDAD_EXPEDICION_DOCU → Ciudad_Expedición_Documento ===
    col("estado_civil_limpio").alias("Cod_Estado_Civil"),
    
    # === MAPEO 10: FEC_REGISTRO → Fecha_Vinculacion ===
    col("nivel_estudios_limpio").alias("Nivel_Estudios"),
    
    # === MAPEO 11: COD_ESTADO → Cod_Estado ===
    col("profesion_limpia").alias("Profesion"),
    # === MAPEO 12: Cod_Tipo_Persona → Cod_Tipo_Persona ===
    col("ocupacion_limpia").alias("Ocupacion_Limpia"),   
    # === MAPEO 13: COD_TIPO_EMPRESA → Cod_Tipo_Empresa ===
    col("Cod_Tipo_Empresa").alias("Cod_Tipo_Empresa"),
    
    # === MAPEO 14: NOMBRE (tipos_empresa) → Tipo_Empresa ===
    col("actividad_economica_limpia").alias("Cod_Actividad_Economica"),
    
    col("tipo_sociedad_limpia").alias("Cod_Tipo_Sociedad"),
    
    col("tipo_id_empresa_limpio").alias("Tipo_Id_Empresa"),
    
    col("nit_empresa_limpio").alias("Nit_Empresa"),
    
    col("ciudad_expedicion_documento_limpia").alias("Ciudad_Expedicion_Documento"),
    
    col("fecha_expedicion_documento_limpia").alias("Fecha_Expedicion_Documento"),
    
    col("tipo_empresa_limpio").alias("Tipo_Empresa"),
    
    # === METADATOS ===
    col("cod_fuente_fk"),
    
    col("job_cargue_id"),
    
    col("fecha_cargue"),

    col("customer_dsociodemograficos_id"))

df_datos_contactabilidad_STG_limpio = df_datos_contactabilidad_STG     \
    .withColumn("cod_tipo_identificacion_limpio", limpiar_tipo_identificacion_udf(col("cod_tipo_identificacion"))) \
    .withColumn("numero_documento_limpio", limpiar_numero_identificacion_udf(col("numero_documento"), col("cod_tipo_identificacion"))) \
    .withColumn("correo_electronico_limpio", limpiar_correo_electronico_udf(col("correo_electronico"))) \
    .withColumn("direccion_limpia", limpiar_direccion_udf(col("direccion"))) \
    .withColumn("barrio_limpio", limpiar_direccion_udf(col("barrio"))) \
    .withColumn("ciudad_limpia", limpiar_ciudad_udf(col("ciudad"))) \
    .withColumn("departamento_limpio", limpiar_ciudad_udf(col("departamento"))) \
    .withColumn("pais_limpio", limpiar_texto_min_udf(col("pais"))) \
    .withColumn("indicativo_limpio", limpiar_texto_min_udf(col("indicativo"))) \
    .withColumn("numero_telefono_limpio", limpiar_numero_telefono_udf(col("numero_telefono"),col("tipo_telefono"))) \
    .withColumn("extension_limpia", limpiar_extension_udf(col("extension")))

df_datos_contactabilidad_STG_limpio = df_datos_contactabilidad_STG_limpio.select(
    # === PK (único campo NOT NULL) ===

    # === MAPEO 1: COD_TIPO_IDENTIFICACION
    col("cod_tipo_identificacion_limpio").alias("Cod_Tipo_Identificacion"),

    # === MAPEO 2: NID_NUMERO_IDENTIFICACION
    col("numero_documento_limpio").cast("string").alias("Numero_Documento"),

    # === MAPEO 3: PRIMER_NOMBRE
    col("correo_electronico_limpio").alias("Correo_Electronico"),

    # === MAPEO 4: SEGUNDO_NOMBRE
    col("direccion_limpia").alias("Direccion"),

    # === MAPEO 5: PRIMER_APELLIDO
    col("barrio_limpio").alias("Barrio"),

    # === MAPEO 6: SEGUNDO_APELLIDO
    col("ciudad_limpia").alias("Ciudad"),

    # === MAPEO 7: Nombre_Completo

    col("departamento_limpio").alias("Departamento"),

    # === MAPEO 8: FEC_EXPEDICION_DOCUMENT → Fecha_Expedición_Documento ===
    col("pais_limpio").alias("Pais"),

    # === MAPEO 9: COD_CIUDAD_EXPEDICION_DOCU → Ciudad_Expedición_Documento ===
    col("indicativo_limpio").alias("Indicativo"),

    # === MAPEO 10: FEC_REGISTRO → Fecha_Vinculacion ===
    col("numero_telefono_limpio").alias("Numero_Telefono"),

    # === MAPEO 11: COD_ESTADO → Cod_Estado ===
    col("extension_limpia").alias("Extension"),

    # === MAPEO 12: Cod_Tipo_Persona → Cod_Tipo_Persona ===
    col("tipo_telefono").alias("Tipo_Telefono"),

    # === METADATOS ===
    col("cod_fuente_fk"),

    col("job_cargue_id"),

    col("fecha_cargue"),

    col("customer_dubic_contact_id"))

df_datos_contactabilidad_STG_limpio.show(3,0)
# acsele_acsele_alfa_prethirdparty
df_prethirdparty = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "acsele_acsele_alfa_prethirdparty",
    ["PK", "STATIC", "TIPODOCUMENTOINPUT", "DOCUMENTOIDENTIDADINPUT", "NOMBREPREDETERMINADOINPUT",
     "FECHAINCLUSIONINPUT", "STATUS", "EMAILINPUT", "INDICATIVOOFICINAVALUE", "TELEFONOOFICINAVALUE", "TELEFONOOFICINACOMPLETINPUT"]
)



# acsele_acsele_alfa_personajuridica (campos reales)
df_personajuridica = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "acsele_acsele_alfa_personajuridica",
    ["PK", "STATIC", "NOMBRECONTACTOINPUT", "RAZONSOCIALINPUT", "TIPOEMPRESAVALUE", "TIPOEMPRESAINPUT",
     "ACTIVIDADECONOMICAVALUE", "TIPOSOCIEDADINPUT", "EXTENSIONDOSINPUT", "TELEFONOOFICINADOSINPUT"]
)
# acsele_acsele_alfa_personanatural
df_personanatural = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "acsele_acsele_alfa_personanatural",
    ["PK", "STATIC", "NOMBREINPUT", "SEGUNDONOMBREINPUT", "APELLIDOINPUT", "APELLIDOMATERNOINPUT",
     "ESTADOCIVILINPUT", "GRADOINSTITUCIONINPUT", "GRADOINSTITUCIONVALUE", "OCUPACIONINPUT",
     "INDICATIVOFIJOINPUT", "TELEFONOCELULARINPUT", "TELEFONOFIJOCOMPLETOINPUT", "TELEFONOINPUT"]
)
# acsele_acsele_alfa_aseguradovida
df_aseguradovida = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "acsele_acsele_alfa_aseguradovida",
    ["PK", "STATIC", "FECHANACIMIENTOINPUT", "CIUDADINPUT", "CIUDADVALUE",
     "DEPARTAMENTOINPUT", "DEPARTAMENTOVALUE", "SEXOINPUT"]
)
# acsele_acsele_alfa_oavidaap
df_oavidaap = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "acsele_acsele_alfa_oavidaap",
    ["PK", "STATIC", "ESTADOCIVILINPUT", "HIJOSINPUT"]
)
# acsele_acsele_alfa_pomihogarseguro
df_pomihogarseguro = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "acsele_acsele_alfa_pomihogarseguro",
    ["PK", "STATIC", "IDEMPLEADORINPUT", "EMPLEADORINPUT"]
)
# acsele_acsele_alfa_direccion
df_direccion = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "acsele_acsele_alfa_direccion",
    ["PK", "STATIC", "TIPODIRECCIONTERCEROINPUT", "DIRECCIONINPUT", "BARRIOINPUT",
     "CIUDADINPUT", "CIUDADVALUE", "DEPARTAMENTOINPUT", "DEPARTAMENTOVALUE", "PAISINPUT", "PAISVALUE"]
)
# alfaprod_general_acu_personas_juridicas
df_personas_juridicas = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "alfaprod_general_acu_personas_juridicas",
    ["COD_TIPO_IDENTIFICACION", "NID_NUMERO_IDENTIFICACION", "NID_CONSECUTIVO", "RAZON_SOCIAL", "COD_TIPO_EMPRESA"]
)

# alfaprd_general_acu_personas_naturales
df_personas_naturales = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "alfaprd_general_acu_personas_naturales",
    ["COD_TIPO_IDENTIFICACION", "NID_NUMERO_IDENTIFICACION", "PRIMER_NOMBRE", "SEGUNDO_NOMBRE", "PRIMER_APELLIDO",
     "SEGUNDO_APELLIDO", "FEC_EXPEDICION_DOCUMENTO", "COD_CIUDAD_EXPEDICION_DOCU", "FEC_REGISTRO", "COD_ESTADO",
     "FEC_NACIMIENTO", "COD_CIUDAD_NACIMIENTO", "COD_DEPARTAMENTO_NACIMIENTO", "NACIONALIDAD", "GENERO",
     "COD_ESTADO_CIVIL", "COD_NIVEL_EDUCATIVO", "COD_PROFESION", "COD_OCUPACION", "COD_ACTIVIDAD_ECONOMICA_CIIU"]
)
# alfaprod_general_acu_tipos_empresas
df_tipos_empresas = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "alfaprod_general_acu_tipos_empresas",
    ["COD_TIPO_EMPRESA", "NOMBRE"]
)
# alfaprod_general_cgr_ciudades_departamentos
df_ciudades_departamentos = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "alfaprod_general_cgr_ciudades_departamentos",
    ["NOMBRE", "COD_DEPARTAMENTO", "COD_PAIS"]
)
# alfaprod_general_acu_niveles_educativos
df_niveles_educativos = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "alfaprod_general_acu_niveles_educativos",
    ["COD_NIVEL_EDUCATIVO", "NOMBRE"]
)
# alfaprod_general_acu_profesiones
df_profesiones = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "alfaprod_general_acu_profesiones",
    ["COD_PROFESION", "NOMBRE"]
)
# alfaprod_arpprod_par_afiliaciones
df_afiliaciones = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "alfaprod_arpprod_par_afiliaciones",
    ["NID_AFILIACION", "COD_TIPO_IDENTIFICACION", "NID_NUMERO_IDENTIFICACION"]
)
# alfaprod_arpprod_par_trabajadores
df_trabajadores = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "alfaprd_arpprod_par_trabajadores",
    ["NID_TRABAJADOR", "NID_AFILIACION", "COD_OCUPACION", "FEC_INGRESO_EMPRESA"]
)
# alfaprod_general_acu_personas_localizaciones (campos reales)
df_localizaciones = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "alfaprod_general_acu_personas_localizaciones",
    ["cod_tipo_identificacion", "nid_numero_identificacion", "nid_consecutivo", "E_MAIL", "COD_TIPO_LOCALIZACION",
     "DIRECCION", "BARRIO_VEREDA", "COD_CIUDAD", "INDICATIVO", "NUMERO_CELULAR", "NUMERO_TELEFONO", "EXTENSION_TELEFONO"]
)
# alfaprod_general_cgr_paises
df_paises = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "alfaprod_general_cgr_paises",
    ["COD_PAIS", "NOMBRE"]
)
# alfaprod_general_cgr_departamentos
df_departamentos = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "alfaprod_general_cgr_departamentos",
    ["COD_DEPARTAMENTO", "NOMBRE"]
)
# smb_cardiff_customer
df_cardiff_customer = cargar_datos_desde_catalogo(
    glueContext,
    "datalake-dev-bronce-catalog-db",
    "smb_cardiff_customer",
    ["CUSTOMER_NO", "ID_TYPE_NAME", "ID_CO", "FIRST_NAME", "MIDDLE_NAME", "FIRST_SURNAME", "SECOND_SURNAME", "FULLNAME",
     "CREATION_DATE", "CUSTOMER_TYPE", "BIRTH_DATE", "BIRTH_YEARS_OLD", "BIRTH_CITY_CODE", "BIRTH_CITY_NAME",
     "BIRTH_STATE", "BIRTH_STATE_NAME", "BIRTH_COUNTRY_CODE", "BIRTH_COUNTRY_NAME", "NATIONALITY", "GENDER_TYPE",
     "GENDER", "MARITAL_STATUS", "OCCUPATION_CODE", "OCCUPATION_NAME", "NUM_CHILDS", "MAIL", "ADDRESS_TYPE",
     "ADDRESS_NAME", "ADDRESS_NEIHBORHOOD", "ADDRESS_CITY_CODE", "ADDRESS_CITY_NAME", "ADDRESS_STATE_CODE",
     "ADDRESS_STATE_NAME", "ADDRESS_COUNTRY_CODE", "ADDRESS_COUNTRY_NAME", "PHONE_CODE_1", "PHONE_CODE_2",
     "PHONE_CODE_3", "CELLPHONE1", "CELLPHONE2", "PHONE_NUMBER_1", "PHONE_NUMBER_2", "PHONE_NUMBER_3",
     "PHONE_EXT_1", "PHONE_EXT_2", "PHONE_EXT_3"]
)
"""ptp = glueContext.create_dynamic_frame.from_catalog(database=DATABASE_NAME, table_name='acsele_acsele_alfa_prethirdparty')
pn = glueContext.create_dynamic_frame.from_catalog(database=DATABASE_NAME, table_name='acsele_acsele_alfa_personanatural')
pj = glueContext.create_dynamic_frame.from_catalog(database=DATABASE_NAME, table_name='acsele_acsele_alfa_personajuridica')
av = glueContext.create_dynamic_frame.from_catalog(database=DATABASE_NAME, table_name='acsele_acsele_alfa_aseguradovida')
pa = glueContext.create_dynamic_frame.from_catalog(database=DATABASE_NAME, table_name='acsele_acsele_alfa_pomihogarseguro')
oavi = glueContext.create_dynamic_frame.from_catalog(database=DATABASE_NAME, table_name='acsele_acsele_alfa_oavidaap')
pomi = glueContext.create_dynamic_frame.from_catalog(database=DATABASE_NAME, table_name='acsele_acsele_alfa_pomihogarseguro')
#dyf.printSchema() 
df_ptp= ptp.toDF()
df_pn= pn.toDF()
df_pj= pj.toDF()
df_av= av.toDF()
df_pa= pa.toDF()
df_oavi= oavi.toDF()
df_pomi= pomi.toDF()"""

# Uso
df_nulidad = calcular_porcentaje_nulidad(df)

df_nulidad.show(150,0)

df_con_fecha = df.withColumn("fecha_valida", to_date(col("FECHANACIMIENTOINPUT"), "yyyy-MM-dd"))


"""df_validado = df.withColumn("fecha_valida",
    when(to_date(col("FEC_NACIMIENTO"), "yyyy-MM-dd").isNotNull(), to_date(col("FEC_NACIMIENTO"), "yyyy-MM-dd"))
    .when(to_timestamp(col("FEC_NACIMIENTO"), "yyyy-MM-dd HH:mm:ss").isNotNull(), to_timestamp(col("FEC_NACIMIENTO"), "yyyy-MM-dd HH:mm:ss"))
    .otherwise(None)
)
"""
#df.select("FECHANACIMIENTOINPUT").show(2,0)
#df_validado.select("fecha_valida").show(2, 0)
df_con_fecha.select("fecha_valida").show(2, 0)
count_no_valid = df_con_fecha.filter(col("fecha_valida").isNull()).count()
no_valid = df_con_fecha.select("FECHANACIMIENTOINPUT").filter(col("fecha_valida").isNull()).show(15)
print("count_no_valid", count_no_valid)
print("No registros", df.count())


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
                    GlueAthenaManager - Estándar Reutilizable
================================================================================

Description:
    Clase estándar para todas las operaciones Glue/Athena en el equipo.
    Una sola implementación que todos los desarrolladores reutilizan.

Author:         Juan Alejandro Carrillo Jaimes
Created:        2025-07-30
Version:        1.0.0
Environment:    AWS Glue

================================================================================
"""

import boto3
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from botocore.exceptions import ClientError
from logger_config import logger

class GlueAthenaManager:
    """
    Clase estándar para todas las operaciones Glue/Athena del proyecto.
    
    USO: Todos los desarrolladores usan esta clase para operaciones Bronze/Silver.
    NO crear clases similares - usar siempre esta.
    """
    
    def __init__(self, glue_context: GlueContext, spark_session: SparkSession):
        """
        Initialize Glue Athena Manager with standard configuration.
        
        Args:
            glue_context (GlueContext): AWS Glue context
            spark_session (SparkSession): Spark session
        """
        self.glue_context = glue_context
        self.spark = spark_session
        self.glue_client = boto3.client('glue')
        
        logger.info("GlueAthenaManager initialized - Standard version", "GlueAthenaManager")
    
    def read_table_from_catalog(self, catalog_name: str, database_name: str, table_name: str, 
                               transformation_ctx: str = None) -> DynamicFrame:
        """
        ESTÁNDAR: Leer tabla desde Glue Catalog.
        
        Args:
            catalog_name (str): Catalog name (ej: datalake-dev-bronte-catalog-db)
            database_name (str): Database name
            table_name (str): Table name
            transformation_ctx (str): Transformation context for Glue
            
        Returns:
            DynamicFrame: Data from table
        """
        try:
            if not transformation_ctx:
                transformation_ctx = f"read_{database_name}_{table_name}"
            
            logger.info(f"Reading table: {catalog_name}.{database_name}.{table_name}", "GlueAthenaManager")
            
            dynamic_frame = self.glue_context.create_dynamic_frame.from_catalog(
                database=database_name,
                table_name=table_name,
                catalog_id=catalog_name,
                transformation_ctx=transformation_ctx
            )
            
            record_count = dynamic_frame.count()
            logger.success(f"Table read successfully: {record_count} records", "GlueAthenaManager")
            
            return dynamic_frame
            
        except Exception as e:
            logger.error(f"Error reading table {table_name}: {str(e)}", "GlueAthenaManager")
            raise
    
    def read_table_with_predicate(self, catalog_name: str, database_name: str, table_name: str,
                                 push_down_predicate: str, transformation_ctx: str = None) -> DynamicFrame:
        """
        ESTÁNDAR: Leer tabla con filtros (predicate pushdown).
        
        Args:
            catalog_name (str): Catalog name
            database_name (str): Database name  
            table_name (str): Table name
            push_down_predicate (str): SQL WHERE clause (ej: "year='2025' AND month='07'")
            transformation_ctx (str): Transformation context
            
        Returns:
            DynamicFrame: Filtered data
        """
        try:
            if not transformation_ctx:
                transformation_ctx = f"read_filtered_{database_name}_{table_name}"
            
            logger.info(f"Reading table with filter: {table_name} WHERE {push_down_predicate}", "GlueAthenaManager")
            
            dynamic_frame = self.glue_context.create_dynamic_frame.from_catalog(
                database=database_name,
                table_name=table_name,
                catalog_id=catalog_name,
                push_down_predicate=push_down_predicate,
                transformation_ctx=transformation_ctx
            )
            
            record_count = dynamic_frame.count()
            logger.success(f"Filtered table read: {record_count} records", "GlueAthenaManager")
            
            return dynamic_frame
            
        except Exception as e:
            logger.error(f"Error reading filtered table: {str(e)}", "GlueAthenaManager")
            raise    
    def write_to_s3_location(self, dynamic_frame: DynamicFrame, s3_path: str, 
                            format_type: str = "glueparquet", partition_keys: list = None,
                            mode: str = "overwrite", transformation_ctx: str = None) -> None:
        """
        ESTÁNDAR: Escribir DynamicFrame a S3.
        
        Args:
            dynamic_frame (DynamicFrame): Data to write
            s3_path (str): S3 destination path
            format_type (str): Format (glueparquet, json, csv)
            partition_keys (list): Partition columns
            mode (str): Write mode (overwrite, append)
            transformation_ctx (str): Transformation context
        """
        try:
            if not transformation_ctx:
                transformation_ctx = f"write_s3_{int(datetime.now().timestamp())}"
            
            if partition_keys is None:
                partition_keys = []
            
            logger.info(f"Writing to S3: {s3_path} (mode: {mode})", "GlueAthenaManager")
            
            connection_options = {
                "path": s3_path,
                "partitionKeys": partition_keys
            }
            
            format_options = {
                "compression": "snappy"
            }
            
            if mode == "append":
                connection_options["enableUpdateCatalog"] = False
                connection_options["writeMode"] = "append"
                format_options["writeMode"] = "append"
            
            self.glue_context.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="s3",
                format=format_type,
                connection_options=connection_options,
                format_options=format_options,
                transformation_ctx=transformation_ctx
            )
            
            logger.success(f"Data written successfully to {s3_path}", "GlueAthenaManager")
            
        except Exception as e:
            logger.error(f"Error writing to S3: {str(e)}", "GlueAthenaManager")
            raise
    
    def execute_athena_query(self, query: str, database: str, s3_output_location: str) -> str:
        """
        ESTÁNDAR: Ejecutar query en Athena.
        
        Args:
            query (str): SQL query to execute
            database (str): Database context
            s3_output_location (str): S3 path for query results
            
        Returns:
            str: Query execution ID
        """
        try:
            athena_client = boto3.client('athena')
            
            logger.info(f"Executing Athena query in database: {database}", "GlueAthenaManager")
            
            response = athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': database},
                ResultConfiguration={'OutputLocation': s3_output_location}
            )
            
            execution_id = response['QueryExecutionId']
            logger.success(f"Query executed successfully: {execution_id}", "GlueAthenaManager")
            
            return execution_id
            
        except Exception as e:
            logger.error(f"Error executing Athena query: {str(e)}", "GlueAthenaManager")
            raise
    
    def table_exists_in_catalog(self, catalog_name: str, database: str, table_name: str) -> bool:
        """
        ESTÁNDAR: Verificar si tabla existe en Glue Catalog.
        
        Args:
            catalog_name (str): Catalog name
            database (str): Database name
            table_name (str): Table name
            
        Returns:
            bool: True if table exists
        """
        try:
            self.glue_client.get_table(
                CatalogId=catalog_name,
                DatabaseName=database,
                Name=table_name
            )
            
            logger.info(f"Table exists: {catalog_name}.{database}.{table_name}", "GlueAthenaManager")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                logger.info(f"Table not found: {table_name}", "GlueAthenaManager")
                return False
            else:
                logger.error(f"Error checking table existence: {str(e)}", "GlueAthenaManager")
                raise    
    def create_table_in_catalog(self, catalog_name: str, database: str, table_name: str,
                               columns: list, partition_keys: list, s3_location: str,
                               table_properties: dict = None) -> bool:
        """
        ESTÁNDAR: Crear tabla en Glue Catalog.
        
        Args:
            catalog_name (str): Catalog name
            database (str): Database name
            table_name (str): Table name
            columns (list): Column definitions [{'Name': 'col', 'Type': 'string'}]
            partition_keys (list): Partition definitions
            s3_location (str): S3 location for data
            table_properties (dict): Additional properties
            
        Returns:
            bool: True if created successfully
        """
        try:
            if self.table_exists_in_catalog(catalog_name, database, table_name):
                logger.info(f"Table {table_name} already exists", "GlueAthenaManager")
                return True
            
            default_properties = {
                'classification': 'parquet',
                'compressionType': 'snappy',
                'created_by': 'silver-standard-manager'
            }
            
            if table_properties:
                default_properties.update(table_properties)
            
            table_input = {
                'Name': table_name,
                'StorageDescriptor': {
                    'Columns': columns,
                    'Location': s3_location,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                },
                'PartitionKeys': partition_keys,
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': default_properties
            }
            
            self.glue_client.create_table(
                CatalogId=catalog_name,
                DatabaseName=database,
                TableInput=table_input
            )
            
            logger.success(f"Table created: {catalog_name}.{database}.{table_name}", "GlueAthenaManager")
            return True
            
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}", "GlueAthenaManager")
            return False
    
    def get_table_partitions(self, catalog_name: str, database: str, table_name: str) -> list:
        """
        ESTÁNDAR: Obtener particiones de una tabla.
        
        Args:
            catalog_name (str): Catalog name
            database (str): Database name
            table_name (str): Table name
            
        Returns:
            list: List of partition information
        """
        try:
            response = self.glue_client.get_partitions(
                CatalogId=catalog_name,
                DatabaseName=database,
                TableName=table_name
            )
            
            partitions = response.get('Partitions', [])
            logger.info(f"Found {len(partitions)} partitions for {table_name}", "GlueAthenaManager")
            
            return partitions
            
        except Exception as e:
            logger.error(f"Error getting partitions: {str(e)}", "GlueAthenaManager")
            return []
dyf = glueContext.create_dynamic_frame.from_catalog(database=DATABASE_NAME, table_name=TABLE_NAME)
dyf.printSchema() 
df = dyf.toDF()
df.show()
s3output = glueContext.getSink(
  path="s3://bucket_name/folder_name",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase="demo", catalogTableName="populations"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(DyF)
job.commit()