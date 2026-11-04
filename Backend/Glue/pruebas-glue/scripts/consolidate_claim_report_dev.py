import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql import SparkSession
from urllib.parse import urlparse

import io
import boto3
import pandas as pd


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def guardar_resultado_parquet(df, ruta_destino_s3):
   """
   Guarda un DataFrame de Spark en formato Parquet en la ruta especificada.
   """
   # Aseguramos el uso del protocolo nativo de Glue
   ruta_limpia = ruta_destino_s3.replace('s3a://', 's3://')
   print(f"Iniciando escritura de datos en: {ruta_limpia}")
   try:
       df.coalesce(1) \
         .write \
         .mode("overwrite") \
         .format("parquet") \
         .save(ruta_limpia)
       print(f"✅ Resultado guardado exitosamente en: {ruta_limpia}")
   except Exception as e:
       print(f"❌ Error al guardar el archivo Parquet: {str(e)}")


def cargar_tabla_particionada_s3(spark: SparkSession, nombre_tabla: str, ruta_base_s3: str, **opciones):
    print(f"\n=======================================================")
    print(f"[DEBUG] 1. Iniciando carga de tabla: '{nombre_tabla}'")
    print(f"[DEBUG] 2. Ruta evaluada: {ruta_base_s3}")
    # --- BLOQUE DE DIAGNÓSTICO BOTO3 ---
    # Normalizamos a s3:// por si estás usando s3a://
    ruta_limpia = ruta_base_s3.replace('s3a://', 's3://')
    try:
       parsed = urlparse(ruta_limpia)
       bucket = parsed.netloc
       prefijo = parsed.path.lstrip('/')
       s3_client = boto3.client('s3')
       resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefijo)
       if 'Contents' in resp:
           archivos_parquet = [obj['Key'] for obj in resp['Contents'] if obj['Key'].endswith('.parquet')]
           archivos_totales = [obj['Key'] for obj in resp['Contents']]
           print(f"[DEBUG] 3. S3 respondió OK. Archivos totales encontrados: {len(archivos_totales)}")
           print(f"[DEBUG] 4. Archivos con extensión .parquet: {len(archivos_parquet)}")
           if archivos_parquet:
               print(f"[DEBUG] 5. Muestra del primer Parquet: s3://{bucket}/{archivos_parquet[0]}")
           else:
               print(f"[ALERTA] La ruta existe, pero NO HAY archivos .parquet adentro. Esto causa el UNABLE_TO_INFER_SCHEMA.")
       else:
           print(f"[ALERTA] Boto3 accedió al bucket, pero la carpeta '{prefijo}' está COMPLETAMENTE VACÍA o no existe.")
    except Exception as e:
       print(f"[ERROR CRÍTICO] Boto3 no pudo acceder a S3. Revisa VPC Endpoints o permisos: {str(e)}")
    # -----------------------------------
    print(f"[DEBUG] 6. Entregando ruta a PySpark para intentar lectura...")
    
    # 1. Aseguramos que Spark use s3:// (el nativo de Glue) y termine en /
    
    ruta_spark = ruta_base_s3.replace('s3a://', 's3://')
    
    if not ruta_spark.endswith('/'):
    
        ruta_spark += '/'
    
    try:
    
        # 2. Reemplazamos basePath por recursiveFileLookup
        
        df = spark.read.format("parquet") \
                  .option("recursiveFileLookup", "true") \
                  .option("mergeSchema", "true") \
                  .options(**opciones) \
                  .load(ruta_spark) # Usamos la ruta limpia
        
        df.cache()
        
        conteo = df.count() 
        
        df.createOrReplaceTempView(nombre_tabla)
        
        print(f"✅ ÉXITO: Spark cargó '{nombre_tabla}' en memoria con {conteo} registros.")
        
        print(f"=======================================================\n")
        
        return df   

    except Exception as e:

        print(f"❌ FALLO SPARK: No se pudo cargar '{nombre_tabla}'. Razón: {str(e)}")

        print(f"=======================================================\n")

        return None

def guardar_excel_s3(df_spark, nombre_bucket, ruta_key_s3):
   """Pasa el DataFrame a Pandas y lo inyecta como Excel (.xlsx) directo en S3."""
   print(f"\nIniciando generación de Excel en: s3://{nombre_bucket}/{ruta_key_s3}")
   try:
       df_pandas = df_spark.toPandas()
       buffer = io.BytesIO()
       with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
           df_pandas.to_excel(writer, index=False, sheet_name='Cruce_Sismo')
       s3_client = boto3.client('s3')
       s3_client.put_object(
           Bucket=nombre_bucket,
           Key=ruta_key_s3,
           Body=buffer.getvalue()
       )
       print("✅ Reporte Excel guardado exitosamente en S3.")
   except Exception as e:
       print(f"❌ Error al generar Excel: {str(e)}")


# ==========================================
# Ejemplo de carga apuntando a la raíz:
# ==========================================
# Tu ruta termina exactamente en el nombre de la tabla, sin incluir la partición
ruta_cali_raiz = "s3://alfa-condor-dev-bucket/file_out/DATA_CALI/"
ruta_manizales_raiz = "s3://alfa-condor-dev-bucket/file_out/DATA_MAN/"
ruta_pereira_raiz = "s3://alfa-condor-dev-bucket/file_out/DATA_PER/"
ruta_sismo_raiz = "s3://alfa-condor-dev-bucket/file_out/SISMO_CRUCE/"

# Al ejecutarla, Spark escaneará todo lo que esté dentro de /data_cali/
df_cali = cargar_tabla_particionada_s3(spark, nombre_tabla="data_cali", ruta_base_s3=ruta_cali_raiz)
df_man = cargar_tabla_particionada_s3(spark, nombre_tabla="data_man", ruta_base_s3=ruta_manizales_raiz)
df_per = cargar_tabla_particionada_s3(spark, nombre_tabla="data_per", ruta_base_s3=ruta_pereira_raiz)
df_sismo = cargar_tabla_particionada_s3(spark, nombre_tabla="sismo_cruce", ruta_base_s3=ruta_sismo_raiz)


# 1. Consolidar los DataFrames de ciudades agregando el origen
df_ciudades = (
   df_cali.select(
       "direccion",
       F.col("CONCEPTO").alias("clasificacion_de_habitabilidad"),
       F.lit("Cali").alias("ciudad_origen")
   )
   .unionAll(
       df_per.select(
           "direccion",
           F.col("clasificacion_de_habitabilidad"),
           F.lit("Pereira").alias("ciudad_origen")
       )
   )
   .unionAll(
       df_man.select(
           "direccion",
           F.col("clasificacion_habitabilidad__survey").alias("clasificacion_de_habitabilidad"),
           F.lit("Manizales").alias("ciudad_origen")
       )
   )
)
# 2. Función de estandarización para aplicar a cualquier columna

def normalizar_direccion(columna):
   c = F.lower(columna)
   c = F.regexp_replace(c, r'[.,#\-]', ' ')
   c = F.regexp_replace(c, r'\b(calle|cl)\b', 'cll')
   c = F.regexp_replace(c, r'\b(carrera|kr|cr)\b', 'cra')
   c = F.regexp_replace(c, r'\b(no|numero|num)\b', '')
   c = F.regexp_replace(c, r'\s+', ' ')
   return F.trim(c)
df_ciudades_limpio = df_ciudades.withColumnRenamed("direccion", "dir_ciudad") \
   .withColumn("ciudad_norm", normalizar_direccion(F.col("dir_ciudad")))
# --- OPTIMIZACIÓN CLAVE 1: Materializar las ciudades ---
# El count() es una acción que obliga a Spark a ejecutar la limpieza AHORA y guardar en RAM,
# rompiendo el linaje para que no recalcule la regex en el cruce.
df_ciudades_limpio.cache()
print(f"Total ciudades listas en RAM: {df_ciudades_limpio.count()}")
# 2. Normalización de direcciones únicas de Sismo
df_sismo_unicas = df_sismo.select("direccion_predio").distinct()
df_sismo_unicas = df_sismo_unicas.withColumn("sismo_norm", normalizar_direccion(F.col("direccion_predio")))
# --- OPTIMIZACIÓN CLAVE 2: Materializar el Sismo ---
df_sismo_unicas.cache()
print(f"Total direcciones únicas sismo en RAM: {df_sismo_unicas.count()}")
# 3. Cruce Cartesiano Seguro en Memoria
# Como ambos DataFrames están limpios y cacheados, el crossJoin es pura matemática rápida.
df_cruce = df_sismo_unicas.crossJoin(F.broadcast(df_ciudades_limpio))
# 4. Cálculo de Similitud
distancia = F.levenshtein(F.col("sismo_norm"), F.col("ciudad_norm"))
long_max = F.greatest(F.length(F.col("sismo_norm")), F.length(F.col("ciudad_norm")), F.lit(1))
df_cruce = df_cruce.withColumn("similitud", (F.lit(1.0) - (distancia.cast("double") / long_max)) * 100)
# --- OPTIMIZACIÓN CLAVE 3: Reducir el Shuffle del Window ---
# Pre-particionamos los datos por la dirección para que el Window se resuelva localmente en cada nodo
# sin intercambiar terabytes de datos por la red del clúster.
df_cruce_particionado = df_cruce.repartition("direccion_predio")
window_spec = Window.partitionBy("direccion_predio").orderBy(F.col("similitud").desc())
df_mejores_matches = df_cruce_particionado.withColumn("rn", F.row_number().over(window_spec)) \
                                         .filter(F.col("rn") == 1) \
                                         .drop("rn")
# 5. Join Final sin pérdida de datos
df_final = df_sismo.join(
   F.broadcast(df_mejores_matches),
   on="direccion_predio",
   how="left"
)
# 6. Reglas de Salida
umbral = 60.0
condicion = F.col("similitud") >= umbral
print(df_final.printSchema())
df_final = df_final.select(
   df_sismo["*"],
   F.when(condicion, F.lit("Si"))\
    .otherwise(F.lit("No")).alias("indicador_cruce"),
   F.when(condicion, F.coalesce(F.col("ciudad_origen"), F.lit("No aplica")))\
    .otherwise(F.lit("No aplica")).alias("origen_datos"),
   F.when(condicion, F.coalesce(F.col("dir_ciudad"), F.lit("No aplica")))\
    .otherwise(F.lit("No aplica")).alias("direccion_cruce_original"),
   F.when(condicion, F.coalesce(F.col("sismo_norm"), F.lit("No aplica")))\
    .otherwise(F.lit("No aplica")).alias("sismo_texto_comparado"),
   F.when(condicion, F.coalesce(F.col("ciudad_norm"), F.lit("No aplica")))\
    .otherwise(F.lit("No aplica")).alias("ciudad_texto_comparado"),
   F.when(condicion, F.coalesce(F.col("clasificacion_de_habitabilidad"), F.lit("No aplica")))\
    .otherwise(F.lit("No aplica")).alias("clasificacion_habitabilidad"),
   # Cast a string obligatorio para poder insertar texto en un campo numérico
   F.when(condicion, F.round(F.col("similitud"), 2).cast("string"))\
    .otherwise(F.lit("No aplica")).alias("porcentaje_similitud")
)

columnas_creadas = [
   "origen_datos",
   "direccion_cruce_original",
   "sismo_texto_comparado",
   "ciudad_texto_comparado",
   "clasificacion",
   "porcentaje_similitud"
]
# Le decimos a Spark: "Revisa una por una estas columnas. Si por x o y razón su valor
# es null, ponle 'No aplica'. Si sí tiene dato, déjale el dato que ya tenía."
for col_name in columnas_creadas:
   df_final = df_final.withColumn(
       col_name,
       F.when(F.col(col_name).isNull(), F.lit("No aplica")).otherwise(F.col(col_name))
   )


print("==============================")

# Ejecutar o guardar resultados
# df_final.show(truncate=False)
# ==========================================
# Ejemplo de uso en tu flujo principal:
# ==========================================
# Ruta donde quieres que quede el resultado (asegúrate de que termine en /)
date = datetime.now().strftime("%Y-%m-%d")
ruta_salida = f"s3://alfa-condor-dev-bucket/file_out/SISMO_FINAL/{date}"
#print(df_final.select("porcentaje_similitud", "clasificacion").orderBy(F.col("porcentaje_similitud").desc()).show(20, truncate = False))
# Llamas a la función pasándole tu dataframe final
guardar_resultado_parquet(df_final, ruta_salida)
ruta_salida_excel = f"file_out_file/reporte_cruce_sismo_{date}.xlsx"
bucket = 'alfa-condor-dev-bucket'
guardar_excel_s3(df_final, bucket, ruta_salida_excel)
print("-------------------- FINAL II ----------------------")

job.commit()