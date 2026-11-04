import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from pyspark.sql import SparkSession
from urllib.parse import urlparse

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
        
ruta_CLI_TB_1 = "s3://alfa-condor-dev-bucket/file_out/CLI_TB_1/"
df_CLI_TB_1 = cargar_tabla_particionada_s3(spark, nombre_tabla="CLI_TB_1", ruta_base_s3=ruta_CLI_TB_1)

ruta_CLI_TB_2 = "s3://alfa-condor-dev-bucket/file_out/CLI_TB_2/"
df_CLI_TB_2 = cargar_tabla_particionada_s3(spark, nombre_tabla="CLI_TB_2", ruta_base_s3=ruta_CLI_TB_2)

ruta_CLI_TB_3 = "s3://alfa-condor-dev-bucket/file_out/CLI_TB_3/"
df_CLI_TB_3 = cargar_tabla_particionada_s3(spark, nombre_tabla="CLI_TB_3", ruta_base_s3=ruta_CLI_TB_3)

ruta_CLI_TB_4 = "s3://alfa-condor-dev-bucket/file_out/CLI_TB_4/"
df_CLI_TB_4 = cargar_tabla_particionada_s3(spark, nombre_tabla="CLI_TB_4", ruta_base_s3=ruta_CLI_TB_4)

ruta_CLI_TB_5 = "s3://alfa-condor-dev-bucket/file_out/CLI_TB_5/"
df_CLI_TB_5 = cargar_tabla_particionada_s3(spark, nombre_tabla="CLI_TB_5", ruta_base_s3=ruta_CLI_TB_5)

ruta_CLI_TB_6 = "s3://alfa-condor-dev-bucket/file_out/CLI_TB_6/"
df_CLI_TB_6 = cargar_tabla_particionada_s3(spark, nombre_tabla="CLI_TB_6", ruta_base_s3=ruta_CLI_TB_6)

ruta_CLI_TB_7 = "s3://alfa-condor-dev-bucket/file_out/CLI_TB_7/"
df_CLI_TB_7 = cargar_tabla_particionada_s3(spark, nombre_tabla="CLI_TB_7", ruta_base_s3=ruta_CLI_TB_7)

ruta_CLI_TB_8 = "s3://alfa-condor-dev-bucket/file_out/CLI_TB_8/"
df_CLI_TB_8 = cargar_tabla_particionada_s3(spark, nombre_tabla="CLI_TB_8", ruta_base_s3=ruta_CLI_TB_8)

ruta_CLI_TB_9 = "s3://alfa-condor-dev-bucket/file_out/CLI_TB_9/"
df_CLI_TB_9 = cargar_tabla_particionada_s3(spark, nombre_tabla="CLI_TB_9", ruta_base_s3=ruta_CLI_TB_9)

ruta_CLI_TB_10 = "s3://alfa-condor-dev-bucket/file_out/CLI_TB_10/"
df_CLI_TB_10 = cargar_tabla_particionada_s3(spark, nombre_tabla="CLI_TB_10", ruta_base_s3=ruta_CLI_TB_10)

ruta_CLI_TB_11 = "s3://alfa-condor-dev-bucket/file_out/CLI_TB_11/"
df_CLI_TB_11 = cargar_tabla_particionada_s3(spark, nombre_tabla="CLI_TB_11", ruta_base_s3=ruta_CLI_TB_11)

ruta_CLI_TB_12 = "s3://alfa-condor-dev-bucket/file_out/CLI_TB_12/"
df_CLI_TB_12 = cargar_tabla_particionada_s3(spark, nombre_tabla="CLI_TB_12", ruta_base_s3=ruta_CLI_TB_12)

ruta_CLI_TB_13 = "s3://alfa-condor-dev-bucket/file_out/CLI_TB_13/"
df_CLI_TB_13 = cargar_tabla_particionada_s3(spark, nombre_tabla="CLI_TB_13", ruta_base_s3=ruta_CLI_TB_13)

ruta_CLI_TB_14 = "s3://alfa-condor-dev-bucket/file_out/CLI_TB_14/"
df_CLI_TB_14 = cargar_tabla_particionada_s3(spark, nombre_tabla="CLI_TB_14", ruta_base_s3=ruta_CLI_TB_14)

# Iteramos del 1 al 14

for i in range(1, 15):

    nombre_df = f"df_CLI_TB_{i}"
    # 1. Validar si la variable existe en la memoria y no es un valor nulo
    if nombre_df in globals() and globals()[nombre_df] is not None:
        df = globals()[nombre_df]
        # 2. Verificar si el DataFrame tiene datos
        if not df.isEmpty():
            print(f"\n✅ {nombre_df} CARGADO. Imprimiendo schema:")
            df.printSchema()
        else:
            print(f"\n⚠️ {nombre_df} existe, pero ESTÁ VACÍO (0 filas).")
    else:
        # 3. Si la variable nunca se creó en el script o falló su carga
        print(f"\n❌ {nombre_df} NO FUE CARGADO o no existe en el entorno.")
        
from pyspark.sql import functions as F
# 1. Función para estandarizar el Grupo 1 (DFs 1, 2, 3, 4, 8, 9, 10, 11, 12, 13, 14)
print("✅ ✅ ✅ Inicio del nuevo DF ✅ ✅ ✅ ")
            
def estandarizar_grupo_1(df):
   return df.select(
       F.col("tip").alias("tipo_documento"),
       F.col("identificacion").alias("numero_documento"),
       F.col("nombre").alias("nombre_completo"),
       F.col("fecha_nac").alias("fecha_nacimiento"),
       F.col("correo_electronico").alias("correo"),
       F.col("genero").alias("genero"),
       F.col("celular_comercial").alias("telefono"),
       F.col("direccion_residencia").alias("direccion")
   )
# 2. Función para estandarizar el Grupo 2 (DFs 5, 6, 7)
def estandarizar_grupo_2(df):
   return df.select(
       F.col("tipo_documento").alias("tipo_documento"),
       F.col("no__documento").alias("numero_documento"),
       F.col("nombres").alias("nombre_completo"),
       F.lit(None).cast("string").alias("fecha_nacimiento"), # Campo inexistente en este grupo
       F.col("correo").alias("correo"),
       F.lit(None).cast("string").alias("genero"), # Campo inexistente en este grupo
       F.col("telefono").alias("telefono"),
       F.col("direccion_correspondencia").alias("direccion")
   )
# 3. Iterar, estandarizar y unir los 14 DataFrames
dfs_unidos = None
grupo_1_ids = [1, 2, 3, 4, 8, 9, 10, 11, 12, 13, 14]
grupo_2_ids = [5, 6, 7]
for i in range(1, 15):
   nombre_df = f"df_CLI_TB_{i}"
   # Validar si el DF fue cargado exitosamente en memoria
   if nombre_df in globals() and globals()[nombre_df] is not None:
       df_actual = globals()[nombre_df]
       # Aplicar el renombramiento según el grupo
       if i in grupo_1_ids:
           df_std = estandarizar_grupo_1(df_actual)
       elif i in grupo_2_ids:
           df_std = estandarizar_grupo_2(df_actual)
       # Unir los datos (unionByName con allowMissingColumns previene errores de orden de columnas)
       if dfs_unidos is None:
           dfs_unidos = df_std
       else:
           dfs_unidos = dfs_unidos.unionByName(df_std, allowMissingColumns=True)
# 4. Agrupar por llaves maestras y eliminar duplicados rescatando la mejor información
df_clientes_maestro = dfs_unidos.groupBy("tipo_documento", "numero_documento").agg(
   F.first("nombre_completo", ignorenulls=True).alias("nombre_completo"),
   F.first("fecha_nacimiento", ignorenulls=True).alias("fecha_nacimiento"),
   F.first("correo", ignorenulls=True).alias("correo"),
   F.first("genero", ignorenulls=True).alias("genero"),
   F.first("telefono", ignorenulls=True).alias("telefono"),
   F.first("direccion", ignorenulls=True).alias("direccion")
)
# Imprimir el resultado para validación
df_clientes_maestro.printSchema()
 
ruta_salida = f"s3://alfa-condor-dev-bucket/file_out/CLIENTES_FINAL"
#print(df_final.select("porcentaje_similitud", "clasificacion").orderBy(F.col("porcentaje_similitud").desc()).show(20, truncate = False))
# Llamas a la función pasándole tu dataframe final
guardar_resultado_parquet(df_clientes_maestro, ruta_salida)
job.commit()