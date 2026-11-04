import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from pyspark.sql import SparkSession
from urllib.parse import urlparse
from pyspark.sql import functions as F
import boto3, traceback, io

import pandas as pd

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

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

def select_fields(df, fields):
    return df.select(*fields)
    
def hacer_join_con_columnas_diferentes(df_izq, df_der, condiciones_join, columnas_adicionales, tipo_join="inner"):
   """
   Realiza un join entre dos DataFrames de Spark cuando las columnas de cruce tienen nombres distintos.
   :param df_izq: DataFrame izquierdo.
   :param df_der: DataFrame derecho.
   :param condiciones_join: Lista de tuplas con los nombres de las columnas (ej: [("id_izq", "id_der"), ("fecha_izq", "fecha_der")]).
   :param columnas_adicionales: Lista con los nombres de las columnas en el df_der que quieres traer.
   :param tipo_join: Tipo de join ("inner", "left", "right", "outer").
   :return: DataFrame resultante.
   """
   # Extraemos solo las columnas necesarias del df_der (las llaves derechas y las adicionales)
   llaves_der = [der for izq, der in condiciones_join]
   columnas_a_traer = list(set(llaves_der + columnas_adicionales))
   df_der_reducido = df_der.select(*columnas_a_traer)
   # Construimos la condición de join combinando cada par con el operador &
   condicion = None
   for izq, der in condiciones_join:
       match_cond = (df_izq[izq] == df_der_reducido[der])
       if condicion is None:
           condicion = match_cond
       else:
           condicion = condicion & match_cond
   # Realizamos el join
   df_resultado = df_izq.join(df_der_reducido, on=condicion, how=tipo_join)
   return df_resultado

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
       
def guardar_dataframe_csv_s3(df, bucket, path_destino):
   bucket = bucket.strip("/")
   path_destino = path_destino.strip("/")
   ruta_completa_s3 = f"s3://{bucket}/{path_destino}/"
   # Opcional: si tienes muchos archivos pequeños, puedes compactarlos con .coalesce() o .repartition()
   # Si te sigue dando error de memoria, prueba agregando .coalesce(4) antes del write
   df.coalesce(1).write \
       .mode("overwrite") \
       .option("header", "true") \
       .option("sep", ",") \
       .option("escape", "\"") \
       .option("ignoreLeadingWhiteSpace", "false") \
       .option("ignoreTrailingWhiteSpace", "false") \
       .csv(ruta_completa_s3)
   print(f"✔️ ÉXITO: DataFrame guardado correctamente en formato CSV en: {ruta_completa_s3}")
# Ejemplo de uso:
# guardar_dataframe_csv_s3(df_resultado_final, "alfa-condor-dev-bucket", "file_out/reporte_cruce_millones")

def convertir_decimal(columna):
    return F.when(
        (F.col(columna).isNull()) |
        (F.trim(F.col(columna)) == ""),
        None
    ).otherwise(
        F.col(columna)
         .cast("double")
         .cast("decimal(38,2)")
    )
"""
#ruta_acsele_1 = "s3://alfa-condor-dev-bucket/file_out/ACSELE_1/"
ruta_datalake_1 = "s3://alfa-condor-dev-bucket/file_out/DATALAKE_1/"
#df_acsele_1 = cargar_tabla_particionada_s3(spark, nombre_tabla="fact_acsele_1", ruta_base_s3=ruta_acsele_1)
df_datalake_1 = cargar_tabla_particionada_s3(spark, nombre_tabla="fact_datalake_1", ruta_base_s3=ruta_datalake_1)

ruta_acsele_2 = "s3://alfa-condor-dev-bucket/file_out/ACSELE_2/"
#ruta_datalake_2 = "s3://alfa-condor-dev-bucket/file_out/DATALAKE_2/"
df_acsele_2 = cargar_tabla_particionada_s3(spark, nombre_tabla="fact_acsele_2", ruta_base_s3=ruta_acsele_2)
#df_datalake_2 = cargar_tabla_particionada_s3(spark, nombre_tabla="fact_datalake_2", ruta_base_s3=ruta_datalake_2)
"""
try:
    ruta_datalake_1 = "s3://alfa-condor-dev-bucket/file_out/DATALAKE_1/"
    df_datalake_1 = cargar_tabla_particionada_s3(spark, nombre_tabla="fact_datalake_1", ruta_base_s3=ruta_datalake_1)
    ruta_acsele_1 = "s3://alfa-condor-dev-bucket/file_out/ACSELE_2/"
    df_acsele_1 = cargar_tabla_particionada_s3(spark, nombre_tabla="fact_acsele_1", ruta_base_s3=ruta_acsele_1)
    
    
    ruta_acsele_2 = "s3://alfa-condor-dev-bucket/file_out/ACSELE_3/"
    ruta_datalake_2 = "s3://alfa-condor-dev-bucket/file_out/DATALAKE_3/"
    df_acsele_2 = cargar_tabla_particionada_s3(spark, nombre_tabla="fact_acsele_2", ruta_base_s3=ruta_acsele_2)
    df_datalake_2 = cargar_tabla_particionada_s3(spark, nombre_tabla="fact_datalake_2", ruta_base_s3=ruta_datalake_2)
    
    ruta_acsele_3 = "s3://alfa-condor-dev-bucket/file_out/ACSELE_4/"
    ruta_datalake_3 = "s3://alfa-condor-dev-bucket/file_out/DATALAKE_4/"
    df_acsele_3 = cargar_tabla_particionada_s3(spark, nombre_tabla="fact_acsele_3", ruta_base_s3=ruta_acsele_3)
    df_datalake_3 = cargar_tabla_particionada_s3(spark, nombre_tabla="fact_datalake_3", ruta_base_s3=ruta_datalake_3)
    
    ruta_acsele_4 = "s3://alfa-condor-dev-bucket/file_out/ACSELE_5/"
    ruta_datalake_4 = "s3://alfa-condor-dev-bucket/file_out/DATALAKE_5/"
    df_acsele_4 = cargar_tabla_particionada_s3(spark, nombre_tabla="fact_acsele_4", ruta_base_s3=ruta_acsele_4)
    df_datalake_4 = cargar_tabla_particionada_s3(spark, nombre_tabla="fact_datalake_4", ruta_base_s3=ruta_datalake_4)
    
    ruta_acsele_5 = "s3://alfa-condor-dev-bucket/file_out/ACSELE_6/"
    df_acsele_5 = cargar_tabla_particionada_s3(spark, nombre_tabla="fact_acsele_5", ruta_base_s3=ruta_acsele_5)
    
    ruta_acsele_6 = "s3://alfa-condor-dev-bucket/file_out/ACSELE_7/"
    df_acsele_6 = cargar_tabla_particionada_s3(spark, nombre_tabla="fact_acsele_6", ruta_base_s3=ruta_acsele_6)
    """
    print(f"✔️ ÉXITO: Spark cargó 'fact_acsele_1' en memoria con {df_acsele_1.count()} registros.")
    print(f"✔️ ÉXITO: Spark cargó 'fact_datalake_1' en memoria con {df_datalake_1.count()} registros.")
    print(f"✔️ ÉXITO: Spark cargó 'fact_acsele_2' en memoria con {df_acsele_2.count()} registros.")
    print(f"✔️ ÉXITO: Spark cargó 'fact_datalake_2' en memoria con {df_datalake_2.count()} registros.")
    print(f"✔️ ÉXITO: Spark cargó 'fact_acsele_3' en memoria con {df_acsele_3.count()} registros.")
    print(f"✔️ ÉXITO: Spark cargó 'fact_datalake_3' en memoria con {df_datalake_3.count()} registros.")
    print(f"✔️ ÉXITO: Spark cargó 'fact_acsele_4' en memoria con {df_acsele_4.count()} registros.")
    print(f"✔️ ÉXITO: Spark cargó 'fact_datalake_4' en memoria con {df_datalake_4.count()} registros.")
    """
    
    lista_dfs_acsele = [df_acsele_1, df_acsele_2, df_acsele_3, df_acsele_4,df_acsele_5, df_acsele_6]
    lista_df_datalake = [df_datalake_1, df_datalake_2, df_datalake_3, df_datalake_4]
    # Define aquí el arreglo de campos que necesitas seleccionar
    fields = ['NRO_POLIZA','NUMERO_CREDITO','CODIGO_SUCURSAL','SUCURSAL','FECHA_NACER_ASEGURADO']
    # Aplica la función a cada DataFrame y luego únelos todos,'X' en uno solo (unionByName es útil si los esquemas son compatibles o tienen columnas similares)
    df_acsele_unido = None
    for df in lista_dfs_acsele:
       df_seleccionado = select_fields(df, fields)
       if df_acsele_unido is None:
           df_acsele_unido = df_seleccionado
       else:
           df_acsele_unido = df_acsele_unido.unionByName(df_seleccionado)
    # Puedes verificar el resultado con un conteo
    
    condiciones = [
       ("numero_poliza", "NRO_POLIZA")
    ]
    # 2. Las columnas adicionales que quieres traer del datalake
    columnas_extras = ["NUMERO_CREDITO","CODIGO_SUCURSAL","SUCURSAL","FECHA_NACER_ASEGURADO"]
    # 3. Aplicamos la función
    i = 1
    for df_dl in lista_df_datalake:
        df_resultado_join = hacer_join_con_columnas_diferentes(
           df_izq=df_dl,
           df_der=df_acsele_unido,
           condiciones_join=condiciones,
           columnas_adicionales=columnas_extras,
           tipo_join="left"
        )
        df_resultado_join = df_resultado_join.drop("NRO_POLIZA")
        df_resultado_join = df_resultado_join.withColumn("VALOR_CREDITO_ASEGURADO", F.lit(""))
        df_resultado_join = df_resultado_join.withColumn("valor_asegurado_poliza",convertir_decimal("valor_asegurado_poliza"))
        df_resultado_join = df_resultado_join.withColumn("valor_prima_poliza",convertir_decimal("valor_prima_poliza"))    
        df_filtered = df_resultado_join.filter(~F.upper(F.col("desc_ciudad")).contains("BOGOT"))
        df_limpio = df_resultado_join.filter(F.col("NUMERO_CREDITO").isNotNull() &(F.trim(F.col("NUMERO_CREDITO")) != ""))
        sin_dup = df_resultado_join.dropDuplicates()
        print(f"✔️ la cantidad de registros totales es {df_resultado_join.count()}")
        print(f"✔️ Total eliminados {df_resultado_join.count() - sin_dup.count()}")
        print(f"✔️ la cantidad de registros Nulos es {df_limpio.count()}")
        print(f"✔️ la cantidad de schema es {df_limpio.printSchema()}")
        print(f"✔️ la cantidad de registros Bgt es {df_filtered.count()}")
        ruta_salida_excel = f"file_out_file/report_cruce_parte_{i}/"
        ruta_salida_excel_bgt = f"file_out_file/report_cruce_parte_{i}_bgt/"
        bucket = 'alfa-condor-dev-bucket'
        guardar_dataframe_csv_s3(df_resultado_join, bucket, ruta_salida_excel)
        guardar_dataframe_csv_s3(df_filtered, bucket, ruta_salida_excel_bgt)
        i = i + 1
        
    job.commit()
except Exception as e:
    print(traceback.format_exc())
    raise e