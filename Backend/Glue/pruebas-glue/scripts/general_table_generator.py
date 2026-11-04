import sys
from pyspark.conf import SparkConf
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import pandas as pd
import io

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# ==========================================
# 1. INICIALIZACIÓN CON SOPORTE PARA DELTA
# ==========================================
conf = SparkConf()
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Inyectamos la configuración al momento de crear el contexto
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

args = getResolvedOptions(
        sys.argv, 
            [
            'PREFIX_TEMPTABLE_FILE',
            'FILE_TEMP_TABLES_NAME',
            'BUCKET_SOURCE_TEMP_FILE',
            'BUCKET_SCRIPTS',
            'PATH_SCRIPTS'
        ]
    )
    
PREFIX_TEMPTABLE_FILE = args['PREFIX_TEMPTABLE_FILE']
FILE_TEMP_TABLES_NAME = args['FILE_TEMP_TABLES_NAME']
BUCKET_SOURCE_TEMP_FILE = args['BUCKET_SOURCE_TEMP_FILE']
BUCKET_SCRIPTS = args['BUCKET_SCRIPTS']
PATH_SCRIPTS = args['PATH_SCRIPTS']

def cargar_s3_particionado_a_vista(spark_session, s3_path, nombre_vista, tipo_lectura):
    try:
        print(f"Montando '{nombre_vista}' [{tipo_lectura}] desde: {s3_path}")
        
        if tipo_lectura == 'recursivo':
            # Busca en todas las subcarpetas, ideal para archivos anidados sin particiones
            df = spark_session.read \
                .option("recursiveFileLookup", "true") \
                .parquet(s3_path)
                
        elif tipo_lectura == 'hive':
            # El pathGlobFilter "*.parquet" es vital: ignora los _$folder$ y 
            # cualquier archivo que no sea estrictamente datos.
            df = spark_session.read \
                .option("basePath", s3_path) \
                .option("mergeSchema", "true") \
                .option("pathGlobFilter", "*.parquet") \
                .parquet(s3_path)

        elif tipo_lectura == 'delta':
            # Lector nativo para tablas con _delta_log/
            # Lee la versión exacta de la tabla y descarta la basura automáticamente
            df = spark_session.read \
                .format("delta") \
                .load(s3_path)
                
        else:
            # Lectura directa
            df = spark_session.read.parquet(s3_path)
            
        df.createOrReplaceTempView(nombre_vista)
        print(df.count())
        print(f" -> OK: '{nombre_vista}' lista.")
        
    except Exception as e:
        print(f" -> ERROR crítico al cargar '{nombre_vista}': {str(e)}")
        raise e
        
def leer_sql_desde_s3(bucket, key):
    """Descarga el archivo .sql de S3 y lo devuelve como un string"""
    print(f"Leyendo archivo SQL desde s3://{bucket}/{key}")
    s3_client = boto3.client('s3')
    respuesta = s3_client.get_object(Bucket=bucket, Key=key)
    # Leemos el contenido y lo decodificamos a texto
    sql_string = respuesta['Body'].read().decode('utf-8')
    return sql_string
# ==========================================
# Ejemplo de uso dentro de tu Job de Glue
# ==========================================
def leer_mapeo_xlsx(bucket, key):
    try:
        print(f"Leyendo mapeo desde s3://{bucket}/{key}")
        s3_client = boto3.client('s3')
        respuesta = s3_client.get_object(Bucket=bucket, Key=key)
        
        # Leemos el Excel
        df_excel = pd.read_excel(io.BytesIO(respuesta['Body'].read()))
        
        # LA SOLUCIÓN: Reemplazamos el dict(zip()) por un ciclo
        mapeo_dict = {}
        
        for index, row in df_excel.iterrows():
            # La llave es la vista (ej: vw_clientes)
            nombre_vista = row['nombre_tabla']
            
            # El valor es un diccionario con las dos propiedades que necesitamos
            mapeo_dict[nombre_vista] = {
                'ruta': row['view_table'],
                'tipo': str(row['tipo_lectura']).strip().lower()
            }
            
        print(f"-> Éxito: Se procesaron {len(mapeo_dict)} tablas desde el Excel.")
        return mapeo_dict
        
    except Exception as e:
        print(f"Error crítico al leer el archivo Excel: {str(e)}")
        raise e

import boto3

def ejecutar_sqls_desde_s3(spark_session, bucket, ruta_carpeta_sqls):
    """
    Busca, ordena y ejecuta secuencialmente archivos .sql de una carpeta en S3.
    """
    try:
        # Aseguramos que el prefijo termine en '/' para que sea una búsqueda exacta de carpeta
        if not ruta_carpeta_sqls.endswith('/'):
            ruta_carpeta_sqls += '/'
            
        print(f"\n--- Iniciando motor SQL en s3://{bucket}/{ruta_carpeta_sqls} ---")
        s3_client = boto3.client('s3')
        
        # 1. Listar el contenido de la carpeta
        respuesta = s3_client.list_objects_v2(Bucket=bucket, Prefix=ruta_carpeta_sqls)
        
        if 'Contents' not in respuesta:
            print(f"⚠️ La carpeta está vacía o no existe en S3.")
            return

        # 2. Filtrar solo los .sql y ordenarlos (01_..., 02_...)
        archivos_sql = sorted([
            obj['Key'] for obj in respuesta['Contents'] 
            if obj['Key'].endswith('.sql')
        ])
        
        if not archivos_sql:
            print("⚠️ No se encontraron archivos con extensión .sql en la carpeta.")
            return

        # 3. Iterar y ejecutar
        for key_sql in archivos_sql:
            nombre_archivo = key_sql.split('/')[-1]  # Extrae solo el nombre (ej: 01_filtro.sql)
            print(f"\n>> Ejecutando script: {nombre_archivo}")
            
            # Descargar el query
            archivo_obj = s3_client.get_object(Bucket=bucket, Key=key_sql)
            query_texto = archivo_obj['Body'].read().decode('utf-8')
            
            # Ejecutar el query en Spark
            df_resultado = spark_session.sql(query_texto)
            
            # Imprimir una muestra en CloudWatch
            print(f"✅ {nombre_archivo} ejecutado correctamente. Muestra:")
            df_resultado.show(5)
            
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO al procesar el archivo {key_sql}: {str(e)}")
        raise e
        
#df_raw = cargar_s3_particionado_a_vista(spark, ruta_s3_origen, vista_clientes)
diccionario_tablas = leer_mapeo_xlsx(BUCKET_SOURCE_TEMP_FILE, PREFIX_TEMPTABLE_FILE+FILE_TEMP_TABLES_NAME)

for vista, config in diccionario_tablas.items():
    cargar_s3_particionado_a_vista(spark, config['ruta'], vista, config['tipo'])
    
ejecutar_sqls_desde_s3(spark, BUCKET_SCRIPTS, PATH_SCRIPTS)
    
job.commit()