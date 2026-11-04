import sys
import boto3
import pandas as pd
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lit, current_timestamp, md5, concat_ws, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime
import io

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BUCKET_SOURCE',
    'ASSET_PREFIX',
    'BRONCE_BUCKET',
    'BRONCE_PREFIX',
    'CONTROL_BUCKET',
    'CONTROL_PREFIX',
    'SCHEMA_CONFIG_PATH'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_client = boto3.client('s3')

ASSET_BUCKET = args['BUCKET_SOURCE']
ASSET_PREFIX = args['ASSET_PREFIX']
BRONCE_BUCKET = args['BRONCE_BUCKET']
BRONCE_PREFIX = args['BRONCE_PREFIX']
CONTROL_BUCKET = args['CONTROL_BUCKET']
CONTROL_PREFIX = args['CONTROL_PREFIX']
SCHEMA_CONFIG_PATH = args['SCHEMA_CONFIG_PATH']

def cargar_schema_desde_s3(s3_path):
    """
    Carga la configuración del schema desde un archivo JSON en S3
    """
    try:
        if s3_path.startswith('s3://'):
            s3_path = s3_path[5:]
        
        bucket, key = s3_path.split('/', 1)
        
        print(f"Cargando schema desde s3://{bucket}/{key}")
        
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        schema_json = obj['Body'].read().decode('utf-8')
        
        schema_config = json.loads(schema_json)
        
        print(f"Schema cargado correctamente")
        print(f"Columnas definidas: {len(schema_config['columns'])}")
        
        if 'business_key' in schema_config:
            print(f"Clave de negocio: {schema_config['business_key']}")
        
        return schema_config
        
    except Exception as e:
        print(f"ERROR al cargar schema desde S3: {str(e)}")
        raise

def validar_estructura(df, expected_columns_config):
    """
    Valida que el DataFrame tenga la cantidad correcta de columnas según configuración JSON
    Los archivos TXT NO tienen headers, solo datos delimitados
    """
    df_columns_count = len(df.columns)
    
    # Obtener columnas requeridas y opcionales
    required_columns = [col for col in expected_columns_config['columns'] if col.get('required', True)]
    optional_columns = [col for col in expected_columns_config['columns'] if not col.get('required', True)]
    
    min_columns = len(required_columns)
    max_columns = len(expected_columns_config['columns'])
    
    print(f"Validación de estructura:")
    print(f"  Columnas en archivo: {df_columns_count}")
    print(f"  Columnas requeridas: {min_columns}")
    print(f"  Columnas totales esperadas: {max_columns}")
    
    # Validar cantidad de columnas
    if df_columns_count < min_columns:
        raise ValueError(
            f"El archivo tiene {df_columns_count} columnas, pero se requieren al menos {min_columns} columnas. "
            f"Columnas requeridas: {[col['name'] for col in required_columns]}"
        )
    
    if df_columns_count > max_columns:
        raise ValueError(
            f"El archivo tiene {df_columns_count} columnas, pero el máximo esperado es {max_columns}. "
            f"Columnas esperadas: {[col['name'] for col in expected_columns_config['columns']]}"
        )
    
    # Advertencia si faltan columnas opcionales
    if df_columns_count < max_columns:
        missing_count = max_columns - df_columns_count
        print(f"  ADVERTENCIA: Faltan {missing_count} columna(s) opcional(es)")
    
    print(f"✓ Validación de estructura completada")
    return True

def obtener_archivos_nuevos(bucket, prefix):
    """
    Lista archivos TXT delimitados en S3 que no han sido procesados
    """
    archivos = []
    
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            print(f"No se encontraron archivos en s3://{bucket}/{prefix}")
            return archivos
        
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.txt'):
                archivos.append({
                    'key': key,
                    'size': obj['Size'],
                    'last_modified': obj['LastModified']
                })
        
        print(f"Se encontraron {len(archivos)} archivos TXT")
        
    except Exception as e:
        print(f"Error al listar archivos: {str(e)}")
        raise
    
    return archivos

def leer_control_desde_s3():
    """
    Lee la tabla de control desde S3 (archivos Parquet)
    """
    # Definir el schema una vez
    schema_control = StructType([
        StructField('archivo', StringType(), True),
        StructField('registros_procesados', IntegerType(), True),
        StructField('fecha_procesamiento', StringType(), True),
        StructField('estado', StringType(), True)
    ])
    
    try:
        control_path = f"s3://{CONTROL_BUCKET}/{CONTROL_PREFIX}/"
        
        # Leer con schema explícito
        df_control = spark.read.schema(schema_control).parquet(control_path)
        
        print(f"Control cargado: {df_control.count()} registros")
        return df_control
        
    except Exception as e:
        print(f"Tabla de control no existe o error al leer: {str(e)}")
        print("Se creará una nueva tabla de control vacía")
        
        return spark.createDataFrame([], schema_control)

def verificar_archivo_procesado(archivo_key, control_df=None):
    """
    Verifica si un archivo ya fue procesado consultando la tabla de control
    """
    try:
        if control_df is None:
            control_df = leer_control_desde_s3()
        
        # Si el control está vacío, el archivo no ha sido procesado
        if control_df.count() == 0:
            print(f"Tabla de control vacía, archivo '{archivo_key}' no ha sido procesado")
            return False
        
        procesado = control_df.filter(col("archivo") == archivo_key).count() > 0
        
        if procesado:
            print(f"Archivo '{archivo_key}' ya fue procesado anteriormente")
        
        return procesado
        
    except Exception as e:
        print(f"Error al verificar archivo: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def leer_txt_desde_s3(bucket, key, schema_config, delimiter='|', encoding='utf-8'):
    """
    Lee un archivo TXT delimitado desde S3 y lo convierte en DataFrame de pandas
    Los archivos NO tienen headers, se asignan nombres desde el JSON schema
    """
    encodings_to_try = [encoding, 'latin-1', 'iso-8859-1', 'cp1252', 'utf-16']
    
    # Obtener nombres de columnas del schema JSON
    column_names = [col['name'] for col in schema_config['columns']]
    
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    txt_data = obj['Body'].read()
    
    for enc in encodings_to_try:
        try:
            print(f"Intentando leer con encoding: {enc}")
            
            df_pandas = pd.read_csv(
                io.BytesIO(txt_data),
                sep=delimiter,
                encoding=enc,
                engine='python',
                header=None,  # NO hay headers en el archivo
                names=column_names,  # Asignar nombres desde JSON
                skipinitialspace=True,
                on_bad_lines='warn'
            )
            
            print(f"Archivo leído exitosamente con encoding: {enc}")
            print(f"  Archivo: {key}")
            print(f"  Registros: {len(df_pandas)}")
            print(f"  Columnas asignadas: {len(df_pandas.columns)}")
            print(f"  Nombres: {list(df_pandas.columns)}")
            
            return df_pandas
            
        except UnicodeDecodeError:
            print(f"  Encoding {enc} no funcionó, probando siguiente...")
            continue
        except Exception as e:
            print(f"  Error con encoding {enc}: {str(e)}")
            continue
    
    raise Exception(f"No se pudo leer el archivo {key} con ningún encoding disponible: {encodings_to_try}")

def calcular_hash_registro(df):
    """
    Calcula un hash para cada registro para detectar cambios
    """
    columnas_excluidas = ['hash_registro', 'fecha_carga', 'archivo_origen', 'ano', 'mes', 'dia']
    columnas_hash = [c for c in df.columns if c not in columnas_excluidas]
    
    df = df.withColumn(
        'hash_registro',
        md5(concat_ws('|', *[col(c).cast('string') for c in columnas_hash]))
    )
    
    print(f"Hash calculado usando columnas: {columnas_hash}")
    return df

def leer_datos_existentes_bronce():
    """
    Lee todos los datos existentes en la capa BRONCE desde S3
    """
    try:
        bronce_path = f"s3://{BRONCE_BUCKET}/{BRONCE_PREFIX}/"
        
        df_existente = spark.read.parquet(bronce_path)
        
        print(f"Datos existentes en BRONCE: {df_existente.count()} registros")
        return df_existente
        
    except Exception as e:
        print(f"No hay datos existentes en BRONCE o error: {str(e)}")
        print("Todos los registros serán tratados como nuevos")
        return None

def detectar_deltas(df_nuevo, clave_negocio=None):
    """
    Detecta registros nuevos o modificados comparando con los datos existentes en BRONCE
    """
    df_existente = leer_datos_existentes_bronce()
    
    if df_existente is None:
        print("No hay datos previos, todos los registros son nuevos")
        return df_nuevo
    
    if clave_negocio is None:
        print("Método: Comparación por hash de registro completo")
        df_delta = df_nuevo.join(
            df_existente.select('hash_registro'),
            on='hash_registro',
            how='left_anti'
        )
    else:
        print(f"Método: Comparación por clave de negocio: {clave_negocio}")
        
        columnas_faltantes = [c for c in clave_negocio if c not in df_nuevo.columns]
        if columnas_faltantes:
            raise ValueError(f"Columnas de clave de negocio no encontradas: {columnas_faltantes}")
        
        df_nuevos = df_nuevo.join(
            df_existente.select(*clave_negocio).distinct(),
            on=clave_negocio,
            how='left_anti'
        )
        
        df_modificados = df_nuevo.join(
            df_existente.select(*clave_negocio, 'hash_registro'),
            on=clave_negocio,
            how='inner'
        ).filter(
            df_nuevo['hash_registro'] != df_existente['hash_registro']
        ).select(df_nuevo['*'])
        
        df_delta = df_nuevos.union(df_modificados)
        
        print(f"  - Registros nuevos: {df_nuevos.count()}")
        print(f"  - Registros modificados: {df_modificados.count()}")
    
    registros_delta = df_delta.count()
    print(f"Total registros delta detectados: {registros_delta}")
    
    return df_delta

def mover_archivo_a_procesados(bucket_source, archivo_key):
    """
    Mueve el archivo procesado a la carpeta /procesados/ en el mismo bucket BRONCE
    """
    try:
        # Extraer nombre del archivo sin la ruta completa
        nombre_archivo = archivo_key.split('/')[-1]
        
        # Ruta destino en BRONCE/procesados/
        destino_key = f"{BRONCE_PREFIX}/procesados/{nombre_archivo}"
        
        print(f"Moviendo archivo procesado...")
        print(f"  Origen: s3://{bucket_source}/{archivo_key}")
        print(f"  Destino: s3://{BRONCE_BUCKET}/{destino_key}")
        
        # Copiar el archivo al destino
        s3_client.copy_object(
            Bucket=BRONCE_BUCKET,
            CopySource={'Bucket': bucket_source, 'Key': archivo_key},
            Key=destino_key
        )
        
        # Eliminar el archivo del origen
        s3_client.delete_object(
            Bucket=bucket_source,
            Key=archivo_key
        )
        
        print(f"Archivo movido exitosamente a procesados")
        
    except Exception as e:
        print(f"ERROR al mover archivo a procesados: {str(e)}")
        print(f"El archivo permanecerá en su ubicación original")
        # No lanzamos excepción para no detener el proceso

def actualizar_control(archivo_key, registros_procesados):
    """
    Actualiza el control escribiendo un nuevo archivo parquet en S3
    """
    try:
        control_path = f"s3://{CONTROL_BUCKET}/{CONTROL_PREFIX}/"
        
        # Crear el nuevo registro
        nuevo_registro = [(
            archivo_key,
            registros_procesados,
            datetime.now().isoformat(),
            'SUCCESS'
        )]
        
        # Definir el schema explícitamente
        schema_control = StructType([
            StructField('archivo', StringType(), True),
            StructField('registros_procesados', IntegerType(), True),
            StructField('fecha_procesamiento', StringType(), True),
            StructField('estado', StringType(), True)
        ])
        
        nuevo_df = spark.createDataFrame(nuevo_registro, schema_control)
        
        control_existente = leer_control_desde_s3()
        
        if control_existente.count() > 0:
            control_final = control_existente.union(nuevo_df)
            print(f"Actualizando control existente...")
        else:
            control_final = nuevo_df
            print(f"Creando control inicial...")
        
        control_final.write \
            .mode("overwrite") \
            .format("parquet") \
            .option("compression", "snappy") \
            .save(control_path)
        
        print(f"Control actualizado: {control_path}")
        print(f"  Archivo registrado: {archivo_key}")
        print(f"  Total archivos en control: {control_final.count()}")
        
    except Exception as e:
        print(f"Error al actualizar control: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def main():
    try:
        print("=== Iniciando proceso de carga incremental ===")
        print(f"Asset Bucket: s3://{ASSET_BUCKET}/{ASSET_PREFIX}")
        print(f"Bronce Bucket: s3://{BRONCE_BUCKET}/{BRONCE_PREFIX}")
        print(f"Control Bucket: s3://{CONTROL_BUCKET}/{CONTROL_PREFIX}")
        
        schema_config = cargar_schema_desde_s3(SCHEMA_CONFIG_PATH)
        
        clave_negocio = schema_config.get('business_key', None)
        
        if clave_negocio:
            print(f"Clave de negocio configurada: {clave_negocio}")
        else:
            print("No hay clave de negocio definida, se usará hash completo del registro")
        
        archivos = obtener_archivos_nuevos(ASSET_BUCKET, ASSET_PREFIX)
        
        if not archivos:
            print("No hay archivos para procesar")
            job.commit()
            return
        
        # Cargar control una sola vez al inicio
        control_df = leer_control_desde_s3()
        
        df_total = None
        archivos_procesados = 0
        archivos_con_error = []
        archivos_exitosos = []
        total_registros_leidos = 0
        
        for archivo in archivos:
            archivo_key = archivo['key']
            print(f"\n--- Procesando: {archivo_key} ---")
            
            if verificar_archivo_procesado(archivo_key, control_df):
                print(f"Archivo ya procesado, saltando...")
                continue
            
            try:
                df_pandas = leer_txt_desde_s3(ASSET_BUCKET, archivo_key, schema_config, delimiter='|')
                
                # Validación estricta de estructura (por cantidad de columnas)
                validar_estructura(df_pandas, schema_config)
                print("✓ Estructura validada correctamente")
                
                df_spark = spark.createDataFrame(df_pandas)
                
                df_spark = df_spark.withColumn('archivo_origen', lit(archivo_key))
                df_spark = df_spark.withColumn('fecha_carga', current_timestamp())
                
                df_spark = calcular_hash_registro(df_spark)
                
                registros_archivo = df_spark.count()
                total_registros_leidos += registros_archivo
                print(f"✓ Registros leídos del archivo: {registros_archivo}")
                
                if df_total is None:
                    df_total = df_spark
                else:
                    df_total = df_total.union(df_spark)
                
                archivos_exitosos.append({
                    'key': archivo_key,
                    'registros': registros_archivo
                })
                archivos_procesados += 1
                
            except ValueError as ve:
                error_msg = f"ERROR DE VALIDACIÓN DE ESQUEMA en {archivo_key}: {str(ve)}"
                print(f"✗ {error_msg}")
                archivos_con_error.append({
                    'key': archivo_key,
                    'error': str(ve),
                    'tipo': 'VALIDACION_ESQUEMA'
                })
                # NO detener el proceso, continuar con el siguiente archivo
                continue
                
            except Exception as e:
                error_msg = f"ERROR al procesar {archivo_key}: {str(e)}"
                print(f"✗ {error_msg}")
                archivos_con_error.append({
                    'key': archivo_key,
                    'error': str(e),
                    'tipo': 'ERROR_PROCESAMIENTO'
                })
                # NO detener el proceso, continuar con el siguiente archivo
                continue
        
        # Resumen de validación de archivos
        print(f"\n{'='*60}")
        print(f"RESUMEN DE VALIDACIÓN DE ARCHIVOS")
        print(f"{'='*60}")
        print(f"Total archivos encontrados: {len(archivos)}")
        print(f"Archivos procesados exitosamente: {len(archivos_exitosos)}")
        print(f"Archivos con errores: {len(archivos_con_error)}")
        
        if archivos_con_error:
            print(f"\n{'='*60}")
            print(f"DETALLE DE ARCHIVOS CON ERRORES:")
            print(f"{'='*60}")
            for err in archivos_con_error:
                print(f"\n✗ Archivo: {err['key']}")
                print(f"  Tipo error: {err['tipo']}")
                print(f"  Detalle: {err['error']}")
        
        if archivos_exitosos:
            print(f"\n{'='*60}")
            print(f"ARCHIVOS VÁLIDOS PARA PROCESAR:")
            print(f"{'='*60}")
            for arch in archivos_exitosos:
                print(f"✓ {arch['key']} - {arch['registros']} registros")
        
        # Si no hay archivos exitosos, terminar el proceso
        if df_total is None or df_total.count() == 0:
            print(f"\n{'='*60}")
            print("⚠ NO HAY DATOS VÁLIDOS PARA PROCESAR")
            print(f"{'='*60}")
            if archivos_con_error:
                print("Todos los archivos tienen errores de validación")
                print("Corrija los archivos y vuelva a ejecutar el proceso")
            job.commit()
            return
        
        print(f"\n{'='*60}")
        print(f"PROCESAMIENTO DE DATOS VÁLIDOS")
        print(f"{'='*60}")
        print(f"Total registros leídos: {total_registros_leidos}")
        
        print("\n=== Detectando deltas ===")
        df_delta = detectar_deltas(df_total, clave_negocio)
        
        registros_delta = df_delta.count()
        
        if registros_delta == 0:
            print("No hay cambios para aplicar")
            print("Actualizando control solo para archivos exitosos...")
            for archivo_exitoso in archivos_exitosos:
                actualizar_control(archivo_exitoso['key'], 0)
                mover_archivo_a_procesados(ASSET_BUCKET, archivo_exitoso['key'])
        else:
            print(f"Aplicando {registros_delta} registros (nuevos o modificados)")
            
            df_delta = df_delta.withColumn('ano', year(current_timestamp()))
            df_delta = df_delta.withColumn('mes', month(current_timestamp()))
            df_delta = df_delta.withColumn('dia', dayofmonth(current_timestamp()))
            
            bronce_path = f"s3://{BRONCE_BUCKET}/{BRONCE_PREFIX}/"
            
            print(f"Escribiendo datos en: {bronce_path}")
            print(f"Con particiones: ano/mes/dia")
            
            df_delta.write \
                .mode("append") \
                .format("parquet") \
                .option("compression", "snappy") \
                .partitionBy("ano", "mes", "dia") \
                .save(bronce_path)
            
            print("✓ Datos escritos en BRONCE correctamente (formato Parquet particionado)")
            
            print("\n=== Actualizando control solo para archivos exitosos ===")
            for archivo_exitoso in archivos_exitosos:
                actualizar_control(archivo_exitoso['key'], archivo_exitoso['registros'])
                mover_archivo_a_procesados(ASSET_BUCKET, archivo_exitoso['key'])
        
        print(f"\n{'='*60}")
        print(f"PROCESO COMPLETADO")
        print(f"{'='*60}")
        print(f"✓ Archivos procesados exitosamente: {len(archivos_exitosos)}")
        print(f"✗ Archivos con errores (no procesados): {len(archivos_con_error)}")
        print(f"✓ Total registros procesados: {total_registros_leidos}")
        print(f"✓ Registros delta aplicados: {registros_delta}")
        print(f"\nUbicaciones:")
        print(f"  BRONCE: s3://{BRONCE_BUCKET}/{BRONCE_PREFIX}/")
        print(f"  CONTROL: s3://{CONTROL_BUCKET}/{CONTROL_PREFIX}/")
        print(f"  PROCESADOS: s3://{BRONCE_BUCKET}/{BRONCE_PREFIX}/procesados/")
        
        if archivos_con_error:
            print(f"\n⚠ ATENCIÓN: {len(archivos_con_error)} archivo(s) no fueron procesados por errores")
            print(f"  Revise el detalle de errores arriba y corrija los archivos")
        
        job.commit()
        
    except Exception as e:
        print(f"\n!!! ERROR CRÍTICO EN EL JOB !!!")
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()