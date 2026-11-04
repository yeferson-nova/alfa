import sys
import boto3
import json
import time
import uuid
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, current_timestamp, md5, concat_ws, year, month, dayofmonth, max as spark_max, input_file_name
from pyspark.sql import functions as spark_sql_functions
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from datetime import datetime
import re

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

spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

s3_client = boto3.client('s3')

ASSET_BUCKET = args['BUCKET_SOURCE']
ASSET_PREFIX = args['ASSET_PREFIX']
BRONCE_BUCKET = args['BRONCE_BUCKET']
BRONCE_PREFIX = args['BRONCE_PREFIX']
CONTROL_BUCKET = args['CONTROL_BUCKET']
CONTROL_PREFIX = args['CONTROL_PREFIX']
SCHEMA_CONFIG_PATH = args['SCHEMA_CONFIG_PATH']


def build_dated_path(base_path):
    """Construye ruta con formato base_path/año/mes/día usando fecha actual"""
    today = datetime.today()
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")
    return f"{base_path}/{year}/{month}/{day}"


def cargar_schema_desde_s3(s3_path):
    """Carga configuración del schema desde archivo JSON en S3"""
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
        
        required_count = sum(1 for col in schema_config['columns'] if col.get('required', True))
        optional_count = len(schema_config['columns']) - required_count
        
        print(f"  - Requeridas: {required_count}")
        print(f"  - Opcionales: {optional_count}")
        
        if 'business_key' in schema_config:
            print(f"Clave de negocio: {schema_config['business_key']}")
        
        required_cols = [col['name'] for col in schema_config['columns'] if col.get('required', True)]
        print(f"Columnas requeridas: {required_cols[:5]}{'...' if len(required_cols) > 5 else ''}")
        
        return schema_config
        
    except Exception as e:
        print(f"ERROR al cargar schema desde S3: {str(e)}")
        raise


def analizar_primera_linea_y_validar(bucket, key, schema_config, delimiter='|'):
    """Análisis robusto de 3 casos: CONTEO, HEADER, o DATOS directos"""
    try:
        s3_path = f"s3://{bucket}/{key}"
        print(f"\n{'='*70}")
        print(f"ANÁLISIS ROBUSTO DE ESTRUCTURA DEL ARCHIVO")
        print(f"{'='*70}")
        
        df_texto = spark.read.text(s3_path).limit(2)
        lineas = [row.value for row in df_texto.collect()]
        
        if len(lineas) < 1:
            raise ValueError("El archivo está vacío. No se puede procesar.")
        
        primera_linea = lineas[0].strip()
        campos_linea0 = primera_linea.split(delimiter)
        num_campos_linea0 = len(campos_linea0)
        
        print(f"\n📋 LÍNEA 0 (primera línea del archivo):")
        print(f"   Contenido: {primera_linea[:100]}{'...' if len(primera_linea) > 100 else ''}")
        print(f"   Campos detectados: {num_campos_linea0}")
        print(f"   Primeros 5 campos: {campos_linea0[:5]}")
        
        columnas_requeridas = [col for col in schema_config['columns'] if col.get('required', True)]
        nombres_requeridos = [col['name'] for col in columnas_requeridas]
        num_columnas_requeridas = len(columnas_requeridas)
        num_columnas_totales = len(schema_config['columns'])
        
        print(f"\n📊 Schema configurado:")
        print(f"   Columnas REQUERIDAS: {num_columnas_requeridas}")
        print(f"   Columnas TOTALES: {num_columnas_totales}")
        
        # PASO 1: ¿ES CONTEO?
        es_conteo = False
        conteo_valor = None
        
        print(f"\n🔍 PASO 1: Verificando si es CONTEO...")
        
        if num_campos_linea0 == 1:
            try:
                conteo_valor = int(campos_linea0[0].strip())
                es_conteo = True
                print(f"   ✓ ES CONTEO: {conteo_valor}")
            except (ValueError, TypeError):
                print(f"   ✗ NO es conteo")
        
        if es_conteo:
            if len(lineas) < 2:
                raise ValueError(f"ERROR: Archivo tiene CONTEO pero no hay datos")
            
            linea_datos = lineas[1].strip()
            campos_datos = linea_datos.split(delimiter)
            num_columnas_datos = len(campos_datos)
            
            if num_columnas_datos < num_columnas_requeridas or num_columnas_datos > num_columnas_totales:
                raise ValueError(f"ERROR: Datos tienen {num_columnas_datos} columnas inválidas")
            
            return {
                'tipo_detectado': 'CONTEO',
                'es_conteo': True,
                'debe_saltar_linea0': True,
                'conteo_esperado': conteo_valor,
                'num_columnas_datos': num_columnas_datos,
                'num_columnas_requeridas': num_columnas_requeridas,
                'num_columnas_totales': num_columnas_totales
            }
        
        # PASO 2: ¿ES HEADER?
        print(f"\n🔍 PASO 2: Verificando si es HEADER...")
        
        campos_normalizados = [c.strip().lower() for c in campos_linea0]
        nombres_normalizados = [n.lower() for n in nombres_requeridos]
        
        coincidencias = sum(1 for i in range(min(len(campos_normalizados), len(nombres_normalizados))) 
                          if campos_normalizados[i] == nombres_normalizados[i])
        porcentaje = (coincidencias / num_columnas_requeridas * 100) if num_columnas_requeridas > 0 else 0
        
        if porcentaje >= 80.0:
            if num_campos_linea0 > num_columnas_totales:
                raise ValueError(f"ERROR: Header tiene {num_campos_linea0} columnas, máximo {num_columnas_totales}")
            
            if len(lineas) < 2:
                raise ValueError("ERROR: Archivo tiene HEADER pero no hay datos")
            
            linea_datos = lineas[1].strip()
            campos_datos = linea_datos.split(delimiter)
            num_columnas_datos = len(campos_datos)
            
            if num_columnas_datos < num_columnas_requeridas or num_columnas_datos > num_columnas_totales:
                raise ValueError(f"ERROR: Datos inválidos después del header")
            
            return {
                'tipo_detectado': 'HEADER',
                'es_header': True,
                'debe_saltar_linea0': True,
                'num_columnas_datos': num_columnas_datos,
                'num_columnas_requeridas': num_columnas_requeridas,
                'num_columnas_totales': num_columnas_totales
            }
        
        # PASO 3: DATOS DIRECTOS
        print(f"\n🔍 PASO 3: DATOS directos")
        
        if num_campos_linea0 < num_columnas_requeridas or num_campos_linea0 > num_columnas_totales:
            raise ValueError(f"ERROR: Datos tienen {num_campos_linea0} columnas inválidas")
        
        return {
            'tipo_detectado': 'DATOS',
            'debe_saltar_linea0': False,
            'num_columnas_datos': num_campos_linea0,
            'num_columnas_requeridas': num_columnas_requeridas,
            'num_columnas_totales': num_columnas_totales
        }
        
    except Exception as e:
        print(f"ERROR en análisis: {str(e)}")
        raise


def crear_spark_schema_desde_config(schema_config):
    """Convierte configuración JSON a StructType de Spark"""
    fields = []
    for col_config in schema_config['columns']:
        col_name = col_config['name']
        fields.append(StructField(col_name, StringType(), True))
    
    print(f"Schema Spark creado: {len(fields)} columnas")
    return StructType(fields)


def obtener_archivos_nuevos(bucket, prefix):
    """Lista archivos TXT delimitados en S3 y los ordena por fecha"""
    archivos = []
   
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
       
        for page in pages:
            if 'Contents' not in page:
                continue
               
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('.txt') and not key.endswith('/'):
                    size_mb = obj['Size'] / (1024 * 1024)
                   
                    # Extraer fecha del nombre del archivo (formato YYYYMMDD)
                    patron_fecha = r'_(\d{8})\.txt$'
                    match = re.search(patron_fecha, key)
                   
                    fecha_extraida = None
                    if match:
                        try:
                            fecha_str = match.group(1)
                            fecha_extraida = datetime.strptime(fecha_str, '%Y%m%d')
                        except ValueError:
                            fecha_extraida = None
                   
                    archivos.append({
                        'key': key,
                        'size': obj['Size'],
                        'size_mb': round(size_mb, 2),
                        'last_modified': obj['LastModified'],
                        'fecha_extraida': fecha_extraida
                    })
       
        # Ordenar: primero los SIN fecha, luego los que tienen fecha (de más antiguo a más reciente)
        archivos_con_fecha = [a for a in archivos if a['fecha_extraida'] is not None]
        archivos_sin_fecha = [a for a in archivos if a['fecha_extraida'] is None]
       
        archivos_con_fecha.sort(key=lambda x: x['fecha_extraida'])
       
        archivos_ordenados = archivos_sin_fecha + archivos_con_fecha
       
        print(f"\n{'='*70}")
        print(f"ORDEN DE PROCESAMIENTO")
        print(f"{'='*70}")
       
        if archivos_sin_fecha:
            print(f"\nArchivos SIN fecha (se procesan primero):")
            for i, archivo in enumerate(archivos_sin_fecha, 1):
                print(f"  {i}. {archivo['key']} ({archivo['size_mb']} MB)")
       
        if archivos_con_fecha:
            print(f"\nArchivos CON fecha (ordenados de más antiguo a más reciente):")
            for i, archivo in enumerate(archivos_con_fecha, 1):
                fecha_str = archivo['fecha_extraida'].strftime('%Y-%m-%d')
                print(f"  {len(archivos_sin_fecha) + i}. {archivo['key']} (Fecha: {fecha_str}, {archivo['size_mb']} MB)")
       
        print(f"\nTotal archivos TXT encontrados: {len(archivos_ordenados)}")
        print(f"{'='*70}\n")
       
    except Exception as e:
        print(f"Error al listar archivos: {str(e)}")
        raise
   
    return archivos_ordenados



def leer_control_desde_s3():
    """Lee tabla de control desde S3"""
    schema_control = StructType([
        StructField('archivo', StringType(), True),
        StructField('registros_procesados', IntegerType(), True),
        StructField('fecha_procesamiento', StringType(), True),
        StructField('estado', StringType(), True)
    ])
    
    try:
        control_path = f"s3://{CONTROL_BUCKET}/{CONTROL_PREFIX}/"
        print(f"Leyendo control desde: {control_path}")
        
        df_control = spark.read.schema(schema_control).parquet(control_path)
        registros_control = df_control.count()
        
        print(f"Control cargado: {registros_control} registros")
        return df_control
        
    except Exception as e:
        print(f"Tabla de control vacía: {str(e)}")
        return spark.createDataFrame([], schema_control)


def verificar_archivo_procesado(archivo_key, control_df=None):
    """Verifica si un archivo ya fue procesado"""
    try:
        if control_df is None:
            control_df = leer_control_desde_s3()
        
        if control_df.count() == 0:
            return False
        
        procesado = control_df.filter(col("archivo") == archivo_key).count() > 0
        
        if procesado:
            print(f"⊘ Archivo '{archivo_key}' ya procesado")
        
        return procesado
        
    except Exception as e:
        print(f"Error al verificar archivo: {str(e)}")
        return False


def leer_txt_desde_linea_datos(bucket, key, schema_config, info_analisis, delimiter='|'):
    """Lee archivo TXT según tipo detectado"""
    try:
        s3_path = f"s3://{bucket}/{key}"
        print(f"\n{'='*70}")
        print(f"LECTURA DE DATOS")
        print(f"{'='*70}")
        print(f"Archivo: {s3_path}")
        print(f"Tipo: {info_analisis['tipo_detectado']}")
        
        debe_saltar = info_analisis['debe_saltar_linea0']
        num_columnas_datos = info_analisis['num_columnas_datos']
        
        spark_schema = crear_spark_schema_desde_config(schema_config)
        
        schema_temporal = StructType([
            StructField(f"_c{i}", StringType(), True) 
            for i in range(num_columnas_datos)
        ])
        
        df_completo = spark.read \
            .schema(schema_temporal) \
            .option("header", "false") \
            .option("delimiter", delimiter) \
            .option("mode", "PERMISSIVE") \
            .option("encoding", "UTF-8") \
            .csv(s3_path)
        
        total_filas = df_completo.count()
        
        if debe_saltar:
            df_con_indice = df_completo.rdd.zipWithIndex()
            df_filtrado_rdd = df_con_indice.filter(lambda x: x[1] >= 1).map(lambda x: x[0])
            df_datos = spark.createDataFrame(df_filtrado_rdd, schema=df_completo.schema)
            registros_datos = df_datos.count()
        else:
            df_datos = df_completo
            registros_datos = total_filas
        
        num_columnas_esperadas = len(spark_schema.fields)
        columnas_seleccionadas = df_datos.columns[:num_columnas_esperadas]
        df_temp = df_datos.select(columnas_seleccionadas)
        
        nombres_nuevos = [field.name for field in spark_schema.fields]
        for i, nombre_nuevo in enumerate(nombres_nuevos):
            df_temp = df_temp.withColumnRenamed(df_temp.columns[i], nombre_nuevo)
        
        df = df_temp
        df.cache()
        
        print(f"\nLectura exitosa: {registros_datos} registros")
        
        return df, registros_datos
        
    except Exception as e:
        print(f"ERROR al leer archivo: {str(e)}")
        raise


def calcular_hash_registro(df):
    """Calcula hash MD5 para cada registro"""
    columnas_excluidas = ['hash_registro', 'fecha_carga', 'archivo_origen', 'ano', 'mes', 'dia', 'fecha_actualizacion']
    columnas_hash = [c for c in df.columns if c not in columnas_excluidas]
    
    df = df.withColumn(
        'hash_registro',
        md5(concat_ws('|', *[col(c).cast('string') for c in columnas_hash]))
    )
    
    return df


def detectar_deltas_scd_type1(df_nuevo, clave_negocio=None):
    """
    SCD Type 1 con materialización de datos ANTES de limpiar archivos
    Evita FileNotFoundException al eliminar archivos que el plan lazy aún necesita
    """
    try:
        bronce_path_base = f"s3://{BRONCE_BUCKET}/{BRONCE_PREFIX}/"
        
        print(f"\n{'='*70}")
        print(f"LEYENDO HISTÓRICO COMPLETO DE BRONCE")
        print(f"{'='*70}")
        print(f"Path base: {bronce_path_base}")
        
        try:
            # PASO 1: Leer con plan lazy
            df_existente = spark.read \
                .option("recursiveFileLookup", "true") \
                .option("pathGlobFilter", "*.parquet") \
                .option("mergeSchema", "true") \
                .parquet(bronce_path_base)
            
            # PASO 2: MATERIALIZAR INMEDIATAMENTE (CRÍTICO)
            print(f"Materializando datos históricos en memoria...")
            df_existente.cache()
            registros_existentes = df_existente.count()
            
            print(f"Histórico materializado: {registros_existentes} registros")
            
        except Exception as e:
            print(f"No hay histórico en BRONCE: {str(e)}")
            print(f"Primera carga - todos los registros son nuevos")
            
            df_final = df_nuevo \
                .withColumn('fecha_carga', current_timestamp()) \
                .withColumn('fecha_actualizacion', current_timestamp())
            
            return df_final, {
                'registros_nuevos': df_nuevo.count(),
                'registros_modificados': 0,
                'registros_sin_cambios': 0,
                'registros_existentes': 0,
                'total_final': df_nuevo.count(),
                'es_primera_carga': True
            }
        
        if clave_negocio is None:
            raise ValueError("SCD Type 1 REQUIERE clave de negocio")
        
        print(f"\nClave de negocio: {clave_negocio}")
        
        columnas_faltantes = [c for c in clave_negocio if c not in df_nuevo.columns]
        if columnas_faltantes:
            raise ValueError(f"Columnas faltantes: {columnas_faltantes}")
        
        # PASO 1: Registros NUEVOS
        print(f"\nPASO 1: Identificando registros NUEVOS...")
        
        claves_existentes = df_existente.select(*clave_negocio).distinct()
        df_nuevos = df_nuevo.join(claves_existentes, on=clave_negocio, how='left_anti')
        count_nuevos = df_nuevos.count()
        print(f"   Registros nuevos: {count_nuevos}")
        
        # PASO 2: Registros MODIFICADOS
        print(f"\nPASO 2: Identificando registros MODIFICADOS...")
        
        df_modificados = df_nuevo.alias('nuevo').join(
            df_existente.select(*clave_negocio, 'hash_registro').alias('existente'),
            on=clave_negocio,
            how='inner'
        ).filter(
            col('nuevo.hash_registro') != col('existente.hash_registro')
        ).select('nuevo.*')
        
        count_modificados = df_modificados.count()
        print(f"   Registros modificados: {count_modificados}")
        
        # PASO 3: Registros SIN CAMBIOS
        print(f"\nPASO 3: Identificando registros SIN cambios...")
        
        df_sin_cambios = df_existente.alias('existente').join(
            df_nuevo.select(*clave_negocio, 'hash_registro').alias('nuevo'),
            on=clave_negocio,
            how='inner'
        ).filter(
            col('existente.hash_registro') == col('nuevo.hash_registro')
        ).select('existente.*')
        
        count_sin_cambios = df_sin_cambios.count()
        print(f"   Registros sin cambios: {count_sin_cambios}")
        
        # PASO 4: CONSTRUIR DATASET FINAL
        print(f"\nPASO 4: Construyendo dataset final...")
        
        claves_modificadas = df_modificados.select(*clave_negocio)
        
        df_historico_vigente = df_existente.join(
            claves_modificadas,
            on=clave_negocio,
            how='left_anti'
        )
        
        df_nuevos_con_fecha = df_nuevos \
            .withColumn('fecha_carga', current_timestamp()) \
            .withColumn('fecha_actualizacion', current_timestamp())
        
        df_modificados_con_fecha = df_modificados \
            .withColumn('fecha_actualizacion', current_timestamp())
        
        df_final = df_historico_vigente \
            .union(df_nuevos_con_fecha) \
            .union(df_modificados_con_fecha)
        
        # MATERIALIZAR dataset final ANTES de limpiar
        print(f"Materializando dataset final...")
        df_final.cache()
        total_final = df_final.count()
        
        print(f"\n{'='*70}")
        print(f"RESUMEN SCD TYPE 1")
        print(f"{'='*70}")
        print(f"  Registros EXISTENTES: {registros_existentes}")
        print(f"  Registros NUEVOS: {count_nuevos}")
        print(f"  Registros MODIFICADOS: {count_modificados}")
        print(f"  Registros SIN CAMBIOS: {count_sin_cambios}")
        print(f"  Total FINAL: {total_final}")
        print(f"{'='*70}")
        
        df_existente.unpersist()
        
        estadisticas = {
            'registros_nuevos': count_nuevos,
            'registros_modificados': count_modificados,
            'registros_sin_cambios': count_sin_cambios,
            'registros_existentes': registros_existentes,
            'total_final': total_final,
            'es_primera_carga': False
        }
        
        return df_final, estadisticas
        
    except Exception as e:
        print(f"ERROR en SCD Type 1: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


def _verify_s3_files_exist(s3_path):
    """Verifica que existan archivos Parquet en path S3"""
    try:
        path_parts = s3_path.replace('s3://', '').split('/', 1)
        bucket = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ''
        
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        parquet_files = 0
        for page in pages:
            if 'Contents' in page:
                parquet_files += sum(1 for obj in page['Contents'] 
                                    if obj['Key'].endswith('.parquet'))
        
        return parquet_files
        
    except Exception as e:
        print(f"Error verificando files: {str(e)}")
        return 0


def _delete_s3_folder_safe(s3_path):
    """Elimina carpeta S3 de forma segura"""
    try:
        path_parts = s3_path.replace('s3://', '').split('/', 1)
        bucket = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ''
        
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        objects_to_delete = []
        for page in pages:
            if 'Contents' not in page:
                continue
            objects_to_delete.extend([{'Key': obj['Key']} for obj in page['Contents']])
        
        if objects_to_delete:
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i:i+1000]
                s3_client.delete_objects(Bucket=bucket, Delete={'Objects': batch})
            print(f"   Eliminados {len(objects_to_delete)} archivos")
        
    except Exception as e:
        print(f"   Warning al eliminar (no crítico): {str(e)}")


def _move_s3_folder_recursive(source_s3, dest_s3):
    """Mueve archivos de source a destination en S3"""
    try:
        source_parts = source_s3.replace('s3://', '').split('/', 1)
        dest_parts = dest_s3.replace('s3://', '').split('/', 1)
        
        bucket = source_parts[0]
        source_prefix = source_parts[1] if len(source_parts) > 1 else ''
        dest_prefix = dest_parts[1] if len(dest_parts) > 1 else ''
        
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=source_prefix)
        
        files_moved = 0
        for page in pages:
            if 'Contents' not in page:
                continue
            
            for obj in page['Contents']:
                source_key = obj['Key']
                relative_key = source_key.replace(source_prefix, '', 1)
                dest_key = dest_prefix + relative_key
                
                s3_client.copy_object(
                    CopySource={'Bucket': bucket, 'Key': source_key},
                    Bucket=bucket,
                    Key=dest_key
                )
                s3_client.delete_object(Bucket=bucket, Key=source_key)
                files_moved += 1
        
        print(f"   Movidos {files_moved} archivos")
        
    except Exception as e:
        print(f"ERROR moviendo archivos: {str(e)}")
        raise


def escribir_bronce_scd_type1(df_final, estadisticas):
    """
    Escribe en BRONCE con strategy de temporales para evitar race condition
    """
    try:
        bronce_path_base = f"s3://{BRONCE_BUCKET}/{BRONCE_PREFIX}/"
        today = datetime.today()
        
        # Path FINAL limpio
        bronce_path_final = f"{bronce_path_base}{today.year}/{today.month:02d}/{today.day:02d}/"
        
        # Path TEMPORAL único
        temp_suffix = f"_TEMP_{int(time.time())}{str(uuid.uuid4())[:8]}"
        bronce_path_temp = f"{bronce_path_base}{today.year}/{today.month:02d}/{today.day:02d}{temp_suffix}/"
        
        print(f"\n{'='*70}")
        print(f"ESCRITURA EN BRONCE (SCD TYPE 1 - CON STRATEGY TEMPORAL)")
        print(f"{'='*70}")
        print(f"Path FINAL:    {bronce_path_final}")
        print(f"Path TEMPORAL: {bronce_path_temp}")
        print(f"Registros: {estadisticas['total_final']}")
        
        try:
            # PASO 1: Escribir a path TEMPORAL
            print(f"\nPASO 1: Escribiendo a path TEMPORAL...")
            
            df_final.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("compression", "snappy") \
                .save(bronce_path_temp)
            
            print(f"   Datos escritos a temporal")
            
            # PASO 2: Verificar integridad
            print(f"\nPASO 2: Verificando integridad...")
            temp_files = _verify_s3_files_exist(bronce_path_temp)
            if not temp_files:
                raise Exception("No files written to temporary path!")
            print(f"   {temp_files} archivos verificados")
            
            # PASO 3: Esperar consistencia S3
            print(f"\nPASO 3: Esperando consistencia S3...")
            time.sleep(2)
            
            # PASO 4: Eliminar path final antiguo
            print(f"\nPASO 4: Limpiando path final antiguo...")
            _delete_s3_folder_safe(bronce_path_final)
            print(f"   Path antiguo eliminado")
            
            # PASO 5: Mover de TEMPORAL a FINAL
            print(f"\nPASO 5: Moviendo a path final...")
            _move_s3_folder_recursive(bronce_path_temp, bronce_path_final)
            print(f"   Datos movidos a final")
            
            # PASO 6: Verificar final
            print(f"\nPASO 6: Verificando integridad final...")
            final_files = _verify_s3_files_exist(bronce_path_final)
            if not final_files:
                raise Exception("No files in final path after move!")
            print(f"   {final_files} archivos verificados")
            
            # PASO 7: Limpiar temporal residual
            print(f"\nPASO 7: Limpiando temporal...")
            _delete_s3_folder_safe(bronce_path_temp)
            
            print(f"\n{'='*70}")
            print(f"BRONCE ACTUALIZADO CON ÉXITO")
            print(f"{'='*70}")
            print(f"   Path: {bronce_path_final}")
            print(f"   Nuevos: {estadisticas['registros_nuevos']}")
            print(f"   Actualizados: {estadisticas['registros_modificados']}")
            print(f"   Sin cambios: {estadisticas['registros_sin_cambios']}")
            print(f"   Total: {estadisticas['total_final']}")
            print(f"{'='*70}")
            
        except Exception as write_error:
            print(f"\nERROR durante escritura: {str(write_error)}")
            print(f"Limpiando path temporal...")
            _delete_s3_folder_safe(bronce_path_temp)
            raise
        
    except Exception as e:
        print(f"ERROR en escritura BRONCE: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


def mover_archivo_a_procesados(bucket_source, archivo_key):
    """Mueve archivo procesado a carpeta /procesados/"""
    try:
        nombre_archivo = archivo_key.split('/')[-1]
        destino_key = f"CLIENTS_ASSETS/phase_bronze/PROCESADOS/{nombre_archivo}"
        
        print(f"Moviendo archivo procesado...")
        print(f"  Origen: s3://{bucket_source}/{archivo_key}")
        print(f"  Destino: s3://{ASSET_BUCKET}/{destino_key}")
        
        s3_client.copy_object(
            Bucket=ASSET_BUCKET,
            CopySource={'Bucket': bucket_source, 'Key': archivo_key},
            Key=destino_key
        )
        
        s3_client.delete_object(
            Bucket=bucket_source,
            Key=archivo_key
        )
        
        print(f"Archivo movido exitosamente")
        
    except Exception as e:
        print(f"ERROR al mover archivo: {str(e)}")


def actualizar_control(archivo_key, registros_procesados):
    """Actualiza tabla de control con APPEND"""
    try:
        control_path_initial = f"s3://{CONTROL_BUCKET}/{CONTROL_PREFIX}/"
        control_path = build_dated_path(control_path_initial)
        
        nuevo_registro = [(
            archivo_key,
            registros_procesados,
            datetime.now().isoformat(),
            'SUCCESS'
        )]
        
        schema_control = StructType([
            StructField('archivo', StringType(), True),
            StructField('registros_procesados', IntegerType(), True),
            StructField('fecha_procesamiento', StringType(), True),
            StructField('estado', StringType(), True)
        ])
        
        nuevo_df = spark.createDataFrame(nuevo_registro, schema_control)
        
        nuevo_df.write \
            .mode("append") \
            .format("parquet") \
            .option("compression", "snappy") \
            .save(control_path)
        
        print(f"Control actualizado: {archivo_key}")
        print(f"   Ruta: {control_path}")
        print(f"   Registros: {registros_procesados}")
        
    except Exception as e:
        print(f"ERROR al actualizar control: {str(e)}")
        raise


def main():
    try:
        print("="*70)
        print("=== PROCESO DE CARGA INCREMENTAL SCD TYPE 1 ===")
        print("=== (Con strategy de temporales para evitar race conditions) ===")
        print("="*70)
        print(f"Asset Bucket: s3://{ASSET_BUCKET}/{ASSET_PREFIX}")
        print(f"Bronce Bucket: s3://{BRONCE_BUCKET}/{BRONCE_PREFIX}")
        print(f"Control Bucket: s3://{CONTROL_BUCKET}/{CONTROL_PREFIX}")
        
        schema_config = cargar_schema_desde_s3(SCHEMA_CONFIG_PATH)
        clave_negocio = schema_config.get('business_key', None)
        
        if clave_negocio:
            print(f"Clave de negocio: {clave_negocio}")
            print(f"Modo: SCD Type 1 (actualizaciones en su lugar)")
        else:
            print("ERROR: Se requiere 'business_key' en el schema para SCD Type 1")
            raise ValueError("Configure 'business_key' en el archivo JSON del schema")
        
        archivos = obtener_archivos_nuevos(ASSET_BUCKET, ASSET_PREFIX)
        
        if not archivos:
            print("No hay archivos para procesar")
            job.commit()
            return
        
        control_df = leer_control_desde_s3()
        
        archivos_exitosos = []
        archivos_con_error = []
        total_registros_procesados = 0
        
        for archivo in archivos:
            archivo_key = archivo['key']
            archivo_size_mb = archivo.get('size_mb', 0)
            
            print(f"\n{'='*70}")
            print(f"PROCESANDO: {archivo_key} ({archivo_size_mb} MB)")
            print(f"{'='*70}")
            
            if verificar_archivo_procesado(archivo_key, control_df):
                print(f"Archivo ya procesado, saltando...")
                continue
            
            try:
                # PASO 1: Analizar estructura
                info_analisis = analizar_primera_linea_y_validar(
                    ASSET_BUCKET, 
                    archivo_key, 
                    schema_config, 
                    delimiter='|'
                )
                
                # PASO 2: Leer datos
                df_spark, registros_reales = leer_txt_desde_linea_datos(
                    ASSET_BUCKET, 
                    archivo_key, 
                    schema_config,
                    info_analisis,
                    delimiter='|'
                )
                
                # PASO 3: Agregar metadatos
                df_spark = df_spark.withColumn('archivo_origen', lit(archivo_key))
                df_spark = df_spark.withColumn('fecha_carga', current_timestamp())
                
                # PASO 4: Calcular hash
                df_spark = calcular_hash_registro(df_spark)
                
                print(f"Registros del archivo: {registros_reales}")
                
                # PASO 5: Aplicar SCD Type 1
                print(f"\n--- Aplicando SCD Type 1 ---")
                df_final, estadisticas = detectar_deltas_scd_type1(df_spark, clave_negocio)
                
                # PASO 6: Escribir en BRONCE
                escribir_bronce_scd_type1(df_final, estadisticas)
                
                # PASO 7: Actualizar control
                actualizar_control(archivo_key, registros_reales)
                
                # PASO 8: Mover archivo a procesados
                mover_archivo_a_procesados(ASSET_BUCKET, archivo_key)
                
                archivos_exitosos.append({
                    'key': archivo_key,
                    'registros': registros_reales,
                    'registros_nuevos': estadisticas['registros_nuevos'],
                    'registros_modificados': estadisticas['registros_modificados'],
                    'registros_sin_cambios': estadisticas['registros_sin_cambios'],
                    'total_final': estadisticas['total_final'],
                    'tipo_detectado': info_analisis['tipo_detectado'],
                    'es_primera_carga': estadisticas.get('es_primera_carga', False)
                })
                
                total_registros_procesados += registros_reales
                df_spark.unpersist()
                
            except ValueError as ve:
                print(f"ERROR DE VALIDACIÓN: {str(ve)}")
                archivos_con_error.append({
                    'key': archivo_key,
                    'error': str(ve),
                    'tipo': 'VALIDACION_ESQUEMA'
                })
                continue
                
            except Exception as e:
                print(f"ERROR DE PROCESAMIENTO: {str(e)}")
                import traceback
                traceback.print_exc()
                archivos_con_error.append({
                    'key': archivo_key,
                    'error': str(e),
                    'tipo': 'ERROR_PROCESAMIENTO'
                })
                continue
        
        # RESUMEN FINAL
        print(f"\n{'='*70}")
        print(f"RESUMEN FINAL")
        print(f"{'='*70}")
        print(f"Total archivos encontrados: {len(archivos)}")
        print(f"Archivos procesados: {len(archivos_exitosos)}")
        print(f"Archivos con errores: {len(archivos_con_error)}")
        print(f"Total registros procesados: {total_registros_procesados}")
        
        if archivos_exitosos:
            print(f"\n--- ARCHIVOS PROCESADOS (SCD TYPE 1) ---")
            for arch in archivos_exitosos:
                print(f"  {arch['key']}")
                print(f"    - Tipo detectado: {arch['tipo_detectado']}")
                print(f"    - Registros procesados: {arch['registros']}")
                
                if arch['es_primera_carga']:
                    print(f"    - PRIMERA CARGA (todos nuevos)")
                else:
                    print(f"    - Nuevos: {arch['registros_nuevos']}")
                    print(f"    - Modificados: {arch['registros_modificados']}")
                    print(f"    - Sin cambios: {arch['registros_sin_cambios']}")
                
                print(f"    - Total final en BRONCE: {arch['total_final']}")
        
        if archivos_con_error:
            print(f"\n--- ARCHIVOS CON ERRORES ---")
            for err in archivos_con_error:
                print(f"  {err['key']}")
                print(f"    Tipo: {err['tipo']}")
                print(f"    Error: {err['error'][:200]}")
        
        print(f"\n{'='*70}")
        job.commit()
        
    except Exception as e:
        print(f"\nERROR CRÍTICO")
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()