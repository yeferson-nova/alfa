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
from pyspark.sql.functions import col, lit, current_timestamp, md5, concat_ws
from pyspark.sql import functions as spark_sql_functions
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime
import re
from collections import defaultdict

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BUCKET_SOURCE',
    'ASSET_PREFIX',
    'BRONCE_BUCKET',
    'CONTROL_BUCKET',
    'CONTROL_PREFIX',
    'SCHEMA_CONFIG_PATH'  # ← UN SOLO JSON con todas las entidades
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
CONTROL_BUCKET = args['CONTROL_BUCKET']
CONTROL_PREFIX = args['CONTROL_PREFIX']
SCHEMA_CONFIG_PATH = args['SCHEMA_CONFIG_PATH']

# ============================================================================
# MAPEO DE ENTIDADES CARDIFF
# ============================================================================
ENTITY_MAPPING = {
    'DOM_CANCELATION': 'CANCELATION',
    'DOM_COLLECTIONS': 'COLLECTIONS',
    'DOM_COVERAGES': 'COVERAGES',
    'DOM_CUSTOMERS': 'CUSTOMERS',
    'DOM_EMISSIONS': 'EMISSIONS',
    'DOM_MATURITY': 'MATURITY',
    'DOM_POLICIES': 'POLICIES',
    'DOM_PRODUCTS': 'PRODUCTS',
    'DOM_REVERSES': 'REVERSES',
    'DOM_SINISTERS': 'SINISTERS'
}

def identify_entity_from_filename(filename):
    """Identifica la entidad desde el nombre del archivo."""
    file_name_only = filename.split('/')[-1]
    
    pattern_full = re.compile(r'(DOM_\w+)_FULL\.txt$', re.IGNORECASE)
    pattern_delta = re.compile(r'(DOM_\w+)_(\d{8})\.txt$', re.IGNORECASE)
    
    match_full = pattern_full.search(file_name_only)
    if match_full:
        entity = match_full.group(1).upper()
        if entity in ENTITY_MAPPING:
            return entity, 'FULL', None
    
    match_delta = pattern_delta.search(file_name_only)
    if match_delta:
        entity = match_delta.group(1).upper()
        date_str = match_delta.group(2)
        if entity in ENTITY_MAPPING:
            return entity, 'DELTA', date_str
    
    return None, None, None

def group_files_by_entity(archivos):
    """Agrupa archivos por entidad y ordena los DELTA por fecha."""
    grouped = defaultdict(list)
    
    for archivo in archivos:
        archivo_key = archivo['key']
        entity, file_type, date_str = identify_entity_from_filename(archivo_key)
        
        if entity:
            archivo['entity'] = entity
            archivo['file_type'] = file_type
            archivo['date_str'] = date_str if date_str else '00000000'
            grouped[entity].append(archivo)
    
    for entity in grouped:
        grouped[entity].sort(key=lambda x: (
            0 if x['file_type'] == 'FULL' else 1,
            x.get('date_str', '00000000')
        ))
    
    return dict(grouped)

def get_bronze_path_for_entity(entity):
    """Genera la ruta Bronze específica para cada entidad."""
    entity_name = ENTITY_MAPPING.get(entity, entity.replace('DOM_', ''))
    return f"CARDIFF/SMB_CARDIFF_{entity_name}"

def build_dated_path(base_path):
    """Construye ruta con formato base_path/año/mes/día usando fecha actual"""
    today = datetime.today()
    year_str = today.strftime("%Y")
    month_str = today.strftime("%m")
    day_str = today.strftime("%d")
    return f"{base_path}/{year_str}/{month_str}/{day_str}"

# ============================================================================
# FUNCIÓN: CARGAR TODOS LOS SCHEMAS DESDE UN SOLO JSON
# ============================================================================
def cargar_schemas_todas_entidades(s3_path):
    """
    Carga UN SOLO JSON con schemas de todas las entidades.
    
    Estructura esperada del JSON:
    {
      "CANCELATION": {
        "business_key": ["cancel_id"],
        "columns": [...]
      },
      "CUSTOMERS": {
        "business_key": ["customer_id"],
        "columns": [...]
      },
      ...
    }
    
    Args:
        s3_path: Ruta S3 al archivo JSON único
        
    Returns:
        dict: Diccionario con schemas por entidad
    """
    try:
        if s3_path.startswith('s3://'):
            s3_path = s3_path[5:]
        
        bucket, key = s3_path.split('/', 1)
        
        print(f"\n{'='*80}")
        print(f"CARGANDO SCHEMAS DE TODAS LAS ENTIDADES")
        print(f"{'='*80}")
        print(f"Archivo: s3://{bucket}/{key}")
        
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        schema_json = obj['Body'].read().decode('utf-8')
        schemas_config = json.loads(schema_json)
        
        print(f"✓ Schemas cargados exitosamente")
        print(f"  Total entidades configuradas: {len(schemas_config)}")
        print(f"  Entidades disponibles: {list(schemas_config.keys())}")
        
        # Validar estructura básica
        for entity_name, config in schemas_config.items():
            if 'columns' not in config:
                raise ValueError(f"Entidad {entity_name} no tiene 'columns' definidas")
            
            required_count = sum(1 for col in config['columns'] if col.get('required', True))
            print(f"\n  📋 {entity_name}:")
            print(f"     - Columnas: {len(config['columns'])} (Req: {required_count})")
            
            if 'business_key' in config:
                bk = config['business_key']
                bk_display = bk if isinstance(bk, list) else [bk]
                print(f"     - Business Key: {bk_display}")
            else:
                print(f"     - ⚠️ WARNING: Sin business_key definida")
        
        print(f"\n{'='*80}")
        return schemas_config
        
    except Exception as e:
        print(f"\nERROR CRÍTICO al cargar schemas: {str(e)}")
        print(f"Verifica que el JSON tenga la estructura correcta")
        raise

def obtener_schema_para_entidad(schemas_config, entity):
    """
    Extrae el schema específico de una entidad desde el diccionario completo.
    
    Args:
        schemas_config: Diccionario con todos los schemas
        entity: Nombre de la entidad (DOM_CUSTOMERS, etc.)
        
    Returns:
        dict: Configuración del schema para esa entidad
    """
    entity_name = ENTITY_MAPPING.get(entity, entity.replace('DOM_', ''))
    
    if entity_name not in schemas_config:
        available = list(schemas_config.keys())
        raise ValueError(
            f"ERROR: No existe schema para entidad '{entity_name}'\n"
            f"Entidades disponibles en JSON: {available}\n"
            f"Agrega la configuración para '{entity_name}' en el archivo JSON"
        )
    
    schema_config = schemas_config[entity_name]
    
    print(f"\n--- Schema seleccionado para {entity_name} ---")
    print(f"  Columnas: {len(schema_config['columns'])}")
    
    if 'business_key' in schema_config:
        bk = schema_config['business_key']
        bk_display = bk if isinstance(bk, list) else [bk]
        print(f"  Business Key: {bk_display}")
    
    return schema_config

def analizar_primera_linea_y_validar(bucket, key, schema_config, delimiter='|'):
    """Análisis robusto de 3 casos: CONTEO, HEADER, o DATOS directos"""
    try:
        s3_path = f"s3://{bucket}/{key}"
        print(f"\n{'='*70}")
        print(f"ANÁLISIS DE ESTRUCTURA DEL ARCHIVO")
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
        print(f"\n🔍 PASO 3: Asumiendo DATOS directos sin header...")
        
        if num_campos_linea0 < num_columnas_requeridas:
            raise ValueError(f"ERROR: Primera línea tiene {num_campos_linea0} columnas, se requieren mínimo {num_columnas_requeridas}")
        
        if num_campos_linea0 > num_columnas_totales:
            raise ValueError(f"ERROR: Primera línea tiene {num_campos_linea0} columnas, máximo permitido {num_columnas_totales}")
        
        return {
            'tipo_detectado': 'DATOS_DIRECTOS',
            'es_datos_directos': True,
            'debe_saltar_linea0': False,
            'num_columnas_datos': num_campos_linea0,
            'num_columnas_requeridas': num_columnas_requeridas,
            'num_columnas_totales': num_columnas_totales
        }
        
    except Exception as e:
        print(f"ERROR analizando archivo: {str(e)}")
        raise

def leer_txt_desde_linea_datos(bucket, key, schema_config, info_analisis, delimiter='|'):
    """Lee archivo TXT desde la línea donde empiezan los datos reales."""
    try:
        s3_path = f"s3://{bucket}/{key}"
        columnas_schema = [col['name'] for col in schema_config['columns']]
        num_columnas_datos = info_analisis['num_columnas_datos']
        columnas_a_usar = columnas_schema[:num_columnas_datos]
        
        print(f"\nConfiguración de lectura:")
        print(f"  - Columnas en schema: {len(columnas_schema)}")
        print(f"  - Columnas detectadas: {num_columnas_datos}")
        print(f"  - Columnas a usar: {len(columnas_a_usar)}")
        
        if info_analisis['debe_saltar_linea0']:
            print(f"  - Saltando línea 0 (contiene {info_analisis['tipo_detectado']})")
            
            df_texto = spark.read.text(s3_path)
            from pyspark.sql.functions import row_number
            from pyspark.sql.window import Window
            
            window = Window.orderBy(spark_sql_functions.lit(1))
            df_con_numero = df_texto.withColumn("row_num", row_number().over(window))
            df_sin_primera = df_con_numero.filter("row_num > 1").drop("row_num")
            
            from pyspark.sql.functions import split, trim, size, length
            
            df_split = df_sin_primera.withColumn("data", split(trim("value"), f"\\{delimiter}"))
            df_split = df_split.filter(length("value") > 0)
            df_split = df_split.filter(size("data") >= info_analisis['num_columnas_requeridas'])
            
            for idx, col_name in enumerate(columnas_a_usar):
                df_split = df_split.withColumn(col_name, col("data")[idx])
            
            df_final = df_split.select(columnas_a_usar)
        else:
            print("  - Leyendo desde línea 0 (datos directos)")
            df_final = spark.read.option("delimiter", delimiter) \
                .option("header", "false") \
                .csv(s3_path)
            
            old_columns = df_final.columns
            for old_col, new_col in zip(old_columns[:len(columnas_a_usar)], columnas_a_usar):
                df_final = df_final.withColumnRenamed(old_col, new_col)
            
            df_final = df_final.select(columnas_a_usar)
        
        registros_leidos = df_final.count()
        print(f"Registros leídos: {registros_leidos}")
        
        if info_analisis.get('es_conteo') and info_analisis.get('conteo_esperado'):
            conteo_esperado = info_analisis['conteo_esperado']
            if registros_leidos != conteo_esperado:
                print(f"⚠️ ADVERTENCIA: Conteo no coincide")
                print(f"   - Esperado: {conteo_esperado}")
                print(f"   - Leído: {registros_leidos}")
        
        return df_final, registros_leidos
        
    except Exception as e:
        print(f"ERROR leyendo archivo: {str(e)}")
        raise

def calcular_hash_registro(df_spark):
    """Calcula hash MD5 de todos los campos del registro"""
    columnas = df_spark.columns
    columnas_sin_meta = [c for c in columnas if c not in ['archivo_origen', 'fecha_carga', 'hash_registro']]
    
    df_con_hash = df_spark.withColumn(
        'hash_registro',
        md5(concat_ws('||', *[col(c) for c in columnas_sin_meta]))
    )
    
    return df_con_hash

def leer_bronce_existente(entity):
    """Lee datos existentes en BRONCE para una entidad específica"""
    bronze_base_path = get_bronze_path_for_entity(entity)
    bronce_path = f"s3://{BRONCE_BUCKET}/{bronze_base_path}"
    
    print(f"Verificando datos existentes en: {bronce_path}")
    
    try:
        response = s3_client.list_objects_v2(
            Bucket=BRONCE_BUCKET,
            Prefix=bronze_base_path,
            MaxKeys=1
        )
        
        if 'Contents' in response:
            print(f"Leyendo datos existentes de BRONCE para {entity}...")
            df_existente = spark.read.parquet(bronce_path + "/")
            registros_existentes = df_existente.count()
            print(f"Registros existentes encontrados: {registros_existentes}")
            return df_existente
        else:
            print(f"No hay datos previos en BRONCE para {entity} (primera carga)")
            return None
            
    except Exception as e:
        print(f"Error leyendo BRONCE para {entity}: {str(e)}")
        return None

def detectar_deltas_scd_type1(df_nuevo, clave_negocio, entity):
    """Detecta cambios según SCD Type 1 para una entidad específica."""
    print(f"Detectando deltas para entidad: {entity}")
    print(f"Clave de negocio: {clave_negocio}")
    
    df_existente = leer_bronce_existente(entity)
    
    if df_existente is None:
        print("Primera carga - todos los registros son nuevos")
        estadisticas = {
            'registros_nuevos': df_nuevo.count(),
            'registros_modificados': 0,
            'registros_sin_cambios': 0,
            'total_final': df_nuevo.count(),
            'es_primera_carga': True
        }
        return df_nuevo, estadisticas
    
    print("Comparando con datos existentes...")
    
    df_nuevo_con_marca = df_nuevo.withColumn('_es_nuevo', lit(1))
    df_existente_con_marca = df_existente.withColumn('_es_existente', lit(1))
    
    # Soporte para business_key simple o compuesta
    if isinstance(clave_negocio, list):
        join_condition = [col(f'nuevo.{k}') == col(f'existente.{k}') for k in clave_negocio]
    else:
        join_condition = col(f'nuevo.{clave_negocio}') == col(f'existente.{clave_negocio}')
    
    df_joined = df_nuevo_con_marca.alias('nuevo').join(
        df_existente_con_marca.alias('existente'),
        join_condition,
        'full_outer'
    )
    
    from pyspark.sql.functions import when, coalesce
    
    df_categorizado = df_joined.withColumn(
        'tipo_cambio',
        when(col('_es_nuevo').isNull(), 'eliminado')
        .when(col('_es_existente').isNull(), 'nuevo')
        .when(col('nuevo.hash_registro') != col('existente.hash_registro'), 'modificado')
        .otherwise('sin_cambios')
    )
    
    registros_nuevos = df_categorizado.filter("tipo_cambio = 'nuevo'").count()
    registros_modificados = df_categorizado.filter("tipo_cambio = 'modificado'").count()
    registros_sin_cambios = df_categorizado.filter("tipo_cambio = 'sin_cambios'").count()
    
    print(f"Resultados de comparación:")
    print(f"  - Nuevos: {registros_nuevos}")
    print(f"  - Modificados: {registros_modificados}")
    print(f"  - Sin cambios: {registros_sin_cambios}")
    
    columnas_finales = df_nuevo.columns
    
    df_final = df_categorizado.filter("tipo_cambio IN ('nuevo', 'modificado')").select(
        *[coalesce(col(f'nuevo.{c}'), col(f'existente.{c}')).alias(c) for c in columnas_finales]
    )
    
    df_sin_cambios_original = df_categorizado.filter("tipo_cambio = 'sin_cambios'").select(
        *[col(f'existente.{c}').alias(c) for c in columnas_finales]
    )
    
    df_resultado = df_final.union(df_sin_cambios_original)
    total_final = df_resultado.count()
    
    estadisticas = {
        'registros_nuevos': registros_nuevos,
        'registros_modificados': registros_modificados,
        'registros_sin_cambios': registros_sin_cambios,
        'total_final': total_final,
        'es_primera_carga': False
    }
    
    return df_resultado, estadisticas

def escribir_bronce_scd_type1(df_final, estadisticas, entity):
    """Escribe datos en BRONCE usando estrategia de reemplazo temporal."""
    bronze_base_path = get_bronze_path_for_entity(entity)
    bronce_path_base = f"s3://{BRONCE_BUCKET}/{bronze_base_path}"
    
    bronce_path_final = build_dated_path(bronce_path_base)
    bronce_path_temp = f"{bronce_path_final}_temp_{uuid.uuid4().hex[:8]}"
    
    print(f"\nEscribiendo en BRONCE para {entity}:")
    print(f"  Ruta temporal: {bronce_path_temp}")
    print(f"  Ruta final: {bronce_path_final}")
    print(f"  Total registros a escribir: {estadisticas['total_final']}")
    
    try:
        print("Escribiendo a ubicación temporal...")
        df_final.write \
            .mode("overwrite") \
            .format("parquet") \
            .option("compression", "snappy") \
            .save(bronce_path_temp)
        
        df_verificacion = spark.read.parquet(bronce_path_temp)
        registros_escritos = df_verificacion.count()
        print(f"Registros escritos en temporal: {registros_escritos}")
        
        if registros_escritos != estadisticas['total_final']:
            raise ValueError(f"Error: Se esperaban {estadisticas['total_final']} registros pero se escribieron {registros_escritos}")
        
        bucket_name = BRONCE_BUCKET
        prefix_final = bronce_path_final.replace(f's3://{bucket_name}/', '')
        
        print(f"Limpiando destino final: {prefix_final}")
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix_final)
        
        delete_keys = []
        for page in pages:
            if 'Contents' in page:
                delete_keys.extend([{'Key': obj['Key']} for obj in page['Contents']])
        
        if delete_keys:
            print(f"Eliminando {len(delete_keys)} archivos existentes...")
            for i in range(0, len(delete_keys), 1000):
                batch = delete_keys[i:i+1000]
                s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': batch}
                )
        
        prefix_temp = bronce_path_temp.replace(f's3://{bucket_name}/', '')
        
        print("Moviendo archivos de temporal a final...")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix_temp)
        
        if 'Contents' in response:
            for obj in response['Contents']:
                old_key = obj['Key']
                new_key = old_key.replace(prefix_temp, prefix_final)
                
                copy_source = {'Bucket': bucket_name, 'Key': old_key}
                s3_client.copy_object(
                    CopySource=copy_source,
                    Bucket=bucket_name,
                    Key=new_key
                )
                s3_client.delete_object(Bucket=bucket_name, Key=old_key)
        
        print(f"✓ Escritura completada exitosamente en: {bronce_path_final}")
        
    except Exception as e:
        print(f"ERROR al escribir en BRONCE para {entity}: {str(e)}")
        raise

# ============================================================================
# CONTROL ÚNICO (NO SEPARADO POR ENTIDAD)
# ============================================================================
def leer_control_unico():
    """Lee tabla de control ÚNICA desde S3"""
    control_path = f"s3://{CONTROL_BUCKET}/{CONTROL_PREFIX}"
    
    try:
        response = s3_client.list_objects_v2(
            Bucket=CONTROL_BUCKET,
            Prefix=CONTROL_PREFIX,
            MaxKeys=1
        )
        
        if 'Contents' in response:
            print(f"Leyendo control único desde: {control_path}")
            df_control = spark.read.parquet(control_path)
            registros_control = df_control.count()
            print(f"  Registros en control: {registros_control}")
            return df_control
        else:
            print(f"No existe tabla de control previa (primera ejecución)")
            return None
            
    except Exception as e:
        print(f"Error leyendo control: {str(e)}")
        return None

def verificar_archivo_procesado(archivo_key, control_df):
    """Verifica si un archivo ya fue procesado en el control único"""
    if control_df is None:
        return False
    
    df_filtrado = control_df.filter(col('archivo') == archivo_key)
    
    if df_filtrado.count() > 0:
        registro = df_filtrado.first()
        print(f"Archivo ya procesado el {registro['fecha_procesamiento']}")
        print(f"   Entidad: {registro.get('entidad', 'N/A')}")
        print(f"   Registros procesados: {registro['registros_procesados']}")
        return True
    
    return False

def actualizar_control_unico(archivo_key, registros_procesados, entity):
    """Actualiza la tabla de control ÚNICA en S3"""
    entity_name = ENTITY_MAPPING.get(entity, entity.replace('DOM_', ''))
    control_path = f"s3://{CONTROL_BUCKET}/{CONTROL_PREFIX}"
    
    try:
        nuevo_registro = [(
            archivo_key,
            entity_name,
            registros_procesados,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'COMPLETADO'
        )]
        
        schema_control = StructType([
            StructField('archivo', StringType(), True),
            StructField('entidad', StringType(), True),
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
        print(f"   Entidad: {entity_name}")
        print(f"   Registros: {registros_procesados}")
        
    except Exception as e:
        print(f"ERROR al actualizar control: {str(e)}")
        raise

def mover_archivo_a_procesados(bucket, key):
    """Mueve archivo a carpeta PROCESSED después de procesarlo"""
    try:
        path_parts = key.rsplit('/', 1)
        if len(path_parts) == 2:
            path_prefix = path_parts[0]
            filename = path_parts[1]
            new_key = f"{path_prefix}/PROCESSED/{filename}"
        else:
            new_key = f"PROCESSED/{key}"
        
        print(f"Moviendo archivo a: {new_key}")
        
        copy_source = {'Bucket': bucket, 'Key': key}
        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=bucket,
            Key=new_key
        )
        
        s3_client.delete_object(Bucket=bucket, Key=key)
        
        print(f"Archivo movido exitosamente")
        
    except Exception as e:
        print(f"ERROR moviendo archivo: {str(e)}")

def obtener_archivos_nuevos(bucket, prefix):
    """Obtiene lista de archivos nuevos para procesar"""
    archivos = []
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    
                    if key.endswith('/') or 'PROCESSED' in key:
                        continue
                    
                    if key.endswith('.txt') and 'DOM_' in key:
                        archivos.append({
                            'key': key,
                            'size': obj['Size'],
                            'size_mb': round(obj['Size'] / (1024 * 1024), 2),
                            'last_modified': obj['LastModified']
                        })
        
        print(f"Archivos Cardiff encontrados: {len(archivos)}")
        return archivos
        
    except Exception as e:
        print(f"Error obteniendo archivos: {str(e)}")
        return []

def main():
    try:
        print("="*80)
        print("=== PROCESO DE CARGA INCREMENTAL MULTI-ENTIDAD SCD TYPE 1 ===")
        print("=== JSON Único + Control Único ===")
        print("="*80)
        print(f"Asset Bucket: s3://{ASSET_BUCKET}/{ASSET_PREFIX}")
        print(f"Bronce Bucket: s3://{BRONCE_BUCKET}")
        print(f"Control Bucket: s3://{CONTROL_BUCKET}/{CONTROL_PREFIX}")
        print(f"Schema Config: {SCHEMA_CONFIG_PATH}")
        print("="*80)
        
        # CARGAR TODOS LOS SCHEMAS DESDE UN SOLO JSON
        schemas_config = cargar_schemas_todas_entidades(SCHEMA_CONFIG_PATH)
        
        # Obtener todos los archivos
        archivos = obtener_archivos_nuevos(ASSET_BUCKET, ASSET_PREFIX)
        
        if not archivos:
            print("No hay archivos para procesar")
            job.commit()
            return
        
        # AGRUPAR ARCHIVOS POR ENTIDAD
        archivos_por_entidad = group_files_by_entity(archivos)
        
        print(f"\n{'='*80}")
        print(f"ARCHIVOS AGRUPADOS POR ENTIDAD:")
        print(f"{'='*80}")
        for entity, files in archivos_por_entidad.items():
            entity_name = ENTITY_MAPPING.get(entity, entity)
            print(f"\n{entity} → SMB_CARDIFF_{entity_name}:")
            for f in files:
                print(f"  - {f['key'].split('/')[-1]} ({f['file_type']}) - {f['size_mb']} MB")
        
        # LEER CONTROL ÚNICO
        control_df = leer_control_unico()
        
        # Procesar cada entidad
        resumen_global = {
            'total_entidades': len(archivos_por_entidad),
            'entidades_procesadas': [],
            'total_archivos': 0,
            'total_registros': 0
        }
        
        for entity, archivos_entidad in archivos_por_entidad.items():
            entity_name = ENTITY_MAPPING.get(entity, entity)
            
            print(f"\n{'='*80}")
            print(f"PROCESANDO ENTIDAD: {entity} → SMB_CARDIFF_{entity_name}")
            print(f"{'='*80}")
            print(f"Archivos a procesar: {len(archivos_entidad)}")
            print(f"Ruta destino: s3://{BRONCE_BUCKET}/{get_bronze_path_for_entity(entity)}/")
            
            # OBTENER SCHEMA ESPECÍFICO PARA ESTA ENTIDAD
            try:
                schema_config = obtener_schema_para_entidad(schemas_config, entity)
                clave_negocio = schema_config.get('business_key', None)
                
                if not clave_negocio:
                    print(f"⚠️ WARNING: Entidad {entity_name} no tiene business_key definida")
                    print(f"Se saltará el procesamiento de esta entidad")
                    continue
                    
            except ValueError as ve:
                print(f"ERROR: {str(ve)}")
                print(f"Saltando entidad {entity_name}")
                continue
            
            archivos_exitosos = []
            archivos_con_error = []
            total_registros_entidad = 0
            
            # Procesar archivos de esta entidad en orden
            for archivo in archivos_entidad:
                archivo_key = archivo['key']
                archivo_size_mb = archivo.get('size_mb', 0)
                file_type = archivo.get('file_type', 'UNKNOWN')
                
                print(f"\n{'='*70}")
                print(f"PROCESANDO: {archivo_key} ({archivo_size_mb} MB)")
                print(f"Tipo: {file_type}")
                print(f"Entidad: {entity_name}")
                print(f"{'='*70}")
                
                # Verificar si ya fue procesado (CONTROL ÚNICO)
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
                    
                    # PASO 5: Aplicar SCD Type 1 (con entity)
                    print(f"\n--- Aplicando SCD Type 1 para {entity_name} ---")
                    df_final, estadisticas = detectar_deltas_scd_type1(df_spark, clave_negocio, entity)
                    
                    # PASO 6: Escribir en BRONCE (con entity)
                    escribir_bronce_scd_type1(df_final, estadisticas, entity)
                    
                    # PASO 7: Actualizar CONTROL ÚNICO
                    actualizar_control_unico(archivo_key, registros_reales, entity)
                    
                    # PASO 8: Mover archivo a PROCESSED
                    mover_archivo_a_procesados(ASSET_BUCKET, archivo_key)
                    
                    archivos_exitosos.append({
                        'key': archivo_key,
                        'entity': entity_name,
                        'file_type': file_type,
                        'registros': registros_reales,
                        'registros_nuevos': estadisticas['registros_nuevos'],
                        'registros_modificados': estadisticas['registros_modificados'],
                        'registros_sin_cambios': estadisticas['registros_sin_cambios'],
                        'total_final': estadisticas['total_final'],
                        'tipo_detectado': info_analisis['tipo_detectado'],
                        'es_primera_carga': estadisticas.get('es_primera_carga', False)
                    })
                    
                    total_registros_entidad += registros_reales
                    df_spark.unpersist()
                    
                except ValueError as ve:
                    print(f"ERROR DE VALIDACIÓN: {str(ve)}")
                    archivos_con_error.append({
                        'key': archivo_key,
                        'entity': entity_name,
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
                        'entity': entity_name,
                        'error': str(e),
                        'tipo': 'ERROR_PROCESAMIENTO'
                    })
                    continue
            
            # Resumen por entidad
            print(f"\n{'='*70}")
            print(f"RESUMEN ENTIDAD {entity_name}")
            print(f"{'='*70}")
            print(f"Archivos procesados: {len(archivos_exitosos)}")
            print(f"Archivos con error: {len(archivos_con_error)}")
            print(f"Total registros: {total_registros_entidad}")
            
            if archivos_exitosos:
                print("\nArchivos exitosos:")
                for arch in archivos_exitosos:
                    print(f"  • {arch['key'].split('/')[-1]} ({arch['file_type']})")
                    if arch['es_primera_carga']:
                        print(f"    PRIMERA CARGA - {arch['registros']} registros")
                    else:
                        print(f"    N:{arch['registros_nuevos']} M:{arch['registros_modificados']} S:{arch['registros_sin_cambios']}")
            
            if archivos_con_error:
                print("\nArchivos con error:")
                for err in archivos_con_error:
                    print(f"  ✗ {err['key'].split('/')[-1]}: {err['error'][:50]}...")
            
            resumen_global['entidades_procesadas'].append({
                'entity': entity_name,
                'archivos_exitosos': len(archivos_exitosos),
                'archivos_error': len(archivos_con_error),
                'registros': total_registros_entidad,
                'detalles': archivos_exitosos
            })
            resumen_global['total_archivos'] += len(archivos_exitosos)
            resumen_global['total_registros'] += total_registros_entidad
        
        # RESUMEN FINAL GLOBAL
        print(f"\n{'='*80}")
        print(f"RESUMEN FINAL GLOBAL")
        print(f"{'='*80}")
        print(f"Entidades procesadas: {resumen_global['total_entidades']}")
        print(f"Total archivos procesados: {resumen_global['total_archivos']}")
        print(f"Total registros procesados: {resumen_global['total_registros']:,}")
        
        print(f"\n--- DETALLES POR ENTIDAD ---")
        for entidad_resumen in resumen_global['entidades_procesadas']:
            print(f"\n📁 SMB_CARDIFF_{entidad_resumen['entity']}:")
            print(f"   Archivos exitosos: {entidad_resumen['archivos_exitosos']}")
            print(f"   Total registros: {entidad_resumen['registros']:,}")
            
            for detalle in entidad_resumen['detalles']:
                print(f"     • {detalle['key'].split('/')[-1]} ({detalle['file_type']})")
                if detalle['es_primera_carga']:
                    print(f"       PRIMERA CARGA - {detalle['registros']:,} registros nuevos")
                else:
                    print(f"       Nuevos: {detalle['registros_nuevos']:,}, Modificados: {detalle['registros_modificados']:,}, Sin cambios: {detalle['registros_sin_cambios']:,}")
        
        print(f"\n{'='*80}")
        print("PROCESO COMPLETADO EXITOSAMENTE")
        print(f"{'='*80}")
        
        job.commit()
        
    except Exception as e:
        print(f"\nERROR CRÍTICO")
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()