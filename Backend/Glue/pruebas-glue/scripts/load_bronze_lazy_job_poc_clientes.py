"""
AWS Glue Job: Lazy Historic Loader - Single Job Sequential Processing
Procesa ventanas temporales secuencialmente en UN SOLO JOB
Cada ventana hace: Lectura Oracle → Escritura Bronze
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, sha2, concat_ws, to_date
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar
import re

# ==============================================================================
# CONFIGURACIÓN SPARK
# ==============================================================================

def setup_spark_configs(spark):
    """Configuraciones optimizadas para grandes volúmenes"""
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ==============================================================================
# GENERACIÓN DE VENTANAS TEMPORALES
# ==============================================================================

def parse_date(date_str):
    """Convierte DDMMYYYY a datetime"""
    return datetime.strptime(date_str, '%d%m%Y')

def get_last_day_of_month(year, month):
    """Retorna el último día del mes (maneja bisiestos)"""
    return calendar.monthrange(year, month)[1]

def generate_monthly_windows(init_date_str, end_date_str):
    """
    Genera ventanas mensuales completas
    
    Ejemplo:
        init: 01012025, end: 01032025
        Retorna:
        [
            (2025-01-01 00:00:00, 2025-01-31 23:59:59),
            (2025-02-01 00:00:00, 2025-02-28 23:59:59),
            (2025-03-01 00:00:00, 2025-03-31 23:59:59)
        ]
    """
    start_date = parse_date(init_date_str)
    
    if end_date_str.upper() == 'SYSDATE':
        end_date = datetime.now()
    else:
        end_date = parse_date(end_date_str)
    
    windows = []
    current_year = start_date.year
    current_month = start_date.month
    
    while True:
        # Primer día del mes
        window_start = datetime(current_year, current_month, 1, 0, 0, 0)
        
        # Último día del mes
        last_day = get_last_day_of_month(current_year, current_month)
        window_end = datetime(current_year, current_month, last_day, 23, 59, 59)
        
        # Si window_start ya pasó el end_date, terminamos
        if window_start > end_date:
            break
        
        # Si window_end excede end_date, ajustamos
        if window_end > end_date:
            window_end = end_date.replace(hour=23, minute=59, second=59)
        
        windows.append((window_start, window_end))
        
        # Avanzar al siguiente mes
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1
    
    return windows

def generate_daily_windows(init_date_str, end_date_str):
    """Genera ventanas diarias"""
    start_date = parse_date(init_date_str).replace(hour=0, minute=0, second=0)
    
    if end_date_str.upper() == 'SYSDATE':
        end_date = datetime.now()
    else:
        end_date = parse_date(end_date_str)
    
    windows = []
    current = start_date
    
    while current <= end_date:
        window_start = current.replace(hour=0, minute=0, second=0)
        window_end = current.replace(hour=23, minute=59, second=59)
        
        if window_end > end_date:
            window_end = end_date.replace(hour=23, minute=59, second=59)
        
        windows.append((window_start, window_end))
        current += timedelta(days=1)
    
    return windows

# ==============================================================================
# CONSTRUCCIÓN DINÁMICA DE QUERIES
# ==============================================================================

def inject_date_filter(base_query, filter_field, start_date, end_date):
    """
    Inyecta o reemplaza filtro temporal en la query SQL
    
    Args:
        base_query: Query SQL base
        filter_field: Campo para filtrar (ej: T.TIME_STAMP)
        start_date: datetime inicio
        end_date: datetime fin
    
    Returns:
        Query SQL con filtro temporal inyectado/reemplazado
    """
    start_str = start_date.strftime('%d/%m/%Y %H:%M:%S')
    end_str = end_date.strftime('%d/%m/%Y %H:%M:%S')
    
    # Crear filtro temporal
    date_filter = (
        f"{filter_field} >= TO_DATE('{start_str}', 'DD/MM/YYYY HH24:MI:SS') "
        f"AND {filter_field} <= TO_DATE('{end_str}', 'DD/MM/YYYY HH24:MI:SS')"
    )
    
    # Detectar si ya existe un filtro en el campo de fecha
    # Patrón para detectar filtros existentes en el campo
    pattern = re.compile(
        rf'{re.escape(filter_field)}\s*>=.*?TO_DATE.*?AND\s*{re.escape(filter_field)}\s*<=.*?(?:TO_DATE|SYSDATE)',
        re.IGNORECASE | re.DOTALL
    )
    
    if pattern.search(base_query):
        # Reemplazar filtro existente
        modified_query = pattern.sub(date_filter, base_query)
        print("  → Filtro temporal REEMPLAZADO")
        return modified_query
    
    # Si no existe, añadir el filtro
    query_upper = base_query.upper()
    
    if 'WHERE' in query_upper:
        # Existe WHERE, añadir con AND
        where_idx = query_upper.find('WHERE')
        after_where = base_query[where_idx + 5:].lstrip()
        
        modified_query = (
            base_query[:where_idx + 5] + 
            f"\n    {date_filter}\n    AND " + 
            after_where
        )
        print("  → Filtro temporal AÑADIDO con AND")
    else:
        # No existe WHERE, buscar FROM y añadir después
        from_match = re.search(r'\bFROM\b', base_query, re.IGNORECASE)
        if from_match:
            # Buscar final de FROM (antes de GROUP BY, ORDER BY, etc)
            end_markers = ['GROUP BY', 'ORDER BY', 'HAVING', 'UNION', 'MINUS', 'INTERSECT']
            from_end = len(base_query)
            
            for marker in end_markers:
                pos = query_upper.find(marker)
                if pos > 0 and pos < from_end:
                    from_end = pos
            
            modified_query = (
                base_query[:from_end].rstrip() +
                f"\nWHERE\n    {date_filter}\n" +
                base_query[from_end:]
            )
            print("  → Filtro temporal AÑADIDO con WHERE")
        else:
            # Fallback: añadir al final
            modified_query = base_query.rstrip() + f"\nWHERE {date_filter}"
            print("  → Filtro temporal AÑADIDO al final")
    
    return modified_query
# ==============================================================================
# LECTURA DE ORACLE
# ==============================================================================

def read_from_oracle(glueContext, table_name, sql_query, connectname, db_schema, primary_key_column):
    """Lee datos de Oracle con query filtrada"""
    
    pk_columns = [c.strip() for c in primary_key_column.split(',')]
    hashfield_value = "ROWID" if len(pk_columns) > 1 else pk_columns[0]
    
    conn_options = {
        "useConnectionProperties": "true",
        "connectionName": connectname,
        "dbtable": f"{db_schema}.{table_name}",
        "hashfield": hashfield_value,
        "hashexpression": hashfield_value,
        "sampleQuery": sql_query
    }
    
    read_start = datetime.now()
    
    df_dynamic = glueContext.create_dynamic_frame.from_options(
        connection_type="oracle",
        connection_options=conn_options
    )
    
    df = df_dynamic.toDF()
    
    read_end = datetime.now()
    read_duration = (read_end - read_start).total_seconds()
    
    return df, read_duration

# ==============================================================================
# ENRIQUECIMIENTO
# ==============================================================================

def enrich_dataframe(df, hash_columns=None):
    """Añade columnas de control y hash"""
    
    # Timestamp de ingesta
    df = df.withColumn("ingestion_ts", current_timestamp())
    df = df.withColumn("ingestion_date", to_date(col("ingestion_ts")))
    
    # Hash para deduplicación
    if hash_columns is None:
        hash_columns = [c for c in df.columns if c not in ['ingestion_ts', 'ingestion_date']]
    
    df = df.withColumn(
        "row_hash",
        sha2(concat_ws("||", *[col(c).cast("string") for c in hash_columns]), 256)
    )
    
    return df

# ==============================================================================
# ESCRITURA EN BRONZE
# ==============================================================================

def write_to_bronze(df, output_path, partition_keys):
    """Escribe a Bronze en ORC con particionamiento"""
    
    write_start = datetime.now()
    
    row_count = df.count()
    
    if row_count == 0:
        print("  ⚠️  Sin registros para escribir, saltando...")
        return 0, 0
    
    # Calcular particiones óptimas
    optimal_partitions = max(10, min(100, row_count // 2_000_000))
    
    print(f"  Registros: {row_count:,}")
    print(f"  Particiones escritura: {optimal_partitions}")
    
    # Repartir antes de escribir
    df_to_write = df.repartition(optimal_partitions, *partition_keys)
    
    df_to_write.write \
        .mode('append') \
        .format('orc') \
        .option('compression', 'snappy') \
        .option('orc.stripe.size', '268435456') \
        .option('orc.compress.size', '262144') \
        .partitionBy(*partition_keys) \
        .save(output_path)
    
    write_end = datetime.now()
    write_duration = (write_end - write_start).total_seconds()
    
    print(f"  Duración escritura: {write_duration:.2f}s")
    
    return write_duration, row_count

# ==============================================================================
# MAIN
# ==============================================================================

def main():
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'DATABASE',
        'TABLE_NAME',
        'DBSCHEMA',
        'CONNECT_NAME',
        'PRIMARY_KEY_COLUMN',
        'SQL_QUERY',
        'S3_OUTPUT_BUCKET',
        'LAZY_LOADING_HISTORIC',
        'INIT_DATE_HISTORIC',
        'END_DATE_HISTORIC',
        'FILTER_DATE_FIELD',
        'PARTITION_KEYS',
        'WINDOW_TYPE'  # monthly, daily
    ])
    
    # Inicializar
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    setup_spark_configs(spark)
    
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    print("="*100)
    print(f"JOB LAZY HISTORIC LOADER")
    print(f"Inicio: {datetime.now()}")
    print("="*100)
    print(f"Tabla: {args['DBSCHEMA']}.{args['TABLE_NAME']}")
    print(f"Fecha inicial: {args['INIT_DATE_HISTORIC']}")
    print(f"Fecha final: {args['END_DATE_HISTORIC']}")
    print(f"Tipo ventana: {args['WINDOW_TYPE']}")
    print("="*100)
    
    # Validar modo lazy
    if int(args.get('LAZY_LOADING_HISTORIC', 0)) != 1:
        print("❌ LAZY_LOADING_HISTORIC debe ser 1")
        return
    
    # Parsear partition keys
    partition_keys_str = args.get('PARTITION_KEYS', '').strip()
    partition_keys = [k.strip() for k in partition_keys_str.split(',')] if partition_keys_str else ['ingestion_date']
    
    # Generar ventanas
    window_type = args.get('WINDOW_TYPE', 'monthly').lower()
    
    if window_type == 'monthly':
        windows = generate_monthly_windows(args['INIT_DATE_HISTORIC'], args['END_DATE_HISTORIC'])
    elif window_type == 'daily':
        windows = generate_daily_windows(args['INIT_DATE_HISTORIC'], args['END_DATE_HISTORIC'])
    else:
        print(f"❌ WINDOW_TYPE inválido: {window_type}")
        return
    
    print(f"\n{'='*100}")
    print(f"VENTANAS GENERADAS: {len(windows)}")
    print(f"{'='*100}")
    for i, (start, end) in enumerate(windows, 1):
        print(f"  [{i:02d}] {start.strftime('%Y-%m-%d %H:%M:%S')} → {end.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*100}\n")
    
    # Ruta Bronze
    bronze_path = f"s3://{args['S3_OUTPUT_BUCKET']}/CLIENTES_TESTS_INC/{args['TABLE_NAME']}"
    
    # Procesar cada ventana
    total_rows = 0
    total_read_time = 0
    total_write_time = 0
    successful_windows = 0
    failed_windows = []
    
    for window_idx, (start_date, end_date) in enumerate(windows, 1):
        print(f"\n{'='*100}")
        print(f"VENTANA [{window_idx}/{len(windows)}]")
        print(f"Período: {start_date.strftime('%Y-%m-%d %H:%M:%S')} → {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*100}")
        
        try:
            # 1. CONSTRUIR QUERY FILTRADA
            print(f"\n[1/3] Preparando query...")
            filtered_query = inject_date_filter(
                args['SQL_QUERY'],
                args['FILTER_DATE_FIELD'],
                start_date,
                end_date
            )
            
            print(f"\nQuery generada:")
            print("-" * 80)
            print(filtered_query[:500] + ("..." if len(filtered_query) > 500 else ""))
            print("-" * 80)
            
            # 2. LEER DE ORACLE
            print(f"\n[2/3] Leyendo desde Oracle...")
            df, read_duration = read_from_oracle(
                glueContext,
                args['TABLE_NAME'],
                filtered_query,
                args['CONNECT_NAME'],
                args['DBSCHEMA'],
                args['PRIMARY_KEY_COLUMN']
            )
            
            print(f"  Lectura completada: {read_duration:.2f}s")
            total_read_time += read_duration
            
            # 3. ENRIQUECER Y ESCRIBIR
            print(f"\n[3/3] Enriqueciendo y escribiendo a Bronze...")
            df_enriched = enrich_dataframe(df)
            
            write_duration, row_count = write_to_bronze(
                df_enriched,
                bronze_path,
                partition_keys
            )
            
            total_write_time += write_duration
            total_rows += row_count
            successful_windows += 1
            
            print(f"\n✅ Ventana [{window_idx}] completada")
            print(f"   Registros: {row_count:,}")
            print(f"   Lectura: {read_duration:.2f}s | Escritura: {write_duration:.2f}s")
            
        except Exception as e:
            print(f"\n❌ ERROR en ventana [{window_idx}]")
            print(f"   {str(e)}")
            failed_windows.append({
                'window': window_idx,
                'start': start_date.isoformat(),
                'end': end_date.isoformat(),
                'error': str(e)
            })
            
            # Continuar con siguiente ventana
            continue
    
    # RESUMEN FINAL
    print(f"\n\n{'='*100}")
    print("RESUMEN FINAL")
    print(f"{'='*100}")
    print(f"Ventanas procesadas: {successful_windows}/{len(windows)}")
    print(f"Total registros: {total_rows:,}")
    print(f"Tiempo lectura total: {total_read_time:.2f}s")
    print(f"Tiempo escritura total: {total_write_time:.2f}s")
    print(f"Tiempo total: {(total_read_time + total_write_time):.2f}s")
    print(f"Promedio por ventana: {((total_read_time + total_write_time) / len(windows)):.2f}s")
    print(f"\nRuta Bronze: {bronze_path}")
    
    if failed_windows:
        print(f"\n⚠️  VENTANAS FALLIDAS: {len(failed_windows)}")
        for fail in failed_windows:
            print(f"  - Ventana {fail['window']}: {fail['start']} → {fail['end']}")
            print(f"    Error: {fail['error']}")
    
    print(f"\nFin Job: {datetime.now()}")
    print(f"{'='*100}\n")
    
    job.commit()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: {str(e)}")
        import traceback
        traceback.print_exc()
        raise"""
AWS Glue Job: Lazy Historic Loader - Single Job Sequential Processing
Procesa ventanas temporales secuencialmente en UN SOLO JOB
Cada ventana hace: Lectura Oracle → Escritura Bronze
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, sha2, concat_ws, to_date
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar
import re

# ==============================================================================
# CONFIGURACIÓN SPARK
# ==============================================================================

def setup_spark_configs(spark):
    """Configuraciones optimizadas para grandes volúmenes"""
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ==============================================================================
# GENERACIÓN DE VENTANAS TEMPORALES
# ==============================================================================

def parse_date(date_str):
    """Convierte DDMMYYYY a datetime"""
    return datetime.strptime(date_str, '%d%m%Y')

def get_last_day_of_month(year, month):
    """Retorna el último día del mes (maneja bisiestos)"""
    return calendar.monthrange(year, month)[1]

def generate_monthly_windows(init_date_str, end_date_str):
    """
    Genera ventanas mensuales completas
    
    Ejemplo:
        init: 01012025, end: 01032025
        Retorna:
        [
            (2025-01-01 00:00:00, 2025-01-31 23:59:59),
            (2025-02-01 00:00:00, 2025-02-28 23:59:59),
            (2025-03-01 00:00:00, 2025-03-31 23:59:59)
        ]
    """
    start_date = parse_date(init_date_str)
    
    if end_date_str.upper() == 'SYSDATE':
        end_date = datetime.now()
    else:
        end_date = parse_date(end_date_str)
    
    windows = []
    current_year = start_date.year
    current_month = start_date.month
    
    while True:
        # Primer día del mes
        window_start = datetime(current_year, current_month, 1, 0, 0, 0)
        
        # Último día del mes
        last_day = get_last_day_of_month(current_year, current_month)
        window_end = datetime(current_year, current_month, last_day, 23, 59, 59)
        
        # Si window_start ya pasó el end_date, terminamos
        if window_start > end_date:
            break
        
        # Si window_end excede end_date, ajustamos
        if window_end > end_date:
            window_end = end_date.replace(hour=23, minute=59, second=59)
        
        windows.append((window_start, window_end))
        
        # Avanzar al siguiente mes
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1
    
    return windows

def generate_daily_windows(init_date_str, end_date_str):
    """Genera ventanas diarias"""
    start_date = parse_date(init_date_str).replace(hour=0, minute=0, second=0)
    
    if end_date_str.upper() == 'SYSDATE':
        end_date = datetime.now()
    else:
        end_date = parse_date(end_date_str)
    
    windows = []
    current = start_date
    
    while current <= end_date:
        window_start = current.replace(hour=0, minute=0, second=0)
        window_end = current.replace(hour=23, minute=59, second=59)
        
        if window_end > end_date:
            window_end = end_date.replace(hour=23, minute=59, second=59)
        
        windows.append((window_start, window_end))
        current += timedelta(days=1)
    
    return windows

# ==============================================================================
# CONSTRUCCIÓN DINÁMICA DE QUERIES
# ==============================================================================
def clean_sql_script(sql_script: str, table_name: str, db_schema: str = None) -> str:
    """
    Limpia y prepara el script SQL para añadir condiciones adicionales
    """
    if not sql_script or not sql_script.strip():
        schema_prefix = f"{db_schema}." if db_schema else ""
        return f"SELECT * FROM {schema_prefix}{table_name} WHERE"
    
    cleaned_script = sql_script.strip().rstrip(';').strip()
    
    if "WHERE" in cleaned_script.upper():
        return cleaned_script + ' AND'
    else:
        return cleaned_script + ' WHERE'


def inject_date_filter(base_query, filter_field, start_date, end_date):
    """
    Inyecta o reemplaza filtro temporal en la query SQL
    
    Args:
        base_query: Query SQL base
        filter_field: Campo para filtrar (ej: T.TIME_STAMP)
        start_date: datetime inicio
        end_date: datetime fin
    
    Returns:
        Query SQL con filtro temporal inyectado/reemplazado
    """
    start_str = start_date.strftime('%d/%m/%Y %H:%M:%S')
    end_str = end_date.strftime('%d/%m/%Y %H:%M:%S')
    
    # Crear filtro temporal
    date_filter = (
        f"{filter_field} >= TO_DATE('{start_str}', 'DD/MM/YYYY HH24:MI:SS') "
        f"AND {filter_field} <= TO_DATE('{end_str}', 'DD/MM/YYYY HH24:MI:SS')"
    )
    
    # Detectar si ya existe un filtro en el campo de fecha
    # Patrón para detectar filtros existentes en el campo
    pattern = re.compile(
        rf'{re.escape(filter_field)}\s*>=.*?TO_DATE.*?AND\s*{re.escape(filter_field)}\s*<=.*?(?:TO_DATE|SYSDATE)',
        re.IGNORECASE | re.DOTALL
    )
    
    if pattern.search(base_query):
        # Reemplazar filtro existente
        modified_query = pattern.sub(date_filter, base_query)
        print("  → Filtro temporal REEMPLAZADO")
        return modified_query
    
    # Si no existe, añadir el filtro
    query_upper = base_query.upper()
    
    if 'WHERE' in query_upper:
        # Existe WHERE, añadir con AND
        where_idx = query_upper.find('WHERE')
        after_where = base_query[where_idx + 5:].lstrip()
        
        modified_query = (
            base_query[:where_idx + 5] + 
            f"\n    {date_filter}\n    AND " + 
            after_where
        )
        print("  → Filtro temporal AÑADIDO con AND")
    else:
        # No existe WHERE, buscar FROM y añadir después
        from_match = re.search(r'\bFROM\b', base_query, re.IGNORECASE)
        if from_match:
            # Buscar final de FROM (antes de GROUP BY, ORDER BY, etc)
            end_markers = ['GROUP BY', 'ORDER BY', 'HAVING', 'UNION', 'MINUS', 'INTERSECT']
            from_end = len(base_query)
            
            for marker in end_markers:
                pos = query_upper.find(marker)
                if pos > 0 and pos < from_end:
                    from_end = pos
            
            modified_query = (
                base_query[:from_end].rstrip() +
                f"\nWHERE\n    {date_filter}\n" +
                base_query[from_end:]
            )
            print("  → Filtro temporal AÑADIDO con WHERE")
        else:
            # Fallback: añadir al final
            modified_query = base_query.rstrip() + f"\nWHERE {date_filter}"
            print("  → Filtro temporal AÑADIDO al final")
    
    return modified_query
# ==============================================================================
# LECTURA DE ORACLE
# ==============================================================================

def read_from_oracle(glueContext, table_name, sql_query, connectname, db_schema, primary_key_column):
    """Lee datos de Oracle con query filtrada"""
    
    pk_columns = [c.strip() for c in primary_key_column.split(',')]
    hashfield_value = "ROWID" if len(pk_columns) > 1 else pk_columns[0]
    
    conn_options = {
        "useConnectionProperties": "true",
        "connectionName": connectname,
        "dbtable": f"{db_schema}.{table_name}",
        "hashfield": hashfield_value,
        "hashexpression": hashfield_value,
        "sampleQuery": sql_query
    }
    
    read_start = datetime.now()
    
    df_dynamic = glueContext.create_dynamic_frame.from_options(
        connection_type="oracle",
        connection_options=conn_options
    )
    
    df = df_dynamic.toDF()
    
    read_end = datetime.now()
    read_duration = (read_end - read_start).total_seconds()
    
    return df, read_duration

# ==============================================================================
# ENRIQUECIMIENTO
# ==============================================================================

def enrich_dataframe(df, hash_columns=None):
    """Añade columnas de control y hash"""
    
    # Timestamp de ingesta
    df = df.withColumn("ingestion_ts", current_timestamp())
    df = df.withColumn("ingestion_date", to_date(col("ingestion_ts")))
    
    # Hash para deduplicación
    if hash_columns is None:
        hash_columns = [c for c in df.columns if c not in ['ingestion_ts', 'ingestion_date']]
    
    df = df.withColumn(
        "row_hash",
        sha2(concat_ws("||", *[col(c).cast("string") for c in hash_columns]), 256)
    )
    
    return df

# ==============================================================================
# ESCRITURA EN BRONZE
# ==============================================================================

def write_to_bronze(df, output_path, partition_keys):
    """Escribe a Bronze en ORC con particionamiento"""
    
    write_start = datetime.now()
    
    row_count = df.count()
    
    if row_count == 0:
        print("  ⚠️  Sin registros para escribir, saltando...")
        return 0, 0
    
    # Calcular particiones óptimas
    optimal_partitions = max(10, min(100, row_count // 2_000_000))
    
    print(f"  Registros: {row_count:,}")
    print(f"  Particiones escritura: {optimal_partitions}")
    
    # Repartir antes de escribir
    df_to_write = df.repartition(optimal_partitions, *partition_keys)
    
    df_to_write.write \
        .mode('append') \
        .format('orc') \
        .option('compression', 'snappy') \
        .option('orc.stripe.size', '268435456') \
        .option('orc.compress.size', '262144') \
        .partitionBy(*partition_keys) \
        .save(output_path)
    
    write_end = datetime.now()
    write_duration = (write_end - write_start).total_seconds()
    
    print(f"  Duración escritura: {write_duration:.2f}s")
    
    return write_duration, row_count

# ==============================================================================
# MAIN
# ==============================================================================

def main():
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'DATABASE',
        'TABLE_NAME',
        'DBSCHEMA',
        'CONNECT_NAME',
        'PRIMARY_KEY_COLUMN',
        'SQL_QUERY',
        'S3_OUTPUT_BUCKET',
        'LAZY_LOADING_HISTORIC',
        'INIT_DATE_HISTORIC',
        'END_DATE_HISTORIC',
        'FILTER_DATE_FIELD',
        'PARTITION_KEYS',
        'WINDOW_TYPE'  # monthly, daily
    ])
    
    # Inicializar
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    setup_spark_configs(spark)
    
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    print("="*100)
    print(f"JOB LAZY HISTORIC LOADER")
    print(f"Inicio: {datetime.now()}")
    print("="*100)
    print(f"Tabla: {args['DBSCHEMA']}.{args['TABLE_NAME']}")
    print(f"Fecha inicial: {args['INIT_DATE_HISTORIC']}")
    print(f"Fecha final: {args['END_DATE_HISTORIC']}")
    print(f"Tipo ventana: {args['WINDOW_TYPE']}")
    print("="*100)
    
    # Validar modo lazy
    if int(args.get('LAZY_LOADING_HISTORIC', 0)) != 1:
        print("❌ LAZY_LOADING_HISTORIC debe ser 1")
        return
    
    # Parsear partition keys
    partition_keys_str = args.get('PARTITION_KEYS', '').strip()
    partition_keys = [k.strip() for k in partition_keys_str.split(',')] if partition_keys_str else ['ingestion_date']
    
    # Generar ventanas
    window_type = args.get('WINDOW_TYPE', 'monthly').lower()
    
    if window_type == 'monthly':
        windows = generate_monthly_windows(args['INIT_DATE_HISTORIC'], args['END_DATE_HISTORIC'])
    elif window_type == 'daily':
        windows = generate_daily_windows(args['INIT_DATE_HISTORIC'], args['END_DATE_HISTORIC'])
    else:
        print(f"❌ WINDOW_TYPE inválido: {window_type}")
        return
    
    print(f"\n{'='*100}")
    print(f"VENTANAS GENERADAS: {len(windows)}")
    print(f"{'='*100}")
    for i, (start, end) in enumerate(windows, 1):
        print(f"  [{i:02d}] {start.strftime('%Y-%m-%d %H:%M:%S')} → {end.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*100}\n")
    
    # Ruta Bronze
    bronze_path = f"s3://{args['S3_OUTPUT_BUCKET']}/CLIENTES_TESTS_INC/{args['TABLE_NAME']}"
    
    # Procesar cada ventana
    total_rows = 0
    total_read_time = 0
    total_write_time = 0
    successful_windows = 0
    failed_windows = []
    
    for window_idx, (start_date, end_date) in enumerate(windows, 1):
        print(f"\n{'='*100}")
        print(f"VENTANA [{window_idx}/{len(windows)}]")
        print(f"Período: {start_date.strftime('%Y-%m-%d %H:%M:%S')} → {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*100}")
        
        try:
            # 1. CONSTRUIR QUERY FILTRADA
            print(f"\n[1/3] Preparando query...")
            filtered_query = inject_date_filter(
                args['SQL_QUERY'],
                args['FILTER_DATE_FIELD'],
                start_date,
                end_date
            )
            
            print(f"\nQuery generada:")
            print("-" * 80)
            print(filtered_query[:500] + ("..." if len(filtered_query) > 500 else ""))
            print("-" * 80)
            
            # 2. LEER DE ORACLE
            clean_query = clean_sql_script(filtered_query, args['TABLE_NAME'], args['DBSCHEMA'])

            print(f"\n[2/3] Leyendo desde Oracle...")
            df, read_duration = read_from_oracle(
                glueContext,
                args['TABLE_NAME'],
                clean_query,
                args['CONNECT_NAME'],
                args['DBSCHEMA'],
                args['PRIMARY_KEY_COLUMN']
            )
            
            print(f"  Lectura completada: {read_duration:.2f}s")
            total_read_time += read_duration
            
            # 3. ENRIQUECER Y ESCRIBIR
            print(f"\n[3/3] Enriqueciendo y escribiendo a Bronze...")
            df_enriched = enrich_dataframe(df)
            
            write_duration, row_count = write_to_bronze(
                df_enriched,
                bronze_path,
                partition_keys
            )
            
            total_write_time += write_duration
            total_rows += row_count
            successful_windows += 1
            
            print(f"\n✅ Ventana [{window_idx}] completada")
            print(f"   Registros: {row_count:,}")
            print(f"   Lectura: {read_duration:.2f}s | Escritura: {write_duration:.2f}s")
            
        except Exception as e:
            print(f"\n❌ ERROR en ventana [{window_idx}]")
            print(f"   {str(e)}")
            failed_windows.append({
                'window': window_idx,
                'start': start_date.isoformat(),
                'end': end_date.isoformat(),
                'error': str(e)
            })
            
            # Continuar con siguiente ventana
            continue
    
    # RESUMEN FINAL
    print(f"\n\n{'='*100}")
    print("RESUMEN FINAL")
    print(f"{'='*100}")
    print(f"Ventanas procesadas: {successful_windows}/{len(windows)}")
    print(f"Total registros: {total_rows:,}")
    print(f"Tiempo lectura total: {total_read_time:.2f}s")
    print(f"Tiempo escritura total: {total_write_time:.2f}s")
    print(f"Tiempo total: {(total_read_time + total_write_time):.2f}s")
    print(f"Promedio por ventana: {((total_read_time + total_write_time) / len(windows)):.2f}s")
    print(f"\nRuta Bronze: {bronze_path}")
    
    if failed_windows:
        print(f"\n⚠️  VENTANAS FALLIDAS: {len(failed_windows)}")
        for fail in failed_windows:
            print(f"  - Ventana {fail['window']}: {fail['start']} → {fail['end']}")
            print(f"    Error: {fail['error']}")
    
    print(f"\nFin Job: {datetime.now()}")
    print(f"{'='*100}\n")
    
    job.commit()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: {str(e)}")
        import traceback
        traceback.print_exc()
        raise