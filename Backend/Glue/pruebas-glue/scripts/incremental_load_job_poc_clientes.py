import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from datetime import datetime
import boto3

## @params: [JOB_NAME]
"""
Params requeridos:
- JOB_NAME
- DATABASE: Nombre del database en Glue Catalog (ej: bronze)
- SQL_QUERY: Query SQL completa
- DBSCHEMA: Schema de Oracle (ej: ACSELE_ALFA)
- TABLE_NAME: Nombre de la tabla (ej: CONTEXTOPERATION)
- CONNECT_NAME: Nombre de la conexión JDBC en Glue
- PRIMARY_KEY_COLUMN: Columna PK
- S3_OUTPUT_BUCKET: Bucket para benchmark
"""

def setup_oracle_connection_options(table_name, sql_script, connectname, db_schema, primary_key_column):
    pk_columns = [col.strip() for col in primary_key_column.split(',')]
    
    if len(pk_columns) > 1:
        hashfield_value = "ROWID"
    else:
        hashfield_value = primary_key_column.strip()
    
    return {
        "useConnectionProperties": "true",
        "connectionName": connectname,
        "dbtable": f"{db_schema}.{table_name}",
        "hashfield": hashfield_value,
        "hashexpression": hashfield_value,
        "sampleQuery": sql_script
    }

def read_table_from_database(table_name, sql_script, connectname, db_schema, primary_key_column):
    print("="*80)
    print(f"LEYENDO TABLA: {db_schema}.{table_name}")
    print("="*80)
    
    conn_options = setup_oracle_connection_options(
        table_name, sql_script, connectname, db_schema, primary_key_column
    )
    
    read_start = datetime.now()
    print(f"Inicio lectura: {read_start}")
    
    df_dynamic = glueContext.create_dynamic_frame.from_options(
        connection_type="oracle",
        connection_options=conn_options
    )
    
    df = df_dynamic.toDF()
    # DIAGNÓSTICO
    print(f"=== REPARTICIONANDO ===")
    #print(f"Particiones: {df.rdd.getNumPartitions()}")
    # FORZAR coalesce
    df = df.repartition(46, args['PRIMARY_KEY_COLUMN'])
    #print(f"Nuevas particiones: {df.rdd.getNumPartitions()}")
    
    read_end = datetime.now()
    read_duration = (read_end - read_start).total_seconds()
    
    print(f"Fin lectura: {read_end}")
    print(f"Duración: {read_duration:.2f}s")
    
    return df, read_duration

# ==============================================================================
# MAIN
# ==============================================================================

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'DATABASE',
    'TABLE_NAME',
    'DBSCHEMA',
    'CONNECT_NAME',
    'PRIMARY_KEY_COLUMN',
    'SQL_QUERY',
    'S3_OUTPUT_BUCKET'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f"Inicio Job: {datetime.now()}")

try:
    # Leer datos
    df, read_duration = read_table_from_database(
        table_name=args['TABLE_NAME'],
        sql_script=args['SQL_QUERY'],
        connectname=args['CONNECT_NAME'],
        db_schema=args['DBSCHEMA'],
        primary_key_column=args['PRIMARY_KEY_COLUMN']
    )
    
    base_path = f"s3://{args['S3_OUTPUT_BUCKET']}/CLIENTES_TESTS_INC/benchmark"
    
    formats = [
        #('parquet', 'snappy'),
        #('parquet', 'gzip'),
        #('parquet', 'zstd'),
        ('orc', 'snappy'),
        #('orc', 'zlib'),
        #('avro', 'snappy')
    ]
    
    results = []
    s3 = boto3.resource('s3')
    
    for fmt, compression in formats:
        print(f"\n{'='*80}")
        print(f"Formato: {fmt.upper()} | Compresión: {compression}")
        print(f"{'='*80}")
        
        output_path = f"{base_path}/{fmt}_{compression}"
        
        # ESCRITURA
        write_start = datetime.now()
        print(f"Inicio escritura: {write_start}")
        #print(f"Iniciando escritura con {df.rdd.getNumPartitions()} particiones")
        
        if fmt == 'parquet':
            df.write.mode('overwrite').option('compression', compression).option('maxRecordsPerFile',5000000).parquet(output_path)
        elif fmt == 'orc':
            df.write.mode('overwrite').option('compression', compression).orc(output_path)
        elif fmt == 'avro':
            df.write.mode('overwrite').format('avro').option('compression', compression).option('maxRecordsPerFile',1000000).save(output_path)
        
        write_end = datetime.now()
        write_duration = (write_end - write_start).total_seconds()
        print(f"Fin escritura: {write_end}")
        print(f"Duración escritura: {write_duration:.2f}s")
        
        # TAMAÑO
        bucket = s3.Bucket(args['S3_OUTPUT_BUCKET'])
        prefix = f"benchmark/{fmt}_{compression}/"
        total_size = sum(obj.size for obj in bucket.objects.filter(Prefix=prefix))
        size_mb = total_size / (1024 * 1024)
        print(f"Tamaño: {size_mb:.2f} MB")
        
        # LECTURA COMPLETA
        read_full_start = datetime.now()
        print(f"Inicio lectura completa: {read_full_start}")
        
        if fmt == 'parquet':
            df_read = spark.read.parquet(output_path)
        elif fmt == 'orc':
            df_read = spark.read.orc(output_path)
        elif fmt == 'avro':
            df_read = spark.read.format('avro').load(output_path)
        
        df_read.take(1)  # Forzar lectura sin count
        read_full_end = datetime.now()
        read_full_duration = (read_full_end - read_full_start).total_seconds()
        print(f"Fin lectura completa: {read_full_end}")
        print(f"Duración lectura: {read_full_duration:.2f}s")
        
        # QUERY SELECTIVO
        query_start = datetime.now()
        print(f"Inicio query selectivo: {query_start}")
        
        df_read.select('ID', 'STATUS', 'TIME_STAMP').filter(col('STATUS') == 2).take(1)
        
        query_end = datetime.now()
        query_duration = (query_end - query_start).total_seconds()
        print(f"Fin query selectivo: {query_end}")
        print(f"Duración query: {query_duration:.2f}s")
        
        results.append({
            'format': fmt,
            'compression': compression,
            'write_start': write_start.isoformat(),
            'write_end': write_end.isoformat(),
            'write_duration_sec': round(write_duration, 2),
            'size_mb': round(size_mb, 2),
            'read_full_start': read_full_start.isoformat(),
            'read_full_end': read_full_end.isoformat(),
            'read_full_duration_sec': round(read_full_duration, 2),
            'query_start': query_start.isoformat(),
            'query_end': query_end.isoformat(),
            'query_duration_sec': round(query_duration, 2)
        })
    
    # RESUMEN
    print("\n" + "="*100)
    print("RESUMEN BENCHMARK")
    print("="*100)
    print(f"Lectura Oracle: {read_duration:.2f}s")
    print("-"*100)
    print(f"{'Formato':<10} {'Comp':<8} {'Write(s)':<10} {'Size(MB)':<10} {'Read(s)':<10} {'Query(s)':<10}")
    print("-"*100)
    
    for r in sorted(results, key=lambda x: x['write_duration_sec']):
        print(f"{r['format']:<10} {r['compression']:<8} {r['write_duration_sec']:<10} {r['size_mb']:<10} "
              f"{r['read_full_duration_sec']:<10} {r['query_duration_sec']:<10}")
    
    print("\n" + "="*100)

    import json
    results_json = json.dumps(results, indent=2)
    results_path = f"{base_path}/results.json"
    spark.sparkContext.parallelize([results_json], 1).saveAsTextFile(results_path)
    
    print(f"\nResultados en: {results_path}")
    print(f"Fin Job: {datetime.now()}")
    
except Exception as e:
    print(f"\nERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

finally:
    job.commit()