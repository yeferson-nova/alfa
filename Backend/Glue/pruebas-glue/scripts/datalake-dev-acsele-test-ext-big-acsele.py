import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import time

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("=== Job iniciado ===")

# Configuración
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.default.parallelism", "10")  # Forzar 10 particiones
spark.conf.set("spark.sql.shuffle.partitions", "10")

CONNECTION_NAME = "datalake-dev-acsele-pre-connection"
DB_SCHEMA = "ACSELE_ALFA"
TABLE_NAME = "COBERTURA"
OUTPUT_PATH = "s3://alfa-datalake-dev-bronce/TEST_BIG_TABLES/COBERTURA_PARALELO/"

# ============================================
# CONFIGURACIÓN DE 10 PARTICIONES PARALELAS
# ============================================
NUM_WORKERS = 10
RECORDS_PER_PARTITION = 10000  # 1M por partición = 10M total para prueba

print(f"=== CONFIGURACIÓN PARALELA ===")
print(f"Workers disponibles: {NUM_WORKERS}")
print(f"Particiones a crear: {NUM_WORKERS}")
print(f"Registros por partición: {RECORDS_PER_PARTITION:,}")
print(f"Total a extraer: {NUM_WORKERS * RECORDS_PER_PARTITION:,}")
print(f"=====================")

# ============================================
# CREAR 10 QUERIES (UNA POR WORKER)
# ============================================
print("\nCreando 10 queries para ejecución paralela...")

predicates = []

for i in range(NUM_WORKERS):
    start_row = (i * RECORDS_PER_PARTITION) + 1
    end_row = (i + 1) * RECORDS_PER_PARTITION

    # Query con rango de ROWNUM para cada partición
    query = f"""
    SELECT * FROM (
        SELECT COB.*, ROWNUM as rn
        FROM (
            SELECT COB.*
            FROM {DB_SCHEMA}.CONTEXTOPERATION CO
            INNER JOIN {DB_SCHEMA}.POLICYDCO PDCO ON PDCO.OPERATIONPK = CO.ID
            INNER JOIN {DB_SCHEMA}.COVERAGEDCO CDCO ON CDCO.OPERATIONPK = PDCO.OPERATIONPK
            INNER JOIN {DB_SCHEMA}.COBERTURA COB ON CDCO.DCOID = COB.PK
            WHERE CO.STATUS = 2
              AND CO.TIME_STAMP BETWEEN TO_DATE('01/05/2025 00:00:00','DD/MM/YYYY HH24:MI:SS')
                                    AND SYSDATE
        ) COB
        WHERE ROWNUM <= {end_row}
    )
    WHERE rn > {start_row - 1}
    """

    predicates.append(query.strip())
    print(f"  Worker {i+1}: Filas {start_row:,} a {end_row:,}")

print(f"\n✓ {len(predicates)} queries creadas para ejecución paralela")

# ============================================
# EXTRACCIÓN PARALELA CON LOS 10 WORKERS
# ============================================
print("\n" + "="*60)
print("INICIANDO EXTRACCIÓN PARALELA")
print("="*60)

start_time = time.time()

# Lista para almacenar DataFrames
dataframes = []

# Procesar cada partición (Spark distribuirá entre los 10 workers)
for idx, predicate in enumerate(predicates):
    print(f"\nPreparando partición {idx+1}/{NUM_WORKERS}...")

    connection_options = {
        "useConnectionProperties": "true",
        "connectionName": CONNECTION_NAME,
        "dbtable": f"{DB_SCHEMA}.{TABLE_NAME}",
        "sampleQuery": predicate,
        "fetchsize": "50000",
        "oracle.net.keepAlive": "true",
        "oracle.net.CONNECT_TIMEOUT": "50000",
        "oracle.jdbc.ReadTimeout": "50000"
    }

    try:
        df_dynamic = glueContext.create_dynamic_frame.from_options(
            connection_type="oracle",
            connection_options=connection_options,
            transformation_ctx=f"extract_partition_{idx}"
        )

        df_part = df_dynamic.toDF()

        # Eliminar columna 'rn' auxiliar
        if 'rn' in df_part.columns:
            df_part = df_part.drop('rn')

        dataframes.append(df_part)
        print(f"✓ Partición {idx+1} preparada")

    except Exception as e:
        print(f"✗ Error en partición {idx+1}: {str(e)}")
        # Continuar con las demás
        continue

extraction_time = time.time() - start_time
print(f"\n✓ Todas las particiones preparadas en {extraction_time:.2f} segundos")

# ============================================
# UNIR TODAS LAS PARTICIONES
# ============================================
if len(dataframes) == 0:
    print("⚠️ No se extrajeron datos")
    job.commit()
    sys.exit(0)

print(f"\nUniendo {len(dataframes)} particiones...")
union_start = time.time()

df_final = dataframes[0]
for df_part in dataframes[1:]:
    df_final = df_final.union(df_part)

union_time = time.time() - union_start
print(f"✓ Unión completada en {union_time:.2f} segundos")

# ============================================
# VALIDACIÓN
# ============================================
print(f"✓ Total de registros: {total_records:,}")

print("\n=== Primeras 5 filas ===")
df_final.show(5, truncate=False)

# ============================================
# GUARDAR EN S3
# ============================================
print("\nGuardando en S3...")
write_start = time.time()

df_final.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("compression", "snappy") \
    .save(OUTPUT_PATH)

write_time = time.time() - write_start
total_time = time.time() - start_time

print(f"✓ Guardado en: {OUTPUT_PATH}")

# ============================================
# RESUMEN FINAL
# ============================================
print("\n" + "="*60)
print("RESUMEN DE PERFORMANCE PARALELA")
print("="*60)
print(f"Workers utilizados: {NUM_WORKERS}")
print(f"Particiones procesadas: {len(dataframes)}")
print(f"Registros totales: {total_records:,}")
print(f"Registros por partición (promedio): {total_records / len(dataframes):,.0f}")

if total_records > 0 and total_time > 0:
    records_per_sec = total_records / total_time
    print(f"\nVelocidad: {records_per_sec:,.0f} registros/segundo")

    # Proyección para 86M
    estimated_seconds = 86000000 / records_per_sec
    estimated_minutes = estimated_seconds / 60
    print(f"\n📊 PROYECCIÓN PARA 86M REGISTROS:")
    print(f"   Tiempo estimado: {estimated_minutes:.1f} minutos ({estimated_minutes/60:.1f} horas)")

print("="*60)

job.commit()
print("\n=== Job completado ===")