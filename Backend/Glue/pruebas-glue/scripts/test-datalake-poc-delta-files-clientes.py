## ----- GLUE LIBRARIES ------------------ ###
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from delta.tables import DeltaTable

## ----- PYTHON EXTERNAL LIBRARIES ------------------ ###
import sys
from datetime import datetime

## ----- CUSTOM LIBRARIES ------------------ ###
from logger_config import logger
from utils import Utils
from s3_handler import S3Handler

## @params: 
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BUCKET_ASSETS',
    'BUCKET_TARGET',
    'DB_NAME',
    'DB_SCHEMA',
    'CONNECTION_NAME',
    'S3_PATH_PROCESS_TABLE_DEFINITIONS',
    'S3_PATH_SAVE_JSON_TABLE_DEFINITIONS',
    'S3_PATH_FILES',
    'CARDIFF_COLUMNS',
    'JOB_PROCESS_ID',
    'S3_PATH_STAGING_TABLES'
])

## ----- INIT JOB ------------------ ###
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================================
# CONFIGURACIÓN
# ============================================================================
BUCKET = "alfa-datalake-dev-silver"
CSV_PATH = f"s3://{BUCKET}/config/MOCK_DATA.csv"
DELTA_PATH = f"s3://{BUCKET}/test/personas_delta/"

# Inicializar S3Handler
s3_handler = S3Handler(bucket_target=BUCKET, glue_context=glueContext)
folder_name = 'POLIZAS_PRODUCTO'
prefix_bucket = "ALFAPROD"
sufix_bucket = "STG"
schemas = ["PRODUCTOS","POLIZAS","CLIENTES"]

for index, schema in enumerate(schemas):
    logger.info(f"==========GENERATE SCHEMA #{index}===================")
    _, s3_path = s3_handler.generate_s3_paths(prefix_bucket,schema,sufix_bucket)
    s3_target_path = s3_path.replace("/ALFAPROD/", "/POLIZAS_PRODUCTO/ALFAPROD/")
    logger.info(f"Path Generated: {s3_target_path}", "test-datalake-poc-delta-files-clientes")




# # ============================================================================
# # 1. LEER CSV
# # ============================================================================
# print("===> 1. Leyendo CSV...")
# df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)
# print(f"Registros leídos: {df.count()}")
# df.show(5)
# df.printSchema()

# # ============================================================================
# # 1. CARGA INICIAL - INSERT con OVERWRITE
# # ============================================================================
# print("\n===> 1. CARGA INICIAL")
# df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)
# print(f"Registros leídos: {df.count()}")

# df_dynamic = DynamicFrame.fromDF(df, glueContext, "df_dynamic")
# s3_handler.save_files_to_s3(
#     df=df_dynamic,
#     s3_target_path=DELTA_PATH,
#     format="delta",
#     mode="overwrite",
#     partition_keys=["gender"]
# )
# print("✅ Carga inicial completada")

# # ============================================================================
# # 2. APPEND - Agregar nuevos registros
# # ============================================================================
# print("\n===> 2. APPEND - Agregar nuevos registros")

# # Leer schema de la tabla existente para mantener tipos de datos
# year = datetime.now().strftime("%Y")
# month = datetime.now().strftime("%m")
# day = datetime.now().strftime("%d")
# full_path = f"{DELTA_PATH}{year}/{month}/{day}"
# df_existing = spark.read.format("delta").load(full_path)

# # Nuevos registros
# nuevos = [(9999, "Carlos", "Test", "carlos@test.com", "Male", "192.168.1.1")]
# df_nuevos = spark.createDataFrame(nuevos, schema=df_existing.schema)

# s3_handler.save_files_to_s3(
#     df=df_nuevos,
#     s3_target_path=DELTA_PATH,
#     format="delta",
#     mode="append",
#     partition_keys=["gender"]
# )
# print("✅ Append completado")

# # ============================================================================
# # 3. UPDATE - Actualizar registros específicos
# # ============================================================================
# print("\n===> 3. UPDATE - Actualizar email de un usuario")

# s3_handler.update_delta_data(
#     s3_target_path=DELTA_PATH,
#     condition="id = 9999",
#     update_values={"email": "carlos.actualizado@test.com"}
# )
# print("✅ Update completado")

# # ============================================================================
# # 4. MERGE (UPSERT) - Actualizar existentes + insertar nuevos
# # ============================================================================
# print("\n===> 4. MERGE - Actualizar id=1 e insertar id=10000")

# datos_merge = [
#     (1, "Juan", "Actualizado", "juan.nuevo@email.com", "Male", "10.0.0.1"),
#     (10000, "Nuevo", "Usuario", "nuevo@email.com", "Female", "10.0.0.2")
# ]
# df_merge = spark.createDataFrame(datos_merge, schema=df_existing.schema)

# s3_handler.merge_delta_data(
#     df=df_merge,
#     s3_target_path=DELTA_PATH,
#     merge_keys=["id"],
#     partition_keys=["gender"]
# )
# print("✅ Merge completado")

# # ============================================================================
# # 5. LEER Y VERIFICAR
# # ============================================================================
# print("\n===> 5. Verificar datos actuales")
# df_final = spark.read.format("delta").load(full_path)
# print(f"Total registros: {df_final.count()}")
# df_final.filter("id IN (1, 9999, 10000)").show()

# # ============================================================================
# # 6. HISTORIAL - Ver todas las operaciones realizadas
# # ============================================================================
# print("\n===> 6. Consultar historial de cambios")
# history = s3_handler.get_delta_history(DELTA_PATH, limit=10)
# history.select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

# # ============================================================================
# # 7. DELETE - Eliminar registros de prueba
# # ============================================================================
# print("\n===> 7. Eliminar registro de prueba")
# s3_handler.delete_delta_data(
#     s3_target_path=DELTA_PATH,
#     condition="id = 9999"
# )

# # Verificar eliminación
# df_after_delete = spark.read.format("delta").load(full_path)
# print(f"Total registros después de delete: {df_after_delete.count()}")
# print("✅ Delete completado")

# # ============================================================================
# # 8. OPTIMIZE - Compactar archivos pequeños
# # ============================================================================
# print("\n===> 8. Optimizar tabla (compactar archivos)")
# s3_handler.optimize_delta_table(DELTA_PATH)
# print("✅ Optimize completado")

# # ============================================================================
# # 9. VACUUM - Limpiar versiones antiguas (OPCIONAL - ejecutar periódicamente)
# # ============================================================================
# print("\n===> 9. Vacuum (limpiar archivos antiguos)")
# # CUIDADO: Solo ejecutar cuando estés seguro que no necesitas versiones antiguas
# # s3_handler.vacuum_delta_table(DELTA_PATH, retention_hours=168)  # 7 días
# print("⚠️ Vacuum omitido en esta ejecución")

# # ============================================================================
# # RESUMEN FINAL
# # ============================================================================
# print("\n" + "="*70)
# print("RESUMEN DE OPERACIONES EJECUTADAS:")
# print("="*70)
# print("✅ INSERT (overwrite) - Carga inicial de datos")
# print("✅ APPEND - Agregar nuevos registros sin borrar existentes")
# print("✅ UPDATE - Actualizar campos específicos de registros")
# print("✅ MERGE (UPSERT) - Actualizar existentes + insertar nuevos")
# print("✅ SELECT - Leer y verificar datos")
# print("✅ HISTORY - Consultar historial de cambios (auditoría)")
# print("✅ DELETE - Eliminar registros por condición")
# print("✅ OPTIMIZE - Compactar archivos para mejor rendimiento")
# print("="*70)

job.commit()