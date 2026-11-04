import sys 
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import sha2, concat_ws, current_date, current_timestamp, lit, col, row_number, max as spark_max
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.window import Window
from awsglue import DynamicFrame
import boto3
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark_conf = spark.conf
spark_conf.set("spark.sql.adaptive.enabled", "true")
spark_conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
#spark_conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "50")
#spark_conf.set("spark.sql.shuffle.partitions", "160")  # Para 4M registros
#spark_conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
#        logger.info("✅ Spark configuration optimized for migration", "IncrementalLoader")
BUCKET_BRONCE='alfa-datalake-dev-bronce'
S3_PATH_SMB_CARDIFF= f's3://{BUCKET_BRONCE}/CARDIFF/SMB_CARDIFF_CUSTOMER/'
df_smb_cardiff = spark.read.option("recursiveFileLookup", "true").parquet(S3_PATH_SMB_CARDIFF)
df_smb_cardiff.count()
from pyspark.sql.functions import col, lit, coalesce, when, concat_ws, regexp_replace
from pyspark.sql.functions import trim, length, isnan, isnull
def clean_phone(phone_col):
    """Limpia números telefónicos removiendo caracteres no numéricos"""
    return when(
        phone_col.isNotNull() & (length(trim(phone_col)) > 0),
        regexp_replace(trim(phone_col), "[^0-9]", "")
    ).otherwise(lit(None))

def coalesce_multiple(*cols):
    """Hace coalesce de múltiples columnas, filtrando nulls y strings vacíos"""
    clean_cols = []
    for c in cols:
        clean_col = when(
            c.isNotNull() & (trim(c) != "") & (trim(c) != "null"),
            trim(c)
        ).otherwise(lit(None))
        clean_cols.append(clean_col)
    return coalesce(*clean_cols)
# NORMALIZACIÓN CARDIFF - MANTENIENDO NOMBRES ORIGINALES
df_cardiff_normalized = df_smb_cardiff.select(
    # === CAMPOS ORIGINALES SIN CAMBIO ===
    col("CUSTOMER_NO"),
    col("ID_CO"),
    col("ID_TYPE"),
    col("ID_TYPE_NAME"),
    col("FIRST_NAME"),
    col("MIDDLE_NAME"),
    col("FIRST_SURNAME"),
    col("SECOND_SURNAME"),
    col("FULLNAME"),
    col("CREATION_DATE"),
    col("BIRTH_DATE"),
    col("LAST_EXECUTION_DATE"),
    col("GENDER"),
    col("GENDER_TYPE"),
    col("BIRTH_YEARS_OLD"),
    col("NATIONALITY"),
    col("MARITAL_STATUS"),
    col("CUSTOMER_TYPE"),
    col("BIRTH_STATE"),
    col("BIRTH_STATE_NAME"),
    col("BIRTH_CITY_CODE"),
    col("BIRTH_CITY_NAME"),
    col("BIRTH_COUNTRY_CODE"),
    col("BIRTH_COUNTRY_NAME"),
    col("ADDRESS_NAME"),
    col("ADDRESS_TYPE"),
    col("ADDRESS_STATE_CODE"),
    col("ADDRESS_STATE_NAME"),
    col("ADDRESS_CITY_CODE"),
    col("ADDRESS_CITY_NAME"),
    col("ADDRESS_NEIHBORHOOD"),
    col("ADDRESS_COUNTRY_CODE"),
    col("ADDRESS_COUNTRY_NAME"),
    col("ADDRESS_WORK"),
    col("ADDRESS_WORK_CITY_CODE"),
    col("ADDRESS_WORK_CITY_NAME"),
    col("OCCUPATION_CODE"),
    col("OCCUPATION_NAME"),
    col("SPOUSE_NAME"),
    col("SPOUSE_ID_TYPE_CODE"),
    col("SPOUSE_ID"),
    col("NUM_CHILDS"),
    col("MONTHLY_INCOME"),
    col("MONTHLY_EXPENSES"),
    col("ECON_CONDITION"),
    col("MARKET_SEGMENT"),
    col("MAIL"),
    col("PARTNER"),
    col("CREATION_CHANNEL"),
    col("ADULT"),
    col("MILITARY"),
    col("SHARE_INFORMATION"),
    col("PETS"),
    col("ID_PIMS"),
    col("ID_TMK"),
    
    # === CAMPOS NORMALIZADOS DE WIDE A LONG ===
    
    # TELÉFONO CELULAR - CONSOLIDADO (CELLPHONE1 + CELLPHONE2 → CELLPHONE)
    coalesce_multiple(
        clean_phone(col("CELLPHONE1")),
        clean_phone(col("CELLPHONE2"))
    ).alias("CELLPHONE"),
    
    # TELÉFONO FIJO 1 - CONSOLIDADO (PHONE_CODE_1 + PHONE_NUMBER_1 + PHONE_EXT_1)
    when(
        clean_phone(col("PHONE_NUMBER_1")).isNotNull(),
        concat_ws("-",
            coalesce(clean_phone(col("PHONE_CODE_1")), lit("")),
            clean_phone(col("PHONE_NUMBER_1")),
            coalesce(clean_phone(col("PHONE_EXT_1")), lit(""))
        )
    ).otherwise(lit(None)).alias("PHONE_1_FULL"),
    
    # TELÉFONO FIJO 2 - CONSOLIDADO (PHONE_CODE_2 + PHONE_NUMBER_2 + PHONE_EXT_2)
    when(
        clean_phone(col("PHONE_NUMBER_2")).isNotNull(),
        concat_ws("-",
            coalesce(clean_phone(col("PHONE_CODE_2")), lit("")),
            clean_phone(col("PHONE_NUMBER_2")),
            coalesce(clean_phone(col("PHONE_EXT_2")), lit(""))
        )
    ).otherwise(lit(None)).alias("PHONE_2_FULL"),
    
    # TELÉFONO FIJO 3 - CONSOLIDADO (PHONE_CODE_3 + PHONE_NUMBER_3 + PHONE_EXT_3)
    when(
        clean_phone(col("PHONE_NUMBER_3")).isNotNull(),
        concat_ws("-",
            coalesce(clean_phone(col("PHONE_CODE_3")), lit("")),
            clean_phone(col("PHONE_NUMBER_3")),
            coalesce(clean_phone(col("PHONE_EXT_3")), lit(""))
        )
    ).otherwise(lit(None)).alias("PHONE_3_FULL"),
    
    # === COMPONENTES TELEFÓNICOS INDIVIDUALES (CONSERVADOS) ===
    clean_phone(col("PHONE_CODE_1")).alias("PHONE_CODE_1"),
    clean_phone(col("PHONE_NUMBER_1")).alias("PHONE_NUMBER_1"),
    clean_phone(col("PHONE_EXT_1")).alias("PHONE_EXT_1"),
    clean_phone(col("PHONE_CODE_2")).alias("PHONE_CODE_2"),
    clean_phone(col("PHONE_NUMBER_2")).alias("PHONE_NUMBER_2"),
    clean_phone(col("PHONE_EXT_2")).alias("PHONE_EXT_2"),
    clean_phone(col("PHONE_CODE_3")).alias("PHONE_CODE_3"),
    clean_phone(col("PHONE_NUMBER_3")).alias("PHONE_NUMBER_3"),
    clean_phone(col("PHONE_EXT_3")).alias("PHONE_EXT_3"),
    
    # === CAMPOS ADICIONALES DE METADATOS ===
    lit("CARDIFF").alias("SOURCE_SYSTEM"),
    lit(3).alias("SOURCE_CODE")
)

# Verificar resultado
count_cardiff = df_cardiff_normalized.count()
print(f"✅ Cardiff normalizado: {count_cardiff:,} registros")
df_cardiff_datos_basicos = df_cardiff_normalized.select(
"ID_TYPE_NAME",
"ID_CO",
"FIRST_NAME",
"MIDDLE_NAME",
"FIRST_SURNAME",
"SECOND_SURNAME",
"FULLNAME",
"CREATION_DATE",
"CUSTOMER_TYPE"
)
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
                   Schema Manager - Manejo de Esquemas y Metadatos
================================================================================

Description:
   Maneja la creación de esquemas vacíos, lectura de esquemas existentes,
   y gestión de campos de control (ID autoincremental, metadatos de carga).

Author:         Development Team
Created:        2025-08-06
Version:        1.0.0
Environment:    Development

================================================================================
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import col, lit, current_date, current_timestamp, max as spark_max, coalesce, row_number
from pyspark.sql.window import Window
from awsglue import DynamicFrame
from datetime import datetime
import boto3
from logger_config import logger

class SchemaManager:
   """
   Maneja esquemas de tablas, IDs autoincrementales y campos de control.
   """
   
   def __init__(self, spark_session, glue_context, s3_handler):
       self._spark = spark_session
       self._glue_context = glue_context
       self.s3_handler = s3_handler
       self.s3_client = boto3.client('s3')
       
       logger.info("SchemaManager initialized", "SchemaManager")

   def create_customers_datos_basicos_schema(self) -> StructType:
       """
       Crear esquema para la tabla customers.datos_basicos.
       
       Returns:
           StructType: Esquema completo de la tabla
       """
       try:
           logger.info("Creating customers.datos_basicos schema", "SchemaManager")
           
           schema = StructType([
               # === ID AUTOINCREMENTAL ===
               StructField("customer_dbasicos_id", IntegerType(), False),
               
               # === CAMPOS DE NEGOCIO ===
               StructField("cod_tipo_identificacion", StringType(), False),
               StructField("numero_documento", StringType(), False),
               StructField("primer_nombre", StringType(), False),
               StructField("segundo_nombre", StringType(), True),  # Nullable
               StructField("nombre_completo", StringType(), False),
               StructField("fecha_expedicion_documento", DateType(), False),
               StructField("ciudad_expedicion_documento", StringType(), False),
               StructField("fecha_vinculacion", DateType(), True),  # Nullable
               StructField("cod_estado", IntegerType(), True),  # Nullable
               StructField("cod_tipo_persona", StringType(), False),
               StructField("cod_tipo_empresa", StringType(), False),
               StructField("tipo_empresa", StringType(), False),
               
               # === CAMPOS DE CONTROL ===
               StructField("cod_fuente_fk", IntegerType(), False),
               StructField("fecha_cargue", DateType(), False),
               StructField("job_cargue_id", StringType(), False)
           ])
           
           logger.success("Schema customers.datos_basicos created successfully", "SchemaManager")
           return schema
           
       except Exception as e:
           logger.error(f"Error creating schema: {str(e)}", "SchemaManager")
           raise

   def create_empty_table_with_schema(self, table_name: str, schema: StructType, s3_path: str) -> None:
       """
       Crear tabla vacía con esquema específico en S3.
       
       Args:
           table_name (str): Nombre de la tabla
           schema (StructType): Esquema de la tabla
           s3_path (str): Ruta S3 donde crear la tabla
       """
       try:
           logger.info(f"Creating empty table '{table_name}' at: {s3_path}", "SchemaManager")
           
           # Crear DataFrame vacío con el esquema
           df_empty = self._spark.createDataFrame([], schema)
           
           # Convertir a DynamicFrame
           df_dynamic = DynamicFrame.fromDF(df_empty, self._glue_context, f"empty_{table_name}")
           
           # Guardar esquema vacío en S3
           self.s3_handler.save_files_to_s3(
               df=df_dynamic,
               s3_target_path=s3_path,
               format="parquet",
               mode="overwrite"
           )
           
           logger.success(f"Empty table '{table_name}' created successfully", "SchemaManager")
           
       except Exception as e:
           logger.error(f"Error creating empty table: {str(e)}", "SchemaManager")
           raise

   def read_existing_schema(self, s3_path: str) -> DataFrame:
       """
       Leer esquema existente de una tabla en S3.
       
       Args:
           s3_path (str): Ruta S3 de la tabla existente
           
       Returns:
           DataFrame: DataFrame con el esquema existente (puede estar vacío)
       """
       try:
           logger.info(f"Reading existing schema from: {s3_path}", "SchemaManager")
           
           # Leer tabla existente
           df_existing = self._spark.read.option("recursiveFileLookup", "true").parquet(s3_path)
           
           record_count = df_existing.count()
           logger.info(f"Existing table loaded: {record_count:,} records", "SchemaManager")
           logger.info(f"Schema columns: {df_existing.columns}", "SchemaManager")
           
           return df_existing
           
       except Exception as e:
           logger.info(f"No existing table found (will create new): {str(e)}", "SchemaManager")
           return None
class AutoIncrementalManager:
   """
   Maneja IDs autoincrementales para tablas.
   """
   
   def __init__(self, spark_session):
       self._spark = spark_session
       logger.info("AutoIncrementalManager initialized", "AutoIncrementalManager")

   def get_next_id_range(self, df_existing: DataFrame, id_column: str, new_record_count: int) -> tuple:
       """
       Obtener el rango de IDs para nuevos registros.
       
       Args:
           df_existing (DataFrame): DataFrame existente (puede estar vacío)
           id_column (str): Nombre de la columna ID
           new_record_count (int): Cantidad de nuevos registros
           
       Returns:
           tuple: (starting_id, ending_id)
       """
       try:
           if df_existing is None or df_existing.count() == 0:
               # Primera carga
               starting_id = 1
               ending_id = new_record_count
               logger.info(f"First load - ID range: {starting_id} to {ending_id}", "AutoIncrementalManager")
           else:
               # Obtener el ID máximo existente
               max_id_row = df_existing.agg(spark_max(col(id_column))).collect()[0]
               max_existing_id = max_id_row[0] if max_id_row[0] is not None else 0
               
               starting_id = max_existing_id + 1
               ending_id = max_existing_id + new_record_count
               
               logger.info(f"Incremental load - Max existing ID: {max_existing_id}", "AutoIncrementalManager")
               logger.info(f"New ID range: {starting_id} to {ending_id}", "AutoIncrementalManager")
           
           return starting_id, ending_id
           
       except Exception as e:
           logger.error(f"Error calculating ID range: {str(e)}", "AutoIncrementalManager")
           raise

   def assign_incremental_ids(self, df_new: DataFrame, id_column: str, 
                            df_existing: DataFrame = None, order_by_columns: list = None) -> DataFrame:
       """
       Asignar IDs autoincrementales a nuevos registros.
       
       Args:
           df_new (DataFrame): DataFrame con nuevos registros
           id_column (str): Nombre de la columna ID
           df_existing (DataFrame, optional): DataFrame existente para calcular siguiente ID
           order_by_columns (list, optional): Columnas para ordenamiento determinístico
           
       Returns:
           DataFrame: DataFrame con IDs asignados
       """
       try:
           new_record_count = df_new.count()
           logger.info(f"Assigning IDs to {new_record_count:,} new records", "AutoIncrementalManager")
           
           if new_record_count == 0:
               logger.info("No records to process", "AutoIncrementalManager")
               return df_new
           
           # Calcular rango de IDs
           starting_id, ending_id = self.get_next_id_range(df_existing, id_column, new_record_count)
           
           # Definir ordenamiento para determinismo
           if order_by_columns:
               window_spec = Window.orderBy(*order_by_columns)
           else:
               window_spec = Window.orderBy(lit(1))  # Ordenamiento neutral
           
           # Asignar IDs secuenciales
           df_with_ids = df_new.withColumn("row_num", row_number().over(window_spec)) \
                               .withColumn(id_column, col("row_num") + (starting_id - 1)) \
                               .drop("row_num")
           
           logger.success(f"IDs assigned successfully: {starting_id} to {ending_id}", "AutoIncrementalManager")
           return df_with_ids
           
       except Exception as e:
           logger.error(f"Error assigning incremental IDs: {str(e)}", "AutoIncrementalManager")
           raise
class ControlFieldsManager:
   """
   Maneja campos de control: cod_fuente_fk, fecha_cargue, job_cargue_id.
   """
   
   def __init__(self, spark_session):
       self._spark = spark_session
       logger.info("ControlFieldsManager initialized", "ControlFieldsManager")

   def add_control_fields(self, df: DataFrame, cod_fuente: int, job_id: str) -> DataFrame:
       """
       Agregar campos de control a un DataFrame.
       
       Args:
           df (DataFrame): DataFrame de entrada
           cod_fuente (int): Código de fuente de datos
           job_id (str): ID del job de carga
           
       Returns:
           DataFrame: DataFrame con campos de control agregados
       """
       try:
           logger.info(f"Adding control fields - cod_fuente: {cod_fuente}, job_id: {job_id}", "ControlFieldsManager")
           
           df_with_control = df.withColumn("cod_fuente_fk", lit(cod_fuente)) \
                              .withColumn("fecha_cargue", current_date()) \
                              .withColumn("job_cargue_id", lit(job_id))
           
           logger.success("Control fields added successfully", "ControlFieldsManager")
           return df_with_control
           
       except Exception as e:
           logger.error(f"Error adding control fields: {str(e)}", "ControlFieldsManager")
           raise

   def get_source_codes(self) -> dict:
       """
       Obtener códigos de fuentes de datos predefinidos.
       
       Returns:
           dict: Mapping de fuentes a códigos
       """
       return {
           "ACSELE": 1,
           "ALFPRD": 2,
           "CARDIFF": 3,
           "EXTERNAL": 4,
           "MANUAL": 5
       }
class TableManager:
   """
   Maneja operaciones completas de tabla (esquema + IDs + control).
   """
   
   def __init__(self, spark_session, glue_context, s3_handler):
       self._spark = spark_session
       self._glue_context = glue_context
       self.s3_handler = s3_handler
       
       # Inicializar managers especializados
       self.schema_manager = SchemaManager(spark_session, glue_context, s3_handler)
       self.id_manager = AutoIncrementalManager(spark_session)
       self.control_manager = ControlFieldsManager(spark_session)
       
       logger.info("TableManager initialized", "TableManager")

   def create_customers_datos_basicos_table(self, s3_path: str) -> None:
       """
       Crear tabla customers.datos_basicos completa con esquema vacío.
       
       Args:
           s3_path (str): Ruta S3 donde crear la tabla
       """
       try:
           logger.info("Creating customers.datos_basicos table", "TableManager")
           
           # Crear esquema
           schema = self.schema_manager.create_customers_datos_basicos_schema()
           
           # Crear tabla vacía
           self.schema_manager.create_empty_table_with_schema(
               table_name="customers_datos_basicos",
               schema=schema,
               s3_path=s3_path
           )
           
           logger.success(f"Table customers.datos_basicos created at: {s3_path}", "TableManager")
           
       except Exception as e:
           logger.error(f"Error creating table: {str(e)}", "TableManager")
           raise

   def insert_data_to_customers_datos_basicos(self, df_new_data: DataFrame, s3_path: str, 
                                            cod_fuente: int, job_id: str,
                                            order_by_columns: list = None) -> dict:
       """
       Insertar datos a la tabla customers.datos_basicos con manejo completo.
       
       Args:
           df_new_data (DataFrame): Nuevos datos a insertar
           s3_path (str): Ruta S3 de la tabla
           cod_fuente (int): Código de fuente de datos
           job_id (str): ID del job de carga
           order_by_columns (list, optional): Columnas para ordenamiento de IDs
           
       Returns:
           dict: Resumen de la operación de inserción
       """
       try:
           logger.info("=" * 80, "TableManager")
           logger.info("INSERTING DATA TO CUSTOMERS.DATOS_BASICOS", "TableManager")
           logger.info("=" * 80, "TableManager")
           
           new_record_count = df_new_data.count()
           logger.info(f"New records to insert: {new_record_count:,}", "TableManager")
           
           # Step 1: Leer tabla existente
           df_existing = self.schema_manager.read_existing_schema(s3_path)
           existing_count = df_existing.count() if df_existing else 0
           
           # Step 2: Agregar campos de control
           df_with_control = self.control_manager.add_control_fields(
               df_new_data, cod_fuente, job_id
           )
           
           # Step 3: Asignar IDs autoincrementales
           df_with_ids = self.id_manager.assign_incremental_ids(
               df_with_control, 
               "customer_dbasicos_id", 
               df_existing,
               order_by_columns
           )
           
           # Step 4: Obtener rango de IDs asignados
           starting_id, ending_id = self.id_manager.get_next_id_range(
               df_existing, "customer_dbasicos_id", new_record_count
           )
           
           # Step 5: Guardar datos
           df_dynamic = DynamicFrame.fromDF(df_with_ids, self._glue_context, "customers_datos_basicos")
           
           self.s3_handler.save_files_to_s3(
               df=df_dynamic,
               s3_target_path=s3_path,
               format="parquet",
               mode="append"  # Siempre append para mantener IDs
           )
           
           # Step 6: Generar resumen
           summary = {
               "table_name": "customers.datos_basicos",
               "s3_path": s3_path,
               "existing_records": existing_count,
               "new_records": new_record_count,
               "total_records": existing_count + new_record_count,
               "id_range_start": starting_id,
               "id_range_end": ending_id,
               "cod_fuente": cod_fuente,
               "job_id": job_id,
               "insert_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
           }
           
           logger.info("=" * 80, "TableManager")
           logger.info("DATA INSERTION COMPLETED SUCCESSFULLY", "TableManager")
           logger.info("=" * 80, "TableManager")
           logger.info(f"Summary:", "TableManager")
           logger.info(f"  - Existing records: {summary['existing_records']:,}", "TableManager")
           logger.info(f"  - New records: {summary['new_records']:,}", "TableManager")
           logger.info(f"  - Total records: {summary['total_records']:,}", "TableManager")
           logger.info(f"  - ID range: {summary['id_range_start']} to {summary['id_range_end']}", "TableManager")
           logger.info(f"  - Source code: {summary['cod_fuente']}", "TableManager")
           logger.info(f"  - Job ID: {summary['job_id']}", "TableManager")
           logger.info("=" * 80, "TableManager")
           
           return summary
           
       except Exception as e:
           logger.error("=" * 80, "TableManager")
           logger.error("DATA INSERTION FAILED", "TableManager")
           logger.error("=" * 80, "TableManager")
           logger.error(f"Error details: {str(e)}", "TableManager")
           raise
from s3_handler import S3Handler
BUCKET_SILVER = 'alfa-datalake-dev-silver'
s3_handler = S3Handler(BUCKET_SILVER, glueContext)
table_manager = TableManager(spark, glueContext, s3_handler)
# create schema
s3_prefix, s3_target_path = s3_handler.generate_s3_paths('CLIENTES', 'DATOS_BASICOS', 'STG')
table_manager.create_customers_datos_basicos_table(s3_target_path)
# ================================================================================
# MAPEO SIMPLE CARDIFF A CUSTOMERS.DATOS_BASICOS - SIN TRANSFORMACIONES
# ================================================================================

print("📋 Mapeando Cardiff a esquema customers.datos_basicos...")

# Mapeo directo y simple - campos vacíos donde no hay datos
df_cardiff_datos_basicos_final = df_cardiff_normalized.select(
    # Campos disponibles en Cardiff
    col("ID_TYPE_NAME").alias("cod_tipo_identificacion"),
    col("ID_CO").alias("numero_documento"),
    col("FIRST_NAME").alias("primer_nombre"),
    col("MIDDLE_NAME").alias("segundo_nombre"),  # Puede ser null
    col("FULLNAME").alias("nombre_completo"),
    col("CREATION_DATE").alias("fecha_expedicion_documento"),
    col("CUSTOMER_TYPE").alias("cod_tipo_persona"),
    
    # Campos que no están en Cardiff - valores vacíos/null
    lit(None).cast("string").alias("ciudad_expedicion_documento"),
    lit(None).cast("date").alias("fecha_vinculacion"),
    lit(None).cast("int").alias("cod_estado"),
    lit(None).cast("string").alias("cod_tipo_empresa"),
    lit(None).cast("string").alias("tipo_empresa")
    
    # Los campos de control los agrega automáticamente TableManager
)

print(f"✅ Datos mapeados: {df_cardiff_datos_basicos_final.count():,} registros")
# Insertar usando TableManager
s3_table_path = s3_target_path
job_id = 'cardiff_debugg_notebook_jucarrillo_cardiff_06082025'
summary = table_manager.insert_data_to_customers_datos_basicos(
    df_new_data=df_cardiff_datos_basicos_final,
    s3_path=s3_table_path,
    cod_fuente=3,  # CARDIFF
    job_id=job_id
)

print(f"✅ Inserción completada: {summary['new_records']:,} registros")
s3_target_path
s3_handler = S3Handler(BUCKET_SILVER, glueContext)
s3_prefix, s3_target_path = s3_handler.generate_s3_paths('CLIENTES', 'DATOS_BASICOS', 'STG')
response = s3_handler.save_files_to_s3(df_dynamic, s3_target_path, format="parquet", mode="overwrite")
df_cardiff_datos_basicos.printSchema()
df_cardiff_normalized.printSchema()
bronze_path='s3://alfa-datalake-dev-bronce/CARDIFF/SMB_CARDIFF_CUSTOMER/'
partition_keys = []

df_existing = spark.read.option("recursiveFileLookup", "true").parquet(bronze_path)
total_records = df_existing.count()
max_id = df_existing.filter("INCREMENTAL_ID is not null").agg(spark_max("INCREMENTAL_ID")).collect()[0][0]
max_id
total_records
total_records
df_existing.show(3)
from s3_handler import S3Handler
s3_handler = S3Handler(BUCKET_BRONCE,glueContext)
bkys = 'ADDRESS_CITY_CODE,ADDRESS_CITY_NAME,ADDRESS_COUNTRY_CODE,ADDRESS_COUNTRY_NAME,ADDRESS_NAME,ADDRESS_NEIHBORHOOD,ADDRESS_STATE_CODE,ADDRESS_STATE_NAME,BIRTH_CITY_CODE,BIRTH_CITY_NAME,BIRTH_COUNTRY_CODE,BIRTH_COUNTRY_NAME,BIRTH_DATE,BIRTH_STATE,BIRTH_STATE_NAME,BIRTH_YEARS_OLD,CELLPHONE1,CELLPHONE2,CREATION_CHANNEL,CREATION_DATE,CUSTOMER_TYPE,ECON_CONDITION,FIRST_NAME,FIRST_SURNAME,FULLNAME,GENDER,GENDER_TYPE,ID_PIMS,ID_TMK,ID_TYPE,MAIL,MIDDLE_NAME,NATIONALITY,OCCUPATION_NAME,PHONE_CODE_1,PHONE_CODE_2,PHONE_NUMBER_1,PHONE_NUMBER_2,SECOND_SURNAME'
business_keys = bkys.strip(',')
business_keys
# Step 2: CHECK if migration is needed
has_incremental_id = "INCREMENTAL_ID" in df_existing.columns

if not has_incremental_id:
    # Step 3: MIGRATE all legacy data (add INCREMENTAL_ID)
    print("=" * 60)
    print("MIGRATING LEGACY DATA TO INCREMENTAL")
    print(f"Legacy records to migrate: {total_records:,}")
    print("=" * 60)

    # Get business keys for consistent ordering 
    last_id = 0
    starting_id = last_id + 1
    # STEP 1: FORCE MATERIALIZATION of existing data BEFORE any processing
    print("Step 1: Forcing materialization of existing data")
    df_existing.cache()  # Cache in memory
    print("✅ Existing data materialized")

    # STEP 2: MIGRATE - Add INCREMENTAL_ID to all legacy records
    print("Using high-performance INCREMENTAL_ID assignment (no global sorting)")

    # Optimizar particionamiento para paralelización
    optimal_partitions = max(200, total_records // 20000)  # ~20K registros por partición
    df_optimized = df_existing.repartition(optimal_partitions)

    df_migrated = df_optimized.withColumn(
        "temp_id", monotonically_increasing_id()
    ).withColumn(
        "INCREMENTAL_ID", col("temp_id") + starting_id
    ).drop("temp_id")

    print("✅ High-performance ID assignment completed")

    # STEP 3: FORCE MATERIALIZATION of migrated data
    print("Step 3: Forcing materialization of migrated data")
    df_migrated.cache()  # Cache migrated data in memory
    migrated_count = df_migrated.count()  # Force full processing and materialization
    print(f"✅ Migrated data materialized with {migrated_count} records")

    # STEP 4: UPDATE control table
    max_id = starting_id + total_records - 1

    # STEP 5: NOW SAFE TO DELETE - all data is materialized in memory
    s3_prefix = bronze_path.replace(f's3://{s3_handler.bucket_target}/', '')
    print(f"Step 5: Safe to delete - all data is in memory. Deleting: {s3_prefix}")
    s3_handler.delete_bucket_files(s3_prefix)
    print("✅ Legacy files deleted - migrated data is safe in memory")

    # STEP 6: Clean up cache before writing (free memory)
    df_existing.unpersist()  # Free original data cache
    print("✅ Original data cache cleared")
    # Step 6: WRITE migrated data (overwrite with migrated version)
    
df_dynamic = DynamicFrame.fromDF(df_migrated, glueContext, "migrated_data")
df_dynamic.count()
s3_handler.save_files_to_s3(
        df=df_dynamic,
        s3_target_path=bronze_path,
        format="parquet", 
        mode="overwrite",
        partition_keys=[]
)
print("=" * 60)
print("LEGACY MIGRATION COMPLETED SUCCESSFULLY")
print(f"✅ Migrated {total_records:,} records")
print(f"✅ INCREMENTAL_ID range: {starting_id} to {max_id}")
print("✅ All legacy data preserved with INCREMENTAL_ID")
print("=" * 60)
s3_handler = S3Handler(BUCKET_SILVER, glueContext)
s3_prefix, s3_target_path = s3_handler.generate_s3_paths('CLIENTES', 'DATOS_BASICOS', 'STG')
response = s3_handler.save_files_to_s3(df_dynamic, s3_target_path, format="parquet", mode="overwrite")
 
BUCKET_SILVER = 'alfa-datalake-dev-silver'
BUCKET_BRONZE = 'alfa-datalake-dev-bronce'
dyf = glueContext.create_dynamic_frame.from_catalog(database='database_name', table_name='table_name')
dyf.printSchema()
df = dyf.toDF()
df.show()
import matplotlib.pyplot as plt

# Set X-axis and Y-axis values
x = [5, 2, 8, 4, 9]
y = [10, 4, 8, 5, 2]
  
# Create a bar chart 
plt.bar(x, y)
  
# Show the plot
s3output = glueContext.getSink(
  path="s3://bucket_name/folder_name",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase="demo", catalogTableName="populations"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(DyF)
job.commit()