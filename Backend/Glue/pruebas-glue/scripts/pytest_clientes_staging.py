# ==========================================================
# CELL 1 – Initialize AWS Glue and Spark contexts
# ==========================================================
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import Row, functions as F
from pyspark.sql.types import StructType, StructField, StringType

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

print("✅ Spark and Glue contexts initialized successfully")
 
# ==========================================================
# CELL 2 – Mock dependencies (logger, s3_handler, utils, schemas)
# ==========================================================
import sys, types

def make_module(name, attrs: dict):
    module = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(module, k, v)
    sys.modules[name] = module
    return module

# --- logger_config ---
class DummyLogger:
    def info(self, msg, src=""): print(f"[INFO] {src}: {msg}")
    def success(self, msg, src=""): print(f"[SUCCESS] {src}: {msg}")
    def error(self, msg, src=""): print(f"[ERROR] {src}: {msg}")
    def _get_timestamp(self):
        import datetime
        return datetime.datetime.now().isoformat()
make_module("logger_config", {"logger": DummyLogger()})

# --- s3_handler ---
class DummyS3Handler:
    def __init__(self, bucket, glue_context): self.bucket = bucket
    def generate_s3_paths(self, prefix, schema, suffix):
        return None, f"s3://dummy/{prefix}/{schema}/{suffix}"
    def save_files_to_s3(self, **kwargs):
        print(f"[MOCK SAVE] → {kwargs.get('s3_target_path')}")
make_module("s3_handler", {"S3Handler": DummyS3Handler})

# --- utils ---
class DummyUtils:
    def configure_spark_optimizations(self, spark, system, module): pass
    def read_full_parquet(self, spark, s3_path):
        schema = StructType([
            StructField("apellido", StringType()),
            StructField("nombre", StringType()),
            StructField("ciudad", StringType()),
            StructField("departamento", StringType()),
            StructField("dirección", StringType()),
            StructField("dirección_de_correo_electrónico", StringType()),
            StructField("email_alternativo_1", StringType()),
            StructField("email_alternativo_2", StringType()),
            StructField("fecha_de_nacimiento", StringType()),
            StructField("número_id", StringType()),
            StructField("tipo_id", StringType()),
            StructField("sexo", StringType()),
            StructField("teléfono_móvil", StringType()),
            StructField("fecha_de_última_actualización", StringType())
        ])
        data = [(
            "Pérez", "Juan", "Bogotá", "Cundinamarca", "Cra 10 # 20-30",
            "juan@example.com", "", "alt@example.com", "1990-01-01",
            "1010101010", "CC", "M", "3001234567", "2025-01-01"
        )]
        return spark.createDataFrame(data, schema)
make_module("utils", {"Utils": DummyUtils})

# --- schemas ---
class DummySchema:
    def __init__(self, spark, glue_context, s3_handler): pass
    def insert_data(self, df_new_data, s3_path, cod_fuente, job_id):
        print(f"[MOCK LOAD] insert_data → {s3_path}")
        return {"status": "ok"}

make_module("datos_basicos_schema", {"DatosBasicosSchema": DummySchema})
make_module("datos_contactabilidad_schema", {"DatosUbicacionContactabilidadSchema": DummySchema})
make_module("datos_sociodemograficos_schema", {"DatosSociodemograficosSchema": DummySchema})

print("✅ Dummy dependencies registered successfully")
 
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
                   Cardiff Staging Data Processor
================================================================================
 
Description:
   Processes and normalizes Cardiff data from wide format to normalized structure.
   Single responsibility: Cardiff staging transformation.
 
Author:         Maria Antonia Bermudez
Created:        2025-08-06
Version:        1.0.0
Environment:    Development
 
================================================================================
"""
 
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, coalesce, when, concat_ws, regexp_replace
from pyspark.sql.functions import trim, length, isnan, isnull,concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
#------- CUSTOM LIBRARIES ---------------#
from datos_basicos_schema import DatosBasicosSchema
from datos_contactabilidad_schema import DatosUbicacionContactabilidadSchema
from datos_sociodemograficos_schema import DatosSociodemograficosSchema
from s3_handler import S3Handler
from awsglue import DynamicFrame
from utils import Utils
 
from logger_config import logger
 
class ServiAlfaStagingProcessor:
   """
   Processes Cardiff staging data, normalizing from wide to long format.
   Single responsibility: Cardiff transformation and normalization.
   
   Author: Juan Alejandro Carrillo Jaimes
   """
   def __init__(self, spark_session, glue_context, bucket_bronze: str, bucket_silver: str, config : str):
       """
       Initialize Cardiff staging processor with required components.
       """
       self._spark = spark_session
       self._glue_context = glue_context
       self.bucket_bronze = bucket_bronze
       self.bucket_silver = bucket_silver
       self.config = config 
       # Initialize S3Handler for Silver
       self.s3_handler = S3Handler(bucket_silver, glue_context)

       # Initialize Utils for load parquet files
       self.utils = Utils()
 
       # Table definitions to save
       self.prefix_bucket = "CLIENTES"
       self.sufix_bucket = "STG"
       self.schemas = ["SERVIALFA"]
        
       self.utils.configure_spark_optimizations(self._spark, "CARDIFF", "CardiffStagingProcessor")
       logger.info("CardiffStagingProcessor initialized", "CardiffStagingProcessor")
   
   def _get_servialfa_table_paths(self) -> dict:
        """
        Get specific S3 paths for CARDIFF bronze.
        
        Returns:
            dict: Dictionary with specific S3 paths for each table
        """
        #servialfa_response = self.utils._read_json_from_s3(s3_target_path=self.json_path)
        """table_paths = {
            "servialfa": f"s3://{self.bucket_bronze}/SERVIALFA/FILES/"
        }
        """
        table_paths = self.config ['stg_paths'] ['servialfa']
        return table_paths
   
   def load_servialfa_data(self) -> DataFrame:
        """
        Load all necessary CARDIFF tables from Bronze layer.
        Replaces the original cargar_datos_desde_catalogo function.
        
        Returns:
            DataFrame: DataFrame with loaded Cardiff data
        """
        try:
            s3_full_path = self._get_servialfa_table_paths()
            return self.utils.read_full_parquet(self._spark, s3_full_path.get("servialfa"))
 
        except Exception as e:
            logger.error(f"Error loading servialfa tables: {str(e)}", "ServialfaStagingProcessor")
            raise
 
   def clean_phone(self, phone_col):
       """
       Clean phone numbers by removing non-numeric characters.
       
       Args:
           phone_col: Phone column to clean
           
       Returns:
           Column: Clean column with only numbers
       """
       return when(
           phone_col.isNotNull() & (length(trim(phone_col)) > 0),
           regexp_replace(trim(phone_col), "[^0-9]", "")
       ).otherwise(lit(None))
 
   def coalesce_multiple(self, *cols):
       """
       Coalesce multiple columns, filtering nulls and empty strings.
       
       Args:
           *cols: Columns to process with coalesce
           
       Returns:
           Column: First non-null/empty column
       """
       clean_cols = []
       for c in cols:
           clean_col = when(
               c.isNotNull() & (trim(c) != "") & (trim(c) != "null"),
               trim(c)
           ).otherwise(lit(None))
           clean_cols.append(clean_col)
       return coalesce(*clean_cols)
 
   def normalize_servialda_data(self, df_raw: DataFrame) -> DataFrame:
       """
       Normalize Cardiff data from wide to normalized structure.
       
       Args:
           df_raw (DataFrame): Raw Cardiff DataFrame from Bronze
           
       Returns:
           DataFrame: Normalized Cardiff DataFrame with consolidated fields
       """
       try:
           logger.info("Starting Cardiff data normalization (wide → long)", "CardiffStagingProcessor")
           
           input_count = df_raw.count()
           logger.info(f"Input Cardiff records: {input_count:,}", "CardiffStagingProcessor")
           
           # CARDIFF NORMALIZATION - MAINTAINING ORIGINAL NAMES
           df_normalized = df_raw.select(
               # ORIGINAL FIELDS WITHOUT CHANGE
               col("apellido"),
               col("nombre"),
               col("ciudad"),
               col("departamento"),
               col("dirección"),
               self.coalesce_multiple(
                   col("dirección_de_correo_electrónico"),
                   col("email_alternativo_1"),
                   col("email_alternativo_2")
               ).alias("correo_electronico"),
               col("fecha_de_nacimiento"),
               col("número_id"),
               col("tipo_id"),
               col("sexo"),
               col("teléfono_móvil"),
               col("fecha_de_última_actualización"),
               # ADDITIONAL METADATA FIELDS
               lit("SERVIALFA").alias("SOURCE_SYSTEM"),
               lit(4).alias("SOURCE_CODE")
           )
           
           output_count = df_normalized.count()
           logger.success(f"Servialfa normalization completed: {output_count:,} records", "ServialfaStagingProcessor")
           
           # Log applied transformations
           logger.info("Transformations applied:", "ServialfaStagingProcessor")
           logger.info("dirección_de_correo_electrónico + email_alternativo_1 + email_alternativo_2 → correo_electronico", "ServialfaStagingProcessor")
           logger.info("Smart coalesce with null/empty string filtering", "servialfaStagingProcessor")
           
           return df_normalized
           
       except Exception as e:
           logger.error(f"Error normalizing Servialfa data: {str(e)}", "ServialfaStagingProcessor")
           raise
 
   def get_normalization_summary(self, df_original: DataFrame, df_normalized: DataFrame) -> dict:
       """
       Generate Cardiff normalization summary.
       
       Args:
           df_original (DataFrame): Original DataFrame
           df_normalized (DataFrame): Normalized DataFrame
           
       Returns:
           dict: Normalization summary
       """
       try:
           original_columns = len(df_original.columns)
           normalized_columns = len(df_normalized.columns)
           
           # Original phone fields vs consolidated
           original_phone_fields = ["CELLPHONE1", "CELLPHONE2", "PHONE_CODE_1", "PHONE_NUMBER_1", "PHONE_EXT_1", 
                                  "PHONE_CODE_2", "PHONE_NUMBER_2", "PHONE_EXT_2", "PHONE_CODE_3", "PHONE_NUMBER_3", "PHONE_EXT_3"]
           consolidated_phone_fields = ["CELLPHONE", "PHONE_1_FULL", "PHONE_2_FULL", "PHONE_3_FULL"]
           
           summary = {
               "input_records": df_original.count(),
               "output_records": df_normalized.count(),
               "original_columns": original_columns,
               "normalized_columns": normalized_columns,
               "original_phone_fields": len(original_phone_fields),
               "consolidated_phone_fields": len(consolidated_phone_fields),
               "phone_consolidation_ratio": f"{len(original_phone_fields)}→{len(consolidated_phone_fields)}",
               "source_system": "CARDIFF",
               "source_code": 3,
               "normalization_timestamp": logger._get_timestamp()
           }
           
           logger.info(f"Cardiff normalization summary: {summary}", "CardiffStagingProcessor")
           return summary
           
       except Exception as e:
           logger.error(f"Error generating normalization summary: {str(e)}", "CardiffStagingProcessor")
           raise
   
   def servialfa_workflow_stg(self, job_id:str):
       """
       Execute complete Cardiff staging workflow.
       """
       try:
           logger.info("=" * 80, "ServialfaStagingProcessor")
           logger.info("STARTING SERVIALFA PROCESSING WORKFLOW", "ServialfaStagingProcessor")
           logger.info("=" * 80, "ServialfaStagingProcessor")

           # 01. Load DataFrame
           logger.info("STEP 1: Loading Servialfa from Bronze", "ServialfaStagingProcessor")
           df_servialfa_bronce = self.load_servialfa_data()
           # 02. Normalize data
           logger.info("STEP 2: Processing Dataframes separately", "ServialfaStagingProcessor")
           df_servialfa_normalized = self.normalize_servialda_data(df_servialfa_bronce)
           
           _,path_servialfa = self.s3_handler.generate_s3_paths(self.prefix_bucket,self.schemas[0],self.sufix_bucket)
           
           # 05. Load Data
           logger.info("STEP 5: Start load data Process", "ServialfaStagingProcessor")

           df_dynamic = DynamicFrame.fromDF(df_servialfa_normalized, self._glue_context, "servialfa_insert")
            
           self.s3_handler.save_files_to_s3(
               df=df_dynamic,
               s3_target_path=path_servialfa,
               format="parquet",
               mode="append"
           )
           """summary = datos_basicos_schema.insert_data(
                    df_new_data=df_cardiff_datos_basicos_stg,
                    s3_path=path_dbasicos,
                    cod_fuente=3,
                    job_id=job_id
           )
           summary = datos_sociodemo_schema.insert_data(
                    df_new_data=df_cardiff_datos_sociodemograficos_stg,
                    s3_path=path_dsociodemo,
                    cod_fuente=3,
                    job_id=job_id
           )
           summary = datos_contactabilidad_schema.insert_data(
                    df_new_data=df_cardiff_datos_contacto_stg,
                    s3_path=path_dcontacto,
                    cod_fuente=3,
                    job_id=job_id
           )"""
           logger.success("STEP 6: Table loaded successfully", "ServialfaStagingProcessor")
           logger.info("=" * 80, "ServialfaStagingProcessor")
           logger.info("SERVIALFA PROCESSING COMPLETED SUCCESSFULLY", "ServialfaStagingProcessor")
           logger.info("=" * 80, "ServialfaStagingProcessor")

       except Exception as e:
            logger.error("=" * 80, "ServialfaStagingProcessor")
            logger.error("SERVIALFA PROCESSING FAILED", "ServialfaStagingProcessor")
            logger.error("=" * 80, "ServialfaStagingProcessor")
            logger.error(f"Error details: {str(e)}", "ServialfaStagingProcessor")
            raise
# ==========================================================
# CELL 4 – Initialize ServiAlfaStagingProcessor (Cardiff)
# ==========================================================
config = {"stg_paths": {"servialfa": {"servialfa": "s3://dummy-bronze/SERVIALFA/FILES/"}}}

processor = ServiAlfaStagingProcessor(
    spark_session=spark,
    glue_context=glue_context,
    bucket_bronze="mock-bronze",
    bucket_silver="mock-silver",
    config=config
)
print("✅ ServiAlfaStagingProcessor (Cardiff) initialized successfully")
 
# ==========================================================
# CELL 5 – Create mock raw DataFrame
# ==========================================================
schema = StructType([
    StructField("apellido", StringType()),
    StructField("nombre", StringType()),
    StructField("ciudad", StringType()),
    StructField("departamento", StringType()),
    StructField("dirección", StringType()),
    StructField("dirección_de_correo_electrónico", StringType()),
    StructField("email_alternativo_1", StringType()),
    StructField("email_alternativo_2", StringType()),
    StructField("fecha_de_nacimiento", StringType()),
    StructField("número_id", StringType()),
    StructField("tipo_id", StringType()),
    StructField("sexo", StringType()),
    StructField("teléfono_móvil", StringType()),
    StructField("fecha_de_última_actualización", StringType())
])
data = [(
    "Pérez", "Juan", "Bogotá", "Cundinamarca", "Cra 10 # 20-30",
    "juan@example.com", "", "alt@example.com", "1990-01-01", 
    "1010101010", "CC", "M", "3001234567", "2025-01-01"
)]
df_cardiff = spark.createDataFrame(data, schema)
print(f"✅ Mock Cardiff DataFrame created with {df_cardiff.count()} record(s)")
 
# ==========================================================
# CELL 6 – Unit tests for Cardiff Processor
# ==========================================================
def test_instance():
    assert processor is not None
    print("✅ test_instance passed")

def test_clean_phone():
    df = spark.createDataFrame([("300-123-4567",), ("",)], ["phone"])
    df = df.withColumn("cleaned", processor.clean_phone(F.col("phone")))
    assert "cleaned" in df.columns
    print("✅ test_clean_phone passed")

def test_coalesce_multiple():
    df = spark.createDataFrame([("", "value")], ["a", "b"])
    df = df.withColumn("merged", processor.coalesce_multiple(F.col("a"), F.col("b")))
    assert df.collect()[0]["merged"] == "value"
    print("✅ test_coalesce_multiple passed")

def test_normalize():
    df_norm = processor.normalize_servialda_data(df_cardiff)
    assert "correo_electronico" in df_norm.columns
    print("✅ test_normalize passed")

def test_summary():
    df_norm = processor.normalize_servialda_data(df_cardiff)
    summary = processor.get_normalization_summary(df_cardiff, df_norm)
    assert "input_records" in summary
    print("✅ test_summary passed")

def run_all_tests():
    print("=== Running Cardiff Processor Tests ===")
    test_instance()
    test_clean_phone()
    test_coalesce_multiple()
    test_normalize()
    test_summary()
    print("=== ✅ All Cardiff tests executed successfully ===")

run_all_tests()
 
# ==========================================================
# CELL 7 – Execute Cardiff Workflow (mock mode)
# ==========================================================
class DummyJob:
    def init(self, *args, **kwargs): print("[MOCK JOB] init called")
    def commit(self): print("[MOCK JOB] commit called")

job = DummyJob()
print("✅ Dummy Glue Job created")

try:
    processor.servialfa_workflow_stg(job_id="TEST_CARDIFF_001")
    print("✅ Full Cardiff workflow executed (mock mode)")
except Exception as e:
    print(f"⚠️ Workflow execution error: {e}")
 
job.commit()