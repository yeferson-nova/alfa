#!/usr/bin/env python3

"""
AWS Glue Job: Enhanced Silver Layer Processing with Dimension Mapping
Description: ETL processing for the Silver layer with dimension mapping capabilities
Author: Data Engineering Team
Date: 2025
"""

import sys
from datetime import datetime
import json
import pandas as pd
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, isnan, when, count
from awsglue.dynamicframe import DynamicFrame
from cardiff_staging_processor import CardiffStagingProcessor
from alfaprd_staging_processor import AlfprdStagingProcessor
from acsele_staging_processor import AcseleStagingProcessor
from servialfa_staging_processor import ServiAlfaStagingProcessor
from data_harmonization_manager import DataHarmonizationManager
from transformer_proccessor import DataCleanupProcessor
from dimension_handler import SCDDimensionHandler
from utils import Utils
from s3_handler import S3Handler
from cleanup import CleanUp


class SilverLayerProcessor:
    """Enhanced Silver layer processor with dimension mapping capabilities."""

    def __init__(self, spark, glue_context, job_name, bucket_silver):
        self.spark = spark
        self.glue_context = glue_context
        self.job_name = job_name
        self.bucket_silver = bucket_silver
        self.bucket_bronze = None 
        self.logger = self._setup_logging()
        self.utils = Utils()
        self.s3_handler = S3Handler(bucket_silver, glue_context)
        self.cleanup = CleanUp()
        self.silver_transformer = DataCleanupProcessor(spark, glue_context)
        self.dimension_handler  = SCDDimensionHandler(spark, glue_context,bucket_silver)
        self.harmonization_manager = DataHarmonizationManager()
       
        self.dimensions_cache = {}
       
        self.utils.configure_spark_optimizations(spark, "SILVER", "datalake-dev-clients-silver-job")
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        


    def _setup_logging(self):
        """Configure logging."""
        import logging
        logger = logging.getLogger(f"SilverLayer-{self.job_name}")
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def load_config(self, s3_config_path):
        """Load JSON configuration from S3."""
        try:
            self.logger.info(f"Loading configuration from: {s3_config_path}")
           
            try:
                df = self.spark.read.option("multiline", "true").json(s3_config_path)
                config_row = df.collect()[0]
                config = config_row.asDict()
               
                config = self._convert_spark_row_to_dict(config)
               
                self.logger.info("Loaded config using Spark JSON reader")
               
            except Exception as spark_error:
                self.logger.warning(f"Spark JSON reader failed: {spark_error}")
              
                config_df = self.spark.read.option("wholetext", "true").text(s3_config_path)
                config_json = config_df.collect()[0][0]
               
                config_json = self._clean_json_string(config_json)
                config = json.loads(config_json)
               
                self.logger.info("Loaded config using text reader")
           
            self._validate_config(config)

            if 'fact_configs' in config:
                for fact_name, fact_config in config['fact_configs'].items():
                    if 'additional_transforms' in fact_config:
                        self.logger.info(f"DEBUG - {fact_name} additional_transforms type: {type(fact_config['additional_transforms'])}")
                        for i, transform in enumerate(fact_config['additional_transforms']):
                            self.logger.info(f"DEBUG - Transform {i}: type={type(transform)}, value={transform}")
           
            self.logger.info("Configuration loaded and validated successfully")
            return config
           
        except Exception as e:
            self.logger.error(f"Error loading config: {str(e)}")
            self.logger.error(f"Please check the JSON file format at: {s3_config_path}")
            raise
   
    def _clean_json_string(self, json_string):
        """Clean JSON string from common formatting issues."""
        if json_string.startswith('\ufeff'):
            json_string = json_string[1:]
       
        json_string = json_string.strip()
       
        self.logger.info(f"Cleaned JSON (first 100 chars): {json_string[:100]}")
       
        return json_string
   
    def _convert_spark_row_to_dict(self, obj):
        """Convert Spark Row objects to regular dictionaries."""
        from pyspark.sql.types import Row
       
        if isinstance(obj, Row):
            return {k: self._convert_spark_row_to_dict(v) for k, v in obj.asDict().items()}
        elif hasattr(obj, 'asDict'):
            return {k: self._convert_spark_row_to_dict(v) for k, v in obj.asDict().items()}
        elif isinstance(obj, list):
            return [self._convert_spark_row_to_dict(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: self._convert_spark_row_to_dict(v) for k, v in obj.items()}
        else:
            return obj

    def _validate_config(self, config):
        """Validate configuration structure."""
        required_keys = ['dim', 'fact']
       
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required key in config: {key}")
       
        for dim in config.get('dim', []):
            if 'name' not in dim or 'status' not in dim:
                raise ValueError(f"Invalid dim structure: {dim}")
       
        for fact in config.get('fact', []):
            if 'name' not in fact or 'status' not in fact:
                raise ValueError(f"Invalid fact structure: {fact}")
                
        if 'fact_configs' in config:
            active_facts = [f['name'] for f in config['fact'] if f.get('status') == 1]
            missing_configs = [f for f in active_facts if f not in config['fact_configs']]
            if missing_configs:
                raise ValueError(f"Missing fact_configs for active facts: {missing_configs}")
       
        self.logger.info("Configuration validation passed")

    def get_active_items(self, config, item_type):
        """Get items with status = 1."""
        active_items = [item['name'] for item in config.get(item_type, []) if item.get('status') == 1]
        self.logger.info(f"Active {item_type}: {active_items}")
        return active_items

    def load_homologation_data(self, s3_homologation_path):
        """Load homologation data."""
        try:
            df_homologaciones = self.spark.read.csv(s3_homologation_path, header=True, inferSchema=True, sep=";")
            df_cod_tipo = df_homologaciones.filter(col("campo_referencia") == "cod_tipo_identificacion")
            df_otros = df_homologaciones.filter(col("campo_referencia") != "cod_tipo_identificacion")
            df_cod_tipo = self.cleanup.normalize_columns(df_cod_tipo, "valor_referencia")
            return df_cod_tipo.unionByName(df_otros)
        except Exception as e:
            self.logger.error(f"Error loading homologation data: {str(e)}")
            raise
    def apply_special_treatment(self, df, name):
        """
        Aplica tratamiento especial según la dimensión.
    
        Args:
            df: DataFrame de la dimensión
            name: Nombre de la dimensión (ej: "clientes_datos_basicos_stg")
        """
        try:
            self.logger.info(f"Applying special treatment for dimension: {name}")
    
            if name.lower() == 'clientes_datos_basicos_stg':
                self.logger.info(f"Aplicando split_name a {name}")
                df = self.silver_transformer.split_name(df)
                df = self.silver_transformer.populate_full_name(df)
                return df
    
            elif name.lower() == 'clientes_datos_sociodemograficos_stg':
                self.logger.info(f"Aplicando assign_colombia_if_missing a {name}")
                df = self.silver_transformer.assign_colombia_if_missing(df)
                return df
    
            else:
                self.logger.info(f"No special treatment defined for {name}")
                return df
    
        except Exception as e:
            self.logger.error(f"Error applying special treatment for {name}: {str(e)}")
            raise

    def load_dimension(self, dim_name):
        """Load dimension data with caching from the new Silver path."""
        if dim_name in self.dimensions_cache:
            self.logger.info(f"Using cached dimension: {dim_name}")
            return self.dimensions_cache[dim_name]
       
        try:
            dim_key = dim_name.replace('.xlsx', '').replace('dim_', '').upper()
           
            silver_path = f's3://{self.bucket_silver}/GENERAL/TRANSFORMATION/GENERAL_SILVER_{dim_key}_CLIENTES/'
           
            try:
                df = self.utils.read_full_parquet(self.spark, silver_path)
                self.logger.info(f"Loaded dimension {dim_name} from Silver GENERAL path: {silver_path}")
            except:
                # Fallback to Bronze
                bronze_dim_key = dim_name.replace('.xlsx', '').replace('.', '_')
                bronze_path = f's3://{self.bucket_bronze}/FILES/FILES/FILES_FILES_{bronze_dim_key}/'
                df = self.utils.read_full_parquet(self.spark, bronze_path)
                df = self.silver_transformer.clean_dims(df)
                self.logger.info(f"Loaded dimension {dim_name} from Bronze layer: {bronze_path}")
           
            # Cache the dimension
            self.dimensions_cache[dim_name] = df
            return df
           
        except Exception as e:
            self.logger.error(f"Error loading dimension {dim_name}: {str(e)}")
            raise

    
    def apply_dimension_mapping(self, df, mapping_config):
        """
        Apply dimension mapping using map_cod_by_nombre function.
     
        Args:
            df: Source DataFrame
            mapping_config: Dictionary with mapping configuration
                {
                    "dimension": "dim_ocupaciones.xlsx",
                    "nombre_origen": "ocupacion",
                    "nombre_referencia": "descripcion",
                    "cod_referencia": "cod_ocupacion",
                    "cod_origen": "cod_ocupacion_original",  # optional
                    "nuevo_cod": "id_ocupacion",
                    "valor_defecto": "999",
                    "columnas_adicionales_cruce": {  # optional - for hierarchical mappings
                        "cod_departamento": "departamento",
                        "otra_columna": "otra_referencia"
                    }
                }
        """
        try:
            dim_name = mapping_config['dimension']
            self.logger.info(f"Applying dimension mapping with {dim_name}")
           
            if mapping_config.get('columnas_adicionales_cruce'):
                self.logger.info(f"Using additional join columns: {mapping_config['columnas_adicionales_cruce']}")
         
            df_dimension = self.load_dimension(dim_name)
            df_dimension = df_dimension.filter(F.col("estado") =='A')
         
            df_mapped = self.silver_transformer.map_cod_by_nombre(
                df_origen=df,
                df_referencia=df_dimension,
                nombre_origen=mapping_config['nombre_origen'],
                nombre_referencia=mapping_config['nombre_referencia'],
                cod_referencia=mapping_config['cod_referencia'],
                cod_origen=mapping_config.get('cod_origen'),
                nuevo_cod=mapping_config.get('nuevo_cod', 'id'),
                valor_defecto=mapping_config.get('valor_defecto'),
                columnas_adicionales_cruce=mapping_config.get('columnas_adicionales_cruce')
            )
         
            self.logger.info(f"Successfully applied dimension mapping for {dim_name}")
            return df_mapped
         
        except Exception as e:
            self.logger.error(f"Error applying dimension mapping: {str(e)}")
            raise
     

    def process_staging_data(self, job_id, bucket_bronze,config):
        """Process staging data from all sources."""
        try:
            processors = {
                'cardiff': CardiffStagingProcessor(self.spark, self.glue_context, bucket_bronze, self.bucket_silver,config),
                'alfaprd': AlfprdStagingProcessor(self.spark, self.glue_context, bucket_bronze, self.bucket_silver,config),
                'acsele': AcseleStagingProcessor(self.spark, self.glue_context, bucket_bronze, self.bucket_silver,config),
                'servialfa': ServiAlfaStagingProcessor(self.spark, self.glue_context, bucket_bronze, self.bucket_silver,config)
            }
           
            results = {}
            for name, processor in processors.items():
                self.logger.info(f"Processing {name} data...")
                if name == 'cardiff':
                    results[name] = processor.cardiff_workflow_stg(job_id)
                elif name == 'alfaprd':
                    results[name] = processor.alfaprd_workflow_stg(job_id)
                elif name == 'acsele':
                    results[name] = processor.acsele_workflow_stg(job_id)
                elif name == 'servialfa':
                    results[name] = processor.servialfa_workflow_stg()    
           
            return results
        except Exception as e:
            self.logger.error(f"Error in staging processing: {str(e)}")
            raise

    def process_dimensions(self, bucket_bronze, bucket_silver,active_dims):
        """Process active dimensions dynamically and save to GENERAL/TRANSFORMATION path."""
        if not active_dims:
            self.logger.info("No active dimensions to process")
            return

        try:
            base_path = f's3://{bucket_bronze}/FILES/FILES/'
           
            for dim_name in active_dims:
                # Extract dimension key from filename (remove .xlsx extension)
                dim_key_bronze = dim_name.replace('.xlsx', '').replace('.', '_')
               
                # For Silver, remove dim_ prefix and use clean name
                dim_key_silver = dim_name.replace('.xlsx', '').replace('dim_', '').upper()
               
                # Build Bronze path dynamically
                dim_path = f"{base_path}FILES_FILES_{dim_key_bronze}/"
                #dim_path = self.silver_transformer._get_latest_dimension_path(base_path,dim_key_bronze)
                s3_path_silver = f's3://{self.bucket_silver}/GENERAL/TRANSFORMATION/{dim_name}/'
                self.logger.info(f"Processing dimension: {dim_name} from {dim_path}")
               
                try:
                    df = self.utils.read_full_parquet(self.spark, dim_path)
                    df_clean = self.silver_transformer.clean_dims(df)
                    df_clean = self.dimension_handler.process_dimension_with_scd(df_clean,dim_name)
                    
                    self.logger.info(f"Successfully processed {dim_name}")
                except Exception as e:
                    self.logger.warning(f"Could not process {dim_name}: {str(e)}")
                   
        except Exception as e:
            self.logger.error(f"Error processing dimensions: {str(e)}")
            raise


    def process_facts(self, config, active_facts, df_homologaciones, job_id):
        """Process active facts using configuration from JSON with dimension mapping support."""
        if not active_facts:
            self.logger.info("No active facts to process")
            return

        try:
            base_path = f's3://{self.bucket_silver}/CLIENTES/'
            fact_configs = config.get('fact_configs', {})
           
            for fact_name in active_facts:
                if fact_name not in fact_configs:
                    self.logger.warning(f"Unknown fact: {fact_name}. Available configs: {list(fact_configs.keys())}")
                    continue
                   
                config_item = fact_configs[fact_name]
                self.logger.info(f"Processing fact: {fact_name}")
               
                try:
                    full_path = f"{base_path}{config_item['path']}"
                    df = self.utils.read_full_parquet(self.spark, full_path)
                    df = self.silver_transformer.date_normalice(df)
                    df_clean = getattr(self.silver_transformer, config_item['clean_method'])(df)
                    df_clean = getattr(self.silver_transformer, config_item['select_method'])(df_clean)
                    
                    clean_method = config_item.get('clean_method')
                    if clean_method:
                        df_clean = getattr(self.silver_transformer, clean_method)(df)
                    else:
                        df_clean = df

                    select_method = config_item.get('select_method')
                    if select_method:
                        df_clean = getattr(self.silver_transformer, select_method)(df_clean)
                    print(config_item['harmonization_table'])
                    df_clean.persist()
                    df_clean.count()
            
                    df_harmonized = self.harmonization_manager.homologacion_i(
                        config_item['harmonization_table'],
                        df_homologaciones,
                        df_clean,
                        "homologacion",
                        "cod_fuente_fk"
                    )
                    
                    df_harmonized.persist()
                    df_harmonized.count()              
                    
                    for transform in config_item.get('additional_transforms', []):
                        try:
                            self.logger.info(f"Processing transform: {type(transform)} - {transform}")
                           
                            if isinstance(transform, str):
                                try:
                                    parsed_transform = json.loads(transform)
                                    self.logger.info(f"Parsed JSON transform: {parsed_transform}")
                                   
                                 
                                    if isinstance(parsed_transform, dict) and parsed_transform.get('type') == 'dimension_mapping':
                                        self.logger.info(f"Applying dimension mapping for {fact_name}")
                                        df_harmonized = self.apply_dimension_mapping(df_harmonized, parsed_transform['config'])
                                        
                                    else:
                                        self.logger.warning(f"Unknown parsed transformation type: {parsed_transform.get('type')}")
                                       
                                except json.JSONDecodeError:
                                    self.logger.info(f"Applying transformation method: {transform}")
                                    df_harmonized = getattr(self.silver_transformer, transform)(df_harmonized)
                                   
                            elif isinstance(transform, dict):
                                if transform.get('type') == 'dimension_mapping':
                                    self.logger.info(f"Applying dimension mapping for {fact_name}")
                                    df_harmonized = self.apply_dimension_mapping(df_harmonized, transform['config'])
                                    
                                else:
                                    self.logger.warning(f"Unknown transformation type: {transform.get('type')}")
                            else:
                                self.logger.error(f"Invalid transformation format - Type: {type(transform)}, Value: {transform}")
                               
                        except AttributeError as attr_error:
                            self.logger.error(f"Method {transform} not found in DataCleanupProcessor: {str(attr_error)}")
                            raise
                        except Exception as transform_error:
                            self.logger.error(f"Error applying transformation {transform}: {str(transform_error)}")
                            raise
                    df_harmonized = self.apply_special_treatment(df_harmonized,config_item['harmonization_table'])
                    self._save_to_silver(df_harmonized, f"{fact_name}_insert", "CLIENTES", config_item['schema'])
                    
                    self.logger.info(f"Successfully processed {fact_name}")
                   
                    df_harmonized.unpersist()
                   
                except Exception as e:
                    self.logger.warning(f"Could not process {fact_name}: {str(e)}")
                   
        except Exception as e:
            self.logger.error(f"Error processing facts: {str(e)}")
            raise

    def _save_to_silver(self, df, insert_name, db_name, schema):
        """Generic method to save data to Silver layer."""

        dynamic_frame = DynamicFrame.fromDF(df, self.glue_context, insert_name)
        prefix, s3_path = self.s3_handler.generate_s3_paths(
            db_name=db_name,
            schema=schema,
            table_name="SILVER"
        )
        self.s3_handler.delete_bucket_files(prefix)
        self.s3_handler.save_files_to_s3(
            df=dynamic_frame,
            s3_target_path=s3_path,
            format="parquet",
            mode="overwrite"
        )
       
    def set_bucket_bronze(self, bucket_bronze):
        """Set bucket bronze for dimension loading."""
        self.bucket_bronze = bucket_bronze

    def extract_excel_data(self, path, sheet): 
        """
        Function to extract data from excel files with better NaN handling
        """
        excel_file_path = path
        df_xl_op = pd.read_excel(excel_file_path, sheet_name=sheet)
        df_xl_op = df_xl_op.where(pd.notnull(df_xl_op), None) 
        for col in df_xl_op.columns:
            df_xl_op[col] = df_xl_op[col].apply(lambda x: str(x) if x is not None else None)
        df = self.spark.createDataFrame(df_xl_op)
        return df


    def homologation_data(self, df_homologaciones):
        """Load homologation data."""
        try:
            
            df_cod_tipo = df_homologaciones.filter(col("campo_referencia") == "cod_tipo_identificacion")
            df_otros = df_homologaciones.filter(col("campo_referencia") != "cod_tipo_identificacion")
            df_cod_tipo = self.cleanup.normalize_columns(df_cod_tipo, "valor_referencia")
            return df_cod_tipo.unionByName(df_otros)
        except Exception as e:
            self.logger.error(f"Error loading homologation data: {str(e)}")
            raise        
        
def main():
    """Main function with enhanced dimension mapping support."""
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'BUCKET_SILVER', 'BUCKET_BRONZE', 'JOB_PROCESS_ID',
        'S3_HOMOLOGATION_PATH', 'S3_STATUS_CONFIG_PATH',
        'LOAD_STG_SILVER', 'LOAD_ORIGIN_TABLE'
    ])

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    start_time = datetime.now()
    print(f"Job {args['JOB_NAME']} start at {start_time.strftime('%Y-%m-%d %H:%M:%S')} with PID: {args['JOB_PROCESS_ID']}")
    try:
        processor = SilverLayerProcessor(spark, glue_context, args['JOB_NAME'], args['BUCKET_SILVER'])
        processor.set_bucket_bronze(args['BUCKET_BRONZE'])
        processor.logger.info(f"Starting Enhanced Silver Layer job: {args['JOB_NAME']}")

        config = processor.load_config(args['S3_STATUS_CONFIG_PATH'])
        active_dims = processor.get_active_items(config, 'dim')
        active_facts = processor.get_active_items(config, 'fact')

        df_homologaciones = None
        if active_facts:
            path = args['S3_HOMOLOGATION_PATH']
            sheet = "homologaciones_clientes"
            df_homologaciones_dos = processor.extract_excel_data(path, sheet)
            df_homologaciones = df_homologaciones_dos.selectExpr(
                "tabla as tabla",
                "campo_referencia as campo_referencia",
                """
                trim(
                  translate(
                    lower(trim(valor_referencia)),
                    'ÁÀÄÂÃÅáàäâãåÉÈËÊéèëêÍÌÏÎíìïîÓÒÖÔÕóòöôõÚÙÜÛúùüûÑñÇç',
                    'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuNnCc'
                  )
                ) as valor_referencia
                """,
                "valor_homologacion",
                "campo_homologacion",
                """CASE         WHEN cod_fuente_fk IS NULL THEN NULL     
                WHEN LOWER(TRIM(cod_fuente_fk)) = 'nan' THEN NULL    
                WHEN cod_fuente_fk LIKE '%.0' THEN REGEXP_REPLACE(cod_fuente_fk, '\\.0$', '')       
                ELSE cod_fuente_fk     END as cod_fuente_fk
                """
            )
            df_homologaciones = processor.homologation_data(df_homologaciones)

        if args.get('LOAD_STG_SILVER', 'true').lower() == 'true':
            processor.process_staging_data(args['JOB_PROCESS_ID'], args['BUCKET_BRONZE'],config)

        processor.process_dimensions(args['BUCKET_BRONZE'],args['BUCKET_SILVER'], active_dims)

        processor.process_facts(config, active_facts, df_homologaciones, args['JOB_PROCESS_ID'])
        
        processor.logger.info("Job completed successfully!")
        processor.logger.info(f"Processed dims: {active_dims}, facts: {active_facts}")

    except Exception as e:
        print(f"Error in Enhanced Silver Layer job: {str(e)}")
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()
