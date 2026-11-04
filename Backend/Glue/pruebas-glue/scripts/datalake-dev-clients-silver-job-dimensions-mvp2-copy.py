#!/usr/bin/env python3

"""
AWS Glue Job: Silver Layer Dimensions Loader
Description: Dedicated job for loading dimension tables only
Author: Data Engineering Team
Date: 2025
"""

import sys
from datetime import datetime
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from utils import Utils
from dimension_handler import SCDDimensionHandler
from s3_handler import S3Handler
from mpv2_transformer_proccessor import DataCleanupProcessor

class DimensionsLoader:
    """Dedicated processor for loading dimension tables."""

    def __init__(self, spark, glue_context, job_name, bucket_silver):
        self.spark = spark
        self.glue_context = glue_context
        self.job_name = job_name
        self.bucket_silver = bucket_silver
        self.bucket_bronze = None 
        self.logger = self._setup_logging()
        self.utils = Utils()
        self.s3_handler = S3Handler(bucket_silver, glue_context)
        self.dimension_handler  = SCDDimensionHandler(spark, glue_context,bucket_silver)
        self.silver_transformer = DataCleanupProcessor(spark, glue_context)
       
        # Configuración de Spark para dimensiones
        self.utils.configure_spark_optimizations(spark, "SILVER", "datalake-dev-clients-silver-dimensions")
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    def _setup_logging(self):
        """Configure logging."""
        import logging
        logger = logging.getLogger(f"DimensionsLoader-{self.job_name}")
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def set_bucket_bronze(self, bucket_bronze):
        """Set bucket bronze for dimension loading."""
        self.bucket_bronze = bucket_bronze

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
            self.logger.info("Configuration loaded and validated successfully")
            return config
           
        except Exception as e:
            self.logger.error(f"Error loading config: {str(e)}")
            raise
   
    def _clean_json_string(self, json_string):
        """Clean JSON string from common formatting issues."""
        if json_string.startswith('\ufeff'):
            json_string = json_string[1:]
        json_string = json_string.strip()
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
        if 'dim' not in config:
            raise ValueError("Missing required key in config: dim")
       
        for dim in config.get('dim', []):
            if 'name' not in dim or 'status' not in dim:
                raise ValueError(f"Invalid dim structure: {dim}")
       
        self.logger.info("Configuration validation passed for dimensions")

    def get_active_dimensions(self, config):
        """Get active dimensions (status = 1)."""
        active_dims = [dim['name'] for dim in config.get('dim', []) if dim.get('status') == 1]
        self.logger.info(f"Active dimensions: {active_dims}")
        return active_dims

    def load_dimension(self, dim_name):
        """Load a single dimension from Bronze to Silver."""
        try:
            self.logger.info(f"Loading dimension: {dim_name}")
            
            # Construir ruta Bronze
            dim_name = dim_name.replace('.xlsx', '')
            s3_bronze_path = f"s3://{self.bucket_bronze}/FILES/FILES/FILES_FILES_{dim_name}/"
            
            # Verificar si existe data en Bronze
            try:
                            
                df_dim = (
                    self.spark.read
                        .option("recursiveFileLookup", "true")
                        .format("parquet")
                        .load(s3_bronze_path)
                )

                record_count = df_dim.count()
                
                if record_count == 0:
                    self.logger.warning(f"No records found for dimension {dim_name}")
                    return
                
                self.logger.info(f"Loaded {record_count} records from {dim_name}")
                
                # Guardar en Silver
                self._save_dimension_to_silver(df_dim, dim_name)
                
                self.logger.info(f"Successfully processed dimension: {dim_name}")
                
            except Exception as read_error:
                self.logger.warning(f"Could not read dimension {dim_name} from Bronze: {str(read_error)}")
                
        except Exception as e:
            self.logger.error(f"Error loading dimension {dim_name}: {str(e)}")
            raise

    def _save_dimension_to_silver(self, df, dim_name):
        """Save dimension to Silver layer with partitioning."""
        try:
            dim_name = dim_name.upper()
            dim_name = dim_name.replace('.xlsx', '').upper()
            s3_prefix = f"GENERAL/TRANSFORMATION/{dim_name}/"
            s3_target_path = f"s3://{self.bucket_silver}/{s3_prefix}"
            self.logger.info(f"Saving dimension to: {s3_target_path}")

            df = self.silver_transformer.clean_dims(df)
            df = self.dimension_handler.process_dimension_with_scd(df,dim_name)
        except Exception as e:
            self.logger.error(f"Error saving dimension {dim_name}: {str(e)}")
            raise

    def process_all_dimensions(self, config):
        """Process all active dimensions."""
        try:
            active_dims = self.get_active_dimensions(config)
            
            if not active_dims:
                self.logger.warning("No active dimensions to process")
                return
            
            self.logger.info(f"Processing {len(active_dims)} dimensions")
            
            for dim_name in active_dims:
                try:
                    self.load_dimension(dim_name)
                except Exception as dim_error:
                    self.logger.error(f"Failed to process dimension {dim_name}: {str(dim_error)}")
                    # Continuar con las siguientes dimensiones
                    continue
            
            self.logger.info("All dimensions processed")
            
        except Exception as e:
            self.logger.error(f"Error processing dimensions: {str(e)}")
            raise


def main():
    """Main function for dimensions loading."""
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 
        'BUCKET_SILVER', 
        'BUCKET_BRONZE', 
        'S3_STATUS_CONFIG_PATH'
    ])

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
  
    start_time = datetime.now()
    print(f"Dimensions Loader Job {args['JOB_NAME']} started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Inicializar procesador
        loader = DimensionsLoader(
            spark, 
            glue_context, 
            args['JOB_NAME'], 
            args['BUCKET_SILVER']
        )
        loader.set_bucket_bronze(args['BUCKET_BRONZE'])
        
        loader.logger.info(f"Starting Dimensions Loader job: {args['JOB_NAME']}")

        # Cargar configuración
        config = loader.load_config(args['S3_STATUS_CONFIG_PATH'])
        
        # Procesar todas las dimensiones activas
        loader.process_all_dimensions(config)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        loader.logger.info(f"Dimensions Loader job completed successfully!")
        loader.logger.info(f"Duration: {duration:.2f} seconds")

    except Exception as e:
        print(f"Error in Dimensions Loader job: {str(e)}")
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()
