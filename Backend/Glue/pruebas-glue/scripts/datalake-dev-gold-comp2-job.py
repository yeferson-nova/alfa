"""
AWS Glue ETL Job - Component 2 Processing

Author: Alejandro Jaimes
Created: 2025-06-02
Last Updated: 2025-06-10
Description: This script extracts and processes main components of Component 2 business logic,
            combining OLA0 and OLA1 data sources into a unified detailed policy fact table.

Architecture:
- OLA0: Historical data from urincterrdeudores
- OLA1: Current data from step_gold, step_gold_plus, and step_gold_bodega
- Output: Unified FACT_POLIZA_DETALLADA table
"""

# IMPORT LIBRARIES
## SYSTEM LIBRARIES
from dataclasses import dataclass
from typing import List, Dict, Optional, Any, Union
import sys
import traceback
import logging

## AWS GLUE LIBRARIES
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

## PYSPARK LIBRARIES
from pyspark.context import SparkContext
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import (
    DecimalType, StringType, IntegerType, BooleanType,
    FloatType, DoubleType, StructType, StructField
)
from pyspark.sql.window import Window

## CUSTOM LIBRARIES
from commons.ExtractHelper import ExtractHelper
from commons.IncrementalCharge import IncrementalCharge
from commons.StepGold import StepGold
from commons.StepGoldPlus import StepGoldPlus
from commons.StepGoldBodega import StepGoldBodega
from commons.TransformationHelper import TransformationHelper
from commons.UrincterrdeudoresHelper import UrincterrdeudoresHelper

# CONFIGURATION
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

## JOB PARAMETERS
args = getResolvedOptions(sys.argv,
                            ['JOB_NAME',
                            'BUCKET_NAME_SILVER',
                            'BUCKET_NAME_GOLD',
                            'BUCKET_NAME_ASSETS',
                            'DATE'])

# ENVIRONMENT VARIABLES
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
BUCKET_NAME_GOLD = args['BUCKET_NAME_GOLD']
BUCKET_NAME_SILVER = args['BUCKET_NAME_SILVER']

# SPARK SESSION INITIALIZATION
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# SPARK CONFIGURATIONS
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")


@dataclass
class Component2Processor:
    """
    Component 2 ETL Processor for detailed policy fact table creation.
    
    This class orchestrates the extraction and transformation of insurance policy data
    from multiple sources (OLA0 and OLA1) into a unified detailed policy fact table.
    
    Attributes:
        args: Job arguments from Glue
        spark: Spark session
        glue_context: Glue context for DataFrame operations
        target_bucket: S3 bucket for output data
        output_path: Full S3 path for saving processed data
    """
    
    args: Any
    spark: Any
    glue_context: Any
    target_bucket: str = BUCKET_NAME_GOLD
    output_path: str = ""

    def __post_init__(self):
        """Initialize private attributes and helper classes after dataclass creation."""
        # Private DataFrames
        self._df_ola0_raw: Optional[DataFrame] = None
        self._df_ola1_raw: Optional[DataFrame] = None
        self._df_ola0_processed: Optional[DataFrame] = None
        self._df_ola1_processed: Optional[DataFrame] = None
        self._df_unified: Optional[DataFrame] = None
        self._total_records: Optional[int] = None 
        
        # Helper classes
        self._incremental_charge = IncrementalCharge(self.glue_context, self.spark, "-", "-")
        self._extract_helper = ExtractHelper(self.glue_context, self.spark, "-", "-")
        self._transformation_helper = TransformationHelper()
        
        # Configuration paths
        self._ola1_config_path = f"s3://{BUCKET_NAME_ASSETS}/support_phase_files/procces_ola1.xlsx"
        self._ola0_config_path = f"s3://{BUCKET_NAME_ASSETS}/support_phase_files/procces_ola0.xlsx"
        
        # Set output path
        self.output_path = f"s3://{self.target_bucket}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_POLIZA_DETALLADA-PF/"

    def _initialize_temporary_tables(self) -> None:
        """
        Initialize temporary tables from Gold layer configuration.
        
        Loads table configurations from Excel files and creates temporary views
        for subsequent processing steps.
        
        Raises:
            Exception: If table initialization fails
        """
        logger.info("🚀 Initializing temporary tables from Gold layer...")
        
        try:
            # Load configuration sheets
            gold_sheets = ["gold_phase", "gold_tables"]

            logger.info(f"<---Extract gold_phase sheet tables from {self._ola1_config_path}")
            df_tables = self._extract_helper.extract_excel_data(self._ola1_config_path, gold_sheets[0])

            logger.info(f"<---Extract gold_tables sheet tables from {self._ola1_config_path}")
            df_tmp_tables = self._extract_helper.extract_excel_data(self._ola1_config_path, gold_sheets[1])

            # Process each table configuration
            for row in df_tmp_tables.collect():
                table_name = row["Table"].upper()
                phase = row["Phase"].upper()
                
                # Determine source path based on phase
                if phase == "SILVER":
                    source_path = f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}_SILVER_"
                else:
                    source_path = f"s3://{BUCKET_NAME_GOLD}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-CARACT_SINIESTROS-"
                
                # Load table as temporary view
                self._incremental_charge.charge_tmp_tables(source_path, table_name, 'Gold')
                logger.info(f" Loaded temporary table: {table_name}")
                
        except Exception as e:
            logger.error(f" Error initializing temporary tables: {str(e)}")
            raise

    def _initialize_raw_dataframes(self) -> None:
        """
        Initialize raw DataFrames from different data sources.
        
        Creates OLA0 DataFrame from urincterrdeudores and OLA1 DataFrame
        from union of step_gold, step_gold_plus, and step_gold_bodega.
        
        Raises:
            Exception: If DataFrame initialization fails
        """
        logger.info("🚀 Initializing raw DataFrames...")
        
        try:
            # Initialize OLA0 DataFrame (Historical data)
            urincterrdeudores_helper = UrincterrdeudoresHelper(self.glue_context, self.spark)
            self._df_ola0_raw = urincterrdeudores_helper.create_urincterrdeudores()
            logger.info(" OLA0 raw DataFrame initialized")
            
            # Initialize OLA1 DataFrame components
            step_gold = StepGold(self.glue_context, self.spark)
            step_gold_plus = StepGoldPlus(self.glue_context, self.spark)
            step_gold_bodega = StepGoldBodega(self.glue_context, self.spark)
            
            # Create individual components
            df_tierra = step_gold.create_component()
            df_plus = step_gold_plus.create_component()
            df_bodega = step_gold_bodega.create_component()
            
            # Union all OLA1 components
            self._df_ola1_raw = df_tierra.union(df_plus).union(df_bodega)
            logger.info(" OLA1 raw DataFrame initialized")
            
        except Exception as e:
            logger.error(f" Error initializing raw DataFrames: {str(e)}")
            raise

    def _process_ola0_fields(self) -> None:
        """
        Process and select required fields from OLA0 DataFrame.
        
        Transforms OLA0 raw data into standardized format with required fields
        for the detailed policy fact table.
        """
        logger.info(" Processing OLA0 fields...")
        
        if self._df_ola0_raw is None:
            raise ValueError("OLA0 raw DataFrame not initialized")
        
        try:
            self._df_ola0_processed = self._df_ola0_raw.select(
                F.col("fecha_desembolso_credito").cast("date"),
                F.col("id_siniestro_fk").cast("string"),
                F.col("id_poliza_fk").cast("string"),
                F.col("fecha_inicio_vigencia").cast("date"),
                F.col("id_fuente_fk").cast("string"),
                F.col("id_poliza_detallada_pk")
            )
            
            logger.info(f" OLA0 processed successfully.")
            
        except Exception as e:
            logger.error(f" Error processing OLA0 fields: {str(e)}")
            raise

    def _process_ola1_fields(self) -> None:
        """
        Process and select required fields from OLA1 DataFrame.
        
        Transforms OLA1 raw data into standardized format, generating synthetic
        primary keys and standardizing field names to match OLA0 structure.
        """
        logger.info(" Processing OLA1 fields...")
        
        if self._df_ola1_raw is None:
            raise ValueError("OLA1 raw DataFrame not initialized")
        
        try:
            # Create window specification for generating sequential IDs
            window_spec = Window.orderBy(F.monotonically_increasing_id())
            
            self._df_ola1_processed = self._df_ola1_raw.withColumn(
                "fecha_desembolso_credito", 
                F.lit(None).cast("date")  # To be defined later
            ).select(
                F.col("fecha_desembolso_credito"),
                F.col("numero_siniestro").cast("string").alias("id_siniestro_fk"),
                F.col("poliza").cast("string").alias("id_poliza_fk"),
                F.col("fecha_inicio_vigencia").cast("date"),
                F.col("id_fuente_fk").cast("string")
            ).distinct()

            self._df_ola1_processed =  self._df_ola1_processed.withColumn(
                "id_poliza_detallada_pk",
                (F.row_number().over(window_spec) + 1000000000).cast("int")
            )
            
            logger.info(f" OLA1 processed successfully.")
            
        except Exception as e:
            logger.error(f" Error processing OLA1 fields: {str(e)}")
            raise

    def _union_dataframes(self) -> None:
        """
        Union processed OLA0 and OLA1 DataFrames into unified dataset.
        
        Combines both processed DataFrames ensuring schema compatibility
        and creating the final detailed policy fact table.
        
        Raises:
            ValueError: If processed DataFrames are not available
        """
        logger.info("🔗 Unioning OLA0 and OLA1 DataFrames...")
        
        if self._df_ola0_processed is None or self._df_ola1_processed is None: #
            raise ValueError("Processed DataFrames not available for union operation")
        
        try:
            # Perform union operation
            self._df_unified = self._df_ola0_processed.union(self._df_ola1_processed)

            # Change fecha_desembolso_credito
            self._df_unified = self._df_unified.withColumn(
                "fecha_desembolso_credito",
                F.lit(None).cast("Date")
            )
            # Log union results
            self._total_records = self._df_unified.count()
            
            logger.info(f" Union completed successfully:")
            logger.info(f"   📊 Total records: {self._total_records}")
        except Exception as e:
            logger.error(f" Error unioning DataFrames: {str(e)}")
            raise

    def _save_results(self) -> None:
        """
        Save unified DataFrame to target S3 location.
        
        Writes the processed data to S3 in Parquet format with appropriate
        partitioning for optimal query performance.
        """
        logger.info(f"💾 Saving results to: {self.output_path}")
        
        if self._df_unified is None:
            raise ValueError("Unified DataFrame not available for saving")
        
        try:
            # Save
            self._extract_helper.save_files(self._df_unified, self.output_path)
            logger.info(f" Results saved successfully: {self.output_path}")
            
        except Exception as e:
            logger.error(f" Error saving results: {str(e)}")
            raise

    def process(self) -> DataFrame:
        """
        Main orchestration method for Component 2 processing.
        
        Executes the complete ETL pipeline including:
        1. Temporary table initialization
        2. Raw DataFrame creation
        3. Field processing for both OLA0 and OLA1
        4. DataFrame union
        5. Result persistence
        
        Returns:
            DataFrame: The unified processed DataFrame
            
        Raises:
            Exception: If any step in the processing pipeline fails
        """
        logger.info(" Starting Component 2 processing pipeline...")
        
        try:
            # Step 1: Initialize temporary tables
            self._initialize_temporary_tables()
            
            # Step 2: Initialize raw DataFrames
            self._initialize_raw_dataframes()
            
            # Step 3: Process OLA0 fields
            self._process_ola0_fields()
            
            # Step 4: Process OLA1 fields
            self._process_ola1_fields()
            
            # Step 5: Union processed DataFrames
            self._union_dataframes()
            
            # Step 6: Save results
            self._save_results()
            
            logger.info("🎉 Component 2 processing completed successfully!")
            return self._df_unified
            
        except Exception as e:
            logger.error(f" Component 2 processing failed: {str(e)}")
            logger.error(f" Traceback: {traceback.format_exc()}")
            raise

    def get_processing_summary(self) -> Dict[str, Any]:
        """
        Get processing summary statistics.
        
        Returns:
            Dict containing processing metrics and statistics
        """
        if self._df_unified is None:
            return {"status": "not_processed"}
        
        return {
            "status": "completed",
            "total_records": self._total_records,
            "output_path": self.output_path,
            "job_name": self.args['JOB_NAME']
        }


# MAIN EXECUTION
if __name__ == "__main__":
    try:
        # Initialize Component 2 processor
        component_processor = Component2Processor(
            args=args,
            spark=spark,
            glue_context=glueContext,
            target_bucket=BUCKET_NAME_GOLD
        )
        
        # Execute processing pipeline
        result_df = component_processor.process()
        
        # Get and log processing summary
        summary = component_processor.get_processing_summary()
        logger.info(f"****** Processing Summary: {summary} **********")
        
        # Commit job
        job.commit()
        logger.info(" Job completed successfully")
        
    except Exception as e:
        logger.error(f" Job failed: {str(e)}")
        raise
    finally:
        # Clean up Spark context
        if 'sc' in locals():
            sc.stop()