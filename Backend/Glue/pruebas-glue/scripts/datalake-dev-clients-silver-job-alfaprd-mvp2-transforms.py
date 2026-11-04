#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
                AWS Glue Job: Silver Layer Processing - ALFAPRD MVP2
================================================================================

Description:
    Production ETL pipeline for Silver layer data processing from ALFAPRD source.
    Implements comprehensive data transformation, quality validation, and 
    harmonization workflows for insurance products, policies, and client data.

    This job orchestrates the complete Silver layer ETL process including:
    - Staging data processing with temporal controls (historic/fallen month modes)
    - Multi-source data harmonization using reference tables
    - Data quality transformations and standardization
    - Delta Lake format persistence with date-based partitioning
    - AWS Glue Crawler integration for metadata management

    Data Sources Processed:
    - ALFAPROD_PRODUCTOS_MVP2: Insurance products catalog
    - ALFAPROD_POLIZAS_MVP2: Policy transactions and details
    - ALFAPROD_CLIENTES_MVP2: Client master data

    Key Features:
    - Supports historic data reprocessing via date range parameters
    - Automatic fallen month detection for incremental loads
    - Modular transformation pipeline with clean/harmonize stages
    - Delta format with yyyy/mm partitioning for optimized queries
    - Comprehensive logging and error handling

Author:         Juan Alejandro Carrillo Jaimes
Email:          jnij@gft.com
Created:        2025-12-03
Last Updated:   2025-12-16
Version:        1.1.0
Environment:    AWS Glue / Production

Dependencies:
    - alfaprd_staging_processor_mvp2: Staging data processor
    - data_harmonization_manager: Data harmonization logic
    - mpv2_transformer_proccessor: Data cleanup and transformations
    - s3_handler: S3 operations manager
    - utils: Configuration and helper utilities
    - cleanup: Data cleanup utilities

Parameters:
    JOB_NAME: AWS Glue job identifier
    BUCKET_SILVER: Target S3 bucket for Silver layer
    BUCKET_BRONZE: Source S3 bucket for Bronze layer
    JOB_PROCESS_ID: Unique job execution identifier
    S3_HOMOLOGATION_PATH: Path to homologation reference data
    S3_STATUS_CONFIG_PATH: Path to job configuration JSON
    LOAD_STG_SILVER: Flag to enable/disable staging processing
    CRAWLER_SILVER: AWS Glue Crawler name for metadata updates
    HISTORIC_PARAM: Processing mode (0=fallen month, 1=historic range)
    START_DATE: Start date for historic mode (format: YYYYMM)
    END_DATE: End date for historic mode (format: YYYYMM)

Output:
    Delta tables in S3 Silver layer:
    - POLIZAS_PRODUCTO/ALFAPRD/{table_name}/
    - Partitioned by yyyy/mm
    - Registered in AWS Glue Data Catalog

================================================================================
"""

# --------- AWS GLUE FUNCTIONS ---------------#
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
# ---------- SPARK FUNCTIONS ---------------#
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit
# ---------- PYTHON LIBRARIES -------------#
import sys
import calendar
from datetime import datetime, timedelta
import pandas as pd
# ---------- CUSTOM LIBRARIES -------------#
from alfaprd_staging_processor_mvp2 import AlfprdStagingProcessorMVP2
from logger_config import logger
from s3_handler import S3Handler
from utils import Utils

#############################################
############  GET PARAMS ####################
#############################################
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BUCKET_SILVER',
    'BUCKET_BRONZE',
    'JOB_PROCESS_ID',
    'S3_HOMOLOGATION_PATH',
    'S3_STATUS_CONFIG_PATH',
    'LOAD_STG_SILVER',
    'CRAWLER_SILVER',
    'HISTORIC_PARAM',
    'START_DATE',
    'END_DATE'
])

#############################################
############  INIT GLUE WITH SPARK ##########
#############################################
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_handler = S3Handler(args['BUCKET_SILVER'], glueContext)
utils_instance = Utils()
utils_instance.configure_spark_optimizations(
    spark, "SILVER", "datalake-dev-clients-silver-job-alfaprd-mvp2"
)
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Load Table from catalog+
def read_from_catalog(database_name:str = None, table_name:str = None):
    """ read from catalog """
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=table_name,
        transformation_ctx=f"read_{table_name}_silver"
    )
    return dynamic_frame.toDF()

df_polizas = read_from_catalog(args['BUCKET_SILVER'], "polizas_alfaprd_silver")
df_productos = read_from_catalog(args['BUCKET_SILVER'], "productos_alfaprd_silver")