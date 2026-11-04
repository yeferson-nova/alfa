#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
                Bronze Phase - AWS Glue Job Script
================================================================================
 
Description:
    Bronze phase data extraction job for AWS Glue environment.
    Handles database extraction, file processing, and Cardiff workflows
    based on configuration parameters.
 
Author:         Data Engineering Team
Created:        2025-07-25
Last Updated:   2025-07-27
Version:        1.0.0
Environment:    AWS Glue
 
================================================================================
"""

## ----- GLUE LIBRARIES ------------------ ###
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
## ----- PYTHON EXTERNAL LIBRARIES ------------------ ###
import sys
import os
from datetime import datetime
## ----- CUSTOM LIBRARIES ------------------ ###
from logger_config import logger
from utils import Utils
from s3_handler import S3Handler
from file_processor import FileProcessor
from data_transformer import DataTransformer
from load_strategy_manager import LoadStrategyManager
from incremental_loader import IncrementalLoader
 

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

start_time = datetime.now()
logger.info(f"Job {args['JOB_NAME']} start at {start_time.strftime('%Y-%m-%d %H:%M:%S')} with PID: {args['JOB_PROCESS_ID']}")

# CLASS
utils = Utils()
S3_PATH_COREPROD_ACSELE_ALFA_POLICYDCO = "s3://alfa-datalake-dev-bronce/COREPROD/ACSELE_ALFA/"
df = utils.read_full_parquet(spark, s3_target_path=os.path.join(S3_PATH_COREPROD_ACSELE_ALFA_POLICYDCO,'COREPROD_ACSELE_ALFA_POLICYDCO'))
print(f"Total Records : {df.count()}")


## --------------- END ------------------ ##
job.commit()