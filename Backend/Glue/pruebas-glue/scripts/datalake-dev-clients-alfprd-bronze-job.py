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
Last Updated:   2025-11-10
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
from datetime import datetime
## ----- CUSTOM LIBRARIES ------------------ ###
from logger_config import logger
from extract_helper import ExtractHelper
from bronze_metadata_manager import BronzeMetadataManager
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
    'S3_PATH_STAGING_TABLES',
    'CRAWLER_BRONZE'
])

## ----- INIT JOB ------------------ ###
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

##--------FUNCTIONS-----------------###
def get_extract_helper(glue_context, spark, schema_name, **kwargs)-> ExtractHelper:
    """
    Init extract helper object according to parameters
    """

    return ExtractHelper(
        glue_context,
        spark,
        kwargs['BUCKET_ASSETS'],
        kwargs['BUCKET_TARGET'],
        schema_name,
        kwargs['DB_NAME'],
        kwargs['CONNECTION_NAME'],
        kwargs['S3_PATH_PROCESS_TABLE_DEFINITIONS'],
        kwargs['S3_PATH_SAVE_JSON_TABLE_DEFINITIONS']
    )

def process_database_workflow(table_definitions, count_execution, total_executions, extract_helper, connection_name, db_name, schema_name, metadata_manager):
    """Process database-based data extraction."""
    logger.info("=" * 60)
    logger.info(f"EXECUTING {count_execution}/{total_executions} WORKFLOW FOR SCHEMA {schema_name.upper()} IN DATABASE {db_name.upper()}")
    logger.info("=" * 60)
    
    try:
        # Load table definitions
        # table_definitions = extract_helper.load_json_connection()
        
        if not table_definitions or 'general_schema' not in table_definitions:
            raise ValueError("No valid table definitions found")
        
        total_records = 0
        tables_processed = 0
        
        # Process each schema and table
        for schema_name, tables in table_definitions['general_schema'].items():
            logger.info(f"Processing database schema: {schema_name}")
            
            for table_info in tables:
                if not table_info.get('status', False):
                    logger.info(f"Skipping inactive table: {table_info.get('table', 'Unknown')}")
                    continue
                
                table_name = table_info.get('table', 'Unknown')
                partition_column = table_info.get('partition_column', 'NA')
                sql_logic = table_info.get('logic', '')
                primary_key = table_info.get('primary_key_column', 'PK')
                type_ingest = table_info.get('type_ingest', 'INITIAL')
                
                logger.info(f"Processing database table: {table_name} (TYPE_LOAD: {type_ingest})")
    
                try:
                    df_result = extract_helper.read_from_database(
                        table_name=table_name,
                        partition_column=partition_column if partition_column != 'NA' else '',
                        sql_script=sql_logic,
                        connectname=connection_name,
                        primary_key_column=primary_key,
                        table_info=table_info
                    )
                    
                    record_count = df_result.count() if df_result else 0
                    total_records += record_count
                    tables_processed += 1
                    
                    # ===== REGISTER DATABASE METADATA =====
                    _, s3_path = s3_handler.generate_s3_paths(
                        db_name, 
                        schema_name, 
                        table_name
                    )
                    
                    metadata_manager.register_table_path(
                        table_name=table_name,
                        s3_path=s3_path,
                        type_ingest=type_ingest,
                        partition_column=partition_column,
                        primary_key=primary_key,
                        sql_logic=sql_logic[:100] if sql_logic else None
                    )
                    
                    logger.info(f"Metadata registered for database table: {table_name}")
                    
                    logger.info(f"Table {table_name} processed successfully. Records: {record_count}")

                    # Update Crawler path
                    utils_instance.update_crawler_targets(args['CRAWLER_BRONZE'], s3_path)
                    
                except Exception as table_error:
                    logger.error(f"Error processing table {table_name}: {str(table_error)}")
        
        return {
            'status': 'SUCCESS',
            'strategy': 'DATABASE',
            'tables_processed': tables_processed,
            'total_records': total_records
        }
        
    except Exception as e:
        logger.error(f"ERROR in Database workflow: {str(e)}")
        return {'status': 'ERROR', 'strategy': 'DATABASE', 'error': str(e)}


def pipeline_execute(job_id, count_execution, total_executions,  schema_name, utils_instance,  glue_context, spark, **kwargs):
    """
    Pipeline Execute workflow
    """
    # Start instance
    start_time = datetime.now()
    # 01. Init Extract Helper
    extract_helper = get_extract_helper(glue_context, spark, schema_name, **kwargs)
    # 02. Init MetadataManager
    metadata_manager = BronzeMetadataManager(
        utils_instance=utils_instance,
        bucket_assets=kwargs['BUCKET_ASSETS'],
        job_id=job_id,
        db_name=kwargs['DB_NAME'],
        db_schema=schema_name,
        strategy="DATABASE",
        s3_path_staging=kwargs['S3_PATH_STAGING_TABLES'],
        prefix="ALFAPROD"
    )
    # 03. Execute database workflow
    table_definitions = utils_instance.load_json_connection(kwargs['DB_NAME'],schema_name,kwargs['S3_PATH_SAVE_JSON_TABLE_DEFINITIONS'])

    result = process_database_workflow(
                                table_definitions,
                                count_execution,
                                total_executions,
                                extract_helper, 
                                kwargs['CONNECTION_NAME'],
                                kwargs['DB_NAME'],
                                schema_name,
                                metadata_manager)
    end_time = datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    # Print final results
    if 'total_records' in result:
        logger.info(f"Total Records: {result['total_records']:,}")
    if 'records_processed' in result:
        logger.info(f"Records Processed: {result['records_processed']:,}")
    if 'tables_processed' in result:
        logger.info(f"Tables Processed: {result['tables_processed']}")
    if 'load_type' in result:
        logger.info(f"Load Type: {result['load_type']}")
    logger.info("=" * 80)
    
    logger.info("\n" + "=" * 80)
    logger.info("EXECUTION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Status: {result.get('status', 'UNKNOWN')}")
    logger.info(f"Strategy: {result.get('strategy', 'UNKNOWN')}")
    logger.info(f"Execution Time: {execution_time:.2f} seconds")
    logger.info(f'Schema Proccessed: {schema}')

if __name__ == '__main__':
    try:
        # Start instance
        start_time = datetime.now()
        logger.info(f"Job {args['JOB_NAME']} start at {start_time.strftime('%Y-%m-%d %H:%M:%S')} with PID: {args['JOB_PROCESS_ID']}")
        SCHEMAS = [items.strip() for items in args['DB_SCHEMA'].split(',')]
        
        # Init Utils and S3Handler Object
        utils_instance = Utils()
        s3_handler = S3Handler(args['BUCKET_TARGET'], glueContext)
        
        # Generate Unique JOB ID
        job_id = args['JOB_PROCESS_ID']
        logger.info(f"Job ID: {job_id}")

        # Execute process
        total_executions = len(SCHEMAS)
        for count_execution, schema in enumerate(SCHEMAS):
            start_time = datetime.now()
            pipeline_execute(job_id,
                                (count_execution + 1) ,
                                total_executions,
                                schema,
                                utils_instance,
                                glueContext,
                                spark,
                                **args
            )
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            logger.info("\n" + "=" * 80)
            logger.info(f"Execution Time: {execution_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"\nCRITICAL ERROR: {str(e)}")
        logger.error("Bronze Phase execution failed!")

    ## --------------- END ------------------ ##
    job.commit()