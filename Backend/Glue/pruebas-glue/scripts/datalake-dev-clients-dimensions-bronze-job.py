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
from datetime import datetime
## ----- CUSTOM LIBRARIES ------------------ ###
from utils import Utils
from logger_config import logger
from extract_helper import ExtractHelper


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
    'JOB_PROCESS_ID'
])

## ----- INIT JOB ------------------ ###
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

start_time = datetime.now()
logger.info(f"Job {args['JOB_NAME']} start at {start_time.strftime('%Y-%m-%d %H:%M:%S')} with PID: {args['JOB_PROCESS_ID']}")

# Process Cardiff columns
CARDIFF_COLUMNS = [item.strip() for item in args['CARDIFF_COLUMNS'].split('|')]

logger.info(f'Cardiff Columns: {CARDIFF_COLUMNS}')
logger.info(f'Cardiff Columns Count: {len(CARDIFF_COLUMNS)}')

# Initialize ExtractHelper
extract_helper = ExtractHelper(
    glueContext,
    spark,
    args['BUCKET_ASSETS'],
    args['BUCKET_TARGET'],
    args['DB_SCHEMA'],
    args['DB_NAME'],
    args['CONNECTION_NAME'],
    args['S3_PATH_PROCESS_TABLE_DEFINITIONS'],
    args['S3_PATH_SAVE_JSON_TABLE_DEFINITIONS']
)

# Generate unique job ID for tracking
utils_instance = Utils()
job_id = utils_instance.generate_unique_job_id(args['JOB_NAME'])
logger.info(f"Job ID: {job_id}")

## ----- MAIN LOGIC ------------------ ###

def determine_extraction_strategy():
    """
    Determine extraction strategy based on DB_NAME and DB_SCHEMA.
    
    Returns:
        str: 'CARDIFF', 'FILES', or 'DATABASE'
    """
    db_name = args['DB_NAME'].upper()
    db_schema = args['DB_SCHEMA'].upper()
    
    if db_name == 'CARDIFF':
        return 'CARDIFF'
    elif db_schema == 'FILES':
        return 'FILES'
    else:
        return 'DATABASE'

def process_cardiff_workflow():
    """Process Cardiff-specific data extraction with improved table_info discovery."""
    logger.info("=" * 60)
    logger.info("EXECUTING CARDIFF WORKFLOW")
    logger.info("=" * 60)
    
    try:
        # Load table definitions
        table_definitions = extract_helper.load_json_connection()
        
        if not table_definitions or 'general_schema' not in table_definitions:
            raise ValueError("No valid table definitions found for Cardiff")
        
        cardiff_table_info = None
        for schema_name, tables in table_definitions['general_schema'].items():
            logger.info(f"Searching Cardiff config in schema: {schema_name}")
            for table_info in tables:
                table_db = table_info.get('db', '').upper()
                table_name = table_info.get('table', '').upper()
                partition_column = table_info.get('partition_column', 'NA')
                
                # Cardiff can be identified by DB='CARDIFF' or TABLE contains 'CARDIFF'
                if table_db == 'CARDIFF' or 'CARDIFF' in table_name:
                    cardiff_table_info = table_info
                    logger.info(f"Cardiff configuration found in {schema_name}: {table_info.get('table', 'Unknown')}")
                    break
            
            if cardiff_table_info:
                break
        
        # Fallback: use first table in FILES schema if Cardiff not found specifically
        if not cardiff_table_info:
            logger.warning("Cardiff specific config not found, using first FILES table")
            if 'FILES' in table_definitions['general_schema'] and table_definitions['general_schema']['FILES']:
                cardiff_table_info = table_definitions['general_schema']['FILES'][0]
            else:
                raise ValueError("No Cardiff or FILES configuration found")
        
        # Get type_load from found configuration
        type_load = cardiff_table_info.get("type_ingest", "DELTA")
        
        logger.info(f"Cardiff Configuration:")
        logger.info(f"  - Load Type: {type_load}")
        logger.info(f"  - Table: {cardiff_table_info.get('table', 'N/A')}")
        logger.info(f"  - DB: {cardiff_table_info.get('db', 'N/A')}")
        logger.info(f"  - Cardiff Columns Count: {len(CARDIFF_COLUMNS)}")
        
        # Process Cardiff files with proper table_info
        df_result = extract_helper.read_and_save_file(
            s3_input_path=args['S3_PATH_FILES'],
            type_load=type_load,
            cardiff_columns=CARDIFF_COLUMNS,
            table_info=cardiff_table_info,
            partition_column=partition_column if partition_column != 'NA' else ''
        )
        
        record_count = df_result.count() if df_result else 0
        logger.info(f"Cardiff processing completed. Records processed: {record_count}")
        
        return {
            'status': 'SUCCESS',
            'strategy': 'CARDIFF', 
            'records_processed': record_count,
            'load_type': type_load,
            'table_info': cardiff_table_info
        }
        
    except Exception as e:
        logger.error(f"ERROR in Cardiff workflow: {str(e)}")
        return {'status': 'ERROR', 'strategy': 'CARDIFF', 'error': str(e)}
 
def process_files_workflow():
    """Process file-based data extraction."""
    logger.info("=" * 60)
    logger.info("EXECUTING FILES WORKFLOW")
    logger.info("=" * 60)
    
    try:
        # Load table definitions
        table_definitions = extract_helper.load_json_connection()
        
        if not table_definitions or 'general_schema' not in table_definitions:
            raise ValueError("No valid table definitions found")
        
        total_records = 0
        tables_processed = 0
        
        # Process each schema and table
        for schema_name, tables in table_definitions['general_schema'].items():
            logger.info(f"Processing schema: {schema_name}")
            
            for table_info in tables:
                if not table_info.get('status', False):
                    logger.info(f"Skipping inactive table: {table_info.get('table', 'Unknown')}")
                    continue
                
                table_name = table_info.get('table', 'Unknown')
                sheet_name = table_info.get('sheet', '')
                partition_column = table_info.get('partition_column', 'NA')
                type_ingest = table_info.get('type_ingest', 'INITIAL')
                
                logger.info(f"Processing file for table: {table_name} (TYPE_LOAD: {type_ingest})")
                
                try:
                    filename = table_name
                    df_result = extract_helper.read_and_save_file(
                        s3_input_path=args['S3_PATH_FILES'],
                        filename=filename,
                        sheet=sheet_name if sheet_name and sheet_name != 'NA' else None,
                        delimiter=",",
                        header=True,
                        partition_column=partition_column if partition_column != 'NA' else None,
                        table_info=table_info 
                    )
                    
                    record_count = df_result.count() if df_result else 0
                    total_records += record_count
                    tables_processed += 1
                    
                    logger.info(f"Table {table_name} processed successfully. Records: {record_count}")
                    
                except Exception as table_error:
                    logger.error(f"Error processing table {table_name}: {str(table_error)}")
        
        return {
            'status': 'SUCCESS',
            'strategy': 'FILES',
            'tables_processed': tables_processed,
            'total_records': total_records
        }
        
    except Exception as e:
        logger.error(f"ERROR in Files workflow: {str(e)}")
        return {'status': 'ERROR', 'strategy': 'FILES', 'error': str(e)}

def process_database_workflow():
    """Process database-based data extraction."""
    logger.info("=" * 60)
    logger.info("EXECUTING DATABASE WORKFLOW")
    logger.info("=" * 60)
    
    try:
        # Load table definitions
        table_definitions = extract_helper.load_json_connection()
        
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
                        connectname=args['CONNECTION_NAME'],
                        primary_key_column=primary_key,
                        table_info=table_info
                    )
                    
                    record_count = df_result.count() if df_result else 0
                    total_records += record_count
                    tables_processed += 1
                    
                    logger.info(f"Table {table_name} processed successfully. Records: {record_count}")
                    
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

## ----- EXECUTION ------------------ ###

try:
    logger.info("=" * 80)
    logger.info("BRONZE PHASE - DATA EXTRACTION PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Job Name: {args['JOB_NAME']}")
    logger.info(f"DB Name: {args['DB_NAME']}")
    logger.info(f"DB Schema: {args['DB_SCHEMA']}")
    logger.info(f"Job ID: {job_id}")
    logger.info("=" * 80)
    
    start_time = datetime.now()
    
    # Determine and execute extraction strategy
    strategy = determine_extraction_strategy()
    logger.info(f"Extraction Strategy: {strategy}")
    
    if strategy == 'CARDIFF':
        result = process_cardiff_workflow()
    elif strategy == 'FILES':
        result = process_files_workflow()
    else:  # DATABASE
        result = process_database_workflow()
    
    end_time = datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    
    # Print final results
    logger.info("\n" + "=" * 80)
    logger.info("EXECUTION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Status: {result.get('status', 'UNKNOWN')}")
    logger.info(f"Strategy: {result.get('strategy', 'UNKNOWN')}")
    logger.info(f"Execution Time: {execution_time:.2f} seconds")
    
    if 'total_records' in result:
        logger.info(f"Total Records: {result['total_records']:,}")
    if 'records_processed' in result:
        logger.info(f"Records Processed: {result['records_processed']:,}")
    if 'tables_processed' in result:
        logger.info(f"Tables Processed: {result['tables_processed']}")
    if 'load_type' in result:
        logger.info(f"Load Type: {result['load_type']}")
    
    logger.info("=" * 80)
    
    if result.get('status') == 'SUCCESS':
        logger.info("Bronze Phase completed successfully!")
    else:
        logger.error("Bronze Phase completed with errors!")
        if 'error' in result:
            logger.error(f"Error details: {result['error']}")

except Exception as e:
    logger.error(f"\nCRITICAL ERROR: {str(e)}")
    logger.error("Bronze Phase execution failed!")

## --------------- END ------------------ ##
job.commit()