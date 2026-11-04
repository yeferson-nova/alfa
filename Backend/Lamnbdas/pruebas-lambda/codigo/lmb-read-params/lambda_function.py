"""
================================================================================
                    SMB Configuration Generator Lambda Function
================================================================================

Description:
    AWS Lambda function that generates SMB extraction configuration for the
    Bronze layer Step Function workflow. Reads Excel configuration file from S3,
    filters active file definitions, and produces the configuration structure
    required by the Step Function's SMB extraction process.

Author:         Juan Alejandro Carrillo Jaimes [juan.carrillo@segurosalfa.com.co]
Created:        2025-12-23
Version:        1.0.0
Environment:    Development

Lambda Configuration:
    - Memory: 256 MB (recommended)
    - Timeout: 60 seconds
    - Runtime: Python 3.11+
    - Layers: pandas, openpyxl

Environment Variables Required:
    - LOG_LEVEL: Logging level (INFO, DEBUG, ERROR) - Default: INFO

Event Structure:
    {
        "bucket": "alfa-datalake-dev-assets",
        "key": "CLIENTS_ASSETS/config/",
        "file": "table_definition_extraction_clients.xlsx"
    }

Response Structure:
    {
        "statusCode": 200,
        "body": {
            "smb_extraction_config": {
                "enabled": true,
                "files": [
                    {
                        "file_name": "DOM_CUSTOMERS_*",
                        "path": "CARDIFF/",
                        "restricted": ["DOM_CUSTOMERS_FULL.txt"]
                    }
                ]
            }
        }
    }

Excel Configuration Schema:
    Required Columns:
        - STATUS: 1 (active) or 0 (inactive)
        - SCHEMA: Must be "FILES" for SMB extraction
        - FILENAME: File name or pattern (supports wildcards)
        - PATH: SMB path where file is located
        - RESTRICTED: Optional JSON array or CSV of restricted files

Integration:
    This lambda is invoked at the start of the Bronze Layer Step Function
    to dynamically determine which files should be extracted from SMB.

================================================================================
"""

import json
import logging
import io
import os
import boto3
import pandas as pd
from typing import Dict, List, Any, Optional

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

# Initialize AWS clients
s3 = boto3.client("s3")


def read_excel_from_s3(bucket: str, key: str, file: str) -> pd.DataFrame:
    """
    Read Excel file from S3 and return as pandas DataFrame.
    
    Args:
        bucket (str): S3 bucket name
        key (str): S3 key (folder path)
        file (str): Excel filename
        
    Returns:
        pd.DataFrame: DataFrame containing Excel data
        
    Raises:
        ClientError: If S3 object cannot be retrieved
        Exception: If Excel file cannot be parsed
    """
    s3_path = key.rstrip("/") + "/" + file
    logger.info(f"Reading Excel file from S3: s3://{bucket}/{s3_path}")

    try:
        obj = s3.get_object(Bucket=bucket, Key=s3_path)
        content = obj["Body"].read()
        
        df = pd.read_excel(io.BytesIO(content), engine="openpyxl")
        logger.info(f"Successfully loaded Excel with {len(df)} rows and {len(df.columns)} columns")
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading Excel from S3: {str(e)}")
        raise


def parse_restricted(value: Any) -> List[str]:
    """
    Parse RESTRICTED field into a list of filenames.
    
    Handles multiple input formats:
    - None/NaN: Returns empty list
    - List: Returns as-is
    - JSON string: Parses and returns list
    - CSV string: Splits on comma
    - Single value: Returns as single-item list
    
    Args:
        value (Any): Value from RESTRICTED column
        
    Returns:
        List[str]: List of restricted filenames
        
    Examples:
        >>> parse_restricted(None)
        []
        >>> parse_restricted('["file1.txt", "file2.csv"]')
        ['file1.txt', 'file2.csv']
        >>> parse_restricted('file1.txt,file2.csv')
        ['file1.txt', 'file2.csv']
    """
    # Handle None/NaN
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []

    # Already a list
    if isinstance(value, list):
        return value

    # String processing
    if isinstance(value, str):
        s = value.strip()
        if s == "":
            return []
            
        # Try JSON parsing
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return parsed
            else:
                logger.warning(f"JSON parsed value is not a list: {type(parsed)}")
                return []
        except json.JSONDecodeError:
            # Not JSON, try CSV
            if "," in s:
                return [item.strip() for item in s.split(",") if item.strip()]
            # Single value
            return [s]

    # Unexpected type
    logger.warning(f"Unexpected type for restricted field: {type(value)}")
    return []


def validate_event(event: Dict[str, Any]) -> tuple:
    """
    Validate and extract required parameters from event.
    
    Args:
        event (Dict[str, Any]): Lambda event payload
        
    Returns:
        tuple: (bucket, key, file, error_message)
               error_message is None if validation passes
    """
    bucket = event.get("bucket")
    key = event.get("key")
    file = event.get("file")

    if not bucket:
        return None, None, None, "Missing required parameter: bucket"
    if not key:
        return None, None, None, "Missing required parameter: key"
    if not file:
        return None, None, None, "Missing required parameter: file"

    return bucket, key, file, None


def build_file_config(row: pd.Series) -> Dict[str, Any]:
    """
    Build file configuration dictionary from DataFrame row.
    
    Args:
        row (pd.Series): DataFrame row containing file configuration
        
    Returns:
        Dict[str, Any]: File configuration with file_name, path, and restricted
    """
    file_config = {
        "file_name": str(row["FILENAME"]),
        "path": str(row["PATH"]),
        "restricted": parse_restricted(row.get("RESTRICTED"))
    }
    
    logger.debug(f"Built config for file: {file_config['file_name']} in path: {file_config['path']}")
    
    return file_config


def generate_smb_config(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate SMB extraction configuration from DataFrame.
    
    Filters for active files (STATUS=1, SCHEMA=FILES) and builds
    the configuration structure expected by Step Function.
    
    Args:
        df (pd.DataFrame): Excel data containing file definitions
        
    Returns:
        Dict[str, Any]: SMB configuration with enabled flag and files list
    """
    # Filter: STATUS=1 AND SCHEMA=FILES
    df_filtered = df[(df["STATUS"] == 1) & (df["SCHEMA"] == "FILES")]
    
    logger.info(f"Total rows in Excel: {len(df)}")
    logger.info(f"Filtered rows (STATUS=1, SCHEMA=FILES): {len(df_filtered)}")

    if len(df_filtered) == 0:
        logger.info("No files to process - SMB extraction disabled")
        return {
            "smb_extraction_config": {
                "enabled": False
            }
        }

    # Build files list
    files_list = []
    for _, row in df_filtered.iterrows():
        file_config = build_file_config(row)
        files_list.append(file_config)

    logger.info(f"SMB extraction enabled with {len(files_list)} file(s) configured")
    
    return {
        "smb_extraction_config": {
            "enabled": True,
            "files": files_list
        }
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for SMB configuration generation.
    
    Orchestrates the entire process:
    1. Validates event parameters
    2. Reads Excel configuration from S3
    3. Filters active file definitions
    4. Generates SMB extraction configuration
    
    Args:
        event (Dict[str, Any]): Lambda event payload
        context (Any): Lambda context object
        
    Returns:
        Dict[str, Any]: Response with statusCode and body containing configuration
        
    Example Event:
        {
            "bucket": "alfa-datalake-dev-assets",
            "key": "CLIENTS_ASSETS/config/",
            "file": "table_definition_extraction_clients.xlsx"
        }
        
    Example Response:
        {
            "statusCode": 200,
            "body": {
                "smb_extraction_config": {
                    "enabled": true,
                    "files": [...]
                }
            }
        }
    """
    logger.info("=" * 80)
    logger.info("SMB CONFIGURATION GENERATOR - START")
    logger.info("=" * 80)
    logger.info(f"Event received: {json.dumps(event)}")

    # Validate event
    bucket, key, file, error = validate_event(event)
    if error:
        logger.error(f"Validation error: {error}")
        return {
            "statusCode": 400,
            "body": {
                "error": error,
                "smb_extraction_config": {
                    "enabled": False
                }
            }
        }

    try:
        # Read Excel from S3
        df = read_excel_from_s3(bucket, key, file)
        
        # Generate SMB configuration
        response = generate_smb_config(df)
        
        logger.info("=" * 80)
        logger.info("SMB CONFIGURATION GENERATOR - SUCCESS")
        logger.info(f"Configuration: {json.dumps(response)}")
        logger.info("=" * 80)
        
        return {
            "statusCode": 200,
            "body": response
        }

    except Exception as e:
        error_msg = f"Error processing SMB configuration: {str(e)}"
        logger.error(error_msg, exc_info=True)
        logger.error("=" * 80)
        logger.error("SMB CONFIGURATION GENERATOR - FAILED")
        logger.error("=" * 80)
        
        return {
            "statusCode": 500,
            "body": {
                "error": error_msg,
                "smb_extraction_config": {
                    "enabled": False
                }
            }
        }