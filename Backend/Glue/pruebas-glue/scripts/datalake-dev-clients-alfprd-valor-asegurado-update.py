import boto3
import json
import sys
import os
import re
import zipfile
from datetime import datetime
from awsglue.utils import getResolvedOptions

# Find and extract utils.zip
glue_libs_dir = [d for d in os.listdir('/tmp') if d.startswith('glue-python-libs')]
if glue_libs_dir:
    utils_zip = os.path.join('/tmp', glue_libs_dir[0], 'utils.zip')
    if os.path.exists(utils_zip):
        extract_path = os.path.join('/tmp', 'extracted_utils')
        with zipfile.ZipFile(utils_zip, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        sys.path.insert(0, extract_path)
        print(f"Extracted utils.zip to: {extract_path}")

from logger_config import logger

# Get job parameters
args = getResolvedOptions(
    sys.argv, 
    ['S3_PATH_VALOR_ASEGURADO_FILES', 'S3_PATH_STATUS_CONFIG_MVP2', 'CONFIG_KEY']
)

s3 = boto3.client('s3')

def parse_s3_path(s3_path):
    """Parse S3 path and return bucket and prefix"""
    path = s3_path.replace('s3://', '')
    parts = path.split('/', 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ''
    return bucket, prefix

def extract_date_from_filename(filename):
    """
    Extract date from filename format: valores_prv_arl_YYYYMM.xlsx
    Returns datetime or None if pattern doesn't match
    """
    pattern = r'valores_prv_arl_(\d{6})\.xlsx'
    match = re.search(pattern, filename)
    if match:
        date_str = match.group(1)  # YYYYMM
        try:
            # Add day 01 to parse as date
            return datetime.strptime(date_str + '01', '%Y%m%d')
        except ValueError:
            logger.warn(f"Invalid date in filename: {filename}")
            return None
    return None

def get_most_recent_file(bucket, prefix):
    """
    List files and return the most recent based on filename pattern
    """
    logger.info(f"Listing files in s3://{bucket}/{prefix}")
    
    try:
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )
        
        if 'Contents' not in response or len(response['Contents']) == 0:
            raise Exception(f"No files found in s3://{bucket}/{prefix}")
        
        valid_files = []
        for obj in response['Contents']:
            filename = obj['Key'].split('/')[-1]
            file_date = extract_date_from_filename(filename)
            
            if file_date:
                valid_files.append({
                    'key': obj['Key'],
                    'filename': filename,
                    'date': file_date,
                    'last_modified': obj['LastModified'],
                    'size': obj['Size']
                })
                logger.info(f"Found valid file: {filename} with date {file_date.strftime('%Y%m')}")
        
        if not valid_files:
            raise Exception(f"No files with correct pattern found in s3://{bucket}/{prefix}")
        
        valid_files.sort(key=lambda x: x['date'], reverse=True)
        most_recent = valid_files[0]
        
        logger.info(f"Most recent file found: {most_recent['filename']}")
        logger.info(f"File date: {most_recent['date'].strftime('%Y%m')}")
        logger.info(f"S3 last modified: {most_recent['last_modified']}")
        logger.info(f"Size: {most_recent['size']} bytes")
        
        return most_recent
        
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        raise

def load_json_config(bucket, key):
    """
    Load JSON configuration from S3
    """
    logger.info(f"Loading configuration from s3://{bucket}/{key}")
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        config = json.loads(response['Body'].read().decode('utf-8'))
        logger.info("Configuration loaded successfully")
        return config
        
    except s3.exceptions.NoSuchKey:
        logger.error(f"Configuration file not found: s3://{bucket}/{key}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error loading configuration: {str(e)}")
        raise

def update_json_config(config, config_key, new_file_path):
    """
    Update JSON with new file path
    """
    logger.info(f"Updating key '{config_key}' in configuration")
    
    keys = config_key.split('.')
    
    current = config
    for key in keys[:-1]:
        if key not in current:
            logger.error(f"Key '{key}' not found in JSON")
            raise KeyError(f"Key not found: {key}")
        current = current[key]
    
    final_key = keys[-1]
    old_value = current.get(final_key, "N/A")
    current[final_key] = new_file_path
    
    logger.info(f"Old value: {old_value}")
    logger.info(f"New value: {new_file_path}")
    
    return config

def save_json_config(bucket, key, config):
    """
    Save updated JSON to S3
    """
    logger.info(f"Saving updated configuration to s3://{bucket}/{key}")
    
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(config, indent=2, ensure_ascii=False),
            ContentType='application/json'
        )
        logger.info("Configuration saved successfully")
        
    except Exception as e:
        logger.error(f"Error saving configuration: {str(e)}")
        raise

def main():
    """
    Main job function
    """
    logger.info("=" * 60)
    logger.info("Starting job: Update most recent file")
    logger.info("=" * 60)
    
    try:
        s3_valor_asegurado = args['S3_PATH_VALOR_ASEGURADO_FILES']
        s3_config_path = args['S3_PATH_STATUS_CONFIG_MVP2']
        config_key = args['CONFIG_KEY']
        
        logger.info(f"Parameter S3_PATH_VALOR_ASEGURADO_FILES: {s3_valor_asegurado}")
        logger.info(f"Parameter S3_PATH_STATUS_CONFIG_MVP2: {s3_config_path}")
        logger.info(f"Parameter CONFIG_KEY: {config_key}")
        
        bucket_files, prefix_files = parse_s3_path(s3_valor_asegurado)
        bucket_config, key_config = parse_s3_path(s3_config_path)
        
        config = load_json_config(bucket_config, key_config)
        
        most_recent_file = get_most_recent_file(bucket_files, prefix_files)
        
        new_file_path = f"s3://{bucket_files}/{most_recent_file['key']}"
        
        updated_config = update_json_config(config, config_key, new_file_path)
        
        save_json_config(bucket_config, key_config, updated_config)
        
        logger.info("=" * 60)
        logger.info("Job completed successfully")
        logger.info("=" * 60)
        logger.info(f"Updated file: {most_recent_file['filename']}")
        logger.info(f"New path: {new_file_path}")
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error("ERROR: Job failed")
        logger.error("=" * 60)
        logger.error(str(e))
        raise

if __name__ == "__main__":
    main()