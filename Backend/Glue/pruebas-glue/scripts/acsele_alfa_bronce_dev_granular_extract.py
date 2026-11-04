"""
Script that loads bronce phase and uploads extracted data to user-defined S3 location in Parquet format sequentially
"""
 
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.ExtractHelper import ExtractHelper
from commons.LogsHelper import LogsHelper
from pyspark.sql import functions as F
 
import sys, traceback, boto3, json, os, shutil
 
###################################################################
######## SET ARGS AND CONFIG COMMONS UTILS ########################
###################################################################

logs = LogsHelper()
logger = logs.logger_conf()

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "DB_SCHEMA",
        "DB_NAME",
        "BUCKET_NAME",
        "BUCKET_NAME_ASSETS",
        "CONNECT_NAME",
        "PERIODS",
        "CERTIFICATE",
    ]
)

PERIODS = json.loads(args['PERIODS'])
 
DB_SCHEMA = args["DB_SCHEMA"]
DB_NAME = args["DB_NAME"]
BUCKET_NAME = args["BUCKET_NAME"]
BUCKET_NAME_ASSETS = args["BUCKET_NAME_ASSETS"]
CONNECT_NAME = args["CONNECT_NAME"]
CERTIFICATE = args["CERTIFICATE"]
PATH_JSON = f"support_phase_files/niif/{DB_SCHEMA.lower()}_tables.json"


logger.info(f"Period: {PERIODS}")
logger.info(f"Schema: {DB_SCHEMA}")
logger.info(f"Connect: {CONNECT_NAME}")
logger.info(f"Bucket_name: {BUCKET_NAME}")
logger.info(f"Json_file: s3://{BUCKET_NAME_ASSETS}/{PATH_JSON}")
 
# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
 
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")


extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME, DB_NAME)


def execute_data_multiple(table_name, part, partition = "-", sql_script = "-", connectname = "-"):
    # Define the connection options
    connect_option = {
        "useConnectionProperties": "true",
        "dbtable": f"{DB_SCHEMA}.{table_name}",
        "connectionName": connectname
    }
    
    # Add sampleQuery if sql_script is not "-"
    if sql_script != "-":
        connect_option["sampleQuery"] = sql_script
        
    try:
        
        tabla_to_extract = glueContext.create_dynamic_frame.from_options(
        connection_type = "oracle",
        connection_options = connect_option,
            transformation_ctx = "TablaExtract"
        )   
        
        if partition == '-':          
            frame_save =  tabla_to_extract     
            logger.info(f"Without partition")
            partition_keys = []            
            
        s3_prefix = f"{DB_NAME}/{DB_SCHEMA}/{table_name}_{part}/"
        
        s3_path = f"s3://{BUCKET_NAME}/{s3_prefix}"
        
        logger.info(f"Save data on:{s3_path}")
        
        #Save files on S3
        amazon_s3 = glueContext.write_dynamic_frame.from_options(
            frame=frame_save, 
            connection_type="s3", 
            format="glueparquet", 
            connection_options={"path": s3_path, "partitionKeys": partition_keys}, 
            format_options={"compression": "snappy"}, 
            transformation_ctx="AmazonS3")  
        
        return amazon_s3
        
    except Exception as e:

        logger.error(f"Se ha producido un error: {e}")   
            
        return Exception

###################################################################
############## VALIDATE FILE IN S3 & READ JSON ####################
###################################################################
 
try:
    # Check if the file exists before attempting to read it
    s3_client.head_object(Bucket=BUCKET_NAME_ASSETS, Key=PATH_JSON)
    logger.info(f"The file {PATH_JSON} exists in S3. Proceeding with the reading.")

    # If the file exists, proceed with the reading
    response = s3_client.get_object(Bucket=BUCKET_NAME_ASSETS, Key=PATH_JSON)
    catalog_tables = json.loads(response["Body"].read().decode("utf-8"))  # Se usa .read()
    logger.info("JSON file successfully loaded.")

except s3_client.exceptions.NoSuchKey:
    logger.error(f"ERROR: The file {PATH_JSON} does not exist in the bucket {BUCKET_NAME_ASSETS}.")
    sys.exit(1)  # Stop execution if the file does not exist

except Exception as e:
    logger.error(f"Unexpected error accessing the JSON in S3: {traceback.format_exc()}")
    sys.exit(1)


###################################################################
################# ITERATE FOR EACH TABLE ##########################
###################################################################

for schema in catalog_tables:
    if schema in DB_SCHEMA:
        for table_info in catalog_tables[schema]:
            if table_info['status']:
                logger.info(f"Tables for ingest: {table_info}")
                fields = ", ".join(table_info["fields"])  # Build comma-separated field list
                table = table_info["table"]
                schema = table_info["schema"]
                start_value = table_info.get("start_value", None)
                limit_values = table_info.get("limit_values", [])
                logic = table_info["logic"]
                type_ingest = table_info["type_ingest"]

                # === FULL LOAD ===
                if not start_value and not limit_values and logic == "-":
                    logger.info(f"[{table}] No start_value or limit_values found. Performing FULL LOAD.")
                    query = f"SELECT {fields} FROM {schema}.{table}"
                    try:
                        logger.info({query})
                        extract_helper.execute_data(
                            table_name  = f"{table}",
                            sql_script  = query.strip(),
                            connectname = CONNECT_NAME
                        )
                        logger.info(f"Executed full load query: {query.strip()}")
                    except Exception as e:
                        logger.error(f"[{table}] ERROR during full load: {e}")

                # === SINGLE QUERY (only one limit) ===
                elif len(limit_values) == 1:
                    logger.info(f"Table with 1 folder")
                    value = limit_values[0]
                    if start_value:
                        query = f"""
                            SELECT {fields} FROM {schema}.{table}
                            WHERE {table_info['partition_column']} >= {start_value}
                              AND {table_info['partition_column']} <= {value}
                        """
                    else:
                        query = f"""
                            SELECT {fields} FROM {schema}.{table}
                            WHERE {table_info['partition_column']} <= {value}
                        """
                    try:
                        logger.info({query})
                        extract_helper.execute_data(
                            table_name  = f"{table}",
                            sql_script  = query.strip(),
                            connectname = CONNECT_NAME
                        )
                        logger.info(f"Executed single query: {query.strip()}")
                    except Exception as e:
                        logger.error(f"ERROR during execution: {e}")
                        
                
                # === OPERATION WITH LOGIC ===
                
                elif logic != "-" and type_ingest == "Initial_Period":
                        logger.info(f"Table with query and periods")
                        logger.info(f"schema for query: {logic}")
                        for period in PERIODS:
                            try:
                                logic_final = logic.format(fields=fields, list_periods=period)
                                logger.info({logic_final})
                                execute_data_multiple(
                                    table_name = f"{table}", 
                                    part = f"part_{period}",
                                    sql_script = logic_final.strip(),
                                    connectname = CONNECT_NAME
                                    )
                                logger.info(f"Executed partitioned query {period}: {logic_final.strip()}")
                                
                            except Exception as e:
                                logger.error(f"ERROR during partition {period}: {e}")
                
                elif logic != "-":
                    logger.info(f"Table with query")
                    try:
                        logger.info({logic})
                        extract_helper.execute_data(
                            table_name  = f"{table}",
                            sql_script  = logic.strip(),
                            connectname = CONNECT_NAME
                        )
                        logger.info(f"Executed table with query: {logic.strip()}")
                    except Exception as e:
                        logger.error(f"ERROR during execution: {e}")                            
                    

                # === MULTIPLE QUERIES (with ranges and overflow) ===
                else:
                    logger.info(f"Table with multiple folder")
                    for limit, value in enumerate(limit_values):
                        if limit == 0:
                            lower = start_value if start_value else 0
                            query = f"""
                                SELECT {fields} FROM {schema}.{table}
                                WHERE {table_info['partition_column']} >= {lower}
                                  AND {table_info['partition_column']} <= {value}
                            """
                        else:
                            prev_value = limit_values[limit - 1]
                            query = f"""
                                SELECT {fields} FROM {schema}.{table}
                                WHERE {table_info['partition_column']} > {prev_value}
                                  AND {table_info['partition_column']} <= {value}
                            """
                        try:
                            logger.info({query})
                            execute_data_multiple(
                                table_name = f"{table}", 
                                part = f"part_{limit}",
                                sql_script = query.strip(),
                                connectname = CONNECT_NAME
                                )
                            logger.info(f"Executed partitioned query {limit}: {query.strip()}")
                        except Exception as e:
                            logger.error(f"ERROR during partition {limit}: {e}")

                    # Final overflow query
                    last_value = limit_values[-1]
                    query = f"""
                        SELECT {fields} FROM {schema}.{table}
                        WHERE {table_info['partition_column']} > {last_value}
                    """
                    try:
                        logger.info({query})
                        execute_data_multiple(
                            table_name = f"{table}", 
                            part = f"part_{limit}_last",
                            sql_script = query.strip(),
                            connectname = CONNECT_NAME
                            )
                        logger.info(f"Executed overflow query: {query.strip()}")
                    except Exception as e:
                        logger.error(f"ERROR during final overflow: {e}")
