import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
connectame = "AlfaPRD_Test"
dbtable = "GENERAL.ACU_PERSONAS_JURIDICAS"  # Tabla base para metadatos
hashfield = "ROWID"

connection_oracle_options = {
    "useConnectionProperties": "True",
    "connectionName": connectame,
    "dbtable": dbtable,  # Necesario para metadatos
    "query": """SELECT 
                    NID_CONSECUTIVO||COD_TIPO_IDENTIFICACION||NID_NUMERO_IDENTIFICACION AS COMPOSITE_PK, 
                    t.* 
                FROM GENERAL.ACU_PERSONAS_JURIDICAS t 
                WHERE ROWNUM <= 10 
                AND $CONDITIONS""",
    "hashfield": hashfield,
    "enablePartitioningForSampleQuery": True,
    "oracle.net.keepAlive": True,
    "oracle.net.CONNECT_TIMEOUT": 5000,
    "oracle.jdbc.ReadTimeout": 5000
}
try:
    # Create dynamic frame
    print("Starting to read from Oracle...")
    df_oracle = glueContext.create_dynamic_frame.from_options(
        connection_type="oracle",
        connection_options=connection_oracle_options
    )
    # Count and print results
    count = df_oracle.count()
    print(df_oracle.show(10))
    print(f"Total records read: {count}")

except Exception as e:
    print(f"Error occurred: {str(e)}")
    raise e
## ----- GLUE LIBRARIES ------------------ ###
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (TimestampType, StructType, StructField, StringType, DateType)
from pyspark.sql.functions import (
    col, date_format, to_date, concat, lit, row_number, current_date
)
from pyspark.sql.window import Window
import boto3
from botocore.exceptions import ClientError
## ----- PYTHON EXTERNAL LIBRARIES ------------------ ###
import pandas as pd
import re
## ----- CUSTOM LIBRARIES ------------------ ###
from utils import Utils

class ExtractHelper():
    def __init__(self, glue_context: GlueContext, 
                       spark_session: SparkSession,
                       *args):
        """
        Initialize Extract Class.
       
        Args:
            glue_context (GlueContext): AWS Glue context for transformations
            spark_session (SparkSession): Spark session for data processing
            *args: Variable arguments containing BUCKET_NAME, DB_SCHEMA, DB_NAME
        """
        self._glue_context = glue_context
        self._spark = spark_session
       
        if len(args) < 3:
            raise ValueError("Missing required parameters: BUCKET_NAME, DB_SCHEMA, DB_NAME")
        # Load args
        (self.BUCKET_NAME, 
            self.DB_SCHEMA, 
            self.DB_NAME, 
            self.CONNECTION_NAME, 
            self.S3_PATH_PROCESS_TABLE_DEFINITIONS, 
            self.S3_PATH_SAVE_JSON_TABLE_DEFINITIONS
        ) = args
        self.s3_client = boto3.client('s3')
        self.utils = Utils()
        self.logger = Utils.logger_conf()

        #Initilize load table definitions
        self._load_table_definitions()
       
        # Initialize date components for partitioning
        current_datetime = datetime.now()
        self.year = current_datetime.strftime("%Y")
        self.month = current_datetime.strftime("%m")
        self.day = current_datetime.strftime("%d")
       
        self.logger.info(f"ExtractHelper initialized with bucket: {self.BUCKET_NAME}, schema: {self.DB_SCHEMA}, database: {self.DB_NAME}")

    def _load_table_definitions(self):
        """
        Load and process table definitions from Excel files stored in S3.
       
        Args:
            process_table_definitions (bool): Flag to determine if processing is needed
            s3_input_path (str): S3 path containing the input Excel file
            s3_target_path (str): S3 path where processed data will be stored
        """
        
        try:
            if not s3_input_path or not s3_target_path:
                raise ValueError("Input and target paths cannot be empty")
            
            self.logger.info(f"Init process to extract table definitions from {self.S3_PATH_PROCESS_TABLE_DEFINITIONS}")
            result = self.utils.extract_and_process_excel_data(self.S3_PATH_PROCESS_TABLE_DEFINITIONS, self.S3_PATH_SAVE_JSON_TABLE_DEFINITIONS)
            self.logger.info(f"Table definitions processed successfully.")
            
               
        except Exception as e:
            self.logger.error(f"Error loading table definitions: {str(e)}")
            raise
        
    def load_json_connection(self,) -> dict:
        #acssop_table_definitions.json
        """
        """
        json_definition = f'{self.DB.lower()}_table_definitions.json'
        json_file_full_path = f'{self.s3_target_path_table_definitions}/{json_definition}'
        try:
            response = self.utils._read_json_from_s3(s3_target_path=json_file_full_path)
            return response
        except Exception as e:
            print(e)
        


    def get_column_partition(self, df, partition_column: str):
        """
        Create partition column based on the data type of the specified column.
       
        Args:
            df: Spark DataFrame to add partition column to
            partition_column (str): Name of the column to use for partitioning
           
        Returns:
            DataFrame with added partition column
        """
        try:
            if not partition_column:
                raise ValueError("Partition column name cannot be empty")
           
            if partition_column not in df.columns:
                raise ValueError(f"Column '{partition_column}' not found in DataFrame")
           
            self.logger.info(f"Creating partition for column: {partition_column}")
           
            column_type = df.schema[partition_column].dataType

            if isinstance(column_type, (TimestampType, DateType)):
                # Case when partition is timestamp or date
                self.logger.info(f"Using date-based partition for column {partition_column}")
                df_with_partition = df.withColumn(f"{partition_column}_partition",
                                                date_format(col(partition_column), "yyyy-MM"))
            else:
                # Case when partition is string, number, etc - use current date
                self.logger.info(f"Using current date partition for non-date column {partition_column}")
                df_with_partition = df.withColumn(f"{partition_column}_partition",
                                                date_format(current_date(), "yyyy-MM"))

            return df_with_partition
           
        except Exception as e:
            self.logger.error(f"Error creating partition column: {str(e)}")
            raise

    def read_delta_cardiff(self, schema: StructType, path_file: str):
        """
        Read Cardiff delta files with specific schema and delimiter.
       
        Args:
            schema (StructType): Schema definition for the CSV file
            path_file (str): S3 path to the CSV file
           
        Returns:
            DataFrame with first row removed (header row)
        """
        try:
            if not path_file:
                raise ValueError("Path file cannot be empty")
           
            if not schema:
                raise ValueError("Schema cannot be None")
           
            self.logger.info(f"Reading Cardiff delta file from: {path_file}")
           
            # 1. read dataframe
            df_cardiff_delta = self._spark.read \
                        .option("header", False) \
                        .schema(schema) \
                        .option("delimiter", "|") \
                        .csv(path_file)
           
            # 2. delete first row (header)
            window = Window.orderBy(lit(1))
            df_cardiff_delta = df_cardiff_delta.withColumn("row_number", row_number().over(window)) \
                                                .filter("row_number > 1") \
                                                .drop("row_number")

            self.logger.info(f"Successfully read Cardiff delta file. Row count: {df_cardiff_delta.count()}")
            return df_cardiff_delta
           
        except Exception as e:
            self.logger.error(f"Error reading Cardiff delta file: {str(e)}")
            raise

    def get_latest_dom_customers_report_file(self, s3_input_path: str) -> str:
        """
        Find the latest DOM_CUSTOMERS report file in the specified S3 path.
       
        Args:
            s3_input_path (str): S3 path to search for DOM_CUSTOMERS files
           
        Returns:
            str: Full S3 path to the latest DOM_CUSTOMERS file
           
        Raises:
            Exception: If no DOM_CUSTOMERS file is found
        """
        try:
            if not s3_input_path:
                raise ValueError("S3 input path cannot be empty")
           
            self.logger.info(f"Searching for DOM_CUSTOMERS file in: {s3_input_path}")
           
            s3_path_clean = s3_input_path.replace('s3://', '').rstrip('/')
            s3_parts = s3_path_clean.split('/', 1)
            bucket = s3_parts[0]
            prefix = s3_parts[1] + '/' if len(s3_parts) > 1 else ''
           
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

            pattern = re.compile(r'DOM_CUSTOMERS_\d{8}\.txt$')

            for obj in response.get('Contents', []):
                if pattern.search(obj['Key']):
                    file_path = f"s3://{bucket}/{obj['Key']}"
                    self.logger.info(f"Found DOM_CUSTOMERS file: {file_path}")
                    return file_path

            raise Exception(f"No DOM_CUSTOMERS file found in {s3_input_path}")
           
        except Exception as e:
            self.logger.error(f"Error finding DOM_CUSTOMERS file: {str(e)}")
            raise

    def read_cardiff_files(self, load_historic: bool, s3_input_path: str, cardiff_columns: list):
        """
        Read Cardiff files either historic or delta based on the load_historic flag.
       
        Args:
            load_historic (bool): Flag to determine if historic data should be loaded
            s3_input_path (str): S3 path containing Cardiff files
            cardiff_columns (list): List of column names for the Cardiff schema
           
        Returns:
            DataFrame containing Cardiff data
        """
        try:
            if not isinstance(load_historic, bool):
                raise TypeError("load_historic must be a boolean")
           
            if not s3_input_path:
                raise ValueError("S3 input path cannot be empty")
           
            if not cardiff_columns or not isinstance(cardiff_columns, list):
                raise ValueError("Cardiff columns must be a non-empty list")
           
            self.logger.info(f"Reading Cardiff files. Historic mode: {load_historic}")
           
            if load_historic:
                historic_filename = f"{s3_input_path}/DOM_CUSTOMERS_FULL.txt"
                self.logger.info(f"Reading historic file: {historic_filename}")
                df_cardiff = self._spark.read.options(delimiter='|', header=True).csv(historic_filename)
                self.logger.info("Cardiff historic files processed successfully")
            else:
                schema_cardiff = StructType([StructField(col, StringType(), True) for col in cardiff_columns])
                cardiff_path = self.get_latest_dom_customers_report_file(s3_input_path)
                df_cardiff = self.read_delta_cardiff(schema=schema_cardiff, path_file=cardiff_path)
                self.logger.info("Cardiff delta files processed successfully")
               
            # Save processed files
            s3_prefix = f"{self.DB_NAME}/{self.DB_SCHEMA}/cardiff_processed/"
            s3_path = f"s3://{self.BUCKET_NAME}/{s3_prefix}"
            df_dynamic = DynamicFrame.fromDF(df_cardiff, self._glue_context, "cardiff_processed")
            self.save_bucket_files(df_dynamic, s3_path)

            self.logger.info("Cardiff load succesfully")
               
        except Exception as e:
            self.logger.error(f"Error reading Cardiff files: {str(e)}")
            raise

    def read_from_database(self, table_name: str, partition_column: str = "", sql_script: str = "", connectname: str = ""):
        """
        Read data from Oracle database using AWS Glue connection.
       
        Args:
            table_name (str): Name of the table to read from
            partition_column (str, optional): Column to use for partitioning
            sql_script (str, optional): Custom SQL script to execute
            connectname (str, optional): AWS Glue connection name
           
        Returns:
            DynamicFrame containing the extracted data
        """
        try:
            if not table_name:
                raise ValueError("Table name cannot be empty")
           
            if not connectname:
                raise ValueError("Connection name cannot be empty")
           
            self.logger.info(f"Starting database extraction for table: {table_name}")
           
            # 1. Set glue config operations
            self._spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            self._spark._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

            # 2. Validation query statement
            if not sql_script:
                sql_script = f"SELECT * FROM {self.DB_SCHEMA}.{table_name};"
                self.logger.info(f"Using default query: {sql_script}")
           
            # 3. Test connection and extract data
            # 3.1. Set connection properties
            connection_options = {
                "useConnectionProperties": "true",
                "dbtable": f"{self.DB_SCHEMA}.{table_name}",
                "connectionName": connectname
            }
           
            self.logger.info(f"Connecting to database with connection: {connectname}")
           
            # 3.2 Get result from connection
            df_dynamic = self._glue_context.create_dynamic_frame.from_options(
                         connection_type="oracle",
                         connection_options=connection_options,
                         transformation_ctx="TablaExtract"
                         )
           
            # 3.3 Set validation for partitions
            partition_keys = []
            df_save = df_dynamic
           
            if partition_column and partition_column.strip():
                self.logger.info(f"Adding partition column: {partition_column}")
                df = df_dynamic.toDF()
                df_partitioned = self.get_column_partition(df, partition_column)
                df_save = DynamicFrame.fromDF(df_partitioned, self._glue_context, "frame_save_dynamic")
                partition_keys = [f"{partition_column}_partition"]
           
            # 3.4 Set prefix to save in bucket
            s3_prefix = f"{self.DB_NAME}/{self.DB_SCHEMA}/{table_name}/"
            s3_target_path = f"s3://{self.BUCKET_NAME}/{s3_prefix}"
           
            # 3.5 Delete the current files
            self.logger.info(f"Deleting existing files in: {s3_prefix}")
            self.delete_bucket_files(s3_prefix)
           
            # 3.6 Save the current files
            self.logger.info(f"Saving files to: {s3_target_path}")
            self.save_bucket_files(df_save, s3_target_path, partition_keys=partition_keys)
           
            # 3.7 Return dataframe
            self.logger.info(f"Database extraction completed successfully for table: {table_name}")
            return df_save
           
        except Exception as e:
            self.logger.error(f"Error reading from database table {table_name}: {str(e)}")
            raise

    def delete_bucket_files(self, prefix: str):
        """
        Delete all files in the specified S3 bucket prefix.
       
        Args:
            prefix (str): S3 prefix path where files should be deleted
        """
        try:
            if not prefix:
                raise ValueError("Prefix cannot be empty")
           
            self.logger.info(f"Deleting files with prefix: {prefix}")
           
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(self.BUCKET_NAME)
           
            objects_to_delete = list(bucket.objects.filter(Prefix=prefix))
           
            if objects_to_delete:
                bucket.objects.filter(Prefix=prefix).delete()
                self.logger.info(f"Deleted {len(objects_to_delete)} objects from {prefix}")
            else:
                self.logger.info(f"No objects found to delete in {prefix}")
               
        except ClientError as e:
            self.logger.error(f"AWS S3 client error while deleting files: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error deleting bucket files: {str(e)}")
            raise

    def save_bucket_files(self, df, s3_target_path: str, format: str = "parquet", mode: str = "overwrite", partition_keys: list = None):
        """
        Save DataFrame to S3 bucket in the specified format.
       
        Args:
            df: DataFrame or DynamicFrame to save
            s3_target_path (str): Target S3 path for saving files
            format (str, optional): File format. Defaults to "parquet"
            mode (str, optional): Save mode. Defaults to "overwrite"
            partition_keys (list, optional): List of partition columns
           
        Returns:
            Response from the write operation
        """
        try:
            if not s3_target_path:
                raise ValueError("S3 target path cannot be empty")
           
            if partition_keys is None:
                partition_keys = []
           
            self.logger.info(f"Saving files to: {s3_target_path} with format: {format}")
           
            # Convert to DynamicFrame if needed
            if hasattr(df, 'toDF'):  # Already a DynamicFrame
                df_dynamic = df
            else:  # Regular DataFrame
                df_dynamic = DynamicFrame.fromDF(df, self._glue_context, "nested")
           
            # Prepare connection options
            connection_options = {
                "path": f"{s3_target_path}{self.year}/{self.month}/{self.day}",
                "partitionKeys": partition_keys
            }
           
            response = self._glue_context.write_dynamic_frame.from_options(
                frame=df_dynamic,
                connection_type="s3",
                format="glueparquet",
                connection_options=connection_options,
                format_options={"compression": "snappy"},
                transformation_ctx="AmazonS3_write"
            )
           
            self.logger.info(f"Files saved successfully to {s3_target_path}")
            return response
           
        except Exception as e:
            self.logger.error(f"Error saving files to S3: {str(e)}")
            raise

    def read_files_from_s3(self, s3_input_path: str, filename: str, file_format: str = "csv", delimiter: str = "|", header: bool = True):
        """
        Read files from S3 with specified format and options.
       
        Args:
            s3_input_path (str): S3 path containing the file
            filename (str): Name of the file to read
            file_format (str, optional): Format of the file. Defaults to "csv"
            delimiter (str, optional): Delimiter for CSV files. Defaults to "|"
            header (bool, optional): Whether file has header. Defaults to True
           
        Returns:
            DataFrame containing the file data
        """
        try:
            if not s3_input_path or not filename:
                raise ValueError("S3 input path and filename cannot be empty")
           
            full_path = f"{s3_input_path.rstrip('/')}/{filename}"
            self.logger.info(f"Reading file: {full_path} with format: {file_format}")
           
            if file_format.lower() == "csv":
                df = self._spark.read \
                    .option("header", header) \
                    .option("delimiter", delimiter) \
                    .csv(full_path)
            elif file_format.lower() == "parquet":
                df = self._spark.read.parquet(full_path)
            elif file_format.lower() == "json":
                df = self._spark.read.json(full_path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
           
            self.logger.info(f"Successfully read file. Row count: {df.count()}")
            return df
           
        except Exception as e:
            self.logger.error(f"Error reading file from S3: {str(e)}")
            raise
### test params
BUCKET_NAME_BRONZE = 'alfa-datalake-dev-assets'
DB_SCHEMA = 'ACSELE_ALFA'
DB_NAME = 'ACSSOP'
CONNECTION_NAME = 'datalake-dev-acsele-qa-connection'
S3_PATH_PROCESS_TABLE_DEFINITIONS = 's3://alfa-datalake-dev-assets/CLIENTS_ASSETS/config/table_definition_extraction_clients.xlsx'
S3_PATH_SAVE_JSON_TABLE_DEFINITIONS = 's3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_bronze/definitions/clients_table_definitions'
#BUCKET_NAME = 
#DB_SCHEMA = 'FILES'
#DB_NAME = 'CARDIFF'
args = [BUCKET_NAME_BRONZE, DB_SCHEMA, DB_NAME, CONNECTION_NAME, S3_PATH_PROCESS_TABLE_DEFINITIONS, S3_PATH_SAVE_JSON_TABLE_DEFINITIONS]
extract_helper = ExtractHelper(glueContext, spark, *args)
job.commit()