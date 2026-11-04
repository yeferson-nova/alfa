## ----- PYTHON EXTERNAL LIBRARIES ------------------ ###
from datetime import datetime
import boto3
#-------SPARK--------------#
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat, lit
#------ AWS GLUE ---------#
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
## ----- CUSTOM LIBRARIES ------------------ ###
from logger_config import logger
from utils import Utils
from s3_handler import S3Handler
from file_processor import FileProcessor
from data_transformer import DataTransformer
from load_strategy_manager import LoadStrategyManager
from incremental_loader import IncrementalLoader
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
class ExtractHelperv2:
    """
    Orchestrator class for data extraction operations in AWS Glue environment.
   
    CORE RESPONSIBILITY: Database extraction orchestration only.
    Delegates file processing, S3 operations, and transformations to specialized classes.
    
    This refactored class follows single responsibility principle and delegates
    specialized operations to dedicated handler classes for better maintainability.
    """
   
    def __init__(self, glue_context: GlueContext, spark_session: SparkSession, *args):
        """
        Initialize ExtractHelper with AWS Glue context and specialized handlers.
       
        Args:
            glue_context (GlueContext): AWS Glue context for data transformations
            spark_session (SparkSession): Spark session for distributed data processing
            *args: Configuration parameters (BUCKET_ASSETS, BUCKET_TARGET, DB_SCHEMA, 
                   DB_NAME, CONNECTION_NAME, S3_PATH_PROCESS_TABLE_DEFINITIONS, 
                   S3_PATH_SAVE_JSON_TABLE_DEFINITIONS)
       
        Raises:
            ValueError: If required parameters are missing
        """
        self._glue_context = glue_context
        self._spark = spark_session
       
        if len(args) < 7:
            raise ValueError("Missing required parameters: BUCKET_ASSETS, BUCKET_TARGET, DB_SCHEMA, DB_NAME, CONNECTION_NAME, S3_PATH_PROCESS_TABLE_DEFINITIONS, S3_PATH_SAVE_JSON_TABLE_DEFINITIONS")
       
        # Load configuration parameters
        (self.BUCKET_ASSETS,
         self.BUCKET_TARGET,
         self.DB_SCHEMA,
         self.DB_NAME,
         self.CONNECTION_NAME,
         self.S3_PATH_PROCESS_TABLE_DEFINITIONS,
         self.S3_PATH_SAVE_JSON_TABLE_DEFINITIONS) = args
       
        # Initialize utilities
        self.utils = Utils()
        
        # Initialize specialized handlers (DEPENDENCY INJECTION)
        self.s3_handler = S3Handler(self.BUCKET_TARGET, self._glue_context)
        self.data_transformer = DataTransformer(self._spark, self._glue_context)
        self.file_processor = FileProcessor(self._spark, self.utils, self.data_transformer, self.s3_handler, self.DB_NAME)
        
        # Initialize core functionality
        self._load_table_definitions()
        self._initialize_date_components()
        self.load_strategy = LoadStrategyManager(self.DB_NAME)
       
        logger.info("ExtractHelper initialized with specialized handlers", "ExtractHelper")
        logger.info(f"Database: {self.DB_NAME}, Schema: {self.DB_SCHEMA}", "ExtractHelper")

    def _initialize_date_components(self) -> None:
        """
        Initialize date components used for data partitioning.
        """
        current_datetime = datetime.now()
        self.year = current_datetime.strftime("%Y")
        self.month = current_datetime.strftime("%m")
        self.day = current_datetime.strftime("%d")
 
    def _load_table_definitions(self) -> None:
        """
        Load and process table definitions from Excel files stored in S3.
        """
        try:
            if not self.S3_PATH_PROCESS_TABLE_DEFINITIONS or not self.S3_PATH_SAVE_JSON_TABLE_DEFINITIONS:
                raise ValueError("Input and target paths cannot be empty")
           
            logger.info(f"Initializing table definitions extraction from {self.S3_PATH_PROCESS_TABLE_DEFINITIONS}", "ExtractHelper")
            self.utils.extract_and_process_excel_data(
                self.S3_PATH_PROCESS_TABLE_DEFINITIONS,
                self.S3_PATH_SAVE_JSON_TABLE_DEFINITIONS
            )
            logger.success("Table definitions processed successfully", "ExtractHelper")
           
        except Exception as e:
            logger.error(f"Error loading table definitions: {str(e)}", "ExtractHelper")
            raise
       
    def load_json_connection(self) -> dict:
        """
        Load JSON connection configuration for the current database and schema.
        
        Returns:
            dict: Dictionary containing table definitions and connection metadata
        """
        json_definition_filename = f'{self.DB_NAME.lower()}_{self.DB_SCHEMA.lower()}_table_definitions.json'
        json_file_full_path = f'{self.S3_PATH_SAVE_JSON_TABLE_DEFINITIONS}/{json_definition_filename}'
       
        try:
            logger.info(f"Loading JSON connection from: {json_file_full_path}", "ExtractHelper")
            response = self.utils._read_json_from_s3(s3_target_path=json_file_full_path)
            logger.success("JSON connection loaded successfully", "ExtractHelper")
            return response
        except Exception as e:
            logger.error(f"Error loading JSON connection: {str(e)}", "ExtractHelper")
            raise
            
    # ================================
    # CORE DATABASE OPERATIONS
    # ================================
    
    def _create_connection_options(self, table_name: str, sql_script: str, connectname: str, 
                                   primary_key_column: str = 'PK', enable_partitioning: bool = False,
                                   partition_column: str = None, lower_bound: int = None, 
                                   upper_bound: int = None, num_partitions: int = 10) -> dict:
        """
        Create connection options dictionary for database operations with enterprise-grade configuration.
        
        IMPROVEMENTS v5.0.0:
        - Increased timeout from 5s to 30 minutes (1800000ms) for complex queries
        - Added fetchsize=10000 for streaming data processing
        - Support for automatic parallelization with partitionColumn
        
        Args:
            table_name (str): Target table name
            sql_script (str): SQL script for data extraction
            connectname (str): Connection name
            primary_key_column (str): Primary Key column(s) - comma separated if composite
            enable_partitioning (bool): Enable automatic parallelization for large datasets
            partition_column (str): Numeric column for data partitioning (e.g., 'ID')
            lower_bound (int): Minimum value for partitioning
            upper_bound (int): Maximum value for partitioning
            num_partitions (int): Number of parallel workers (default: 10)
        
        Returns:
            dict: Connection options for Glue with optimized configuration
        """
        pk_columns = [col.strip() for col in primary_key_column.split(',')]
        
        if len(pk_columns) > 1:
            hashfield_value = "ROWID"
            logger.info(f"Composite primary key detected: {pk_columns}. Using ROWID as hashfield.", "ExtractHelper")
        else:
            hashfield_value = primary_key_column.strip()
            logger.info(f"Single primary key: {hashfield_value}", "ExtractHelper")

        # Base configuration with IMPROVED timeouts and streaming
        connection_options = {
            "useConnectionProperties": "True",
            "connectionName": connectname,
            "dbtable": f"{self.DB_SCHEMA}.{table_name}",
            "hashfield": hashfield_value,
            "sampleQuery": sql_script,
            "enablePartitioningForSampleQuery": True,
            "oracle.net.keepAlive": True,
            "oracle.net.CONNECT_TIMEOUT": 60000,      # 60 seconds connection timeout
            "oracle.jdbc.ReadTimeout": 1800000,       # 30 minutes read timeout (FIXED)
            "fetchsize": 10000                        # Stream data in chunks of 10k rows (NEW)
        }
        
        # Add parallelization if enabled
        if enable_partitioning and partition_column:
            connection_options.update({
                "partitionColumn": partition_column,
                "lowerBound": str(lower_bound) if lower_bound is not None else "1",
                "upperBound": str(upper_bound) if upper_bound is not None else "1000000",
                "numPartitions": str(num_partitions)
            })
            logger.info(f"Parallelization enabled: {num_partitions} workers on column '{partition_column}'", "ExtractHelper")
        
        return connection_options
    
    def _create_composite_pk_column(self, df_dynamic, primary_key_column: str):
        """
        Create COMPOSITE_PK column for tables with composite primary keys.
        
        Args:
            df_dynamic: DynamicFrame from database extraction
            primary_key_column (str): Comma-separated primary key columns
            
        Returns:
            DynamicFrame: DataFrame with COMPOSITE_PK column added if applicable
        """
        pk_columns = [col.strip() for col in primary_key_column.split(',')]
        
        if len(pk_columns) > 1:
            logger.info(f"Creating COMPOSITE_PK from columns: {pk_columns}", "ExtractHelper")
            
            df_spark = df_dynamic.toDF()
            
            missing_columns = [col for col in df_spark.columns if col not in pk_columns]
            if missing_columns:
                raise ValueError(f"Primary key columns not found in DataFrame: {missing_columns}")
            
            df_with_composite = df_spark.withColumn(
                "COMPOSITE_PK",
                F.concat(*[F.coalesce(F.col(col), F.lit("")) for col in pk_columns])
            )
            
            df_result = DynamicFrame.fromDF(df_with_composite, self._glue_context, "composite_pk_frame")
            
            logger.success("COMPOSITE_PK column created successfully", "ExtractHelper")
            return df_result
        else:
            logger.info("Single primary key - no COMPOSITE_PK column needed", "ExtractHelper")
            return df_dynamic
                 
    def read_from_database(self, table_name: str, partition_column: str = "", 
                          sql_script: str = "", connectname: str = "", 
                          primary_key_column: str = "PK", table_info: dict = None,
                          enable_db_partitioning: bool = False, db_partition_column: str = None,
                          lower_bound: int = None, upper_bound: int = None, num_partitions: int = 10):
        """
        CORE FUNCTION: Extract data from Oracle database using AWS Glue connection.
        
        IMPROVEMENTS v5.0.0:
        - Enterprise-grade timeout configuration (30 minutes)
        - Streaming data processing with fetchsize
        - Optional automatic parallelization for large datasets
        
        Args:
            table_name (str): Name of the database table to extract
            partition_column (str, optional): Column name for S3 data partitioning
            sql_script (str, optional): Custom SQL query for data filtering
            connectname (str, optional): AWS Glue connection name for database access
            primary_key_column (str, optional): Primary key column(s) for hashfield
            table_info (dict, optional): Table configuration info for strategy decisions
            enable_db_partitioning (bool, optional): Enable parallel extraction from database
            db_partition_column (str, optional): Numeric column for parallel extraction (e.g., 'ID')
            lower_bound (int, optional): Minimum value for parallel extraction
            upper_bound (int, optional): Maximum value for parallel extraction
            num_partitions (int, optional): Number of parallel workers (default: 10)
        
        Returns:
            DynamicFrame: Extracted and processed data ready for further transformations
        
        Example with parallelization:
            df = extract_helper.read_from_database(
                table_name="LARGE_TABLE",
                enable_db_partitioning=True,
                db_partition_column="ID",
                lower_bound=1,
                upper_bound=10000000,
                num_partitions=20  # 20 workers processing in parallel
            )
        """
        try:
            if not table_name:
                raise ValueError("Table name cannot be empty")
        
            if not connectname:
                raise ValueError("Connection name cannot be empty")
        
            logger.info(f"Starting database extraction for table: {table_name}", "ExtractHelper")

            # Clean and prepare SQL script
            sql_script_cleaned = self.utils._clean_sql_script(sql_script, table_name, db_schema=self.DB_SCHEMA)
        
            # Create connection options with IMPROVED configuration
            connection_options = self._create_connection_options(
                table_name, sql_script_cleaned, connectname, primary_key_column,
                enable_partitioning=enable_db_partitioning,
                partition_column=db_partition_column,
                lower_bound=lower_bound,
                upper_bound=upper_bound,
                num_partitions=num_partitions
            )
        
            logger.info(f"Connecting to database with connection: {connectname}", "ExtractHelper")
            if enable_db_partitioning:
                logger.info(f"Parallel extraction enabled: {num_partitions} workers", "ExtractHelper")
        
            # CORE RESPONSIBILITY: Extract data from database
            df_dynamic = self._glue_context.create_dynamic_frame.from_options(
                        connection_type="oracle",
                        connection_options=connection_options,
                        transformation_ctx=f"extract_{self.DB_SCHEMA}_{table_name}"
                        )
            
            # Create COMPOSITE_PK column if needed
            df_dynamic = self._create_composite_pk_column(df_dynamic, primary_key_column)
        
            # DELEGATE partitioning to DataTransformer
            partition_keys = []
            df_save = df_dynamic
        
            if partition_column and partition_column.strip() and partition_column != 'NA':
                logger.info(f"Delegating partitioning to DataTransformer: {partition_column}", "ExtractHelper")
                df = df_dynamic.toDF()
                df_partitioned = self.data_transformer.create_partition_column(df, partition_column)
                df_save = DynamicFrame.fromDF(df_partitioned, self._glue_context, "frame_save_dynamic")
                partition_keys = [f"{partition_column}_partition"]
            
            # DELEGATE S3 operations to S3Handler with strategy decision
            s3_prefix, s3_target_path = self.s3_handler.generate_s3_paths(self.DB_NAME, self.DB_SCHEMA, table_name)
        
            # NEW: INCREMENTAL vs TRADITIONAL strategy decision
            if table_info and self.load_strategy.should_use_incremental_loading(table_info):
                logger.info("Database INCREMENTAL: Using IncrementalLoader approach", "ExtractHelper")
                
                # Use IncrementalLoader for database INCREMENTAL loading
                incremental_loader = IncrementalLoader(self._spark, self._glue_context, self.s3_handler)
                
                summary = incremental_loader.load_incremental_data_with_ids(
                    df=df_save.toDF(),
                    key_columns=None,
                    layer="BRONZE",
                    table_name=f"{self.DB_SCHEMA}_{table_name}",
                    bronze_path=s3_target_path,
                    business_key_columns=None,
                    partition_keys=partition_keys,
                    table_info=table_info
                )
                
                logger.info(f"Database INCREMENTAL processing completed: {summary}", "ExtractHelper")
                return df_save
                
            else:
                # Traditional strategy
                logger.info("Using traditional loading strategy", "ExtractHelper")
                
                if table_info:
                    self.load_strategy.log_strategy_decision(table_info)
                    
                    if self.load_strategy.should_delete_existing_files(table_info):
                        logger.info("Strategy decision: DELETE existing files before load", "ExtractHelper")
                        self.s3_handler.delete_bucket_files(s3_prefix)
                    else:
                        logger.info("Strategy decision: PRESERVE existing files (append mode)", "ExtractHelper")
                else:
                    logger.info("No table_info provided, using default DELETE strategy", "ExtractHelper")
                    self.s3_handler.delete_bucket_files(s3_prefix)
                
                load_mode = self.load_strategy.get_load_mode(table_info) if table_info else "overwrite"
                self.s3_handler.save_files_to_s3(df_save, s3_target_path, mode=load_mode, partition_keys=partition_keys)
                
                logger.success(f"Database extraction completed successfully for table: {table_name}", "ExtractHelper")
                return df_save
        
        except Exception as e:
            logger.error(f"Error reading from database table {table_name}: {str(e)}", "ExtractHelper")
            raise
                
 
    # ================================
    # FILE PROCESSING ORCHESTRATION - REFACTORED v5.0.0
    # ================================
    
    def _is_cardiff_processing(self) -> bool:
        """Check if current processing is for Cardiff database."""
        return self.DB_NAME.upper() == 'CARDIFF'
    
    def _process_cardiff_file(self, s3_input_path: str, type_load: str, 
                             cardiff_columns: list, table_info: dict, partition_keys: list):
        """
        Process Cardiff-specific files with optional incremental loading.
        
        Args:
            s3_input_path (str): Base S3 path where files are located
            type_load (str): Load type for Cardiff ('INITIAL' or 'DELTA')
            cardiff_columns (list): Column definitions for Cardiff files
            table_info (dict): Table configuration for strategy decisions
            partition_keys (list): Partition keys for data organization
            
        Returns:
            DataFrame: Processed Spark DataFrame
        """
        logger.info("Processing Cardiff file", "ExtractHelper")
        df_cardiff = self._cardiff_incremental_load(
            s3_input_path, type_load, table_info, cardiff_columns, partition_keys
        )
        return df_cardiff
    
    def _process_standard_file(self, s3_input_path: str, filename: str, sheet: str,
                               delimiter: str, header: bool, table_info: dict):
        """
        Process standard (non-Cardiff) files.
        
        Args:
            s3_input_path (str): Base S3 path where files are located
            filename (str): Specific filename
            sheet (str): Excel sheet name
            delimiter (str): Field delimiter for CSV files
            header (bool): Whether file contains header row
            table_info (dict): Table configuration
            
        Returns:
            DataFrame: Processed Spark DataFrame
        """
        logger.info("Processing standard file", "ExtractHelper")
        df_spark = self.file_processor.read_standard_file(
            s3_input_path, filename, sheet, delimiter, header, table_info
        )
        return df_spark
    
    def _apply_partitioning(self, df_spark, partition_column: str):
        """
        Apply partitioning transformation to DataFrame if specified.
        
        Args:
            df_spark: Spark DataFrame
            partition_column (str): Column name for data partitioning
            
        Returns:
            tuple: (DataFrame, list of partition keys)
        """
        partition_keys = []
        if partition_column and partition_column.strip() and partition_column.upper() != 'NA':
            logger.info(f"Applying partitioning on column: {partition_column}", "ExtractHelper")
            df_spark = self.data_transformer.create_partition_column(df_spark, partition_column)
            partition_keys = [f"{partition_column}_partition"]
        return df_spark, partition_keys
    
    def _extract_table_name(self, filename: str) -> str:
        """Extract table name from filename."""
        table_name = filename.split('.')[0] if filename and '.' in filename else "processed_data"
        return table_name
    
    def _prepare_for_save(self, df_spark, table_name: str):
        """
        Prepare DataFrame and S3 paths for saving.
        
        Args:
            df_spark: Spark DataFrame
            table_name (str): Name of the table
            
        Returns:
            tuple: (DynamicFrame, S3 path)
        """
        df_dynamic = DynamicFrame.fromDF(df_spark, self._glue_context, "processed_data")
        s3_prefix, s3_path = self.s3_handler.generate_s3_paths(self.DB_NAME, self.DB_SCHEMA, table_name)
        return df_dynamic, s3_path
    
    def _execute_load_strategy(self, df_dynamic, df_spark, s3_path: str, partition_keys: list,
                               table_info: dict, table_name: str):
        """
        Execute appropriate load strategy (INCREMENTAL or TRADITIONAL).
        
        Args:
            df_dynamic: DynamicFrame for traditional loading
            df_spark: Spark DataFrame for incremental loading
            s3_path (str): Target S3 path
            partition_keys (list): Partition keys
            table_info (dict): Table configuration
            table_name (str): Name of the table
        """
        if table_info and self.load_strategy.should_use_incremental_loading(table_info):
            self._execute_incremental_load(df_spark, s3_path, partition_keys, table_info, table_name)
        else:
            self._execute_traditional_load(df_dynamic, s3_path, partition_keys, table_info)
    
    def _execute_incremental_load(self, df_spark, s3_path: str, partition_keys: list,
                                  table_info: dict, table_name: str):
        """Execute incremental loading strategy."""
        logger.info("Executing INCREMENTAL loading strategy", "ExtractHelper")
        
        incremental_loader = IncrementalLoader(self._spark, self._glue_context, self.s3_handler)
        
        summary = incremental_loader.load_incremental_data_with_ids(
            df=df_spark,
            key_columns=None,
            layer="BRONZE",
            table_name=table_name,
            bronze_path=s3_path,
            business_key_columns=None,
            partition_keys=partition_keys,
            table_info=table_info
        )
        
        logger.info(f"INCREMENTAL file loading completed: {summary}", "ExtractHelper")
    
    def _execute_traditional_load(self, df_dynamic, s3_path: str, partition_keys: list, table_info: dict):
        """Execute traditional loading strategy."""
        logger.info("Executing traditional loading strategy", "ExtractHelper")
        
        s3_prefix = s3_path.replace(f"s3://{self.BUCKET_TARGET}/", "")
        
        if table_info:
            self.load_strategy.log_strategy_decision(table_info)
            
            if self.load_strategy.should_delete_existing_files(table_info):
                logger.info("Strategy: DELETE existing files before load", "ExtractHelper")
                self.s3_handler.delete_bucket_files(s3_prefix)
            else:
                logger.info("Strategy: PRESERVE existing files (append mode)", "ExtractHelper")
            
            load_mode = self.load_strategy.get_load_mode(table_info)
        else:
            logger.info("No table_info provided, using default strategy", "ExtractHelper")
            self.s3_handler.delete_bucket_files(s3_prefix)
            load_mode = "overwrite"
        
        self.s3_handler.save_files_to_s3(df_dynamic, s3_path, mode=load_mode, partition_keys=partition_keys)
 

    def read_and_save_file(self, s3_input_path: str, filename: str = None, sheet: str = None, 
                          delimiter: str = ",", header: bool = True, partition_column: str = None,
                          type_load: str = None, cardiff_columns: list = None, table_info: dict = None):
        """
        ORCHESTRATION FUNCTION: Coordinates file processing using specialized handlers.
        
        REFACTORED v5.0.0:
        - Reduced cognitive complexity from 21 to 3
        - Extracted 7 specialized methods following Single Responsibility Principle
        - Improved maintainability and testability
        
        Args:
            s3_input_path (str): Base S3 path where files are located
            filename (str, optional): Specific filename for standard files
            sheet (str, optional): Excel sheet name. If None/empty, treats as flat file
            delimiter (str, optional): Field delimiter for CSV files
            header (bool, optional): Whether file contains header row
            partition_column (str, optional): Column name for data partitioning
            type_load (str, optional): Load type for Cardiff ('INITIAL' or 'DELTA')
            cardiff_columns (list, optional): Column definitions for Cardiff files
            table_info (dict, optional): Table configuration for strategy decisions

        Returns:
            DataFrame: Processed Spark DataFrame
        """
        try:
            logger.info(f"Starting file processing orchestration for DB: {self.DB_NAME}", "ExtractHelper")

            # Step 1: Cardiff vs Standard processing
            if self._is_cardiff_processing():
                partition_keys = []
                return self._process_cardiff_file(s3_input_path, type_load, cardiff_columns, 
                                                  table_info, partition_keys)
            
            # Step 2: Process standard file
            df_spark = self._process_standard_file(s3_input_path, filename, sheet, 
                                                   delimiter, header, table_info)
            
            # Step 3: Apply partitioning
            df_spark, partition_keys = self._apply_partitioning(df_spark, partition_column)
            
            # Step 4: Extract table name
            table_name = self._extract_table_name(filename)
            
            # Step 5: Prepare for save
            df_dynamic, s3_path = self._prepare_for_save(df_spark, table_name)
            
            # Step 6: Execute load strategy
            self._execute_load_strategy(df_dynamic, df_spark, s3_path, partition_keys, 
                                       table_info, table_name)
            
            logger.success("File processing orchestration completed successfully", "ExtractHelper")
            return df_spark

        except Exception as e:
            logger.error(f"Error in file processing orchestration: {str(e)}", "ExtractHelper")
            raise

    def _cardiff_incremental_load(self, s3_input_path: str, type_load: str = None,
                                 table_info: dict = None, cardiff_columns: list = None,
                                 partition_keys: list = None):
        """
        Implements incremental load if the origin is CARDIFF.
        
        Args:
            s3_input_path (str): Base S3 path where files are located
            type_load (str, optional): Load type for Cardiff ('INITIAL' or 'DELTA')
            table_info (dict, optional): Table configuration for strategy decisions
            cardiff_columns (list, optional): Column definitions for Cardiff files
            partition_keys (list, optional): Partition keys for data organization

        Returns:
            DataFrame: Processed Spark DataFrame
        """
        # Check if Cardiff should use INCREMENTAL
        if table_info and self.load_strategy.should_use_incremental_loading(table_info):
            
            logger.info("Cardiff INCREMENTAL: Using hybrid Cardiff + IncrementalLoader approach", "ExtractHelper")
            
            # Step 1: Use Cardiff-specific file logic to READ the data
            df_cardiff = self.file_processor.read_cardiff_file_data_only(
                s3_input_path, type_load, cardiff_columns, table_info
            )

            # Step 2: Get Cardiff-specific paths
            s3_prefix = f"CARDIFF/SMB_CARDIFF_CUSTOMER/"
            s3_path = f"s3://{self.BUCKET_TARGET}/{s3_prefix}"

            # Step 3: Configure Cardiff-specific parameters for INCREMENTAL
            cardiff_config = self._get_cardiff_incremental_config(partition_keys)    
            logger.info("Cardiff INCREMENTAL configuration:", "ExtractHelper")
            logger.info(f"  - Partition keys: {cardiff_config['partition_keys']}", "ExtractHelper")

            # Step 4: Use IncrementalLoader with table_info configuration
            incremental_loader = IncrementalLoader(self._spark, self._glue_context, self.s3_handler)

            summary = incremental_loader.load_incremental_data_with_ids(
                df=df_cardiff,
                key_columns=None,
                layer="BRONZE",
                table_name="SMB_CARDIFF_CUSTOMER",
                bronze_path=s3_path,
                business_key_columns=None,
                partition_keys=cardiff_config.get('partition_keys', []),
                table_info=table_info 
            )
            
            # Step 5: Cardiff file movement (delegate to FileProcessor)
            self.file_processor.move_cardiff_processed_file(s3_input_path, type_load)

            logger.info(f"Cardiff INCREMENTAL processing completed: {summary}", "ExtractHelper")
            return df_cardiff

        else:
            # Traditional Cardiff flow (INITIAL or DELTA without INCREMENTAL_ID)
            df_spark = self.file_processor.read_cardiff_file(s3_input_path, type_load, cardiff_columns, table_info, partition_keys)
            return df_spark

    def _get_cardiff_incremental_config(self, partition_keys: list = None) -> dict:
        """
        Get Cardiff-specific configuration for INCREMENTAL loading from table_info.
        
        Simplified version - IncrementalLoader will handle business keys and hash columns.
        
        Args:
            partition_keys (list): Partition keys for data organization
            
        Returns:
            dict: Cardiff-specific configuration for incremental loading
        """
        try:
            logger.info("Configuring Cardiff INCREMENTAL parameters from table_info", "ExtractHelper")
            
            config = {
                "partition_keys": partition_keys if partition_keys else [],
                "table_name": "SMB_CARDIFF_CUSTOMER",
                "layer": "BRONZE"
            }
            
            logger.info(f"Cardiff INCREMENTAL configuration completed - partition_keys: {partition_keys}", "ExtractHelper")
            return config
            
        except Exception as e:
            logger.error(f"Error configuring Cardiff INCREMENTAL: {str(e)}", "ExtractHelper")
            # Fallback configuration
            return {
                "partition_keys": [],
                "table_name": "SMB_CARDIFF_CUSTOMER",
                "layer": "BRONZE"
            }
BUCKET_ASSETS = 'alfa-datalake-dev-assets'
BUCKET_TARGET = 'alfa-datalake-dev-bronce'
S3_PATH_STAGING_TABLES = 's3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_silver/staging_definitions/'
S3_PATH_FILES = 'test'
DB_NAME = 'COREPROD'
DB_SCHEMA = 'ACSELE_ALFA'
CONNECTION_NAME = 'datalake-dev-acsele-qa-connection'
S3_PATH_PROCESS_TABLE_DEFINITIONS = 's3://alfa-datalake-dev-assets/CLIENTS_ASSETS/config/table_definition_extraction_clients.xlsx'
S3_PATH_SAVE_JSON_TABLE_DEFINITIONS = 's3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_bronze/definitions/clients_table_definitions'
extract_helper = ExtractHelperv2(
        glueContext,
        spark,
        BUCKET_ASSETS,
        BUCKET_TARGET,
        DB_SCHEMA,
        DB_NAME,
        CONNECTION_NAME,
        S3_PATH_PROCESS_TABLE_DEFINITIONS,
        S3_PATH_SAVE_JSON_TABLE_DEFINITIONS
    )
table_definitions = extract_helper.load_json_connection()
table_definitions['general_schema']['ACSELE_ALFA'][19]['primary_key_column']
df = extract_helper.read_from_database(
    table_name=table_definitions['general_schema']['ACSELE_ALFA'][19]['table'],
    partition_column=table_definitions['general_schema']['ACSELE_ALFA'][19]['partition_column'],  # Sin particionamiento S3
    sql_script= table_definitions['general_schema']['ACSELE_ALFA'][19]['logic'],  # Solo 1000 registros de prueba
    connectname=CONNECTION_NAME,
    primary_key_column=table_definitions['general_schema']['ACSELE_ALFA'][19]['primary_key_column'],
    table_info=None,  # ⚠️ Esto hará que ejecute el guardado de todos modos
    enable_db_partitioning=True,
    db_partition_column="ROWID",
    num_partitions=5
)
for schema_name, tables in table_definitions['general_schema'].items():
    for table_info in tables:
        if not table_info.get('status', False):
            #logger.info(f"Skipping inactive table: {table_info.get('table', 'Unknown')}")
            continue
        table_name = table_info.get('table', 'Unknown')
        partition_column = table_info.get('partition_column', 'NA')
        sql_logic = table_info.get('logic', '')
        primary_key = table_info.get('primary_key_column', 'PK')
        type_ingest = table_info.get('type_ingest', 'INITIAL')
        print(table_name)
        print(sql_logic)
                    # filename = table_name
                    # df_result = extract_helper.read_and_save_file(
                    #     s3_input_path=args['S3_PATH_FILES'],
                    #     filename=filename,
                    #     sheet=sheet_name if sheet_name and sheet_name != 'NA' else None,
                    #     delimiter=",",
                    #     header=True,
                    #     partition_column=partition_column if partition_column != 'NA' else None,
                    #     table_info=table_info 
                    # )
orchestrator = ConcordanceMatrixOrchestrator(
    spark_session=spark,
    glue_context=glueContext,
    bucket_silver=BUCKET_SILVER,
    s3_path_priority_sources=S3_PATH_PRIORITY_SOURCES,
    selected_columns=SELECTED_COLUMNS,
    s3_path_input=S3_PATH_INPUT,
    main_folder="CLIENTES",
    prefix_bucket=PREFIX_BUCKET
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
   col, lit, concat, count, when, desc, row_number, 
   current_timestamp, date_format, hash, first, upper, trim
)
from pyspark.sql.window import Window
# Configure Spark optimizations
orchestrator.utils.configure_spark_optimizations(spark, "Engine Algorithm", "Silver")
# Step 1: Load input data
df_input = orchestrator._load_input_data()
# Step 2: Load external priority matrix
priority_matrix_df = orchestrator._load_priority_external_matrix()
priority_matrix_df.show(3)
print(f"Processing {len(orchestrator.selected_columns)} columns: {orchestrator.selected_columns}")
# Step 3: Create concordance matrix
concordance_matrix = orchestrator._create_concordance_matrix(df_input)
# Step 4: Split by concordance quality
major_concordance_cases = ["C1", "C2", "C3"]
low_concordance_cases = ["C4", "C5", "FALLBACK"]

df_concordance_major = orchestrator._split_dataframe_by_rule(concordance_matrix, major_concordance_cases)
df_concordance_low = orchestrator._split_dataframe_by_rule(concordance_matrix, low_concordance_cases)

# Step 5: Resolve low concordance cases
fix_concordance_matrix = orchestrator._resolve_low_concordance_cases(
           df_concordance_low, df_input, priority_matrix_df
)
# Step 6: Union matrices
df_final = df_concordance_major.unionByName(fix_concordance_matrix)
# Step 7: Apply hierarchical deduplication
df_final_deduplicated = orchestrator._deduplicate_by_rule_priority(df_final)
# Step 8: Robust selecting record with the best record
window_best_value = Window.partitionBy("PK", "Field").orderBy(
                when(upper(trim(col("Value"))) == "NO REGISTRA", 999).otherwise(1),
                when(col("RuleApplied") == "C1", 1)
                .when(col("RuleApplied") == "C2", 2)
                .when(col("RuleApplied") == "EXTERNAL_RESOLVED", 3)
                .when(col("RuleApplied") == "C3", 4)
                .otherwise(5)
)
df_for_pivot = df_final_deduplicated \
                .withColumn("rn", row_number().over(window_best_value)) \
                .filter(col("rn") == 1) \
                .drop("rn")
# Step 8: Create pivoted matrix
df_final_pivoted = (df_for_pivot
           .groupBy("PK")
           .pivot("Field")
           .agg(first("Value")))
# Step 9: Validation Data
pk_validate = '1-52144889' # CASE 1
# MATRIX CRITERIA
print("===============MATRIX CRITERIA ===================")
df_final_deduplicated.select(*df_final_deduplicated.columns).filter(col("pk") == pk_validate).show(truncate=False)
print("===============MATRIX PIVOTED ===================")
# MATRIX PIVOTED
df_final_pivoted.select(*df_final_pivoted.columns).filter(col("pk") == pk_validate).show()
pk_validate = '1-4920004' # CASE 2
# MATRIX CRITERIA
print("===============MATRIX CRITERIA ===================")
df_final_deduplicated.select(*df_final_deduplicated.columns).filter(col("pk") == pk_validate).show(truncate=False)
print("===============MATRIX PIVOTED ===================")
# MATRIX PIVOTED
df_final_pivoted.select(*df_final_pivoted.columns).filter(col("pk") == pk_validate).show()
pk_validate = '1-15379254' # CASE 3
# MATRIX CRITERIA
print("===============MATRIX CRITERIA ===================")
df_final_deduplicated.select(*df_final_deduplicated.columns).filter(col("pk") == pk_validate).show(truncate=False)
print("===============MATRIX PIVOTED ===================")
# MATRIX PIVOTED
df_final_pivoted.select(*df_final_pivoted.columns).filter(col("pk") == pk_validate).show()
# Verificar datos ANTES del pipeline
pk_validate = '1-98531283'

print("="*80)
print("DATOS ORIGINALES (df_input)")
print("="*80)

df_input.filter(col("PK") == pk_validate) \
    .select("PK", "direccion", "cod_fuente_fk") \
    .orderBy("cod_fuente_fk") \
    .show(truncate=False)

# Contar registros por fuente
print("\nRESUMEN POR FUENTE:")
df_input.filter(col("PK") == pk_validate) \
    .groupBy("cod_fuente_fk") \
    .agg(
        count("*").alias("total_registros"),
        first("direccion").alias("ejemplo_direccion")
    ) \
    .orderBy("cod_fuente_fk") \
    .show(truncate=False)

print("="*80)
print("MATRIZ DE CONCORDANCIA INICIAL (concordance_matrix)")
print("="*80)

# Este es el resultado ANTES de separar en major/low
concordance_matrix = orchestrator._create_concordance_matrix(df_input)

concordance_matrix.filter(
    (col("PK") == pk_validate) & 
    (col("Field") == "direccion")
).show(truncate=False)

# Ver cuántos registros hay para este PK-Field
num_records = concordance_matrix.filter(
    (col("PK") == pk_validate) & 
    (col("Field") == "direccion")
).count()

print(f"\n⚠️ TOTAL de registros para PK={pk_validate}, Field=direccion: {num_records}")
print("="*80)
print("SEPARACIÓN: MAJOR CONCORDANCE (C1, C2, C3)")
print("="*80)

major_concordance_cases = ["C1", "C2", "C3"]
df_concordance_major = concordance_matrix.filter(col("RuleApplied").isin(major_concordance_cases))

df_concordance_major.filter(
    (col("PK") == pk_validate) & 
    (col("Field") == "direccion")
).show(truncate=False)

print("="*80)
print("SEPARACIÓN: LOW CONCORDANCE (C4, C5, FALLBACK)")
print("="*80)

low_concordance_cases = ["C4", "C5", "FALLBACK"]
df_concordance_low = concordance_matrix.filter(col("RuleApplied").isin(low_concordance_cases))

df_concordance_low.filter(
    (col("PK") == pk_validate) & 
    (col("Field") == "direccion")
).show(truncate=False)
print("="*80)
print("DESPUÉS DE EXTERNAL RESOLVER (fix_concordance_matrix)")
print("="*80)

# Cargar matriz de prioridades
priority_matrix_df = orchestrator._load_priority_external_matrix()

# Resolver casos bajos
fix_concordance_matrix = orchestrator._resolve_low_concordance_cases(
    df_concordance_low, df_input, priority_matrix_df
)

fix_concordance_matrix.filter(
    (col("PK") == pk_validate) & 
    (col("Field") == "direccion")
).show(truncate=False)

print("="*80)
print("DESPUÉS DE UNION (df_final - ANTES de deduplicar)")
print("="*80)

df_final = df_concordance_major.unionByName(fix_concordance_matrix)

df_final.filter(
    (col("PK") == pk_validate) & 
    (col("Field") == "direccion")
).orderBy("RuleApplied", "Value") \
.show(truncate=False)

# Contar cuántos registros hay
num_before_dedup = df_final.filter(
    (col("PK") == pk_validate) & 
    (col("Field") == "direccion")
).count()

print(f"\n⚠️ REGISTROS ANTES DE DEDUPLICAR: {num_before_dedup}")
print("⚠️ Si hay más de 1 registro, hay DUPLICADOS que necesitan deduplicación")
print("="*80)
print("DESPUÉS DE DEDUPLICACIÓN (df_final_deduplicated)")
print("="*80)

df_final_deduplicated = orchestrator._deduplicate_by_rule_priority(df_final)

df_final_deduplicated.filter(
    (col("PK") == pk_validate) & 
    (col("Field") == "direccion")
).show(truncate=False)

# Verificar que solo hay 1 registro
num_after_dedup = df_final_deduplicated.filter(
    (col("PK") == pk_validate) & 
    (col("Field") == "direccion")
).count()

print(f"\n✅ REGISTROS DESPUÉS DE DEDUPLICAR: {num_after_dedup}")
print("✅ Debería ser exactamente 1")
print("="*80)
print("MATRIZ PIVOTEADA (df_final_pivoted)")
print("="*80)

df_final_pivoted = (df_final_deduplicated
                   .groupBy("PK")
                   .pivot("Field")
                   .agg(first("Value")))

df_final_pivoted.filter(col("PK") == pk_validate) \
    .select("PK", "direccion") \
    .show(truncate=False)
print("="*80)
print("🔍 COMPARACIÓN DIRECTA: MATRIZ vs PIVOTED")
print("="*80)

# Obtener valor de matriz deduplicada
matriz_value = df_final_deduplicated.filter(
    (col("PK") == pk_validate) & 
    (col("Field") == "direccion")
).select("Value", "RuleApplied", "Source").first()

# Obtener valor de pivoted
pivoted_value = df_final_pivoted.filter(col("PK") == pk_validate) \
    .select("direccion").first()

print(f"\n📊 MATRIZ DEDUPLICADA:")
if matriz_value:
    print(f"   Value: {matriz_value['Value']}")
    print(f"   RuleApplied: {matriz_value['RuleApplied']}")
    print(f"   Source: {matriz_value['Source']}")
else:
    print("   ⚠️ NO HAY DATOS")

print(f"\n📊 MATRIZ PIVOTEADA:")
if pivoted_value and pivoted_value['direccion']:
    print(f"   direccion: {pivoted_value['direccion']}")
else:
    print("   ⚠️ NO HAY DATOS")

print("\n🎯 RESULTADO:")
if matriz_value and pivoted_value:
    if matriz_value['Value'] == pivoted_value['direccion']:
        print("   ✅ VALORES COINCIDEN - Todo OK")
    else:
        print("   ❌ VALORES DIFERENTES - HAY BUG!")
        print(f"\n   Matriz:   '{matriz_value['Value'][:50]}...'")
        print(f"   Pivoted:  '{pivoted_value['direccion'][:50]}...'")
from pyspark.sql import functions as F
print("="*80)
print("TODOS LOS REGISTROS PARA ESTE PK EN df_final_deduplicated")
print("="*80)

df_final_deduplicated.filter(col("PK") == pk_validate) \
    .orderBy("Field") \
    .show(truncate=False)

# Contar total de registros
total_fields = df_final_deduplicated.filter(col("PK") == pk_validate).count()
print(f"\n✅ TOTAL de campos para este PK: {total_fields}")
print("="*80)
print("PIVOTEO MANUAL PASO A PASO")
print("="*80)

# Paso 1: GroupBy
grouped = df_final_deduplicated.groupBy("PK")

# Paso 2: Pivot
pivoted = grouped.pivot("Field")

# Paso 3: Agregación
final_pivot = pivoted.agg(first("Value"))

# Filtrar el PK problemático
final_pivot.filter(col("PK") == pk_validate) \
    .select("PK", "direccion") \
    .show(truncate=False)

# Verificar si hay otros campos que puedan dar pistas
final_pivot.filter(col("PK") == pk_validate).show(truncate=False)
print("="*80)
print("VERIFICAR DUPLICADOS OCULTOS POR FIELD")
print("="*80)

# Buscar duplicados por PK-Field
duplicates_check = df_final_deduplicated.groupBy("PK", "Field") \
    .agg(
        count("*").alias("count"),
        F.collect_list("Value").alias("all_values"),
        F.collect_list("RuleApplied").alias("all_rules")
    ) \
    .filter(col("PK") == pk_validate)

duplicates_check.show(truncate=False)

# Verificar si hay más de 1 valor para direccion
duplicates_direccion = duplicates_check.filter(col("Field") == "direccion")
duplicates_direccion.show(truncate=False)
print("="*80)
print("PIVOT CON ORDENAMIENTO EXPLÍCITO")
print("="*80)

# Intentar con ordenamiento explícito antes del pivot
df_ordered = df_final_deduplicated \
    .withColumn("order_priority", 
                when(col("RuleApplied") == "C1", 1)
                .when(col("RuleApplied") == "C2", 2)
                .when(col("RuleApplied") == "EXTERNAL_RESOLVED", 3)
                .when(col("RuleApplied") == "C3", 4)
                .otherwise(5))

# Crear ventana ordenada
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_order = Window.partitionBy("PK", "Field") \
    .orderBy("order_priority")

# Asegurar que solo hay 1 registro por PK-Field
df_unique = df_ordered \
    .withColumn("rn", row_number().over(window_order)) \
    .filter(col("rn") == 1) \
    .drop("rn", "order_priority")

# Pivotar
df_pivot_ordered = df_unique.groupBy("PK").pivot("Field").agg(first("Value"))

df_pivot_ordered.filter(col("PK") == pk_validate) \
    .select("PK", "direccion") \
    .show(truncate=False)
print("="*80)
print("COMPORTAMIENTO DE first() SOBRE DIRECCION")
print("="*80)

# Test directo de first() sobre el campo direccion
test_first = df_final_deduplicated \
    .filter((col("PK") == pk_validate) & (col("Field") == "direccion")) \
    .agg(
        first("Value").alias("first_value"),
        F.max("Value").alias("max_value"),
        F.min("Value").alias("min_value"),
        count("*").alias("total_count")
    )

test_first.show(truncate=False)
print("="*80)
print("VERIFICAR PARTICIONAMIENTO Y CACHÉ")
print("="*80)

# Forzar collect para materializar datos
materialized = df_final_deduplicated.filter(col("PK") == pk_validate).collect()

print(f"✅ Registros materializados: {len(materialized)}")
for row in materialized:
    if row['Field'] == 'direccion':
        print(f"\nField: {row['Field']}")
        print(f"Value: {row['Value']}")
        print(f"RuleApplied: {row['RuleApplied']}")
        print(f"Source: {row['Source']}")
job.commit()