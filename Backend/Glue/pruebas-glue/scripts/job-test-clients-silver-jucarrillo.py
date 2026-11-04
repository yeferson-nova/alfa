# glue config
# add python files
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, current_date, count, max as spark_max

## ----- IMPORTS CUSTOM ------------------ ###
from logger_config import logger
from extract_helper import ExtractHelper
from incremental_loader import IncrementalLoader

## ----- CONFIGURACIÓN INICIAL ------------------ ###
# Parámetros de prueba (normalmente vienen de Glue job parameters)
test_args = {
    'JOB_NAME': 'test_incremental_loading',
    'BUCKET_ASSETS': 'your-assets-bucket',
    'BUCKET_TARGET': 'your-target-bucket', 
    'DB_NAME': 'ALFPRD',
    'DB_SCHEMA': 'SCHEMA_TEST',
    'CONNECTION_NAME': 'oracle-connection',
    'S3_PATH_PROCESS_TABLE_DEFINITIONS': 's3://bucket/config/table_definitions.xlsx',
    'S3_PATH_SAVE_JSON_TABLE_DEFINITIONS': 's3://bucket/definitions',
    'TABLE_NAME': 'CUSTOMERS',  # Tabla de prueba
    'HASH_COLUMNS': 'CUSTOMER_ID,FIRST_NAME,LAST_NAME,EMAIL,PHONE'  # Columnas para hash
}

  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(test_args['JOB_NAME'], test_args)
 #Checking for legacy data at: s3://alfa-datalake-dev-bronce/CARDIFF/SMB_CARDIFF_CUSTOMER/
parquet_path = 's3://alfa-datalake-dev-bronce/CARDIFF/SMB_CARDIFF_CUSTOMER/'
df_existing = spark.read.parquet(parquet_path)
total_records = df_existing.count()
# ENV VARS
#SELECT * FROM "datalake-dev-bronce-catalog-db"."acsele_acsele_alfa_direccion" LIMIT 10;
DATABASE_NAME = 'datalake-dev-bronce-catalog-db'
TABLE_NAME = 'acsele_acsele_alfa_direccion'
## ----- CONFIGURACIÓN INICIAL ------------------ ###
# Parámetros de prueba (normalmente vienen de Glue job parameters)
test_args = {
    'JOB_NAME': 'test_incremental_loading',
    'BUCKET_ASSETS': 'alfa-datalake-dev-assets',
    'BUCKET_TARGET': 'alfa-datalake-dev-assets', 
    'DB_NAME': 'ACSSOP',
    'DB_SCHEMA': 'ACSELE_ALFA',
    'CONNECTION_NAME': 'datalake-dev-acsele-sop-connection',
    'S3_PATH_PROCESS_TABLE_DEFINITIONS': 's3://alfa-datalake-dev-assets/CLIENTS_ASSETS/config/table_definition_extraction_clients.xlsx',
    'S3_PATH_SAVE_JSON_TABLE_DEFINITIONS': 's3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_bronze/definitions/clients_table_definitions',
    'TABLE_NAME': 'PERSONANATURAL',  # Tabla de prueba
    'HASH_COLUMNS': 'APELLIDOINPUT,APELLIDOMATERNOINPUT,APELLIDOSINPUT,ESTADOCIVILINPUT,ESTADOCIVILVALUE,NOMBREINPUT,NOMBRESINPUT,OCUPACIONINPUT,OCUPACIONVALUE,SEGUNDONOMBREINPUT,TELEFONOCELULARINPUT,TELEFONOFIJOCOMPLETOINPUT,TELEFONOINPUT'  # Columnas para hash
}
## ----- PASO 1: INICIALIZAR COMPONENTES ------------------ ###
logger.info("PASO 1: Inicializando componentes")

# Inicializar ExtractHelper para operaciones de base de datos
extract_helper = ExtractHelper(
    glueContext,
    spark,
    test_args['BUCKET_ASSETS'],
    test_args['BUCKET_TARGET'], 
    test_args['DB_SCHEMA'],
    test_args['DB_NAME'],
    test_args['CONNECTION_NAME'],
    test_args['S3_PATH_PROCESS_TABLE_DEFINITIONS'],
    test_args['S3_PATH_SAVE_JSON_TABLE_DEFINITIONS']
)

# Inicializar IncrementalLoader para lógica incremental
incremental_loader = IncrementalLoader(
    spark_session=spark,
    glue_context=glueContext,
    bucket_target=test_args['BUCKET_TARGET']
)

logger.success("Componentes inicializados correctamente")
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
                Test de Carga Incremental desde Base de Datos
================================================================================

Descripción:
    Test completo para probar la funcionalidad de carga incremental usando
    hash-based comparison desde una base de datos Oracle hacia Bronze layer.

Autor:         Development Team
Creado:        2025-08-04
Versión:       1.0.0
Entorno:       AWS Glue Testing

================================================================================
"""

## ----- IMPORTS NECESARIOS ------------------ ###
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job



## ----- INICIALIZACIÓN SPARK/GLUE ------------------ ###
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(test_args['JOB_NAME'], test_args)

logger.info("=" * 80)
logger.info("INICIANDO TEST DE CARGA INCREMENTAL DESDE BASE DE DATOS")
logger.info("=" * 80)

## ----- PASO 1: INICIALIZAR COMPONENTES ------------------ ###
logger.info("PASO 1: Inicializando componentes")

# Inicializar ExtractHelper para operaciones de base de datos
extract_helper = ExtractHelper(
    glueContext,
    spark,
    test_args['BUCKET_ASSETS'],
    test_args['BUCKET_TARGET'], 
    test_args['DB_SCHEMA'],
    test_args['DB_NAME'],
    test_args['CONNECTION_NAME'],
    test_args['S3_PATH_PROCESS_TABLE_DEFINITIONS'],
    test_args['S3_PATH_SAVE_JSON_TABLE_DEFINITIONS']
)

# Inicializar IncrementalLoader para lógica incremental
incremental_loader = IncrementalLoader(
    spark_session=spark,
    glue_context=glueContext,
    bucket_target=test_args['BUCKET_TARGET']
)

logger.success("Componentes inicializados correctamente")

## ----- PASO 2: CONFIGURAR PATHS Y COLUMNAS ------------------ ###
logger.info("PASO 2: Configurando paths y columnas para prueba")

table_name = test_args['TABLE_NAME']
hash_columns = [col.strip() for col in test_args['HASH_COLUMNS'].split(',')]

# Generar paths S3 para Bronze y Control
bronze_path = f"s3://{test_args['BUCKET_TARGET']}/CLIENTS/{test_args['DB_NAME']}/{test_args['DB_SCHEMA']}/{table_name}/"
control_path = f"s3://{test_args['BUCKET_TARGET']}/CONTROL/INCREMENTAL/{test_args['DB_NAME']}/{table_name}_HASH_CONTROL/"

logger.info(f"Configuración de prueba:")
logger.info(f"  - Tabla: {table_name}")
logger.info(f"  - Columnas hash: {hash_columns}")
logger.info(f"  - Bronze path: {bronze_path}")
logger.info(f"  - Control path: {control_path}")

## ----- PASO 3: EXTRAER DATOS DE BASE DE DATOS ------------------ ###
logger.info("PASO 3: Extrayendo datos desde base de datos")

try:
    # Configurar SQL para extracción (puedes personalizar según tu tabla)
    sql_script = f"""
    SELECT 
        CUSTOMER_ID,
        FIRST_NAME, 
        LAST_NAME,
        EMAIL,
        PHONE,
        REGISTRATION_DATE,
        LAST_UPDATE_DATE,
        STATUS
    FROM {test_args['DB_SCHEMA']}.{table_name}
    WHERE STATUS = 'ACTIVE'
    AND LAST_UPDATE_DATE >= SYSDATE - 30
    """
    
    logger.info("Ejecutando extracción desde base de datos...")
    logger.info(f"SQL: {sql_script}")
    
    # Usar ExtractHelper para extracción (AQUÍ NO se guarda a S3 aún)
    # Necesitamos modificar para que solo extraiga sin guardar
    connection_options = {
        "useConnectionProperties": "True",
        "connectionName": test_args['CONNECTION_NAME'],
        "dbtable": f"{test_args['DB_SCHEMA']}.{table_name}",
        "hashfield": "CUSTOMER_ID",
        "sampleQuery": sql_script,
        "enablePartitioningForSampleQuery": True
    }
    
    # Extraer datos directamente
    df_database = glueContext.create_dynamic_frame.from_options(
        connection_type="oracle",
        connection_options=connection_options,
        transformation_ctx=f"extract_test_{table_name}"
    ).toDF()
    
    db_record_count = df_database.count()
    logger.success(f"Extracción completada: {db_record_count:,} registros desde BD")
    
    # Mostrar muestra de datos
    logger.info("Muestra de datos extraídos:")
    df_database.show(5, truncate=False)
    
except Exception as e:
    logger.error(f"Error en extracción desde BD: {str(e)}")
    raise
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
                    GlueAthenaManager - Estándar Reutilizable
================================================================================

Description:
    Clase estándar para todas las operaciones Glue/Athena en el equipo.
    Una sola implementación que todos los desarrolladores reutilizan.

Author:         Juan Alejandro Carrillo Jaimes
Created:        2025-07-30
Version:        1.0.0
Environment:    AWS Glue

================================================================================
"""

import boto3
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from botocore.exceptions import ClientError
from logger_config import logger

class GlueAthenaManager:
    """
    Clase estándar para todas las operaciones Glue/Athena del proyecto.
    
    USO: Todos los desarrolladores usan esta clase para operaciones Bronze/Silver.
    NO crear clases similares - usar siempre esta.
    """
    
    def __init__(self, glue_context: GlueContext, spark_session: SparkSession):
        """
        Initialize Glue Athena Manager with standard configuration.
        
        Args:
            glue_context (GlueContext): AWS Glue context
            spark_session (SparkSession): Spark session
        """
        self.glue_context = glue_context
        self.spark = spark_session
        self.glue_client = boto3.client('glue')
        
        logger.info("GlueAthenaManager initialized - Standard version", "GlueAthenaManager")
    
    def read_table_from_catalog(self, catalog_name: str, database_name: str, table_name: str, 
                               transformation_ctx: str = None) -> DynamicFrame:
        """
        ESTÁNDAR: Leer tabla desde Glue Catalog.
        
        Args:
            catalog_name (str): Catalog name (ej: datalake-dev-bronte-catalog-db)
            database_name (str): Database name
            table_name (str): Table name
            transformation_ctx (str): Transformation context for Glue
            
        Returns:
            DynamicFrame: Data from table
        """
        try:
            if not transformation_ctx:
                transformation_ctx = f"read_{database_name}_{table_name}"
            
            logger.info(f"Reading table: {catalog_name}.{database_name}.{table_name}", "GlueAthenaManager")
            
            dynamic_frame = self.glue_context.create_dynamic_frame.from_catalog(
                database=database_name,
                table_name=table_name,
                catalog_id=catalog_name,
                transformation_ctx=transformation_ctx
            )
            
            record_count = dynamic_frame.count()
            logger.success(f"Table read successfully: {record_count} records", "GlueAthenaManager")
            
            return dynamic_frame
            
        except Exception as e:
            logger.error(f"Error reading table {table_name}: {str(e)}", "GlueAthenaManager")
            raise
    
    def read_table_with_predicate(self, catalog_name: str, database_name: str, table_name: str,
                                 push_down_predicate: str, transformation_ctx: str = None) -> DynamicFrame:
        """
        ESTÁNDAR: Leer tabla con filtros (predicate pushdown).
        
        Args:
            catalog_name (str): Catalog name
            database_name (str): Database name  
            table_name (str): Table name
            push_down_predicate (str): SQL WHERE clause (ej: "year='2025' AND month='07'")
            transformation_ctx (str): Transformation context
            
        Returns:
            DynamicFrame: Filtered data
        """
        try:
            if not transformation_ctx:
                transformation_ctx = f"read_filtered_{database_name}_{table_name}"
            
            logger.info(f"Reading table with filter: {table_name} WHERE {push_down_predicate}", "GlueAthenaManager")
            
            dynamic_frame = self.glue_context.create_dynamic_frame.from_catalog(
                database=database_name,
                table_name=table_name,
                catalog_id=catalog_name,
                push_down_predicate=push_down_predicate,
                transformation_ctx=transformation_ctx
            )
            
            record_count = dynamic_frame.count()
            logger.success(f"Filtered table read: {record_count} records", "GlueAthenaManager")
            
            return dynamic_frame
            
        except Exception as e:
            logger.error(f"Error reading filtered table: {str(e)}", "GlueAthenaManager")
            raise    
    def write_to_s3_location(self, dynamic_frame: DynamicFrame, s3_path: str, 
                            format_type: str = "glueparquet", partition_keys: list = None,
                            mode: str = "overwrite", transformation_ctx: str = None) -> None:
        """
        ESTÁNDAR: Escribir DynamicFrame a S3.
        
        Args:
            dynamic_frame (DynamicFrame): Data to write
            s3_path (str): S3 destination path
            format_type (str): Format (glueparquet, json, csv)
            partition_keys (list): Partition columns
            mode (str): Write mode (overwrite, append)
            transformation_ctx (str): Transformation context
        """
        try:
            if not transformation_ctx:
                transformation_ctx = f"write_s3_{int(datetime.now().timestamp())}"
            
            if partition_keys is None:
                partition_keys = []
            
            logger.info(f"Writing to S3: {s3_path} (mode: {mode})", "GlueAthenaManager")
            
            connection_options = {
                "path": s3_path,
                "partitionKeys": partition_keys
            }
            
            format_options = {
                "compression": "snappy"
            }
            
            if mode == "append":
                connection_options["enableUpdateCatalog"] = False
                connection_options["writeMode"] = "append"
                format_options["writeMode"] = "append"
            
            self.glue_context.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="s3",
                format=format_type,
                connection_options=connection_options,
                format_options=format_options,
                transformation_ctx=transformation_ctx
            )
            
            logger.success(f"Data written successfully to {s3_path}", "GlueAthenaManager")
            
        except Exception as e:
            logger.error(f"Error writing to S3: {str(e)}", "GlueAthenaManager")
            raise
    
    def execute_athena_query(self, query: str, database: str, s3_output_location: str) -> str:
        """
        ESTÁNDAR: Ejecutar query en Athena.
        
        Args:
            query (str): SQL query to execute
            database (str): Database context
            s3_output_location (str): S3 path for query results
            
        Returns:
            str: Query execution ID
        """
        try:
            athena_client = boto3.client('athena')
            
            logger.info(f"Executing Athena query in database: {database}", "GlueAthenaManager")
            
            response = athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': database},
                ResultConfiguration={'OutputLocation': s3_output_location}
            )
            
            execution_id = response['QueryExecutionId']
            logger.success(f"Query executed successfully: {execution_id}", "GlueAthenaManager")
            
            return execution_id
            
        except Exception as e:
            logger.error(f"Error executing Athena query: {str(e)}", "GlueAthenaManager")
            raise
    
    def table_exists_in_catalog(self, catalog_name: str, database: str, table_name: str) -> bool:
        """
        ESTÁNDAR: Verificar si tabla existe en Glue Catalog.
        
        Args:
            catalog_name (str): Catalog name
            database (str): Database name
            table_name (str): Table name
            
        Returns:
            bool: True if table exists
        """
        try:
            self.glue_client.get_table(
                CatalogId=catalog_name,
                DatabaseName=database,
                Name=table_name
            )
            
            logger.info(f"Table exists: {catalog_name}.{database}.{table_name}", "GlueAthenaManager")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                logger.info(f"Table not found: {table_name}", "GlueAthenaManager")
                return False
            else:
                logger.error(f"Error checking table existence: {str(e)}", "GlueAthenaManager")
                raise    
    def create_table_in_catalog(self, catalog_name: str, database: str, table_name: str,
                               columns: list, partition_keys: list, s3_location: str,
                               table_properties: dict = None) -> bool:
        """
        ESTÁNDAR: Crear tabla en Glue Catalog.
        
        Args:
            catalog_name (str): Catalog name
            database (str): Database name
            table_name (str): Table name
            columns (list): Column definitions [{'Name': 'col', 'Type': 'string'}]
            partition_keys (list): Partition definitions
            s3_location (str): S3 location for data
            table_properties (dict): Additional properties
            
        Returns:
            bool: True if created successfully
        """
        try:
            if self.table_exists_in_catalog(catalog_name, database, table_name):
                logger.info(f"Table {table_name} already exists", "GlueAthenaManager")
                return True
            
            default_properties = {
                'classification': 'parquet',
                'compressionType': 'snappy',
                'created_by': 'silver-standard-manager'
            }
            
            if table_properties:
                default_properties.update(table_properties)
            
            table_input = {
                'Name': table_name,
                'StorageDescriptor': {
                    'Columns': columns,
                    'Location': s3_location,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                },
                'PartitionKeys': partition_keys,
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': default_properties
            }
            
            self.glue_client.create_table(
                CatalogId=catalog_name,
                DatabaseName=database,
                TableInput=table_input
            )
            
            logger.success(f"Table created: {catalog_name}.{database}.{table_name}", "GlueAthenaManager")
            return True
            
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}", "GlueAthenaManager")
            return False
    
    def get_table_partitions(self, catalog_name: str, database: str, table_name: str) -> list:
        """
        ESTÁNDAR: Obtener particiones de una tabla.
        
        Args:
            catalog_name (str): Catalog name
            database (str): Database name
            table_name (str): Table name
            
        Returns:
            list: List of partition information
        """
        try:
            response = self.glue_client.get_partitions(
                CatalogId=catalog_name,
                DatabaseName=database,
                TableName=table_name
            )
            
            partitions = response.get('Partitions', [])
            logger.info(f"Found {len(partitions)} partitions for {table_name}", "GlueAthenaManager")
            
            return partitions
            
        except Exception as e:
            logger.error(f"Error getting partitions: {str(e)}", "GlueAthenaManager")
            return []
dyf = glueContext.create_dynamic_frame.from_catalog(database=DATABASE_NAME, table_name=TABLE_NAME)
dyf.printSchema()
df = dyf.toDF()
df.show()
s3output = glueContext.getSink(
  path="s3://bucket_name/folder_name",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase="demo", catalogTableName="populations"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(DyF)
job.commit()