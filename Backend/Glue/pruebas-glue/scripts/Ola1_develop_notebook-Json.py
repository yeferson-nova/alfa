#%additional_python_modules openpyxl==3.0.10, pyxlsb==1.0.8


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from commons.ExtractHelper import ExtractHelper
from pyspark.sql import functions as F

import boto3, datetime, json

s3_client = boto3.client("s3")

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
df_ola0 = spark.read.parquet("s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_MOVIMIENTO/2025/4/11/")
df_ola1  = spark.read.parquet("s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_MOVIMIENTO/2025/5/7/")
cols_ola0 = df_ola0.columns
cols_ola1 = df_ola1.columns

print(cols_ola0)
print(cols_ola1)
import json
import boto3
import re

# Configuración de rutas en S3
input_bucket_name = "alfa-datalake-dev-assets"
output_bucket_name = "alfa-datalake-dev-assets"
input_key = "support_phase_files/ola1/alfaprd_prv_desc_tables.txt"
output_key = "support_phase_files/ola1/alfaprd_arpprod_rules_table_prv.json"


# Crear cliente de S3
s3 = boto3.client("s3")

# Descargar el archivo desde S3
response = s3.get_object(Bucket=input_bucket_name, Key=input_key)
lines = response["Body"].read().decode("utf-8").split("\n")

# Inicializar estructura JSON
output_json = {}

def convert_data_type(oracle_type):
    """Convierte los tipos de datos de Oracle a JSON."""
    oracle_type = oracle_type.upper()
    if "NUMBER" in oracle_type:
        match = re.search(r"NUMBER\((\d+)(?:,(\d+))?\)", oracle_type)
        if match:
            precision = match.group(1)
            scale = match.group(2) if match.group(2) else "0"
            return f"DECIMAL({precision},{scale})"
        return "DECIMAL"
    elif "VARCHAR2" in oracle_type or "CHAR" in oracle_type:
        return "STRING"
    elif "DATE" in oracle_type:
        return "DATE"
    return "STRING"

i = 0
while i < len(lines):
    line = lines[i].strip()
    i += 1  # Asegurar que no quede atrapado en la misma línea
    if not line:
        continue
    
    # Detectar inicio de una tabla
    if line.startswith("# TABLA:"):
        table_info = line.replace("# TABLA:", "").strip().split(".")
        if len(table_info) < 2:
            continue
        
        schema_name, table_name = table_info
        
        primary_key = None
        
        # Buscar clave primaria
        if i < len(lines) and lines[i].startswith("PK:"):
            primary_key = lines[i].replace("PK:", "").strip()
            i += 1
        
        # Saltar encabezados de la tabla
        while i < len(lines) and ("Nombre" in lines[i] or "-" in lines[i]):
            i += 1
        
        # Inicializar listas de reglas
        column_exists = []
        column_data_type = []
        is_complete = []
        is_unique = []
        is_primary_key = []
        
        # Extraer columnas y sus atributos
        while i < len(lines):
            line = lines[i].strip()
            if not line or line.startswith("# TABLA:"):
                break  # Si encontramos una nueva tabla, salimos
            
            parts = line.split()
            if len(parts) < 2:  # Evitar líneas inválidas
                i += 1
                continue
            
            col_name = parts[0]
            is_not_null = "NOT NULL" in line
            data_type = convert_data_type(parts[-1])
            
            # Agregar reglas
            column_exists.append(f"ColumnExists \"{col_name}\"",)
            column_data_type.append(f"ColumnDataType \"{col_name}\" = \"{data_type}\"",)
            if is_not_null or col_name == primary_key:
                is_complete.append(f"IsComplete \"{col_name}\"",)
            if col_name == primary_key:
                is_unique.append(f"IsUnique \"{col_name}\"",)
                is_primary_key.append(f"IsPrimaryKey \"{col_name}\"")
            
            i += 1
        
        # Consolidar reglas en el orden correcto
        rules = column_exists + column_data_type + is_complete + is_unique + is_primary_key
        
        # Asegurar que la última regla termina con "," excepto la última de la lista
        for j in range(len(rules) - 1):
            rules[j] = rules[j] + ","
        
        # Agregar la tabla al JSON
        if schema_name not in output_json:
            output_json[schema_name] = []
        
        output_json[schema_name].append({
            "table": table_name,
            "rules": rules,
            "db": "alfaprd",
            "schema": schema_name,
            "status": True
        })

# Guardar el JSON en S3
s3.put_object(Bucket=output_bucket_name, Key=output_key, Body=json.dumps(output_json, indent=4))

print(f"Archivo JSON generado y guardado en S3: s3://{output_bucket_name}/{output_key}")
"""
Create common libreries to use in glue.
"""
from awsglue import DynamicFrame
from datetime import datetime
import pandas as pd
from pyspark.sql.functions import col, concat, lit, date_format, to_date
from pyspark.sql.types import TimestampType
class ExtractHelper():
    def __init__(self, glue_context, spark, *args):
        """
        Initialice Extract Class.
        """
        self._glue_context = glue_context
        self._spark = spark
        self.DB_SCHEMA, self.BUCKET_NAME, self.DB_NAME = args
        
    def obtain_date(self)->None:
        """
        Funtion to obtain current date
        """
        self.current_date = datetime.now()
        self.day = self.current_date.day
        self.month = self.current_date.month
        self.year = self.current_date.year 
        
    def get_column_partition(self, input_dataframe, partition):
        if isinstance(input_dataframe.schema[partition].dataType, TimestampType):
        # Case when partition is timestamp
            df_with_partition = input_dataframe.withColumn(f"{partition}_partition", date_format(col(partition), "yyyy-MM"))
        else:
            # Case when partitio isn't timestamp 
            df_with_partition = input_dataframe.withColumn(f"{partition}_partition", date_format(to_date(concat(col(partition).cast("string"), lit("01")), 'yyyyMMdd'), 'yyyy-MM'))

        return df_with_partition

    def execute_data(self, table_name, fields:str = "*", partition:str = "-", sql_script:str = "-", connectname:str = "-") -> str:
        """
        Functions that allows extract data from Oracle DB

        arg:
            table_name: Name of table to extract.
            fields: Fields of table to extract.
            sql_script: this values is only required if the extraction data need any type of logical interview.
        return:
            return msj with the response of operation.
        """
        # Define the connection options
        connect_option = {
            "useConnectionProperties": "true",
            "dbtable": f"{self.DB_SCHEMA}.{table_name}",
            "connectionName": connectname
        }

        # Add sampleQuery if sql_script is not "-"
        if sql_script != "-":
            connect_option["sampleQuery"] = sql_script

        # Log the beginning of extraction
        print(f"start to extract info {table_name}")

        # Log the connection options
        print(connect_option)


        try:
            
            tabla_to_extract = self._glue_context.create_dynamic_frame.from_options(
            connection_type = "oracle",
            connection_options = connect_option,
                transformation_ctx = "TablaExtract"
            )                        
            print(partition)
            if partition == '-':          
                frame_save =  tabla_to_extract     
                partition_keys = []

            else: 
                print(f"Col partition: {partition}")
                tabla_to_df = tabla_to_extract.toDF()                    
                frame_save = self.get_column_partition(tabla_to_df, partition)                        
                frame_save = DynamicFrame.fromDF(frame_save, self._glue_context, "frame_save_dynamic") 
                partition_keys = [f"{partition}_partition"]

            print(frame_save.show(1))

            s3_path = f"s3://{self.BUCKET_NAME}/{self.DB_NAME}/{self.DB_SCHEMA}/{table_name}/"
            print(f"{s3_path}")

            amazon_s3 = self._glue_context.write_dynamic_frame.from_options(
                frame=frame_save, 
                connection_type="s3", 
                format="glueparquet", 
                connection_options={"path": s3_path, "partitionKeys": partition_keys}, 
                format_options={"compression": "snappy"}, 
                transformation_ctx="AmazonS3")            
            return amazon_s3

        except Exception as e:
            return None                           
DB_SCHEMA = 'GENERAL'
DB_NAME = 'ALFAPRD'
BUCKET_NAME = 'alfa-niif-bronce-test'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
CONNECT_NAME = 'ALFAPRD_QA_TEST'
PERIOD = '202501'
CERTIFICATE = True

PATH_JSON = f"support_phase_files/niif/{DB_NAME.lower()}_{DB_SCHEMA.lower()}_tables.json"


print(f"Period: {PERIOD}")
print(f"Schema: {DB_SCHEMA}")
print(f"Connect: {CONNECT_NAME}")
print(f"Bucket_name: {BUCKET_NAME}")
print(f"Json_file: s3://{BUCKET_NAME_ASSETS}/{PATH_JSON}")

###################################################################
######################## READ FILE JSON ###########################
###################################################################

catalog_tables_object = s3_client.get_object(Bucket = BUCKET_NAME_ASSETS, Key = PATH_JSON)
catalog_tables = json.load(catalog_tables_object['Body'])

extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME, DB_NAME)
###################################################################
################# ITERATE FOR EACH TABLES #########################
###################################################################

for schema in catalog_tables:
    if schema in DB_SCHEMA:
        for table_info in catalog_tables[schema]:
                if table_info['status']:
                    print(f"Tables for ingest: {schema}.{table_info['table']}")
                    try:
                        extract_helper.execute_data(table_info['table'], table_info['fields'], table_info['partition_column'], connectname= CONNECT_NAME)    
                        print("Successfully")
                    except Exception as e:
                        print(f"ERROR WITH INGEST: {e}")
    else:
        print("NOT TABLE FOR INGEST")
if partition == '-':
    frame_save =  tabla_to_extract     
    print(f"Without partition")
sql_script = 'SELECT de.*, p.periodo FROM GENERAL.ANP_DETALLE_SOLICITUDES_GIRO de JOIN GENERAL.ANP_PERIODO_SOLICITUDES_GIRO p ON de.nid_periodo_solicitud_giro = p.nid_periodo_solicitud_giro'
# Define the connection options
connect_option = {
    "useConnectionProperties": "true",
    "dbtable": f"{DB_SCHEMA}.{table_name}",
    "connectionName": "ALFAPRD_QA_TEST"
}

# Add sampleQuery if sql_script is not "-"
if sql_script != "-":
    connect_option["sampleQuery"] = sql_script
print(connect_option)
simple_execution = {
        "useConnectionProperties": "true",
        "dbtable": f"GENERAL.CGR_ESTADOS_PERSONAS",
        "connectionName": "ALFAPRD_QA_TEST"
    }
logica_execution = {
        "useConnectionProperties": "true",
        "dbtable":f"GENERAL.ANP_PENSIONES",
        "sampleQuery": "SELECT  GENERAL.ANP_PENSIONES.*, TO_DATE(FEC_EXPEDICION, 'YYYY-MM-DD') AS FEC_EXPEDICION_PARTITION FROM GENERAL.ANP_PENSIONES;",
        "connectionName": "ALFAPRD_QA_TEST",
}
my_tabla = glueContext.create_dynamic_frame.from_options(
            connection_type = "oracle",
            connection_options = simple_execution,
                transformation_ctx = "MyTabla"
            )
my_tabla.printSchema()
# Diccionario con el nombre de la tabla y el nombre de la columna partición
partition_columns = {
    "GENERAL_ANP_DETALLE_SOLICITUDES_GIRO": ["periodo_partition"],
    "GENERAL_ANP_DISTRIBUCION_VIG_PENSION": ["fec_inicial_partition"],
    "GENERAL_ANP_PENSIONADO_NOVEDAD": ["fec_ingreso_partition"],
    "GENERAL_ANP_PENSIONES": ["fec_expedicion_partition"],
    "GENERAL_ANP_PERIODO_SOLICITUDES_GIRO": ["periodo_partition"],
    "GENERAL_CGR_CONCEPTOS": [],  # No tiene columna partición
    "GENERAL_CGR_ORIGENES": []  # No tiene columna partición
}

for table, partition_column in partition_columns.items():    
    s3_path = f"s3://alfa-datalake-dev-silver/ALFAPRD/GENERAL/SILVER_{table}/"
    print(f"Tabla: df_tbl_{table.lower()}, Columna de Partición: {partition_column}, {s3_path}")
    
partition = 'PERIODO'
from pyspark.sql.functions import col,date_format
from awsglue import DynamicFrame
if partition == '-':          
    frame_save =  my_tabla 
    print(f"Without partition")
else: 
    print(f"Col partition: {partition}")
    tabla_to_df = my_tabla.toDF()
    #frame_save = tabla_to_df.withColumn(f"{partition}_partition", col(f"{partition}").cast("date"))
    #frame_save = tabla_to_df.withColumn(f"{partition}_partition", date_format(col(f"{partition}"), "yyyy-MM"))    
    #frame_save = tabla_to_df.withColumn(f"{partition}_partition", col(f"{partition}").cast("date"))
    frame_save = get_column_partition(tabla_to_df, partition)
    frame_save = DynamicFrame.fromDF(frame_save, glueContext, "frame_save_dynamic")
frame_save.printSchema()
df_partition = tabla_to_df.select("PERIODO")
df_partition = df_partition.withColumn("PERIODO_str", concat(col(partition).cast("string"), lit("01")))
df_partition = df_partition.withColumn("PERIODO_ts", to_date(col("PERIODO_str"), "yyyyMMdd"))
df_partition = df_partition.withColumn(f"{partition}_partition", date_format(col("PERIODO_ts"), "yyyy-MM")) \
               .drop("PERIODO_str").drop("PERIODO_ts")
df_partition.show(1)
df_with_partition = tabla_to_df.withColumn(
    f"{partition}_partition",
    date_format(to_date(concat(col(partition).cast("string"), lit("01")), 'yyyyMMdd'), 'yyyy-MM'))
df_with_partition.show(1, vertical = True)
from pyspark.sql.functions import col, concat, lit, date_format, to_date

def get_column_partition(df, partition):
    if df.schema[partition].dataType == 'timestamp':
        # Caso cuando PERIODO es timestamp
        df_with_partition = df.withColumn(f"{partition}_partition", date_format(col(partition), "yyyy-MM"))
    else:
        # Caso cuando PERIODO no es timestamp (suponiendo decimal YYYYMM)
        df_with_partition = df.withColumn(f"{partition}_partition", date_format(to_date(concat(col(partition).cast("string"), lit("01")), 'yyyyMMdd'), 'yyyy-MM'))
    return df_with_partition
frame_save.toDF().show(1, truncate = False, vertical = True)
amazon_s3 = glueContext.write_dynamic_frame.from_options(
                frame=my_tabla, 
                connection_type="s3", 
                format="glueparquet", 
                connection_options={"path":"s3://alfa-niif-bronce-test/ALFPRD/GENERAL/"}, 
                format_options={"compression": "snappy"}, 
                transformation_ctx="AmazonS3") 
fields = "FEC_INICIAL"
fields_cast = (fields, "cast:date")  # This should be a tuple, not a formatted string
print(fields_cast)
# ('FEC_INICIAL', 'cast:date')

my_tabla_map = my_tabla.resolveChoice(specs=[fields_cast])
my_tabla_map.printSchema()

#my_tabla_map = my_tabla.resolveChoice(specs=[("FEC_INICIAL", "cast:date")])
my_tabla_map = my_tabla.resolveChoice(specs=[(fields_cast)])
my_tabla_map.printSchema()
my_tabla_map.toDF().show(5)
s3_path = f"s3://{BUCKET_NAME}/{DB_NAME}/{DB_SCHEMA}/ANP_PENSIONES/"
print(f"{s3_path}")
# Leer los datos desde S3 como DataFrame de PySpark
df = spark.read.parquet(s3_path)

# Mostrar esquema y datos
df.printSchema()
df.count()
df.show(1, truncate = False, vertical = True)
amazon_s3 = glueContext.write_dynamic_frame.from_options(frame=my_tabla, connection_type="s3", format="glueparquet", connection_options={"path": f"s3://{BUCKET_NAME}/ALFPRD_test/{DB_SCHEMA}/{table_name}/", "partitionKeys": ["FEC_EXPEDICION"]}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3")            
dyf = glueContext.create_dynamic_frame.from_catalog(database='database_name', table_name='table_name')
dyf.printSchema()
df = dyf.toDF()
df.show()
import matplotlib.pyplot as plt

# Set X-axis and Y-axis values
x = [5, 2, 8, 4, 9]
y = [10, 4, 8, 5, 2]
  
# Create a bar chart 
plt.bar(x, y)
  
# Show the plot
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
from pyspark.sql.functions import col, trim
from awsglue import DynamicFrame
DB_CATALOG_GLUE = 'datalake-dev-bronce-catalog-db'
DB_NAME = 'ALFAPRD'
BUCKET_NAME = 'alfa-datalake-dev-silver'
DB_SCHEMA = 'GENERAL'
anp_detalle_solicitudes_giro = glueContext.create_dynamic_frame.from_catalog(f"{DB_CATALOG_GLUE}", table_name=f"{DB_NAME}_GENERAL_ANP_DETALLE_SOLICITUDES_GIRO").toDF()
anp_detalle_solicitudes_giro = df_tbl_anp_detalle_solicitudes_giro.select(
col("NID_PERIODO_SOLICITUD_GIRO").alias("NID_PERIODO_SOLICITUD_GIRO"),
trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
col("periodo_partition").alias("periodo_partition")
)
anp_detalle_solicitudes_giro = DynamicFrame.fromDF(anp_detalle_solicitudes_giro, glueContext, "frame_save_dynamic") 
df_tbl_anp_distribucion_vig_pension = glueContext.create_dynamic_frame.from_catalog(f"{DB_CATALOG_GLUE}", table_name=f"{DB_NAME}_GENERAL_ANP_DISTRIBUCION_VIG_PENSION").toDF()
df_tbl_anp_distribucion_vig_pension = df_tbl_anp_distribucion_vig_pension.select(
col("NID_DISTRIBUCION_VIGENCIA").alias("NID_DISTRIBUCION_VIGENCIA"),
col("fec_inicial_partition").alias("fec_inicial_partition")
)
spark.conf.get("spark.sql.legacy.parquet.int96RebaseModeInRead")
df_tbl_anp_pensionado_novedad = glueContext.create_dynamic_frame.from_catalog(f"{DB_CATALOG_GLUE}", table_name=f"{DB_NAME}_GENERAL_ANP_PENSIONADO_NOVEDAD").toDF()
df_tbl_anp_pensionado_novedad = df_tbl_anp_pensionado_novedad.select(
col("NID_DISTRIBUCION_VIGENCIA").alias("NID_DISTRIBUCION_VIGENCIA"),
trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
trim(col("COD_CONCEPTO")).alias("COD_CONCEPTO"),
col("VLR_TOTAL").alias("VLR_TOTAL"),
col("fec_ingreso_partition").alias("fec_ingreso_partition")
)
df_tbl_anp_pensionado_novedad = df_tbl_anp_pensionado_novedad.dropDuplicates()
frame_save = DynamicFrame.fromDF(df_tbl_anp_pensionado_novedad, glueContext, "frame_save_dynamic") 
s3_path = f"s3://{BUCKET_NAME}/{DB_NAME}/{DB_SCHEMA}/SILVER_ANP_PENSIONADO_NOVEDAD/"
partition = ["fec_ingreso_partition"]
glueContext.write_dynamic_frame.from_options(
            frame=frame_save, 
            connection_type="s3", 
            format="glueparquet", 
            connection_options={"path": s3_path, "partitionKeys": partition}, 
            format_options={"compression": "snappy"}, 
            transformation_ctx="AmazonS3")   
df_tbl_anp_pensiones = glueContext.create_dynamic_frame.from_catalog(f"{DB_CATALOG_GLUE}", table_name=f"{DB_NAME}_GENERAL_ANP_PENSIONES").toDF()
df_tbl_anp_pensiones = df_tbl_anp_pensiones.select(
col("NID_PENSION").alias("NID_PENSION"),
col("NID_PROCEDENCIA").alias("NID_PROCEDENCIA"),
trim(col("COD_ORIGEN")).alias("COD_ORIGEN"),
col("FEC_EXPEDICION").alias("FEC_EXPEDICION"),
col("fec_expedicion_partition").alias("fec_expedicion_partition")
)
df_tbl_anp_periodo_solicitudes_giro = glueContext.create_dynamic_frame.from_catalog(f"{DB_CATALOG_GLUE}", table_name=f"{DB_NAME}_GENERAL_ANP_PERIODO_SOLICITUDES_GIRO").toDF()
df_tbl_anp_periodo_solicitudes_giro = df_tbl_anp_periodo_solicitudes_giro.select(
trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
col("VLR_NETO").alias("VLR_NETO"),
col("NUM_SOLICITUD_TES").alias("NUM_SOLICITUD_TES"),
col("FEC_ENVIO_TES").alias("FEC_ENVIO_TES"),
trim(col("COD_CONCEPTO_ANP")).alias("COD_CONCEPTO_ANP"),
col("NID_PERIODO_SOLICITUD_GIRO").alias("NID_PERIODO_SOLICITUD_GIRO"),
col("periodo_partition").alias("periodo_partition")
)
df_tbl_cgr_conceptos = glueContext.create_dynamic_frame.from_catalog(f"{DB_CATALOG_GLUE}", table_name=f"{DB_NAME}_GENERAL_CGR_CONCEPTOS").toDF()
df_tbl_cgr_conceptos = df_tbl_cgr_conceptos.select(
trim(col("COD_CONCEPTO")).alias("COD_CONCEPTO"),
col("NOMBRE_CONCEPTO").alias("NOMBRE_CONCEPTO")
)
df_tbl_cgr_origenes = glueContext.create_dynamic_frame.from_catalog(f"{DB_CATALOG_GLUE}", table_name=f"{DB_NAME}_GENERAL_CGR_ORIGENES").toDF()
df_tbl_cgr_origenes = df_tbl_cgr_origenes.select(
trim(col("COD_ORIGEN")).alias("COD_ORIGEN"),
col("NOMBRE").alias("NOMBRE")
)
partition_columns = {
        "anp_detalle_solicitudes_giro": ["periodo_partition"],
        "ANP_DISTRIBUCION_VIG_PENSION": ["fec_inicial_partition"],
        "ANP_PENSIONADO_NOVEDAD": ["fec_ingreso_partition"],
        "ANP_PENSIONES": ["fec_expedicion_partition"],
        "ANP_PERIODO_SOLICITUDES_GIRO": ["periodo_partition"],
        "CGR_CONCEPTOS": [],  
        "CGR_ORIGENES": []  
    }
    
for table, partition_column in partition_columns.items():
   
    print(table)
    
    print(f"Tabla: df_tbl_{table.lower()}, Columna de Partición: {partition_column}")

    frame_save = DynamicFrame.fromDF(df_tbl_anp_detalle_solicitudes_giro, glueContext, "frame_save_dynamic") 
    #s3_path = f"s3://{BUCKET_NAME}/{DB_NAME}/SILVER_{table}/"

    #logger.info(f"save data on: {s3_path}")

anp_detalle_solicitudes_giro
df_tbl_anp_detalle_solicitudes_giro
import boto3
import difflib

# Configuración del cliente S3
s3_client = boto3.client("s3")
bucket_name = "alfa-niif-bronce-test"
prefix1 = "ALFAPRD-UT/commons_glue/"
prefix2 = "ALFAPRD-UT/commons_repo/"

def list_files(prefix, extensions):
    """Lista archivos con extensiones específicas en un prefijo de S3."""
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return {obj["Key"].split("/")[-1]: obj["Key"] 
            for obj in response.get("Contents", []) 
            if any(obj["Key"].endswith(ext) for ext in extensions)}

def get_file_content(key):
    """Obtiene el contenido de un archivo en S3 y lo devuelve como lista de líneas."""
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    return response["Body"].read().decode("utf-8").splitlines()

# Extensiones a comparar (.py y .csv)
extensions = [".py", ".csv"]

# Listar archivos en ambas carpetas
files1 = list_files(prefix1, extensions)
files2 = list_files(prefix2, extensions)

# Comparar archivos que están en ambas carpetas
common_files = set(files1.keys()) & set(files2.keys())

for filename in common_files:
    print(f"\nComparando archivo: {filename}\n" + "-" * 50)
    
    content1 = get_file_content(files1[filename])
    content2 = get_file_content(files2[filename])

    diff = difflib.unified_diff(content1, content2, fromfile=f"{prefix1}{filename}", tofile=f"{prefix2}{filename}", lineterm="")
    print("\n".join(diff) or "Los archivos son idénticos.")
import boto3
import ast
import csv
import io
import datetime

# Configuración del cliente S3
s3_client = boto3.client("s3")
bucket_name = "alfa-niif-bronce-test"
prefix = "ALFAPRD-UT/commons2/"  # Cambia a "ALFAPRD-UT/commons2/" si deseas usar el otro prefijo

# Definir el nombre del archivo CSV
csv_output_key = f"{prefix}python_functions.csv"

def list_files(prefix):
    """Lista archivos .py en un prefijo de S3."""
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return {obj["Key"].split("/")[-1]: obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".py")}

def get_file_content(key):
    """Obtiene el contenido de un archivo en S3."""
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    return response["Body"].read().decode("utf-8")

def extract_functions(code):
    """Extrae las funciones definidas en un script de Python."""
    functions = []
    try:
        tree = ast.parse(code)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                functions.append(node.name)
    except SyntaxError:
        functions.append("Error al analizar el archivo")
    return functions

# Listar archivos en el prefijo especificado
files = list_files(prefix)

# Guardar funciones en CSV
csv_buffer = io.StringIO()
csv_writer = csv.writer(csv_buffer)
csv_writer.writerow(["Archivo", "Funcion"])

for filename, key in files.items():
    code = get_file_content(key)
    functions = extract_functions(code)
    for func in functions:
        csv_writer.writerow([filename, func])

# Subir CSV a S3
s3_client.put_object(Bucket=bucket_name, Key=csv_output_key, Body=csv_buffer.getvalue())
print(f"Archivo CSV guardado en s3://{bucket_name}/{csv_output_key}")
import json
import boto3

# Configuración de rutas en S3
bucket_name = "alfa-niif-bronce-test"
input_key = "ALFAPRD-UT/archivosJson generados/archivo con desc of table.txt"
output_key = "ALFAPRD-UT/archivosJson generados/output.json"

# Crear cliente de S3
s3 = boto3.client("s3")

# Descargar el archivo desde S3
response = s3.get_object(Bucket=bucket_name, Key=input_key)
lines = response["Body"].read().decode("utf-8").split("\n")

# Mostrar contenido del archivo para depuración
print("Contenido del archivo .txt:")
for line in lines:
    print(line)

# Inicializar estructura JSON
output_json = {}

i = 0
while i < len(lines):
    line = lines[i].strip()
    if not line:
        i += 1
        continue
    
    # Detectar tabla
    if line.lower().startswith("desc"):
        table_info = line.split()
        if len(table_info) < 2:
            i += 1
            continue
        
        full_table_name = table_info[1]  # Formato esperado: SCHEMA.TABLE
        if "." not in full_table_name:
            i += 1
            continue
        
        schema_name, table_name = full_table_name.split(".", 1)
        print(f"Procesando tabla: {schema_name}.{table_name}")
        i += 1
        
        primary_key = None
        
        # Buscar clave primaria
        if i < len(lines) and lines[i].strip().lower().startswith("pk"):
            pk_line = lines[i].strip().split()
            if len(pk_line) > 1:
                primary_key = pk_line[1]
            print(f"Clave primaria detectada: {primary_key}")
            i += 1
        
        # Saltar encabezados de la tabla
        while i < len(lines) and ("Nombre" in lines[i] or "-" in lines[i]):
            i += 1
        
        # Inicializar listas para agrupar reglas
        column_exists = []
        column_data_type = []
        is_complete = []
        is_unique = []
        is_primary_key = []
        
        # Extraer las columnas y sus atributos
        while i < len(lines) and lines[i].strip():
            parts = lines[i].split()
            if len(parts) < 2:
                print(f"Saltando línea inesperada en {i+1}: {lines[i]}")
                i += 1
                continue
            
            col_name = parts[0]
            is_not_null = "NOT NULL" in lines[i]
            data_type = "STRING"
            
            # Agregar reglas a listas específicas
            column_exists.append(f"ColumnExists \"{col_name}\",")
            column_data_type.append(f"ColumnDataType \"{col_name}\" = \"{data_type}\",")
            if is_not_null or col_name == primary_key:
                is_complete.append(f"IsComplete \"{col_name}\",")
            if col_name == primary_key:
                is_unique.append(f"IsUnique \"{col_name}\",")
                is_primary_key.append(f"IsPrimaryKey \"{col_name}\"")
            
            print(f"Procesada columna: {col_name} - Not Null: {is_not_null}")
            i += 1
        
        # Consolidar reglas en el orden requerido
        rules = column_exists + column_data_type + is_complete + is_unique + is_primary_key
        
        # Agregar al JSON
        if schema_name not in output_json:
            output_json[schema_name] = []
        
        output_json[schema_name].append({
            "table": table_name,
            "rules": rules,
            "db": "alfaprd",
            "schema": schema_name,
            "status": True
        })

# Verificar salida antes de guardar
print("JSON generado:")
print(json.dumps(output_json, indent=4))

# Guardar el JSON en S3
s3.put_object(Bucket=bucket_name, Key=output_key, Body=json.dumps(output_json, indent=4))

print(f"Archivo JSON generado y guardado en S3: s3://{bucket_name}/{output_key}")
import json
import boto3
import re

# Configuración de rutas en S3
input_bucket_name = "alfa-niif-bronce-test"
output_bucket_name = "alfa-datalake-dev-assets"
input_key = "ALFAPRD-UT/archivosJson generados/desc/alfaprd_arpprod_desc_table.txt"
output_key = "support_phase_files/niif/alfaprd_arpprod_rules_table.json"

# Crear cliente de S3
s3 = boto3.client("s3")

# Descargar el archivo desde S3
response = s3.get_object(Bucket=input_bucket_name, Key=input_key)
lines = response["Body"].read().decode("utf-8").split("\n")

# Inicializar estructura JSON
output_json = {}

def convert_data_type(oracle_type):
    """Convierte los tipos de datos de Oracle a JSON."""
    oracle_type = oracle_type.upper()
    if "NUMBER" in oracle_type:
        match = re.search(r"NUMBER\((\d+)(?:,(\d+))?\)", oracle_type)
        if match:
            precision = match.group(1)
            scale = match.group(2) if match.group(2) else "0"
            return f"DECIMAL({precision},{scale})"
        return "DECIMAL"
    elif "VARCHAR2" in oracle_type or "CHAR" in oracle_type:
        return "STRING"
    elif "DATE" in oracle_type:
        return "DATE"
    return "STRING"

i = 0
while i < len(lines):
    line = lines[i].strip()
    i += 1  # Asegurar que no quede atrapado en la misma línea
    if not line:
        continue
    
    # Detectar inicio de una tabla
    if line.startswith("# TABLA:"):
        table_info = line.replace("# TABLA:", "").strip().split(".")
        if len(table_info) < 2:
            continue
        
        schema_name, table_name = table_info
        
        primary_key = None
        
        # Buscar clave primaria
        if i < len(lines) and lines[i].startswith("PK:"):
            primary_key = lines[i].replace("PK:", "").strip()
            i += 1
        
        # Saltar encabezados de la tabla
        while i < len(lines) and ("Nombre" in lines[i] or "-" in lines[i]):
            i += 1
        
        # Inicializar listas de reglas
        column_exists = []
        column_data_type = []
        is_complete = []
        is_unique = []
        is_primary_key = []
        
        # Extraer columnas y sus atributos
        while i < len(lines):
            line = lines[i].strip()
            if not line or line.startswith("# TABLA:"):
                break  # Si encontramos una nueva tabla, salimos
            
            parts = line.split()
            if len(parts) < 2:  # Evitar líneas inválidas
                i += 1
                continue
            
            col_name = parts[0]
            is_not_null = "NOT NULL" in line
            data_type = convert_data_type(parts[-1])
            
            # Agregar reglas
            column_exists.append(f"ColumnExists \"{col_name}\"",)
            column_data_type.append(f"ColumnDataType \"{col_name}\" = \"{data_type}\"",)
            if is_not_null or col_name == primary_key:
                is_complete.append(f"IsComplete \"{col_name}\"",)
            if col_name == primary_key:
                is_unique.append(f"IsUnique \"{col_name}\"",)
                is_primary_key.append(f"IsPrimaryKey \"{col_name}\"")
            
            i += 1
        
        # Consolidar reglas en el orden correcto
        rules = column_exists + column_data_type + is_complete + is_unique + is_primary_key
        
        # Asegurar que la última regla termina con "," excepto la última de la lista
        for j in range(len(rules) - 1):
            rules[j] = rules[j] + ","
        
        # Agregar la tabla al JSON
        if schema_name not in output_json:
            output_json[schema_name] = []
        
        output_json[schema_name].append({
            "table": table_name,
            "rules": rules,
            "db": "alfaprd",
            "schema": schema_name,
            "status": True
        })

# Guardar el JSON en S3
s3.put_object(Bucket=output_bucket_name, Key=output_key, Body=json.dumps(output_json, indent=4))

print(f"Archivo JSON generado y guardado en S3: s3://{output_bucket_name}/{output_key}")
from awsglue import DynamicFrame
from pyspark.sql.functions import col, trim, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

DB_CATALOG_GLUE = 'datalake-dev-silver-catalog-db'
df_anp_detalle_solicitudes_giro = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_DETALLE_SOLICITUDES_GIRO").toDF()
df_anp_distribucion_vig_pension = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_DISTRIBUCION_VIG_PENSION").toDF()
df_anp_pensionado_novedad = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PENSIONADO_NOVEDAD").toDF()
df_anp_pensiones = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PENSIONES").toDF()
df_anp_periodo_solicitudes_giro = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PERIODO_SOLICITUDES_GIRO").toDF()
df_cgr_conceptos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_CONCEPTOS").toDF()
df_cgr_origenes = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_ORIGENES").toDF()
#join anp_pensionado_novedad ->  anp_distribucion_vig_pension -> anp_pensiones -> cgr_origenes
df_join_anp_pensionado = df_anp_pensionado_novedad.join(
    df_anp_distribucion_vig_pension,
    df_anp_pensionado_novedad["nid_distribucion_vigencia"] == df_anp_distribucion_vig_pension["nid_distribucion_vigencia"],
    "left"
)
df_join_anp_pensionado = df_join_anp_pensionado.join(
    df_anp_pensiones,
    df_anp_distribucion_vig_pension["nid_pension"] == df_anp_pensiones["nid_pension"],
    "left"
)

df_join_anp_pensionado = df_join_anp_pensionado.join(
    df_cgr_origenes,
    df_anp_pensiones["cod_origen"] == df_cgr_origenes["cod_origen"],
    "left"
)
# filter where cod_concepto
df_join_anp_pensionado_filtered = df_join_anp_pensionado.filter(
    df_anp_pensionado_novedad["cod_concepto"] == '28'
)
#select
df_join_anp_pensionado_filtered = df_join_anp_pensionado_filtered.select(
    df_anp_pensionado_novedad["nid_numero_identificacion"],
    df_anp_pensionado_novedad["cod_tipo_identificacion"],
    df_anp_pensiones["nid_procedencia"],
    df_anp_pensiones["fec_expedicion"],
    df_cgr_origenes["nombre"],
    df_anp_pensionado_novedad["vlr_total"].alias("valor_total"),
    df_anp_pensionado_novedad["fec_ingreso_partition"]
)
# join anp_periodo_solicitudes_giro -> cgr_conceptos -> anp_detalle_solicitudes_giro
df_join_anp_periodo_solicitudes_giro= df_anp_periodo_solicitudes_giro.join(
    df_cgr_conceptos,
    df_anp_periodo_solicitudes_giro["cod_concepto_anp"] == df_cgr_conceptos["cod_concepto"],
    "left"
)


df_join_anp_periodo_solicitudes_giro = df_join_anp_periodo_solicitudes_giro.join(
    df_anp_detalle_solicitudes_giro,
    df_anp_periodo_solicitudes_giro["nid_periodo_solicitud_giro"] == df_anp_detalle_solicitudes_giro["nid_periodo_solicitud_giro"],
    "left"
)
# filter bln_anulado_point, nombre_concepto, num_solicitud_tes
df_join_anp_periodo_solicitudes_giro_filtered = df_join_anp_periodo_solicitudes_giro.filter(
    (df_anp_periodo_solicitudes_giro["bln_anulado_point"] == "N") &  # Filter for non-anulled records
    (df_cgr_conceptos["nombre_concepto"] == "PAGO AUXILIO FUNERARIO") &  # Filter for specific concept
    (df_anp_periodo_solicitudes_giro["num_solicitud_tes"].isNotNull())  # Ensure num_solicitud_tes is not null
)

# Select DISTINCT rows with required columns
df_join_anp_periodo_solicitudes_giro_filtered = df_join_anp_periodo_solicitudes_giro_filtered.select(
    df_anp_detalle_solicitudes_giro["nid_numero_identificacion"],
    df_anp_detalle_solicitudes_giro["cod_tipo_identificacion"],
    df_anp_periodo_solicitudes_giro["num_solicitud_tes"],
    df_cgr_conceptos["nombre_concepto"],
    df_anp_periodo_solicitudes_giro["fec_envio_tes"]
).distinct()  # Ensure distinct rows
# JOIN subqueries
df_join_subqueries = df_join_anp_pensionado_filtered.join(
    df_join_anp_periodo_solicitudes_giro_filtered,
    (df_join_anp_pensionado_filtered["nid_numero_identificacion"] == df_join_anp_periodo_solicitudes_giro_filtered["nid_numero_identificacion"]) &
    (df_join_anp_pensionado_filtered["cod_tipo_identificacion"] == df_join_anp_periodo_solicitudes_giro_filtered["cod_tipo_identificacion"]),
    "left"
)
# Select the required columns with the exact DataFrame references
df_auxilios_funerarios_query = df_join_subqueries.select(
    df_join_anp_periodo_solicitudes_giro_filtered["nid_numero_identificacion"].alias("id_afiliado"),
    df_join_anp_periodo_solicitudes_giro_filtered["cod_tipo_identificacion"].alias("tipo_identificacion_afiliado"),
    df_join_anp_periodo_solicitudes_giro_filtered["num_solicitud_tes"],
    df_join_anp_periodo_solicitudes_giro_filtered["nombre_concepto"],
    df_join_anp_pensionado_filtered["nid_procedencia"].alias("numero_poliza"),
    df_join_anp_pensionado_filtered["fec_expedicion"],
    df_join_anp_pensionado_filtered["nombre"].alias("origen"),
    df_join_anp_pensionado_filtered["valor_total"],
    df_join_anp_periodo_solicitudes_giro_filtered["fec_envio_tes"]
).distinct()  # Ensure DISTINCT rows

#Add partition to table 
df_auxilios_funerarios_query = df_auxilios_funerarios_query.withColumn("PERIODO_PARTITION", date_format(df_join_anp_pensionado_filtered["fec_expedicion"], "yyyy-MM"))

#modify dates for table 
df_auxilios_funerarios_query = df_auxilios_funerarios_query.withColumn("Periodo_de_expedicion_de_la_poliza", date_format(df_auxilios_funerarios_query["fec_expedicion"], "yyyyMM")) \
                                                           .withColumn("Mes_de_pago", date_format(df_auxilios_funerarios_query["fec_envio_tes"], "yyyyMM"))
df_auxilios_funerarios_query.show()
#Select final dataframe for gold stage
df_auxilios_gold = df_auxilios_funerarios_query.select(
    df_auxilios_funerarios_query["numero_poliza"].alias("Numero_de_poliza"),
    df_auxilios_funerarios_query["Periodo_de_expedicion_de_la_poliza"].alias("Periodo_de_expedicion_de_la_poliza"),
    df_auxilios_funerarios_query["origen"].alias("Origen_de_la_renta"),
    df_auxilios_funerarios_query["valor_total"].cast(FloatType()).alias("Valor_auxilio_funerario_pagado"),
    df_auxilios_funerarios_query["Mes_de_pago"].alias("Mes_de_pago"),
    df_auxilios_funerarios_query["PERIODO_PARTITION"].alias("PERIODO_PARTITION")
)
df_auxilios_gold.show()
# Select the required columns with the exact DataFrame references
df_auxilios_funerarios = df_join_subqueries.select(
    df_join_anp_periodo_solicitudes_giro_filtered["nid_numero_identificacion"].alias("id_afiliado"),
    df_join_anp_periodo_solicitudes_giro_filtered["cod_tipo_identificacion"].alias("tipo_identificacion_afiliado"),
    df_join_anp_periodo_solicitudes_giro_filtered["num_solicitud_tes"],
    df_join_anp_periodo_solicitudes_giro_filtered["nombre_concepto"],
    df_join_anp_pensionado_filtered["nid_procedencia"].alias("numero_poliza"),
    df_join_anp_pensionado_filtered["fec_expedicion"],
    df_join_anp_pensionado_filtered["nombre"].alias("origen"),
    df_join_anp_pensionado_filtered["valor_total"],
    df_join_anp_periodo_solicitudes_giro_filtered["fec_envio_tes"],
    df_join_anp_pensionado_filtered["fec_ingreso_partition"]
)  # Ensure DISTINCT rows

# Show the resulting DataFrame
df_auxilios_funerarios.show(truncate=False)

df_auxilios_funerarios.count()
# Map the selected data to the schema
df_auxilios_gold = df_auxilios_funerarios.select(
    df_auxilios_funerarios["numero_poliza"].alias("Numero_de_poliza"),
    df_auxilios_funerarios["fec_expedicion"].alias("Periodo_de_expedicion_de_la_poliza"),
    df_auxilios_funerarios["origen"].alias("Origen_de_la_renta"),
    df_auxilios_funerarios["valor_total"].cast(FloatType()).alias("Valor_auxilio_funerario_pagado"),
    df_auxilios_funerarios["fec_envio_tes"].alias("Mes_de_pago"),
    df_auxilios_funerarios["fec_ingreso_partition"].alias("PERIODO_PARTITION")
)

# Show the resulting DataFrame with the new schema
df_auxilios_gold.show(truncate=False)
# Save the transformed DataFrame to S3
frame_save = DynamicFrame.fromDF(df_auxilios_gold, glueContext, "frame_save_dynamic")
s3_path = f"s3://alfa-datalake-dev-gold/GOLD/NIIF/AUXILIOS_FUNERARIOS/"
job.commit()