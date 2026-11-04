# -----------------------------
# Imports y Configuración
# -----------------------------
import pandas as pd
import tempfile
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType, DateType
from awsglue import DynamicFrame
from awsglue.transforms import SelectFromCollection
from awsgluedq.transforms import EvaluateDataQuality
import inspect
from datetime import datetime
from zoneinfo import ZoneInfo
import os
import traceback
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO


# Configuración de rutas S3 para logs - se configurarán desde job parameters de AWS Glue
path_log = None
path_formato_ulaes = None

# Initialize a global dataframe to store logs with enhanced structure
log_columns = ["caso_de_uso","columna", "indice", "fecha_ejecucion", "origen_id_lote", "estado", "fallo", "regla"]
log_df = pd.DataFrame(columns=log_columns)

# -----------------------------
# Funciones de Logging Base
# -----------------------------
def log_validation_failure(caso_de_uso, columna, indice, fallo, origen_id_lote=None, estado=0):
    """
    Enhanced logging function that automatically detects the calling function name
    and logs validation failures with detailed information.
    """
    global log_df
    
    frame = inspect.currentframe()
    try:
        caller_frame = frame.f_back
        regla_func_name = caller_frame.f_code.co_name
        
        if regla_func_name == '<lambda>':
            caller_caller_frame = caller_frame.f_back
            if caller_caller_frame:
                regla_func_name = caller_caller_frame.f_code.co_name
    except:
        regla_func_name = "unknown_function"
    finally:
        del frame
    
    #timestamp = datetime.now().isoformat()
    timestamp = datetime.now(ZoneInfo("Etc/GMT+5")).strftime("%Y-%m-%d %H:%M:%S")
    
    new_log = {
        "caso_de_uso": caso_de_uso,
        "columna": columna,
        "indice": indice,
        "fecha_ejecucion": timestamp,
        "origen_id_lote": origen_id_lote,
        "estado": estado,
        "fallo": fallo,
        "regla": regla_func_name
    }
    
    log_df = pd.concat([log_df, pd.DataFrame([new_log])], ignore_index=True)
    logger.info(f"LOG REGISTERED: {regla_func_name} - {columna}[{indice}]: {fallo}")


def log_column_validation(expected_columns, actual_columns, case_use, df_row_count=0):
    """
    Logs column presence and order validation results.
    """
    missing_columns = [col for col in expected_columns if col not in actual_columns]
    extra_columns = [col for col in actual_columns if col not in expected_columns]
    
    if missing_columns:
        fallo_msg = f"Faltan columnas requeridas: {', '.join(missing_columns)}"
        log_validation_failure(
            caso_de_uso = f"{case_use}",
            columna="columnas_faltantes", 
            indice=0, 
            fallo=fallo_msg,
            estado=0
        )
    
    if extra_columns:
        fallo_msg = f"Columnas adicionales no esperadas: {', '.join(extra_columns)}"
        log_validation_failure(
            caso_de_uso = f"{case_use}",
            columna="columnas_extra", 
            indice=0, 
            fallo=fallo_msg,
            estado=0
        )
    
    if not missing_columns and not extra_columns:
        
        logger.info("Todas las columnas requeridas están presentes")
    
    if len(expected_columns) == len(actual_columns):
        out_of_order_columns = []
        for i, (expected, actual) in enumerate(zip(expected_columns, actual_columns)):
            if expected != actual:
                out_of_order_columns.append(f"Posición {i}: esperado '{expected}', encontrado '{actual}'")
        
        if out_of_order_columns:
            fallo_msg = f"Columnas fuera de orden: {'; '.join(out_of_order_columns)}"
            log_validation_failure(
                caso_de_uso = f"{case_use}",
                columna="columnas_orden", 
                indice=0, 
                fallo=fallo_msg,
                estado=0
            )
        else:
            logger.info("Todas las columnas están en el orden correcto")


def log_row_validation_failure(df_dataquality, case_use):
    """
    Process DataFrame of DataQuality  and write errors usando log_validation_failure.
    """
    
    df_errores_pandas = df_dataquality.toPandas()
    
    for _, row in df_errores_pandas.iterrows():
        log_validation_failure(
            caso_de_uso =f"{case_use}",
            columna =row.get("rule_type", "columna_desconocida"),
            indice =row.get("excel_row", -1),
            fallo =row.get("DataQualityRulesFail", "Sin detalle de fallo"),
            estado =0
        )


def save_logs_to_s3_parquet(s3_client, s3_path):
    """
    Function to save logs to S3 in Parquet format.
    """
    global log_df
    
    if log_df.empty:
        logger.info("No logs to save.")
        return
    
    try:
        if not s3_path.startswith('s3://'):
            raise ValueError("S3 path must start with 's3://'")
        
        path_without_prefix = s3_path[5:]
        bucket, key = path_without_prefix.split('/', 1)
        
        # Convert pandas DataFrame to PyArrow Table
        table = pa.Table.from_pandas(log_df)
        
        # Write to BytesIO buffer
        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_content = parquet_buffer.getvalue()
        
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=parquet_content,
            ContentType='application/octet-stream'
        )
        
        logger.info(f"Logs successfully saved to Parquet: {s3_path}")
        parquet_buffer.close()
        
    except Exception as e:
        logger.info(f"Error saving logs to S3 Parquet: {str(e)}")
        raise

def save_consolidated_logs(s3_client, table_name=None):
    """
    Saves all accumulated logs to a single consolidated file in S3 in Parquet format only.
    """
    global log_df
    
    if log_df.empty:
        logger.info("No logs to save.")
        return
    
    try:
        timestamp = datetime.now(ZoneInfo("Etc/GMT+5")).strftime("%Y-%m-%d %H:%M:%S")
        table_suffix = f"_{table_name}" if table_name else ""
        
        # Save as Parquet format only
        consolidated_log_path_parquet = f"{path_log}consolidated_logs{table_suffix}_{timestamp}.parquet"
        save_logs_to_s3_parquet(s3_client, consolidated_log_path_parquet)
        logger.info(f"All logs consolidated and saved to Parquet: {consolidated_log_path_parquet}")
        
        # Clear logs after saving to prevent accumulation
        clear_logs()
        
    except Exception as e:
        logger.info(f"Error saving consolidated logs: {str(e)}")
        raise

def clear_logs():
    """
    Clears the global log DataFrame to prevent accumulation between executions.
    """
    global log_df
    log_df = pd.DataFrame(columns=log_columns)
    logger.info("Log DataFrame cleared.")

def save_format_file(s3_client, df_spark, path_files):
    """
    Saves the processed DataFrame as Format file in Parquet format (.parquet) in the specified S3 path.
    """
    try:
        
        timestamp = datetime.now(ZoneInfo("Etc/GMT+5")).strftime("%Y-%m-%d %H:%M:%S")

        path_save_files = f"{path_files}/fecha_carga={timestamp}/"
        
        # Save the DataFrame in Parquet format to the output path
        logger.info(f'Saving the file in Parquet format to {path_save_files}')
        df_spark.write.mode("overwrite").parquet(path_save_files)
        logger.info('File successfully saved in Parquet format')
        
        logger.info(f"Parquet file successfully saved to: {path_save_files}")
            
    except Exception as e:
        logger.info(f"Error saving Format Parquet file: {str(e)}")
        log_validation_failure(
            caso_de_uso = f"{case_use}",
            columna="format_file", 
            indice=0, 
            fallo=f"Error al guardar archivo Format Parquet: {str(e)}",
            estado=0
        )
        raise

def get_log_summary():
    """
    Returns a summary of the current logs for debugging purposes.
    """
    global log_df
    if log_df.empty:
        return "No logs recorded."
    
    summary = f"""
    Total logs: {len(log_df)}
    Failed validations: {len(log_df[log_df['cumplio'] == 0])}
    Successful validations: {len(log_df[log_df['cumplio'] == 1])}
    Unique rules: {log_df['regla'].nunique()}
    Unique columns: {log_df['columna'].nunique()}
    """
    return summary

# -----------------------------
# Funciones de Utilidad
# -----------------------------
def convertir_excel_a_csv_fijo(path_xlsx):
    """
    Convierte un archivo Excel a CSV manteniendo la estructura fija.
    """
    import zipfile
    import xml.etree.ElementTree as ET
    import csv

    with zipfile.ZipFile(path_xlsx) as z:
        shared_strings = []
        if 'xl/sharedStrings.xml' in z.namelist():
            tree = ET.parse(z.open('xl/sharedStrings.xml'))
            for si in tree.getroot():
                text = ''.join([t.text or '' for t in si.iter() if t.tag.endswith('t')])
                shared_strings.append(text)

        sheet = ET.parse(z.open('xl/worksheets/sheet1.xml')).getroot()
        data = []
        encabezado = []

        for row in sheet.iter():
            if row.tag.endswith('row'):
                row_data = []
                row_index = int(row.attrib.get("r"))
                col_map = {}

                for cell in row:
                    if cell.tag.endswith('c'):
                        cell_type = cell.attrib.get('t')
                        cell_ref = cell.attrib.get('r')
                        col_letter = ''.join(filter(str.isalpha, cell_ref))

                        value = ""
                        for child in cell:
                            if child.tag.endswith('v'):
                                value = child.text
                        if cell_type == 's' and value:
                            value = shared_strings[int(value)]
                        col_map[col_letter] = value

                sorted_cols = sorted(col_map.items(), key=lambda x: x[0])
                row_data = [col_map.get(chr(65 + i), "") for i in range(len(col_map))]

                if row_index == 1:
                    encabezado = row_data
                else:
                    row_dict = {"excel_row": str(row_index)}
                    for i in range(len(encabezado)):
                        col_name = encabezado[i] if i < len(encabezado) else f"COL_{i}"
                        row_dict[col_name] = row_data[i] if i < len(row_data) else ""
                    data.append(row_dict)

    temp_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode='w', newline='', encoding='utf-8')
    fieldnames = ["excel_row"] + encabezado
    writer = csv.DictWriter(temp_csv, fieldnames=fieldnames)
    writer.writeheader()
    for row in data:
        writer.writerow(row)
    temp_csv.close()
    return temp_csv.name

def filtrar_reglas_por_tipo(rules_list, tipos_regla):
    """
    Filtra las reglas que contienen alguno de los tipos dados.
    """
    reglas_filtradas = []
    for regla in rules_list:
        regla_limpia = regla.strip().rstrip(",")
        if any(tipo in regla_limpia for tipo in tipos_regla):
            reglas_filtradas.append(regla_limpia)
    return reglas_filtradas

# -----------------------------
# Funciones de Procesamiento
# -----------------------------
def excel_s3_to_spark_df(s3_client, bucket, key_xlsx, spark, glueContext, case_use):
    """
    Convierte archivo Excel de S3 a Spark DataFrame con logging integrado.
    """
    
    try:
        excel_file_path = f"s3://{bucket}/{key_xlsx}"
        
        df = pd.read_excel(excel_file_path)
        
        df.insert(0, "excel_row", range(2, 2 + len(df)))
        
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(0)
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].fillna(pd.Timestamp("1900-01-01"))
            else:
                df[col] = df[col].fillna("").astype(str)

        df_spark = spark.createDataFrame(df)
        
        expected_columns = df.columns.tolist()
        actual_columns = df_spark.columns
        log_column_validation(expected_columns, actual_columns, len(df))

        return df_spark

    except FileNotFoundError:
        logger.info("File Not Found")
        raise
    
    except Exception as e:
        log_validation_failure(
            caso_de_uso = f"{case_use}",
            columna="proceso", 
            indice=0, 
            fallo=f"Error al procesar archivo Excel: {str(e)}",
            estado=0
        )
        raise

def glue_quality(df, rules):
    """
    Ejecuta evaluación de calidad de datos usando AWS Glue.
    """
    try:
        EvaluateDataQuality_ruleset = f"""Rules = [{rules}]"""
        
        EvaluateDataQualityMultiframe = EvaluateDataQuality().process_rows(
            frame=df,
            ruleset=EvaluateDataQuality_ruleset,
            publishing_options={
                "dataQualityEvaluationContext": f"DataQuality_functions",
                "enableDataQualityMetrics": True,
                "enableDataQualityResultsPublishing": False,
            },
            additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
        )      
        
        ruleOutcomes = SelectFromCollection.apply(
            dfc=EvaluateDataQualityMultiframe,
            key="rowLevelOutcomes",
            transformation_ctx="rowLevelOutcomes",
        )
        
        return ruleOutcomes.toDF()
    
    except Exception as e:
        log_validation_failure(
            caso_de_uso = f"{case_use}",
            columna="calidad", 
            indice=0, 
            fallo=f"Error en evaluación de calidad: {str(e)}",
            estado=0
        )
        raise

# -----------------------------
# Funciones de Validación
# -----------------------------
def verificar_archivo(s3_client, bucket, key, case_use):
    """
    Verifica si un archivo existe en un bucket de S3.
    """
    try:
        logger.info(f"Checking file existence: s3://{bucket}/{key}")
        s3_client.head_object(Bucket=bucket, Key=key)
        logger.info(f"SUCCESS: The file '{key}' exists in the bucket '{bucket}'.")
        
        return True
        
    except s3_client.exceptions.NoSuchKey:
        logger.info(f"ERROR: The file '{key}' does not exist in the bucket '{bucket}'.")
        logger.info(f"Full path checked: s3://{bucket}/{key}")
        
        return False
        
    except Exception as e:
        logger.info(f"Unexpected error accessing the file in S3: {str(e)}")
        logger.info(f"Full path checked: s3://{bucket}/{key}")
        logger.info(f"Error type: {type(e).__name__}")
        
        log_validation_failure(
            caso_de_uso = f"{case_use}",
            columna="archivo_verificacion", 
            indice=0, 
            fallo=f"Error inesperado al verificar archivo: {str(e)}",
            estado=0
        )
        
        return False

def verificar_estructura_archivo(df, columnas_esperadas,  case_use, s3_client=None):
    """
    Verifica si el DataFrame tiene las columnas esperadas en el orden correcto.
    """
    try:
        columnas_actuales = df.columns
        errores = []
     
        for idx, col in enumerate(columnas_esperadas):
            if idx >= len(columnas_actuales) or columnas_actuales[idx] != col:
                error_msg = (
                    f"Columna esperada en posición {idx}: '{col}', "
                    f"encontrada: '{columnas_actuales[idx] if idx < len(columnas_actuales) else 'N/A'}'"
                )
                errores.append(error_msg)
                
                log_validation_failure(
                    caso_de_uso = f"{case_use}",
                    columna=f"estructura_pos_{idx}",
                    indice=idx,
                    fallo=error_msg,
                    estado=0
                )
     
        if errores:
            logger.info("Invalid file structure:")
            for err in errores:
                logger.info(f"   - {err}")
            
            log_validation_failure(
                caso_de_uso = f"{case_use}",
                columna="estructura_archivo",
                indice=0,
                fallo=f"Estructura de archivo inválida. Errores encontrados: {len(errores)}",
                estado=0
            )
            
            raise ValueError("Invalid column structure.")
        else:
            logger.info("File structure validated successfully.")
            
    except Exception as e:
        log_validation_failure(
            caso_de_uso = f"{case_use}",
            columna="estructura_validacion",
            indice=0,
            fallo=f"Error inesperado al validar estructura: {str(e)}",
            estado=0
        )
        
        raise

def verificar_atributo(df, rules):
    df_original = df.toDF()  # Convertimos el DynamicFrame original a Spark DF
    df_original = df_original.withColumn("__row_id__", F.monotonically_increasing_id())
 
    # Ejecutar reglas de calidad
    df_with_index = DynamicFrame.fromDF(df_original, glueContext, "df_with_index")
    df_results = glue_quality(df_with_index, rules)
    
    # Filtrar solo fallos
    df_failed = df_results.filter(F.col("DataQualityEvaluationResult") == "Failed")
    
    df_failed = df_failed.withColumn("DataQualityRulesFail", explode(col("DataQualityRulesFail")))

    return df_failed

# -----------------------------
# Funciones de disponibilidad
# -----------------------------

def move_file(s3_client, bucket_bronce, bucket_landing, source_key, case_use, file, extension, status):
    
    timestamp = datetime.now(ZoneInfo("Etc/GMT+5")).strftime("%Y-%m-%d %H:%M:%S")
    
    
    destinations = {bucket_landing: f"{case_use}/{status}/{file}/fecha_carga={timestamp}/{file}{extension}"}
    
    # if status is 'OUT', make two copies
    if status.upper() == "OUT":
        destinations.update({bucket_bronce: f"FILES/{file}/fecha_carga={timestamp}/{file}{extension}"})

    for bucket, dest_key in destinations.items():
        
        logger.info(f"BUCKET {bucket}")
        logger.info(f"BUCKET {dest_key}")

        try:
            s3_client.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket_landing, 'Key': source_key},
                Key=dest_key
            )
            #s3_client.delete_object(Bucket=bucket, Key=source_key)
            logger.info(f"File moved to path {status}: s3://{bucket}/{dest_key}")
            
        except Exception as e:
            
            logger.info(f"Error moving file to {status}: {e}")


def modify_df_schema(df: DataFrame, changes_schema: dict):
    """
    modify schema a dataframe allow schema 
    """
    df_schema = df
    
    logger.info(changes_schema)
    
    for name_col, new_type in changes_schema.items():
        df_schema = df_schema.withColumn(name_col, col(name_col).cast(new_type))
    
    logger.info(df_schema.logger.infoSchema())
    
    return df_schema
    
    

#########################################################################################################################################################################################################
"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F
from commons.ExtractHelper import ExtractHelper
from commons.LogsHelper import LogsHelper
import pandas as pd
from pathlib import Path
import sys, traceback, boto3, json, tempfile, zipfile
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

###################################################################
######## SET ARGS AND CONFIG COMMONS UTILS ########################
###################################################################
logs = LogsHelper()
logger = logs.logger_conf()

args = getResolvedOptions(
    sys.argv, 
        [
        'JOB_NAME',
        'DB_SCHEMA',
        'DB_NAME',
        'BUCKET_NAME',
        'BUCKET_NAME_ASSETS',
        'CONNECT_NAME', 
        'PERIOD',
        'CERTIFICATE',
        'PATH_LOGS_ULAES',
        'PATH_FORMATO_ULAES',
        'EXCEL_FILE_PATH',
        'FILES',
        'BUCKET_SOURCE',
        'EXTENSION',
        'COLUMNAS_ESPERADAS_NIIF17_ULAES_VIDA',
        'COLUMNAS_ESPERADAS_NIIF17_ULAES_GENERALES',
        'COLUMNAS_ESPERADAS_NIIF17_PREVISIONAL',
        'SCHEMA_NIIF17_ULAES_VIDA',
        'SCHEMA_NIIF17_ULAES_GENERALES',
        'SCHEMA_NIIF17_PREVISIONAL'
        ]
    )

DB_SCHEMA = args['DB_SCHEMA']
DB_NAME = args['DB_NAME']
BUCKET_NAME = args['BUCKET_NAME']
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
BUCKET_SOURCE = args['BUCKET_SOURCE']
CONNECT_NAME = args['CONNECT_NAME']
PERIOD = args['PERIOD']
CERTIFICATE = args['CERTIFICATE']


# Configure path variables from job parameters
path_log = args['PATH_LOGS_ULAES']
path_formato_ulaes = args['PATH_FORMATO_ULAES']
EXCEL_FILE_PATH = args['EXCEL_FILE_PATH']
FILES = args['FILES']
EXTENSION = args['EXTENSION']


# Parse FILES from job parameters (JSON string to Python list)
files = json.loads(args['FILES'])
# Parse COLUMNAS_ESPERADAS from job parameters (JSON string to Python list)
columnas_esperadas = {
    "NIIF17_ULAES_VIDA": json.loads(args['COLUMNAS_ESPERADAS_NIIF17_ULAES_VIDA']),
    "NIIF17_ULAES_GENERALES": json.loads(args['COLUMNAS_ESPERADAS_NIIF17_ULAES_GENERALES']),
    "NIIF17_PREVISIONAL": json.loads(args['COLUMNAS_ESPERADAS_NIIF17_PREVISIONAL'])
}


SCHEMA_NIIF17_ULAES_VIDA = {
    "COMPANIA": StringType(),
    "TIPO_ULAE": StringType(),
    "CODRAMO": StringType(),
    "INDBANCAS": StringType(),
    "PERIODO": StringType(),
    "ULAE": IntegerType()
}
SCHEMA_NIIF17_ULAES_GENERALES = {
    "COMPANIA": StringType(),
    "TIPO_ULAE": StringType(),
    "CODRAMO": StringType(),
    "INDBANCAS": StringType(),
    "PERIODO": StringType(),
    "ULAE": IntegerType()
}
SCHEMA_NIIF17_PREVISIONAL = {
    "POLIZA": StringType(),                
    "REVERSION": DoubleType(),
    "REAL": DoubleType(),
    "CAUSACION": DoubleType(),
    "EMISION_P&G": DoubleType(),
    "INICIO_DE_VIGENCIA": DateType(),
    "FIN_DE_VIGENCIA": DateType(),
    "MES_DE_CIERRE": StringType(),
    "VALOR_PRIMA_CEDIDA": DoubleType()
}

schema_output = {
    "NIIF17_ULAES_VIDA": SCHEMA_NIIF17_ULAES_VIDA,
    "NIIF17_ULAES_GENERALES": SCHEMA_NIIF17_ULAES_GENERALES,
    "NIIF17_PREVISIONAL": SCHEMA_NIIF17_PREVISIONAL
}


logger.info(f"Period: {PERIOD}")
logger.info(f"Schema: {DB_SCHEMA}")
logger.info(f"Connect: {CONNECT_NAME}")
logger.info(f"Bucket_name: {BUCKET_NAME}")
logger.info(f"Columnas esperadas: {columnas_esperadas}")
logger.info(f"PATH_LOGS_ULAES: {path_log}")
logger.info(f"PATH_FORMATO_ULAES: {path_formato_ulaes}")
logger.info(f"EXCEL_FILE_PATH: {EXCEL_FILE_PATH}")
logger.info(f"FILES: {FILES}")
logger.info(f"BUCKET_SOURCE: {BUCKET_SOURCE}")
logger.info(f"SCHEMA: {schema_output}")



# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3_client = boto3.client("s3")

extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME, DB_NAME)

###################################################################
################# ITERATE FOR EACH FILE  ##########################
###################################################################


for case_use, file in files.items():
    
    # Check if the file exists before attempting to read it
    # File Dictionary - Using parameters from job configuration
    excel_file_path = f"s3://{BUCKET_SOURCE}/{case_use}/IN/{file}{EXTENSION}"
    rules_file_path = f"s3://{BUCKET_NAME_ASSETS}/support_phase_files/niif/{file.lower()}_rules_tables.json"
    bronce_file_path = f"s3://{BUCKET_NAME}/{case_use}/{file}"
    
    
    key_file = f"{case_use}/IN/{file}{EXTENSION}"
    key_rules = f"support_phase_files/niif/{file.lower()}_rules_tables.json"
    
    columnas_esperadas_case = columnas_esperadas[case_use]
    schema_output_case = schema_output[case_use]
    
    logger.info(f"columnas: {columnas_esperadas_case}")
    
    if verificar_archivo(s3_client, BUCKET_SOURCE, key_file, case_use):
        
        logger.info(f"Loading file and Checking structure from s3://{excel_file_path}")
        
        frame_save_from_file = excel_s3_to_spark_df(s3_client, BUCKET_SOURCE, key_file, spark, glueContext, case_use)
        
        frame_save = modify_df_schema(frame_save_from_file, schema_output_case)
        
        verificar_estructura_archivo(frame_save, columnas_esperadas_case, case_use)
        
        # Convert to DynamicFrame
        frame_save = DynamicFrame.fromDF(frame_save, glueContext, "frame_save_dynamic")
        logger.info(f"Excel file uploaded and validated: {excel_file_path}")
        
        if verificar_archivo(s3_client, BUCKET_NAME_ASSETS, key_rules, case_use):
            
            logger.info(f"Loading rules from s3://{rules_file_path}")
            
            response = s3_client.get_object(Bucket=BUCKET_NAME_ASSETS, Key=key_rules)
            catalog_rules = json.loads(response["Body"].read().decode("utf-8"))
            
            logger.info("Rules JSON loaded successfully")
            
            
            # Dictionary to store EvaluateDataQualityMultiframe for each table
    
            for schema in catalog_rules:
                if schema in DB_SCHEMA:
                    for table_info in catalog_rules[schema]:
                            if table_info['status']:
                                try:
                                    frame_to_evaluate = frame_save
                                    #logger.info(f"Tables for ingest: {schema}.{table_info['rules']}")
                                    
                                    all_rules = table_info['rules']
                                    
                                    tipos_deseados = ["ColumnExists", "ColumnLength", "ColumnValues", "IsComplete", "CustomSql", "ColumnDataType"]
                                    
                                    df_failes_outcomes_total = None
                                    
                                    for tipo in tipos_deseados:
                                        filtered_rules = filtrar_reglas_por_tipo(all_rules, [tipo])
                                        if filtered_rules:
                                            # Dynamically build the set of rules
                                            rules_str = ",\n".join(filtered_rules)
                                            df_fails = verificar_atributo(frame_to_evaluate, rules_str)
                                            df_fails = df_fails.withColumn("rule_type", F.lit(tipo))
                                            if df_failes_outcomes_total is None:
                                                    df_failes_outcomes_total = df_fails
                                            else:
                                                df_failes_outcomes_total = df_failes_outcomes_total.unionByName(df_fails, allowMissingColumns=True)
                                                
                                    logger.info("Successfully Evaluate Data Quality ")
                                    
                                    if df_failes_outcomes_total.count() == 0:
                                        # Save the processed DataFrame as file
                                        try:
                                            save_format_file(s3_client, frame_save.toDF(), bronce_file_path)
                                            logger.info("File saved successfully.")
                                            move_file(s3_client, BUCKET_NAME, BUCKET_SOURCE, key_file, case_use, file, EXTENSION, status = 'OUT')
                                            
                                        except Exception as e:
                                            move_file(s3_client, BUCKET_NAME, BUCKET_SOURCE, key_file, case_use, file, EXTENSION, status = 'ERROR')
                                            logger.info(f"Error saving File: {e}")
                                    else:
                                        
                                        log_row_validation_failure(df_failes_outcomes_total, case_use)
                                        move_file(s3_client, BUCKET_NAME, BUCKET_SOURCE, key_file, case_use, file, EXTENSION, status = 'ERROR')
                                        logger.info(f"Data Quality is Wrong")
                                        
                                except Exception as e:
                                    
                                    logger.info(f"ERROR WITH Evaluate Data Quality: {e}")
                else:
                    
                    logger.info("NOT TABLE FOR Evaluate Data Quality")
            
        else: 
            move_file(s3_client, BUCKET_NAME, BUCKET_SOURCE, key_file, case_use, file, EXTENSION, status = 'ERROR')
            logger.info(f"CRITICAL ERROR: File 's3://{BUCKET_SOURCE}/{case_use}/IN/{file}{EXTENSION}' not found in bucket '{BUCKET_NAME_ASSETS}'. Please verify the path and permissions.")
            
        
    else:
        move_file(s3_client, BUCKET_NAME, BUCKET_SOURCE, key_file, case_use, file, EXTENSION, status = 'ERROR')
        logger.info(f"CRITICAL ERROR: File 's3://{BUCKET_SOURCE}/{case_use}/IN/{file}{EXTENSION}' not found in bucket '{BUCKET_SOURCE}'. Please verify the path and permissions.")
        
        


# Save all accumulated logs to a single consolidated file
try:
    save_consolidated_logs(s3_client, table_name="ulaes_generales")
    logger.info("All processing logs have been consolidated and saved successfully.")
except Exception as e:
    logger.info(f"Error saving consolidated logs: {e}")

# End of job
job.commit()

    