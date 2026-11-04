# -----------------------------
# Imports y Configuración
# -----------------------------
import pandas as pd
import tempfile
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from awsglue import DynamicFrame
from awsglue.transforms import SelectFromCollection
from awsgluedq.transforms import EvaluateDataQuality
import inspect
from datetime import datetime
import os
import traceback
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import openpyxl

# Configuración de rutas S3 para logs
path_log = 's3://alfa-datalake-dev-langding/NIIF_ULAES/TEST_QUALITY/'  # Ruta para logs consolidados
path_formato_ulaes = 's3://alfa-datalake-dev-langding/NIIF_ULAES/TEST_QUALITY/'  # Ruta para archivo Formato_ULAES_PSI_Q

# Initialize a global dataframe to store logs with enhanced structure
log_columns = ["columna", "indice", "timestamp", "origen_id_lote", "cumplio", "fallo", "regla"]
log_df = pd.DataFrame(columns=log_columns)

# -----------------------------
# Funciones de Logging Base
# -----------------------------
def log_validation_failure(columna, indice, fallo, origen_id_lote=None, cumplio=0):
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
    
    timestamp = datetime.now().isoformat()
    
    new_log = {
        "columna": columna,
        "indice": indice,
        "timestamp": timestamp,
        "origen_id_lote": origen_id_lote,
        "cumplio": cumplio,
        "fallo": fallo,
        "regla": regla_func_name
    }
    
    log_df = pd.concat([log_df, pd.DataFrame([new_log])], ignore_index=True)
    print(f"LOG REGISTERED: {regla_func_name} - {columna}[{indice}]: {fallo}")

def log_file_status(archivo_encontrado, ruta_archivo):
    """
    Logs file search status with 1 for found, 0 for not found.
    """
    valor_archivo = 1 if archivo_encontrado else 0
    fallo_msg = f"Archivo {'encontrado' if archivo_encontrado else 'no encontrado'} en ruta: {ruta_archivo}"
    log_validation_failure(
        columna="archivo", 
        indice=0, 
        fallo=fallo_msg,
        cumplio=valor_archivo
    )

def log_column_validation(expected_columns, actual_columns, df_row_count=0):
    """
    Logs column presence and order validation results.
    """
    missing_columns = [col for col in expected_columns if col not in actual_columns]
    extra_columns = [col for col in actual_columns if col not in expected_columns]
    
    if missing_columns:
        fallo_msg = f"Faltan columnas requeridas: {', '.join(missing_columns)}"
        log_validation_failure(
            columna="columnas_faltantes", 
            indice=0, 
            fallo=fallo_msg,
            cumplio=0
        )
    
    if extra_columns:
        fallo_msg = f"Columnas adicionales no esperadas: {', '.join(extra_columns)}"
        log_validation_failure(
            columna="columnas_extra", 
            indice=0, 
            fallo=fallo_msg,
            cumplio=0
        )
    
    if not missing_columns and not extra_columns:
        log_validation_failure(
            columna="columnas_presencia", 
            indice=0, 
            fallo="Todas las columnas requeridas están presentes",
            cumplio=1
        )
    
    if len(expected_columns) == len(actual_columns):
        out_of_order_columns = []
        for i, (expected, actual) in enumerate(zip(expected_columns, actual_columns)):
            if expected != actual:
                out_of_order_columns.append(f"Posición {i}: esperado '{expected}', encontrado '{actual}'")
        
        if out_of_order_columns:
            fallo_msg = f"Columnas fuera de orden: {'; '.join(out_of_order_columns)}"
            log_validation_failure(
                columna="columnas_orden", 
                indice=0, 
                fallo=fallo_msg,
                cumplio=0
            )
        else:
            log_validation_failure(
                columna="columnas_orden", 
                indice=0, 
                fallo="Todas las columnas están en el orden correcto",
                cumplio=1
            )

def log_row_validation_failure(row_data, row_index, column_name, validation_rule, error_message, excel_row=None):
    """
    Logs individual row validation failures with detailed context.
    """
    indice_final = excel_row if excel_row is not None else row_index
    
    fallo_detallado = f"Regla: {validation_rule} | Error: {error_message}"
    if row_data and column_name in row_data:
        fallo_detallado += f" | Valor: '{row_data[column_name]}'"
    
    log_validation_failure(
        columna=column_name,
        indice=indice_final,
        fallo=fallo_detallado,
        cumplio=0
    )

def save_logs_to_s3(s3_client, s3_path):
    """
    Enhanced function to save logs to S3 in txt format.
    """
    global log_df
    
    if log_df.empty:
        print("No logs to save.")
        return
    
    try:
        if not s3_path.startswith('s3://'):
            raise ValueError("S3 path must start with 's3://'")
        
        path_without_prefix = s3_path[5:]
        bucket, key = path_without_prefix.split('/', 1)
        
        from io import StringIO
        csv_buffer = StringIO()
        log_df.to_csv(csv_buffer, sep='\t', index=False)
        log_content = csv_buffer.getvalue()
        
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=log_content.encode('utf-8'),
            ContentType='text/plain'
        )
        
        print(f"Logs successfully saved to {s3_path}")
        csv_buffer.close()
        
    except Exception as e:
        print(f"Error saving logs to S3: {str(e)}")
        raise

def save_logs_to_s3_parquet(s3_client, s3_path):
    """
    Function to save logs to S3 in Parquet format.
    """
    global log_df
    
    if log_df.empty:
        print("No logs to save.")
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
        
        print(f"Logs successfully saved to Parquet: {s3_path}")
        parquet_buffer.close()
        
    except Exception as e:
        print(f"Error saving logs to S3 Parquet: {str(e)}")
        raise

def save_consolidated_logs(s3_client, table_name=None):
    """
    Saves all accumulated logs to a single consolidated file in S3 in both TXT and Parquet formats.
    """
    global log_df
    
    if log_df.empty:
        print("No logs to save.")
        return
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        table_suffix = f"_{table_name}" if table_name else ""
        
        # Save as TXT format (existing functionality)
        consolidated_log_path_txt = f"{path_log}consolidated_logs{table_suffix}_{timestamp}.txt"
        save_logs_to_s3(s3_client, consolidated_log_path_txt)
        print(f"All logs consolidated and saved to TXT: {consolidated_log_path_txt}")
        
        # Save as Parquet format (new functionality)
        consolidated_log_path_parquet = f"{path_log}consolidated_logs{table_suffix}_{timestamp}.parquet"
        save_logs_to_s3_parquet(s3_client, consolidated_log_path_parquet)
        print(f"All logs consolidated and saved to Parquet: {consolidated_log_path_parquet}")
        
        # Clear logs after saving to prevent accumulation
        clear_logs()
        
    except Exception as e:
        print(f"Error saving consolidated logs: {str(e)}")
        raise

def clear_logs():
    """
    Clears the global log DataFrame to prevent accumulation between executions.
    """
    global log_df
    log_df = pd.DataFrame(columns=log_columns)
    print("Log DataFrame cleared.")

def save_formato_ulaes_file(s3_client, df_spark, table_name=None):
    """
    Saves the processed DataFrame as Formato_ULAES_PSI_Q file in Excel format (.xlsx) in the specified S3 path.
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        table_suffix = f"_{table_name}" if table_name else ""
        formato_file_path = f"{path_formato_ulaes}Formato_ULAES_PSI_Q{table_suffix}_{timestamp}.xlsx"
        
        # Convert Spark DataFrame to Pandas for easier S3 upload
        df_pandas = df_spark.toPandas()
        
        # Save to S3 as Excel (.xlsx)
        excel_buffer = BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            df_pandas.to_excel(writer, index=False, sheet_name='ULAES_Data')
        excel_content = excel_buffer.getvalue()
        
        # Extract bucket and key from S3 path
        if not formato_file_path.startswith('s3://'):
            raise ValueError("S3 path must start with 's3://'")
        
        path_without_prefix = formato_file_path[5:]
        bucket, key = path_without_prefix.split('/', 1)
        
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=excel_content,
            ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
        print(f"Formato_ULAES_PSI_Q Excel file successfully saved to: {formato_file_path}")
        excel_buffer.close()
        
    except Exception as e:
        print(f"Error saving Formato_ULAES_PSI_Q Excel file: {str(e)}")
        log_validation_failure(
            columna="formato_ulaes_file", 
            indice=0, 
            fallo=f"Error al guardar archivo Formato_ULAES_PSI_Q Excel: {str(e)}",
            cumplio=0
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
def excel_s3_to_spark_df(s3_client, bucket, key_xlsx, spark, glueContext, table_name=None):
    """
    Convierte archivo Excel de S3 a Spark DataFrame con logging integrado.
    """
    try:
        temp_xlsx = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        s3_client.download_file(bucket, key_xlsx, temp_xlsx.name)
        log_file_status(archivo_encontrado=True, ruta_archivo=f"s3://{bucket}/{key_xlsx}")

        ruta_csv = convertir_excel_a_csv_fijo(temp_xlsx.name)
        df = pd.read_csv(ruta_csv)

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
        log_file_status(archivo_encontrado=False, ruta_archivo=f"s3://{bucket}/{key_xlsx}")
        raise
    
    except Exception as e:
        log_validation_failure(
            columna="proceso", 
            indice=0, 
            fallo=f"Error al procesar archivo Excel: {str(e)}",
            cumplio=0
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
            key="ruleOutcomes",
            transformation_ctx="rowLevelOutcomes",
        )
        
        return ruleOutcomes.toDF()
    
    except Exception as e:
        log_validation_failure(
            columna="calidad", 
            indice=0, 
            fallo=f"Error en evaluación de calidad: {str(e)}",
            cumplio=0
        )
        raise

# -----------------------------
# Funciones de Validación
# -----------------------------
def verificar_archivo(s3_client, bucket, key):
    """
    Verifica si un archivo existe en un bucket de S3.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        print(f"The file '{key}' exist in the bucket '{bucket}'.")
        
        log_file_status(archivo_encontrado=True, ruta_archivo=f"s3://{bucket}/{key}")
        
        return True
        
    except s3_client.exceptions.NoSuchKey:
        print(f"ERROR: The file '{key}' does not exist in the bucket '{bucket}'.")
        
        log_file_status(archivo_encontrado=False, ruta_archivo=f"s3://{bucket}/{key}")
        
        return False
        
    except Exception as e:
        print(f"Unexpected error accessing the file in S3: {str(e)}")
        
        log_validation_failure(
            columna="archivo_verificacion", 
            indice=0, 
            fallo=f"Error inesperado al verificar archivo: {str(e)}",
            cumplio=0
        )
        
        return False

def verificar_estructura_archivo(df, columnas_esperadas, s3_client=None):
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
                    columna=f"estructura_pos_{idx}",
                    indice=idx,
                    fallo=error_msg,
                    cumplio=0
                )
     
        if errores:
            print("Invalid file structure:")
            for err in errores:
                print(f"   - {err}")
            
            log_validation_failure(
                columna="estructura_archivo",
                indice=0,
                fallo=f"Estructura de archivo inválida. Errores encontrados: {len(errores)}",
                cumplio=0
            )
            
            raise ValueError("Invalid column structure.")
        else:
            print("File structure validated successfully.")
            
            log_validation_failure(
                columna="estructura_archivo",
                indice=0,
                fallo="Estructura de archivo validada correctamente",
                cumplio=1
            )
    
    except Exception as e:
        log_validation_failure(
            columna="estructura_validacion",
            indice=0,
            fallo=f"Error inesperado al validar estructura: {str(e)}",
            cumplio=0
        )
        
        raise

def verificar_atributo(df, rules):
    df_original = df.toDF()  # Convertimos el DynamicFrame original a Spark DF
    df_original = df_original.withColumn("__row_id__", F.monotonically_increasing_id())
 
    # Ejecutar reglas de calidad
    df_with_index = DynamicFrame.fromDF(df_original, glueContext, "df_with_index")
    df_results = glue_quality(df_with_index, rules)
 
    # Filtrar solo fallos
    df_failed = df_results.filter(F.col("outcome") == "Failed")
 
    # Unir por posición usando el mismo "__row_id__"
    df_failed = df_failed.withColumn("__row_id__", F.monotonically_increasing_id())
 
    # Traer excel_row desde el original
    df_final = df_failed.join(df_original.select("__row_id__", "excel_row"), on="__row_id__", how="left")
 
    return df_final.drop("__row_id__")



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
        'COLUMNAS_ESPERADAS'
        ]
    )

DB_SCHEMA = args['DB_SCHEMA']
DB_NAME = args['DB_NAME']
BUCKET_NAME = args['BUCKET_NAME']
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
CONNECT_NAME = args['CONNECT_NAME']
PERIOD = args['PERIOD']
CERTIFICATE = args['CERTIFICATE']

# Parse COLUMNAS_ESPERADAS from job parameters (JSON string to Python list)
import json
columnas_esperadas = json.loads(args['COLUMNAS_ESPERADAS'])

logger.info(f"Period: {PERIOD}")
logger.info(f"Schema: {DB_SCHEMA}")
logger.info(f"Connect: {CONNECT_NAME}")
logger.info(f"Bucket_name: {BUCKET_NAME}")
logger.info(f"Columnas esperadas: {columnas_esperadas}")



# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3_client = boto3.client("s3")

extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME, DB_NAME)

###################################################################
############## VALIDATE FILE IN S3 & READ JSON ####################
###################################################################

# Check if the file exists before attempting to read it
# File Dictionary
PATH_JSON = {
    "file_data": [
        "support_phase_files/niif/PREV_EULAE/ULAES_GENERALES_AAAAMM.xlsx"
    ],
    "file_rules": f"support_phase_files/niif/{DB_NAME.lower()}_{DB_SCHEMA.lower()}_rules_tables.json"
}
logger.info(f"Json_file: s3://{BUCKET_NAME_ASSETS}/{PATH_JSON}")

# Validate and process Excel files
for path_excel in PATH_JSON["file_data"]:
    print(f"Processing file: {path_excel}")
    extension = Path(path_excel).suffix
    if extension != '.xlsx':
        print(f"Error: {path_excel} it is not a file .xlsx")
        continue
 
    if not verificar_archivo(s3_client, BUCKET_NAME_ASSETS, path_excel):
        sys.exit(1)
 
    # Load and validate structure
    frame_save = excel_s3_to_spark_df(s3_client, BUCKET_NAME_ASSETS, path_excel, spark, glueContext)
    verificar_estructura_archivo(frame_save, columnas_esperadas)
 
    # Convert to DynamicFrame
    frame_save = DynamicFrame.fromDF(frame_save, glueContext, "frame_save_dynamic")
    print(f"Excel file uploaded and validated: {path_excel}")
 
 
# Validate and process JSON file
path_rules = PATH_JSON["file_rules"]
print(f"Processing rules file: {path_rules}")
 
if not verificar_archivo(s3_client, BUCKET_NAME_ASSETS, path_rules):
    sys.exit(1)
    
response = s3_client.get_object(Bucket=BUCKET_NAME_ASSETS, Key=path_rules)
catalog_tables = json.loads(response["Body"].read().decode("utf-8"))
print("Rules JSON loaded successfully")
    
###################################################################
################# ITERATE FOR EACH TABLE ##########################
###################################################################

# Dictionary to store EvaluateDataQualityMultiframe for each table
    
for schema in catalog_tables:
    if schema in DB_SCHEMA:
        for table_info in catalog_tables[schema]:
                if table_info['status']:
                    print(f"Tables for ingest: {schema}.{table_info['table']}")
                    try:
                        #ulaes_file_ulaes_generales
                        #frame_to_evaluate = glueContext.create_dynamic_frame.from_catalog(f"{DB_CATALOG_GLUE}", table_name=f"{DB_NAME}_{schema}_{table_info['table']}")
                        frame_to_evaluate = frame_save
                        
                        print(f"Tables for ingest: {schema}.{table_info['rules']}")
                        
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
                                    
                        print("Successfully Evaluate Data Quality ")
                    except Exception as e:
                        print(f"ERROR WITH Evaluate Data Quality: {e}")
    else:
        print("NOT TABLE FOR Evaluate Data Quality")

# Save the processed DataFrame as Formato_ULAES_PSI_Q file
try:
    save_formato_ulaes_file(s3_client, frame_save.toDF(), table_name="ulaes_generales")
    print("Formato_ULAES_PSI_Q file saved successfully.")
except Exception as e:
    print(f"Error saving Formato_ULAES_PSI_Q file: {e}")

# Save all accumulated logs to a single consolidated file
try:
    save_consolidated_logs(s3_client, table_name="ulaes_generales")
    print("All processing logs have been consolidated and saved successfully.")
except Exception as e:
    print(f"Error saving consolidated logs: {e}")

# End of job
job.commit()

    