# -----------------------------
# Imports y Configuración
# -----------------------------
"""Script that validator and load bronce phase"""
import inspect, json, os, sys, tempfile, traceback, zipfile
import xml.etree.ElementTree as ET
from datetime import datetime
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo
import boto3
import ast
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import SelectFromCollection
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.functions import col, explode, when, regexp_replace, lower, trim
from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType)

from commons3.ExtractHelper import ExtractHelper
from commons3.LogsHelper import LogsHelper
from commons3.ParameterBuilder import ParameterBuilder

###################################################################
######## SET ARGS AND CONFIG COMMONS UTILS ########################
###################################################################
logs = LogsHelper()
logger = logs.logger_conf()

args = getResolvedOptions(
    sys.argv, 
        [
        'JOB_NAME',
        'TABLES_PARAMS',
        'CASE_USE_EVALUATE']
)

CASE_USE_EVALUATE = json.loads(args['CASE_USE_EVALUATE'])
tables_params = json.loads(args['TABLES_PARAMS'])

#Read params from aux lib
builder = ParameterBuilder(table_names=tables_params )
params = builder.process_and_build_all(max_workers=10)
json_params = json.loads(params)

# Call function to process and group data
unified_buckets = builder.group_and_unify_structures(json_params, 'directory')
unified_structures = builder.group_and_unify_structures(json_params, 'structure')
unified_files = builder.group_and_unify_structures(json_params, 'file')
unified_locations = builder.group_and_unify_structures(json_params, 'location')
unified_attributes = builder.group_and_transform_attributes(json_params, 'attributes')
unified_rules = builder.create_rules(json_params, ['func_domain','func_type','func_symbol', 'func_spaces', 'func_null'])


BUCKET_NAME = unified_buckets['bronce']
BUCKET_SOURCE = unified_buckets['landing']
BUCKET_SILVER = unified_buckets['silver']
location_logs = unified_locations['LOGS']
columns_logs = ast.literal_eval(unified_structures['LOGS'])
files = unified_files
path_log = f"s3://{BUCKET_SILVER}/LOGS/{location_logs[0]}/"

expected_columns = {key: unified_structures[key] for key in CASE_USE_EVALUATE}
expected_columns_brz = {f"{key}_BRZ": unified_structures[f"{key}_BRZ"] for key in CASE_USE_EVALUATE}

def build_schemas_with_order(fields_map,ordered_columns_map):
    type_mapping = {
        "VARCHAR": StringType(),
        "CHAR": StringType(),
        "NUMBER": IntegerType(),
        "NUMERIC": DoubleType(),
        "DATE": DateType()
    }

    schema_output = {}

    for table_name, fields in fields_map.items():
        # Convert string list to actual list
        ordered_cols = ast.literal_eval(ordered_columns_map.get(table_name, "[]"))

        # Map types by name
        type_dict = {f["NAME"]: type_mapping.get(f["TYPE"].upper(), StringType()) for f in fields}

        # Build ordered schema (only if the columns exist in the dict)
        ordered_schema = {col: type_dict[col] for col in ordered_cols if col in type_dict}

        schema_output[table_name] = ordered_schema

    return schema_output


schema_output = build_schemas_with_order(unified_attributes, unified_structures)


#point of validation params
logger.info(f"Bucket_name_bronce: {BUCKET_NAME}")
logger.info(f"Bucket_name_landing: {BUCKET_SOURCE}")
logger.info(f"Bucket_name_silver: {BUCKET_SILVER}")
logger.info(f"Expected columns: {expected_columns}")
logger.info(f"schema columns logs: {columns_logs}")
logger.info(f"Unified locations: {unified_locations}")
logger.info(f"SCHEMA: {schema_output}")
logger.info(f"FILES: {files}")
logger.info(f"rules: {unified_rules}")


# Initialize a global dataframe to store logs with enhanced structure
log_df = pd.DataFrame(columns=columns_logs)

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

def log_row_validation_failure(df_dataquality, case_use):
    """
    Process DataFrame of DataQuality  and write errors usando log_validation_failure.
    """
    
    df_errores_pandas = df_dataquality.toPandas()
    
    for _, row in df_errores_pandas.iterrows():
        log_validation_failure(
            caso_de_uso =f"{case_use}",
            columna =row.get("rule_type", "columna_desconocida"),
            indice =int(row.get("NO_REGISTRO", -1)),
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
    log_df = pd.DataFrame(columns=columns_logs)
    logger.info("Log DataFrame cleared.")

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
# Utilidades
# -----------------------------

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

def save_format_file(s3_client, df_spark, path_files):
    """
    Saves the processed DataFrame as Format file in Parquet format (.parquet) in the specified S3 path.
    """
    try:
        
        timestamp = datetime.now(ZoneInfo("Etc/GMT+5")).strftime("%Y-%m-%d %H:%M:%S")

        path_save_files = f"s3://{path_files}/fecha_carga={timestamp}/"
        
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

# -----------------------------
# Funciones de Procesamiento
# -----------------------------
def excel_s3_to_spark_df(s3_client, bucket, key_xlsx, spark, glueContext, case_use):
    """
    Convierte archivo Excel de S3 a Spark DataFrame con logging integrado.
    Controla el caso donde el archivo está vacío.
    """
    
    try:
        excel_file_path = f"s3://{bucket}/{key_xlsx}"
        
        df = pd.read_excel(excel_file_path, dtype=str)
        
        # vacio
        if df.empty:
            log_validation_failure(
                caso_de_uso=f"{case_use}",
                columna="proceso",
                indice=0,
                fallo=f"El archivo Excel está vacío: {key_xlsx}",
                estado=0
            )
            logger.info(f"Archivo Excel vacío: {key_xlsx}. Se devolverá un DataFrame vacío.")
            
            
            return spark.createDataFrame([], schema="NO_REGISTRO string")

        df.insert(0, "NO_REGISTRO", range(2, 2 + len(df)))
        
        for col in df.columns:
            df[col] = df[col].fillna("").astype(str)
        
        df_spark = spark.createDataFrame(df)

        return df_spark

    except FileNotFoundError:
        logger.info("File Not Found in S3")
        log_validation_failure(
            caso_de_uso=f"{case_use}",
            columna="proceso",
            indice=0,
            fallo=f"Archivo no encontrado en S3: {key_xlsx}",
            estado=0
        )
        return spark.createDataFrame([], schema="NO_REGISTRO string")

    except ValueError as ve:
        if "empty dataset" in str(ve).lower():
            log_validation_failure(
                caso_de_uso=f"{case_use}",
                columna="proceso",
                indice=0,
                fallo=f"Error: archivo Excel vacío o sin datos ({key_xlsx})",
                estado=0
            )
            logger.info(f" Empty or data-free Excel file: {key_xlsx}")
            return spark.createDataFrame([], schema="NO_REGISTRO string")
        else:
            raise

    except Exception as e:
        log_validation_failure(
            caso_de_uso=f"{case_use}",
            columna="proceso", 
            indice=0, 
            fallo=f"Error al procesar archivo Excel: {str(e)}",
            estado=0
        )
        return spark.createDataFrame([], schema="NO_REGISTRO string")

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

def verificar_estructura_archivo(df, expected_columns,  case_use, s3_client=None):
    """
    Verifica si el DataFrame tiene las columnas esperadas en el orden correcto.
    """
    try:
        
        columnas_actuales = [col if col not in [None, ''] else 'VACIA' for col in df.columns]
        
        missing_columns = [col for col in expected_columns if col not in columnas_actuales]
        extra_columns = [col for col in columnas_actuales if col not in expected_columns]
        
        errores = []
        
        if missing_columns:
            error_msg = f"Faltan columnas requeridas: {', '.join(missing_columns)}"
            errores.append(error_msg)
            log_validation_failure(
                caso_de_uso = f"{case_use}",
                columna="columnas_faltantes", 
                indice=0, 
                fallo=error_msg,
                estado=0
            )
    
        if extra_columns:
            error_msg = f"Columnas adicionales no esperadas: {', '.join(extra_columns)}"
            errores.append(error_msg)
            log_validation_failure(
                caso_de_uso = f"{case_use}",
                columna="columnas_extra", 
                indice=0, 
                fallo=error_msg,
                estado=0
            )
     
        for idx, col in enumerate(expected_columns):
            if idx >= len(columnas_actuales) or columnas_actuales[idx] != col:
                error_msg = (
                    f"Columna esperada en posición {idx}: '{col}', "
                    f"encontrada: '{columnas_actuales[idx] if idx < len(columnas_actuales) else 'N/A'}'"
                )
                errores.append(error_msg)
                
                log_validation_failure(
                    caso_de_uso = f"{case_use}",
                    columna=f"estructura_pos_{idx}",
                    indice=0,
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
            
            logger.error("Invalid column structure.")
            
            return False
        else:
            
            return True
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

def modify_df_schema_for_rules(df: DataFrame, columns_brz: list):
    """
    modify schema a dataframe allow schema 
    """
    try:
        df_schema_for_rules = df 
        
        for indice, name_col in enumerate(df_schema_for_rules.columns):
            if name_col != columns_brz[:-1][indice]:
                df_schema_for_rules = df_schema_for_rules.withColumnRenamed(name_col, columns_brz[:-1][indice])
        
        for name_col in df_schema_for_rules.columns:
            df_schema_for_rules = df_schema_for_rules.withColumn(
                name_col,
                when(col(name_col) == '', None).otherwise(col(name_col)).cast(StringType())
            )
            
    except Exception as e:
        return df
        logger.info(f"Error create schema file {e}")

    return df_schema_for_rules

def modify_df_schema(df: DataFrame, changes_schema: dict):
    """
    modify schema a dataframe allow schema 
    """
    df_schema = df.toDF()

    df_schema = df_schema.withColumn("ID_LOTE_CARGUE", F.lit(0))
    
    for name_col, new_type in changes_schema.items():
        df_schema = df_schema.withColumn(name_col, col(name_col).cast(new_type))
        df_schema = df_schema.withColumn(name_col, when(col(name_col) == '', None).otherwise(col(name_col)))
    
    return df_schema
    

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3_client = boto3.client("s3")

###################################################################
################# ITERATE FOR EACH FILE  ##########################
###################################################################

for case_use, file in files.items():
    if case_use in CASE_USE_EVALUATE:
        file_item = file['file']
        file_extension = file['ext']
        unified_locations_case = unified_locations[case_use]
        expected_columns_case = json.loads(expected_columns[case_use])
        expected_columns_case.insert(0, "NO_REGISTRO")
        expected_columns_case_brz = json.loads(expected_columns_brz[f"{case_use}_BRZ"])
        schema_output_case = schema_output[f"{case_use}_BRZ"]
        
        # Check if the file exists before attempting to read it
        # File Dictionary - Using parameters from job configuration
        excel_file_path = f"{BUCKET_SOURCE}/{case_use}/{unified_locations_case[0]}/{file_item}{file_extension}"
        bronce_file_path = f"{BUCKET_NAME}/{case_use}/{file_item}"
    
        key_file = f"{case_use}/{unified_locations_case[0]}/{file_item}{file_extension}"
        key_rules = f"support_phase_files/niif/{file_item.lower()}_rules_tables.json"
        
        logger.info(f"Starting for case : {case_use}")
        logger.info(f"columns: {expected_columns_case}")
        
        if verificar_archivo(s3_client, BUCKET_SOURCE, key_file, case_use):
    
            logger.info(f"Loading file and Checking structure from {excel_file_path}")
    
            frame_save_from_file = excel_s3_to_spark_df(s3_client, BUCKET_SOURCE, key_file, spark, glueContext, case_use)
            
            frame_save_from_file.show(truncate = False)
            
            if verificar_estructura_archivo(frame_save_from_file, expected_columns_case, case_use):
                
                frame_to_evaluate = modify_df_schema_for_rules(frame_save_from_file, expected_columns_case_brz)
                
                frame_to_evaluate.show(truncate = False)
    
                # Convert to DynamicFrame
                frame_to_evaluate = DynamicFrame.fromDF(frame_to_evaluate, glueContext, "frame_save_dynamic")
                logger.info(f"Excel file uploaded and validated: {excel_file_path}")        
                
                if unified_rules[case_use]:
                    try:
                        frame_evaluate = frame_to_evaluate
                        all_rules = unified_rules[case_use]
                        tipos_deseados = ["ColumnExists","ColumnLength","ColumnValues","IsComplete","CustomSql","ColumnDataType"]
                        df_failes_outcomes_total = None         
                        
                        for tipo in tipos_deseados:
                            filtered_rules = filtrar_reglas_por_tipo(all_rules, [tipo])
                            if filtered_rules:
                                rules_str = ",\n".join(filtered_rules)
                                print(rules_str)
                                df_fails = verificar_atributo(frame_evaluate, rules_str)
                                df_fails = df_fails.withColumn("rule_type", F.lit(tipo))
                                if df_failes_outcomes_total is None:
                                    df_failes_outcomes_total = df_fails
                                else:
                                    df_failes_outcomes_total = df_failes_outcomes_total.unionByName(df_fails, allowMissingColumns=True)
                                    
                        logger.info("Successfully Evaluate Data Quality ")
    
    
                        if df_failes_outcomes_total.count() == 0:
                            # Save the processed DataFrame as file
                            try:
                                frame_save = modify_df_schema(frame_evaluate, schema_output_case)
                                frame_save.show()
                                save_format_file(s3_client,frame_save,bronce_file_path)
                                logger.info("File saved successfully.")
                                move_file(s3_client,BUCKET_NAME,BUCKET_SOURCE,key_file,case_use,file_item,file_extension,status = unified_locations_case[1])
    
                            except Exception as e:
                                move_file(s3_client,BUCKET_NAME,BUCKET_SOURCE,key_file,case_use,file_item,file_extension,status= unified_locations_case[2])
                                logger.info(f"Error saving File: {e}")
                        else:
                            log_row_validation_failure(df_failes_outcomes_total, case_use)
                            move_file(s3_client,BUCKET_NAME,BUCKET_SOURCE,key_file,case_use,file_item,file_extension,status= unified_locations_case[2])
                            logger.info(f"Data Quality is Wrong")
    
                    except Exception as e:
    
                        logger.info(f"ERROR WITH Evaluate Data Quality: {e}")
    
                else:
                    move_file(s3_client,BUCKET_NAME,BUCKET_SOURCE,key_file,case_use,file_item,file_extension,status= unified_locations_case[2])
                    logger.info(f"CRITICAL ERROR: Rules in {case_use} not exist Please verify the params and permissions.")
    
            else:
                move_file(s3_client,BUCKET_NAME,BUCKET_SOURCE,key_file,case_use,file_item,file_extension,status= unified_locations_case[2])
                logger.info(f"CRITICAL ERROR: File 's3://{BUCKET_SOURCE}/{case_use}/{unified_locations_case[0]}/{file_item}{file_extension}' not found schema correct . Please verify the schema")
        else:
            move_file(s3_client,BUCKET_NAME,BUCKET_SOURCE,key_file,case_use,file_item,file_extension,status= unified_locations_case[2])
            logger.info(f"CRITICAL ERROR: File 's3://{BUCKET_SOURCE}/{case_use}/{unified_locations_case[0]}/{file_item}{file_extension}' not found in bucket '{BUCKET_SOURCE}'. Please verify the path and permissions.")                

# Save all accumulated logs to a single consolidated file
try:
    save_consolidated_logs(s3_client, table_name="logs")
    logger.info("All processing logs have been consolidated and saved successfully.")
except Exception as e:
    logger.info(f"Error saving consolidated logs: {e}")

# End of job
job.commit()



    