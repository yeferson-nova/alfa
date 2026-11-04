from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, trim, regexp_replace
from pyspark.sql.types import StringType, DoubleType, LongType, DecimalType, DateType
from pyspark.storagelevel import StorageLevel
import json
import logging
import re
import boto3
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ------------------------
# Utilidades de S3 y tipos
# ------------------------

def parse_s3_path(s3_path: str):
    parsed = urlparse(s3_path)
    return parsed.netloc, parsed.path.lstrip('/')

def load_json_from_s3(bucket: str, key: str) -> dict:
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    return json.loads(content)

def write_json_to_s3(data: dict, s3_path: str):
    bucket, key = parse_s3_path(s3_path)
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(data, indent=2).encode('utf-8'))

def get_spark_type_from_oracle(oracle_type: str):
    oracle_type = oracle_type.strip().upper()
    if oracle_type.startswith("VARCHAR2") or oracle_type.startswith("CHAR"):
        return StringType()
    if oracle_type.startswith("NUMBER"):
        match = re.match(r"NUMBER\((\d+)(?:,\s*(\d+))?\)", oracle_type)
        if match:
            precision = int(match.group(1))
            scale = int(match.group(2)) if match.group(2) else 0
            return LongType() if scale == 0 and precision <= 19 else DecimalType(precision, scale)
        return DoubleType()
    if oracle_type.startswith("FLOAT(53)"):
        return DecimalType(38,10)
    if oracle_type == "DATE":
        return DateType()
    return StringType()


# ------------------------
# Proceso individual por tabla
# ------------------------

def process_single_table(table_name, config, spark, s3_base_path, silver_base_path, resumen):
    """
    Función optimizada para procesar una tabla.
    - Consolida todas las transformaciones de columna en un único .select() para un plan de ejecución eficiente.
    - Maneja robustamente la lectura y validación de esquemas.
    - Aplica repartición para balancear la carga de escritura.
    """
    schema_def_raw = config.get("schema", {})
    if not schema_def_raw or not isinstance(schema_def_raw, dict):
        logging.warning(f"[{table_name}] Esquema inválido o no encontrado en la configuración.")
        return

    try:
        # --- 1. LECTURA Y VALIDACIÓN INICIAL ---
        # Pre-procesamos los nombres de columnas a minúsculas para consistencia
        schema_def = {k.lower(): v for k, v in schema_def_raw.items()}
        columnas_requeridas = list(schema_def.keys())

        s3_path = f"{s3_base_path}{table_name}"
        df = spark.read.parquet(s3_path)
        
        # Validar columnas faltantes antes de continuar
        columnas_existentes = {c.lower() for c in df.columns}
        faltantes = [c for c in columnas_requeridas if c not in columnas_existentes]
        
        # Llenar el resumen inicial
        resumen[table_name] = {
            "columnas_homologadas": list(config.get("homologaciones", {}).keys()),
            "columnas_renombradas": config.get("rename", {}),
            "columnas_faltantes": faltantes,
            "filas_entrada": 0, # Se actualizará después si es necesario
            "filas_salida": 0
        }

        if faltantes:
            logging.warning(f"[{table_name}] Columnas requeridas faltantes en el origen: {faltantes}. Saltando procesamiento.")
            return

        # Seleccionamos solo las columnas requeridas para optimizar desde el inicio
        df = df.select(columnas_requeridas)

        # --- 2. CONSTRUCCIÓN DE EXPRESIONES DE TRANSFORMACIÓN (EL NÚCLEO DE LA OPTIMIZACIÓN) ---
        select_expressions = []
        
        # Pre-procesamos las configuraciones para un acceso eficiente
        homologaciones = {k.lower(): v for k, v in config.get("homologaciones", {}).items()}
        fields_to_clean = {f.lower() for f in config.get("fields_to_clean", [])}
        rename_map = {k.lower(): v.lower() for k, v in config.get("rename", {}).items()}

        for column_name in df.columns:
            # Empezamos con la columna base
            current_expr = col(column_name)

            # A. Cast de tipos según el esquema
            col_type = schema_def.get(column_name)
            if col_type:
                if "DATE" in col_type.upper():
                    if table_name.upper() == "RPND13Y" and column_name in ["fvigdesde", "fvighasta", "fvigenddk", "fvigendhk", "entrydte"]:
                        # Lógica especial y robusta para fechas en formato DDMMYYYY
                        current_expr = to_date(lpad(current_expr.cast("string"), 8, "0"), "ddMMyyyy")
                    else:
                        current_expr = to_date(current_expr)
                else:
                    spark_type = get_spark_type_from_oracle(col_type) # Asume que esta función de utilidad existe
                    current_expr = current_expr.cast(spark_type)

            # B. Homologación
            if column_name in homologaciones:
                mapping = homologaciones[column_name]
                homolog_expr = None
                for old_val, new_val in mapping.items():
                    homolog_expr = when(current_expr == old_val, new_val) if homolog_expr is None else homolog_expr.when(current_expr == old_val, new_val)
                if homolog_expr:
                    current_expr = homolog_expr.otherwise(current_expr)

            # C. Limpieza de campos
            if column_name in fields_to_clean:
                current_expr = trim(regexp_replace(current_expr, " {2,}", " "))
            
            # D. Renombramiento (se aplica como alias a la expresión final)
            final_col_name = rename_map.get(column_name, column_name)
            
            select_expressions.append(current_expr.alias(final_col_name))

        # --- 3. APLICACIÓN DE TODAS LAS TRANSFORMACIONES EN UN SOLO PASO ---
        df_transformado = df.select(select_expressions)

        # --- 4. REPARTICIÓN Y ESCRITURA ---
        # Fijamos un número de particiones robusto para balancear la carga de escritura.
        df_final = df_transformado.repartition(500)

        # Opcional: Si necesitas el conteo, este es el momento de hacerlo.
        # count = df_final.count()
        # resumen[table_name]["filas_entrada"] = count
        # resumen[table_name]["filas_salida"] = count
        
        silver_path = f"{silver_base_path}{table_name}_test/"
        df_final.write.mode("overwrite").option("maxRecordsPerFile", 10_000_000).parquet(silver_path)
        logging.info(f"[{table_name}] Escritura completada en Silver: {silver_path}")

    except Exception as e:
        # Captura de errores más detallada para depuración
        logging.error(f"[{table_name}] Error en procesamiento. Causa: {str(e)}", exc_info=True)
        # Relanzar la excepción para que el job de Glue falle explícitamente si es necesario
        raise e


# ------------------------
# Proceso secuencial de todas las tablas
# ------------------------

def process_all_tables_sequential(spark: SparkSession,
                                  s3_schema_path: str,
                                  s3_base_path: str,
                                  silver_base_path: str,
                                  s3_reporte_path: str):
    bucket, key = parse_s3_path(s3_schema_path)
    json_schema = load_json_from_s3(bucket, key)
    if not json_schema:
        logging.error("[Global] Esquema JSON no válido.")
        return

    resumen = {}
    for table_name, config in json_schema.items():
        process_single_table(table_name, config, spark, s3_base_path, silver_base_path, resumen)

    write_json_to_s3(resumen, s3_reporte_path)
    logging.info("[Global] Procesamiento completado.")


# ------------------------
# Main
# ------------------------

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("IngestaSilverSecuencial") \
        .config("spark.executor.memory", "24g") \
        .config("spark.executor.memoryOverhead", "4g") \
        .config("spark.executor.cores", "5") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "500") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY") \
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY") \
        .getOrCreate()

    s3_schema_path = "s3://alfa-datalake-dev-assets/support_phase_files/niif/Silver_pcr-tradicional.json"
    s3_base_path = "s3://alfa-datalake-dev-bronce/ACSELE/ACSELE_ALFA_FULL/"
    silver_base_path = "s3://alfa-datalake-dev-silver/ACSELE/ACSELE_ALFA_FULL/"
    s3_reporte_path = "s3://alfa-datalake-dev-bronce/ACSELE/resumen_ingesta.json"

    process_all_tables_sequential(spark, s3_schema_path, s3_base_path, silver_base_path, s3_reporte_path)
