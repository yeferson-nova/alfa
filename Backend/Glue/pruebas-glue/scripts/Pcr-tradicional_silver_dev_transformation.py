from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, trim, regexp_replace, date_format
from pyspark.sql.types import StringType, DoubleType, LongType, DecimalType, DateType
from pyspark.storagelevel import StorageLevel
import json, sys, re, boto3
from urllib.parse import urlparse
from commons.LogsHelper import LogsHelper
from awsglue.utils import getResolvedOptions



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
    if oracle_type == "DATE":
        return DateType()
    return StringType()


# ------------------------
# Proceso individual por tabla
# ------------------------
def process_single_table(table_name, config, spark, s3_base_path, silver_base_path, resumen):

    schema_def = config.get("schema", {})
    partition_col = config.get("partition_col", {})
    order = config.get("order", {})
    
    if not schema_def or not isinstance(schema_def, dict):
        logger.warning(f"[{table_name}] Esquema inválido.")
        return

    try:
        columnas_requeridas = [c.lower() for c in schema_def.keys()]
        s3_path = f"{s3_base_path}{table_name}"
        df = spark.read.parquet(s3_path).select([col(c) for c in columnas_requeridas])

        resumen[table_name] = {
            "columnas_homologadas": list(config.get("homologaciones", {}).keys()),
            "columnas_renombradas": config.get("rename", {}),
            "columnas_faltantes": [],
            "filas_entrada": 0,
            "filas_salida": 0,
        }

        columnas_df = df.columns
        faltantes = [c for c in columnas_requeridas if c not in columnas_df]
        if faltantes:
            resumen[table_name]["columnas_faltantes"] = faltantes
            logger.warning(f"[{table_name}] Columnas faltantes: {faltantes}")
            return

        # Cast de tipos según esquema

        for col_name, col_type in schema_def.items():
            col_lower = col_name.lower()
            if "DATE" in col_type.upper():
                if table_name == "RPND13Y" and col_lower in [
                    "fvigdesde",
                    "fvighasta",
                    "fvigenddk",
                    "fvigendhk",
                    "entrydte",
                ]:
                    # Custom date parsing for RPND13Y date fields with format DDMMYYYY possibly missing leading zeros
                    df = df.withColumn(
                        col_lower,
                        to_date(
                            when(
                                col(col_lower).isNotNull(),
                                # Pad left with zeros to length 8 and parse with format ddMMyyyy
                                col(col_lower).cast("string").rlike("^\\d{7,8}$"),
                            ).otherwise(None)
                        ),
                    )

                    # More explicit transformation with padding and to_date with format

                    from pyspark.sql.functions import lpad, expr

                    df = df.withColumn(
                        col_lower,
                        to_date(
                            lpad(col(col_lower).cast("string"), 8, "0"), "ddMMyyyy"
                        ),
                    )

                else:
                    df = df.withColumn(col_lower, to_date(col(col_lower)))
            else:
                spark_type = get_spark_type_from_oracle(col_type)
                df = df.withColumn(col_lower, col(col_lower).cast(spark_type))

        # Homologación

        homologaciones = config.get("homologaciones", {})
        for col_name, mapping in homologaciones.items():
            col_lower = col_name.lower()
            if col_lower in df.columns:
                expr = None
                for old_val, new_val in mapping.items():
                    expr = (
                        when(col(col_lower) == old_val, new_val)
                        if expr is None
                        else expr.when(col(col_lower) == old_val, new_val)
                    )
                expr = expr.otherwise(col(col_lower))
                df = df.withColumn(col_lower, expr)

        # Limpieza de campos especificados en fields_to_clean

        fields_to_clean = config.get("fields_to_clean", [])
        for field in fields_to_clean:
            field_lower = field.lower()
            if field_lower in df.columns:
                # Trim spaces and replace double spaces with single space
                df = df.withColumn(
                    field_lower, trim(regexp_replace(col(field_lower), " {2,}", " "))
                )

        # Renombramiento (al final)

        rename_map = config.get("rename", {})
        for original, nuevo in rename_map.items():
            if original.lower() in df.columns:
                df = df.withColumnRenamed(original.lower(), nuevo.lower())
        #df = df.repartition(500)
        df = df.repartition(min(spark.sparkContext.defaultParallelism * 4, 200))
        # df.persist(StorageLevel.MEMORY_AND_DISK)
        resumen[table_name]["filas_entrada"] = 0
        silver_path = f"{silver_base_path}{table_name}/"

        if order != "-":
            df = df.repartition(300, order)
            df = df.sortWithinPartitions(order)
            
        if partition_col != "-":
            df = df.withColumn(
                f"{partition_col}_partition",
                date_format(partition_col, "yyyy-MM")
            )
            df.write.mode("overwrite").option("maxRecordsPerFile", 10_000_000).partitionBy(f"{partition_col}_partition").parquet(
                silver_path)
        else:
            df.write.mode("overwrite").option("maxRecordsPerFile", 10_000_000).parquet(
                silver_path)
            

        logger.info(f"[{table_name}] Escrita en Silver: {silver_path}")

        resumen[table_name]["filas_salida"] = 0

        df.unpersist()

    except Exception as e:

        logger.error(f"[{table_name}] Error en procesamiento: {str(e)}")


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
        logger.error("[Global] Esquema JSON no válido.")
        return

    resumen = {}
    for table_name, config in json_schema.items():
        if config["status"] :
            process_single_table(table_name, config, spark, s3_base_path, silver_base_path, resumen)

    write_json_to_s3(resumen, s3_reporte_path)
    logger.info("[Global] Procesamiento completado.")


# ------------------------
# Main
# ------------------------

if __name__ == "__main__":
    
    ###################################################################
    ######## SET ARGS AND CONFIG COMMONS UTILS ########################
    ###################################################################
    
    spark = SparkSession.builder \
        .appName("IngestaSilverSecuencial") \
        .config("spark.executor.memory", "16g") \
        .config("spark.executor.cores", "5") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "80") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY") \
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY") \
        .getOrCreate()


    
    logs = LogsHelper()
    logger = logs.logger_conf()
    

    args = getResolvedOptions(
        sys.argv, 
        [
            'JOB_NAME',
            'BUCKET_NAME',
            'BUCKET_NAME_ASSETS',
            'CERTIFICATE',
            'DB_CATALOG_GLUE',
            'DB_NAME',
            'DB_SCHEMA',
            'PERIOD',
            'BUCKET_NAME_BRONCE'
        ]
    )
    
    BUCKET_NAME = args['BUCKET_NAME']
    BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
    CERTIFICATE = args['CERTIFICATE']
    DB_CATALOG_GLUE = args['DB_CATALOG_GLUE']
    DB_NAME = args['DB_NAME']
    DB_SCHEMA = args['DB_SCHEMA']
    PERIOD = args['PERIOD']
    BUCKET_NAME_BRONCE = args['BUCKET_NAME_BRONCE']
    
    # Rutas construidas dinámicamente a partir de los parámetros
    s3_schema_path = f"s3://{BUCKET_NAME_ASSETS}/support_phase_files/niif/Silver_pcr-tradicional.json"
    s3_base_path = f"s3://{BUCKET_NAME_BRONCE}/{DB_NAME}/{DB_SCHEMA}/"
    silver_base_path = f"s3://{BUCKET_NAME}/{DB_NAME}/{DB_SCHEMA}/"
    s3_reporte_path = f"s3://{BUCKET_NAME}/{DB_NAME}/resumen_ingesta.json"


    process_all_tables_sequential(spark, s3_schema_path, s3_base_path, silver_base_path, s3_reporte_path)
