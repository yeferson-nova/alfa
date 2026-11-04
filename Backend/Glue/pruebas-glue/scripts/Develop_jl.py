from pyspark.context import SparkContext
from awsglue.context import GlueContext

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


from cleanup import CleanUp
from logger_config import logger
from pyspark.sql import functions as F
import boto3
from datetime import datetime
from pyspark.sql.functions import col, lit, current_timestamp, lower , when, row_number, max as spark_max, split, size, concat_ws,trim
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

import re
import unicodedata
import datetime
import math

from typing import List, Optional, Any
from pyspark.sql import functions as F, Window
from pyspark.sql.functions import (
    split, to_date, date_format, col, regexp_replace, trim, when, isnan, isnull,
    upper, lower, regexp_extract, length, abs as spark_abs, ceil, coalesce,
    translate, ltrim, rtrim, current_date, datediff, year, month, dayofmonth, regexp_like
)

from pyspark.sql import functions as F
import boto3
from datetime import datetime
from pyspark.sql.functions import col, lit, current_timestamp, lower , when, row_number, max as spark_max, split, size, concat_ws,trim,upper
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
df_hashes_previos = spark.read.parquet("s3://alfa-datalake-dev-silver/CLIENTES/DATOS_BASICOS/CLIENTES_DATOS_BASICOS_STG/2025/09/03/")
def split_name(df, campo_origen="primer_nombre"):
    """
    Función definitiva para dividir nombres - Cubre 5 grupos de casos:

    Grupo 1: Todo en primer_nombre (40-50%) - Procesa campos + genera nombre_completo
    Grupo 2: Solo nombre_completo (20-30%) - Procesa campos + preserva nombre_completo  
    Grupo 3: Datos estructurados (15-20%) - Solo optimiza nombre_completo
    Grupo 4: Datos parciales (5-10%) - Solo optimiza nombre_completo
    Grupo 5: Fuente diferente (Variable) - No hace nada

    SOLO procesa registros con cod_fuente_fk = 1
    """
    es_fuente_uno = (col("cod_fuente_fk") ==1)
    # Limpieza del campo origen
    df = df.withColumn(campo_origen + "_limpio",
                    upper(trim(regexp_replace(
                        regexp_replace(col(campo_origen), r'\s+', ' '),
                        r'[^\w\sáéíóúÁÉÍÓÚñÑüÜ.-]', ''
                    ))))

    # GRUPO 1: Condición para procesar campos individuales desde primer_nombre
    debe_procesar_primer_nombre = (
        (col("cod_fuente_fk") == 1) &
        (col(campo_origen).isNotNull()) &
        (length(trim(col(campo_origen))) > 0) &
        (size(split(col(campo_origen + "_limpio"), " ")) >= 2) &
        (col("segundo_nombre").isNull()) &
        (col("primer_apellido").isNull()) &
        (col("segundo_apellido").isNull())
    )

    # GRUPO 2: Condición para procesar campos individuales desde nombre_completo
    debe_procesar_nombre_completo = (
        (col("cod_fuente_fk") == 1) &
        (col(campo_origen).isNull() | (length(trim(col(campo_origen))) == 0)) &
        (col("nombre_completo").isNotNull()) &
        (length(trim(col("nombre_completo"))) > 0) &
        (size(split(upper(trim(col("nombre_completo"))), " ")) >= 2) &
        (col("segundo_nombre").isNull()) &
        (col("primer_apellido").isNull()) &
        (col("segundo_apellido").isNull())
    )

    # Determinar fuente y partes a usar
    df = df.withColumn("fuente_texto",
                    when(debe_procesar_nombre_completo, upper(trim(col("nombre_completo"))))
                    .otherwise(col(campo_origen + "_limpio")))

    df = df.withColumn("partes_array", split(col("fuente_texto"), " "))

    # Condición combinada para procesar
    debe_procesar = debe_procesar_primer_nombre | debe_procesar_nombre_completo

    # PROCESAMIENTO POR CANTIDAD DE PALABRAS

    # === 2 PALABRAS: NOMBRE + APELLIDO ===
    df = df.withColumn("primer_nombre",
                    when(debe_procesar & (size(col("partes_array")) == 2),
                        col("partes_array")[0]
                    ).otherwise(col("primer_nombre")))

    df = df.withColumn("segundo_nombre",
                    when(debe_procesar & (size(col("partes_array")) == 2),
                        lit(None).cast("string")
                    ).otherwise(col("segundo_nombre")))

    df = df.withColumn("primer_apellido",
                    when(debe_procesar & (size(col("partes_array")) == 2),
                        col("partes_array")[1]
                    ).otherwise(col("primer_apellido")))

    df = df.withColumn("segundo_apellido",
                    when(debe_procesar & (size(col("partes_array")) == 2),
                        lit(None).cast("string")
                    ).otherwise(col("segundo_apellido")))

    # === 3 PALABRAS: NOMBRE + NOMBRE + APELLIDO ===
    df = df.withColumn("primer_nombre",
                    when(debe_procesar & (size(col("partes_array")) == 3),
                        col("partes_array")[0]
                    ).otherwise(col("primer_nombre")))

    df = df.withColumn("segundo_nombre",
                    when(debe_procesar & (size(col("partes_array")) == 3),
                        col("partes_array")[1]
                    ).otherwise(col("segundo_nombre")))

    df = df.withColumn("primer_apellido",
                    when(debe_procesar & (size(col("partes_array")) == 3),
                        col("partes_array")[2]
                    ).otherwise(col("primer_apellido")))

    df = df.withColumn("segundo_apellido",
                    when(debe_procesar & (size(col("partes_array")) == 3),
                        lit(None).cast("string")
                    ).otherwise(col("segundo_apellido")))

    # === 4 PALABRAS: NOMBRE + NOMBRE + APELLIDO + APELLIDO ===
    df = df.withColumn("primer_nombre",
                    when(debe_procesar & (size(col("partes_array")) == 4),
                        col("partes_array")[0]
                    ).otherwise(col("primer_nombre")))

    df = df.withColumn("segundo_nombre",
                    when(debe_procesar & (size(col("partes_array")) == 4),
                        col("partes_array")[1]
                    ).otherwise(col("segundo_nombre")))

    df = df.withColumn("primer_apellido",
                    when(debe_procesar & (size(col("partes_array")) == 4),
                        col("partes_array")[2]
                    ).otherwise(col("primer_apellido")))

    df = df.withColumn("segundo_apellido",
                    when(debe_procesar & (size(col("partes_array")) == 4),
                        col("partes_array")[3]
                    ).otherwise(col("segundo_apellido")))

    # === 5+ PALABRAS: Primeras 4 palabras ===
    df = df.withColumn("primer_nombre",
                    when(debe_procesar & (size(col("partes_array")) >= 5),
                        col("partes_array")[0]
                    ).otherwise(col("primer_nombre")))

    df = df.withColumn("segundo_nombre",
                    when(debe_procesar & (size(col("partes_array")) >= 5),
                        col("partes_array")[1]
                    ).otherwise(col("segundo_nombre")))

    df = df.withColumn("primer_apellido",
                    when(debe_procesar & (size(col("partes_array")) >= 5),
                        col("partes_array")[2]
                    ).otherwise(col("primer_apellido")))

    df = df.withColumn("segundo_apellido",
                    when(debe_procesar & (size(col("partes_array")) >= 5),
                        col("partes_array")[3]
                    ).otherwise(col("segundo_apellido")))

    # GENERACIÓN DE nombre_completo (PARA TODOS LOS GRUPOS)
    df = df.withColumn("nombre_completo_generado",
                    trim(concat_ws(" ",
                        when(col("primer_nombre").isNotNull() & (length(trim(col("primer_nombre"))) > 0),
                            trim(col("primer_nombre"))),
                        when(col("segundo_nombre").isNotNull() & (length(trim(col("segundo_nombre"))) > 0),
                            trim(col("segundo_nombre"))),
                        when(col("primer_apellido").isNotNull() & (length(trim(col("primer_apellido"))) > 0),
                            trim(col("primer_apellido"))),
                        when(col("segundo_apellido").isNotNull() & (length(trim(col("segundo_apellido"))) > 0),
                            trim(col("segundo_apellido")))
                    )))

    # OPTIMIZACIÓN DE nombre_completo (SOLO PARA FUENTE 1)
    df = df.withColumn("nombre_completo",
                    when(
                        es_fuente_uno & (
                            # Si está vacío, usar el generado
                            col("nombre_completo").isNull() | (length(trim(col("nombre_completo"))) == 0)
                        ),
                        col("nombre_completo_generado")
                    ).when(
                        es_fuente_uno & (
                            # Si el generado tiene más palabras
                            size(split(trim(col("nombre_completo_generado")), " ")) >
                            size(split(trim(col("nombre_completo")), " "))
                        ),
                        col("nombre_completo_generado")
                    ).when(
                        es_fuente_uno & (
                            # Si el generado es más largo (+10 chars)
                            length(trim(col("nombre_completo_generado"))) >
                            (length(trim(col("nombre_completo"))) + 10)
                        ),
                        col("nombre_completo_generado")
                    ).otherwise(
                        # Para fuente 1: preservar actual si es bueno
                        # Para otras fuentes: NO TOCAR NADA
                        when(es_fuente_uno, upper(trim(col("nombre_completo"))))
                        .otherwise(col("nombre_completo"))
                    ))

    # Limpieza de columnas temporales
    df = df.drop(
        campo_origen + "_limpio",
        "fuente_texto",
        "partes_array",
        "nombre_completo_generado"
    )

    return df

df_split = split_name(df_hashes_previos)
df_split.filter(col("numero_documento")=="27442523").show()


data = [
    # Caso 1: nombre completo mal diligenciado en primer_nombre
    ("101", "CASTRO PARDO JULIAN CAMILO", None, None, None, "CASTRO PARDO JULIAN CAMILO", 1),
    
    # Caso 2: nombre bien distribuido, nombre_completo correcto
    ("102", "JULIAN", "CAMILO", "CASTRO", "PARDO", "JULIAN CAMILO CASTRO PARDO", 1),
    
    # Caso 3: nombre bien distribuido, pero nombre_completo incompleto
    ("103", "JULIAN", "CAMILO", "CASTRO", "PARDO", "JULIAN CASTRO", 1),
    
    # Caso 4: nombre completo vacío, pero campos distribuidos presentes
    ("104", "JULIAN", "CAMILO", "CASTRO", "PARDO", None, 1)
]

columns = ["numero_documento", "primer_nombre", "segundo_nombre", "primer_apellido", "segundo_apellido", "nombre_completo", "cod_fuente_fk"]

df = spark.createDataFrame(data, columns)

from pyspark.sql.functions import split, size, col, when, concat_ws, trim, length

def split_name(df, campo_origen="primer_nombre"):
    partes = split(col(campo_origen), " ")


    df = df.withColumn("primer_nombre", when(
                            (col("cod_fuente_fk") == 1) & 
                            (size(partes) >= 1) & 
                            (col("segundo_nombre").isNull()) & 
                            (col("primer_apellido").isNull()) & 
                            (col("segundo_apellido").isNull()),
                            partes[0]
                        ).otherwise(col("primer_nombre"))) \
           .withColumn("segundo_nombre", when(
                            (col("cod_fuente_fk") == 1) & 
                            (size(partes) >= 2) & 
                            (col("segundo_nombre").isNull()),
                            partes[1]
                        ).otherwise(col("segundo_nombre"))) \
           .withColumn("primer_apellido", when(
                            (col("cod_fuente_fk") == 1) & 
                            (size(partes) >= 3) & 
                            (col("primer_apellido").isNull()),
                            partes[2]
                        ).otherwise(col("primer_apellido"))) \
           .withColumn("segundo_apellido", when(
                            (col("cod_fuente_fk") == 1) & 
                            (size(partes) >= 4) & 
                            (col("segundo_apellido").isNull()),
                            partes[3]
                        ).otherwise(col("segundo_apellido")))

    
    df = df.withColumn("nombre_completo_generado", concat_ws(" ",
        trim(col("primer_nombre")),
        trim(col("segundo_nombre")),
        trim(col("primer_apellido")),
        trim(col("segundo_apellido"))
    ))

   
    df = df.withColumn("nombre_completo", when(
        (col("nombre_completo").isNull()) |
        (length(trim(col("nombre_completo_generado"))) > length(trim(col("nombre_completo")))),
        trim(col("nombre_completo_generado"))
    ).otherwise(trim(col("nombre_completo"))))

    return df
 
df_resultado = split_name(df)
df_resultado.select(
    "numero_documento",
    "primer_nombre",
    "segundo_nombre",
    "primer_apellido",
    "segundo_apellido",
    "nombre_completo"
   
).show(truncate=False)

connection_options = {
    "connectionName": "sipren_dev_tierra",
    "query": "SELECT * FROM v_afiliados_porvenir WHERE ROWNUM <= 10"
}

try:
  
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="custom.jdbc",  
        connection_options=connection_options
    )

  
    df = dynamic_frame.toDF()
    df.show()
    print("Conexión JDBC realizada con éxito .")
except Exception as e:
    print(" No se pudo conecta.")

    print(str(e))


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from commons.ExtractHelper import ExtractHelper
from pyspark.sql import functions as F

import boto3
import datetime

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")
job = Job(glueContext)
ExtractHelper = ExtractHelper(glueContext, spark, "-", "-")

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from commons.ExtractHelper import ExtractHelper
from pyspark.sql import functions as F

import boto3
import datetime

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")
job = Job(glueContext)
ExtractHelper = ExtractHelper(glueContext, spark, "-", "-")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")

import datetime
from pyspark.sql import functions as F
s3_client = boto3.client("s3")
bucket_name = "alfa-bronce-test-jl"
BUCKET_NAME = "alfa-bronce-test-jl"
path
path_incre = f"s3://{BUCKET_NAME}/GEN/FILES/GEN-FILES-INCREMENTALL/"
path = f"{path_incre}"
df = ExtractHelper.extract_parquet_data(path, "*", "2025-1-8")

path_11 = f"s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_ASEGURADO/"
path_6 = f"s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_ASEGURADO/"

df_11 = ExtractHelper.extract_parquet_data(path_11, "*", "2025-1-11")
df_6 =  ExtractHelper.extract_parquet_data(path_6, "*", "2025-6-3")
df_11.printSchema()
df_6.printSchema()
df.show()
df = df.filter(F.col("max_id")=="1231585")
df = df.withColumn(
    "fecha_ejecucion",
    F.lit("2025-01-07")
)
df.show()
path_target = f"s3://{bucket_name}/GEN/FILES/GEN-FILES-"
path = f"{path_target}INCREMENTALL/"
ExtractHelper.save_files(df,path)
fields_and_tables = {
    "claim":"claimid"
}
def obtain_field(table_name):
    if table_name in fields_and_tables:
        return fields_and_tables[table_name]
print(obtain_field("claim"))
data = [
    ("CLAIM", "1635559", "2024/11/12")
    ]
df = spark.createDataFrame(data, ["tabla", "max_id", "fecha_ejecucion"])
path_target = f"s3://{bucket_name}/GEN/FILES/GEN-FILES-"
path = f"{path_target}INCREMENTAL/"
ExtractHelper.save_files(df,path)
bucket_name = "alfa-datalake-dev-bronce"
BUCKET_NAME = "alfa-datalake-dev-bronce"
def get_last_date(bucket_name, path):
    prefix = f"{path}"
    reponse = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")

    if 'CommonPrefixes' in reponse:
        dates = []
        for prefixArr in reponse['CommonPrefixes']:
            date = prefixArr['Prefix'].replace(prefix, '').strip('/')
            dates.append(date)
        
        last_date = max(dates)
        return last_date
    else:
        return "Date not found"

path = 'GEN/FILES/GEN-FILES-INCREMENTAL/'
str_date = ''
for i in range(3):
    date_str = ''
    Date = get_last_date(bucket_name, path)
    date_str = date_str + Date + '/'
    str_date = str_date + Date + '-'
    if date_str != 'Date not found':
        path = path + date_str
path_incre = f"s3://{BUCKET_NAME}/GEN/FILES/GEN-FILES-INCREMENTAL/"
path = f"{path_incre}"
Date = str_date[:-1]
Date
df = ExtractHelper.extract_parquet_data(path, "*", Date)
df.show()
import pyspark.sql.functions as F

tb = 'CLAIM'
df_filter = df.filter(df['tabla'] == tb)
max_date = df_filter.agg(F.max('fecha_ejecucion')).collect()[0][0]
id_max_date = df_filter.filter(df_filter['fecha_ejecucion'] == max_date).select('max_id').collect()[0][0]
print(id_max_date)
from pyspark.sql import functions as F
path_incre = f"s3://alfa-datalake-dev-bronce/GEN/FILES/GEN-FILES-INCREMENTAL/"
Date = "2024-11-20"
path = f"{path_incre}"
df = ExtractHelper.extract_parquet_data(path, "*", Date)
df.show()
max_value = int(df.agg(F.max("CLAIMID")).collect()[0][0])
max_value
ExtractHelper = ExtractHelper(glueContext, spark, "-", "-")
PolizaHelper = PolizaHelper(glueContext, spark)

Urincterrdeudores_helper = UrincterrdeudoresHelper(glueContext, spark)
TransformationHelper = TransformationHelper()
ClaimHelper = ClaimHelper(glueContext, spark)
def parse_schema(schema_str):
    fields = re.split(r',\s*(?=(?:[^()]*\([^()]*\))*[^()]*$)', schema_str)
    schema = []
    type_mapping = {
        "STRING": StringType,
        "INTEGER": IntegerType,
        "BOOLEAN": BooleanType,
        "FLOAT": FloatType,
        "DOUBLE": DoubleType
    }

    for field in fields:
        field = field.strip()
        if ":" not in field:
            print(f"Formato inválido en el esquema: {field}")
            continue
        field_name, field_type = map(str.strip, field.split(":", 1))
        field_type = field_type.upper()  

        try:
            if field_type.startswith("DECIMAL"):
                precision, scale = map(int, re.findall(r'\d+', field_type))
                schema.append(StructField(field_name, DecimalType(precision, scale), True))
            elif field_type in type_mapping:
                schema.append(StructField(field_name, type_mapping[field_type](), True))
            else:
                print(f"Tipo de dato desconocido para el campo {field_name}: {field_type}")
        except Exception as e:
            print(f"Error al procesar el esquema para el campo {field}: {e}")

    return StructType(schema)


def charge_tmp_tables(path, table)->None:
    """
    Function to extract data from parquets to df

    arg:
        path: The ubication of patquet files.
        tables: tables to extarct and create tmp views
    return:
        df: Return df with the data of parquet files.
    """
    fields = "*"
    table = table.upper()
    path = f"{path}{table}/"
    df = extract_parquet_data_gold(path, fields)
    df.createOrReplaceGlobalTempView(table)
    print(table)
    
def extract_parquet_data_gold(path, fields):
    """
    Function to extract data from parquets to df

    arg:
        path: The ubication of patquet files.
        fields: Fields that we need return by the function.
    return:
        df: Return df with the data of parquet files.
    """
    parquet_data = glueContext.create_dynamic_frame.from_options(
        format_options={},
        connection_type="s3",
        format="parquet",
        connection_options=
        {
            "paths": 
                [f"{path}2024/11/18"],
            "recurse": True
        },
        transformation_ctx="AmazonS3"
    )
    df = parquet_data.toDF()
    return df
path = f"s3://alfa-datalake-dev-assets/support_phase_files/componente5/procces_ola0.xlsx"
sheet = "gold_phase"
df_tables = ExtractHelper.extract_excel_data(path, sheet)
sheet = "gold_tables"
df_tmp_tables = ExtractHelper.extract_excel_data(path, sheet)
for row in df_tmp_tables.collect():
        table = row["Table"].upper()
        path_source =f"s3://alfa-datalake-dev-silver/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}_SILVER_"
        print(path_source+table)
        charge_tmp_tables(path_source, table)
        
df = spark.sql(
"""
WITH datos_sin_duplicados AS (
    SELECT DISTINCT
        cedula AS CC_NIT_ASEGURADO,
        fecha_nacimiento AS FECHA_NACIMIENTO_ASEGURADO,
        genero_asegurado AS GENERO_ASEGURADO
    FROM global_temp.asegurado
    UNION ALL
    SELECT DISTINCT
        cc_nit_asegurado AS CC_NIT_ASEGURADO,
        fecha_nacimiento_asegurado AS FECHA_NACIMIENTO_ASEGURADO,
        genero_ AS GENERO_ASEGURADO
    FROM global_temp.cierresgenerales_comp5
    UNION ALL
    SELECT DISTINCT
        cc_nit_asegurado AS CC_NIT_ASEGURADO,
        fecha_nacimiento_asegurado AS FECHA_NACIMIENTO_ASEGURADO,
        genero_ AS GENERO_ASEGURADO
    FROM global_temp.cierrevida_comp5
)
SELECT 
    row_number() over (order by CC_NIT_ASEGURADO) as id_asegurado_pk,
    CC_NIT_ASEGURADO as cedula,
    MAX(FECHA_NACIMIENTO_ASEGURADO) AS fecha_nacimiento,
    MAX(GENERO_ASEGURADO) as genero
FROM datos_sin_duplicados
GROUP BY CC_NIT_ASEGURADO
ORDER BY id_asegurado_pk
"""
)
df.show()
path = f"s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-{table}/"
response = ExtractHelper.save_files(df,path)
path = f"s3://alfa-datalake-dev-assets/support_phase_files/DIM_CAUSA_SINIESTRO.xlsx"
sheet = "CAUSAS"
df_tables = ExtractHelper.extract_excel_data(path, sheet)
df_tables.show(5)
path = f"s3://alfa-gold-test/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-DIM_CAUSA_SINIESTRO/"
ExtractHelper.save_files(df_tables,path)
path = f"s3://alfa-gold-test/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-DIM_TOMADOR_UNIFICADO/"
Date = "2024-11-9"
df = ExtractHelper.extract_parquet_data(path, "*", Date)
path = f"s3://alfa-gold-test/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_POLIZA/"
Date = "2024-11-23"
df_2 = ExtractHelper.extract_parquet_data(path, "*", Date)
df.printSchema()
df_2.printSchema()
df = df.select("id_poliza_lider_pk")
df.count()
df_2.count()
df_2 = df_2.select("NRO_ACUERDO")
df_result = df_2.join(df, F.upper(df.id_poliza_lider_pk) == F.upper(df_2.NRO_ACUERDO), "left")
from pyspark.sql import functions as F
test = df_result.filter(F.col("id_poliza_lider_pk").isNull())
test.count()
test_1 = test.select("NRO_ACUERDO","tomador_unificado","negocio_alfa","segmento")
test_1.count()
test_1 = test_1.withColumn(
    "id_poliza_lider_pk",
    F.col("NRO_ACUERDO")
)
test_1 = test_1.withColumn(
    "tomador_unificado",
    F.lit("No registra")
)
test_1 = test_1.withColumn(
    "negocio_alfa",
    F.lit("No registra")
)
test_1 = test_1.withColumn(
    "segmento",
    F.lit("No registra")
)
test_1.show(5)
test_1 = test_1.select("id_poliza_lider_pk", "tomador_unificado", "negocio_alfa", "segmento")
path = f"s3://alfa-gold-test/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-DIM_TOMADOR_UNIFICADO/"
ExtractHelper.save_files(test_1,path)
path_principal = "s3://alfa-gold-test-jl/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_SINIESTROS-PF/"
nombre = path_principal.split("-")[-2]
nombre = nombre[:-2]
nombre = "FACT_SINIESTROS-PF"
import boto3
def get_partition_data(path):
    s3 = boto3.client("s3")
    bucket = path.split("/")[2]
    prefix = "/".join(path_principal.split("/")[3:])
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter = '/')
    
    partitions = []
    
    for content in response.get("CommonPrefixes", []):
        partition = content.get("Prefix")#.split("/")[-2]
        response_m = s3.list_objects_v2(Bucket=bucket, Prefix=partition, Delimiter = '/')
        for content in response_m.get("CommonPrefixes", []):
            partition_m = content.get("Prefix")#.split("/")[-2]
            response_d = s3.list_objects_v2(Bucket=bucket, Prefix=partition_m, Delimiter = '/')
            for content in response_d.get("CommonPrefixes", []):
                partition_d_v2 = content.get("Prefix")#.split("/")[-4:]
                partition_d = content.get("Prefix")[:-1].split("/")[-3:]
                partitions.append(partition_d)
        
    return partitions
particiones = get_partition_data(path_principal)
particiones
dfs = []
particiones = get_partition_data(path_principal)
nombre = path_principal.split("-")[-1]
nombre = nombre[:-1]
table = "FACT_SINIESTROS"
dfs = []
for fechas in particiones:
    path = f"{path_principal}{fechas[0]}/{fechas[1]}/{fechas[2]}/"
    df = spark.read.parquet(path)
    dfs.append(df)
    
if dfs:
    df_incremental = dfs[0]
    print(len(dfs))
    if len(dfs) > 1:
        for df in dfs[1:]:
            df_incremental = df_incremental.union(df)
            df_incremental.createOrReplaceGlobalTempView(table.upper())
    else:
        df_incremental.createOrReplaceGlobalTempView(table.upper())
else:
    print("Sin Data")
df_test = spark.sql("SELECT DISTINCT * FROM global_temp.FACT_SINIESTROS")
df_test.count()
df_incremental.count()
from pyspark.sql.window import Window
from pyspark.sql import functions as F
pruebaJose = [
    ("5001210640", "Avisado", "2023-06-13", 1000000.0, ""),
    ("5001210640", "Pagado", "2023-07-14", -6703900.0, ""),
    ("5001210640", "Pagado", "2023-08-03", -6703900.0, ""),
    ("5001210640", "Pagado", "2023-08-03", -1.0, ""),
    ("5001210640", "Pagado", "2023-08-03", -1.0, ""),
    ("5001210640", "Pagado", "2023-08-03", -1.0, ""),
    ("5001210640", "Avisado", "2023-08-03", 1.0, ""),
    ("5001210640", "Rechazo de pago", "2023-08-03", 1.0, ""),
    ("5001210640", "Rechazo de pago", "2023-08-03", 1.0, ""),
    ("5001210640", "Pagado", "2023-08-03", 6703900.0, ""),
    ("5001210640", "Avisado", "2023-08-03", -6703900.0, ""),
    ("123456", "Constitucion reserva", "2018-03-12", 5115821.0, ""),
    ("123456", "Pagado", "2018-03-13", -4842293.0, ""),
    ("123456", "Pagado", "2018-04-18", -273528.0, ""),
    ("123456", "Pagado", "2018-04-14", -273528.0, ""),
    ("123456", "Pagado", "2018-04-18", -103528.0, ""),
    ("123456", "Pagado", "2018-04-13", -103528.0, ""),
    ("123456", "Rechazo de pago", "2018-04-18", -103528.0, ""),
    ("123456", "Rechazo de pago", "2018-04-19", 273528.0, ""),
    ("123456", "Pagado", "2018-05-10", -273528.0, ""),
    ("123456", "Pagado", "2018-05-10", -273528.0, ""),
    ("123456", "Rechazo de pago", "2018-05-17", 273528.0, ""),
    
    
    ("1818", "Rechazo de pago", "2018-05-19", 273528.0, ""),
    ("1818", "Pagado", "2018-05-10", -273528.0, ""),
    ("1818", "Pagado", "2018-05-10", 273528.0, ""),
    ("1818", "Pagado", "2018-06-17", 273528.0, "")
]
columns = ['numero_siniestro','tipo_transaccion','fecha_transaccion','valor_transaccion','observaciones']
df = spark.createDataFrame(pruebaJose, columns)
df = df.select(
                F.col("numero_siniestro").alias("id_siniestro_fk"),
                F.month("fecha_transaccion").alias("mes"),
                F.year("fecha_transaccion").alias("ano"),
                F.col("tipo_transaccion").alias("estado"), 
                F.col("valor_transaccion").alias("monto")

            )
df.filter(F.col("numero_siniestro")==123456).show()
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


# Datos de entrada

# Agregar columna con valor absoluto para el emparejamiento
df1 = df.withColumn("valor_abs", F.abs(F.col("monto")))

# Separar pagos y rechazos
df_pago = (
    df1.filter(F.col("estado") == "Pagado")
    .withColumn("prioridad", F.when(F.col("monto") < 0, 1).otherwise(2))  # Prioridad para pagos
)
df_rechazo = df1.filter(F.col("estado") == "Rechazo de pago").withColumn("prioridad", F.lit(0))  # Prioridad fija para rechazos

# Crear ventana para priorizar pagos y rechazos
windowSpec = Window.partitionBy("valor_abs", "ano", "mes", "id_siniestro_fk").orderBy("prioridad")

# Agregar índice para control del emparejamiento en pagos y rechazos
df_pago = df_pago.withColumn("grupo", F.row_number().over(windowSpec))
df_rechazo = df_rechazo.withColumn("grupo", F.row_number().over(windowSpec))

# Emparejar rechazos con pagos (usando alias para evitar conflictos)
df_join = df_rechazo.alias("rechazo").join(
    df_pago.alias("pago"),
    on=["valor_abs", "ano", "mes", "id_siniestro_fk", "grupo"],
    how="inner"
)

# Filtrar pagos y rechazos emparejados
df_emparejados = df_join.select(
    F.col("rechazo.estado").alias("estado_rechazo"),
    F.col("rechazo.monto").alias("monto_rechazo"),
    F.col("pago.estado").alias("estado_pago"),
    F.col("pago.monto").alias("monto_pago")
)

# Obtener pagos no emparejados
df_pagos_restantes = df_pago.alias("pago").join(
    df_join.alias("emp"), 
    on=["valor_abs", "ano", "mes", "id_siniestro_fk", "grupo"], 
    how="left_anti"
)
df_final = df_pagos_restantes.select("estado", "monto", "ano", "mes", "id_siniestro_fk")
df2 = df.filter(~F.col("estado").isin('Pagado','Rechazo de pago'))
# Resultado final

df_total = df2.union(df_final)
# Mostrar resultados
df_total.show()

df_total.filter(F.col("id_siniestro_fk")==5001210640).show()
df1 = df.filter(F.col("tipo_transaccion").isin('Pagado','Rechazo de pago'))
df_pago = df1.filter(F.col("tipo_transaccion").isin('Pagado'))
df_rechazo = df1.filter(F.col("tipo_transaccion").isin('Rechazo de pago'))
df1.filter(F.col("numero_siniestro")==123456).show()
windowsSpect = Window.partitionBy("valor_transaccion_ajustado","anio","mes","numero_siniestro").orderBy("fecha_transaccion")
df_rechazos = df_rechazo.withColumn(
    "grupo", F.row_number().over(windowsSpect)
).alias("A")
df_pago = df_pago.withColumn(
    "grupo", F.row_number().over(windowsSpect)
).alias("B")
df_join = df_rechazos.join(df_pago, on= ["valor_transaccion_ajustado", "anio", "mes", "numero_siniestro","grupo"], how="outer")
df_join.show()
df_join.filter(F.col("numero_siniestro")==123456).orderBy("grupo").show()
df_final = df_join.filter((F.col("A.tipo_transaccion").isNull()) & (F.col("B.tipo_transaccion").isNotNull()))
df_final = df_final.select('B.numero_siniestro','B.tipo_transaccion','B.fecha_transaccion','B.valor_transaccion','B.observaciones')#.filter(F.col("numero_siniestro")==123456).show()
df2 = df.filter(~F.col("tipo_transaccion").isin('Pagado','Rechazo de pago'))
df_total = df2.union(df_final)
df_total.show()
df_rechazos = df1.filter(
    F.col("tipo_transaccion") == "Rechazo de pago"
).withColumn(
    "grupo", F.row_number().over(windowsSpect)
)

df_pago = df1.filter(
    F.col("tipo_transaccion") == "Pagado"
).withColumn(
    "grupo", F.row_number().over(windowsSpect)
)
df_resultado = df_rechazos.union(df_pago)
df_resultado.filter(F.col("numero_siniestro")==123456).show()
df_rechazos = df1 = df.filter(F.col("tipo_transaccion").isin('Rechazo de pago')).alias("A")
df_pagos = df1 = df.filter(F.col("tipo_transaccion").isin('Pagado')).alias("B")
df1 = df1.withColumn("valor_transaccion", F.abs(F.col("valor_transaccion")))
window_spec_r = Window.partitionBy("valor_transaccion").orderBy("fecha_transaccion","numero_siniestro")
df_rechazo =  df1.filter(F.col("tipo_transaccion").isin('Rechazo de pago')).withColumn("grupo",F.row_number().over(window_spec_r))

window_spec_g = Window.partitionBy("valor_transaccion").orderBy("fecha_transaccion","numero_siniestro")
df_pago =  df1.filter(F.col("tipo_transaccion").isin('Pagado')).withColumn("grupo", F.when(F.col("numero_siniestro").isNotNull(),F.row_number().over(window_spec_g)).otherwise(F.lit(None)))

df_join = df_rechazo.alias("rechazo").join(
    df_pago.alias("pago"),
    F.col("rechazo.valor_transaccion") == F.col("pago.valor_transaccion"),
    "outer"
)
df_result = df_join.select(
    F.col("rechazo.numero_siniestro").alias("numero_siniestro"),
    F.col("rechazo.fecha_transaccion").alias("fecha"),
    F.col("rechazo.valor_transaccion").alias("valor_transaccion"),
    F.when(
        F.col("rechazo.tipo_transaccion").isNotNull(), F.col("rechazo.tipo_transaccion")
    ).otherwise(F.col("pago.tipo_transaccion")).alias("tipo_transaccion"),
    F.coalesce(F.col("rechazo.grupo"),F.col("pago.grupo")).alias("grupo")
)
df_result.show()
df_grupo.filter(F.col("numero_siniestro")==123456).show()
df_join = df_grupo.alias("rechazo").join(
    df_grupo.alias("pago"),
    (F.col("rechazo.valor_transaccion") == F.col("pago.valor_transaccion")) &
    (F.col("rechazo.group") == F.col("pago.group")),
    "outer"
)
df_join.filter(F.col("rechazo.numero_siniestro")==123456).show()
df_final = df_join.select("rechazo.*")
df_final.show()
df1 = df.filter(F.col("estado").isin('Pagado','Rechazo de pago'))
df_pago = df1.filter(F.col("estado") == "Pagado").filter(F.col("monto") < 0)
df_rechazo = df1.filter(F.col("estado") == "Rechazo de pago").filter(F.col("monto") > 0)
df_pago = df_pago.withColumn("valor_transaccion_ajustado", F.abs(F.col("monto")))
df_rechazo = df_rechazo.withColumn("valor_transaccion_ajustado", F.abs(F.col("monto")))
windowsSpect = Window.partitionBy("valor_transaccion_ajustado","ano","mes","id_siniestro_fk").orderBy("ano","mes")
df_rechazos = df_rechazo.withColumn("grupo", F.row_number().over(windowsSpect)).alias("A")
df_pago = df_pago.withColumn("grupo", F.row_number().over(windowsSpect)).alias("B")
df_join = df_rechazos.join(df_pago, on= ["valor_transaccion_ajustado", "ano", "mes", "id_siniestro_fk","grupo"], how="outer")
df_final = df_join.filter((F.col("A.estado").isNull()) & (F.col("B.estado").isNotNull()))
df_final = df_final.select('id_siniestro_fk','mes','ano','B.estado','B.monto')
df2 = df.filter(~F.col("estado").isin('Pagado','Rechazo de pago'))

df_total = df2.union(df_final)

df_final.filter(F.col("id_siniestro_fk")==1818).show()

    # Filtra pagos y rechazos
df_pago = df.filter(F.col("estado") == "Pagado").filter(F.col("monto") < 0)
df_rechazo = df.filter(F.col("estado") == "Rechazo de pago").filter(F.col("monto") > 0)
print(df_pago.show())
# Define ventana para agrupamiento por atributos relevantes
windowSpec = Window.partitionBy("id_siniestro_fk", "ano", "mes").orderBy("ano", "mes")

# Añade columna para numerar filas dentro del grupo, con nombres únicos
df_rechazo = df_rechazo.withColumn("grupo_rechazo", F.row_number().over(windowSpec))
df_pago = df_pago.withColumn("grupo_pago", F.row_number().over(windowSpec))

# Renombra columnas relevantes para evitar conflictos en la unión
df_rechazo = df_rechazo.withColumnRenamed("id_siniestro_fk", "rechazo_id_siniestro_fk")
df_pago = df_pago.withColumnRenamed("id_siniestro_fk", "pago_id_siniestro_fk")
df_pago = df_pago.withColumnRenamed("monto", "pago_monto")
df_rechazo = df_rechazo.withColumnRenamed("monto", "rechazo_monto")
df_pago = df_pago.withColumnRenamed("ano", "pago_ano")
df_rechazo = df_rechazo.withColumnRenamed("ano", "rechazo_ano")
df_pago = df_pago.withColumnRenamed("mes", "pago_mes")
df_rechazo = df_rechazo.withColumnRenamed("mes", "rechazo_mes")
df_pago = df_pago.withColumnRenamed("estado", "pago_estado")
df_rechazo = df_rechazo.withColumnRenamed("estado", "rechazo_estado")
# Une pagos negativos con rechazos positivos por grupo y monto opuesto
df_join = df_rechazo.join(
    df_pago.filter(F.col("monto") < 0),

    (F.col("rechazo_id_siniestro_fk") == F.col("pago_id_siniestro_fk")) &
    (F.col("rechazo_monto") == -F.col("pago_monto")),
    how="inner"
)

# Selecciona solo las columnas necesarias tras la unión
df_join = df_join.select(
    F.col("rechazo_id_siniestro_fk").alias("id_siniestro_fk"),
    F.col("rechazo_ano").alias("ano"),
    F.col("rechazo_mes").alias("mes"),
    F.col("rechazo_monto").alias("monto"),
    F.col("rechazo_estado").alias("estado"),
)

# Filtra los registros válidos y elimina duplicados
df_final = df.subtract(df_join)
df_final = df_final.filter(~F.col("estado").isin('Rechazo de pago'))
df_final.filter(F.col("id_siniestro_fk")==1818).show()
def create_tmp_view( df, tb_name, phase):
        """
        This funtion help us to create tmp vies, global or normal tmp views.
        """
        print(df.printSchema())
        print(tb_name)
        print(phase)
        try:
            if phase == 'Gold':
                df.createOrReplaceGlobalTempView(tb_name)
            else:
                df.createOrReplaceTempView(tb_name)
        except Exception as e:
            print( f"Error al crear la vista {e}")

def return_table( table):
    """
    This Function Help us to return the name of tmp table iew depend the fact tables in gold catalog.
    """
    if table.upper() == "FACT_POLIZA-PF":
        table = "POLIZAS"
    if table.upper() == "FACT_SINIESTROS-PF":
        table = "SINIESTROS"
    if table.upper() == "FACT_POLIZA_DETALLADA-PF":
        table = "POLIZADETALLADA"
    if table.upper() == "FACT_ASEGURADO":
        table = "ASEGURADOGOLD"
    return table

def charge_tmp_tables( path, table, phase:str = 'Silver')->None:
    """
    Function to extract data from parquets to df

    arg:
        path: The ubication of patquet files.
        tables: tables to extarct and create tmp views
    return:
        df: Return df with the data of parquet files.
    """
    table = table.upper()
    path = f"{path}{table}/"
    print(path)
    particiones = get_partition_data(path)
    dfs = []
    print(particiones)
    for fechas in particiones:
        print(f" este es el path para cargar la data {path}")
        df = extract_parquet_data_silver(path, fechas[0],fechas[1],fechas[2])
        
        df = df.withColumn(
            "partition" , F.lit(f"{fechas[0]}-{fechas[1]}-{fechas[2]}")
        )
        columns = sorted(df.columns)
        df = df.select(columns)
        dfs.append(df)
        table = return_table(table)
    if dfs:
        df_incremental = dfs[0]
        if len(dfs) > 1:
            for df in dfs[1:]:
                df_incremental = df_incremental.union(df)
                create_tmp_view(df_incremental, table.upper(), phase)
        else:
            create_tmp_view(df_incremental, table.upper(), phase)
    else:
        df_incremental = None
    return df_incremental

def get_partition_data( path):
    s3 = boto3.client("s3")
    bucket = path.split("/")[2]
    prefix = "/".join(path.split("/")[3:])

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter = '/')

    partitions = []
    print(f"este es el path para sacar las particiones: {path}")
    for content in response.get("CommonPrefixes", []):
        partition = content.get("Prefix")
        print(content)
        response_m = s3.list_objects_v2(Bucket=bucket, Prefix=partition, Delimiter = '/')
        for content in response_m.get("CommonPrefixes", []):
            partition_m = content.get("Prefix")
            print(partition_m)
            response_d = s3.list_objects_v2(Bucket=bucket, Prefix=partition_m, Delimiter = '/')
            for content in response_d.get("CommonPrefixes", []):
                partition_d = content.get("Prefix")[:-1].split("/")[-3:]
                partitions.append(partition_d)

        return partitions
    
def extract_parquet_data_silver( path, year, month, day):
        """
        Function to extract data from parquets to df

        arg:
            path: The ubication of patquet files.
            fields: Fields that we need return by the function.
        return:
            df: Return df with the data of parquet files.
        """
        try:
            parquet_data = glueContext.create_dynamic_frame.from_options(
                format_options={},
                connection_type="s3",
                format="parquet",
                connection_options=
                {
                    "paths": 
                        [f"{path}{year}/{month}/{day}"],
                    "recurse": True
                },
                transformation_ctx="AmazonS3"
            )
        except Exception as e:
            print(e)
        df = parquet_data.toDF()
        return df
from commons.IncrementalCharge import IncrementalCharge
IncrementalCharge = IncrementalCharge(glueContext, spark, "-", "-")
path_source = "s3://alfa-bronce-test-jl/CAMBIADOS/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"
table = "CLAIM_MODIFY"
IncrementalCharge.charge_tmp_tables(path_source, table, 'Gold')
df_data_modify = spark.sql("SELECT * FROM global_temp.CLAIM_MODIFY")
df_data_modify = df_data_modify.filter(F.col("RN") == 1)
print(df_data_modify.count())
df_data_modify.filter(F.col("CLAIMID")=='1642991').show()
path_source = "s3://alfa-bronce-test-jl/CAMBIADOS/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"
table = "CLAIM_MODIFY"
IncrementalCharge.charge_tmp_tables(path_source, table, 'Gold')
df_data_modify = spark.sql("SELECT * FROM global_temp.CLAIM_MODIFY")
df_data_modify = df_data_modify.filter(F.col("RN") == 1)
print(df_data_modify.count())
df_data_modify.filter(F.col("CLAIMID")=='1642991').show()
#df.filter(F.col("CLAIMID")=='1640520').show()
#df_data_modify.count()
path_source = "s3://alfa-bronce-test-jl/CAMBIADOS/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"
table = "CLAIM"
IncrementalCharge.charge_tmp_tables(path_source, table, 'Gold')
df = spark.sql("SELECT * FROM global_temp.CLAIM")
df.filter(F.col("CLAIMID")=='1642991').show()

columnas_df_principal = sorted(df.columns)
columnas_df_modify = df_data_modify.columns
columnas_df_principal
columnas = set(columnas_df_modify).difference(set(columnas_df_principal))
column = []
for i in columnas:
    column.append(i)
df_data_modify = df_data_modify.drop("partition")
df_cambiados = df.alias('A').join(
    df_data_modify.alias('B'),
    F.col("A.CLAIMID") == F.col("B.CLAIMID"),
    "INNER"
).select(df_data_modify["*"],df["partition"])##guardo los que se van ha cambiar con la particion donde estan
df_cambiados.show(1)
df_cambiados.count()
date_cambiados = df_cambiados.select("partition").distinct().collect()##guard las particiones que tienen cambios
df.count()
for i in date_cambiados:
    df_i = df.filter(F.col("partition")==i["partition"]).select(columnas_df_principal)
    print(f"Cantidad de datos en df original: {df_i.count()}")
    df_cambiados_i = df_cambiados.alias('B').filter(F.col("partition")==i["partition"]).select(columnas_df_principal)
    print(f"Conteo de df con los datos que se cambian: {df_cambiados_i.count()}")
    #df = df.join(df_incremental, on="cedula", how='left_anti')
    #ids = df_cambiados_i.select("CLAIMID").distinct().collect()
    #df_i = df_i.filter(~F.col("CLAIMID").isin(ids))
    df_i = df_i.join(
        df_cambiados_i,
        on = "CLAIMID",
        how = "left_anti"
    )#.select(df_i["*"])##Quito los que se van ha modificar de la particiones
    print(f"Cantidad de datos del df original si los registros que se cambian :{df_i.count()}")
    df_cambiados_i = df_cambiados_i.drop(*column)
    df_i = df_i.union(df_cambiados_i)
    path_source = "s3://alfa-bronce-test-jl/CAMBIADOS/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"
    table = "CLAIM"
    delete_data(path_source, table, i["partition"])
    save_files(df_i, path_source, table, i["partition"])
    print(f"Union de los df tiene que dar lo del primero: {df_i.count()}")
path = f"s3://alfa-datalake-dev-assets/support_phase_files/Registros_cambiados.xlsx"
sheet = "Hoja1"
df_data = ExtractHelper.extract_excel_data(path, sheet)
BUCKET_NAME = "alfa-bronce-test-jl"
s3_client = boto3.client("s3")
def delete_data(path_source, table, partition):
    folder_date = partition.split('-')
    year = folder_date[0]
    month = folder_date[1]
    day = folder_date[2]
    path_source = path_source.replace(f"s3://{BUCKET_NAME}/","")
    path = f"{path_source}{table}/{year}/{month}/{day}/"
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix = path)
    if 'Contents' in response:
        for obj in response['Contents']:
            s3_client.delete_object(Bucket=BUCKET_NAME, Key=obj['Key'])
from awsglue import DynamicFrame
def save_files(df, path, table, partition):
        """
        This function save df in parquet files.

        arg:
            df: Dataframe to save in parquet files.
            tb_name: Name of table to save (save in path s3)
            path: path to save files.
        retunr:
            response: response of the execution
        """
        folder_date = partition.split('-')
        year = folder_date[0]
        month = folder_date[1]
        day = folder_date[2]
        path = f"{path}{table}/"
        print(df.count())
        print(f"{path}{year}/{month}/{day}")
        df_dynamic = DynamicFrame.fromDF(df , glueContext, "nested")
        response = glueContext.write_dynamic_frame.from_options(
            frame=df_dynamic, 
            connection_type="s3", 
            format="glueparquet", 
            connection_options={"path": f"{path}{year}/{month}/{day}/", "partitionKeys": []},
            format_options={"compression": "snappy"}, 
            transformation_ctx="AmazonS3_write"
        )
        return response
#df_data.show()
data = []
columns = ["TABLA", "PK_FIELD", "PK_KEY", "MODIFY_FIELD","VALUE","OLD_VALUE","DATE"]
date = current_date = datetime.datetime.now()
try:
    for row in df_data.collect():
        path_source = f"s3://{BUCKET_NAME}/CAMBIADOS/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"
        table = row["Tabla"]
        IncrementalCharge.charge_tmp_tables(path_source, table, 'Gold')
        print("Termine la carga")
        df = None
        df = spark.sql(f"SELECT * FROM global_temp.{table}")
        if df != None:
            #df_search = df.filter(F.col(row["Id_field"]) == row["Id"]).select("partition")
            partition = df.filter(F.col(row["Id_field"]) == row["Id"]).select("partition").first()[0]
            print(partition)
            df_to_modify = df.filter(F.col("partition") == partition)
            df_to_modify.count()
            df_search = df_to_modify.filter(F.col(row["Id_field"]) == row["Id"])
            df_search.show()
            old_value = df_search.filter(F.col(row["Id_field"]) == row["Id"]).select(row["modify_field"].upper()).first()[0]
            df_search = df_search.withColumn(
                row["modify_field"].upper(), F.lit(row["value"])
            )
            df_search.show()
            df_final = df_to_modify.filter(~(F.col(row["Id_field"]) == row["Id"]))
            df_final.count()
            df_final = df_final.union(df_search)
            df_final = df_final.drop("partition")
            df_final.count()
            df_final.filter(F.col(row["Id_field"]) == row["Id"]).show()
            delete_data(path_source, table, partition)
            save_files(df_final, path_source, table, partition)
            historical_row = (row["Tabla"], row["Id_field"], row["Id"], row["modify_field"], row["value"], str(old_value), f"{date.year}-{date.month}-{date.day}")
            data.append(historical_row)
            df = spark.createDataFrame(data, columns)
            df.show()
        else:
            print("No hay datos par amodificar")
except Exception as e:
    print(e)
from commons.IncrementalCharge import IncrementalCharge
from pyspark.sql.types import IntegerType
IncrementalCharge = IncrementalCharge(glueContext, spark, "-", "-")
path = f"s3://alfa-datalake-dev-assets/support_phase_files/procces_ola1.xlsx"
sheet = "gold_phase"
df_tables = ExtractHelper.extract_excel_data(path, sheet)
sheet = "gold_tables"
df_tmp_tables = ExtractHelper.extract_excel_data(path, sheet)


for row in df_tmp_tables.collect():
    table = row["Table"].upper()
    if row["Phase"].upper() == "SILVER":
        path_source =f"s3://alfa-datalake-dev-silver/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}_SILVER_"
    else:
        path_source =f"s3://alfa-datalake-dev-gold/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-CARACT_SINIESTROS-"
        print(table)
    IncrementalCharge.charge_tmp_tables(path_source, table, 'Gold')
from commons.StepGold import StepGold
from commons.StepGoldPlus import StepGoldPlus
from commons.StepGoldBodega import StepGoldBodega

StepGold = StepGold(glueContext, spark)
StepGoldPlus = StepGoldPlus(glueContext, spark)
StepGoldBodega = StepGoldBodega(glueContext, spark)
df_tierra = StepGold.create_component()
df_plus = StepGoldPlus.create_component()
df_bodega = StepGoldBodega.create_component()
        
df_tierra.count()
df_plus.count()
df_bodega.count()
df_tmp_tables = ['bodega_plus', 'snr_dato_basico','snr_causa','snr_origen','rsv_cobertura','rcd_polizas']
for row in df_tmp_tables:
    path_source =f"s3://alfa-silver-jl/SIPREN/TRANSFORMATION/SIPREN_SILVER_"
    IncrementalCharge.charge_tmp_tables(path_source, row, 'Gold')
DF_BODEGA = spark.sql("select * from global_temp.bodega_plus")
DF_DATO_BASICO = spark.sql("select * from global_temp.snr_dato_basico")
DF_CAUSAS = spark.sql("select * from global_temp.snr_causa")
DF_ORIGEN = spark.sql("select * from global_temp.snr_origen")
DF_COBERTURA = spark.sql("select * from global_temp.rsv_cobertura")#se usa ?
DF_POLIZAS = spark.sql("select * from global_temp.rcd_polizas")

DF_RESULT = DF_BODEGA.alias('B').join(
    DF_DATO_BASICO.alias('D'), F.col('B.NID_SINIESTRO') == F.col('D.ID_SINIESTRO'), 'left'
    ).join(
    DF_CAUSAS.alias('c'), F.col('D.COD_CAUSA_SINIESTRO') == F.col('c.ID_CAUSAL'), 'left'
    ).join(
    DF_ORIGEN.alias('o'), F.col('D.COD_ORIGEN_SINIESTRO') == F.col('o.COD_ORIGEN_SINIESTRO'), 'left'
    ).join(
    DF_POLIZAS.alias('p'),  F.coalesce(F.col('B.fecha_siniestro'), F.to_date(F.concat_ws('', F.col('B.periodo_point'), F.lit('01')), 'yyyyMMdd')).between(F.col('p.FEC_INICIAL_VIGENCIA'),F.col('p.FEC_FINAL_VIGENCIA'))
    )
    
DF_UNO = DF_RESULT.filter(
    (F.col('B.NID_SINIESTRO').cast(IntegerType()) >= 20200000000) & (F.col("b.cod_movimiento") == "Recuperacion")
).selectExpr(
    "'3' as id_categoria_fk",
    "b.concepto_pago AS categoria",
    "'Facturas' AS tipo",
    "CASE b.cod_movimiento  WHEN 'Pago' THEN 7 ELSE 7 END AS id_tipo_transaccion_fk",
    "CASE b.cod_movimiento  WHEN 'Pago' THEN 'Recobros' ELSE 'Recobros' END AS tipo_transaccion",
    "b.NID_SINIESTRO AS numero_siniestro",
    "abs(cast(b.VLR_pago as double)) AS valor_transaccion",
    "b.VLR_pago AS valor_transaccion_ajustado",
    "cast(to_date(b.periodo_point || '01' ,'%yyyy%MM%dd') as date)  AS fecha_transaccion",    
    "COALESCE(cast(b.fecha_siniestro as date), cast(to_date(b.periodo_point || '01' ,'%yyyy%MM%d') as date))  AS fecha_ocurrencia",
    "c.ID_CAUSAL as cod_causa",    
    "COALESCE(c.NOMBRE, 'Otras causas') AS causa",
    "o.cod_origen_siniestro as cod_origen", 
    "COALESCE(o.NOMBRE,'Incapacidad total y permanente') AS origen",
    "cast(to_date(b.periodo_point || '01' ,'%yyyy%MM%dd') as date)   AS fecha_aviso",
    "COALESCE(p.NID_POLIZA ,999) AS poliza",
    "'3' as id_fuente_fk",
    "'50' AS compania",
    "41 AS id_producto_fk",
    "31 AS id_ramo_fk",
    "'Seguridad Social' AS unidad_de_negocio",   
    "CONCAT('PRV-',LPAD(COALESCE(CAST(p.NID_POLIZA AS VARCHAR(10)),'999'),7,'0'),'-00') as poliza_lider"






)
DF_DOS.count()
    
df_recuperacion = DF_RESULT.filter(
    (F.col('B.NID_SINIESTRO')== "99999999")
).selectExpr(
    "'2'  AS id_categoria_fk",
    "b.concepto_pago    AS categoria",
    "'Recuperacion' AS tipo",
    "CASE b.cod_movimiento  WHEN 'Pago' THEN  5   ELSE 5 END AS id_tipo_transaccion_fk",
    "CASE b.cod_movimiento  WHEN 'Pago' THEN 'Pago'  ELSE 'Pago'  END AS tipo_transaccion",    
    "b.NID_SINIESTRO AS numero_siniestro",
    "abs(cast(b.VLR_pago as double)) AS valor_transaccion",
    "b.VLR_pago AS valor_transaccion_ajustado",
    "cast(to_date(b.periodo_point || '01' ,'%yyyy%MM%dd') as date)  AS fecha_transaccion",
    "COALESCE(cast(b.fecha_siniestro as date), cast(to_date(b.periodo_point || '01' ,'%yyyy%MM%dd') as date))  AS fecha_ocurrencia",    
    "'384'	as cod_causa",
    "COALESCE(c.NOMBRE,'Otras causas' ) AS causa",
    "'742' as cod_origen",    
    "COALESCE(o.NOMBRE,'Incapacidad total y permanente' ) AS origen",
    "cast(to_date(b.periodo_point || '01' ,'%yyyy%MM%dd') as date)   AS fecha_aviso",
    "COALESCE(p.NID_POLIZA ,999) AS poliza",
    "'3' as id_fuente_fk",
    "'50' AS compania",
    "41 AS id_producto_fk",    
    "31 AS id_ramo_fk",
    "'Seguridad Social' AS unidad_de_negocio",
    "CONCAT('PRV-',LPAD(COALESCE(CAST(p.NID_POLIZA AS VARCHAR(10)),'999'),7,'0'),'-00') as poliza_lider"

    



)
DF_BODEGA = DF_UNO.union(df_recuperacion)
df_recuperacion.count()
##tierra
df_tmp_tables_bodega = ['bodega_plus', 'snr_dato_basico','snr_causa','snr_origen','rsv_cobertura','rcd_polizas','spr_detalle_reserva','spr_reserva']
df_tmp_tables_tierra = ['spr_detalle_reserva','spr_reserva','spr_conceptos_pagos_siniestros','cgr_movimientos_reserva','spr_siniestros','spr_causas_siniestros','cgr_origenes','vw_dsdafil_alfa','ben_solicitudes_v','ppr_polizas','spr_incapacidades_sol_audit','spr_detalle_pago_siniestro','spr_pagos','spr_recuperaciones','spr_tipos_recuperaciones']
df_tmp_tables_plus = ['bodega_plus','snr_dato_basico','snr_causa','snr_origen','rsv_cobertura','rcd_polizas','rsv_dato_reserva','pgs_dato_pago','rsv_tipo_movimiento','snr_dato_tramite']
for row in df_tmp_tables_tierra:
    path_source_plus_bodega =f"s3://alfa-silver-jl/SIPREN/TRANSFORMATION/SIPREN_SILVER_"
    path_source_tierra  = f"s3://alfa-datalake-dev-silver/SIPREN/TRANSFORMATION/SIPREN_SILVER_"
    IncrementalCharge.charge_tmp_tables(path_source_tierra, row, 'Gold')
from commons.StepGold import StepGold
StepGold = StepGold(glueContext, spark)

df_main = StepGold.create_component()
df_main.show(1)
df_main.count()
###PLUS###
df_tmp_tables_bodega = ['bodega_plus', 'snr_dato_basico','snr_causa','snr_origen','rsv_cobertura','rcd_polizas','spr_detalle_reserva','spr_reserva']
df_tmp_tables_tierra = ['spr_detalle_reserva','spr_reserva','spr_conceptos_pagos_siniestros','cgr_movimientos_reserva','spr_siniestros','spr_causas_siniestros','cgr_origenes','vw_dsdafil_alfa','ben_solicitudes_v','ppr_polizas','spr_incapacidades_sol_audit','spr_detalle_pago_siniestro','spr_pagos','spr_recuperaciones','spr_tipos_recuperaciones']
df_tmp_tables_plus = ['bodega_plus','snr_dato_basico','snr_causa','snr_origen','rsv_cobertura','rcd_polizas','rsv_dato_reserva','pgs_dato_pago','rsv_tipo_movimiento','snr_dato_tramite']
for row in df_tmp_tables_bodega:
    path_source_plus_bodega =f"s3://alfa-silver-jl/SIPREN/TRANSFORMATION/SIPREN_SILVER_"
    path_source_tierra  = f"s3://alfa-datalake-dev-silver/SIPREN/TRANSFORMATION/SIPREN_SILVER_"
    IncrementalCharge.charge_tmp_tables(path_source_plus_bodega, row, 'Gold')
from commons.StepGoldPlus import StepGoldPlus
StepGoldPlus = StepGoldPlus(glueContext, spark)
df_main_plus = StepGoldPlus.create_component()
df_main_plus.count()
df_plus_tierra = df_main_plus.union(df_main) 
df_plus_tierra.count()
df_all = df_plus_tierra.union(DF_BODEGA)
df_all.count()
#################### ESTA ES
from commons.StepGold import StepGold
from commons.IncrementalCharge import IncrementalCharge
from pyspark.sql.types import IntegerType
IncrementalCharge = IncrementalCharge(glueContext, spark, "-", "-")
StepGold = StepGold(glueContext, spark)
path = f"s3://alfa-datalake-dev-assets/support_phase_files/procces_ola1.xlsx"
sheet = "gold_phase"
df_tables = ExtractHelper.extract_excel_data(path, sheet)
sheet = "gold_tables"
df_tmp_tables = ExtractHelper.extract_excel_data(path, sheet)

for row in df_tmp_tables.collect():
    table = row["Table"].upper()
    if row["Phase"].upper() == "SILVER":
        path_source =f"s3://alfa-datalake-dev-silver/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}_SILVER_"
    else:
        path_source =f"s3://alfa-datalake-dev-gold/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-CARACT_SINIESTROS-"
        print(table)
    IncrementalCharge.charge_tmp_tables(path_source, table, 'Gold')
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from pyspark.sql import functions as F, Window


class StepGold:
    def __init__(self, glue_context, spark):
        self.spark = spark

    def create_component(self):
        """
        This Function to create gold_layer Table.
        """


        decimal_cast = "decimal(19, 0)"
        SIN_COD_CAUSA_AFP = "SIN.cod_causa_siniestro_afp"
        CP_COD_CONCEPTO_PAGO_SINIESTRO = "CP.cod_concepto_pago_siniestro"
        TM_COD_MOVIMIENTO = "TM.cod_movimiento"
        SIN_COD_ORIGEN = "SIN.cod_origen"
        SIN_NID_SINIESTRO ="SIN.nid_siniestro"
        CS_COD_CAUSA_SINIESTRO = "CS.cod_causa_siniestro"
        CO_COD_ORIGEN = "CO.cod_origen"
        SIN_FEC_SINIESTRO = "SIN.fec_siniestro"
        P_FEC_INICIAL_VIGENCIA = "P.fec_inicial_vigencia"
        P_FEC_FINAL_VIGENCIA = "P.fec_final_vigencia"
        DR_VALOR_RESERVA = "DR.vlr_reserva"
        AFIL_CEDULA ="AF.AFIL_CEDULA"
        PS_VLR_PAGO ="PS.vlr_pago"
        SIN_FEC_SOLICITUD_AFP = "SIN.fec_solicitud_afp"
        TM_DESCRIPCION ="TM.descripcion"
        ISA_DIA_181 ="ISA_FIL.dia_181"
        CS_DESCRIPCION = "CS.descripcion"
        UNIDAD_DE_NEGOCIO = "Seguridad social"
        P_NID_POLIZA ="P.nid_poliza"
        CO_NOMBRE = "CO.nombre"
        PA_NID_IDENTIFICACION = "PA.NID_IDENTIFICACION"
        SIN_NID_NUM_IDEN = "SIN.NID_NUMERO_IDENTIFICACION"
        SIN_COD_TIPO_IDEN = "SIN.COD_TIPO_IDENTIFICACION"
        PA_COD_TIPO_IDEN = "PA.COD_TIPO_IDENTIFICACION_AFP"
        ISA_FIL_NID_SINIESTRO = "ISA_FIL.NID_SINIESTRO"
        SIN_NID_SOL = "SIN.NID_SOLICITUD_AFP"
        BP_NID_SOLICITUD_AFP = "BP.NID_SOLICITUD_AFP"
        BP_CIUDAD_SIN = "BP.CIUDAD_SINIESTRO"
        SIN_COD_DEP = "SIN.COD_DEPARTAMENTO"
        AF_FIL_IN = "AF.afil_fecing"
        AF_CLASEI = "AF.AFIL_CLASEI"
        AF_FEC_NAC = "AF.afil_fecnac"
        AF_SEXO = "AF.afil_sexo"
        AF_NUMCUEN = "AF.AFIL_NUMCUE"
        PA_CUENTA_IND = "PA.NID_CUENTA_INDIVIDUAL"
        DESC_CAUSA = "Otras causas"
   

        
        SIN_FEC_INICIO_SOLICITUD_AFP ="SIN.fec_inicio_solicitud_afp"
        DR = self.spark.table("global_temp.spr_detalle_reserva")
        SR = self.spark.table("global_temp.spr_reserva")
        CP = self.spark.table("global_temp.spr_conceptos_pagos_siniestros")
        TM = self.spark.table("global_temp.cgr_movimientos_reserva")
        SIN = self.spark.table("global_temp.spr_siniestros").alias("SIN")
        CS = self.spark.table("global_temp.spr_causas_siniestros")
        CO = self.spark.table("global_temp.cgr_origenes")
        AF = self.spark.table("global_temp.vw_dsdafil_alfa").alias("AF")
        BP = self.spark.table("global_temp.ben_solicitudes_v")
        P  = self.spark.table("global_temp.ppr_polizas")
        ISA= self.spark.table("global_temp.spr_incapacidades_sol_audit").alias("ISA")
        PS = self.spark.table("global_temp.spr_detalle_pago_siniestro")
        PG = self.spark.table("global_temp.spr_pagos")
        PA = self.spark.table("global_temp.ppr_afiliados").alias("PA")
        RP = self.spark.table("global_temp.spr_recuperaciones")
        TR = self.spark.table("global_temp.spr_tipos_recuperaciones")
        DT = self.spark.table("global_temp.spr_detalle_recuperaciones").alias("DT")

        ciudad_siniestro = BP.select(F.col("SOLICITUD").alias("NID_SOLICITUD_AFP"),
                                    F.col("CIUDAD_SINIESTRO")).distinct()




        isa_max_df = ISA.groupBy("NID_SINIESTRO").agg(
            F.max("NID_SOLICITUD_AUDIT").alias("MAX_AUDIT_ID")
        ).withColumnRenamed("NID_SINIESTRO","MAX_NID_SINIESTRO")

        isa_filtrada = ISA.join(
            isa_max_df.alias("max"),
            (F.col("ISA.NID_SINIESTRO") == F.col("max.MAX_NID_SINIESTRO")) &
            (F.col("ISA.NID_SOLICITUD_AUDIT") == F.col("max.MAX_AUDIT_ID"))
        )



        #  “Reserva”
        df_reserva = (
            DR.alias("DR")
            .join(SR.alias("SR"), F.col("DR.nid_reserva") == F.col("SR.nid_reserva"))
            .join(CP.alias("CP"), F.col("DR.cod_concepto_pago_siniestro") == F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO))
            .join(TM.alias("TM"), F.col("DR.cod_movimiento") == F.col(TM_COD_MOVIMIENTO))
            .join(SIN.alias("SIN"), F.col("DR.nid_siniestro") == F.col(SIN_NID_SINIESTRO))
            .join(CS.alias("CS"), F.col(SIN_COD_CAUSA_AFP) == F.col(CS_COD_CAUSA_SINIESTRO),"left")
            .join(CO.alias("CO"), F.col(SIN_COD_ORIGEN) == F.col(CO_COD_ORIGEN))
            .join(PA, (F.col(SIN_NID_NUM_IDEN) == F.col(PA_NID_IDENTIFICACION)) &
                    (F.col(SIN_COD_TIPO_IDEN) == F.col(PA_COD_TIPO_IDEN)),"left")
            .join(isa_filtrada.alias("ISA_FIL"), F.col(SIN_NID_SINIESTRO) == F.col(ISA_FIL_NID_SINIESTRO), "left")
            .join(AF, (F.col(SIN_NID_NUM_IDEN) == F.col(AFIL_CEDULA)) &
                    (F.col(SIN_COD_TIPO_IDEN) == F.col(AF_CLASEI)) &
                    (F.col(AF_NUMCUEN) == F.col(PA_CUENTA_IND)),"left")
            .join(
                P.alias("P"),
                ( F.col(SIN_FEC_SINIESTRO) >= F.col(P_FEC_INICIAL_VIGENCIA)) &
                ( F.col(SIN_FEC_SINIESTRO)<= F.col(P_FEC_FINAL_VIGENCIA)),"left"
            )
 
            .join(ciudad_siniestro.alias("BP"), F.col(SIN_NID_SOL) == F.col(BP_NID_SOLICITUD_AFP),"left")      
            .filter(F.col(DR_VALOR_RESERVA) != 0)
            .select(
                F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO).cast("string").alias("id_categoria_fk"),
                F.col("CP.descripcion").alias("categoria"),
                F.lit("Reserva").alias("tipo"),
                F.col("DR.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
                F.col(TM_DESCRIPCION).alias("tipo_transaccion"),
                F.col("DR.nid_siniestro").cast("string").alias("numero_siniestro"),
                F.abs(F.col(DR_VALOR_RESERVA) ).alias("valor_transaccion"),
                F.col(DR_VALOR_RESERVA).alias("valor_transaccion_ajustado"),
                F.col("SR.fec_generado").alias("fecha_transaccion"),
                F.when(
                    F.col(CO_COD_ORIGEN) == '2' ,
                    F.col(ISA_DIA_181)
                ).otherwise(
                    F.col(SIN_FEC_SINIESTRO)
                ).alias("fecha_ocurrencia"),
                F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
                F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),
                F.col(CO_COD_ORIGEN).cast(decimal_cast).alias("cod_origen"),
                F.col(CO_NOMBRE).alias("origen"),
                F.coalesce(F.col(SIN_FEC_INICIO_SOLICITUD_AFP), F.col(SIN_FEC_SOLICITUD_AFP)).alias("fecha_aviso"),
                F.coalesce(F.col(P_NID_POLIZA), F.lit(999)).alias("poliza"),
                F.lit("2").cast("string").alias("id_fuente_fk"),
                F.lit("50").cast("string").alias("compania"),
                F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
                F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
                F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.when(
                     F.col(BP_CIUDAD_SIN).isNull(),
                     F.col(SIN_COD_DEP).cast("int")
                ).otherwise(
                     F.col(SIN_COD_DEP).cast("int")
                ).alias("lugar_ocurrencia"),
                F.col(AF_FIL_IN).alias("fecha_inicio_vigencia"),
                F.col(AFIL_CEDULA).alias("cedula"),
                F.col(AF_FEC_NAC).alias("fecha_nacimiento"),
                F.col(AF_SEXO).alias("genero")                
            )
        )


        #  “Pago”
        df_pago = (
            PS.alias("PS")
            .join(PG.alias("PG"), F.col("PG.nid_pago") == F.col("PS.nid_pago"))
            .join(CP.alias("CP"), F.col("PS.cod_concepto_pago_siniestro") == F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO))
            .join(TM.alias("TM"), F.col("PS.cod_movimiento") == F.col(TM_COD_MOVIMIENTO))
            .join(SIN.alias("SIN"), F.col("PS.nid_siniestro") == F.col(SIN_NID_SINIESTRO))
            .join(CS.alias("CS"), F.col(SIN_COD_CAUSA_AFP)  == F.col(CS_COD_CAUSA_SINIESTRO),"left")
            .join(CO.alias("CO"), F.col(SIN_COD_ORIGEN)               == F.col(CO_COD_ORIGEN))
            .join(PA, (F.col(SIN_NID_NUM_IDEN) == F.col(PA_NID_IDENTIFICACION)) &
                    (F.col(SIN_COD_TIPO_IDEN) == F.col(PA_COD_TIPO_IDEN)),"left")
            .join(isa_filtrada.alias("ISA_FIL"), F.col(SIN_NID_SINIESTRO) == F.col(ISA_FIL_NID_SINIESTRO), "left")
            .join(AF, (F.col(SIN_NID_NUM_IDEN) == F.col(AFIL_CEDULA)) &
                    (F.col(SIN_COD_TIPO_IDEN) == F.col(AF_CLASEI)) &
                    (F.col(AF_NUMCUEN) == F.col(PA_CUENTA_IND)),"left")
            .join(
                P.alias("P"),
                ( F.col(SIN_FEC_SINIESTRO) >= F.col(P_FEC_INICIAL_VIGENCIA)) &
                ( F.col(SIN_FEC_SINIESTRO)<= F.col(P_FEC_FINAL_VIGENCIA)),"left"
            )
 
            .join(ciudad_siniestro.alias("BP"), F.col(SIN_NID_SOL) == F.col(BP_NID_SOLICITUD_AFP),"left")
            .filter(F.col(PS_VLR_PAGO) != 0)
            .select(
                F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO).cast("string").alias("id_categoria_fk"),
                F.col("CP.descripcion").alias("categoria"),
                F.lit("Pago").alias("tipo"),
                F.col("PS.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
                F.col(TM_DESCRIPCION).alias("tipo_transaccion"),
                F.col("PS.nid_siniestro").cast("string").alias("numero_siniestro"),
                F.abs(F.col(PS_VLR_PAGO)).alias("valor_transaccion"),
                F.col(PS_VLR_PAGO).alias("valor_transaccion_ajustado"),
                F.col("PG.fec_generado").alias("fecha_transaccion"),
                F.when(
                    F.col(CO_COD_ORIGEN) == '2' ,
                    F.col(ISA_DIA_181)
                ).otherwise(
                    F.col(SIN_FEC_SINIESTRO)
                ).alias("fecha_ocurrencia"),
                F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
                F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),
                F.col(CO_COD_ORIGEN).cast(decimal_cast).alias("cod_origen"),
                F.col(CO_NOMBRE).alias("origen"),
                F.coalesce(F.col(SIN_FEC_INICIO_SOLICITUD_AFP), F.col(SIN_FEC_SOLICITUD_AFP)).alias("fecha_aviso"),
                F.coalesce(F.col(P_NID_POLIZA), F.lit(999)).alias("poliza"),
                F.lit("2").cast("string").alias("id_fuente_fk"),
                F.lit("50").cast("string").alias("compania"),
                F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
                F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
                F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.when(
                     F.col(BP_CIUDAD_SIN).isNull(),
                     F.col(SIN_COD_DEP).cast("int")
                ).otherwise(
                     F.col(SIN_COD_DEP).cast("int")
                ).alias("lugar_ocurrencia"),
                F.col(AF_FIL_IN).alias("fecha_inicio_vigencia"),
                F.col(AFIL_CEDULA).alias("cedula"),
                F.col(AF_FEC_NAC).alias("fecha_nacimiento"),
                F.col(AF_SEXO).alias("genero")                
            



            )
        )


        #  “Recuperaciones”
        df_recup = (
            DT.alias("DT")
            .join(RP.alias("RP"),F.col("RP.nid_recuperacion") == F.col("DT.nid_recuperacion"),"left")
            .join(TR.alias("TR"), F.col("RP.cod_tipo_recuperacion") == F.col("TR.cod_tipo_recuperacion"))
            .join(TM.alias("TM"), F.col("RP.cod_movimiento") == F.col(TM_COD_MOVIMIENTO))
            .join(SIN.alias("SIN"), F.col("RP.nid_siniestro")    == F.col(SIN_NID_SINIESTRO))
            .join(CS.alias("CS"), F.col(SIN_COD_CAUSA_AFP) == F.col(CS_COD_CAUSA_SINIESTRO),"left")
            .join(CO.alias("CO"), F.col(SIN_COD_ORIGEN)               == F.col(CO_COD_ORIGEN))
            .join(PA, (F.col(SIN_NID_NUM_IDEN) == F.col(PA_NID_IDENTIFICACION)) &
                    (F.col(SIN_COD_TIPO_IDEN) == F.col(PA_COD_TIPO_IDEN)),"left")
            .join(isa_filtrada.alias("ISA_FIL"), F.col(SIN_NID_SINIESTRO) == F.col(ISA_FIL_NID_SINIESTRO), "left")
            .join(AF, (F.col(SIN_NID_NUM_IDEN) == F.col(AFIL_CEDULA)) &
                    (F.col(SIN_COD_TIPO_IDEN) == F.col(AF_CLASEI)) &
                    (F.col(AF_NUMCUEN) == F.col(PA_CUENTA_IND)),"left")
            .join(
                P.alias("P"),
                ( F.col(SIN_FEC_SINIESTRO) >= F.col(P_FEC_INICIAL_VIGENCIA)) &
                ( F.col(SIN_FEC_SINIESTRO)<= F.col(P_FEC_FINAL_VIGENCIA)),"left"
            )
 
            .join(ciudad_siniestro.alias("BP"), F.col(SIN_NID_SOL) == F.col(BP_NID_SOLICITUD_AFP),"left")
            .filter(F.col("RP.nid_recibo_caja_point") != 0)
            .select(
                F.col("TR.cod_tipo_recuperacion").cast("string").alias("id_categoria_fk"),
                F.col("TR.nombre").alias("categoria"),
                F.lit("Recuperaciones").alias("tipo"),
                F.col("RP.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
                F.col(TM_DESCRIPCION).alias("tipo_transaccion"),
                F.col("RP.nid_siniestro").cast("string").alias("numero_siniestro"),
                F.abs(F.col("DT.VLR_CONCEPTO")).alias("valor_transaccion"),
                F.col("DT.VLR_CONCEPTO").alias("valor_transaccion_ajustado"),
                F.col("RP.fec_creacion_registro").alias("fecha_transaccion"),
                F.when(
                    F.col(CO_COD_ORIGEN) == '2' ,
                    F.col(ISA_DIA_181)
                ).otherwise(
                    F.col(SIN_FEC_SINIESTRO)
                ).alias("fecha_ocurrencia"), 
                F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
                F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),
                F.col(CO_COD_ORIGEN).cast(decimal_cast).alias("cod_origen"),
                F.col(CO_NOMBRE).alias("origen"),
                F.coalesce(F.col(SIN_FEC_INICIO_SOLICITUD_AFP), F.col(SIN_FEC_SOLICITUD_AFP)).alias("fecha_aviso"),
                F.coalesce(F.col(P_NID_POLIZA), F.lit(999)).alias("poliza"),
                F.lit("2").cast("string").alias("id_fuente_fk"),
                F.lit("50").cast("string").alias("compania"),
                F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
                F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
                F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.when(
                     F.col(BP_CIUDAD_SIN).isNull(),
                     F.col(SIN_COD_DEP).cast("int")
                ).otherwise(
                     F.col(SIN_COD_DEP).cast("int")
                ).alias("lugar_ocurrencia"),
                F.col(AF_FIL_IN).alias("fecha_inicio_vigencia"),
                F.col(AFIL_CEDULA).alias("cedula"),
                F.col(AF_FEC_NAC).alias("fecha_nacimiento"),
                F.col(AF_SEXO).alias("genero")                
            
            )
        )



        df_final = df_reserva.unionByName(df_pago).unionByName(df_recup)


        return df_final
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from pyspark.sql import functions as F



class StepGoldBodega:
    def __init__(self, glue_context, spark):
        self.spark = spark

    def create_component(self):
        """
        This Function to create gold_layer Table.
        """

        B_NID_SINIESTRO = 'B.NID_SINIESTRO'


        
        DF_BODEGA = self.spark.sql("select * from global_temp.bodega_plus")
        DF_DATO_BASICO = self.spark.sql("select * from global_temp.snr_dato_basico")
        DF_CAUSAS = self.spark.sql("select * from global_temp.snr_causa")
        DF_ORIGEN = self.spark.sql("select * from global_temp.snr_origen")
        DF_POLIZAS = self.spark.sql("select * from global_temp.rcd_polizas")
        DF_CU_PERSONAS = self.spark.sql("select * from global_temp.CU_PERSONA_CLIENTE")
        DF_VW_CONSULTA = self.spark.sql("select * from global_temp.VW_CONSULTA_CLIENTE")
        DF_VW_LOCALIZACIONES = self.spark.sql("select * from global_temp.VW_LOCALIZACIONES")

       
        DF_RESULT = DF_BODEGA.alias('B').join(
            DF_DATO_BASICO.alias('D'), F.col(B_NID_SINIESTRO) == F.col('D.ID_SINIESTRO'), 'left'
            ).join(
            DF_CAUSAS.alias('c'), F.col('D.COD_CAUSA_SINIESTRO') == F.col('c.ID_CAUSAL'), 'left'
            ).join(
            DF_ORIGEN.alias('o'), F.col('D.COD_ORIGEN_SINIESTRO') == F.col('o.COD_ORIGEN_SINIESTRO'), 'left'
            ).join(
            DF_POLIZAS.alias('p'),  F.coalesce(F.col('B.fecha_siniestro'), F.to_date(F.concat_ws('', F.col('B.periodo_point'), F.lit('01')), 'yyyyMMdd')).between(F.col('p.FEC_INICIAL_VIGENCIA'),F.col('p.FEC_FINAL_VIGENCIA'))
            ).join(
            DF_CU_PERSONAS.alias('pcl'), F.col("pcl.NUM_PERSONA") == F.col("D.NUM_PERSONA")
            ).join(
            DF_VW_CONSULTA.alias('af'), F.col("pcl.NUM_IDENTIFICACION") == F.col("af.NID_NUMERO_IDENTIFICACION")
            ).join(
            DF_VW_LOCALIZACIONES.alias('af2'), F.col("PCL.NUM_IDENTIFICACION") == F.col("af2.NID_NUMERO_IDENTIFICACION")
            )
            
        df_facturas = DF_RESULT.filter(
            (F.col(B_NID_SINIESTRO).cast(IntegerType()) >= 20200000000) & (F.col("b.cod_movimiento") == "Recuperacion")
        ).selectExpr(
            "'3' as id_categoria_fk",
            "b.concepto_pago AS categoria",
            "'Facturas' AS tipo",
            "CASE b.cod_movimiento  WHEN 'Pago' THEN 7 ELSE 7 END AS id_tipo_transaccion_fk",
            "CASE b.cod_movimiento  WHEN 'Pago' THEN 'Recobros' ELSE 'Recobros' END AS tipo_transaccion",
            "b.NID_SINIESTRO AS numero_siniestro",
            "abs(cast(b.VLR_pago as double)) AS valor_transaccion",
            "b.VLR_pago AS valor_transaccion_ajustado",
            "cast(to_date(b.periodo_point || '01' ,'%yyyy%MM%dd') as date)  AS fecha_transaccion",    
            "COALESCE(cast(b.fecha_siniestro as date), cast(to_date(b.periodo_point || '01' ,'%yyyy%MM%d') as date))  AS fecha_ocurrencia",
            "c.ID_CAUSAL as cod_causa",    
            "COALESCE(c.NOMBRE, 'Otras causas') AS causa",
            "o.cod_origen_siniestro as cod_origen", 
            "COALESCE(o.NOMBRE,'Incapacidad total y permanente') AS origen",
            "cast(to_date(b.periodo_point || '01' ,'%yyyy%MM%dd') as date)   AS fecha_aviso",
            "COALESCE(p.NID_POLIZA ,999) AS poliza",
            "'3' as id_fuente_fk",
            "'50' AS compania",
            "41 AS id_producto_fk",
            "31 AS id_ramo_fk",
            "'Seguridad social' AS unidad_de_negocio",   
            "CONCAT('PRV-',LPAD(COALESCE(CAST(p.NID_POLIZA AS VARCHAR(10)),'999'),7,'0'),'-00') as poliza_lider",
            "AF2.COD_DEPARTAMENTO || AF2.COD_CIUDAD  AS lugar_ocurrencia", 
            "AF.FEC_INGRESO_AFP AS fecha_inicio_vigencia",
            "af.NID_NUMERO_IDENTIFICACION AS cedula",
            "AF.FEC_NACIMIENTO  AS fecha_nacimiento",      
            "AF.GENERO  AS genero"



        )

    
        df_pagos = DF_RESULT.filter(
            (F.col(B_NID_SINIESTRO)== "99999999")
        ).selectExpr(
            "'2'  AS id_categoria_fk",
            "b.concepto_pago    AS categoria",
            "'Recduperacion' AS tipo",
            "CASE b.cod_movimiento  WHEN 'Pago' THEN  5   ELSE 5 END AS id_tipo_transaccion_fk",
            "CASE b.cod_movimiento  WHEN 'Pago' THEN 'Pago'  ELSE 'Pago'  END AS tipo_transaccion",    
            "b.NID_SINIESTRO AS numero_siniestro",
            "abs(cast(b.VLR_pago as double)) AS valor_transaccion",
            "b.VLR_pago AS valor_transaccion_ajustado",
            "cast(to_date(b.periodo_point || '01' ,'%yyyy%MM%dd') as date)  AS fecha_transaccion",
            "COALESCE(cast(b.fecha_siniestro as date), cast(to_date(b.periodo_point || '01' ,'%yyyy%MM%dd') as date))  AS fecha_ocurrencia",    
            "'384'	as cod_causa",
            "COALESCE(c.NOMBRE,'Otras causas' ) AS causa",
            "'742' as cod_origen",    
            "COALESCE(o.NOMBRE,'Incapacidad total y permanente' ) AS origen",
            "cast(to_date(b.periodo_point || '01' ,'%yyyy%MM%dd') as date)   AS fecha_aviso",
            "COALESCE(p.NID_POLIZA ,999) AS poliza",
            "'3' as id_fuente_fk",
            "'50' AS compania",
            "41 AS id_producto_fk",    
            "31 AS id_ramo_fk",
            "'Seguridad social' AS unidad_de_negocio",
            "CONCAT('PRV-',LPAD(COALESCE(CAST(p.NID_POLIZA AS VARCHAR(10)),'999'),7,'0'),'-00') as poliza_lider",
            "AF2.COD_DEPARTAMENTO || AF2.COD_CIUDAD  AS lugar_ocurrencia", 
            "AF.FEC_INGRESO_AFP AS fecha_inicio_vigencia",
            "af.NID_NUMERO_IDENTIFICACION AS cedula",
            "AF.FEC_NACIMIENTO  AS fecha_nacimiento",      
            "AF.GENERO  AS genero"

            







        
        )

        df_final = df_facturas.union(df_pagos)


        return df_final
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

class StepGoldPlus:
    def __init__(self, glue_context, spark):
        self.spark = spark

    def create_component(self):
        


        P_NID_POLIZA ="P.nid_poliza"
        SDT_ID_SINIESTRO = "SDT.ID_SINIESTRO"
        SDB_FECHA_SINIESTRO = "SDB.FECHA_SINIESTRO"
        CP_COD_COBERTURA=  "CP.COD_COBERTURA"
        CS_ID_CAUSAL = "CS.ID_CAUSAL"
        DP_VLR_PAGO = "DP.VLR_PAGO"
        CO_COD_ORIGEN_SINIESTRO = "CO.COD_ORIGEN_SINIESTRO"
        CP_NOMBRE = "CP.NOMBRE"
        PCL_NUM_IDEN  = "PCL.NUM_IDENTIFICACION"


        DR = self.spark.table("global_temp.rsv_dato_reserva")
        DP = self.spark.table("global_temp.pgs_dato_pago")
        TM = self.spark.table("global_temp.rsv_tipo_movimiento")
        CP = self.spark.table("global_temp.rsv_cobertura")
        SDT = self.spark.table("global_temp.snr_dato_tramite")
        SDB = self.spark.table("global_temp.snr_dato_basico")
        CS = self.spark.table("global_temp.snr_causa")
        CO = self.spark.table("global_temp.snr_origen")
        P = self.spark.table("global_temp.rcd_polizas")
        PCL = self.spark.table("global_temp.cu_persona_cliente")
        AF = self.spark.table("global_temp.vw_consulta_cliente")
        AF2 = self.spark.table("global_temp.vw_localizaciones")

        
        df_reserva = DR.alias("DR") \
            .join(TM.alias("TM"), F.col("TM.COD_TIPO_MOVIMIENTO").cast("string") == F.col("DR.COD_TIPO_MOVIMIENTO").cast("string")) \
            .join(CP.alias("CP"), F.col("DR.COD_COBERTURA").cast("string") == F.col(CP_COD_COBERTURA).cast("string")) \
            .join(SDT.alias("SDT"), F.col("DR.ID_TRAMITE") == F.col("SDT.ID_TRAMITE")) \
            .join(SDB.alias("SDB"), F.col(SDT_ID_SINIESTRO) == F.col("SDB.ID_SINIESTRO")) \
            .join(CS.alias("CS"), F.col("SDB.COD_CAUSA_SINIESTRO").cast("string") == F.col(CS_ID_CAUSAL).cast("string")) \
            .join(CO.alias("CO"), F.col("SDB.COD_ORIGEN_SINIESTRO") == F.col(CO_COD_ORIGEN_SINIESTRO)) \
            .join(P.alias("P"), F.col(SDB_FECHA_SINIESTRO).between(F.col("P.FEC_INICIAL_VIGENCIA"), F.col("P.FEC_FINAL_VIGENCIA"))) \
            .join(PCL.alias("PCL"), F.col("PCL.NUM_PERSONA").cast("string") == F.col("SDB.NUM_PERSONA").cast("string")) \
            .join(AF.alias("AF"), F.col(PCL_NUM_IDEN).cast("string") == F.col("AF.NID_NUMERO_IDENTIFICACION").cast("string")) \
            .join(AF2.alias("AF2"), F.col(PCL_NUM_IDEN).cast("string") == F.col("AF2.NID_NUMERO_IDENTIFICACION").cast("string")) \
            .select(
                F.col(CP_COD_COBERTURA).alias("id_categoria_fk"),
                F.col(CP_NOMBRE).alias("categoria"),
                F.lit("Reserva").alias("tipo"),
                F.col("DR.COD_TIPO_MOVIMIENTO").alias("id_tipo_transaccion_fk"),
                F.col("TM.NOMBRE").alias("tipo_transaccion"),
                F.col(SDT_ID_SINIESTRO).alias("numero_siniestro"),
                F.abs(F.col("DR.VLR_MOVIMIENTO")).alias("valor_transaccion"),
                F.col("DR.VLR_MOVIMIENTO").alias("valor_transaccion_ajustado"),
                F.col("DR.FECHA_MOVIMIENTO").alias("fecha_transaccion"),
                F.col(SDB_FECHA_SINIESTRO).alias("fecha_ocurrencia"),
                F.col(CS_ID_CAUSAL).alias("cod_causa"),
                F.col("CS.NOMBRE").alias("causa"),
                F.when(F.col(CP_COD_COBERTURA) == '3', F.col(CP_COD_COBERTURA)).otherwise(F.col(CO_COD_ORIGEN_SINIESTRO)).alias("cod_origen"),
                F.when(F.col(CP_COD_COBERTURA) == '3', F.col(CP_NOMBRE)).otherwise(F.col("CO.NOMBRE")).alias("origen"),
                F.coalesce(F.col("SDT.FEC_ACT_SOLICITUD_AFP"), F.col("SDT.FEC_SOLICITUD_AFP")).alias("fecha_aviso"),
                F.coalesce(F.col("P.NID_POLIZA"), F.lit(999)).alias("poliza"),
                F.lit("3").cast("string").alias("id_fuente_fk"),
                F.lit("50").alias("compania"),
                F.lit(41).alias("id_producto_fk"),
                F.lit(31).alias("id_ramo_fk"),
                F.lit("Seguridad social").alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.concat(F.col("AF2.COD_DEPARTAMENTO"), F.col("AF2.COD_CIUDAD")).alias("lugar_ocurrencia"), 
                F.col("AF.FEC_INGRESO_AFP").alias("fecha_inicio_vigencia"),
                F.col("af.NID_NUMERO_IDENTIFICACION").alias("cedula"),
                F.col("AF.FEC_NACIMIENTO").alias("fecha_nacimiento"),
                F.col("AF.GENERO").alias("genero")
                
            )

        
        df_pago = DP.alias("DP") \
            .join(TM.alias("TM"), F.col("TM.COD_TIPO_MOVIMIENTO").cast("string") == F.col("DP.COD_TIPO_MOVIMIENTO").cast("string")) \
            .join(CP.alias("CP"), F.col("DP.COD_COBERTURA").cast("string") == F.col(CP_COD_COBERTURA).cast("string")) \
            .join(SDT.alias("SDT"), F.col("DP.ID_TRAMITE") == F.col("SDT.ID_TRAMITE")) \
            .join(SDB.alias("SDB"), F.col(SDT_ID_SINIESTRO) == F.col("SDB.ID_SINIESTRO")) \
            .join(CS.alias("CS"), F.col("SDB.COD_CAUSA_SINIESTRO").cast("string") == F.col(CS_ID_CAUSAL).cast("string")) \
            .join(CO.alias("CO"), F.col("SDB.COD_ORIGEN_SINIESTRO") == F.col(CO_COD_ORIGEN_SINIESTRO)) \
            .join(P.alias("P"), F.col(SDB_FECHA_SINIESTRO).between(F.col("P.FEC_INICIAL_VIGENCIA"), F.col("P.FEC_FINAL_VIGENCIA"))) \
            .join(PCL.alias("PCL"), F.col("PCL.NUM_PERSONA").cast("string") == F.col("SDB.NUM_PERSONA").cast("string")) \
            .join(AF.alias("AF"), F.col(PCL_NUM_IDEN).cast("string") == F.col("AF.NID_NUMERO_IDENTIFICACION").cast("string")) \
            .join(AF2.alias("AF2"), F.col(PCL_NUM_IDEN).cast("string") == F.col("AF2.NID_NUMERO_IDENTIFICACION").cast("string")) \
            .where(F.col(DP_VLR_PAGO).cast("double") != 0) \
            .select(
                F.col(CP_COD_COBERTURA).alias("id_categoria_fk"),
                F.col(CP_NOMBRE).alias("categoria"),
                F.lit("Pago").alias("tipo"),
                F.col("DP.COD_TIPO_MOVIMIENTO").alias("id_tipo_transaccion_fk"),
                F.col("TM.NOMBRE").alias("tipo_transaccion"),
                F.col(SDT_ID_SINIESTRO).alias("numero_siniestro"),
                F.abs(F.col(DP_VLR_PAGO).cast("double")).alias("valor_transaccion"),
                F.col(DP_VLR_PAGO).cast("double").alias("valor_transaccion_ajustado"),
                F.col("DP.FECHA_PAGO").alias("fecha_transaccion"),
                F.col(SDB_FECHA_SINIESTRO).alias("fecha_ocurrencia"),
                F.col(CS_ID_CAUSAL).alias("cod_causa"),
                F.col("CS.NOMBRE").alias("causa"),
		        F.when(F.col(CP_COD_COBERTURA) == '3', F.col(CP_COD_COBERTURA)).otherwise(F.col(CO_COD_ORIGEN_SINIESTRO)).alias("cod_origen"),
                F.when(F.col(CP_COD_COBERTURA) == '3', F.col(CP_NOMBRE)).otherwise(F.col("CO.NOMBRE")).alias("origen"),
                F.coalesce(F.col("SDT.FEC_ACT_SOLICITUD_AFP"), F.col("SDT.FEC_SOLICITUD_AFP")).alias("fecha_aviso"),
                F.coalesce(F.col("P.NID_POLIZA"), F.lit(999)).alias("poliza"),
                F.lit("3").cast("string").alias("id_fuente_fk"),
                F.lit("50").alias("compania"),
                F.lit(41).alias("id_producto_fk"),
                F.lit(31).alias("id_ramo_fk"),
                F.lit("Seguridad social").alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.concat(F.col("AF2.COD_DEPARTAMENTO"), F.col("AF2.COD_CIUDAD")).alias("lugar_ocurrencia"), 
                F.col("AF.FEC_INGRESO_AFP").alias("fecha_inicio_vigencia"),
                F.col("af.NID_NUMERO_IDENTIFICACION").alias("cedula"),
                F.col("AF.FEC_NACIMIENTO").alias("fecha_nacimiento"),
                F.col("AF.GENERO").alias("genero")
 
            )

        df_gold_plus = df_reserva.unionByName(df_pago)

        return df_gold_plus

StepGold = StepGold(glueContext, spark)
StepGoldPlus = StepGoldPlus(glueContext, spark)
StepGoldBodega = StepGoldBodega(glueContext, spark)
df_tierra = StepGold.create_component()
df_plus = StepGoldPlus.create_component()
df_bodega = StepGoldBodega.create_component()
df_main = df_tierra.union(df_plus).union(df_bodega)
decimal_cast = "decimal(19, 0)"
df_poliza_plus = df_main.select(
    F.col("poliza").cast("string").alias("id_poliza_pk"),
    F.col("id_ramo_fk").cast(decimal_cast),
    F.col("compania"),
    F.col("id_producto_fk").cast(decimal_cast),
    F.col("id_fuente_fk"),
    F.col("unidad_de_negocio"),
    F.col("poliza_lider")
)  

from pyspark.sql.functions import  col,count,countDistinct
from pyspark.context import SparkContext
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import (
    DecimalType, StringType, IntegerType, BooleanType,
    FloatType, DoubleType, StructType, StructField
)
from pyspark.sql.window import Window

window_spec = Window.orderBy(F.monotonically_increasing_id())
df_poliza_plus = df_poliza_plus.distinct()
df_ola1_processed = df_main.withColumn(
    "fecha_desembolso_credito", 
    F.lit(None).cast("date")  # To be defined later
).select(
    F.col("fecha_desembolso_credito"),
    F.col("numero_siniestro").cast("string").alias("id_siniestro_fk"),
    F.col("poliza").cast("string").alias("id_poliza_fk"),
    F.col("fecha_inicio_vigencia").cast("date"),
    F.col("id_fuente_fk").cast("string")
).distinct()

df_ola1_processed =  df_ola1_processed.withColumn(
    "id_poliza_detallada_pk",
    (F.row_number().over(window_spec) + 1000000000).cast("int")
)
df_nul = df_ola1_processed.filter(col("fecha_inicio_vigencia").isNotNull())
sin_pol = df_nul.groupBy("id_poliza_fk") \
    .agg(countDistinct("id_siniestro_fk").alias("cantidad")) \
    .filter(col("cantidad") > 1) 



sin_pol.show(2)
duplicados = df_nul.groupBy("id_poliza_fk") \
    .agg(count("*").alias("cantidad")) \
    .filter(col("cantidad") > 1) 



resultado = df_nul.join(duplicados.select("id_poliza_fk"), on ="id_poliza_fk")
resultado.
resultado.filter(
df_nul.filter(col("id_poliza_fk")=='8').show()
df_poliza_plus.filter(col("id_poliza_pk")=='9').show()
duplicados.show(4)
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from pyspark.sql import functions as F, Window


class StepGold:
    def __init__(self, glue_context, spark):
        self.spark = spark

    def create_component(self):
        """
        This Function to create gold_layer Table.
        """


        decimal_cast = "decimal(19, 0)"
        SIN_COD_CAUSA_AFP = "SIN.cod_causa_siniestro_afp"
        CP_COD_CONCEPTO_PAGO_SINIESTRO = "CP.cod_concepto_pago_siniestro"
        TM_COD_MOVIMIENTO = "TM.cod_movimiento"
        SIN_COD_ORIGEN = "SIN.cod_origen"
        SIN_NID_SINIESTRO ="SIN.nid_siniestro"
        CS_COD_CAUSA_SINIESTRO = "CS.cod_causa_siniestro"
        CO_COD_ORIGEN = "CO.cod_origen"
        SIN_FEC_SINIESTRO = "SIN.fec_siniestro"
        P_FEC_INICIAL_VIGENCIA = "P.fec_inicial_vigencia"
        P_FEC_FINAL_VIGENCIA = "P.fec_final_vigencia"
        DR_VALOR_RESERVA = "DR.vlr_reserva"
        AFIL_CEDULA ="AF.AFIL_CEDULA"
        PS_VLR_PAGO ="PS.vlr_pago"
        SIN_FEC_SOLICITUD_AFP = "SIN.fec_solicitud_afp"
        TM_DESCRIPCION ="TM.descripcion"
        ISA_DIA_181 ="ISA_FIL.dia_181"
        CS_DESCRIPCION = "CS.descripcion"
        UNIDAD_DE_NEGOCIO = "Seguridad social"
        P_NID_POLIZA ="P.nid_poliza"
        CO_NOMBRE = "CO.nombre"
        PA_NID_IDENTIFICACION = "PA.NID_IDENTIFICACION"
        SIN_NID_NUM_IDEN = "SIN.NID_NUMERO_IDENTIFICACION"
        SIN_COD_TIPO_IDEN = "SIN.COD_TIPO_IDENTIFICACION"
        PA_COD_TIPO_IDEN = "PA.COD_TIPO_IDENTIFICACION_AFP"
        ISA_FIL_NID_SINIESTRO = "ISA_FIL.NID_SINIESTRO"
        SIN_NID_SOL = "SIN.NID_SOLICITUD_AFP"
        BP_NID_SOLICITUD_AFP = "BP.NID_SOLICITUD_AFP"
        BP_CIUDAD_SIN = "BP.CIUDAD_SINIESTRO"
        SIN_COD_DEP = "SIN.COD_DEPARTAMENTO"
        AF_FIL_IN = "AF.afil_fecing"
        AF_CLASEI = "AF.AFIL_CLASEI"
        AF_FEC_NAC = "AF.afil_fecnac"
        AF_SEXO = "AF.afil_sexo"
        AF_NUMCUEN = "AF.AFIL_NUMCUE"
        PA_CUENTA_IND = "PA.NID_CUENTA_INDIVIDUAL"
        DESC_CAUSA = "Otras causas"
   

        
        SIN_FEC_INICIO_SOLICITUD_AFP ="SIN.fec_inicio_solicitud_afp"
        DR = self.spark.table("global_temp.spr_detalle_reserva")
        PN = self.spark.table("global_temp.acu_personas_naturales").alias("PN")
        SR = self.spark.table("global_temp.spr_reserva")
        CP = self.spark.table("global_temp.spr_conceptos_pagos_siniestros")
        TM = self.spark.table("global_temp.cgr_movimientos_reserva")
        SIN = self.spark.table("global_temp.spr_siniestros").alias("SIN")
        CS = self.spark.table("global_temp.spr_causas_siniestros")
        CO = self.spark.table("global_temp.cgr_origenes").alias("CO")
        AF = self.spark.table("global_temp.vw_dsdafil_alfa").alias("AF")
        BP = self.spark.table("global_temp.ben_solicitudes_v")
        P  = self.spark.table("global_temp.ppr_polizas")
        ISA= self.spark.table("global_temp.spr_incapacidades_sol_audit").alias("ISA")
        PS = self.spark.table("global_temp.spr_detalle_pago_siniestro")
        PG = self.spark.table("global_temp.spr_pagos")
        PA = self.spark.table("global_temp.ppr_afiliados").alias("PA")
        RP = self.spark.table("global_temp.spr_recuperaciones").alias("RP")
        TR = self.spark.table("global_temp.spr_tipos_recuperaciones")
        DT = self.spark.table("global_temp.spr_detalle_recuperaciones").alias("DT")
        H2 = spark.table("global_temp.Spr_siniestros_origenes_hist").alias("H2")



        window_origen = Window.partitionBy("H2.NID_SINIESTRO").orderBy(F.col("H2.NID_SINIESTRO_ORIGEN_HIST").desc())

        OSF = (
            H2.join(CO, F.col("H2.COD_ORIGEN_SINIESTRO_NEW") == F.col("CO.COD_ORIGEN"), "left")
            .withColumn("RN", F.row_number().over(window_origen))
            .filter(F.col("RN") == 1)
            .select(
                F.col("H2.NID_SINIESTRO"),
                F.col("CO.NOMBRE").alias("nombre_origen_final")
            )
            .alias("OSF")
        )




        diff_expr = F.abs(
            F.expr("int(date_format(H2.FEC_MODIFICACION, 'yyyyMM'))") - F.col("RP.PERIODO_POINT").cast("int")
        )

      
        window_spec = Window.partitionBy("H2.NID_SINIESTRO").orderBy(diff_expr)


        SH = (
            H2.join(RP, F.col("H2.NID_SINIESTRO") == F.col("RP.NID_SINIESTRO"), "inner")
            .withColumn("RN", F.row_number().over(window_spec))
            .filter(F.col("RN") == 1)
            .select(
                F.col("H2.NID_SINIESTRO"),
                F.col("H2.COD_ORIGEN_SINIESTRO_ANT"),
                F.col("H2.COD_ORIGEN_SINIESTRO_NEW"),
                F.col("RP.PERIODO_POINT")
            )
            .alias("SH")
        )


        isa_max_df = ISA.groupBy("NID_SINIESTRO").agg(
            F.max("NID_SOLICITUD_AUDIT").alias("MAX_AUDIT_ID")
        ).withColumnRenamed("NID_SINIESTRO","MAX_NID_SINIESTRO")

        isa_filtrada = ISA.join(
            isa_max_df.alias("max"),
            (F.col("ISA.NID_SINIESTRO") == F.col("max.MAX_NID_SINIESTRO")) &
            (F.col("ISA.NID_SOLICITUD_AUDIT") == F.col("max.MAX_AUDIT_ID"))
        )




        #  “Recuperaciones”
        df_recup = (
            DT.alias("DT")
            .join(RP.alias("RP"),F.col("RP.nid_recuperacion") == F.col("DT.nid_recuperacion"),"left")
            .join(TR.alias("TR"), F.col("RP.cod_tipo_recuperacion") == F.col("TR.cod_tipo_recuperacion"))
            .join(TM.alias("TM"), F.col("RP.cod_movimiento") == F.col(TM_COD_MOVIMIENTO))
            .join(SIN.alias("SIN"), F.col("RP.nid_siniestro")    == F.col(SIN_NID_SINIESTRO))
            .join(CS.alias("CS"), F.col(SIN_COD_CAUSA_AFP) == F.col(CS_COD_CAUSA_SINIESTRO),"left")
            .join(CO.alias("CO"), F.col(SIN_COD_ORIGEN)               == F.col(CO_COD_ORIGEN))
            .join(PA, (F.col(SIN_NID_NUM_IDEN) == F.col(PA_NID_IDENTIFICACION)) &
                    (F.col(SIN_COD_TIPO_IDEN) == F.col(PA_COD_TIPO_IDEN)),"left")
            .join(isa_filtrada.alias("ISA_FIL"), F.col(SIN_NID_SINIESTRO) == F.col(ISA_FIL_NID_SINIESTRO), "left")

            .join(
                P.alias("P"),
                ( F.col(SIN_FEC_SINIESTRO) >= F.col(P_FEC_INICIAL_VIGENCIA)) &
                ( F.col(SIN_FEC_SINIESTRO)<= F.col(P_FEC_FINAL_VIGENCIA)),"left"
            )
 
            .join(
                SH,
                (F.col("SH.NID_SINIESTRO") == F.col("SIN.nid_siniestro")) &
                (F.col("SH.PERIODO_POINT") == F.col("RP.PERIODO_POINT")),
                "left"
            )
            .join(
                OSF,
                F.col("SIN.nid_siniestro") == F.col("OSF.NID_SINIESTRO"),
                "left"
            )
            .join(
                PN,
                (F.col("SIN.NID_NUMERO_IDENTIFICACION") == F.col("PN.NID_NUMERO_IDENTIFICACION"))&
                (F.col("SIN.COD_TIPO_IDENTIFICACION") == F.col("PN.COD_TIPO_IDENTIFICACION"))
            )
            .filter(F.col("RP.nid_recibo_caja_point") != 0)
            .select(
                F.col("TR.cod_tipo_recuperacion").cast("string").alias("id_categoria_fk"),
                F.col("TR.nombre").alias("categoria"),
                F.lit("Recuperaciones").alias("tipo"),
                F.col("RP.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
                F.col(TM_DESCRIPCION).alias("tipo_transaccion"),
                F.col("RP.nid_siniestro").cast("string").alias("numero_siniestro"),
                F.abs(F.col("DT.VLR_CONCEPTO")).alias("valor_transaccion"),
                F.col("DT.VLR_CONCEPTO").alias("valor_transaccion_ajustado"),
                F.col("RP.fec_creacion_registro").alias("fecha_transaccion"),
                F.when(
                    F.col(CO_COD_ORIGEN) == '2' ,
                    F.col(ISA_DIA_181)
                ).otherwise(
                    F.col(SIN_FEC_SINIESTRO)
                ).alias("fecha_ocurrencia"), 
                F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
                F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),

                # Cod Origen: versión codificada
                F.when(F.col("TR.NOMBRE") == "AUXILIO FUNERARIO", F.lit("4"))
                 .when(F.col("TR.NOMBRE") == "SUBSIDIO POR INCAPACIDAD TEMPORAL", F.lit("5"))
                 .when(F.col("SH.COD_ORIGEN_SINIESTRO_NEW") == F.col("OSF.nombre_origen_final"), F.col("SH.COD_ORIGEN_SINIESTRO_NEW"))
                 .otherwise(F.col("CO.cod_origen").cast("string")).alias("cod_origen"),                
                F.when(F.col("TR.NOMBRE") == "AUXILIO FUNERARIO", F.lit("AUXILIO FUNERARIO"))
                 .when(F.col("TR.NOMBRE") == "SUBSIDIO POR INCAPACIDAD TEMPORAL", F.lit("INCAPACIDAD"))
                 .when(F.col("SH.COD_ORIGEN_SINIESTRO_NEW") == F.col("OSF.nombre_origen_final"), F.col("SH.COD_ORIGEN_SINIESTRO_NEW"))
                 .otherwise(F.col("CO.NOMBRE")).alias("origen"),
                F.coalesce(F.col(SIN_FEC_INICIO_SOLICITUD_AFP), F.col(SIN_FEC_SOLICITUD_AFP)).alias("fecha_aviso"),
                F.coalesce(F.col(P_NID_POLIZA), F.lit(999)).alias("poliza"),
                F.lit("2").cast("string").alias("id_fuente_fk"),
                F.lit("50").cast("string").alias("compania"),
                F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
                F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
                F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.coalesce(
                F.col("SIN.FEC_INICIO_SOLICITUD_AFP"),
                F.to_date(
                    F.concat_ws("", F.col("PA.priper_cotrep").cast("string"), F.lit("01")),
                    "yyyyMMdd"
                    )
                ).alias("fecha_inicio_vigencia"),
                F.coalesce(F.col("SIN.COD_CIUDAD"), F.lit("01")).alias("ciudad_lugar_ocurrencia"),
                F.coalesce(F.col("SIN.COD_DEPARTAMENTO").cast("string"), F.lit("01001")).alias("depto_lugar_ocurrencia"),
                F.concat(F.col("SIN.COD_DEPARTAMENTO").cast("string"), F.col("SIN.COD_CIUDAD").cast("string"))
                 .cast("int")
                 .alias("lugar_ocurrencia"),
                F.col("SIN.NID_NUMERO_IDENTIFICACION").alias("cedula"),
                F.col("PN.FEC_NACIMIENTO").alias("fecha_nacimiento"),
                F.col("PN.GENERO").alias("genero"),
                F.col("RP.PERIODO_POINT")
            
            )
        )



        df_final = df_recup

        return df_final
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from pyspark.sql import functions as F, Window


class StepGold:
    def __init__(self, glue_context, spark):
        self.spark = spark
        
    
    def build_SH(self,base_df, h2_df, periodo_col, base_alias, h2_alias="H2"):
        diff_expr = F.abs(
            F.expr(f"int(date_format({h2_alias}.FEC_MODIFICACION, 'yyyyMM'))") - F.col(f"{base_alias}.{periodo_col}").cast("int")
        )

        window_spec = Window.partitionBy(f"{h2_alias}.NID_SINIESTRO").orderBy(diff_expr)

        SH = (
            h2_df.join(base_df, F.col(f"{h2_alias}.NID_SINIESTRO") == F.col(f"{base_alias}.NID_SINIESTRO"), "inner")
            .withColumn("RN", F.row_number().over(window_spec))
            .filter(F.col("RN") == 1)
            .select(
                F.col(f"{h2_alias}.NID_SINIESTRO"),
                F.col(f"{h2_alias}.COD_ORIGEN_SINIESTRO_ANT"),
                F.col(f"{h2_alias}.COD_ORIGEN_SINIESTRO_NEW"),
                F.col(f"{base_alias}.{periodo_col}").alias("PERIODO_POINT")
            )
            .alias("SH")
        )

        return SH


    def create_component(self):
        """
        This Function to create gold_layer Table.
        """


        decimal_cast = "decimal(19, 0)"
        SIN_COD_CAUSA_AFP = "SIN.cod_causa_siniestro_afp"
        CP_COD_CONCEPTO_PAGO_SINIESTRO = "CP.cod_concepto_pago_siniestro"
        TM_COD_MOVIMIENTO = "TM.cod_movimiento"
        SIN_COD_ORIGEN = "SIN.cod_origen"
        SIN_NID_SINIESTRO ="SIN.nid_siniestro"
        CS_COD_CAUSA_SINIESTRO = "CS.cod_causa_siniestro"
        CO_COD_ORIGEN = "CO.cod_origen"
        SIN_FEC_SINIESTRO = "SIN.fec_siniestro"
        P_FEC_INICIAL_VIGENCIA = "P.fec_inicial_vigencia"
        P_FEC_FINAL_VIGENCIA = "P.fec_final_vigencia"
        DR_VALOR_RESERVA = "DR.vlr_reserva"
        AFIL_CEDULA ="AF.AFIL_CEDULA"
        PS_VLR_PAGO ="PS.vlr_pago"
        SIN_FEC_SOLICITUD_AFP = "SIN.fec_solicitud_afp"
        TM_DESCRIPCION ="TM.descripcion"
        ISA_DIA_181 ="ISA_FIL.dia_181"
        CS_DESCRIPCION = "CS.descripcion"
        UNIDAD_DE_NEGOCIO = "Seguridad social"
        P_NID_POLIZA ="P.nid_poliza"
        CO_NOMBRE = "CO.nombre"
        PA_NID_IDENTIFICACION = "PA.NID_IDENTIFICACION"
        SIN_NID_NUM_IDEN = "SIN.NID_NUMERO_IDENTIFICACION"
        SIN_COD_TIPO_IDEN = "SIN.COD_TIPO_IDENTIFICACION"
        PA_COD_TIPO_IDEN = "PA.COD_TIPO_IDENTIFICACION_AFP"
        ISA_FIL_NID_SINIESTRO = "ISA_FIL.NID_SINIESTRO"
        SIN_NID_SOL = "SIN.NID_SOLICITUD_AFP"
        BP_NID_SOLICITUD_AFP = "BP.NID_SOLICITUD_AFP"
        BP_CIUDAD_SIN = "BP.CIUDAD_SINIESTRO"
        SIN_COD_DEP = "SIN.COD_DEPARTAMENTO"
        AF_FIL_IN = "AF.afil_fecing"
        AF_CLASEI = "AF.AFIL_CLASEI"
        AF_FEC_NAC = "AF.afil_fecnac"
        AF_SEXO = "AF.afil_sexo"
        AF_NUMCUEN = "AF.AFIL_NUMCUE"
        PA_CUENTA_IND = "PA.NID_CUENTA_INDIVIDUAL"
        DESC_CAUSA = "Otras causas"
   

        
        SIN_FEC_INICIO_SOLICITUD_AFP ="SIN.fec_inicio_solicitud_afp"
        DR = self.spark.table("global_temp.spr_detalle_reserva")
        PN = self.spark.table("global_temp.acu_personas_naturales").alias("PN")
        SR = self.spark.table("global_temp.spr_reserva")
        CP = self.spark.table("global_temp.spr_conceptos_pagos_siniestros")
        TM = self.spark.table("global_temp.cgr_movimientos_reserva")
        SIN = self.spark.table("global_temp.spr_siniestros").alias("SIN")
        CS = self.spark.table("global_temp.spr_causas_siniestros")
        CO = self.spark.table("global_temp.cgr_origenes").alias("CO")
        P  = self.spark.table("global_temp.ppr_polizas")
        ISA= self.spark.table("global_temp.spr_incapacidades_sol_audit").alias("ISA")
        PS = self.spark.table("global_temp.spr_detalle_pago_siniestro")
        PG = self.spark.table("global_temp.spr_pagos")
        PA = self.spark.table("global_temp.ppr_afiliados").alias("PA")
        RP = self.spark.table("global_temp.spr_recuperaciones").alias("RP")
        TR = self.spark.table("global_temp.spr_tipos_recuperaciones")
        DT = self.spark.table("global_temp.spr_detalle_recuperaciones").alias("DT")
        H2 = spark.table("global_temp.Spr_siniestros_origenes_hist").alias("H2")
        

        window_origen = Window.partitionBy("H2.NID_SINIESTRO").orderBy(F.col("H2.NID_SINIESTRO_ORIGEN_HIST").desc())

        OSF = (
            H2.join(CO, F.col("H2.COD_ORIGEN_SINIESTRO_NEW") == F.col("CO.COD_ORIGEN"), "left")
            .withColumn("RN", F.row_number().over(window_origen))
            .filter(F.col("RN") == 1)
            .select(
                F.col("H2.NID_SINIESTRO"),
                F.col("CO.NOMBRE").alias("nombre_origen_final")
            )
            .alias("OSF")
        )



        SH_RP = self.build_SH(RP,H2,"PERIODO_POINT",base_alias="RP",h2_alias="H2")
        #SH_PG = self.build_SH(PG,H2,"PERIODO_POINT",base_alias="PG",h2_alias="H2")
        SH_RE = self.build_SH(SR,H2,"PERIODO_POINT",base_alias="SR",h2_alias="H2")


        isa_max_df = ISA.groupBy("NID_SINIESTRO").agg(
            F.max("NID_SOLICITUD_AUDIT").alias("MAX_AUDIT_ID")
        ).withColumnRenamed("NID_SINIESTRO","MAX_NID_SINIESTRO")

        isa_filtrada = ISA.join(
            isa_max_df.alias("max"),
            (F.col("ISA.NID_SINIESTRO") == F.col("max.MAX_NID_SINIESTRO")) &
            (F.col("ISA.NID_SOLICITUD_AUDIT") == F.col("max.MAX_AUDIT_ID"))
        )


        #  “Reserva”
        df_reserva = (
            DR.alias("DR")
            .join(SR.alias("SR"), F.col("DR.nid_reserva") == F.col("SR.nid_reserva"))
            .join(CP.alias("CP"), F.col("DR.cod_concepto_pago_siniestro") == F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO))
            .join(TM.alias("TM"), F.col("DR.cod_movimiento") == F.col(TM_COD_MOVIMIENTO))
            .join(SIN.alias("SIN"), F.col("DR.nid_siniestro") == F.col(SIN_NID_SINIESTRO))
            .join(CS.alias("CS"), F.col(SIN_COD_CAUSA_AFP) == F.col(CS_COD_CAUSA_SINIESTRO),"left")
            .join(CO.alias("CO"), F.col(SIN_COD_ORIGEN) == F.col(CO_COD_ORIGEN))
            .join(PA, (F.col(SIN_NID_NUM_IDEN) == F.col(PA_NID_IDENTIFICACION)) &
                    (F.col(SIN_COD_TIPO_IDEN) == F.col(PA_COD_TIPO_IDEN)),"left")
            .join(isa_filtrada.alias("ISA_FIL"), F.col(SIN_NID_SINIESTRO) == F.col(ISA_FIL_NID_SINIESTRO), "left")
            .join(
                PN,
                (F.col("SIN.NID_NUMERO_IDENTIFICACION") == F.col("PN.NID_NUMERO_IDENTIFICACION"))&
                (F.col("SIN.COD_TIPO_IDENTIFICACION") == F.col("PN.COD_TIPO_IDENTIFICACION"))
            )
            .join(
                SH_RE,
                (F.col("SH.NID_SINIESTRO") == F.col("SIN.nid_siniestro")) &
                (F.col("SH.PERIODO_POINT") == F.col("RP.PERIODO_POINT")),
                "left"
            )
            .join(
                OSF,
                F.col("SIN.nid_siniestro") == F.col("OSF.NID_SINIESTRO"),
                "left"
            )
            .join(
                P.alias("P"),
                ( F.col(SIN_FEC_SINIESTRO) >= F.col(P_FEC_INICIAL_VIGENCIA)) &
                ( F.col(SIN_FEC_SINIESTRO)<= F.col(P_FEC_FINAL_VIGENCIA)),"left"
            )
            .filter(F.col(DR_VALOR_RESERVA) != 0)
            .select(
                F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO).cast("string").alias("id_categoria_fk"),
                F.col("CP.descripcion").alias("categoria"),
                F.lit("Reserva").alias("tipo"),
                F.col("DR.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
                F.col(TM_DESCRIPCION).alias("tipo_transaccion"),
                F.col("DR.nid_siniestro").cast("string").alias("numero_siniestro"),
                F.abs(F.col(DR_VALOR_RESERVA) ).alias("valor_transaccion"),
                F.col(DR_VALOR_RESERVA).alias("valor_transaccion_ajustado"),
                F.col("SR.fec_generado").alias("fecha_transaccion"),
                F.when(
                    F.col(CO_COD_ORIGEN) == '2' ,
                    F.col(ISA_DIA_181)
                ).otherwise(
                    F.col(SIN_FEC_SINIESTRO)
                ).alias("fecha_ocurrencia"),
                F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
                F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),
                F.col(CO_COD_ORIGEN).cast(decimal_cast).alias("cod_origen"),
                F.col(CO_NOMBRE).alias("origen"),
                F.coalesce(F.col(SIN_FEC_INICIO_SOLICITUD_AFP), F.col(SIN_FEC_SOLICITUD_AFP)).alias("fecha_aviso"),
                F.coalesce(F.col(P_NID_POLIZA), F.lit(999)).alias("poliza"),
                F.lit("2").cast("string").alias("id_fuente_fk"),
                F.lit("50").cast("string").alias("compania"),
                F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
                F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
                F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.coalesce(F.col("SIN.COD_CIUDAD"), F.lit("01")).alias("ciudad_lugar_ocurrencia"),
                F.coalesce(F.col("SIN.COD_DEPARTAMENTO").cast("string"), F.lit("01001")).alias("depto_lugar_ocurrencia"),
                F.coalesce(
                F.col("SIN.FEC_INICIO_SOLICITUD_AFP"),
                F.to_date(
                    F.concat_ws("", F.col("PA.priper_cotrep").cast("string"), F.lit("01")),
                    "yyyyMMdd"
                    )
                ).alias("fecha_inicio_vigencia"),
                F.col("SIN.NID_NUMERO_IDENTIFICACION").alias("cedula"),
                F.col("PN.FEC_NACIMIENTO").alias("fecha_nacimiento"),
                F.col("PN.GENERO").alias("genero")             
            )
        )


      


        #  “Recuperaciones”
        df_recup = (
            DT.alias("DT")
            .join(RP.alias("RP"),F.col("RP.nid_recuperacion") == F.col("DT.nid_recuperacion"),"left")
            .join(TR.alias("TR"), F.col("RP.cod_tipo_recuperacion") == F.col("TR.cod_tipo_recuperacion"))
            .join(TM.alias("TM"), F.col("RP.cod_movimiento") == F.col(TM_COD_MOVIMIENTO))
            .join(SIN.alias("SIN"), F.col("RP.nid_siniestro")    == F.col(SIN_NID_SINIESTRO))
            .join(CS.alias("CS"), F.col(SIN_COD_CAUSA_AFP) == F.col(CS_COD_CAUSA_SINIESTRO),"left")
            .join(CO.alias("CO"), F.col(SIN_COD_ORIGEN)               == F.col(CO_COD_ORIGEN))
            .join(PA, (F.col(SIN_NID_NUM_IDEN) == F.col(PA_NID_IDENTIFICACION)) &
                    (F.col(SIN_COD_TIPO_IDEN) == F.col(PA_COD_TIPO_IDEN)),"left")
            .join(isa_filtrada.alias("ISA_FIL"), F.col(SIN_NID_SINIESTRO) == F.col(ISA_FIL_NID_SINIESTRO), "left")
            .join(
                P.alias("P"),
                ( F.col(SIN_FEC_SINIESTRO) >= F.col(P_FEC_INICIAL_VIGENCIA)) &
                ( F.col(SIN_FEC_SINIESTRO)<= F.col(P_FEC_FINAL_VIGENCIA)),"left"
            )
            .join(
                SH_RP,
                (F.col("SH.NID_SINIESTRO") == F.col("SIN.nid_siniestro")) &
                (F.col("SH.PERIODO_POINT") == F.col("RP.PERIODO_POINT")),
                "left"
            )
            .join(
                OSF,
                F.col("SIN.nid_siniestro") == F.col("OSF.NID_SINIESTRO"),
                "left"
            )
            .join(
                PN,
                (F.col("SIN.NID_NUMERO_IDENTIFICACION") == F.col("PN.NID_NUMERO_IDENTIFICACION"))&
                (F.col("SIN.COD_TIPO_IDENTIFICACION") == F.col("PN.COD_TIPO_IDENTIFICACION"))
            )
            .filter(F.col("RP.nid_recibo_caja_point") != 0)
            .select(
                F.col("TR.cod_tipo_recuperacion").cast("string").alias("id_categoria_fk"),
                F.col("TR.nombre").alias("categoria"),
                F.lit("Recuperaciones").alias("tipo"),
                F.col("RP.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
                F.col(TM_DESCRIPCION).alias("tipo_transaccion"),
                F.col("RP.nid_siniestro").cast("string").alias("numero_siniestro"),
                F.abs(F.col("DT.VLR_CONCEPTO")).alias("valor_transaccion"),
                F.col("DT.VLR_CONCEPTO").alias("valor_transaccion_ajustado"),
                F.col("RP.fec_creacion_registro").alias("fecha_transaccion"),
                F.when(
                    F.col(CO_COD_ORIGEN) == '2' ,
                    F.col(ISA_DIA_181)
                ).otherwise(
                    F.col(SIN_FEC_SINIESTRO)
                ).alias("fecha_ocurrencia"), 
                F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
                F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),
                F.col(CO_COD_ORIGEN).cast(decimal_cast).alias("cod_origen"),
                F.col(CO_NOMBRE).alias("origen"),
                F.coalesce(F.col(SIN_FEC_INICIO_SOLICITUD_AFP), F.col(SIN_FEC_SOLICITUD_AFP)).alias("fecha_aviso"),
                F.coalesce(F.col(P_NID_POLIZA), F.lit(999)).alias("poliza"),
                F.lit("2").cast("string").alias("id_fuente_fk"),
                F.lit("50").cast("string").alias("compania"),
                F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
                F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
                F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.coalesce(F.col("SIN.COD_CIUDAD"), F.lit("01")).alias("ciudad_lugar_ocurrencia"),
                F.coalesce(F.col("SIN.COD_DEPARTAMENTO").cast("string"), F.lit("01001")).alias("depto_lugar_ocurrencia"),
                F.coalesce(
                F.col("SIN.FEC_INICIO_SOLICITUD_AFP"),
                F.to_date(
                    F.concat_ws("", F.col("PA.priper_cotrep").cast("string"), F.lit("01")),
                    "yyyyMMdd"
                    )
                ).alias("fecha_inicio_vigencia"),
                F.col("SIN.NID_NUMERO_IDENTIFICACION").alias("cedula"),
                F.col("PN.FEC_NACIMIENTO").alias("fecha_nacimiento"),
                F.col("PN.GENERO").alias("genero")  
            
            )
        )



        df_final = df_reserva.unionByName(df_recup)


        return df_final
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from pyspark.sql import functions as F, Window


class StepGold:
    def __init__(self, glue_context, spark):
        self.spark = spark
        
    
    def build_SH(self,base_df, h2_df, periodo_col,base_llave, base_alias,h2_alias="H2"):
        diff_expr = F.abs(
            F.expr(f"int(date_format({h2_alias}.FEC_MODIFICACION, 'yyyyMM'))") - F.col(f"{base_alias}.{periodo_col}").cast("int")
        )

        window_spec = Window.partitionBy(f"{h2_alias}.NID_SINIESTRO").orderBy(diff_expr)

        SH = (
            h2_df.join(base_df, F.col(f"{h2_alias}.NID_SINIESTRO") == F.col(f"{base_alias}.{base_llave}"), "inner")
            .withColumn("RN", F.row_number().over(window_spec))
            .filter(F.col("RN") == 1)
            .select(
                F.col(f"{h2_alias}.NID_SINIESTRO"),
                F.col(f"{h2_alias}.COD_ORIGEN_SINIESTRO_ANT"),
                F.col(f"{h2_alias}.FEC_MODIFICACION"),
                F.col(f"{h2_alias}.COD_ORIGEN_SINIESTRO_NEW"),
                F.col(f"{base_alias}.{periodo_col}").alias("PERIODO_POINT"),
                
            )
            .alias("SH")
        )

        return SH


    def create_component(self):
        """
        This Function to create gold_layer Table.
        """


        decimal_cast = "decimal(19, 0)"
        SIN_COD_CAUSA_AFP = "SIN.cod_causa_siniestro_afp"
        CP_COD_CONCEPTO_PAGO_SINIESTRO = "CP.cod_concepto_pago_siniestro"
        TM_COD_MOVIMIENTO = "TM.cod_movimiento"
        SIN_COD_ORIGEN = "SIN.cod_origen"
        SIN_NID_SINIESTRO ="SIN.nid_siniestro"
        CS_COD_CAUSA_SINIESTRO = "CS.cod_causa_siniestro"
        CO_COD_ORIGEN = "CO.cod_origen"
        SIN_FEC_SINIESTRO = "SIN.fec_siniestro"
        P_FEC_INICIAL_VIGENCIA = "P.fec_inicial_vigencia"
        P_FEC_FINAL_VIGENCIA = "P.fec_final_vigencia"
        DR_VALOR_RESERVA = "DR.vlr_reserva"
        AFIL_CEDULA ="AF.AFIL_CEDULA"
        PS_VLR_PAGO ="PS.vlr_pago"
        SIN_FEC_SOLICITUD_AFP = "SIN.fec_solicitud_afp"
        TM_DESCRIPCION ="TM.descripcion"
        ISA_DIA_181 ="ISA_FIL.dia_181"
        CS_DESCRIPCION = "CS.descripcion"
        UNIDAD_DE_NEGOCIO = "Seguridad social"
        P_NID_POLIZA ="P.nid_poliza"
        CO_NOMBRE = "CO.nombre"
        PA_NID_IDENTIFICACION = "PA.NID_IDENTIFICACION"
        SIN_NID_NUM_IDEN = "SIN.NID_NUMERO_IDENTIFICACION"
        SIN_COD_TIPO_IDEN = "SIN.COD_TIPO_IDENTIFICACION"
        PA_COD_TIPO_IDEN = "PA.COD_TIPO_IDENTIFICACION_AFP"
        ISA_FIL_NID_SINIESTRO = "ISA_FIL.NID_SINIESTRO"
        SIN_NID_SOL = "SIN.NID_SOLICITUD_AFP"
        BP_NID_SOLICITUD_AFP = "BP.NID_SOLICITUD_AFP"
        BP_CIUDAD_SIN = "BP.CIUDAD_SINIESTRO"
        SIN_COD_DEP = "SIN.COD_DEPARTAMENTO"
        AF_FIL_IN = "AF.afil_fecing"
        AF_CLASEI = "AF.AFIL_CLASEI"
        AF_FEC_NAC = "AF.afil_fecnac"
        AF_SEXO = "AF.afil_sexo"
        AF_NUMCUEN = "AF.AFIL_NUMCUE"
        PA_CUENTA_IND = "PA.NID_CUENTA_INDIVIDUAL"
        DESC_CAUSA = "Otras causas"
   

        
        SIN_FEC_INICIO_SOLICITUD_AFP ="SIN.fec_inicio_solicitud_afp"
        DR = self.spark.table("global_temp.spr_detalle_reserva")
        PN = self.spark.table("global_temp.acu_personas_naturales").alias("PN")
        SR = self.spark.table("global_temp.spr_reserva").alias("SR")
        CP = self.spark.table("global_temp.spr_conceptos_pagos_siniestros")
        TM = self.spark.table("global_temp.cgr_movimientos_reserva")
        SIN = self.spark.table("global_temp.spr_siniestros").alias("SIN")
        CS = self.spark.table("global_temp.spr_causas_siniestros")
        CO = self.spark.table("global_temp.cgr_origenes").alias("CO")
        P  = self.spark.table("global_temp.ppr_polizas")
        ISA= self.spark.table("global_temp.spr_incapacidades_sol_audit").alias("ISA")
        PS = self.spark.table("global_temp.spr_detalle_pago_siniestro")
        PG = self.spark.table("global_temp.spr_pagos").alias("PG")
        PA = self.spark.table("global_temp.ppr_afiliados").alias("PA")
        RP = self.spark.table("global_temp.spr_recuperaciones").alias("RP")
        TR = self.spark.table("global_temp.spr_tipos_recuperaciones")
        DT = self.spark.table("global_temp.spr_detalle_recuperaciones").alias("DT")
        H2 = spark.table("global_temp.Spr_siniestros_origenes_hist").alias("H2")





        SH_RE = self.build_SH(SR,H2,"PERIODO_POINT","NID_SINIESTRO",base_alias="SR",h2_alias="H2").alias("SH_RE")


        isa_max_df = ISA.groupBy("NID_SINIESTRO").agg(
            F.max("NID_SOLICITUD_AUDIT").alias("MAX_AUDIT_ID")
        ).withColumnRenamed("NID_SINIESTRO","MAX_NID_SINIESTRO")

        isa_filtrada = ISA.join(
            isa_max_df.alias("max"),
            (F.col("ISA.NID_SINIESTRO") == F.col("max.MAX_NID_SINIESTRO")) &
            (F.col("ISA.NID_SOLICITUD_AUDIT") == F.col("max.MAX_AUDIT_ID"))
        )


        #  “Reserva”
        df_reserva = (
            DR.alias("DR")
            .join(SR.alias("SR"), F.col("DR.nid_reserva") == F.col("SR.nid_reserva"))
            .join(CP.alias("CP"), F.col("DR.cod_concepto_pago_siniestro") == F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO))
            .join(TM.alias("TM"), F.col("DR.cod_movimiento") == F.col(TM_COD_MOVIMIENTO))
            .join(SIN.alias("SIN"), F.col("DR.nid_siniestro") == F.col(SIN_NID_SINIESTRO))
            .join(CS.alias("CS"), F.col(SIN_COD_CAUSA_AFP) == F.col(CS_COD_CAUSA_SINIESTRO),"left")
            .join(CO.alias("CO"), F.col(SIN_COD_ORIGEN) == F.col(CO_COD_ORIGEN))
            .join(PA, (F.col(SIN_NID_NUM_IDEN) == F.col(PA_NID_IDENTIFICACION)) &
                    (F.col(SIN_COD_TIPO_IDEN) == F.col(PA_COD_TIPO_IDEN)),"left")
            .join(isa_filtrada.alias("ISA_FIL"), F.col(SIN_NID_SINIESTRO) == F.col(ISA_FIL_NID_SINIESTRO), "left")
            .join(
                PN,
                (F.col("SIN.NID_NUMERO_IDENTIFICACION") == F.col("PN.NID_NUMERO_IDENTIFICACION"))&
                (F.col("SIN.COD_TIPO_IDENTIFICACION") == F.col("PN.COD_TIPO_IDENTIFICACION"))
            )
            .join(
                SH_RE,
                (F.col("SH_RE.NID_SINIESTRO") == F.col("SR.nid_siniestro")) ,
                "left"
            )
            .join(
                P.alias("P"),
                ( F.col(SIN_FEC_SINIESTRO) >= F.col(P_FEC_INICIAL_VIGENCIA)) &
                ( F.col(SIN_FEC_SINIESTRO)<= F.col(P_FEC_FINAL_VIGENCIA)),"left"
            )
            
            .join(
                CO.alias("CO3"),
                F.col("CO3.COD_ORIGEN") == F.col("SH_RE.COD_ORIGEN_SINIESTRO_NEW"),
                "left"
            )
            .join(
                CO.alias("CO4"),
                F.col("CO4.COD_ORIGEN") == F.col("SH_RE.COD_ORIGEN_SINIESTRO_ANT"),
                "left"
            )
            .filter(F.col(DR_VALOR_RESERVA) != 0)
            .select(
                F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO).cast("string").alias("id_categoria_fk"),
                F.col("CP.descripcion").alias("categoria"),
                F.lit("Reserva").alias("tipo"),
                F.col("DR.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
                F.col(TM_DESCRIPCION).alias("tipo_transaccion"),
                F.col("DR.nid_siniestro").cast("string").alias("numero_siniestro"),
                F.abs(F.col(DR_VALOR_RESERVA) ).alias("valor_transaccion"),
                F.col(DR_VALOR_RESERVA).alias("valor_transaccion_ajustado"),
                F.col("SR.fec_generado").alias("fecha_transaccion"),
                F.when(
                    F.col(CO_COD_ORIGEN) == '2' ,
                    F.col(ISA_DIA_181)
                ).otherwise(
                    F.col(SIN_FEC_SINIESTRO)
                ).alias("fecha_ocurrencia"),
                F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
                F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),
                F.when(F.col("SR.PERIODO_POINT").cast("int") >= F.expr("int(date_format(SH_RE.FEC_MODIFICACION, 'yyyyMM'))"),
                       F.col("SH_RE.COD_ORIGEN_SINIESTRO_NEW"))
                 .when(F.col("SR.PERIODO_POINT").cast("int") < F.expr("int(date_format(SH_RE.FEC_MODIFICACION, 'yyyyMM'))"),
                       F.col("SH_RE.COD_ORIGEN_SINIESTRO_ANT"))
                 .otherwise(F.col("CO.cod_origen").cast("string"))
                 .alias("cod_origen"),

                F.when(F.col("SR.PERIODO_POINT").cast("int") >= F.expr("int(date_format(SH_RE.FEC_MODIFICACION, 'yyyyMM'))"),
                       F.col("CO3.NOMBRE"))
                 .when(F.col("SR.PERIODO_POINT").cast("int") < F.expr("int(date_format(SH_RE.FEC_MODIFICACION, 'yyyyMM'))"),
                       F.col("CO4.NOMBRE"))
                 .otherwise(F.col("CO.NOMBRE"))
                 .alias("origen"),

                F.coalesce(F.col(SIN_FEC_INICIO_SOLICITUD_AFP), F.col(SIN_FEC_SOLICITUD_AFP)).alias("fecha_aviso"),
                F.coalesce(F.col(P_NID_POLIZA), F.lit(999)).alias("poliza"),
                F.lit("2").cast("string").alias("id_fuente_fk"),
                F.lit("50").cast("string").alias("compania"),
                F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
                F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
                F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.coalesce(F.col("SIN.COD_CIUDAD"), F.lit("01")).alias("ciudad_lugar_ocurrencia"),
                F.coalesce(F.col("SIN.COD_DEPARTAMENTO").cast("string"), F.lit("01001")).alias("depto_lugar_ocurrencia"),
                F.coalesce(
                F.col("SIN.FEC_INICIO_SOLICITUD_AFP"),
                F.to_date(
                    F.concat_ws("", F.col("PA.priper_cotrep").cast("string"), F.lit("01")),
                    "yyyyMMdd"
                    )
                ).alias("fecha_inicio_vigencia"),
                F.col("SIN.NID_NUMERO_IDENTIFICACION").alias("cedula"),
                F.col("PN.FEC_NACIMIENTO").alias("fecha_nacimiento"),
                F.col("PN.GENERO").alias("genero")             
            )
        )

        


        df_final = df_reserva


        return df_final

transacciones = StepGold(glueContext, spark)
df_reservas = transacciones.create_component()

# Mostrar el resultado

df_reservas.count()
df_reservas.filter(F.col("numero_siniestro") == "201803037").show(truncate = False)
RP = spark.table("global_temp.spr_recuperaciones").alias("RP")
RP.printSchema()
df_reservas.count()
df_reservas.filter(F.col("numero_siniestro") == "201803037").show()
df_test = spark.sql("select * from global_temp.spr_reserva")
df_test.printSchema()
df_tierra = StepGold.create_component()
from pyspark.sql import SparkSession, functions as F, Window


# Cargar tablas desde global_temp con alias
DR = spark.table("global_temp.spr_detalle_reserva").alias("DR")
SR = spark.table("global_temp.spr_reserva").alias("SR")
CP = spark.table("global_temp.spr_conceptos_pagos_siniestros").alias("CP")
TM = spark.table("global_temp.cgr_movimientos_reserva").alias("TM")
SIN = spark.table("global_temp.spr_siniestros").alias("SIN")
CS = spark.table("global_temp.spr_causas_siniestros").alias("CS")
CO = spark.table("global_temp.cgr_origenes").alias("CO")
P = spark.table("global_temp.ppr_polizas").alias("P")
PA = spark.table("global_temp.ppr_afiliados").alias("PA")
AF = spark.table("global_temp.vw_dsdafil_alfa").alias("AF")
ISA = spark.table("global_temp.spr_incapacidades_sol_audit").alias("ISA")
SH = spark.table("global_temp.spr_siniestros_origenes_hist").alias("SH")
CO3 = spark.table("global_temp.cgr_origenes").alias("CO3")
BP = spark.table("global_temp.ben_solicitudes_v").alias("BP")



# Subconsulta: nombre del origen más reciente
H4 = spark.table("global_temp.spr_siniestros_origenes_hist").alias("H4")

max_hist = H4.groupBy(F.col("NID_SINIESTRO")) \
             .agg(F.max("NID_SINIESTRO_ORIGEN_HIST").alias("MAX_HIST")) \
             .alias("MH")

# Subconsulta completa con joins desambiguados
hist_nombre = H4.alias("H4") \
    .join(max_hist, 
          (F.col("H4.NID_SINIESTRO_ORIGEN_HIST") == F.col("MH.MAX_HIST")) & 
          (F.col("H4.NID_SINIESTRO") == F.col("MH.NID_SINIESTRO"))) \
    .join(CO3.alias("CO3"), F.col("H4.COD_ORIGEN_SINIESTRO_NEW") == F.col("CO3.COD_ORIGEN")) \
    .select(
        F.col("H4.NID_SINIESTRO").alias("nid_siniestro_hist"),
        F.col("CO3.NOMBRE").alias("nombre_origen")
    )
hist_nombre.createOrReplaceGlobalTempView("temp_hist_nombre")

# Subconsulta: histórico más cercano al periodo_point
H2 = spark.table("global_temp.spr_siniestros_origenes_hist").alias("H2")
historico = H2.join(DR, F.col("H2.NID_SINIESTRO") == F.col("DR.NID_SINIESTRO")) \
              .join(SR, F.col("DR.NID_RESERVA") == F.col("SR.NID_RESERVA")) \
              .withColumn("PERIODOR", F.concat_ws("", 
                  F.year("H2.FEC_MODIFICACION").cast("string"), 
                  F.lpad(F.month("H2.FEC_MODIFICACION").cast("string"), 2, "0"))) \
              .withColumn("X", F.abs(F.col("PERIODOR").cast("int") - F.col("SR.PERIODO_POINT"))) \
              .withColumn("row", F.row_number().over(Window.partitionBy("H2.NID_SINIESTRO").orderBy("X"))) \
              .filter(F.col("row") == 1) \
              .select(F.col("H2.NID_SINIESTRO"), F.col("H2.COD_ORIGEN_SINIESTRO_ANT"))

historico.createOrReplaceGlobalTempView("temp_historico")

# Subconsulta: ciudad del siniestro
ciudad_siniestro = BP.select(F.col("SOLICITUD").alias("NID_SOLICITUD_AFP"),
                             F.col("CIUDAD_SINIESTRO"))
ciudad_siniestro.createOrReplaceGlobalTempView("temp_ciudad")


# Unir todo y aplicar lógica
df_final = DR.join(SR, F.col("DR.NID_RESERVA") == F.col("SR.NID_RESERVA")) \
    .join(CP, F.col("DR.COD_CONCEPTO_PAGO_SINIESTRO") == F.col("CP.COD_CONCEPTO_PAGO_SINIESTRO")) \
    .join(TM, F.col("DR.COD_MOVIMIENTO") == F.col("TM.COD_MOVIMIENTO")) \
    .join(SIN, F.col("DR.NID_SINIESTRO") == F.col("SIN.NID_SINIESTRO")) \
    .join(CS, F.col("SIN.COD_CAUSA_SINIESTRO_AFP") == F.col("CS.COD_CAUSA_SINIESTRO")) \
    .join(CO, F.col("SIN.COD_ORIGEN") == F.col("CO.COD_ORIGEN")) \
    .join(PA, (F.col("SIN.NID_NUMERO_IDENTIFICACION") == F.col("PA.NID_IDENTIFICACION")) &
             (F.col("SIN.COD_TIPO_IDENTIFICACION") == F.col("PA.COD_TIPO_IDENTIFICACION_AFP"))) \
    .join(AF, (F.col("SIN.NID_NUMERO_IDENTIFICACION") == F.col("AF.AFIL_CEDULA")) &
              (F.col("SIN.COD_TIPO_IDENTIFICACION") == F.col("AF.AFIL_CLASEI")) &
              (F.col("AF.AFIL_NUMCUE") == F.col("PA.NID_CUENTA_INDIVIDUAL"))) \
    .join(ISA, (F.col("SIN.NID_SINIESTRO") == F.col("ISA.NID_SINIESTRO")),) \
    .join(P, ((F.coalesce(F.col("ISA.DIA_181"), F.col("SIN.fec_siniestro"))) >= F.col("P.FEC_INICIAL_VIGENCIA")) & 
              ((F.coalesce(F.col("ISA.DIA_181"), F.col("SIN.fec_siniestro"))) <= F.col("P.FEC_FINAL_VIGENCIA"))) \
    .join(SH, (F.col("SIN.NID_SINIESTRO") == F.col("SH.NID_SINIESTRO")), ) \
    .join(hist_nombre.alias("HN"), F.col("SIN.NID_SINIESTRO") == F.col("HN.nid_siniestro_hist"), ) \
    .join(historico.alias("HIST"), F.col("SIN.NID_SINIESTRO") == F.col("HIST.NID_SINIESTRO"),) \
    .join(ciudad_siniestro.alias("BP"), F.col("SIN.NID_SOLICITUD_AFP") == F.col("BP.NID_SOLICITUD_AFP"), ) \
    .filter(F.col("DR.vlr_reserva") != 0) \
    .selectExpr(
       
        "SR.periodo_point AS periodo_point",
        "CP.COD_CONCEPTO_PAGO_SINIESTRO AS id_categoria_fk",
        "CP.DESCRIPCION AS categoria",
        "'Reserva' AS tipo",
        "DR.COD_MOVIMIENTO AS id_tipo_transaccion_fk",
        "TM.DESCRIPCION AS tipo_transaccion",
        "DR.NID_SINIESTRO AS numero_siniestro",
        "ABS(DR.VLR_RESERVA) AS valor_transaccion",
        "DR.VLR_RESERVA AS valor_transaccion_ajustado",
        "SR.FEC_GENERADO AS fecha_transaccion",
        "COALESCE(ISA.DIA_181, SIN.FEC_SINIESTRO) AS fecha_ocurrencia",
        "CS.DESCRIPCION AS Causa",
        "CS.COD_CAUSA_SINIESTRO AS cod_causa",
        """CASE 
            WHEN upper(CP.DESCRIPCION) = 'AUXILIO FUNERARIO' THEN 'AUXILIO FUNERARIO'
            WHEN upper(CP.DESCRIPCION) = 'SUBSIDIO POR INCAPACIDAD TEMPORAL' THEN 'INCAPACIDAD'
            WHEN SH.COD_ORIGEN_SINIESTRO_new = HN.nombre_origen THEN SH.COD_ORIGEN_SINIESTRO_new
            ELSE CO.NOMBRE 
        END AS Origen""",
        """CASE 
            WHEN upper(CP.DESCRIPCION) = 'AUXILIO FUNERARIO' THEN '4'
            WHEN upper(CP.DESCRIPCION) = 'SUBSIDIO POR INCAPACIDAD TEMPORAL' THEN '5'
            WHEN SH.COD_ORIGEN_SINIESTRO_new = HN.nombre_origen THEN SH.COD_ORIGEN_SINIESTRO_new
            ELSE CAST(CO.COD_ORIGEN AS STRING) 
        END AS cod_Origen""",
        "COALESCE(P.NID_POLIZA, 999) AS POLIZA",
        "COALESCE(SIN.FEC_INICIO_SOLICITUD_AFP, SIN.FEC_SOLICITUD_AFP) AS Fecha_Aviso",
        "AF.AFIL_FECING AS Fecha_Inicio_Vigencia",
        """CASE 
            WHEN BP.CIUDAD_SINIESTRO IS NULL THEN CAST(SIN.COD_DEPARTAMENTO AS INT)
            ELSE CAST(SIN.COD_DEPARTAMENTO AS INT)
        END AS Lugar_Ocurrencia""",
        "AF.AFIL_CEDULA AS Cedula",
        "AF.AFIL_FECNAC AS Fecha_Nacimiento",
        "AF.AFIL_SEXO AS Genero"
    ).distinct()

contador = StepGoldCount(spark)
total = contador.count_total_records()
print(f"Total registros: {total}")

from pyspark.sql import functions as F 
from pyspark.sql.window import Window
from pyspark.sql import functions as F, Window


DR = spark.table("global_temp.spr_detalle_reserva").alias("DR")
SR = spark.table("global_temp.spr_reserva").alias("SR")
CP = spark.table("global_temp.spr_conceptos_pagos_siniestros").alias("CP")
TM = spark.table("global_temp.cgr_movimientos_reserva").alias("TM")
SIN = spark.table("global_temp.spr_siniestros").alias("SIN")
CS = spark.table("global_temp.spr_causas_siniestros").alias("CS")
CO = spark.table("global_temp.cgr_origenes").alias("CO")
P = spark.table("global_temp.ppr_polizas").alias("P")
PA = spark.table("global_temp.ppr_afiliados").alias("PA")
AF = spark.table("global_temp.vw_dsdafil_alfa").alias("AF")
ISA = spark.table("global_temp.spr_incapacidades_sol_audit").alias("ISA")
SH = spark.table("global_temp.spr_siniestros_origenes_hist").alias("SH")
CO3 = spark.table("global_temp.cgr_origenes").alias("CO3")
BP = spark.table("global_temp.ben_solicitudes_v").alias("BP")
PS = spark.table("global_temp.spr_detalle_pago_siniestro")
PG = spark.table("global_temp.spr_pagos")
RP = spark.table("global_temp.spr_recuperaciones")
TR = spark.table("global_temp.spr_tipos_recuperaciones")



H4 = spark.table("global_temp.spr_siniestros_origenes_hist").alias("H4")

max_hist = H4.groupBy(F.col("NID_SINIESTRO")) \
            .agg(F.max("NID_SINIESTRO_ORIGEN_HIST").alias("MAX_HIST")) \
            .alias("MH")


hist_nombre = H4.alias("H4") \
    .join(max_hist, 
        (F.col("H4.NID_SINIESTRO_ORIGEN_HIST") == F.col("MH.MAX_HIST")) & 
        (F.col("H4.NID_SINIESTRO") == F.col("MH.NID_SINIESTRO"))) \
    .join(CO3.alias("CO3"), F.col("H4.COD_ORIGEN_SINIESTRO_NEW") == F.col("CO3.COD_ORIGEN")) \
    .select(
        F.col("H4.NID_SINIESTRO").alias("nid_siniestro_hist"),
        F.col("CO3.NOMBRE").alias("nombre_origen")
    )
hist_nombre.createOrReplaceGlobalTempView("temp_hist_nombre")


H2 = spark.table("global_temp.spr_siniestros_origenes_hist").alias("H2")
historico = H2.join(DR, F.col("H2.NID_SINIESTRO") == F.col("DR.NID_SINIESTRO")) \
            .join(SR, F.col("DR.NID_RESERVA") == F.col("SR.NID_RESERVA")) \
            .withColumn("PERIODOR", F.concat_ws("", 
                F.year("H2.FEC_MODIFICACION").cast("string"), 
                F.lpad(F.month("H2.FEC_MODIFICACION").cast("string"), 2, "0"))) \
            .withColumn("X", F.abs(F.col("PERIODOR").cast("int") - F.col("SR.PERIODO_POINT"))) \
            .withColumn("row", F.row_number().over(Window.partitionBy("H2.NID_SINIESTRO").orderBy("X"))) \
            .filter(F.col("row") == 1) \
            .select(F.col("H2.NID_SINIESTRO"), F.col("H2.COD_ORIGEN_SINIESTRO_ANT"))

historico.createOrReplaceGlobalTempView("temp_historico")


ciudad_siniestro = BP.select(F.col("SOLICITUD").alias("NID_SOLICITUD_AFP"),
                            F.col("CIUDAD_SINIESTRO"))
ciudad_siniestro.createOrReplaceGlobalTempView("temp_ciudad")



df_final = DR.join(SR, F.col("DR.NID_RESERVA") == F.col("SR.NID_RESERVA")) \
    .join(CP, F.col("DR.COD_CONCEPTO_PAGO_SINIESTRO") == F.col("CP.COD_CONCEPTO_PAGO_SINIESTRO")) \
    .join(TM, F.col("DR.COD_MOVIMIENTO") == F.col("TM.COD_MOVIMIENTO")) \
    .join(SIN, F.col("DR.NID_SINIESTRO") == F.col("SIN.NID_SINIESTRO")) \
    .join(CS, F.col("SIN.COD_CAUSA_SINIESTRO_AFP") == F.col("CS.COD_CAUSA_SINIESTRO")) \
    .join(CO, F.col("SIN.COD_ORIGEN") == F.col("CO.COD_ORIGEN")) \
    .join(PA, (F.col("SIN.NID_NUMERO_IDENTIFICACION") == F.col("PA.NID_IDENTIFICACION")) &
            (F.col("SIN.COD_TIPO_IDENTIFICACION") == F.col("PA.COD_TIPO_IDENTIFICACION_AFP"))) \
    .join(AF, (F.col("SIN.NID_NUMERO_IDENTIFICACION") == F.col("AF.AFIL_CEDULA")) &
            (F.col("SIN.COD_TIPO_IDENTIFICACION") == F.col("AF.AFIL_CLASEI")) &
            (F.col("AF.AFIL_NUMCUE") == F.col("PA.NID_CUENTA_INDIVIDUAL"))) \
    .join(ISA, (F.col("SIN.NID_SINIESTRO") == F.col("ISA.NID_SINIESTRO")),) \
    .join(P, ((F.coalesce(F.col("ISA.DIA_181"), F.col("SIN.fec_siniestro"))) >= F.col("P.FEC_INICIAL_VIGENCIA")) & 
            ((F.coalesce(F.col("ISA.DIA_181"), F.col("SIN.fec_siniestro"))) <= F.col("P.FEC_FINAL_VIGENCIA"))) \
    .join(ciudad_siniestro.alias("BP"), F.col("SIN.NID_SOLICITUD_AFP") == F.col("BP.NID_SOLICITUD_AFP"), ) \
    .filter(F.col("DR.vlr_reserva") != 0) \
    .selectExpr(
    
        "SR.periodo_point AS periodo_point",
        "CP.COD_CONCEPTO_PAGO_SINIESTRO AS id_categoria_fk",
        "CP.DESCRIPCION AS categoria",
        "'Reserva' AS tipo",
        "DR.COD_MOVIMIENTO AS id_tipo_transaccion_fk",
        "TM.DESCRIPCION AS tipo_transaccion",
        "DR.NID_SINIESTRO AS numero_siniestro",
        "ABS(DR.VLR_RESERVA) AS valor_transaccion",
        "DR.VLR_RESERVA AS valor_transaccion_ajustado",
        "SR.FEC_GENERADO AS fecha_transaccion",
        "COALESCE(ISA.DIA_181, SIN.FEC_SINIESTRO) AS fecha_ocurrencia",
        "CS.DESCRIPCION AS Causa",pf
        "CS.COD_CAUSA_SINIESTRO AS cod_causa",
        "CO.cod_origen",
        "CO.nombre",
        "COALESCE(P.NID_POLIZA, 999) AS POLIZA",
        "COALESCE(SIN.FEC_INICIO_SOLICITUD_AFP, SIN.FEC_SOLICITUD_AFP) AS Fecha_Aviso",
        "AF.AFIL_FECING AS Fecha_Inicio_Vigencia",
        """CASE 
            WHEN BP.CIUDAD_SINIESTRO IS NULL THEN CAST(SIN.COD_DEPARTAMENTO AS INT)
            ELSE CAST(SIN.COD_DEPARTAMENTO AS INT)
        END AS Lugar_Ocurrencia""",
        "AF.AFIL_CEDULA AS Cedula",
        "AF.AFIL_FECNAC AS Fecha_Nacimiento",
        "AF.AFIL_SEXO AS Genero"
    ).distinct()

from commons.StepGold import StepGold
from commons.IncrementalCharge import IncrementalCharge
from pyspark.sql.types import IntegerType
IncrementalCharge = IncrementalCharge(glueContext, spark, "-", "-")
StepGold = StepGold(glueContext, spark)
path = f"s3://alfa-datalake-dev-assets/support_phase_files/procces_ola1.xlsx"
sheet = "gold_phase"
df_tables = ExtractHelper.extract_excel_data(path, sheet)
sheet = "gold_tables"
df_tmp_tables = ExtractHelper.extract_excel_data(path, sheet)

for row in df_tmp_tables.collect():
    table = row["Table"].upper()
    if row["Phase"].upper() == "SILVER":
        path_source =f"s3://alfa-datalake-dev-silver/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}_SILVER_"
    else:
        path_source =f"s3://alfa-datalake-dev-gold/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-CARACT_SINIESTROS-"
        print(table)
    IncrementalCharge.charge_tmp_tables(path_source, table, 'Gold')
		   
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from pyspark.sql import functions as F, Window


class StepGold_old:
    def __init__(self, glue_context, spark):
        self.spark = spark

    def create_component(self):
        """
        This Function to create gold_layer Table.
        """


        decimal_cast = "decimal(19, 0)"
        SIN_COD_CAUSA_AFP = "SIN.cod_causa_siniestro_afp"
        CP_COD_CONCEPTO_PAGO_SINIESTRO = "CP.cod_concepto_pago_siniestro"
        TM_COD_MOVIMIENTO = "TM.cod_movimiento"
        SIN_COD_ORIGEN = "SIN.cod_origen"
        SIN_NID_SINIESTRO ="SIN.nid_siniestro"
        CS_COD_CAUSA_SINIESTRO = "CS.cod_causa_siniestro"
        CO_COD_ORIGEN = "CO.cod_origen"
        SIN_FEC_SINIESTRO = "SIN.fec_siniestro"
        P_FEC_INICIAL_VIGENCIA = "P.fec_inicial_vigencia"
        P_FEC_FINAL_VIGENCIA = "P.fec_final_vigencia"
        DR_VALOR_RESERVA = "DR.vlr_reserva"
        ISA_NID_SINIESTRO ="ISA.nid_siniestro"
        PS_VLR_PAGO ="PS.vlr_pago"
        SIN_FEC_SOLICITUD_AFP = "SIN.fec_solicitud_afp"
        TM_DESCRIPCION ="TM.descripcion"
        ISA_DIA_181 ="ISA_FIL.dia_181"
        CS_DESCRIPCION = "CS.descripcion"
        UNIDAD_DE_NEGOCIO = "Seguridad social"
        P_NID_POLIZA ="P.nid_poliza"
        CO_NOMBRE = "CO.nombre"

        
        SIN_FEC_INICIO_SOLICITUD_AFP ="SIN.fec_inicio_solicitud_afp"
        DR = self.spark.table("global_temp.spr_detalle_reserva")
        SR = self.spark.table("global_temp.spr_reserva")
        CP = self.spark.table("global_temp.spr_conceptos_pagos_siniestros")
        TM = self.spark.table("global_temp.cgr_movimientos_reserva")
        SIN = self.spark.table("global_temp.spr_siniestros").alias("SIN")
        CS = self.spark.table("global_temp.spr_causas_siniestros")
        CO = self.spark.table("global_temp.cgr_origenes")
        AF = self.spark.table("global_temp.vw_dsdafil_alfa").alias("AF")
        BP = self.spark.table("global_temp.ben_solicitudes_v")
        P  = self.spark.table("global_temp.ppr_polizas")
        ISA= self.spark.table("global_temp.spr_incapacidades_sol_audit").alias("ISA")
        AUDIT= self.spark.table("global_temp.spr_incapacidades_sol_audit").alias("AUDIT")
        PS = self.spark.table("global_temp.spr_detalle_pago_siniestro")
        PG = self.spark.table("global_temp.spr_pagos")
        PA = spark.table("global_temp.ppr_afiliados").alias("PA")
        RP = self.spark.table("global_temp.spr_recuperaciones")
        TR = self.spark.table("global_temp.spr_tipos_recuperaciones")

        ciudad_siniestro = BP.select(F.col("SOLICITUD").alias("NID_SOLICITUD_AFP"),
                                    F.col("CIUDAD_SINIESTRO"))

        AF_filtrado = AF \
            .join(SIN, F.col("AF.AFIL_CEDULA") == F.col("SIN.NID_NUMERO_IDENTIFICACION")) \
            .join(PA, F.col("AF.AFIL_NUMCUE") == F.col("PA.NID_CUENTA_INDIVIDUAL")) \
            .filter(F.col("AF.AFIL_CLASEI") == F.col("SIN.COD_TIPO_IDENTIFICACION")) \
            .select("AF.*")  # Nos quedamos solo con columnas de AF para evitar duplicados



        isa_max_df = ISA.groupBy("NID_SINIESTRO").agg(
            F.max("NID_SOLICITUD_AUDIT").alias("MAX_AUDIT_ID")
        ).withColumnRenamed("NID_SINIESTRO","MAX_NID_SINIESTRO")

        ISA_filtrada = ISA.join(
            isa_max_df.alias("max"),
            (F.col("ISA.NID_SINIESTRO") == F.col("max.MAX_NID_SINIESTRO")) &
            (F.col("ISA.NID_SOLICITUD_AUDIT") == F.col("max.MAX_AUDIT_ID"))
        )


        #  “Reserva”
        df_reserva = (
            DR.alias("DR")
            .join(SR.alias("SR"), F.col("DR.nid_reserva") == F.col("SR.nid_reserva"))
            .join(CP.alias("CP"), F.col("DR.cod_concepto_pago_siniestro") == F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO))
            .join(TM.alias("TM"), F.col("DR.cod_movimiento") == F.col(TM_COD_MOVIMIENTO))
            .join(SIN.alias("SIN"), F.col("DR.nid_siniestro") == F.col(SIN_NID_SINIESTRO))
            .join(CS.alias("CS"), F.col(SIN_COD_CAUSA_AFP) == F.col(CS_COD_CAUSA_SINIESTRO))
            .join(CO.alias("CO"), F.col(SIN_COD_ORIGEN) == F.col(CO_COD_ORIGEN))
            .join(PA, (F.col("SIN.NID_NUMERO_IDENTIFICACION") == F.col("PA.NID_IDENTIFICACION")) &
                    (F.col("SIN.COD_TIPO_IDENTIFICACION") == F.col("PA.COD_TIPO_IDENTIFICACION_AFP")))
            .join(ISA_filtrada.alias("ISA_FIL"), F.col("SIN.NID_SINIESTRO") == F.col("ISA_FIL.NID_SINIESTRO"), "left")
            .join(AF_filtrado.alias("AF"), F.col("SIN.NID_NUMERO_IDENTIFICACION") == F.col("AF.AFIL_CEDULA"), "inner")
            .join(
                P.alias("P"),
                ((F.coalesce(F.col("ISA_FIL.dia_181"), F.col("SIN.fec_siniestro"))) >= F.col(P_FEC_INICIAL_VIGENCIA)) &
                ((F.coalesce(F.col("ISA_FIL.dia_181"), F.col("SIN.fec_siniestro")))<= F.col(P_FEC_FINAL_VIGENCIA))
            )
 
            .join(ciudad_siniestro.alias("BP"), F.col("SIN.NID_SOLICITUD_AFP") == F.col("BP.NID_SOLICITUD_AFP"),"left")      
            .filter(F.col(DR_VALOR_RESERVA) != 0)
            .select(
                F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO).cast("string").alias("id_categoria_fk"),
                F.col("CP.descripcion").alias("categoria"),
                F.lit("Reserva").alias("tipo"),
                F.col("DR.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
                F.col(TM_DESCRIPCION).alias("tipo_transaccion"),
                F.col("DR.nid_siniestro").cast("string").alias("numero_siniestro"),
                F.abs(F.col(DR_VALOR_RESERVA) ).alias("valor_transaccion"),
                F.col(DR_VALOR_RESERVA).alias("valor_transaccion_ajustado"),
                F.col("SR.fec_generado").alias("fecha_transaccion"),
                F.col(CS_COD_CAUSA_SINIESTRO).cast("string").alias("cod_causa"),
                F.col(CS_DESCRIPCION).alias("causa"),
                F.col(CO_COD_ORIGEN).cast(decimal_cast).alias("cod_origen"),
                F.col(CO_NOMBRE).alias("origen"),
                F.coalesce(F.col(SIN_FEC_INICIO_SOLICITUD_AFP), F.col(SIN_FEC_SOLICITUD_AFP)).alias("fecha_aviso"),
                F.coalesce(F.col(P_NID_POLIZA), F.lit(999)).alias("poliza"),    
                F.lit("2").cast("string").alias("id_fuente_fk"),
                F.lit("50").cast("string").alias("compania"),
                F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
                F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
                F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
                F.when(
                     F.col("BP.CIUDAD_SINIESTRO").isNull(),
                     F.col("SIN.COD_DEPARTAMENTO").cast("int")
                ).otherwise(
                     F.col("SIN.COD_DEPARTAMENTO").cast("int")
                ).alias("Lugar_ocurrencia"),
                F.col("AF.afil_fecing").alias("fecha_inicio_vigencia"),
                F.col("AF.afil_cedula").alias("cedula"),
                F.col("AF.afil_fecnac").alias("fecha_nacimiento"),
                F.col("AF.afil_sexo").alias("genero")
                
            )
        )






        df_final = df_reserva

        return df_final
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from pyspark.sql import functions as F, Window


class StepGold:
    def __init__(self, glue_context, spark):
        self.spark = spark

    def create_component(self):
        """
        This Function to create gold_layer Table.
        """


        decimal_cast = "decimal(19, 0)"
        SIN_COD_CAUSA_AFP = "SIN.cod_causa_siniestro_afp"
        CP_COD_CONCEPTO_PAGO_SINIESTRO = "CP.cod_concepto_pago_siniestro"
        TM_COD_MOVIMIENTO = "TM.cod_movimiento"
        SIN_COD_ORIGEN = "SIN.cod_origen"
        SIN_NID_SINIESTRO ="SIN.nid_siniestro"
        CS_COD_CAUSA_SINIESTRO = "CS.cod_causa_siniestro"
        CO_COD_ORIGEN = "CO.cod_origen"
        SIN_FEC_SINIESTRO = "SIN.fec_siniestro"
        P_FEC_INICIAL_VIGENCIA = "P.fec_inicial_vigencia"
        P_FEC_FINAL_VIGENCIA = "P.fec_final_vigencia"
        DR_VALOR_RESERVA = "DR.vlr_reserva"
        AFIL_CEDULA ="AF.AFIL_CEDULA"
        PS_VLR_PAGO ="PS.vlr_pago"
        SIN_FEC_SOLICITUD_AFP = "SIN.fec_solicitud_afp"
        TM_DESCRIPCION ="TM.descripcion"
        ISA_DIA_181 ="ISA_FIL.dia_181"
        CS_DESCRIPCION = "CS.descripcion"
        UNIDAD_DE_NEGOCIO = "Seguridad social"
        P_NID_POLIZA ="P.nid_poliza"
        CO_NOMBRE = "CO.nombre"
        PA_NID_IDENTIFICACION = "PA.NID_IDENTIFICACION"
        SIN_NID_NUM_IDEN = "SIN.NID_NUMERO_IDENTIFICACION"
        SIN_COD_TIPO_IDEN = "SIN.COD_TIPO_IDENTIFICACION"
        PA_COD_TIPO_IDEN = "PA.COD_TIPO_IDENTIFICACION_AFP"
        ISA_FIL_NID_SINIESTRO = "ISA_FIL.NID_SINIESTRO"
        SIN_NID_SOL = "SIN.NID_SOLICITUD_AFP"
        BP_NID_SOLICITUD_AFP = "BP.NID_SOLICITUD_AFP"
        BP_CIUDAD_SIN = "BP.CIUDAD_SINIESTRO"
        SIN_COD_DEP = "SIN.COD_DEPARTAMENTO"
        AF_FIL_IN = "AF.afil_fecing"
        AF_CEL = "AF.afil_cedula"
        AF_FEC_NAC = "AF.afil_fecnac"
        AF_SEXO = "AF.afil_sexo"

        
        SIN_FEC_INICIO_SOLICITUD_AFP ="SIN.fec_inicio_solicitud_afp"
        DR = self.spark.table("global_temp.spr_detalle_reserva")
        SR = self.spark.table("global_temp.spr_reserva")
        CP = self.spark.table("global_temp.spr_conceptos_pagos_siniestros")
        TM = self.spark.table("global_temp.cgr_movimientos_reserva")
        SIN = self.spark.table("global_temp.spr_siniestros").alias("SIN")
        CS = self.spark.table("global_temp.spr_causas_siniestros")
        CO = self.spark.table("global_temp.cgr_origenes")
        AF = self.spark.table("global_temp.vw_dsdafil_alfa").alias("AF")
        BP = self.spark.table("global_temp.ben_solicitudes_v")
        P  = self.spark.table("global_temp.ppr_polizas")
        ISA= self.spark.table("global_temp.spr_incapacidades_sol_audit").alias("ISA")
        PS = self.spark.table("global_temp.spr_detalle_pago_siniestro")
        PG = self.spark.table("global_temp.spr_pagos")
        PA = spark.table("global_temp.ppr_afiliados").alias("PA")
        RP = self.spark.table("global_temp.spr_recuperaciones")
        TR = self.spark.table("global_temp.spr_tipos_recuperaciones")

        ciudad_siniestro = BP.select(F.col("SOLICITUD").alias("NID_SOLICITUD_AFP"),
                                    F.col("CIUDAD_SINIESTRO")).distinct()




        isa_max_df = ISA.groupBy("NID_SINIESTRO").agg(
            F.max("NID_SOLICITUD_AUDIT").alias("MAX_AUDIT_ID")
        ).withColumnRenamed("NID_SINIESTRO","MAX_NID_SINIESTRO")

        isa_filtrada = ISA.join(
            isa_max_df.alias("max"),
            (F.col("ISA.NID_SINIESTRO") == F.col("max.MAX_NID_SINIESTRO")) &
            (F.col("ISA.NID_SOLICITUD_AUDIT") == F.col("max.MAX_AUDIT_ID"))
        )




        #  “Pago”
        df_pago = (
            PS.alias("PS")
            .join(PG.alias("PG"), F.col("PG.nid_pago") == F.col("PS.nid_pago"))
            .join(CP.alias("CP"), F.col("PS.cod_concepto_pago_siniestro") == F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO))
            .join(TM.alias("TM"), F.col("PS.cod_movimiento") == F.col(TM_COD_MOVIMIENTO))
            .join(SIN.alias("SIN"), F.col("PS.nid_siniestro") == F.col(SIN_NID_SINIESTRO))
            .join(CS.alias("CS"), F.col(SIN_COD_CAUSA_AFP)  == F.col(CS_COD_CAUSA_SINIESTRO))
            .join(CO.alias("CO"), F.col(SIN_COD_ORIGEN)               == F.col(CO_COD_ORIGEN))
            .join(PA, (F.col(SIN_NID_NUM_IDEN) == F.col(PA_NID_IDENTIFICACION)) &
                    (F.col(SIN_COD_TIPO_IDEN) == F.col(PA_COD_TIPO_IDEN)))
            .join(isa_filtrada.alias("ISA_FIL"), F.col(SIN_NID_SINIESTRO) == F.col(ISA_FIL_NID_SINIESTRO), "left")
           .join(AF, (F.col("SIN.NID_NUMERO_IDENTIFICACION") == F.col("AF.AFIL_CEDULA")) &
                    (F.col("SIN.COD_TIPO_IDENTIFICACION") == F.col("AF.AFIL_CLASEI")) &
                    (F.col("AF.AFIL_NUMCUE") == F.col("PA.NID_CUENTA_INDIVIDUAL")))
            .join(
                P.alias("P"),
                ( F.col(SIN_FEC_SINIESTRO) >= F.col(P_FEC_INICIAL_VIGENCIA)) &
                ( F.col(SIN_FEC_SINIESTRO)<= F.col(P_FEC_FINAL_VIGENCIA))
            )
 
            .join(ciudad_siniestro.alias("BP"), F.col(SIN_NID_SOL) == F.col(BP_NID_SOLICITUD_AFP),"left")
            .filter(F.col(PS_VLR_PAGO) != 0)
            .select(
                F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO).cast("string").alias("id_categoria_fk"),
                F.col("CP.descripcion").alias("categoria"),
                F.lit("Pago").alias("tipo"),
                F.col("PS.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
                F.col(TM_DESCRIPCION).alias("tipo_transaccion"),
                F.col("PS.nid_siniestro").cast("string").alias("numero_siniestro"),
                F.abs(F.col(PS_VLR_PAGO)).alias("valor_transaccion"),
                F.col(PS_VLR_PAGO).alias("valor_transaccion_ajustado"),
                F.col("PG.fec_generado").alias("fecha_transaccion"),
                F.coalesce(F.col(ISA_DIA_181), F.col(SIN_FEC_SINIESTRO)).alias("fecha_ocurrencia"),
                F.col(CS_COD_CAUSA_SINIESTRO).cast("string").alias("cod_causa"),
                F.col(CS_DESCRIPCION).alias("causa"),
                F.col(CO_COD_ORIGEN).cast(decimal_cast).alias("cod_origen"),
                F.col(CO_NOMBRE).alias("origen"),
                F.coalesce(F.col(SIN_FEC_INICIO_SOLICITUD_AFP), F.col(SIN_FEC_SOLICITUD_AFP)).alias("fecha_aviso"),
                F.coalesce(F.col(P_NID_POLIZA), F.lit(999)).alias("poliza"),
                F.lit("2").cast("string").alias("id_fuente_fk"),
                F.lit("50").cast("string").alias("compania"),
                F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
                F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
                F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.when(
                     F.col(BP_CIUDAD_SIN).isNull(),
                     F.col(SIN_COD_DEP).cast("int")
                ).otherwise(
                     F.col(SIN_COD_DEP).cast("int")
                ).alias("Lugar_ocurrencia")



            )
        )





        df_final = df_pago


        return df_final
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from pyspark.sql import functions as F, Window


class StepGold:
    def __init__(self, glue_context, spark):
        self.spark = spark

    def create_component(self):
        """
        This Function to create gold_layer Table.
        """


        decimal_cast = "decimal(19, 0)"
        SIN_COD_CAUSA_AFP = "SIN.cod_causa_siniestro_afp"
        CP_COD_CONCEPTO_PAGO_SINIESTRO = "CP.cod_concepto_pago_siniestro"
        TM_COD_MOVIMIENTO = "TM.cod_movimiento"
        SIN_COD_ORIGEN = "SIN.cod_origen"
        SIN_NID_SINIESTRO ="SIN.nid_siniestro"
        CS_COD_CAUSA_SINIESTRO = "CS.cod_causa_siniestro"
        CO_COD_ORIGEN = "CO.cod_origen"
        SIN_FEC_SINIESTRO = "SIN.fec_siniestro"
        P_FEC_INICIAL_VIGENCIA = "P.fec_inicial_vigencia"
        P_FEC_FINAL_VIGENCIA = "P.fec_final_vigencia"
        DR_VALOR_RESERVA = "DR.vlr_reserva"
        AFIL_CEDULA ="AF.AFIL_CEDULA"
        PS_VLR_PAGO ="PS.vlr_pago"
        SIN_FEC_SOLICITUD_AFP = "SIN.fec_solicitud_afp"
        TM_DESCRIPCION ="TM.descripcion"
        ISA_DIA_181 ="ISA_FIL.dia_181"
        CS_DESCRIPCION = "CS.descripcion"
        UNIDAD_DE_NEGOCIO = "Seguridad social"
        P_NID_POLIZA ="P.nid_poliza"
        CO_NOMBRE = "CO.nombre"
        PA_NID_IDENTIFICACION = "PA.NID_IDENTIFICACION"
        SIN_NID_NUM_IDEN = "SIN.NID_NUMERO_IDENTIFICACION"
        SIN_COD_TIPO_IDEN = "SIN.COD_TIPO_IDENTIFICACION"
        PA_COD_TIPO_IDEN = "PA.COD_TIPO_IDENTIFICACION_AFP"
        ISA_FIL_NID_SINIESTRO = "ISA_FIL.NID_SINIESTRO"
        SIN_NID_SOL = "SIN.NID_SOLICITUD_AFP"
        BP_NID_SOLICITUD_AFP = "BP.NID_SOLICITUD_AFP"
        BP_CIUDAD_SIN = "BP.CIUDAD_SINIESTRO"
        SIN_COD_DEP = "SIN.COD_DEPARTAMENTO"
        AF_FIL_IN = "AF.afil_fecing"
        AF_CLASEI = "AF.AFIL_CLASEI"
        AF_FEC_NAC = "AF.afil_fecnac"
        AF_SEXO = "AF.afil_sexo"
        AF_NUMCUEN = "AF.AFIL_NUMCUE"
        PA_CUENTA_IND = "PA.NID_CUENTA_INDIVIDUAL"
        DESC_CAUSA = "Otras causas"
   

        
        SIN_FEC_INICIO_SOLICITUD_AFP ="SIN.fec_inicio_solicitud_afp"
        DR = self.spark.table("global_temp.spr_detalle_reserva")
        SR = self.spark.table("global_temp.spr_reserva")
        CP = self.spark.table("global_temp.spr_conceptos_pagos_siniestros")
        TM = self.spark.table("global_temp.cgr_movimientos_reserva")
        SIN = self.spark.table("global_temp.spr_siniestros").alias("SIN")
        CS = self.spark.table("global_temp.spr_causas_siniestros")
        CO = self.spark.table("global_temp.cgr_origenes")
        AF = self.spark.table("global_temp.vw_dsdafil_alfa").alias("AF")
        BP = self.spark.table("global_temp.ben_solicitudes_v")
        P  = self.spark.table("global_temp.ppr_polizas")
        ISA= self.spark.table("global_temp.spr_incapacidades_sol_audit").alias("ISA")
        PS = self.spark.table("global_temp.spr_detalle_pago_siniestro")
        PG = self.spark.table("global_temp.spr_pagos")
        PA = self.spark.table("global_temp.ppr_afiliados").alias("PA")
        RP = self.spark.table("global_temp.spr_recuperaciones")
        TR = self.spark.table("global_temp.spr_tipos_recuperaciones")
        DT = self.spark.table("global_temp.spr_detalle_recuperaciones").alias("DT")

        ciudad_siniestro = BP.select(F.col("SOLICITUD").alias("NID_SOLICITUD_AFP"),
                                    F.col("CIUDAD_SINIESTRO")).distinct()


        # Subconsulta H2
        H2 = self.spark.table("global_temp.spr_siniestros_origenes_hist").alias("H2")
        RP = RP.alias("RP")  # Asegúrate de que RP ya esté cargado en el pipeline

        H2_periodo = H2.withColumn(
            "PERIDOR",
            F.concat(
                F.year(F.col("H2.FEC_MODIFICACION")).cast("string"),
                F.lpad(F.month(F.col("H2.FEC_MODIFICACION")).cast("string"), 2, "0")
            )
        )

        # JOIN con RP por NID_SINIESTRO
        H2_con_rp = H2_periodo.join(
            RP,
            F.col("H2.NID_SINIESTRO") == F.col("RP.NID_SINIESTRO")
        ).withColumn(
            "X",
            F.abs(
                F.col("PERIDOR").cast("int") - F.col("RP.PERIODO_POINT").cast("int")
            )
        )

        # Seleccionar el registro con menor diferencia por siniestro
        window_spec = Window.partitionBy("H2.NID_SINIESTRO").orderBy("X")

        HISTORICO = (
            H2_con_rp
            .withColumn("row", F.row_number().over(window_spec))
            .filter(F.col("row") == 1)
            .select(
                F.col("H2.NID_SINIESTRO").alias("HIST_NID_SINIESTRO"),
                F.col("H2.COD_ORIGEN_SINIESTRO_ANT").alias("HIST_COD_ORIGEN_SINIESTRO_ANT")
            )
            .alias("HISTORICO")
        )

        # Tabla SH
        SH = self.spark.table("global_temp.spr_siniestros_origenes_hist").alias("SH")

        # JOIN SH con subconsulta
        SH_JOINED = SH.join(
            HISTORICO,
            (F.col("SH.NID_SINIESTRO") == F.col("HISTORICO.HIST_NID_SINIESTRO")) &
            (F.col("SH.COD_ORIGEN_SINIESTRO_ANT") == F.col("HISTORICO.HIST_COD_ORIGEN_SINIESTRO_ANT")),
            "left"
        ).alias("SH_JOINED")

        # Tabla CGR_ORIGENES
        CO2 = self.spark.table("global_temp.CGR_ORIGENES").alias("CO2")

        # JOIN con CO2
        CO2_JOINED = CO2.alias("CO2")


        isa_max_df = ISA.groupBy("NID_SINIESTRO").agg(
            F.max("NID_SOLICITUD_AUDIT").alias("MAX_AUDIT_ID")
        ).withColumnRenamed("NID_SINIESTRO","MAX_NID_SINIESTRO")

        isa_filtrada = ISA.join(
            isa_max_df.alias("max"),
            (F.col("ISA.NID_SINIESTRO") == F.col("max.MAX_NID_SINIESTRO")) &
            (F.col("ISA.NID_SOLICITUD_AUDIT") == F.col("max.MAX_AUDIT_ID"))
        )






        #  “Reserva”
        df_reserva = (
            DR.alias("DR")
            .join(SR.alias("SR"), F.col("DR.nid_reserva") == F.col("SR.nid_reserva"))
            .join(CP.alias("CP"), F.col("DR.cod_concepto_pago_siniestro") == F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO))
            .join(TM.alias("TM"), F.col("DR.cod_movimiento") == F.col(TM_COD_MOVIMIENTO))
            .join(SIN.alias("SIN"), F.col("DR.nid_siniestro") == F.col(SIN_NID_SINIESTRO))
            .join(CS.alias("CS"), F.col(SIN_COD_CAUSA_AFP) == F.col(CS_COD_CAUSA_SINIESTRO),"left")
            .join(CO.alias("CO"), F.col(SIN_COD_ORIGEN) == F.col(CO_COD_ORIGEN))
            .join(PA, (F.col(SIN_NID_NUM_IDEN) == F.col(PA_NID_IDENTIFICACION)) &
                    (F.col(SIN_COD_TIPO_IDEN) == F.col(PA_COD_TIPO_IDEN)),"left")
            .join(isa_filtrada.alias("ISA_FIL"), F.col(SIN_NID_SINIESTRO) == F.col(ISA_FIL_NID_SINIESTRO), "left")
            .join(AF, (F.col(SIN_NID_NUM_IDEN) == F.col(AFIL_CEDULA)) &
                    (F.col(SIN_COD_TIPO_IDEN) == F.col(AF_CLASEI)) &
                    (F.col(AF_NUMCUEN) == F.col(PA_CUENTA_IND)),"left")
            .join(
                P.alias("P"),
                ( F.col(SIN_FEC_SINIESTRO) >= F.col(P_FEC_INICIAL_VIGENCIA)) &
                ( F.col(SIN_FEC_SINIESTRO)<= F.col(P_FEC_FINAL_VIGENCIA)),"left"
            )
 
            .join(ciudad_siniestro.alias("BP"), F.col(SIN_NID_SOL) == F.col(BP_NID_SOLICITUD_AFP),"left")      

            .join(SH_JOINED, F.col("SIN.NID_SINIESTRO") == F.col("SH_JOINED.NID_SINIESTRO"), "left")
            .join(CO2_JOINED, F.col("CO2.COD_ORIGEN") == F.col("SH_JOINED.COD_ORIGEN_SINIESTRO_ANT"), "left")

            .filter(F.col(DR_VALOR_RESERVA) != 0)
            .select(
                F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO).cast("string").alias("id_categoria_fk"),
                F.col("CP.descripcion").alias("categoria"),
                F.lit("Reserva").alias("tipo"),
                F.col("DR.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
                F.col(TM_DESCRIPCION).alias("tipo_transaccion"),
                F.col("DR.nid_siniestro").cast("string").alias("numero_siniestro"),
                F.abs(F.col(DR_VALOR_RESERVA) ).alias("valor_transaccion"),
                F.col(DR_VALOR_RESERVA).alias("valor_transaccion_ajustado"),
                F.col("SR.fec_generado").alias("fecha_transaccion"),
                F.when(
                    F.col(CO_COD_ORIGEN) == '2' ,
                    F.col(ISA_DIA_181)
                ).otherwise(
                    F.col(SIN_FEC_SINIESTRO)
                ).alias("fecha_ocurrencia"),
                F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
                F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),
                F.col(CO_COD_ORIGEN).cast(decimal_cast).alias("cod_origen"),
                F.col(CO_NOMBRE).alias("origen"),
                F.coalesce(F.col(SIN_FEC_INICIO_SOLICITUD_AFP), F.col(SIN_FEC_SOLICITUD_AFP)).alias("fecha_aviso"),
                F.coalesce(F.col(P_NID_POLIZA), F.lit(999)).alias("poliza"),
                F.lit("2").cast("string").alias("id_fuente_fk"),
                F.lit("50").cast("string").alias("compania"),
                F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
                F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
                F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.when(
                     F.col(BP_CIUDAD_SIN).isNull(),
                     F.col(SIN_COD_DEP).cast("int")
                ).otherwise(
                     F.col(SIN_COD_DEP).cast("int")
                ).alias("lugar_ocurrencia"),
                F.col(AF_FIL_IN).alias("fecha_inicio_vigencia"),
                F.col(AFIL_CEDULA).alias("cedula"),
                F.col(AF_FEC_NAC).alias("fecha_nacimiento"),
                F.col(AF_SEXO).alias("genero")                
            )
        )





        df_final = df_reserva


        return df_final
transacciones = StepGold(glueContext, spark)
df_reservas = transacciones.create_component()
#df_reservas.count()
df_reservas.count()
df_sin_causa = df_reservas.filter(
    (F.col("tipo") == "Recuperaciones")&
    (F.col("cod_causa") == "111")&
    (F.col("fecha_transaccion").between("2017-01-01","2024-07-31"))
)
df_sin_causa.show()
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from pyspark.sql import functions as F, Window


class StepGold:
    def __init__(self, glue_context, spark):
        self.spark = spark

    def create_component(self):
        """
        This Function to create gold_layer Table.
        """


        decimal_cast = "decimal(19, 0)"
        SIN_COD_CAUSA_AFP = "SIN.cod_causa_siniestro_afp"
        CP_COD_CONCEPTO_PAGO_SINIESTRO = "CP.cod_concepto_pago_siniestro"
        TM_COD_MOVIMIENTO = "TM.cod_movimiento"
        SIN_COD_ORIGEN = "SIN.cod_origen"
        SIN_NID_SINIESTRO ="SIN.nid_siniestro"
        CS_COD_CAUSA_SINIESTRO = "CS.cod_causa_siniestro"
        CO_COD_ORIGEN = "CO.cod_origen"
        SIN_FEC_SINIESTRO = "SIN.fec_siniestro"
        P_FEC_INICIAL_VIGENCIA = "P.fec_inicial_vigencia"
        P_FEC_FINAL_VIGENCIA = "P.fec_final_vigencia"
        DR_VALOR_RESERVA = "DR.vlr_reserva"
        AFIL_CEDULA ="AF.AFIL_CEDULA"
        PS_VLR_PAGO ="PS.vlr_pago"
        SIN_FEC_SOLICITUD_AFP = "SIN.fec_solicitud_afp"
        TM_DESCRIPCION ="TM.descripcion"
        ISA_DIA_181 ="ISA_FIL.dia_181"
        CS_DESCRIPCION = "CS.descripcion"
        UNIDAD_DE_NEGOCIO = "Seguridad social"
        P_NID_POLIZA ="P.nid_poliza"
        CO_NOMBRE = "CO.nombre"
        PA_NID_IDENTIFICACION = "PA.NID_IDENTIFICACION"
        SIN_NID_NUM_IDEN = "SIN.NID_NUMERO_IDENTIFICACION"
        SIN_COD_TIPO_IDEN = "SIN.COD_TIPO_IDENTIFICACION"
        PA_COD_TIPO_IDEN = "PA.COD_TIPO_IDENTIFICACION_AFP"
        ISA_FIL_NID_SINIESTRO = "ISA_FIL.NID_SINIESTRO"
        SIN_NID_SOL = "SIN.NID_SOLICITUD_AFP"
        BP_NID_SOLICITUD_AFP = "BP.NID_SOLICITUD_AFP"
        BP_CIUDAD_SIN = "BP.CIUDAD_SINIESTRO"
        SIN_COD_DEP = "SIN.COD_DEPARTAMENTO"
        AF_FIL_IN = "AF.afil_fecing"
        AF_CEL = "AF.afil_cedula"
        AF_FEC_NAC = "AF.afil_fecnac"
        AF_SEXO = "AF.afil_sexo"

        
        SIN_FEC_INICIO_SOLICITUD_AFP ="SIN.fec_inicio_solicitud_afp"
        DR = self.spark.table("global_temp.spr_detalle_reserva")
        SR = self.spark.table("global_temp.spr_reserva")
        CP = self.spark.table("global_temp.spr_conceptos_pagos_siniestros")
        TM = self.spark.table("global_temp.cgr_movimientos_reserva")
        SIN = self.spark.table("global_temp.spr_siniestros").alias("SIN")
        CS = self.spark.table("global_temp.spr_causas_siniestros")
        CO = self.spark.table("global_temp.cgr_origenes")
        AF = self.spark.table("global_temp.vw_dsdafil_alfa").alias("AF")
        BP = self.spark.table("global_temp.ben_solicitudes_v")
        P  = self.spark.table("global_temp.ppr_polizas")
        ISA= self.spark.table("global_temp.spr_incapacidades_sol_audit").alias("ISA")
        PS = self.spark.table("global_temp.spr_detalle_pago_siniestro")
        PG = self.spark.table("global_temp.spr_pagos")
        PA = spark.table("global_temp.ppr_afiliados").alias("PA")
        RP = self.spark.table("global_temp.spr_recuperaciones")
        TR = self.spark.table("global_temp.spr_tipos_recuperaciones")

        ciudad_siniestro = BP.select(F.col("SOLICITUD").alias("NID_SOLICITUD_AFP"),
                                    F.col("CIUDAD_SINIESTRO")).distinct()




        isa_max_df = ISA.groupBy("NID_SINIESTRO").agg(
            F.max("NID_SOLICITUD_AUDIT").alias("MAX_AUDIT_ID")
        ).withColumnRenamed("NID_SINIESTRO","MAX_NID_SINIESTRO")

        isa_filtrada = ISA.join(
            isa_max_df.alias("max"),
            (F.col("ISA.NID_SINIESTRO") == F.col("max.MAX_NID_SINIESTRO")) &
            (F.col("ISA.NID_SOLICITUD_AUDIT") == F.col("max.MAX_AUDIT_ID"))
        )

        af_nulos = AF.filter(
            F.col("afil_cedula").isNull() | F.col("afil_clasei").isNull()
        )

        # Verificar cuántas filas tienen valores nulos en las columnas clave en SIN
        sin_nulos = SIN.filter(
            F.col("NID_NUMERO_IDENTIFICACION").isNull() | F.col("COD_TIPO_IDENTIFICACION").isNull()
        )
        
        ap_nulos = PA.filter(
            F.col("NID_CUENTA_INDIVIDUAL").isNull() | F.col("COD_TIPO_IDENTIFICACION_AFP").isNull() | F.col("NID_IDENTIFICACION").isNull()
        )

        print(f"Filas con nulos en AF: {af_nulos.count()}")
        print(f"Filas con nulos en SIN: {sin_nulos.count()}")
        print(f"Filas con nulos en PA: {ap_nulos.count()}")


        PA = PA.filter(F.col("NID_CUENTA_INDIVIDUAL").isNotNull()  | F.col("COD_TIPO_IDENTIFICACION_AFP").isNotNull() | F.col("NID_IDENTIFICACION").isNotNull())

        nids_validos = SIN.select(F.col("NID_NUMERO_IDENTIFICACION"),F.col("COD_TIPO_IDENTIFICACION")).distinct().alias("IDS")
        #AF = AF.dropDuplicates(["AFIL_CEDULA", "AFIL_CLASEI"]).alias("AF")
        
        print(f"filas antes{AF.count()}")
        # Filtrar AF usando semi join para emular el IN (SELECT...)
        af_filtrado = AF.join(
            nids_validos,
            (F.col("AF.afil_cedula") == F.col("IDS.NID_NUMERO_IDENTIFICACION")) &
            (F.col("AF.afil_clasei") == F.col("IDS.COD_TIPO_IDENTIFICACION")),
            "semi"
        ).filter(
            # Asegurarse de que las columnas no sean nulas en el semi join
            F.col("AF.afil_cedula").isNotNull() &
            F.col("AF.afil_clasei").isNotNull()
        )
        print(f"filas despues {af_filtrado.count()}")

        PA = PA.filter(F.col("PA.NID_CUENTA_INDIVIDUAL").isNotNull())
        #  “Recuperaciones”
        df_recup = (
            RP.alias("RP")
            .join(TR.alias("TR"), F.col("RP.cod_tipo_recuperacion") == F.col("TR.cod_tipo_recuperacion"))
            .join(TM.alias("TM"), F.col("RP.cod_movimiento") == F.col(TM_COD_MOVIMIENTO))
            .join(SIN.alias("SIN"), F.col("RP.nid_siniestro")    == F.col(SIN_NID_SINIESTRO))
            .join(CS.alias("CS"), F.col(SIN_COD_CAUSA_AFP) == F.col(CS_COD_CAUSA_SINIESTRO))
            .join(CO.alias("CO"), F.col(SIN_COD_ORIGEN)               == F.col(CO_COD_ORIGEN))
            .join(PA, (F.col(SIN_NID_NUM_IDEN) == F.col(PA_NID_IDENTIFICACION)) &
                    (F.col(SIN_COD_TIPO_IDEN) == F.col(PA_COD_TIPO_IDEN)))
            .join(isa_filtrada.alias("ISA_FIL"), F.col(SIN_NID_SINIESTRO) == F.col(ISA_FIL_NID_SINIESTRO), "left")
            .join(AF, (F.col("SIN.NID_NUMERO_IDENTIFICACION") == F.col("AF.AFIL_CEDULA")) &
                    (F.col("SIN.COD_TIPO_IDENTIFICACION") == F.col("AF.AFIL_CLASEI")) &
                    (F.col("AF.AFIL_NUMCUE") == F.col("PA.NID_CUENTA_INDIVIDUAL")))

            .join(
                P.alias("P"),
                (F.col(SIN_FEC_SINIESTRO) >= F.col(P_FEC_INICIAL_VIGENCIA)) &
                ( F.col(SIN_FEC_SINIESTRO)<= F.col(P_FEC_FINAL_VIGENCIA))
            )
 
            .join(ciudad_siniestro.alias("BP"), F.col(SIN_NID_SOL) == F.col(BP_NID_SOLICITUD_AFP),"left")
            .select(
                F.col("TR.cod_tipo_recuperacion").cast("string").alias("id_categoria_fk"),
                F.col("TR.nombre").alias("categoria"),
                F.lit("Recuperaciones").alias("tipo"),
                F.col("RP.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
                F.col(TM_DESCRIPCION).alias("tipo_transaccion"),
                F.col("RP.nid_siniestro").cast("string").alias("numero_siniestro"),
                F.abs(F.col("RP.vlr_recuperacion")).alias("valor_transaccion"),
                F.col("RP.vlr_recuperacion").alias("valor_transaccion_ajustado"),
                F.col("RP.fec_creacion_registro").alias("fecha_transaccion"),
                F.coalesce(F.col(ISA_DIA_181), F.col(SIN_FEC_SINIESTRO)).alias("fecha_ocurrencia"),  
                F.col(CS_COD_CAUSA_SINIESTRO).cast("string").alias("cod_causa"),
                F.col(CS_DESCRIPCION).alias("causa"),
                F.col(CO_COD_ORIGEN).cast(decimal_cast).alias("cod_origen"),
                F.col(CO_NOMBRE).alias("origen"),
                F.coalesce(F.col(SIN_FEC_INICIO_SOLICITUD_AFP), F.col(SIN_FEC_SOLICITUD_AFP)).alias("fecha_aviso"),
                F.coalesce(F.col(P_NID_POLIZA), F.lit(999)).alias("poliza"),
                F.lit("2").cast("string").alias("id_fuente_fk"),
                F.lit("50").cast("string").alias("compania"),
                F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
                F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
                F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.when(
                     F.col(BP_CIUDAD_SIN).isNull(),
                     F.col(SIN_COD_DEP).cast("int")
                ).otherwise(
                     F.col(SIN_COD_DEP).cast("int")
                ).alias("Lugar_ocurrencia"),
                F.col(AF_FIL_IN).alias("fecha_inicio_vigencia"),
                F.col(AF_CEL).alias("cedula"),
                F.col(AF_FEC_NAC).alias("fecha_nacimiento"),
                F.col(AF_SEXO).alias("genero")
            )
        )



        df_final =df_recup


        return df_final

transacciones = StepGold(glueContext, spark)
df_reservas = transacciones.create_component()
df_reservas.count()

#todooooooooooooooooooo
AFIL_CEDULA ="AF.AFIL_CEDULA"

transacciones = StepGold_test(glueContext, spark)
df_reservas = transacciones.create_component()

from awsglue import DynamicFrame
from datetime import datetime
from pyspark.sql import functions as F
import pandas as pd
import boto3
from botocore.exceptions import ClientError
def save_files(df, path,glue_context):
    """
    This function save df in parquet files.

    arg:
        df: Dataframe to save in parquet files.
        tb_name: Name of table to save (save in path s3)
        path: path to save files.
    retunr:
        response: response of the execution
    """
     
    current_date = datetime.now()
    day = current_date.day
    month = current_date.month
    year = current_date.year 
    df_dynamic = DynamicFrame.fromDF(df , glue_context, "nested")
    response = glue_context.write_dynamic_frame.from_options(
        frame=df_dynamic, 
        connection_type="s3", 
        format="glueparquet", 
        connection_options={"path": f"{path}{year}/{month}/{day}", "partitionKeys": []},
        format_options={"compression": "snappy"}, 
        transformation_ctx="AmazonS3_write"
    )
    return response
save_files(df_reservas,path,glueContext)
path = "s3://alfa-datalake-dev-silver/SIPREN/TRANSFORMATION/prueba_causas/"
df_reservas.count()
df_test =df_final.filter(F.col("numero_siniestro")==201701196)
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from pyspark.sql import functions as F



class StepGoldBodega:
    def __init__(self, glue_context, spark):
        self.spark = spark

    def create_component(self):
        """
        This Function to create gold_layer Table.
        """

        B_NID_SINIESTRO = 'B.NID_SINIESTRO'


        
        DF_BODEGA = self.spark.sql("select * from global_temp.bodega_plus")
        DF_DATO_BASICO = self.spark.sql("select * from global_temp.snr_dato_basico")
        DF_CAUSAS = self.spark.sql("select * from global_temp.snr_causa")
        DF_ORIGEN = self.spark.sql("select * from global_temp.snr_origen")
        DF_POLIZAS = self.spark.sql("select * from global_temp.rcd_polizas")
        DF_CU_PERSONAS = self.spark.sql("select * from global_temp.CU_PERSONA_CLIENTE")
        DF_VW_CONSULTA = self.spark.sql("select * from global_temp.VW_CONSULTA_CLIENTE")
        DF_VW_LOCALIZACIONES = self.spark.sql("select * from global_temp.VW_LOCALIZACIONES")

       
        DF_RESULT = DF_BODEGA.alias('B').join(
            DF_DATO_BASICO.alias('D'), F.col(B_NID_SINIESTRO) == F.col('D.ID_SINIESTRO'), 'left'
            ).join(
            DF_CAUSAS.alias('c'), F.col('D.COD_CAUSA_SINIESTRO') == F.col('c.ID_CAUSAL'), 'left'
            ).join(
            DF_ORIGEN.alias('o'), F.col('D.COD_ORIGEN_SINIESTRO') == F.col('o.COD_ORIGEN_SINIESTRO'), 'left'
            ).join(
            DF_POLIZAS.alias('p'),  F.coalesce(F.col('B.fecha_siniestro'), F.to_date(F.concat_ws('', F.col('B.periodo_point'), F.lit('01')), 'yyyyMMdd')).between(F.col('p.FEC_INICIAL_VIGENCIA'),F.col('p.FEC_FINAL_VIGENCIA'))
            ).join(
            DF_CU_PERSONAS.alias('pcl'), F.col("pcl.NUM_PERSONA") == F.col("D.NUM_PERSONA"), "left"
            ).join(
            DF_VW_CONSULTA.alias('af'), F.col("pcl.NUM_IDENTIFICACION") == F.col("af.NID_NUMERO_IDENTIFICACION"),"left"
            ).join(
            DF_VW_LOCALIZACIONES.alias('af2'), F.col("PCL.NUM_IDENTIFICACION") == F.col("af2.NID_NUMERO_IDENTIFICACION"),"left"
            )
            

    
        df_pagos = DF_RESULT.selectExpr(
            "'2'  AS id_categoria_fk",
  
            "b.NID_SINIESTRO AS numero_siniestro",
    
            "31 AS id_ramo_fk"

            







        
        )

        df_final = df_pagos


        return df_final
transacciones = StepGoldBodega(glueContext, spark)
df_reservas = transacciones.create_component()
df_reservas.count()
df_reservas.filter(F.col("numero_siniestro") == "99999999").show()

df_reservas.show()
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from pyspark.sql import functions as F, Window


class StepGold:
    def __init__(self, glue_context, spark):
        self.spark = spark

    def create_component(self):
        """
        This Function to create gold_layer Table.
        """


        decimal_cast = "decimal(19, 0)"
        SIN_COD_CAUSA_AFP = "SIN.cod_causa_siniestro_afp"
        CP_COD_CONCEPTO_PAGO_SINIESTRO = "CP.cod_concepto_pago_siniestro"
        TM_COD_MOVIMIENTO = "TM.cod_movimiento"
        SIN_COD_ORIGEN = "SIN.cod_origen"
        SIN_NID_SINIESTRO ="SIN.nid_siniestro"
        CS_COD_CAUSA_SINIESTRO = "CS.cod_causa_siniestro"
        CO_COD_ORIGEN = "CO.cod_origen"
        SIN_FEC_SINIESTRO = "SIN.fec_siniestro"
        P_FEC_INICIAL_VIGENCIA = "P.fec_inicial_vigencia"
        P_FEC_FINAL_VIGENCIA = "P.fec_final_vigencia"
        DR_VALOR_RESERVA = "DR.vlr_reserva"
        AFIL_CEDULA ="AF.AFIL_CEDULA"
        PS_VLR_PAGO ="PS.vlr_pago"
        SIN_FEC_SOLICITUD_AFP = "SIN.fec_solicitud_afp"
        TM_DESCRIPCION ="TM.descripcion"
        ISA_DIA_181 ="ISA_FIL.dia_181"
        CS_DESCRIPCION = "CS.descripcion"
        UNIDAD_DE_NEGOCIO = "Seguridad social"
        P_NID_POLIZA ="P.nid_poliza"
        CO_NOMBRE = "CO.nombre"
        PA_NID_IDENTIFICACION = "PA.NID_IDENTIFICACION"
        SIN_NID_NUM_IDEN = "SIN.NID_NUMERO_IDENTIFICACION"
        SIN_COD_TIPO_IDEN = "SIN.COD_TIPO_IDENTIFICACION"
        PA_COD_TIPO_IDEN = "PA.COD_TIPO_IDENTIFICACION_AFP"
        ISA_FIL_NID_SINIESTRO = "ISA_FIL.NID_SINIESTRO"
        SIN_NID_SOL = "SIN.NID_SOLICITUD_AFP"
        BP_NID_SOLICITUD_AFP = "BP.NID_SOLICITUD_AFP"
        BP_CIUDAD_SIN = "BP.CIUDAD_SINIESTRO"
        SIN_COD_DEP = "SIN.COD_DEPARTAMENTO"
        AF_FIL_IN = "AF.afil_fecing"
        AF_CLASEI = "AF.AFIL_CLASEI"
        AF_FEC_NAC = "AF.afil_fecnac"
        AF_SEXO = "AF.afil_sexo"
        AF_NUMCUEN = "AF.AFIL_NUMCUE"
        PA_CUENTA_IND = "PA.NID_CUENTA_INDIVIDUAL"
        DESC_CAUSA = "Otras causas"
   

        
        SIN_FEC_INICIO_SOLICITUD_AFP ="SIN.fec_inicio_solicitud_afp"
        DR = self.spark.table("global_temp.spr_detalle_reserva")
        SR = self.spark.table("global_temp.spr_reserva")
        CP = self.spark.table("global_temp.spr_conceptos_pagos_siniestros")
        TM = self.spark.table("global_temp.cgr_movimientos_reserva")
        SIN = self.spark.table("global_temp.spr_siniestros").alias("SIN")
        CS = self.spark.table("global_temp.spr_causas_siniestros")
        CO = self.spark.table("global_temp.cgr_origenes").alias("CO")
        AF = self.spark.table("global_temp.vw_dsdafil_alfa").alias("AF")
        BP = self.spark.table("global_temp.ben_solicitudes_v")
        P  = self.spark.table("global_temp.ppr_polizas")
        ISA= self.spark.table("global_temp.spr_incapacidades_sol_audit").alias("ISA")
        PS = self.spark.table("global_temp.spr_detalle_pago_siniestro")
        PG = self.spark.table("global_temp.spr_pagos")
        PA = self.spark.table("global_temp.ppr_afiliados").alias("PA")
        RP = self.spark.table("global_temp.spr_recuperaciones").alias("RP")
        TR = self.spark.table("global_temp.spr_tipos_recuperaciones")
        DT = self.spark.table("global_temp.spr_detalle_recuperaciones").alias("DT")
        H2 = spark.table("global_temp.Spr_siniestros_origenes_hist").alias("H2")

        ciudad_siniestro = BP.select(F.col("SOLICITUD").alias("NID_SOLICITUD_AFP"),
                                    F.col("CIUDAD_SINIESTRO")).distinct()


        window_origen = Window.partitionBy("H2.NID_SINIESTRO").orderBy(F.col("H2.NID_SINIESTRO_ORIGEN_HIST").desc())

        OSF = (
            H2.join(CO, F.col("H2.COD_ORIGEN_SINIESTRO_NEW") == F.col("CO.COD_ORIGEN"), "left")
            .withColumn("RN", F.row_number().over(window_origen))
            .filter(F.col("RN") == 1)
            .select(
                F.col("H2.NID_SINIESTRO"),
                F.col("CO.NOMBRE").alias("nombre_origen_final")
            )
            .alias("OSF")
        )




        diff_expr = F.abs(
            F.expr("int(date_format(H2.FEC_MODIFICACION, 'yyyyMM'))") - F.col("RP.PERIODO_POINT").cast("int")
        )

        # Definir ventana con partición por siniestro y orden por diferencia
        window_spec = Window.partitionBy("H2.NID_SINIESTRO").orderBy(diff_expr)

        # Construir el DataFrame SH (subquery filtrada)
        SH = (
            H2.join(RP, F.col("H2.NID_SINIESTRO") == F.col("RP.NID_SINIESTRO"), "inner")
            .withColumn("RN", F.row_number().over(window_spec))
            .filter(F.col("RN") == 1)
            .select(
                F.col("H2.NID_SINIESTRO"),
                F.col("H2.COD_ORIGEN_SINIESTRO_ANT"),
                F.col("H2.COD_ORIGEN_SINIESTRO_NEW"),
                F.col("RP.PERIODO_POINT")
            )
            .alias("SH")
        )


        isa_max_df = ISA.groupBy("NID_SINIESTRO").agg(
            F.max("NID_SOLICITUD_AUDIT").alias("MAX_AUDIT_ID")
        ).withColumnRenamed("NID_SINIESTRO","MAX_NID_SINIESTRO")

        isa_filtrada = ISA.join(
            isa_max_df.alias("max"),
            (F.col("ISA.NID_SINIESTRO") == F.col("max.MAX_NID_SINIESTRO")) &
            (F.col("ISA.NID_SOLICITUD_AUDIT") == F.col("max.MAX_AUDIT_ID"))
        )




        #  “Recuperaciones”
        df_recup = (
            DT.alias("DT")
            .join(RP.alias("RP"),F.col("RP.nid_recuperacion") == F.col("DT.nid_recuperacion"),"left")
            .join(TR.alias("TR"), F.col("RP.cod_tipo_recuperacion") == F.col("TR.cod_tipo_recuperacion"))
            .join(TM.alias("TM"), F.col("RP.cod_movimiento") == F.col(TM_COD_MOVIMIENTO))
            .join(SIN.alias("SIN"), F.col("RP.nid_siniestro")    == F.col(SIN_NID_SINIESTRO))
            .join(CS.alias("CS"), F.col(SIN_COD_CAUSA_AFP) == F.col(CS_COD_CAUSA_SINIESTRO),"left")
            .join(CO.alias("CO"), F.col(SIN_COD_ORIGEN)               == F.col(CO_COD_ORIGEN))
            .join(PA, (F.col(SIN_NID_NUM_IDEN) == F.col(PA_NID_IDENTIFICACION)) &
                    (F.col(SIN_COD_TIPO_IDEN) == F.col(PA_COD_TIPO_IDEN)),"left")
            .join(isa_filtrada.alias("ISA_FIL"), F.col(SIN_NID_SINIESTRO) == F.col(ISA_FIL_NID_SINIESTRO), "left")

            .join(
                P.alias("P"),
                ( F.col(SIN_FEC_SINIESTRO) >= F.col(P_FEC_INICIAL_VIGENCIA)) &
                ( F.col(SIN_FEC_SINIESTRO)<= F.col(P_FEC_FINAL_VIGENCIA)),"left"
            )
 
            .join(
                SH,
                (F.col("SH.NID_SINIESTRO") == F.col("SIN.nid_siniestro")) &
                (F.col("SH.PERIODO_POINT") == F.col("RP.PERIODO_POINT")),
                "left"
            )
            .join(
                OSF,
                F.col("SIN.nid_siniestro") == F.col("OSF.NID_SINIESTRO"),
                "left"
            )


            .filter(F.col("RP.nid_recibo_caja_point") != 0)
            .select(
                F.col("TR.cod_tipo_recuperacion").cast("string").alias("id_categoria_fk"),
                F.col("TR.nombre").alias("categoria"),
                F.lit("Recuperaciones").alias("tipo"),
                F.col("RP.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
                F.col(TM_DESCRIPCION).alias("tipo_transaccion"),
                F.col("RP.nid_siniestro").cast("string").alias("numero_siniestro"),
                F.abs(F.col("DT.VLR_CONCEPTO")).alias("valor_transaccion"),
                F.col("DT.VLR_CONCEPTO").alias("valor_transaccion_ajustado"),
                F.col("RP.fec_creacion_registro").alias("fecha_transaccion"),
                F.when(
                    F.col(CO_COD_ORIGEN) == '2' ,
                    F.col(ISA_DIA_181)
                ).otherwise(
                    F.col(SIN_FEC_SINIESTRO)
                ).alias("fecha_ocurrencia"), 
                F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
                F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),

                # Cod Origen: versión codificada
                F.when(F.col("TR.NOMBRE") == "AUXILIO FUNERARIO", F.lit("4"))
                 .when(F.col("TR.NOMBRE") == "SUBSIDIO POR INCAPACIDAD TEMPORAL", F.lit("5"))
                 .when(F.col("SH.COD_ORIGEN_SINIESTRO_NEW") == F.col("OSF.nombre_origen_final"), F.col("SH.COD_ORIGEN_SINIESTRO_NEW"))
                 .otherwise(F.col("CO.cod_origen").cast("string")).alias("cod_origen"),                
                F.when(F.col("TR.NOMBRE") == "AUXILIO FUNERARIO", F.lit("AUXILIO FUNERARIO"))
                 .when(F.col("TR.NOMBRE") == "SUBSIDIO POR INCAPACIDAD TEMPORAL", F.lit("INCAPACIDAD"))
                 .when(F.col("SH.COD_ORIGEN_SINIESTRO_NEW") == F.col("OSF.nombre_origen_final"), F.col("SH.COD_ORIGEN_SINIESTRO_NEW"))
                 .otherwise(F.col("CO.NOMBRE")).alias("origen"),



                F.coalesce(F.col(SIN_FEC_INICIO_SOLICITUD_AFP), F.col(SIN_FEC_SOLICITUD_AFP)).alias("fecha_aviso"),
                F.coalesce(F.col(P_NID_POLIZA), F.lit(999)).alias("poliza"),
                F.lit("2").cast("string").alias("id_fuente_fk"),
                F.lit("50").cast("string").alias("compania"),
                F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
                F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
                F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.col("RP.PERIODO_POINT")
            
            )
        )



        df_final = df_recup

        return df_final
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from pyspark.sql import functions as F, Window


class StepGold:
    def __init__(self, glue_context, spark):
        self.spark = spark
        
    
    def build_SH(self,base_df, h2_df, periodo_col,base_llave, base_alias,h2_alias="H2"):
        diff_expr = F.abs(
            F.expr(f"int(date_format({h2_alias}.FEC_MODIFICACION, 'yyyyMM'))") - F.col(f"{base_alias}.{periodo_col}").cast("int")
        )

        window_spec = Window.partitionBy(f"{h2_alias}.NID_SINIESTRO").orderBy(diff_expr)

        SH = (
            h2_df.join(base_df, F.col(f"{h2_alias}.NID_SINIESTRO") == F.col(f"{base_alias}.{base_llave}"), "inner")
            .withColumn("RN", F.row_number().over(window_spec))
            .filter(F.col("RN") == 1)
            .select(
                F.col(f"{h2_alias}.NID_SINIESTRO"),
                F.col(f"{h2_alias}.COD_ORIGEN_SINIESTRO_ANT"),
                F.col(f"{h2_alias}.COD_ORIGEN_SINIESTRO_NEW"),
                F.col(f"{h2_alias}.FEC_MODIFICACION"),
                F.col(f"{base_alias}.{periodo_col}").alias("PERIODO_POINT")
            )
            .alias("SH")
        )

        return SH


    def create_component(self):
        """
        This Function to create gold_layer Table.
        """


        decimal_cast = "decimal(19, 0)"
        SIN_COD_CAUSA_AFP = "SIN.cod_causa_siniestro_afp"
        CP_COD_CONCEPTO_PAGO_SINIESTRO = "CP.cod_concepto_pago_siniestro"
        TM_COD_MOVIMIENTO = "TM.cod_movimiento"
        SIN_COD_ORIGEN = "SIN.cod_origen"
        SIN_NID_SINIESTRO ="SIN.nid_siniestro"
        CS_COD_CAUSA_SINIESTRO = "CS.cod_causa_siniestro"
        CO_COD_ORIGEN = "CO.cod_origen"
        SIN_FEC_SINIESTRO = "SIN.fec_siniestro"
        P_FEC_INICIAL_VIGENCIA = "P.fec_inicial_vigencia"
        P_FEC_FINAL_VIGENCIA = "P.fec_final_vigencia"
        DR_VALOR_RESERVA = "DR.vlr_reserva"
        AFIL_CEDULA ="AF.AFIL_CEDULA"
        PS_VLR_PAGO ="PS.vlr_pago"
        SIN_FEC_SOLICITUD_AFP = "SIN.fec_solicitud_afp"
        TM_DESCRIPCION ="TM.descripcion"
        ISA_DIA_181 ="ISA_FIL.dia_181"
        CS_DESCRIPCION = "CS.descripcion"
        UNIDAD_DE_NEGOCIO = "Seguridad social"
        P_NID_POLIZA ="P.nid_poliza"
        CO_NOMBRE = "CO.nombre"
        PA_NID_IDENTIFICACION = "PA.NID_IDENTIFICACION"
        SIN_NID_NUM_IDEN = "SIN.NID_NUMERO_IDENTIFICACION"
        SIN_COD_TIPO_IDEN = "SIN.COD_TIPO_IDENTIFICACION"
        PA_COD_TIPO_IDEN = "PA.COD_TIPO_IDENTIFICACION_AFP"
        ISA_FIL_NID_SINIESTRO = "ISA_FIL.NID_SINIESTRO"
        SIN_NID_SOL = "SIN.NID_SOLICITUD_AFP"
        BP_NID_SOLICITUD_AFP = "BP.NID_SOLICITUD_AFP"
        BP_CIUDAD_SIN = "BP.CIUDAD_SINIESTRO"
        SIN_COD_DEP = "SIN.COD_DEPARTAMENTO"
        AF_FIL_IN = "AF.afil_fecing"
        AF_CLASEI = "AF.AFIL_CLASEI"
        AF_FEC_NAC = "AF.afil_fecnac"
        AF_SEXO = "AF.afil_sexo"
        AF_NUMCUEN = "AF.AFIL_NUMCUE"
        PA_CUENTA_IND = "PA.NID_CUENTA_INDIVIDUAL"
        DESC_CAUSA = "Otras causas"
   

        
        SIN_FEC_INICIO_SOLICITUD_AFP ="SIN.fec_inicio_solicitud_afp"
        DR = self.spark.table("global_temp.spr_detalle_reserva")
        PN = self.spark.table("global_temp.acu_personas_naturales").alias("PN")
        SR = self.spark.table("global_temp.spr_reserva").alias("SR")
        CP = self.spark.table("global_temp.spr_conceptos_pagos_siniestros")
        TM = self.spark.table("global_temp.cgr_movimientos_reserva")
        SIN = self.spark.table("global_temp.spr_siniestros").alias("SIN")
        CS = self.spark.table("global_temp.spr_causas_siniestros")
        CO = self.spark.table("global_temp.cgr_origenes").alias("CO")
        P  = self.spark.table("global_temp.ppr_polizas")
        ISA= self.spark.table("global_temp.spr_incapacidades_sol_audit").alias("ISA")
        PS = self.spark.table("global_temp.spr_detalle_pago_siniestro")
        PG = self.spark.table("global_temp.spr_pagos").alias("PG")
        PA = self.spark.table("global_temp.ppr_afiliados").alias("PA")
        RP = self.spark.table("global_temp.spr_recuperaciones").alias("RP")
        TR = self.spark.table("global_temp.spr_tipos_recuperaciones")
        DT = self.spark.table("global_temp.spr_detalle_recuperaciones").alias("DT")
        H2 = spark.table("global_temp.Spr_siniestros_origenes_hist").alias("H2")


        window_origen = Window.partitionBy("H2.NID_SINIESTRO").orderBy(F.col("H2.NID_SINIESTRO_ORIGEN_HIST").desc())

        OSF = (
            H2.join(CO, F.col("H2.COD_ORIGEN_SINIESTRO_NEW") == F.col("CO.COD_ORIGEN"), "left")
            .withColumn("RN", F.row_number().over(window_origen))
            .filter(F.col("RN") == 1)
            .select(
                F.col("H2.NID_SINIESTRO"),
                F.col("CO.NOMBRE").alias("nombre_origen_final")
            )
            .alias("OSF")
        )



        SH_RP = self.build_SH(RP,H2,"PERIODO_POINT","NID_SINIESTRO",base_alias="RP",h2_alias="H2").alias("SH_RP")
        SH_PG = self.build_SH(PG,H2,"PERIODO_POINT","NID_SINIESTRO",base_alias="PG",h2_alias="H2").alias("SH_PG")
        SH_RE = self.build_SH(SR,H2,"PERIODO_POINT","NID_SINIESTRO",base_alias="SR",h2_alias="H2").alias("SH_RE")


        isa_max_df = ISA.groupBy("NID_SINIESTRO").agg(
            F.max("NID_SOLICITUD_AUDIT").alias("MAX_AUDIT_ID")
        ).withColumnRenamed("NID_SINIESTRO","MAX_NID_SINIESTRO")

        isa_filtrada = ISA.join(
            isa_max_df.alias("max"),
            (F.col("ISA.NID_SINIESTRO") == F.col("max.MAX_NID_SINIESTRO")) &
            (F.col("ISA.NID_SOLICITUD_AUDIT") == F.col("max.MAX_AUDIT_ID"))
        )

        #  “Reserva”
        df_reserva = (
            DR.alias("DR")
            .join(SR.alias("SR"), F.col("DR.nid_reserva") == F.col("SR.nid_reserva"))
            .join(CP.alias("CP"), F.col("DR.cod_concepto_pago_siniestro") == F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO))
            .join(TM.alias("TM"), F.col("DR.cod_movimiento") == F.col(TM_COD_MOVIMIENTO))
            .join(SIN.alias("SIN"), F.col("DR.nid_siniestro") == F.col(SIN_NID_SINIESTRO))
            .join(CS.alias("CS"), F.col(SIN_COD_CAUSA_AFP) == F.col(CS_COD_CAUSA_SINIESTRO),"left")
            .join(CO.alias("CO"), F.col(SIN_COD_ORIGEN) == F.col(CO_COD_ORIGEN))
            .join(PA, (F.col(SIN_NID_NUM_IDEN) == F.col(PA_NID_IDENTIFICACION)) &
                    (F.col(SIN_COD_TIPO_IDEN) == F.col(PA_COD_TIPO_IDEN)),"left")
            .join(isa_filtrada.alias("ISA_FIL"), F.col(SIN_NID_SINIESTRO) == F.col(ISA_FIL_NID_SINIESTRO), "left")
            .join(
                PN,
                (F.col("SIN.NID_NUMERO_IDENTIFICACION") == F.col("PN.NID_NUMERO_IDENTIFICACION"))&
                (F.col("SIN.COD_TIPO_IDENTIFICACION") == F.col("PN.COD_TIPO_IDENTIFICACION"))
            )
            .join(
                SH_RE,
                (F.col("SH_RE.NID_SINIESTRO") == F.col("SIN.nid_siniestro")) &
                (F.col("SH_RE.PERIODO_POINT") == F.col("SR.PERIODO_POINT")),
                "left"
            )
            .join(
                P.alias("P"),
                ( F.col(SIN_FEC_SINIESTRO) >= F.col(P_FEC_INICIAL_VIGENCIA)) &
                ( F.col(SIN_FEC_SINIESTRO)<= F.col(P_FEC_FINAL_VIGENCIA)),"left"
            )
            .join(
                CO.alias("CO3"),
                F.col("CO3.COD_ORIGEN") == F.col("SH_RE.COD_ORIGEN_SINIESTRO_NEW"),
                "left"
            )
            .join(
                CO.alias("CO4"),
                F.col("CO4.COD_ORIGEN") == F.col("SH_RE.COD_ORIGEN_SINIESTRO_ANT"),
                "left"
            )           
            .filter(F.col(DR_VALOR_RESERVA) != 0)
            .select(
                F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO).cast("string").alias("id_categoria_fk"),
                F.col("CP.descripcion").alias("categoria"),
                F.lit("Reserva").alias("tipo"),
                F.col("DR.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
                F.col(TM_DESCRIPCION).alias("tipo_transaccion"),
                F.col("DR.nid_siniestro").cast("string").alias("numero_siniestro"),
                F.abs(F.col(DR_VALOR_RESERVA) ).alias("valor_transaccion"),
                F.col(DR_VALOR_RESERVA).alias("valor_transaccion_ajustado"),
                F.col("SR.fec_generado").alias("fecha_transaccion"),
                F.when(
                    F.col(CO_COD_ORIGEN) == '2' ,
                    F.col(ISA_DIA_181)
                ).otherwise(
                    F.col(SIN_FEC_SINIESTRO)
                ).alias("fecha_ocurrencia"),
                F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
                F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),
                F.when(F.col("SR.PERIODO_POINT").cast("int") >= F.expr("int(date_format(SH_RE.FEC_MODIFICACION, 'yyyyMM'))"),
                       F.col("SH_RE.COD_ORIGEN_SINIESTRO_NEW"))
                 .when(F.col("SR.PERIODO_POINT").cast("int") < F.expr("int(date_format(SH_RE.FEC_MODIFICACION, 'yyyyMM'))"),
                       F.col("SH_RE.COD_ORIGEN_SINIESTRO_ANT"))
                 .otherwise(F.col("CO.cod_origen").cast("string"))
                 .alias("cod_origen"),

                F.when(F.col("SR.PERIODO_POINT").cast("int") >= F.expr("int(date_format(SH_RE.FEC_MODIFICACION, 'yyyyMM'))"),
                       F.col("CO3.NOMBRE"))
                 .when(F.col("SR.PERIODO_POINT").cast("int") < F.expr("int(date_format(SH_RE.FEC_MODIFICACION, 'yyyyMM'))"),
                       F.col("CO4.NOMBRE"))
                 .otherwise(F.col("CO.NOMBRE"))
                 .alias("origen"),
                F.coalesce(F.col(SIN_FEC_INICIO_SOLICITUD_AFP), F.col(SIN_FEC_SOLICITUD_AFP)).alias("fecha_aviso"),
                F.coalesce(F.col(P_NID_POLIZA), F.lit(999)).alias("poliza"),
                F.lit("2").cast("string").alias("id_fuente_fk"),
                F.lit("50").cast("string").alias("compania"),
                F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
                F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
                F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.coalesce(F.col("SIN.COD_CIUDAD"), F.lit("01")).alias("ciudad_lugar_ocurrencia"),
                F.coalesce(F.col("SIN.COD_DEPARTAMENTO").cast("string"), F.lit("01001")).alias("depto_lugar_ocurrencia"),
                F.coalesce(
                F.col("SIN.FEC_INICIO_SOLICITUD_AFP"),
                F.to_date(
                    F.concat_ws("", F.col("PA.priper_cotrep").cast("string"), F.lit("01")),
                    "yyyyMMdd"
                    )
                ).alias("fecha_inicio_vigencia"),
                F.col("SIN.NID_NUMERO_IDENTIFICACION").alias("cedula"),
                F.col("PN.FEC_NACIMIENTO").alias("fecha_nacimiento"),
                F.col("PN.GENERO").alias("genero")             
            )
        )

        


        #  “Recuperaciones”
        df_recup = (
            DT.alias("DT")
            .join(RP.alias("RP"),F.col("RP.nid_recuperacion") == F.col("DT.nid_recuperacion"),"left")
            .join(TR.alias("TR"), F.col("RP.cod_tipo_recuperacion") == F.col("TR.cod_tipo_recuperacion"))
            .join(TM.alias("TM"), F.col("RP.cod_movimiento") == F.col(TM_COD_MOVIMIENTO))
            .join(SIN.alias("SIN"), F.col("RP.nid_siniestro")    == F.col(SIN_NID_SINIESTRO))
            .join(CS.alias("CS"), F.col(SIN_COD_CAUSA_AFP) == F.col(CS_COD_CAUSA_SINIESTRO),"left")
            .join(CO.alias("CO"), F.col(SIN_COD_ORIGEN)               == F.col(CO_COD_ORIGEN))
            .join(PA, (F.col(SIN_NID_NUM_IDEN) == F.col(PA_NID_IDENTIFICACION)) &
                    (F.col(SIN_COD_TIPO_IDEN) == F.col(PA_COD_TIPO_IDEN)),"left")
            .join(isa_filtrada.alias("ISA_FIL"), F.col(SIN_NID_SINIESTRO) == F.col(ISA_FIL_NID_SINIESTRO), "left")
            .join(
                P.alias("P"),
                ( F.col(SIN_FEC_SINIESTRO) >= F.col(P_FEC_INICIAL_VIGENCIA)) &
                ( F.col(SIN_FEC_SINIESTRO)<= F.col(P_FEC_FINAL_VIGENCIA)),"left"
            )
            .join(
                SH_RP,
                (F.col("SH_RP.NID_SINIESTRO") == F.col("SIN.nid_siniestro")) &
                (F.col("SH_RP.PERIODO_POINT") == F.col("RP.PERIODO_POINT")),
                "left"
            )
            .join(
                PN,
                (F.col("SIN.NID_NUMERO_IDENTIFICACION") == F.col("PN.NID_NUMERO_IDENTIFICACION"))&
                (F.col("SIN.COD_TIPO_IDENTIFICACION") == F.col("PN.COD_TIPO_IDENTIFICACION"))
            )
            .join(
                CO.alias("CO3"),
                F.col("CO3.COD_ORIGEN") == F.col("SH_RP.COD_ORIGEN_SINIESTRO_NEW"),
                "left"
            )
            .join(
                CO.alias("CO4"),
                F.col("CO4.COD_ORIGEN") == F.col("SH_RP.COD_ORIGEN_SINIESTRO_ANT"),
                "left"
            )            
            .filter(F.col("RP.nid_recibo_caja_point") != 0)
            .select(
                F.col("TR.cod_tipo_recuperacion").cast("string").alias("id_categoria_fk"),
                F.col("TR.nombre").alias("categoria"),
                F.lit("Recuperaciones").alias("tipo"),
                F.col("RP.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
                F.col(TM_DESCRIPCION).alias("tipo_transaccion"),
                F.col("RP.nid_siniestro").cast("string").alias("numero_siniestro"),
                F.abs(F.col("DT.VLR_CONCEPTO")).alias("valor_transaccion"),
                F.col("DT.VLR_CONCEPTO").alias("valor_transaccion_ajustado"),
                F.col("RP.fec_creacion_registro").alias("fecha_transaccion"),
                F.when(
                    F.col(CO_COD_ORIGEN) == '2' ,
                    F.col(ISA_DIA_181)
                ).otherwise(
                    F.col(SIN_FEC_SINIESTRO)
                ).alias("fecha_ocurrencia"), 
                F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
                F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),
                F.when(F.col("RP.PERIODO_POINT").cast("int") >= F.expr("int(date_format(SH_RP.FEC_MODIFICACION, 'yyyyMM'))"),
                       F.col("SH_RP.COD_ORIGEN_SINIESTRO_NEW"))
                 .when(F.col("RP.PERIODO_POINT").cast("int") < F.expr("int(date_format(SH_RP.FEC_MODIFICACION, 'yyyyMM'))"),
                       F.col("SH_RP.COD_ORIGEN_SINIESTRO_ANT"))
                 .otherwise(F.col("CO.cod_origen").cast("string"))
                 .alias("cod_origen"),

                F.when(F.col("RP.PERIODO_POINT").cast("int") >= F.expr("int(date_format(SH_RP.FEC_MODIFICACION, 'yyyyMM'))"),
                       F.col("CO3.NOMBRE"))
                 .when(F.col("RP.PERIODO_POINT").cast("int") < F.expr("int(date_format(SH_RP.FEC_MODIFICACION, 'yyyyMM'))"),
                       F.col("CO4.NOMBRE"))
                 .otherwise(F.col("CO.NOMBRE"))
                 .alias("origen"),
                F.coalesce(F.col(SIN_FEC_INICIO_SOLICITUD_AFP), F.col(SIN_FEC_SOLICITUD_AFP)).alias("fecha_aviso"),
                F.coalesce(F.col(P_NID_POLIZA), F.lit(999)).alias("poliza"),
                F.lit("2").cast("string").alias("id_fuente_fk"),
                F.lit("50").cast("string").alias("compania"),
                F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
                F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
                F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.coalesce(F.col("SIN.COD_CIUDAD"), F.lit("01")).alias("ciudad_lugar_ocurrencia"),
                F.coalesce(F.col("SIN.COD_DEPARTAMENTO").cast("string"), F.lit("01001")).alias("depto_lugar_ocurrencia"),
                F.coalesce(
                F.col("SIN.FEC_INICIO_SOLICITUD_AFP"),
                F.to_date(
                    F.concat_ws("", F.col("PA.priper_cotrep").cast("string"), F.lit("01")),
                    "yyyyMMdd"
                    )
                ).alias("fecha_inicio_vigencia"),
                F.col("SIN.NID_NUMERO_IDENTIFICACION").alias("cedula"),
                F.col("PN.FEC_NACIMIENTO").alias("fecha_nacimiento"),
                F.col("PN.GENERO").alias("genero")  
            
            )
        )



        #  “Pago”
        df_pago = (
            PS.alias("PS")
            .join(PG.alias("PG"), F.col("PG.nid_pago") == F.col("PS.nid_pago"))
            .join(CP.alias("CP"), F.col("PS.cod_concepto_pago_siniestro") == F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO))
            .join(TM.alias("TM"), F.col("PS.cod_movimiento") == F.col(TM_COD_MOVIMIENTO))
            .join(SIN.alias("SIN"), F.col("PS.nid_siniestro") == F.col(SIN_NID_SINIESTRO))
            .join(CS.alias("CS"), F.col(SIN_COD_CAUSA_AFP)  == F.col(CS_COD_CAUSA_SINIESTRO),"left")
            .join(CO.alias("CO"), F.col(SIN_COD_ORIGEN)               == F.col(CO_COD_ORIGEN))
            .join(PA, (F.col(SIN_NID_NUM_IDEN) == F.col(PA_NID_IDENTIFICACION)) &
                    (F.col(SIN_COD_TIPO_IDEN) == F.col(PA_COD_TIPO_IDEN)),"left")
            .join(isa_filtrada.alias("ISA_FIL"), F.col(SIN_NID_SINIESTRO) == F.col(ISA_FIL_NID_SINIESTRO), "left")
            .join(
                PN,
                (F.col("SIN.NID_NUMERO_IDENTIFICACION") == F.col("PN.NID_NUMERO_IDENTIFICACION"))&
                (F.col("SIN.COD_TIPO_IDENTIFICACION") == F.col("PN.COD_TIPO_IDENTIFICACION"))
            )
            .join(
                SH_PG,
                (F.col("SH_PG.NID_SINIESTRO") == F.col("SIN.nid_siniestro")) &
                (F.col("SH_PG.PERIODO_POINT") == F.col("PG.PERIODO_POINT")),
                "left"
            ).join(
                P.alias("P"),
                ( F.col(SIN_FEC_SINIESTRO) >= F.col(P_FEC_INICIAL_VIGENCIA)) &
                ( F.col(SIN_FEC_SINIESTRO)<= F.col(P_FEC_FINAL_VIGENCIA)),"left"
            )
            .join(
                CO.alias("CO3"),
                F.col("CO3.COD_ORIGEN") == F.col("SH_PG.COD_ORIGEN_SINIESTRO_NEW"),
                "left"
            )
            .join(
                CO.alias("CO4"),
                F.col("CO4.COD_ORIGEN") == F.col("SH_PG.COD_ORIGEN_SINIESTRO_ANT"),
                "left"
            ) 

            .filter(F.col(PS_VLR_PAGO) != 0)
            .select(
                F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO).cast("string").alias("id_categoria_fk"),
                F.col("CP.descripcion").alias("categoria"),
                F.lit("Pago").alias("tipo"),
                F.col("PS.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
                F.col(TM_DESCRIPCION).alias("tipo_transaccion"),
                F.col("PS.nid_siniestro").cast("string").alias("numero_siniestro"),
                F.abs(F.col(PS_VLR_PAGO)).alias("valor_transaccion"),
                F.col(PS_VLR_PAGO).alias("valor_transaccion_ajustado"),
                F.col("PG.fec_generado").alias("fecha_transaccion"),
                F.when(
                    F.col(CO_COD_ORIGEN) == '2' ,
                    F.col(ISA_DIA_181)
                ).otherwise(
                    F.col(SIN_FEC_SINIESTRO)
                ).alias("fecha_ocurrencia"),
                F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
                F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),
                F.when(F.col("PG.PERIODO_POINT").cast("int") >= F.expr("int(date_format(SH_PG.FEC_MODIFICACION, 'yyyyMM'))"),
                       F.col("SH_PG.COD_ORIGEN_SINIESTRO_NEW"))
                 .when(F.col("PG.PERIODO_POINT").cast("int") < F.expr("int(date_format(SH_PG.FEC_MODIFICACION, 'yyyyMM'))"),
                       F.col("SH_PG.COD_ORIGEN_SINIESTRO_ANT"))
                 .otherwise(F.col("CO.cod_origen").cast("string"))
                 .alias("cod_origen"),

                F.when(F.col("PG.PERIODO_POINT").cast("int") >= F.expr("int(date_format(SH_PG.FEC_MODIFICACION, 'yyyyMM'))"),
                       F.col("CO3.NOMBRE"))
                 .when(F.col("PG.PERIODO_POINT").cast("int") < F.expr("int(date_format(SH_PG.FEC_MODIFICACION, 'yyyyMM'))"),
                       F.col("CO4.NOMBRE"))
                 .otherwise(F.col("CO.NOMBRE"))
                 .alias("origen"),
                F.coalesce(F.col(SIN_FEC_INICIO_SOLICITUD_AFP), F.col(SIN_FEC_SOLICITUD_AFP)).alias("fecha_aviso"),
                F.coalesce(F.col(P_NID_POLIZA), F.lit(999)).alias("poliza"),
                F.lit("2").cast("string").alias("id_fuente_fk"),
                F.lit("50").cast("string").alias("compania"),
                F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
                F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
                F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.coalesce(F.col("SIN.COD_CIUDAD"), F.lit("01")).alias("ciudad_lugar_ocurrencia"),
                F.coalesce(F.col("SIN.COD_DEPARTAMENTO").cast("string"), F.lit("01001")).alias("depto_lugar_ocurrencia"),
                F.coalesce(
                F.col("SIN.FEC_INICIO_SOLICITUD_AFP"),
                F.to_date(
                    F.concat_ws("", F.col("PA.priper_cotrep").cast("string"), F.lit("01")),
                    "yyyyMMdd"
                    )
                ).alias("fecha_inicio_vigencia"),
                F.col("SIN.NID_NUMERO_IDENTIFICACION").alias("cedula"),
                F.col("PN.FEC_NACIMIENTO").alias("fecha_nacimiento"),
                F.col("PN.GENERO").alias("genero")               
            



            )
        )
        df_final = df_reserva.unionByName(df_pago).unionByName(df_recup)


        return df_final

transacciones = StepGold(glueContext, spark)
df_reservas = transacciones.create_component()

# Mostrar el resultado

df_reservas.count()
df_reservas.filter(F.col("numero_siniestro") == "201900980").show()
# Para probar:

df= StepReserva(glueContext, spark).run()
# df_res.printSchema()
df_data = StepReserva.run()

contador = StepGoldCount(spark)
df = contador.union_data()
#df.count()##269279
df_test =df.filter(F.col("numero_siniestro")==201701196)
#Reser 111592
#pagos 111497
#reci 46190

df_test.count()
SR.printSchema()
PG.printSchema()
RP.printSchema()
print(count_total_records)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, LongType, DoubleType, FloatType
from pyspark.sql import functions as F
from datetime import date
schema_bodega = StructType([
    StructField("NID_SINIESTRO", StringType(), True),
    StructField("COD_MOVIMIENTO", StringType(), True),
    StructField("concepto_pago", StringType(), True),
    StructField("VLR_pago", StringType(), True),
    StructField("periodo_point", StringType(), True),
    StructField("fecha_siniestro", DateType(), True),
])

data_bodega = [
    ("20212345678", "Recuperacion", "Hospitalización", "150000.0", "202204", date(2022, 4, 15)),
    ("99999999", "Pago", "Indemnización", "200000.0", "202204", date(2022, 4, 10))
]
DF_BODEGA = spark.createDataFrame(data_bodega, schema=schema_bodega)

DF_BODEGA.createOrReplaceGlobalTempView("bodega_plus")

# DF_DATO_BASICO
schema_dato = StructType([
    StructField("ID_SINIESTRO", StringType(), True),
    StructField("COD_CAUSA_SINIESTRO", StringType(), True),
    StructField("COD_ORIGEN_SINIESTRO", StringType(), True),
])
DF_DATO_BASICO = spark.createDataFrame([("20212345678", "384", "742")], schema=schema_dato)

DF_DATO_BASICO.createOrReplaceGlobalTempView("snr_dato_basico")

# DF_CAUSAS
schema_causas = StructType([
    StructField("ID_CAUSAL", StringType(), True),
    StructField("NOMBRE", StringType(), True),
])
DF_CAUSAS = spark.createDataFrame([("384", "Accidente laboral")], schema=schema_causas)

DF_CAUSAS.createOrReplaceGlobalTempView("snr_causa")

# DF_ORIGEN
schema_origen = StructType([
    StructField("COD_ORIGEN_SINIESTRO", StringType(), True),
    StructField("NOMBRE", StringType(), True),
])
DF_ORIGEN = spark.createDataFrame([("742", "Incapacidad total permanente")], schema=schema_origen)

DF_ORIGEN.createOrReplaceGlobalTempView("snr_origen")

# DF_POLIZAS
schema_polizas = StructType([
    StructField("FEC_INICIAL_VIGENCIA", DateType(), True),
    StructField("FEC_FINAL_VIGENCIA", DateType(), True),
    StructField("NID_POLIZA", IntegerType(), True),
])
DF_POLIZAS = spark.createDataFrame([
    (date(2022, 1, 1), date(2022, 12, 31), 12345)
], schema=schema_polizas)
B_NID_SINIESTRO = 'B.NID_SINIESTRO'
DF_RESULT = DF_BODEGA.alias('B').join(
    DF_DATO_BASICO.alias('D'), F.col(B_NID_SINIESTRO) == F.col('D.ID_SINIESTRO'), 'left'
    ).join(
    DF_CAUSAS.alias('c'), F.col('D.COD_CAUSA_SINIESTRO') == F.col('c.ID_CAUSAL'), 'left'
    ).join(
    DF_ORIGEN.alias('o'), F.col('D.COD_ORIGEN_SINIESTRO') == F.col('o.COD_ORIGEN_SINIESTRO'), 'left'
    ).join(
    DF_POLIZAS.alias('p'),  F.coalesce(F.col('B.fecha_siniestro'), F.to_date(F.concat_ws('', F.col('B.periodo_point'), F.lit('01')), 'yyyyMMdd')).between(F.col('p.FEC_INICIAL_VIGENCIA'),F.col('p.FEC_FINAL_VIGENCIA'))
    )

df_facturas = DF_RESULT.filter(
    (F.col(B_NID_SINIESTRO).cast(IntegerType()) >= 20200000000) & (F.col("b.cod_movimiento") == "Recuperacion")
).selectExpr(
    "'3' as id_categoria_fk",
    "b.concepto_pago AS categoria",
    "'Facturas' AS tipo",
    "CASE b.cod_movimiento  WHEN 'Pago' THEN 7 ELSE 7 END AS id_tipo_transaccion_fk",
    "CASE b.cod_movimiento  WHEN 'Pago' THEN 'Recobros' ELSE 'Recobros' END AS tipo_transaccion",
    "b.NID_SINIESTRO AS numero_siniestro",
    "abs(cast(b.VLR_pago as double)) AS valor_transaccion",
    "b.VLR_pago AS valor_transaccion_ajustado",
    "cast(to_date(b.periodo_point || '01' ,'%yyyy%MM%dd') as date)  AS fecha_transaccion",    
    "COALESCE(cast(b.fecha_siniestro as date), cast(to_date(b.periodo_point || '01' ,'%yyyy%MM%d') as date))  AS fecha_ocurrencia",
    "c.ID_CAUSAL as cod_causa",    
    "COALESCE(c.NOMBRE, 'Otras causas') AS causa",
    "o.cod_origen_siniestro as cod_origen", 
    "COALESCE(o.NOMBRE,'Incapacidad total y permanente') AS origen",
    "cast(to_date(b.periodo_point || '01' ,'%yyyy%MM%dd') as date)   AS fecha_aviso",
    "COALESCE(p.NID_POLIZA ,999) AS poliza",
    "'3' as id_fuente_fk",
    "'50' AS compania",
    "41 AS id_producto_fk",
    "31 AS id_ramo_fk",
    "'Seguridad Social' AS unidad_de_negocio",   
    "CONCAT('PRV-',LPAD(COALESCE(CAST(p.NID_POLIZA AS VARCHAR(10)),'999'),7,'0'),'-00') as poliza_lider"



)


df_pagos = DF_RESULT.filter(
    (F.col(B_NID_SINIESTRO)== "99999999")
).selectExpr(
    "'2'  AS id_categoria_fk",
    "b.concepto_pago    AS categoria",
    "'Recduperacion' AS tipo",
    "CASE b.cod_movimiento  WHEN 'Pago' THEN  5   ELSE 5 END AS id_tipo_transaccion_fk",
    "CASE b.cod_movimiento  WHEN 'Pago' THEN 'Pago'  ELSE 'Pago'  END AS tipo_transaccion",    
    "b.NID_SINIESTRO AS numero_siniestro",
    "abs(cast(b.VLR_pago as double)) AS valor_transaccion",
    "b.VLR_pago AS valor_transaccion_ajustado",
    "cast(to_date(b.periodo_point || '01' ,'%yyyy%MM%dd') as date)  AS fecha_transaccion",
    "COALESCE(cast(b.fecha_siniestro as date), cast(to_date(b.periodo_point || '01' ,'%yyyy%MM%dd') as date))  AS fecha_ocurrencia",    
    "'384'	as cod_causa",
    "COALESCE(c.NOMBRE,'Otras causas' ) AS causa",
    "'742' as cod_origen",    
    "COALESCE(o.NOMBRE,'Incapacidad total y permanente' ) AS origen",
    "cast(to_date(b.periodo_point || '01' ,'%yyyy%MM%dd') as date)   AS fecha_aviso",
    "COALESCE(p.NID_POLIZA ,999) AS poliza",
    "'3' as id_fuente_fk",
    "'50' AS compania",
    "41 AS id_producto_fk",    
    "31 AS id_ramo_fk",
    "'Seguridad Social' AS unidad_de_negocio",
"CONCAT('PRV-',LPAD(COALESCE(CAST(p.NID_POLIZA AS VARCHAR(10)),'999'),7,'0'),'-00') as poliza_lider"










)

df_final = df_facturas.union(df_pagos)
df_final.show()
df_final.printSchema()
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, LongType, DoubleType, FloatType
from pyspark.sql import functions as F
from datetime import date

data_dr = [
    (1, "C1", "M1", 100, 500.0),
]
schema_dr = StructType([
    StructField("nid_reserva", IntegerType()),
    StructField("cod_concepto_pago_siniestro", StringType()),
    StructField("cod_movimiento", StringType()),
    StructField("nid_siniestro", IntegerType()),
    StructField("valor_reserva", DoubleType()),
])
DR = spark.createDataFrame(data_dr, schema=schema_dr)
DR.createOrReplaceGlobalTempView("spr_detalle_reserva")
# DataFrame SR (fecha generada para reservas)
data_sr = [
    (1, "2025-06-01"),
]
schema_sr = StructType([
    StructField("nid_reserva", IntegerType()),
    StructField("fec_generado", StringType()),
])
SR = spark.createDataFrame(data_sr, schema=schema_sr)
SR.createOrReplaceGlobalTempView("spr_reserva")
# DataFrame CP (conceptos pagos)
data_cp = [
    ("C1", "Categoría 1"),
]
schema_cp = StructType([
    StructField("cod_concepto_pago_siniestro", StringType()),
    StructField("descripcion", StringType()),
])
CP = spark.createDataFrame(data_cp, schema=schema_cp)
CP.createOrReplaceGlobalTempView("spr_conceptos_pagos_siniestros")
# DataFrame TM (tipos movimientos)
data_tm = [
    ("M1", "Movimiento Tipo 1"),
]
schema_tm = StructType([
    StructField("cod_movimiento", StringType()),
    StructField("descripcion", StringType()),
])
TM = spark.createDataFrame(data_tm, schema=schema_tm)
TM.createOrReplaceGlobalTempView("cgr_movimientos_reserva")
# DataFrame SIN (siniestros)
data_sin = [
    (100, "CA1", 10, "ID123", "2025-05-30", None, "2025-05-25"),
]
schema_sin = StructType([
    StructField("nid_siniestro", IntegerType()),
    StructField("cod_causa_afp", StringType()),
    StructField("cod_origen", IntegerType()),
    StructField("nid_numero_identificacion", StringType()),
    StructField("fec_siniestro", StringType()),
    StructField("fec_inicio_solicitud_afp", StringType()),
    StructField("fec_solicitud_afp", StringType()),
])
SIN = spark.createDataFrame(data_sin, schema=schema_sin)
SIN.createOrReplaceGlobalTempView("spr_siniestros")
# DataFrame CS (causas siniestros)
data_cs = [
    ("CA1", "Causa 1"),
]
schema_cs = StructType([
    StructField("cod_causa_siniestro", StringType()),
    StructField("descripcion", StringType()),
])
CS = spark.createDataFrame(data_cs, schema=schema_cs)
CS.createOrReplaceGlobalTempView("spr_causas_siniestros")
# DataFrame CO (orígenes)
data_co = [
    (10, "Origen 10"),
]
schema_co = StructType([
    StructField("cod_origen", IntegerType()),
    StructField("nombre", StringType()),
])
CO = spark.createDataFrame(data_co, schema=schema_co)
CO.createOrReplaceGlobalTempView("cgr_origenes")
# DataFrame P (polizas)
data_p = [
    (123, "2025-01-01", "2025-12-31"),
]
schema_p = StructType([
    StructField("nid_poliza", IntegerType()),
    StructField("fec_inicial_vigencia", StringType()),
    StructField("fec_final_vigencia", StringType()),
])
P = spark.createDataFrame(data_p, schema=schema_p)
P.createOrReplaceGlobalTempView("ppr_polizas")
# DataFrame ISA (datos adicionales siniestros)
data_isa = [
    (100, None),
]
schema_isa = StructType([
    StructField("nid_siniestro", IntegerType()),
    StructField("dia_181", StringType()),
])
ISA = spark.createDataFrame(data_isa, schema=schema_isa)
ISA.createOrReplaceGlobalTempView("spr_incapacidades_sol_audit")

# DataFrame PS (datos adicionales siniestros)
data_ps = [
    ('1', '5', '386915', '100', '3906210.00'),
]
schema_ps = StructType([
    StructField("cod_concepto_pago_siniestro", StringType()),
    StructField("cod_movimiento", StringType()),
    StructField("nid_pago", StringType()),
    StructField("nid_siniestro", StringType()),
    StructField("vlr_pago", StringType()),
])
PS = spark.createDataFrame(data_ps, schema=schema_ps)
PS.createOrReplaceGlobalTempView("spr_detalle_pago_siniestro")

# DataFrame PG (datos adicionales siniestros)
data_pg = [
    ( date(2020, 2, 21),'M1', '202002'),
]
schema_pg = StructType([
    StructField("fec_generado", DateType()),
    StructField("nid_pago", StringType()),
    StructField("periodo_point", StringType()),
])
PG = spark.createDataFrame(data_pg, schema=schema_pg)
PG.createOrReplaceGlobalTempView("spr_pagos")
# DataFrame PG (datos adicionales siniestros)
data_rp = [
    ( 'BP01','123456789', 'Antonia'),
]
schema_rp = StructType([
    StructField("id", StringType()),
    StructField("cedula", StringType()),
    StructField("nombre", StringType()),
])
PG = spark.createDataFrame(data_rp, schema=schema_rp)
PG.createOrReplaceGlobalTempView("spr_recuperaciones")

# DataFrame PG (datos adicionales siniestros)
data_rp = [
    ( '5','3', date(2020, 2, 21), '60134492', '14000', '100', '201104', '1280814.00'),
]
schema_rp = StructType([
    StructField("cod_movimiento", StringType()),
    StructField("cod_tipo_recuperacion", StringType()),
    StructField("fec_creacion_registro", DateType()),
    StructField("nid_recibo_caja_point", StringType()),
    StructField("nid_recuperacion", StringType()),
    StructField("nid_siniestro", StringType()),
    StructField("periodo_point", StringType()),
    StructField("vlr_recuperacion", StringType())
])
RP = spark.createDataFrame(data_rp, schema=schema_rp)
RP.createOrReplaceGlobalTempView("spr_recuperaciones")
# DataFrame PG (datos adicionales siniestros)
data_tr = [
    ( '5','Codigo'),
]
schema_tr = StructType([
    StructField("cod_tipo_recuperacion", StringType()),
    StructField("nombre", StringType())
])
TR = spark.createDataFrame(data_tr, schema=schema_tr)
TR.createOrReplaceGlobalTempView("spr_tipos_recuperaciones")

decimal_cast = "decimal(19, 0)"
SIN_COD_CAUSA_AFP = "SIN.cod_causa_siniestro_afp"
CP_COD_CONCEPTO_PAGO_SINIESTRO = "CP.cod_concepto_pago_siniestro"
TM_COD_MOVIMIENTO = "TM.cod_movimiento"
SIN_COD_ORIGEN = "SIN.cod_origen"
SIN_NID_SINIESTRO ="SIN.nid_siniestro"
CS_COD_CAUSA_SINIESTRO = "CS.cod_causa_siniestro"
CO_COD_ORIGEN = "CO.cod_origen"
SIN_FEC_SINIESTRO = "SIN.fec_siniestro"
P_FEC_INICIAL_VIGENCIA = "P.fec_inicial_vigencia"
P_FEC_FINAL_VIGENCIA = "P.fec_final_vigencia"
DR_VALOR_RESERVA = "DR.vlr_reserva"
ISA_NID_SINIESTRO ="ISA.nid_siniestro"
PS_VLR_PAGO ="PS.vlr_pago"
SIN_FEC_SOLICITUD_AFP = "SIN.fec_solicitud_afp"
TM_DESCRIPCION ="TM.descripcion"
ISA_DIA_181 ="ISA.dia_181"
CS_DESCRIPCION = "CS.descripcion"
UNIDAD_DE_NEGOCIO = "Seguridad social"
P_NID_POLIZA ="P.nid_poliza"
CO_NOMBRE = "CO.nombre"


SIN_FEC_INICIO_SOLICITUD_AFP ="SIN.fec_inicio_solicitud_afp"
DR = spark.table("global_temp.spr_detalle_reserva")
SR = spark.table("global_temp.spr_reserva")
CP = spark.table("global_temp.spr_conceptos_pagos_siniestros")
TM = spark.table("global_temp.cgr_movimientos_reserva")
SIN = spark.table("global_temp.spr_siniestros")
CS = spark.table("global_temp.spr_causas_siniestros")
CO = spark.table("global_temp.cgr_origenes")
#AF = spark.table("global_temp.vw_dsdafil_alfa")
#BP = spark.table("global_temp.ben_solicitudes_v")
P  = spark.table("global_temp.ppr_polizas")
ISA= spark.table("global_temp.spr_incapacidades_sol_audit")
PS = spark.table("global_temp.spr_detalle_pago_siniestro")
PG = spark.table("global_temp.spr_pagos")
RP = spark.table("global_temp.spr_recuperaciones")
TR = spark.table("global_temp.spr_tipos_recuperaciones")


#  “Reserva”
df_reserva = (
    DR.alias("DR")
    .join(SR.alias("SR"), F.col("DR.nid_reserva") == F.col("SR.nid_reserva"))
    .join(CP.alias("CP"), F.col("DR.cod_concepto_pago_siniestro") == F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO))
    .join(TM.alias("TM"), F.col("DR.cod_movimiento") == F.col(TM_COD_MOVIMIENTO))
    .join(SIN.alias("SIN"), F.col("DR.nid_siniestro") == F.col(SIN_NID_SINIESTRO))
    .join(CS.alias("CS"), F.col(SIN_COD_CAUSA_AFP) == F.col(CS_COD_CAUSA_SINIESTRO))
    .join(CO.alias("CO"), F.col(SIN_COD_ORIGEN) == F.col(CO_COD_ORIGEN))
    #.join(AF.alias("AF"), F.col("SIN.nid_numero_identificacion") == F.col("AF.afil_cedula"))
    #.join(BP.alias("BP"), F.col("SIN.nid_numero_identificacion") == F.col("BP.id"))
    .join(
        P.alias("P"),
        (F.col(SIN_FEC_SINIESTRO) >= F.col(P_FEC_INICIAL_VIGENCIA)) &
        (F.col(SIN_FEC_SINIESTRO) <= F.col(P_FEC_FINAL_VIGENCIA))
    )
    .join(ISA.alias("ISA"), F.col(SIN_NID_SINIESTRO) == F.col(ISA_NID_SINIESTRO), "left")
    .filter(F.col(DR_VALOR_RESERVA) != 0)
    .select(
        F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO).cast("string").alias("id_categoria_fk"),
        F.col("CP.descripcion").alias("categoria"),
        F.lit("Reserva").alias("tipo"),
        F.col("DR.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
        F.col(TM_DESCRIPCION).alias("tipo_transaccion"),
        F.col("DR.nid_siniestro").cast("string").alias("numero_siniestro"),
        F.abs(F.col(DR_VALOR_RESERVA) ).alias("valor_transaccion"),
        F.col(DR_VALOR_RESERVA).alias("valor_transaccion_ajustado"),
        F.col("SR.fec_generado").alias("fecha_transaccion"),
        F.coalesce(F.col(ISA_DIA_181), F.col(SIN_FEC_SINIESTRO)).alias("fecha_ocurrencia"),
        F.col(CS_COD_CAUSA_SINIESTRO).cast("string").alias("cod_causa"),
        F.col(CS_DESCRIPCION).alias("causa"),
        F.col(CO_COD_ORIGEN).cast(decimal_cast).alias("cod_origen"),
        F.col(CO_NOMBRE).alias("origen"),
        F.coalesce(F.col(SIN_FEC_INICIO_SOLICITUD_AFP), F.col(SIN_FEC_SOLICITUD_AFP)).alias("fecha_aviso"),
        F.coalesce(F.col(P_NID_POLIZA), F.lit(999)).alias("poliza"),
        F.lit("2").cast("string").alias("id_fuente_fk"),
        F.lit("50").cast("string").alias("compania"),
        F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
        F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
        F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
        F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider")
    )
)


#  “Pago”
df_pago = (
    PS.alias("PS")
    .join(PG.alias("PG"), F.col("PG.nid_pago") == F.col("PS.nid_pago"))
    .join(CP.alias("CP"), F.col("PS.cod_concepto_pago_siniestro") == F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO))
    .join(TM.alias("TM"), F.col("PS.cod_movimiento") == F.col(TM_COD_MOVIMIENTO))
    .join(SIN.alias("SIN"), F.col("PS.nid_siniestro") == F.col(SIN_NID_SINIESTRO))
    .join(CS.alias("CS"), F.col(SIN_COD_CAUSA_AFP)  == F.col(CS_COD_CAUSA_SINIESTRO))
    .join(CO.alias("CO"), F.col(SIN_COD_ORIGEN)               == F.col(CO_COD_ORIGEN))
    #.join(AF.alias("AF"), F.col("SIN.nid_numero_identificacion") == F.col("AF.afil_cedula"))
    #.join(BP.alias("BP"), F.col("SIN.nid_numero_identificacion") == F.col("BP.id"))
    .join(
        P.alias("P"),
        (F.col(SIN_FEC_SINIESTRO) >= F.col(P_FEC_INICIAL_VIGENCIA)) &
        (F.col(SIN_FEC_SINIESTRO) <= F.col(P_FEC_FINAL_VIGENCIA))
    )
    .join(ISA.alias("ISA"), F.col(SIN_NID_SINIESTRO) == F.col(ISA_NID_SINIESTRO), "left")
    .filter(F.col(PS_VLR_PAGO) != 0)
    .select(
        F.col(CP_COD_CONCEPTO_PAGO_SINIESTRO).cast("string").alias("id_categoria_fk"),
        F.col("CP.descripcion").alias("categoria"),
        F.lit("Pago").alias("tipo"),
        F.col("PS.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
        F.col(TM_DESCRIPCION).alias("tipo_transaccion"),
        F.col("PS.nid_siniestro").cast("string").alias("numero_siniestro"),
        F.abs(F.col(PS_VLR_PAGO)).alias("valor_transaccion"),
        F.col(PS_VLR_PAGO).alias("valor_transaccion_ajustado"),
        F.col("PG.fec_generado").alias("fecha_transaccion"),
        F.coalesce(F.col(ISA_DIA_181), F.col(SIN_FEC_SINIESTRO)).alias("fecha_ocurrencia"),
        F.col(CS_COD_CAUSA_SINIESTRO).cast("string").alias("cod_causa"),
        F.col(CS_DESCRIPCION).alias("causa"),
        F.col(CO_COD_ORIGEN).cast(decimal_cast).alias("cod_origen"),
        F.col(CO_NOMBRE).alias("origen"),
        F.coalesce(F.col(SIN_FEC_INICIO_SOLICITUD_AFP), F.col(SIN_FEC_SOLICITUD_AFP)).alias("fecha_aviso"),
        F.coalesce(F.col(P_NID_POLIZA), F.lit(999)).alias("poliza"),
        F.lit("2").cast("string").alias("id_fuente_fk"),
        F.lit("50").cast("string").alias("compania"),
        F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
        F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
        F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
        F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider")



    )
)


#  “Recuperaciones”
df_recup = (
    RP.alias("RP")
    .join(TR.alias("TR"), F.col("RP.cod_tipo_recuperacion") == F.col("TR.cod_tipo_recuperacion"))
    .join(TM.alias("TM"), F.col("RP.cod_movimiento") == F.col(TM_COD_MOVIMIENTO))
    .join(SIN.alias("SIN"), F.col("RP.nid_siniestro")    == F.col(SIN_NID_SINIESTRO))
    .join(CS.alias("CS"), F.col(SIN_COD_CAUSA_AFP) == F.col(CS_COD_CAUSA_SINIESTRO))
    .join(CO.alias("CO"), F.col(SIN_COD_ORIGEN)               == F.col(CO_COD_ORIGEN))
    #.join(AF.alias("AF"), F.col("SIN.nid_numero_identificacion") == F.col("AF.afil_cedula"))
    #.join(BP.alias("BP"), F.col("SIN.nid_numero_identificacion") == F.col("BP.id"))
    .join(
        P.alias("P"),
        (F.col(SIN_FEC_SINIESTRO) >= F.col(P_FEC_INICIAL_VIGENCIA)) &
        (F.col(SIN_FEC_SINIESTRO) <= F.col(P_FEC_FINAL_VIGENCIA))
    )
    .join(ISA.alias("ISA"), F.col(SIN_NID_SINIESTRO) == F.col(ISA_NID_SINIESTRO), "left")
    .select(
        F.col("TR.cod_tipo_recuperacion").cast("string").alias("id_categoria_fk"),
        F.col("TR.nombre").alias("categoria"),
        F.lit("Recuperaciones").alias("tipo"),
        F.col("RP.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
        F.col(TM_DESCRIPCION).alias("tipo_transaccion"),
        F.col("RP.nid_siniestro").cast("string").alias("numero_siniestro"),
        F.abs(F.col("RP.vlr_recuperacion")).alias("valor_transaccion"),
        F.col("RP.vlr_recuperacion").alias("valor_transaccion_ajustado"),
        F.col("RP.fec_creacion_registro").alias("fecha_transaccion"),
        F.coalesce(F.col(ISA_DIA_181), F.col(SIN_FEC_SINIESTRO)).alias("fecha_ocurrencia"),  
        F.col(CS_COD_CAUSA_SINIESTRO).cast("string").alias("cod_causa"),
        F.col(CS_DESCRIPCION).alias("causa"),
        F.col(CO_COD_ORIGEN).cast(decimal_cast).alias("cod_origen"),
        F.col(CO_NOMBRE).alias("origen"),
        F.coalesce(F.col(SIN_FEC_INICIO_SOLICITUD_AFP), F.col(SIN_FEC_SOLICITUD_AFP)).alias("fecha_aviso"),
        F.coalesce(F.col(P_NID_POLIZA), F.lit(999)).alias("poliza"),
        F.lit("2").cast("string").alias("id_fuente_fk"),
        F.lit("50").cast("string").alias("compania"),
        F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
        F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
        F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
        F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider")
    )
)



df_final = df_reserva.unionByName(df_pago).unionByName(df_recup)


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, LongType, DoubleType, FloatType
from pyspark.sql import functions as F
from datetime import date
# ----------------------------
# Estructuras y DataFrames
# ----------------------------

schema_DR = StructType([
    StructField("nid_reserva", IntegerType(), True),
    StructField("nid_siniestro", IntegerType(), True),
    StructField("cod_concepto_pago_siniestro", StringType(), True),
    StructField("cod_movimiento", StringType(), True),
    StructField("vlr_reserva", IntegerType(), True)
])
DR = spark.createDataFrame([
    (1, 100, "C1", "M1", 1000)
], schema_DR)
DR.createOrReplaceGlobalTempView("spr_detalle_reserva")
schema_SR = StructType([
    StructField("nid_reserva", IntegerType(), True),
    StructField("fec_generado", StringType(), True)
])
SR = spark.createDataFrame([
    (1, "2023-06-10")
], schema_SR)
SR.createOrReplaceGlobalTempView("spr_reserva")
schema_CP = StructType([
    StructField("cod_concepto_pago_siniestro", StringType(), True),
    StructField("descripcion", StringType(), True)
])
CP = spark.createDataFrame([
    ("C1", "Incapacidad Total")
], schema_CP)
CP.createOrReplaceGlobalTempView("spr_conceptos_pagos_siniestros")
schema_TM = StructType([
    StructField("cod_movimiento", StringType(), True),
    StructField("descripcion", StringType(), True)
])
TM = spark.createDataFrame([
    ("M1", "Inicial")
], schema_TM)
TM.createOrReplaceGlobalTempView("cgr_movimientos_reserva")
schema_SIN = StructType([
    StructField("nid_siniestro", IntegerType(), True),
    StructField("cod_causa_siniestro_afp", StringType(), True),
    StructField("cod_origen", StringType(), True),
    StructField("fec_siniestro", StringType(), True),
    StructField("nid_numero_identificacion", StringType(), True),
    StructField("fec_inicio_solicitud_afp", StringType(), True),
    StructField("fec_solicitud_afp", StringType(), True)
])
SIN = spark.createDataFrame([
    (100, "CA1", "OR1", "2023-06-01", "CC123", "2023-06-05", None)
], schema_SIN)
SIN.createOrReplaceGlobalTempView("spr_siniestros")
schema_CS = StructType([
    StructField("cod_causa_siniestro", StringType(), True),
    StructField("descripcion", StringType(), True)
])
CS = spark.createDataFrame([
    ("CA1", "Enfermedad común")
], schema_CS)
CS.createOrReplaceGlobalTempView("spr_causas_siniestros")
schema_CO = StructType([
    StructField("cod_origen", StringType(), True),
    StructField("nombre", StringType(), True)
])
CO = spark.createDataFrame([
    ("OR1", "Origen Laboral")
], schema_CO)
CO.createOrReplaceGlobalTempView("cgr_origenes")
schema_P = StructType([
    StructField("nid_poliza", IntegerType(), True),
    StructField("fec_inicial_vigencia", StringType(), True),
    StructField("fec_final_vigencia", StringType(), True)
])
P = spark.createDataFrame([
    (1, "2023-01-01", "2023-12-31")
], schema_P)
P.createOrReplaceGlobalTempView("ppr_polizas")
schema_ISA = StructType([
    StructField("nid_siniestro", IntegerType(), True),
    StructField("dia_181", StringType(), True)
])
ISA = spark.createDataFrame([
    (100, "2023-07-01")
], schema_ISA)
ISA.createOrReplaceGlobalTempView("spr_incapacidades_sol_audit")
schema_PG = StructType([
    StructField("nid_pago", IntegerType(), True),
    StructField("fec_generado", StringType(), True)
])
PG = spark.createDataFrame([
    (10, "2023-06-15")
], schema_PG)
PG.createOrReplaceGlobalTempView("spr_pagos")
schema_PS = StructType([
    StructField("nid_pago", IntegerType(), True),
    StructField("cod_concepto_pago_siniestro", StringType(), True),
    StructField("cod_movimiento", StringType(), True),
    StructField("nid_siniestro", IntegerType(), True),
    StructField("vlr_pago", IntegerType(), True)
])
PS = spark.createDataFrame([
    (10, "C1", "M1", 100, 800)
], schema_PS)
PS.createOrReplaceGlobalTempView("spr_detalle_pago_siniestro")
schema_RP = StructType([
    StructField("cod_tipo_recuperacion", StringType(), True),
    StructField("cod_movimiento", StringType(), True),
    StructField("nid_siniestro", IntegerType(), True),
    StructField("vlr_recuperacion", IntegerType(), True),
    StructField("fec_creacion_registro", StringType(), True)
])
RP = spark.createDataFrame([
    ("R1", "M1", 100, 400, "2023-06-20")
], schema_RP)
RP.createOrReplaceGlobalTempView("spr_recuperaciones")
schema_TR = StructType([
    StructField("cod_tipo_recuperacion", StringType(), True),
    StructField("nombre", StringType(), True)
])
TR = spark.createDataFrame([
    ("R1", "Recuperación Judicial")
], schema_TR)
TR.createOrReplaceGlobalTempView("spr_tipos_recuperaciones")
from commons.StepGold import StepGold
from commons.IncrementalCharge import IncrementalCharge
from pyspark.sql.types import IntegerType
IncrementalCharge = IncrementalCharge(glueContext, spark, "-", "-")
StepGold = StepGold(glueContext, spark)
table = "DIM_TIPO_TRANSACCION"
#table = "FACT_SINIESTROS-PF"
path_source =f"s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-"
IncrementalCharge.charge_tmp_tables(path_source, table, 'Gold')
from pyspark.sql import functions as F
DF = spark.table("global_temp.fact_movimiento")
DF_S = spark.table("global_temp.siniestros")
df_tp = spark.table("global_temp.dim_tipo_transaccion")
DF = DF_S.join(DF,DF["id_siniestro_fk"] == DF_S["id_siniestro_pk"], "inner")
columnas = ["fecha_transaccion",
        "fecha_aviso",
        #"FECHA_OBJECION", 
        "fecha_ocurrencia"]
from functools import reduce
import traceback
try:
    col_val_exp = reduce(
        lambda acc, c: F.when(F.col(c).isNotNull(), F.col(c)).otherwise(acc), reversed(columnas), F.lit(None)
    )
    col_nm_exp = reduce(lambda acc, c: F.when(F.col(c).isNotNull(), F.lit(c)).otherwise(acc), reversed(columnas), F.lit(None))
except Exception as e:
    print(traceback.format_exc())
    raise e
col_nm_exp = reduce(lambda acc; c: when(col(c).isNotNull(), lit(c)).otherwise(acc), reversed(columnas), lit(None))
df_result = DF.withColumn("valor", col_val_exp).withColumn("campo", col_nm_exp)
i = 0
for column in columnas:
    if i == 0:
        df = DF.select("id_siniestro_pk", F.lit(column).alias("nombre"), F.col(column).alias("valor"), F.col("id_tipo_transaccion_fk"))
        i = 1
    else:
        df_1 = DF.select("id_siniestro_pk", F.lit(column).alias("nombre"), F.col(column).alias("valor"), F.col("id_tipo_transaccion_fk"))
        df = df_1.union(df)
df.show(1)
df = df.withColumn("year", F.year(F.col("valor"))) \
.withColumn("month", F.month(F.col("valor"))) \
.withColumn("month-string", F.date_format(F.col("valor"),"MMM")) \
.withColumn("day", F.dayofmonth(F.col("valor"))) \
.withColumn("trimestre", F.quarter(F.col("valor"))) \
.withColumn("trimestreQ", F.concat(F.lit("Q"), F.quarter(F.col("valor")))) \
.withColumn("id_mov", F.concat(F.col("id_siniestro_pk"), F.col("id_tipo_transaccion_fk"))) \
#trimestre
#trimestre = Q + trimestre
#mes(numero)
#mes(nombre)
#dia

#Id_siniestro + id_tipo_transaccion
df.show(1)
df = df.join(df_tp, F.upper(df["nombre"]) == F.upper(df_tp["tipo_de_transaccion"]), "inner")
df_tp.show()
df.show(1)
"000001-2023-000080-001" -> fk
df.filter(F.col("id_siniestro_pk") == "000001-2023-000080").select("id_siniestro_pk","nombre","valor").count()
job.commit()