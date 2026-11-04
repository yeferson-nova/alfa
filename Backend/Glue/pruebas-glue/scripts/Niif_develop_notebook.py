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
"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim, date_format, to_date, lit, coalesce, min, max, broadcast, year, when, concat_ws, substring, countDistinct, months_between, round, floor, greatest, concat, explode, sequence, log, exp, row_number, regexp_replace, concat, last_day, count, least, trunc, round as spark_round, first, desc, asc
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, DoubleType, DecimalType
from pyspark.sql.window import Window

import sys, traceback, boto3, json

DB_CATALOG_GLUE = 'datalake-dev-silver-catalog-db'
PERIOD = 202411
#Read tables from silver schema arpprod
df_gar_tipos_endosos_arp = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_GAR_TIPOS_ENDOSOS_ARP").toDF()
df_hist_datos_invalidez_muerte = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_HIST_DATOS_INVALIDEZ_MUERTE").toDF()
df_par_afiliaciones = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_PAR_AFILIACIONES").toDF()
df_par_endosos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_PAR_ENDOSOS").toDF()
df_par_polizas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_PAR_POLIZAS").toDF()
df_par_trabajadores = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_PAR_TRABAJADORES").toDF()
df_sar_siniestros = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_SINIESTROS").toDF()

#Read tables from silver schema general

df_acu_personas_juridicas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ACU_PERSONAS_JURIDICAS").toDF()
df_acu_personas_naturales = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ACU_PERSONAS_NATURALES").toDF()
df_cgr_sucursales_alfa = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_SUCURSALES_ALFA").toDF()
df_cgr_tipos_endosos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_TIPOS_ENDOSOS").toDF()
df_rva_reserva_matematica_transic = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_RVA_RESERVA_MATEMATICA_TRANSIC").toDF()
df_par_endosos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_PAR_ENDOSOS").toDF()
df_par_endosos = df_par_endosos.filter(df_par_endosos["PERIODO_POINT"].isin(PERIOD))
df_join_par_endosos = df_par_endosos.join(
    broadcast(df_cgr_tipos_endosos),
    df_par_endosos["cod_tipo_endoso"] == df_cgr_tipos_endosos["cod_tipo_endoso"],
    "inner"
)
df_join_par_endosos = df_join_par_endosos.join(
    broadcast(df_gar_tipos_endosos_arp),
    df_par_endosos["cod_tipo_endoso_arp"] == df_gar_tipos_endosos_arp["cod_tipo_endoso_arp"],
    "inner"
)

df_join_par_endosos = df_join_par_endosos.join(
    df_par_polizas,
    (df_par_endosos["cod_sucursal_alfa"] == df_par_polizas["cod_sucursal_alfa"]) & (df_par_endosos["nid_poliza"] == df_par_polizas["nid_poliza"]) & (df_par_endosos["modulo"] == df_par_polizas["modulo"]),
    "inner"
)

df_join_par_endosos = df_join_par_endosos.join(
    broadcast(df_cgr_sucursales_alfa),
    df_par_endosos["cod_sucursal_alfa"] == df_cgr_sucursales_alfa["cod_sucursal_alfa"],
    "inner"
)

df_join_par_endosos = df_join_par_endosos.join(
    df_par_afiliaciones,
    df_par_endosos["nid_poliza"] == df_par_afiliaciones["nid_afiliacion"],
    "inner"
)

df_join_par_endosos = df_join_par_endosos.join(
    df_acu_personas_juridicas,
    (df_par_afiliaciones["cod_tipo_identificacion"] == df_acu_personas_juridicas["cod_tipo_identificacion"]) & (df_par_afiliaciones["nid_numero_identificacion"] == df_acu_personas_juridicas["nid_numero_identificacion"]) ,
    "left"
)


df_join_par_endosos = df_join_par_endosos.join(
    df_acu_personas_naturales,
    (df_par_afiliaciones["cod_tipo_identificacion"] == df_acu_personas_naturales["cod_tipo_identificacion"]) & (df_par_afiliaciones["nid_numero_identificacion"] == df_acu_personas_naturales["nid_numero_identificacion"]) ,
    "left"
)


df_join_par_endosos = df_join_par_endosos.join(
    df_par_trabajadores,
    df_par_endosos["nid_poliza"] == df_par_trabajadores["nid_afiliacion"],
    "left"
)


df_join_par_endosos = df_join_par_endosos.join(
    df_sar_siniestros,
    df_sar_siniestros["nid_trabajador"] == df_par_trabajadores["nid_trabajador"],
    "left"
)


df_join_par_endosos = df_join_par_endosos.join(
    broadcast(df_hist_datos_invalidez_muerte),
    df_sar_siniestros["nid_siniestro"] == df_hist_datos_invalidez_muerte["nid_siniestro"],
    "left"
)

df_join_par_endosos = df_join_par_endosos.join(
    df_rva_reserva_matematica_transic,
    df_sar_siniestros["nid_siniestro"] == df_rva_reserva_matematica_transic["nid_siniestro_renta"],
    "left"
)
df_select_subquerie_par_endosos = df_join_par_endosos.select (
    df_par_endosos["nid_poliza"],
    df_par_endosos["nid_endoso"],
    df_par_endosos["vlr_prima"],
    df_par_endosos["fec_expedicion"],
    df_par_endosos["periodo"].alias("periodo_expedicion"),
    df_par_endosos["fec_proceso_point"],
    df_par_endosos["periodo_point"],
    df_par_endosos["cod_tipo_endoso"],
    df_par_endosos["cod_tipo_endoso_arp"],
    df_par_endosos["cod_sucursal_alfa"],
    df_cgr_sucursales_alfa["nombre"].alias("nom_sucursal_alfa"),
    df_par_endosos["modulo"],
    df_par_afiliaciones["nid_afiliacion"],
    df_par_afiliaciones["cod_tipo_identificacion"],
    df_par_afiliaciones["nid_numero_identificacion"],
    df_acu_personas_juridicas["cod_tipo_identificacion"].alias("apj_cod_tipo_identificacion"),
    df_acu_personas_juridicas["nid_numero_identificacion"].alias("apj_nid_numero_identificacion"),
    df_acu_personas_juridicas["nombre_comercial"],
    df_acu_personas_juridicas["razon_social"],
    df_acu_personas_naturales["cod_tipo_identificacion"].alias("apn_cod_tipo_identificacion"),
    df_acu_personas_naturales["nid_numero_identificacion"].alias("apn_nid_numero_identificacion"),
    df_acu_personas_naturales["primer_apellido"],
    df_acu_personas_naturales["segundo_apellido"],
    df_acu_personas_naturales["primer_nombre"],
    df_acu_personas_naturales["segundo_nombre"],
    when(df_acu_personas_juridicas["cod_tipo_identificacion"].isNotNull() & df_acu_personas_naturales["cod_tipo_identificacion"].isNotNull(), 1)
        .otherwise(0)
        .alias("val_razon_natural"),
    df_par_trabajadores["nid_trabajador"],
    df_sar_siniestros["nid_siniestro"]
).distinct()
df_select_subquerie_par_endosos.count()
df_primas_arl_query = df_select_subquerie_par_endosos.select (
    df_select_subquerie_par_endosos["nid_poliza"],
    df_select_subquerie_par_endosos["nid_endoso"],
    df_select_subquerie_par_endosos["vlr_prima"],
    df_select_subquerie_par_endosos["fec_expedicion"],
    df_select_subquerie_par_endosos["periodo_expedicion"],
    df_select_subquerie_par_endosos["fec_proceso_point"],
    df_select_subquerie_par_endosos["periodo_point"],
    df_select_subquerie_par_endosos["cod_tipo_endoso"],
    df_select_subquerie_par_endosos["cod_tipo_endoso_arp"],
    df_select_subquerie_par_endosos["cod_sucursal_alfa"],
    df_select_subquerie_par_endosos["nom_sucursal_alfa"],
    df_select_subquerie_par_endosos["modulo"],
    df_select_subquerie_par_endosos["nid_afiliacion"],
    df_select_subquerie_par_endosos["cod_tipo_identificacion"],
    df_select_subquerie_par_endosos["nid_numero_identificacion"],

    when(df_select_subquerie_par_endosos["APJ_COD_TIPO_IDENTIFICACION"].isNull(),df_select_subquerie_par_endosos["APN_COD_TIPO_IDENTIFICACION"])
    .otherwise(df_select_subquerie_par_endosos["APJ_COD_TIPO_IDENTIFICACION"])
    .alias("cod_tipo_identificacion_tercero"),

    when(df_select_subquerie_par_endosos["APJ_NID_NUMERO_IDENTIFICACION"].isNull(),df_select_subquerie_par_endosos["APN_NID_NUMERO_IDENTIFICACION"])
    .otherwise(df_select_subquerie_par_endosos["APJ_NID_NUMERO_IDENTIFICACION"])
    .alias("nid_numero_identificacion_tercero"),

    when(df_select_subquerie_par_endosos["apj_cod_tipo_identificacion"].isNotNull() & df_select_subquerie_par_endosos["apj_nid_numero_identificacion"].isNotNull() & df_select_subquerie_par_endosos["nombre_comercial"].isNotNull() & (df_select_subquerie_par_endosos["nombre_comercial"] == df_select_subquerie_par_endosos["razon_social"]),df_select_subquerie_par_endosos["nombre_comercial"])
    .when(df_select_subquerie_par_endosos["apj_cod_tipo_identificacion"].isNotNull() & df_select_subquerie_par_endosos["apj_nid_numero_identificacion"].isNotNull() & df_select_subquerie_par_endosos["nombre_comercial"].isNull() ,df_select_subquerie_par_endosos["razon_social"])
    .when(df_select_subquerie_par_endosos["apj_cod_tipo_identificacion"].isNotNull() & df_select_subquerie_par_endosos["apj_nid_numero_identificacion"].isNotNull() & df_select_subquerie_par_endosos["nombre_comercial"].isNotNull() & (df_select_subquerie_par_endosos["nombre_comercial"] != df_select_subquerie_par_endosos["razon_social"]),df_select_subquerie_par_endosos["razon_social"])
    .otherwise(concat_ws(" ", 
        df_select_subquerie_par_endosos["primer_apellido"], 
        df_select_subquerie_par_endosos["segundo_apellido"], 
        df_select_subquerie_par_endosos["primer_nombre"], 
        df_select_subquerie_par_endosos["segundo_nombre"] ))
    .alias("nom_tercero"),    

    df_select_subquerie_par_endosos["val_razon_natural"],
    df_select_subquerie_par_endosos["nid_trabajador"],
    df_select_subquerie_par_endosos["nid_siniestro"]
).distinct()
df_primas_arl_query.count()
df_primas_arl_query = df_primas_arl_query.withColumn("PERIODO_PARTITION",date_format(to_date(col("periodo_point"), "yyyyMM"), "yyyy-MM"))

df_primas_arl_gold = df_primas_arl_query.select(

    df_primas_arl_query["nid_poliza"],
    df_primas_arl_query["nid_endoso"],
    df_primas_arl_query["vlr_prima"],
    df_primas_arl_query["fec_expedicion"].cast(DateType()),
    df_primas_arl_query["periodo_expedicion"].alias("per_expedicion"),
    df_primas_arl_query["fec_proceso_point"].cast(DateType()),
    df_primas_arl_query["periodo_point"].alias("per_proceso_point"),
    df_primas_arl_query["cod_tipo_endoso"],
    df_primas_arl_query["cod_tipo_endoso_arp"],
    df_primas_arl_query["cod_sucursal_alfa"],
    df_primas_arl_query["nom_sucursal_alfa"],
    df_primas_arl_query["modulo"],
    df_primas_arl_query["nid_afiliacion"],
    df_primas_arl_query["cod_tipo_identificacion_tercero"],
    df_primas_arl_query["nid_numero_identificacion_tercero"],
    df_primas_arl_query["nom_tercero"],
    df_primas_arl_query["nid_trabajador"],
    df_primas_arl_query["nid_siniestro"],
    lit(None).cast(IntegerType()).alias("id_lote_cargue"),
    df_primas_arl_query["PERIODO_PARTITION"]
).distinct()
df_primas_arl_gold.count()
"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim, date_format, to_date, lit, coalesce, min, max, broadcast, year, when, concat_ws, substring, countDistinct, months_between, round, floor, greatest, concat, explode, sequence, log, exp, row_number, regexp_replace, concat, last_day, count, least, trunc, round as spark_round, first, desc, asc
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, DoubleType, DecimalType
from pyspark.sql.window import Window

import sys, traceback, boto3, json


BUCKET_NAME = 'alfa-datalake-dev-gold'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
DB_NAME = 'ALFAPRD'
DB_SCHEMA = 'PRV'
PERIOD = '202501'
CERTIFICATE = 'False'
DB_CATALOG_GLUE_SILVER = 'datalake-dev-silver-catalog-db'
DB_CATALOG_GLUE_BRONCE = 'datalake-dev-bronce-catalog-db'
TABLE_NAME_GOLD = 'RESERVA_ARP_GOLD'
#FEC_CORTE = '2025-04-16'
FEC_CORTE_DATE = '2025-01-01'


extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME, DB_NAME)

#----------------------------------------------------------------StructType final-----------------------------------------------------------------------------------------------------------#
print(f"StructType final: {TABLE_NAME_GOLD}")

# Define the schema for the  Final DataFrame
schema_reserva_arp_gold = StructType([
        StructField("RAMO", IntegerType(), True),                                      # campo 1 - NUMBER(2,0)
        StructField("UNIDAD", IntegerType(), True),                                    # campo 2 - NUMBER(1,0)
        StructField("EQUIVALENCIA", StringType(), True),                               # campo 3 - NUMBER(1,0)
        StructField("NO_POLIZA", IntegerType(), True),                                 # campo 4 - NUMBER(10,0)
        StructField("VIG_INICIAL", DateType(), True),                                  # campo 5 - DATE YYYYY-MM-DD
        StructField("NO_SINIESTRO", IntegerType(), True),                              # campo 6 - NUMBER(10,0)
        StructField("FECHA_CAUSACION_DERECHO", DateType(), True),                      # campo 7 - DATE YYYYY-MM-DD

        StructField("PERIODO_CAUSACION_DERECHO", StringType(), True),              # campo 8 - VARCHAR2(3 BYTE)        
        StructField("FECHA_INICIO_PERIODO_CAUSACION_DERECHO", DateType(), True),    # campo 9 - DATE YYYYY-MM-DD
        StructField("FECHA_FIN_PERIODO_CAUSACION_DERECHO", DateType(), True),       # campo 10 - DATE YYYYY-MM-DD

        StructField("ORIGEN_PENSION", IntegerType(), True),                         # campo 11 - NUMBER(1,0)
        StructField("PARENT", IntegerType(), True),                                 # campo 12 - NUMBER(1,0)
        StructField("TIPO_IDENTIF", IntegerType(), True),                           # campo 13  - NUMBER(2,0)
        StructField("NO_IDENTIF", StringType(), True),                              # campo 14 - VARCHAR2(15 BYTE)
        StructField("SEXO", IntegerType(), True),                                   # campo 15 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO", DateType(), True),                          # campo 16 - DATE YYYYY-MM-DD
        StructField("NOMBRE", StringType(), True),                                  # campo 17 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS", IntegerType(), True),                               # campo 18 - NUMBER(1,0)
        StructField("ESTADO", IntegerType(), True),                                 # campo 19 - NUMBER(1,0)
        StructField("NO_HIJOS", IntegerType(), True),                               # campo 20 - NUMBER(15,0) ***
        StructField("NO_HIJOS_INVALIDOS", IntegerType(), True),                     # campo 21 - NUMBER(15,0) ***
        StructField("INGRESO_BASE_LIQUIDACION", DecimalType(10,2), True),           # campo 22 - VARCHAR2(3 BYTE)
        StructField("MESADA", DecimalType(16,2), True),                             # campo 23 - DECIMAL(16,2)
        StructField("NO_MESADAS", IntegerType(), True),                             # campo 24 - DECIMAL(2,0)
        StructField("RETROACTIVO", DecimalType(10,2), True),                        # campo 25 - NUMBER(10,2)   +++
        StructField("GRADO_INVALIDEZ_INICIAL", DecimalType(5,2), True),             # campo 26 - DECIMAL(5,2)
        StructField("FECHA_ULTIMA_MOD", DateType(), True),                          # campo 27 - DATE YYYYY-MM-DD
        StructField("GRADO_INVALIDEZ_ACTUAL", DecimalType(5,2), True),              # campo 28 - DECIMAL(5,2)
        StructField("GASTOS", DecimalType(5,4), True),                              # campo 29 - DECIMAL(5,4)

        StructField("PARENT_1", IntegerType(), True),                               # campo 30 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_1", IntegerType(), True),                         # campo 31 - NUMBER(1,0)
        StructField("NO_IDENTIF_1", StringType(), True),                            # campo 32 - NUMBER(15,0)
        StructField("SEXO_1", IntegerType(), True),                                 # campo 33 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_1", DateType(), True),                        # campo 34 - DATE YYYYY-MM-DD
        StructField("NOMBRE_1", StringType(), True),                                # campo 35 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_1", IntegerType(), True),                             # campo 36 - NUMBER(1,0)
        StructField("ESTADO_1", IntegerType(), True),                               # campo 37 - NUMBER(1,0)
        StructField("CON_DERECHO_1", StringType(), True),                           # campo 38 - VARCHAR(1)

        StructField("PARENT_2", IntegerType(), True),                               # campo 39 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_2", IntegerType(), True),                         # campo 40 - NUMBER(1,0)
        StructField("NO_IDENTIF_2", StringType(), True),                            # campo 41 - NUMBER(15,0)
        StructField("SEXO_2", IntegerType(), True),                                 # campo 42 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_2", DateType(), True),                        # campo 43 - DATE YYYYY-MM-DD
        StructField("NOMBRE_2", StringType(), True),                                # campo 44 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_2", IntegerType(), True),                             # campo 45 - NUMBER(1,0)
        StructField("ESTADO_2", IntegerType(), True),                               # campo 46 - NUMBER(1,0)
        StructField("CON_DERECHO_2", StringType(), True),                           # campo 47 - VARCHAR(1)

        StructField("PARENT_3", IntegerType(), True),                               # campo 48 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_3", IntegerType(), True),                         # campo 49 - NUMBER(1,0)
        StructField("NO_IDENTIF_3", StringType(), True),                            # campo 50 - NUMBER(15,0)
        StructField("SEXO_3", IntegerType(), True),                                 # campo 51- NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_3", DateType(), True),                        # campo 52 - DATE YYYYY-MM-DD
        StructField("NOMBRE_3", StringType(), True),                                # campo 53 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_3", IntegerType(), True),                             # campo 54 - NUMBER(1,0)
        StructField("ESTADO_3", IntegerType(), True),                               # campo 55 - NUMBER(1,0)
        StructField("CON_DERECHO_3", StringType(), True),                           # campo 56 - VARCHAR(1)

        StructField("PARENT_4", IntegerType(), True),                               # campo 57 -  NUMBER(1,0)
        StructField("TIPO_IDENTIF_4", IntegerType(), True),                         # campo 58 - NUMBER(1,0)
        StructField("NO_IDENTIF_4", StringType(), True),                            # campo 59 - NUMBER(15,0)
        StructField("SEXO_4", IntegerType(), True),                                 # campo 60 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_4", DateType(), True),                        # campo 61 - DATE YYYYY-MM-DD
        StructField("NOMBRE_4", StringType(), True),                                # campo 62 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_4", IntegerType(), True),                             # campo 63 - NUMBER(1,0)
        StructField("ESTADO_4", IntegerType(), True),                               # campo 64 - NUMBER(1,0)
        StructField("CON_DERECHO_4", StringType(), True),                           # campo 65 - VARCHAR(1)

        StructField("PARENT_5", IntegerType(), True),                               # campo 66 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_5", IntegerType(), True),                         # campo 67 - NUMBER(1,0)
        StructField("NO_IDENTIF_5", StringType(), True),                            # campo 68 - NUMBER(15,0)
        StructField("SEXO_5", IntegerType(), True),                                 # campo 69 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_5", DateType(), True),                        # campo 70 - DATE YYYYY-MM-DD
        StructField("NOMBRE_5", StringType(), True),                                # campo 71 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_5", IntegerType(), True),                             # campo 72 - NUMBER(1,0)
        StructField("ESTADO_5", IntegerType(), True),                               # campo 73 - NUMBER(1,0)
        StructField("CON_DERECHO_5", StringType(), True),                           # campo 74 - VARCHAR(1)

        StructField("PARENT_6", IntegerType(), True),                               # campo 75 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_6", IntegerType(), True),                         # campo 76 - NUMBER(1,0)
        StructField("NO_IDENTIF_6", StringType(), True),                            # campo 77 - NUMBER(15,0)
        StructField("SEXO_6", IntegerType(), True),                                 # campo 78 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_6", DateType(), True),                        # campo 79 - DATE YYYYY-MM-DD
        StructField("NOMBRE_6", StringType(), True),                                # campo 80 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_6", IntegerType(), True),                             # campo 81 - NUMBER(1,0)
        StructField("ESTADO_6", IntegerType(), True),                               # campo 82 - NUMBER(1,0)
        StructField("CON_DERECHO_6", StringType(), True),                           # campo 83 - VARCHAR(1)

        StructField("PARENT_7", IntegerType(), True),                               # campo 84 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_7", IntegerType(), True),                         # campo 85 - NUMBER(1,0)
        StructField("NO_IDENTIF_7", StringType(), True),                            # campo 86 - NUMBER(15,0)
        StructField("SEXO_7", IntegerType(), True),                                 # campo 87 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_7", DateType(), True),                        # campo 88 - DATE YYYYY-MM-DD
        StructField("NOMBRE_7", StringType(), True),                                # campo 89 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_7", IntegerType(), True),                             # campo 90 - NUMBER(1,0)
        StructField("ESTADO_7", IntegerType(), True),                               # campo 91 - NUMBER(1,0)
        StructField("CON_DERECHO_7", StringType(), True),                           # campo 92 - VARCHAR(1)

        StructField("PARENT_8", IntegerType(), True),                               # campo 93 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_8", IntegerType(), True),                         # campo 94 - NUMBER(1,0)
        StructField("NO_IDENTIF_8", StringType(), True),                            # campo 95 - NUMBER(15,0)
        StructField("SEXO_8", IntegerType(), True),                                 # campo 96 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_8", DateType(), True),                        # campo 97 - DATE YYYYY-MM-DD
        StructField("NOMBRE_8", StringType(), True),                                # campo 98 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_8", IntegerType(), True),                             # campo 99 - NUMBER(1,0)
        StructField("ESTADO_8", IntegerType(), True),                               # campo 100 - NUMBER(1,0)
        StructField("CON_DERECHO_8", StringType(), True),                           # campo 101 - VARCHAR(1)

        StructField("PARENT_9", IntegerType(), True),                               # campo 102 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_9", IntegerType(), True),                         # campo 103 - NUMBER(1,0)
        StructField("NO_IDENTIF_9", StringType(), True),                            # campo 104 - NUMBER(15,0)
        StructField("SEXO_9", IntegerType(), True),                                 # campo 105 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_9", DateType(), True),                        # campo 106 - DATE YYYYY-MM-DD
        StructField("NOMBRE_9", StringType(), True),                                # campo 107 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_9", IntegerType(), True),                             # campo 108 - NUMBER(1,0)
        StructField("ESTADO_9", IntegerType(), True),                               # campo 109 - NUMBER(1,0)
        StructField("CON_DERECHO_9", StringType(), True),                           # campo 110 - VARCHAR(1)

        StructField("PARENT_10", IntegerType(), True),                              # campo 111 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_10", IntegerType(), True),                        # campo 112 - NUMBER(1,0)
        StructField("NO_IDENTIF_10", StringType(), True),                           # campo 113 - NUMBER(15,0)
        StructField("SEXO_10", IntegerType(), True),                                # campo 114 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_10", DateType(), True),                       # campo 115 - DATE YYYYY-MM-DD
        StructField("NOMBRE_10", StringType(), True),                               # campo 116 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_10", IntegerType(), True),                            # campo 117 - NUMBER(1,0)
        StructField("ESTADO_10", IntegerType(), True),                              # campo 118 - NUMBER(1,0)
        StructField("CON_DERECHO_10", StringType(), True),                          # campo 119 - VARCHAR(1)

        StructField("PARENT_11", IntegerType(), True),                              # campo 120 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_11", IntegerType(), True),                        # campo 121 - NUMBER(1,0)
        StructField("NO_IDENTIF_11", StringType(), True),                           # campo 122 - NUMBER(15,0)
        StructField("SEXO_11", IntegerType(), True),                                # campo 123 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_11", DateType(), True),                       # campo 124 - DATE YYYYY-MM-DD
        StructField("NOMBRE_11", StringType(), True),                               # campo 125 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_11", IntegerType(), True),                            # campo 126 - NUMBER(1,0)
        StructField("ESTADO_11", IntegerType(), True),                              # campo 127 - NUMBER(1,0)
        StructField("CON_DERECHO_11", StringType(), True),                          # campo 128 - VARCHAR(1)

        StructField("PARENT_12", IntegerType(), True),                              # campo 129 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_12", IntegerType(), True),                        # campo 130 - NUMBER(1,0)
        StructField("NO_IDENTIF_12", StringType(), True),                           # campo 131 - NUMBER(15,0)
        StructField("SEXO_12", IntegerType(), True),                                # campo 132 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_12", DateType(), True),                       # campo 133 - DATE YYYYY-MM-DD
        StructField("NOMBRE_12", StringType(), True),                               # campo 134 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_12", IntegerType(), True),                            # campo 135 - NUMBER(1,0)
        StructField("ESTADO_12", IntegerType(), True),                              # campo 136 - NUMBER(1,0)
        StructField("CON_DERECHO_12", StringType(), True),                          # campo 137 - VARCHAR(1)

        StructField("PARENT_13", IntegerType(), True),                              # campo 138 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_13", IntegerType(), True),                        # campo 139 - NUMBER(1,0)
        StructField("NO_IDENTIF_13", StringType(), True),                           # campo 140 - NUMBER(15,0)
        StructField("SEXO_13", IntegerType(), True),                                # campo 141 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_13", DateType(), True),                       # campo 142 - DATE YYYYY-MM-DD
        StructField("NOMBRE_13", StringType(), True),                               # campo 143 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_13", IntegerType(), True),                            # campo 144 - NUMBER(1,0)
        StructField("ESTADO_13", IntegerType(), True),                              # campo 145 - NUMBER(1,0)
        StructField("CON_DERECHO_13", StringType(), True),                          # campo 146 - VARCHAR(1)

        StructField("PARENT_14", IntegerType(), True),                              # campo 147 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_14", IntegerType(), True),                        # campo 148 - NUMBER(1,0)
        StructField("NO_IDENTIF_14", StringType(), True),                           # campo 149 - NUMBER(15,0)
        StructField("SEXO_14", IntegerType(), True),                                # campo 150 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_14", DateType(), True),                       # campo 151 - DATE YYYYY-MM-DD
        StructField("NOMBRE_14", StringType(), True),                               # campo 152 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_14", IntegerType(), True),                            # campo 153 - NUMBER(1,0)
        StructField("ESTADO_14", IntegerType(), True),                              # campo 154 - NUMBER(1,0)
        StructField("CON_DERECHO_14", StringType(), True),                          # campo 155 - VARCHAR(1)

        StructField("PARENT_15", IntegerType(), True),                              # campo 156 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_15", IntegerType(), True),                        # campo 157 - NUMBER(1,0)
        StructField("NO_IDENTIF_15", StringType(), True),                           # campo 158 - NUMBER(15,0)
        StructField("SEXO_15", IntegerType(), True),                                # campo 159 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_15", DateType(), True),                       # campo 160 - DATE YYYYY-MM-DD
        StructField("NOMBRE_15", StringType(), True),                               # campo 161 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_15", IntegerType(), True),                            # campo 162 - NUMBER(1,0)
        StructField("ESTADO_15", IntegerType(), True),                              # campo 163 - NUMBER(1,0)
        StructField("CON_DERECHO_15", StringType(), True),                          # campo 164 - VARCHAR(1)

        StructField("NO_RAD_NOT_TEC", DecimalType(20,0), True),                     # campo 165 - NUMBER(20,0)
        StructField("INTERES_TEC_PRIMA", DecimalType(15,10), True),                 # campo 166 - DECIMAL(15,10)
        StructField("TASA_CREC_PRIMA", DecimalType(5,2), True),                     # campo 167 - DECIMAL(5,2)
        StructField("GASTOS_PRIMA", DecimalType(5,4), True),                        # campo 168 - DECIMAL(5,4)
        StructField("PRIMA", DecimalType(17,2), True),                              # campo 169 - DECIMAL(30,2)
        StructField("FECHA_INICIAL_RENTA_DIFERIDA", DateType(), True),              # campo 170 - DATE YYYYY-MM-DD
        StructField("VALOR_MESADA_BENEF_PERIO_DIFER", DecimalType(10,2), True),     # campo 171 - DECIMAL(10,2)
        StructField("VALOR_AUXILIO_FUNERARIO", IntegerType(), True),                # campo 172 - NUMBER(20,0)
        StructField("OBSERVACIONES", StringType(), True),                           # campo 173 - VARCHAR2(80 BYTE)
        StructField("VALOR_RESERVA_RV08", DecimalType(14,2), True),                 # campo 174 - NUMBER(14,2)
        StructField("VALOR_RESERVA_RV89", DecimalType(14,2), True),                 # campo 175 - NUMBER(14,2)
        StructField("PAR_0",DecimalType(28,10), True),                              # campo 176 - N/A
        StructField("PARMA", DecimalType(28,10), True),                             # campo 177 - N/A
        StructField("PORCENTAJE_EXC_PARMA", DecimalType(10,2), True),               # campo 178 - N/A
        StructField("POR_AMORT_ACUM", DecimalType(10,2), True),                     # campo 179 - NUMBER(5,2)
        StructField("FEC_AVISO_SINIESTRO", DateType(), True),                       # campo 180 - N/A
        StructField("FAIR_VALUE", DecimalType(14,2), True),                         # campo 181 - N/A
        StructField("INSCRIPCION_MECANISMO", IntegerType(), True),                  # campo 182 - VARCHAR2(1 BYTE)
        StructField("FECHA_INSCRIPCION", DateType(), True),                         # campo 183 - DATE YYYYY-MM-DD
        StructField("PARAMETRO_DESLIZAMIENTO", DecimalType(5,4), True),             # campo 184 - NUMBER(20,15)
        StructField("TASA_FAC_SEG", DecimalType(5,4), True),                        # campo 185 - PARAMETERS
        StructField("VALOR_COBERTURA", DecimalType(14,2), True),                    # campo 186 - NUMBER(14,2)
        StructField("FECHA_EXPED_POLIZA", DateType(), True),                        # campo 187 - DATE YYYYY-MM-DD
        StructField("FECHA_MUERTE_AFILIADO", DateType(), True),                     # campo 188 - DATE YYYYY-MM-DD
        StructField("INTERES_TECNICO_NIIF4", DecimalType(15,10), True),             # campo 189 - NUMBER(15)

        StructField("RAMO_RESERVA", StringType(), True),                            # campo 190 - VARCHAR2(3 BYTE)
        StructField("COD_CONCEPTO_MEDICO_AM", StringType(), True),                  # campo 191 - VARCHAR2(3 BYTE)
        StructField("COD_ESTADO_RESERVA", StringType(), True),                      # campo 192 - VARCHAR2(3 BYTE)
        StructField("NO_CALCULO", DecimalType(10, 0), True),                        # campo 193 - NUMBER(10,0) [Entero]
        StructField("FECHA_CALCULO", DateType(), True),                             # campo 194 - DATE (YYYY-MM-DD)
        StructField("ANO_SALARIO_MINIMO", DecimalType(4, 0), True),                 # campo 195 - NUMBER(4,0) [Entero]
        StructField("MES_SALARIO_MINIMO", DecimalType(2, 0), True),                 # campo 196 - NUMBER(2,0) [Entero]
        StructField("VLR_SALARIO_MINIMO", DecimalType(14, 2), True),                # campo 197 - NUMBER(14,2) [Monetario]
        StructField("COD_COBERTURA", DecimalType(10, 0), True),                     # campo 198 - NUMBER(10,0) [Entero]
        StructField("FEC_PAGO_COBERTURA", DateType(), True),                        # campo 199 - DATE (YYYY-MM-DD)
        StructField("NO_PAGO", IntegerType(), True),                                # campo 200 - NUMBER(20,0)


        StructField("ID_LOTE_CARGUE", IntegerType(), True)                          # campo 190 - partition
        ]) 


#-------------------------------------------------------------------------Extract source arpprod and general-----------------------------------------------------------------------------#    
print(f"Extract source: {DB_SCHEMA}")
#Read tables from silver arpprod
df_hist_datos_invalidez_muerte = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_ARPPROD_SILVER_HIST_DATOS_INVALIDEZ_MUERTE").toDF()
df_hist_sar_pcl_pension = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_ARPPROD_SILVER_HIST_SAR_PCL_PENSION").toDF()
df_ind_datos_reservas_siniestros = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_ARPPROD_SILVER_IND_DATOS_RESERVAS_SINIESTROS").toDF()
df_par_trabajadores = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_ARPPROD_SILVER_PAR_TRABAJADORES").toDF()
df_sar_auditoria_medica = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_AUDITORIA_MEDICA").toDF()
df_sar_beneficiarios_siniestros = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_BENEFICIARIOS_SINIESTROS").toDF()
df_sar_datos_invalidez_muerte = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_DATOS_INVALIDEZ_MUERTE").toDF()
df_sar_reserva_pcj_amortizacion = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_RESERVA_PCJ_AMORTIZACION").toDF()
df_sar_siniestros = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_SINIESTROS").toDF()
df_sar_pagos_coberturas  = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_PAGOS_COBERTURAS").toDF()
df_sar_calculos_actuariales = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_CALCULOS_ACTUARIALES").toDF()

#Read tables from silver general
df_acu_personas_naturales = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ACU_PERSONAS_NATURALES").toDF()
df_cgr_equivalencia_tipos_id = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_CGR_EQUIVALENCIA_TIPOS_ID").toDF()	
df_anp_ipc = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ANP_IPC").toDF()
df_rva_reserva_matematica_transic = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_RVA_RESERVA_MATEMATICA_TRANSIC").toDF()
df_cgr_origenes = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_CGR_ORIGENES").toDF()
df_pgr_salarios_minimos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_PGR_SALARIOS_MINIMOS").toDF()

#-------------------------------------------------------------------------Extract source FILE              -----------------------------------------------------------------------------#    
print(f"Extract source: FILE")
#Read tables from silver arpprod
df_files_campos_unica_vez = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_BRONCE}", table_name="FILES_CAMPOS_UNICA_VEZ").toDF()

#---------------------------------------------------------------------Init Businees Logic---------------------------------------------------------------------------------------------------#

print(f"Writing to {TABLE_NAME_GOLD} with period = {PERIOD}, Init Businees Logic")
from datetime import datetime, timedelta, date
#---------------------------------------------------------------------alias table's ---------------------------------------------------------------------------------------------------------#
print(f"Alias for all DataFrames of  {DB_SCHEMA}")
# Alias for all DataFrames of ARPPROD
sar = df_sar_siniestros.alias("sar")
tra = df_par_trabajadores.alias("tra")
inv = df_sar_datos_invalidez_muerte.alias("inv")
aud = df_sar_auditoria_medica.alias("aud")
res = df_ind_datos_reservas_siniestros.alias("res")
ben = df_sar_beneficiarios_siniestros.alias("ben")
mue = df_hist_datos_invalidez_muerte.alias("mue")
pen = df_hist_sar_pcl_pension.alias("pen")
ion = df_sar_reserva_pcj_amortizacion.alias("ion")
cob = df_sar_pagos_coberturas.alias("cob")
cal = df_sar_calculos_actuariales.alias("cal")

# Alias para todos los DataFrames de GENERAL
ben_apn = df_acu_personas_naturales.alias("ben_apn")
tra_apn = df_acu_personas_naturales.alias("tra_apn")
ben_eqid = df_cgr_equivalencia_tipos_id.alias("ben_eqid")
tra_eqid = df_cgr_equivalencia_tipos_id.alias("tra_eqid")
org = df_cgr_origenes.alias("org")
ipc = df_anp_ipc.alias("ipc")
sic = df_rva_reserva_matematica_transic.alias("sic")
mos = df_pgr_salarios_minimos.alias("mos")

# Alias para tablas de files bronce
campos_unica_vez = df_files_campos_unica_vez.alias("campos_unica_vez")

#-------------------------------------------------------------------------Variables-----------------------------------------------------------------------------------------------------# 
print(f"Calculation of Variables")
# Convertir a objeto datetime
dt = datetime.strptime(FEC_CORTE_DATE, "%Y-%m-%d")

# Periodo (yyyymm)
FEC_CORTE_PERIOD = dt.strftime("%Y%m")
STR_CORTE_PERIOD = date_format(dt.strftime("%Y%m"), "yyyyMM")

# Año (yyyy)
FEC_CORTE_ANIO = int(dt.strftime("%Y")) 
from pyspark.sql.functions import col, trim, date_format, to_date, lit, coalesce, min, max, broadcast, year, when, concat_ws, substring, countDistinct, months_between, round, floor, greatest, concat, explode, sequence, log, exp, row_number, regexp_replace, concat, last_day, count, least, trunc, round as spark_round, first
#----------------------------------------------------------withColumn source--------------------------------------------------------------------------------------------------------------#
print(f"Add columns to sources")
df_existe_ion = ion.select("nid_siniestro").dropDuplicates()

sar = sar.join(
    df_existe_ion,  # sin columna existe todavía
    on="nid_siniestro",
    how="left"
).withColumn(
    "existe",
    when(col("nid_siniestro").isNotNull(), lit(True)).otherwise(lit(False))
)

sar = sar.withColumn("periodo_siniestro", date_format(col("sar.fec_siniestro"), "yyyyMM")) \
         .withColumn(
             "fecha_inicio_periodo_siniestro",
             to_date(
                 concat(date_format(col("sar.fec_siniestro"), "yyyy-MM"), lit("-01")),
                 "yyyy-MM-dd"
             )
         ) \
         .withColumn(
             "fecha_fin_periodo_siniestro",
             last_day(
                 to_date(
                     concat(date_format(col("sar.fec_siniestro"), "yyyy-MM"), lit("-01")),
                     "yyyy-MM-dd"
                 )
             )
         )


ben = ben.withColumn("cod_parentesco_int",
    when(col("cod_parentesco").rlike("^[0-9]+$"), col("cod_parentesco").cast("int")).otherwise(None)
)

tra_apn = tra_apn.withColumn(
    "edad_beneficiario",
    floor(months_between(to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd"), col("fec_nacimiento")) / 12)
)

ben_apn = ben_apn.withColumn(
    "edad_beneficiario",
    floor(months_between(to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd"), col("fec_nacimiento")) / 12)
)

df_filter_sal = (
    mos.withColumnRenamed("ANO", "ANIO_SAL")
       .withColumnRenamed("VLR_SALARIO_MINIMO", "VLR_SMIN")
       .filter(col("MES") == 1)
)

campos_unica_vez = campos_unica_vez.withColumn(
"nid_siniestro",
col("RENTA").cast(DecimalType(10, 0))
)
df_filter_cob = (
    cob.withColumn("anio_pago", year(col("fec_pago")))
        .withColumn("anio_mes_pago", date_format(col("fec_pago"), "yyyy-MM"))
    .filter(
   ## (col("cod_cobertura") == 17) &
    (date_format(col("fec_pago"), "yyyyMM") == lit(FEC_CORTE_PERIOD)) & (col("nid_siniestro") == 2020001679)
).select("nid_siniestro", "vlr_pago", "cod_cobertura", "FEC_PAGO", "nid_pago", "anio_pago", "anio_mes_pago"))


siniestros_sin_17 = (
    df_filter_cob.groupBy("nid_siniestro", "anio_mes_pago")
    .agg(
        F.max(F.when(F.col("cod_cobertura") == 17, 1).otherwise(0)).alias("tiene_17"),
        F.max("fec_pago").alias("fec_referencia") # Tomamos una fecha del grupo para saber el mes
    )
    .filter(F.col("tiene_17") == 0)
)

df_filter_cob.printSchema()
df_registros_1717.printSchema()
# 3. Crear los registros 1717 con la fecha del último día del mes
df_registros_1717 = (
    siniestros_sin_17.select(
        col("nid_siniestro"),
        lit(None).alias("vlr_pago").cast("decimal(14,2)"),           # Valor por defecto 0
        lit(1717).alias("cod_cobertura").cast("decimal(10,0)"),
        last_day(F.col("fec_referencia")).cast("timestamp").alias("fec_pago"),
        lit(0000).alias("nid_pago").cast("decimal(10,0)"),        # No existe un ID de pago real para este registro generado
        year(last_day(col("fec_referencia"))).alias("anio_pago"),
        date_format(col("fec_referencia"), "yyyy-MM").alias("anio_mes_pago")
    )
)

df_final_cob_completo = df_filter_cob.unionByName(df_registros_1717)
df_final_cob_completo.orderBy(F.col("fec_pago").desc()).show()
#--------------------------------------------------------------subconsultas---------------------------------------------------------------------------------------------------------------#
print(f"Generate subqueries")
# 1
df_filter_sic = sic.filter(col("ramo") == "ARP")

# 3
df_filter_inv = (
    inv.withColumn("anio_aprobacion", year(col("fec_aprobacion_pension")))
       .filter(
           (col("fec_aprobacion_pension") <= to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd"))
       )
)

# 4
df_filter_aud_1 = (
    aud.select(
        when(col("cod_concepto_medico_am") == "4", "2")
          .when(col("cod_concepto_medico_am") == "5", "1")
          .otherwise(None)
          .alias("cod_origen"),
        col("nid_siniestro"),
        col("cod_concepto_medico_am"),
        col("nid_auditoria_medica"),
        col("fec_registra"),
        col("fec_registra_partition")
    )
)

# 5
df_groupby_his = (
    mue.groupBy("nid_siniestro")
       .agg(min("nid_hist_datos_inv_mte").alias("min_nid_hist_datos_inv_mte"))
       .withColumnRenamed("nid_siniestro", "nid_siniestro_group")
       .join(
           mue,
           col("min_nid_hist_datos_inv_mte") == mue["nid_hist_datos_inv_mte"],
           "inner"
       )
)

# 6
df_groupby_hijos = (
    ben.filter((col("cod_parentesco") == "4") & (col("bln_con_derecho") == "S"))
       .groupBy("nid_siniestro")
       .agg(count("*").alias("NO_HIJOS"))
       .withColumn(
           "NO_HIJOS",
           when(col("NO_HIJOS") == 0, None).otherwise(col("NO_HIJOS"))
       )
)

# 7
df_groupby_hijos_invalidos = (
    ben.filter(
        (col("cod_parentesco") == "4") &
        (col("bln_con_derecho") == "S") &
        (col("cod_estado_persona") == "2")
    )
    .groupBy("nid_siniestro")
    .agg(count("*").alias("NO_HIJOS_INVALIDOS"))
    .withColumn(
        "NO_HIJOS_INVALIDOS",
        when(col("NO_HIJOS_INVALIDOS") == 0, None).otherwise(col("NO_HIJOS_INVALIDOS"))
    )
)

# 8
df_filter_aud_2 = aud.filter(col("cod_concepto_medico_am") == "4")

pen = pen.withColumnRenamed("fec_registra", "fec_registra_pcl")

window_asc = Window.partitionBy("nid_siniestro").orderBy(col("fec_registra_pcl").asc())
window_desc = Window.partitionBy("nid_siniestro").orderBy(col("fec_registra_pcl").desc())

df_first_pcl = (
    pen.join(
        df_filter_aud_2,
        on="nid_siniestro",
        how="inner"
    )
    .select(
        col("nid_siniestro"),
        first("pcj_pcl").over(window_asc).alias("primero"),
        first("pcj_pcl").over(window_desc).alias("ultimo"),
        first("fec_registra_pcl").over(window_desc).alias("ultima_modificacion")
    )
    .dropDuplicates(["nid_siniestro", "primero", "ultimo", "ultima_modificacion"])
)

#---------------------------------------------------------------------alias tablas -------------------------------------------------------------------------------------------------------#
print(f"Alias for all subqueries")
fsal = df_filter_sal.alias("fsal")
fmat = df_filter_sic.alias("fmat")
fcov = df_final_cob_completo.alias("fcov")
finv = df_filter_inv.alias("finv")
fuad = df_filter_aud_1.alias("fuad")
his = df_groupby_his.alias("his")
hjs = df_groupby_hijos.alias("hjs")
hjv = df_groupby_hijos_invalidos.alias("hjv")
pcl = df_first_pcl.alias("pcl")
fcov.show()
#----------------------------------------------------------------------join principal-----------------------------------------------------------------------------------------------------#
print(f"Main Join Siniestros")
df_join_principal_reserva_arp =(
    sar
#par_trabajadores
.join(tra, sar["nid_trabajador"] == tra["nid_trabajador"], "inner")
#sar_auditoria_medica filtrado
.join(fuad, sar["nid_siniestro"] == fuad["nid_siniestro"], "inner")
# sar_datos_invalidez_muerte filtrado
.join(finv, (fuad["nid_siniestro"] == finv["nid_siniestro"]) & (fuad["nid_auditoria_medica"] == finv["nid_auditoria_medica"]), "left")
#ind_datos_reservas_siniestros
.join(res, sar["nid_siniestro"] == res["nid_siniestro"], "inner")
#rva_reserva_matematica_transic
    .join(fmat, sar["nid_siniestro"] == fmat["nid_siniestro_renta"], "left")
#sar_pagos_coberturas
    .join(fcov, sar["nid_siniestro"] == fcov["nid_siniestro"], "left")
#pgr_salarios_minimos
    #.join(fsal, finv["anio_aprobacion"] == fsal["ANIO_SAL"], "left")
    .join(fsal, fcov["anio_pago"] == fsal["ANIO_SAL"], "left")    
#hist_datos_invalidez_muerte
    .join(his, sar["nid_siniestro"] == his["nid_siniestro"], "left")
#hijos 
    .join(hjs, sar["nid_siniestro"] == hjs["nid_siniestro"], "left")
#hijos inválidos 
    .join(hjv, sar["nid_siniestro"] == hjv["nid_siniestro"], "left")
#hist_sar_pcl_pension
    .join(pcl, sar["nid_siniestro"] == pcl["nid_siniestro"], "left")
#acu_personas_naturales  trabajadores
    .join(tra_apn, (tra["COD_TIPO_IDENTIFICACION"] == tra_apn["COD_TIPO_IDENTIFICACION"]) &
                (tra["NID_NUMERO_IDENTIFICACION"] == tra_apn["NID_NUMERO_IDENTIFICACION"]), "left")
#cgr_equivalencia_tipos  trabajadores
    .join(tra_eqid, tra["cod_tipo_identificacion"] == tra_eqid["cod_tipo_documento"], "left")
#cgr_origenes
    .join(org, fuad["cod_origen"] == org["cod_origen"], "left")
#anp_ipc
    .join(ipc, finv["anio_aprobacion"] == ipc["anio"], "left")
#campos_unica_vez
    .join(campos_unica_vez, on="nid_siniestro", how="left")
)
siniestros_final_df = df_join_principal_reserva_arp.withColumn(
    "VALOR_AUXILIO_FUNERARIO",
    coalesce(
        col("vlr_pago"),
        when(col("org.COD_ORIGEN_SUPER").isin(1, 3),
            when(col("finv.vlr_pension") <= col("VLR_SMIN") * 5,
                round(col("VLR_SMIN") * 5, 0))
            .when(col("finv.vlr_pension") <= col("VLR_SMIN") * 10,
                round(col("finv.vlr_pension"), 0))
            .otherwise(round(col("VLR_SMIN") * 10, 0))
        ).otherwise(lit(0))
    )  # Aquí aplicas el cast directamente
)
siniestros_final_df.filter(sar["nid_siniestro"] == 2020001679).select("nid_afiliacion", "fec_pago", "VALOR_AUXILIO_FUNERARIO", fcov["cod_cobertura"]).show()
df_join_principal_reserva_arp.filter(sar["nid_siniestro"] == 2020001679).select("nid_afiliacion", fcov["fec_pago"], fsal["vlr_smin"]).show()
df_join_principal_reserva_arp.filter(sar["nid_siniestro"] == 2009001921).select(calc2_latest["pcj_gastos_exp"], calc2_latest["pcj_deslizamiento_sm"], sar["nid_siniestro"], calc2_latest["nid_calculo"]).distinct().show()
finv.select("FEC_APROBACION_PENSION").show(n=1)
"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim, date_format, to_date, lit, coalesce, min, max, broadcast, year, when, concat_ws, substring, countDistinct, months_between, round, floor, greatest, concat, explode, sequence, log, exp, sum as sum_, row_number
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, DoubleType, DecimalType
from pyspark.sql.window import Window

import sys, traceback, boto3, json


BUCKET_NAME = 'alfa-datalake-dev-gold'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
DB_NAME = 'ALFAPRD'
DB_SCHEMA = 'PRV'
PERIOD = '199001'
CERTIFICATE = 'False'
DB_CATALOG_GLUE_SILVER = 'datalake-dev-silver-catalog-db'
DB_CATALOG_GLUE_BRONCE = 'datalake-dev-bronce-catalog-db'
TABLE_NAME_GOLD = 'RESERVA_RVI_GOLD'
#FEC_CORTE = '2025-04-16'
FEC_CORTE_DATE = '1990-01-01'


extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME, DB_NAME)

#----------------------------------------------------------------StructType final-----------------------------------------------------------------------------------------------------------#
print(f"StructType final: {TABLE_NAME_GOLD}")


# Define the schema for the  Final DataFrame
schema_reserva_rvi_gold = StructType([                                         
        StructField("RAMO", IntegerType(), True),                                      # campo 1 - NUMBER(2,0)
        StructField("UNIDAD", IntegerType(), True),                                    # campo 2 - NUMBER(1,0)
        StructField("EQUIVALENCIA", StringType(), True),                               # campo 3 - NUMBER(1,0)
        StructField("NO_POLIZA", IntegerType(), True),                                 # campo 4 - NUMBER(10,0)
        StructField("VIG_INICIAL", DateType(), True),                                  # campo 5 - DATE YYYYY-MM-DD
        StructField("NO_SINIESTRO", IntegerType(), True),                              # campo 6 - NUMBER(10,0)
        StructField("FECHA_CAUSACION_DERECHO", DateType(), True),                      # campo 7 - DATE YYYYY-MM-DD
        StructField("ORIGEN_PENSION", IntegerType(), True),                            # campo 8 - NUMBER(1,0)
        StructField("PARENT", IntegerType(), True),                                    # campo 9 - NUMBER(1,0)
        StructField("TIPO_IDENTIF", IntegerType(), True),                              # campo 10  - NUMBER(2,0)
        StructField("NO_IDENTIF", StringType(), True),                                 # campo 11 - VARCHAR2(15 BYTE)
        StructField("SEXO", IntegerType(), True),                                      # campo 12 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO", DateType(), True),                             # campo 13 - DATE YYYYY-MM-DD
        StructField("NOMBRE", StringType(), True),                                     # campo 14 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS", IntegerType(), True),                                  # campo 15 - NUMBER(1,0)
        StructField("ESTADO", IntegerType(), True),                                    # campo 16 - NUMBER(1,0)
        StructField("NO_HIJOS", IntegerType(), True),                                  # campo 17 - NUMBER(15,0) ***
        StructField("NO_HIJOS_INVALIDOS", IntegerType(), True),                        # campo 18 - NUMBER(15,0) ***
        StructField("INGRESO_BASE_LIQUIDACION", DecimalType(10,2), True),              # campo 19 - VARCHAR2(3 BYTE)
        StructField("MESADA", DecimalType(16,2), True),                                # campo 20 - DECIMAL(16,2)
        StructField("NO_MESADAS", IntegerType(), True),                                # campo 21 - IntegerType()
        StructField("RETROACTIVO", DecimalType(10,2), True),                           # campo 22 - NUMBER(10,2)   +++
        StructField("GRADO_INVALIDEZ_INICIAL", DecimalType(5,2), True),                # campo 23 - decimal(5,2)
        StructField("FECHA_ULTIMA_MOD", DateType(), True),                             # campo 24 - DATE YYYYY-MM-DD
        StructField("GRADO_INVALIDEZ_ACTUAL", DecimalType(5,2), True),                 # campo 25 - decimal(5,2)
        StructField("GASTOS", DecimalType(5,4), True),                                 # campo 26 - DECIMAL(5,4)          

        StructField("PARENT_1", IntegerType(), True),                                  # campo 27 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_1", IntegerType(), True),                            # campo 28 - NUMBER(1,0)
        StructField("NO_IDENTIF_1", StringType(), True),                               # campo 29 - NUMBER(15,0)
        StructField("SEXO_1", IntegerType(), True),                                    # campo 30 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_1", DateType(), True),                           # campo 31 - DATE YYYYY-MM-DD
        StructField("NOMBRE_1", StringType(), True),                                   # campo 32 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_1", IntegerType(), True),                                # campo 33 - NUMBER(1,0)
        StructField("ESTADO_1", IntegerType(), True),                                  # campo 34 - NUMBER(1,0)
        StructField("CON_DERECHO_1", StringType(), True),                              # campo 35 - VARCHAR(1)           

        StructField("PARENT_2", IntegerType(), True),                                  # campo 36 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_2", IntegerType(), True),                            # campo 37 - NUMBER(1,0)
        StructField("NO_IDENTIF_2", StringType(), True),                               # campo 38 - NUMBER(15,0)
        StructField("SEXO_2", IntegerType(), True),                                    # campo 39 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_2", DateType(), True),                           # campo 40 - DATE YYYYY-MM-DD
        StructField("NOMBRE_2", StringType(), True),                                   # campo 41 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_2", IntegerType(), True),                                # campo 42 - NUMBER(1,0)
        StructField("ESTADO_2", IntegerType(), True),                                  # campo 43 - NUMBER(1,0)
        StructField("CON_DERECHO_2", StringType(), True),                              # campo 44 - VARCHAR(1)           

        StructField("PARENT_3", IntegerType(), True),                                  # campo 45 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_3", IntegerType(), True),                            # campo 46 - NUMBER(1,0)
        StructField("NO_IDENTIF_3", StringType(), True),                               # campo 47 - NUMBER(15,0)
        StructField("SEXO_3", IntegerType(), True),                                    # campo 48 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_3", DateType(), True),                           # campo 49 - DATE YYYYY-MM-DD
        StructField("NOMBRE_3", StringType(), True),                                   # campo 50 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_3", IntegerType(), True),                                # campo 51 - NUMBER(1,0)
        StructField("ESTADO_3", IntegerType(), True),                                  # campo 52 - NUMBER(1,0)
        StructField("CON_DERECHO_3", StringType(), True),                              # campo 53 - VARCHAR(1)

        StructField("PARENT_4", IntegerType(), True),                                  # campo 54 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_4", IntegerType(), True),                            # campo 55 - NUMBER(1,0)
        StructField("NO_IDENTIF_4", StringType(), True),                               # campo 56 - NUMBER(15,0)
        StructField("SEXO_4", IntegerType(), True),                                    # campo 57 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_4", DateType(), True),                           # campo 58 - DATE YYYYY-MM-DD
        StructField("NOMBRE_4", StringType(), True),                                   # campo 59 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_4", IntegerType(), True),                                # campo 60 - NUMBER(1,0)
        StructField("ESTADO_4", IntegerType(), True),                                  # campo 61 - NUMBER(1,0)
        StructField("CON_DERECHO_4", StringType(), True),                              # campo 62 - VARCHAR(1)

        StructField("PARENT_5", IntegerType(), True),                                  # campo 63 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_5", IntegerType(), True),                            # campo 64 - NUMBER(1,0)
        StructField("NO_IDENTIF_5", StringType(), True),                               # campo 65 - NUMBER(15,0)
        StructField("SEXO_5", IntegerType(), True),                                    # campo 66 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_5", DateType(), True),                           # campo 67 - DATE YYYYY-MM-DD
        StructField("NOMBRE_5", StringType(), True),                                   # campo 68 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_5", IntegerType(), True),                                # campo 69 - NUMBER(1,0)
        StructField("ESTADO_5", IntegerType(), True),                                  # campo 70 - NUMBER(1,0)
        StructField("CON_DERECHO_5", StringType(), True),                              # campo 71 - VARCHAR(1)

        StructField("PARENT_6", IntegerType(), True),                                  # campo 72 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_6", IntegerType(), True),                            # campo 73 - NUMBER(1,0)
        StructField("NO_IDENTIF_6", StringType(), True),                               # campo 74 - NUMBER(15,0)
        StructField("SEXO_6", IntegerType(), True),                                    # campo 75 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_6", DateType(), True),                           # campo 76 - DATE YYYYY-MM-DD
        StructField("NOMBRE_6", StringType(), True),                                   # campo 77 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_6", IntegerType(), True),                                # campo 78 - NUMBER(1,0)
        StructField("ESTADO_6", IntegerType(), True),                                  # campo 79 - NUMBER(1,0)
        StructField("CON_DERECHO_6", StringType(), True),                              # campo 80 - VARCHAR(1)

        StructField("PARENT_7", IntegerType(), True),                                  # campo 81 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_7", IntegerType(), True),                            # campo 82 - NUMBER(1,0)
        StructField("NO_IDENTIF_7", StringType(), True),                               # campo 83 - NUMBER(15,0)
        StructField("SEXO_7", IntegerType(), True),                                    # campo 84 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_7", DateType(), True),                           # campo 85 - DATE YYYYY-MM-DD
        StructField("NOMBRE_7", StringType(), True),                                   # campo 86 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_7", IntegerType(), True),                                # campo 87 - NUMBER(1,0)
        StructField("ESTADO_7", IntegerType(), True),                                  # campo 88 - NUMBER(1,0)
        StructField("CON_DERECHO_7", StringType(), True),                              # campo 89 - VARCHAR(1)

        StructField("PARENT_8", IntegerType(), True),                                  # campo 90 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_8", IntegerType(), True),                            # campo 91 - NUMBER(1,0)
        StructField("NO_IDENTIF_8", StringType(), True),                               # campo 92 - NUMBER(15,0)
        StructField("SEXO_8", IntegerType(), True),                                    # campo 93 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_8", DateType(), True),                           # campo 94 - DATE YYYYY-MM-DD
        StructField("NOMBRE_8", StringType(), True),                                   # campo 95 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_8", IntegerType(), True),                                # campo 96 - NUMBER(1,0)
        StructField("ESTADO_8", IntegerType(), True),                                  # campo 97 - NUMBER(1,0)
        StructField("CON_DERECHO_8", StringType(), True),                              # campo 98 - VARCHAR(1)

        StructField("PARENT_9", IntegerType(), True),                                  # campo 99 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_9", IntegerType(), True),                            # campo 100 - NUMBER(1,0)
        StructField("NO_IDENTIF_9", StringType(), True),                               # campo 101 - NUMBER(15,0)
        StructField("SEXO_9", IntegerType(), True),                                    # campo 102 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_9", DateType(), True),                           # campo 103 - DATE YYYYY-MM-DD
        StructField("NOMBRE_9", StringType(), True),                                   # campo 104 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_9", IntegerType(), True),                                # campo 105 - NUMBER(1,0)
        StructField("ESTADO_9", IntegerType(), True),                                  # campo 106 - NUMBER(1,0)
        StructField("CON_DERECHO_9", StringType(), True),                              # campo 107 - VARCHAR(1)

        StructField("PARENT_10", IntegerType(), True),                                 # campo 108 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_10", IntegerType(), True),                           # campo 109 - NUMBER(1,0)
        StructField("NO_IDENTIF_10", StringType(), True),                              # campo 110 - NUMBER(15,0)
        StructField("SEXO_10", IntegerType(), True),                                   # campo 111 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_10", DateType(), True),                          # campo 112 - DATE YYYYY-MM-DD
        StructField("NOMBRE_10", StringType(), True),                                  # campo 113 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_10", IntegerType(), True),                               # campo 114 - NUMBER(1,0)
        StructField("ESTADO_10", IntegerType(), True),                                 # campo 115 - NUMBER(1,0)
        StructField("CON_DERECHO_10", StringType(), True),                             # campo 116 - VARCHAR(1)

        StructField("PARENT_11", IntegerType(), True),                                 # campo 117 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_11", IntegerType(), True),                           # campo 118 - NUMBER(1,0)
        StructField("NO_IDENTIF_11", StringType(), True),                              # campo 119 - NUMBER(15,0)
        StructField("SEXO_11", IntegerType(), True),                                   # campo 120 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_11", DateType(), True),                          # campo 121 - DATE YYYYY-MM-DD
        StructField("NOMBRE_11", StringType(), True),                                  # campo 122 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_11", IntegerType(), True),                               # campo 123 - NUMBER(1,0)
        StructField("ESTADO_11", IntegerType(), True),                                 # campo 124 - NUMBER(1,0)
        StructField("CON_DERECHO_11", StringType(), True),                             # campo 125 - VARCHAR(1)

        StructField("PARENT_12", IntegerType(), True),                                 # campo 126 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_12", IntegerType(), True),                           # campo 127 - NUMBER(1,0)
        StructField("NO_IDENTIF_12", StringType(), True),                              # campo 128 - NUMBER(15,0)
        StructField("SEXO_12", IntegerType(), True),                                   # campo 129 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_12", DateType(), True),                          # campo 130 - DATE YYYYY-MM-DD
        StructField("NOMBRE_12", StringType(), True),                                  # campo 131 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_12", IntegerType(), True),                               # campo 132 - NUMBER(1,0)
        StructField("ESTADO_12", IntegerType(), True),                                 # campo 133 - NUMBER(1,0)
        StructField("CON_DERECHO_12", StringType(), True),                             # campo 134 - VARCHAR(1)

        StructField("PARENT_13", IntegerType(), True),                                 # campo 135 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_13", IntegerType(), True),                           # campo 136 - NUMBER(1,0)
        StructField("NO_IDENTIF_13", StringType(), True),                              # campo 137 - NUMBER(15,0)
        StructField("SEXO_13", IntegerType(), True),                                   # campo 138 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_13", DateType(), True),                          # campo 139 - DATE YYYYY-MM-DD
        StructField("NOMBRE_13", StringType(), True),                                  # campo 140 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_13", IntegerType(), True),                               # campo 141 - NUMBER(1,0)
        StructField("ESTADO_13", IntegerType(), True),                                 # campo 142 - NUMBER(1,0)
        StructField("CON_DERECHO_13", StringType(), True),                             # campo 143 - VARCHAR(1)

        StructField("PARENT_14", IntegerType(), True),                                 # campo 144 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_14", IntegerType(), True),                           # campo 145 - NUMBER(1,0)
        StructField("NO_IDENTIF_14", StringType(), True),                              # campo 146 - NUMBER(15,0)
        StructField("SEXO_14", IntegerType(), True),                                   # campo 147 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_14", DateType(), True),                          # campo 148 - DATE YYYYY-MM-DD
        StructField("NOMBRE_14", StringType(), True),                                  # campo 149 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_14", IntegerType(), True),                               # campo 150 - NUMBER(1,0)
        StructField("ESTADO_14", IntegerType(), True),                                 # campo 151 - NUMBER(1,0)
        StructField("CON_DERECHO_14", StringType(), True),                             # campo 152 - VARCHAR(1)	

        StructField("PARENT_15", IntegerType(), True),                                 # campo 153 - NUMBER(1,0)
        StructField("TIPO_IDENTIF_15", IntegerType(), True),                           # campo 154 - NUMBER(1,0)
        StructField("NO_IDENTIF_15", StringType(), True),                              # campo 155 - NUMBER(15,0)
        StructField("SEXO_15", IntegerType(), True),                                   # campo 156 - NUMBER(1,0)
        StructField("FECHA_NACIMIENTO_15", DateType(), True),                          # campo 157 - DATE YYYYY-MM-DD
        StructField("NOMBRE_15", StringType(), True),                                  # campo 158 - VARCHAR2(80 BYTE)
        StructField("ESTUDIOS_15", IntegerType(), True),                               # campo 159 - NUMBER(1,0)
        StructField("ESTADO_15", IntegerType(), True),                                 # campo 160 - NUMBER(1,0)
        StructField("CON_DERECHO_15", StringType(), True),                             # campo 161 - VARCHAR(1)

        StructField("NO_RAD_NOT_TEC", DecimalType(20,0), True),                        # campo 162 - NUMBER(20,0)
        StructField("INTERES_TEC_PRIMA", DecimalType(15,10), True),                    # campo 163 - DECIMAL(15,10)
        StructField("TASA_CREC_PRIMA", DecimalType(5,2), True),                        # campo 164 - decimal(7,2)
        StructField("GASTOS_PRIMA", DecimalType(5,4), True),                           # campo 165 - DECIMAL(7,4)
        StructField("PRIMA", DecimalType(17,2), True),                                 # campo 166 - DECIMAL(30,2)
        StructField("FECHA_INICIAL_RENTA_DIFERIDA", DateType(), True),                 # campo 167 - DATE YYYYY-MM-DD
        StructField("VALOR_MESADA_BENEF_PERIO_DIFER", DecimalType(10,2), True),        # campo 168 - DECIMAL(10,2)
        StructField("VALOR_AUXILIO_FUNERARIO", IntegerType(), True),                   # campo 169 - NUMBER(20,0)
        StructField("OBSERVACIONES", StringType(), True),                              # campo 170 - VARCHAR2(80 BYTE)
        StructField("VALOR_RESERVA_RV89", DecimalType(14,2), True),                    # campo 171 - NUMBER(14,2)
        StructField("VALOR_RESERVA_RV08", DecimalType(14,2), True),                    # campo 172 - NUMBER(14,2)
        StructField("PAR_0", DecimalType(28,10), True),                                # campo 173 - N/A
        StructField("PARMA", DecimalType(28,10), True),                                # campo 174 - N/A
        StructField("PORCENTAJE_EXC_PARMA", DecimalType(10,2), True),                  # campo 175 - N/A
        StructField("POR_AMORT_ACUM", DecimalType(10,2), True),                        # campo 176 - NUMBER(5,2)
        StructField("FEC_AVISO_SINIESTRO", DateType(), True),                          # campo 177 - N/A
        StructField("FAIR_VALUE", DecimalType(14,2), True),                            # campo 178 - N/A
        StructField("INSCRIPCION_MECANISMO", IntegerType(), True),                     # campo 179 - VARCHAR2(1 BYTE)
        StructField("FECHA_INSCRIPCION", DateType(), True),                            # campo 180 - DATE YYYYY-MM-DD
        StructField("PARAMETRO_DESLIZAMIENTO", DecimalType(5,4), True),                # campo 181 - NUMBER(7,15)
        StructField("TASA_FAC_SEG", DecimalType(10,2), True),                          # campo 182 - PARAMETERS
        StructField("VALOR_COBERTURA", DecimalType(14,2), True),                       # campo 183 - NUMBER(14,2)
        StructField("FECHA_EXPED_POLIZA", DateType(), True),                           # campo 184 - DATE YYYYY-MM-DD
        StructField("FECHA_MUERTE_AFILIADO", DateType(), True),                        # campo 185 - DATE YYYYY-MM-DD
        StructField("INTERES_TECNICO_NIIF4", DecimalType(15,10), True),                # campo 186 - NUMBER(15)

        StructField("CODIGO_CONCEPTO_RENTA", StringType(), True),                      # campo 187 - VARCHAR(1)
        StructField("PERIODO_PAGO_RENTA", StringType(), True),                         # campo 188 - VARCHAR(4)
        StructField("PERIODO_EXPEDICION_RENTA", StringType(), True),                   # campo 189 - VARCHAR(4)
        StructField("FECHA_EXPEDICION_RENTA", DateType(), True),                       # campo 190 - DATE
        StructField("PERIODO_INVALIDACION_RENTA", StringType(), True),                 # campo 191 - VARCHAR(4)
        StructField("FECHA_INVALIDACION_RENTA", DateType(), True),                     # campo 192 - DATE
        StructField("PERIODO_POINT_RENTA", StringType(), True),                        # campo 193 - VARCHAR(4)
        StructField("FECHA_POINT_RENTA", DateType(), True),                            # campo 194 - DATE
        StructField("FECHA_INICIO_PERIODO_PAGO_RENTA", DateType(), True),              # campo 195 - DATE
        StructField("FECHA_FIN_PERIODO_PAGO_RENTA", DateType(), True),                 # campo 196 - DATE
        StructField("FECHA_INICIAL_PAGO_RENTA", DateType(), True),                     # campo 197 - DATE
        StructField("PERIODO_PRIMER_PAGO_RENTA", StringType(), True),                  # campo 198 - VARCHAR(4) 
        StructField("FECHA_PRIMER_PAGO_RENTA", DateType(), True),                      # campo 199 - DATE 
        StructField("PERIODO_ULTIMO_PAGO_RENTA", StringType(), True),                  # campo 200 - VARCHAR(4)
        StructField("FECHA_INICIO_PERIODO_ULTIMO_PAGO_RENTA", DateType(), True),       # campo 201 - DATE
        StructField("FECHA_FIN_PERIODO_ULTIMO_PAGO_RENTA", DateType(), True),          # campo 202 - DATE
        StructField("VALIDACION_EXPEDICION_RENTA", StringType(), True),                # campo 203 - VARCHAR(1)
        StructField("VALIDACION_PAGO_RENTA", StringType(), True),                      # campo 204 - VARCHAR(1)
        StructField("PERIODO_INVALIDACION_RENTA_HOMOLOGADO", StringType(), True),      # campo 205 - VARCHAR(4)
        StructField("FECHA_INVALIDACION_RENTA_HOMOLOGADO", DateType(), True),          # campo 206 - DATE

        StructField("ID_LOTE_CARGUE", IntegerType(), True)                             # campo 207 - partition
])

#-------------------------------------------------------------------------extract prv and genera---------------------------------------------------------------------------------------#
print(f"Extract source: {DB_SCHEMA}")
#Read tables from silver prv
# SELECT rentas
df_prv_rentas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_PRV_SILVER_PRV_RENTAS").toDF()
df_prv_rentas = df_prv_rentas.select("nid_rentas", "nid_siniestro", "fec_expedicion", "fec_causacion", "nid_numero_identificacion", "num_mesadas", "pcj_pcl_inicial", "fec_ultima_mod", "pcj_pcl_actual", "cod_origen", "pcj_interes_tecnico", "pcj_interes_reserva", "fec_invalidacion_registro", "fec_envio_point", "cod_tipo_identificacion", "vlr_pension_inicial", "fec_causacion", "fec_ultima_mod", "cod_estado_persona").distinct()

# SELECT rentas_notas
df_prv_rentas_notas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_PRV_SILVER_PRV_RENTAS_NOTAS").toDF()
df_prv_rentas_notas = df_prv_rentas_notas.select("nid_rentas", "num_radicado_nota", "cod_ramo_sfc").distinct()

# SELECT beneficiarios
df_prv_beneficiarios = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_PRV_SILVER_PRV_BENEFICIARIOS").toDF()
df_prv_beneficiarios = df_prv_beneficiarios.select("nid_rentas", "cod_tipo_identificacion", "nid_numero_identificacion", "cod_estado_persona", "bln_con_derecho", "cant_hijos", "cod_parentesco", "fec_aparicion", "fec_muerte", "bln_es_beneficiario").distinct()

# SELECT rentas_aprobadas_obp
df_prv_rentas_aprobadas_obp = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_PRV_SILVER_PRV_RENTAS_APROBADAS_OBP").toDF()
df_prv_rentas_aprobadas_obp = df_prv_rentas_aprobadas_obp.select("nid_rentas", "inscripcion_mecanismo", "parametro_deslizamiento", "fecha_inscripcion", "cobertura").distinct()

# SELECT endosos_point
df_prv_endosos_point = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_PRV_SILVER_PRV_ENDOSOS_POINT").toDF()
df_prv_endosos_point = df_prv_endosos_point.select("nid_rentas", "vlr_prima")

# SELECT siniestros
df_spr_siniestros = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_PRV_SILVER_SPR_SINIESTROS").toDF()
df_spr_siniestros = df_spr_siniestros.select("nid_siniestro", "cod_estado_siniestro").distinct()

# SELECT crv_Calculos_2
df_crv_calculos_2 = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_PRV_SILVER_CRV_CALCULOS_2").toDF()
df_crv_calculos_2 = df_crv_calculos_2.select("nid_calculo", "nid_rentas", "fec_corte_calculo", "pcj_factor_seguridad", "pcj_tasa_administracion", "pcj_tasa_administracion").distinct()

#Read tables from silver general
#acu_personas_naturales
df_acu_personas_naturales = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ACU_PERSONAS_NATURALES").toDF()
df_acu_personas_naturales = df_acu_personas_naturales.select("cod_tipo_identificacion", "nid_numero_identificacion", "fec_nacimiento", "fec_muerte", "genero", "primer_apellido", "segundo_apellido", "primer_nombre", "segundo_nombre").distinct()

#rva_reserva_matematica_transic
df_rva_reserva_matematica_transic = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_RVA_RESERVA_MATEMATICA_TRANSIC").toDF()
df_rva_reserva_matematica_transic = df_rva_reserva_matematica_transic.select("nid_siniestro_renta", "ramo", "vlr_reserva_rv89", "vlr_reserva_rv08" ).distinct()

#anp_pensionado_novedad
df_anp_pensionado_novedad = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PENSIONADO_NOVEDAD").toDF()
df_anp_pensionado_novedad= df_anp_pensionado_novedad.select("nid_distribucion_vigencia", "vlr_fijo").distinct()

#anp_distribucion_vig_pension
df_anp_distribucion_vig_pension = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ANP_DISTRIBUCION_VIG_PENSION").toDF()
df_anp_distribucion_vig_pension = df_anp_distribucion_vig_pension.select("nid_pension", "nid_distribucion_vigencia").distinct()

#anp_pensiones
df_anp_pensiones = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PENSIONES").toDF()
df_anp_pensiones = df_anp_pensiones.select("nid_procedencia", "nid_pension").distinct()

#anp_movimientos_pago
df_anp_movimientos_pago = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ANP_MOVIMIENTOS_PAGO").toDF()
df_anp_movimientos_pago = df_anp_movimientos_pago.select("periodo_pago", "nid_distribucion_vigencia", "fec_inicial").distinct()

#anp_ipc
df_anp_ipc = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ANP_IPC").toDF()

#pgr_salarios_minimos
df_pgr_salarios_minimos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_PGR_SALARIOS_MINIMOS").toDF()

#cgr_equivalencia_tipos_id
df_cgr_equivalencia_tipos_id = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_CGR_EQUIVALENCIA_TIPOS_ID").toDF()

#cgr_estados_personas
df_cgr_estados_personas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_CGR_ESTADOS_PERSONAS").toDF()

#cgr_parentescos
df_cgr_parentescos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_CGR_PARENTESCOS").toDF()

#-------------------------------------------------------------------------Extract source FILE              -----------------------------------------------------------------------------#    
print(f"Extract source: FILE")
#Read tables from bronce file
df_files_campos_unica_vez = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_BRONCE}", table_name="FILES_CAMPOS_UNICA_VEZ").toDF()


#---------------------------------------------------------------------Init Businees Logic------------------------------------------------------------------------------------------------#

print(f"Writing to {TABLE_NAME_GOLD} with period = {PERIOD}, Init Businees Logic")

df_prv_rentas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_PRV_SILVER_PRV_RENTAS").toDF()
df_prv_rentas = df_prv_rentas.select("nid_rentas", "nid_siniestro", "fec_expedicion", "fec_causacion", "nid_numero_identificacion", "num_mesadas", "pcj_pcl_inicial", "fec_ultima_mod", "pcj_pcl_actual", "cod_origen", "pcj_interes_tecnico", "pcj_interes_reserva", "fec_invalidacion_registro", "fec_envio_point", "cod_tipo_identificacion", "vlr_pension_inicial", "fec_causacion", "fec_ultima_mod", "cod_estado_persona").distinct()




#---------------------------------------------------------------------alias tablas ------------------------------------------------------------------------------------------------------#
# Alias para todos los DataFrames de PRV
print(f"Alias for all DataFrames of  {DB_SCHEMA}")
ren = df_prv_rentas.alias("ren")
ren_not = df_prv_rentas_notas.alias("ren_not")
ben = df_prv_beneficiarios.alias("ben")
ren_aprob = df_prv_rentas_aprobadas_obp.alias("ren_aprob")
end_point = df_prv_endosos_point.alias("end_point")
sin = df_spr_siniestros.alias("sin")
calc2 = df_crv_calculos_2.alias("calc2")

# Alias para tablas de General Silver
per_nat = df_acu_personas_naturales.alias("per_nat")
eq_tid = df_cgr_equivalencia_tipos_id.alias("eq_tid")
est_per = df_cgr_estados_personas.alias("est_per")
par = df_cgr_parentescos.alias("par")
ipc = df_anp_ipc.alias("ipc")
res_mat = df_rva_reserva_matematica_transic.alias("res_mat")
sal_min = df_pgr_salarios_minimos.alias("sal_min")
pen_nov = df_anp_pensionado_novedad.alias("pen_nov")
dist_vig_pen = df_anp_distribucion_vig_pension.alias("dist_vig_pen")
pens = df_anp_pensiones.alias("pens")
mov_pago = df_anp_movimientos_pago.alias("mov_pago")

# Alias para tablas de files bronce
campos_unica_vez = df_files_campos_unica_vez.alias("campos_unica_vez")

df_salarios_expedicion = sal_min.alias("exp")
df_salarios_corte = sal_min.alias("corte")
from datetime import datetime, timedelta, date

#-------------------------------------------------------------------------Variables-----------------------------------------------------------------------------------------------------# 
print(f"Calculation of Variables")
# Convertir a objeto datetime
dt = datetime.strptime(FEC_CORTE_DATE, "%Y-%m-%d")

# Periodo (yyyymm)
FEC_CORTE_PERIOD = dt.strftime("%Y%m")

# Año (yyyy)
FEC_CORTE_ANIO = int(dt.strftime("%Y")) 

#Salario
salario_minimo_actual = sal_min.filter((col("ano") == FEC_CORTE_ANIO) & (col("mes") == 1) ).select("vlr_salario_minimo").first()[0]


#----------------------------------------------------------withColumn source------------------------------------------------------------------------------------------------------------#
print(f"Add columns to sources")
# Año de expedición y año anterior al corte
ren = ren.withColumn("anio_expedicion", year("fec_expedicion"))
#ren = ren.withColumn("anio_anterior_corte", lit(FEC_CORTE_ANIO - 1))

per_nat = per_nat.withColumn(
    "edad_beneficiario",
    floor(months_between(to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd"), col("fec_nacimiento")) / 12)
)

campos_unica_vez = campos_unica_vez.withColumn(
"nid_rentas",
col("RENTA").cast(DecimalType(10, 0))
)

#--------------------------------------------------------------subconsultas-------------------------------------------------------------------------------------------------------------#
print(f"Generate subqueries")

# main rentas

# Filtro principal de fecha
#ren = ren.filter((date_format(ren["fec_expedicion"], "yyyyMM") >= F.lit(FEC_CORTE_PERIOD)) & (ren["nid_rentas"] == 97184) )
#ren = ren.filter((date_format(ren["fec_expedicion"], "yyyyMM") >= F.lit(FEC_CORTE_PERIOD)) & (ren["nid_rentas"] == 729) )
ren = ren.filter((date_format(ren["fec_expedicion"], "yyyyMM") >= F.lit(FEC_CORTE_PERIOD)) & (ren["nid_rentas"] == 216) )

# 1. Prima
sum_prima = (
    end_point
    .groupBy("nid_rentas")
    .agg(sum_("vlr_prima").alias("vlr_prima"))
)

# 2. Movimientos de pago
mov_pago_filter = (
    mov_pago
    .filter(
        (mov_pago["periodo_pago"] >= F.lit(FEC_CORTE_PERIOD)) 
    )
)
# 2.1. Cálculo del año de corte (2025)
mov_pago_filter = mov_pago_filter.withColumn(
    "anio_corte",
    year(to_date(mov_pago["periodo_pago"], "yyyyMM"))
)

# 2.2. Cálculo del año de corte anterior (2024)
mov_pago_filter = mov_pago_filter.withColumn(
    "anio_corte_anterior",
    (col("anio_corte") - 1)
)
# 3. Reserva matemática (solo ramo RVI) 
res_mat_filter = res_mat.filter(res_mat["ramo"] == "RVI")

# 4. Último cálculo (calc2) por renta y corte  # 5. Reserva matemática (solo ramo RVI) # 4. Movimientos de pago # 3. Hijos inválidos  # 2. Hijos  # 1. Prima
w = Window.partitionBy("nid_rentas", date_format( "fec_corte_calculo", "yyyyMM"))

calc2_latest = (
    calc2
    .withColumn("nid_calculo_max", F.max("nid_calculo").over(w))
    .filter(F.col("nid_calculo") == F.col("nid_calculo_max"))
    .drop("nid_calculo_max")
)


# 5. Auxilios funerario (aux_funerario) # 7. Calculos para mesada (ipc_acumulado)  # 6. Último cálculo (calc2) por renta y corte  # 5. Reserva matemática (solo ramo RVI) # 4. Movimientos de pago # 3. Hijos inválidos  # 2. Hijos  # 1. Prima
aux_funerario = ren.join(
    pens,
    ren["nid_rentas"] == pens["nid_procedencia"],
    "left"
).join(
    dist_vig_pen,
    pens["nid_pension"] == dist_vig_pen["nid_pension"],
    "left"
).join(
    pen_nov,
    dist_vig_pen["nid_distribucion_vigencia"] == pen_nov["nid_distribucion_vigencia"],
    "left"
).groupBy(ren["nid_rentas"]).agg(
    coalesce(max(pen_nov["vlr_fijo"]), lit(0)).alias("VLR_AUX_FUNERARIO_BASE")
)
#----------------------------------------------------------------------join principal----------------------------------------------------------------------------------------------------#
print(f"Join Main Rentas")
join_rentas_df = (
    # prv_rentas
    ren
    # anp_pensiones
    .join(pens, ren["nid_rentas"] == pens["nid_procedencia"], "inner")
    # anp_distribucion_vig_pension
    .join(dist_vig_pen, pens["nid_pension"] == dist_vig_pen["nid_pension"], "inner")
    # anp_movimientos_pago con condiciones (periodo vs fec_expedicion)
    .join(
        mov_pago_filter,
        (dist_vig_pen["nid_distribucion_vigencia"] == mov_pago_filter["nid_distribucion_vigencia"]),
        "inner"
    )
    # acu_personas_naturales
    .join(per_nat,
        (per_nat["cod_tipo_identificacion"] == ren["cod_tipo_identificacion"])
        & (per_nat["nid_numero_identificacion"] == ren["nid_numero_identificacion"]),
        "inner",
    )
    # cgr_estados_personas
    .join(est_per, est_per["cod_estado_persona"] == ren["cod_estado_persona"], "inner")
    # anp_ipc (por año de fec_expedicion)
    .join(ipc, ipc["anio"] == year(ren["fec_expedicion"]), "inner")
    # equivalencia_tipos_id
    .join(eq_tid, eq_tid["cod_tipo_documento"] == ren["cod_tipo_identificacion"], "left")
    # reserva_matematica_transic
    .join(res_mat_filter, res_mat_filter["nid_siniestro_renta"] == ren["nid_rentas"], "left")
    # prv_rentas_notas
    .join(ren_not, ren["nid_rentas"] == ren_not["nid_rentas"], "left")
    # spr_siniestros
    .join(sin, ren["nid_siniestro"] == sin["nid_siniestro"], "left")
    # prv_rentas_aprobadas_obp
    .join(ren_aprob, (ren_aprob["nid_rentas"] == ren["nid_rentas"]), "left")
    # crv_calculos_2 (max nid_calculo)
    .join(
        calc2_latest,
        (calc2_latest["nid_rentas"] == ren["nid_rentas"]) &
        (date_format(calc2_latest["fec_corte_calculo"], "yyyyMM") == date_format(ren["fec_expedicion"], "yyyyMM")),
        "left"
    )
    # endosos_point prima
    .join(sum_prima, sum_prima["nid_rentas"] == ren["nid_rentas"], "left")
    #join beneficiarios hijos/ observaciones beneficiarios
    .join(ben, on="nid_rentas", how="left" )   
    # Join con salario mínimo del año de expedición (mes = 1) mesada
    .join(
        broadcast(df_salarios_expedicion),
        (ren["anio_expedicion"] == df_salarios_expedicion["ano"]) & 
        (df_salarios_expedicion["mes"] == 1),
        "left"
    )
    # Join con salario mínimo del año de corte (mes = 1) mesada
    .join(
        broadcast(df_salarios_corte),
        (year(to_date(mov_pago["periodo_pago"], "yyyyMM")) == df_salarios_corte["ano"]) &
        (df_salarios_corte["mes"] == 1),
        "left"
    )
    # Join con valor base para calcular aux funerarios
    .join(aux_funerario, on="nid_rentas", how="left")
    #campos_unica_vez
    .join(campos_unica_vez, on="nid_rentas", how="left")
    # Filtro principal de fecha
    .filter((date_format(ren["fec_expedicion"], "yyyyMM") >= F.lit(FEC_CORTE_PERIOD)))

)
# Expandir rango de años entre expedición y corte para mesada
df_ano_rentas = join_rentas_df.select(ren["nid_rentas"], ren["anio_expedicion"],mov_pago_filter["anio_corte_anterior"],mov_pago_filter["periodo_pago"])
df_ano_rentas.show()

ipc_acumulado = df_ano_rentas.withColumn(
    "anio_ipc",
    explode(sequence(col("anio_expedicion").cast("int"), col("anio_corte_anterior").cast("int")))
)
ipc_acumulado.show()
# 1. Calculos para mesada (ipc_acumulado) 
# Expandir rango de años entre expedición y corte para mesada
df_ano_rentas = join_rentas_df.select(ren["nid_rentas"], ren["anio_expedicion"],mov_pago_filter["anio_corte_anterior"],mov_pago_filter["periodo_pago"])

ipc_acumulado = df_ano_rentas.withColumn(
    "anio_ipc",
    explode(sequence(col("anio_expedicion").cast("int"), col("anio_corte_anterior").cast("int")))
)

# Join con tabla de IPC   para mesada
ipc_acumulado = ipc_acumulado.join(
    ipc.select(col("ANIO").alias("anio_ipc"), (1 + (col("PCJ_IPC")/100)).alias("pcj_ipc_anp")),
    on="anio_ipc",
    how="left"
).distinct()

# Factor acumulado (logaritmos → suma → exp)   para mesada
incremento_factor = (
    ipc_acumulado
    .withColumn(
        "log_factor",
        # 1. Calcular el logaritmo natural del factor anual
        log(col("pcj_ipc_anp"))
    )
    .groupBy("nid_rentas", "anio_expedicion", "anio_corte_anterior", "periodo_pago")
    .agg(
        # 2. Sumar los logaritmos y aplicar la exponencial para obtener el producto
        exp(sum_("log_factor")).alias("factor_incremento_acumulado")
    )
)

# 2. Num Hijos
df_hijos = (
    join_rentas_df    
    .filter(
        (ben["cod_parentesco"] == "4")
        & (
            (date_format(ben["fec_aparicion"], "yyyyMM") < mov_pago_filter["periodo_pago"])
            | ben["fec_aparicion"].isNull()
        )
        & (
            ((ben["bln_con_derecho"] == "S") & (ben["bln_es_beneficiario"] == "S"))
            | (date_format(ben["fec_muerte"], "yyyyMM") > mov_pago_filter["periodo_pago"])
        )
    )
    .groupBy(ben["nid_rentas"])
    .agg(
        when(countDistinct(ben["nid_numero_identificacion"]) == 0, None)
        .otherwise(countDistinct(ben["nid_numero_identificacion"]))
        .alias("num_hijos")
    )
)

#3. Hijos Invalidos
df_hijos_invalidos = (
    join_rentas_df
    .filter(
        (ben["cod_parentesco"] == "4")
        & (ben["bln_con_derecho"] == "S")
        & (ben["cod_estado_persona"] == "2")
    )
    .groupBy(ben["nid_rentas"])
    .agg(
        when(countDistinct(ben["nid_numero_identificacion"]) == 0, None)
        .otherwise(countDistinct(ben["nid_numero_identificacion"]))
        .alias("num_hijos_invalidos")
    )
)

#4.Clasificacion de los beneficiarios para las observaciones (ben_filter) 
ben_filter = ben.filter(
    col("cod_parentesco").isin("3", "10")
).select(col("nid_rentas").alias("nid_rentas_ben"),col("cod_parentesco").alias("cod_parentesco_ben")).distinct()
ben_filter = ben_filter.withColumn("HAS_BENEFICIARIO_CLAVE", lit(True))
df_prueba = join_rentas_df.select(
        ren["nid_rentas"],
        "nid_siniestro_renta",
        "vlr_reserva_rv89",
        "vlr_reserva_rv08",
        "periodo_pago"
    )

#(months_between(to_date(mov_pago_filter["periodo_pago"], "yyyyMM"), F.to_date(F.lit("201804"), "yyyyMM")) + 91)
# 1. Agregamos la columna con el cálculo
df_validacion = df_prueba.withColumn(
    "meses_calculados",
    months_between(
        to_date(mov_pago_filter["periodo_pago"], "yyyyMM"), 
        to_date(F.lit("201804"), "yyyyMM")
    ) + 91
)

# 2. Hacemos el select distinct para ver cada periodo de pago y su resultado
df_resultados_unicos = df_validacion.select(
    "periodo_pago", 
    "meses_calculados"
).distinct().orderBy("periodo_pago")
df_resultados_unicos.show(100)
#campo 62  PARMA
final_df = join_rentas_df.withColumn(
    "PARMA",
    when(
        res_mat["nid_siniestro_renta"].isNotNull(),
        ((100 - ((res_mat["vlr_reserva_rv89"] / res_mat["vlr_reserva_rv08"]) * 100)) / 240)
    ).otherwise(None)
)

#campo 64 POR_AMORT_ACUM
final_df = final_df.withColumn(
    "POR_AMORT_ACUM",
    when(
        res_mat["nid_siniestro_renta"].isNotNull(),
    when(
        (((res_mat["vlr_reserva_rv89"] / res_mat["vlr_reserva_rv08"]) * 100) +  col("PARMA") * (months_between(to_date(mov_pago_filter["periodo_pago"], "yyyyMM"), F.to_date(F.lit("201804"), "yyyyMM")) + 91)) > 100, 100)
        .otherwise(((res_mat["vlr_reserva_rv89"] / res_mat["vlr_reserva_rv08"]) * 100) + col("PARMA") * (months_between(to_date(mov_pago_filter["periodo_pago"], "yyyyMM"), F.to_date(F.lit("201804"), "yyyyMM")) + 91)
    ) 
    ).otherwise(None)
)
from pyspark.sql.functions import desc

final_df.select("PARMA", "POR_AMORT_ACUM", mov_pago_filter["periodo_pago"]) \
    .orderBy(desc("periodo_pago")) \
    .show()
join_rentas_df_path  = f"s3://{BUCKET_NAME}/GOLD/NIIF/RM_TMP/join_rentas_df_tmp"
join_rentas_df.write.mode("overwrite").parquet(join_rentas_df_path)
rentas_df = spark.read.parquet(join_rentas_df_path).repartition(200)
# 1. Calculos para mesada (ipc_acumulado) 
# Expandir rango de años entre expedición y corte para mesada
df_ano_rentas = join_rentas_df.select(ren["nid_rentas"], ren["anio_expedicion"],mov_pago_filter["anio_corte_anterior"],mov_pago_filter["periodo_pago"])

ipc_acumulado = df_ano_rentas.withColumn(
    "anio_ipc",
    explode(sequence(col("anio_expedicion").cast("int"), col("anio_corte_anterior").cast("int")))
)

# Join con tabla de IPC   para mesada
ipc_acumulado = ipc_acumulado.join(
    ipc.select(col("ANIO").alias("anio_ipc"), (1 + (col("PCJ_IPC")/100)).alias("pcj_ipc_anp")),
    on="anio_ipc",
    how="left"
).distinct()

# Factor acumulado (logaritmos → suma → exp)   para mesada
incremento_factor = (
    ipc_acumulado
    .withColumn(
        "log_factor",
        # 1. Calcular el logaritmo natural del factor anual
        log(col("pcj_ipc_anp"))
    )
    .groupBy("nid_rentas", "anio_expedicion", "anio_corte_anterior", "periodo_pago")
    .agg(
        # 2. Sumar los logaritmos y aplicar la exponencial para obtener el producto
        exp(sum_("log_factor")).alias("factor_incremento_acumulado")
    )
)

# 2. Num Hijos
df_hijos = (
    join_rentas_df    
    .filter(
        (ben["cod_parentesco"] == "4")
        & (
            (date_format(ben["fec_aparicion"], "yyyyMM") < mov_pago_filter["periodo_pago"])
            | ben["fec_aparicion"].isNull()
        )
        & (
            ((ben["bln_con_derecho"] == "S") & (ben["bln_es_beneficiario"] == "S"))
            | (date_format(ben["fec_muerte"], "yyyyMM") > mov_pago_filter["periodo_pago"])
        )
    )
    .groupBy(ben["nid_rentas"])
    .agg(
        when(countDistinct(ben["nid_numero_identificacion"]) == 0, None)
        .otherwise(countDistinct(ben["nid_numero_identificacion"]))
        .alias("num_hijos")
    )
)

#3. Hijos Invalidos
df_hijos_invalidos = (
    join_rentas_df
    .filter(
        (ben["cod_parentesco"] == "4")
        & (ben["bln_con_derecho"] == "S")
        & (ben["cod_estado_persona"] == "2")
    )
    .groupBy(ben["nid_rentas"])
    .agg(
        when(countDistinct(ben["nid_numero_identificacion"]) == 0, None)
        .otherwise(countDistinct(ben["nid_numero_identificacion"]))
        .alias("num_hijos_invalidos")
    )
)

#4.Clasificacion de los beneficiarios para las observaciones (ben_filter) 
ben_filter = ben.filter(
    col("cod_parentesco").isin("3", "10")
).select(col("nid_rentas").alias("nid_rentas_ben"),col("cod_parentesco").alias("cod_parentesco_ben")).distinct()
ben_filter = ben_filter.withColumn("HAS_BENEFICIARIO_CLAVE", lit(True))
incremento_factor_df_path  = f"s3://{BUCKET_NAME}/GOLD/NIIF/RM_TMP/incremento_factor_df_tmp"
incremento_factor.write.mode("overwrite").parquet(incremento_factor_df_path)
incremento_factor_df = spark.read.parquet(incremento_factor_df_path).repartition(200)
#Join final join_rentas_df
join_rentas_df = (
    # hijos 
    join_rentas_df  
    .join(df_hijos, df_hijos["nid_rentas"] == ren["nid_rentas"], "left")
    # hijos inválidos 
    .join(df_hijos_invalidos, df_hijos_invalidos["nid_rentas"] == ren["nid_rentas"], "left")
    #Join beneficiarios observaciones
    .join(ben_filter, ben_filter["nid_rentas_ben"] == ren["nid_rentas"], how="left")
    # Join con incremento_factor para mesada
    #.join(incremento_factor, (incremento_factor["nid_rentas"] == ren["nid_rentas"]) & (mov_pago_filter["periodo_pago"] == incremento_factor["periodo_pago"]), how="left")
    #.join(incremento_factor, incremento_factor["nid_rentas"] == ren["nid_rentas"], how="left")
    .join(incremento_factor_df, on=["nid_rentas", "periodo_pago"], how="left")

)
from pyspark.sql.functions import desc
join_rentas_df.select(
    ren["nid_rentas"], 
    "periodo_pago", 
    "factor_incremento_acumulado"
).distinct().orderBy(desc("periodo_pago")).show()
df_hijos = (
    join_rentas_df    
    .filter(
        (ben["cod_parentesco"] == "4")
        & (
            (date_format(ben["fec_aparicion"], "yyyyMM") < mov_pago_filter["periodo_pago"])
            | ben["fec_aparicion"].isNull()
        )
        & (
            ((ben["bln_con_derecho"] == "S") & (ben["bln_es_beneficiario"] == "S"))
            | (date_format(ben["fec_muerte"], "yyyyMM") > mov_pago_filter["periodo_pago"])
        )
    )
    .groupBy(ben["nid_rentas"])
    .agg(
        when(countDistinct(ben["nid_numero_identificacion"]) == 0, None)
        .otherwise(countDistinct(ben["nid_numero_identificacion"]))
        .alias("num_hijos")
    )
)
df_hijos_invalidos = (
    join_rentas_df
    .filter(
        (ben["cod_parentesco"] == "4")
        & (ben["bln_con_derecho"] == "S")
        & (ben["cod_estado_persona"] == "2")
    )
    .groupBy(ben["nid_rentas"])
    .agg(
        when(countDistinct(ben["nid_numero_identificacion"]) == 0, None)
        .otherwise(countDistinct(ben["nid_numero_identificacion"]))
        .alias("num_hijos_invalidos")
    )
)
ben_filter = ben.filter(
    col("cod_parentesco").isin("3", "10")
).select(col("nid_rentas").alias("nid_rentas_ben"),col("cod_parentesco").alias("cod_parentesco_ben")).distinct()
ben_filter = ben_filter.withColumn("HAS_BENEFICIARIO_CLAVE", lit(True))
join_rentas_df = (
    # hijos 
    join_rentas_df  
    .join(df_hijos, df_hijos["nid_rentas"] == ren["nid_rentas"], "left")
    # hijos inválidos 
    .join(df_hijos_invalidos, df_hijos_invalidos["nid_rentas"] == ren["nid_rentas"], "left")
    #Join beneficiarios observaciones
    .join(ben_filter, ben_filter["nid_rentas_ben"] == ren["nid_rentas"], how="left")
    # Join con incremento_factor para mesada
    .join(incremento_factor, on="nid_rentas", how="left")

)
#campo 7  ORIGEN_PENSION
final_df = join_rentas_df.withColumn("ORIGEN_PENSION",
    when(per_nat["fec_muerte"].isNull(),
        when(ren["nid_rentas"].isNotNull(),
            when(df_spr_siniestros["cod_estado_siniestro"] == "4", 3)
            .otherwise(1)
        ).when((ren["nid_rentas"].isNull()) & (ren["cod_origen"] == "3"), 3)
         .when((ren["nid_rentas"].isNull()) & (ren["cod_afp"] != "1") & (ren["cod_origen"] == "2"), 1)
         .when((ren["nid_rentas"].isNull()) & (ren["cod_origen"] == "2"), 1)
    ).otherwise(
        when(ren["cod_origen"] == "1",
            when((date_format(per_nat["fec_muerte"], "yyyyMMdd").cast("long") >= date_format(ren["fec_expedicion"], "yyyyMMdd").cast("long")) &
                 ren["nid_rentas"].isNull(), 5)
            .when(date_format(per_nat["fec_muerte"], "yyyyMMdd").cast("long") < date_format(ren["fec_expedicion"], "yyyyMMdd").cast("long"), 2)
            .when((date_format(per_nat["fec_muerte"], "yyyyMMdd").cast("long") >= date_format(ren["fec_expedicion"], "yyyyMMdd").cast("long")) &
                  ren["nid_rentas"].isNotNull(), 4)
        ).when(ren["cod_origen"] == "2", 1)
         .when(ren["cod_origen"] == "3", 3)
    )
)
final_df = final_df.withColumn(
    "tiempoTemporalidad",
    when((col("clase_pension") == 3) & 
        (ben_filter["COD_PARENTESCO_BEN"] == 3) & 
        (ben_filter["COD_PARENTESCO_BEN"] == 3),
        when(col("fec_nacimiento") > col("fec_nacimiento"),
            (25 - col("edad_beneficiario") + 1) * 12
        ).otherwise(
            (25 - col("edad_beneficiario") + 1) * 12
        )
    ).when((col("clase_pension") == 3) & 
        (ben_filter["COD_PARENTESCO_BEN"] == 3) & 
        ben_filter["COD_PARENTESCO_BEN"].isNull(),
        (25 - col("edad_beneficiario") + 1) * 12
    ).when((col("clase_pension") == 3) & (
       ((ben_filter["COD_PARENTESCO_BEN"] == 7) & (ben_filter["COD_PARENTESCO_BEN"] == 7)) |
       ((ben_filter["COD_PARENTESCO_BEN"] == 7) & ben_filter["COD_PARENTESCO_BEN"].isNull())),
    when((year(to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd")) - year(per_nat["fec_muerte"])) == 20,
         floor(240 - months_between(to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd"), per_nat["fec_muerte"]))
    ).otherwise(
         (20 - (year(to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd")) - year(per_nat["fec_muerte"]))) * 12
    )
    ).when((col("clase_pension") == 3) & (
       ((ben_filter["COD_PARENTESCO_BEN"] == 3) & (ben_filter["COD_PARENTESCO_BEN"] == 7)) |
       ((ben_filter["COD_PARENTESCO_BEN"] == 7) & (ben_filter["COD_PARENTESCO_BEN"] == 3))),
    when(ben_filter["COD_PARENTESCO_BEN"] == 3,
         (25 - col("edad_beneficiario") + 1) * 12
    ).otherwise(
         (25 - col("edad_beneficiario") + 1) * 12
    ).alias("temporalidad1")
    )
)
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
import boto3
import json
import csv
import re
from collections import defaultdict
from io import StringIO

# Parámetros de S3
bucket_name = "alfa-niif-bronce-test"
input_key = "ALFAPRD-UT/consultasSqlOro/consulta.sql"
json_output_key = "ALFAPRD-UT/consultasJsonOro/consulta.json"
csv_output_key = "ALFAPRD-UT/consultasJsonOro/listaconsulta.csv"

# Leer archivo desde S3
s3 = boto3.client("s3")
response = s3.get_object(Bucket=bucket_name, Key=input_key)
sql_content = response["Body"].read().decode("utf-8")

# Procesar las consultas
queries = [q.strip().replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ') for q in sql_content.strip().split(";") if q.strip()]
schemas = defaultdict(lambda: defaultdict(lambda: {
    "fields": [],
    "joins": [],
    "where": [],
    "group_by": [],
    "order_by": [],
    "sql_query": []
}))

query_id_map = {}
query_id = 1

for query in queries:
    query_id_map[query] = query_id
    query_id += 1

# Parseo de cada consulta
for query in queries:
    qid = query_id_map[query]
    select_match = re.search(r"SELECT\s+(.*?)\s+FROM", query, re.DOTALL | re.IGNORECASE)
    fields = [f.strip() for f in select_match.group(1).split(",")] if select_match else []

    from_match = re.search(r"FROM\s+(\w+\.\w+)\s+(\w+)", query, re.IGNORECASE)
    if not from_match:
        continue
    main_table, main_alias = from_match.groups()

    join_matches = re.findall(
        r"(LEFT|INNER|RIGHT|FULL)?\s*JOIN\s+(\w+\.\w+)\s+(\w+)\s+ON\s+(.*?)(?:WHERE|ORDER|GROUP|$)",
        query, re.DOTALL | re.IGNORECASE
    )
    joins = [ {
        "type": (j[0].strip() + " JOIN") if j[0] else "JOIN",
        "table": j[1],
        "on": j[3].strip(),
        "main_table": main_table,
        "id_sql_query": [qid]
    } for j in join_matches]

    order_by = []
    order_match = re.search(r"ORDER BY\s+(.*)", query, re.IGNORECASE)
    if order_match:
        order_by = [{"name": o.strip().replace(";", ""), "id_sql_query": [qid]} for o in order_match.group(1).split(",")]

    alias_to_table = {main_alias: main_table}
    for join in join_matches:
        alias_to_table[join[2]] = join[1]

    for field in fields:
        match_alias = re.match(r"(\w+)\.(\w+)(\s+as\s+(\w+))?", field, re.IGNORECASE)
        if match_alias:
            alias, col, _, col_alias = match_alias.groups()
            field_obj = {
                "name": field,
                "alias": col_alias if col_alias else None,
                "id_sql_query": [qid]
            }
            if alias in alias_to_table:
                schema, table = alias_to_table[alias].split(".")
                schemas[schema][table]["fields"].append(field_obj)

    for join in joins:
        for target in [main_table, join["table"]]:
            schema, table = target.split(".")
            existing = schemas[schema][table]["joins"]
            found = False
            for j in existing:
                if j["type"] == join["type"] and j["table"] == join["table"] and j["on"] == join["on"]:
                    j["id_sql_query"].append(qid)
                    found = True
                    break
            if not found:
                schemas[schema][table]["joins"].append(join)

    main_schema, main_tab = main_table.split(".")
    schemas[main_schema][main_tab]["order_by"].extend(order_by)
    cleaned_query = query.strip()
    schemas[main_schema][main_tab]["sql_query"].append({"id": qid, "query": cleaned_query})
    for join in joins:
        jschema, jtable = join["table"].split(".")
        schemas[jschema][jtable]["sql_query"].append({"id": qid, "query": cleaned_query})

# Asignar condiciones WHERE por alias
for schema in schemas:
    for table in schemas[schema]:
        schemas[schema][table]["where"] = []

for query in queries:
    qid = query_id_map[query]
    from_match = re.search(r"FROM\s+(\w+\.\w+)\s+(\w+)", query, re.IGNORECASE)
    if not from_match:
        continue
    main_table, main_alias = from_match.groups()
    join_matches = re.findall(
        r"(LEFT|INNER|RIGHT|FULL)?\s*JOIN\s+(\w+\.\w+)\s+(\w+)\s+ON\s+(.*?)(?:WHERE|ORDER|GROUP|$)",
        query, re.DOTALL | re.IGNORECASE)
    alias_to_table = {main_alias: main_table}
    for join in join_matches:
        alias_to_table[join[2]] = join[1]

    where_match = re.search(r"WHERE\s+(.*?)(ORDER BY|GROUP BY|$)", query, re.DOTALL | re.IGNORECASE)
    if where_match:
        where_conditions = [w.strip() for w in re.split(r"\s+AND\s+", where_match.group(1).strip())]
        for condition in where_conditions:
            match = re.search(r"(\b\w+)\.", condition)
            if match:
                alias = match.group(1)
                if alias in alias_to_table:
                    schema, table = alias_to_table[alias].split(".")
                    exists = False
                    for cond in schemas[schema][table]["where"]:
                        if cond["and"] == condition:
                            cond["id_sql_query"].append(qid)
                            exists = True
                            break
                    if not exists:
                        schemas[schema][table]["where"].append({"and": condition, "id_sql_query": [qid]})

# Construir JSON con estructura final
final_output = {}
for schema, tables in schemas.items():
    final_output[schema] = []
    for table_name, data in tables.items():
        unique_fields = {}
        for f in data["fields"]:
            key = (f["name"], f["alias"])
            if key in unique_fields:
                unique_fields[key]["id_sql_query"].extend(f["id_sql_query"])
            else:
                unique_fields[key] = f

        unique_joins = {}
        for j in data["joins"]:
            key = (j["type"], j["table"], j["on"])
            if key in unique_joins:
                unique_joins[key]["id_sql_query"].extend(j["id_sql_query"])
            else:
                unique_joins[key] = j

        unique_where = {}
        for w in data["where"]:
            key = w["and"]
            if key in unique_where:
                unique_where[key]["id_sql_query"].extend(w["id_sql_query"])
            else:
                unique_where[key] = w

        unique_order = {}
        for o in data["order_by"]:
            key = o["name"]
            if key in unique_order:
                unique_order[key]["id_sql_query"].extend(o["id_sql_query"])
            else:
                unique_order[key] = o

        final_output[schema].append({
            "tables": table_name,
            "fields": list(unique_fields.values()),
            "joins": list(unique_joins.values()),
            "where": list(unique_where.values()),
            "group_by": data["group_by"],
            "order_by": list(unique_order.values()),
            "sql_query": data["sql_query"]
        })

# Guardar JSON final en S3
s3.put_object(
    Bucket=bucket_name,
    Key=json_output_key,
    Body=json.dumps(final_output, indent=4).encode("utf-8")
)

# Crear CSV con id y consulta
csv_buffer = StringIO()
writer = csv.DictWriter(csv_buffer, fieldnames=["id", "sql_query"])
writer.writeheader()
for query, qid in query_id_map.items():
    writer.writerow({"id": qid, "sql_query": query})

s3.put_object(
    Bucket=bucket_name,
    Key=csv_output_key,
    Body=csv_buffer.getvalue().encode("utf-8")
)

print(f"✅ Archivos generados correctamente en S3:\n- JSON: s3://{bucket_name}/{json_output_key}\n- CSV: s3://{bucket_name}/{csv_output_key}")
import boto3
import json
import csv
import re
from collections import defaultdict
from io import StringIO

# Configuración de S3
bucket_name = "alfa-niif-bronce-test"
input_key = "ALFAPRD-UT/consultasSqlOro/consulta.sql"
json_output_key = "ALFAPRD-UT/consultasJsonOro/consulta.json"
csv_output_key = "ALFAPRD-UT/consultasJsonOro/listaconsulta.csv"

# Leer archivo desde S3
s3 = boto3.client("s3")
response = s3.get_object(Bucket=bucket_name, Key=input_key)
sql_content = response["Body"].read().decode("utf-8")

# Procesar consultas SQL
queries = [q.strip().replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ') 
          for q in sql_content.strip().split(";") if q.strip()]

# Estructura para almacenar metadatos
schemas = defaultdict(lambda: defaultdict(lambda: {
    "fields": defaultdict(lambda: {"name": "", "alias": None, "id_sql_query": []}),
    "joins": [],
    "where": [],
    "group_by": [],
    "order_by": [],
    "sql_query": []
}))

query_id_map = {query: idx+1 for idx, query in enumerate(queries)}

def extract_fields_from_expression(expression, alias_to_table, qid):
    fields = []
    pattern = re.compile(r'(\b\w+)\.(\w+)(?:\s+AS\s+(\w+))?', re.IGNORECASE)
    for match in pattern.finditer(expression):
        alias, col, col_alias = match.groups()
        if alias in alias_to_table:
            fields.append({
                "name": f"{alias}.{col}",
                "alias": col_alias,
                "id_sql_query": [qid]
            })
    return fields

def extract_joins(query):
    joins = []
    pattern = re.compile(
        r'(LEFT|RIGHT|INNER|FULL)\s+JOIN\s+([\w\.]+)\s+(\w+)\s+ON\s+(.+?)(?=(?:\s+(?:LEFT|RIGHT|INNER|FULL)\s+JOIN|\s+WHERE|\s+ORDER\s+BY|\s+GROUP\s+BY|$))',
        re.IGNORECASE | re.DOTALL
    )
    for match in pattern.finditer(query):
        join_type, join_table, join_alias, join_condition = match.groups()
        joins.append({
            "type": f"{join_type} JOIN",
            "table": join_table,
            "alias": join_alias,
            "on": join_condition.strip()
        })
    return joins

def process_query(query, qid):
    # Extraer tabla principal
    from_match = re.search(r"FROM\s+([\w\.]+)\s+(\w+)", query, re.IGNORECASE)
    if not from_match:
        return
    
    main_table, main_alias = from_match.groups()
    main_schema, main_tab = main_table.split('.') if '.' in main_table else ('default', main_table)
    alias_to_table = {main_alias: main_table}
    
    # Extraer JOINs
    joins = extract_joins(query)
    for join in joins:
        alias_to_table[join["alias"]] = join["table"]
    
    # Procesar campos SELECT
    select_match = re.search(r"SELECT\s+(.+?)\s+FROM", query, re.IGNORECASE | re.DOTALL)
    if select_match:
        select_fields = extract_fields_from_expression(select_match.group(1), alias_to_table, qid)
        for field in select_fields:
            alias = field["name"].split('.')[0]
            schema, table = alias_to_table[alias].split('.') if '.' in alias_to_table[alias] else ('default', alias_to_table[alias])
            field_obj = schemas[schema][table]["fields"][field["name"]]
            if not field_obj["name"] or qid not in field_obj["id_sql_query"]:
                field_obj.update({
                    "name": field["name"],
                    "alias": field["alias"],
                    "id_sql_query": sorted(list(set(field_obj.get("id_sql_query", []) + [qid])))
                })
    
    # Procesar JOINs y sus condiciones
    for join in joins:
        schema, table = join["table"].split('.') if '.' in join["table"] else ('default', join["table"])
        
        # Registrar JOIN en tabla principal
        main_join = {
            "type": join["type"],
            "table": join["table"],
            "on": join["on"],
            "id_sql_query": [qid]
        }
        schemas[main_schema][main_tab]["joins"].append(main_join)
        
        # Registrar JOIN inverso en tabla secundaria
        reverse_join = {
            "type": join["type"],
            "table": main_table,
            "on": join["on"],
            "id_sql_query": [qid]
        }
        schemas[schema][table]["joins"].append(reverse_join)
        
        # Procesar campos de condiciones ON
        on_fields = extract_fields_from_expression(join["on"], alias_to_table, qid)
        for field in on_fields:
            alias = field["name"].split('.')[0]
            schema, table = alias_to_table[alias].split('.') if '.' in alias_to_table[alias] else ('default', alias_to_table[alias])
            field_obj = schemas[schema][table]["fields"][field["name"]]
            if not field_obj["name"] or qid not in field_obj["id_sql_query"]:
                field_obj.update({
                    "name": field["name"],
                    "alias": field["alias"],
                    "id_sql_query": sorted(list(set(field_obj.get("id_sql_query", []) + [qid])))
                })
    
    # Procesar WHERE
    where_match = re.search(r"WHERE\s+(.+?)(?:ORDER BY|GROUP BY|$)", query, re.IGNORECASE | re.DOTALL)
    if where_match:
        conditions = [cond.strip() for cond in re.split(r"\s+AND\s+", where_match.group(1))]
        for cond in conditions:
            for alias in alias_to_table:
                if f"{alias}." in cond:
                    schema, table = alias_to_table[alias].split('.') if '.' in alias_to_table[alias] else ('default', alias_to_table[alias])
                    existing_where = next((w for w in schemas[schema][table]["where"] if w["and"] == cond), None)
                    if not existing_where:
                        schemas[schema][table]["where"].append({
                            "and": cond,
                            "id_sql_query": [qid]
                        })
                    elif qid not in existing_where["id_sql_query"]:
                        existing_where["id_sql_query"].append(qid)
    
    # Registrar consulta SQL en todas las tablas involucradas
    tables_involved = {main_table: True}
    for join in joins:
        tables_involved[join["table"]] = True
    
    for table_full in tables_involved:
        schema, table = table_full.split('.') if '.' in table_full else ('default', table_full)
        schemas[schema][table]["sql_query"].append({
            "id": qid,
            "query": query.strip()
        })

# Procesar todas las consultas
for query, qid in query_id_map.items():
    process_query(query, qid)

# Construir JSON final
final_output = {}
for schema in schemas:
    final_output[schema] = []
    for table in schemas[schema]:
        # Procesar campos
        fields = []
        for field_name, field_data in schemas[schema][table]["fields"].items():
            if field_data["name"]:  # Solo si el campo existe
                fields.append({
                    "name": field_data["name"],
                    "alias": field_data["alias"],
                    "id_sql_query": sorted(list(set(field_data["id_sql_query"])))
                })
        
        # Eliminar duplicados en joins
        unique_joins = []
        seen_joins = set()
        for join in schemas[schema][table]["joins"]:
            join_key = (join["type"], join["table"], join["on"])
            if join_key not in seen_joins:
                seen_joins.add(join_key)
                join["id_sql_query"] = sorted(list(set(join["id_sql_query"])))
                unique_joins.append(join)
        
        # Eliminar duplicados en where
        unique_where = []
        seen_where = set()
        for where in schemas[schema][table]["where"]:
            if where["and"] not in seen_where:
                seen_where.add(where["and"])
                where["id_sql_query"] = sorted(list(set(where["id_sql_query"])))
                unique_where.append(where)
        
        # Ordenar consultas por ID
        sql_queries = sorted(schemas[schema][table]["sql_query"], key=lambda x: x["id"])
        
        final_output[schema].append({
            "tables": table,
            "fields": sorted(fields, key=lambda x: x["name"]),
            "joins": unique_joins,
            "where": unique_where,
            "group_by": schemas[schema][table]["group_by"],
            "order_by": schemas[schema][table]["order_by"],
            "sql_query": sql_queries
        })

# Guardar JSON en S3
s3.put_object(
    Bucket=bucket_name,
    Key=json_output_key,
    Body=json.dumps(final_output, indent=4, ensure_ascii=False).encode("utf-8")
)

# Generar CSV de consultas
csv_buffer = StringIO()
writer = csv.DictWriter(csv_buffer, fieldnames=["id", "sql_query"])
writer.writeheader()
for query, qid in query_id_map.items():
    writer.writerow({"id": qid, "sql_query": query})

s3.put_object(
    Bucket=bucket_name,
    Key=csv_output_key,
    Body=csv_buffer.getvalue().encode("utf-8")
)

print(f"✅ Archivos generados:\n- JSON: s3://{bucket_name}/{json_output_key}\n- CSV: s3://{bucket_name}/{csv_output_key}")
import boto3
import json
import re

def get_partition_columns(rules):
    partition_columns = []
    for rule in rules:
        if isinstance(rule, str) and rule.startswith("IsPrimaryKey"):
            match = re.search(r'IsPrimaryKey\s+"?([\w_]+)"?', rule)
            if match:
                partition_columns.append(match.group(1).upper())
    return list(set(partition_columns))

# Parámetros S3
bucket = "alfa-niif-bronce-test"
consulta_key = "ALFAPRD-UT/consultasJsonOro/consulta.json"
output_key = "ALFAPRD-UT/utilitariosSilver/tables_case_use.py"
bucket_rules = "alfa-datalake-dev-assets"

# Inicializar cliente S3
s3 = boto3.client("s3")

# Leer archivo consulta.json desde S3
response = s3.get_object(Bucket=bucket, Key=consulta_key)
consultas_data = json.loads(response["Body"].read().decode("utf-8"))

# Cargar archivos de reglas desde S3
rules_data = {}
for schema in consultas_data.keys():
    rules_key = f"support_phase_files/niif/alfaprd_{schema.lower()}_rules_tables.json"
    try:
        resp = s3.get_object(Bucket=bucket_rules, Key=rules_key)
        rules_data[schema.lower()] = json.loads(resp["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        print(f"❌ No se encontró archivo de reglas para {schema}")
        rules_data[schema.lower()] = {}

# Construcción del archivo Python
py_lines = ["from pyspark.sql.functions import col, trim\n\n", "tables_case_use = {\n"]

for schema, tables in consultas_data.items():
    schema_lower = schema.lower()
    schema_rules_dict = rules_data.get(schema_lower, {})
    schema_rules = schema_rules_dict.get(schema.upper(), [])

    py_lines.append(f'    "{schema.upper()}": {{\n')
    for table in tables:
        table_name = table["tables"]
        fields = table["fields"]

        table_name_clean = table_name.strip().upper()
        rule_entry = {}
        for tbl in schema_rules:
            if isinstance(tbl, dict):
                tbl_table = tbl.get("table", "")
                tbl_name = tbl_table.strip().upper()
                if tbl_name == table_name_clean or tbl_name.endswith(f".{table_name_clean}"):
                    rule_entry = tbl
                    break

        if not rule_entry:
            print(f"❌ No se encontraron reglas para tabla '{table_name}' en esquema '{schema}'")
            continue

        rules = rule_entry.get("rules", [])
        partition_columns = get_partition_columns(rules)

        selected_cols = []
        selected_cols_set = set()
        string_cols = set()

        for f in fields:
            original = f["name"]
            match = re.match(r'(?:\w+\.)?(\w+)', original, re.IGNORECASE)
            if match:
                colname = match.group(1).upper()
                if colname not in selected_cols_set:
                    selected_cols.append(colname)
                    selected_cols_set.add(colname)
                    has_string_type = any(
                        re.search(rf'ColumnDataType\s+"?{colname}"?\s*=\s*"STRING"', rule, re.IGNORECASE)
                        for rule in rules
                    )
                    if has_string_type:
                        string_cols.add(colname)

        ordered_cols = partition_columns + [c for c in selected_cols if c not in partition_columns]

        clean_fields = []
        for colname in ordered_cols:
            if colname in string_cols:
                clean_fields.append(f'trim(col("{colname}")).alias("{colname}")')
            else:
                clean_fields.append(f'col("{colname}").alias("{colname}")')

        transform_str = (
            "        [\n"
            "            lambda df: df.select(\n"
            "                " + ",\n                ".join(clean_fields) + "\n"
            "            ),\n"
            "            lambda df: df.dropDuplicates()\n"
            "        ]"
        )

        py_lines.append(f'        "{table_name.upper()}": {{\n')
        py_lines.append(f'            "partition_columns": {partition_columns},\n')
        py_lines.append(f'            "schema": "{schema.upper()}",\n')
        py_lines.append(f'            "transform": \n{transform_str}\n')
        py_lines.append("        },\n")
    py_lines.append("    },\n")
py_lines.append("}")

# Subir archivo generado a S3
s3.put_object(
    Bucket=bucket,
    Key=output_key,
    Body="".join(py_lines).encode("utf-8")
)

print(f"✅ Archivo tables_case_use.py guardado correctamente en s3://{bucket}/{output_key}")
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
DB_CATALOG_GLUE = 'datalake-dev-silver-catalog-db'
 #Read tables from silver schema general
df_acu_personas_naturales = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ACU_PERSONAS_NATURALES").toDF()
df_cgr_origenes = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_ORIGENES").toDF()
df_cgr_tipos_identificaciones = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_TIPOS_IDENTIFICACIONES").toDF()

#Read tables from silver schema prv
df_prv_endosos_point = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_ENDOSOS_POINT").toDF()
df_prv_rentas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_RENTAS").toDF()
df_prv_saldos_matematica = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_SALDOS_MATEMATICA").toDF()
df_join_prv_rentas = df_prv_rentas.join(
        df_prv_saldos_matematica,
        df_prv_rentas["nid_rentas"] == df_prv_saldos_matematica["nid_rentas"],
        "left"
    )
    
# Join result with acu_personas_naturales (INNER JOIN)
df_join_prv_rentas = df_join_prv_rentas.join(
    df_acu_personas_naturales,
    (df_prv_rentas["cod_tipo_identificacion"] == df_acu_personas_naturales["cod_tipo_identificacion"]) &
    (df_prv_rentas["nid_numero_identificacion"] == df_acu_personas_naturales["nid_numero_identificacion"]),
    "inner"
)

# Join result with cgr_origenes (INNER JOIN)
df_join_prv_rentas = df_join_prv_rentas.join(
    df_cgr_origenes,
    df_prv_rentas["cod_origen"] == df_cgr_origenes["cod_origen"],
    "inner"
)
df_join_prv_rentas.printSchema()
# Select DISTINCT rows with required columns
df_campos_anexos_1 = df_join_prv_rentas.select(
    df_prv_saldos_matematica["nid_rentas"],
    df_prv_saldos_matematica["nid_calculo"],
    df_prv_rentas["fec_expedicion"],
    df_prv_rentas["fec_envio_point"],
    df_cgr_origenes["nombre"].alias("tipo_renta"),
    df_prv_rentas["vlr_prima_unica"]
).distinct()  # Ensure distinct rows
df_campos_anexos_1.show()
#Add cols tipo_proceso, y unidad__de_cuenta    
df_campos_anexos_1 = df_campos_anexos_1.withColumn("Tipo_proceso", lit("Renta nueva"))\
    .withColumn("Unidad_de_cuenta", lit(""))

#Select final dataframe for gold stage
df_campos_anexos_1 = df_campos_anexos_1.select(
    df_campos_anexos_1["nid_rentas"].alias("Numero_renta"),          # NID_RENTAS from PRV_SALDOS_MATEMATICA
    df_campos_anexos_1["vlr_prima_unica"].alias("Valor_prima"),      # vlr_prima_unica from PRV_RENTAS
    df_campos_anexos_1["tipo_renta"].alias("Tipo_de_renta"),             # Nombre from CGR_ORIGENES (joined via PRV_RENTAS.cod_origen)
    df_campos_anexos_1["fec_expedicion"].alias("Fecha_expedicion"),  # FEC_EXPEDICION from PRV_RENTAS
    df_campos_anexos_1["fec_envio_point"].alias("Periodo_point"),    # FEC_ENVIO_POINT from PRV_RENTAS
    df_campos_anexos_1["Tipo_proceso"].alias("Tipo_proceso"),        # Tipo_proceso from N/A
    df_campos_anexos_1["Unidad_de_cuenta"].alias("Unidad_de_cuenta") # Unidad_de_cuenta from N/A
)
df_campos_anexos_1.show()
from pyspark.sql.functions import col, trim, date_format, lit
df_prv_endosos_point.printSchema()
#-----------------------------------------------------------------#
#------------------------ SubQuery -------------------------------#
#-----------------------------------------------------------------#

df_filter_prv_endosos_point = df_prv_endosos_point.filter(
    (df_prv_endosos_point["bln_proceso"] == "S" ) &                                         # Filter for specific concept bln_proceso
    (df_prv_endosos_point["vlr_total_recaudo"] != df_prv_endosos_point["vlr_total"] ) &     # Compare columns for vlr_total_recaudo & vlr_total
    (df_prv_endosos_point["nid_endoso_reversion"].isNull()) &                               # Ensure nid_endoso_reversion is null
    (df_prv_endosos_point["nid_endoso_reversado"].isNull())                                 # Ensure nid_endoso_reversado is null
)


#-----------------------------------------------------------------#
#---------------------- Join Tables ------------------------------#
#-----------------------------------------------------------------#
# Subquery -> prv_rentas -> cgr_origenes

df_join_prv_endosos_point = df_filter_prv_endosos_point.join(
    df_prv_rentas,
    df_filter_prv_endosos_point["nid_rentas"] == df_prv_rentas["nid_rentas"]
)

df_join_prv_endosos_point = df_join_prv_endosos_point.join(
    df_cgr_origenes,
    df_filter_prv_endosos_point["cod_origen"] == df_cgr_origenes["cod_origen"]
)

# Select rows with required columns
df_campos_anexos_2 = df_join_prv_endosos_point.select(
    df_filter_prv_endosos_point["nid_rentas"],
    df_filter_prv_endosos_point["vlr_total"],
    df_cgr_origenes["nombre"],
    df_filter_prv_endosos_point["fec_expedicion"],
    df_filter_prv_endosos_point["fec_envio_point"],
)
#-----------------------------------------------------------------#
#----------------------- Transforms ------------------------------#
#-----------------------------------------------------------------#

#Add cols tipo_proceso, y unidad__de_cuenta    
df_campos_anexos_2 = df_campos_anexos_2.withColumn("Tipo_proceso", lit("Endoso"))\
    .withColumn("Unidad_de_cuenta", lit(""))

#Select final dataframe for gold stage
df_campos_anexos_2 = df_campos_anexos_2.select(
    df_campos_anexos_2["nid_rentas"].alias("Numero_renta"),          # NID_RENTAS from PRV_ENDOSOS_POINT
    df_campos_anexos_2["vlr_total"].alias("Valor_prima"),            # vlr_prima_unica from PRV_ENDOSOS_POINT
    df_campos_anexos_2["nombre"].alias("Tipo_de_renta"),             # Nombre from CGR_ORIGENES (joined via PRV_RENTAS.cod_origen)
    df_campos_anexos_2["fec_expedicion"].alias("Fecha_expedicion"),  # FEC_EXPEDICION from PRV_ENDOSOS_POINT
    df_campos_anexos_2["fec_envio_point"].alias("Periodo_point"),    # FEC_ENVIO_POINT from PRV_ENDOSOS_POINT
    df_campos_anexos_2["Tipo_proceso"].alias("Tipo_proceso"),        # Tipo_proceso from N/A
    df_campos_anexos_2["Unidad_de_cuenta"].alias("Unidad_de_cuenta") # Unidad_de_cuenta from N/A
)
"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import col, trim

import sys, traceback, boto3, json
BUCKET_NAME = 'alfa-datalake-dev-silver'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
DB_NAME = 'ALFAPRD'
DB_SCHEMA = 'GENERAL'
PERIOD = '202501'
CERTIFICATE = False
DB_CATALOG_GLUE = 'datalake-dev-bronce-catalog-db'

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue import DynamicFrame
from pyspark.sql.functions import col, trim
import boto3

# Configuración fija para pruebas en notebook
BUCKET_NAME = 'alfa-datalake-dev-silver'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
DB_NAME = 'ALFAPRD'
DB_SCHEMA = 'GENERAL'
PERIOD = '202501'
CERTIFICATE = False
DB_CATALOG_GLUE = 'datalake-dev-bronce-catalog-db'

# Inicialización de contexto
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")



# Diccionario de tablas
tables_case_use = {
    "CGR_ORIGENES": {
        "partition_columns": [],
        "schema": "GENERAL",
        "transform": [
            lambda df: df.select(
                trim(col("COD_ORIGEN")).alias("COD_ORIGEN"),
                trim(col("COD_ORIGEN_SUPER")).alias("COD_ORIGEN_SUPER"),
                trim(col("NOMBRE")).alias("NOMBRE")
            ),
            lambda df: df.dropDuplicates()
        ]
    }
}


from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue import DynamicFrame
from pyspark.sql.functions import col, trim
from pyspark.sql.functions import regexp_replace
import boto3

# Configuración
BUCKET_NAME = 'alfa-datalake-dev-silver'
DB_NAME = 'ALFAPRD'
DB_SCHEMA = 'GENERAL'
DB_CATALOG_GLUE = 'datalake-dev-bronce-catalog-db'

# Inicialización
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client("s3")

# Leer la tabla desde el catálogo de Glue
table_name = f"{DB_NAME}_{DB_SCHEMA}_ACU_PERSONAS_NATURALES"

df_raw = glueContext.create_dynamic_frame.from_catalog(
    database=DB_CATALOG_GLUE,
    table_name=table_name
).toDF()

# Mostrar campos antes del trim
print("📌 Antes de aplicar trim():")
df_raw.filter((col("NID_NUMERO_IDENTIFICACION").isin("39151988", "38885387","32885265","22446462")) ).select(
    "NID_NUMERO_IDENTIFICACION",
    "COD_TIPO_IDENTIFICACION",
    "GENERO",
    "PRIMER_APELLIDO",
    "PRIMER_NOMBRE",
    "SEGUNDO_APELLIDO",
    "SEGUNDO_NOMBRE"
).show(truncate=False)

# Aplicar trim (como en tu transformación)
df_trimmed = df_raw.filter( (col("NID_NUMERO_IDENTIFICACION").isin("39151988", "38885387","32885265","22446462")) ).select(
    col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
    trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
    col("FEC_NACIMIENTO").alias("FEC_NACIMIENTO"),
    trim(col("GENERO")).alias("GENERO"),
    trim(regexp_replace(regexp_replace(trim(col("PRIMER_APELLIDO")), r"[\r\n\t]", " "), r"\s+", " ")).alias("PRIMER_APELLIDO"),
    trim(regexp_replace(regexp_replace(trim(col("PRIMER_NOMBRE")), r"[\r\n\t]", " "), r"\s+", " ")).alias("PRIMER_NOMBRE"),
    trim(regexp_replace(regexp_replace(trim(col("SEGUNDO_APELLIDO")), r"[\r\n\t]", " "), r"\s+", " ")).alias("SEGUNDO_APELLIDO"),
    trim(regexp_replace(regexp_replace(trim(col("SEGUNDO_NOMBRE")), r"[\r\n\t]", " "), r"\s+", " ")).alias("SEGUNDO_NOMBRE"),
    col("FEC_MUERTE").alias("FEC_MUERTE")
).dropDuplicates()

# Mostrar campos después del trim
print("✅ Después de aplicar trim():")
df_trimmed.select(
    "NID_NUMERO_IDENTIFICACION",
    "COD_TIPO_IDENTIFICACION",
    "GENERO",
    "PRIMER_APELLIDO",
    "PRIMER_NOMBRE",
    "SEGUNDO_APELLIDO",
    "SEGUNDO_NOMBRE"
).show(truncate=False)
"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim, date_format, to_date, lit, coalesce, min, max, broadcast, year, when, concat_ws, substring, countDistinct, months_between, round, floor, greatest, concat, explode, sequence, log, exp, sum as sum_, row_number
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, DoubleType, DecimalType
from pyspark.sql.window import Window

import sys, traceback, boto3, json


BUCKET_NAME = 'alfa-datalake-dev-gold'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
DB_NAME = 'ALFAPRD'
DB_SCHEMA = 'PRV'
PERIOD = '202501'
CERTIFICATE = 'False'
DB_CATALOG_GLUE = 'datalake-dev-silver-catalog-db'
TABLE_NAME_GOLD = 'RESERVA_RVI_GOLD'
FEC_CORTE = '2003-12-29'
FEC_CORTE_DATE = '2024-11-07'

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
# Define the schema for the  Final DataFrame
schema_reserva_rvi_gold = StructType([
    StructField("RAMO", IntegerType(), True),                         # campo 1 - VARCHAR2(3 BYTE)
    StructField("UNIDAD", IntegerType(), True),                       # campo 2 - N/A
    StructField("NO_POLIZA", DecimalType(10,0), True),                   # campo 3 - NUMBER(10,0)
    StructField("VIG_INICIAL", DateType(), True),                    # campo 4
    StructField("NO_SINIESTRO", DecimalType(10,0), True),               # campo 5 - NUMBER(10,0)
    StructField("FECHA_CAUSACION_DERECHO", DateType(), True),       # campo 6
    StructField("ORIGEN_PENSION", IntegerType(), True),              # campo 7 - VARCHAR2(3 BYTE)
    StructField("PARENT", IntegerType(), True),                       # campo 8 - N/A
    StructField("TIPO_IDENTIF", StringType(), True),                # campo 9 - VARCHAR2(2 BYTE)
    StructField("NO_IDENTIF", DecimalType(15,0), True),                     # campo 10 - NUMBER(15,0)
    StructField("SEXO", IntegerType(), True),                         # campo 11 - CHAR(1 BYTE)
    StructField("FECHA_NACIMIENTO", DateType(), True),              # campo 12
    StructField("NOMBRE", StringType(), True),                      # campo 13 - VARCHAR2(80 BYTE)
    StructField("ESTUDIOS", StringType(), True),                    # campo 14 - N/A
    StructField("ESTADO", StringType(), True),                      # campo 15 - VARCHAR2(3 BYTE)
    StructField("NO_HIJOS", IntegerType(), True),                      # campo 16 - NUMBER(15,0)
    StructField("NO_HIJOS_INVALIDOS", IntegerType(), True),            # campo 17 - NUMBER(15,0)
    StructField("INGRESO_BASE_LIQUIDACION", StringType(), True),    # campo 18 - N/A
    StructField("MESADA", DecimalType(16,2), True),                      # campo 19 - N/A
    StructField("NO_MESADAS", DecimalType(2,0), True),                 # campo 20 - NUMBER(2,0)
    StructField("RETROACTIVO", StringType(), True),                 # campo 21 - N/A
    StructField("GRADO_INVALIDEZ_INICIAL", DecimalType(5,2), True),      # campo 22 - NUMBER(5,2)
    StructField("FECHA_ULTIMA_MOD", DateType(), True),              # campo 23
    StructField("GRADO_INVALIDEZ_ACTUAL", DecimalType(5,2), True),       # campo 24 - NUMBER(5,2)
    StructField("GASTOS", DecimalType(5,4), True),                      # campo 25 - NUMBER(5,4)
    StructField("PARENT_1", IntegerType(), True),                    # campo 26 - VARCHAR2(3 BYTE)
    StructField("TIPO_IDENTIF_1", StringType(), True),              # campo 27 - VARCHAR2(3 BYTE)
    StructField("NO_IDENTIF_1", DecimalType(15,0), True),                  # campo 28 - NUMBER(15,0)
    StructField("SEXO_1", IntegerType(), True),                      # campo 29 - CHAR(1 BYTE)
    StructField("FECHA_NACIMIENTO_1", DateType(), True),           # campo 30
    StructField("NOMBRE_1", StringType(), True),                    # campo 31 - VARCHAR2(80 BYTE)
    StructField("ESTUDIOS_1", IntegerType(), True),                  # campo 32 - N/A
    StructField("ESTADO_1", StringType(), True),                    # campo 33 - VARCHAR2(3 BYTE)
    StructField("PARENT_2", IntegerType(), True),                    # campo 34 - VARCHAR2(3 BYTE)
    StructField("TIPO_IDENTIF_2", StringType(), True),              # campo 35 - VARCHAR2(3 BYTE)
    StructField("NO_IDENTIF_2", DecimalType(15,0), True),                  # campo 36 - NUMBER(15,0)
    StructField("SEXO_2", IntegerType(), True),                      # campo 37 - CHAR(1 BYTE)
    StructField("FECHA_NACIMIENTO_2", DateType(), True),           # campo 38
    StructField("NOMBRE_2", StringType(), True),                    # campo 39 - VARCHAR2(80 BYTE)
    StructField("ESTUDIOS_2", IntegerType(), True),                  # campo 40 - N/A
    StructField("ESTADO_2", StringType(), True),                    # campo 41 - VARCHAR2(3 BYTE)
    StructField("PARENT_3", IntegerType(), True),                    # campo 42 - VARCHAR2(3 BYTE)
    StructField("TIPO_IDENTIF_3", StringType(), True),              # campo 43 - VARCHAR2(3 BYTE)
    StructField("NO_IDENTIF_3", DecimalType(15,0), True),                  # campo 44 - NUMBER(15,0)
    StructField("SEXO_3", IntegerType(), True),                      # campo 45 - CHAR(1 BYTE)
    StructField("FECHA_NACIMIENTO_3", DateType(), True),           # campo 46
    StructField("NOMBRE_3", StringType(), True),                    # campo 47 - VARCHAR2(80 BYTE)
    StructField("ESTUDIOS_3", IntegerType(), True),                  # campo 48 - N/A
    StructField("ESTADO_3", StringType(), True),                    # campo 49 - VARCHAR2(3 BYTE)
    StructField("NO_RAD_NOT_TEC", DecimalType(20,0), True),               # campo 50 - NUMBER(20,0)
    StructField("INTERES_TEC_PRIMA", DecimalType(19,10), True),          # campo 51 - NUMBER(15,10)
    StructField("TASA_CREC_PRIMA", DecimalType(5,2), True),             # campo 52 - NUMBER(5,2)
    StructField("GASTOS_PRIMA", DecimalType(5,4), True),               # campo 53 - NUMBER(5,4)
    StructField("PRIMA", DecimalType(30,2), True),                        # campo 54 - NUMBER(17,2)
    StructField("FECHA_INICIAL_RENTA_DIFERIDA", StringType(), True), # campo 55 - N/A
    StructField("VALOR_MESADA_BENEF_PERIO_DIFER", StringType(), True), # campo 56 - N/A
    StructField("VALOR_AUXILIO_FUNERARIO", DecimalType(14,2), True),    # campo 57 - N/A
    StructField("OBSERVACIONES", StringType(), True),              # campo 58 - N/A
    StructField("VALOR_RESERVA_RV08", DecimalType(14,2), True),          # campo 59 - NUMBER(14,2)
    StructField("VALOR_RESERVA_RV89", DecimalType(14,2), True),          # campo 60 - NUMBER(14,2)
    StructField("PAR_0",DecimalType(28,2), True),                      # campo 61 - N/A
    StructField("PARMA", DecimalType(28,2), True),                      # campo 62 - N/A
    StructField("PORCENTAJE_EXC_PARMA", StringType(), True),      # campo 63 - N/A
    StructField("POR_AMORT_ACUM", DecimalType(34,2), True),              # campo 64 - NUMBER(5,2)
    StructField("FEC_AVISO_SINIESTRO", StringType(), True),       # campo 65 - N/A
    StructField("FAIR_VALUE", StringType(), True),                 # campo 66 - N/A
    StructField("INSCRIPCION_MECANISMO", StringType(), True),      # campo 67 - VARCHAR2(1 BYTE)
    StructField("FECHA_INSCRIPCION", DateType(), True),            # campo 68
    StructField("PARAMETRO_DESLIZAMIENTO", DecimalType(28,15), True),    # campo 69 - NUMBER(20,15)
    StructField("TASA_FAC_SEG", DecimalType(10,2), True),               # campo 70 - PARAMETERS
    StructField("VALOR_COBERTURA", DecimalType(14,2), True),             # campo 71 - NUMBER(14,2)
    StructField("FECHA_EXPED_POLIZA", DateType(), True),           # campo 72
    StructField("FECHA_MUERTE_AFILIADO", DateType(), True),        # campo 73
    StructField("INTERES_TECNICO_NIIF4", StringType(), True),      # campo 74 - NUMBER(15,10)
    StructField("UNIDAD_DE_CUENTA", StringType(), True),            # campo 75 - N/A
    StructField("PERIODO_PARTITION", StringType(), True)            # campo 76 - partition
])
#Read tables from silver prv
df_prv_rentas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_RENTAS").toDF()
df_prv_rentas_notas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_RENTAS_NOTAS").toDF()
df_prv_beneficiarios = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_BENEFICIARIOS").toDF()
df_prv_rentas_aprobadas_obp = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_RENTAS_APROBADAS_OBP").toDF()
df_prv_endosos_point = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_ENDOSOS_POINT").toDF()
df_spr_siniestros = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_SPR_SINIESTROS").toDF()
#Read tables from silver general
df_acu_personas_naturales = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ACU_PERSONAS_NATURALES").toDF()
df_cgr_equivalencia_tipos_id = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_EQUIVALENCIA_TIPOS_ID").toDF()
df_cgr_estados_personas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_ESTADOS_PERSONAS").toDF()
df_cgr_parentescos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_PARENTESCOS").toDF()
df_anp_ipc = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_IPC").toDF()
df_rva_reserva_matematica_transic = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_RVA_RESERVA_MATEMATICA_TRANSIC").toDF()
df_pgr_salarios_minimos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_PGR_SALARIOS_MINIMOS").toDF()
df_anp_pensionado_novedad = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PENSIONADO_NOVEDAD").toDF()
df_anp_distribucion_vig_pension = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_DISTRIBUCION_VIG_PENSION").toDF()
df_anp_pensiones = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PENSIONES").toDF()
hasta aca buuuuu
df_anp_pensionado_novedad_filtered = df_anp_pensionado_novedad.filter(
      (df_anp_pensionado_novedad["cod_concepto"] == '28') &
          (df_anp_pensionado_novedad["fec_ingreso"]  < to_date(lit(FEC_CORTE), "yyyy-MM-dd"))
  )

df_prv_rentas_filtered = df_prv_rentas.filter(
    (df_prv_rentas["fec_invalidacion_registro"].isNull() | (df_prv_rentas["fec_invalidacion_registro"] > to_date(lit(FEC_CORTE), "yyyy-MM-dd"))) &
        (df_prv_rentas["fec_expedicion"] <= to_date(lit(FEC_CORTE), "yyyy-MM-dd"))
)

df_rva_reserva_matematica_transic_filtered = df_rva_reserva_matematica_transic.filter(
    (df_rva_reserva_matematica_transic["ramo"] == 'RVI')
)

#Step 1  Agregar la columna "orden"

window_spec = Window.partitionBy("nid_rentas").orderBy("cod_parentesco")

df_prv_beneficiarios = df_prv_beneficiarios.withColumn("orden", row_number().over(window_spec))

df_acu_personas_naturales = df_acu_personas_naturales.withColumn(
    "edad_beneficiario",
    floor(months_between(to_date(lit(FEC_CORTE), "yyyy-MM-dd"), col("fec_nacimiento")) / 12)
) 

df_beneficiarios_filtrados = df_prv_beneficiarios.filter(
    col("cod_parentesco").isin("3", "10")
).select("nid_rentas").distinct()
anio_corte = int(FEC_CORTE.split("-")[0]) if "-" in FEC_CORTE else int(FEC_CORTE[:4])
df_acu_personas_naturales = df_acu_personas_naturales.withColumn(
    "edad_beneficiario",
    floor(months_between(to_date(lit(FEC_CORTE), "yyyy-MM-dd"), col("fec_nacimiento")) / 12)
)
#Step 1 : join prv_rentas ->  prv_rentas_notas  ->  prv_beneficiarios -> prv_rentas_aprobadas_obp -> prv_endosos_point  -> spr_siniestros
df_join_reserva_rvi = df_prv_rentas_filtered.join(
    df_prv_rentas_notas,
    df_prv_rentas_filtered["nid_rentas"] == df_prv_rentas_notas["nid_rentas"],
    "left"
)
 
df_join_reserva_rvi = df_join_reserva_rvi.join(
    df_prv_beneficiarios,
    df_prv_rentas_filtered["nid_rentas"] == df_prv_beneficiarios["nid_rentas"],
    "left"
)
 
df_join_reserva_rvi = df_join_reserva_rvi.join(
    df_prv_rentas_aprobadas_obp,
    df_prv_rentas_filtered["nid_rentas"] == df_prv_rentas_aprobadas_obp["nid_rentas"],
    "left"
)
 
df_join_reserva_rvi = df_join_reserva_rvi.join(
    df_prv_endosos_point,
    df_prv_rentas_filtered["nid_rentas"] == df_prv_endosos_point["nid_rentas"],
    "left"
)
 
df_join_reserva_rvi = df_join_reserva_rvi.join(
    df_spr_siniestros,
    df_prv_rentas_filtered["nid_rentas"] == df_spr_siniestros["nid_siniestro"],
    "left"
)
#Step  1 a join 
df_aux_funerario = df_join_reserva_rvi.join(
    df_anp_pensiones,
    df_prv_rentas_filtered["nid_rentas"] == df_anp_pensiones["nid_procedencia"],
    "left"
).join(
    df_anp_distribucion_vig_pension,
    df_anp_pensiones["nid_pension"] == df_anp_distribucion_vig_pension["nid_pension"],
    "left"
).join(
    df_anp_pensionado_novedad_filtered,
    df_anp_distribucion_vig_pension["nid_distribucion_vigencia"] == df_anp_pensionado_novedad_filtered["nid_distribucion_vigencia"],
    "left"
).groupBy(df_prv_rentas_filtered["nid_rentas"]).agg(
    coalesce(max(df_anp_pensionado_novedad_filtered["vlr_fijo"]), lit(0)).alias("vlr_aux_funerario_base")
)



df_aux_funerario = df_aux_funerario.withColumnRenamed("nid_rentas", "nid_rentas_aux")

df_join_reserva_rvi = df_join_reserva_rvi.join(
    df_aux_funerario,
    df_prv_rentas_filtered["nid_rentas"] == df_aux_funerario["nid_rentas_aux"],
    "left"
)

# Luego, marcamos en df_campos_rvi_final si ese nid_rentas tiene beneficiarios clave
df_join_reserva_rvi = df_join_reserva_rvi.join(
    df_beneficiarios_filtrados.withColumn("has_beneficiario_clave", lit(True)),
    on="nid_rentas",
    how="left"
)
#Step 2 : join df_join_reserva_rvi ->  acu_personas_naturales  ->  rva_reserva_matematica_transic  ->  cgr_estados_personas -> df_anp_pensiones -> // df_anp_distribucion_vig_pension ->  df_anp_pensionado_novedad  -> // cgr_equivalencia_tipos_id  ->  cgr_parentescos

df_join_reserva_rvi_general = df_join_reserva_rvi.join(
    df_acu_personas_naturales,
    (df_prv_rentas_filtered["cod_tipo_identificacion"] == df_acu_personas_naturales["cod_tipo_identificacion"]) &
    (df_prv_rentas_filtered["nid_numero_identificacion"] == df_acu_personas_naturales["nid_numero_identificacion"]),
    "left"
)


df_join_reserva_rvi_general = df_join_reserva_rvi.join(
    df_acu_personas_naturales,
    (df_prv_beneficiarios["cod_tipo_identificacion"] == df_acu_personas_naturales["cod_tipo_identificacion"]) &
    (df_prv_beneficiarios["nid_numero_identificacion"] == df_acu_personas_naturales["nid_numero_identificacion"]),
    "left"
)


df_join_reserva_rvi_general = df_join_reserva_rvi_general.join(
    broadcast(df_rva_reserva_matematica_transic_filtered),
    df_prv_rentas_filtered["nid_rentas"] == df_rva_reserva_matematica_transic_filtered["nid_siniestro_renta"],
    "left"
)

df_cgr_parentescos = df_cgr_parentescos.withColumnRenamed("cod_parentesco", "cod_parentesco_aux")

df_join_reserva_rvi_general = df_join_reserva_rvi_general.join(
    broadcast(df_cgr_parentescos),
    df_prv_beneficiarios["cod_parentesco"] == df_cgr_parentescos["cod_parentesco_aux"],
    "left"
)
 
df_join_reserva_rvi_general = df_join_reserva_rvi_general.join(
    broadcast(df_cgr_equivalencia_tipos_id),
    df_prv_beneficiarios["cod_tipo_identificacion"] == df_cgr_equivalencia_tipos_id["cod_tipo_documento"],
    "left"
)
 
df_join_reserva_rvi_general = df_join_reserva_rvi_general.join(
    broadcast(df_cgr_estados_personas),
    df_prv_rentas_filtered["cod_estado_persona"] == df_cgr_estados_personas["cod_estado_persona"],
    "left"
)
# Asignar valor a mesada

df_salarios_expedicion = df_pgr_salarios_minimos.alias("exp")
df_salarios_corte = df_pgr_salarios_minimos.alias("corte")

df_join_reserva_rvi_general = df_join_reserva_rvi_general.withColumn("anio_expedicion",year(df_prv_rentas_filtered["fec_expedicion"]))

df_join_reserva_rvi_general = df_join_reserva_rvi_general.join(
    broadcast(df_salarios_expedicion),
       df_join_reserva_rvi_general["anio_expedicion"] == df_salarios_expedicion["ano"],
       "left"
   )
df_join_reserva_rvi_general = df_join_reserva_rvi_general.join(
       broadcast(df_salarios_corte),
       (lit(anio_corte) == df_salarios_corte["ano"]) & 
       (df_salarios_corte["mes"] == 1),  
      "left"
   )

df_join_reserva_rvi_general = df_join_reserva_rvi_general.withColumn(
    "anio_anterior_corte",
    lit(anio_corte - 1)
)

df_renta_ipc = df_join_reserva_rvi_general.withColumn(
    "anio_ipc",
    explode(
        sequence(
            col("anio_expedicion").cast("int"),
            col("anio_anterior_corte").cast("int")
        )
    )
)

df_renta_ipc = df_renta_ipc.join(
    df_anp_ipc.select(col("ANIO").alias("anio_ipc"), col("PCJ_IPC").alias("pcj_ipc_anp")),
    on="anio_ipc",
    how="left"
)

df_renta_ipc = df_renta_ipc.withColumn("log_factor", log(1 + (col("pcj_ipc_anp") / 100)))

df_factor = df_renta_ipc.groupBy(df_prv_rentas_filtered["nid_rentas"]).agg(
    exp(sum_("log_factor")).alias("factor_incremento")
)

df_join_reserva_rvi_general = df_join_reserva_rvi_general.join(
    df_factor,
    on="nid_rentas",
    how="left"
)

df_join_reserva_rvi_general = df_join_reserva_rvi_general.withColumn(
    "mesada",
    when(
        col("vlr_pension_inicial") <= col("exp.vlr_salario_minimo"),
        col("corte.vlr_salario_minimo")
    ).when(
        (col("vlr_pension_inicial") > col("exp.vlr_salario_minimo")) &
        ((col("vlr_pension_inicial") * col("factor_incremento")) < col("corte.vlr_salario_minimo")),
        col("corte.vlr_salario_minimo")
    ).otherwise(
        round(col("vlr_pension_inicial") * col("factor_incremento"), 2)
    )
)
#campo 16
df_hijos = df_prv_beneficiarios.filter(
    (df_prv_beneficiarios["cod_parentesco"] == "4") & ((df_prv_beneficiarios["fec_aparicion"] < to_date(lit(FEC_CORTE), "yyyy-MM-dd")) | df_prv_beneficiarios["fec_aparicion"].isNull()) &
    (((df_prv_beneficiarios["bln_con_derecho"] == "S") & (df_prv_beneficiarios["bln_es_beneficiario"] == "S")) |(df_prv_beneficiarios["fec_muerte"] > to_date(lit(FEC_CORTE), "yyyy-MM-dd")) )
    ).groupBy("nid_rentas").agg(
        when(countDistinct("nid_numero_identificacion") == 0, None).otherwise(countDistinct("nid_numero_identificacion")).alias("NO_HIJOS"))
        
#campo 17
df_hijos_invalidos = df_prv_beneficiarios.filter(
    (df_prv_beneficiarios["cod_parentesco"] == "4") & (df_prv_beneficiarios["bln_con_derecho"] == "S") & (df_prv_beneficiarios["cod_estado_persona"] == "2")
    ).groupBy("nid_rentas").agg(
        when(countDistinct("nid_numero_identificacion") == 0, None).otherwise(countDistinct("nid_numero_identificacion")).alias("NO_HIJOS_INVALIDOS"))
        
#campo 52
df_tasa_crec_prima = df_prv_rentas_filtered.withColumn("anio_expedicion", year(col("fec_expedicion")))

df_tasa_crec_prima = df_tasa_crec_prima.join(
    df_anp_ipc.select(col("ANIO"), col("PCJ_IPC").alias("pcj_ipc_prima")),
    df_tasa_crec_prima["anio_expedicion"] == df_anp_ipc["anio"],
    "left"
)

#campo 53
df_sum_prima = df_join_reserva_rvi_general.groupBy(df_prv_rentas_filtered["nid_rentas"]).agg(
    _sum("vlr_prima").alias("PRIMA")
)

#campo 7
df_campos_rvi_final = df_join_reserva_rvi_general.withColumn("ORIGEN_PENSION",
    when(df_acu_personas_naturales["fec_muerte"].isNull(),
        when(df_prv_rentas_filtered["nid_rentas"].isNotNull(),
            when(df_spr_siniestros["cod_estado_siniestro"] == "4", 3)
            .otherwise(1)
        ).when((df_prv_rentas_filtered["nid_rentas"].isNull()) & (df_prv_rentas_filtered["cod_origen"] == "3"), 3)
         .when((df_prv_rentas_filtered["nid_rentas"].isNull()) & (df_prv_rentas_filtered["cod_afp"] != "1") & (df_prv_rentas_filtered["cod_origen"] == "2"), 1)
         .when((df_prv_rentas_filtered["nid_rentas"].isNull()) & (df_prv_rentas_filtered["cod_origen"] == "2"), 1)
    ).otherwise(
        when(df_prv_rentas_filtered["cod_origen"] == "1",
            when((date_format(df_acu_personas_naturales["fec_muerte"], "yyyyMMdd").cast("long") >= date_format(df_prv_rentas_filtered["fec_expedicion"], "yyyyMMdd").cast("long")) &
                 df_prv_rentas_filtered["nid_rentas"].isNull(), 5)
            .when(date_format(df_acu_personas_naturales["fec_muerte"], "yyyyMMdd").cast("long") < date_format(df_prv_rentas_filtered["fec_expedicion"], "yyyyMMdd").cast("long"), 2)
            .when((date_format(df_acu_personas_naturales["fec_muerte"], "yyyyMMdd").cast("long") >= date_format(df_prv_rentas_filtered["fec_expedicion"], "yyyyMMdd").cast("long")) &
                  df_prv_rentas_filtered["nid_rentas"].isNotNull(), 4)
        ).when(df_prv_rentas_filtered["cod_origen"] == "2", 1)
         .when(df_prv_rentas_filtered["cod_origen"] == "3", 3)
    )
)
#campo 11
df_campos_rvi_final = df_campos_rvi_final.withColumn("SEXO",
    when(df_acu_personas_naturales["genero"] == "M", 1).when(df_acu_personas_naturales["genero"] == "F", 2)
)

#campo 13
df_campos_rvi_final = df_campos_rvi_final.withColumn("NOMBRE",
    substring(trim(concat_ws(" ",trim(df_acu_personas_naturales["primer_apellido"]),trim(df_acu_personas_naturales["segundo_apellido"]),trim(df_acu_personas_naturales["primer_nombre"]),trim(df_acu_personas_naturales["segundo_nombre"]))),1, 30)
)

#campo 16 
df_campos_rvi_final = df_campos_rvi_final.join(
    df_hijos,
    df_prv_rentas_filtered["NID_RENTAS"] == df_hijos["nid_rentas"],
    "left"
).drop(df_hijos["nid_rentas"])

#campo 17
df_campos_rvi_final = df_campos_rvi_final.join(
    df_hijos_invalidos,
    df_prv_rentas_filtered["NID_RENTAS"] == df_hijos_invalidos["nid_rentas"],
    "left"
).drop(df_hijos_invalidos["nid_rentas"])

#campo 27
df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "PARENT_1",
    when(df_cgr_parentescos["cod_parentesco_aux"] == "1", 5)
    .when(df_cgr_parentescos["cod_parentesco_aux"] == "2", 6)
    .when(df_cgr_parentescos["cod_parentesco_aux"] == "3", 4)
    .when(df_cgr_parentescos["cod_parentesco_aux"] == "4", 3)
    .when(df_cgr_parentescos["cod_parentesco_aux"] == "6", 8)
)

#campo 32
df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "ESTUDIOS_1",
    when((df_cgr_parentescos["cod_parentesco_aux"] == "4") &(df_prv_beneficiarios["cod_estado_persona"] == "1") & (months_between(to_date(lit(FEC_CORTE), "yyyy-MM-dd"), df_acu_personas_naturales["fec_nacimiento"]) / 12 >= 18), 1 )
)

#campo 51
df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "INTERES_TEC_PRIMA",
    when(df_prv_rentas_filtered["fec_expedicion"] < to_date(lit(FEC_CORTE), "yyyy-MM-dd"), df_prv_rentas_filtered["pcj_interes_tecnico"]
    ).otherwise(df_prv_rentas_filtered["pcj_interes_reserva"] * 100 )
)

#campo 52 
df_campos_rvi_final = df_campos_rvi_final.join(
    df_tasa_crec_prima.select("nid_rentas", "pcj_ipc_prima"), 
    on="nid_rentas",
    how="left"
).withColumn("TASA_CREC_PRIMA", df_tasa_crec_prima["pcj_ipc_prima"]).drop("pcj_ipc")

#campo 53
df_campos_rvi_final = df_campos_rvi_final.join(
    df_sum_prima,
    on="nid_rentas",
    how="left"
)
#campo 61
df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "PAR_0",
    when(
        df_rva_reserva_matematica_transic_filtered["nid_siniestro_renta"].isNotNull(),
        round((df_rva_reserva_matematica_transic_filtered["vlr_reserva_rv89"] / df_rva_reserva_matematica_transic_filtered["vlr_reserva_rv08"]) * 100, 2)
    ).otherwise(None)
)
#campo 62
df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "PARMA",
    when(
        df_rva_reserva_matematica_transic_filtered["nid_siniestro_renta"].isNotNull(),
        round(((100 - ((df_rva_reserva_matematica_transic_filtered["vlr_reserva_rv89"] / df_rva_reserva_matematica_transic_filtered["vlr_reserva_rv08"]) * 100)) / 240), 2)
    ).otherwise(None)
)
#campo 64
df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "POR_AMORT_ACUM",
    when(
        (col("PAR_0") +  col("PARMA") * (months_between(to_date(lit(FEC_CORTE), "yyyyMM"), to_date(lit("201804"), "yyyyMM")) + 91)) > 100, 100)
        .otherwise(col("PAR_0") + col("PARMA") * (months_between(to_date(lit(FEC_CORTE), "yyyyMM"), to_date(lit("201804"), "yyyyMM")) + 91)
    )
)
#campo 67
df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "INSCRIPCION_MECANISMO", 
    coalesce(df_prv_rentas_aprobadas_obp["inscripcion_mecanismo"], lit("0"))
)
#campo 69
df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "PARAMETRO_DESLIZAMIENTO", 
    df_prv_rentas_filtered["vlr_exceso_smmlv_ipc_exp"] * 100
)

# auxilios Funerarios

salario_minimo_actual = df_pgr_salarios_minimos.filter(
    (col("ano") == anio_corte) & 
    (col("mes") == 1) 
).select("vlr_salario_minimo").first()[0]


df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "VALOR_AUXILIO_FUNERARIO",
    when(
        col("ORIGEN_PENSION").isin(1, 3),
        when(
            col("mesada") <= (5 * salario_minimo_actual),
            round(lit(5 * salario_minimo_actual), 2)
        ).when(
            (col("mesada") > (5 * salario_minimo_actual)) & 
            (col("mesada") <= (10 * salario_minimo_actual)),
            round(col("mesada"), 2)
        ).otherwise(
            round(lit(10 * salario_minimo_actual), 2)
        )
    ).otherwise(
        col("vlr_aux_funerario_base")
    )
)
# observaciones

# Finalmente, creamos la columna clase_pension usando las reglas
df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "clase_pension",
    when(col("cod_origen").isin(2, 3), lit(1))
    .when(col("has_beneficiario_clave") == True, lit(1))
    .otherwise(lit(3))
)

df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "tiempoTemporalidad",
    when((col("clase_pension") == 3) & 
         (col("cod_parentesco_aux") == 3) & 
         (col("cod_parentesco_aux") == 3),
         when(col("fec_nacimiento") > col("fec_nacimiento"),
              (25 - col("edad_beneficiario") + 1) * 12
         ).otherwise(
              (25 - col("edad_beneficiario") + 1) * 12
         )
    ).when((col("clase_pension") == 3) & 
           (col("cod_parentesco_aux") == 3) & 
           col("cod_parentesco_aux").isNull(),
           (25 - col("edad_beneficiario") + 1) * 12
    ).when((col("clase_pension") == 3) & (
           ((col("cod_parentesco_aux") == 7) & (col("cod_parentesco_aux") == 7)) |
           ((col("cod_parentesco_aux") == 7) & col("cod_parentesco_aux").isNull())),
        when((year(to_date(lit(FEC_CORTE), "yyyy-MM-dd")) - year(df_acu_personas_naturales["fec_muerte"])) == 20,
             floor(240 - months_between(to_date(lit(FEC_CORTE), "yyyy-MM-dd"), df_acu_personas_naturales["fec_muerte"]))
        ).otherwise(
             (20 - (year(to_date(lit(FEC_CORTE), "yyyy-MM-dd")) - year(df_acu_personas_naturales["fec_muerte"]))) * 12
        )
    ).when((col("clase_pension") == 3) & (
           ((col("cod_parentesco_aux") == 3) & (col("cod_parentesco_aux") == 7)) |
           ((col("cod_parentesco_aux") == 7) & (col("cod_parentesco_aux") == 3))),
        when(col("cod_parentesco_aux") == 3,
             (25 - col("edad_beneficiario") + 1) * 12
        ).otherwise(
             (25 - col("edad_beneficiario") + 1) * 12
        ).alias("temporalidad1")
    )
)


df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "temporalidad1",
    when((col("clase_pension") == 3) &
         (((col("cod_parentesco_aux") == 3) & (col("cod_parentesco_aux") == 7)) |
          ((col("cod_parentesco_aux") == 7) & (col("cod_parentesco_aux") == 3))),
         when(col("cod_parentesco_aux") == 3,
              (25 - col("edad_beneficiario") + 1) * 12
         ).otherwise(
              (25 - col("edad_beneficiario") + 1) * 12
         )
    )
)


df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "temporalidad2",
    when((col("clase_pension") == 3) &
         (((df_cgr_parentescos["cod_parentesco_aux"] == 3) & (df_cgr_parentescos["cod_parentesco_aux"] == 7)) |
          ((df_cgr_parentescos["cod_parentesco_aux"] == 7) & (df_cgr_parentescos["cod_parentesco_aux"] == 3))),
         (20 - (year(to_date(lit(FEC_CORTE), "yyyy-MM-dd")) - year(df_acu_personas_naturales["fec_muerte"]))) * 12
    )
)

df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "tiempoTemporalidad",
    when((col("clase_pension") == 3) &
         (((col("cod_parentesco_aux") == 3) & (col("cod_parentesco_aux") == 7)) |
          ((col("cod_parentesco_aux") == 7) & (col("cod_parentesco_aux") == 3))),
         greatest(col("temporalidad1"), col("temporalidad2"))
    ).otherwise(col("tiempoTemporalidad"))  
)

df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "observaciones",
    when(
        df_acu_personas_naturales["fec_muerte"].isNotNull() & col("PARAMETRO_DESLIZAMIENTO").isNotNull(),
        concat(lit("FMA "), df_acu_personas_naturales["fec_muerte"], lit(" DESLIZAMIENT 0"), col("PARAMETRO_DESLIZAMIENTO"))
    ).when(
        df_acu_personas_naturales["fec_muerte"].isNotNull(),
        concat(lit("FMA "), df_acu_personas_naturales["fec_muerte"])
    ).when(
        col("PARAMETRO_DESLIZAMIENTO").isNotNull(),
        concat(lit("DESLIZAMIENT 0"), col("PARAMETRO_DESLIZAMIENTO"))
    )
)
df_campos_rvi_final.filter((df_prv_rentas_filtered["nid_rentas"] == '153')).show()
# Pivot implícito (como en tu SQL)
df_resultados_beneficiario = df_campos_rvi_final.groupBy(df_prv_rentas_filtered["nid_rentas"]).agg(
	max(when(df_prv_beneficiarios["orden"] == 1, df_prv_beneficiarios["cod_estado_persona"])).alias("ESTADO_1"),
	max(when(df_prv_beneficiarios["orden"] == 1, df_campos_rvi_final["ESTUDIOS_1"])).alias("ESTUDIOS_1"),
	max(when(df_prv_beneficiarios["orden"] == 1, df_acu_personas_naturales["fec_nacimiento"])).alias("FECHA_NACIMIENTO_1"),
    max(when(df_prv_beneficiarios["orden"] == 1, df_cgr_equivalencia_tipos_id["cod_tipo_super"])).alias("TIPO_IDENTIF_1"),
    max(when(df_prv_beneficiarios["orden"] == 1, df_prv_beneficiarios["nid_numero_identificacion"])).alias("NO_IDENTIF_1"),
    max(when(df_prv_beneficiarios["orden"] == 1, df_campos_rvi_final["NOMBRE"])).alias("NOMBRE_1"),
	max(when(df_prv_beneficiarios["orden"] == 1, df_campos_rvi_final["PARENT_1"])).alias("PARENT_1"),
	max(when(df_prv_beneficiarios["orden"] == 1, df_campos_rvi_final["SEXO"])).alias("SEXO_1"),
	max(when(df_prv_beneficiarios["orden"] == 2, df_prv_beneficiarios["cod_estado_persona"])).alias("ESTADO_2"),
	max(when(df_prv_beneficiarios["orden"] == 2, df_campos_rvi_final["ESTUDIOS_1"])).alias("ESTUDIOS_2"),
	max(when(df_prv_beneficiarios["orden"] == 2, df_acu_personas_naturales["fec_nacimiento"])).alias("FECHA_NACIMIENTO_2"),
    max(when(df_prv_beneficiarios["orden"] == 2, df_cgr_equivalencia_tipos_id["cod_tipo_super"])).alias("TIPO_IDENTIF_2"),
    max(when(df_prv_beneficiarios["orden"] == 2, df_prv_beneficiarios["nid_numero_identificacion"])).alias("NO_IDENTIF_2"),
    max(when(df_prv_beneficiarios["orden"] == 2, df_campos_rvi_final["NOMBRE"])).alias("NOMBRE_2"),
	max(when(df_prv_beneficiarios["orden"] == 2, df_campos_rvi_final["PARENT_1"])).alias("PARENT_2"),
	max(when(df_prv_beneficiarios["orden"] == 2, df_campos_rvi_final["SEXO"])).alias("SEXO_2"),
	max(when(df_prv_beneficiarios["orden"] == 3, df_prv_beneficiarios["cod_estado_persona"])).alias("ESTADO_3"),
	max(when(df_prv_beneficiarios["orden"] == 3, df_campos_rvi_final["ESTUDIOS_1"])).alias("ESTUDIOS_3"),
	max(when(df_prv_beneficiarios["orden"] == 3, df_acu_personas_naturales["fec_nacimiento"])).alias("FECHA_NACIMIENTO_3"),
    max(when(df_prv_beneficiarios["orden"] == 3, df_cgr_equivalencia_tipos_id["cod_tipo_super"])).alias("TIPO_IDENTIF_3"),
    max(when(df_prv_beneficiarios["orden"] == 3, df_prv_beneficiarios["nid_numero_identificacion"])).alias("NO_IDENTIF_3"),
    max(when(df_prv_beneficiarios["orden"] == 3, df_campos_rvi_final["NOMBRE"])).alias("NOMBRE_3"),
	max(when(df_prv_beneficiarios["orden"] == 3, df_campos_rvi_final["PARENT_1"])).alias("PARENT_3"),
	max(when(df_prv_beneficiarios["orden"] == 3, df_campos_rvi_final["SEXO"])).alias("SEXO_3")
    )


df_resultados_beneficiario = df_resultados_beneficiario.withColumnRenamed("nid_rentas", "nid_rentas_orden")
df_campos_rvi_final = df_campos_rvi_final.filter(df_prv_beneficiarios["orden"] == 1)
df_resultados_beneficiario.filter((df_resultados_beneficiario["nid_rentas_orden"] == '153')).show()
df_campos_rvi_final = df_campos_rvi_final.join(
    df_resultados_beneficiario,
    df_prv_rentas_filtered["nid_rentas"] == df_resultados_beneficiario["nid_rentas_orden"],
    "left"
)
df_campos_rvi_final.filter((df_prv_rentas_filtered["nid_rentas"] == '153')).show()
df_campos_rvi_final = df_campos_rvi_final.select(
    #df_prv_rentas_filtered["nid_rentas"],  #
    df_prv_rentas_filtered["nid_rentas"].alias("NO_POLIZA"),  #campo 3 *trm
    df_prv_rentas_filtered["fec_expedicion"].cast(DateType()).alias("VIG_INICIAL"),  #campo 4
    df_prv_rentas_filtered["nid_rentas"].alias("NO_SINIESTRO"),  #campo 5
    df_prv_rentas_filtered["fec_causacion"].cast(DateType()).alias("FECHA_CAUSACION_DERECHO"),  #campo 6
    df_prv_rentas_filtered["nid_numero_identificacion"].alias("NO_IDENTIF"),  #campo 10
    df_prv_rentas_filtered["num_mesadas"].alias("NO_MESADAS"),  #campo 20
    df_prv_rentas_filtered["pcj_pcl_inicial"].alias("GRADO_INVALIDEZ_INICIAL"),  #campo 22
    df_prv_rentas_filtered["fec_ultima_mod"].cast(DateType()).alias("FECHA_ULTIMA_MOD"),  #campo 23
    df_prv_rentas_filtered["pcj_pcl_actual"].alias("GRADO_INVALIDEZ_ACTUAL"),  #campo 24
    df_prv_rentas_filtered["pc_ad_exp"].alias("GASTOS"),  #campo 25
    df_prv_rentas_filtered["pc_ad_exp"].alias("GASTOS_PRIMA"),  #campo 53
    #df_prv_rentas_filtered["vlr_exceso_smmlv_ipc_exp"].alias("PARAMETRO_DESLIZAMIENTO"),  #campo 69
    df_campos_rvi_final["PARAMETRO_DESLIZAMIENTO"].alias("PARAMETRO_DESLIZAMIENTO"),
    df_prv_rentas_filtered["fec_expedicion"].cast(DateType()).alias("FECHA_EXPED_POLIZA"),  #campo 72
    df_prv_rentas_notas["COD_RAMO_SFC"].cast(IntegerType()).alias("RAMO"), #campo 1
    df_prv_rentas_notas["NUM_RADICADO_NOTA"].alias("NO_RAD_NOT_TEC"), #campo 50
    #df_prv_rentas_aprobadas_obp["inscripcion_mecanismo"].alias("INSCRIPCION_MECANISMO"), #campo 67 *trm
    df_campos_rvi_final["INSCRIPCION_MECANISMO"].alias("INSCRIPCION_MECANISMO"),
    df_prv_rentas_aprobadas_obp["fecha_inscripcion"].cast(DateType()).alias("FECHA_INSCRIPCION"), #campo 68
    df_prv_rentas_aprobadas_obp["cobertura"].alias("VALOR_COBERTURA"), #campo 71
    #df_prv_endosos_point["vlr_prima"].alias("PRIMA"), #campo 54 *trm
    df_campos_rvi_final["PRIMA"].alias("PRIMA"),
    df_resultados_beneficiario["TIPO_IDENTIF_1"].alias("TIPO_IDENTIF_1"), #campo  27
    df_resultados_beneficiario["NO_IDENTIF_1"].alias("NO_IDENTIF_1"), #campo 28
    df_resultados_beneficiario["ESTADO_1"].alias("ESTADO_1"), #campo 33 
    df_resultados_beneficiario["TIPO_IDENTIF_2"].alias("TIPO_IDENTIF_2"), #campo 35
    df_resultados_beneficiario["NO_IDENTIF_2"].alias("NO_IDENTIF_2"), #campo 36 
    df_resultados_beneficiario["ESTADO_2"].alias("ESTADO_2"), #campo 41
    df_resultados_beneficiario["TIPO_IDENTIF_3"].alias("TIPO_IDENTIF_3"), #campo 43
    df_resultados_beneficiario["NO_IDENTIF_3"].alias("NO_IDENTIF_3"), #campo 44
    df_resultados_beneficiario["ESTADO_3"].alias("ESTADO_3"), #campo 49
    df_acu_personas_naturales["fec_nacimiento"].cast(DateType()).alias("FECHA_NACIMIENTO"), #campo 12
    df_resultados_beneficiario["FECHA_NACIMIENTO_1"].cast(DateType()).alias("FECHA_NACIMIENTO_1"), #campo 30
    df_resultados_beneficiario["FECHA_NACIMIENTO_2"].cast(DateType()).alias("FECHA_NACIMIENTO_2"), #campo 38
    df_resultados_beneficiario["FECHA_NACIMIENTO_3"].cast(DateType()).alias("FECHA_NACIMIENTO_3"), #campo 46
    df_acu_personas_naturales["fec_muerte"].cast(DateType()).alias("FECHA_MUERTE_AFILIADO"), #campo 73
    df_cgr_equivalencia_tipos_id["cod_tipo_super"].alias("TIPO_IDENTIF"), #campo 9
    df_prv_rentas_filtered["cod_estado_persona"].alias("ESTADO"), #campo 15
    df_rva_reserva_matematica_transic_filtered["vlr_reserva_rv08"].alias("VALOR_RESERVA_RV08"), #campo 59
    df_rva_reserva_matematica_transic_filtered["vlr_reserva_rv89"].alias("VALOR_RESERVA_RV89"), #campo 60
    #df_prv_rentas_filtered["pcj_interes_tecnico"].alias("INTERES_TEC_PRIMA"), # campos 51 *trm case 
    df_campos_rvi_final["INTERES_TEC_PRIMA"].alias("INTERES_TEC_PRIMA"),
    #df_prv_rentas_filtered["cod_origen"].alias("ORIGEN_PENSION"), # campos 7 *trm case 
    df_campos_rvi_final["ORIGEN_PENSION"].alias("ORIGEN_PENSION"),
    #df_prv_beneficiarios["nid_numero_identificacion"].alias("NO_HIJOS"), # campos 16 *trm case 
    df_campos_rvi_final["NO_HIJOS"].alias("NO_HIJOS"),
    #df_prv_beneficiarios["nid_numero_identificacion"].alias("NO_HIJOS_INVALIDOS"), # campos 17 *trm case 
    df_campos_rvi_final["NO_HIJOS_INVALIDOS"].alias("NO_HIJOS_INVALIDOS"),
    #df_prv_beneficiarios["cod_parentesco"].alias("PARENT_1"), # campos 26 *trm case
    df_resultados_beneficiario["PARENT_1"].alias("PARENT_1"), # campos 26 *trm case 
    df_resultados_beneficiario["PARENT_2"].alias("PARENT_2"), # campos 34 *trm case 
    df_resultados_beneficiario["PARENT_3"].alias("PARENT_3"), # campos 42 *trm case 
    #df_acu_personas_naturales["genero"].alias("SEXO"), # campos 11 *trm case 
    df_campos_rvi_final["SEXO"].alias("SEXO"),
    df_resultados_beneficiario["SEXO_1"].alias("SEXO_1"), # campos 29 *trm case 
    df_resultados_beneficiario["SEXO_2"].alias("SEXO_2"), # campos 37 *trm case 
    df_resultados_beneficiario["SEXO_3"].alias("SEXO_3"), # campos 45 *trm case 
    #df_prv_beneficiarios["cod_estado_persona"].alias("ESTUDIOS_1"), # campos 32 *trm case 
    df_resultados_beneficiario["ESTUDIOS_1"].alias("ESTUDIOS_1"),
    df_resultados_beneficiario["ESTUDIOS_2"].alias("ESTUDIOS_2"), # campos 40 *trm case 
    df_resultados_beneficiario["ESTUDIOS_3"].alias("ESTUDIOS_3"), # campos 48 *trm case 
    df_campos_rvi_final["observaciones"].cast(StringType()).alias("OBSERVACIONES"), # campos 58 *trm case 
    #df_prv_rentas_filtered["nid_rentas"].alias("PAR_0"), # campos 61 *trm case 
    df_campos_rvi_final["PAR_0"].alias("PAR_0"),
    #df_rva_reserva_matematica_transic["vlr_reserva_rv89"].alias("PARMA"), # campos 62 *trm case 
    df_campos_rvi_final["PARMA"].alias("PARMA"),
    #df_rva_reserva_matematica_transic["vlr_reserva_rv89"].alias("POR_AMORT_ACUM"), # campos 63 *trm case
    df_campos_rvi_final["POR_AMORT_ACUM"].alias("POR_AMORT_ACUM"),
    #df_acu_personas_naturales["PRIMER_NOMBRE"].alias("NOMBRE"), # campos 13 *trm  regexp_replace
    df_campos_rvi_final["NOMBRE"].alias("NOMBRE"),
    df_resultados_beneficiario["NOMBRE_1"].alias("NOMBRE_1"), # campos 31 *trm  regexp_replace
    df_resultados_beneficiario["NOMBRE_2"].alias("NOMBRE_2"), # campos 39 *trm  regexp_replace
    df_resultados_beneficiario["NOMBRE_3"].alias("NOMBRE_3"), # campos 47 *trm  regexp_replace
    #df_prv_rentas_filtered["nid_rentas"].alias("TASA_CREC_PRIMA"), # campos 52 *trm  subquery
    #df_campos_rvi_final["TASA_CREC_PRIMA"].alias("TASA_CREC_PRIMA"),
    df_campos_rvi_final["mesada"].alias("MESADA"), # campos 19 *trm  validar query
    df_campos_rvi_final["VALOR_AUXILIO_FUNERARIO"].alias("VALOR_AUXILIO_FUNERARIO") # campos 57 *trm     
)
    
df_campos_rvi_final.filter((df_prv_rentas_filtered["nid_rentas"] == '153')).show()
#Add cols UNIDAD, PARENT, ESTUDIOS, INGRESO_BASE_LIQUIDACION, RETROACTIVO, FECHA_INICIAL_RENTA_DIFERIDA, VALOR_MESADA_BENEF_PERIO_DIFER , PORCENTAJE_EXC_PARMA, FEC_AVISO_SINIESTRO, UNIDAD_DE_CUENTA
# campo 2 , # campo 8, # campo 14, # campo 18, # campo 21, # campo 55, # campo 56, # campo 63, # campo 65, # campo 75
df_campos_rvi_final = df_campos_rvi_final.withColumn("UNIDAD", lit(1))\
    .withColumn("PARENT", lit(1))\
    .withColumn("ESTUDIOS", lit(None).cast(StringType()))\
    .withColumn("INGRESO_BASE_LIQUIDACION", lit(None).cast(StringType()))\
    .withColumn("RETROACTIVO", lit(None).cast(StringType()))\
    .withColumn("FECHA_INICIAL_RENTA_DIFERIDA", lit(None).cast(StringType()))\
    .withColumn("VALOR_MESADA_BENEF_PERIO_DIFER", lit(None).cast(StringType()))\
    .withColumn("PORCENTAJE_EXC_PARMA", lit(None).cast(StringType()))\
    .withColumn("FEC_AVISO_SINIESTRO", lit(None).cast(StringType()))\
    .withColumn("UNIDAD_DE_CUENTA", lit(None).cast(StringType()))\
    .withColumn("FAIR_VALUE", lit(None).cast(StringType()))\
    .withColumn("INTERES_TECNICO_NIIF4", lit(None).cast(StringType()))
df_campos_rvi_final = df_campos_rvi_final.withColumn("PARAMETRO_DESLIZAMIENTO", lit(1.0).cast(DecimalType(28,15)))
    #Add partition to table 
    df_campos_rvi_final = df_campos_rvi_final.withColumn("PERIODO_PARTITION", date_format(df_campos_rvi_final["VIG_INICIAL"], "yyyy-MM"))
    
    #-----------------------------------------------------------------#
    #--------------------- AssigN Struct -----------------------------#
    #-----------------------------------------------------------------#
  
    #Select final dataframe for gold stage
    #Load df_reserva_rvi_gold with structType and data
    column_names = [field.name for field in schema_reserva_rvi_gold]
    df_campos_rvi_final = df_campos_rvi_final.select(*column_names)
    
    logger.info(f"End df")
    
    df_reserva_rvi_gold = spark.createDataFrame(df_campos_rvi_final.rdd, schema=schema_reserva_rvi_gold)
"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim, date_format, to_date, lit, min, max, when, first, count, substring, concat_ws, floor, months_between, year, round, least, trunc, round as spark_round, coalesce, concat, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, DoubleType, DecimalType
from pyspark.sql.window import Window

import sys, traceback, boto3, json


BUCKET_NAME = 'alfa-datalake-dev-gold'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
DB_NAME = 'ALFAPRD'
DB_SCHEMA = 'PRV'
PERIOD = '202501'
CERTIFICATE = 'False'
DB_CATALOG_GLUE = 'datalake-dev-silver-catalog-db'
TABLE_NAME_GOLD = 'RESERVA_RVI_GOLD'
FEC_CORTE = '2003-12-29'
FEC_CORTE_DATE = '2024-11-07'

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")


schema_reserva_arp_gold = StructType([
            StructField("RAMO", IntegerType(), True),                         # campo 1 - VARCHAR2(3 BYTE)
            StructField("UNIDAD", IntegerType(), True),                       # campo 2 - N/A
            StructField("NO_POLIZA", DecimalType(10,0), True),                   # campo 3 - NUMBER(10,0)
            StructField("VIG_INICIAL", DateType(), True),                    # campo 4
            StructField("NO_SINIESTRO", DecimalType(10,0), True),               # campo 5 - NUMBER(10,0)
            StructField("FECHA_CAUSACION_DERECHO", DateType(), True),       # campo 6
            StructField("ORIGEN_PENSION", IntegerType(), True),              # campo 7 - VARCHAR2(3 BYTE)
            StructField("PARENT", IntegerType(), True),                       # campo 8 - N/A
            StructField("TIPO_IDENTIF", StringType(), True),                # campo 9 - VARCHAR2(2 BYTE)
            StructField("NO_IDENTIF", DecimalType(15,0), True),                     # campo 10 - NUMBER(15,0)
            StructField("SEXO", IntegerType(), True),                         # campo 11 - CHAR(1 BYTE)
            StructField("FECHA_NACIMIENTO", DateType(), True),              # campo 12
            StructField("NOMBRE", StringType(), True),                      # campo 13 - VARCHAR2(80 BYTE)
            StructField("ESTUDIOS", StringType(), True),                    # campo 14 - N/A
            StructField("ESTADO", StringType(), True),                      # campo 15 - VARCHAR2(3 BYTE)
            StructField("NO_HIJOS", IntegerType(), True),                      # campo 16 - NUMBER(15,0)
            StructField("NO_HIJOS_INVALIDOS", IntegerType(), True),            # campo 17 - NUMBER(15,0)
            StructField("INGRESO_BASE_LIQUIDACION", StringType(), True),    # campo 18 - N/A
            StructField("MESADA", DecimalType(16,2), True),                      # campo 19 - N/A
            StructField("NO_MESADAS", DecimalType(2,0), True),                 # campo 20 - NUMBER(2,0)
            StructField("RETROACTIVO", StringType(), True),                 # campo 21 - N/A
            StructField("GRADO_INVALIDEZ_INICIAL", DecimalType(5,2), True),      # campo 22 - NUMBER(5,2)
            StructField("FECHA_ULTIMA_MOD", DateType(), True),              # campo 23
            StructField("GRADO_INVALIDEZ_ACTUAL", DecimalType(5,2), True),       # campo 24 - NUMBER(5,2)
            StructField("GASTOS", DecimalType(5,4), True),                      # campo 25 - NUMBER(5,4)
            StructField("PARENT_1", IntegerType(), True),                    # campo 26 - VARCHAR2(3 BYTE)
            StructField("TIPO_IDENTIF_1", StringType(), True),              # campo 27 - VARCHAR2(3 BYTE)
            StructField("NO_IDENTIF_1", DecimalType(15,0), True),                  # campo 28 - NUMBER(15,0)
            StructField("SEXO_1", IntegerType(), True),                      # campo 29 - CHAR(1 BYTE)
            StructField("FECHA_NACIMIENTO_1", DateType(), True),           # campo 30
            StructField("NOMBRE_1", StringType(), True),                    # campo 31 - VARCHAR2(80 BYTE)
            StructField("ESTUDIOS_1", IntegerType(), True),                  # campo 32 - N/A
            StructField("ESTADO_1", StringType(), True),                    # campo 33 - VARCHAR2(3 BYTE)
            StructField("PARENT_2", IntegerType(), True),                    # campo 34 - VARCHAR2(3 BYTE)
            StructField("TIPO_IDENTIF_2", StringType(), True),              # campo 35 - VARCHAR2(3 BYTE)
            StructField("NO_IDENTIF_2", DecimalType(15,0), True),                  # campo 36 - NUMBER(15,0)
            StructField("SEXO_2", IntegerType(), True),                      # campo 37 - CHAR(1 BYTE)
            StructField("FECHA_NACIMIENTO_2", DateType(), True),           # campo 38
            StructField("NOMBRE_2", StringType(), True),                    # campo 39 - VARCHAR2(80 BYTE)
            StructField("ESTUDIOS_2", IntegerType(), True),                  # campo 40 - N/A
            StructField("ESTADO_2", StringType(), True),                    # campo 41 - VARCHAR2(3 BYTE)
            StructField("PARENT_3", IntegerType(), True),                    # campo 42 - VARCHAR2(3 BYTE)
            StructField("TIPO_IDENTIF_3", StringType(), True),              # campo 43 - VARCHAR2(3 BYTE)
            StructField("NO_IDENTIF_3", DecimalType(15,0), True),                  # campo 44 - NUMBER(15,0)
            StructField("SEXO_3", IntegerType(), True),                      # campo 45 - CHAR(1 BYTE)
            StructField("FECHA_NACIMIENTO_3", DateType(), True),           # campo 46
            StructField("NOMBRE_3", StringType(), True),                    # campo 47 - VARCHAR2(80 BYTE)
            StructField("ESTUDIOS_3", IntegerType(), True),                  # campo 48 - N/A
            StructField("ESTADO_3", StringType(), True),                    # campo 49 - VARCHAR2(3 BYTE)
            StructField("NO_RAD_NOT_TEC", DecimalType(20,0), True),               # campo 50 - NUMBER(20,0)
            StructField("INTERES_TEC_PRIMA", DecimalType(19,10), True),          # campo 51 - NUMBER(15,10)
            StructField("TASA_CREC_PRIMA", DecimalType(5,2), True),             # campo 52 - NUMBER(5,2)
            StructField("GASTOS_PRIMA", DecimalType(5,4), True),               # campo 53 - NUMBER(5,4)
            StructField("PRIMA", DecimalType(30,2), True),                        # campo 54 - NUMBER(17,2)
            StructField("FECHA_INICIAL_RENTA_DIFERIDA", StringType(), True), # campo 55 - N/A
            StructField("VALOR_MESADA_BENEF_PERIO_DIFER", StringType(), True), # campo 56 - N/A
            StructField("VALOR_AUXILIO_FUNERARIO", DecimalType(14,2), True),    # campo 57 - N/A
            StructField("OBSERVACIONES", StringType(), True),              # campo 58 - N/A
            StructField("VALOR_RESERVA_RV08", DecimalType(14,2), True),          # campo 59 - NUMBER(14,2)
            StructField("VALOR_RESERVA_RV89", DecimalType(14,2), True),          # campo 60 - NUMBER(14,2)
            StructField("PAR_0",DecimalType(28,2), True),                      # campo 61 - N/A
            StructField("PARMA", DecimalType(28,2), True),                      # campo 62 - N/A
            StructField("PORCENTAJE_EXC_PARMA", StringType(), True),      # campo 63 - N/A
            StructField("POR_AMORT_ACUM", DecimalType(34,2), True),              # campo 64 - NUMBER(5,2)
            StructField("FEC_AVISO_SINIESTRO", StringType(), True),       # campo 65 - N/A
            StructField("FAIR_VALUE", StringType(), True),                 # campo 66 - N/A
            StructField("INSCRIPCION_MECANISMO", StringType(), True),      # campo 67 - VARCHAR2(1 BYTE)
            StructField("FECHA_INSCRIPCION", DateType(), True),            # campo 68
            StructField("PARAMETRO_DESLIZAMIENTO", DecimalType(28,15), True),    # campo 69 - NUMBER(20,15)
            StructField("TASA_FAC_SEG", DecimalType(10,2), True),               # campo 70 - PARAMETERS
            StructField("VALOR_COBERTURA", DecimalType(14,2), True),             # campo 71 - NUMBER(14,2)
            StructField("FECHA_EXPED_POLIZA", DateType(), True),           # campo 72
            StructField("FECHA_MUERTE_AFILIADO", DateType(), True),        # campo 73
            StructField("INTERES_TECNICO_NIIF4", StringType(), True),      # campo 74 - NUMBER(15,10)
            StructField("UNIDAD_DE_CUENTA", StringType(), True),            # campo 75 - N/A
            StructField("PERIODO_PARTITION", StringType(), True)            # campo 76 - partition
        ])

#Read tables from silver arpprod
df_hist_datos_invalidez_muerte = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_HIST_DATOS_INVALIDEZ_MUERTE").toDF()
df_hist_sar_pcl_pension = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_HIST_SAR_PCL_PENSION").toDF()
df_ind_datos_reservas_siniestros = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_IND_DATOS_RESERVAS_SINIESTROS").toDF()
df_par_trabajadores = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_PAR_TRABAJADORES").toDF()
df_sar_auditoria_medica = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_AUDITORIA_MEDICA").toDF()
df_sar_beneficiarios_siniestros = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_BENEFICIARIOS_SINIESTROS").toDF()
df_sar_datos_invalidez_muerte = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_DATOS_INVALIDEZ_MUERTE").toDF()
df_sar_reserva_pcj_amortizacion = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_RESERVA_PCJ_AMORTIZACION").toDF()
df_sar_siniestros = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_SINIESTROS").toDF()
df_sar_pagos_coberturas  = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_PAGOS_COBERTURAS").toDF()



#Read tables from silver general
df_acu_personas_naturales = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ACU_PERSONAS_NATURALES").toDF()
df_cgr_equivalencia_tipos_id = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_EQUIVALENCIA_TIPOS_ID").toDF()	
df_anp_ipc = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_IPC").toDF()
df_rva_reserva_matematica_transic = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_RVA_RESERVA_MATEMATICA_TRANSIC").toDF()
df_cgr_origenes = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_ORIGENES").toDF()
df_pgr_salarios_minimos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_PGR_SALARIOS_MINIMOS").toDF()
df_filter_rva_reserva_matematica_transic = df_rva_reserva_matematica_transic.filter(
        (df_rva_reserva_matematica_transic["ramo"] == "ARP" )  )

#Make aggregated for min id nid_calculo groupBy 
df_aggregated_hist_datos_invalidez_muerte = (
        df_hist_datos_invalidez_muerte.groupBy(
            df_hist_datos_invalidez_muerte["nid_siniestro"]
).agg(
        min(df_hist_datos_invalidez_muerte["nid_hist_datos_inv_mte"]).alias("min_nid_hist_datos_inv_mte"))  # Take max id with groupBy
)

df_aggregated_hist_datos_invalidez_muerte = df_aggregated_hist_datos_invalidez_muerte.withColumnRenamed("nid_siniestro", "nid_siniestro_group") 

df_aggregated_hist_datos_invalidez_muerte = df_aggregated_hist_datos_invalidez_muerte.join(
    df_hist_datos_invalidez_muerte,
    df_aggregated_hist_datos_invalidez_muerte["min_nid_hist_datos_inv_mte"] == df_hist_datos_invalidez_muerte["nid_hist_datos_inv_mte"],
    "inner"
)


# Renombrar columnas para evitar ambigüedades
df_salarios_min = df_pgr_salarios_minimos.withColumnRenamed("ANO", "ANIO_SAL") \
                                 .withColumnRenamed("VLR_SALARIO_MINIMO", "VLR_SMIN")

df_salarios_min = df_salarios_min.filter(col("MES") == 1)

df_sar_auditoria_medica = df_sar_auditoria_medica.select(
    when(df_sar_auditoria_medica["cod_concepto_medico_am"] == "4", "2").when(df_sar_auditoria_medica["cod_concepto_medico_am"] == "5", "1").otherwise(None).alias("cod_origen"),
    df_sar_auditoria_medica["nid_siniestro"],
    df_sar_auditoria_medica["cod_concepto_medico_am"],
    df_sar_auditoria_medica["nid_auditoria_medica"],
    df_sar_auditoria_medica["fec_registra"],
    df_sar_auditoria_medica["fec_registra_partition"]
)

# Filtro en auditoría
df_auditoria_filtrada = df_sar_auditoria_medica.filter(df_sar_auditoria_medica["cod_concepto_medico_am"] == "4")

df_hist_sar_pcl_pension = df_hist_sar_pcl_pension.withColumnRenamed("fec_registra", "fec_registra_pcl")

# Join entre los dos
df_joined = df_hist_sar_pcl_pension.join(
    df_auditoria_filtrada,
    on="nid_siniestro",
    how="inner"
)

# Definir partición por nid_siniestro y ordenar por fecha
window_asc = Window.partitionBy("nid_siniestro").orderBy(df_hist_sar_pcl_pension["fec_registra_pcl"])
window_desc = Window.partitionBy("nid_siniestro").orderBy(df_hist_sar_pcl_pension["fec_registra_pcl"].desc())

# Agregar las columnas usando funciones de ventana
df_first_hist_sar_pcl_pension = df_joined.select(
    col("nid_siniestro"),
    first("pcj_pcl").over(window_asc).alias("primero"),
    first("pcj_pcl").over(window_desc).alias("ultimo"),
    first("fec_registra_pcl").over(window_desc).alias("ultima_modificacion")
).dropDuplicates(["nid_siniestro", "primero", "ultimo", "ultima_modificacion"])


#Step 1  Agregar la columna "orden"

window_spec = Window.partitionBy("nid_siniestro").orderBy("cod_parentesco")

df_sar_beneficiarios_siniestros = df_sar_beneficiarios_siniestros.withColumn("orden", row_number().over(window_spec))


df_hijos = df_sar_beneficiarios_siniestros.filter(
    (df_sar_beneficiarios_siniestros["cod_parentesco"] == "4") &
     (df_sar_beneficiarios_siniestros["bln_con_derecho"] == "S")
    ).groupBy("nid_siniestro").agg(
        count("*").alias("NO_HIJOS")
        ).withColumn(
            "NO_HIJOS",
            when(col("NO_HIJOS") == 0, None)
            .otherwise(col("NO_HIJOS"))
            )


conteo_hijos_invalidos = df_sar_beneficiarios_siniestros.filter(
    (df_sar_beneficiarios_siniestros["cod_parentesco"] == "4") &
    (df_sar_beneficiarios_siniestros["bln_con_derecho"] == "S") &
    (df_sar_beneficiarios_siniestros["cod_estado_persona"] == "2")
    ).groupBy("nid_siniestro").agg(
        count("*").alias("NO_HIJOS_INVALIDOS")
        ).withColumn(
            "NO_HIJOS_INVALIDOS",
            when(col("NO_HIJOS_INVALIDOS") == 0, None).otherwise(col("NO_HIJOS_INVALIDOS"))
            )

df_existentes_amortizacion = df_sar_reserva_pcj_amortizacion.select("nid_siniestro").dropDuplicates()


df_siniestros_with_flag = df_sar_siniestros.join(
    df_existentes_amortizacion.withColumn("existe", lit(True)),
    on="nid_siniestro",
    how="left"
    ).withColumn(
        "existe",
        when(col("existe").isNull(), lit(False)).otherwise(col("existe"))
    )


# Convertir string a formato fecha y luego a "yyyymm"
fec_corte_yyyymm = date_format(to_date(lit(FEC_CORTE), "yyyy-MM-dd"), "yyyyMM")
 
# Filtrar df_sar_pagos_coberturas
df_filtrado_pagos_coberturas = df_sar_pagos_coberturas.filter(
    (col("cod_cobertura") == 17) &
    (date_format("fec_pago", "yyyyMM") == fec_corte_yyyymm)
).select("nid_siniestro", "vlr_pago")


sar = df_sar_siniestros.alias("sar")
tra = df_par_trabajadores.alias("tra")
inv = df_sar_datos_invalidez_muerte.alias("inv")
aud = df_sar_auditoria_medica.alias("aud")
res = df_ind_datos_reservas_siniestros.alias("res")
ben = df_sar_beneficiarios_siniestros.alias("ben")
pcl = df_first_hist_sar_pcl_pension.alias("pcl")
mat = df_filter_rva_reserva_matematica_transic.alias("mat")
his = df_aggregated_hist_datos_invalidez_muerte.alias("his")
ben_apn = df_acu_personas_naturales.alias("ben_apn")
tra_apn = df_acu_personas_naturales.alias("tra_apn")
ben_eqid = df_cgr_equivalencia_tipos_id.alias("ben_eqid")
tra_eqid = df_cgr_equivalencia_tipos_id.alias("tra_eqid")
org = df_cgr_origenes.alias("org")
sal = df_salarios_min.alias("sal")
ipc = df_anp_ipc.alias("ipc")
amo = df_siniestros_with_flag.alias("amo") 
hjs = df_hijos.alias("hjs")
hjv = conteo_hijos_invalidos.alias("hjv")
cov = df_filtrado_pagos_coberturas.alias("cov")


df_join_reserva_arp = sar.join(tra, sar["nid_trabajador"] == tra["nid_trabajador"], "inner") \
    .join(inv, sar["nid_siniestro"] == inv["nid_siniestro"], "inner") \
    .join(aud, inv["nid_auditoria_medica"] == aud["nid_auditoria_medica"], "inner") \
    .join(res, sar["nid_siniestro"] == res["nid_siniestro"], "inner") \
    .join(ben, sar["nid_siniestro"] == ben["nid_siniestro"], "left") \
    .join(pcl, sar["nid_siniestro"] == pcl["nid_siniestro"], "left") \
    .join(mat, sar["nid_siniestro"] == mat["nid_siniestro_renta"], "left") \
    .join(amo, mat["nid_siniestro_renta"] == amo["nid_siniestro"], "left") \
    .join(his, sar["nid_siniestro"] == his["nid_siniestro"], "left") \
    .join(ben_apn, (ben["COD_TIPO_IDENTIFICACION"] == ben_apn["COD_TIPO_IDENTIFICACION"]) &
                (ben["NID_NUMERO_IDENTIFICACION"] == ben_apn["NID_NUMERO_IDENTIFICACION"]), "left") \
    .join(tra_apn, (tra["COD_TIPO_IDENTIFICACION"] == tra_apn["COD_TIPO_IDENTIFICACION"]) &
                (tra["NID_NUMERO_IDENTIFICACION"] == tra_apn["NID_NUMERO_IDENTIFICACION"]), "left") \
    .join(ben_eqid, ben["cod_tipo_identificacion"] == ben_eqid["cod_tipo_documento"], "left") \
    .join(tra_eqid, tra["cod_tipo_identificacion"] == tra_eqid["cod_tipo_documento"], "left") \
    .join(org, aud["cod_origen"] == org["cod_origen"], "left") \
    .join(hjs, sar["nid_siniestro"] == hjs["nid_siniestro"], "left") \
    .join(hjv, sar["nid_siniestro"] == hjv["nid_siniestro"], "left") \



df_join_reserva_arp = df_join_reserva_arp.withColumn(
    "anio_aprobacion", year(col("inv.fec_aprobacion_pension"))
)

df_join_reserva_arp = df_join_reserva_arp.join(
    df_anp_ipc,
    df_join_reserva_arp["anio_aprobacion"] == df_anp_ipc["anio"],
    "left"
)

# Unir con tabla de salarios mínimos por año
df_join_reserva_arp = df_join_reserva_arp.join(
    sal,
    df_join_reserva_arp["anio_aprobacion"] == sal["ANIO_SAL"],
    "left"
)


df_join_reserva_arp = df_join_reserva_arp.join(
    cov,
    on="nid_siniestro",
    how="left"
)


#campo 11
df_campos_arp_final = df_join_reserva_arp.withColumn("SEXO",
    when(col("tra_apn.genero") == "M", 1).when(col("tra_apn.genero") == "F", 2)
)


df_campos_arp_final = df_campos_arp_final.withColumn("SEXO_1",
    when(col("ben_apn.genero") == "M", 1).when(col("ben_apn.genero") == "F", 2)
)


# Agregamos la columna "NOMBRE"
df_campos_arp_final = df_campos_arp_final.withColumn(
    "NOMBRE",
    substring(
        trim(
            concat_ws(
                " ",
                col("tra_apn.PRIMER_APELLIDO"),
                col("tra_apn.SEGUNDO_APELLIDO"),
                col("tra_apn.PRIMER_NOMBRE"),
                col("tra_apn.SEGUNDO_NOMBRE")
            )
        ),
        1,
        30
    )
)


# Agregamos la columna "NOMBRE_1"
df_campos_arp_final = df_campos_arp_final.withColumn(
    "NOMBRE_1",
    substring(
        trim(
            concat_ws(
                " ",
                col("ben_apn.PRIMER_APELLIDO"),
                col("ben_apn.SEGUNDO_APELLIDO"),
                col("ben_apn.PRIMER_NOMBRE"),
                col("ben_apn.SEGUNDO_NOMBRE")
            )
        ),
        1,
        30
    )
)


# campos 22, 23, 24 .. case 
df_campos_arp_final = df_campos_arp_final \
.withColumn(
    "grado_invalidez_inicial",
    when(
        (col("pcl.nid_siniestro").isNull()) | (col("pcl.primero") == col("pcl.ultimo")),
        None
    ).otherwise(col("pcl.primero"))
) \
.withColumn(
    "FECHA_ULTIMA_MOD",
    when(
        (col("pcl.nid_siniestro").isNull()) | (col("pcl.primero") == col("pcl.ultimo")),
        None
    ).otherwise(col("pcl.ultima_modificacion"))
) \
.withColumn(
    "grado_invalidez_actual",
    when(
        (col("pcl.nid_siniestro").isNull()) | (col("pcl.primero") == col("pcl.ultimo")),
        None
    ).otherwise(col("pcl.ultimo"))
)


#campos 26, 34, 42
df_campos_arp_final = df_campos_arp_final.withColumn(
"PARENT_1",
when((col("ben.cod_parentesco") == "1") & (col("ben.bln_con_derecho") == "S"), lit(5))  # PADRE
.when((col("ben.cod_parentesco") == "2") & (col("ben.bln_con_derecho") == "S"), lit(6))  # MADRE
.when(
    (col("ben.cod_parentesco").isin("3", "10")) & (col("ben.bln_con_derecho") == "S"),
    when(
        floor(months_between(to_date(lit("29/12/2003"), "dd/MM/yyyy"), col("ben_apn.fec_nacimiento")) / 12) < 30,
        lit(7)  # Temporal
    ).otherwise(lit(4))  # Vitalicia
)
.when(
    (col("ben.cod_parentesco") == "4") & (col("ben.bln_con_derecho") == "S"),
    when(col("ben.cod_estado_persona") == "2", lit(2))  # HIJO INVÁLIDO
    .when(col("ben.cod_estado_persona") == "1", lit(3))  # HIJO VÁLIDO
)
.when((col("ben.cod_parentesco") == "6") & (col("ben.bln_con_derecho") == "S"), lit(8))  # HERMANO
.otherwise(None)
)


# campos 32, 40, 48
df_campos_arp_final = df_campos_arp_final.withColumn(
"ESTUDIOS_1",
when(
    (col("ben.cod_parentesco") == "4") & 
    (col("ben.cod_estado_persona") == "1") & 
    (floor(months_between(to_date(lit("29/12/2003"), "dd/MM/yyyy"), col("ben_apn.fec_nacimiento")) / 12) >= 18),
    lit(1)
    ).otherwise(None)
    )


# campos 61
df_campos_arp_final = df_campos_arp_final.withColumn(
    "PAR_0",
    when(
        (col("amo.existe")) & (col("mat.vlr_reserva_rv08") != 0),
        round((col("mat.vlr_reserva_rv89") / col("mat.vlr_reserva_rv08")) * 100, 2)
    ).otherwise(None)
)

# campos 62
df_campos_arp_final = df_campos_arp_final.withColumn(
    "PARMA",
    when(
        (col("amo.existe")) & (col("mat.vlr_reserva_rv08") != 0),
        round(100 - round((col("mat.vlr_reserva_rv89") / col("mat.vlr_reserva_rv08")) * 100, 2)/240, 10)
    ).otherwise(None)
    )

# campo 63 
fec_corte = "20240430"  # <-- Actualiza si es necesario
fec_corte_dt = to_date(lit(fec_corte), "yyyyMMdd")
fecha_base_dt = to_date(lit(fec_corte), "yyyyMMdd")

df_campos_arp_final = df_campos_arp_final.withColumn(
    "POR_AMORT_ACUM",
    when(
        (col("amo.existe")) & (col("mat.vlr_reserva_rv08") != 0),
        least(
            spark_round(
                spark_round((col("mat.vlr_reserva_rv89") / col("mat.vlr_reserva_rv08")) * 100, 2) +
                (
                    spark_round(
                        (100 - spark_round((col("mat.vlr_reserva_rv89") / col("mat.vlr_reserva_rv08")) * 100, 2)) / 240,
                        10
                    ) * (floor(months_between(fec_corte_dt, fecha_base_dt)) + lit(91))
                ),
                0
            ),
            lit(100)
        )
    ).otherwise(None)
)


#campo 15
df_campos_arp_final = df_campos_arp_final.withColumn("ESTADO",
    when((col("tra_apn.fec_muerte").isNotNull()) |(col("aud.cod_origen") == "1"), "3")
    .when(aud["cod_origen"] == "2", 2)
    .otherwise(1)
)

df_campos_arp_final = df_campos_arp_final.withColumn("ESTADO_1",
    when((col("ben_apn.fec_muerte").isNotNull()) |(col("aud.cod_origen")  == "1"), "3")
    .when(aud["cod_origen"] == "2", 2)
    .otherwise(1)
)


# campo 20 no_mesada 
# Condiciones para asignar NO_MESADA
df_campos_arp_final = df_campos_arp_final.withColumn(
    "NO_MESADAS",
    when(col("his.FEC_DICTAMEN_INVMTE") <= lit("2005-07-25"), 14)
    .when(
        (col("his.FEC_DICTAMEN_INVMTE") > lit("2005-07-25")) & (col("his.FEC_DICTAMEN_INVMTE") <= lit("2011-07-31")),
        when(col("his.VLR_PENSION") <= 3 * col("sal.VLR_SMIN"), 14).otherwise(13)
    )
    .otherwise(13)  # Valor por defecto si no cae en los rangos
)


# En lugar de crear "vlr_auxilio_funerarios", crea directamente "VALOR_AUXILIO_FUNERARIO"
df_campos_arp_final = df_campos_arp_final.withColumn(
    "VALOR_AUXILIO_FUNERARIO",
    coalesce(
        col("vlr_pago"),
        when(col("org.COD_ORIGEN_SUPER").isin(1, 3),
            when(col("inv.vlr_pension") <= col("VLR_SMIN") * 5,
                round(col("VLR_SMIN") * 5, 0))
            .when(col("inv.vlr_pension") <= col("VLR_SMIN") * 10,
                round(col("inv.vlr_pension"), 0))
            .otherwise(round(col("VLR_SMIN") * 10, 0))
        ).otherwise(lit(0))
    ).cast(DecimalType(14, 2))  # Aquí aplicas el cast directamente
)

# Pivot implícito (como en tu SQL)  df_sar_beneficiarios_siniestros
df_resultados_beneficiario = df_campos_arp_final.groupBy(sar["nid_siniestro"]).agg(
	max(when(df_sar_beneficiarios_siniestros["orden"] == 1, df_campos_arp_final["ESTADO_1"])).alias("ESTADO_1"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 1, df_campos_arp_final["ESTUDIOS_1"])).alias("ESTUDIOS_1"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 1, ben_apn["fec_nacimiento"])).alias("FECHA_NACIMIENTO_1"),
    max(when(df_sar_beneficiarios_siniestros["orden"] == 1, ben_eqid["COD_TIPO_SUPER"])).alias("TIPO_IDENTIF_1"),
    max(when(df_sar_beneficiarios_siniestros["orden"] == 1, ben["nid_numero_identificacion"])).alias("NO_IDENTIF_1"),
    max(when(df_sar_beneficiarios_siniestros["orden"] == 1, df_campos_arp_final["NOMBRE_1"])).alias("NOMBRE_1"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 1, df_campos_arp_final["PARENT_1"])).alias("PARENT_1"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 1, df_campos_arp_final["SEXO"])).alias("SEXO_1"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 2, df_campos_arp_final["ESTADO_1"])).alias("ESTADO_2"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 2, df_campos_arp_final["ESTUDIOS_1"])).alias("ESTUDIOS_2"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 2, ben_apn["fec_nacimiento"])).alias("FECHA_NACIMIENTO_2"),
    max(when(df_sar_beneficiarios_siniestros["orden"] == 2, ben_eqid["COD_TIPO_SUPER"])).alias("TIPO_IDENTIF_2"),
    max(when(df_sar_beneficiarios_siniestros["orden"] == 2, ben["nid_numero_identificacion"])).alias("NO_IDENTIF_2"),
    max(when(df_sar_beneficiarios_siniestros["orden"] == 2, df_campos_arp_final["NOMBRE_1"])).alias("NOMBRE_2"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 2, df_campos_arp_final["PARENT_1"])).alias("PARENT_2"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 2, df_campos_arp_final["SEXO"])).alias("SEXO_2"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 3, df_campos_arp_final["ESTADO_1"])).alias("ESTADO_3"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 3, df_campos_arp_final["ESTUDIOS_1"])).alias("ESTUDIOS_3"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 3, ben_apn["fec_nacimiento"])).alias("FECHA_NACIMIENTO_3"),
    max(when(df_sar_beneficiarios_siniestros["orden"] == 3, ben_eqid["COD_TIPO_SUPER"])).alias("TIPO_IDENTIF_3"),
    max(when(df_sar_beneficiarios_siniestros["orden"] == 3, ben["nid_numero_identificacion"])).alias("NO_IDENTIF_3"),
    max(when(df_sar_beneficiarios_siniestros["orden"] == 3, df_campos_arp_final["NOMBRE_1"])).alias("NOMBRE_3"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 3, df_campos_arp_final["PARENT_1"])).alias("PARENT_3"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 3, df_campos_arp_final["SEXO"])).alias("SEXO_3")
    )


df_resultados_beneficiario = df_resultados_beneficiario.withColumnRenamed("nid_siniestro", "nid_siniestro_orden")

df_campos_arp_final = df_campos_arp_final.filter(df_sar_beneficiarios_siniestros["orden"] == 1)

df_campos_arp_final = df_campos_arp_final.join(
    df_resultados_beneficiario,
    sar["nid_siniestro"] == df_resultados_beneficiario["nid_siniestro_orden"],
    "left"
)
df_campos_arp_final = df_campos_arp_final.select(
    tra["nid_afiliacion"].alias("NO_POLIZA"),  # campo 3
    inv["fec_aprobacion_pension"].cast(DateType()).alias("VIG_INICIAL"),     # campo 4
    sar["nid_siniestro"].alias("NO_SINIESTRO"),             # campo 5
    sar["fec_siniestro"].cast(DateType()).alias("FECHA_CAUSACION_DERECHO"),  # campo 6
    org["cod_origen_super"].cast(IntegerType()).alias("ORIGEN_PENSION"),        # campo 7
    ben_eqid["cod_tipo_super"].cast(StringType()).alias("TIPO_IDENTIF"),           # campo 9
    tra["NID_NUMERO_IDENTIFICACION"].alias("NO_IDENTIF"),              # campo 10
    df_campos_arp_final["SEXO"].alias("SEXO"),                       # campo 11 * tra_apn
    col("tra_apn.fec_nacimiento").cast(DateType()).alias("FECHA_NACIMIENTO"),    # campo 12  *tra_apn
    df_campos_arp_final["NOMBRE"].alias("NOMBRE"),               # campo 13
    df_campos_arp_final["ESTADO"].alias("ESTADO"),          # campo 15
    hjs["NO_HIJOS"].cast(IntegerType()).alias("NO_HIJOS"),                # campo 16
    hjv["NO_HIJOS_INVALIDOS"].cast(IntegerType()).alias("NO_HIJOS_INVALIDOS"),      # campo 17
    res["vrl_ibl_manual"].cast(StringType()).alias("INGRESO_BASE_LIQUIDACION"),# campo 18
    inv["vlr_pension"].alias("MESADA"),                     # campo 19
    df_campos_arp_final["NO_MESADAS"].cast(DecimalType(2,0)).alias("NO_MESADAS"),               # campo 20
    df_campos_arp_final["GRADO_INVALIDEZ_INICIAL"].alias("GRADO_INVALIDEZ_INICIAL"),        # campo 22
    df_campos_arp_final["FECHA_ULTIMA_MOD"].cast(DateType()).alias("FECHA_ULTIMA_MOD"),   # campo 23
    df_campos_arp_final["GRADO_INVALIDEZ_ACTUAL"].alias("GRADO_INVALIDEZ_ACTUAL"),          # campo 24
    df_resultados_beneficiario["PARENT_1"].alias("PARENT_1"),                # campo 26
    df_resultados_beneficiario["TIPO_IDENTIF_1"].alias("TIPO_IDENTIF_1"),         # campo 27
    df_resultados_beneficiario["NO_IDENTIF_1"].alias("NO_IDENTIF_1"), # campo 28
    df_resultados_beneficiario["SEXO_1"].alias("SEXO_1"),                      # campo 29
    df_resultados_beneficiario["FECHA_NACIMIENTO_1"].cast(DateType()).alias("FECHA_NACIMIENTO_1"),  # campo 30
    df_resultados_beneficiario["NOMBRE_1"].alias("NOMBRE_1"),             # campo 31
    df_resultados_beneficiario["ESTUDIOS_1"].alias("ESTUDIOS_1"),              # campo 32
    df_resultados_beneficiario["ESTADO_1"].alias("ESTADO_1"),            # campo 33
    df_resultados_beneficiario["PARENT_2"].alias("PARENT_2"),                # campo 34
    df_resultados_beneficiario["TIPO_IDENTIF_2"].alias("TIPO_IDENTIF_2"),         # campo 35
    df_resultados_beneficiario["NO_IDENTIF_2"].alias("NO_IDENTIF_2"), # campo 36
    df_resultados_beneficiario["SEXO_2"].alias("SEXO_2"),                      # campo 37
    df_resultados_beneficiario["FECHA_NACIMIENTO_2"].cast(DateType()).alias("FECHA_NACIMIENTO_2"),  # campo 38
    df_resultados_beneficiario["NOMBRE_2"].alias("NOMBRE_2"),             # campo 39
    df_resultados_beneficiario["ESTUDIOS_2"].alias("ESTUDIOS_2"),              # campo 40
    df_resultados_beneficiario["ESTADO_2"].alias("ESTADO_2"),            # campo 41
    df_resultados_beneficiario["PARENT_3"].alias("PARENT_3"),                # campo 42
    df_resultados_beneficiario["TIPO_IDENTIF_3"].alias("TIPO_IDENTIF_3"),         # campo 43
    df_resultados_beneficiario["NO_IDENTIF_3"].alias("NO_IDENTIF_3"), # campo 44
    df_resultados_beneficiario["SEXO_3"].alias("SEXO_3"),                      # campo 45
    df_resultados_beneficiario["FECHA_NACIMIENTO_3"].cast(DateType()).alias("FECHA_NACIMIENTO_3"),  # campo 46
    df_resultados_beneficiario["NOMBRE_3"].alias("NOMBRE_3"),             # campo 47
    df_resultados_beneficiario["ESTUDIOS_3"].alias("ESTUDIOS_3"),              # campo 48
    df_resultados_beneficiario["ESTADO_3"].alias("ESTADO_3"),            # campo 49
    #tas["pcj_ipc"].alias("TASA_CREC_PRIMA"),          # campo 52
    df_campos_arp_final["pcj_ipc"].alias("TASA_CREC_PRIMA"),          # campo 52
    #col("df_campos_arp_final.pcj_ipc").alias("TASA_CREC_PRIMA"),          # campo 52
    #df_campos_arp_final["VALOR_AUXILIO_FUNERARIO"].cast(DecimalType(14,2)).alias("VALOR_AUXILIO_FUNERARIO"),  # campo 57
    df_campos_arp_final["VALOR_AUXILIO_FUNERARIO"].alias("VALOR_AUXILIO_FUNERARIO"),
    mat["vlr_reserva_rv08"].alias("VALOR_RESERVA_RV08"),    # campo 59
    mat["vlr_reserva_rv89"].alias("VALOR_RESERVA_RV89"),    # campo 60
    df_campos_arp_final["PAR_0"].cast(DecimalType(28,2)).alias("PAR_0"),                    # campo 61
    df_campos_arp_final["PARMA"].cast(DecimalType(28,2)).alias("PARMA"),                     # campo 62
    df_campos_arp_final["POR_AMORT_ACUM"].alias("POR_AMORT_ACUM"),           # campo 64
    inv["fec_aprobacion_pension"].cast(DateType()).alias("FECHA_EXPED_POLIZA"), # campo72
    col("tra_apn.fec_muerte").cast(DateType()).alias("FECHA_MUERTE_AFILIADO")   # campo 73 * tra_apn
)
df_campos_arp_final = df_campos_arp_final \
    .withColumn("RAMO", lit(39)) \
    .withColumn("UNIDAD", lit(None).cast(IntegerType())) \
    .withColumn("PARENT", lit(None).cast(IntegerType())) \
    .withColumn("ESTUDIOS", lit(None).cast(StringType())) \
    .withColumn("RETROACTIVO", lit(None).cast(StringType())) \
    .withColumn("NO_RAD_NOT_TEC", lit(2011028468000000.0).cast(DecimalType(20,0))) \
    .withColumn("INTERES_TEC_PRIMA", lit(None).cast(DecimalType(19, 10))) \
    .withColumn("GASTOS_PRIMA", lit(None).cast(DecimalType(5, 4))) \
    .withColumn("PRIMA", lit(None).cast(DecimalType(30, 2))) \
    .withColumn("FECHA_INICIAL_RENTA_DIFERIDA", lit(None).cast(StringType())) \
    .withColumn("VALOR_MESADA_BENEF_PERIO_DIFER", lit(None).cast(StringType())) \
    .withColumn("OBSERVACIONES", lit(None).cast(StringType())) \
    .withColumn("PORCENTAJE_EXC_PARMA", lit(None).cast(StringType())) \
    .withColumn("FEC_AVISO_SINIESTRO", lit(None).cast(StringType())) \
    .withColumn("INSCRIPCION_MECANISMO", lit("0").cast(StringType())) \
    .withColumn("FECHA_INSCRIPCION", lit(None).cast(DateType())) \
    .withColumn("TASA_FAC_SEG", lit("0").cast(DecimalType(10, 2))) \
    .withColumn("VALOR_COBERTURA", lit(None).cast(DecimalType(14, 2))) \
    .withColumn("UNIDAD_DE_CUENTA", lit(None).cast(StringType())) \
    .withColumn("FAIR_VALUE", lit(None).cast(StringType())) \
    .withColumn("INTERES_TECNICO_NIIF4", lit(None).cast(StringType()))
df_campos_arp_final = df_campos_arp_final.withColumn("PARAMETRO_DESLIZAMIENTO", lit(1.0).cast(DecimalType(28,15)))\
        .withColumn("GASTOS", lit(1.0).cast(DecimalType(5,4)))
df_campos_arp_final = df_campos_arp_final.withColumn("PERIODO_PARTITION", date_format(df_campos_arp_final["FECHA_CAUSACION_DERECHO"], "yyyy-MM"))
column_names = [field.name for field in schema_reserva_arp_gold]

df_campos_arp_final = df_campos_arp_final.select(*column_names)
df_real_schema = {field.name: field.dataType for field in df_campos_arp_final.schema.fields}
 
# Comparación
mismatched_types = []
 
for field in schema_reserva_arp_gold:
    name = field.name
    expected_type = field.dataType
    real_type = df_real_schema.get(name)
    if real_type and real_type != expected_type:
        mismatched_types.append((name, str(real_type), str(expected_type)))
 
# Mostrar diferencias
for name, real, expected in mismatched_types:
    print(f"{name}: real = {real}, esperado = {expected}")
df_reserva_arp_gold = spark.createDataFrame(df_campos_arp_final.rdd, schema=schema_reserva_arp_gold)
df_rvi_table = spark.read.option("basePath", "s3://alfa-datalake-dev-gold/GOLD/NIIF/RESERVA_RVI_GOLD/") \
    .parquet("s3://alfa-datalake-dev-gold/GOLD/NIIF/RESERVA_RVI_GOLD/*")
df_campos_arp_final = df_campos_arp_final.select(*column_names)
df_campos_gold_final = df_rvi_table .union(df_campos_arp_final)
df_campos_gold_final = df_campos_gold_final.dropDuplicates()
df_campos_gold_final.groupBy("NO_SINIESTRO") \
    .count() \
    .filter("count > 1") \
    .show(truncate=False)
"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim, date_format, to_date, lit, coalesce, min, max, broadcast, year, when, concat_ws, substring, countDistinct, months_between, round, floor, greatest, concat, explode, sequence, log, exp, sum as sum_, row_number
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, DoubleType, DecimalType
from pyspark.sql.window import Window

import sys, traceback, boto3, json


BUCKET_NAME = 'alfa-datalake-dev-gold'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
DB_NAME = 'ALFAPRD'
DB_SCHEMA = 'PRV'
PERIOD = '202501'
CERTIFICATE = 'False'
DB_CATALOG_GLUE = 'datalake-dev-silver-catalog-db'
TABLE_NAME_GOLD = 'RESERVA_RVI_GOLD'
FEC_CORTE = '2025-04-16'
FEC_CORTE_DATE = '2024-11-07'

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# Define the schema for the  Final DataFrame
schema_reserva_rvi_gold = StructType([
    StructField("RAMO", IntegerType(), True),                         # campo 1 - VARCHAR2(3 BYTE)
    StructField("UNIDAD", IntegerType(), True),                       # campo 2 - N/A
    StructField("NO_POLIZA", DecimalType(10,0), True),                   # campo 3 - NUMBER(10,0)
    StructField("VIG_INICIAL", DateType(), True),                    # campo 4
    StructField("NO_SINIESTRO", DecimalType(10,0), True),               # campo 5 - NUMBER(10,0)
    StructField("FECHA_CAUSACION_DERECHO", DateType(), True),       # campo 6
    StructField("ORIGEN_PENSION", IntegerType(), True),              # campo 7 - VARCHAR2(3 BYTE)
    StructField("PARENT", IntegerType(), True),                       # campo 8 - N/A
    StructField("TIPO_IDENTIF", StringType(), True),                # campo 9 - VARCHAR2(2 BYTE)
    StructField("NO_IDENTIF", DecimalType(15,0), True),                     # campo 10 - NUMBER(15,0)
    StructField("SEXO", IntegerType(), True),                         # campo 11 - CHAR(1 BYTE)
    StructField("FECHA_NACIMIENTO", DateType(), True),              # campo 12
    StructField("NOMBRE", StringType(), True),                      # campo 13 - VARCHAR2(80 BYTE)
    StructField("ESTUDIOS", StringType(), True),                    # campo 14 - N/A
    StructField("ESTADO", StringType(), True),                      # campo 15 - VARCHAR2(3 BYTE)
    StructField("NO_HIJOS", IntegerType(), True),                      # campo 16 - NUMBER(15,0)
    StructField("NO_HIJOS_INVALIDOS", IntegerType(), True),            # campo 17 - NUMBER(15,0)
    StructField("INGRESO_BASE_LIQUIDACION", StringType(), True),    # campo 18 - N/A
    StructField("MESADA", DecimalType(16,2), True),                      # campo 19 - N/A
    StructField("NO_MESADAS", DecimalType(2,0), True),                 # campo 20 - NUMBER(2,0)
    StructField("RETROACTIVO", StringType(), True),                 # campo 21 - N/A
    StructField("GRADO_INVALIDEZ_INICIAL", DecimalType(5,2), True),      # campo 22 - NUMBER(5,2)
    StructField("FECHA_ULTIMA_MOD", DateType(), True),              # campo 23
    StructField("GRADO_INVALIDEZ_ACTUAL", DecimalType(5,2), True),       # campo 24 - NUMBER(5,2)
    StructField("GASTOS", DecimalType(5,4), True),                      # campo 25 - NUMBER(5,4)
    StructField("PARENT_1", IntegerType(), True),                    # campo 26 - VARCHAR2(3 BYTE)
    StructField("TIPO_IDENTIF_1", StringType(), True),              # campo 27 - VARCHAR2(3 BYTE)
    StructField("NO_IDENTIF_1", DecimalType(15,0), True),                  # campo 28 - NUMBER(15,0)
    StructField("SEXO_1", IntegerType(), True),                      # campo 29 - CHAR(1 BYTE)
    StructField("FECHA_NACIMIENTO_1", DateType(), True),           # campo 30
    StructField("NOMBRE_1", StringType(), True),                    # campo 31 - VARCHAR2(80 BYTE)
    StructField("ESTUDIOS_1", IntegerType(), True),                  # campo 32 - N/A
    StructField("ESTADO_1", StringType(), True),                    # campo 33 - VARCHAR2(3 BYTE)
    StructField("PARENT_2", IntegerType(), True),                    # campo 34 - VARCHAR2(3 BYTE)
    StructField("TIPO_IDENTIF_2", StringType(), True),              # campo 35 - VARCHAR2(3 BYTE)
    StructField("NO_IDENTIF_2", DecimalType(15,0), True),                  # campo 36 - NUMBER(15,0)
    StructField("SEXO_2", IntegerType(), True),                      # campo 37 - CHAR(1 BYTE)
    StructField("FECHA_NACIMIENTO_2", DateType(), True),           # campo 38
    StructField("NOMBRE_2", StringType(), True),                    # campo 39 - VARCHAR2(80 BYTE)
    StructField("ESTUDIOS_2", IntegerType(), True),                  # campo 40 - N/A
    StructField("ESTADO_2", StringType(), True),                    # campo 41 - VARCHAR2(3 BYTE)
    StructField("PARENT_3", IntegerType(), True),                    # campo 42 - VARCHAR2(3 BYTE)
    StructField("TIPO_IDENTIF_3", StringType(), True),              # campo 43 - VARCHAR2(3 BYTE)
    StructField("NO_IDENTIF_3", DecimalType(15,0), True),                  # campo 44 - NUMBER(15,0)
    StructField("SEXO_3", IntegerType(), True),                      # campo 45 - CHAR(1 BYTE)
    StructField("FECHA_NACIMIENTO_3", DateType(), True),           # campo 46
    StructField("NOMBRE_3", StringType(), True),                    # campo 47 - VARCHAR2(80 BYTE)
    StructField("ESTUDIOS_3", IntegerType(), True),                  # campo 48 - N/A
    StructField("ESTADO_3", StringType(), True),                    # campo 49 - VARCHAR2(3 BYTE)
    StructField("NO_RAD_NOT_TEC", DecimalType(20,0), True),               # campo 50 - NUMBER(20,0)
    StructField("INTERES_TEC_PRIMA", DecimalType(19,10), True),          # campo 51 - NUMBER(15,10)
    StructField("TASA_CREC_PRIMA", DecimalType(5,2), True),             # campo 52 - NUMBER(5,2)
    StructField("GASTOS_PRIMA", DecimalType(5,4), True),               # campo 53 - NUMBER(5,4)
    StructField("PRIMA", DecimalType(30,2), True),                        # campo 54 - NUMBER(17,2)
    StructField("FECHA_INICIAL_RENTA_DIFERIDA", StringType(), True), # campo 55 - N/A
    StructField("VALOR_MESADA_BENEF_PERIO_DIFER", StringType(), True), # campo 56 - N/A
    StructField("VALOR_AUXILIO_FUNERARIO", DecimalType(14,2), True),    # campo 57 - N/A
    StructField("OBSERVACIONES", StringType(), True),              # campo 58 - N/A
    StructField("VALOR_RESERVA_RV08", DecimalType(14,2), True),          # campo 59 - NUMBER(14,2)
    StructField("VALOR_RESERVA_RV89", DecimalType(14,2), True),          # campo 60 - NUMBER(14,2)
    StructField("PAR_0",DecimalType(28,2), True),                      # campo 61 - N/A
    StructField("PARMA", DecimalType(28,2), True),                      # campo 62 - N/A
    StructField("PORCENTAJE_EXC_PARMA", StringType(), True),      # campo 63 - N/A
    StructField("POR_AMORT_ACUM", DecimalType(34,2), True),              # campo 64 - NUMBER(5,2)
    StructField("FEC_AVISO_SINIESTRO", StringType(), True),       # campo 65 - N/A
    StructField("FAIR_VALUE", StringType(), True),                 # campo 66 - N/A
    StructField("INSCRIPCION_MECANISMO", StringType(), True),      # campo 67 - VARCHAR2(1 BYTE)
    StructField("FECHA_INSCRIPCION", DateType(), True),            # campo 68
    StructField("PARAMETRO_DESLIZAMIENTO", DecimalType(28,15), True),    # campo 69 - NUMBER(20,15)
    StructField("TASA_FAC_SEG", DecimalType(10,2), True),               # campo 70 - PARAMETERS
    StructField("VALOR_COBERTURA", DecimalType(14,2), True),             # campo 71 - NUMBER(14,2)
    StructField("FECHA_EXPED_POLIZA", DateType(), True),           # campo 72
    StructField("FECHA_MUERTE_AFILIADO", DateType(), True),        # campo 73
    StructField("INTERES_TECNICO_NIIF4", StringType(), True),      # campo 74 - NUMBER(15,10)
    StructField("UNIDAD_DE_CUENTA", StringType(), True),            # campo 75 - N/A
    StructField("PERIODO_PARTITION", StringType(), True)            # campo 76 - partition
])

################################################################################################################### source
#Read tables from silver prv
df_prv_rentas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_RENTAS").toDF()
df_prv_rentas_notas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_RENTAS_NOTAS").toDF()
df_prv_beneficiarios = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_BENEFICIARIOS").toDF()
df_prv_rentas_aprobadas_obp = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_RENTAS_APROBADAS_OBP").toDF()
df_prv_endosos_point = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_PRV_ENDOSOS_POINT").toDF()
df_spr_siniestros = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_PRV_SILVER_SPR_SINIESTROS").toDF()

#Read tables from silver general
df_acu_personas_naturales = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ACU_PERSONAS_NATURALES").toDF()
df_cgr_equivalencia_tipos_id = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_EQUIVALENCIA_TIPOS_ID").toDF()
df_cgr_estados_personas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_ESTADOS_PERSONAS").toDF()
df_cgr_parentescos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_PARENTESCOS").toDF()
df_anp_ipc = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_IPC").toDF()
df_rva_reserva_matematica_transic = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_RVA_RESERVA_MATEMATICA_TRANSIC").toDF()
df_pgr_salarios_minimos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_PGR_SALARIOS_MINIMOS").toDF()
df_anp_pensionado_novedad = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PENSIONADO_NOVEDAD").toDF()
df_anp_distribucion_vig_pension = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_DISTRIBUCION_VIG_PENSION").toDF()
df_anp_pensiones = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PENSIONES").toDF()

################################################################################################################### variable y alias 

df_salarios_expedicion = df_pgr_salarios_minimos.alias("exp")
df_salarios_corte = df_pgr_salarios_minimos.alias("corte")

anio_corte = int(FEC_CORTE.split("-")[0]) if "-" in FEC_CORTE else int(FEC_CORTE[:4])

salario_minimo_actual = df_pgr_salarios_minimos.filter((col("ano") == anio_corte) & (col("mes") == 1) ).select("vlr_salario_minimo").first()[0]

################################################################################################################### filter source

df_anp_pensionado_novedad_filtered = df_anp_pensionado_novedad.filter(
      (df_anp_pensionado_novedad["cod_concepto"] == '28') &
          (df_anp_pensionado_novedad["fec_ingreso"]  < to_date(lit(FEC_CORTE), "yyyy-MM-dd"))
  )

df_prv_rentas_filtered = df_prv_rentas.filter(
    (df_prv_rentas["fec_invalidacion_registro"].isNull() | (df_prv_rentas["fec_invalidacion_registro"] > to_date(lit(FEC_CORTE), "yyyy-MM-dd"))) &
        (df_prv_rentas["fec_expedicion"] <= to_date(lit(FEC_CORTE), "yyyy-MM-dd"))
)

df_rva_reserva_matematica_transic_filtered = df_rva_reserva_matematica_transic.filter(
    (df_rva_reserva_matematica_transic["ramo"] == 'RVI')
)

df_beneficiarios_filtrados = df_prv_beneficiarios.filter(
    col("cod_parentesco").isin("3", "10")
).select("nid_rentas").distinct()

################################################################################################################### Add  column source
df_cgr_parentescos = df_cgr_parentescos.withColumnRenamed("cod_parentesco", "cod_parentesco_aux")

window_spec = Window.partitionBy("nid_rentas").orderBy("cod_parentesco")

df_prv_beneficiarios = df_prv_beneficiarios.withColumn("orden", row_number().over(window_spec))


df_prv_rentas_filtered = df_prv_rentas_filtered.withColumn("anio_expedicion",year(df_prv_rentas_filtered["fec_expedicion"]))
df_prv_rentas_filtered = df_prv_rentas_filtered.withColumn("anio_anterior_corte",lit(anio_corte - 1))



df_acu_personas_naturales = df_acu_personas_naturales.withColumn(
    "edad_beneficiario",
    floor(months_between(to_date(lit(FEC_CORTE), "yyyy-MM-dd"), col("fec_nacimiento")) / 12)
)
################################################################################################################### Add table stage

###################### Beneficiario tabla
df_resultados_beneficiario = df_prv_rentas_filtered.join(
    df_prv_beneficiarios,
    df_prv_rentas_filtered["nid_rentas"] == df_prv_beneficiarios["nid_rentas"],
    "left"
).join(
    broadcast(df_cgr_parentescos),
    df_prv_beneficiarios["cod_parentesco"] == df_cgr_parentescos["cod_parentesco_aux"],
    "left"
).join(
    broadcast(df_cgr_estados_personas),
    df_prv_beneficiarios["cod_estado_persona"] == df_cgr_estados_personas["cod_estado_persona"],
    "left"
).join(
    broadcast(df_cgr_equivalencia_tipos_id),
    df_prv_beneficiarios["cod_tipo_identificacion"] == df_cgr_equivalencia_tipos_id["cod_tipo_documento"],
    "left"
).join(
    df_acu_personas_naturales,
    (df_prv_beneficiarios["cod_tipo_identificacion"] == df_acu_personas_naturales["cod_tipo_identificacion"]) &
    (df_prv_beneficiarios["nid_numero_identificacion"] == df_acu_personas_naturales["nid_numero_identificacion"]),
    "left"
)

# campos 
df_resultados_beneficiario = df_resultados_beneficiario.withColumn("SEXO",
    when(df_acu_personas_naturales["genero"] == "M", 1).when(df_acu_personas_naturales["genero"] == "F", 2)
)


df_resultados_beneficiario = df_resultados_beneficiario.withColumn("NOMBRE",
    substring(trim(concat_ws(" ",trim(df_acu_personas_naturales["primer_apellido"]),trim(df_acu_personas_naturales["segundo_apellido"]),trim(df_acu_personas_naturales["primer_nombre"]),trim(df_acu_personas_naturales["segundo_nombre"]))),1, 30)
)

df_resultados_beneficiario = df_resultados_beneficiario.withColumn(
    "PARENT_1",
    when(df_cgr_parentescos["cod_parentesco_aux"] == "1", 5)
    .when(df_cgr_parentescos["cod_parentesco_aux"] == "2", 6)
    .when(df_cgr_parentescos["cod_parentesco_aux"] == "3", 4)
    .when(df_cgr_parentescos["cod_parentesco_aux"] == "4", 3)
    .when(df_cgr_parentescos["cod_parentesco_aux"] == "6", 8)
)

df_resultados_beneficiario = df_resultados_beneficiario.withColumn(
    "ESTUDIOS_1",
    when((df_cgr_parentescos["cod_parentesco_aux"] == "4") &(df_prv_beneficiarios["cod_estado_persona"] == "1") & (months_between(to_date(lit(FEC_CORTE), "yyyy-MM-dd"), df_acu_personas_naturales["fec_nacimiento"]) / 12 >= 18), 1 )
)


# Pivot implícito 
df_resultados_beneficiario = df_resultados_beneficiario.groupBy(df_prv_rentas_filtered["nid_rentas"]).agg(
	max(when(df_prv_beneficiarios["orden"] == 1, df_prv_beneficiarios["cod_estado_persona"])).alias("ESTADO_1"),
	max(when(df_prv_beneficiarios["orden"] == 1, df_resultados_beneficiario["ESTUDIOS_1"])).alias("ESTUDIOS_1"),
	max(when(df_prv_beneficiarios["orden"] == 1, df_acu_personas_naturales["fec_nacimiento"])).alias("FECHA_NACIMIENTO_1"),
    max(when(df_prv_beneficiarios["orden"] == 1, df_cgr_equivalencia_tipos_id["cod_tipo_super"])).alias("TIPO_IDENTIF_1"),
    max(when(df_prv_beneficiarios["orden"] == 1, df_prv_beneficiarios["nid_numero_identificacion"])).alias("NO_IDENTIF_1"),
    max(when(df_prv_beneficiarios["orden"] == 1, df_resultados_beneficiario["NOMBRE"])).alias("NOMBRE_1"),
	max(when(df_prv_beneficiarios["orden"] == 1, df_resultados_beneficiario["PARENT_1"])).alias("PARENT_1"),
	max(when(df_prv_beneficiarios["orden"] == 1, df_resultados_beneficiario["SEXO"])).alias("SEXO_1"),
	max(when(df_prv_beneficiarios["orden"] == 2, df_prv_beneficiarios["cod_estado_persona"])).alias("ESTADO_2"),
	max(when(df_prv_beneficiarios["orden"] == 2, df_resultados_beneficiario["ESTUDIOS_1"])).alias("ESTUDIOS_2"),
	max(when(df_prv_beneficiarios["orden"] == 2, df_acu_personas_naturales["fec_nacimiento"])).alias("FECHA_NACIMIENTO_2"),
    max(when(df_prv_beneficiarios["orden"] == 2, df_cgr_equivalencia_tipos_id["cod_tipo_super"])).alias("TIPO_IDENTIF_2"),
    max(when(df_prv_beneficiarios["orden"] == 2, df_prv_beneficiarios["nid_numero_identificacion"])).alias("NO_IDENTIF_2"),
    max(when(df_prv_beneficiarios["orden"] == 2, df_resultados_beneficiario["NOMBRE"])).alias("NOMBRE_2"),
	max(when(df_prv_beneficiarios["orden"] == 2, df_resultados_beneficiario["PARENT_1"])).alias("PARENT_2"),
	max(when(df_prv_beneficiarios["orden"] == 2, df_resultados_beneficiario["SEXO"])).alias("SEXO_2"),
	max(when(df_prv_beneficiarios["orden"] == 3, df_prv_beneficiarios["cod_estado_persona"])).alias("ESTADO_3"),
	max(when(df_prv_beneficiarios["orden"] == 3, df_resultados_beneficiario["ESTUDIOS_1"])).alias("ESTUDIOS_3"),
	max(when(df_prv_beneficiarios["orden"] == 3, df_acu_personas_naturales["fec_nacimiento"])).alias("FECHA_NACIMIENTO_3"),
    max(when(df_prv_beneficiarios["orden"] == 3, df_cgr_equivalencia_tipos_id["cod_tipo_super"])).alias("TIPO_IDENTIF_3"),
    max(when(df_prv_beneficiarios["orden"] == 3, df_prv_beneficiarios["nid_numero_identificacion"])).alias("NO_IDENTIF_3"),
    max(when(df_prv_beneficiarios["orden"] == 3, df_resultados_beneficiario["NOMBRE"])).alias("NOMBRE_3"),
	max(when(df_prv_beneficiarios["orden"] == 3, df_resultados_beneficiario["PARENT_1"])).alias("PARENT_3"),
	max(when(df_prv_beneficiarios["orden"] == 3, df_resultados_beneficiario["SEXO"])).alias("SEXO_3")
    )


df_resultados_beneficiario = df_resultados_beneficiario.withColumnRenamed("nid_rentas", "nid_rentas_orden")


###################### factor IPC
df_renta_ipc = df_prv_rentas_filtered.withColumn(
    "anio_ipc",
    explode(
        sequence(
            col("anio_expedicion").cast("int"),
            col("anio_anterior_corte").cast("int")
        )
    )
)

df_renta_ipc = df_renta_ipc.join(
    df_anp_ipc.select(col("ANIO").alias("anio_ipc"), col("PCJ_IPC").alias("pcj_ipc_anp")),
    on="anio_ipc",
    how="left"
)

df_renta_ipc = df_renta_ipc.withColumn("log_factor", log(1 + (col("pcj_ipc_anp") / 100)))

df_factor = df_renta_ipc.groupBy(df_prv_rentas_filtered["nid_rentas"]).agg(
    exp(sum_("log_factor")).alias("factor_incremento")
)


###################### aux_funerario
df_aux_funerario = df_prv_rentas_filtered.join(
    df_anp_pensiones,
    df_prv_rentas_filtered["nid_rentas"] == df_anp_pensiones["nid_procedencia"],
    "left"
).join(
    df_anp_distribucion_vig_pension,
    df_anp_pensiones["nid_pension"] == df_anp_distribucion_vig_pension["nid_pension"],
    "left"
).join(
    df_anp_pensionado_novedad_filtered,
    df_anp_distribucion_vig_pension["nid_distribucion_vigencia"] == df_anp_pensionado_novedad_filtered["nid_distribucion_vigencia"],
    "left"
).groupBy(df_prv_rentas_filtered["nid_rentas"]).agg(
    coalesce(max(df_anp_pensionado_novedad_filtered["vlr_fijo"]), lit(0)).alias("vlr_aux_funerario_base")
)


df_aux_funerario = df_aux_funerario.withColumnRenamed("nid_rentas", "nid_rentas_aux")


###################### hijos
df_hijos = df_prv_beneficiarios.filter(
    (df_prv_beneficiarios["cod_parentesco"] == "4") & ((df_prv_beneficiarios["fec_aparicion"] < to_date(lit(FEC_CORTE), "yyyy-MM-dd")) | df_prv_beneficiarios["fec_aparicion"].isNull()) &
    (((df_prv_beneficiarios["bln_con_derecho"] == "S") & (df_prv_beneficiarios["bln_es_beneficiario"] == "S")) |(df_prv_beneficiarios["fec_muerte"] > to_date(lit(FEC_CORTE), "yyyy-MM-dd")) )
    ).groupBy("nid_rentas").agg(
        when(countDistinct("nid_numero_identificacion") == 0, None).otherwise(countDistinct("nid_numero_identificacion")).alias("NO_HIJOS"))
    
    
###################### hijos invalidos
df_hijos_invalidos = df_prv_beneficiarios.filter(
    (df_prv_beneficiarios["cod_parentesco"] == "4") & (df_prv_beneficiarios["bln_con_derecho"] == "S") & (df_prv_beneficiarios["cod_estado_persona"] == "2")
    ).groupBy("nid_rentas").agg(
        when(countDistinct("nid_numero_identificacion") == 0, None).otherwise(countDistinct("nid_numero_identificacion")).alias("NO_HIJOS_INVALIDOS"))


###################### tasa crecimiento
df_tasa_crec_prima = df_prv_rentas_filtered.join(
    df_anp_ipc.select(col("ANIO"), col("PCJ_IPC").alias("pcj_ipc_prima")),
    df_prv_rentas_filtered["anio_expedicion"] == df_anp_ipc["anio"],
    "left"
)


###################### prima
df_sum_prima = df_prv_endosos_point.groupBy(df_prv_endosos_point["nid_rentas"]).agg(
    _sum("vlr_prima").alias("PRIMA")
)
################################################################################################################### Add join principal
#Step 1 : join prv_rentas ->  prv_rentas_notas  -> prv_rentas_aprobadas_obp -> prv_endosos_point  -> spr_siniestros
#Step 2 : join acu_personas_naturales  ->  rva_reserva_matematica_transic  ->  cgr_estados_personas -> cgr_equivalencia_tipos_id -> df_salarios_expedicion -> df_salarios_corte
#Step 3 : join df_aux_funerario ->  df_hijos  ->  df_hijos_invalidos  ->  df_tasa_crec_prima -> df_sum_prima ->  df_beneficiarios_filtrados -> df_factor -> df_resultados_beneficiario

df_join_principal_reserva_rvi = df_prv_rentas_filtered.join(
    df_prv_rentas_notas,
    df_prv_rentas_filtered["nid_rentas"] == df_prv_rentas_notas["nid_rentas"],
    "left"
).join(
    df_prv_rentas_aprobadas_obp,
    df_prv_rentas_filtered["nid_rentas"] == df_prv_rentas_aprobadas_obp["nid_rentas"],
    "left"
).join(
    df_spr_siniestros,
    df_prv_rentas_filtered["nid_rentas"] == df_spr_siniestros["nid_siniestro"],
    "left"
).join(
    broadcast(df_cgr_equivalencia_tipos_id),
    df_prv_rentas_filtered["cod_tipo_identificacion"] == df_cgr_equivalencia_tipos_id["cod_tipo_documento"],
    "left"
).join(
    df_acu_personas_naturales,
    (df_prv_rentas_filtered["cod_tipo_identificacion"] == df_acu_personas_naturales["cod_tipo_identificacion"]) &
    (df_prv_rentas_filtered["nid_numero_identificacion"] == df_acu_personas_naturales["nid_numero_identificacion"]),
    "left"
).join(
    broadcast(df_cgr_estados_personas),
    df_prv_rentas_filtered["cod_estado_persona"] == df_cgr_estados_personas["cod_estado_persona"],
    "left"
).join(
    broadcast(df_rva_reserva_matematica_transic_filtered),
    df_prv_rentas_filtered["nid_rentas"] == df_rva_reserva_matematica_transic_filtered["nid_siniestro_renta"],
    "left"
).join(
    broadcast(df_salarios_expedicion),
    (df_prv_rentas_filtered["anio_expedicion"] == df_salarios_expedicion["ano"]) & 
    (df_salarios_expedicion["mes"] == 1),
    "left"
).join(
    broadcast(df_salarios_corte),
    (lit(anio_corte) == df_salarios_corte["ano"]) & 
    (df_salarios_corte["mes"] == 1),  
    "left"
).join(
    df_aux_funerario,
    df_prv_rentas_filtered["nid_rentas"] == df_aux_funerario["nid_rentas_aux"],
    "left"
).join(
    df_hijos,
    df_prv_rentas_filtered["NID_RENTAS"] == df_hijos["nid_rentas"],
    "left"
).join(
    df_hijos_invalidos,
    df_prv_rentas_filtered["NID_RENTAS"] == df_hijos_invalidos["nid_rentas"],
    "left"
).join(
    df_tasa_crec_prima.select("nid_rentas", "pcj_ipc_prima"), 
    on="nid_rentas",
    how="left"
).join(
    df_sum_prima,
    on="nid_rentas",
    how="left"
).join(
    df_beneficiarios_filtrados.withColumn("has_beneficiario_clave", lit(True)),
    on="nid_rentas",
    how="left"
).join(
    df_factor,
    on="nid_rentas",
    how="left"
).join(
    df_resultados_beneficiario,
    df_prv_rentas_filtered["nid_rentas"] == df_resultados_beneficiario["nid_rentas_orden"],
    "left"
)
#campo 7
df_campos_rvi_final = df_join_principal_reserva_rvi.withColumn("ORIGEN_PENSION",
    when(df_acu_personas_naturales["fec_muerte"].isNull(),
        when(df_prv_rentas_filtered["nid_rentas"].isNotNull(),
            when(df_spr_siniestros["cod_estado_siniestro"] == "4", 3)
            .otherwise(1)
        ).when((df_prv_rentas_filtered["nid_rentas"].isNull()) & (df_prv_rentas_filtered["cod_origen"] == "3"), 3)
         .when((df_prv_rentas_filtered["nid_rentas"].isNull()) & (df_prv_rentas_filtered["cod_afp"] != "1") & (df_prv_rentas_filtered["cod_origen"] == "2"), 1)
         .when((df_prv_rentas_filtered["nid_rentas"].isNull()) & (df_prv_rentas_filtered["cod_origen"] == "2"), 1)
    ).otherwise(
        when(df_prv_rentas_filtered["cod_origen"] == "1",
            when((date_format(df_acu_personas_naturales["fec_muerte"], "yyyyMMdd").cast("long") >= date_format(df_prv_rentas_filtered["fec_expedicion"], "yyyyMMdd").cast("long")) &
                 df_prv_rentas_filtered["nid_rentas"].isNull(), 5)
            .when(date_format(df_acu_personas_naturales["fec_muerte"], "yyyyMMdd").cast("long") < date_format(df_prv_rentas_filtered["fec_expedicion"], "yyyyMMdd").cast("long"), 2)
            .when((date_format(df_acu_personas_naturales["fec_muerte"], "yyyyMMdd").cast("long") >= date_format(df_prv_rentas_filtered["fec_expedicion"], "yyyyMMdd").cast("long")) &
                  df_prv_rentas_filtered["nid_rentas"].isNotNull(), 4)
        ).when(df_prv_rentas_filtered["cod_origen"] == "2", 1)
         .when(df_prv_rentas_filtered["cod_origen"] == "3", 3)
    )
)

#campo 11
df_campos_rvi_final = df_campos_rvi_final.withColumn("SEXO",
    when(df_acu_personas_naturales["genero"] == "M", 1).when(df_acu_personas_naturales["genero"] == "F", 2)
)

#campo 13
df_campos_rvi_final = df_campos_rvi_final.withColumn("NOMBRE",
    substring(trim(concat_ws(" ",trim(df_acu_personas_naturales["primer_apellido"]),trim(df_acu_personas_naturales["segundo_apellido"]),trim(df_acu_personas_naturales["primer_nombre"]),trim(df_acu_personas_naturales["segundo_nombre"]))),1, 30)
)


df_campos_rvi_final = df_campos_rvi_final.withColumn("TASA_CREC_PRIMA", df_tasa_crec_prima["pcj_ipc_prima"])

#campo 51
df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "INTERES_TEC_PRIMA",
    when(df_prv_rentas_filtered["fec_expedicion"] < to_date(lit(FEC_CORTE), "yyyy-MM-dd"), df_prv_rentas_filtered["pcj_interes_tecnico"]
    ).otherwise(df_prv_rentas_filtered["pcj_interes_reserva"] * 100 )
)

#campo 61
df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "PAR_0",
    when(
        df_rva_reserva_matematica_transic_filtered["nid_siniestro_renta"].isNotNull(),
        round((df_rva_reserva_matematica_transic_filtered["vlr_reserva_rv89"] / df_rva_reserva_matematica_transic_filtered["vlr_reserva_rv08"]) * 100, 2)
    ).otherwise(None)
)
#campo 62
df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "PARMA",
    when(
        df_rva_reserva_matematica_transic_filtered["nid_siniestro_renta"].isNotNull(),
        round(((100 - ((df_rva_reserva_matematica_transic_filtered["vlr_reserva_rv89"] / df_rva_reserva_matematica_transic_filtered["vlr_reserva_rv08"]) * 100)) / 240), 2)
    ).otherwise(None)
)
#campo 64
df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "POR_AMORT_ACUM",
    when(
        (col("PAR_0") +  col("PARMA") * (months_between(to_date(lit(FEC_CORTE), "yyyyMM"), to_date(lit("201804"), "yyyyMM")) + 91)) > 100, 100)
        .otherwise(col("PAR_0") + col("PARMA") * (months_between(to_date(lit(FEC_CORTE), "yyyyMM"), to_date(lit("201804"), "yyyyMM")) + 91)
    )
)
#campo 67
df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "INSCRIPCION_MECANISMO", 
    coalesce(df_prv_rentas_aprobadas_obp["inscripcion_mecanismo"], lit("0"))
)
#campo 69
df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "PARAMETRO_DESLIZAMIENTO", 
    df_prv_rentas_filtered["vlr_exceso_smmlv_ipc_exp"] * 100
)

#mesada 
df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "mesada",
    when(
        col("vlr_pension_inicial") <= col("exp.vlr_salario_minimo"),
        col("corte.vlr_salario_minimo")
    ).when(
        (col("vlr_pension_inicial") > col("exp.vlr_salario_minimo")) &
        ((col("vlr_pension_inicial") * col("factor_incremento")) < col("corte.vlr_salario_minimo")),
        col("corte.vlr_salario_minimo")
    ).otherwise(
        round(col("vlr_pension_inicial") * col("factor_incremento"), 2)
    )
)

# VALOR_AUXILIO_FUNERARIO
df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "VALOR_AUXILIO_FUNERARIO",
    when(
        col("ORIGEN_PENSION").isin(1, 3),
        when(
            col("mesada") <= (5 * salario_minimo_actual),
            round(lit(5 * salario_minimo_actual), 2)
        ).when(
            (col("mesada") > (5 * salario_minimo_actual)) & 
            (col("mesada") <= (10 * salario_minimo_actual)),
            round(col("mesada"), 2)
        ).otherwise(
            round(lit(10 * salario_minimo_actual), 2)
        )
    ).otherwise(
        col("vlr_aux_funerario_base")
    )
)

# observaciones
# Finalmente, creamos la columna clase_pension usando las reglas
df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "clase_pension",
    when(col("cod_origen").isin(2, 3), lit(1))
    .when(col("has_beneficiario_clave") == True, lit(1))
    .otherwise(lit(3))
)

df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "tiempoTemporalidad",
    when((col("clase_pension") == 3) & 
         (col("PARENT_1") == 3) & 
         (col("PARENT_1") == 3),
         when(col("fec_nacimiento") > col("fec_nacimiento"),
              (25 - col("edad_beneficiario") + 1) * 12
         ).otherwise(
              (25 - col("edad_beneficiario") + 1) * 12
         )
    ).when((col("clase_pension") == 3) & 
           (col("PARENT_1") == 3) & 
           col("PARENT_1").isNull(),
           (25 - col("edad_beneficiario") + 1) * 12
    ).when((col("clase_pension") == 3) & (
           ((col("PARENT_1") == 7) & (col("PARENT_1") == 7)) |
           ((col("PARENT_1") == 7) & col("PARENT_1").isNull())),
        when((year(to_date(lit(FEC_CORTE), "yyyy-MM-dd")) - year(df_acu_personas_naturales["fec_muerte"])) == 20,
             floor(240 - months_between(to_date(lit(FEC_CORTE), "yyyy-MM-dd"), df_acu_personas_naturales["fec_muerte"]))
        ).otherwise(
             (20 - (year(to_date(lit(FEC_CORTE), "yyyy-MM-dd")) - year(df_acu_personas_naturales["fec_muerte"]))) * 12
        )
    ).when((col("clase_pension") == 3) & (
           ((col("PARENT_1") == 3) & (col("PARENT_1") == 7)) |
           ((col("PARENT_1") == 7) & (col("PARENT_1") == 3))),
        when(col("PARENT_1") == 3,
             (25 - col("edad_beneficiario") + 1) * 12
        ).otherwise(
             (25 - col("edad_beneficiario") + 1) * 12
        ).alias("temporalidad1")
    )
)

df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "temporalidad1",
    when((col("clase_pension") == 3) &
         (((col("PARENT_1") == 3) & (col("PARENT_1") == 7)) |
          ((col("PARENT_1") == 7) & (col("PARENT_1") == 3))),
         when(col("PARENT_1") == 3,
              (25 - col("edad_beneficiario") + 1) * 12
         ).otherwise(
              (25 - col("edad_beneficiario") + 1) * 12
         )
    )
)

df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "temporalidad2",
    when((col("clase_pension") == 3) &
         (((col("PARENT_1") == 3) & (col("PARENT_1") == 7)) |
          ((col("PARENT_1") == 7) & (col("PARENT_1") == 3))),
         (20 - (year(to_date(lit(FEC_CORTE), "yyyy-MM-dd")) - year(df_acu_personas_naturales["fec_muerte"]))) * 12
    )
)

df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "tiempoTemporalidad",
    when((col("clase_pension") == 3) &
         (((col("PARENT_1") == 3) & (col("PARENT_1") == 7)) |
          ((col("PARENT_1") == 7) & (col("PARENT_1") == 3))),
         greatest(col("temporalidad1"), col("temporalidad2"))
    ).otherwise(col("tiempoTemporalidad"))  
)

df_campos_rvi_final = df_campos_rvi_final.withColumn(
    "observaciones",
    when(
        df_acu_personas_naturales["fec_muerte"].isNotNull() & col("PARAMETRO_DESLIZAMIENTO").isNotNull(),
        concat(lit("FMA "), df_acu_personas_naturales["fec_muerte"], lit(" DESLIZAMIENT 0"), col("PARAMETRO_DESLIZAMIENTO"))
    ).when(
        df_acu_personas_naturales["fec_muerte"].isNotNull(),
        concat(lit("FMA "), df_acu_personas_naturales["fec_muerte"])
    ).when(
        col("PARAMETRO_DESLIZAMIENTO").isNotNull(),
        concat(lit("DESLIZAMIENT 0"), col("PARAMETRO_DESLIZAMIENTO"))
    )
)
df_campos_rvi_final = df_campos_rvi_final.select(
    #df_prv_rentas_filtered["nid_rentas"],  #
    df_prv_rentas_filtered["nid_rentas"].alias("NO_POLIZA"),  #campo 3 *trm
    df_prv_rentas_filtered["fec_expedicion"].cast(DateType()).alias("VIG_INICIAL"),  #campo 4
    df_prv_rentas_filtered["nid_rentas"].alias("NO_SINIESTRO"),  #campo 5
    df_prv_rentas_filtered["fec_causacion"].cast(DateType()).alias("FECHA_CAUSACION_DERECHO"),  #campo 6
    df_prv_rentas_filtered["nid_numero_identificacion"].alias("NO_IDENTIF"),  #campo 10
    df_prv_rentas_filtered["num_mesadas"].alias("NO_MESADAS"),  #campo 20
    df_prv_rentas_filtered["pcj_pcl_inicial"].alias("GRADO_INVALIDEZ_INICIAL"),  #campo 22
    df_prv_rentas_filtered["fec_ultima_mod"].cast(DateType()).alias("FECHA_ULTIMA_MOD"),  #campo 23
    df_prv_rentas_filtered["pcj_pcl_actual"].alias("GRADO_INVALIDEZ_ACTUAL"),  #campo 24
    df_prv_rentas_filtered["pc_ad_exp"].alias("GASTOS"),  #campo 25
    df_prv_rentas_filtered["pc_ad_exp"].alias("GASTOS_PRIMA"),  #campo 53
    #df_prv_rentas_filtered["vlr_exceso_smmlv_ipc_exp"].alias("PARAMETRO_DESLIZAMIENTO"),  #campo 69
    df_campos_rvi_final["PARAMETRO_DESLIZAMIENTO"].alias("PARAMETRO_DESLIZAMIENTO"),
    df_prv_rentas_filtered["fec_expedicion"].cast(DateType()).alias("FECHA_EXPED_POLIZA"),  #campo 72
    df_prv_rentas_notas["COD_RAMO_SFC"].cast(IntegerType()).alias("RAMO"), #campo 1
    df_prv_rentas_notas["NUM_RADICADO_NOTA"].alias("NO_RAD_NOT_TEC"), #campo 50
    #df_prv_rentas_aprobadas_obp["inscripcion_mecanismo"].alias("INSCRIPCION_MECANISMO"), #campo 67 *trm
    df_campos_rvi_final["INSCRIPCION_MECANISMO"].alias("INSCRIPCION_MECANISMO"),
    df_prv_rentas_aprobadas_obp["fecha_inscripcion"].cast(DateType()).alias("FECHA_INSCRIPCION"), #campo 68
    df_prv_rentas_aprobadas_obp["cobertura"].alias("VALOR_COBERTURA"), #campo 71
    #df_prv_endosos_point["vlr_prima"].alias("PRIMA"), #campo 54 *trm
    df_campos_rvi_final["PRIMA"].alias("PRIMA"),
    df_resultados_beneficiario["TIPO_IDENTIF_1"].alias("TIPO_IDENTIF_1"), #campo  27
    df_resultados_beneficiario["NO_IDENTIF_1"].alias("NO_IDENTIF_1"), #campo 28
    df_resultados_beneficiario["ESTADO_1"].alias("ESTADO_1"), #campo 33 
    df_resultados_beneficiario["TIPO_IDENTIF_2"].alias("TIPO_IDENTIF_2"), #campo 35
    df_resultados_beneficiario["NO_IDENTIF_2"].alias("NO_IDENTIF_2"), #campo 36 
    df_resultados_beneficiario["ESTADO_2"].alias("ESTADO_2"), #campo 41
    df_resultados_beneficiario["TIPO_IDENTIF_3"].alias("TIPO_IDENTIF_3"), #campo 43
    df_resultados_beneficiario["NO_IDENTIF_3"].alias("NO_IDENTIF_3"), #campo 44
    df_resultados_beneficiario["ESTADO_3"].alias("ESTADO_3"), #campo 49
    df_acu_personas_naturales["fec_nacimiento"].cast(DateType()).alias("FECHA_NACIMIENTO"), #campo 12
    df_resultados_beneficiario["FECHA_NACIMIENTO_1"].cast(DateType()).alias("FECHA_NACIMIENTO_1"), #campo 30
    df_resultados_beneficiario["FECHA_NACIMIENTO_2"].cast(DateType()).alias("FECHA_NACIMIENTO_2"), #campo 38
    df_resultados_beneficiario["FECHA_NACIMIENTO_3"].cast(DateType()).alias("FECHA_NACIMIENTO_3"), #campo 46
    df_acu_personas_naturales["fec_muerte"].cast(DateType()).alias("FECHA_MUERTE_AFILIADO"), #campo 73
    df_cgr_equivalencia_tipos_id["cod_tipo_super"].alias("TIPO_IDENTIF"), #campo 9
    df_prv_rentas_filtered["cod_estado_persona"].alias("ESTADO"), #campo 15
    df_rva_reserva_matematica_transic_filtered["vlr_reserva_rv08"].alias("VALOR_RESERVA_RV08"), #campo 59
    df_rva_reserva_matematica_transic_filtered["vlr_reserva_rv89"].alias("VALOR_RESERVA_RV89"), #campo 60
    #df_prv_rentas_filtered["pcj_interes_tecnico"].alias("INTERES_TEC_PRIMA"), # campos 51 *trm case 
    df_campos_rvi_final["INTERES_TEC_PRIMA"].alias("INTERES_TEC_PRIMA"),
    #df_prv_rentas_filtered["cod_origen"].alias("ORIGEN_PENSION"), # campos 7 *trm case 
    df_campos_rvi_final["ORIGEN_PENSION"].alias("ORIGEN_PENSION"),
    #df_prv_beneficiarios["nid_numero_identificacion"].alias("NO_HIJOS"), # campos 16 *trm case 
    df_campos_rvi_final["NO_HIJOS"].alias("NO_HIJOS"),
    #df_prv_beneficiarios["nid_numero_identificacion"].alias("NO_HIJOS_INVALIDOS"), # campos 17 *trm case 
    df_campos_rvi_final["NO_HIJOS_INVALIDOS"].alias("NO_HIJOS_INVALIDOS"),
    #df_prv_beneficiarios["cod_parentesco"].alias("PARENT_1"), # campos 26 *trm case
    df_resultados_beneficiario["PARENT_1"].alias("PARENT_1"), # campos 26 *trm case 
    df_resultados_beneficiario["PARENT_2"].alias("PARENT_2"), # campos 34 *trm case 
    df_resultados_beneficiario["PARENT_3"].alias("PARENT_3"), # campos 42 *trm case 
    #df_acu_personas_naturales["genero"].alias("SEXO"), # campos 11 *trm case 
    df_campos_rvi_final["SEXO"].alias("SEXO"),
    df_resultados_beneficiario["SEXO_1"].alias("SEXO_1"), # campos 29 *trm case 
    df_resultados_beneficiario["SEXO_2"].alias("SEXO_2"), # campos 37 *trm case 
    df_resultados_beneficiario["SEXO_3"].alias("SEXO_3"), # campos 45 *trm case 
    #df_prv_beneficiarios["cod_estado_persona"].alias("ESTUDIOS_1"), # campos 32 *trm case 
    df_resultados_beneficiario["ESTUDIOS_1"].alias("ESTUDIOS_1"),
    df_resultados_beneficiario["ESTUDIOS_2"].alias("ESTUDIOS_2"), # campos 40 *trm case 
    df_resultados_beneficiario["ESTUDIOS_3"].alias("ESTUDIOS_3"), # campos 48 *trm case 
    df_campos_rvi_final["observaciones"].cast(StringType()).alias("OBSERVACIONES"), # campos 58 *trm case 
    #df_prv_rentas_filtered["nid_rentas"].alias("PAR_0"), # campos 61 *trm case 
    df_campos_rvi_final["PAR_0"].alias("PAR_0"),
    #df_rva_reserva_matematica_transic["vlr_reserva_rv89"].alias("PARMA"), # campos 62 *trm case 
    df_campos_rvi_final["PARMA"].alias("PARMA"),
    #df_rva_reserva_matematica_transic["vlr_reserva_rv89"].alias("POR_AMORT_ACUM"), # campos 63 *trm case
    df_campos_rvi_final["POR_AMORT_ACUM"].alias("POR_AMORT_ACUM"),
    #df_acu_personas_naturales["PRIMER_NOMBRE"].alias("NOMBRE"), # campos 13 *trm  regexp_replace
    df_campos_rvi_final["NOMBRE"].alias("NOMBRE"),
    df_resultados_beneficiario["NOMBRE_1"].alias("NOMBRE_1"), # campos 31 *trm  regexp_replace
    df_resultados_beneficiario["NOMBRE_2"].alias("NOMBRE_2"), # campos 39 *trm  regexp_replace
    df_resultados_beneficiario["NOMBRE_3"].alias("NOMBRE_3"), # campos 47 *trm  regexp_replace
    #df_prv_rentas_filtered["nid_rentas"].alias("TASA_CREC_PRIMA"), # campos 52 *trm  subquery
    df_campos_rvi_final["TASA_CREC_PRIMA"].alias("TASA_CREC_PRIMA"),
    df_campos_rvi_final["mesada"].alias("MESADA"), # campos 19 *trm  validar query
    df_campos_rvi_final["VALOR_AUXILIO_FUNERARIO"].alias("VALOR_AUXILIO_FUNERARIO") # campos 57 *trm     
)
    
df_campos_rvi_final = df_campos_rvi_final.withColumn("UNIDAD", lit(1))\
    .withColumn("PARENT", lit(1))\
    .withColumn("ESTUDIOS", lit(None).cast(StringType()))\
    .withColumn("INGRESO_BASE_LIQUIDACION", lit(None).cast(StringType()))\
    .withColumn("RETROACTIVO", lit(None).cast(StringType()))\
    .withColumn("FECHA_INICIAL_RENTA_DIFERIDA", lit(None).cast(StringType()))\
    .withColumn("VALOR_MESADA_BENEF_PERIO_DIFER", lit(None).cast(StringType()))\
    .withColumn("PORCENTAJE_EXC_PARMA", lit(None).cast(StringType()))\
    .withColumn("FEC_AVISO_SINIESTRO", lit(None).cast(StringType()))\
    .withColumn("UNIDAD_DE_CUENTA", lit(None).cast(StringType()))\
    .withColumn("FAIR_VALUE", lit(None).cast(StringType()))\
    .withColumn("INTERES_TECNICO_NIIF4", lit(None).cast(StringType()))


df_campos_rvi_final = df_campos_rvi_final.withColumn("PARAMETRO_DESLIZAMIENTO", lit(1.0).cast(DecimalType(28,15)))
    
#Add partition to table 
df_campos_rvi_final = df_campos_rvi_final.withColumn("PERIODO_PARTITION", date_format(df_campos_rvi_final["VIG_INICIAL"], "yyyy-MM"))
    

df_campos_rvi_final.filter((df_prv_rentas_filtered["nid_rentas"] == '153')).show()
#-----------------------------------------------------------------#
#--------------------- AssigN Struct -----------------------------#
#-----------------------------------------------------------------#
  
#Select final dataframe for gold stage
#Load df_reserva_rvi_gold with structType and data
column_names = [field.name for field in schema_reserva_rvi_gold]
df_campos_rvi_final = df_campos_rvi_final.select(*column_names)
    
logger.info(f"End df")
    
df_reserva_rvi_gold = spark.createDataFrame(df_campos_rvi_final.rdd, schema=schema_reserva_rvi_gold)
"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim, date_format, to_date, lit, min, max, when, first, count, substring, concat_ws, floor, months_between, year, round, least, trunc, round as spark_round, coalesce, concat, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, DoubleType, DecimalType
from pyspark.sql.window import Window

import sys, traceback, boto3, json


BUCKET_NAME = 'alfa-datalake-dev-gold'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
DB_NAME = 'ALFAPRD'
DB_SCHEMA = 'PRV'
PERIOD = '202501'
CERTIFICATE = 'False'
DB_CATALOG_GLUE = 'datalake-dev-silver-catalog-db'
TABLE_NAME_GOLD = 'RESERVA_RVI_GOLD'
FEC_CORTE = '2003-12-29'
FEC_CORTE_DATE = '2024-11-07'

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")


schema_reserva_arp_gold = StructType([
            StructField("RAMO", IntegerType(), True),                         # campo 1 - VARCHAR2(3 BYTE)
            StructField("UNIDAD", IntegerType(), True),                       # campo 2 - N/A
            StructField("NO_POLIZA", DecimalType(10,0), True),                   # campo 3 - NUMBER(10,0)
            StructField("VIG_INICIAL", DateType(), True),                    # campo 4
            StructField("NO_SINIESTRO", DecimalType(10,0), True),               # campo 5 - NUMBER(10,0)
            StructField("FECHA_CAUSACION_DERECHO", DateType(), True),       # campo 6
            StructField("ORIGEN_PENSION", IntegerType(), True),              # campo 7 - VARCHAR2(3 BYTE)
            StructField("PARENT", IntegerType(), True),                       # campo 8 - N/A
            StructField("TIPO_IDENTIF", StringType(), True),                # campo 9 - VARCHAR2(2 BYTE)
            StructField("NO_IDENTIF", DecimalType(15,0), True),                     # campo 10 - NUMBER(15,0)
            StructField("SEXO", IntegerType(), True),                         # campo 11 - CHAR(1 BYTE)
            StructField("FECHA_NACIMIENTO", DateType(), True),              # campo 12
            StructField("NOMBRE", StringType(), True),                      # campo 13 - VARCHAR2(80 BYTE)
            StructField("ESTUDIOS", StringType(), True),                    # campo 14 - N/A
            StructField("ESTADO", StringType(), True),                      # campo 15 - VARCHAR2(3 BYTE)
            StructField("NO_HIJOS", IntegerType(), True),                      # campo 16 - NUMBER(15,0)
            StructField("NO_HIJOS_INVALIDOS", IntegerType(), True),            # campo 17 - NUMBER(15,0)
            StructField("INGRESO_BASE_LIQUIDACION", StringType(), True),    # campo 18 - N/A
            StructField("MESADA", DecimalType(16,2), True),                      # campo 19 - N/A
            StructField("NO_MESADAS", DecimalType(2,0), True),                 # campo 20 - NUMBER(2,0)
            StructField("RETROACTIVO", StringType(), True),                 # campo 21 - N/A
            StructField("GRADO_INVALIDEZ_INICIAL", DecimalType(5,2), True),      # campo 22 - NUMBER(5,2)
            StructField("FECHA_ULTIMA_MOD", DateType(), True),              # campo 23
            StructField("GRADO_INVALIDEZ_ACTUAL", DecimalType(5,2), True),       # campo 24 - NUMBER(5,2)
            StructField("GASTOS", DecimalType(5,4), True),                      # campo 25 - NUMBER(5,4)
            StructField("PARENT_1", IntegerType(), True),                    # campo 26 - VARCHAR2(3 BYTE)
            StructField("TIPO_IDENTIF_1", StringType(), True),              # campo 27 - VARCHAR2(3 BYTE)
            StructField("NO_IDENTIF_1", DecimalType(15,0), True),                  # campo 28 - NUMBER(15,0)
            StructField("SEXO_1", IntegerType(), True),                      # campo 29 - CHAR(1 BYTE)
            StructField("FECHA_NACIMIENTO_1", DateType(), True),           # campo 30
            StructField("NOMBRE_1", StringType(), True),                    # campo 31 - VARCHAR2(80 BYTE)
            StructField("ESTUDIOS_1", IntegerType(), True),                  # campo 32 - N/A
            StructField("ESTADO_1", StringType(), True),                    # campo 33 - VARCHAR2(3 BYTE)
            StructField("PARENT_2", IntegerType(), True),                    # campo 34 - VARCHAR2(3 BYTE)
            StructField("TIPO_IDENTIF_2", StringType(), True),              # campo 35 - VARCHAR2(3 BYTE)
            StructField("NO_IDENTIF_2", DecimalType(15,0), True),                  # campo 36 - NUMBER(15,0)
            StructField("SEXO_2", IntegerType(), True),                      # campo 37 - CHAR(1 BYTE)
            StructField("FECHA_NACIMIENTO_2", DateType(), True),           # campo 38
            StructField("NOMBRE_2", StringType(), True),                    # campo 39 - VARCHAR2(80 BYTE)
            StructField("ESTUDIOS_2", IntegerType(), True),                  # campo 40 - N/A
            StructField("ESTADO_2", StringType(), True),                    # campo 41 - VARCHAR2(3 BYTE)
            StructField("PARENT_3", IntegerType(), True),                    # campo 42 - VARCHAR2(3 BYTE)
            StructField("TIPO_IDENTIF_3", StringType(), True),              # campo 43 - VARCHAR2(3 BYTE)
            StructField("NO_IDENTIF_3", DecimalType(15,0), True),                  # campo 44 - NUMBER(15,0)
            StructField("SEXO_3", IntegerType(), True),                      # campo 45 - CHAR(1 BYTE)
            StructField("FECHA_NACIMIENTO_3", DateType(), True),           # campo 46
            StructField("NOMBRE_3", StringType(), True),                    # campo 47 - VARCHAR2(80 BYTE)
            StructField("ESTUDIOS_3", IntegerType(), True),                  # campo 48 - N/A
            StructField("ESTADO_3", StringType(), True),                    # campo 49 - VARCHAR2(3 BYTE)
            StructField("NO_RAD_NOT_TEC", DecimalType(20,0), True),               # campo 50 - NUMBER(20,0)
            StructField("INTERES_TEC_PRIMA", DecimalType(19,10), True),          # campo 51 - NUMBER(15,10)
            StructField("TASA_CREC_PRIMA", DecimalType(5,2), True),             # campo 52 - NUMBER(5,2)
            StructField("GASTOS_PRIMA", DecimalType(5,4), True),               # campo 53 - NUMBER(5,4)
            StructField("PRIMA", DecimalType(30,2), True),                        # campo 54 - NUMBER(17,2)
            StructField("FECHA_INICIAL_RENTA_DIFERIDA", StringType(), True), # campo 55 - N/A
            StructField("VALOR_MESADA_BENEF_PERIO_DIFER", StringType(), True), # campo 56 - N/A
            StructField("VALOR_AUXILIO_FUNERARIO", DecimalType(14,2), True),    # campo 57 - N/A
            StructField("OBSERVACIONES", StringType(), True),              # campo 58 - N/A
            StructField("VALOR_RESERVA_RV08", DecimalType(14,2), True),          # campo 59 - NUMBER(14,2)
            StructField("VALOR_RESERVA_RV89", DecimalType(14,2), True),          # campo 60 - NUMBER(14,2)
            StructField("PAR_0",DecimalType(28,2), True),                      # campo 61 - N/A
            StructField("PARMA", DecimalType(28,2), True),                      # campo 62 - N/A
            StructField("PORCENTAJE_EXC_PARMA", StringType(), True),      # campo 63 - N/A
            StructField("POR_AMORT_ACUM", DecimalType(34,2), True),              # campo 64 - NUMBER(5,2)
            StructField("FEC_AVISO_SINIESTRO", StringType(), True),       # campo 65 - N/A
            StructField("FAIR_VALUE", StringType(), True),                 # campo 66 - N/A
            StructField("INSCRIPCION_MECANISMO", StringType(), True),      # campo 67 - VARCHAR2(1 BYTE)
            StructField("FECHA_INSCRIPCION", DateType(), True),            # campo 68
            StructField("PARAMETRO_DESLIZAMIENTO", DecimalType(28,15), True),    # campo 69 - NUMBER(20,15)
            StructField("TASA_FAC_SEG", DecimalType(10,2), True),               # campo 70 - PARAMETERS
            StructField("VALOR_COBERTURA", DecimalType(14,2), True),             # campo 71 - NUMBER(14,2)
            StructField("FECHA_EXPED_POLIZA", DateType(), True),           # campo 72
            StructField("FECHA_MUERTE_AFILIADO", DateType(), True),        # campo 73
            StructField("INTERES_TECNICO_NIIF4", StringType(), True),      # campo 74 - NUMBER(15,10)
            StructField("UNIDAD_DE_CUENTA", StringType(), True),            # campo 75 - N/A
            StructField("PERIODO_PARTITION", StringType(), True)            # campo 76 - partition
        ])

################################################################################################################### source

#Read tables from silver arpprod
df_hist_datos_invalidez_muerte = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_HIST_DATOS_INVALIDEZ_MUERTE").toDF()
df_hist_sar_pcl_pension = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_HIST_SAR_PCL_PENSION").toDF()
df_ind_datos_reservas_siniestros = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_IND_DATOS_RESERVAS_SINIESTROS").toDF()
df_par_trabajadores = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_PAR_TRABAJADORES").toDF()
df_sar_auditoria_medica = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_AUDITORIA_MEDICA").toDF()
df_sar_beneficiarios_siniestros = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_BENEFICIARIOS_SINIESTROS").toDF()
df_sar_datos_invalidez_muerte = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_DATOS_INVALIDEZ_MUERTE").toDF()
df_sar_reserva_pcj_amortizacion = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_RESERVA_PCJ_AMORTIZACION").toDF()
df_sar_siniestros = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_SINIESTROS").toDF()
df_sar_pagos_coberturas  = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_PAGOS_COBERTURAS").toDF()
df_sar_reserva_pcj_amortizacion  = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_ARPPROD_SILVER_SAR_RESERVA_PCJ_AMORTIZACION").toDF()


#Read tables from silver general
df_acu_personas_naturales = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ACU_PERSONAS_NATURALES").toDF()
df_cgr_equivalencia_tipos_id = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_EQUIVALENCIA_TIPOS_ID").toDF()	
df_anp_ipc = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_IPC").toDF()
df_rva_reserva_matematica_transic = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_RVA_RESERVA_MATEMATICA_TRANSIC").toDF()
df_cgr_origenes = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_ORIGENES").toDF()
df_pgr_salarios_minimos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_PGR_SALARIOS_MINIMOS").toDF()

################################################################################################################### variable y alias 
# Convertir string a formato fecha y luego a "yyyymm"
fec_corte_yyyymm = date_format(to_date(lit(FEC_CORTE), "yyyy-MM-dd"), "yyyyMM")

################################################################################################################### filter source
df_sar_datos_invalidez_muerte = df_sar_datos_invalidez_muerte.withColumn(
    "anio_aprobacion", year(df_sar_datos_invalidez_muerte["fec_aprobacion_pension"])
)

df_sar_auditoria_medica = df_sar_auditoria_medica.filter(
    col("cod_concepto_medico_am").isin("4", "5")
)

df_sar_datos_invalidez_muerte = df_sar_datos_invalidez_muerte.filter(
    (col("cod_estado_reserva") == "1") &
    (col("fec_aprobacion_pension") <= to_date(lit("2025-04-16")))
)

# Renombrar columnas para evitar ambigüedades
df_salarios_min = df_pgr_salarios_minimos.withColumnRenamed("ANO", "ANIO_SAL") \
                                 .withColumnRenamed("VLR_SALARIO_MINIMO", "VLR_SMIN")

df_salarios_min = df_salarios_min.filter(col("MES") == 1)


df_filter_rva_reserva_matematica_transic = df_rva_reserva_matematica_transic.filter(
        (df_rva_reserva_matematica_transic["ramo"] == "ARP" )  )
        
        
df_sar_auditoria_medica = df_sar_auditoria_medica.select(
    when(df_sar_auditoria_medica["cod_concepto_medico_am"] == "4", "2").when(df_sar_auditoria_medica["cod_concepto_medico_am"] == "5", "1").otherwise(None).alias("cod_origen"),
    df_sar_auditoria_medica["nid_siniestro"],
    df_sar_auditoria_medica["cod_concepto_medico_am"],
    df_sar_auditoria_medica["nid_auditoria_medica"],
    df_sar_auditoria_medica["fec_registra"],
    df_sar_auditoria_medica["fec_registra_partition"]
) 


window_spec = Window.partitionBy("nid_siniestro").orderBy("cod_parentesco")

df_sar_beneficiarios_siniestros = df_sar_beneficiarios_siniestros.withColumn("orden", row_number().over(window_spec))

# Filtrar df_sar_pagos_coberturas
df_filtrado_pagos_coberturas = df_sar_pagos_coberturas.filter(
    (col("cod_cobertura") == 17) &
    (date_format("fec_pago", "yyyyMM") == fec_corte_yyyymm)
).select("nid_siniestro", "vlr_pago")

  

################################################################################################################### Add  column source
df_aggregated_hist_datos_invalidez_muerte = (
        df_hist_datos_invalidez_muerte.groupBy(
            df_hist_datos_invalidez_muerte["nid_siniestro"]
).agg(
        min(df_hist_datos_invalidez_muerte["nid_hist_datos_inv_mte"]).alias("min_nid_hist_datos_inv_mte"))  # Take max id with groupBy
)

df_aggregated_hist_datos_invalidez_muerte = df_aggregated_hist_datos_invalidez_muerte.withColumnRenamed("nid_siniestro", "nid_siniestro_group") 

df_aggregated_hist_datos_invalidez_muerte = df_aggregated_hist_datos_invalidez_muerte.join(
    df_hist_datos_invalidez_muerte,
    df_aggregated_hist_datos_invalidez_muerte["min_nid_hist_datos_inv_mte"] == df_hist_datos_invalidez_muerte["nid_hist_datos_inv_mte"],
    "inner"
)
#--
# Filtro en auditoría
df_auditoria_filtrada = df_sar_auditoria_medica.filter(df_sar_auditoria_medica["cod_concepto_medico_am"] == "4")

df_hist_sar_pcl_pension = df_hist_sar_pcl_pension.withColumnRenamed("fec_registra", "fec_registra_pcl")

# Join entre los dos
df_joined = df_hist_sar_pcl_pension.join(
    df_auditoria_filtrada,
    on="nid_siniestro",
    how="inner"
)

# Definir partición por nid_siniestro y ordenar por fecha
window_asc = Window.partitionBy("nid_siniestro").orderBy(df_hist_sar_pcl_pension["fec_registra_pcl"])
window_desc = Window.partitionBy("nid_siniestro").orderBy(df_hist_sar_pcl_pension["fec_registra_pcl"].desc())

# Agregar las columnas usando funciones de ventana
df_first_hist_sar_pcl_pension = df_joined.select(
    col("nid_siniestro"),
    first("pcj_pcl").over(window_asc).alias("primero"),
    first("pcj_pcl").over(window_desc).alias("ultimo"),
    first("fec_registra_pcl").over(window_desc).alias("ultima_modificacion")
).dropDuplicates(["nid_siniestro", "primero", "ultimo", "ultima_modificacion"])

df_hijos = df_sar_beneficiarios_siniestros.filter(
    (df_sar_beneficiarios_siniestros["cod_parentesco"] == "4") &
     (df_sar_beneficiarios_siniestros["bln_con_derecho"] == "S")
    ).groupBy("nid_siniestro").agg(
        count("*").alias("NO_HIJOS")
        ).withColumn(
            "NO_HIJOS",
            when(col("NO_HIJOS") == 0, None)
            .otherwise(col("NO_HIJOS"))
            )

conteo_hijos_invalidos = df_sar_beneficiarios_siniestros.filter(
    (df_sar_beneficiarios_siniestros["cod_parentesco"] == "4") &
    (df_sar_beneficiarios_siniestros["bln_con_derecho"] == "S") &
    (df_sar_beneficiarios_siniestros["cod_estado_persona"] == "2")
    ).groupBy("nid_siniestro").agg(
        count("*").alias("NO_HIJOS_INVALIDOS")
        ).withColumn(
            "NO_HIJOS_INVALIDOS",
            when(col("NO_HIJOS_INVALIDOS") == 0, None).otherwise(col("NO_HIJOS_INVALIDOS"))
            )


df_existentes_amortizacion = df_sar_reserva_pcj_amortizacion.select("nid_siniestro").dropDuplicates()

df_siniestros_with_flag = df_sar_siniestros.join(
    df_existentes_amortizacion.withColumn("existe", lit(True)),
    on="nid_siniestro",
    how="left"
    ).withColumn(
        "existe",
        when(col("existe").isNull(), lit(False)).otherwise(col("existe"))
    )
sar = df_sar_siniestros.alias("sar")
tra = df_par_trabajadores.alias("tra")
inv = df_sar_datos_invalidez_muerte.alias("inv")
aud = df_sar_auditoria_medica.alias("aud")
res = df_ind_datos_reservas_siniestros.alias("res")
ben = df_sar_beneficiarios_siniestros.alias("ben")
pcl = df_first_hist_sar_pcl_pension.alias("pcl")
mat = df_filter_rva_reserva_matematica_transic.alias("mat")
his = df_aggregated_hist_datos_invalidez_muerte.alias("his")
ben_apn = df_acu_personas_naturales.alias("ben_apn")
tra_apn = df_acu_personas_naturales.alias("tra_apn")
ben_eqid = df_cgr_equivalencia_tipos_id.alias("ben_eqid")
tra_eqid = df_cgr_equivalencia_tipos_id.alias("tra_eqid")
org = df_cgr_origenes.alias("org")
sal = df_salarios_min.alias("sal")
ipc = df_anp_ipc.alias("ipc")
amo = df_siniestros_with_flag.alias("amo") 
hjs = df_hijos.alias("hjs")
hjv = conteo_hijos_invalidos.alias("hjv")
cov = df_filtrado_pagos_coberturas.alias("cov")
################################################################################################################### Add table stage

df_resultados_beneficiario = sar.join(tra, sar["nid_trabajador"] == tra["nid_trabajador"], "inner") \
    .join(inv, sar["nid_siniestro"] == inv["nid_siniestro"], "inner") \
    .join(aud, inv["nid_auditoria_medica"] == aud["nid_auditoria_medica"], "inner") \
    .join(res, sar["nid_siniestro"] == res["nid_siniestro"], "inner") \
    .join(ben, sar["nid_siniestro"] == ben["nid_siniestro"], "left") \
    .join(ben_apn, (ben["COD_TIPO_IDENTIFICACION"] == ben_apn["COD_TIPO_IDENTIFICACION"]) &
                   (ben["NID_NUMERO_IDENTIFICACION"] == ben_apn["NID_NUMERO_IDENTIFICACION"]), "left") \
    .join(ben_eqid, ben["cod_tipo_identificacion"] == ben_eqid["cod_tipo_documento"], "left") \
    

# campos 
# Agregamos la columna "NOMBRE_1"
df_resultados_beneficiario = df_resultados_beneficiario.withColumn("NOMBRE_1",
    substring(
        trim(
            concat_ws(
                " ",
                col("ben_apn.PRIMER_APELLIDO"),
                col("ben_apn.SEGUNDO_APELLIDO"),
                col("ben_apn.PRIMER_NOMBRE"),
                col("ben_apn.SEGUNDO_NOMBRE")
            )
        ),
        1,
        30
    )
)

df_resultados_beneficiario = df_resultados_beneficiario.withColumn("SEXO_1",
    when(col("ben_apn.genero") == "M", 1).when(col("ben_apn.genero") == "F", 2)
)

#campos 26, 34, 42
df_resultados_beneficiario = df_resultados_beneficiario.withColumn("PARENT_1",
when((col("ben.cod_parentesco") == "1") & (col("ben.bln_con_derecho") == "S"), lit(5))  # PADRE
.when((col("ben.cod_parentesco") == "2") & (col("ben.bln_con_derecho") == "S"), lit(6))  # MADRE
.when(
    (col("ben.cod_parentesco").isin("3", "10")) & (col("ben.bln_con_derecho") == "S"),
    when(
        floor(months_between(to_date(lit("29/12/2003"), "dd/MM/yyyy"), col("ben_apn.fec_nacimiento")) / 12) < 30,
        lit(7)  # Temporal
    ).otherwise(lit(4))  # Vitalicia
)
.when(
    (col("ben.cod_parentesco") == "4") & (col("ben.bln_con_derecho") == "S"),
    when(col("ben.cod_estado_persona") == "2", lit(2))  # HIJO INVÁLIDO
    .when(col("ben.cod_estado_persona") == "1", lit(3))  # HIJO VÁLIDO
)
.when((col("ben.cod_parentesco") == "6") & (col("ben.bln_con_derecho") == "S"), lit(8))  # HERMANO
.otherwise(None)
)

df_resultados_beneficiario = df_resultados_beneficiario.withColumn("ESTADO_1",
    when((col("ben_apn.fec_muerte").isNotNull()) |(col("aud.cod_origen")  == "1"), "3")
    .when(aud["cod_origen"] == "2", 2)
    .otherwise(1)
)

# campos 32, 40, 48
df_resultados_beneficiario = df_resultados_beneficiario.withColumn(
"ESTUDIOS_1",
when(
    (col("ben.cod_parentesco") == "4") & 
    (col("ben.cod_estado_persona") == "1") & 
    (floor(months_between(to_date(lit("29/12/2003"), "dd/MM/yyyy"), col("ben_apn.fec_nacimiento")) / 12) >= 18),
    lit(1)
    ).otherwise(None)
    )

# Pivot implícito (como en tu SQL)  df_sar_beneficiarios_siniestros
df_resultados_beneficiario = df_resultados_beneficiario.groupBy(sar["nid_siniestro"]).agg(
	max(when(df_sar_beneficiarios_siniestros["orden"] == 1, df_resultados_beneficiario["ESTADO_1"])).alias("ESTADO_1"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 1, df_resultados_beneficiario["ESTUDIOS_1"])).alias("ESTUDIOS_1"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 1, ben_apn["fec_nacimiento"])).alias("FECHA_NACIMIENTO_1"),
    max(when(df_sar_beneficiarios_siniestros["orden"] == 1, ben_eqid["COD_TIPO_SUPER"])).alias("TIPO_IDENTIF_1"),
    max(when(df_sar_beneficiarios_siniestros["orden"] == 1, ben["nid_numero_identificacion"])).alias("NO_IDENTIF_1"),
    max(when(df_sar_beneficiarios_siniestros["orden"] == 1, df_resultados_beneficiario["NOMBRE_1"])).alias("NOMBRE_1"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 1, df_resultados_beneficiario["PARENT_1"])).alias("PARENT_1"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 1, df_resultados_beneficiario["SEXO_1"])).alias("SEXO_1"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 2, df_resultados_beneficiario["ESTADO_1"])).alias("ESTADO_2"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 2, df_resultados_beneficiario["ESTUDIOS_1"])).alias("ESTUDIOS_2"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 2, ben_apn["fec_nacimiento"])).alias("FECHA_NACIMIENTO_2"),
    max(when(df_sar_beneficiarios_siniestros["orden"] == 2, ben_eqid["COD_TIPO_SUPER"])).alias("TIPO_IDENTIF_2"),
    max(when(df_sar_beneficiarios_siniestros["orden"] == 2, ben["nid_numero_identificacion"])).alias("NO_IDENTIF_2"),
    max(when(df_sar_beneficiarios_siniestros["orden"] == 2, df_resultados_beneficiario["NOMBRE_1"])).alias("NOMBRE_2"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 2, df_resultados_beneficiario["PARENT_1"])).alias("PARENT_2"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 2, df_resultados_beneficiario["SEXO_1"])).alias("SEXO_2"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 3, df_resultados_beneficiario["ESTADO_1"])).alias("ESTADO_3"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 3, df_resultados_beneficiario["ESTUDIOS_1"])).alias("ESTUDIOS_3"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 3, ben_apn["fec_nacimiento"])).alias("FECHA_NACIMIENTO_3"),
    max(when(df_sar_beneficiarios_siniestros["orden"] == 3, ben_eqid["COD_TIPO_SUPER"])).alias("TIPO_IDENTIF_3"),
    max(when(df_sar_beneficiarios_siniestros["orden"] == 3, ben["nid_numero_identificacion"])).alias("NO_IDENTIF_3"),
    max(when(df_sar_beneficiarios_siniestros["orden"] == 3, df_resultados_beneficiario["NOMBRE_1"])).alias("NOMBRE_3"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 3, df_resultados_beneficiario["PARENT_1"])).alias("PARENT_3"),
	max(when(df_sar_beneficiarios_siniestros["orden"] == 3, df_resultados_beneficiario["SEXO_1"])).alias("SEXO_3")
    )
df_resultados_beneficiario = df_resultados_beneficiario.withColumnRenamed("nid_siniestro", "nid_siniestro_orden")

res_ben = df_resultados_beneficiario.alias("res_ben")  
df_join_principal_reserva_arp = sar.join(tra, sar["nid_trabajador"] == tra["nid_trabajador"], "inner") \
    .join(inv, sar["nid_siniestro"] == inv["nid_siniestro"], "inner") \
    .join(aud, inv["nid_auditoria_medica"] == aud["nid_auditoria_medica"], "inner") \
    .join(res, sar["nid_siniestro"] == res["nid_siniestro"], "inner") \
    .join(pcl, sar["nid_siniestro"] == pcl["nid_siniestro"], "left") \
    .join(mat, sar["nid_siniestro"] == mat["nid_siniestro_renta"], "left") \
    .join(amo, mat["nid_siniestro_renta"] == amo["nid_siniestro"], "left") \
    .join(his, sar["nid_siniestro"] == his["nid_siniestro"], "left") \
    .join(tra_apn, (tra["COD_TIPO_IDENTIFICACION"] == tra_apn["COD_TIPO_IDENTIFICACION"]) &
                (tra["NID_NUMERO_IDENTIFICACION"] == tra_apn["NID_NUMERO_IDENTIFICACION"]), "left") \
    .join(tra_eqid, tra["cod_tipo_identificacion"] == tra_eqid["cod_tipo_documento"], "left") \
    .join(org, aud["cod_origen"] == org["cod_origen"], "left") \
    .join(hjs, sar["nid_siniestro"] == hjs["nid_siniestro"], "left") \
    .join(hjv, sar["nid_siniestro"] == hjv["nid_siniestro"], "left") \
    .join(ipc, inv["anio_aprobacion"] == ipc["anio"], "left") \
    .join(sal, inv["anio_aprobacion"] == sal["ANIO_SAL"], "left") \
    .join(cov, sar["nid_siniestro"] == cov["nid_siniestro"], "left") \
    .join(res_ben, sar["nid_siniestro"] == res_ben["nid_siniestro_orden"], "left")
# Agregamos la columna "NOMBRE"
df_campos_arp_final = df_join_principal_reserva_arp.withColumn(
    "NOMBRE",
    substring(
        trim(
            concat_ws(
                " ",
                col("tra_apn.PRIMER_APELLIDO"),
                col("tra_apn.SEGUNDO_APELLIDO"),
                col("tra_apn.PRIMER_NOMBRE"),
                col("tra_apn.SEGUNDO_NOMBRE")
            )
        ),
        1,
        30
    )
)

#campo 11
df_campos_arp_final = df_campos_arp_final.withColumn("SEXO",
    when(col("tra_apn.genero") == "M", 1).when(col("tra_apn.genero") == "F", 2)
)

# campos 22, 23, 24 .. case 
df_campos_arp_final = df_campos_arp_final \
.withColumn(
    "grado_invalidez_inicial",
    when(
        (col("pcl.nid_siniestro").isNull()) | (col("pcl.primero") == col("pcl.ultimo")),
        None
    ).otherwise(col("pcl.primero"))
) \
.withColumn(
    "FECHA_ULTIMA_MOD",
    when(
        (col("pcl.nid_siniestro").isNull()) | (col("pcl.primero") == col("pcl.ultimo")),
        None
    ).otherwise(col("pcl.ultima_modificacion"))
) \
.withColumn(
    "grado_invalidez_actual",
    when(
        (col("pcl.nid_siniestro").isNull()) | (col("pcl.primero") == col("pcl.ultimo")),
        None
    ).otherwise(col("pcl.ultimo"))
)

# campos 61
df_campos_arp_final = df_campos_arp_final.withColumn(
    "PAR_0",
    when(
        (col("amo.existe")) & (col("mat.vlr_reserva_rv08") != 0),
        round((col("mat.vlr_reserva_rv89") / col("mat.vlr_reserva_rv08")) * 100, 2)
    ).otherwise(None)
)

# campos 62
df_campos_arp_final = df_campos_arp_final.withColumn(
    "PARMA",
    when(
        (col("amo.existe")) & (col("mat.vlr_reserva_rv08") != 0),
        round(100 - round((col("mat.vlr_reserva_rv89") / col("mat.vlr_reserva_rv08")) * 100, 2)/240, 10)
    ).otherwise(None)
    )

# campo 63 
fec_corte = "20240430"  # <-- Actualiza si es necesario
fec_corte_dt = to_date(lit(fec_corte), "yyyyMMdd")
fecha_base_dt = to_date(lit(fec_corte), "yyyyMMdd")

df_campos_arp_final = df_campos_arp_final.withColumn(
    "POR_AMORT_ACUM",
    when(
        (col("amo.existe")) & (col("mat.vlr_reserva_rv08") != 0),
        least(
            spark_round(
                spark_round((col("mat.vlr_reserva_rv89") / col("mat.vlr_reserva_rv08")) * 100, 2) +
                (
                    spark_round(
                        (100 - spark_round((col("mat.vlr_reserva_rv89") / col("mat.vlr_reserva_rv08")) * 100, 2)) / 240,
                        10
                    ) * (floor(months_between(fec_corte_dt, fecha_base_dt)) + lit(91))
                ),
                0
            ),
            lit(100)
        )
    ).otherwise(None)
)


#campo 15
df_campos_arp_final = df_campos_arp_final.withColumn("ESTADO",
    when((col("tra_apn.fec_muerte").isNotNull()) |(col("aud.cod_origen") == "1"), "3")
    .when(aud["cod_origen"] == "2", 2)
    .otherwise(1)
)

# campo 20 no_mesada 
# Condiciones para asignar NO_MESADA
df_campos_arp_final = df_campos_arp_final.withColumn(
    "NO_MESADAS",
    when(col("his.FEC_DICTAMEN_INVMTE") <= lit("2005-07-25"), 14)
    .when(
        (col("his.FEC_DICTAMEN_INVMTE") > lit("2005-07-25")) & (col("his.FEC_DICTAMEN_INVMTE") <= lit("2011-07-31")),
        when(col("his.VLR_PENSION") <= 3 * col("sal.VLR_SMIN"), 14).otherwise(13)
    )
    .otherwise(13)  # Valor por defecto si no cae en los rangos
)

# En lugar de crear "vlr_auxilio_funerarios", crea directamente "VALOR_AUXILIO_FUNERARIO"
df_campos_arp_final = df_campos_arp_final.withColumn(
    "VALOR_AUXILIO_FUNERARIO",
    coalesce(
        col("vlr_pago"),
        when(col("org.COD_ORIGEN_SUPER").isin(1, 3),
            when(col("inv.vlr_pension") <= col("VLR_SMIN") * 5,
                round(col("VLR_SMIN") * 5, 0))
            .when(col("inv.vlr_pension") <= col("VLR_SMIN") * 10,
                round(col("inv.vlr_pension"), 0))
            .otherwise(round(col("VLR_SMIN") * 10, 0))
        ).otherwise(lit(0))
    ).cast(DecimalType(14, 2))  # Aquí aplicas el cast directamente
)
df_campos_arp_final = df_campos_arp_final.select(
    tra["nid_afiliacion"].alias("NO_POLIZA"),  # campo 3
    inv["fec_aprobacion_pension"].cast(DateType()).alias("VIG_INICIAL"),     # campo 4
    sar["nid_siniestro"].alias("NO_SINIESTRO"),             # campo 5
    sar["fec_siniestro"].cast(DateType()).alias("FECHA_CAUSACION_DERECHO"),  # campo 6
    org["cod_origen_super"].cast(IntegerType()).alias("ORIGEN_PENSION"),        # campo 7
    ben_eqid["cod_tipo_super"].cast(StringType()).alias("TIPO_IDENTIF"),           # campo 9
    tra["NID_NUMERO_IDENTIFICACION"].alias("NO_IDENTIF"),              # campo 10
    df_campos_arp_final["SEXO"].alias("SEXO"),                       # campo 11 * tra_apn
    col("tra_apn.fec_nacimiento").cast(DateType()).alias("FECHA_NACIMIENTO"),    # campo 12  *tra_apn
    df_campos_arp_final["NOMBRE"].alias("NOMBRE"),               # campo 13
    df_campos_arp_final["ESTADO"].alias("ESTADO"),          # campo 15
    hjs["NO_HIJOS"].cast(IntegerType()).alias("NO_HIJOS"),                # campo 16
    hjv["NO_HIJOS_INVALIDOS"].cast(IntegerType()).alias("NO_HIJOS_INVALIDOS"),      # campo 17
    res["vrl_ibl_manual"].cast(StringType()).alias("INGRESO_BASE_LIQUIDACION"),# campo 18
    inv["vlr_pension"].alias("MESADA"),                     # campo 19
    df_campos_arp_final["NO_MESADAS"].cast(DecimalType(2,0)).alias("NO_MESADAS"),               # campo 20
    df_campos_arp_final["GRADO_INVALIDEZ_INICIAL"].alias("GRADO_INVALIDEZ_INICIAL"),        # campo 22
    df_campos_arp_final["FECHA_ULTIMA_MOD"].cast(DateType()).alias("FECHA_ULTIMA_MOD"),   # campo 23
    df_campos_arp_final["GRADO_INVALIDEZ_ACTUAL"].alias("GRADO_INVALIDEZ_ACTUAL"),          # campo 24
    df_resultados_beneficiario["PARENT_1"].alias("PARENT_1"),                # campo 26
    df_resultados_beneficiario["TIPO_IDENTIF_1"].alias("TIPO_IDENTIF_1"),         # campo 27
    df_resultados_beneficiario["NO_IDENTIF_1"].alias("NO_IDENTIF_1"), # campo 28
    df_resultados_beneficiario["SEXO_1"].alias("SEXO_1"),                      # campo 29
    df_resultados_beneficiario["FECHA_NACIMIENTO_1"].cast(DateType()).alias("FECHA_NACIMIENTO_1"),  # campo 30
    df_resultados_beneficiario["NOMBRE_1"].alias("NOMBRE_1"),             # campo 31
    df_resultados_beneficiario["ESTUDIOS_1"].alias("ESTUDIOS_1"),              # campo 32
    df_resultados_beneficiario["ESTADO_1"].alias("ESTADO_1"),            # campo 33
    df_resultados_beneficiario["PARENT_2"].alias("PARENT_2"),                # campo 34
    df_resultados_beneficiario["TIPO_IDENTIF_2"].alias("TIPO_IDENTIF_2"),         # campo 35
    df_resultados_beneficiario["NO_IDENTIF_2"].alias("NO_IDENTIF_2"), # campo 36
    df_resultados_beneficiario["SEXO_2"].alias("SEXO_2"),                      # campo 37
    df_resultados_beneficiario["FECHA_NACIMIENTO_2"].cast(DateType()).alias("FECHA_NACIMIENTO_2"),  # campo 38
    df_resultados_beneficiario["NOMBRE_2"].alias("NOMBRE_2"),             # campo 39
    df_resultados_beneficiario["ESTUDIOS_2"].alias("ESTUDIOS_2"),              # campo 40
    df_resultados_beneficiario["ESTADO_2"].alias("ESTADO_2"),            # campo 41
    df_resultados_beneficiario["PARENT_3"].alias("PARENT_3"),                # campo 42
    df_resultados_beneficiario["TIPO_IDENTIF_3"].alias("TIPO_IDENTIF_3"),         # campo 43
    df_resultados_beneficiario["NO_IDENTIF_3"].alias("NO_IDENTIF_3"), # campo 44
    df_resultados_beneficiario["SEXO_3"].alias("SEXO_3"),                      # campo 45
    df_resultados_beneficiario["FECHA_NACIMIENTO_3"].cast(DateType()).alias("FECHA_NACIMIENTO_3"),  # campo 46
    df_resultados_beneficiario["NOMBRE_3"].alias("NOMBRE_3"),             # campo 47
    df_resultados_beneficiario["ESTUDIOS_3"].alias("ESTUDIOS_3"),              # campo 48
    df_resultados_beneficiario["ESTADO_3"].alias("ESTADO_3"),            # campo 49
    #tas["pcj_ipc"].alias("TASA_CREC_PRIMA"),          # campo 52
    df_campos_arp_final["pcj_ipc"].alias("TASA_CREC_PRIMA"),          # campo 52
    #col("df_campos_arp_final.pcj_ipc").alias("TASA_CREC_PRIMA"),          # campo 52
    #df_campos_arp_final["VALOR_AUXILIO_FUNERARIO"].cast(DecimalType(14,2)).alias("VALOR_AUXILIO_FUNERARIO"),  # campo 57
    df_campos_arp_final["VALOR_AUXILIO_FUNERARIO"].alias("VALOR_AUXILIO_FUNERARIO"),
    mat["vlr_reserva_rv08"].alias("VALOR_RESERVA_RV08"),    # campo 59
    mat["vlr_reserva_rv89"].alias("VALOR_RESERVA_RV89"),    # campo 60
    df_campos_arp_final["PAR_0"].cast(DecimalType(28,2)).alias("PAR_0"),                    # campo 61
    df_campos_arp_final["PARMA"].cast(DecimalType(28,2)).alias("PARMA"),                     # campo 62
    df_campos_arp_final["POR_AMORT_ACUM"].alias("POR_AMORT_ACUM"),           # campo 64
    inv["fec_aprobacion_pension"].cast(DateType()).alias("FECHA_EXPED_POLIZA"), # campo72
    col("tra_apn.fec_muerte").cast(DateType()).alias("FECHA_MUERTE_AFILIADO")   # campo 73 * tra_apn
)
num_filas = df_campos_arp_final.count()
print(f"Número de filas: {num_filas}")
num_filas = df_campos_arp_final.count()
print(f"Número de filas: {num_filas}")
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# Inicializar Spark
spark = SparkSession.builder.appName("Comparar CSV GOLD vs SILVER").getOrCreate()

# Parámetros S3
bucket_name = "alfa-niif-bronce-test"
input_key_gold = "ALFAPRD-UT/archivosJson generados/niif_reserva_matematica_gold-40.csv"
input_key_silver = "ALFAPRD-UT/archivosJson generados/niif_reserva_matematica_silver-40.csv"
output_json = "ALFAPRD-UT/archivosJson generados/resultado_reserva_matematica.json"
output_csv = "ALFAPRD-UT/archivosJson generados/resultado_reserva_matematica.csv"

# Leer archivos CSV
df_gold = spark.read.option("header", True).csv(f"s3://{bucket_name}/{input_key_gold}")
df_silver = spark.read.option("header", True).csv(f"s3://{bucket_name}/{input_key_silver}")

# Llave para comparar
columna_llave = "no_siniestro"

# Columnas a ignorar
columnas_ignoradas = ["unidad_de_cuenta", "interes_tecnico_niif4", "tasa_fac_seg", "fair_value", "observaciones", "valor_auxilio_funerario",  "mesada", "nombre", "nombre_1", "nombre_2", "nombre_3", "periodo_partition"]

# Columnas comunes sin ignoradas
columnas_comunes = [c for c in df_gold.columns if c in df_silver.columns and c not in columnas_ignoradas]

# Convertir todo a string
for c in columnas_comunes:
    df_gold = df_gold.withColumn(c, col(c).cast(StringType()))
    df_silver = df_silver.withColumn(c, col(c).cast(StringType()))

# Join con alias explícitos
df_gold_alias = df_gold.alias("gold")
df_silver_alias = df_silver.alias("silver")

df_join = df_gold_alias.join(
    df_silver_alias,
    on=col(f"gold.{columna_llave}") == col(f"silver.{columna_llave}"),
    how="left"
)

# Comparar campo a campo sin ambigüedad
diferencias = []
for row in df_join.select([col(f"gold.{columna_llave}").alias("no_siniestro")] + 
                          [col(f"gold.{c}").alias(f"gold_{c}") for c in columnas_comunes] + 
                          [col(f"silver.{c}").alias(f"silver_{c}") for c in columnas_comunes]).collect():
    siniestro = row["no_siniestro"]
    for c in columnas_comunes:
        val_gold = row[f"gold_{c}"]
        val_silver = row[f"silver_{c}"]
        if val_gold != val_silver:
            diferencias.append({
                "no_siniestro": siniestro,
                "campo": c,
                "valor_gold": val_gold,
                "valor_silver": val_silver
            })

# Guardar como JSON
s3 = boto3.client("s3")
s3.put_object(
    Bucket=bucket_name,
    Key=output_json,
    Body=json.dumps(diferencias, indent=4).encode("utf-8")
)

# Convertir diferencias a DataFrame y guardar como CSV
df_diferencias = spark.createDataFrame(diferencias)
df_diferencias.write.mode("overwrite").option("header", True).csv(f"s3://{bucket_name}/{output_csv}")

print(f"✅ Resultado guardado en:\n- JSON: s3://{bucket_name}/{output_json}\n- CSV:  s3://{bucket_name}/{output_csv}")
"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import col, trim

from datetime import datetime
from dateutil.relativedelta import relativedelta

import sys, traceback, boto3, json

BUCKET_NAME = 'alfa-datalake-dev-silver'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
DB_NAME = 'ALFAPRD'
DB_SCHEMA = 'GENERAL'
PERIOD = '2025-01'
CERTIFICATE = 'False'
DB_CATALOG_GLUE = 'datalake-dev-bronce-catalog-db'

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME, DB_NAME)
# CCalculate the last 3 periods manually
base_date = datetime.strptime(PERIOD, "%Y-%m")
last_three_periods = [
    base_date.strftime("%Y%m"),
    (base_date - relativedelta(months=1)).strftime("%Y-%m"),
    (base_date - relativedelta(months=2)).strftime("%Y-%m")
]

###############################################################
################ Init stage silver ############################
###############################################################


# Define the dictionary of tables and their respective partition columns.
tables_case_use = {
"ANP_DETALLE_SOLICITUDES_GIRO": {
    "partition_columns": ["periodo_partition"],
    "schema": "GENERAL",
    "transform": [
        lambda df: df.select(
            col("NID_PERIODO_SOLICITUD_GIRO").alias("NID_PERIODO_SOLICITUD_GIRO"),
            trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
            col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
            col("periodo_partition").alias("periodo_partition")
        ),
        lambda df: df.dropDuplicates()
    ]
},
"ANP_DISTRIBUCION_VIG_PENSION": {
    "partition_columns": ["fec_inicial_partition"],
    "schema": "GENERAL",
    "transform": [
        lambda df: df.select(
            col("NID_DISTRIBUCION_VIGENCIA").alias("NID_DISTRIBUCION_VIGENCIA"),
            col("NID_PENSION").alias("NID_PENSION"),
            col("FEC_INICIAL").alias("FEC_INICIAL"),
            col("fec_inicial_partition").alias("fec_inicial_partition")
        ),
        lambda df: df.dropDuplicates()
    ]
},
"ANP_PENSIONADO_NOVEDAD": {
    "partition_columns": ["fec_ingreso_partition"],
    "schema": "GENERAL",
    "transform": [
        lambda df: df.select(
            col("NID_DISTRIBUCION_VIGENCIA").alias("NID_DISTRIBUCION_VIGENCIA"),
            trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
            col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
            trim(col("COD_CONCEPTO")).alias("COD_CONCEPTO"),
            col("VLR_TOTAL").alias("VLR_TOTAL"),
            col("VLR_FIJO").alias("VLR_FIJO"),
            col("FEC_INGRESO").alias("FEC_INGRESO"),
            col("fec_ingreso_partition").alias("fec_ingreso_partition")
        ),
        lambda df: df.dropDuplicates()  # Deduplication
    ]
},
"ANP_PENSIONES": {
    "partition_columns": ["fec_expedicion_partition"],
    "schema": "GENERAL",
    "transform": [
        lambda df: df.select(
            col("NID_PENSION").alias("NID_PENSION"),
            col("NID_PROCEDENCIA").alias("NID_PROCEDENCIA"),
            trim(col("COD_ORIGEN")).alias("COD_ORIGEN"),
            col("FEC_EXPEDICION").alias("FEC_EXPEDICION"),
            col("fec_expedicion_partition").alias("fec_expedicion_partition")
        ),
        lambda df: df.dropDuplicates()
    ]
},
"ANP_PERIODO_SOLICITUDES_GIRO": {
    "partition_columns": ["periodo_partition"],
    "schema": "GENERAL",
    "transform": [
        lambda df: df.select(
            trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
            col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
            col("VLR_NETO").alias("VLR_NETO"),
            col("NUM_SOLICITUD_TES").alias("NUM_SOLICITUD_TES"),
            trim(col("BLN_ANULADO_POINT")).alias("BLN_ANULADO_POINT"),
            col("FEC_ENVIO_TES").alias("FEC_ENVIO_TES"),
            trim(col("COD_CONCEPTO_ANP")).alias("COD_CONCEPTO_ANP"),
            col("NID_PERIODO_SOLICITUD_GIRO").alias("NID_PERIODO_SOLICITUD_GIRO"),
            col("periodo_partition").alias("periodo_partition")
        ),
        lambda df: df.dropDuplicates()
    ]
},
"CGR_CONCEPTOS": {
    "partition_columns": [],
    "schema": "GENERAL",
    "transform": [
        lambda df: df.select(
            trim(col("COD_CONCEPTO")).alias("COD_CONCEPTO"),
            col("NOMBRE_CONCEPTO").alias("NOMBRE_CONCEPTO")
        ),
        lambda df: df.dropDuplicates()
    ]
},
"CGR_ORIGENES": {
    "partition_columns": [],
    "schema": "GENERAL",
    "transform": [
        lambda df: df.select(
            trim(col("COD_ORIGEN")).alias("COD_ORIGEN"),
            trim(col("COD_ORIGEN_SUPER")).alias("COD_ORIGEN_SUPER"),
            trim(col("NOMBRE")).alias("NOMBRE")
        ),
        lambda df: df.dropDuplicates()
    ]
}
}
###################################################################
#################### save stage silver ############################
###################################################################


for table, config in tables_case_use.items():
    
    
    partition_columns = config["partition_columns"]
    transform_functions = config["transform"]
    schema = config["schema"]

    # Load the table data as a DataFrame
    table_name = f"{DB_NAME}_{schema}_{table}"
    df_from_bronce = glueContext.create_dynamic_frame.from_catalog(
        database=f"{DB_CATALOG_GLUE}",
        table_name=table_name
    ).toDF()
    
    #Apply filter if there are partition columns
    if partition_columns:
        partition_col = partition_columns[0] 
        df_from_bronce = df_from_bronce.filter(col(partition_col).isin(last_three_periods))
    
    #Initialize of df to transform
    df_to_silver = df_from_bronce
    
    # Apply each transformation in sequence
    for transform_function in transform_functions:
        df_to_silver = transform_function(df_to_silver)
    
        
"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import col, trim, date_format, lit 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

import sys, traceback, boto3, json


BUCKET_NAME = 'alfa-datalake-dev-gold'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
DB_NAME = 'ALFAPRD'
DB_SCHEMA = 'GENERAL'
PERIOD = '2025-01'
PERIODS =  3       
CERTIFICATE = 'False'
DB_CATALOG_GLUE = 'datalake-dev-silver-catalog-db'
TABLE_NAME_GOLD = 'AUXILIOS_FUNERARIOS_GOLD'

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME, DB_NAME)
				# Define the schema for the  Final DataFrame
schema_auxilios_gold = StructType([
    StructField("Numero_de_poliza", IntegerType(), True),  # Numeric
    StructField("Periodo_de_expedicion_de_la_poliza", StringType(), True),  # AAAAMM (Date format stored as String)
    StructField("Origen_de_la_renta", StringType(), True),  # String with specific categories
    StructField("Valor_auxilio_funerario_pagado", FloatType(), True),  # Numeric
    StructField("Mes_de_pago", StringType(), True),  # AAAAMM (Date format stored as String)
    StructField("PERIODO_PARTITION", StringType(), True)  # AAAAMM (Date format stored as String)
])
fecha_inicio = datetime.strptime(PERIOD, "%Y-%m")
fecha_limite = fecha_inicio - relativedelta(months=PERIODS)
limite_str = fecha_limite.strftime("%Y-%m")



#Read tables from silver
df_anp_detalle_solicitudes_giro = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_DETALLE_SOLICITUDES_GIRO").toDF()
df_anp_distribucion_vig_pension = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_DISTRIBUCION_VIG_PENSION").toDF()
df_anp_pensionado_novedad = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PENSIONADO_NOVEDAD").toDF()
df_anp_pensiones = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PENSIONES").toDF()
df_anp_periodo_solicitudes_giro = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PERIODO_SOLICITUDES_GIRO").toDF()
df_cgr_conceptos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_CONCEPTOS").toDF()
df_cgr_origenes = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="ALFAPRD_GENERAL_SILVER_CGR_ORIGENES").toDF() 



# Lista de tablas a cargar
tables = [
    "ANP_DETALLE_SOLICITUDES_GIRO",
    "ANP_DISTRIBUCION_VIG_PENSION",
    "ANP_PENSIONADO_NOVEDAD",
    "ANP_PENSIONES",
    "ANP_PERIODO_SOLICITUDES_GIRO",
    "CGR_CONCEPTOS",
    "CGR_ORIGENES"
]

dfs = {}

# Cargar y filtrar DataFrames
for table in tables:
    table_name = f"ALFAPRD_GENERAL_SILVER_{table}"
    df = glueContext.create_dynamic_frame.from_catalog(
        database=f"{DB_CATALOG_GLUE}",
        table_name=table_name
    ).toDF()

    # Detectar si hay columna partición
    partition_cols = [c for c in df.columns if c.endswith("_partition")]
    if partition_cols:
        # Usar la primera columna con sufijo _partition
        df = df.filter(col(partition_cols[0]) >= lit(limite_str))

    dfs[f"df_{table.lower()}"] = df

# Acceso individual si lo necesitas
df_anp_detalle_solicitudes_giro = dfs["df_anp_detalle_solicitudes_giro"]
df_anp_distribucion_vig_pension = dfs["df_anp_distribucion_vig_pension"]
df_anp_pensionado_novedad = dfs["df_anp_pensionado_novedad"]
df_anp_pensiones = dfs["df_anp_pensiones"]
df_anp_periodo_solicitudes_giro = dfs["df_anp_periodo_solicitudes_giro"]
df_cgr_conceptos = dfs["df_cgr_conceptos"]
df_cgr_origenes = dfs["df_cgr_origenes"]
###################################################################
################### Init Businees Logic ###########################
###################################################################


#-----------------------------------------------------------------#
#---------------------- Join Tables ------------------------------#
#-----------------------------------------------------------------#


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

# Step 2: Perform LEFT JOIN with anp_detalle_solicitudes_giro (d)
df_join_anp_periodo_solicitudes_giro = df_join_anp_periodo_solicitudes_giro.join(
    df_anp_detalle_solicitudes_giro,
    df_anp_periodo_solicitudes_giro["nid_periodo_solicitud_giro"] == df_anp_detalle_solicitudes_giro["nid_periodo_solicitud_giro"],
    "left"
)

# filter bln_anulado_point, nombre_concepto, num_solicitud_tes
df_join_anp_periodo_solicitudes_giro_filtered = df_join_anp_periodo_solicitudes_giro.filter(
    (df_anp_periodo_solicitudes_giro["bln_anulado_point"] == "N") &  # Filter for non-anulled records
    (df_cgr_conceptos["nombre_concepto"] == "PAGO AUXILIO FUNERARIO") &  # Filter for specific concept
    (df_anp_periodo_solicitudes_giro["num_solicitud_tes"] > 0)  # Ensure num_solicitud_tes is greater to 0
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

#Filter join 
df_join_subqueries_filtered = df_join_subqueries.filter(
    df_join_subqueries['fec_envio_tes'].isNotNull())

#-----------------------------------------------------------------#
#----------------------- Transforms ------------------------------#
#-----------------------------------------------------------------#


# Select the required columns with the exact DataFrame references
df_auxilios_funerarios_query = df_join_subqueries_filtered.select(
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

#Add date AAAAMM

#modify dates for table 
df_auxilios_funerarios_query = df_auxilios_funerarios_query.withColumn("Periodo_de_expedicion_de_la_poliza", date_format(df_auxilios_funerarios_query["fec_expedicion"], "yyyyMM"))
df_auxilios_funerarios_query = df_auxilios_funerarios_query.withColumn("Mes_de_pago", date_format(df_auxilios_funerarios_query["fec_envio_tes"], "yyyyMM"))

#Select final dataframe for gold stage
df_auxilios_gold = df_auxilios_funerarios_query.select(
    df_auxilios_funerarios_query["numero_poliza"].alias("Numero_de_poliza"),
    df_auxilios_funerarios_query["Periodo_de_expedicion_de_la_poliza"].alias("Periodo_de_expedicion_de_la_poliza"),
    df_auxilios_funerarios_query["origen"].alias("Origen_de_la_renta"),
    df_auxilios_funerarios_query["valor_total"].cast(FloatType()).alias("Valor_auxilio_funerario_pagado"),
    df_auxilios_funerarios_query["Mes_de_pago"].alias("Mes_de_pago"),
    df_auxilios_funerarios_query["PERIODO_PARTITION"].alias("PERIODO_PARTITION")
)


###################################################################
##################### Save files Data #############################
###################################################################

partition_columns = ["PERIODO_PARTITION"]
from awsglue.context import GlueContext
from pyspark.context import SparkContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
# Leer tabla DynamoDB como DynamicFrame
dyf_dynamodb = glueContext.create_dynamic_frame_from_options(
    connection_type="dynamodb",
    connection_options={
        "dynamodb.input.tableName": "datalake-dev-tipo-concepto",
        "dynamodb.throughput.read.percent": "1.0"  # puedes reducirlo si es necesario
    }
)
# Convertir a DataFrame para manipular con PySpark
df_dynamodb = dyf_dynamodb.toDF()
df_dynamodb.show()
from awsglue.context import GlueContext
from pyspark.context import SparkContext
# Initialize Spark and Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
# Lista de nombres de tablas DynamoDB
tablas_dynamodb = [
   "datalake-dev-artefacto-concepto-nota-params",
    "datalake-dev-artefacto",
    "datalake-dev-tipo-artefacto",
    "datalake-dev-concepto",
    "datalake-dev-tipo-concepto",
    "datalake-dev-nota",
    "datalake-dev-tipo-nota"
]
# Dictionary to store the DataFrames
dfs_dynamodb = {}
# Read each table in a loop
for nombre_tabla in tablas_dynamodb:
    print(f"🔄 reading table: {nombre_tabla}")
    dyf = glueContext.create_dynamic_frame_from_options(
        connection_type="dynamodb",
        connection_options={
            "dynamodb.input.tableName": nombre_tabla,
            "dynamodb.throughput.read.percent": "1.0"
        }
    )
    df = dyf.toDF()
    dfs_dynamodb[nombre_tabla] = df
    # Mostrar 5 registros como ejemplo
    df.show()
from pyspark.sql.functions import col, max as spark_max

df_nota = dfs_dynamodb["datalake-dev-nota"]

# Convertimos idNota a número si viene como string
df_nota = df_nota.withColumn("idNota_num", col("idNota").cast("int"))

# Obtenemos el máximo
max_idNota = df_nota.select(spark_max("idNota_num")).collect()[0][0]

print(f"✅ Máximo idNota encontrado: {max_idNota}")

df_nota_filtrado = (
    df_nota.filter((col("idNota") >= 197) & (col("idNota") <= 211))
            .orderBy(col("idNota"))
)

df_nota_filtrado.show(truncate=False)
from pyspark.sql.functions import col, max as spark_max

df_artefacto = dfs_dynamodb["datalake-dev-artefacto"]

# Convertimos idNota a número si viene como string
df_artefacto = df_artefacto.withColumn("idArtefacto_num", col("idArtefacto").cast("int"))

# Obtenemos el máximo
max_idArtefacto = df_artefacto.select(spark_max("idArtefacto_num")).collect()[0][0]

print(f"✅ Máximo idArtefacto encontrado: {max_idArtefacto}")
from pyspark.sql.functions import col, max as spark_max

df_params = dfs_dynamodb["datalake-dev-artefacto-concepto-nota-params"]

# Convertimos idNota a número si viene como string
df_params = df_params.withColumn("idArtefactoConceptoNota_num", col("idArtefactoConceptoNota").cast("int"))

# Obtenemos el máximo
max_idArtefactoConceptoNota = df_params.select(spark_max("idArtefactoConceptoNota_num")).collect()[0][0]

print(f"✅ Máximo idArtefactoConceptoNota encontrado: {max_idArtefactoConceptoNota}")
from pyspark.sql.functions import col

df_nota = dfs_dynamodb["datalake-dev-nota"]

# Convertimos idNota a número
df_nota = df_nota.withColumn("idNota_num", col("idNota").cast("int"))

# Obtenemos todos los idNota distintos
ids = [row["idNota_num"] for row in df_nota.select("idNota_num").distinct().collect()]

ids_ordenados = sorted(ids)
max_id = ids_ordenados[-1]
min_id = ids_ordenados[0]

print(f"📌 Rango encontrado: {min_id} → {max_id}")

# Detectar faltantes
ids_completos = set(range(min_id, max_id + 1))
faltantes = sorted(list(ids_completos - set(ids)))

if len(faltantes) == 0:
    print("✅ Todos los idNota están completos y consecutivos ✅")
else:
    print(f"⚠️ IdNota faltantes: {faltantes}")

from pyspark.sql.functions import col

df_nota = dfs_dynamodb["datalake-dev-artefacto"]

# Convertimos idNota a número
df_nota = df_nota.withColumn("idArtefacto_num", col("idArtefacto").cast("int"))

# Obtenemos todos los idNota distintos
ids = [row["idArtefacto_num"] for row in df_nota.select("idArtefacto_num").distinct().collect()]

ids_ordenados = sorted(ids)
max_id = ids_ordenados[-1]
min_id = ids_ordenados[0]

print(f"📌 Rango encontrado: {min_id} → {max_id}")

# Detectar faltantes
ids_completos = set(range(min_id, max_id + 1))
faltantes = sorted(list(ids_completos - set(ids)))

if len(faltantes) == 0:
    print("✅ Todos los idArtefacto están completos y consecutivos ✅")
else:
    print(f"⚠️ idArtefacto faltantes: {faltantes}")
from pyspark.sql.functions import col

df_nota = dfs_dynamodb["datalake-dev-artefacto-concepto-nota-params"]

# Convertimos idNota a número
df_nota = df_nota.withColumn("idArtefactoConceptoNota_num", col("idArtefactoConceptoNota").cast("int"))

# Obtenemos todos los idNota distintos
ids = [row["idArtefactoConceptoNota_num"] for row in df_nota.select("idArtefactoConceptoNota_num").distinct().collect()]

ids_ordenados = sorted(ids)
max_id = ids_ordenados[-1]
min_id = ids_ordenados[0]

print(f"📌 Rango encontrado: {min_id} → {max_id}")

# Detectar faltantes
ids_completos = set(range(min_id, max_id + 1))
faltantes = sorted(list(ids_completos - set(ids)))

if len(faltantes) == 0:
    print("✅ Todos los idArtefactoConceptoNota están completos y consecutivos ✅")
else:
    print(f"⚠️ idArtefactoConceptoNota faltantes: {faltantes}")
df_artefacto = dfs_dynamodb["datalake-dev-artefacto"]
df_tipo_artefacto = dfs_dynamodb["datalake-dev-tipo-artefacto"]
df_concepto = dfs_dynamodb["datalake-dev-concepto"]
df_tipo_concepto = dfs_dynamodb["datalake-dev-tipo-concepto"]
df_nota = dfs_dynamodb["datalake-dev-nota"]
df_tipo_nota = dfs_dynamodb["datalake-dev-tipo-nota"]
df_relacion = dfs_dynamodb["datalake-dev-artefacto-concepto-nota"]
 
# JOIN 1: Artifact with its type
df_artefacto_join = df_artefacto.join(
    df_tipo_artefacto,
    df_artefacto["tipoArtefactoId"] == df_tipo_artefacto["idTipoArtefacto"],
    "left"
)
 
# JOIN 2: Concept with its type
df_concepto_join = df_concepto.join(
    df_tipo_concepto,
    df_concepto["tipoConceptoId"] == df_tipo_concepto["idTipoConcepto"],
    "left"
)
 
# JOIN 3: Note with its type
df_nota_join = df_nota.join(
    df_tipo_nota,
    df_nota["tipoNotaId"] == df_tipo_nota["idTipoNota"],
    "left"
)
 
# JOIN 4: Relationship artifact-concept-note with artifact
df_full = df_relacion.join(
    df_artefacto_join,
    df_relacion["artefactoId"] == df_artefacto["idArtefacto"],
    "left"
).join(
    df_concepto_join,
    df_relacion["conceptoId"] == df_concepto["idConcepto"],
    "left"
).join(
    df_nota_join,
    df_relacion["notaId"] == df_nota["idNota"],
    "left"
)
df_artefacto = dfs_dynamodb["datalake-dev-artefacto"]
df_artefacto.show(truncate = False)
DB_SCHEMA = 'PREVISIONAL'
DB_NAME = 'ALFAPRD'
DB_CATALOG_GLUE = 'datalake-dev-bronce-catalog-db'
BUCKET_NAME = 'alfa-datalake-dev-bronce'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
CONNECT_NAME = 'ALFAPRD_QA_TEST'
PERIOD = '202501'
CERTIFICATE = 'False'
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import SelectFromCollection
from awsglue.job import Job
from awsglue import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality
from commons.LogsHelper import LogsHelper
from pyspark.sql import functions as F

import sys, traceback, boto3, json


DB_SCHEMA = 'FILE'
DB_NAME = 'ULAES'
DB_CATALOG_GLUE = 'datalake-dev-bronce-catalog-db'
BUCKET_NAME = 'alfa-datalake-dev-bronce'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
CONNECT_NAME = 'ALFAPRD_QA_TEST'
PERIOD = '202501'
CERTIFICATE = 'False'
PATH_JSON = f"support_phase_files/niif/{DB_NAME.lower()}_{DB_SCHEMA.lower()}_rules_tables.json"
# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client("s3")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
try:
    # Check if the file exists before attempting to read it
    s3_client.head_object(Bucket=BUCKET_NAME_ASSETS, Key=PATH_JSON)
    print(f"The file {PATH_JSON} exists in S3. Proceeding with the reading.")

    # If the file exists, proceed with the reading
    response = s3_client.get_object(Bucket=BUCKET_NAME_ASSETS, Key=PATH_JSON)
    catalog_tables = json.loads(response["Body"].read().decode("utf-8"))  # Se usa .read()
    print("JSON file successfully loaded.")

except s3_client.exceptions.NoSuchKey:
    print(f"ERROR: The file {PATH_JSON} does not exist in the bucket {BUCKET_NAME_ASSETS}.")
    sys.exit(1)  # Stop execution if the file does not exist

except Exception as e:
    print(f"Unexpected error accessing the JSON in S3: {traceback.format_exc()}")
    sys.exit(1)
for schema in catalog_tables:
    if schema in DB_SCHEMA:
        for table_info in catalog_tables[schema]:
                if table_info['status']:
                    #logger.info(f"Tables for ingest: {schema}.{table_info['table']}")
                    try:
                        
                        frame_to_evaluate = glueContext.create_dynamic_frame.from_catalog(f"{DB_CATALOG_GLUE}", table_name=f"{DB_NAME}_{schema}_{table_info['table']}")
                        
                        #logger.info(f"Tables for ingest: {schema}.{table_info['rules']}")
                        
                        # Dynamically build the set of rules
                        rules_str = "\n".join(table_info['rules'])                        
                        
                        EvaluateDataQuality_ruleset = f"""
                            Rules = [
                                {rules_str}
                            ]
                        """ 

                        
                        EvaluateDataQualityMultiframe= EvaluateDataQuality().process_rows(
                            frame=frame_to_evaluate,
                            ruleset=EvaluateDataQuality_ruleset,
                            publishing_options={
                                "dataQualityEvaluationContext": f"DataQuality_{table_info['table']}",
                                "enableDataQualityMetrics": True,
                                "enableDataQualityResultsPublishing": True,
                            },
                            additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
                        )      
                        
                        #logger.info(EvaluateDataQualityMultiframe)
                        
                        ruleOutcomes = SelectFromCollection.apply(
                            dfc=EvaluateDataQualityMultiframe,
                            key="ruleOutcomes",
                            transformation_ctx="ruleOutcomes",
                        )
                        
                        print("Successfully Evaluate Data Quality ")
                    except Exception as e:
                        print(f"ERROR WITH Evaluate Data Quality: {e}")
    else:
        print("NOT TABLE FOR Evaluate Data Quality")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace
import boto3
import os
from typing import List, Dict, Tuple
import pandas as pd

class ExcelValidator:
    def __init__(self, spark_session: SparkSession, s3_bucket: str, s3_path: str):
        """
        Initialize ExcelValidator with SparkSession and S3 configuration.
        
        Args:
            spark_session: Active SparkSession
            s3_bucket: S3 bucket name
            s3_path: Path within the S3 bucket
        """
        self.spark = spark_session
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.s3_client = boto3.client('s3')

    def validate_file_existence(self, filename: str) -> bool:
        """
        Verify if the file exists in S3 with matching name and extension.
        
        Args:
            filename: Name of the file to validate
            
        Returns:
            bool: True if file exists with matching name and extension
        """
        try:
            # Get file extension
            file_extension = os.path.splitext(filename)[1].lower()
            
            # List objects in S3 with the same name
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=f"{self.s3_path}/{filename}"
            )
            
            # Check if file exists in S3
            if 'Contents' in response:
                s3_filename = response['Contents'][0]['Key']
                s3_extension = os.path.splitext(s3_filename)[1].lower()
                return file_extension == s3_extension
            
            return False
        except Exception as e:
            print(f"Error validating file existence: {str(e)}")
            return False

    def validate_column_headers(self, df: pd.DataFrame, expected_headers: List[str]) -> bool:
        """
        Verify if the column headers match the expected order.
        
        Args:
            df: Pandas DataFrame with the Excel data
            expected_headers: List of expected column headers in order
            
        Returns:
            bool: True if headers match expected order
        """
        current_headers = df.columns.tolist()
        return current_headers == expected_headers

    def read_first_sheet(self, filename: str) -> pd.DataFrame:
        """
        Read only the first sheet of the Excel file.
        
        Args:
            filename: Name of the Excel file
            
        Returns:
            pd.DataFrame: DataFrame containing first sheet data
        """
        try:
            # Read only first sheet
            df = pd.read_excel(filename, sheet_name=0)
            return df
        except Exception as e:
            print(f"Error reading Excel file: {str(e)}")
            return None

    def check_null_values(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        Check for null values in each column.
        
        Args:
            df: DataFrame to check
            
        Returns:
            Dict[str, int]: Dictionary with column names and null count
        """
        return df.isnull().sum().to_dict()

    def remove_whitespace(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove leading and trailing whitespace from all string columns.
        
        Args:
            df: DataFrame to clean
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        # Convert DataFrame to PySpark DataFrame
        spark_df = self.spark.createDataFrame(df)
        
        # Apply trim to string columns
        for column in spark_df.columns:
            if str(spark_df.schema[column].dataType) == 'StringType':
                spark_df = spark_df.withColumn(column, trim(col(column)))
        
        # Convert back to Pandas DataFrame
        return spark_df.toPandas()

    def validate_data_types(self, df: pd.DataFrame, expected_types: Dict[str, str]) -> List[Tuple[str, str]]:
        """
        Validate data types of columns.
        
        Args:
            df: DataFrame to validate
            expected_types: Dictionary of column names and expected types
            
        Returns:
            List[Tuple[str, str]]: List of (column_name, error_message) for invalid types
        """
        errors = []
        for column, expected_type in expected_types.items():
            if column in df.columns:
                try:
                    df[column].astype(expected_type)
                except Exception as e:
                    errors.append((column, str(e)))
        return errors

    def check_problematic_symbols(self, df: pd.DataFrame, symbols: List[str]) -> Dict[str, List[str]]:
        """
        Check for problematic symbols in string columns.
        
        Args:
            df: DataFrame to check
            symbols: List of symbols to check for
            
        Returns:
            Dict[str, List[str]]: Dictionary with column names and found symbols
        """
        problematic_symbols = {}
        
        # Convert DataFrame to PySpark DataFrame
        spark_df = self.spark.createDataFrame(df)
        
        for column in spark_df.columns:
            if str(spark_df.schema[column].dataType) == 'StringType':
                found_symbols = []
                for symbol in symbols:
                    # Count occurrences of symbol
                    count = spark_df.filter(col(column).contains(symbol)).count()
                    if count > 0:
                        found_symbols.append(symbol)
                if found_symbols:
                    problematic_symbols[column] = found_symbols
        
        return problematic_symbols

def create_spark_session() -> SparkSession:
    """
    Create and configure SparkSession.
    
    Returns:
        SparkSession: Configured SparkSession
    """
    return SparkSession.builder \
        .appName("ExcelValidator") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .getOrCreate()

# Example usage
if __name__ == "__main__":
    # Initialize Spark session
    spark = create_spark_session()
    
    # Configure validator
    validator = ExcelValidator(
        spark_session=spark,
        s3_bucket="your-bucket-name",
        s3_path="your/path/in/bucket"
    )
    
    # Example file to validate
    filename = "ARCHIVO_CESION_PREVISIONALES_AAAAMM.xlsx"
    
    # Example expected headers
    expected_headers = ["Column1", "Column2", "Column3"]  # Replace with actual headers
    
    # Example expected data types
    expected_types = {
        "Column1": "str",
        "Column2": "int",
        "Column3": "float"
    }
    
    # Example problematic symbols to check
    symbols_to_check = ['#', '$', '%', '&', '*']
    
    # Validate file existence in S3
    if validator.validate_file_existence(filename):
        # Read first sheet
        df = validator.read_first_sheet(filename)
        
        if df is not None:
            # Validate headers
            headers_valid = validator.validate_column_headers(df, expected_headers)
            print(f"Headers valid: {headers_valid}")
            
            # Check null values
            null_counts = validator.check_null_values(df)
            print(f"Null counts: {null_counts}")
            
            # Remove whitespace
            df = validator.remove_whitespace(df)
            
            # Validate data types
            type_errors = validator.validate_data_types(df, expected_types)
            print(f"Type errors: {type_errors}")
            
            # Check problematic symbols
            problematic_symbols = validator.check_problematic_symbols(df, symbols_to_check)
            print(f"Problematic symbols: {problematic_symbols}")
    
    # Stop Spark session
    spark.stop()

import boto3
import zipfile
import xml.etree.ElementTree as ET
import tempfile
import csv
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# -----------------------------
# Glue Context
# -----------------------------
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
s3 = boto3.client('s3')

# -----------------------------
# Ruta del archivo Excel
# -----------------------------
bucket = "alfa-datalake-dev-assets"
key = "support_phase_files/nilf/PREV_EULAE/ULAES_GENERALES_AAAAMM.xlsx"

# -----------------------------
# Descargar archivo desde S3
# -----------------------------
def descargar_excel(bucket, key):
    tmp_excel = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    s3.download_file(bucket, key, tmp_excel.name)
    return tmp_excel.name

ruta_excel = descargar_excel(bucket, key)
print(f"✅ Archivo descargado: {ruta_excel}")

# -----------------------------
# Extraer Sheet1.xml y convertir a CSV temporal
# -----------------------------
def convertir_excel_a_csv(excel_path):
    with zipfile.ZipFile(excel_path) as z:
        shared_strings = []
        if 'xl/sharedStrings.xml' in z.namelist():
            tree = ET.parse(z.open('xl/sharedStrings.xml'))
            root = tree.getroot()
            for si in root:
                text = ''.join([t.text or '' for t in si.iter() if t.tag.endswith('t')])
                shared_strings.append(text)

        sheet_path = 'xl/worksheets/sheet1.xml'
        tree = ET.parse(z.open(sheet_path))
        root = tree.getroot()

        data = []
        row_data = []
        current_row = None

        for row in root.iter():
            tag = row.tag.split('}')[-1]
            if tag == 'row':
                if row_data:
                    data.append(row_data)
                    row_data = []
            elif tag == 'c':
                cell_type = row.attrib.get('t')
                value = None
                for child in row:
                    if child.tag.endswith('v'):
                        value = child.text
                if cell_type == 's' and value is not None:
                    value = shared_strings[int(value)]
                row_data.append(value)
        if row_data:
            data.append(row_data)

    tmp_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode='w', newline='')
    writer = csv.writer(tmp_csv)
    for row in data:
        writer.writerow(row)
    tmp_csv.close()
    return tmp_csv.name

ruta_csv = convertir_excel_a_csv(ruta_excel)
print(f"✅ CSV temporal generado: {ruta_csv}")

# -----------------------------
# Leer CSV con Spark
# -----------------------------
df = spark.read.option("header", True).option("sep", ",").csv(ruta_csv)
print("✅ CSV cargado como DataFrame.")
df.show(5)

# -----------------------------
# Validaciones
# -----------------------------
columnas_esperadas = ["COMPANIA", "TIPO_ULAE", "CODRAMO", "INDBANCAS", "PERIODO", "ULAE"]
if df.columns == columnas_esperadas:
    print("✅ Columnas en orden correcto.")
else:
    print(f"❌ Columnas desordenadas:\nEsperado: {columnas_esperadas}\nActual:   {df.columns}")

print("🔎 Nulos por columna:")
df.select([when(col(c).isNull(), 1).otherwise(0).alias(c) for c in df.columns]) \
  .groupBy().sum().show()

for c, t in df.dtypes:
    if t == 'string':
        df = df.withColumn(c, trim(col(c)))
print("🧹 Espacios eliminados.")

caracteres_conflictivos = r'[!@#$%^&*(){}[\];:\'",<>/?\\|`~]'
columnas_con_conflictos = []
for c, t in df.dtypes:
    if t == 'string':
        if df.filter(col(c).rlike(caracteres_conflictivos)).count() > 0:
            columnas_con_conflictos.append(c)
if columnas_con_conflictos:
    print(f"❌ Columnas con caracteres conflictivos: {columnas_con_conflictos}")
else:
    print("✅ Sin caracteres conflictivos.")


import re
 
def filtrar_reglas_por_tipo(rules, tipos_deseados):
    """
    Filtra las reglas que comienzan con alguno de los tipos dados, quitando comas al final.
    """
    reglas_filtradas = []
 
    for regla in rules:
        # Detecta y separa si hay una expresión compuesta
        if "AND" in regla or "OR" in regla:
            # Extrae condiciones dentro del paréntesis
            condiciones = re.split(r"\s+(AND|OR)\s+", regla.strip("()"))
            condiciones = [c.strip() for c in condiciones if c not in ("AND", "OR")]
            for cond in condiciones:
                if any(tipo in cond for tipo in tipos_deseados):
                    reglas_filtradas.append(cond)
        else:
            if any(tipo in regla for tipo in tipos_deseados):
                reglas_filtradas.append(regla)
    return reglas_filtradas

def limpiar_reglas(rules):
    reglas_limpias = []
    for r in rules:
        r = r.strip().rstrip(',')
 
        if r.startswith("CustomSql"):
            # Cambiar comillas simples internas por comillas dobles escapadas
            r = re.sub(r"'", r'\\"', r)  # OJO: comillas dobles escapadas
 
        # Eliminar comas finales nuevamente
        r = r.strip().rstrip(',')
        reglas_limpias.append(r)
    return reglas_limpias
import boto3
import pandas as pd
import zipfile
import xml.etree.ElementTree as ET
import tempfile
import csv
import os

# -----------------------------
# Parámetros
# -----------------------------
bucket = "alfa-datalake-dev-assets"
prefix = "support_phase_files/niif/PREV_EULAE/"
nombre_deseado = "ULAES_GENERALES_AAAAMM.xlsx"

# -----------------------------
# Buscar archivo en S3
# -----------------------------
s3 = boto3.client("s3")
response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

archivo_key = None
for obj in response.get("Contents", []):
    if obj["Key"].endswith(nombre_deseado):
        archivo_key = obj["Key"]
        break

if not archivo_key:
    raise FileNotFoundError(f"No se encontró {nombre_deseado} en {prefix}")

# -----------------------------
# Descargar archivo XLSX
# -----------------------------
temp_xlsx = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
s3.download_file(bucket, archivo_key, temp_xlsx.name)

# -----------------------------
# Convertir XLSX a CSV
# -----------------------------
def convertir_excel_a_csv(path_xlsx):
    with zipfile.ZipFile(path_xlsx) as z:
        # Leer sharedStrings
        shared_strings = []
        if 'xl/sharedStrings.xml' in z.namelist():
            tree = ET.parse(z.open('xl/sharedStrings.xml'))
            for si in tree.getroot():
                text = ''.join([t.text or '' for t in si.iter() if t.tag.endswith('t')])
                shared_strings.append(text)

        # Leer hoja 1
        sheet = ET.parse(z.open('xl/worksheets/sheet1.xml')).getroot()
        data = []
        for row in sheet.iter() if sheet is not None else []:
            tag = row.tag.split('}')[-1]
            if tag == 'row':
                row_data = []
                for cell in row:
                    if cell.tag.endswith('c'):
                        cell_type = cell.attrib.get('t')
                        value = None
                        for child in cell:
                            if child.tag.endswith('v'):
                                value = child.text
                        if cell_type == 's' and value:
                            value = shared_strings[int(value)]
                        row_data.append(value)
                data.append(row_data)

    temp_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode='w', newline='', encoding='utf-8')
    writer = csv.writer(temp_csv)
    for row in data:
        writer.writerow(row)
    temp_csv.close()
    return temp_csv.name

ruta_csv = convertir_excel_a_csv(temp_xlsx.name)

# -----------------------------
# Validaciones con pandas
# -----------------------------
df = pd.read_csv(ruta_csv)
columnas_esperadas = ["COMPANIA", "TIPO_ULAE", "CODRAMO", "INDBANCAS", "PERIODO", "ULAE"]
columnas_ok = list(df.columns) == columnas_esperadas

nulos = df.isnull().sum().to_dict()
espacios = df.applymap(lambda x: isinstance(x, str) and (x != x.strip()))
espacios_en_blanco = espacios.any().to_dict()
tipos = df.dtypes.astype(str).to_dict()
simbolos = df.applymap(lambda x: isinstance(x, str) and any(c in x for c in "!@#$%^&*(){}[];:'\"<>/?\\|`~"))
simbolos_conflictivos = simbolos.any().to_dict()

# -----------------------------
# Mostrar resultados
# -----------------------------
import json
resultado = {
    "archivo_detectado_en_s3": archivo_key,
    "columnas_ok": columnas_ok,
    "columnas_actuales": list(df.columns),
    "nulos": nulos,
    "espacios_en_blanco": espacios_en_blanco,
    "tipos_detectados": tipos,
    "caracteres_conflictivos": simbolos_conflictivos
}

print(json.dumps(resultado, indent=2, ensure_ascii=False))


#-----------------------job_1 load bronce y dos funciones nuevas para commons.ExtractHelper (convertir_excel_a_csv_fijo, excel_s3_to_spark_df)
"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F
import pandas as pd

import sys, traceback, boto3, json, tempfile, zipfile
import xml.etree.ElementTree as ET

###################################################################
######## SET ARGS AND CONFIG COMMONS UTILS ########################
###################################################################

DB_SCHEMA = 'FILE'
DB_NAME = 'ULAES'
BUCKET_NAME = 'alfa-datalake-dev-bronce'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
CONNECT_NAME = 'ALFAPRD_QA_TEST'
PERIOD = '202501'
CERTIFICATE = 'False'

PATH_JSON = f"support_phase_files/niif/PREV_EULAE/ULAES_GENERALES_AAAAMM.xlsx"

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client("s3")

extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME, DB_NAME)

###################################################################
############## VALIDATE FILE IN S3 & READ JSON ####################
###################################################################

# Check if the file exists before attempting to read it
print(f"The file {PATH_JSON} exists in S3. Proceeding with the reading.")
columnas_esperadas = ["COMPANIA", "TIPO_ULAE", "CODRAMO", "INDBANCAS", "PERIODO", "ULAE"]

# Check file
if not verificar_archivo(s3_client, BUCKET_NAME_ASSETS, PATH_JSON):
    sys.exit(1)

    
# Upload file and check structure
if DB_SCHEMA == 'FILE':
    frame_save = excel_s3_to_spark_df(s3_client, BUCKET_NAME_ASSETS, PATH_JSON, spark, glueContext)
    verificar_estructura_archivo(frame_save, columnas_esperadas)
    frame_save = DynamicFrame.fromDF(df_spark, glueContext, "frame_save_dynamic") 
    
###################################################################
################# ITERATE FOR EACH TABLE ##########################
###################################################################

if DB_SCHEMA == 'FILE':
    try:
        partition_keys = []
        table_name = 'ULAES_GENERALES'
        s3_prefix = f"{DB_NAME}/{DB_SCHEMA}/{table_name}/"        
        s3_path = f"s3://{BUCKET_NAME}/{s3_prefix}"
        #extract_helper.save_files_s3(frame_save, s3_path, partition_keys)
        print("Successfully Extract ")
    except Exception as e:
        print(f"ERROR WITH INGEST: {e}")
else:
    for schema in catalog_tables:
        if schema in DB_SCHEMA:
            for table_info in catalog_tables[schema]:
                if table_info['status']:
                    logger.info(f"Tables for ingest: {schema}.{table_info['table']}")
                    try:
                        extract_helper.execute_data(
                            table_info['table'], 
                            table_info['partition_column'],  
                            table_info['logic'], 
                            connectname= CONNECT_NAME)
                        logger.info("Successfully Extract ")
                        
                    except Exception as e:
                        logger.info(f"ERROR WITH INGEST: {e}")
        else:
            logger.info("NOT TABLE FOR INGEST")
    

punto 1 y 2 historia 
funciones
# -----------------------------
# Convertir XLSX a CSV
# -----------------------------
def convertir_excel_a_csv_fijo(path_xlsx):
    import zipfile
    import xml.etree.ElementTree as ET
    import csv

    with zipfile.ZipFile(path_xlsx) as z:
        # Leer sharedStrings
        shared_strings = []
        if 'xl/sharedStrings.xml' in z.namelist():
            tree = ET.parse(z.open('xl/sharedStrings.xml'))
            for si in tree.getroot():
                text = ''.join([t.text or '' for t in si.iter() if t.tag.endswith('t')])
                shared_strings.append(text)

        # Leer hoja 1
        sheet = ET.parse(z.open('xl/worksheets/sheet1.xml')).getroot()
        data = []
        encabezado = []

        for row in sheet.iter():
            if row.tag.endswith('row'):
                row_data = []
                row_index = int(row.attrib.get("r"))  # número de fila Excel
                col_map = {}  # mapa de columna Excel → valor (ej. A1, B1, etc.)

                for cell in row:
                    if cell.tag.endswith('c'):
                        cell_type = cell.attrib.get('t')
                        cell_ref = cell.attrib.get('r')  # como "D5", "C1"
                        col_letter = ''.join(filter(str.isalpha, cell_ref))  # "D", "C"

                        value = ""
                        for child in cell:
                            if child.tag.endswith('v'):
                                value = child.text
                        if cell_type == 's' and value:
                            value = shared_strings[int(value)]
                        col_map[col_letter] = value

                # Ordenar columnas por letra (A, B, C...)
                sorted_cols = sorted(col_map.items(), key=lambda x: x[0])
                row_data = [col_map.get(chr(65 + i), "") for i in range(len(col_map))]
                #data.append(row_data)
                # Guardar encabezado
                if row_index == 1:
                    encabezado = row_data
                else:
                    row_dict = {"excel_row": str(row_index)}
                    for i in range(len(encabezado)):
                        col_name = encabezado[i] if i < len(encabezado) else f"COL_{i}"
                        row_dict[col_name] = row_data[i] if i < len(row_data) else ""
                    data.append(row_dict)

    # Guardar como CSV temporal
    temp_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode='w', newline='', encoding='utf-8')
    fieldnames = ["excel_row"] + encabezado
    writer = csv.DictWriter(temp_csv, fieldnames=fieldnames)
    writer.writeheader()
    for row in data:
        writer.writerow(row)
    temp_csv.close()
    return temp_csv.name
 
import pandas as pd
import tempfile
import boto3
from pyspark.sql import SparkSession

def excel_s3_to_spark_df(s3_client, bucket, key_xlsx, spark, glueContext):
    # Paso 1: Descargar .xlsx desde S3
    temp_xlsx = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    s3_client.download_file(bucket, key_xlsx, temp_xlsx.name)

    # Paso 2: Convertir a CSV con estructura fija
    ruta_csv = convertir_excel_a_csv_fijo(temp_xlsx.name)

    # Paso 3: Leer con Pandas
    df = pd.read_csv(ruta_csv)

    # Paso 4: Limpieza de NaNs
    # Limpieza genérica por tipo de dato
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(0)
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].fillna(pd.Timestamp("1900-01-01"))
        else:
            df[col] = df[col].fillna("").astype(str)


    # Paso 5: Convertir a Spark DataFrame
    df_spark = spark.createDataFrame(df)
    
    #Dynamic_Frame = DynamicFrame.fromDF(df_spark, glueContext, "frame_save_dynamic") 

    return df_spark #Dynamic_Frame


def filtrar_reglas_por_tipo(rules_list, tipos_regla):
    """
    Filtra las reglas que contienen alguno de los tipos dados (útil para reglas compuestas).
    """
    reglas_filtradas = []
    for regla in rules_list:
        regla_limpia = regla.strip().rstrip(",")
        if any(tipo in regla_limpia for tipo in tipos_regla):
            reglas_filtradas.append(regla_limpia)
    return reglas_filtradas

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

def glue_quality (df, rules):
    
    EvaluateDataQuality_ruleset = f"""Rules = [{rules}]"""
    
    EvaluateDataQualityMultiframe= EvaluateDataQuality().process_rows(
        frame=df,
        ruleset=EvaluateDataQuality_ruleset,
        publishing_options={
            "dataQualityEvaluationContext": f"DataQuality_{table_info['table']}",
            "enableDataQualityMetrics": True,
            "enableDataQualityResultsPublishing": False,
        },
        additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
    )      
    #logger.info(EvaluateDataQualityMultiframe)
    
    ruleOutcomes = SelectFromCollection.apply(
        dfc=EvaluateDataQualityMultiframe,
        key="ruleOutcomes",
        transformation_ctx="rowLevelOutcomes",
    )
    
    
    df_ruleOutcomes = ruleOutcomes.toDF()
    return df_ruleOutcomes


def verificar_archivo(s3_client, bucket, key):
    """
    Verifica si un archivo existe en un bucket de S3.
    Retorna True si existe, False en caso contrario.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        print(f"The file '{key}' exist in the bucket '{bucket}'.")  
        return True
    except s3_client.exceptions.NoSuchKey:
        print(f"ERROR: The file '{key}' does not exist in the bucket '{bucket}'.")
        return False
    except Exception:
        print(f"Unexpected error accessing the JSON in S3:  {traceback.format_exc()}")
        return False
 
 
def verificar_estructura_archivo(df, columnas_esperadas):
    """
    Verifica si el DataFrame tiene las columnas esperadas en el orden correcto.
    Lanza una excepción si hay discrepancias.
    """
    columnas_actuales = df.columns
    errores = []
 
    for idx, col in enumerate(columnas_esperadas):
        if idx >= len(columnas_actuales) or columnas_actuales[idx] != col:
            errores.append(
                f"Columna esperada en posición {idx}: '{col}', "
                f"encontrada: '{columnas_actuales[idx] if idx < len(columnas_actuales) else 'N/A'}'"
            )
 
    if errores:
        print("Invalid file structure:")
        for err in errores:
            print(f"   - {err}")
        raise ValueError("Invalid column structure in the file               .=(")
    else:
        print("File structure validated successfully.")
"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F
import pandas as pd

import sys, traceback, boto3, json, tempfile, zipfile
import xml.etree.ElementTree as ET

###################################################################
######## SET ARGS AND CONFIG COMMONS UTILS ########################
###################################################################

DB_SCHEMA = 'FILE'
DB_NAME = 'ULAES'
BUCKET_NAME = 'alfa-datalake-dev-bronce'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
CONNECT_NAME = 'ALFAPRD_QA_TEST'
PERIOD = '202501'
CERTIFICATE = 'False'

PATH_JSON = f"support_phase_files/niif/PREV_EULAE/ULAES_GENERALES_AAAAMM.xlsx"

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client("s3")

extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME, DB_NAME)

###################################################################
############## VALIDATE FILE IN S3 & READ JSON ####################
###################################################################
 
try:
    # Check if the file exists before attempting to read it
    s3_client.head_object(Bucket=BUCKET_NAME_ASSETS, Key=PATH_JSON)
    print(f"The file {PATH_JSON} exists in S3. Proceeding with the reading.")

    # If the file exists, proceed with the reading
    if DB_SCHEMA == 'FILE':
        frame_save = excel_s3_to_spark_df(s3_client, BUCKET_NAME_ASSETS, PATH_JSON, spark, glueContext)
        print("xlsx file successfully loaded.")
    else:
        response = s3_client.get_object(Bucket=BUCKET_NAME_ASSETS, Key=PATH_JSON)
        catalog_tables = json.loads(response["Body"].read().decode("utf-8"))  # Se usa .read()
        logger.info("JSON file successfully loaded.")        

except s3_client.exceptions.NoSuchKey:
    print(f"ERROR: The file {PATH_JSON} does not exist in the bucket {BUCKET_NAME_ASSETS}.")
    sys.exit(1)  # Stop execution if the file does not exist

except Exception as e:
    print(f"Unexpected error accessing the JSON in S3: {traceback.format_exc()}")
    sys.exit(1)
    
###################################################################
################# ITERATE FOR EACH TABLE ##########################
###################################################################

if DB_SCHEMA == 'FILE':
    try:
        partition_keys = []
        table_name = 'ULAES_GENERALES'
        s3_prefix = f"{DB_NAME}/{DB_SCHEMA}/{table_name}/"        
        s3_path = f"s3://{BUCKET_NAME}/{s3_prefix}"
        #extract_helper.save_files_s3(frame_save, s3_path, partition_keys)
        print("Successfully Extract ")
    except Exception as e:
        print(f"ERROR WITH INGEST: {e}")
else:
    for schema in catalog_tables:
        if schema in DB_SCHEMA:
            for table_info in catalog_tables[schema]:
                if table_info['status']:
                    logger.info(f"Tables for ingest: {schema}.{table_info['table']}")
                    try:
                        extract_helper.execute_data(
                            table_info['table'], 
                            table_info['partition_column'],  
                            table_info['logic'], 
                            connectname= CONNECT_NAME)
                        logger.info("Successfully Extract ")
                        
                    except Exception as e:
                        logger.info(f"ERROR WITH INGEST: {e}")
        else:
            logger.info("NOT TABLE FOR INGEST")
    

#-----------------------job_2 quality bronce y tres funciones nuevas para commons.ExtractHelper (filtrar_reglas_por_tipo, verificar_atributo, glue_quality)
"""Script that quality bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import SelectFromCollection
from awsglue.job import Job
from awsglue import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality
from commons.LogsHelper import LogsHelper
from pyspark.sql import functions as F

import sys, traceback, boto3, json


###################################################################
######## SET ARGS AND CONFIG COMMONS UTILS ########################
###################################################################


DB_SCHEMA = 'FILE'
DB_NAME = 'ULAES'
DB_CATALOG_GLUE = 'datalake-dev-bronce-catalog-db'
BUCKET_NAME = 'alfa-datalake-dev-bronce'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
CONNECT_NAME = 'ALFAPRD_QA_TEST'
PERIOD = '202501'
CERTIFICATE = 'False'

PATH_JSON = f"support_phase_files/niif/{DB_NAME.lower()}_{DB_SCHEMA.lower()}_rules_tables.json"


# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

###################################################################
############## VALIDATE FILE IN S3 & READ JSON ####################
###################################################################

try:
    # Check if the file exists before attempting to read it
    s3_client.head_object(Bucket=BUCKET_NAME_ASSETS, Key=PATH_JSON)
    print(f"The file {PATH_JSON} exists in S3. Proceeding with the reading.")

    # If the file exists, proceed with the reading
    response = s3_client.get_object(Bucket=BUCKET_NAME_ASSETS, Key=PATH_JSON)
    catalog_tables = json.loads(response["Body"].read().decode("utf-8"))  # Se usa .read()
    print("JSON file successfully loaded.")

except s3_client.exceptions.NoSuchKey:
    print(f"ERROR: The file {PATH_JSON} does not exist in the bucket {BUCKET_NAME_ASSETS}.")
    sys.exit(1)  # Stop execution if the file does not exist

except Exception as e:
    print(f"Unexpected error accessing the JSON in S3: {traceback.format_exc()}")
    sys.exit(1)

    
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
                                #filtered_rules = limpiar_reglas(filtered_rules)
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
rules_str 
# ---------------------------------------------------------------  FIN CÓDIGO JOB´S 
df_failes_outcomes_total.show(truncate = True)
# -----------------------------
# IMPORTS
# -----------------------------

import zipfile
import xml.etree.ElementTree as ET
import csv
import tempfile
import pandas as pd
import boto3
import traceback
import sys
import json
import datetime
import inspect

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.transforms import SelectFromCollection
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# -----------------------------
# FUNCIONES AUXILIARES
# -----------------------------

def convertir_excel_a_csv_fijo(path_xlsx):
    with zipfile.ZipFile(path_xlsx) as z:
        # Leer sharedStrings
        shared_strings = []
        if 'xl/sharedStrings.xml' in z.namelist():
            tree = ET.parse(z.open('xl/sharedStrings.xml'))
            for si in tree.getroot():
                text = ''.join([t.text or '' for t in si.iter() if t.tag.endswith('t')])
                shared_strings.append(text)

        # Leer hoja 1
        sheet = ET.parse(z.open('xl/worksheets/sheet1.xml')).getroot()
        data = []

        for row in sheet.iter():
            if row.tag.endswith('row'):
                row_data = []
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
                data.append(row_data)

    temp_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode='w', newline='', encoding='utf-8')
    writer = csv.writer(temp_csv)
    for row in data:
        writer.writerow(row)
    temp_csv.close()
    return temp_csv.name


def excel_s3_to_spark_df(s3_client, bucket, key_xlsx, spark, glueContext):
    temp_xlsx = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    s3_client.download_file(bucket, key_xlsx, temp_xlsx.name)
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
    Dynamic_Frame = DynamicFrame.fromDF(df_spark, glueContext, "frame_save_dynamic")
    return Dynamic_Frame


def filtrar_reglas_por_tipo(all_rules, tipos_filtrados):
    reglas_filtradas = [rule for rule in all_rules if any(tipo in rule for tipo in tipos_filtrados)]
    return reglas_filtradas


def obtener_nombre_funcion_actual():
    return inspect.stack()[1].function


def generar_log_error(
    spark,
    columna=None,
    valor=None,
    fallo=None
):
    nombre_funcion = obtener_nombre_funcion_actual()
    timestamp_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    schema = [
        "columna",
        "valor",
        "timestamp",
        "origen_id_lote",
        "cumplio",
        "fallo",
        "Regla"
    ]

    values = [
        columna,
        valor,
        timestamp_now,
        None,
        0,
        fallo,
        nombre_funcion
    ]

    return spark.createDataFrame([values], schema=schema)


def verificar_atributo(dynamic_frame, rules_str, spark):
    """
    Ejecuta EvaluateDataQuality y devuelve DF con resultados de fallos.
    """
    try:
        dq_ruleset = f"""
            Rules = [
                {rules_str}
            ]
        """
        dq_result = EvaluateDataQuality().process_rows(
            frame=dynamic_frame,
            ruleset=dq_ruleset,
            publishing_options={}
        )

        df_result = dq_result.toDF()
        df_fails = df_result.filter(F.col("Outcome") != "Passed")

        if df_fails.count() == 0:
            return None

        timestamp_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        nombre_funcion = obtener_nombre_funcion_actual()

        df_fails_transformed = df_fails.withColumn("timestamp", F.lit(timestamp_now)) \
            .withColumn("origen_id_lote", F.lit(None).cast("string")) \
            .withColumn("cumplio", F.lit(0)) \
            .withColumn("fallo", F.concat_ws("|",
                                             F.col("ColumnName"),
                                             F.col("Constraint"),
                                             F.col("ConstraintCheckValue"),
                                             F.col("Outcome"))) \
            .withColumn("Regla", F.lit(nombre_funcion)) \
            .select(
                F.col("ColumnName").alias("columna"),
                F.col("ConstraintCheckValue").alias("valor"),
                "timestamp",
                "origen_id_lote",
                "cumplio",
                "fallo",
                "Regla"
            )

        return df_fails_transformed

    except Exception as e:
        return generar_log_error(
            spark=spark,
            columna=None,
            valor=None,
            fallo=str(e)
        )

# -----------------------------
# SCRIPT PRINCIPAL
# -----------------------------

DB_SCHEMA = 'FILE'
DB_NAME = 'ULAES'
DB_CATALOG_GLUE = 'datalake-dev-bronce-catalog-db'
BUCKET_NAME = 'alfa-datalake-dev-bronce'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
CONNECT_NAME = 'ALFAPRD_QA_TEST'
PERIOD = '202501'
CERTIFICATE = 'False'

PATH_JSON = f"support_phase_files/niif/{DB_NAME.lower()}_{DB_SCHEMA.lower()}_rules_tables.json"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# -----------------------------
# LOAD JSON FILE
# -----------------------------

try:
    s3_client.head_object(Bucket=BUCKET_NAME_ASSETS, Key=PATH_JSON)
    print(f"The file {PATH_JSON} exists in S3. Proceeding with the reading.")
    response = s3_client.get_object(Bucket=BUCKET_NAME_ASSETS, Key=PATH_JSON)
    catalog_tables = json.loads(response["Body"].read().decode("utf-8"))
    print("JSON file successfully loaded.")

except s3_client.exceptions.NoSuchKey:
    print(f"ERROR: The file {PATH_JSON} does not exist in the bucket {BUCKET_NAME_ASSETS}.")
    sys.exit(1)
except Exception as e:
    print(f"Unexpected error accessing the JSON in S3: {traceback.format_exc()}")
    sys.exit(1)

# -----------------------------
# PROCESS EACH TABLE
# -----------------------------

df_failes_outcomes_total = None

for schema in catalog_tables:
    if schema in DB_SCHEMA:
        for table_info in catalog_tables[schema]:
            if table_info['status']:
                print(f"Processing table: {schema}.{table_info['table']}")
                try:
                    frame_to_evaluate = glueContext.create_dynamic_frame.from_catalog(
                        database=DB_CATALOG_GLUE,
                        table_name=f"{DB_NAME}_{schema}_{table_info['table']}"
                    )

                    all_rules = table_info['rules']
                    tipos_deseados = ["ColumnExists", "ColumnDataType", "ColumnLength"]

                    for tipo in tipos_deseados:
                        filtered_rules = filtrar_reglas_por_tipo(all_rules, [tipo])
                        if filtered_rules:
                            rules_str = ",\n".join(filtered_rules)
                            df_fails = verificar_atributo(frame_to_evaluate, rules_str, spark)
                            if df_fails is not None:
                                df_fails = df_fails.withColumn("rule_type", F.lit(tipo))
                                if df_failes_outcomes_total is None:
                                    df_failes_outcomes_total = df_fails
                                else:
                                    df_failes_outcomes_total = df_failes_outcomes_total.unionByName(
                                        df_fails, allowMissingColumns=True
                                    )

                    print("Successfully evaluated data quality.")

                except Exception as e:
                    df_log = generar_log_error(
                        spark=spark,
                        columna=None,
                        valor=None,
                        fallo=str(e)
                    )
                    if df_failes_outcomes_total is None:
                        df_failes_outcomes_total = df_log
                    else:
                        df_failes_outcomes_total = df_failes_outcomes_total.unionByName(
                            df_log, allowMissingColumns=True
                        )
    else:
        print("NO TABLES FOR Evaluate Data Quality")

# -----------------------------
# SHOW RESULTS
# -----------------------------

if df_failes_outcomes_total:
    df_failes_outcomes_total.show(truncate=False)
else:
    print("No data quality issues found.")


Funciones con Log
# -----------------------------
# Convertir XLSX a CSV
# -----------------------------
def convertir_excel_a_csv_fijo(path_xlsx):
    import zipfile
    import xml.etree.ElementTree as ET
    import csv

    with zipfile.ZipFile(path_xlsx) as z:
        # Leer sharedStrings
        shared_strings = []
        if 'xl/sharedStrings.xml' in z.namelist():
            tree = ET.parse(z.open('xl/sharedStrings.xml'))
            for si in tree.getroot():
                text = ''.join([t.text or '' for t in si.iter() if t.tag.endswith('t')])
                shared_strings.append(text)

        # Leer hoja 1
        sheet = ET.parse(z.open('xl/worksheets/sheet1.xml')).getroot()
        data = []
        encabezado = []

        for row in sheet.iter():
            if row.tag.endswith('row'):
                row_data = []
                row_index = int(row.attrib.get("r"))  # número de fila Excel
                col_map = {}  # mapa de columna Excel → valor (ej. A1, B1, etc.)

                for cell in row:
                    if cell.tag.endswith('c'):
                        cell_type = cell.attrib.get('t')
                        cell_ref = cell.attrib.get('r')  # como "D5", "C1"
                        col_letter = ''.join(filter(str.isalpha, cell_ref))  # "D", "C"

                        value = ""
                        for child in cell:
                            if child.tag.endswith('v'):
                                value = child.text
                        if cell_type == 's' and value:
                            value = shared_strings[int(value)]
                        col_map[col_letter] = value

                # Ordenar columnas por letra (A, B, C...)
                sorted_cols = sorted(col_map.items(), key=lambda x: x[0])
                row_data = [col_map.get(chr(65 + i), "") for i in range(len(col_map))]
                #data.append(row_data)
                # Guardar encabezado
                if row_index == 1:
                    encabezado = row_data
                else:
                    row_dict = {"excel_row": str(row_index)}
                    for i in range(len(encabezado)):
                        col_name = encabezado[i] if i < len(encabezado) else f"COL_{i}"
                        row_dict[col_name] = row_data[i] if i < len(row_data) else ""
                    data.append(row_dict)

    # Guardar como CSV temporal
    temp_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode='w', newline='', encoding='utf-8')
    fieldnames = ["excel_row"] + encabezado
    writer = csv.DictWriter(temp_csv, fieldnames=fieldnames)
    writer.writeheader()
    for row in data:
        writer.writerow(row)
    temp_csv.close()
    return temp_csv.name
 
import pandas as pd
import tempfile
import boto3
from pyspark.sql import SparkSession
import inspect
from datetime import datetime
import os

# Configuración de ruta S3 para logs
path_log = 's3://tu-bucket-de-logs/logs/validaciones/'  # Cambiar por la ruta S3 completa deseada

# Initialize a global dataframe to store logs with enhanced structure
log_columns = ["columna", "indice", "timestamp", "origen_id_lote", "cumplio", "fallo", "regla"]
log_df = pd.DataFrame(columns=log_columns)

def log_validation_failure(columna, indice, fallo, origen_id_lote=None, cumplio=0):
    """
    Enhanced logging function that automatically detects the calling function name
    and logs validation failures with detailed information.
    
    Args:
        columna: Name of the column that failed validation
        indice: Row index to identify the specific record that failed
        fallo: Detailed description of the failure
        origen_id_lote: Batch origin ID (default: None)
        cumplio: Compliance status (default: 0 for failure)
    """
    global log_df
    
    # Automatically detect the calling function name
    frame = inspect.currentframe()
    try:
        # Get the caller's frame (the function that called this logging function)
        caller_frame = frame.f_back
        regla_func_name = caller_frame.f_code.co_name
        
        # If called from a lambda or anonymous function, try to get more context
        if regla_func_name == '<lambda>':
            # Try to get the function that contains the lambda
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
    
    Args:
        archivo_encontrado: Boolean indicating if file was found
        ruta_archivo: Path of the file being searched
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
    
    Args:
        expected_columns: List of expected column names
        actual_columns: List of actual column names found
        df_row_count: Total number of rows in the dataframe for context
    """
    # Check if all expected columns are present
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
    
    # Check column order
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
    
    Args:
        row_data: The data from the failed row
        row_index: The index of the row that failed
        column_name: The specific column that failed validation
        validation_rule: The validation rule that was applied
        error_message: Detailed error message
        excel_row: Original Excel row number if available
    """
    # Use excel_row if available, otherwise use row_index
    indice_final = excel_row if excel_row is not None else row_index
    
    # Create detailed failure message
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
    Enhanced function to save logs to S3 in txt format, optimized for AWS Glue environment.
    Uses a complete S3 path (s3://bucket/path/file.txt) for flexibility.
    
    Args:
        s3_client: Boto3 S3 client
        s3_path: Complete S3 path including bucket and key (e.g., s3://bucket/path/file.txt)
    """
    global log_df
    
    if log_df.empty:
        print("No logs to save.")
        return
    
    try:
        # Parse S3 path to extract bucket and key
        if not s3_path.startswith('s3://'):
            raise ValueError("S3 path must start with 's3://'")
        
        # Remove s3:// prefix and split bucket and key
        path_without_prefix = s3_path[5:]  # Remove 's3://'
        bucket, key = path_without_prefix.split('/', 1)
        
        # En AWS Glue, es mejor usar StringIO para manejar los datos en memoria
        # en lugar de archivos temporales en disco
        from io import StringIO
        
        # Crear buffer en memoria
        csv_buffer = StringIO()
        
        # Guardar DataFrame como CSV en el buffer
        log_df.to_csv(csv_buffer, sep='\t', index=False)
        
        # Obtener el contenido del buffer
        log_content = csv_buffer.getvalue()
        
        # Subir directamente a S3 desde memoria
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=log_content.encode('utf-8'),
            ContentType='text/plain'
        )
        
        print(f"Logs successfully saved to {s3_path}")
        
        # Cerrar el buffer
        csv_buffer.close()
        
    except Exception as e:
        print(f"Error saving logs to S3: {str(e)}")
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

def excel_s3_to_spark_df(s3_client, bucket, key_xlsx, spark, glueContext, table_name=None):
    """
    Convierte archivo Excel de S3 a Spark DataFrame con logging integrado.
    
    Args:
        s3_client: Cliente S3 de boto3
        bucket: Nombre del bucket S3
        key_xlsx: Clave del archivo Excel en S3
        spark: Sesión de Spark
        glueContext: Contexto de AWS Glue
        table_name: Nombre de la tabla para los logs (opcional)
    """
    try:
        # Paso 1: Descargar .xlsx desde S3
        temp_xlsx = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        s3_client.download_file(bucket, key_xlsx, temp_xlsx.name)
        log_file_status(archivo_encontrado=True, ruta_archivo=f"s3://{bucket}/{key_xlsx}")

        # Paso 2: Convertir a CSV con estructura fija
        ruta_csv = convertir_excel_a_csv_fijo(temp_xlsx.name)

        # Paso 3: Leer con Pandas
        df = pd.read_csv(ruta_csv)

        # Paso 4: Limpieza de NaNs
        # Limpieza genérica por tipo de dato
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(0)
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].fillna(pd.Timestamp("1900-01-01"))
            else:
                df[col] = df[col].fillna("").astype(str)

        # Paso 5: Convertir a Spark DataFrame
        df_spark = spark.createDataFrame(df)
        
        # Log column validation
        expected_columns = df.columns.tolist()
        actual_columns = df_spark.columns
        log_column_validation(expected_columns, actual_columns, len(df))
        
        #Dynamic_Frame = DynamicFrame.fromDF(df_spark, glueContext, "frame_save_dynamic") 

        # Guardar logs de procesamiento exitoso
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        table_suffix = f"_{table_name}" if table_name else ""
        log_file_path = f"{path_log}processing_logs{table_suffix}_{timestamp}.txt"
        
        try:
            save_logs_to_s3(s3_client, log_file_path)
            print(f"Processing logs saved to: {log_file_path}")
        except Exception as log_error:
            print(f"Error saving processing logs to S3: {log_error}")

        return df_spark #Dynamic_Frame

    except FileNotFoundError:
        log_file_status(archivo_encontrado=False, ruta_archivo=f"s3://{bucket}/{key_xlsx}")
        
        # Guardar logs de archivo no encontrado
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_log_path = f"{path_log}file_not_found_logs_{timestamp}.txt"
        try:
            save_logs_to_s3(s3_client, error_log_path)
            print(f"File not found logs saved to: {error_log_path}")
        except Exception as log_error:
            print(f"Error saving file not found logs to S3: {log_error}")
        
        raise
    
    except Exception as e:
        log_validation_failure(
            columna="proceso", 
            indice=0, 
            fallo=f"Error al procesar archivo Excel: {str(e)}",
            cumplio=0
        )
        
        # Guardar logs de error general
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        table_suffix = f"_{table_name}" if table_name else ""
        error_log_path = f"{path_log}processing_error_logs{table_suffix}_{timestamp}.txt"
        try:
            save_logs_to_s3(s3_client, error_log_path)
            print(f"Processing error logs saved to: {error_log_path}")
        except Exception as log_error:
            print(f"Error saving processing error logs to S3: {log_error}")
        
        raise

def filtrar_reglas_por_tipo(rules_list, tipos_regla):
    """
    Filtra las reglas que contienen alguno de los tipos dados (útil para reglas compuestas).
    """
    reglas_filtradas = []
    for regla in rules_list:
        regla_limpia = regla.strip().rstrip(",")
        if any(tipo in regla_limpia for tipo in tipos_regla):
            reglas_filtradas.append(regla_limpia)
    return reglas_filtradas

def verificar_atributo(df, rules, table_name=None):
    """
    Verifica atributos del DataFrame usando reglas de calidad y registra los resultados.
    
    Args:
        df: DataFrame a validar
        rules: Reglas de calidad a aplicar
        table_name: Nombre de la tabla para los logs (opcional)
    """
    try:
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
        
        # Log validation failures
        failed_rows = df_final.collect()
        for row in failed_rows:
            log_row_validation_failure(
                row_data=row.asDict(),
                row_index=row["__row_id__"],
                column_name=row.get("column", "unknown"),
                validation_rule=row.get("rule", "unknown"),
                error_message=row.get("message", "Fallo desconocido"),
                excel_row=row.get("excel_row")
            )
        
        # Guardar logs en S3 después de procesar todas las validaciones
        if failed_rows:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            table_suffix = f"_{table_name}" if table_name else ""
            log_file_path = f"{path_log}validation_logs{table_suffix}_{timestamp}.txt"
            
            try:
                save_logs_to_s3(s3_client, log_file_path)
                print(f"Validation logs saved to: {log_file_path}")
            except Exception as log_error:
                print(f"Error saving logs to S3: {log_error}")
     
        return df_final.drop("__row_id__")
    
    except Exception as e:
        log_validation_failure(
            columna="validacion", 
            indice=0, 
            fallo=f"Error al validar atributos: {str(e)}",
            cumplio=0
        )
        
        # Intentar guardar logs de error
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_log_path = f"{path_log}error_logs_{timestamp}.txt"
            save_logs_to_s3(s3_client, error_log_path)
            print(f"Error logs saved to: {error_log_path}")
        except Exception as log_error:
            print(f"Error saving error logs to S3: {log_error}")
        
        raise

def glue_quality(df, rules):
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
        
        df_ruleOutcomes = ruleOutcomes.toDF()
        return df_ruleOutcomes
    
    except Exception as e:
        log_validation_failure(
            columna="calidad", 
            indice=0, 
            fallo=f"Error en evaluación de calidad: {str(e)}",
            cumplio=0
        )
        raise

        
def verificar_archivo(s3_client, bucket, key):
    """
    Verifica si un archivo existe en un bucket de S3.
    Retorna True si existe, False en caso contrario.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        print(f"The file '{key}' exist in the bucket '{bucket}'.")  
        return True
    except s3_client.exceptions.NoSuchKey:
        print(f"ERROR: The file '{key}' does not exist in the bucket '{bucket}'.")
        return False
    except Exception:
        print(f"Unexpected error accessing the JSON in S3:  {traceback.format_exc()}")
        return False
 
 
def verificar_estructura_archivo(df, columnas_esperadas):
    """
    Verifica si el DataFrame tiene las columnas esperadas en el orden correcto.
    Lanza una excepción si hay discrepancias.
    """
    columnas_actuales = df.columns
    errores = []
 
    for idx, col in enumerate(columnas_esperadas):
        if idx >= len(columnas_actuales) or columnas_actuales[idx] != col:
            errores.append(
                f"Columna esperada en posición {idx}: '{col}', "
                f"encontrada: '{columnas_actuales[idx] if idx < len(columnas_actuales) else 'N/A'}'"
            )
 
    if errores:
        print("Invalid file structure:")
        for err in errores:
            print(f"   - {err}")
        raise ValueError("Invalid column structure.")
    else:
        print("File structure validated successfully.")
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

# Configuración de ruta S3 para logs
path_log = 's3://tu-bucket-de-logs/logs/validaciones/'  # Cambiar por la ruta S3 completa deseada

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

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        table_suffix = f"_{table_name}" if table_name else ""
        log_file_path = f"{path_log}processing_logs{table_suffix}_{timestamp}.txt"
        
        try:
            save_logs_to_s3(s3_client, log_file_path)
            print(f"Processing logs saved to: {log_file_path}")
        except Exception as log_error:
            print(f"Error saving processing logs to S3: {log_error}")

        return df_spark

    except FileNotFoundError:
        log_file_status(archivo_encontrado=False, ruta_archivo=f"s3://{bucket}/{key_xlsx}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_log_path = f"{path_log}file_not_found_logs_{timestamp}.txt"
        try:
            save_logs_to_s3(s3_client, error_log_path)
            print(f"File not found logs saved to: {error_log_path}")
        except Exception as log_error:
            print(f"Error saving file not found logs to S3: {log_error}")
        
        raise
    
    except Exception as e:
        log_validation_failure(
            columna="proceso", 
            indice=0, 
            fallo=f"Error al procesar archivo Excel: {str(e)}",
            cumplio=0
        )
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        table_suffix = f"_{table_name}" if table_name else ""
        error_log_path = f"{path_log}processing_error_logs{table_suffix}_{timestamp}.txt"
        try:
            save_logs_to_s3(s3_client, error_log_path)
            print(f"Processing error logs saved to: {error_log_path}")
        except Exception as log_error:
            print(f"Error saving processing error logs to S3: {log_error}")
        
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
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = f"{path_log}file_verification_success_{timestamp}.txt"
        
        try:
            save_logs_to_s3(s3_client, log_file_path)
            print(f"File verification logs saved to: {log_file_path}")
        except Exception as log_error:
            print(f"Error saving file verification logs to S3: {log_error}")
        
        return True
        
    except s3_client.exceptions.NoSuchKey:
        print(f"ERROR: The file '{key}' does not exist in the bucket '{bucket}'.")
        
        log_file_status(archivo_encontrado=False, ruta_archivo=f"s3://{bucket}/{key}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_log_path = f"{path_log}file_not_found_verification_{timestamp}.txt"
        
        try:
            save_logs_to_s3(s3_client, error_log_path)
            print(f"File not found verification logs saved to: {error_log_path}")
        except Exception as log_error:
            print(f"Error saving file not found verification logs to S3: {log_error}")
        
        return False
        
    except Exception as e:
        print(f"Unexpected error accessing the file in S3: {str(e)}")
        
        log_validation_failure(
            columna="archivo_verificacion", 
            indice=0, 
            fallo=f"Error inesperado al verificar archivo: {str(e)}",
            cumplio=0
        )
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_log_path = f"{path_log}file_verification_error_{timestamp}.txt"
        
        try:
            save_logs_to_s3(s3_client, error_log_path)
            print(f"File verification error logs saved to: {error_log_path}")
        except Exception as log_error:
            print(f"Error saving file verification error logs to S3: {log_error}")
        
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
            
            if s3_client:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                error_log_path = f"{path_log}structure_validation_error_{timestamp}.txt"
                
                try:
                    save_logs_to_s3(s3_client, error_log_path)
                    print(f"Structure validation error logs saved to: {error_log_path}")
                except Exception as log_error:
                    print(f"Error saving structure validation error logs to S3: {log_error}")
            
            raise ValueError("Invalid column structure.")
        else:
            print("File structure validated successfully.")
            
            log_validation_failure(
                columna="estructura_archivo",
                indice=0,
                fallo="Estructura de archivo validada correctamente",
                cumplio=1
            )
            
            if s3_client:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                success_log_path = f"{path_log}structure_validation_success_{timestamp}.txt"
                
                try:
                    save_logs_to_s3(s3_client, success_log_path)
                    print(f"Structure validation success logs saved to: {success_log_path}")
                except Exception as log_error:
                    print(f"Error saving structure validation success logs to S3: {log_error}")
    
    except Exception as e:
        log_validation_failure(
            columna="estructura_validacion",
            indice=0,
            fallo=f"Error inesperado al validar estructura: {str(e)}",
            cumplio=0
        )
        
        if s3_client:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_log_path = f"{path_log}structure_validation_unexpected_error_{timestamp}.txt"
            
            try:
                save_logs_to_s3(s3_client, error_log_path)
                print(f"Structure validation unexpected error logs saved to: {error_log_path}")
            except Exception as log_error:
                print(f"Error saving structure validation unexpected error logs to S3: {log_error}")
        
        raise

def verificar_atributo(df, rules, s3_client, table_name=None):
    """
    Verifica atributos del DataFrame usando reglas de calidad.
    """
    try:
        df_original = df.toDF()
        df_original = df_original.withColumn("__row_id__", F.monotonically_increasing_id())
     
        df_with_index = DynamicFrame.fromDF(df_original, glueContext, "df_with_index")
        df_results = glue_quality(df_with_index, rules)
     
        df_failed = df_results.filter(F.col("outcome") == "Failed")
        df_failed = df_failed.withColumn("__row_id__", F.monotonically_increasing_id())
        df_final = df_failed.join(df_original.select("__row_id__", "excel_row"), on="__row_id__", how="left")
        
        failed_rows = df_final.collect()
        for row in failed_rows:
            log_row_validation_failure(
                row_data=row.asDict(),
                row_index=row["__row_id__"],
                column_name=row.get("column", "unknown"),
                validation_rule=row.get("rule", "unknown"),
                error_message=row.get("message", "Fallo desconocido"),
                excel_row=row.get("excel_row")
            )
        
        if failed_rows:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            table_suffix = f"_{table_name}" if table_name else ""
            log_file_path = f"{path_log}validation_logs{table_suffix}_{timestamp}.txt"
            
            try:
                save_logs_to_s3(s3_client, log_file_path)
                print(f"Validation logs saved to: {log_file_path}")
            except Exception as log_error:
                print(f"Error saving logs to S3: {log_error}")
     
        return df_final.drop("__row_id__")
    
    except Exception as e:
        log_validation_failure(
            columna="validacion", 
            indice=0, 
            fallo=f"Error al validar atributos: {str(e)}",
            cumplio=0
        )
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_log_path = f"{path_log}error_logs_{timestamp}.txt"
            save_logs_to_s3(s3_client, error_log_path)
            print(f"Error logs saved to: {error_log_path}")
        except Exception as log_error:
            print(f"Error saving error logs to S3: {log_error}")
        
        raise

"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F
import pandas as pd
from pathlib import Path

import sys, traceback, boto3, json, tempfile, zipfile
import xml.etree.ElementTree as ET

###################################################################
######## SET ARGS AND CONFIG COMMONS UTILS ########################
###################################################################

DB_SCHEMA = 'FILE'
DB_NAME = 'ULAES'
BUCKET_NAME = 'alfa-datalake-dev-bronce'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
CONNECT_NAME = 'ALFAPRD_QA_TEST'
PERIOD = '202501'
CERTIFICATE = 'False'
columnas_esperadas = ["excel_row", "COMPANIA", "TIPO_ULAE", "CODRAMO", "INDBANCAS", "PERIODO", "ULAE"]


PATH_JSON = f"support_phase_files/niif/PREV_EULAE/ULAES_GENERALES_AAAAMM.xlsx"
extension = Path(PATH_JSON).suffix  # ".xlsx"

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client("s3")

extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME, DB_NAME)

###################################################################
############## VALIDATE FILE IN S3 & READ JSON ####################
###################################################################

# Check if the file exists before attempting to read it
print(f"The file {PATH_JSON} exists in S3. Proceeding with the reading.")


# Check file
if not verificar_archivo(s3_client, BUCKET_NAME_ASSETS, PATH_JSON):
    sys.exit(1)

    
# Upload file and check structure
if extension == '.xlsx':
    frame_save = excel_s3_to_spark_df(s3_client, BUCKET_NAME_ASSETS, PATH_JSON, spark, glueContext)
    verificar_estructura_archivo(frame_save, columnas_esperadas)
    frame_save = DynamicFrame.fromDF(frame_save, glueContext, "frame_save_dynamic")
    print("xlsx file successfully loaded.")
else:
    response = s3_client.get_object(Bucket=BUCKET_NAME_ASSETS, Key=PATH_JSON)
    catalog_tables = json.loads(response["Body"].read().decode("utf-8"))  # Se usa .read()
    print("JSON file successfully loaded.")  
    
###################################################################
################# ITERATE FOR EACH TABLE ##########################
###################################################################

if DB_SCHEMA == 'FILE':
    try:
        partition_keys = []
        table_name = 'ULAES_GENERALES'
        s3_prefix = f"{DB_NAME}/{DB_SCHEMA}/{table_name}/"        
        s3_path = f"s3://{BUCKET_NAME}/{s3_prefix}"
        #extract_helper.save_files_s3(frame_save, s3_path, partition_keys)
        print("Successfully Extract ")
    except Exception as e:
        print(f"ERROR WITH INGEST: {e}")
else:
    for schema in catalog_tables:
        if schema in DB_SCHEMA:
            for table_info in catalog_tables[schema]:
                if table_info['status']:
                    logger.info(f"Tables for ingest: {schema}.{table_info['table']}")
                    try:
                        extract_helper.execute_data(
                            table_info['table'], 
                            table_info['partition_column'],  
                            table_info['logic'], 
                            connectname= CONNECT_NAME)
                        logger.info("Successfully Extract ")
                        
                    except Exception as e:
                        logger.info(f"ERROR WITH INGEST: {e}")
        else:
            logger.info("NOT TABLE FOR INGEST")
    

frame_save.show()
"""Script that quality bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import SelectFromCollection
from awsglue.job import Job
from awsglue import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality
from commons.LogsHelper import LogsHelper
from pyspark.sql import functions as F
from pathlib import Path

import sys, traceback, boto3, json, tempfile, zipfile
import xml.etree.ElementTree as ET


###################################################################
######## SET ARGS AND CONFIG COMMONS UTILS ########################
###################################################################


DB_SCHEMA = 'FILE'
DB_NAME = 'ULAES'
DB_CATALOG_GLUE = 'datalake-dev-bronce-catalog-db'
BUCKET_NAME = 'alfa-datalake-dev-bronce'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
CONNECT_NAME = 'ALFAPRD_QA_TEST'
PERIOD = '202501'
CERTIFICATE = 'False'

PATH_JSON = f"support_phase_files/niif/{DB_NAME.lower()}_{DB_SCHEMA.lower()}_rules_tables.json"
extension = Path(PATH_JSON).suffix  # ".xlsx"

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

###################################################################
############## VALIDATE FILE IN S3 & READ JSON ####################
###################################################################

# Check if the file exists before attempting to read it
print(f"The file {PATH_JSON} exists in S3. Proceeding with the reading.")


# Check file
if not verificar_archivo(s3_client, BUCKET_NAME_ASSETS, PATH_JSON):
    sys.exit(1)

    
# Upload file and check structure
if extension == '.xlsx':
    frame_save = excel_s3_to_spark_df(s3_client, BUCKET_NAME_ASSETS, PATH_JSON, spark, glueContext)
    verificar_estructura_archivo(frame_save, columnas_esperadas)
    frame_save = DynamicFrame.fromDF(frame_save, glueContext, "frame_save_dynamic")
    print("xlsx file successfully loaded.")
else:
    response = s3_client.get_object(Bucket=BUCKET_NAME_ASSETS, Key=PATH_JSON)
    catalog_tables = json.loads(response["Body"].read().decode("utf-8"))  # Se usa .read()
    print("JSON file successfully loaded.")  
    
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
                                df_fails = verificar_atributo(frame_to_evaluate, rules_str) # s3_client, table_info['table']
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

# Configuración de ruta S3 para logs
path_log = 's3://tu-bucket-de-logs/logs/validaciones/'  # Cambiar por la ruta S3 completa deseada

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

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        table_suffix = f"_{table_name}" if table_name else ""
        log_file_path = f"{path_log}processing_logs{table_suffix}_{timestamp}.txt"
        
        try:
            save_logs_to_s3(s3_client, log_file_path)
            print(f"Processing logs saved to: {log_file_path}")
        except Exception as log_error:
            print(f"Error saving processing logs to S3: {log_error}")

        return df_spark

    except FileNotFoundError:
        log_file_status(archivo_encontrado=False, ruta_archivo=f"s3://{bucket}/{key_xlsx}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_log_path = f"{path_log}file_not_found_logs_{timestamp}.txt"
        try:
            save_logs_to_s3(s3_client, error_log_path)
            print(f"File not found logs saved to: {error_log_path}")
        except Exception as log_error:
            print(f"Error saving file not found logs to S3: {log_error}")
        
        raise
    
    except Exception as e:
        log_validation_failure(
            columna="proceso", 
            indice=0, 
            fallo=f"Error al procesar archivo Excel: {str(e)}",
            cumplio=0
        )
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        table_suffix = f"_{table_name}" if table_name else ""
        error_log_path = f"{path_log}processing_error_logs{table_suffix}_{timestamp}.txt"
        try:
            save_logs_to_s3(s3_client, error_log_path)
            print(f"Processing error logs saved to: {error_log_path}")
        except Exception as log_error:
            print(f"Error saving processing error logs to S3: {log_error}")
        
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
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = f"{path_log}file_verification_success_{timestamp}.txt"
        
        try:
            save_logs_to_s3(s3_client, log_file_path)
            print(f"File verification logs saved to: {log_file_path}")
        except Exception as log_error:
            print(f"Error saving file verification logs to S3: {log_error}")
        
        return True
        
    except s3_client.exceptions.NoSuchKey:
        print(f"ERROR: The file '{key}' does not exist in the bucket '{bucket}'.")
        
        log_file_status(archivo_encontrado=False, ruta_archivo=f"s3://{bucket}/{key}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_log_path = f"{path_log}file_not_found_verification_{timestamp}.txt"
        
        try:
            save_logs_to_s3(s3_client, error_log_path)
            print(f"File not found verification logs saved to: {error_log_path}")
        except Exception as log_error:
            print(f"Error saving file not found verification logs to S3: {log_error}")
        
        return False
        
    except Exception as e:
        print(f"Unexpected error accessing the file in S3: {str(e)}")
        
        log_validation_failure(
            columna="archivo_verificacion", 
            indice=0, 
            fallo=f"Error inesperado al verificar archivo: {str(e)}",
            cumplio=0
        )
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_log_path = f"{path_log}file_verification_error_{timestamp}.txt"
        
        try:
            save_logs_to_s3(s3_client, error_log_path)
            print(f"File verification error logs saved to: {error_log_path}")
        except Exception as log_error:
            print(f"Error saving file verification error logs to S3: {log_error}")
        
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
            
            if s3_client:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                error_log_path = f"{path_log}structure_validation_error_{timestamp}.txt"
                
                try:
                    save_logs_to_s3(s3_client, error_log_path)
                    print(f"Structure validation error logs saved to: {error_log_path}")
                except Exception as log_error:
                    print(f"Error saving structure validation error logs to S3: {log_error}")
            
            raise ValueError("Invalid column structure.")
        else:
            print("File structure validated successfully.")
            
            log_validation_failure(
                columna="estructura_archivo",
                indice=0,
                fallo="Estructura de archivo validada correctamente",
                cumplio=1
            )
            
            if s3_client:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                success_log_path = f"{path_log}structure_validation_success_{timestamp}.txt"
                
                try:
                    save_logs_to_s3(s3_client, success_log_path)
                    print(f"Structure validation success logs saved to: {success_log_path}")
                except Exception as log_error:
                    print(f"Error saving structure validation success logs to S3: {log_error}")
    
    except Exception as e:
        log_validation_failure(
            columna="estructura_validacion",
            indice=0,
            fallo=f"Error inesperado al validar estructura: {str(e)}",
            cumplio=0
        )
        
        if s3_client:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_log_path = f"{path_log}structure_validation_unexpected_error_{timestamp}.txt"
            
            try:
                save_logs_to_s3(s3_client, error_log_path)
                print(f"Structure validation unexpected error logs saved to: {error_log_path}")
            except Exception as log_error:
                print(f"Error saving structure validation unexpected error logs to S3: {log_error}")
        
        raise

def verificar_atributo(df, rules, s3_client, table_name=None):
    """
    Verifica atributos del DataFrame usando reglas de calidad.
    """
    try:
        df_original = df.toDF()
        df_original = df_original.withColumn("__row_id__", F.monotonically_increasing_id())
     
        df_with_index = DynamicFrame.fromDF(df_original, glueContext, "df_with_index")
        df_results = glue_quality(df_with_index, rules)
     
        df_failed = df_results.filter(F.col("outcome") == "Failed")
        df_failed = df_failed.withColumn("__row_id__", F.monotonically_increasing_id())
        df_final = df_failed.join(df_original.select("__row_id__", "excel_row"), on="__row_id__", how="left")
        
        failed_rows = df_final.collect()
        for row in failed_rows:
            log_row_validation_failure(
                row_data=row.asDict(),
                row_index=row["__row_id__"],
                column_name=row.get("column", "unknown"),
                validation_rule=row.get("rule", "unknown"),
                error_message=row.get("message", "Fallo desconocido"),
                excel_row=row.get("excel_row")
            )
        
        if failed_rows:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            table_suffix = f"_{table_name}" if table_name else ""
            log_file_path = f"{path_log}validation_logs{table_suffix}_{timestamp}.txt"
            
            try:
                save_logs_to_s3(s3_client, log_file_path)
                print(f"Validation logs saved to: {log_file_path}")
            except Exception as log_error:
                print(f"Error saving logs to S3: {log_error}")
     
        return df_final.drop("__row_id__")
    
    except Exception as e:
        log_validation_failure(
            columna="validacion", 
            indice=0, 
            fallo=f"Error al validar atributos: {str(e)}",
            cumplio=0
        )
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_log_path = f"{path_log}error_logs_{timestamp}.txt"
            save_logs_to_s3(s3_client, error_log_path)
            print(f"Error logs saved to: {error_log_path}")
        except Exception as log_error:
            print(f"Error saving error logs to S3: {log_error}")
        
        raise

"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F
import pandas as pd
from pathlib import Path

import sys, traceback, boto3, json, tempfile, zipfile
import xml.etree.ElementTree as ET

###################################################################
######## SET ARGS AND CONFIG COMMONS UTILS ########################
###################################################################

DB_SCHEMA = 'FILE'
DB_NAME = 'ULAES'
BUCKET_NAME = 'alfa-datalake-dev-bronce'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
CONNECT_NAME = 'ALFAPRD_QA_TEST'
PERIOD = '202501'
CERTIFICATE = 'False'
columnas_esperadas = ["excel_row", "COMPANIA", "TIPO_ULAE", "CODRAMO", "INDBANCAS", "PERIODO", "ULAE"]

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
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
                                df_fails = verificar_atributo(frame_to_evaluate, rules_str, s3_client, table_info['table'])
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

    

def verificar_estructura_archivo(df, columnas_esperadas, s3_client=None):
    """
    Verifica si el DataFrame tiene las columnas esperadas en el orden correcto.
    Versión corregida que maneja correctamente los tipos de DataFrame y optimiza el logging.
    """
    try:
        # Manejo correcto de diferentes tipos de DataFrame
        from pyspark.sql import DataFrame as SparkDataFrame
        import pandas as pd
        
        if isinstance(df, SparkDataFrame):
            columnas_actuales = list(df.columns)
        elif isinstance(df, pd.DataFrame):
            columnas_actuales = df.columns.tolist()
        elif hasattr(df, 'columns'):
            columnas_actuales = list(df.columns)
        else:
            raise TypeError("df debe ser un Spark DataFrame o pandas DataFrame")
        
        errores = []
        
        # Validar longitud primero
        if len(columnas_actuales) != len(columnas_esperadas):
            error_msg = f"Número de columnas incorrecto. Esperadas: {len(columnas_esperadas)}, Encontradas: {len(columnas_actuales)}"
            errores.append(error_msg)
        
        # Validar cada columna solo si las longitudes coinciden o son similares
        min_length = min(len(columnas_actuales), len(columnas_esperadas))
        for idx in range(min_length):
            if columnas_actuales[idx] != columnas_esperadas[idx]:
                error_msg = f"Columna en posición {idx}: esperada '{columnas_esperadas[idx]}', encontrada '{columnas_actuales[idx]}'"
                errores.append(error_msg)
        
        # Verificar columnas faltantes si hay menos columnas actuales
        if len(columnas_actuales) < len(columnas_esperadas):
            for idx in range(len(columnas_actuales), len(columnas_esperadas)):
                error_msg = f"Columna faltante en posición {idx}: '{columnas_esperadas[idx]}'"
                errores.append(error_msg)
        
        # Registrar resultado en un solo log optimizado
        if errores:
            print("Invalid file structure:")
            for err in errores[:5]:  # Mostrar solo los primeros 5 errores en consola
                print(f"   - {err}")
            if len(errores) > 5:
                print(f"   ... y {len(errores) - 5} errores más")
            
            # Crear resumen de errores para el log
            error_summary = f"Errores de estructura ({len(errores)}): " + "; ".join(errores[:3])
            if len(errores) > 3:
                error_summary += f" ... y {len(errores) - 3} errores más"
            
            log_validation_failure(
                columna="estructura_archivo",
                indice=0,
                fallo=error_summary,
                cumplio=0
            )
            
            # Guardar logs solo una vez
            if s3_client:
                try:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    error_log_path = f"{path_log}structure_validation_error_{timestamp}.txt"
                    save_logs_to_s3(s3_client, error_log_path)
                    print(f"Structure validation error logs saved to: {error_log_path}")
                except Exception as log_error:
                    print(f"Error saving structure validation error logs to S3: {log_error}")
            
            raise ValueError("Estructura de columnas inválida")
        else:
            print("File structure validated successfully.")
            
            log_validation_failure(
                columna="estructura_archivo",
                indice=0,
                fallo="Estructura de archivo validada correctamente",
                cumplio=1
            )
            
            # Guardar logs de éxito solo una vez
            if s3_client:
                try:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    success_log_path = f"{path_log}structure_validation_success_{timestamp}.txt"
                    save_logs_to_s3(s3_client, success_log_path)
                    print(f"Structure validation success logs saved to: {success_log_path}")
                except Exception as log_error:
                    print(f"Error saving structure validation success logs to S3: {log_error}")
    
    except ValueError as ve:
        # Re-lanzar errores de validación sin logging adicional
        raise ve
    except TypeError as te:
        # Manejar errores de tipo específicamente
        log_validation_failure(
            columna="estructura_validacion",
            indice=0,
            fallo=f"Error de tipo de DataFrame: {str(te)}",
            cumplio=0
        )
        raise te
    except Exception as e:
        # Manejar errores inesperados
        log_validation_failure(
            columna="estructura_validacion",
            indice=0,
            fallo=f"Error inesperado al validar estructura: {str(e)}",
            cumplio=0
        )
        
        if s3_client:
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                error_log_path = f"{path_log}structure_validation_unexpected_error_{timestamp}.txt"
                save_logs_to_s3(s3_client, error_log_path)
                print(f"Structure validation unexpected error logs saved to: {error_log_path}")
            except Exception as log_error:
                print(f"Error saving structure validation unexpected error logs to S3: {log_error}")
        
        raise
# -----------------------------
# Imports y Configuración
# -----------------------------
import pandas as pd
import tempfile
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, col
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


# Configuración de rutas S3 para logs - se configurarán desde job parameters de AWS Glue
path_log = None
path_formato_ulaes = None

# Initialize a global dataframe to store logs with enhanced structure
log_columns = ["caso","columna", "indice", "timestamp", "origen_id_lote", "cumplio", "fallo", "regla"]
log_df = pd.DataFrame(columns=log_columns)

# -----------------------------
# Funciones de Logging Base
# -----------------------------
def log_validation_failure(caso,columna, indice, fallo, origen_id_lote=None, cumplio=0):
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
        "caso": caso,
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

def log_file_status(archivo_encontrado, ruta_archivo, case_use):
    """
    Logs file search status with 1 for found, 0 for not found.
    """
    valor_archivo = 1 if archivo_encontrado else 0
    fallo_msg = f"Archivo {'encontrado' if archivo_encontrado else 'no encontrado'} en ruta: {ruta_archivo}"
    log_validation_failure(
        caso = f"{case_use}",
        columna="archivo", 
        indice=0, 
        fallo=fallo_msg,
        cumplio=valor_archivo
    )

def log_column_validation(expected_columns, actual_columns, case_use, df_row_count=0):
    """
    Logs column presence and order validation results.
    """
    missing_columns = [col for col in expected_columns if col not in actual_columns]
    extra_columns = [col for col in actual_columns if col not in expected_columns]
    
    if missing_columns:
        fallo_msg = f"Faltan columnas requeridas: {', '.join(missing_columns)}"
        log_validation_failure(
            caso = f"{case_use}",
            columna="columnas_faltantes", 
            indice=0, 
            fallo=fallo_msg,
            cumplio=0
        )
    
    if extra_columns:
        fallo_msg = f"Columnas adicionales no esperadas: {', '.join(extra_columns)}"
        log_validation_failure(
            caso = f"{case_use}",
            columna="columnas_extra", 
            indice=0, 
            fallo=fallo_msg,
            cumplio=0
        )
    
    if not missing_columns and not extra_columns:
        log_validation_failure(
            caso = f"{case_use}",
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
                caso = f"{case_use}",
                columna="columnas_orden", 
                indice=0, 
                fallo=fallo_msg,
                cumplio=0
            )
        else:
            log_validation_failure(
                caso = f"{case_use}",
                columna="columnas_orden", 
                indice=0, 
                fallo="Todas las columnas están en el orden correcto",
                cumplio=1
            )


def log_row_validation_failure(df_dataquality, case_use):
    """
    Process DataFrame of DataQuality  and write errors usando log_validation_failure.
    """
    
    df_errores_pandas = df_dataquality.toPandas()
    
    for _, row in df_errores_pandas.iterrows():
        log_validation_failure(
            caso=f"{case_use}",
            columna=row.get("rule_type", "columna_desconocida"),
            indice=row.get("excel_row", -1),
            fallo=row.get("DataQualityRulesFail", "Sin detalle de fallo"),
            cumplio=0
        )


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
    Saves all accumulated logs to a single consolidated file in S3 in Parquet format only.
    """
    global log_df
    
    if log_df.empty:
        print("No logs to save.")
        return
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        table_suffix = f"_{table_name}" if table_name else ""
        
        # Save as Parquet format only
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

def save_format_file(s3_client, df_spark, path_files):
    """
    Saves the processed DataFrame as Format file in Parquet format (.parquet) in the specified S3 path.
    """
    try:
        
        timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

        path_save_files = f"{path_files}/fecha_carga={timestamp}/"
        
        print(f"test save {path_save_files}")
        
        # Save the DataFrame in Parquet format to the output path
        logger.info(f'Saving the file in Parquet format to {path_save_files}')
        df_spark.write.mode("overwrite").parquet(path_save_files)
        logger.info('File successfully saved in Parquet format')
        
        print(f"Parquet file successfully saved to: {path_save_files}")
            
    except Exception as e:
        print(f"Error saving Format Parquet file: {str(e)}")
        log_validation_failure(
            caso = f"{case_use}",
            columna="format_file", 
            indice=0, 
            fallo=f"Error al guardar archivo Format Parquet: {str(e)}",
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
def excel_s3_to_spark_df(s3_client, bucket, key_xlsx, spark, glueContext, case_use):
    """
    Convierte archivo Excel de S3 a Spark DataFrame con logging integrado.
    """
    try:
        temp_xlsx = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        s3_client.download_file(bucket, key_xlsx, temp_xlsx.name)
        log_file_status(archivo_encontrado=True, ruta_archivo=f"s3://{bucket}/{key_xlsx}", case_use=case_use)

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
        log_file_status(archivo_encontrado=False, ruta_archivo=f"s3://{bucket}/{key_xlsx}", case_use=case_use)
        raise
    
    except Exception as e:
        log_validation_failure(
            caso = f"{case_use}",
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
            key="rowLevelOutcomes",
            transformation_ctx="rowLevelOutcomes",
        )
        
        return ruleOutcomes.toDF()
    
    except Exception as e:
        log_validation_failure(
            caso = f"{case_use}",
            columna="calidad", 
            indice=0, 
            fallo=f"Error en evaluación de calidad: {str(e)}",
            cumplio=0
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
        print(f"Checking file existence: s3://{bucket}/{key}")
        s3_client.head_object(Bucket=bucket, Key=key)
        print(f"SUCCESS: The file '{key}' exists in the bucket '{bucket}'.")
        
        log_file_status(archivo_encontrado=True, ruta_archivo=f"s3://{bucket}/{key}",case_use=case_use)
        
        return True
        
    except s3_client.exceptions.NoSuchKey:
        print(f"ERROR: The file '{key}' does not exist in the bucket '{bucket}'.")
        print(f"Full path checked: s3://{bucket}/{key}")
        
        log_file_status(archivo_encontrado=False, ruta_archivo=f"s3://{bucket}/{key}", case_use=case_use)
        
        return False
        
    except Exception as e:
        print(f"Unexpected error accessing the file in S3: {str(e)}")
        print(f"Full path checked: s3://{bucket}/{key}")
        print(f"Error type: {type(e).__name__}")
        
        log_validation_failure(
            caso = f"{case_use}",
            columna="archivo_verificacion", 
            indice=0, 
            fallo=f"Error inesperado al verificar archivo: {str(e)}",
            cumplio=0
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
                    caso = f"{case_use}",
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
                caso = f"{case_use}",
                columna="estructura_archivo",
                indice=0,
                fallo=f"Estructura de archivo inválida. Errores encontrados: {len(errores)}",
                cumplio=0
            )
            
            raise ValueError("Invalid column structure.")
        else:
            print("File structure validated successfully.")
            
            log_validation_failure(
                caso = f"{case_use}",
                columna="estructura_archivo",
                indice=0,
                fallo="Estructura de archivo validada correctamente",
                cumplio=1
            )
    
    except Exception as e:
        log_validation_failure(
            caso = f"{case_use}",
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
    df_failed = df_results.filter(F.col("DataQualityEvaluationResult") == "Failed")
    
    df_failed = df_failed.withColumn("DataQualityRulesFail", explode(col("DataQualityRulesFail")))
    
    print(f"TEST QUALITY")
    print(df_failed.show(truncate = False))

    return df_failed

# -----------------------------
# Funciones de disponibilidad
# -----------------------------


def move_file(s3_client, bucket, source_key, case_use, file, extension, status):
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    
    
    dest_key = f"{case_use}/{status}/{file}/fecha_carga={timestamp}/{file}{extension}"
    
    try:
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': source_key},
            Key=dest_key
        )
        s3_client.delete_object(Bucket=bucket, Key=source_key)
        print(f"File moved to path {status}: s3://{bucket}/{dest_key}")
        
    except Exception as e:
        
        print(f"Error moving file to {status}: {e}")

"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F
import pandas as pd
from pathlib import Path

import sys, traceback, boto3, json, tempfile, zipfile
import xml.etree.ElementTree as ET

###################################################################
######## SET ARGS AND CONFIG COMMONS UTILS ########################
###################################################################


 #key_file = f"ULAES_VIDA/IN/{file}{EXTENSION}"

DB_SCHEMA = 'FILE'
DB_NAME = 'ULAES'
BUCKET_NAME = 'alfa-datalake-dev-bronce'
BUCKET_NAME_ASSETS = 'alfa-datalake-dev-assets'
BUCKET_SOURCE = 'alfa-datalake-dev-langding'
CONNECT_NAME = 'ALFAPRD_QA_TEST'
PERIOD = '202501'
CERTIFICATE = 'False'
DB_CATALOG_GLUE = 'datalake-dev-bronce-catalog-db'

# Configure path variables from job parameters
path_log = 's3://alfa-datalake-dev-silver/LOGS/FILES_VALIDATION/'
path_formato_ulaes = 's3://alfa-datalake-dev-bronce/ULAES/ARCHIVOS/'
EXCEL_FILE_PATH = 's3://alfa-datalake-dev-langding/NIIF_ULAES/ULAES_GENERALES_AAAAMM.xlsx'
FILES = '{ "NIIF17_ULAES_VIDA": "ULAES_VIDA",     "NIIF17_ULAES_GENERALES": "ULAES_GENERALES",     "NIIF17_PREVISIONAL": "ARCHIVO_CESION_PREVISIONALES" }'
EXTENSION = '.xlsx'


# Parse FILES from job parameters (JSON string to Python list)
files = json.loads('{ "NIIF17_ULAES_VIDA": "ULAES_VIDA",     "NIIF17_ULAES_GENERALES": "ULAES_GENERALES",     "NIIF17_PREVISIONAL": "ARCHIVO_CESION_PREVISIONALES" }')
# Parse COLUMNAS_ESPERADAS from job parameters (JSON string to Python list)
columnas_esperadas = {
    "NIIF17_ULAES_VIDA": json.loads('["excel_row","COMPANIA","TIPO_ULAE","CODRAMO","INDBANCAS","PERIODO","ULAE"]'),
    "NIIF17_ULAES_GENERALES": json.loads('["excel_row","COMPANIA","TIPO_ULAE","CODRAMO","INDBANCAS","PERIODO","ULAE"]'),
    "NIIF17_PREVISIONAL": json.loads('["excel_row","POLIZA","REVERSION","REAL","CAUSACION","EMISION_P&G","INICIO_DE_VIGENCIA","FIN_DE_VIGENCIA","MES_DE_CIERRE","VALOR_PRIMA_CEDIDA"]')
}



# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client("s3")

extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME, DB_NAME)
for case_use, file in files.items():
    
    # Check if the file exists before attempting to read it
    # File Dictionary - Using parameters from job configuration
    excel_file_path = f"s3://{BUCKET_SOURCE}/{case_use}/IN/{file}{EXTENSION}"
    rules_file_path = f"s3://{BUCKET_NAME_ASSETS}/support_phase_files/niif/{file.lower()}_rules_tables.json"
    bronce_file_path = f"s3://{BUCKET_NAME}/{case_use}/{file}"
    
    
    key_file = f"{case_use}/IN/{file}{EXTENSION}"
    key_rules = f"support_phase_files/niif/{file.lower()}_rules_tables.json"
    
    columnas_esperadas_case = columnas_esperadas[case_use]
    
    print(f"columnas: {columnas_esperadas_case}")
    print(f"case_use: {case_use}, key_file: {key_file} ")
    
    if verificar_archivo(s3_client, BUCKET_SOURCE, key_file, case_use):
        
        print(f"Loading file and Checking structure from s3://{excel_file_path}")
        
        frame_save = excel_s3_to_spark_df(s3_client, BUCKET_SOURCE, key_file, spark, glueContext, case_use)
        verificar_estructura_archivo(frame_save, columnas_esperadas_case, case_use)
        
        # Normalizar columnas antes de evaluar reglas
        columnas_string_por_archivo = {
            "NIIF17_ULAES_VIDA": ["CODRAMO", "PERIODO", "INDBANCAS"],
            "NIIF17_ULAES_GENERALES": ["CODRAMO", "PERIODO", "INDBANCAS"],
            "NIIF17_PREVISIONAL": []  # No requiere conversión
        }
        
        columnas_a_string = columnas_string_por_archivo.get(case_use, [])
        
        for col_name in columnas_a_string:
            if col_name in frame_save.columns:
                if col_name == "INDBANCAS":
                    frame_save = frame_save.withColumn(
                        col_name,
                        F.when(F.col(col_name).isNull(), "").otherwise(F.col(col_name)).cast("string")
                    )
                else:
                    frame_save = frame_save.withColumn(
                        col_name, F.col(col_name).cast("string")
                    )
        
        # Convert to DynamicFrame
        frame_save = DynamicFrame.fromDF(frame_save, glueContext, "frame_save_dynamic")
        print(f"Excel file uploaded and validated: {excel_file_path}")
        
        if verificar_archivo(s3_client, BUCKET_NAME_ASSETS, key_rules, case_use):
            
            print(f"Loading rules from s3://{rules_file_path}")
            
            response = s3_client.get_object(Bucket=BUCKET_NAME_ASSETS, Key=key_rules)
            catalog_rules = json.loads(response["Body"].read().decode("utf-8"))
            
            print("Rules JSON loaded successfully")
            
            
            # Dictionary to store EvaluateDataQualityMultiframe for each table
    
            for schema in catalog_rules:
                if schema in DB_SCHEMA:
                    for table_info in catalog_rules[schema]:
                            if table_info['status']:
                                try:
                                    frame_to_evaluate = frame_save
                                    #print(f"Tables for ingest: {schema}.{table_info['rules']}")
                                    
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
                                    
                                    if df_failes_outcomes_total.count() == 0:
                                        # Save the processed DataFrame as file
                                        try:
                                            save_format_file(s3_client, frame_save.toDF(), bronce_file_path)
                                            print("File saved successfully.")
                                            move_file(s3_client, BUCKET_SOURCE, key_file, case_use, file, EXTENSION, status = 'OUT')
                                            
                                        except Exception as e:
                                            move_file(s3_client, BUCKET_SOURCE, key_file, case_use, file, EXTENSION, status = 'ERROR')
                                            print(f"Error saving File: {e}")
                                    else:
                                        
                                        log_row_validation_failure(df_failes_outcomes_total, case_use)
                                        move_file(s3_client, BUCKET_SOURCE, key_file, case_use, file, EXTENSION, status = 'ERROR')
                                        print(f"Data Quality is Wrong")
                                        
                                except Exception as e:
                                    
                                    print(f"ERROR WITH Evaluate Data Quality: {e}")
                else:
                    
                    print("NOT TABLE FOR Evaluate Data Quality")
            
        else: 
            move_file(s3_client, BUCKET_SOURCE, key_file, case_use, file, EXTENSION, status = 'ERROR')
            print(f"CRITICAL ERROR: File 's3://{BUCKET_SOURCE}/{case_use}/IN/{file}{EXTENSION}' not found in bucket '{BUCKET_NAME_ASSETS}'. Please verify the path and permissions.")
            
        
    else:
        move_file(s3_client, BUCKET_SOURCE , key_file, case_use, file, EXTENSION, status = 'ERROR')
        print(f"CRITICAL ERROR: File 's3://{BUCKET_SOURCE}/{case_use}/IN/{file}{EXTENSION}' not found in bucket '{BUCKET_SOURCE}'. Please verify the path and permissions.")
        
        


# Save all accumulated logs to a single consolidated file
try:
    save_consolidated_logs(s3_client, table_name="ulaes_generales")
    print("All processing logs have been consolidated and saved successfully.")
except Exception as e:
    print(f"Error saving consolidated logs: {e}")

# End of job
job.commit()

frame_save.show()
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('datalake-dev-artefacto')
import pandas as pd
from io import StringIO
import json

csv_data = """
ID_ARTEFACTO,ARTEFACTO_ID,TIPO_ARTEFACTO_ID,CODIGO_ARTEFACTO,NOMBRE_ARTEFACTO,DESCRIPCION_ARTEFACTO,FECHA_CREACION,FECHA_ACTUALIZACION,ELIMINADO
1,,4,SLN_PRY_NIIF_17,Proyecto NIIF17,,2025-08-03,,0
2,1,5,RPS_S3_LND,S3 Landing,,2025-08-03,,0
3,1,5,RPS_S3_BRZ,S3 Bronze,,2025-08-03,,0
4,1,5,RPS_S3_SLV,S3 Silver,,2025-08-03,,0
5,1,5,RPS_S3_GLD,S3 Gold,,2025-08-03,,0
"""  # Pega aquí todos los registros adicionales si quieres

# Leer el CSV desde el string
df = pd.read_csv(StringIO(csv_data))

# Renombrar columnas a camelCase
df.columns = [
    "idArtefacto", "artefactoId", "tipoArtefactoId", "codigoArtefacto",
    "nombreArtefacto", "descripcionArtefacto", "fechaCreacion",
    "fechaActualizacion", "eliminado"
]

# Reemplazar valores nulos con cadena vacía
df = df.fillna("")

# Convertir a lista de diccionarios
items = df.to_dict(orient="records")

# Mostrar el JSON formateado
print(json.dumps(items, indent=2, ensure_ascii=False))

items = [
  {
    "idArtefacto": "77",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_ARCHIVO_ULAES_VIDA ",
    "nombreArtefacto": "Funcion VERIFICAR ARCHIVO ULAES_VIDA",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "78",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_ARCHIVO_ULAES_GENERALES",
    "nombreArtefacto": "Funcion VERIFICAR ARCHIVO ULAES_GENERALES",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "79",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_ARCHIVO_PREVISIONALES",
    "nombreArtefacto": "Funcion VERIFICAR ARCHIVO PREVISIONALES",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "80",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_ESTRUCTURA_ULAES_VIDA      ",
    "nombreArtefacto": "Funcion VERIFICAR ESTRUCTURA ULAES_VIDA",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "81",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_ESTRUCTURA_ULAES_GENERALES",
    "nombreArtefacto": "Funcion VERIFICAR ESTRUCTURA ULAES_GENERALES",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "82",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_ESTRUCTURA_PREVISIONALES",
    "nombreArtefacto": "Funcion VERIFICAR ESTRUCTURA PREVISIONALES",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "83",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_NULIDAD_ULAES_VIDA",
    "nombreArtefacto": "Funcion VERIFICAR NULIDAD ULAES_VIDA",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "84",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_NULIDAD_ULAES_GENERALES",
    "nombreArtefacto": "Funcion VERIFICAR NULIDAD ULAES_GENERALES",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "85",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_NULIDAD_PREVISIONALES ",
    "nombreArtefacto": "Funcion VERIFICAR NULIDAD PREVISIONALES",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "86",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_ESPACIO_ULAES_VIDA",
    "nombreArtefacto": "Funcion VERIFICAR ESPACIO ULAES_VIDA",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "87",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_ESPACIO_ULAES_GENERALES",
    "nombreArtefacto": "Funcion VERIFICAR ESPACIO ULAES_GENERALES",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "88",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_ESPACIO_PREVISIONALES",
    "nombreArtefacto": "Funcion VERIFICAR ESPACIO PREVISIONALES",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "89",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_TIPO_ULAES_VIDA",
    "nombreArtefacto": "Funcion VERIFICAR TIPO ULAES_VIDA",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "90",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_TIPO_ULAES_GENERALES",
    "nombreArtefacto": "Funcion VERIFICAR TIPO ULAES_GENERALES",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "91",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_TIPO_PREVISIONALES",
    "nombreArtefacto": "Funcion VERIFICAR TIPO PREVISIONALES",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "92",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_DOMINIO_ULAES_VIDA",
    "nombreArtefacto": "Funcion VERIFICAR DOMINIO ULAES_VIDA",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "93",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_DOMINIO_ULAES_GENERALES ",
    "nombreArtefacto": "Funcion VERIFICAR DOMINIO ULAES_GENERALES",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "94",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_DOMINIO_PREVISIONALES",
    "nombreArtefacto": "Funcion VERIFICAR DOMINIO PREVISIONALES",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "95",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_SIMBOLO_ULAES_VIDA ",
    "nombreArtefacto": "Funcion VERIFICAR SIMBOLO ULAES_VIDA",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "96",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_SIMBOLO_ULAES_GENERALES",
    "nombreArtefacto": "Funcion VERIFICAR SIMBOLO ULAES_GENERALES",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "97",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_SIMBOLO_PREVISIONALES",
    "nombreArtefacto": "Funcion VERIFICAR SIMBOLO PREVISIONALES",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "98",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_FORMATO_ULAES_VIDA",
    "nombreArtefacto": "Funcion VERIFICAR FORMATO ULAES_VIDA",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "99",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_FORMATO_ULAES_GENERALES",
    "nombreArtefacto": "Funcion VERIFICAR FORMATO ULAES_GENERALES",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefacto": "100",
    "artefactoId": "1",
    "tipoArtefactoId": "8",
    "codigoArtefacto": "FUNC_VERIFICAR_FORMATO_PREVISIONALES ",
    "nombreArtefacto": "Funcion VERIFICAR FORMATO PREVISIONALES",
    "descripcionArtefacto": "",
    "fechaCreacion": "2025-08-03",
    "fechaActualizacion": "",
    "eliminado": "0"
  }
]
with table.batch_writer() as batch:
    for item in items:
        batch.put_item(Item=item)
for item in items: 
    print(item)
import boto3
import os
import logging
from botocore.exceptions import ClientError
logger = logging.getLogger()
logger.setLevel(logging.INFO)
DYNAMODB = boto3.resource('dynamodb')
def denormalize_and_save(artefacto_id: str):
    """
    Lee los datos normalizados para un artefacto, los denormaliza y los guarda en la tabla de cache.
    """
        
    logger.info(f"Iniciando denormalización para artefacto_id: {artefacto_id}")
    
    index_acn_table = "artefactoId-index"
    
    try:
        response = source_acn_table.query(
            IndexName=index_acn_table,
            KeyConditionExpression=boto3.dynamodb.conditions.Key('artefactoId').eq(artefacto_id)
        )
        acn_items = response.get('Items', [])
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.error(f"El GSI '{index_acn_table}' no fue encontrado. Por favor, créalo.")
        raise

    if not acn_items:
        logger.warning(f"No se encontraron relaciones para el artefacto_id: {artefacto_id}")
        return
        
    # 2. Recolectar todos los IDs necesarios para la lectura en lote (Batch)
    concepto_ids = list(set(item['conceptoId'] for item in acn_items if 'conceptoId' in item))
    nota_ids = list(set(item['notaId'] for item in acn_items if 'notaId' in item))

    # 3. Usar BatchGetItem para leer todos los conceptos y notas en solo dos llamadas
    try:
        batch_response = DYNAMODB.batch_get_item(
            RequestItems={
                source_concepto_table.name: {'Keys': [{'idConcepto': id} for id in concepto_ids]},
                source_nota_table.name: {'Keys': [{'idNota': id} for id in nota_ids]}
            }
        )
        conceptos_data = {item['idConcepto']: item for item in batch_response['Responses'][source_concepto_table.name]}
        notas_data = {item['idNota']: item for item in batch_response['Responses'][source_nota_table.name]}
    except ClientError as e:
        logger.error(f"Error en batch_get_item: {e}")
        return

    # 4. Obtener la información del artefacto principal
    artefacto_data = source_artefacto_table.get_item(Key={'idArtefacto': artefacto_id}).get('Item', {})

    # 5. Construir el objeto JSON denormalizado final
    parametros_list = []
    for item in acn_items:
        concepto_id = item.get('conceptoId')
        nota_id = item.get('notaId')
        parametros_list.append({
            "concepto": conceptos_data.get(concepto_id, {}),
            "nota": notas_data.get(nota_id, {})
        })

    final_object = {
        "caso_de_uso": artefacto_id, # La PK de la tabla de cache
        "artefacto": artefacto_data,
        "parametros": parametros_list
    }        
    
    return final_object
# --- Referencias a las Tablas ---
source_artefacto_table = DYNAMODB.Table("datalake-dev-artefacto")
source_acn_table = DYNAMODB.Table("datalake-dev-artefacto-concepto-nota-params")
source_concepto_table = DYNAMODB.Table("datalake-dev-concepto")
source_nota_table = DYNAMODB.Table("datalake-dev-nota")
try:
    response = source_artefacto_table.scan(ProjectionExpression="idArtefacto")
    artefactos = response.get('Items', [])

    # Manejar paginación si la tabla de artefactos es grande
    while 'LastEvaluatedKey' in response:
        response = source_artefacto_table.scan(
            ProjectionExpression="idArtefacto",
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        artefactos.extend(response.get('Items', []))

except ClientError as e:
    logger.error(f"Error al escanear la tabla de artefactos: {e}")
from typing import Dict, Any, Optional, List
def _find_nota_by_id_concepto(parametros_list: List[Dict[str, Any]], concepto_id: str) -> List[str]:
    """
    Devuelve una lista con todos los 'valorNota' que coinciden con el 'idConcepto' dado.
    Si no se encuentran coincidencias, devuelve una lista vacía.
    """
    resultados = []

    for param_entry in parametros_list:
        concepto = param_entry.get('concepto', {})
        nota = param_entry.get('nota', {})

        if concepto.get('idConcepto') == concepto_id:
            valor_nota = nota.get('valorNota')
            nota_id = nota.get('idNota')
            if valor_nota and nota_id:
                resultados.append((int(nota_id), valor_nota))  # Cast para orden correcto

    # Ordenar por notaId
    resultados.sort(key=lambda x: x[0])

    # Retornar solo los valoresNota, ordenados
    return [valor for _, valor in resultados]
def get_params_drt(parametros_obj: Dict[str, Any],tipo_artefacto_id: str = "5",id_concepto_llave: str = "6", id_concepto_valor: str = "7" ) -> Optional[Dict[str, str]]:
        """
        Si el artefacto es del tipo correcto, construye un diccionario {llave: valor}
        usando los valores de nota de dos conceptos específicos.

        Por defecto, usa el valor de la nota del concepto 6 como llave y el del 7 como valor.

        Args:
            parametros_obj: El objeto denormalizado completo.
            tipo_artefacto_id: El ID del tipo de artefacto a validar.
            id_concepto_llave: El 'idConcepto' cuyo 'valorNota' será la llave del diccionario.
            id_concepto_valor: El 'idConcepto' cuyo 'valorNota' será el valor del diccionario.

        Returns:
            Un diccionario con una sola entrada {llave: valor}, o None si no se cumplen
            las condiciones.
        """
        # Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None
            
        artefacto = parametros_obj.get("artefacto", {})            
        if str(artefacto.get("tipoArtefactoId")) != str(tipo_artefacto_id):
            return None # No es el tipo de artefacto que nos interesa, salimos silenciosamente.

        # Usamos la función auxiliar para encontrar nuestras dos piezas de información
        parametros_list = parametros_obj.get("parametros", [])          
        llave = _find_nota_by_id_concepto(parametros_list, id_concepto_llave)
        valor = _find_nota_by_id_concepto(parametros_list, id_concepto_valor)       
        
        # Construir el resultado solo si encontramos ambas partes
        if llave and valor:
            # Usamos .lower() para que coincida exactamente con tu ejemplo "{landing: ...}"
            resultado = {llave[0].lower(): valor[0]}
            return resultado
        
        logging.warning(
            f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
            f"encontraron los conceptos para la llave (ID {id_concepto_llave}) "
            f"o el valor (ID {id_concepto_valor})."
        )
        return None
def get_params_ubi(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "6", id_concepto_llave: str = "18",id_concepto_valor: str = "7") -> Optional[Dict[str, List[str]]]:
        """
        Si un artefacto es del tipo '6', extrae un caso de uso y construye una
        lista de sus carpetas asociadas (IN, OUT, ERROR, etc.).

        La lógica específica es:
        - La llave del diccionario se obtiene del 'valorNota' asociado al 'idConcepto' 18.
        - Los valores de la lista se obtienen de los 'valorNota' asociados al 'idConcepto' 7.

        Args:
            parametros_obj: El objeto denormalizado completo.

        Returns:
            Un diccionario con el formato {caso_de_uso: [lista_de_carpetas]} o None
            si no se cumplen las condiciones.
        """

        # 1. Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None

        artefacto = parametros_obj.get("artefacto", {})
        if str(artefacto.get("tipoArtefactoId")) != tipo_artefacto_id:
            return None # No es el tipo de artefacto que nos interesa, salimos.

        # 2. variables para recolectar la llave y los valores                

        parametros_list = parametros_obj.get("parametros", [])          
        llave = _find_nota_by_id_concepto(parametros_list, id_concepto_llave)
        valor = _find_nota_by_id_concepto(parametros_list, id_concepto_valor)     

        # 4. Construir el resultado final si encontramos todo lo necesario
        if llave and valor:
            # Filtramos la lista de valores para excluir el valor que es igual a la llave
            valores_finales = [v for v in valor if v != llave[0]]

            return {llave[0]: valores_finales}

        logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {id_concepto_llave}) o no había valores (ID {id_concepto_valor}).")
        return None
def get_params_arch(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "1", id_concepto_llave: str = "18",id_concepto_valor: list = ["6", "8", "19"]) -> Optional[Dict[str, List[str]]]:
        """
        Si un artefacto es del tipo '6', extrae un caso de uso y construye una
        lista de sus carpetas asociadas (IN, OUT, ERROR, etc.).

        La lógica específica es:
        - La llave del diccionario se obtiene del 'valorNota' asociado al 'idConcepto' 18.
        - Los valores de la lista se obtienen de los 'valorNota' asociados al 'idConcepto' 7.

        Args:
            parametros_obj: El objeto denormalizado completo.

        Returns:
            Un diccionario con el formato {caso_de_uso: [lista_de_carpetas]} o None
            si no se cumplen las condiciones.
        """

        # 1. Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None

        artefacto = parametros_obj.get("artefacto", {})
        if str(artefacto.get("tipoArtefactoId")) != tipo_artefacto_id:
            return None # No es el tipo de artefacto que nos interesa, salimos.

        # 2. variables para recolectar la llave y los valores                
        
        parametros_list = parametros_obj.get("parametros", [])          
        llave = _find_nota_by_id_concepto(parametros_list, id_concepto_llave)        
        valores = []
        for valor_id in id_concepto_valor:
            valor = _find_nota_by_id_concepto(parametros_list, valor_id)
            valores.append(valor[0])

        # 4. Construir el resultado final si encontramos todo lo necesario
        if llave and valor:
            # Filtramos la lista de valores para excluir el valor que es igual a la llave            

            return {llave[0]: {"file":valores[0] ,"ext": valores[1], "sheet": valores[2]}}

        logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {id_concepto_llave}) o no había valores (ID {id_concepto_valor}).")
        return None
def get_params_stru(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "2", id_concepto_llave: str = "18",id_concepto_valor: str = "10") -> Optional[Dict[str, List[str]]]:
        """
        Si un artefacto es del tipo '6', extrae un caso de uso y construye una
        lista de sus carpetas asociadas (IN, OUT, ERROR, etc.).

        La lógica específica es:
        - La llave del diccionario se obtiene del 'valorNota' asociado al 'idConcepto' 18.
        - Los valores de la lista se obtienen de los 'valorNota' asociados al 'idConcepto' 7.

        Args:
            parametros_obj: El objeto denormalizado completo.

        Returns:
            Un diccionario con el formato {caso_de_uso: [lista_de_carpetas]} o None
            si no se cumplen las condiciones.
        """

        # 1. Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None

        artefacto = parametros_obj.get("artefacto", {})
        if str(artefacto.get("tipoArtefactoId")) != tipo_artefacto_id:
            return None # No es el tipo de artefacto que nos interesa, salimos.

        # 2. variables para recolectar la llave y los valores                
        
        parametros_list = parametros_obj.get("parametros", [])          
        llave = _find_nota_by_id_concepto(parametros_list, id_concepto_llave)        
        valor = _find_nota_by_id_concepto(parametros_list, id_concepto_valor)     

        # 4. Construir el resultado final si encontramos todo lo necesario
        if llave and valor:
            # Filtramos la lista de valores para excluir el valor que es igual a la llave            

            return {llave[0]: valor[0]}

        
        logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {id_concepto_llave}) o no había valores (ID {id_concepto_valor}).")
        return None
def get_params_atr(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "3", id_concepto_llave: str = "18",id_concepto_valor: list = ["6","12","13","14","15","16","20","21"]) -> Optional[Dict[str, List[str]]]:
        """
        Si un artefacto es del tipo '6', extrae un caso de uso y construye una
        lista de sus carpetas asociadas (IN, OUT, ERROR, etc.).

        La lógica específica es:
        - La llave del diccionario se obtiene del 'valorNota' asociado al 'idConcepto' 18.
        - Los valores de la lista se obtienen de los 'valorNota' asociados al 'idConcepto' 7.

        Args:
            parametros_obj: El objeto denormalizado completo.

        Returns:
            Un diccionario con el formato {caso_de_uso: [lista_de_carpetas]} o None
            si no se cumplen las condiciones.
        """

        # 1. Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None

        artefacto = parametros_obj.get("artefacto", {})                
        if str(artefacto.get("tipoArtefactoId")) != tipo_artefacto_id:
            return None # No es el tipo de artefacto que nos interesa, salimos.

        # 2. variables para recolectar la llave y los valores                
        
        parametros_list = parametros_obj.get("parametros", [])                       
        
        resultados = []

        for param in parametros_list:
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})

            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")

            if nombre_concepto and valor_nota and nota_id:
                resultados.append((int(nota_id), nombre_concepto, valor_nota))
        
        resultados.sort(key=lambda x: x[0])

        # Construir diccionario final
        resultado_final = {nombre.replace(" ", "_"): valor for _, nombre, valor in resultados}        

        logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {id_concepto_llave}) o no había valores (ID {id_concepto_valor}).")
        return None
def get_params_func_arch(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "8", id_concepto_llave: str = "18",id_concepto_valor: list = ["6","7","8"]) -> Optional[Dict[str, List[str]]]:

        # 1. Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None

        artefacto = parametros_obj.get("artefacto", {})       
        if not (str(artefacto.get("tipoArtefactoId")) == tipo_artefacto_id and any(keyword in str(artefacto.get("codigoArtefacto", "")) for keyword in ["VERIFICAR_ARCHIVO", "VERIFICAR_ESTRUCTURA"])):
            return None # No es el tipo de artefacto que nos interesa, salimos.

        # 2. variables para recolectar la llave y los valores                
        
        parametros_list = parametros_obj.get("parametros", [])                       
        
        resultados = []

        for param in parametros_list:
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})

            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")
                        

            if nombre_concepto and valor_nota and nota_id:
                resultados.append((int(nota_id), nombre_concepto, valor_nota))
        
        resultados.sort(key=lambda x: x[0])

        # Construir diccionario final
        resultado_final = {nombre.replace(" ", "_"): valor for _, nombre, valor in resultados}                
        
        caso_de_uso = resultado_final.get('Caso_de_uso')
        lista_atributos = resultado_final.get('Lista_Atributos')

        if lista_atributos:
            return {
                caso_de_uso: lista_atributos
            }
        else:
            ubicacion = resultado_final.get('Ubicación', '')
            nombre = resultado_final.get('Nombre', '')
            extension = resultado_final.get('Extensión', '')
            return {
                caso_de_uso: f"{ubicacion}/{nombre}{extension}"
            }         

        logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {id_concepto_llave}) o no había valores (ID {id_concepto_valor}).")
        return None    
def get_params_func_nul(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "8") -> Optional[Dict[str, List[str]]]:

        # 1. Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None

        artefacto = parametros_obj.get("artefacto", {})       
        if not (str(artefacto.get("tipoArtefactoId")) == tipo_artefacto_id and any(keyword in str(artefacto.get("codigoArtefacto", "")) for keyword in ["VERIFICAR_NULIDAD"])):
            return None # No es el tipo de artefacto que nos interesa, salimos.

        # 2. variables para recolectar la llave y los valores                
        
        parametros_list = parametros_obj.get("parametros", [])                       
        
        resultados = []

        for param in parametros_list:
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})

            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")                        

            if nombre_concepto and valor_nota and nota_id:
                resultados.append((int(nota_id), nombre_concepto, valor_nota))
        
        resultados.sort(key=lambda x: x[0])                
        
        # Separar nombres y reglas
        nombres = [valor for _, tipo, valor in resultados if tipo == 'Nombre']
        reglas = [valor for _, tipo, valor in resultados if tipo == 'Regla']  
        caso_de_uso = next((valor for _, tipo, valor in resultados if tipo == 'Caso de uso'), '')


        # Emparejar regla con nombre (por posición)
        resultado_formateado = ','.join(
            f'{regla} "{nombre}"' for regla, nombre in zip(reglas, nombres)
        )
                                     

        # Retornar en diccionario 
        
        return {caso_de_uso: resultado_formateado}     

        logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {id_concepto_llave}) o no había valores (ID {id_concepto_valor}).")
        return None    
def get_params_func_space(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "8") -> Optional[Dict[str, List[str]]]:

        # 1. Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None

        artefacto = parametros_obj.get("artefacto", {})       
        if not (str(artefacto.get("tipoArtefactoId")) == tipo_artefacto_id and any(keyword in str(artefacto.get("codigoArtefacto", "")) for keyword in ["VERIFICAR_ESPACIO"])):
            return None # No es el tipo de artefacto que nos interesa, salimos.

        # 2. variables para recolectar la llave y los valores                
        
        parametros_list = parametros_obj.get("parametros", [])                       
        
        resultados = []

        for param in parametros_list:
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})

            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")                    

            if nombre_concepto and valor_nota and nota_id:
                resultados.append((int(nota_id), nombre_concepto, valor_nota))
        
        resultados.sort(key=lambda x: x[0])                
                
        
        # Separar nombres y reglas
        nombres = [valor for _, tipo, valor in resultados if tipo == 'Nombre']
        reglas = [valor for _, tipo, valor in resultados if tipo == 'Regla'] 
        expresion = next((valor for _, tipo, valor in resultados if tipo == 'Expresion'), '') 
        caso_de_uso = next((valor for _, tipo, valor in resultados if tipo == 'Caso de uso'), '')
        
        reglas_final = []
        
        for atributo in nombres: 
            
            regla_final = ' '.join([ reglas[0], atributo, reglas[1], expresion,reglas[2], atributo, reglas[3] ])            
            reglas_final.append(regla_final)
                       

        # Emparejar regla con nombre (por posición)
        print(reglas_final)
        
        resultado_formateado = ', \n'.join(reglas_final)

        # Retornar en diccionario 
        
        return {caso_de_uso: resultado_formateado}     

        logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {id_concepto_llave}) o no había valores (ID {id_concepto_valor}).")
        return None    
    
    
def get_params_func_type(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "8") -> Optional[Dict[str, List[str]]]:

        # 1. Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None

        artefacto = parametros_obj.get("artefacto", {})       
        if not (str(artefacto.get("tipoArtefactoId")) == tipo_artefacto_id and any(keyword in str(artefacto.get("codigoArtefacto", "")) for keyword in ["VERIFICAR_TIPO"])):
            return None # No es el tipo de artefacto que nos interesa, salimos.

        # 2. variables para recolectar la llave y los valores                
        
        parametros_list = parametros_obj.get("parametros", [])                       
        
        resultados = []

        for param in parametros_list:
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})

            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")                    

            if nombre_concepto and valor_nota and nota_id:
                resultados.append((int(nota_id), nombre_concepto, valor_nota))
        
        resultados.sort(key=lambda x: x[0])                
                
        
        # Separar nombres y reglas
        nombres = [valor for _, tipo, valor in resultados if tipo == 'Nombre']
        reglas = [valor for _, tipo, valor in resultados if tipo == 'Regla'] 
        tipo_dato = [valor for _, tipo, valor in resultados if tipo == 'Tipo Dato']         
        restriccion  = [valor for _, tipo, valor in resultados if tipo == 'Restricción']         
        
        caso_de_uso = next((valor for _, tipo, valor in resultados if tipo == 'Caso de uso'), '')
        
        reglas_final = []
        
        print(nombres)
        print(reglas)
        print(tipo_dato)
        print(restriccion)
        print(caso_de_uso)
        
        for atributo in nombres: 
            
            print(regla[0],atributo,[tipo_dato])
            
            regla_final = ' '.join([ reglas[0], atributo, reglas[1], expresion,reglas[2], atributo, reglas[3] ])            
            reglas_final.append(regla_final)
                       

        # Emparejar regla con nombre (por posición)
        print(reglas_final)
        
        resultado_formateado = ', \n'.join(reglas_final)

        # Retornar en diccionario 
        
        return {caso_de_uso: resultado_formateado}     

        logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {id_concepto_llave}) o no había valores (ID {id_concepto_valor}).")
        return None    
    
    
processed_count = 0
for artefacto in artefactos:
    artefacto_id = artefacto['idArtefacto']    
    try:
        result = None
        result = denormalize_and_save(artefacto_id)        
        if result:
            print(f"\n== Resultado para artefacto {artefacto_id} ==")            
            #params_drt = get_params_drt(result)
            #params_ubi = get_params_ubi(result)
            #params_arch = get_params_arch(result)
            #params_stru = get_params_stru(result)
            #params_atr = get_params_atr(result)
            #params_func_arch = get_params_func_arch(result)
            #params_func_nul = get_params_func_nul(result)
            #params_func_spaces = get_params_func_space(result)
            params_func_type = get_params_func_type(result)
            #print(params_func_spaces)             
            print(params_func_type)
            #print(params_func_nul)
            #print(params_func_arch)
            #print(params_atr)
            #print(params_stru)
            #print(params_arch)
            #print(params_drt)
            #print(params_ubi)
        processed_count += 1
    except Exception as e:
        logger.error(f"Falló el procesamiento para el artefacto {artefacto_id}: {e}")
        
#%additional_python_modules openpyxl==3.0.10, pyxlsb==1.0.8

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from commons3.ExtractHelper import ExtractHelper
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

CASE_USE_EVALUATE =  ["NIIF17_TABLASMORTALIDADNIIF4"]

tables_params = { "artefacto": "datalake-dev-artefacto",     "acn": "datalake-dev-artefacto-concepto-nota-params",     "concepto": "datalake-dev-concepto",     "nota": "datalake-dev-nota" }


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
unified_structures
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
    print(f"LOG REGISTERED: {regla_func_name} - {columna}[{indice}]: {fallo}")

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
    Saves all accumulated logs to a single consolidated file in S3 in Parquet format only.
    """
    global log_df
    
    if log_df.empty:
        print("No logs to save.")
        return
    
    try:
        timestamp = datetime.now(ZoneInfo("Etc/GMT+5")).strftime("%Y-%m-%d %H:%M:%S")
        table_suffix = f"_{table_name}" if table_name else ""
        
        # Save as Parquet format only
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
    log_df = pd.DataFrame(columns=columns_logs)
    print("Log DataFrame cleared.")

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
        print(f'Saving the file in Parquet format to {path_save_files}')
        df_spark.write.mode("overwrite").parquet(path_save_files)
        print('File successfully saved in Parquet format')
        
        print(f"Parquet file successfully saved to: {path_save_files}")
            
    except Exception as e:
        print(f"Error saving Format Parquet file: {str(e)}")
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
    """
    
    try:
        excel_file_path = f"s3://{bucket}/{key_xlsx}"
        
        df = pd.read_excel(excel_file_path, dtype=str)
    # Crear datos dummy simulando contenido del Excel
        data = [
            Row(NO_REGISTRO=2, COLUMNA1="Valor_1A", COLUMNA2="Valor_1B", COLUMNA3="Valor_1C"),
            Row(NO_REGISTRO=3, COLUMNA1="Valor_2A", COLUMNA2="Valor_2B", COLUMNA3="Valor_2C"),
            Row(NO_REGISTRO=4, COLUMNA1="Valor_3A", COLUMNA2="Valor_3B", COLUMNA3="Valor_3C")
        ]
        
        # Crear DataFrame dummy
        df_spark = spark.createDataFrame(data)

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
        print(f"Checking file existence: s3://{bucket}/{key}")
        s3_client.head_object(Bucket=bucket, Key=key)
        print(f"SUCCESS: The file '{key}' exists in the bucket '{bucket}'.")
        
        return True
        
    except s3_client.exceptions.NoSuchKey:
        print(f"ERROR: The file '{key}' does not exist in the bucket '{bucket}'.")
        print(f"Full path checked: s3://{bucket}/{key}")
        
        return False
        
    except Exception as e:
        print(f"Unexpected error accessing the file in S3: {str(e)}")
        print(f"Full path checked: s3://{bucket}/{key}")
        print(f"Error type: {type(e).__name__}")
        
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
            print("Invalid file structure:")
            for err in errores:
                print(f"   - {err}")
            
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
            print("File structure validated successfully.")
            
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
        
        print(f"BUCKET {bucket}")
        print(f"BUCKET {dest_key}")

        try:
            s3_client.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket_landing, 'Key': source_key},
                Key=dest_key
            )
            #s3_client.delete_object(Bucket=bucket, Key=source_key)
            print(f"File moved to path {status}: s3://{bucket}/{dest_key}")
            
        except Exception as e:
            
            print(f"Error moving file to {status}: {e}")

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
        print(f"Error create schema file {e}")

    return df_schema_for_rules

def modify_df_schema(df: DataFrame, changes_schema: dict):
    """
    modify schema a dataframe allow schema 
    """
    df_schema = df.toDF()

    df_schema = df_schema.withColumn("ID_LOTE_CARGUE", F.lit(None))
    
    for name_col, new_type in changes_schema.items():
        df_schema = df_schema.withColumn(name_col, col(name_col).cast(new_type))
        df_schema = df_schema.withColumn(name_col, when(col(name_col) == '', None).otherwise(col(name_col)))
    
    return df_schema
all_rules = unified_rules[case_use]
tipos_deseados = ["ColumnExists","ColumnLength","ColumnValues","IsComplete","CustomSql","ColumnDataType"]
for tipo in tipos_deseados: 
    filtered_rules = filtrar_reglas_por_tipo(all_rules, [tipo])
    rules_str = ",\n".join(filtered_rules)
from pyspark.sql import Row
# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
##job = Job(glueContext)
##job.init(args['JOB_NAME'], args)
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
        
        #logger.info(f"Starting for case : {case_use}")
        #logger.info(f"columns: {expected_columns_case}")
        
        if verificar_archivo(s3_client, BUCKET_SOURCE, key_file, case_use):
    
            print(f"Loading file and Checking structure from {excel_file_path}")
    

            
         
pip install openpyxl
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
job.commit()