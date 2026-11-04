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

"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim, date_format, to_date, lit, coalesce, min, max, broadcast, year, when, concat_ws, substring, countDistinct, months_between, round, floor, greatest, concat, explode, sequence, log, exp, row_number, regexp_replace
from pyspark.sql.functions import sum as sum_
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, DoubleType, DecimalType
from pyspark.sql.window import Window
from datetime import datetime, timedelta, date

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

import sys, traceback, boto3, json

#--------------------------------------------------------------------------SET ARGS AND CONFIG COMMONS UTILS ----------------------------------------------------------------------------------------------------#
BUCKET_NAME = "alfa-datalake-dev-gold"
BUCKET_NAME_ASSETS = "alfa-datalake-dev-assets"
DB_NAME = "ALFAPRD"
DB_SCHEMA = "PRV"
#PERIOD = "2025-04-16"
CERTIFICATE = "False"
DB_CATALOG_GLUE_SILVER = "datalake-dev-silver-catalog-db"
DB_CATALOG_GLUE_BRONCE = "datalake-dev-bronce-catalog-db"
TABLE_NAME_GOLD = "RESERVA_RVI_GOLD"
FEC_CORTE_DATE = "2024-12-01"
##PARAMS_NAME = "tasa_fac_seg"
##PARAMS_PATH = "/datalake/dev/niif17/"

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
#----------------------------------------------------------------StructType final--------------------------------------------------------------------------------------------------------------#
try:

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
			StructField("NO_MESADAS", IntegerType(), True),                                # campo 21 - DECIMAL(2,0)
			StructField("RETROACTIVO", DecimalType(10,2), True),                           # campo 22 - NUMBER(10,2)   +++
			StructField("GRADO_INVALIDEZ_INICIAL", DecimalType(5,2), True),                # campo 23 - DECIMAL(5,2)
			StructField("FECHA_ULTIMA_MOD", DateType(), True),                             # campo 24 - DATE YYYYY-MM-DD
			StructField("GRADO_INVALIDEZ_ACTUAL", DecimalType(5,2), True),                 # campo 25 - DECIMAL(5,2)
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
			StructField("TASA_CREC_PRIMA", DecimalType(5,2), True),                        # campo 164 - DECIMAL(5,2)
			StructField("GASTOS_PRIMA", DecimalType(5,4), True),                           # campo 165 - DECIMAL(5,4)
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
			StructField("PARAMETRO_DESLIZAMIENTO", DecimalType(5,4), True),                # campo 181 - NUMBER(20,15)
			StructField("TASA_FAC_SEG", DecimalType(5,4), True),                           # campo 182 - PARAMETERS ******************************************************************************************
			StructField("VALOR_COBERTURA", DecimalType(14,2), True),                       # campo 183 - NUMBER(14,2)
			StructField("FECHA_EXPED_POLIZA", DateType(), True),                           # campo 184 - DATE YYYYY-MM-DD
			StructField("FECHA_MUERTE_AFILIADO", DateType(), True),                        # campo 185 - DATE YYYYY-MM-DD
			StructField("INTERES_TECNICO_NIIF4", DecimalType(15,10), True),                # campo 186 - NUMBER(15
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
    
except Exception as e:
    logger.error(f"An error occurred: {e}")

#-------------------------------------------------------------------------extract prv and general-----------------------------------------------------------------------------------------------------#

#Read tables from silver prv
df_prv_rentas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_PRV_SILVER_PRV_RENTAS").toDF()
df_prv_rentas_notas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_PRV_SILVER_PRV_RENTAS_NOTAS").toDF()
df_prv_beneficiarios = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_PRV_SILVER_PRV_BENEFICIARIOS").toDF()
df_prv_rentas_aprobadas_obp = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_PRV_SILVER_PRV_RENTAS_APROBADAS_OBP").toDF()
df_prv_endosos_point = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_PRV_SILVER_PRV_ENDOSOS_POINT").toDF()
df_spr_siniestros = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_PRV_SILVER_SPR_SINIESTROS").toDF()
df_crv_calculos_2 = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_PRV_SILVER_CRV_CALCULOS_2").toDF()

#Read tables from silver general
df_acu_personas_naturales = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ACU_PERSONAS_NATURALES").toDF()
df_cgr_equivalencia_tipos_id = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_CGR_EQUIVALENCIA_TIPOS_ID").toDF()
df_cgr_estados_personas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_CGR_ESTADOS_PERSONAS").toDF()
df_cgr_parentescos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_CGR_PARENTESCOS").toDF()
df_anp_ipc = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ANP_IPC").toDF()
df_rva_reserva_matematica_transic = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_RVA_RESERVA_MATEMATICA_TRANSIC").toDF()
df_pgr_salarios_minimos = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_PGR_SALARIOS_MINIMOS").toDF()
df_anp_pensionado_novedad = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PENSIONADO_NOVEDAD").toDF()
df_anp_distribucion_vig_pension = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ANP_DISTRIBUCION_VIG_PENSION").toDF()
df_anp_pensiones = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PENSIONES").toDF()
df_anp_movimientos_pago = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ANP_MOVIMIENTOS_PAGO").toDF()

#-------------------------------------------------------------------------Extract source FILE              -----------------------------------------------------------------------------#    
print(f"Extract source: FILE")
#Read tables from bronce file
df_files_campos_unica_vez = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_BRONCE}", table_name="FILES_CAMPOS_UNICA_VEZ").toDF()

#---------------------------------------------------------------------Init Businees Logic---------------------------------------------------------------------------------------------------#

#logger.info(f"Writing to {TABLE_NAME_GOLD} with period = {PERIOD}")

#---------------------------------------------------------------------alias tablas ---------------------------------------------------------------------------------------------------------#
# Alias para todos los DataFrames de PRV
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

 # 2. Hijos(df_hijos)  # 1. Prima (sum_prima)
# 6. Último cálculo (calc2) por renta y corte  # 5. Reserva matemática (solo ramo RVI, res_mat_filter) # 4. Movimientos (mov_pago_filter) de pago # 3. Hijos inválidos (df_hijos_invalidos) 
# 9. Clasificacion de los beneficiarios para las observaciones (ben_filter) 8. Auxilios funerario (aux_funerario) # 7. Calculos para mesada (ipc_acumulado)  


#-------------------------------------------------------------------------Variables-----------------------------------------------------------------------------------------------------# 
# Convertir a objeto datetime
dt = datetime.strptime(FEC_CORTE_DATE, "%Y-%m-%d")

# Periodo (yyyymm)
FEC_CORTE_PERIOD = dt.strftime("%Y%m")

# Año (yyyy)
FEC_CORTE_ANIO = int(dt.strftime("%Y")) 

#Salario
salario_minimo_actual = sal_min.filter((col("ano") == FEC_CORTE_ANIO) & (col("mes") == 1) ).select("vlr_salario_minimo").first()[0]

#----------------------------------------------------------withColumn source--------------------------------------------------------------------------------------------------------------------#
# Año de expedición y año anterior al corte
ren = ren.withColumn("anio_expedicion", year("fec_expedicion"))
ren = ren.withColumn("anio_anterior_corte", lit(FEC_CORTE_ANIO - 1))

per_nat = per_nat.withColumn(
    "edad_beneficiario",
    floor(months_between(to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd"), col("fec_nacimiento")) / 12)
)

campos_unica_vez = campos_unica_vez.withColumn(
    "nid_rentas",
    col("RENTA").cast(DecimalType(10, 0))
)
#--------------------------------------------------------------subconsultas----------------------------------------------------------------------------------------------------------------#

# 1. Prima
sum_prima = (
    end_point
    .groupBy("nid_rentas")
    .agg(sum_("vlr_prima").alias("vlr_prima"))
)

# 2. Hijos
df_hijos = (
    ben
    .filter(
        (ben["cod_parentesco"] == "4")
        & (
            (date_format(ben["fec_aparicion"], "yyyyMM") < F.lit(FEC_CORTE_PERIOD))
            | ben["fec_aparicion"].isNull()
        )
        & (
            ((ben["bln_con_derecho"] == "S") & (ben["bln_es_beneficiario"] == "S"))
            | (date_format(ben["fec_muerte"], "yyyyMM") > F.lit(FEC_CORTE_PERIOD))
        )
    )
    .groupBy("nid_rentas")
    .agg(
        when(countDistinct("nid_numero_identificacion") == 0, None)
        .otherwise(countDistinct("nid_numero_identificacion"))
        .alias("num_hijos")
    )
)

# 3. Hijos inválidos
df_hijos_invalidos = (
    ben
    .filter(
        (ben["cod_parentesco"] == "4")
        & (ben["bln_con_derecho"] == "S")
        & (ben["cod_estado_persona"] == "2")
    )
    .groupBy("nid_rentas")
    .agg(
        when(countDistinct("nid_numero_identificacion") == 0, None)
        .otherwise(countDistinct("nid_numero_identificacion"))
        .alias("num_hijos_invalidos")
    )
)

# 4. Movimientos de pago
mov_pago_filter = (
    mov_pago
    .filter(
        (mov_pago["periodo_pago"] >= F.lit(FEC_CORTE_PERIOD)) 
        & (mov_pago["cod_concepto"] == lit(1))
    )
)

# 5. Reserva matemática (solo ramo RVI) 
res_mat_filter = res_mat.filter(res_mat["ramo"] == "RVI")

# 6. Último cálculo (calc2) por renta y corte  # 5. Reserva matemática (solo ramo RVI) # 4. Movimientos de pago # 3. Hijos inválidos  # 2. Hijos  # 1. Prima
w = Window.partitionBy("nid_rentas", "fec_corte_calculo")

calc2_latest = (
    calc2
    .withColumn("nid_calculo_max", F.max("nid_calculo").over(w))
    .filter(F.col("nid_calculo") == F.col("nid_calculo_max"))
    .drop("nid_calculo_max")
)

# 7. Calculos para mesada (ipc_acumulado)
# Expandir rango de años entre expedición y corte para mesada
ipc_acumulado = ren.withColumn(
    "anio_ipc",
    explode(sequence(col("anio_expedicion").cast("int"), col("anio_anterior_corte").cast("int")))
)

# Join con tabla de IPC   para mesada
ipc_acumulado = ipc_acumulado.join(
    ipc.select(col("ANIO").alias("anio_ipc"), col("PCJ_IPC").alias("pcj_ipc_anp")),
    on="anio_ipc",
    how="left"
)

# Factor acumulado (logaritmos → suma → exp)   para mesada
ipc_acumulado = ipc_acumulado.withColumn("log_factor", log(1 + (col("pcj_ipc_anp") / 100)))

incremento_factor = (
    ipc_acumulado
    .groupBy("nid_rentas")
    .agg(exp(sum_("log_factor")).alias("factor_incremento"))
)

# 8. Auxilios funerario (aux_funerario) # 7. Calculos para mesada (ipc_acumulado)  # 6. Último cálculo (calc2) por renta y corte  # 5. Reserva matemática (solo ramo RVI) # 4. Movimientos de pago # 3. Hijos inválidos  # 2. Hijos  # 1. Prima
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

# 9. Clasificacion de los beneficiarios para las observaciones (ben_filter) 
ben_filter = ben.filter(
    col("cod_parentesco").isin("3", "10")
).select("nid_rentas", "cod_parentesco").distinct()
#----------------------------------------------------------------------join principal--------------------------------------------------------------------------------------------------------#

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
        (dist_vig_pen["nid_distribucion_vigencia"] == mov_pago_filter["nid_distribucion_vigencia"])
        & (mov_pago_filter["periodo_pago"] == date_format(ren["fec_expedicion"], "yyyyMM")),
        "inner",
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
    .join(ren_aprob, ren_aprob["nid_rentas"] == ren["nid_rentas"], "left")
    # crv_calculos_2 (max nid_calculo)
    .join(
        calc2_latest,
        (calc2_latest["nid_rentas"] == ren["nid_rentas"]) &
        (date_format(calc2_latest["fec_corte_calculo"], "yyyyMM") == date_format(ren["fec_expedicion"], "yyyyMM")),
        "left"
    )
    # endosos_point prima
    .join(sum_prima, sum_prima["nid_rentas"] == ren["nid_rentas"], "left")
    # hijos 
    .join(df_hijos, df_hijos["nid_rentas"] == ren["nid_rentas"], "left")
    # hijos inválidos 
    .join(df_hijos_invalidos, df_hijos_invalidos["nid_rentas"] == ren["nid_rentas"], "left")
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
        (lit(FEC_CORTE_ANIO) == df_salarios_corte["ano"]) &
        (df_salarios_corte["mes"] == 1),
        "left"
    )
    # Join con incremento_factor para mesada
    .join(incremento_factor, on="nid_rentas", how="left")
    # Join con valor base para calcular aux funerarios
    .join(aux_funerario, on="nid_rentas", how="left")
    #Join beneficiarios observaciones
    .join(ben_filter.withColumn("HAS_BENEFICIARIO_CLAVE", lit(True)), on="nid_rentas", how="left")
    #
    .join(campos_unica_vez, on="nid_rentas", how="left")
    # Filtro principal de fecha
    .filter((date_format(ren["fec_expedicion"], "yyyyMM") >= F.lit(FEC_CORTE_PERIOD)) & (ren["nid_rentas"] == 137469))
)

#---------------------------------------------------------------------case campos ---------------------------------------------------------------------------------------------------------#
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

#campo 11 SEXO
final_df = final_df.withColumn("SEXO",
    when(per_nat["genero"] == "M", 1).when(per_nat["genero"] == "F", 2)
)

#campo 13 NOMBRE
final_df = final_df.withColumn("NOMBRE",
    trim(substring(
        regexp_replace(
            concat_ws(
                " ",
                trim(coalesce(per_nat["primer_apellido"], lit(""))),
                trim(coalesce(per_nat["segundo_apellido"], lit(""))),
                trim(coalesce(per_nat["primer_nombre"], lit(""))),
                trim(coalesce(per_nat["segundo_nombre"], lit("")))
            ),
            " +", " "  # Reemplaza múltiples espacios por uno solo
        ), 1, 30
    ))
)

# campo 51 INTERES_TEC_PRIMA
final_df = final_df.withColumn(
    "INTERES_TEC_PRIMA",
    F.when(
        ren["fec_expedicion"] < F.to_date(F.lit("2020-01-10"), "yyyy-MM-dd"),
        ren["pcj_interes_tecnico"]
    ).otherwise(ren["pcj_interes_reserva"] * 100)
)

#campo 61 PAR_0
final_df = final_df.withColumn(
    "PAR_0",
    when(
        res_mat["nid_siniestro_renta"].isNotNull(),
        round((res_mat["vlr_reserva_rv89"] / res_mat["vlr_reserva_rv08"]) * 100, 2)
    ).otherwise(None)
)

#campo 62  PARMA
final_df = final_df.withColumn(
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
        ((res_mat["vlr_reserva_rv89"] / res_mat["vlr_reserva_rv08"]) * 100 +  col("PARMA") * (months_between( F.to_date(F.date_format(F.lit(FEC_CORTE_DATE), "yyyyMM"), "yyyyMM"), F.to_date(F.lit("201804"), "yyyyMM")) + 91)) > 100, 100)
        .otherwise((res_mat["vlr_reserva_rv89"] / res_mat["vlr_reserva_rv08"]) * 100 + col("PARMA") * (months_between(F.to_date(F.date_format(F.lit(FEC_CORTE_DATE), "yyyyMM"), "yyyyMM"), F.to_date(F.lit("201804"), "yyyyMM")) + 91)
    ) 
    ).otherwise(None)
)


#campo 67 INSCRIPCION_MECANISMO
final_df = final_df.withColumn(
    "INSCRIPCION_MECANISMO", 
    coalesce(ren_aprob["inscripcion_mecanismo"], lit(0))
)

#campo 69 PARAMETRO_DESLIZAMIENTO
final_df = final_df.withColumn(
    "parametro_deslizamiento_cal", 
    calc2_latest["vlr_exceso_smmlv_ipc"] * 100
)

final_df = final_df.withColumn(
    "parametro_deslizamiento_ins", 
    ren_aprob["PARAMETRO_DESLIZAMIENTO"]
)


    # parametro_deslizamiento → CASE WHEN ren_aprob.fecha_inscripcion is not null THEN parametro_deslizamiento_ins ELSE parametro_deslizamiento_cal END
final_df = final_df.withColumn(
        "PARAMETRO_DESLIZAMIENTO",
        F.when(F.col("ren_aprob.fecha_inscripcion").isNotNull(), F.col("parametro_deslizamiento_ins"))
         .otherwise(F.col("parametro_deslizamiento_cal"))
    )

#campo 20 MESADA
final_df = final_df.withColumn(
    "MESADA",
    when(
        col("ren.vlr_pension_inicial") <= col("exp.vlr_salario_minimo"),
        col("corte.vlr_salario_minimo")
    ).when(
        (col("ren.vlr_pension_inicial") > col("exp.vlr_salario_minimo")) &
        ((col("ren.vlr_pension_inicial") * col("factor_incremento")) < col("corte.vlr_salario_minimo")),
        col("corte.vlr_salario_minimo")
    ).otherwise(
        round(col("ren.vlr_pension_inicial") * col("factor_incremento"), 2)
    ).cast(DecimalType(16, 2))
)

# VALOR_AUXILIO_FUNERARIO
final_df = final_df.withColumn(
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
        col("VLR_AUX_FUNERARIO_BASE")
    ).cast(IntegerType())
)

# observaciones

# Finalmente, creamos la columna clase_pension usando las reglas
final_df = final_df.withColumn(
    "clase_pension",
    when(col("ren.cod_origen").isin(2, 3), lit(1))
    .when(col("HAS_BENEFICIARIO_CLAVE") == True, lit(1))
    .otherwise(lit(3))
)

final_df = final_df.withColumn(
    "tiempoTemporalidad",
    when((col("clase_pension") == 3) & 
        (col("COD_PARENTESCO") == 3) & 
        (col("COD_PARENTESCO") == 3),
        when(col("fec_nacimiento") > col("fec_nacimiento"),
            (25 - col("edad_beneficiario") + 1) * 12
        ).otherwise(
            (25 - col("edad_beneficiario") + 1) * 12
        )
    ).when((col("clase_pension") == 3) & 
        (col("COD_PARENTESCO") == 3) & 
        col("COD_PARENTESCO").isNull(),
        (25 - col("edad_beneficiario") + 1) * 12
    ).when((col("clase_pension") == 3) & (
       ((col("COD_PARENTESCO") == 7) & (col("COD_PARENTESCO") == 7)) |
       ((col("COD_PARENTESCO") == 7) & col("COD_PARENTESCO").isNull())),
    when((year(to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd")) - year(per_nat["fec_muerte"])) == 20,
         floor(240 - months_between(to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd"), per_nat["fec_muerte"]))
    ).otherwise(
         (20 - (year(to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd")) - year(per_nat["fec_muerte"]))) * 12
    )
    ).when((col("clase_pension") == 3) & (
       ((col("COD_PARENTESCO") == 3) & (col("COD_PARENTESCO") == 7)) |
       ((col("COD_PARENTESCO") == 7) & (col("COD_PARENTESCO") == 3))),
    when(col("COD_PARENTESCO") == 3,
         (25 - col("edad_beneficiario") + 1) * 12
    ).otherwise(
         (25 - col("edad_beneficiario") + 1) * 12
    ).alias("temporalidad1")
    )
)


final_df = final_df.withColumn(
    "temporalidad1",
    when((col("clase_pension") == 3) &
            (((col("COD_PARENTESCO") == 3) & (col("COD_PARENTESCO") == 7)) |
            ((col("COD_PARENTESCO") == 7) & (col("COD_PARENTESCO") == 3))),
            when(col("COD_PARENTESCO") == 3,
                (25 - col("edad_beneficiario") + 1) * 12
            ).otherwise(
                (25 - col("edad_beneficiario") + 1) * 12
            )
    )
)

final_df = final_df.withColumn(
    "temporalidad2",
    when((col("clase_pension") == 3) &
        (((col("COD_PARENTESCO")== 3) & (col("COD_PARENTESCO") == 7)) |
        ((col("COD_PARENTESCO") == 7) & (col("COD_PARENTESCO") == 3))),
        (20 - (year(to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd")) - year(per_nat["fec_muerte"]))) * 12
    )
)

final_df = final_df.withColumn(
    "tiempoTemporalidad",
    when((col("clase_pension") == 3) &
        (((col("COD_PARENTESCO") == 3) & (col("COD_PARENTESCO") == 7)) |
        ((col("COD_PARENTESCO") == 7) & (col("COD_PARENTESCO") == 3))),
        greatest(col("temporalidad1"), col("temporalidad2"))
    ).otherwise(col("tiempoTemporalidad"))  
)

final_df = final_df.withColumn(
    "OBSERVACIONES",
    when(
        per_nat["fec_muerte"].isNotNull() & col("PARAMETRO_DESLIZAMIENTO").isNotNull(),
        concat(lit("FMA "), per_nat["fec_muerte"], lit(" DESLIZAMIENT 0"), col("PARAMETRO_DESLIZAMIENTO"))
    ).when(
        per_nat["fec_muerte"].isNotNull(),
        concat(lit("FMA "), per_nat["fec_muerte"])
    ).when(
        col("PARAMETRO_DESLIZAMIENTO").isNotNull(),
        concat(lit("DESLIZAMIENT 0"), col("PARAMETRO_DESLIZAMIENTO"))
    )
)

#-----------------------------------------------------------------------columnas auditoria -------------------------------------------------------------------------------------------------------#
# Ventana por renta
w_renta = Window.partitionBy("ren.nid_rentas")

final_df = (
    final_df
    # ren.nid_rentas → llave del registro
    .withColumn("nid_rentas_key", F.col("ren.nid_rentas"))
    # mp.cod_concepto → codigo_concepto_renta
    .withColumn("codigo_concepto_renta", F.col("cod_concepto"))
    # mp.periodo_pago → periodo_pago_renta
    .withColumn("periodo_pago_renta", F.col("periodo_pago"))
    # to_char(r.fec_expedicion, 'yyyymm')
    .withColumn("periodo_expedicion_renta", F.date_format(F.col("ren.fec_expedicion"), "yyyyMM"))
    .withColumn("fecha_expedicion_renta", F.col("ren.fec_expedicion"))
    # to_char(r.fec_invalidacion_registro, 'yyyymm')
    .withColumn("periodo_invalidacion_renta", F.date_format(F.col("fec_invalidacion_registro"), "yyyyMM"))
    .withColumn("fecha_invalidacion_renta", F.col("fec_invalidacion_registro"))
    # to_char(r.fec_envio_point, 'yyyymm')
    .withColumn("periodo_point_renta", F.date_format(F.col("fec_envio_point"), "yyyyMM"))
    .withColumn("fecha_point_renta", F.col("fec_envio_point"))
    # fecha_inicio_periodo_pago_renta = primer día del mes
    .withColumn(
        "fecha_inicio_periodo_pago_renta",
        F.to_date(
            F.concat_ws(
                "-",
                F.col("periodo_pago").substr(1, 4),
                F.col("periodo_pago").substr(5, 2),
                F.lit("01")
            ),
            "yyyy-MM-dd"
        )
    )
    # fecha_fin_periodo_pago_renta = último día del mes
    .withColumn("fecha_fin_periodo_pago_renta", F.last_day(F.col("fecha_inicio_periodo_pago_renta")))
    # mp.fec_inicial
    .withColumn("fecha_inicial_pago_renta", F.col("mov_pago.fec_inicial"))
    # min(mp.fec_inicial) over (partition by r.nid_rentas)
    .withColumn("fecha_primer_pago_renta", F.min("mov_pago.fec_inicial").over(w_renta))
    # max(mp.periodo_pago) over (partition by r.nid_rentas)
    .withColumn("periodo_ultimo_pago_renta", F.max("periodo_pago").over(w_renta))
)

#--------------------------------------------------------------select enriquecido----------------------------------------------------------------------------------------------------------------#
data_enriquecida_final_df = (
    final_df
    # periodo_primer_pago_renta → TO_CHAR(fecha_primer_pago_renta, 'yyyymm')
    .withColumn("periodo_primer_pago_renta", F.date_format(F.col("fecha_primer_pago_renta"), "yyyyMM"))

    # fecha_inicio_periodo_ultimo_pago_renta → TO_DATE(periodo_ultimo_pago_renta || '01', 'yyyymmdd')
    .withColumn(
        "fecha_inicio_periodo_ultimo_pago_renta",
        F.to_date(F.concat_ws("", F.col("periodo_ultimo_pago_renta"), F.lit("01")), "yyyyMMdd")
    )

    # fecha_fin_periodo_ultimo_pago_renta → LAST_DAY(TO_DATE(periodo_ultimo_pago_renta || '01', 'yyyymmdd'))
    .withColumn(
        "fecha_fin_periodo_ultimo_pago_renta",
        F.last_day(F.to_date(F.concat_ws("", F.col("periodo_ultimo_pago_renta"), F.lit("01")), "yyyyMMdd"))
    )

    # validacion_expedicion_renta → CASE WHEN fecha_expedicion_renta BETWEEN add_months(fecha_primer_pago_renta, -1) AND add_months(fecha_primer_pago_renta, 1)
    .withColumn(
        "validacion_expedicion_renta",
        F.when(
            (F.col("fecha_expedicion_renta").between(
                F.add_months(F.col("fecha_primer_pago_renta"), -1),
                F.add_months(F.col("fecha_primer_pago_renta"), 1)
            )),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    # validacion_pago_renta → CASE WHEN fecha_inicial_pago_renta BETWEEN fecha_inicio_periodo_pago_renta AND fecha_fin_periodo_pago_renta
    .withColumn(
        "validacion_pago_renta",
        F.when(
            (F.col("fecha_inicial_pago_renta").between(
                F.col("fecha_inicio_periodo_pago_renta"),
                F.col("fecha_fin_periodo_pago_renta")
            )),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

)

#-----------------------------------------------------------------select final -------------------------------------------------------------------------------------------------------------#
# calculamos el MAX(fec_expedicion) en un DF
max_fec_df = ren.agg(F.max("fec_expedicion").alias("max_fec_expedicion"))

# lo transformamos al primer día del mes -1 mes
max_fec_df = max_fec_df.withColumn(
    "fecha_ref",
    F.trunc(F.add_months(F.col("max_fec_expedicion"), -1), "month")
).drop("max_fec_expedicion")

# lo unimos (cross join porque es un solo valor para todos)
data_enriquecida_final_df = data_enriquecida_final_df.crossJoin(F.broadcast(max_fec_df))

# aplicamos la lógica
rentas_final_df = (
    data_enriquecida_final_df
    .withColumn(
        "periodo_invalidacion_renta_homologado",
        F.when(
            F.col("fecha_invalidacion_renta").isNull(),
            F.when(
                F.col("fecha_ref") <= F.col("fecha_fin_periodo_ultimo_pago_renta"),
                F.lit(None)
            ).otherwise(
                F.date_format(F.date_add(F.col("fecha_fin_periodo_ultimo_pago_renta"), 1), "yyyyMM")
            )
        ).otherwise(
            F.date_format(F.date_add(F.col("fecha_fin_periodo_ultimo_pago_renta"), 1), "yyyyMM")
        )
    )
    .withColumn(
        "fecha_invalidacion_renta_homologado",
        F.when(
            F.col("fecha_invalidacion_renta").isNull(),
            F.when(
                F.col("fecha_ref") <= F.col("fecha_fin_periodo_ultimo_pago_renta"),
                F.lit(None)
            ).otherwise(
                F.date_add(F.col("fecha_fin_periodo_ultimo_pago_renta"), 1)
            )
        ).otherwise(
            F.date_add(F.col("fecha_fin_periodo_ultimo_pago_renta"), 1)
        )
    )
    .drop("fecha_ref")  # si ya no quieres que quede en el DF
)

#-----------------------------------------------------------------select final base -------------------------------------------------------------------------------------------------------------#
from pyspark.sql.functions import col, to_date

rentas_final_dff = rentas_final_df.select(
    # ----------------------------- campos base -----------------------------
    ren_not["cod_ramo_sfc"].cast("int").alias("RAMO"),                               # NUMBER(2,0)
    col("ren.nid_rentas").cast("int").alias("NO_POLIZA"),                            # NUMBER(10,0) → to_char
    to_date(col("ren.fec_expedicion"), "yyyy/MM/dd").alias("VIG_INICIAL"),                         # DATE
    ren["nid_rentas"].cast("int").alias("NO_SINIESTRO"),                             # NUMBER(10,0)
    to_date(col("ren.fec_causacion"), "yyyy/MM/dd").alias("FECHA_CAUSACION_DERECHO"),              # DATE
    eq_tid["cod_tipo_super"].cast("int").alias("TIPO_IDENTIF"),                      # NUMBER(2,0)
    ren["nid_numero_identificacion"].cast("string").alias("NO_IDENTIF"),             # VARCHAR2(15)
    to_date(col("per_nat.fec_nacimiento"), "yyyy/MM/dd").alias("FECHA_NACIMIENTO"),                # DATE
    est_per["cod_estado_persona"].cast("int").alias("ESTADO"),                       # NUMBER(1,0)
    ren["num_mesadas"].cast("int").alias("NO_MESADAS"),                              # DECIMAL(2,0)
    ren["pcj_pcl_inicial"].cast("decimal(5,2)").alias("GRADO_INVALIDEZ_INICIAL"),    # DECIMAL(5,2)
    to_date(col("ren.fec_ultima_mod"), "yyyy/MM/dd").alias("FECHA_ULTIMA_MOD"),                    # DATE
    ren["pcj_pcl_actual"].cast("decimal(5,2)").alias("GRADO_INVALIDEZ_ACTUAL"),      # DECIMAL(5,2)
    ren_not["num_radicado_nota"].cast("decimal(20,0)").alias("NO_RAD_NOT_TEC"),                # NUMBER
    ipc["pcj_ipc"].cast("decimal(5,2)").alias("TASA_CREC_PRIMA"),                    # DECIMAL
    res_mat["vlr_reserva_rv89"].cast("decimal(14,2)").alias("VALOR_RESERVA_RV89"),   # DECIMAL(16,2)
    res_mat["vlr_reserva_rv08"].cast("decimal(14,2)").alias("VALOR_RESERVA_RV08"),   # DECIMAL(16,2)
    to_date(col("ren_aprob.fecha_inscripcion"), "yyyy/MM/dd").alias("FECHA_INSCRIPCION"),          # DATE
    ren_aprob["cobertura"].cast("decimal(14,2)").alias("VALOR_COBERTURA"),           # DECIMAL(16,2)
    to_date(col("ren.fec_expedicion"), "yyyy/MM/dd").alias("FECHA_EXPED_POLIZA"),                  # DATE
    to_date(col("per_nat.fec_muerte"), "yyyy/MM/dd").alias("FECHA_MUERTE_AFILIADO"),               # DATE

    # ----------------------------- agregados intermedios -----------------------------
    df_hijos["num_hijos"].cast("int").alias("NO_HIJOS"),                               # NUMBER(15,0)
    sum_prima["vlr_prima"].cast("decimal(17,2)").alias("PRIMA"),                       # DECIMAL(16,2)
    df_hijos_invalidos["num_hijos_invalidos"].cast("int").alias("NO_HIJOS_INVALIDOS"), # NUMBER(15,0)
        
    rentas_final_df["PARAMETRO_DESLIZAMIENTO"].cast("decimal(5,4)").alias("PARAMETRO_DESLIZAMIENTO"),
   #calc2_latest["pcj_factor_seguridad"].cast("decimal(10,2)").alias("TASA_FAC_SEG"),        # DECIMAL(10, 2)
    calc2_latest["pcj_factor_seguridad"].alias("TASA_FAC_SEG"),
    calc2_latest["pcj_tasa_administracion"].cast("decimal(5,4)").alias("GASTOS_PRIMA"),      # DECIMAL(5,4)
    calc2_latest["pcj_tasa_administracion"].cast("decimal(5,4)").alias("GASTOS"),            # DECIMAL(5,4)

    rentas_final_df["OBSERVACIONES"].cast("string").alias("OBSERVACIONES"),          # VARCHAR
    rentas_final_df["VALOR_AUXILIO_FUNERARIO"].cast("int").alias("VALOR_AUXILIO_FUNERARIO"),
    rentas_final_df["MESADA"].cast("decimal(16,2)").alias("MESADA"),                 # DECIMAL(16,2)
    rentas_final_df["INSCRIPCION_MECANISMO"].cast("string").alias("INSCRIPCION_MECANISMO"),
    rentas_final_df["POR_AMORT_ACUM"].cast("decimal(10,2)").alias("POR_AMORT_ACUM"),
    rentas_final_df["PARMA"].cast("decimal(28,10)").alias("PARMA"),
    rentas_final_df["PAR_0"].cast("decimal(28,10)").alias("PAR_0"),
    rentas_final_df["INTERES_TEC_PRIMA"].cast("decimal(15,10)").alias("INTERES_TEC_PRIMA"),
    rentas_final_df["NOMBRE"].cast("string").alias("NOMBRE"),
    rentas_final_df["SEXO"].cast("int").alias("SEXO"),
    rentas_final_df["ORIGEN_PENSION"].cast("int").alias("ORIGEN_PENSION"),
                                            
    # ----------------------------- columnas archivo -----------------------------                                     
    campos_unica_vez["INTERES_TECNICO_NIIF4"].cast("decimal(14,2)").alias("INTERES_TECNICO_NIIF4"),
    campos_unica_vez["FAIR_VALUE"].cast("decimal(14,2)").alias("FAIR_VALUE"),

    # ----------------------------- columnas de auditoría -----------------------------
    rentas_final_df["nid_rentas_key"].cast("decimal(10,0)").alias("nid_rentas_key"),
    rentas_final_df["codigo_concepto_renta"].cast("string").alias("CODIGO_CONCEPTO_RENTA"),
    rentas_final_df["periodo_pago_renta"].cast("string").alias("PERIODO_PAGO_RENTA"),
    rentas_final_df["periodo_expedicion_renta"].cast("string").alias("PERIODO_EXPEDICION_RENTA"),
    rentas_final_df["fecha_expedicion_renta"].cast("date").alias("FECHA_EXPEDICION_RENTA"),
    rentas_final_df["periodo_invalidacion_renta"].cast("string").alias("PERIODO_INVALIDACION_RENTA"),
    rentas_final_df["fecha_invalidacion_renta"].cast("date").alias("FECHA_INVALIDACION_RENTA"),
    rentas_final_df["periodo_point_renta"].cast("string").alias("PERIODO_POINT_RENTA"),
    rentas_final_df["fecha_point_renta"].cast("date").alias("FECHA_POINT_RENTA"),
    rentas_final_df["fecha_inicio_periodo_pago_renta"].cast("date").alias("FECHA_INICIO_PERIODO_PAGO_RENTA"),
    rentas_final_df["fecha_fin_periodo_pago_renta"].cast("date").alias("FECHA_FIN_PERIODO_PAGO_RENTA"),
    rentas_final_df["fecha_inicial_pago_renta"].cast("date").alias("FECHA_INICIAL_PAGO_RENTA"),
    rentas_final_df["fecha_primer_pago_renta"].cast("date").alias("FECHA_PRIMER_PAGO_RENTA"),
    rentas_final_df["periodo_primer_pago_renta"].cast("string").alias("PERIODO_PRIMER_PAGO_RENTA"),
    rentas_final_df["periodo_ultimo_pago_renta"].cast("string").alias("PERIODO_ULTIMO_PAGO_RENTA"),
    rentas_final_df["fecha_inicio_periodo_ultimo_pago_renta"].cast("date").alias("FECHA_INICIO_PERIODO_ULTIMO_PAGO_RENTA"),
    rentas_final_df["fecha_fin_periodo_ultimo_pago_renta"].cast("date").alias("FECHA_FIN_PERIODO_ULTIMO_PAGO_RENTA"),
    rentas_final_df["validacion_expedicion_renta"].cast("string").alias("VALIDACION_EXPEDICION_RENTA"),
    rentas_final_df["validacion_pago_renta"].cast("string").alias("VALIDACION_PAGO_RENTA"),
    rentas_final_df["periodo_invalidacion_renta_homologado"].cast("string").alias("PERIODO_INVALIDACION_RENTA_HOMOLOGADO"),
    rentas_final_df["fecha_invalidacion_renta_homologado"].cast("date").alias("FECHA_INVALIDACION_RENTA_HOMOLOGADO")
).distinct()

dist = (
    join_rentas_df
    .withColumn("len", F.length("TASA_FAC_SEG"))
    .groupBy("len")
    .count()
    .orderBy("len")
    .collect()
)

# Pasarlo a lista limpia
dist_list = [(row["len"], row["count"]) for row in dist]

dist_list
#-----------------------------------------------------------------funcion pivot beneficiarios -------------------------------------------------------------------------------------------------------------#
from pyspark.sql import functions as F

def pivot_beneficiarios(df, max_n=15):
    """
    Pivotear beneficiarios de filas a columnas.
    
    :param df: DataFrame con al menos las columnas:
               - nid_rentas
               - rw_beneficiario
               - estado, estudios, fecha_nacimiento, tipo_identif,
                 no_identif, nombre, parent, sexo, con_derecho
    :param max_n: Número máximo de beneficiarios a pivotear.
    :return: DataFrame con columnas expandidas hasta max_n.
    """
    
    def generar_campos(n):
        return [
            F.max(F.when(F.col("rw_beneficiario") == n, F.col("estado"))).alias(f"ESTADO_{n}"),
            F.max(F.when(F.col("rw_beneficiario") == n, F.col("estudios"))).alias(f"ESTUDIOS_{n}"),
            F.max(F.when(F.col("rw_beneficiario") == n, F.col("fecha_nacimiento"))).alias(f"FECHA_NACIMIENTO_{n}"),
            F.max(F.when(F.col("rw_beneficiario") == n, F.col("tipo_identif"))).alias(f"TIPO_IDENTIF_{n}"),
            F.max(F.when(F.col("rw_beneficiario") == n, F.col("no_identif"))).alias(f"NO_IDENTIF_{n}"),
            F.max(F.when(F.col("rw_beneficiario") == n, F.col("nombre"))).alias(f"NOMBRE_{n}"),
            F.max(F.when(F.col("rw_beneficiario") == n, F.col("parent"))).alias(f"PARENT_{n}"),
            F.max(F.when(F.col("rw_beneficiario") == n, F.col("sexo"))).alias(f"SEXO_{n}"),
            F.max(F.when(F.col("rw_beneficiario") == n, F.col("con_derecho"))).alias(f"CON_DERECHO_{n}")
        ]
    
    # Generar todas las expresiones hasta max_n
    expresiones = []
    for i in range(1, max_n + 1):
        expresiones.extend(generar_campos(i))
    
    # Ejecutar el groupBy con todas las expresiones
    return df.groupBy("nid_rentas_ben_key").agg(*expresiones)

#-----------------------------------------------------------------join beneficiarios -------------------------------------------------------------------------------------------------------------#
join_ben_df = (
    # prv_rentas
    ren
    # anp_pensiones
    .join(pens, ren["nid_rentas"] == pens["nid_procedencia"], "inner")
    # anp_distribucion_vig_pension
    .join(dist_vig_pen, pens["nid_pension"] == dist_vig_pen["nid_pension"], "inner")
    # anp_movimientos_pago con condiciones (periodo vs fec_expedicion)
    .join(
        mov_pago_filter,
        (dist_vig_pen["nid_distribucion_vigencia"] == mov_pago_filter["nid_distribucion_vigencia"])
        & (mov_pago_filter["periodo_pago"] == date_format(ren["fec_expedicion"], "yyyyMM")),
        "inner",
    )
    # prv_beneficiario 
    .join(
        ben,
        (ben["nid_rentas"] == ren["nid_rentas"]) &
        (ben["cod_tipo_identificacion"] == mov_pago_filter["cod_tipo_identificacion"]) &
        (ben["nid_numero_identificacion"] == mov_pago_filter["nid_numero_identificacion"]),
        "inner"
    )
    # acu_personas_naturales
    .join(
        per_nat,
        (ben["cod_tipo_identificacion"] == per_nat["cod_tipo_identificacion"]) &
        (ben["nid_numero_identificacion"] == per_nat["nid_numero_identificacion"]),
        "inner"
    )
    # equivalencia_tipos_id
    .join(eq_tid, eq_tid["cod_tipo_documento"] == ben["cod_tipo_identificacion"], "left")
    # cgr_parentescos
    .join(par, par["cod_parentesco"] == ben["cod_parentesco"], "left")
    .filter(F.date_format(ren["fec_expedicion"], "yyyyMM") >= "202412")
    .filter(ren["nid_rentas"] == 137469)
)

#-----------------------------------------------------------------case y pivot beneficiarios -------------------------------------------------------------------------------------------------------------#
# Definimos ventana para el dense_rank
w_benef = Window.partitionBy("ren.nid_rentas") \
                .orderBy(
                    F.col("par.cod_parentesco").desc(),
                    F.col("ben.nid_numero_identificacion"),
                    F.col("per_nat.fec_nacimiento")
)


parent_col = (
    F.when(F.col("par.cod_parentesco") == "1", 5)   # padre
     .when(F.col("par.cod_parentesco") == "2", 6)   # madre
     .when(
         (F.col("par.cod_parentesco") == "10") &
         ((F.months_between(F.col("ren.fec_expedicion"), F.col("per_nat.fec_nacimiento")) / 12) < 30) &
         (F.col("ben.cant_hijos") == 0),
         7                                          # compañera <30 años sin hijos, pensión temporal
     )
     .when(F.col("par.cod_parentesco") == "3", 4)   # cónyuge vitalicio
     .when(F.col("par.cod_parentesco") == "10", 4)  # compañera vitalicia
     .when(F.col("par.cod_parentesco") == "4", 3)   # hijo/a
     .when(F.col("par.cod_parentesco") == "19", 2)  # hijo inválido
     .when(F.col("par.cod_parentesco") == "21", 8)  # hermano/a
     .when(F.col("par.cod_parentesco") == "11", 11)  # el mismo/a
     .otherwise(None)
)


# Construcción del DF final
rank_ben_df = (
    join_ben_df  # DF  de los joins
    .withColumn("nid_rentas_ben_key", F.col("ren.nid_rentas"))
    .withColumn("rw_beneficiario", F.dense_rank().over(w_benef))
    .withColumn("parent", parent_col)
    .withColumn("tipo_identif", F.col("eq_tid.cod_tipo_super"))
    .withColumn("no_identif", F.col("ben.nid_numero_identificacion"))
    .withColumn("sexo",
        F.when(F.col("per_nat.genero") == "m", 1)
         .when(F.col("per_nat.genero") == "f", 2)
    )
    .withColumn("fecha_nacimiento", F.col("per_nat.fec_nacimiento"))
    .withColumn("nombre",
        F.substring(
            F.regexp_replace(
                F.concat_ws(
                    " ",
                    F.trim(F.coalesce(F.col("per_nat.primer_apellido"), F.lit(""))),
                    F.trim(F.coalesce(F.col("per_nat.segundo_apellido"), F.lit(""))),
                    F.trim(F.coalesce(F.col("per_nat.primer_nombre"), F.lit(""))),
                    F.trim(F.coalesce(F.col("per_nat.segundo_nombre"), F.lit("")))
                ),
                " +", " "  # reemplaza múltiples espacios por uno solo
            ),
            1,
            30
        )
    )
    .withColumn("estudios",
        F.when(
            (F.col("par.cod_parentesco") == "4") &  # hijo/a
            (F.col("ben.cod_estado_persona") == "1") &  # válido
            ((F.months_between(F.col("ren.fec_expedicion"), F.col("per_nat.fec_nacimiento")) / 12) >= 18),
            1
        )
    )
    .withColumn("estado", F.col("ben.cod_estado_persona"))
    .withColumn("con_derecho", F.col("ben.bln_con_derecho"))
    .select(
    col("nid_rentas_ben_key").cast("decimal(10,0)"),   # clave técnica
    col("rw_beneficiario").cast("int"),                # numerador de ventana
    col("parent").cast("int"),                         # campo n - NUMBER(1,0)
    col("tipo_identif").cast("int"),                   # campo n - NUMBER(1,0)
    col("no_identif").cast("string"),                  # campo n - NUMBER(15,0)
    col("sexo").cast("int"),                           # campo n - NUMBER(1,0)
    col("fecha_nacimiento").cast("date"),              # campo n - DATE
    col("nombre").cast("string"),                      # campo n - VARCHAR2(80 BYTE)
    col("estudios").cast("int"),                       # campo n - NUMBER(1,0)
    col("estado").cast("int"),                         # campo n - NUMBER(1,0)
    col("con_derecho").cast("string")                  # campo n - VARCHAR(1)
)

)

beneficiarios_final_df = pivot_beneficiarios(rank_ben_df, max_n=15)
#-----------------------------------------------------------------join rentas y beneficiarios -------------------------------------------------------------------------------------------------------------#
join_rvi_final_df = (
    rentas_final_dff
    .join(
        beneficiarios_final_df,
        on=[
            F.col("nid_rentas_key") == F.col("nid_rentas_ben_key")
        ],
        how="left"
    )
)
 #-----------------------------------------------------------------campos constantes----------------------------------------------------------------------------------------#
campos_rvi_final_df = join_rvi_final_df.withColumn("UNIDAD", lit(1).cast(IntegerType()))\
        .withColumn("PARENT", lit(1).cast(IntegerType()))\
        .withColumn("ESTUDIOS", lit(None).cast(IntegerType()))\
        .withColumn("INGRESO_BASE_LIQUIDACION", lit(None).cast(DecimalType(10,2)))\
        .withColumn("RETROACTIVO", lit(None).cast(DecimalType(10,2)))\
        .withColumn("FECHA_INICIAL_RENTA_DIFERIDA", lit(None).cast(DateType()))\
        .withColumn("VALOR_MESADA_BENEF_PERIO_DIFER", lit(None).cast(DecimalType(10,2)))\
        .withColumn("PORCENTAJE_EXC_PARMA", lit(None).cast(DecimalType(10,2)))\
        .withColumn("FEC_AVISO_SINIESTRO", lit(None).cast(DateType()))\
        .withColumn("EQUIVALENCIA", lit(None).cast(StringType()))\
        .withColumn("ID_LOTE_CARGUE", lit(1).cast(IntegerType())) 

campos_rvi_final_df = campos_rvi_final_df.withColumns({
    #----------------------------------------------------------------- rentas sin beneficiario -------------------------------------------------
    "ESTADO_1": F.when(F.col("ESTADO_1").isNull(), F.col("ESTADO")).otherwise(F.col("ESTADO_1")),
    "ESTUDIOS_1": F.when(F.col("ESTUDIOS_1").isNull(), F.col("ESTUDIOS")).otherwise(F.col("ESTUDIOS_1")),
    "FECHA_NACIMIENTO_1": F.when(F.col("FECHA_NACIMIENTO_1").isNull(), F.col("FECHA_NACIMIENTO")).otherwise(F.col("FECHA_NACIMIENTO_1")),
    "TIPO_IDENTIF_1": F.when(F.col("TIPO_IDENTIF_1").isNull(), F.col("TIPO_IDENTIF")).otherwise(F.col("TIPO_IDENTIF_1")),
    "NOMBRE_1": F.when(F.col("NOMBRE_1").isNull(), F.col("NOMBRE")).otherwise(F.col("NOMBRE_1")),
    "PARENT_1": F.when(F.col("PARENT_1").isNull(), F.col("PARENT")).otherwise(F.col("PARENT_1")),
    "SEXO_1": F.when(F.col("SEXO_1").isNull(), F.col("SEXO")).otherwise(F.col("SEXO_1")),
    "CON_DERECHO_1": F.when(F.col("CON_DERECHO_1").isNull(), F.col("CON_DERECHO_1")).otherwise(F.col("CON_DERECHO_1")),

    #----------------------------------------------------------------- campos nulos -------------------------------------------------
    "NO_HIJOS": F.when(F.col("NO_HIJOS").isNull(), F.lit(0)).otherwise(F.col("NO_HIJOS")),
    "NO_HIJOS_INVALIDOS": F.when(F.col("NO_HIJOS_INVALIDOS").isNull(), F.lit(0)).otherwise(F.col("NO_HIJOS_INVALIDOS"))
})
#-----------------------------------------------------------------select final ---------------------------------------------------------------------------------------------------------#
column_names = [field.name for field in schema_reserva_rvi_gold]
campos_rvi_final_df = campos_rvi_final_df.select(*column_names)



#-----------------------------------------------------------------Save files Data ------------------------------------------------------------------------------------------------------#
reserva_rvi_gold_df = spark.createDataFrame(campos_rvi_final_df.rdd, schema=schema_reserva_rvi_gold)

partition_columns = ["ID_LOTE_CARGUE"]
  
# Save the transformed DataFrame to S3
frame_save = DynamicFrame.fromDF(reserva_rvi_gold_df, glueContext, "frame_save_dynamic")

s3_prefix = f"GOLD/NIIF/{TABLE_NAME_GOLD}/"

s3_path = f"s3://{BUCKET_NAME}/{s3_prefix}"

#logger.info(f"Truncate & Saving data to: {s3_path}")

extract_helper.delete_s3_folder(BUCKET_NAME, s3_prefix)
frame_save.count()
"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim, date_format, to_date, lit, min, max, when, first, count, substring, concat_ws, floor, months_between, year, round, least, trunc, round as spark_round, coalesce, row_number, regexp_replace, concat, last_day
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, DoubleType, DecimalType
from pyspark.sql.window import Window

from datetime import datetime, timedelta, date

import sys, traceback, boto3, json

#--------------------------------------------------------------------------SET ARGS AND CONFIG COMMONS UTILS ----------------------------------------------------------------------------------------------------#
BUCKET_NAME = "alfa-datalake-dev-gold"
BUCKET_NAME_ASSETS = "alfa-datalake-dev-assets"
DB_NAME = "ALFAPRD"
DB_SCHEMA = "ARPPROD"
#PERIOD = "2025-04-16"
CERTIFICATE = "False"
DB_CATALOG_GLUE_SILVER = "datalake-dev-silver-catalog-db"
DB_CATALOG_GLUE_BRONCE = "datalake-dev-bronce-catalog-db"
TABLE_NAME_GOLD = "RESERVA_ARP_GOLD"
FEC_CORTE_DATE = "2024-12-01"
##PARAMS_NAME = "tasa_fac_seg"
##PARAMS_PATH = "/datalake/dev/niif17/"


#logger.info(f"Period: {PERIOD}")
#logger.info(f"DB_CATALOG_GLUE_SILVER: {DB_CATALOG_GLUE}")

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
#job = Job(glueContext)
#job.init(args['JOB_NAME'], args)
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")


extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME, DB_NAME)
#----------------------------------------------------------------StructType final--------------------------------------------------------------------------------------------------------------#

try:

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
        
            StructField("ID_LOTE_CARGUE", IntegerType(), True)                          # campo 190 - partition
            ]) 

except Exception as e:
    logger.error(f"An error occurred: {e}")
#-------------------------------------------------------------------------extract arpprod and general-----------------------------------------------------------------------------------------------------#    

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
#logger.info(f"Extract source: FILE")
#Read tables from silver arpprod
df_files_campos_unica_vez = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_BRONCE}", table_name="FILES_CAMPOS_UNICA_VEZ").toDF()


#---------------------------------------------------------------------Init Businees Logic---------------------------------------------------------------------------------------------------#

#logger.info(f"Writing to {TABLE_NAME_GOLD} with period = {PERIOD}")

#---------------------------------------------------------------------alias tablas ---------------------------------------------------------------------------------------------------------#
# Alias para todos los DataFrames de ARPPROD
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
# Convertir a objeto datetime
dt = datetime.strptime(FEC_CORTE_DATE, "%Y-%m-%d")

# Periodo (yyyymm)
FEC_CORTE_PERIOD = dt.strftime("%Y%m")
STR_CORTE_PERIOD = date_format(dt.strftime("%Y%m"), "yyyyMM")

# Año (yyyy)
FEC_CORTE_ANIO = int(dt.strftime("%Y")) 

#----------------------------------------------------------withColumn source---------------------------------------------------------------------------------------------------------------#

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
#--------------------------------------------------------------subconsultas----------------------------------------------------------------------------------------------------------------#

# 1
df_filter_sic = sic.filter(col("ramo") == "ARP")

# 2
df_filter_cob = cob.filter(
    (col("cod_cobertura") == 17) &
    (date_format(col("fec_pago"), "yyyyMM") == lit(FEC_CORTE_PERIOD))
).select("nid_siniestro", "vlr_pago")

# 3
df_filter_inv = (
    inv.withColumn("anio_aprobacion", year(col("fec_aprobacion_pension")))
       .filter(
           (col("cod_estado_reserva") == "1") &
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
    .filter(col("cod_concepto_medico_am").isin("4", "5"))
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

window_asc = Window.partitionBy("nid_siniestro").orderBy(col("fec_registra"))
window_desc = Window.partitionBy("nid_siniestro").orderBy(col("fec_registra").desc())

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
        first("fec_registra").over(window_desc).alias("ultima_modificacion")
    )
    .dropDuplicates(["nid_siniestro", "primero", "ultimo", "ultima_modificacion"])
)

# 9
w = Window.partitionBy("nid_siniestro", "fec_calculo")
    
calc2_latest = (
    cal
    .withColumn("nid_calculo_max", F.max("nid_calculo").over(w))
    .filter(F.col("nid_calculo") == F.col("nid_calculo_max"))
    .drop("nid_calculo_max")
)

#---------------------------------------------------------------------alias tablas ---------------------------------------------------------------------------------------------------------#

fsal = df_filter_sal.alias("fsal")
fmat = df_filter_sic.alias("fmat")
fcov = df_filter_cob.alias("fcov")
finv = df_filter_inv.alias("finv")
fuad = df_filter_aud_1.alias("fuad")
his = df_groupby_his.alias("his")
hjs = df_groupby_hijos.alias("hjs")
hjv = df_groupby_hijos_invalidos.alias("hjv")
pcl = df_first_pcl.alias("pcl")



#----------------------------------------------------------------------join principal--------------------------------------------------------------------------------------------------------#
#logger.info(f"Main Join Siniestros")
df_join_principal_reserva_arp =(
    sar
    #par_trabajadores
    .join(tra, sar["nid_trabajador"] == tra["nid_trabajador"], "inner")
    # sar_datos_invalidez_muerte filtrado
    .join(finv, sar["nid_siniestro"] == finv["nid_siniestro"], "left")
    #sar_auditoria_medica filtrado
    .join(fuad, finv["nid_auditoria_medica"] == fuad["nid_auditoria_medica"], "inner")
    #ind_datos_reservas_siniestros
    .join(res, sar["nid_siniestro"] == res["nid_siniestro"], "inner")
    # crv_calculos_2 (max nid_calculo)
   .join(
        calc2_latest,
        (calc2_latest["nid_siniestro"] == sar["nid_siniestro"]) &
        (date_format(calc2_latest["fec_calculo"], "yyyyMM") ==  F.lit(FEC_CORTE_PERIOD)),
        "left"
        )
    #pgr_salarios_minimos
        .join(fsal, finv["anio_aprobacion"] == fsal["ANIO_SAL"], "left")
    #rva_reserva_matematica_transic
        .join(fmat, sar["nid_siniestro"] == fmat["nid_siniestro_renta"], "left")
    #sar_pagos_coberturas
        .join(fcov, sar["nid_siniestro"] == fcov["nid_siniestro"], "left")
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
    # Filtro principal de fecha
        #.filter((date_format(ren["fec_aprobacion"], "yyyyMM") >= F.lit(FEC_CORTE_PERIOD))) # & (ren["nid_rentas"] == 137469)
    ) 


#---------------------------------------------------------------------case campos ---------------------------------------------------------------------------------------------------------#

# campo 13
siniestros_final_df = df_join_principal_reserva_arp.withColumn("NOMBRE",
    trim(substring(regexp_replace(
            concat_ws(
                " ",
                trim(col("tra_apn.PRIMER_APELLIDO")),
                trim(col("tra_apn.SEGUNDO_APELLIDO")),
                trim(col("tra_apn.PRIMER_NOMBRE")),
                trim(col("tra_apn.SEGUNDO_NOMBRE"))
            ),
            " +", " "  # Reemplaza múltiples espacios por uno solo
        ),  1,30))
)

#campo 11
siniestros_final_df = siniestros_final_df.withColumn("SEXO",
    when(col("tra_apn.genero") == "M", 1).when(col("tra_apn.genero") == "F", 2)
)

#campo 15
siniestros_final_df = siniestros_final_df.withColumn("ESTADO",
    when((col("tra_apn.fec_muerte").isNotNull()) |(col("fuad.cod_origen") == "1"), "3")
    .when(fuad["cod_origen"] == "2", 2)
    .otherwise(1)
)

# campos 22, 23, 24 .. case 
siniestros_final_df = siniestros_final_df \
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
siniestros_final_df = siniestros_final_df.withColumn(
    "PAR_0",
    when(
        (col("existe")) & (col("fmat.vlr_reserva_rv08") != 0),
        round((col("fmat.vlr_reserva_rv89") / col("fmat.vlr_reserva_rv08")) * 100, 2)
    ).otherwise(None)
)


# campos 62
siniestros_final_df = siniestros_final_df.withColumn(
    "PARMA",
    when(
        (col("existe")) & (col("fmat.vlr_reserva_rv08") != 0),
        round((100 - round((col("fmat.vlr_reserva_rv89") / col("fmat.vlr_reserva_rv08")) * 100, 2))/240, 10)
        ).otherwise(None)
)

# campo 64 
fec_corte_dt = to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd")
fecha_base_dt = to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd")

siniestros_final_df = siniestros_final_df.withColumn(
    "POR_AMORT_ACUM",
    when(
        (col("existe")) & (col("fmat.vlr_reserva_rv08") != 0),
        least(
            spark_round(
                spark_round((col("fmat.vlr_reserva_rv89") / col("fmat.vlr_reserva_rv08")) * 100, 2) +
                (
                    spark_round(
                        (100 - spark_round((col("fmat.vlr_reserva_rv89") / col("fmat.vlr_reserva_rv08")) * 100, 2)) / 240,
                        10
                    ) * (floor(months_between(fec_corte_dt, fecha_base_dt)) + lit(91))
                ),
                0
            ),
            lit(100)
        )
    ).otherwise(None)
)

# campo 20 no_mesada 
# Condiciones para asignar NO_MESADA

siniestros_final_df = siniestros_final_df.withColumn(
    "NO_MESADAS",
    when(col("his.FEC_DICTAMEN_INVMTE") <= fec_corte_dt, 14)
    .when(
        (col("his.FEC_DICTAMEN_INVMTE") > fec_corte_dt) & (col("his.FEC_DICTAMEN_INVMTE") <= fecha_base_dt),
        when(col("his.VLR_PENSION") <= 3 * col("fsal.VLR_SMIN"), 14).otherwise(13)
        )
        .otherwise(13)  # Valor por defecto si no cae en los rangos
)

# campo 57 aux funerario
siniestros_final_df = siniestros_final_df.withColumn(
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
    ).cast(IntegerType())  # Aquí aplicas el cast directamente
)
#-----------------------------------------------------------------select final base casteos-----------------------------------------------------------------------------------------------#

siniestros_final_dff = siniestros_final_df.select(
    
# ----------------------------- campos base -----------------------------
        sar["nid_siniestro"].alias("nid_siniestro_key"),                                                # campo 3
        tra["nid_afiliacion"].cast(IntegerType()).alias("NO_POLIZA"),                                   # campo 3
        F.to_date(finv["fec_aprobacion_pension"], "yyyy/MM/dd").alias("VIG_INICIAL"),                   # campo 4
        sar["nid_siniestro"].cast(IntegerType()).alias("NO_SINIESTRO"),                                 # campo 5
        F.to_date(sar["fec_siniestro"], "yyyy/MM/dd").alias("FECHA_CAUSACION_DERECHO"),                 # campo 6
        org["cod_origen_super"].cast(IntegerType()).alias("ORIGEN_PENSION"),                            # campo 7
        tra_eqid["cod_tipo_super"].cast(IntegerType()).alias("TIPO_IDENTIF"),                           # campo 9
        tra["NID_NUMERO_IDENTIFICACION"].cast("string").alias("NO_IDENTIF"),                            # campo 10
        F.to_date(tra_apn["fec_nacimiento"], "yyyy/MM/dd").alias("FECHA_NACIMIENTO"),                   # campo 12  *tra_apn
        res["vrl_ibl_manual"].cast(DecimalType(10,2)).alias("INGRESO_BASE_LIQUIDACION"),                # campo 18
        finv["vlr_pension"].cast("decimal(16,2)").alias("MESADA"),                                      # campo 19
        finv["COD_ESTADO_RESERVA"].cast(StringType()).alias("COD_ESTADO_RESERVA"),                      # campo 192
        fmat["vlr_reserva_rv08"].cast("decimal(14,2)").alias("VALOR_RESERVA_RV08"),                     # campo 59
        fmat["vlr_reserva_rv89"].cast("decimal(14,2)").alias("VALOR_RESERVA_RV89"),                     # campo 60
        fmat["ramo"].cast(StringType()).alias("RAMO_RESERVA"),                                          # campo 190  
        fuad["COD_CONCEPTO_MEDICO_AM"].cast(StringType()).alias("COD_CONCEPTO_MEDICO_AM"),              # campo 191
        F.to_date(finv["fec_aprobacion_pension"], "yyyy/MM/dd").alias("FECHA_EXPED_POLIZA"),            # campo72
        F.to_date(tra_apn["fec_muerte"], "yyyy/MM/dd").alias("FECHA_MUERTE_AFILIADO"),                  # campo 73 * tra_apn
        ipc["pcj_ipc"].cast("decimal(5,2)").alias("TASA_CREC_PRIMA"),                                   # campo 52
        
# ----------------------------- agregados intermedios -----------------------------
        hjs["NO_HIJOS"].cast(IntegerType()).alias("NO_HIJOS"),                                          # campo 16
        hjv["NO_HIJOS_INVALIDOS"].cast(IntegerType()).alias("NO_HIJOS_INVALIDOS"),                      # campo 17
    
        calc2_latest["pcj_deslizamiento_sm"].cast("decimal(5,4)").alias("PARAMETRO_DESLIZAMIENTO"),     # DECIMAL(10, 2)
        calc2_latest["pcj_gastos_exp"].cast("decimal(5,4)").alias("GASTOS"),                      # DECIMAL(10, 2)
        calc2_latest["NID_CALCULO"].cast("decimal(10,0)").alias("NO_CALCULO"),                          # campo 193
        calc2_latest["FEC_CALCULO"].cast(DateType()).alias("FECHA_CALCULO"),                            # campo 194
        fsal["ANO"].cast(IntegerType()).alias("ANO_SALARIO_MINIMO"),                                    # campo 195
        fsal["MES"].cast(IntegerType()).alias("MES_SALARIO_MINIMO"),                                    # campo 196
        fsal["VLR_SALARIO_MINIMO"].cast("decimal(14,2)").alias("VLR_SALARIO_MINIMO"),                   # campo 197
        fcov["COD_COBERTURA"].cast("decimal(10,0)").alias("COD_COBERTURA"),                             # campo 198
        fcov["FEC_PAGO"].cast(DateType()).alias("FEC_PAGO"),                                            # campo 199
        
        siniestros_final_df["NOMBRE"].cast("string").alias("NOMBRE"),                                   # campo 13
        siniestros_final_df["SEXO"].cast(IntegerType()).alias("SEXO"),                                  # campo 11 * tra_apn
        siniestros_final_df["ESTADO"].cast(IntegerType()).alias("ESTADO"),                              # campo 15        
        siniestros_final_df["NO_MESADAS"].cast(DecimalType(2,0)).alias("NO_MESADAS"),                   # campo 20
        siniestros_final_df["GRADO_INVALIDEZ_INICIAL"].cast("decimal(5,2)").alias("GRADO_INVALIDEZ_INICIAL"),  # campo 22
        F.to_date(siniestros_final_df["FECHA_ULTIMA_MOD"], "yyyy/MM/dd").alias("FECHA_ULTIMA_MOD"),            # campo 23
        siniestros_final_df["GRADO_INVALIDEZ_ACTUAL"].cast("decimal(5,2)").alias("GRADO_INVALIDEZ_ACTUAL"),    # campo 24
        siniestros_final_df["VALOR_AUXILIO_FUNERARIO"].cast(IntegerType()).alias("VALOR_AUXILIO_FUNERARIO"),   # campo 57
        siniestros_final_df["PAR_0"].cast(DecimalType(28,10)).alias("PAR_0"),                            # campo 61
        siniestros_final_df["PARMA"].cast(DecimalType(28,10)).alias("PARMA"),                            # campo 62
        siniestros_final_df["POR_AMORT_ACUM"].cast(DecimalType(10,2)).alias("POR_AMORT_ACUM"),           # campo 64
    
# ----------------------------- columnas archivo -----------------------------                                     
        campos_unica_vez["INTERES_TECNICO_NIIF4"].cast("decimal(14,2)").alias("INTERES_TECNICO_NIIF4"),
        campos_unica_vez["FAIR_VALUE"].cast("decimal(14,2)").alias("FAIR_VALUE"),
        
# ----------------------------- columnas de auditoría -----------------------------
        sar["periodo_siniestro"].alias("PERIODO_CAUSACION_DERECHO"),                                             # campo 5
        F.to_date(sar["fecha_inicio_periodo_siniestro"], "yyyy/MM/dd").alias("FECHA_INICIO_PERIODO_CAUSACION_DERECHO"),                  # campo 5
        F.to_date(sar["fecha_fin_periodo_siniestro"], "yyyy/MM/dd").alias("FECHA_FIN_PERIODO_CAUSACION_DERECHO")                          # campo 5
    ).distinct()
    
 
#-----------------------------------------------------------------funcion pivot beneficiarios --------------------------------------------------------------------------------------------#
def pivot_beneficiarios(df, max_n=15):
    """
    Pivotear beneficiarios de filas a columnas.
    
    :param df: DataFrame con al menos las columnas:
               - nid_rentas
               - rw_beneficiario
               - estado, estudios, fecha_nacimiento, tipo_identif,
                 no_identif, nombre, parent, sexo, con_derecho
    :param max_n: Número máximo de beneficiarios a pivotear.
    :return: DataFrame con columnas expandidas hasta max_n.
    """
    
    def generar_campos(n):
        return [
            F.max(F.when(F.col("rw_beneficiario") == n, F.col("estado"))).alias(f"ESTADO_{n}"),
            F.max(F.when(F.col("rw_beneficiario") == n, F.col("estudios"))).alias(f"ESTUDIOS_{n}"),
            F.max(F.when(F.col("rw_beneficiario") == n, F.col("fecha_nacimiento"))).alias(f"FECHA_NACIMIENTO_{n}"),
            F.max(F.when(F.col("rw_beneficiario") == n, F.col("tipo_identif"))).alias(f"TIPO_IDENTIF_{n}"),
            F.max(F.when(F.col("rw_beneficiario") == n, F.col("no_identif"))).alias(f"NO_IDENTIF_{n}"),
            F.max(F.when(F.col("rw_beneficiario") == n, F.col("nombre"))).alias(f"NOMBRE_{n}"),
            F.max(F.when(F.col("rw_beneficiario") == n, F.col("parent"))).alias(f"PARENT_{n}"),
            F.max(F.when(F.col("rw_beneficiario") == n, F.col("sexo"))).alias(f"SEXO_{n}"),
            F.max(F.when(F.col("rw_beneficiario") == n, F.col("con_derecho"))).alias(f"CON_DERECHO_{n}")
        ]
    
    # Generar todas las expresiones hasta max_n
    expresiones = []
    for i in range(1, max_n + 1):
        expresiones.extend(generar_campos(i))
    
    # Ejecutar el groupBy con todas las expresiones
    return df.groupBy("nid_siniestro_ben_key").agg(*expresiones)

#-----------------------------------------------------------------join beneficiarios ----------------------------------------------------------------------------------------------------#

join_ben_df =(
#sar_siniestros
sar
#par_trabajadores
.join(tra, sar["nid_trabajador"] == tra["nid_trabajador"], "inner")
# sar_datos_invalidez_muerte filtrado
   .join(finv, sar["nid_siniestro"] == finv["nid_siniestro"], "inner")
#sar_auditoria_medica filtrado
   .join(fuad, finv["nid_auditoria_medica"] == fuad["nid_auditoria_medica"], "inner")
#ind_datos_reservas_siniestros
   .join(res, sar["nid_siniestro"] == res["nid_siniestro"], "inner")
#sar_beneficiarios_siniestros
   .join(ben, sar["nid_siniestro"] == ben["nid_siniestro"], "left")
#acu_personas_naturales beneficiarios
   .join(ben_apn, (ben["COD_TIPO_IDENTIFICACION"] == ben_apn["COD_TIPO_IDENTIFICACION"]) &
              (ben["NID_NUMERO_IDENTIFICACION"] == ben_apn["NID_NUMERO_IDENTIFICACION"]), "left")
#cgr_equivalencia_tipos_id beneficiarios
   .join(ben_eqid, ben["cod_tipo_identificacion"] == ben_eqid["cod_tipo_documento"], "left")
)
#-----------------------------------------------------------------case y pivot beneficiarios --------------------------------------------------------------------------------------------#
# Definimos ventana para el dense_rank

# Ventana
window_spec = Window.partitionBy(col("ben.nid_siniestro")) \
                   .orderBy(
                       col("cod_parentesco_int").asc_nulls_last(),
                       col("edad_beneficiario").asc()
                   )

# Una sola instrucción:
rank_ben_df = join_ben_df.select(
    # clave técnica (renombrada)
    col("sar.nid_siniestro").alias("nid_siniestro_ben_key").cast("decimal(10,0)"),

    # numerador ventana
    row_number().over(window_spec).alias("rw_beneficiario").cast("int"),

    # parent
    when((col("ben.cod_parentesco") == "1") & (col("ben.bln_con_derecho") == "S"), lit(5))
    .when((col("ben.cod_parentesco") == "2") & (col("ben.bln_con_derecho") == "S"), lit(6))
    .when(
        (col("ben.cod_parentesco").isin("3", "10")) & (col("ben.bln_con_derecho") == "S"),
        when(
            floor(
                months_between(
                    to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd"),
                    col("ben_apn.fec_nacimiento")
                ) / 12
            ) < 30,
            lit(7)
        ).otherwise(lit(4))
    )
    .when(
        (col("ben.cod_parentesco") == "4") & (col("ben.bln_con_derecho") == "S"),
        when(col("ben.cod_estado_persona") == "2", lit(2))
        .when(col("ben.cod_estado_persona") == "1", lit(3))
    )
    .when((col("ben.cod_parentesco") == "6") & (col("ben.bln_con_derecho") == "S"), lit(8))
    .otherwise(None)
    .alias("parent").cast("int"),

    # tipo identif
    col("ben_eqid.COD_TIPO_SUPER").alias("tipo_identif").cast("int"),

    # no identif
    col("ben.nid_numero_identificacion").alias("no_identif").cast("string"),

    # sexo
    when(col("ben_apn.genero") == "M", lit(1))
    .when(col("ben_apn.genero") == "F", lit(2))
    .alias("sexo").cast("int"),

    # fecha nacimiento
    col("ben_apn.fec_nacimiento").alias("fecha_nacimiento").cast("date"),

    # nombre
    trim(
        substring(
            regexp_replace(
                concat_ws(
                    " ",
                    trim(coalesce(col("ben_apn.PRIMER_APELLIDO"), lit(""))),
                    trim(coalesce(col("ben_apn.SEGUNDO_APELLIDO"), lit(""))),
                    trim(coalesce(col("ben_apn.PRIMER_NOMBRE"), lit(""))),
                    trim(coalesce(col("ben_apn.SEGUNDO_NOMBRE"), lit("")))
                ),
                " +", " "
            ),
            1, 30
        )
    ).alias("nombre").cast("string"),

    # estudios
    when(
        (col("ben.cod_parentesco") == "4") &
        (col("ben.cod_estado_persona") == "1") &
        (
            floor(
                months_between(
                    to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd"),
                    col("ben_apn.fec_nacimiento")
                ) / 12
            ) >= 18
        ),
        lit(1)
    ).otherwise(None)
    .alias("estudios").cast("int"),

    # estado
    when((col("ben_apn.fec_muerte").isNotNull()) | (col("cod_origen") == "1"), lit(3))
    .when(col("cod_origen") == "2", lit(2))
    .otherwise(lit(1))
    .alias("estado").cast("int"),

    # con derecho
    col("BLN_CON_DERECHO").alias("con_derecho").cast("string")
)

#Pivot final
beneficiarios_final_df = pivot_beneficiarios(rank_ben_df, max_n=15)

#-----------------------------------------------------------------join rentas y beneficiario-----------------------------------------------------------------------------------------#
join_arp_final_df = (
    siniestros_final_dff
    .join(
        beneficiarios_final_df,
        on=[
            F.col("nid_siniestro_key") == F.col("nid_siniestro_ben_key")
        ],
        how="left"
    )
)
#-----------------------------------------------------------------campos nulos y constantes----------------------------------------------------------------------------------------#
#Add cols campo 1, campo 2, campo 8, campo 14, campo 21, campo 50, campo 51, campo 53, campo 54, campo 55, campo 56, campo 58, campo 63, campo 65, campo 67, campo 68, campo 70, campo 71, campo 75
df_campos_arp_final = join_arp_final_df \
    .withColumn("RAMO", lit(39)) \
    .withColumn("UNIDAD", lit(1).cast(IntegerType())) \
    .withColumn("PARENT", lit(1).cast(IntegerType())) \
    .withColumn("ESTUDIOS", lit(None).cast(IntegerType())) \
    .withColumn("RETROACTIVO", lit(None).cast(StringType())) \
    .withColumn("NO_RAD_NOT_TEC", lit(2011028468000000).cast(DecimalType(20,0))) \
    .withColumn("INTERES_TEC_PRIMA", lit(None).cast(DecimalType(19, 10))) \
    .withColumn("GASTOS_PRIMA", lit(None).cast(DecimalType(5, 4))) \
    .withColumn("PRIMA", lit(None).cast(DecimalType(30, 2))) \
    .withColumn("FECHA_INICIAL_RENTA_DIFERIDA", lit(None).cast(DateType())) \
    .withColumn("VALOR_MESADA_BENEF_PERIO_DIFER", lit(None).cast(DecimalType(10,2))) \
    .withColumn("OBSERVACIONES", lit(None).cast(StringType())) \
    .withColumn("PORCENTAJE_EXC_PARMA", lit(None).cast(DecimalType(10,2))) \
    .withColumn("FEC_AVISO_SINIESTRO", lit(None).cast(DateType())) \
    .withColumn("INSCRIPCION_MECANISMO", lit(0).cast(IntegerType())) \
    .withColumn("FECHA_INSCRIPCION", lit(None).cast(DateType())) \
    .withColumn("TASA_FAC_SEG", lit("0").cast(DecimalType(10, 2))) \
    .withColumn("VALOR_COBERTURA", lit(None).cast(DecimalType(14, 2))) \
    .withColumn("EQUIVALENCIA", lit(None).cast(StringType())) \
    .withColumn("ID_LOTE_CARGUE", lit(1).cast(IntegerType()))

#-----------------------------------------------------------------select final ---------------------------------------------------------------------------------------------------------#
column_names = [field.name for field in schema_reserva_arp_gold]
df_campos_arp_final = df_campos_arp_final.select(*column_names)


from pyspark.sql.types import IntegerType, LongType, DecimalType, StringType
from pyspark.sql.functions import col

# Revisa tu schema_reserva_arp_gold
for field in df_campos_arp_final.schema.fields:
    # Si en el schema esperas Integer pero el dato es Decimal, castea a Long
    if field.name == "NO_RAD_NOT_TEC":
        df_campos_arp_final = df_campos_arp_final.withColumn(
            field.name, col(field.name).cast(LongType())
        )
#-----------------------------------------------------------------Save files Data ------------------------------------------------------------------------------------------------------#
reserva_arp_gold_df = spark.createDataFrame(df_campos_arp_final.rdd, schema=schema_reserva_arp_gold)

partition_columns = ["ID_LOTE_CARGUE"]


# Save the transformed DataFrame to S3
frame_save = DynamicFrame.fromDF(reserva_arp_gold_df, glueContext, "frame_save_dynamic")

s3_prefix = f"GOLD/NIIF/{TABLE_NAME_GOLD}/"

s3_path = f"s3://{BUCKET_NAME}/{s3_prefix}"



#logger.info(f"Truncate & Saving data to: {s3_path}")

extract_helper.delete_s3_folder(BUCKET_NAME, s3_prefix)
extract_helper.save_files_s3(frame_save, s3_path, partition_columns)

import boto3
import pandas as pd
from io import StringIO

# === Parámetros ===
bucket_name = "alfa-niif-bronce-test"
prefix = "ALFAPRD-UT/parametrosModelo/"

# === Cliente S3 ===
s3 = boto3.client("s3")

# === Listar archivos CSV en el prefijo ===
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

csv_files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]
if not csv_files:
    raise FileNotFoundError(f"❌ No se encontraron archivos CSV en s3://{bucket_name}/{prefix}")

# === Leer cada archivo en un DataFrame ===
dfs = {}
for key in csv_files:
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    csv_data = obj["Body"].read().decode("utf-8")
    df_name = key.split("/")[-1].replace(".csv", "").upper()
    dfs[df_name] = pd.read_csv(StringIO(csv_data))
    print(f"✅ {df_name:<30} → {dfs[df_name].shape[0]} filas, {dfs[df_name].shape[1]} columnas")

# === Asignar DataFrames individuales ===
df_tipo_artefacto = dfs.get("TIPO_ARTEFACTO")
df_tipo_concepto = dfs.get("TIPO_CONCEPTO")
df_tipo_nota = dfs.get("TIPO_NOTA")
df_artefacto = dfs.get("ARTEFACTO")
df_concepto = dfs.get("CONCEPTO")
df_nota = dfs.get("NOTA")
df_artefacto_concepto_nota = dfs.get("ARTEFACTO_CONCEPTO_NOTA")

print("\n📊 Ejemplo de contenido (TIPO_ARTEFACTO):")
print(df_tipo_artefacto.head())

job.commit()