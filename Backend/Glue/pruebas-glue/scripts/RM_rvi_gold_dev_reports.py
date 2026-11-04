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
import time

start_time = time.time()

#-----------------------------------------------------------------SET ARGS AND CONFIG COMMONS----------------------------------------------------------------------------------------------#
logs = LogsHelper()
logger = logs.logger_conf()

args = getResolvedOptions(
    sys.argv, 
        [
        'JOB_NAME',
        'BUCKET_NAME',
        'DB_NAME',
        'DB_SCHEMA',
        'BUCKET_NAME_ASSETS',
        'PERIOD',
        'CERTIFICATE',
        'DB_CATALOG_GLUE_BRONCE',
        'DB_CATALOG_GLUE_SILVER',
        'TABLE_NAME_GOLD',
        'VALORES_CONSTANTES',
        'FEC_CORTE_DATE'
        ]
    )
    
BUCKET_NAME = args['BUCKET_NAME']
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
DB_NAME = args['DB_NAME']
DB_SCHEMA = args['DB_SCHEMA']
PERIOD = args['PERIOD']
CERTIFICATE = args['CERTIFICATE']
DB_CATALOG_GLUE_BRONCE = args['DB_CATALOG_GLUE_BRONCE']
DB_CATALOG_GLUE_SILVER = args['DB_CATALOG_GLUE_SILVER']
TABLE_NAME_GOLD = args['TABLE_NAME_GOLD']
FEC_CORTE_DATE = args['FEC_CORTE_DATE']

VALORES_CONSTANTES = json.loads(args['VALORES_CONSTANTES'])

logger.info(f"Period: {PERIOD}")
logger.info(f"DB_CATALOG_GLUE_SILVER: {DB_CATALOG_GLUE_SILVER}")


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

#----------------------------------------------------------------StructType final-----------------------------------------------------------------------------------------------------------#
logger.info(f"StructType final: {TABLE_NAME_GOLD}")
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
    logger.info(f"Extract source: {DB_SCHEMA}")
    #Read tables from silver prv
    # SELECT rentas
    df_prv_rentas = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_PRV_SILVER_PRV_RENTAS").toDF()
    df_prv_rentas = df_prv_rentas.select("nid_rentas", "nid_siniestro", "fec_expedicion", "fec_causacion", "nid_numero_identificacion", "num_mesadas", "pcj_pcl_inicial", "fec_ultima_mod", "pcj_pcl_actual", "cod_origen", "cod_afp", "pcj_interes_tecnico", "pcj_interes_reserva", "fec_invalidacion_registro", "fec_envio_point", "cod_tipo_identificacion", "vlr_pension_inicial", "fec_causacion", "fec_ultima_mod", "cod_estado_persona", "vlr_salario_base").distinct()
    
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
    df_prv_endosos_point = df_prv_endosos_point.select("nid_rentas", "vlr_prima").distinct()
    
    # SELECT siniestros
    df_spr_siniestros = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_PRV_SILVER_SPR_SINIESTROS").toDF()
    df_spr_siniestros = df_spr_siniestros.select("nid_siniestro", "cod_estado_siniestro").distinct()
    
    # SELECT crv_Calculos_2
    df_crv_calculos_2 = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_PRV_SILVER_CRV_CALCULOS_2").toDF()
    df_crv_calculos_2 = df_crv_calculos_2.select("nid_calculo", "nid_rentas", "fec_corte_calculo", "pcj_factor_seguridad", "pcj_tasa_administracion", "pcj_tasa_administracion", "vlr_exceso_smmlv_ipc").distinct()
    
    #Read tables from silver general
    #acu_personas_naturales
    df_acu_personas_naturales = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ACU_PERSONAS_NATURALES").toDF()
    df_acu_personas_naturales = df_acu_personas_naturales.select("cod_tipo_identificacion", "nid_numero_identificacion", "fec_nacimiento", "fec_muerte", "genero", "primer_apellido", "segundo_apellido", "primer_nombre", "segundo_nombre").distinct()
    
    #rva_reserva_matematica_transic
    df_rva_reserva_matematica_transic = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_RVA_RESERVA_MATEMATICA_TRANSIC").toDF()
    df_rva_reserva_matematica_transic = df_rva_reserva_matematica_transic.select("nid_siniestro_renta", "ramo", "vlr_reserva_rv89", "vlr_reserva_rv08" ).distinct()
    
    #anp_pensionado_novedad
    df_anp_pensionado_novedad = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PENSIONADO_NOVEDAD").toDF()
    df_anp_pensionado_novedad= df_anp_pensionado_novedad.select("nid_distribucion_vigencia", "vlr_fijo", "cod_concepto").distinct()
    
    #anp_distribucion_vig_pension
    df_anp_distribucion_vig_pension = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ANP_DISTRIBUCION_VIG_PENSION").toDF()
    df_anp_distribucion_vig_pension = df_anp_distribucion_vig_pension.select("nid_pension", "nid_distribucion_vigencia").distinct()
    
    #anp_pensiones
    df_anp_pensiones = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ANP_PENSIONES").toDF()
    df_anp_pensiones = df_anp_pensiones.select("nid_procedencia", "nid_pension").distinct()
    
    #anp_movimientos_pago
    df_anp_movimientos_pago = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_SILVER}", table_name="ALFAPRD_GENERAL_SILVER_ANP_MOVIMIENTOS_PAGO").toDF()
    df_anp_movimientos_pago = df_anp_movimientos_pago.select("periodo_pago", "nid_distribucion_vigencia", "fec_inicial", "cod_concepto").distinct()
    
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
    logger.info(f"Extract source: FILE")
    #Read tables from bronce file
    df_files_campos_unica_vez = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_BRONCE}", table_name="FILES_CAMPOS_UNICA_VEZ").toDF()
    
    
    #---------------------------------------------------------------------Init Businees Logic------------------------------------------------------------------------------------------------#
    
    logger.info(f"Writing to {TABLE_NAME_GOLD} with period = {PERIOD}, Init Businees Logic")
    
    #---------------------------------------------------------------------alias tablas ------------------------------------------------------------------------------------------------------#
    # Alias para todos los DataFrames de PRV
    logger.info(f"Alias for all DataFrames of  {DB_SCHEMA}")
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
    

    #-------------------------------------------------------------------------Variables-----------------------------------------------------------------------------------------------------# 
    logger.info(f"Calculation of Variables")
    # Convertir a objeto datetime
    dt = datetime.strptime(FEC_CORTE_DATE, "%Y-%m-%d")
    
    # Periodo (yyyymm)
    FEC_CORTE_PERIOD = dt.strftime("%Y%m")
    
    # Año (yyyy)
    FEC_CORTE_ANIO = int(dt.strftime("%Y")) 
    
    #Salario
    #salario_minimo_actual = sal_min.filter((col("ano") == FEC_CORTE_ANIO) & (col("mes") == 1) ).select("vlr_salario_minimo").first()[0]

    
    #----------------------------------------------------------withColumn source------------------------------------------------------------------------------------------------------------#
    logger.info(f"Add columns to sources")
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
    logger.info(f"Generate subqueries")
    
    # Filtro principal de fecha
    ren = ren.filter((date_format(ren["fec_expedicion"], "yyyyMM") >= F.lit(FEC_CORTE_PERIOD)))
    
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
        pen_nov.filter(F.col("cod_concepto") == "28"),
        dist_vig_pen["nid_distribucion_vigencia"] == pen_nov["nid_distribucion_vigencia"],
        "left"
    ).groupBy(ren["nid_rentas"]).agg(
        coalesce(max(pen_nov["vlr_fijo"]), lit(0)).alias("VLR_AUX_FUNERARIO_BASE")
    )
    

    #----------------------------------------------------------------------join principal----------------------------------------------------------------------------------------------------#
    logger.info(f"Join Main Rentas")
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
    
    #join_rentas_df_path  = f"s3://{BUCKET_NAME}/GOLD/NIIF/RM_TMP/join_rentas_df_tmp"
    #join_rentas_df.write.mode("overwrite").parquet(join_rentas_df_path)
    #rentas_df = spark.read.parquet(join_rentas_df_path).repartition(200)  
    
    logger.info("Create Main Join")
    
    #---------------------------------------------------------------------subqueries post join ------------------------------------------------------------------------------------------------------#
    logger.info(f"Subqueries post join")
    
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
        .join(incremento_factor_df, on=["nid_rentas", "periodo_pago"], how="left")
    
    )
    
    #---------------------------------------------------------------------case campos ------------------------------------------------------------------------------------------------------#
    logger.info(f"Case columns")
    #campo 7  ORIGEN_PENSION
    final_df = join_rentas_df.withColumn("ORIGEN_PENSION",
        when(per_nat["fec_muerte"].isNull(),
            when(ren["nid_siniestro"].isNotNull(),
                when(df_spr_siniestros["cod_estado_siniestro"] == "4", 3)
                .otherwise(1)
            ).when((ren["nid_siniestro"].isNull()) & (ren["cod_origen"] == "3"), 3)
             .when((ren["nid_siniestro"].isNull()) & (ren["cod_afp"] != "1") & (ren["cod_origen"] == "2"), 1)
             .when((ren["nid_siniestro"].isNull()) & (ren["cod_origen"] == "2"), 1)
        ).otherwise(
            when(ren["cod_origen"] == "1",
                when((date_format(per_nat["fec_muerte"], "yyyyMMdd").cast("long") >= date_format(ren["fec_expedicion"], "yyyyMMdd").cast("long")) &
                     ren["nid_siniestro"].isNull(), 5)
                .when(date_format(per_nat["fec_muerte"], "yyyyMMdd").cast("long") < date_format(ren["fec_expedicion"], "yyyyMMdd").cast("long"), 2)
                .when((date_format(per_nat["fec_muerte"], "yyyyMMdd").cast("long") >= date_format(ren["fec_expedicion"], "yyyyMMdd").cast("long")) &
                      ren["nid_siniestro"].isNotNull(), 4)
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
            ((res_mat["vlr_reserva_rv89"] / res_mat["vlr_reserva_rv08"]) * 100 +  col("PARMA") * (months_between(to_date(mov_pago_filter["periodo_pago"]), F.to_date(F.lit("201804"), "yyyyMM")) + 91)) > 100, 100)
            .otherwise((res_mat["vlr_reserva_rv89"] / res_mat["vlr_reserva_rv08"]) * 100 + col("PARMA") * (months_between(to_date(mov_pago_filter["periodo_pago"]), F.to_date(F.lit("201804"), "yyyyMM")) + 91)
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
            ((col("ren.vlr_pension_inicial") * col("factor_incremento_acumulado")) < col("corte.vlr_salario_minimo")),
            col("corte.vlr_salario_minimo")
        ).otherwise(
            round(col("ren.vlr_pension_inicial") * col("factor_incremento_acumulado"), 2)
        ).cast(DecimalType(16, 2))
    )
    
    # VALOR_AUXILIO_FUNERARIO
    final_df = final_df.withColumn(
        "VALOR_AUXILIO_FUNERARIO",
        when(
            col("ORIGEN_PENSION").isin(1, 3),
            when(
                col("mesada") <= (5 * col("corte.vlr_salario_minimo")),
                round(lit(5 * col("corte.vlr_salario_minimo")), 2)
            ).when(
                (col("mesada") > (5 * col("corte.vlr_salario_minimo"))) & 
                (col("mesada") <= (10 * col("corte.vlr_salario_minimo"))),
                round(col("mesada"), 2)
            ).otherwise(
                round(lit(10 * col("corte.vlr_salario_minimo")), 2)
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
            (col("COD_PARENTESCO_BEN") == 3) & 
            (col("COD_PARENTESCO_BEN") == 3),
            when(col("fec_nacimiento") > col("fec_nacimiento"),
                (25 - col("edad_beneficiario") + 1) * 12
            ).otherwise(
                (25 - col("edad_beneficiario") + 1) * 12
            )
        ).when((col("clase_pension") == 3) & 
            (col("COD_PARENTESCO_BEN") == 3) & 
            col("COD_PARENTESCO_BEN").isNull(),
            (25 - col("edad_beneficiario") + 1) * 12
        ).when((col("clase_pension") == 3) & (
           ((col("COD_PARENTESCO_BEN") == 7) & (col("COD_PARENTESCO_BEN") == 7)) |
           ((col("COD_PARENTESCO_BEN") == 7) & col("COD_PARENTESCO_BEN").isNull())),
        when((year(to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd")) - year(per_nat["fec_muerte"])) == 20,
             floor(240 - months_between(to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd"), per_nat["fec_muerte"]))
        ).otherwise(
             (20 - (year(to_date(lit(FEC_CORTE_DATE), "yyyy-MM-dd")) - year(per_nat["fec_muerte"]))) * 12
        )
        ).when((col("clase_pension") == 3) & (
           ((col("COD_PARENTESCO_BEN") == 3) & (col("COD_PARENTESCO_BEN") == 7)) |
           ((col("COD_PARENTESCO_BEN") == 7) & (col("COD_PARENTESCO_BEN") == 3))),
        when(col("COD_PARENTESCO_BEN") == 3,
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
    
    #-----------------------------------------------------------------------columnas auditoria ----------------------------------------------------------------------------------------------#
    logger.info(f"Select Base - Auditoria columns")
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
    
    #--------------------------------------------------------------select enriquecido--------------------------------------------------------------------------------------------------------#
    logger.info(f"Select Enriquecido")
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
    
    #-----------------------------------------------------------------select final ----------------------------------------------------------------------------------------------------------#
    logger.info(f"Select final")
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
    
    #-----------------------------------------------------------------select final base casteos--------------------------------------------------------------------------------------------#
    logger.info(f"Select Casts")
    try:
        rentas_final_dff = rentas_final_df.select(
    # ----------------------------- campos base -----------------------------
            ren_not["cod_ramo_sfc"].cast("int").alias("RAMO"),                               # NUMBER(2,0)
            col("ren.nid_rentas").cast("int").alias("NO_POLIZA"),                            # NUMBER(10,0) → to_char
            to_date(col("ren.fec_expedicion"), "yyyy/MM/dd").alias("VIG_INICIAL"),                         # DATE
            ren["nid_rentas"].cast("int").alias("NO_SINIESTRO"),                             # NUMBER(10,0)
            ren["vlr_salario_base"].cast("decimal(10,2)").alias("INGRESO_BASE_LIQUIDACION"),
            to_date(col("ren.fec_causacion"), "yyyy/MM/dd").alias("FECHA_CAUSACION_DERECHO"),              # DATE
            eq_tid["cod_tipo_super"].cast("int").alias("TIPO_IDENTIF"),                      # NUMBER(2,0)
            ren["nid_numero_identificacion"].cast("string").alias("NO_IDENTIF"),             # VARCHAR2(15)
            to_date(col("per_nat.fec_nacimiento"), "yyyy/MM/dd").alias("FECHA_NACIMIENTO"),                # DATE
            est_per["cod_estado_persona"].cast("int").alias("ESTADO"),                       # NUMBER(1,0)
            ren["num_mesadas"].cast("int").alias("NO_MESADAS"),                              # DECIMAL(2,0)
            ren["pcj_pcl_inicial"].cast("decimal(5,2)").alias("GRADO_INVALIDEZ_INICIAL"),    # decimal(7,2)
            to_date(col("ren.fec_ultima_mod"), "yyyy/MM/dd").alias("FECHA_ULTIMA_MOD"),                    # DATE
            ren["pcj_pcl_actual"].cast("decimal(7,2)").alias("GRADO_INVALIDEZ_ACTUAL"),      # decimal(7,2)
            ren_not["num_radicado_nota"].cast("decimal(20,0)").alias("NO_RAD_NOT_TEC"),                # NUMBER
            ipc["pcj_ipc"].cast("decimal(5,2)").alias("TASA_CREC_PRIMA"),                    # DECIMAL
            coalesce(res_mat["vlr_reserva_rv89"], lit(VALORES_CONSTANTES["VALOR_RESERVA_RV89"])).cast("decimal(14,2)").alias("VALOR_RESERVA_RV89"),   # DECIMAL(16,2)
            coalesce(res_mat["vlr_reserva_rv08"], lit(VALORES_CONSTANTES["VALOR_RESERVA_RV08"])).cast("decimal(14,2)").alias("VALOR_RESERVA_RV08"),   # DECIMAL(16,2)
            ren_aprob["fecha_inscripcion"].cast("date").alias("FECHA_INSCRIPCION"),          # DATE
            ren_aprob["cobertura"].cast("decimal(14,2)").alias("VALOR_COBERTURA"),           # DECIMAL(16,2)
            to_date(col("ren.fec_expedicion"), "yyyy/MM/dd").alias("FECHA_EXPED_POLIZA"),                  # DATE
            to_date(col("per_nat.fec_muerte"), "yyyy/MM/dd").alias("FECHA_MUERTE_AFILIADO"),               # DATE
    
    # ----------------------------- agregados intermedios -----------------------------
            df_hijos["num_hijos"].cast("int").alias("NO_HIJOS"),                             # NUMBER(15,0)
            sum_prima["vlr_prima"].cast("decimal(17,2)").alias("PRIMA"),                     # DECIMAL(16,2)
            df_hijos_invalidos["num_hijos_invalidos"].cast("int").alias("NO_HIJOS_INVALIDOS"), # NUMBER(15,0)
        
            rentas_final_df["PARAMETRO_DESLIZAMIENTO"].cast("decimal(5,4)").alias("PARAMETRO_DESLIZAMIENTO"),
            calc2_latest["pcj_factor_seguridad"].cast("decimal(10,2)").alias("TASA_FAC_SEG"),        # DECIMAL(10, 2)
            calc2_latest["pcj_tasa_administracion"].cast("decimal(5,4)").alias("GASTOS_PRIMA"),      # DECIMAL(7,4)
            calc2_latest["pcj_tasa_administracion"].cast("decimal(5,4)").alias("GASTOS"),            # DECIMAL(7,4)
    
            rentas_final_df["OBSERVACIONES"].cast("string").alias("OBSERVACIONES"),          # VARCHAR
            rentas_final_df["VALOR_AUXILIO_FUNERARIO"].cast("int").alias("VALOR_AUXILIO_FUNERARIO"),
            rentas_final_df["MESADA"].cast("decimal(16,2)").alias("MESADA"),                 # DECIMAL(16,2)
            rentas_final_df["INSCRIPCION_MECANISMO"].cast("int").alias("INSCRIPCION_MECANISMO"),
            coalesce(rentas_final_df["POR_AMORT_ACUM"], lit(VALORES_CONSTANTES["POR_AMORT_ACUM"])).cast("decimal(10,2)").alias("POR_AMORT_ACUM"),
            coalesce(rentas_final_df["PARMA"], lit("0")).cast("decimal(28,10)").alias("PARMA"),
            coalesce(rentas_final_df["PAR_0"], lit("0")).cast("decimal(28,10)").alias("PAR_0"),
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
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    
    #-----------------------------------------------------------------funcion pivot beneficiarios ----------------------------------------------------------------------------------------#
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
    
    #-----------------------------------------------------------------join beneficiarios ----------------------------------------------------------------------------------------------------#
    logger.info(f"Join beneficiarios Rentas")
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
            (dist_vig_pen["nid_distribucion_vigencia"] == mov_pago_filter["nid_distribucion_vigencia"]),
            "inner"
        )
        # prv_beneficiario 
        .join(
            ben,
            (ben["nid_rentas"] == ren["nid_rentas"]),
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
        .filter(F.date_format(ren["fec_expedicion"], "yyyyMM") >=  F.lit(FEC_CORTE_PERIOD))
        #.filter(ren["nid_rentas"] == 137469)
    )
    
    #-----------------------------------------------------------------case y pivot beneficiarios --------------------------------------------------------------------------------------------#
    logger.info(f"Case and pivot columns of beneficiarios")
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
            F.when(F.col("per_nat.genero") == "M", 1)
             .when(F.col("per_nat.genero") == "F", 2)
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
    
    #-----------------------------------------------------------------join rentas y beneficiario-----------------------------------------------------------------------------------------#
    logger.info(f"Join Final Rentas")
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
    
    #-----------------------------------------------------------------campos nulos y constantes----------------------------------------------------------------------------------------#
    logger.info(f"Add columns null and constants")
    campos_rvi_final_df = join_rvi_final_df.withColumn("UNIDAD", lit(1).cast(IntegerType()))\
        .withColumn("PARENT", lit(1).cast(IntegerType()))\
        .withColumn("ESTUDIOS", lit(None).cast(IntegerType()))\
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
    logger.info(f"Select Final struct Rentas")
    column_names = [field.name for field in schema_reserva_rvi_gold]
    campos_rvi_final_df = campos_rvi_final_df.select(*column_names)
    
    
    #-----------------------------------------------------------------Save files Data ------------------------------------------------------------------------------------------------------#
    logger.info(f"Save files Data")
    reserva_rvi_gold_df = spark.createDataFrame(campos_rvi_final_df.rdd, schema=schema_reserva_rvi_gold)
    
    partition_columns = ["PERIODO_PAGO_RENTA"]
    
    distinct_partitions = [row[0] for row in reserva_rvi_gold_df.select(f"{partition_columns[0]}").distinct().collect()]
      
    # Save the transformed DataFrame to S3
    frame_save = DynamicFrame.fromDF(reserva_rvi_gold_df, glueContext, "frame_save_dynamic")
    
    s3_prefix = f"GOLD/NIIF/{TABLE_NAME_GOLD}/"
    
    s3_path = f"s3://{BUCKET_NAME}/{s3_prefix}"
    
    
    for period in distinct_partitions:
            specific_partition_prefix = f"{s3_prefix}{partition_columns[0]}={period}/"
            logger.info(f"Truncate data for partitions {period} to: {specific_partition_prefix}")
            extract_helper.delete_s3_folder(BUCKET_NAME, specific_partition_prefix)
    
    logger.info(f"Saving data to: {s3_path}")
    
    try:
        extract_helper.save_files_s3(frame_save, s3_path, partition_columns)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    
    end_time = time.time()
    elapsed = end_time - start_time  
    logger.info(f"END, Total execution time: {elapsed/60:.2f} minutos")

except Exception as e:
    logger.error(f"An error occurred: {e}")