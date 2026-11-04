"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim, date_format, to_date, lit, coalesce, min, max, broadcast, year, when, concat_ws, substring, countDistinct, months_between, round, floor, greatest, concat, explode, sequence, log, exp, row_number, regexp_replace, concat, last_day, count, least, trunc, round as spark_round, first, desc, asc
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
    logger.info(f"Extract source: {DB_SCHEMA}")
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
    logger.info(f"Extract source: FILE")
    #Read tables from silver arpprod
    df_files_campos_unica_vez = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE_BRONCE}", table_name="FILES_CAMPOS_UNICA_VEZ").toDF()
    
#---------------------------------------------------------------------Init Businees Logic---------------------------------------------------------------------------------------------------#
    
    logger.info(f"Writing to {TABLE_NAME_GOLD} with period = {PERIOD}, Init Businees Logic")
    
#---------------------------------------------------------------------alias table's ---------------------------------------------------------------------------------------------------------#
    logger.info(f"Alias for all DataFrames of  {DB_SCHEMA}")
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
    logger.info(f"Calculation of Variables")
	# Convertir a objeto datetime
    dt = datetime.strptime(FEC_CORTE_DATE, "%Y-%m-%d")
    
    # Periodo (yyyymm)
    FEC_CORTE_PERIOD = dt.strftime("%Y%m")
    STR_CORTE_PERIOD = date_format(dt.strftime("%Y%m"), "yyyyMM")
    
    # Año (yyyy)
    FEC_CORTE_ANIO = int(dt.strftime("%Y")) 
    
    #----------------------------------------------------------withColumn source--------------------------------------------------------------------------------------------------------------#
    logger.info(f"Add columns to sources")
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
    
    #--------------------------------------------------------------subconsultas---------------------------------------------------------------------------------------------------------------#
    logger.info(f"Generate subqueries")
    # 1
    df_filter_sic = sic.filter(col("ramo") == "ARP")
    
    # 2 cobertura
    df_filter_cob = (
        cob.withColumn("anio_pago", year(col("fec_pago")))
            .withColumn("anio_mes_pago", date_format(col("fec_pago"), "yyyy-MM"))
            .filter( (date_format(col("fec_pago"), "yyyyMM") == lit(FEC_CORTE_PERIOD))
           ).select("nid_siniestro", "vlr_pago", "cod_cobertura", "FEC_PAGO", "nid_pago", "anio_pago", "anio_mes_pago"))
    
    
    df_siniestros_sin_17 = (
        df_filter_cob.groupBy("nid_siniestro", "anio_mes_pago")
        .agg(
            F.max(F.when(F.col("cod_cobertura") == 17, 1).otherwise(0)).alias("have_17"),
            F.max("fec_pago").alias("fec_referencia")
        )
        .filter(F.col("have_17") == 0)
    )
    
    
    df_registros_1717 = (
        df_siniestros_sin_17.select(
            col("nid_siniestro"),
            lit(None).alias("vlr_pago").cast("decimal(14,2)"),
            lit(1717).alias("cod_cobertura").cast("decimal(10,0)"),
            last_day(F.col("fec_referencia")).cast("timestamp").alias("fec_pago"),
            lit(0000).alias("nid_pago").cast("decimal(10,0)"), 
            year(last_day(col("fec_referencia"))).alias("anio_pago"),
            date_format(col("fec_referencia"), "yyyy-MM").alias("anio_mes_pago")
        )
    )

    df_final_cob_completo = df_filter_cob.unionByName(df_registros_1717)
    
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
    logger.info(f"Alias for all subqueries")
    fsal = df_filter_sal.alias("fsal")
    fmat = df_filter_sic.alias("fmat")
    fcov = df_final_cob_completo.alias("fcov")
    finv = df_filter_inv.alias("finv")
    fuad = df_filter_aud_1.alias("fuad")
    his = df_groupby_his.alias("his")
    hjs = df_groupby_hijos.alias("hjs")
    hjv = df_groupby_hijos_invalidos.alias("hjv")
    pcl = df_first_pcl.alias("pcl")
    
    #----------------------------------------------------------------------join principal-----------------------------------------------------------------------------------------------------#
    logger.info(f"Main Join Siniestros")
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
    #---------------------------------------------------------------------subqueries post join  --------------------------------------------------------------------------------------------------------#
    
    
    #Sar_calculos_Actuariales -> gastos, parametro_deslizamiento, tasa_fac_seg, FECHA_CALCULO
    
    df_subq_join = df_join_principal_reserva_arp.select(tra["nid_afiliacion"].alias("no_poliza"), finv["fec_aprobacion_pension"].alias("fecha_aprobacion_pension"), sar["nid_siniestro"].alias("no_siniestro")).distinct()
    
    window_max = Window.partitionBy("no_poliza", "no_siniestro", "fecha_aprobacion_pension").orderBy(col("nid_calculo").desc())
    calc2_latest = (
        df_subq_join.join(
            cal, 
            (df_subq_join["no_siniestro"] == cal["nid_siniestro"]) & 
            (df_subq_join["fecha_aprobacion_pension"] <= cal["fec_calculo"]), 
            "inner"
        )
        # Creamos una columna temporal con el ranking
        .withColumn("nid_calculo_max", F.max("nid_calculo").over(window_max))
        # Filtramos para quedarnos solo con el registro del nid_calculo máximo
        .filter(F.col("nid_calculo") == F.col("nid_calculo_max"))
        # Seleccionamos las columnas deseadas y eliminamos la temporal
        .select(
            "no_poliza",
            "no_siniestro",
            "fecha_aprobacion_pension",
            "nid_calculo",
            "pcj_admon",
            "pcj_deslizamiento_sm",
            "pcj_factor_seguridad",
            "fec_calculo"        
        )
    )
    
    
    df_join_principal_reserva_arp = (
        df_join_principal_reserva_arp    
        .join(calc2_latest, calc2_latest["no_siniestro"] == sar["nid_siniestro"], "inner") 
    )
    
    
    #---------------------------------------------------------------------case campos --------------------------------------------------------------------------------------------------------#
    logger.info(f"Case columns")
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
        when((col("tra_apn.fec_muerte").isNotNull()) |(col("fuad.cod_origen") == "1"), 3)
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
        when(col("his.FEC_DICTAMEN_INVMTE") <= to_date(lit("2005-07-25"), "yyyy-MM-dd"), 14)
        .when(
            (col("his.FEC_DICTAMEN_INVMTE") > to_date(lit("2005-07-25"), "yyyy-MM-dd")) & (col("his.FEC_DICTAMEN_INVMTE") <= fecha_base_dt),
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
        )  # Aquí aplicas el cast directamente
    )
    
    #-----------------------------------------------------------------select final base casteos-----------------------------------------------------------------------------------------------#
    logger.info(f"Select final base casts")
    siniestros_final_df = siniestros_final_df.select(
        
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
        calc2_latest["pcj_admon"].cast("decimal(5,4)").alias("GASTOS"),                            # DECIMAL(10, 2)
        calc2_latest["NID_CALCULO"].cast("decimal(10,0)").alias("NO_CALCULO"),                          # campo 193
        calc2_latest["FEC_CALCULO"].cast(DateType()).alias("FECHA_CALCULO"),                            # campo 194
        fsal["ANIO_SAL"].cast("decimal(4,0)").alias("ANO_SALARIO_MINIMO"),                                    # campo 195
        fsal["MES"].cast("decimal(2,0)").alias("MES_SALARIO_MINIMO"),                                   # campo 196
        fsal["VLR_SMIN"].cast("decimal(14,2)").alias("VLR_SALARIO_MINIMO"),                             # campo 197
        fcov["COD_COBERTURA"].cast("decimal(10,0)").alias("COD_COBERTURA"),                             # campo 198
        fcov["FEC_PAGO"].cast(DateType()).alias("FEC_PAGO_COBERTURA"),                                  # campo 199
        fcov["nid_pago"].cast(IntegerType()).alias("NO_PAGO"),                                             # campo 200
        siniestros_final_df["NOMBRE"].cast("string").alias("NOMBRE"),                                   # campo 13
        siniestros_final_df["SEXO"].cast(IntegerType()).alias("SEXO"),                                  # campo 11 * tra_apn
        siniestros_final_df["ESTADO"].cast(IntegerType()).alias("ESTADO"),                              # campo 15        
        siniestros_final_df["NO_MESADAS"].cast(IntegerType()).alias("NO_MESADAS"),                           # campo 20
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
    logger.info(f"beneficiarios Join Siniestros")
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
    logger.info(f"Case and columns of beneficiarios")
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
        when((col("ben.cod_parentesco") == "1"), lit(5))
        .when((col("ben.cod_parentesco") == "2") , lit(6))
        .when(
            (col("ben.cod_parentesco").isin("3", "10")),
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
            (col("ben.cod_parentesco") == "4") ,
            when(col("ben.cod_estado_persona") == "2", lit(2))
            .when(col("ben.cod_estado_persona") == "1", lit(3))
        )
        .when((col("ben.cod_parentesco") == "6") , lit(8))
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
        col("ben.cod_estado_persona").alias("estado").cast("int"),
    
        # con derecho
        col("BLN_CON_DERECHO").alias("con_derecho").cast("string")
    )
    
    #Pivot final
    beneficiarios_final_df = pivot_beneficiarios(rank_ben_df, max_n=15)
    
    #-----------------------------------------------------------------join rentas y beneficiario-----------------------------------------------------------------------------------------#
    logger.info(f"Final Join Siniestros")
    join_arp_final_df = (
        siniestros_final_df
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
    logger.info(f"Add columns null and constants")
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
    logger.info(f"Final select struct siniestros")
    column_names = [field.name for field in schema_reserva_arp_gold]
    df_campos_arp_final = df_campos_arp_final.select(*column_names)
    
    #-----------------------------------------------------------------Save files Data ------------------------------------------------------------------------------------------------------#
    logger.info(f"Save files Data")
    reserva_arp_gold_df = spark.createDataFrame(df_campos_arp_final.rdd, schema=schema_reserva_arp_gold)
    
    partition_columns = ["PERIODO_CAUSACION_DERECHO"]
    
    distinct_partitions = [row[0] for row in reserva_arp_gold_df.select(f"{partition_columns[0]}").distinct().collect()]
    
    # Save the transformed DataFrame to S3
    frame_save = DynamicFrame.fromDF(reserva_arp_gold_df, glueContext, "frame_save_dynamic")
    
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
    logger.info(f"END, Tiempo total de ejecución: {elapsed/60:.2f} minutos")

except Exception as e:
    logger.error(f"An error occurred: {e}")