"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.functions import col, trim

import sys, traceback, boto3, json

###################################################################
######## SET ARGS AND CONFIG COMMONS UTILS ########################
###################################################################

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
        'DB_CATALOG_GLUE'
        ]
    )
BUCKET_NAME = args['BUCKET_NAME']
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
DB_NAME = args['DB_NAME']
DB_SCHEMA = args['DB_SCHEMA']
PERIOD = args['PERIOD']
CERTIFICATE = args['CERTIFICATE']
DB_CATALOG_GLUE = args['DB_CATALOG_GLUE']


logger.info(f"Period: {PERIOD}")
logger.info(f"DB_CATALOG_GLUE_BRONCE: {DB_CATALOG_GLUE}")

# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME, DB_NAME)

###################################################################
#################### Init stage silver ############################
###################################################################

try:
    # Define the dictionary of tables and their respective partition columns.
    tables_case_use = {
        "ACU_PERSONAS_JURIDICAS": {
            "partition_columns": [],
            "schema": "GENERAL",
            "transform": 
        [
                lambda df: df.select(
                    trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
                    col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
                    col("NID_CONSECUTIVO").alias("NID_CONSECUTIVO"),
                    col("FEC_CONSTITUCION").alias("FEC_CONSTITUCION"),
                    col("PIN_POINT").alias("PIN_POINT"),
                    col("FEC_REGISTRO").alias("FEC_REGISTRO"),
                    trim(col("COD_REGIMEN")).alias("COD_REGIMEN"),
                    trim(col("COD_ACTIVIDAD_ECONOMICA_ICA")).alias("COD_ACTIVIDAD_ECONOMICA_ICA"),
                    trim(col("COD_ACTIVIDAD_ECONOMICA_DIAN")).alias("COD_ACTIVIDAD_ECONOMICA_DIAN"),
                    trim(col("SIGLA")).alias("SIGLA"),
                    trim(col("COD_ACTIVIDAD_ECONOMICA_SGSS")).alias("COD_ACTIVIDAD_ECONOMICA_SGSS"),
                    trim(col("COD_ACTIVIDAD_ECONOMICA_CIIU")).alias("COD_ACTIVIDAD_ECONOMICA_CIIU"),
                    trim(col("NOMBRE_COMERCIAL")).alias("NOMBRE_COMERCIAL"),
                    trim(col("COD_GRUPO_EMPRESARIAL")).alias("COD_GRUPO_EMPRESARIAL"),
                    col("DIGITO_VERIFICACION").alias("DIGITO_VERIFICACION"),
                    trim(col("RAZON_SOCIAL")).alias("RAZON_SOCIAL"),
                    trim(col("COD_ESTADO")).alias("COD_ESTADO"),
                    trim(col("COD_DEPARTAMENTO")).alias("COD_DEPARTAMENTO"),
                    trim(col("COD_CIUDAD")).alias("COD_CIUDAD"),
                    trim(col("COD_SECTOR_EMPRESARIAL")).alias("COD_SECTOR_EMPRESARIAL"),
                    trim(col("COD_NATURALEZA_EMPRESA")).alias("COD_NATURALEZA_EMPRESA"),
                    trim(col("COD_TIPO_EMPRESA")).alias("COD_TIPO_EMPRESA"),
                    trim(col("COD_CLASE_SOCIEDAD")).alias("COD_CLASE_SOCIEDAD"),
                    trim(col("COD_TAMANO_EMPRESA")).alias("COD_TAMANO_EMPRESA"),
                    trim(col("COD_ENTIDAD_SUPERVISORA")).alias("COD_ENTIDAD_SUPERVISORA"),
                    trim(col("OBSERVACIONES")).alias("OBSERVACIONES"),
                    trim(col("REPRESENTANTE_LEGAL")).alias("REPRESENTANTE_LEGAL"),
                    trim(col("COD_TIPO_IDENTIFICACION_REPLEG")).alias("COD_TIPO_IDENTIFICACION_REPLEG"),
                    trim(col("NID_NUMERO_IDENTIFICACION_REPL")).alias("NID_NUMERO_IDENTIFICACION_REPL"),
                    col("DIGITO_VERIFICACION_REPLEG").alias("DIGITO_VERIFICACION_REPLEG"),
                    col("FEC_INICIO_CESE_ACTIVIDAD").alias("FEC_INICIO_CESE_ACTIVIDAD"),
                    trim(col("COD_TIPO_CESE_ACTIVIDAD")).alias("COD_TIPO_CESE_ACTIVIDAD"),
                    col("FEC_FIN_ACTIVIDAD_COMERCIAL").alias("FEC_FIN_ACTIVIDAD_COMERCIAL"),
                    trim(col("USUARIO_MODIFICACION")).alias("USUARIO_MODIFICACION"),
                    col("FEC_MODIFICACION").alias("FEC_MODIFICACION"),
                    trim(col("CORREO_REPRESENTANTE_LEGAL")).alias("CORREO_REPRESENTANTE_LEGAL"),
                    trim(col("CARGO_REPRESENTANTE_LEGAL")).alias("CARGO_REPRESENTANTE_LEGAL")
                ),
                lambda df: df.dropDuplicates()
            ]
        },    
        "ACU_PERSONAS_NATURALES": {
            "partition_columns": [],
            "schema": "GENERAL",
            "transform": 
        [
            lambda df: df.select(
				trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
				col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
				trim(col("PRIMER_APELLIDO")).alias("PRIMER_APELLIDO"),
				trim(col("SEGUNDO_APELLIDO")).alias("SEGUNDO_APELLIDO"),
				trim(col("PRIMER_NOMBRE")).alias("PRIMER_NOMBRE"),
				trim(col("SEGUNDO_NOMBRE")).alias("SEGUNDO_NOMBRE"),
				col("FEC_NACIMIENTO").alias("FEC_NACIMIENTO"),
				trim(col("GENERO")).alias("GENERO"),
				trim(col("COD_ESTADO_CIVIL")).alias("COD_ESTADO_CIVIL"),
				col("PIN_POINT").alias("PIN_POINT"),
				col("FEC_MUERTE").alias("FEC_MUERTE"),
				col("FEC_REGISTRO").alias("FEC_REGISTRO"),
				col("VLR_NIVEL_INGRESOS").alias("VLR_NIVEL_INGRESOS"),
				trim(col("CARGO")).alias("CARGO"),
				trim(col("COD_OCUPACION")).alias("COD_OCUPACION"),
				trim(col("COD_ACTIVIDAD_ECONOMICA_CIIU")).alias("COD_ACTIVIDAD_ECONOMICA_CIIU"),
				trim(col("COD_DEPARTAMENTO_NACIMIENTO")).alias("COD_DEPARTAMENTO_NACIMIENTO"),
				trim(col("COD_CIUDAD_NACIMIENTO")).alias("COD_CIUDAD_NACIMIENTO"),
				trim(col("COD_TRATAMIENTO")).alias("COD_TRATAMIENTO"),
				trim(col("COD_ESTADO")).alias("COD_ESTADO"),
				trim(col("COD_TIPO_SANGUINEO")).alias("COD_TIPO_SANGUINEO"),
				trim(col("COD_EPS_ULTIMA")).alias("COD_EPS_ULTIMA"),
				trim(col("COD_NIVEL_EDUCATIVO")).alias("COD_NIVEL_EDUCATIVO"),
				trim(col("COD_PROFESION")).alias("COD_PROFESION"),
				trim(col("COD_ENTIDAD_EDUCATIVA")).alias("COD_ENTIDAD_EDUCATIVA"),
				trim(col("COD_ESTADO_SALUD")).alias("COD_ESTADO_SALUD"),
				trim(col("OBSERVACIONES")).alias("OBSERVACIONES"),
				trim(col("COD_ACTIVIDAD_ECONOMICA_SGSS")).alias("COD_ACTIVIDAD_ECONOMICA_SGSS"),
				trim(col("COD_AFP_ULTIMA")).alias("COD_AFP_ULTIMA"),
				col("FEC_MODIFICACION_MUERTE").alias("FEC_MODIFICACION_MUERTE"),
				trim(col("NID_NUIP")).alias("NID_NUIP"),
				trim(col("USUARIO_CREACION")).alias("USUARIO_CREACION"),
				trim(col("USUARIO_MODIFICACION")).alias("USUARIO_MODIFICACION"),
				col("FEC_MODIFICACION").alias("FEC_MODIFICACION"),
				trim(col("COD_OCUPACION_CIUO_88")).alias("COD_OCUPACION_CIUO_88"),
				trim(col("COD_ARP")).alias("COD_ARP"),
				col("FEC_EXPEDICION_DOCUMENTO").alias("FEC_EXPEDICION_DOCUMENTO"),
				trim(col("COD_CIUDAD_EXPEDICION_DOCU")).alias("COD_CIUDAD_EXPEDICION_DOCU"),
				trim(col("COD_DEPARTAMENTO_EXPE_DOCU")).alias("COD_DEPARTAMENTO_EXPE_DOCU"),
				trim(col("BLN_RESIDENTE_EXTERIOR")).alias("BLN_RESIDENTE_EXTERIOR"),
				col("FEC_INGRESO_AFP").alias("FEC_INGRESO_AFP"),
				trim(col("CONDICION_INVALIDEZ")).alias("CONDICION_INVALIDEZ"),
				trim(col("NACIONALIDAD")).alias("NACIONALIDAD"),
				col("NUM_CUENTA_AFP").alias("NUM_CUENTA_AFP"),
				col("VLR_SALARIO_BASE_AFP").alias("VLR_SALARIO_BASE_AFP"),
				col("ULT_PERIODO_COT_AFP").alias("ULT_PERIODO_COT_AFP"),
				trim(col("COD_COMENT_FEC_MUERTE")).alias("COD_COMENT_FEC_MUERTE"),
				trim(col("ORIGEN_CAMBIO")).alias("ORIGEN_CAMBIO")
            ),
            lambda df: df.dropDuplicates()
        ]
        },    
        "CGR_SUCURSALES_ALFA": {
            "partition_columns": [],
            "schema": "GENERAL",
            "transform": 
        [
                lambda df: df.select(
                trim(col("COD_SUCURSAL_ALFA")).alias("COD_SUCURSAL_ALFA"),
                trim(col("COD_ESTADO")).alias("COD_ESTADO"),
                trim(col("NOMBRE")).alias("NOMBRE"),
                trim(col("COD_SUCURSAL_VIDA")).alias("COD_SUCURSAL_VIDA"),
                trim(col("COD_AGRUPADORA")).alias("COD_AGRUPADORA"),
                trim(col("COD_DEPARTAMENTO")).alias("COD_DEPARTAMENTO"),
                trim(col("COD_CIUDAD")).alias("COD_CIUDAD"),
                trim(col("COD_COMPANIA")).alias("COD_COMPANIA"),
                trim(col("DIRECCION")).alias("DIRECCION"),
                trim(col("TELEFONO")).alias("TELEFONO")
                ),
                lambda df: df.dropDuplicates()
            ]
        },        
        "CGR_TIPOS_ENDOSOS": {
            "partition_columns": [],
            "schema": "GENERAL",
            "transform": 
        [
                lambda df: df.select(
                    col("COD_TIPO_ENDOSO").alias("COD_TIPO_ENDOSO"),
                    trim(col("NOMBRE_TIPO")).alias("NOMBRE_TIPO"),
                    trim(col("COD_TRANSACCION_POINT")).alias("COD_TRANSACCION_POINT")
                ),
                lambda df: df.dropDuplicates()
            ]
        },
        
        "RVA_RESERVA_MATEMATICA_TRANSIC": {
            "partition_columns": [],
            "schema": "GENERAL",
            "transform": 
        [
            lambda df: df.select(
				col("RAMO").cast("string").alias("RAMO"),
				col("NID_SINIESTRO_RENTA").cast("bigint").alias("NID_SINIESTRO_RENTA"),
				col("VLR_RESERVA_RV89").cast("decimal(14,2)").alias("VLR_RESERVA_RV89"),
				col("VLR_SALDO_RESERVA").cast("decimal(38,10)").alias("VLR_SALDO_RESERVA"),
				col("VLR_RESERVA_RV08").cast("decimal(14,2)").alias("VLR_RESERVA_RV08"),
				col("PCJ_VARIACION_RESERVA").cast("decimal(8,5)").alias("PCJ_VARIACION_RESERVA"),
				col("DIFERENCIA_PCJ").cast("decimal(8,5)").alias("DIFERENCIA_PCJ"),
				col("PCJ_DELTA").cast("decimal(13,10)").alias("PCJ_DELTA")
            ),
            lambda df: df.dropDuplicates()
        ]
        },        

        "GAR_TIPOS_ENDOSOS_ARP": {
            "partition_columns": [],
            "schema": "ARPPROD",
            "transform": 
        [
                lambda df: df.select(                
                    trim(col("COD_TIPO_ENDOSO_ARP")).alias("COD_TIPO_ENDOSO_ARP"),
                    trim(col("DESCRIPCION")).alias("DESCRIPCION"),
                    trim(col("ABREVIATURA")).alias("ABREVIATURA"),
                ),
                lambda df: df.dropDuplicates()
            ]
        },
    
         "HIST_DATOS_INVALIDEZ_MUERTE": {
            "partition_columns": ["FEC_REGISTRA_PARTITION"],
            "schema": "ARPPROD",
            "transform": 
        [
            lambda df: df.select(
				col("NID_HIST_DATOS_INV_MTE").alias("NID_HIST_DATOS_INV_MTE"),
				col("NID_SINIESTRO").alias("NID_SINIESTRO"),
				col("NID_AUDITORIA_MEDICA").alias("NID_AUDITORIA_MEDICA"),
				trim(col("COD_GRADO_INVALIDEZ")).alias("COD_GRADO_INVALIDEZ"),
				trim(col("BLN_INVALIDO_CON_AUXILIAR")).alias("BLN_INVALIDO_CON_AUXILIAR"),
				trim(col("COD_TIPO_ENTIDAD_CALIFICA")).alias("COD_TIPO_ENTIDAD_CALIFICA"),
				trim(col("COD_ESTADO_RESERVA")).alias("COD_ESTADO_RESERVA"),
				col("FEC_CALIFICA_ENTIDAD").alias("FEC_CALIFICA_ENTIDAD"),
				col("FEC_REGISTRA").alias("FEC_REGISTRA"),
				trim(col("USUARIO_REGISTRA")).alias("USUARIO_REGISTRA"),
				trim(col("BLN_ADMPENSIONES")).alias("BLN_ADMPENSIONES"),
				col("FEC_APROBACION_PENSION").alias("FEC_APROBACION_PENSION"),
				col("FEC_DICTAMEN_INVMTE").alias("FEC_DICTAMEN_INVMTE"),
				col("VLR_PENSION").alias("VLR_PENSION"),
                col("FEC_REGISTRA_PARTITION").alias("FEC_REGISTRA_PARTITION")
            ),
            lambda df: df.dropDuplicates()
        ]
        },    
         "PAR_AFILIACIONES": {
            "partition_columns": ["FEC_REGISTRO_PARTITION"],
            "schema": "ARPPROD",
            "transform": 
        [
            lambda df: df.select(
                col("NID_AFILIACION").alias("NID_AFILIACION"),
                col("NID_AFILIACION_POTENCIAL").alias("NID_AFILIACION_POTENCIAL"),
                col("COD_TIPO_IDENTIFICACION").alias("COD_TIPO_IDENTIFICACION"),
                col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
                col("NID_CONSECUTIVO").alias("NID_CONSECUTIVO"),
                col("FEC_INICIO_VIGENCIA").alias("FEC_INICIO_VIGENCIA"),
                col("FEC_RADICACION").alias("FEC_RADICACION"),
                col("PERIODO_ULTIMA_PROYECCION").alias("PERIODO_ULTIMA_PROYECCION"),
                col("PERIODO_ULTIMO_PAGO").alias("PERIODO_ULTIMO_PAGO"),
                col("FEC_REGISTRO").alias("FEC_REGISTRO"),
                col("COD_ESTADO").alias("COD_ESTADO"),
                col("COD_PROCESAMIENTO_AUTOLIQ").alias("COD_PROCESAMIENTO_AUTOLIQ"),
                col("COD_FORMA_PAGO").alias("COD_FORMA_PAGO"),
                col("COD_EXCEPCION").alias("COD_EXCEPCION"),
                col("COD_SUCURSAL_ALFA").alias("COD_SUCURSAL_ALFA"),
                col("BLN_INTERMEDIARIO").alias("BLN_INTERMEDIARIO"),
                col("NUM_SUCURSALES").alias("NUM_SUCURSALES"),
                col("NUM_CENTROS_TRABAJO").alias("NUM_CENTROS_TRABAJO"),
                col("COD_CLASE_AFILIACION").alias("COD_CLASE_AFILIACION"),
                col("COD_ARP_ANTERIOR").alias("COD_ARP_ANTERIOR"),
                col("BLN_EXTEMPORANEA").alias("BLN_EXTEMPORANEA"),
                col("BLN_ACUERDO_PAGO").alias("BLN_ACUERDO_PAGO"),
                col("BLN_PYME").alias("BLN_PYME"),
                col("FEC_FIN_VIGENCIA").alias("FEC_FIN_VIGENCIA"),
                col("PCJ_MAXIMO_INTERMEDIACION").alias("PCJ_MAXIMO_INTERMEDIACION"),
                col("BLN_CONTRATANTE").alias("BLN_CONTRATANTE"),
                col("NUM_FORMULARIO").alias("NUM_FORMULARIO"),
                col("COD_TIPO_APORTANTE").alias("COD_TIPO_APORTANTE"),
                col("COD_TIPO_VINCULADOR_LABORAL").alias("COD_TIPO_VINCULADOR_LABORAL"),
                col("USUARIO_REGISTRO").alias("USUARIO_REGISTRO"),
                col("USUARIO_MODIFICACION").alias("USUARIO_MODIFICACION"),
                col("FEC_MODIFICACION").alias("FEC_MODIFICACION"),
                col("USUARIO_ENCARGADO").alias("USUARIO_ENCARGADO"),
                col("FEC_ASIGNA_ENCARGADO").alias("FEC_ASIGNA_ENCARGADO"),
                col("BLN_OLCSA").alias("BLN_OLCSA"),
                col("BLN_EMPRESA_ESPECIAL").alias("BLN_EMPRESA_ESPECIAL"),
                col("COD_TIPO_CLIENTE").alias("COD_TIPO_CLIENTE"),
                col("COD_TIPO_DESAFILIACION").alias("COD_TIPO_DESAFILIACION"),
                col("COD_CLASE_APORTANTE").alias("COD_CLASE_APORTANTE"),
                col("COD_TIPO_CARNET").alias("COD_TIPO_CARNET"),
                col("BLN_ACTUALIZA_TIPO_CARNE").alias("BLN_ACTUALIZA_TIPO_CARNE"),
                col("NID_INTERMEDIARIO").alias("NID_INTERMEDIARIO"),
                col("NID_CANAL").alias("NID_CANAL"),
                col("PCJ_INTERMEDIACION").alias("PCJ_INTERMEDIACION"),
                col("ESTADO_SINCRONIZACION").alias("ESTADO_SINCRONIZACION"),
                col("SEDE_PRINCIPAL").alias("SEDE_PRINCIPAL"),
                col("ZONA").alias("ZONA"),
                col("FEC_REGISTRO_PARTITION").alias("FEC_REGISTRO_PARTITION")
            ),
            lambda df: df.dropDuplicates()
        ]
        },      
 
        "PAR_ENDOSOS": {
            "partition_columns": ["FEC_EXPEDICION_PARTITION"],
            "schema": "ARPPROD",
            "transform": 
        [
                lambda df: df.select(
                    col("NID_ENDOSO").alias("NID_ENDOSO"),
                    trim(col("COD_SUCURSAL_ALFA")).alias("COD_SUCURSAL_ALFA"),
                    col("NID_POLIZA").alias("NID_POLIZA"),
                    trim(col("MODULO")).alias("MODULO"),
                    col("FEC_EXPEDICION").alias("FEC_EXPEDICION"),
                    col("VLR_ASEGURADO").alias("VLR_ASEGURADO"),
                    trim(col("HORA_DESDE")).alias("HORA_DESDE"),
                    trim(col("HORA_HASTA")).alias("HORA_HASTA"),
                    col("FEC_INICIAL").alias("FEC_INICIAL"),
                    col("FEC_FINAL").alias("FEC_FINAL"),
                    trim(col("PERIODO")).alias("PERIODO"),
                    col("VLR_PRIMA").alias("VLR_PRIMA"),
                    col("VLR_GASTOS").alias("VLR_GASTOS"),
                    col("VLR_IVA").alias("VLR_IVA"),
                    col("PCJ_IVA").alias("PCJ_IVA"),
                    col("PCJ_GASTOS").alias("PCJ_GASTOS"),
                    col("VLR_TOTAL").alias("VLR_TOTAL"),
                    trim(col("OBJETO")).alias("OBJETO"),
                    col("FEC_ENVIO_POINT").alias("FEC_ENVIO_POINT"),
                    col("FEC_PROCESO_POINT").alias("FEC_PROCESO_POINT"),
                    trim(col("BLN_PROCESO")).alias("BLN_PROCESO"),
                    col("COD_TIPO_ENDOSO").alias("COD_TIPO_ENDOSO"),
                    col("COD_RAZON_MODIFICACION").alias("COD_RAZON_MODIFICACION"),
                    col("NUM_ENDOSO_POINT").alias("NUM_ENDOSO_POINT"),
                    trim(col("PERIODO_POINT")).alias("PERIODO_POINT"),
                    trim(col("COD_TIPO_OPERACION")).alias("COD_TIPO_OPERACION"),
                    col("NID_ENDOSO_REVERSION").alias("NID_ENDOSO_REVERSION"),
                    col("NID_ENDOSO_REVERSADO").alias("NID_ENDOSO_REVERSADO"),
                    col("VLR_TOTAL_RECAUDO").alias("VLR_TOTAL_RECAUDO"),
                    trim(col("BLN_PROCESADO_RECAUDO")).alias("BLN_PROCESADO_RECAUDO"),
                    trim(col("COD_TIPO_ENDOSO_ARP")).alias("COD_TIPO_ENDOSO_ARP"),
                    col("VLR_TOTAL_RECAUDO_POINT").alias("VLR_TOTAL_RECAUDO_POINT"),
                    col("FEC_EXPEDICION_PARTITION").alias("FEC_EXPEDICION_PARTITION")
                ),
                lambda df: df.dropDuplicates()
            ]
        },

        "PAR_POLIZAS": {
            "partition_columns": ["FEC_EXPEDICION_PARTITION"],
            "schema": "ARPPROD",
            "transform": 
        [
            lambda df: df.select(
                trim(col("COD_SUCURSAL_ALFA")).alias("COD_SUCURSAL_ALFA"),
                col("NID_POLIZA").alias("NID_POLIZA"),
                trim(col("MODULO")).alias("MODULO"),
                trim(col("COD_POLIZA_POINT")).alias("COD_POLIZA_POINT"),
                col("NUM_ENDOSO").alias("NUM_ENDOSO"),
                col("FEC_EXPEDICION").alias("FEC_EXPEDICION"),
                trim(col("HORA_DESDE")).alias("HORA_DESDE"),
                trim(col("HORA_HASTA")).alias("HORA_HASTA"),
                trim(col("COD_RAMO")).alias("COD_RAMO"),
                col("COD_COASEGURO").alias("COD_COASEGURO"),
                trim(col("COD_DEPARTAMENTO")).alias("COD_DEPARTAMENTO"),
                trim(col("COD_CIUDAD")).alias("COD_CIUDAD"),
                col("COD_AGENTE").alias("COD_AGENTE"),
                col("FEC_INICIAL_VIGENCIA").alias("FEC_INICIAL_VIGENCIA"),
                col("FEC_FINAL_VIGENCIA").alias("FEC_FINAL_VIGENCIA"),
                col("FEC_INGRESO_POINT").alias("FEC_INGRESO_POINT"),
                trim(col("PERIODO_ACTIVO")).alias("PERIODO_ACTIVO"),
                trim(col("BLN_VIGENTE")).alias("BLN_VIGENTE"),
                col("COD_PRODUCTO").alias("COD_PRODUCTO"),                                        
                col("FEC_EXPEDICION_PARTITION").alias("FEC_EXPEDICION_PARTITION")
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        
        "PAR_TRABAJADORES": {
            "partition_columns": ["FEC_INGRESO_ARP_PARTITION"],
            "schema": "ARPPROD",
            "transform": 
        [
            lambda df: df.select(
                col("NID_TRABAJADOR").alias("NID_TRABAJADOR"),
                col("NID_AFILIACION").alias("NID_AFILIACION"),
                col("NID_SUCURSAL_AFILIACION").alias("NID_SUCURSAL_AFILIACION"),
                col("NID_CENTRO_TRABAJO_SUCURSAL").alias("NID_CENTRO_TRABAJO_SUCURSAL"),
                trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
                col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
                col("FEC_INGRESO_ARP").alias("FEC_INGRESO_ARP"),
                col("FEC_RETIRO_ARP").alias("FEC_RETIRO_ARP"),
                col("VLR_SUELDO_BASICO").alias("VLR_SUELDO_BASICO"),
                col("VLR_IBC_INICIAL").alias("VLR_IBC_INICIAL"),
                col("FEC_ULTIMO_AUMENTO_SUELDO").alias("FEC_ULTIMO_AUMENTO_SUELDO"),
                trim(col("BLN_CARNETIZADO")).alias("BLN_CARNETIZADO"),
                col("VLR_IBC_ACTUAL").alias("VLR_IBC_ACTUAL"),
                col("FEC_REGISTRO").alias("FEC_REGISTRO"),
                trim(col("COD_PERIODICIDAD_PAGO")).alias("COD_PERIODICIDAD_PAGO"),
                trim(col("COD_TIPO_VINCULACION")).alias("COD_TIPO_VINCULACION"),
                trim(col("COD_TIPO_CONTRATO")).alias("COD_TIPO_CONTRATO"),
                trim(col("COD_AFP")).alias("COD_AFP"),
                trim(col("COD_ESTADO_TRABAJADOR")).alias("COD_ESTADO_TRABAJADOR"),
                trim(col("COD_EPS")).alias("COD_EPS"),
                trim(col("BLN_EXTEMPORANEA")).alias("BLN_EXTEMPORANEA"),
                trim(col("BLN_INDEPENDIENTE")).alias("BLN_INDEPENDIENTE"),
                col("FEC_EXPEDICION_CARNE").alias("FEC_EXPEDICION_CARNE"),
                trim(col("COD_TIPO_COTIZANTE")).alias("COD_TIPO_COTIZANTE"),
                trim(col("USUARIO_REGISTRO")).alias("USUARIO_REGISTRO"),
                trim(col("USUARIO_MODIFICACION")).alias("USUARIO_MODIFICACION"),
                col("FEC_MODIFICACION").alias("FEC_MODIFICACION"),
                col("FEC_INGRESO_EMPRESA").alias("FEC_INGRESO_EMPRESA"),
                col("FEC_RETIRO_EMPRESA").alias("FEC_RETIRO_EMPRESA"),
                trim(col("COD_OCUPACION")).alias("COD_OCUPACION"),
                trim(col("COD_TIPO_PENSIONADO")).alias("COD_TIPO_PENSIONADO"),
                trim(col("EXTRANJERO_COTIZANTE")).alias("EXTRANJERO_COTIZANTE"),
                trim(col("COLOMBIANO_RESIDENTE_EXTERIOR")).alias("COLOMBIANO_RESIDENTE_EXTERIOR"),
                trim(col("COD_DEPTO_UBIC_LABORAL")).alias("COD_DEPTO_UBIC_LABORAL"),
                trim(col("COD_MUNICIPIO_UBIC_LABORAL")).alias("COD_MUNICIPIO_UBIC_LABORAL"),
                trim(col("BLN_ENVIO_RUAF")).alias("BLN_ENVIO_RUAF"),
                col("FEC_ENVIO_RUAF").alias("FEC_ENVIO_RUAF"),
                trim(col("BLN_APROBO_RUAF")).alias("BLN_APROBO_RUAF"),
                col("FEC_APROBO_RUAF").alias("FEC_APROBO_RUAF"),
                trim(col("BLN_REPORTA_ESTADO")).alias("BLN_REPORTA_ESTADO"),
                col("FEC_REPORTA_ESTADO").alias("FEC_REPORTA_ESTADO"),
                trim(col("ORIGEN")).alias("ORIGEN"),
                trim(col("PERIODO_REPORTA_ESTADO")).alias("PERIODO_REPORTA_ESTADO"),
                trim(col("COD_SUBTIPO_COTIZANTE")).alias("COD_SUBTIPO_COTIZANTE"),
                col("NUM_IMPRESION_CARNE").alias("NUM_IMPRESION_CARNE"),
                col("ESTADO_SINCRONIZACION").alias("ESTADO_SINCRONIZACION"),
                col("FEC_INGRESO_ARP_PARTITION").alias("FEC_INGRESO_ARP_PARTITION")
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "SAR_SINIESTROS": {
            "partition_columns": ["FEC_SINIESTRO_PARTITION"],
            "schema": "ARPPROD",
            "transform": 
        [
            lambda df: df.select(
				col("NID_SINIESTRO").alias("NID_SINIESTRO"),
				col("FEC_AVISO").alias("FEC_AVISO"),
				col("FEC_SINIESTRO").alias("FEC_SINIESTRO"),
				trim(col("HORA_AVISO")).alias("HORA_AVISO"),
				trim(col("COD_CIUDAD_AVISO")).alias("COD_CIUDAD_AVISO"),
				trim(col("COD_DEPTO_AVISO")).alias("COD_DEPTO_AVISO"),
				trim(col("COD_PAIS_AVISO")).alias("COD_PAIS_AVISO"),
				trim(col("COD_ESTADO_SINIESTRO")).alias("COD_ESTADO_SINIESTRO"),
				trim(col("COD_TIPO_SINIESTRO")).alias("COD_TIPO_SINIESTRO"),
				trim(col("COD_TIPO_EVENTO")).alias("COD_TIPO_EVENTO"),
				trim(col("COD_ESTADO_RECLAMACION")).alias("COD_ESTADO_RECLAMACION"),
				trim(col("ATENDIDO_URGENCIA")).alias("ATENDIDO_URGENCIA"),
				trim(col("COD_SUCURSAL_ALFA_ENCARGADA")).alias("COD_SUCURSAL_ALFA_ENCARGADA"),
				trim(col("BLN_SOLICITA_COBERTURA")).alias("BLN_SOLICITA_COBERTURA"),
				trim(col("BLN_REQUIERE_CALIF_PCL")).alias("BLN_REQUIERE_CALIF_PCL"),
				trim(col("BLN_REPORTADO_FURAT")).alias("BLN_REPORTADO_FURAT"),
				trim(col("BLN_REPORTADO_FUREP")).alias("BLN_REPORTADO_FUREP"),
				trim(col("BLN_ASIGNADO")).alias("BLN_ASIGNADO"),
				trim(col("BLN_EXTEMPORANEO")).alias("BLN_EXTEMPORANEO"),
				col("FEC_REGISTRO").alias("FEC_REGISTRO"),
				trim(col("USUARIO_REGISTRA")).alias("USUARIO_REGISTRA"),
				col("FEC_RECLAMO").alias("FEC_RECLAMO"),
				trim(col("NID_SINIESTRO_POINT")).alias("NID_SINIESTRO_POINT"),
				col("FEC_AVISO_POINT").alias("FEC_AVISO_POINT"),
				col("FEC_OBJECION_POINT").alias("FEC_OBJECION_POINT"),
				col("FEC_MODIFICACION").alias("FEC_MODIFICACION"),
				trim(col("USUARIO_MODIFICA")).alias("USUARIO_MODIFICA"),
				trim(col("OBSERVACIONES_AVISO")).alias("OBSERVACIONES_AVISO"),
				col("PERIODO").alias("PERIODO"),
				trim(col("BLN_AVISO_EMPLEADOR")).alias("BLN_AVISO_EMPLEADOR"),
				col("NID_TRABAJADOR").alias("NID_TRABAJADOR"),
				col("FEC_DICTAMEN_INVMTE").alias("FEC_DICTAMEN_INVMTE"),
				col("FEC_APROBACION_INVMTE").alias("FEC_APROBACION_INVMTE"),
				trim(col("COD_GRADO_INVALIDEZ")).alias("COD_GRADO_INVALIDEZ"),
				trim(col("COD_ESTADO_RESERVA")).alias("COD_ESTADO_RESERVA"),
				trim(col("COD_ESTADO_CLINICO")).alias("COD_ESTADO_CLINICO"),
				trim(col("COD_TIPO_CAPTURA")).alias("COD_TIPO_CAPTURA"),
				trim(col("BLN_TIENE_COBERTURA")).alias("BLN_TIENE_COBERTURA"),
				col("FEC_ASIGNA_COBERTURA").alias("FEC_ASIGNA_COBERTURA"),
				trim(col("OBSERVACIONES_COBERTURA")).alias("OBSERVACIONES_COBERTURA"),
				trim(col("MOTIVO_CAMBIO_ESTADO_SINIESTRO")).alias("MOTIVO_CAMBIO_ESTADO_SINIESTRO"),
				trim(col("BLN_ADMPENSIONES")).alias("BLN_ADMPENSIONES"),
				col("FEC_ADMPENSIONES").alias("FEC_ADMPENSIONES"),
				trim(col("COBRAR_OTRA_ARP")).alias("COBRAR_OTRA_ARP"),
				trim(col("PROCESO_JURIDICO")).alias("PROCESO_JURIDICO"),
				trim(col("RESULTADO_EJECUCION_CALCULO")).alias("RESULTADO_EJECUCION_CALCULO"),
				trim(col("COD_FACTOR_SEGURIDAD")).alias("COD_FACTOR_SEGURIDAD"),
                col("FEC_SINIESTRO_PARTITION").alias("FEC_SINIESTRO_PARTITION")
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
        
        if schema in DB_SCHEMA:
        
            logger.info(f"Transform data from table : {table}")
            
            # Load the table data as a DataFrame
            table_name = f"{DB_NAME}_{schema}_{table}"
            logger.info(f"Transform data from table : {table_name}")
            df_from_bronce = glueContext.create_dynamic_frame.from_catalog(
                database=f"{DB_CATALOG_GLUE}",
                table_name=table_name
            ).toDF()
            
            #Initialize of df to transform
            df_to_silver = df_from_bronce
            
            # Apply each transformation in sequence
            for transform_function in transform_functions:
                df_to_silver = transform_function(df_to_silver)
            
            
            # Save the transformed DataFrame to S3
            frame_save = DynamicFrame.fromDF(df_to_silver, glueContext, "frame_save_dynamic")
            
            
            s3_prefix = f"{DB_NAME}/{schema}/SILVER_{table}/"
                
            s3_path = f"s3://{BUCKET_NAME}/{s3_prefix}"
                
            logger.info(f"Truncate & Saving data to: {s3_path}")
            
            extract_helper.delete_s3_folder(BUCKET_NAME, s3_prefix)
            
            extract_helper.save_files_s3(frame_save, s3_path, partition_columns)

except Exception as e:
    logger.error(f"An error occurred: {e}")
    