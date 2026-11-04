
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
        },
        "SAR_AUDITORIA_MEDICA": {
            "partition_columns": ["FEC_REGISTRA_PARTITION"],
            "schema": "ARPPROD",
            "transform": 
        [
            lambda df: df.select(
				col("NID_SINIESTRO").alias("NID_SINIESTRO"),
				col("NID_AUDITORIA_MEDICA").alias("NID_AUDITORIA_MEDICA"),
				trim(col("COD_CONCEPTO_MEDICO_AM")).alias("COD_CONCEPTO_MEDICO_AM"),
				trim(col("USUARIO_REGISTRA")).alias("USUARIO_REGISTRA"),
				col("FEC_REGISTRA").alias("FEC_REGISTRA"),
				trim(col("USUARIO_MODIFICA")).alias("USUARIO_MODIFICA"),
				col("FEC_MODIFICA").alias("FEC_MODIFICA"),
				col("PCJ_RETENCION").alias("PCJ_RETENCION"),
				col("PCJ_PCL_PROYECTADA").alias("PCJ_PCL_PROYECTADA"),
				trim(col("COD_DEPARTAMENTO")).alias("COD_DEPARTAMENTO"),
				trim(col("COD_PAIS")).alias("COD_PAIS"),
				trim(col("BLN_JUNTA_NACIONAL")).alias("BLN_JUNTA_NACIONAL"),
				col("FEC_ENVIO").alias("FEC_ENVIO"),
				trim(col("COD_TIPO_CALIFICACION_PCL")).alias("COD_TIPO_CALIFICACION_PCL"),
				trim(col("BLN_JUNTA_ALFA")).alias("BLN_JUNTA_ALFA"),
				trim(col("BLN_JUNTA_REGIONAL")).alias("BLN_JUNTA_REGIONAL"),
				col("FEC_DICTAMEN_PCL").alias("FEC_DICTAMEN_PCL"),
				trim(col("COD_TIPO_ENTIDAD_CALIFICA_PCL")).alias("COD_TIPO_ENTIDAD_CALIFICA_PCL"),
				col("FEC_DICTAMEN_PCL_FINAL").alias("FEC_DICTAMEN_PCL_FINAL"),
				trim(col("BLN_INTER_RESP_APELA")).alias("BLN_INTER_RESP_APELA"),
				trim(col("COD_INTER_QUIEN_APELA")).alias("COD_INTER_QUIEN_APELA"),
				trim(col("COD_INTER_REMITIDO_APELA")).alias("COD_INTER_REMITIDO_APELA"),
				col("FEC_INTER_PAG_HONOR").alias("FEC_INTER_PAG_HONOR"),
				col("FEC_INTER_ENV_APELA").alias("FEC_INTER_ENV_APELA"),
				trim(col("BLN_REGI_RESP_APELA")).alias("BLN_REGI_RESP_APELA"),
				trim(col("COD_REGI_QUIEN_APELA")).alias("COD_REGI_QUIEN_APELA"),
				trim(col("COD_REGI_REMITIDO_APELA")).alias("COD_REGI_REMITIDO_APELA"),
				col("FEC_REGI_PAG_HONOR").alias("FEC_REGI_PAG_HONOR"),
				col("FEC_REGI_ENV_APELA").alias("FEC_REGI_ENV_APELA"),
                col("FEC_REGISTRA_PARTITION").alias("FEC_REGISTRA_PARTITION")
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "IND_DATOS_RESERVAS_SINIESTROS": {
            "partition_columns": ["FEC_REGISTRA_PARTITION"],
            "schema": "ARPPROD",
            "transform": 
        [
            lambda df: df.select(
				col("NID_SINIESTRO").alias("NID_SINIESTRO"),
				col("VRL_IBL_MANUAL").alias("VRL_IBL_MANUAL"),
				col("FEC_IBL_MANUAL").alias("FEC_IBL_MANUAL"),
				trim(col("USUARIO_REGISTRA")).alias("USUARIO_REGISTRA"),
				col("FEC_REGISTRA").alias("FEC_REGISTRA"),
				trim(col("USUARIO_MODIFICA")).alias("USUARIO_MODIFICA"),
				col("FEC_MODIFICA").alias("FEC_MODIFICA"),
				col("NID_GRUPO_IBL").alias("NID_GRUPO_IBL"),
                col("FEC_REGISTRA_PARTITION").alias("FEC_REGISTRA_PARTITION")
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "SAR_BENEFICIARIOS_SINIESTROS": {
            "partition_columns": ["FEC_REGISTRA_PARTITION"],
            "schema": "ARPPROD",
            "transform": 
        [
            lambda df: df.select(
				col("NID_BENEFICIARIO").alias("NID_BENEFICIARIO"),
				col("NID_SINIESTRO").alias("NID_SINIESTRO"),
				trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
				col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
				trim(col("COD_ESTADO_PERSONA")).alias("COD_ESTADO_PERSONA"),
				trim(col("COD_PARENTESCO")).alias("COD_PARENTESCO"),
				trim(col("BLN_CON_DERECHO")).alias("BLN_CON_DERECHO"),
				col("FEC_PERDIDA_DERECHO").alias("FEC_PERDIDA_DERECHO"),
				trim(col("BLN_CERTIFICADO_ESCOLARIDAD")).alias("BLN_CERTIFICADO_ESCOLARIDAD"),
				col("PCJ_PENSION").alias("PCJ_PENSION"),
				col("CANT_HIJOS").alias("CANT_HIJOS"),
				col("FEC_REGISTRA").alias("FEC_REGISTRA"),
				trim(col("USUARIO_REGISTRA")).alias("USUARIO_REGISTRA"),
				col("FEC_MODIFICA").alias("FEC_MODIFICA"),
				trim(col("USUARIO_MODIFICA")).alias("USUARIO_MODIFICA"),
                col("FEC_REGISTRA_PARTITION").alias("FEC_REGISTRA_PARTITION")
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "SAR_RESERVA_PCJ_AMORTIZACION": {
            "partition_columns": [],
            "schema": "ARPPROD",
            "transform": 
        [
            lambda df: df.select(
				col("NID_RESERVA_PCJ_AMORTIZACION").alias("NID_RESERVA_PCJ_AMORTIZACION"),
				col("PERIODO").alias("PERIODO"),
				col("PCJ_RESERVA_CONTABLE").alias("PCJ_RESERVA_CONTABLE"),
				col("PCJ_DELTA").alias("PCJ_DELTA"),
				col("NID_SINIESTRO").alias("NID_SINIESTRO"),
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "HIST_SAR_PCL_PENSION": {
            "partition_columns": ["FEC_REGISTRA_PARTITION"],
            "schema": "ARPPROD",
            "transform": 
        [
            lambda df: df.select(
				col("NID_HIST_PCL_PENSION").alias("NID_HIST_PCL_PENSION"),
				col("NID_SINIESTRO").alias("NID_SINIESTRO"),
				col("NID_AUDITORIA_MEDICA").alias("NID_AUDITORIA_MEDICA"),
				col("PCJ_PCL").alias("PCJ_PCL"),
				col("FEC_DICTAMEN").alias("FEC_DICTAMEN"),
				trim(col("COD_TIPO_ENTIDAD_CALIFICA")).alias("COD_TIPO_ENTIDAD_CALIFICA"),
				trim(col("USUARIO_REGISTRA")).alias("USUARIO_REGISTRA"),
				col("FEC_REGISTRA").alias("FEC_REGISTRA"),
                col("FEC_REGISTRA_PARTITION").alias("FEC_REGISTRA_PARTITION")
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
        "SAR_PAGOS_COBERTURAS": {
            "partition_columns": ["FEC_MODIFICACION_PARTITION"],
            "schema": "ARPPROD",
            "transform": 
        [
            lambda df: df.select(
				col("NID_PAGO").alias("NID_PAGO"),
				col("FEC_PAGO").alias("FEC_PAGO"),
				col("NUM_DIAS_IT").alias("NUM_DIAS_IT"),
				col("VLR_PAGO").alias("VLR_PAGO"),
				col("VLR_PAGO_VP").alias("VLR_PAGO_VP"),
				col("FEC_CALCULO_VP").alias("FEC_CALCULO_VP"),
				col("VLR_SALARIO_BASE").alias("VLR_SALARIO_BASE"),
				trim(col("COD_COBERTURA_POINT")).alias("COD_COBERTURA_POINT"),
				col("FEC_PAGO_TESORERIA").alias("FEC_PAGO_TESORERIA"),
				col("FEC_GENERACION_CIERRE").alias("FEC_GENERACION_CIERRE"),
				trim(col("ID_QUIEN_ELABORA")).alias("ID_QUIEN_ELABORA"),
				trim(col("BLN_PROCESADO_RESERVAS")).alias("BLN_PROCESADO_RESERVAS"),
				col("PCJ_PCL_ACTUAL").alias("PCJ_PCL_ACTUAL"),
				col("NID_SINIESTRO").alias("NID_SINIESTRO"),
				col("COD_COBERTURA").alias("COD_COBERTURA"),
				col("FEC_MODIFICACION").alias("FEC_MODIFICACION"),
				trim(col("USUARIO_MODIFICA")).alias("USUARIO_MODIFICA"),
				col("FEC_REGISTRA").alias("FEC_REGISTRA"),
				trim(col("COD_CONCEPTO")).alias("COD_CONCEPTO"),
				trim(col("COD_FORMA_PAGO")).alias("COD_FORMA_PAGO"),
				trim(col("COD_SUCURSAL_GIRO")).alias("COD_SUCURSAL_GIRO"),
				trim(col("COD_TIPO_IDENTIFICACION_BEN")).alias("COD_TIPO_IDENTIFICACION_BEN"),
				col("NID_PERIODO_SOLICITUD_GIRO").alias("NID_PERIODO_SOLICITUD_GIRO"),
				col("NID_NUMERO_IDENTIFI_BEN").alias("NID_NUMERO_IDENTIFI_BEN"),
				trim(col("NUMERO_FACTURA")).alias("NUMERO_FACTURA"),
				col("VLR_ICA").alias("VLR_ICA"),
				col("FEC_MODIFICACION_PARTITION").alias("FEC_MODIFICACION_PARTITION")
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "PAR_AUTOLIQUIDACION_TRABAJADOR": {
            "partition_columns": ["FEC_REGISTRO_PARTITION"],
            "schema": "ARPPROD",
            "transform": 
        [
            lambda df: df.select(
                col("NID_DETALLE_AUTOLIQUIDACION").alias("NID_DETALLE_AUTOLIQUIDACION"), 
                col("NID_AUTOLIQUIDACION").alias("NID_AUTOLIQUIDACION"),          
				col("NID_TRABAJADOR").alias("NID_TRABAJADOR"),               
				col("NUM_DIAS_COTIZADOS").alias("NUM_DIAS_COTIZADOS"),           
				col("VLR_SALARIO_BASICO").alias("VLR_SALARIO_BASICO"),           
				col("VLR_NETO_NOVEDADES").alias("VLR_NETO_NOVEDADES"),           
				trim(col("COD_AFP")).alias("COD_AFP"),                      
				trim(col("COD_EPS")).alias("COD_EPS"),                      
				col("VLR_TARIFA_ORIGINAL").alias("VLR_TARIFA_ORIGINAL"),          
				col("VLR_TARIFA_ASIGNADA").alias("VLR_TARIFA_ASIGNADA"),          
				col("VLR_COTIZ_OBLIG_ORIGINAL").alias("VLR_COTIZ_OBLIG_ORIGINAL"),     
				col("VLR_COTIZ_OBLIG_CALC_AUTOLIQ").alias("VLR_COTIZ_OBLIG_CALC_AUTOLIQ"), 
				col("VLR_COTIZ_OBLIG_CALC_ASIGNA").alias("VLR_COTIZ_OBLIG_CALC_ASIGNA"),  
				col("VLR_INGRESO_BASE_COTIZ_ORIG").alias("VLR_INGRESO_BASE_COTIZ_ORIG"),  
				col("VLR_INGRESO_BASE_COTIZ_ASIG").alias("VLR_INGRESO_BASE_COTIZ_ASIG"),  
				col("VLR_INGRESO_BASE_COTIZ_REF").alias("VLR_INGRESO_BASE_COTIZ_REF"),   
				col("FEC_REGISTRO").alias("FEC_REGISTRO"),                 
				trim(col("USUARIO_REGISTRO")).alias("USUARIO_REGISTRO"),             
				col("FEC_MODIFICACION").alias("FEC_MODIFICACION"),             
				trim(col("USUARIO_MODIFICACION")).alias("USUARIO_MODIFICACION"),         
				col("DIAS_SLN").alias("DIAS_SLN"),
				col("FEC_REGISTRO_PARTITION").alias("FEC_REGISTRO_PARTITION")
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "PAR_AUTOLIQUIDACION_SUCURSAL": {
            "partition_columns": ["FEC_REGISTRO_PARTITION"],
            "schema": "ARPPROD",
            "transform": 
        [
            lambda df: df.select(
				col("NID_AUTOLIQUIDACION").alias("NID_AUTOLIQUIDACION"),
				col("NID_AFILIACION").alias("NID_AFILIACION"),
				col("NID_SUCURSAL_AFILIACION").alias("NID_SUCURSAL_AFILIACION"),
				col("FEC_REGISTRO").alias("FEC_REGISTRO"),
				col("NUM_RADICACION").alias("NUM_RADICACION"),
				col("FEC_RADICACION").alias("FEC_RADICACION"),
				col("PERIODO_COTIZACION").alias("PERIODO_COTIZACION"),
				col("NUM_TRABAJADORES").alias("NUM_TRABAJADORES"),
				trim(col("BLN_CORRECCION")).alias("BLN_CORRECCION"),
				col("PERIODO_CORRECCION").alias("PERIODO_CORRECCION"),
				col("NUM_PLANILLA").alias("NUM_PLANILLA"),
				col("NUM_AUTORIZACION").alias("NUM_AUTORIZACION"),
				col("FEC_AUTORIZACION").alias("FEC_AUTORIZACION"),
				trim(col("BLN_PROCESANDO")).alias("BLN_PROCESANDO"),
				col("VLR_SALDO_RECAUDO").alias("VLR_SALDO_RECAUDO"),
				trim(col("COD_ORIGEN_AUTOLIQUIDACION")).alias("COD_ORIGEN_AUTOLIQUIDACION"),
				col("VLR_TOT_APORT_PERI_DECLA_AUTO").alias("VLR_TOT_APORT_PERI_DECLA_AUTO"),
				col("VLR_INCAPACIDADES_PAGADA_AUTO").alias("VLR_INCAPACIDADES_PAGADA_AUTO"),
				col("VLR_COT_PAG_OTROS_RIESGO_AUTO").alias("VLR_COT_PAG_OTROS_RIESGO_AUTO"),
				col("VLR_APORTES_NETOS_AUTO").alias("VLR_APORTES_NETOS_AUTO"),
				col("VLR_INTERESES_MORA_AUTO").alias("VLR_INTERESES_MORA_AUTO"),
				col("VLR_APORT_INTERESES_MORA_AUTO").alias("VLR_APORT_INTERESES_MORA_AUTO"),
				col("VLR_SALDO_PERIODOS_ANTER_AUTO").alias("VLR_SALDO_PERIODOS_ANTER_AUTO"),
				col("VLR_TOTAL_A_PAGAR_AUTO").alias("VLR_TOTAL_A_PAGAR_AUTO"),
				col("VLR_TOT_APORT_PERI_DECLA_ASIG").alias("VLR_TOT_APORT_PERI_DECLA_ASIG"),
				col("VLR_INCAPACIDADES_PAGADA_ASIG").alias("VLR_INCAPACIDADES_PAGADA_ASIG"),
				col("VLR_COT_PAG_OTROS_RIESGO_ASIG").alias("VLR_COT_PAG_OTROS_RIESGO_ASIG"),
				col("VLR_APORTES_NETOS_ASIG").alias("VLR_APORTES_NETOS_ASIG"),
				col("VLR_INTERESES_MORA_ASIG").alias("VLR_INTERESES_MORA_ASIG"),
				col("VLR_APORT_INTERESES_MORA_ASIG").alias("VLR_APORT_INTERESES_MORA_ASIG"),
				col("VLR_SALDO_PERIODOS_ANTER_ASIG").alias("VLR_SALDO_PERIODOS_ANTER_ASIG"),
				col("VLR_TOTAL_A_PAGAR_ASIG").alias("VLR_TOTAL_A_PAGAR_ASIG"),
				col("VLR_TOT_APORT_PERI_DECLA_ORIG").alias("VLR_TOT_APORT_PERI_DECLA_ORIG"),
				col("VLR_INCAPACIDADES_PAGADA_ORIG").alias("VLR_INCAPACIDADES_PAGADA_ORIG"),
				col("VLR_COT_PAG_OTROS_RIESGO_ORIG").alias("VLR_COT_PAG_OTROS_RIESGO_ORIG"),
				col("VLR_APORTES_NETOS_ORIG").alias("VLR_APORTES_NETOS_ORIG"),
				col("VLR_INTERESES_MORA_ORIG").alias("VLR_INTERESES_MORA_ORIG"),
				col("VLR_APORT_INTERESES_MORA_ORIG").alias("VLR_APORT_INTERESES_MORA_ORIG"),
				col("VLR_SALDO_PERIODOS_ANTER_ORIG").alias("VLR_SALDO_PERIODOS_ANTER_ORIG"),
				col("VLR_TOTAL_A_PAGAR_ORIG").alias("VLR_TOTAL_A_PAGAR_ORIG"),
				trim(col("BLN_AUTOLIQUIDACION_VALIDA")).alias("BLN_AUTOLIQUIDACION_VALIDA"),
				col("PERIODO_PRODUCCION").alias("PERIODO_PRODUCCION"),
				trim(col("BLN_AVANZADO_PERIODO_PRODUCC")).alias("BLN_AVANZADO_PERIODO_PRODUCC"),
				trim(col("BLN_AUTOLIQUIDACION_DEFINITIVA")).alias("BLN_AUTOLIQUIDACION_DEFINITIVA"),
				col("FEC_AUTOLIQUIDACION_DEFINITIVA").alias("FEC_AUTOLIQUIDACION_DEFINITIVA"),
				col("COD_OPERADOR").alias("COD_OPERADOR"),
				trim(col("USUARIO_REGISTRA")).alias("USUARIO_REGISTRA"),
				trim(col("USUARIO_MODIFICA")).alias("USUARIO_MODIFICA"),
				col("FEC_MODIFICACION").alias("FEC_MODIFICACION"),
				col("FEC_PAGO").alias("FEC_PAGO"),
				col("FEC_CORRECCION").alias("FEC_CORRECCION"),
				trim(col("COD_TIPO_PLANILLA")).alias("COD_TIPO_PLANILLA"),
				trim(col("COD_MODALIDAD_PLANILLA")).alias("COD_MODALIDAD_PLANILLA"),
				col("NUM_PLANILLA_CORRECCION").alias("NUM_PLANILLA_CORRECCION"),
				col("VLR_TOTAL_IBC_ORIG").alias("VLR_TOTAL_IBC_ORIG"),
				col("VLR_TOTAL_IBC_ASIG").alias("VLR_TOTAL_IBC_ASIG"),
				col("VLR_TOTAL_DESCUENTOS_ORIG").alias("VLR_TOTAL_DESCUENTOS_ORIG"),
				col("VLR_TOTAL_DESCUENTOS_ASIG").alias("VLR_TOTAL_DESCUENTOS_ASIG"),
				col("FEC_REGISTRO_PARTITION").alias("FEC_REGISTRO_PARTITION")
            ),
            lambda df: df.dropDuplicates()
        ]
        },
		"SAR_DATOS_INVALIDEZ_MUERTE": {
            "partition_columns": ["FEC_REGISTRA_PARTITION"],
            "schema": "ARPPROD",
            "transform": 
        [
            lambda df: df.select(
				col("NID_SINIESTRO").alias("NID_SINIESTRO"),
				col("NID_AUDITORIA_MEDICA").alias("NID_AUDITORIA_MEDICA"),
				trim(col("COD_GRADO_INVALIDEZ")).alias("COD_GRADO_INVALIDEZ"),
				trim(col("BLN_INVALIDO_CON_AUXILIAR")).alias("BLN_INVALIDO_CON_AUXILIAR"),
				trim(col("COD_TIPO_ENTIDAD_CALIFICA")).alias("COD_TIPO_ENTIDAD_CALIFICA"),
				trim(col("COD_ESTADO_RESERVA")).alias("COD_ESTADO_RESERVA"),
				col("FEC_CALIFICA_ENTIDAD").alias("FEC_CALIFICA_ENTIDAD"),
				col("FEC_DICTAMEN_INVMTE").alias("FEC_DICTAMEN_INVMTE"),
				col("FEC_APROBACION_PENSION").alias("FEC_APROBACION_PENSION"),
				col("FEC_REGISTRA").alias("FEC_REGISTRA"),
				trim(col("USUARIO_REGISTRA")).alias("USUARIO_REGISTRA"),
				col("FEC_MODIFICA").alias("FEC_MODIFICA"),
				trim(col("USUARIO_MODIFICA")).alias("USUARIO_MODIFICA"),
				col("VLR_PENSION").alias("VLR_PENSION"),
				col("FEC_REGISTRA_PARTITION").alias("FEC_REGISTRA_PARTITION")
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "SAR_CALCULOS_ACTUARIALES": {
            "partition_columns": ["FEC_CALCULO_PARTITION"],
            "schema": "ARPPROD",
            "transform": 
        [
            lambda df: df.select(
				col("NID_SINIESTRO"              ).alias("NID_SINIESTRO"),              
				col("NID_CALCULO"                ).alias("NID_CALCULO"),                
				col("VLR_PENSION"                ).alias("VLR_PENSION"),                
				col("PCJ_PENSION"                ).alias("PCJ_PENSION"),                
				col("VLR_RESERVA_CALCULO"        ).alias("VLR_RESERVA_CALCULO"),        
				col("VLR_AUXILIO_FUNERARIO"      ).alias("VLR_AUXILIO_FUNERARIO"),      
				col("NUM_MESADAS_ATRASADAS"      ).alias("NUM_MESADAS_ATRASADAS"),      
				col("VLR_MESADAS_ATRASADAS"      ).alias("VLR_MESADAS_ATRASADAS"),      
				col("VLR_SALARIO_BASE"           ).alias("VLR_SALARIO_BASE"),           
				col("NUM_DIAS_TRABAJADOS"        ).alias("NUM_DIAS_TRABAJADOS"),        
				col("FEC_CALCULO"                ).alias("FEC_CALCULO"),                
				col("COD_ORIGEN"                 ).alias("COD_ORIGEN"),                 
				trim(col("COD_GRADO_INVALIDEZ"        )).alias("COD_GRADO_INVALIDEZ"),        
				trim(col("COD_GRADO_INVALIDEZ_INICIAL")).alias("COD_GRADO_INVALIDEZ_INICIAL"),
				trim(col("FEC_NACIMIENTO_TRABAJADOR"  )).alias("FEC_NACIMIENTO_TRABAJADOR"),  
				col("NUM_EDAD_TRABAJADOR"        ).alias("NUM_EDAD_TRABAJADOR"),        
				col("GENERO_TRABAJADOR"          ).alias("GENERO_TRABAJADOR"),          
				trim(col("COD_ESTADO_TRABAJADOR"      )).alias("COD_ESTADO_TRABAJADOR"),      
				trim(col("COD_ESTADO_CIVIL"           )).alias("COD_ESTADO_CIVIL"),           
				trim(col("SOBREVIVIENTES"             )).alias("SOBREVIVIENTES"),             
				trim(col("BENEFICIARIO_UNO"           )).alias("BENEFICIARIO_UNO"),           
				col("FEC_NACIMIENTO_BEN_UNO"     ).alias("FEC_NACIMIENTO_BEN_UNO"),     
				col("NUM_EDAD_BENEFICIARIO_UNO"  ).alias("NUM_EDAD_BENEFICIARIO_UNO"),  
				col("GENERO_BENEFICIARIO_UNO"    ).alias("GENERO_BENEFICIARIO_UNO"),    
				trim(col("COD_ESTADO_BENEFICIARIO_UNO")).alias("COD_ESTADO_BENEFICIARIO_UNO"),
				trim(col("COD_TIPO_RESERVA_BEN_UNO"   )).alias("COD_TIPO_RESERVA_BEN_UNO"),   
				trim(col("BENEFICIARIO_DOS"           )).alias("BENEFICIARIO_DOS"),           
				col("FEC_NACIMIENTO_BEN_DOS"     ).alias("FEC_NACIMIENTO_BEN_DOS"),     
				col("NUM_EDAD_BENEFICIARIO_DOS"  ).alias("NUM_EDAD_BENEFICIARIO_DOS"),  
				col("GENERO_BENEFICIARIO_DOS"    ).alias("GENERO_BENEFICIARIO_DOS"),    
				col("COD_ESTADO_BENEFICIARIO_DOS").alias("COD_ESTADO_BENEFICIARIO_DOS"),
				trim(col("COD_TIPO_RESERVA_BEN_DOS"   )).alias("COD_TIPO_RESERVA_BEN_DOS"),   
				trim(col("NUM_MESADAS"                )).alias("NUM_MESADAS"),                
				col("PCJ_TI"                     ).alias("PCJ_TI"),                     
				col("PCJ_TK"                     ).alias("PCJ_TK"),                     
				col("PCJ_ADMON"                  ).alias("PCJ_ADMON"),                  
				col("SAL_MINIMO_FEC_CALC"        ).alias("SAL_MINIMO_FEC_CALC"),        
				trim(col("USUARIO_REGISTRA"           )).alias("USUARIO_REGISTRA"),           
				col("FEC_REGISTRA"               ).alias("FEC_REGISTRA"),               
				col("VLR_SUMA_MESADAS"           ).alias("VLR_SUMA_MESADAS"),           
				col("VLR_SUMA_AUX_FUN"           ).alias("VLR_SUMA_AUX_FUN"),           
				col("VLR_SUMA_GASTOS"            ).alias("VLR_SUMA_GASTOS"),            
				trim(col("COD_TIPO_ID_BEN_UNO"        )).alias("COD_TIPO_ID_BEN_UNO"),        
				col("NID_NUMERO_ID_BEN_UNO"      ).alias("NID_NUMERO_ID_BEN_UNO"),      
				trim(col("COD_TIPO_ID_BEN_DOS"        )).alias("COD_TIPO_ID_BEN_DOS"),        
				col("NID_NUMERO_ID_BEN_DOS"      ).alias("NID_NUMERO_ID_BEN_DOS"),      
				col("FEC_MUERTE_AFILIADO"        ).alias("FEC_MUERTE_AFILIADO"),        
				col("VLR_RESERVA_CONTABLE"       ).alias("VLR_RESERVA_CONTABLE"),       
				col("BLN_CASO_TRANSICION"        ).alias("BLN_CASO_TRANSICION"),        
				col("MES_DIFERIDO_TRANSICION"    ).alias("MES_DIFERIDO_TRANSICION"),    
				col("PCJ_APLICADO_TRANSICION"    ).alias("PCJ_APLICADO_TRANSICION"),    
				col("VLR_EXCESO_SMMLV_IPC"       ).alias("VLR_EXCESO_SMMLV_IPC"),       
				col("PCJ_TASA_ADMINISTRACION"    ).alias("PCJ_TASA_ADMINISTRACION"),    
				col("TAM_MESADAS_ATRASADAS"      ).alias("TAM_MESADAS_ATRASADAS"),      
				col("NUM_CALCULO_CA"             ).alias("NUM_CALCULO_CA"),             
				col("PCJ_DESLIZAMIENTO_SM"       ).alias("PCJ_DESLIZAMIENTO_SM"),       
				col("PCJ_TMR_EQUIVALENTE"        ).alias("PCJ_TMR_EQUIVALENTE"),        
				col("PCJ_FACTOR_SEGURIDAD"       ).alias("PCJ_FACTOR_SEGURIDAD"),       
				col("VLR_GASTOS_EXP_FIJOS"       ).alias("VLR_GASTOS_EXP_FIJOS"),       
				col("VLR_GASTOS_ADM_FIJOS"       ).alias("VLR_GASTOS_ADM_FIJOS"),       
				col("PCJ_GASTOS_EXP"             ).alias("PCJ_GASTOS_EXP"),
				col("FEC_CALCULO_PARTITION").alias("FEC_CALCULO_PARTITION")
            ),
            lambda df: df.dropDuplicates()
        ]
        }
    }
    
    ###################################################################
    #################### save stage silver ############################
    ###################################################################
    
    
    for table, config in tables_case_use.items():
        
        logger.info(f"Transform data from table : {table}")
        
        partition_columns = config["partition_columns"]
        transform_functions = config["transform"]
        schema = config["schema"]

        # Load the table data as a DataFrame
        table_name = f"{DB_NAME}_{schema}_{table}"
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
        
        
        s3_prefix = f"{DB_NAME}/{DB_SCHEMA}/SILVER_{table}/"
            
        s3_path = f"s3://{BUCKET_NAME}/{s3_prefix}"
            
        logger.info(f"Truncate & Saving data to: {s3_path}")
        
        extract_helper.delete_s3_folder(BUCKET_NAME, s3_prefix)
        
        extract_helper.save_files_s3(frame_save, s3_path, partition_columns)

except Exception as e:
    logger.error(f"An error occurred: {e}")
    