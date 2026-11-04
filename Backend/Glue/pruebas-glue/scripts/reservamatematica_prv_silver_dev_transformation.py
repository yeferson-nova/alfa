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
       "PRV_RENTAS": {
        "partition_columns": ["FEC_EXPEDICION_PARTITION"],
        "schema": "PRV",
        "transform": [
            lambda df: df.select(
				col("NID_RENTAS").alias("NID_RENTAS"),
				col("FEC_EXPEDICION").alias("FEC_EXPEDICION"),
				col("FEC_VIGENCIA_INICIAL").alias("FEC_VIGENCIA_INICIAL"),
				col("VLR_SALARIO_BASE").alias("VLR_SALARIO_BASE"),
				col("VLR_PRIMA_UNICA").alias("VLR_PRIMA_UNICA"),
				col("VLR_CUENTA_INDIVIDUAL").alias("VLR_CUENTA_INDIVIDUAL"),
				col("VLR_BONO_PENSIONAL").alias("VLR_BONO_PENSIONAL"),
				col("VLR_PENSION_INICIAL").alias("VLR_PENSION_INICIAL"),
				col("VLR_PENSION_ACTUAL").alias("VLR_PENSION_ACTUAL"),
				col("FEC_ULT_INCREMENTO_PENSION").alias("FEC_ULT_INCREMENTO_PENSION"),
				col("PCJ_ULT_INCREMENTO_PENSION").alias("PCJ_ULT_INCREMENTO_PENSION"),
				col("VLR_INDEMNIZACION_SEGURO").alias("VLR_INDEMNIZACION_SEGURO"),
				col("VLR_OTROS_APORTES").alias("VLR_OTROS_APORTES"),
				col("FEC_PAGO").alias("FEC_PAGO"),
				trim(col("LUGAR_PAGO")).alias("LUGAR_PAGO"),
				trim(col("SIMBOLO_POINT")).alias("SIMBOLO_POINT"),
				trim(col("RAMO_POINT")).alias("RAMO_POINT"),
				trim(col("MODULO_POINT")).alias("MODULO_POINT"),
				trim(col("ENDOSO_POINT")).alias("ENDOSO_POINT"),
				trim(col("EDO")).alias("EDO"),
				trim(col("BLN_POLIZA_EMITIDA")).alias("BLN_POLIZA_EMITIDA"),
				trim(col("BLN_ACTUALIZADO_POINT")).alias("BLN_ACTUALIZADO_POINT"),
				trim(col("COD_DEPARTAMENTO")).alias("COD_DEPARTAMENTO"),
				trim(col("COD_CIUDAD")).alias("COD_CIUDAD"),
				trim(col("COD_ORIGEN")).alias("COD_ORIGEN"),
				trim(col("COD_GRADO_INVALIDEZ")).alias("COD_GRADO_INVALIDEZ"),
				trim(col("COD_OPCION_CALCULO")).alias("COD_OPCION_CALCULO"),
				col("NID_SINIESTRO").alias("NID_SINIESTRO"),
				col("NID_COTIZACION").alias("NID_COTIZACION"),
				trim(col("COD_SUCURSAL_ALFA")).alias("COD_SUCURSAL_ALFA"),
				trim(col("COD_COMPANIA")).alias("COD_COMPANIA"),
				trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
				col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
				col("NID_AFILIADO").alias("NID_AFILIADO"),
				trim(col("COD_AFP")).alias("COD_AFP"),
				col("NUM_DIAS_TRABAJADOS").alias("NUM_DIAS_TRABAJADOS"),
				trim(col("LUGAR_EXPEDICION")).alias("LUGAR_EXPEDICION"),
				trim(col("COD_ESTADO_PERSONA")).alias("COD_ESTADO_PERSONA"),
				col("PCJ_PENSION").alias("PCJ_PENSION"),
				col("NUM_ENDOSO_ULT").alias("NUM_ENDOSO_ULT"),
				trim(col("COD_TIPO_OPERACION")).alias("COD_TIPO_OPERACION"),
				trim(col("BLN_PROCESADO_POINT")).alias("BLN_PROCESADO_POINT"),
				col("FEC_PROCESO_POINT").alias("FEC_PROCESO_POINT"),
				col("FEC_ENVIO_POINT").alias("FEC_ENVIO_POINT"),
				col("NID_POLIZA_POINT").alias("NID_POLIZA_POINT"),
				col("VLR_PRIMA_UNICA_BONO").alias("VLR_PRIMA_UNICA_BONO"),
				col("VLR_PRIMA_UNICA_SUMA").alias("VLR_PRIMA_UNICA_SUMA"),
				col("VLR_PRIMA_UNICA_GARANTIA").alias("VLR_PRIMA_UNICA_GARANTIA"),
				col("VLR_PAGADO_BONO").alias("VLR_PAGADO_BONO"),
				col("VLR_PAGADO_SUMA").alias("VLR_PAGADO_SUMA"),
				col("VLR_PAGADO_GARANTIA").alias("VLR_PAGADO_GARANTIA"),
				col("VLR_CAPITAL_VAR").alias("VLR_CAPITAL_VAR"),
				col("NID_SIMULACION").alias("NID_SIMULACION"),
				col("VLR_ACUMULADO_PAGOS").alias("VLR_ACUMULADO_PAGOS"),
				col("VLR_PRIMA_CUENTA_INDIVIDUAL").alias("VLR_PRIMA_CUENTA_INDIVIDUAL"),
				col("VLR_PRIMA_UNICA_OTROS_APORTES").alias("VLR_PRIMA_UNICA_OTROS_APORTES"),
				col("NID_RECUPERACION").alias("NID_RECUPERACION"),
				col("FEC_INVALIDACION_REGISTRO").alias("FEC_INVALIDACION_REGISTRO"),
				col("FEC_ACUMULADO_PAGOS").alias("FEC_ACUMULADO_PAGOS"),
				col("NID_RENTA_MIGRACION").alias("NID_RENTA_MIGRACION"),
				trim(col("COD_MODALIDAD_RENTA")).alias("COD_MODALIDAD_RENTA"),
				trim(col("COD_CLASE_RENTA")).alias("COD_CLASE_RENTA"),
				trim(col("COD_CATEGORIA_RENTA")).alias("COD_CATEGORIA_RENTA"),
				trim(col("RESULTADO_EJECUCION_CALCULO")).alias("RESULTADO_EJECUCION_CALCULO"),
				col("NUM_RELIQUIDACION").alias("NUM_RELIQUIDACION"),
				trim(col("COD_CAUSA_ORIGEN_COMUN")).alias("COD_CAUSA_ORIGEN_COMUN"),
				col("FEC_ESTRUCTURACION_INVALIDEZ").alias("FEC_ESTRUCTURACION_INVALIDEZ"),
				trim(col("BLN_TIENE_DERECHO")).alias("BLN_TIENE_DERECHO"),
				trim(col("COD_CAUSA_RETIRO_SUPER")).alias("COD_CAUSA_RETIRO_SUPER"),
				col("FEC_REHABILITACION").alias("FEC_REHABILITACION"),
				col("NUM_RESOLUCION").alias("NUM_RESOLUCION"),
				col("FEC_RESOLUCION").alias("FEC_RESOLUCION"),
				col("NUM_MESADAS").alias("NUM_MESADAS"),
				col("PCJ_INTERES_RESERVA").alias("PCJ_INTERES_RESERVA"),
				col("PCJ_INTERES_TECNICO").alias("PCJ_INTERES_TECNICO"),
				trim(col("USUARIO_SINCRONIZACION")).alias("USUARIO_SINCRONIZACION"),
				trim(col("USUARIO_CREACION")).alias("USUARIO_CREACION"),
				col("FEC_RECONOCIMIENTO_PENSION").alias("FEC_RECONOCIMIENTO_PENSION"),
				col("FEC_DOCSOPTEJUD").alias("FEC_DOCSOPTEJUD"),
				trim(col("OBSERVACIONES_FALLOJUD")).alias("OBSERVACIONES_FALLOJUD"),
				col("PT_TK").alias("PT_TK"),
				col("PC_J12").alias("PC_J12"),
				col("VLR_EXCESO_SMMLV_IPC").alias("VLR_EXCESO_SMMLV_IPC"),
				col("VLR_EXCESO_SMMLV_IPC_EXP").alias("VLR_EXCESO_SMMLV_IPC_EXP"),
				col("PC_AD").alias("PC_AD"),
				col("PC_AD_EXP").alias("PC_AD_EXP"),
				trim(col("COD_FACTOR_SEGURIDAD")).alias("COD_FACTOR_SEGURIDAD"),
				col("VLR_GASTOS_EXP_FIJOS").alias("VLR_GASTOS_EXP_FIJOS"),
				col("VLR_GASTOS_ADM_FIJOS").alias("VLR_GASTOS_ADM_FIJOS"),
				col("PCJ_GASTOS_EXP").alias("PCJ_GASTOS_EXP"),
				col("PCJ_ADMINISTRACION").alias("PCJ_ADMINISTRACION"),
				trim(col("COD_ORIGEN_SIN")).alias("COD_ORIGEN_SIN"),
				col("FEC_CAUSACION").alias("FEC_CAUSACION"),
				trim(col("COD_ORIGEN_INI")).alias("COD_ORIGEN_INI"),
				col("FEC_ULTIMA_MOD").alias("FEC_ULTIMA_MOD"),
				col("PCJ_PCL_INICIAL").alias("PCJ_PCL_INICIAL"),
				col("PCJ_PCL_ACTUAL").alias("PCJ_PCL_ACTUAL"),
                col("FEC_EXPEDICION_PARTITION").alias("FEC_EXPEDICION_PARTITION")
            ),
			lambda df: df.dropDuplicates()
        ]
        },
        "PRV_BENEFICIARIOS": {
            "partition_columns": ["fec_aparicion_partition"],
            "schema": "PRV",
            "transform": 
        [
            lambda df: df.select(
				col("NID_RENTAS").alias("NID_RENTAS"),
				trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
				col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
				col("NID_RENTAS_RESP").alias("NID_RENTAS_RESP"),
				trim(col("COD_TIPO_IDENTIFICACION_RESP")).alias("COD_TIPO_IDENTIFICACION_RESP"),
				col("NID_NUMERO_IDENTIFICACION_RESP").alias("NID_NUMERO_IDENTIFICACION_RESP"),
				trim(col("COD_PARENTESCO")).alias("COD_PARENTESCO"),
				trim(col("COD_ESTADO_PERSONA")).alias("COD_ESTADO_PERSONA"),
				trim(col("BLN_ES_BENEFICIARIO")).alias("BLN_ES_BENEFICIARIO"),
				trim(col("BLN_ES_TUTOR")).alias("BLN_ES_TUTOR"),
				trim(col("BLN_CERTIFICADO_ESCOLARIDAD")).alias("BLN_CERTIFICADO_ESCOLARIDAD"),
				trim(col("BLN_DEPENDENCIA_ECONOMICA")).alias("BLN_DEPENDENCIA_ECONOMICA"),
				col("FEC_INICIO_PENSION").alias("FEC_INICIO_PENSION"),
				col("FEC_MUERTE").alias("FEC_MUERTE"),
				col("FEC_APARICION").alias("FEC_APARICION"),
				col("PCJ_PENSION").alias("PCJ_PENSION"),
				col("VLR_INIC_PENSION").alias("VLR_INIC_PENSION"),
				trim(col("BLN_CON_DERECHO")).alias("BLN_CON_DERECHO"),
				col("FEC_PERDIDA_DERECHO").alias("FEC_PERDIDA_DERECHO"),
				trim(col("COD_TIPO_IDENTIFICACION_PADRE")).alias("COD_TIPO_IDENTIFICACION_PADRE"),
				col("NID_NUMERO_IDENTIFICACION_PADR").alias("NID_NUMERO_IDENTIFICACION_PADR"),
				col("CANT_HIJOS").alias("CANT_HIJOS"),
				col("FEC_INVALIDEZ").alias("FEC_INVALIDEZ"),
				trim(col("COD_GRADO_INVALIDEZ")).alias("COD_GRADO_INVALIDEZ"),
				trim(col("COD_CAUSA_RETIRO_SUPER")).alias("COD_CAUSA_RETIRO_SUPER"),
                col("fec_aparicion_partition").alias("fec_aparicion_partition")  
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "PRV_ENDOSOS_POINT": {
            "partition_columns": ["FEC_PROCESO_POINT_PARTITION"],
            "schema": "PRV",
            "transform": [
                lambda df: df.select(
					col("NID_POLIZA_POINT").alias("NID_POLIZA_POINT"),
					col("NID_RENTAS").alias("NID_RENTAS"),
					col("NID_ENDOSO_POINT").alias("NID_ENDOSO_POINT"),
					trim(col("MODULO")).alias("MODULO"),
					col("NUM_ENDOSO_POINT").alias("NUM_ENDOSO_POINT"),
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
					trim(col("PERIODO_POINT")).alias("PERIODO_POINT"),
					trim(col("COD_TIPO_OPERACION")).alias("COD_TIPO_OPERACION"),
					col("NID_ENDOSO_REVERSION").alias("NID_ENDOSO_REVERSION"),
					col("NID_ENDOSO_REVERSADO").alias("NID_ENDOSO_REVERSADO"),
					col("FEC_CREACION").alias("FEC_CREACION"),
					col("FEC_ULTIMA_ACTUALIZACION").alias("FEC_ULTIMA_ACTUALIZACION"),
					col("VLR_TOTAL_RECAUDO").alias("VLR_TOTAL_RECAUDO"),
					trim(col("BLN_PROCESADO_RECAUDOS")).alias("BLN_PROCESADO_RECAUDOS"),
                    col("FEC_PROCESO_POINT_PARTITION").alias("FEC_PROCESO_POINT_PARTITION")
                )
            ]
        },
        "SPR_SINIESTROS": {
            "partition_columns": ["fec_siniestro_partition"],
            "schema": "PRV",
            "transform": 
        [
            lambda df: df.select(
				col("NID_SINIESTRO").alias("NID_SINIESTRO"),
				col("NID_SINIESTRO_MIGRACION").alias("NID_SINIESTRO_MIGRACION"),
				col("NID_SOLICITUD_AFP").alias("NID_SOLICITUD_AFP"),
				trim(col("NID_SINIESTRO_POINT")).alias("NID_SINIESTRO_POINT"),
				col("FEC_SINIESTRO").alias("FEC_SINIESTRO"),
				col("FEC_SOLICITUD_AFP").alias("FEC_SOLICITUD_AFP"),
				trim(col("PAIS")).alias("PAIS"),
				col("FEC_AVISO").alias("FEC_AVISO"),
				trim(col("COD_ESTADO_PERSONA")).alias("COD_ESTADO_PERSONA"),
				col("FEC_ULTIMA_COTIZACION").alias("FEC_ULTIMA_COTIZACION"),
				col("NUM_SEMANAS_COTIZADAS_AFP").alias("NUM_SEMANAS_COTIZADAS_AFP"),
				col("FEC_ESTRUCTURACION_INCAPACIDAD").alias("FEC_ESTRUCTURACION_INCAPACIDAD"),
				trim(col("COD_GRADO_INVALIDEZ")).alias("COD_GRADO_INVALIDEZ"),
				col("EDAD_AFP").alias("EDAD_AFP"),
				col("EDAD_ASEGURADORA").alias("EDAD_ASEGURADORA"),
				col("NUM_HIJOS").alias("NUM_HIJOS"),
				col("VLR_SALARIO_BASE").alias("VLR_SALARIO_BASE"),
				trim(col("BLN_26_SEMANAS")).alias("BLN_26_SEMANAS"),
				col("NUM_DIAS_TRABAJADOS").alias("NUM_DIAS_TRABAJADOS"),
				col("FEC_PRIMERA_LIQUIDACION").alias("FEC_PRIMERA_LIQUIDACION"),
				col("FEC_PAGO").alias("FEC_PAGO"),
				trim(col("COD_OPCION_ULTIMO_CALCULO")).alias("COD_OPCION_ULTIMO_CALCULO"),
				col("VLR_CUENTA_INDIVIDUAL_ACTUAL").alias("VLR_CUENTA_INDIVIDUAL_ACTUAL"),
				col("VLR_CUENTA_INDIVIDUAL_SOLAFP").alias("VLR_CUENTA_INDIVIDUAL_SOLAFP"),
				col("VLR_BONO_PENSIONAL_INICIAL").alias("VLR_BONO_PENSIONAL_INICIAL"),
				trim(col("BLN_SOLICITA_AUXILIO_FUNERARIO")).alias("BLN_SOLICITA_AUXILIO_FUNERARIO"),
				trim(col("BLN_APROBADO_AUXILIO_FUNERARIO")).alias("BLN_APROBADO_AUXILIO_FUNERARIO"),
				col("VLR_AUXILIO_FUNERARIO").alias("VLR_AUXILIO_FUNERARIO"),
				trim(col("COD_RESULTADO_ANALISIS")).alias("COD_RESULTADO_ANALISIS"),
				trim(col("OBSERVACIONES_ASEGURADORA")).alias("OBSERVACIONES_ASEGURADORA"),
				col("NID_EPS_BENEFICIARIOS").alias("NID_EPS_BENEFICIARIOS"),
				trim(col("BLN_COTIZABA_AL_SINIESTRO")).alias("BLN_COTIZABA_AL_SINIESTRO"),
				trim(col("OBSERVACIONES_AFP")).alias("OBSERVACIONES_AFP"),
				trim(col("COD_ORIGEN")).alias("COD_ORIGEN"),
				trim(col("COD_ESTADO_BONO")).alias("COD_ESTADO_BONO"),
				col("VLR_EMISION_BONO").alias("VLR_EMISION_BONO"),
				col("FEC_EMISION_BONO").alias("FEC_EMISION_BONO"),
				col("FEC_REDENCION_EMISION").alias("FEC_REDENCION_EMISION"),
				col("VLR_PAGO_BONO_MINAFP").alias("VLR_PAGO_BONO_MINAFP"),
				col("FEC_PAGO_BONO_MINAFP").alias("FEC_PAGO_BONO_MINAFP"),
				col("VLR_PAGO_BONO_AFPASG").alias("VLR_PAGO_BONO_AFPASG"),
				col("FEC_PAGO_BONO_AFPASG").alias("FEC_PAGO_BONO_AFPASG"),
				trim(col("COD_ESTADO_SINIESTRO")).alias("COD_ESTADO_SINIESTRO"),
				col("VLR_RESERVA_INICIAL").alias("VLR_RESERVA_INICIAL"),
				col("VLR_RESERVA_ULTIMO").alias("VLR_RESERVA_ULTIMO"),
				col("FEC_ULTIMO_CALCULO").alias("FEC_ULTIMO_CALCULO"),
				col("VLR_SUMA_ADICIONAL_ULT_LIQ").alias("VLR_SUMA_ADICIONAL_ULT_LIQ"),
				col("VLR_GARANTIA_ESTATAL_ULT_LIQ").alias("VLR_GARANTIA_ESTATAL_ULT_LIQ"),
				col("VLR_MESADAS_ATRASADAS_ULT_LIQ").alias("VLR_MESADAS_ATRASADAS_ULT_LIQ"),
				trim(col("USUARIO_ASEGURADORA")).alias("USUARIO_ASEGURADORA"),
				trim(col("USUARIO_BENEFICIOS")).alias("USUARIO_BENEFICIOS"),
				col("NID_AFILIADO").alias("NID_AFILIADO"),
				trim(col("COD_TIPO_IDENTIFICACION")).alias("COD_TIPO_IDENTIFICACION"),
				col("NID_NUMERO_IDENTIFICACION").alias("NID_NUMERO_IDENTIFICACION"),
				trim(col("COD_DEPARTAMENTO")).alias("COD_DEPARTAMENTO"),
				trim(col("COD_CIUDAD")).alias("COD_CIUDAD"),
				col("COD_CAUSA_SINIESTRO_AFP").alias("COD_CAUSA_SINIESTRO_AFP"),
				col("NUM_PERIODOS_PRIMA").alias("NUM_PERIODOS_PRIMA"),
				col("VLR_PERIODOS_PRIMA").alias("VLR_PERIODOS_PRIMA"),
				col("NUM_PERIODOS_HISTORIA_LABORAL").alias("NUM_PERIODOS_HISTORIA_LABORAL"),
				col("VLR_PERIODOS_HISTORIA_LABORAL").alias("VLR_PERIODOS_HISTORIA_LABORAL"),
				trim(col("COD_AFP")).alias("COD_AFP"),
				trim(col("RESULTADO_EJECUCION_CALCULO")).alias("RESULTADO_EJECUCION_CALCULO"),
				col("FEC_EJECUCION_ULTIMO_CALCULO").alias("FEC_EJECUCION_ULTIMO_CALCULO"),
				col("FEC_GENE_PAGO_LOTE").alias("FEC_GENE_PAGO_LOTE"),
				col("FEC_MARCA_GENERAR_COTIZ").alias("FEC_MARCA_GENERAR_COTIZ"),
				col("FEC_GENE_COTIZ").alias("FEC_GENE_COTIZ"),
				col("NUM_RELIQUIDACION").alias("NUM_RELIQUIDACION"),
				trim(col("COD_CAUSA_SINIESTRO_MIG")).alias("COD_CAUSA_SINIESTRO_MIG"),
				col("NID_SOLICITUD_TEMPORAL_AFP").alias("NID_SOLICITUD_TEMPORAL_AFP"),
				col("NID_ACTUALIZACION_AVISO_ULT").alias("NID_ACTUALIZACION_AVISO_ULT"),
				col("NID_ACTUALIZACION_CANC_ULT").alias("NID_ACTUALIZACION_CANC_ULT"),
				col("NID_ACTUALIZACION_BONO_ULT").alias("NID_ACTUALIZACION_BONO_ULT"),
				trim(col("COD_ASEGURADORA")).alias("COD_ASEGURADORA"),
				trim(col("BLN_MARCA_PAGO_LOTE")).alias("BLN_MARCA_PAGO_LOTE"),
				col("VLR_PENSION_TOTAL_ULT").alias("VLR_PENSION_TOTAL_ULT"),
				trim(col("COD_ETAPA")).alias("COD_ETAPA"),
				col("NID_POLIZA").alias("NID_POLIZA"),
				col("FEC_APROBACION_ASG").alias("FEC_APROBACION_ASG"),
				col("FEC_OBJECION_ASG").alias("FEC_OBJECION_ASG"),
				col("FEC_INGRESO").alias("FEC_INGRESO"),
				col("FEC_OBJECION_POINT").alias("FEC_OBJECION_POINT"),
				col("FEC_PAGO_POINT").alias("FEC_PAGO_POINT"),
				col("FEC_AVISO_POINT").alias("FEC_AVISO_POINT"),
				col("FEC_RECUPERACION_BONO").alias("FEC_RECUPERACION_BONO"),
				col("VLR_AUXILIO_FUNERARIO_ULT_LIQ").alias("VLR_AUXILIO_FUNERARIO_ULT_LIQ"),
				col("NID_ACTUALIZACION_CASO_ULT").alias("NID_ACTUALIZACION_CASO_ULT"),
				col("ULT_PERIODO_COTIZADO_NOREVERSA").alias("ULT_PERIODO_COTIZADO_NOREVERSA"),
				col("VLR_BONO_PEN_TRASLADO_REGIMEN").alias("VLR_BONO_PEN_TRASLADO_REGIMEN"),
				col("FEC_ULTIMA_ACTUALIZACION").alias("FEC_ULTIMA_ACTUALIZACION"),
				col("VLR_BONO_PEN_ACTUALIZADO_SINI").alias("VLR_BONO_PEN_ACTUALIZADO_SINI"),
				col("FEC_RECIBIDO_EXPEDIENTE").alias("FEC_RECIBIDO_EXPEDIENTE"),
				trim(col("COD_CAUSA_RELIQUIDACION")).alias("COD_CAUSA_RELIQUIDACION"),
				col("VLR_CUOTAS_PARTE").alias("VLR_CUOTAS_PARTE"),
				col("VLR_CUPON_PPAL").alias("VLR_CUPON_PPAL"),
				trim(col("COD_ESTADO_CASO_AFP")).alias("COD_ESTADO_CASO_AFP"),
				col("NID_CALCULO_RELIQUI").alias("NID_CALCULO_RELIQUI"),
				trim(col("BLN_AVISO_AUT")).alias("BLN_AVISO_AUT"),
				col("VLR_INCAPACIDAD").alias("VLR_INCAPACIDAD"),
				trim(col("CAUSA_ORIGEN_COMUN")).alias("CAUSA_ORIGEN_COMUN"),
				col("NID_ACTUALIZACION_CASO_AFP_ULT").alias("NID_ACTUALIZACION_CASO_AFP_ULT"),
				trim(col("BLN_FINANCIAR_SOLO_CUOTA_PARTE")).alias("BLN_FINANCIAR_SOLO_CUOTA_PARTE"),
				trim(col("BLN_FINANCIAR_CUPON_PPAL")).alias("BLN_FINANCIAR_CUPON_PPAL"),
				trim(col("BLN_TIENE_DERECHO")).alias("BLN_TIENE_DERECHO"),
				trim(col("COD_MUERTE_FASECOLDA")).alias("COD_MUERTE_FASECOLDA"),
				col("VLR_BONO_PENSIONAL_CUENTA").alias("VLR_BONO_PENSIONAL_CUENTA"),
				col("FEC_ACREDITACION_BONO").alias("FEC_ACREDITACION_BONO"),
				col("FEC_INICIO_SOLICITUD_AFP").alias("FEC_INICIO_SOLICITUD_AFP"),
				col("FEC_PRIMERA_CALIFICACION_INVAL").alias("FEC_PRIMERA_CALIFICACION_INVAL"),
				col("FEC_RECLAMA_BONO_SIN_REDIMIR").alias("FEC_RECLAMA_BONO_SIN_REDIMIR"),
				trim(col("BLN_VALIDADO_RAIS")).alias("BLN_VALIDADO_RAIS"),
				col("PCJ_PCL").alias("PCJ_PCL"),
				col("NUM_MESADAS").alias("NUM_MESADAS"),
				col("FEC_AVISO_ORIGINAL").alias("FEC_AVISO_ORIGINAL"),
				col("PCJ_INTERES_RESERVA").alias("PCJ_INTERES_RESERVA"),
				trim(col("TASA_PROMEDIO")).alias("TASA_PROMEDIO"),
				trim(col("USUARIO_CREACION")).alias("USUARIO_CREACION"),
				col("COD_CLASE_JURIDICO").alias("COD_CLASE_JURIDICO"),
				trim(col("COD_FACTOR_SEGURIDAD")).alias("COD_FACTOR_SEGURIDAD"),			
                col("fec_siniestro_partition").alias("fec_siniestro_partition")
            ),
            lambda df: df.dropDuplicates()
        ]
        },			
        "PRV_RENTAS_APROBADAS_OBP": {
            "partition_columns": ["fecha_inscripcion_partition"],
            "schema": "PRV",
            "transform": 
        [
            lambda df: df.select(
				col("NID_RENTAS").alias("NID_RENTAS"),
				trim(col("INSCRIPCION_MECANISMO")).alias("INSCRIPCION_MECANISMO"),
				col("FECHA_INSCRIPCION").alias("FECHA_INSCRIPCION"),
				col("PARAMETRO_DESLIZAMIENTO").alias("PARAMETRO_DESLIZAMIENTO"),
				col("TASA_FACTOR_SEGURIDAD").alias("TASA_FACTOR_SEGURIDAD"),
				col("COBERTURA").alias("COBERTURA"),	
                col("fecha_inscripcion_partition").alias("fecha_inscripcion_partition")
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "PRV_RENTAS_NOTAS": {
            "partition_columns": ["fec_modificacion_partition"],
            "schema": "PRV",
            "transform": 
        [
            lambda df: df.select(
				col("NID_RENTAS").alias("NID_RENTAS"),
				col("NUM_RADICADO_NOTA").alias("NUM_RADICADO_NOTA"),
				trim(col("COD_RAMO_SFC")).alias("COD_RAMO_SFC"),
				col("FEC_MODIFICACION").alias("FEC_MODIFICACION"),
                col("fec_modificacion_partition").alias("fec_modificacion_partition")    
            ),
            lambda df: df.dropDuplicates()
        ]
        },
        "PRV_SALDOS_MATEMATICA": {
            "partition_columns": ["FEC_CORTE_CALCULO_PARTITION"],
            "schema": "PRV",
            "transform": 
        [
            lambda df: df.select(
				col("NID_RENTAS").alias("NID_RENTAS"),
				col("NID_CALCULO").alias("NID_CALCULO"),
				col("MAX").alias("MAX"),
				col("VLR_RESERVA_CALCULADA").alias("VLR_RESERVA_CALCULADA"),
				trim(col("PERIODO")).alias("PERIODO"),
				trim(col("PERIODO_INGRESO")).alias("PERIODO_INGRESO"),
				col("VLR_PENSION_REAL").alias("VLR_PENSION_REAL"),
				col("FEC_CORTE_CALCULO").alias("FEC_CORTE_CALCULO"),
				col("VLR_RESERVA_CONTABLE").alias("VLR_RESERVA_CONTABLE"),
				col("VLR_DIFERENCIA_RESOLUCION").alias("VLR_DIFERENCIA_RESOLUCION"),
				trim(col("BLN_CASO_TRANSICION")).alias("BLN_CASO_TRANSICION"),
				col("MES_DIFERIDO_TRANSICION").alias("MES_DIFERIDO_TRANSICION"),
				col("PCJ_APLICADO_TRANSICION").alias("PCJ_APLICADO_TRANSICION"),
                col("FEC_CORTE_CALCULO_PARTITION").alias("FEC_CORTE_CALCULO_PARTITION")    
            ),
            lambda df: df.dropDuplicates()
        ]
        },
       "CRV_CALCULOS_2": {
        "partition_columns": ["FEC_CORTE_CALCULO_PARTITION"],
        "schema": "PRV",
        "transform": [
            lambda df: df.select(
				col("NID_RENTAS").alias("NID_RENTAS"),
				col("NID_CALCULO").alias("NID_CALCULO"),
				col("VLR_PENSION_REAL").alias("VLR_PENSION_REAL"),
				col("VLR_PENSION_CALCULOS").alias("VLR_PENSION_CALCULOS"),
				col("VLR_RESERVA_CALCULADA").alias("VLR_RESERVA_CALCULADA"),
				col("VLR_INCREMENTO_PENSION").alias("VLR_INCREMENTO_PENSION"),
				col("FEC_CALCULO").alias("FEC_CALCULO"),
				col("PCJ_TASA_INTERES_TECNICO").alias("PCJ_TASA_INTERES_TECNICO"),
				col("PCJ_TASA_INFLACION").alias("PCJ_TASA_INFLACION"),
				col("PCJ_TASA_ADMINISTRACION").alias("PCJ_TASA_ADMINISTRACION"),
				col("FEC_CORTE_CALCULO").alias("FEC_CORTE_CALCULO"),
				col("FEC_INICIAL_EVENTO").alias("FEC_INICIAL_EVENTO"),
				col("FEC_SISTEMA").alias("FEC_SISTEMA"),
				trim(col("SOBREVIVIENTES")).alias("SOBREVIVIENTES"),
				trim(col("COD_ORIGEN")).alias("COD_ORIGEN"),
				trim(col("COD_GRADO_INVALIDEZ")).alias("COD_GRADO_INVALIDEZ"),
				col("FEC_NACIMIENTO_TRABAJADOR").alias("FEC_NACIMIENTO_TRABAJADOR"),
				col("NUM_EDAD_TRABAJADOR").alias("NUM_EDAD_TRABAJADOR"),
				trim(col("GENERO_TRABAJADOR")).alias("GENERO_TRABAJADOR"),
				trim(col("COD_ESTADO_TRABAJADOR")).alias("COD_ESTADO_TRABAJADOR"),
				trim(col("COD_ESTADO_CIVIL")).alias("COD_ESTADO_CIVIL"),
				trim(col("BENEFICIARIO_UNO")).alias("BENEFICIARIO_UNO"),
				col("FEC_NACIMIENTO_BEN_UNO").alias("FEC_NACIMIENTO_BEN_UNO"),
				col("NUM_EDAD_BENEFICIARIO_UNO").alias("NUM_EDAD_BENEFICIARIO_UNO"),
				trim(col("GENERO_BENEFICIARIO_UNO")).alias("GENERO_BENEFICIARIO_UNO"),
				trim(col("COD_ESTADO_BENEFICIARIO_UNO")).alias("COD_ESTADO_BENEFICIARIO_UNO"),
				trim(col("COD_TIPO_RESERVA_BEN_UNO")).alias("COD_TIPO_RESERVA_BEN_UNO"),
				trim(col("BENEFICIARIO_DOS")).alias("BENEFICIARIO_DOS"),
				col("FEC_NACIMIENTO_BEN_DOS").alias("FEC_NACIMIENTO_BEN_DOS"),
				col("NUM_EDAD_BENEFICIARIO_DOS").alias("NUM_EDAD_BENEFICIARIO_DOS"),
				trim(col("GENERO_BENEFICIARIO_DOS")).alias("GENERO_BENEFICIARIO_DOS"),
				trim(col("COD_ESTADO_BENEFICIARIO_DOS")).alias("COD_ESTADO_BENEFICIARIO_DOS"),
				trim(col("COD_TIPO_RESERVA_BEN_DOS")).alias("COD_TIPO_RESERVA_BEN_DOS"),
				col("NUM_MESADAS").alias("NUM_MESADAS"),
				col("VLR_SUMA_MESADAS").alias("VLR_SUMA_MESADAS"),
				col("VLR_SUMA_AUX_FUN").alias("VLR_SUMA_AUX_FUN"),
				col("VLR_SUMA_GASTOS").alias("VLR_SUMA_GASTOS"),
				trim(col("COD_TIPO_ID_BEN_UNO")).alias("COD_TIPO_ID_BEN_UNO"),
				col("NID_NUMERO_ID_BEN_UNO").alias("NID_NUMERO_ID_BEN_UNO"),
				trim(col("COD_TIPO_ID_BEN_DOS")).alias("COD_TIPO_ID_BEN_DOS"),
				col("NID_NUMERO_ID_BEN_DOS").alias("NID_NUMERO_ID_BEN_DOS"),
				col("FEC_MUERTE_AFILIADO").alias("FEC_MUERTE_AFILIADO"),
				trim(col("BLN_CASO_TRANSICION")).alias("BLN_CASO_TRANSICION"),
				col("MES_DIFERIDO_TRANSICION").alias("MES_DIFERIDO_TRANSICION"),
				col("PCJ_APLICADO_TRANSICION").alias("PCJ_APLICADO_TRANSICION"),
				col("VLR_RESERVA_CONTABLE").alias("VLR_RESERVA_CONTABLE"),
				col("VLR_EXCESO_SMMLV_IPC").alias("VLR_EXCESO_SMMLV_IPC"),
				col("PCJ_INTERES_RESERVA").alias("PCJ_INTERES_RESERVA"),
				col("PCJ_TASA_EQUIVALENTE").alias("PCJ_TASA_EQUIVALENTE"),
				col("NUM_ANIOS_ANTES_SMMLV").alias("NUM_ANIOS_ANTES_SMMLV"),
				col("NUM_ANIOS_DIFERIMIENTO").alias("NUM_ANIOS_DIFERIMIENTO"),
				col("PC_J12").alias("PC_J12"),
				col("PCJ_TMR_EQUIVALENTE").alias("PCJ_TMR_EQUIVALENTE"),
				col("USUARIO_CREACION").alias("USUARIO_CREACION"),
				col("PCJ_GASTOS_EXP").alias("PCJ_GASTOS_EXP"),
				col("PCJ_FACTOR_SEGURIDAD").alias("PCJ_FACTOR_SEGURIDAD"),
				col("NID_CALCULO_CA").alias("NID_CALCULO_CA"),
				col("VLR_GASTOS_EXP_FIJOS").alias("VLR_GASTOS_EXP_FIJOS"),
				col("VLR_GASTOS_ADM_FIJOS").alias("VLR_GASTOS_ADM_FIJOS"),
                col("FEC_CORTE_CALCULO_PARTITION").alias("FEC_CORTE_CALCULO_PARTITION")
            )
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
    