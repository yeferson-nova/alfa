"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, LongType
from pyspark.sql.functions import lit,col, split, explode, trim, lower, regexp_replace

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
        'DB_CATALOG_GLUE',
        'TABLE_NAME_GOLD'
        ]
    )
BUCKET_NAME = args['BUCKET_NAME']
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
DB_NAME = args['DB_NAME']
DB_SCHEMA = args['DB_SCHEMA']
PERIOD = args['PERIOD']
CERTIFICATE = args['CERTIFICATE']
DB_CATALOG_GLUE = args['DB_CATALOG_GLUE']
TABLE_NAME_GOLD = args['TABLE_NAME_GOLD']

logger.info(f"Period: {PERIOD}")
logger.info(f"DB_CATALOG_GLUE_SILVER: {DB_CATALOG_GLUE}")

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

DB_CATALOG_GLUE = 'datalake-dev-silver-catalog-db'
PERIODO = "2025-01"
FECHA_INICIO = "2025-01-01"
FECHA_FIN = "2025-01-31"


###################################################################
##################### Init stage gold #############################
###################################################################

# Define the schema for the  Final DataFrame
schema_pcr_tradicional_gold = StructType([
        StructField("ORIGEN", StringType(), False),                    # campo 1 ORIGEN
        StructField("COMPANIA", StringType(), True),                   # campo 2 COMPANIA
        StructField("RAMO", StringType(), True),                       # campo 3 RAMO
        StructField("DESC_RAMO", StringType(), True),                  # campo 4 DESC_RAMO
        StructField("PRODUCTO", DecimalType(19,0), False),             # campo 5 PRODUCTO
        StructField("DESC_PRODUCTO", StringType(), True),              # campo 6 DESC_PRODUCTO
        StructField("RAMO_TECNICO", StringType(), True),               # campo 7 RAMO_TECNICO
        StructField("DESC_RAMO_TECNICO", StringType(), True),          # campo 8 DESC_RAMO_TECNICO
        StructField("POLIZA_MATRIZ", StringType(), True),              # campo 9 POLIZA_MATRIZ
        StructField("POLIZA", StringType(), True),                     # campo 10 POLIZA
        StructField("SUCURSAL", StringType(), True),                   # campo 11 SUCURSAL
        StructField("MODULO", StringType(), True),                     # campo 12 MODULO
        StructField("UNIDAD_RIESGO", StringType(), True),              # campo 13 UNIDAD_RIESGO
        StructField("COBERTURA", StringType(), True),                  # campo 14 COBERTURA
        StructField("DESC_COBERTURA", StringType(), True),             # campo 15 DESC_COBERTURA
        StructField("ENDOSO", DecimalType(19,0), False),               # campo 16 ENDOSO
        StructField("FECHA_ENDOSO", DateType(), False),                # campo 17 FECHA_ENDOSO
        StructField("MESPRO", StringType(), False),                    # campo 18 MESPRO
        StructField("UNIDAD_NEGOCIO", StringType(), True),             # campo 19 UNIDAD_NEGOCIO
        StructField("FECHA_INICIO_POLIZA_MATRIZ", DateType(), False), # campo 20 FECHA_INICIO_POLIZA_MATRIZ
        StructField("FECHA_FIN_POLIZA_MATRIZ", DateType(), True),    # campo 21 FECHA_FIN_POLIZA_MATRIZ
        StructField("FECHA_INICIO_COBERTURA", DateType(), False),     # campo 22 FECHA_INICIO_COBERTURA
        StructField("FECHA_FIN_COBERTURA", DateType(), True),        # campo 23 FECHA_FIN_COBERTURA
        StructField("ID_TOMADOR_POLIZA_MATRIZ", StringType(), True),   # campo 24 ID_TOMADOR_POLIZA_MATRIZ
        StructField("TOMADOR_POLIZA_MATRIZ", StringType(), True),      # campo 25 TOMADOR_POLIZA_MATRIZ
        StructField("ID_TOMADOR_POLIZA", DecimalType(19,0), False),          # campo 26 ID_TOMADOR_POLIZA
        StructField("TOMADOR_POLIZA", StringType(), True),             # campo 27 TOMADOR_POLIZA
        StructField("TIPO_COASEGURO", StringType(), True),             # campo 28 TIPO_COASEGURO
        StructField("PORCPART_ALFA", StringType(), True),              # campo 29 PORCPART_ALFA
        StructField("FRECUENCIA_PAGO", StringType(), True),            # campo 30 FRECUENCIA_PAGO
        StructField("PRIMA_EMITIDA", DecimalType(38,10), False),              # campo 31 PRIMA_EMITIDA
        StructField("PRIMACED_COASEGURO", StringType(), True),         # campo 32 PRIMACED_COASEGURO
        StructField("PRIMAALFA", StringType(), True),                  # campo 33 PRIMAALFA
        StructField("PORC_COM_INTERMEDIACION", StringType(), True),    # campo 34 PORC_COM_INTERMEDIACION
        StructField("COMISION_INTERMEDIACION", StringType(), True),    # campo 35 COMISION_INTERMEDIACION
        StructField("IVA_COMISION_INTERMEDIACION",  DecimalType(38,10), True),# campo 36 IVA_COMISION_INTERMEDIACION
        StructField("PORCCOMRECAUDO", StringType(), True),             # campo 37 PORCCOMRECAUDO
        StructField("COMISION_RECAUDO", DecimalType(38,10), False),           # campo 38 COMISION_RECAUDO
        StructField("IVA_COMISION_RECAUDO", DecimalType(38,10), True),       # campo 39 IVA_COMISION_RECAUDO
        StructField("CODREASEG", StringType(), True),                  # campo 40 CODREASEG
        StructField("NOMREASEG", StringType(), False),                  # campo 41 NOMREASEG
        StructField("CLASE_REASEGURADOR", StringType(), True),         # campo 42 CLASE_REASEGURADOR
        StructField("PRIMACEDAUT", StringType(), True),                # campo 43 PRIMACEDAUT
        StructField("PRIMACEDAUT_INT", StringType(), True),            # campo 44 PRIMACEDAUT_INT
        StructField("PRIMACEDAUT_EXT", StringType(), True),            # campo 45 PRIMACEDAUT_EXT
        StructField("PRIMACEDFAC", StringType(), True),                # campo 46 PRIMACEDFAC
        StructField("PRIMACEDFAC_INT", StringType(), True),            # campo 47 PRIMACEDFAC_INT
        StructField("PRIMACEDFAC_EXT", StringType(), True),            # campo 48 PRIMACEDFAC_EXT
        StructField("COMISIONREASEGAUT", StringType(), True),          # campo 49 COMISIONREASEGAUT
        StructField("COMISIONREASEGAUT_INT", StringType(), True),      # campo 50 COMISIONREASEGAUT_INT
        StructField("COMISIONREASEGAUT_EXT", StringType(), True),      # campo 51 COMISIONREASEGAUT_EXT
        StructField("COMISIONREASEGFAC", StringType(), True),          # campo 52 COMISIONREASEGFAC
        StructField("COMISIONREASEGFAC_INT", StringType(), True),      # campo 53 COMISIONREASEGFAC_INT
        StructField("COMISIONREASEGFAC_EXT", StringType(), True),      # campo 54 COMISIONREASEGFAC_EXT
        StructField("GASTOS_EXP_PORCENTAJE", StringType(), True),      # campo 55 GASTOS_EXP_PORCENTAJE
        StructField("GASTOS_EXP", StringType(), True),                 # campo 56 GASTOS_EXP
        StructField("UNIDAD_DE_CUENTA", StringType(), True)            # campo 57 UNIDAD_DE_CUENTA
    ])
    
    
#-----------------------------------------------------------------#
#----------------------- Read Sources ----------------------------#
#-----------------------------------------------------------------#  

# SELECT df_contextoperation_par
df_contextoperation_par = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_contextoperation_par").toDF()
df_contextoperation_par_select = df_contextoperation_par.select("id", "time_stamp", "status", "time_stamp_partition")

# SELECT: df_thirdpartymovementpolicy_par
df_thirdpartymovementpolicy_par = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_thirdpartymovementpolicy_par").toDF()
df_thirdpartymovementpolicy_par_select = df_thirdpartymovementpolicy_par.select("operationpk", "policyid", "thirdparty").distinct()

# SELECT: prepolicy
df_prepolicy = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_prepolicy").toDF()
df_prepolicy_select = df_prepolicy.select("pk", "static", "numeropolizainput", "tipopolizavalue", "tipopolizainput", "oficinainput", "frecuenciapagoinput").distinct()

# SELECT: policydco
df_policydco = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_policydco").toDF()
df_policydco_select = df_policydco.select("agregatedobjectid", "dcoid", "operationpk", "initialdate", "finishdate").distinct()

# SELECT: agregatedpolicy
df_agregatedpolicy = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_agregatedpolicy").toDF()
df_agregatedpolicy_select = df_agregatedpolicy.select("agregatedpolicyid", "productid").distinct()

# SELECT: product
df_product = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_product").toDF()
df_product_select = df_product.select("pro_dcoid", "productid", "description").distinct()

# SELECT: preproduct
df_preproduct = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_preproduct").toDF()
df_preproduct_select = df_preproduct.select("pk", "companiainput", "ramoinput", "codigoproductoinput", "descripcionproductoinput", "ramovalue").distinct()

# SELECT: stpo_policyparticipation
df_stpo_policyparticipation = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_stpo_policyparticipation").toDF()
df_stpo_policyparticipation_select = df_stpo_policyparticipation.select("thirdpartyid", "rol_id", "agregatedobjectid", "operationpk").distinct()

# SELECT: stpo_policyparticipationdco
df_stpo_policyparticipationdco = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_stpo_policyparticipationdco").toDF()
df_stpo_policyparticipationdco_select = df_stpo_policyparticipationdco.select("operationpk", "agregatedobjectid", "dcoid").distinct()

# SELECT: stps_relationagreementpolicy
df_stps_relationagreementpolicy = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_stps_relationagreementpolicy").toDF()
df_stps_relationagreementpolicy_select = df_stps_relationagreementpolicy.select("rsa_policyid", "rsa_ast_id").distinct()

# SELECT: stps_agreementstaticdat
df_stps_agreementstaticdat = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_stps_agreementstaticdat").toDF()
df_stps_agreementstaticdat_select = df_stps_agreementstaticdat.select("ast_id", "ast_numeroacu").distinct()

# SELECT: prethirdparty
df_prethirdparty = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_prethirdparty").toDF()
df_prethirdparty_select = df_prethirdparty.select("static", "nombrepredeterminadoinput", "tipodocumentoinput", "tipodocumentovalue", "documentoidentidadinput").distinct()

# SELECT: coveragedco
df_coveragedco = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_coveragedco_par").toDF()
df_coveragedco_select = df_coveragedco.select("operationpk", "dcoid").distinct()

# SELECT: cobertura
df_cobertura = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_cobertura").toDF()
df_cobertura_select = df_cobertura.select(
    col("pk"),
    regexp_replace(col("cobramocontableinput"), r"\.0", "").alias("cobramocontableinput"),   
    col("cobcodigocoberturainput"),
    col("cobdescripcioncobertinput"),
    regexp_replace(col("cobramoreaseguroinput"), r"\.0", "").alias("cobramoreaseguroinput"),  
    col("cobprimareasegurovalue").cast(DecimalType(38, 10)).alias("cobprimareasegurovalue"),
    col("cobprimamovimientovalue").cast(DecimalType(38, 10)).alias("cobprimamovimientovalue"),
    col("cobprimavalue").cast(DecimalType(38, 10)).alias("cobprimavalue")
).distinct()

# SELECT: riskunitdco
df_riskunitdco = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_riskunitdco").toDF()
df_riskunitdco_select = df_riskunitdco.select("operationpk", "dcoid", "initialdate", "finishdate").distinct()

# SELECT: preriskunits
df_preriskunits = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_preriskunits").toDF()
df_preriskunits_select = df_preriskunits.select("pk", "numunidriesgoinput").distinct()

# SELECT: stag_agreementversion
df_stag_agreementversion = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_stag_agreementversion").toDF()
df_stag_agreementversion_select = df_stag_agreementversion.select("ast_id", "dcoid", "ast_fecini", "ast_fecfin").distinct()

# SELECT: preagreement (Nota: esta tabla ya fue definida arriba, se repite la lógica)
df_preagreement = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_preagreement").toDF()
df_preagreement_select = df_preagreement.select("pk", "unidadnegocioinput").distinct()

# SELECT: reinsuranceoperation
df_reinsuranceoperation = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_reinsuranceoperation").toDF()
df_reinsuranceoperation_select = df_reinsuranceoperation.select("cor_id", "agregatedpolicyid", "operationid", "agregatedriskunitid", "totalpremium").distinct()

# SELECT: reinsuranceoperationgroupinfo
df_reinsuranceoperationgroupinfo = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_reinsuranceoperationgroupinfo").toDF()
df_reinsuranceoperationgroupinfo_select = df_reinsuranceoperationgroupinfo.select("operationid", "reinsurancegroupinfoid", "groupid", "premiumamount").distinct()

# SELECT: reinsuranceoperationcomponent
df_reinsuranceoperationcomponent = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_reinsuranceoperationcomponent").toDF()
df_reinsuranceoperationcomponent_select = df_reinsuranceoperationcomponent.select("reinsurancegroupinfoid", "reinopercomponentid", "contracttype").distinct()

# SELECT: reinsuranceoperationdetail
df_reinsuranceoperationdetail = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_reinsuranceoperationdetail").toDF()
df_reinsuranceoperationdetail_select = df_reinsuranceoperationdetail.select("reinopercomponentid", "thirdpartyid", "thirdpartyname", "rod_proratedpremium", "reinsuranceoperationdetailid").distinct()

# SELECT: ri_reinsurancegroup
df_ri_reinsurancegroup = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_ri_reinsurancegroup").toDF()
df_ri_reinsurancegroup_select = df_ri_reinsurancegroup.select("code", "rekind_id", "description", "reinsurancegroupid", "rig_companycode").distinct()

# SELECT: stri_reinsurancekind
df_stri_reinsurancekind = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_stri_reinsurancekind").toDF()
df_stri_reinsurancekind_select = df_stri_reinsurancekind.select("rekind_id", "rek_code", "rek_description").distinct()

# SELECT: thirdpartyrole
df_thirdpartyrole = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_thirdpartyrole").toDF()
df_thirdpartyrole_select = df_thirdpartyrole.select("thirdpartyid", "iddcorole").distinct()

# SELECT: reasegurador
df_reasegurador = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_reasegurador").toDF()
df_reasegurador_select = df_reasegurador.select("pk", "clasereaseguroinput", "codigoreaseguradorinput").distinct()

# SELECT: stst_systemproperty
df_stst_systemproperty = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_stst_systemproperty").toDF()

# SELECT: stts_role
df_stts_role_select = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_stts_role").toDF()

#SELECT: openitem
df_openitem = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_openitem").toDF()
df_openitem_select = df_openitem.select(col("operationpk"), col("opm_substatus"), col("openitemid"), col("dty_id"), col("amount").cast(DecimalType(38, 10)).alias("amount")).distinct()

#SELECT: stca_doctype
df_stca_doctype = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_stca_doctype").toDF()
df_stca_doctype_select = df_stca_doctype.select("dty_id", "dty_description", "dty_isdebit").distinct()

#SELECT: uaadetail
df_uaadetail = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_uaadetail").toDF()
df_uaadetail_select = df_uaadetail.select(col("openitemid"), col("property").cast(LongType()).alias("property"), col("value").cast(DecimalType(38, 10)).alias("value")).distinct()

#SELECT: property
df_property = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_property").toDF()
df_property_select = df_property.select("propertyid", "simbolo").distinct()

#SELECT: property
df_participacionintermediarios = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_participacionintermediarios").toDF()
df_participacionintermediarios_select = df_participacionintermediarios.select(col("pk"), col("porcentajecomisioninput").cast(DecimalType(38, 10)).alias("porcentajecomisioninput")).distinct()

#-----------------------------------------------------------------#
#--------------------- Filter Sources ----------------------------#
#-----------------------------------------------------------------#  
#FILTER CONTEXTOPERATION FOR PERIOD
df_contextoperation_par_filter = df_contextoperation_par_select.filter((df_contextoperation_par_select["time_stamp_partition"]== PERIODO) & (df_contextoperation_par_select["status"] == 2) & (df_contextoperation_par_select["time_stamp"].between(FECHA_INICIO,FECHA_FIN)))
df_stpo_policyparticipation_filter_tomador = df_stpo_policyparticipation.filter(df_stpo_policyparticipation["rol_id"] == 25935)
df_stpo_policyparticipation_filter_pagador = df_stpo_policyparticipation.filter(df_stpo_policyparticipation["rol_id"] == 33935)
df_contextoperation_par_filter = df_contextoperation_par_filter.filter(df_contextoperation_par_filter["id"] == 990117679)


df_contextoperation_par_filter = df_contextoperation_par_filter.repartition("id")
df_thirdpartymovementpolicy_par_select = df_thirdpartymovementpolicy_par_select.repartition("operationpk")

#-----------------------------------------------------------------#
#--------------------- create view temp --------------------------#
#-----------------------------------------------------------------#  

#---Información
df_contextoperation_par_filter.createOrReplaceTempView("contextoperation_par")
df_thirdpartymovementpolicy_par_select.createOrReplaceTempView("thirdpartymovementpolicy_par")
df_policydco_select.createOrReplaceTempView("policydco")
df_prepolicy_select.createOrReplaceTempView("prepolicy")
df_agregatedpolicy_select.createOrReplaceTempView("agregatedpolicy")
df_product_select.createOrReplaceTempView("product")
df_preproduct_select.createOrReplaceTempView("preproduct")
df_stpo_policyparticipation_select.createOrReplaceTempView("stpo_policyparticipation")

df_stpo_policyparticipation_filter_tomador.createOrReplaceTempView("stpo_policyparticipation_tomador")
df_stpo_policyparticipation_filter_pagador.createOrReplaceTempView("stpo_policyparticipation_pagador")

df_prethirdparty_select.createOrReplaceTempView("prethirdparty")
df_riskunitdco_select.createOrReplaceTempView("riskunitdco")
df_preriskunits_select.createOrReplaceTempView("preriskunits")
df_coveragedco_select.createOrReplaceTempView("coveragedco")
df_cobertura_select.createOrReplaceTempView("cobertura")
df_ri_reinsurancegroup_select.createOrReplaceTempView("ri_reinsurancegroup")
df_stri_reinsurancekind_select.createOrReplaceTempView("stri_reinsurancekind")
df_reinsuranceoperation_select.createOrReplaceTempView("reinsuranceoperation")
df_reinsuranceoperationgroupinfo_select.createOrReplaceTempView("reinsuranceoperationgroupinfo")
df_reinsuranceoperationcomponent_select.createOrReplaceTempView("reinsuranceoperationcomponent")
df_reinsuranceoperationdetail_select.createOrReplaceTempView("reinsuranceoperationdetail")
df_thirdpartyrole_select.createOrReplaceTempView("thirdpartyrole")
df_reasegurador_select.createOrReplaceTempView("reasegurador")
df_stps_relationagreementpolicy_select.createOrReplaceTempView("stps_relationagreementpolicy")
df_stps_agreementstaticdat_select.createOrReplaceTempView("stps_agreementstaticdat")
df_stag_agreementversion_select.createOrReplaceTempView("stag_agreementversion")
df_preagreement_select.createOrReplaceTempView("preagreement")

#---Operación
df_stpo_policyparticipationdco_select.createOrReplaceTempView("stpo_policyparticipationdco")
df_participacionintermediarios_select.createOrReplaceTempView("participacionintermediarios")
df_openitem_select.createOrReplaceTempView("openitem")
df_stca_doctype_select.createOrReplaceTempView("stca_doctype")
df_uaadetail_select.createOrReplaceTempView("uaadetail")
df_property_select.createOrReplaceTempView("property")


df_query_informacion = spark.sql (""" 
    
    SELECT
        'ASCEL-E'                                           as ORIGEN
        , ctx.id                                            as id         
        , tpmp.operationpk 					                as tpmp_operationpk
        , ppd.companiainput                                 as COMPANIA
        , cb.cobramocontableinput                           as RAMO_CONTABLE
        , rkc.rek_description                               as DESC_RAMO_CONTABLE
        , ppd.ramovalue                                     as RAMO_TECNICO
        , ppd.ramoinput                                     as DESC_RAMO_TECNICO 
        , rk.rek_code                                       as RAMO_REASEGURO
        , rk.rek_description                                as DESC_RAMO_REASEGURO 
        , rg.code                                           as GRUPO_REASEGURO
        , pd.productid                                      as PRODUCTO
        , pd.description                                    as DESC_PRODUCTO
        , st.ast_numeroacu					                as POLIZA_MATRIZ
        , pre.numeropolizainput				                as POLIZA
        , pre.oficinainput				                    as SUCURSAL
        , pru.numunidriesgoinput                            as UNIDAD_RIESGO
        , cb.cobcodigocoberturainput                        as COBERTURA
        , cb.cobdescripcioncobertinput                      as DESC_COBERTURA
        , pdco.operationpk                                  as ENDOSO
        , ctx.time_stamp                                    as FECHA_ENDOSO
        , prea.unidadnegocioinput                           as UNIDAD_NEGOCIO
        , pdco.initialdate                                  as FECHA_INICIO_POLIZA_MATRIZ
        , pdco.finishdate                                   as FECHA_FIN_POLIZA_MATRIZ
        , rskdco.initialdate                                as FECHA_INICIO_COBERTURA
        , rskdco.finishdate                                 as FECHA_FIN_COBERTURA
        , pp_tomador.thirdpartyid			                as ID_TOMADOR_POLIZA      
        , ptp_tomador.nombrepredeterminadoinput             as TOMADOR_POLIZA 
        , pp_pagador.thirdpartyid			                as ID_TOMADOR_POLIZA_MATRIZ 
        , ptp_pagador.nombrepredeterminadoinput             as TOMADOR_POLIZA_MATRIZ 
        , pre.frecuenciapagoinput                           as FRECUENCIA_PAGO
        , array_agg( distinct ra.codigoreaseguradorinput)   as CODREASEG
        , max(ra.clasereaseguroinput)                       as CLASE_REASEGURADOR 
        , array_agg(distinct rod.thirdpartyname)            as NOMREASEG
        , date_trunc('month', ctx.time_stamp)               as PER_FEC_INI_VIG_OPE
        , last_day(ctx.time_stamp)                 as PER_FEC_FIN_VIG_OPE
        , 0                                                 as ID_LOTE_CARGUE
        , ctx.time_stamp_partition                          as PERIODO
    	
    FROM
        contextoperation_par ctx
    INNER JOIN 
        thirdpartymovementpolicy_par tpmp ON ctx.id = tpmp.operationpk
    INNER JOIN 
        policydco pdco ON ctx.id = pdco.operationpk
    INNER JOIN
        prepolicy pre ON pdco.dcoid = pre.pk
    
    --PRODUCT      
    INNER JOIN 
        agregatedpolicy ap ON pdco.agregatedobjectid = ap.agregatedpolicyid
    INNER JOIN 
        product pd ON ap.productid = pd.productid
    INNER JOIN
        preproduct ppd ON pd.pro_dcoid = ppd.pk
        
    --ROL_TOMADOR
    INNER JOIN
        stpo_policyparticipation_tomador pp_tomador ON tpmp.THIRDPARTY = pp_tomador.thirdpartyid
    INNER JOIN 
        prethirdparty ptp_tomador ON pp_tomador.thirdpartyid = ptp_tomador.static
    
    --ROL_PAGADOR
    LEFT JOIN
        stpo_policyparticipation_pagador pp_pagador ON tpmp.THIRDPARTY = pp_pagador.thirdpartyid
    LEFT JOIN 
        prethirdparty ptp_pagador ON pp_pagador.thirdpartyid = ptp_pagador.static
        
    --RIESGO
    INNER JOIN 
        riskunitdco rskdco ON ctx.id = rskdco.operationpk
    INNER JOIN 
        preriskunits pru ON rskdco.dcoid = pru.pk
        
    --COBERTURA/POLIZA EN LEFT PORQUE NO TENEMOS COVERAGEDCO COMPLETA
    INNER JOIN 
        coveragedco cdco ON ctx.id = cdco.operationpk
    INNER JOIN 
        cobertura cb  ON cdco.dcoid = cb.pk 
    
    
    --REASEGURO RAMO    
    INNER JOIN
        ri_reinsurancegroup rg ON rg.code = (replace(cb.cobramoreaseguroinput, '.0', ''))
    INNER JOIN
        stri_reinsurancekind rk ON rg.rekind_id = rk.rekind_id
    LEFT JOIN 
        stri_reinsurancekind rkc ON (replace(cb.cobramocontableinput, '.0', '')) = rkc.rek_code
    
    --REASEGURO DETAIL
    INNER JOIN 
        reinsuranceoperation ro ON ro.cor_id = pdco.operationpk and ro.agregatedpolicyid = pdco.agregatedobjectid
    INNER JOIN 
        reinsuranceoperationgroupinfo rog ON rog.operationid = ro.operationid
    INNER JOIN 
        ri_reinsurancegroup ON rg.reinsurancegroupid = rog.groupid
    INNER JOIN 
        reinsuranceoperationcomponent roc ON roc.reinsurancegroupinfoid = rog.reinsurancegroupinfoid
    INNER JOIN 
        reinsuranceoperationdetail rod ON rod.reinopercomponentid = roc.reinopercomponentid
    INNER JOIN 
        thirdpartyrole tpr ON tpr.thirdpartyid = rod.thirdpartyid
    INNER JOIN 
        reasegurador ra  ON tpr.iddcorole = ra.pk
    
    --ACUERDO
    INNER JOIN 
        stps_relationagreementpolicy sr ON pre.static = sr.rsa_policyid
    INNER JOIN 
        stps_agreementstaticdat st ON sr.rsa_ast_id = st.ast_id
    INNER JOIN 
        stag_agreementversion av ON sr.rsa_ast_id = av.ast_id
    INNER JOIN 
        preagreement prea ON av.dcoid = prea.pk
    
    GROUP BY
         ctx.id                                 
        , tpmp.operationpk 					    
        , ppd.companiainput                     
        , cb.cobramocontableinput               
        , rkc.rek_description                   
        , ppd.ramovalue                         
        , ppd.ramoinput                         
        , rk.rek_code                           
        , rk.rek_description                    
        , rg.code                               
        , pd.productid                          
        , pd.description                        
        , st.ast_numeroacu					 
        , pre.numeropolizainput				 
        , pre.oficinainput				  
        , pru.numunidriesgoinput                
        , cb.cobcodigocoberturainput            
        , cb.cobdescripcioncobertinput          
        , pdco.operationpk                      
        , ctx.time_stamp                        
        , prea.unidadnegocioinput               
        , pdco.initialdate                      
        , pdco.finishdate                       
        , rskdco.initialdate                    
        , rskdco.finishdate                     
        , pp_tomador.thirdpartyid			    
        , ptp_tomador.nombrepredeterminadoinput 
        , pp_pagador.thirdpartyid			    
        , ptp_pagador.nombrepredeterminadoinput 
        , pre.frecuenciapagoinput               
        , ctx.time_stamp_partition              
        , date_trunc('month', ctx.time_stamp)               
        , last_day(ctx.time_stamp)               
""")

df_query_informacion_repartition = df_query_informacion.repartition(200)


logger.info(f"Writing to PRUEBA_MODULAR with period = {PERIODO}")
# Write to the original path as well for compatibility
df_query_informacion_repartition.write.mode("overwrite").partitionBy("PERIODO").parquet("s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL/PRUEBA_MODULAR/")

