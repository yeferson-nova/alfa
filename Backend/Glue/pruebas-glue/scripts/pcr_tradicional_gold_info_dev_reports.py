"""Script that load bronce phase"""

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, LongType
from pyspark.sql.functions import lit,col, split, explode, trim, lower, regexp_replace, last_day, date_trunc

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
        'PERIODO',
        'FECHA_INICIO',
        'FECHA_FIN',
        'CERTIFICATE',
        'DB_CATALOG_GLUE'
        ]
    )
BUCKET_NAME = args['BUCKET_NAME']
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
DB_NAME = args['DB_NAME']
DB_SCHEMA = args['DB_SCHEMA']
PERIODO = args['PERIODO']
FECHA_INICIO = args['FECHA_INICIO']
FECHA_FIN = args['FECHA_FIN']
CERTIFICATE = args['CERTIFICATE']
DB_CATALOG_GLUE = args['DB_CATALOG_GLUE']

logger.info(f"Period: {PERIODO}")

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
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "500")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
spark.conf.set("spark.default.parallelism", "200")

extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME, DB_NAME)

DB_CATALOG_GLUE = 'datalake-dev-silver-catalog-db'

logger.info(f"Period: {PERIODO} and {FECHA_INICIO} to {FECHA_FIN}")
logger.info(f"Start extraction from DB_CATALOG_GLUE_SILVER: {DB_CATALOG_GLUE}")


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

logger.info(f"start load source from: {DB_CATALOG_GLUE}")

# SELECT df_contextoperation_par
df_contextoperation_par = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_contextoperation").toDF()
df_contextoperation_par_select = df_contextoperation_par.select("id", "time_stamp", "status", "time_stamp_partition")

# SELECT: df_thirdpartymovementpolicy_par
df_thirdpartymovementpolicy_par = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_thirdpartymovementpolicy").toDF()
df_thirdpartymovementpolicy_par_select = df_thirdpartymovementpolicy_par.select("operationpk", "policyid", col("thirdparty").cast("bigint").alias("thirdparty")).distinct()

# SELECT: prepolicy
df_prepolicy = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_prepolicy").toDF()
df_prepolicy_select = df_prepolicy.select("pk", "static", "numeropolizainput", "tipopolizavalue", "tipopolizainput", "oficinainput", "frecuenciapagoinput").distinct()

# SELECT: policydco
df_policydco = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_policydco").toDF()
df_policydco_select = df_policydco.select("agregatedobjectid", "dcoid", "operationpk", "initialdate", "finishdate").distinct()

# SELECT: agregatedpolicy
df_agregatedpolicy = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_agregatedpolicy").toDF()
df_agregatedpolicy_select = df_agregatedpolicy.select("agregatedpolicyid", "productid", "operationpk").distinct()

# SELECT: product
df_product = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_product").toDF()
df_product_select = df_product.select("pro_dcoid", "productid", "description").distinct()

# SELECT: preproduct
df_preproduct = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_preproduct").toDF()
df_preproduct_select = df_preproduct.select("pk", "companiainput", "ramoinput", "codigoproductoinput", "descripcionproductoinput", "ramovalue").distinct()

# SELECT: stpo_policyparticipation
df_stpo_policyparticipation = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_stpo_policyparticipation").toDF()
df_stpo_policyparticipation_select = df_stpo_policyparticipation.select("thirdpartyid", "rol_id", "agregatedobjectid", "operationpk").distinct()


# SELECT: stps_relationagreementpolicy
df_stps_relationagreementpolicy = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_stps_relationagreementpolicy").toDF()
df_stps_relationagreementpolicy_select = df_stps_relationagreementpolicy.select("rsa_policyid", "rsa_ast_id").distinct()

# SELECT: stps_agreementstaticdat
df_stps_agreementstaticdat = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_stps_agreementstaticdat").toDF()
df_stps_agreementstaticdat_select = df_stps_agreementstaticdat.select("ast_id", "ast_numeroacu").distinct()

# SELECT: prethirdparty
df_prethirdparty = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_prethirdparty").toDF()
#df_prethirdparty_select = df_prethirdparty.select("static", "nombrepredeterminadoinput", "tipodocumentoinput", "tipodocumentovalue", "documentoidentidadinput").distinct()
df_prethirdparty_select = df_prethirdparty.select(col("static").cast("bigint").alias("static"), "nombrepredeterminadoinput").distinct()

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



#-----------------------------------------------------------------#
#--------------------- Filter Sources ----------------------------#
#-----------------------------------------------------------------#  
#FILTER CONTEXTOPERATION FOR PERIOD
df_contextoperation_par_filter = df_contextoperation_par_select.filter((df_contextoperation_par_select["time_stamp_partition"]== PERIODO) & (df_contextoperation_par_select["status"] == 2) & (df_contextoperation_par_select["time_stamp"].between(FECHA_INICIO,FECHA_FIN)))
df_stpo_policyparticipation_filter_tomador = df_stpo_policyparticipation.filter(df_stpo_policyparticipation["rol_id"] == 25935)
df_stpo_policyparticipation_filter_pagador = df_stpo_policyparticipation.filter(df_stpo_policyparticipation["rol_id"] == 33935)
#df_contextoperation_par_filter = df_contextoperation_par_filter.filter(df_contextoperation_par_filter["id"] == 990117679)


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

#df_prethirdparty_select.createOrReplaceTempView("prethirdparty")
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


tmp_tomador_filter_path  = f"s3://{BUCKET_NAME}/GOLD/NIIF/PCR_TRADICIONAL_TMP/tomador_filter_tmp"
df_stpo_policyparticipation_filter_tomador.write.mode("overwrite").parquet(tmp_tomador_filter_path)
stpo_policyparticipation_tomador = spark.read.parquet(tmp_tomador_filter_path).repartition(200, "operationpk")  
stpo_policyparticipation_tomador.createOrReplaceTempView("stpo_policyparticipation_tomador")

tmp_pagador_filter_path  = f"s3://{BUCKET_NAME}/GOLD/NIIF/PCR_TRADICIONAL_TMP/pagador_filter_tmp"
df_stpo_policyparticipation_filter_pagador.write.mode("overwrite").parquet(tmp_pagador_filter_path)
stpo_policyparticipation_pagador = spark.read.parquet(tmp_pagador_filter_path).repartition(200, "operationpk")  
stpo_policyparticipation_pagador.createOrReplaceTempView("stpo_policyparticipation_pagador")


tmp_prethirdparty_order_path  = f"s3://{BUCKET_NAME}/GOLD/NIIF/PCR_TRADICIONAL_TMP/prethirdparty_order_tmp"
df_prethirdparty_select.write.mode("overwrite").parquet(tmp_prethirdparty_order_path)
prethirdparty = spark.read.parquet(tmp_prethirdparty_order_path).repartition(200, "static")  
prethirdparty.createOrReplaceTempView("prethirdparty")

logger.info(f"All sources its created")

#-----------------------------------------------------------------#
#---------------------- df_base_logic ----------------------------#
#-----------------------------------------------------------------#

df_base_metadata = spark.sql("""
SELECT
    ctx.id, ctx.time_stamp, ctx.time_stamp_partition, tpmp.operationpk, tpmp.THIRDPARTY, pdco.operationpk as pdco_operationpk, pdco.initialdate, pdco.finishdate, pdco.dcoid, pdco.agregatedobjectid,pre.numeropolizainput,pre.oficinainput,pre.frecuenciapagoinput, pre.pk, pre.static
FROM
    contextoperation_par ctx
INNER JOIN 
    thirdpartymovementpolicy_par tpmp ON ctx.id = tpmp.operationpk
INNER JOIN 
    policydco pdco ON ctx.id = pdco.operationpk
INNER JOIN
    prepolicy pre ON pdco.dcoid = pre.pk
    
GROUP BY ctx.id, ctx.time_stamp, ctx.time_stamp_partition, tpmp.operationpk, tpmp.THIRDPARTY, pdco.operationpk, pdco.initialdate, pdco.finishdate, pdco.dcoid, pdco.agregatedobjectid,pre.numeropolizainput,pre.oficinainput,pre.frecuenciapagoinput, pre.pk, pre.static        

""")

tmp_base_path  = f"s3://{BUCKET_NAME}/GOLD/NIIF/PCR_TRADICIONAL_TMP/base_tmp"
df_base_metadata.write.mode("overwrite").parquet(tmp_base_path)
df_base_safe = spark.read.parquet(tmp_base_path).repartition(200, "id")  
df_base_safe.createOrReplaceTempView("df_base_safe")
logger.info(f"create view df_base from s3 tmp")


#-----------------------------------------------------------------#
#---------------------- df_product_logic -------------------------#
#-----------------------------------------------------------------#
df_product_metadata = spark.sql("""
SELECT bse.id, bse.agregatedobjectid, pd.productid, pd.description, ppd.companiainput, ppd.ramovalue, ppd.ramoinput 
    FROM df_base_safe bse
    --PRODUCT      
    INNER JOIN 
        agregatedpolicy ap ON bse.agregatedobjectid = ap.agregatedpolicyid
    INNER JOIN 
        product pd ON ap.productid = pd.productid
    INNER JOIN
        preproduct ppd ON pd.pro_dcoid = ppd.pk
    GROUP BY bse.id, bse.agregatedobjectid, pd.productid, pd.description, ppd.companiainput, ppd.ramovalue, ppd.ramoinput 
""")

tmp_product_path  = f"s3://{BUCKET_NAME}/GOLD/NIIF/PCR_TRADICIONAL_TMP/product_tmp"
df_product_metadata.write.mode("overwrite").parquet(tmp_product_path)
df_product_safe = spark.read.parquet(tmp_product_path).repartition(200, "id")  
df_product_safe.createOrReplaceTempView("df_product_safe")

logger.info(f"create view df_product from s3 tmp")

#-----------------------------------------------------------------#
#---------------------- df_tomador_logic -------------------------#
#-----------------------------------------------------------------#
df_tomador_metadata = spark.sql ("""
SELECT 
bse.id, bse.THIRDPARTY, pp_tomador.thirdpartyid as thirdpartyid_tomador, ptp_tomador.nombrepredeterminadoinput as nombrepredeterminadoinput_tomador

FROM df_base_safe bse

--AGREGATEDPOLICY
INNER JOIN 
    agregatedpolicy ap ON bse.agregatedobjectid = ap.agregatedpolicyid
--ROL_TOMADOR
INNER JOIN
    stpo_policyparticipation_tomador pp_tomador ON ap.operationpk = pp_tomador.operationpk
INNER JOIN 
    prethirdparty ptp_tomador ON pp_tomador.thirdpartyid = ptp_tomador.static

GROUP BY 
    bse.id, bse.THIRDPARTY, pp_tomador.thirdpartyid, ptp_tomador.nombrepredeterminadoinput

""")

tmp_tomador_path  = f"s3://{BUCKET_NAME}/GOLD/NIIF/PCR_TRADICIONAL_TMP/tomador_tmp"
df_tomador_metadata = df_tomador_metadata.repartition(200)
df_tomador_metadata.write.mode("overwrite").parquet(tmp_tomador_path)
df_tomador_safe = spark.read.parquet(tmp_tomador_path).repartition(200, "id")  
df_tomador_safe.createOrReplaceTempView("df_tomador_safe")

logger.info(f"create view df_tomador_safe from s3 tmp")

#-----------------------------------------------------------------#
#---------------------- df_pagador_logic -------------------------#
#-----------------------------------------------------------------#
df_pagador_metadata = spark.sql ("""
SELECT
bse.id, bse.THIRDPARTY, pp_pagador.thirdpartyid as thirdpartyid_pagador, ptp_pagador.nombrepredeterminadoinput as nombrepredeterminadoinput_pagador

FROM df_base_safe bse

--AGREGATEDPOLICY
INNER JOIN 
    agregatedpolicy ap ON bse.agregatedobjectid = ap.agregatedpolicyid
-- ROL_PAGADOR
INNER JOIN
    stpo_policyparticipation_pagador pp_pagador ON ap.operationpk = pp_pagador.operationpk
INNER JOIN
    prethirdparty ptp_pagador ON pp_pagador.thirdpartyid = ptp_pagador.static

GROUP BY 
    bse.id, bse.THIRDPARTY, pp_pagador.thirdpartyid, ptp_pagador.nombrepredeterminadoinput
""")

tmp_pagador_path  = f"s3://{BUCKET_NAME}/GOLD/NIIF/PCR_TRADICIONAL_TMP/pagador_tmp"
df_pagador_metadata = df_pagador_metadata.repartition(200)
df_pagador_metadata.write.mode("overwrite").parquet(tmp_pagador_path)

df_pagador_safe = spark.read.parquet(tmp_pagador_path).repartition(200, "id")  
df_pagador_safe.createOrReplaceTempView("df_pagador_safe")

logger.info(f"create view df_pagador_safe from s3 tmp")

#-----------------------------------------------------------------#
#---------------------- df_riesgo_logic --------------------------#
#-----------------------------------------------------------------#

df_riesgo_metadata = spark.sql ("""
SELECT 
bse.id, rskdco.initialdate, rskdco.finishdate, pru.numunidriesgoinput 

FROM df_base_safe bse

--RIESGO
INNER JOIN 
    riskunitdco rskdco ON bse.id = rskdco.operationpk
INNER JOIN 
    preriskunits pru ON rskdco.dcoid = pru.pk

GROUP BY 
    bse.id, rskdco.initialdate, rskdco.finishdate, pru.numunidriesgoinput 
""")

tmp_riesgo_path  = f"s3://{BUCKET_NAME}/GOLD/NIIF/PCR_TRADICIONAL_TMP/riesgo_tmp"
df_riesgo_metadata.write.mode("overwrite").parquet(tmp_riesgo_path)
df_riesgo_safe = spark.read.parquet(tmp_riesgo_path).repartition(200, "id")  
df_riesgo_safe.createOrReplaceTempView("df_riesgo_safe")

logger.info(f"create view df_riesgo_safe from s3 tmp")

#-----------------------------------------------------------------#
#---------------------- df_acuerdo_logic -------------------------#
#-----------------------------------------------------------------#

df_acuerdo_metadata = spark.sql ("""
SELECT 
bse.id, st.ast_numeroacu, prea.unidadnegocioinput 

FROM df_base_safe bse

--ACUERDO
INNER JOIN 
    stps_relationagreementpolicy sr ON bse.static = sr.rsa_policyid
INNER JOIN 
    stps_agreementstaticdat st ON sr.rsa_ast_id = st.ast_id
INNER JOIN 
    stag_agreementversion av ON sr.rsa_ast_id = av.ast_id
INNER JOIN 
    preagreement prea ON av.dcoid = prea.pk


GROUP BY 
    bse.id, st.ast_numeroacu, prea.unidadnegocioinput 
""")

tmp_acuerdo_path  = f"s3://{BUCKET_NAME}/GOLD/NIIF/PCR_TRADICIONAL_TMP/acuerdo_tmp"
df_acuerdo_metadata.write.mode("overwrite").parquet(tmp_acuerdo_path)
df_acuerdo_safe = spark.read.parquet(tmp_acuerdo_path).repartition(200, "id")  
df_acuerdo_safe.createOrReplaceTempView("df_acuerdo_safe")

logger.info(f"create view df_acuerdo_safe from s3 tmp")


#-----------------------------------------------------------------#
#---------------------- df_cobertura_logic -----------------------#
#-----------------------------------------------------------------#


df_cobertura_metadata = spark.sql (""" 

SELECT bse.id, cb.cobramocontableinput, cb.cobcodigocoberturainput, cb.cobdescripcioncobertinput, rk.rek_code,  rk.rek_description, rkc.rek_description as rkc_rek_description, rg.code     

FROM
    df_base_safe bse
    
--COBERTURA/POLIZA EN LEFT PORQUE NO TENEMOS COVERAGEDCO COMPLETA
INNER JOIN 
    coveragedco cdco ON bse.id = cdco.operationpk
INNER JOIN 
    cobertura cb  ON cdco.dcoid = cb.pk     
    
--REASEGURO RAMO    
INNER JOIN
    ri_reinsurancegroup rg ON rg.code = cb.cobramoreaseguroinput
INNER JOIN
    stri_reinsurancekind rk ON rg.rekind_id = rk.rekind_id
LEFT JOIN 
    stri_reinsurancekind rkc ON cb.cobramocontableinput = rkc.rek_code     
GROUP BY 
    bse.id, cb.cobramocontableinput, cb.cobcodigocoberturainput, cb.cobdescripcioncobertinput, rk.rek_code,  rk.rek_description, rkc.rek_description , rg.code  
    
""")


tmp_cobertura_path  = f"s3://{BUCKET_NAME}/GOLD/NIIF/PCR_TRADICIONAL_TMP/cobertura_tmp"
df_cobertura_metadata.write.mode("overwrite").parquet(tmp_cobertura_path)
df_cobertura_safe = spark.read.parquet(tmp_cobertura_path).repartition(500, "id")  
df_cobertura_safe.createOrReplaceTempView("df_cobertura_safe")

logger.info(f"create view df_cobertura from s3 tmp")

#-----------------------------------------------------------------#
#---------------------- df_reaseguro_logic -----------------------#
#-----------------------------------------------------------------#

df_reaseguro_metadata = spark.sql (""" 

SELECT bse.id, rod.thirdpartyname, ra.codigoreaseguradorinput, ra.clasereaseguroinput

FROM  df_base_safe bse
INNER JOIN 
    reinsuranceoperation ro ON ro.cor_id = bse.pdco_operationpk and ro.agregatedpolicyid = bse.agregatedobjectid
INNER JOIN 
    reinsuranceoperationgroupinfo rog ON rog.operationid = ro.operationid
INNER JOIN 
    ri_reinsurancegroup rg ON rg.reinsurancegroupid = rog.groupid
INNER JOIN 
    reinsuranceoperationcomponent roc ON roc.reinsurancegroupinfoid = rog.reinsurancegroupinfoid
INNER JOIN 
    reinsuranceoperationdetail rod ON rod.reinopercomponentid = roc.reinopercomponentid
INNER JOIN 
    thirdpartyrole tpr ON tpr.thirdpartyid = rod.thirdpartyid
INNER JOIN 
    reasegurador ra  ON tpr.iddcorole = ra.pk

""")
tmp_reaseguro_path  = f"s3://{BUCKET_NAME}/GOLD/NIIF/PCR_TRADICIONAL_TMP/reaseguro_tmp"
df_reaseguro_metadata.write.mode("overwrite").parquet(tmp_reaseguro_path)
df_reaseguro_safe = spark.read.parquet(tmp_reaseguro_path).repartition(500, "id")  
df_reaseguro_safe.createOrReplaceTempView("df_reaseguro_safe")

logger.info(f"create view df_reaseguro from s3 tmp")



#-----------------------------------------------------------------#
#----------------df_reaseguro_group_logic -----------------------#
#-----------------------------------------------------------------#

df_reaseguro_group_metadata = spark.sql (""" 

SELECT bse.id, collect_set(thirdpartyname) as thirdpartyname, collect_set(codigoreaseguradorinput) as codigoreaseguradorinput, max(clasereaseguroinput) as clasereaseguroinput

FROM  df_reaseguro_safe bse

GROUP BY 
    id
""")

tmp_reaseguro_group_path  = f"s3://{BUCKET_NAME}/GOLD/NIIF/PCR_TRADICIONAL_TMP/reaseguro_group_tmp"
df_reaseguro_group_metadata.write.mode("overwrite").parquet(tmp_reaseguro_group_path)
df_reaseguro_group_safe = spark.read.parquet(tmp_reaseguro_group_path).repartition(500, "id")  
df_reaseguro_group_safe.createOrReplaceTempView("df_reaseguro_group_safe")

logger.info(f"create view df_reaseguro_group from s3 tmp")

logger.info(f"Count data from {df_base_safe.count()}")





logger.info(f"start with logic info gold")


df_query_pre_informacion_metadata = (
    df_base_safe.alias("bse")
    .join(df_product_safe.alias("prod"), col("prod.id") == col("bse.id"), "inner")
    .join(df_tomador_safe.alias("tom"), col("tom.id") == col("bse.id"), "inner")
    .join(df_pagador_safe.alias("pag"), col("pag.id") == col("bse.id"), "left")
    .join(df_riesgo_safe.alias("ries"), col("ries.id") == col("bse.id"), "inner")
    .join(df_cobertura_safe.alias("cb"), col("cb.id") == col("bse.id"), "inner")
    .join(df_acuerdo_safe.alias("acd"), col("acd.id") == col("bse.id"), "inner")
    .select(
        lit("ASCEL-E").alias("ORIGEN"),
        col("bse.id").alias("ID"),
        col("bse.operationpk").alias("OPERACION"),
        col("prod.companiainput").alias("COMPANIA"),
        col("cb.cobramocontableinput").alias("RAMO_CONTABLE"),
        col("cb.rkc_rek_description").alias("DESC_RAMO_CONTABLE"),
        col("prod.ramovalue").alias("RAMO_TECNICO"),
        col("prod.ramoinput").alias("DESC_RAMO_TECNICO"),
        col("cb.rek_code").alias("RAMO_REASEGURO"),
        col("cb.rek_description").alias("DESC_RAMO_REASEGURO"),
        col("cb.code").alias("GRUPO_REASEGURO"),
        col("prod.productid").alias("PRODUCTO"),
        col("prod.description").alias("DESC_PRODUCTO"),
        col("acd.ast_numeroacu").alias("POLIZA_MATRIZ"),
        col("bse.numeropolizainput").alias("POLIZA"),
        col("bse.oficinainput").alias("SUCURSAL"),
        col("ries.numunidriesgoinput").alias("UNIDAD_RIESGO"),
        col("cb.cobcodigocoberturainput").alias("COBERTURA"),
        col("cb.cobdescripcioncobertinput").alias("DESC_COBERTURA"),
        col("bse.operationpk").alias("ENDOSO"),
        col("bse.time_stamp").alias("FECHA_ENDOSO"),
        col("acd.unidadnegocioinput").alias("UNIDAD_NEGOCIO"),
        col("bse.initialdate").alias("FECHA_INICIO_POLIZA_MATRIZ"),
        col("bse.finishdate").alias("FECHA_FIN_POLIZA_MATRIZ"),
        col("ries.initialdate").alias("FECHA_INICIO_COBERTURA"),
        col("ries.finishdate").alias("FECHA_FIN_COBERTURA"),
        col("tom.thirdpartyid_tomador").alias("ID_TOMADOR_POLIZA"),
        col("tom.nombrepredeterminadoinput_tomador").alias("TOMADOR_POLIZA"),
        col("pag.thirdpartyid_pagador").alias("ID_TOMADOR_POLIZA_MATRIZ"),
        col("pag.nombrepredeterminadoinput_pagador").alias("TOMADOR_POLIZA_MATRIZ"),
        col("bse.frecuenciapagoinput").alias("FRECUENCIA_PAGO"),
        col("bse.time_stamp_partition").alias("PERIODO"),
        date_trunc("month", col("bse.time_stamp")).alias("PER_FEC_INI_VIG_OPE"),
        last_day("bse.time_stamp").alias("PER_FEC_FIN_VIG_OPE"),
        lit(0).alias("ID_LOTE_CARGUE")
    )
)

df_query_pre_informacion_metadata = df_query_pre_informacion_metadata.repartition(1200, "id")
df_query_pre_informacion_path  = f"s3://{BUCKET_NAME}/GOLD/NIIF/PCR_TRADICIONAL_TMP/query_pre_informacion_tmp"
df_query_pre_informacion_metadata.write.mode("overwrite").parquet(df_query_pre_informacion_path)
df_query_pre_informacion = spark.read.parquet(df_query_pre_informacion_path).repartition(800, "id")  
df_query_pre_informacion.createOrReplaceTempView("query_pre_informacion")

logger.info(f"create view preview df_query_informacion from s3 tmp")

df_query_informacion = spark.sql("""

    SELECT
        qpi.ORIGEN
        ,qpi.ID
        ,qpi.OPERACION
        ,qpi.COMPANIA
        ,qpi.RAMO_CONTABLE
        ,qpi.DESC_RAMO_CONTABLE
        ,qpi.RAMO_TECNICO
        ,qpi.DESC_RAMO_TECNICO
        ,qpi.RAMO_REASEGURO
        ,qpi.DESC_RAMO_REASEGURO
        ,qpi.GRUPO_REASEGURO
        ,qpi.PRODUCTO
        ,qpi.DESC_PRODUCTO
        ,qpi.POLIZA_MATRIZ
        ,qpi.POLIZA
        ,qpi.SUCURSAL
        ,qpi.UNIDAD_RIESGO
        ,qpi.COBERTURA
        ,qpi.DESC_COBERTURA
        ,qpi.ENDOSO
        ,qpi.FECHA_ENDOSO
        ,qpi.UNIDAD_NEGOCIO
        ,qpi.FECHA_INICIO_POLIZA_MATRIZ
        ,qpi.FECHA_FIN_POLIZA_MATRIZ
        ,qpi.FECHA_INICIO_COBERTURA
        ,qpi.FECHA_FIN_COBERTURA
        ,qpi.ID_TOMADOR_POLIZA
        ,qpi.TOMADOR_POLIZA
        ,qpi.ID_TOMADOR_POLIZA_MATRIZ
        ,qpi.TOMADOR_POLIZA_MATRIZ
        ,qpi.FRECUENCIA_PAGO
        ,qpi.PER_FEC_INI_VIG_OPE
        ,qpi.PER_FEC_FIN_VIG_OPE
        ,rsg.thirdpartyname           as NOMREASEG
        ,rsg.codigoreaseguradorinput  as CODREASEG
        ,rsg.clasereaseguroinput      as CLASE_REASEGURADOR
        ,qpi.ID_LOTE_CARGUE
        ,qpi.PERIODO
        FROM 
            query_pre_informacion qpi
        INNER JOIN 
            df_reaseguro_group_safe rsg ON rsg.id = qpi.id    
""")

df_query_informacion = df_query_informacion.repartition(200, "id")

# Deduplicar por las 7 columnas definidas como clave de unicidad
df_query_informacion_unique = df_query_informacion.dropDuplicates(["operacion","ramo_contable","ramo_tecnico","ramo_reaseguro", "producto", "poliza", "cobertura"])

logger.info(f"Writing to GOLD PCR_TRADICIONAL with period = {PERIODO}")

df_query_informacion_unique.write.mode("overwrite").partitionBy("PERIODO").option("compression", "snappy").parquet(f"s3://{BUCKET_NAME}/GOLD/NIIF/PCR_TRADICIONAL_TMP/pcr_info_tmp/")

logger.info(f"Finished Write to tmp GOLD INFO PCR_TRADICIONAL with period = {PERIODO}")

