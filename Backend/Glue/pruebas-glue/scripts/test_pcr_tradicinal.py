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
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
from datetime import datetime, timedelta
FECHA_INICIO = "2025-01-01"
FECHA_FIN = "2025-01-31"

inicio = datetime.strptime(FECHA_INICIO, "%Y-%m-%d")
fin = datetime.strptime(FECHA_FIN, "%Y-%m-%d")

dias = 6

rangos = [
    (
        (ini := inicio + timedelta(days=i)).strftime("%Y-%m-%d"),
        min(ini + timedelta(days=dias-1), fin).strftime("%Y-%m-%d")
    )
    for i in range(0, (fin - inicio).days + 1, dias)
]

for r in rangos:
    print(r[1])
niif_pcr_tradicional_informacion = spark.read.parquet("s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL_INFO/PERIODO=2025-01/").repartition(200)  # repartition aquí es razonable
niif_pcr_tradicional_informacion.createOrReplaceTempView("niif_pcr_tradicional_informacion")
niif_pcr_tradicional_operativo = spark.read.parquet("s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL/PRUEBA_MODULAR_OPERATIVO/").repartition(200)  # repartition aquí es razonable
niif_pcr_tradicional_operativo.createOrReplaceTempView("niif_pcr_tradicional_operativo")
df_test = spark.sql ("""
SELECT id, cod_cobertura, ramo_contable, ramo_reaseguro FROM niif_pcr_tradicional_operativo WHERE id = 987418034
""").show()
df_test = spark.sql ("""
SELECT id, cobertura, ramo_contable, ramo_reaseguro FROM niif_pcr_tradicional_informacion WHERE id = 987418034
""").show()
niif_pcr_tradicional_informacion.printSchema()
df_query_pcr_tradicional_gold = spark.sql ("""
SELECT
     info.ORIGEN
    ,info.id                                                        as operacion --Nuevo
    ,info.COMPANIA
    ,info.RAMO_CONTABLE                                                          --cambia nombre por ramo contable
    ,info.DESC_RAMO_CONTABLE                                                     --cambia nombre por desciprcion ramo contable
    ,info.RAMO_TECNICO                                                           --Se cambia de posicion ramo_tecnico
    ,info.DESC_RAMO_TECNICO                                                      --Se cambia de posicion  ramo_tecnico
    ,info.RAMO_REASEGURO                                                         --Se agrega ramo_reaseguro
    ,info.DESC_RAMO_REASEGURO                                                    --Se agrega ramo_reaseguro
    ,info.GRUPO_REASEGURO                                                        --Se agrega ramo_reaseguro
    ,info.PRODUCTO
    ,info.DESC_PRODUCTO
    ,info.POLIZA_MATRIZ
    ,info.POLIZA
    ,info.SUCURSAL
    ,info.UNIDAD_RIESGO
    ,info.COBERTURA
    ,info.DESC_COBERTURA
    ,info.ENDOSO
    ,info.FECHA_ENDOSO
    ,info.UNIDAD_NEGOCIO
    ,info.FECHA_INICIO_POLIZA_MATRIZ
    ,info.FECHA_FIN_POLIZA_MATRIZ
    ,info.FECHA_INICIO_COBERTURA
    ,info.FECHA_FIN_COBERTURA
    ,info.ID_TOMADOR_POLIZA
    ,info.TOMADOR_POLIZA
    ,info.ID_TOMADOR_POLIZA_MATRIZ
    ,info.TOMADOR_POLIZA_MATRIZ
    ,ope.PORCPART_ALFA
    ,info.FRECUENCIA_PAGO
    ,ope.PRIMA_EMITIDA_VALUE_DETALLE                                             --Se agrega PRIMA_EMITIDA_VALUE                                            
    ,ope.PRIMA_EMITIDA_REASEGURO_DETALLE                                         --Se agrega PRIMA_EMITIDA_REASEGURO
    ,ope.PRIMA_EMITIDA_MOVIMIENTO_DETALLE                                        --Se agrega PRIMA_EMITIDA_MOVIMIENTO
    ,ope.PRIMA_ALFA_VALUE_DETALLE                                                --Se agrega PRIMA_ALFA_VALUE
    ,ope.PRIMA_ALFA_REASEGURO_DETALLE                                            --Se agrega PRIMA_ALFA_REASEGURO
    ,ope.PRIMA_ALFA_MOVIMIENTO_DETALLE                                           --Se agrega PRIMA_ALFA_MOVIMIENTO
    ,ope.PRIMACED_COASEGURO
    ,ope.PORC_COM_INTERMEDIACION
    ,ope.COMISION_INTERMEDIACION
    ,ope.IVA_COMISION_INTERMEDIACION
    ,ope.COMISION_RECAUDO
    ,ope.PORCCOMRECAUDO_VALUE
    ,ope.PORCCOMRECAUDO_REASEGURO
    ,ope.PORCCOMRECAUDO_MOVIMIENTO
    ,ope.IVA_COMISION_RECAUDO
    ,info.CODREASEG
    ,info.CLASE_REASEGURADOR
    ,info.NOMREASEG
    ,ope.PRIMACEDAUT
    ,ope.PRIMACEDFAC
    ,ope.COMISIONREASEGAUT
    ,ope.COMISIONREASEGFAC
    ,ope.PRIMACEDAUT_INT
    ,ope.PRIMACEDAUT_EXT
    ,ope.PRIMACEDFAC_INT
    ,ope.PRIMACEDFAC_EXT
    ,ope.COMISIONREASEGAUT_INT
    ,ope.COMISIONREASEGAUT_EXT
    ,ope.COMISIONREASEGFAC_INT
    ,ope.COMISIONREASEGFAC_EXT
    ,info.PER_FEC_INI_VIG_OPE
    ,info.PER_FEC_FIN_VIG_OPE
    ,info.ID_LOTE_CARGUE
    ,ope.PERIODO
FROM niif_pcr_tradicional_informacion info 
INNER JOIN niif_pcr_tradicional_operativo ope ON info.id = ope.id and info.ramo_contable = ope.ramo_contable and info.ramo_reaseguro = ope.ramo_reaseguro  and info.cobertura = ope.cod_cobertura
""")
df_query_pcr_tradicional_gold.count()
df_query_pcr_tradicional_gold_unique = df_query_pcr_tradicional_gold.dropDuplicates(["operacion","ramo_contable","ramo_tecnico","ramo_reaseguro", "producto", "poliza", "cobertura"])

df_query_pcr_tradicional_gold_unique.count()
df_query_pcr_tradicional_gold_unique.filter(F.col("operacion") == 990117679).count()
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, LongType
from pyspark.sql.functions import lit,col, split, explode, trim, lower, regexp_replace

DB_CATALOG_GLUE = 'datalake-dev-silver-catalog-db'
PERIODO = "2025-01"
FECHA_INICIO = "2025-01-01"
FECHA_FIN = "2025-01-31"
def description_stst_systemproperty(df_stst_systemproperty, syp_property_value):

    df_stst_systemproperty_filter = df_stst_systemproperty.filter(lower(col("SYP_PROPERTY")) == syp_property_value.lower())
    
    exploded_stst_systemproperty_filter = df_stst_systemproperty_filter.select(explode(split(df_stst_systemproperty_filter["SYP_VALUE"], ",")).alias("DESCRIPTION"))
    
    trimmed_stst_systemproperty_filter = exploded_stst_systemproperty_filter.select(lower(trim(exploded_stst_systemproperty_filter["DESCRIPTION"])).alias("DESCRIPTION"))    
    
    return trimmed_stst_systemproperty_filter
# --- Lectura y Selección para cada CTE ---

# SELECT df_contextoperation_par
df_contextoperation_par = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_contextoperation_par").toDF()
df_contextoperation_par_select = df_contextoperation_par.select("id", "time_stamp", "status", "time_stamp_partition")

# SELECT: df_thirdpartymovementpolicy_par
df_thirdpartymovementpolicy_par = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_thirdpartymovementpolicy_par").toDF()
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

#SELECT: participacionintermediarios
df_participacionintermediarios = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_participacionintermediarios").toDF()
df_participacionintermediarios_select = df_participacionintermediarios.select(col("pk"), col("porcentajecomisioninput").cast(DecimalType(38, 10)).alias("porcentajecomisioninput")).distinct()
#FILTER CONTEXTOPERATION FOR PERIOD
df_contextoperation_par_filter = df_contextoperation_par_select.filter((df_contextoperation_par_select["time_stamp_partition"]== PERIODO) & (df_contextoperation_par_select["status"] == 2) & (df_contextoperation_par_select["time_stamp"].between(FECHA_INICIO,FECHA_FIN)))
df_stpo_policyparticipation_filter_tomador = df_stpo_policyparticipation.filter(df_stpo_policyparticipation["rol_id"] == 25935)
df_stpo_policyparticipation_filter_pagador = df_stpo_policyparticipation.filter(df_stpo_policyparticipation["rol_id"] == 33935)
df_contextoperation_par_filter = df_contextoperation_par_filter.filter(df_contextoperation_par_filter["id"] == 990117679)
#df_contextoperation_par_filter = df_contextoperation_par_filter.filter(df_contextoperation_par_filter["id"] == 986532776)
#spark.sparkContext.setCheckpointDir("s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL_TMP/")
df_contextoperation_par_filter = df_contextoperation_par_filter.repartition("id")
df_thirdpartymovementpolicy_par_select = df_thirdpartymovementpolicy_par_select.repartition("operationpk")
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

#---Operación
df_stpo_policyparticipationdco_select.createOrReplaceTempView("stpo_policyparticipationdco")
df_participacionintermediarios_select.createOrReplaceTempView("participacionintermediarios")
df_openitem_select.createOrReplaceTempView("openitem")
df_stca_doctype_select.createOrReplaceTempView("stca_doctype")
df_uaadetail_select.createOrReplaceTempView("uaadetail")
df_property_select.createOrReplaceTempView("property")
tmp_tomador_filter_path  = "s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL_TMP/tomador_filter_tmp"
df_stpo_policyparticipation_filter_tomador.write.mode("overwrite").parquet(tmp_tomador_filter_path)
stpo_policyparticipation_tomador = spark.read.parquet(tmp_tomador_filter_path).repartition(200, "thirdpartyid")  # repartition aquí es razonable
stpo_policyparticipation_tomador.createOrReplaceTempView("stpo_policyparticipation_tomador")

tmp_pagador_filter_path  = "s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL_TMP/pagador_filter_tmp"
df_stpo_policyparticipation_filter_pagador.write.mode("overwrite").parquet(tmp_pagador_filter_path)
stpo_policyparticipation_pagador = spark.read.parquet(tmp_pagador_filter_path).repartition(200, "thirdpartyid")  # repartition aquí es razonable
stpo_policyparticipation_pagador.createOrReplaceTempView("stpo_policyparticipation_pagador")


tmp_prethirdparty_order_path  = "s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL_TMP/prethirdparty_order_tmp"
df_prethirdparty_select.write.mode("overwrite").parquet(tmp_prethirdparty_order_path)
prethirdparty = spark.read.parquet(tmp_prethirdparty_order_path).repartition(200, "static")  # repartition aquí es razonable
prethirdparty.createOrReplaceTempView("prethirdparty")
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
tmp_base_path  = "s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL_TMP/base_tmp"
df_base_metadata.write.mode("overwrite").parquet(tmp_base_path)
df_base_safe = spark.read.parquet(tmp_base_path).repartition(200, "id")  # repartition aquí es razonable
df_base_safe.createOrReplaceTempView("df_base_safe")
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
tmp_product_path  = "s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL_TMP/product_tmp"
df_product_metadata.write.mode("overwrite").parquet(tmp_product_path)
df_product_safe = spark.read.parquet(tmp_product_path).repartition(200, "id")  # repartition aquí es razonable
df_product_safe.createOrReplaceTempView("df_product_safe")
#df_base_safe_poliza = spark.read.parquet(tmp_base_path).repartition(200, "THIRDPARTY")  # repartition aquí es razonable
#df_base_safe_poliza.createOrReplaceTempView("df_base_safe_poliza")

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
tmp_tomador_path  = "s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL_TMP/tomador_tmp"
df_tomador_metadata = df_tomador_metadata.repartition(200)
df_tomador_metadata.write.mode("overwrite").parquet(tmp_tomador_path)
df_tomador_safe = spark.read.parquet(tmp_tomador_path).repartition(200, "id")  # repartition aquí es razonable
df_tomador_safe.createOrReplaceTempView("df_tomador_safe")
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
tmp_pagador_path  = "s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL_TMP/pagador_tmp"
df_pagador_metadata = df_pagador_metadata.repartition(200)
df_pagador_metadata.write.mode("overwrite").parquet(tmp_pagador_path)
df_pagador_safe = spark.read.parquet(tmp_pagador_path).repartition(200, "id")  # repartition aquí es razonable
df_pagador_safe.createOrReplaceTempView("df_pagador_safe")
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
tmp_riesgo_path  = "s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL_TMP/riesgo_tmp"
df_riesgo_metadata.write.mode("overwrite").parquet(tmp_riesgo_path)
df_riesgo_safe = spark.read.parquet(tmp_riesgo_path).repartition(200, "id")  # repartition aquí es razonable
df_riesgo_safe.createOrReplaceTempView("df_riesgo_safe")
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
tmp_acuerdo_path  = "s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL_TMP/acuerdo_tmp"
df_acuerdo_metadata.write.mode("overwrite").parquet(tmp_acuerdo_path)
df_acuerdo_safe = spark.read.parquet(tmp_acuerdo_path).repartition(200, "id")  # repartition aquí es razonable
df_acuerdo_safe.createOrReplaceTempView("df_acuerdo_safe")
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
tmp_cobertura_path  = "s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL_TMP/cobertura_tmp"
df_cobertura_metadata.write.mode("overwrite").parquet(tmp_cobertura_path)
df_cobertura_safe = spark.read.parquet(tmp_cobertura_path).repartition(500, "id")  # repartition aquí es razonable
df_cobertura_safe.createOrReplaceTempView("df_cobertura_safe")
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
GROUP BY 
    bse.id, rod.thirdpartyname, ra.codigoreaseguradorinput, ra.clasereaseguroinput
""")
tmp_reaseguro_path  = "s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL_TMP/reaseguro_tmp"
df_reaseguro_metadata.write.mode("overwrite").parquet(tmp_reaseguro_path)
df_reaseguro_safe = spark.read.parquet(tmp_reaseguro_path).repartition(500, "id")  # repartition aquí es razonable
df_reaseguro_safe.createOrReplaceTempView("df_reaseguro_safe")
df_reaseguro_group_metadata = spark.sql (""" 

SELECT bse.id, collect_set(thirdpartyname) as thirdpartyname, collect_set(codigoreaseguradorinput) as codigoreaseguradorinput, max(clasereaseguroinput) as clasereaseguroinput

FROM  df_reaseguro_safe bse

group by id

""")
tmp_reaseguro_group_path  = "s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL_TMP/reaseguro_group_tmp"
df_reaseguro_group_metadata.write.mode("overwrite").parquet(tmp_reaseguro_group_path)
df_reaseguro_group_safe = spark.read.parquet(tmp_reaseguro_group_path).repartition(500, "id")  # repartition aquí es razonable
df_reaseguro_group_safe.createOrReplaceTempView("df_reaseguro_group_safe")
print(df_base_safe.count())
print(df_product_safe.count())
print(df_tomador_safe.count())
print(df_riesgo_safe.count())
print(df_cobertura_safe.count())
print(df_acuerdo_safe.count())
print(df_reaseguro_safe.count())
print(df_reaseguro_group_safe.count())
test_vol_metadata = spark.sql("""

SELECT 
    'ASCEL-E'                                           as ORIGEN
    , bse.id
    , bse.operationpk                                   as tpmp_operationpk
    , prod.companiainput                                as COMPANIA
    , cb.cobramocontableinput                           as RAMO_CONTABLE
    , cb.rkc_rek_description                            as DESC_RAMO_CONTABLE
    , prod.ramovalue                                    as RAMO_TECNICO
    , prod.ramoinput                                    as DESC_RAMO_TECNICO 
    , cb.rek_code                                       as RAMO_REASEGURO
    , cb.rek_description                                as DESC_RAMO_REASEGURO 
    , cb.code                                           as GRUPO_REASEGURO
    , prod.productid                                    as PRODUCTO
    , prod.description                                  as DESC_PRODUCTO
    , acd.ast_numeroacu                                 as POLIZA_MATRIZ
    , bse.numeropolizainput                             as POLIZA
    , bse.oficinainput                                  as SUCURSAL
    , ries.numunidriesgoinput                           as UNIDAD_RIESGO
    , cb.cobcodigocoberturainput                        as COBERTURA
    , cb.cobdescripcioncobertinput                      as DESC_COBERTURA
    , bse.operationpk                                   as ENDOSO
    , bse.time_stamp                                    as FECHA_ENDOSO
    , acd.unidadnegocioinput                            as UNIDAD_NEGOCIO
    , bse.initialdate                                   as FECHA_INICIO_POLIZA_MATRIZ
    , bse.finishdate                                    as FECHA_FIN_POLIZA_MATRIZ
    , ries.initialdate                                  as FECHA_INICIO_COBERTURA
    , ries.finishdate                                   as FECHA_FIN_COBERTURA
    , tom.thirdpartyid_tomador                          as ID_TOMADOR_POLIZA      
    , tom.nombrepredeterminadoinput_tomador             as TOMADOR_POLIZA 
    , pag.thirdpartyid_pagador                          as ID_TOMADOR_POLIZA_MATRIZ 
    , pag.nombrepredeterminadoinput_pagador             as TOMADOR_POLIZA_MATRIZ 
    , bse.frecuenciapagoinput                           as FRECUENCIA_PAGO
    --, collect_set(rsg.codigoreaseguradorinput)          as CODREASEG
    --, max(rsg.clasereaseguroinput)                      as CLASE_REASEGURADOR 
    --, collect_set(rsg.thirdpartyname)                   as NOMREASEG
    , bse.time_stamp_partition                          as PERIODO
    , date_trunc('month', bse.time_stamp)               as PER_FEC_INI_VIG_OPE
    , last_day(bse.time_stamp)                          as PER_FEC_FIN_VIG_OPE
    , 0                                                 as ID_LOTE_CARGUE

FROM
    df_base_safe bse
    
--PRODUCT      
INNER JOIN 
    df_product_safe prod ON prod.id = bse.id
    
--ROL_TOMADOR/pagador
INNER JOIN
    df_tomador_safe tom ON tom.id = bse.id
    
--ROL_PAGADOR
INNER JOIN
    df_pagador_safe pag ON tom.id = bse.id

--RIESGO
INNER JOIN 
    df_riesgo_safe ries ON ries.id = bse.id
    
--COBERTURA/POLIZA --REASEGURO RAMO    
INNER JOIN 
    df_cobertura_safe cb ON cb.id = bse.id

    
--ACUERDO
INNER JOIN 
    df_acuerdo_safe acd

GROUP BY
     bse.id
    , bse.operationpk                                  
    , prod.companiainput                               
    , cb.cobramocontableinput                          
    , cb.rkc_rek_description                           
    , prod.ramovalue                                   
    , prod.ramoinput                                   
    , cb.rek_code                                      
    , cb.rek_description                               
    , cb.code                                          
    , prod.productid                                   
    , prod.description                                 
    , acd.ast_numeroacu                                
    , bse.numeropolizainput                            
    , bse.oficinainput                                 
    , ries.numunidriesgoinput                          
    , cb.cobcodigocoberturainput                       
    , cb.cobdescripcioncobertinput                     
    , bse.operationpk                                  
    , bse.time_stamp                                   
    , acd.unidadnegocioinput                           
    , bse.initialdate                                  
    , bse.finishdate                                   
    , ries.initialdate                                
    , ries.finishdate                                 
    , tom.thirdpartyid_tomador                         
    , tom.nombrepredeterminadoinput_tomador            
    , pag.thirdpartyid_pagador                         
    , pag.nombrepredeterminadoinput_pagador            
    , bse.frecuenciapagoinput                                  
    , bse.time_stamp_partition                         
    , date_trunc('month', bse.time_stamp)              
    , last_day(bse.time_stamp)                                 

""")
test_vol_path  = "s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL_TMP/test_vol_tmp"
test_vol_metadata.write.mode("overwrite").parquet(test_vol_path)
test_vol_safe = spark.read.parquet(test_vol_path).repartition(800, "id")  # repartition aquí es razonable
test_vol_safe.createOrReplaceTempView("test_vol_safe")
test_vol_safe.printSchema()
df_informacion_pcr = spark.sql(""" 
SELECT
sf.ORIGEN
,sf.id
,sf.tpmp_operationpk
,sf.COMPANIA
,sf.RAMO_CONTABLE
,sf.DESC_RAMO_CONTABLE
,sf.RAMO_TECNICO
,sf.DESC_RAMO_TECNICO
,sf.RAMO_REASEGURO
,sf.DESC_RAMO_REASEGURO
,sf.GRUPO_REASEGURO
,sf.PRODUCTO
,sf.DESC_PRODUCTO
,sf.POLIZA_MATRIZ
,sf.POLIZA
,sf.SUCURSAL
,sf.UNIDAD_RIESGO
,sf.COBERTURA
,sf.DESC_COBERTURA
,sf.ENDOSO
,sf.FECHA_ENDOSO
,sf.UNIDAD_NEGOCIO
,sf.FECHA_INICIO_POLIZA_MATRIZ
,sf.FECHA_FIN_POLIZA_MATRIZ
,sf.FECHA_INICIO_COBERTURA
,sf.FECHA_FIN_COBERTURA
,sf.ID_TOMADOR_POLIZA
,sf.TOMADOR_POLIZA
,sf.ID_TOMADOR_POLIZA_MATRIZ
,sf.TOMADOR_POLIZA_MATRIZ
,sf.FRECUENCIA_PAGO
,sf.PER_FEC_INI_VIG_OPE
,sf.PER_FEC_FIN_VIG_OPE
,rsg.thirdpartyname           as NOMREASEG
,rsg.codigoreaseguradorinput  as CODREASEG
,rsg.clasereaseguroinput      as CLASE_REASEGURADOR
,sf.ID_LOTE_CARGUE
,sf.PERIODO
FROM test_vol_safe sf
INNER JOIN df_reaseguro_group_safe rsg ON rsg.id = sf.id
""")
df_informacion_pcr.show(truncate = False)
test_vol.count()
query_informacion = spark.sql (""" 
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
    stpo_policyparticipation pp_tomador ON tpmp.THIRDPARTY = pp_tomador.thirdpartyid AND pp_tomador.rol_id = 25935
INNER JOIN 
    prethirdparty ptp_tomador ON pp_tomador.thirdpartyid = ptp_tomador.static

--ROL_PAGADOR
LEFT JOIN
    stpo_policyparticipation pp_pagador ON tpmp.THIRDPARTY = pp_pagador.thirdpartyid AND pp_pagador.rol_id = 33935
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
df_query_operativo.show()
query_informacion.write.mode("overwrite").partitionBy("PERIODO").parquet("s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL_TEST_INFO")
spark.sql("USE `datalake-dev-silver-catalog-db`")
df_query_por_comission_intermediary = spark.sql("""
        SELECT
            pi.porcentajecomisioninput as PORC_COM_INTERMEDIACION,
            ctx.id as id_cte_porc_com_intermediacion
        FROM contextoperation_par ctx
        INNER JOIN stpo_policyparticipation pp ON ctx.id = pp.operationpk
        INNER JOIN stpo_policyparticipationdco ppd ON pp.operationpk = ppd.operationpk and pp.agregatedobjectid = ppd.agregatedobjectid
        INNER JOIN participacionintermediarios pi ON ppd.dcoid = pi.pk
""")

df_query_por_comission_intermediary.createOrReplaceTempView("query_por_comission_intermediary")
df_query_comission_percentage = spark.sql ("""
    
        SELECT 
            CASE
                WHEN SUM(ro.totalpremium) = 0 THEN 0
                ELSE ABS((SUM(rog.premiumamount) * 100) / SUM(ro.totalpremium))
            END AS group_percentage,
            ctx.id as id_query_comission_percentage
        FROM 
            contextoperation_par ctx
        INNER JOIN 
            policydco pdco ON ctx.id = pdco.operationpk
        INNER JOIN 
            reinsuranceoperation ro ON ro.cor_id = pdco.operationpk and ro.agregatedpolicyid = pdco.agregatedobjectid
        INNER JOIN 
            reinsuranceoperationgroupinfo rog ON rog.operationid = ro.operationid
        INNER JOIN 
            reinsuranceoperationcomponent roc ON roc.reinsurancegroupinfoid = rog.reinsurancegroupinfoid
        INNER JOIN 
            ri_reinsurancegroup rg ON rg.reinsurancegroupid = rog.groupid        
            
        GROUP BY ctx.id, ro.cor_id, ro.AGREGATEDRISKUNITID, rg.REKIND_ID, roc.CONTRACTTYPE, ro.TOTALPREMIUM, rg.RIG_COMPANYCODE
        
    """)
df_query_comission_percentage.createOrReplaceTempView("query_comission_percentage")
df_descriptions_comission_intermediary = description_stst_systemproperty(df_stst_systemproperty, "rrc.commisionintermediary")
df_descriptions_comission_intermediary.createOrReplaceTempView("descriptions_comission_intermediary")

df_query_comission_intermediary = spark.sql ("""
        SELECT
            sum(det.value) *(qcp.group_percentage/100)                   as COMISION_INTERMEDIACION,
        	ctx.id                                                      as id_comission_intermediary
        from contextoperation_par ctx
        INNER JOIN openitem oi ON ctx.id = oi.operationpk
        INNER JOIN stca_doctype dt ON dt.dty_id = oi.dty_id
        INNER JOIN uaadetail det ON det.openitemid = oi.openitemid
        INNER JOIN property pp ON pp.propertyid = det.property
        INNER JOIN query_comission_percentage qcp ON ctx.id = qcp.id_query_comission_percentage
        and lower(oi.opm_substatus) <> 'notapplied'
        and lower(dt.dty_description) in (SELECT DESCRIPTION FROM descriptions_comission_intermediary)
        and lower(pp.simbolo) in ('comision')
        group by ctx.id, qcp.group_percentage 
        
        """ )

df_query_comission_intermediary.createOrReplaceTempView("query_comission_intermediary")
df_descriptions_comission_intermediary_iva = description_stst_systemproperty(df_stst_systemproperty, "rrc.commisionintermediary")
df_descriptions_comission_intermediary_iva.createOrReplaceTempView("descriptions_comission_intermediary_iva")

df_query_comission_intermediary_iva = spark.sql ("""
        select
            sum(det.value)                   as IVA_COMISION_INTERMEDIACION,
        	ctx.id                           as id_comission_intermediary_iva
        from contextoperation_par ctx
        inner join openitem oi on ctx.id = oi.operationpk
        inner join stca_doctype dt on dt.dty_id = oi.dty_id
        inner join uaadetail det on det.openitemid = oi.openitemid
        inner join property pp on pp.propertyid = det.property
        and lower(oi.opm_substatus) <> 'notapplied'
        and lower(dt.dty_description) in (SELECT DESCRIPTION FROM descriptions_comission_intermediary_iva)
        and lower(pp.simbolo) in ('ivacomision')
        group by ctx.id 
        
        """)
df_query_comission_intermediary_iva.createOrReplaceTempView("query_comission_intermediary_iva")
df_comission_collection  = description_stst_systemproperty(df_stst_systemproperty, "rrc.commissioncollectiondoctype")
df_comission_collection.createOrReplaceTempView("comission_collection")

df_query_comission_collection = spark.sql ("""
        
        SELECT
        	sum(case 
        			when dt.dty_isdebit = 0 then oi.amount
        			else oi.amount * (-1) 
        		end) as COMISION_RECAUDO,
        	ctx.id as id_cte_comision_recaudo
        	
        FROM contextoperation_par ctx
        INNER JOIN openitem oi ON ctx.id = oi.operatiONpk
        INNER JOIN stca_doctype dt ON dt.dty_id = oi.dty_id
        and lower(oi.opm_substatus) <> 'notapplied'
        and lower(dt.dty_description) in (SELECT DESCRIPTION FROM comission_collection)
        group by ctx.id
        
    """)
df_query_comission_collection.createOrReplaceTempView("query_comission_collection")
df_comission_collection_iva  = description_stst_systemproperty(df_stst_systemproperty, "rrc.taxcommissioncollectiondoctype")
df_comission_collection_iva.createOrReplaceTempView("comission_collection_iva")

df_query_comission_collection_iva = spark.sql ("""
        
        SELECT
        	sum(case 
        			when dt.dty_isdebit = 0 then oi.amount
        			else oi.amount * (-1) 
        		end) as IVA_COMISION_RECAUDO,
        	ctx.id as id_cte_comision_recaudo_iva
        	
        FROM contextoperation_par ctx
        INNER JOIN openitem oi ON ctx.id = oi.operatiONpk
        INNER JOIN stca_doctype dt ON dt.dty_id = oi.dty_id
        and lower(oi.opm_substatus) <> 'notapplied'
        and lower(dt.dty_description) in (SELECT DESCRIPTION FROM comission_collection_iva)
        group by ctx.id
        
    """)
df_query_comission_collection_iva.createOrReplaceTempView("query_comission_collection_iva")
df_query_premium_detail_reinsurance_comission = spark.sql("""
    
        SELECT
            ctx.id as id_cte_premium_detail,
            SUM(ROD.ROD_PRORATEDPREMIUM) AS PREMIUM_DETAIL,
            SUM(PT.ROPP_VALUE) as REINSURANCE_COMMISSION
        from contextoperation_par ctx
        INNER JOIN policydco pdco on ctx.id = pdco.operationpk
        INNER JOIN reinsuranceoperation ro on ro.cor_id = pdco.operationpk and ro.agregatedpolicyid = pdco.agregatedobjectid
        INNER JOIN reinsuranceoperationgroupinfo ROG ON ROG.OPERATIONID = RO.OPERATIONID
        INNER JOIN reinsuranceoperationcomponent ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
        INNER JOIN reinsuranceoperationdetail ROD ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
        LEFT JOIN ri_reinsurancegroup RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
        LEFT JOIN acsele_alfa_full_reinsuranceoperationproperty PT ON  PT.ROD_ID = ROD.REINSURANCEOPERATIONDETAILID AND lower(PT.ROPP_NAME) IN ('compricedida', 'compricedidaind')
        GROUP BY ROG.OPERATIONID, ROC.CONTRACTTYPE, ctx.id

    
    """)
df_query_premium_detail_reinsurance_comission.createOrReplaceTempView("query_premium_detail_reinsurance_comission")
df_query_prima = spark.sql ("""
    SELECT
          ctx.id                            as id     
        , cb.cobramocontableinput           as RAMO_CONTABLE
        , rg.code                           as GRUPO_REASEGURO
        , rg.description                    as DSC_GRUPO_REASEGURO
        , rk.rek_code                       as RAMO_REASEGURO
    	, rk.rek_description                as DESC_RAMO_REASEGURO
    	, sum(cb.cobprimavalue)             as PRIMA_EMITIDA_VALUE
    	, sum(cb.cobprimareasegurovalue)    as PRIMA_EMITIDA_REASEGURO
    	, sum(cb.cobprimamovimientovalue)   as PRIMA_EMITIDA_MOVIMIENTO
        , ctx.time_stamp_partition          as PERIODO
        
    FROM
        contextoperation_par ctx
    --COBERTURA
    INNER JOIN 
        coveragedco cdco ON ctx.id = cdco.operationpk
    INNER JOIN 
        cobertura cb  ON cdco.dcoid = cb.pk     
    INNER JOIN
        ri_reinsurancegroup rg ON rg.code = cb.cobramoreaseguroinput
    INNER JOIN
        stri_reinsurancekind rk ON rg.rekind_id = rk.rekind_id
    GROUP BY 
        ctx.id                            
        , cb.cobramocontableinput           
        , rg.code                           
        , rg.description                    
        , rk.rek_code                       
    	, rk.rek_description                
        , ctx.time_stamp_partition 
    
    """)
df_query_prima.createOrReplaceTempView("query_prima")
df_query_prima.show()
df_query_coberturas = spark.sql ("""

    SELECT 
         prima.id                            
        ,prima.RAMO_CONTABLE                 
        ,prima.GRUPO_REASEGURO
        ,prima.DSC_GRUPO_REASEGURO
        ,prima.RAMO_REASEGURO
        ,prima.DESC_RAMO_REASEGURO
        ,prima.PRIMA_EMITIDA_VALUE
        ,prima.PRIMA_EMITIDA_REASEGURO
        ,prima.PRIMA_EMITIDA_MOVIMIENTO
        ,cb.COBCODIGOCOBERTURAINPUT         as COD_COBERTURA
        ,cb.COBDESCRIPCIONCOBERTINPUT       as DESC_COBERTURA
        ,prima.PERIODO 
        ,count(cb.cobcodigocoberturainput) OVER (PARTITION BY prima.id, prima.RAMO_CONTABLE ,prima.GRUPO_REASEGURO, prima.RAMO_REASEGURO) as CANT_COBERTURAS
    FROM
        query_prima prima
    --COBERTURA
    INNER JOIN 
        coveragedco cdco ON prima.id = cdco.operationpk
    INNER JOIN 
        cobertura cb  ON cdco.dcoid = cb.pk     
    INNER JOIN 
        ri_reinsurancegroup rg ON rg.code = cb.cobramoreaseguroinput and rg.code = prima.GRUPO_REASEGURO
    INNER JOIN 
        stri_reinsurancekind rk ON rg.rekind_id = rk.rekind_id and rk.rek_code = prima.RAMO_REASEGURO

    GROUP BY
        prima.id
        , prima.RAMO_CONTABLE
        , prima.GRUPO_REASEGURO
        , prima.DSC_GRUPO_REASEGURO
        , prima.RAMO_REASEGURO
        , prima.DESC_RAMO_REASEGURO
        , prima.PRIMA_EMITIDA_VALUE
        , prima.PRIMA_EMITIDA_REASEGURO
        , prima.PRIMA_EMITIDA_MOVIMIENTO
        , cb.COBCODIGOCOBERTURAINPUT
        , cb.COBDESCRIPCIONCOBERTINPUT
        , prima.PERIODO

""")

df_query_coberturas.createOrReplaceTempView("query_coberturas")
df_query_operativo = spark.sql ("""     
        SELECT DISTINCT
        id
        ,RAMO_CONTABLE
        ,GRUPO_REASEGURO
        ,DSC_GRUPO_REASEGURO
        ,RAMO_REASEGURO
        ,DESC_RAMO_REASEGURO
        ,PRIMA_EMITIDA_VALUE
        ,PRIMA_EMITIDA_REASEGURO
        ,PRIMA_EMITIDA_MOVIMIENTO
        ,COD_COBERTURA
        ,DESC_COBERTURA
        ,CANT_COBERTURAS
        ,(PRIMA_EMITIDA_VALUE/CANT_COBERTURAS)                                                                                          as PRIMA_EMITIDA_VALUE_DETALLE
        ,(PRIMA_EMITIDA_REASEGURO/CANT_COBERTURAS)                                                                                      as PRIMA_EMITIDA_REASEGURO_DETALLE
        ,(PRIMA_EMITIDA_MOVIMIENTO/CANT_COBERTURAS)                                                                                     as PRIMA_EMITIDA_MOVIMIENTO_DETALLE
        , 0                                                                                                                             as PORCPART_ALFA
        ,(PRIMA_EMITIDA_VALUE/CANT_COBERTURAS) * (100/100)                                                                              as PRIMA_ALFA_VALUE_DETALLE
        ,(PRIMA_EMITIDA_REASEGURO/CANT_COBERTURAS) * (100/100)                                                                          as PRIMA_ALFA_REASEGURO_DETALLE
        ,(PRIMA_EMITIDA_MOVIMIENTO/CANT_COBERTURAS) * (100/100)                                                                         as PRIMA_ALFA_MOVIMIENTO_DETALLE
        , 0                                                                                                                             as PRIMACED_COASEGURO
        , COALESCE(qpc.PORC_COM_INTERMEDIACION, 0)                                                                                      as PORC_COM_INTERMEDIACION
        , COALESCE(qci.COMISION_INTERMEDIACION, 0)                                                                                      as COMISION_INTERMEDIACION
        , COALESCE(qcv.IVA_COMISION_INTERMEDIACION, 0)                                                                                  as IVA_COMISION_INTERMEDIACION
        , COALESCE(qcc.COMISION_RECAUDO, 0)                                                                                             as COMISION_RECAUDO
        , COALESCE(qcc.COMISION_RECAUDO, 0)*100                                                                                         as PORCCOMRECAUDO
        , COALESCE(qcci.IVA_COMISION_RECAUDO, 0)                                                                                        as IVA_COMISION_RECAUDO
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0,qpd.PREMIUM_DETAIL)               as PRIMACEDAUT
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),qpd.PREMIUM_DETAIL, 0)              as PRIMACEDFAC
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0, qpd.REINSURANCE_COMMISSION)      as COMISIONREASEGAUT
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),qpd.REINSURANCE_COMMISSION, 0)      as COMISIONREASEGFAC
        , IF(RA.CLASEREASEGUROINPUT = 'Interior', qpd.PREMIUM_DETAIL, 0)                                                                as PRIMACEDAUT_INT
        , IF(RA.CLASEREASEGUROINPUT = 'Exterior', qpd.PREMIUM_DETAIL, 0)                                                                as PRIMACEDAUT_EXT
        , IF(RA.CLASEREASEGUROINPUT = 'Interior', qpd.PREMIUM_DETAIL, 0)                                                                as PRIMACEDFAC_INT
        , IF(RA.CLASEREASEGUROINPUT = 'Exterior', qpd.PREMIUM_DETAIL, 0)                                                                as PRIMACEDFAC_EXT
        , IF(RA.CLASEREASEGUROINPUT = 'Interior', qpd.REINSURANCE_COMMISSION, 0)                                                        as COMISIONREASEGAUT_INT
        , IF(RA.CLASEREASEGUROINPUT = 'Exterior', qpd.REINSURANCE_COMMISSION, 0)                                                        as COMISIONREASEGAUT_EXT
        , IF(RA.CLASEREASEGUROINPUT = 'Interior', qpd.REINSURANCE_COMMISSION, 0)                                                        as COMISIONREASEGFAC_INT
        , IF(RA.CLASEREASEGUROINPUT = 'Exterior', qpd.REINSURANCE_COMMISSION, 0)                                                        as COMISIONREASEGFAC_EXT
        , MAX(RA.CLASEREASEGUROINPUT)                                                                                                   as CLASE_REASEGURADOR
        ,PERIODO
        FROM 
            query_coberturas qc
        INNER JOIN policydco pdco ON id = pdco.operationpk
        INNER JOIN REINSURANCEOPERATION RO ON ro.cor_id = pdco.operationpk and ro.agregatedpolicyid = pdco.agregatedobjectid
        INNER JOIN REINSURANCEOPERATIONGROUPINFO ROG ON ROG.OPERATIONID = RO.OPERATIONID
        INNER JOIN REINSURANCEOPERATIONCOMPONENT ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
        INNER JOIN RI_REINSURANCEGROUP RG ON RG.REINSURANCEGROUPID = ROG.Groupid
        INNER JOIN reinsuranceoperationdetail rod ON rod.reinopercomponentid = roc.reinopercomponentid
        INNER JOIN thirdpartyrole tpr ON tpr.thirdpartyid = rod.thirdpartyid
        INNER JOIN reasegurador ra  ON tpr.iddcorole = ra.pk
        LEFT JOIN query_por_comission_intermediary qpc ON id = qpc.id_cte_porc_com_intermediacion
        LEFT JOIN query_comission_intermediary qci ON id = qci.id_comission_intermediary
        LEFT JOIN query_comission_intermediary_iva qcv ON id = qcv.id_comission_intermediary_iva
        LEFT JOIN query_comission_collection qcc ON id = qcc.id_cte_comision_recaudo
        LEFT JOIN query_comission_collection_iva qcci ON  id = qcci.id_cte_comision_recaudo_iva
        LEFT JOIN query_premium_detail_reinsurance_comission qpd ON id = qpd.id_cte_premium_detail
        
        group by id
        ,RAMO_CONTABLE
        ,GRUPO_REASEGURO
        ,DSC_GRUPO_REASEGURO
        ,RAMO_REASEGURO
        ,DESC_RAMO_REASEGURO
        ,PRIMA_EMITIDA_VALUE
        ,PRIMA_EMITIDA_REASEGURO
        ,PRIMA_EMITIDA_MOVIMIENTO
        ,COD_COBERTURA
        ,DESC_COBERTURA
        ,CANT_COBERTURAS
        ,qpc.PORC_COM_INTERMEDIACION
        ,qci.COMISION_INTERMEDIACION
        ,qcv.IVA_COMISION_INTERMEDIACION
        ,qcc.COMISION_RECAUDO
        ,qcci.IVA_COMISION_RECAUDO
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0,qpd.PREMIUM_DETAIL)         
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),qpd.PREMIUM_DETAIL, 0)        
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0, qpd.REINSURANCE_COMMISSION)
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),qpd.REINSURANCE_COMMISSION, 0)
        , IF(RA.CLASEREASEGUROINPUT = 'Interior', qpd.PREMIUM_DETAIL, 0)                                                          
        , IF(RA.CLASEREASEGUROINPUT = 'Exterior', qpd.PREMIUM_DETAIL, 0)                                                          
        , IF(RA.CLASEREASEGUROINPUT = 'Interior', qpd.PREMIUM_DETAIL, 0)                                                          
        , IF(RA.CLASEREASEGUROINPUT = 'Exterior', qpd.PREMIUM_DETAIL, 0)                                                          
        , IF(RA.CLASEREASEGUROINPUT = 'Interior', qpd.REINSURANCE_COMMISSION, 0)                                                  
        , IF(RA.CLASEREASEGUROINPUT = 'Exterior', qpd.REINSURANCE_COMMISSION, 0)                                                  
        , IF(RA.CLASEREASEGUROINPUT = 'Interior', qpd.REINSURANCE_COMMISSION, 0)                                                  
        , IF(RA.CLASEREASEGUROINPUT = 'Exterior', qpd.REINSURANCE_COMMISSION, 0)
        ,PERIODO

""")
df_query_operativo.count()
query_informacion.show(truncate =False)
spark.sql("USE `datalake-dev-silver-catalog-db`")
df_test_información  = spark.sql("""--TOMADOR_POLIZA, ID_TOMADOR_POLIZA, POLIZA_MATRIZ, POLIZA
WITH contextoperation_par AS (
        SELECT id, status, time_stamp_partition , time_stamp FROM acsele_alfa_full_contextoperation_par ctx
        WHERE ctx.time_stamp_partition = '2025-01' and ctx.status = 2 and ctx.time_stamp BETWEEN DATE '2025-01-01' AND DATE '2025-01-31' 
        --and ctx.id IN (990117679, 988660345, 986532776, 986556152)
        --and ctx.id IN (990117679)
        
),
    thirdpartymovementpolicy_par AS (
        SELECT operationpk, policyid, cast(THIRDPARTY as BIGINT) as THIRDPARTY FROM acsele_alfa_full_thirdpartymovementpolicy_par GROUP BY operationpk, policyid, THIRDPARTY
    ),
    
    prepolicy AS (
       SELECT pk, static, numeropolizainput, tipopolizavalue, tipopolizainput, oficinainput, frecuenciapagoinput FROM  acsele_alfa_full_prepolicy GROUP BY pk, static, numeropolizainput, tipopolizavalue, tipopolizainput, oficinainput, frecuenciapagoinput
    ),
    
    policydco AS (
        SELECT agregatedobjectid, dcoid, operationpk, initialdate, finishdate FROM acsele_alfa_full_policydco GROUP BY agregatedobjectid, dcoid, operationpk, initialdate, finishdate
    ),
    
    agregatedpolicy AS (
        SELECT agregatedpolicyid, productid  FROM acsele_alfa_full_agregatedpolicy GROUP BY agregatedpolicyid, productid
    ),
    
    product AS (
        SELECT pro_dcoid ,productid, description FROM acsele_alfa_full_product GROUP BY  pro_dcoid ,productid, description
    ),
    
    preproduct AS (
        SELECT pk, companiainput,ramoinput,codigoproductoinput, descripcionproductoinput, ramovalue FROM acsele_alfa_full_preproduct GROUP BY pk, companiainput,ramoinput,codigoproductoinput, descripcionproductoinput, ramovalue  
    ),
    
    stpo_policyparticipation AS (
        SELECT thirdpartyid, rol_id FROM acsele_alfa_full_stpo_policyparticipation GROUP BY thirdpartyid, rol_id
    ),
    
    stps_relationagreementpolicy AS (
        SELECT rsa_policyid, rsa_ast_id  FROM acsele_alfa_full_stps_relationagreementpolicy GROUP BY rsa_policyid, rsa_ast_id
    ),
    
    stps_agreementstaticdat AS (
        SELECT ast_id, ast_numeroacu FROM acsele_alfa_full_stps_agreementstaticdat  GROUP BY  ast_id, ast_numeroacu
    ),
    
    prethirdparty AS (
        SELECT static, nombrepredeterminadoinput, tipodocumentoinput, tipodocumentovalue, documentoidentidadinput FROM acsele_alfa_full_prethirdparty GROUP BY static, nombrepredeterminadoinput, tipodocumentoinput, tipodocumentovalue, documentoidentidadinput
    ),
        
    coveragedco AS (
        SELECT operationpk, dcoid FROM acsele_alfa_full_coveragedco_par GROUP BY operationpk, dcoid
    ),
    
    cobertura AS (
        SELECT pk,cobramocontableinput, cobcodigocoberturainput, cobdescripcioncobertinput,cobramoreaseguroinput FROM acsele_alfa_full_cobertura GROUP BY pk,cobramocontableinput, cobcodigocoberturainput, cobdescripcioncobertinput, cobramoreaseguroinput
    ),
    
    riskunitdco AS (
        SELECT operationpk, dcoid, initialdate, finishdate FROM acsele_alfa_full_riskunitdco GROUP BY operationpk, dcoid, initialdate, finishdate
    ),
    
    preriskunits AS (
        SELECT pk, numunidriesgoinput FROM acsele_alfa_full_preriskunits GROUP BY pk, numunidriesgoinput
    ),
    
    stag_agreementversion AS (
        SELECT ast_id, dcoid, ast_fecini, ast_fecfin FROM acsele_alfa_full_stag_agreementversion GROUP BY ast_id, dcoid, ast_fecini, ast_fecfin
    ),
    
    preagreement AS (
        SELECT pk, unidadnegocioinput FROM acsele_alfa_full_preagreement GROUP BY pk, unidadnegocioinput 
    ),
    
    reinsuranceoperation AS (
        SELECT cor_id, agregatedpolicyid, operationid FROM acsele_alfa_full_reinsuranceoperation GROUP BY cor_id, agregatedpolicyid, operationid
    ),
    
    reinsuranceoperationgroupinfo AS (
        SELECT  operationid, reinsurancegroupinfoid, groupid FROM acsele_alfa_full_reinsuranceoperationgroupinfo GROUP BY operationid, reinsurancegroupinfoid, groupid
    ),
    
    reinsuranceoperationcomponent AS (
        SELECT reinsurancegroupinfoid, reinopercomponentid FROM acsele_alfa_full_reinsuranceoperationcomponent GROUP BY reinsurancegroupinfoid, reinopercomponentid
    ),
    
    reinsuranceoperationdetail AS (
        SELECT  reinopercomponentid, thirdpartyid, thirdpartyname FROM acsele_alfa_full_reinsuranceoperationdetail GROUP BY reinopercomponentid, thirdpartyid, thirdpartyname
    ),
    
    ri_reinsurancegroup AS (
        SELECT code, rekind_id, reinsurancegroupid FROM acsele_alfa_full_ri_reinsurancegroup GROUP BY code, rekind_id, reinsurancegroupid
    ),
    
    stri_reinsurancekind AS (
        SELECT rekind_id, rek_code, rek_description FROM acsele_alfa_full_stri_reinsurancekind GROUP BY rekind_id, rek_code, rek_description
    ),

    thirdpartyrole AS (
        SELECT thirdpartyid, iddcorole FROM acsele_alfa_full_thirdpartyrole GROUP BY thirdpartyid, iddcorole
    ),
    
    reasegurador AS (
        SELECT pk, clasereaseguroinput, codigoreaseguradorinput FROM acsele_alfa_full_reasegurador GROUP BY pk, clasereaseguroinput, codigoreaseguradorinput
    ),
    
    
query_tomador AS (
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
    , ctx.time_stamp_partition                          as PERIODO
    , date_trunc('month', ctx.time_stamp)               as PER_FEC_INI_VIG_OPE
    , last_day(ctx.time_stamp)                          as PER_FEC_FIN_VIG_OPE
    , 0                                                 as ID_LOTE_CARGUE
	
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
    stpo_policyparticipation pp_tomador ON tpmp.THIRDPARTY = pp_tomador.thirdpartyid AND pp_tomador.rol_id = 25935
INNER JOIN 
    prethirdparty ptp_tomador ON pp_tomador.thirdpartyid = ptp_tomador.static

--ROL_PAGADOR
LEFT JOIN
    stpo_policyparticipation pp_pagador ON tpmp.THIRDPARTY = pp_pagador.thirdpartyid AND pp_pagador.rol_id = 33935
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

    
	)


SELECT *  FROM query_tomador ORDER BY id DESC

""")
df_test_información.write.mode("overwrite").partitionBy("PERIODO").parquet("s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL/PCR_TABLA_INFORMACIÓN/")
df_test_operativo = spark.sql ("""
--TOMADOR_POLIZA, ID_TOMADOR_POLIZA, POLIZA_MATRIZ, POLIZA
WITH contextoperation_par AS (
        SELECT id, status, time_stamp_partition , time_stamp FROM acsele_alfa_full_contextoperation_par ctx
        WHERE ctx.time_stamp_partition = '2025-01' and ctx.status = 2 and ctx.time_stamp BETWEEN DATE '2025-01-01' AND DATE '2025-01-31' 
        --and ctx.id IN (990117679, 988660345, 986532776, 986556152)
        --and ctx.id IN (988814513)
        --and ctx.id IN (990117679)
        --and ctx.id IN (988290408)

    ),
    thirdpartymovementpolicy_par AS (
        SELECT operationpk, policyid, cast(THIRDPARTY as BIGINT) as THIRDPARTY, concept, ammount FROM acsele_alfa_full_thirdpartymovementpolicy_par GROUP BY operationpk, policyid, THIRDPARTY, concept, ammount
    ),
    
    prepolicy AS (
       SELECT pk, static, numeropolizainput, tipopolizavalue, tipopolizainput, oficinainput, frecuenciapagoinput FROM  acsele_alfa_full_prepolicy GROUP BY pk, static, numeropolizainput, tipopolizavalue, tipopolizainput, oficinainput, frecuenciapagoinput
    ),
    
    policydco AS (
        SELECT agregatedobjectid, dcoid, operationpk, initialdate, finishdate FROM acsele_alfa_full_policydco GROUP BY agregatedobjectid, dcoid, operationpk, initialdate, finishdate
    ),
    
    agregatedpolicy AS (
        SELECT agregatedpolicyid, productid  FROM acsele_alfa_full_agregatedpolicy GROUP BY agregatedpolicyid, productid
    ),
    
    product AS (
        SELECT pro_dcoid ,productid, description FROM acsele_alfa_full_product GROUP BY  pro_dcoid ,productid, description
    ),
    
    preproduct AS (
        SELECT pk, companiainput,ramoinput,codigoproductoinput, descripcionproductoinput, ramovalue FROM acsele_alfa_full_preproduct GROUP BY pk, companiainput,ramoinput,codigoproductoinput, descripcionproductoinput, ramovalue  
    ),

    coveragedco AS (
        SELECT operationpk, dcoid FROM acsele_alfa_full_coveragedco_par GROUP BY operationpk, dcoid
    ),
    
    cobertura AS (
        SELECT pk,
        cobramocontableinput, 
        cobcodigocoberturainput, 
        cobdescripcioncobertinput,
        cobramoreaseguroinput, 
        cast(cobprimareasegurovalue as decimal(38,7)) as cobprimareasegurovalue,
        cast(cobprimamovimientovalue as decimal(38,7)) as cobprimamovimientovalue,
        cast(cobprimavalue as decimal(38,7)) as  cobprimavalue
        FROM acsele_alfa_full_cobertura GROUP BY pk,cobramocontableinput, cobcodigocoberturainput, cobdescripcioncobertinput,cobramoreaseguroinput,cobprimareasegurovalue,cobprimamovimientovalue,cobprimavalue
    ),
    
    ri_reinsurancegroup AS (
        SELECT code, rekind_id, description, reinsurancegroupid, RIG_COMPANYCODE FROM acsele_alfa_full_ri_reinsurancegroup GROUP BY code, rekind_id, description, reinsurancegroupid, RIG_COMPANYCODE
    ),
    
    stri_reinsurancekind AS (
        SELECT rekind_id, rek_code, rek_description FROM acsele_alfa_full_stri_reinsurancekind GROUP BY rekind_id, rek_code, rek_description
    ),

    stpo_policyparticipation AS (
        SELECT thirdpartyid, operationpk, rol_id, agregatedobjectid FROM acsele_alfa_full_stpo_policyparticipation GROUP BY thirdpartyid, operationpk, rol_id, agregatedobjectid
    ),

    stpo_policyparticipationdco AS (
        SELECT operationpk, agregatedobjectid, dcoid FROM acsele_alfa_full_stpo_policyparticipationdco GROUP BY operationpk, agregatedobjectid, dcoid
    ),
    
    participacionintermediarios AS (
        SELECT pk, porcentajecomisioninput FROM acsele_alfa_full_participacionintermediarios GROUP BY pk, porcentajecomisioninput
    ),
    
    openitem AS (
        SELECT operationpk, opm_substatus, openitemid, dty_id, CAST(amount AS Decimal(38,10)) as amount FROM acsele_alfa_full_openitem GROUP BY operationpk, opm_substatus, openitemid, dty_id, amount
    ),
    
    stca_doctype AS (
        SELECT dty_id, dty_description, dty_isdebit FROM acsele_alfa_full_stca_doctype GROUP BY dty_id, dty_description, dty_isdebit
    ),
    
    uaadetail AS (
        SELECT openitemid, cast(property as bigint) as property , cast(value as decimal(38,10)) as value FROM acsele_alfa_full_uaadetail GROUP BY  openitemid, property, value
    ),
    
    property AS (
        SELECT propertyid, simbolo FROM acsele_alfa_full_property GROUP BY propertyid, simbolo
    ),
    
    reinsuranceoperation AS (
        SELECT cor_id, agregatedpolicyid, operationid, AGREGATEDRISKUNITID, TOTALPREMIUM FROM acsele_alfa_full_reinsuranceoperation GROUP BY cor_id, agregatedpolicyid, operationid, AGREGATEDRISKUNITID, TOTALPREMIUM
    ),
    
    reinsuranceoperationgroupinfo AS (
        SELECT  operationid, reinsurancegroupinfoid, groupid, premiumamount FROM acsele_alfa_full_reinsuranceoperationgroupinfo GROUP BY operationid, reinsurancegroupinfoid, groupid, premiumamount
    ),
    
    reinsuranceoperationcomponent AS (
        SELECT reinsurancegroupinfoid, reinopercomponentid, CONTRACTTYPE FROM acsele_alfa_full_reinsuranceoperationcomponent GROUP BY reinsurancegroupinfoid, reinopercomponentid, CONTRACTTYPE
    ),
    
    reinsuranceoperationdetail AS (
        SELECT  reinopercomponentid, thirdpartyid, thirdpartyname, ROD_PRORATEDPREMIUM, REINSURANCEOPERATIONDETAILID FROM acsele_alfa_full_reinsuranceoperationdetail GROUP BY reinopercomponentid, thirdpartyid, thirdpartyname, ROD_PRORATEDPREMIUM, REINSURANCEOPERATIONDETAILID
    ),
    
    thirdpartyrole AS (
        SELECT thirdpartyid, iddcorole FROM acsele_alfa_full_thirdpartyrole GROUP BY thirdpartyid, iddcorole
    ),
    
    reasegurador AS (
        SELECT pk, clasereaseguroinput, codigoreaseguradorinput FROM acsele_alfa_full_reasegurador GROUP BY pk, clasereaseguroinput, codigoreaseguradorinput
    ),
    
    query_por_comission_intermediary AS (
        SELECT
            CASt(pi.porcentajecomisioninput AS DECIMAL(38,7)) as PORC_COM_INTERMEDIACION,
        	ctx.id as id_cte_porc_com_intermediacion
        FROM contextoperation_par ctx
        INNER JOIN stpo_policyparticipation pp ON ctx.id = pp.operationpk
        INNER JOIN stpo_policyparticipationdco ppd ON pp.operationpk = ppd.operationpk and pp.agregatedobjectid = ppd.agregatedobjectid
        INNER JOIN participacionintermediarios pi ON ppd.dcoid = pi.pk
    ),
    
    query_comission_percentage AS (
    
        SELECT 
            CASE
                WHEN SUM(ro.totalpremium) = 0 THEN 0
                ELSE ABS((SUM(rog.premiumamount) * 100) / SUM(ro.totalpremium))
            END AS group_percentage,
            ctx.id as id_query_comission_percentage
        FROM 
            contextoperation_par ctx
        INNER JOIN 
            policydco pdco ON ctx.id = pdco.operationpk
        INNER JOIN 
            reinsuranceoperation ro ON ro.cor_id = pdco.operationpk and ro.agregatedpolicyid = pdco.agregatedobjectid
        INNER JOIN 
            reinsuranceoperationgroupinfo rog ON rog.operationid = ro.operationid
        INNER JOIN 
            reinsuranceoperationcomponent roc ON roc.reinsurancegroupinfoid = rog.reinsurancegroupinfoid
        INNER JOIN 
            ri_reinsurancegroup rg ON rg.reinsurancegroupid = rog.groupid        
            
        GROUP BY ctx.id, ro.cor_id, ro.AGREGATEDRISKUNITID, rg.REKIND_ID, roc.CONTRACTTYPE, ro.TOTALPREMIUM, rg.RIG_COMPANYCODE
        
    ),
    
    query_comission_intermediary AS (
        select
            sum(det.value) *(qcp.group_percentage/100)                   as COMISION_INTERMEDIACION,
        	ctx.id                                                      as id_comission_intermediary
        from contextoperation_par ctx
        inner join openitem oi on ctx.id = oi.operationpk
        inner join stca_doctype dt on dt.dty_id = oi.dty_id
        inner join uaadetail det on det.openitemid = oi.openitemid
        inner join property pp on pp.propertyid = det.property
        inner join query_comission_percentage qcp on ctx.id = qcp.id_query_comission_percentage
        and lower(oi.opm_substatus) <> 'notapplied'
        and lower(dt.dty_description) in (
            SELECT
            lower(trim(elements)) AS description
        FROM
            acsele_alfa_full_stst_systemproperty,
            UNNEST(SPLIT(syp_value, ',')) AS t (elements)
        WHERE
            lower(syp_property) = 'rrc.commisionintermediary')
        and lower(pp.simbolo) in ('comision')
        group by ctx.id,qcp.group_percentage ),
    
    query_comission_intermediary_iva AS (
        select
            sum(det.value)                   as IVA_COMISION_INTERMEDIACION,
        	ctx.id                           as id_comission_intermediary_iva
        from contextoperation_par ctx
        inner join openitem oi on ctx.id = oi.operationpk
        inner join stca_doctype dt on dt.dty_id = oi.dty_id
        inner join uaadetail det on det.openitemid = oi.openitemid
        inner join property pp on pp.propertyid = det.property
        and lower(oi.opm_substatus) <> 'notapplied'
        and lower(dt.dty_description) in (
            SELECT
            lower(trim(elements)) AS description
        FROM
            acsele_alfa_full_stst_systemproperty,
            UNNEST(SPLIT(syp_value, ',')) AS t (elements)
        WHERE
            lower(syp_property) = 'rrc.commisionintermediary')
        and lower(pp.simbolo) in ('ivacomision')
        group by ctx.id ),
        
    query_comission_collection AS (
        
        SELECT
        	sum(case 
        			when dt.dty_isdebit = 0 then oi.amount
        			else oi.amount * (-1) 
        		end) as COMISION_RECAUDO,
        	ctx.id as id_cte_comision_recaudo
        	
        FROM contextoperation_par ctx
        INNER JOIN openitem oi ON ctx.id = oi.operatiONpk
        INNER JOIN stca_doctype dt ON dt.dty_id = oi.dty_id
        and lower(oi.opm_substatus) <> 'notapplied'
        and lower(dt.dty_description) in (
        SELECT
            lower(trim(elements)) AS description
        FROM
            acsele_alfa_full_stst_systemproperty,
            UNNEST(SPLIT(syp_value, ',')) AS t (elements)
        WHERE
            lower(syp_property) = 'rrc.commissioncollectiondoctype')
        group by ctx.id
        
    ),
    
    query_comission_collection_iva AS (
        
        SELECT
        	sum(case 
        			when dt.dty_isdebit = 0 then oi.amount
        			else oi.amount * (-1) 
        		end) as IVA_COMISION_RECAUDO,
        	ctx.id as id_cte_comision_recaudo_iva
        	
        FROM contextoperation_par ctx
        INNER JOIN openitem oi ON ctx.id = oi.operatiONpk
        INNER JOIN stca_doctype dt ON dt.dty_id = oi.dty_id
        and lower(oi.opm_substatus) <> 'notapplied'
        and lower(dt.dty_description) in (
        SELECT
            lower(trim(elements)) AS description
        FROM
            acsele_alfa_full_stst_systemproperty,
            UNNEST(SPLIT(syp_value, ',')) AS t (elements)
        WHERE
            lower(syp_property) = 'rrc.taxcommissioncollectiondoctype')
        group by ctx.id
        
    ),    
        
    query_premium_detail_reinsurance_comission AS (
    
        SELECT
            ctx.id as id_cte_premium_detail,
            SUM(ROD.ROD_PRORATEDPREMIUM) AS PREMIUM_DETAIL,
            SUM(PT.ROPP_VALUE) as REINSURANCE_COMMISSION
        from contextoperation_par ctx
        INNER JOIN policydco pdco on ctx.id = pdco.operationpk
        INNER JOIN reinsuranceoperation ro on ro.cor_id = pdco.operationpk and ro.agregatedpolicyid = pdco.agregatedobjectid
        INNER JOIN reinsuranceoperationgroupinfo ROG ON ROG.OPERATIONID = RO.OPERATIONID
        INNER JOIN reinsuranceoperationcomponent ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
        INNER JOIN reinsuranceoperationdetail ROD ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
        LEFT JOIN ri_reinsurancegroup RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
        LEFT JOIN acsele_alfa_full_reinsuranceoperationproperty PT ON  PT.ROD_ID = ROD.REINSURANCEOPERATIONDETAILID AND lower(PT.ROPP_NAME) IN ('compricedida', 'compricedidaind')
        GROUP BY ROG.OPERATIONID, ROC.CONTRACTTYPE, ctx.id

    
    ),
    
    query_prima AS (
    SELECT
          ctx.id                            as id     
        , cb.cobramocontableinput           as RAMO_CONTABLE
        , rg.code                           as GRUPO_REASEGURO
        , rg.description                    as DSC_GRUPO_REASEGURO
        , rk.rek_code                       as RAMO_REASEGURO
    	, rk.rek_description                as DESC_RAMO_REASEGURO
    	, sum(cb.cobprimavalue)             as PRIMA_EMITIDA_VALUE
    	, sum(cb.cobprimareasegurovalue)    as PRIMA_EMITIDA_REASEGURO
    	, sum(cb.cobprimamovimientovalue)   as PRIMA_EMITIDA_MOVIMIENTO
        , ctx.time_stamp_partition          as PERIODO
        
    FROM
        contextoperation_par ctx
    --COBERTURA
    INNER JOIN 
        coveragedco cdco ON ctx.id = cdco.operationpk
    INNER JOIN 
        cobertura cb  ON cdco.dcoid = cb.pk     
    INNER JOIN
        ri_reinsurancegroup rg ON rg.code = (replace(cb.cobramoreaseguroinput, '.0', ''))
    INNER JOIN
        stri_reinsurancekind rk ON rg.rekind_id = rk.rekind_id
    GROUP BY 
        ctx.id                            
        , cb.cobramocontableinput           
        , rg.code                           
        , rg.description                    
        , rk.rek_code                       
    	, rk.rek_description                
        , ctx.time_stamp_partition 
    
    ),
    	
    query_coberturas AS (
    
        SELECT 
             prima.id                            
            ,prima.RAMO_CONTABLE                 
            ,prima.GRUPO_REASEGURO
            ,prima.DSC_GRUPO_REASEGURO
            ,prima.RAMO_REASEGURO
            ,prima.DESC_RAMO_REASEGURO
            ,prima.PRIMA_EMITIDA_VALUE
            ,prima.PRIMA_EMITIDA_REASEGURO
            ,prima.PRIMA_EMITIDA_MOVIMIENTO
            ,cb.COBCODIGOCOBERTURAINPUT         as COD_COBERTURA
    	    ,cb.COBDESCRIPCIONCOBERTINPUT       as DESC_COBERTURA
    	    ,prima.PERIODO 
    	    ,count(cb.cobcodigocoberturainput) OVER (PARTITION BY prima.id, prima.RAMO_CONTABLE ,prima.GRUPO_REASEGURO, prima.RAMO_REASEGURO) as CANT_COBERTURAS
        FROM
            query_prima prima
        --COBERTURA
        INNER JOIN 
            coveragedco cdco ON prima.id = cdco.operationpk
        INNER JOIN 
            cobertura cb  ON cdco.dcoid = cb.pk     
        INNER JOIN 
            ri_reinsurancegroup rg ON rg.code = (replace(cb.cobramoreaseguroinput, '.0', '')) and rg.code = prima.GRUPO_REASEGURO
        INNER JOIN 
            stri_reinsurancekind rk ON rg.rekind_id = rk.rekind_id and rk.rek_code = prima.RAMO_REASEGURO
        
        GROUP BY
            prima.id
            , prima.RAMO_CONTABLE
            , prima.GRUPO_REASEGURO
            , prima.DSC_GRUPO_REASEGURO
            , prima.RAMO_REASEGURO
            , prima.DESC_RAMO_REASEGURO
            , prima.PRIMA_EMITIDA_VALUE
            , prima.PRIMA_EMITIDA_REASEGURO
            , prima.PRIMA_EMITIDA_MOVIMIENTO
            , cb.COBCODIGOCOBERTURAINPUT
            , cb.COBDESCRIPCIONCOBERTINPUT
            , prima.PERIODO
    
    ),
    
    query_primas_detalle AS (
        
        SELECT DISTINCT
        id
        ,RAMO_CONTABLE
        ,GRUPO_REASEGURO
        ,DSC_GRUPO_REASEGURO
        ,RAMO_REASEGURO
        ,DESC_RAMO_REASEGURO
        ,PRIMA_EMITIDA_VALUE
        ,PRIMA_EMITIDA_REASEGURO
        ,PRIMA_EMITIDA_MOVIMIENTO
        ,COD_COBERTURA
        ,DESC_COBERTURA
        ,CANT_COBERTURAS
        ,(PRIMA_EMITIDA_VALUE/CANT_COBERTURAS)                                                                                          as PRIMA_EMITIDA_VALUE_DETALLE
        ,(PRIMA_EMITIDA_REASEGURO/CANT_COBERTURAS)                                                                                      as PRIMA_EMITIDA_REASEGURO_DETALLE
        ,(PRIMA_EMITIDA_MOVIMIENTO/CANT_COBERTURAS)                                                                                     as PRIMA_EMITIDA_MOVIMIENTO_DETALLE
        , 0                                                                                                                             as PORCPART_ALFA
        ,(PRIMA_EMITIDA_VALUE/CANT_COBERTURAS) * (100/100)                                                                              as PRIMA_ALFA_VALUE_DETALLE
        ,(PRIMA_EMITIDA_REASEGURO/CANT_COBERTURAS) * (100/100)                                                                          as PRIMA_ALFA_REASEGURO_DETALLE
        ,(PRIMA_EMITIDA_MOVIMIENTO/CANT_COBERTURAS) * (100/100)                                                                         as PRIMA_ALFA_MOVIMIENTO_DETALLE
        , 0                                                                                                                             as PRIMACED_COASEGURO
        , COALESCE(qpc.PORC_COM_INTERMEDIACION, 0)                                                                                      as PORC_COM_INTERMEDIACION
        , COALESCE(qci.COMISION_INTERMEDIACION, 0)                                                                                      as COMISION_INTERMEDIACION
        , COALESCE(qcv.IVA_COMISION_INTERMEDIACION, 0)                                                                                  as IVA_COMISION_INTERMEDIACION
        , COALESCE(qcc.COMISION_RECAUDO, 0)                                                                                             as COMISION_RECAUDO
        , COALESCE(qcc.COMISION_RECAUDO, 0)*100                                                                                         as PORCCOMRECAUDO
        , COALESCE(qcci.IVA_COMISION_RECAUDO, 0)                                                                                        as IVA_COMISION_RECAUDO
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0,qpd.PREMIUM_DETAIL)               as PRIMACEDAUT
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),qpd.PREMIUM_DETAIL, 0)              as PRIMACEDFAC
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0, qpd.REINSURANCE_COMMISSION)      as COMISIONREASEGAUT
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),qpd.REINSURANCE_COMMISSION, 0)      as COMISIONREASEGFAC
        , IF(RA.CLASEREASEGUROINPUT = 'Interior', qpd.PREMIUM_DETAIL, 0)                                                                as PRIMACEDAUT_INT
        , IF(RA.CLASEREASEGUROINPUT = 'Exterior', qpd.PREMIUM_DETAIL, 0)                                                                as PRIMACEDAUT_EXT
        , IF(RA.CLASEREASEGUROINPUT = 'Interior', qpd.PREMIUM_DETAIL, 0)                                                                as PRIMACEDFAC_INT
        , IF(RA.CLASEREASEGUROINPUT = 'Exterior', qpd.PREMIUM_DETAIL, 0)                                                                as PRIMACEDFAC_EXT
        , IF(RA.CLASEREASEGUROINPUT = 'Interior', qpd.REINSURANCE_COMMISSION, 0)                                                        as COMISIONREASEGAUT_INT
        , IF(RA.CLASEREASEGUROINPUT = 'Exterior', qpd.REINSURANCE_COMMISSION, 0)                                                        as COMISIONREASEGAUT_EXT
        , IF(RA.CLASEREASEGUROINPUT = 'Interior', qpd.REINSURANCE_COMMISSION, 0)                                                        as COMISIONREASEGFAC_INT
        , IF(RA.CLASEREASEGUROINPUT = 'Exterior', qpd.REINSURANCE_COMMISSION, 0)                                                        as COMISIONREASEGFAC_EXT
        , MAX(RA.CLASEREASEGUROINPUT)                                                                                                   as CLASE_REASEGURADOR
        ,PERIODO
        FROM 
            query_coberturas qc
        INNER JOIN policydco pdco ON id = pdco.operationpk
        INNER JOIN REINSURANCEOPERATION RO ON ro.cor_id = pdco.operationpk and ro.agregatedpolicyid = pdco.agregatedobjectid
        INNER JOIN REINSURANCEOPERATIONGROUPINFO ROG ON ROG.OPERATIONID = RO.OPERATIONID
        INNER JOIN REINSURANCEOPERATIONCOMPONENT ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
        INNER JOIN RI_REINSURANCEGROUP RG ON RG.REINSURANCEGROUPID = ROG.Groupid
        INNER JOIN reinsuranceoperationdetail rod ON rod.reinopercomponentid = roc.reinopercomponentid
        INNER JOIN thirdpartyrole tpr ON tpr.thirdpartyid = rod.thirdpartyid
        INNER JOIN reasegurador ra  ON tpr.iddcorole = ra.pk
        LEFT JOIN query_por_comission_intermediary qpc ON id = qpc.id_cte_porc_com_intermediacion
        LEFT JOIN query_comission_intermediary qci ON id = qci.id_comission_intermediary
        LEFT JOIN query_comission_intermediary_iva qcv ON id = qcv.id_comission_intermediary_iva
        LEFT JOIN query_comission_collection qcc ON id = qcc.id_cte_comision_recaudo
        LEFT JOIN query_comission_collection_iva qcci ON  id = qcci.id_cte_comision_recaudo_iva
        LEFT JOIN query_premium_detail_reinsurance_comission qpd ON id = qpd.id_cte_premium_detail
        
        group by id
        ,RAMO_CONTABLE
        ,GRUPO_REASEGURO
        ,DSC_GRUPO_REASEGURO
        ,RAMO_REASEGURO
        ,DESC_RAMO_REASEGURO
        ,PRIMA_EMITIDA_VALUE
        ,PRIMA_EMITIDA_REASEGURO
        ,PRIMA_EMITIDA_MOVIMIENTO
        ,COD_COBERTURA
        ,DESC_COBERTURA
        ,CANT_COBERTURAS
        ,qpc.PORC_COM_INTERMEDIACION
        ,qci.COMISION_INTERMEDIACION
        ,qcv.IVA_COMISION_INTERMEDIACION
        ,qcc.COMISION_RECAUDO
        ,qcci.IVA_COMISION_RECAUDO
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0,qpd.PREMIUM_DETAIL)         
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),qpd.PREMIUM_DETAIL, 0)        
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0, qpd.REINSURANCE_COMMISSION)
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),qpd.REINSURANCE_COMMISSION, 0)
        , IF(RA.CLASEREASEGUROINPUT = 'Interior', qpd.PREMIUM_DETAIL, 0)                                                          
        , IF(RA.CLASEREASEGUROINPUT = 'Exterior', qpd.PREMIUM_DETAIL, 0)                                                          
        , IF(RA.CLASEREASEGUROINPUT = 'Interior', qpd.PREMIUM_DETAIL, 0)                                                          
        , IF(RA.CLASEREASEGUROINPUT = 'Exterior', qpd.PREMIUM_DETAIL, 0)                                                          
        , IF(RA.CLASEREASEGUROINPUT = 'Interior', qpd.REINSURANCE_COMMISSION, 0)                                                  
        , IF(RA.CLASEREASEGUROINPUT = 'Exterior', qpd.REINSURANCE_COMMISSION, 0)                                                  
        , IF(RA.CLASEREASEGUROINPUT = 'Interior', qpd.REINSURANCE_COMMISSION, 0)                                                  
        , IF(RA.CLASEREASEGUROINPUT = 'Exterior', qpd.REINSURANCE_COMMISSION, 0)
        ,PERIODO
    )

SELECT * FROM   query_primas_detalle;





""")

df_test_operativo.write.mode("overwrite").partitionBy("PERIODO").parquet("s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL/PCR_TABLA_OPERATIVA/")
df_test_detalle = spark.sql (""" WITH contextoperation_par AS (
        SELECT id, status, time_stamp_partition , time_stamp FROM acsele_alfa_full_contextoperation_par ctx
        WHERE ctx.time_stamp_partition = '2025-01' and ctx.status = 2 and ctx.time_stamp BETWEEN DATE '2025-01-01' AND DATE '2025-01-31' 
        --and ctx.id IN (990117679, 988660345, 986532776, 986556152)
        --and ctx.id IN (988814513)
        --and ctx.id IN (990117679)
        --and ctx.id IN (988290408)

    ),
    
    openitem AS (
        SELECT operationpk, opm_substatus, openitemid, dty_id, CAST(amount AS Decimal(38,10)) as amount FROM acsele_alfa_full_openitem GROUP BY operationpk, opm_substatus, openitemid, dty_id, amount
    ),
    
    stca_doctype AS (
        SELECT dty_id, dty_description, dty_isdebit FROM acsele_alfa_full_stca_doctype GROUP BY dty_id, dty_description, dty_isdebit
    ),
    
    coveragedco AS (
        SELECT operationpk, dcoid FROM acsele_alfa_full_coveragedco_par GROUP BY operationpk, dcoid
    ),
    
    cobertura AS (
        SELECT pk,
        cobramocontableinput, 
        cobcodigocoberturainput, 
        cobdescripcioncobertinput,
        cobramoreaseguroinput, 
        cast(cobprimareasegurovalue as decimal(38,7)) as cobprimareasegurovalue,
        cast(cobprimamovimientovalue as decimal(38,7)) as cobprimamovimientovalue,
        cast(cobprimavalue as decimal(38,7)) as  cobprimavalue
        FROM acsele_alfa_full_cobertura GROUP BY pk,cobramocontableinput, cobcodigocoberturainput, cobdescripcioncobertinput,cobramoreaseguroinput,cobprimareasegurovalue,cobprimamovimientovalue,cobprimavalue
    ),
    
    ri_reinsurancegroup AS (
        SELECT code, rekind_id, description, reinsurancegroupid, RIG_COMPANYCODE FROM acsele_alfa_full_ri_reinsurancegroup GROUP BY code, rekind_id, description, reinsurancegroupid, RIG_COMPANYCODE
    ),
    
    stri_reinsurancekind AS (
        SELECT rekind_id, rek_code, rek_description FROM acsele_alfa_full_stri_reinsurancekind GROUP BY rekind_id, rek_code, rek_description
    ),
    
    query_prima_total AS (
        
        select
              ctx.id
            , op.amount
            , DT.DTY_DESCRIPTION
            , DT.DTY_ISDEBIT
            , (CASE WHEN DT.DTY_ISDEBIT = 1 THEN op.AMOUNT ELSE op.AMOUNT * (-1) END) as prima_totalizador
            , NULL as ID_LOTE_CARGUE
        from contextoperation_par ctx
        inner join openitem op
        on ctx.id = op.operationpk
        inner join stca_doctype dt
        on dt.dty_id = op.dty_id
        WHERE lower(DT.DTY_DESCRIPTION) IN (        
        SELECT
            lower(trim(elements)) AS description
        FROM
            acsele_alfa_full_stst_systemproperty,
            UNNEST(SPLIT(syp_value, ',')) AS t (elements)
        WHERE
            lower(syp_property) = 'rrc.issuedpremiumdoctype')
        group by ctx.id
            , op.amount
            , DT.DTY_DESCRIPTION
            , DT.DTY_ISDEBIT    
            , (CASE WHEN DT.DTY_ISDEBIT = 1 THEN op.AMOUNT ELSE op.AMOUNT * (-1) END)
        order by ctx.id
    
    ),
    
    query_detalle_prima AS (
    
    SELECT 
         ctx.id
    	, cb.cobramocontableinput                                       as cod_ramo_contable
    	, rg.code                                                       as cod_grupo_reaseguro
    	, rg.description                                                as dsc_grupo_reaseguro
    	, rk.rek_code                                                   as cod_ramo_reaseguro
    	, rk.rek_description                                            as dsc_ramo_reaseguro
        , sum(cobprimareasegurovalue)                                   as prima_reaseguro	
        , sum(cobprimamovimientovalue)                                  as prima_movimiento
        , sum(cobprimamovimientovalue)                                  as prima_cobertura
        , ctx.time_stamp_partition as PERIODO
        FROM contextoperation_par ctx
        INNER JOIN coveragedco cdco on ctx.id = cdco.operationpk
        INNER JOIN cobertura cb on cdco.dcoid = cb.pk
        INNER JOIN ri_reinsurancegroup rg on rg.code = (replace(cb.cobramoreaseguroinput, '.0', ''))
        INNER JOIN stri_reinsurancekind rk on rg.rekind_id = rk.rekind_id
        
        GROUP BY  ctx.id
	            , cb.cobramocontableinput
	            , rg.code
	            , rg.description
	            , rk.rek_code
	            , rk.rek_description
	            , ctx.time_stamp_partition
    
    ),
    
    query_detalle_prima_total AS (
    
    SELECT
           id
         , cod_ramo_contable
         , sum(prima_reaseguro) as prima_reaseguro
         , sum(prima_movimiento) as prima_movimiento
         , sum(prima_cobertura) as prima_cobertura
         , PERIODO
    FROM query_detalle_prima
    GROUP BY id
            , cod_ramo_contable
            , periodo
    
    )
    
    
    SELECT * FROM query_detalle_prima_total


""")


df_test_detalle.write.mode("overwrite").partitionBy("PERIODO").parquet("s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL/PCR_TABLA_DETALLE_RAMO/")
df_test_totalizador = spark.sql (""" WITH contextoperation_par AS (
        SELECT id, status, time_stamp_partition , time_stamp FROM acsele_alfa_full_contextoperation_par ctx
        WHERE ctx.time_stamp_partition = '2025-01' and ctx.status = 2 and ctx.time_stamp BETWEEN DATE '2025-01-01' AND DATE '2025-01-31' 
        --and ctx.id IN (990117679, 988660345, 986532776, 986556152)
        --and ctx.id IN (988814513)
        --and ctx.id IN (990117679)
        --and ctx.id IN (988290408)

    ),
    
    openitem AS (
        SELECT operationpk, opm_substatus, openitemid, dty_id, CAST(amount AS Decimal(38,10)) as amount FROM acsele_alfa_full_openitem GROUP BY operationpk, opm_substatus, openitemid, dty_id, amount
    ),
    
    stca_doctype AS (
        SELECT dty_id, dty_description, dty_isdebit FROM acsele_alfa_full_stca_doctype GROUP BY dty_id, dty_description, dty_isdebit
    ),
    
    coveragedco AS (
        SELECT operationpk, dcoid FROM acsele_alfa_full_coveragedco_par GROUP BY operationpk, dcoid
    ),
    
    cobertura AS (
        SELECT pk,
        cobramocontableinput, 
        cobcodigocoberturainput, 
        cobdescripcioncobertinput,
        cobramoreaseguroinput, 
        cast(cobprimareasegurovalue as decimal(38,7)) as cobprimareasegurovalue,
        cast(cobprimamovimientovalue as decimal(38,7)) as cobprimamovimientovalue,
        cast(cobprimavalue as decimal(38,7)) as  cobprimavalue
        FROM acsele_alfa_full_cobertura GROUP BY pk,cobramocontableinput, cobcodigocoberturainput, cobdescripcioncobertinput,cobramoreaseguroinput,cobprimareasegurovalue,cobprimamovimientovalue,cobprimavalue
    ),
    
    ri_reinsurancegroup AS (
        SELECT code, rekind_id, description, reinsurancegroupid, RIG_COMPANYCODE FROM acsele_alfa_full_ri_reinsurancegroup GROUP BY code, rekind_id, description, reinsurancegroupid, RIG_COMPANYCODE
    ),
    
    stri_reinsurancekind AS (
        SELECT rekind_id, rek_code, rek_description FROM acsele_alfa_full_stri_reinsurancekind GROUP BY rekind_id, rek_code, rek_description
    ),
    
    query_prima_total AS (
        
        select
              ctx.id
            , op.amount
            , DT.DTY_DESCRIPTION
            , DT.DTY_ISDEBIT
            , (CASE WHEN DT.DTY_ISDEBIT = 1 THEN op.AMOUNT ELSE op.AMOUNT * (-1) END) as prima_totalizador
            , NULL as ID_LOTE_CARGUE
        from contextoperation_par ctx
        inner join openitem op
        on ctx.id = op.operationpk
        inner join stca_doctype dt
        on dt.dty_id = op.dty_id
        WHERE lower(DT.DTY_DESCRIPTION) IN (        
        SELECT
            lower(trim(elements)) AS description
        FROM
            acsele_alfa_full_stst_systemproperty,
            UNNEST(SPLIT(syp_value, ',')) AS t (elements)
        WHERE
            lower(syp_property) = 'rrc.issuedpremiumdoctype')
        group by ctx.id
            , op.amount
            , DT.DTY_DESCRIPTION
            , DT.DTY_ISDEBIT    
            , (CASE WHEN DT.DTY_ISDEBIT = 1 THEN op.AMOUNT ELSE op.AMOUNT * (-1) END)
        order by ctx.id
    
    )
    
    
    SELECT * FROM query_prima_total


""")


df_test_totalizador.write.mode("overwrite").partitionBy("PERIODO").parquet("s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL/PCR_TABLA_TOTALIZADOR")
DB_CATALOG_GLUE = 'datalake-dev-silver-catalog-db'
PERIODO = "2025-01"
FECHA_INICIO = "2025-01-01"
FECHA_FIN = "2025-01-31"
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, LongType
from pyspark.sql.functions import lit,col, split, explode, trim, lower
###################################################################
##################### Init stage gold #############################
###################################################################
#try:

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
# --- Lectura y Selección para cada CTE ---

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
df_stpo_policyparticipation_select = df_stpo_policyparticipation.select("thirdpartyid", "rol_id", "agregatedobjectid").distinct()

# SELECT: stpo_policyparticipationdco
df_stpo_policyparticipation = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_stpo_policyparticipationdco").toDF()
df_stpo_policyparticipation_select = df_stpo_policyparticipation.select("operationpk", "agregatedobjectid", "dcoid").distinct()

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
df_coveragedco = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_coveragedco").toDF()
df_coveragedco_select = df_coveragedco.select("operationpk", "dcoid").distinct()

# SELECT: cobertura
df_cobertura = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_cobertura").toDF()
df_cobertura_select = df_cobertura.select(
    col("pk"),
    col("cobramocontableinput"),
    col("cobcodigocoberturainput"),
    col("cobdescripcioncobertinput"),
    col("cobramoreaseguroinput"),
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
df_openitem_select = df_reasegurador.select(col("pk"), col("clasereaseguroinput"), col("codigoreaseguradorinput"), col("amount").cast(DecimalType(38, 10)).alias("amount")).distinct()


#SELECT: stca_doctype
df_openitem = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_openitem").toDF()
df_openitem_select = df_reasegurador.select("dty_id", "dty_description", "dty_isdebit").distinct()

#SELECT: uaadetail
df_openitem = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_openitem").toDF()
df_openitem_select = df_reasegurador.select(col("openitemid"), col("property").cast(LongType()).alias("property"), col("value").cast(DecimalType(38, 10)).alias("value")).distinct()

#SELECT: property
df_openitem = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_property").toDF()
df_openitem_select = df_reasegurador.select("propertyid", "simbolo").distinct()

#FILTER CONTEXTOPERATION FOR PERIOD
df_contextoperation_par_filter = df_contextoperation_par_select.filter((df_contextoperation_par_select["time_stamp_partition"]== PERIODO) & (df_contextoperation_par_select["status"] == 2) & (df_contextoperation_par_select["time_stamp"].between(FECHA_INICIO,FECHA_FIN)))
df_contextoperation_par_filter = df_contextoperation_par_filter.filter(df_contextoperation_par_filter["id"] == 986532776)
df_contextoperation_par_filter = df_contextoperation_par_filter.repartition("id")
df_thirdpartymovementpolicy_par_select = df_thirdpartymovementpolicy_par_select.repartition("operationpk")
# join contextoperation & thirdpartymovementpolicy
df_join_full_test = df_contextoperation_par_filter.join(
    df_thirdpartymovementpolicy_par_select, df_contextoperation_par_filter["id"] == df_thirdpartymovementpolicy_par_select["operationpk"], "inner"
    ).join(
    df_coveragedco_select, df_contextoperation_par_filter["id"] == df_coveragedco_select["operationpk"]
    )
# join contextoperation & thirdpartymovementpolicy
df_join_contextoperation_thirdpartymovementpolicy = df_contextoperation_par_filter.join(
    df_thirdpartymovementpolicy_par_select, df_contextoperation_par_filter["id"] == df_thirdpartymovementpolicy_par_select["operationpk"], "inner")
df_join_contextoperation_thirdpartymovementpolicy.show()
df_join_full_test.count()
df_join_cobertura = df_coveragedco_select.join(
    df_cobertura_select, df_coveragedco_select["dcoid"] == df_cobertura_select["pk"], "left"
)
df_join_cobertura.count()
#INITIAL JOINS

# join contextoperation & thirdpartymovementpolicy
df_join_contextoperation_thirdpartymovementpolicy = df_contextoperation_par_filter.join(
    df_thirdpartymovementpolicy_par_select, df_contextoperation_par_filter["id"] == df_thirdpartymovementpolicy_par_select["operationpk"], "inner")

# join_cobertura
df_join_cobertura = df_coveragedco_select.join(
    df_cobertura_select, df_coveragedco_select["dcoid"] == df_cobertura_select["pk"], "left"
)


#join acuerdo
df_join_acuerdo = df_stps_relationagreementpolicy_select.join(    
    df_stps_agreementstaticdat_select, df_stps_relationagreementpolicy_select["rsa_ast_id"] == df_stps_agreementstaticdat_select["ast_id"], "inner"
).join(    
    df_stag_agreementversion_select, df_stps_relationagreementpolicy_select["rsa_ast_id"] == df_stag_agreementversion_select["ast_id"], "inner"
).join(    
    df_preagreement_select, df_stag_agreementversion_select["dcoid"] == df_preagreement_select["pk"], "inner"
)

#join producto
df_join_producto = df_agregatedpolicy_select.join(
    df_product_select, df_agregatedpolicy_select["productid"] == df_product_select["productid"], "inner"
).join(
    df_preproduct_select, df_product_select["pro_dcoid"] == df_preproduct_select["pk"], "inner"
)


# join reaseguro
df_join_reaseguro = df_reinsuranceoperation_select.join(
    df_reinsuranceoperationgroupinfo_select, df_reinsuranceoperation_select["operationid"] == df_reinsuranceoperationgroupinfo_select["operationid"], "inner"
).join(
    df_reinsuranceoperationcomponent_select, df_reinsuranceoperationgroupinfo_select["reinsurancegroupinfoid"] == df_reinsuranceoperationcomponent_select["reinsurancegroupinfoid"], "inner"
).join(
    df_reinsuranceoperationdetail_select, df_reinsuranceoperationcomponent_select["reinopercomponentid"] == df_reinsuranceoperationdetail_select["reinopercomponentid"], "inner"
).join(
    df_thirdpartyrole_select, df_reinsuranceoperationdetail_select["thirdpartyid"] == df_thirdpartyrole_select["thirdpartyid"], "inner"
).join(
    df_reasegurador_select, df_thirdpartyrole_select["iddcorole"] == df_reasegurador_select["pk"], "inner"
)

#join rol
df_join_rol = df_stpo_policyparticipation_select.join(
    df_prethirdparty_select, df_stpo_policyparticipation_select["thirdpartyid"] == df_prethirdparty_select["static"], "inner"
).join(
    df_stts_role_select, df_stpo_policyparticipation_select["rol_id"] == df_stts_role_select["rol_id"], "inner"
)


#join riesgo 
df_join_riesgo = df_riskunitdco_select.join(
    df_preriskunits_select, df_riskunitdco_select["dcoid"] == df_preriskunits_select["pk"], "inner"
)
# Realiza la cadena de joins
df_final_join_pcr_tradicional = df_join_contextoperation_thirdpartymovementpolicy.join(
    df_policydco_select, df_join_contextoperation_thirdpartymovementpolicy["id"] == df_policydco_select["operationpk"],
    "inner"
).join(
    df_prepolicy_select, df_policydco_select["dcoid"] == df_prepolicy_select["pk"],
    "inner"
).join(
    df_join_producto,
    df_policydco_select["agregatedobjectid"] == df_join_producto["agregatedpolicyid"],
    "inner"
).join(
    df_join_rol,
    df_join_contextoperation_thirdpartymovementpolicy["thirdparty"] == df_join_rol["thirdpartyid"],
    "inner"
).join(
    df_join_riesgo,
    df_join_contextoperation_thirdpartymovementpolicy["id"] == df_join_riesgo["operationpk"],
    "inner"
).join(
    df_join_acuerdo,
    df_prepolicy_select["static"] == df_join_acuerdo["rsa_policyid"],
    "inner"
).join(
    df_join_reaseguro,
    (df_policydco_select["operationpk"] == df_join_reaseguro["cor_id"]) & (df_policydco_select["agregatedobjectid"] == df_join_reaseguro["agregatedpolicyid"]),
    "inner"
).join(
    df_join_cobertura,
    df_join_contextoperation_thirdpartymovementpolicy["id"] == df_join_cobertura["operationpk"],
    "left"
)
df_final_pcr_tradicional_select = df_final_join_pcr_tradicional.select (    
        lit("Acsel-e").alias("ORIGEN"),                                                                                               # campo 1 ORIGEN
        df_join_contextoperation_thirdpartymovementpolicy["id"],
        df_join_contextoperation_thirdpartymovementpolicy["operationpk"],
        df_join_producto["companiainput"].alias("COMPANIA"),
        df_join_cobertura["cobramocontableinput"].alias("RAMO"),
        df_join_producto["ramoinput"].alias("DESC_RAMO"),
        df_join_producto["codigoproductoinput"].alias("PRODUCTO"),
        df_join_producto["descripcionproductoinput"].alias("DESC_PRODUCTO"),
        df_join_producto["ramovalue"].alias("RAMO_TECNICO"),
        df_join_producto["ramoinput"].alias("DESC_RAMO_TECNICO"),
        df_join_acuerdo["ast_numeroacu"].alias("POLIZA_MATRIZ"),
        df_prepolicy_select["numeropolizainput"].alias("POLIZA"),
        df_prepolicy_select["oficinainput"].alias("SUCURSAL"),
        df_join_riesgo["numunidriesgoinput"].alias("UNIDAD_RIESGO"),
        df_join_cobertura["cobcodigocoberturainput"].alias("COBERTURA"),
        df_join_cobertura["cobdescripcioncobertinput"].alias("DESC_COBERTURA"),
        df_policydco_select["operationpk"].alias("ENDOSO"),
        df_join_contextoperation_thirdpartymovementpolicy["time_stamp"].alias("FECHA_ENDOSO"),
        df_join_acuerdo["unidadnegocioinput"].alias("UNIDAD_NEGOCIO"),
        df_policydco_select["initialdate"].alias("FECHA_INICIO_POLIZA_MATRIZ"),
        df_policydco_select["finishdate"].alias("FECHA_FIN_POLIZA_MATRIZ"),
        df_join_riesgo["initialdate"].alias("FECHA_INICIO_COBERTURA"),
        df_join_riesgo["finishdate"].alias("FECHA_FIN_COBERTURA"),
        df_join_rol["thirdpartyid"].alias("ID_TOMADOR_POLIZA"),
        df_join_rol["nombrepredeterminadoinput"].alias("TOMADOR_POLIZA"),
        df_prepolicy_select["frecuenciapagoinput"].alias("FRECUENCIA_PAGO"),
        df_join_reaseguro["codigoreaseguradorinput"].alias("CODREASEG"),
        df_join_reaseguro["clasereaseguroinput"].alias("NOMREASEG"),
        df_join_reaseguro["thirdpartyname"].alias("CLASE_REASEGURADOR"),
        df_prepolicy_select["tipopolizainput"].alias("dsc_tipo_poliza"),
        #df_join_rol["rol_id"].alias("cod_poliza_rol"),
        df_join_rol["rol_description"].alias("dsc_poliza_rol"),

        df_join_contextoperation_thirdpartymovementpolicy["time_stamp_partition"].alias("PERIODO")                                                              #campo gold PERIODO
    
)
df_final_pcr_tradicional_select.write.mode("overwrite").partitionBy("PERIODO").parquet("s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL/TABLA_INFO/")
df_join_contextoperation_thirdpartymovementpolicy 
INNER JOIN df_policydco_select ON df_join_contextoperation_thirdpartymovementpolicy[id] = df_policydco_select[operationpk]
INNER JOIN df_prepolicy_select ON df_policydco_select[dcoid] = df_prepolicy_select[pk]

INNER JOIN df_join_producto ON df_policydco_select[agregatedobjectid] = df_join_producto[agregatedpolicyid]

INNER JOIN df_join_rol ON df_join_contextoperation_thirdpartymovementpolicy[thirdpartyid] = df_join_rol[thirdpartyid]

INNER JOIN df_join_riesgo ON df_join_contextoperation_thirdpartymovementpolicy[id] = df_join_riesgo[operationpk]

INNER JOIN df_join_acuerdo ON df_prepolicy_select[static] = df_join_acuerdo[rsa_policyid]

INNER JOIN df_join_reaseguro ON df_policydco_select[operationpk] = df_join_reaseguro[cor_id] AND df_policydco_select[agregatedobjectid] = df_join_reaseguro[agregatedpolicyid]

LEFT JOIN df_join_cobertura ON df_join_contextoperation_thirdpartymovementpolicy[id] = df_join_cobertura[operationpk]
df_test_join.explain()
df_test_join.write.mode("overwrite").parquet("s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL/PRUEBA_JOIN/")
df_join_contextoperation_thirdpartymovementpolicy.show(1, truncate = False)
df_contextoperation_par_filter.createOrReplaceTempView("df_contextoperation_par_filter")
df_thirdpartymovementpolicy_par_select = df_thirdpartymovementpolicy_par.select("operationpk", "thirdparty")
df_thirdpartymovementpolicy_par_select.createOrReplaceTempView("df_thirdpartymovementpolicy_par_select")
#CASE_SYP_PROPERTY

CASE_SYP_PROPERTY = {
    '05': 'rrc.issuedpremium.firedoctype',
    '13': 'rrc.issuedpremium.earthquakedoctype',
    '10': 'rrc.issuedpremium.liabilitydoctype',
    '12': 'rrc.issuedpremium.subtractiondoctype',
    '01': 'rrc.issuedpremium.assistsdoctype',
    '24': 'rrc.issuedpremium.unemploymentdoctype',
    '02': 'rrc.commisionintermediary',
    '03': 'rrc.commissioncollection.earthquakedoctype',
    '04': 'bondholder.rol'
}

#DEF DESCRIPTION_STST_SYSTEMPROPERTY


def description_stst_systemproperty(df_stst_systemproperty, syp_property_value):

    df_stst_systemproperty_filter = df_stst_systemproperty.filter(lower(col("SYP_PROPERTY")) == syp_property_value.lower())
    
    exploded_stst_systemproperty_filter = df_stst_systemproperty_filter.select(explode(split(df_stst_systemproperty_filter["SYP_VALUE"], ",")).alias("DESCRIPTION"))
    
    trimmed_stst_systemproperty_filter = exploded_stst_systemproperty_filter.select(lower(trim(exploded_stst_systemproperty_filter["DESCRIPTION"])).alias("DESCRIPTION"))    
    
    return trimmed_stst_systemproperty_filter
spark.sql("USE `datalake-dev-silver-catalog-db`")
#--COMPANIA, RAMO, DESC_RAMO, COBERTURA, DESC_COBERTURA, 
df_cte_compania = spark.sql("""
    SELECT
        CASE 
            WHEN rg.rig_companycode = '0.0' THEN 'Seguros Alfa S.A.'
            WHEN rg.rig_companycode = '50.0'THEN 'Seguros Alfa S.A.'
        END AS COMPANIA,
        replace(cb.cobramocontableinput, '.0', '') AS RAMO,
        pp.ramoinput AS DESC_RAMO,
        replace(cb.cobcodigocoberturainput, '.0', '') AS COBERTURA,
        cb.cobdescripcioncobertinput AS DESC_COBERTURA,
        ctx.time_stamp,
        ctx.id as id_cte_compania,
        ctx.time_stamp_partition
    FROM df_contextoperation_par_filter ctx
    INNER JOIN acsele_alfa_full_coveragedco cdco ON ctx.id = cdco.operationpk
    INNER JOIN acsele_alfa_full_cobertura cb ON cdco.dcoid = cb.pk 
    INNER JOIN acsele_alfa_full_policydco pdco ON cdco.operationpk = pdco.operationpk
    INNER JOIN acsele_alfa_full_prepolicy ppy  ON pdco.dcoid = ppy.pk
    INNER JOIN acsele_alfa_full_agregatedpolicy ap ON pdco.agregatedobjectid = ap.agregatedpolicyid
    INNER JOIN acsele_alfa_full_product pd ON ap.productid = pd.productid
    INNER JOIN acsele_alfa_full_preproduct pp ON pd.pro_dcoid = pp.pk
    INNER JOIN acsele_alfa_full_ri_reinsurancegroup rg ON rg.code in (replace(cb.cobramoreaseguroinput, '.0', ''))
    INNER JOIN acsele_alfa_full_stri_reinsurancekind rk ON rg.rekind_id = rk.rekind_id 
""")
#775088
#--PRODUCTO, DESC_PRODUCTO,RAMO_TECNICO, DESC_RAMO_TECNICO, POLIZA, SUCURSAL, ENDOSO, FECHA_ENDOSO, MESPRO, FECHA_INICIO_POLIZA_MATRIZ, FECHA_FIN_POLIZA_MATRIZ, TIPO_COASEGURO, PORCPART_ALFA
df_cte_producto = spark.sql ("""
    SELECT
        ap.productid AS PRODUCTO,
        pr.descripcionproductoinput AS DESC_PRODUCTO,
        pr.ramovalue AS RAMO_TECNICO,
        pr.ramovalue AS DESC_RAMO_TECNICO,
        pp.numeropolizainput AS POLIZA,
        pp.oficinainput AS SUCURSAL,
        pdco.operatiONpk AS ENDOSO,
        ctx.time_stamp AS FECHA_ENDOSO,
        date_format(ctx.time_stamp, 'yyyyMM') AS MESPRO,
        pdco.initialdate AS FECHA_INICIO_POLIZA_MATRIZ,
        pdco.finishdate AS FECHA_FIN_POLIZA_MATRIZ,
        ctx.time_stamp,
        ctx.id as id_cte_producto
    FROM df_contextoperation_par_filter ctx
    INNER JOIN acsele_alfa_full_policydco pdco ON ctx.id = pdco.operatiONpk
    INNER JOIN acsele_alfa_full_prepolicy pp ON pdco.dcoid = pp.pk
    INNER JOIN acsele_alfa_full_agregatedpolicy ap  ON pdco.agregatedobjectid = ap.agregatedpolicyid
    INNER JOIN acsele_alfa_full_financialplandco fpdco ON pdco.operatiONpk = fpdco.operatiONpk and pdco.agregatedobjectid = fpdco.agregatedparentid
    INNER JOIN acsele_alfa_full_product pt ON ap.productid = pt.productid
    INNER JOIN acsele_alfa_full_preproduct pr ON pt.pro_dcoid = pr.pk
""")
#--POLIZA_MATRIZ, UNIDAD_NEGOCIO

df_cte_poliza_matriz = spark.sql ("""

SELECT 	
    ags.ast_numeroacu AS POLIZA_MATRIZ,
    prea.unidadnegocioinput AS UNIDAD_NEGOCIO,
    ctx.time_stamp,
    ctx.id as id_cte_poliza_matriz
FROM df_contextoperation_par_filter ctx
INNER JOIN acsele_alfa_full_policydco pdco ON ctx.id = pdco.operatiONpk
INNER JOIN acsele_alfa_full_stps_relationagreementpolicy rap  ON pdco.agregatedobjectid = rap.rsa_policyid
INNER JOIN acsele_alfa_full_stps_agreementstaticdat ags ON rap.rsa_ast_id = ags.ast_id
INNER JOIN acsele_alfa_full_stag_agreementversion av ON rap.rsa_ast_id = av.ast_id
INNER JOIN acsele_alfa_full_preagreement prea ON av.dcoid = prea.pk
WHERE ctx.time_stamp between av.ast_fecini and av.ast_fecfin;
""")
#--UNIDAD_RIESGO, FECHA_INICIO_COBERTURA, FECHA_FIN_COBERTURA

df_cte_unidad_riesgo = spark.sql ("""
SELECT   
    pru.numunidriesgoinput AS UNIDAD_RIESGO,
    rskdco.initialdate AS FECHA_INICIO_COBERTURA,
    rskdco.finishdate AS FECHA_FIN_COBERTURA,
    ctx.time_stamp,
    ctx.status,
    ctx.id as id_cte_unidad_riesgo
FROM df_contextoperation_par_filter ctx
INNER JOIN acsele_alfa_full_riskunitdco rskdco ON ctx.id = rskdco.operationpk
INNER JOIN acsele_alfa_full_preriskunits pru ON rskdco.dcoid = pru.pk
""")
#--FRECUENCIA_PAGO

df_cte_frecuencia_pago = spark.sql ("""
SELECT   
    pp.frecuenciapagoinput as FRECUENCIA_PAGO,
    ctx.time_stamp,
	ctx.status,
	ctx.id as id_cte_frecuencia_pago
FROM df_contextoperation_par_filter ctx
INNER JOIN acsele_alfa_full_policydco pdco ON ctx.id = pdco.operationpk
INNER JOIN acsele_alfa_full_prepolicy pp ON pdco.dcoid = pp.pk
INNER JOIN acsele_alfa_full_financialplandco fpdco ON pdco.agregatedobjectid = fpdco.agregatedparentid AND pdco.operationpk = fpdco.operationpk
""")
#--ID_TOMADOR_POLIZA

df_cte_id_tomador_poliza = spark.sql ("""
SELECT DISTINCT
    tp.tpt_id AS ID_TOMADOR_POLIZA,
    ctx.id as id_cte_id_tomador_poliza
FROM df_contextoperation_par_filter ctx
INNER JOIN df_thirdpartymovementpolicy_par_select tpmp ON ctx.id = tpmp.OPERATIONPK
INNER JOIN acsele_alfa_full_stpo_policyparticipation pp ON cast(tpmp.THIRDPARTY as BIGINT) = pp.thirdpartyid
INNER JOIN acsele_alfa_full_stte_thirdparty tp ON cast(tpmp.THIRDPARTY as BIGINT) = tp.tpt_id
INNER JOIN acsele_alfa_full_prethirdparty ptp ON tp.iddco = ptp.pk
""")

#--TOMADOR_POLIZA
SYP_PROPERTY_CODE = '04'
SYP_PROPERTY_VALUE = CASE_SYP_PROPERTY.get(SYP_PROPERTY_CODE)
descriptions_tomador_poliza = description_stst_systemproperty(df_stst_systemproperty, SYP_PROPERTY_VALUE)
descriptions_tomador_poliza.createOrReplaceTempView("descriptions_tomador_poliza")


df_cte_policyparticipation_pre = spark.sql (
"""
    WITH role_pre AS (SELECT * FROM acsele_alfa_full_stts_role rl WHERE lower(rl.ROL_DESCRIPTION) IN (SELECT DESCRIPTION FROM descriptions_tomador_poliza)
    ),
    policyparticipation_pre AS (
        SELECT DISTINCT 
            pp.thirdpartyid 
        FROM 
            acsele_alfa_full_stpo_policyparticipation pp 
        WHERE 
            pp.rol_id IN (SELECT DISTINCT rol_id FROM role_pre)
    )
    SELECT thirdpartyid FROM policyparticipation_pre
"""
)
df_cte_policyparticipation_pre.createOrReplaceTempView("df_cte_policyparticipation_pre")

df_cte_tomador_poliza = spark.sql (
"""
select
    DISTINCT
    ptp.nombrepredeterminadoinput as TOMADOR_POLIZA,
   ctx.id as id_cte_tomador_poliza
from df_contextoperation_par_filter ctx
inner join df_thirdpartymovementpolicy_par_select tpmp on ctx.id = tpmp.OPERATIONPK
inner join df_cte_policyparticipation_pre pp on cast(tpmp.THIRDPARTY as BIGINT) = pp.thirdpartyid
inner join acsele_alfa_full_stte_thirdparty tp on pp.thirdpartyid = tp.tpt_id
inner join acsele_alfa_full_prethirdparty ptp on tp.iddco = ptp.pk
"""
)

#PRIMA_EMITIDA

SYP_PROPERTY_CODE = '13'
SYP_PROPERTY_VALUE = CASE_SYP_PROPERTY.get(SYP_PROPERTY_CODE)
descriptions_prima_emitida = description_stst_systemproperty(df_stst_systemproperty, SYP_PROPERTY_VALUE)
descriptions_prima_emitida.createOrReplaceTempView("descriptions_prima_emitida")

df_cte_prima_emitida = spark.sql (""" 

SELECT 
    sum(case 
            when dt.dty_isdebit = 1 then CAST(oi.amount AS decimal(38,10))
            else CAST(oi.amount AS decimal(38,10)) * (-1) 
        end) as PRIMA_EMITIDA,
    ctx.id as id_cte_prima_emitida
FROM df_contextoperation_par_filter ctx
INNER JOIN acsele_alfa_full_openitem oi ON ctx.id = oi.operationpk
INNER JOIN acsele_alfa_full_stca_doctype dt ON dt.dty_id = oi.dty_id
WHERE lower(oi.opm_substatus) <> 'notapplied'
    and lower(dt.dty_description) in (SELECT DESCRIPTION FROM descriptions_prima_emitida)
GROUP BY ctx.id
""")

#--PORC_COM_INTERMEDIACION

df_cte_porcentaje_comision_intermediacion = spark.sql ("""
SELECT
    pi.porcentajecomisioninput as PORC_COM_INTERMEDIACION,
	ctx.id as id_cte_porc_com_intermediacion
FROM df_contextoperation_par_filter ctx
INNER JOIN acsele_alfa_full_stpo_policyparticipation pp ON ctx.id = pp.operationpk
INNER JOIN acsele_alfa_full_stpo_policyparticipationdco ppd ON pp.operationpk = ppd.operationpk and pp.agregatedobjectid = ppd.agregatedobjectid
INNER JOIN acsele_alfa_full_participacionintermediarios pi ON ppd.dcoid = pi.pk
""")


#--COMISION_INTERMEDIACION

SYP_PROPERTY_CODE = '02'
SYP_PROPERTY_VALUE = CASE_SYP_PROPERTY.get(SYP_PROPERTY_CODE)
descriptions_iva_comision = description_stst_systemproperty(df_stst_systemproperty, SYP_PROPERTY_VALUE)
descriptions_iva_comision.createOrReplaceTempView("descriptions_iva_comision")

df_cte_comision_intermediacion = spark.sql(""" 

SELECT
     sum(cast(det.value as decimal(38,10))) as COMISION_INTERMEDIACION,
    ctx.id as id_cte_comission_intermediacion
FROM df_contextoperation_par_filter ctx
INNER JOIN acsele_alfa_full_openitem oi ON ctx.id = oi.operationpk
INNER JOIN  acsele_alfa_full_stca_doctype dt ON dt.dty_id = oi.dty_id
INNER JOIN acsele_alfa_full_uaadetail det ON det.openitemid = oi.openitemid
INNER JOIN acsele_alfa_full_property pp ON pp.propertyid = cast(det.property as bigint)
WHERE lower(oi.opm_substatus) <> 'notapplied'
and lower(dt.dty_description) in ( SELECT DESCRIPTION FROM descriptions_iva_comision)
and lower(pp.simbolo) in ('comision')
group by ctx.id;
""")
#--IVA_COMISION_INTERMEDIACION --IVA_COMISION_INTERMEDIACION

SYP_PROPERTY_CODE = '02'
SYP_PROPERTY_VALUE = CASE_SYP_PROPERTY.get(SYP_PROPERTY_CODE)
descriptions_iva_comision = description_stst_systemproperty(df_stst_systemproperty, SYP_PROPERTY_VALUE)
descriptions_iva_comision.createOrReplaceTempView("descriptions_iva_comision")

df_cte_iva_comision_intermediacion = spark.sql(""" 

SELECT
     sum(cast(det.value as decimal(38,10))) as IVA_COMISION_INTERMEDIACION,
    ctx.id as id_cte_iva_comission_intermediacion
FROM df_contextoperation_par_filter ctx
INNER JOIN acsele_alfa_full_openitem oi ON ctx.id = oi.operationpk
INNER JOIN  acsele_alfa_full_stca_doctype dt ON dt.dty_id = oi.dty_id
INNER JOIN acsele_alfa_full_uaadetail det ON det.openitemid = oi.openitemid
INNER JOIN acsele_alfa_full_property pp ON pp.propertyid = cast(det.property as bigint)
WHERE lower(oi.opm_substatus) <> 'notapplied'
and lower(dt.dty_description) in ( SELECT DESCRIPTION FROM descriptions_iva_comision)
and lower(pp.simbolo) in ('ivacomision')
group by ctx.id;

""")

#--COMISION_RECAUDO

SYP_PROPERTY_CODE = '03'
SYP_PROPERTY_VALUE = CASE_SYP_PROPERTY.get(SYP_PROPERTY_CODE)
descriptions_comision_recaudo = description_stst_systemproperty(df_stst_systemproperty, SYP_PROPERTY_VALUE)
descriptions_comision_recaudo.createOrReplaceTempView("descriptions_comision_recaudo")


df_cte_comision_recaudo = spark.sql("""

SELECT
    sum(case 
            when dt.dty_isdebit = 0 then CAST(oi.amount AS decimal(38,10)) 
            else CAST(oi.amount AS decimal(38,10)) * (-1) 
        end) as COMISION_RECAUDO,
    ctx.id as id_cte_comision_recaudo
FROM df_contextoperation_par_filter ctx
INNER JOIN acsele_alfa_full_openitem oi ON ctx.id = oi.operatiONpk
INNER JOIN acsele_alfa_full_stca_doctype dt ON dt.dty_id = oi.dty_id
and lower(oi.opm_substatus) <> 'notapplied'
and lower(dt.dty_description) in (SELECT DESCRIPTION FROM descriptions_comision_recaudo)
group by ctx.id;

""")
#--NOMREASEG

df_cte_nomreaseg = spark.sql (
    """
select
  ctx.id as id_cte_nomreaseg
,rod.thirdpartyid as thirdparty_id
, rod.thirdpartyname as NOMREASEG
, rod.participationid as participation_id
from df_contextoperation_par_filter ctx
inner join acsele_alfa_full_policydco pdco on ctx.id = pdco.operationpk
inner join acsele_alfa_full_reinsuranceoperation ro on ro.cor_id = pdco.operationpk and ro.agregatedpolicyid = pdco.agregatedobjectid
inner join acsele_alfa_full_reinsuranceoperationgroupinfo rog on rog.operationid = ro.operationid
inner join acsele_alfa_full_reinsuranceoperationcomponent roc on roc.reinsurancegroupinfoid = rog.reinsurancegroupinfoid
inner join acsele_alfa_full_reinsuranceoperationdetail rod on rod.reinopercomponentid = roc.reinopercomponentid
inner join acsele_alfa_full_ri_reinsurancegroup rg on rg.reinsurancegroupid = rog.groupid
WHERE (lower(roc.contracttype) = 'mandatoryfacultativecontract' or lower(roc.contracttype) = 'facultativereinsurancecontract')
group by ctx.id
,rod.thirdpartyid
, rod.thirdpartyname
, rod.participationid""")

#--CLASE_REASEGURADOR

df_cte_clase_reasegurador = spark.sql ("""  
SELECT  
DISTINCT
ctx.id as id_cte_clase_reasegurador,
ra.clasereaseguroinput as CLASE_REASEGURADOR,
ra.codigoreaseguradorinput as CODREASEG
from df_contextoperation_par_filter ctx
inner join df_thirdpartymovementpolicy_par_select tpmp on ctx.id = tpmp.OPERATIONPK
inner join acsele_alfa_full_stpo_policyparticipation pp on cast(tpmp.THIRDPARTY as BIGINT) = pp.thirdpartyid
inner join acsele_alfa_full_thirdpartyrole tpr on cast(tpmp.THIRDPARTY as BIGINT) = tpr.thirdpartyid
left join acsele_alfa_full_reasegurador ra  on tpr.iddcorole = ra.pk

""")
#--PRIMACEDAUT, PRIMACEDFAC, COMISIONREASEGAUT, COMISIONREASEGFAC --3.845.773

df_cte_premium_detail = spark.sql (""" 
SELECT
    ctx.id as id_cte_premium_detail,
    ROG.OPERATIONID,
    ROC.CONTRACTTYPE,
    SUM(RD.ROD_PRORATEDPREMIUM) AS PREMIUM_DETAIL
from df_contextoperation_par_filter ctx
INNER JOIN acsele_alfa_full_policydco pdco on ctx.id = pdco.operationpk
INNER JOIN acsele_alfa_full_reinsuranceoperation ro on ro.cor_id = pdco.operationpk and ro.agregatedpolicyid = pdco.agregatedobjectid
INNER JOIN acsele_alfa_full_reinsuranceoperationgroupinfo ROG ON ROG.OPERATIONID = RO.OPERATIONID
INNER JOIN acsele_alfa_full_reinsuranceoperationcomponent ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
INNER JOIN acsele_alfa_full_reinsuranceoperationdetail RD ON RD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
GROUP BY ROG.OPERATIONID, ROC.CONTRACTTYPE, ctx.id
""")

df_cte_premium_detail.createOrReplaceTempView("df_cte_premium_detail")


df_cte_reinsurance_commission = spark.sql (""" 
SELECT 
    ctx.id as id_cte_reinsurance_commission,
    ROG.OPERATIONID,
    ROC.CONTRACTTYPE,
    SUM(PT.ROPP_VALUE) as REINSURANCE_COMMISSION
FROM df_contextoperation_par_filter ctx
INNER JOIN acsele_alfa_full_policydco pdco ON ctx.id = pdco.operationpk
INNER JOIN acsele_alfa_full_reinsuranceoperation RO ON ro.cor_id = pdco.operationpk and ro.agregatedpolicyid = pdco.agregatedobjectid
INNER JOIN acsele_alfa_full_reinsuranceoperationgroupinfo ROG ON ROG.OPERATIONID = RO.OPERATIONID
INNER JOIN acsele_alfa_full_reinsuranceoperationcomponent ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
INNER JOIN acsele_alfa_full_reinsuranceoperationdetail ROD ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
INNER JOIN acsele_alfa_full_ri_reinsurancegroup RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
INNER JOIN acsele_alfa_full_reinsuranceoperationproperty PT ON  PT.ROD_ID = ROD.REINSURANCEOPERATIONDETAILID
GROUP BY ctx.id,  ROG.OPERATIONID, ROC.CONTRACTTYPE
""")
df_cte_reinsurance_commission.createOrReplaceTempView("df_cte_reinsurance_commission")


df_cte_primaced_comisionreaseg = spark.sql ("""

SELECT  DISTINCT
    ctx.id as id_cte_primaced_comisionreaseg,
    IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0,cpd.PREMIUM_DETAIL) AS PRIMACEDAUT,
    IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),cpd.PREMIUM_DETAIL, 0) AS PRIMACEDFAC,
    IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0, crc.REINSURANCE_COMMISSION) AS COMISIONREASEGAUT,
    IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),crc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGFAC
from df_contextoperation_par_filter ctx
INNER JOIN acsele_alfa_full_policydco pdco on ctx.id = pdco.operationpk
INNER JOIN acsele_alfa_full_reinsuranceoperation ro on ro.cor_id = pdco.operationpk and ro.agregatedpolicyid = pdco.agregatedobjectid
INNER JOIN acsele_alfa_full_reinsuranceoperationgroupinfo ROG ON ROG.OPERATIONID = RO.OPERATIONID
INNER JOIN acsele_alfa_full_reinsuranceoperationcomponent ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
INNER JOIN acsele_alfa_full_reinsuranceoperationdetail ROD ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
INNER JOIN acsele_alfa_full_ri_reinsurancegroup RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
INNER JOIN acsele_alfa_full_policydco PLO ON RO.cor_id = PLO.operationpk
LEFT JOIN df_cte_premium_detail cpd ON cpd.id_cte_premium_detail = ctx.id
LEFT JOIN df_cte_reinsurance_commission crc ON crc.id_cte_reinsurance_commission = ctx.id

""")



#--PRIMACEDAUT_INT,PRIMACEDAUT_EXT,PRIMACEDFAC_INT,PRIMACEDFAC_EXT,COMISIONREASEGAUT_INT,COMISIONREASEGAUT_EXT,COMISIONREASEGFAC_INT,COMISIONREASEGFAC_EXT 

df_cte_primaced_comisionreaseg.createOrReplaceTempView("df_cte_primaced_comisionreaseg")

df_cte_primaced_comisionreaseg_intext = spark.sql (""" 
select
distinct
  ctx.id as id_cte_primaced_comisionreaseg_intext,
  ra.CLASEREASEGUROINPUT CLASE_REASEGURO,
    IF(RA.CLASEREASEGUROINPUT = 'Interior', cpc.PRIMACEDAUT, 0) AS PRIMACEDAUT_INT,
    IF(RA.CLASEREASEGUROINPUT = 'Exterior', cpc.PRIMACEDAUT, 0) AS PRIMACEDAUT_EXT,
    IF(RA.CLASEREASEGUROINPUT = 'Interior', cpc.PRIMACEDFAC, 0) AS PRIMACEDFAC_INT,
    IF(RA.CLASEREASEGUROINPUT = 'Exterior', cpc.PRIMACEDFAC, 0) AS PRIMACEDFAC_EXT,
    IF(RA.CLASEREASEGUROINPUT = 'Interior', cpc.COMISIONREASEGAUT, 0) AS COMISIONREASEGAUT_INT,
    IF(RA.CLASEREASEGUROINPUT = 'Exterior', cpc.COMISIONREASEGAUT, 0) AS COMISIONREASEGAUT_EXT,
    IF(RA.CLASEREASEGUROINPUT = 'Interior', cpc.COMISIONREASEGFAC, 0) AS COMISIONREASEGFAC_INT,
    IF(RA.CLASEREASEGUROINPUT = 'Exterior', cpc.COMISIONREASEGFAC, 0) AS COMISIONREASEGFAC_EXT
from df_contextoperation_par_filter ctx
inner join acsele_alfa_full_policydco pdco on ctx.id = pdco.operationpk
inner join acsele_alfa_full_reinsuranceoperation ro on ro.cor_id = pdco.operationpk and ro.agregatedpolicyid = pdco.agregatedobjectid
inner join acsele_alfa_full_reinsuranceoperationgroupinfo rog on rog.operationid = ro.operationid
inner join acsele_alfa_full_reinsuranceoperationcomponent roc on roc.reinsurancegroupinfoid = rog.reinsurancegroupinfoid
inner join acsele_alfa_full_reinsuranceoperationdetail rod on rod.reinopercomponentid = roc.reinopercomponentid
inner join acsele_alfa_full_ri_reinsurancegroup rg on rg.reinsurancegroupid = rog.groupid
inner join acsele_alfa_full_thirdpartyrole tpr ON tpr.thirdpartyid = rod.thirdpartyid
inner join acsele_alfa_full_reasegurador ra  on tpr.iddcorole = ra.pk
LEFT JOIN df_cte_primaced_comisionreaseg cpc ON cpc.id_cte_primaced_comisionreaseg = ctx.id
""")
#df_cte_producto.show()
#df_cte_unidad_riesgo.show()
#df_cte_frecuencia_pago.show()
#df_cte_poliza_matriz.show()
#warning !!!! df_cte_clase_reasegurador.show()
df_cte_id_tomador_poliza.show()
#df_cte_tomador_poliza.show()
#df_cte_primaced_comisionreaseg.show()
#df_cte_primaced_comisionreaseg_intext.show()
#df_cte_nomreaseg.show()
#df_cte_compania.show()
#df_cte_prima_emitida.show()
#df_cte_comision_recaudo.show()
#df_cte_iva_comision_intermediacion.show()
#df_cte_comision_intermediacion.show()
#df_cte_porcentaje_comision_intermediacion.show()
df_cte_clase_reasegurador.count()
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from functools import reduce

# --- Asumimos que todos tus DataFrames ya existen ---
# df_cte_producto, df_cte_unidad_riesgo, etc.

# 1. Crea una lista con los DataFrames y sus nombres
dfs_a_contar = [
    ("df_cte_producto", df_cte_producto),
    ("df_cte_unidad_riesgo", df_cte_unidad_riesgo),
    ("df_cte_frecuencia_pago", df_cte_frecuencia_pago),
    ("df_cte_poliza_matriz", df_cte_poliza_matriz),
    #("df_cte_clase_reasegurador", df_cte_clase_reasegurador),
    #("df_cte_id_tomador_poliza", df_cte_id_tomador_poliza),
    #("df_cte_tomador_poliza", df_cte_tomador_poliza),
    ("df_cte_primaced_comisionreaseg", df_cte_primaced_comisionreaseg),
    ("df_cte_primaced_comisionreaseg_intext", df_cte_primaced_comisionreaseg_intext),
    ("df_cte_nomreaseg", df_cte_nomreaseg),
    ("df_cte_compania", df_cte_compania),
    ("df_cte_prima_emitida", df_cte_prima_emitida),
    ("df_cte_comision_recaudo", df_cte_comision_recaudo),
    ("df_cte_iva_comision_intermediacion", df_cte_iva_comision_intermediacion),
    ("df_cte_comision_intermediacion", df_cte_comision_intermediacion),
    ("df_cte_porcentaje_comision_intermediacion", df_cte_porcentaje_comision_intermediacion)
]

# 2. Crea un DataFrame para cada conteo y únelos
# Usamos 'reduce' para aplicar 'union' a toda la lista de forma elegante
union_df = reduce(
    DataFrame.unionAll,
    [df.select(lit(name).alias("nombre_df")) for name, df in dfs_a_contar]
)

# 3. Agrupa y cuenta todo en un solo job de Spark
conteos_finales = union_df.groupBy("nombre_df").count()

#4. Order by count
conteos_finales = conteos_finales.orderBy("count")


# 4. Muestra el resultado final
print("--- Conteo de Registros por DataFrame ---")
conteos_finales.show(truncate=False)
# 4. Muestra el resultado final
print("--- Conteo de Registros por DataFrame ---")
conteos_finales.show(truncate=False)
###################################################################
###################### FINAL JOIN  ################################
###################################################################
#-----------------------------------------------------------------#
#---------------------- Join  Tables -----------------------------#
#-----------------------------------------------------------------#
df_campos_final_pcr_join = df_cte_producto.join(
    df_cte_unidad_riesgo, df_cte_producto["id_cte_producto"] == df_cte_unidad_riesgo["id_cte_unidad_riesgo"], "left"
    ).join(
    df_cte_frecuencia_pago, df_cte_producto["id_cte_producto"] == df_cte_frecuencia_pago["id_cte_frecuencia_pago"], "left"
    ).join(
    df_cte_poliza_matriz, df_cte_producto["id_cte_producto"] == df_cte_poliza_matriz["id_cte_poliza_matriz"], "left"
    ).join(
    df_cte_clase_reasegurador, df_cte_producto["id_cte_producto"] == df_cte_clase_reasegurador["id_cte_clase_reasegurador"], "left"
    ).join(
    df_cte_id_tomador_poliza, df_cte_producto["id_cte_producto"] == df_cte_id_tomador_poliza["id_cte_id_tomador_poliza"], "left" 
    ).join(
    df_cte_tomador_poliza, df_cte_producto["id_cte_producto"] == df_cte_tomador_poliza["id_cte_tomador_poliza"], "left"
    ).join(
    df_cte_primaced_comisionreaseg, df_cte_producto["id_cte_producto"] == df_cte_primaced_comisionreaseg["id_cte_primaced_comisionreaseg"], "left"
    ).join(
    df_cte_primaced_comisionreaseg_intext, df_cte_producto["id_cte_producto"] == df_cte_primaced_comisionreaseg_intext["id_cte_primaced_comisionreaseg_intext"], "left"
    ).join(
    df_cte_nomreaseg, df_cte_producto["id_cte_producto"] == df_cte_nomreaseg["id_cte_nomreaseg"], "left"
    ).join(
    df_cte_compania, df_cte_producto["id_cte_producto"] == df_cte_compania["id_cte_compania"], "left"
    ).join(
    df_cte_prima_emitida, df_cte_producto["id_cte_producto"] == df_cte_prima_emitida["id_cte_prima_emitida"], "left"
    ).join(
    df_cte_comision_recaudo, df_cte_producto["id_cte_producto"] == df_cte_comision_recaudo["id_cte_comision_recaudo"], "left"
    ).join(
    df_cte_iva_comision_intermediacion, df_cte_producto["id_cte_producto"] == df_cte_iva_comision_intermediacion["id_cte_iva_comission_intermediacion"], "left"
    ).join(
    df_cte_comision_intermediacion, df_cte_producto["id_cte_producto"] == df_cte_comision_intermediacion["id_cte_comission_intermediacion"], "left"
    ).join(
    df_cte_porcentaje_comision_intermediacion, df_cte_producto["id_cte_producto"] == df_cte_porcentaje_comision_intermediacion["id_cte_porc_com_intermediacion"], "left"
    )
df_campos_final_pcr_select = df_campos_final_pcr_join.select(
    lit("Acsel-e").alias("ORIGEN"),                                                                                               # campo 1 ORIGEN
    df_cte_compania["COMPANIA"].alias("COMPANIA"),                                                                                # campo 2 COMPANIA
    df_cte_compania["RAMO"].alias("RAMO"),                                                                                        # campo 3 RAMO
    df_cte_compania["DESC_RAMO"].alias("DESC_RAMO"),                                                                              # campo 4 DESC_RAMO
    df_cte_producto["PRODUCTO"].alias("PRODUCTO"),                                                                                # campo 5 DESC_RAMO
    df_cte_producto["DESC_PRODUCTO"].alias("DESC_PRODUCTO"),                                                                      # campo 6 DESC_PRODUCTO
    df_cte_producto["RAMO_TECNICO"].alias("RAMO_TECNICO"),                                                                        # campo 7 RAMO_TECNICO
    df_cte_producto["DESC_RAMO_TECNICO"].alias("DESC_RAMO_TECNICO"),                                                              # campo 8 DESC_RAMO_TECNICO
    df_cte_poliza_matriz["POLIZA_MATRIZ"].alias("POLIZA_MATRIZ"),                                                                 # campo 9 POLIZA_MATRIZ
    df_cte_producto["POLIZA"].alias("POLIZA"),                                                                                    # campo 10 POLIZA
    df_cte_producto["SUCURSAL"].alias("SUCURSAL"),                                                                                # campo 11 SUCURSAL
    lit(None).alias("MODULO"),                                                                                                    # campo 12 SUCURSAL        
    df_cte_unidad_riesgo["UNIDAD_RIESGO"].alias("UNIDAD_RIESGO"),                                                                 # campo 13 DESC_COBERTURA
    df_cte_compania["COBERTURA"].alias("COBERTURA"),                                                                              # campo 14 COBERTURA
    df_cte_compania["DESC_COBERTURA"].alias("DESC_COBERTURA"),                                                                    # campo 15 DESC_COBERTURA       
    df_cte_producto["ENDOSO"].alias("ENDOSO"),                                                                                    # campo 16 ENDOSO
    df_cte_producto["FECHA_ENDOSO"].alias("FECHA_ENDOSO"),                                                                        # campo 17 FECHA_ENDOSO
    df_cte_producto["MESPRO"].alias("MESPRO"),                                                                                    # campo 18 MESPRO
    df_cte_poliza_matriz["UNIDAD_NEGOCIO"].alias("UNIDAD_NEGOCIO"),                                                               # campo 19 UNIDAD_NEGOCIO
    df_cte_producto["FECHA_INICIO_POLIZA_MATRIZ"].alias("FECHA_INICIO_POLIZA_MATRIZ"),                                            # campo 20 FECHA_INICIO_POLIZA_MATRIZ
    df_cte_producto["FECHA_FIN_POLIZA_MATRIZ"].alias("FECHA_FIN_POLIZA_MATRIZ"),                                                  # campo 21 FECHA_FIN_POLIZA_MATRIZ
    df_cte_unidad_riesgo["FECHA_INICIO_COBERTURA"].alias("FECHA_INICIO_COBERTURA"),                                               # campo 22 FECHA_INICIO_COBERTURA
    df_cte_unidad_riesgo["FECHA_FIN_COBERTURA"].alias("FECHA_FIN_COBERTURA"),                                                     # campo 23 FECHA_FIN_COBERTURA
    lit(None).alias("ID_TOMADOR_POLIZA_MATRIZ"),                                                                                  # campo 24 ID_TOMADOR_POLIZA_MATRIZ    
    lit(None).alias("TOMADOR_POLIZA_MATRIZ"),                                                                                     # campo 25 TOMADOR_POLIZA_MATRIZ    
    df_cte_id_tomador_poliza["ID_TOMADOR_POLIZA"].alias("ID_TOMADOR_POLIZA"),                                                     # campo 26 ID_TOMADOR_POLIZA
    df_cte_tomador_poliza["TOMADOR_POLIZA"].alias("TOMADOR_POLIZA"),                                                              # campo 27 TOMADOR_POLIZA
    lit(None).alias("TIPO_COASEGURO"),                                                                                            # campo 28 TIPO_COASEGURO        
    lit(0).alias("PORCPART_ALFA"),                                                                                                # campo 29 PORCPART_ALFA 
    df_cte_frecuencia_pago["FRECUENCIA_PAGO"].alias("FRECUENCIA_PAGO"),                                                           # campo 30  FRECUENCIA_PAGO    
    df_cte_prima_emitida["PRIMA_EMITIDA"].alias("PRIMA_EMITIDA"),                                                                 # campo 31 PRIMA_EMITIDA    
    (df_cte_prima_emitida["PRIMA_EMITIDA"] - (df_cte_prima_emitida["PRIMA_EMITIDA"] * (100/100))).alias("PRIMACED_COASEGURO"),    # campo 32 PRIMACED_COASEGURO
    (df_cte_prima_emitida["PRIMA_EMITIDA"] * (100/100)).alias("PRIMAALFA"),                                                       # campo 33 PRIMAALFA    
    df_cte_porcentaje_comision_intermediacion["PORC_COM_INTERMEDIACION"].alias("PORC_COM_INTERMEDIACION"),                        # campo 34 PORC_COM_INTERMEDIACION
    df_cte_comision_intermediacion["COMISION_INTERMEDIACION"].alias("COMISION_INTERMEDIACION"),                                   # campo 35 COMISION_INTERMEDIACION
    df_cte_iva_comision_intermediacion["IVA_COMISION_INTERMEDIACION"].alias("IVA_COMISION_INTERMEDIACION"),                       # campo 36 IVA_COMISION_INTERMEDIACION
    (( df_cte_comision_recaudo["COMISION_RECAUDO"] * 100) / df_cte_prima_emitida["PRIMA_EMITIDA"]).alias("PORCCOMRECAUDO"),       # campo 37 PORCCOMRECAUDO
    df_cte_comision_recaudo["COMISION_RECAUDO"].alias("COMISION_RECAUDO"),                                                        # campo 38 COMISION_RECAUDO
    df_cte_iva_comision_intermediacion["IVA_COMISION_INTERMEDIACION"].alias("IVA_COMISION_INTERMEDIACION"),                       # campo 39 IVA_COMISION_RECAUDO
    df_cte_clase_reasegurador["CODREASEG"].alias("CODREASEG"),                                                                    # campo 40 CODREASEG
    df_cte_nomreaseg["NOMREASEG"].alias("NOMREASEG"),                                                                             # campo 41 NOMREASEG
    df_cte_clase_reasegurador["CLASE_REASEGURADOR"].alias("CLASE_REASEGURADOR"),                                                  # campo 42 CLASE_REASEGURADOR
    df_cte_primaced_comisionreaseg["PRIMACEDAUT"].alias("PRIMACEDAUT"),                                                           # campo 43 PRIMACEDAUT 
    df_cte_primaced_comisionreaseg_intext["PRIMACEDAUT_INT"].alias("PRIMACEDAUT_INT"),                                            # campo 44 PRIMACEDAUT_INT
    df_cte_primaced_comisionreaseg_intext["PRIMACEDAUT_EXT"].alias("PRIMACEDAUT_EXT"),                                            # campo 45 PRIMACEDAUT_EXT
    df_cte_primaced_comisionreaseg["PRIMACEDFAC"].alias("PRIMACEDFAC"),                                                           # campo 46 PRIMACEDFAC
    df_cte_primaced_comisionreaseg_intext["PRIMACEDFAC_INT"].alias("PRIMACEDFAC_INT"),                                            # campo 47 PRIMACEDFAC_INT
    df_cte_primaced_comisionreaseg_intext["PRIMACEDFAC_EXT"].alias("PRIMACEDFAC_EXT"),                                            # campo 48 PRIMACEDFAC_EXT
    df_cte_primaced_comisionreaseg["COMISIONREASEGAUT"].alias("COMISIONREASEGAUT"),                                               # campo 49 COMISIONREASEGAUT
    df_cte_primaced_comisionreaseg_intext["COMISIONREASEGAUT_INT"].alias("COMISIONREASEGAUT_INT"),                                # campo 50 COMISIONREASEGAUT_INT
    df_cte_primaced_comisionreaseg_intext["COMISIONREASEGAUT_EXT"].alias("COMISIONREASEGAUT_EXT"),                                # campo 51 COMISIONREASEGAUT_EXT
    df_cte_primaced_comisionreaseg["COMISIONREASEGFAC"].alias("COMISIONREASEGFAC"),                                               # campo 52 COMISIONREASEGFAC
    df_cte_primaced_comisionreaseg_intext["COMISIONREASEGFAC_INT"].alias("COMISIONREASEGFAC_INT"),                                # campo 53 COMISIONREASEGFAC_INT
    df_cte_primaced_comisionreaseg_intext["COMISIONREASEGFAC_EXT"].alias("COMISIONREASEGFAC_EXT"),                                # campo 54 COMISIONREASEGFAC_EXT
    lit(None).alias("GASTOS_EXP_PORCENTAJE"),                                                                                     # campo 55 GASTOS_EXP_PORCENTAJE    
    lit(None).alias("GASTOS_EXP"),                                                                                                # campo 56 GASTOS_EXP    
    lit(None).alias("UNIDAD_DE_CUENTA"),                                                                                          # campo 57 UNIDAD_DE_CUENTA    
            
    df_campos_final_pcr_join["time_stamp_partition"].alias("PERIODO")                                                              #campo gold PERIODO
    
)
df_campos_final_pcr_select.show(1)
# Write to the original path as well for compatibility
df_campos_final_pcr_select.write.mode("overwrite").parquet("s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL/PRUEBA_MODULAR/")
df_cte_unidad_riesgo.show(1)
#df_cte_poliza_matriz.show(1)
#df_cte_producto.show(1)
#df_cte_compania.show(1)
df_cte_prima_emitida.count()
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, trim, regexp_replace, date_format
from pyspark.sql.types import StringType, DoubleType, LongType, DecimalType, DateType
from pyspark.storagelevel import StorageLevel
spark = SparkSession.builder \
    .appName("IngestaSilverSecuencial") \
    .config("spark.executor.memory", "24g") \
    .config("spark.executor.memoryOverhead", "4g") \
    .config("spark.executor.cores", "5") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "500") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY") \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
    .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY") \
    .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY") \
    .getOrCreate()

s3_schema_path = "s3://alfa-datalake-dev-assets/support_phase_files/niif/Silver_pcr-tradicional.json"
s3_base_path = "s3://alfa-datalake-dev-bronce/ACSELE/ACSELE_ALFA_FULL/"
silver_base_path = "s3://alfa-datalake-dev-silver/ACSELE/ACSELE_ALFA_FULL/"
s3_reporte_path = "s3://alfa-datalake-dev-bronce/ACSELE/resumen_ingesta.json"
# testeo para acsele_alfa_full_thirdpartymovementpolicy
ruta_entrada = "s3://alfa-datalake-dev-bronce/ACSELE/ACSELE_ALFA_FULL/THIRDPARTYMOVEMENTPOLICY/"
ruta_salida =  "s3://alfa-datalake-dev-silver/ACSELE/ACSELE_ALFA_FULL/THIRDPARTYMOVEMENTPOLICY_PAR/"
#read
df_entrada = spark.read.parquet(ruta_entrada)
spark.sql("USE `datalake-dev-silver-catalog-db`")
# Define el número deseado de archivos/buckets
num_archivos_salida = 200

# Añade .repartition() ANTES del .write
(df_entrada.repartition(num_archivos_salida, "OPERATIONPK")
    .write
    .mode("overwrite")
    .bucketBy(num_archivos_salida, "OPERATIONPK")
    .sortBy("OPERATIONPK")
    .option("path", ruta_salida)
    .saveAsTable("acsele_alfa_full_thirdpartymovementpolicy_par")
)
.option("path", ruta_salida) \
### testeo para particionar información 

ruta_entrada = "s3://alfa-datalake-dev-silver/ACSELE/ACSELE_ALFA_FULL/CONTEXTOPERATION/"
ruta_salida =  "s3://alfa-datalake-dev-silver/ACSELE/ACSELE_ALFA_FULL/CONTEXTOPERATION_PAR/"

#read
df_entrada = spark.read.parquet(ruta_entrada)

df_with_partition = df_entrada.withColumn(f"time_stamp_partition", date_format(col("time_stamp"), "yyyy-MM"))
df_with_partition.write \
           .mode("overwrite") \
           .partitionBy("time_stamp_partition") \
           .parquet(ruta_salida)
import re
def get_spark_type_from_oracle(oracle_type: str):
    oracle_type = oracle_type.strip().upper()
    if oracle_type.startswith("VARCHAR2") or oracle_type.startswith("CHAR"):
        return StringType()
    if oracle_type.startswith("NUMBER"):
        match = re.match(r"NUMBER\((\d+)(?:,\s*(\d+))?\)", oracle_type)
        if match:
            precision = int(match.group(1))
            scale = int(match.group(2)) if match.group(2) else 0
            return LongType() if scale == 0 and precision <= 19 else DecimalType(precision, scale)
        return DoubleType()
    if oracle_type == "DATE":
        return DateType()
    return StringType()
json_schema = {
"PREPOLICY": {
      "schema": {
        "PK":	"NUMBER(38)",
        "STATIC":	"NUMBER(38)",
        "STATUS":	"NUMBER(38)",
        "NUMEROPOLIZAINPUT":	"VARCHAR2(150)",
        "NUMEROPOLIZAVALUE":	"VARCHAR2(150)",
        "NUMEROPOLIZAISMANUAL":	"VARCHAR2(2)",
        "FECHAEMISIONINPUT":	"VARCHAR2(10)",
        "FECHAEMISIONVALUE":	"VARCHAR2(10)",
        "FECHAEMISIONISMANUAL":	"VARCHAR2(2)",
        "MONEDAINPUT":	"VARCHAR2(170)",
        "MONEDAVALUE":	"VARCHAR2(170)",
        "MONEDAISMANUAL":	"VARCHAR2(2)",
        "TIPOPRODUCCIONINPUT":	"VARCHAR2(170)",
        "TIPOPRODUCCIONVALUE":	"VARCHAR2(170)",
        "TIPOPRODUCCIONISMANUAL":	"VARCHAR2(2)",
        "OFICINAINPUT":	"VARCHAR2(170)",
        "OFICINAVALUE":	"VARCHAR2(170)",
        "OFICINAISMANUAL":	"VARCHAR2(2)",
        "INDAUTRENOVACIONISMANUAL":	"VARCHAR2(2)",
        "TIPOMONEDAISMANUAL":	"VARCHAR2(2)",
        "TIPOVIGENCIAINPUT":	"VARCHAR2(170)",
        "TIPOVIGENCIAVALUE":	"VARCHAR2(170)",
        "TIPOVIGENCIAISMANUAL":	"VARCHAR2(2)",
        "VIGENCIAINPUT":	"VARCHAR2(170)",
        "VIGENCIAVALUE":	"VARCHAR2(170)",
        "VIGENCIAISMANUAL":	"VARCHAR2(2)",
        "FECHAPROXFACTURACIONINPUT":	"VARCHAR2(10)",
        "FECHAPROXFACTURACIONVALUE":	"VARCHAR2(10)",
        "FECHAPROXFACTURACIONISMANUAL":	"VARCHAR2(2)",
        "CANALVENTAINPUT":	"VARCHAR2(150)",
        "CANALVENTAVALUE":	"VARCHAR2(150)",
        "CANALVENTAISMANUAL":	"VARCHAR2(2)",
        "FECHAPROXIMAGENPRIMAINPUT":	"VARCHAR2(10)",
        "FECHAPROXIMAGENPRIMAVALUE":	"VARCHAR2(10)",
        "FECHAPROXIMAGENPRIMAISMANUAL":	"VARCHAR2(2)",
        "FRECUENCIAPAGOINPUT":	"VARCHAR2(170)",
        "FRECUENCIAPAGOVALUE":	"VARCHAR2(170)",
        "FRECUENCIAPAGOISMANUAL":	"VARCHAR2(2)",
        "FECHAEVENTOANTERIORINPUT":	"VARCHAR2(150)",
        "FECHAEVENTOANTERIORVALUE":	"VARCHAR2(150)",
        "FECHAEVENTOANTERIORISMANUAL":	"VARCHAR2(2)",
        "TIPOPOLIZAINPUT":	"VARCHAR2(150)",
        "TIPOPOLIZAVALUE":	"VARCHAR2(150)",
        "TIPOPOLIZAISMANUAL":	"VARCHAR2(2)",
        "NUMEROPOLIZAASISMANUAL":	"VARCHAR2(2)",
        "FECHASUSPCANCELISMANUAL":	"VARCHAR2(2)",
        "FLAGSUSPCANCELISMANUAL":	"VARCHAR2(2)",
        "CODSUPERINTISMANUAL":	"VARCHAR2(2)",
        "NOMBREARCHIVOTRAMAISMANUAL":	"VARCHAR2(2)",
        "NUMPLANINTERISMANUAL":	"VARCHAR2(2)",
        "FECINIPOLMATRIZISMANUAL":	"VARCHAR2(2)",
        "FECFINPOLMATRIZISMANUAL":	"VARCHAR2(2)",
        "NOMCOMASEGURADOPOLISMANUAL":	"VARCHAR2(2)",
        "NOMCOMBENEFICIARIOPOLISMANUAL":	"VARCHAR2(2)",
        "JOBSUPDAYSISMANUAL":	"VARCHAR2(2)",
        "JOBSUPDATEISMANUAL":	"VARCHAR2(2)",
        "JOBSUPINDISMANUAL":	"VARCHAR2(2)",
        "PRIMATRAMAISMANUAL":	"VARCHAR2(2)",
        "NUMEROCOTIZACIONINPUT":	"VARCHAR2(150)",
        "NUMEROCOTIZACIONVALUE":	"VARCHAR2(150)",
        "NUMEROPROPUESTAINPUT":	"VARCHAR2(150)",
        "NUMEROPROPUESTAVALUE":	"VARCHAR2(150)",
        "TIPOMONEDAINPUT":	"VARCHAR2(170)",
        "TIPOMONEDAVALUE":	"VARCHAR2(170)",
        "POLIZAMAESTRAINPUT":	"VARCHAR2(150)",
        "POLIZAMAESTRAVALUE":	"VARCHAR2(150)"
      },
      "rename": {},
      "homologaciones": {}
    }
}


# Asegúrate de tener todas las importaciones necesarias al inicio de tu script
from pyspark.sql.functions import col, when, trim, regexp_replace, to_date, lpad
import logging

def process_single_table(table_name, config, spark, s3_base_path, silver_base_path, resumen):
    """
    Función optimizada para procesar una tabla.
    - Consolida todas las transformaciones de columna en un único .select() para un plan de ejecución eficiente.
    - Maneja robustamente la lectura y validación de esquemas.
    - Aplica repartición para balancear la carga de escritura.
    """
    schema_def_raw = config.get("schema", {})
    if not schema_def_raw or not isinstance(schema_def_raw, dict):
        logging.warning(f"[{table_name}] Esquema inválido o no encontrado en la configuración.")
        return

    try:
        # --- 1. LECTURA Y VALIDACIÓN INICIAL ---
        # Pre-procesamos los nombres de columnas a minúsculas para consistencia
        schema_def = {k.lower(): v for k, v in schema_def_raw.items()}
        columnas_requeridas = list(schema_def.keys())

        s3_path = f"{s3_base_path}{table_name}"
        df = spark.read.parquet(s3_path)
        
        # Validar columnas faltantes antes de continuar
        columnas_existentes = {c.lower() for c in df.columns}
        faltantes = [c for c in columnas_requeridas if c not in columnas_existentes]
        
        # Llenar el resumen inicial
        resumen[table_name] = {
            "columnas_homologadas": list(config.get("homologaciones", {}).keys()),
            "columnas_renombradas": config.get("rename", {}),
            "columnas_faltantes": faltantes,
            "filas_entrada": 0, # Se actualizará después si es necesario
            "filas_salida": 0
        }

        if faltantes:
            logging.warning(f"[{table_name}] Columnas requeridas faltantes en el origen: {faltantes}. Saltando procesamiento.")
            return

        # Seleccionamos solo las columnas requeridas para optimizar desde el inicio
        df = df.select(columnas_requeridas)

        # --- 2. CONSTRUCCIÓN DE EXPRESIONES DE TRANSFORMACIÓN (EL NÚCLEO DE LA OPTIMIZACIÓN) ---
        select_expressions = []
        
        # Pre-procesamos las configuraciones para un acceso eficiente
        homologaciones = {k.lower(): v for k, v in config.get("homologaciones", {}).items()}
        fields_to_clean = {f.lower() for f in config.get("fields_to_clean", [])}
        rename_map = {k.lower(): v.lower() for k, v in config.get("rename", {}).items()}

        for column_name in df.columns:
            # Empezamos con la columna base
            current_expr = col(column_name)

            # A. Cast de tipos según el esquema
            col_type = schema_def.get(column_name)
            if col_type:
                if "DATE" in col_type.upper():
                    if table_name.upper() == "RPND13Y" and column_name in ["fvigdesde", "fvighasta", "fvigenddk", "fvigendhk", "entrydte"]:
                        # Lógica especial y robusta para fechas en formato DDMMYYYY
                        current_expr = to_date(lpad(current_expr.cast("string"), 8, "0"), "ddMMyyyy")
                    else:
                        current_expr = to_date(current_expr)
                else:
                    spark_type = get_spark_type_from_oracle(col_type) # Asume que esta función de utilidad existe
                    current_expr = current_expr.cast(spark_type)

            # B. Homologación
            if column_name in homologaciones:
                mapping = homologaciones[column_name]
                homolog_expr = None
                for old_val, new_val in mapping.items():
                    homolog_expr = when(current_expr == old_val, new_val) if homolog_expr is None else homolog_expr.when(current_expr == old_val, new_val)
                if homolog_expr:
                    current_expr = homolog_expr.otherwise(current_expr)

            # C. Limpieza de campos
            if column_name in fields_to_clean:
                current_expr = trim(regexp_replace(current_expr, " {2,}", " "))
            
            # D. Renombramiento (se aplica como alias a la expresión final)
            final_col_name = rename_map.get(column_name, column_name)
            
            select_expressions.append(current_expr.alias(final_col_name))

        # --- 3. APLICACIÓN DE TODAS LAS TRANSFORMACIONES EN UN SOLO PASO ---
        df_transformado = df.select(select_expressions)

        # --- 4. REPARTICIÓN Y ESCRITURA ---
        # Fijamos un número de particiones robusto para balancear la carga de escritura.
        df_final = df_transformado.repartition(500)

        # Opcional: Si necesitas el conteo, este es el momento de hacerlo.
        # count = df_final.count()
        # resumen[table_name]["filas_entrada"] = count
        # resumen[table_name]["filas_salida"] = count
        
        silver_path = f"{silver_base_path}{table_name}_test/"
        df_final.write.mode("overwrite").option("maxRecordsPerFile", 10_000_000).parquet(silver_path)
        logging.info(f"[{table_name}] Escritura completada en Silver: {silver_path}")

    except Exception as e:
        # Captura de errores más detallada para depuración
        logging.error(f"[{table_name}] Error en procesamiento. Causa: {str(e)}", exc_info=True)
        # Relanzar la excepción para que el job de Glue falle explícitamente si es necesario
        raise e
resumen = {}
for table_name, config in json_schema.items():    
    process_single_table(table_name, config, spark, s3_base_path, silver_base_path, resumen)
for table_name, config in json_schema.items():
    s3_path = f"{s3_base_path}{table_name}"
    df = spark.read.parquet(s3_path).select([col(c) for c in columnas_requeridas])    
    schema_def = config.get("schema", {})
    
    for col_name, col_type in schema_def.items():
        col_lower = col_name.lower()
        print(col_name, col_type)
        spark_type = get_spark_type_from_oracle(col_type)
        df = df.withColumn(col_lower, col(col_lower).cast(spark_type))
df = df.repartition(500)
silver_path = f"{silver_base_path}{table_name}_test/"
silver_path
df.write.mode("overwrite").option("maxRecordsPerFile", 10_000_000).parquet(silver_path)
table_name = "PREPOLICY"
columnas_requeridas = ["PK","STATIC","STATUS","NUMEROPOLIZAINPUT","NUMEROPOLIZAVALUE","NUMEROPOLIZAISMANUAL","FECHAEMISIONINPUT","FECHAEMISIONVALUE","FECHAEMISIONISMANUAL","MONEDAINPUT","MONEDAVALUE","MONEDAISMANUAL","TIPOPRODUCCIONINPUT","TIPOPRODUCCIONVALUE","TIPOPRODUCCIONISMANUAL","OFICINAINPUT","OFICINAVALUE","OFICINAISMANUAL","INDAUTRENOVACIONISMANUAL","TIPOMONEDAISMANUAL","TIPOVIGENCIAINPUT","TIPOVIGENCIAVALUE","TIPOVIGENCIAISMANUAL","VIGENCIAINPUT","VIGENCIAVALUE","VIGENCIAISMANUAL","FECHAPROXFACTURACIONINPUT","FECHAPROXFACTURACIONVALUE","FECHAPROXFACTURACIONISMANUAL","CANALVENTAINPUT","CANALVENTAVALUE","CANALVENTAISMANUAL","FECHAPROXIMAGENPRIMAINPUT","FECHAPROXIMAGENPRIMAVALUE","FECHAPROXIMAGENPRIMAISMANUAL","FRECUENCIAPAGOINPUT","FRECUENCIAPAGOVALUE","FRECUENCIAPAGOISMANUAL","FECHAEVENTOANTERIORINPUT","FECHAEVENTOANTERIORVALUE","FECHAEVENTOANTERIORISMANUAL","TIPOPOLIZAINPUT","TIPOPOLIZAVALUE","TIPOPOLIZAISMANUAL","NUMEROPOLIZAASISMANUAL","FECHASUSPCANCELISMANUAL","FLAGSUSPCANCELISMANUAL","CODSUPERINTISMANUAL","NOMBREARCHIVOTRAMAISMANUAL","NUMPLANINTERISMANUAL","FECINIPOLMATRIZISMANUAL","FECFINPOLMATRIZISMANUAL","NOMCOMASEGURADOPOLISMANUAL","NOMCOMBENEFICIARIOPOLISMANUAL","JOBSUPDAYSISMANUAL","JOBSUPDATEISMANUAL","JOBSUPINDISMANUAL","PRIMATRAMAISMANUAL","NUMEROCOTIZACIONINPUT","NUMEROCOTIZACIONVALUE","NUMEROPROPUESTAINPUT","NUMEROPROPUESTAVALUE","TIPOMONEDAINPUT","TIPOMONEDAVALUE","POLIZAMAESTRAINPUT","POLIZAMAESTRAVALUE"]
s3_path
s3_path = f"{s3_base_path}{table_name}"
df = spark.read.parquet(s3_path).select([col(c) for c in columnas_requeridas])
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


spark.sql("USE `datalake-dev-silver-catalog-db`")


sql_producto = """
SELECT DISTINCT
    DCO.operationpk,
    CO.id,
    CO.TIME_STAMP AS FECHA_ENDOSO,
    FPDCO.CCY_ID AS CURRENCY_ID,
    PP.NUMEROPOLIZAINPUT AS POLIZA,
    AP.PRODUCTID AS PRODUCTO,
    PP.OFICINAINPUT AS SUCURSAL,
    PP.TIPOPOLIZAINPUT AS POLICYTYPE_NAME,
    PP.TIPOPOLIZAVALUE AS POLICYTYPE_CODE,
    PP.FRECUENCIAPAGOINPUT AS FRECUENCIA_PAGO,
    DCO.INITIALDATE AS FECHA_INICIO_POLIZA_MATRIZ,
    DCO.FINISHDATE AS FECHA_FIN_POLIZA_MATRIZ
FROM acsele_acsele_alfa_POLICYDCO DCO
JOIN acsele_acsele_alfa_CONTEXTOPERATION CO ON CO.ID = DCO.OPERATIONPK
JOIN acsele_acsele_alfa_PREPOLICY PP ON PP.PK = DCO.DCOID
JOIN acsele_acsele_alfa_FINANCIALPLANDCO FPDCO ON FPDCO.AGREGATEDPARENTID = DCO.AGREGATEDOBJECTID AND FPDCO.OPERATIONPK = DCO.OPERATIONPK
JOIN acsele_acsele_alfa_AGREGATEDPOLICY AP ON AP.AGREGATEDPOLICYID = DCO.AGREGATEDOBJECTID
WHERE CO.id >= 986530136 and CO.status = 2

"""
df_producto = spark.sql(sql_producto)
#df_producto.createOrReplaceTempView("TMP_PRODUCTO")
df_producto.show(5)
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


spark.sql("USE `datalake-dev-silver-catalog-db`")


sql_producto = """
SELECT DISTINCT
    CDCO.OPERATIONPK AS DCOID,
    PP.COMPANIAINPUT AS COMPANIA,
    REPLACE(PP.COMPANIAVALUE, '.0', '') AS CODIGO_COMPANIA,
    PP.DESCRIPCIONPRODUCTOINPUT AS NOMBRE_PRODUCTO,
    PP.CODIGOPRODUCTOINPUT AS CODIGO_PRODUCTO,
    PPRE.NUMEROPOLIZAINPUT AS NUMERO_POLIZA,
    PPRE.POLIZAMAESTRAINPUT AS ACUERDO,
    REPLACE(CB.COBCODIGOCOBERTURAINPUT, '.0', '') AS COBERTURA,
    CB.COBDESCRIPCIONCOBERTINPUT AS DESC_COBERTURA,
    CASE WHEN CB.COBACUMULACIONINPUT IN ('A', 'P') THEN 'SI' ELSE 'NO' END AS ACUMULA,
    REPLACE(CB.COBRAMOREASEGUROINPUT, '.0', '') AS RAMO_REASEGURO,
    REPLACE(CB.COBRAMOCONTABLEINPUT, '.0', '') AS RAMO,
    PP.RAMOINPUT AS DESC_RAMO,
    RG.DESCRIPTION AS CODIGO_RAMO_REASEGURO,
    RK.REK_CODE AS GRUPO_REASEGURO,
    RK.REK_DESCRIPTION AS NOMBRE_GRUPO_REASEGURO,
    COALESCE(CB.COBPRIMAINPUT, '0') AS PRIMA,
    CB.COBPRIMAREASEGUROINPUT AS PRIMA_REASEGURO
FROM acsele_acsele_alfa_COVERAGEDCO CDCO
JOIN acsele_acsele_alfa_COBERTURA CB ON CB.PK = CDCO.DCOID
LEFT JOIN acsele_acsele_alfa_POLICYDCO PDCO ON PDCO.OPERATIONPK = CDCO.OPERATIONPK
JOIN acsele_acsele_alfa_CONTEXTOPERATION CO ON CO.ID = PDCO.OPERATIONPK
LEFT JOIN acsele_acsele_alfa_AGREGATEDPOLICY AP ON AP.AGREGATEDPOLICYID = PDCO.AGREGATEDOBJECTID
LEFT JOIN acsele_acsele_alfa_PRODUCT PD ON PD.PRODUCTID = AP.PRODUCTID
LEFT JOIN acsele_acsele_alfa_PREPRODUCT PP ON PP.PK = PD.PRO_DCOID
LEFT JOIN acsele_acsele_alfa_PREPOLICY PPRE ON PPRE.PK = PDCO.DCOID
LEFT JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.CODE = REPLACE(CB.COBRAMOREASEGUROINPUT, '.0', '')
LEFT JOIN acsele_acsele_alfa_STRI_REINSURANCEKIND RK ON RK.REKIND_ID = RG.REKIND_ID
WHERE CO.id >= 986530136 and CO.status = 2
"""
df_producto = spark.sql(sql_producto)
#df_producto.createOrReplaceTempView("TMP_PRODUCTO")
df_producto.show(5)
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, explode, split, col, trim

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


spark.sql("USE `datalake-dev-silver-catalog-db`")

syp_df = spark.sql("""
    SELECT SYP_VALUE
    FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
    WHERE SYP_PROPERTY = 'RRC.issuedPremium.earthquakeDocType'
""")

syp_exploded = syp_df.select(explode(split(col("SYP_VALUE"), ",")).alias("DESCRIPTION"))
syp_trimmed = syp_exploded.select(trim(col("DESCRIPTION")).alias("DESCRIPTION"))
syp_trimmed.createOrReplaceTempView("ISSUED_PREMIUM_DOCS")

# -----------------------------
# Lógica del campo PRIMA_EMITIDA
# -----------------------------
sql_prima_emitida = """
SELECT
  OI.OPERATIONPK AS DCOID,
  SUM(CASE WHEN DT.DTY_ISDEBIT = 1 THEN OI.AMOUNT ELSE OI.AMOUNT * (-1) END) AS PRIMA_EMITIDA
FROM acsele_acsele_alfa_OPENITEM OI
JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
WHERE OI.OPM_SUBSTATUS <> 'notApplied'
  AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM ISSUED_PREMIUM_DOCS)
GROUP BY OI.OPERATIONPK
"""

df_prima_emitida = spark.sql(sql_prima_emitida)
#df_prima_emitida.createOrReplaceTempView("TMP_PRIMA_EMITIDA")
df_prima_emitida.show(5)
print("✅ Campo PRIMA_EMITIDA generado correctamente en vista temporal: TMP_PRIMA_EMITIDA")

from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, explode, split, col, trim

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


spark.sql("USE `datalake-dev-silver-catalog-db`")

syp_df = spark.sql("""
    SELECT SYP_VALUE
    FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
    WHERE SYP_PROPERTY = 'RRC.commissionCollection.earthquakeDocType'
""")
syp_exploded = syp_df.select(explode(split(col("SYP_VALUE"), ",")).alias("DESCRIPTION"))
syp_trimmed = syp_exploded.select(trim(col("DESCRIPTION")).alias("DESCRIPTION"))
syp_trimmed.createOrReplaceTempView("COMISION_RECAUDO_DOCS")

# -----------------------------
# Lógica del campo COMISION_RECAUDO
# -----------------------------
sql_comision_recaudo = """
SELECT
  OI.OPERATIONPK AS DCOID,
  SUM(CASE WHEN DT.DTY_ISDEBIT = 0 THEN OI.AMOUNT ELSE OI.AMOUNT * (-1) END) AS COMISION_RECAUDO
FROM acsele_acsele_alfa_OPENITEM OI
JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
WHERE OI.OPM_SUBSTATUS <> 'notApplied'
  AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM COMISION_RECAUDO_DOCS)
GROUP BY OI.OPERATIONPK
"""

df_comision_recaudo = spark.sql(sql_comision_recaudo)
#df_comision_recaudo.createOrReplaceTempView("TMP_COMISION_RECAUDO")
df_comision_recaudo.show(5)
print("✅ Campo COMISION_RECAUDO generado correctamente en vista temporal: TMP_COMISION_RECAUDO")

from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

#PPD.OPERATIONPK AS DCOID,

spark.sql("USE `datalake-dev-silver-catalog-db`")

sql_porc_com_intermediacion = """
SELECT
  
  ppd.operationpk,
  pp.agregatedobjectid,  
  PI.PORCENTAJECOMISIONINPUT AS PORC_COM_INTERMEDIACION
FROM acsele_acsele_alfa_STPO_POLICYPARTICIPATION PP
INNER JOIN acsele_acsele_alfa_STPO_POLICYPARTICIPATIONDCO PPD ON PP.AGREGATEDOBJECTID = PPD.AGREGATEDOBJECTID
INNER JOIN acsele_acsele_alfa_PARTICIPACIONINTERMEDIARIOS PI ON PI.PK = PPD.DCOID
WHERE pp.agregatedobjectid >= 474064566
and ppd.operationpk >= 986530136
"""

df_porc_com_intermediacion = spark.sql(sql_porc_com_intermediacion)
#df_porc_com_intermediacion.createOrReplaceTempView("TMP_PORC_COM_INTERMEDIACION")
df_porc_com_intermediacion.show(5)
print("✅ Campo PORC_COM_INTERMEDIACION generado correctamente en vista temporal: TMP_PORC_COM_INTERMEDIACION")

from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


spark.sql("USE `datalake-dev-silver-catalog-db`")


#--DCO.OPERATIONPK AS DCOID,

sql_unidad_negocio = """
SELECT

  DISTINCT (RAP.rsa_policyid),
  RAP.rsa_ast_id,
  AV.ast_id,
  AV.dcoid,
  PREA.UNIDADNEGOCIOINPUT AS UNIDAD_NEGOCIO
FROM acsele_acsele_alfa_POLICYDCO DCO  
INNER JOIN acsele_acsele_alfa_STPS_RELATIONAGREEMENTPOLICY RAP ON RAP.RSA_POLICYID = DCO.AGREGATEDOBJECTID
INNER JOIN acsele_acsele_alfa_STAG_AGREEMENTVERSION AV ON AV.AST_ID = RAP.RSA_AST_ID 
INNER JOIN acsele_acsele_alfa_PREAGREEMENT PREA ON PREA.PK = AV.DCOID
WHERE RAP.rsa_policyid >= 183066469 AND AV.ast_id >= 73001
"""

df_unidad_negocio = spark.sql(sql_unidad_negocio)
#df_unidad_negocio.createOrReplaceTempView("TMP_UNIDAD_NEGOCIO")
df_unidad_negocio.show(5)
print("✅ Campo UNIDAD_NEGOCIO generado correctamente en vista temporal: TMP_UNIDAD_NEGOCIO")
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


spark.sql("USE `datalake-dev-silver-catalog-db`")

sql_desc_producto = """
SELECT
  DCO.OPERATIONPK AS DCOID,
  CO.TIME_STAMP AS ENDORSEMENT_DATE,
  FPDCO.CCY_ID AS CURRENCY_ID, 
  PP.NUMEROPOLIZAINPUT AS POLICY_NUMBER, 
  AP.PRODUCTID, 
  PR.DESCRIPCIONPRODUCTOINPUT AS DESC_PRODUCTO,
  PR.RAMOINPUT,
  PR.RAMOVALUE,
  PP.OFICINAINPUT AS OFFICE,
  PP.TIPOPOLIZAINPUT AS POLICYTYPE_NAME, 
  PP.TIPOPOLIZAVALUE AS POLICYTYPE_CODE,
  PP.FRECUENCIAPAGOINPUT AS PAYMENTFREQUENCY_NAME,
  DCO.INITIALDATE AS POLICY_BEGINDATE,
  DCO.FINISHDATE AS POLICY_ENDDATE
FROM acsele_acsele_alfa_POLICYDCO DCO 
INNER JOIN acsele_acsele_alfa_CONTEXTOPERATION CO ON CO.ID = DCO.OPERATIONPK
INNER JOIN acsele_acsele_alfa_PREPOLICY PP ON PP.PK = DCO.DCOID
INNER JOIN acsele_acsele_alfa_FINANCIALPLANDCO FPDCO ON FPDCO.AGREGATEDPARENTID = DCO.AGREGATEDOBJECTID AND FPDCO.OPERATIONPK = DCO.OPERATIONPK
INNER JOIN acsele_acsele_alfa_AGREGATEDPOLICY AP ON AP.AGREGATEDPOLICYID = DCO.AGREGATEDOBJECTID  
INNER JOIN acsele_acsele_alfa_PRODUCT PT ON PT.PRODUCTID = AP.PRODUCTID
INNER JOIN acsele_acsele_alfa_PREPRODUCT PR ON PT.PRO_DCOID = PR.PK
WHERE CO.id >= 986530136 and CO.status = 2
"""

df_desc_producto = spark.sql(sql_desc_producto)
#df_desc_producto.createOrReplaceTempView("TMP_DESC_PRODUCTO")
df_desc_producto.show(5)
print("✅ Campo DESC_PRODUCTO generado correctamente en vista temporal: TMP_DESC_PRODUCTO")

from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


spark.sql("USE `datalake-dev-silver-catalog-db`")

sql_riskunit = """
SELECT
  DCO.OPERATIONPK AS DCOID,
  PRU.pk,
  PRU.NUMUNIDRIESGOINPUT AS RISKUNIT_NUMBER,
  DCO.INITIALDATE AS RISKUNIT_BEGINDATE, 
  DCO.FINISHDATE AS RISKUNIT_ENDDATE
FROM acsele_acsele_alfa_PRERISKUNITS PRU
INNER JOIN acsele_acsele_alfa_RISKUNITDCO DCO ON DCO.DCOID = PRU.PK
WHERE PRU.pk >= 61652035099
"""

df_riskunit = spark.sql(sql_riskunit)
#df_riskunit.createOrReplaceTempView("TMP_RISKUNIT")
df_riskunit.show(5)
print("✅ Campo RISKUNIT generado correctamente en vista temporal: TMP_RISKUNIT")

from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, explode, split, col, trim

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


spark.sql("USE `datalake-dev-silver-catalog-db`")

bondholder_df = spark.sql("""
    SELECT SYP_VALUE
    FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
    WHERE SYP_PROPERTY = 'bondholder.rol'
""")
bondholder_exploded = bondholder_df.select(explode(split(col("SYP_VALUE"), ",")).alias("ROL_DESCRIPTION"))
bondholder_trimmed = bondholder_exploded.select(trim(col("ROL_DESCRIPTION")).alias("ROL_DESCRIPTION"))
bondholder_trimmed.createOrReplaceTempView("BONDHOLDER_ROLES")

# -----------------------------
# Lógica del campo TOMADOR_POLIZA
# -----------------------------

#TP.TPT_ID AS THIRDPARTY_ID,
#PPD.OPERATIONPK AS DCOID,

sql_tomador_poliza = """
SELECT

  
  DISTINCT (PP.agregatedobjectid),
  PTP.NOMBREPREDETERMINADOINPUT AS TOMADOR_POLIZA
FROM acsele_acsele_alfa_STPO_POLICYPARTICIPATION PP
INNER JOIN acsele_acsele_alfa_STPO_POLICYPARTICIPATIONDCO PPD ON PP.AGREGATEDOBJECTID = PPD.AGREGATEDOBJECTID 
INNER JOIN acsele_acsele_alfa_STTE_THIRDPARTY TP ON TP.TPT_ID = PP.THIRDPARTYID
INNER JOIN acsele_acsele_alfa_PRETHIRDPARTY PTP ON PTP.PK = TP.IDDCO
INNER JOIN acsele_acsele_alfa_STTS_ROLE RL ON RL.ROL_ID = PP.ROL_ID
WHERE RL.ROL_DESCRIPTION IN (SELECT ROL_DESCRIPTION FROM BONDHOLDER_ROLES) AND PP.agregatedobjectid >= 474064566
"""

df_tomador_poliza = spark.sql(sql_tomador_poliza)
#df_tomador_poliza.createOrReplaceTempView("TMP_TOMADOR_POLIZA")
df_tomador_poliza.show(5)
print("✅ Campo TOMADOR_POLIZA generado correctamente en vista temporal: TMP_TOMADOR_POLIZA")

from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


spark.sql("USE `datalake-dev-silver-catalog-db`")

sql_codreaseg = """
SELECT
  DISTINCT (TPR.THIRDPARTYID) AS DCOID,
  RA.CLASEREASEGUROINPUT AS CLASE_REASEGURO,
  RA.CODIGOREASEGURADORINPUT AS CODREASEG
FROM acsele_acsele_alfa_THIRDPARTYROLE TPR
INNER JOIN acsele_acsele_alfa_REASEGURADOR RA ON RA.PK = TPR.IDDCOROLE
"""

df_codreaseg = spark.sql(sql_codreaseg)
#df_codreaseg.createOrReplaceTempView("TMP_CODREASEG")
df_codreaseg.show(5)
print("✅ Campo CODREASEG generado correctamente en vista temporal: TMP_CODREASEG")
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


spark.sql("USE `datalake-dev-silver-catalog-db`")

sql_nomreaseg = """
SELECT
  DISTINCT (RUD.DCOID) AS DCOID,
  RO.operationid, 
  ROD.THIRDPARTYID AS THIRDPARTY_ID, 
  ROD.THIRDPARTYNAME AS NOMREASEG,
  ROD.PARTICIPATIONID AS PARTICIPATION_ID, 
  SUM(ROD.ROD_PRORATEDPREMIUM) AS DETAIL_PREMIUM
FROM acsele_acsele_alfa_REINSURANCEOPERATION RO
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO ROG ON ROG.OPERATIONID = RO.OPERATIONID
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONDETAIL ROD ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
INNER JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
INNER JOIN acsele_acsele_alfa_RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
WHERE ROC.CONTRACTTYPE = 'MandatoryFacultativeContract' AND ro.operationid >= 943602721 AND roc.reinsurancegroupinfoid >= 674470981
GROUP BY ROD.THIRDPARTYID, ROD.THIRDPARTYNAME, ROD.PARTICIPATIONID, RUD.OPERATIONPK, RUD.DCOID, RO.operationid
"""

df_nomreaseg = spark.sql(sql_nomreaseg)
#df_nomreaseg.createOrReplaceTempView("TMP_NOMREASEG")
df_nomreaseg.show(5)
print("✅ Campo NOMREASEG generado correctamente en vista temporal: TMP_NOMREASEG")

from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


sql_compania = """
SELECT
    RUD.dcoid,
    RG.RIG_COMPANYCODE AS COMPANIA
FROM acsele_acsele_alfa_RI_REINSURANCEGROUP RG
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO ROG ON ROG.GROUPID = RG.REINSURANCEGROUPID
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATION RO ON RO.OPERATIONID = ROG.OPERATIONID
INNER JOIN acsele_acsele_alfa_RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
--WHERE RO.operationid >= 943602721
"""

df_compania = spark.sql(sql_compania)
df_compania.createOrReplaceTempView("TMP_COMPANIA")
df_compania.show(5)
print("✅ Campo COMPANIA generado correctamente en vista temporal: TMP_COMPANIA")


from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

# =============================
# CAMPO 2: POLIZA_MATRIZ
# =============================

# Ejecutar consulta
#DCO.OPERATIONPK AS DCOID,
sql_poliza_matriz = """
SELECT
  PREA.UNIDADNEGOCIOINPUT AS BUSINESSUNIT_NAME,
  AGS.AST_NUMEROACU AS POLIZA_MATRIZ
  FROM acsele_acsele_alfa_POLICYDCO DCO
INNER JOIN acsele_acsele_alfa_STPS_RELATIONAGREEMENTPOLICY RAP ON RAP.RSA_POLICYID = DCO.AGREGATEDOBJECTID
INNER JOIN acsele_acsele_alfa_STAG_AGREEMENTVERSION AV ON AV.AST_ID = RAP.RSA_AST_ID
INNER JOIN acsele_acsele_alfa_PREAGREEMENT PREA ON PREA.PK = AV.DCOID
INNER JOIN acsele_acsele_alfa_STPS_AGREEMENTSTATICDAT AGS ON RAP.RSA_AST_ID = AGS.AST_ID
WHERE DCO.operationpk >= 986530136
"""

df_poliza_matriz = spark.sql(sql_poliza_matriz)
#df_poliza_matriz.createOrReplaceTempView("TMP_POLIZA_MATRIZ")
df_poliza_matriz.show(5)
print("✅ Campo POLIZA_MATRIZ generado correctamente en vista temporal: TMP_POLIZA_MATRIZ")

from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.sql("USE `datalake-dev-silver-catalog-db`")

sql_id_tomador_poliza = """
SELECT
  PAR.PERCENTAGE,
  PRE.DOCUMENTOIDENTIDADINPUT AS ID_TOMADOR_POLIZA,
  TP.TPT_ID AS THIRDPARTY_ID
FROM acsele_acsele_alfa_RI_PARTICIPATION PAR
inner JOIN acsele_acsele_alfa_STTE_THIRDPARTY TP ON TP.TPT_ID = PAR.THIRDPARTYID
inner JOIN acsele_acsele_alfa_PRETHIRDPARTY PRE ON TP.IDDCO = PRE.PK
"""

df_id_tomador_poliza = spark.sql(sql_id_tomador_poliza)
#df_id_tomador_poliza.createOrReplaceTempView("TMP_ID_TOMADOR_POLIZA")
df_id_tomador_poliza.show(5)

print("✅ Campo ID_TOMADOR_POLIZA optimizado generado correctamente en vista temporal: TMP_ID_TOMADOR_POLIZA")

from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, explode, split, col, trim

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


sysprop_df = spark.sql("""
    SELECT SYP_VALUE
    FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
    WHERE SYP_PROPERTY = 'RRC.commisionIntermediary'
""")

exploded_df = sysprop_df.select(explode(split(col("SYP_VALUE"), ",")).alias("DESCRIPTION"))
trimmed_df = exploded_df.select(trim(col("DESCRIPTION")).alias("DESCRIPTION"))
trimmed_df.createOrReplaceTempView("COMM_INTERMEDIARY_DOCS")

# -----------------------------
# Lógica del campo IVA_COMISION_INTERMEDIACION
# -----------------------------
sql_iva_comision_intermediacion = """
SELECT
  OI.OPERATIONPK AS DCOID,
  SUM(DET.VALUE) AS IVA_COMISION_INTERMEDIACION
FROM acsele_acsele_alfa_OPENITEM OI
INNER JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
INNER JOIN acsele_acsele_alfa_UAADETAIL DET ON DET.OPENITEMID = OI.OPENITEMID
INNER JOIN acsele_acsele_alfa_PROPERTY PP ON PP.PROPERTYID = DET.PROPERTY
WHERE OI.OPM_SUBSTATUS <> 'notApplied'
  AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM COMM_INTERMEDIARY_DOCS)
  AND PP.SIMBOLO = 'IvaComision'
  AND OI.operationpk >= 986536769
GROUP BY OI.OPERATIONPK
"""

df_iva_comision_intermediacion = spark.sql(sql_iva_comision_intermediacion)
#df_iva_comision_intermediacion.createOrReplaceTempView("TMP_IVA_COMISION_INTERMEDIACION")
df_iva_comision_intermediacion.show(5)
print("✅ Campo IVA_COMISION_INTERMEDIACION generado correctamente en vista temporal: TMP_IVA_COMISION_INTERMEDIACION")

from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, explode, split, col, trim

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.sql("USE `datalake-dev-silver-catalog-db`")

sysprop_df_recaudo = spark.sql("""
    SELECT SYP_VALUE
    FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
    WHERE SYP_PROPERTY = 'RRC.commisionIntermediary'
""")

exploded_df_recaudo = sysprop_df_recaudo.select(explode(split(col("SYP_VALUE"), ",")).alias("DESCRIPTION"))
trimmed_df_recaudo = exploded_df_recaudo.select(trim(col("DESCRIPTION")).alias("DESCRIPTION"))
trimmed_df_recaudo.createOrReplaceTempView("COMM_INTERMEDIARY_DOCS_RECAUDO")

# -----------------------------
# Lógica del campo IVA_COMISION_RECAUDO con DCOID
# -----------------------------
sql_iva_recaudo = """
SELECT
  OI.OPERATIONPK AS DCOID,
  SUM(DET.VALUE) AS IVA_COMISION_RECAUDO
FROM acsele_acsele_alfa_OPENITEM OI
INNER JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
INNER JOIN acsele_acsele_alfa_UAADETAIL DET ON DET.OPENITEMID = OI.OPENITEMID
INNER JOIN acsele_acsele_alfa_PROPERTY PP ON PP.PROPERTYID = DET.PROPERTY
WHERE OI.OPM_SUBSTATUS <> 'notApplied'
  AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM COMM_INTERMEDIARY_DOCS_RECAUDO)
  AND PP.SIMBOLO = 'IvaComision'
  AND OI.operationpk >= 986536769
GROUP BY OI.OPERATIONPK
"""

df_iva_recaudo = spark.sql(sql_iva_recaudo)
#df_iva_recaudo.createOrReplaceTempView("TMP_IVA_COMISION_RECAUDO")
df_iva_recaudo.show(5)
print("✅ Campo IVA_COMISION_RECAUDO generado correctamente en vista temporal: TMP_IVA_COMISION_RECAUDO")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, trim, when, expr, lit

# -----------------------------
# Configuración para Glue G1X
# -----------------------------
spark = SparkSession.builder \
    .appName("Campos_Normativos_Unificado") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.sql("USE `datalake-dev-silver-catalog-db`")

# -----------------------------
# Crear tabla unificada de descripciones
# -----------------------------
df_sysprop = spark.sql("SELECT * FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY") \
    .filter(col("SYP_PROPERTY").isin(
        "RRC.issuedPremium.earthquakeDocType",
        "RRC.commisionIntermediary",
        "RRC.commissionCollection.earthquakeDocType"
    ))

df_desc = df_sysprop.withColumn("DESCRIPTION", explode(split(col("SYP_VALUE"), ","))) \
    .withColumn("DESCRIPTION", trim(col("DESCRIPTION"))) \
    .withColumn("TIPO",
       when(col("SYP_PROPERTY") == "RRC.issuedPremium.earthquakeDocType", "ISSUED_PREMIUM")
       .when(col("SYP_PROPERTY") == "RRC.commisionIntermediary", "INTERMEDIARY")
       .when(col("SYP_PROPERTY") == "RRC.commissionCollection.earthquakeDocType", "COLLECTION")
    )

df_desc.createOrReplaceTempView("DOC_TYPES")

# -----------------------------
# Consulta con JOINs seguros para bruto
# -----------------------------
sql_bruto = """
SELECT
  OI.OPERATIONPK AS DCOID,
  SUM(CASE 
    WHEN DTY.TIPO = 'ISSUED_PREMIUM' AND DT.DTY_ISDEBIT = 1 THEN OI.AMOUNT
    WHEN DTY.TIPO = 'ISSUED_PREMIUM' AND DT.DTY_ISDEBIT = 0 THEN OI.AMOUNT * -1
    ELSE 0 END
  ) AS ISSUEDPREMIUM,
  SUM(CASE 
    WHEN DTY.TIPO = 'INTERMEDIARY' AND PP.SIMBOLO = 'Comision' THEN DET.VALUE
    ELSE 0 END
  ) AS COMMISION_INTERMEDIARY,
  SUM(CASE 
    WHEN DTY.TIPO = 'COLLECTION' AND PP.SIMBOLO = 'ComisionRecaudo' THEN OI.AMOUNT
    ELSE 0 END
  ) AS COMMISSIONCOLLECTION
FROM acsele_acsele_alfa_OPENITEM OI
JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON OI.DTY_ID = DT.DTY_ID
LEFT JOIN acsele_acsele_alfa_UAADETAIL DET ON DET.OPENITEMID = OI.OPENITEMID
LEFT JOIN acsele_acsele_alfa_PROPERTY PP ON PP.PROPERTYID = DET.PROPERTY
JOIN DOC_TYPES DTY ON DT.DTY_DESCRIPTION = DTY.DESCRIPTION
WHERE OI.OPM_SUBSTATUS <> 'notApplied' AND OI.operationpk >= 986536769
GROUP BY OI.OPERATIONPK
"""

df_bruto = spark.sql(sql_bruto)

# Derivados bruto
df_bruto = df_bruto \
    .withColumn("PERCENTAGEPARTICALFA", lit(100)) \
    .withColumn("ALFAPREMIUM", expr("ISSUEDPREMIUM * (PERCENTAGEPARTICALFA / 100)")) \
    .withColumn("PORCPART_ALFA", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
    .withColumn("PRIMACED_COASEGURO", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
    .withColumn("GROUP_PERCENTAGE", lit(100)) \
    .withColumn("BROKERAGECOMMISSION", expr("COMMISION_INTERMEDIARY * (GROUP_PERCENTAGE / 100)")) \
    .withColumn("PORCCOMRECAUDO", expr("(COMMISSIONCOLLECTION * 100) / ISSUEDPREMIUM")) \
    .withColumn("TIPO_REGISTRO", lit("BRUTO"))

# -----------------------------
# Consulta detalle reasegurador
# -----------------------------
sql_detalle = """
SELECT
  RUD.OPERATIONPK AS DCOID,
  RO.TOTALPREMIUM AS ISSUEDPREMIUM,
  ABS(SUM(ROG.PREMIUMAMOUNT) * 100 / RO.TOTALPREMIUM) AS GROUP_PERCENTAGE,
  0 AS PERCENTAGEPARTICALFA,
  'DETALLE' AS TIPO_REGISTRO
FROM acsele_acsele_alfa_REINSURANCEOPERATION RO
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO ROG ON ROG.OPERATIONID = RO.OPERATIONID
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
INNER JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
INNER JOIN acsele_acsele_alfa_RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
WHERE RO.TOTALPREMIUM > 0 AND ROG.operationid >= 943602721
GROUP BY RUD.OPERATIONPK, RO.TOTALPREMIUM
"""

df_detalle = spark.sql(sql_detalle) \
    .withColumn("ALFAPREMIUM", expr("ISSUEDPREMIUM * (PERCENTAGEPARTICALFA / 100)")) \
    .withColumn("PORCPART_ALFA", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
    .withColumn("PRIMACED_COASEGURO", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
    .withColumn("COMMISION_INTERMEDIARY", lit(None).cast("double")) \
    .withColumn("COMMISSIONCOLLECTION", lit(None).cast("double")) \
    .withColumn("BROKERAGECOMMISSION", lit(None).cast("double")) \
    .withColumn("PORCCOMRECAUDO", lit(None).cast("double"))

# -----------------------------
# Unificación final
# -----------------------------
df_total = df_bruto.select(df_detalle.columns).unionByName(df_detalle)
#df_total.createOrReplaceTempView("TMP_IVA_COMISION_RECAUDO")
df_total.show(5)
print("✅ Campo ")
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


spark.sql("USE `datalake-dev-silver-catalog-db`")

sql_primaced = """
WITH premium_detail AS (
  SELECT
    ROG2.OPERATIONID,
    ROC2.CONTRACTTYPE,
    SUM(RD.ROD_PRORATEDPREMIUM) AS SUM_PRIMA
  FROM acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG2
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC2
    ON ROC2.REINSURANCEGROUPINFOID = ROG2.REINSURANCEGROUPINFOID
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail RD
    ON RD.REINOPERCOMPONENTID = ROC2.REINOPERCOMPONENTID
    WHERE ROG2.operationid >= 943602721
  GROUP BY ROG2.OPERATIONID, ROC2.CONTRACTTYPE

),
reinsurance_commission AS (
  SELECT
    ROG2.OPERATIONID,
    ROC2.CONTRACTTYPE,
    SUM(PT.ROPP_VALUE) AS SUM_COMISION
  FROM acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG2
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC2
    ON ROC2.REINSURANCEGROUPINFOID = ROG2.REINSURANCEGROUPINFOID
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail RD
    ON RD.REINOPERCOMPONENTID = ROC2.REINOPERCOMPONENTID
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationproperty PT
    ON PT.ROD_ID = RD.REINSURANCEOPERATIONDETAILID
  WHERE PT.ROPP_NAME IN ('ComPriCedida', 'ComPriCedidaInd') AND ROG2.operationid >= 943602721
  GROUP BY ROG2.OPERATIONID, ROC2.CONTRACTTYPE
)
SELECT
  PLO.OPERATIONPK,
  PLO.DCOID,
  RO.OPERATIONID,
  ROC.REINSURANCEGROUPINFOID,
  ROD.REINOPERCOMPONENTID,
  RG.REINSURANCEGROUPID,
  IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0,dp_aut.SUM_PRIMA) AS PRIMACEDAUT,
  IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),dp_fac.SUM_PRIMA, 0) AS PRIMACEDFAC,
  IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0, rc_aut.SUM_COMISION) AS COMISIONREASEGAUT,
  IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),rc_fac.SUM_COMISION, 0) AS COMISIONREASEGFAC
FROM acsele_acsele_alfa_reinsuranceoperation RO
INNER JOIN acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG
  ON ROG.OPERATIONID = RO.OPERATIONID
INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC
  ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail ROD
  ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
INNER JOIN acsele_acsele_alfa_ri_reinsurancegroup RG
  ON RG.REINSURANCEGROUPID = ROG.GROUPID
INNER JOIN acsele_acsele_alfa_policydco PLO
  ON RO.cor_id = PLO.operationpk
LEFT JOIN premium_detail dp_aut 
  ON dp_aut.OPERATIONID = RO.OPERATIONID AND dp_aut.CONTRACTTYPE NOT IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
LEFT JOIN premium_detail dp_fac 
  ON dp_fac.OPERATIONID = RO.OPERATIONID AND dp_fac.CONTRACTTYPE IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
LEFT JOIN reinsurance_commission rc_aut 
  ON rc_aut.OPERATIONID = RO.OPERATIONID AND rc_aut.CONTRACTTYPE NOT IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
LEFT JOIN reinsurance_commission rc_fac 
  ON rc_fac.OPERATIONID = RO.OPERATIONID AND rc_fac.CONTRACTTYPE IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
WHERE ROG.operationid >= 943602721
"""
sql_primaced = spark.sql(sql_primaced)
#sql_primaced.createOrReplaceTempView("TMP_PRODUCTO")
sql_primaced.show(5)
print('hola mundo')
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


spark.sql("USE `datalake-dev-silver-catalog-db`")

sql_primacedfac = """
WITH detail_premium AS (
  SELECT
    ROD.THIRDPARTYID,
    ROD.THIRDPARTYNAME,
    ROD.PARTICIPATIONID,
    SUM(ROD.ROD_PRORATEDPREMIUM) AS DETAIL_PREMIUM
  FROM acsele_acsele_alfa_reinsuranceoperation RO
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG ON ROG.OPERATIONID = RO.OPERATIONID
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail ROD ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
  INNER JOIN acsele_acsele_alfa_ri_reinsurancegroup RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
  WHERE ROG.operationid >= 943602721
  GROUP BY ROD.THIRDPARTYID, ROD.THIRDPARTYNAME, ROD.PARTICIPATIONID
),
reinsurance_commission AS (
  SELECT
    ROD.REINSURANCEOPERATIONDETAILID,
    SUM(PT.ROPP_VALUE) AS REINSURANCE_COMMISSION
  FROM acsele_acsele_alfa_reinsuranceoperationproperty PT
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail ROD
    ON PT.ROD_ID = ROD.REINSURANCEOPERATIONDETAILID
  WHERE PT.ROPP_NAME IN ('ComPriCedida', 'ComPriCedidaInd') AND ROD.reinopercomponentid >= 869903722
  GROUP BY ROD.REINSURANCEOPERATIONDETAILID
)
SELECT
  RA.CLASEREASEGUROINPUT AS CLASE_REASEGURO,
  RA.CODIGOREASEGURADORINPUT AS CODIGO_REASEGURADOR,
  RUD.OPERATIONPK AS DCOID,
  ROD.THIRDPARTYID,
  ROD.THIRDPARTYNAME,
  ROD.PARTICIPATIONID,
  IF(RA.CLASEREASEGUROINPUT = 'Interior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDAUT_INT,
  IF(RA.CLASEREASEGUROINPUT = 'Exterior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDAUT_EXT,
  IF(RA.CLASEREASEGUROINPUT = 'Interior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDFAC_INT,
  IF(RA.CLASEREASEGUROINPUT = 'Exterior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDFAC_EXT,
  IF(RA.CLASEREASEGUROINPUT = 'Interior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGAUT_INT,
  IF(RA.CLASEREASEGUROINPUT = 'Exterior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGAUT_EXT,
  IF(RA.CLASEREASEGUROINPUT = 'Interior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGFAC_INT,
  IF(RA.CLASEREASEGUROINPUT = 'Exterior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGFAC_EXT
FROM acsele_acsele_alfa_reinsuranceoperationdetail ROD
JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC ON ROC.REINOPERCOMPONENTID = ROD.REINOPERCOMPONENTID
JOIN acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG ON ROG.REINSURANCEGROUPINFOID = ROC.REINSURANCEGROUPINFOID
JOIN acsele_acsele_alfa_reinsuranceoperation RO ON RO.OPERATIONID = ROG.OPERATIONID
JOIN acsele_acsele_alfa_riskunitdco RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
JOIN acsele_acsele_alfa_thirdpartyrole TPR ON TPR.THIRDPARTYID = ROD.THIRDPARTYID
JOIN acsele_acsele_alfa_reasegurador RA ON RA.PK = TPR.IDDCOROLE
LEFT JOIN detail_premium dp
  ON dp.THIRDPARTYID = ROD.THIRDPARTYID
  AND dp.PARTICIPATIONID = ROD.PARTICIPATIONID
LEFT JOIN reinsurance_commission rc
  ON rc.REINSURANCEOPERATIONDETAILID = ROD.REINSURANCEOPERATIONDETAILID
WHERE ROD.reinopercomponentid >= 869903722
"""
sql_primacedfac = spark.sql(sql_primacedfac)
#sql_primaced.createOrReplaceTempView("TMP_PRODUCTO")
sql_primacedfac.show(5)
print('hola mundo 2')
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# -----------------------------
# Configuración para Glue G1X
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PORC_COM_INTERMEDIACION") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.sql("USE `datalake-dev-silver-catalog-db`")

# -----------------------------
# Lógica del campo PORC_COM_INTERMEDIACION
# -----------------------------
sql_porc_com_intermediacion = """
SELECT
  PPD.OPERATIONPK AS DCOID,
  PI.PORCENTAJECOMISIONINPUT AS PORC_COM_INTERMEDIACION
FROM acsele_acsele_alfa_STPO_POLICYPARTICIPATION PP
INNER JOIN acsele_acsele_alfa_STPO_POLICYPARTICIPATIONDCO PPD ON PP.AGREGATEDOBJECTID = PPD.AGREGATEDOBJECTID
INNER JOIN acsele_acsele_alfa_PARTICIPACIONINTERMEDIARIOS PI ON PI.PK = PPD.DCOID
"""

df_porc_com_intermediacion = spark.sql(sql_porc_com_intermediacion)
#df_porc_com_intermediacion.createOrReplaceTempView("TMP_PORC_COM_INTERMEDIACION")
df_porc_com_intermediacion.show(5)
print("✅ Campo PORC_COM_INTERMEDIACION generado correctamente en vista temporal: TMP_PORC_COM_INTERMEDIACION")

from pyspark.sql import SparkSession

# -----------------------------
# Configuración para Glue G1X
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_UNIDAD_NEGOCIO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.sql("USE `datalake-dev-silver-catalog-db`")

# -----------------------------
# Lógica del campo UNIDAD_NEGOCIO (sin filtro de fecha)
# -----------------------------
sql_unidad_negocio = """
SELECT
  DCO.OPERATIONPK AS DCOID,
  PREA.UNIDADNEGOCIOINPUT AS UNIDAD_NEGOCIO
FROM acsele_acsele_alfa_POLICYDCO DCO  
INNER JOIN acsele_acsele_alfa_STPS_RELATIONAGREEMENTPOLICY RAP ON RAP.RSA_POLICYID = DCO.AGREGATEDOBJECTID
INNER JOIN acsele_acsele_alfa_STAG_AGREEMENTVERSION AV ON AV.AST_ID = RAP.RSA_AST_ID 
INNER JOIN acsele_acsele_alfa_PREAGREEMENT PREA ON PREA.PK = AV.DCOID
"""

df_unidad_negocio = spark.sql(sql_unidad_negocio)
#df_unidad_negocio.createOrReplaceTempView("TMP_UNIDAD_NEGOCIO")
df_unidad_negocio.show(5)
print("✅ Campo UNIDAD_NEGOCIO generado correctamente en vista temporal: TMP_UNIDAD_NEGOCIO")

from pyspark.sql import SparkSession

# -----------------------------
# Configuración para Glue G1X
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_DESC_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.sql("USE `datalake-dev-silver-catalog-db`")

# -----------------------------
# Lógica del campo DESC_PRODUCTO (sin filtro OPERATIONPK)
# -----------------------------
sql_desc_producto = """
SELECT
  DCO.OPERATIONPK AS DCOID,
  CO.TIME_STAMP AS ENDORSEMENT_DATE,
  FPDCO.CCY_ID AS CURRENCY_ID, 
  PP.NUMEROPOLIZAINPUT AS POLICY_NUMBER, 
  AP.PRODUCTID, 
  PR.DESCRIPCIONPRODUCTOINPUT AS DESC_PRODUCTO,
  PR.RAMOINPUT,
  PR.RAMOVALUE,
  PP.OFICINAINPUT AS OFFICE,
  PP.TIPOPOLIZAINPUT AS POLICYTYPE_NAME, 
  PP.TIPOPOLIZAVALUE AS POLICYTYPE_CODE,
  PP.FRECUENCIAPAGOINPUT AS PAYMENTFREQUENCY_NAME,
  DCO.INITIALDATE AS POLICY_BEGINDATE,
  DCO.FINISHDATE AS POLICY_ENDDATE
FROM acsele_acsele_alfa_POLICYDCO DCO 
INNER JOIN acsele_acsele_alfa_CONTEXTOPERATION CO ON CO.ID = DCO.OPERATIONPK
INNER JOIN acsele_acsele_alfa_PREPOLICY PP ON PP.PK = DCO.DCOID
INNER JOIN acsele_acsele_alfa_FINANCIALPLANDCO FPDCO ON FPDCO.AGREGATEDPARENTID = DCO.AGREGATEDOBJECTID AND FPDCO.OPERATIONPK = DCO.OPERATIONPK
INNER JOIN acsele_acsele_alfa_AGREGATEDPOLICY AP ON AP.AGREGATEDPOLICYID = DCO.AGREGATEDOBJECTID  
INNER JOIN acsele_acsele_alfa_PRODUCT PT ON PT.PRODUCTID = AP.PRODUCTID
INNER JOIN acsele_acsele_alfa_PREPRODUCT PR ON PT.PRO_DCOID = PR.PK
"""

df_desc_producto = spark.sql(sql_desc_producto)
#df_desc_producto.createOrReplaceTempView("TMP_DESC_PRODUCTO")
df_desc_producto.show(5)
print("✅ Campo DESC_PRODUCTO generado correctamente en vista temporal: TMP_DESC_PRODUCTO")

from pyspark.sql import SparkSession

# -----------------------------
# Configuración para Glue G1X
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_RISKUNIT") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.sql("USE `datalake-dev-silver-catalog-db`")

# -----------------------------
# Lógica del campo RISKUNIT (sin filtros)
# -----------------------------
sql_riskunit = """
SELECT
  DCO.OPERATIONPK AS DCOID,
  PRU.NUMUNIDRIESGOINPUT AS RISKUNIT_NUMBER,
  DCO.INITIALDATE AS RISKUNIT_BEGINDATE, 
  DCO.FINISHDATE AS RISKUNIT_ENDDATE
FROM acsele_acsele_alfa_PRERISKUNITS PRU
INNER JOIN acsele_acsele_alfa_RISKUNITDCO DCO ON DCO.DCOID = PRU.PK
"""

df_riskunit = spark.sql(sql_riskunit)
#df_riskunit.createOrReplaceTempView("TMP_RISKUNIT")
df_riskunit.show(5)
print("✅ Campo RISKUNIT generado correctamente en vista temporal: TMP_RISKUNIT")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, trim

# -----------------------------
# Configuración para Glue G1X
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_TOMADOR_POLIZA") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.sql("USE `datalake-dev-silver-catalog-db`")

# -----------------------------
# Explosión de descripciones válidas desde STST_SYSTEMPROPERTY
# -----------------------------
bondholder_df = spark.sql("""
    SELECT SYP_VALUE
    FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
    WHERE SYP_PROPERTY = 'bondholder.rol'
""")
bondholder_exploded = bondholder_df.select(explode(split(col("SYP_VALUE"), ",")).alias("ROL_DESCRIPTION"))
bondholder_trimmed = bondholder_exploded.select(trim(col("ROL_DESCRIPTION")).alias("ROL_DESCRIPTION"))
bondholder_trimmed.createOrReplaceTempView("BONDHOLDER_ROLES")

# -----------------------------
# Lógica del campo TOMADOR_POLIZA
# -----------------------------
sql_tomador_poliza = """
SELECT
  PPD.OPERATIONPK AS DCOID,
  TP.TPT_ID AS THIRDPARTY_ID,
  PTP.NOMBREPREDETERMINADOINPUT AS TOMADOR_POLIZA
FROM acsele_acsele_alfa_STPO_POLICYPARTICIPATION PP
INNER JOIN acsele_acsele_alfa_STPO_POLICYPARTICIPATIONDCO PPD ON PP.AGREGATEDOBJECTID = PPD.AGREGATEDOBJECTID 
INNER JOIN acsele_acsele_alfa_STTE_THIRDPARTY TP ON TP.TPT_ID = PP.THIRDPARTYID
INNER JOIN acsele_acsele_alfa_PRETHIRDPARTY PTP ON PTP.PK = TP.IDDCO
INNER JOIN acsele_acsele_alfa_STTS_ROLE RL ON RL.ROL_ID = PP.ROL_ID
WHERE RL.ROL_DESCRIPTION IN (SELECT ROL_DESCRIPTION FROM BONDHOLDER_ROLES)
"""

df_tomador_poliza = spark.sql(sql_tomador_poliza)
#df_tomador_poliza.createOrReplaceTempView("TMP_TOMADOR_POLIZA")
df_tomador_poliza.show(5)
print("✅ Campo TOMADOR_POLIZA generado correctamente en vista temporal: TMP_TOMADOR_POLIZA")

from pyspark.sql import SparkSession

# -----------------------------
# Configuración para Glue G1X
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_CODREASEG") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.sql("USE `datalake-dev-silver-catalog-db`")

# -----------------------------
# Lógica del campo CODREASEG (sin filtro THIRDPARTYID)
# -----------------------------
sql_codreaseg = """
SELECT
  TPR.THIRDPARTYID AS DCOID,
  RA.CLASEREASEGUROINPUT AS CLASE_REASEGURO,
  RA.CODIGOREASEGURADORINPUT AS CODREASEG
FROM acsele_acsele_alfa_THIRDPARTYROLE TPR
INNER JOIN acsele_acsele_alfa_REASEGURADOR RA ON RA.PK = TPR.IDDCOROLE
"""

df_codreaseg = spark.sql(sql_codreaseg)
#df_codreaseg.createOrReplaceTempView("TMP_CODREASEG")
df_codreaseg.show(5)
print("✅ Campo CODREASEG generado correctamente en vista temporal: TMP_CODREASEG")

from pyspark.sql import SparkSession

# -----------------------------
# Configuración para Glue G1X
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_NOMREASEG") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.sql("USE `datalake-dev-silver-catalog-db`")

# -----------------------------
# Lógica del campo NOMREASEG (sin filtros)
# -----------------------------
sql_nomreaseg = """
SELECT
  RUD.DCOID AS DCOID,
  ROD.THIRDPARTYID AS THIRDPARTY_ID, 
  ROD.THIRDPARTYNAME AS NOMREASEG,
  ROD.PARTICIPATIONID AS PARTICIPATION_ID, 
  SUM(ROD.ROD_PRORATEDPREMIUM) AS DETAIL_PREMIUM
FROM acsele_acsele_alfa_REINSURANCEOPERATION RO
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO ROG ON ROG.OPERATIONID = RO.OPERATIONID
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONDETAIL ROD ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
INNER JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
INNER JOIN acsele_acsele_alfa_RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
WHERE ROC.CONTRACTTYPE = 'MandatoryFacultativeContract'
GROUP BY ROD.THIRDPARTYID, ROD.THIRDPARTYNAME, ROD.PARTICIPATIONID, RUD.OPERATIONPK, RUD.DCOID
"""

df_nomreaseg = spark.sql(sql_nomreaseg)
#df_nomreaseg.createOrReplaceTempView("TMP_NOMREASEG")
df_nomreaseg.show(5)
print("✅ Campo NOMREASEG generado correctamente en vista temporal: TMP_NOMREASEG")

from pyspark.sql import SparkSession

# -----------------------------
# Configuración para Glue G1X
# -----------------------------
spark = SparkSession.builder \
    .appName("Campos_COMPANIA_POLIZA_MATRIZ") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.sql("USE `datalake-dev-silver-catalog-db`")

# Ejecutar consulta
sql_compania = """
SELECT
  RUD.OPERATIONPK AS DCOID,
  RG.RIG_COMPANYCODE AS COMPANIA
FROM acsele_acsele_alfa_RI_REINSURANCEGROUP RG
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO ROG ON ROG.GROUPID = RG.REINSURANCEGROUPID
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATION RO ON RO.OPERATIONID = ROG.OPERATIONID
INNER JOIN acsele_acsele_alfa_RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID

"""

df_compania = spark.sql(sql_compania)
df_compania.createOrReplaceTempView("TMP_COMPANIA")
df_compania.show(5)
print("✅ Campo COMPANIA generado correctamente en vista temporal: TMP_COMPANIA")

# =============================
# CAMPO 2: POLIZA_MATRIZ
# =============================

# Ejecutar consulta
sql_poliza_matriz = """
SELECT
  DCO.OPERATIONPK AS DCOID,
  PREA.UNIDADNEGOCIOINPUT AS BUSINESSUNIT_NAME,
  AGS.AST_NUMEROACU AS POLIZA_MATRIZ
FROM acsele_acsele_alfa_POLICYDCO DCO
INNER JOIN acsele_acsele_alfa_STPS_RELATIONAGREEMENTPOLICY RAP ON RAP.RSA_POLICYID = DCO.AGREGATEDOBJECTID
INNER JOIN acsele_acsele_alfa_STAG_AGREEMENTVERSION AV ON AV.AST_ID = RAP.RSA_AST_ID
INNER JOIN acsele_acsele_alfa_PREAGREEMENT PREA ON PREA.PK = AV.DCOID
INNER JOIN acsele_acsele_alfa_STPS_AGREEMENTSTATICDAT AGS ON RAP.RSA_AST_ID = AGS.AST_ID
"""

df_poliza_matriz = spark.sql(sql_poliza_matriz)
#df_poliza_matriz.createOrReplaceTempView("TMP_POLIZA_MATRIZ")
df_poliza_matriz.show(5)
print("✅ Campo POLIZA_MATRIZ generado correctamente en vista temporal: TMP_POLIZA_MATRIZ")

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder \
    .appName("Campo_ID_TOMADOR_POLIZA_Optimizado") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.sql("USE `datalake-dev-silver-catalog-db`")

# Ejecutar la consulta optimizada
sql_id_tomador_poliza = """
SELECT
  RUD.OPERATIONPK AS DCOID,
  PAR.PERCENTAGE,
  PRE.DOCUMENTOIDENTIDADINPUT AS ID_TOMADOR_POLIZA,
  TP.TPT_ID AS THIRDPARTY_ID
FROM acsele_acsele_alfa_RI_PARTICIPATION PAR
INNER JOIN acsele_acsele_alfa_STTE_THIRDPARTY TP ON TP.TPT_ID = PAR.THIRDPARTYID
INNER JOIN acsele_acsele_alfa_PRETHIRDPARTY PRE ON TP.IDDCO = PRE.PK
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONDETAIL ROD ON ROD.THIRDPARTYID = PAR.THIRDPARTYID
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT ROC ON ROC.REINOPERCOMPONENTID = ROD.REINOPERCOMPONENTID
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO ROG ON ROG.REINSURANCEGROUPINFOID = ROC.REINSURANCEGROUPINFOID
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATION RO ON RO.OPERATIONID = ROG.OPERATIONID
INNER JOIN acsele_acsele_alfa_RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
"""

df_id_tomador_poliza = spark.sql(sql_id_tomador_poliza)
#df_id_tomador_poliza.createOrReplaceTempView("TMP_ID_TOMADOR_POLIZA")
#df_id_tomador_poliza.show(5)

print("✅ Campo ID_TOMADOR_POLIZA optimizado generado correctamente en vista temporal: TMP_ID_TOMADOR_POLIZA")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, trim

# -----------------------------
# Configuración para Glue G1X
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_IVA_COMISION_INTERMEDIACION") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.sql("USE `datalake-dev-silver-catalog-db`")

# -----------------------------
# Explosión de SYP_VALUE como lista de DTY_DESCRIPTION válidas
# -----------------------------
sysprop_df = spark.sql("""
    SELECT SYP_VALUE
    FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
    WHERE SYP_PROPERTY = 'RRC.commisionIntermediary'
""")

exploded_df = sysprop_df.select(explode(split(col("SYP_VALUE"), ",")).alias("DESCRIPTION"))
trimmed_df = exploded_df.select(trim(col("DESCRIPTION")).alias("DESCRIPTION"))
trimmed_df.createOrReplaceTempView("COMM_INTERMEDIARY_DOCS")

# -----------------------------
# Lógica del campo IVA_COMISION_INTERMEDIACION
# -----------------------------
sql_iva_comision_intermediacion = """
SELECT
  OI.OPERATIONPK AS DCOID,
  SUM(DET.VALUE) AS IVA_COMISION_INTERMEDIACION
FROM acsele_acsele_alfa_OPENITEM OI
INNER JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
INNER JOIN acsele_acsele_alfa_UAADETAIL DET ON DET.OPENITEMID = OI.OPENITEMID
INNER JOIN acsele_acsele_alfa_PROPERTY PP ON PP.PROPERTYID = DET.PROPERTY
WHERE OI.OPM_SUBSTATUS <> 'notApplied'
  AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM COMM_INTERMEDIARY_DOCS)
  AND PP.SIMBOLO = 'IvaComision'
GROUP BY OI.OPERATIONPK
"""

df_iva_comision_intermediacion = spark.sql(sql_iva_comision_intermediacion)
#df_iva_comision_intermediacion.createOrReplaceTempView("TMP_IVA_COMISION_INTERMEDIACION")
df_iva_comision_intermediacion.show(5)
print("✅ Campo IVA_COMISION_INTERMEDIACION generado correctamente en vista temporal: TMP_IVA_COMISION_INTERMEDIACION")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, trim

# -----------------------------
# Configuración para Glue G1X
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_IVA_COMISION_RECAUDO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.sql("USE `datalake-dev-silver-catalog-db`")

# -----------------------------
# Explosión de descripciones válidas desde STST_SYSTEMPROPERTY
# -----------------------------
sysprop_df_recaudo = spark.sql("""
    SELECT SYP_VALUE
    FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
    WHERE SYP_PROPERTY = 'RRC.commisionIntermediary'
""")

exploded_df_recaudo = sysprop_df_recaudo.select(explode(split(col("SYP_VALUE"), ",")).alias("DESCRIPTION"))
trimmed_df_recaudo = exploded_df_recaudo.select(trim(col("DESCRIPTION")).alias("DESCRIPTION"))
trimmed_df_recaudo.createOrReplaceTempView("COMM_INTERMEDIARY_DOCS_RECAUDO")

# -----------------------------
# Lógica del campo IVA_COMISION_RECAUDO con DCOID
# -----------------------------
sql_iva_recaudo = """
SELECT
  OI.OPERATIONPK AS DCOID,
  SUM(DET.VALUE) AS IVA_COMISION_RECAUDO
FROM acsele_acsele_alfa_OPENITEM OI
INNER JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
INNER JOIN acsele_acsele_alfa_UAADETAIL DET ON DET.OPENITEMID = OI.OPENITEMID
INNER JOIN acsele_acsele_alfa_PROPERTY PP ON PP.PROPERTYID = DET.PROPERTY
WHERE OI.OPM_SUBSTATUS <> 'notApplied'
  AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM COMM_INTERMEDIARY_DOCS_RECAUDO)
  AND PP.SIMBOLO = 'IvaComision'
GROUP BY OI.OPERATIONPK
"""

df_iva_recaudo = spark.sql(sql_iva_recaudo)
#df_iva_recaudo.createOrReplaceTempView("TMP_IVA_COMISION_RECAUDO")
df_iva_recaudo.show(5)
print("✅ Campo IVA_COMISION_RECAUDO generado correctamente en vista temporal: TMP_IVA_COMISION_RECAUDO")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, trim, when, expr, lit

# -----------------------------
# Configuración para Glue G1X
# -----------------------------
spark = SparkSession.builder \
    .appName("Campos_Normativos_Unificado") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.sql("USE `datalake-dev-silver-catalog-db`")

# -----------------------------
# Crear tabla unificada de descripciones
# -----------------------------
df_sysprop = spark.sql("SELECT * FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY") \
    .filter(col("SYP_PROPERTY").isin(
        "RRC.issuedPremium.earthquakeDocType",
        "RRC.commisionIntermediary",
        "RRC.commissionCollection.earthquakeDocType"
    ))

df_desc = df_sysprop.withColumn("DESCRIPTION", explode(split(col("SYP_VALUE"), ","))) \
    .withColumn("DESCRIPTION", trim(col("DESCRIPTION"))) \
    .withColumn("TIPO",
        when(col("SYP_PROPERTY") == "RRC.issuedPremium.earthquakeDocType", "ISSUED_PREMIUM")
       .when(col("SYP_PROPERTY") == "RRC.commisionIntermediary", "INTERMEDIARY")
       .when(col("SYP_PROPERTY") == "RRC.commissionCollection.earthquakeDocType", "COLLECTION")
    )

df_desc.createOrReplaceTempView("DOC_TYPES")

# -----------------------------
# Consulta con JOINs seguros para bruto
# -----------------------------
sql_bruto = """
SELECT
  OI.OPERATIONPK AS DCOID,
  SUM(CASE 
    WHEN DTY.TIPO = 'ISSUED_PREMIUM' AND DT.DTY_ISDEBIT = 1 THEN OI.AMOUNT
    WHEN DTY.TIPO = 'ISSUED_PREMIUM' AND DT.DTY_ISDEBIT = 0 THEN OI.AMOUNT * -1
    ELSE 0 END
  ) AS ISSUEDPREMIUM,
  SUM(CASE 
    WHEN DTY.TIPO = 'INTERMEDIARY' AND PP.SIMBOLO = 'Comision' THEN DET.VALUE
    ELSE 0 END
  ) AS COMMISION_INTERMEDIARY,
  SUM(CASE 
    WHEN DTY.TIPO = 'COLLECTION' AND PP.SIMBOLO = 'ComisionRecaudo' THEN OI.AMOUNT
    ELSE 0 END
  ) AS COMMISSIONCOLLECTION
FROM acsele_acsele_alfa_OPENITEM OI
JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON OI.DTY_ID = DT.DTY_ID
LEFT JOIN acsele_acsele_alfa_UAADETAIL DET ON DET.OPENITEMID = OI.OPENITEMID
LEFT JOIN acsele_acsele_alfa_PROPERTY PP ON PP.PROPERTYID = DET.PROPERTY
JOIN DOC_TYPES DTY ON DT.DTY_DESCRIPTION = DTY.DESCRIPTION
WHERE OI.OPM_SUBSTATUS <> 'notApplied'
GROUP BY OI.OPERATIONPK
"""

df_bruto = spark.sql(sql_bruto)

# Derivados bruto
df_bruto = df_bruto \
    .withColumn("PERCENTAGEPARTICALFA", lit(100)) \
    .withColumn("ALFAPREMIUM", expr("ISSUEDPREMIUM * (PERCENTAGEPARTICALFA / 100)")) \
    .withColumn("PORCPART_ALFA", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
    .withColumn("PRIMACED_COASEGURO", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
    .withColumn("GROUP_PERCENTAGE", lit(100)) \
    .withColumn("BROKERAGECOMMISSION", expr("COMMISION_INTERMEDIARY * (GROUP_PERCENTAGE / 100)")) \
    .withColumn("PORCCOMRECAUDO", expr("(COMMISSIONCOLLECTION * 100) / ISSUEDPREMIUM")) \
    .withColumn("TIPO_REGISTRO", lit("BRUTO"))

# -----------------------------
# Consulta detalle reasegurador
# -----------------------------
sql_detalle = """
SELECT
  RUD.OPERATIONPK AS DCOID,
  RO.TOTALPREMIUM AS ISSUEDPREMIUM,
  ABS(SUM(ROG.PREMIUMAMOUNT) * 100 / RO.TOTALPREMIUM) AS GROUP_PERCENTAGE,
  0 AS PERCENTAGEPARTICALFA,
  'DETALLE' AS TIPO_REGISTRO
FROM acsele_acsele_alfa_REINSURANCEOPERATION RO
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO ROG ON ROG.OPERATIONID = RO.OPERATIONID
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
INNER JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
INNER JOIN acsele_acsele_alfa_RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
WHERE RO.TOTALPREMIUM > 0
GROUP BY RUD.OPERATIONPK, RO.TOTALPREMIUM
"""

df_detalle = spark.sql(sql_detalle) \
    .withColumn("ALFAPREMIUM", expr("ISSUEDPREMIUM * (PERCENTAGEPARTICALFA / 100)")) \
    .withColumn("PORCPART_ALFA", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
    .withColumn("PRIMACED_COASEGURO", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
    .withColumn("COMMISION_INTERMEDIARY", lit(None).cast("double")) \
    .withColumn("COMMISSIONCOLLECTION", lit(None).cast("double")) \
    .withColumn("BROKERAGECOMMISSION", lit(None).cast("double")) \
    .withColumn("PORCCOMRECAUDO", lit(None).cast("double"))

# -----------------------------
# Unificación final
# -----------------------------
df_total = df_bruto.select(df_detalle.columns).unionByName(df_detalle)
#df_total.createOrReplaceTempView("TMP_IVA_COMISION_RECAUDO")
df_total.show(5)
print("✅ Campo ")
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


spark.sql("USE `datalake-dev-silver-catalog-db`")

sql_primaced = """
WITH premium_detail AS (
  SELECT
    ROG2.OPERATIONID,
    ROC2.CONTRACTTYPE,
    SUM(RD.ROD_PRORATEDPREMIUM) AS SUM_PRIMA
  FROM acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG2
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC2
    ON ROC2.REINSURANCEGROUPINFOID = ROG2.REINSURANCEGROUPINFOID
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail RD
    ON RD.REINOPERCOMPONENTID = ROC2.REINOPERCOMPONENTID
  GROUP BY ROG2.OPERATIONID, ROC2.CONTRACTTYPE
),
reinsurance_commission AS (
  SELECT
    ROG2.OPERATIONID,
    ROC2.CONTRACTTYPE,
    SUM(PT.ROPP_VALUE) AS SUM_COMISION
  FROM acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG2
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC2
    ON ROC2.REINSURANCEGROUPINFOID = ROG2.REINSURANCEGROUPINFOID
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail RD
    ON RD.REINOPERCOMPONENTID = ROC2.REINOPERCOMPONENTID
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationproperty PT
    ON PT.ROD_ID = RD.REINSURANCEOPERATIONDETAILID
  WHERE PT.ROPP_NAME IN ('ComPriCedida', 'ComPriCedidaInd')
  GROUP BY ROG2.OPERATIONID, ROC2.CONTRACTTYPE
)
SELECT
  PLO.OPERATIONPK,
  PLO.DCOID,
  RO.OPERATIONID,
  ROC.REINSURANCEGROUPINFOID,
  ROD.REINOPERCOMPONENTID,
  RG.REINSURANCEGROUPID,
  IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0,dp_aut.SUM_PRIMA) AS PRIMACEDAUT,
  IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),dp_fac.SUM_PRIMA, 0) AS PRIMACEDFAC,
  IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0, rc_aut.SUM_COMISION) AS COMISIONREASEGAUT,
  IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),rc_fac.SUM_COMISION, 0) AS COMISIONREASEGFAC
FROM acsele_acsele_alfa_reinsuranceoperation RO
INNER JOIN acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG
  ON ROG.OPERATIONID = RO.OPERATIONID
INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC
  ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail ROD
  ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
INNER JOIN acsele_acsele_alfa_ri_reinsurancegroup RG
  ON RG.REINSURANCEGROUPID = ROG.GROUPID
INNER JOIN acsele_acsele_alfa_policydco PLO
  ON RO.cor_id = PLO.operationpk
LEFT JOIN premium_detail dp_aut 
  ON dp_aut.OPERATIONID = RO.OPERATIONID AND dp_aut.CONTRACTTYPE NOT IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
LEFT JOIN premium_detail dp_fac 
  ON dp_fac.OPERATIONID = RO.OPERATIONID AND dp_fac.CONTRACTTYPE IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
LEFT JOIN reinsurance_commission rc_aut 
  ON rc_aut.OPERATIONID = RO.OPERATIONID AND rc_aut.CONTRACTTYPE NOT IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
LEFT JOIN reinsurance_commission rc_fac 
  ON rc_fac.OPERATIONID = RO.OPERATIONID AND rc_fac.CONTRACTTYPE IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
"""
sql_primaced = spark.sql(sql_primaced)
#sql_primaced.createOrReplaceTempView("TMP_PRODUCTO")
sql_primaced.show(5)
print('hola mundo')
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


spark.sql("USE `datalake-dev-silver-catalog-db`")

sql_primacedfac = """
WITH detail_premium AS (
  SELECT
    ROD.THIRDPARTYID,
    ROD.THIRDPARTYNAME,
    ROD.PARTICIPATIONID,
    SUM(ROD.ROD_PRORATEDPREMIUM) AS DETAIL_PREMIUM
  FROM acsele_acsele_alfa_reinsuranceoperation RO
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG ON ROG.OPERATIONID = RO.OPERATIONID
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail ROD ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
  INNER JOIN acsele_acsele_alfa_ri_reinsurancegroup RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
  GROUP BY ROD.THIRDPARTYID, ROD.THIRDPARTYNAME, ROD.PARTICIPATIONID
),
reinsurance_commission AS (
  SELECT
    ROD.REINSURANCEOPERATIONDETAILID,
    SUM(PT.ROPP_VALUE) AS REINSURANCE_COMMISSION
  FROM acsele_acsele_alfa_reinsuranceoperationproperty PT
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail ROD
    ON PT.ROD_ID = ROD.REINSURANCEOPERATIONDETAILID
  WHERE PT.ROPP_NAME IN ('ComPriCedida', 'ComPriCedidaInd')
  GROUP BY ROD.REINSURANCEOPERATIONDETAILID
)
SELECT
  RA.CLASEREASEGUROINPUT AS CLASE_REASEGURO,
  RA.CODIGOREASEGURADORINPUT AS CODIGO_REASEGURADOR,
  RUD.OPERATIONPK AS DCOID,
  ROD.THIRDPARTYID,
  ROD.THIRDPARTYNAME,
  ROD.PARTICIPATIONID,
  IF(RA.CLASEREASEGUROINPUT = 'Interior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDAUT_INT,
  IF(RA.CLASEREASEGUROINPUT = 'Exterior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDAUT_EXT,
  IF(RA.CLASEREASEGUROINPUT = 'Interior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDFAC_INT,
  IF(RA.CLASEREASEGUROINPUT = 'Exterior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDFAC_EXT,
  IF(RA.CLASEREASEGUROINPUT = 'Interior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGAUT_INT,
  IF(RA.CLASEREASEGUROINPUT = 'Exterior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGAUT_EXT,
  IF(RA.CLASEREASEGUROINPUT = 'Interior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGFAC_INT,
  IF(RA.CLASEREASEGUROINPUT = 'Exterior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGFAC_EXT
FROM acsele_acsele_alfa_reinsuranceoperationdetail ROD
JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC ON ROC.REINOPERCOMPONENTID = ROD.REINOPERCOMPONENTID
JOIN acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG ON ROG.REINSURANCEGROUPINFOID = ROC.REINSURANCEGROUPINFOID
JOIN acsele_acsele_alfa_reinsuranceoperation RO ON RO.OPERATIONID = ROG.OPERATIONID
JOIN acsele_acsele_alfa_riskunitdco RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
JOIN acsele_acsele_alfa_thirdpartyrole TPR ON TPR.THIRDPARTYID = ROD.THIRDPARTYID
JOIN acsele_acsele_alfa_reasegurador RA ON RA.PK = TPR.IDDCOROLE
LEFT JOIN detail_premium dp
  ON dp.THIRDPARTYID = ROD.THIRDPARTYID
  AND dp.PARTICIPATIONID = ROD.PARTICIPATIONID
LEFT JOIN reinsurance_commission rc
  ON rc.REINSURANCEOPERATIONDETAILID = ROD.REINSURANCEOPERATIONDETAILID
"""
sql_primacedfac = spark.sql(sql_primacedfac)
#sql_primaced.createOrReplaceTempView("TMP_PRODUCTO")
#sql_primacedfac.show(5)
print('hola mundo 2')
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


spark.sql("USE `datalake-dev-silver-catalog-db`")

sql_producto = """
SELECT DISTINCT
    DCO.DCOID,
    CO.TIME_STAMP AS FECHA_ENDOSO,
    FPDCO.CCY_ID AS CURRENCY_ID,
    PP.NUMEROPOLIZAINPUT AS POLIZA,
    AP.PRODUCTID AS PRODUCTO,
    PP.OFICINAINPUT AS SUCURSAL,
    PP.TIPOPOLIZAINPUT AS POLICYTYPE_NAME,
    PP.TIPOPOLIZAVALUE AS POLICYTYPE_CODE,
    PP.FRECUENCIAPAGOINPUT AS FRECUENCIA_PAGO,
    DCO.INITIALDATE AS FECHA_INICIO_POLIZA_MATRIZ,
    DCO.FINISHDATE AS FECHA_FIN_POLIZA_MATRIZ
FROM acsele_acsele_alfa_POLICYDCO DCO
JOIN acsele_acsele_alfa_CONTEXTOPERATION CO ON CO.ID = DCO.OPERATIONPK
JOIN acsele_acsele_alfa_PREPOLICY PP ON PP.PK = DCO.DCOID
JOIN acsele_acsele_alfa_FINANCIALPLANDCO FPDCO ON FPDCO.AGREGATEDPARENTID = DCO.AGREGATEDOBJECTID AND FPDCO.OPERATIONPK = DCO.OPERATIONPK
JOIN acsele_acsele_alfa_AGREGATEDPOLICY AP ON AP.AGREGATEDPOLICYID = DCO.AGREGATEDOBJECTID
"""
df_producto = spark.sql(sql_producto)
df_producto.createOrReplaceTempView("TMP_PRODUCTO")
#df_producto.show(5)



#---------------------------------------------------------------------------------------------------------------------------------------

spark.sql("USE `datalake-dev-silver-catalog-db`")

# -----------------------------
# Consulta reestructurada (sin subconsultas correlacionadas)
# -----------------------------
sql_ramo_final = '''
SELECT DISTINCT
    CDCO.OPERATIONPK AS DCOID,
    PP.COMPANIAINPUT AS COMPANIA,
    REPLACE(PP.COMPANIAVALUE, '.0', '') AS CODIGO_COMPANIA,
    PP.DESCRIPCIONPRODUCTOINPUT AS NOMBRE_PRODUCTO,
    PP.CODIGOPRODUCTOINPUT AS CODIGO_PRODUCTO,
    PPRE.NUMEROPOLIZAINPUT AS NUMERO_POLIZA,
    PPRE.POLIZAMAESTRAINPUT AS ACUERDO,
    REPLACE(CB.COBCODIGOCOBERTURAINPUT, '.0', '') AS COBERTURA,
    CB.COBDESCRIPCIONCOBERTINPUT AS DESC_COBERTURA,
    CASE WHEN CB.COBACUMULACIONINPUT IN ('A', 'P') THEN 'SI' ELSE 'NO' END AS ACUMULA,
    REPLACE(CB.COBRAMOREASEGUROINPUT, '.0', '') AS RAMO_REASEGURO,
    REPLACE(CB.COBRAMOCONTABLEINPUT, '.0', '') AS RAMO,
    PP.RAMOINPUT AS DESC_RAMO,
    RG.DESCRIPTION AS CODIGO_RAMO_REASEGURO,
    RK.REK_CODE AS GRUPO_REASEGURO,
    RK.REK_DESCRIPTION AS NOMBRE_GRUPO_REASEGURO,
    COALESCE(CB.COBPRIMAINPUT, '0') AS PRIMA,
    CB.COBPRIMAREASEGUROINPUT AS PRIMA_REASEGURO
FROM acsele_acsele_alfa_COVERAGEDCO CDCO
JOIN acsele_acsele_alfa_COBERTURA CB ON CB.PK = CDCO.DCOID
LEFT JOIN acsele_acsele_alfa_POLICYDCO PDCO ON PDCO.OPERATIONPK = CDCO.OPERATIONPK
LEFT JOIN acsele_acsele_alfa_AGREGATEDPOLICY AP ON AP.AGREGATEDPOLICYID = PDCO.AGREGATEDOBJECTID
LEFT JOIN acsele_acsele_alfa_PRODUCT PD ON PD.PRODUCTID = AP.PRODUCTID
LEFT JOIN acsele_acsele_alfa_PREPRODUCT PP ON PP.PK = PD.PRO_DCOID
LEFT JOIN acsele_acsele_alfa_PREPOLICY PPRE ON PPRE.PK = PDCO.DCOID
LEFT JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.CODE = REPLACE(CB.COBRAMOREASEGUROINPUT, '.0', '')
LEFT JOIN acsele_acsele_alfa_STRI_REINSURANCEKIND RK ON RK.REKIND_ID = RG.REKIND_ID
'''

df_ramo_final = spark.sql(sql_ramo_final)
df_ramo_final.createOrReplaceTempView("TMP_RAMO")
#df_ramo_final.show(5)
print("✅ Campo RAMO reestructurado sin subconsultas y con los nombres finales de salida.")

#--------------------------JOIN OK DF 1 Y 2 -----------------------------------------------------------------------------------------------
#DF1 PK = POLICYDCO.DCOID DF2 PK = POLICYDCO.OPERATIONPK  RENAME DCOID

df_test_join = spark.sql(
    """
    select p.DCOID, p.FECHA_ENDOSO, p.CURRENCY_ID, p.POLIZA, p.PRODUCTO, p.SUCURSAL, 
    p.POLICYTYPE_NAME, p.POLICYTYPE_CODE, p.FRECUENCIA_PAGO, p.FECHA_INICIO_POLIZA_MATRIZ, 
    p.FECHA_FIN_POLIZA_MATRIZ, r.COMPANIA, r.CODIGO_COMPANIA, r.NOMBRE_PRODUCTO, r.CODIGO_PRODUCTO, 
    r.ACUERDO, r.COBERTURA, r.DESC_COBERTURA, r.ACUMULA, r.RAMO_REASEGURO, r.RAMO, r.DESC_RAMO, 
    r.CODIGO_RAMO_REASEGURO, r.GRUPO_REASEGURO, r.NOMBRE_GRUPO_REASEGURO, r.PRIMA, r.PRIMA_REASEGURO
    from TMP_PRODUCTO p
    INNER JOIN TMP_RAMO r
    on p.DCOID = r.DCOID
    """)
#df_test_join.show(5)
print("✅ JOIN OK DF 1 Y 2")

df_test_join = df_test_join.repartition(100)

s3_out_path = "s3://alfa-datalake-dev-gold/GOLD/PCR_TEST_GOLD/"
df_test_join.write\
.mode("overwrite")\
.option("compression", "snappy")\
.parquet(s3_out_path)

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
#from commons.ExtractHelper import ExtractHelper
from pyspark.sql import functions as F
from awsglue import DynamicFrame

import boto3, datetime, json

s3_client = boto3.client("s3")  

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.default.parallelism", "200")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

#----LOGIC OF DATAFRAME TO SAVE ----

spark.sql("USE `datalake-dev-silver-catalog-db`")

sql_producto = """
SELECT DISTINCT
    DCO.DCOID,
    CO.TIME_STAMP AS FECHA_ENDOSO,
    FPDCO.CCY_ID AS CURRENCY_ID,
    PP.NUMEROPOLIZAINPUT AS POLIZA,
    AP.PRODUCTID AS PRODUCTO,
    PP.OFICINAINPUT AS SUCURSAL,
    PP.TIPOPOLIZAINPUT AS POLICYTYPE_NAME,
    PP.TIPOPOLIZAVALUE AS POLICYTYPE_CODE,
    PP.FRECUENCIAPAGOINPUT AS FRECUENCIA_PAGO,
    DCO.INITIALDATE AS FECHA_INICIO_POLIZA_MATRIZ,
    DCO.FINISHDATE AS FECHA_FIN_POLIZA_MATRIZ
FROM acsele_acsele_alfa_POLICYDCO DCO
JOIN acsele_acsele_alfa_CONTEXTOPERATION CO ON CO.ID = DCO.OPERATIONPK
JOIN acsele_acsele_alfa_PREPOLICY PP ON PP.PK = DCO.DCOID
JOIN acsele_acsele_alfa_FINANCIALPLANDCO FPDCO ON FPDCO.AGREGATEDPARENTID = DCO.AGREGATEDOBJECTID AND FPDCO.OPERATIONPK = DCO.OPERATIONPK
JOIN acsele_acsele_alfa_AGREGATEDPOLICY AP ON AP.AGREGATEDPOLICYID = DCO.AGREGATEDOBJECTID
"""
df_producto = spark.sql(sql_producto)
df_producto.createOrReplaceTempView("TMP_PRODUCTO")
#df_producto.show(5)



#---------------------------------------------------------------------------------------------------------------------------------------

spark.sql("USE `datalake-dev-silver-catalog-db`")

# -----------------------------
# Consulta reestructurada (sin subconsultas correlacionadas)
# -----------------------------
sql_ramo_final = '''
SELECT DISTINCT
    CDCO.OPERATIONPK AS DCOID,
    PP.COMPANIAINPUT AS COMPANIA,
    REPLACE(PP.COMPANIAVALUE, '.0', '') AS CODIGO_COMPANIA,
    PP.DESCRIPCIONPRODUCTOINPUT AS NOMBRE_PRODUCTO,
    PP.CODIGOPRODUCTOINPUT AS CODIGO_PRODUCTO,
    PPRE.NUMEROPOLIZAINPUT AS NUMERO_POLIZA,
    PPRE.POLIZAMAESTRAINPUT AS ACUERDO,
    REPLACE(CB.COBCODIGOCOBERTURAINPUT, '.0', '') AS COBERTURA,
    CB.COBDESCRIPCIONCOBERTINPUT AS DESC_COBERTURA,
    CASE WHEN CB.COBACUMULACIONINPUT IN ('A', 'P') THEN 'SI' ELSE 'NO' END AS ACUMULA,
    REPLACE(CB.COBRAMOREASEGUROINPUT, '.0', '') AS RAMO_REASEGURO,
    REPLACE(CB.COBRAMOCONTABLEINPUT, '.0', '') AS RAMO,
    PP.RAMOINPUT AS DESC_RAMO,
    RG.DESCRIPTION AS CODIGO_RAMO_REASEGURO,
    RK.REK_CODE AS GRUPO_REASEGURO,
    RK.REK_DESCRIPTION AS NOMBRE_GRUPO_REASEGURO,
    COALESCE(CB.COBPRIMAINPUT, '0') AS PRIMA,
    CB.COBPRIMAREASEGUROINPUT AS PRIMA_REASEGURO
FROM acsele_acsele_alfa_COVERAGEDCO CDCO
JOIN acsele_acsele_alfa_COBERTURA CB ON CB.PK = CDCO.DCOID
LEFT JOIN acsele_acsele_alfa_POLICYDCO PDCO ON PDCO.OPERATIONPK = CDCO.OPERATIONPK
LEFT JOIN acsele_acsele_alfa_AGREGATEDPOLICY AP ON AP.AGREGATEDPOLICYID = PDCO.AGREGATEDOBJECTID
LEFT JOIN acsele_acsele_alfa_PRODUCT PD ON PD.PRODUCTID = AP.PRODUCTID
LEFT JOIN acsele_acsele_alfa_PREPRODUCT PP ON PP.PK = PD.PRO_DCOID
LEFT JOIN acsele_acsele_alfa_PREPOLICY PPRE ON PPRE.PK = PDCO.DCOID
LEFT JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.CODE = REPLACE(CB.COBRAMOREASEGUROINPUT, '.0', '')
LEFT JOIN acsele_acsele_alfa_STRI_REINSURANCEKIND RK ON RK.REKIND_ID = RG.REKIND_ID
'''

df_ramo_final = spark.sql(sql_ramo_final)
df_ramo_final.createOrReplaceTempView("TMP_RAMO")
#df_ramo_final.show(5)
print("✅ Campo RAMO reestructurado sin subconsultas y con los nombres finales de salida.")

#--------------------------JOIN OK DF 1 Y 2 -----------------------------------------------------------------------------------------------
#DF1 PK = POLICYDCO.DCOID DF2 PK = POLICYDCO.OPERATIONPK  RENAME DCOID

df_test_join = spark.sql(
    """
    select p.DCOID, p.FECHA_ENDOSO, p.CURRENCY_ID, p.POLIZA, p.PRODUCTO, p.SUCURSAL, 
    p.POLICYTYPE_NAME, p.POLICYTYPE_CODE, p.FRECUENCIA_PAGO, p.FECHA_INICIO_POLIZA_MATRIZ, 
    p.FECHA_FIN_POLIZA_MATRIZ, r.COMPANIA, r.CODIGO_COMPANIA, r.NOMBRE_PRODUCTO, r.CODIGO_PRODUCTO, 
    r.ACUERDO, r.COBERTURA, r.DESC_COBERTURA, r.ACUMULA, r.RAMO_REASEGURO, r.RAMO, r.DESC_RAMO, 
    r.CODIGO_RAMO_REASEGURO, r.GRUPO_REASEGURO, r.NOMBRE_GRUPO_REASEGURO, r.PRIMA, r.PRIMA_REASEGURO
    from TMP_PRODUCTO p
    INNER JOIN TMP_RAMO r
    on p.DCOID = r.DCOID
    """)
#df_test_join.show(5)
print("✅ JOIN OK DF 1 Y 2")


frame_save = DynamicFrame.fromDF(df_test_join, glueContext, "frame_save_dynamic")


s3_out_path = "s3://alfa-datalake-dev-gold/GOLD/PCR_TEST_GOLD/"

glueContext.write_dynamic_frame.from_options(
            frame=frame_save, 
            connection_type="s3", 
            format="glueparquet", 
            connection_options={"path": s3_out_path, "partitionKeys": []}, 
            format_options={"compression": "snappy"}, 
            transformation_ctx="AmazonS3")  
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import date_format, col, split, explode, trim, when, expr, lit
# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


spark.sql("USE `datalake-dev-silver-catalog-db`")

syp_df = spark.sql("""
    SELECT SYP_VALUE
    FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
    WHERE SYP_PROPERTY = 'RRC.issuedPremium.earthquakeDocType'
""")

syp_exploded = syp_df.select(explode(split(col("SYP_VALUE"), ",")).alias("DESCRIPTION"))
syp_trimmed = syp_exploded.select(trim(col("DESCRIPTION")).alias("DESCRIPTION"))
syp_trimmed.createOrReplaceTempView("ISSUED_PREMIUM_DOCS")

bondholder_df = spark.sql("""
    SELECT SYP_VALUE
    FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
    WHERE SYP_PROPERTY = 'bondholder.rol'
""")
bondholder_exploded = bondholder_df.select(explode(split(col("SYP_VALUE"), ",")).alias("ROL_DESCRIPTION"))
bondholder_trimmed = bondholder_exploded.select(trim(col("ROL_DESCRIPTION")).alias("ROL_DESCRIPTION"))
bondholder_trimmed.createOrReplaceTempView("BONDHOLDER_ROLES")



sysprop_df_recaudo = spark.sql("""
    SELECT SYP_VALUE
    FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
    WHERE SYP_PROPERTY = 'RRC.commisionIntermediary'
""")

exploded_df_recaudo = sysprop_df_recaudo.select(explode(split(col("SYP_VALUE"), ",")).alias("DESCRIPTION"))
trimmed_df_recaudo = exploded_df_recaudo.select(trim(col("DESCRIPTION")).alias("DESCRIPTION"))
trimmed_df_recaudo.createOrReplaceTempView("COMM_INTERMEDIARY_DOCS_RECAUDO")


sysprop_df = spark.sql("""
    SELECT SYP_VALUE
    FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
    WHERE SYP_PROPERTY = 'RRC.commisionIntermediary'
""")

exploded_df = sysprop_df.select(explode(split(col("SYP_VALUE"), ",")).alias("DESCRIPTION"))
trimmed_df = exploded_df.select(trim(col("DESCRIPTION")).alias("DESCRIPTION"))
trimmed_df.createOrReplaceTempView("COMM_INTERMEDIARY_DOCS")


df_sysprop = spark.sql("SELECT * FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY") \
    .filter(col("SYP_PROPERTY").isin(
        "RRC.issuedPremium.earthquakeDocType",
        "RRC.commisionIntermediary",
        "RRC.commissionCollection.earthquakeDocType"
    ))

df_sysprop = spark.sql("SELECT * FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY") \
    .filter(col("SYP_PROPERTY").isin(
        "RRC.issuedPremium.earthquakeDocType",
        "RRC.commisionIntermediary",
        "RRC.commissionCollection.earthquakeDocType"
    ))

df_desc = df_sysprop.withColumn("DESCRIPTION", explode(split(col("SYP_VALUE"), ","))) \
    .withColumn("DESCRIPTION", trim(col("DESCRIPTION"))) \
    .withColumn("TIPO",
        when(col("SYP_PROPERTY") == "RRC.issuedPremium.earthquakeDocType", "ISSUED_PREMIUM")
       .when(col("SYP_PROPERTY") == "RRC.commisionIntermediary", "INTERMEDIARY")
       .when(col("SYP_PROPERTY") == "RRC.commissionCollection.earthquakeDocType", "COLLECTION")
    )

df_desc.createOrReplaceTempView("DOC_TYPES")

# -----------------------------
# Consulta con JOINs seguros para bruto
# -----------------------------
sql_bruto = """
SELECT
  OI.OPERATIONPK AS DCOID,
  SUM(CASE 
    WHEN DTY.TIPO = 'ISSUED_PREMIUM' AND DT.DTY_ISDEBIT = 1 THEN OI.AMOUNT
    WHEN DTY.TIPO = 'ISSUED_PREMIUM' AND DT.DTY_ISDEBIT = 0 THEN OI.AMOUNT * -1
    ELSE 0 END
  ) AS ISSUEDPREMIUM,
  SUM(CASE 
    WHEN DTY.TIPO = 'INTERMEDIARY' AND PP.SIMBOLO = 'Comision' THEN DET.VALUE
    ELSE 0 END
  ) AS COMMISION_INTERMEDIARY,
  SUM(CASE 
    WHEN DTY.TIPO = 'COLLECTION' AND PP.SIMBOLO = 'ComisionRecaudo' THEN OI.AMOUNT
    ELSE 0 END
  ) AS COMMISSIONCOLLECTION
FROM acsele_acsele_alfa_OPENITEM OI
JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON OI.DTY_ID = DT.DTY_ID
LEFT JOIN acsele_acsele_alfa_UAADETAIL DET ON DET.OPENITEMID = OI.OPENITEMID
LEFT JOIN acsele_acsele_alfa_PROPERTY PP ON PP.PROPERTYID = DET.PROPERTY
JOIN DOC_TYPES DTY ON DT.DTY_DESCRIPTION = DTY.DESCRIPTION
WHERE OI.OPM_SUBSTATUS <> 'notApplied' AND OI.operationpk >= 986536769
GROUP BY OI.OPERATIONPK
"""

df_bruto = spark.sql(sql_bruto)

# Derivados bruto
df_bruto = df_bruto \
    .withColumn("PERCENTAGEPARTICALFA", lit(100)) \
    .withColumn("ALFAPREMIUM", expr("ISSUEDPREMIUM * (PERCENTAGEPARTICALFA / 100)")) \
    .withColumn("PORCPART_ALFA", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
    .withColumn("PRIMACED_COASEGURO", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
    .withColumn("GROUP_PERCENTAGE", lit(100)) \
    .withColumn("BROKERAGECOMMISSION", expr("COMMISION_INTERMEDIARY * (GROUP_PERCENTAGE / 100)")) \
    .withColumn("PORCCOMRECAUDO", expr("(COMMISSIONCOLLECTION * 100) / ISSUEDPREMIUM")) \
    .withColumn("TIPO_REGISTRO", lit("BRUTO"))

# -----------------------------
# Consulta detalle reasegurador
# -----------------------------
sql_detalle = """
SELECT
  RUD.OPERATIONPK AS DCOID,
  RO.TOTALPREMIUM AS ISSUEDPREMIUM,
  ABS(SUM(ROG.PREMIUMAMOUNT) * 100 / RO.TOTALPREMIUM) AS GROUP_PERCENTAGE,
  0 AS PERCENTAGEPARTICALFA,
  'DETALLE' AS TIPO_REGISTRO
FROM acsele_acsele_alfa_REINSURANCEOPERATION RO
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO ROG ON ROG.OPERATIONID = RO.OPERATIONID
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
INNER JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
INNER JOIN acsele_acsele_alfa_RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
WHERE RO.TOTALPREMIUM > 0 AND ROG.operationid >= 943602721
GROUP BY RUD.OPERATIONPK, RO.TOTALPREMIUM
"""

df_detalle = spark.sql(sql_detalle) \
    .withColumn("ALFAPREMIUM", expr("ISSUEDPREMIUM * (PERCENTAGEPARTICALFA / 100)")) \
    .withColumn("PORCPART_ALFA", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
    .withColumn("PRIMACED_COASEGURO", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
    .withColumn("COMMISION_INTERMEDIARY", lit(None).cast("double")) \
    .withColumn("COMMISSIONCOLLECTION", lit(None).cast("double")) \
    .withColumn("BROKERAGECOMMISSION", lit(None).cast("double")) \
    .withColumn("PORCCOMRECAUDO", lit(None).cast("double"))

# -----------------------------
# Unificación final
# -----------------------------
df_total = df_bruto.select(df_detalle.columns).unionByName(df_detalle)
df_total.createOrReplaceTempView("TMP_IVA_COMISION_RECAUDO")



sql_producto = """
WITH
cte_poliza_base AS (
    SELECT DISTINCT
        DCO.OPERATIONPK AS DCOID,
        CO.TIME_STAMP AS FECHA_ENDOSO,
        PP.NUMEROPOLIZAINPUT AS POLIZA,
        AP.PRODUCTID AS PRODUCTO,
        PP.OFICINAINPUT AS SUCURSAL,
        PP.FRECUENCIAPAGOINPUT AS FRECUENCIA_PAGO,
        DCO.INITIALDATE AS FECHA_INICIO_POLIZA_MATRIZ,
        DCO.FINISHDATE AS FECHA_FIN_POLIZA_MATRIZ
    FROM acsele_acsele_alfa_POLICYDCO DCO
    JOIN acsele_acsele_alfa_CONTEXTOPERATION CO ON CO.ID = DCO.OPERATIONPK
    JOIN acsele_acsele_alfa_PREPOLICY PP ON PP.PK = DCO.DCOID
    JOIN acsele_acsele_alfa_FINANCIALPLANDCO FPDCO ON FPDCO.AGREGATEDPARENTID = DCO.AGREGATEDOBJECTID AND FPDCO.OPERATIONPK = DCO.OPERATIONPK
    JOIN acsele_acsele_alfa_AGREGATEDPOLICY AP ON AP.AGREGATEDPOLICYID = DCO.AGREGATEDOBJECTID
    WHERE CO.id >= 986530136 AND CO.status = 2
),
cte_poliza_matriz AS (
    SELECT
        DISTINCT dco.OPERATIONPK AS DCOID,
        RAP.rsa_policyid,
        AGS.AST_NUMEROACU AS POLIZA_MATRIZ,
        RAP.rsa_ast_id,
        AV.ast_id,
        AV.dcoid AS ID,
        PREA.UNIDADNEGOCIOINPUT AS UNIDAD_NEGOCIO
    FROM acsele_acsele_alfa_POLICYDCO DCO  
    INNER JOIN acsele_acsele_alfa_STPS_RELATIONAGREEMENTPOLICY RAP ON RAP.RSA_POLICYID = DCO.AGREGATEDOBJECTID
    INNER JOIN acsele_acsele_alfa_STPS_AGREEMENTSTATICDAT AGS ON RAP.RSA_AST_ID = AGS.AST_ID
    INNER JOIN acsele_acsele_alfa_STAG_AGREEMENTVERSION AV ON AV.AST_ID = RAP.RSA_AST_ID 
    INNER JOIN acsele_acsele_alfa_PREAGREEMENT PREA ON PREA.PK = AV.DCOID
    WHERE RAP.rsa_policyid >= 183066469 AND AV.ast_id >= 73001
),
cte_unidad_riesgo AS (
    SELECT
        DCO.OPERATIONPK AS DCOID,
        PRU.pk,
        PRU.NUMUNIDRIESGOINPUT AS RISKUNIT_NUMBER,
        DCO.INITIALDATE AS RISKUNIT_BEGINDATE,
        DCO.FINISHDATE AS RISKUNIT_ENDDATE
    FROM acsele_acsele_alfa_PRERISKUNITS PRU
    INNER JOIN acsele_acsele_alfa_RISKUNITDCO DCO ON DCO.DCOID = PRU.PK
    WHERE PRU.pk >= 61652035099
),
cte_tomadores AS (
  SELECT
        DISTINCT PPD.OPERATIONPK AS DCOID,
        PTP.DOCUMENTOIDENTIDADINPUT AS ID_TOMADOR_POLIZA,
        PTP.NOMBREPREDETERMINADOINPUT AS TOMADOR_POLIZA
    FROM acsele_acsele_alfa_STPO_POLICYPARTICIPATION PP
    INNER JOIN acsele_acsele_alfa_STPO_POLICYPARTICIPATIONDCO PPD ON PP.AGREGATEDOBJECTID = PPD.AGREGATEDOBJECTID 
    INNER JOIN acsele_acsele_alfa_STTE_THIRDPARTY TP ON TP.TPT_ID = PP.THIRDPARTYID
    INNER JOIN acsele_acsele_alfa_PRETHIRDPARTY PTP ON PTP.PK = TP.IDDCO
    INNER JOIN acsele_acsele_alfa_STTS_ROLE RL ON RL.ROL_ID = PP.ROL_ID
    WHERE RL.ROL_DESCRIPTION IN (SELECT ROL_DESCRIPTION FROM BONDHOLDER_ROLES) AND PP.agregatedobjectid >= 474064566
),
cte_prima_emitida AS (
    SELECT
        OI.OPERATIONPK AS DCOID,
        SUM(CASE WHEN DT.DTY_ISDEBIT = 1 THEN CAST(OI.AMOUNT AS DECIMAL) ELSE (CAST(OI.AMOUNT AS DECIMAL)*-1) END) AS PRIMA_EMITIDA
    FROM acsele_acsele_alfa_OPENITEM OI
    JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
    WHERE OI.OPM_SUBSTATUS <> 'notApplied'
      AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM ISSUED_PREMIUM_DOCS)
    GROUP BY OI.OPERATIONPK
),
cte_comision_intermediacion AS (
    SELECT
        ppd.operationpk AS DCOID,
        pp.agregatedobjectid,  
        PI.PORCENTAJECOMISIONINPUT AS PORC_COM_INTERMEDIACION
    FROM acsele_acsele_alfa_STPO_POLICYPARTICIPATION PP
    INNER JOIN acsele_acsele_alfa_STPO_POLICYPARTICIPATIONDCO PPD ON PP.AGREGATEDOBJECTID = PPD.AGREGATEDOBJECTID
    INNER JOIN acsele_acsele_alfa_PARTICIPACIONINTERMEDIARIOS PI ON PI.PK = PPD.DCOID
    WHERE pp.agregatedobjectid >= 474064566
    and ppd.operationpk >= 986530136
   
),
cte_nombre_reaseg AS (
    SELECT
        DISTINCT (RUD.DCOID) AS DCOID,
        RO.operationid, 
        ROD.THIRDPARTYID AS THIRDPARTY_ID, 
        ROD.THIRDPARTYNAME AS NOMREASEG,
        ROD.PARTICIPATIONID AS PARTICIPATION_ID, 
        SUM(ROD.ROD_PRORATEDPREMIUM) AS DETAIL_PREMIUM
    FROM acsele_acsele_alfa_REINSURANCEOPERATION RO
    INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO ROG ON ROG.OPERATIONID = RO.OPERATIONID
    INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
    INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONDETAIL ROD ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
    INNER JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
    INNER JOIN acsele_acsele_alfa_RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
    WHERE ROC.CONTRACTTYPE = 'MandatoryFacultativeContract' AND ro.operationid >= 943602721 AND roc.reinsurancegroupinfoid >= 674470981
    GROUP BY ROD.THIRDPARTYID, ROD.THIRDPARTYNAME, ROD.PARTICIPATIONID, RUD.OPERATIONPK, RUD.DCOID, RO.operationid
   
),
cte_iva_comision_recaudo AS (
    SELECT
        OI.OPERATIONPK AS DCOID,
        SUM(CAST(DET.VALUE AS DECIMAL)) AS IVA_COMISION_RECAUDO
    FROM acsele_acsele_alfa_OPENITEM OI
    INNER JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
    INNER JOIN acsele_acsele_alfa_UAADETAIL DET ON DET.OPENITEMID = OI.OPENITEMID
    INNER JOIN acsele_acsele_alfa_PROPERTY PP ON PP.PROPERTYID = CAST(DET.PROPERTY as DECIMAL)
    WHERE OI.OPM_SUBSTATUS <> 'notApplied'
    AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM COMM_INTERMEDIARY_DOCS_RECAUDO)
    AND PP.SIMBOLO = 'IvaComision'
    GROUP BY OI.OPERATIONPK
),
cte_prima_reaseguro AS (
    SELECT
        RUD.OPERATIONPK AS DCOID,
        RO.TOTALPREMIUM AS ISSUEDPREMIUM,
        ABS(SUM(ROG.PREMIUMAMOUNT) * 100 / RO.TOTALPREMIUM) AS GROUP_PERCENTAGE,
        0 AS PERCENTAGEPARTICALFA,
        'DETALLE' AS TIPO_REGISTRO
    FROM acsele_acsele_alfa_REINSURANCEOPERATION RO
    INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO ROG ON ROG.OPERATIONID = RO.OPERATIONID
    INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
    INNER JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
    INNER JOIN acsele_acsele_alfa_RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
    WHERE RO.TOTALPREMIUM > 0 AND ROG.operationid >= 943602721
    GROUP BY RUD.OPERATIONPK, RO.TOTALPREMIUM
),
cte_reaseguro AS (
    WITH detail_premium AS (
        SELECT
            ROD.THIRDPARTYID,
            ROD.THIRDPARTYNAME,
            ROD.PARTICIPATIONID,
            SUM(ROD.ROD_PRORATEDPREMIUM) AS DETAIL_PREMIUM
        FROM acsele_acsele_alfa_reinsuranceoperation RO
        INNER JOIN acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG ON ROG.OPERATIONID = RO.OPERATIONID
        INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
        INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail ROD ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
        INNER JOIN acsele_acsele_alfa_ri_reinsurancegroup RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
        GROUP BY ROD.THIRDPARTYID, ROD.THIRDPARTYNAME, ROD.PARTICIPATIONID
    ),
    reinsurance_commission AS (
        SELECT
            ROD.THIRDPARTYID,
            ROD.THIRDPARTYNAME,
            ROD.PARTICIPATIONID,
            ROD.REINSURANCEOPERATIONDETAILID,
            SUM(PT.ROPP_VALUE) AS REINSURANCE_COMMISSION
        FROM acsele_acsele_alfa_reinsuranceoperationproperty PT
        INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail ROD
            ON PT.ROD_ID = ROD.REINSURANCEOPERATIONDETAILID
        WHERE PT.ROPP_NAME IN ('ComPriCedida', 'ComPriCedidaInd')
        GROUP BY ROD.REINSURANCEOPERATIONDETAILID, ROD.THIRDPARTYID, ROD.THIRDPARTYNAME, ROD.PARTICIPATIONID
    )
    SELECT
        DISTINCT RUD.OPERATIONPK AS DCOID,
        RA.CLASEREASEGUROINPUT AS CLASE_REASEGURO,
        RA.CODIGOREASEGURADORINPUT AS CODIGO_REASEGURADOR,
        ROD.THIRDPARTYID,
        ROD.THIRDPARTYNAME,
        ROD.PARTICIPATIONID,
        IF(RA.CLASEREASEGUROINPUT = 'Interior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDAUT_INT,
        IF(RA.CLASEREASEGUROINPUT = 'Exterior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDAUT_EXT,
        IF(RA.CLASEREASEGUROINPUT = 'Interior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDFAC_INT,
        IF(RA.CLASEREASEGUROINPUT = 'Exterior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDFAC_EXT,
        IF(RA.CLASEREASEGUROINPUT = 'Interior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGAUT_INT,
        IF(RA.CLASEREASEGUROINPUT = 'Exterior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGAUT_EXT,
        IF(RA.CLASEREASEGUROINPUT = 'Interior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGFAC_INT,
        IF(RA.CLASEREASEGUROINPUT = 'Exterior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGFAC_EXT
        FROM acsele_acsele_alfa_reinsuranceoperationdetail ROD
    RIGHT JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC ON ROC.REINOPERCOMPONENTID = ROD.REINOPERCOMPONENTID
    JOIN acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG ON ROG.REINSURANCEGROUPINFOID = ROC.REINSURANCEGROUPINFOID
    JOIN acsele_acsele_alfa_reinsuranceoperation RO ON RO.OPERATIONID = ROG.OPERATIONID
    JOIN acsele_acsele_alfa_riskunitdco RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
    JOIN acsele_acsele_alfa_thirdpartyrole TPR ON TPR.THIRDPARTYID = ROD.THIRDPARTYID
    JOIN acsele_acsele_alfa_reasegurador RA ON RA.PK = TPR.IDDCOROLE
    LEFT JOIN detail_premium dp
    ON dp.THIRDPARTYID = ROD.THIRDPARTYID
    AND dp.PARTICIPATIONID = ROD.PARTICIPATIONID
    LEFT JOIN reinsurance_commission rc
    ON rc.THIRDPARTYID = ROD.THIRDPARTYID
    AND rc.PARTICIPATIONID = ROD.PARTICIPATIONID
    and rc.REINSURANCEOPERATIONDETAILID = ROD.REINSURANCEOPERATIONDETAILID
),
cte_acuerdo AS (
    SELECT DISTINCT
        CDCO.OPERATIONPK AS DCOID,
        PP.COMPANIAINPUT AS COMPANIA,
        REPLACE(PP.COMPANIAVALUE, '.0', '') AS CODIGO_COMPANIA,
        PP.DESCRIPCIONPRODUCTOINPUT AS NOMBRE_PRODUCTO,
        PP.CODIGOPRODUCTOINPUT AS CODIGO_PRODUCTO,
        PPRE.NUMEROPOLIZAINPUT AS NUMERO_POLIZA,
        PPRE.POLIZAMAESTRAINPUT AS ACUERDO,
        REPLACE(CB.COBCODIGOCOBERTURAINPUT, '.0', '') AS COBERTURA,
        CB.COBDESCRIPCIONCOBERTINPUT AS DESC_COBERTURA,
        CASE WHEN CB.COBACUMULACIONINPUT IN ('A', 'P') THEN 'SI' ELSE 'NO' END AS ACUMULA,
        REPLACE(CB.COBRAMOREASEGUROINPUT, '.0', '') AS RAMO_REASEGURO,
        REPLACE(CB.COBRAMOCONTABLEINPUT, '.0', '') AS RAMO,
        PP.RAMOINPUT AS DESC_RAMO,
        RG.DESCRIPTION AS CODIGO_RAMO_REASEGURO,
        RK.REK_CODE AS GRUPO_REASEGURO,
        RK.REK_DESCRIPTION AS NOMBRE_GRUPO_REASEGURO,
        COALESCE(CB.COBPRIMAINPUT, '0') AS PRIMA,
        CB.COBPRIMAREASEGUROINPUT AS PRIMA_REASEGURO
    FROM acsele_acsele_alfa_COVERAGEDCO CDCO
    JOIN acsele_acsele_alfa_COBERTURA CB ON CB.PK = CDCO.DCOID
    LEFT JOIN acsele_acsele_alfa_POLICYDCO PDCO ON PDCO.OPERATIONPK = CDCO.OPERATIONPK
    JOIN acsele_acsele_alfa_CONTEXTOPERATION CO ON CO.ID = PDCO.OPERATIONPK
    LEFT JOIN acsele_acsele_alfa_AGREGATEDPOLICY AP ON AP.AGREGATEDPOLICYID = PDCO.AGREGATEDOBJECTID
    LEFT JOIN acsele_acsele_alfa_PRODUCT PD ON PD.PRODUCTID = AP.PRODUCTID
    LEFT JOIN acsele_acsele_alfa_PREPRODUCT PP ON PP.PK = PD.PRO_DCOID
    LEFT JOIN acsele_acsele_alfa_PREPOLICY PPRE ON PPRE.PK = PDCO.DCOID
    LEFT JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.CODE = REPLACE(CB.COBRAMOREASEGUROINPUT, '.0', '')
    LEFT JOIN acsele_acsele_alfa_STRI_REINSURANCEKIND RK ON RK.REKIND_ID = RG.REKIND_ID
    WHERE CO.id >= 986530136 and CO.status = 2
),
cte_primcedaut AS (
    WITH premium_detail AS (
        SELECT
            ROG2.OPERATIONID,
            ROC2.CONTRACTTYPE,
            SUM(RD.ROD_PRORATEDPREMIUM) AS SUM_PRIMA
        FROM acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG2
        INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC2
            ON ROC2.REINSURANCEGROUPINFOID = ROG2.REINSURANCEGROUPINFOID
        INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail RD
            ON RD.REINOPERCOMPONENTID = ROC2.REINOPERCOMPONENTID
        GROUP BY ROG2.OPERATIONID, ROC2.CONTRACTTYPE
),
reinsurance_commission AS (
  SELECT
    ROG2.OPERATIONID,
    ROC2.CONTRACTTYPE,
    SUM(PT.ROPP_VALUE) AS SUM_COMISION
  FROM acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG2
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC2
    ON ROC2.REINSURANCEGROUPINFOID = ROG2.REINSURANCEGROUPINFOID
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail RD
    ON RD.REINOPERCOMPONENTID = ROC2.REINOPERCOMPONENTID
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationproperty PT
    ON PT.ROD_ID = RD.REINSURANCEOPERATIONDETAILID
  WHERE PT.ROPP_NAME IN ('ComPriCedida', 'ComPriCedidaInd')
  GROUP BY ROG2.OPERATIONID, ROC2.CONTRACTTYPE
)
    SELECT
        DISTINCT PLO.DCOID,
        PLO.OPERATIONPK,
        RO.OPERATIONID,
        ROC.REINSURANCEGROUPINFOID,
        ROD.REINOPERCOMPONENTID,
        RG.REINSURANCEGROUPID,
        IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0,dp_aut.SUM_PRIMA) AS PRIMACEDAUT,
        IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),dp_fac.SUM_PRIMA, 0) AS PRIMACEDFAC,
        IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0, rc_aut.SUM_COMISION) AS COMISIONREASEGAUT,
        IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),rc_fac.SUM_COMISION, 0) AS COMISIONREASEGFAC
    FROM acsele_acsele_alfa_reinsuranceoperation RO
    INNER JOIN acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG
    ON ROG.OPERATIONID = RO.OPERATIONID
    INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC
    ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
    INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail ROD
    ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
    INNER JOIN acsele_acsele_alfa_ri_reinsurancegroup RG
    ON RG.REINSURANCEGROUPID = ROG.GROUPID
    INNER JOIN acsele_acsele_alfa_policydco PLO
    ON RO.cor_id = PLO.operationpk
    LEFT JOIN premium_detail dp_aut
    ON dp_aut.OPERATIONID = RO.OPERATIONID AND dp_aut.CONTRACTTYPE NOT IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
    LEFT JOIN premium_detail dp_fac
    ON dp_fac.OPERATIONID = RO.OPERATIONID AND dp_fac.CONTRACTTYPE IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
    LEFT JOIN reinsurance_commission rc_aut
    ON rc_aut.OPERATIONID = RO.OPERATIONID AND rc_aut.CONTRACTTYPE NOT IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
    LEFT JOIN reinsurance_commission rc_fac
    ON rc_fac.OPERATIONID = RO.OPERATIONID AND rc_fac.CONTRACTTYPE IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
   
),
cte_iva_comision_intermediacion AS (
    SELECT
        OI.OPERATIONPK AS DCOID,
        SUM(CAST(DET.VALUE AS DECIMAL)) AS IVA_COMISION_INTERMEDIACION
    FROM acsele_acsele_alfa_OPENITEM OI
    JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
    JOIN acsele_acsele_alfa_UAADETAIL DET ON DET.OPENITEMID = OI.OPENITEMID
    JOIN acsele_acsele_alfa_PROPERTY PP ON PP.PROPERTYID = CAST(DET.PROPERTY AS BIGINT)
    WHERE OI.OPM_SUBSTATUS <> 'notApplied'
      AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM COMM_INTERMEDIARY_DOCS)
      AND PP.SIMBOLO = 'IvaComision'
    GROUP BY OI.OPERATIONPK
)
 SELECT
    'Acsele' AS ORIGEN,
    ac.COMPANIA,
    ac.RAMO,
    ac.DESC_RAMO,
    ac.CODIGO_PRODUCTO AS PRODUCTO,
    ac.NOMBRE_PRODUCTO AS DESC_PRODUCTO,
    ac.RAMO AS RAMO_TECNICO,
    CASE ac.RAMO
        WHEN '1' THEN 'AUTOS'
        WHEN '3' THEN 'EQUIPO ELECTRONICO'
        WHEN '4' THEN 'CUMPLIMIENTO'
        WHEN '5' THEN 'INCENDIO'
        WHEN '6' THEN 'LUCRO'
        WHEN '7' THEN 'MANEJO'
        WHEN '8' THEN 'ROTURA MAQUINARIA'
        WHEN '9' THEN 'CASCO'
        WHEN '10' THEN 'RESPONSABILIDAD'
        WHEN '12' THEN 'SUSTRACCION'
        WHEN '13' THEN 'TERREMOTO'
        WHEN '14' THEN 'TODO RIESGO'
        WHEN '15' THEN 'TRANSPORTE'
        WHEN '16' THEN 'VIDRIOS'
        WHEN '18' THEN 'ACCIDENTES PERSONALES'
        WHEN '20' THEN 'VIDA GRUPO NO CONTRIBUTIVO'
        WHEN '22' THEN 'DEUDORES'
        WHEN '24' THEN 'DESEMPLEO'
        WHEN '25' THEN 'HOGAR'
        WHEN '30' THEN 'EXEQUIAS'
        WHEN '31' THEN 'PREVISIONALES'
        WHEN '34' THEN 'SALUD'
        WHEN '40' THEN 'VIDA INDIVIDUAL'
        WHEN '44' THEN 'JUDICIAL'
        WHEN '50' THEN 'RIESGOS PROFESIONALES'
        WHEN '51' THEN 'RENTAS VITALICIAS'
        WHEN '52' THEN 'RENTAS TASA VARIABLE'
        WHEN '70' THEN 'VIDA GRUPO CONTRIBUTIVO'
        WHEN '95' THEN 'MONTAJE'
        WHEN '96' THEN 'EQUIPO Y MAQUINARIA'
  END AS DESC_RAMO_TECNICO,
    pm.POLIZA_MATRIZ,
    pb.POLIZA,
    pb.SUCURSAL,
    '' AS MODULO,
    ur.RISKUNIT_NUMBER AS UNIDAD_RIESGO,
    ac.COBERTURA,
    ac.DESC_COBERTURA,
    ac.DCOID AS ENDOSO,
    pb.FECHA_ENDOSO,
    pb.FECHA_ENDOSO AS MESPRO,
    pm.UNIDAD_NEGOCIO,
    pb.FECHA_INICIO_POLIZA_MATRIZ,
    pb.FECHA_FIN_POLIZA_MATRIZ,
    ur.RISKUNIT_BEGINDATE AS FECHA_INICIO_COBERTURA,
    ur.RISKUNIT_ENDDATE AS FECHA_FIN_COBERTURA,
    t.ID_TOMADOR_POLIZA AS ID_TOMADOR_POLIZA_MATRIZ,
    t.TOMADOR_POLIZA AS TOMADOR_POLIZA_MATRIZ,
    t.ID_TOMADOR_POLIZA AS ID_TOMADOR_POLIZA,
    t.TOMADOR_POLIZA,
    'D' AS TIPO_COASEGURO,
    0 as PORCPART_ALFA,
    pb.FRECUENCIA_PAGO,
    pe.PRIMA_EMITIDA,
    pre.ISSUEDPREMIUM - (pre.ISSUEDPREMIUM * (pre.PERCENTAGEPARTICALFA/100)) AS PRIMACED_COASEGURO,
    (pre.ISSUEDPREMIUM * (pre.PERCENTAGEPARTICALFA/100)) AS PRIMAALFA,
    ci.PORC_COM_INTERMEDIACION,
    coi.COMMISION_INTERMEDIARY AS COMISION_INTERMEDIACION,
    iva.IVA_COMISION_INTERMEDIACION,
    coi.PORCCOMRECAUDO AS PORCCOMRECAUDO,
    coi.COMMISSIONCOLLECTION AS COMISION_RECAUDO,
    ivr.IVA_COMISION_RECAUDO AS IVA_COMISION_RECAUDO,
    rea.CODIGO_REASEGURADOR AS CODREASEG,
    nomre.NOMREASEG AS NOMREASEG,
    rea.CLASE_REASEGURO AS CLASE_REASEGURADOR,
    prc.PRIMACEDAUT AS PRIMACEDAUT,
    rea.PRIMACEDAUT_INT AS PRIMACEDAUT_INT,
    rea.PRIMACEDAUT_EXT AS PRIMACEDAUT_EXT,
    prc.PRIMACEDFAC AS PRIMACEDFAC,
    rea.PRIMACEDFAC_INT AS PRIMACEDFAC_INT,
    rea.PRIMACEDFAC_EXT AS PRIMACEDFAC_EXT,
    prc.COMISIONREASEGAUT AS COMISIONREASEGAUT,
    rea.COMISIONREASEGAUT_INT AS COMISIONREASEGAUT_INT,
    rea.COMISIONREASEGAUT_EXT AS COMISIONREASEGAUT_EXT,
    prc.COMISIONREASEGFAC AS COMISIONREASEGFAC,
    rea.COMISIONREASEGFAC_INT AS COMISIONREASEGFAC_INT,
    rea.COMISIONREASEGFAC_EXT AS COMISIONREASEGFAC_EXT,
    'NULL' AS GASTOS_EXP_PORCENTAJE,
    'NULL' AS GASTOS_EXP
FROM cte_poliza_base pb
LEFT JOIN cte_poliza_matriz pm ON pm.DCOID = pb.DCOID
LEFT JOIN cte_unidad_riesgo ur ON ur.DCOID = pb.DCOID
LEFT JOIN cte_tomadores t ON t.DCOID = pb.DCOID
LEFT JOIN cte_prima_emitida pe ON pe.DCOID = pb.DCOID
LEFT JOIN cte_comision_intermediacion ci ON ci.DCOID = pb.DCOID
LEFT JOIN cte_iva_comision_intermediacion iva ON iva.DCOID = pb.DCOID
LEFT JOIN cte_acuerdo ac ON ac.DCOID = pb.DCOID
LEFT JOIN cte_prima_reaseguro pre ON pre.DCOID = pb.DCOID
LEFT JOIN TMP_IVA_COMISION_RECAUDO coi ON coi.DCOID = pb.DCOID
LEFT JOIN cte_iva_comision_recaudo ivr ON ivr.DCOID = pb.DCOID
LEFT JOIN cte_reaseguro rea ON rea.DCOID = pb.DCOID
LEFT JOIN cte_nombre_reaseg nomre ON nomre.DCOID = pb.DCOID
LEFT JOIN cte_primcedaut prc ON prc.DCOID = pb.DCOID
WHERE pb.DCOID = 986530143
GROUP BY
    pb.DCOID,
    ac.COMPANIA,
    ac.RAMO,
    ac.DESC_RAMO,
    ac.CODIGO_PRODUCTO,
    ac.NOMBRE_PRODUCTO,
    pm.POLIZA_MATRIZ,
    pb.POLIZA,
    pb.SUCURSAL,
    ur.RISKUNIT_NUMBER,
    ac.COBERTURA,
    ac.DESC_COBERTURA,
    ac.DCOID,
    pb.FECHA_ENDOSO,
    pb.FECHA_INICIO_POLIZA_MATRIZ,
    pb.FECHA_FIN_POLIZA_MATRIZ,
    ur.RISKUNIT_BEGINDATE,
    ur.RISKUNIT_ENDDATE,
    ac.ACUERDO,
    pb.FRECUENCIA_PAGO,
    pre.ISSUEDPREMIUM,
    pre.PERCENTAGEPARTICALFA,
    pre.GROUP_PERCENTAGE,
    ci.PORC_COM_INTERMEDIACION,
    rea.CODIGO_REASEGURADOR,
    nomre.NOMREASEG,
    rea.CLASE_REASEGURO,
    prc.PRIMACEDAUT,
    rea.PRIMACEDAUT_INT,
    rea.PRIMACEDAUT_EXT,
    prc.PRIMACEDFAC,
    rea.PRIMACEDFAC_INT,
    rea.PRIMACEDFAC_EXT,
    prc.COMISIONREASEGAUT,
    rea.COMISIONREASEGAUT_INT,
    rea.COMISIONREASEGAUT_EXT,
    prc.COMISIONREASEGFAC,
    rea.COMISIONREASEGFAC_INT,
    rea.COMISIONREASEGFAC_EXT,
    pm.UNIDAD_NEGOCIO,
    pe.PRIMA_EMITIDA,
    coi.COMMISION_INTERMEDIARY,
    iva.IVA_COMISION_INTERMEDIACION,
    coi.PORCCOMRECAUDO,
    coi.COMMISSIONCOLLECTION,
    ivr.IVA_COMISION_RECAUDO,
    t.ID_TOMADOR_POLIZA,
    t.TOMADOR_POLIZA
"""
df_producto = spark.sql(sql_producto)
#df_producto.createOrReplaceTempView("TMP_PRODUCTO")
#df_producto.show(5)
from pyspark.sql.functions import col, count, when, lit
from pyspark.sql import DataFrame
df_producto.write.mode("overwrite").parquet("s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL/NOTEBOOK/")



import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, coalesce, sum as spark_sum, abs as spark_abs, 
    split, explode, trim, regexp_replace, date_format, 
    broadcast, desc, asc, row_number, rank, dense_rank,
    upper, lower, concat, concat_ws, substring, length,
    isnan, isnull, count, max as spark_max, min as spark_min,
    avg, stddev, variance, first, last, collect_list, collect_set
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DateType, TimestampType, BooleanType
)
from pyspark.sql.window import Window

def create_spark_session():
    """Crear y configurar sesión de Spark para AWS Glue"""
    spark = SparkSession.builder \
        .appName("PCR_TRADICIONAL_ORO") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.sql.shuffle.partitions", "800") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.default.parallelism", "200") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.broadcastTimeout", "3600") \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.storage.blockManagerSlaveTimeoutMs", "300000") \
        .config("spark.sql.autoBroadcastJoinThreshold", "100485760") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()
    
    # Configurar checkpoint directory usando S3 en lugar de /tmp
    try:
        spark.sparkContext.setCheckpointDir("s3://aws-glue-assets-123456789012-us-east-1/checkpoints/")
    except:
        # Si no se puede usar S3, usar directorio local
        import os
        checkpoint_dir = "/tmp/spark_checkpoints"
        os.makedirs(checkpoint_dir, exist_ok=True)
        spark.sparkContext.setCheckpointDir(checkpoint_dir)
    
    return spark

def get_or_create_spark():
    """Obtener sesión existente o crear una nueva con reintentos"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            spark = SparkSession.getActiveSession()
            if spark is not None:
                return spark
            
            print("No hay sesión activa, creando nueva sesión...")
            return create_spark_session()
            
        except Exception as e:
            retry_count += 1
            if retry_count == max_retries:
                raise Exception(f"No se pudo crear sesión de Spark después de {max_retries} intentos: {str(e)}")
            print(f"Error al crear sesión (intento {retry_count}): {str(e)}")
            time.sleep(5)  # Esperar antes de reintentar

def load_base_tables(spark):
    """Cargar todas las tablas base necesarias"""
    print("\nCargando tablas base...")
    print("="*50)
    
    tables = {}
    total_registros = 0
    
    # Tablas principales
    print("\nIniciando carga de tablas:")
    table_names = [
        "acsele_acsele_alfa_STST_SYSTEMPROPERTY",
        "acsele_acsele_alfa_POLICYDCO", 
        "acsele_acsele_alfa_CONTEXTOPERATION",
        "acsele_acsele_alfa_PREPOLICY",
        "acsele_acsele_alfa_FINANCIALPLANDCO",
        "acsele_acsele_alfa_AGREGATEDPOLICY",
        "acsele_acsele_alfa_COVERAGEDCO",
        "acsele_acsele_alfa_COBERTURA",
        "acsele_acsele_alfa_PRODUCT",
        "acsele_acsele_alfa_PREPRODUCT",
        "acsele_acsele_alfa_RI_REINSURANCEGROUP",
        "acsele_acsele_alfa_STRI_REINSURANCEKIND",
        "acsele_acsele_alfa_STPS_RELATIONAGREEMENTPOLICY",
        "acsele_acsele_alfa_STPS_AGREEMENTSTATICDAT",
        "acsele_acsele_alfa_STAG_AGREEMENTVERSION",
        "acsele_acsele_alfa_PREAGREEMENT",
        "acsele_acsele_alfa_PRERISKUNITS",
        "acsele_acsele_alfa_RISKUNITDCO",
        "acsele_acsele_alfa_STPO_POLICYPARTICIPATION",
        "acsele_acsele_alfa_STPO_POLICYPARTICIPATIONDCO",
        "acsele_acsele_alfa_STTE_THIRDPARTY",
        "acsele_acsele_alfa_PRETHIRDPARTY",
        "acsele_acsele_alfa_STTS_ROLE",
        "acsele_acsele_alfa_OPENITEM",
        "acsele_acsele_alfa_STCA_DOCTYPE",
        "acsele_acsele_alfa_PARTICIPACIONINTERMEDIARIOS",
        "acsele_acsele_alfa_UAADETAIL",
        "acsele_acsele_alfa_PROPERTY",
        "acsele_acsele_alfa_REINSURANCEOPERATION",
        "acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO",
        "acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT",
        "acsele_acsele_alfa_REINSURANCEOPERATIONDETAIL",
        "acsele_acsele_alfa_reinsuranceoperationproperty"
    ]
    
    for i, table_name in enumerate(table_names, 1):
        try:
            print(f"[{i}/{len(table_names)}] Cargando {table_name}...")
            table_df = spark.table(table_name)
            count = table_df.count()
            tables[table_name] = table_df
            total_registros += count
            print(f"✓ {table_name}: {count:,} registros")
        except Exception as e:
            print(f"✗ ERROR - {table_name}: {str(e)}")
            
    print(f"\n{'='*50}")
    print(f"RESUMEN CARGA DE TABLAS:")
    print(f"✓ Tablas cargadas exitosamente: {len(tables)}")
    print(f"✓ Total de registros: {total_registros:,}")
    print(f"{'='*50}")
    return tables

def process_system_properties(tables):
    """Procesar propiedades del sistema"""
    print("\nProcesando propiedades del sistema...")
    print("="*50)
    
    try:
        print("1. Cargando tabla SYSTEMPROPERTY...")
        syp_df = tables["acsele_acsele_alfa_STST_SYSTEMPROPERTY"]
        total_props = syp_df.count()
        print(f"✓ Total propiedades disponibles: {total_props:,}")
        
        # Filtrar propiedades específicas
        target_properties = [
            'RRC.issuedPremium.earthquakeDocType',
            'RRC.commisionIntermediary', 
            'bondholder.rol',
            'RRC.commissionCollection.earthquakeDocType'
        ]
        print("\n2. Filtrando propiedades requeridas...")
        print(f"Buscando: {', '.join(target_properties)}")
        
        filtered_syp = syp_df.filter(col("SYP_PROPERTY").isin(target_properties))
        found_props = filtered_syp.select("SYP_PROPERTY").distinct().collect()
        found_props = [row.SYP_PROPERTY for row in found_props]
        
        print("\nPropiedades encontradas:")
        for prop in target_properties:
            if prop in found_props:
                print(f"✓ {prop}")
            else:
                print(f"✗ {prop} - NO ENCONTRADA")
        
        # Procesar cada tipo de propiedad
        print("\n3. Procesando valores de propiedades...")
        def process_property(property_name, alias="DESCRIPTION"):
            df = filtered_syp.filter(
                col("SYP_PROPERTY") == property_name
            ).select(
                explode(split(col("SYP_VALUE"), ",")).alias(alias)
            ).select(
                trim(col(alias)).alias(alias)
            )
            count = df.count()
            print(f"✓ {property_name}: {count} valores")
            return df
        
        result = {
            'issued_premium_docs': process_property('RRC.issuedPremium.earthquakeDocType'),
            'comm_intermediary_docs': process_property('RRC.commisionIntermediary'),
            'bondholder_roles': process_property('bondholder.rol', "ROL_DESCRIPTION"),
            'comm_collection_docs': process_property('RRC.commissionCollection.earthquakeDocType')
        }
        
        print("\n" + "="*50)
        print("✓ PROPIEDADES DEL SISTEMA PROCESADAS")
        print("="*50)
        return result
        
    except Exception as e:
        print(f"\n✗ ERROR en process_system_properties:")
        print(f"Detalle: {str(e)}")
        print("\nStack trace:")
        import traceback
        traceback.print_exc()
        raise

def process_policy_base(tables):
    """Procesar información base de pólizas"""
    print("\nProcesando información base de pólizas...")
    print("="*50)
    
    try:
        print("1. Cargando tablas necesarias...")
        dco_df = tables["acsele_acsele_alfa_POLICYDCO"]
        co_df = tables["acsele_acsele_alfa_CONTEXTOPERATION"]
        pp_df = tables["acsele_acsele_alfa_PREPOLICY"]
        fpdco_df = tables["acsele_acsele_alfa_FINANCIALPLANDCO"]
        ap_df = tables["acsele_acsele_alfa_AGREGATEDPOLICY"]
        print("✓ Tablas cargadas")
        
        # Filtrar operaciones válidas
        print("\n2. Filtrando operaciones válidas...")
        print("Filtros: id >= 986530136 AND status = 2")
        co_filtered = co_df.filter((col("id") >= 986530136) & (col("status") == 2))
        filtered_count = co_filtered.count()
        print(f"✓ Operaciones filtradas: {filtered_count:,} registros")
        
        if filtered_count == 0:
            print("⚠ ADVERTENCIA: No se encontraron operaciones válidas")
            return None
        
        # Joins para obtener información base de pólizas
        print("\n3. Realizando joins...")
        print("DCO -> CO -> PP -> FPDCO -> AP")
        
        policy_base = dco_df.alias("DCO") \
            .join(co_filtered.alias("CO"), col("CO.ID") == col("DCO.OPERATIONPK")) \
            .join(pp_df.alias("PP"), col("PP.PK") == col("DCO.DCOID")) \
            .join(fpdco_df.alias("FPDCO"), 
                  (col("FPDCO.AGREGATEDPARENTID") == col("DCO.AGREGATEDOBJECTID")) & 
                  (col("FPDCO.OPERATIONPK") == col("DCO.OPERATIONPK"))) \
            .join(ap_df.alias("AP"), col("AP.AGREGATEDPOLICYID") == col("DCO.AGREGATEDOBJECTID")) \
            .select(
                col("DCO.OPERATIONPK").alias("DCOID"),
                col("CO.TIME_STAMP").alias("FECHA_ENDOSO"),
                col("PP.NUMEROPOLIZAINPUT").alias("POLIZA"),
                col("AP.PRODUCTID").alias("PRODUCTO"),
                col("PP.OFICINAINPUT").alias("SUCURSAL"),
                col("PP.FRECUENCIAPAGOINPUT").alias("FRECUENCIA_PAGO"),
                col("DCO.INITIALDATE").alias("FECHA_INICIO_POLIZA_MATRIZ"),
                col("DCO.FINISHDATE").alias("FECHA_FIN_POLIZA_MATRIZ")
            ).distinct()
        
        print("✓ Joins completados")
        
        print("\n4. Contando registros finales...")
        count = policy_base.count()
        
        print("\n" + "="*50)
        print(f"✓ INFORMACIÓN BASE DE PÓLIZAS PROCESADA")
        print(f"✓ Registros resultantes: {count:,}")
        print("="*50)
        
        return policy_base
        
    except Exception as e:
        print(f"\n✗ ERROR en process_policy_base:")
        print(f"Detalle: {str(e)}")
        print("\nStack trace:")
        import traceback
        traceback.print_exc()
        raise

def process_policy_matrix(tables):
    """Procesar información de matriz de pólizas"""
    print("\nProcesando matriz de pólizas...")
    print("="*50)
    
    try:
        print("1. Cargando tablas necesarias...")
        dco_df = tables["acsele_acsele_alfa_POLICYDCO"]
        rap_df = tables["acsele_acsele_alfa_STPS_RELATIONAGREEMENTPOLICY"]
        ags_df = tables["acsele_acsele_alfa_STPS_AGREEMENTSTATICDAT"]
        av_df = tables["acsele_acsele_alfa_STAG_AGREEMENTVERSION"]
        prea_df = tables["acsele_acsele_alfa_PREAGREEMENT"]
        print("✓ Tablas cargadas")
        
        # Filtros
        print("\n2. Aplicando filtros...")
        print("- RAP: rsa_policyid >= 183066469")
        rap_filtered = rap_df.filter(col("rsa_policyid") >= 183066469)
        rap_count = rap_filtered.count()
        print(f"✓ RAP filtrado: {rap_count:,} registros")
        
        print("- AV: ast_id >= 73001")
        av_filtered = av_df.filter(col("ast_id") >= 73001)
        av_count = av_filtered.count()
        print(f"✓ AV filtrado: {av_count:,} registros")
        
        if rap_count == 0 or av_count == 0:
            print("\n⚠ ADVERTENCIA: Uno o más filtros no retornaron registros")
            return None
        
        # Joins
        print("\n3. Realizando joins...")
        print("DCO -> RAP -> AGS -> AV -> PREA")
        
        policy_matrix = dco_df.alias("DCO") \
            .join(rap_filtered.alias("RAP"), col("RAP.RSA_POLICYID") == col("DCO.AGREGATEDOBJECTID")) \
            .join(ags_df.alias("AGS"), col("RAP.RSA_AST_ID") == col("AGS.AST_ID")) \
            .join(av_filtered.alias("AV"), col("AV.AST_ID") == col("RAP.RSA_AST_ID")) \
            .join(prea_df.alias("PREA"), col("PREA.PK") == col("AV.DCOID")) \
            .select(
                col("DCO.OPERATIONPK").alias("DCOID"),
                col("RAP.rsa_policyid"),
                col("AGS.AST_NUMEROACU").alias("POLIZA_MATRIZ"),
                col("RAP.rsa_ast_id"),
                col("AV.ast_id"),
                col("AV.dcoid").alias("ID"),
                col("PREA.UNIDADNEGOCIOINPUT").alias("UNIDAD_NEGOCIO")
            ).distinct()
        
        print("✓ Joins completados")
        
        print("\n4. Contando registros finales...")
        count = policy_matrix.count()
        
        print("\n" + "="*50)
        print(f"✓ MATRIZ DE PÓLIZAS PROCESADA")
        print(f"✓ Registros resultantes: {count:,}")
        print("="*50)
        
        return policy_matrix
        
    except Exception as e:
        print(f"\n✗ ERROR en process_policy_matrix:")
        print(f"Detalle: {str(e)}")
        print("\nStack trace:")
        import traceback
        traceback.print_exc()
        raise

def process_risk_units(tables):
    """Procesar unidades de riesgo"""
    print("\nProcesando unidades de riesgo...")
    print("="*50)
    
    try:
        print("1. Cargando tablas necesarias...")
        pru_df = tables["acsele_acsele_alfa_PRERISKUNITS"]
        dco_df = tables["acsele_acsele_alfa_RISKUNITDCO"]
        print("✓ Tablas cargadas")
        
        # Filtro
        print("\n2. Aplicando filtros...")
        print("- PRU: pk >= 61652035099")
        pru_filtered = pru_df.filter(col("pk") >= 61652035099)
        filtered_count = pru_filtered.count()
        print(f"✓ PRU filtrado: {filtered_count:,} registros")
        
        if filtered_count == 0:
            print("\n⚠ ADVERTENCIA: El filtro no retornó registros")
            return None
        
        # Joins
        print("\n3. Realizando joins...")
        print("PRU -> DCO")
        
        risk_units = pru_filtered.alias("PRU") \
            .join(dco_df.alias("DCO"), col("DCO.DCOID") == col("PRU.PK")) \
            .select(
                col("DCO.OPERATIONPK").alias("DCOID"),
                col("PRU.pk"),
                col("PRU.NUMUNIDRIESGOINPUT").alias("RISKUNIT_NUMBER"),
                col("DCO.INITIALDATE").alias("RISKUNIT_BEGINDATE"),
                col("DCO.FINISHDATE").alias("RISKUNIT_ENDDATE")
            )
        
        print("✓ Joins completados")
        
        print("\n4. Contando registros finales...")
        count = risk_units.count()
        
        print("\n" + "="*50)
        print(f"✓ UNIDADES DE RIESGO PROCESADAS")
        print(f"✓ Registros resultantes: {count:,}")
        print("="*50)
        
        return risk_units
        
    except Exception as e:
        print(f"\n✗ ERROR en process_risk_units:")
        print(f"Detalle: {str(e)}")
        print("\nStack trace:")
        import traceback
        traceback.print_exc()
        raise

def process_policy_holders(tables, system_props):
    """Procesar tomadores de pólizas"""
    print("\nProcesando tomadores de pólizas...")
    print("="*50)
    
    try:
        print("1. Cargando tablas necesarias...")
        pp_df = tables["acsele_acsele_alfa_STPO_POLICYPARTICIPATION"]
        ppd_df = tables["acsele_acsele_alfa_STPO_POLICYPARTICIPATIONDCO"]
        tp_df = tables["acsele_acsele_alfa_STTE_THIRDPARTY"]
        ptp_df = tables["acsele_acsele_alfa_PRETHIRDPARTY"]
        rl_df = tables["acsele_acsele_alfa_STTS_ROLE"]
        print("✓ Tablas cargadas")
        
        print("\n2. Aplicando filtros...")
        print("- PP: agregatedobjectid >= 474064566")
        pp_filtered = pp_df.filter(col("agregatedobjectid") >= 474064566)
        filtered_count = pp_filtered.count()
        print(f"✓ PP filtrado: {filtered_count:,} registros")
        
        if filtered_count == 0:
            print("\n⚠ ADVERTENCIA: El filtro no retornó registros")
            return None
        
        print("\n3. Procesando roles válidos...")
        bondholder_roles_list = [row.ROL_DESCRIPTION for row in system_props['bondholder_roles'].collect()]
        print(f"Roles encontrados: {', '.join(bondholder_roles_list)}")
        
        rl_filtered = rl_df.filter(col("ROL_DESCRIPTION").isin(bondholder_roles_list))
        roles_count = rl_filtered.count()
        print(f"✓ Roles filtrados: {roles_count:,} registros")
        
        if roles_count == 0:
            print("\n⚠ ADVERTENCIA: No se encontraron roles válidos")
            return None
        
        print("\n4. Realizando joins...")
        print("PP -> PPD -> TP -> PTP -> RL")
        
        policy_holders = pp_filtered.alias("PP") \
            .join(ppd_df.alias("PPD"), col("PP.AGREGATEDOBJECTID") == col("PPD.AGREGATEDOBJECTID")) \
            .join(tp_df.alias("TP"), col("TP.TPT_ID") == col("PP.THIRDPARTYID")) \
            .join(ptp_df.alias("PTP"), col("PTP.PK") == col("TP.IDDCO")) \
            .join(rl_filtered.alias("RL"), col("RL.ROL_ID") == col("PP.ROL_ID")) \
            .select(
                col("PPD.OPERATIONPK").alias("DCOID"),
                col("PTP.DOCUMENTOIDENTIDADINPUT").alias("ID_TOMADOR_POLIZA"),
                col("PTP.NOMBREPREDETERMINADOINPUT").alias("TOMADOR_POLIZA")
            ).distinct()
        
        print("✓ Joins completados")
        
        print("\n5. Contando registros finales...")
        count = policy_holders.count()
        
        print("\n" + "="*50)
        print(f"✓ TOMADORES DE PÓLIZAS PROCESADOS")
        print(f"✓ Registros resultantes: {count:,}")
        print("="*50)
        
        return policy_holders
        
    except Exception as e:
        print(f"\n✗ ERROR en process_policy_holders:")
        print(f"Detalle: {str(e)}")
        print("\nStack trace:")
        import traceback
        traceback.print_exc()
        raise

def process_issued_premium(tables, system_props):
    """Procesar prima emitida"""
    print("\nProcesando prima emitida...")
    print("="*50)
    
    try:
        print("1. Cargando tablas necesarias...")
        oi_df = tables["acsele_acsele_alfa_OPENITEM"]
        dt_df = tables["acsele_acsele_alfa_STCA_DOCTYPE"]
        print("✓ Tablas cargadas")
        
        print("\n2. Procesando documentos válidos...")
        issued_docs_list = [row.DESCRIPTION for row in system_props['issued_premium_docs'].collect()]
        print(f"Documentos encontrados: {', '.join(issued_docs_list)}")
        
        dt_filtered = dt_df.filter(col("DTY_DESCRIPTION").isin(issued_docs_list))
        docs_count = dt_filtered.count()
        print(f"✓ Documentos filtrados: {docs_count:,} registros")
        
        if docs_count == 0:
            print("\n⚠ ADVERTENCIA: No se encontraron documentos válidos")
            return None
        
        print("\n3. Filtrando items abiertos...")
        print("Filtro: OPM_SUBSTATUS != 'notApplied'")
        oi_filtered = oi_df.filter(col("OPM_SUBSTATUS") != 'notApplied')
        items_count = oi_filtered.count()
        print(f"✓ Items filtrados: {items_count:,} registros")
        
        if items_count == 0:
            print("\n⚠ ADVERTENCIA: No se encontraron items abiertos válidos")
            return None
        
        print("\n4. Realizando joins y cálculos...")
        print("OI -> DT")
        
        issued_premium = oi_filtered.alias("OI") \
            .join(dt_filtered.alias("DT"), col("DT.DTY_ID") == col("OI.DTY_ID")) \
            .select(
                col("OI.OPERATIONPK").alias("DCOID"),
                when(col("DT.DTY_ISDEBIT") == 1, col("OI.AMOUNT").cast(DecimalType(18,2)))
                .otherwise(col("OI.AMOUNT").cast(DecimalType(18,2)) * -1).alias("AMOUNT")
            ) \
            .groupBy("DCOID") \
            .agg(spark_sum("AMOUNT").alias("PRIMA_EMITIDA"))
        
        print("✓ Joins y cálculos completados")
        
        print("\n5. Contando registros finales...")
        count = issued_premium.count()
        
        print("\n" + "="*50)
        print(f"✓ PRIMA EMITIDA PROCESADA")
        print(f"✓ Registros resultantes: {count:,}")
        print("="*50)
        
        return issued_premium
        
    except Exception as e:
        print(f"\n✗ ERROR en process_issued_premium:")
        print(f"Detalle: {str(e)}")
        print("\nStack trace:")
        import traceback
        traceback.print_exc()
        raise

def process_intermediation_commission(tables):
    """Procesar comisión de intermediación"""
    print("\nProcesando comisión de intermediación...")
    print("="*50)
    
    try:
        print("1. Cargando tablas necesarias...")
        pp_df = tables["acsele_acsele_alfa_STPO_POLICYPARTICIPATION"]
        ppd_df = tables["acsele_acsele_alfa_STPO_POLICYPARTICIPATIONDCO"]
        pi_df = tables["acsele_acsele_alfa_PARTICIPACIONINTERMEDIARIOS"]
        print("✓ Tablas cargadas")
        
        print("\n2. Aplicando filtros...")
        print("- PP: agregatedobjectid >= 474064566")
        print("- PPD: operationpk >= 986530136")
        pp_filtered = pp_df.filter(col("agregatedobjectid") >= 474064566)
        ppd_filtered = ppd_df.filter(col("operationpk") >= 986530136)
        pp_count = pp_filtered.count()
        ppd_count = ppd_filtered.count()
        print(f"✓ PP filtrado: {pp_count:,} registros")
        print(f"✓ PPD filtrado: {ppd_count:,} registros")
        
        if pp_count == 0 or ppd_count == 0:
            print("\n⚠ ADVERTENCIA: Uno o más filtros no retornaron registros")
            return None
        
        print("\n3. Realizando joins...")
        print("PP -> PPD -> PI")
        
        intermediation_commission = pp_filtered.alias("PP") \
            .join(ppd_filtered.alias("PPD"), col("PP.AGREGATEDOBJECTID") == col("PPD.AGREGATEDOBJECTID")) \
            .join(pi_df.alias("PI"), col("PI.PK") == col("PPD.DCOID")) \
            .select(
                col("PPD.operationpk").alias("DCOID"),
                col("PP.agregatedobjectid"),
                col("PI.PORCENTAJECOMISIONINPUT").alias("PORC_COM_INTERMEDIACION")
            )
        
        print("✓ Joins completados")
        
        print("\n4. Contando registros finales...")
        count = intermediation_commission.count()
        
        print("\n" + "="*50)
        print(f"✓ COMISIÓN DE INTERMEDIACIÓN PROCESADA")
        print(f"✓ Registros resultantes: {count:,}")
        print("="*50)
        
        return intermediation_commission
        
    except Exception as e:
        print(f"\n✗ ERROR en process_intermediation_commission:")
        print(f"Detalle: {str(e)}")
        print("\nStack trace:")
        import traceback
        traceback.print_exc()
        raise

def process_commission_vat(tables, system_props):
    """Procesar IVA de comisión de intermediación y recaudo"""
    print("\nProcesando IVA de comisiones...")
    print("="*50)
    
    try:
        print("1. Cargando tablas necesarias...")
        oi_df = tables["acsele_acsele_alfa_OPENITEM"]
        dt_df = tables["acsele_acsele_alfa_STCA_DOCTYPE"]
        det_df = tables["acsele_acsele_alfa_UAADETAIL"]
        pp_df = tables["acsele_acsele_alfa_PROPERTY"]
        print("✓ Tablas cargadas")
        
        print("\n2. Aplicando filtros base...")
        print("- OI: OPM_SUBSTATUS != 'notApplied'")
        print("- PP: SIMBOLO = 'IvaComision'")
        oi_filtered = oi_df.filter(col("OPM_SUBSTATUS") != 'notApplied')
        pp_filtered = pp_df.filter(col("SIMBOLO") == 'IvaComision')
        oi_count = oi_filtered.count()
        pp_count = pp_filtered.count()
        print(f"✓ OI filtrado: {oi_count:,} registros")
        print(f"✓ PP filtrado: {pp_count:,} registros")
        
        if oi_count == 0 or pp_count == 0:
            print("\n⚠ ADVERTENCIA: Uno o más filtros base no retornaron registros")
            return None
        
        def process_commission_type(doc_type_list, column_name, type_name):
            print(f"\n3.{type_name[0]}. Procesando {type_name}...")
            print(f"Documentos: {', '.join(doc_type_list)}")
            
            dt_filtered = dt_df.filter(col("DTY_DESCRIPTION").isin(doc_type_list))
            dt_count = dt_filtered.count()
            print(f"✓ Documentos filtrados: {dt_count:,} registros")
            
            if dt_count == 0:
                print(f"⚠ ADVERTENCIA: No se encontraron documentos para {type_name}")
                return None
            
            result = oi_filtered.alias("OI") \
                .join(dt_filtered.alias("DT"), col("DT.DTY_ID") == col("OI.DTY_ID")) \
                .join(det_df.alias("DET"), col("DET.OPENITEMID") == col("OI.OPENITEMID")) \
                .join(pp_filtered.alias("PP"), col("PP.PROPERTYID") == col("DET.PROPERTY").cast("bigint")) \
                .select(
                    col("OI.OPERATIONPK").alias("DCOID"),
                    col("DET.VALUE").cast(DecimalType(18,2)).alias("VALUE")
                ) \
                .groupBy("DCOID") \
                .agg(spark_sum("VALUE").alias(column_name))
            
            result_count = result.count()
            print(f"✓ {type_name} procesado: {result_count:,} registros")
            return result
        
        # Procesar IVA de comisión de intermediación
        comm_docs_list = [row.DESCRIPTION for row in system_props['comm_intermediary_docs'].collect()]
        intermediation_vat = process_commission_type(comm_docs_list, "IVA_COMISION_INTERMEDIACION", "IVA comisión intermediación")
        
        # Procesar IVA de comisión de recaudo
        collection_docs_list = [row.DESCRIPTION for row in system_props['comm_collection_docs'].collect()]
        collection_vat = process_commission_type(collection_docs_list, "IVA_COMISION_RECAUDO", "IVA comisión recaudo")
        
        print("\n4. Uniendo resultados...")
        if intermediation_vat is not None and collection_vat is not None:
            commission_vat = intermediation_vat.join(
                collection_vat,
                ["DCOID"],
                "outer"
            ).select(
                col("DCOID"),
                coalesce(col("IVA_COMISION_INTERMEDIACION"), lit(0)).alias("IVA_COMISION_INTERMEDIACION"),
                coalesce(col("IVA_COMISION_RECAUDO"), lit(0)).alias("IVA_COMISION_RECAUDO")
            )
        elif intermediation_vat is not None:
            commission_vat = intermediation_vat.select(
                col("DCOID"),
                col("IVA_COMISION_INTERMEDIACION"),
                lit(0).alias("IVA_COMISION_RECAUDO")
            )
        elif collection_vat is not None:
            commission_vat = collection_vat.select(
                col("DCOID"),
                lit(0).alias("IVA_COMISION_INTERMEDIACION"),
                col("IVA_COMISION_RECAUDO")
            )
        else:
            print("\n⚠ ADVERTENCIA: No se procesaron datos de IVA")
            return None
        
        print("✓ Resultados unidos")
        
        print("\n5. Contando registros finales...")
        count = commission_vat.count()
        
        print("\n" + "="*50)
        print(f"✓ IVA DE COMISIONES PROCESADO")
        print(f"✓ Registros resultantes: {count:,}")
        print("="*50)
        
        return commission_vat
        
    except Exception as e:
        print(f"\n✗ ERROR en process_commission_vat:")
        print(f"Detalle: {str(e)}")
        print("\nStack trace:")
        import traceback
        traceback.print_exc()
        raise

def process_reinsurance(tables):
    """Procesar información de reaseguro"""
    print("\nProcesando información de reaseguro...")
    print("="*50)
    
    try:
        print("1. Cargando tablas necesarias...")
        ro_df = tables["acsele_acsele_alfa_REINSURANCEOPERATION"]
        rog_df = tables["acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO"]
        rud_df = tables["acsele_acsele_alfa_RISKUNITDCO"]
        roc_df = tables["acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT"]
        rod_df = tables["acsele_acsele_alfa_REINSURANCEOPERATIONDETAIL"]
        rop_df = tables["acsele_acsele_alfa_reinsuranceoperationproperty"]
        print("✓ Tablas cargadas")
        
        print("\n2. Aplicando filtros...")
        print("- RO: TOTALPREMIUM > 0")
        print("- ROG: operationid >= 943602721")
        ro_filtered = ro_df.filter(col("TOTALPREMIUM") > 0)
        rog_filtered = rog_df.filter(col("operationid") >= 943602721)
        ro_count = ro_filtered.count()
        rog_count = rog_filtered.count()
        print(f"✓ RO filtrado: {ro_count:,} registros")
        print(f"✓ ROG filtrado: {rog_count:,} registros")
        
        if ro_count == 0 or rog_count == 0:
            print("\n⚠ ADVERTENCIA: Uno o más filtros no retornaron registros")
            return None
            
        print("\n3. Procesando datos...")
        
        # Paso 1: Join base entre RO, ROG y RUD
        print("3.1 Creando join base...")
        base_join = ro_filtered.alias("RO") \
            .join(broadcast(rog_filtered).alias("ROG"), 
                  col("ROG.OPERATIONID") == col("RO.OPERATIONID")) \
            .join(rud_df.alias("RUD"), 
                  col("RUD.AGREGATEDOBJECTID") == col("RO.AGREGATEDRISKUNITID")) \
            .select(
                col("RUD.OPERATIONPK").alias("DCOID"),
                col("RO.OPERATIONID"),
                col("ROG.REINSURANCEGROUPINFOID"),
                col("ROG.GROUPID").alias("REINSURANCEGROUPID")
            ).distinct()
        
        # Checkpoint para persistir resultados intermedios
        print("Persistiendo resultados intermedios...")
        try:
            base_join = base_join.persist()  # Usar persist en lugar de checkpoint
            base_join.count()  # Forzar materialización
            print("✓ Resultados persistidos")
        except Exception as e:
            print(f"⚠ Advertencia: No se pudo persistir resultados intermedios: {str(e)}")
            print("Continuando sin persistencia...")
        
        # Paso 2: Procesar primas por tipo de contrato
        print("3.2 Procesando primas...")
        premium_calc = roc_df.alias("ROC") \
            .join(rod_df.alias("ROD"), 
                  col("ROD.REINOPERCOMPONENTID") == col("ROC.REINOPERCOMPONENTID")) \
            .groupBy("ROC.REINSURANCEGROUPINFOID", "ROC.CONTRACTTYPE") \
            .agg(
                spark_sum(when(
                    ~col("ROC.CONTRACTTYPE").isin("FacultativeReinsuranceContract", "MandatoryFacultativeContract"),
                    col("ROD.ROD_PRORATEDPREMIUM")
                ).otherwise(lit(0))).alias("PRIMACEDAUT"),
                spark_sum(when(
                    col("ROC.CONTRACTTYPE").isin("FacultativeReinsuranceContract", "MandatoryFacultativeContract"),
                    col("ROD.ROD_PRORATEDPREMIUM")
                ).otherwise(lit(0))).alias("PRIMACEDFAC")
            )
        
        # Paso 3: Procesar comisiones
        print("3.3 Procesando comisiones...")
        commission_calc = roc_df.alias("ROC") \
            .join(rod_df.alias("ROD"), 
                  col("ROD.REINOPERCOMPONENTID") == col("ROC.REINOPERCOMPONENTID")) \
            .join(rop_df.alias("PT"), 
                  col("PT.ROD_ID") == col("ROD.REINSURANCEOPERATIONDETAILID")) \
            .filter(col("PT.ROPP_NAME").isin("ComPriCedida", "ComPriCedidaInd")) \
            .groupBy("ROC.REINSURANCEGROUPINFOID", "ROC.CONTRACTTYPE") \
            .agg(
                spark_sum(when(
                    ~col("ROC.CONTRACTTYPE").isin("FacultativeReinsuranceContract", "MandatoryFacultativeContract"),
                    col("PT.ROPP_VALUE")
                ).otherwise(lit(0))).alias("COMISIONREASEGAUT"),
                spark_sum(when(
                    col("ROC.CONTRACTTYPE").isin("FacultativeReinsuranceContract", "MandatoryFacultativeContract"),
                    col("PT.ROPP_VALUE")
                ).otherwise(lit(0))).alias("COMISIONREASEGFAC")
            )
        
        # Paso 4: Join final
        print("3.4 Realizando join final...")
        reinsurance = base_join \
            .join(broadcast(premium_calc), "REINSURANCEGROUPINFOID", "left") \
            .join(broadcast(commission_calc), "REINSURANCEGROUPINFOID", "left") \
            .select(
                col("DCOID"),
                col("OPERATIONID"),
                col("REINSURANCEGROUPINFOID"),
                col("REINSURANCEGROUPID"),
                coalesce(col("PRIMACEDAUT"), lit(0)).alias("PRIMACEDAUT"),
                coalesce(col("PRIMACEDFAC"), lit(0)).alias("PRIMACEDFAC"),
                coalesce(col("COMISIONREASEGAUT"), lit(0)).alias("COMISIONREASEGAUT"),
                coalesce(col("COMISIONREASEGFAC"), lit(0)).alias("COMISIONREASEGFAC")
            )
        
        print("✓ Procesamiento completado")
        
        print("\n4. Contando registros finales...")
        count = reinsurance.count()
        
        print("\n" + "="*50)
        print(f"✓ INFORMACIÓN DE REASEGURO PROCESADA")
        print(f"✓ Registros resultantes: {count:,}")
        print("="*50)
        
        return reinsurance
        
    except Exception as e:
        print(f"\n✗ ERROR en process_reinsurance:")
        print(f"Detalle: {str(e)}")
        print("\nStack trace:")
        import traceback
        traceback.print_exc()
        raise

def process_agreement_data(tables):
    """Procesar datos de acuerdo"""
    print("\nProcesando datos de acuerdo...")
    print("="*50)
    
    try:
        print("1. Cargando tablas necesarias...")
        cdco_df = tables["acsele_acsele_alfa_COVERAGEDCO"]
        cb_df = tables["acsele_acsele_alfa_COBERTURA"]
        pdco_df = tables["acsele_acsele_alfa_POLICYDCO"]
        co_df = tables["acsele_acsele_alfa_CONTEXTOPERATION"]
        ap_df = tables["acsele_acsele_alfa_AGREGATEDPOLICY"]
        pd_df = tables["acsele_acsele_alfa_PRODUCT"]
        pp_df = tables["acsele_acsele_alfa_PREPRODUCT"]
        ppre_df = tables["acsele_acsele_alfa_PREPOLICY"]
        rg_df = tables["acsele_acsele_alfa_RI_REINSURANCEGROUP"]
        rk_df = tables["acsele_acsele_alfa_STRI_REINSURANCEKIND"]
        print("✓ Tablas cargadas")
        
        print("\n2. Aplicando filtros...")
        print("- CO: id >= 986530136 AND status = 2")
        co_filtered = co_df.filter((col("id") >= 986530136) & (col("status") == 2))
        co_count = co_filtered.count()
        print(f"✓ CO filtrado: {co_count:,} registros")
        
        if co_count == 0:
            print("\n⚠ ADVERTENCIA: No se encontraron operaciones válidas")
            return None
        
        print("\n3. Realizando joins...")
        print("CDCO -> CB -> PDCO -> CO -> AP -> PD -> PP -> PPRE -> RG -> RK")
        
        agreement_data = cdco_df.alias("CDCO") \
            .join(cb_df.alias("CB"), col("CB.PK") == col("CDCO.DCOID")) \
            .join(pdco_df.alias("PDCO"), col("PDCO.OPERATIONPK") == col("CDCO.OPERATIONPK")) \
            .join(co_filtered.alias("CO"), col("CO.ID") == col("PDCO.OPERATIONPK")) \
            .join(ap_df.alias("AP"), col("AP.AGREGATEDPOLICYID") == col("PDCO.AGREGATEDOBJECTID"), "left") \
            .join(pd_df.alias("PD"), col("PD.PRODUCTID") == col("AP.PRODUCTID"), "left") \
            .join(pp_df.alias("PP"), col("PP.PK") == col("PD.PRO_DCOID"), "left") \
            .join(ppre_df.alias("PPRE"), col("PPRE.PK") == col("PDCO.DCOID"), "left") \
            .join(rg_df.alias("RG"), 
                  col("RG.CODE") == regexp_replace(col("CB.COBRAMOREASEGUROINPUT"), "\\.0", ""), "left") \
            .join(rk_df.alias("RK"), col("RK.REKIND_ID") == col("RG.REKIND_ID"), "left") \
            .select(
                col("CDCO.OPERATIONPK").alias("DCOID"),
                col("PP.COMPANIAINPUT").alias("COMPANIA"),
                regexp_replace(col("PP.COMPANIAVALUE"), "\\.0", "").alias("CODIGO_COMPANIA"),
                col("PP.DESCRIPCIONPRODUCTOINPUT").alias("NOMBRE_PRODUCTO"),
                col("PP.CODIGOPRODUCTOINPUT").alias("CODIGO_PRODUCTO"),
                col("PPRE.NUMEROPOLIZAINPUT").alias("NUMERO_POLIZA"),
                col("PPRE.POLIZAMAESTRAINPUT").alias("ACUERDO"),
                regexp_replace(col("CB.COBCODIGOCOBERTURAINPUT"), "\\.0", "").alias("COBERTURA"),
                col("CB.COBDESCRIPCIONCOBERTINPUT").alias("DESC_COBERTURA"),
                when(col("CB.COBACUMULACIONINPUT").isin("A", "P"), "SI").otherwise("NO").alias("ACUMULA"),
                regexp_replace(col("CB.COBRAMOREASEGUROINPUT"), "\\.0", "").alias("RAMO_REASEGURO"),
                regexp_replace(col("CB.COBRAMOCONTABLEINPUT"), "\\.0", "").alias("RAMO"),
                col("PP.RAMOINPUT").alias("DESC_RAMO"),
                col("RG.DESCRIPTION").alias("CODIGO_RAMO_REASEGURO"),
                col("RK.REK_CODE").alias("GRUPO_REASEGURO"),
                col("RK.REK_DESCRIPTION").alias("NOMBRE_GRUPO_REASEGURO"),
                coalesce(col("CB.COBPRIMAINPUT"), lit("0")).alias("PRIMA"),
                col("CB.COBPRIMAREASEGUROINPUT").alias("PRIMA_REASEGURO")
            ).distinct()
        
        print("✓ Joins completados")
        
        print("\n4. Contando registros finales...")
        count = agreement_data.count()
        
        print("\n" + "="*50)
        print(f"✓ DATOS DE ACUERDO PROCESADOS")
        print(f"✓ Registros resultantes: {count:,}")
        print("="*50)
        
        return agreement_data
        
    except Exception as e:
        print(f"\n✗ ERROR en process_agreement_data:")
        print(f"Detalle: {str(e)}")
        print("\nStack trace:")
        import traceback
        traceback.print_exc()
        raise

def create_branch_mapping():
    """Crear mapeo de ramos"""
    return {
        '1': 'AUTOS',
        '3': 'EQUIPO ELECTRONICO',
        '4': 'CUMPLIMIENTO',
        '5': 'INCENDIO',
        '6': 'LUCRO',
        '7': 'MANEJO',
        '8': 'ROTURA MAQUINARIA',
        '9': 'CASCO',
        '10': 'RESPONSABILIDAD',
        '12': 'SUSTRACCION',
        '13': 'TERREMOTO',
        '14': 'TODO RIESGO',
        '15': 'TRANSPORTE',
        '16': 'VIDRIOS',
        '18': 'ACCIDENTES PERSONALES',
        '20': 'VIDA GRUPO NO CONTRIBUTIVO',
        '22': 'DEUDORES',
        '24': 'DESEMPLEO',
        '25': 'HOGAR',
        '30': 'EXEQUIAS',
        '31': 'PREVISIONALES',
        '34': 'SALUD',
        '40': 'VIDA INDIVIDUAL',
        '44': 'JUDICIAL',
        '50': 'RIESGOS PROFESIONALES',
        '51': 'RENTAS VITALICIAS',
        '52': 'RENTAS TASA VARIABLE',
        '70': 'VIDA GRUPO CONTRIBUTIVO',
        '95': 'MONTAJE',
        '96': 'EQUIPO Y MAQUINARIA'
    }

def create_final_result(spark, processed_data):
    """Crear resultado final combinando todos los datos procesados"""
    print("\nCreando resultado final...")
    print("="*50)
    
    try:
        print("1. Verificando DataFrames procesados...")
        # Extraer DataFrames procesados
        components = ['agreement_data', 'policy_base', 'policy_matrix', 'risk_units', 
                     'policy_holders', 'issued_premium', 'intermediation_commission', 
                     'commission_vat', 'reinsurance']
        
        for component in components:
            df = processed_data.get(component)
            if df is not None:
                count = df.count()
                print(f"✓ {component}: {count:,} registros")
            else:
                print(f"✗ {component}: DataFrame es None")
        
        agreement_data = processed_data['agreement_data']
        policy_base = processed_data['policy_base']
        policy_matrix = processed_data['policy_matrix']
        risk_units = processed_data['risk_units']
        policy_holders = processed_data['policy_holders']
        issued_premium = processed_data['issued_premium']
        intermediation_commission = processed_data['intermediation_commission']
        commission_vat = processed_data['commission_vat']
        reinsurance = processed_data['reinsurance']
        
        print("\n2. Creando mapeo de ramos...")
        branch_mapping = create_branch_mapping()
        print(f"✓ Mapeo creado con {len(branch_mapping)} ramos")
        
        # Crear expresión CASE para mapeo de ramos
        print("\n3. Construyendo expresión CASE para ramos...")
        branch_case_expr = when(col("ac.RAMO") == '1', 'AUTOS')
        for code, description in branch_mapping.items():
            if code != '1':
                branch_case_expr = branch_case_expr.when(col("ac.RAMO") == code, description)
        print("✓ Expresión CASE construida")
        
        print("\n4. Realizando joins finales...")
        print("Base: agreement_data")
        print("Joins: policy_base -> policy_matrix -> risk_units -> policy_holders -> issued_premium -> intermediation_commission -> commission_vat -> reinsurance")
        
        # Join principal con agreement_data como base
        print("Realizando joins principales...")
        result = agreement_data.alias("ac") \
            .join(policy_base.alias("pb"), col("pb.DCOID") == col("ac.DCOID"), "left") \
            .join(policy_matrix.alias("pm"), col("pm.DCOID") == col("pb.DCOID"), "left") \
            .join(risk_units.alias("ur"), col("ur.DCOID") == col("pb.DCOID"), "left") \
            .join(policy_holders.alias("t"), col("t.DCOID") == col("pb.DCOID"), "left") \
            .join(issued_premium.alias("pe"), col("pe.DCOID") == col("pb.DCOID"), "left") \
            .join(intermediation_commission.alias("ci"), col("ci.DCOID") == col("pb.DCOID"), "left") \
            .join(commission_vat.alias("iva"), col("iva.DCOID") == col("pb.DCOID"), "left") \
            .join(reinsurance.alias("pre"), col("pre.DCOID") == col("pb.DCOID"), "left")
        
        # Checkpoint después de los joins
        print("Persistiendo joins intermedios...")
        try:
            result = result.persist()  # Usar persist en lugar de checkpoint
            result.count()  # Forzar materialización
            print("✓ Joins persistidos")
        except Exception as e:
            print(f"⚠ Advertencia: No se pudo persistir joins intermedios: {str(e)}")
            print("Continuando sin persistencia...")
        
        # Aplicar selección final
        print("Aplicando selección final...")
        result = result.select(
            lit('Acsele').alias("ORIGEN"),
            col("ac.COMPANIA"),
            col("ac.RAMO"),
            col("ac.DESC_RAMO"),
            col("ac.CODIGO_PRODUCTO").alias("PRODUCTO"),
            col("ac.NOMBRE_PRODUCTO").alias("DESC_PRODUCTO"),
            col("ac.RAMO").alias("RAMO_TECNICO"),
            branch_case_expr.alias("DESC_RAMO_TECNICO"),
            col("pm.POLIZA_MATRIZ"),
            col("pb.POLIZA"),
            col("pb.SUCURSAL"),
            lit("").alias("MODULO"),
            col("ur.RISKUNIT_NUMBER").alias("UNIDAD_RIESGO"),
            col("ac.COBERTURA"),
            col("ac.DESC_COBERTURA"),
            col("ac.DCOID").alias("ENDOSO"),
            col("pb.FECHA_ENDOSO"),
            col("pb.FECHA_ENDOSO").alias("MESPRO"),
            col("pm.UNIDAD_NEGOCIO"),
            col("pb.FECHA_INICIO_POLIZA_MATRIZ"),
            col("pb.FECHA_FIN_POLIZA_MATRIZ"),
            col("ur.RISKUNIT_BEGINDATE").alias("FECHA_INICIO_COBERTURA"),
            col("ur.RISKUNIT_ENDDATE").alias("FECHA_FIN_COBERTURA"),
            col("t.ID_TOMADOR_POLIZA").alias("ID_TOMADOR_POLIZA_MATRIZ"),
            col("t.TOMADOR_POLIZA").alias("TOMADOR_POLIZA_MATRIZ"),
            col("t.ID_TOMADOR_POLIZA"),
            col("t.TOMADOR_POLIZA"),
            lit("D").alias("TIPO_COASEGURO"),
            lit(0).alias("PORCPART_ALFA"),
            col("pb.FRECUENCIA_PAGO"),
            coalesce(col("pe.PRIMA_EMITIDA"), lit(0)).alias("PRIMA_EMITIDA"),
            coalesce(col("pre.PRIMACEDAUT") + col("pre.PRIMACEDFAC"), lit(0)).alias("PRIMACED_COASEGURO"),
            coalesce(col("pre.COMISIONREASEGAUT") + col("pre.COMISIONREASEGFAC"), lit(0)).alias("PRIMAALFA"),
            coalesce(col("ci.PORC_COM_INTERMEDIACION"), lit(0)).alias("PORC_COM_INTERMEDIACION"),
            coalesce(col("iva.IVA_COMISION_INTERMEDIACION"), lit(0)).alias("IVA_COMISION_INTERMEDIACION"),
            coalesce(col("iva.IVA_COMISION_RECAUDO"), lit(0)).alias("IVA_COMISION_RECAUDO")
        )
        
        print("✓ Joins y selección completados")
        
        print("\n5. Contando registros finales...")
        final_count = result.count()
        
        print("\n" + "="*50)
        print(f"✓ RESULTADO FINAL CREADO")
        print(f"✓ Registros totales: {final_count:,}")
        print("="*50)
        
        return result
        
    except Exception as e:
        print(f"\n✗ ERROR en create_final_result:")
        print(f"Detalle: {str(e)}")
        print("\nStack trace:")
        import traceback
        traceback.print_exc()
        raise

# Variables globales para uso en notebook
spark = None
tables = None
system_props = None
processed_data = None
result_df = None

def execute_with_retry(func, *args, max_retries=2, **kwargs):
    """Ejecutar función con reintentos en caso de fallo de sesión"""
    global spark
    
    for attempt in range(max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_msg = str(e).lower()
            if ("illegalsessionstateexception" in error_msg or 
                "session" in error_msg and "unavailable" in error_msg or
                "replserver" in error_msg) and attempt < max_retries:
                
                print(f"\n⚠ Error de sesión detectado (intento {attempt + 1}/{max_retries + 1})")
                print(f"Error: {str(e)}")
                print("Intentando recrear sesión...")
                
                try:
                    # Recrear sesión
                    spark = get_or_create_spark()
                    spark.sql("USE `datalake-dev-silver-catalog-db`")
                    print("✓ Sesión recreada exitosamente")
                    time.sleep(2)  # Esperar un poco antes de reintentar
                except Exception as session_error:
                    print(f"✗ Error al recrear sesión: {str(session_error)}")
                    if attempt == max_retries:
                        raise
            else:
                raise

def run_notebook():
    """
    Función para ejecutar el proceso en un notebook
    """
    global spark, tables, system_props, processed_data, result_df
    
    print("INICIANDO PROCESAMIENTO PCR_TRADICIONAL")
    print("="*50)
    
    try:
        # Paso 1: Crear sesión
        print("\nPASO 1: Creando sesión Spark...")
        spark = get_or_create_spark()
        spark.sql("USE `datalake-dev-silver-catalog-db`")
        print("✓ Sesión Spark creada")
        
        # Paso 2: Cargar tablas
        print("\nPASO 2: Cargando tablas...")
        tables = execute_with_retry(load_base_tables, spark)
        
        # Paso 3: Propiedades del sistema
        print("\nPASO 3: Procesando propiedades del sistema...")
        system_props = execute_with_retry(process_system_properties, tables)
        
        # Paso 4: Procesar cada componente
        print("\nPASO 4: Procesando componentes individuales...")
        
        processed_data = {}
        
        print("\nProcesando policy_base...")
        processed_data['policy_base'] = execute_with_retry(process_policy_base, tables)
        
        print("\nProcesando policy_matrix...")
        processed_data['policy_matrix'] = execute_with_retry(process_policy_matrix, tables)
        
        print("\nProcesando risk_units...")
        processed_data['risk_units'] = execute_with_retry(process_risk_units, tables)
        
        print("\nProcesando policy_holders...")
        processed_data['policy_holders'] = execute_with_retry(process_policy_holders, tables, system_props)
        
        print("\nProcesando issued_premium...")
        processed_data['issued_premium'] = execute_with_retry(process_issued_premium, tables, system_props)
        
        print("\nProcesando intermediation_commission...")
        processed_data['intermediation_commission'] = execute_with_retry(process_intermediation_commission, tables)
        
        print("\nProcesando commission_vat...")
        processed_data['commission_vat'] = execute_with_retry(process_commission_vat, tables, system_props)
        
        print("\nProcesando reinsurance...")
        processed_data['reinsurance'] = execute_with_retry(process_reinsurance, tables)
        
        print("\nProcesando agreement_data...")
        processed_data['agreement_data'] = execute_with_retry(process_agreement_data, tables)
        
        # Paso 5: Resultado final
        print("\nPASO 5: Creando resultado final...")
        result_df = execute_with_retry(create_final_result, spark, processed_data)
        
        print("\n" + "="*50)
        print("✓ PROCESO COMPLETADO EXITOSAMENTE")
        print("="*50)
        print("\nVariables disponibles:")
        print("- spark: Sesión de Spark")
        print("- tables: Diccionario de tablas base")
        print("- system_props: Propiedades del sistema")
        print("- processed_data: Datos procesados por componente")
        print("- result_df: DataFrame con resultado final")
        
        return {
            'spark': spark,
            'tables': tables,
            'system_props': system_props,
            'processed_data': processed_data,
            'result_df': result_df
        }
        
    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}")
        print("\nDetalles del error:")
        import traceback
        traceback.print_exc()
        raise

run_notebook()






"""
PCR_TRADICIONAL - Pure PySpark Implementation
Complete conversion to PySpark DataFrame operations with minimal SQL
"""

import sys
import time
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, coalesce, sum as spark_sum, abs as spark_abs, 
    split, explode, trim, regexp_replace, expr, first,
    count, max as spark_max, min as spark_min, collect_list
)
from pyspark.sql.types import DecimalType

def create_spark_session():
    """Create and configure Spark session for AWS Glue"""
    from pyspark import SparkContext
    from awsglue.context import GlueContext
    from pyspark.sql import SparkSession

    try:
        sc = SparkContext.getOrCreate()
    except Exception:
        sc = SparkContext()

    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    
    # Set optimized configurations
    spark.conf.set("spark.sql.shuffle.partitions", "400")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.default.parallelism", "200")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    
    return spark, glueContext

def process_system_properties(spark):
    """Process system properties using pure PySpark"""
    print("Processing system properties...")
    
    # Use database
    spark.sql("USE `datalake-dev-silver-catalog-db`")
    
    # Get system properties table
    syp_df = spark.table("acsele_acsele_alfa_STST_SYSTEMPROPERTY")
    
    # ISSUED_PREMIUM_DOCS
    issued_premium_docs = syp_df \
        .filter(col("SYP_PROPERTY") == 'RRC.issuedPremium.earthquakeDocType') \
        .select(explode(split(col("SYP_VALUE"), ",")).alias("DESCRIPTION")) \
        .select(trim(col("DESCRIPTION")).alias("DESCRIPTION"))
    
    # BONDHOLDER_ROLES
    bondholder_roles = syp_df \
        .filter(col("SYP_PROPERTY") == 'bondholder.rol') \
        .select(explode(split(col("SYP_VALUE"), ",")).alias("ROL_DESCRIPTION")) \
        .select(trim(col("ROL_DESCRIPTION")).alias("ROL_DESCRIPTION"))
    
    # COMM_INTERMEDIARY_DOCS
    comm_intermediary_docs = syp_df \
        .filter(col("SYP_PROPERTY") == 'RRC.commisionIntermediary') \
        .select(explode(split(col("SYP_VALUE"), ",")).alias("DESCRIPTION")) \
        .select(trim(col("DESCRIPTION")).alias("DESCRIPTION"))
    
    # DOC_TYPES
    doc_types_df = syp_df \
        .filter(col("SYP_PROPERTY").isin([
            "RRC.issuedPremium.earthquakeDocType",
            "RRC.commisionIntermediary", 
            "RRC.commissionCollection.earthquakeDocType"
        ])) \
        .withColumn("DESCRIPTION", explode(split(col("SYP_VALUE"), ","))) \
        .withColumn("DESCRIPTION", trim(col("DESCRIPTION"))) \
        .withColumn("TIPO",
            when(col("SYP_PROPERTY") == "RRC.issuedPremium.earthquakeDocType", "ISSUED_PREMIUM")
            .when(col("SYP_PROPERTY") == "RRC.commisionIntermediary", "INTERMEDIARY")
            .when(col("SYP_PROPERTY") == "RRC.commissionCollection.earthquakeDocType", "COLLECTION")
        )
    
    return {
        'issued_premium_docs': issued_premium_docs,
        'bondholder_roles': bondholder_roles,
        'comm_intermediary_docs': comm_intermediary_docs,
        'doc_types': doc_types_df
    }

def process_bruto_data(spark, doc_types_df):
    """Process BRUTO data using pure PySpark"""
    print("Processing BRUTO data...")
    
    # Get tables
    openitem_df = spark.table("acsele_acsele_alfa_OPENITEM")
    doctype_df = spark.table("acsele_acsele_alfa_STCA_DOCTYPE")
    uaadetail_df = spark.table("acsele_acsele_alfa_UAADETAIL")
    property_df = spark.table("acsele_acsele_alfa_PROPERTY")
    
    # Process BRUTO with PySpark operations
    bruto_df = openitem_df.alias("OI") \
        .join(doctype_df.alias("DT"), col("OI.DTY_ID") == col("DT.DTY_ID")) \
        .join(doc_types_df.alias("DTY"), col("DT.DTY_DESCRIPTION") == col("DTY.DESCRIPTION")) \
        .join(uaadetail_df.alias("DET"), col("DET.OPENITEMID") == col("OI.OPENITEMID"), "left") \
        .join(property_df.alias("PP"), col("PP.PROPERTYID") == col("DET.PROPERTY"), "left") \
        .filter((col("OI.OPM_SUBSTATUS") != 'notApplied') & (col("OI.operationpk") >= 986536769)) \
        .groupBy("OI.OPERATIONPK") \
        .agg(
            spark_sum(
                when((col("DTY.TIPO") == "ISSUED_PREMIUM") & (col("DT.DTY_ISDEBIT") == 1), col("OI.AMOUNT"))
                .when((col("DTY.TIPO") == "ISSUED_PREMIUM") & (col("DT.DTY_ISDEBIT") == 0), col("OI.AMOUNT") * -1)
                .otherwise(0)
            ).alias("ISSUEDPREMIUM"),
            spark_sum(
                when((col("DTY.TIPO") == "INTERMEDIARY") & (col("PP.SIMBOLO") == "Comision"), col("DET.VALUE"))
                .otherwise(0)
            ).alias("COMMISION_INTERMEDIARY"),
            spark_sum(
                when((col("DTY.TIPO") == "COLLECTION") & (col("PP.SIMBOLO") == "ComisionRecaudo"), col("OI.AMOUNT"))
                .otherwise(0)
            ).alias("COMMISSIONCOLLECTION")
        ) \
        .withColumn("PERCENTAGEPARTICALFA", lit(100)) \
        .withColumn("ALFAPREMIUM", expr("ISSUEDPREMIUM * (PERCENTAGEPARTICALFA / 100)")) \
        .withColumn("PORCPART_ALFA", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
        .withColumn("PRIMACED_COASEGURO", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
        .withColumn("GROUP_PERCENTAGE", lit(100)) \
        .withColumn("BROKERAGECOMMISSION", expr("COMMISION_INTERMEDIARY * (GROUP_PERCENTAGE / 100)")) \
        .withColumn("PORCCOMRECAUDO", expr("(COMMISSIONCOLLECTION * 100) / ISSUEDPREMIUM")) \
        .withColumn("TIPO_REGISTRO", lit("BRUTO")) \
        .withColumnRenamed("OPERATIONPK", "DCOID")
    
    return bruto_df

def process_detalle_data(spark):
    """Process DETALLE data using pure PySpark"""
    print("Processing DETALLE data...")
    
    # Get tables
    reinsurance_op_df = spark.table("acsele_acsele_alfa_REINSURANCEOPERATION")
    reinsurance_group_df = spark.table("acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO")
    reinsurance_comp_df = spark.table("acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT")
    ri_group_df = spark.table("acsele_acsele_alfa_RI_REINSURANCEGROUP")
    riskunit_dco_df = spark.table("acsele_acsele_alfa_RISKUNITDCO")
    
    # Process DETALLE with PySpark operations
    detalle_df = reinsurance_op_df.alias("RO") \
        .join(reinsurance_group_df.alias("ROG"), col("ROG.OPERATIONID") == col("RO.OPERATIONID")) \
        .join(reinsurance_comp_df.alias("ROC"), col("ROC.REINSURANCEGROUPINFOID") == col("ROG.REINSURANCEGROUPINFOID")) \
        .join(ri_group_df.alias("RG"), col("RG.REINSURANCEGROUPID") == col("ROG.GROUPID")) \
        .join(riskunit_dco_df.alias("RUD"), col("RUD.AGREGATEDOBJECTID") == col("RO.AGREGATEDRISKUNITID")) \
        .filter((col("RO.TOTALPREMIUM") > 0) & (col("ROG.operationid") >= 943602721)) \
        .groupBy("RUD.OPERATIONPK", "RO.TOTALPREMIUM") \
        .agg(
            (spark_abs(spark_sum(col("ROG.PREMIUMAMOUNT")) * 100 / col("RO.TOTALPREMIUM"))).alias("GROUP_PERCENTAGE")
        ) \
        .select(
            col("OPERATIONPK").alias("DCOID"),
            col("TOTALPREMIUM").alias("ISSUEDPREMIUM"),
            col("GROUP_PERCENTAGE"),
            lit(0).alias("PERCENTAGEPARTICALFA"),
            lit("DETALLE").alias("TIPO_REGISTRO")
        ) \
        .withColumn("ALFAPREMIUM", expr("ISSUEDPREMIUM * (PERCENTAGEPARTICALFA / 100)")) \
        .withColumn("PORCPART_ALFA", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
        .withColumn("PRIMACED_COASEGURO", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
        .withColumn("COMMISION_INTERMEDIARY", lit(None).cast("double")) \
        .withColumn("COMMISSIONCOLLECTION", lit(None).cast("double")) \
        .withColumn("BROKERAGECOMMISSION", lit(None).cast("double")) \
        .withColumn("PORCCOMRECAUDO", lit(None).cast("double"))
    
    return detalle_df

def process_cte_poliza_base(spark):
    """Process policy base data using pure PySpark"""
    print("Processing policy base data...")
    
    # Get tables
    policydco_df = spark.table("acsele_acsele_alfa_POLICYDCO")
    contextop_df = spark.table("acsele_acsele_alfa_CONTEXTOPERATION")
    prepolicy_df = spark.table("acsele_acsele_alfa_PREPOLICY")
    financial_df = spark.table("acsele_acsele_alfa_FINANCIALPLANDCO")
    aggregated_df = spark.table("acsele_acsele_alfa_AGREGATEDPOLICY")
    
    # Process with PySpark operations
    poliza_base = policydco_df.alias("DCO") \
        .join(contextop_df.alias("CO"), col("CO.ID") == col("DCO.OPERATIONPK")) \
        .join(prepolicy_df.alias("PP"), col("PP.PK") == col("DCO.DCOID")) \
        .join(financial_df.alias("FPDCO"), 
              (col("FPDCO.AGREGATEDPARENTID") == col("DCO.AGREGATEDOBJECTID")) & 
              (col("FPDCO.OPERATIONPK") == col("DCO.OPERATIONPK"))) \
        .join(aggregated_df.alias("AP"), col("AP.AGREGATEDPOLICYID") == col("DCO.AGREGATEDOBJECTID")) \
        .filter((col("CO.id") >= 986530136) & (col("CO.status") == 2)) \
        .select(
            col("DCO.OPERATIONPK").alias("DCOID"),
            col("CO.TIME_STAMP").alias("FECHA_ENDOSO"),
            col("PP.NUMEROPOLIZAINPUT").alias("POLIZA"),
            col("AP.PRODUCTID").alias("PRODUCTO"),
            col("PP.OFICINAINPUT").alias("SUCURSAL"),
            col("PP.FRECUENCIAPAGOINPUT").alias("FRECUENCIA_PAGO"),
            col("DCO.INITIALDATE").alias("FECHA_INICIO_POLIZA_MATRIZ"),
            col("DCO.FINISHDATE").alias("FECHA_FIN_POLIZA_MATRIZ")
        ).distinct()
    
    return poliza_base

def process_cte_poliza_matriz(spark):
    """Process policy matrix data using pure PySpark"""
    print("Processing policy matrix data...")
    
    # Get tables
    policydco_df = spark.table("acsele_acsele_alfa_POLICYDCO")
    relation_df = spark.table("acsele_acsele_alfa_STPS_RELATIONAGREEMENTPOLICY")
    agreement_static_df = spark.table("acsele_acsele_alfa_STPS_AGREEMENTSTATICDAT")
    agreement_version_df = spark.table("acsele_acsele_alfa_STAG_AGREEMENTVERSION")
    preagreement_df = spark.table("acsele_acsele_alfa_PREAGREEMENT")
    
    # Process with PySpark operations
    poliza_matriz = policydco_df.alias("DCO") \
        .join(relation_df.alias("RAP"), col("RAP.RSA_POLICYID") == col("DCO.AGREGATEDOBJECTID")) \
        .join(agreement_static_df.alias("AGS"), col("RAP.RSA_AST_ID") == col("AGS.AST_ID")) \
        .join(agreement_version_df.alias("AV"), col("AV.AST_ID") == col("RAP.RSA_AST_ID")) \
        .join(preagreement_df.alias("PREA"), col("PREA.PK") == col("AV.DCOID")) \
        .filter((col("RAP.rsa_policyid") >= 183066469) & (col("AV.ast_id") >= 73001)) \
        .select(
            col("DCO.OPERATIONPK").alias("DCOID"),
            col("AGS.AST_NUMEROACU").alias("POLIZA_MATRIZ"),
            col("PREA.UNIDADNEGOCIOINPUT").alias("UNIDAD_NEGOCIO")
        ).distinct()
    
    return poliza_matriz

def process_cte_unidad_riesgo(spark):
    """Process risk unit data using pure PySpark"""
    print("Processing risk unit data...")
    
    # Get tables
    preriskunits_df = spark.table("acsele_acsele_alfa_PRERISKUNITS")
    riskunit_dco_df = spark.table("acsele_acsele_alfa_RISKUNITDCO")
    
    # Process with PySpark operations
    unidad_riesgo = preriskunits_df.alias("PRU") \
        .join(riskunit_dco_df.alias("DCO"), col("DCO.DCOID") == col("PRU.PK")) \
        .filter(col("PRU.pk") >= 61652035099) \
        .select(
            col("DCO.OPERATIONPK").alias("DCOID"),
            col("PRU.NUMUNIDRIESGOINPUT").alias("RISKUNIT_NUMBER"),
            col("DCO.INITIALDATE").alias("RISKUNIT_BEGINDATE"),
            col("DCO.FINISHDATE").alias("RISKUNIT_ENDDATE")
        )
    
    return unidad_riesgo

def process_cte_tomadores(spark, bondholder_roles):
    """Process policy holders data using pure PySpark"""
    print("Processing policy holders data...")
    
    # Get tables
    policy_participation_df = spark.table("acsele_acsele_alfa_STPO_POLICYPARTICIPATION")
    policy_participation_dco_df = spark.table("acsele_acsele_alfa_STPO_POLICYPARTICIPATIONDCO")
    thirdparty_df = spark.table("acsele_acsele_alfa_STTE_THIRDPARTY")
    prethirdparty_df = spark.table("acsele_acsele_alfa_PRETHIRDPARTY")
    role_df = spark.table("acsele_acsele_alfa_STTS_ROLE")
    
    # Get bondholder roles list
    bondholder_roles_list = [row.ROL_DESCRIPTION for row in bondholder_roles.collect()]
    
    # Process with PySpark operations
    tomadores = policy_participation_df.alias("PP") \
        .join(policy_participation_dco_df.alias("PPD"), col("PP.AGREGATEDOBJECTID") == col("PPD.AGREGATEDOBJECTID")) \
        .join(thirdparty_df.alias("TP"), col("TP.TPT_ID") == col("PP.THIRDPARTYID")) \
        .join(prethirdparty_df.alias("PTP"), col("PTP.PK") == col("TP.IDDCO")) \
        .join(role_df.alias("RL"), col("RL.ROL_ID") == col("PP.ROL_ID")) \
        .filter((col("RL.ROL_DESCRIPTION").isin(bondholder_roles_list)) & 
                (col("PP.agregatedobjectid") >= 474064566)) \
        .select(
            col("PPD.OPERATIONPK").alias("DCOID"),
            col("PTP.DOCUMENTOIDENTIDADINPUT").alias("ID_TOMADOR_POLIZA"),
            col("PTP.NOMBREPREDETERMINADOINPUT").alias("TOMADOR_POLIZA")
        ).distinct()
    
    return tomadores

def process_cte_prima_emitida(spark, issued_premium_docs):
    """Process issued premium data using pure PySpark"""
    print("Processing issued premium data...")
    
    # Get tables
    openitem_df = spark.table("acsele_acsele_alfa_OPENITEM")
    doctype_df = spark.table("acsele_acsele_alfa_STCA_DOCTYPE")
    
    # Get issued docs list
    issued_docs_list = [row.DESCRIPTION for row in issued_premium_docs.collect()]
    
    # Process with PySpark operations
    prima_emitida = openitem_df.alias("OI") \
        .join(doctype_df.alias("DT"), col("DT.DTY_ID") == col("OI.DTY_ID")) \
        .filter((col("OI.OPM_SUBSTATUS") != 'notApplied') & 
                (col("DT.DTY_DESCRIPTION").isin(issued_docs_list))) \
        .select(
            col("OI.OPERATIONPK").alias("DCOID"),
            when(col("DT.DTY_ISDEBIT") == 1, col("OI.AMOUNT").cast(DecimalType(18,2)))
            .otherwise(col("OI.AMOUNT").cast(DecimalType(18,2)) * -1).alias("AMOUNT")
        ) \
        .groupBy("DCOID") \
        .agg(spark_sum("AMOUNT").alias("PRIMA_EMITIDA"))
    
    return prima_emitida

def process_cte_comision_intermediacion(spark):
    """Process intermediation commission data using pure PySpark"""
    print("Processing intermediation commission data...")
    
    # Get tables
    policy_participation_df = spark.table("acsele_acsele_alfa_STPO_POLICYPARTICIPATION")
    policy_participation_dco_df = spark.table("acsele_acsele_alfa_STPO_POLICYPARTICIPATIONDCO")
    participacion_df = spark.table("acsele_acsele_alfa_PARTICIPACIONINTERMEDIARIOS")
    
    # Process with PySpark operations
    comision_intermediacion = policy_participation_df.alias("PP") \
        .join(policy_participation_dco_df.alias("PPD"), col("PP.AGREGATEDOBJECTID") == col("PPD.AGREGATEDOBJECTID")) \
        .join(participacion_df.alias("PI"), col("PI.PK") == col("PPD.DCOID")) \
        .filter((col("PP.agregatedobjectid") >= 474064566) & 
                (col("PPD.operationpk") >= 986530136)) \
        .select(
            col("PPD.operationpk").alias("DCOID"),
            col("PI.PORCENTAJECOMISIONINPUT").alias("PORC_COM_INTERMEDIACION")
        )
    
    return comision_intermediacion

def process_cte_acuerdo(spark):
    """Process agreement data using pure PySpark"""
    print("Processing agreement data...")
    
    # Get tables
    coveragedco_df = spark.table("acsele_acsele_alfa_COVERAGEDCO")
    cobertura_df = spark.table("acsele_acsele_alfa_COBERTURA")
    policydco_df = spark.table("acsele_acsele_alfa_POLICYDCO")
    contextop_df = spark.table("acsele_acsele_alfa_CONTEXTOPERATION")
    aggregated_df = spark.table("acsele_acsele_alfa_AGREGATEDPOLICY")
    product_df = spark.table("acsele_acsele_alfa_PRODUCT")
    preproduct_df = spark.table("acsele_acsele_alfa_PREPRODUCT")
    
    # Process with PySpark operations
    acuerdo = coveragedco_df.alias("CDCO") \
        .join(cobertura_df.alias("CB"), col("CB.PK") == col("CDCO.DCOID")) \
        .join(policydco_df.alias("PDCO"), col("PDCO.OPERATIONPK") == col("CDCO.OPERATIONPK"), "left") \
        .join(contextop_df.alias("CO"), col("CO.ID") == col("PDCO.OPERATIONPK")) \
        .join(aggregated_df.alias("AP"), col("AP.AGREGATEDPOLICYID") == col("PDCO.AGREGATEDOBJECTID"), "left") \
        .join(product_df.alias("PD"), col("PD.PRODUCTID") == col("AP.PRODUCTID"), "left") \
        .join(preproduct_df.alias("PP"), col("PP.PK") == col("PD.PRO_DCOID"), "left") \
        .filter((col("CO.id") >= 986530136) & (col("CO.status") == 2)) \
        .select(
            col("CDCO.OPERATIONPK").alias("DCOID"),
            col("PP.COMPANIAINPUT").alias("COMPANIA"),
            col("PP.DESCRIPCIONPRODUCTOINPUT").alias("NOMBRE_PRODUCTO"),
            col("PP.CODIGOPRODUCTOINPUT").alias("CODIGO_PRODUCTO"),
            regexp_replace(col("CB.COBCODIGOCOBERTURAINPUT"), "\\.0", "").alias("COBERTURA"),
            col("CB.COBDESCRIPCIONCOBERTINPUT").alias("DESC_COBERTURA"),
            regexp_replace(col("CB.COBRAMOCONTABLEINPUT"), "\\.0", "").alias("RAMO"),
            col("PP.RAMOINPUT").alias("DESC_RAMO")
        ).distinct()
    
    return acuerdo

def process_cte_nombre_reaseg(spark):
    """Process reinsurance name data using pure PySpark"""
    print("Processing reinsurance name data...")
    
    # Get tables
    reinsurance_op_df = spark.table("acsele_acsele_alfa_REINSURANCEOPERATION")
    reinsurance_group_df = spark.table("acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO")
    reinsurance_comp_df = spark.table("acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT")
    reinsurance_detail_df = spark.table("acsele_acsele_alfa_REINSURANCEOPERATIONDETAIL")
    ri_group_df = spark.table("acsele_acsele_alfa_RI_REINSURANCEGROUP")
    riskunit_dco_df = spark.table("acsele_acsele_alfa_RISKUNITDCO")
    
    # Process with PySpark operations
    nombre_reaseg = reinsurance_op_df.alias("RO") \
        .join(reinsurance_group_df.alias("ROG"), col("ROG.OPERATIONID") == col("RO.OPERATIONID")) \
        .join(reinsurance_comp_df.alias("ROC"), col("ROC.REINSURANCEGROUPINFOID") == col("ROG.REINSURANCEGROUPINFOID")) \
        .join(reinsurance_detail_df.alias("ROD"), col("ROD.REINOPERCOMPONENTID") == col("ROC.REINOPERCOMPONENTID")) \
        .join(ri_group_df.alias("RG"), col("RG.REINSURANCEGROUPID") == col("ROG.GROUPID")) \
        .join(riskunit_dco_df.alias("RUD"), col("RUD.AGREGATEDOBJECTID") == col("RO.AGREGATEDRISKUNITID")) \
        .filter((col("ROC.CONTRACTTYPE") == 'MandatoryFacultativeContract') & 
                (col("RO.operationid") >= 943602721) & 
                (col("ROC.reinsurancegroupinfoid") >= 674470981)) \
        .groupBy("ROD.THIRDPARTYID", "ROD.THIRDPARTYNAME", "ROD.PARTICIPATIONID", 
                 "RUD.OPERATIONPK", "RUD.DCOID", "RO.operationid") \
        .agg(spark_sum("ROD.ROD_PRORATEDPREMIUM").alias("DETAIL_PREMIUM")) \
        .select(
            col("DCOID"),
            col("THIRDPARTYNAME").alias("NOMREASEG")
        ).distinct()
    
    return nombre_reaseg

def process_cte_reaseguro(spark):
    """Process reinsurance data using pure PySpark"""
    print("Processing reinsurance data...")
    
    # Get tables
    reinsurance_op_df = spark.table("acsele_acsele_alfa_REINSURANCEOPERATION")
    reinsurance_group_df = spark.table("acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO")
    reinsurance_comp_df = spark.table("acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT")
    reinsurance_detail_df = spark.table("acsele_acsele_alfa_REINSURANCEOPERATIONDETAIL")
    thirdpartyrole_df = spark.table("acsele_acsele_alfa_THIRDPARTYROLE")
    reasegurador_df = spark.table("acsele_acsele_alfa_REASEGURADOR")
    riskunit_dco_df = spark.table("acsele_acsele_alfa_RISKUNITDCO")
    
    # Process detail premium subquery
    detail_premium = reinsurance_op_df.alias("RO") \
        .join(reinsurance_group_df.alias("ROG"), col("ROG.OPERATIONID") == col("RO.OPERATIONID")) \
        .join(reinsurance_comp_df.alias("ROC"), col("ROC.REINSURANCEGROUPINFOID") == col("ROG.REINSURANCEGROUPINFOID")) \
        .join(reinsurance_detail_df.alias("ROD"), col("ROD.REINOPERCOMPONENTID") == col("ROC.REINOPERCOMPONENTID")) \
        .groupBy("ROD.THIRDPARTYID", "ROD.THIRDPARTYNAME", "ROD.PARTICIPATIONID") \
        .agg(spark_sum("ROD.ROD_PRORATEDPREMIUM").alias("DETAIL_PREMIUM"))
    
    # Process reinsurance commission subquery
    reinsurance_prop_df = spark.table("acsele_acsele_alfa_REINSURANCEOPERATIONPROPERTY")
    reinsurance_commission = reinsurance_prop_df.alias("PT") \
        .join(reinsurance_detail_df.alias("ROD"), col("PT.ROD_ID") == col("ROD.REINSURANCEOPERATIONDETAILID")) \
        .filter(col("PT.ROPP_NAME").isin("ComPriCedida", "ComPriCedidaInd")) \
        .groupBy("ROD.REINSURANCEOPERATIONDETAILID", "ROD.THIRDPARTYID", "ROD.THIRDPARTYNAME", "ROD.PARTICIPATIONID") \
        .agg(spark_sum("PT.ROPP_VALUE").alias("REINSURANCE_COMMISSION"))
    
    # Main reinsurance query
    reaseguro = reinsurance_detail_df.alias("ROD") \
        .join(reinsurance_comp_df.alias("ROC"), col("ROC.REINOPERCOMPONENTID") == col("ROD.REINOPERCOMPONENTID")) \
        .join(reinsurance_group_df.alias("ROG"), col("ROG.REINSURANCEGROUPINFOID") == col("ROC.REINSURANCEGROUPINFOID")) \
        .join(reinsurance_op_df.alias("RO"), col("RO.OPERATIONID") == col("ROG.OPERATIONID")) \
        .join(riskunit_dco_df.alias("RUD"), col("RUD.AGREGATEDOBJECTID") == col("RO.AGREGATEDRISKUNITID")) \
        .join(thirdpartyrole_df.alias("TPR"), col("TPR.THIRDPARTYID") == col("ROD.THIRDPARTYID")) \
        .join(reasegurador_df.alias("RA"), col("RA.PK") == col("TPR.IDDCOROLE")) \
        .join(detail_premium.alias("dp"), 
              (col("dp.THIRDPARTYID") == col("ROD.THIRDPARTYID")) & 
              (col("dp.PARTICIPATIONID") == col("ROD.PARTICIPATIONID")), "left") \
        .join(reinsurance_commission.alias("rc"),
              (col("rc.THIRDPARTYID") == col("ROD.THIRDPARTYID")) & 
              (col("rc.PARTICIPATIONID") == col("ROD.PARTICIPATIONID")) &
              (col("rc.REINSURANCEOPERATIONDETAILID") == col("ROD.REINSURANCEOPERATIONDETAILID")), "left") \
        .select(
            col("RUD.OPERATIONPK").alias("DCOID"),
            col("RA.CLASEREASEGUROINPUT").alias("CLASE_REASEGURO"),
            col("RA.CODIGOREASEGURADORINPUT").alias("CODIGO_REASEGURADOR"),
            col("ROD.THIRDPARTYID"),
            col("ROD.THIRDPARTYNAME"),
            col("ROD.PARTICIPATIONID"),
            when(col("RA.CLASEREASEGUROINPUT") == "Interior", col("dp.DETAIL_PREMIUM")).otherwise(lit(0)).alias("PRIMACEDAUT_INT"),
            when(col("RA.CLASEREASEGUROINPUT") == "Exterior", col("dp.DETAIL_PREMIUM")).otherwise(lit(0)).alias("PRIMACEDAUT_EXT"),
            when(col("RA.CLASEREASEGUROINPUT") == "Interior", col("dp.DETAIL_PREMIUM")).otherwise(lit(0)).alias("PRIMACEDFAC_INT"),
            when(col("RA.CLASEREASEGUROINPUT") == "Exterior", col("dp.DETAIL_PREMIUM")).otherwise(lit(0)).alias("PRIMACEDFAC_EXT"),
            when(col("RA.CLASEREASEGUROINPUT") == "Interior", col("rc.REINSURANCE_COMMISSION")).otherwise(lit(0)).alias("COMISIONREASEGAUT_INT"),
            when(col("RA.CLASEREASEGUROINPUT") == "Exterior", col("rc.REINSURANCE_COMMISSION")).otherwise(lit(0)).alias("COMISIONREASEGAUT_EXT"),
            when(col("RA.CLASEREASEGUROINPUT") == "Interior", col("rc.REINSURANCE_COMMISSION")).otherwise(lit(0)).alias("COMISIONREASEGFAC_INT"),
            when(col("RA.CLASEREASEGUROINPUT") == "Exterior", col("rc.REINSURANCE_COMMISSION")).otherwise(lit(0)).alias("COMISIONREASEGFAC_EXT")
        ).distinct()
    
    return reaseguro

def process_cte_iva_comision_intermediacion(spark, comm_intermediary_docs):
    """Process intermediation commission VAT data using pure PySpark"""
    print("Processing intermediation commission VAT data...")
    
    # Get tables
    openitem_df = spark.table("acsele_acsele_alfa_OPENITEM")
    doctype_df = spark.table("acsele_acsele_alfa_STCA_DOCTYPE")
    uaadetail_df = spark.table("acsele_acsele_alfa_UAADETAIL")
    property_df = spark.table("acsele_acsele_alfa_PROPERTY")
    
    # Get comm docs list
    comm_docs_list = [row.DESCRIPTION for row in comm_intermediary_docs.collect()]
    
    # Process with PySpark operations
    iva_comision = openitem_df.alias("OI") \
        .join(doctype_df.alias("DT"), col("DT.DTY_ID") == col("OI.DTY_ID")) \
        .join(uaadetail_df.alias("DET"), col("DET.OPENITEMID") == col("OI.OPENITEMID")) \
        .join(property_df.alias("PP"), col("PP.PROPERTYID") == col("DET.PROPERTY").cast("bigint")) \
        .filter((col("OI.OPM_SUBSTATUS") != 'notApplied') & 
                (col("DT.DTY_DESCRIPTION").isin(comm_docs_list)) & 
                (col("PP.SIMBOLO") == 'IvaComision')) \
        .select(
            col("OI.OPERATIONPK").alias("DCOID"),
            col("DET.VALUE").cast(DecimalType(18,2)).alias("VALUE")
        ) \
        .groupBy("DCOID") \
        .agg(spark_sum("VALUE").alias("IVA_COMISION_INTERMEDIACION"))
    
    return iva_comision

def process_cte_iva_comision_recaudo(spark):
    """Process collection commission VAT data using pure PySpark"""
    print("Processing collection commission VAT data...")
    
    # Get tables
    openitem_df = spark.table("acsele_acsele_alfa_OPENITEM")
    doctype_df = spark.table("acsele_acsele_alfa_STCA_DOCTYPE")
    uaadetail_df = spark.table("acsele_acsele_alfa_UAADETAIL")
    property_df = spark.table("acsele_acsele_alfa_PROPERTY")
    
    # Get comm intermediary docs for recaudo (reuse from system properties)
    syp_df = spark.table("acsele_acsele_alfa_STST_SYSTEMPROPERTY")
    comm_intermediary_docs_recaudo = syp_df \
        .filter(col("SYP_PROPERTY") == 'RRC.commisionIntermediary') \
        .select(explode(split(col("SYP_VALUE"), ",")).alias("DESCRIPTION")) \
        .select(trim(col("DESCRIPTION")).alias("DESCRIPTION"))
    
    comm_docs_recaudo_list = [row.DESCRIPTION for row in comm_intermediary_docs_recaudo.collect()]
    
    # Process with PySpark operations
    iva_comision_recaudo = openitem_df.alias("OI") \
        .join(doctype_df.alias("DT"), col("DT.DTY_ID") == col("OI.DTY_ID")) \
        .join(uaadetail_df.alias("DET"), col("DET.OPENITEMID") == col("OI.OPENITEMID")) \
        .join(property_df.alias("PP"), col("PP.PROPERTYID") == col("DET.PROPERTY").cast("decimal")) \
        .filter((col("OI.OPM_SUBSTATUS") != 'notApplied') & 
                (col("DT.DTY_DESCRIPTION").isin(comm_docs_recaudo_list)) & 
                (col("PP.SIMBOLO") == 'IvaComision')) \
        .select(
            col("OI.OPERATIONPK").alias("DCOID"),
            col("DET.VALUE").cast(DecimalType(18,2)).alias("VALUE")
        ) \
        .groupBy("DCOID") \
        .agg(spark_sum("VALUE").alias("IVA_COMISION_RECAUDO"))
    
    return iva_comision_recaudo

def process_cte_primcedaut(spark):
    """Process ceded premium data using pure PySpark"""
    print("Processing ceded premium data...")
    
    # Get tables
    reinsurance_op_df = spark.table("acsele_acsele_alfa_REINSURANCEOPERATION")
    reinsurance_group_df = spark.table("acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO")
    reinsurance_comp_df = spark.table("acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT")
    reinsurance_detail_df = spark.table("acsele_acsele_alfa_REINSURANCEOPERATIONDETAIL")
    ri_group_df = spark.table("acsele_acsele_alfa_RI_REINSURANCEGROUP")
    policydco_df = spark.table("acsele_acsele_alfa_POLICYDCO")
    reinsurance_prop_df = spark.table("acsele_acsele_alfa_REINSURANCEOPERATIONPROPERTY")
    
    # Premium detail subquery
    premium_detail = reinsurance_group_df.alias("ROG2") \
        .join(reinsurance_comp_df.alias("ROC2"), col("ROC2.REINSURANCEGROUPINFOID") == col("ROG2.REINSURANCEGROUPINFOID")) \
        .join(reinsurance_detail_df.alias("RD"), col("RD.REINOPERCOMPONENTID") == col("ROC2.REINOPERCOMPONENTID")) \
        .groupBy("ROG2.OPERATIONID", "ROC2.CONTRACTTYPE") \
        .agg(spark_sum("RD.ROD_PRORATEDPREMIUM").alias("SUM_PRIMA"))
    
    # Reinsurance commission subquery
    reinsurance_commission = reinsurance_group_df.alias("ROG2") \
        .join(reinsurance_comp_df.alias("ROC2"), col("ROC2.REINSURANCEGROUPINFOID") == col("ROG2.REINSURANCEGROUPINFOID")) \
        .join(reinsurance_detail_df.alias("RD"), col("RD.REINOPERCOMPONENTID") == col("ROC2.REINOPERCOMPONENTID")) \
        .join(reinsurance_prop_df.alias("PT"), col("PT.ROD_ID") == col("RD.REINSURANCEOPERATIONDETAILID")) \
        .filter(col("PT.ROPP_NAME").isin("ComPriCedida", "ComPriCedidaInd")) \
        .groupBy("ROG2.OPERATIONID", "ROC2.CONTRACTTYPE") \
        .agg(spark_sum("PT.ROPP_VALUE").alias("SUM_COMISION"))
    
    # Main query
    primcedaut = reinsurance_op_df.alias("RO") \
        .join(reinsurance_group_df.alias("ROG"), col("ROG.OPERATIONID") == col("RO.OPERATIONID")) \
        .join(reinsurance_comp_df.alias("ROC"), col("ROC.REINSURANCEGROUPINFOID") == col("ROG.REINSURANCEGROUPINFOID")) \
        .join(reinsurance_detail_df.alias("ROD"), col("ROD.REINOPERCOMPONENTID") == col("ROC.REINOPERCOMPONENTID")) \
        .join(ri_group_df.alias("RG"), col("RG.REINSURANCEGROUPID") == col("ROG.GROUPID")) \
        .join(policydco_df.alias("PLO"), col("RO.cor_id") == col("PLO.operationpk")) \
        .join(premium_detail.alias("dp_aut"), 
              (col("dp_aut.OPERATIONID") == col("RO.OPERATIONID")) & 
              (~col("dp_aut.CONTRACTTYPE").isin("FacultativeReinsuranceContract", "MandatoryFacultativeContract")), "left") \
        .join(premium_detail.alias("dp_fac"), 
              (col("dp_fac.OPERATIONID") == col("RO.OPERATIONID")) & 
              (col("dp_fac.CONTRACTTYPE").isin("FacultativeReinsuranceContract", "MandatoryFacultativeContract")), "left") \
        .join(reinsurance_commission.alias("rc_aut"), 
              (col("rc_aut.OPERATIONID") == col("RO.OPERATIONID")) & 
              (~col("rc_aut.CONTRACTTYPE").isin("FacultativeReinsuranceContract", "MandatoryFacultativeContract")), "left") \
        .join(reinsurance_commission.alias("rc_fac"), 
              (col("rc_fac.OPERATIONID") == col("RO.OPERATIONID")) & 
              (col("rc_fac.CONTRACTTYPE").isin("FacultativeReinsuranceContract", "MandatoryFacultativeContract")), "left") \
        .select(
            col("PLO.DCOID"),
            col("PLO.OPERATIONPK"),
            col("RO.OPERATIONID"),
            col("ROC.REINSURANCEGROUPINFOID"),
            col("ROD.REINOPERCOMPONENTID"),
            col("RG.REINSURANCEGROUPID"),
            when(col("ROC.CONTRACTTYPE").isin("FacultativeReinsuranceContract", "MandatoryFacultativeContract"), lit(0))
            .otherwise(coalesce(col("dp_aut.SUM_PRIMA"), lit(0))).alias("PRIMACEDAUT"),
            when(col("ROC.CONTRACTTYPE").isin("FacultativeReinsuranceContract", "MandatoryFacultativeContract"), 
                 coalesce(col("dp_fac.SUM_PRIMA"), lit(0)))
            .otherwise(lit(0)).alias("PRIMACEDFAC"),
            when(col("ROC.CONTRACTTYPE").isin("FacultativeReinsuranceContract", "MandatoryFacultativeContract"), lit(0))
            .otherwise(coalesce(col("rc_aut.SUM_COMISION"), lit(0))).alias("COMISIONREASEGAUT"),
            when(col("ROC.CONTRACTTYPE").isin("FacultativeReinsuranceContract", "MandatoryFacultativeContract"), 
                 coalesce(col("rc_fac.SUM_COMISION"), lit(0)))
            .otherwise(lit(0)).alias("COMISIONREASEGFAC")
        ).distinct()
    
    return primcedaut

def create_branch_mapping_expression():
    """Create branch mapping expression for RAMO field"""
    return when(col("ac.RAMO") == '1', 'AUTOS') \
        .when(col("ac.RAMO") == '3', 'EQUIPO ELECTRONICO') \
        .when(col("ac.RAMO") == '4', 'CUMPLIMIENTO') \
        .when(col("ac.RAMO") == '5', 'INCENDIO') \
        .when(col("ac.RAMO") == '6', 'LUCRO') \
        .when(col("ac.RAMO") == '7', 'MANEJO') \
        .when(col("ac.RAMO") == '8', 'ROTURA MAQUINARIA') \
        .when(col("ac.RAMO") == '9', 'CASCO') \
        .when(col("ac.RAMO") == '10', 'RESPONSABILIDAD') \
        .when(col("ac.RAMO") == '12', 'SUSTRACCION') \
        .when(col("ac.RAMO") == '13', 'TERREMOTO') \
        .when(col("ac.RAMO") == '14', 'TODO RIESGO') \
        .when(col("ac.RAMO") == '15', 'TRANSPORTE') \
        .when(col("ac.RAMO") == '16', 'VIDRIOS') \
        .when(col("ac.RAMO") == '18', 'ACCIDENTES PERSONALES') \
        .when(col("ac.RAMO") == '20', 'VIDA GRUPO NO CONTRIBUTIVO') \
        .when(col("ac.RAMO") == '22', 'DEUDORES') \
        .when(col("ac.RAMO") == '24', 'DESEMPLEO') \
        .when(col("ac.RAMO") == '25', 'HOGAR') \
        .when(col("ac.RAMO") == '30', 'EXEQUIAS') \
        .when(col("ac.RAMO") == '31', 'PREVISIONALES') \
        .when(col("ac.RAMO") == '34', 'SALUD') \
        .when(col("ac.RAMO") == '40', 'VIDA INDIVIDUAL') \
        .when(col("ac.RAMO") == '44', 'JUDICIAL') \
        .when(col("ac.RAMO") == '50', 'RIESGOS PROFESIONALES') \
        .when(col("ac.RAMO") == '51', 'RENTAS VITALICIAS') \
        .when(col("ac.RAMO") == '52', 'RENTAS TASA VARIABLE') \
        .when(col("ac.RAMO") == '70', 'VIDA GRUPO CONTRIBUTIVO') \
        .when(col("ac.RAMO") == '95', 'MONTAJE') \
        .when(col("ac.RAMO") == '96', 'EQUIPO Y MAQUINARIA')

def get_id_ranges(spark, table_name, id_column, batch_size):
    """Get ID ranges for batch processing"""
    print(f"Calculating ID ranges for {table_name}...")
    
    # Get min and max IDs
    id_stats = spark.table(table_name).agg(
        spark_min(id_column).alias("min_id"),
        spark_max(id_column).alias("max_id"),
        count("*").alias("total_count")
    ).collect()[0]
    
    min_id = id_stats["min_id"]
    max_id = id_stats["max_id"]
    total_count = id_stats["total_count"]
    
    print(f"  ID range: {min_id:,} to {max_id:,}")
    print(f"  Total records: {total_count:,}")
    
    # Calculate batch ranges based on ID distribution
    id_range = max_id - min_id
    batch_id_size = max(1, id_range // max(1, total_count // batch_size))
    
    ranges = []
    current_id = min_id
    batch_num = 1
    
    while current_id <= max_id:
        end_id = min(current_id + batch_id_size, max_id)
        ranges.append({
            'batch_num': batch_num,
            'start_id': current_id,
            'end_id': end_id
        })
        current_id = end_id + 1
        batch_num += 1
    
    print(f"  Created {len(ranges)} batches with ID ranges")
    return ranges, total_count

def process_data_by_batches(spark, batch_size=100000):
    """
    Main data processing function with ID-based batch processing
    
    Args:
        spark: SparkSession object
        batch_size: Number of records to process in each batch (default: 100000)
    """
    try:
        print("Starting PCR_TRADICIONAL processing with ID-based batching...")
        total_start_time = time.time()
        
        # Process system properties (global, not batched)
        print("\nStep 1/4: Processing system properties...")
        step_start = time.time()
        system_props = process_system_properties(spark)
        print(f"✓ System properties processed in {time.time() - step_start:.2f}s")
        
        # Get ID ranges for main processing table
        print("\nStep 2/4: Calculating ID ranges for batch processing...")
        step_start = time.time()
        id_ranges, total_records = get_id_ranges(
            spark, 
            "acsele_acsele_alfa_POLICYDCO", 
            "OPERATIONPK", 
            batch_size
        )
        print(f"✓ ID ranges calculated in {time.time() - step_start:.2f}s")
        print(f"✓ Will process {len(id_ranges)} batches covering {total_records:,} total records")
        
        # Process each batch
        print(f"\nStep 3/4: Processing {len(id_ranges)} batches...")
        all_results = []
        
        for i, id_range in enumerate(id_ranges, 1):
            batch_start_time = time.time()
            start_id = id_range['start_id']
            end_id = id_range['end_id']
            
            print(f"\n  Batch {i}/{len(id_ranges)}: Processing IDs {start_id:,} to {end_id:,}")
            
            # Process this batch
            batch_result = process_single_batch(spark, system_props, start_id, end_id, i)
            
            if batch_result is not None and batch_result.count() > 0:
                all_results.append(batch_result)
                batch_count = batch_result.count()
                batch_time = time.time() - batch_start_time
                print(f"  ✓ Batch {i} completed: {batch_count:,} records in {batch_time:.2f}s")
            else:
                print(f"  ✓ Batch {i} completed: 0 records (no data in ID range)")
        
        # Combine all batch results
        print(f"\nStep 4/4: Combining all batch results...")
        step_start = time.time()
        
        if all_results:
            final_df = all_results[0]
            for batch_df in all_results[1:]:
                final_df = final_df.unionByName(batch_df)
        else:
            # Create empty DataFrame with correct schema if no results
            final_df = spark.createDataFrame([], get_output_schema())
        
        final_count = final_df.count()
        print(f"✓ Combined {len(all_results)} batches into {final_count:,} total records")
        print(f"✓ Combination completed in {time.time() - step_start:.2f}s")
        return complete_batch_processing(spark, final_df, total_start_time)
    except Exception as e:
        print(f"Error in batch processing: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
        
def process_single_batch(spark, system_props, start_id, end_id, batch_num):
    """Process a single batch of data based on ID range"""
    try:
        print(f"    Processing batch {batch_num} (IDs {start_id:,} to {end_id:,})...")
        batch_start = time.time()

        # Add ID range filter to each CTE
        poliza_base = process_cte_poliza_base(spark) \
            .filter((col("DCOID") >= start_id) & (col("DCOID") <= end_id))
        
        if poliza_base.count() == 0:
            print(f"    No records found in ID range {start_id:,} to {end_id:,}")
            return None
            
        # Process remaining CTEs with ID filters
        poliza_matriz = process_cte_poliza_matriz(spark) \
            .filter((col("DCOID") >= start_id) & (col("DCOID") <= end_id))
            
        unidad_riesgo = process_cte_unidad_riesgo(spark) \
            .filter((col("DCOID") >= start_id) & (col("DCOID") <= end_id))
            
        # Process remaining CTEs with ID filters where applicable
        tomadores = process_cte_tomadores(spark, system_props['bondholder_roles']) \
            .filter((col("DCOID") >= start_id) & (col("DCOID") <= end_id))
            
        prima_emitida = process_cte_prima_emitida(spark, system_props['issued_premium_docs']) \
            .filter((col("DCOID") >= start_id) & (col("DCOID") <= end_id))
            
        comision_intermediacion = process_cte_comision_intermediacion(spark) \
            .filter((col("DCOID") >= start_id) & (col("DCOID") <= end_id))
            
        acuerdo = process_cte_acuerdo(spark) \
            .filter((col("DCOID") >= start_id) & (col("DCOID") <= end_id))
            
        iva_comision = process_cte_iva_comision_intermediacion(spark, system_props['comm_intermediary_docs']) \
            .filter((col("DCOID") >= start_id) & (col("DCOID") <= end_id))
            
        nombre_reaseg = process_cte_nombre_reaseg(spark) \
            .filter((col("DCOID") >= start_id) & (col("DCOID") <= end_id))
            
        reaseguro = process_cte_reaseguro(spark) \
            .filter((col("DCOID") >= start_id) & (col("DCOID") <= end_id))
            
        iva_comision_recaudo = process_cte_iva_comision_recaudo(spark) \
            .filter((col("DCOID") >= start_id) & (col("DCOID") <= end_id))
            
        primcedaut = process_cte_primcedaut(spark) \
            .filter((col("DCOID") >= start_id) & (col("DCOID") <= end_id))
        
        # Process BRUTO and DETALLE data with ID filters
        bruto_df = process_bruto_data(spark, system_props['doc_types']) \
            .filter((col("DCOID") >= start_id) & (col("DCOID") <= end_id))
            
        detalle_df = process_detalle_data(spark) \
            .filter((col("DCOID") >= start_id) & (col("DCOID") <= end_id))
        
        # Union BRUTO and DETALLE for this batch
        tmp_iva_comision_recaudo = bruto_df.select(detalle_df.columns).unionByName(detalle_df)
        
        # Create branch mapping expression
        branch_expr = create_branch_mapping_expression()
        
        # Final join and selection for this batch
        batch_result = poliza_base.alias("pb") \
            .join(poliza_matriz.alias("pm"), col("pm.DCOID") == col("pb.DCOID"), "left") \
            .join(unidad_riesgo.alias("ur"), col("ur.DCOID") == col("pb.DCOID"), "left") \
            .join(tomadores.alias("t"), col("t.DCOID") == col("pb.DCOID"), "left") \
            .join(prima_emitida.alias("pe"), col("pe.DCOID") == col("pb.DCOID"), "left") \
            .join(comision_intermediacion.alias("ci"), col("ci.DCOID") == col("pb.DCOID"), "left") \
            .join(iva_comision.alias("iva"), col("iva.DCOID") == col("pb.DCOID"), "left") \
            .join(acuerdo.alias("ac"), col("ac.DCOID") == col("pb.DCOID"), "left") \
            .join(tmp_iva_comision_recaudo.alias("coi"), col("coi.DCOID") == col("pb.DCOID"), "left") \
            .join(iva_comision_recaudo.alias("ivr"), col("ivr.DCOID") == col("pb.DCOID"), "left") \
            .join(reaseguro.alias("rea"), col("rea.DCOID") == col("pb.DCOID"), "left") \
            .join(nombre_reaseg.alias("nomre"), col("nomre.DCOID") == col("pb.DCOID"), "left") \
            .join(primcedaut.alias("prc"), col("prc.DCOID") == col("pb.DCOID"), "left") \
            .select(
                lit('Acsele').alias("ORIGEN"),
                col("ac.COMPANIA"),
                col("ac.RAMO"),
                col("ac.DESC_RAMO"),
                col("ac.CODIGO_PRODUCTO").alias("PRODUCTO"),
                col("ac.NOMBRE_PRODUCTO").alias("DESC_PRODUCTO"),
                col("ac.RAMO").alias("RAMO_TECNICO"),
                branch_expr.alias("DESC_RAMO_TECNICO"),
                col("pm.POLIZA_MATRIZ"),
                col("pb.POLIZA"),
                col("pb.SUCURSAL"),
                lit("").alias("MODULO"),
                col("ur.RISKUNIT_NUMBER").alias("UNIDAD_RIESGO"),
                col("ac.COBERTURA"),
                col("ac.DESC_COBERTURA"),
                col("ac.DCOID").alias("ENDOSO"),
                col("pb.FECHA_ENDOSO"),
                col("pb.FECHA_ENDOSO").alias("MESPRO"),
                col("pm.UNIDAD_NEGOCIO"),
                col("pb.FECHA_INICIO_POLIZA_MATRIZ"),
                col("pb.FECHA_FIN_POLIZA_MATRIZ"),
                col("ur.RISKUNIT_BEGINDATE").alias("FECHA_INICIO_COBERTURA"),
                col("ur.RISKUNIT_ENDDATE").alias("FECHA_FIN_COBERTURA"),
                col("t.ID_TOMADOR_POLIZA").alias("ID_TOMADOR_POLIZA_MATRIZ"),
                col("t.TOMADOR_POLIZA").alias("TOMADOR_POLIZA_MATRIZ"),
                col("t.ID_TOMADOR_POLIZA"),
                col("t.TOMADOR_POLIZA"),
                lit("D").alias("TIPO_COASEGURO"),
                lit(0).alias("PORCPART_ALFA"),
                col("pb.FRECUENCIA_PAGO"),
                coalesce(col("pe.PRIMA_EMITIDA"), lit(0)).alias("PRIMA_EMITIDA"),
                coalesce(expr("coi.ISSUEDPREMIUM - (coi.ISSUEDPREMIUM * (coi.PERCENTAGEPARTICALFA/100))"), lit(0)).alias("PRIMACED_COASEGURO"),
                coalesce(expr("(coi.ISSUEDPREMIUM * (coi.PERCENTAGEPARTICALFA/100))"), lit(0)).alias("PRIMAALFA"),
                coalesce(col("ci.PORC_COM_INTERMEDIACION"), lit(0)).alias("PORC_COM_INTERMEDIACION"),
                coalesce(col("coi.COMMISION_INTERMEDIARY"), lit(0)).alias("COMISION_INTERMEDIACION"),
                coalesce(col("iva.IVA_COMISION_INTERMEDIACION"), lit(0)).alias("IVA_COMISION_INTERMEDIACION"),
                coalesce(col("coi.PORCCOMRECAUDO"), lit(0)).alias("PORCCOMRECAUDO"),
                coalesce(col("coi.COMMISSIONCOLLECTION"), lit(0)).alias("COMISION_RECAUDO"),
                coalesce(col("ivr.IVA_COMISION_RECAUDO"), lit(0)).alias("IVA_COMISION_RECAUDO"),
                coalesce(col("rea.CODIGO_REASEGURADOR"), lit("")).alias("CODREASEG"),
                coalesce(col("nomre.NOMREASEG"), lit("")).alias("NOMREASEG"),
                coalesce(col("rea.CLASE_REASEGURO"), lit("")).alias("CLASE_REASEGURADOR"),
                coalesce(col("prc.PRIMACEDAUT"), lit(0)).alias("PRIMACEDAUT"),
                coalesce(col("rea.PRIMACEDAUT_INT"), lit(0)).alias("PRIMACEDAUT_INT"),
                coalesce(col("rea.PRIMACEDAUT_EXT"), lit(0)).alias("PRIMACEDAUT_EXT"),
                coalesce(col("prc.PRIMACEDFAC"), lit(0)).alias("PRIMACEDFAC"),
                coalesce(col("rea.PRIMACEDFAC_INT"), lit(0)).alias("PRIMACEDFAC_INT"),
                coalesce(col("rea.PRIMACEDFAC_EXT"), lit(0)).alias("PRIMACEDFAC_EXT"),
                coalesce(col("prc.COMISIONREASEGAUT"), lit(0)).alias("COMISIONREASEGAUT"),
                coalesce(col("rea.COMISIONREASEGAUT_INT"), lit(0)).alias("COMISIONREASEGAUT_INT"),
                coalesce(col("rea.COMISIONREASEGAUT_EXT"), lit(0)).alias("COMISIONREASEGAUT_EXT"),
                coalesce(col("prc.COMISIONREASEGFAC"), lit(0)).alias("COMISIONREASEGFAC"),
                coalesce(col("rea.COMISIONREASEGFAC_INT"), lit(0)).alias("COMISIONREASEGFAC_INT"),
                coalesce(col("rea.COMISIONREASEGFAC_EXT"), lit(0)).alias("COMISIONREASEGFAC_EXT"),
                lit('NULL').alias("GASTOS_EXP_PORCENTAJE"),
                lit('NULL').alias("GASTOS_EXP")
            )
        
        batch_time = time.time() - batch_start
        print(f"    ✓ Batch {batch_num} processing completed in {batch_time:.2f}s")
        
        return batch_result
        
    except Exception as e:
        print(f"    ✗ Error processing batch {batch_num}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def get_output_schema():
    """Define the output schema for empty DataFrame creation"""
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
    
    return StructType([
        StructField("ORIGEN", StringType(), True),
        StructField("COMPANIA", StringType(), True),
        StructField("RAMO", StringType(), True),
        StructField("DESC_RAMO", StringType(), True),
        StructField("PRODUCTO", StringType(), True),
        StructField("DESC_PRODUCTO", StringType(), True),
        StructField("RAMO_TECNICO", StringType(), True),
        StructField("DESC_RAMO_TECNICO", StringType(), True),
        StructField("POLIZA_MATRIZ", StringType(), True),
        StructField("POLIZA", StringType(), True),
        StructField("SUCURSAL", StringType(), True),
        StructField("MODULO", StringType(), True),
        StructField("UNIDAD_RIESGO", StringType(), True),
        StructField("COBERTURA", StringType(), True),
        StructField("DESC_COBERTURA", StringType(), True),
        StructField("ENDOSO", StringType(), True),
        StructField("FECHA_ENDOSO", DateType(), True),
        StructField("MESPRO", DateType(), True),
        StructField("UNIDAD_NEGOCIO", StringType(), True),
        StructField("FECHA_INICIO_POLIZA_MATRIZ", DateType(), True),
        StructField("FECHA_FIN_POLIZA_MATRIZ", DateType(), True),
        StructField("FECHA_INICIO_COBERTURA", DateType(), True),
        StructField("FECHA_FIN_COBERTURA", DateType(), True),
        StructField("ID_TOMADOR_POLIZA_MATRIZ", StringType(), True),
        StructField("TOMADOR_POLIZA_MATRIZ", StringType(), True),
        StructField("ID_TOMADOR_POLIZA", StringType(), True),
        StructField("TOMADOR_POLIZA", StringType(), True),
        StructField("TIPO_COASEGURO", StringType(), True),
        StructField("PORCPART_ALFA", IntegerType(), True),
        StructField("FRECUENCIA_PAGO", StringType(), True),
        StructField("PRIMA_EMITIDA", DecimalType(18,2), True),
        StructField("PRIMACED_COASEGURO", DecimalType(18,2), True),
        StructField("PRIMAALFA", DecimalType(18,2), True),
        StructField("PORC_COM_INTERMEDIACION", DecimalType(18,2), True),
        StructField("COMISION_INTERMEDIACION", DecimalType(18,2), True),
        StructField("IVA_COMISION_INTERMEDIACION", DecimalType(18,2), True),
        StructField("PORCCOMRECAUDO", DecimalType(18,2), True),
        StructField("COMISION_RECAUDO", DecimalType(18,2), True),
        StructField("IVA_COMISION_RECAUDO", DecimalType(18,2), True),
        StructField("CODREASEG", StringType(), True),
        StructField("NOMREASEG", StringType(), True),
        StructField("CLASE_REASEGURADOR", StringType(), True),
        StructField("PRIMACEDAUT", DecimalType(18,2), True),
        StructField("PRIMACEDAUT_INT", DecimalType(18,2), True),
        StructField("PRIMACEDAUT_EXT", DecimalType(18,2), True),
        StructField("PRIMACEDFAC", DecimalType(18,2), True),
        StructField("PRIMACEDFAC_INT", DecimalType(18,2), True),
        StructField("PRIMACEDFAC_EXT", DecimalType(18,2), True),
        StructField("COMISIONREASEGAUT", DecimalType(18,2), True),
        StructField("COMISIONREASEGAUT_INT", DecimalType(18,2), True),
        StructField("COMISIONREASEGAUT_EXT", DecimalType(18,2), True),
        StructField("COMISIONREASEGFAC", DecimalType(18,2), True),
        StructField("COMISIONREASEGFAC_INT", DecimalType(18,2), True),
        StructField("COMISIONREASEGFAC_EXT", DecimalType(18,2), True),
        StructField("GASTOS_EXP_PORCENTAJE", StringType(), True),
        StructField("GASTOS_EXP", StringType(), True)
    ])

def write_final_results(spark, final_df, total_start_time):
    """Write final results to S3 with monitoring"""
    print("\nWriting final result to S3...")
    step_start = time.time()
    
    # Get final count
    final_count = final_df.count()
    print(f"✓ Final dataset contains {final_count:,} records")
    
    # Write to S3
    final_df.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", "1000000") \
        .option("compression", "snappy") \
        .parquet("s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL/")
    print(f"✓ Data written to S3 in {time.time() - step_start:.2f}s")
    
    # Show sample results
    print("\nSample results:")
    final_df.show(5, truncate=False)
    
    # Processing summary
    total_time = time.time() - total_start_time
    print(f"\n{'='*60}")
    print(f"PROCESSING COMPLETED SUCCESSFULLY!")
    print(f"{'='*60}")
    print(f"Total processing time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    print(f"Total records processed: {final_count:,}")
    print(f"Average processing rate: {final_count/total_time:.0f} records/second")
    print(f"Output location: s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL/")
    print(f"Output format: Parquet with Snappy compression")
    print(f"{'='*60}")
    
    return final_df


# Complete the batch processing function
def complete_batch_processing(spark, final_df, total_start_time):
    """Complete the batch processing with final write"""
    try:
        return write_final_results(spark, final_df, total_start_time)
    except Exception as e:
        print(f"Error in processing: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def main():
    """Main function - works both as Glue job and notebook"""
    spark = None
    glueContext = None
    
    try:
        # Check if running as Glue job or notebook
        try:
            # Try to get job parameters (Glue job mode)
            args = getResolvedOptions(sys.argv, ['JOB_NAME'])
            job_name = args['JOB_NAME']
            print(f"Running as AWS Glue job: {job_name}")
            
            # Initialize Glue job
            spark, glueContext = create_spark_session()
            job = Job(glueContext)
            job.init(job_name, args)
            
        except Exception:
            # Running in notebook mode
            print("Running in notebook mode")
            spark, glueContext = create_spark_session()
        
        # Process data
        result_df = process_data_by_batches(spark)
        
        # Commit job if running as Glue job
        try:
            if 'job' in locals():
                job.commit()
        except:
            pass
            
        return result_df
        
    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if spark:
            spark.stop()

# For notebook execution
def run_notebook():
    """Function to run in notebook environment"""
    return main()

# Entry point
if __name__ == "__main__":
    main()

run_notebook()
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import date_format, col, split, explode, trim, when, expr, lit
# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRODUCTO") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


spark.sql("USE `datalake-dev-silver-catalog-db`")

syp_df = spark.sql("""
    SELECT SYP_VALUE
    FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
    WHERE SYP_PROPERTY = 'RRC.issuedPremium.earthquakeDocType'
""")

syp_exploded = syp_df.select(explode(split(col("SYP_VALUE"), ",")).alias("DESCRIPTION"))
syp_trimmed = syp_exploded.select(trim(col("DESCRIPTION")).alias("DESCRIPTION"))
syp_trimmed.createOrReplaceTempView("ISSUED_PREMIUM_DOCS")

bondholder_df = spark.sql("""
    SELECT SYP_VALUE
    FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
    WHERE SYP_PROPERTY = 'bondholder.rol'
""")
bondholder_exploded = bondholder_df.select(explode(split(col("SYP_VALUE"), ",")).alias("ROL_DESCRIPTION"))
bondholder_trimmed = bondholder_exploded.select(trim(col("ROL_DESCRIPTION")).alias("ROL_DESCRIPTION"))
bondholder_trimmed.createOrReplaceTempView("BONDHOLDER_ROLES")



sysprop_df_recaudo = spark.sql("""
    SELECT SYP_VALUE
    FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
    WHERE SYP_PROPERTY = 'RRC.commisionIntermediary'
""")

exploded_df_recaudo = sysprop_df_recaudo.select(explode(split(col("SYP_VALUE"), ",")).alias("DESCRIPTION"))
trimmed_df_recaudo = exploded_df_recaudo.select(trim(col("DESCRIPTION")).alias("DESCRIPTION"))
trimmed_df_recaudo.createOrReplaceTempView("COMM_INTERMEDIARY_DOCS_RECAUDO")


sysprop_df = spark.sql("""
    SELECT SYP_VALUE
    FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
    WHERE SYP_PROPERTY = 'RRC.commisionIntermediary'
""")

exploded_df = sysprop_df.select(explode(split(col("SYP_VALUE"), ",")).alias("DESCRIPTION"))
trimmed_df = exploded_df.select(trim(col("DESCRIPTION")).alias("DESCRIPTION"))
trimmed_df.createOrReplaceTempView("COMM_INTERMEDIARY_DOCS")


df_sysprop = spark.sql("SELECT * FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY") \
    .filter(col("SYP_PROPERTY").isin(
        "RRC.issuedPremium.earthquakeDocType",
        "RRC.commisionIntermediary",
        "RRC.commissionCollection.earthquakeDocType"
    ))

df_sysprop = spark.sql("SELECT * FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY") \
    .filter(col("SYP_PROPERTY").isin(
        "RRC.issuedPremium.earthquakeDocType",
        "RRC.commisionIntermediary",
        "RRC.commissionCollection.earthquakeDocType"
    ))

df_desc = df_sysprop.withColumn("DESCRIPTION", explode(split(col("SYP_VALUE"), ","))) \
    .withColumn("DESCRIPTION", trim(col("DESCRIPTION"))) \
    .withColumn("TIPO",
        when(col("SYP_PROPERTY") == "RRC.issuedPremium.earthquakeDocType", "ISSUED_PREMIUM")
       .when(col("SYP_PROPERTY") == "RRC.commisionIntermediary", "INTERMEDIARY")
       .when(col("SYP_PROPERTY") == "RRC.commissionCollection.earthquakeDocType", "COLLECTION")
    )

df_desc.createOrReplaceTempView("DOC_TYPES")

# -----------------------------
# Consulta con JOINs seguros para bruto
# -----------------------------
sql_bruto = """
SELECT
  OI.OPERATIONPK AS DCOID,
  SUM(CASE 
    WHEN DTY.TIPO = 'ISSUED_PREMIUM' AND DT.DTY_ISDEBIT = 1 THEN OI.AMOUNT
    WHEN DTY.TIPO = 'ISSUED_PREMIUM' AND DT.DTY_ISDEBIT = 0 THEN OI.AMOUNT * -1
    ELSE 0 END
  ) AS ISSUEDPREMIUM,
  SUM(CASE 
    WHEN DTY.TIPO = 'INTERMEDIARY' AND PP.SIMBOLO = 'Comision' THEN DET.VALUE
    ELSE 0 END
  ) AS COMMISION_INTERMEDIARY,
  SUM(CASE 
    WHEN DTY.TIPO = 'COLLECTION' AND PP.SIMBOLO = 'ComisionRecaudo' THEN OI.AMOUNT
    ELSE 0 END
  ) AS COMMISSIONCOLLECTION
FROM acsele_acsele_alfa_OPENITEM OI
JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON OI.DTY_ID = DT.DTY_ID
LEFT JOIN acsele_acsele_alfa_UAADETAIL DET ON DET.OPENITEMID = OI.OPENITEMID
LEFT JOIN acsele_acsele_alfa_PROPERTY PP ON PP.PROPERTYID = DET.PROPERTY
JOIN DOC_TYPES DTY ON DT.DTY_DESCRIPTION = DTY.DESCRIPTION
WHERE OI.OPM_SUBSTATUS <> 'notApplied' AND OI.operationpk >= 986536769
GROUP BY OI.OPERATIONPK
"""

df_bruto = spark.sql(sql_bruto)

# Derivados bruto
df_bruto = df_bruto \
    .withColumn("PERCENTAGEPARTICALFA", lit(100)) \
    .withColumn("ALFAPREMIUM", expr("ISSUEDPREMIUM * (PERCENTAGEPARTICALFA / 100)")) \
    .withColumn("PORCPART_ALFA", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
    .withColumn("PRIMACED_COASEGURO", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
    .withColumn("GROUP_PERCENTAGE", lit(100)) \
    .withColumn("BROKERAGECOMMISSION", expr("COMMISION_INTERMEDIARY * (GROUP_PERCENTAGE / 100)")) \
    .withColumn("PORCCOMRECAUDO", expr("(COMMISSIONCOLLECTION * 100) / ISSUEDPREMIUM")) \
    .withColumn("TIPO_REGISTRO", lit("BRUTO"))

# -----------------------------
# Consulta detalle reasegurador
# -----------------------------
sql_detalle = """
SELECT
  RUD.OPERATIONPK AS DCOID,
  RO.TOTALPREMIUM AS ISSUEDPREMIUM,
  ABS(SUM(ROG.PREMIUMAMOUNT) * 100 / RO.TOTALPREMIUM) AS GROUP_PERCENTAGE,
  0 AS PERCENTAGEPARTICALFA,
  'DETALLE' AS TIPO_REGISTRO
FROM acsele_acsele_alfa_REINSURANCEOPERATION RO
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO ROG ON ROG.OPERATIONID = RO.OPERATIONID
INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
INNER JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
INNER JOIN acsele_acsele_alfa_RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
WHERE RO.TOTALPREMIUM > 0 AND ROG.operationid >= 943602721
GROUP BY RUD.OPERATIONPK, RO.TOTALPREMIUM
"""

df_detalle = spark.sql(sql_detalle) \
    .withColumn("ALFAPREMIUM", expr("ISSUEDPREMIUM * (PERCENTAGEPARTICALFA / 100)")) \
    .withColumn("PORCPART_ALFA", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
    .withColumn("PRIMACED_COASEGURO", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
    .withColumn("COMMISION_INTERMEDIARY", lit(None).cast("double")) \
    .withColumn("COMMISSIONCOLLECTION", lit(None).cast("double")) \
    .withColumn("BROKERAGECOMMISSION", lit(None).cast("double")) \
    .withColumn("PORCCOMRECAUDO", lit(None).cast("double"))

# -----------------------------
# Unificación final
# -----------------------------
df_total = df_bruto.select(df_detalle.columns).unionByName(df_detalle)
df_total.createOrReplaceTempView("TMP_IVA_COMISION_RECAUDO")



sql_producto = """
WITH
cte_poliza_base AS (
    SELECT DISTINCT
        DCO.OPERATIONPK AS DCOID,
        CO.TIME_STAMP AS FECHA_ENDOSO,
        PP.NUMEROPOLIZAINPUT AS POLIZA,
        AP.PRODUCTID AS PRODUCTO,
        PP.OFICINAINPUT AS SUCURSAL,
        PP.FRECUENCIAPAGOINPUT AS FRECUENCIA_PAGO,
        DCO.INITIALDATE AS FECHA_INICIO_POLIZA_MATRIZ,
        DCO.FINISHDATE AS FECHA_FIN_POLIZA_MATRIZ
    FROM acsele_acsele_alfa_POLICYDCO DCO
    JOIN acsele_acsele_alfa_CONTEXTOPERATION CO ON CO.ID = DCO.OPERATIONPK
    JOIN acsele_acsele_alfa_PREPOLICY PP ON PP.PK = DCO.DCOID
    JOIN acsele_acsele_alfa_FINANCIALPLANDCO FPDCO ON FPDCO.AGREGATEDPARENTID = DCO.AGREGATEDOBJECTID AND FPDCO.OPERATIONPK = DCO.OPERATIONPK
    JOIN acsele_acsele_alfa_AGREGATEDPOLICY AP ON AP.AGREGATEDPOLICYID = DCO.AGREGATEDOBJECTID
    WHERE CO.id >= 986530136 AND CO.status = 2
),
cte_poliza_matriz AS (
    SELECT
        DISTINCT dco.OPERATIONPK AS DCOID,
        RAP.rsa_policyid,
        AGS.AST_NUMEROACU AS POLIZA_MATRIZ,
        RAP.rsa_ast_id,
        AV.ast_id,
        AV.dcoid AS ID,
        PREA.UNIDADNEGOCIOINPUT AS UNIDAD_NEGOCIO
    FROM acsele_acsele_alfa_POLICYDCO DCO  
    INNER JOIN acsele_acsele_alfa_STPS_RELATIONAGREEMENTPOLICY RAP ON RAP.RSA_POLICYID = DCO.AGREGATEDOBJECTID
    INNER JOIN acsele_acsele_alfa_STPS_AGREEMENTSTATICDAT AGS ON RAP.RSA_AST_ID = AGS.AST_ID
    INNER JOIN acsele_acsele_alfa_STAG_AGREEMENTVERSION AV ON AV.AST_ID = RAP.RSA_AST_ID 
    INNER JOIN acsele_acsele_alfa_PREAGREEMENT PREA ON PREA.PK = AV.DCOID
    WHERE RAP.rsa_policyid >= 183066469 AND AV.ast_id >= 73001
),
cte_unidad_riesgo AS (
    SELECT
        DCO.OPERATIONPK AS DCOID,
        PRU.pk,
        PRU.NUMUNIDRIESGOINPUT AS RISKUNIT_NUMBER,
        DCO.INITIALDATE AS RISKUNIT_BEGINDATE,
        DCO.FINISHDATE AS RISKUNIT_ENDDATE
    FROM acsele_acsele_alfa_PRERISKUNITS PRU
    INNER JOIN acsele_acsele_alfa_RISKUNITDCO DCO ON DCO.DCOID = PRU.PK
    WHERE PRU.pk >= 61652035099
),
cte_tomadores AS (
  SELECT
        DISTINCT PPD.OPERATIONPK AS DCOID,
        PTP.DOCUMENTOIDENTIDADINPUT AS ID_TOMADOR_POLIZA,
        PTP.NOMBREPREDETERMINADOINPUT AS TOMADOR_POLIZA
    FROM acsele_acsele_alfa_STPO_POLICYPARTICIPATION PP
    INNER JOIN acsele_acsele_alfa_STPO_POLICYPARTICIPATIONDCO PPD ON PP.AGREGATEDOBJECTID = PPD.AGREGATEDOBJECTID 
    INNER JOIN acsele_acsele_alfa_STTE_THIRDPARTY TP ON TP.TPT_ID = PP.THIRDPARTYID
    INNER JOIN acsele_acsele_alfa_PRETHIRDPARTY PTP ON PTP.PK = TP.IDDCO
    INNER JOIN acsele_acsele_alfa_STTS_ROLE RL ON RL.ROL_ID = PP.ROL_ID
    WHERE RL.ROL_DESCRIPTION IN (SELECT ROL_DESCRIPTION FROM BONDHOLDER_ROLES) AND PP.agregatedobjectid >= 474064566
),
cte_prima_emitida AS (
    SELECT
        OI.OPERATIONPK AS DCOID,
        SUM(CASE WHEN DT.DTY_ISDEBIT = 1 THEN CAST(OI.AMOUNT AS DECIMAL) ELSE (CAST(OI.AMOUNT AS DECIMAL)*-1) END) AS PRIMA_EMITIDA
    FROM acsele_acsele_alfa_OPENITEM OI
    JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
    WHERE OI.OPM_SUBSTATUS <> 'notApplied'
      AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM ISSUED_PREMIUM_DOCS)
    GROUP BY OI.OPERATIONPK
),
cte_comision_intermediacion AS (
    SELECT
        ppd.operationpk AS DCOID,
        pp.agregatedobjectid,  
        PI.PORCENTAJECOMISIONINPUT AS PORC_COM_INTERMEDIACION
    FROM acsele_acsele_alfa_STPO_POLICYPARTICIPATION PP
    INNER JOIN acsele_acsele_alfa_STPO_POLICYPARTICIPATIONDCO PPD ON PP.AGREGATEDOBJECTID = PPD.AGREGATEDOBJECTID
    INNER JOIN acsele_acsele_alfa_PARTICIPACIONINTERMEDIARIOS PI ON PI.PK = PPD.DCOID
    WHERE pp.agregatedobjectid >= 474064566
    and ppd.operationpk >= 986530136
   
),
cte_nombre_reaseg AS (
    SELECT
        DISTINCT (RUD.DCOID) AS DCOID,
        RO.operationid, 
        ROD.THIRDPARTYID AS THIRDPARTY_ID, 
        ROD.THIRDPARTYNAME AS NOMREASEG,
        ROD.PARTICIPATIONID AS PARTICIPATION_ID, 
        SUM(ROD.ROD_PRORATEDPREMIUM) AS DETAIL_PREMIUM
    FROM acsele_acsele_alfa_REINSURANCEOPERATION RO
    INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO ROG ON ROG.OPERATIONID = RO.OPERATIONID
    INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
    INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONDETAIL ROD ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
    INNER JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
    INNER JOIN acsele_acsele_alfa_RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
    WHERE ROC.CONTRACTTYPE = 'MandatoryFacultativeContract' AND ro.operationid >= 943602721 AND roc.reinsurancegroupinfoid >= 674470981
    GROUP BY ROD.THIRDPARTYID, ROD.THIRDPARTYNAME, ROD.PARTICIPATIONID, RUD.OPERATIONPK, RUD.DCOID, RO.operationid
   
),
cte_iva_comision_recaudo AS (
    SELECT
        OI.OPERATIONPK AS DCOID,
        SUM(CAST(DET.VALUE AS DECIMAL)) AS IVA_COMISION_RECAUDO
    FROM acsele_acsele_alfa_OPENITEM OI
    INNER JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
    INNER JOIN acsele_acsele_alfa_UAADETAIL DET ON DET.OPENITEMID = OI.OPENITEMID
    INNER JOIN acsele_acsele_alfa_PROPERTY PP ON PP.PROPERTYID = CAST(DET.PROPERTY as DECIMAL)
    WHERE OI.OPM_SUBSTATUS <> 'notApplied'
    AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM COMM_INTERMEDIARY_DOCS_RECAUDO)
    AND PP.SIMBOLO = 'IvaComision'
    GROUP BY OI.OPERATIONPK
),
cte_prima_reaseguro AS (
    SELECT
        RUD.OPERATIONPK AS DCOID,
        RO.TOTALPREMIUM AS ISSUEDPREMIUM,
        ABS(SUM(ROG.PREMIUMAMOUNT) * 100 / RO.TOTALPREMIUM) AS GROUP_PERCENTAGE,
        0 AS PERCENTAGEPARTICALFA,
        'DETALLE' AS TIPO_REGISTRO
    FROM acsele_acsele_alfa_REINSURANCEOPERATION RO
    INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO ROG ON ROG.OPERATIONID = RO.OPERATIONID
    INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
    INNER JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
    INNER JOIN acsele_acsele_alfa_RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
    WHERE RO.TOTALPREMIUM > 0 AND ROG.operationid >= 943602721
    GROUP BY RUD.OPERATIONPK, RO.TOTALPREMIUM
),
cte_reaseguro AS (
    WITH detail_premium AS (
        SELECT
            ROD.THIRDPARTYID,
            ROD.THIRDPARTYNAME,
            ROD.PARTICIPATIONID,
            SUM(ROD.ROD_PRORATEDPREMIUM) AS DETAIL_PREMIUM
        FROM acsele_acsele_alfa_reinsuranceoperation RO
        INNER JOIN acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG ON ROG.OPERATIONID = RO.OPERATIONID
        INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
        INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail ROD ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
        INNER JOIN acsele_acsele_alfa_ri_reinsurancegroup RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
        GROUP BY ROD.THIRDPARTYID, ROD.THIRDPARTYNAME, ROD.PARTICIPATIONID
    ),
    reinsurance_commission AS (
        SELECT
            ROD.THIRDPARTYID,
            ROD.THIRDPARTYNAME,
            ROD.PARTICIPATIONID,
            ROD.REINSURANCEOPERATIONDETAILID,
            SUM(PT.ROPP_VALUE) AS REINSURANCE_COMMISSION
        FROM acsele_acsele_alfa_reinsuranceoperationproperty PT
        INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail ROD
            ON PT.ROD_ID = ROD.REINSURANCEOPERATIONDETAILID
        WHERE PT.ROPP_NAME IN ('ComPriCedida', 'ComPriCedidaInd')
        GROUP BY ROD.REINSURANCEOPERATIONDETAILID, ROD.THIRDPARTYID, ROD.THIRDPARTYNAME, ROD.PARTICIPATIONID
    )
    SELECT
        DISTINCT RUD.OPERATIONPK AS DCOID,
        RA.CLASEREASEGUROINPUT AS CLASE_REASEGURO,
        RA.CODIGOREASEGURADORINPUT AS CODIGO_REASEGURADOR,
        ROD.THIRDPARTYID,
        ROD.THIRDPARTYNAME,
        ROD.PARTICIPATIONID,
        IF(RA.CLASEREASEGUROINPUT = 'Interior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDAUT_INT,
        IF(RA.CLASEREASEGUROINPUT = 'Exterior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDAUT_EXT,
        IF(RA.CLASEREASEGUROINPUT = 'Interior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDFAC_INT,
        IF(RA.CLASEREASEGUROINPUT = 'Exterior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDFAC_EXT,
        IF(RA.CLASEREASEGUROINPUT = 'Interior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGAUT_INT,
        IF(RA.CLASEREASEGUROINPUT = 'Exterior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGAUT_EXT,
        IF(RA.CLASEREASEGUROINPUT = 'Interior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGFAC_INT,
        IF(RA.CLASEREASEGUROINPUT = 'Exterior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGFAC_EXT
        FROM acsele_acsele_alfa_reinsuranceoperationdetail ROD
    RIGHT JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC ON ROC.REINOPERCOMPONENTID = ROD.REINOPERCOMPONENTID
    JOIN acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG ON ROG.REINSURANCEGROUPINFOID = ROC.REINSURANCEGROUPINFOID
    JOIN acsele_acsele_alfa_reinsuranceoperation RO ON RO.OPERATIONID = ROG.OPERATIONID
    JOIN acsele_acsele_alfa_riskunitdco RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
    JOIN acsele_acsele_alfa_thirdpartyrole TPR ON TPR.THIRDPARTYID = ROD.THIRDPARTYID
    JOIN acsele_acsele_alfa_reasegurador RA ON RA.PK = TPR.IDDCOROLE
    LEFT JOIN detail_premium dp
    ON dp.THIRDPARTYID = ROD.THIRDPARTYID
    AND dp.PARTICIPATIONID = ROD.PARTICIPATIONID
    LEFT JOIN reinsurance_commission rc
    ON rc.THIRDPARTYID = ROD.THIRDPARTYID
    AND rc.PARTICIPATIONID = ROD.PARTICIPATIONID
    and rc.REINSURANCEOPERATIONDETAILID = ROD.REINSURANCEOPERATIONDETAILID
),
cte_acuerdo AS (
    SELECT DISTINCT
        CDCO.OPERATIONPK AS DCOID,
        PP_PROD.COMPANIAINPUT AS COMPANIA,
        REPLACE(PP_PROD.COMPANIAVALUE, '.0', '') AS CODIGO_COMPANIA,
        PP_PROD.DESCRIPCIONPRODUCTOINPUT AS NOMBRE_PRODUCTO,
        PP_PROD.CODIGOPRODUCTOINPUT AS CODIGO_PRODUCTO,
        PP_POL.NUMEROPOLIZAINPUT AS NUMERO_POLIZA,
        PP_POL.POLIZAMAESTRAINPUT AS ACUERDO,
        REPLACE(CB.COBCODIGOCOBERTURAINPUT, '.0', '') AS CODIGO_COBERTURA,
        CB.COBDESCRIPCIONCOBERTINPUT AS COBERTURA,
        (case when CB.COBACUMULACIONINPUT IN ('A', 'P') then 'SI' else 'NO' end) AS ACUMULA,
        REPLACE(CB.COBRAMOREASEGUROINPUT, '.0', '') AS RAMO_REASEGURO,
        REPLACE(CB.COBRAMOCONTABLEINPUT, '.0', '') AS RAMO_CONTABLE,
        PP_PROD.RAMOINPUT AS DESC_RAMO_CONTABLE,
        RG.DESCRIPTION AS CODIGO_RAMO_REASEGURO,
        RK.REK_CODE AS GRUPO_REASEGURO,
        RK.REK_DESCRIPTION AS NOMBRE_GRUPO_REASEGURO,
        NVL(CB.COBPRIMAINPUT, '0') AS PRIMA,
        CB.COBPRIMAREASEGUROINPUT AS PRIMA_REASEGURO
    FROM acsele_acsele_alfa_COVERAGEDCO CDCO
    JOIN acsele_acsele_alfa_COBERTURA CB ON CB.PK = CDCO.DCOID
    LEFT JOIN acsele_acsele_alfa_POLICYDCO PDCO ON PDCO.OPERATIONPK = CDCO.OPERATIONPK
    LEFT JOIN acsele_acsele_alfa_PREPOLICY PP_POL ON PP_POL.PK = PDCO.DCOID
    LEFT JOIN acsele_acsele_alfa_AGREGATEDPOLICY AP ON AP.AGREGATEDPOLICYID = PDCO.AGREGATEDOBJECTID
    LEFT JOIN acsele_acsele_alfa_PRODUCT PD ON PD.PRODUCTID = AP.PRODUCTID
    LEFT JOIN acsele_acsele_alfa_PREPRODUCT PP_PROD ON PP_PROD.PK = PD.PRO_DCOID
    LEFT JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.CODE = REPLACE(CB.COBRAMOREASEGUROINPUT, '.0', '')
    LEFT JOIN acsele_acsele_alfa_STRI_REINSURANCEKIND RK ON RK.REKIND_ID = RG.REKIND_ID
    JOIN acsele_acsele_alfa_CONTEXTOPERATION CO ON CO.ID = PDCO.OPERATIONPK
    WHERE CO.id >= 986530136 and CO.status = 2
),
cte_primcedaut AS (
    WITH premium_detail AS (
        SELECT
            ROG2.OPERATIONID,
            ROC2.CONTRACTTYPE,
            SUM(RD.ROD_PRORATEDPREMIUM) AS SUM_PRIMA
        FROM acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG2
        INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC2
            ON ROC2.REINSURANCEGROUPINFOID = ROG2.REINSURANCEGROUPINFOID
        INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail RD
            ON RD.REINOPERCOMPONENTID = ROC2.REINOPERCOMPONENTID
        GROUP BY ROG2.OPERATIONID, ROC2.CONTRACTTYPE
),
reinsurance_commission AS (
  SELECT
    ROG2.OPERATIONID,
    ROC2.CONTRACTTYPE,
    SUM(PT.ROPP_VALUE) AS SUM_COMISION
  FROM acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG2
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC2
    ON ROC2.REINSURANCEGROUPINFOID = ROG2.REINSURANCEGROUPINFOID
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail RD
    ON RD.REINOPERCOMPONENTID = ROC2.REINOPERCOMPONENTID
  INNER JOIN acsele_acsele_alfa_reinsuranceoperationproperty PT
    ON PT.ROD_ID = RD.REINSURANCEOPERATIONDETAILID
  WHERE PT.ROPP_NAME IN ('ComPriCedida', 'ComPriCedidaInd')
  GROUP BY ROG2.OPERATIONID, ROC2.CONTRACTTYPE
)
    SELECT
        DISTINCT PLO.DCOID,
        PLO.OPERATIONPK,
        RO.OPERATIONID,
        ROC.REINSURANCEGROUPINFOID,
        ROD.REINOPERCOMPONENTID,
        RG.REINSURANCEGROUPID,
        IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0,dp_aut.SUM_PRIMA) AS PRIMACEDAUT,
        IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),dp_fac.SUM_PRIMA, 0) AS PRIMACEDFAC,
        IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0, rc_aut.SUM_COMISION) AS COMISIONREASEGAUT,
        IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),rc_fac.SUM_COMISION, 0) AS COMISIONREASEGFAC
    FROM acsele_acsele_alfa_reinsuranceoperation RO
    INNER JOIN acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG
    ON ROG.OPERATIONID = RO.OPERATIONID
    INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC
    ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
    INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail ROD
    ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
    INNER JOIN acsele_acsele_alfa_ri_reinsurancegroup RG
    ON RG.REINSURANCEGROUPID = ROG.GROUPID
    INNER JOIN acsele_acsele_alfa_policydco PLO
    ON RO.cor_id = PLO.operationpk
    LEFT JOIN premium_detail dp_aut
    ON dp_aut.OPERATIONID = RO.OPERATIONID AND dp_aut.CONTRACTTYPE NOT IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
    LEFT JOIN premium_detail dp_fac
    ON dp_fac.OPERATIONID = RO.OPERATIONID AND dp_fac.CONTRACTTYPE IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
    LEFT JOIN reinsurance_commission rc_aut
    ON rc_aut.OPERATIONID = RO.OPERATIONID AND rc_aut.CONTRACTTYPE NOT IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
    LEFT JOIN reinsurance_commission rc_fac
    ON rc_fac.OPERATIONID = RO.OPERATIONID AND rc_fac.CONTRACTTYPE IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
   
),
cte_iva_comision_intermediacion AS (
    SELECT
        OI.OPERATIONPK AS DCOID,
        SUM(CAST(DET.VALUE AS DECIMAL)) AS IVA_COMISION_INTERMEDIACION
    FROM acsele_acsele_alfa_OPENITEM OI
    JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
    JOIN acsele_acsele_alfa_UAADETAIL DET ON DET.OPENITEMID = OI.OPENITEMID
    JOIN acsele_acsele_alfa_PROPERTY PP ON PP.PROPERTYID = CAST(DET.PROPERTY AS BIGINT)
    WHERE OI.OPM_SUBSTATUS <> 'notApplied'
      AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM COMM_INTERMEDIARY_DOCS)
      AND PP.SIMBOLO = 'IvaComision'
    GROUP BY OI.OPERATIONPK
)
 SELECT
    'Acsele' AS ORIGEN,
    ac.COMPANIA,
    ac.RAMO_CONTABLE AS RAMO,
    ac.DESC_RAMO_CONTABLE AS DESC_RAMO,
    ac.CODIGO_PRODUCTO AS PRODUCTO,
    ac.NOMBRE_PRODUCTO AS DESC_PRODUCTO,
    ac.RAMO_CONTABLE AS RAMO_TECNICO,
    CASE ac.RAMO_CONTABLE
        WHEN '1' THEN 'AUTOS'
        WHEN '3' THEN 'EQUIPO ELECTRONICO'
        WHEN '4' THEN 'CUMPLIMIENTO'
        WHEN '5' THEN 'INCENDIO'
        WHEN '6' THEN 'LUCRO'
        WHEN '7' THEN 'MANEJO'
        WHEN '8' THEN 'ROTURA MAQUINARIA'
        WHEN '9' THEN 'CASCO'
        WHEN '10' THEN 'RESPONSABILIDAD'
        WHEN '12' THEN 'SUSTRACCION'
        WHEN '13' THEN 'TERREMOTO'
        WHEN '14' THEN 'TODO RIESGO'
        WHEN '15' THEN 'TRANSPORTE'
        WHEN '16' THEN 'VIDRIOS'
        WHEN '18' THEN 'ACCIDENTES PERSONALES'
        WHEN '20' THEN 'VIDA GRUPO NO CONTRIBUTIVO'
        WHEN '22' THEN 'DEUDORES'
        WHEN '24' THEN 'DESEMPLEO'
        WHEN '25' THEN 'HOGAR'
        WHEN '30' THEN 'EXEQUIAS'
        WHEN '31' THEN 'PREVISIONALES'
        WHEN '34' THEN 'SALUD'
        WHEN '40' THEN 'VIDA INDIVIDUAL'
        WHEN '44' THEN 'JUDICIAL'
        WHEN '50' THEN 'RIESGOS PROFESIONALES'
        WHEN '51' THEN 'RENTAS VITALICIAS'
        WHEN '52' THEN 'RENTAS TASA VARIABLE'
        WHEN '70' THEN 'VIDA GRUPO CONTRIBUTIVO'
        WHEN '95' THEN 'MONTAJE'
        WHEN '96' THEN 'EQUIPO Y MAQUINARIA'
  END AS DESC_RAMO_TECNICO,
    pm.POLIZA_MATRIZ,
    pb.POLIZA,
    pb.SUCURSAL,
    '' AS MODULO,
    ur.RISKUNIT_NUMBER AS UNIDAD_RIESGO,
    ac.CODIGO_COBERTURA AS COBERTURA,
    ac.COBERTURA AS DESC_COBERTURA,
    ac.DCOID AS ENDOSO,
    pb.FECHA_ENDOSO,
    pb.FECHA_ENDOSO AS MESPRO,
    pm.UNIDAD_NEGOCIO,
    pb.FECHA_INICIO_POLIZA_MATRIZ,
    pb.FECHA_FIN_POLIZA_MATRIZ,
    ur.RISKUNIT_BEGINDATE AS FECHA_INICIO_COBERTURA,
    ur.RISKUNIT_ENDDATE AS FECHA_FIN_COBERTURA,
    t.ID_TOMADOR_POLIZA AS ID_TOMADOR_POLIZA_MATRIZ,
    t.TOMADOR_POLIZA AS TOMADOR_POLIZA_MATRIZ,
    t.ID_TOMADOR_POLIZA AS ID_TOMADOR_POLIZA,
    t.TOMADOR_POLIZA,
    'D' AS TIPO_COASEGURO,
    0 as PORCPART_ALFA,
    pb.FRECUENCIA_PAGO,
    pe.PRIMA_EMITIDA,
    pre.ISSUEDPREMIUM - (pre.ISSUEDPREMIUM * (pre.PERCENTAGEPARTICALFA/100)) AS PRIMACED_COASEGURO,
    (pre.ISSUEDPREMIUM * (pre.PERCENTAGEPARTICALFA/100)) AS PRIMAALFA,
    ci.PORC_COM_INTERMEDIACION,
    coi.COMMISION_INTERMEDIARY AS COMISION_INTERMEDIACION,
    iva.IVA_COMISION_INTERMEDIACION,
    coi.PORCCOMRECAUDO AS PORCCOMRECAUDO,
    coi.COMMISSIONCOLLECTION AS COMISION_RECAUDO,
    ivr.IVA_COMISION_RECAUDO AS IVA_COMISION_RECAUDO,
    rea.CODIGO_REASEGURADOR AS CODREASEG,
    nomre.NOMREASEG AS NOMREASEG,
    rea.CLASE_REASEGURO AS CLASE_REASEGURADOR,
    prc.PRIMACEDAUT AS PRIMACEDAUT,
    rea.PRIMACEDAUT_INT AS PRIMACEDAUT_INT,
    rea.PRIMACEDAUT_EXT AS PRIMACEDAUT_EXT,
    prc.PRIMACEDFAC AS PRIMACEDFAC,
    rea.PRIMACEDFAC_INT AS PRIMACEDFAC_INT,
    rea.PRIMACEDFAC_EXT AS PRIMACEDFAC_EXT,
    prc.COMISIONREASEGAUT AS COMISIONREASEGAUT,
    rea.COMISIONREASEGAUT_INT AS COMISIONREASEGAUT_INT,
    rea.COMISIONREASEGAUT_EXT AS COMISIONREASEGAUT_EXT,
    prc.COMISIONREASEGFAC AS COMISIONREASEGFAC,
    rea.COMISIONREASEGFAC_INT AS COMISIONREASEGFAC_INT,
    rea.COMISIONREASEGFAC_EXT AS COMISIONREASEGFAC_EXT,
    'NULL' AS GASTOS_EXP_PORCENTAJE,
    'NULL' AS GASTOS_EXP
FROM cte_poliza_base pb
LEFT JOIN cte_poliza_matriz pm ON pm.DCOID = pb.DCOID
LEFT JOIN cte_unidad_riesgo ur ON ur.DCOID = pb.DCOID
LEFT JOIN cte_tomadores t ON t.DCOID = pb.DCOID
LEFT JOIN cte_prima_emitida pe ON pe.DCOID = pb.DCOID
LEFT JOIN cte_comision_intermediacion ci ON ci.DCOID = pb.DCOID
LEFT JOIN cte_iva_comision_intermediacion iva ON iva.DCOID = pb.DCOID
LEFT JOIN cte_acuerdo ac ON ac.DCOID = pb.DCOID
LEFT JOIN cte_prima_reaseguro pre ON pre.DCOID = pb.DCOID
LEFT JOIN TMP_IVA_COMISION_RECAUDO coi ON coi.DCOID = pb.DCOID
LEFT JOIN cte_iva_comision_recaudo ivr ON ivr.DCOID = pb.DCOID
LEFT JOIN cte_reaseguro rea ON rea.DCOID = pb.DCOID
LEFT JOIN cte_nombre_reaseg nomre ON nomre.DCOID = pb.DCOID
LEFT JOIN cte_primcedaut prc ON prc.DCOID = pb.DCOID
--WHERE pb.DCOID = 986530143
GROUP BY
    pb.DCOID,
    ac.COMPANIA,
    ac.RAMO_CONTABLE,
    ac.DESC_RAMO_CONTABLE,
    ac.CODIGO_PRODUCTO,
    ac.NOMBRE_PRODUCTO,
    pm.POLIZA_MATRIZ,
    pb.POLIZA,
    pb.SUCURSAL,
    ur.RISKUNIT_NUMBER,
    ac.CODIGO_COBERTURA,
    ac.COBERTURA,
    ac.DCOID,
    pb.FECHA_ENDOSO,
    pb.FECHA_INICIO_POLIZA_MATRIZ,
    pb.FECHA_FIN_POLIZA_MATRIZ,
    ur.RISKUNIT_BEGINDATE,
    ur.RISKUNIT_ENDDATE,
    ac.ACUERDO,
    pb.FRECUENCIA_PAGO,
    pre.ISSUEDPREMIUM,
    pre.PERCENTAGEPARTICALFA,
    pre.GROUP_PERCENTAGE,
    ci.PORC_COM_INTERMEDIACION,
    rea.CODIGO_REASEGURADOR,
    nomre.NOMREASEG,
    rea.CLASE_REASEGURO,
    prc.PRIMACEDAUT,
    rea.PRIMACEDAUT_INT,
    rea.PRIMACEDAUT_EXT,
    prc.PRIMACEDFAC,
    rea.PRIMACEDFAC_INT,
    rea.PRIMACEDFAC_EXT,
    prc.COMISIONREASEGAUT,
    rea.COMISIONREASEGAUT_INT,
    rea.COMISIONREASEGAUT_EXT,
    prc.COMISIONREASEGFAC,
    rea.COMISIONREASEGFAC_INT,
    rea.COMISIONREASEGFAC_EXT,
    pm.UNIDAD_NEGOCIO,
    pe.PRIMA_EMITIDA,
    coi.COMMISION_INTERMEDIARY,
    iva.IVA_COMISION_INTERMEDIACION,
    coi.PORCCOMRECAUDO,
    coi.COMMISSIONCOLLECTION,
    ivr.IVA_COMISION_RECAUDO,
    t.ID_TOMADOR_POLIZA,
    t.TOMADOR_POLIZA
"""
df_producto = spark.sql(sql_producto)
#df_producto.createOrReplaceTempView("TMP_PRODUCTO")
#df_producto.show(5)
from pyspark.sql.functions import col, count, when, lit
from pyspark.sql import DataFrame
df_producto.write.mode("overwrite").parquet("s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL/TEST_CAMPO/")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    date_format, col, split, explode, trim, when, expr, lit,
    count, abs, sum, coalesce, cast
)
from pyspark.sql.types import DecimalType
import time

# =============================================================================
# CONFIGURACIÓN Y SETUP
# =============================================================================

def create_spark_session():
    """
    Creates and configures the Spark session for the PCR Tradicional process
    """
    spark = SparkSession.builder \
        .appName("Campo_PRODUCTO_Modular") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.sql.shuffle.partitions", "400") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.default.parallelism", "200") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .getOrCreate()
    
    # Set the database
    spark.sql("USE `datalake-dev-silver-catalog-db`")
    
    return spark

# S3 paths configuration
S3_BASE_PATH = "s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL/PRUEBA_MODULAR/"
S3_PATHS = {
    "policy": f"{S3_BASE_PATH}/policy",
    "risk_bondholder": f"{S3_BASE_PATH}/risk_bondholder",
    "premium_commission": f"{S3_BASE_PATH}/premium_commission",
    "reinsurance": f"{S3_BASE_PATH}/reinsurance",
    "agreement": f"{S3_BASE_PATH}/agreement",
    "final": f"{S3_BASE_PATH}/final"
}

def write_to_s3(df, path, mode="overwrite"):
    """Writes a DataFrame to S3 in parquet format"""
    df.write.mode(mode).parquet(path)
    print(f"Data written to: {path}")

def read_from_s3(spark, path):
    """Reads a parquet file from S3 into a DataFrame"""
    return spark.read.parquet(path)

# =============================================================================
# STEP 1: SYSTEM PROPERTIES PROCESSING
# =============================================================================

def process_system_properties(spark):
    """
    Processes system properties and creates temporary views
    """
    print("Processing system properties...")
    
    # Process ISSUED_PREMIUM_DOCS
    syp_df = spark.sql("""
        SELECT SYP_VALUE
        FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
        WHERE SYP_PROPERTY = 'RRC.issuedPremium.earthquakeDocType'
    """)
    
    syp_exploded = syp_df.select(explode(split(col("SYP_VALUE"), ",")).alias("DESCRIPTION"))
    syp_trimmed = syp_exploded.select(trim(col("DESCRIPTION")).alias("DESCRIPTION"))
    syp_trimmed.createOrReplaceTempView("ISSUED_PREMIUM_DOCS")
    
    # Process BONDHOLDER_ROLES
    bondholder_df = spark.sql("""
        SELECT SYP_VALUE
        FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
        WHERE SYP_PROPERTY = 'bondholder.rol'
    """)
    bondholder_exploded = bondholder_df.select(explode(split(col("SYP_VALUE"), ",")).alias("ROL_DESCRIPTION"))
    bondholder_trimmed = bondholder_exploded.select(trim(col("ROL_DESCRIPTION")).alias("ROL_DESCRIPTION"))
    bondholder_trimmed.createOrReplaceTempView("BONDHOLDER_ROLES")
    
    # Process COMM_INTERMEDIARY_DOCS_RECAUDO
    sysprop_df_recaudo = spark.sql("""
        SELECT SYP_VALUE
        FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
        WHERE SYP_PROPERTY = 'RRC.commisionIntermediary'
    """)
    
    exploded_df_recaudo = sysprop_df_recaudo.select(explode(split(col("SYP_VALUE"), ",")).alias("DESCRIPTION"))
    trimmed_df_recaudo = exploded_df_recaudo.select(trim(col("DESCRIPTION")).alias("DESCRIPTION"))
    trimmed_df_recaudo.createOrReplaceTempView("COMM_INTERMEDIARY_DOCS_RECAUDO")
    
    # Process COMM_INTERMEDIARY_DOCS
    sysprop_df = spark.sql("""
        SELECT SYP_VALUE
        FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY
        WHERE SYP_PROPERTY = 'RRC.commisionIntermediary'
    """)
    
    exploded_df = sysprop_df.select(explode(split(col("SYP_VALUE"), ",")).alias("DESCRIPTION"))
    trimmed_df = exploded_df.select(trim(col("DESCRIPTION")).alias("DESCRIPTION"))
    trimmed_df.createOrReplaceTempView("COMM_INTERMEDIARY_DOCS")
    
    # Process DOC_TYPES
    df_sysprop = spark.sql("SELECT * FROM acsele_acsele_alfa_STST_SYSTEMPROPERTY") \
        .filter(col("SYP_PROPERTY").isin(
            "RRC.issuedPremium.earthquakeDocType",
            "RRC.commisionIntermediary",
            "RRC.commissionCollection.earthquakeDocType"
        ))
    
    df_desc = df_sysprop.withColumn("DESCRIPTION", explode(split(col("SYP_VALUE"), ","))) \
        .withColumn("DESCRIPTION", trim(col("DESCRIPTION"))) \
        .withColumn("TIPO",
            when(col("SYP_PROPERTY") == "RRC.issuedPremium.earthquakeDocType", "ISSUED_PREMIUM")
           .when(col("SYP_PROPERTY") == "RRC.commisionIntermediary", "INTERMEDIARY")
           .when(col("SYP_PROPERTY") == "RRC.commissionCollection.earthquakeDocType", "COLLECTION")
        )
    
    df_desc.createOrReplaceTempView("DOC_TYPES")
    
    print("System properties processed and temporary views created")

# =============================================================================
# STEP 2: DOCUMENT TYPES PROCESSING
# =============================================================================

def process_document_types(spark):
    """
    Processes document types and creates bruto and detalle calculations
    """
    print("Processing document types...")
    
    # Consulta con JOINs seguros para bruto
    sql_bruto = """
    SELECT
      OI.OPERATIONPK AS DCOID,
      SUM(CASE 
        WHEN DTY.TIPO = 'ISSUED_PREMIUM' AND DT.DTY_ISDEBIT = 1 THEN OI.AMOUNT
        WHEN DTY.TIPO = 'ISSUED_PREMIUM' AND DT.DTY_ISDEBIT = 0 THEN OI.AMOUNT * -1
        ELSE 0 END
      ) AS ISSUEDPREMIUM,
      SUM(CASE 
        WHEN DTY.TIPO = 'INTERMEDIARY' AND PP.SIMBOLO = 'Comision' THEN DET.VALUE
        ELSE 0 END
      ) AS COMMISION_INTERMEDIARY,
      SUM(CASE 
        WHEN DTY.TIPO = 'COLLECTION' AND PP.SIMBOLO = 'ComisionRecaudo' THEN OI.AMOUNT
        ELSE 0 END
      ) AS COMMISSIONCOLLECTION
    FROM acsele_acsele_alfa_OPENITEM OI
    JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON OI.DTY_ID = DT.DTY_ID
    LEFT JOIN acsele_acsele_alfa_UAADETAIL DET ON DET.OPENITEMID = OI.OPENITEMID
    LEFT JOIN acsele_acsele_alfa_PROPERTY PP ON PP.PROPERTYID = DET.PROPERTY
    JOIN DOC_TYPES DTY ON DT.DTY_DESCRIPTION = DTY.DESCRIPTION
    WHERE OI.OPM_SUBSTATUS <> 'notApplied' AND OI.operationpk >= 986536769
    GROUP BY OI.OPERATIONPK
    """
    
    df_bruto = spark.sql(sql_bruto)
    
    # Derivados bruto
    df_bruto = df_bruto \
        .withColumn("PERCENTAGEPARTICALFA", lit(100)) \
        .withColumn("ALFAPREMIUM", expr("ISSUEDPREMIUM * (PERCENTAGEPARTICALFA / 100)")) \
        .withColumn("PORCPART_ALFA", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
        .withColumn("PRIMACED_COASEGURO", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
        .withColumn("GROUP_PERCENTAGE", lit(100)) \
        .withColumn("BROKERAGECOMMISSION", expr("COMMISION_INTERMEDIARY * (GROUP_PERCENTAGE / 100)")) \
        .withColumn("PORCCOMRECAUDO", expr("(COMMISSIONCOLLECTION * 100) / ISSUEDPREMIUM")) \
        .withColumn("TIPO_REGISTRO", lit("BRUTO"))
    
    # Consulta detalle reasegurador
    sql_detalle = """
    SELECT
      RUD.OPERATIONPK AS DCOID,
      RO.TOTALPREMIUM AS ISSUEDPREMIUM,
      ABS(SUM(ROG.PREMIUMAMOUNT) * 100 / RO.TOTALPREMIUM) AS GROUP_PERCENTAGE,
      0 AS PERCENTAGEPARTICALFA,
      'DETALLE' AS TIPO_REGISTRO
    FROM acsele_acsele_alfa_REINSURANCEOPERATION RO
    INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO ROG ON ROG.OPERATIONID = RO.OPERATIONID
    INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
    INNER JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
    INNER JOIN acsele_acsele_alfa_RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
    WHERE RO.TOTALPREMIUM > 0 AND ROG.operationid >= 943602721
    GROUP BY RUD.OPERATIONPK, RO.TOTALPREMIUM
    """
    
    df_detalle = spark.sql(sql_detalle) \
        .withColumn("ALFAPREMIUM", expr("ISSUEDPREMIUM * (PERCENTAGEPARTICALFA / 100)")) \
        .withColumn("PORCPART_ALFA", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
        .withColumn("PRIMACED_COASEGURO", expr("ISSUEDPREMIUM - ALFAPREMIUM")) \
        .withColumn("COMMISION_INTERMEDIARY", lit(None).cast("double")) \
        .withColumn("COMMISSIONCOLLECTION", lit(None).cast("double")) \
        .withColumn("BROKERAGECOMMISSION", lit(None).cast("double")) \
        .withColumn("PORCCOMRECAUDO", lit(None).cast("double"))
    
    # Unificación final
    df_total = df_bruto.select(df_detalle.columns).unionByName(df_detalle)
    df_total.createOrReplaceTempView("TMP_IVA_COMISION_RECAUDO")
    
    # Store in S3
    document_types_path = f"{S3_PATHS['premium_commission']}/document_types"
    write_to_s3(df_total, document_types_path)
    
    return df_total

# =============================================================================
# STEP 3: POLICY INFORMATION PROCESSING
# =============================================================================

def process_policy_info(spark):
    """
    Processes policy base and matrix information
    """
    print("Processing policy information...")
    
    # SELECT Poliza Base
    sql_poliza_base = """
    SELECT DISTINCT
        DCO.OPERATIONPK AS DCOID,
        CO.TIME_STAMP AS FECHA_ENDOSO,
        PP.NUMEROPOLIZAINPUT AS POLIZA,
        AP.PRODUCTID AS PRODUCTO,
        PP.OFICINAINPUT AS SUCURSAL,
        PP.FRECUENCIAPAGOINPUT AS FRECUENCIA_PAGO,
        DCO.INITIALDATE AS FECHA_INICIO_POLIZA_MATRIZ,
        DCO.FINISHDATE AS FECHA_FIN_POLIZA_MATRIZ
    FROM acsele_acsele_alfa_POLICYDCO DCO
    JOIN acsele_acsele_alfa_CONTEXTOPERATION CO ON CO.ID = DCO.OPERATIONPK
    JOIN acsele_acsele_alfa_PREPOLICY PP ON PP.PK = DCO.DCOID
    JOIN acsele_acsele_alfa_FINANCIALPLANDCO FPDCO ON FPDCO.AGREGATEDPARENTID = DCO.AGREGATEDOBJECTID AND FPDCO.OPERATIONPK = DCO.OPERATIONPK
    JOIN acsele_acsele_alfa_AGREGATEDPOLICY AP ON AP.AGREGATEDPOLICYID = DCO.AGREGATEDOBJECTID
    WHERE CO.id >= 986530136 AND CO.status = 2
    """
    
    df_poliza_base = spark.sql(sql_poliza_base)
    
    # SELECT Poliza Matriz
    sql_poliza_matriz = """
    SELECT
        DISTINCT dco.OPERATIONPK AS DCOID,
        RAP.rsa_policyid,
        AGS.AST_NUMEROACU AS POLIZA_MATRIZ,
        RAP.rsa_ast_id,
        AV.ast_id,
        AV.dcoid AS ID,
        PREA.UNIDADNEGOCIOINPUT AS UNIDAD_NEGOCIO
    FROM acsele_acsele_alfa_POLICYDCO DCO  
    INNER JOIN acsele_acsele_alfa_STPS_RELATIONAGREEMENTPOLICY RAP ON RAP.RSA_POLICYID = DCO.AGREGATEDOBJECTID
    INNER JOIN acsele_acsele_alfa_STPS_AGREEMENTSTATICDAT AGS ON RAP.RSA_AST_ID = AGS.AST_ID
    INNER JOIN acsele_acsele_alfa_STAG_AGREEMENTVERSION AV ON AV.AST_ID = RAP.RSA_AST_ID 
    INNER JOIN acsele_acsele_alfa_PREAGREEMENT PREA ON PREA.PK = AV.DCOID
    WHERE RAP.rsa_policyid >= 183066469 AND AV.ast_id >= 73001
    """
    
    df_poliza_matriz = spark.sql(sql_poliza_matriz)
    
    # Store in S3
    policy_base_path = f"{S3_PATHS['policy']}/policy_base"
    policy_matrix_path = f"{S3_PATHS['policy']}/policy_matrix"
    
    write_to_s3(df_poliza_base, policy_base_path)
    write_to_s3(df_poliza_matriz, policy_matrix_path)
    
    return df_poliza_base, df_poliza_matriz

# =============================================================================
# STEP 4: RISK AND BONDHOLDER PROCESSING
# =============================================================================

def process_risk_bondholder(spark):
    """
    Processes risk unit and bondholder information
    """
    print("Processing risk and bondholder information...")
    
    # SELECT Unidad Riesgo
    sql_unidad_riesgo = """
    SELECT
        DCO.OPERATIONPK AS DCOID,
        PRU.pk,
        PRU.NUMUNIDRIESGOINPUT AS RISKUNIT_NUMBER,
        DCO.INITIALDATE AS RISKUNIT_BEGINDATE,
        DCO.FINISHDATE AS RISKUNIT_ENDDATE
    FROM acsele_acsele_alfa_PRERISKUNITS PRU
    INNER JOIN acsele_acsele_alfa_RISKUNITDCO DCO ON DCO.DCOID = PRU.PK
    WHERE PRU.pk >= 61652035099
    """
    
    df_unidad_riesgo = spark.sql(sql_unidad_riesgo)
    
    # SELECT Tomadores
    sql_tomadores = """
    SELECT
        DISTINCT PPD.OPERATIONPK AS DCOID,
        PTP.DOCUMENTOIDENTIDADINPUT AS ID_TOMADOR_POLIZA,
        PTP.NOMBREPREDETERMINADOINPUT AS TOMADOR_POLIZA
    FROM acsele_acsele_alfa_STPO_POLICYPARTICIPATION PP
    INNER JOIN acsele_acsele_alfa_STPO_POLICYPARTICIPATIONDCO PPD ON PP.AGREGATEDOBJECTID = PPD.AGREGATEDOBJECTID 
    INNER JOIN acsele_acsele_alfa_STTE_THIRDPARTY TP ON TP.TPT_ID = PP.THIRDPARTYID
    INNER JOIN acsele_acsele_alfa_PRETHIRDPARTY PTP ON PTP.PK = TP.IDDCO
    INNER JOIN acsele_acsele_alfa_STTS_ROLE RL ON RL.ROL_ID = PP.ROL_ID
    WHERE RL.ROL_DESCRIPTION IN (SELECT ROL_DESCRIPTION FROM BONDHOLDER_ROLES) 
    AND PP.agregatedobjectid >= 474064566
    """
    
    df_tomadores = spark.sql(sql_tomadores)
    
    # Store in S3
    risk_unit_path = f"{S3_PATHS['risk_bondholder']}/risk_unit"
    bondholder_path = f"{S3_PATHS['risk_bondholder']}/bondholder"
    
    write_to_s3(df_unidad_riesgo, risk_unit_path)
    write_to_s3(df_tomadores, bondholder_path)
    
    return df_unidad_riesgo, df_tomadores

# =============================================================================
# STEP 5: PREMIUM PROCESSING
# =============================================================================

def process_premium_info(spark):
    """
    Processes premium emitida information
    """
    print("Processing premium information...")
    
    # SELECT Prima Emitida
    sql_prima_emitida = """
    SELECT
        OI.OPERATIONPK AS DCOID,
        SUM(CASE 
            WHEN DT.DTY_ISDEBIT = 1 THEN CAST(OI.AMOUNT AS DECIMAL) 
            ELSE (CAST(OI.AMOUNT AS DECIMAL)*-1) 
        END) AS PRIMA_EMITIDA
    FROM acsele_acsele_alfa_OPENITEM OI
    JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
    WHERE OI.OPM_SUBSTATUS <> 'notApplied'
      AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM ISSUED_PREMIUM_DOCS)
    GROUP BY OI.OPERATIONPK
    """
    
    df_prima_emitida = spark.sql(sql_prima_emitida)
    
    # Store in S3
    premium_path = f"{S3_PATHS['premium_commission']}/premium"
    write_to_s3(df_prima_emitida, premium_path)
    
    return df_prima_emitida

# =============================================================================
# STEP 6: COMMISSION PROCESSING
# =============================================================================

def process_commission_info(spark):
    """
    Processes commission intermediation and IVA information
    """
    print("Processing commission information...")
    
    # SELECT Comision Intermediacion
    sql_comision_intermediacion = """
    SELECT
        ppd.operationpk AS DCOID,
        pp.agregatedobjectid,  
        PI.PORCENTAJECOMISIONINPUT AS PORC_COM_INTERMEDIACION
    FROM acsele_acsele_alfa_STPO_POLICYPARTICIPATION PP
    INNER JOIN acsele_acsele_alfa_STPO_POLICYPARTICIPATIONDCO PPD ON PP.AGREGATEDOBJECTID = PPD.AGREGATEDOBJECTID
    INNER JOIN acsele_acsele_alfa_PARTICIPACIONINTERMEDIARIOS PI ON PI.PK = PPD.DCOID
    WHERE pp.agregatedobjectid >= 474064566
    and ppd.operationpk >= 986530136
    """
    
    df_comision_intermediacion = spark.sql(sql_comision_intermediacion)
    
    # SELECT IVA Comision Recaudo
    sql_iva_comision_recaudo = """
    SELECT
        OI.OPERATIONPK AS DCOID,
        SUM(CAST(DET.VALUE AS DECIMAL)) AS IVA_COMISION_RECAUDO
    FROM acsele_acsele_alfa_OPENITEM OI
    INNER JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
    INNER JOIN acsele_acsele_alfa_UAADETAIL DET ON DET.OPENITEMID = OI.OPENITEMID
    INNER JOIN acsele_acsele_alfa_PROPERTY PP ON PP.PROPERTYID = DET.PROPERTY
    WHERE OI.OPM_SUBSTATUS <> 'notApplied'
    AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM COMM_INTERMEDIARY_DOCS_RECAUDO)
    AND PP.SIMBOLO = 'IvaComision'
    GROUP BY OI.OPERATIONPK
    """
    
    df_iva_comision_recaudo = spark.sql(sql_iva_comision_recaudo)
    
    # SELECT IVA Comision Intermediacion
    sql_iva_comision_intermediacion = """
    SELECT
        OI.OPERATIONPK AS DCOID,
        SUM(CAST(DET.VALUE AS DECIMAL)) AS IVA_COMISION_INTERMEDIACION
    FROM acsele_acsele_alfa_OPENITEM OI
    JOIN acsele_acsele_alfa_STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
    JOIN acsele_acsele_alfa_UAADETAIL DET ON DET.OPENITEMID = OI.OPENITEMID
    JOIN acsele_acsele_alfa_PROPERTY PP ON PP.PROPERTYID = CAST(DET.PROPERTY AS BIGINT)
    WHERE OI.OPM_SUBSTATUS <> 'notApplied'
      AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM COMM_INTERMEDIARY_DOCS)
      AND PP.SIMBOLO = 'IvaComision'
    GROUP BY OI.OPERATIONPK
    """
    
    df_iva_comision_intermediacion = spark.sql(sql_iva_comision_intermediacion)
    
    # Store in S3
    commission_intermediation_path = f"{S3_PATHS['premium_commission']}/commission_intermediation"
    iva_recaudo_path = f"{S3_PATHS['premium_commission']}/iva_recaudo"
    iva_intermediation_path = f"{S3_PATHS['premium_commission']}/iva_intermediation"
    
    write_to_s3(df_comision_intermediacion, commission_intermediation_path)
    write_to_s3(df_iva_comision_recaudo, iva_recaudo_path)
    write_to_s3(df_iva_comision_intermediacion, iva_intermediation_path)
    
    return df_comision_intermediacion, df_iva_comision_recaudo, df_iva_comision_intermediacion

# =============================================================================
# STEP 7: PRIMA REASEGURO PROCESSING
# =============================================================================

def process_prima_reaseguro(spark):
    """
    Processes prima reaseguro information
    """
    print("Processing prima reaseguro...")
    
    # SELECT Prima Reaseguro
    sql_prima_reaseguro = """
    SELECT
        RUD.OPERATIONPK AS DCOID,
        RO.TOTALPREMIUM AS ISSUEDPREMIUM,
        ABS(SUM(ROG.PREMIUMAMOUNT) * 100 / RO.TOTALPREMIUM) AS GROUP_PERCENTAGE,
        0 AS PERCENTAGEPARTICALFA,
        'DETALLE' AS TIPO_REGISTRO
    FROM acsele_acsele_alfa_REINSURANCEOPERATION RO
    INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO ROG ON ROG.OPERATIONID = RO.OPERATIONID
    INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
    INNER JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
    INNER JOIN acsele_acsele_alfa_RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
    WHERE RO.TOTALPREMIUM > 0 AND ROG.operationid >= 943602721
    GROUP BY RUD.OPERATIONPK, RO.TOTALPREMIUM
    """
    
    df_prima_reaseguro = spark.sql(sql_prima_reaseguro)
    
    # Store in S3
    prima_reaseguro_path = f"{S3_PATHS['reinsurance']}/prima_reaseguro"
    write_to_s3(df_prima_reaseguro, prima_reaseguro_path)
    
    return df_prima_reaseguro

# =============================================================================
# STEP 8: REINSURANCE PROCESSING
# =============================================================================

def process_reinsurance_info(spark):
    """
    Processes reinsurance information including nombre reaseg and reaseguro details
    """
    print("Processing reinsurance information...")
    
    # SELECT Nombre Reaseg
    sql_nombre_reaseg = """
    SELECT
        DISTINCT (RUD.DCOID) AS DCOID,
        RO.operationid, 
        ROD.THIRDPARTYID AS THIRDPARTY_ID, 
        ROD.THIRDPARTYNAME AS NOMREASEG,
        ROD.PARTICIPATIONID AS PARTICIPATION_ID, 
        SUM(ROD.ROD_PRORATEDPREMIUM) AS DETAIL_PREMIUM
    FROM acsele_acsele_alfa_REINSURANCEOPERATION RO
    INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONGROUPINFO ROG ON ROG.OPERATIONID = RO.OPERATIONID
    INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONCOMPONENT ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
    INNER JOIN acsele_acsele_alfa_REINSURANCEOPERATIONDETAIL ROD ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
    INNER JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
    INNER JOIN acsele_acsele_alfa_RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
    WHERE ROC.CONTRACTTYPE = 'MandatoryFacultativeContract' 
    AND ro.operationid >= 943602721 
    AND roc.reinsurancegroupinfoid >= 674470981
    GROUP BY ROD.THIRDPARTYID, ROD.THIRDPARTYNAME, ROD.PARTICIPATIONID, RUD.OPERATIONPK, RUD.DCOID, RO.operationid
    """
    
    df_nombre_reaseg = spark.sql(sql_nombre_reaseg)
    
    # SELECT Reaseguro (complex query with CTEs)
    sql_reaseguro = """
    WITH detail_premium AS (
        SELECT
            ROD.THIRDPARTYID,
            ROD.THIRDPARTYNAME,
            ROD.PARTICIPATIONID,
            SUM(ROD.ROD_PRORATEDPREMIUM) AS DETAIL_PREMIUM
        FROM acsele_acsele_alfa_reinsuranceoperation RO
        INNER JOIN acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG ON ROG.OPERATIONID = RO.OPERATIONID
        INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
        INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail ROD ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
        INNER JOIN acsele_acsele_alfa_ri_reinsurancegroup RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
        GROUP BY ROD.THIRDPARTYID, ROD.THIRDPARTYNAME, ROD.PARTICIPATIONID
    ),
    reinsurance_commission AS (
        SELECT
            ROD.THIRDPARTYID,
            ROD.THIRDPARTYNAME,
            ROD.PARTICIPATIONID,
            ROD.REINSURANCEOPERATIONDETAILID,
            SUM(PT.ROPP_VALUE) AS REINSURANCE_COMMISSION
        FROM acsele_acsele_alfa_reinsuranceoperationproperty PT
        INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail ROD
            ON PT.ROD_ID = ROD.REINSURANCEOPERATIONDETAILID
        WHERE PT.ROPP_NAME IN ('ComPriCedida', 'ComPriCedidaInd')
        GROUP BY ROD.REINSURANCEOPERATIONDETAILID, ROD.THIRDPARTYID, ROD.THIRDPARTYNAME, ROD.PARTICIPATIONID
    )
    SELECT
        DISTINCT RUD.OPERATIONPK AS DCOID,
        RA.CLASEREASEGUROINPUT AS CLASE_REASEGURO,
        RA.CODIGOREASEGURADORINPUT AS CODIGO_REASEGURADOR,
        ROD.THIRDPARTYID,
        ROD.THIRDPARTYNAME,
        ROD.PARTICIPATIONID,
        IF(RA.CLASEREASEGUROINPUT = 'Interior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDAUT_INT,
        IF(RA.CLASEREASEGUROINPUT = 'Exterior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDAUT_EXT,
        IF(RA.CLASEREASEGUROINPUT = 'Interior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDFAC_INT,
        IF(RA.CLASEREASEGUROINPUT = 'Exterior', dp.DETAIL_PREMIUM, 0) AS PRIMACEDFAC_EXT,
        IF(RA.CLASEREASEGUROINPUT = 'Interior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGAUT_INT,
        IF(RA.CLASEREASEGUROINPUT = 'Exterior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGAUT_EXT,
        IF(RA.CLASEREASEGUROINPUT = 'Interior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGFAC_INT,
        IF(RA.CLASEREASEGUROINPUT = 'Exterior', rc.REINSURANCE_COMMISSION, 0) AS COMISIONREASEGFAC_EXT
    FROM acsele_acsele_alfa_reinsuranceoperationdetail ROD
    RIGHT JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC ON ROC.REINOPERCOMPONENTID = ROD.REINOPERCOMPONENTID
    JOIN acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG ON ROG.REINSURANCEGROUPINFOID = ROC.REINSURANCEGROUPINFOID
    JOIN acsele_acsele_alfa_reinsuranceoperation RO ON RO.OPERATIONID = ROG.OPERATIONID
    JOIN acsele_acsele_alfa_riskunitdco RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
    JOIN acsele_acsele_alfa_thirdpartyrole TPR ON TPR.THIRDPARTYID = ROD.THIRDPARTYID
    JOIN acsele_acsele_alfa_reasegurador RA ON RA.PK = TPR.IDDCOROLE
    LEFT JOIN detail_premium dp
    ON dp.THIRDPARTYID = ROD.THIRDPARTYID
    AND dp.PARTICIPATIONID = ROD.PARTICIPATIONID
    LEFT JOIN reinsurance_commission rc
    ON rc.THIRDPARTYID = ROD.THIRDPARTYID
    AND rc.PARTICIPATIONID = ROD.PARTICIPATIONID
    and rc.REINSURANCEOPERATIONDETAILID = ROD.REINSURANCEOPERATIONDETAILID
    """
    
    df_reaseguro = spark.sql(sql_reaseguro)
    
    # Store in S3
    nombre_reaseg_path = f"{S3_PATHS['reinsurance']}/nombre_reaseg"
    reaseguro_path = f"{S3_PATHS['reinsurance']}/reaseguro"
    
    write_to_s3(df_nombre_reaseg, nombre_reaseg_path)
    write_to_s3(df_reaseguro, reaseguro_path)
    
    return df_nombre_reaseg, df_reaseguro

# =============================================================================
# STEP 9: PRIMCEDAUT PROCESSING
# =============================================================================

def process_primcedaut(spark):
    """
    Processes primcedaut information
    """
    print("Processing primcedaut...")
    
    # SELECT Primcedaut (complex query with CTEs)
    sql_primcedaut = """
    WITH premium_detail AS (
        SELECT
            ROG2.OPERATIONID,
            ROC2.CONTRACTTYPE,
            SUM(RD.ROD_PRORATEDPREMIUM) AS SUM_PRIMA
        FROM acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG2
        INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC2
            ON ROC2.REINSURANCEGROUPINFOID = ROG2.REINSURANCEGROUPINFOID
        INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail RD
            ON RD.REINOPERCOMPONENTID = ROC2.REINOPERCOMPONENTID
        GROUP BY ROG2.OPERATIONID, ROC2.CONTRACTTYPE
    ),
    reinsurance_commission AS (
      SELECT
        ROG2.OPERATIONID,
        ROC2.CONTRACTTYPE,
        SUM(PT.ROPP_VALUE) AS SUM_COMISION
      FROM acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG2
      INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC2
        ON ROC2.REINSURANCEGROUPINFOID = ROG2.REINSURANCEGROUPINFOID
      INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail RD
        ON RD.REINOPERCOMPONENTID = ROC2.REINOPERCOMPONENTID
      INNER JOIN acsele_acsele_alfa_reinsuranceoperationproperty PT
        ON PT.ROD_ID = RD.REINSURANCEOPERATIONDETAILID
      WHERE PT.ROPP_NAME IN ('ComPriCedida', 'ComPriCedidaInd')
      GROUP BY ROG2.OPERATIONID, ROC2.CONTRACTTYPE
    )
    SELECT
        DISTINCT PLO.DCOID,
        PLO.OPERATIONPK,
        RO.OPERATIONID,
        ROC.REINSURANCEGROUPINFOID,
        ROD.REINOPERCOMPONENTID,
        RG.REINSURANCEGROUPID,
        IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0,dp_aut.SUM_PRIMA) AS PRIMACEDAUT,
        IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),dp_fac.SUM_PRIMA, 0) AS PRIMACEDFAC,
        IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0, rc_aut.SUM_COMISION) AS COMISIONREASEGAUT,
        IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),rc_fac.SUM_COMISION, 0) AS COMISIONREASEGFAC
    FROM acsele_acsele_alfa_reinsuranceoperation RO
    INNER JOIN acsele_acsele_alfa_reinsuranceoperationgroupinfo ROG
    ON ROG.OPERATIONID = RO.OPERATIONID
    INNER JOIN acsele_acsele_alfa_reinsuranceoperationcomponent ROC
    ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
    INNER JOIN acsele_acsele_alfa_reinsuranceoperationdetail ROD
    ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
    INNER JOIN acsele_acsele_alfa_ri_reinsurancegroup RG
    ON RG.REINSURANCEGROUPID = ROG.GROUPID
    INNER JOIN acsele_acsele_alfa_policydco PLO
    ON RO.cor_id = PLO.operationpk
    LEFT JOIN premium_detail dp_aut
    ON dp_aut.OPERATIONID = RO.OPERATIONID AND dp_aut.CONTRACTTYPE NOT IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
    LEFT JOIN premium_detail dp_fac
    ON dp_fac.OPERATIONID = RO.OPERATIONID AND dp_fac.CONTRACTTYPE IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
    LEFT JOIN reinsurance_commission rc_aut
    ON rc_aut.OPERATIONID = RO.OPERATIONID AND rc_aut.CONTRACTTYPE NOT IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
    LEFT JOIN reinsurance_commission rc_fac
    ON rc_fac.OPERATIONID = RO.OPERATIONID AND rc_fac.CONTRACTTYPE IN ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract')
    """
    
    df_primcedaut = spark.sql(sql_primcedaut)
    
    # Store in S3
    primcedaut_path = f"{S3_PATHS['reinsurance']}/primcedaut"
    write_to_s3(df_primcedaut, primcedaut_path)
    
    return df_primcedaut

# =============================================================================
# STEP 10: AGREEMENT PROCESSING
# =============================================================================

def process_agreement_info(spark):
    """
    Processes agreement information
    """
    print("Processing agreement information...")
    
    # SELECT Acuerdo
    sql_acuerdo = """
    SELECT DISTINCT
        CDCO.OPERATIONPK AS DCOID,
        PP.COMPANIAINPUT AS COMPANIA,
        REPLACE(PP.COMPANIAVALUE, '.0', '') AS CODIGO_COMPANIA,
        PP.DESCRIPCIONPRODUCTOINPUT AS NOMBRE_PRODUCTO,
        PP.CODIGOPRODUCTOINPUT AS CODIGO_PRODUCTO,
        PPRE.NUMEROPOLIZAINPUT AS NUMERO_POLIZA,
        PPRE.POLIZAMAESTRAINPUT AS ACUERDO,
        REPLACE(CB.COBCODIGOCOBERTURAINPUT, '.0', '') AS COBERTURA,
        CB.COBDESCRIPCIONCOBERTINPUT AS DESC_COBERTURA,
        CASE WHEN CB.COBACUMULACIONINPUT IN ('A', 'P') THEN 'SI' ELSE 'NO' END AS ACUMULA,
        REPLACE(CB.COBRAMOREASEGUROINPUT, '.0', '') AS RAMO_REASEGURO,
        REPLACE(CB.COBRAMOCONTABLEINPUT, '.0', '') AS RAMO,
        PP.RAMOINPUT AS DESC_RAMO,
        RG.DESCRIPTION AS CODIGO_RAMO_REASEGURO,
        RK.REK_CODE AS GRUPO_REASEGURO,
        RK.REK_DESCRIPTION AS NOMBRE_GRUPO_REASEGURO,
        COALESCE(CB.COBPRIMAINPUT, '0') AS PRIMA,
        CB.COBPRIMAREASEGUROINPUT AS PRIMA_REASEGURO
    FROM acsele_acsele_alfa_COVERAGEDCO CDCO
    JOIN acsele_acsele_alfa_COBERTURA CB ON CB.PK = CDCO.DCOID
    LEFT JOIN acsele_acsele_alfa_POLICYDCO PDCO ON PDCO.OPERATIONPK = CDCO.OPERATIONPK
    JOIN acsele_acsele_alfa_CONTEXTOPERATION CO ON CO.ID = PDCO.OPERATIONPK
    LEFT JOIN acsele_acsele_alfa_AGREGATEDPOLICY AP ON AP.AGREGATEDPOLICYID = PDCO.AGREGATEDOBJECTID
    LEFT JOIN acsele_acsele_alfa_PRODUCT PD ON PD.PRODUCTID = AP.PRODUCTID
    LEFT JOIN acsele_acsele_alfa_PREPRODUCT PP ON PP.PK = PD.PRO_DCOID
    LEFT JOIN acsele_acsele_alfa_PREPOLICY PPRE ON PPRE.PK = PDCO.DCOID
    LEFT JOIN acsele_acsele_alfa_RI_REINSURANCEGROUP RG ON RG.CODE = REPLACE(CB.COBRAMOREASEGUROINPUT, '.0', '')
    LEFT JOIN acsele_acsele_alfa_STRI_REINSURANCEKIND RK ON RK.REKIND_ID = RG.REKIND_ID
    WHERE CO.id >= 986530136 and CO.status = 2
    """
    
    df_acuerdo = spark.sql(sql_acuerdo)
    
    # Store in S3
    agreement_path = f"{S3_PATHS['agreement']}/agreement"
    write_to_s3(df_acuerdo, agreement_path)
    
    return df_acuerdo

# =============================================================================
# STEP 11: COMBINE ALL RESULTS
# =============================================================================

def combine_all_results(spark):
    """
    Reads all intermediate DataFrames from S3, combines them, and creates the final result
    """
    print("Reading intermediate results from S3 and combining...")
    
    # Read policy information
    df_poliza_base = read_from_s3(spark, f"{S3_PATHS['policy']}/policy_base")
    df_poliza_matriz = read_from_s3(spark, f"{S3_PATHS['policy']}/policy_matrix")
    
    # Read risk and bondholder information
    df_unidad_riesgo = read_from_s3(spark, f"{S3_PATHS['risk_bondholder']}/risk_unit")
    df_tomadores = read_from_s3(spark, f"{S3_PATHS['risk_bondholder']}/bondholder")
    
    # Read premium and commission information
    df_prima_emitida = read_from_s3(spark, f"{S3_PATHS['premium_commission']}/premium")
    df_comision_intermediacion = read_from_s3(spark, f"{S3_PATHS['premium_commission']}/commission_intermediation")
    df_iva_comision_recaudo = read_from_s3(spark, f"{S3_PATHS['premium_commission']}/iva_recaudo")
    df_iva_comision_intermediacion = read_from_s3(spark, f"{S3_PATHS['premium_commission']}/iva_intermediation")
    df_prima_reaseguro = read_from_s3(spark, f"{S3_PATHS['reinsurance']}/prima_reaseguro")
    df_document_types = read_from_s3(spark, f"{S3_PATHS['premium_commission']}/document_types")
    
    # Read reinsurance information
    df_nombre_reaseg = read_from_s3(spark, f"{S3_PATHS['reinsurance']}/nombre_reaseg")
    df_reaseguro = read_from_s3(spark, f"{S3_PATHS['reinsurance']}/reaseguro")
    df_primcedaut = read_from_s3(spark, f"{S3_PATHS['reinsurance']}/primcedaut")
    
    # Read agreement information
    df_acuerdo = read_from_s3(spark, f"{S3_PATHS['agreement']}/agreement")
    
    print("All intermediate results loaded successfully")
    
    # Create aliases for easier reference
    pb = df_poliza_base.alias("pb")
    pm = df_poliza_matriz.alias("pm")
    ur = df_unidad_riesgo.alias("ur")
    t = df_tomadores.alias("t")
    pe = df_prima_emitida.alias("pe")
    ci = df_comision_intermediacion.alias("ci")
    iva = df_iva_comision_intermediacion.alias("iva")
    ac = df_acuerdo.alias("ac")
    pre = df_prima_reaseguro.alias("pre")
    coi = df_document_types.alias("coi")
    ivr = df_iva_comision_recaudo.alias("ivr")
    rea = df_reaseguro.alias("rea")
    nomre = df_nombre_reaseg.alias("nomre")
    prc = df_primcedaut.alias("prc")
    
    print("Starting final join operations...")
    
    # Perform the main join operation
    df_final = pb.join(pm, pb.DCOID == pm.DCOID, "left") \
        .join(ur, pb.DCOID == ur.DCOID, "left") \
        .join(t, pb.DCOID == t.DCOID, "left") \
        .join(pe, pb.DCOID == pe.DCOID, "left") \
        .join(ci, pb.DCOID == ci.DCOID, "left") \
        .join(iva, pb.DCOID == iva.DCOID, "left") \
        .join(ac, pb.DCOID == ac.DCOID, "left") \
        .join(pre, pb.DCOID == pre.DCOID, "left") \
        .join(coi, pb.DCOID == coi.DCOID, "left") \
        .join(ivr, pb.DCOID == ivr.DCOID, "left") \
        .join(rea, pb.DCOID == rea.DCOID, "left") \
        .join(nomre, pb.DCOID == nomre.DCOID, "left") \
        .join(prc, pb.DCOID == prc.DCOID, "left")
    
    # Select and transform final columns
    df_producto = df_final.select(
        lit('Acsele').alias('ORIGEN'),
        col('ac.COMPANIA'),
        col('ac.RAMO'),
        col('ac.DESC_RAMO'),
        col('ac.CODIGO_PRODUCTO').alias('PRODUCTO'),
        col('ac.NOMBRE_PRODUCTO').alias('DESC_PRODUCTO'),
        col('ac.RAMO').alias('RAMO_TECNICO'),
        when(col('ac.RAMO') == '1', 'AUTOS')
        .when(col('ac.RAMO') == '3', 'EQUIPO ELECTRONICO')
        .when(col('ac.RAMO') == '4', 'CUMPLIMIENTO')
        .when(col('ac.RAMO') == '5', 'INCENDIO')
        .when(col('ac.RAMO') == '6', 'LUCRO')
        .when(col('ac.RAMO') == '7', 'MANEJO')
        .when(col('ac.RAMO') == '8', 'ROTURA MAQUINARIA')
        .when(col('ac.RAMO') == '9', 'CASCO')
        .when(col('ac.RAMO') == '10', 'RESPONSABILIDAD')
        .when(col('ac.RAMO') == '12', 'SUSTRACCION')
        .when(col('ac.RAMO') == '13', 'TERREMOTO')
        .when(col('ac.RAMO') == '14', 'TODO RIESGO')
        .when(col('ac.RAMO') == '15', 'TRANSPORTE')
        .when(col('ac.RAMO') == '16', 'VIDRIOS')
        .when(col('ac.RAMO') == '18', 'ACCIDENTES PERSONALES')
        .when(col('ac.RAMO') == '20', 'VIDA GRUPO NO CONTRIBUTIVO')
        .when(col('ac.RAMO') == '22', 'DEUDORES')
        .when(col('ac.RAMO') == '24', 'DESEMPLEO')
        .when(col('ac.RAMO') == '25', 'HOGAR')
        .when(col('ac.RAMO') == '30', 'EXEQUIAS')
        .when(col('ac.RAMO') == '31', 'PREVISIONALES')
        .when(col('ac.RAMO') == '34', 'SALUD')
        .when(col('ac.RAMO') == '40', 'VIDA INDIVIDUAL')
        .when(col('ac.RAMO') == '44', 'JUDICIAL')
        .when(col('ac.RAMO') == '50', 'RIESGOS PROFESIONALES')
        .when(col('ac.RAMO') == '51', 'RENTAS VITALICIAS')
        .when(col('ac.RAMO') == '52', 'RENTAS TASA VARIABLE')
        .when(col('ac.RAMO') == '70', 'VIDA GRUPO CONTRIBUTIVO')
        .when(col('ac.RAMO') == '95', 'MONTAJE')
        .when(col('ac.RAMO') == '96', 'EQUIPO Y MAQUINARIA')
        .alias('DESC_RAMO_TECNICO'),
        col('pm.POLIZA_MATRIZ'),
        col('pb.POLIZA'),
        col('pb.SUCURSAL'),
        lit('').alias('MODULO'),
        col('ur.RISKUNIT_NUMBER').alias('UNIDAD_RIESGO'),
        col('ac.COBERTURA'),
        col('ac.DESC_COBERTURA'),
        col('pb.DCOID').alias('ENDOSO'),
        col('pb.FECHA_ENDOSO'),
        col('pb.FECHA_ENDOSO').alias('MESPRO'),
        col('pm.UNIDAD_NEGOCIO'),
        col('pb.FECHA_INICIO_POLIZA_MATRIZ'),
        col('pb.FECHA_FIN_POLIZA_MATRIZ'),
        col('ur.RISKUNIT_BEGINDATE').alias('FECHA_INICIO_COBERTURA'),
        col('ur.RISKUNIT_ENDDATE').alias('FECHA_FIN_COBERTURA'),
        col('t.ID_TOMADOR_POLIZA').alias('ID_TOMADOR_POLIZA_MATRIZ'),
        col('t.TOMADOR_POLIZA').alias('TOMADOR_POLIZA_MATRIZ'),
        col('t.ID_TOMADOR_POLIZA'),
        col('t.TOMADOR_POLIZA'),
        lit('D').alias('TIPO_COASEGURO'),
        lit(0).alias('PORCPART_ALFA'),
        col('pb.FRECUENCIA_PAGO'),
        col('pe.PRIMA_EMITIDA'),
        expr("pre.ISSUEDPREMIUM - (pre.ISSUEDPREMIUM * (pre.PERCENTAGEPARTICALFA/100))").alias('PRIMACED_COASEGURO'),
        expr("(pre.ISSUEDPREMIUM * (pre.PERCENTAGEPARTICALFA/100))").alias('PRIMAALFA'),
        col('ci.PORC_COM_INTERMEDIACION'),
        col('coi.COMMISION_INTERMEDIARY').alias('COMISION_INTERMEDIACION'),
        col('iva.IVA_COMISION_INTERMEDIACION'),
        col('coi.PORCCOMRECAUDO'),
        col('coi.COMMISSIONCOLLECTION').alias('COMISION_RECAUDO'),
        col('ivr.IVA_COMISION_RECAUDO'),
        col('rea.CODIGO_REASEGURADOR').alias('CODREASEG'),
        col('nomre.NOMREASEG'),
        col('rea.CLASE_REASEGURO').alias('CLASE_REASEGURADOR'),
        col('prc.PRIMACEDAUT'),
        col('rea.PRIMACEDAUT_INT'),
        col('rea.PRIMACEDAUT_EXT'),
        col('prc.PRIMACEDFAC'),
        col('rea.PRIMACEDFAC_INT'),
        col('rea.PRIMACEDFAC_EXT'),
        col('prc.COMISIONREASEGAUT'),
        col('rea.COMISIONREASEGAUT_INT'),
        col('rea.COMISIONREASEGAUT_EXT'),
        col('prc.COMISIONREASEGFAC'),
        col('rea.COMISIONREASEGFAC_INT'),
        col('rea.COMISIONREASEGFAC_EXT'),
        lit('NULL').alias('GASTOS_EXP_PORCENTAJE'),
        lit('NULL').alias('GASTOS_EXP')
    )
    
    print("Final transformations completed")
    
    # Write final result to S3
    final_path = f"{S3_PATHS['final']}/final_result"
    write_to_s3(df_producto, final_path)
    
    return df_producto

# =============================================================================
# MAIN EXECUTION FUNCTION
# =============================================================================

def main():
    """
    Main execution function that orchestrates the entire PCR Tradicional process
    """
    start_time = time.time()
    
    print("=" * 60)
    print("Starting PCR Tradicional Modular Process")
    print("=" * 60)
    
    spark = None
    
    try:
        # Step 1: Create Spark session and process system properties
        print("\n1. Creating Spark session and processing system properties...")
        spark = create_spark_session()
        process_system_properties(spark)
        
        # Step 2: Process document types
        print("\n2. Processing document types...")
        df_document_types = process_document_types(spark)
        
        # Step 3: Process policy information
        print("\n3. Processing policy information...")
        df_poliza_base, df_poliza_matriz = process_policy_info(spark)
        
        # Step 4: Process risk and bondholder information
        print("\n4. Processing risk and bondholder information...")
        df_unidad_riesgo, df_tomadores = process_risk_bondholder(spark)
        
        # Step 5: Process premium information
        print("\n5. Processing premium information...")
        df_prima_emitida = process_premium_info(spark)
        
        # Step 6: Process commission information
        print("\n6. Processing commission information...")
        df_comision_intermediacion, df_iva_comision_recaudo, df_iva_comision_intermediacion = process_commission_info(spark)
        
        # Step 7: Process prima reaseguro
        print("\n7. Processing prima reaseguro...")
        df_prima_reaseguro = process_prima_reaseguro(spark)
        
        # Step 8: Process reinsurance information
        print("\n8. Processing reinsurance information...")
        df_nombre_reaseg, df_reaseguro = process_reinsurance_info(spark)
        
        # Step 9: Process primcedaut
        print("\n9. Processing primcedaut...")
        df_primcedaut = process_primcedaut(spark)
        
        # Step 10: Process agreement information
        print("\n10. Processing agreement information...")
        df_acuerdo = process_agreement_info(spark)
        
        # Step 11: Combine all results
        print("\n11. Combining all results...")
        df_final = combine_all_results(spark)
        
        # Calculate execution time
        end_time = time.time()
        execution_time = end_time - start_time
        
        print("\n" + "=" * 60)
        print("PCR Tradicional Modular Process Completed Successfully!")
        print(f"Total execution time: {execution_time:.2f} seconds")
        print("=" * 60)
        
        # Show final result summary
        print(f"\nFinal result count: {df_final.count()} records")
        print("Final result schema:")
        df_final.printSchema()
        
        # Write to the original path as well for compatibility
        df_final.write.mode("overwrite").parquet("s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL/PRUEBA_MODULAR/")
        print("Final result also written to original path: s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL/PRUEBA_MODULAR/")
        
        return df_final
        
    except Exception as e:
        print(f"\nError during execution: {str(e)}")
        print("Process failed!")
        raise e
    
    finally:
        # Stop Spark session
        if spark is not None:
            spark.stop()
            print("Spark session stopped")

# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    df_result = main()

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

job.commit()