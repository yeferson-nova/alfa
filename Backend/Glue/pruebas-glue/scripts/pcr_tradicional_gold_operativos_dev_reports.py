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
        'PERIODO',
        'FECHA_INICIO',
        'FECHA_FIN',
        'CERTIFICATE',
        'DB_CATALOG_GLUE',
        'TABLE_NAME_GOLD',
        'TABLE_NAME_TOTAL',
        'TABLE_NAME_DETALLE'
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
TABLE_NAME_GOLD = args['TABLE_NAME_GOLD']
TABLE_NAME_DETALLE = args['TABLE_NAME_DETALLE']
TABLE_NAME_TOTAL = args['TABLE_NAME_TOTAL']

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
    

def description_stst_systemproperty(df_stst_systemproperty, syp_property_value):

    df_stst_systemproperty_filter = df_stst_systemproperty.filter(lower(col("SYP_PROPERTY")) == syp_property_value.lower())
    
    exploded_stst_systemproperty_filter = df_stst_systemproperty_filter.select(explode(split(df_stst_systemproperty_filter["SYP_VALUE"], ",")).alias("DESCRIPTION"))
    
    trimmed_stst_systemproperty_filter = exploded_stst_systemproperty_filter.select(lower(trim(exploded_stst_systemproperty_filter["DESCRIPTION"])).alias("DESCRIPTION"))    
    
    return trimmed_stst_systemproperty_filter    
#-----------------------------------------------------------------#
#----------------------- Read Sources ----------------------------#
#-----------------------------------------------------------------#  

# --- Lectura y Selección para cada CTE ---

# SELECT df_contextoperation_par
df_contextoperation_par = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_contextoperation").toDF()
df_contextoperation_par_select = df_contextoperation_par.select("id", "time_stamp", "status", "time_stamp_partition")

# SELECT: df_thirdpartymovementpolicy_par
df_thirdpartymovementpolicy_par = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_thirdpartymovementpolicy").toDF()
df_thirdpartymovementpolicy_par_select = df_thirdpartymovementpolicy_par.select("operationpk", "policyid", "thirdparty").distinct()

# SELECT: policydco
df_policydco = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_policydco").toDF()
df_policydco_select = df_policydco.select("agregatedobjectid", "dcoid", "operationpk", "initialdate", "finishdate").distinct()

# SELECT: preproduct
df_preproduct = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_preproduct").toDF()
df_preproduct_select = df_preproduct.select("pk", "companiainput", "ramoinput", "codigoproductoinput", "descripcionproductoinput", "ramovalue").distinct()

# SELECT: stpo_policyparticipation
df_stpo_policyparticipation = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_stpo_policyparticipation").toDF()
df_stpo_policyparticipation_select = df_stpo_policyparticipation.select("thirdpartyid", "rol_id", "agregatedobjectid", "operationpk").distinct()

# SELECT: stpo_policyparticipationdco
df_stpo_policyparticipationdco = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_stpo_policyparticipationdco").toDF()
df_stpo_policyparticipationdco_select = df_stpo_policyparticipationdco.select("operationpk", "agregatedobjectid", "dcoid").distinct()

# SELECT: coveragedco
df_coveragedco = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_coveragedco").toDF()
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

# SELECT: stag_agreementversion
df_stag_agreementversion = glueContext.create_dynamic_frame.from_catalog(database=f"{DB_CATALOG_GLUE}", table_name="acsele_alfa_full_stag_agreementversion").toDF()
df_stag_agreementversion_select = df_stag_agreementversion.select("ast_id", "dcoid", "ast_fecini", "ast_fecfin").distinct()

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

df_contextoperation_par_filter = df_contextoperation_par_filter.repartition("id")
df_thirdpartymovementpolicy_par_select = df_thirdpartymovementpolicy_par_select.repartition("operationpk")

#-----------------------------------------------------------------#
#--------------------- create view temp --------------------------#
#-----------------------------------------------------------------#  

#---Información
df_contextoperation_par_filter.createOrReplaceTempView("contextoperation_par")
df_thirdpartymovementpolicy_par_select.createOrReplaceTempView("thirdpartymovementpolicy_par")
df_policydco_select.createOrReplaceTempView("policydco")
df_stpo_policyparticipation_select.createOrReplaceTempView("stpo_policyparticipation")
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

#---Operación
df_stpo_policyparticipationdco_select.createOrReplaceTempView("stpo_policyparticipationdco")
df_participacionintermediarios_select.createOrReplaceTempView("participacionintermediarios")
df_openitem_select.createOrReplaceTempView("openitem")
df_stca_doctype_select.createOrReplaceTempView("stca_doctype")
df_uaadetail_select.createOrReplaceTempView("uaadetail")
df_property_select.createOrReplaceTempView("property")
print(f"USE `{DB_CATALOG_GLUE}`")
spark.sql(f"USE `{DB_CATALOG_GLUE}`")

#-----------------------------------------------------------------#
#------------df_query_por_comission_intermediary------------------#
#-----------------------------------------------------------------#

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


#-----------------------------------------------------------------#
#------------df_query_por_comission_percentage -------------------#
#-----------------------------------------------------------------#

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

#-----------------------------------------------------------------#
#--------------df_query_comission_intermediary -------------------#
#-----------------------------------------------------------------#

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

#-----------------------------------------------------------------#
#--------------df_query_comission_intermediary_iva----------------#
#-----------------------------------------------------------------#

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


#-----------------------------------------------------------------#
#------------------ df_query_comission_collection ----------------#
#-----------------------------------------------------------------#

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

#-----------------------------------------------------------------#
#---------------- df_query_comission_collection_iva---------------#
#-----------------------------------------------------------------#

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


#-----------------------------------------------------------------#
#----------df_query_premium_detail_reinsurance_comission----------#
#-----------------------------------------------------------------#

df_query_premium_detail_reinsurance_comission = spark.sql("""
    
        SELECT
            ctx.id as id_cte_premium_detail,
            COALESCE(SUM(ROD.ROD_PRORATEDPREMIUM), 0) AS PREMIUM_DETAIL,
            COALESCE(SUM(PT.ROPP_VALUE),0) AS REINSURANCE_COMMISSION
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

#-----------------------------------------------------------------#
#------------------------- df_query_prima ------------------------#
#-----------------------------------------------------------------#

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

#-----------------------------------------------------------------#
#------------------------ df_query_coberturas---------------------#
#-----------------------------------------------------------------#
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

#-----------------------------------------------------------------#
#------------------------ query_primas_detalle--------------------#
#-----------------------------------------------------------------#

query_primas_detalle = spark.sql ("""     
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
        ,(PRIMA_EMITIDA_VALUE/CANT_COBERTURAS)                                                                                              as PRIMA_EMITIDA_VALUE_DETALLE
        ,(PRIMA_EMITIDA_REASEGURO/CANT_COBERTURAS)                                                                                          as PRIMA_EMITIDA_REASEGURO_DETALLE
        ,(PRIMA_EMITIDA_MOVIMIENTO/CANT_COBERTURAS)                                                                                         as PRIMA_EMITIDA_MOVIMIENTO_DETALLE
        , 0                                                                                                                                 as PORCPART_ALFA
        ,(PRIMA_EMITIDA_VALUE/CANT_COBERTURAS) * (100/100)                                                                                  as PRIMA_ALFA_VALUE_DETALLE
        ,(PRIMA_EMITIDA_REASEGURO/CANT_COBERTURAS) * (100/100)                                                                              as PRIMA_ALFA_REASEGURO_DETALLE
        ,(PRIMA_EMITIDA_MOVIMIENTO/CANT_COBERTURAS) * (100/100)                                                                             as PRIMA_ALFA_MOVIMIENTO_DETALLE
        , 0                                                                                                                                 as PRIMACED_COASEGURO
        , COALESCE(qpc.PORC_COM_INTERMEDIACION, 0)                                                                                          as PORC_COM_INTERMEDIACION
        , COALESCE(qci.COMISION_INTERMEDIACION, 0)                                                                                          as COMISION_INTERMEDIACION
        , COALESCE(qcv.IVA_COMISION_INTERMEDIACION, 0)                                                                                      as IVA_COMISION_INTERMEDIACION
        , COALESCE(qcc.COMISION_RECAUDO, 0)                                                                                                 as COMISION_RECAUDO
        , ((COALESCE(qcc.COMISION_RECAUDO, 0)*100)/CANT_COBERTURAS) / NULLIF(((PRIMA_EMITIDA_VALUE/CANT_COBERTURAS) * (100/100)),0)         as PORCCOMRECAUDO_VALUE
        , ((COALESCE(qcc.COMISION_RECAUDO, 0)*100)/CANT_COBERTURAS) / NULLIF(((PRIMA_EMITIDA_REASEGURO/CANT_COBERTURAS) * (100/100)),0)     as PORCCOMRECAUDO_REASEGURO
        , ((COALESCE(qcc.COMISION_RECAUDO, 0)*100)/CANT_COBERTURAS) / NULLIF(((PRIMA_EMITIDA_MOVIMIENTO/CANT_COBERTURAS) * (100/100)),0)    as PORCCOMRECAUDO_MOVIMIENTO
        , COALESCE(qcci.IVA_COMISION_RECAUDO, 0)                                                                                            as IVA_COMISION_RECAUDO
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0,qpd.PREMIUM_DETAIL)                   as PRIMACEDAUT
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),qpd.PREMIUM_DETAIL, 0)                  as PRIMACEDFAC
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),0, qpd.REINSURANCE_COMMISSION)          as COMISIONREASEGAUT
        , IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),qpd.REINSURANCE_COMMISSION, 0)          as COMISIONREASEGFAC
        , MAX(RA.CLASEREASEGUROINPUT)                                                                                                       as CLASE_REASEGURADOR
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
        ,PERIODO

""")


query_primas_detalle = query_primas_detalle.repartition(300)
tmp_primas_detalle_path  = f"s3://{BUCKET_NAME}/GOLD/NIIF/PCR_TRADICIONAL_TMP/primas_detalle_tmp"
query_primas_detalle.write.mode("overwrite").parquet(tmp_primas_detalle_path)
query_primas_detalle_safe = spark.read.parquet(tmp_primas_detalle_path).repartition(300)  
query_primas_detalle_safe.createOrReplaceTempView("query_primas_detalle")


#-----------------------------------------------------------------#
#------------------------ df_query_operativo ---------------------#
#-----------------------------------------------------------------#

df_query_operativo = spark.sql ("""

    SELECT 
          qpde.id
        , qpde.RAMO_CONTABLE
        , qpde.GRUPO_REASEGURO
        , qpde.DSC_GRUPO_REASEGURO
        , qpde.RAMO_REASEGURO
        , qpde.DESC_RAMO_REASEGURO
        , qpde.PRIMA_EMITIDA_VALUE
        , qpde.PRIMA_EMITIDA_REASEGURO
        , qpde.PRIMA_EMITIDA_MOVIMIENTO
        , qpde.COD_COBERTURA
        , qpde.DESC_COBERTURA
        , qpde.CANT_COBERTURAS
        , qpde.PRIMA_EMITIDA_VALUE_DETALLE
        , qpde.PRIMA_EMITIDA_REASEGURO_DETALLE
        , qpde.PRIMA_EMITIDA_MOVIMIENTO_DETALLE
        , qpde.PORCPART_ALFA
        , qpde.PRIMA_ALFA_VALUE_DETALLE
        , qpde.PRIMA_ALFA_REASEGURO_DETALLE
        , qpde.PRIMA_ALFA_MOVIMIENTO_DETALLE
        , qpde.PRIMACED_COASEGURO
        , qpde.PORC_COM_INTERMEDIACION
        , qpde.COMISION_INTERMEDIACION
        , qpde.IVA_COMISION_INTERMEDIACION
        , qpde.COMISION_RECAUDO
        , qpde.PORCCOMRECAUDO_VALUE
        , qpde.PORCCOMRECAUDO_REASEGURO
        , qpde.PORCCOMRECAUDO_MOVIMIENTO
        , qpde.IVA_COMISION_RECAUDO
        , qpde.PRIMACEDAUT
        , qpde.PRIMACEDFAC
        , qpde.COMISIONREASEGAUT
        , qpde.COMISIONREASEGFAC
        , qpde.CLASE_REASEGURADOR
        , IF(qpde.CLASE_REASEGURADOR = 'Interior', qpde.PRIMACEDAUT, 0)                                                                as PRIMACEDAUT_INT
        , IF(qpde.CLASE_REASEGURADOR = 'Exterior', qpde.PRIMACEDAUT, 0)                                                                as PRIMACEDAUT_EXT
        , IF(qpde.CLASE_REASEGURADOR = 'Interior', qpde.PRIMACEDFAC, 0)                                                                as PRIMACEDFAC_INT
        , IF(qpde.CLASE_REASEGURADOR = 'Exterior', qpde.PRIMACEDFAC, 0)                                                                as PRIMACEDFAC_EXT
        , IF(qpde.CLASE_REASEGURADOR = 'Interior', qpde.COMISIONREASEGAUT, 0)                                                        as COMISIONREASEGAUT_INT
        , IF(qpde.CLASE_REASEGURADOR = 'Exterior', qpde.COMISIONREASEGAUT, 0)                                                        as COMISIONREASEGAUT_EXT
        , IF(qpde.CLASE_REASEGURADOR = 'Interior', qpde.COMISIONREASEGFAC, 0)                                                        as COMISIONREASEGFAC_INT
        , IF(qpde.CLASE_REASEGURADOR = 'Exterior', qpde.COMISIONREASEGFAC, 0)                                                        as COMISIONREASEGFAC_EXT
        , qpde.PERIODO
    FROM query_primas_detalle qpde


""")

df_query_operativo_repartition = df_query_operativo.repartition(300)
logger.info(f"Writing to Operativo Pcr with period = {PERIODO}")
# Write to the pcr operativo to tmp path
df_query_operativo_repartition.write.mode("overwrite").partitionBy("PERIODO").parquet(f"s3://{BUCKET_NAME}/GOLD/NIIF/PCR_TRADICIONAL_TMP/pcr_operativo_tmp/")
logger.info(f"Finished write to Operativo Pcr with period = {PERIODO}")

#-----------------------------------------------------------------#
#------------------ df_query_pcr_tradicional ---------------------#
#-----------------------------------------------------------------#

#Create the final table inner operativo e informacion
niif_pcr_tradicional_informacion = spark.read.parquet(f"s3://{BUCKET_NAME}/GOLD/NIIF/PCR_TRADICIONAL_TMP/pcr_info_tmp/").repartition(200)
niif_pcr_tradicional_informacion.createOrReplaceTempView("niif_pcr_tradicional_informacion")
niif_pcr_tradicional_operativo = spark.read.parquet(f"s3://{BUCKET_NAME}/GOLD/NIIF/PCR_TRADICIONAL_TMP/pcr_operativo_tmp/").repartition(200)
niif_pcr_tradicional_operativo.createOrReplaceTempView("niif_pcr_tradicional_operativo")


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


df_query_pcr_tradicional_gold_unique = df_query_pcr_tradicional_gold.dropDuplicates(["operacion","ramo_contable","ramo_tecnico","ramo_reaseguro", "producto", "poliza", "cobertura"])
df_query_pcr_tradicional_gold_unique.write.mode("overwrite").partitionBy("PERIODO").parquet(f"s3://{BUCKET_NAME}/GOLD/NIIF/{TABLE_NAME_GOLD}/")

logger.info(f"Finished write table PCR_TRADICIONAL_GOLD with period = {PERIODO}")

#-----------------------------------------------------------------#
#---------------------- df_prima_detalle_logic -------------------#
#-----------------------------------------------------------------#

df_descriptions_prima_total = description_stst_systemproperty(df_stst_systemproperty, "rrc.issuedpremiumdoctype")
df_descriptions_prima_total.createOrReplaceTempView("descriptions_prima_total")

df_prima_total = spark.sql ("""

select
              ctx.id
            , op.amount
            , DT.DTY_DESCRIPTION
            , DT.DTY_ISDEBIT
            , (CASE WHEN DT.DTY_ISDEBIT = 1 THEN op.AMOUNT ELSE op.AMOUNT * (-1) END) as prima_totalizador
            , 0 as ID_LOTE_CARGUE
            , ctx.time_stamp_partition as PERIODO
        from contextoperation_par ctx
        inner join openitem op
        on ctx.id = op.operationpk
        inner join stca_doctype dt
        on dt.dty_id = op.dty_id
        WHERE lower(DT.DTY_DESCRIPTION) IN (SELECT DESCRIPTION FROM descriptions_prima_total)
        group by ctx.id
            , op.amount
            , DT.DTY_DESCRIPTION
            , DT.DTY_ISDEBIT    
            , (CASE WHEN DT.DTY_ISDEBIT = 1 THEN op.AMOUNT ELSE op.AMOUNT * (-1) END)
            , ctx.time_stamp_partition
        order by ctx.id

""")

df_prima_total.write.mode("overwrite").partitionBy("PERIODO").parquet(f"s3://{BUCKET_NAME}/GOLD/NIIF/{TABLE_NAME_TOTAL}/")

#-----------------------------------------------------------------#
#---------------------- df_prima_total_logic ---------------------#
#-----------------------------------------------------------------#


df_query_detalle_prima = spark.sql("""
    
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
    
    """)
df_query_detalle_prima.createOrReplaceTempView("df_query_detalle_prima")

    
df_query_detalle_prima_total = spark.sql("""

    SELECT
           id
         , cod_ramo_contable
         , sum(prima_reaseguro) as prima_reaseguro
         , sum(prima_movimiento) as prima_movimiento
         , sum(prima_cobertura) as prima_cobertura
         , 0 as ID_LOTE_CARGUE
         , PERIODO
    FROM df_query_detalle_prima
    GROUP BY id
            , cod_ramo_contable
            , periodo

""")


df_query_detalle_prima_total.write.mode("overwrite").partitionBy("PERIODO").parquet(f"s3://{BUCKET_NAME}/GOLD/NIIF/{TABLE_NAME_DETALLE}/")

logger.info(f"Finished for table total and table detail with period = {PERIODO}")

#-----------------------------------------------------------------#
#---------------------- Clean tmp files --------------------------#
#-----------------------------------------------------------------#

prefix_to_delete = 'GOLD/NIIF/PCR_TRADICIONAL_TMP/'
s3 = boto3.resource('s3')
bucket = s3.Bucket(BUCKET_NAME)
response = bucket.objects.filter(Prefix=prefix_to_delete).delete()

logger.info(f"Clean tables from path tmp with period = {PERIODO}")
