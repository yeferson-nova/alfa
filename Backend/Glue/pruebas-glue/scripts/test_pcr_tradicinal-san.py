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

S3_BASE_PATH = "s3://alfa-datalake-dev-silver/ACSELE/TRANSFORMATION/"

# -----------------------------
# Función para cargar vistas
# -----------------------------
def cargar_tabla(nombre):
    df = spark.read.parquet(f"{S3_BASE_PATH}{nombre}/")
    df.createOrReplaceTempView(nombre)
    return df

# -----------------------------
# Cargar tablas necesarias
# -----------------------------
cargar_tabla("POLICYDCO")
cargar_tabla("CONTEXTOPERATION")
cargar_tabla("PREPOLICY")
cargar_tabla("FINANCIALPLANDCO")
cargar_tabla("AGREGATEDPOLICY")

# -----------------------------
# Lógica del campo PRODUCTO
# -----------------------------

#dcoid es endoso
#FECHA_FIN_POLIZA_MATRIZ = fecha_fin_poliza_matriz

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
FROM POLICYDCO DCO
JOIN CONTEXTOPERATION CO ON CO.ID = DCO.OPERATIONPK
JOIN PREPOLICY PP ON PP.PK = DCO.DCOID
JOIN FINANCIALPLANDCO FPDCO ON FPDCO.AGREGATEDPARENTID = DCO.AGREGATEDOBJECTID AND FPDCO.OPERATIONPK = DCO.OPERATIONPK
JOIN AGREGATEDPOLICY AP ON AP.AGREGATEDPOLICYID = DCO.AGREGATEDOBJECTID
"""

df_producto = spark.sql(sql_producto)
df_producto.createOrReplaceTempView("TMP_PRODUCTO")
df_producto.show(5)
print("✅ Campo PRODUCTO generado correctamente en vista temporal: TMP_PRODUCTO")

from pyspark.sql import SparkSession

# -----------------------------
# Configuración para Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_RAMO_Reestructurado_Final") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

S3_BASE_PATH = "s3://alfa-datalake-dev-silver/ACSELE/TRANSFORMATION/"

def cargar_tabla(nombre):
    df = spark.read.parquet(f"{S3_BASE_PATH}{nombre}/")
    df.createOrReplaceTempView(nombre)
    return df

# -----------------------------
# Cargar tablas necesarias
# -----------------------------
tablas = [
    "COVERAGEDCO", "COBERTURA", "POLICYDCO", "AGREGATEDPOLICY", "PRODUCT", "PREPRODUCT",
    "PREPOLICY", "RI_REINSURANCEGROUP", "STRI_REINSURANCEKIND"
]
for t in tablas:
    cargar_tabla(t)

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
FROM COVERAGEDCO CDCO
JOIN COBERTURA CB ON CB.PK = CDCO.DCOID
LEFT JOIN POLICYDCO PDCO ON PDCO.OPERATIONPK = CDCO.OPERATIONPK
LEFT JOIN AGREGATEDPOLICY AP ON AP.AGREGATEDPOLICYID = PDCO.AGREGATEDOBJECTID
LEFT JOIN PRODUCT PD ON PD.PRODUCTID = AP.PRODUCTID
LEFT JOIN PREPRODUCT PP ON PP.PK = PD.PRO_DCOID
LEFT JOIN PREPOLICY PPRE ON PPRE.PK = PDCO.DCOID
LEFT JOIN RI_REINSURANCEGROUP RG ON RG.CODE = REPLACE(CB.COBRAMOREASEGUROINPUT, '.0', '')
LEFT JOIN STRI_REINSURANCEKIND RK ON RK.REKIND_ID = RG.REKIND_ID
'''

df_ramo_final = spark.sql(sql_ramo_final)
df_ramo_final.createOrReplaceTempView("TMP_RAMO")
df_ramo_final.show(5)
print("✅ Campo RAMO reestructurado sin subconsultas y con los nombres finales de salida.")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, explode, trim

# -----------------------------
# Configuración para Glue G1X
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRIMA_EMITIDA") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

S3_BASE_PATH = "s3://alfa-datalake-dev-silver/ACSELE/TRANSFORMATION/"

def cargar_tabla(nombre):
    df = spark.read.parquet(f"{S3_BASE_PATH}{nombre}/")
    df.createOrReplaceTempView(nombre)
    return df

# -----------------------------
# Cargar tablas necesarias
# -----------------------------
cargar_tabla("OPENITEM")
cargar_tabla("STCA_DOCTYPE")
cargar_tabla("STST_SYSTEMPROPERTY")

# -----------------------------
# Preparar lista de descripciones válidas
# -----------------------------
# Suplantamos la lógica del CASE '13'... por directamente filtrar la propiedad 'RRC.issuedPremium.earthquakeDocType'

syp_df = spark.sql("""
    SELECT SYP_VALUE
    FROM STST_SYSTEMPROPERTY
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
FROM OPENITEM OI
JOIN STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
WHERE OI.OPM_SUBSTATUS <> 'notApplied'
  AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM ISSUED_PREMIUM_DOCS)
GROUP BY OI.OPERATIONPK
"""

df_prima_emitida = spark.sql(sql_prima_emitida)
df_prima_emitida.createOrReplaceTempView("TMP_PRIMA_EMITIDA")
df_prima_emitida.show(5)
print("✅ Campo PRIMA_EMITIDA generado correctamente en vista temporal: TMP_PRIMA_EMITIDA")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, explode, trim

# -----------------------------
# Configuración para Glue G1X
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_COMISION_RECAUDO") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

S3_BASE_PATH = "s3://alfa-datalake-dev-silver/ACSELE/TRANSFORMATION/"

def cargar_tabla(nombre):
    df = spark.read.parquet(f"{S3_BASE_PATH}{nombre}/")
    df.createOrReplaceTempView(nombre)
    return df

# -----------------------------
# Cargar tablas necesarias
# -----------------------------
cargar_tabla("OPENITEM")
cargar_tabla("STCA_DOCTYPE")
cargar_tabla("STST_SYSTEMPROPERTY")

# -----------------------------
# Preparar lista de descripciones válidas
# -----------------------------
# Simulamos el CASE '13' para seleccionar la propiedad correcta
syp_df = spark.sql("""
    SELECT SYP_VALUE
    FROM STST_SYSTEMPROPERTY
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
FROM OPENITEM OI
JOIN STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
WHERE OI.OPM_SUBSTATUS <> 'notApplied'
  AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM COMISION_RECAUDO_DOCS)
GROUP BY OI.OPERATIONPK
"""

df_comision_recaudo = spark.sql(sql_comision_recaudo)
df_comision_recaudo.createOrReplaceTempView("TMP_COMISION_RECAUDO")
df_comision_recaudo.show(5)
print("✅ Campo COMISION_RECAUDO generado correctamente en vista temporal: TMP_COMISION_RECAUDO")

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

S3_BASE_PATH = "s3://alfa-datalake-dev-silver/ACSELE/TRANSFORMATION/"

def cargar_tabla(nombre):
    df = spark.read.parquet(f"{S3_BASE_PATH}{nombre}/")
    df.createOrReplaceTempView(nombre)
    return df

# -----------------------------
# Cargar tablas necesarias
# -----------------------------
cargar_tabla("STPO_POLICYPARTICIPATION")
cargar_tabla("STPO_POLICYPARTICIPATIONDCO")
cargar_tabla("PARTICIPACIONINTERMEDIARIOS")

# -----------------------------
# Lógica del campo PORC_COM_INTERMEDIACION
# -----------------------------
sql_porc_com_intermediacion = """
SELECT
  PPD.OPERATIONPK AS DCOID,
  PI.PORCENTAJECOMISIONINPUT AS PORC_COM_INTERMEDIACION
FROM STPO_POLICYPARTICIPATION PP
INNER JOIN STPO_POLICYPARTICIPATIONDCO PPD ON PP.AGREGATEDOBJECTID = PPD.AGREGATEDOBJECTID
INNER JOIN PARTICIPACIONINTERMEDIARIOS PI ON PI.PK = PPD.DCOID
"""

df_porc_com_intermediacion = spark.sql(sql_porc_com_intermediacion)
df_porc_com_intermediacion.createOrReplaceTempView("TMP_PORC_COM_INTERMEDIACION")
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

S3_BASE_PATH = "s3://alfa-datalake-dev-silver/ACSELE/TRANSFORMATION/"

def cargar_tabla(nombre):
    df = spark.read.parquet(f"{S3_BASE_PATH}{nombre}/")
    df.createOrReplaceTempView(nombre)
    return df

# -----------------------------
# Cargar tablas necesarias
# -----------------------------
cargar_tabla("POLICYDCO")
cargar_tabla("STPS_RELATIONAGREEMENTPOLICY")
cargar_tabla("STAG_AGREEMENTVERSION")
cargar_tabla("PREAGREEMENT")

# -----------------------------
# Lógica del campo UNIDAD_NEGOCIO (sin filtro de fecha)
# -----------------------------
sql_unidad_negocio = """
SELECT
  DCO.OPERATIONPK AS DCOID,
  PREA.UNIDADNEGOCIOINPUT AS UNIDAD_NEGOCIO
FROM POLICYDCO DCO  
INNER JOIN STPS_RELATIONAGREEMENTPOLICY RAP ON RAP.RSA_POLICYID = DCO.AGREGATEDOBJECTID
INNER JOIN STAG_AGREEMENTVERSION AV ON AV.AST_ID = RAP.RSA_AST_ID 
INNER JOIN PREAGREEMENT PREA ON PREA.PK = AV.DCOID
"""

df_unidad_negocio = spark.sql(sql_unidad_negocio)
df_unidad_negocio.createOrReplaceTempView("TMP_UNIDAD_NEGOCIO")
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

S3_BASE_PATH = "s3://alfa-datalake-dev-silver/ACSELE/TRANSFORMATION/"

def cargar_tabla(nombre):
    df = spark.read.parquet(f"{S3_BASE_PATH}{nombre}/")
    df.createOrReplaceTempView(nombre)
    return df

# -----------------------------
# Cargar tablas necesarias
# -----------------------------
cargar_tabla("POLICYDCO")
cargar_tabla("CONTEXTOPERATION")
cargar_tabla("PREPOLICY")
cargar_tabla("FINANCIALPLANDCO")
cargar_tabla("AGREGATEDPOLICY")
cargar_tabla("PRODUCT")
cargar_tabla("PREPRODUCT")

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
FROM POLICYDCO DCO 
INNER JOIN CONTEXTOPERATION CO ON CO.ID = DCO.OPERATIONPK
INNER JOIN PREPOLICY PP ON PP.PK = DCO.DCOID
INNER JOIN FINANCIALPLANDCO FPDCO ON FPDCO.AGREGATEDPARENTID = DCO.AGREGATEDOBJECTID AND FPDCO.OPERATIONPK = DCO.OPERATIONPK
INNER JOIN AGREGATEDPOLICY AP ON AP.AGREGATEDPOLICYID = DCO.AGREGATEDOBJECTID  
INNER JOIN PRODUCT PT ON PT.PRODUCTID = AP.PRODUCTID
INNER JOIN PREPRODUCT PR ON PT.PRO_DCOID = PR.PK
"""

df_desc_producto = spark.sql(sql_desc_producto)
df_desc_producto.createOrReplaceTempView("TMP_DESC_PRODUCTO")
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

S3_BASE_PATH = "s3://alfa-datalake-dev-silver/ACSELE/TRANSFORMATION/"

def cargar_tabla(nombre):
    df = spark.read.parquet(f"{S3_BASE_PATH}{nombre}/")
    df.createOrReplaceTempView(nombre)
    return df

# -----------------------------
# Cargar tablas necesarias
# -----------------------------
cargar_tabla("PRERISKUNITS")
cargar_tabla("RISKUNITDCO")

# -----------------------------
# Lógica del campo RISKUNIT (sin filtros)
# -----------------------------
sql_riskunit = """
SELECT
  DCO.OPERATIONPK AS DCOID,
  PRU.NUMUNIDRIESGOINPUT AS RISKUNIT_NUMBER,
  DCO.INITIALDATE AS RISKUNIT_BEGINDATE, 
  DCO.FINISHDATE AS RISKUNIT_ENDDATE
FROM PRERISKUNITS PRU
INNER JOIN RISKUNITDCO DCO ON DCO.DCOID = PRU.PK
"""

df_riskunit = spark.sql(sql_riskunit)
df_riskunit.createOrReplaceTempView("TMP_RISKUNIT")
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

S3_BASE_PATH = "s3://alfa-datalake-dev-silver/ACSELE/TRANSFORMATION/"

def cargar_tabla(nombre):
    df = spark.read.parquet(f"{S3_BASE_PATH}{nombre}/")
    df.createOrReplaceTempView(nombre)
    return df

# -----------------------------
# Cargar tablas necesarias
# -----------------------------
cargar_tabla("STPO_POLICYPARTICIPATION")
cargar_tabla("STPO_POLICYPARTICIPATIONDCO")
cargar_tabla("STTE_THIRDPARTY")
cargar_tabla("PRETHIRDPARTY")
cargar_tabla("STTS_ROLE")
cargar_tabla("STST_SYSTEMPROPERTY")

# -----------------------------
# Explosión de descripciones válidas desde STST_SYSTEMPROPERTY
# -----------------------------
bondholder_df = spark.sql("""
    SELECT SYP_VALUE
    FROM STST_SYSTEMPROPERTY
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
FROM STPO_POLICYPARTICIPATION PP
INNER JOIN STPO_POLICYPARTICIPATIONDCO PPD ON PP.AGREGATEDOBJECTID = PPD.AGREGATEDOBJECTID 
INNER JOIN STTE_THIRDPARTY TP ON TP.TPT_ID = PP.THIRDPARTYID
INNER JOIN PRETHIRDPARTY PTP ON PTP.PK = TP.IDDCO
INNER JOIN STTS_ROLE RL ON RL.ROL_ID = PP.ROL_ID
WHERE RL.ROL_DESCRIPTION IN (SELECT ROL_DESCRIPTION FROM BONDHOLDER_ROLES)
"""

df_tomador_poliza = spark.sql(sql_tomador_poliza)
df_tomador_poliza.createOrReplaceTempView("TMP_TOMADOR_POLIZA")
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

S3_BASE_PATH = "s3://alfa-datalake-dev-silver/ACSELE/TRANSFORMATION/"

def cargar_tabla(nombre):
    df = spark.read.parquet(f"{S3_BASE_PATH}{nombre}/")
    df.createOrReplaceTempView(nombre)
    return df

# -----------------------------
# Cargar tablas necesarias
# -----------------------------
cargar_tabla("THIRDPARTYROLE")
cargar_tabla("REASEGURADOR")

# -----------------------------
# Lógica del campo CODREASEG (sin filtro THIRDPARTYID)
# -----------------------------
sql_codreaseg = """
SELECT
  TPR.THIRDPARTYID AS DCOID,
  RA.CLASEREASEGUROINPUT AS CLASE_REASEGURO,
  RA.CODIGOREASEGURADORINPUT AS CODREASEG
FROM THIRDPARTYROLE TPR
INNER JOIN REASEGURADOR RA ON RA.PK = TPR.IDDCOROLE
"""

df_codreaseg = spark.sql(sql_codreaseg)
df_codreaseg.createOrReplaceTempView("TMP_CODREASEG")
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

S3_BASE_PATH = "s3://alfa-datalake-dev-silver/ACSELE/TRANSFORMATION/"

def cargar_tabla(nombre):
    df = spark.read.parquet(f"{S3_BASE_PATH}{nombre}/")
    df.createOrReplaceTempView(nombre)
    return df

# -----------------------------
# Cargar tablas necesarias
# -----------------------------
cargar_tabla("REINSURANCEOPERATION")
cargar_tabla("REINSURANCEOPERATIONGROUPINFO")
cargar_tabla("REINSURANCEOPERATIONCOMPONENT")
cargar_tabla("REINSURANCEOPERATIONDETAIL")
cargar_tabla("RI_REINSURANCEGROUP")
cargar_tabla("RISKUNITDCO")

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
FROM REINSURANCEOPERATION RO
INNER JOIN REINSURANCEOPERATIONGROUPINFO ROG ON ROG.OPERATIONID = RO.OPERATIONID
INNER JOIN REINSURANCEOPERATIONCOMPONENT ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
INNER JOIN REINSURANCEOPERATIONDETAIL ROD ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
INNER JOIN RI_REINSURANCEGROUP RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
INNER JOIN RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
WHERE ROC.CONTRACTTYPE = 'MandatoryFacultativeContract'
GROUP BY ROD.THIRDPARTYID, ROD.THIRDPARTYNAME, ROD.PARTICIPATIONID, RUD.OPERATIONPK, RUD.DCOID
"""

df_nomreaseg = spark.sql(sql_nomreaseg)
df_nomreaseg.createOrReplaceTempView("TMP_NOMREASEG")
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

# -----------------------------
# Ruta base en S3
# -----------------------------
S3_BASE_PATH = "s3://alfa-datalake-dev-silver/ACSELE/TRANSFORMATION/"

# -----------------------------
# Función para cargar tablas
# -----------------------------
def cargar_tabla(nombre):
    df = spark.read.parquet(f"{S3_BASE_PATH}{nombre}/")
    df.createOrReplaceTempView(nombre)
    return df

# =============================
# CAMPO 1: COMPANIA
# =============================

# Cargar tabla necesaria
cargar_tabla("RI_REINSURANCEGROUP")

# Ejecutar consulta
sql_compania = """
SELECT
  RUD.OPERATIONPK AS DCOID,
  RG.RIG_COMPANYCODE AS COMPANIA
FROM RI_REINSURANCEGROUP RG
INNER JOIN REINSURANCEOPERATIONGROUPINFO ROG ON ROG.GROUPID = RG.REINSURANCEGROUPID
INNER JOIN REINSURANCEOPERATION RO ON RO.OPERATIONID = ROG.OPERATIONID
INNER JOIN RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID

"""

df_compania = spark.sql(sql_compania)
df_compania.createOrReplaceTempView("TMP_COMPANIA")
df_compania.show(5)
print("✅ Campo COMPANIA generado correctamente en vista temporal: TMP_COMPANIA")

# =============================
# CAMPO 2: POLIZA_MATRIZ
# =============================

# Cargar tablas necesarias
cargar_tabla("POLICYDCO")
cargar_tabla("STPS_RELATIONAGREEMENTPOLICY")
cargar_tabla("STAG_AGREEMENTVERSION")
cargar_tabla("PREAGREEMENT")
cargar_tabla("STPS_AGREEMENTSTATICDAT")

# Ejecutar consulta
sql_poliza_matriz = """
SELECT
  DCO.OPERATIONPK AS DCOID,
  PREA.UNIDADNEGOCIOINPUT AS BUSINESSUNIT_NAME,
  AGS.AST_NUMEROACU AS POLIZA_MATRIZ
FROM POLICYDCO DCO
INNER JOIN STPS_RELATIONAGREEMENTPOLICY RAP ON RAP.RSA_POLICYID = DCO.AGREGATEDOBJECTID
INNER JOIN STAG_AGREEMENTVERSION AV ON AV.AST_ID = RAP.RSA_AST_ID
INNER JOIN PREAGREEMENT PREA ON PREA.PK = AV.DCOID
INNER JOIN STPS_AGREEMENTSTATICDAT AGS ON RAP.RSA_AST_ID = AGS.AST_ID
"""

df_poliza_matriz = spark.sql(sql_poliza_matriz)
df_poliza_matriz.createOrReplaceTempView("TMP_POLIZA_MATRIZ")
df_poliza_matriz.show(5)
print("✅ Campo POLIZA_MATRIZ generado correctamente en vista temporal: TMP_POLIZA_MATRIZ")

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# -----------------------------
# SparkSession configurada para Glue G1X
# -----------------------------
spark = SparkSession.builder \
    .appName("Optimizado_ID_TOMADOR_POLIZA_reparticionado") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

S3_BASE_PATH = "s3://alfa-datalake-dev-silver/ACSELE/TRANSFORMATION/"

def cargar(nombre, columnas):
    return spark.read.parquet(f"{S3_BASE_PATH}{nombre}/").select(*columnas)

# Cargar tablas (solo las muy pequeñas con broadcast)
df_par = cargar("RI_PARTICIPATION", ["THIRDPARTYID", "PERCENTAGE"])
df_tp = broadcast(cargar("STTE_THIRDPARTY", ["TPT_ID", "IDDCO"]))
df_pre = broadcast(cargar("PRETHIRDPARTY", ["PK", "DOCUMENTOIDENTIDADINPUT"]))

# Reparticionar de forma controlada (evitar skew)
df_rod = (
    cargar("REINSURANCEOPERATIONDETAIL", ["REINOPERCOMPONENTID", "THIRDPARTYID"])
    .repartition(200)  # Mejora la distribución sin usar una columna sesgada
)

# Resto de tablas
df_roc = cargar("REINSURANCEOPERATIONCOMPONENT", ["REINOPERCOMPONENTID", "REINSURANCEGROUPINFOID"])
df_rog = cargar("REINSURANCEOPERATIONGROUPINFO", ["REINSURANCEGROUPINFOID", "OPERATIONID"])
df_ro = cargar("REINSURANCEOPERATION", ["OPERATIONID", "AGREGATEDRISKUNITID"])
df_rud = cargar("RISKUNITDCO", ["AGREGATEDOBJECTID", "OPERATIONPK"])

# Join completo optimizado
joined = (
    df_par
    .join(df_tp, df_par.THIRDPARTYID == df_tp.TPT_ID, "inner")
    .join(df_pre, df_tp.IDDCO == df_pre.PK, "inner")
    .join(df_rod, df_par.THIRDPARTYID == df_rod.THIRDPARTYID, "inner")
    .join(df_roc, df_rod.REINOPERCOMPONENTID == df_roc.REINOPERCOMPONENTID, "inner")
    .join(df_rog, df_roc.REINSURANCEGROUPINFOID == df_rog.REINSURANCEGROUPINFOID, "inner")
    .join(df_ro, df_rog.OPERATIONID == df_ro.OPERATIONID, "inner")
    .join(df_rud, df_ro.AGREGATEDRISKUNITID == df_rud.AGREGATEDOBJECTID, "inner")
    .select(
        df_rud.OPERATIONPK.alias("DCOID"),
        df_par.PERCENTAGE,
        df_pre.DOCUMENTOIDENTIDADINPUT.alias("ID_TOMADOR_POLIZA"),
        df_tp.TPT_ID.alias("THIRDPARTY_ID")
    )
)

# Vista temporal
joined.createOrReplaceTempView("TMP_ID_TOMADOR_POLIZA")
joined.show(5)
print("✅ Campo ID_TOMADOR_POLIZA generado sin persistencia ni shuffle skew.")

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

# -----------------------------
# Ruta base en S3
# -----------------------------
S3_BASE_PATH = "s3://alfa-datalake-dev-silver/ACSELE/TRANSFORMATION/"

# -----------------------------
# Función para cargar tablas
# -----------------------------
def cargar_tabla(nombre):
    df = spark.read.parquet(f"{S3_BASE_PATH}{nombre}/")
    df.createOrReplaceTempView(nombre)
    return df

# -----------------------------
# Cargar tablas necesarias
# -----------------------------
cargar_tabla("OPENITEM")
cargar_tabla("STCA_DOCTYPE")
cargar_tabla("UAADETAIL")
cargar_tabla("PROPERTY")
cargar_tabla("STST_SYSTEMPROPERTY")

# -----------------------------
# Explosión de SYP_VALUE como lista de DTY_DESCRIPTION válidas
# -----------------------------
sysprop_df = spark.sql("""
    SELECT SYP_VALUE
    FROM STST_SYSTEMPROPERTY
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
FROM OPENITEM OI
INNER JOIN STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
INNER JOIN UAADETAIL DET ON DET.OPENITEMID = OI.OPENITEMID
INNER JOIN PROPERTY PP ON PP.PROPERTYID = DET.PROPERTY
WHERE OI.OPM_SUBSTATUS <> 'notApplied'
  AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM COMM_INTERMEDIARY_DOCS)
  AND PP.SIMBOLO = 'IvaComision'
GROUP BY OI.OPERATIONPK
"""

df_iva_comision_intermediacion = spark.sql(sql_iva_comision_intermediacion)
df_iva_comision_intermediacion.createOrReplaceTempView("TMP_IVA_COMISION_INTERMEDIACION")
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

# -----------------------------
# Ruta base en S3
# -----------------------------
S3_BASE_PATH = "s3://alfa-datalake-dev-silver/ACSELE/TRANSFORMATION/"

# -----------------------------
# Función para cargar tablas
# -----------------------------
def cargar_tabla(nombre):
    df = spark.read.parquet(f"{S3_BASE_PATH}{nombre}/")
    df.createOrReplaceTempView(nombre)
    return df

# -----------------------------
# Cargar tablas necesarias
# -----------------------------
cargar_tabla("OPENITEM")
cargar_tabla("STCA_DOCTYPE")
cargar_tabla("UAADETAIL")
cargar_tabla("PROPERTY")
cargar_tabla("STST_SYSTEMPROPERTY")

# -----------------------------
# Explosión de descripciones válidas desde STST_SYSTEMPROPERTY
# -----------------------------
sysprop_df_recaudo = spark.sql("""
    SELECT SYP_VALUE
    FROM STST_SYSTEMPROPERTY
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
FROM OPENITEM OI
INNER JOIN STCA_DOCTYPE DT ON DT.DTY_ID = OI.DTY_ID
INNER JOIN UAADETAIL DET ON DET.OPENITEMID = OI.OPENITEMID
INNER JOIN PROPERTY PP ON PP.PROPERTYID = DET.PROPERTY
WHERE OI.OPM_SUBSTATUS <> 'notApplied'
  AND DT.DTY_DESCRIPTION IN (SELECT DESCRIPTION FROM COMM_INTERMEDIARY_DOCS_RECAUDO)
  AND PP.SIMBOLO = 'IvaComision'
GROUP BY OI.OPERATIONPK
"""

df_iva_recaudo = spark.sql(sql_iva_recaudo)
df_iva_recaudo.createOrReplaceTempView("TMP_IVA_COMISION_RECAUDO")
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

# -----------------------------
# Ruta base en S3
# -----------------------------
S3_BASE_PATH = "s3://alfa-datalake-dev-silver/ACSELE/TRANSFORMATION/"

# -----------------------------
# Función para cargar tablas
# -----------------------------
def cargar_tabla(nombre):
    df = spark.read.parquet(f"{S3_BASE_PATH}{nombre}/")
    df.createOrReplaceTempView(nombre)
    return df

# -----------------------------
# Cargar tablas necesarias
# -----------------------------
for tabla in [
    "OPENITEM", "STCA_DOCTYPE", "UAADETAIL", "PROPERTY", "STST_SYSTEMPROPERTY",
    "REINSURANCEOPERATION", "REINSURANCEOPERATIONGROUPINFO",
    "REINSURANCEOPERATIONCOMPONENT", "RI_REINSURANCEGROUP", "RISKUNITDCO"
]:
    cargar_tabla(tabla)

# -----------------------------
# Crear tabla unificada de descripciones
# -----------------------------
df_sysprop = spark.sql("SELECT * FROM STST_SYSTEMPROPERTY") \
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
FROM OPENITEM OI
JOIN STCA_DOCTYPE DT ON OI.DTY_ID = DT.DTY_ID
LEFT JOIN UAADETAIL DET ON DET.OPENITEMID = OI.OPENITEMID
LEFT JOIN PROPERTY PP ON PP.PROPERTYID = DET.PROPERTY
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
FROM REINSURANCEOPERATION RO
INNER JOIN REINSURANCEOPERATIONGROUPINFO ROG ON ROG.OPERATIONID = RO.OPERATIONID
INNER JOIN REINSURANCEOPERATIONCOMPONENT ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
INNER JOIN RI_REINSURANCEGROUP RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
INNER JOIN RISKUNITDCO RUD ON RUD.AGREGATEDOBJECTID = RO.AGREGATEDRISKUNITID
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

df_total.show(5, truncate=False)
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# -----------------------------
# Configuración para ejecución en Glue G1X con 10 workers
# -----------------------------
spark = SparkSession.builder \
    .appName("Campo_PRIMACEDFAC") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

S3_BASE_PATH = "s3://alfa-datalake-dev-silver/ACSELE/TRANSFORMATION/"

# -----------------------------
# Función para cargar vistas
# -----------------------------
def cargar_tabla(nombre):
    df = spark.read.parquet(f"{S3_BASE_PATH}{nombre}/")
    df.createOrReplaceTempView(nombre)
    return df

# -----------------------------
# Cargar tablas necesarias
# -----------------------------
cargar_tabla("POLICYDCO")
cargar_tabla("REINSURANCEOPERATION")
cargar_tabla("REINSURANCEOPERATIONGROUPINFO")
cargar_tabla("REINSURANCEOPERATIONCOMPONENT")
cargar_tabla("REINSURANCEOPERATIONDETAIL")
cargar_tabla("RI_REINSURANCEGROUP")

# -----------------------------
# Lógica del campo PRIMACEDAUT_INT
# -----------------------------

#dcoid es endoso
#FECHA_FIN_POLIZA_MATRIZ = fecha_fin_poliza_matriz

sql_primacedfac = """
SELECT 
    PLO.OPERATIONPK,
    PLO.DCOID,
    RO.OPERATIONID,
    ROC.REINSURANCEGROUPINFOID,
    ROD.REINOPERCOMPONENTID,
    RG.REINSURANCEGROUPID,
    IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),'0','DETAIL_PREMIUM') AS PRIMACEDAUT,
    IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),'DETAIL_PREMIUM','0') AS PRIMACEDFAC,
    IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),'0','REINSURANCE_COMISSION') AS COMISIONREASEGAUT,
    IF(ROC.CONTRACTTYPE in ('FacultativeReinsuranceContract', 'MandatoryFacultativeContract'),'REINSURANCE_COMISSION','0') AS COMISIONREASEGFAC
FROM reinsuranceoperation RO
INNER JOIN reinsuranceoperationgroupinfo  ROG ON ROG.OPERATIONID = RO.OPERATIONID
INNER JOIN reinsuranceoperationcomponent ROC ON ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
INNER JOIN reinsuranceoperationdetail  ROD ON ROD.REINOPERCOMPONENTID = ROC.REINOPERCOMPONENTID
INNER JOIN ri_reinsurancegroup  RG ON RG.REINSURANCEGROUPID = ROG.GROUPID
INNER JOIN policydco PLO ON RO.cor_id = PLO.operationpk
"""

df_primacedfac = spark.sql(sql_primacedfac)
df_primacedfac.createOrReplaceTempView("TMP_PRIMACEDFAC")
df_primacedfac.show(5)
print("✅ Campo PRIMACEDFAC generado correctamente en vista temporal: TMP_PRIMACEDFAC")
job.commit()