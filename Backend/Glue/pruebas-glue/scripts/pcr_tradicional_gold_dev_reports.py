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
--WHERE pb.DCOID = 986530143
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
df_producto.write.mode("overwrite").parquet("s3://alfa-datalake-dev-gold/GOLD/NIIF/PCR_TRADICIONAL/")


