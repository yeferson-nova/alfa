"""Preparacion PCR Vida en Glue 4.0. Version de prueba, no certificada por QA.
No usa JDBC ni modifica Oracle. Escribe salida y actualiza runtime solo si se ejecuta.
"""
import sys
import json
import uuid
from datetime import datetime
from urllib.parse import urlparse
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark import StorageLevel

MODULOS = {'VIDA_PARTICIPANTES': 'SELECT DISTINCT PP.AGREGATEDPARENTID AS POLIZA, ROL.ROL_DESCRIPTION,\n PT.DOCUMENTOIDENTIDADINPUT, PT.NOMBREPREDETERMINADOINPUT, PPPOL.STATUS\n FROM STPO_POLICYPARTICIPATIONDCO PPPOL\n JOIN STPO_POLICYPARTICIPATION PP ON PP.AGREGATEDOBJECTID=PPPOL.AGREGATEDOBJECTID\n JOIN STTS_ROLE ROL ON ROL.ROL_ID=PP.ROL_ID\n JOIN PRETHIRDPARTY PT ON PT.STATIC=PP.THIRDPARTYID', 'VIDA_TOMADOR_CANDIDATOS': "SELECT DISTINCT POLIZA, DOCUMENTOIDENTIDADINPUT AS IDENTIFICACION_TOMADOR, NOMBREPREDETERMINADOINPUT AS TOMADOR FROM VIDA_PARTICIPANTES WHERE UPPER(ROL_DESCRIPTION) LIKE '%TOMADOR%'", 'VIDA_TOMADOR': 'SELECT POLIZA, MAX(IDENTIFICACION_TOMADOR) AS IDENTIFICACION_TOMADOR, MAX(TOMADOR) AS TOMADOR FROM VIDA_TOMADOR_CANDIDATOS GROUP BY POLIZA', 'VIDA_MAESTRO_CANDIDATOS': "SELECT DISTINCT POLIZA, CONCAT(COALESCE(DOCUMENTOIDENTIDADINPUT,''), '-', COALESCE(NOMBREPREDETERMINADOINPUT,'')) AS INFORMACION_PAGADOR_MAESTRO FROM VIDA_PARTICIPANTES WHERE UPPER(ROL_DESCRIPTION) LIKE '%MAESTRO%' AND STATUS=1", 'VIDA_MAESTRO': 'SELECT POLIZA, MAX(INFORMACION_PAGADOR_MAESTRO) AS INFORMACION_PAGADOR_MAESTRO FROM VIDA_MAESTRO_CANDIDATOS GROUP BY POLIZA', 'VIDA_AGENTE_CANDIDATOS': "SELECT DISTINCT POLIZA, NOMBREPREDETERMINADOINPUT AS AGENTE FROM VIDA_PARTICIPANTES WHERE UPPER(ROL_DESCRIPTION) LIKE '%AGENTE%'", 'VIDA_AGENTE': 'SELECT POLIZA, MAX(AGENTE) AS AGENTE FROM VIDA_AGENTE_CANDIDATOS GROUP BY POLIZA', 'VIDA_ASEGURADO': "SELECT PPDCO.OPERATIONPK, MAX(PT.NOMBREPREDETERMINADOINPUT) AS PNOMASEG,\n MAX(PT.DOCUMENTOIDENTIDADINPUT) AS PIDENTASEG, MAX(PT.TIPODOCUMENTOINPUT) AS PTIPIDENTASEG\n FROM STPO_INSOBJPARTICIPATION PP\n JOIN STPO_INSOBJPARTICIPATIONDCO PPDCO ON PPDCO.AGREGATEDOBJECTID=PP.AGREGATEDOBJECTID AND PPDCO.STATUS=1\n JOIN STTS_ROLE ROL ON ROL.ROL_ID=PP.ROL_ID AND ROL.ROL_DESCRIPTION LIKE '%Asegurado%'\n JOIN AGREGATEDINSURANCEOBJECT AGO ON PP.AGREGATEDPARENTID=AGO.AGREGATEDINSURANCEOBJECTID\n JOIN PRETHIRDPARTY PT ON PT.STATIC=PP.THIRDPARTYID GROUP BY PPDCO.OPERATIONPK", 'VIDA_MIN_OBJETO': 'SELECT AGREGATEDPARENTID, OPERATIONPK, MIN(AGREGATEDOBJECTID) AS MIN_ID FROM INSURANCEOBJECTDCO GROUP BY AGREGATEDPARENTID, OPERATIONPK', 'VIDA_BASE': "SELECT\n  AP.PRODUCTID AS IDPRODUCTO,\n  PREP.COMPANIAVALUE AS CODIGO_COMPANIA,\n  PREP.COMPANIAINPUT AS COMPANIA,\n  PREP.STATIC AS PREP_STATIC,\n  PREP.DESCRIPCIONPRODUCTOINPUT AS PRODUCTO,\n  PREP.RAMOVALUE AS CODIGO_RAMO,\n  PREP.RAMOINPUT AS RAMO,\n  PP.OFICINAVALUE AS CODIGO_SUCURSAL,\n  PP.OFICINAINPUT AS SUCURSAL,\n  TOM.IDENTIFICACION_TOMADOR AS IDENTIFICACION_TOMADOR,\n  TOM.TOMADOR AS TOMADOR,\n  MAE.INFORMACION_PAGADOR_MAESTRO AS INFORMACION_PAGADOR_MAESTRO,\n  SR.RSA_AST_ID AS IDACUERDO,\n  ST.AST_NUMEROACU AS NUMERO_POLIZA_MATRIZ,\n  ST.AST_ID AS ST_AST_ID,\n  AGRV.AST_FECINI AS FECHAINI_ACUERDO,\n  AGRV.AST_FECFIN AS FECHAFIN_ACUERDO,\n  PR.DESCRIPTION AS PRODUCTO_DEL_ACUERDO,\n  PP.TIPOPOLIZAINPUT AS TIPO_POLIZA,\n  PP.NUMEROPOLIZAINPUT AS NRO_POLIZA,\n  CO.TIME_STAMP AS FECHA_EMISION,\n  DATE_FORMAT(CO.TIME_STAMP, 'yyyyMM') AS PERIODO_EMISION,\n  PO.INITIALDATE AS FECHA_INCIAL_POL,\n  PO.FINISHDATE AS FECHA_FINAL_POL,\n  LIUNC.TIPOPRODUCCIONINPUT AS TIPO_PRODUCCION,\n  LIUNC.UNIDADNEGOCIOINPUT AS UNIDAD_NEGOCIO,\n  LIUNC.UNIDADNEGOCIOVALUE AS COD_UNIDAD_NEGOCIO,\n  CO.STATUS AS ESTADO_POLIZA,\n  CO.ID AS NRO_OPERACION,\n  AGRV.DESCRIPTION AS LINEA_CREDITO,\n  LIUNC.LINEACREDITOINPUT AS LINEA_CREDITO_NEGO,\n  COALESCE(EVENT.DESCRIPTION, PO.EVENT) AS DESCRIPCION_EVENTO,\n  PP.MONEDAINPUT AS MONEDA_ACUERDO,\n  RURPOL.AGREGATEDRISKUNITID AS UNIDAD_RIESGO,\n  URPOL.NUMEROCREDITOINPUT AS NUMERO_CREDITO,\n  URPOL.FECHADESEMBOLSOINPUT AS FECHA_DESEMBOLSO,\n  RI.INITIALDATE AS FECHA_INICIAL_UR,\n  RI.FINISHDATE AS FECHA_FINAL_UR,\n  AGE.AGENTE AS Agente,\n  ASE.PNOMASEG AS PNOMASEG,\n  ASE.PIDENTASEG AS PIDENTASEG,\n  ASE.PTIPIDENTASEG AS PTIPIDENTASEG,\n  PO.OPERATIONPK AS PO_OPERATIONPK,\n  PO.DCOID AS PO_DCOID,\n  PO.AGREGATEDOBJECTID,\n  PP.PK AS PP_PK,\n  PP.STATIC AS PP_STATIC,\n  PP.FRECUENCIAPAGOINPUT,\n  SR.RSA_POLICYID,\n  RI.OPERATIONPK AS RI_OPERATIONPK,\n  AGRV.AST_ID AS AGRV_AST_ID,\n  AGRV.DCOID AS AGRV_DCOID,\n  LIUNC.PK AS LIUNC_PK,\n  AP.AGREGATEDPOLICYID,\n  OASEGVD.EDADVALUE AS EDAD_ASEGUADO,\n  OASEGVD.FECHANACIMIENTOASEGINPUT AS FECHA_NACER_ASEGURADO,\n  OASEGVD.DIRECCIONASEGURADOINPUT AS DIRECCION_ASEGUADO,\n  OASEGVD.DEPARTAMENTOINPUT AS DESC_DEPARTAMENTO,\n  OASEGVD.DEPARTAMENTOVALUE AS COD_DEPARTAMENTO,\n  OASEGVD.CIUDADINPUT AS DESC_CIUDAD,\n  OASEGVD.CIUDADVALUE AS COD_CIUDAD,\n  OASEGVD.FACTORVIDAVALUE AS FACTOR_VIDA,\n  NULLIF(\n    CONCAT(\n      COALESCE(SUBSTRING(OASEGVD.DEPARTAMENTOVALUE, 1, LENGTH(OASEGVD.DEPARTAMENTOVALUE) - 2), ''),\n      COALESCE(LPAD(REPLACE(OASEGVD.CIUDADVALUE, '.0', ''), 3, '0'), '')\n    ),\n    ''\n  ) AS CODIGO_DIVIPOLA_MUNICIPIO\nFROM CONTEXTOPERATION AS CO\nINNER JOIN POLICYDCO AS PO\n  ON PO.OPERATIONPK = CO.ID\nINNER JOIN PREPOLICY AS PP\n  ON PP.PK = PO.DCOID\nINNER JOIN STPS_RELATIONAGREEMENTPOLICY AS SR\n  ON PP.STATIC = SR.RSA_POLICYID\nINNER JOIN STPS_AGREEMENTSTATICDAT AS ST\n  ON ST.AST_ID = SR.RSA_AST_ID\nINNER JOIN RISKUNITDCO AS RI\n  ON RI.OPERATIONPK = CO.ID\nINNER JOIN STAG_AGREEMENTVERSION AS AGRV\n  ON ST.AST_ID = AGRV.AST_ID\n  AND AGRV.AST_FECINI <= RI.INITIALDATE\n  AND AGRV.AST_FECFIN >= RI.INITIALDATE\nLEFT JOIN PREAGREEMENT AS LIUNC\n  ON LIUNC.PK = AGRV.DCOID\nINNER JOIN AGREGATEDPOLICY AS AP\n  ON AP.AGREGATEDPOLICYID = PO.AGREGATEDOBJECTID\nLEFT JOIN AGREGATEDRISKUNIT AS RURPOL\n  ON RI.OPERATIONPK = RURPOL.OPERATIONPK\nINNER JOIN URVDEUDORES AS URPOL\n  ON RI.DCOID = URPOL.PK\nINNER JOIN PREPRODUCT AS PREP\n  ON PREP.STATIC = AP.PRODUCTID AND PREP.STATIC = 48897 AND PREP.COMPANIAVALUE = '50.0'\nINNER JOIN PRODUCT AS PR\n  ON PR.PRODUCTID = PREP.STATIC\nINNER JOIN EVENTDCO AS ED\n  ON ED.OPERATIONPK = PO.OPERATIONPK AND ED.IDDCOEVENT = PO.IDDCOEVENT\nLEFT OUTER JOIN EVENTTYPE AS EVENT\n  ON EVENT.EVENTTYPEID = ED.EVENTTYPEID\nINNER JOIN VIDA_MIN_OBJETO AS MINOBJ\n  ON MINOBJ.AGREGATEDPARENTID = RI.AGREGATEDOBJECTID\n  AND MINOBJ.OPERATIONPK = PO.OPERATIONPK\nINNER JOIN INSURANCEOBJECTDCO AS IODCO\n  ON IODCO.AGREGATEDPARENTID = RI.AGREGATEDOBJECTID\n  AND PO.OPERATIONPK = IODCO.OPERATIONPK\n  AND IODCO.STATUS = 1\n  AND IODCO.AGREGATEDOBJECTID = MINOBJ.MIN_ID\nINNER JOIN OAVDEUDORES AS OASEGVD\n  ON IODCO.DCOID = OASEGVD.PK\nLEFT JOIN VIDA_TOMADOR AS TOM\n  ON TOM.POLIZA = AP.AGREGATEDPOLICYID\nLEFT JOIN VIDA_MAESTRO AS MAE\n  ON MAE.POLIZA = AP.AGREGATEDPOLICYID\nLEFT JOIN VIDA_AGENTE AS AGE\n  ON AGE.POLIZA = AP.AGREGATEDPOLICYID\nLEFT JOIN VIDA_ASEGURADO AS ASE\n  ON ASE.OPERATIONPK = PO.OPERATIONPK\nWHERE\n  CO.STATUS = 2", 'VIDA_MOVIMIENTOS': "SELECT\n  CO.IDPRODUCTO,\n  CO.CODIGO_COMPANIA,\n  CO.COMPANIA,\n  CO.PREP_STATIC,\n  CO.PRODUCTO,\n  CO.CODIGO_RAMO,\n  CO.RAMO,\n  CO.CODIGO_SUCURSAL,\n  CO.SUCURSAL,\n  CO.IDENTIFICACION_TOMADOR,\n  CO.TOMADOR,\n  CO.INFORMACION_PAGADOR_MAESTRO,\n  CO.IDACUERDO,\n  CO.NUMERO_POLIZA_MATRIZ,\n  CO.ST_AST_ID,\n  CO.FECHAINI_ACUERDO,\n  CO.FECHAFIN_ACUERDO,\n  CO.PRODUCTO_DEL_ACUERDO,\n  CO.TIPO_POLIZA,\n  CO.NRO_POLIZA,\n  CO.FECHA_EMISION,\n  CO.PERIODO_EMISION,\n  CO.FECHA_INCIAL_POL,\n  CO.FECHA_FINAL_POL,\n  CO.TIPO_PRODUCCION,\n  CO.UNIDAD_NEGOCIO,\n  CO.COD_UNIDAD_NEGOCIO,\n  CO.ESTADO_POLIZA,\n  CO.NRO_OPERACION,\n  CO.LINEA_CREDITO,\n  CO.LINEA_CREDITO_NEGO,\n  CO.DESCRIPCION_EVENTO,\n  CO.MONEDA_ACUERDO,\n  CO.UNIDAD_RIESGO,\n  CO.NUMERO_CREDITO,\n  CO.FECHA_DESEMBOLSO,\n  CO.FECHA_INICIAL_UR,\n  CO.FECHA_FINAL_UR,\n  CO.AGENTE,\n  CO.PNOMASEG,\n  CO.PIDENTASEG,\n  CO.PTIPIDENTASEG,\n  CO.FRECUENCIAPAGOINPUT,\n  CO.RSA_POLICYID,\n  CO.RI_OPERATIONPK,\n  CO.AGRV_DCOID,\n  CO.LIUNC_PK,\n  CO.AGREGATEDPOLICYID,\n  CO.EDAD_ASEGUADO,\n  CO.FECHA_NACER_ASEGURADO,\n  CO.DIRECCION_ASEGUADO,\n  CO.DESC_DEPARTAMENTO,\n  CO.COD_DEPARTAMENTO,\n  CO.DESC_CIUDAD,\n  CO.COD_CIUDAD,\n  CO.FACTOR_VIDA,\n  CO.CODIGO_DIVIPOLA_MUNICIPIO,\n  TM.OPERATIONPK,\n  TM.CONCEPT,\n  TM.AMMOUNT\nFROM THIRDPARTYMOVEMENTPOLICY AS TM, VIDA_BASE AS CO\nWHERE\n  CO.NRO_OPERACION = TM.OPERATIONPK\n  AND TM.AMMOUNT <> 0\n  AND UPPER(CONCEPT) IN (\n    'PRIMAEMITIDA',\n    'PRIMADEVUELTA',\n    'PRIMAANU',\n    'PRIMAREHAB',\n    'PRIMAREHABIND',\n    'PRIMAPERIODOPROVIS',\n    'PRIMAANUIND',\n    'PRIMAEMITIDAIND',\n    'PRIMADEVUELTAIND',\n    'PRIMAPERIODOPROVISIND',\n    'PRIMAREHABIND',\n    'PRIMAEMITIDADEV',\n    'COSTORECAUDACIONDEV',\n    'COSTORECAUDACIONDEVIND',\n    'COSTORECAUDOANU',\n    'COSTORECAUDOANUIND',\n    'COSTORECAUDACION',\n    'COSTORECAUDACIONIND',\n    'COSTORECAUDOPROVIS',\n    'COSTORECAUDOPROVISIND',\n    'COSTORECAUDOREHAB',\n    'COSTORECAUDOREHABIND',\n    'IVACOSTORECAUDACIONDEV',\n    'IVACOSTORECAUDACIONDEVIND',\n    'IVACOSTORECAUDOANU',\n    'IVACOSTORECAUDOANUIND',\n    'IVACOSTORECAUDACION',\n    'IVACOSTORECAUDACIONIND',\n    'IVACOSTORECAUDOPROVIS',\n    'IVACOSTORECAUDOPROVISIND',\n    'IVACOSTORECAUDOREHAB',\n    'IVACOSTORECAUDOREHABIND',\n    'IVACOSTORECAUDO',\n    'COMISIONVIDADEV',\n    'COMISIONVIDAINDDEV',\n    'COMISIONVIDA',\n    'COMISIONVIDAIND',\n    'SUMAASEGPERIODO',\n    'PRIMA',\n    'PRIMADEV',\n    'PRIMAEMITIDAPAGVIDAAP',\n    'PRIMAEMITIDAVIDAAP',\n    'PRIMAEMITIDAPAGDEVVIDAAP',\n    'PRIMAEMITIDAANUVIDAAP',\n    'COMISIONAGENTE',\n    'COMISIONAGENTEDEV',\n    'COSTORECAUDACIONDEVVIDAAP',\n    'COSTORECAUDACIONANUVIDAAP',\n    'COSTORECAUDACIONANUVIDAAP',\n    'COSTORECAUDACIONDEVVIDAAP',\n    'COSTORECAUDACIONPAGVIDAAP',\n    'COSTORECAUDACIONREHABVIDAAP',\n    'COSTORECAUDACIONVIDAAP',\n    'IVACOSTORECAUDACIONDEVVIDAAP',\n    'IVACOSTORECAUDACIONANUVIDAAP',\n    'IVACOSTORECAUDACIONVIDAAP',\n    'IVACOSTORECAUDACIONPAGVIDAAP',\n    'IVACOSTORECAUDACIONDEVVIDAAP',\n    'IVACOSTORECAUDACIONANUVIDAAP',\n    'IVACOSTORECAUDACIONREHABVIDAAP',\n    'COSTORECAUDOINCANU',\n    'COSTORECAUDOINCENDIODEV',\n    'COSTORECAUDOINCPROVIS',\n    'COSTORECAUDOINCREHAB',\n    'COSTORECAUDOINCENDIO',\n    'COSTORECAUDOTERREMOTODEV',\n    'COSTORECAUDOTERRANU',\n    'COSTORECAUDOTERREMOTO',\n    'COSTORECAUDOTERRREHAB',\n    'COSTORECAUDOTERRPROVIS'\n  )", 'VIDA_FINAL': "SELECT\n  TM.IDPRODUCTO,\n  TM.CODIGO_COMPANIA,\n  TM.COMPANIA,\n  TM.PREP_STATIC AS CODIGO_PRODUCTO,\n  TM.PRODUCTO,\n  TM.CODIGO_RAMO,\n  TM.RAMO,\n  TM.CODIGO_SUCURSAL,\n  TM.SUCURSAL,\n  TM.IDENTIFICACION_TOMADOR,\n  TM.TOMADOR,\n  TM.INFORMACION_PAGADOR_MAESTRO AS PAGADOR_MAESTRO,\n  TM.IDACUERDO,\n  TM.NUMERO_POLIZA_MATRIZ AS ACUERDO,\n  TM.ST_AST_ID,\n  TM.FECHAINI_ACUERDO,\n  TM.FECHAFIN_ACUERDO,\n  TM.PRODUCTO_DEL_ACUERDO,\n  TM.TIPO_POLIZA,\n  TM.NRO_POLIZA,\n  TM.FECHA_EMISION,\n  TM.PERIODO_EMISION AS PERIODO_OPERACION,\n  TM.FECHA_INCIAL_POL,\n  TM.FECHA_FINAL_POL,\n  TM.TIPO_PRODUCCION,\n  TM.UNIDAD_NEGOCIO,\n  TM.COD_UNIDAD_NEGOCIO AS CODIGO_UNIDAD_NEGOCIO,\n  TM.ESTADO_POLIZA,\n  TM.NRO_OPERACION,\n  TM.LINEA_CREDITO,\n  TM.LINEA_CREDITO_NEGO,\n  TM.DESCRIPCION_EVENTO,\n  TM.MONEDA_ACUERDO,\n  TM.UNIDAD_RIESGO,\n  TM.NUMERO_CREDITO,\n  TM.FECHA_DESEMBOLSO,\n  TM.FECHA_INICIAL_UR,\n  TM.FECHA_FINAL_UR,\n  TM.AGENTE,\n  TM.PNOMASEG,\n  TM.PIDENTASEG,\n  TM.PTIPIDENTASEG,\n  TM.FRECUENCIAPAGOINPUT AS FRECUENCIA_PAGO,\n  TM.RSA_POLICYID,\n  TM.RI_OPERATIONPK,\n  TM.AGRV_DCOID,\n  TM.LIUNC_PK,\n  TM.AGREGATEDPOLICYID,\n  TM.OPERATIONPK,\n  TM.EDAD_ASEGUADO,\n  TM.FECHA_NACER_ASEGURADO,\n  TM.DIRECCION_ASEGUADO,\n  TM.DESC_DEPARTAMENTO,\n  TM.COD_DEPARTAMENTO,\n  TM.DESC_CIUDAD,\n  TM.COD_CIUDAD,\n  TM.FACTOR_VIDA,\n  TM.CODIGO_DIVIPOLA_MUNICIPIO,\n  SUM(\n    CASE\n      WHEN UPPER(TM.CONCEPT) IN (\n        'PRIMADEVUELTA',\n        'PRIMAANU',\n        'PRIMAANUIND',\n        'PRIMADEVUELTAIND',\n        'PRIMAEMITIDADEV',\n        'PRIMAEMITIDAANUVIDAAP',\n        'PRIMAEMITIDAPAGDEVVIDAAP'\n      )\n      THEN TM.AMMOUNT * (\n        -1\n      )\n      WHEN UPPER(TM.CONCEPT) IN (\n        'PRIMAEMITIDA',\n        'PRIMAREHAB',\n        'PRIMAREHABIND',\n        'PRIMAPERIODOPROVIS',\n        'PRIMAEMITIDAIND',\n        'PRIMAPERIODOPROVISIND',\n        'PRIMAREHABIND',\n        'PRIMAEMITIDAVIDAAP',\n        'PRIMAEMITIDAPAGVIDAAP'\n      )\n      THEN TM.AMMOUNT\n    END\n  ) AS PRIMA,\n  SUM(\n    CASE\n      WHEN UPPER(TM.CONCEPT) IN (\n        'COSTORECAUDACIONDEV',\n        'COSTORECAUDACIONDEVIND',\n        'COSTORECAUDOANU',\n        'COSTORECAUDOANUIND',\n        'COSTORECAUDACIONANUVIDAAP',\n        'COSTORECAUDACIONDEVVIDAAP'\n      )\n      THEN TM.AMMOUNT * (\n        -1\n      )\n      WHEN UPPER(TM.CONCEPT) IN (\n        'COSTORECAUDACION',\n        'COSTORECAUDACIONIND',\n        'COSTORECAUDOPROVIS',\n        'COSTORECAUDOPROVISIND',\n        'COSTORECAUDOREHAB',\n        'COSTORECAUDOREHABIND',\n        'COSTORECAUDACIONREHABVIDAAP',\n        'COSTORECAUDACIONVIDAAP',\n        'COSTORECAUDACIONPAGVIDAAP'\n      )\n      THEN TM.AMMOUNT\n    END\n  ) AS COSTO_RECAUDO,\n  SUM(\n    CASE\n      WHEN UPPER(TM.CONCEPT) IN (\n        'IVACOSTORECAUDACIONDEV',\n        'IVACOSTORECAUDACIONDEVIND',\n        'IVACOSTORECAUDOANU',\n        'IVACOSTORECAUDOANUIND',\n        'IVACOSTORECAUDACIONDEVVIDAAP',\n        'IVACOSTORECAUDACIONANUVIDAAP'\n      )\n      THEN TM.AMMOUNT * (\n        -1\n      )\n      WHEN UPPER(TM.CONCEPT) IN (\n        'IVACOSTORECAUDACION',\n        'IVACOSTORECAUDACIONIND',\n        'IVACOSTORECAUDOPROVIS',\n        'IVACOSTORECAUDOPROVISIND',\n        'IVACOSTORECAUDOREHAB',\n        'IVACOSTORECAUDOREHABIND',\n        'IVACOSTORECAUDACIONVIDAAP',\n        'IVACOSTORECAUDACIONREHABVIDAAP',\n        'IVACOSTORECAUDACIONPAGVIDAAP'\n      )\n      THEN TM.AMMOUNT\n    END\n  ) AS IVA_COSTO_RECAUDO,\n  SUM(\n    CASE\n      WHEN UPPER(TM.CONCEPT) IN ('COMISIONVIDADEV', 'COMISIONVIDAINDDEV', 'COMISIONAGENTEDEV')\n      THEN TM.AMMOUNT * (\n        -1\n      )\n      WHEN UPPER(TM.CONCEPT) IN ('COMISIONVIDA', 'COMISIONVIDAIND', 'COMISIONAGENTE')\n      THEN TM.AMMOUNT\n    END\n  ) AS COMISION,\n  SUM(CASE WHEN TM.CONCEPT IN ('SumaAsegPeriodo') THEN TM.AMMOUNT END) AS VALOR_ASEGURADO\nFROM VIDA_MOVIMIENTOS AS TM\nGROUP BY\n  TM.IDPRODUCTO,\n  TM.CODIGO_COMPANIA,\n  TM.COMPANIA,\n  TM.PREP_STATIC,\n  TM.PRODUCTO,\n  TM.CODIGO_RAMO,\n  TM.RAMO,\n  TM.CODIGO_SUCURSAL,\n  TM.SUCURSAL,\n  TM.IDENTIFICACION_TOMADOR,\n  TM.TOMADOR,\n  TM.INFORMACION_PAGADOR_MAESTRO,\n  TM.IDACUERDO,\n  TM.NUMERO_POLIZA_MATRIZ,\n  TM.ST_AST_ID,\n  TM.FECHAINI_ACUERDO,\n  TM.FECHAFIN_ACUERDO,\n  TM.PRODUCTO_DEL_ACUERDO,\n  TM.TIPO_POLIZA,\n  TM.NRO_POLIZA,\n  TM.FECHA_EMISION,\n  TM.PERIODO_EMISION,\n  TM.FECHA_INCIAL_POL,\n  TM.FECHA_FINAL_POL,\n  TM.TIPO_PRODUCCION,\n  TM.UNIDAD_NEGOCIO,\n  TM.COD_UNIDAD_NEGOCIO,\n  TM.ESTADO_POLIZA,\n  TM.NRO_OPERACION,\n  TM.LINEA_CREDITO,\n  TM.LINEA_CREDITO_NEGO,\n  TM.DESCRIPCION_EVENTO,\n  TM.MONEDA_ACUERDO,\n  TM.UNIDAD_RIESGO,\n  TM.NUMERO_CREDITO,\n  TM.FECHA_DESEMBOLSO,\n  TM.FECHA_INICIAL_UR,\n  TM.FECHA_FINAL_UR,\n  TM.AGENTE,\n  TM.PNOMASEG,\n  TM.PIDENTASEG,\n  TM.PTIPIDENTASEG,\n  TM.FRECUENCIAPAGOINPUT,\n  TM.RSA_POLICYID,\n  TM.RI_OPERATIONPK,\n  TM.AGRV_DCOID,\n  TM.LIUNC_PK,\n  TM.AGREGATEDPOLICYID,\n  TM.OPERATIONPK,\n  TM.EDAD_ASEGUADO,\n  TM.FECHA_NACER_ASEGURADO,\n  TM.DIRECCION_ASEGUADO,\n  TM.DESC_DEPARTAMENTO,\n  TM.COD_DEPARTAMENTO,\n  TM.DESC_CIUDAD,\n  TM.COD_CIUDAD,\n  TM.FACTOR_VIDA,\n  TM.CODIGO_DIVIPOLA_MUNICIPIO"}
REQUERIDAS = {'STPO_POLICYPARTICIPATION': ['AGREGATEDOBJECTID', 'AGREGATEDPARENTID', 'ROL_ID', 'THIRDPARTYID'], 'STTS_ROLE': ['ROL_DESCRIPTION', 'ROL_ID'], 'PRETHIRDPARTY': ['DOCUMENTOIDENTIDADINPUT', 'NOMBREPREDETERMINADOINPUT', 'STATIC', 'TIPODOCUMENTOINPUT'], 'STPO_POLICYPARTICIPATIONDCO': ['AGREGATEDOBJECTID', 'STATUS'], 'STPO_INSOBJPARTICIPATIONDCO': ['AGREGATEDOBJECTID', 'OPERATIONPK', 'STATUS'], 'STPO_INSOBJPARTICIPATION': ['AGREGATEDOBJECTID', 'AGREGATEDPARENTID', 'ROL_ID', 'THIRDPARTYID'], 'AGREGATEDINSURANCEOBJECT': ['AGREGATEDINSURANCEOBJECTID'], 'AGREGATEDPOLICY': ['AGREGATEDPOLICYID', 'PRODUCTID'], 'PREPRODUCT': ['COMPANIAINPUT', 'COMPANIAVALUE', 'DESCRIPCIONPRODUCTOINPUT', 'RAMOINPUT', 'RAMOVALUE', 'STATIC'], 'PREPOLICY': ['FRECUENCIAPAGOINPUT', 'MONEDAINPUT', 'NUMEROPOLIZAINPUT', 'OFICINAINPUT', 'OFICINAVALUE', 'PK', 'STATIC', 'TIPOPOLIZAINPUT'], 'STPS_RELATIONAGREEMENTPOLICY': ['RSA_AST_ID', 'RSA_POLICYID'], 'STPS_AGREEMENTSTATICDAT': ['AST_ID', 'AST_NUMEROACU'], 'STAG_AGREEMENTVERSION': ['AST_FECFIN', 'AST_FECINI', 'AST_ID', 'DCOID', 'DESCRIPTION'], 'PRODUCT': ['DESCRIPTION', 'PRODUCTID'], 'CONTEXTOPERATION': ['ID', 'STATUS', 'TIME_STAMP'], 'POLICYDCO': ['AGREGATEDOBJECTID', 'DCOID', 'EVENT', 'FINISHDATE', 'IDDCOEVENT', 'INITIALDATE', 'OPERATIONPK'], 'PREAGREEMENT': ['LINEACREDITOINPUT', 'PK', 'TIPOPRODUCCIONINPUT', 'UNIDADNEGOCIOINPUT', 'UNIDADNEGOCIOVALUE'], 'EVENTTYPE': ['DESCRIPTION', 'EVENTTYPEID'], 'AGREGATEDRISKUNIT': ['AGREGATEDRISKUNITID', 'OPERATIONPK'], 'URVDEUDORES': ['FECHADESEMBOLSOINPUT', 'NUMEROCREDITOINPUT', 'PK'], 'RISKUNITDCO': ['AGREGATEDOBJECTID', 'DCOID', 'FINISHDATE', 'INITIALDATE', 'OPERATIONPK'], 'OAVDEUDORES': ['CIUDADINPUT', 'CIUDADVALUE', 'DEPARTAMENTOINPUT', 'DEPARTAMENTOVALUE', 'DIRECCIONASEGURADOINPUT', 'EDADVALUE', 'FACTORVIDAVALUE', 'FECHANACIMIENTOASEGINPUT', 'PK'], 'EVENTDCO': ['EVENTTYPEID', 'IDDCOEVENT', 'OPERATIONPK'], 'INSURANCEOBJECTDCO': ['AGREGATEDOBJECTID', 'AGREGATEDPARENTID', 'DCOID', 'OPERATIONPK', 'STATUS'], 'THIRDPARTYMOVEMENTPOLICY': ['AMMOUNT', 'CONCEPT', 'OPERATIONPK']}

def opcional(nombre, defecto):
    flag = '--' + nombre
    if flag in sys.argv:
        return getResolvedOptions(sys.argv, [nombre])[nombre]
    return defecto

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'RUTA_PARAMETROS_EXTRACCION', 'S3_SLV_PREP_PATH'])
    capa = opcional('CAPA_ENTRADA', 'SILVER').upper()
    if capa not in ('BRONZE', 'SILVER'):
        raise ValueError('CAPA_ENTRADA debe ser BRONZE o SILVER')
    session = boto3.Session()
    if session.client('sts').get_caller_identity()['Account'] != '533267035849':
        raise ValueError('Version controlada solo DEV')
    s3 = session.client('s3')
    def partes(uri):
        p = urlparse(uri)
        if p.scheme != 's3' or not p.netloc or not p.path.strip('/'):
            raise ValueError('URI S3 invalida')
        return p.netloc, p.path.lstrip('/')
    def leer(uri):
        b,k=partes(uri)
        return json.loads(s3.get_object(Bucket=b, Key=k)['Body'].read().decode('utf-8-sig'))
    def guardar(uri, valor):
        b,k=partes(uri)
        s3.put_object(Bucket=b, Key=k, Body=json.dumps(valor,ensure_ascii=False,indent=2).encode(),ContentType='application/json')
    ruta_runtime=args['RUTA_PARAMETROS_EXTRACCION']
    runtime=leer(ruta_runtime)
    if runtime.get('estado_extraccion') != 'SUCCESS' or runtime.get('nombre_caso') != 'pcr_vida_prueba':
        raise ValueError('Runtime no corresponde a extraccion Vida exitosa')
    # VIDA_PREPARACION_PARAMETRIZADA_V1
    import re
    periodo = str(runtime.get('periodo_negocio', ''))
    if not re.fullmatch(r'[0-9]{6}', periodo):
        raise ValueError('Periodo del runtime invalido')
    inicio_dt = datetime.strptime(periodo, '%Y%m')
    if inicio_dt.strftime('%Y%m') != periodo:
        raise ValueError('Periodo no canonico')
    desde = inicio_dt.strftime('%Y-%m-01')
    hasta = datetime(inicio_dt.year + (inicio_dt.month == 12), inicio_dt.month % 12 + 1, 1).strftime('%Y-%m-%d')
    ruta_cfg_prep = opcional('RUTA_CONFIG_PREPARACION', '')
    cfg_prep = leer(ruta_cfg_prep) if ruta_cfg_prep else {'producto_id':48897,'compania_value':'50.0'}
    producto = cfg_prep['producto_id']
    compania = cfg_prep['compania_value']
    if type(producto) is not int or producto < 1:
        raise ValueError('producto_id debe ser entero positivo')
    if not isinstance(compania,str) or not compania:
        raise ValueError('compania_value debe ser cadena no vacia')
    modulos = dict(MODULOS)
    filtro_original = "PREP.STATIC = 48897 AND PREP.COMPANIAVALUE = '50.0'"
    if modulos['VIDA_BASE'].count(filtro_original) != 1:
        raise ValueError('Filtro producto/compania no reconocido')
    filtro = "PREP.STATIC = " + str(producto) + " AND PREP.COMPANIAVALUE = '" + compania.replace("'", "''") + "'"
    modulos['VIDA_BASE'] = modulos['VIDA_BASE'].replace(filtro_original, filtro, 1)
    if capa=='SILVER':
        if runtime.get('estado_validacion') not in ('SUCCESS','FAILED'):
            raise ValueError('Ejecutar primero validacion tecnica estandar')
        if runtime.get('estado_remediacion')!='SUCCESS' or runtime.get('estado_disponibilizacion_silver')!='SUCCESS':
            raise ValueError('Ejecutar primero disponibilizacion Bronze a Silver')
    gc=GlueContext(SparkContext.getOrCreate());spark=gc.spark_session
    for key in ('spark.sql.parquet.datetimeRebaseModeInRead','spark.sql.parquet.int96RebaseModeInRead','spark.sql.parquet.datetimeRebaseModeInWrite','spark.sql.parquet.int96RebaseModeInWrite'):
        spark.conf.set(key,'LEGACY')
    spark.conf.set('spark.sql.caseSensitive','false')
    spark.conf.set('spark.sql.ansi.enabled','true')
    spark.conf.set('spark.sql.decimalOperations.allowPrecisionLoss','false')
    job=Job(gc);job.init(args['JOB_NAME'],args)
    datasets=runtime['datasets_extraidos']
    indice={d['tabla_relativa'].split('/')[-1].upper():d for d in datasets}
    if len(indice)!=len(datasets):raise ValueError('Datasets ambiguos')
    lote=runtime['lote_cargue_id'];metricas={};cache=[]
    try:
        for nombre, columnas in REQUERIDAS.items():
            if nombre not in indice:raise ValueError('Fuente ausente: '+nombre)
            d=indice[nombre]
            ruta=d['ruta_dataset_destino' if capa=='SILVER' else 'ruta_dataset_origen']
            df=spark.read.parquet(ruta)
            nombres=[c.upper() for c in df.columns]
            if len(nombres)!=len(set(nombres)):raise ValueError('Columnas ambiguas: '+nombre)
            df=df.toDF(*nombres)
            faltantes=set(columnas+['LOTE_CARGUE_ID'])-set(nombres)
            if faltantes:raise ValueError('Columnas ausentes '+nombre+': '+str(sorted(faltantes)))
            lotes=d.get('lotes_preparacion_silver') or [lote]
            df=df.filter(F.col('LOTE_CARGUE_ID').isin([int(x) for x in lotes]))
            # Oracle interpreta cadena vacia como NULL; no se recortan espacios.
            for campo in df.schema.fields:
                if isinstance(campo.dataType,StringType):
                    df=df.withColumn(campo.name,F.when(F.col(campo.name)=='',F.lit(None)).otherwise(F.col(campo.name)))
            df=df.persist(StorageLevel.MEMORY_AND_DISK);cache.append(df)
            n=df.count()
            if n!=int(d['total_registros']):raise ValueError('Conteo distinto del runtime: '+nombre)
            df.createOrReplaceTempView(nombre);metricas[nombre]=n
        pivote=spark.table('CONTEXTOPERATION')
        # Operaciones del pivote real; no colectar el periodo en el driver.
        esperados_df = pivote.select(F.col('ID').alias('NRO_OPERACION')).distinct()
        n_esperados = esperados_df.count()
        if pivote.filter(F.col('ID').isNull()).limit(1).count() or n_esperados != metricas['CONTEXTOPERATION']:
            raise ValueError('Pivote con IDs nulos o duplicados')
        if pivote.filter((F.col('STATUS')!=2)|F.col('STATUS').isNull()|F.col('TIME_STAMP').isNull()|(F.col('TIME_STAMP')<F.lit(desde).cast('timestamp'))|(F.col('TIME_STAMP')>=F.lit(hasta).cast('timestamp'))).limit(1).count():
            raise ValueError('Periodo o estado del pivote incorrecto')

        # Selecciones no ordenadas autorizadas solo para desarrollo provisional.
        from pyspark.sql.types import StructType
        selection_root = opcional('RUTA_SELECCIONES', '')
        if not selection_root.startswith('s3://alfa-datalake-dev-silver/pcr-vida/'):
            raise ValueError('Ruta selecciones no aislada')
        selection_info = {}
        selection_frames = {}
        baseline = {}
        measures = ['PRIMA','COSTO_RECAUDO','IVA_COSTO_RECAUDO','COMISION','VALOR_ASEGURADO']
        def exists(uri):
            b,k=partes(uri)
            return s3.list_objects_v2(Bucket=b,Prefix=k.rstrip('/')+'/',MaxKeys=1).get('KeyCount',0)>0
        def choose(candidate_view, output_view, fields):
            candidates=spark.table(candidate_view).select('POLIZA',*fields).distinct()
            path=selection_root.rstrip('/')+'/'+output_view.lower()+'/'
            selected_path=path+'seleccion/'
            commit_uri=path+'commit.json'
            try:
                commit=leer(commit_uri)
            except s3.exceptions.NoSuchKey:
                commit=None
            except Exception as exc:
                from botocore.exceptions import ClientError
                if isinstance(exc,ClientError) and exc.response['Error']['Code'] in ('NoSuchKey','404','NotFound'):
                    commit=None
                else:raise
            if commit is None:
                # No reutilizar una escritura parcial sin commit.
                import uuid
                attempt=path+'intentos/'+uuid.uuid4().hex+'/'
                selected_path=attempt+'seleccion/'
                candidates_path=attempt+'candidatos/'
                candidates.write.mode('errorifexists').option('compression','snappy').parquet(candidates_path)
                stable_candidates=spark.read.parquet(candidates_path)
                # dropDuplicates conserva una fila completa; no MAX independiente por atributo.
                stable_candidates.dropDuplicates(['POLIZA']).write.mode('errorifexists').option('compression','snappy').parquet(selected_path)
                selected=spark.read.parquet(selected_path)
            else:
                if commit.get('lote')!=lote or commit.get('periodo')!=periodo or commit.get('fields')!=fields:
                    raise ValueError('Seleccion persistida incompatible')
                selected_path=commit['seleccion'];candidates_path=commit['candidatos']
                selected=spark.read.parquet(selected_path)
                stable_candidates=spark.read.parquet(candidates_path)
            # Igualdad de conjuntos con NULL seguro, sin exceptAll.
            def anti(left,right,columns):
                l=left.alias('l');r=right.alias('r')
                cond=None
                for col in columns:
                    term=l[col].eqNullSafe(r[col]);cond=term if cond is None else cond & term
                return l.join(r,cond,'left_anti')
            columns=['POLIZA']+fields
            if anti(candidates,stable_candidates,columns).limit(1).count() or anti(stable_candidates,candidates,columns).limit(1).count():
                raise ValueError('Candidatos cambiaron; no reutilizar seleccion')
            if selected.groupBy('POLIZA').count().filter('count != 1').limit(1).count():raise ValueError('Seleccion no unica')
            if anti(selected,stable_candidates,columns).limit(1).count():raise ValueError('Fila seleccionada no es candidata')
            if anti(stable_candidates.select('POLIZA').distinct(),selected,['POLIZA']).limit(1).count():raise ValueError('Falta poliza en seleccion')
            ambiguous=stable_candidates.groupBy('POLIZA').count().filter('count > 1').select('POLIZA')
            namb=ambiguous.count();nselected=selected.count()
            if commit is None:
                alternatives=anti(stable_candidates,selected,columns)
                alt_path=attempt+'alternativos/'
                alternatives.write.mode('errorifexists').option('compression','snappy').parquet(alt_path)
                commit={'lote':lote,'periodo':periodo,'fields':fields,'seleccion':selected_path,'candidatos':candidates_path,'alternativos':alt_path,'polizas_seleccionadas':nselected,'polizas_ambiguas':namb,'criterio':'SELECCION_NO_ORDENADA','provisional':True}
                guardar(commit_uri,commit)
            selection_info[output_view]=commit
            selected.createOrReplaceTempView(output_view)
            selection_frames[output_view]=selected
            ambiguous.createOrReplaceTempView(output_view+'_AMBIGUOS')
            return selected
        def op_summary(df,with_count=False):
            expr=[F.sum(c).alias(c) for c in measures]
            if with_count:expr.append(F.count('*').alias('_FILAS'))
            return df.groupBy('NRO_OPERACION').agg(*expr)
        def assert_same(left,right,columns,label):
            l=left.alias('l');r=right.alias('r')
            cond=l['NRO_OPERACION'].eqNullSafe(r['NRO_OPERACION'])
            for col in columns:cond=cond & l[col].eqNullSafe(r[col])
            if l.join(r,cond,'left_anti').limit(1).count() or r.join(l,cond,'left_anti').limit(1).count():
                raise ValueError('Diferencias antes/despues: '+label)
        def baseline_nulls():
            # Referencia de control, no referencia E1: mismos joins/filtros/importes,
            # solo atributos tomador/agente NULL; no excluye ninguna poliza.
            for view,fields in [('VIDA_TOMADOR',['IDENTIFICACION_TOMADOR','TOMADOR']),('VIDA_AGENTE',['AGENTE'])]:
                frame=selection_frames[view]
                frame.select('POLIZA',*[F.lit(None).cast(frame.schema[f].dataType).alias(f) for f in fields]).createOrReplaceTempView(view)
            try:
                b=spark.sql(modulos['VIDA_BASE']);b.createOrReplaceTempView('VIDA_BASE')
                m=spark.sql(modulos['VIDA_MOVIMIENTOS']).persist(StorageLevel.MEMORY_AND_DISK);cache.append(m);m.createOrReplaceTempView('VIDA_MOVIMIENTOS')
                f=spark.sql(modulos['VIDA_FINAL'])
                baseline['filas_base']=b.count();baseline['filas_movimientos']=m.count();baseline['filas_final']=f.count()
                baseline['mov_ops']=m.groupBy('NRO_OPERACION').count().persist(StorageLevel.MEMORY_AND_DISK);cache.append(baseline['mov_ops']);baseline['mov_ops'].count()
                baseline['sum_ops']=op_summary(f).persist(StorageLevel.MEMORY_AND_DISK);cache.append(baseline['sum_ops']);baseline['sum_ops'].count()
            finally:
                for view,frame in selection_frames.items():frame.createOrReplaceTempView(view)

        conteos={};etapas={};ambiguos={
            'VIDA_TOMADOR_CANDIDATOS':'TOMADOR',
            'VIDA_MAESTRO_CANDIDATOS':'PAGADOR_MAESTRO',
            'VIDA_AGENTE_CANDIDATOS':'AGENTE'}
        for nombre,sql in modulos.items():
            if nombre=='VIDA_TOMADOR':
                choose('VIDA_TOMADOR_CANDIDATOS','VIDA_TOMADOR',['IDENTIFICACION_TOMADOR','TOMADOR'])
                continue
            if nombre=='VIDA_AGENTE':
                choose('VIDA_AGENTE_CANDIDATOS','VIDA_AGENTE',['AGENTE'])
                continue
            if nombre=='VIDA_BASE':baseline_nulls()
            df=spark.sql(sql)
            if nombre in ambiguos:
                conflictos=df.groupBy('POLIZA').count().filter('count > 1').count()
                if conflictos and nombre not in ('VIDA_TOMADOR_CANDIDATOS','VIDA_AGENTE_CANDIDATOS'):
                    raise ValueError('AMBIGUEDAD '+ambiguos[nombre]+': requiere regla; no se omite')
            if nombre in ('VIDA_BASE','VIDA_MOVIMIENTOS','VIDA_FINAL'):
                df=df.persist(StorageLevel.MEMORY_AND_DISK);cache.append(df)
                conteos[nombre]=df.count();etapas[nombre]=df
                print('ETAPA',nombre,'FILAS',conteos[nombre])
                # Una muestra automatica puede no contener operaciones del producto; se reporta SIN_DATOS.
            df.createOrReplaceTempView(nombre)
        final=etapas['VIDA_FINAL']
        if conteos['VIDA_BASE']!=baseline['filas_base'] or conteos['VIDA_MOVIMIENTOS']!=baseline['filas_movimientos']:
            raise ValueError('Filas base/movimientos cambiaron por seleccion')
        assert_same(baseline['mov_ops'],etapas['VIDA_MOVIMIENTOS'].groupBy('NRO_OPERACION').count(),['count'],'movimientos por operacion')
        assert_same(baseline['sum_ops'],op_summary(final),measures,'importes por operacion')
        # Marcas despues de agrupar: no cambian las sumas ni el conjunto de movimientos.
        for view,column in [('VIDA_TOMADOR','ESTADO_TOMADOR'),('VIDA_AGENTE','ESTADO_AGENTE')]:
            ids=spark.table(view+'_AMBIGUOS').select(F.col('POLIZA').alias('_POLIZA_AMBIGUA'))
            final=final.join(ids,final['AGREGATEDPOLICYID']==ids['_POLIZA_AMBIGUA'],'left').withColumn(column,F.when(F.col('_POLIZA_AMBIGUA').isNotNull(),F.lit('SELECCION_NO_ORDENADA')).otherwise(F.lit('SIN_AMBIGUEDAD_DETECTADA'))).drop('_POLIZA_AMBIGUA')
        final=final.withColumn('SALIDA_PROVISIONAL',F.lit(True))
        if final.count()!=conteos['VIDA_FINAL']:raise ValueError('Marcas duplicaron filas finales')
        importes=['PRIMA','COSTO_RECAUDO','IVA_COSTO_RECAUDO','COMISION','VALOR_ASEGURADO']
        totales=final.agg(*[F.sum(c).alias(c) for c in importes]).first().asDict()
        ops_final = final.select('NRO_OPERACION').distinct()
        if ops_final.join(esperados_df, on='NRO_OPERACION', how='left_anti').limit(1).count():
            raise ValueError('Operaciones fuera del pivote')
        n_ops_final = ops_final.count()
        n_sin_salida = esperados_df.join(ops_final,on='NRO_OPERACION',how='left_anti').count()
        salida=args['S3_SLV_PREP_PATH'].rstrip('/')+'/ejecucion_preparacion='+uuid.uuid4().hex+'/'
        final=final.withColumn('LOTE_CARGUE_ID',F.lit(int(lote)))
        final.write.mode('errorifexists').option('compression','snappy').parquet(salida)
        informe={'estado':'SUCCESS_PROVISIONAL','selecciones_persistidas':selection_info,'control_antes_despues':{'referencia':'Mismos datos y reglas con atributos tomador/agente NULL; NO E1 Oracle','filas_base_antes':baseline['filas_base'],'filas_movimientos_antes':baseline['filas_movimientos'],'filas_final_antes':baseline['filas_final'],'filas_final_despues':conteos['VIDA_FINAL'],'movimientos_por_operacion_iguales':True,'importes_por_operacion_iguales':True},'salida_provisional':True,'capa_entrada':capa,'certificado_qa':False,
            'periodo_negocio':runtime['periodo_negocio'],'lote_cargue_id':lote,
            'conteos_fuentes':metricas,'conteos_etapas':conteos,
            'operaciones_con_salida':n_ops_final,'operaciones_sin_salida':n_sin_salida,
            'operaciones_pivote':n_esperados,'producto_id':producto,'compania_value':compania,
            'estado_datos':'CON_DATOS' if conteos['VIDA_FINAL'] else 'SIN_DATOS',
            'advertencia_cobertura':'El pivote es compartido: operaciones sin salida pueden no pertenecer a Vida; no acredita completitud funcional',
            'totales':{k:None if v is None else str(v) for k,v in totales.items()},
            'ruta_silver_preparada':salida,'fecha_preparacion':datetime.now().isoformat(),
            'requiere_revision_analitica':runtime.get('estado_validacion')=='FAILED',
            'advertencia':'Comparacion de cifras con SQL original pendiente. No certifica completitud de extraccion.'}
        guardar(args['S3_SLV_PREP_PATH'].rstrip('/')+'/controles/'+salida.rstrip('/').split('/')[-1]+'.json',informe)
        runtime['preparacion_vida_prueba']=informe
        if capa=='SILVER':
            runtime.update(estado_preparacion_silver='SUCCESS_PROVISIONAL',ruta_silver_preparada=salida,
                total_registros_silver_preparada=conteos['VIDA_FINAL'],fecha_preparacion_silver=informe['fecha_preparacion'],
                modo_escritura_silver_preparada='errorifexists',silver_preparada_historica=False,
                silver_preparacion=informe)
        # BRONZE es solo diagnostico: no simula validacion ni disponibilizacion.
        guardar(ruta_runtime,runtime)
        print('PREPARACION VIDA OK; FILAS:',conteos['VIDA_FINAL'])
        print('SALIDA:',salida)
        print('OPERACIONES SIN SALIDA:',n_sin_salida)
        job.commit()
    finally:
        for df in cache:df.unpersist()

if __name__=='__main__':
    main()