

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from commons.ExtractHelper import ExtractHelper
from pyspark.sql import functions as F
from pyspark.sql.functions import col

import boto3
import datetime

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")
job = Job(glueContext)
ExtractHelper = ExtractHelper(glueContext, spark, "-", "-")
print(10 * "*" + "Librerías Cargadas Correctamente" + "*" * 10)
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from commons.ExtractHelper import ExtractHelper
from commons.TransformationHelper import TransformationHelper
from commons.AseguradoHelper import AseguradoHelper
from pyspark.sql import functions as F
from pyspark.sql.functions import split, to_date, date_format, col, regexp_replace

from commons.IncrementalCharge import IncrementalCharge
import traceback

import boto3
import json

DB_SCHEMA_BRONCE = "ACSELE_ALFA"
DB_SCHEMA_SILVER = "TRANSFORMATION"
BUCKET_NAME_BRONCE = "alfa-datalake-dev-bronce"
BUCKET_NAME_SILVER = "alfa-datalake-dev-silver"
BUCKET_NAME_ASSETS = "alfa-datalake-dev-assets"
DB_SOURCE = "ACSELE"

spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")


ExtractHelper = ExtractHelper(glueContext, spark, "-", "-")
TransformationHelper = TransformationHelper()
AseguradoHelper = AseguradoHelper(glueContext, spark)
IncrementalCharge = IncrementalCharge(glueContext, spark, DB_SCHEMA_BRONCE, BUCKET_NAME_BRONCE)


##OPEN PARAM in JSON##
s3 = boto3.client('s3')
obj = s3.get_object(Bucket = BUCKET_NAME_ASSETS, Key = 'support_phase_files/silver-procces.json')
data = json.loads(obj['Body'].read().decode('utf-8'))
###
for procces, info in data.items():
    if procces != '-': ## change here according to the process use. ejm: procces == ola0, remember change the condition
        path = f"s3://{BUCKET_NAME_ASSETS}/support_phase_files/{info['file_name']}"
        sheet = "silver_tables"
        df_tables = ExtractHelper.extract_excel_data(path, sheet)
        sheet = "tmp_silver"
        df_tables_tmp_silver = ExtractHelper.extract_excel_data(path, sheet)
        path = f"s3://{BUCKET_NAME_ASSETS}/support_phase_files/{info['hom_file_name']}"
        sheet = info['sheet']
        df_homologaciones_dos = ExtractHelper.extract_excel_data(path, sheet)
        path = f"s3://{BUCKET_NAME_ASSETS}/support_phase_files/{info['nom_file_name']}"
        sheet = "Capitalizacion_datos"
        df_capitalizacion = ExtractHelper.extract_excel_data(path, sheet)
        sheet = "Casteo_datos"
        df_casteodata = ExtractHelper.extract_excel_data(path, sheet)

        tabla_to_proccess = df_homologaciones_dos.select("tabla").distinct().rdd.flatMap(lambda x:x).collect()

        df_homologaciones_principal = df_homologaciones_dos.selectExpr(
        "tabla as tabla",
        "campo_referencia as campo_referencia",
        "valor_referencia",
        "valor_homologacion",
        "campo_homologacion"
        )
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
                    Homologaciones Manager - Data Harmonization Engine
================================================================================
Description:
    Specialized class for data homologation and remediation operations.
    Handles field mapping, value transformation, and data quality enhancement
    through configurable homologation rules.
Author:         Maria Cardona
Created:        2025-08-10
Last Updated:   2025-08-10
Version:        1.0.0 - Initial Implementation
Environment:    Development
Dependencies:   pyspark, logging
Class:          HomologacionesManager
Main Methods:   homologacion_i(), homologacion_j()
================================================================================
"""
## ----- SPARK LIBRARIES ------------------ ###
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
## ----- PYTHON LIBRARIES ------------------ ###
import logging


class HomologacionesManager:
    """
    Manager class for data homologation and remediation operations.
    CORE RESPONSIBILITY: Data harmonization through field mapping and value transformation.
    This class handles two main operations:
    1. Homologation: Direct field value replacement based on mapping rules
    2. Remediation: Cross-field value mapping and data enrichment
    The class follows a two-step process:
    - Step 1 (homologacion_i): Orchestrates the transformation process
    - Step 2 (homologacion_j): Executes specific field transformations
    """
    def __init__(self):
        """
        Initialize HomologacionesManager.
        """
        #logger.info("HomologacionesManager initialized", "HomologacionesManager")
    def homologacion_j(self, df, valor, df_homologaciones, tipo):
        """
        Second Step to join in the fuction to help us to homologate or remediate data
        Args:
            table: table to procces
            df_homologaciones_dos: df to hel to homologation
            df: df to remediate
            valor: this is the value to homologate or remediate
            tipo: type of procces to deploy homologate or remediate
        """
        df = df.withColumn("ID", F.monotonically_increasing_id())
        campo_modificar = valor
        columns_to_drop = [
            "campo_referencia",
            "tabla",
            "campo_homologacion",
            "valor_referencia",
            "valor_homologacion"
        ]
        df_homologaciones = df_homologaciones.drop("tipo")
        df_inicial = df.select("ID",valor)
        df_union = df_inicial.join(df_homologaciones, F.upper(df_inicial[valor])==F.upper(df_homologaciones["valor_referencia"]), "left")
        nombre_campo = df_homologaciones.select('campo_homologacion','valor_homologacion').first()
        nombre_campo_str = nombre_campo["campo_homologacion"] 
        if tipo == 'homologacion':
            df_union = df_union.withColumn(
                campo_modificar,
                F.when(
                    F.col("valor_homologacion").isNotNull(),
                    F.col("valor_homologacion")
                ).otherwise(F.col(valor))
            )
            df_union = df_union.drop(*columns_to_drop)
            df = df.drop(valor)
            df = df.alias('d').join(df_union.alias('o'), "ID", "INNER")
        else:
            if nombre_campo_str not in df.columns:
                df = df.withColumn(nombre_campo_str, F.lit(None))
            else:
                logging.debug("pass")
            df_union = df_union.withColumn(
                f"{nombre_campo_str}-val", 
                F.when(
                    F.col("valor_homologacion").isNotNull(),
                    F.col("valor_homologacion")
                ).otherwise(F.lit(None))
            )
            df_union = df_union.drop(*columns_to_drop)
            df = df.drop(valor)
            df = df.alias('d').join(df_union.alias('o'), "ID", "INNER")
            df = df.withColumn(nombre_campo_str, F.coalesce(F.col(f"o.{nombre_campo_str}-val"), F.col(f"{nombre_campo_str}")))
            df = df.drop(f"{nombre_campo_str}-val")
        df = df.drop('ID')
        return df
    def homologacion_i(self, table, df_homologaciones_dos,df_homologaciones_principal, df, tipo):
        """
        First Step to join in the fuction to help us to homologate or remediate data
        Args:
            table: table to procces
            df_homologaciones_dos: df to hel to homologation
            df: df to remediate
            df_homologaciones_principal: this df contains the transformation rules
            tipo: type of procces to deploy homologate or remediate
        """
        table = table.lower()
        df_campos = df_homologaciones_principal.select("tabla","campo_referencia","campo_homologacion","valor_homologacion","valor_referencia").distinct()
        df_campos = df_campos.withColumn("campo_referencia",F.lit(F.lower(F.col("campo_referencia"))))\
                            .withColumn("campo_homologacion",F.lit(F.lower(F.col("campo_homologacion"))))\
                            .withColumn("tipo",F.when(F.col("campo_referencia") == F.col("campo_homologacion"), "H")
                                                .when(F.col("campo_referencia") != F.col("campo_homologacion"), "R")
                                                .otherwise("X"))
        if tipo == "homologacion":
            condicion = F.col("campo_referencia") == F.col("campo_homologacion")
        if tipo == "remediacion":
            condicion = F.col("campo_referencia") != F.col("campo_homologacion")
        df_campos = df_campos.filter(condicion)
        df_campos = df_campos.distinct()
        columns_list = [col.lower() for col in df.columns]
        tabla_to_proccess = df_homologaciones_dos.select(F.lower("campo_homologacion")).distinct().rdd.flatMap(lambda x:x).collect()
        for i in (tabla_to_proccess):
            valor = i
            if i in columns_list:
                if tipo == 'homologacion':
                    df_homologa = df_campos.filter("tabla = '"+table+"' and tipo = 'H' and campo_referencia = '"+i+"'").distinct()
                    if df_homologa.limit(1).count() > 0:
                        df = self.homologacion_j(df, valor, df_homologa, tipo)
                    else:
                        logging.debug("pass")
                elif tipo == 'remediacion':
                    df_remedia = df_campos.filter("tabla = '"+table+"' and tipo = 'R' and campo_referencia = '"+i+"'").distinct()
                    if df_remedia.limit(1).count() > 0:
                        df = self.homologacion_j(df, valor, df_remedia, tipo)
                    else:
                        logging.debug("pass")
                else:
                    logging.debug("pass")
            else:
                logging.debug("pass")
        return df

    """
    # Inicializar la clase
    homologaciones_manager = HomologacionesManager()
    # Usar exactamente como antes
    df_resultado = homologaciones_manager.homologacion_i(
        table="mi_tabla",
        df_homologaciones_dos=df_campos,
        df_homologaciones_principal=df_reglas,
        df=df_datos,
        tipo="homologacion"  # o "remediacion"
)
    """
import logging
Date = "2025-5-14"
for row in df_tables.collect():
    table = row["Table"].upper()
    tabla = row["Table"].upper()
    status = int(float(row["Status"]))
    if status == 1:
        if row["DB"] == "SIPREN":

            if row["Logic"] == '-':
                if row["SCHEMA"] == "FILES":
                    path_source = f"s3://{BUCKET_NAME_BRONCE}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-{row['SCHEMA'].upper()}-"
                else:
                    path_source = f"s3://{BUCKET_NAME_BRONCE}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-{row['DB'].upper()}_ALFA-"

                path = f"{path_source}{table}/"
                df = ExtractHelper.extract_parquet_data(path, row["Fields"], Date)
                print(f"Entre a {table}")

                if df != None:
                    df_homologaciones_principal = TransformationHelper.trim_data(df_homologaciones_principal)
                    df = TransformationHelper.trim_data(df)
                    for capitalizacion in df_capitalizacion.collect():
                        if capitalizacion["TABLA"].lower() == table.lower() and table.lower() == "rsv_comentario_reserva":
                            df = TransformationHelper.data_normalice(capitalizacion["CAMPO"],df)
                            df = df.withColumn( 
                                capitalizacion["CAMPO"],
                                F.concat(
                                    F.upper(F.substring(F.col(capitalizacion["CAMPO"]), 1, 1)),
                                    F.lower(F.substring(F.col(capitalizacion["CAMPO"]), 2, 1000))
                                )
                            )
                            df = df.withColumn(
                                "comentario_reserva",
                                regexp_replace(F.col("comentario_reserva"), r'[^a-zA-Z0-9\s]','')
                                )

                            df = df.withColumn(
                                "comentario_reserva",
                                regexp_replace(F.col("comentario_reserva"), r'\b[a-zA-Z]\b','')
                                )

                            df = df.withColumn(
                                "comentario_reserva",
                                F.trim(F.col("comentario_reserva"))
                                )
                    if tabla.upper() == 'SNR_CAUSA':
                        df = df.filter(F.col('CLASE')=="CAUSA_SINIESTRO")
                        df = df.drop("CLASE")                    
                    if table.lower() in tabla_to_proccess:
                        homologaciones_manager = HomologacionesManager()
                        # Usar exactamente como antes
                        df_resultado = homologaciones_manager.homologacion_i(table, df_homologaciones_principal,df_homologaciones_principal, df, 'homologacion')
                        df_resultado.show(truncate=False)

df1_prethird = glueContext.create_dynamic_frame.from_catalog(
    database="datalake-dev-bronce-catalog-db",
    table_name="acsele_acsele_alfa_prethirdparty"
).toDF().select(
    col("TIPODOCUMENTOINPUT").alias("Cod_Tipo_Identificacion"),
    col("DOCUMENTOIDENTIDADINPUT").alias("Numero_Documento"),
    col("EMAILINPUT").alias("Correo_Electronico"),
    col("indicativooficinavalue").alias("Indicativo"),
    col("TELEFONOOFICINAVALUE").alias("Numero_Telefono"),
    col("TELEFONOOFICINACOMPLETINPUT").alias("Numero_Telefono")
)

df_direcci = glueContext.create_dynamic_frame.from_catalog(
    database="datalake-dev-bronce-catalog-db",
    table_name="acsele_acsele_alfa_direccion"
).toDF().select(
    col("TIPODIRECCIONTERCEROINPUT").alias("Tipo_Direccion"),
    col("DIRECCIONINPUT").alias("Direccion"),
    col("BARRIOINPUT").alias("Barrio"),
    col("CIUDADINPUT").alias("Cod_Ciudad"),
    col("CIUDADVALUE").alias("Ciudad"),
    col("DEPARTAMENTOINPUT").alias("Cod_Departamento"),
    col("DEPARTAMENTOVALUE").alias("Departamento"),
    col("PAISINPUT").alias("Cod_Pais"),
    col("PAISVALUE").alias("Pais")
    
)



df2 = glueContext.create_dynamic_frame.from_catalog(
    database="datalake-dev-bronce-catalog-db",
    table_name="alfaprd_general_acu_personas_naturales"
).toDF().select(col("COD_TIPO_IDENTIFICACION").alias("Cod_Tipo_Identificacion"))

###
df3 = glueContext.create_dynamic_frame.from_catalog(
    database="datalake-dev-bronce-catalog-db",
    table_name="alfaprod_general_acu_personas_juridicas" 
).toDF().select(col("COD_TIPO_IDENTIFICACION").alias("Cod_Tipo_Identificacion"))


df4 = glueContext.create_dynamic_frame.from_catalog(
    database="datalake-dev-bronce-catalog-db",
    table_name="smb_cardiff_customer"
).toDF().select(col("ID_TYPE_NAME").alias("Cod_Tipo_Identificacion"))

df_final = df_direcci#.unionAll(df2).unionAll(df3).unionAll(df4)

df_final.show(2)
df2.show()
import nltk
nltk.download('stopwords')

from nltk.corpus import stopwords
stop_words = set(stopwords.words('spanish'))
print(stop_words)
from commons.StepGold import StepGold
from commons.IncrementalCharge import IncrementalCharge
from pyspark.sql.types import IntegerType
import sys
import io

IncrementalCharge = IncrementalCharge(glueContext, spark, "-", "-")
StepGold = StepGold(glueContext, spark)
path = f"s3://alfa-datalake-dev-assets/support_phase_files/procces_ola1.xlsx"
sheet = "gold_phase"
df_tables = ExtractHelper.extract_excel_data(path, sheet)
sheet = "gold_tables"
df_tmp_tables = ExtractHelper.extract_excel_data(path, sheet)

for row in df_tmp_tables.collect():
    table = row["Table"].upper()
    print(table)
    if row["Phase"].upper() == "SILVER":
        path_source =f"s3://alfa-datalake-dev-silver/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}_SILVER_"
    else:
        path_source =f"s3://alfa-datalake-dev-gold/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-CARACT_SINIESTROS-"
    IncrementalCharge.charge_tmp_tables(path_source, table, 'Gold')
    
print("### Tablas StepGold cargadas correctamente")
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from pyspark.sql import functions as F, Window


class StepGold:
    def __init__(self, glue_context, spark):
        self.spark = spark

    def add_origin(self, df_mov, df_origin):
        """
        This Funtions help us to find and asocciate origins to any moves.
        Ars:
            df_mov = df that contain moves of sipren
            df_origin= df that contains origins data

            result = return df with new, old or default origin.
        """
        join_origin = df_mov.alias("M").join(
            df_origin.alias("O"),
            (F.col("M.numero_siniestro") == F.col("O.nid_siniestro")) &
            (F.col("M.fecha_transaccion") < F.col("O.fec_modificacion")),
            "left"
        )

        w = Window.partitionBy("M.numero_siniestro", "M.fecha_transaccion") \
            .orderBy(F.col("O.fec_modificacion"))

        origin_per_range = join_origin.withColumn("rn", F.row_number().over(w)) \
                            .filter("rn = 1") \
                            .select("M.*", F.col("O.cod_origen_siniestro_ant").alias("origen_ant"))

        last = df_origin.withColumn("rn", F.row_number().over(
                Window.partitionBy("nid_siniestro").orderBy(F.col("fec_modificacion").desc())
            )).filter("rn = 1").select("nid_siniestro", F.col("cod_origen_siniestro_new").alias("origen_new"))

        final_df = df_mov.alias("M").join(
            origin_per_range.alias("R"),
            on = ["numero_siniestro", "fecha_transaccion"],
            how = "left"
        )

        final_df = final_df.join(last.alias("U"),
                     final_df["numero_siniestro"] == last["nid_siniestro"],
                     "left").drop("nid_siniestro")

        final_df = final_df.withColumn(
                "origen_final",
                F.when(F.col("R.origen_ant").isNotNull(), F.col("R.origen_ant")).otherwise(F.col("U.origen_new"))
        )

        final_df = final_df.withColumn(
                "origen_final",
                F.when(F.col("origen_final").isNull(), F.col("M.origen_default")).otherwise(F.col("origen_final"))
        )


        return final_df

   
    def create_component(self):
        """
        This Function to create gold_layer Table combining Pagos, Recuperaciones and Reservas.
        """

        decimal_cast = "decimal(19, 0)"
        SIN_COD_CAUSA_AFP = "SIN.cod_causa_siniestro_afp"
        CP_COD_CONCEPTO_PAGO_SINIESTRO = "CP.cod_concepto_pago_siniestro"
        TM_COD_MOVIMIENTO = "TM.cod_movimiento"
        SIN_COD_ORIGEN = "SIN.cod_origen"
        SIN_NID_SINIESTRO ="SIN.nid_siniestro"
        CS_COD_CAUSA_SINIESTRO = "CS.cod_causa_siniestro"
        CO_COD_ORIGEN = "CO.cod_origen"
        SIN_FEC_SINIESTRO = "SIN.fec_siniestro"
        P_FEC_INICIAL_VIGENCIA = "P.fec_inicial_vigencia"
        P_FEC_FINAL_VIGENCIA = "P.fec_final_vigencia"
        DR_VALOR_RESERVA = "DR.vlr_reserva"
        AFIL_CEDULA ="AF.AFIL_CEDULA"
        PS_VLR_PAGO ="PS.vlr_pago"
        SIN_FEC_SOLICITUD_AFP = "SIN.fec_solicitud_afp"
        TM_DESCRIPCION ="TM.descripcion"
        ISA_DIA_181 ="ISA_FIL.dia_181"
        CS_DESCRIPCION = "CS.descripcion"
        UNIDAD_DE_NEGOCIO = "Seguridad social"
        P_NID_POLIZA ="P.nid_poliza"
        CO_NOMBRE = "CO.nombre"
        PA_NID_IDENTIFICACION = "PA.NID_IDENTIFICACION"
        SIN_NID_NUM_IDEN = "SIN.NID_NUMERO_IDENTIFICACION"
        SIN_COD_TIPO_IDEN = "SIN.COD_TIPO_IDENTIFICACION"
        PA_COD_TIPO_IDEN = "PA.COD_TIPO_IDENTIFICACION_AFP"
        ISA_FIL_NID_SINIESTRO = "ISA_FIL.NID_SINIESTRO"
        SIN_NID_SOL = "SIN.NID_SOLICITUD_AFP"
        BP_NID_SOLICITUD_AFP = "BP.NID_SOLICITUD_AFP"
        BP_CIUDAD_SIN = "BP.CIUDAD_SINIESTRO"
        SIN_COD_DEP = "SIN.COD_DEPARTAMENTO"
        AF_FIL_IN = "AF.afil_fecing"
        AF_CLASEI = "AF.AFIL_CLASEI"
        AF_FEC_NAC = "AF.afil_fecnac"
        AF_SEXO = "AF.afil_sexo"
        AF_NUMCUEN = "AF.AFIL_NUMCUE"
        PA_CUENTA_IND = "PA.NID_CUENTA_INDIVIDUAL"
        DESC_CAUSA = "Otras causas"
        SIN_FEC_INICIO_SOLICITUD_AFP ="SIN.fec_inicio_solicitud_afp"

        # =====================================================
        # PAGOS - IMPLEMENTACIÓN EXACTA DEL ARCHIVO ORIGINAL
        # =====================================================
        
        # Cargar tablas para PAGOS
        DR = self.spark.table("global_temp.spr_detalle_reserva")
        PN = self.spark.table("global_temp.acu_personas_naturales").alias("PN")
        SR = self.spark.table("global_temp.spr_reserva").alias("SR")
        CP = self.spark.table("global_temp.spr_conceptos_pagos_siniestros")
        TM = self.spark.table("global_temp.cgr_movimientos_reserva")
        SIN = self.spark.table("global_temp.spr_siniestros").alias("SIN")
        CS = self.spark.table("global_temp.spr_causas_siniestros")
        CO = self.spark.table("global_temp.cgr_origenes").alias("CO")
        CO3_P = self.spark.table("global_temp.cgr_origenes").alias("CO3")  # Para PAGOS new
        CO4_P = self.spark.table("global_temp.cgr_origenes").alias("CO4")  # Para PAGOS ant
        P  = self.spark.table("global_temp.ppr_polizas")
        ISA= self.spark.table("global_temp.spr_incapacidades_sol_audit").alias("ISA")
        PS = self.spark.table("global_temp.spr_detalle_pago_siniestro")
        PG = self.spark.table("global_temp.spr_pagos").alias("PG")
        PA = self.spark.table("global_temp.ppr_afiliados").alias("PA")
        RP = self.spark.table("global_temp.spr_recuperaciones").alias("RP")
        TR = self.spark.table("global_temp.spr_tipos_recuperaciones")
        DT = self.spark.table("global_temp.spr_detalle_recuperaciones").alias("DT")
        H2 = self.spark.table("global_temp.Spr_siniestros_origenes_hist").alias("H2")
        df_origen = self.spark.table("global_temp.Spr_siniestros_origenes_hist")
        # CTEs para PAGOS
        historico_mas_cercano = (
            H2.alias("H2")
            .join(PG.alias("PG"), F.col("H2.NID_SINIESTRO") == F.col("PG.NID_SINIESTRO"))
            .withColumn(
                "periodo_diferencia",
                F.abs(
                    (F.year(F.col("H2.FEC_MODIFICACION")) * 100 + F.month(F.col("H2.FEC_MODIFICACION"))) - 
                    F.col("PG.PERIODO_POINT").cast("int")
                )
            )
            .withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("H2.NID_SINIESTRO", "PG.PERIODO_POINT")
                    .orderBy("periodo_diferencia")
                )
            )
            .select(
                F.col("H2.NID_SINIESTRO"),
                F.col("H2.COD_ORIGEN_SINIESTRO_ANT"),
                F.col("H2.COD_ORIGEN_SINIESTRO_NEW"),
                F.col("H2.NID_SINIESTRO_ORIGEN_HIST"),
                F.col("PG.PERIODO_POINT"),
                F.col("rn")
            )
            .alias("HMC")
        )

        max_solicitud_audit = (
            ISA.groupBy("NID_SINIESTRO")
            .agg(F.max("NID_SOLICITUD_AUDIT").alias("max_solicitud_audit"))
            .alias("MSA")
        )

        hist_fecha_maxima = (
            H2.alias("T1")
            .join(
                H2.groupBy("NID_SINIESTRO")
                .agg(F.max("FEC_MODIFICACION").alias("FECHA_MODIFICACION_MAX"))
                .alias("T2"),
                (F.col("T1.NID_SINIESTRO") == F.col("T2.NID_SINIESTRO")) &
                (F.col("T1.FEC_MODIFICACION") == F.col("T2.FECHA_MODIFICACION_MAX"))
            )
            .select(
                F.col("T1.NID_SINIESTRO_ORIGEN_HIST"),
                F.col("T1.NID_SINIESTRO"),
                F.col("T1.FEC_MODIFICACION"),
                F.col("T1.COD_ORIGEN_SINIESTRO_NEW"),
                F.col("T1.COD_ORIGEN_SINIESTRO_ANT")
            )
            .alias("HFM")
        )

        max_origen_hist = (
            H2.withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("NID_SINIESTRO")
                    .orderBy(F.col("NID_SINIESTRO_ORIGEN_HIST").desc())
                )
            )
            .filter(F.col("rn") == 1)
            .select(
                F.col("NID_SINIESTRO"),
                F.col("COD_ORIGEN_SINIESTRO_NEW"),
                F.col("COD_ORIGEN_SINIESTRO_ANT"),
                F.col("rn")
            )
            .alias("MOH")
        )

        isa_filtrada = (
            ISA.join(
                max_solicitud_audit,
                (F.col("ISA.NID_SINIESTRO") == F.col("MSA.NID_SINIESTRO")) &
                (F.col("ISA.NID_SOLICITUD_AUDIT") == F.col("MSA.max_solicitud_audit"))
            )
            .select(
                F.col("ISA.NID_SINIESTRO").alias("isa_nid_siniestro_pagos"),
                F.col("ISA.NID_SOLICITUD_AUDIT").alias("isa_nid_solicitud_audit_pagos"),
                F.col("ISA.DIA_181").alias("isa_dia_181_pagos")
            )
            .alias("ISA_FILTERED_PAGOS")
        )

        fecha_ocurrencia_calculada = (
            PS.alias("PS")
            .join(PG.alias("PG"), F.col("PG.nid_pago") == F.col("PS.nid_pago"))
            .join(SIN.alias("SIN"), F.col("SIN.nid_siniestro") == F.col("PS.nid_siniestro"))
            .join(CO.alias("CO"), F.col("SIN.cod_origen") == F.col("CO.cod_origen"))
            .join(max_solicitud_audit.alias("MSA"), 
                  F.col("SIN.nid_siniestro") == F.col("MSA.nid_siniestro"), "left")
            .join(isa_filtrada.alias("ISA_FIL_CTE"), 
                  (F.col("SIN.nid_siniestro") == F.col("ISA_FIL_CTE.isa_nid_siniestro_pagos")) &
                  (F.col("ISA_FIL_CTE.isa_nid_solicitud_audit_pagos") == F.col("MSA.max_solicitud_audit")), "left")
            .join(historico_mas_cercano.alias("HMC"), 
                  (F.col("HMC.nid_siniestro") == F.col("SIN.nid_siniestro")) &
                  (F.col("HMC.periodo_point") == F.col("PG.periodo_point")) &
                  (F.col("HMC.rn") == 1), "left")
            .join(H2.alias("SH"), 
                  (F.col("PS.nid_siniestro") == F.col("SH.nid_siniestro")) &
                  (F.col("SH.cod_origen_siniestro_ant") == F.col("HMC.cod_origen_siniestro_ant")) &
                  (F.col("HMC.periodo_point") == F.col("PG.periodo_point")) &
                  (F.col("HMC.rn") == 1), "left")
            .filter(F.col("PS.vlr_pago") != 0)
            .withColumn(
                "cod_origen_calculado",
                F.when(
                    F.col("PG.periodo_point").cast("int") >= 
                    ((F.year(F.col("SH.fec_modificacion")) * 100) + F.month(F.col("SH.fec_modificacion"))),
                    F.col("SH.cod_origen_siniestro_new")
                ).when(
                    F.col("PG.periodo_point").cast("int") < 
                    ((F.year(F.col("SH.fec_modificacion")) * 100) + F.month(F.col("SH.fec_modificacion"))),
                    F.col("SH.cod_origen_siniestro_ant")
                ).otherwise(
                    F.col("CO.cod_origen")
                )
            )
            .withColumn(
                "fecha_ocurrencia_new",
                F.when(
                    (F.col("cod_origen_calculado") == 747) & F.col("ISA_FIL_CTE.isa_dia_181_pagos").isNotNull(),
                    F.col("ISA_FIL_CTE.isa_dia_181_pagos")
                ).otherwise(
                    F.col("SIN.fec_siniestro")
                )
            )
            .select(
                F.col("PS.nid_pago"),
                F.col("PS.nid_siniestro"),
                F.col("PG.periodo_point"),
                F.col("SIN.fec_siniestro"),
                F.col("ISA_FIL_CTE.isa_dia_181_pagos").alias("dia_181"),
                F.col("cod_origen_calculado"),
                F.col("fecha_ocurrencia_new")
            )
            .alias("FOC")
        )

        # Query principal PAGOS
        df_pago = (
            PS.alias("PS")
            .join(PG.alias("PG"), F.col("PG.nid_pago") == F.col("PS.nid_pago"))
            #.join(fecha_ocurrencia_calculada.alias("FOC"), F.col("PS.nid_pago") == F.col("FOC.nid_pago"))
            .join(CP.alias("CP"), F.col("PS.cod_concepto_pago_siniestro") == F.col("CP.cod_concepto_pago_siniestro"))
            .join(TM.alias("TM"), F.col("PS.cod_movimiento") == F.col("TM.cod_movimiento"))
            .join(SIN.alias("SIN"), F.col("SIN.nid_siniestro") == F.col("PS.nid_siniestro"))
            .join(CS.alias("CS"), F.col("SIN.cod_causa_siniestro_afp") == F.col("CS.cod_causa_siniestro"), "left")
            .join(CO.alias("CO"), F.col("SIN.cod_origen") == F.col("CO.cod_origen"))
            .join(PN.alias("PN2"), 
                  (F.col("SIN.nid_numero_identificacion") == F.col("PN2.nid_numero_identificacion")) &
                  (F.col("SIN.cod_tipo_identificacion") == F.col("PN2.cod_tipo_identificacion")))
            .join(PA.alias("PA"), 
                  (F.col("SIN.nid_numero_identificacion") == F.col("PA.nid_identificacion")) &
                  (F.col("SIN.cod_tipo_identificacion") == F.col("PA.cod_tipo_identificacion_afp")), "left")
            .join(PN.alias("PN"), 
                  (F.col("SIN.nid_numero_identificacion") == F.col("PN.nid_numero_identificacion")) &
                  (F.col("SIN.cod_tipo_identificacion") == F.col("PN.cod_tipo_identificacion")), "left")
            .join(max_solicitud_audit.alias("MSA"), 
                  F.col("SIN.nid_siniestro") == F.col("MSA.nid_siniestro"), "left")
            .join(isa_filtrada.alias("ISA_FIL_MAIN"), 
                  (F.col("SIN.nid_siniestro") == F.col("ISA_FIL_MAIN.isa_nid_siniestro_pagos")) &
                  (F.col("ISA_FIL_MAIN.isa_nid_solicitud_audit_pagos") == F.col("MSA.max_solicitud_audit")), "left")

            
            
        ).filter(F.col(PS_VLR_PAGO) != 0)

 

        # =====================================================
        # SELECCIONES FINALES CON CAMPOS EN ORDEN ESPECÍFICO
        # =====================================================
        
        # PAGOS con campos en orden específico
        pagos_final = df_pago.select(
                F.col("PG.nid_pago").alias("nid_pago"),#no esta
                F.col("PG.periodo_point").alias("periodo_point"),#no esta
                F.col("CP.cod_concepto_pago_siniestro").cast("string").alias("id_categoria_fk"),
                F.col("CP.descripcion").alias("categoria"),
                F.lit("Pago").alias("tipo"),
                F.col("PS.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
                F.col("TM.descripcion").alias("tipo_transaccion"),               
                F.col("PG.nid_siniestro").cast("string").alias("numero_siniestro"),                
                F.abs(F.col("PS.vlr_pago")).alias("valor_transaccion"),
                F.col("PS.vlr_pago").alias("valor_transaccion_ajustado"),   
                F.col("PG.fec_generado").alias("fecha_transaccion"),
                #F.col("FOC.fecha_ocurrencia_new").alias("fecha_ocurrencia"),
                F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
                F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),
                # Cod_origen (ya calculado en CTE)
                F.col("CO.COD_ORIGEN").alias("origen_default"),

                F.coalesce(F.col("SIN.fec_inicio_solicitud_afp"), F.col("SIN.fec_solicitud_afp")).alias("fecha_aviso"),                
                #F.coalesce(F.col("P.nid_poliza"), F.lit(999)).alias("poliza"),
                # CAMPOS HARDCODED (manteniendo los existentes)
                F.lit("2").cast("string").alias("id_fuente_fk"),
                F.lit("50").cast("string").alias("compania"),
                F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
                F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
                F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
                #F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col("P.nid_poliza").cast("string"), F.lit("999")), 7, "0"), F.lit("-00")).alias("poliza_lider"),                
                F.coalesce(F.col("SIN.cod_ciudad"), F.lit("01")).alias("ciudad_lugar_ocurrencia"),
                F.coalesce(F.col("SIN.cod_departamento").cast("string"), F.lit("01001")).alias("depto_lugar_ocurrencia"),                
                # Fecha_Inicio_Vigencia con validación mejorada (equivalente al REGEXP_LIKE del SQL)
                F.coalesce(
                    F.date_format(F.col("PA.fec_afiliacion_afp"), "yyyy-MM-dd"),
                    F.when(
                        F.col("PA.priper_cotrep").rlike("^[0-9]{5,6}$"),
                        F.date_format(
                            F.to_date(
                                F.concat(
                                    F.substring(F.trim(F.col("PA.priper_cotrep")), 1, 4),
                                    F.lpad(F.substring(F.trim(F.col("PA.priper_cotrep")), 5, 2), 2, "0"),
                                    F.lit("01")
                                ),
                                "yyyyMMdd"
                            ),
                            "yyyy-MM-dd"
                        )
                    ).otherwise(F.lit(None))
                ).alias("fecha_inicio_vigencia"),
                # Campos de persona
                F.col("SIN.nid_numero_identificacion").alias("cedula"),
                F.col("PN.fec_nacimiento").alias("fecha_nacimiento"),
                F.col("PN.genero").alias("genero"),
                F.col("ISA_FIL_MAIN.isa_dia_181_pagos"),
                F.col("SIN.fec_siniestro")
  
        ).distinct()
        pagos_final = pagos_final.drop("periodo_point")
        df_payments = self.add_origin(pagos_final, df_origen)
        
        
 
        
        # =====================================================
        # UNION FINAL
        # =====================================================
        df_final = df_payments
        df_final = df_final.drop("origen_default","origen_new","origen_ant")
        df_final = df_final.withColumnRenamed("origen_final", "cod_origen")


        columns = [
            "nid_pago",
            "id_categoria_fk",
            "categoria",
            "tipo",
            "id_tipo_transaccion_fk",
            "tipo_transaccion",
            "numero_siniestro",
            "valor_transaccion",
            "valor_transaccion_ajustado",
            "fecha_transaccion",
            #"fecha_ocurrencia",
            "cod_causa",
            "causa",
            "cod_origen",
            "fecha_aviso",
            #"poliza",
            "id_fuente_fk",
            "compania",
            "id_producto_fk",
            "id_ramo_fk",
            "unidad_de_negocio",
            #"poliza_lider",
            "ciudad_lugar_ocurrencia",
            "depto_lugar_ocurrencia",
            "fecha_inicio_vigencia",
            "cedula",
            "fecha_nacimiento",
            "genero",
            #"fec_siniestro",
            #"dia_181"
        ]

        new_columns = []
        cont = {}

        for col in df_final.columns:
            if col not in cont:
                cont[col] = 0
                new_columns.append(col)
            else:
                cont[col] += 1
                new_columns.append(f"{col}_{cont[col]}")

        df_final = df_final.toDF(*new_columns)

        df_final = df_final.select(columns)
        print(fecha_ocurrencia_calculada.printSchema())
        
        fecha_ocurrencia_calculada_v2 = (
            PS.alias("PS")
            .join(PG.alias("PG"), F.col("PG.nid_pago") == F.col("PS.nid_pago"))
            .join(SIN.alias("SIN"), F.col("SIN.nid_siniestro") == F.col("PS.nid_siniestro"))
            .join(CO.alias("CO"), F.col("SIN.cod_origen") == F.col("CO.cod_origen"))
            .join(max_solicitud_audit.alias("MSA"), 
                  F.col("SIN.nid_siniestro") == F.col("MSA.nid_siniestro"), "left")
            .join(isa_filtrada.alias("ISA_FIL_CTE"), 
                  (F.col("SIN.nid_siniestro") == F.col("ISA_FIL_CTE.isa_nid_siniestro_pagos")) &
                  (F.col("ISA_FIL_CTE.isa_nid_solicitud_audit_pagos") == F.col("MSA.max_solicitud_audit")), "left")
            .join(historico_mas_cercano.alias("HMC"), 
                  (F.col("HMC.nid_siniestro") == F.col("SIN.nid_siniestro")) &
                  (F.col("HMC.periodo_point") == F.col("PG.periodo_point")) &
                  (F.col("HMC.rn") == 1), "left")
            .join(H2.alias("SH"), 
                  (F.col("PS.nid_siniestro") == F.col("SH.nid_siniestro")) &
                  (F.col("SH.cod_origen_siniestro_ant") == F.col("HMC.cod_origen_siniestro_ant")) &
                  (F.col("HMC.periodo_point") == F.col("PG.periodo_point")) &
                  (F.col("HMC.rn") == 1), "left")
            .filter(F.col("PS.vlr_pago") != 0)
            .select(
                F.col("PS.nid_pago"),
                F.col("PS.nid_siniestro"),
                F.col("PG.periodo_point"),
                F.col("SIN.fec_siniestro"),
                F.col("ISA_FIL_CTE.isa_dia_181_pagos").alias("dia_181")
            )
            .alias("FOC")
        )
        return df_final, fecha_ocurrencia_calculada_v2, P
step_gold = StepGold(glueContext, spark)
_df, df, poliza_df = step_gold.create_component()
_df.filter((F.col("id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==1)).count()
_df.filter((F.col("numero_siniestro") == 20123445)&(F.col("id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==1)).select("numero_siniestro","nid_pago").show()
df = df.distinct()
df.filter((F.col("nid_siniestro") == 20123445)&(F.col("nid_pago")==403447)).show()
df_total = _df.alias("A").join(
    df.alias("B"),
    (F.col("A.numero_siniestro") == F.col("B.nid_siniestro")) & (F.col("A.nid_pago") == F.col("B.nid_pago")),
    "inner"
)
df_total.select("B.dia_181")
#df_total.filter((F.col("A.numero_siniestro") == 20123445)&(F.col("A.nid_pago")==403447)).show()
#df_total.printSchema()
df_total.filter((F.col("id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==1)).count()
df_total = df_total.withColumn(
    "fecha_ocurrencia",
    F.when(
        F.col("A.cod_origen").isin(742, 746, 741, 745),
        F.col("B.fec_siniestro")
    ).otherwise(F.col("B.dia_181"))
)
df_total.filter((F.col("id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==1)).count()
df_total = df_total.join(
    poliza_df.alias("C"),
    (F.col("fecha_ocurrencia") >= F.col("C.fec_inicial_vigencia")) &
    (F.col("fecha_ocurrencia") <= F.col("C.fec_final_vigencia")), "left"
)
df_total = df_total.select("A.*","fecha_ocurrencia","C.nid_poliza")
df_total = df_total.withColumn(
    "poliza",
    F.coalesce(F.col("C.nid_poliza"), F.lit(999)),
).withColumn(
    "poliza_lider",
    F.concat(
        F.lit("PRV-"),
        F.lpad(F.coalesce(F.col("C.nid_poliza").cast("string"), F.lit("999")), 7, "0"),
        F.lit("-00")
    )
)
df_total = df_total.drop("nid_poliza","nid_pago")
df_total.filter((F.col("id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==1)).count()
df_total.filter((F.col("numero_siniestro")==20101857)&(F.col("id_tipo_transaccion_fk") =='5')).select("cod_origen").orderBy("cod_origen").show()
from pyspark.sql.functions import col, year, month, sum as _sum, round, format_number
 
# Filtrar los datos como en la cláusula WHERE
df_filtrado = df_total.filter(
    (col("id_fuente_fk") == '2') &
    (year(col("fecha_transaccion")) == 2022) &
    (col("id_tipo_transaccion_fk") == '5')
)
 
# Agrupar por mes y sumar el valor de transacción
df_resultado = df_filtrado.groupBy(
    month(col("fecha_transaccion")).alias("mes")
).agg(
    format_number(round(_sum("valor_transaccion"), 2), 2).alias("suma_valor_formateado")
)
 
df_resultado.orderBy("mes").show(truncate=False)
ene	 93.960.811.952 	4065
feb	 92.358.158.029 	3866
mar	 82.958.140.631 	3812
abr	 124.133.903.210 	3932
may	 91.288.287.283 	3731
jun	 82.200.956.198 	3546
jul	 88.633.509.903 	3771
ago	 84.625.123.330 	4117
sep	 88.957.624.883 	4504
oct	 89.753.225.328 	4348
nov	 83.702.810.947 	3958
dic	 97.956.711.919 	4343

def add_ocurrence_fields(self, df, df_ocurrence, df_poliza):
    """
    Function Do Something
    """
    df_ocurrence = df_ocurrence.distinct()
    df_poliza = df_poliza.distinct()
    
    df_total = df_ocurrence.alias("A").join(
        df.alias("B"),
        (F.col("A.numero_siniestro") == F.col("B.nid_siniestro")) & (F.col("A.nid_pago") == F.col("B.nid_pago")),
        "inner"
    )
    
    df_total = df_total.withColumn(
        "fecha_ocurrencia",
        F.when(
            F.col("A.cod_origen").isin(742, 746, 741, 745),
            F.col("B.fec_siniestro")
        ).otherwise(F.col("B.dia_181"))
    )
    
    df_total = df_poliza.join(
        df_poliza.alias("C"),
        (F.col("fecha_ocurrencia") >= F.col("C.fec_inicial_vigencia")) &
        (F.col("fecha_ocurrencia") <= F.col("C.fec_final_vigencia")), "left"
    )
    
    df_total = df_total.select("A.*","fecha_ocurrencia","C.nid_poliza")
    
    df_total = df_total.withColumn(
        "poliza",
        F.coalesce(F.col("C.nid_poliza"), F.lit(999)),
    ).withColumn(
        "poliza_lider",
        F.concat(
            F.lit("PRV-"),
            F.lpad(F.coalesce(F.col("C.nid_poliza").cast("string"), F.lit("999")), 7, "0"),
            F.lit("-00")
        )
    )
    
    df_total = df_total.drop("nid_poliza","nid_pago")
    
    return df
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from pyspark.sql import functions as F, Window


class StepGold:
    def __init__(self, glue_context, spark):
        self.spark = spark
        
    def add_ocurrence_fields(self, df, df_ocurrence, df_poliza):
        """
        Function Do Something
        """
        df_ocurrence = df_ocurrence.distinct()
        df_poliza = df_poliza.distinct()

        df_total = df.alias("A").join(
            df_ocurrence.alias("B"),
            (F.col("A.numero_siniestro") == F.col("B.nid_siniestro")) & (F.col("A.nid_pago") == F.col("B.nid_pago")),
            "inner"
        )

        df_total = df_total.withColumn(
            "fecha_ocurrencia",
            F.when(
                F.col("A.cod_origen").isin(742, 746, 741, 745),
                F.col("B.fec_siniestro")
            ).otherwise(
                F.when(
                    F.col("B.dia_181").isNull(),
                    F.col("B.fec_siniestro")
                ).otherwise(F.col("B.dia_181"))
            )
        )


        df_total = df_total.join(
            df_poliza.alias("C"),
            (F.col("fecha_ocurrencia") >= F.col("C.fec_inicial_vigencia")) &
            (F.col("fecha_ocurrencia") <= F.col("C.fec_final_vigencia")), "left"
        )

        df_total = df_total.select("A.*","fecha_ocurrencia","C.nid_poliza")

        df_total = df_total.withColumn(
            "poliza",
            F.coalesce(F.col("C.nid_poliza"), F.lit(999)),
        ).withColumn(
            "poliza_lider",
            F.concat(
                F.lit("PRV-"),
                F.lpad(F.coalesce(F.col("C.nid_poliza").cast("string"), F.lit("999")), 7, "0"),
                F.lit("-00")
            )
        )

        df_total = df_total.drop("nid_poliza","nid_pago")

        return df_total

    def add_origin(self, df_mov, df_origin):
        """
        This Funtions help us to find and asocciate origins to any moves.
        Ars:
            df_mov = df that contain moves of sipren
            df_origin= df that contains origins data

            result = return df with new, old or default origin.
        """
        join_origin = df_mov.alias("M").join(
            df_origin.alias("O"),
            (F.col("M.numero_siniestro") == F.col("O.nid_siniestro")) &
            (F.col("M.fecha_transaccion") < F.col("O.fec_modificacion")),
            "left"
        )

        w = Window.partitionBy("M.numero_siniestro", "M.fecha_transaccion") \
            .orderBy(F.col("O.fec_modificacion"))

        origin_per_range = join_origin.withColumn("rn", F.row_number().over(w)) \
                            .filter("rn = 1") \
                            .select("M.*", F.col("O.cod_origen_siniestro_ant").alias("origen_ant"))

        last = df_origin.withColumn("rn", F.row_number().over(
                Window.partitionBy("nid_siniestro").orderBy(F.col("fec_modificacion").desc())
            )).filter("rn = 1").select("nid_siniestro", F.col("cod_origen_siniestro_new").alias("origen_new"))

        final_df = df_mov.alias("M").join(
            origin_per_range.alias("R"),
            on = ["numero_siniestro", "fecha_transaccion"],
            how = "left"
        )

        final_df = final_df.join(last.alias("U"),
                     final_df["numero_siniestro"] == last["nid_siniestro"],
                     "left").drop("nid_siniestro")

        final_df = final_df.withColumn(
                "origen_final",
                F.when(F.col("R.origen_ant").isNotNull(), F.col("R.origen_ant")).otherwise(F.col("U.origen_new"))
        )

        final_df = final_df.withColumn(
                "origen_final",
                F.when(F.col("origen_final").isNull(), F.col("M.origen_default")).otherwise(F.col("origen_final"))
        )


        return final_df

   
    def create_component(self):
        """
        This Function to create gold_layer Table combining Pagos, Recuperaciones and Reservas.
        """

        decimal_cast = "decimal(19, 0)"
        SIN_COD_CAUSA_AFP = "SIN.cod_causa_siniestro_afp"
        CP_COD_CONCEPTO_PAGO_SINIESTRO = "CP.cod_concepto_pago_siniestro"
        TM_COD_MOVIMIENTO = "TM.cod_movimiento"
        SIN_COD_ORIGEN = "SIN.cod_origen"
        SIN_NID_SINIESTRO ="SIN.nid_siniestro"
        CS_COD_CAUSA_SINIESTRO = "CS.cod_causa_siniestro"
        CO_COD_ORIGEN = "CO.cod_origen"
        SIN_FEC_SINIESTRO = "SIN.fec_siniestro"
        P_FEC_INICIAL_VIGENCIA = "P.fec_inicial_vigencia"
        P_FEC_FINAL_VIGENCIA = "P.fec_final_vigencia"
        DR_VALOR_RESERVA = "DR.vlr_reserva"
        AFIL_CEDULA ="AF.AFIL_CEDULA"
        PS_VLR_PAGO ="PS.vlr_pago"
        SIN_FEC_SOLICITUD_AFP = "SIN.fec_solicitud_afp"
        TM_DESCRIPCION ="TM.descripcion"
        ISA_DIA_181 ="ISA_FIL.dia_181"
        CS_DESCRIPCION = "CS.descripcion"
        UNIDAD_DE_NEGOCIO = "Seguridad social"
        P_NID_POLIZA ="P.nid_poliza"
        CO_NOMBRE = "CO.nombre"
        PA_NID_IDENTIFICACION = "PA.NID_IDENTIFICACION"
        SIN_NID_NUM_IDEN = "SIN.NID_NUMERO_IDENTIFICACION"
        SIN_COD_TIPO_IDEN = "SIN.COD_TIPO_IDENTIFICACION"
        PA_COD_TIPO_IDEN = "PA.COD_TIPO_IDENTIFICACION_AFP"
        ISA_FIL_NID_SINIESTRO = "ISA_FIL.NID_SINIESTRO"
        SIN_NID_SOL = "SIN.NID_SOLICITUD_AFP"
        BP_NID_SOLICITUD_AFP = "BP.NID_SOLICITUD_AFP"
        BP_CIUDAD_SIN = "BP.CIUDAD_SINIESTRO"
        SIN_COD_DEP = "SIN.COD_DEPARTAMENTO"
        AF_FIL_IN = "AF.afil_fecing"
        AF_CLASEI = "AF.AFIL_CLASEI"
        AF_FEC_NAC = "AF.afil_fecnac"
        AF_SEXO = "AF.afil_sexo"
        AF_NUMCUEN = "AF.AFIL_NUMCUE"
        PA_CUENTA_IND = "PA.NID_CUENTA_INDIVIDUAL"
        DESC_CAUSA = "Otras causas"
        SIN_FEC_INICIO_SOLICITUD_AFP ="SIN.fec_inicio_solicitud_afp"

        # =====================================================
        # PAGOS - IMPLEMENTACIÓN EXACTA DEL ARCHIVO ORIGINAL
        # =====================================================
        
        # Cargar tablas para PAGOS
        DR = self.spark.table("global_temp.spr_detalle_reserva")
        PN = self.spark.table("global_temp.acu_personas_naturales").alias("PN")
        SR = self.spark.table("global_temp.spr_reserva").alias("SR")
        CP = self.spark.table("global_temp.spr_conceptos_pagos_siniestros")
        TM = self.spark.table("global_temp.cgr_movimientos_reserva")
        SIN = self.spark.table("global_temp.spr_siniestros").alias("SIN")
        CS = self.spark.table("global_temp.spr_causas_siniestros")
        CO = self.spark.table("global_temp.cgr_origenes").alias("CO")
        CO3_P = self.spark.table("global_temp.cgr_origenes").alias("CO3")  # Para PAGOS new
        CO4_P = self.spark.table("global_temp.cgr_origenes").alias("CO4")  # Para PAGOS ant
        P  = self.spark.table("global_temp.ppr_polizas")
        ISA= self.spark.table("global_temp.spr_incapacidades_sol_audit").alias("ISA")
        PS = self.spark.table("global_temp.spr_detalle_pago_siniestro")
        PG = self.spark.table("global_temp.spr_pagos").alias("PG")
        PA = self.spark.table("global_temp.ppr_afiliados").alias("PA")
        RP = self.spark.table("global_temp.spr_recuperaciones").alias("RP")
        TR = self.spark.table("global_temp.spr_tipos_recuperaciones")
        DT = self.spark.table("global_temp.spr_detalle_recuperaciones").alias("DT")
        H2 = self.spark.table("global_temp.Spr_siniestros_origenes_hist").alias("H2")
        df_origen = self.spark.table("global_temp.Spr_siniestros_origenes_hist")
        # CTEs para PAGOS
        historico_mas_cercano = (
            H2.alias("H2")
            .join(PG.alias("PG"), F.col("H2.NID_SINIESTRO") == F.col("PG.NID_SINIESTRO"))
            .withColumn(
                "periodo_diferencia",
                F.abs(
                    (F.year(F.col("H2.FEC_MODIFICACION")) * 100 + F.month(F.col("H2.FEC_MODIFICACION"))) - 
                    F.col("PG.PERIODO_POINT").cast("int")
                )
            )
            .withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("H2.NID_SINIESTRO", "PG.PERIODO_POINT")
                    .orderBy("periodo_diferencia")
                )
            )
            .select(
                F.col("H2.NID_SINIESTRO"),
                F.col("H2.COD_ORIGEN_SINIESTRO_ANT"),
                F.col("H2.COD_ORIGEN_SINIESTRO_NEW"),
                F.col("H2.NID_SINIESTRO_ORIGEN_HIST"),
                F.col("PG.PERIODO_POINT"),
                F.col("rn")
            )
            .alias("HMC")
        )

        max_solicitud_audit = (
            ISA.groupBy("NID_SINIESTRO")
            .agg(F.max("NID_SOLICITUD_AUDIT").alias("max_solicitud_audit"))
            .alias("MSA")
        )

        hist_fecha_maxima = (
            H2.alias("T1")
            .join(
                H2.groupBy("NID_SINIESTRO")
                .agg(F.max("FEC_MODIFICACION").alias("FECHA_MODIFICACION_MAX"))
                .alias("T2"),
                (F.col("T1.NID_SINIESTRO") == F.col("T2.NID_SINIESTRO")) &
                (F.col("T1.FEC_MODIFICACION") == F.col("T2.FECHA_MODIFICACION_MAX"))
            )
            .select(
                F.col("T1.NID_SINIESTRO_ORIGEN_HIST"),
                F.col("T1.NID_SINIESTRO"),
                F.col("T1.FEC_MODIFICACION"),
                F.col("T1.COD_ORIGEN_SINIESTRO_NEW"),
                F.col("T1.COD_ORIGEN_SINIESTRO_ANT")
            )
            .alias("HFM")
        )

        max_origen_hist = (
            H2.withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("NID_SINIESTRO")
                    .orderBy(F.col("NID_SINIESTRO_ORIGEN_HIST").desc())
                )
            )
            .filter(F.col("rn") == 1)
            .select(
                F.col("NID_SINIESTRO"),
                F.col("COD_ORIGEN_SINIESTRO_NEW"),
                F.col("COD_ORIGEN_SINIESTRO_ANT"),
                F.col("rn")
            )
            .alias("MOH")
        )

        isa_filtrada = (
            ISA.join(
                max_solicitud_audit,
                (F.col("ISA.NID_SINIESTRO") == F.col("MSA.NID_SINIESTRO")) &
                (F.col("ISA.NID_SOLICITUD_AUDIT") == F.col("MSA.max_solicitud_audit"))
            )
            .select(
                F.col("ISA.NID_SINIESTRO").alias("isa_nid_siniestro_pagos"),
                F.col("ISA.NID_SOLICITUD_AUDIT").alias("isa_nid_solicitud_audit_pagos"),
                F.col("ISA.DIA_181").alias("isa_dia_181_pagos")
            )
            .alias("ISA_FILTERED_PAGOS")
        )

        fecha_ocurrencia_calculada = (
            PS.alias("PS")
            .join(PG.alias("PG"), F.col("PG.nid_pago") == F.col("PS.nid_pago"))
            .join(SIN.alias("SIN"), F.col("SIN.nid_siniestro") == F.col("PS.nid_siniestro"))
            .join(CO.alias("CO"), F.col("SIN.cod_origen") == F.col("CO.cod_origen"))
            .join(max_solicitud_audit.alias("MSA"), 
                  F.col("SIN.nid_siniestro") == F.col("MSA.nid_siniestro"), "left")
            .join(isa_filtrada.alias("ISA_FIL_CTE"), 
                  (F.col("SIN.nid_siniestro") == F.col("ISA_FIL_CTE.isa_nid_siniestro_pagos")) &
                  (F.col("ISA_FIL_CTE.isa_nid_solicitud_audit_pagos") == F.col("MSA.max_solicitud_audit")), "left")
            .join(historico_mas_cercano.alias("HMC"), 
                  (F.col("HMC.nid_siniestro") == F.col("SIN.nid_siniestro")) &
                  (F.col("HMC.periodo_point") == F.col("PG.periodo_point")) &
                  (F.col("HMC.rn") == 1), "left")
            .join(H2.alias("SH"), 
                  (F.col("PS.nid_siniestro") == F.col("SH.nid_siniestro")) &
                  (F.col("SH.cod_origen_siniestro_ant") == F.col("HMC.cod_origen_siniestro_ant")) &
                  (F.col("HMC.periodo_point") == F.col("PG.periodo_point")) &
                  (F.col("HMC.rn") == 1), "left")
            .filter(F.col("PS.vlr_pago") != 0)
            .select(
                F.col("PS.nid_pago"),
                F.col("PS.nid_siniestro"),
                F.col("PG.periodo_point"),
                F.col("SIN.fec_siniestro"),
                F.col("ISA_FIL_CTE.isa_dia_181_pagos").alias("dia_181")
            )
            .alias("FOC")
        )


        # Query principal PAGOS
        df_pago = (
            PS.alias("PS")
            .join(PG.alias("PG"), F.col("PG.nid_pago") == F.col("PS.nid_pago"))
            .join(CP.alias("CP"), F.col("PS.cod_concepto_pago_siniestro") == F.col("CP.cod_concepto_pago_siniestro"))
            .join(TM.alias("TM"), F.col("PS.cod_movimiento") == F.col("TM.cod_movimiento"))
            .join(SIN.alias("SIN"), F.col("SIN.nid_siniestro") == F.col("PS.nid_siniestro"))
            .join(CS.alias("CS"), F.col("SIN.cod_causa_siniestro_afp") == F.col("CS.cod_causa_siniestro"), "left")
            .join(CO.alias("CO"), F.col("SIN.cod_origen") == F.col("CO.cod_origen"))
            .join(PN.alias("PN2"), 
                  (F.col("SIN.nid_numero_identificacion") == F.col("PN2.nid_numero_identificacion")) &
                  (F.col("SIN.cod_tipo_identificacion") == F.col("PN2.cod_tipo_identificacion")))
            .join(PA.alias("PA"), 
                  (F.col("SIN.nid_numero_identificacion") == F.col("PA.nid_identificacion")) &
                  (F.col("SIN.cod_tipo_identificacion") == F.col("PA.cod_tipo_identificacion_afp")), "left")
            .join(PN.alias("PN"), 
                  (F.col("SIN.nid_numero_identificacion") == F.col("PN.nid_numero_identificacion")) &
                  (F.col("SIN.cod_tipo_identificacion") == F.col("PN.cod_tipo_identificacion")), "left")
            .join(max_solicitud_audit.alias("MSA"), 
                  F.col("SIN.nid_siniestro") == F.col("MSA.nid_siniestro"), "left")
            .join(isa_filtrada.alias("ISA_FIL_MAIN"), 
                  (F.col("SIN.nid_siniestro") == F.col("ISA_FIL_MAIN.isa_nid_siniestro_pagos")) &
                  (F.col("ISA_FIL_MAIN.isa_nid_solicitud_audit_pagos") == F.col("MSA.max_solicitud_audit")), "left")
            
            
        ).filter(F.col(PS_VLR_PAGO) != 0)
        

        

        # =====================================================
        # RECUPERACIONES - IMPLEMENTACIÓN EXACTA DEL ARCHIVO ORIGINAL
        # =====================================================
        
        # Cargar tablas para RECUPERACIONES (usando alias diferentes)
        DR_R = self.spark.table("global_temp.spr_detalle_reserva")
        PN_R = self.spark.table("global_temp.acu_personas_naturales").alias("PN")
        PN2_R = self.spark.table("global_temp.acu_personas_naturales").alias("PN2")
        SR_R = self.spark.table("global_temp.spr_reserva").alias("SR")
        CP_R = self.spark.table("global_temp.spr_conceptos_pagos_siniestros")
        TM_R = self.spark.table("global_temp.cgr_movimientos_reserva")
        SIN_R = self.spark.table("global_temp.spr_siniestros").alias("SIN")
        CS_R = self.spark.table("global_temp.spr_causas_siniestros")
        CO_R = self.spark.table("global_temp.cgr_origenes").alias("CO")
        CO3_R = self.spark.table("global_temp.cgr_origenes").alias("CO3")
        CO4_R = self.spark.table("global_temp.cgr_origenes").alias("CO4")
        P_R  = self.spark.table("global_temp.ppr_polizas")
        P2_R = self.spark.table("global_temp.ppr_polizas").alias("P2")
        ISA_R= self.spark.table("global_temp.spr_incapacidades_sol_audit").alias("ISA")
        PS_R = self.spark.table("global_temp.spr_detalle_pago_siniestro")
        PG_R = self.spark.table("global_temp.spr_pagos").alias("PG")
        PA_R = self.spark.table("global_temp.ppr_afiliados").alias("PA")
        RP_R = self.spark.table("global_temp.spr_recuperaciones").alias("RP")
        TR_R = self.spark.table("global_temp.spr_tipos_recuperaciones")
        DT_R = self.spark.table("global_temp.spr_detalle_recuperaciones").alias("DT")
        H2_R = self.spark.table("global_temp.Spr_siniestros_origenes_hist").alias("H2")

        # CTEs para RECUPERACIONES
        historico_mas_cercano_recup = (
            H2_R.alias("H2")
            .join(RP_R.alias("RP"), F.col("H2.NID_SINIESTRO") == F.col("RP.NID_SINIESTRO"))
            .withColumn(
                "periodo_diferencia",
                F.abs(
                    (F.year(F.col("H2.FEC_MODIFICACION")) * 100 + F.month(F.col("H2.FEC_MODIFICACION"))) - 
                    F.col("RP.PERIODO_POINT").cast("int")
                )
            )
            .withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("H2.NID_SINIESTRO", "RP.PERIODO_POINT")
                    .orderBy("periodo_diferencia")
                )
            )
            .select(
                F.col("H2.NID_SINIESTRO"),
                F.col("H2.COD_ORIGEN_SINIESTRO_ANT"),
                F.col("H2.COD_ORIGEN_SINIESTRO_NEW"),
                F.col("H2.NID_SINIESTRO_ORIGEN_HIST"),
                F.col("RP.PERIODO_POINT"),
                F.col("rn")
            )
            .alias("HMC_RECUP")
        )

        max_solicitud_audit_recup = (
            ISA_R.groupBy("NID_SINIESTRO")
            .agg(F.max("NID_SOLICITUD_AUDIT").alias("max_solicitud_audit"))
            .alias("MSA_RECUP")
        )

        hist_fecha_maxima_recup = (
            H2_R.alias("T1")
            .join(
                H2_R.groupBy("NID_SINIESTRO")
                .agg(F.max("FEC_MODIFICACION").alias("FECHA_MODIFICACION_MAX"))
                .alias("T2"),
                (F.col("T1.NID_SINIESTRO") == F.col("T2.NID_SINIESTRO")) &
                (F.col("T1.FEC_MODIFICACION") == F.col("T2.FECHA_MODIFICACION_MAX"))
            )
            .select(
                F.col("T1.NID_SINIESTRO_ORIGEN_HIST"),
                F.col("T1.NID_SINIESTRO"),
                F.col("T1.FEC_MODIFICACION"),
                F.col("T1.COD_ORIGEN_SINIESTRO_NEW"),
                F.col("T1.COD_ORIGEN_SINIESTRO_ANT")
            )
            .alias("HFM_RECUP")
        )

        max_origen_hist_recup = (
            H2_R.withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("NID_SINIESTRO")
                    .orderBy(F.col("NID_SINIESTRO_ORIGEN_HIST").desc())
                )
            )
            .filter(F.col("rn") == 1)
            .select(
                F.col("NID_SINIESTRO"),
                F.col("COD_ORIGEN_SINIESTRO_NEW"),
                F.col("COD_ORIGEN_SINIESTRO_ANT"),
                F.col("rn")
            )
            .alias("MOH_RECUP")
        )

        isa_filtrada_recup = (
            ISA_R.join(
                max_solicitud_audit_recup,
                (F.col("ISA.NID_SINIESTRO") == F.col("MSA_RECUP.NID_SINIESTRO")) &
                (F.col("ISA.NID_SOLICITUD_AUDIT") == F.col("MSA_RECUP.max_solicitud_audit"))
            )
            .select(
                F.col("ISA.NID_SINIESTRO").alias("isa_nid_siniestro_recup"),
                F.col("ISA.NID_SOLICITUD_AUDIT").alias("isa_nid_solicitud_audit_recup"),
                F.col("ISA.DIA_181").alias("isa_dia_181_recup"),
                F.col("ISA.FEC_SOLICITUD").alias("isa_fec_solicitud_recup")
            )
            .alias("ISA_FILTERED_RECUP")
        )
        fecha_ocurrencia_calculada_recup = (
            DT_R.alias("DT")
            .join(RP_R.alias("RP"), F.col("DT.nid_recuperacion") == F.col("RP.nid_recuperacion"), "left")
            .join(SIN_R.alias("SIN"), F.col("SIN.nid_siniestro") == F.col("RP.nid_siniestro"))
            .join(CO_R.alias("CO"), F.col("SIN.cod_origen") == F.col("CO.cod_origen"))
            .join(max_solicitud_audit_recup.alias("MSA_CTE"), 
                  F.col("SIN.nid_siniestro") == F.col("MSA_CTE.nid_siniestro"), "left")
            .join(isa_filtrada_recup.alias("ISA_FIL_CTE"), 
                  (F.col("SIN.nid_siniestro") == F.col("ISA_FIL_CTE.isa_nid_siniestro_recup")) &
                  (F.col("ISA_FIL_CTE.isa_nid_solicitud_audit_recup") == F.col("MSA_CTE.max_solicitud_audit")), "left")
            .join(historico_mas_cercano_recup.alias("HMC"), 
                  (F.col("HMC.nid_siniestro") == F.col("SIN.nid_siniestro")) &
                  (F.col("HMC.periodo_point") == F.col("RP.periodo_point")) &
                  (F.col("HMC.rn") == 1), "left")
            .join(H2_R.alias("SH"), 
                  (F.col("RP.nid_siniestro") == F.col("SH.nid_siniestro")) &
                  (F.col("SH.cod_origen_siniestro_ant") == F.col("HMC.cod_origen_siniestro_ant")) &
                  (F.col("HMC.periodo_point") == F.col("RP.periodo_point")) &
                  (F.col("HMC.rn") == 1), "left")
            .filter(F.col("RP.nid_recibo_caja_point") != 0)
            .select(
                F.col("DT.nid_recuperacion").alias("nid_pago"),
                F.col("RP.nid_siniestro"),
                F.col("RP.periodo_point"),
                F.col("SIN.fec_siniestro"),
                F.col("ISA_FIL_CTE.isa_dia_181_recup").alias("dia_181")
            )
            .alias("FOC_RECUP")
        )

        # Query principal RECUPERACIONES
        df_recup = (
             DT_R.alias("DT")
            .join(RP_R.alias("RP"), F.col("RP.nid_recuperacion") == F.col("DT.nid_recuperacion"), "left")
            .join(TR_R.alias("TR"), F.col("RP.cod_tipo_recuperacion") == F.col("TR.cod_tipo_recuperacion"))
            .join(TM_R.alias("TM"), F.col("RP.cod_movimiento") == F.col("TM.cod_movimiento"))
            .join(SIN_R.alias("SIN"), F.col("SIN.nid_siniestro") == F.col("RP.nid_siniestro"))
            .join(CS_R.alias("CS"), F.col("SIN.cod_causa_siniestro_afp") == F.col("CS.cod_causa_siniestro"), "left")
            .join(CO_R.alias("CO"), F.col("SIN.cod_origen") == F.col("CO.cod_origen"))
            .join(PN2_R, 
                  (F.col("SIN.nid_numero_identificacion") == F.col("PN2.nid_numero_identificacion")) &
                  (F.col("SIN.cod_tipo_identificacion") == F.col("PN2.cod_tipo_identificacion")))
            .join(PA_R.alias("PA"), 
                  (F.col("SIN.nid_numero_identificacion") == F.col("PA.nid_identificacion")) &
                  (F.col("SIN.cod_tipo_identificacion") == F.col("PA.cod_tipo_identificacion_afp")), "left")
            .join(PN_R, 
                  (F.col("SIN.nid_numero_identificacion") == F.col("PN.nid_numero_identificacion")) &
                  (F.col("SIN.cod_tipo_identificacion") == F.col("PN.cod_tipo_identificacion")), "left")
            .join(max_solicitud_audit_recup.alias("MSA_MAIN"), 
                  F.col("SIN.nid_siniestro") == F.col("MSA_MAIN.nid_siniestro"), "left")
            .join(isa_filtrada_recup.alias("ISA_FIL_MAIN"), 
                  (F.col("SIN.nid_siniestro") == F.col("ISA_FIL_MAIN.isa_nid_siniestro_recup")) &
                  (F.col("ISA_FIL_MAIN.isa_nid_solicitud_audit_recup") == F.col("MSA_MAIN.max_solicitud_audit")), "left")
      
            
        ).filter(F.col("RP.nid_recibo_caja_point") != 0)

        # =====================================================
        # RESERVAS - IMPLEMENTACIÓN EXACTA DEL ARCHIVO ORIGINAL
        # =====================================================
        
        # Cargar tablas para RESERVAS (usando alias diferentes)
        DR_S = self.spark.table("global_temp.spr_detalle_reserva")
        PN_S = self.spark.table("global_temp.acu_personas_naturales").alias("PN")
        PN2_S = self.spark.table("global_temp.acu_personas_naturales").alias("PN2")
        SR_S = self.spark.table("global_temp.spr_reserva").alias("SR")
        CP_S = self.spark.table("global_temp.spr_conceptos_pagos_siniestros")
        TM_S = self.spark.table("global_temp.cgr_movimientos_reserva")
        SIN_S = self.spark.table("global_temp.spr_siniestros").alias("SIN")
        CS_S = self.spark.table("global_temp.spr_causas_siniestros")
        CO_S = self.spark.table("global_temp.cgr_origenes").alias("CO")
        CO3_S = self.spark.table("global_temp.cgr_origenes").alias("CO3")
        CO4_S = self.spark.table("global_temp.cgr_origenes").alias("CO4")
        P_S  = self.spark.table("global_temp.ppr_polizas")
        ISA_S= self.spark.table("global_temp.spr_incapacidades_sol_audit").alias("ISA")
        PS_S = self.spark.table("global_temp.spr_detalle_pago_siniestro")
        PG_S = self.spark.table("global_temp.spr_pagos").alias("PG")
        PA_S = self.spark.table("global_temp.ppr_afiliados").alias("PA")
        RP_S = self.spark.table("global_temp.spr_recuperaciones").alias("RP")
        TR_S = self.spark.table("global_temp.spr_tipos_recuperaciones")
        DT_S = self.spark.table("global_temp.spr_detalle_recuperaciones").alias("DT")
        H2_S = self.spark.table("global_temp.Spr_siniestros_origenes_hist").alias("H2")

        # CTEs para RESERVAS
        historico_mas_cercano_reservas = (
            H2_S.alias("H2")
            .join(SR_S.alias("SR"), F.col("H2.NID_SINIESTRO") == F.col("SR.NID_SINIESTRO"))
            .withColumn(
                "periodo_diferencia",
                F.abs(
                    (F.year(F.col("H2.FEC_MODIFICACION")) * 100 + F.month(F.col("H2.FEC_MODIFICACION"))) - 
                    F.col("SR.PERIODO_POINT").cast("int")
                )
            )
            .withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("H2.NID_SINIESTRO", "SR.PERIODO_POINT")
                    .orderBy("periodo_diferencia")
                )
            )
            .select(
                F.col("H2.NID_SINIESTRO"),
                F.col("H2.COD_ORIGEN_SINIESTRO_ANT"),
                F.col("H2.COD_ORIGEN_SINIESTRO_NEW"),
                F.col("H2.NID_SINIESTRO_ORIGEN_HIST"),
                F.col("SR.PERIODO_POINT"),
                F.col("rn")
            )
            .alias("HMC_RESERVAS")
        )

        max_solicitud_audit_reservas = (
            ISA_S.groupBy("NID_SINIESTRO")
            .agg(F.max("NID_SOLICITUD_AUDIT").alias("max_solicitud_audit"))
            .alias("MSA_RESERVAS")
        )

        hist_fecha_maxima_reservas = (
            H2_S.alias("T1")
            .join(
                H2_S.groupBy("NID_SINIESTRO")
                .agg(F.max("FEC_MODIFICACION").alias("FECHA_MODIFICACION_MAX"))
                .alias("T2"),
                (F.col("T1.NID_SINIESTRO") == F.col("T2.NID_SINIESTRO")) &
                (F.col("T1.FEC_MODIFICACION") == F.col("T2.FECHA_MODIFICACION_MAX"))
            )
            .select(
                F.col("T1.NID_SINIESTRO_ORIGEN_HIST"),
                F.col("T1.NID_SINIESTRO"),
                F.col("T1.FEC_MODIFICACION"),
                F.col("T1.COD_ORIGEN_SINIESTRO_NEW"),
                F.col("T1.COD_ORIGEN_SINIESTRO_ANT")
            )
            .alias("HFM_RESERVAS")
        )

        max_origen_hist_reservas = (
            H2_S.withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("NID_SINIESTRO")
                    .orderBy(F.col("NID_SINIESTRO_ORIGEN_HIST").desc())
                )
            )
            .filter(F.col("rn") == 1)
            .select(
                F.col("NID_SINIESTRO"),
                F.col("COD_ORIGEN_SINIESTRO_NEW"),
                F.col("COD_ORIGEN_SINIESTRO_ANT"),
                F.col("rn")
            )
            .alias("MOH_RESERVAS")
        )

        isa_filtrada_reservas = (
            ISA_S.join(
                max_solicitud_audit_reservas,
                (F.col("ISA.NID_SINIESTRO") == F.col("MSA_RESERVAS.NID_SINIESTRO")) &
                (F.col("ISA.NID_SOLICITUD_AUDIT") == F.col("MSA_RESERVAS.max_solicitud_audit"))
            )
            .select(
                F.col("ISA.NID_SINIESTRO").alias("isa_nid_siniestro_reservas"),
                F.col("ISA.NID_SOLICITUD_AUDIT").alias("isa_nid_solicitud_audit_reservas"),
                F.col("ISA.DIA_181").alias("isa_dia_181_reservas")
            )
            .alias("ISA_FILTERED_RESERVAS")
        )


        # Query principal RESERVAS
        df_reserva = (
            DR_S.alias("DR")
            .join(SR_S.alias("SR"), F.col("DR.nid_reserva") == F.col("SR.nid_reserva"))
            .join(CP_S.alias("CP"), F.col("DR.cod_concepto_pago_siniestro") == F.col("CP.cod_concepto_pago_siniestro"))
            .join(TM_S.alias("TM"), F.col("DR.cod_movimiento") == F.col("TM.cod_movimiento"))
            .join(SIN_S.alias("SIN"), F.col("SIN.nid_siniestro") == F.col("DR.nid_siniestro"))
            .join(CS_S.alias("CS"), F.col("SIN.cod_causa_siniestro_afp") == F.col("CS.cod_causa_siniestro"))
            .join(CO_S.alias("CO"), F.col("SIN.cod_origen") == F.col("CO.cod_origen"))
            .join(PN2_S, 
                  (F.col("SIN.nid_numero_identificacion") == F.col("PN2.nid_numero_identificacion")) &
                  (F.col("SIN.cod_tipo_identificacion") == F.col("PN2.cod_tipo_identificacion")))
            .join(PA_S.alias("PA"), 
                  (F.col("SIN.nid_numero_identificacion") == F.col("PA.nid_identificacion")) &
                  (F.col("SIN.cod_tipo_identificacion") == F.col("PA.cod_tipo_identificacion_afp")), "left")
            .join(PN_S, 
                  (F.col("SIN.nid_numero_identificacion") == F.col("PN.nid_numero_identificacion")) &
                  (F.col("SIN.cod_tipo_identificacion") == F.col("PN.cod_tipo_identificacion")), "left")
            .join(max_solicitud_audit_reservas.alias("MSA_MAIN"), 
                  F.col("SIN.nid_siniestro") == F.col("MSA_MAIN.nid_siniestro"), "left")
            .join(isa_filtrada_reservas.alias("ISA_FIL_MAIN"), 
                  (F.col("SIN.nid_siniestro") == F.col("ISA_FIL_MAIN.isa_nid_siniestro_reservas")) &
                  (F.col("ISA_FIL_MAIN.isa_nid_solicitud_audit_reservas") == F.col("MSA_MAIN.max_solicitud_audit")), "left")
           
        ).filter(F.col("DR.VLR_RESERVA") != 0)

        # =====================================================
        # SELECCIONES FINALES CON CAMPOS EN ORDEN ESPECÍFICO
        # =====================================================
        
        fecha_ocurrencia_calculada_reservas = (
            DR_S.alias("DR")
            .join(SR_S.alias("SR"), F.col("DR.nid_reserva") == F.col("SR.nid_reserva"))
            .join(CP_S.alias("CP"), F.col("DR.cod_concepto_pago_siniestro") == F.col("CP.cod_concepto_pago_siniestro"))
            .join(SIN_S.alias("SIN"), F.col("SIN.nid_siniestro") == F.col("DR.nid_siniestro"))
            .join(CO_S.alias("CO"), F.col("SIN.cod_origen") == F.col("CO.cod_origen"))
            .join(max_solicitud_audit_reservas.alias("MSA_CTE"), 
                  F.col("SIN.nid_siniestro") == F.col("MSA_CTE.nid_siniestro"), "left")
            .join(isa_filtrada_reservas.alias("ISA_FIL_CTE"), 
                  (F.col("SIN.nid_siniestro") == F.col("ISA_FIL_CTE.isa_nid_siniestro_reservas")) &
                  (F.col("ISA_FIL_CTE.isa_nid_solicitud_audit_reservas") == F.col("MSA_CTE.max_solicitud_audit")), "left")
            .join(historico_mas_cercano_reservas.alias("HMC"), 
                  (F.col("HMC.nid_siniestro") == F.col("SIN.nid_siniestro")) &
                  (F.col("HMC.periodo_point") == F.col("SR.periodo_point")) &
                  (F.col("HMC.rn") == 1), "left")
            .join(H2_S.alias("SH"), 
                  (F.col("DR.nid_siniestro") == F.col("SH.nid_siniestro")) &
                  (F.col("SH.cod_origen_siniestro_ant") == F.col("HMC.cod_origen_siniestro_ant")) &
                  (F.col("HMC.periodo_point") == F.col("SR.periodo_point")) &
                  (F.col("HMC.rn") == 1), "left")
            .filter(F.col("DR.vlr_reserva") != 0)
            .select(
                F.col("DR.nid_reserva").alias("nid_pago"),
                F.col("DR.nid_siniestro"),
                F.col("SR.periodo_point"),
                F.col("SIN.fec_siniestro"),
                F.col("ISA_FIL_CTE.isa_dia_181_reservas").alias("dia_181")
            )
            .alias("FOC_RESERVAS")
        )
        
        
        # PAGOS con campos en orden específico
        pagos_final = df_pago.select(
                F.col("PG.nid_pago").alias("nid_pago"),#no esta
                F.col("PG.periodo_point").alias("periodo_point"),#no esta
                F.col("CP.cod_concepto_pago_siniestro").cast("string").alias("id_categoria_fk"),
                F.col("CP.descripcion").alias("categoria"),
                F.lit("Pago").alias("tipo"),
                F.col("PS.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
                F.col("TM.descripcion").alias("tipo_transaccion"),               
                F.col("PG.nid_siniestro").cast("string").alias("numero_siniestro"),                
                F.abs(F.col("PS.vlr_pago")).alias("valor_transaccion"),
                F.col("PS.vlr_pago").alias("valor_transaccion_ajustado"),   
                F.col("PG.fec_generado").alias("fecha_transaccion"),
                F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
                F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),
                # Cod_origen (ya calculado en CTE)
                F.col("CO.COD_ORIGEN").alias("origen_default"),
                F.coalesce(F.col("SIN.fec_inicio_solicitud_afp"), F.col("SIN.fec_solicitud_afp")).alias("fecha_aviso"),                
                # CAMPOS HARDCODED (manteniendo los existentes)
                F.lit("2").cast("string").alias("id_fuente_fk"),
                F.lit("50").cast("string").alias("compania"),
                F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
                F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
                F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),              
                F.coalesce(F.col("SIN.cod_ciudad"), F.lit("01")).alias("ciudad_lugar_ocurrencia"),
                F.coalesce(F.col("SIN.cod_departamento").cast("string"), F.lit("01001")).alias("depto_lugar_ocurrencia"),                
                # Fecha_Inicio_Vigencia con validación mejorada (equivalente al REGEXP_LIKE del SQL)
                F.coalesce(
                    F.date_format(F.col("PA.fec_afiliacion_afp"), "yyyy-MM-dd"),
                    F.when(
                        F.col("PA.priper_cotrep").rlike("^[0-9]{5,6}$"),
                        F.date_format(
                            F.to_date(
                                F.concat(
                                    F.substring(F.trim(F.col("PA.priper_cotrep")), 1, 4),
                                    F.lpad(F.substring(F.trim(F.col("PA.priper_cotrep")), 5, 2), 2, "0"),
                                    F.lit("01")
                                ),
                                "yyyyMMdd"
                            ),
                            "yyyy-MM-dd"
                        )
                    ).otherwise(F.lit(None))
                ).alias("fecha_inicio_vigencia"),
                # Campos de persona
                F.col("SIN.nid_numero_identificacion").alias("cedula"),
                F.col("PN.fec_nacimiento").alias("fecha_nacimiento"),
                F.col("PN.genero").alias("genero")
        ).distinct()
        pagos_final = pagos_final.drop("periodo_point")
        df_payments = self.add_origin(pagos_final, df_origen)
        
        
        # RECUPERACIONES con campos en orden específico
        recuperaciones_final = df_recup.select(
            F.col("DT.NID_RECUPERACION").alias("NID_RECUPERACION"),
            F.col("RP.periodo_point").alias("periodo_point"),#no esta
            F.col("TR.cod_tipo_recuperacion").cast("string").alias("id_categoria_fk"),
            F.col("TR.nombre").alias("categoria"),
            F.lit("Recuperaciones").alias("tipo"),
            F.col("RP.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
            F.col("TM.descripcion").alias("tipo_transaccion"),           
            F.col("RP.nid_siniestro").cast("string").alias("numero_siniestro"),
            F.abs(F.col("DT.vlr_concepto")).alias("valor_transaccion"),
            F.col("DT.vlr_concepto").alias("valor_transaccion_ajustado"),            
            F.col("RP.fec_creacion_registro").alias("fecha_transaccion"),  
            F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
            F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),
            # Cod_origen (ya calculado en CTE)
            F.col("CO.COD_ORIGEN").alias("origen_default"),
   
            F.coalesce(F.col("SIN.fec_inicio_solicitud_afp"), F.col("SIN.fec_solicitud_afp")).alias("fecha_aviso"),

            F.lit("2").cast("string").alias("id_fuente_fk"),
            F.lit("50").cast("string").alias("compania"),
            F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
            F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
            F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
            F.coalesce(F.col("SIN.cod_ciudad"), F.lit("01")).alias("ciudad_lugar_ocurrencia"),
            F.coalesce(F.col("SIN.cod_departamento").cast("string"), F.lit("01001")).alias("depto_lugar_ocurrencia"),
            F.coalesce(
                F.col("SIN.fec_inicio_solicitud_afp"),
                F.when(
                    F.col("PA.priper_cotrep").rlike("^[0-9]{5,6}$"),
                    F.to_date(
                        F.concat(
                            F.substring(F.trim(F.col("PA.priper_cotrep")), 1, 4),
                            F.lpad(F.substring(F.trim(F.col("PA.priper_cotrep")), 5, 2), 2, "0"),
                            F.lit("01")
                        ),
                        "yyyyMMdd"
                    )
                ).otherwise(F.lit(None))
            ).alias("fecha_inicio_vigencia"),
            # Campos de persona
            F.col("SIN.nid_numero_identificacion").alias("cedula"),
            F.col("PN.fec_nacimiento").alias("fecha_nacimiento"),
            F.col("PN.genero").alias("genero")
          
        ).distinct()
        recuperaciones_final = recuperaciones_final.withColumnRenamed("NID_RECUPERACION", "nid_pago")
        recuperaciones_final = recuperaciones_final.drop("periodo_point")
        df_recovery = self.add_origin(recuperaciones_final, df_origen)

        
        # RESERVAS con campos en orden específico
        reservas_final = df_reserva.select(
            F.col("DR.NID_RESERVA").alias("NID_RESERVA"),
            F.col("SR.periodo_point").alias("periodo_point"),#no esta
            F.col("CP.cod_concepto_pago_siniestro").cast("string").alias("id_categoria_fk"),
            F.col("CP.descripcion").alias("categoria"),
            F.lit("Reservas").alias("tipo"),
            F.col("DR.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
            F.col("TM.descripcion").alias("tipo_transaccion"),
            F.col("DR.nid_siniestro").cast("string").alias("numero_siniestro"),                
            # Campos de valores
            F.abs(F.col("DR.vlr_reserva")).alias("valor_transaccion"),
            F.col("DR.vlr_reserva").alias("valor_transaccion_ajustado"),
            # Campos de fechas
            F.col("SR.fec_generado").alias("fecha_transaccion"),
            F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
            F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),                
            # Cod_origen (ya calculado en CTE)
            F.col("CO.COD_ORIGEN").alias("origen_default"),
            F.coalesce(F.col("SIN.fec_inicio_solicitud_afp"), F.col("SIN.fec_solicitud_afp")).alias("fecha_aviso"),
            # CAMPOS HARDCODED (manteniendo los existentes)
            F.lit("2").cast("string").alias("id_fuente_fk"),
            F.lit("50").cast("string").alias("compania"),
            F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
            F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
            F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
            F.coalesce(F.col("SIN.cod_ciudad"), F.lit("01")).alias("ciudad_lugar_ocurrencia"),
            F.coalesce(F.col("SIN.cod_departamento").cast("string"), F.lit("01001")).alias("depto_lugar_ocurrencia"),
            # Fecha_Inicio_Vigencia con validación mejorada (MISMA QUE PAGOS)
            F.coalesce(
                F.date_format(F.col("PA.fec_afiliacion_afp"), "yyyy-MM-dd"),
                F.when(
                    F.col("PA.priper_cotrep").rlike("^[0-9]{5,6}$"),
                    F.date_format(
                        F.to_date(
                            F.concat(
                                F.substring(F.trim(F.col("PA.priper_cotrep")), 1, 4),
                                F.lpad(F.substring(F.trim(F.col("PA.priper_cotrep")), 5, 2), 2, "0"),
                                F.lit("01")
                            ),
                            "yyyyMMdd"
                        ),
                        "yyyy-MM-dd"
                    )
                ).otherwise(F.lit(None))
            ).alias("fecha_inicio_vigencia"),                
            # Campos de persona
            F.col("SIN.nid_numero_identificacion").alias("cedula"),
            F.col("PN.fec_nacimiento").alias("fecha_nacimiento"),
            F.col("PN.genero").alias("genero")
        ).distinct()
        reservas_final = reservas_final.withColumnRenamed("NID_RESERVA", "nid_pago")
        reservas_final = reservas_final.drop("periodo_point")
        df_reserver = self.add_origin(reservas_final, df_origen)

        
        # =====================================================
        # UNION FINAL
        # =====================================================
        df_final = df_payments.unionAll(df_recovery).unionAll(df_reserver)
        df_final = df_final.drop("origen_default","origen_new","origen_ant")
        df_final = df_final.withColumnRenamed("origen_final", "cod_origen")


        columns = [
            "id_categoria_fk",
            "categoria",
            "tipo",
            "id_tipo_transaccion_fk",
            "tipo_transaccion",
            "numero_siniestro",
            "valor_transaccion",
            "valor_transaccion_ajustado",
            "fecha_transaccion",
            "cod_causa",
            "causa",
            "cod_origen",
            "fecha_aviso",
            "id_fuente_fk",
            "compania",
            "id_producto_fk",
            "id_ramo_fk",
            "unidad_de_negocio",
            "ciudad_lugar_ocurrencia",
            "depto_lugar_ocurrencia",
            "fecha_inicio_vigencia",
            "cedula",
            "fecha_nacimiento",
            "genero",
            "nid_pago"
        ]

        new_columns = []
        cont = {}

        for col in df_final.columns:
            if col not in cont:
                cont[col] = 0
                new_columns.append(col)
            else:
                cont[col] += 1
                new_columns.append(f"{col}_{cont[col]}")

        df_final = df_final.toDF(*new_columns)

        df_final = df_final.select(columns)

        total_ocurrance = fecha_ocurrencia_calculada.unionAll(fecha_ocurrencia_calculada_reservas).unionAll(fecha_ocurrencia_calculada_recup)
        df_final = self.add_ocurrence_fields(df_final, total_ocurrance, P)
        

        return df_final
step_gold = StepGold(glueContext, spark)
df = step_gold.create_component()
df.count()
df.filter((F.col("id_tipo_transaccion_fk") =='1') &(F.year(F.col("fecha_transaccion"))==2020) &(F.month(F.col("fecha_transaccion"))==6)).select("numero_siniestro").count()
df.filter((F.col("numero_siniestro")==20101857) &(F.col("id_tipo_transaccion_fk") == '5')).select("cod_origen","poliza","fecha_ocurrencia","id_tipo_transaccion_fk","fecha_transaccion").orderBy("cod_origen").show()
from pyspark.sql.functions import col, year, month, round, sum as _sum, format_number
 
 
df_filtrado = df.filter(
    (col("id_fuente_fk") == '2') &
    (year(col("fecha_transaccion")) == 2022) &
    (col("id_tipo_transaccion_fk") == '7')
)
 
 
df_resultado = df_filtrado.groupBy(
    month(col("fecha_transaccion")).alias("mes")
).agg(
    format_number(round(_sum("valor_transaccion"), 2), 2).alias("suma_valor_formateado")
)
 
df_resultado.orderBy("mes").show(truncate=False)
from commons.StepGold import StepGold
from commons.StepGoldPlus import StepGoldPlus
from commons.StepGoldBodega import StepGoldBodega
from commons.SiprenDocumentoHelper import SiprenDocumentoHelper
from commons.UrincterrdeudoresHelper import UrincterrdeudoresHelper
StepGold = StepGold(glueContext, spark)
StepGoldPlus = StepGoldPlus(glueContext, spark)
StepGoldBodega = StepGoldBodega(glueContext, spark)
SiprenDocumentoHelper = SiprenDocumentoHelper(glueContext, spark)
Urincterrdeudores_helper = UrincterrdeudoresHelper(glueContext, spark)
df_tierra = StepGold.create_component()
df_plus = StepGoldPlus.create_component()
df_bodega = StepGoldBodega.create_component()
df_main = df_tierra.unionByName(df_plus).unionByName(df_bodega)
df_poliza_detallada_ola0 = Urincterrdeudores_helper.create_urincterrdeudores()   
df_plus.count()
from pyspark.sql import functions as F,Window
from pyspark.sql.types import StringType

class StepGoldPlusT:
    def __init__(self, glue_context, spark):
        self.spark = spark

    def create_component(self):
        


        P_NID_POLIZA ="P.nid_poliza"
        SDT_ID_SINIESTRO = "SDT.ID_SINIESTRO"
        SDB_FECHA_SINIESTRO = "SDB.FECHA_SINIESTRO"
        CP_COD_COBERTURA=  "CP.COD_COBERTURA"
        CS_ID_CAUSAL = "CS.ID_CAUSAL"
        DP_VLR_PAGO = "DP.VLR_PAGO"
        CO_COD_ORIGEN_SINIESTRO = "CO.COD_ORIGEN_SINIESTRO"
        CP_NOMBRE = "CP.NOMBRE"
        PCL_NUM_IDEN  = "PCL.NUM_IDENTIFICACION"
        AF2_NID_NUMERO = "AF2.NID_NUMERO_IDENTIFICACION"


        DR = self.spark.table("global_temp.rsv_dato_reserva")
        print(DR.count())
        print("rsv_dato_reserva")
        DP = self.spark.table("global_temp.pgs_dato_pago")
        print(DP.count())
        print("rsv_dato_pago")
        TM = self.spark.table("global_temp.rsv_tipo_movimiento")
        print(TM.count())
        print("rsv_tipo_movimiento")
        CP = self.spark.table("global_temp.rsv_cobertura")
        print(CP.count())
        print("rsv_cobertura")
        SDT = self.spark.table("global_temp.snr_dato_tramite")
        print(SDT.count())
        print("snr_dato_tramite")
        SDB = self.spark.table("global_temp.snr_dato_basico")
        print(SDB.count())
        print("snr_dato_basico")
        CS = self.spark.table("global_temp.snr_causa")
        print(CS.count())
        print("snr_causa")
        CO = self.spark.table("global_temp.snr_origen")
        print(CO.count())
        print("snr_origen")
        P = self.spark.table("global_temp.rcd_polizas")
        print(P.count())
        print("rcd_polizas")
        PCL = self.spark.table("global_temp.cu_persona_cliente")
        print(PCL.count())
        print("cu_persona_cliente")
        AF = self.spark.table("global_temp.vw_consulta_cliente")
        print(AF.count())
        print("vw_conslta_cli")
        AF2 = self.spark.table("global_temp.vw_localizaciones")
        print(AF2.count())
        print("vw_loca")
        
        return False
df_plus.select("id_tipo_transaccion_fk").distinct().show()
from pyspark.sql import functions as F,Window
from pyspark.sql.types import StringType

class StepGoldPlus:
    def __init__(self, glue_context, spark):
        self.spark = spark

    def create_component(self):
        


        P_NID_POLIZA ="P.nid_poliza"
        SDT_ID_SINIESTRO = "SDT.ID_SINIESTRO"
        SDB_FECHA_SINIESTRO = "SDB.FECHA_SINIESTRO"
        CP_COD_COBERTURA=  "CP.COD_COBERTURA"
        CS_ID_CAUSAL = "CS.ID_CAUSAL"
        DP_VLR_PAGO = "DP.VLR_PAGO"
        CO_COD_ORIGEN_SINIESTRO = "CO.COD_ORIGEN_SINIESTRO"
        CP_NOMBRE = "CP.NOMBRE"
        PCL_NUM_IDEN  = "PCL.NUM_IDENTIFICACION"
        AF2_NID_NUMERO = "AF2.NID_NUMERO_IDENTIFICACION"


        DR = self.spark.table("global_temp.rsv_dato_reserva")
        DP = self.spark.table("global_temp.pgs_dato_pago")
        TM = self.spark.table("global_temp.rsv_tipo_movimiento")
        CP = self.spark.table("global_temp.rsv_cobertura")
        SDT = self.spark.table("global_temp.snr_dato_tramite")
        SDB = self.spark.table("global_temp.snr_dato_basico")
        CS = self.spark.table("global_temp.snr_causa")
        CO = self.spark.table("global_temp.snr_origen")
        P = self.spark.table("global_temp.rcd_polizas")
        PCL = self.spark.table("global_temp.cu_persona_cliente")
        AF = self.spark.table("global_temp.vw_consulta_cliente")
        AF2 = self.spark.table("global_temp.vw_localizaciones")

        ##validacion lugar ocurrencia
        AF = AF.withColumn(
            "valor_max", F.greatest(F.col("COD_CIUDAD"), F.col("COD_DEPARTAMENTO"))
        )
        window_cedula = Window.partitionBy("NID_NUMERO_IDENTIFICACION").orderBy(F.col("valor_max").desc())
        AF = AF.withColumn("rank", F.row_number().over(window_cedula))
        AF = AF.filter(F.col("rank") == 1).drop("valor_max", "rank")
        
        df_reserva = DR.alias("DR") \
            .join(TM.alias("TM"), F.col("TM.COD_TIPO_MOVIMIENTO").cast("string") == F.col("DR.COD_TIPO_MOVIMIENTO").cast("string")) \
            .join(CP.alias("CP"), F.col("DR.COD_COBERTURA").cast("string") == F.col(CP_COD_COBERTURA).cast("string")) \
            .join(SDT.alias("SDT"), F.col("DR.ID_TRAMITE") == F.col("SDT.ID_TRAMITE")) \
            .join(SDB.alias("SDB"), F.col(SDT_ID_SINIESTRO) == F.col("SDB.ID_SINIESTRO")) \
            .join(CS.alias("CS"), F.col("SDB.COD_CAUSA_SINIESTRO").cast("string") == F.col(CS_ID_CAUSAL).cast("string")) \
            .join(CO.alias("CO"), F.col("SDB.COD_ORIGEN_SINIESTRO") == F.col(CO_COD_ORIGEN_SINIESTRO)) \
            .join(P.alias("P"), F.col(SDB_FECHA_SINIESTRO).between(F.col("P.FEC_INICIAL_VIGENCIA"), F.col("P.FEC_FINAL_VIGENCIA"))) \
            .join(PCL.alias("PCL"), F.col("PCL.NUM_PERSONA").cast("string") == F.col("SDB.NUM_PERSONA").cast("string")) \
            .join(AF.alias("AF"), (F.col(PCL_NUM_IDEN).cast("string") == F.col("AF.NID_NUMERO_IDENTIFICACION").cast("string")) & (F.col("PCL.cod_tipo_identificacion") == F.col("AF.cod_tipo_identificacion")),"left") \
            .join(AF2.alias("AF2"), (F.col(PCL_NUM_IDEN).cast("string") == F.col(AF2_NID_NUMERO).cast("string")) & (F.col("PCL.cod_tipo_identificacion") == F.col("AF2.cod_tipo_identificacion")),"left") \
            .select(
                F.col(CP_COD_COBERTURA).alias("id_categoria_fk"),
                F.col(CP_NOMBRE).alias("categoria"),
                F.lit("Reserva").alias("tipo"),
                F.col("DR.COD_TIPO_MOVIMIENTO").alias("id_tipo_transaccion_fk"),
                F.col("TM.NOMBRE").alias("tipo_transaccion"),
                F.col(SDT_ID_SINIESTRO).alias("numero_siniestro"),
                (F.col("DR.VLR_MOVIMIENTO")).alias("valor_transaccion"),
                F.col("DR.VLR_MOVIMIENTO").alias("valor_transaccion_ajustado"),
                F.col("DR.FECHA_MOVIMIENTO").alias("fecha_transaccion"),
                F.col(SDB_FECHA_SINIESTRO).alias("fecha_ocurrencia"),
                F.col(CS_ID_CAUSAL).alias("cod_causa"),
                F.col("CS.NOMBRE").alias("causa"),
                F.when(F.col(CP_COD_COBERTURA) == '3', F.col(CP_COD_COBERTURA)).otherwise(F.col(CO_COD_ORIGEN_SINIESTRO)).alias("cod_origen"),
                F.coalesce(F.col("SDT.FEC_ACT_SOLICITUD_AFP"), F.col("SDT.FEC_SOLICITUD_AFP")).alias("fecha_aviso"),
                F.coalesce(F.col("P.NID_POLIZA"), F.lit(999)).alias("poliza"),
                F.lit("3").cast("string").alias("id_fuente_fk"),
                F.lit("50").alias("compania"),
                F.lit(41).alias("id_producto_fk"),
                F.lit(31).alias("id_ramo_fk"),
                F.lit("Seguridad social").alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.coalesce(F.col("AF.COD_CIUDAD"), F.lit("01")).alias("ciudad_lugar_ocurrencia"),
                F.coalesce(F.col("AF.COD_DEPARTAMENTO").cast("string"), F.lit("01001")).alias("depto_lugar_ocurrencia"),
                F.col("AF2.FEC_INGRESO_AFP").alias("fecha_inicio_vigencia"),
                F.coalesce(F.col(AF2_NID_NUMERO),F.lit("No registra")).alias("cedula"),
                F.col("AF2.FEC_NACIMIENTO").alias("fecha_nacimiento"),
                F.coalesce(F.col("AF2.GENERO"),F.lit("No registra")).alias("genero")
                
            )
        
        print(df_reserva.select("id_tipo_transaccion_fk").distinct().show())
        
        df_pago = DP.alias("DP") \
            .join(TM.alias("TM"), F.col("TM.COD_TIPO_MOVIMIENTO").cast("string") == F.col("DP.COD_TIPO_MOVIMIENTO").cast("string")) \
            .join(CP.alias("CP"), F.col("DP.COD_COBERTURA").cast("string") == F.col(CP_COD_COBERTURA).cast("string")) \
            .join(SDT.alias("SDT"), F.col("DP.ID_TRAMITE") == F.col("SDT.ID_TRAMITE")) \
            .join(SDB.alias("SDB"), F.col(SDT_ID_SINIESTRO) == F.col("SDB.ID_SINIESTRO")) \
            .join(CS.alias("CS"), F.col("SDB.COD_CAUSA_SINIESTRO").cast("string") == F.col(CS_ID_CAUSAL).cast("string")) \
            .join(CO.alias("CO"), F.col("SDB.COD_ORIGEN_SINIESTRO") == F.col(CO_COD_ORIGEN_SINIESTRO)) \
            .join(P.alias("P"), F.col(SDB_FECHA_SINIESTRO).between(F.col("P.FEC_INICIAL_VIGENCIA"), F.col("P.FEC_FINAL_VIGENCIA"))) \
            .join(PCL.alias("PCL"), F.col("PCL.NUM_PERSONA").cast("string") == F.col("SDB.NUM_PERSONA").cast("string")) \
            .join(AF.alias("AF"), (F.col(PCL_NUM_IDEN).cast("string") == F.col("AF.NID_NUMERO_IDENTIFICACION").cast("string")) & (F.col("PCL.cod_tipo_identificacion") == F.col("AF.cod_tipo_identificacion")),"left") \
            .join(AF2.alias("AF2"), (F.col(PCL_NUM_IDEN).cast("string") == F.col(AF2_NID_NUMERO).cast("string")) & (F.col("PCL.cod_tipo_identificacion") == F.col("AF2.cod_tipo_identificacion")),"left") \
            .where(F.col(DP_VLR_PAGO).cast("double") != 0) \
            .select(
                F.col(CP_COD_COBERTURA).alias("id_categoria_fk"),
                F.col(CP_NOMBRE).alias("categoria"),
                F.lit("Pago").alias("tipo"),
                F.col("DP.COD_TIPO_MOVIMIENTO").alias("id_tipo_transaccion_fk"),
                F.col("TM.NOMBRE").alias("tipo_transaccion"),
                F.col(SDT_ID_SINIESTRO).alias("numero_siniestro"),
                (F.col(DP_VLR_PAGO).cast("double")).alias("valor_transaccion"),
                F.col(DP_VLR_PAGO).cast("double").alias("valor_transaccion_ajustado"),
                F.col("DP.FECHA_PAGO").alias("fecha_transaccion"),
                F.col(SDB_FECHA_SINIESTRO).alias("fecha_ocurrencia"),
                F.col(CS_ID_CAUSAL).alias("cod_causa"),
                F.col("CS.NOMBRE").alias("causa"),
		        F.when(F.col(CP_COD_COBERTURA) == '3', F.col(CP_COD_COBERTURA)).otherwise(F.col(CO_COD_ORIGEN_SINIESTRO)).alias("cod_origen"),
                F.coalesce(F.col("SDT.FEC_ACT_SOLICITUD_AFP"), F.col("SDT.FEC_SOLICITUD_AFP")).alias("fecha_aviso"),
                F.coalesce(F.col("P.NID_POLIZA"), F.lit(999)).alias("poliza"),
                F.lit("3").cast("string").alias("id_fuente_fk"),
                F.lit("50").alias("compania"),
                F.lit(41).alias("id_producto_fk"),
                F.lit(31).alias("id_ramo_fk"),
                F.lit("Seguridad saocial").alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.coalesce(F.col("AF.COD_CIUDAD"), F.lit("01")).alias("ciudad_lugar_ocurrencia"),
                F.coalesce(F.col("AF.COD_DEPARTAMENTO").cast("string"), F.lit("01001")).alias("depto_lugar_ocurrencia"),
                F.col("AF2.FEC_INGRESO_AFP").alias("fecha_inicio_vigencia"),
                F.coalesce(F.col(AF2_NID_NUMERO),F.lit("No registra")).alias("cedula"),
                F.col("AF2.FEC_NACIMIENTO").alias("fecha_nacimiento"),
                F.coalesce(F.col("AF2.GENERO"),F.lit("No registra")).alias("genero")
 
            )

        df_gold_plus = df_reserva.unionByName(df_pago)

        df_gold_plus = df_gold_plus.withColumn(
            "valor_transaccion",
            F.when(F.col("id_tipo_transaccion_fk").isin('2','4'), F.col("valor_transaccion") *-1)
            .otherwise(F.col("valor_transaccion"))
        )

        return df_gold_plus, DR

StepGoldPlus = StepGoldPlus(glueContext, spark)
df, _df = StepGoldPlus.create_component()
_df.select("COD_TIPO_MOVIMIENTO").distinct().show()
df.select("id_tipo_transaccion_fk").distinct().show()
from pyspark.sql import functions as F,Window
from pyspark.sql.types import StringType

class StepGoldPlus:
    def __init__(self, glue_context, spark):
        self.spark = spark

    def create_component(self):
        


        P_NID_POLIZA ="P.nid_poliza"
        SDT_ID_SINIESTRO = "SDT.ID_SINIESTRO"
        SDB_FECHA_SINIESTRO = "SDB.FECHA_SINIESTRO"
        CP_COD_COBERTURA=  "CP.COD_COBERTURA"
        CS_ID_CAUSAL = "CS.ID_CAUSAL"
        DP_VLR_PAGO = "DP.VLR_PAGO"
        CO_COD_ORIGEN_SINIESTRO = "CO.COD_ORIGEN_SINIESTRO"
        CP_NOMBRE = "CP.NOMBRE"
        PCL_NUM_IDEN  = "PCL.NUM_IDENTIFICACION"
        AF2_NID_NUMERO = "AF2.NID_NUMERO_IDENTIFICACION"


        DR = self.spark.table("global_temp.rsv_dato_reserva")
        DP = self.spark.table("global_temp.pgs_dato_pago")
        TM = self.spark.table("global_temp.rsv_tipo_movimiento")
        CP = self.spark.table("global_temp.rsv_cobertura")
        SDT = self.spark.table("global_temp.snr_dato_tramite")
        SDB = self.spark.table("global_temp.snr_dato_basico")
        CS = self.spark.table("global_temp.snr_causa")
        CO = self.spark.table("global_temp.snr_origen")
        P = self.spark.table("global_temp.rcd_polizas")
        PCL = self.spark.table("global_temp.cu_persona_cliente")
        AF = self.spark.table("global_temp.vw_consulta_cliente")
        AF2 = self.spark.table("global_temp.vw_localizaciones")
        CR = self.spark.table("global_temp.rsv_comentario_reserva")

        ##validacion lugar ocurrencia
        AF = AF.withColumn(
            "valor_max", F.greatest(F.col("COD_CIUDAD"), F.col("COD_DEPARTAMENTO"))
        )
        window_cedula = Window.partitionBy("NID_NUMERO_IDENTIFICACION").orderBy(F.col("valor_max").desc())
        AF = AF.withColumn("rank", F.row_number().over(window_cedula))
        AF = AF.filter(F.col("rank") == 1).drop("valor_max", "rank")
        
        df_reserva = DR.alias("DR") \
            .join(TM.alias("TM"), F.col("TM.COD_TIPO_MOVIMIENTO").cast("string") == F.col("DR.COD_TIPO_MOVIMIENTO").cast("string")) \
            .join(CP.alias("CP"), F.col("DR.COD_COBERTURA").cast("string") == F.col(CP_COD_COBERTURA).cast("string")) \
            .join(SDT.alias("SDT"), F.col("DR.ID_TRAMITE") == F.col("SDT.ID_TRAMITE")) \
            .join(SDB.alias("SDB"), F.col(SDT_ID_SINIESTRO) == F.col("SDB.ID_SINIESTRO")) \
            .join(CS.alias("CS"), F.col("SDB.COD_CAUSA_SINIESTRO").cast("string") == F.col(CS_ID_CAUSAL).cast("string")) \
            .join(CO.alias("CO"), F.col("SDB.COD_ORIGEN_SINIESTRO") == F.col(CO_COD_ORIGEN_SINIESTRO)) \
            .join(P.alias("P"), F.col(SDB_FECHA_SINIESTRO).between(F.col("P.FEC_INICIAL_VIGENCIA"), F.col("P.FEC_FINAL_VIGENCIA"))) \
            .join(PCL.alias("PCL"), F.col("PCL.NUM_PERSONA").cast("string") == F.col("SDB.NUM_PERSONA").cast("string")) \
            .join(AF.alias("AF"), (F.col(PCL_NUM_IDEN).cast("string") == F.col("AF.NID_NUMERO_IDENTIFICACION").cast("string")) & (F.col("PCL.cod_tipo_identificacion") == F.col("AF.cod_tipo_identificacion")),"left") \
            .join(AF2.alias("AF2"), (F.col(PCL_NUM_IDEN).cast("string") == F.col(AF2_NID_NUMERO).cast("string")) & (F.col("PCL.cod_tipo_identificacion") == F.col("AF2.cod_tipo_identificacion")),"left") \
            .select(
                F.col(CP_COD_COBERTURA).alias("id_categoria_fk"),
                F.col(CP_NOMBRE).alias("categoria"),
                F.lit("Reserva").alias("tipo"),
                F.col("DR.COD_TIPO_MOVIMIENTO").alias("id_tipo_transaccion_fk"),
                F.col("TM.NOMBRE").alias("tipo_transaccion"),
                F.col(SDT_ID_SINIESTRO).alias("numero_siniestro"),
                (F.col("DR.VLR_MOVIMIENTO")).alias("valor_transaccion"),
                F.col("DR.VLR_MOVIMIENTO").alias("valor_transaccion_ajustado"),
                F.col("DR.FECHA_MOVIMIENTO").alias("fecha_transaccion"),
                F.col(SDB_FECHA_SINIESTRO).alias("fecha_ocurrencia"),
                F.col(CS_ID_CAUSAL).alias("cod_causa"),
                F.col("CS.NOMBRE").alias("causa"),
                F.when(F.col(CP_COD_COBERTURA) == '3', F.col(CP_COD_COBERTURA)).otherwise(F.col(CO_COD_ORIGEN_SINIESTRO)).alias("cod_origen"),
                F.coalesce(F.col("SDT.FEC_ACT_SOLICITUD_AFP"), F.col("SDT.FEC_SOLICITUD_AFP")).alias("fecha_aviso"),
                F.coalesce(F.col("P.NID_POLIZA"), F.lit(999)).alias("poliza"),
                F.lit("3").cast("string").alias("id_fuente_fk"),
                F.lit("50").alias("compania"),
                F.lit(41).alias("id_producto_fk"),
                F.lit(31).alias("id_ramo_fk"),
                F.lit("Seguridad social").alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.coalesce(F.col("AF.COD_CIUDAD"), F.lit("01")).alias("ciudad_lugar_ocurrencia"),
                F.coalesce(F.col("AF.COD_DEPARTAMENTO").cast("string"), F.lit("01001")).alias("depto_lugar_ocurrencia"),
                F.col("AF2.FEC_INGRESO_AFP").alias("fecha_inicio_vigencia"),
                F.coalesce(F.col(AF2_NID_NUMERO),F.lit("No registra")).alias("cedula"),
                F.col("AF2.FEC_NACIMIENTO").alias("fecha_nacimiento"),
                F.coalesce(F.col("AF2.GENERO"),F.lit("No registra")).alias("genero"),
                F.col("DR.OBSERVACIONES")
            )
        
        

        
        df_pago = DP.alias("DP") \
            .join(TM.alias("TM"), F.col("TM.COD_TIPO_MOVIMIENTO").cast("string") == F.col("DP.COD_TIPO_MOVIMIENTO").cast("string")) \
            .join(CP.alias("CP"), F.col("DP.COD_COBERTURA").cast("string") == F.col(CP_COD_COBERTURA).cast("string")) \
            .join(SDT.alias("SDT"), F.col("DP.ID_TRAMITE") == F.col("SDT.ID_TRAMITE")) \
            .join(SDB.alias("SDB"), F.col(SDT_ID_SINIESTRO) == F.col("SDB.ID_SINIESTRO")) \
            .join(CS.alias("CS"), F.col("SDB.COD_CAUSA_SINIESTRO").cast("string") == F.col(CS_ID_CAUSAL).cast("string")) \
            .join(CO.alias("CO"), F.col("SDB.COD_ORIGEN_SINIESTRO") == F.col(CO_COD_ORIGEN_SINIESTRO)) \
            .join(P.alias("P"), F.col(SDB_FECHA_SINIESTRO).between(F.col("P.FEC_INICIAL_VIGENCIA"), F.col("P.FEC_FINAL_VIGENCIA"))) \
            .join(PCL.alias("PCL"), F.col("PCL.NUM_PERSONA").cast("string") == F.col("SDB.NUM_PERSONA").cast("string")) \
            .join(AF.alias("AF"), (F.col(PCL_NUM_IDEN).cast("string") == F.col("AF.NID_NUMERO_IDENTIFICACION").cast("string")) & (F.col("PCL.cod_tipo_identificacion") == F.col("AF.cod_tipo_identificacion")),"left") \
            .join(AF2.alias("AF2"), (F.col(PCL_NUM_IDEN).cast("string") == F.col(AF2_NID_NUMERO).cast("string")) & (F.col("PCL.cod_tipo_identificacion") == F.col("AF2.cod_tipo_identificacion")),"left") \
            .where(F.col(DP_VLR_PAGO).cast("double") != 0) \
            .select(
                F.col(CP_COD_COBERTURA).alias("id_categoria_fk"),
                F.col(CP_NOMBRE).alias("categoria"),
                F.lit("Pago").alias("tipo"),
                F.col("DP.COD_TIPO_MOVIMIENTO").alias("id_tipo_transaccion_fk"),
                F.col("TM.NOMBRE").alias("tipo_transaccion"),
                F.col(SDT_ID_SINIESTRO).alias("numero_siniestro"),
                (F.col(DP_VLR_PAGO).cast("double")).alias("valor_transaccion"),
                F.col(DP_VLR_PAGO).cast("double").alias("valor_transaccion_ajustado"),
                F.col("DP.FECHA_PAGO").alias("fecha_transaccion"),
                F.col(SDB_FECHA_SINIESTRO).alias("fecha_ocurrencia"),
                F.col(CS_ID_CAUSAL).alias("cod_causa"),
                F.col("CS.NOMBRE").alias("causa"),
		        F.when(F.col(CP_COD_COBERTURA) == '3', F.col(CP_COD_COBERTURA)).otherwise(F.col(CO_COD_ORIGEN_SINIESTRO)).alias("cod_origen"),
                F.coalesce(F.col("SDT.FEC_ACT_SOLICITUD_AFP"), F.col("SDT.FEC_SOLICITUD_AFP")).alias("fecha_aviso"),
                F.coalesce(F.col("P.NID_POLIZA"), F.lit(999)).alias("poliza"),
                F.lit("3").cast("string").alias("id_fuente_fk"),
                F.lit("50").alias("compania"),
                F.lit(41).alias("id_producto_fk"),
                F.lit(31).alias("id_ramo_fk"),
                F.lit("Seguridad saocial").alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.coalesce(F.col("AF.COD_CIUDAD"), F.lit("01")).alias("ciudad_lugar_ocurrencia"),
                F.coalesce(F.col("AF.COD_DEPARTAMENTO").cast("string"), F.lit("01001")).alias("depto_lugar_ocurrencia"),
                F.col("AF2.FEC_INGRESO_AFP").alias("fecha_inicio_vigencia"),
                F.coalesce(F.col(AF2_NID_NUMERO),F.lit("No registra")).alias("cedula"),
                F.col("AF2.FEC_NACIMIENTO").alias("fecha_nacimiento"),
                F.coalesce(F.col("AF2.GENERO"),F.lit("No registra")).alias("genero")
 
            )
        df_reserva = df_reserva.drop("OBSERVACIONES")
        df_gold_plus = df_reserva.unionByName(df_pago)
        
        df_gold_plus = df_gold_plus.withColumn(
            "valor_transaccion",
            F.when(F.col("id_tipo_transaccion_fk").isin('2','4'), F.col("valor_transaccion") *-1)
            .otherwise(F.col("valor_transaccion"))
        )

        return df_gold_plus, _df_reserva, CR, DR

StepGoldPlus = StepGoldPlus(glueContext, spark)
df, _df, _cr, _dr = StepGoldPlus.create_component()
_cr.orderBy("ID_COMENTARIO_RESERVA").select("COD_TIPO_MOVIMIENTO", "ID_COMENTARIO_RESERVA","comentario_reserva").show(truncate = False)
DATA = [
    ("1","1"),
    ("3","4"),
    ("5","4"),
    ("7","5"),
    ("11","7"),
    ("14","8"),
    ("16","2"),
    ("17","2"),
    ("21","2"),
    ("2","2"),
    ("4","2"),
    ("6","2"),
    ("10","2"),
    ("13","2"),
    ("8","2"),
    ("12","1"),
    ("22","1"),
    ("9","1"),
    ("23","1"),
    ("24","1")
]
from functools import reduce
homolog_when = reduce(
    lambda acc, DATA:acc.when(F.col("ID_COMENTARIO_RESERVA") == DATA[0], DATA[1]),
    DATA,
    F.when(F.col("ID_COMENTARIO_RESERVA").isNull(), None)
)
cr = _cr.withColumn(
    "homog_id_mov",
    homolog_when.otherwise(F.lit(0))
)
cr.orderBy("ID_COMENTARIO_RESERVA").select("COD_TIPO_MOVIMIENTO", "ID_COMENTARIO_RESERVA","comentario_reserva","homog_id_mov").show(truncate = False)
cr_dict = cr.select("comentario_reserva","homog_id_mov")
reglas_dict = dict(cr_dict.collect())
def buscar_en_texto(text):
    for palabra, valor in reglas_dict.items():
        if palabra.lower() in text.lower():
            return valor
    return None
from pyspark.sql.functions import udf
buscar_udf = udf(buscar_en_texto, StringType())
from pyspark.sql.functions import split, to_date, date_format, col, regexp_replace
from commons.TransformationHelper import TransformationHelper
TransformationHelper = TransformationHelper()
_df = TransformationHelper.data_normalice("OBSERVACIONES",_df)
_df = _df.withColumn( 
    "OBSERVACIONES",
    F.concat(
        F.upper(F.substring(F.col("OBSERVACIONES"), 1, 1)),
        F.lower(F.substring(F.col("OBSERVACIONES"), 2, 1000))
    )
)
_df = _df.withColumn(
    "OBSERVACIONES",
    regexp_replace(F.col("OBSERVACIONES"), r'[^a-zA-Z0-9\s]','')
    )

_df = _df.withColumn(
    "OBSERVACIONES",
    regexp_replace(F.col("OBSERVACIONES"), r'\b[a-zA-Z]\b','')
    )

_df = _df.withColumn(
    "OBSERVACIONES",
    F.trim(F.col("OBSERVACIONES"))
    )
_df = _df.withColumn(
    "homog_id_mov",
    buscar_udf(F.col("OBSERVACIONES"))
)
disminucion = ["17","11","14","21","3","12","5","8","7","22"]
aumento = ["2","10","13","1","4","9","6"]
_df = _df.withColumn(
    "id_tipo_transaccion_fk",
    F.when(
        (F.col("id_tipo_transaccion_fk") == 1) & (F.col("homog_id_mov").isin(aumento)),
        F.lit(F.col("homog_id_mov"))
    ).when(
        (F.col("id_tipo_transaccion_fk") == 2) & (F.col("homog_id_mov").isin(disminucion)),
        F.lit(F.col("homog_id_mov"))
    ).otherwise(F.col("id_tipo_transaccion_fk"))
)
_df = _df.withColumn(
    "id_tipo_transaccion_fk",
    F.coalesce(F.col("homog_id_mov"), F.col("id_tipo_transaccion_fk"))
)
_df = _df.withColumn(
    "id_tipo_transaccion_fk",
    F.when(F.col("OBSERVACIONES")=="Cet", F.lit("4")).otherwise(F.col("id_tipo_transaccion_fk"))
)
_df.filter(F.col("numero_siniestro") == 20252300577).select("OBSERVACIONES","homog_id_mov","id_tipo_transaccion_fk").show(truncate = False)
df_plus.filter(F.col("numero_siniestro") == 20252300577).select("id_tipo_transaccion_fk").show()
_df.select("OBSERVACIONES","homog_id_mov","id_tipo_transaccion_fk").show(truncate = False)
from pyspark.sql import functions as F,Window
from pyspark.sql.types import StringType
from functools import reduce
from pyspark.sql.functions import udf
from pyspark.sql.functions import split, to_date, date_format, col, regexp_replace
from commons.TransformationHelper import TransformationHelper

TransformationHelper = TransformationHelper()

class StepGoldPlus:
    def __init__(self, glue_context, spark):
        self.spark = spark

        
    def create_component(self):
        


        P_NID_POLIZA ="P.nid_poliza"
        SDT_ID_SINIESTRO = "SDT.ID_SINIESTRO"
        SDB_FECHA_SINIESTRO = "SDB.FECHA_SINIESTRO"
        CP_COD_COBERTURA=  "CP.COD_COBERTURA"
        CS_ID_CAUSAL = "CS.ID_CAUSAL"
        DP_VLR_PAGO = "DP.VLR_PAGO"
        CO_COD_ORIGEN_SINIESTRO = "CO.COD_ORIGEN_SINIESTRO"
        CP_NOMBRE = "CP.NOMBRE"
        PCL_NUM_IDEN  = "PCL.NUM_IDENTIFICACION"
        AF2_NID_NUMERO = "AF2.NID_NUMERO_IDENTIFICACION"


        DR = self.spark.table("global_temp.rsv_dato_reserva")
        DP = self.spark.table("global_temp.pgs_dato_pago")
        TM = self.spark.table("global_temp.rsv_tipo_movimiento")
        CP = self.spark.table("global_temp.rsv_cobertura")
        SDT = self.spark.table("global_temp.snr_dato_tramite")
        SDB = self.spark.table("global_temp.snr_dato_basico")
        CS = self.spark.table("global_temp.snr_causa")
        CO = self.spark.table("global_temp.snr_origen")
        P = self.spark.table("global_temp.rcd_polizas")
        PCL = self.spark.table("global_temp.cu_persona_cliente")
        AF = self.spark.table("global_temp.vw_consulta_cliente")
        AF2 = self.spark.table("global_temp.vw_localizaciones")
        CR = self.spark.table("global_temp.rsv_comentario_reserva")

        DATA = [
            ("1","1"),
            ("3","4"),
            ("5","4"),
            ("7","5"),
            #("11","7"),
            ("14","8"),
            ("16","2"),
            ("17","2"),
            ("21","2"),
            ("2","2"),
            ("4","2"),
            ("6","2"),
            ("10","2"),
            ("13","2"),
            ("8","2"),
            ("12","1"),
            ("22","1"),
            ("9","1"),
            ("23","1"),
            ("24","1")
        ]

        ##validacion lugar ocurrencia
        AF = AF.withColumn(
            "valor_max", F.greatest(F.col("COD_CIUDAD"), F.col("COD_DEPARTAMENTO"))
        )
        window_cedula = Window.partitionBy("NID_NUMERO_IDENTIFICACION").orderBy(F.col("valor_max").desc())
        AF = AF.withColumn("rank", F.row_number().over(window_cedula))
        AF = AF.filter(F.col("rank") == 1).drop("valor_max", "rank")
        
        df_reserva = DR.alias("DR") \
            .join(TM.alias("TM"), F.col("TM.COD_TIPO_MOVIMIENTO").cast("string") == F.col("DR.COD_TIPO_MOVIMIENTO").cast("string")) \
            .join(CP.alias("CP"), F.col("DR.COD_COBERTURA").cast("string") == F.col(CP_COD_COBERTURA).cast("string")) \
            .join(SDT.alias("SDT"), F.col("DR.ID_TRAMITE") == F.col("SDT.ID_TRAMITE")) \
            .join(SDB.alias("SDB"), F.col(SDT_ID_SINIESTRO) == F.col("SDB.ID_SINIESTRO")) \
            .join(CS.alias("CS"), F.col("SDB.COD_CAUSA_SINIESTRO").cast("string") == F.col(CS_ID_CAUSAL).cast("string")) \
            .join(CO.alias("CO"), F.col("SDB.COD_ORIGEN_SINIESTRO") == F.col(CO_COD_ORIGEN_SINIESTRO)) \
            .join(P.alias("P"), F.col(SDB_FECHA_SINIESTRO).between(F.col("P.FEC_INICIAL_VIGENCIA"), F.col("P.FEC_FINAL_VIGENCIA"))) \
            .join(PCL.alias("PCL"), F.col("PCL.NUM_PERSONA").cast("string") == F.col("SDB.NUM_PERSONA").cast("string")) \
            .join(AF.alias("AF"), (F.col(PCL_NUM_IDEN).cast("string") == F.col("AF.NID_NUMERO_IDENTIFICACION").cast("string")) & (F.col("PCL.cod_tipo_identificacion") == F.col("AF.cod_tipo_identificacion")),"left") \
            .join(AF2.alias("AF2"), (F.col(PCL_NUM_IDEN).cast("string") == F.col(AF2_NID_NUMERO).cast("string")) & (F.col("PCL.cod_tipo_identificacion") == F.col("AF2.cod_tipo_identificacion")),"left") \
            .select(
                F.col(CP_COD_COBERTURA).alias("id_categoria_fk"),
                F.col(CP_NOMBRE).alias("categoria"),
                F.lit("Reserva").alias("tipo"),
                F.col("DR.COD_TIPO_MOVIMIENTO").alias("id_tipo_transaccion_fk"),
                F.col("TM.NOMBRE").alias("tipo_transaccion"),
                F.col(SDT_ID_SINIESTRO).alias("numero_siniestro"),
                (F.col("DR.VLR_MOVIMIENTO")).alias("valor_transaccion"),
                F.col("DR.VLR_MOVIMIENTO").alias("valor_transaccion_ajustado"),
                F.col("DR.FECHA_MOVIMIENTO").alias("fecha_transaccion"),
                F.col(SDB_FECHA_SINIESTRO).alias("fecha_ocurrencia"),
                F.col(CS_ID_CAUSAL).alias("cod_causa"),
                F.col("CS.NOMBRE").alias("causa"),
                F.when(F.col(CP_COD_COBERTURA) == '3', F.col(CP_COD_COBERTURA)).otherwise(F.col(CO_COD_ORIGEN_SINIESTRO)).alias("cod_origen"),
                F.coalesce(F.col("SDT.FEC_ACT_SOLICITUD_AFP"), F.col("SDT.FEC_SOLICITUD_AFP")).alias("fecha_aviso"),
                F.coalesce(F.col("P.NID_POLIZA"), F.lit(999)).alias("poliza"),
                F.lit("3").cast("string").alias("id_fuente_fk"),
                F.lit("50").alias("compania"),
                F.lit(41).alias("id_producto_fk"),
                F.lit(31).alias("id_ramo_fk"),
                F.lit("Seguridad social").alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.coalesce(F.col("AF.COD_CIUDAD"), F.lit("01")).alias("ciudad_lugar_ocurrencia"),
                F.coalesce(F.col("AF.COD_DEPARTAMENTO").cast("string"), F.lit("01001")).alias("depto_lugar_ocurrencia"),
                F.col("AF2.FEC_INGRESO_AFP").alias("fecha_inicio_vigencia"),
                F.coalesce(F.col(AF2_NID_NUMERO),F.lit("No registra")).alias("cedula"),
                F.col("AF2.FEC_NACIMIENTO").alias("fecha_nacimiento"),
                F.coalesce(F.col("AF2.GENERO"),F.lit("No registra")).alias("genero"),
                F.col("DR.OBSERVACIONES")
                
            )
        
        homolog_when = reduce(
            lambda acc, DATA:acc.when(F.col("ID_COMENTARIO_RESERVA") == DATA[0], DATA[1]),
            DATA,
            F.when(F.col("ID_COMENTARIO_RESERVA").isNull(), None)
        )

        cr = CR.withColumn(
            "homog_id_mov",
            homolog_when.otherwise(F.lit(0))
        )   

        cr_dict = cr.select("comentario_reserva","homog_id_mov")
        reglas_dict = dict(cr_dict.collect())
        
        def buscar_en_texto(text):
            for palabra, valor in reglas_dict.items():
                if palabra.lower() in text.lower():
                    return valor
            return None
        
        buscar_udf = udf(buscar_en_texto, StringType())

        df_reserva = TransformationHelper.data_normalice("OBSERVACIONES",df_reserva)
        df_reserva = df_reserva.withColumn( 
            "OBSERVACIONES",
            F.concat(
                F.upper(F.substring(F.col("OBSERVACIONES"), 1, 1)),
                F.lower(F.substring(F.col("OBSERVACIONES"), 2, 1000))
            )
        )
        df_reserva = df_reserva.withColumn(
            "OBSERVACIONES",
            regexp_replace(F.col("OBSERVACIONES"), r'[^a-zA-Z0-9\s]','')
            )

        df_reserva = df_reserva.withColumn(
            "OBSERVACIONES",
            regexp_replace(F.col("OBSERVACIONES"), r'\b[a-zA-Z]\b','')
            )

        df_reserva = df_reserva.withColumn(
            "OBSERVACIONES",
            F.trim(F.col("OBSERVACIONES"))
            )
        
        df_reserva = df_reserva.withColumn(
            "homog_id_mov",
            buscar_udf(F.col("OBSERVACIONES"))
        )

        disminucion = ["17","14","21","3","12","5","8","7","22"]#se borro el 11
        aumento = ["2","10","13","1","4","9","6","1"]

        df_reserva = df_reserva.withColumn(
             "id_tipo_transaccion_fk",
            F.when(
                (F.col("id_tipo_transaccion_fk") == 1) & (F.col("homog_id_mov").isin(aumento)),
                F.lit(F.col("homog_id_mov"))
            ).when(
                (F.col("id_tipo_transaccion_fk") == 2) & (F.col("homog_id_mov").isin(disminucion)),
                F.lit(F.col("homog_id_mov"))
            ).otherwise(F.col("id_tipo_transaccion_fk"))
        )

        df_reserva = df_reserva.withColumn(
            "id_tipo_transaccion_fk",
            F.when(F.col("OBSERVACIONES")=="Cet", F.lit("4")).otherwise(F.col("id_tipo_transaccion_fk"))
        )

        df_reserva = df_reserva.drop("OBSERVACIONES", "homog_id_mov")

        df_pago = DP.alias("DP") \
            .join(TM.alias("TM"), F.col("TM.COD_TIPO_MOVIMIENTO").cast("string") == F.col("DP.COD_TIPO_MOVIMIENTO").cast("string")) \
            .join(CP.alias("CP"), F.col("DP.COD_COBERTURA").cast("string") == F.col(CP_COD_COBERTURA).cast("string")) \
            .join(SDT.alias("SDT"), F.col("DP.ID_TRAMITE") == F.col("SDT.ID_TRAMITE")) \
            .join(SDB.alias("SDB"), F.col(SDT_ID_SINIESTRO) == F.col("SDB.ID_SINIESTRO")) \
            .join(CS.alias("CS"), F.col("SDB.COD_CAUSA_SINIESTRO").cast("string") == F.col(CS_ID_CAUSAL).cast("string")) \
            .join(CO.alias("CO"), F.col("SDB.COD_ORIGEN_SINIESTRO") == F.col(CO_COD_ORIGEN_SINIESTRO)) \
            .join(P.alias("P"), F.col(SDB_FECHA_SINIESTRO).between(F.col("P.FEC_INICIAL_VIGENCIA"), F.col("P.FEC_FINAL_VIGENCIA"))) \
            .join(PCL.alias("PCL"), F.col("PCL.NUM_PERSONA").cast("string") == F.col("SDB.NUM_PERSONA").cast("string")) \
            .join(AF.alias("AF"), (F.col(PCL_NUM_IDEN).cast("string") == F.col("AF.NID_NUMERO_IDENTIFICACION").cast("string")) & (F.col("PCL.cod_tipo_identificacion") == F.col("AF.cod_tipo_identificacion")),"left") \
            .join(AF2.alias("AF2"), (F.col(PCL_NUM_IDEN).cast("string") == F.col(AF2_NID_NUMERO).cast("string")) & (F.col("PCL.cod_tipo_identificacion") == F.col("AF2.cod_tipo_identificacion")),"left") \
            .where(F.col(DP_VLR_PAGO).cast("double") != 0) \
            .select(
                F.col(CP_COD_COBERTURA).alias("id_categoria_fk"),
                F.col(CP_NOMBRE).alias("categoria"),
                F.lit("Pago").alias("tipo"),
                F.col("DP.COD_TIPO_MOVIMIENTO").alias("id_tipo_transaccion_fk"),
                F.col("TM.NOMBRE").alias("tipo_transaccion"),
                F.col(SDT_ID_SINIESTRO).alias("numero_siniestro"),
                (F.col(DP_VLR_PAGO).cast("double")).alias("valor_transaccion"),
                F.col(DP_VLR_PAGO).cast("double").alias("valor_transaccion_ajustado"),
                F.col("DP.FECHA_PAGO").alias("fecha_transaccion"),
                F.col(SDB_FECHA_SINIESTRO).alias("fecha_ocurrencia"),
                F.col(CS_ID_CAUSAL).alias("cod_causa"),
                F.col("CS.NOMBRE").alias("causa"),
		        F.when(F.col(CP_COD_COBERTURA) == '3', F.col(CP_COD_COBERTURA)).otherwise(F.col(CO_COD_ORIGEN_SINIESTRO)).alias("cod_origen"),
                F.coalesce(F.col("SDT.FEC_ACT_SOLICITUD_AFP"), F.col("SDT.FEC_SOLICITUD_AFP")).alias("fecha_aviso"),
                F.coalesce(F.col("P.NID_POLIZA"), F.lit(999)).alias("poliza"),
                F.lit("3").cast("string").alias("id_fuente_fk"),
                F.lit("50").alias("compania"),
                F.lit(41).alias("id_producto_fk"),
                F.lit(31).alias("id_ramo_fk"),
                F.lit("Seguridad saocial").alias("unidad_de_negocio"),
                F.concat(F.lit("PRV-"),F.lpad(F.coalesce(F.col(P_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),F.lit("-00")).alias("poliza_lider"),
                F.coalesce(F.col("AF.COD_CIUDAD"), F.lit("01")).alias("ciudad_lugar_ocurrencia"),
                F.coalesce(F.col("AF.COD_DEPARTAMENTO").cast("string"), F.lit("01001")).alias("depto_lugar_ocurrencia"),
                F.col("AF2.FEC_INGRESO_AFP").alias("fecha_inicio_vigencia"),
                F.coalesce(F.col(AF2_NID_NUMERO),F.lit("No registra")).alias("cedula"),
                F.col("AF2.FEC_NACIMIENTO").alias("fecha_nacimiento"),
                F.coalesce(F.col("AF2.GENERO"),F.lit("No registra")).alias("genero")
 
            )

        df_gold_plus = df_reserva.unionByName(df_pago)

        df_gold_plus = df_gold_plus.withColumn(
            "valor_transaccion",
            F.when(F.col("id_tipo_transaccion_fk").isin('2','4'), F.col("valor_transaccion") *-1)
            .otherwise(F.col("valor_transaccion"))
        )

        return df_gold_plus, df_reserva

StepGoldPlus = StepGoldPlus(glueContext, spark)
df, _df = StepGoldPlus.create_component()
_df.select("id_tipo_transaccion_fk").distinct().show()
job.commit()