

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from commons.ExtractHelper import ExtractHelper
from pyspark.sql import functions as F

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
from commons.StepGold import StepGold
from commons.IncrementalCharge import IncrementalCharge
from pyspark.sql.types import IntegerType
import sys
import io

IncrementalCharge = IncrementalCharge(glueContext, spark, "-", "-")
IncrementalCharge.charge_tmp_tables("s3://alfa-datalake-dev-silver/CLIENTES/DATOS_SOCIODEMOGRAFICOS/CLIENTES_DATOS_SOCIODEMOGRAFICOS_SILVER/2025/09/08/", "CLIENTES_DATOS_SOCIODEMOGRAFICOS_SILVER", 'Gold')
df  = spark.sql("SELECT * FROM global_temp.CLIENTES_DATOS_SOCIODEMOGRAFICOS_SILVER")
spark.catalog.listTables("global_temp")
ruta = spark.read.parquet("s3://alfa-datalake-dev-silver/CLIENTES/DATOS_SOCIODEMOGRAFICOS/CLIENTES_DATOS_SOCIODEMOGRAFICOS_SILVER/2025/09/08/")
ruta2 = spark.read.parquet("s3://alfa-datalake-dev-silver/CLIENTES/DATOS_SOCIODEMOGRAFICOS/CLIENTES_DATOS_SOCIODEMOGRAFICOS_STG/2025/09/03/")
ruta.createOrReplaceGlobalTempView("CLIENTES_DATOS_SOCIODEMOGRAFICOS_SILVER")
ruta2.createOrReplaceGlobalTempView("CLIENTES_DATOS_SOCIODEMOGRAFICOS_STG")
resultadostg = spark.sql("SELECT * FROM global_temp.CLIENTES_DATOS_SOCIODEMOGRAFICOS_STG")
resultadostg.select("nacionalidad").distinct().show()
resultado = spark.sql("SELECT * FROM global_temp.CLIENTES_DATOS_SOCIODEMOGRAFICOS_SILVER")
resultado.select("nacionalidad").distinct().show()
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from pyspark.sql import functions as F, Window
step_gold = StepGold(glueContext, spark)
df_pagos = step_gold.create_payments_component_original()
step_gold = StepGold(glueContext, spark)
df_pagos = step_gold.create_component_pagos()
df_pagos.count()
df = step_gold.create_component()
df = step_gold.create_component()
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
        C_NID_POLIZA  = "C.nid_poliza"

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

        df_total = df_total.select("A.*","fecha_ocurrencia",C_NID_POLIZA)

        df_total = df_total.withColumn(
            "poliza",
            F.coalesce(F.col(C_NID_POLIZA), F.lit(999)),
        ).withColumn(
            "poliza_lider",
            F.concat(
                F.lit("PRV-"),
                F.lpad(F.coalesce(F.col(C_NID_POLIZA).cast("string"), F.lit("999")), 7, "0"),
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

        CS_COD_CAUSA_SINIESTRO = "CS.cod_causa_siniestro"


        PS_VLR_PAGO ="PS.vlr_pago"
        DETALLE_RESERVA = "global_temp.spr_detalle_reserva"
        ACU_PERSONAS_NATURALES = "global_temp.acu_personas_naturales"
        RESERVA = "global_temp.spr_reserva"
        CONCEPTOS_PAGOS =  "global_temp.spr_conceptos_pagos_siniestros"
        CGR_MOVIMIENTO_RESERVA = "global_temp.cgr_movimientos_reserva"
        SPR_SINIESTROS = "global_temp.spr_siniestros"
        SPR_CAUSAS_SINIESTROS = "global_temp.spr_causas_siniestros"
        CGR_ORIGENES = "global_temp.cgr_origenes"
        PPR_POLIZAS = "global_temp.ppr_polizas"
        INCAPACIDADES = "global_temp.spr_incapacidades_sol_audit"
        DETALLE_PAGO ="global_temp.spr_detalle_pago_siniestro"
        SPR_PAGOS ="global_temp.spr_pagos"
        PPR_AFILIADO  ="global_temp.ppr_afiliados"
        TIPOS_RECUPERACIONES ="global_temp.spr_tipos_recuperaciones"
        DETALLE_RECUPERACIONES ="global_temp.spr_detalle_recuperaciones"
        ORIGENES_HIST = "global_temp.Spr_siniestros_origenes_hist"
        SPR_RECUPERACIONES = "global_temp.spr_recuperaciones"

        UNIDAD_DE_NEGOCIO = "Seguridad social"
        DESC_CAUSA = "Otras causas"
        CS_DESCRIPCION = "CS.descripcion"
 

        # =====================================================
        # PAGOS - IMPLEMENTACIÓN EXACTA DEL ARCHIVO ORIGINAL
        # =====================================================
        
        # Cargar tablas para PAGOS

        PN = self.spark.table(ACU_PERSONAS_NATURALES).alias("PN")
        CP = self.spark.table(CONCEPTOS_PAGOS)
        TM = self.spark.table(CGR_MOVIMIENTO_RESERVA)
        SIN = self.spark.table(SPR_SINIESTROS).alias("SIN")
        CS = self.spark.table(SPR_CAUSAS_SINIESTROS)
        CO = self.spark.table(CGR_ORIGENES).alias("CO")
        P  = self.spark.table(PPR_POLIZAS)
        ISA= self.spark.table(INCAPACIDADES).alias("ISA")
        PS = self.spark.table(DETALLE_PAGO)
        PG = self.spark.table(SPR_PAGOS).alias("PG")
        PA = self.spark.table(PPR_AFILIADO).alias("PA")
        H2 = self.spark.table(ORIGENES_HIST).alias("H2")
        df_origen = self.spark.table(ORIGENES_HIST)
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
                F.lit("1").alias("periodo_point"),
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
            
            
        ).filter(F.col(PS_VLR_PAGO) != 0)
        

        

        # =====================================================
        # RECUPERACIONES - IMPLEMENTACIÓN EXACTA DEL ARCHIVO ORIGINAL
        # =====================================================
        
        # Cargar tablas para RECUPERACIONES (usando alias diferentes)

        PN_R = self.spark.table(ACU_PERSONAS_NATURALES).alias("PN")
        PN2_R = self.spark.table(ACU_PERSONAS_NATURALES).alias("PN2")
        TM_R = self.spark.table(CGR_MOVIMIENTO_RESERVA)
        SIN_R = self.spark.table(SPR_SINIESTROS).alias("SIN")
        CS_R = self.spark.table(SPR_CAUSAS_SINIESTROS)
        CO_R = self.spark.table(CGR_ORIGENES).alias("CO")
        ISA_R= self.spark.table(INCAPACIDADES).alias("ISA")
        PA_R = self.spark.table(PPR_AFILIADO).alias("PA")
        RP_R = self.spark.table(SPR_RECUPERACIONES).alias("RP")
        TR_R = self.spark.table(TIPOS_RECUPERACIONES)
        DT_R = self.spark.table(DETALLE_RECUPERACIONES).alias("DT")
        H2_R = self.spark.table(ORIGENES_HIST).alias("H2")

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
                F.lit("1").alias("periodo_point"),
                F.col("SIN.fec_siniestro"),
                F.col("ISA_FIL_CTE.isa_dia_181_recup").alias("dia_181")
            )
            .alias("FOC_RECUP")
        )

        # Query principal RECUPERACIONES
       
        # =====================================================
        # RESERVAS - IMPLEMENTACIÓN EXACTA DEL ARCHIVO ORIGINAL
        # =====================================================
        DR_S = self.spark.table(DETALLE_RESERVA)
        PN_S = self.spark.table(ACU_PERSONAS_NATURALES).alias("PN")
        PN2_S = self.spark.table(ACU_PERSONAS_NATURALES).alias("PN2")
        SR_S = self.spark.table(RESERVA).alias("SR")
        CP_S = self.spark.table(CONCEPTOS_PAGOS)
        TM_S = self.spark.table(CGR_MOVIMIENTO_RESERVA)
        SIN_S = self.spark.table(SPR_SINIESTROS).alias("SIN")
        CS_S = self.spark.table(SPR_CAUSAS_SINIESTROS)
        CO_S = self.spark.table(CGR_ORIGENES).alias("CO")
        ISA_S= self.spark.table(INCAPACIDADES).alias("ISA")
        PA_S = self.spark.table(PPR_AFILIADO).alias("PA")
        H2_S = self.spark.table(ORIGENES_HIST).alias("H2")

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


     
        # =====================================================
        # SELECCIONES FINALES CON CAMPOS EN ORDEN ESPECÍFICO
        # =====================================================

        
        # PAGOS con campos en orden específico
        pagos_final = df_pago.select(
                F.col("PG.nid_pago").alias("nid_pago"),#no esta
                F.col("PG.periodo_point").alias("periodo_point")
                       )
        pagos_final = pagos_final.drop("periodo_point")
        #df_payments = self.add_origin(pagos_final, df_origen)
        
        print(df_pago.count())
        # RECUPERACIONES con campos en orden específico
     
        # =====================================================
        # UNION FINAL
        # =====================================================
        df_final = pagos_final
        df_final = df_final.drop("origen_default","origen_new","origen_ant")
        df_final = df_final.withColumnRenamed("origen_final", "cod_origen")

 
        return df_final
step_gold = StepGold(glueContext, spark)
df_pagos = step_gold.create_component()
df_pagos.count()
step_gold = StepGold(glueContext, spark)
df_pagos = step_gold.create_component()
df_origen.filter(F.col("nid_siniestro") == 20101857).show()
df_origen.count()
df_origen.count()
--4174703569
--8349298124
join_origen = df_pagos.alias("M").join(
    df_origen.alias("O"),
    (F.col("M.numero_siniestro") == F.col("O.nid_siniestro")) &
    (F.col("M.fecha_transaccion") < F.col("O.fec_modificacion")),
    "left"
)

w = Window.partitionBy("M.numero_siniestro", "M.fecha_transaccion") \
    .orderBy(F.col("O.fec_modificacion"))

origen_por_rango = join_origen.withColumn("rn", F.row_number().over(w)) \
                    .filter("rn = 1") \
                    .select("M.*", F.col("O.cod_origen_siniestro_ant").alias("origen_ant"))

ultima = df_origen.withColumn("rn", F.row_number().over(
        Window.partitionBy("nid_siniestro").orderBy(F.col("fec_modificacion").desc())
    )).filter("rn = 1").select("nid_siniestro", F.col("cod_origen_siniestro_new").alias("origen_new"))

df = df_pagos.alias("M").join(
    origen_por_rango.alias("R"),
    on = ["numero_siniestro", "fecha_transaccion"],
    how = "left"
)

df = df.join(ultima.alias("U"),
             df["numero_siniestro"] == ultima["nid_siniestro"],
             "left").drop("nid_siniestro")

df = df.withColumn(
        "origen_final",
        F.when(F.col("R.origen_ant").isNotNull(), F.col("R.origen_ant")).otherwise(F.col("U.origen_new"))
)

df = df.withColumn(
        "origen_final",
        F.when(F.col("origen_final").isNull(), F.col("M.origen_default")).otherwise(F.col("origen_final"))
)
join_origen = df_pagos.alias("M").join(
    df_origen.alias("O"),
    (F.col("M.numero_siniestro") == F.col("O.nid_siniestro")) &
    (F.col("M.fecha_transaccion") < F.col("O.fec_modificacion")),
    "left"
)
join_origen.filter((F.col("numero_siniestro") == 20154973) & (F.year(F.col("fecha_transaccion")) == 2017)).count()
w = Window.partitionBy("M.numero_siniestro", "M.fecha_transaccion") \
    .orderBy(F.col("O.fec_modificacion"))
origen_por_rango = join_origen.withColumn("rn", F.row_number().over(w)) \
                    .filter("rn = 1") \
                    .select("M.*", F.col("O.cod_origen_siniestro_ant").alias("origen_ant"))
#obtener ultimo origen

ultima = df_origen.withColumn("rn", F.row_number().over(
        Window.partitionBy("nid_siniestro").orderBy(F.col("fec_modificacion").desc())
    )).filter("rn = 1").select("nid_siniestro", F.col("cod_origen_siniestro_new").alias("origen_new"))
df = df_pagos.alias("M").join(
    origen_por_rango.alias("R"),
    on = ["numero_siniestro", "fecha_transaccion"],
    how = "left"
)

df = df.join(ultima.alias("U"),
             df["numero_siniestro"] == ultima["nid_siniestro"],
             "left").drop("nid_siniestro")
df = df.withColumn(
        "origen_final",
        F.when(F.col("R.origen_ant").isNotNull(), F.col("R.origen_ant")).otherwise(F.col("U.origen_new"))
)
df = df.withColumn(
        "origen_final",
        F.when(F.col("origen_final").isNull(), F.col("M.origen_default")).otherwise(F.col("origen_final"))
)
df.filter(F.col("origen_final").isNull()).count()
df.filter((F.col("numero_siniestro") == 20145619) & (F.year(F.col("fecha_transaccion")) == 2017) & (F.month(F.col("fecha_transaccion")) == 1)).count()
df.filter((F.col("numero_siniestro") == 20147084) & (F.year(F.col("fecha_transaccion")) == 2017) & (F.month(F.col("fecha_transaccion")) == 1)).select("numero_siniestro", "fecha_transaccion", "M.valor_transaccion","M.fecha_ocurrencia","origen_final").show()
df_test = df.groupBy(F.col("numero_siniestro")) \
    .agg(F.countDistinct("origen_final").alias("count")) \
    .filter(F.col("count") > 1) \
    .select("numero_siniestro")
df.filter((F.col("numero_siniestro") == 20145619)).select("numero_siniestro", "fecha_transaccion", "M.valor_transaccion","M.fecha_ocurrencia","origen_final").show(truncate =False)
df.filter((F.col("numero_siniestro") == 20145619) & (F.year(F.col("fecha_transaccion")) == 2017) & (F.month(F.col("fecha_transaccion")) == 1)).select("numero_siniestro", "fecha_transaccion", "M.valor_transaccion","M.fecha_ocurrencia","origen_final").show()
df_pagos.filter((F.col("numero_siniestro") == 20113345) & (F.year(F.col("fecha_transaccion")) == 2017) & (F.month(F.col("fecha_transaccion")) == 1)).count()
df_pagos.filter((F.col("numero_siniestro") == 201700254) & (F.year(F.col("fecha_transaccion")) == 2017) & (F.month(F.col("fecha_transaccion")) == 1)).select("numero_siniestro", "fecha_transaccion", "valor_transaccion","fecha_ocurrencia").show()
datos = [

20101857,
20103542,
20113345,
20120106,
20123871,
20133511,
20141949,
20142291,
20142374,
20143127,
20144010,
20144387,
20146217,
20146900,
20147084,
20151988,
20152034,
20153008,
20153678,
20154973,
20155875,
20156179,
20157360,
20158738,
20161186,
20161350,
20161473,
20161913,
20162085,
20162218,
20162299,
20162321,
20163191,
20164723,
20164763,
20164771,
20165311,
20165375,
20165831,
20166265,
20166481,
20166498,
20166523,
20166991,
20167659,
20167670,
20167684,
20167983,
20167990,
20167994,
20168310,
20168503,
20168814,
20168831,
20168967,
20168997,
20169013,
20169381,
20169448,
20169571,
201698003,
201698232,
201698517,
201698715,
201698739,
201698745,
201698755,
201698890,
201698904,
201700159,
201700160,
201700254,
201700381,
201700397,
201700538,
201700540,
201700714

]
df_count = df.filter((F.col("numero_siniestro").isin(datos)) & (F.year(F.col("fecha_transaccion")) == 2017) & (F.month(F.col("fecha_transaccion")) == 1)).groupBy("numero_siniestro").count()
df_count.write.mode("overwrite").csv("s3://alfa-datalake-dev-assets/result_ola1/")
df.count()
df_pagos.filter((F.col("numero_siniestro") == 20145619) & (F.year(F.col("fecha_transaccion")) == 2017) & (F.month(F.col("fecha_transaccion")) == 1)).distinct().count()
df.filter((F.col("numero_siniestro") == 20145619) & (F.year(F.col("fecha_transaccion")) == 2017) & (F.month(F.col("fecha_transaccion")) == 1)).select("numero_siniestro", "fecha_transaccion", "M.valor_transaccion","M.fecha_ocurrencia","origen_final").show()
df_test = df_pagos.filter((F.col("numero_siniestro") == 20145619) & (F.year(F.col("fecha_transaccion")) == 2017) & (F.month(F.col("fecha_transaccion")) == 1))
df_pagos.filter((F.col("numero_siniestro") == 20145619) & (F.year(F.col("fecha_transaccion")) == 2017) & (F.month(F.col("fecha_transaccion")) == 1)).select("numero_siniestro", "fecha_transaccion", "valor_transaccion","fecha_ocurrencia").show()
for col in df_test.columns:
    n_distintos = df_test.select(col).distinct().count()
    if n_distintos > 1:
        print(f"Columna '{col}' tiene {n_distintos} valores distintos.")
joined = df_pagos.alias("M").join(
    df_origen.alias("O"),
    (F.col("M.numero_siniestro") == F.col("O.nid_siniestro")) &
    (F.col("M.fecha_transaccion") < F.col("O.fec_modificacion")),
    "left"
)
joined.filter((F.col("numero_siniestro") == 20154973) & (F.year(F.col("fecha_transaccion")) == 2017)).count()
window = Window.partitionBy("M.numero_siniestro", "M.fecha_transaccion") \
    .orderBy(F.col("O.fec_modificacion").asc())
#joined = joined.withColumn("rn", F.row_number().over(window)) \
#    .filter(F.col("rn") == 1)
joined.filter((F.col("numero_siniestro") == 20154973) & (F.year(F.col("fecha_transaccion")) == 2017)).count()
con_rango = joined.withColumn("rn", F.row_number().over(window)).filter("rn = 1")
con_origen = con_rango.withColumn("origen_final", F.col("cod_origen_siniestro_ant")).select("M.*","origen_final")
sin_origen = df_pagos.alias("M").join(
    con_origen.select("numero_siniestro","fecha_transaccion"),
    on = ["numero_siniestro", "fecha_transaccion"],
    how = "left_anti"
)

ultima_fecha = df_origen.withColumn("rn", F.row_number().over(
        Window.partitionBy("nid_siniestro").orderBy(F.col("fec_modificacion").desc())
    )).filter("rn = 1").select("nid_siniestro", F.col("cod_origen_siniestro_new").alias("origen_final"))

sin_origen = sin_origen.join(ultima_fecha, F.col("nid_siniestro") == F.col("numero_siniestro"), how="left")
sin_origen = sin_origen.drop("nid_siniestro")
resultado_final = con_origen.unionByName(sin_origen)
resultado_final.filter((F.col("numero_siniestro") == 20154973) & (F.year(F.col("fecha_transaccion")) == 2017)).count()
movimiento_con_origen = joined.withColumn(
    "origen_final",
     F.col("cod_origen_siniestro_ant")
)
sin_origen = movimiento_con_origen.filter(F.col("origen_final").isNull()).select("M.*")
ultima_fecha = df_origen.withColumn("rn", F.row_number().over(
        Window.partitionBy("nid_siniestro").orderBy(F.col("fec_modificacion").desc())
    )).filter("rn = 1").select("nid_siniestro", F.col("cod_origen_siniestro_new").alias("origen_final"))

completados = sin_origen.join(ultima_fecha, F.col("numero_siniestro") == F.col("nid_siniestro"), "left") 

con_origen_directo = movimiento_con_origen.filter(F.col("origen_final").isNotNull()) \
                                            .select("M.*", "origen_final")
completados = completados.drop("nid_siniestro")
resultado_final = con_origen_directo.unionByName(completados)
resultado_final.filter((F.col("numero_siniestro") == 20154973) & (F.year(F.col("fecha_transaccion")) == 2017)).count()
df_pagos.filter((F.col("numero_siniestro") == 20154973) & (F.year(F.col("fecha_transaccion")) == 2017)).count()
final_df = movimiento_con_origen.filter(F.col("origen_final").isNotNull()) \
    .unionByName(
        sin_origen.join(ultima_fecha,F.col("numero_siniestro") == F.col("numero_siniestro"), "left")
            .withColumnRenamed("cod_origen_siniestro_new", "origen_final")
    )
window_origen = Window.orderBy("fec_modificacion")
df_origen_rango = df_origenes \
    .withColumn("fecha_fin", F.lead("fec_modificacion").over(window_origen)) \
    .withColumn("fecha_fin", F.col("fecha_fin") - F.expr("INTERVAL 1 DAYS"))
df_origen_rango.filter(F.col("nid_siniestro")==20101857).show()
df_movimientos_pago = df_pagos.join(
    df_origen_rango,
    (F.col("fecha_transaccion") >= F.col("FEC_MODIFICACION")) & 
    ((F.col("fecha_transaccion") <= F.col("fecha_fin"))|(F.col("fecha_fin").isNull())),
    "Left"
)
df_movimientos_pago.filter((F.col("numero_siniestro") == 20101857) & (F.year(F.col("fecha_transaccion")) == 2017)).select("numero_siniestro","fecha_transaccion", "FEC_MODIFICACION", "COD_ORIGEN_SINIESTRO_ANT", "COD_ORIGEN_SINIESTRO_NEW","fecha_fin","nid_siniestro_origen_hist").show()
df_movimientos_pago.show()
df_movimientos_pago = df_movimientos_pago.withColumn(
    "def_origen",
    F.when((F.col("fecha_transaccion") <= F.col("FEC_MODIFICACION")), F.col("COD_ORIGEN_SINIESTRO_ANT"))
    .otherwise(F.col("COD_ORIGEN_SINIESTRO_NEW"))
)
df_movimientos_pago.filter((F.col("numero_siniestro") == 20101857) & (F.year(F.col("fecha_transaccion")) == 2017)).select("numero_siniestro","def_origen").show()
df_min_fecha = df_origen.select(F.min(F.col("SH.FEC_MODIFICACION")).alias("fecha_minima"))
df_full = df_movimientos_pago.crossJoin(df_min_fecha)
df_movimientos_pago = df_full.withColumn(
    "origen_def",
    F.when(F.col("fecha_transaccion") < F.col("fecha_minima"), F.col("COD_ORIGEN_SINIESTRO_ANT"))
    .otherwise(F.col("COD_ORIGEN_SINIESTRO_NEW"))
)
df_movimientos_pago.show(1) 
df_pagos = df_pagos.filter((F.col("numero_siniestro") == 20103542) & (F.year(F.col("fecha_transaccion"))== 2017))
df_pagos.show()
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
    
    def add_occurency_date(self, df):
        """
        Any comment
        """
        df = df.drop("poliza_lider", "poliza", "fecha_ocurrencia")
        df = df.withColumn(
            "fecha_ocurrencia",
            F.when(
                F.col("origen_final").isin(742, 746, 741, 745),
                F.col("M.fec_siniestro")
            ).otherwise(F.col("M.dia_181"))
        )
        return df
   
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
            .join(fecha_ocurrencia_calculada.alias("FOC"), F.col("PS.nid_pago") == F.col("FOC.nid_pago"))
            .join(CP.alias("CP"), F.col("PS.cod_concepto_pago_siniestro") == F.col("CP.cod_concepto_pago_siniestro"))
            .join(TM.alias("TM"), F.col("PS.cod_movimiento") == F.col("TM.cod_movimiento"))
            .join(SIN.alias("SIN"), F.col("SIN.nid_siniestro") == F.col("FOC.nid_siniestro"))
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
            .join(P.alias("P"), 
                  (F.col("FOC.fecha_ocurrencia_new") >= F.col("P.fec_inicial_vigencia")) &
                  (F.col("FOC.fecha_ocurrencia_new") <= F.col("P.fec_final_vigencia")), "left")
            
            
        ).filter(F.col(PS_VLR_PAGO) != 0)
        
        df_pago_v2 = (
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
            .withColumn(
                "cod_origen_calculado",
                F.when(
                    F.col("RP.periodo_point").cast("int") >= 
                    ((F.year(F.col("SH.fec_modificacion")) * 100) + F.month(F.col("SH.fec_modificacion"))),
                    F.col("SH.cod_origen_siniestro_new")
                ).when(
                    F.col("RP.periodo_point").cast("int") < 
                    ((F.year(F.col("SH.fec_modificacion")) * 100) + F.month(F.col("SH.fec_modificacion"))),
                    F.col("SH.cod_origen_siniestro_ant")
                ).otherwise(
                    F.col("CO.cod_origen")
                )
            )
            .withColumn(
                "fecha_ocurrencia_new",
                F.when(
                    (F.col("cod_origen_calculado") == 747) & F.col("ISA_FIL_CTE.isa_dia_181_recup").isNotNull(),
                    F.col("ISA_FIL_CTE.isa_dia_181_recup")
                ).otherwise(
                    F.col("SIN.fec_siniestro")
                )
            )
            .select(
                F.col("DT.nid_recuperacion"),
                F.col("RP.nid_siniestro"),
                F.col("RP.periodo_point"),
                F.col("SIN.fec_siniestro"),
                F.col("ISA_FIL_CTE.isa_dia_181_recup").alias("dia_181"),
                F.col("cod_origen_calculado"),
                F.col("fecha_ocurrencia_new")
            )
            .alias("FOC_RECUP")
        )

        # Query principal RECUPERACIONES
        df_recup = (
            fecha_ocurrencia_calculada_recup.alias("FOC")
            .join(DT_R.alias("DT"), F.col("DT.nid_recuperacion") == F.col("FOC.nid_recuperacion"), "left")
            .join(RP_R.alias("RP"), F.col("RP.nid_recuperacion") == F.col("FOC.nid_recuperacion"), "left")
            .join(TR_R.alias("TR"), F.col("RP.cod_tipo_recuperacion") == F.col("TR.cod_tipo_recuperacion"))
            .join(TM_R.alias("TM"), F.col("RP.cod_movimiento") == F.col("TM.cod_movimiento"))
            .join(SIN_R.alias("SIN"), F.col("SIN.nid_siniestro") == F.col("FOC.nid_siniestro"))
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

            .join(P_R.alias("P"), 
                  (F.col("FOC.fecha_ocurrencia_new") >= F.col("P.fec_inicial_vigencia")) &
                  (F.col("FOC.fecha_ocurrencia_new") <= F.col("P.fec_final_vigencia")), "left")
      
            
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
            .withColumn(
                "cod_origen_calculado",
                F.when(
                    F.col("SR.periodo_point").cast("int") >= 
                    ((F.year(F.col("SH.fec_modificacion")) * 100) + F.month(F.col("SH.fec_modificacion"))),
                    F.col("SH.cod_origen_siniestro_new")
                ).when(
                    F.col("SR.periodo_point").cast("int") < 
                    ((F.year(F.col("SH.fec_modificacion")) * 100) + F.month(F.col("SH.fec_modificacion"))),
                    F.col("SH.cod_origen_siniestro_ant")
                ).otherwise(
                    F.col("CO.cod_origen")
                )
            )
            .withColumn(
                "fecha_ocurrencia_new",
                F.when(
                    (F.col("cod_origen_calculado") == 747) & F.col("ISA_FIL_CTE.isa_dia_181_reservas").isNotNull(),
                    F.col("ISA_FIL_CTE.isa_dia_181_reservas")
                ).otherwise(
                    F.col("SIN.fec_siniestro")
                )
            )
            .select(
                F.col("DR.nid_reserva"),
                F.col("DR.nid_siniestro"),
                F.col("SR.periodo_point"),
                F.col("SIN.fec_siniestro"),
                F.col("ISA_FIL_CTE.isa_dia_181_reservas").alias("dia_181"),
                F.col("CP.descripcion").alias("concepto_descripcion"),
                F.col("cod_origen_calculado"),
                F.col("fecha_ocurrencia_new")
            )
            .alias("FOC_RESERVAS")
        )

        # Query principal RESERVAS
        df_reserva = (
        
        
        
            fecha_ocurrencia_calculada_reservas.alias("FOC")
            .join(DR_S.alias("DR"), F.col("DR.nid_reserva") == F.col("FOC.nid_reserva"))
            .join(SR_S.alias("SR"), F.col("DR.nid_reserva") == F.col("SR.nid_reserva"))
            .join(CP_S.alias("CP"), F.col("DR.cod_concepto_pago_siniestro") == F.col("CP.cod_concepto_pago_siniestro"))
            .join(TM_S.alias("TM"), F.col("DR.cod_movimiento") == F.col("TM.cod_movimiento"))
            .join(SIN_S.alias("SIN"), F.col("SIN.nid_siniestro") == F.col("FOC.nid_siniestro"))
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
            .join(P_S.alias("P"), 
                  (F.col("FOC.fecha_ocurrencia_new") >= F.col("P.fec_inicial_vigencia")) &
                  (F.col("FOC.fecha_ocurrencia_new") <= F.col("P.fec_final_vigencia")), "left")
           
        ).filter(F.col("DR.VLR_RESERVA") != 0)

        # =====================================================
        # SELECCIONES FINALES CON CAMPOS EN ORDEN ESPECÍFICO
        # =====================================================
        
        # PAGOS con campos en orden específico
        pagos_final = df_pago_v2.select(
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
                #F.concat(
                #    F.lit("PRV-"),
                #    F.lpad(F.coalesce(F.col("P.nid_poliza").cast("string"), F.lit("999")), 7, "0"),
                #    F.lit("-00")
                #).alias("poliza_lider"),                
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
                F.col("ISA_FIL_MAIN.isa_dia_181_pagos").alias("dia_181"),
                F.col("SIN.fec_siniestro")
        ).distinct()
        pagos_final = pagos_final.drop("nid_pago","periodo_point")
        df_payments = self.add_origin(pagos_final, df_origen)
        
        
        # RECUPERACIONES con campos en orden específico
        recuperaciones_final = df_recup.select(
            F.col("DT.NID_RECUPERACION").alias("NID_RECUPERACION"),
            F.col("FOC.periodo_point").alias("periodo_point"),#no esta
            F.col("TR.cod_tipo_recuperacion").cast("string").alias("id_categoria_fk"),
            F.col("TR.nombre").alias("categoria"),
            F.lit("Recuperaciones").alias("tipo"),
            F.col("RP.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
            F.col("TM.descripcion").alias("tipo_transaccion"),           
            F.col("FOC.nid_siniestro").cast("string").alias("numero_siniestro"),
            F.abs(F.col("DT.vlr_concepto")).alias("valor_transaccion"),
            F.col("DT.vlr_concepto").alias("valor_transaccion_ajustado"),            
            F.col("RP.fec_creacion_registro").alias("fecha_transaccion"),
            F.col("FOC.fecha_ocurrencia_new").alias("fecha_ocurrencia"),    
            F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
            F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),
            # Cod_origen (ya calculado en CTE)
            F.col("CO.COD_ORIGEN").alias("origen_default"),
   
            F.coalesce(F.col("SIN.fec_inicio_solicitud_afp"), F.col("SIN.fec_solicitud_afp")).alias("fecha_aviso"),
            F.coalesce(F.col("P.nid_poliza"), F.lit(999)).alias("poliza"),
            F.lit("2").cast("string").alias("id_fuente_fk"),
            F.lit("50").cast("string").alias("compania"),
            F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
            F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
            F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
            F.concat(
                F.lit("PRV-"),
                F.lpad(F.coalesce(F.col("P.nid_poliza").cast("string"), F.lit("999")), 7, "0"),
                F.lit("-00")
            ).alias("poliza_lider"),
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
            F.col("PN.genero").alias("genero"),
            F.col("FOC.dia_181"),
            F.col("FOC.fec_siniestro")            
        ).distinct()
        recuperaciones_final = recuperaciones_final.drop("NID_RECUPERACION","periodo_point")
        df_recovery = self.add_origin(recuperaciones_final, df_origen)

        
        # RESERVAS con campos en orden específico
        reservas_final = df_reserva.select(
            F.col("DR.NID_RESERVA").alias("NID_RESERVA"),
            F.col("FOC.periodo_point").alias("periodo_point"),#no esta
            F.col("CP.cod_concepto_pago_siniestro").cast("string").alias("id_categoria_fk"),
            F.col("CP.descripcion").alias("categoria"),
            F.lit("Reservas").alias("tipo"),
            F.col("DR.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
            F.col("TM.descripcion").alias("tipo_transaccion"),
            F.col("FOC.nid_siniestro").cast("string").alias("numero_siniestro"),                
            # Campos de valores
            F.abs(F.col("DR.vlr_reserva")).alias("valor_transaccion"),
            F.col("DR.vlr_reserva").alias("valor_transaccion_ajustado"),
            # Campos de fechas
            F.col("SR.fec_generado").alias("fecha_transaccion"),
            F.col("FOC.fecha_ocurrencia_new").alias("fecha_ocurrencia"),
            F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
            F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),                
            # Cod_origen (ya calculado en CTE)
            F.col("CO.COD_ORIGEN").alias("origen_default"),
            F.coalesce(F.col("SIN.fec_inicio_solicitud_afp"), F.col("SIN.fec_solicitud_afp")).alias("fecha_aviso"),
            F.coalesce(F.col("P.nid_poliza"), F.lit(999)).alias("poliza"),
            # CAMPOS HARDCODED (manteniendo los existentes)
            F.lit("2").cast("string").alias("id_fuente_fk"),
            F.lit("50").cast("string").alias("compania"),
            F.lit(41).cast(decimal_cast).alias("id_producto_fk"),
            F.lit(31).cast(decimal_cast).alias("id_ramo_fk"),
            F.lit(UNIDAD_DE_NEGOCIO).alias("unidad_de_negocio"),
            F.concat(
                F.lit("PRV-"),
                F.lpad(F.coalesce(F.col("P.nid_poliza").cast("string"), F.lit("999")), 7, "0"),
                F.lit("-00")
            ).alias("poliza_lider"),
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
            F.col("PN.genero").alias("genero"),
            F.col("FOC.dia_181"),
            F.col("FOC.fec_siniestro")
        ).distinct()
        reservas_final = reservas_final.drop("NID_RESERVA","periodo_point")
        df_reserver = self.add_origin(reservas_final, df_origen)

        
        # =====================================================
        # UNION FINAL
        # =====================================================
        
        #df_payments = self.add_occurency_date(df_payments)
        #df_recovery = self.add_occurency_date(df_recovery)
        #df_reserver = self.add_occurency_date(df_reserver)
        
        df_final = df_payments
        #df_final = df_payments.unionAll(df_recovery).unionAll(df_reserver)
        #df_final = df_final.drop("origen_default","origen_new","origen_ant")
        #df_final = df_final.withColumnRenamed("origen_final", "cod_origen")


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
            #"fecha_ocurrencia",
            "cod_causa",
            "causa",
            #"cod_origen",
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
            "fec_siniestro",
            "dia_181"
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
        
        

        return df_payments,df_final, pagos_final
step_gold = StepGold(glueContext, spark)
dfs, df, df_ = step_gold.create_component()
df_.filter((F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==1)).count()
dfs = dfs.drop("poliza_lider", "poliza", "fecha_ocurrencia")
dfs.filter((F.col("M.id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==1)).count()
dfs.filter((F.col("M.numero_siniestro") == 20153008 )&(F.col("M.id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==1)).select("M.fec_siniestro","M.dia_181","origen_final","M.fecha_ocurrencia").show()
df_.filter(F.col("nid_siniestro") == 201804661).show()
dfs.filter((F.col("M.numero_siniestro") == 20123591 )&(F.col("M.id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==1)).select("M.fec_siniestro","M.dia_181","origen_final","M.fecha_ocurrencia").show()
_dfs = dfs.groupBy("M.numero_siniestro").agg(F.count("*").alias("conteo")).filter(F.col("conteo") > 1 ).select("*")
__dfs = dfs.groupBy("M.numero_siniestro").agg(F.count("*").alias("conteo")).filter(F.col("conteo") == 1 ).select("*")
_dfs = dfs.join(_dfs, "numero_siniestro", "inner")
__dfs = dfs.join(__dfs, "numero_siniestro", "inner")
_dfs = _dfs.withColumn(
    "fecha_validacion",
    F.when(
        F.col("origen_final").isin(742, 746, 741, 745),
        F.col("M.fec_siniestro")
    ).otherwise(F.col("M.dia_181"))
)
_dfs = _dfs.filter((F.col("fecha_validacion") == F.col("M.fecha_ocurrencia")))
__dfs = __dfs.withColumn(
    "fecha_validacion",
    F.col("M.fecha_ocurrencia")
)
df_total = _dfs.unionAll(__dfs)
df_total.filter((F.col("M.id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==1)).count()
_dfs.filter((F.col("M.numero_siniestro") == 201804661 )&(F.col("M.id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==1)).select("M.fec_siniestro","M.dia_181","origen_final","M.fecha_ocurrencia","fecha_validacion").show()
df_data = _dfs.filter((F.col("M.id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==1)).select("M.numero_siniestro")
_dfs.filter((F.col("M.id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==1)).count()
df_prueba_1 = _dfs.filter((F.col("fecha_validacion") == F.col("M.fecha_ocurrencia")) & (F.col("M.id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==1))
df_prueba_1.count()
df_prueba_2 = _dfs.filter((F.col("fecha_validacion") != F.col("M.fecha_ocurrencia")) & (F.col("M.id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==1))
df_pruebas = df_prueba_2.join(
    df_prueba_1,
    "numero_siniestro",
    "leftanti"
)

df_prueba_2.select("numero_siniestro").show()
df_pruebas.select("numero_siniestro").show()
df_pruebas.count()
df_final = df_pruebas.unionAll(df_prueba_1)
df_final.filter((F.col("M.id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==4)).agg(
    F.sum(F.col("M.valor_transaccion"))
).show() 
# 2 -92.358.158.029 
# 3 - 82.958.140.631 
# 4 -  124.133.903.210 
 
#ya vengo
df_1.groupBy("M.numero_siniestro").agg(F.count("*").alias("cantidad")).filter(F.col("cantidad") == 1).select("*").count()
df_3 = df_1.join(df_2, "numero_siniestro", "inner")
df_3.select("numero_siniestro").show()
_df_1 = _dfs.filter((F.col("M.id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==1)).select("M.numero_siniestro")
_dfs.filter(F.col("M.numero_siniestro")=='20123591').select("M.fecha_ocurrencia").show()
df_data.write.mode("overwrite").csv("s3://alfa-datalake-dev-assets/result_ola1/")
_dfs.filter((F.col("M.id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==2)).agg(
    F.sum(F.col("M.valor_transaccion"))
).show() 
dfs.filter((F.col("numero_siniestro") == 20145619) & ).count()
dfs.filter((F.col("numero_siniestro") == 20145619) & (F.year(F.col("fecha_transaccion")) == 2017) & (F.month(F.col("fecha_transaccion")) == 1)).count()
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
    "fecha_ocurrencia",
    "cod_causa",
    "causa",
    "cod_origen",
    "fecha_aviso",
    "poliza",
    "id_fuente_fk",
    "compania",
    "id_producto_fk",
    "id_ramo_fk",
    "unidad_de_negocio",
    "poliza_lider",
    "ciudad_lugar_ocurrencia",
    "depto_lugar_ocurrencia",
    "fecha_inicio_vigencia",
    "cedula",
    "fecha_nacimiento",
    "genero"
]
new_columns = []
cont = {}

for col in df.columns:
    if col not in cont:
        cont[col] = 0
        new_columns.append(col)
    else:
        cont[col] += 1
        new_columns.append(f"{col}_{cont[col]}")


df = df.toDF(*new_columns)
df.printSchema()
df = df.select(columns)
df.filter((F.col("numero_siniestro") == 20145619) & (F.year(F.col("fecha_transaccion")) == 2017) & (F.month(F.col("fecha_transaccion")) == 1)).count()
df = dfs.drop("origen_ant","origen_new")
df.filter(
    (F.col("numero_siniestro") == 20145619) & (F.col("M.tipo") == 'Pago') & (F.year(F.col("fecha_transaccion")) = 2017) & (F.month(F.col("fecha_transaccion")) = 1)
).count()
dfs.filter((F.col("numero_siniestro") == 20101857) & (F.col("M.tipo") == 'Pago')).count()
df_test = df_test.select("PS.cod_concepto_pago_siniestro", "PS.cod_movimiento", "SIN.cod_origen", "SIN.cod_tipo_identificacion", "CP.descripcion", "PN.fec_nacimiento", "fec_siniestro", "genero", "nid_numero_identificacion", "nid_pago", "nid_poliza", "nid_siniestro", "periodo_point")
df_test.write.mode("overwrite").csv("s3://alfa-datalake-dev-assets/result_ola1/")
df.filter((F.col("numero_siniestro") == 20101857) & (F.col("M.tipo") == 'Pago')).select("cod_origen").show()
dfs.count()
job.commit()