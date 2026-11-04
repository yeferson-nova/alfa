

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
df = spark.read.parquet("s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-DIM_CATEGORIA/2025/7/17/") 
df.show()
from pyspark.sql import Row


ruta_s3 = "s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-DIM_CATEGORIA/2025/7/17"


df = spark.read.parquet(ruta_s3)


nuevo_registro = spark.createDataFrame([Row(id_categoria_pk=999, categoria="No codigo")])


df_actualizado = df.unionByName(nuevo_registro)



df_actualizado.show()
from awsglue import DynamicFrame
from datetime import datetime
from pyspark.sql import functions as F
import pandas as pd
import boto3
from botocore.exceptions import ClientError
current_date = datetime.now()
day = current_date.day
month = current_date.month
year = current_date.year 

ruta_s3 = "s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-DIM_CATEGORIA/"
path = ruta_s3
df_dynamic = DynamicFrame.fromDF(df_actualizado , glueContext, "nested")
response = glueContext.write_dynamic_frame.from_options(
    frame=df_dynamic, 
    connection_type="s3", 
    format="glueparquet", 
    connection_options={"path": f"{path}{year}/{month}/{day}", "partitionKeys": []},
    format_options={"compression": "snappy"}, 
    transformation_ctx="AmazonS3_write"
)
df_actualizado.show()
df_actualizado.write.mode("overwrite").parquet(ruta_s3)
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
    if row["Phase"].upper() == "SILVER":
        path_source =f"s3://alfa-datalake-dev-silver/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}_SILVER_"
    else:
        path_source =f"s3://alfa-datalake-dev-gold/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-CARACT_SINIESTROS-"
    IncrementalCharge.charge_tmp_tables(path_source, table, 'Gold')
    
print("### Tablas StepGold cargadas correctamente")
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField,LongType
from pyspark.sql import functions as F,Window



class StepGoldBodega:
    def __init__(self, glue_context, spark):
        self.spark = spark

    def create_component(self):
        """
        This Function to create gold_layer Table.
        """

        B_NID_SINIESTRO = 'B.NID_SINIESTRO'
        PCL_NUM_IDEN  = "PCL.NUM_IDENTIFICACION"


        PPR_AFILIADO  ="global_temp.ppr_afiliados"
        DF_BODEGA = self.spark.sql("select * from global_temp.bodega_plus")
        DF_DATO_BASICO = self.spark.sql("select * from global_temp.snr_dato_basico")
        DF_CAUSAS = self.spark.sql("select * from global_temp.snr_causa")
        DF_ORIGEN = self.spark.sql("select * from global_temp.snr_origen")
        DF_POLIZAS = self.spark.sql("select * from global_temp.rcd_polizas")
        DF_CU_PERSONAS = self.spark.sql("select * from global_temp.CU_PERSONA_CLIENTE")
        DF_VW_CONSULTA = self.spark.sql("select * from global_temp.VW_CONSULTA_CLIENTE")
        DF_VW_LOCALIZACIONES = self.spark.sql("select * from global_temp.VW_LOCALIZACIONES")
        SDT = self.spark.table("global_temp.snr_dato_tramite")
        PA = self.spark.table(PPR_AFILIADO).alias("PA")
        
        DF_VW_CONSULTA = DF_VW_CONSULTA.withColumn(
            "valor_max", F.greatest(F.col("COD_CIUDAD"), F.col("COD_DEPARTAMENTO"))
        )

        window_cedula = Window.partitionBy("NID_NUMERO_IDENTIFICACION").orderBy(F.col("valor_max").desc())
        DF_VW_CONSULTA = DF_VW_CONSULTA.withColumn("rank", F.row_number().over(window_cedula))
        DF_VW_CONSULTA = DF_VW_CONSULTA.filter(F.col("rank") == 1).drop("valor_max", "rank")

        window = Window.partitionBy("ID_SINIESTRO").orderBy(F.col("fec_act_solicitud_afp").asc())
        SDT = SDT.withColumn("rank", F.row_number().over(window))
        SDT = SDT.filter(F.col("rank") == 1).drop("rank")

       
        DF_RESULT = DF_BODEGA.alias('B').join(
            DF_DATO_BASICO.alias('D'), F.col(B_NID_SINIESTRO) == F.col('D.ID_SINIESTRO'), 'left'
            ).join(
            DF_CAUSAS.alias('c'), F.col('D.COD_CAUSA_SINIESTRO') == F.col('c.ID_CAUSAL'), 'left'
            ).join(
            DF_ORIGEN.alias('o'), F.col('D.COD_ORIGEN_SINIESTRO') == F.col('o.COD_ORIGEN_SINIESTRO'), 'left'
            ).join(
            DF_POLIZAS.alias('p'),  F.coalesce(F.col('B.fecha_siniestro'), F.to_date(F.concat_ws('', F.col('B.periodo_point'), F.lit('01')), 'yyyyMMdd')).between(F.col('p.FEC_INICIAL_VIGENCIA'),F.col('p.FEC_FINAL_VIGENCIA'))
            ).join(
            DF_CU_PERSONAS.alias('pcl'), F.col("pcl.NUM_PERSONA") == F.col("D.NUM_PERSONA"),"left"
            ).join(
            DF_VW_CONSULTA.alias('af'), (F.col("pcl.NUM_IDENTIFICACION") == F.col("af.NID_NUMERO_IDENTIFICACION")) & (F.col("pcl.cod_tipo_identificacion") == F.col("af.cod_tipo_identificacion")),"left"
            ).join(
            DF_VW_LOCALIZACIONES.alias('af2'), (F.col("PCL.NUM_IDENTIFICACION") == F.col("af2.NID_NUMERO_IDENTIFICACION")) & (F.col("PCL.cod_tipo_identificacion") == F.col("af2.cod_tipo_identificacion")),"left"
            ).join(
            SDT.alias("SDT"), F.col("D.ID_SINIESTRO") == F.col("SDT.ID_SINIESTRO"),"left"
            ).join(
            PA.alias("PA"), (F.col(PCL_NUM_IDEN) == F.col("PA.nid_identificacion")) & (F.col("PCL.cod_tipo_identificacion") == F.col("PA.cod_tipo_identificacion_afp")), "left") 
            
        df_facturas = DF_RESULT.filter(
            (F.col(B_NID_SINIESTRO).cast(LongType()) >= 20200000000) & (F.col("b.tipo_movimiento").like("Recuperaci%"))
        ).selectExpr(
            "'3' as id_categoria_fk",
            "b.concepto_pago AS categoria",
            "'Facturas' AS tipo",
            "CASE b.cod_movimiento  WHEN 'Pago' THEN 7 ELSE 7 END AS id_tipo_transaccion_fk",
            "CASE b.cod_movimiento  WHEN 'Pago' THEN 'Recobros' ELSE 'Recobros' END AS tipo_transaccion",
            "b.NID_SINIESTRO AS numero_siniestro",
            "(cast(b.VLR_pago as double)) AS valor_transaccion",
            "b.VLR_pago AS valor_transaccion_ajustado",
            "cast(to_date(b.periodo_point || '01' ,'yyyyMMdd') as date)  AS fecha_transaccion",    
            "COALESCE(cast(d.FECHA_SINIESTRO as date), cast(to_date(b.periodo_point || '01' ,'yyyyMMdd') as date))  AS fecha_ocurrencia",
            "c.ID_CAUSAL as cod_causa",    
            "COALESCE(c.NOMBRE, 'Otras causas') AS causa",
            "o.cod_origen_siniestro as cod_origen", 
            "COALESCE(cast(SDT.FEC_ACT_SOLICITUD_AFP as date) ,cast(SDT.FEC_SOLICITUD_AFP as date)) AS fecha_aviso",
            "COALESCE(p.NID_POLIZA ,999) AS poliza",
            "'3' as id_fuente_fk",
            "'50' AS compania",
            "41 AS id_producto_fk",
            "31 AS id_ramo_fk",
            "'Seguridad social' AS unidad_de_negocio",   
            "CONCAT('PRV-',LPAD(COALESCE(CAST(p.NID_POLIZA AS VARCHAR(10)),'999'),7,'0'),'-00') as poliza_lider",
            "CONCAT(CASE WHEN AF.COD_DEPARTAMENTO IS NULL OR AF.COD_DEPARTAMENTO = '0' THEN '01' ELSE AF.COD_DEPARTAMENTO END ,CASE WHEN AF.COD_CIUDAD IS NULL OR AF.COD_CIUDAD = '0' THEN '01001' ELSE AF.COD_CIUDAD END ) as ciudad_lugar_ocurrencia",
            "CASE WHEN AF.COD_DEPARTAMENTO IS NULL OR AF.COD_DEPARTAMENTO = '0' THEN '01' ELSE AF.COD_DEPARTAMENTO END AS depto_lugar_ocurrencia",
            "AF2.FEC_INGRESO_AFP AS fecha_inicio_vigencia",
            "COALESCE(PCL.NUM_IDENTIFICACION,AF.NID_NUMERO_IDENTIFICACION,'No registra') AS cedula",
            "AF2.FEC_NACIMIENTO  AS fecha_nacimiento",      
            "COALESCE(AF2.GENERO,'No registra')  AS genero"



        )

    
        df_pagos = DF_RESULT.filter(
            (F.col(B_NID_SINIESTRO)== "99999999")  | (F.col(B_NID_SINIESTRO)== 99999999)
        ).selectExpr(
            "'2'  AS id_categoria_fk",
            "b.concepto_pago    AS categoria",
            "'Recduperacion' AS tipo",
            "CASE b.cod_movimiento  WHEN 'Pago' THEN  5   ELSE 5 END AS id_tipo_transaccion_fk",
            "CASE b.cod_movimiento  WHEN 'Pago' THEN 'Pago'  ELSE 'Pago'  END AS tipo_transaccion",    
            "b.NID_SINIESTRO AS numero_siniestro",
            "(cast(b.VLR_pago as double)) AS valor_transaccion",
            "b.VLR_pago AS valor_transaccion_ajustado",
            "cast(to_date(b.periodo_point || '01' ,'yyyyMMdd') as date)  AS fecha_transaccion",
            "cast(to_date(b.periodo_point || '01' ,'yyyyMMdd') as date)  AS fecha_ocurrencia",    
            "'384'	as cod_causa",
            "COALESCE(c.NOMBRE,'Otras causas' ) AS causa",
            "'742' as cod_origen",    
            "cast(to_date(b.periodo_point || '01' ,'yyyyMMdd') as date)   AS fecha_aviso",
            "COALESCE(p.NID_POLIZA ,999) AS poliza",
            "'3' as id_fuente_fk",
            "'50' AS compania",
            "41 AS id_producto_fk",    
            "31 AS id_ramo_fk",
            "'Seguridad social' AS unidad_de_negocio",
            "CONCAT('PRV-',LPAD(COALESCE(CAST(p.NID_POLIZA AS VARCHAR(10)),'999'),7,'0'),'-00') as poliza_lider",
            "CONCAT('01','01001') AS ciudad_lugar_ocurrencia",
            "'01' AS depto_lugar_ocurrencia",
            "NULL AS fecha_inicio_vigencia",
            "'No registra' AS cedula",
            "NULL  AS fecha_nacimiento",      
            "'No registra' AS genero"



        
        )

        df_final = df_facturas.union(df_pagos)
        df_final = df_final.withColumn(
            "valor_transaccion",
            F.when(F.col("id_tipo_transaccion_fk").isin('2','4','7','6'), F.col("valor_transaccion") *-1)
            .otherwise(F.col("valor_transaccion"))
        )
        return df_final
StepGoldBodega = StepGoldBodega(glueContext, spark)
df_plus_v = StepGoldBodega.create_component()
df_plus_v.count()
IncrementalCharge.charge_tmp_tables("s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_MOVIMIENTO/2025/8/11", "", 'Gold')
DR = spark.table("global_temp.GOLD-CARACT_SINIESTROS-FACT_MOVIMIENTO")
df.filter(F.col("id_siniestro_fk") =="20242303722").show()
StepGoldBodega = StepGoldPlus(glueContext, spark)
df_plus_v = StepGoldBodega.create_component()
df_cedula = df_plus_v.select("cedula","numero_siniestro")
df_cedula.filter(F.col("numero_siniestro") =="20242303722").show()
df_cedula.filter(F.col("cedula") == "No registra").count()
df_sebas = df_plus_v.select("fecha_inicio_vigencia")
df_sebas.count()
df_sebas.filter(F.col("fecha_inicio_vigencia").isNull()).count()--146074 ---182594
df = df_bodega.filter(F.col("numero_siniestro") == '20242304120')
df.select("cedula").show()
df_bodega.write.format("csv").mode("overwrite").save("s3://alfa-datalake-dev-assets/5/19/")
df_bodega.filter(F.col("id_tipo_transaccion_fk").like("antoni%")).select("id_tipo_transaccion_fk").show(truncate=False)

df_bodega.count()
df_bodega.filter((F.col("numero_siniestro") ==201705325) & (F.col("id_tipo_transaccion_fk") =='1') ).show()
from pyspark.sql.functions import col, year, month, round, sum as _sum, format_number


df_filtrado = df_bodega.filter(
    (col("id_fuente_fk") == '2') &
    (year(col("fecha_transaccion")) == 2022) &
    (col("id_tipo_transaccion_fk") == '5')
)


df_resultado = df_filtrado.groupBy(
    year(col("fecha_transaccion")).alias("mes")
).agg(
    format_number(round(_sum("valor_transaccion"), 2), 2).alias("suma_valor_formateado")
)

df_resultado.orderBy("mes").show(truncate=False)


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
                F.lit("1").alias("periodo_point"),
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
                (F.col("PS.vlr_pago")).alias("valor_transaccion"),
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
        )
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
            (F.col("DT.vlr_concepto")).alias("valor_transaccion"),
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
          
        )
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
            (F.col("DR.vlr_reserva")).alias("valor_transaccion"),
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
        )
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
        
        df_final = df_final.withColumn(
            "valor_transaccion",
            F.when(F.col("id_tipo_transaccion_fk").isin('2','4','5','8'), F.col("valor_transaccion") *-1)
            .otherwise(F.col("valor_transaccion"))
        )
        return df_final
import sys
import traceback
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from datetime import datetime, timedelta
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from commons.TransformationHelper import TransformationHelper
from commons.PolizaHelper import PolizaHelper
from commons.UrincterrdeudoresHelper import UrincterrdeudoresHelper
from commons.ClaimHelper import ClaimHelper
from commons.EstadoSiniestroHelperA import EstadoSiniestroHelperA
from commons.IncrementalCharge import IncrementalCharge
from commons.DocumentoHelper import DocumentoHelper
from pyspark.sql import functions as F
from commons.StepGold import StepGold
from commons.StepGoldPlus import StepGoldPlus
from commons.StepGoldBodega import StepGoldBodega
from commons.GoldTransformation import GoldTransformation
from commons.FechaObjHelper import FechaObjHelper
from commons.StepGold import StepGold
from commons.StepGoldPlus import StepGoldPlus
from commons.StepGoldBodega import StepGoldBodega
from commons.SiprenDocumentoHelper import SiprenDocumentoHelper
import re
from pyspark.sql.window import Window
import boto3
import traceback



spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")

BUCKET_NAME_SILVER = f"alfa-datalake-dev-silver"
BUCKET_NAME_GOLD = f"alfa-datalake-dev-gold"


ExtractHelper = ExtractHelper(glueContext, spark, "-", "-")
IncrementalCharge = IncrementalCharge(glueContext, spark, "-", "-")
PolizaHelper = PolizaHelper(glueContext, spark)
Urincterrdeudores_helper = UrincterrdeudoresHelper(glueContext, spark)
TransformationHelper = TransformationHelper()
ClaimHelper = ClaimHelper(glueContext, spark)
GoldTransformation = GoldTransformation(glueContext, spark, 'None')
DocumentoHelper = DocumentoHelper(glueContext, spark)
FechaObj_helper = FechaObjHelper(glueContext, spark) 
EstadoSiniestro_Helper = EstadoSiniestroHelperA(glueContext, spark)
StepGold = StepGold(glueContext, spark)
StepGoldPlus = StepGoldPlus(glueContext, spark)
StepGoldBodega = StepGoldBodega(glueContext, spark)
SiprenDocumentoHelper = SiprenDocumentoHelper(glueContext, spark)

path = f"s3://alfa-datalake-dev-assets/support_phase_files/procces_ola1.xlsx"
sheet = "gold_phase"
df_tables = ExtractHelper.extract_excel_data(path, sheet)
sheet = "gold_tables"
df_tmp_tables = ExtractHelper.extract_excel_data(path, sheet)

def obtain_last_date(fecha = None):
    """
    This Function help us to obtein the last cut date.
    """
    if fecha is None:
        fecha = datetime.now()

    primer_dia_mes_actual = datetime(fecha.year, fecha.month, 1)

    ultimo_dia_mes_anterior = primer_dia_mes_actual - timedelta(days=1)

    print(f"{ultimo_dia_mes_anterior.year}-{ultimo_dia_mes_anterior.month}-1", f"{ultimo_dia_mes_anterior.year}-{ultimo_dia_mes_anterior.month}-{ultimo_dia_mes_anterior.day}")
    return f"{ultimo_dia_mes_anterior.year}-{ultimo_dia_mes_anterior.month}-1", f"{ultimo_dia_mes_anterior.year}-{ultimo_dia_mes_anterior.month}-{ultimo_dia_mes_anterior.day}"


try:

    for row in df_tmp_tables.collect():
        table = row["Table"].upper()
        if row["Phase"].upper() == "SILVER":
            path_source =f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}_SILVER_"
        else:
            path_source =f"s3://{BUCKET_NAME_GOLD}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-CARACT_SINIESTROS-"
        IncrementalCharge.charge_tmp_tables(path_source, table, 'Gold')
    execute_dim = 0
    if execute_dim == 1:
        for row in df_tables.collect():
            if row['Status'] == '1':
                sql = row['Logic']
                df = spark.sql(sql)
                
                schema_str = row['ESQUEMA']
                
                table = row['Table'].upper()
                schema = TransformationHelper.parse_schema(schema_str)
                
                for field in schema.fields:
                    field_name = field.name
                    field_type = field.dataType
                    
                    df = df.withColumn(field_name, df[field_name].cast(field_type))
                
                df = spark.createDataFrame(df.rdd, schema)
                df= df.distinct()
                df_incremental = None
                try:
                    df_incremental = spark.sql(f"SELECT * FROM global_temp.{table}")
                except Exception as e:
                    print(e)
                if df_incremental is not None:
                    df = df.join(df_incremental, on=row['PK'], how='left_anti')
                path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-{table}/"
                ExtractHelper.save_files(df,path)
            if row['Status'] == '2':
                if row['Table'] == 'DIM_CIUDAD':
                    df = orchestrator_gold_dane(args, spark, glueContext, "ACSELE_SILVER_MUNICIPIO")
                    path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-DIM_CIUDAD/"
                    ExtractHelper.save_files(df,path)
                if row['Table'] == 'DIM_DEPARTAMENTO':   
                    df = orchestrator_gold_dane(args, spark, glueContext, "DEPARTMENT")
                    path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-DIM_DEPARTAMENTO/"
                    ExtractHelper.save_files(df,path)
    execute_fact = 1
    if execute_fact == 1:


        df_tierra = StepGold.create_component()
        df_plus = StepGoldPlus.create_component()
        df_bodega = StepGoldBodega.create_component()
        df_main = df_tierra.unionAll(df_plus).unionAll(df_bodega)
        



        
 
        

    
        df_movimientos_plus = df_main.select(
            F.col("numero_siniestro").alias("id_siniestro_fk"),
            F.col("id_tipo_transaccion_fk"),
            F.col("id_categoria_fk"),
            F.col("fecha_transaccion").cast("date"),
            F.lit(None).cast("date").alias("FECHA_OBJECION"),
            F.col("valor_transaccion_ajustado").cast("double"),
            F.col("valor_transaccion").cast("double"),
            F.col("id_fuente_fk"),
            F.col("cod_origen").alias("id_cobertura_fk"),#Mover a movimientos
            F.col("fecha_ocurrencia").cast("date").alias("fecha_ocurrencia"),#Mover a movimientos
            F.col("poliza").cast("string").alias("id_poliza_fk"),#Mover a movimientos
            F.col("depto_lugar_ocurrencia").cast("string").alias("id_departamento_ocurrencia_fk"),
            F.col("ciudad_lugar_ocurrencia").cast("string").alias("id_ciudad_ocurrencia_fk")
        )
        df_movimientos = df_movimientos_plus

        fecha_corte_1, fecha_corte_2 = obtain_last_date()
        #df_movimientos = df_movimientos.filter(F.to_date((F.col("fecha_transaccion")),"YYYY-mm-dd") >= fecha_corte_1)
        #df_movimientos = df_movimientos.filter(F.to_date((F.col("fecha_transaccion")),"YYYY-mm-dd") <= fecha_corte_2)
        
        path = f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_MOVIMIENTO-TEST/"
        ExtractHelper.save_files(df_movimientos,path)
        
        
except Exception as e:
    print(traceback.format_exc())
    raise e    
df_bodega.count()
from awsglue import DynamicFrame
from datetime import datetime
from pyspark.sql import functions as F
import pandas as pd
import boto3
from botocore.exceptions import ClientError
df_dynamic = DynamicFrame.fromDF(df_bodega , glueContext, "nested")
response = glueContext.write_dynamic_frame.from_options(
    frame=df_dynamic, 
    connection_type="s3", 
    format="glueparquet", 
    connection_options={"path": f"{path}{year}/{month}/{day}", "partitionKeys": []},
    format_options={"compression": "snappy"}, 
    transformation_ctx="AmazonS3_write"
)
current_date = datetime.now()
day = current_date.day
month = current_date.month
year = current_date.year 


path = f"s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-RESERVAS-MOV/"
df_dynamic = DynamicFrame.fromDF(df_bodega , glueContext, "nested")
response = glueContext.write_dynamic_frame.from_options(
    frame=df_dynamic, 
    connection_type="s3", 
    format="glueparquet", 
    connection_options={"path": f"{path}{year}/{month}/{day}", "partitionKeys": []},
    format_options={"compression": "snappy"}, 
    transformation_ctx="AmazonS3_write"
)

path = f"s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-RESERVAS-MOV/"
save_files(df_bodega,path)
SDT = spark.table("global_temp.snr_dato_tramite")
SDT = SDT.groupBy("ID_SINIESTRO").agg( 
    F.min("fec_act_solicitud_afp").alias("fecha_solicitud_afp")
)
SDT.printSchema()
df_bodega = StepGoldBodega.create_component()
df_bodega.count()
df_bodega.filter(F.col("numero_siniestro") == '20232302314').select("fecha_transaccion","fecha_ocurrencia","fecha_aviso").show()
df_bodega.count()
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from pyspark.sql import functions as F, Window


class StepGold:
    def __init__(self, glue_context, spark):
        self.spark = spark
   
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
            .join(historico_mas_cercano.alias("HMC"), 
                  (F.col("HMC.nid_siniestro") == F.col("SIN.nid_siniestro")) &
                  (F.col("HMC.periodo_point") == F.col("FOC.periodo_point")) &
                  (F.col("HMC.rn") == 1), "left")
            .join(hist_fecha_maxima.alias("T1"), 
                  F.col("T1.nid_siniestro_origen_hist") == F.col("HMC.nid_siniestro_origen_hist"), "left")
            .join(H2.alias("SH"), 
                  (F.col("FOC.nid_siniestro") == F.col("SH.nid_siniestro")) &
                  (F.col("SH.cod_origen_siniestro_ant") == F.col("HMC.cod_origen_siniestro_ant")) &
                  (F.col("HMC.periodo_point") == F.col("FOC.periodo_point")) &
                  (F.col("HMC.rn") == 1), "left")
            .join(max_origen_hist.alias("MOH"), 
                  (F.col("MOH.nid_siniestro") == F.col("SIN.nid_siniestro")) &
                  (F.col("MOH.rn") == 1), "left")
            .join(CO3_P, 
                  F.col("SH.cod_origen_siniestro_new") == F.col("CO3.cod_origen"), "left")
            .join(CO4_P, 
                  F.col("SH.cod_origen_siniestro_ant") == F.col("CO4.cod_origen"), "left")
            .join(P.alias("P"), 
                  (F.col("FOC.fecha_ocurrencia_new") >= F.col("P.fec_inicial_vigencia")) &
                  (F.col("FOC.fecha_ocurrencia_new") <= F.col("P.fec_final_vigencia")), "left")
            .distinct()
        )

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
            .join(historico_mas_cercano_recup.alias("HMC"), 
                  (F.col("HMC.nid_siniestro") == F.col("SIN.nid_siniestro")) &
                  (F.col("HMC.periodo_point") == F.col("FOC.periodo_point")) &
                  (F.col("HMC.rn") == 1), "left")
            .join(hist_fecha_maxima_recup.alias("T1"), 
                  F.col("T1.nid_siniestro_origen_hist") == F.col("HMC.nid_siniestro_origen_hist"), "left")
            .join(H2_R.alias("SH"), 
                  (F.col("RP.nid_siniestro") == F.col("SH.nid_siniestro")) &
                  (F.col("SH.cod_origen_siniestro_ant") == F.col("HMC.cod_origen_siniestro_ant")) &
                  (F.col("HMC.periodo_point") == F.col("FOC.periodo_point")) &
                  (F.col("HMC.rn") == 1), "left")
            .join(max_origen_hist_recup.alias("MOH"), 
                  (F.col("MOH.nid_siniestro") == F.col("SIN.nid_siniestro")) &
                  (F.col("MOH.rn") == 1), "left")
            .join(hist_fecha_maxima_recup.alias("T2"), 
                  F.col("T2.nid_siniestro_origen_hist") == F.col("SH.nid_siniestro_origen_hist"), "left")
            .join(CO3_R, F.col("SH.cod_origen_siniestro_new") == F.col("CO3.cod_origen"), "left")
            .join(CO4_R, F.col("SH.cod_origen_siniestro_ant") == F.col("CO4.cod_origen"), "left")
            .join(P_R.alias("P"), 
                  (F.col("FOC.fecha_ocurrencia_new") >= F.col("P.fec_inicial_vigencia")) &
                  (F.col("FOC.fecha_ocurrencia_new") <= F.col("P.fec_final_vigencia")), "left")
            .join(P2_R, 
                  (F.col("ISA_FIL_MAIN.isa_fec_solicitud_recup") >= F.col("P2.fec_inicial_vigencia")) &
                  (F.col("ISA_FIL_MAIN.isa_fec_solicitud_recup") <= F.col("P2.fec_final_vigencia")), "left")
            .distinct()
        )

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
            .join(historico_mas_cercano_reservas.alias("HMC"), 
                  (F.col("HMC.nid_siniestro") == F.col("SIN.nid_siniestro")) &
                  (F.col("HMC.periodo_point") == F.col("FOC.periodo_point")) &
                  (F.col("HMC.rn") == 1), "left")
            .join(hist_fecha_maxima_reservas.alias("T1"), 
                  F.col("T1.nid_siniestro_origen_hist") == F.col("HMC.nid_siniestro_origen_hist"), "left")
            .join(H2_S.alias("SH"), 
                  (F.col("DR.nid_siniestro") == F.col("SH.nid_siniestro")) &
                  (F.col("SH.cod_origen_siniestro_ant") == F.col("HMC.cod_origen_siniestro_ant")) &
                  (F.col("HMC.periodo_point") == F.col("FOC.periodo_point")) &
                  (F.col("HMC.rn") == 1), "left")
            .join(max_origen_hist_reservas.alias("MOH"), 
                  (F.col("MOH.nid_siniestro") == F.col("SIN.nid_siniestro")) &
                  (F.col("MOH.rn") == 1), "left")
            .join(CO3_S, F.col("SH.cod_origen_siniestro_new") == F.col("CO3.cod_origen"), "left")
            .join(CO4_S, F.col("SH.cod_origen_siniestro_ant") == F.col("CO4.cod_origen"), "left")
            .join(P_S.alias("P"), 
                  (F.col("FOC.fecha_ocurrencia_new") >= F.col("P.fec_inicial_vigencia")) &
                  (F.col("FOC.fecha_ocurrencia_new") <= F.col("P.fec_final_vigencia")), "left")
            .distinct()
        )

        # =====================================================
        # SELECCIONES FINALES CON CAMPOS EN ORDEN ESPECÍFICO
        # =====================================================
        
        # PAGOS con campos en orden específico
        pagos_final = df_pago.select(
                F.col("CP.cod_concepto_pago_siniestro").cast("string").alias("id_categoria_fk"),
                F.col("CP.descripcion").alias("categoria"),
                F.lit("Pago").alias("tipo"),
                F.col("PS.cod_movimiento").cast("string").alias("id_tipo_transaccion_fk"),
                F.col("TM.descripcion").alias("tipo_transaccion"),               
                F.col("FOC.nid_siniestro").cast("string").alias("numero_siniestro"),                
                F.abs(F.col("PS.vlr_pago")).alias("valor_transaccion"),
                F.col("PS.vlr_pago").alias("valor_transaccion_ajustado"),   
                F.col("PG.fec_generado").alias("fecha_transaccion"),
                F.col("FOC.fecha_ocurrencia_new").alias("fecha_ocurrencia"),
                F.coalesce(F.col(CS_COD_CAUSA_SINIESTRO), F.lit("384")).alias("cod_causa"),
                F.coalesce(F.col(CS_DESCRIPCION), F.lit(DESC_CAUSA)).alias("causa"),
                # Cod_origen (ya calculado en CTE)
                F.col("FOC.cod_origen_calculado").alias("cod_origen"),
               
                # Lógica de origen (equivalente al CASE del SQL)
                F.when(
                    F.col("PG.periodo_point").cast("int") >=
                    ((F.year(F.col("SH.fec_modificacion")) * 100) + F.month(F.col("SH.fec_modificacion"))),
                    F.col("CO3.nombre")
                ).when(
                    F.col("PG.periodo_point").cast("int") <
                    ((F.year(F.col("SH.fec_modificacion")) * 100) + F.month(F.col("SH.fec_modificacion"))),
                    F.col("CO4.nombre")
                ).otherwise(
                    F.col("CO.nombre")
                ).alias("origen"),                
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
        )

        # RECUPERACIONES con campos en orden específico
        recuperaciones_final = df_recup.select(
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
            F.col("FOC.cod_origen_calculado").alias("cod_origen"),
            
            # Lógica de origen (MISMA QUE PAGOS)
            F.when(
                F.col("RP.periodo_point").cast("int") >= 
                ((F.year(F.col("SH.fec_modificacion")) * 100) + F.month(F.col("SH.fec_modificacion"))),
                F.col("CO3.nombre")
            ).when(
                F.col("RP.periodo_point").cast("int") < 
                ((F.year(F.col("SH.fec_modificacion")) * 100) + F.month(F.col("SH.fec_modificacion"))),
                F.col("CO4.nombre")
            ).otherwise(
                F.col("CO.nombre")
            ).alias("origen"),
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
            F.col("PN.genero").alias("genero")
        )

        # RESERVAS con campos en orden específico
        reservas_final = df_reserva.select(
                
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
            F.col("FOC.cod_origen_calculado").alias("cod_origen"),
            
            # Lógica de origen (MISMA QUE PAGOS)
            F.when(
                F.col("SR.periodo_point").cast("int") >= 
                ((F.year(F.col("SH.fec_modificacion")) * 100) + F.month(F.col("SH.fec_modificacion"))),
                F.col("CO3.nombre")
            ).when(
                F.col("SR.periodo_point").cast("int") < 
                ((F.year(F.col("SH.fec_modificacion")) * 100) + F.month(F.col("SH.fec_modificacion"))),
                F.col("CO4.nombre")
            ).otherwise(
                F.col("CO.nombre")
            ).alias("origen"),
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
            F.col("PN.genero").alias("genero")     
        )

        # =====================================================
        # UNION FINAL
        # =====================================================
        df_final = pagos_final.union(recuperaciones_final).union(reservas_final)
        print("=" * 20)
        print(f"Total Payments: {pagos_final.count()}")
        print(f"Total Recoveries: {recuperaciones_final.count()}")
        print(f"Total Reserves: {reservas_final.count()}")
        print(f"Total Final: {df_final.count()}")
        print("=" * 20)
        return df_final
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
        pagos_final = pagos_final.drop("nid_pago","periodo_point")
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
        recuperaciones_final = recuperaciones_final.drop("NID_RECUPERACION","periodo_point")
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
        reservas_final = reservas_final.drop("NID_RESERVA","periodo_point")
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
            "genero"
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
        

        return df_final
step_gold = StepGold(glueContext, spark)
df_pagos = step_gold.create_component()
df_pagos.filter((F.col("id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==1)).select("numero_siniestro").count()
df_pagos.filter((F.col("numero_siniestro") ==20123445) & (F.col("id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==1)).show()
from pyspark.sql.functions import col, year, month, round, sum as _sum, format_number


df_filtrado = df_pagos.filter(
    (col("id_fuente_fk") == '2') &
    (year(col("fecha_transaccion")) == 2022) &
    (col("id_tipo_transaccion_fk") == '5')
)


df_resultado = df_filtrado.groupBy(
    month(col("fecha_transaccion")).alias("mes")
).agg(
    format_number(round(_sum("valor_transaccion"), 2), 2).alias("suma_valor_formateado")
)

df_resultado.orderBy("mes").show(truncate=False)


df_pagos.count()
from pyspark.sql.functions import col, year, month, round, sum as _sum, format_number


df_filtrado = df_pagos.filter(
    (year(col("fecha_transaccion")) == 2022) &
    (month(col("fecha_transaccion")) == 6) &
    (col("id_tipo_transaccion_fk") == '5') &
    (col("id_fuente_fk") == '2')
)


df_resultado = df_filtrado.agg(
    format_number(round(_sum("valor_transaccion_ajustado"), 2), 2).alias("valor_total_formateado")
)

df_resultado.show()

df_pagos.filter((F.col("numero_siniestro") ==202002563) & (F.col("id_tipo_transaccion_fk") =='5') &(F.year(F.col("fecha_transaccion"))==2022) &(F.month(F.col("fecha_transaccion"))==6)).show()
df.select("valor_transaccion").show()
df_pagos.count()
df = df_pagos.filter((F.col("numero_siniestro") == 20113345) & (F.col("tipo") == "Pago") & (F.year("fecha_transaccion") == 2017))
df.select("fecha_ocurrencia","fecha_transaccion","fecha_inicio_vigencia","poliza","valor_transaccion").show()
df = step_gold.create_component()
df = step_gold.create_component()
job.commit()