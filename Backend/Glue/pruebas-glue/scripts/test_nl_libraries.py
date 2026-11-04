
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
path = f"s3://alfa-datalake-dev-assets/FS_files/DATA_NIFF_PARAMS/07_PruebaEscritorio_ParametrosActuariales_TablasMortalidadNiif17_V1.0.xlsx"
sheet = "TasaMortNIIF17_SRC_3"
df2 = ExtractHelper.extract_excel_data(path, sheet)
df2 = df2.withColumn(
    "fecha_carga",
    F.lit("2025-02-01 12:00:00")
)
df = df.unionByName(df2)
df.write.mode("overwrite").parquet("s3://alfa-datalake-dev-bronce/parametros/PRM7_3_v1/")
df.printSchema()
df = df.withColumnRenamed("FEC_INI_VIG","FECHA_INICIO")
df.filter(F.col("T")=="120").show()
df_1 = spark.read.parquet("s3://alfa-datalake-dev-bronce/parametros/parametros_tres_cuatro/")
df_1.printSchema()
path = f"s3://alfa-datalake-dev-bronce/NIFF_TEST/des_4.1"
ExtractHelper.save_files(df.coalesce(1),path)
df = spark.read.parquet("s3://alfa-datalake-dev-bronce/parametros/PRM6_1/")
df = df.withColumn(
    "fecha_carga",
    F.lit("2024-12-01 12:00:00")
)
table = "PRM8_2"
fecha = "2025-01-01"
df = spark.read.parquet(f"s3://alfa-datalake-dev-bronce/parametros/{table}/")
df = df.withColumn(
    "fecha_carga",
    F.lit(f"{fecha} 12:00:00")
)
df.write.mode("overwrite").parquet(f"s3://alfa-datalake-dev-bronce/parametros/{table}_v1/")
df.show(1)
df.write.mode("overwrite").parquet("s3://alfa-datalake-dev-bronce/parametros/PRM6_1_v1/")
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
import nltk
nltk.download('stopwords', quiet=True)
from nltk.corpus import stopwords
stop_words = set(stopwords.words('spanish'))
print(stop_words)
CR = spark.table("global_temp.rsv_comentario_reserva")
_CR = CR
def remove_stopwords(text):
    if text is None:
        return None
    #words = word_tokenize(text.lower())
    words = text.lower().split()
    filter_words = [p for p in words if p.isalpha() and p not in stop_words]
    return ' '.join(filter_words)
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
delete_udf = udf(remove_stopwords, StringType())
_CR = _CR.withColumn('comentario_reserva', delete_udf('comentario_reserva'))
_CR.select("ID_COMENTARIO_RESERVA","comentario_reserva").orderBy("ID_COMENTARIO_RESERVA").show(truncate=False)
from pyspark.sql import functions as F,Window
from pyspark.sql.types import StringType
from functools import reduce
from pyspark.sql.functions import udf
from pyspark.sql.functions import split, to_date, date_format, col, regexp_replace
from commons.TransformationHelper import TransformationHelper
import nltk
nltk.download('stopwords', quiet=True)
from nltk.corpus import stopwords

TransformationHelper = TransformationHelper()

class StepGoldPlus:
    def __init__(self, glue_context, spark):
        self.spark = spark

        
    def create_component(self):
        
        def buscar_en_texto(text):
            for palabra, valor in reglas_dict.items():
                if palabra.lower() in text.lower():
                    return valor
            return None
        
        def remove_stopwords(text):
            if text is None:
                return None
            words = text.lower().split()
            filter_words = [p for p in words if p.isalpha() and p not in STOP_WORDS]
            return ' '.join(filter_words)
        
        delete_udf = udf(remove_stopwords, StringType())
        buscar_udf = udf(buscar_en_texto, StringType())
        
        STOP_WORDS = set(stopwords.words('spanish'))
        
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
        CR = CR.withColumn('comentario_reserva', delete_udf('comentario_reserva'))

        DATA = [
            ('1','1'),
            ('2','3'),
            ('10','3'),
            ('13','3'),
            ('1','1'),
            ('4','3'),
            ('9','3'),
            ('6','3'),
            ('17','2'),
            ('11','2'),
            ('14','2'),
            ('21','2'),
            ('3','2'),
            ('12','2'),
            ('5','2'),
            ('8','4'),
            ('7','2'),
            ('22','4'),
            ('16','2')
            #('24',''), Son reversiones no se hacen
            #('23','')
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
        print(reglas_dict)
        

        
        df_reserva = df_reserva.withColumn('OBSERVACIONES', delete_udf('OBSERVACIONES'))
        #df_reserva = df_reserva.withColumn(
        #    "id_tipo_transaccion_fk",
        #     F.when(
        #        (F.col("id_tipo_transaccion_fk") == 2),
        #        F.lit(4)
        #    ).otherwise(F.col("id_tipo_transaccion_fk"))
        #)
        CR = CR.withColumn( 
            "comentario_reserva",
            F.concat(
                F.lower(F.col("comentario_reserva"))
            )
        )
        df_reserva = TransformationHelper.data_normalice("OBSERVACIONES",df_reserva)
        df_reserva = df_reserva.withColumn( 
            "OBSERVACIONES",
                F.lower(F.col("OBSERVACIONES"))
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
        print(df_reserva.select("OBSERVACIONES").filter(F.col("numero_siniestro")==20212300026).show(truncate = False))
        
        df_reserva = df_reserva.withColumn(
            "homog_id_mov",
            buscar_udf(F.col("OBSERVACIONES"))
        )
        #print(CR.select("comentario_reserva").show(truncate = False))
        print("print otro")
        print(df_reserva.select("OBSERVACIONES","homog_id_mov","id_tipo_transaccion_fk").filter(F.col("numero_siniestro")==20212301333).show(truncate = False))

        disminucion = ["2","17","14","21","12","5","8","7","22"]#se borro el 11
        aumento = ["10","13","1","4","9","6","1","3"]
        
        

        df_reserva = df_reserva.withColumn(
             "id_tipo_transaccion_fk",
            F.when(
                (F.col("id_tipo_transaccion_fk") == 3) & (F.col("homog_id_mov").isin(aumento)),
                F.lit(F.col("homog_id_mov"))
            )
            .when(
                (F.col("id_tipo_transaccion_fk") == 2) & ((F.col("homog_id_mov").isNull()) | (F.col("homog_id_mov") == "2")),
                F.lit(F.lit("4"))
            )
            .otherwise(F.col("id_tipo_transaccion_fk"))
        )

        df_reserva = df_reserva.withColumn(
            "id_tipo_transaccion_fk",
            F.when(F.col("OBSERVACIONES")=="cet", F.lit("4")).otherwise(F.col("id_tipo_transaccion_fk"))
        )   

        df_reserva = df_reserva.withColumn(
            "id_tipo_transaccion_fk",
            F.when(F.col("OBSERVACIONES").isin("aumento generacion tramite auxilio funerario"), F.lit("1")).otherwise(F.col("id_tipo_transaccion_fk"))
        )

        print(df_reserva.select("OBSERVACIONES","homog_id_mov").filter(F.col("id_tipo_transaccion_fk")=="4").show(truncate = False))
        
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
        print(df_gold_plus.select("id_tipo_transaccion_fk").filter(F.col("numero_siniestro")==20212300026).show(truncate = False))
        return df_gold_plus

from pyspark.sql import functions as F,Window
from pyspark.sql.types import StringType
from functools import reduce
from pyspark.sql.functions import udf
from pyspark.sql.functions import split, to_date, date_format, col, regexp_replace
from commons.TransformationHelper import TransformationHelper
import nltk
nltk.download('stopwords', quiet=True)
from nltk.corpus import stopwords

TransformationHelper = TransformationHelper()

class StepGoldPlus:
    def __init__(self, glue_context, spark):
        self.spark = spark

        
    def create_component(self):
        
        def buscar_en_texto(text):
            for palabra, valor in reglas_dict.items():
                if palabra.lower() in text.lower():
                    return valor
            return None
        
        def buscar_en_texto_2(text):
            for palabra, valor in reglas_dict_2.items():
                if palabra.lower() in text.lower():
                    return valor
            return None
        
        def remove_stopwords(text):
            if text is None:
                return None
            words = text.lower().split()
            filter_words = [p for p in words if p.isalpha() and p not in STOP_WORDS]
            return ' '.join(filter_words)
        
        delete_udf = udf(remove_stopwords, StringType())
        buscar_udf = udf(buscar_en_texto, StringType())
        buscar_udf_2 = udf(buscar_en_texto_2, StringType())
        
        STOP_WORDS = set(stopwords.words('spanish'))
        
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
        CR = CR.withColumn('comentario_reserva', delete_udf('comentario_reserva'))

        DATA = [
            ('1','1'),
            ('2','3'),
            ('10','3'),
            ('13','3'),
            ('1','1'),
            ('4','3'),
            ('9','3'),
            ('6','3'),
            ('17','2'),
            ('11','2'),
            ('14','2'),
            ('21','2'),
            ('3','2'),
            ('12','4'),
            ('5','2'),
            ('8','4'),
            ('7','2'),
            ('22','4'),
            ('16','2')
            #('24',''), Son reversiones no se hacen
            #('23','')
        ]

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
        #print(cr.show(truncate = False))
        cr_dict = cr.select("comentario_reserva","homog_id_mov")
        reglas_dict = dict(cr_dict.collect())
        cr_dict_2 = cr.select("comentario_reserva","ID_COMENTARIO_RESERVA")
        reglas_dict_2 = dict(cr_dict_2.collect())
        
        df_reserva = df_reserva.withColumn('OBSERVACIONES', delete_udf('OBSERVACIONES'))
        CR = CR.withColumn( 
            "comentario_reserva",
            F.concat(
                F.lower(F.col("comentario_reserva"))
            )
        )
        df_reserva = TransformationHelper.data_normalice("OBSERVACIONES",df_reserva)
        df_reserva = df_reserva.withColumn( 
            "OBSERVACIONES",
                F.lower(F.col("OBSERVACIONES"))
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
        df_reserva = df_reserva.withColumn(
            "id_comentario",
            buscar_udf_2(F.col("OBSERVACIONES"))
        )
        numero_siniestro = 20222305600

        disminucion = ["17","14","21","12","5","8","7","22","3","11"]#se borro el 11
        aumento = ["10","13","1","4","9","6","1","2"]
        df_reserva = df_reserva.withColumn(
            "id_tipo_transaccion_fk",
            F.when(F.col("id_tipo_transaccion_fk")==2, F.lit(4)).otherwise(F.col("id_tipo_transaccion_fk"))
        ) 
        print(df_reserva.select("numero_siniestro","id_tipo_transaccion_fk","id_comentario","OBSERVACIONES", "homog_id_mov","fecha_transaccion").filter((F.col("numero_siniestro")==numero_siniestro) & (F.year(F.col("fecha_transaccion"))==2022)).show(truncate = False))
        df_reserva = df_reserva.withColumn(
             "id_tipo_transaccion_fk",
            F.when(
                (F.col("id_tipo_transaccion_fk") == 3) & (F.col("id_comentario").isin(aumento)),
                F.lit(F.col("homog_id_mov"))
            ).when(
                (F.col("id_tipo_transaccion_fk") == 4) & (F.col("id_comentario").isin(disminucion)),
                F.lit(F.col("homog_id_mov"))
            #).otherwise(F.lit("4"))
            ).otherwise(F.col("id_tipo_transaccion_fk")) # back up no estoy seguro de este cambio.
        )

        df_reserva = df_reserva.withColumn(
            "id_tipo_transaccion_fk",
            F.when(F.col("OBSERVACIONES")=="cet", F.lit("4")).otherwise(F.col("id_tipo_transaccion_fk"))
        )   
        df_reserva = df_reserva.withColumn(
            "id_tipo_transaccion_fk",
            F.when(F.col("OBSERVACIONES").isin("aumento generacion tramite auxilio"), F.lit("1")).otherwise(F.col("id_tipo_transaccion_fk"))
        )
        print(df_reserva.select("numero_siniestro","id_tipo_transaccion_fk","OBSERVACIONES", "homog_id_mov","fecha_transaccion").filter((F.col("numero_siniestro")==numero_siniestro) & (F.year(F.col("fecha_transaccion"))==2022)).show(truncate = False))
        df_reserva = df_reserva.drop("OBSERVACIONES", "homog_id_mov","id_comentario")
        print(df_reserva.select("numero_siniestro","id_tipo_transaccion_fk","fecha_transaccion").filter((F.col("numero_siniestro")==numero_siniestro) & (F.year(F.col("fecha_transaccion"))==2022)).show(truncate = False))
        

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
            .where((F.col(DP_VLR_PAGO).cast("double") != 0)) \
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

        df_pago = df_pago.filter(
            ((F.col("id_tipo_transaccion_fk"))  == '5')  & (F.lower(F.col("DP.BLN_REVERSADO")) != 's') |
            (F.col("id_tipo_transaccion_fk") == '6')
        )
        df_gold_plus = df_reserva.unionAll(df_pago)
        print(df_gold_plus.select("numero_siniestro","id_tipo_transaccion_fk","fecha_transaccion").filter((F.col("numero_siniestro")==numero_siniestro) & (F.year(F.col("fecha_transaccion"))==2022)).show(truncate = False))
        df_gold_plus = df_gold_plus.withColumn(
            "valor_transaccion",
            F.when(F.col("id_tipo_transaccion_fk").isin('2','4','6'), F.col("valor_transaccion") *-1)
            .otherwise(F.col("valor_transaccion"))
        )

        return df_gold_plus

from pyspark.sql import functions as F,Window
from pyspark.sql.types import StringType
from functools import reduce
from pyspark.sql.functions import udf
from pyspark.sql.functions import split, to_date, date_format, col, regexp_replace
from commons.TransformationHelper import TransformationHelper
import nltk
nltk.download('stopwords', quiet=True)
from nltk.corpus import stopwords

TransformationHelper = TransformationHelper()

class StepGoldPlus:
    def __init__(self, glue_context, spark):
        self.spark = spark

        
    def create_component(self):
        
        def buscar_en_texto(text):
            for palabra, valor in reglas_dict.items():
                if palabra.lower() in text.lower():
                    return valor
            return None
        
        def buscar_en_texto_2(text):
            for palabra, valor in reglas_dict_2.items():
                if palabra.lower() in text.lower():
                    return valor
            return None
        
        def remove_stopwords(text):
            if text is None:
                return None
            words = text.lower().split()
            filter_words = [p for p in words if p.isalpha() and p not in STOP_WORDS]
            return ' '.join(filter_words)
        
        delete_udf = udf(remove_stopwords, StringType())
        buscar_udf = udf(buscar_en_texto, StringType())
        buscar_udf_2 = udf(buscar_en_texto_2, StringType())
        
        STOP_WORDS = set(stopwords.words('spanish'))
        
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
        CR = CR.withColumn('comentario_reserva', delete_udf('comentario_reserva'))

        DATA = [
            ('1','1'),
            ('2','3'),
            ('10','3'),
            ('13','3'),
            ('1','1'),
            ('4','3'),
            ('9','3'),
            ('6','3'),
            ('17','2'),
            ('11','2'),
            ('14','2'),
            ('21','2'),
            ('3','2'),
            ('12','4'),
            ('5','2'),
            ('8','4'),
            ('7','2'),
            ('22','4'),
            ('16','2')
            #('24',''), Son reversiones no se hacen
            #('23','')
        ]

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
        cr_dict_2 = cr.select("comentario_reserva","ID_COMENTARIO_RESERVA")
        reglas_dict_2 = dict(cr_dict_2.collect())
        
        df_reserva = df_reserva.withColumn('OBSERVACIONES', delete_udf('OBSERVACIONES'))
        CR = CR.withColumn( 
            "comentario_reserva",
            F.concat(
                F.lower(F.col("comentario_reserva"))
            )
        )
        df_reserva = TransformationHelper.data_normalice("OBSERVACIONES",df_reserva)
        df_reserva = df_reserva.withColumn( 
            "OBSERVACIONES",
                F.lower(F.col("OBSERVACIONES"))
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
        df_reserva = df_reserva.withColumn(
            "id_comentario",
            buscar_udf_2(F.col("OBSERVACIONES"))
        )

        disminucion = ["17","14","21","12","5","8","7","22","3","11","16"]
        aumento = ["10","13","1","4","9","6","1","2"]
        df_reserva = df_reserva.withColumn(
            "id_tipo_transaccion_fk",
            F.when(F.col("id_tipo_transaccion_fk")==2, F.lit(4)).otherwise(F.col("id_tipo_transaccion_fk"))
        ) 
        df_reserva = df_reserva.withColumn(
             "id_tipo_transaccion_fk",
            F.when(
                (F.col("id_tipo_transaccion_fk") == 3) & (F.col("id_comentario").isin(aumento)),
                F.lit(F.col("homog_id_mov"))
            ).when(
                (F.col("id_tipo_transaccion_fk") == 4) & (F.col("id_comentario").isin(disminucion)),
                F.lit(F.col("homog_id_mov"))
            ).otherwise(F.col("id_tipo_transaccion_fk"))
        )

        df_reserva = df_reserva.withColumn(
            "id_tipo_transaccion_fk",
            F.when(F.col("OBSERVACIONES")=="cet", F.lit("4")).otherwise(F.col("id_tipo_transaccion_fk"))
        )   
        df_reserva = df_reserva.withColumn(
            "id_tipo_transaccion_fk",
            F.when(F.col("OBSERVACIONES").isin("aumento generacion tramite auxilio"), F.lit("1")).otherwise(F.col("id_tipo_transaccion_fk"))
        )
        df_reserva = df_reserva.drop("OBSERVACIONES", "homog_id_mov","id_comentario")
        

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
            .where((F.col(DP_VLR_PAGO).cast("double") != 0)) \
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

        df_pago = df_pago.filter(
            ((F.col("id_tipo_transaccion_fk"))  == '5')  & (F.lower(F.col("DP.BLN_REVERSADO")) != 's') |
            (F.col("id_tipo_transaccion_fk") == '6')
        )
        df_gold_plus = df_reserva.unionAll(df_pago)
        df_gold_plus = df_gold_plus.withColumn(
            "valor_transaccion",
            F.when(F.col("id_tipo_transaccion_fk").isin('2','4','6'), F.col("valor_transaccion") *-1)
            .otherwise(F.col("valor_transaccion"))
        )
        print("este es")

        return df_gold_plus

from pyspark.sql import functions as F,Window
from pyspark.sql.types import StringType
from functools import reduce
from pyspark.sql.functions import udf
from pyspark.sql.functions import split, to_date, date_format, col, regexp_replace
from commons.TransformationHelper import TransformationHelper
import nltk
nltk.download('stopwords', quiet=True)
from nltk.corpus import stopwords

TransformationHelper = TransformationHelper()

class StepGoldPlus:
    def __init__(self, glue_context, spark):
        self.spark = spark

        
    def create_component(self):
        
        def buscar_en_texto(text):
            for palabra, valor in reglas_dict.items():
                if palabra.lower() in text.lower():
                    return valor
            return None
        
        def buscar_en_texto_2(text):
            for palabra, valor in reglas_dict_2.items():
                if palabra.lower() in text.lower():
                    return valor
            return None
        
        def remove_stopwords(text):
            if text is None:
                return None
            words = text.lower().split()
            filter_words = [p for p in words if p.isalpha() and p not in STOP_WORDS]
            return ' '.join(filter_words)
        
        delete_udf = udf(remove_stopwords, StringType())
        buscar_udf = udf(buscar_en_texto, StringType())
        buscar_udf_2 = udf(buscar_en_texto_2, StringType())
        
        STOP_WORDS = set(stopwords.words('spanish'))
        
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
        CR = CR.withColumn('comentario_reserva', delete_udf('comentario_reserva'))

        DATA = [
            ('1','1'),
            ('2','3'),
            ('10','3'),
            ('13','3'),
            ('1','1'),
            ('4','3'),
            ('9','3'),
            ('6','3'),
            ('17','2'),
            ('11','2'),
            ('14','2'),
            ('21','2'),
            ('3','2'),
            ('12','4'),
            ('5','2'),
            ('8','4'),
            ('7','2'),
            ('22','4'),
            ('16','2')
            #('24',''), Son reversiones no se hacen
            #('23','')
        ]

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
                F.when(
                    (F.col("AF.COD_CIUDAD").isNull())| (F.col("AF.COD_CIUDAD") == '0'),
                    F.lit("01001")
                ).otherwise(F.col("AF.COD_CIUDAD")).alias("ciudad_lugar_ocurrencia"),
                F.when(
                    (F.col("AF.cod_departamento").isNull())| (F.col("AF.cod_departamento") == '0'),
                    F.lit("01")
                ).otherwise(F.col("AF.cod_departamento")).alias("depto_lugar_ocurrencia"), 
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
        cr_dict_2 = cr.select("comentario_reserva","ID_COMENTARIO_RESERVA")
        reglas_dict_2 = dict(cr_dict_2.collect())
        
        df_reserva = df_reserva.withColumn('OBSERVACIONES', delete_udf('OBSERVACIONES'))
        CR = CR.withColumn( 
            "comentario_reserva",
            F.concat(
                F.lower(F.col("comentario_reserva"))
            )
        )
        df_reserva = TransformationHelper.data_normalice("OBSERVACIONES",df_reserva)
        df_reserva = df_reserva.withColumn( 
            "OBSERVACIONES",
                F.lower(F.col("OBSERVACIONES"))
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
        df_reserva = df_reserva.withColumn(
            "id_comentario",
            buscar_udf_2(F.col("OBSERVACIONES"))
        )

        disminucion = ["17","14","21","12","5","8","7","22","3","11","16"]
        aumento = ["10","13","1","4","9","6","1","2"]
        df_reserva = df_reserva.withColumn(
            "id_tipo_transaccion_fk",
            F.when(F.col("id_tipo_transaccion_fk")==2, F.lit(4)).otherwise(F.col("id_tipo_transaccion_fk"))
        ) 
        df_reserva = df_reserva.withColumn(
             "id_tipo_transaccion_fk",
            F.when(
                (F.col("id_tipo_transaccion_fk") == 3) & (F.col("id_comentario").isin(aumento)),
                F.lit(F.col("homog_id_mov"))
            ).when(
                (F.col("id_tipo_transaccion_fk") == 4) & (F.col("id_comentario").isin(disminucion)),
                F.lit(F.col("homog_id_mov"))
            ).otherwise(F.col("id_tipo_transaccion_fk"))
        )

        df_reserva = df_reserva.withColumn(
            "id_tipo_transaccion_fk",
            F.when(F.col("OBSERVACIONES")=="cet", F.lit("4")).otherwise(F.col("id_tipo_transaccion_fk"))
        )   
        df_reserva = df_reserva.withColumn(
            "id_tipo_transaccion_fk",
            F.when(F.col("OBSERVACIONES").isin("aumento generacion tramite auxilio"), F.lit("1")).otherwise(F.col("id_tipo_transaccion_fk"))
        )
        df_reserva = df_reserva.drop("OBSERVACIONES", "homog_id_mov","id_comentario")
        

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
            .where((F.col(DP_VLR_PAGO).cast("double") != 0)) \
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
                F.when(
                    (F.col("AF.COD_CIUDAD").isNull())| (F.col("AF.COD_CIUDAD") == '0'),
                    F.lit("01001")
                ).otherwise(F.col("AF.COD_CIUDAD")).alias("ciudad_lugar_ocurrencia"),
                F.when(
                    (F.col("AF.cod_departamento").isNull())| (F.col("AF.cod_departamento") == '0'),
                    F.lit("01")
                ).otherwise(F.col("AF.cod_departamento")).alias("depto_lugar_ocurrencia"), 
                F.col("AF2.FEC_INGRESO_AFP").alias("fecha_inicio_vigencia"),
                F.coalesce(F.col(AF2_NID_NUMERO),F.lit("No registra")).alias("cedula"),
                F.col("AF2.FEC_NACIMIENTO").alias("fecha_nacimiento"),
                F.coalesce(F.col("AF2.GENERO"),F.lit("No registra")).alias("genero")
 
            )

        df_pago = df_pago.filter(
            ((F.col("id_tipo_transaccion_fk"))  == '5')  & (F.lower(F.col("DP.BLN_REVERSADO")) != 's') |
            (F.col("id_tipo_transaccion_fk") == '6')
        )
        df_gold_plus = df_reserva.unionAll(df_pago)
        df_gold_plus = df_gold_plus.withColumn(
            "valor_transaccion",
            F.when(F.col("id_tipo_transaccion_fk").isin('2','4','6'), F.col("valor_transaccion") *-1)
            .otherwise(F.col("valor_transaccion"))
        )

        return df_gold_plus
StepGoldPlus = StepGoldPlus(glueContext, spark)
df =StepGoldPlus.create_component()
df.filter(F.col("numero_siniestro").isin('2')).count()#21712,638 ; 22350-19682,2668; 22350
df.select("numero_siniestro","id_tipo_transaccion_fk").filter(F.col("numero_siniestro")==20222302993).show()#21712,638 ; 22350-19682,2668; 22350
#21712, 
df.select("fecha_transaccion","id_tipo_transaccion_fk").filter(F.col("numero_siniestro")==20212301333).show()
df_export = df.filter((F.col("id_tipo_transaccion_fk").isin('4')) & (F.year(F.col("fecha_transaccion")) == 2024))
df_export.filter(F.col("numero_siniestro")==20212301333).count()

df_export.coalesce(1).write.format("csv").option("header",True).mode("overwrite").save("s3://alfa-gold-test/test/")
from pyspark.sql.functions import col, year, month, round, sum as _sum, format_number
 
 
df_1 = df.select("fecha_transaccion","id_tipo_transaccion_fk","valor_transaccion").filter(
    (col("id_fuente_fk") == '3') &
    (year(col("fecha_transaccion")) == 2023) 
)
  
df_resultado = df_1.groupBy(
    year(col("fecha_transaccion")).alias("año"), "id_tipo_transaccion_fk"
).agg(
    format_number(round(_sum("valor_transaccion"), 2), 2).alias("suma_valor_formateado")
)
 
df_resultado.orderBy("id_tipo_transaccion_fk").show(truncate=False)
df.select("id_tipo_transaccion_fk").distinct().show()
df.groupBy(F.col("id_tipo_transaccion_fk")).agg(
    F.count("*")
).show()
from pyspark.sql import functions as F,Window
from pyspark.sql.types import StringType
from functools import reduce
from pyspark.sql.functions import udf
from pyspark.sql.functions import split, to_date, date_format, col, regexp_replace
from commons.TransformationHelper import TransformationHelper
import nltk
nltk.download('stopwords', quiet=True)
from nltk.corpus import stopwords

TransformationHelper = TransformationHelper()

class StepGoldPlus:
    def __init__(self, glue_context, spark):
        self.spark = spark

        
    def create_component(self):
        
        def buscar_en_texto(text):
            for palabra, valor in reglas_dict.items():
                if palabra.lower() in text.lower():
                    return valor
            return None
        
        def buscar_en_texto_2(text):
            for palabra, valor in reglas_dict_2.items():
                if palabra.lower() in text.lower():
                    return valor
            return None
        
        def remove_stopwords(text):
            if text is None:
                return None
            words = text.lower().split()
            filter_words = [p for p in words if p.isalpha() and p not in STOP_WORDS]
            return ' '.join(filter_words)
        
        delete_udf = udf(remove_stopwords, StringType())
        buscar_udf = udf(buscar_en_texto, StringType())
        buscar_udf_2 = udf(buscar_en_texto_2, StringType())
        
        STOP_WORDS = set(stopwords.words('spanish'))
        
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
        PPR_AFILIADO  ="global_temp.ppr_afiliados"

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
        CR = CR.withColumn('comentario_reserva', delete_udf('comentario_reserva'))
        PA = self.spark.table(PPR_AFILIADO).alias("PA")

        DATA = [
            ('1','1'),
            ('2','3'),
            ('10','3'),
            ('13','3'),
            ('1','1'),
            ('4','3'),
            ('9','3'),
            ('6','3'),
            ('17','2'),
            ('11','2'),
            ('14','2'),
            ('21','2'),
            ('3','2'),
            ('12','4'),
            ('5','2'),
            ('8','4'),
            ('7','2'),
            ('22','4'),
            ('16','2')
            #('24',''), Son reversiones no se hacen
            #('23','')
        ]

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
            .join(PA.alias("PA"), (F.col(PCL_NUM_IDEN) == F.col("PA.nid_identificacion")) & (F.col(AF2_NID_NUMERO) == F.col("PA.cod_tipo_identificacion_afp")), "left") \
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
                F.concat(
                    F.when(
                        (F.col("AF.cod_departamento").isNull()) | (F.col("AF.cod_departamento") == '0'),
                        F.lit("01")
                    ).otherwise(F.col("AF.cod_departamento")),
                    F.when(
                        (F.col("AF.COD_CIUDAD").isNull()) | (F.col("AF.COD_CIUDAD") == '0'),
                        F.lit("01001")
                    ).otherwise(F.col("AF.COD_CIUDAD"))
                ).alias("ciudad_lugar_ocurrencia"),
                F.when(
                    (F.col("AF.cod_departamento").isNull())| (F.col("AF.cod_departamento") == '0'),
                    F.lit("01")
                ).otherwise(F.col("AF.cod_departamento")).alias("depto_lugar_ocurrencia"), 
                F.coalesce(
                    F.date_format(F.col("AF2.FEC_INGRESO_AFP"), "yyyy-MM-dd"),
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
        cr_dict_2 = cr.select("comentario_reserva","ID_COMENTARIO_RESERVA")
        reglas_dict_2 = dict(cr_dict_2.collect())
        
        df_reserva = df_reserva.withColumn('OBSERVACIONES', delete_udf('OBSERVACIONES'))
        CR = CR.withColumn( 
            "comentario_reserva",
            F.concat(
                F.lower(F.col("comentario_reserva"))
            )
        )
        df_reserva = TransformationHelper.data_normalice("OBSERVACIONES",df_reserva)
        df_reserva = df_reserva.withColumn( 
            "OBSERVACIONES",
                F.lower(F.col("OBSERVACIONES"))
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
        df_reserva = df_reserva.withColumn(
            "id_comentario",
            buscar_udf_2(F.col("OBSERVACIONES"))
        )

        disminucion = ["17","14","21","12","5","8","7","22","3","11","16"]
        aumento = ["10","13","1","4","9","6","1","2"]
        df_reserva = df_reserva.withColumn(
            "id_tipo_transaccion_fk",
            F.when(F.col("id_tipo_transaccion_fk")==2, F.lit(4)).otherwise(F.col("id_tipo_transaccion_fk"))
        ) 
        df_reserva = df_reserva.withColumn(
             "id_tipo_transaccion_fk",
            F.when(
                (F.col("id_tipo_transaccion_fk") == 3) & (F.col("id_comentario").isin(aumento)),
                F.lit(F.col("homog_id_mov"))
            ).when(
                (F.col("id_tipo_transaccion_fk") == 4) & (F.col("id_comentario").isin(disminucion)),
                F.lit(F.col("homog_id_mov"))
            ).otherwise(F.col("id_tipo_transaccion_fk"))
        )

        df_reserva = df_reserva.withColumn(
            "id_tipo_transaccion_fk",
            F.when(F.col("OBSERVACIONES")=="cet", F.lit("4")).otherwise(F.col("id_tipo_transaccion_fk"))
        )   
        df_reserva = df_reserva.withColumn(
            "id_tipo_transaccion_fk",
            F.when(F.col("OBSERVACIONES").isin("aumento generacion tramite auxilio"), F.lit("1")).otherwise(F.col("id_tipo_transaccion_fk"))
        )
        df_reserva = df_reserva.drop("OBSERVACIONES", "homog_id_mov","id_comentario")
        

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
            .join(PA.alias("PA"), (F.col(PCL_NUM_IDEN) == F.col("PA.nid_identificacion")) & (F.col(AF2_NID_NUMERO) == F.col("PA.cod_tipo_identificacion_afp")), "left") \
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
                F.concat(
                    F.when(
                        (F.col("AF.cod_departamento").isNull()) | (F.col("AF.cod_departamento") == '0'),
                        F.lit("01")
                    ).otherwise(F.col("AF.cod_departamento")),
                    F.when(
                        (F.col("AF.COD_CIUDAD").isNull()) | (F.col("AF.COD_CIUDAD") == '0'),
                        F.lit("01001")
                    ).otherwise(F.col("AF.COD_CIUDAD"))
                ).alias("ciudad_lugar_ocurrencia"),
                F.when(
                    (F.col("AF.cod_departamento").isNull())| (F.col("AF.cod_departamento") == '0'),
                    F.lit("01")
                ).otherwise(F.col("AF.cod_departamento")).alias("depto_lugar_ocurrencia"), 
                F.coalesce(
                    F.date_format(F.col("AF2.FEC_INGRESO_AFP"), "yyyy-MM-dd"),
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
                F.coalesce(F.col(AF2_NID_NUMERO),F.lit("No registra")).alias("cedula"),
                F.col("AF2.FEC_NACIMIENTO").alias("fecha_nacimiento"),
                F.coalesce(F.col("AF2.GENERO"),F.lit("No registra")).alias("genero")
 
            )

        df_pago = df_pago.filter(
            ((F.col("id_tipo_transaccion_fk"))  == '5')  & (F.lower(F.col("DP.BLN_REVERSADO")) != 's') |
            (F.col("id_tipo_transaccion_fk") == '6')
        )
        df_gold_plus = df_reserva.unionAll(df_pago)
        df_gold_plus = df_gold_plus.withColumn(
            "valor_transaccion",
            F.when(F.col("id_tipo_transaccion_fk").isin('2','4','6'), F.col("valor_transaccion") *-1)
            .otherwise(F.col("valor_transaccion"))
        )
        print("PLUS ANTONIA")

        return df_gold_plus
df.count()
from commons.StepGold import StepGold
from commons.StepGoldPlus import StepGoldPlus
from commons.StepGoldBodega import StepGoldBodega
from pyspark.sql.window import Window
from commons.EstadoSiniestroHelperA import EstadoSiniestroHelperA
StepGoldPlus = StepGoldPlus(glueContext, spark)
StepGoldBodega = StepGoldBodega(glueContext, spark)
StepGold = StepGold(glueContext, spark)
EstadoSiniestro_Helper = EstadoSiniestroHelperA(glueContext, spark)

df_tierra = StepGold.create_component()
df_plus = StepGoldPlus.create_component()
df_plus = df_plus.filter(~F.col("fecha_transaccion").isNull())
df_bodega = StepGoldBodega.create_component()
df_bodega = df_bodega.withColumn("fecha_objecion", F.lit(None))
df_main = df_tierra.unionByName(df_plus).unionByName(df_bodega)

df_movimientos_plus = df_main.select(
            F.col("numero_siniestro").alias("id_siniestro_fk"),
            F.col("id_tipo_transaccion_fk"),
            F.col("id_categoria_fk"),
            F.col("fecha_transaccion").cast("date"),
            F.col("fecha_objecion").cast("date").alias("FECHA_OBJECION"),
            F.col("valor_transaccion_ajustado").cast("double"),
            F.col("valor_transaccion").cast("double"),
            F.col("id_fuente_fk"),
            F.col("cod_origen").alias("id_cobertura_fk"),
            F.col("fecha_ocurrencia").cast("date").alias("fecha_ocurrencia"),
            F.col("poliza").cast("string").alias("id_poliza_fk"),
            F.col("depto_lugar_ocurrencia").cast("string").alias("id_departamento_ocurrencia_fk"),
            F.col("ciudad_lugar_ocurrencia").cast("string").alias("id_ciudad_ocurrencia_fk")
        )
df_movimientos_dim = spark.sql("SELECT * FROM global_temp.DIM_TIPO_TRANSACCION")
df_estado_plus = df_movimientos_plus.join( df_movimientos_dim, F.col("id_tipo_transaccion_pk") == F.col("id_tipo_transaccion_fk"), "inner")

df_estado_plus = df_estado_plus.select(
        F.col("id_siniestro_fk"),
        F.col("fecha_transaccion"),
        F.col("tipo_de_transaccion").alias("TIPO_TRANSACCION"),#RA
        F.col("id_fuente_fk"),#id_fuente
        F.col("FECHA_OBJECION"),#null
        F.col("valor_transaccion_ajustado").alias("VALOR_TRANSACCION"),#monto
        F.lit("").alias("fundamento_no_pago")
    )
df = df_estado_plus.filter(F.col("id_fuente_fk").isin("2","3"))
df = df .withColumn(
    "valor_transaccion",
    F.when(
        ((F.col("id_fuente_fk") == "0") & (F.lower(F.col("tipo_transaccion")) == "rechazo pago")) |
 
        ((F.col("id_fuente_fk") == "1") & (F.lower(F.col("tipo_transaccion")).isin([
            "disminucion reserva", "liberacion reserva", "pago","reversion del recupero"
        ]))) |
 
        ((F.col("id_fuente_fk") == "2") & (F.lower(F.col("tipo_transaccion")).isin([
            "pago", "rechazo de pago"
        ]))) |
 
        ((F.col("id_fuente_fk") == "3") & (F.lower(F.col("tipo_transaccion")).isin([
            "pago", "rechazo de pago"
        ]))),
        F.col("VALOR_TRANSACCION") * -1  # Multiplicar por -1
        #(mantener valor original)
    ).otherwise(F.col("VALOR_TRANSACCION")).alias("valor_transaccion_ajustado")
)
df = df.filter(
    ~F.lower(F.col("tipo_transaccion")).isin(["recobro", "reversion del recupero"])
)
df.filter((F.col("id_fuente_fk") == "2")&(F.year(F.col("fecha_transaccion"))==2002)).count()
df = df.filter(F.year(F.col("fecha_transaccion")) >= 2021)
#df = df.filter(F.year(F.col("fecha_transaccion")) < 2021)
df_estado= EstadoSiniestro_Helper.create_estado_siniestro(df)
df_mov_ant = spark.sql("SELECT * FROM global_temp.FACT_ESTADO_SINIESTRO")
df_mov_ant = create_reserv_mov(df_mov_ant)
df_mov_ant.count()
ano = df_mov_ant.select(F.max("ano").alias("ano_actual")).collect()[0]
print(ano)
df_mov_ant.filter(F.col("id_siniestro_fk")=="201907705").show()
df = df.unionAll(df_mov_ant)
df_mov_ant.count()
#df_estado.agg(F.max("ano")).show()
df_estado.select("estado").distinct().show()
path = f"s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_ESTADO_SINIESTRO/"
ExtractHelper.save_files(df_estado,path)
df = df.unionAll(df_mov_ant)
df_mov_ant = create_reserv_mov(df_estado)
df = df.unionAll(df_mov_ant)
array_year = df.select(F.year(F.col("fecha_transaccion")).alias("anio")).distinct().collect()
array_fuente = df.select(F.year(F.col("id_fuente_fk")).alias("fuente")).distinct().collect()
array_fuente
year_result = []

for i in range(4, len(array_year), 5):
    year_result.append(array_year[i]["anio"])
    
if (len(array_year) % 5 != 0):
    year_result.append(array_year[len(array_year) - 1]["anio"])
    
df = df.filter(F.year(F.col("fecha_transaccion")) >= 2021)
df_incremental = spark.sql("SELECT * FROM global_temp.FACT_ESTADO_SINIESTRO")
if df_incremental:
    df_mov_reserva = create_reserv_mov(df_incremental)
    df_mov_reserva = df_movimientos.unionAll(df_mov_reserva)
    df_estado = EstadoSiniestro_Helper.create_estado_siniestro(df_mov_reserva)
    path = f"s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_ESTADO_SINIESTRO_SIPREN_parition/"
    ExtractHelper.save_files(df_estado.repartition(200),path)
else:
    array_year = df_movimientos.select(F.year(F.col("fecha_transaccion")).alias("anio")).distinct().collect()
    
    year_result = []

    for i in range(4, len(array_year), 5):
        year_result.append(array_year[i]["anio"])

    if (len(array_year) % 5 != 0):
        year_result.append(array_year[len(array_year) - 1]["anio"])
    
    prev_x = None
    for x in year_result:
        if prev_x is None:
            df_mov_reserva = df_movimientos.filter(F.year(F.col("fecha_transaccion")) < x)
            df_estado_initial = EstadoSiniestro_Helper.create_estado_siniestro(df_mov_reserva)
        else:
            df_mov_reserva = df_movimientos.filter((F.year(F.col("fecha_transaccion")) > prev_x) & (F.year(F.col("fecha_transaccion")) < x))
            df_estado_sec = EstadoSiniestro_Helper.create_estado_siniestro(df_mov_reserva)
            df_estado_initial = df_estado_initial.unionAll(df_estado_sec)
        
        path = f"s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_ESTADO_SINIESTRO_SIPREN_parition/"
        ExtractHelper.save_files(df_estado_initial.repartition(200),path)
##ejecutar para segunda vuelta

if df_mov_reserva:
    df = df.unionAll(df_mov_reserva)
    path = f"s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_ESTADO_SINIESTRO_SIPREN_parition/"
    ExtractHelper.save_files(df_estado.repartition(200),path)
else:
    df_estado= EstadoSiniestro_Helper.create_estado_siniestro(df)
    path = f"s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_ESTADO_SINIESTRO_SIPREN_parition/"
    ExtractHelper.save_files(df_estado.repartition(200),path)
    
df = df.unionAll(df_mov_reserva)
df_mov = df.filter(F.year(F.col("fecha_transaccion")) < 1998)#[1998, 2003, 2008, 2013, 2018, 2023, 2025]
df_estado= EstadoSiniestro_Helper.create_estado_siniestro(df)
path = f"s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_ESTADO_SINIESTRO/"
ExtractHelper.save_files(df_estado,path)
df_ant = spark.sql("SELECT * FROM global_tamp.FACT_ESTADO")
def create_reserv_mov(df):
    """
    This function help us to indentificate old claim in reserved and return new df with new data for estado_siniestro
    """
    df_mov_ra = df.filter(F.col("estado") == "En Reserva")
    print(df.select(F.max("ano")).collect()[0][0])
    ano_max = df.select(F.max("ano")).collect()[0][0]
    df_mov_ra = df_mov_ra.filter(F.col("ano") == ano_max)
    print(df_mov_ra.select(F.max("mes")).collect()[0][0])
    mes_max = df_mov_ra.select(F.max("mes")).collect()[0][0]
    #window_spec = Window.partitionBy("id_siniestro_fk").orderBy(F.desc("ano"), F.desc("mes"))
    #df_filtered = df_mov_ra.withColumn("row_number", F.row_number().over(window_spec))
    #df_filtered = df_filtered.filter(F.col("row_number") == 1).drop("row_number")
    df_mov_ra = df_mov_ra.filter(F.col("mes") == mes_max)
    
    #ano_actual_row = df.select(F.max("ano").alias("ano_actual")).collect()[0]
    #ano_actual = ano_actual_row["ano_actual"]
    #mes_actual_row = df.select(F.max("mes").alias("mes_actual")).collect()[0]
    #mes_actual = mes_actual_row["mes_actual"]
    
    df_mov_ra = df_mov_ra.select(
        F.col("id_siniestro_fk"),
        F.to_date(
            F.concat_ws(
            "-", F.lit(ano_max), F.lpad(F.lit(mes_max),2,"0"), F.lpad(F.lit(1),2 ,"0")
        ), "yyyy-MM-dd"
        ).alias("fecha_transaccion"),
        F.lit("Reserva anterior").alias("TIPO_TRANSACCION"),#RA
        F.col("id_fuente_fk"),#id_fuente
        F.lit(None).alias("FECHA_OBJECION"),#null
        F.col("monto").alias("VALOR_TRANSACCION"),#monto
        F.lit("").alias("fundamento_no_pago")#""
    )
    
    return df_mov_ra
df_mov_ra = df_estado.filter(F.col("estado") == "En Reserva")
window_spec = Window.partitionBy("id_siniestro_fk").orderBy(F.desc("ano"), F.desc("mes"))
df_filtered = df_mov_ra.withColumn("row_number", F.row_number().over(window_spec))
df_filtered = df_filtered.filter(F.col("row_number") == 1).drop("row_number")
df_filtered.show()
df_mov_reserva = df_filtered.select(
        F.col("id_siniestro_fk"),
        F.to_date(
            F.concat_ws(
            "-", F.col("ano"), F.lpad(F.col("mes"),2,"0"), F.lpad(F.lit(1),2 ,"0")
        ), "yyyy-MM-dd"
        ).alias("fecha_transaccion"),
        F.lit("Reserva anterior").alias("TIPO_TRANSACCION"),#RA
        F.col("id_fuente_fk"),#id_fuente
        F.lit(None).alias("FECHA_OBJECION"),#null
        F.col("monto").alias("VALOR_TRANSACCION"),#monto
        F.lit("").alias("fundamento_no_pago")#""
    )
df_mov_reserva.show()
from pyspark.sql.functions import col, year, month, round, sum as _sum, format_number
 
 
df_mov_ra_mayores = df_mov_ra.select("id_siniestro_fk")
  
df_resultado = df_mov_ra_mayores.groupBy(
    year(col("fecha_transaccion")).alias("año"), "tipo_transaccion"
).agg(
    format_number(round(_sum("valor_transaccion"), 2), 2).alias("suma_valor_formateado")
)
 
df_resultado.orderBy("tipo_transaccion").show(truncate=False)
df_estado_uno = df_estado.filter(F.col("ano") <= 2019)
df_estado_uno_dos = df_estado.filter(F.col("ano") <= 2019).filter(F.col("ano") >= 2021)
df_estado_dos = df_estado.filter(F.col("ano") > 2021)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
df_estado.repartition(500).write.mode("overwrite").option("compression", "snappy").option("maxRecordsPerFile", 100000).parquet("s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_ESTADO_SINIESTRO_SIPREN_T/")
df_estado= EstadoSiniestro_Helper.create_estado_siniestro(df)
path = f"s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_ESTADO_SINIESTRO_SIPREN_parition/"
ExtractHelper.save_files(df_estado.repartition(200),path)
from awsglue import DynamicFrame
from datetime import datetime
from pyspark.sql import functions as F
import pandas as pd
import boto3
from botocore.exceptions import ClientError
def save_files(df, path):
    """
    This function save df in parquet files.

    arg:
        df: Dataframe to save in parquet files.
        tb_name: Name of table to save (save in path s3)
        path: path to save files.
    retunr:
        response: response of the execution
    """
    df_dynamic = DynamicFrame.fromDF(df , glueContext, "nested")
    response = glueContext.write_dynamic_frame.from_options(
        frame=df_dynamic, 
        connection_type="s3", 
        format="glueparquet", 
        connection_options={"path": f"{path}{self.year}/{self.month}/{self.day}", "partitionKeys": []},
        format_options={"compression": "snappy","maxRecordsPerFile":100000}, 
        transformation_ctx="AmazonS3_write"
    )
    return response
df_estado_plus = df_estado_plus.filter(F.col("id_fuente_fk")=="3")
df_main.select("id_fuente_fk").distinct().show()
df_estado_plus.select("id_fuente_fk").distinct().show()
df_estado_plus = df_estado_plus.withcolumn(
    "VALOR_TRANSACCION",
    F.when(
        F.col("VALOR_TRANSACCION").isNull(), F.lit(0)
    ).otherwise(F.col("VALOR_TRANSACCION"))
)
df_estado= EstadoSiniestro_Helper.create_estado_siniestro(df_estado_plus)
df_estado_plus.filter(F.col("VALOR_TRANSACCION").isNull()).count()
VIDA = spark.table("global_temp.CIERREVIDA")
StepGold = StepGold(glueContext, spark)
StepGoldPlus = StepGoldPlus(glueContext, spark)
from commons.StepGold import StepGold
from commons.IncrementalCharge import IncrementalCharge
from commons.TransformationHelper import TransformationHelper
from pyspark.sql.types import IntegerType
import sys
import io
 
IncrementalCharge = IncrementalCharge(glueContext, spark, "-", "-")
TransformationHelper = TransformationHelper()

path_source = "s3://alfa-datalake-dev-bronce/SIPREN/TIERRA/SIPREN-SIPREN_ALFA-SPR_CAUSAS_SINIESTROS/"
#IncrementalCharge.charge_tmp_tables(path_source, "SNR_CAUSA", 'Gold')
df = ExtractHelper.extract_parquet_data(path_source, "cod_causa_siniestro,descripcion", "2025-9-4")
#df = ExtractHelper.extract_parquet_data(path_source, "ID_CAUSAL,NOMBRE", "2025-9-4")
df = TransformationHelper.trim_data(df)
df = spark.sql("SELECT * FROM global_temp.SNR_CAUSA")
print(df.filter(F.lower(F.col("descripcion"))=="catastrofe natural").count())
print(df.filter(F.lower(F.col("descripcion"))=="Catastrofe natural").show())
path = f"s3://alfa-datalake-dev-assets/support_phase_files/Homologaciones-tierra-vf.xlsx"
sheet = "Homologaciones_vf"
df_homologaciones_dos = ExtractHelper.extract_excel_data(path, sheet)

tabla_to_proccess = df_homologaciones_dos.select("tabla").distinct().rdd.flatMap(lambda x:x).collect()

df_homologaciones_principal = df_homologaciones_dos.selectExpr(
"tabla as tabla",
"campo_referencia as campo_referencia",
"valor_referencia",
"valor_homologacion",
"campo_homologacion"
)
df_homologaciones_principal = TransformationHelper.trim_data(df_homologaciones_principal)
df = TransformationHelper.homologacion_i("snr_causa", df_homologaciones_principal,df_homologaciones_principal, df, 'homologacion')
print(df.filter(F.lower(F.col("descripcion"))=="muerte natural").count())
print(df.filter(F.lower(F.col("descripcion"))=="muerte natural").show())
df = TransformationHelper.homologacion_i("spr_causas_siniestros", df_homologaciones_principal,df_homologaciones_principal, df, 'remediacion')
print(df.filter(F.lower(F.col("descripcion"))=="muerte natural").count())
print(df.filter(F.lower(F.col("descripcion"))=="muerte natural").show())
df = df.distinct()
df.groupBy("cod_causa_siniestro").count().filter(F.col("cod_causa_siniestro")>2).show()
df.groupBy("cod_causa_siniestro").count().filter(F.col("cod_causa_siniestro")>1).show()
print(df.filter(F.lower(F.col("descripcion"))=="ceguera").count())
print(df.filter(F.lower(F.col("descripcion"))=="ceguera").show())
path = f"s3://alfa-datalake-dev-bronce/SIPREN/TIERRA/SIPREN-SIPREN_ALFA-SPR_CAUSAS_SINIESTROS/"
ExtractHelper.save_files(df,path)
from commons.StepGold import StepGold
from commons.IncrementalCharge import IncrementalCharge
from commons.TransformationHelper import TransformationHelper
from pyspark.sql.types import IntegerType
import sys
import io
 
IncrementalCharge = IncrementalCharge(glueContext, spark, "-", "-")
TransformationHelper = TransformationHelper()

path_source =f"s3://alfa-datalake-dev-bronce/SIPREN/PLUS/SIPREN-SIPREN_ALFA-SNR_CAUSA/"
#IncrementalCharge.charge_tmp_tables(path_source, "SNR_CAUSA", 'Gold')
df = ExtractHelper.extract_parquet_data(path_source, "ID_CAUSAL,NOMBRE,CLASE", "2025-9-4")
df = TransformationHelper.trim_data(df)
print(df.filter(F.lower(F.col("nombre"))=="choque electrico").count())
print(df.filter(F.lower(F.col("nombre"))=="choque electrico").show())
path = f"s3://alfa-datalake-dev-assets/support_phase_files/Homologaciones_plus_vf.xlsx"
sheet = "Homologaciones_Sipren_Plus"
df_homologaciones_dos = ExtractHelper.extract_excel_data(path, sheet)

tabla_to_proccess = df_homologaciones_dos.select("tabla").distinct().rdd.flatMap(lambda x:x).collect()

df_homologaciones_principal = df_homologaciones_dos.selectExpr(
"tabla as tabla",
"campo_referencia as campo_referencia",
"valor_referencia",
"valor_homologacion",
"campo_homologacion"
)
df_homologaciones_principal = TransformationHelper.trim_data(df_homologaciones_principal)
df = df.filter(F.col('CLASE')=="CAUSA_SINIESTRO")
df = df.drop("CLASE") 
df = TransformationHelper.homologacion_i("snr_causa", df_homologaciones_principal,df_homologaciones_principal, df, 'homologacion')
print(df.filter(F.lower(F.col("nombre"))=="choque electrico").count())
print(df.filter(F.lower(F.col("nombre"))=="choque electrico").show())
df = TransformationHelper.homologacion_i("snr_causa", df_homologaciones_principal,df_homologaciones_principal, df, 'remediacion')
print(df.filter(F.lower(F.col("nombre"))=="accidentes de transito").count())
print(df.filter(F.lower(F.col("nombre"))=="accidentes de transito").show())
path = f"s3://alfa-datalake-dev-silver/SIPREN/TRANSFORMATION/SIPREN_SILVER_SNR_CAUSA/"
ExtractHelper.save_files(df,path)
from commons.StepGold import StepGold
from commons.IncrementalCharge import IncrementalCharge
from commons.TransformationHelper import TransformationHelper
from pyspark.sql.types import IntegerType
import sys
import io
 
IncrementalCharge = IncrementalCharge(glueContext, spark, "-", "-")
TransformationHelper = TransformationHelper()

path_source = "s3://alfa-datalake-dev-silver/SIPREN/TRANSFORMATION/SIPREN_SILVER_SNR_CAUSA/"
#IncrementalCharge.charge_tmp_tables(path_source, "SNR_CAUSA", 'Gold')
df = ExtractHelper.extract_parquet_data(path_source, "*", "2025-9-5")
#df = ExtractHelper.extract_parquet_data(path_source, "ID_CAUSAL,NOMBRE", "2025-9-4")
df = df.distinct()
df.select("id_causal").distinct().count()
df.groupBy("id_causal").count().show()

df.filter(F.col("id_causal")=="245").show()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, LongType, DoubleType, FloatType
from pyspark.sql import functions as F
schema_DR = StructType([
    StructField("nid_reserva", IntegerType(), True),
    StructField("nid_siniestro", IntegerType(), True),
    StructField("cod_concepto_pago_siniestro", StringType(), True),
    StructField("cod_movimiento", StringType(), True),
    StructField("vlr_reserva", IntegerType(), True)
])

DR = spark.createDataFrame([
    (1, 100, "C1", "M1", 1000)
], schema_DR)
DR.createOrReplaceGlobalTempView("spr_detalle_reserva")
schema_SR = StructType([
    StructField("nid_reserva", IntegerType(), True),
    StructField("fec_generado", StringType(), True),
    StructField("NID_SINIESTRO", StringType(), True),
    StructField("PERIODO_POINT", StringType(), True)
])
SR = spark.createDataFrame([
    (1, "2023-06-10",100,"20202")
], schema_SR)
SR.createOrReplaceGlobalTempView("spr_reserva")
schema_CP = StructType([
    StructField("cod_concepto_pago_siniestro", StringType(), True),
    StructField("descripcion", StringType(), True),
    StructField("PERIODO_POINT", StringType(), True)
])
CP = spark.createDataFrame([
    ("C1", "Incapacidad Total","20202")
], schema_CP)
CP.createOrReplaceGlobalTempView("spr_conceptos_pagos_siniestros")
schema_TM = StructType([
    StructField("cod_movimiento", StringType(), True),
    StructField("descripcion", StringType(), True)
])
TM = spark.createDataFrame([
    ("M1", "Inicial")
], schema_TM)
TM.createOrReplaceGlobalTempView("cgr_movimientos_reserva")
schema_SIN = StructType([
    StructField("nid_siniestro", IntegerType(), True),
    StructField("cod_causa_siniestro_afp", StringType(), True),
    StructField("cod_origen", StringType(), True),
    StructField("fec_siniestro", StringType(), True),
    StructField("nid_numero_identificacion", StringType(), True),
    StructField("fec_inicio_solicitud_afp", StringType(), True),
    StructField("fec_solicitud_afp", StringType(), True),
    StructField("FEC_OBJECION_ASG", StringType(), True),
    StructField("cod_tipo_identificacion", StringType(), True)
])
SIN = spark.createDataFrame([
    (100, "CA1", "OR1", "2023-06-01", "CC123", "2023-06-05", None, "2023-06-05","CDO")
], schema_SIN)
SIN.createOrReplaceGlobalTempView("spr_siniestros")
print(SIN.printSchema())
schema_CS = StructType([
    StructField("cod_causa_siniestro", StringType(), True),
    StructField("descripcion", StringType(), True)
])
CS = spark.createDataFrame([
    ("CA1", "Enfermedad comun")
], schema_CS)
CS.createOrReplaceGlobalTempView("spr_causas_siniestros")
schema_CO = StructType([
    StructField("cod_origen", StringType(), True),
    StructField("nombre", StringType(), True)
])
CO = spark.createDataFrame([
    ("OR1", "Origen Laboral")
], schema_CO)
CO.createOrReplaceGlobalTempView("cgr_origenes")
schema_P = StructType([
    StructField("nid_poliza", IntegerType(), True),
    StructField("fec_inicial_vigencia", StringType(), True),
    StructField("fec_final_vigencia", StringType(), True)
])
P = spark.createDataFrame([
    (1, "2023-01-01", "2023-12-31")
], schema_P)
P.createOrReplaceGlobalTempView("ppr_polizas")
schema_ISA = StructType([
    StructField("nid_siniestro", IntegerType(), True),
    StructField("dia_181", StringType(), True),
    StructField("NID_SOLICITUD_AUDIT", StringType(), True),
    StructField("FEC_SOLICITUD", StringType(), True)
])
ISA = spark.createDataFrame([
    (100, "2023-07-01","1234","2020-12-12")
], schema_ISA)
ISA.createOrReplaceGlobalTempView("spr_incapacidades_sol_audit")
schema_PG = StructType([
    StructField("nid_pago", IntegerType(), True),
    StructField("fec_generado", StringType(), True),
    StructField("nid_siniestro", IntegerType(), True),
    StructField("periodo_point", StringType(), True),
    StructField("FEC_SOLICITUD", StringType(), True)
])
PG = spark.createDataFrame([
    (10, "2023-06-15",100,"202012","2020-10-12")
], schema_PG)
PG.createOrReplaceGlobalTempView("spr_pagos")
schema_PS = StructType([
    StructField("nid_pago", IntegerType(), True),
    StructField("cod_concepto_pago_siniestro", StringType(), True),
    StructField("cod_movimiento", StringType(), True),
    StructField("nid_siniestro", IntegerType(), True),
    StructField("vlr_pago", IntegerType(), True)
])
PS = spark.createDataFrame([
    (10, "C1", "M1", 100, 800)
], schema_PS)
PS.createOrReplaceGlobalTempView("spr_detalle_pago_siniestro")
schema_RP = StructType([
    StructField("cod_tipo_recuperacion", StringType(), True),
    StructField("cod_movimiento", StringType(), True),
    StructField("nid_siniestro", IntegerType(), True),
    StructField("vlr_recuperacion", IntegerType(), True),
    StructField("fec_creacion_registro", StringType(), True),
    StructField("PERIODO_POINT", StringType(), True),
    StructField("nid_recuperacion", StringType(), True),
    StructField("nid_recibo_caja_point", StringType(), True)
])
RP = spark.createDataFrame([
    ("R1", "M1", 100, 400, "2023-06-20","202012","123","1234")
], schema_RP)
RP.createOrReplaceGlobalTempView("spr_recuperaciones")
schema_TR = StructType([
    StructField("cod_tipo_recuperacion", StringType(), True),
    StructField("nombre", StringType(), True)
])
TR = spark.createDataFrame([
    ("R1", "Recuperacion Judicial")
], schema_TR)
TR.createOrReplaceGlobalTempView("spr_tipos_recuperaciones")  
schema_ACU = StructType([
    StructField("nid_numero_identificacion", StringType(), True),
    StructField("cod_tipo_identificacion", StringType(), True),
    StructField("fec_nacimiento", StringType(), True),
    StructField("genero", StringType(), True)
])
ACU = spark.createDataFrame([
    ("CC123", "TT","2023-07-20", "2023-10-20")
], schema_ACU)
ACU.createOrReplaceGlobalTempView("acu_personas_naturales")  
schema_PPR = StructType([
    StructField("nid_identificacion", StringType(), True),
    StructField("cod_tipo_identificacion_afp", StringType(), True),
    StructField("fec_afiliacion_afp", StringType(), True),
    StructField("priper_cotrep", StringType(), True)
])
PPR = spark.createDataFrame([
    ("CC123", "TT","2023-07-20", "A2")
], schema_PPR)
PPR.createOrReplaceGlobalTempView("ppr_afiliados")  
schema_H2 = StructType([
    StructField("NID_SINIESTRO", StringType(), True),
    StructField("FEC_MODIFICACION", StringType(), True),
    StructField("COD_ORIGEN_SINIESTRO_ANT", StringType(), True),
    StructField("COD_ORIGEN_SINIESTRO_NEW", StringType(), True),
    StructField("NID_SINIESTRO_ORIGEN_HIST", StringType(), True)
])
H2 = spark.createDataFrame([
    (100, "2023-07-20", "001","002","123")
], schema_H2)
H2.createOrReplaceGlobalTempView("Spr_siniestros_origenes_hist")  
schema_DT_R = StructType([
    StructField("nid_recuperacion", StringType(), True),
    StructField("vlr_concepto", StringType(), True)
])
DT_R = spark.createDataFrame([
    (100, "1000")
], schema_DT_R)
DT_R.createOrReplaceGlobalTempView("spr_detalle_recuperaciones")  
from commons.StepGold import StepGold
from commons.StepGoldPlus import StepGoldPlus
from commons.StepGoldBodega import StepGoldBodega
from pyspark.sql.window import Window
from commons.EstadoSiniestroHelperA import EstadoSiniestroHelperA
StepGoldPlus = StepGoldPlus(glueContext, spark)
StepGoldBodega = StepGoldBodega(glueContext, spark)
StepGold = StepGold(glueContext, spark)
EstadoSiniestro_Helper = EstadoSiniestroHelperA(glueContext, spark)

df_tierra = StepGold.create_component()

path = f"s3://alfa-datalake-dev-assets/FS_files/BODEGA_PLUS_SILVER/BODEGA_SILVER.xlsx"
sheet = "BODEGA_SILVER"
df_tables = ExtractHelper.extract_excel_data(path, sheet)
df_tables.printSchema()
from pyspark.sql import functions as F

columnas_clean = ["nid_siniestro", "periodo_point", "cod_movimiento"]
df_tables_2 = df_tables
for columna in columnas_clean:
    df_tables_2 = df_tables_2.withColumn(columna, 
                                       F.regexp_replace(F.col(columna), r"\.0+$", "")
                                      )
df_tables.show(2)
df_tables_2.show(2)
df_claimriskunit = spark.table("global_temp.CLAIMRISKUNIT").alias("CRU")
df_claimriskunit.limit(10).coalesce(1).write.mode("overwrite").csv("s3://alfa-datalake-dev-assets/PYTEST_FILES/claimriskunit/", header=True)
df_claiminsuranceobject = spark.table("global_temp.CLAIMINSURANCEOBJECT").alias("CIO")
df_claiminsuranceobject.limit(10).coalesce(1).write.mode("overwrite").csv("s3://alfa-datalake-dev-assets/PYTEST_FILES/claiminsuranceobject/", header=True)
df_stpo_insobjparticipation = spark.table("global_temp.STPO_INSOBJPARTICIPATION").alias("IOP")
df_stpo_insobjparticipation.limit(10).coalesce(1).write.mode("overwrite").csv("s3://alfa-datalake-dev-assets/PYTEST_FILES/stpo_insobjparticipation/", header=True)
df_prethirdparty = spark.table("global_temp.PRETHIRDPARTY").alias("PRE")
df_prethirdparty.limit(10).coalesce(1).write.mode("overwrite").csv("s3://alfa-datalake-dev-assets/PYTEST_FILES/prethirdparty/", header=True)
#df_thirdpartyrole = spark.table("global_temp.thirdpartyrole").alias("tpr")
#df_thirdpartyrole.limit(10).coalesce(1).write.mode("overwrite").csv("s3://alfa-datalake-dev-assets/PYTEST_FILES/thirdpartyrole/", header=True)
#df_sst_role = spark.table("global_temp.stts_role").alias("str")
#df_sst_role.limit(10).coalesce(1).write.mode("overwrite").csv("s3://alfa-datalake-dev-assets/PYTEST_FILES/sst_role/", header=True)
df_remuertevdeudor = spark.table("global_temp.REMUERTEVDEUDOR").select(
        F.col("FECHANACIMIENTOASEGINPUT"),
        F.col("GENEROASEGURADOINPUT"),
        F.col("pk")
    ).alias("RMS")
df_remuertevdeudor.limit(10).coalesce(1).write.mode("overwrite").csv("s3://alfa-datalake-dev-assets/PYTEST_FILES/remuertevdeudor/", header=True)
df_reincapacidadtotperman = spark.table("global_temp.REINCAPACIDADTOTPERMAN").select(
    F.col("FECHANACIMIENTOASEGINPUT"),
    F.col("GENEROASEGURADOINPUT"),
    F.col("pk")
).alias("RMS2")
df_reincapacidadtotperman.limit(10).coalesce(1).write.mode("overwrite").csv("s3://alfa-datalake-dev-assets/PYTEST_FILES/reincapacidadtotperman/", header=True)
df_reauxiliofunerariovdeudor = spark.table("global_temp.REAUXILIOFUNERARIOVDEUDOR").select(
    F.col("FECHANACIMIENTOASEGINPUT"),
    F.col("GENEROASEGURADOINPUT"),
    F.col("pk")
).alias("RMS3")
df_reauxiliofunerariovdeudor.limit(10).coalesce(1).write.mode("overwrite").csv("s3://alfa-datalake-dev-assets/PYTEST_FILES/reauxiliofunerariovdeudor/", header=True)
df_reenfermedadesgvdeudor = spark.table("global_temp.REENFERMEDADESGVDEUDOR").select(
    F.col("FECHANACIMIENTOASEGINPUT"),
    F.col("GENEROASEGURADOINPUT"),
    F.col("pk")
).alias("RMS4")
df_reenfermedadesgvdeudor.limit(10).coalesce(1).write.mode("overwrite").csv("s3://alfa-datalake-dev-assets/PYTEST_FILES/reenfermedadesgvdeudor/", header=True)
DETALLE_RESERVA = "global_temp.spr_detalle_reserva"
DETALLE_RESERVA = spark.table(DETALLE_RESERVA)
DETALLE_RESERVA.limit(100).coalesce(1).write.mode("overwrite").csv(f"s3://alfa-datalake-dev-assets/PYTEST_FILES/DETALLE_RESERVA/", header=True)
ACU_PERSONAS_NATURALES = "global_temp.acu_personas_naturales"
ACU_PERSONAS_NATURALES = spark.table(ACU_PERSONAS_NATURALES)
ACU_PERSONAS_NATURALES.limit(100).coalesce(1).write.mode("overwrite").csv(f"s3://alfa-datalake-dev-assets/PYTEST_FILES/ACU_PERSONAS_NATURALES/", header=True)
RESERVA = "global_temp.spr_reserva"
RESERVA = spark.table(RESERVA)
RESERVA.limit(100).coalesce(1).write.mode("overwrite").csv(f"s3://alfa-datalake-dev-assets/PYTEST_FILES/RESERVA/", header=True)
CONCEPTOS_PAGOS =  "global_temp.spr_conceptos_pagos_siniestros"
CONCEPTOS_PAGOS = spark.table(CONCEPTOS_PAGOS)
CONCEPTOS_PAGOS.limit(100).coalesce(1).write.mode("overwrite").csv(f"s3://alfa-datalake-dev-assets/PYTEST_FILES/CONCEPTOS_PAGOS/", header=True)
CGR_MOVIMIENTO_RESERVA = "global_temp.cgr_movimientos_reserva"
CGR_MOVIMIENTO_RESERVA = spark.table(CGR_MOVIMIENTO_RESERVA)
CGR_MOVIMIENTO_RESERVA.limit(100).coalesce(1).write.mode("overwrite").csv(f"s3://alfa-datalake-dev-assets/PYTEST_FILES/CGR_MOVIMIENTO_RESERVA/", header=True)
SPR_SINIESTROS = "global_temp.spr_siniestros"
SPR_SINIESTROS = spark.table(SPR_SINIESTROS)
SPR_SINIESTROS.limit(100).coalesce(1).write.mode("overwrite").csv(f"s3://alfa-datalake-dev-assets/PYTEST_FILES/SPR_SINIESTROS/", header=True)
SPR_CAUSAS_SINIESTROS = "global_temp.spr_causas_siniestros"
SPR_CAUSAS_SINIESTROS = spark.table(SPR_CAUSAS_SINIESTROS)
SPR_CAUSAS_SINIESTROS.limit(100).coalesce(1).write.mode("overwrite").csv(f"s3://alfa-datalake-dev-assets/PYTEST_FILES/SPR_CAUSAS_SINIESTROS/", header=True)
CGR_ORIGENES = "global_temp.cgr_origenes"
CGR_ORIGENES = spark.table(CGR_ORIGENES)
CGR_ORIGENES.limit(100).coalesce(1).write.mode("overwrite").csv(f"s3://alfa-datalake-dev-assets/PYTEST_FILES/CGR_ORIGENES/", header=True)
PPR_POLIZAS = "global_temp.ppr_polizas"
PPR_POLIZAS = spark.table(PPR_POLIZAS)
PPR_POLIZAS.limit(100).coalesce(1).write.mode("overwrite").csv(f"s3://alfa-datalake-dev-assets/PYTEST_FILES/PPR_POLIZAS/", header=True)
INCAPACIDADES = "global_temp.spr_incapacidades_sol_audit"
INCAPACIDADES = spark.table(INCAPACIDADES)
INCAPACIDADES.limit(100).coalesce(1).write.mode("overwrite").csv(f"s3://alfa-datalake-dev-assets/PYTEST_FILES/INCAPACIDADES/", header=True)
DETALLE_PAGO ="global_temp.spr_detalle_pago_siniestro"
DETALLE_PAGO = spark.table(DETALLE_PAGO)
DETALLE_PAGO.limit(100).coalesce(1).write.mode("overwrite").csv(f"s3://alfa-datalake-dev-assets/PYTEST_FILES/DETALLE_PAGO/", header=True)
SPR_PAGOS ="global_temp.spr_pagos"
SPR_PAGOS = spark.table(SPR_PAGOS)
SPR_PAGOS.limit(100).coalesce(1).write.mode("overwrite").csv(f"s3://alfa-datalake-dev-assets/PYTEST_FILES/SPR_PAGOS/", header=True)
PPR_AFILIADO  ="global_temp.ppr_afiliados"
PPR_AFILIADO = spark.table(PPR_AFILIADO)
PPR_AFILIADO.limit(100).coalesce(1).write.mode("overwrite").csv(f"s3://alfa-datalake-dev-assets/PYTEST_FILES/PPR_AFILIADO/", header=True)
TIPOS_RECUPERACIONES ="global_temp.spr_tipos_recuperaciones"
TIPOS_RECUPERACIONES = spark.table(TIPOS_RECUPERACIONES)
TIPOS_RECUPERACIONES.limit(100).coalesce(1).write.mode("overwrite").csv(f"s3://alfa-datalake-dev-assets/PYTEST_FILES/TIPOS_RECUPERACIONES/", header=True)
DETALLE_RECUPERACIONES ="global_temp.spr_detalle_recuperaciones"
DETALLE_RECUPERACIONES = spark.table(DETALLE_RECUPERACIONES)
DETALLE_RECUPERACIONES.limit(100).coalesce(1).write.mode("overwrite").csv(f"s3://alfa-datalake-dev-assets/PYTEST_FILES/DETALLE_RECUPERACIONES/", header=True)
ORIGENES_HIST = "global_temp.Spr_siniestros_origenes_hist"
ORIGENES_HIST = spark.table(ORIGENES_HIST)
ORIGENES_HIST.limit(100).coalesce(1).write.mode("overwrite").csv(f"s3://alfa-datalake-dev-assets/PYTEST_FILES/ORIGENES_HIST/", header=True)
SPR_RECUPERACIONES = "global_temp.spr_recuperaciones"
SPR_RECUPERACIONES = spark.table(SPR_RECUPERACIONES)
SPR_RECUPERACIONES.limit(100).coalesce(1).write.mode("overwrite").csv(f"s3://alfa-datalake-dev-assets/PYTEST_FILES/SPR_RECUPERACIONES/", header=True)
job.commit()