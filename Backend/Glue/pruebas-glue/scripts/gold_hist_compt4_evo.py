import sys
import traceback
import compt3.src.utils.common.df_method as df_mtd
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from datetime import datetime, timedelta
from commons.ExtractHelper import ExtractHelper
from pyspark.sql.types import DecimalType, StringType, IntegerType, BooleanType, FloatType, DoubleType, StructType, StructField
from commons.TransformationHelper import TransformationHelper
from commons.ClaimHelper import ClaimHelper
from commons.EstadoSiniestroHelperA import EstadoSiniestroHelperA
from commons.IncrementalCharge import IncrementalCharge
from commons.DocumentoHelper import DocumentoHelper
from pyspark.sql import functions as F
from step_gold.orchestrator_gold import main
from compt3.orchestrator.orchestrator_gold_dane import orchestrator_gold_dane
from commons.StepGold import StepGold
from commons.StepGoldPlus import StepGoldPlus
from commons.StepGoldBodega import StepGoldBodega
from commons.GoldTransformation import GoldTransformation
from commons.FechaObjHelper import FechaObjHelper
import re
from pyspark.sql.window import Window
 
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME_SILVER', 'BUCKET_NAME_GOLD','BUCKET_NAME_ASSETS','YEAR_USE','MONTH_USE'])

NM_BUCKET_ASSETS = args['BUCKET_NAME_ASSETS']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")
sc.setCheckpointDir("s3://alfa-datalake-qa-assets/glue_gold_tmp/")

BUCKET_NAME_SILVER = args['BUCKET_NAME_SILVER']
BUCKET_NAME_GOLD = args['BUCKET_NAME_GOLD']
YEAR_USE = args['YEAR_USE']
MONTH_USE = args['MONTH_USE']

ExtractHelper = ExtractHelper(glueContext, spark, "-", "-")
IncrementalCharge = IncrementalCharge(glueContext, spark, "-", "-")
TransformationHelper = TransformationHelper()
ClaimHelper = ClaimHelper(glueContext, spark)
GoldTransformation = GoldTransformation(glueContext, spark, 'None')
DocumentoHelper = DocumentoHelper(glueContext, spark)
FechaObj_helper = FechaObjHelper(glueContext, spark) 
EstadoSiniestro_Helper = EstadoSiniestroHelperA(glueContext, spark)
StepGold = StepGold(glueContext, spark)
StepGoldPlus = StepGoldPlus(glueContext, spark)
StepGoldBodega = StepGoldBodega(glueContext, spark)

path = f"s3://{NM_BUCKET_ASSETS}/support_phase_files/procces_ola1.xlsx"
sheet = "gold_phase"
df_tables = ExtractHelper.extract_excel_data(path, sheet)
sheet = "gold_tables"
df_tmp_tables = ExtractHelper.extract_excel_data(path, sheet)

try:
    mes_uso = 11
    
    for row in df_tmp_tables.collect():
        table = row["Table"].upper()
        if row["Phase"].upper() == "SILVER":
            path_source =f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}_SILVER_"
        else:
            path_source =f"s3://{BUCKET_NAME_GOLD}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-CARACT_SINIESTROS-"
        IncrementalCharge.charge_tmp_tables(path_source, table, 'Gold')
                    
        
    df_tierra = StepGold.create_component()
    try:
        df_tierra = df_tierra.checkpoint(eager= True)
        print("cree el checkpoitn de tierra")
    except Exception as e:
        print(e)
        
    df_plus = StepGoldPlus.create_component()
    df_plus = df_plus.filter(~F.col("fecha_transaccion").isNull())
    try:
        df_plus = df_plus.checkpoint(eager= True)
        print("cree el checkpoitn de plus")
    except Exception as e:
        print(e)
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
    df_estado_plus = df_movimientos_plus.join(df_movimientos_dim, F.col("id_tipo_transaccion_pk") == F.col("id_tipo_transaccion_fk"), "inner")
    
    df_estado_plus = df_estado_plus.select(
            F.col("id_siniestro_fk"),
            F.col("fecha_transaccion"),
            F.col("tipo_de_transaccion").alias("TIPO_TRANSACCION"),#RA
            F.col("id_fuente_fk"),#id_fuente
            F.col("FECHA_OBJECION"),#null
            F.col("valor_transaccion_ajustado").alias("VALOR_TRANSACCION"),#monto
            F.lit("").alias("fundamento_no_pago")
        )
        
    df_fact_estado = df_estado_plus.withColumn(
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
        ).otherwise(F.col("VALOR_TRANSACCION")).alias("valor_transaccion_ajustado")
    )
    df_fact_estado = df_fact_estado.filter(
        ~F.lower(F.col("tipo_transaccion")).isin(["recobro", "reversion del recupero"])
    )
    
    path_estado = f"s3://{BUCKET_NAME_GOLD}/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-FACT_ESTADO_SINIESTRO/"
    df_estado_mv = df_fact_estado.filter(F.col("id_fuente_fk").isin("2","3"))
    df_estado = df_estado_mv.filter(F.year(F.col("fecha_transaccion"))<2021)
    df_estado = EstadoSiniestro_Helper.create_estado_siniestro(df_estado, "S") 
    path = path_estado
    window = Window.partitionBy("id_siniestro_fk","mes","ano","estado").orderBy(F.col("id_siniestro_fk"))
    df_estado = df_estado.withColumn(
        "row_num",
        F.row_number().over(window)
        ).filter(
            (F.col("estado") != "En Reserva") |
            ((F.col("estado") == "En Reserva") & (F.col("row_num") == 1))
        ).drop("row_num")
    ExtractHelper.save_files(df_estado,path) # cargue primer parte del historico de SIPREN (tierra/plus)
    df_estado_ant = TransformationHelper.create_reserv_mov(df_estado)# Convertirala en estado Anterior (MOV)
    df_estado = df_estado_mv.filter(F.year(F.col("fecha_transaccion"))>=2021)
    df_estado = df_estado.unionAll(df_estado_ant)#Se unen los movimientos
    df_estado = EstadoSiniestro_Helper.create_estado_siniestro(df_estado) 
    df_estado_2021 = df_estado.filter((F.col("ano") < YEAR_USE))
    df_estado_2025 = df_estado.filter(F.col("ano") == YEAR_USE)
    df_estado_2025 = df_estado_2025.filter(F.col("mes") < MONTH_USE)
    df_estado = df_estado_2021.unionByName(df_estado_2025)
    path = path_estado
    ExtractHelper.save_files(df_estado,path)
    
except Exception as e:
    print(traceback.format_exc())
    raise e
