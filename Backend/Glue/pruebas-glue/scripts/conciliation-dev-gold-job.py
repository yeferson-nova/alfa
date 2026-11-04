"""Script that load bronce phase"""
import sys, boto3, traceback

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import datacompy
##from awsglue.job import Job
##from awsglue import DynamicFrame
from commons.LogsHelper import LogsHelper
from pyspark.sql import functions as F
from pyspark.sql.functions import col, date_format ##, coalesce
from pyspark.sql.functions import (lit, year, month, sum as spark_sum)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
##job = Job(glueContext)
logs = LogsHelper()
logger = logs.logger_conf()
sns_client = boto3.client('sns',region_name = 'us-east-1') # reemplazar después

args = getResolvedOptions(
    sys.argv, 
        [
        'DB_NAME_CAL_SIL',
        'DB_NAME_CAL_GOLD',
        'BUCKET_NAME_GOLD',
        ]
    )
DB_NAME_CAL_SIL = args['DB_NAME_CAL_SIL']
DB_NAME_CAL_GOLD = args['DB_NAME_CAL_GOLD']
BUCKET_NAME_GOLD = args['BUCKET_NAME_GOLD']

#Clase con caché de DataFrames
class CatalogReader:

    def __init__(self, glue_context, database):
        self.glue_context = glue_context
        self.database = database
        self._cache = {}

    def read(self, table_name):
        if table_name not in self._cache:
            self._cache[table_name] = (
                self.glue_context.create_dynamic_frame.from_catalog(
                    database=self.database,
                    table_name=table_name
                ).toDF()
            )

        return self._cache[table_name]


"""Inicio de proceso comparativo"""

try:
    ##creamos el objeto de la clase para cada catalogo a usar
    gold = CatalogReader(glue_context=glueContext,database="datalake-dev-gold-catalog-db")
    bronce = CatalogReader(glue_context=glueContext,database="bronce_incremental")
    
    ##gold_caract_siniestros_fact_movimiento_view
    df_vw = gol.read("gold_caract_siniestros_fact_movimiento_view")
    logger.info("Mostradas primeras 10 filas de gold_caract_siniestros_fact_movimiento_view")
    df_vw.show(10, truncate=False)
    
    # consultar tabla ibnr origen
    tabla_ibnr = bronce.read("ibnr_202511")
    tabla_ibnr.show(20, truncate=False)
    logger.info("Mostradas primeras 20 filas de ibnr_202511")           

    #resto de tablas
    df_poliza = gol.read("gold_caract_siniestros_fact_poliza_pf")
    df_tipo_transaccion = gol.read("gold_caract_siniestros_dim_tipo_transaccion")
    df_fuente = gol.read("gold_caract_siniestros_dim_fuente")

    df_resultado = (
        df_vw.alias("vw")
        .join(
            df_poliza.alias("pl"),
            col("vw.id_poliza_pk") == col("pl.id_poliza_pk"),
            "inner"
        )
        .join(
            df_tipo_transaccion.alias("dtt"),
            col("vw.id_tipo_transaccion_fk").cast("decimal(38,0)") == col("dtt.id_tipo_transaccion_pk"),
            "inner"
        )
        .join(
            df_fuente.alias("fue"),
            col("vw.id_fuente_fk").cast("decimal(38,0)") == col("fue.id_fuente"),
            "left"
        )
        .filter(
            (col("fue.desc_fuente") == "Acsele") &
            (month(col("vw.Fecha de Transaccion")) == 11) &
            (year(col("vw.Fecha de Transaccion")) == 2025)
        )
        .groupBy(
            date_format(col("vw.Fecha de Transaccion"), "yyyyMM").alias("PERIODO"),
            col("pl.compania"),
            col("dtt.tipo_de_transaccion"),
            col("fue.desc_fuente")
        )
        .agg(
            spark_sum(
                col("vw.valor_transaccion_ajustado").cast("bigint")
            ).alias("valor_transaccion_ajustado")
        )
        .orderBy(
            col("compania"),
            col("tipo_de_transaccion")
        )
    )
    df_resultado.show()
    print(f"Registros: {df.count():,}")
    df_resultado.show(10, truncate=False)


except Exception as e:
    logger.error(f"ERROR WITH Evaluate error ATRIBUTO: {traceback.format_exc()}")  
                    

