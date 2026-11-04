import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from utils import Utils
from mpv2_transformer_proccessor import DataCleanupProcessor
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
utils_instance = Utils()
# load stg data
s3_path_silver = "s3://alfa-datalake-dev-silver/POLIZAS_PRODUCTO/ALFAPRD/POLIZAS_ALFAPRD_SILVER/"
s3_path_stg = "s3://alfa-datalake-dev-silver/POLIZAS_PRODUCTO/ALFAPRD/ALFAPRD_POLIZAS_STG/"
df_policies_alfaprd_stg = utils_instance.read_full_delta(spark, s3_path_stg)
df_policies_alfprd_silver = utils_instance.read_full_delta(spark, s3_path_silver)
from pyspark.sql import functions as F
df_policies_alfaprd_stg.printSchema()
# Staging
df_policies_alfaprd_stg.groupBy("cod_sucursal").agg(
    F.count("*").alias("total_registros")
).show()
# Silver
df_policies_alfprd_silver.groupBy("cod_sucursal").agg(
    F.count("*").alias("total_registros")
).show()
processor = DataCleanupProcessor(spark, glueContext)
# Check Before Homologation
df_policies_alfaprd_stg.groupBy("linea_credito").agg(
    F.count("*").alias("total_registros")
).show()
# apply homologation
df_homo_stg_policies = processor.clean_alfaprd_policies_data(df_policies_alfaprd_stg)
df_homo_stg_policies.groupBy("linea_credito").agg(
    F.count("*").alias("total_registros")
).show()
from cleanup import CleanUp
import pandas as pd
cleanup = CleanUp()
def load_homologation_data(s3_homologation_path, homologation_sheet):
    """Load and process homologation data from Excel."""
    path = s3_homologation_path
    sheet = homologation_sheet
    
    df_xl = pd.read_excel(path, sheet_name=sheet)
    df_xl = df_xl.where(pd.notnull(df_xl), None)
    
    for col_name in df_xl.columns:
        df_xl[col_name] = df_xl[col_name].apply(
            lambda x: str(x) if x is not None else None
        )
    
    df_homologaciones = spark.createDataFrame(df_xl)
    
    df_homologaciones = df_homologaciones.selectExpr(
        "tabla as tabla",
        "campo_referencia as campo_referencia",
        """
        trim(
          translate(
            lower(trim(valor_referencia)),
            'ÁÀÄÂÃÅáàäâãåÉÈËÊéèëêÍÌÏÎíìïîÓÒÖÔÕóòöôõÚÙÜÛúùüûÑñÇç',
            'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuNnCc'
          )
        ) as valor_referencia
        """,
        "valor_homologacion",
        "campo_homologacion",
        """
        CASE 
            WHEN cod_fuente_fk IS NULL THEN NULL     
            WHEN LOWER(TRIM(cod_fuente_fk)) = 'nan' THEN NULL    
            WHEN cod_fuente_fk LIKE '%.0' THEN REGEXP_REPLACE(cod_fuente_fk, '\\.0$', '')       
            ELSE cod_fuente_fk 
        END as cod_fuente_fk
        """
    )
    
    df_cod_tipo = df_homologaciones.filter(
        col("campo_referencia") == "cod_tipo_identificacion"
    )
    df_otros = df_homologaciones.filter(
        col("campo_referencia") != "cod_tipo_identificacion"
    )
    
    df_cod_tipo = cleanup.normalize_columns(df_cod_tipo, "valor_referencia")
    print(f"Homologations loades successfully from {path}\nSheet Used: {sheet}")
    return df_cod_tipo.unionByName(df_otros)
from pyspark.sql.functions import (
    col, split, when, size, trim, concat_ws,
    length, regexp_replace, upper, coalesce, lit,initcap, expr,date_format
)
s3_homologation_path = 's3://alfa-datalake-dev-clientes-assets/phase_silver/definitions/homologaciones_polizas.xlsx'
homologation_sheet = 'homologaciones_polizas'
df_homo = load_homologation_data(s3_homologation_path,homologation_sheet)
field_filters = ['linea_credito','canal_venta']
table_filter = 'polizas_alfaprd_silver'
df_homo.select("tabla","campo_referencia","valor_referencia").filter( (col("campo_referencia").isin(field_filters)) & (col("tabla") == table_filter)).show(truncate=False)
job.commit()