import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
#----------CUSTOM LIBRARIES -----------#
from acsele_staging_processor import AcseleStagingProcessor
from alfaprd_staging_processor import AlfprdStagingProcessor
from cardiff_staging_processor import CardiffStagingProcessor
from s3_handler import S3Handler
from utils import Utils
import os

  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
from pyspark.sql.functions import col
BUCKET_BRONZE='alfa-datalake-dev-bronce'
BUCKET_SILVER='alfa-datalake-dev-silver'
alfprd_processor = AlfprdStagingProcessor(spark,glueContext,BUCKET_BRONZE,BUCKET_SILVER)
table_paths = alfprd_processor._get_alfaprd_table_paths()
df_alfaprd_general_acu_personas_naturales = alfprd_processor.read_full_parquet(s3_target_path=table_paths["general_acu_personas_naturales"])
df_alfaprd_general_acu_personas_juridicas = alfprd_processor.read_full_parquet(s3_target_path=table_paths["general_acu_personas_juridicas"])
df_alfaprd_general_acu_tipos_empresa = alfprd_processor.read_full_parquet(s3_target_path=table_paths["general_acu_tipos_empresa"])
df_alfaprd_general_cgr_cuidades_departamentos = alfprd_processor.read_full_parquet(s3_target_path=table_paths["general_cgr_cuidades_departamentos"])
df_alfaprd_general_acu_niveles_educativos = alfprd_processor.read_full_parquet(s3_target_path=table_paths["general_acu_niveles_educativos"])
df_alfaprd_general_acu_profesiones = alfprd_processor.read_full_parquet(s3_target_path=table_paths["general_acu_profesiones"])
df_alfaprd_arpprod_par_afiliaciones = alfprd_processor.read_full_parquet(s3_target_path=table_paths["arpprod_par_afiliaciones"])
df_alfaprd_arpprod_par_trabajadores = alfprd_processor.read_full_parquet(s3_target_path=table_paths["arpprod_par_trabajadores"])
df_alfaprd_general_acu_personas_localizaciones = alfprd_processor.read_full_parquet(s3_target_path=table_paths["general_acu_personas_localizaciones"])
df_alfaprd_general_cgr_departamanetos = alfprd_processor.read_full_parquet(s3_target_path=table_paths["general_cgr_departamanetos"])
df_alfaprd_general_cgr_paises = alfprd_processor.read_full_parquet(s3_target_path=table_paths["general_cgr_paises"])
df_alfaprd_general_cgr_ocupaciones = alfprd_processor.read_full_parquet(s3_target_path=table_paths["general_cgr_ocupaciones"])
df_alfaprd_general_cgr_estados_civiles = alfprd_processor.read_full_parquet(s3_target_path=table_paths["general_cgr_estados_civiles"])
df_alfaprd_general_acu_actividades_economica_ciiu = alfprd_processor.read_full_parquet(s3_target_path=table_paths["general_cgr_act_eco_ciuu"])
df_alfaprd_general_cgr_tipos_identificaciones  = alfprd_processor.read_full_parquet(s3_target_path=table_paths["general_cgr_tipos_id"])
df_general_cgr_estados = alfprd_processor.read_full_parquet(s3_target_path=table_paths["general_cgr_estados"])
df_alfprd_pn_datos_basicos = alfprd_processor.process_personas_naturales_datos_basicos(
                df_alfaprd_general_acu_personas_naturales, df_alfaprd_general_cgr_tipos_identificaciones, 
                df_alfaprd_general_cgr_cuidades_departamentos, df_alfaprd_general_cgr_departamanetos,
                df_general_cgr_estados
)

df_alfprd_pj_datos_basicos = alfprd_processor.process_personas_juridicas_datos_basicos(
     df_alfaprd_general_acu_personas_juridicas,
     df_alfaprd_general_cgr_tipos_identificaciones,
     df_general_cgr_estados,
     df_alfaprd_general_acu_tipos_empresa
)
df_alfprd_pn_demograficos = alfprd_processor.process_personas_naturales_demograficos(
     df_alfaprd_general_acu_personas_naturales,
     df_alfaprd_general_cgr_tipos_identificaciones,
     df_alfaprd_general_cgr_cuidades_departamentos,
     df_alfaprd_general_cgr_departamanetos,
     df_alfaprd_general_cgr_paises,
     df_alfaprd_general_cgr_estados_civiles,
     df_alfaprd_general_acu_niveles_educativos,
     df_alfaprd_general_acu_actividades_economica_ciiu,
     df_alfaprd_general_cgr_ocupaciones,
     df_alfaprd_general_acu_profesiones,
     df_alfaprd_arpprod_par_afiliaciones,
     df_alfaprd_arpprod_par_trabajadores,
     df_alfaprd_general_acu_personas_juridicas
)

df_alfprd_pj_demograficos = alfprd_processor.process_personas_juridicas_demograficos(
     df_alfaprd_general_acu_personas_juridicas,
     df_alfaprd_general_cgr_tipos_identificaciones,
     df_alfaprd_general_cgr_cuidades_departamentos,
     df_alfaprd_general_cgr_departamanetos,
     df_alfaprd_general_cgr_paises,
     df_alfaprd_general_acu_actividades_economica_ciiu
)
df_alfprd_pn_contactabilidad = alfprd_processor.process_personas_naturales_contactabilidad(
     df_alfaprd_general_acu_personas_naturales,
     df_alfaprd_general_acu_personas_localizaciones,
     df_alfaprd_general_cgr_cuidades_departamentos,
     df_alfaprd_general_cgr_departamanetos,
     df_alfaprd_general_cgr_paises
)
df_alfprd_pj_contactabilidad = alfprd_processor.process_personas_juridicas_contactabilidad(
     df_alfaprd_general_acu_personas_juridicas,
     df_alfaprd_general_acu_personas_localizaciones,
     df_alfaprd_general_cgr_cuidades_departamentos,
     df_alfaprd_general_cgr_departamanetos,
     df_alfaprd_general_cgr_paises
)
df_alfaprd_datos_basicos_stg = alfprd_processor.consolidate_datos_basicos_alfaprd_data(df_alfprd_pn_datos_basicos, df_alfprd_pj_datos_basicos)
df_alfaprd_datos_sociodemograficos_stg = alfprd_processor.consolidate_datos_sociodemograficos_alfaprd_data(df_alfprd_pn_demograficos, df_alfprd_pj_demograficos)
df_alfaprd_datos_contacto_stg = alfprd_processor.consolidate_datos_contactabilidad_alfaprd_data(df_alfprd_pn_contactabilidad, df_alfprd_pj_contactabilidad)
df_alfaprd_db = df_alfaprd_datos_basicos_stg.select(col("ciudad_expedicion_documento")).distinct()
df_alfraprd_so = df_alfaprd_datos_sociodemograficos_stg.select(col("cod_ciudad_nacimiento")).distinct()
df_alfraprd_con = df_alfaprd_datos_contacto_stg.select(col("cod_ciudad")).distinct()
df_alfaprd_db.show(3, truncate=False)
df_alfraprd_so.show(3, truncate=False)
df_alfraprd_con.show(3, truncate=False)
#def __init__(self, spark_session, glue_context, bucket_bronze: str, bucket_silver: str):
dyf = glueContext.create_dynamic_frame.from_catalog(database='database_name', table_name='table_name')
dyf.printSchema()
df = dyf.toDF()
df.show()
import matplotlib.pyplot as plt

# Set X-axis and Y-axis values
x = [5, 2, 8, 4, 9]
y = [10, 4, 8, 5, 2]
  
# Create a bar chart 
plt.bar(x, y)
  
# Show the plot
s3output = glueContext.getSink(
  path="s3://bucket_name/folder_name",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase="demo", catalogTableName="populations"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(DyF)
job.commit()