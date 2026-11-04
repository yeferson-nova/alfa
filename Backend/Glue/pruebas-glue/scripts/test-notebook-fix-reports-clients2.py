## FIRST CELL
### SECOND CELL
#------ AWS GLUE ---------#
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
#-------SPARK--------------#
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
#-----CUSTOM IMPORTS -----------#
from alfaprd_staging_processor import AlfprdStagingProcessor
from s3_handler import S3Handler
from utils import Utils
#------PYTHON IMPORTS -------#
import os
import sys

  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
### THIRD CELL
# Import Libraries
BUCKET_BRONCE = 'alfa-datalake-dev-bronce'
BUCKET_SILVER = 'alfa-datalake-dev-silver'
processor = AlfprdStagingProcessor(spark, glueContext, BUCKET_BRONCE, BUCKET_SILVER)
### FOUR CELL
table_paths = processor._get_alfaprd_table_paths()
df_alfaprd_general_acu_personas_naturales = processor.utils.read_full_parquet(spark,s3_target_path=table_paths["general_acu_personas_naturales"])
df_alfaprd_general_acu_personas_juridicas = processor.utils.read_full_parquet(spark,s3_target_path=table_paths["general_acu_personas_juridicas"])
df_alfaprd_general_acu_tipos_empresa = processor.utils.read_full_parquet(spark,s3_target_path=table_paths["general_acu_tipos_empresa"])
df_alfaprd_general_cgr_cuidades_departamentos = processor.utils.read_full_parquet(spark,s3_target_path=table_paths["general_cgr_cuidades_departamentos"])
df_alfaprd_general_acu_niveles_educativos = processor.utils.read_full_parquet(spark,s3_target_path=table_paths["general_acu_niveles_educativos"])
df_alfaprd_general_acu_profesiones = processor.utils.read_full_parquet(spark,s3_target_path=table_paths["general_acu_profesiones"])
df_alfaprd_arpprod_par_afiliaciones = processor.utils.read_full_parquet(spark,s3_target_path=table_paths["arpprod_par_afiliaciones"])
df_alfaprd_arpprod_par_trabajadores = processor.utils.read_full_parquet(spark,s3_target_path=table_paths["arpprod_par_trabajadores"])
df_alfaprd_general_acu_personas_localizaciones = processor.utils.read_full_parquet(spark,s3_target_path=table_paths["general_acu_personas_localizaciones"])
df_alfaprd_general_cgr_departamanetos = processor.utils.read_full_parquet(spark,s3_target_path=table_paths["general_cgr_departamanetos"])
df_alfaprd_general_cgr_paises = processor.utils.read_full_parquet(spark,s3_target_path=table_paths["general_cgr_paises"])
df_alfaprd_general_cgr_ocupaciones = processor.utils.read_full_parquet(spark,s3_target_path=table_paths["general_cgr_ocupaciones"])
df_alfaprd_general_cgr_estados_civiles = processor.utils.read_full_parquet(spark,s3_target_path=table_paths["general_cgr_estados_civiles"])
df_alfaprd_general_acu_actividades_economica_ciiu = processor.utils.read_full_parquet(spark,s3_target_path=table_paths["general_cgr_act_eco_ciuu"])
df_alfaprd_general_cgr_tipos_identificaciones  = processor.utils.read_full_parquet(spark,s3_target_path=table_paths["general_cgr_tipos_id"])
df_general_cgr_estados = processor.utils.read_full_parquet(spark,s3_target_path=table_paths["general_cgr_estados"])
### FIVE CELL
df_alfprd_pn_datos_basicos = processor.process_personas_naturales_datos_basicos(
                df_alfaprd_general_acu_personas_naturales, df_alfaprd_general_cgr_tipos_identificaciones, 
                df_alfaprd_general_cgr_cuidades_departamentos, df_alfaprd_general_cgr_departamanetos,
                df_general_cgr_estados
           )
df_alfprd_pj_datos_basicos = processor.process_personas_juridicas_datos_basicos(
                df_alfaprd_general_acu_personas_juridicas,
                df_alfaprd_general_cgr_tipos_identificaciones,
                df_general_cgr_estados,
                df_alfaprd_general_acu_tipos_empresa
)
df_alfaprd_datos_basicos_stg = processor.consolidate_datos_basicos_alfaprd_data(df_alfprd_pn_datos_basicos, df_alfprd_pj_datos_basicos)
df_alfaprd_datos_basicos_stg.filter(col("NID_NUMERO_IDENTIFICACION") == "1001316228").show()
df_alfaprd_datos_basicos_stg.count()
### FIVE CELL
df_alfprd_pn_datos_basicos = processor.process_personas_naturales_datos_basicos(
                df_alfaprd_general_acu_personas_naturales, df_alfaprd_general_cgr_tipos_identificaciones, 
                df_alfaprd_general_cgr_cuidades_departamentos, df_alfaprd_general_cgr_departamanetos,
                df_general_cgr_estados
           )

df_alfprd_pj_datos_basicos = processor.process_personas_juridicas_datos_basicos(
                df_alfaprd_general_acu_personas_juridicas,
                df_alfaprd_general_cgr_tipos_identificaciones,
                df_general_cgr_estados,
                df_alfaprd_general_acu_tipos_empresa
)

df_alfprd_pn_demograficos = processor.process_personas_naturales_demograficos(
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
df_alfprd_pj_demograficos = processor.process_personas_juridicas_demograficos(
                df_alfaprd_general_acu_personas_juridicas,
                df_alfaprd_general_cgr_tipos_identificaciones,
                df_alfaprd_general_cgr_cuidades_departamentos,
                df_alfaprd_general_cgr_departamanetos,
                df_alfaprd_general_cgr_paises,
                df_alfaprd_general_acu_actividades_economica_ciiu
)
df_alfprd_pn_contactabilidad = processor.process_personas_naturales_contactabilidad(
                df_alfaprd_general_acu_personas_naturales,
                df_alfaprd_general_acu_personas_localizaciones,
                df_alfaprd_general_cgr_cuidades_departamentos,
                df_alfaprd_general_cgr_departamanetos,
                df_alfaprd_general_cgr_paises
)
df_alfprd_pj_contactabilidad = processor.process_personas_juridicas_contactabilidad(
                df_alfaprd_general_acu_personas_juridicas,
                df_alfaprd_general_acu_personas_localizaciones,
                df_alfaprd_general_cgr_cuidades_departamentos,
                df_alfaprd_general_cgr_departamanetos,
                df_alfaprd_general_cgr_paises
)
### SIX CELL
## DATOS SOCIODEMOGRAFICOS
df_processed_pn_socio_extended = df_alfprd_pn_demograficos \
        .withColumn("fec_constitucion", lit(None).cast("date")) \
        .withColumn("cod_ciudad", lit(None).cast("string")) \
        .withColumn("TIPO_PERSONA", lit("NATURAL"))
       # Legal Persons (PJ) - Add columns that only natural persons have
df_processed_pj_socio_extended = df_alfprd_pj_demograficos \
            .withColumn("FEC_NACIMIENTO", lit(None).cast("date")) \
            .withColumn("COD_CIUDAD_NACIMIENTO", lit(None).cast("string")) \
            .withColumn("NACIONALIDAD", lit(None).cast("string")) \
            .withColumn("GENERO", lit(None).cast("string")) \
            .withColumn("COD_ESTADO_CIVIL", lit(None).cast("string")) \
            .withColumn("estado_civil", lit(None).cast("string")) \
            .withColumn("COD_NIVEL_EDUCATIVO", lit(None).cast("string")) \
            .withColumn("nivel_educativo", lit(None).cast("string")) \
            .withColumn("COD_PROFESION", lit(None).cast("string")) \
            .withColumn("profesion", lit(None).cast("string")) \
            .withColumn("COD_OCUPACION", lit(None).cast("string")) \
            .withColumn("ocupacion", lit(None).cast("string")) \
            .withColumn("cod_tipo_identificacion_emp", lit(None).cast("string")) \
            .withColumn("nid_numero_identificacion_tra", lit(None).cast("string")) \
            .withColumn("nombre_empresa", lit(None).cast("string")) \
            .withColumn("cod_cargo", lit(None).cast("string")) \
            .withColumn("fecha_ingreso_empresa", lit(None).cast("date")) \
            .withColumn("NID_AFILIACION", lit(None).cast("decimal")) \
            .withColumn("TIPO_PERSONA", lit("JURIDICA"))
       # NORMALIZE COLUMN NAMES BEFORE UNION
 
       # Normalize PN - rename columns to match PJ
df_processed_pn_socio_normalized = df_processed_pn_socio_extended \
            .withColumnRenamed("COD_CIUDAD_NACIMIENTO", "cod_ciudad_origen") \
            .withColumnRenamed("cod_ciudad", "cod_ciudad_constitucion")
 
       # Normalize PJ - rename columns to match PN  
df_processed_pj_socio_normalized = df_processed_pj_socio_extended \
            .withColumnRenamed("cod_ciudad", "cod_ciudad_constitucion") \
            .withColumnRenamed("COD_CIUDAD_NACIMIENTO", "cod_ciudad_origen")
       # FINAL SOCIODEMOGRAPHIC UNION
df_sociodemograficos_processed = df_processed_pn_socio_normalized.unionByName(df_processed_pj_socio_normalized)
#data_codes = ['80113205','79052356','1019055943','24537502','1140828637']
data_codes = ['51577669']
df_sociodemograficos_processed.filter(col("nid_numero_identificacion").isin(data_codes)).select(col("cod_ciudad_origen")).show(10, truncate=False)
data_codes = ['51577669']
df_processed_pn_socio_extended.filter(col("nid_numero_identificacion").isin(data_codes)).show(10, truncate=False)
job.commit()