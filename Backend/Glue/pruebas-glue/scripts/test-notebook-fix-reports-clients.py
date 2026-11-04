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
from s3_handler import S3Handler
from alfaprd_staging_processor import AlfprdStagingProcessor
from utils import Utils
#------PYTHON IMPORTS -------#
import os
import sys
## ----- INIT JOB ------------------ ###
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark.__class__
glueContext.__class__
# Initialice AlfprdStagingProcessor
BUCKET_BRONCE = 'alfa-datalake-dev-bronce'
BUCKET_SILVER = 'alfa-datalake-dev-silver'
processor_alfprd = AlfprdStagingProcessor(spark, glueContext, BUCKET_BRONCE, BUCKET_SILVER)
# load bronce data
table_paths = processor_alfprd._get_alfaprd_table_paths()
df_alfaprd_general_acu_personas_naturales = processor_alfprd.utils.read_full_parquet(spark,s3_target_path=table_paths["general_acu_personas_naturales"])
df_alfaprd_general_acu_personas_juridicas = processor_alfprd.utils.read_full_parquet(spark,s3_target_path=table_paths["general_acu_personas_juridicas"])
df_alfaprd_general_acu_tipos_empresa = processor_alfprd.utils.read_full_parquet(spark,s3_target_path=table_paths["general_acu_tipos_empresa"])
df_alfaprd_general_cgr_cuidades_departamentos = processor_alfprd.utils.read_full_parquet(spark,s3_target_path=table_paths["general_cgr_cuidades_departamentos"])
df_alfaprd_general_acu_niveles_educativos = processor_alfprd.utils.read_full_parquet(spark,s3_target_path=table_paths["general_acu_niveles_educativos"])
df_alfaprd_general_acu_profesiones = processor_alfprd.utils.read_full_parquet(spark,s3_target_path=table_paths["general_acu_profesiones"])
df_alfaprd_arpprod_par_afiliaciones = processor_alfprd.utils.read_full_parquet(spark,s3_target_path=table_paths["arpprod_par_afiliaciones"])
df_alfaprd_arpprod_par_trabajadores = processor_alfprd.utils.read_full_parquet(spark,s3_target_path=table_paths["arpprod_par_trabajadores"])
df_alfaprd_general_acu_personas_localizaciones = processor_alfprd.utils.read_full_parquet(spark,s3_target_path=table_paths["general_acu_personas_localizaciones"])
df_alfaprd_general_cgr_departamanetos = processor_alfprd.utils.read_full_parquet(spark,s3_target_path=table_paths["general_cgr_departamanetos"])
df_alfaprd_general_cgr_paises = processor_alfprd.utils.read_full_parquet(spark,s3_target_path=table_paths["general_cgr_paises"])
df_alfaprd_general_cgr_ocupaciones = processor_alfprd.utils.read_full_parquet(spark,s3_target_path=table_paths["general_cgr_ocupaciones"])
df_alfaprd_general_cgr_estados_civiles = processor_alfprd.utils.read_full_parquet(spark,s3_target_path=table_paths["general_cgr_estados_civiles"])
df_alfaprd_general_acu_actividades_economica_ciiu = processor_alfprd.utils.read_full_parquet(spark,s3_target_path=table_paths["general_cgr_act_eco_ciuu"])
df_alfaprd_general_cgr_tipos_identificaciones  = processor_alfprd.utils.read_full_parquet(spark,s3_target_path=table_paths["general_cgr_tipos_id"])
df_general_cgr_estados = processor_alfprd.utils.read_full_parquet(spark,s3_target_path=table_paths["general_cgr_estados"])
# 02. Normalize data
df_alfprd_pn_datos_basicos = processor_alfprd.process_personas_naturales_datos_basicos(
                df_alfaprd_general_acu_personas_naturales, df_alfaprd_general_cgr_tipos_identificaciones, 
                df_alfaprd_general_cgr_cuidades_departamentos, df_alfaprd_general_cgr_departamanetos,
                df_general_cgr_estados
)
df_alfprd_pj_datos_basicos = processor_alfprd.process_personas_juridicas_datos_basicos(
                df_alfaprd_general_acu_personas_juridicas,
                df_alfaprd_general_cgr_tipos_identificaciones,
                df_general_cgr_estados,
                df_alfaprd_general_acu_tipos_empresa
)
df_alfprd_pn_demograficos = processor_alfprd.process_personas_naturales_demograficos(
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
df_alfprd_pj_demograficos = processor_alfprd.process_personas_juridicas_demograficos(
                df_alfaprd_general_acu_personas_juridicas,
                df_alfaprd_general_cgr_tipos_identificaciones,
                df_alfaprd_general_cgr_cuidades_departamentos,
                df_alfaprd_general_cgr_departamanetos,
                df_alfaprd_general_cgr_paises,
                df_alfaprd_general_acu_actividades_economica_ciiu
)
df_alfprd_pn_contactabilidad = processor_alfprd.process_personas_naturales_contactabilidad(
                df_alfaprd_general_acu_personas_naturales,
                df_alfaprd_general_acu_personas_localizaciones,
                df_alfaprd_general_cgr_cuidades_departamentos,
                df_alfaprd_general_cgr_departamanetos,
                df_alfaprd_general_cgr_paises
)
df_alfprd_pj_contactabilidad = processor_alfprd.process_personas_juridicas_contactabilidad(
                df_alfaprd_general_acu_personas_juridicas,
                df_alfaprd_general_acu_personas_localizaciones,
                df_alfaprd_general_cgr_cuidades_departamentos,
                df_alfaprd_general_cgr_departamanetos,
                df_alfaprd_general_cgr_paises
)
# 03. Show Demografic Flow
df_processed_pn_socio_extended = df_alfprd_pn_demograficos \
        .withColumn("fec_constitucion", lit(None).cast("date")) \
        .withColumn("cod_ciudad", lit(None).cast("string")) \
        .withColumn("TIPO_PERSONA", lit("NATURAL"))

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
df_processed_pj_socio_extended.filter(col("COD_CIUDAD_NACIMIENTO").rlike("[A-Za-z]")).select(col("COD_CIUDAD_NACIMIENTO")).show(10)
df_processed_pn_socio_extended.filter(col("cod_ciudad").rlike("[A-Za-z]")).select(col("COD_CIUDAD_NACIMIENTO")).show(10)
# Normalize PN - rename columns to match PJ
df_processed_pn_socio_normalized = df_processed_pn_socio_extended \
            .withColumnRenamed("COD_CIUDAD_NACIMIENTO", "cod_ciudad_origen") \
            .withColumnRenamed("cod_ciudad", "cod_ciudad_constitucion")
 
       # Normalize PJ - rename columns to match PN  
df_processed_pj_socio_normalized = df_processed_pj_socio_extended \
            .withColumnRenamed("cod_ciudad", "cod_ciudad_constitucion") \
            .withColumnRenamed("COD_CIUDAD_NACIMIENTO", "cod_ciudad_origen")
df_processed_pj_socio_normalized.printSchema()
df_processed_pj_socio_normalized.filter(col("cod_ciudad_origen").rlike("[A-Za-z]")).select(col("cod_ciudad_origen")).show(10)
df_processed_pn_socio_normalized.filter(col("cod_ciudad_origen").rlike("[A-Za-z]")).select(col("cod_ciudad_origen")).show(10)
df_sociodemograficos_processed = df_processed_pn_socio_normalized.unionByName(df_processed_pj_socio_normalized)
numeros_identificacion = ["80113205","79052356","1019055943","24537502","1140828637"]
df_sociodemograficos_processed.filter(col("NID_NUMERO_IDENTIFICACION").isin(numeros_identificacion)).show(truncate=False)
df_alfprd_sociodemograficos = df_sociodemograficos_processed.select(
            col("COD_TIPO_IDENTIFICACION").alias("cod_tipo_identificacion"),
            col("NID_NUMERO_IDENTIFICACION").cast("string").alias("numero_documento"),
            col("FEC_NACIMIENTO").cast("date").alias("fecha_nacimiento"),
            lit(None).cast("string").alias("edad"),
            col("cod_ciudad_origen").alias("cod_ciudad_nacimiento"),
            col("COD_DEPARTAMENTO").alias("cod_departamento_nacimiento"),
            lit(None).cast("string").alias("cod_pais_nacimiento"),
            col("nombre").alias("ciudad_nacimiento"),
            lit(None).cast("string").alias("departamento_nacimiento"),
            lit(None).cast("string").alias("pais_nacimiento"),
            col("NACIONALIDAD").alias("nacionalidad"),
            col("GENERO").alias("genero"),
            col("COD_ESTADO_CIVIL").alias("cod_estado_civil"),
            col("COD_NIVEL_EDUCATIVO").alias("cod_nivel_estudios"),
            col("nivel_educativo").alias("nivel_estudios"),
            col("COD_PROFESION").alias("cod_profesion"),
            col("profesion").alias("profesion"),
            col("COD_OCUPACION").alias("cod_ocupacion"),
            col("ocupacion").alias("ocupacion"),
            col("COD_ACTIVIDAD_ECONOMICA_CIIU").alias("cod_actividad_economica"),
            lit(None).cast("int").alias("numero_hijos"),
            lit(None).cast("string").alias("cod_tipo_sociedad"),
            col("cod_tipo_identificacion_emp").alias("tipo_id_empresa"),
            col("nid_numero_identificacion_tra").alias("nit_empresa"),
            col("NID_AFILIACION").cast("string").alias("afiliacion_id"),
            col("cod_cargo").alias("cod_cargo_empresa"),
            col("fecha_ingreso_empresa").cast("date").alias("fecha_ingreso_empresa")
       )
df_alfprd_sociodemograficos.filter(col("ciudad_nacimiento").rlike("[A-Za-z]")).select(col("ciudad_nacimiento")).show(10)
# 04. Create schemas and paths
df_alfaprd_datos_sociodemograficos_stg.filter(col("cod_ciudad_nacimiento"))
job.commit()