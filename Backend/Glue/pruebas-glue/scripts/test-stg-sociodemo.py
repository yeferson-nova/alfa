#!/usr/bin/env python3

"""
AWS Glue Job: Enhanced Silver Layer Processing with Dimension Mapping
Description: ETL processing for the Silver layer with dimension mapping capabilities
Author: Data Engineering Team
Date: 2025
"""

import sys
from datetime import datetime
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
# Custom libraries
from alfaprd_staging_processor import AlfprdStagingProcessor
from datos_sociodemograficos_schema import DatosSociodemograficosSchema

from pyspark.sql.functions import col, lit, concat_ws, trim, coalesce, row_number, concat
from pyspark.sql.types import StringType, IntegerType, DateType
from pyspark.sql.window import Window

def main():
    """Main function with enhanced dimension mapping support."""
    # Get job arguments
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'BUCKET_SILVER', 'BUCKET_BRONZE', 'JOB_PROCESS_ID',
        'S3_HOMOLOGATION_PATH', 'S3_STATUS_CONFIG_PATH',
        'LOAD_STG_SILVER', 'LOAD_ORIGIN_TABLE'
    ])

    # Initialize contexts
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    start_time = datetime.now()
    print(f"Job {args['JOB_NAME']} start at {start_time.strftime('%Y-%m-%d %H:%M:%S')} with PID: {args['JOB_PROCESS_ID']}")
    
    try:
        # Initialize processor
        processor = AlfprdStagingProcessor(spark, glue_context, args['BUCKET_BRONZE'], args['BUCKET_SILVER'])
        # Load configuration
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
        df_alfprd_sociodemograficos = df_sociodemograficos_processed.select(
            col("COD_TIPO_IDENTIFICACION").alias("cod_tipo_identificacion"),
            col("NID_NUMERO_IDENTIFICACION").cast("string").alias("numero_documento"),
            col("FEC_NACIMIENTO").cast("date").alias("fecha_nacimiento"),
            lit(None).cast("string").alias("edad"),
            col("cod_ciudad_origen").alias("cod_ciudad_nacimiento"),
            col("COD_DEPARTAMENTO").alias("cod_departamento_nacimiento"),
            lit(None).cast("string").alias("cod_pais_nacimiento"),
            col("cod_ciudad_origen").alias("ciudad_nacimiento"),
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
        datos_sociodemo_schema = DatosSociodemograficosSchema(spark, glue_context, processor.s3_handler)
        _,path_dsociodemo = processor.s3_handler.generate_s3_paths(processor.prefix_bucket,processor.schemas[1],processor.sufix_bucket)
        summary = datos_sociodemo_schema.insert_data(
                    df_new_data=df_alfprd_sociodemograficos,
                    s3_path=path_dsociodemo,
                    cod_fuente=2,
                    job_id=args['JOB_PROCESS_ID']
        )
        

    except Exception as e:
        print(f"Error in Enhanced Silver Layer job: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()
