
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from datetime import datetime
import pandas as pd

def extract_excel_data(spark, path, sheet_name):
    """
    Carga Excel desde S3 (usando el conector de Spark) o local,
    y devuelve DataFrame Spark. Si lo cargas con Pandas, convierto a Spark.
    """
    # Opción 1: si tienes habilitado spark-excel (plugin), usar:
    # df = spark.read.format("com.crealytics.spark.excel") \
    #     .option("header", "true").option("inferSchema", "true") \
    #     .option("dataAddress", f"'{sheet_name}'!A1") \
    #     .load(path)

    # Opción 2: lectura con Pandas (común en Glue) y paso a Spark:
    pdf = pd.read_excel(path, sheet_name=sheet_name)
    # Reemplazo NaN por None para que Spark no rompa
    pdf = pdf.where(pd.notnull(pdf), None)
    for col in pdf.columns:
        pdf[col] = pdf[col].apply(lambda x: str(x) if x is not None else None)
    return spark.createDataFrame(pdf)

def main():
    """Main function that only loads, normalizes, and shows homologation file."""
    args = getResolvedOptions(
        sys.argv,
        [
            'JOB_NAME', 'BUCKET_SILVER', 'BUCKET_BRONZE', 'JOB_PROCESS_ID',
            'S3_HOMOLOGATION_PATH', 'S3_STATUS_CONFIG_PATH',
            'LOAD_STG_SILVER', 'LOAD_ORIGIN_TABLE', 'HISTORIC_PARAM'
        ]
    )

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)

    start_time = datetime.now()
    print(f"Job {args['JOB_NAME']} start at {start_time.strftime('%Y-%m-%d %H:%M:%S')} with PID: {args['JOB_PROCESS_ID']}")

    try:
        # === Solo cargar el Excel de homologaciones y mostrar ===
        path = args['S3_HOMOLOGATION_PATH']
        sheet = "homologaciones_polizas"

        df_homologaciones_dos = extract_excel_data(spark, path, sheet)

        # Normalización completa (tabla, campo_referencia, valor_referencia, campo_homologacion)
        df_homologaciones = df_homologaciones_dos.selectExpr(
            """
            translate(lower(trim(tabla)),
              'ÁÀÄÂÃÅáàäâãåÉÈËÊéèëêÍÌÏÎíìïîÓÒÖÔÕóòöôõÚÙÜÛúùüûÑñÇç',
              'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuNnCc'
            ) as tabla
            """,
            """
            translate(lower(trim(campo_referencia)),
              'ÁÀÄÂÃÅáàäâãåÉÈËÊéèëêÍÌÏÎíìïîÓÒÖÔÕóòöôõÚÙÜÛúùüûÑñÇç',
              'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuNnCc'
            ) as campo_referencia
            """,
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
            """
            translate(lower(trim(campo_homologacion)),
              'ÁÀÄÂÃÅáàäâãåÉÈËÊéèëêÍÌÏÎíìïîÓÒÖÔÕóòöôõÚÙÜÛúùüûÑñÇç',
              'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuNnCc'
            ) as campo_homologacion
            """,
            # Limpieza cod_fuente_fk (NULL, 'nan', quitar '.0')
            """
            CASE
              WHEN cod_fuente_fk IS NULL THEN NULL
              WHEN LOWER(TRIM(cod_fuente_fk)) = 'nan' THEN NULL
              WHEN cod_fuente_fk LIKE '%.0' THEN REGEXP_REPLACE(cod_fuente_fk, '\\.0$', '')
              ELSE cod_fuente_fk
            END as cod_fuente_fk
            """
        )

        # Mostrar (no persistimos ni guardamos, solo inspección)
               df_homologaciones.show(truncate=False)

    finally:
        job.commit()

if __name__ == "__main__":
    import sys
