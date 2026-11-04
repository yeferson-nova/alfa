import sys
import logging
import pandas as pd
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

# Configuración de logging
def setup_logging():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

logger = setup_logging()

# Inicialización del contexto de Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Leer los parámetros del script
args = getResolvedOptions(sys.argv, [
    'JOB_NAME'
])

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # Ruta de origen en S3
    input_path = 's3://alfa-datalake-dev-assets/support_phase_files/niif/RPND13Y_202501.xlsx'
    output_path = 's3://alfa-datalake-dev-bronce/ACSELE/ACSELE_ALFA/RPND13Y/'

    logger.info(f'Leyendo el archivo desde {input_path}')

    # Leer el archivo Excel con pandas
    df_pandas = pd.read_excel(input_path)

    # Convertir todas las columnas a tipo string en pandas para evitar conflictos de tipos
    df_pandas = df_pandas.astype(str)

    # Convertir el DataFrame de pandas a un DataFrame de Spark
    df_spark = spark.createDataFrame(df_pandas)

    logger.info('Archivo leído exitosamente con pandas y convertido a Spark')

    # Mostrar el esquema del DataFrame
    df_spark.printSchema()
    df_spark.show(5)

    # Guardar el DataFrame en formato Parquet en la ruta de destino
    logger.info(f'Almacenando el archivo en formato Parquet en {output_path}')
    df_spark.write.mode("overwrite").parquet(output_path)
    logger.info('Archivo almacenado exitosamente en formato Parquet')

except Exception as e:
    logger.error(f'Error procesando el archivo: {str(e)}')
    sys.exit(1)

finally:
    job.commit()
