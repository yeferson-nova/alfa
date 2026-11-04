import sys
import json
from urllib.parse import urlparse

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Inicializar Spark/Glue y logger
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()

args = getResolvedOptions(sys.argv, ['JOB_NAME'])  # JOB_NAME siempre existe
# DATABASE_NAME es opcional (puedes pasarlo como argumento del job)
database_name = None
try:
    more_args = getResolvedOptions(sys.argv, ['DATABASE_NAME'])
    database_name = more_args.get('DATABASE_NAME')
except Exception:
    pass

if not database_name:
    # Valor por defecto si no se pasó parámetro
    database_name = 'alfa-datalake-acsele-bronce'

glue = boto3.client('glue')

def list_buckets_from_db(db_name: str) -> set:
    buckets = set()
    next_token = None
    while True:
        resp = glue.get_tables(DatabaseName=db_name, NextToken=next_token) if next_token \
               else glue.get_tables(DatabaseName=db_name)
        for tbl in resp.get('TableList', []):
            sd = tbl.get('StorageDescriptor') or {}
            loc = sd.get('Location')
            if loc and loc.startswith('s3://'):
                parsed = urlparse(loc)
                if parsed.netloc:
                    buckets.add(parsed.netloc)
        next_token = resp.get('NextToken')
        if not next_token:
            break
    return buckets

def main():
    logger.info(f"=== Inicio Job: {args['JOB_NAME']} ===")
    logger.info(f"Database objetivo: {database_name}")

    try:
        buckets = list_buckets_from_db(database_name)
        if not buckets:
            logger.warn("No se encontraron buckets (¿DB vacío o tablas sin Location?).")
        else:
            logger.info("Buckets encontrados:")
            for b in sorted(buckets):
                logger.info(f"- {b}")

        # También lo dejo en una sola línea JSON para búsquedas rápidas en Logs Insights
        logger.info("RESULTADO_JSON=" + json.dumps({
            "database": database_name,
            "buckets": sorted(buckets)
        }))
        logger.info("=== Fin Job OK ===")
    except (BotoCoreError, ClientError) as e:
        logger.error("Error de AWS SDK al consultar Glue", exc_info=True)
        raise
    except Exception as e:
        logger.error("Error no controlado", exc_info=True)
        raise

if __name__ == "__main__":
    main()