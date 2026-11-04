
import sys, os, json
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

from extraction.logger import get_logger
from extraction.params_dynamo import ParamResolver, ALIAS_MAP
from extraction.source_readers import read_from_source
from extraction.bronze_writer import write_bronze_and_catalog
from extraction.load_strategy import decide_load_mode

logger = get_logger("job")

args = getResolvedOptions(sys.argv, [
    "LOG_LEVEL",
    "JOB_NAME",
    "JOB_BRZ_ID",
    "TBL_PRM_ARTEFACTO",
    "TBL_PRM_CONCEPTO",
    "TBL_PRM_NOTA",
    "TBL_PRM_ACN"
])

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

SENSITIVE_KEYS = {"password", "passwd", "secret", "token", "authorization",
                  "access_key", "secret_key", "pwd"}

def _mask(o):
    if isinstance(o, dict):
        return {k: ("***" if k.lower() in SENSITIVE_KEYS else _mask(v)) for k, v in o.items()}
    if isinstance(o, list):
        return [_mask(x) for x in o]
    return o

try:
    resolver = ParamResolver(
        table_artefacto=args["TBL_PRM_ARTEFACTO"],
        table_concepto=args["TBL_PRM_CONCEPTO"],
        table_nota=args["TBL_PRM_NOTA"],
        table_acn=args["TBL_PRM_ACN"],
    )

    job_id = int(args["JOB_BRZ_ID"])
    raw_cfg = resolver.build_maps_for_job(job_id)
    cfg = resolver.normalize_maps(raw_cfg, ALIAS_MAP)
    
    safe_cfg = _mask(cfg)
    logger.info("CFG completo:\n" + json.dumps(safe_cfg, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    print(json.dumps(safe_cfg, ensure_ascii=False, indent=2, sort_keys=True, default=str))

    mode = decide_load_mode(cfg)
    logger.info("Estrategia de carga: {}".format(mode.upper()))
    
    job.commit()
    logger.info("✅ Finalizado con éxito")

    # df = read_from_source(spark, cfg)
    # logger.info("Leídos: {} rows, {} cols".format(df.count(), len(df.columns)))

    # out = write_bronze_and_catalog(glue_ctx, df, cfg)
    # logger.info("Bronce OK en: {}".format(out))
    

except Exception as e:
    logger.exception("Error: {}".format(e))
    job.commit()
    raise
