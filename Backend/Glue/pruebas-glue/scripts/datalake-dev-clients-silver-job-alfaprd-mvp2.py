"""
================================================================================
AWS Glue Job: Silver Layer Processing - ALFAPRD MVP2 - Modular Version
================================================================================

Description:
    Main entry point for the ALFAPRD MVP2 Silver layer processing pipeline.
    Orchestrates the full ETL flow from Bronze/STG sources to the Silver
    Delta Lake tables, covering all three business units: ARPIS, RENTAS,
    and PREVISIONALES.

    Idempotency guard (STEP 0):
        Before any data is written, the job resolves the target period(s)
        and checks whether data already exists in the four append-mode
        Silver tables. If data is found, it is deleted for that period only,
        ensuring clean reprocessing without duplicates. The job fails
        explicitly if deletion is incomplete.

    Managed Silver tables:
        - ALFAPRD_CLIENTES_STG      (append, partitioned by yyyy/mm)
        - ALFAPRD_POLIZAS_STG       (append, partitioned by yyyy/mm)
        - CLIENTES_ALFAPRD_SILVER   (append, partitioned by yyyy/mm)
        - POLIZAS_ALFAPRD_SILVER    (append, partitioned by yyyy/mm)

    Processing modes:
        - FALLEN_MONTH: Automatically processes the previous calendar month.
        - HISTORIC:     Processes a date range defined by START_DATE / END_DATE.

    Execution flow:
        0. Idempotency check and cleanup (delete period if reprocessing)
        1. Resolve job parameters
        2. Initialize Glue context
        3. Initialize AlfaprdMVP2SilverProcessor orchestrator
        4. Execute orchestrator (staging → productos → ARPIS → RENTAS → PREVISIONALES)
        5. Commit Glue job

Author:         Juan Alejandro Carrillo Jaimes
Created:        2026-02-05
Version:        2.0.0

Glue Parameters:
    --JOB_NAME:               Job identifier
    --BUCKET_SILVER:          Silver layer bucket name
    --BUCKET_BRONZE:          Bronze layer bucket name
    --BUCKET_GOLD:            Gold layer bucket name
    --BUCKET_ASSETS:          Assets bucket name
    --JOB_PROCESS_ID:         Unique process execution identifier
    --S3_HOMOLOGATION_PATH:   S3 path to the homologation Excel file
    --S3_STATUS_CONFIG_PATH:  S3 path to the status configuration file
    --LOAD_STG_SILVER:        Whether to execute staging ("true" / "false")
    --CRAWLER_SILVER:         Name of the Glue crawler for Silver Delta tables
    --HISTORIC_PARAM:         Historic mode flag ("1" = historic, "0" = fallen month)
    --START_DATE:             Start period in yyyymm format (historic mode only)
    --END_DATE:               End period in yyyymm format (historic mode only)
    --HOMOLOGATION_SHEET:     Sheet name within the homologation Excel file

================================================================================
"""

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import sys
import calendar
from datetime import datetime, timedelta

from alfaprd_mvp2_silver_processor import AlfaprdMVP2SilverProcessor
from logger_config import logger


# ========================================================================
#                    LOGGER WRAPPER
# ========================================================================
def log_function(message, class_name):
    """
    Wrapper to make logger callable as a function.

    This allows all processor classes to use logger as:
        self.logger("message", "ClassName")

    Instead of:
        self.logger.info("message", "ClassName")
    """
    logger.info(message, class_name)

# ========================================================================
#                    IDEMPOTENCIA: CHECK & CLEANUP
# ========================================================================
def _resolve_periods(args):
    """
    Resolves the periods to be processed based on job parameters.

    Supports two modes:
        - FALLEN_MONTH: Automatically uses the previous calendar month.
        - HISTORIC: Uses the date range defined by START_DATE and END_DATE
          parameters (yyyymm format).

    Args:
        args (dict): Resolved Glue job parameters.

    Returns:
        list[tuple[str, str]]: List of (yyyy, mm) tuples representing
        each period to process.
    """
    historic_param = args.get('HISTORIC_PARAM', '0')

    if historic_param == '1':
        start_param = args['START_DATE']  # yyyymm
        end_param   = args['END_DATE']    # yyyymm

        start_year  = int(start_param[:4])
        start_month = int(start_param[4:6])
        end_year    = int(end_param[:4])
        end_month   = int(end_param[4:6])
    else:
        # FALLEN MONTH: previous calendar month
        today             = datetime.now()
        first_day_current = today.replace(day=1)
        last_day_prev     = first_day_current - timedelta(days=1)
        start_year        = last_day_prev.year
        start_month       = last_day_prev.month
        end_year          = start_year
        end_month         = start_month

    # Build list of periods within the range
    periods = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        periods.append((str(year), f"{month:02d}"))
        month += 1
        if month > 12:
            month = 1
            year += 1

    return periods


def check_and_cleanup_period(spark, args):
    """
    STEP 0 – Silver idempotency guard.

    For each period to be processed, checks whether data already exists
    in the Silver tables (STG + final Silver). If data is found, it is
    deleted before reloading, preventing duplicate records on reprocessing runs.

    Managed tables:
        - ALFAPRD_CLIENTES_STG
        - ALFAPRD_POLIZAS_STG
        - CLIENTES_ALFAPRD_SILVER
        - POLIZAS_ALFAPRD_SILVER

    Behavior:
        - Table does not exist yet  → skipped silently (first load scenario).
        - Table exists, no data for period → no action taken.
        - Table exists, data found for period → reprocess detected: deletes only
          that period partition, logs the Delta version for manual rollback,
          and validates the deletion was complete before proceeding.

    Args:
        spark: Active SparkSession.
        args (dict): Resolved Glue job parameters.

    Raises:
        RuntimeError: If deletion verification fails (records still present
        after delete).
    """
    from delta.tables import DeltaTable

    BUCKET = args['BUCKET_SILVER']

    TABLES = {
        "ALFAPRD_CLIENTES_STG":    f"s3://{BUCKET}/POLIZAS_PRODUCTO/ALFAPRD/ALFAPRD_CLIENTES_STG/",
        "ALFAPRD_POLIZAS_STG":     f"s3://{BUCKET}/POLIZAS_PRODUCTO/ALFAPRD/ALFAPRD_POLIZAS_STG/",
        "CLIENTES_ALFAPRD_SILVER": f"s3://{BUCKET}/POLIZAS_PRODUCTO/ALFAPRD/CLIENTES_ALFAPRD_SILVER/",
        "POLIZAS_ALFAPRD_SILVER":  f"s3://{BUCKET}/POLIZAS_PRODUCTO/ALFAPRD/POLIZAS_ALFAPRD_SILVER/",
    }

    periods = _resolve_periods(args)

    logger.info("=" * 80, "Idempotencia")
    logger.info("STEP 0: PRE-LOAD VALIDATION AND CLEANUP (IDEMPOTENCY)", "Idempotencia")
    logger.info(f"Periods to process: {[f'{y}{m}' for y, m in periods]}", "Idempotencia")
    logger.info("=" * 80, "Idempotencia")

    for table_name, path in TABLES.items():

        logger.info(f"\n{'─' * 60}", "Idempotencia")
        logger.info(f"Table: {table_name}", "Idempotencia")
        logger.info(f"Path:  {path}", "Idempotencia")

        # ── 1. Verify that the Delta table exists ─────────────────────────
        try:
            delta_table = DeltaTable.forPath(spark, path)
        except Exception:
            logger.info(
                f"  ⚠ Table does not exist yet in Delta Lake – skipping cleanup: {table_name}",
                "Idempotencia"
            )
            continue

        df_existing = spark.read.format("delta").load(path)

        # ── 2. For each period, check whether data exists ─────────────────
        for yyyy, mm in periods:
            count_existing = (
                df_existing
                .filter(f"yyyy = '{yyyy}' AND mm = '{mm}'")
                .count()
            )

            if count_existing == 0:
                logger.info(
                    f"[{yyyy}{mm}] No prior data found in {table_name} – no cleanup required",
                    "Idempotencia"
                )
                continue

            # ── 3. Data exists → reprocess → DELETE ───────────────────────
            logger.info(
                f"  🔄 [{yyyy}{mm}] REPROCESS detected: {count_existing:,} records in {table_name}",
                "Idempotencia"
            )
            logger.info(
                f"  🗑  Deleting records yyyy='{yyyy}', mm='{mm}' from {table_name}...",
                "Idempotencia"
            )

            # Save version before deletion (for manual rollback if needed)
            version_before = delta_table.history(1).first()['version']

            delta_table.delete(f"yyyy = '{yyyy}' AND mm = '{mm}'")

            version_after = delta_table.history(1).first()['version']

            # Verify deletion was complete
            count_after = (
                spark.read.format("delta").load(path)
                .filter(f"yyyy = '{yyyy}' AND mm = '{mm}'")
                .count()
            )

            if count_after > 0:
                raise RuntimeError(
                    f"[Idempotency] FAILURE: {count_after:,} records still present in "
                    f"{table_name} for yyyy={yyyy}, mm={mm} after deletion."
                )

            logger.info(
                f" [{yyyy}{mm}] {count_existing:,} records deleted from {table_name}. "
                f"Delta version: {version_before} → {version_after}. "
                f"Rollback available via: RESTORE TABLE delta.`{path}` TO VERSION AS OF {version_before}",
                "Idempotencia"
            )

    logger.info("=" * 80, "Idempotencia")
    logger.info("STEP 0: VALIDATION AND CLEANUP COMPLETED", "Idempotencia")
    logger.info("=" * 80, "Idempotencia")


# ========================================================================
#                    MAIN EXECUTION
# ========================================================================
if __name__ == "__main__":

    # ====================================================================
    # STEP 1: GET JOB PARAMETERS
    # ====================================================================
    logger.info("=" * 80, "MainJob")
    logger.info("STARTING GLUE JOB: ALFAPRD MVP2 SILVER - MODULAR VERSION", "MainJob")
    logger.info("=" * 80, "MainJob")

    try:
        args = getResolvedOptions(sys.argv, [
            'JOB_NAME',
            'BUCKET_SILVER',
            'BUCKET_BRONZE',
            'BUCKET_GOLD',
            'BUCKET_ASSETS',
            'JOB_PROCESS_ID',
            'S3_HOMOLOGATION_PATH',
            'S3_STATUS_CONFIG_PATH',
            'LOAD_STG_SILVER',
            'CRAWLER_SILVER',
            'HISTORIC_PARAM',
            'START_DATE',
            'END_DATE',
            'HOMOLOGATION_SHEET'
        ])

        logger.info("Job parameters resolved successfully", "MainJob")
        logger.info(f"Job Name: {args['JOB_NAME']}", "MainJob")
        logger.info(f"Job Process ID: {args['JOB_PROCESS_ID']}", "MainJob")
        logger.info(f"Bucket Silver: {args['BUCKET_SILVER']}", "MainJob")
        logger.info(f"Load STG: {args['LOAD_STG_SILVER']}", "MainJob")
        logger.info(f"Historic Mode: {args['HISTORIC_PARAM']}", "MainJob")

        if args['HISTORIC_PARAM'] == '1':
            logger.info(f"Period: {args['START_DATE']} to {args['END_DATE']}", "MainJob")

    except Exception as e:
        logger.error(f"Error resolving job parameters: {str(e)}", "MainJob")
        raise

    # ====================================================================
    # STEP 2: INITIALIZE GLUE CONTEXT
    # ====================================================================
    logger.info("=" * 80, "MainJob")
    logger.info("INITIALIZING GLUE CONTEXT", "MainJob")
    logger.info("=" * 80, "MainJob")

    try:
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args['JOB_NAME'], args)

        logger.info("Glue context initialized successfully", "MainJob")
        logger.info(f"Spark version: {spark.version}", "MainJob")

    except Exception as e:
        logger.error(f"Error initializing Glue context: {str(e)}", "MainJob")
        raise

    # ====================================================================
    # STEP 0: IDEMPOTENCY – VALIDATE AND CLEAN PERIOD IF REPROCESSING
    # ====================================================================
    #
    # This step runs BEFORE staging and the main orchestrator.
    # Responsibilities:
    #   1. Resolve which period(s) will be loaded
    #   2. For each Silver table (STG + final Silver), check whether data
    #      already exists for that period
    #   3. If data exists → reprocess detected → delete ONLY that period
    #   4. If no data exists → normal load, nothing is touched
    #
    # The job fails explicitly if deletion is incomplete, guaranteeing
    # that data is NEVER written on top of a partially cleaned state.
    # ====================================================================
    try:
        check_and_cleanup_period(spark, args)
    except Exception as e:
        logger.error(f"Error in STEP 0 (Idempotency): {str(e)}", "MainJob")
        import traceback
        logger.error(traceback.format_exc(), "MainJob")
        job.commit()
        raise

    # ====================================================================
    # STEP 3: INITIALIZE ORCHESTRATOR
    # ====================================================================
    logger.info("=" * 80, "MainJob")
    logger.info("INITIALIZING ORCHESTRATOR", "MainJob")
    logger.info("=" * 80, "MainJob")

    try:
        processor = AlfaprdMVP2SilverProcessor(
            args=args,
            spark=spark,
            glue_context=glueContext,
            logger=log_function
        )

        logger.info("Orchestrator initialized successfully", "MainJob")

    except Exception as e:
        logger.error(f"Error initializing orchestrator: {str(e)}", "MainJob")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}", "MainJob")
        job.commit()
        raise

    # ====================================================================
    # STEP 4: EXECUTE ORCHESTRATOR
    # ====================================================================
    logger.info("=" * 80, "MainJob")
    logger.info("EXECUTING ORCHESTRATOR", "MainJob")
    logger.info("=" * 80, "MainJob")

    start_time = datetime.now()

    try:
        processor.run()

        execution_time = (datetime.now() - start_time).total_seconds()

        logger.info("=" * 80, "MainJob")
        logger.success("JOB EXECUTION COMPLETED SUCCESSFULLY", "MainJob")
        logger.info(f"Total execution time: {execution_time:.2f} seconds ({execution_time/60:.2f} minutes)", "MainJob")
        logger.info("=" * 80, "MainJob")

    except Exception as e:
        execution_time = (datetime.now() - start_time).total_seconds()

        logger.info("=" * 80, "MainJob")
        logger.error("JOB EXECUTION FAILED", "MainJob")
        logger.error(f"Error: {str(e)}", "MainJob")
        logger.info(f"Execution time before failure: {execution_time:.2f} seconds", "MainJob")
        logger.info("=" * 80, "MainJob")

        import traceback
        logger.error("Full traceback:", "MainJob")
        logger.error(traceback.format_exc(), "MainJob")

        job.commit()
        raise

    # ====================================================================
    # STEP 5: COMMIT JOB
    # ====================================================================
    logger.info("=" * 80, "MainJob")
    logger.info("COMMITTING GLUE JOB", "MainJob")
    logger.info("=" * 80, "MainJob")

    try:
        job.commit()
        logger.success("Glue job committed successfully", "MainJob")

    except Exception as e:
        logger.error(f"Error committing job: {str(e)}", "MainJob")
        raise

    logger.info("=" * 80, "MainJob")
    logger.info("GLUE JOB FINISHED", "MainJob")
    logger.info("=" * 80, "MainJob")