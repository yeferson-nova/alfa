"""
================================================================================
Previsional Siniestros Gold Update Job
================================================================================

Description:
    Job that runs after the main Gold pipeline for each period.
    Finds previsional ALFAPRD clients that have accumulated siniestros
    payments or reservas in the current period, and updates their most
    recent fact_poliza_emision record in Gold with the new valor_asegurado.

    No new records are created. No fact_clientes_polizas changes.
    Only fact_poliza_emision.valor_asegurado is updated, and only on
    the single most recent record per poliza.

    Matching logic:
        num_poliza in Gold has format: NID_POLIZA-TIPO_ID-CEDULA
        e.g. '9-1-98468659'

        Siniestros key: CONCAT(TIPO_ID, '-', CEDULA)
        e.g. '1-98468659'

        Extraction: split(num_poliza, '-') → take fields [1] and [2]
        (zero-indexed), concat with '-'.

    Priority hierarchy for valor_asegurado:
        1. PAGOS   (actual payments)   — takes precedence
        2. RESERVA (reserved amounts)  — fallback if no PAGOS

    Periods with no siniestros activity are skipped entirely.

Author:         Data Team
Created:        2026-03-04
Version:        3.0.0

Glue Parameters:
    --JOB_NAME:      Job identifier
    --BUCKET_SILVER: Silver layer bucket name (used for Athena output paths)
    --BUCKET_GOLD:   Gold layer bucket name
    --YYYY:          Processing year  (e.g. "2025")
    --MM:            Processing month (e.g. "10")
    --DB_GOLD:       Athena Gold catalog database name

================================================================================
"""

import sys
import calendar
import traceback
from datetime import date
from typing import Dict

# AWS Glue
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# Delta
from delta.tables import DeltaTable

# PySpark
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType
from pyspark.sql.window import Window

# Custom
from utils import Utils
from logger_config import logger


# ============================================================================
#                              CONSTANTS
# ============================================================================

_GOLD_BASE_PATH   = "GOLD/CLIENTES_GOLD"
_PATH_FACT_POLIZA = "GOLD-CARACT_CLIENTES-FACT_POLIZA_EMISION"


# ============================================================================
#                         SINIESTROS UPDATE PROCESSOR
# ============================================================================

class PrevisionalSiniestrosUpdateProcessor:
    """
    Updates fact_poliza_emision.valor_asegurado for previsional ALFAPRD
    clients that have siniestros activity in the current period.

    Operates exclusively on the most recent Gold record per poliza.
    Does not create new records. Does not touch fact_clientes_polizas.

    Matching:
        num_poliza format → NID_POLIZA-TIPO_ID-CEDULA (e.g. '9-1-98468659')
        Siniestros key    → TIPO_ID-CEDULA            (e.g. '1-98468659')
        Extraction        → split('-')[1] + '-' + split('-')[2]

    Args:
        spark:          Active SparkSession.
        bucket_silver:  S3 bucket for Athena temporary output paths.
        bucket_gold:    S3 bucket for Gold layer.
        yyyy:           Processing year as string  (e.g. "2025").
        mm:             Processing month as string (e.g. "10").
        db_gold:        Athena Gold catalog database name.
        utils:          Shared Utils instance.
    """

    CLASS_NAME = "PrevisionalSiniestrosUpdate"

    def __init__(
        self,
        spark: SparkSession,
        bucket_silver: str,
        bucket_gold: str,
        yyyy: str,
        mm: str,
        db_gold: str,
        utils: Utils,
    ):
        self.spark         = spark
        self.bucket_silver = bucket_silver
        self.bucket_gold   = bucket_gold
        self.yyyy          = yyyy
        self.mm            = mm
        self.db_gold       = db_gold
        self.utils         = utils

        self.last_day = date(
            int(yyyy), int(mm),
            calendar.monthrange(int(yyyy), int(mm))[1]
        ).strftime("%Y-%m-%d")

        self.gold_path = (
            f"s3://{self.bucket_gold}/{_GOLD_BASE_PATH}/{_PATH_FACT_POLIZA}/"
        )

        self.athena_cleanup_paths = []

    # ------------------------------------------------------------------ #
    #                           PUBLIC ENTRY                              #
    # ------------------------------------------------------------------ #

    def run(self) -> Dict:
        """
        Execute the full siniestros update for the current period.

        Steps:
            1.  Load previsional ALFAPRD records from fact_poliza_emision.
                Keep only the most recent record per num_poliza.
            2.  Build siniestros matching key from num_poliza.
            3.  Load siniestros PAGOS accumulated <= last day of period.
            4.  Load siniestros RESERVA accumulated <= last day of period.
            5.  Apply PAGOS > RESERVA hierarchy → nuevo_valor_asegurado.
            6.  Filter to clients with siniestros activity (valor > 0).
            7.  Execute Delta UPDATE on fact_poliza_emision using record id.

        Returns:
            dict with update summary.
        """
        logger.info("=" * 80, self.CLASS_NAME)
        logger.info(
            f"PREVISIONAL SINIESTROS UPDATE | period: {self.yyyy}-{self.mm}",
            self.CLASS_NAME,
        )
        logger.info("=" * 80, self.CLASS_NAME)

        # Step 1: Load most recent previsional records from Gold
        logger.info("Step 1: Loading most recent previsional records from Gold", self.CLASS_NAME)
        df_polizas = self._load_last_previsional_polizas()

        count_polizas = df_polizas.count()
        logger.info(f"Previsional polizas found: {count_polizas:,}", self.CLASS_NAME)

        if count_polizas == 0:
            logger.info("No previsional polizas in Gold — nothing to update", self.CLASS_NAME)
            return self._empty_result()

        # DEBUG: check if target poliza is present after Step 1
        _debug = df_polizas.filter(F.col("num_poliza") == "9-1-98468659")
        _debug_count = _debug.count()
        logger.info(f"[DEBUG] Step 1 — '9-1-98468659' found: {_debug_count}", self.CLASS_NAME)
        if _debug_count > 0:
            for row in _debug.collect():
                logger.info(f"[DEBUG] Step 1 row: {row.asDict()}", self.CLASS_NAME)

        # Step 2: Build siniestros matching key
        logger.info("Step 2: Building siniestros matching key from num_poliza", self.CLASS_NAME)
        df_polizas = self._build_siniestros_key(df_polizas)

        # DEBUG: check pk_siniestro built for target poliza
        _debug = df_polizas.filter(F.col("num_poliza") == "9-1-98468659")
        if _debug.count() > 0:
            for row in _debug.collect():
                logger.info(f"[DEBUG] Step 2 — pk_siniestro built: {row['pk_siniestro']}", self.CLASS_NAME)

        # Step 3: Load PAGOS
        logger.info("Step 3: Loading siniestros PAGOS", self.CLASS_NAME)
        df_pagos = self._load_siniestros_pagos()

        # Step 4: Load RESERVA
        logger.info("Step 4: Loading siniestros RESERVA", self.CLASS_NAME)
        df_reserva = self._load_siniestros_reserva()

        # Step 5: Apply hierarchy PAGOS > RESERVA
        logger.info("Step 5: Applying PAGOS > RESERVA hierarchy", self.CLASS_NAME)

        # DEBUG: check if pk_client '1-98468659' exists in PAGOS before join
        _debug_pagos = df_pagos.filter(F.col("pk_client") == "1-98468659")
        logger.info(f"[DEBUG] PAGOS — '1-98468659' found: {_debug_pagos.count()}", self.CLASS_NAME)
        if _debug_pagos.count() > 0:
            for row in _debug_pagos.collect():
                logger.info(f"[DEBUG] PAGOS row: {row.asDict()}", self.CLASS_NAME)

        # DEBUG: show a sample of pk_client values from PAGOS to check format
        logger.info("[DEBUG] PAGOS pk_client sample:", self.CLASS_NAME)
        for row in df_pagos.limit(5).collect():
            logger.info(f"[DEBUG] PAGOS sample: {row.asDict()}", self.CLASS_NAME)

        df_with_valor = self._apply_siniestros_hierarchy(df_polizas, df_pagos, df_reserva)

        # DEBUG: check nuevo_valor_asegurado for target poliza after hierarchy
        _debug = df_with_valor.filter(F.col("num_poliza") == "9-1-98468659")
        if _debug.count() > 0:
            for row in _debug.collect():
                logger.info(f"[DEBUG] After hierarchy: {row.asDict()}", self.CLASS_NAME)

        # Step 6: Filter to clients with actual siniestros activity
        df_to_update = df_with_valor.filter(
            F.col("nuevo_valor_asegurado").isNotNull() &
            (F.col("nuevo_valor_asegurado") > 0)
        )

        count_to_update = df_to_update.count()
        logger.info(
            f"Polizas with siniestros activity to update: {count_to_update:,}",
            self.CLASS_NAME,
        )

        # DEBUG: check if target poliza made it through hierarchy
        _debug = df_to_update.filter(F.col("num_poliza") == "9-1-98468659")
        _debug_count = _debug.count()
        logger.info(f"[DEBUG] Step 6 — '9-1-98468659' in update set: {_debug_count}", self.CLASS_NAME)
        if _debug_count > 0:
            for row in _debug.collect():
                logger.info(f"[DEBUG] Step 6 row: {row.asDict()}", self.CLASS_NAME)

        if count_to_update == 0:
            logger.info("No siniestros activity found — nothing to update", self.CLASS_NAME)
            return self._empty_result()

        # Step 7: Execute Delta UPDATE
        logger.info("Step 7: Executing Delta UPDATE on fact_poliza_emision", self.CLASS_NAME)
        resultado = self._execute_delta_update(df_to_update)

        logger.info("=" * 80, self.CLASS_NAME)
        logger.info("UPDATE SUMMARY", self.CLASS_NAME)
        logger.info(f"  Period:            {self.yyyy}-{self.mm}", self.CLASS_NAME)
        logger.info(f"  Polizas evaluated: {count_polizas:,}", self.CLASS_NAME)
        logger.info(f"  Polizas updated:   {resultado['registros_actualizados']:,}", self.CLASS_NAME)
        logger.info("=" * 80, self.CLASS_NAME)

        return resultado

    # ------------------------------------------------------------------ #
    #                         GOLD READERS                                #
    # ------------------------------------------------------------------ #

    def _load_last_previsional_polizas(self) -> DataFrame:
        """
        Load the most recent previsional ALFAPRD record per num_poliza
        from fact_poliza_emision.

        Filters:
            - id_fuente = '2'  (ALFAPRD)
            - id_producto matches previsional products in dim_producto

        Window: ROW_NUMBER OVER (PARTITION BY num_poliza ORDER BY
                anio_proceso DESC, mes_proceso DESC) = 1

        Returns:
            DataFrame with columns: id, num_poliza, anio_proceso, mes_proceso.
        """
        path_dim_producto = (
            f"s3://{self.bucket_gold}/GOLD/CLIENTES_GOLD/"
            "GOLD-CARACT_CLIENTES-DIM_PRODUCTO/"
        )

        logger.info(f"Reading dim_producto from: {path_dim_producto}", self.CLASS_NAME)
        df_dim_producto = (
            self.spark.read.format("parquet").load(path_dim_producto)
            .filter(F.lower(F.col("descripcion")).contains("previsional"))
            .select(F.col("id").alias("id_producto_previsional"))
            .distinct()
        )

        ids_previsional = [r.id_producto_previsional for r in df_dim_producto.collect()]
        logger.info(f"Previsional product ids: {ids_previsional}", self.CLASS_NAME)

        logger.info(f"Reading fact_poliza_emision from: {self.gold_path}", self.CLASS_NAME)
        df_poliza = self.utils.read_full_delta(self.spark, self.gold_path)

        df_previsional = df_poliza.filter(
            (F.col("id_fuente") == "2") &
            (F.col("id_producto").isin(ids_previsional))
        )

        # Keep only the most recent record per num_poliza
        window_last = Window.partitionBy("num_poliza").orderBy(
            F.col("anio_proceso").desc(),
            F.col("mes_proceso").desc(),
        )

        return (
            df_previsional
            .withColumn("_rn", F.row_number().over(window_last))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .select("id", "num_poliza", "anio_proceso", "mes_proceso")
        )

    # ------------------------------------------------------------------ #
    #                      MATCHING KEY BUILDER                           #
    # ------------------------------------------------------------------ #

    def _build_siniestros_key(self, df: DataFrame) -> DataFrame:
        """
        Extract the siniestros matching key from num_poliza.

        num_poliza format: NID_POLIZA-TIPO_ID-CEDULA  (e.g. '9-1-98468659')
        Siniestros key:    TIPO_ID-CEDULA              (e.g. '1-98468659')

        Extraction: split by '-', take index [1] and [2], rejoin with '-'.

        Args:
            df: DataFrame with num_poliza column.

        Returns:
            DataFrame with pk_siniestro column added.
        """
        # num_poliza format: NID_POLIZA-TIPO_ID-CEDULA (e.g. '9-1-98468659')
        # Siniestros key:    CONCAT('1', '-', CEDULA)  (e.g. '1-98468659')
        # TIPO_ID is always '1' (homologated). Extract CEDULA from split index [2].
        parts = F.split(F.col("num_poliza"), "-")
        return df.withColumn(
            "pk_siniestro",
            F.concat(F.lit("1"), F.lit("-"), parts.getItem(2))
        )

    # ------------------------------------------------------------------ #
    #                        SINIESTROS LOADERS                          #
    # ------------------------------------------------------------------ #

    def _load_siniestros_pagos(self) -> DataFrame:
        """
        Load accumulated PAGOS from Gold siniestros,
        filtered to fecha_transaccion <= last day of current period.
        Aggregated by TIPO_ID-CEDULA (pk_client).
        """
        athena_output = (
            f"s3://{self.bucket_silver}/phase_bronze/athena-views/"
            f"reconciliation-pagos-{self.yyyy}{self.mm}/"
        )
        self.athena_cleanup_paths.append(athena_output)

        query = f"""
        SELECT
            CONCAT('1', '-', CEDULA) AS pk_client,
            CAST(SUM(T1.valor_transaccion) AS DECIMAL(18,5)) AS valor_pagos
        FROM "{self.db_gold}"."gold_caract_siniestros_fact_movimiento_view" T1
        INNER JOIN "{self.db_gold}"."gold_caract_siniestros_fact_poliza_pf" T2
            ON T1.id_poliza_pk = T2.id_poliza_pk
        INNER JOIN "{self.db_gold}"."gold_caract_siniestros_dim_tipo_transaccion" T3
            ON CAST(T1.id_tipo_transaccion_fk AS INT) = T3.id_tipo_transaccion_pk
        WHERE T1.id_fuente_fk IN ('2', '3')
        AND LOWER(T3.tipo_de_transaccion) = 'pago'
        AND T1."Fecha de Transaccion" <= DATE '{self.last_day}'
        GROUP BY CEDULA
        """

        try:
            df = self.utils.execute_athena_query_to_dataframe(
                spark=self.spark,
                query=query,
                database=self.db_gold,
                output_location=athena_output,
            )
            logger.info(f"Siniestros PAGOS loaded: {df.count():,}", self.CLASS_NAME)
            return df
        except Exception as e:
            logger.error(f"Failed to load PAGOS — returning empty: {str(e)}", self.CLASS_NAME)
            return self.spark.createDataFrame(
                [], "pk_client string, valor_pagos decimal(18,5)"
            )

    def _load_siniestros_reserva(self) -> DataFrame:
        """
        Load accumulated RESERVA from Gold siniestros,
        filtered to ano <= yyyy AND mes <= mm.
        Aggregated by TIPO_ID-CEDULA (pk_client).
        """
        athena_output = (
            f"s3://{self.bucket_silver}/phase_bronze/athena-views/"
            f"reconciliation-reserva-{self.yyyy}{self.mm}/"
        )
        self.athena_cleanup_paths.append(athena_output)

        query = f"""
        WITH query_reserva AS (
            SELECT
                ano, mes, estado,
                SUM(monto) AS sum_monto,
                id_siniestro_fk
            FROM "{self.db_gold}"."gold_caract_siniestros_fact_estado_siniestro"
            WHERE id_fuente_fk IN ('2', '3')
            AND estado = 'En Reserva'
            GROUP BY ano, mes, estado, id_siniestro_fk
        )
        SELECT
            CONCAT('1', '-', T2.cedula) AS pk_client,
            CAST(SUM(T1.sum_monto) AS DECIMAL(18,5)) AS valor_reserva
        FROM query_reserva T1
        INNER JOIN "{self.db_gold}"."gold_caract_siniestros_fact_siniestros_pf" T2
            ON T1.id_siniestro_fk = T2.id_siniestro_pk
        WHERE T1.ano <= {self.yyyy}
        AND T1.mes <= {self.mm}
        GROUP BY T2.cedula
        """

        try:
            df = self.utils.execute_athena_query_to_dataframe(
                spark=self.spark,
                query=query,
                database=self.db_gold,
                output_location=athena_output,
            )
            logger.info(f"Siniestros RESERVA loaded: {df.count():,}", self.CLASS_NAME)
            return df
        except Exception as e:
            logger.error(f"Failed to load RESERVA — returning empty: {str(e)}", self.CLASS_NAME)
            return self.spark.createDataFrame(
                [], "pk_client string, valor_reserva decimal(18,5)"
            )

    # ------------------------------------------------------------------ #
    #                     SINIESTROS HIERARCHY                            #
    # ------------------------------------------------------------------ #

    def _apply_siniestros_hierarchy(
        self,
        df_polizas: DataFrame,
        df_pagos: DataFrame,
        df_reserva: DataFrame,
    ) -> DataFrame:
        """
        Join polizas with PAGOS and RESERVA, apply PAGOS > RESERVA hierarchy.

        Args:
            df_polizas: Previsional polizas with pk_siniestro column.
            df_pagos:   Aggregated PAGOS per pk_client.
            df_reserva: Aggregated RESERVA per pk_client.

        Returns:
            DataFrame with nuevo_valor_asegurado column.
            NULL means no siniestros activity — will be filtered out.
        """
        df = df_polizas.join(
            df_pagos.withColumnRenamed("pk_client", "_pk_pagos"),
            df_polizas["pk_siniestro"] == F.col("_pk_pagos"),
            "left",
        ).drop("_pk_pagos")

        # DEBUG: check join result for target client
        _debug = df.filter(F.col("num_poliza") == "9-1-98468659")
        if _debug.count() > 0:
            for row in _debug.collect():
                logger.info(f"[DEBUG] After PAGOS join: {row.asDict()}", "SiniestrosHierarchy")

        # DEBUG: force trim and re-check if whitespace is the issue
        _pk = df_pagos.filter(
            F.trim(F.col("pk_client")) == "1-98468659"
        )
        logger.info(f"[DEBUG] PAGOS with trim match: {_pk.count()}", "SiniestrosHierarchy")

        # DEBUG: check length of pk_client vs pk_siniestro
        _len_pagos = df_pagos.filter(F.col("pk_client").contains("98468659")).select(
            F.col("pk_client"),
            F.length(F.col("pk_client")).alias("len")
        )
        _len_pol = df_polizas.filter(F.col("pk_siniestro") == "1-98468659").select(
            F.col("pk_siniestro"),
            F.length(F.col("pk_siniestro")).alias("len")
        )
        logger.info(f"[DEBUG] PAGOS pk_client length:", "SiniestrosHierarchy")
        for row in _len_pagos.collect():
            logger.info(f"[DEBUG]   '{row['pk_client']}' len={row['len']}", "SiniestrosHierarchy")
        logger.info(f"[DEBUG] Poliza pk_siniestro length:", "SiniestrosHierarchy")
        for row in _len_pol.collect():
            logger.info(f"[DEBUG]   '{row['pk_siniestro']}' len={row['len']}", "SiniestrosHierarchy")

        df = df.join(
            df_reserva.withColumnRenamed("pk_client", "_pk_reserva"),
            df["pk_siniestro"] == F.col("_pk_reserva"),
            "left",
        ).drop("_pk_reserva")

        return df.withColumn(
            "nuevo_valor_asegurado",
            F.when(
                F.col("valor_pagos").isNotNull() & (F.col("valor_pagos") > 0),
                F.col("valor_pagos").cast(DecimalType(18, 4)),
            ).when(
                F.col("valor_reserva").isNotNull() & (F.col("valor_reserva") > 0),
                F.col("valor_reserva").cast(DecimalType(18, 4)),
            ).otherwise(
                F.lit(None).cast(DecimalType(18, 4))
            ),
        )

    # ------------------------------------------------------------------ #
    #                         DELTA UPDATE                                #
    # ------------------------------------------------------------------ #

    def _execute_delta_update(self, df_to_update: DataFrame) -> Dict:
        """
        Execute Delta UPDATE on fact_poliza_emision.

        Updates only valor_asegurado on the specific record identified
        by its id (MD5). All other fields remain untouched.

        Uses MERGE on id → whenMatched → UPDATE valor_asegurado only.

        Args:
            df_to_update: DataFrame with columns: id, nuevo_valor_asegurado.

        Returns:
            Dict with update summary metrics.
        """
        delta_table = DeltaTable.forPath(self.spark, self.gold_path)
        count_antes = delta_table.toDF().count()

        # Minimal source: only id + new value
        df_source = df_to_update.select(
            F.col("id").alias("_upd_id"),
            F.col("nuevo_valor_asegurado").cast(DecimalType(18, 4)).alias("_upd_valor"),
        )

        count_source = df_source.count()
        logger.info(
            f"Executing MERGE/UPDATE | records to update: {count_source:,}",
            self.CLASS_NAME,
        )

        (
            delta_table.alias("target")
            .merge(
                df_source.alias("source"),
                "target.id = source._upd_id",
            )
            .whenMatchedUpdate(
                set={"valor_asegurado": "source._upd_valor"}
            )
            .execute()
        )

        # Regenerate symlink manifest so Athena sees the updated values.
        # The Gold table is registered in Glue Data Catalog via symlink format
        # manifest (_symlink_format_manifest). Delta does not update the manifest
        # automatically on UPDATE — it must be explicitly regenerated after
        # every write operation that modifies data.
        logger.info("Regenerating symlink manifest for Athena compatibility", self.CLASS_NAME)
        delta_table.generate("symlink_format_manifest")
        logger.info("Symlink manifest regenerated", self.CLASS_NAME)

        # DEBUG: read back from Delta and confirm value was updated
        _debug = delta_table.toDF().filter(F.col("num_poliza") == "9-1-98468659")
        _debug_count = _debug.count()
        logger.info(f"[DEBUG] Post-update — '9-1-98468659' in Delta: {_debug_count}", self.CLASS_NAME)
        if _debug_count > 0:
            for row in _debug.select("num_poliza", "valor_asegurado", "anio_proceso", "mes_proceso").collect():
                logger.info(f"[DEBUG] Post-update row: {row.asDict()}", self.CLASS_NAME)

        count_despues = delta_table.toDF().count()

        logger.info(
            f"UPDATE completed | updated: {count_source:,} | "
            f"Gold total before: {count_antes:,} | after: {count_despues:,}",
            self.CLASS_NAME,
        )

        return {
            "estrategia":             "update_valor_asegurado",
            "periodo":                f"{self.yyyy}-{self.mm}",
            "registros_evaluados":    count_source,
            "registros_actualizados": count_source,
            "registros_finales":      count_despues,
        }

    # ------------------------------------------------------------------ #
    #                            HELPERS                                  #
    # ------------------------------------------------------------------ #

    def _empty_result(self) -> Dict:
        return {
            "estrategia":             "SKIP",
            "periodo":                f"{self.yyyy}-{self.mm}",
            "registros_evaluados":    0,
            "registros_actualizados": 0,
            "registros_finales":      0,
        }

    def cleanup(self):
        """Delete temporary Athena output paths from S3."""
        if not self.athena_cleanup_paths:
            return
        try:
            import boto3
            s3 = boto3.client("s3")
            for path in self.athena_cleanup_paths:
                parts  = path.replace("s3://", "").split("/", 1)
                bucket = parts[0]
                prefix = parts[1] if len(parts) > 1 else ""
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    if "Contents" in page:
                        objects = [{"Key": o["Key"]} for o in page["Contents"]]
                        if objects:
                            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
            self.athena_cleanup_paths.clear()
            logger.info("Athena cleanup completed", self.CLASS_NAME)
        except Exception as e:
            logger.error(f"Cleanup error (non-fatal): {str(e)}", self.CLASS_NAME)


# ============================================================================
#                               GLUE ENTRY POINT
# ============================================================================

def main():
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "BUCKET_SILVER",
        "BUCKET_GOLD",
        "YYYY",
        "MM",
        "DB_GOLD",
    ])

    sc           = SparkContext()
    glue_context = GlueContext(sc)
    spark        = glue_context.spark_session
    job          = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    yyyy          = args["YYYY"]
    mm            = args["MM"].zfill(2)
    bucket_silver = args["BUCKET_SILVER"]
    bucket_gold   = args["BUCKET_GOLD"]
    db_gold       = args["DB_GOLD"]

    logger.info(f"Job started | period: {yyyy}-{mm}", "main")
    logger.info(f"Bucket Silver: {bucket_silver}", "main")
    logger.info(f"Bucket Gold:   {bucket_gold}", "main")
    logger.info(f"DB Gold:       {db_gold}", "main")

    utils     = Utils()
    processor = PrevisionalSiniestrosUpdateProcessor(
        spark=spark,
        bucket_silver=bucket_silver,
        bucket_gold=bucket_gold,
        yyyy=yyyy,
        mm=mm,
        db_gold=db_gold,
        utils=utils,
    )

    try:
        resultado = processor.run()
        logger.info(
            f"Job completed successfully | "
            f"updated: {resultado['registros_actualizados']:,}",
            "main",
        )
    except Exception as e:
        logger.error(f"Job failed: {str(e)}", "main")
        logger.error(traceback.format_exc(), "main")
        raise
    finally:
        processor.cleanup()
        job.commit()


if __name__ == "__main__":
    main()
 