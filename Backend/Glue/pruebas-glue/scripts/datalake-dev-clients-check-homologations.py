"""
================================================================================
                AWS Glue Job: Silver Layer Processing - ALFAPRD MVP2
                            REFACTORED VERSION
================================================================================

Description:
    Production ETL pipeline for Silver layer data processing from ALFAPRD source.
    Implements comprehensive data transformation, quality validation, and 
    harmonization workflows for insurance products, policies, and client data.

    NUEVO: Clase SilverAlfaprdProcessor para mejor organización y mantenimiento.

Author:         Juan Alejandro Carrillo Jaimes
Email:          jnij@gft.com
Created:        2025-12-03
Last Updated:   2025-01-19
Version:        2.0.0
Environment:    AWS Glue / Production

Key Changes:
    - Refactorizado a clase SilverAlfaprdProcessor
    - Concatenaciones solo para ARPIS y PREVISIONAL (no RENTAS)
    - Clientes RENTAS filtrados por pólizas activas con Delta Lake
    - Métodos organizados por responsabilidad

Dependencies:
    - alfaprd_staging_processor_mvp2: Staging data processor
    - data_harmonization_manager: Data harmonization logic
    - mpv2_transformer_proccessor: Data cleanup and transformations
    - s3_handler: S3 operations manager with Delta support
    - utils: Configuration and helper utilities
    - cleanup: Data cleanup utilities

================================================================================
"""

# --------- AWS GLUE FUNCTIONS ---------------#
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
# ---------- SPARK FUNCTIONS ---------------#
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, lit, coalesce, when, substring, 
    max as spark_max, sum as spark_sum, count,
    row_number, desc, concat, to_date, date_format, current_date,
    add_months, lpad, split, trim, upper
)
from pyspark.sql.types import DecimalType
# ---------- PYTHON LIBRARIES -------------#
import sys
import calendar
from datetime import datetime, timedelta
import pandas as pd
# ---------- CUSTOM LIBRARIES -------------#
from alfaprd_staging_processor_mvp2 import AlfprdStagingProcessorMVP2
from cleanup import CleanUp
from data_harmonization_manager import DataHarmonizationManager
from logger_config import logger
from mpv2_transformer_proccessor import DataCleanupProcessor
from s3_handler import S3Handler
from utils import Utils


class SilverAlfaprdProcessor:
    """
    Main processor class for ALFAPRD Silver Layer ETL.
    
    Responsibilities:
        - Staging data processing
        - Data transformations and harmonization
        - Delta Lake operations
        - S3 persistence
    
    Architecture:
        - Follows Single Responsibility Principle
        - Delegates specific operations to specialized handlers
        - Supports both historic and incremental modes
    """
    
    def __init__(self, args: dict, spark, glue_context):
        """
        Initialize SilverAlfaprdProcessor with AWS Glue configuration.
        
        Args:
            args (dict): Resolved Glue job arguments
            spark: SparkSession instance
            glue_context: GlueContext instance
        """
        self.args = args
        self.spark = spark
        self.glue_context = glue_context
        
        # Initialize handlers
        self.s3_handler = S3Handler(args['BUCKET_SILVER'], glue_context)
        self.utils = Utils()
        self.transformer = DataCleanupProcessor(spark, glue_context)
        self.harmonization_manager = DataHarmonizationManager()
        self.cleanup = CleanUp()
        
        # Configure Spark optimizations
        self.utils.configure_spark_optimizations(
            spark, "SILVER", "datalake-dev-clients-silver-job-alfaprd-mvp2"
        )
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        
        # Load configuration
        self.config = self.utils.load_staging_config(
            s3_config_path=args['S3_STATUS_CONFIG_PATH'],
            spark=spark
        )
        
        logger.info("SilverAlfaprdProcessor initialized successfully")
    
    # ========================================================================
    #                    STAGING PROCESSING
    # ========================================================================
    
    def process_staging(self) -> dict:
        """
        Process staging tables using AlfprdStagingProcessorMVP2.
        
        Modes:
            - FALLEN_MONTH: Process previous month automatically
            - HISTORIC: Process date range from START_DATE to END_DATE
        
        Returns:
            dict: Execution periods with start/end dates and processing mode
        """
        # CASO 1: STAGING DESHABILITADO
        if self.args['LOAD_STG_SILVER'].lower() != "true":
            logger.info("=" * 80)
            logger.info("STAGING PROCESSING DISABLED - Extracting periods from parameters")
            logger.info("=" * 80)
            return self._extract_periods_from_parameters()
        
        # CASO 2: STAGING HABILITADO
        logger.info("=" * 80)
        logger.info("STAGING PROCESSING ENABLED - Starting staging workflow")
        logger.info("=" * 80)
        return self._execute_staging_workflow()
    
    def _extract_periods_from_parameters(self) -> dict:
        """Extract processing periods from job parameters (staging disabled)."""
        try:
            historic_param = self.args.get('HISTORIC_PARAM', '0')
            
            if historic_param == '1':
                # HISTORIC MODE
                start_param = self.args['START_DATE']
                end_param = self.args['END_DATE']
                
                start_year = int(start_param[:4])
                start_month = int(start_param[4:6])
                end_year = int(end_param[:4])
                end_month = int(end_param[4:6])
                
                start_date = datetime(start_year, start_month, 1).strftime("%d/%m/%Y 00:00:00")
                last_day = calendar.monthrange(end_year, end_month)[1]
                end_date = datetime(end_year, end_month, last_day).strftime("%d/%m/%Y 23:59:59")
                
                logger.info(f"HISTORIC MODE: Using period {start_param} to {end_param}")
            else:
                # FALLEN MONTH MODE
                today = datetime.now()
                first_day_current_month = today.replace(day=1)
                last_day_previous_month = first_day_current_month - timedelta(days=1)
                first_day_previous_month = last_day_previous_month.replace(day=1)
                
                start_date = first_day_previous_month.strftime("%d/%m/%Y 00:00:00")
                end_date = last_day_previous_month.strftime("%d/%m/%Y 23:59:59")
                
                start_year = first_day_previous_month.year
                start_month = first_day_previous_month.month
                end_year = first_day_previous_month.year
                end_month = first_day_previous_month.month
                
                logger.info(f"FALLEN MONTH MODE: Processing {first_day_previous_month.strftime('%Y%m')}")
            
            execution_periods = {
                "start_period": f"{start_year}{start_month:02d}",
                "end_period": f"{end_year}{end_month:02d}",
                "start_date": start_date,
                "end_date": end_date,
                "mode": "HISTORIC" if historic_param == '1' else "FALLEN_MONTH",
                "result": {"status": "STAGING_SKIPPED", "message": "Staging disabled"}
            }
            
            self._log_period_info(execution_periods)
            return execution_periods
            
        except Exception as e:
            logger.error(f"Error extracting periods: {str(e)}")
            return self._get_fallback_period()
    
    def _execute_staging_workflow(self) -> dict:
        """Execute complete staging workflow (staging enabled)."""
        try:
            historic_param = self.args.get('HISTORIC_PARAM', '0')
            
            # Determine period range
            if historic_param == '1':
                start_param = self.args['START_DATE']
                end_param = self.args['END_DATE']
                start_year = int(start_param[:4])
                start_month = int(start_param[4:6])
                end_year = int(end_param[:4])
                end_month = int(end_param[4:6])
                
                start_date = datetime(start_year, start_month, 1).strftime("%d/%m/%Y 00:00:00")
                last_day = calendar.monthrange(end_year, end_month)[1]
                end_date = datetime(end_year, end_month, last_day).strftime("%d/%m/%Y 23:59:59")
                
                logger.info(f"HISTORIC MODE: Processing {start_param} to {end_param}")
            else:
                today = datetime.now()
                first_day_current_month = today.replace(day=1)
                last_day_previous_month = first_day_current_month - timedelta(days=1)
                first_day_previous_month = last_day_previous_month.replace(day=1)
                
                start_date = first_day_previous_month.strftime("%d/%m/%Y 00:00:00")
                end_date = last_day_previous_month.strftime("%d/%m/%Y 23:59:59")
                start_year = first_day_previous_month.year
                start_month = first_day_previous_month.month
                end_year = first_day_previous_month.year
                end_month = first_day_previous_month.month
                
                logger.info(f"FALLEN MONTH MODE: Processing {first_day_previous_month.strftime('%Y%m')}")
            
            self._log_period_info({
                "start_period": f"{start_year}{start_month:02d}",
                "end_period": f"{end_year}{end_month:02d}",
                "start_date": start_date,
                "end_date": end_date
            })
            
            # Initialize and run processor
            processor_stg = AlfprdStagingProcessorMVP2(
                spark_session=self.spark,
                glue_context=self.glue_context,
                bucket_bronze=self.args['BUCKET_BRONZE'],
                bucket_silver=self.args['BUCKET_SILVER'],
                config=self.config,
                crawler=self.args['CRAWLER_SILVER']
            )
            
            result = processor_stg.run_pipeline(
                job_id=self.args['JOB_PROCESS_ID'],
                start_year=start_year,
                start_month=start_month,
                end_year=end_year,
                end_month=end_month,
                execute_flow=3
            )
            
            execution_periods = {
                "start_period": f"{start_year}{start_month:02d}",
                "end_period": f"{end_year}{end_month:02d}",
                "start_date": start_date,
                "end_date": end_date,
                "mode": "HISTORIC" if historic_param == '1' else "FALLEN_MONTH",
                "result": result
            }
            
            logger.success("Staging processing completed successfully")
            return execution_periods
            
        except Exception as e:
            logger.error(f"Error in staging workflow: {str(e)}")
            raise
    
    def _get_fallback_period(self) -> dict:
        """Get fallback period (current month) in case of error."""
        current_date = datetime.now()
        current_period = current_date.strftime('%Y%m')
        
        logger.warning(f"Using current period as fallback: {current_period}")
        
        return {
            "start_period": current_period,
            "end_period": current_period,
            "start_date": current_date.strftime("%d/%m/%Y 00:00:00"),
            "end_date": current_date.strftime("%d/%m/%Y 23:59:59"),
            "mode": "FALLBACK",
            "result": {"status": "PARAMETER_ERROR"}
        }
    
    def _log_period_info(self, periods: dict):
        """Log period configuration information."""
        print("=" * 80)
        print("PERIOD RANGE CONFIGURATION:")
        print(f"  START_DATE: {periods.get('start_date', 'N/A')}")
        print(f"  END_DATE: {periods.get('end_date', 'N/A')}")
        print(f"  START_PERIOD: {periods.get('start_period', 'N/A')}")
        print(f"  END_PERIOD: {periods.get('end_period', 'N/A')}")
        print(f"  MODE: {periods.get('mode', 'N/A')}")
        print("=" * 80)
    
    # ========================================================================
    #                    DATA TRANSFORMATIONS
    # ========================================================================
    
    def transform_clientes_poliza(self, df):
        """
        Transform poliza field for ALFAPRD_CLIENTES_MVP2.
        
        ⚠️ IMPORTANTE: Solo aplica para ARPIS y PREVISIONAL, NO para RENTAS.
        
        Logic:
        1. Split poliza by '-' and take first element
        2. Reconstruct: poliza = CONCAT(first_element, '-', tipo_id, '-', num_id)
        
        Example:
            Input:  poliza='9-2-1022990929', tipo='2', num='1022990929', origen='previsional'
            Output: poliza='9-1-1022990929' (after homologation tipo='1')
        
        Args:
            df: DataFrame with poliza, cod_tipo_identificacion, numero_identificacion, origen
        
        Returns:
            DataFrame with transformed poliza field
        """
        required_cols = ['poliza', 'cod_tipo_identificacion', 'numero_identificacion', 'origen']
        
        if not all(c in df.columns for c in required_cols):
            logger.warning("Required fields not found for poliza transformation, skipping")
            return df
        
        logger.info("=" * 80)
        logger.info("TRANSFORMING POLIZA FIELD (ARPIS + PREVISIONAL ONLY)")
        logger.info("=" * 80)
        
        # Separar poliza por guion y tomar primer elemento
        df = df.withColumn(
            "poliza_primer_elemento",
            trim(split(col("poliza"), "-").getItem(0))
        )
        
        # Reconstruir SOLO para arpis y previsional
        df = df.withColumn(
            "poliza",
            when(
                upper(col("origen")).isin(["ARPIS", "PREVISIONAL"]),
                concat(
                    coalesce(col("poliza_primer_elemento"), lit("")),
                    lit("-"),
                    coalesce(col("cod_tipo_identificacion"), lit("")),
                    lit("-"),
                    coalesce(col("numero_identificacion"), lit(""))
                )
            ).otherwise(col("poliza"))  # RENTAS se mantiene original
        ).drop("poliza_primer_elemento")
        
        logger.success("Poliza transformation completed (ARPIS + PREVISIONAL)")
        logger.info("RENTAS poliza preserved as original")
        
        return df
    
    def transform_polizas_numero_poliza(self, df):
        """
        Transform numero_poliza field for ALFAPRD_POLIZAS_MVP2.
        
        ⚠️ IMPORTANTE: Solo aplica para ARPIS y PREVISIONAL, NO para RENTAS.
        
        Logic: numero_poliza = CONCAT(numero_poliza_matriz, '-', tipo_id, '-', num_id)
        
        Args:
            df: DataFrame with numero_poliza_matriz, cod_tipo_identificacion, 
                numero_identificacion, origen
        
        Returns:
            DataFrame with transformed numero_poliza field
        """
        required_cols = ['numero_poliza_matriz', 'cod_tipo_identificacion', 
                        'numero_identificacion', 'origen']
        
        if not all(c in df.columns for c in required_cols):
            logger.warning("Required fields not found for numero_poliza transformation, skipping")
            return df
        
        logger.info("=" * 80)
        logger.info("TRANSFORMING NUMERO_POLIZA FIELD (ARPIS + PREVISIONAL ONLY)")
        logger.info("=" * 80)
        
        # Concatenar SOLO para arpis y previsional
        df = df.withColumn(
            "numero_poliza",
            when(
                upper(col("origen")).isin(["ARPIS", "PREVISIONAL"]),
                concat(
                    coalesce(col("numero_poliza_matriz"), lit("")),
                    lit("-"),
                    coalesce(col("cod_tipo_identificacion"), lit("")),
                    lit("-"),
                    coalesce(col("numero_identificacion"), lit(""))
                )
            ).otherwise(col("numero_poliza"))  # RENTAS se mantiene original
        )
        
        logger.success("Numero_poliza transformation completed (ARPIS + PREVISIONAL)")
        logger.info("RENTAS numero_poliza preserved as original")
        
        return df
    
    def transform_clientes_tomador_previsional_final(self, df):
        """
        Transform TOMADOR PREVISIONAL to default identification (NIT PORVENIR).
        
        ⚠️ CRÍTICO: Este método debe ejecutarse DESPUÉS de homologación y 
        reconstrucción de poliza.
        
        Business Rule:
            - PREVISIONAL TOMADOR: cod_tipo='3' (NIT), numero='800144331' (PORVENIR)
            - Poliza ya está correctamente construida con cod_tipo homologado
        
        Args:
            df: DataFrame with origen, rol, cod_tipo_identificacion, numero_identificacion
        
        Returns:
            DataFrame with updated identification for TOMADOR PREVISIONAL
        """
        required_cols = ['origen', 'rol', 'cod_tipo_identificacion', 'numero_identificacion']
        
        if not all(c in df.columns for c in required_cols):
            logger.warning("Required fields not found for TOMADOR transformation, skipping")
            return df
        
        logger.info("=" * 80)
        logger.info("FINAL STEP: Updating TOMADOR PREVISIONAL identification")
        logger.info("Poliza field remains unchanged (already constructed)")
        logger.info("=" * 80)
        
        df = df.withColumn(
            "cod_tipo_identificacion",
            when(
                (upper(col("origen")) == "PREVISIONAL") & (col("rol") == "3"),
                lit("3")  # NIT
            ).otherwise(col("cod_tipo_identificacion"))
        )
        
        df = df.withColumn(
            "numero_identificacion",
            when(
                (upper(col("origen")) == "PREVISIONAL") & (col("rol") == "3"),
                lit("800144331")  # PORVENIR
            ).otherwise(col("numero_identificacion"))
        )
        
        logger.success("TOMADOR PREVISIONAL updated to 3-800144331")
        
        return df
    
    # ========================================================================
    #                    GOLD LAYER INTEGRATION
    # ========================================================================
    
    def load_previsional_siniestros_from_gold(self, athena_output_location: str):
        """
        Load Previsional siniestros (PAGOS) from Gold layer via Athena.
        
        Args:
            athena_output_location: S3 path for Athena query results
        
        Returns:
            DataFrame with pk_client, valor_transaccion, Nro_Siniestros
        """
        try:
            logger.info("=" * 80)
            logger.info("LOADING PREVISIONAL SINIESTROS (PAGOS) FROM GOLD")
            logger.info("=" * 80)
            
            query = f"""
            SELECT
                T1.id_fuente_fk,
                T1.id_poliza_pk,
                T2.unidad_de_negocio,
                "Fecha de Transaccion",
                '1' AS "Tipo_Identificacion",
                CEDULA AS "Numero_Documento_Asegurado",
                T1.id_tipo_transaccion_fk,
                T3.tipo_de_transaccion,
                count(id_siniestro_fk) AS Nro_Siniestros,
                sum(valor_transaccion) AS valor_transaccion
            FROM "{self.args['BUCKET_GOLD']}"."gold_caract_siniestros_fact_movimiento_view" T1
            INNER JOIN "{self.args['BUCKET_GOLD']}"."gold_caract_siniestros_fact_poliza_pf" T2
                ON T1.id_poliza_pk = T2.id_poliza_pk
            INNER JOIN "{self.args['BUCKET_GOLD']}"."gold_caract_siniestros_dim_tipo_transaccion" T3
                ON CAST(T1.id_tipo_transaccion_fk AS INT) = T3.id_tipo_transaccion_pk
                AND T1.id_fuente_fk = '3'
            WHERE UPPER(T3.tipo_de_transaccion) = 'PAGO'
            GROUP BY "Fecha de Transaccion", T1.id_fuente_fk, T1.id_poliza_pk,
                T2.unidad_de_negocio, CEDULA, T1.id_tipo_transaccion_fk, T3.tipo_de_transaccion
            """
            
            df_siniestros = self.utils.execute_athena_query_to_dataframe(
                spark=self.spark,
                query=query,
                database=self.args['BUCKET_GOLD'],
                output_location=athena_output_location
            )
            
            df_siniestros = df_siniestros \
                .withColumn("valor_transaccion", col("valor_transaccion").cast(DecimalType(18, 2))) \
                .withColumn(
                    "pk_client",
                    concat(col("Tipo_Identificacion"), lit("-"), col("Numero_Documento_Asegurado"))
                ) \
                .select("pk_client", "valor_transaccion", "Nro_Siniestros")
            
            count = df_siniestros.count()
            logger.success(f"Previsional siniestros (PAGOS) loaded: {count:,} records")
            
            return df_siniestros
            
        except Exception as e:
            logger.error(f"Failed to load siniestros (PAGOS): {str(e)}")
            return None
    
    def load_previsional_reserva_siniestros_from_gold(self, athena_output_location: str):
        """
        Load Previsional reserva siniestros from Gold layer via Athena.
        
        Args:
            athena_output_location: S3 path for Athena query results
        
        Returns:
            DataFrame with pk_client, valor_transaccion
        """
        try:
            logger.info("=" * 80)
            logger.info("LOADING PREVISIONAL RESERVA SINIESTROS FROM GOLD")
            logger.info("=" * 80)
            
            query = f"""
            WITH query_reserva AS (
                SELECT
                    ano, mes, estado,
                    SUM(monto) AS sum_monto,
                    id_siniestro_fk
                FROM "{self.args['BUCKET_GOLD']}"."gold_caract_siniestros_fact_estado_siniestro"
                WHERE id_fuente_fk = '3' AND estado = 'En Reserva'
                GROUP BY ano, mes, estado, id_siniestro_fk
            )
            SELECT 
                T1.sum_monto,
                T1.id_siniestro_fk,
                '1' AS "tipo_identificacion_asegurado",
                T2.cedula AS "numero_documento_asegurado"
            FROM query_reserva T1
            INNER JOIN "{self.args['BUCKET_GOLD']}"."gold_caract_siniestros_fact_siniestros_pf" T2
            ON T1.id_siniestro_fk = T2.id_siniestro_pk
            """
            
            df_reserva = self.utils.execute_athena_query_to_dataframe(
                spark=self.spark,
                query=query,
                database=self.args['BUCKET_GOLD'],
                output_location=athena_output_location
            )
            
            df_reserva = df_reserva \
                .withColumn("valor_transaccion", col("sum_monto").cast(DecimalType(18, 2))) \
                .withColumn(
                    "pk_client",
                    concat(col("tipo_identificacion_asegurado"), lit("-"), 
                          col("numero_documento_asegurado"))
                ) \
                .select("pk_client", "valor_transaccion")
            
            count = df_reserva.count()
            logger.success(f"Previsional reserva siniestros loaded: {count:,} records")
            
            return df_reserva
            
        except Exception as e:
            logger.error(f"Failed to load reserva siniestros: {str(e)}")
            return None
    
    def update_previsional_polizas_with_siniestros(self, df_polizas_harmonized):
        """
        Update PREVISIONAL polizas with siniestros data (PAGOS and RESERVA).
        
        Three-tier update logic:
        1. PAGOS (actual payments) - Priority 1
        2. RESERVA (reserved amounts) - Priority 2
        3. DEFAULT valor_asegurado - Priority 3
        
        Args:
            df_polizas_harmonized: Harmonized polizas DataFrame
        
        Returns:
            Tuple: (updated_dataframe, athena_pagos_path, athena_reserva_path)
        """
        bucket_assets = self.args['BUCKET_ASSETS']
        athena_output_pagos = f"s3://{bucket_assets}/phase_bronze/athena-views/valor-asegurado-pagos-gold/"
        athena_output_reserva = f"s3://{bucket_assets}/phase_bronze/athena-views/valor-asegurado-reserva-gold/"
        
        try:
            # Filter PREVISIONAL vs others
            df_previsional = df_polizas_harmonized.filter(col("origen") == "previsional")
            df_otros = df_polizas_harmonized.filter(col("origen") != "previsional")
            
            logger.info("Processing PREVISIONAL polizas for valor_asegurado update")
            
            # Create PK_POLIZA_CLIENTE
            df_previsional = df_previsional.withColumn(
                "pk_poliza_cliente",
                concat(col("cod_tipo_identificacion"), lit("-"), col("numero_identificacion"))
            )
            
            # Load siniestros data
            df_siniestros_pagos = self.load_previsional_siniestros_from_gold(athena_output_pagos)
            df_siniestros_reserva = self.load_previsional_reserva_siniestros_from_gold(athena_output_reserva)
            
            # Handle None cases
            if df_siniestros_pagos is None:
                df_siniestros_pagos = self.spark.createDataFrame(
                    [], "pk_client: string, valor_transaccion: decimal(18,2), Nro_Siniestros: long"
                )
            
            if df_siniestros_reserva is None:
                df_siniestros_reserva = self.spark.createDataFrame(
                    [], "pk_client: string, valor_transaccion: decimal(18,2)"
                )
            
            # Materialize Athena data (CRITICAL!)
            logger.info("Materializing Athena data to avoid cleanup conflicts")
            df_siniestros_pagos.cache()
            df_siniestros_reserva.cache()
            
            pagos_count = df_siniestros_pagos.count()
            reserva_count = df_siniestros_reserva.count()
            
            logger.info(f"Materialized PAGOS: {pagos_count:,} records")
            logger.info(f"Materialized RESERVA: {reserva_count:,} records")
            
            # Rename columns to avoid conflicts
            df_pagos_renamed = df_siniestros_pagos \
                .withColumnRenamed("pk_client", "pk_pagos") \
                .withColumnRenamed("valor_transaccion", "valor_pagos") \
                .withColumnRenamed("Nro_Siniestros", "nro_siniestros_pagos")
            
            df_reserva_renamed = df_siniestros_reserva \
                .withColumnRenamed("pk_client", "pk_reserva") \
                .withColumnRenamed("valor_transaccion", "valor_reserva")
            
            # Join with PAGOS
            logger.info("Joining with PAGOS data")
            df_with_pagos = df_previsional.join(
                df_pagos_renamed,
                df_previsional["pk_poliza_cliente"] == df_pagos_renamed["pk_pagos"],
                "left"
            )
            
            # Join with RESERVA
            logger.info("Joining with RESERVA data")
            df_with_all = df_with_pagos.join(
                df_reserva_renamed,
                df_with_pagos["pk_poliza_cliente"] == df_reserva_renamed["pk_reserva"],
                "left"
            )
            
            # Apply three-tier logic
            logger.info("Applying three-tier update logic")
            df_updated = df_with_all.withColumn(
                "valor_asegurado",
                when(col("valor_pagos").isNotNull(), col("valor_pagos"))  # Tier 1
                .when(col("valor_reserva").isNotNull(), col("valor_reserva"))  # Tier 2
                .otherwise(col("valor_asegurado"))  # Tier 3
            )
            
            # Add source indicator
            df_updated = df_updated.withColumn(
                "valor_asegurado_source",
                when(col("valor_pagos").isNotNull(), lit("PAGOS"))
                .when(col("valor_reserva").isNotNull(), lit("RESERVA"))
                .otherwise(lit("DEFAULT"))
            )
            
            # Drop auxiliary columns
            df_previsional_final = df_updated.drop(
                "pk_pagos", "valor_pagos", "nro_siniestros_pagos",
                "pk_reserva", "valor_reserva"
            )
            
            logger.success("PREVISIONAL valor_asegurado updated successfully")
            
            # Union back
            df_final = df_previsional_final.unionByName(df_otros, allowMissingColumns=True)
            
            # Unpersist cached data
            df_siniestros_pagos.unpersist()
            df_siniestros_reserva.unpersist()
            
            return df_final, athena_output_pagos, athena_output_reserva
            
        except Exception as e:
            logger.error(f"Error updating PREVISIONAL polizas: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
    
    # ========================================================================
    #                    RENTAS CLIENTES FILTERING (NEW)
    # ========================================================================
    
    def filter_rentas_clientes_by_active_policies(self, df_clientes_harmonized):
        """
        Filter RENTAS clients to keep only those with ACTIVE policies.
        
        Strategy with Delta Lake:
        1. Read POLIZAS_ALFAPRD_SILVER (Delta format)
        2. Filter RENTAS polizas with estado_poliza = 'ACTIVO'
        3. Extract valid client identifications
        4. Filter df_clientes to keep only valid RENTAS clients
        5. Preserve ARPIS and PREVISIONAL clients unchanged
        
        Args:
            df_clientes_harmonized: Harmonized clients DataFrame
        
        Returns:
            DataFrame with filtered RENTAS clients (only active policies)
        """
        try:
            logger.info("=" * 80)
            logger.info("FILTERING RENTAS CLIENTS BY ACTIVE POLICIES")
            logger.info("=" * 80)
            
            # Read POLIZAS from Silver (Delta format)
            polizas_path = f"s3://{self.args['BUCKET_SILVER']}/POLIZAS_PRODUCTO/ALFAPRD/POLIZAS_ALFAPRD_SILVER/"
            
            logger.info(f"Reading polizas from: {polizas_path}")
            df_polizas = self.spark.read.format("delta").load(polizas_path)
            
            # Filter RENTAS ACTIVAS
            df_rentas_activas = df_polizas.filter(
                (upper(col("origen")) == "RENTAS") & 
                (upper(col("estado_poliza")) == "ACTIVO")
            )
            
            # Create PK for join (tipo-numero)
            df_rentas_activas = df_rentas_activas.withColumn(
                "pk_cliente_poliza",
                concat(col("cod_tipo_identificacion"), lit("-"), col("numero_identificacion"))
            ).select("pk_cliente_poliza").distinct()
            
            activas_count = df_rentas_activas.count()
            logger.info(f"Found {activas_count:,} RENTAS clients with active policies")
            
            # Create PK in clients DataFrame
            df_clientes = df_clientes_harmonized.withColumn(
                "pk_cliente",
                concat(col("cod_tipo_identificacion"), lit("-"), col("numero_identificacion"))
            )
            
            # Separate RENTAS vs others
            df_rentas_clientes = df_clientes.filter(upper(col("origen")) == "RENTAS")
            df_otros_clientes = df_clientes.filter(upper(col("origen")) != "RENTAS")
            
            rentas_original_count = df_rentas_clientes.count()
            logger.info(f"Original RENTAS clients: {rentas_original_count:,}")
            
            # Filter RENTAS by active policies
            df_rentas_filtered = df_rentas_clientes.join(
                df_rentas_activas,
                df_rentas_clientes["pk_cliente"] == df_rentas_activas["pk_cliente_poliza"],
                "inner"
            ).drop("pk_cliente_poliza", "pk_cliente")
            
            rentas_filtered_count = df_rentas_filtered.count()
            logger.info(f"Filtered RENTAS clients: {rentas_filtered_count:,}")
            logger.info(f"Removed: {rentas_original_count - rentas_filtered_count:,} inactive clients")
            
            # Union back
            df_final = df_rentas_filtered.unionByName(
                df_otros_clientes.drop("pk_cliente"),
                allowMissingColumns=True
            )
            
            logger.success("RENTAS clients filtered successfully")
            logger.info("ARPIS and PREVISIONAL clients preserved")
            
            return df_final
            
        except Exception as e:
            logger.error(f"Error filtering RENTAS clients: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
    
    def delete_and_reload_rentas_clientes_delta(self, df_clientes_filtered):
        """
        Delete RENTAS clients from Delta table and reload filtered data.
        
        Strategy:
        1. Use Delta DELETE to remove origen='rentas' records
        2. APPEND filtered RENTAS clients
        3. Preserve ARPIS and PREVISIONAL data
        
        Args:
            df_clientes_filtered: DataFrame with filtered RENTAS clients
        
        Returns:
            dict: Operation summary
        """
        try:
            logger.info("=" * 80)
            logger.info("DELTA OPERATION: DELETE + RELOAD RENTAS CLIENTS")
            logger.info("=" * 80)
            
            clientes_path = f"s3://{self.args['BUCKET_SILVER']}/POLIZAS_PRODUCTO/ALFAPRD/CLIENTES_ALFAPRD_SILVER/"
            
            # Step 1: DELETE RENTAS records
            logger.info("Step 1: Deleting RENTAS clients from Delta table")
            self.s3_handler.delete_delta_data(
                s3_target_path=clientes_path,
                condition="UPPER(origen) = 'RENTAS'"
            )
            logger.success("RENTAS clients deleted successfully")
            
            # Step 2: APPEND filtered RENTAS clients
            logger.info("Step 2: Appending filtered RENTAS clients")
            df_rentas_only = df_clientes_filtered.filter(upper(col("origen")) == "RENTAS")
            
            rentas_count = df_rentas_only.count()
            logger.info(f"Appending {rentas_count:,} filtered RENTAS clients")
            
            # Add partition columns
            current_date = datetime.now()
            yyyy, mm = current_date.strftime("%Y"), current_date.strftime("%m")
            
            df_rentas_partitioned = df_rentas_only \
                .withColumn("yyyy", lit(yyyy)) \
                .withColumn("mm", lit(mm))
            
            self.s3_handler.save_files_to_s3(
                df=df_rentas_partitioned,
                s3_target_path=clientes_path,
                format="delta",
                mode="append",
                partition_keys=["yyyy", "mm"]
            )
            
            logger.success("Filtered RENTAS clients appended successfully")
            
            # Update crawler
            self.utils.update_crawler_targets(
                crawler_name=self.args['CRAWLER_SILVER'],
                s3_path=clientes_path,
                source_type="delta"
            )
            
            return {
                "status": "SUCCESS",
                "operation": "DELETE_AND_RELOAD",
                "records_appended": rentas_count,
                "message": "RENTAS clients filtered by active policies"
            }
            
        except Exception as e:
            logger.error(f"Error in Delta DELETE+RELOAD operation: {str(e)}")
            raise
    
    # ========================================================================
    #                    HOMOLOGATION
    # ========================================================================
    
    def load_homologation_data(self):
        """
        Load and process homologation data from Excel.
        
        Returns:
            DataFrame: Processed homologation table
        """
        try:
            path = self.args['S3_HOMOLOGATION_PATH']
            sheet = self.args['HOMOLOGATION_SHEET']
            
            logger.info(f"Loading homologations from: {path}")
            
            df_xl = pd.read_excel(path, sheet_name=sheet)
            df_xl = df_xl.where(pd.notnull(df_xl), None)
            
            for col_name in df_xl.columns:
                df_xl[col_name] = df_xl[col_name].apply(
                    lambda x: str(x) if x is not None else None
                )
            
            df_homologaciones = self.spark.createDataFrame(df_xl)
            
            df_homologaciones = df_homologaciones.selectExpr(
                "tabla as tabla",
                "campo_referencia as campo_referencia",
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
                "campo_homologacion",
                """
                CASE 
                    WHEN cod_fuente_fk IS NULL THEN NULL     
                    WHEN LOWER(TRIM(cod_fuente_fk)) = 'nan' THEN NULL    
                    WHEN cod_fuente_fk LIKE '%.0' THEN REGEXP_REPLACE(cod_fuente_fk, '\\.0$', '')       
                    ELSE cod_fuente_fk 
                END as cod_fuente_fk
                """
            )
            
            # Clean n/a values
            field_filters = ['linea_credito', 'canal_venta']
            tables_filter = ['productos_alfaprd_silver','polizas_alfaprd_silver']
            
            df_homo_na = df_homologaciones.withColumn(
                "valor_referencia",
                when(
                    (col("campo_referencia").isin(field_filters)) &
                    (col("tabla").isin(tables_filter)),
                    lit("n/a")
                ).otherwise(col("valor_referencia"))
            )
            
            df_cod_tipo = df_homo_na.filter(col("campo_referencia") == "cod_tipo_identificacion")
            df_otros = df_homo_na.filter(col("campo_referencia") != "cod_tipo_identificacion")
            
            df_cod_tipo = self.cleanup.normalize_columns(df_cod_tipo, "valor_referencia")
            
            logger.success(f"Homologations loaded successfully from {path}")
            
            return df_cod_tipo.unionByName(df_otros)
            
        except Exception as e:
            logger.error(f"Error loading homologation data: {str(e)}")
            raise
    
    # ========================================================================
    #                    PERSISTENCE
    # ========================================================================
    
    def save_to_silver(self, df, output_name: str):
        """
        Save DataFrame to Silver layer in Delta format.

        Supports different strategies:
        - PRODUCTOS: OVERWRITE (small table, full load)
        - POLIZAS: APPEND incremental (detect new records)
        - CLIENTES: APPEND incremental (partitioned by period)

        Args:
            df: DataFrame to save
            output_name: Output table name
        """
        try:
            s3_prefix = f"POLIZAS_PRODUCTO/ALFAPRD/{output_name}/"
            s3_target_path = f"s3://{self.args['BUCKET_SILVER']}/{s3_prefix}"

            logger.info(f"Saving to Silver: {output_name}")

            # ═══════════════════════════════════════════════════════════
            # STRATEGY 1: PRODUCTOS (OVERWRITE - Full load)
            # ═══════════════════════════════════════════════════════════
            if output_name == "PRODUCTOS_ALFAPRD_SILVER":
                logger.info("PRODUCTOS: Using OVERWRITE mode (full load)")

                # Delete existing data
                self.s3_handler.delete_bucket_files(s3_prefix, format="delta")

                # Add partition columns
                current_date = datetime.now()
                yyyy, mm = current_date.strftime("%Y"), current_date.strftime("%m")

                df_partitioned = df \
                    .withColumn("yyyy", lit(yyyy)) \
                    .withColumn("mm", lit(mm))

                # Save to Delta
                self.s3_handler.save_files_to_s3(
                    df=df_partitioned,
                    s3_target_path=s3_target_path,
                    format="delta",
                    mode="overwrite",
                    partition_keys=["yyyy", "mm"]
                )

                logger.success(f"PRODUCTOS saved successfully: {output_name}")

                # Clean STG after successful save
                logger.info("Cleaning PRODUCTOS STG after successful Silver load")
                stg_prefix = "POLIZAS_PRODUCTO/ALFAPRD/PRODUCTOS_STG/"
                self.s3_handler.delete_bucket_files(stg_prefix, format="delta")
                logger.success("PRODUCTOS STG cleaned")

            # ═══════════════════════════════════════════════════════════
            # STRATEGY 2: POLIZAS (APPEND - Incremental with dedup)
            # ═══════════════════════════════════════════════════════════
            elif output_name == "POLIZAS_ALFAPRD_SILVER":
                logger.info("POLIZAS: Using APPEND mode (incremental with dedup)")

                # Add partition columns
                current_date = datetime.now()
                yyyy, mm = current_date.strftime("%Y"), current_date.strftime("%m")

                df_partitioned = df \
                    .withColumn("yyyy", lit(yyyy)) \
                    .withColumn("mm", lit(mm))

                # Check if table exists
                try:
                    df_existing = self.spark.read.format("delta").load(s3_target_path)
                    table_exists = df_existing.limit(1).count() > 0
                except:
                    table_exists = False

                if not table_exists:
                    # First load: OVERWRITE to create table
                    logger.info("POLIZAS: First load - creating table")

                    self.s3_handler.save_files_to_s3(
                        df=df_partitioned,
                        s3_target_path=s3_target_path,
                        format="delta",
                        mode="overwrite",
                        partition_keys=["yyyy", "mm"]
                    )

                    records_saved = df_partitioned.count()
                    logger.success(f"POLIZAS first load completed: {records_saved:,} records")

                else:
                    # Incremental load: Detect NEW records and APPEND
                    logger.info("POLIZAS: Incremental load - detecting new records")

                    # Merge keys to identify unique polizas
                    merge_keys = [
                        "numero_poliza_matriz",
                        "numero_poliza",
                        "cod_cobertura"
                    ]

                    # LEFT ANTI JOIN: Only records NOT in Silver
                    df_new_only = df_partitioned.join(
                        df_existing.select(*merge_keys).distinct(),
                        on=merge_keys,
                        how="left_anti"
                    )

                    new_records_count = df_new_only.count()
                    total_records = df_partitioned.count()

                    logger.info(f"New records detected: {new_records_count:,} out of {total_records:,}", "SilverProcessor")

                    if new_records_count == 0:
                        logger.info("No new records to append", "SilverProcessor")
                    else:
                        # APPEND only new records
                        self.s3_handler.save_files_to_s3(
                            df=df_new_only,
                            s3_target_path=s3_target_path,
                            format="delta",
                            mode="append",
                            partition_keys=["yyyy", "mm"]
                        )

                        reduction_pct = round((1 - new_records_count / total_records) * 100, 2)
                        logger.success(f"POLIZAS appended: {new_records_count:,} new records")
                        logger.info(f"Data reduction: {reduction_pct}% already existed")

                # POLIZAS STG is NOT cleaned (needed for next incremental load)
                logger.info("POLIZAS STG maintained (required for incremental)")

            # ═══════════════════════════════════════════════════════════
            # STRATEGY 3: CLIENTES (APPEND - Incremental partitioned)
            # ═══════════════════════════════════════════════════════════
            elif output_name == "CLIENTES_ALFAPRD_SILVER":
                logger.info("CLIENTES: Using APPEND mode (preserves historical data)")

                # Add partition columns
                current_date = datetime.now()
                yyyy, mm = current_date.strftime("%Y"), current_date.strftime("%m")

                df_partitioned = df \
                    .withColumn("yyyy", lit(yyyy)) \
                    .withColumn("mm", lit(mm))

                # APPEND without deleting
                self.s3_handler.save_files_to_s3(
                    df=df_partitioned,
                    s3_target_path=s3_target_path,
                    format="delta",
                    mode="append",
                    partition_keys=["yyyy", "mm"]
                )

                logger.success(f"CLIENTES data appended to Silver")
                logger.info("CLIENTES STG maintained (managed by Lifecycle Policy)")

            else:
                # Default behavior for other tables
                logger.warning(f"Unknown output: {output_name}, using default OVERWRITE")

                self.s3_handler.delete_bucket_files(s3_prefix, format="delta")

                current_date = datetime.now()
                yyyy, mm = current_date.strftime("%Y"), current_date.strftime("%m")

                df_partitioned = df \
                    .withColumn("yyyy", lit(yyyy)) \
                    .withColumn("mm", lit(mm))

                self.s3_handler.save_files_to_s3(
                    df=df_partitioned,
                    s3_target_path=s3_target_path,
                    format="delta",
                    mode="overwrite",
                    partition_keys=["yyyy", "mm"]
                )

            # Update crawler
            self.utils.update_crawler_targets(
                crawler_name=self.args['CRAWLER_SILVER'],
                s3_path=s3_target_path,
                source_type="delta"
            )

            logger.success(f"Silver save completed: {output_name}")

        except Exception as e:
            logger.error(f"Error saving to Silver: {str(e)}")
            raise
    
    # ========================================================================
    #                    FACTS PROCESSING
    # ========================================================================
    
    def process_facts(self, df_homologaciones, execution_periods: dict):
        """
        Process all ALFAPRD facts with homologation and transformations.
        
        Args:
            df_homologaciones: Homologation DataFrame
            execution_periods: Dict with start/end periods
        """
        try:
            base_path = f's3://{self.args["BUCKET_SILVER"]}/'
            fact_configs = self.config.get('fact_configs', {})
            
            alfaprd_facts = [
                #"ALFAPRD_PRODUCTOS_MVP2",
                #"ALFAPRD_POLIZAS_MVP2",
                "ALFAPRD_CLIENTES_MVP2"
            ]
            
            active_facts = [f['name'] for f in self.config.get('fact', []) if f.get('status') == 1]
            active_facts = [fact for fact in active_facts if fact in alfaprd_facts]
            
            if not active_facts:
                logger.info("No active ALFAPRD facts to process")
                return
            
            athena_cleanup_paths = []
            
            # Process each fact
            for fact_name in active_facts:
                if fact_name not in fact_configs:
                    logger.warning(f"Config not found for: {fact_name}")
                    continue
                
                logger.info("=" * 80)
                logger.info(f"PROCESSING FACT: {fact_name}")
                logger.info("=" * 80)
                
                fact_config = fact_configs[fact_name]
                
                # Load source data
                full_path = f"{base_path}{fact_config['path']}"
                if fact_name == "ALFAPRD_CLIENTES_MVP2":
                    # Extraer yyyy y mm del periodo de ejecución
                    start_period = execution_periods.get('start_period')
                    if start_period:
                        yyyy = str(start_period)[:4]
                        mm = str(start_period)[4:6]

                        logger.info(f"CLIENTES: Reading from STG partition yyyy={yyyy}, mm={mm}")

                        # Leer solo la partición específica
                        df_source = self.utils.read_delta_with_partitions(
                            spark=self.spark,
                            s3_target_path=full_path,
                            partition_filters={"yyyy": yyyy, "mm": mm}
                        )
                    else:
                        logger.warning("CLIENTES: No start_period provided, reading full STG")
                        df_source = self.utils.read_full_delta(self.spark, full_path)
                else:
                    # Para POLIZAS y PRODUCTOS: Leer full (comportamiento actual)
                    df_source = self.utils.read_full_delta(self.spark, full_path)

                df_source = self.transformer.date_normalice(df_source)
                df_source.persist()
                
                # Process each output
                for output_config in fact_config.get('outputs', []):
                    output_name = output_config['name']
                    
                    logger.info(f"Processing output: {output_name}")
                    
                    # Apply cleanup
                    clean_method = output_config.get('clean_method')
                    if clean_method:
                        df_clean = getattr(self.transformer, clean_method)(df_source)
                    else:
                        df_clean = df_source
                    
                    # Apply selection
                    select_method = output_config.get('select_method')
                    if select_method:
                        df_clean = getattr(self.transformer, select_method)(df_clean)
                    
                    # Apply specific transformations
                    if fact_name == "ALFAPRD_CLIENTES_MVP2":
                        logger.info("Applying CLIENTES transformations")
                        df_clean = self.transform_clientes_poliza(df_clean)
                        df_clean = self.transform_clientes_tomador_previsional_final(df_clean)
                    
                    elif fact_name == "ALFAPRD_POLIZAS_MVP2":
                        logger.info("Applying POLIZAS transformations")
                        df_clean = self.transform_polizas_numero_poliza(df_clean)
                    
                    df_clean.persist()
                    
                    # Apply homologation
                    harmonization_table = output_config['harmonization_table']
                    
                    if harmonization_table != "pendiente_definir":
                        logger.info(f"Applying homologation for {harmonization_table}")
                        df_harmonized = self.harmonization_manager.homologacion_i(
                            harmonization_table,
                            df_homologaciones,
                            df_clean,
                            "homologacion",
                            "cod_fuente_fk"
                        )
                    else:
                        logger.info("Skipping homologation (pending definition)")
                        df_harmonized = df_clean
                    
                    df_harmonized.persist()
                    
                    # Special processing for POLIZAS
                    if output_name == "POLIZAS_ALFAPRD_SILVER":
                        logger.info("Applying PREVISIONAL siniestros update")
                        df_harmonized, athena_pagos, athena_reserva = \
                            self.update_previsional_polizas_with_siniestros(df_harmonized)
                        athena_cleanup_paths.extend([athena_pagos, athena_reserva])
                    
                    # Special processing for CLIENTES (NEW)
                    if output_name == "CLIENTES_ALFAPRD_SILVER":
                        logger.info("=" * 80)
                        logger.info("APPLYING RENTAS CLIENTS FILTERING (NEW)")
                        logger.info("=" * 80)
                        df_harmonized = self.filter_rentas_clientes_by_active_policies(df_harmonized)
                    
                    # Add period columns
                    df_harmonized = df_harmonized \
                        .withColumn("periodo_inicio", lit(execution_periods['start_period'])) \
                        .withColumn("periodo_fin", lit(execution_periods['end_period']))
                    
                    # Save to Silver
                    self.save_to_silver(df_harmonized, output_name)
                    
                    df_clean.unpersist()
                    df_harmonized.unpersist()
                
                df_source.unpersist()
            
            # Cleanup Athena results
            if athena_cleanup_paths:
                logger.info("=" * 80)
                logger.info("CLEANING UP ATHENA RESULTS")
                logger.info("=" * 80)
                
                for athena_path in athena_cleanup_paths:
                    try:
                        self.utils.cleanup_athena_results(athena_path)
                        logger.info(f"Cleaned up: {athena_path}")
                    except Exception as cleanup_error:
                        logger.warning(f"Cleanup failed for {athena_path}: {str(cleanup_error)}")
                
                logger.success("Athena cleanup completed")
            
        except Exception as e:
            logger.error(f"Error processing facts: {str(e)}")
            raise
    
    # ========================================================================
    #                    MAIN EXECUTION
    # ========================================================================
    
    def run(self):
        """
        Execute complete Silver layer processing workflow.
        
        Steps:
        1. Load homologation data
        2. Process staging (optional)
        3. Process facts with transformations
        4. Apply special filters (RENTAS clients)
        5. Save to Delta Lake format
        """
        start_time = datetime.now()
        
        try:
            logger.info("=" * 80)
            logger.info("STARTING SILVER ALFAPRD PROCESSOR")
            logger.info(f"Job ID: {self.args['JOB_PROCESS_ID']}")
            logger.info("=" * 80)
            
            # Step 1: Load homologation data
            logger.info("Step 1: Loading homologation data")
            df_homologaciones = self.load_homologation_data()
            # ============================================================================
            # INSPECCIONAR HOMOLOGACIONES EN MEMORIA
            # ============================================================================
            
            # Ver cuántas filas tiene en total
            print(f"Total filas: {df_homologaciones.count()}")
            
            # Filtrar solo polizas_alfaprd_silver
            filters = ['clientes_alfaprd_silver']
            df_polizas = df_homologaciones.filter(col("tabla").isin(filters))
            print(f"Filas para polizas_alfaprd_silver: {df_polizas.count()}")
            
            # Ver qué tablas están disponibles
            print("\nTablas disponibles:")
            df_homologaciones.select("tabla").distinct().show(truncate=False)
            
            # Ver campos a homologar para polizas
            print("\nCampos de polizas_alfaprd_silver:")
            df_polizas.groupBy("campo_referencia").count().orderBy("campo_referencia").show(truncate=False)
            
            # Ver ejemplos de homologaciones
            print("\nEjemplos de homologaciones:")
            df_polizas.select(
                "campo_referencia",
                "valor_referencia",
                "valor_homologacion",
                "cod_fuente_fk"
            ).show(50, truncate=False)
            
            # Ver campos específicos
            print("\nEstado poliza:")
            df_polizas.filter(col("campo_referencia") == "estado_poliza").show(truncate=False)
            
            print("\nTipo poliza:")
            df_polizas.filter(col("campo_referencia") == "tipo_poliza").show(truncate=False)
            
            print("\nCon cod_fuente_fk:")
            df_polizas.filter(col("cod_fuente_fk").isNotNull()).show(50, truncate=False)
            
            print("\nSin cod_fuente_fk:")
            df_polizas.filter(col("cod_fuente_fk").isNull()).show(50, truncate=False)
            
            
            
            # Execution summary
            execution_time = (datetime.now() - start_time).total_seconds()
            
            logger.info("=" * 80)
            logger.success("SILVER ALFAPRD PROCESSOR COMPLETED SUCCESSFULLY")
            logger.info(f"Total execution time: {execution_time:.2f} seconds")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error("=" * 80)
            logger.error("SILVER ALFAPRD PROCESSOR FAILED")
            logger.error(f"Error: {str(e)}")
            logger.error("=" * 80)
            raise


# ============================================================================
#                    GLUE JOB ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Get parameters
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'BUCKET_SILVER',
        'BUCKET_BRONZE',
        'BUCKET_GOLD',
        'JOB_PROCESS_ID',
        'S3_HOMOLOGATION_PATH',
        'S3_STATUS_CONFIG_PATH',
        'LOAD_STG_SILVER',
        'CRAWLER_SILVER',
        'HISTORIC_PARAM',
        'START_DATE',
        'END_DATE',
        'HOMOLOGATION_SHEET',
        'BUCKET_ASSETS'
    ])
    
    # Initialize Glue
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Execute processor
    processor = SilverAlfaprdProcessor(args, spark, glueContext)
    processor.run()
    
    # Commit job
    job.commit()