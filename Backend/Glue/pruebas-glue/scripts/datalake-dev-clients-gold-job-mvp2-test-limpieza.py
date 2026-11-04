"""
================================================================================
                    Gold Layer - Incremental Processing with Delta Lake
================================================================================

Description:
    Consolidates multiple sources (Cardiff, ACSELE, ALFAPRD) and generates 
    fact tables with dimensional enrichment using incremental Delta Lake loading.

Author:         Data Team
Created:        2024-12-10
Last Updated:   2025-01-29 (Added usar_particiones parameter)
Version:        2.2.0 (MODIFIED - usar_particiones support)
Environment:    AWS Glue / Production

Main Features:
    - Incremental loading with MERGE strategy
    - Multi-source consolidation (Cardiff, ACSELE, ALFAPRD)
    - Automatic dimensional enrichment
    - Delta Lake ACID transactions
    - Intelligent deduplication for ACSELE
    - Date partitioning (anio_proceso/mes_proceso)

Glue Parameters:
    --JOB_NAME: Job identifier
    --BUCKET_SILVER: Silver layer bucket
    --BUCKET_GOLD: Gold layer bucket
    --S3_CONFIG_PATH: Configuration JSON path
    --ANIO: Year to process (optional, default: current)
    --MES: Month to process (optional, default: current)

================================================================================
"""

import sys
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta

# AWS Glue imports
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# PySpark imports
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, monotonically_increasing_id,
    concat, concat_ws, lower, coalesce, when, sum as spark_sum, row_number,md5, abs as spark_abs, hash as spark_hash
)
from pyspark.sql.types import (
    StringType, IntegerType, DecimalType, DateType, 
    TimestampType, StructType, StructField
)
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Delta Lake
from delta.tables import DeltaTable

# Custom libraries
from transformer_proccessor_gold import TransformerGold
from utils import Utils
from s3_handler import S3Handler
from delta_gold_loader import DeltaGoldLoader


# ============================================================================
#                           MAIN PROCESSOR CLASS
# ============================================================================

class GoldPolizasIncrementalProcessor:
    """
    Processes data from multiple sources and generates Gold fact tables
    with incremental Delta Lake loading.
    """
    
    def __init__(self, spark, glue_context, job_name, bucket_silver, bucket_gold, db_gold_athena):
        """Initialize processor with required AWS Glue components."""
        self.spark = spark
        self.glue_context = glue_context
        self.job_name = job_name
        self.bucket_silver = bucket_silver
        self.bucket_gold = bucket_gold
        self.db_gold_athena = db_gold_athena
        
        # Initialize helpers
        self.utils = Utils()
        self.logger = self._setup_logging()
        self.gold = TransformerGold()
        self.s3_handler = S3Handler(bucket_gold, glue_context)
        self.gold_loader = DeltaGoldLoader(spark, modo_debug=False)
        
        # Cache and config
        self.dimensions_cache = {}
        self.config = None
        # Athena cleanup
        self.athena_cleanup_paths = []
        self.logger.info("✓ Gold Processor initialized")
    
    # ========================================================================
    #                          SETUP & CONFIGURATION
    # ========================================================================
    
    def _setup_logging(self):
        """Configure logging with proper formatting."""
        logger = logging.getLogger(f"GoldPolizas-{self.job_name}")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def load_config(self, s3_config_path: str) -> Dict:
        """Load JSON configuration from S3."""
        try:
            self.logger.info(f"Loading config: {s3_config_path}")
            
            df_config = self.spark.read.option("multiline", "true").json(s3_config_path)
            config_row = df_config.collect()[0]
            config = self._row_to_dict(config_row.asDict())
            
            self.logger.info("✓ Configuration loaded")
            return config
            
        except Exception as e:
            self.logger.error(f"Error loading config: {str(e)}")
            raise
    def _generar_id_determinista(self, df: DataFrame, llaves_negocio: List[str]) -> DataFrame:
        """
        Genera un ID determinístico basado en hash de las llaves de negocio.
        
        Ventajas:
        - El mismo registro siempre tiene el mismo ID
        - Compatible con MERGE incremental
        - No genera duplicados en JOINs
        - Reproducible entre ejecuciones
        
        Args:
            df: DataFrame con los datos
            llaves_negocio: Columnas que identifican únicamente el registro
        
        Returns:
            DataFrame con columna Id determinística
        """
        from pyspark.sql.types import StringType
        from pyspark.sql import functions as F
        
        self.logger.info("→ Generating deterministic ID...")
        
        if not llaves_negocio:
            # Fallback: usar todas las columnas excepto metadatos
            columnas_excluir = ['anio_proceso', 'mes_proceso', 'fecha_carga_gold']
            llaves_negocio = [c for c in df.columns if c not in columnas_excluir]
            self.logger.warning(
                f"⚠️  No business keys defined, using all columns: {len(llaves_negocio)} cols"
            )
        
        # Validar que las llaves existan en el DataFrame
        llaves_disponibles = [llave for llave in llaves_negocio if llave in df.columns]
        
        if not llaves_disponibles:
            self.logger.error(f"❌ None of the business keys exist in DataFrame!")
            self.logger.error(f"  Business keys: {llaves_negocio}")
            self.logger.error(f"  Available columns: {df.columns}")
            raise ValueError("Cannot generate deterministic ID without business keys")
        
        if len(llaves_disponibles) < len(llaves_negocio):
            llaves_faltantes = set(llaves_negocio) - set(llaves_disponibles)
            self.logger.warning(f"⚠️  Missing business keys: {llaves_faltantes}")
        
        self.logger.info(f"  Business keys used: {llaves_disponibles}")
        
        # Generar ID usando MD5 hash de las llaves concatenadas
        df_con_id = df.withColumn(
            "Id",
            md5(
                concat_ws("||", *[
                    F.coalesce(col(llave).cast(StringType()), lit("NULL")) 
                    for llave in llaves_disponibles
                ])
            )
        )
        
        # Validar que no haya colisiones (IDs duplicados)
        count_total = df_con_id.count()
        count_unique_ids = df_con_id.select("Id").distinct().count()
        
        if count_unique_ids < count_total:
            duplicados = count_total - count_unique_ids
            pct_duplicados = (duplicados / count_total) * 100
            self.logger.warning(
                f"⚠️  ID collisions detected: {duplicados:,} ({pct_duplicados:.2f}%)"
            )
            self.logger.warning("  → This may indicate incomplete business keys")
        else:
            self.logger.info(f"  ✓ Unique IDs: {count_unique_ids:,} (no collisions)")
        
        return df_con_id



    def _row_to_dict(self, obj):
        """Convert Spark Row to regular dict recursively."""
        from pyspark.sql.types import Row
        
        if isinstance(obj, Row):
            return {k: self._row_to_dict(v) for k, v in obj.asDict().items()}
        elif hasattr(obj, 'asDict'):
            return {k: self._row_to_dict(v) for k, v in obj.asDict().items()}
        elif isinstance(obj, list):
            return [self._row_to_dict(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: self._row_to_dict(v) for k, v in obj.items()}
        else:
            return obj
    
    # ========================================================================
    #                          TYPE INFERENCE
    # ========================================================================
    
    def _inferir_tipo_columna(self, nombre_col: str):
        """
        Infer data type based on column name.
        
        Rules:
        - Id_* → StringType
        - Fecha_* → DateType
        - Valor_* → DecimalType(18,4)
        - Default → StringType
        """
        nombre_lower = nombre_col.lower()
        
        if nombre_col.startswith("Id_"):
            return StringType()
        
        if nombre_col.startswith("Fecha_"):
            return DateType()
        
        if nombre_col.startswith("Valor_"):
            return DecimalType(18, 4)
        
        if any(x in nombre_lower for x in ['descripcion', 'codigo', 'compania', 'fuente']):
            return StringType()
        
        return StringType()
    
    def cast_date_columns(self, df: DataFrame) -> DataFrame:
        """Convert columns with 'fecha' in name to DateType."""
        for col_name in df.columns:
            if 'fecha' in col_name.lower() and not col_name.startswith('id_'):
                df = df.withColumn(col_name, F.to_date(F.col(col_name), 'yyyy-MM-dd'))
        return df
    
    # ========================================================================
    #                          SCHEMA HELPER
    # ========================================================================
    def _get_clientes_schema(self):
        """Schema esperado para fact_clientes."""
        return StructType([
            StructField("Id_poliza",              StringType(), True),
            StructField("Id_tipo_identificacion", StringType(), True),
            StructField("Numero_identificacion",  StringType(), True),
            StructField("Id_rol",                 StringType(), True),
            StructField("Id_estado",              StringType(), True),
            StructField("Id_fuente",              StringType(), True),
            StructField("pk",                     StringType(), True),
        ])

    def _get_poliza_emision_schema(self):
        """Define el schema esperado para fact_poliza_emision."""
        return StructType([
            StructField("Id_periodo_calculo", StringType(), True),
            StructField("Num_poliza", StringType(), True),
            StructField("Id_tipo", StringType(), True),
            StructField("Id_producto", StringType(), True),
            StructField("Id_canal_venta", StringType(), True),
            StructField("Id_estado", StringType(), True),
            StructField("Id_linea_credito", StringType(), True),
            StructField("Id_tipo_negocio", StringType(), True),
            StructField("Id_fecha_emision", StringType(), True),
            StructField("Fecha_emision_poliza", DateType(), True),
            StructField("Id_fecha_inicio_vigencia", StringType(), True),
            StructField("Fecha_inicio_vigencia", DateType(), True),
            StructField("Id_fecha_fin_vigencia", StringType(), True),
            StructField("Fecha_fin_vigencia", DateType(), True),
            StructField("Valor_asegurado", DecimalType(18,4), True),
            StructField("Valor_prima", DecimalType(18,4), True),
            StructField("Fecha_efectiva_cancelacion", DateType(), True),
            StructField("Fecha_cancelacion", DateType(), True),
            StructField("Motivo_cancelacion", StringType(), True),
            StructField("Numero_poliza_matriz", StringType(), True),
            StructField("Id_fuente", StringType(), True)
        ])
    
    # ========================================================================
    #                          DIMENSIONS PROCESSING
    # ========================================================================
    
    def process_dimensions(self, config: Dict, yyyy: str, mm: str):
        """
        Process dimensions: copy from Silver to Gold.
        Dimensions are NOT incremental - full replacement.
        """
        self.logger.info("=" * 80)
        self.logger.info("STEP 1: PROCESSING DIMENSIONS")
        self.logger.info("=" * 80)
        
        dimensions = config.get('dimensions', [])
        active_dims = [d for d in dimensions if d.get('status') == 1]
        
        self.logger.info(f"Active dimensions: {len(active_dims)}")
        
        for dim_config in active_dims:
            try:
                dim_name = dim_config['name']
                self.logger.info(f"\n→ {dim_name}")
                
                # Build paths
                dim_key = dim_name.replace('dim_', '').replace('.xlsx', '').upper()
                silver_path = f"s3://{self.bucket_silver}/GENERAL/TRANSFORMATION/GENERAL_SILVER_DIM_{dim_key}_CLIENTES/"
                gold_path = f"s3://{self.bucket_gold}/GOLD/CLIENTES_GOLD/GOLD-CARACT_CLIENTES-DIM_DIM_{dim_key}_GOLD/"
                
                self.logger.info(f"  Silver: {silver_path}")
                
                # Read from Silver
                df_dim = self.spark.read.format("parquet").load(silver_path)
                count = df_dim.count()
                self.logger.info(f"  Records: {count:,}")
                
                if count == 0:
                    self.logger.warning(f"  ⚠️  No data in {dim_name}")
                    continue
                
                # Add load timestamp
                df_dim = df_dim.withColumn("fecha_carga_gold", current_timestamp())
                
                # Save to Gold (OVERWRITE)
                self.logger.info(f"  Gold: {gold_path}")
                dynamic_frame = DynamicFrame.fromDF(df_dim, self.glue_context, f"{dim_key}_gold")
                
                self.s3_handler.save_files_to_s3(
                    df=dynamic_frame,
                    s3_target_path=gold_path,
                    format="parquet",
                    mode="overwrite"
                )
                
                self.logger.info(f"  ✓ Saved to Gold")
                
                # Cache for fact enrichment
                df_dim_gold = self.utils.read_full_parquet(self.spark, gold_path)
                df_dim_gold.cache()
                self.dimensions_cache[dim_name] = df_dim_gold
                self.logger.info(f"  ✓ Cached in memory")
                
            except Exception as e:
                self.logger.error(f"  ✗ Error in {dim_name}: {str(e)}")
                continue
        
        self.logger.info(f"\n✓ {len(active_dims)} dimensions processed")
    
    # ========================================================================
    #                    ALFAPRD SPECIAL PROCESSING - CORREGIDO
    # ========================================================================

    def load_fact_cobertura_alfaprd(self, yyyy: str, mm: str,) -> DataFrame:
        """
        Load fact_cobertura GOLD-CARACT_CLIENTES-FACT_COBERTURA using direct Spark.
        
        - Load fact cobertura alfaprd
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info("LOADING FACT_COBERTURA | ALFAPRD")
            self.logger.info("=" * 80)

            # STEP 1: Set path of table
            PATH_POLIZAS = f's3://{self.bucket_silver}/POLIZAS_PRODUCTO/ALFAPRD/POLIZAS_ALFAPRD_SILVER/'
            
            # STEP 2: Reading polizas table
            self.logger.info("→ Reading POLIZAS table...")
            self.logger.info(f"  Path: {PATH_POLIZAS}")

            df_polizas = self.spark.read.format("delta").load(PATH_POLIZAS) \
                                .where(f"yyyy = '{yyyy}' AND mm = '{mm}'")
            total_polizas = df_polizas.count()

            if total_polizas == 0:
                self.logger.warning(f"  ⚠️  No polizas found")
                self.logger.warning(f"  → Returning empty DataFrame with schema")
                return self.spark.createDataFrame([], self._get_poliza_emision_schema())
            
            # STEP 3. Select fields
            df_poliza_coberturas = df_polizas.select(
                col("numero_poliza").alias("Id_poliza"),
                col("fecha_emision_poliza").alias("Fecha_emision_poliza"),
                col("desc_cobertura").alias("Descripcion"),
                col("cod_cobertura").alias("Id_cobertura"),
                lit(0).alias("Valor_asegurado"),
                lit(0).alias("Valor_prima"),
                col("cod_fuente_fk").alias("Id_fuente")
            )

            # STEP 4. Return output
            self.logger.info("=" * 80)
            self.logger.info("✓ POLIZA_COBERTURAS_ALFAPRD loaded successfully")
            self.logger.info(f"  Final records: {total_polizas:,}")
            self.logger.info("=" * 80)

            return df_poliza_coberturas

        except Exception as e:
            self.logger.error(f"Failed to load ALFAPRD poliza_coberturas: {str(e)}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise 
    
    
    def _load_siniestros_from_gold(self, athena_output_location):
        """
        Load CTE siniestros from Gold layer via Athena.

        CTE: cte_sin_ult_movimiento
        - Obtiene el último movimiento por poliza/cedula
        - Se usa para JOIN con polizas PREVISIONAL

        Args:
            athena_output_location: S3 path for Athena query results

        Returns:
            DataFrame con:
            - id_siniestro_fk
            - numero_identificacion_sin (cedula)
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info("LOADING CTE_SIN_ULT_MOVIMIENTO FROM GOLD")
            self.logger.info("=" * 80)

            bucket_gold = self.db_gold_athena

            # Query exacta del CTE
            query = f"""
            SELECT 
                id_siniestro_fk,
                cedula AS numero_identificacion_sin
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY id_fuente_fk, SPLIT_PART(id_poliza_pk, '-', 1), CEDULA
                        ORDER BY "Fecha de Transaccion" DESC, id_siniestro_fk DESC
                    ) AS rn
                FROM "{bucket_gold}".gold_caract_siniestros_fact_movimiento_view
            ) s
            WHERE s.rn = 1
            """

            self.logger.info("Executing Athena query for CTE siniestros...")
            self.logger.info(f"  Database: {bucket_gold}")
            self.logger.info(f"  Output: {athena_output_location}")

            df_siniestros = self.utils.execute_athena_query_to_dataframe(
                spark=self.spark,
                query=query,
                database=bucket_gold,
                output_location=athena_output_location
            )

            count = df_siniestros.count()
            self.logger.info(f"  ✓ CTE Siniestros loaded: {count:,} records")

            if count > 0:
                self.logger.info(f"  Columns: {df_siniestros.columns}")
                self.logger.info("  Sample data:")
                df_siniestros.show(5, truncate=False)

            # Track para cleanup
            self.athena_cleanup_paths.append(athena_output_location)

            return df_siniestros

        except Exception as e:
            self.logger.error(f"Failed to load CTE siniestros: {str(e)}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")

            self.logger.warning("Continuing with empty siniestros DataFrame")

            # Schema exacto del CTE
            schema = StructType([
                StructField("id_siniestro_fk", StringType(), True),
                StructField("numero_identificacion_sin", StringType(), True)
            ])
            return self.spark.createDataFrame([], schema)

    def _load_mvp1_clientes(self) -> DataFrame:
        """
        Load gold_caract_clientes_fact_cliente (MVP1) from Gold layer.

        Esta tabla se usa para el INNER JOIN en CTE cliente_unico_mvp1.

        Returns:
            DataFrame con columnas:
            - id_tipo_documento (cast a string)
            - numero_documento (cast a string)
            - otras columnas de MVP1 si se necesitan
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info("LOADING MVP1: gold_caract_clientes_fact_cliente")
            self.logger.info("=" * 80)

            # Path de MVP1 en Gold
            MVP1_PATH = f"s3://{self.bucket_gold}/GOLD/CLIENTES_GOLD/GOLD-CARACT-CLIENTES_FACT_CLIENTE/"

            self.logger.info(f"  Path: {MVP1_PATH}")

            # Cargar MVP1 usando utils
            df_mvp1 = self.utils.read_full_parquet(self.spark, MVP1_PATH)

            count = df_mvp1.count()
            self.logger.info(f"  Records loaded: {count:,}")

            if count == 0:
                self.logger.warning("  ⚠️  MVP1 is empty")
            else:
                self.logger.info(f"  Columns: {df_mvp1.columns}")

            # Cast columnas necesarias para el JOIN
            df_mvp1_prep = df_mvp1.select(
                col("id_tipo_documento").cast(StringType()).alias("id_tipo_documento"),
                col("numero_documento").cast(StringType()).alias("numero_documento")
                # Agregar otras columnas si se necesitan
            ).distinct()

            count_distinct = df_mvp1_prep.count()
            self.logger.info(f"  Unique clients: {count_distinct:,}")

            self.logger.info("✓ MVP1 loaded successfully")

            return df_mvp1_prep

        except Exception as e:
            self.logger.error(f"Failed to load MVP1: {str(e)}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")

            # Retornar DataFrame vacío con schema esperado
            self.logger.warning("Returning empty MVP1 DataFrame")
            schema = StructType([
                StructField("id_tipo_documento", StringType(), True),
                StructField("numero_documento", StringType(), True)
            ])
            return self.spark.createDataFrame([], schema)
    
    def _process_previsional_with_gold(
        self, 
        df_polizas_previsional: DataFrame,
        df_clientes_silver: DataFrame,
        df_mvp1: DataFrame,
        df_siniestros: DataFrame,
        yyyy: str,
        mm: str
    ) -> DataFrame:
        """
        Procesa polizas PREVISIONAL/PREVISIONAL_PLUS replicando SQL exacto.

        ORDEN CORRECTO:
        1. Filtrar clientes PREVISIONAL
        2. AGRUPAR por cliente (cte_aseg_silver_prv)
        3. INNER JOIN con MVP1
        4. INNER JOIN con siniestros
        5. Desagrupar para recuperar polizas
        6. INNER JOIN con polizas
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info("PROCESSING PREVISIONAL WITH BUSINESS LOGIC")
            self.logger.info("=" * 80)

            # STEP 1: CTE cte_aseg_silver_prv - AGRUPAR clientes primero
            self.logger.info("Step 1: CTE cte_aseg_silver_prv (aggregate clientes first)...")

            df_cte_aseg_silver_prv = df_clientes_silver.groupBy(
                "rol",
                "cod_tipo_identificacion",
                "numero_identificacion",
                "yyyy",
                "mm"
            ).agg(
                spark_sum("valor_prima").alias("valor_prima_periodo")
            ).withColumn(
                "periodo_ejecucion",
                concat(col("yyyy"), col("mm"))
            )

            count_agrupado = df_cte_aseg_silver_prv.count()
            self.logger.info(f"  Clientes agregados: {count_agrupado:,} records")

            # STEP 2: CTE cte_cliente_unico_mvp1 - INNER JOIN MVP1
            self.logger.info("Step 2: CTE cte_cliente_unico_mvp1 (INNER JOIN MVP1)...")

            df_cte_cliente_unico_mvp1 = df_cte_aseg_silver_prv.join(
                df_mvp1,
                (df_cte_aseg_silver_prv["cod_tipo_identificacion"] == df_mvp1["id_tipo_documento"]) &
                (df_cte_aseg_silver_prv["numero_identificacion"] == df_mvp1["numero_documento"]),
                "inner"
            ).select(
                df_cte_aseg_silver_prv["*"]
            )

            count_mvp1 = df_cte_cliente_unico_mvp1.count()
            self.logger.info(f"  Clientes in MVP1: {count_mvp1:,} records")

            # STEP 3: INNER JOIN con siniestros (ESTE ES EL FILTRO CLAVE)
            self.logger.info("Step 3: INNER JOIN cte_cliente_unico_mvp1 with siniestros...")

            df_clientes_con_siniestros = df_cte_cliente_unico_mvp1.join(
                df_siniestros,
                df_cte_cliente_unico_mvp1["numero_identificacion"] == df_siniestros["numero_identificacion_sin"],
                "inner"
            ).select(
                df_cte_cliente_unico_mvp1["*"]
            )

            count_con_siniestros = df_clientes_con_siniestros.count()
            self.logger.info(f"  Clientes with siniestros (FILTERED): {count_con_siniestros:,} records")
            self.logger.info(f"  Expected: ~51,676 (from SQL query)")

            if count_con_siniestros == 0:
                self.logger.warning("  No clientes have siniestros - returning empty DataFrame")
                return self.spark.createDataFrame([], self._get_poliza_emision_schema())

            # STEP 4: Recuperar polizas de los clientes filtrados
            self.logger.info("Step 4: Recovering polizas from filtered clientes...")

            # Necesitamos volver a los clientes SIN agrupar para obtener las polizas
            df_clientes_filtrados_ids = df_clientes_con_siniestros.select(
                "cod_tipo_identificacion",
                "numero_identificacion"
            ).distinct()

            # JOIN con clientes originales para recuperar polizas
            df_clientes_con_polizas = df_clientes_silver.join(
                df_clientes_filtrados_ids,
                ["cod_tipo_identificacion", "numero_identificacion"],
                "inner"
            )

            count_clientes_polizas = df_clientes_con_polizas.count()
            self.logger.info(f"  Clientes with polizas: {count_clientes_polizas:,} records")

            # STEP 5: Agregar por poliza (ahora sí con poliza)
            self.logger.info("Step 5: Aggregating by poliza...")

            df_clientes_silver_vlr_prima = df_clientes_con_polizas.groupBy(
                "poliza",
                "rol",
                "cod_tipo_identificacion",
                "numero_identificacion"
            ).agg(
                spark_sum("valor_prima").alias("valor_prima_periodo")
            )

            clientes_agg_count = df_clientes_silver_vlr_prima.count()
            self.logger.info(f"  Clientes agregados por poliza: {clientes_agg_count:,}")

            # STEP 6: CTE polizas_silver - Renombrar columnas
            self.logger.info("Step 6: CTE polizas_silver (renaming columns)...")

            df_polizas_silver = df_polizas_previsional.select(
                col("numero_poliza").alias("Num_poliza"),
                col("tipo_poliza").alias("Id_tipo"),
                col("cod_producto").alias("Id_producto"),
                col("canal_venta").alias("Id_canal_venta"),
                col("estado_poliza").alias("Id_estado"),
                col("linea_credito").alias("Id_linea_credito"),
                col("tipo_negocio").alias("Id_tipo_negocio"),
                col("fecha_emision_poliza").alias("Id_fecha_emision"),
                col("fecha_emision_poliza").alias("Fecha_emision_poliza"),
                col("fecha_inicio_vigencia").alias("Id_fecha_inicio_vigencia"),
                col("fecha_inicio_vigencia").alias("Fecha_inicio_vigencia"),
                col("fecha_fin_vigencia").alias("Id_fecha_fin_vigencia"),
                col("fecha_fin_vigencia").alias("Fecha_fin_vigencia"),
                col("valor_asegurado").alias("Valor_asegurado"),
                col("valor_prima").alias("Valor_prima"),
                col("fecha_efectiva_cancelacion").alias("Fecha_efectiva_cancelacion"),
                col("fecha_cancelacion").alias("Fecha_cancelacion"),
                col("motivo_cancelacion").alias("Motivo_cancelacion"),
                col("numero_poliza_matriz").alias("Numero_poliza_matriz"),
                col("cod_fuente_fk").alias("Id_fuente"),
                col("yyyy"),
                col("mm"),
                col("origen")
            )

            # STEP 7: INNER JOIN polizas con clientes filtrados
            self.logger.info("Step 7: INNER JOIN polizas_silver with filtered clientes...")

            df_poliza_emision = df_polizas_silver.join(
                df_clientes_silver_vlr_prima,
                df_polizas_silver["Num_poliza"] == df_clientes_silver_vlr_prima["poliza"],
                "inner"
            ).drop(df_clientes_silver_vlr_prima["poliza"])

            after_join_count = df_poliza_emision.count()
            self.logger.info(f"  After INNER JOIN: {after_join_count:,} records")

            # STEP 8: SELECT final + Filtro Id_estado = '1'
            self.logger.info("Step 8: SELECT final with filter Id_estado = '1'...")

            df_poliza_emision = df_poliza_emision.select(
                concat(
                    col("yyyy"),
                    col("mm")
                ).alias("Id_periodo_calculo"),

                col("Num_poliza"),
                col("Id_tipo"),
                col("Id_producto"),
                col("Id_canal_venta"),
                col("Id_estado"),
                col("Id_linea_credito"),
                col("Id_tipo_negocio"),
                col("Id_fecha_emision"),
                col("Fecha_emision_poliza"),
                col("Id_fecha_inicio_vigencia"),
                col("Fecha_inicio_vigencia"),
                col("Id_fecha_fin_vigencia"),
                col("Fecha_fin_vigencia"),
                col("Valor_asegurado"),
                col("valor_prima_periodo").alias("Valor_prima"),
                col("Fecha_efectiva_cancelacion"),
                col("Fecha_cancelacion"),
                col("Motivo_cancelacion"),
                col("Numero_poliza_matriz"),
                col("Id_fuente"),
                col("Numero_poliza_matriz").alias("id_poliza")
            ).filter(
                col("Id_estado") == "1"
            )

            after_filter_count = df_poliza_emision.count()
            self.logger.info(f"  After filter Id_estado='1': {after_filter_count:,} records")

            # STEP 9: Cast de tipos de datos
            self.logger.info("Step 9: Casting data types...")

            df_poliza_emision = df_poliza_emision \
                .withColumn("Id_periodo_calculo", col("Id_periodo_calculo").cast(StringType())) \
                .withColumn("Num_poliza", col("Num_poliza").cast(StringType())) \
                .withColumn("Id_tipo", col("Id_tipo").cast(StringType())) \
                .withColumn("Id_producto", col("Id_producto").cast(StringType())) \
                .withColumn("Id_canal_venta", col("Id_canal_venta").cast(StringType())) \
                .withColumn("Id_estado", col("Id_estado").cast(StringType())) \
                .withColumn("Id_linea_credito", col("Id_linea_credito").cast(StringType())) \
                .withColumn("Id_tipo_negocio", col("Id_tipo_negocio").cast(StringType())) \
                .withColumn("Id_fecha_emision", col("Id_fecha_emision").cast(StringType())) \
                .withColumn("Fecha_emision_poliza", col("Fecha_emision_poliza").cast(DateType())) \
                .withColumn("Id_fecha_inicio_vigencia", col("Id_fecha_inicio_vigencia").cast(StringType())) \
                .withColumn("Fecha_inicio_vigencia", col("Fecha_inicio_vigencia").cast(DateType())) \
                .withColumn("Id_fecha_fin_vigencia", col("Id_fecha_fin_vigencia").cast(StringType())) \
                .withColumn("Fecha_fin_vigencia", col("Fecha_fin_vigencia").cast(DateType())) \
                .withColumn("Valor_asegurado", col("Valor_asegurado").cast(DecimalType(18, 4))) \
                .withColumn("Valor_prima", col("Valor_prima").cast(DecimalType(18, 4))) \
                .withColumn("Fecha_efectiva_cancelacion", col("Fecha_efectiva_cancelacion").cast(DateType())) \
                .withColumn("Fecha_cancelacion", col("Fecha_cancelacion").cast(DateType())) \
                .withColumn("Motivo_cancelacion", col("Motivo_cancelacion").cast(StringType())) \
                .withColumn("Numero_poliza_matriz", col("Numero_poliza_matriz").cast(StringType())) \
                .withColumn("Id_fuente", col("Id_fuente").cast(StringType())) \
                .withColumn("id_poliza", col("id_poliza").cast(StringType()))

            count_final = df_poliza_emision.count()
            self.logger.info(f"  PREVISIONAL processed: {count_final:,} records")

            self.logger.info("=" * 80)

            return df_poliza_emision

        except Exception as e:
            self.logger.error(f"Failed to process PREVISIONAL: {str(e)}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise


    def _process_rentas_arl_standard(
        self,
        df_polizas_otros: DataFrame,
        df_clientes_silver: DataFrame,
        yyyy: str,
        mm: str
    ) -> DataFrame:
        """Procesa polizas RENTAS/ARPIS con lógica estándar."""
        try:
            self.logger.info("=" * 80)
            self.logger.info("PROCESSING RENTAS/ARPIS WITH STANDARD LOGIC")
            self.logger.info("=" * 80)

            # STEP 1: Agregar clientes por poliza (clientes_silver_vlr_prima)
            self.logger.info("Step 1: Aggregating clientes by poliza...")

            df_clientes_silver_vlr_prima = df_clientes_silver.groupBy(
                "poliza",
                "rol",
                "cod_tipo_identificacion",
                "numero_identificacion"
            ).agg(
                spark_sum("valor_prima").alias("valor_prima_periodo")
            )

            clientes_agrupados = df_clientes_silver_vlr_prima.count()
            self.logger.info(f"  Clientes agregados: {clientes_agrupados:,}")

            # STEP 2: CTE polizas_silver
            self.logger.info("Step 2: CTE polizas_silver (renaming columns)...")

            df_polizas_silver = df_polizas_otros.select(
                col("numero_poliza").alias("Num_poliza"),
                col("tipo_poliza").alias("Id_tipo"),
                col("cod_producto").alias("Id_producto"),
                col("canal_venta").alias("Id_canal_venta"),
                col("estado_poliza").alias("Id_estado"),
                col("linea_credito").alias("Id_linea_credito"),
                col("tipo_negocio").alias("Id_tipo_negocio"),
                col("fecha_emision_poliza").alias("Id_fecha_emision"),
                col("fecha_emision_poliza").alias("Fecha_emision_poliza"),
                col("fecha_inicio_vigencia").alias("Id_fecha_inicio_vigencia"),
                col("fecha_inicio_vigencia").alias("Fecha_inicio_vigencia"),
                col("fecha_fin_vigencia").alias("Id_fecha_fin_vigencia"),
                col("fecha_fin_vigencia").alias("Fecha_fin_vigencia"),
                col("valor_asegurado").alias("Valor_asegurado"),
                col("valor_prima").alias("Valor_prima"),
                col("fecha_efectiva_cancelacion").alias("Fecha_efectiva_cancelacion"),
                col("fecha_cancelacion").alias("Fecha_cancelacion"),
                col("motivo_cancelacion").alias("Motivo_cancelacion"),
                col("numero_poliza_matriz").alias("Numero_poliza_matriz"),
                col("cod_fuente_fk").alias("Id_fuente"),
                col("yyyy"),
                col("mm"),
                col("origen")
            )

            # STEP 3: LEFT JOIN polizas con clientes
            self.logger.info("Step 3: LEFT JOIN polizas_silver with clientes_silver_vlr_prima...")

            df_poliza_emision = df_polizas_silver.join(
                df_clientes_silver_vlr_prima,
                df_polizas_silver["Num_poliza"] == df_clientes_silver_vlr_prima["poliza"],
                "left"
            ).drop(df_clientes_silver_vlr_prima["poliza"])

            after_join_count = df_poliza_emision.count()
            self.logger.info(f"  After JOIN: {after_join_count:,} records")

            # STEP 4: SELECT final con CASE para Valor_prima + Filtro Id_estado = '1'
            self.logger.info("Step 4: SELECT final with CASE for Valor_prima + filter Id_estado='1'...")

            df_poliza_emision = df_poliza_emision.select(
                concat(
                    col("yyyy"), 
                    col("mm")
                ).alias("Id_periodo_calculo"),

                col("Num_poliza"),
                col("Id_tipo"),
                col("Id_producto"),
                col("Id_canal_venta"),
                col("Id_estado"),
                col("Id_linea_credito"),
                col("Id_tipo_negocio"),
                col("Id_fecha_emision"),
                col("Fecha_emision_poliza"),
                col("Id_fecha_inicio_vigencia"),
                col("Fecha_inicio_vigencia"),
                col("Id_fecha_fin_vigencia"),
                col("Fecha_fin_vigencia"),
                col("Valor_asegurado"),

                # CASE para Valor_prima (solo para RENTAS/ARL)
                when(
                    (
                        (col("valor_prima_periodo").isNull()) | 
                        (col("valor_prima_periodo") == 0)
                    ) & 
                    (lower(col("origen")) == "rentas"),
                    col("Valor_prima")
                ).otherwise(
                    col("valor_prima_periodo")
                ).alias("Valor_prima"),

                col("Fecha_efectiva_cancelacion"),
                col("Fecha_cancelacion"),
                col("Motivo_cancelacion"),
                col("Numero_poliza_matriz"),
                col("Id_fuente"),
                col("Numero_poliza_matriz").alias("id_poliza")
            ).filter(
                col("Id_estado") == "1"
            )

            after_filter_count = df_poliza_emision.count()
            self.logger.info(f"  After filter Id_estado='1': {after_filter_count:,} records")

            # STEP 5: Cast de tipos de datos
            self.logger.info("Step 5: Casting data types...")

            df_poliza_emision = df_poliza_emision \
                .withColumn("Id_periodo_calculo", col("Id_periodo_calculo").cast(StringType())) \
                .withColumn("Num_poliza", col("Num_poliza").cast(StringType())) \
                .withColumn("Id_tipo", col("Id_tipo").cast(StringType())) \
                .withColumn("Id_producto", col("Id_producto").cast(StringType())) \
                .withColumn("Id_canal_venta", col("Id_canal_venta").cast(StringType())) \
                .withColumn("Id_estado", col("Id_estado").cast(StringType())) \
                .withColumn("Id_linea_credito", col("Id_linea_credito").cast(StringType())) \
                .withColumn("Id_tipo_negocio", col("Id_tipo_negocio").cast(StringType())) \
                .withColumn("Id_fecha_emision", col("Id_fecha_emision").cast(StringType())) \
                .withColumn("Fecha_emision_poliza", col("Fecha_emision_poliza").cast(DateType())) \
                .withColumn("Id_fecha_inicio_vigencia", col("Id_fecha_inicio_vigencia").cast(StringType())) \
                .withColumn("Fecha_inicio_vigencia", col("Fecha_inicio_vigencia").cast(DateType())) \
                .withColumn("Id_fecha_fin_vigencia", col("Id_fecha_fin_vigencia").cast(StringType())) \
                .withColumn("Fecha_fin_vigencia", col("Fecha_fin_vigencia").cast(DateType())) \
                .withColumn("Valor_asegurado", col("Valor_asegurado").cast(DecimalType(18, 4))) \
                .withColumn("Valor_prima", col("Valor_prima").cast(DecimalType(18, 4))) \
                .withColumn("Fecha_efectiva_cancelacion", col("Fecha_efectiva_cancelacion").cast(DateType())) \
                .withColumn("Fecha_cancelacion", col("Fecha_cancelacion").cast(DateType())) \
                .withColumn("Motivo_cancelacion", col("Motivo_cancelacion").cast(StringType())) \
                .withColumn("Numero_poliza_matriz", col("Numero_poliza_matriz").cast(StringType())) \
                .withColumn("Id_fuente", col("Id_fuente").cast(StringType())) \
                .withColumn("id_poliza", col("id_poliza").cast(StringType()))

            total_final = df_poliza_emision.count()
            self.logger.info(f"  RENTAS/ARPIS processed: {total_final:,} records")

            self.logger.info("=" * 80)

            return df_poliza_emision

        except Exception as e:
            self.logger.error(f"Failed to process RENTAS/ARPIS: {str(e)}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    def load_fact_poliza_emision_alfaprd(self, yyyy: str, mm: str) -> DataFrame:
        """
        Load FACT_POLIZA_EMISION from ALFAPRD.

        🆕 v3.2: Lógica separada por origen:
        - previsional/previsional_plus: Usa MVP1 + CTE siniestros
        - rentas/arpis: Lógica estándar
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info("LOADING FACT_POLIZA_EMISION | ALFAPRD v3.2")
            self.logger.info("=" * 80)

            # Setup Athena output location
            bucket_assets = self.bucket_silver
            athena_output = f"s3://{bucket_assets}/temp/athena-siniestros-{yyyy}{mm}/"

            self.logger.info(f"Period: yyyy={yyyy}, mm={mm}")

            # Paths
            PATH_POLIZAS = f's3://{self.bucket_silver}/POLIZAS_PRODUCTO/ALFAPRD/POLIZAS_ALFAPRD_SILVER/'
            PATH_CLIENTES = f's3://{self.bucket_silver}/POLIZAS_PRODUCTO/ALFAPRD/CLIENTES_ALFAPRD_SILVER/'

            # ========================================================================
            # STEP 1: Cargar datos base
            # ========================================================================
            self.logger.info("Step 1: Loading base data (Clientes, Polizas)...")

            df_clientes = self.spark.read.format("delta").load(PATH_CLIENTES) \
                .where(f"yyyy = '{yyyy}' AND mm = '{mm}' AND rol = '2' AND cod_estado_cliente = '1'")

            df_polizas = self.spark.read.format("delta").load(PATH_POLIZAS) \
                .where(f"yyyy = '{yyyy}' AND mm = '{mm}'")

            total_clientes = df_clientes.count()
            total_polizas = df_polizas.count()

            self.logger.info(f"  Clientes: {total_clientes:,}")
            self.logger.info(f"  Polizas: {total_polizas:,}")

            if total_clientes == 0 or total_polizas == 0:
                self.logger.warning("No data - returning empty DataFrame")
                return self.spark.createDataFrame([], self._get_poliza_emision_schema())

            # ========================================================================
            # STEP 2: Separar polizas por origen
            # ========================================================================
            self.logger.info("Step 2: Separating by origen...")

            df_polizas_previsional = df_polizas.filter(
                lower(col("origen")).isin(["previsional", "previsional_plus"])
            )

            df_polizas_otros = df_polizas.filter(
                lower(col("origen")).isin(["rentas", "arpis"])
            )

            count_prev = df_polizas_previsional.count()
            count_otros = df_polizas_otros.count()

            self.logger.info(f"  PREVISIONAL/PREVISIONAL_PLUS: {count_prev:,}")
            self.logger.info(f"  RENTAS/ARPIS: {count_otros:,}")

            results = []

            # ========================================================================
            # STEP 3: Procesar PREVISIONAL (con MVP1 + Siniestros)
            # ========================================================================
            if count_prev > 0:
                self.logger.info("Step 3: Processing PREVISIONAL...")

                # Cargar MVP1
                df_mvp1 = self._load_mvp1_clientes()

                # Cargar CTE siniestros
                df_siniestros = self._load_siniestros_from_gold(athena_output)

                # Procesar con lógica completa
                df_prev_result = self._process_previsional_with_gold(
                    df_polizas_previsional,
                    df_clientes,
                    df_mvp1,
                    df_siniestros,
                    yyyy, mm
                )

                results.append(df_prev_result)
            else:
                self.logger.info("Step 3: No PREVISIONAL polizas to process")

            # ========================================================================
            # STEP 4: Procesar RENTAS/ARPIS (lógica estándar)
            # ========================================================================
            if count_otros > 0:
                self.logger.info("Step 4: Processing RENTAS/ARPIS (standard)...")
                df_otros_result = self._process_rentas_arl_standard(
                    df_polizas_otros,
                    df_clientes,
                    yyyy, mm
                )
                results.append(df_otros_result)
            else:
                self.logger.info("Step 4: No RENTAS/ARPIS polizas to process")

            # ========================================================================
            # STEP 5: Unir resultados
            # ========================================================================
            if not results:
                self.logger.warning("No results - returning empty DataFrame")
                return self.spark.createDataFrame([], self._get_poliza_emision_schema())

            self.logger.info("Step 5: Unioning results...")
            df_final = results[0]
            for df in results[1:]:
                df_final = df_final.unionByName(df, allowMissingColumns=True)

            total = df_final.count()

            self.logger.info("=" * 80)
            self.logger.info("✓ POLIZA_EMISION_ALFAPRD loaded successfully")
            self.logger.info(f"  Total records: {total:,}")
            self.logger.info("=" * 80)

            return df_final

        except Exception as e:
            self.logger.error(f"Failed: {str(e)}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise
    
    def _cleanup_athena_results(self):
        """
        Cleanup Athena query results from S3.

        Deletes temporary files created by Athena queries to Gold.
        """
        if not self.athena_cleanup_paths:
            self.logger.info("No Athena paths to cleanup")
            return

        self.logger.info("=" * 80)
        self.logger.info("CLEANUP: Deleting Athena query results")
        self.logger.info("=" * 80)

        cleaned_count = 0
        failed_count = 0

        for path in self.athena_cleanup_paths:
            try:
                self.logger.info(f"  Cleaning: {path}")

                # Parsear S3 path (formato: s3://bucket/prefix/path/)
                if not path.startswith("s3://"):
                    self.logger.warning(f"  ⚠️  Invalid S3 path: {path}")
                    failed_count += 1
                    continue

                path_parts = path.replace("s3://", "").split("/", 1)
                bucket = path_parts[0]
                prefix = path_parts[1] if len(path_parts) > 1 else ""

                # Usar boto3 para eliminar objetos
                import boto3
                s3 = boto3.client('s3')

                # Listar objetos con el prefijo
                paginator = s3.get_paginator('list_objects_v2')
                objects_deleted = 0

                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    if 'Contents' in page:
                        objects = [{'Key': obj['Key']} for obj in page['Contents']]
                        if objects:
                            response = s3.delete_objects(
                                Bucket=bucket,
                                Delete={'Objects': objects}
                            )
                            objects_deleted += len(response.get('Deleted', []))

                self.logger.info(f"    ✓ Deleted {objects_deleted} objects from {path}")
                cleaned_count += 1

            except Exception as e:
                self.logger.warning(f"    ⚠️  Failed to cleanup {path}: {str(e)}")
                failed_count += 1
                continue

        self.logger.info(f"✓ Athena cleanup completed")
        self.logger.info(f"  Cleaned: {cleaned_count} paths")
        if failed_count > 0:
            self.logger.warning(f"  Failed: {failed_count} paths")

        self.athena_cleanup_paths.clear()
    
    def cleanup(self):
        """
        Clean cached resources and Athena temporary files.

        ✅ v3.2: Incluye cleanup de Athena results
        """
        self.logger.info("\n" + "=" * 80)
        self.logger.info("CLEANING RESOURCES")
        self.logger.info("=" * 80)

        # Cleanup dimension cache (código existente)
        count_cache = len(self.dimensions_cache)

        for name, df in self.dimensions_cache.items():
            try:
                df.unpersist()
            except:
                pass

        self.dimensions_cache.clear()
        self.spark.catalog.clearCache()

        self.logger.info(f"✓ {count_cache} cached objects released from memory")

        # 🆕 Cleanup Athena results
        self._cleanup_athena_results()

        self.logger.info("=" * 80)
    # ========================================================================
    #                    SOURCE CONSOLIDATION - CORREGIDO
    # ========================================================================
    def consolidate_sources(
        self,
        sources_config: Dict,
        yyyy: str,
        mm: str,
        unified_columns: List[str],
        fact_name: str
    ) -> DataFrame:
        """
        Consolidate data from multiple sources (Cardiff, ACSELE, ALFAPRD).
        Reads yyyy/mm partitions from each source and unions them.
        Processing order: Dimensions first, then Facts.
        Special handling for FACT_POLIZA_EMISION and FACT_COBERTURA from ALFAPRD.
        """
        dfs_consolidados = []
    
        self.logger.info(f"Sources to process: {list(sources_config.keys())}")
        self.logger.info(f"Period: {yyyy}-{mm}")
    
        sources = list(sources_config.keys())
        dimension_sources = [s for s in sources if 'DIM_' in fact_name.upper() or 'DIMENSION_' in fact_name.upper()]
        fact_sources = [s for s in sources if s not in dimension_sources]
    
        is_fact_poliza_emision = "FACT_POLIZA_EMISION" in fact_name.upper()
        is_fact_cobertura = "FACT_COBERTURA" in fact_name.upper()
        is_fact_clientes = "FACT_CLIENTES" in fact_name.upper()
    
        if (is_fact_poliza_emision or is_fact_cobertura or is_fact_clientes) and "alfaprd" in fact_sources:
            fact_sources.remove("alfaprd")
            fact_sources.append("alfaprd")
    
        processing_order = dimension_sources + fact_sources
    
        for source_name in processing_order:
            source_config = sources_config[source_name]
    
            try:
                self.logger.info(f"Source: {source_name}")
    
                tabla_silver = source_config['tabla_silver']
                self.logger.info(f"Silver table: {tabla_silver}")
    
                if self.config and 'sources' in self.config:
                    source_info = self.config.get('sources', {}).get(source_name, {})
                    base_path = source_info.get('base_path', f"{source_name.upper()}/")
                    self.logger.info(f"Base path from config: {base_path}")
                else:
                    base_path = f"{source_name.upper()}/"
                    self.logger.info(f"Base path (default): {base_path}")
    
                # ========================================================================
                # ✅ CORRECCIÓN: Skip lectura normal para ALFAPRD con casos especiales
                # ========================================================================
                if source_name == "alfaprd" and is_fact_poliza_emision:
                    self.logger.info("⚠️  ALFAPRD FACT_POLIZA_EMISION: Skipping normal read")
                    self.logger.info("    → Will use load_fact_poliza_emision_alfaprd() instead")
                    df_source = None
                    df_mapped = None
    
                elif source_name == "alfaprd" and is_fact_cobertura:
                    self.logger.info("⚠️  ALFAPRD FACT_COBERTURA: Skipping normal read")
                    self.logger.info("    → Will use load_fact_cobertura_alfaprd() instead")
                    df_source = None
                    df_mapped = None
                
                elif source_name == "alfaprd" and is_fact_clientes:
                    self.logger.info("⚠️  ALFAPRD FACT_CLIENTES: Skipping normal read")
                    self.logger.info("    → Will use load_fact_clientes_alfaprd() instead")
                    df_source = None
                    df_mapped = None
    
                # ========================================================================
                # Lectura normal para TODOS los demás casos
                # ========================================================================
                else:
                    formato = "parquet" if source_name == "acsele" else "delta"
    
                    if formato == "delta" and tabla_silver == "PRODUCTOS_ALFAPRD_SILVER":
                        # Caso especial: PRODUCTOS_ALFAPRD_SILVER (full load)
                        path_full_load = f"s3://{self.bucket_silver}/{base_path}{tabla_silver}"
                        self.logger.info(f"Format: Parquet")
                        self.logger.info(f"Strategy: Full Load")
                        df_source = self.spark.read.format("parquet").load(path_full_load)
    
                    elif formato == "delta":
                        # CARDIFF y otros ALFAPRD (excepto los casos especiales)
                        path_base = f"s3://{self.bucket_silver}/{base_path}{tabla_silver}"
                        self.logger.info(f"Format: Delta")
                        self.logger.info(f"Base path (no partitions): {path_base}")
                        self.logger.info(f"Filter: yyyy={yyyy}, mm={mm}")
    
                        df_source = self.spark.read.format("delta").load(path_base) \
                            .where(f"yyyy = '{yyyy}' AND mm = '{mm}'")
    
                    else:
                        # ACSELE (parquet con particiones físicas)
                        ruta_base = f"s3://{self.bucket_silver}/{base_path}{tabla_silver}/yyyy={yyyy}/mm={mm}"
                        ruta_particion = f"{ruta_base}/"
                        self.logger.info(f"Format: Parquet")
                        self.logger.info(f"Partition path: {ruta_particion}")
    
                        df_source = self.spark.read.format("parquet").load(ruta_particion)
    
                    # Procesar el DataFrame leído
                    if df_source is not None:
                        self.logger.info("DataFrame loaded, counting records...")
                        count_source = df_source.count()
    
                        if count_source == 0:
                            self.logger.warning(f"No data for period {yyyy}-{mm}")
                            continue
    
                        self.logger.info(f"Records: {count_source:,}")
    
                        column_mapping = source_config.get('column_mapping', {})
                        self.logger.info(f"Applying column mapping ({len(column_mapping)} columns)...")
                        df_mapped = self._apply_column_mapping(df_source, column_mapping)
    
                        target_cols = list(column_mapping.keys())
                        df_mapped = df_mapped.select(*[c for c in target_cols if c in df_mapped.columns])
                    else:
                        df_mapped = None
    
                # ========================================================================
                # Aplicar transformaciones según la fuente
                # ========================================================================
                if source_name == 'acsele':
                    self.logger.info("Applying ACSELE transformations...")
                    df_mapped = self._apply_acsele_transformation_router(
                        df_mapped, 
                        fact_name
                    )
    
                elif source_name == 'alfaprd':
                    self.logger.info("Applying ALFAPRD transformations...")

                    if is_fact_cobertura:
                        df_mapped = self._apply_alfaprd_transformation_router(
                            None, fact_name, yyyy, mm, fact_coberturas=True
                        )
                    elif is_fact_poliza_emision:
                        df_mapped = self._apply_alfaprd_transformation_router(
                            None, fact_name, yyyy, mm, fact_coberturas=False
                        )
                    elif is_fact_clientes:                          # 🆕
                        df_mapped = self._apply_alfaprd_transformation_router(
                            None, fact_name, yyyy, mm, fact_clientes=True
                        )
                    else:
                        df_mapped = self._apply_alfaprd_transformation_router(
                            df_mapped, fact_name, yyyy, mm, fact_coberturas=False
                        )
    
                # ========================================================================
                # Finalizar procesamiento de la fuente
                # ========================================================================
                if df_mapped is None:
                    self.logger.warning(f"No data returned from transformations for {source_name}")
                    continue
    
                if "Id_fuente" not in df_mapped.columns:
                    source_id = self.config.get('sources', {}).get(source_name, {}).get('source_id', source_name.upper())
                    df_mapped = df_mapped.withColumn("Id_fuente", lit(source_id).cast(StringType()))
                    self.logger.info(f"Added Id_fuente: {source_id}")
    
                for col_unif in unified_columns:
                    if col_unif not in df_mapped.columns and col_unif != 'Id':
                        tipo_dato = self._inferir_tipo_columna(col_unif)
                        df_mapped = df_mapped.withColumn(col_unif, lit(None).cast(tipo_dato))
                        self.logger.debug(f"Column {col_unif} added as {tipo_dato}")
    
                cols_select = [c for c in unified_columns if c != 'Id' and c in df_mapped.columns]
                df_mapped = df_mapped.select(*cols_select)
    
                dfs_consolidados.append(df_mapped)
                self.logger.info(f"{source_name} processed successfully")
    
            except Exception as e:
                self.logger.error(f"Error in {source_name}: {str(e)}")
                self.logger.error(f"Error type: {type(e).__name__}")
                import traceback
                self.logger.error(f"Traceback:\n{traceback.format_exc()}")
                continue
    
        if not dfs_consolidados:
            self.logger.error("CONSOLIDATION FAILED")
            self.logger.error(f"No sources could be read for fact: {fact_name}")
            self.logger.error(f"Period: {yyyy}-{mm}")
            self.logger.error(f"Sources attempted: {list(sources_config.keys())}")
            raise ValueError(f"No sources could be read for {fact_name} in period {yyyy}-{mm}")
    
        self.logger.info(f"Consolidating {len(dfs_consolidados)} sources...")
    
        df_consolidado = dfs_consolidados[0]
        for df in dfs_consolidados[1:]:
            df_consolidado = df_consolidado.unionByName(df, allowMissingColumns=True)
    
        count_total = df_consolidado.count()
        self.logger.info(f"Total consolidated: {count_total:,} records")
    
        return df_consolidado
    
    def load_fact_clientes_alfaprd(self, yyyy: str, mm: str) -> DataFrame:
        """
        Load FACT_CLIENTES from ALFAPRD.

        Lógica separada por origen:
        - previsional/previsional_plus: Aplica filtro MVP1 + siniestros
        - rentas/arpis: Lógica estándar sin filtro
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info("LOADING FACT_CLIENTES | ALFAPRD")
            self.logger.info("=" * 80)

            PATH_CLIENTES = f's3://{self.bucket_silver}/POLIZAS_PRODUCTO/ALFAPRD/CLIENTES_ALFAPRD_SILVER/'
            PATH_POLIZAS  = f's3://{self.bucket_silver}/POLIZAS_PRODUCTO/ALFAPRD/POLIZAS_ALFAPRD_SILVER/'
            athena_output = f"s3://{self.bucket_silver}/temp/athena-siniestros-clientes-{yyyy}{mm}/"

            self.logger.info(f"Period: yyyy={yyyy}, mm={mm}")

            # ── STEP 1: Cargar datos base ──────────────────────────────────────
            self.logger.info("Step 1: Loading base data...")

            df_clientes = self.spark.read.format("delta").load(PATH_CLIENTES) \
                .where(f"yyyy = '{yyyy}' AND mm = '{mm}'")

            df_polizas = self.spark.read.format("delta").load(PATH_POLIZAS) \
                .where(f"yyyy = '{yyyy}' AND mm = '{mm}'")\
                .select("numero_poliza", "origen").distinct()

            total_clientes = df_clientes.count()
            self.logger.info(f"  Clientes loaded: {total_clientes:,}")

            if total_clientes == 0:
                self.logger.warning("  No clientes found — returning empty DataFrame")
                return self.spark.createDataFrame([], self._get_clientes_schema())

            # STEP 2: JOIN clientes con origen de poliza
            self.logger.info("Step 2: Joining clientes with poliza origen...")

            df_polizas = df_polizas.select(
                col("numero_poliza"),
                col("origen").alias("origen_poliza")
            ).distinct()

            # Eliminar origen de clientes si ya existe para evitar ambigüedad
            if "origen" in df_clientes.columns:
                df_clientes = df_clientes.drop("origen")

            df_clientes = df_clientes.join(
                df_polizas,
                df_clientes["poliza"] == df_polizas["numero_poliza"],
                "left"
            ).drop("numero_poliza") \
            .withColumnRenamed("origen_poliza", "origen")

            # ── STEP 3: Separar por origen ────────────────────────────────────
            self.logger.info("Step 3: Splitting by origen...")

            origenes_previsional = ["previsional", "previsional_plus"]

            df_clientes_previsional = df_clientes.filter(
                F.lower(F.col("origen")).isin(origenes_previsional)
            )
            df_clientes_otros = df_clientes.filter(
                ~F.lower(F.col("origen")).isin(origenes_previsional) |
                F.col("origen").isNull()
            )

            count_prev  = df_clientes_previsional.count()
            count_otros = df_clientes_otros.count()
            self.logger.info(f"  PREVISIONAL clientes: {count_prev:,}")
            self.logger.info(f"  RENTAS/ARPIS clientes: {count_otros:,}")

            results = []

            # ── STEP 4: Procesar PREVISIONALES ────────────────────────────────
            if count_prev > 0:
                self.logger.info("Step 4: Processing PREVISIONAL clientes...")

                df_mvp1       = self._load_mvp1_clientes()
                df_siniestros = self._load_siniestros_from_gold(athena_output)

                df_prev_result = self._process_clientes_previsional_with_gold(
                    df_clientes_previsional,
                    df_mvp1,
                    df_siniestros,
                    yyyy, mm
                )
                results.append(df_prev_result)
            else:
                self.logger.info("Step 4: No PREVISIONAL clientes to process")

            # ── STEP 5: Procesar RENTAS/ARPIS ─────────────────────────────────
            if count_otros > 0:
                self.logger.info("Step 5: Processing RENTAS/ARPIS clientes (standard)...")

                df_otros_result = self._process_clientes_standard(
                    df_clientes_otros,
                    yyyy, mm
                )
                results.append(df_otros_result)
            else:
                self.logger.info("Step 5: No RENTAS/ARPIS clientes to process")

            # ── STEP 6: Unir resultados ────────────────────────────────────────
            if not results:
                self.logger.warning("No results — returning empty DataFrame")
                return self.spark.createDataFrame([], self._get_clientes_schema())

            self.logger.info("Step 6: Unioning results...")
            df_final = results[0]
            for df in results[1:]:
                df_final = df_final.unionByName(df, allowMissingColumns=True)

            total = df_final.count()
            self.logger.info(f"  Total FACT_CLIENTES ALFAPRD: {total:,}")
            self.logger.info("=" * 80)

            return df_final

        except Exception as e:
            self.logger.error(f"Failed to load FACT_CLIENTES ALFAPRD: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise
    
    def _process_clientes_previsional_with_gold(
        self,
        df_clientes_previsional: DataFrame,
        df_mvp1: DataFrame,
        df_siniestros: DataFrame,
        yyyy: str,
        mm: str
    ) -> DataFrame:
        """
        Procesa clientes PREVISIONAL/PREVISIONAL_PLUS.

        Solo pasan los clientes que cruzan con MVP1 y tienen siniestro.

        ORDEN:
        1. Agrupar clientes (cte_aseg_silver_prv)
        2. INNER JOIN con MVP1
        3. INNER JOIN con siniestros
        4. Recuperar clientes originales filtrados
        5. Mapear al esquema unificado
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info("PROCESSING CLIENTES PREVISIONAL WITH BUSINESS LOGIC")
            self.logger.info("=" * 80)

            # STEP 1: Agrupar clientes
            self.logger.info("Step 1: Aggregating clientes (cte_aseg_silver_prv)...")

            df_agrupado = df_clientes_previsional.groupBy(
                "cod_tipo_identificacion",
                "numero_identificacion",
                "yyyy",
                "mm"
            ).agg(
                spark_sum("valor_prima").alias("valor_prima_periodo")
            )

            count_agrupado = df_agrupado.count()
            self.logger.info(f"  Clientes agregados: {count_agrupado:,}")

            # STEP 2: INNER JOIN con MVP1
            self.logger.info("Step 2: INNER JOIN with MVP1...")

            df_con_mvp1 = df_agrupado.join(
                df_mvp1,
                (df_agrupado["cod_tipo_identificacion"] == df_mvp1["id_tipo_documento"]) &
                (df_agrupado["numero_identificacion"]   == df_mvp1["numero_documento"]),
                "inner"
            ).select(df_agrupado["*"])

            count_mvp1 = df_con_mvp1.count()
            self.logger.info(f"  Clientes in MVP1: {count_mvp1:,}")

            # STEP 3: INNER JOIN con siniestros
            self.logger.info("Step 3: INNER JOIN with siniestros...")

            df_con_siniestros = df_con_mvp1.join(
                df_siniestros,
                df_con_mvp1["numero_identificacion"] == df_siniestros["numero_identificacion_sin"],
                "inner"
            ).select(
                df_con_mvp1["cod_tipo_identificacion"],
                df_con_mvp1["numero_identificacion"]
            ).distinct()

            count_siniestros = df_con_siniestros.count()
            self.logger.info(f"  Clientes with siniestros (FILTERED): {count_siniestros:,}")

            if count_siniestros == 0:
                self.logger.warning("  No clientes passed siniestros filter — returning empty")
                return self.spark.createDataFrame([], self._get_clientes_schema())

            # STEP 4: Recuperar registros originales de los clientes filtrados
            self.logger.info("Step 4: Recovering original clientes records...")

            df_result = df_clientes_previsional.join(
                df_con_siniestros,
                ["cod_tipo_identificacion", "numero_identificacion"],
                "inner"
            )

            count_result = df_result.count()
            self.logger.info(f"  Clientes previsional final: {count_result:,}")

            # STEP 5: Mapear al esquema unificado
            self.logger.info("Step 5: Mapping to unified schema...")

            df_mapped = df_result.select(
                col("poliza").cast(StringType()).alias("Id_poliza"),
                col("cod_tipo_identificacion").cast(StringType()).alias("Id_tipo_identificacion"),
                col("numero_identificacion").cast(StringType()).alias("Numero_identificacion"),
                col("rol").cast(StringType()).alias("Id_rol"),
                col("cod_estado_cliente").cast(StringType()).alias("Id_estado"),
                col("cod_fuente_fk").cast(StringType()).alias("Id_fuente"),
                concat_ws("-",
                    col("cod_tipo_identificacion").cast(StringType()),
                    col("numero_identificacion").cast(StringType())
                ).alias("pk")
            )

            self.logger.info("=" * 80)
            return df_mapped

        except Exception as e:
            self.logger.error(f"Failed to process CLIENTES PREVISIONAL: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise

    def _process_clientes_standard(
        self,
        df_clientes: DataFrame,
        yyyy: str,
        mm: str
    ) -> DataFrame:
        """
        Procesa clientes RENTAS/ARPIS con lógica estándar.
        Sin filtro de siniestros, mapeo directo al esquema unificado.
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info("PROCESSING CLIENTES RENTAS/ARPIS STANDARD")
            self.logger.info("=" * 80)

            df_mapped = df_clientes.select(
                col("poliza").cast(StringType()).alias("Id_poliza"),
                col("cod_tipo_identificacion").cast(StringType()).alias("Id_tipo_identificacion"),
                col("numero_identificacion").cast(StringType()).alias("Numero_identificacion"),
                col("rol").cast(StringType()).alias("Id_rol"),
                col("cod_estado_cliente").cast(StringType()).alias("Id_estado"),
                col("cod_fuente_fk").cast(StringType()).alias("Id_fuente"),
                concat_ws("-",
                    col("cod_tipo_identificacion").cast(StringType()),
                    col("numero_identificacion").cast(StringType())
                ).alias("pk")
            )

            total = df_mapped.count()
            self.logger.info(f"  RENTAS/ARPIS clientes processed: {total:,}")
            self.logger.info("=" * 80)

            return df_mapped

        except Exception as e:
            self.logger.error(f"Failed to process CLIENTES standard: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise
        # ========================================================================
        #                    TRANSFORMATION ROUTERS
        # ========================================================================
        
    def _apply_acsele_transformation_router(
            self, 
            df_mapped: DataFrame, 
            fact_name: str
        ) -> DataFrame:
            """Route ACSELE transformations based on fact type."""
            
            if fact_name == 'GOLD-CARACT_CLIENTES-FACT_POLIZA_EMISION':
                self.logger.info("    Applying ACSELE transformation (poliza_emision)...")
                df_mapped = df_mapped.distinct()
                
                return self._apply_acsele_transformation(
                    df_mapped,
                    col_id_poliza='Num_poliza',
                    col_fecha='Fecha_emision_poliza',
                    col_valor_asegurado='Valor_asegurado',
                    col_valor_prima='Valor_prima',
                    col_agrupacion_adicional = "Id_periodo_calculo"

                )
            
            elif fact_name == 'GOLD-CARACT_CLIENTES-FACT_COBERTURA':
                self.logger.info("    Applying ACSELE transformation (cobertura)...")
                
                return self._apply_acsele_transformation(
                    df_mapped,
                    col_id_poliza='Id_poliza',
                    col_fecha='Fecha_emision_poliza',
                    col_valor_asegurado='Valor_asegurado',
                    col_valor_prima='Valor_prima',
                    col_agrupacion_adicional="Descripcion"
                )
            
            return df_mapped
    
    def _apply_alfaprd_transformation_router(
        self,
        df_mapped: DataFrame,
        fact_name: str,
        yyyy: str,
        mm: str,
        fact_coberturas: bool = False,
        fact_clientes: bool = False
    ) -> DataFrame:
        """
        Route ALFAPRD transformations based on fact type.

        v3.1: Sin Athena - Todo en Spark
        - Eliminado parámetro athena_output_location
        - Procesamiento directo en Spark
        """

        if fact_name == 'GOLD-CARACT_CLIENTES-FACT_POLIZA_EMISION' and fact_coberturas == False:
            try:
                self.logger.info("=" * 80)
                self.logger.info("ALFAPRD SPECIAL PROCESSING | FACT_POLIZA_EMISION")
                self.logger.info("=" * 80)

                # Cargar directamente desde Delta (sin Athena)
                # Ya no necesita athena_output_location
                df_fact_poliza_emision = self.load_fact_poliza_emision_alfaprd(yyyy, mm)

                # Materializar datos
                self.logger.info("→ Materializing data...")
                df_fact_poliza_emision.persist()

                fact_count = df_fact_poliza_emision.count()
                self.logger.info(f"  Materialized: {fact_count:,} records")

                # ✅ Ya no agregamos a athena_cleanup_queue porque no usamos Athena

                return df_fact_poliza_emision

            except Exception as e:
                self.logger.error(f"Failed ALFAPRD poliza_emision processing: {str(e)}")
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")
                raise

        elif fact_name == 'GOLD-CARACT_CLIENTES-FACT_COBERTURA' and fact_coberturas == True:
            try:
                self.logger.info("=" * 80)
                self.logger.info("ALFAPRD SPECIAL PROCESSING | FACT_COBERTURA")
                self.logger.info("=" * 80)

                # Ya no necesita athena_output_location
                df_fact_poliza_cobertura = self.load_fact_cobertura_alfaprd(yyyy, mm)

                # Materializar datos
                self.logger.info("→ Materializing data...")
                df_fact_poliza_cobertura.persist()

                fact_count = df_fact_poliza_cobertura.count()
                self.logger.info(f"  Materialized: {fact_count:,} records")

                return df_fact_poliza_cobertura

            except Exception as e:
                self.logger.error(f"Failed ALFAPRD fact_cobertura processing: {str(e)}")
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")
                raise
        
        elif fact_name == 'GOLD-CARACT_CLIENTES-FACT_CLIENTES_POLIZAS' and fact_clientes == True:
            try:
                self.logger.info("=" * 80)
                self.logger.info("ALFAPRD SPECIAL PROCESSING | FACT_CLIENTES")
                self.logger.info("=" * 80)

                # Ya no necesita athena_output_location
                df_fact_clientes_poliza = self.load_fact_clientes_alfaprd(yyyy, mm)

                # Materializar datos
                self.logger.info("→ Materializing data...")
                df_fact_clientes_poliza.persist()

                fact_count = df_fact_clientes_poliza.count()
                self.logger.info(f"  Materialized: {fact_count:,} records")

                return df_fact_clientes_poliza

            except Exception as e:
                self.logger.error(f"Failed ALFAPRD clientes_poliza processing: {str(e)}")
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")
                raise

        return df_mapped
    
    # ========================================================================
    #                    COLUMN MAPPING
    # ========================================================================
    
    def _apply_column_mapping(self, df: DataFrame, mapping: Dict) -> DataFrame:
        """
        Apply column mapping with support for:
        - Literal values (numbers, strings, null)
        - concat() expressions
        - concat_ws() expressions
        - Direct column mapping
        """
        if not mapping:
            return df
        
        for target_col, source_col in mapping.items():
            # 1) Numeric literal
            try:
                valor_numerico = float(source_col)
                tipo_dato = self._inferir_tipo_columna(target_col)
                df = df.withColumn(target_col, lit(valor_numerico).cast(tipo_dato))
                self.logger.debug(f"  Column {target_col} = {valor_numerico} (literal)")
                continue
            except ValueError:
                pass
            
            # 2) NULL literal
            if isinstance(source_col, str) and source_col.lower() == "null":
                tipo_dato = self._inferir_tipo_columna(target_col)
                df = df.withColumn(target_col, lit(None).cast(tipo_dato))
                self.logger.debug(f"  Column {target_col} = NULL (literal)")
                continue
            
            # 3) concat_ws() expression
            if isinstance(source_col, str) and source_col.strip().lower().startswith("concat_ws("):
                df = self._apply_concat_ws(df, target_col, source_col)
                continue
            
            # 4) concat() expression
            if isinstance(source_col, str) and source_col.strip().lower().startswith("concat("):
                df = self._apply_concat(df, target_col, source_col)
                continue
            
            # 5) Direct column mapping
            if source_col in df.columns:
                df = df.withColumn(target_col, col(source_col))
            else:
                self.logger.warning(
                    f"  ⚠️  Source column '{source_col}' not found for '{target_col}'"
                )
        
        return df
    
    def _apply_concat_ws(self, df: DataFrame, target_col: str, expr_str: str) -> DataFrame:
        """Apply concat_ws() expression: concat_ws('-', col1, col2)"""
        inner = expr_str[len("concat_ws("):-1]
        parts = [p.strip() for p in inner.split(",")]
        sep = parts[0].strip().strip("'").strip('"')
        
        cols = []
        for p in parts[1:]:
            if p.startswith("'") or p.startswith('"'):
                cols.append(lit(p.strip().strip("'").strip('"')))
            else:
                if p in df.columns:
                    cols.append(col(p))
                else:
                    self.logger.warning(
                        f"  ⚠️  Column '{p}' not found for concat_ws in '{target_col}'"
                    )
                    cols.append(lit(None))
        
        return df.withColumn(target_col, concat_ws(sep, *cols))
    
    def _apply_concat(self, df: DataFrame, target_col: str, expr_str: str) -> DataFrame:
        """Apply concat() expression: concat(arg1, arg2)"""
        inner = expr_str[len("concat("):-1]
        parts = []
        
        for p in inner.split(","):
            p = p.strip()
            if p.startswith("'") or p.startswith('"'):
                parts.append(lit(p.strip().strip("'").strip('"')))
            else:
                if p in df.columns:
                    parts.append(col(p))
                else:
                    self.logger.warning(
                        f"  ⚠️  Column '{p}' not found for concat in '{target_col}'"
                    )
                    parts.append(lit(None))
        
        return df.withColumn(target_col, concat(*parts))
    
    # ========================================================================
    #                    ACSELE DEDUPLICATION
    # ========================================================================
    
    def _apply_acsele_transformation(
        self, 
        df: DataFrame,
        col_id_poliza: str = 'Id_poliza',
        col_fecha: str = 'Fecha_emision_poliza',
        col_valor_asegurado: str = 'Valor_asegurado',
        col_valor_prima: str = 'Valor_prima',
        col_agrupacion_adicional: Optional[str] = None,
        col_no_operacion: str = 'Nro_operacion'
    ) -> DataFrame:
        """
        Apply ACSELE deduplication logic.
        
        Logic:
        1. Sum all prima values per policy
        2. Order by fecha DESC, no_operacion DESC
        3. Find first record where valor_asegurado > 100
        4. If no valid record exists, take the first one
        """
        # Validate required columns
        required_cols = [col_id_poliza, col_fecha, col_valor_asegurado, col_valor_prima]
        
        if col_agrupacion_adicional:
            required_cols.append(col_agrupacion_adicional)
        
        tiene_no_operacion = col_no_operacion in df.columns
        
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            self.logger.warning(
                f"      ⚠️  Missing columns for ACSELE transformation: {missing_cols}"
            )
            return df
        
        # Build partition columns
        partition_cols = [col_id_poliza]
        if col_agrupacion_adicional:
            partition_cols.append(col_agrupacion_adicional)
        
        self.logger.info(f"      Deduplicating by: {', '.join(partition_cols)}")
        
        # Step 1: Calculate total prima per policy
        df = df.withColumn(
            "valor_prima_total",
            spark_sum(col_valor_prima).over(Window.partitionBy(*partition_cols))
        )
        
        # Step 2: Order by fecha DESC, no_operacion DESC
        order_by = [col(col_fecha).desc()]
        if tiene_no_operacion:
            order_by.append(col(col_no_operacion).desc())
        
        window_spec = Window.partitionBy(*partition_cols).orderBy(*order_by)
        df = df.withColumn("rn", row_number().over(window_spec))
        
        # Step 3: Mark valid records (valor_asegurado > 100)
        df = df.withColumn(
            "es_valido",
            when(col(col_valor_asegurado) > 100, 1).otherwise(0)
        )
        
        # Step 4: Get first valid record or first record if no valid exists
        window_valido = Window.partitionBy(*partition_cols).orderBy(
            col("es_valido").desc(),
            col("rn").asc()
        )
        
        df = df.withColumn("rn_final", row_number().over(window_valido))
        df_result = df.filter(col("rn_final") == 1)
        
        # Update valor_prima with total
        df_result = df_result.withColumn(col_valor_prima, col("valor_prima_total"))
        
        # Drop auxiliary columns
        df_result = df_result.drop("valor_prima_total", "rn", "es_valido", "rn_final")
        
        count_final = df_result.count()
        self.logger.info(f"      ✓ {count_final:,} unique records")
        
        return df_result
    
    # ============================================
    # ENRIQUECIMIENTO CON DIMENSIONES
    # ============================================
    
    def enrich_with_dimensions(self, df: DataFrame, dims_config: Dict, yyyy, mm) -> DataFrame:
        """
        Enriquecer fact table con FKs de dimensiones o con otras FACTS.
    
        - Mantiene el comportamiento actual para DIMs.
        - Cuando el JSON trae column_fact_source/column_dim_source, realiza JOIN adicional por fuente (pensado para FACT→FACT).
        """
        from pyspark.sql import functions as F
    
        df_enriched = df
    
        for dim_label, dim_config in dims_config.items():
            try:
                # -------------------------------
                # 1) Extraer configuración
                # -------------------------------
                dim_path    = dim_config.get('dim_path')
                col_fact    = dim_config.get('column_fact')        # Columna en FACT destino para el lookup (ej: Id_poliza - número)
                col_dim     = dim_config.get('column_dim')         # Columna en la tabla de referencia para matchear (ej: Num_poliza)
                col_dim_id  = dim_config.get('column_id')          # Surrogate ID en la tabla de referencia
                col_fact_id = dim_config.get('column_fact_id')     # Dónde guardar el FK en la FACT destino
    
                # NUEVO: solo para JOIN fact→fact por fuente (opcional, via JSON)
                col_fact_src = dim_config.get('column_fact_source')   # p.ej. "Id_fuente" en FACT_CLIENTES
                col_dim_src  = dim_config.get('column_dim_source')    # p.ej. "Id_fuente" en FACT_POLIZA_EMISION
    
                self.logger.info(f"→ {dim_label}: {dim_path}")
    
                # Validar configuración base
                if not all([dim_path, col_fact, col_dim, col_dim_id, col_fact_id]):
                    self.logger.warning(f"⚠️ Configuración incompleta para {dim_label}")
                    continue
    
                # Validar que la columna de FACT exista
                if col_fact not in df_enriched.columns:
                    self.logger.warning(f"⚠️ Columna {col_fact} no existe en fact table destino")
                    continue
    
                # ---------------------------------------------
                # 2) Cargar referencia (DIM o FACT) desde cache/S3
                #    - Primero intenta como FACT particionada (Delta + filtros anio/mes)
                #    - Si falla, lee completa (Delta o Parquet)
                # ---------------------------------------------
                if dim_path not in self.dimensions_cache:
                    self.logger.info(f"  Cargando dimensión/fact desde {dim_path}")
    
                    # Intentar como FACT Delta con particiones anio_proceso/mes_proceso
                    try:
                        df_dim = self.spark.read.format("delta").load(dim_path) \
                                    .where(f"anio_proceso = '{yyyy}' AND mes_proceso = '{mm}'")
                        self.logger.info(f"  ✓ Leído como FACT (anio_proceso={yyyy}, mes_proceso={mm})")
                    except Exception as e_fact:
                        self.logger.info(f"  No fue posible leer como FACT particionada: {str(e_fact)}")
                        # Fallback: lectura completa (Delta) o Parquet
                        df_dim = None
                        try:
                            df_dim = self.spark.read.format("delta").load(dim_path)
                            self.logger.info("  ✓ Leído como conjunto completo (Delta)")
                        except Exception as e_delta:
                            self.logger.info(f"  No es Delta: {str(e_delta)} — intentando Parquet recursivo")
                            df_dim = self.spark.read \
                                       .option("mergeSchema", "true") \
                                       .option("recursiveFileLookup", "true") \
                                       .parquet(dim_path)
                            self.logger.info("  ✓ Leído como conjunto completo (Parquet)")
    
                    # (Opcional) Filtrar 'estado' activo si existe
                    if 'estado' in df_dim.columns:
                        df_dim = df_dim.filter(F.col('estado') == 'A')
                        self.logger.info("  ✓ Filtrado por estado = 'A'")
    
                    # Cachear por ruta (clave que usas luego)
                    df_dim.cache()
                    self.dimensions_cache[dim_path] = df_dim
    
                else:
                    self.logger.info("✓ Referencia obtenida del cache")
                    df_dim = self.dimensions_cache[dim_path]
    
                self.logger.info(f"columnas en la referencia: {df_dim.columns}")
    
                # Validar columnas en referencia
                if col_dim not in df_dim.columns:
                    self.logger.warning(f"⚠️ Columna {col_dim} no existe en la referencia {dim_label}")
                    continue
                if col_dim_id not in df_dim.columns:
                    self.logger.warning(f"⚠️ Columna {col_dim_id} (ID) no existe en la referencia {dim_label}")
                    continue
    
                # ------------------------------------------------------
                # 3) Preparar lookup minimal (y fuente si fue configurado)
                # ------------------------------------------------------
                select_cols = [
                    F.col(col_dim).alias(f"_dim_{col_dim}"),
                    F.col(col_dim_id).alias(f"_dim_{col_dim_id}")
                ]
    
                # NUEVO: incluir fuente del lado referencia SOLO si está configurado en el JSON
                if col_dim_src and (col_dim_src in df_dim.columns):
                    select_cols.append(F.col(col_dim_src).alias(f"_dim_{col_dim_src}"))
    
                df_dim_lookup = df_dim.select(*select_cols).distinct()
                self.logger.info(f"columnas en el lookup: {df_dim_lookup.columns}")
    
                # ------------------------
                # 4) Armar condición JOIN
                # ------------------------
                join_cond = (df_enriched[col_fact] == df_dim_lookup[f"_dim_{col_dim}"])
    
                # NUEVO: si se solicitaron columnas de fuente y están disponibles en ambos lados, unir también por fuente
                if col_fact_src and col_dim_src \
                   and (col_fact_src in df_enriched.columns) \
                   and (f"_dim_{col_dim_src}" in df_dim_lookup.columns):
                    join_cond = join_cond & (df_enriched[col_fact_src] == df_dim_lookup[f"_dim_{col_dim_src}"])
                    self.logger.info(f"  JOIN por [{col_fact} + {col_fact_src}] → [{col_dim} + {col_dim_src}]")
                else:
                    self.logger.info(f"  JOIN por [{col_fact}] → [{col_dim}]")
    
                # ------------------------
                # 5) Ejecutar JOIN y setear FK
                # ------------------------
                df_enriched = df_enriched.join(df_dim_lookup, join_cond, "left")
    
                df_enriched = df_enriched.withColumn(
                    col_fact_id,  # FK en la fact destino
                    F.col(f"_dim_{col_dim_id}")
                ).drop(f"_dim_{col_dim}", f"_dim_{col_dim_id}")
    
                # limpiar columna fuente temporal si se usó
                if col_fact_src and col_dim_src and f"_dim_{col_dim_src}" in df_dim_lookup.columns:
                    df_enriched = df_enriched.drop(f"_dim_{col_dim_src}")
    
                self.logger.info(f"  ✓ FK {col_fact_id} agregado desde {dim_label}")
    
            except Exception as e:
                self.logger.error(f"✗ Error en {dim_label}: {str(e)}")
                import traceback
                self.logger.error(traceback.format_exc())
                continue
    
        return df_enriched
        
    def _obtener_llaves_negocio(self, fact_name: str) -> List[str]:
        """
        Determina las llaves de negocio según la fact table.

        Estas columnas identifican un registro único para MERGE.
        IMPORTANTE: Siempre incluir Id_Fuente para evitar conflictos entre fuentes.

        Returns:
            Lista de columnas que forman la llave única
        """
        llaves_por_fact = {
            # FACT_POLIZA_EMISION
            "fact_poliza_emision": ["Num_poliza", "Id_fuente", "Id_periodo_calculo"],

            # DIM_PRODUCTO  
            "dim_producto": ["Codigo", "Id_fuente"],

            # FACT_CLIENTES
            "fact_clientes": ["Id_poliza", "Id_tipo_identificacion", "Numero_identificacion", "Id_rol"],

            # FACT_COBERTURA
            # Una póliza puede tener múltiples coberturas
            # La misma cobertura puede renovarse (diferentes fechas)
            "fact_cobertura": ["Id_poliza", "Id_cobertura", "Id_fuente"],
        }

        llaves = llaves_por_fact.get(fact_name, [])

        if not llaves:
            self.logger.warning(
                f"⚠️  No hay llaves de negocio definidas para {fact_name}. "
                f"Se usará modo REEMPLAZO (borra y recarga todo el periodo)"
            )
        else:
            self.logger.info(f"Llaves de negocio para {fact_name}: {llaves}")

        return llaves
    
    # ========================================================================
    #                    FACT TABLE PROCESSING
    # ========================================================================
    
    def process_fact_table(self, fact_name: str, fact_config: Dict, yyyy: str, mm: str):
        """Process a single fact table with all transformations and enrichment."""
        try:
            tabla_name = fact_config['tabla_name']

            self.logger.info("\n" + "=" * 80)
            self.logger.info(f"PROCESSING FACT TABLE: {fact_name.upper()}")
            self.logger.info("=" * 80)
            self.logger.info(f"Target table: {tabla_name}")

            # STEP 1: Consolidate sources
            self.logger.info("\n→ STEP 1: Consolidating sources...")

            sources_config = fact_config.get('sources_config', {})
            unified_columns = fact_config.get('unified_columns', [])

            df_consolidado = self.consolidate_sources(
                sources_config,
                yyyy,
                mm,
                unified_columns,
                tabla_name
            )

            count_inicial = df_consolidado.count()
            self.logger.info(f"  ✓ Consolidated: {count_inicial:,} records")

            # STEP 2: Add partition columns (conditional based on config)
            # 🆕 Leer configuración de particiones desde JSON
            usar_particiones = fact_config.get('usar_particiones', True)  # Default: True
            
            if usar_particiones:
                self.logger.info("\n→ STEP 2: Adding partition columns...")
                df_consolidado = df_consolidado \
                    .withColumn("anio_proceso", lit(int(yyyy))) \
                    .withColumn("mes_proceso", lit(int(mm)))
                self.logger.info(f"  ✓ Partitions added: anio_proceso={yyyy}, mes_proceso={mm}")
            else:
                self.logger.info("\n→ STEP 2: Partition columns SKIPPED (usar_particiones=False)")
                self.logger.info("  ⚠️  This table will NOT use anio_proceso/mes_proceso partitions")
            # STEP 3: Enrich with dimensions
            self.logger.info("\n→ STEP 3: Enriching with dimensions...")

            dims_config = fact_config.get('dims', {})

            if dims_config:
                df_enriched = self.enrich_with_dimensions(df_consolidado, dims_config,yyyy,mm)
            else:
                df_enriched = df_consolidado
                self.logger.info("  No dimensions configured")

            # STEP 4: Select final columns
            self.logger.info("\n→ STEP 4: Selecting final columns...")

            final_columns = unified_columns.copy()

            # Add dimension FK columns
            for dim_label, dim_config in dims_config.items():
                col_fact_id = dim_config.get('column_fact_id')
                if col_fact_id and col_fact_id not in final_columns:
                    final_columns.append(col_fact_id)

            if "Id" in final_columns:
                final_columns.remove("Id")

            # Select only available columns
            cols_disponibles = [c for c in final_columns if c in df_enriched.columns]
            df_final = df_enriched.select(*cols_disponibles)

            llaves_negocio = self._obtener_llaves_negocio(fact_name)
            
            if "Id" not in df_final.columns:
                df_final = self._generar_id_determinista(df_final, llaves_negocio)
                self.logger.info("  ✓ Deterministic Id column added")
            

            # Apply business transformations
            df_final = self.cast_date_columns(df_final)

            count_final = df_final.count()
            self.logger.info(f"  Final records: {count_final:,}")
            self.logger.info(f"  Final columns: {len(df_final.columns)}")

            # STEP 5: Filter NULLs in business keys
            llaves_negocio = self._obtener_llaves_negocio(fact_name)

            if llaves_negocio:
                self.logger.info("\n→ Filtering NULLs in business keys...")

                total_con_nulls = 0
                for col_llave in llaves_negocio:
                    if col_llave in df_final.columns:
                        count_nulls = df_final.filter(col(col_llave).isNull()).count()

                        if count_nulls > 0:
                            pct_nulls = (count_nulls / count_inicial) * 100
                            self.logger.warning(
                                f"  ⚠️  {col_llave}: {count_nulls:,} NULLs ({pct_nulls:.2f}%)"
                            )
                            total_con_nulls += count_nulls

                if total_con_nulls > 0:
                    self.logger.info("  → Filtering records with NULLs...")

                    for col_llave in llaves_negocio:
                        if col_llave in df_final.columns:
                            df_final = df_final.filter(col(col_llave).isNotNull())

                    count_final = df_final.count()
                    registros_filtrados = count_inicial - count_final
                    pct_filtrado = (registros_filtrados / count_inicial) * 100

                    self.logger.warning(
                        f"  ⚠️  Filtered records: {registros_filtrados:,} ({pct_filtrado:.2f}%)"
                    )
                    self.logger.info(f"  ✓ Valid records: {count_final:,}")
                else:
                    self.logger.info("  ✓ No NULLs in business keys")

            # STEP 6: Load to Gold with incremental strategy
            self.logger.info("\n→ STEP 6: Loading to Gold...")
            gold_path = f"s3://{self.bucket_gold}/GOLD/CLIENTES_GOLD/{tabla_name}/"

            if "Id" not in df_final.columns:
                self.logger.error("❌ CRITICAL: Id column missing before gold load!")
                self.logger.error(f"Available columns: {df_final.columns}")
                raise ValueError("Id column is required but not present in final DataFrame")

            # 🆕 Leer configuración de particiones desde JSON
            usar_particiones = fact_config.get('usar_particiones', True)  # Default: True
            
            resultado = self.gold_loader.cargar(
                df=df_final,
                ruta_gold=gold_path,
                llaves_negocio=llaves_negocio,
                anio=int(yyyy),
                mes=int(mm),
                optimizar=True,
                z_order_columns=llaves_negocio,
                usar_particiones=usar_particiones  # 🆕 NUEVO PARÁMETRO
            )

            self.logger.info(f"\n{'=' * 80}")
            self.logger.info(f"✓ {fact_name.upper()} COMPLETED")
            self.logger.info(f"  Strategy: {resultado['estrategia']}")
            self.logger.info(f"  Inserted: {resultado['registros_insertados']:,}")
            self.logger.info(f"  Total in Gold: {resultado['registros_finales']:,}")
            self.logger.info(f"{'=' * 80}")

        except Exception as e:
            self.logger.error(f"\n✗ Error processing {fact_name}: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    def move_periods (self,spark,origin_path,year):
        old_path = f"{origin_path}_OLD/"
        origin_path = f"{origin_path}/"
        from delta.tables import DeltaTable
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        self.logger.info(f"Leyendo datos de la ruta {origin_path}")
        df = spark.read.format("delta").load(origin_path)
    
        df_mover = df.filter(F.col("anio_proceso") == year)
        self.logger.info(f"Copiando datos del año {year} a la ruta: {old_path}")
        (df_mover.write
            .format("delta")
            .mode("append")
            .partitionBy("anio_proceso")
            .save(old_path))
    
        dt = DeltaTable.forPath(spark, origin_path)
        self.logger.info(f"Eliminando datos del año {year} de la ruta: {origin_path}")
        dt.delete(f"anio_proceso = {year}")
        dt.vacuum(0)
        
        self.logger.info(f"generando manifest para datos originales")
        dt.generate("symlink_format_manifest")
        self.logger.info(f"generando manifest para datos antiguos guardados")
        DeltaTable.forPath(spark, old_path).generate("symlink_format_manifest")

# ============================================================================
#                           MAIN EXECUTION
# ============================================================================

def main():
    """Main Glue job function."""
    
    # Get required parameters
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'BUCKET_SILVER',
        'BUCKET_GOLD',
        'S3_CONFIG_PATH'

    ])
    
    # Optional parameters
    anio = None
    mes = None
    
    try:
        anio = getResolvedOptions(sys.argv, ['ANIO'])['ANIO']
    except:
        pass
    
    try:
        mes = getResolvedOptions(sys.argv, ['MES'])['MES']
    except:
        pass
    
   # # Determine execution period
   # # Determine execution period
   # fecha_actual = datetime.now()
   # 
   # if not anio and not mes:
   #     # MODO MES CAÍDO: Procesar mes anterior (igual que Silver)
   #     primer_dia_mes_actual = fecha_actual.replace(day=1)
   #     ultimo_dia_mes_anterior = primer_dia_mes_actual - timedelta(days=1)
   #    
   #     yyyy = str(ultimo_dia_mes_anterior.year)
   #     mm = f"{ultimo_dia_mes_anterior.month:02d}"
   #    
   #     print(f"[MES CAÍDO] Procesando periodo: {yyyy}-{mm}")
   #    
   # elif anio and not mes:
   #     # Si solo se pasa año, usar mes ANTERIOR también
   #     primer_dia_mes_actual = fecha_actual.replace(day=1)
   #     ultimo_dia_mes_anterior = primer_dia_mes_actual - timedelta(days=1)
   #    
   #     yyyy = str(anio)
   #     mm = f"{ultimo_dia_mes_anterior.month:02d}"
   #    
   # elif mes and not anio:
   #     # Si solo se pasa mes, usar año actual
   #     mm = f"{int(mes):02d}"
   #     yyyy = str(fecha_actual.year)
   #    
   # else:
   #     # Si se pasan ambos, usar exactamente lo que se pasó
   #     yyyy = str(anio)
   #     mm = f"{int(mes):02d}"
   #  
#
   # 
   # # Initialize Spark and Glue
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
   
    processor = None
   # 
   # try:
   #     print(f"""
   #             {'=' * 80}
   #             JOB: GOLD POLIZAS - INCREMENTAL DELTA LOADING (CORRECTED v2.1.0)
   #             {'=' * 80}
   #             Period: {yyyy}-{mm}
   #             Silver: s3://{args['BUCKET_SILVER']}
   #             Gold: s3://{args['BUCKET_GOLD']}
   #             Config: {args['S3_CONFIG_PATH']}
   #             {'=' * 80}
   #             """
   #     )
   #     
   #     # Initialize processor
    processor = GoldPolizasIncrementalProcessor(
        spark,
        glue_context,
        args['JOB_NAME'],
        args['BUCKET_SILVER'],
        args['BUCKET_GOLD'],
        "test"
    )
   #     
   #     # Load configuration
   #     config = processor.load_config(args['S3_CONFIG_PATH'])
   #     processor.config = config
   #     
   #     # STEP 1: Process dimensions
   #     processor.process_dimensions(config, yyyy, mm)
   #     
   #     # STEP 2: Process fact tables
   #     gold_configs = config.get('gold_configs', {})
   #     
   #     # Processing order (dependencies)
   #     orden_facts = ['dim_producto', 'fact_poliza_emision', 'fact_cobertura']
   #     
   #     facts_procesadas = 0
   #     for fact_name in orden_facts:
   #         if fact_name in gold_configs:
   #             fact_config = gold_configs[fact_name]
   #             if fact_config.get('status') == 1:
   #                 processor.process_fact_table(fact_name, fact_config, yyyy, mm)
   #                 facts_procesadas += 1
   #     
   #     # Process any other facts
   #     for fact_name, fact_config in gold_configs.items():
   #         if fact_name not in orden_facts and fact_config.get('status') == 1:
   #             processor.process_fact_table(fact_name, fact_config, yyyy, mm)
   #             facts_procesadas += 1
   #     
   #     # Final summary
   #     print(f"\n{'=' * 80}")
   #     print("✓ JOB COMPLETED SUCCESSFULLY")
   #     print(f"{'=' * 80}")
   #     print(f"  Period processed: {yyyy}-{mm}")
   #     print(f"  Dimensions: {len([d for d in config.get('dimensions',[]) if d.get('status')==1])}")
   #     print(f"  Facts generated: {facts_procesadas}")
   #     print(f"{'=' * 80}")
   #     
   #     job.commit()
   #     
   # except Exception as e:
   #     print(f"\n{'=' * 80}")
   #     print("✗ JOB ERROR")
   #     print(f"{'=' * 80}")
   #     print(f"Error: {str(e)}")
   #     print(f"{'=' * 80}")
   #     import traceback
   #     traceback.print_exc()
   #     raise
   #     
   # finally:
   #     if processor:
   #         processor.cleanup()
   #     spark.stop()
   #s3://alfa-datalake-dev-gold/GOLD/CLIENTES_GOLD/GOLD-CARACT_CLIENTES-FACT_POLIZA_EMISION/
    Gold = f"s3://{args['BUCKET_GOLD']}"
    path = f"{Gold}/GOLD/CLIENTES_GOLD/GOLD-CARACT_CLIENTES-FACT_POLIZA_EMISION"
    processor.move_periods(spark,path,2025)
    

    


if __name__ == "__main__":
    main()