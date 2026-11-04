"""
Job Glue: Silver → Gold con Carga Incremental Delta
Consolida múltiples fuentes (Cardiff, acsele, alfaprd) y genera fact tables con dimensiones

Autor: Data
Fecha: 2024-12-10

Parámetros Glue:
    --JOB_NAME: Nombre del job
    --BUCKET_SILVER: Bucket de Silver (ej: mi-bucket-silver)
    --BUCKET_GOLD: Bucket de Gold (ej: mi-bucket-gold)
    --S3_CONFIG_PATH: Ruta del JSON config (ej: s3://bucket/config/gold_config_polizas.json)
    --ANIO: Año a procesar (opcional, default: actual)
    --MES: Mes a procesar (opcional, default: actual)

FIXES:
- ✅ Evita columnas tipo void usando cast explícito
- ✅ Agrega particionamiento físico por anio_proceso/mes_proceso
- ✅ Infiere tipos de dato basado en nombres de columnas
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from utils import Utils
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, monotonically_increasing_id,
    year, month, coalesce, when, upper, trim, row_number, sum
)
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DateType, 
    TimestampType, DecimalType, DoubleType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
from typing import Dict, List, Optional
import logging
from s3_handler import S3Handler
# Importar DeltaGoldLoader
from delta_gold_loader import DeltaGoldLoader

# ============================================
# CLASE PRINCIPAL
# ============================================

class GoldPolizasIncrementalProcessor:
    """
    Procesa datos de múltiples fuentes (Cardiff, ACSELE) 
    y genera fact tables en Gold con carga incremental Delta
    """
    
    def __init__(self, spark, glue_context, job_name, bucket_silver, bucket_gold):
        self.spark = spark
        self.glue_context = glue_context
        self.job_name = job_name
        self.bucket_silver = bucket_silver
        self.bucket_gold = bucket_gold
        self.utils = Utils()
        self.logger = self._setup_logging()
        self.dimensions_cache = {}
        self.s3_handler = S3Handler(bucket_gold, glue_context)
        self.config = None  # Configuración JSON cargada
        
        # Inicializar DeltaGoldLoader para carga incremental
        self.gold_loader = DeltaGoldLoader(spark, modo_debug=False)
        
        self.logger.info("✓ Procesador Gold inicializado")
    
    def _setup_logging(self):
        """Configurar logging"""
        logger = logging.getLogger(f"GoldPolizas-{self.job_name}")
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
    
    # ============================================
    # INFERENCIA DE TIPOS DE DATO
    # ============================================
    
    def _inferir_tipo_columna(self, nombre_col: str):
        """
        Infiere el tipo de dato basado en el nombre de la columna
        
        Reglas:
        - Id_* → StringType (IDs son strings para manejar valores mixtos)
        - Fecha_* → DateType
        - Valor_* → DecimalType(18,4)
        - *_fuente → StringType
        - Descripcion, Codigo, Compania → StringType
        - Default → StringType (seguro)
        """
        nombre_lower = nombre_col.lower()
        
        # IDs como string (más flexible para valores mixtos)
        if nombre_col.startswith("Id_"):
            return StringType()
        
        # Fechas
        if nombre_col.startswith("Fecha_"):
            return DateType()
        
        # Valores monetarios
        if nombre_col.startswith("Valor_"):
            return DecimalType(18, 4)
        
        # Campos de texto conocidos
        if any(x in nombre_lower for x in ['descripcion', 'codigo', 'compania', 'fuente']):
            return StringType()
        
        # Default: String (más seguro)
        return StringType()
    
    # ============================================
    # CARGA DE CONFIGURACIÓN
    # ============================================
    
    def load_config(self, s3_config_path: str) -> Dict:
        """Cargar configuración JSON desde S3"""
        try:
            self.logger.info(f"Cargando configuración: {s3_config_path}")
            
            # Leer JSON desde S3
            df_config = self.spark.read.option("multiline", "true").json(s3_config_path)
            config_row = df_config.collect()[0]
            config = self._row_to_dict(config_row.asDict())
            
            self.logger.info("✓ Configuración cargada exitosamente")
            return config
            
        except Exception as e:
            self.logger.error(f"Error cargando configuración: {str(e)}")
            raise
    
    def _row_to_dict(self, obj):
        """Convertir Spark Row a dict regular"""
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
    
    # ============================================
    # PROCESAMIENTO DE DIMENSIONES
    # ============================================
    
    def process_dimensions(self, config: Dict, yyyy: str, mm: str):
        """
        Procesar dimensiones: copiar de Silver a Gold
        Las dimensiones NO son incrementales, se reemplazan completas
        """
        self.logger.info("="*80)
        self.logger.info("PASO 1: PROCESANDO DIMENSIONES")
        self.logger.info("="*80)
        
        dimensions = config.get('dimensions', [])
        active_dims = [d for d in dimensions if d.get('status') == 1]
        
        self.logger.info(f"Dimensiones activas: {len(active_dims)}")
        
        for dim_config in active_dims:
            try:
                dim_name = dim_config['name']
                self.logger.info(f"\n→ {dim_name}")
                
                # Armar rutas usando convención
                dim_key = dim_name.replace('dim_', '').upper()
                dim_key = dim_key.replace('.xlsx', '')
                
                # Silver path
                silver_path = f"s3://{self.bucket_silver}/GENERAL/TRANSFORMATION/GENERAL_SILVER_DIM_{dim_key}_CLIENTES/"
                self.logger.info(f"  Silver: {silver_path}")
                
                # Leer Silver
                df_dim = self.spark.read.format("parquet").load(silver_path)
                count = df_dim.count()
                self.logger.info(f"  Registros: {count:,}")
                
                if count == 0:
                    self.logger.warning(f"  ⚠️  Sin datos en {dim_name}")
                    continue
                
                # Agregar timestamp de carga
                df_dim = df_dim.withColumn("fecha_carga_gold", current_timestamp())
                
                # Gold path
                gold_path = f"s3://{self.bucket_gold}/GOLD/CLIENTES_GOLD/GOLD-CARACT_CLIENTES-DIM_DIM_{dim_key}_GOLD/"
                self.logger.info(f"  Gold: {gold_path}")
                dynamic_frame = DynamicFrame.fromDF(df_dim, self.glue_context, f"{dim_key}_gold")
                # Guardar en Gold (OVERWRITE completo)
                self.s3_handler.save_files_to_s3(
                    df=dynamic_frame,
                    s3_target_path=gold_path,
                    format="parquet",
                    mode="overwrite"
                )
                
                self.logger.info(f"  ✓ Guardada en Gold")
                
                # Cachear dimensión para usar en facts
                
                df_dim_gold = self.utils.read_full_parquet(self.spark, gold_path)
                df_dim_gold.cache()
                self.dimensions_cache[dim_name] = df_dim_gold
                self.logger.info(f"  ✓ Cacheada en memoria")
                
            except Exception as e:
                self.logger.error(f"  ✗ Error en {dim_name}: {str(e)}")
                continue
        
        self.logger.info(f"\n✓ {len(active_dims)} dimensiones procesadas")
    
    # ============================================
    # CONSOLIDACIÓN DE FUENTES (CARDIFF + ACSELE)
    # ============================================
    
    def consolidate_sources(
        self,
        sources_config: Dict,
        yyyy: str,
        mm: str,
        unified_columns: List[str],
        fact_name: str  # ← nuevo parámetro   
    ) -> DataFrame:
        """
        Consolida datos de múltiples fuentes (Cardiff, ACSELE)
        Lee las particiones yyyy/mm de cada fuente y las une
        """
        dfs_consolidados = []
        
        for source_name, source_config in sources_config.items():
            try:
                self.logger.info(f"  → Fuente: {source_name.upper()}")
                
                # Construir ruta con particiones
                tabla_silver = source_config['tabla_silver']
                
                # Obtener base_path de la configuración (si existe)
                if self.config and 'sources' in self.config:
                    source_info = self.config.get('sources', {}).get(source_name, {})
                    base_path = source_info.get('base_path', f"{source_name.upper()}/")
                else:
                    base_path = f"{source_name.upper()}/"

                # Construir ruta con base_path
                ruta_particion = f"s3://{self.bucket_silver}/{base_path}{tabla_silver}/yyyy={yyyy}/mm={mm}"
                
                self.logger.info(f"    Ruta: {ruta_particion}")
                
                # Leer partición
                df_source = self.spark.read.format("delta").load(ruta_particion)
                count_source = df_source.count()
                
                if count_source == 0:
                    self.logger.warning(f"    ⚠️  Sin datos")
                    continue
                
                self.logger.info(f"    Registros: {count_source:,}")
                
                # Aplicar mapeo de columnas
                column_mapping = source_config.get('column_mapping', {})
                df_mapped = self._apply_column_mapping(df_source, column_mapping)
                
                target_cols = list(column_mapping.keys())
                df_mapped = df_mapped.select(*[c for c in target_cols if c in df_mapped.columns])

                # Aplicar transformación AUTOMÁTICA para ACSELE (si aplica)
                if source_name.lower() == 'acsele':
                    if fact_name == 'GOLD-CARACT_CLIENTES-FACT_POLIZA_EMISION':
                        self.logger.info(f"    Aplicando transformación automática ACSELE (poliza_emision)...")

                        df_mapped =df_mapped.distinct()
                        print(df_mapped.show(2))

                        df_mapped = self._apply_acsele_transformation(
                            df_mapped,
                            col_id_poliza='Numero_poliza',
                            col_fecha='Fecha_emision_poliza',
                            col_valor_asegurado='Valor_asegurado',
                            col_valor_prima='Valor_prima'
                            
                        )
                    elif fact_name == 'GOLD-CARACT_CLIENTES-FACT_COBERTURA':
                        self.logger.info(f"    Aplicando transformación automática ACSELE (cobertura)...")
                        df_mapped = self._apply_acsele_transformation(
                            df_mapped,
                            col_id_poliza='Id_poliza',
                            col_fecha='Fecha_emision_poliza',
                            col_valor_asegurado='Valor_asegurado',
                            col_valor_prima='Valor_prima',
                            col_agrupacion_adicional = "Descripcion"
                        )
                # Agregar ID de fuente si no existe (identificar origen: CARDIFF o ACSELE)
                if "Id_fuente" not in df_mapped.columns:
                    source_id = self.config.get('sources', {}).get(source_name, {}).get('source_id', source_name.upper())
                    df_mapped = df_mapped.withColumn("Id_fuente", lit(source_id).cast(StringType()))
                
                # ⚠️ CRÍTICO: Agregar columnas faltantes CON TIPO DE DATO EXPLÍCITO
                for col_unif in unified_columns:
                    if col_unif not in df_mapped.columns and col_unif != 'Id':
                        # Inferir tipo de dato basado en nombre de columna
                        tipo_dato = self._inferir_tipo_columna(col_unif)
                        df_mapped = df_mapped.withColumn(col_unif, lit(None).cast(tipo_dato))
                        self.logger.debug(f"      Columna {col_unif} agregada como {tipo_dato}")
                
                # Seleccionar solo columnas unificadas (excepto Id que se genera después)
                cols_select = [c for c in unified_columns if c != 'Id' and c in df_mapped.columns]
                df_mapped = df_mapped.select(*cols_select)
                
                dfs_consolidados.append(df_mapped)
                self.logger.info(f"    ✓ {source_name.upper()} procesada")
                
            except Exception as e:
                self.logger.warning(f"    ✗ Error en {source_name}: {str(e)}")
                continue
        
        # Unir todas las fuentes
        if not dfs_consolidados:
            raise ValueError("No se pudo leer ninguna fuente")
        
        self.logger.info(f"\n  Consolidando {len(dfs_consolidados)} fuentes...")
        
        df_consolidado = dfs_consolidados[0]
        for df in dfs_consolidados[1:]:
            df_consolidado = df_consolidado.unionByName(df, allowMissingColumns=True)
        
        count_total = df_consolidado.count()
        self.logger.info(f"  ✓ Total consolidado: {count_total:,} registros")
        
        return df_consolidado
    

    def _apply_column_mapping(self, df: DataFrame, mapping: Dict) -> DataFrame:
        """
        Aplica mapeo duplicando columnas (alias) en vez de renombrar.
        Esto permite soportar múltiples target_col desde el mismo source_col.
        """
        if not mapping:
            return df
    
        for target_col, source_col in mapping.items():
            if source_col in df.columns:
                df = df.withColumn(target_col, col(source_col))
            # Si el source no existe, podrías intentar buscar si ya fue renombrado/aliaseado,
            # pero con alias no es necesario en la mayoría de casos.
    
        return df

    
    def _apply_acsele_transformation(
        self, 
        df: DataFrame,
        col_id_poliza: str = 'Id_poliza',
        col_fecha: str = 'Fecha_emision_poliza',
        col_valor_asegurado: str = 'Valor_asegurado',
        col_valor_prima: str = 'Valor_prima',
        col_agrupacion_adicional: Optional[str] = None
    ) -> DataFrame:
        """
        Aplicar transformación automática para ACSELE (genérica para diferentes facts)
        
        Lógica:
        - Deduplica por número de póliza (y opcionalmente otra columna)
        - Prioriza registros con valor_asegurado != 0
        - Suma todos los valores de prima de la póliza
        - Se queda con el registro más reciente según la fecha especificada (DESC)
        
        Args:
            df: DataFrame de ACSELE después del mapeo de columnas
            col_id_poliza: Nombre de la columna con el ID de póliza
            col_fecha: Nombre de la columna con la fecha para ordenar (ej: Fecha_emision_poliza, Fecha_fin_cobertura)
            col_valor_asegurado: Nombre de la columna con el valor asegurado
            col_valor_prima: Nombre de la columna con el valor de prima
            col_agrupacion_adicional: (Opcional) Columna adicional para agrupar (ej: Descripcion para coberturas)
        
        Returns:
            DataFrame deduplicado
        """
        # Verificar que existan las columnas necesarias
        required_cols = [col_id_poliza, col_fecha, col_valor_asegurado, col_valor_prima]
        
        # Si hay columna adicional, agregarla a las requeridas
        if col_agrupacion_adicional:
            required_cols.append(col_agrupacion_adicional)
        
        missing_cols = [c for c in required_cols if c not in df.columns]
        
        if missing_cols:
            self.logger.warning(f"      ⚠️  Faltan columnas para transformación ACSELE: {missing_cols}")
            self.logger.warning(f"      Se omite la transformación")
            return df
        
        # Construir lista de columnas para particionBy
        partition_cols = [col_id_poliza]
        if col_agrupacion_adicional:
            partition_cols.append(col_agrupacion_adicional)
            self.logger.info(f"      Deduplicando por: {', '.join(partition_cols)} (usando {col_fecha})...")
        else:
            self.logger.info(f"      Deduplicando por póliza (usando {col_fecha})...")
        
        # Window para priorizar registros con valor_asegurado != 0 y fecha más reciente
        window_nonzero = Window.partitionBy(*partition_cols).orderBy(
            when(col(col_valor_asegurado) != 0, 0).otherwise(1),
            col(col_fecha).desc()
        )
        
        # Calcular suma de primas por las columnas de partición
        df = df.withColumn(
            "valor_prima_total",
            sum(col_valor_prima).over(Window.partitionBy(*partition_cols))
        )
        
        # Asignar ranking
        df = df.withColumn("rn", row_number().over(window_nonzero))
        
        # Filtrar solo el primer registro
        df = df.filter(col("rn") == 1).drop("rn")
        
        # Reemplazar valor_prima con la suma total
        df = df.withColumn(col_valor_prima, col("valor_prima_total")).drop("valor_prima_total")
        
        count_dedup = df.count()
        if col_agrupacion_adicional:
            self.logger.info(f"      ✓ {count_dedup:,} combinaciones únicas")
        else:
            self.logger.info(f"      ✓ {count_dedup:,} pólizas únicas")
        
        return df
    
    # ============================================
    # ENRIQUECIMIENTO CON DIMENSIONES
    # ============================================
    
    def enrich_with_dimensions(self, df: DataFrame, dims_config: Dict) -> DataFrame:
        """
        Enriquecer fact table con FKs de dimensiones
        
        Args:
            df: DataFrame de la fact table
            dims_config: Configuración de dimensiones a joinear
        
        Returns:
            DataFrame enriquecido con columnas FK
        """
        df_enriched = df
        
        for dim_label, dim_config in dims_config.items():
            try:
                dim_name = dim_config.get('dimension_name')
                col_fact_lookup = dim_config.get('column_fact_lookup')
                col_dim_lookup = dim_config.get('column_dim_lookup')
                col_fact_id = dim_config.get('column_fact_id')
                
                self.logger.info(f"    → {dim_label}: {dim_name}")
                
                # Obtener dimensión de cache
                if dim_name not in self.dimensions_cache:
                    self.logger.warning(f"      ⚠️  Dimensión {dim_name} no está en cache")
                    continue
                
                df_dim = self.dimensions_cache[dim_name]
                
                # Preparar dimensión para join
                df_dim_lookup = df_dim.select(
                    col(col_dim_lookup).alias(f"_dim_{col_dim_lookup}"),
                    col("Id").alias(col_fact_id)
                ).distinct()
                
                # Join con fact
                df_enriched = df_enriched.join(
                    df_dim_lookup,
                    df_enriched[col_fact_lookup] == df_dim_lookup[f"_dim_{col_dim_lookup}"],
                    "left"
                ).drop(f"_dim_{col_dim_lookup}")
                
                self.logger.info(f"      ✓ FK {col_fact_id} agregado")
                
            except Exception as e:
                self.logger.error(f"      ✗ Error en {dim_label}: {str(e)}")
                continue
        
        return df_enriched
    
    
    # ============================================
    # DETERMINACIÓN DE LLAVES DE NEGOCIO
    # ============================================
    
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
            # Una póliza puede existir en Cardiff Y ACSELE → necesita Id_Fuente
            "fact_poliza_emision": ["Numero_poliza", "Id_fuente"],
            
            # DIM_PRODUCTO  
            # Un producto puede existir en Cardiff Y ACSELE → necesita Id_Fuente
            # Además, mismo código puede estar en diferentes compañías
            "dim_producto": ["Codigo", "Codigo_compania", "Id_fuente"],
            
            # FACT_COBERTURA
            # Una póliza puede tener múltiples coberturas
            # La misma cobertura puede renovarse (diferentes fechas)
            # Y puede existir en Cardiff Y ACSELE
            "fact_cobertura": ["Id_poliza","Id_cobertura", "Id_fuente"],
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
    
    # ============================================
    # PROCESAMIENTO DE FACT TABLES
    # ============================================
    
    def process_fact_table(
        self,
        fact_name: str,
        fact_config: Dict,
        yyyy: str,
        mm: str
    ):
        """Procesar una fact table específica"""
        
        try:
            self.logger.info("\n" + "="*80)
            self.logger.info(f"PROCESANDO FACT: {fact_name.upper()}")
            self.logger.info("="*80)
            
            tabla_name = fact_config['tabla_name']
            sources_config = fact_config['sources_config']
            unified_columns = fact_config['unified_columns']
            
            self.logger.info(f"Tabla Gold: {tabla_name}")
            self.logger.info(f"Fuentes configuradas: {len(sources_config)}")
            
            # PASO 1: Consolidar fuentes
            self.logger.info("\n→ PASO 1: Consolidando fuentes...")
            df_consolidated = self.consolidate_sources(
                sources_config,
                yyyy,
                mm,
                unified_columns,
                tabla_name
            )
            
            # PASO 2: Enriquecer con dimensiones (obtener FKs)
            self.logger.info("\n→ PASO 2: Enriqueciendo con dimensiones...")
            dims_config = fact_config.get('dims', {})
            
            if dims_config:
                df_enriched = self.enrich_with_dimensions(df_consolidated, dims_config)
            else:
                df_enriched = df_consolidated
                self.logger.info("  Sin dimensiones configuradas")
            
           
            # PASO 3: Agregar ID único
            self.logger.info("\n→ PASO 3: Agregando ID único...")
            
            if "Id" not in df_enriched.columns:
                df_enriched = df_enriched.withColumn(
                    "Id", 
                    monotonically_increasing_id()
                )
            
            # PASO 4: Seleccionar columnas finales
            self.logger.info("\n→ PASO 4: Seleccionando columnas finales...")
            
            final_columns = unified_columns.copy()
            
            # Agregar columnas FK de dimensiones
            for dim_label, dim_config in dims_config.items():
                col_fact_id = dim_config.get('column_fact_id')
                if col_fact_id and col_fact_id not in final_columns:
                    final_columns.append(col_fact_id)
            
            # Seleccionar solo columnas disponibles
            cols_disponibles = [c for c in final_columns if c in df_enriched.columns]
            df_final = df_enriched.select(*cols_disponibles)
            
            count_final = df_final.count()
            self.logger.info(f"  Registros finales: {count_final:,}")
            
            # PASO 5: Cargar a Gold usando DeltaGoldLoader (carga incremental inteligente)
            self.logger.info("\n→ PASO 5: Cargando a Gold con DeltaGoldLoader...")
            gold_path = f"s3://{self.bucket_gold}/GOLD/CLIENTES_GOLD/{tabla_name}/"
            
            # Determinar llaves de negocio según la fact
            llaves_negocio = self._obtener_llaves_negocio(fact_name)
            
            count_inicial = df_final.count()
            self.logger.info(f"  Registros totales: {count_inicial:,}")
            
            # 🆕 FILTRAR NULLS EN LLAVES DE NEGOCIO
            self.logger.info("\n→ Verificando NULLs en llaves de negocio...")
            
            llaves_negocio = self._obtener_llaves_negocio(fact_name)
            
            if llaves_negocio:
                # Reportar NULLs por cada columna
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
               
                # Filtrar registros con NULLs en cualquier llave
                if total_con_nulls > 0:
                    self.logger.info("  → Filtrando registros con NULLs...")
                   
                    for col_llave in llaves_negocio:
                        if col_llave in df_final.columns:
                            df_final = df_final.filter(col(col_llave).isNotNull())
                   
                    count_final = df_final.count()
                    registros_filtrados = count_inicial - count_final
                    pct_filtrado = (registros_filtrados / count_inicial) * 100
                   
                    self.logger.warning(
                        f"  ⚠️  Registros filtrados: {registros_filtrados:,} ({pct_filtrado:.2f}%)"
                    )
                    self.logger.info(f"  ✓ Registros válidos: {count_final:,}")
                else:
                    self.logger.info("  ✓ Sin NULLs en llaves de negocio")

                
            resultado = self.gold_loader.cargar(
                df=df_final,
                ruta_gold=gold_path,
                llaves_negocio=llaves_negocio,  # ← Activa MERGE
                anio=int(yyyy),
                mes=int(mm),
                optimizar=True,
                z_order_columns=llaves_negocio  # ← Optimizar por llaves
            )
            
            self.logger.info(f"\n{'='*80}")
            self.logger.info(f"✓ {fact_name.upper()} COMPLETADA")
            self.logger.info(f"  Estrategia: {resultado['estrategia']}")
            self.logger.info(f"  Registros insertados: {resultado['registros_insertados']:,}")
            self.logger.info(f"  Total en Gold: {resultado['registros_finales']:,}")
            self.logger.info(f"{'='*80}")
            
        except Exception as e:
            self.logger.error(f"\n✗ Error procesando {fact_name}: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
    
    # ============================================
    # CLEANUP
    # ============================================
    
    def cleanup(self):
        """Limpiar recursos cacheados"""
        self.logger.info("\n" + "="*80)
        self.logger.info("LIMPIANDO RECURSOS")
        self.logger.info("="*80)
        
        count_cache = len(self.dimensions_cache)
        
        for name, df in self.dimensions_cache.items():
            try:
                df.unpersist()
            except:
                pass
        
        self.dimensions_cache.clear()
        self.spark.catalog.clearCache()
        
        self.logger.info(f"✓ {count_cache} objetos liberados de memoria")

# ============================================
# FUNCIÓN PRINCIPAL
# ============================================

def main():
    """Función principal del job Glue"""
    
    # Obtener parámetros obligatorios
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'BUCKET_SILVER',
        'BUCKET_GOLD',
        'S3_CONFIG_PATH'
    ])
    
    # Parámetros opcionales (año y mes)

    anio = None
    mes = None
    
    # Intentar leer parámetros ANIO/MES (opcionales)
    try:
        anio = getResolvedOptions(sys.argv, ['ANIO'])['ANIO']
    except:
        pass
    
    try:
        mes = getResolvedOptions(sys.argv, ['MES'])['MES']
    except:
        pass
    
    def periodo_anterior(fecha_base: datetime):
        """
        Devuelve (yyyy, mm) del mes inmediatamente anterior a fecha_base.
        Maneja correctamente enero -> diciembre del año anterior.
        """
        primer_dia_mes_actual = fecha_base.replace(day=1)
        ultimo_dia_mes_anterior = primer_dia_mes_actual - timedelta(days=1)
        return str(ultimo_dia_mes_anterior.year), f"{ultimo_dia_mes_anterior.month:02d}"
    
    # Fecha de ejecución
    fecha_actual = datetime.now()
    
    if not anio and not mes:
        # Caso 1: no se pasa nada → usar mes anterior y año correspondiente
        yyyy, mm = periodo_anterior(fecha_actual)
    
    elif anio and not mes:
        # Caso 2: solo ANIO → usar mes anterior al actual
        yyyy_tmp, mm = periodo_anterior(fecha_actual)
        yyyy = str(anio)  # respetar el año pasado por parámetro
    
    elif mes and not anio:
        # Caso 3: solo MES → decidir el año correcto:
        # Si el MES pasado coincide con el "mes anterior" a hoy, usar el año calculado del periodo anterior.
        # Si no coincide, por simplicidad usar el año actual (ajusta esta regla si tu negocio requiere otra cosa).
        yyyy_prev, mm_prev = periodo_anterior(fecha_actual)
        mm = f"{int(mes):02d}"
        yyyy = yyyy_prev if mm == mm_prev else str(fecha_actual.year)
    
    else:
        # Caso 4: ANIO y MES presentes → respetar ambos
        yyyy = str(anio)
        mm = f"{int(mes):02d}"
    
    # Normalizar formatos finales
    yyyy = str(yyyy)
    mm = f"{int(mm):02d}"

    
    # Inicializar Spark y Glue
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    
    processor = None
    
    try:
        print(f"""
{'='*80}
JOB: GOLD POLIZAS - CARGA INCREMENTAL DELTA
{'='*80}
Periodo: {yyyy}-{mm}
Silver: s3://{args['BUCKET_SILVER']}
Gold: s3://{args['BUCKET_GOLD']}
Config: {args['S3_CONFIG_PATH']}
{'='*80}
""")
        
        # Inicializar procesador
        processor = GoldPolizasIncrementalProcessor(
            spark,
            glue_context,
            args['JOB_NAME'],
            args['BUCKET_SILVER'],
            args['BUCKET_GOLD']
        )
        
        # Cargar configuración JSON
        config = processor.load_config(args['S3_CONFIG_PATH'])
        processor.config = config  # Guardar config en el procesador
        
        # PASO 1: Procesar dimensiones (Silver → Gold)
        processor.process_dimensions(config, yyyy, mm)
        
        # PASO 2: Procesar fact tables (en orden de dependencias)
        gold_configs = config.get('gold_configs', {})
        
        # Orden: dim_producto primero (porque poliza_emision depende de él)
        orden_facts = ['dim_producto', 'fact_poliza_emision', 'fact_cobertura']
        
        facts_procesadas = 0
        for fact_name in orden_facts:
            if fact_name in gold_configs:
                fact_config = gold_configs[fact_name]
                if fact_config.get('status') == 1:
                    processor.process_fact_table(fact_name, fact_config, yyyy, mm)
                    facts_procesadas += 1
        
        # Procesar cualquier otra fact que no esté en el orden
        for fact_name, fact_config in gold_configs.items():
            if fact_name not in orden_facts and fact_config.get('status') == 1:
                processor.process_fact_table(fact_name, fact_config, yyyy, mm)
                facts_procesadas += 1
        
        # Resumen final
        print(f"\n{'='*80}")
        print("✓ JOB COMPLETADO EXITOSAMENTE")
        print(f"{'='*80}")
        print(f"  Periodo procesado: {yyyy}-{mm}")
        print(f"  Dimensiones: {len([d for d in config.get('dimensions',[]) if d.get('status')==1])}")
        print(f"  Facts generadas: {facts_procesadas}")
        print(f"{'='*80}")
        
        job.commit()
        
    except Exception as e:
        print(f"\n{'='*80}")
        print(f"✗ ERROR EN EL JOB")
        print(f"{'='*80}")
        print(f"Error: {str(e)}")
        print(f"{'='*80}")
        import traceback
        traceback.print_exc()
        raise
        
    finally:
        if processor:
            processor.cleanup()
        spark.stop()

if __name__ == "__main__":
    main()
