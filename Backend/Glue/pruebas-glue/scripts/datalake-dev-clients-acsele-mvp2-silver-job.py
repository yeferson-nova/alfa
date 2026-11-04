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
import pandas as pd
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, isnan, when, count
from awsglue.dynamicframe import DynamicFrame
from acsele_staging_mvp2 import AcseleProductoPolizasStagingProcessor
from data_harmonization_manager import DataHarmonizationManager
from mpv2_transformer_proccessor import DataCleanupProcessor
from utils import Utils
from datetime import datetime, timedelta
import calendar
from s3_handler import S3Handler
from cleanup import CleanUp



class SilverLayerProcessor:
    """Enhanced Silver layer processor with dimension mapping capabilities."""

    def __init__(self, spark, glue_context, job_name, bucket_silver):
        self.spark = spark
        self.glue_context = glue_context
        self.job_name = job_name
        self.bucket_silver = bucket_silver
        self.bucket_bronze = None 
        self.logger = self._setup_logging()
        self.utils = Utils()
        self.s3_handler = S3Handler(bucket_silver, glue_context)
        self.cleanup = CleanUp()
        self.silver_transformer = DataCleanupProcessor(spark, glue_context)
        self.harmonization_manager = DataHarmonizationManager()

       
        self.dimensions_cache = {}
       
        self.utils.configure_spark_optimizations(spark, "SILVER", "datalake-dev-clients-silver-job")
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


    def _setup_logging(self):
        """Configure logging."""
        import logging
        logger = logging.getLogger(f"SilverLayer-{self.job_name}")
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def load_config(self, s3_config_path):
        """Load JSON configuration from S3."""
        try:
            self.logger.info(f"Loading configuration from: {s3_config_path}")
           
            try:
                df = self.spark.read.option("multiline", "true").json(s3_config_path)
                config_row = df.collect()[0]
                config = config_row.asDict()
               
                config = self._convert_spark_row_to_dict(config)
               
                self.logger.info("Loaded config using Spark JSON reader")
               
            except Exception as spark_error:
                self.logger.warning(f"Spark JSON reader failed: {spark_error}")
              
                config_df = self.spark.read.option("wholetext", "true").text(s3_config_path)
                config_json = config_df.collect()[0][0]
               
                config_json = self._clean_json_string(config_json)
                config = json.loads(config_json)
               
                self.logger.info("Loaded config using text reader")
           
            self._validate_config(config)

            if 'fact_configs' in config:
                for fact_name, fact_config in config['fact_configs'].items():
                    for output in fact_config.get('outputs', []):
                        if 'additional_transforms' in output:
                            self.logger.info(f"DEBUG - {output['name']} additional_transforms type: {type(output['additional_transforms'])}")
                            for i, transform in enumerate(output['additional_transforms']):
                                self.logger.info(f"DEBUG - Transform {i}: type={type(transform)}, value={transform}")
           
            self.logger.info("Configuration loaded and validated successfully")
            return config
           
        except Exception as e:
            self.logger.error(f"Error loading config: {str(e)}")
            self.logger.error(f"Please check the JSON file format at: {s3_config_path}")
            raise
   
    def _clean_json_string(self, json_string):
        """Clean JSON string from common formatting issues."""
        if json_string.startswith('\ufeff'):
            json_string = json_string[1:]
       
        json_string = json_string.strip()
       
        self.logger.info(f"Cleaned JSON (first 100 chars): {json_string[:100]}")
       
        return json_string
   
    def _convert_spark_row_to_dict(self, obj):
        """Convert Spark Row objects to regular dictionaries."""
        from pyspark.sql.types import Row
       
        if isinstance(obj, Row):
            return {k: self._convert_spark_row_to_dict(v) for k, v in obj.asDict().items()}
        elif hasattr(obj, 'asDict'):
            return {k: self._convert_spark_row_to_dict(v) for k, v in obj.asDict().items()}
        elif isinstance(obj, list):
            return [self._convert_spark_row_to_dict(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: self._convert_spark_row_to_dict(v) for k, v in obj.items()}
        else:
            return obj

    def _validate_config(self, config):
        """Validate configuration structure."""
        required_keys = ['dim', 'fact']
       
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required key in config: {key}")
       
        for dim in config.get('dim', []):
            if 'name' not in dim or 'status' not in dim:
                raise ValueError(f"Invalid dim structure: {dim}")
       
        for fact in config.get('fact', []):
            if 'name' not in fact or 'status' not in fact:
                raise ValueError(f"Invalid fact structure: {fact}")
                
        if 'fact_configs' in config:
            active_facts = [f['name'] for f in config['fact'] if f.get('status') == 1]
            missing_configs = [f for f in active_facts if f not in config['fact_configs']]
            if missing_configs:
                raise ValueError(f"Missing fact_configs for active facts: {missing_configs}")
       
        self.logger.info("Configuration validation passed")

    def get_active_items(self, config, item_type):
        """Get items with status = 1."""
        active_items = [item['name'] for item in config.get(item_type, []) if item.get('status') == 1]
        self.logger.info(f"Active {item_type}: {active_items}")
        return active_items

    def load_homologation_data(self, s3_homologation_path):
        """Load homologation data."""
        try:
            df_homologaciones = self.spark.read.csv(s3_homologation_path, header=True, inferSchema=True, sep=";")
            df_cod_tipo = df_homologaciones.filter(col("campo_referencia") == "cod_tipo_identificacion")
            df_otros = df_homologaciones.filter(col("campo_referencia") != "cod_tipo_identificacion")
            df_cod_tipo = self.cleanup.normalize_columns(df_cod_tipo, "valor_referencia")
            return df_cod_tipo.unionByName(df_otros)
        except Exception as e:
            self.logger.error(f"Error loading homologation data: {str(e)}")
            raise


    def _calculate_client_status(self, df, periodo_inicio, periodo_fin):
        """
        Calcula y SOBRESCRIBE 'estado_cliente' (1=activo, 2=inactivo) con manejo de nulos según negocio:
        - Si 'fecha_inicio_de_vigencia' existe y 'fecha_fin_de_vigencia' NO existe → 1
        - Si NO existe ninguna de las dos → 1
        - Si existen ambas → traslape estándar
        """
        # Si faltan ambas columnas, asignar 1 directamente
        if ("fecha_inicio_de_vigencia" not in df.columns) or ("fecha_fin_de_vigencia" not in df.columns):
            self.logger.warning("No se encontraron columnas de vigencia; estado_cliente = '1' por defecto.")
            return df.withColumn("estado_cliente", F.lit("1"))

        # Construcción del periodo
        df = df.withColumn(
            "periodo_inicio_date",
            F.to_date(
                F.concat(
                    F.substring(F.lit(periodo_inicio), 1, 4),
                    F.lit("-"),
                    F.substring(F.lit(periodo_inicio), 5, 2),
                    F.lit("-01")
                ), "yyyy-MM-dd"
            )
        ).withColumn(
            "periodo_fin_date",
            F.last_day(
                F.to_date(
                    F.concat(
                        F.substring(F.lit(periodo_fin), 1, 4),
                        F.lit("-"),
                        F.substring(F.lit(periodo_fin), 5, 2),
                        F.lit("-01")
                    ), "yyyy-MM-dd"
                )
            )
        )

        # Cast defensivo
        df = df.withColumn("fecha_inicio_de_vigencia", F.to_date(F.col("fecha_inicio_de_vigencia"), "yyyy-MM-dd")) \
            .withColumn("fecha_fin_de_vigencia",   F.to_date(F.col("fecha_fin_de_vigencia"),   "yyyy-MM-dd"))

        # Regla de negocio nulos
        regla_nulos = F.when(
            (F.col("fecha_inicio_de_vigencia").isNotNull()) & (F.col("fecha_fin_de_vigencia").isNull()),
            F.lit("1")
        ).when(
            (F.col("fecha_inicio_de_vigencia").isNull()) & (F.col("fecha_fin_de_vigencia").isNull()),
            F.lit("1")
        )

        # Traslape estándar
        overlap_calc = F.when(
            (F.col("fecha_inicio_de_vigencia") <= F.col("periodo_fin_date")) &
            (F.col("fecha_fin_de_vigencia")   >= F.col("periodo_inicio_date")),
            F.lit("1")
        ).otherwise(F.lit("2"))

        # Priorizar la regla de nulos
        df = df.withColumn(
            "estado_cliente",
            F.when(regla_nulos.isNotNull(), regla_nulos).otherwise(overlap_calc)
        )

        # Limpieza
        df = df.drop("periodo_inicio_date", "periodo_fin_date")

        return df


    def _calculate_estado_poliza(self, df, periodo_inicio, periodo_fin, modo="overlap"):
        """
        Calcula y SOBRESCRIBE 'id_estado' (1=vigente, 2=vencida) para pólizas.
        Reglas de negocio:
        - Si 'fecha_inicio_de_vigencia' existe y 'fecha_fin_de_vigencia' NO existe → 1
        - Si NO existe ninguna de las dos → 1
        - Si existen ambas → aplicar traslape (overlap) o rangos (opcional)
        """

        # Construcción del periodo (YYYYMM → primer día y último día del mes)
        df = df.withColumn(
            "periodo_inicio_date",
            F.to_date(
                F.concat(
                    F.substring(F.lit(periodo_inicio), 1, 4),
                    F.lit("-"),
                    F.substring(F.lit(periodo_inicio), 5, 2),
                    F.lit("-01")
                ), "yyyy-MM-dd"
            )
        ).withColumn(
            "periodo_fin_date",
            F.last_day(
                F.to_date(
                    F.concat(
                        F.substring(F.lit(periodo_fin), 1, 4),
                        F.lit("-"),
                        F.substring(F.lit(periodo_fin), 5, 2),
                        F.lit("-01")
                    ), "yyyy-MM-dd"
                )
            )
        )

        # Cast defensivo si vinieran como string; si ya son date, no afecta
        df = df.withColumn("fecha_inicio_de_vigencia", F.to_date(F.col("fecha_inicio_de_vigencia"), "yyyy-MM-dd")) \
            .withColumn("fecha_fin_de_vigencia",   F.to_date(F.col("fecha_fin_de_vigencia"),   "yyyy-MM-dd"))

        # Regla de negocio para nulos:
        # 1) inicio NO nulo y fin nulo → 1
        # 2) inicio nulo y fin nulo → 1
        # 3) en otros casos (ambos presentes) → calcular
        regla_nulos = F.when(
            (F.col("fecha_inicio_de_vigencia").isNotNull()) & (F.col("fecha_fin_de_vigencia").isNull()),
            F.lit("1")
        ).when(
            (F.col("fecha_inicio_de_vigencia").isNull()) & (F.col("fecha_fin_de_vigencia").isNull()),
            F.lit("1")
        )

        # Cálculo de overlap (solo se aplica si no se disparó la regla de nulos)
        overlap_calc = F.when(
            (F.col("fecha_inicio_de_vigencia") <= F.col("periodo_fin_date")) &
            (F.col("fecha_fin_de_vigencia")   >= F.col("periodo_inicio_date")),
            F.lit("1")
        ).otherwise(F.lit("2"))

        if modo == "rangos":
            dias_hasta_vencimiento = F.datediff(F.col("fecha_fin_de_vigencia"), F.col("periodo_fin_date"))
            rangos_calc = (
                F.when(F.col("fecha_inicio_de_vigencia") > F.col("periodo_fin_date"), F.lit("NO_INICIA"))
                .when(dias_hasta_vencimiento < 0, F.lit("VENCIDA"))
                .when((dias_hasta_vencimiento >= 0) & (dias_hasta_vencimiento <= 30), F.lit("POR_VENCER_30"))
                .when((dias_hasta_vencimiento >= 31) & (dias_hasta_vencimiento <= 60), F.lit("POR_VENCER_60"))
                .otherwise(F.lit("VIGENTE"))
            )
            estado_poliza_calculada = rangos_calc
        else:
            estado_poliza_calculada = overlap_calc

        # Priorizar regla_nulos; si no aplica, usar estado_poliza_calculada
        df = df.withColumn(
            "id_estado",
            F.when(regla_nulos.isNotNull(), regla_nulos).otherwise(estado_poliza_calculada)
        )

        # Limpieza
        df = df.drop("periodo_inicio_date", "periodo_fin_date")

        return df

    def _calculate_periods(self):
        """
        Calcula periodo_inicio y periodo_fin respetando HISTORIC_PARAM.
        Este método es la ÚNICA fuente de verdad para los periodos.
        
        Returns:
            tuple: (periodo_inicio, periodo_fin, start_date, end_date)
                - periodo_inicio/fin: formato YYYYMM
                - start_date/end_date: formato DD/MM/YYYY HH:MM:SS (para staging)
        """
        try:
            args = getResolvedOptions(sys.argv, ['HISTORIC_PARAM'])
            historic_param = args['HISTORIC_PARAM']
        except:
            historic_param = '0'
            self.logger.warning("HISTORIC_PARAM no encontrado, usando modo mes caído por defecto")
        
        if historic_param == '1':
            # MODO HISTÓRICO: Usar START_DATE y END_DATE
            try:
                args = getResolvedOptions(sys.argv, ['START_DATE', 'END_DATE'])
                
                start_param = args['START_DATE']  # formato YYYYMM
                end_param = args['END_DATE']      # formato YYYYMM
                
                start_year = int(start_param[:4])
                start_month = int(start_param[4:6])
                
                end_year = int(end_param[:4])
                end_month = int(end_param[4:6])
                
                # Fechas para staging
                start_date = datetime(start_year, start_month, 1).strftime("%d/%m/%Y 00:00:00")
                last_day = calendar.monthrange(end_year, end_month)[1]
                end_date = datetime(end_year, end_month, last_day).strftime("%d/%m/%Y 23:59:59")
                
                # Periodos para silver
                periodo_inicio = start_param
                periodo_fin = end_param
                
                self.logger.info(f"[HISTORIC_PARAM=1] Procesando rango histórico: {start_param} a {end_param}")
                
            except Exception as e:
                self.logger.error(f"Error: HISTORIC_PARAM=1 requiere START_DATE y END_DATE. Error: {str(e)}")
                raise ValueError("START_DATE y END_DATE son requeridos cuando HISTORIC_PARAM=1")
        
        else:
            # MODO MES CAÍDO: Calcular mes anterior automáticamente
            hoy = datetime.now()
            
            # Primer día del mes actual
            primer_dia_mes_actual = hoy.replace(day=1)
            
            # Último día del mes anterior
            ultimo_dia_mes_anterior = primer_dia_mes_actual - timedelta(days=1)
            
            # Primer día del mes anterior
            primer_dia_mes_anterior = ultimo_dia_mes_anterior.replace(day=1)
            
            # Fechas para staging
            start_date = primer_dia_mes_anterior.strftime("%d/%m/%Y 00:00:00")
            end_date = ultimo_dia_mes_anterior.strftime("%d/%m/%Y 23:59:59")
            
            # Periodos para silver (inicio y fin son iguales en mes caído)
            periodo_mes_anterior = primer_dia_mes_anterior.strftime('%Y%m')
            periodo_inicio = periodo_mes_anterior
            periodo_fin = periodo_mes_anterior
            
            self.logger.info(f"[HISTORIC_PARAM=0] Procesando mes caído: {periodo_mes_anterior}")
            self.logger.info(f"Fecha actual: {hoy.strftime('%Y-%m-%d')}, procesando mes anterior completo")
        
        # Guardar en la instancia para uso posterior
        self.periodo_inicio = periodo_inicio
        self.periodo_fin = periodo_fin
        
        self.logger.info(f"✓ Periodos calculados: periodo_inicio={periodo_inicio}, periodo_fin={periodo_fin}")
        self.logger.info(f"✓ Fechas para staging: start_date={start_date}, end_date={end_date}")
        
        return periodo_inicio, periodo_fin, start_date, end_date

    def _ensure_period_bounds(self):
        """
        Garantiza que existan self.periodo_inicio y self.periodo_fin.
        Usa _calculate_periods() si no están definidos.
        """
        pi = getattr(self, 'periodo_inicio', None)
        pf = getattr(self, 'periodo_fin', None)
    
        if pi and pf:
            return pi, pf
        
        # Si no existen, calcularlos
        self.logger.info("[Periodos] No están definidos, calculando...")
        pi, pf, _, _ = self._calculate_periods()
        return pi, pf


    def _filter_df_by_period(self, df, periodo_inicio, periodo_fin):
        """
        Aplica filtro por periodo al df STG con estrategia en cascada:
          1) Si existen columnas 'periodo_inicio' y 'periodo_fin' (strings YYYYMM) -> Igualdad exacta
             Regla: (periodo_inicio_reg == periodo_inicio_job) AND (periodo_fin_reg == periodo_fin_job)
          2) Si existe columna 'periodo' (string YYYYMM) -> BETWEEN [pi, pf]
          3) Si no existen, derivar 'periodo' de fecha candidata (fecha_emision_poliza, fecha_inicio_vigencia, fecha)
             y aplicar BETWEEN [pi, pf].
          4) Si no es posible, devolver sin filtrar (WARN).
        """
        cols = set(df.columns)
    
        # 1) Igualdad exacta con periodo_inicio/periodo_fin presentes en STG
        if {'periodo_inicio', 'periodo_fin'}.issubset(cols):
            df = df.withColumn("periodo_inicio", F.lpad(F.col("periodo_inicio").cast("string"), 6, "0")) \
                   .withColumn("periodo_fin",    F.lpad(F.col("periodo_fin").cast("string"),    6, "0"))
            return df.filter(
                (F.col("periodo_inicio") == F.lit(periodo_inicio)) &
                (F.col("periodo_fin")    == F.lit(periodo_fin))
            )
    
        # 2) Columna periodo única
        if 'periodo' in cols:
            df = df.withColumn("periodo", F.lpad(F.col("periodo").cast("string"), 6, "0"))
            return df.filter(F.col("periodo").between(F.lit(periodo_inicio), F.lit(periodo_fin)))
    
        # 3) Derivar columna periodo desde una fecha
        candidate_dates = [c for c in ["fecha_emision_poliza", "fecha_inicio_vigencia", "fecha"] if c in cols]
        if candidate_dates:
            dcol = candidate_dates[0]
            df = df.withColumn(dcol, F.to_date(F.col(dcol))) \
                   .withColumn("periodo", F.date_format(F.col(dcol), "yyyyMM"))
            return df.filter(F.col("periodo").between(F.lit(periodo_inicio), F.lit(periodo_fin)))
    
        # 4) No se pudo filtrar
        self.logger.warning("[Periodos] No se encontraron columnas periodo/fechas para filtrar STG; se procesará todo el dataset.")
        return df

    def apply_special_treatment(self, df, name):
        """
        Aplica tratamiento especial según la dimensión.
    
        Args:
            df: DataFrame de la dimensión
            name: Nombre de la dimensión (ej: "POLIZAS_datos_basicos_stg")
        """
        try:
            self.logger.info(f"Applying special treatment for dimension: {name}")
    
            if name.lower() == 'stg':
                self.logger.info(f"Aplicando split_name a {name}")
                df = self.silver_transformer.split_name(df)
                df = self.silver_transformer.populate_full_name(df)
                return df
    
            elif name.lower() == 'stg':
                self.logger.info(f"Aplicando assign_colombia_if_missing a {name}")
                df = self.silver_transformer.assign_colombia_if_missing(df)
                return df
    
            else:
                self.logger.info(f"No special treatment defined for {name}")
                return df
    
        except Exception as e:
            self.logger.error(f"Error applying special treatment for {name}: {str(e)}")
            raise

    def load_dimension(self, dim_name):
        """Load dimension data with caching from the new Silver path."""
        if dim_name in self.dimensions_cache:
            self.logger.info(f"Using cached dimension: {dim_name}")
            return self.dimensions_cache[dim_name]
       
        try:
            dim_key = dim_name.replace('.xlsx', '').replace('dim_', '').upper()
           
            silver_path = f's3://{self.bucket_silver}/GENERAL/TRANSFORMATION/GENERAL_SILVER_{dim_key}_POLIZAS/'
           
            try:
                df = self.utils.read_full_delta(self.spark, silver_path)
                self.logger.info(f"Loaded dimension {dim_name} from Silver GENERAL path: {silver_path}")
            except:
                # Fallback to Bronze
                bronze_dim_key = dim_name.replace('.xlsx', '').replace('.', '_')
                bronze_path = f's3://{self.bucket_bronze}/FILES/FILES/FILES_FILES_{bronze_dim_key}/'
                df = self.utils.read_full_delta(self.spark, bronze_path)
                df = self.silver_transformer.clean_dims(df)
                self.logger.info(f"Loaded dimension {dim_name} from Bronze layer: {bronze_path}")
           
            # Cache the dimension
            self.dimensions_cache[dim_name] = df
            return df
           
        except Exception as e:
            self.logger.error(f"Error loading dimension {dim_name}: {str(e)}")
            raise

    
    def apply_dimension_mapping(self, df, mapping_config):
        """
        Apply dimension mapping using map_cod_by_nombre function.
     
        Args:
            df: Source DataFrame
            mapping_config: Dictionary with mapping configuration
        """
        try:
            dim_name = mapping_config['dimension']
            self.logger.info(f"Applying dimension mapping with {dim_name}")
           
            if mapping_config.get('columnas_adicionales_cruce'):
                self.logger.info(f"Using additional join columns: {mapping_config['columnas_adicionales_cruce']}")
         
            df_dimension = self.load_dimension(dim_name)
            df_dimension = df_dimension.filter(F.col("estado") == 'A')
         
            df_mapped = self.silver_transformer.map_cod_by_nombre(
                df_origen=df,
                df_referencia=df_dimension,
                nombre_origen=mapping_config['nombre_origen'],
                nombre_referencia=mapping_config['nombre_referencia'],
                cod_referencia=mapping_config['cod_referencia'],
                cod_origen=mapping_config.get('cod_origen'),
                nuevo_cod=mapping_config.get('nuevo_cod', 'id'),
                valor_defecto=mapping_config.get('valor_defecto'),
                columnas_adicionales_cruce=mapping_config.get('columnas_adicionales_cruce')
            )
         
            self.logger.info(f"Successfully applied dimension mapping for {dim_name}")
            return df_mapped
         
        except Exception as e:
            self.logger.error(f"Error applying dimension mapping: {str(e)}")
            raise
     

    def process_staging_data(self, job_id, bucket_bronze, config):
        '''Process staging data from all sources.'''
        try:
            processors = {
                'acsele': AcseleProductoPolizasStagingProcessor(
                    self.spark,
                    self.glue_context,
                    bucket_bronze,
                    self.bucket_silver,
                    config
                )
            }
            
            # ============================================================
            # CALCULAR PERIODOS USANDO EL MÉTODO CENTRALIZADO
            # ============================================================
            periodo_inicio, periodo_fin, start_date, end_date = self._calculate_periods()
            
            print("=" * 80)
            print("PROCESS_STAGING_DATA - PERIODOS CALCULADOS:")
            print(f"  START_DATE: {start_date}")
            print(f"  END_DATE: {end_date}")
            print(f"  PERIODO_INICIO: {periodo_inicio}")
            print(f"  PERIODO_FIN: {periodo_fin}")
            print("=" * 80)
            
            results = {}
            for name, processor in processors.items():
                self.logger.info(f"Processing {name} data...")
                if name == 'acsele':
                    results[name], df_polizas = processor.polizas_workflow_stg(start_date, end_date, job_id)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in staging processing: {str(e)}")
            raise
        

    def process_dimensions(self, bucket_bronze, bucket_silver, active_dims):
        """Process active dimensions dynamically and save to GENERAL/TRANSFORMATION path."""
        if not active_dims:
            self.logger.info("No active dimensions to process")
            return

        try:
            base_path = f's3://{bucket_bronze}/FILES/FILES/'
           
            for dim_name in active_dims:
                dim_key_bronze = dim_name.replace('.xlsx', '').replace('.', '_')
                dim_key_silver = dim_name.replace('.xlsx', '').replace('dim_', '').upper()
               
                dim_path = f"{base_path}FILES_FILES_{dim_key_bronze}/"
                s3_path_silver = f's3://{self.bucket_silver}/GENERAL/TRANSFORMATION/{dim_name}/'
                self.logger.info(f"Processing dimension: {dim_name} from {dim_path}")
               
                try:
                    df = self.utils.read_full_delta(self.spark, dim_path)
                    df_clean = self.silver_transformer.clean_dims(df)
                    df_clean = self.dimension_handler.process_dimension_with_scd(df_clean, dim_name)
                    
                    self.logger.info(f"Successfully processed {dim_name}")
                except Exception as e:
                    self.logger.warning(f"Could not process {dim_name}: {str(e)}")
                   
        except Exception as e:
            self.logger.error(f"Error processing dimensions: {str(e)}")
            raise

    def process_facts(self, config, active_facts, df_homologaciones, job_id):
        """
        Process active facts using configuration from JSON with dimension mapping support.
        Optimized: reads S3 once per source and applies multiple outputs.
        """
        if not active_facts:
            self.logger.info("No active facts to process")
            return

        try:
            # ============================================================
            # CALCULAR PERIODOS AL INICIO (respetando HISTORIC_PARAM)
            # ============================================================
            periodo_inicio, periodo_fin, _, _ = self._calculate_periods()

            yyyy = periodo_inicio[:4]
            mm = periodo_inicio[4:6]
            print(f"PARTICION A LEER {yyyy} / {mm}")
            print("=" * 80)
            print("PROCESS_FACTS - PERIODOS PARA SILVER:")
            print(f"  PERIODO_INICIO: {periodo_inicio}")
            print(f"  PERIODO_FIN: {periodo_fin}")
            print("=" * 80)
            # ============================================================
            
            base_path = f's3://{self.bucket_silver}/'
            fact_configs = config.get('fact_configs', {})
           
            for fact_name in active_facts:
                if fact_name not in fact_configs:
                    self.logger.warning(f"Unknown fact: {fact_name}. Available configs: {list(fact_configs.keys())}")
                    continue
                   
                fact_config = fact_configs[fact_name]
                self.logger.info(f"Processing fact source: {fact_name}")
               
                try:
                    # Lectura única de S3 para esta fuente
                    full_path_silver = f"{base_path}{fact_config['path']}"
                    partitioned_path = f"{full_path_silver}yyyy={yyyy}/mm={mm}"
                    df_source = self.utils.read_full_parquet(self.spark,partitioned_path)
                    self.logger.info(f"Reading data from: {partitioned_path}")
                    df_source = self.silver_transformer.date_normalice(df_source)
                    
                    #df_source = self._filter_df_by_period(df_source, periodo_inicio, periodo_fin)
                    # ================================================================

                    
                    # Persistir el DataFrame base para reutilizarlo en múltiples outputs
                    df_source.persist()
                    source_count = df_source.count()
                    self.logger.info(f"Loaded {source_count} records from {fact_name}")
                    
                    # Procesar cada output definido para esta fuente
                    for output_config in fact_config.get('outputs', []):
                        output_name = output_config['name']
                        self.logger.info(f"Processing output: {output_name}")
                        
                        try:
                            # Aplicar clean_method si existe
                            clean_method = output_config.get('clean_method')
                            if clean_method:
                                df_clean = getattr(self.silver_transformer, clean_method)(df_source)
                            else:
                                df_clean = df_source

                            # Aplicar select_method si existe
                            select_method = output_config.get('select_method')
                            if select_method:
                                df_clean = getattr(self.silver_transformer, select_method)(df_clean)
                            
                            self.logger.info(f"Harmonization table: {output_config['harmonization_table']}")
                            df_clean.persist()  # Optimización: count() removido, evaluación ocurre en primer uso
                    
                            # Aplicar homologación
                            df_harmonized = self.harmonization_manager.homologacion_i(
                                output_config['harmonization_table'],
                                df_homologaciones,
                                df_clean,
                                "homologacion",
                                "cod_fuente_fk"
                            )
                            
                            df_harmonized.persist()  # Optimización: count() removido, evaluación ocurre en primer uso
                            
                            # Aplicar transformaciones adicionales
                            for transform in output_config.get('additional_transforms', []):
                                try:
                                    self.logger.info(f"Processing transform: {type(transform)} - {transform}")
                                   
                                    if isinstance(transform, str):
                                        try:
                                            parsed_transform = json.loads(transform)
                                            self.logger.info(f"Parsed JSON transform: {parsed_transform}")
                                         
                                            if isinstance(parsed_transform, dict) and parsed_transform.get('type') == 'dimension_mapping':
                                                self.logger.info(f"Applying dimension mapping for {output_name}")
                                                df_harmonized = self.apply_dimension_mapping(df_harmonized, parsed_transform['config'])
                                            else:
                                                self.logger.warning(f"Unknown parsed transformation type: {parsed_transform.get('type')}")
                                               
                                        except json.JSONDecodeError:
                                            self.logger.info(f"Applying transformation method: {transform}")
                                            df_harmonized = getattr(self.silver_transformer, transform)(df_harmonized)
                                           
                                    elif isinstance(transform, dict):
                                        if transform.get('type') == 'dimension_mapping':
                                            self.logger.info(f"Applying dimension mapping for {output_name}")
                                            df_harmonized = self.apply_dimension_mapping(df_harmonized, transform['config'])
                                        else:
                                            self.logger.warning(f"Unknown transformation type: {transform.get('type')}")
                                    else:
                                        self.logger.error(f"Invalid transformation format - Type: {type(transform)}, Value: {transform}")
                                       
                                except AttributeError as attr_error:
                                    self.logger.error(f"Method {transform} not found in DataCleanupProcessor: {str(attr_error)}")
                                    raise
                                except Exception as transform_error:
                                    self.logger.error(f"Error applying transformation {transform}: {str(transform_error)}")
                                    raise
                            
                            # Aplicar tratamiento especial
                            df_harmonized = self.apply_special_treatment(df_harmonized, output_config['harmonization_table'])
                            
                            # ============================================================
                            # NUEVO: Agregar columnas periodo_inicio y periodo_fin
                            # ============================================================
                            df_harmonized = df_harmonized \
                                .withColumn("periodo_inicio", F.lit(periodo_inicio)) \
                                .withColumn("periodo_fin", F.lit(periodo_fin))
                            
                            self.logger.info(f"✓ Agregadas columnas periodo_inicio={periodo_inicio}, periodo_fin={periodo_fin} a {output_name}")
                            # ============================================================
                            # ============================================================
                            # NUEVO: Calcular estado_cliente para CLIENTES (solo ACSELE)
                            # ============================================================
                            if output_name in ("CLIENTES_ACSELE_SILVER", "POLIZA_ACSELE_SILVER"):
                                self.logger.info("Calculando estado_cliente para clientes ACSELE...")
                                df_harmonized = self._calculate_client_status(df_harmonized, periodo_inicio, periodo_fin)
                                df_harmonized = self._calculate_estado_poliza(df_harmonized, periodo_inicio, periodo_fin)
                            # ============================================================
                     
                            # Guardar en Silver
                            self._save_to_silver(df_harmonized, output_name, "ACSELE", "POLIZAS_PRODUCTO",yyyy,mm)
         
                
                            self.logger.info(f"Successfully processed output: {output_name}")
                           
                            df_clean.unpersist(blocking=False)
                            df_harmonized.unpersist(blocking=False)
                            
                        except Exception as output_error:
                            self.logger.error(f"Error processing output {output_name}: {str(output_error)}")
                            raise
                    
                    # Liberar el DataFrame fuente después de procesar todos los outputs
                    df_source.unpersist(blocking=False)
                    self.logger.info(f"Successfully processed all outputs for {fact_name}")
                   
                except Exception as e:
                    self.logger.warning(f"Could not process {fact_name}: {str(e)}")
                    raise
                   
        except Exception as e:
            self.logger.error(f"Error processing facts: {str(e)}")
            raise


    def _save_to_silver(self, df, insert_name, db_name, schema,yyyy,mm):
        s3_prefix = f"{schema}/{db_name}/{insert_name}/"
        s3_target_path = f"s3://{self.bucket_silver}/{s3_prefix}"
        self.logger.info(f"Saving to: {s3_target_path}")
    
        df_partitioned = df.withColumn("yyyy", F.lit(yyyy)) \
                           .withColumn("mm", F.lit(mm)) 

        df_partitioned.write \
            .format("parquet") \
            .mode("overwrite") \
            .partitionBy("yyyy", "mm") \
            .save(s3_target_path)

        #df.write.format("delta").mode("overwrite").save(s3_target_path)
        self.logger.info(f"Data saved in parquet format at {s3_target_path}")    

    def process_origin_tables(self):
        """Generate origin tables."""
        try:
            source_data_processor = SourceDataGenerator(self.spark, self.glue_context, self.bucket_silver)
            source_data_processor.create_and_save_fuentes()
            self.logger.info("Origin tables generated successfully")
        except Exception as e:
            self.logger.error(f"Error processing origin tables: {str(e)}")
            raise

    def set_bucket_bronze(self, bucket_bronze):
        """Set bucket bronze for dimension loading."""
        self.bucket_bronze = bucket_bronze

    def extract_excel_data(self, path, sheet): 
        """
        Function to extract data from excel files with better NaN handling
        """
        excel_file_path = path
        df_xl_op = pd.read_excel(excel_file_path, sheet_name=sheet)
        df_xl_op = df_xl_op.where(pd.notnull(df_xl_op), None) 
        for col in df_xl_op.columns:
            df_xl_op[col] = df_xl_op[col].apply(lambda x: str(x) if x is not None else None)
        df = self.spark.createDataFrame(df_xl_op)
        return df


    def homologation_data(self, df_homologaciones):
        """Load homologation data."""
        try:
            df_cod_tipo = df_homologaciones.filter(col("campo_referencia") == "cod_tipo_identificacion")
            df_otros = df_homologaciones.filter(col("campo_referencia") != "cod_tipo_identificacion")
            df_cod_tipo = self.cleanup.normalize_columns(df_cod_tipo, "valor_referencia")
            return df_cod_tipo.unionByName(df_otros)
        except Exception as e:
            self.logger.error(f"Error loading homologation data: {str(e)}")
            raise        
        

def main():
    """Main function with enhanced dimension mapping support."""
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'BUCKET_SILVER', 'BUCKET_BRONZE', 'JOB_PROCESS_ID',
        'S3_HOMOLOGATION_PATH', 'S3_STATUS_CONFIG_PATH',
        'LOAD_STG_SILVER', 'LOAD_ORIGIN_TABLE','HISTORIC_PARAM', 'HOMOLOGATION_SHEET'
    ])

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
  
    start_time = datetime.now()
    print(f"Job {args['JOB_NAME']} start at {start_time.strftime('%Y-%m-%d %H:%M:%S')} with PID: {args['JOB_PROCESS_ID']}")
    try:
        processor = SilverLayerProcessor(spark, glue_context, args['JOB_NAME'], args['BUCKET_SILVER'])
        processor.set_bucket_bronze(args['BUCKET_BRONZE'])
        
        processor.logger.info(f"Starting Enhanced Silver Layer job: {args['JOB_NAME']}")

        config = processor.load_config(args['S3_STATUS_CONFIG_PATH'])
        active_dims = processor.get_active_items(config, 'dim')
        active_facts = processor.get_active_items(config, 'fact')
        active_facts = [fact for fact in active_facts if fact in ("ACSELE_MVP2", "ACSELE_CLIENTES_MVP2")]
        df_homologaciones = None
        if active_facts:
            path = args['S3_HOMOLOGATION_PATH']
            sheet = args['HOMOLOGATION_SHEET']
            df_homologaciones_dos = processor.extract_excel_data(path, sheet)
            df_homologaciones = df_homologaciones_dos.selectExpr(
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
                """CASE         WHEN cod_fuente_fk IS NULL THEN NULL     
                WHEN LOWER(TRIM(cod_fuente_fk)) = 'nan' THEN NULL    
                WHEN cod_fuente_fk LIKE '%.0' THEN REGEXP_REPLACE(cod_fuente_fk, '\\.0$', '')       
                ELSE cod_fuente_fk     END as cod_fuente_fk
                """
            )
            df_homologaciones = processor.homologation_data(df_homologaciones)
            # Optimización: show() removido (era costoso y retornaba None)print(f" Poliza {df_filter} Producto {df_filterf}")
        if args.get('LOAD_ORIGIN_TABLE', 'true').lower() == 'true':
            processor.process_origin_tables()

        if args.get('LOAD_STG_SILVER', 'true').lower() == 'true':
            processor.process_staging_data(args['JOB_PROCESS_ID'], args['BUCKET_BRONZE'], config)

        # Procesar facts con la nueva estructura optimizada
        processor.process_facts(config, active_facts, df_homologaciones, args['JOB_PROCESS_ID'])
        
        processor.logger.info("Job completed successfully!")
        processor.logger.info(f"Processed dims: {active_dims}, facts: {active_facts}")

    except Exception as e:
        print(f"Error in Enhanced Silver Layer job: {str(e)}")
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()