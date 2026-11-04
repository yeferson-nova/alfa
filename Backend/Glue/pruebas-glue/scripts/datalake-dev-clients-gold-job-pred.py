
#!/usr/bin/env python3
"""
AWS Glue Job: Gold Layer Processing
Description: ETL processing from Silver to Gold layer with selective column loading
Author: Data Engineering Team
Date: 2025
"""
import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, current_timestamp
from fact_handler_gold import SCDFactHandler
from transformer_proccessor_gold import TransformerGold
from remediation_impact_analyzer import RemediationImpactAnalyzer
from transformer_proccessor import DataCleanupProcessor
from gold_fact_remediation_processor import GoldFactRemediationProcessor

from utils import Utils
from s3_handler import S3Handler


class GoldLayerProcessor:
    """Gold layer processor with selective column loading and dimension enrichment."""

    def __init__(self, spark, glue_context, job_name, bucket_gold, bucket_silver):
        self.spark = spark
        self.glue_context = glue_context
        self.job_name = job_name
        self.bucket_gold = bucket_gold
        self.bucket_silver = bucket_silver
        self.logger = self._setup_logging()
        self.utils = Utils()
        self.gold = TransformerGold()
        self.transformer_proccessor = DataCleanupProcessor(spark,glue_context)
        self.s3_handler = S3Handler(bucket_gold, glue_context)
        self.fact_handler = SCDFactHandler(spark,glue_context,bucket_gold)
        # Cache for dimensions
        self.dimensions_cache = {}
        self.utils.configure_spark_optimizations(spark, "GOLD", "datalake-dev-clients-gold-job")
        self.remediation_processor = GoldFactRemediationProcessor(
            spark, glue_context, job_name, bucket_gold, self.logger, self.utils)
        self.impact_analyzer = RemediationImpactAnalyzer(
            spark, glue_context, bucket_gold, self.logger, self.utils
            
        )


    def _setup_logging(self):
        """Configure logging."""
        import logging
        logger = logging.getLogger(f"GoldLayer-{self.job_name}")
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def load_config(self, s3_config_path):
        """Load Gold configuration from S3."""
        try:
            self.logger.info(f"Loading Gold configuration from: {s3_config_path}")
           
            df = self.spark.read.option("multiline", "true").json(s3_config_path)
            config_row = df.collect()[0]
            config = config_row.asDict()
           
            # Convert Row objects to dictionaries
            config = self._convert_spark_row_to_dict(config)
           
            self.logger.info("Gold configuration loaded successfully")
            return config
           
        except Exception as e:
            self.logger.error(f"Error loading Gold config: {str(e)}")
            raise

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

    def get_active_items(self, config, item_type):
        """Get items with status = 1."""
        active_items = [item['name'] for item in config.get(item_type, []) if item.get('status') == 1]
        self.logger.info(f"Active {item_type}: {active_items}")
        return active_items

    def load_silver_data(self, table_path):
        """Load data from Silver layer."""
        try:
            full_path = f's3://{self.bucket_silver}/{table_path}'
            self.logger.info(f"Loading Silver data from: {full_path}")
           
            df = self.utils.read_full_parquet(self.spark, full_path)
            count = df.count()
            self.logger.info(f"Loaded {count:,} records from Silver")
           
            return df
           
        except Exception as e:
            self.logger.error(f"Error loading Silver data from {table_path}: {str(e)}")
            raise

    def load_dimension(self, dim_config):
        """Load dimension data from Silver layer."""
        dim_name = dim_config['name']
       
        if dim_name in self.dimensions_cache:
            self.logger.info(f"Using cached dimension: {dim_name}")
            return self.dimensions_cache[dim_name]
       
        try:
            dim_key = dim_name.replace('.xlsx', '').replace('dim_', '').upper()
            silver_path = f's3://{self.bucket_silver}/GENERAL/TRANSFORMATION/GENERAL_SILVER_{dim_key}_CLIENTES/'
           
            self.logger.info(f"Loading dimension {dim_name} from: {silver_path}")
            df = self.utils.read_full_parquet(self.spark, silver_path)
           
            if 'columns' in dim_config and dim_config['columns']:
                selected_columns = dim_config['columns']
                self.logger.info(f"Selecting columns for {dim_name}: {selected_columns}")
                df = df.select(*selected_columns)
           
            df.cache()
            self.dimensions_cache[dim_name] = df
           
            dim_count = df.count()
            self.logger.info(f"Loaded dimension {dim_name}: {dim_count:,} records")
           
            return df
           
        except Exception as e:
            self.logger.error(f"Error loading dimension {dim_name}: {str(e)}")
            raise

    def apply_column_selection(self, df, column_config, missing_allowed):
        """Apply column selection based on configuration."""
        try:
            if not column_config:
                self.logger.info("No column selection specified, using all columns")
                return df
           
            selection_type = column_config.get('type', 'select')
            columns = column_config.get('columns', [])
           
            if selection_type == 'select' and columns:
                self.logger.info(f"Selecting specific columns: {columns}")
                available_columns = df.columns
                missing_columns = [col for col in columns if col not in available_columns]
                if missing_columns:
                    self.logger.warning(f"Missing columns will be skipped: {missing_columns}")
                    columns = [col for col in columns if col in available_columns]
               
                df_selected = df.select(*columns)
                if missing_allowed:
                   for column in missing_columns:
                       print(f"creating empty column: {column}")
                       df_selected = df_selected.withColumn(column, lit(""))   
            elif selection_type == 'exclude' and columns:
                self.logger.info(f"Excluding columns: {columns}")
                remaining_columns = [col for col in df.columns if col not in columns]
                df_selected = df.select(*remaining_columns)
               
            else:
                self.logger.info("Invalid column selection config, using all columns")
                df_selected = df
           
            selected_count = len(df_selected.columns)
            self.logger.info(f"Column selection result: {selected_count} columns")
           
            return df_selected
           
        except Exception as e:
            self.logger.error(f"Error applying column selection: {str(e)}")
            raise

    def process_global_dimensions(self, config):
        """Process global dimensions based on status flag."""
        try:
            dimensions = config.get('dimensions', [])
            active_dimensions = [dim for dim in dimensions if dim.get('status') == 1]
           
            if not active_dimensions:
                self.logger.info("No active dimensions to process")
                return
           
            self.logger.info(f"Processing {len(active_dimensions)} global dimensions")
           
            for dim_config in active_dimensions:
                dim_name = dim_config['name']
                self.logger.info(f"Loading global dimension: {dim_name}")
               
                # Load dimension with specified columns
                df_dimension = self.load_dimension(dim_config)
               
                # Save each dimension to its own Gold path
                self._save_dimension_to_gold(df_dimension, dim_name)
               
                dim_count = df_dimension.count()
                self.logger.info(f"Successfully processed dimension {dim_name}: {dim_count:,} records")
           
        except Exception as e:
            self.logger.error(f"Error processing global dimensions: {str(e)}")
            raise

    def _save_dimension_to_gold(self, df, dim_name):
        """Save dimension to Gold layer."""
        try:
            dim_key = dim_name.replace('.xlsx', '').replace('dim_', '').upper()
            table_name = f"DIM_{dim_key}_GOLD"
           
            df_with_metadata = self.add_gold_metadata(df)
           
            gold_path = f's3://{self.bucket_gold}/GOLD/CLIENTES_GOLD/GOLD-CARACT_CLIENTES-DIM_{table_name.upper()}/'
            final_count = df_with_metadata.count()
            self.logger.info(f"Saving dimension {dim_name}: {final_count:,} records to {gold_path}")
           
            dynamic_frame = DynamicFrame.fromDF(df_with_metadata, self.glue_context, f"{table_name}_gold")
            
            self.s3_handler.delete_bucket_files(f"GOLD/CLIENTES_GOLD/GOLD-CARACT_CLIENTES-DIM_{table_name.upper()}/")
            
            self.s3_handler.save_files_to_s3(
                df=dynamic_frame,
                s3_target_path=gold_path,
                format="parquet",
                mode="overwrite"
            )
           
            self.logger.info(f"Successfully saved dimension {dim_name} to Gold")
           
        except Exception as e:
            self.logger.error(f"Error saving dimension {dim_name} to Gold: {str(e)}")
            raise

    def add_gold_metadata(self, df):
        """Add basic Gold layer metadata."""
        try:
            self.logger.info("Adding Gold layer metadata")
           
            df_with_metadata = df.withColumn("fecha_procesamiento_gold", current_timestamp())
           
            return df_with_metadata
           
        except Exception as e:
            self.logger.error(f"Error adding Gold metadata: {str(e)}")
            raise
    
    def _create_servialfa (self,config):
        
        try:
        
            from pyspark.sql.functions import monotonically_increasing_id,concat_ws, col
            base_path = f's3://{self.bucket_silver}'
            
            contact_path = f"{base_path}/{config.get('silver_servialfa_path', {})}"
            
            
            df_servialfa = self.utils.read_full_parquet(self.spark, contact_path)
            
            
            self.logger.info(f"servialfa schema ")
            
            df_servialfa.printSchema()
            

            df_servialfa = df_servialfa.withColumn("PK", concat_ws("-", col("cod_tipo_identificacion"), col("Numero_Documento")))
            
            dims = config.get('dims', {})
            
            for dim in dims:
                self.logger.info(f"crossing dimension ID for {dim}")
                dimen = dims[dim]
                dim_path = dimen['dim_path']
                self.logger.info(f"dimention path: {dim_path}}}")
                df1 = self.utils.read_full_parquet(self.spark, dim_path)
                df1.printSchema()
                df_servialfa = self.gold.cross_code_with_id(df_servialfa,df1 ,dimen['column_fact'], dimen['column_dim'], dimen['column_id'], dimen['column_fact_id'])
            
            column_selection = config.get('column_selection',{})
            
            df_servialfa =  df_servialfa.withColumn("id", monotonically_increasing_id())
            
            df_servialfa = self.gold.apply_column_selection(df_servialfa,column_selection, "true")

            #df_servialfa = self.transformer_proccessor.to_date(df_servialfa)
            
            #df_servialfa = self.gold.remediate_dataframe(df_servialfa)
            
            self.fact_handler.process_dimension_with_scd(df_servialfa,"SERVIALFA")
 
            
        except Exception as e:
            self.logger.error(f"Error creating servialfa table: {str(e)}")
            raise


    
    def _create_fact_client (self,config):
        
        from pyspark.sql.functions import split
        from pyspark.sql.functions import monotonically_increasing_id
        try:
            base_path = f's3://{self.bucket_silver}'
            
            basic_path = f"{base_path}/{config.get('silver_basic_data_path', {})}"
            
            socio_path = f"{base_path}/{config.get('silver_socio_data_path', {})}"
            df_basic = self.utils.read_full_parquet(self.spark, basic_path)
            
            df_socio = self.utils.read_full_parquet(self.spark, socio_path)
            
            df_client = df_basic.join(df_socio.drop("created_at"),on="pk",how="left")
            
            dims = config.get('dims', {})
            
            for dim in dims:
                self.logger.info(f"crossing dimension ID for {dim}")
                dimen = dims[dim]
                dim_path = dimen['dim_path']
                self.logger.info(f"dimention path: {dim_path}}}")
                df1 = self.utils.read_full_parquet(self.spark, dim_path)
                df_client = self.gold.cross_code_with_id(df_client, df1,dimen['column_fact'], dimen['column_dim'], dimen['column_id'], dimen['column_fact_id'])
            
            df_client.printSchema()
            column_selection = config.get('column_selection',{})
            
            df_client = df_client.withColumn("Id", monotonically_increasing_id())
            
            df_client = df_client.withColumnRenamed("PK","PK_CLIENTE")
            
            df_client = self.gold.populate_company_name(df_client)
            
            df_client = self.gold.apply_column_selection(df_client,column_selection, "true")
            
            df_client = self.transformer_proccessor.to_date(df_client)
            
            df_client = self.gold.remediate_dataframe(df_client)
            
            self.fact_handler.process_dimension_with_scd(df_client,"CLIENTE")
            
 
            
        except Exception as e:
            self.logger.error(f"Error creating client fact: {str(e)}")
            raise
        
    def _create_fact_contact_location (self,config):
        
        try:
        
            from pyspark.sql.functions import monotonically_increasing_id
            base_path = f's3://{self.bucket_silver}'
            
            contact_path = f"{base_path}/{config.get('silver_location_contact_path', {})}"
            
            
            df_contact = self.utils.read_full_parquet(self.spark, contact_path)
            
            df_contact.printSchema()
            
            dims = config.get('dims', {})
            
            for dim in dims:
                self.logger.info(f"crossing dimension ID for {dim}")
                dimen = dims[dim]
                dim_path = dimen['dim_path']
                self.logger.info(f"dimention path: {dim_path}}}")
                df1 = self.utils.read_full_parquet(self.spark, dim_path)
                df1.printSchema()
                df_contact = self.gold.cross_code_with_id(df_contact,df1 ,dimen['column_fact'], dimen['column_dim'], dimen['column_id'], dimen['column_fact_id'])
            
            column_selection = config.get('column_selection',{})
            
            df_contact =  df_contact.withColumn("Id", monotonically_increasing_id())
            
            df_contact = self.gold.apply_column_selection(df_contact,column_selection, "true")
            
            df_contact = self.transformer_proccessor.to_date(df_contact)
            
            df_contact = self.gold.remediate_dataframe(df_contact)
            
            self.fact_handler.process_dimension_with_scd(df_contact,"UBICACION_CONTACTABILIDAD")


            
        except Exception as e:
            self.logger.error(f"Error creating contact fact: {str(e)}")
            raise    

    def cleanup_resources(self):
        """Clean up cached resources."""
        self.logger.info("Cleaning up Gold layer resources...")
       
        for dim_name, df in self.dimensions_cache.items():
            try:
                df.unpersist()
            except:
                pass
        self.dimensions_cache.clear()
       
        self.spark.catalog.clearCache()
        self.logger.info("Resource cleanup completed")

    def execute_fact_remediation(self, config):
        """
        Execute fact table remediation using SERVIALFA.
        """
        try:
            remediation_config = config.get('remediation_config', {})
            
            if not remediation_config or not remediation_config.get('enabled', 0):
                self.logger.info("Remediation not enabled - skipping")
                return
                
            self.logger.info("Starting fact table remediation")
            
            self.remediation_processor.validate_remediation_config(remediation_config)
            
            priority_source = remediation_config['priority_source']
            fact_tables_remediation = remediation_config['fact_tables']
            
   
            self.logger.info(f"Loading priority source: {priority_source['name']}")
            df_priority_source = self.remediation_processor.load_priority_source(
                priority_source_path=priority_source['path'],
                source_name=priority_source['name'],
                priority_level=priority_source.get('priority_level', 1)
            )
            

            for fact_name, fact_config in fact_tables_remediation.items():
                if not fact_config.get('enabled', 1):
                    self.logger.info(f"Skipping {fact_name} - disabled")
                    continue
                    
                self.logger.info(f"Processing remediation: {fact_name}")
                
                try:
            
                    df_fact_current = self.remediation_processor.load_gold_fact_table(
                        fact_table_path=fact_config['gold_path'],
                        fact_name=fact_name
                    )
                    
                  
                    column_mappings = fact_config.get('servialfa_source_columns', {})
                    
                   
                    df_fact_remediated = self.remediation_processor.remediate_fact_table(
                        df_fact_current=df_fact_current,
                        df_priority_source=df_priority_source,
                        key_columns=fact_config['key_columns'],
                        remediation_columns=fact_config['remediation_columns'],
                        fact_name=fact_name,
                        column_mappings=column_mappings
                    )
                    
                    self.logger.info(f"Caching remediated {fact_name} in memory")
                    df_fact_remediated.cache()
                    record_count = df_fact_remediated.count()
                    self.logger.info(f"Cached {record_count} records for {fact_name}")
                   
                    self.logger.info(f"Saving remediated {fact_name} to {fact_config['gold_path']}")
                   
                    df_fact_remediated.write \
                        .mode("overwrite") \
                        .option("compression", "snappy") \
                        .parquet(fact_config['gold_path'])
                   
                    self.logger.info(f"{fact_name} remediated and saved successfully")
                   
                    df_fact_current.unpersist()
                    df_fact_remediated.unpersist()
                   
                except Exception as e:
                    self.logger.error(f"Error in remediation of {fact_name}: {str(e)}")
                    try:
                        df_fact_current.unpersist()
                        df_fact_remediated.unpersist()
                    except:
                        pass
                    continue
           
            # Clean priority source
            df_priority_source.unpersist()
            self.logger.info("Remediation process completed")
           
        except Exception as e:
            self.logger.error(f"Error in remediation: {str(e)}")


    def analyze_remediation_impact(self, config, save_to_s3=True):
        """
        Analiza el impacto de la remediación sin ejecutarla.
       
        Args:
            config (dict): Configuración completa del Gold layer
            save_to_s3 (bool): Si guardar el informe en S3
           
        Returns:
            dict: Informe de impacto en formato JSON
        """
        try:
            self.logger.info("=== Iniciando análisis de impacto de remediación ===")
           
            remediation_config = config.get('remediation_config', {})
           
            # Generar informe
            impact_report = self.impact_analyzer.generate_report(remediation_config)
           
            # Log resumen
            if impact_report.get("status") == "completed":
                summary = impact_report.get("summary", {})
                self.logger.info(f"Análisis completado:")
                self.logger.info(f"  - Facts analizadas: {summary.get('total_facts_analyzed', 0)}")
                self.logger.info(f"  - Registros totales: {summary.get('total_records_analyzed', 0):,}")
                self.logger.info(f"  - Registros a remediar: {summary.get('total_records_to_remediate', 0):,}")
                self.logger.info(f"  - Mejora general: {summary.get('overall_improvement_percentage', 0)}%")
               
                # Log recomendaciones principales
                recommendations = impact_report.get("recommendations", [])
                high_priority = [r for r in recommendations if r.get("priority") == "high"]
               
                if high_priority:
                    self.logger.info(f"Recomendaciones de alta prioridad: {len(high_priority)}")
                    for rec in high_priority[:3]:  # Top 3
                        self.logger.info(f"  - {rec.get('reason', '')}")
           
            # Guardar en S3 si se solicita
            if save_to_s3:
                output_path = self.impact_analyzer.save_report_to_s3(impact_report)
                self.logger.info(f"Informe guardado en: {output_path}")
           
            return impact_report
           
        except Exception as e:
            self.logger.error(f"Error en análisis de impacto: {str(e)}")
            return {
                "status": "error",
                "error_message": str(e),
                "timestamp": datetime.now().isoformat()
            }

def main():
    """Main function for Gold layer processing."""
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'BUCKET_GOLD', 'BUCKET_SILVER',
        'S3_GOLD_CONFIG_PATH'
    ])

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)

    processor = None
    try:
        processor = GoldLayerProcessor(
            spark, glue_context, args['JOB_NAME'],
            args['BUCKET_GOLD'], args['BUCKET_SILVER']
        )
       
        processor.logger.info(f"Starting Gold Layer job: {args['JOB_NAME']}")
       
        config = processor.load_config(args['S3_GOLD_CONFIG_PATH'])
        
        processor.process_global_dimensions(config)
        table_config = config.get('gold_configs', {})


        processor.logger.info("Modo: Solo análisis de impacto (sin remediación)")
       
         
        impact_report = processor.analyze_remediation_impact(config, save_to_s3=True)
       

        processor.logger.info("Análisis de impacto completado exitosamente")
               

        clients_fact_config = table_config.get('clientes_fact', {})
        
        contact_location_fact_config = table_config.get('contact_location_fact', {})
        
        servilafa_config = table_config.get('servialfa', {})
        
        location_fact_config = table_config.get('location_fact', {})
        if servilafa_config.get('status', 0 ) == 1:
            processor._create_servialfa(servilafa_config)
        if clients_fact_config.get('status', 0 ) == 1:
            processor._create_fact_client(clients_fact_config)
        if contact_location_fact_config.get('status', 0 ) == 1:
            processor._create_fact_contact_location(contact_location_fact_config)
        
        processor.execute_fact_remediation(config)

       
        processor.logger.info("Gold Layer job completed successfully!")
       
    except Exception as e:
        print(f"Error in Gold Layer job: {str(e)}")
        if processor:
            processor.logger.error(f"Job failed: {str(e)}")
        raise
    finally:
        if processor:
            processor.cleanup_resources()
        job.commit()


if __name__ == "__main__":
    main()
