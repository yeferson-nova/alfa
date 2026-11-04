## ----- GLUE LIBRARIES ------------------ ###
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## ----- CUSTOM LIBRARIES ------------------ ###
from datos_basicos_schema import DatosBasicosSchema
from datos_contactabilidad_schema import DatosUbicacionContactabilidadSchema
from datos_sociodemograficos_schema import DatosSociodemograficosSchema
from alfaprd_staging_processor import AlfprdStagingProcessor
from cardiff_staging_processor import CardiffStagingProcessor
from acsele_staging_processor import AcseleStagingProcessor
from data_harmonization_manager import DataHarmonizationManager
from transformer_proccessor import DataCleanupProcessor
from source_catalog_schema import SourceDataGenerator
from utils import Utils
from logger_config import logger
from s3_handler import S3Handler
## ----- PYTHON EXTERNAL LIBRARIES ------------------ ###
import sys
import os
## ----- INIT SPARK CONFIG ------------------ ###
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

def load_staging_data(bucket_silver, job_id):
    """
    Load processed data from staging.

    Args:
        bucket_silver (str): Silver bucket name containing staging data
        job_id (str): Unique identifier for the current job execution

    Returns:
        dict: Dictionary containing loaded staging data for all data types

    Raises:
        Exception: If there's an error loading staging data
    """
    try:
        logger.info("Loading data from staging...")

        # Build staging paths
        base_path = f's3://{bucket_silver}/CLIENTES/'

        # Load basic data
        datos_basicos_path = f"{base_path}/DATOS_BASICOS/"
        datos_basicos_stg = utils.read_full_parquet(spark, datos_basicos_path)

        # Load sociodemographic data
        datos_sociodemo_path = f"{base_path}/DATOS_SOCIODEMOGRAFICOS/"
        datos_sociodemo_stg = utils.read_full_parquet(spark, datos_sociodemo_path)

        # Load contactability data
        datos_contactabilidad_path = f"{base_path}/DATOS_UBICACION_Y_CONTACTABILIDAD/"
        datos_contactabilidad_stg = utils.read_full_parquet(spark, datos_contactabilidad_path)

        return {
            'datos_basicos': datos_basicos_stg,
            'datos_sociodemograficos': datos_sociodemo_stg,
            'datos_contactabilidad': datos_contactabilidad_stg
        }

    except Exception as e:
        logger.error(f"Error loading staging data: {str(e)}")
        raise

datos_sociodemo_stg.co
staging_data = load_staging_data("alfa-datalake-dev-silver", "silver")
datos_socio = staging_data["datos_sociodemograficos"]
datos_socio.count()
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define el esquema
schema = StructType([
    StructField("table_name", StringType(), True),
    StructField("reference_field", StringType(), True),
    StructField("reference_value", StringType(), True),
    StructField("harmonization_field", StringType(), True),
    StructField("harmonization_value", DoubleType(), True)
])

# Ruta del archivo CSV en S3
csv_path = "s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_silver/definitions/Homologaciones-clientes.csv"

# Leer el archivo CSV con el esquema definido
df = spark.read.csv(csv_path, header=True, schema=schema)

# Mostrar los primeros registros
df.filter(F.col("table_name") == 'clientes_datos_sociodemograficos_stg').show(2)
harmonization_manager = DataHarmonizationManager()


harmonized_sociodemo = harmonization_manager.apply_harmonization(
    table_name="clientes_datos_sociodemograficos_stg",
    df_homologaciones_dos=df_hom,
    df_homologaciones_principal=df_hom,
    df=datos_socio,
    tipo="harmonization"
)
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
harmonized_sociodemo.show(2)
harmonized_sociodemo.count()
from pyspark.sql import functions as F
df_hom.filter(F.col("table_name") == "clientes_datos_sociodemograficos_stg").count()
s3_homologation_path = "s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_silver/definitions/Homologaciones-clientes-test.xlsx"
def load_homologation_data( s3_homologation_path):
    """
    Cargar datos de homologación desde Excel en S3
    """
    try:
        logger.info(f"Cargando datos de homologación desde: {s3_homologation_path}")

        # Leer archivo Excel desde S3 (requiere pandas y openpyxl)
        s3_input_parts = utils._parse_s3_path(s3_homologation_path)
        df_homologaciones = utils._read_excel_from_s3(s3_input_parts=s3_input_parts, sheet_name='Homologaciones_vf')


        logger.info(f"Datos de homologación cargados: {df_homologaciones.count()} registros")
        df_homologaciones['table_name'] = df_homologaciones['table_name'].astype(str)
        df_homologaciones['reference_value'] = df_homologaciones['reference_value'].astype(str)
        df_homologaciones['reference_field'] = df_homologaciones['reference_field'].astype(str)
        df_homologaciones['harmonization_field'] = df_homologaciones['harmonization_field'].astype(str)
        df_homologaciones = spark.createDataFrame(df_homologaciones)
        
        return df_homologaciones

    except Exception as e:
        logger.error(f"Error cargando datos de homologación: {str(e)}")
        raise
utils = Utils()
s3_homologation_path = "s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_silver/definitions/Homologaciones-clientes-sin.xlsx"
df_hom = load_homologation_data(s3_homologation_path)
df_hom.printSchema()
BUCKET_SILVER = 'alfa-datalake-dev-silver'
BUCKET_BRONZE = 'alfa-datalake-dev-bronce'
JOB_ID = 'test-job-silver-complete-end-to-end-110825'
S3_HOMOLOGATION_PATH = 's3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_silver/definitions/Homologaciones-clientes.xlsx'
LOAD_STG_SILVER = True # Default value False.
LOAD_ORIGIN_TABLE = True # Apply for the first time of the load
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")
if LOAD_ORIGIN_TABLE:
    source_data_processor = SourceDataGenerator(spark, glueContext, BUCKET_SILVER)
    source_data_processor.create_and_save_fuentes()
if LOAD_STG_SILVER:
    cardiff_processor = CardiffStagingProcessor(
        spark_session=spark,
        glue_context=glueContext,
        bucket_bronze=BUCKET_BRONZE,
        bucket_silver=BUCKET_SILVER
    )

    alfaprd_processor = AlfprdStagingProcessor(
        spark_session=spark,
        glue_context=glueContext,
        bucket_bronze=BUCKET_BRONZE,
        bucket_silver=BUCKET_SILVER
    )

    acsele_processor = AcseleStagingProcessor(
        spark_session=spark,
        glue_context=glueContext,
        bucket_bronze=BUCKET_BRONZE,
        bucket_silver=BUCKET_SILVER
    )
    cardiff_processor.cardiff_workflow_stg(JOB_ID)
    alfaprd_processor.alfaprd_workflow_stg(JOB_ID)
    acsele_processor.acsele_workflow_stg(JOB_ID)
# Load Staging tables
utils = Utils()
s3_prefix = f's3://{BUCKET_SILVER}/CLIENTES/'
datos_basicos_stg = utils.read_full_parquet(spark, s3_target_path=os.path.join(s3_prefix,'DATOS_BASICOS'))
datos_sociodemo_stg = utils.read_full_parquet(spark, s3_target_path=os.path.join(s3_prefix,'DATOS_SOCIODEMOGRAFICOS'))
datos_contactabilidad_stg = utils.read_full_parquet(spark, s3_target_path=os.path.join(s3_prefix,'DATOS_UBICACION_Y_CONTACTABILIDAD'))
datos_contactabilidad_stg.printSchema()
datos_sociodemo_stg.printSchema()
# Apply Clean and Transformations
cleanup = DataCleanupProcessor(spark, glueContext)
 
df_DB= cleanup.clean_basic_data(datos_basicos_stg)
df_DB_limpio = cleanup.select_basic_data(df_DB)
df_DS= cleanup.clean_sociodemographic_data(datos_sociodemo_stg)
df_DS_limpio = cleanup.select_sociodemographic_data(df_DS)
df_DC= cleanup.clean_contact_data(datos_contactabilidad_stg)
df_DC_limpio = cleanup.select_contact_data(df_DC)
df_db_limpio = silver_transformer.select_basic_data(df_db)
# Initialize the manager
#path_excel = 's3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_silver/definitions/Homologaciones-clientes.xlsx'
s3_input_parts = utils._parse_s3_path(S3_HOMOLOGATION_PATH)
df = utils._read_excel_from_s3(s3_input_parts=s3_input_parts, sheet_name='Homologaciones_vf')
harmonization_manager = DataHarmonizationManager()
df_homologaciones_principal = spark.createDataFrame(df)
    
# Method 1: Using orchestrate_harmonization directly
datos_basicos_clean = harmonization_manager.orchestrate_harmonization(
        table_name="clientes_datos_basicos_stg",
        harmonization_fields_df=df_homologaciones_principal,
        harmonization_rules_df=df_homologaciones_principal,
        target_df=datos_basicos_stg,
        process_type="harmonization"  # or "remediation"
)

datos_sociodemograficos_clean = harmonization_manager.orchestrate_harmonization(
        table_name="clientes_datos_sociodemograficos_stg",
        harmonization_fields_df=df_homologaciones_principal,
        harmonization_rules_df=df_homologaciones_principal,
        target_df=datos_basicos_stg,
        process_type="harmonization"  # or "remediation"
)

datos_contactabilidad_clean = harmonization_manager.orchestrate_harmonization(
        table_name="clientes_datos_ubicacion_y_contactabilidad_stg",
        harmonization_fields_df=df_homologaciones_principal,
        harmonization_rules_df=df_homologaciones_principal,
        target_df=datos_basicos_stg,
        process_type="harmonization"  # or "remediation"
)

## ----- GLUE LIBRARIES ------------------ ###
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

## ----- CUSTOM LIBRARIES ------------------ ###
from logger_config import logger
from s3_handler import S3Handler
from file_processor import FileProcessor
from datos_basicos_schema import DatosBasicosSchema
from datos_contactabilidad_schema import DatosUbicacionContactabilidadSchema
from datos_sociodemograficos_schema import DatosSociodemograficosSchema
from alfaprd_staging_processor import AlfprdStagingProcessor
from cardiff_staging_processor import CardiffStagingProcessor
from acsele_staging_processor import AcseleStagingProcessor
## ----- PYTHON EXTERNAL LIBRARIES ------------------ ###
import sys

## ----- INIT SPARK CONFIG ------------------ ###
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
BUCKET_SILVER = 'alfa-datalake-dev-silver'
BUCKET_BRONZE = 'alfa-datalake-dev-bronce'
JOB_ID = 'alfa-datalake-dev-cliens-silver090825'
COD_FUENTES = {
  "ACSELE" : 1,
  "ALFPRD" : 2,
  "CARDIFF" : 3
}
from pyspark.sql.functions import col,lit
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")
from utils import Utils
import os
utils = Utils()
s3_prefix = f's3://{BUCKET_SILVER}/CLIENTES/'
datos_basicos_stg = utils.read_full_parquet(spark, s3_target_path=os.path.join(s3_prefix,'DATOS_BASICOS'))
datos_sociodemo_stg = utils.read_full_parquet(spark, s3_target_path=os.path.join(s3_prefix,'DATOS_SOCIODEMOGRAFICOS'))
datos_contactabilidad_stg = utils.read_full_parquet(spark, s3_target_path=os.path.join(s3_prefix,'DATOS_UBICACION_Y_CONTACTABILIDAD'))
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""================================================================================
                    Cleanup Utility - AWS Glue
================================================================================
 
Description:
    Utility class with cleanup functions.
 
Author:         Jose Guzman Moreno [jose.guzman@gft.com]
Created:        2025-07-21
Last Updated:   2025-07-24
Version:        1.0.0
Environment:    Development

Dependencies:   re, unicodedata, datetime, math
 
Class:          cleanup


================================================================================
"""

import re
import unicodedata
import datetime
import math 
from logger_config import logger
from typing import List,Optional

class CleanUp:


    def __init__(self):
        """
          init  .


        Returns:
            type: Description of return value.
        """

    def round_up_number(self, value: float) -> Optional[int]:
        """
        Aproximate number to the largest number.

        Parameters:
            value (type): Description.

        Returns:
            type: Description of return value.
        """
        try:
            return math.ceil(float(value))
        except:
            return None

    def calculate_age(self, birth_date: str, format: str = "%d/%m/%Y") -> Optional[int]:
        """
        calculate age.

        Parameters:
            birth_date (type): Description.
            format (type): Description.

        Returns:
            type: Description of return value.
        """
        try:
            birth = datetime.datetime.strptime(birth_date, format)
            today = datetime.datetime.today()
            age = today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day))
            return age
        except:
            return None

    def concatenate_if_applicable(self, *args: str) -> str:
        """
        concatenate if applicable.

        Parameters:
            *args (type): Description.

        Returns:
            type: Description of return value.
        """
        return ' '.join([str(arg) for arg in args if arg])

    def to_lowercase(self,text: Optional[str]) -> Optional[str]:

        """
        convert to lowercase.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        return text.lower() if text else text

    def to_uppercase(self,text: Optional[str]) -> Optional[str]:

        """
        convert to uppercase .

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        return text.upper() if text else text

    def absolute_value(self, value: float) -> Optional[float]:
        """
        absolute value.

        Parameters:
            value (type): Description.

        Returns:
            type: Description of return value.
        """
        try:
            return abs(float(value))
        except:
            return None

    def adjust_day_month_out_of_range(self, date_str: str) -> str:
        """
        adjust day month out of range.

        Parameters:
            date_str (type): Description.

        Returns:
            type: Description of return value.
        """
        try:
            day, month, year = map(int, date_str.split('/'))
            day = min(max(day, 1), 31)
            month = min(max(month, 1), 12)
            return f"{day:02d}/{month:02d}/{year}"
        except:
            return date_str

    def only_numbers(self,text: str) -> str:

        """
        numbers only.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        return re.sub(r'[^0-9]', '', text)

    def only_alphanumeric(self,text: str) -> str:

        """
        alphanumeric only.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        return re.sub(r'[^a-zA-Z0-9]', '', text)

    def remove_special_characters(self, text: str) -> str:
        """
        remove special characters.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        text = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', text)
        text = re.sub(r'[^\w\s]', '', text)
        return text

    def remove_nit_verification_digit(self, nit: str) -> str:
        """
        remove check digit.

        Parameters:
            nit (type): Description.

        Returns:
            type: Description of return value.
        """
        return nit.split('-')[0] if '-' in nit else nit

    def remove_extra_spaces(self, text: str) -> str:
        """
        remove extra spaces.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        return ' '.join(text.split())

    def remove_letters_if_document(self, text: str, doc_type: str) -> str:
        """
        remove letters if the document is applicable.

        Parameters:
            text (type): Description.
            doc_type (type): Description.

        Returns:
            type: Description of return value.
        """
        doc_types = ['CC', 'TI', 'RC', 'CE', 'NIT']
        return re.sub(r'[A-Za-z]', '', text.upper()) if doc_type in doc_types else text

    def clean_email(self, email: str) -> str:
        """
        clean email.

        Parameters:
            email (type): Description.

        Returns:
            type: Description of return value.
        """
        email = re.sub(r'[\\"\\,\\:\\;\\<\\>\\(\\)\\[\\]\\{\\}\\|]', '', email)
        email = self.remove_accents(email)
        email = email.replace('..', '.')
        email = email.strip('.')
        email = re.sub(r'\.@', '@', email)
        email = re.sub(r'@+', '@', email)
        return email.lower()

    def remove_accents(self, text: str) -> str:
        """
        remove accents.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        if not text:
            return text
        text = unicodedata.normalize('NFD', text)
        text = text.encode('ASCII', 'ignore').decode('utf-8')
        return text

    def remove_text_special_characters(self, text: str) -> str:
        """
        remove special characters.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        text = self.remove_special_characters(text)
        text = self.remove_accents(text)
        return text


    def replace_multiple_spaces(self, text: str) -> str:
        """
        replace multiple spaces.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        return re.sub(r'\s+', ' ', text)
        
    def select_minimum_date(self, date_list: List[str], format: str = "%d/%m/%Y") -> Optional[str]:
        """
        select lower date.

        Parameters:
            date_list (type): Description.
            format (type): Description.

        Returns:
            type: Description of return value.
        """
        dates = []
        for date in date_list:
            try:
                dates.append(datetime.datetime.strptime(date, format))
            except:
                continue
        return min(dates).strftime(format) if dates else None

    def validate_date(self, date_str: str, format: str = "%d/%m/%Y") -> Optional[str]:
        """
        validate date.

        Parameters:
            date_str (type): Description.
            format (type): Description.

        Returns:
            type: Description of return value.
        """
        try:
            date = datetime.datetime.strptime(date_str, format)
            if date.year < 1900 or date >= datetime.datetime.today():
                return None
            return date_str
        except:
            return None

    def adjust_invalid_date(self, date_str: str, format: str = "%d/%m/%Y") -> str:
        """
        adjust invalid date.

        Parameters:
            date_str (type): Description.
            format (type): Description.

        Returns:
            type: Description of return value.
        """
        try:
            day, month, year = map(int, date_str.split('/'))
            month = min(max(mes, 1), 12)
            last_day = (datetime.date(year, month % 12 + 1, 1) - datetime.timedelta(days=1)).day
            day = min(day, last_day)
            return f"{day:02d}/{month:02d}/{year}"
        except:
            return date_str

    def change_invalid_date_to_null(self, date_str: str, format: str = "%d/%m/%Y") -> Optional[str]:
        """
        change invalid date to null.

        Parameters:
            date_str (type): Description.
            format (type): Description.

        Returns:
            type: Description of return value.
        """
        try:
            date = datetime.datetime.strptime(date_str, format)
            if date.year < 1900 or date >= datetime.datetime.today():
                return None
            return date_str
        except:
            return None
    def clean_id_type(self, text: Optional[str]) -> Optional[str]:
        """
        clean id type.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """

        if text is None:
                return None  

        text = self.remove_accents(text)
        text = self.to_lowercase(text)
        text = self.remove_extra_spaces(text)
        text = self.remove_special_characters(text)
        return text

    def clean_id_number(self, text: Optional[str], doc_type: str) -> Optional[str]:
        """
        clean id number.

        Parameters:
            text (type): Description.
            doc_type (type): Description.

        Returns:
            type: Description of return value.
        """
        if text is None:
                return None 
        text = self.only_alphanumeric(text)
        text = self.remove_extra_spaces(text)
        text = self.remove_nit_verification_digit(text)
        text = self.remove_letters_if_document(text, doc_type)
        return text

    def clean_names(self, text: Optional[str]) -> Optional[str]:
        """
        clean names.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        if text is None:
                return None 
        text = self.remove_special_characters(text)
        text = self.to_uppercase(text)
        text = self.remove_accents(text)
        text = re.sub(r'[0-9]', '', text) if re.search(r'[a-zA-Z]', text) else text
        text = self.remove_extra_spaces(text)
        return text

    def clean_full_name(self, text: Optional[str]) -> Optional[str]:
        """
        clean full name.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        if text is None:
                return None 
        text = self.replace_multiple_spaces(text)
        text = text.strip()
        return text

    def clean_business_name(self, text: Optional[str]) -> Optional[str]:
        """
        clean business name.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        if text is None:
                return None 
        text = self.to_uppercase(text)
        text = self.remove_accents(text)
        text = self.replace_multiple_spaces(text)
        text = text.strip()
        return text

    def clean_date(self, date_str: Optional[datetime.date]) -> Optional[str]:
        """
        clean date.

        Parameters:
            date_str (type): Description.

        Returns:
            type: Description of return value.
        """
        if date_str is None:
                return None 
        date_str = self.remove_special_characters(date_str.strftime("%d/%m/%Y"))
        date_str = self.remove_extra_spaces(date_str)
        date_str = self.adjust_day_month_out_of_range(date_str)
        date_str = self.adjust_invalid_date(date_str)
        date_str = self.change_invalid_date_to_null(date_str)
        return date_str

    def clean_date_no_null(self, date_str: Optional[datetime.date]) -> Optional[str]:
        """
        clear date no to be nul .

        Parameters:
            date_str (type): Description.

        Returns:
            type: Description of return value.
        """
        if date_str is None:
                return None 
        date_str = self.remove_special_characters(date_str.strftime("%d/%m/%Y"))
        date_str = self.remove_extra_spaces(date_str)
        date_str = self.adjust_day_month_out_of_range(date_str)
        date_str = self.adjust_invalid_date(date_str)
        return date_str

    def clean_city(self,text: Optional[str]) -> Optional[str]:

        """
        clean city.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        if text is None:
                return None 
        text = self.remove_special_characters(text)
        text = self.to_uppercase(text)
        text = self.remove_accents(text)
        text = self.remove_extra_spaces(text)
        return text

    def clean_country(self,text: Optional[str]) -> Optional[str]:

        """
        clean country.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        if text is None:
                return None 
        text = self.remove_special_characters(text)
        text = self.to_lowercase(text)
        text = self.remove_accents(text)
        text = self.remove_extra_spaces(text)
        return text

    def clean_text_lower(self, text: Optional[str]) -> Optional[str]:
        """
        clean text and convert to lowercase.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        if text is None:
                return None 
        text = self.remove_special_characters(text)
        text = self.to_lowercase(text)
        text = self.remove_accents(text)
        text = self.remove_extra_spaces(text)
        return text

    def clean_address(self, text: Optional[str]) -> Optional[str]:
        """
        clean address.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        if text is None:
                return None 
        text = self.remove_special_characters(text)
        text = self.to_uppercase(text)
        text = self.remove_accents(text)
        text = self.remove_extra_spaces(text)
        text = text.strip()
        return text

    def clean_gender(self, text: Optional[str]) -> Optional[str]:
        """
        clean gender.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        if text is None:
                return None 
        text = self.remove_special_characters(text)
        text = self.to_lowercase(text)
        text = self.remove_accents(text)
        text = self.remove_extra_spaces(text)
        return text

    def clean_email_address(self, email: Optional[str]) -> Optional[str]:
        """
        clean email address.

        Parameters:
            email (type): Description.

        Returns:
            type: Description of return value.
        """
        if email is None:
                return None 
        return self.clean_email(email)

    def clean_address(self, text: Optional[str]) -> Optional[str]:
        """
        clean addres.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        if text is None:
                return None

        text = self.remove_special_characters(text)
        text = self.to_uppercase(text)
        text = self.remove_accents(text)
        text = self.remove_extra_spaces(text)
        return text

    def clean_phone_number(self, text: Optional[str]) -> Optional[str]:
        """
        clean phone number.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        if text is None:
                return None
        text = self.only_numbers(text)
        text = self.remove_extra_spaces(text)
        return text[:10] if len(text) > 10 else text

    def clean_extension(self, text: Optional[str]) -> Optional[str]:
        """
        clean extension.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        if text is None:
                return None
        return self.only_numbers(text)

    def clean_children_count(self, value: Optional[str]) -> Optional[int]:
        """
        clean children count.

        Parameters:
            value (type): Description.

        Returns:
            type: Description of return value.
        """
        if value is None:
                return None
        value = self.only_numbers(value)
        value = self.absolute_value(value)
        value = self.round_up_number(value)
        return value

    def clean_company_nit(self, text: Optional[str], doc_type: str) -> Optional[str]:
        """
        clean company nit.

        Parameters:
            text (type): Description.
            doc_type (type): Description.

        Returns:
            type: Description of return value.
        """
        if text is None:
                return None
        text = re.sub(r'[^a-zA-Z0-9\\-_]', '', text)
        text = remove_extra_spaces(text)
        text = remove_letters_if_document(text, doc_type)
        return text

    def clean_company_name(self, text: Optional[str]) -> Optional[str]:
        """
        clean company name.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        if text is None:
                return None
        text = to_uppercase(text)
        text = remove_accents(text)
        text = remove_extra_spaces(text)
        return text

    def clean_company_position(self, text: Optional[str]) -> Optional[str]:
        """
        clean company position.

        Parameters:
            text (type): Description.

        Returns:
            type: Description of return value.
        """
        if text is None:
                return None
        text = self.remove_accents(text)
        text = self.remove_extra_spaces(text)
        return text

    def clean_company_entry_date(self, date_str: Optional[datetime.date]) -> Optional[str]:
        """
        clean company entry date.

        Parameters:
            date_str (type): Description.

        Returns:
            type: Description of return value.
        """

        return self.clean_date(date_str)


    def clean_phone_number_by_type(self, number: Optional[str], phone_type: str) -> Optional[str]:
        """
        clean phone number.

        Parameters:
            number (type): Description.
            phone_type (type): Description.

        Returns:
            type: Description of return value.
        """
        if number is None:
            return None

        # Eliminar espacios y caracteres no numéricos
        clean_number = re.sub(r'[^0-9]', '', str(number).strip())

        # Ajustar longitud según tipo de teléfono

        clean_number = clean_number[:10]  # máximo 10 dígitos 

        return clean_number
"""================================================================================
                    Apply Cleanup Utility - AWS Glue
================================================================================
 
Description:
    Utility class with cleanup applications.
 
Author:         Jose Guzman Moreno [jose.guzman@gft.com]
Created:        2025-07-21
Last Updated:   2025-07-24
Version:        1.0.0
Environment:    Development
 
Class:          Apply_cleanup


================================================================================
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,udf
from pyspark.sql.types import StringType
from typing import Any

from logger_config import logger

cleanup = CleanUp()

"""================================================================================
                    UDFs definition- AWS Glue
================================================================================
"""
clean_id_type_udf = udf(cleanup.clean_id_type, StringType())
clean_document_number_udf = udf(cleanup.clean_id_number, StringType())
clean_names_udf = udf(cleanup.clean_names, StringType())
clean_full_name_udf = udf(cleanup.clean_full_name, StringType())
clean_company_name_udf = udf(cleanup.clean_company_name, StringType())
clean_date_udf = udf(cleanup.clean_date, StringType())
clean_date_not_null_udf = udf(cleanup.clean_date_no_null, StringType())
clean_city_udf = udf(cleanup.clean_city, StringType())
clean_country_udf = udf(cleanup.clean_country, StringType())
clean_gender_udf = udf(cleanup.clean_gender, StringType())
clean_text_lower_udf = udf(cleanup.clean_text_lower, StringType())
clean_email_udf = udf(cleanup.clean_email, StringType())
clean_address_udf = udf(cleanup.clean_address, StringType())
clean_phone_number_udf = udf(cleanup.clean_phone_number, StringType())
clean_extension_udf = udf(cleanup.clean_extension, StringType())
clean_children_count_udf = udf(cleanup.clean_children_count, StringType())
clean_company_nit_udf = udf(cleanup.clean_company_nit, StringType())
clean_company_name_udf = udf(cleanup.clean_company_name, StringType())
clean_company_position_udf = udf(cleanup.clean_company_position, StringType())
clean_company_entry_date_udf = udf(cleanup.clean_company_entry_date, StringType())
clean_address_udf = udf(cleanup.clean_address, StringType())


class ApplyCleanup:

    def __init__(self, spark_session: Any, glue_context: Any) -> None:
        """ 
        class initialization
        """

        self._spark = spark_session
        self._glue_context = glue_context

    


    def clean_basic_data(self, df: Any) -> Any:
        """
        implement all the clean up UDFs for basic data.

        Parameters:
            df

        Returns:
            df_basic_data_STG_clean.
        """

        logger.info(f"cleanning basic data fields ")

        df_basic_data_STG_clean = df \
        .withColumn("cod_tipo_identificacion_limpio", clean_id_type_udf(col("cod_tipo_identificacion"))) \
        .withColumn("numero_documento_limpio", clean_document_number_udf(col("numero_documento"),col("cod_tipo_identificacion"))) \
        .withColumn("primer_nombre_limpio", clean_names_udf(col("primer_nombre"))) \
        .withColumn("segundo_nombre_limpio", clean_names_udf(col("segundo_nombre"))) \
        .withColumn("primer_apellido_limpio", clean_names_udf(col("primer_apellido"))) \
        .withColumn("segundo_apellido_limpio", clean_names_udf(col("segundo_apellido"))) \
        .withColumn("nombre_completo_limpio", clean_full_name_udf(col("nombre_completo"))) \
        .withColumn("fecha_vinculacion_limpia", clean_date_udf(col("fecha_vinculacion"))) \
        .withColumn("ciudad_expedicion_documento_limpia", clean_city_udf(col("ciudad_expedicion_documento"))) \
        .withColumn("fecha_expedicion_documento_limpia", clean_date_udf(col("fecha_expedicion_documento"))) \
        .withColumn("tipo_empresa_limpio", clean_id_type_udf(col("tipo_empresa")))


        logger.info(f" basic data fields cleaned")

        return df_basic_data_STG_clean

    
    def clean_sociodemographic_data(self, df: Any) -> Any:
        """
        implement all the clean up UDFs for sociodemographic data.

        Parameters:
            df

        Returns:
            df_sociodemographic_data_STG_clean.   
        """
        logger.info(f"cleanning sociodemographic data fields ")

        df_sociodemographic_data_STG_clean = df \
        .withColumn("cod_tipo_identificacion_limpio", clean_id_type_udf(col("cod_tipo_identificacion"))) \
        .withColumn("numero_documento_limpio", clean_document_number_udf(col("numero_documento"), col("cod_tipo_identificacion"))) \
        .withColumn("fecha_nacimiento_limpia", clean_date_udf(col("fecha_nacimiento"))) \
        .withColumn("ciudad_nacimiento_limpia", clean_city_udf(col("ciudad_nacimiento"))) \
        .withColumn("departamento_nacimiento_limpio", clean_city_udf(col("departamento_nacimiento"))) \
        .withColumn("pais_nacimiento_limpio", clean_text_lower_udf(col("pais_nacimiento"))) \
        .withColumn("nacionalidad_limpia", clean_text_lower_udf(col("nacionalidad"))) \
        .withColumn("genero_limpio", clean_text_lower_udf(col("genero"))) \
        .withColumn("estado_civil_limpio", clean_text_lower_udf(col("cod_estado_civil"))) \
        .withColumn("nivel_estudios_limpio", clean_text_lower_udf(col("nivel_estudios"))) \
        .withColumn("profesion_limpia", clean_text_lower_udf(col("profesion"))) \
        .withColumn("ocupacion_limpia", clean_text_lower_udf(col("ocupacion"))) \
        .withColumn("actividad_economica_limpia", clean_text_lower_udf(col("cod_actividad_economica"))) \
        .withColumn("tipo_sociedad_limpia", clean_text_lower_udf(col("cod_tipo_sociedad"))) \
        .withColumn("tipo_id_empresa_limpio", clean_text_lower_udf(col("tipo_id_empresa"))) \
        .withColumn("nit_empresa_limpio", clean_company_nit_udf(col("nit_empresa"), col("tipo_id_empresa"))) \
        .withColumn("ciudad_expedicion_documento_limpia", clean_city_udf(col("ciudad_expedicion_documento"))) \
        .withColumn("fecha_expedicion_documento_limpia", clean_date_not_null_udf(col("fecha_expedicion_documento"))) \
        .withColumn("tipo_empresa_limpio", clean_text_lower_udf(col("tipo_empresa"))) 

        logger.info(f"sociodemographic data fields cleaned")

        return df_sociodemographic_data_STG_clean



    def clean_contact_data(self, df: Any) -> Any:
        """
        implement all the clean up UDFs for contact data.

        Parameters:
            df

        Returns:
            df_contact_data_STG_clean.
        """

        logger.info(f"cleanning contact data fields ")

        df_contact_data_STG_clean = df     \
        .withColumn("cod_tipo_identificacion_limpio", clean_id_type_udf(col("cod_tipo_identificacion"))) \
        .withColumn("numero_documento_limpio", clean_document_number_udf(col("numero_documento"), col("cod_tipo_identificacion"))) \
        .withColumn("correo_electronico_limpio", clean_email_udf(col("correo_electronico"))) \
        .withColumn("direccion_limpia", clean_address_udf(col("direccion"))) \
        .withColumn("barrio_limpio", clean_address_udf(col("barrio"))) \
        .withColumn("ciudad_limpia", clean_city_udf(col("ciudad"))) \
        .withColumn("departamento_limpio", clean_city_udf(col("departamento"))) \
        .withColumn("pais_limpio", clean_text_lower_udf(col("pais"))) \
        .withColumn("indicativo_limpio", clean_text_lower_udf(col("indicativo"))) \
        .withColumn("numero_telefono_limpio", clean_phone_number_udf(col("numero_telefono"))) \
        .withColumn("extension_limpia", clean_extension_udf(col("extension")))

        logger.info(f"contact data fields cleaned")

        return df_contact_data_STG_clean 


    def select_basic_data(self, df: Any) -> Any:
        """
        select the cleaned colums for basic data.

        Parameters:
            df

        Returns:
            df.
        """
        logger.info(f"Seleccting basic data fields cleaned")

        df= df.select(
        

         
        col("cod_tipo_identificacion_limpio").alias("Cod_Tipo_Identificacion"),

        
        col("numero_documento_limpio").cast("string").alias("Numero_Documento"),

        
        col("primer_nombre_limpio").alias("Primer_Nombre"),

         
        col("segundo_nombre_limpio").alias("Segundo_Nombre"),

        
        col("primer_apellido_limpio").alias("Primer_Apellido"),

        
        col("segundo_apellido_limpio").alias("Segundo_Apellido"),

        
    
        col("nombre_completo_limpio").alias("Nombre_Completo"),

        
        col("fecha_expedicion_documento_limpia").alias("Fecha_Expedición_Documento"),

        
        col("ciudad_expedicion_documento_limpia").alias("Ciudad_Expedición_Documento"),

        
        col("fecha_vinculacion_limpia").alias("Fecha_Vinculacion"),

        
        col("cod_estado").alias("Cod_Estado"),
        
        col("cod_tipo_persona").alias("Cod_Tipo_Persona"),   
        
        col("Cod_Tipo_Empresa").alias("Cod_Tipo_Empresa"),

        
        col("tipo_empresa_limpio").alias("Tipo_Empresa"),


        
        col("cod_fuente_fk"),

        col("job_cargue_id"),

        col("fecha_cargue"),

        col("customer_dbasicos_id"))

        logger.info(f"basic data fields Selected")

        return df



    def select_sociodemographic_data(self, df: Any) -> Any:
        """
        select the cleaned colums for sociodemographic data.

        Parameters:
            df

        Returns:
            df.
        """
        logger.info(f"Seleccting sociodemographic data fields cleaned ")

        df=df.select(
       
        col("cod_tipo_identificacion_limpio").alias("Cod_Tipo_Identificacion"),
        
        col("numero_documento_limpio").cast("string").alias("Numero_Documento"),
       
        col("fecha_nacimiento_limpia").alias("Fecha_Nacimiento"),
        
        col("ciudad_nacimiento_limpia").alias("Ciudad_Nacimiento"),
        
        col("departamento_nacimiento_limpio").alias("Departamento_Nacimiento"),
        
        col("pais_nacimiento_limpio").alias("Pais_Nacimiento"),
        
        col("nacionalidad_limpia").alias("Nacionalidad"),
        
        col("genero_limpio").alias("Genero"),
        
        col("estado_civil_limpio").alias("Cod_Estado_Civil"),
        
        col("nivel_estudios_limpio").alias("Nivel_Estudios"),
        
        col("profesion_limpia").alias("Profesion"),
        
        col("ocupacion_limpia").alias("Ocupacion_Limpia"),   
        
        col("Cod_Tipo_Empresa").alias("Cod_Tipo_Empresa"),
        
        col("actividad_economica_limpia").alias("Cod_Actividad_Economica"),
        col("tipo_sociedad_limpia").alias("Cod_Tipo_Sociedad"),
        col("tipo_id_empresa_limpio").alias("Tipo_Id_Empresa"),
        col("nit_empresa_limpio").alias("Nit_Empresa"),
        col("ciudad_expedicion_documento_limpia").alias("Ciudad_Expedicion_Documento"),
        col("fecha_expedicion_documento_limpia").alias("Fecha_Expedicion_Documento"),
        col("tipo_empresa_limpio").alias("Tipo_Empresa"),
        
        col("cod_fuente_fk"),
        col("job_cargue_id"),
        col("fecha_cargue"),
        col("customer_dsociodemograficos_id"))

        logger.info(f"sociodemographic data fields Selected ")

        
        return df


    def select_contact_data(self, df: Any) -> Any:
        """
        select the cleaned colums for contact data.

        Parameters:
            df

        Returns:
            df.
        """

        logger.info(f"Seleccting contact data fields cleaned ")

        df = df.select(
        
        col("cod_tipo_identificacion_limpio").alias("Cod_Tipo_Identificacion"),

        
        col("numero_documento_limpio").cast("string").alias("Numero_Documento"),

        
        col("correo_electronico_limpio").alias("Correo_Electronico"),

        
        col("direccion_limpia").alias("Direccion"),

        
        col("barrio_limpio").alias("Barrio"),

        
        col("ciudad_limpia").alias("Ciudad"),

        

        col("departamento_limpio").alias("Departamento"),

       
        col("pais_limpio").alias("Pais"),

        
        col("indicativo_limpio").alias("Indicativo"),

        
        col("numero_telefono_limpio").alias("Numero_Telefono"),

        
        col("extension_limpia").alias("Extension"),

        
        col("tipo_telefono").alias("Tipo_Telefono"),

        
        col("cod_fuente_fk"),

        col("job_cargue_id"),

        col("fecha_cargue"),

        col("customer_dubic_contact_id"))

        logger.info(f"contact data fields Selected ")

        return df


cleanup = ApplyCleanup(spark, glueContext)
 
df_DB= cleanup.clean_basic_data(datos_basicos_stg)
df_DB_limpio = cleanup.select_basic_data(df_DB)
df_DS= cleanup.clean_sociodemographic_data(datos_sociodemo_stg)
df_DS_limpio = cleanup.select_sociodemographic_data(df_DS)
df_DC= cleanup.clean_contact_data(datos_contactabilidad_stg)
df_DC_limpio = cleanup.select_contact_data(df_DC)
df_DB_limpio.printSchema()
df_DB_limpio.show(5,truncate=False)
df_result.filter(col("cod_tipo_identificacion" )  == "CC").show(3)
df_ciuu = datos_sociodemo_stg.select(col("cod_actividad_economica")).distinct()
df_ocu = datos_sociodemo_stg.select(col("ocupacion")).distinct()
df_ec = datos_sociodemo_stg.select(col("cod_estado_civil")).distinct()
df_gen = datos_sociodemo_stg.select(col("genero",)).distinct()
df_gen.show()
df_ciuu.show()
df_ciuu.coalesce(1).write.format("csv").option("header",True).mode("append").save("s3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_silver/definitions/")
df_ocu.show()
path_excel = 's3://alfa-datalake-dev-assets/CLIENTS_ASSETS/phase_silver/definitions/Homologaciones-clientes.xlsx'
s3_input_parts = utils._parse_s3_path(path_excel)
df = utils._read_excel_from_s3(s3_input_parts=s3_input_parts, sheet_name='Homologaciones_vf')
df.columns
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
                    Data Harmonization Manager - Data Transformation Engine
================================================================================
Description:
    Specialized class for data harmonization and remediation operations.
    Handles field mapping, value transformation, and data quality enhancement
    through configurable harmonization rules.
Author:         Maria Cardona
Created:        2025-08-10
Last Updated:   2025-08-10
Version:        2.0.0 - Enhanced Implementation with English Standards
Environment:    Development
Dependencies:   pyspark, logger_config
Class:          DataHarmonizationManager
Main Methods:   orchestrate_harmonization(), execute_field_transformation()
================================================================================
"""
## ----- SPARK LIBRARIES ------------------ ###
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
## ----- PYTHON LIBRARIES ------------------ ###
from logger_config import logger
from typing import List, Dict, Optional


class DataHarmonizationManager:
    """
    Manager class for data harmonization and remediation operations.
    CORE RESPONSIBILITY: Data harmonization through field mapping and value transformation.
    
    Author: Juan Alejandro Carrillo Jaimes
    
    This class handles two main operations:
    1. Harmonization: Direct field value replacement based on mapping rules
    2. Remediation: Cross-field value mapping and data enrichment
    
    The class follows a two-step process:
    - Step 1 (orchestrate_harmonization): Orchestrates the transformation process
    - Step 2 (execute_field_transformation): Executes specific field transformations
    """
    
    # Class constants for process types
    PROCESS_TYPE_HARMONIZATION = "harmonization"
    PROCESS_TYPE_REMEDIATION = "remediation"
    
    # Internal field type markers
    FIELD_TYPE_HARMONIZATION = "H"
    FIELD_TYPE_REMEDIATION = "R"
    FIELD_TYPE_UNKNOWN = "X"
    
    def __init__(self):
        """
        Initialize DataHarmonizationManager with logging setup.
        """
        logger.info("DataHarmonizationManager initialized", "DataHarmonizationManager")
        
    def _validate_input_parameters(self, df: DataFrame, target_field: str, 
                                 harmonization_rules: DataFrame, process_type: str) -> None:
        """
        Validate input parameters for harmonization operations.
        
        Args:
            df: Input DataFrame to process
            target_field: Field name to be transformed
            harmonization_rules: DataFrame containing transformation rules
            process_type: Type of process (harmonization or remediation)
            
        Raises:
            ValueError: If parameters are invalid
        """
        if df is None or df.count() == 0:
            raise ValueError("Input DataFrame cannot be None or empty")
        
        if not target_field or target_field.strip() == "":
            raise ValueError("Target field cannot be empty")
            
        if harmonization_rules is None or harmonization_rules.count() == 0:
            raise ValueError("Harmonization rules DataFrame cannot be None or empty")
            
        if process_type not in [self.PROCESS_TYPE_HARMONIZATION, self.PROCESS_TYPE_REMEDIATION]:
            raise ValueError(f"Process type must be '{self.PROCESS_TYPE_HARMONIZATION}' or '{self.PROCESS_TYPE_REMEDIATION}'")

    def execute_field_transformation(self, df: DataFrame, target_field: str, 
                                   harmonization_rules: DataFrame, process_type: str) -> DataFrame:
        """
        Execute specific field transformations based on harmonization rules.
        
        This method performs the actual field-level transformation by:
        1. Adding unique identifiers for join operations
        2. Joining with harmonization rules
        3. Applying transformations based on process type
        4. Cleaning up temporary columns
        
        Args:
            df: DataFrame to process
            target_field: The field name to harmonize or remediate
            harmonization_rules: DataFrame containing transformation rules
            process_type: Type of process to deploy (harmonization or remediation)
            
        Returns:
            DataFrame: Processed DataFrame with harmonized/remediated data
            
        Raises:
            ValueError: If input parameters are invalid
            Exception: If transformation fails
        """
        try:
            self._validate_input_parameters(df, target_field, harmonization_rules, process_type)
            
            logger.info(f"Executing {process_type} transformation for field: {target_field}", 
                       "DataHarmonizationManager")
            
            # Add unique identifier for safe joins
            df_with_id = df.withColumn("temp_transformation_id", F.monotonically_increasing_id())
            
            # Prepare columns to drop after transformation
            cleanup_columns = [
                "reference_field", "table_name", "harmonization_field", 
                "reference_value", "harmonization_value"
            ]
            
            # Remove process type column to avoid conflicts
            harmonization_rules_clean = harmonization_rules.drop("field_type")
            
            # Create initial DataFrame for join operations
            df_initial = df_with_id.select("temp_transformation_id", target_field)
            
            # Join with harmonization rules using case-insensitive matching
            df_joined = df_initial.join(
                harmonization_rules_clean, 
                F.upper(df_initial[target_field]) == F.upper(harmonization_rules_clean["reference_value"]), 
                "left"
            )
            
            # Extract harmonization field name for remediation
            harmonization_field_info = harmonization_rules_clean.select(
                'harmonization_field', 'harmonization_value'
            ).first()
            
            if harmonization_field_info is None:
                logger.warning(f"No harmonization rules found for field: {target_field}", 
                             "DataHarmonizationManager")
                return df.drop("temp_transformation_id") if "temp_transformation_id" in df.columns else df
            
            harmonization_field_name = harmonization_field_info["harmonization_field"]
            
            if process_type == self.PROCESS_TYPE_HARMONIZATION:
                # Direct field value replacement
                df_transformed = df_joined.withColumn(
                    target_field,
                    F.when(
                        F.col("harmonization_value").isNotNull(),
                        F.col("harmonization_value")
                    ).otherwise(F.col(target_field))
                )
                
                # Clean up temporary columns
                df_transformed = df_transformed.drop(*cleanup_columns)
                
                # Rejoin with original DataFrame
                df_final = df_with_id.drop(target_field).alias('original').join(
                    df_transformed.alias('transformed'), 
                    "temp_transformation_id", 
                    "inner"
                )
                
            else:  # REMEDIATION
                # Cross-field value mapping and enrichment
                
                # Add harmonization field if it doesn't exist
                if harmonization_field_name not in df_with_id.columns:
                    df_with_id = df_with_id.withColumn(harmonization_field_name, F.lit(None).cast(StringType()))
                    logger.info(f"Added new field for remediation: {harmonization_field_name}", 
                               "DataHarmonizationManager")
                
                # Create temporary column for new values
                temp_column_name = f"{harmonization_field_name}_temp_val"
                df_transformed = df_joined.withColumn(
                    temp_column_name, 
                    F.when(
                        F.col("harmonization_value").isNotNull(),
                        F.col("harmonization_value")
                    ).otherwise(F.lit(None))
                )
                
                # Clean up rule columns
                df_transformed = df_transformed.drop(*cleanup_columns)
                
                # Rejoin with original DataFrame
                df_rejoined = df_with_id.drop(target_field).alias('original').join(
                    df_transformed.alias('transformed'), 
                    "temp_transformation_id", 
                    "inner"
                )
                
                # Merge values using coalesce (prioritize new values over existing)
                df_final = df_rejoined.withColumn(
                    harmonization_field_name, 
                    F.coalesce(
                        F.col(f"transformed.{temp_column_name}"), 
                        F.col(f"original.{harmonization_field_name}")
                    )
                ).drop(temp_column_name)
            
            # Remove temporary ID column
            df_result = df_final.drop("temp_transformation_id")
            
            result_count = df_result.count()
            logger.success(f"Field transformation completed successfully. Records processed: {result_count}", 
                          "DataHarmonizationManager")
            
            return df_result
            
        except Exception as e:
            logger.error(f"Error in field transformation for {target_field}: {str(e)}", 
                        "DataHarmonizationManager")
            raise
        
    def orchestrate_harmonization(self, table_name: str, harmonization_fields_df: DataFrame, 
                                 harmonization_rules_df: DataFrame, target_df: DataFrame, 
                                 process_type: str) -> DataFrame:
        """
        Orchestrate the harmonization transformation process for the entire DataFrame.
        
        This method coordinates the harmonization process by:
        1. Preparing and filtering harmonization rules
        2. Identifying applicable fields for transformation
        3. Iterating through fields and applying transformations
        4. Managing the overall transformation workflow
        
        Args:
            table_name: Name of the table being processed
            harmonization_fields_df: DataFrame containing field definitions for harmonization
            harmonization_rules_df: DataFrame that contains the transformation rules
            target_df: DataFrame to be harmonized/remediated
            process_type: Type of process to deploy (harmonization or remediation)
            
        Returns:
            DataFrame: Fully processed DataFrame with all applicable transformations applied
            
        Raises:
            ValueError: If input parameters are invalid
            Exception: If orchestration process fails
        """
        try:
            logger.info(f"Starting {process_type} orchestration for table: {table_name}", 
                       "DataHarmonizationManager")
            
            # Validate inputs
            if not table_name or table_name.strip() == "":
                raise ValueError("Table name cannot be empty")
                
            if harmonization_rules_df is None or harmonization_rules_df.count() == 0:
                raise ValueError("Harmonization rules DataFrame cannot be None or empty")
                
            if target_df is None or target_df.count() == 0:
                raise ValueError("Target DataFrame cannot be None or empty")
            
            # Normalize table name for consistent processing
            normalized_table_name = table_name.lower()
            
            # Prepare field mapping rules with normalized column names
            field_rules = harmonization_rules_df.select(
                "table_name", "reference_field", "harmonization_field", 
                "harmonization_value", "reference_value"
            ).distinct()
            
            # Add normalized field names and determine field type
            field_rules = field_rules.withColumn(
                "reference_field_normalized", F.lower(F.col("reference_field"))
            ).withColumn(
                "harmonization_field_normalized", F.lower(F.col("harmonization_field"))
            ).withColumn(
                "field_type", 
                F.when(
                    F.col("reference_field_normalized") == F.col("harmonization_field_normalized"), 
                    self.FIELD_TYPE_HARMONIZATION
                ).when(
                    F.col("reference_field_normalized") != F.col("harmonization_field_normalized"), 
                    self.FIELD_TYPE_REMEDIATION
                ).otherwise(self.FIELD_TYPE_UNKNOWN)
            )
            
            # Filter rules based on process type
            if process_type == self.PROCESS_TYPE_HARMONIZATION:
                filter_condition = F.col("reference_field_normalized") == F.col("harmonization_field_normalized")
                logger.info("Filtering rules for harmonization (same field transformations)", 
                           "DataHarmonizationManager")
            elif process_type == self.PROCESS_TYPE_REMEDIATION:
                filter_condition = F.col("reference_field_normalized") != F.col("harmonization_field_normalized")
                logger.info("Filtering rules for remediation (cross-field transformations)", 
                           "DataHarmonizationManager")
            else:
                raise ValueError(f"Invalid process type: {process_type}")
            
            filtered_field_rules = field_rules.filter(filter_condition).distinct()
            
            # Get target DataFrame column names in lowercase for matching
            target_columns_lower = [col.lower() for col in target_df.columns]
            
            # Extract list of fields to process from harmonization_fields_df
            fields_to_process = harmonization_fields_df.select(
                F.lower("harmonization_field")
            ).distinct().rdd.flatMap(lambda x: x).collect()
            
            # Initialize result DataFrame
            result_df = target_df
            processed_fields_count = 0
            
            logger.info(f"Found {len(fields_to_process)} potential fields to process", 
                       "DataHarmonizationManager")
            
            # Process each field
            for field_name in fields_to_process:
                try:
                    # Check if field exists in target DataFrame
                    if field_name in target_columns_lower:
                        
                        # Get specific rules for this field and table
                        field_specific_rules = filtered_field_rules.filter(
                            (F.col("table_name") == normalized_table_name) & 
                            (F.col("field_type") == (self.FIELD_TYPE_HARMONIZATION if process_type == self.PROCESS_TYPE_HARMONIZATION 
                                                   else self.FIELD_TYPE_REMEDIATION)) & 
                            (F.col("reference_field_normalized") == field_name)
                        ).distinct()
                        
                        # Apply transformation if rules exist
                        if field_specific_rules.count() > 0:
                            logger.info(f"Processing field: {field_name} with {process_type}", 
                                       "DataHarmonizationManager")
                            
                            result_df = self.execute_field_transformation(
                                df=result_df,
                                target_field=field_name,
                                harmonization_rules=field_specific_rules,
                                process_type=process_type
                            )
                            processed_fields_count += 1
                            
                            logger.success(f"Successfully processed field: {field_name}", 
                                          "DataHarmonizationManager")
                        else:
                            logger.info(f"No {process_type} rules found for field: {field_name}", 
                                       "DataHarmonizationManager")
                    else:
                        logger.info(f"Field {field_name} not found in target DataFrame, skipping", 
                                   "DataHarmonizationManager")
                        
                except Exception as field_error:
                    logger.error(f"Error processing field {field_name}: {str(field_error)}", 
                                "DataHarmonizationManager")
                    # Continue processing other fields
                    continue
            
            final_record_count = result_df.count()
            logger.success(f"{process_type.title()} orchestration completed successfully", 
                          "DataHarmonizationManager")
            logger.info(f"Summary: Processed {processed_fields_count} fields, Final records: {final_record_count}", 
                       "DataHarmonizationManager")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error in {process_type} orchestration: {str(e)}", 
                        "DataHarmonizationManager")
            raise

    # Convenience methods for backward compatibility and ease of use
    def harmonize_data(self, table_name: str, harmonization_fields_df: DataFrame, 
                      harmonization_rules_df: DataFrame, target_df: DataFrame) -> DataFrame:
        """
        Convenience method for data harmonization.
        
        Args:
            table_name: Name of the table being processed
            harmonization_fields_df: DataFrame containing field definitions
            harmonization_rules_df: DataFrame containing transformation rules
            target_df: DataFrame to be harmonized
            
        Returns:
            DataFrame: Harmonized DataFrame
        """
        return self.orchestrate_harmonization(
            table_name=table_name,
            harmonization_fields_df=harmonization_fields_df,
            harmonization_rules_df=harmonization_rules_df,
            target_df=target_df,
            process_type=self.PROCESS_TYPE_HARMONIZATION
        )
    
    def remediate_data(self, table_name: str, harmonization_fields_df: DataFrame, 
                      harmonization_rules_df: DataFrame, target_df: DataFrame) -> DataFrame:
        """
        Convenience method for data remediation.
        
        Args:
            table_name: Name of the table being processed
            harmonization_fields_df: DataFrame containing field definitions
            harmonization_rules_df: DataFrame containing transformation rules
            target_df: DataFrame to be remediated
            
        Returns:
            DataFrame: Remediated DataFrame
        """
        return self.orchestrate_harmonization(
            table_name=table_name,
            harmonization_fields_df=harmonization_fields_df,
            harmonization_rules_df=harmonization_rules_df,
            target_df=target_df,
            process_type=self.PROCESS_TYPE_REMEDIATION
        )
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")
from pyspark.sql.functions import col,lit
df.columns
df_homologaciones_principal = spark.createDataFrame(df)
# Initialize the manager
harmonization_manager = DataHarmonizationManager()
    
# Method 1: Using orchestrate_harmonization directly
df_result = harmonization_manager.orchestrate_harmonization(
        table_name="datos_basicos_stg",
        harmonization_fields_df=df_homologaciones_principal,
        harmonization_rules_df=df_homologaciones_principal,
        target_df=datos_basicos_stg,
        process_type="harmonization"  # or "remediation"
)
df_result.filter(col("cod_tipo_identificacion" )  == "CC").show(3)
from incremental_control_manager import IncrementalControlManager
s3_handler = S3Handler(BUCKET_BRONZE, glueContext)
control_manager = IncrementalControlManager(spark, glueContext,s3_handler)
# 1. Verificar control table
control_summary = control_manager.get_control_table_summary()
control_summary.show()
#CARDIFF/SMB_CARDIFF_CUSTOMER/
control_prefix = control_manager.control_path.replace(f's3://{control_manager.s3_handler.bucket_target}/', '')
print(control_prefix)
# EJECUTAR ESTE CÓDIGO:
from pyspark.sql.functions import col, lit, concat_ws, trim, coalesce, to_date
print("===== CONTROL TABLE DEBUG =====")
df_control = control_manager._load_control_table()
print(f"Control table records found: {df_control.count()}")

if df_control.count() > 0:
    print("All control records:")
    df_control.show(truncate=False)
    
    # Filtro específico para Cardiff
    cardiff_records = df_control.filter(
        (col("layer") == "BRONZE") & 
        (col("table_name") == "SMB_CARDIFF_CUSTOMER")
    )
    print(f"Cardiff records found: {cardiff_records.count()}")
    cardiff_records.show(truncate=False)

# Test del get_last_incremental_id
last_id = control_manager.get_last_incremental_id("BRONZE", "SMB_CARDIFF_CUSTOMER")
print(f"Last incremental ID returned: {last_id}")
# EJECUTAR ESTE DEBUGGING:
print("===== UPSERT FILTER DEBUG =====")

df_control = control_manager._load_control_table()
print("BEFORE filter:")
df_control.show()

# Test del filter que debería ELIMINAR registros de Cardiff
layer = "BRONZE"
table_name = "SMB_CARDIFF_CUSTOMER"

df_without_current = df_control.filter(
    ~((col("layer") == layer.upper()) & (col("table_name") == table_name.upper()))
)

print("AFTER filter (should be EMPTY for Cardiff):")
print(f"Records after filter: {df_without_current.count()}")
df_without_current.show()

# Verificar también el filter positivo
df_cardiff_only = df_control.filter(
    (col("layer") == layer.upper()) & (col("table_name") == table_name.upper())
)
print("Cardiff records that SHOULD be removed:")
print(f"Cardiff records found: {df_cardiff_only.count()}")
df_cardiff_only.show()
# EJECUTAR ESTE CÓDIGO para ver la estructura real de S3:
import boto3

s3_client = boto3.client('s3')
bucket = "alfa-datalake-dev-bronce"  # o el bucket correcto donde está control table
prefix = "CONTROL/INCREMENTAL_CONTROL/"

print("===== S3 FILES STRUCTURE =====")
response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

if 'Contents' in response:
    for obj in response['Contents']:
        file_key = obj['Key']
        file_size = obj['Size']
        last_modified = obj['LastModified']
        print(f"File: {file_key}")
        print(f"  Size: {file_size} bytes")
        print(f"  Modified: {last_modified}")
        print("-" * 50)
        
    print(f"Total files: {len(response['Contents'])}")
else:
    print("No files found in control path")

# Verificar si hay múltiples archivos Parquet
parquet_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]
print(f"Parquet files: {len(parquet_files)}")
for pf in parquet_files:
    print(f"  - {pf}")
# EJECUTAR CLEANUP MANUAL:
print("===== MANUAL S3 CLEANUP =====")

# Delete todos los archivos de control existentes
control_prefix = "CONTROL/INCREMENTAL_CONTROL/"
s3_handler.delete_bucket_files(control_prefix)

print("Existing control files deleted")

# Verificar que se eliminaron
response = s3_client.list_objects_v2(Bucket="alfa-datalake-dev-bronce", Prefix=control_prefix)
remaining_files = len(response.get('Contents', []))
print(f"Remaining files after cleanup: {remaining_files}")
# CREAR CONTROL TABLE CON ÚNICO REGISTRO CORRECTO:
from datetime import datetime

new_record_data = [{
    "layer": "BRONZE",
    "table_name": "SMB_CARDIFF_CUSTOMER",
    "incremental_id": 4427771,  # El último ID correcto
    "last_execution_date": datetime.now(),
    "partitions": ""
}]

schema = control_manager._get_control_table_schema()
df_clean_control = spark.createDataFrame(new_record_data, schema)

# Guardar usando el método fijo
control_manager._save_control_table(df_clean_control)

print("Clean control table created")
from pyspark.sql.functions import col, lit, min, max, count
print("===== CURRENT STATE ANALYSIS =====")

# 1. Verificar estado actual del control table
last_id = control_manager.get_last_incremental_id("BRONZE", "SMB_CARDIFF_CUSTOMER")
print(f"Current last INCREMENTAL_ID: {last_id}")

# 2. Analizar distribución de IDs en Bronze
df_bronze = spark.read.option("recursiveFileLookup", "true").parquet(
    "s3://alfa-datalake-dev-bronce/CARDIFF/SMB_CARDIFF_CUSTOMER/"
)

id_stats = df_bronze.agg(
    min("INCREMENTAL_ID").alias("min_id"),
    max("INCREMENTAL_ID").alias("max_id"),
    count("INCREMENTAL_ID").alias("total_records")
).collect()[0]

print(f"Bronze data - Min ID: {id_stats['min_id']}, Max ID: {id_stats['max_id']}, Total: {id_stats['total_records']:,}")

# 3. Identificar punto de rollback (antes del último delta)
rollback_target = 4427771  # Después de la migración inicial
records_after_rollback = df_bronze.filter(col("INCREMENTAL_ID") > rollback_target).count()
print(f"Records to remove (ID > {rollback_target}): {records_after_rollback:,}")
print("===== BRONZE DATA ROLLBACK =====")

# 1. Filter datos hasta el punto de rollback
df_rollback = df_bronze.filter(col("INCREMENTAL_ID") <= rollback_target)
rollback_count = df_rollback.count()
print(f"Records to keep (ID <= {rollback_target}): {rollback_count:,}")

# 2. Cache datos de rollback en memoria
print("Caching rollback data in memory...")
df_rollback.cache()
rollback_count_cached = df_rollback.count()  # Force materialization
print(f"✅ Rollback data cached: {rollback_count_cached:,} records")

# 3. DELETE existing Bronze data
bronze_prefix = "CARDIFF/SMB_CARDIFF_CUSTOMER/"
print(f"Deleting existing Bronze data: {bronze_prefix}")
s3_handler.delete_bucket_files(bronze_prefix)
from awsglue import DynamicFrame
# 4. Save rolled-back data
print("Saving rolled-back data to Bronze...")
df_dynamic_rollback = DynamicFrame.fromDF(df_rollback, glueContext, "rollback_data")

s3_handler.save_files_to_s3(
    df=df_dynamic_rollback,
    s3_target_path="s3://alfa-datalake-dev-bronce/CARDIFF/SMB_CARDIFF_CUSTOMER/",
    format="parquet",
    mode="overwrite",
    partition_keys=[]  # Si aplica
)

print("✅ Bronze data rollback completed")

# 5. Cleanup cache
df_rollback.unpersist()
# 2. Verificar counts reales en Bronze
df_bronze = read_full_parquet("s3://alfa-datalake-dev-bronce/CARDIFF/SMB_CARDIFF_CUSTOMER/")
print(f"Total records in Bronze: {df_bronze.count()}")
print(f"Records with INCREMENTAL_ID: {df_bronze.filter(col('INCREMENTAL_ID').isNotNull()).count()}")
#print(f"Max INCREMENTAL_ID: {df_bronze.agg(max('INCREMENTAL_ID')).collect()[0][0]}")
result = df_bronze.agg(spark_max("incremental_id").alias("max_id")).collect()
print(f"Max INCREMENTAL_ID: {result[0][0]}")
# 3. Verificar duplicados por business key
business_key = "CUSTOMER_NO"  # Cardiff business key
duplicates = df_bronze.groupBy(business_key).count().filter(col("count") > 1)
print(f"Duplicate business keys: {duplicates.count()}")
cardiff_processor = CardiffStagingProcessor(
    spark_session=spark,
    glue_context=glueContext,
    bucket_bronze=BUCKET_BRONZE,
    bucket_silver=BUCKET_SILVER)

alfaprd_processor = AlfprdStagingProcessor(
    spark_session=spark,
    glue_context=glueContext,
    bucket_bronze=BUCKET_BRONZE,
    bucket_silver=BUCKET_SILVER)

acsele_processor = AcseleStagingProcessor(
    spark_session=spark,
    glue_context=glueContext,
    bucket_bronze=BUCKET_BRONZE,
    bucket_silver=BUCKET_SILVER
)
# Process worflows
cardiff_processor.cardiff_workflow_stg(JOB_ID)
alfaprd_processor.alfaprd_workflow_stg(JOB_ID)
acsele_processor.acsele_workflow_stg(JOB_ID)
s3_handler_silver = S3Handler(BUCKET_SILVER, glueContext)
s3_handler_bronze = S3Handler(BUCKET_BRONZE, glueContext)
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from awsglue import DynamicFrame
from pyspark.sql.functions import col, lit, concat_ws, trim, coalesce, to_date
from pyspark.sql.types import StringType, IntegerType, DateType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import current_date, max as spark_max, row_number
# Configuraciones Spark optimizadas para lectura masiva
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true") 
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
def read_full_parquet(s3_target_path:str = None, recursive_file_lookup:str = "true",
                          merge_schema:str = "false", path_glob_filter:str = "*.parquet"):
    return spark.read.option("recursiveFileLookup", recursive_file_lookup)\
        .option("mergeSchema", merge_schema)\
        .option("pathGlobFilter", path_glob_filter)\
        .parquet(s3_target_path)
def create_schema():
    """
        Create schema for fuentes_clientes table.
        
        Returns:
            StructType: Table schema definition
    """
    return StructType([
            StructField("codigo_fuente", IntegerType(), False),
            StructField("fuente", StringType(), False)
        ])
    
def generate_fuentes_data():
    """
        Generate basic fuentes data.
        
        Returns:
            list: List of tuples with fuentes data
    """
    return [
            (1, "ACSELE"),
            (2, "ALFPRD"),
            (3, "CARDIFF")
        ]
# Create schema
schema = create_schema()
# Generate data
data = generate_fuentes_data()
# Create DataFrame
df = spark.createDataFrame(data, schema)
# Set current date for fecha_creacion
df = df.withColumn("fecha_creacion", current_date())
df_dynamic = DynamicFrame.fromDF(df, glueContext, "fuentes_clientes")
s3_handler = S3Handler(BUCKET_SILVER, glueContext)
s3_prefix, s3_target_path = s3_handler.generate_s3_paths('CLIENTES', 'ORIGEN', 'CODIGO_FUENTES')
response = s3_handler.save_files_to_s3(df_dynamic, s3_target_path, format="parquet", mode="overwrite")
S3_PATH_SMB_CARDIFF= f's3://{BUCKET_BRONZE}/CARDIFF/SMB_CARDIFF_CUSTOMER/'
cardiff_processor = CardiffStagingProcessor(spark)
df_smb_cardiff = read_full_parquet(S3_PATH_SMB_CARDIFF)
df_cardiff_normalized = cardiff_processor.normalize_cardiff_data(df_smb_cardiff)
datos_basicos_schema = DatosBasicosSchema(spark, glueContext, s3_handler_silver)
s3_prefix, s3_target_path = s3_handler_silver.generate_s3_paths('CLIENTES', 'DATOS_BASICOS', 'STG')
datos_basicos_schema.create_empty_schema(s3_target_path)
df_cardiff_datos_basicos_stg = df_cardiff_normalized.select(
    # Campos disponibles en Cardiff
    col("ID_TYPE_NAME").alias("cod_tipo_identificacion"),
    col("ID_CO").alias("numero_documento"),
    col("FIRST_NAME").alias("primer_nombre"),
    col("MIDDLE_NAME").alias("segundo_nombre"),
    col("FIRST_SURNAME").alias("primer_apellido"),
    col("SECOND_SURNAME").alias("segundo_apellido"),
    col("FULLNAME").alias("nombre_completo"),
    col("CREATION_DATE").cast("date").alias("fecha_vinculacion"),
    col("CUSTOMER_TYPE").alias("cod_tipo_persona"),
    # Campos que no están en Cardiff - valores vacíos/null
    lit(None).cast("string").alias("ciudad_expedicion_documento"),
    lit(None).cast("date").alias("fecha_expedicion_documento"),
    lit(None).cast("int").alias("cod_estado"),
    lit(None).cast("string").alias("cod_tipo_empresa"),
    lit(None).cast("string").alias("tipo_empresa")
)
df_cardiff_datos_basicos_stg.printSchema()
summary = datos_basicos_schema.insert_data(
    df_new_data=df_cardiff_datos_basicos_stg,
    s3_path=s3_target_path,
    cod_fuente=COD_FUENTES.get("CARDIFF",3),  # CARDIFF
    job_id=JOB_ID
)
datos_sociodemo_schema = DatosSociodemograficosSchema(spark, glueContext, s3_handler_silver)
s3_prefix, s3_target_path = s3_handler_silver.generate_s3_paths('CLIENTES', 'DATOS_SOCIODEMOGRAFICOS', 'STG')
datos_sociodemo_schema.create_empty_schema(s3_target_path)
df_cardiff_datos_sociodemograficos_stg = df_cardiff_normalized.select(
    col("ID_TYPE_NAME").alias("cod_tipo_identificacion").cast("string"),
    col("ID_CO").alias("numero_documento").cast("string"),
    col("BIRTH_DATE").alias("fecha_nacimiento").cast(DateType()),
    col("BIRTH_YEARS_OLD").alias("edad").cast("string"),
    col("BIRTH_CITY_CODE").alias("cod_ciudad_nacimiento").cast("string"),
    col("BIRTH_STATE").alias("cod_departamento_nacimiento").cast("string"),
    col("BIRTH_COUNTRY_CODE").alias("cod_pais_nacimiento").cast("string"),
    col("BIRTH_CITY_NAME").alias("ciudad_nacimiento").cast("string"),
    col("BIRTH_STATE_NAME").alias("departamento_nacimiento").cast("string"),
    col("BIRTH_COUNTRY_NAME").alias("pais_nacimiento").cast("string"),
    col("NATIONALITY").alias("nacionalidad").cast("string"),
    col("GENDER").alias("genero").cast("string"),
    col("MARITAL_STATUS").alias("cod_estado_civil").cast("string"),
    lit(None).cast("string").alias("cod_nivel_estudios"),
    lit(None).cast("string").alias("nivel_estudios"),
    lit(None).cast("string").alias("cod_profesion"),
    lit(None).cast("string").alias("profesion"),
    col("OCCUPATION_CODE").alias("cod_ocupacion").cast("string"),
    col("OCCUPATION_NAME").alias("ocupacion").cast("string"),
    lit(None).cast("string").alias("cod_actividad_economica"),
    col("NUM_CHILDS").alias("numero_hijos").cast(IntegerType()),
    lit(None).cast("string").alias("cod_tipo_sociedad"),
    lit(None).cast("string").alias("tipo_id_empresa"),
    lit(None).cast("string").alias("nit_empresa"),
    lit(None).cast("string").alias("afiliacion_id"),
    lit(None).cast("string").alias("cod_cargo_empresa"),
    lit(None).cast(DateType()).alias("fecha_ingreso_empresa"),
    lit(None).cast("string").alias("ciudad_expedicion_documento"),
    lit(None).cast(DateType()).alias("fecha_expedicion_documento"),
    lit(None).cast(IntegerType()).alias("cod_estado"),
    lit(None).cast("string").alias("cod_tipo_empresa"),
    lit(None).cast("string").alias("tipo_empresa")
)

df_cardiff_datos_sociodemograficos_stg.printSchema()
df_cardiff_datos_sociodemograficos_stg.count()
s3_target_path
summary = datos_sociodemo_schema.insert_data(
    df_new_data=df_cardiff_datos_sociodemograficos_stg,
    s3_path=s3_target_path,
    cod_fuente=COD_FUENTES.get("CARDIFF",3),  # CARDIFF
    job_id=JOB_ID
)
datos_contactabilidad_schema = DatosUbicacionContactabilidadSchema(spark, glueContext, s3_handler_silver)
s3_prefix, s3_target_path = s3_handler_silver.generate_s3_paths('CLIENTES', 'DATOS_UBICACION_Y_CONTACTABILIDAD', 'STG')
datos_contactabilidad_schema.create_empty_schema(s3_target_path)
df_cardiff_datos_contacto_stg = df_cardiff_normalized.select(
    # Campos disponibles en Cardiff
    col("ID_TYPE_NAME").alias("cod_tipo_identificacion"),
    col("ID_CO").alias("numero_documento"),
    col("MAIL").alias("correo_electronico"),
    col("ADDRESS_TYPE").alias("tipo_direccion"),
    col("ADDRESS_NAME").alias("direccion"),
    col("ADDRESS_NEIHBORHOOD").alias("barrio"),
    col("ADDRESS_CITY_CODE").alias("cod_ciudad"),
    col("ADDRESS_STATE_CODE").alias("cod_departamento"),
    col("ADDRESS_COUNTRY_NAME").alias("cod_pais"),
    col("ADDRESS_CITY_NAME").alias("ciudad"),
    col("ADDRESS_STATE_NAME").alias("departamento"),
    col("ADDRESS_COUNTRY_CODE").alias("pais"),
    col("PHONE_CODE").alias("indicativo"),
    coalesce(col("PHONE_NUMBER"), col("CELLPHONE")).alias("numero_telefono"),
    col("PHONE_EXT").alias("extension"),
    # Campos que no están en Cardiff - valores vacíos/null
    lit(None).cast("string").alias("tipo_telefono")
)
df_cardiff_datos_contacto_stg.printSchema()
summary = datos_contactabilidad_schema.insert_data(
    df_new_data=df_cardiff_datos_contacto_stg,
    s3_path=s3_target_path,
    cod_fuente=COD_FUENTES.get("CARDIFF",3),  # CARDIFF
    job_id=JOB_ID
)
alfaprd_processor = AlfprdStagingProcessor(spark)
############## ALFPRD ###################
s3_prefix = 's3:/'
s3_alfaprod_general_prefix = 'ALFAPRD/GENERAL'
s3_alfaprd_general_prefix = 'ALFAPROD/GENERAL'
S3_PATH_ALFAPRD_GENERAL_ACU_PERSONAS_NATURALES = f'{s3_prefix}/{BUCKET_BRONZE}/{s3_alfaprod_general_prefix}/ACU_PERSONAS_NATURALES/'
S3_PATH_ALFAPRD_GENERAL_ACU_PERSONAS_JURIDICAS = f'{s3_prefix}/{BUCKET_BRONZE}/{s3_alfaprd_general_prefix}/ALFAPROD_GENERAL_ACU_PERSONAS_JURIDICAS/'
S3_PATH_ALFAPRD_GENERAL_ACU_TIPOS_EMPRESA = f'{s3_prefix}/{BUCKET_BRONZE}/{s3_alfaprd_general_prefix}/ALFAPROD_GENERAL_ACU_TIPOS_EMPRESAS/'
S3_PATH_ALFAPRD_GENERAL_CGR_CUIDADES_DEPARTAMENTOS = f'{s3_prefix}/{BUCKET_BRONZE}/{s3_alfaprd_general_prefix}/ALFAPROD_GENERAL_CGR_CIUDADES_DEPARTAMENTOS/'
S3_PATH_ALFAPRD_GENERAL_ACU_NIVELES_EDUCATIVOS = f'{s3_prefix}/{BUCKET_BRONZE}/{s3_alfaprd_general_prefix}/ALFAPROD_GENERAL_ACU_NIVELES_EDUCATIVOS/'
S3_PATH_ALFAPRD_GENERAL_ACU_PROFESIONES = f'{s3_prefix}/{BUCKET_BRONZE}/{s3_alfaprd_general_prefix}/ALFAPROD_GENERAL_ACU_PROFESIONES/'
S3_PATH_ALFAPRD_ARPPROD_PAR_AFILIACIONES = f'{s3_prefix}/{BUCKET_BRONZE}/ALFAPROD/ARPPROD/ALFAPROD_ARPPROD_PAR_AFILIACIONES/'
S3_PATH_ALFAPRD_ARPPROD_PAR_TRABAJADORES = f'{s3_prefix}/{BUCKET_BRONZE}/ALFAPRD/ARPPROD/PAR_TRABAJADORES/'
S3_PATH_ALFAPRD_GENERAL_ACU_PERSONAS_LOCALIZACIONES = f'{s3_prefix}/{BUCKET_BRONZE}/{s3_alfaprd_general_prefix}/ALFAPROD_GENERAL_ACU_PERSONAS_LOCALIZACIONES/'
S3_PATH_ALFAPRD_GENERAL_CGR_DEPARTAMANETOS = f'{s3_prefix}/{BUCKET_BRONZE}/{s3_alfaprd_general_prefix}/ALFAPROD_GENERAL_CGR_DEPARTAMENTOS/'
S3_PATH_ALFAPRD_GENERAL_CGR_PAISES = f'{s3_prefix}/{BUCKET_BRONZE}/{s3_alfaprd_general_prefix}/ALFAPROD_GENERAL_CGR_PAISES/'
S3_PATH_ALFAPRD_GENERAL_CGR_ESTADOS = f'{s3_prefix}/{BUCKET_BRONZE}/{s3_alfaprd_general_prefix}/ALFAPROD_GENERAL_CGR_ESTADOS/'
S3_PATH_ALFAPRD_GENERAL_CGR_TIPOS_ID = f'{s3_prefix}/{BUCKET_BRONZE}/{s3_alfaprd_general_prefix}/ALFAPROD_GENERAL_CGR_TIPOS_IDENTIFICACIONES/'
S3_PATH_ALFAPRD_GENERAL_CGR_OCUPACIONES = f'{s3_prefix}/{BUCKET_BRONZE}/{s3_alfaprd_general_prefix}/ALFAPROD_GENERAL_CGR_OCUPACIONES/'
S3_PATH_ALFAPRD_GENERAL_CGR_ACT_ECO_CIUU = f'{s3_prefix}/{BUCKET_BRONZE}/{s3_alfaprd_general_prefix}/ALFAPROD_GENERAL_ACU_ACTIVIDADES_ECONOMICA_CIIU/'
S3_PATH_ALFAPRD_GENERAL_CGR_ESTADOS_CIVILES = f'{s3_prefix}/{BUCKET_BRONZE}/{s3_alfaprd_general_prefix}/ALFAPROD_GENERAL_CGR_ESTADOS_CIVILES/'
#################### SOURCES #########################
##### PRINCIPAL TABLES #####
df_alfaprd_general_acu_personas_naturales = alfaprd_processor.read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_ACU_PERSONAS_NATURALES)
df_alfaprd_general_acu_personas_juridicas = alfaprd_processor.read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_ACU_PERSONAS_JURIDICAS)
df_alfaprd_general_acu_tipos_empresa = alfaprd_processor.read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_ACU_TIPOS_EMPRESA)
df_alfaprd_general_cgr_cuidades_departamentos = alfaprd_processor.read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_CGR_CUIDADES_DEPARTAMENTOS)
df_alfaprd_general_acu_niveles_educativos = alfaprd_processor.read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_ACU_NIVELES_EDUCATIVOS)
df_alfaprd_general_acu_profesiones = alfaprd_processor.read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_ACU_PROFESIONES)
df_alfaprd_arpprod_par_afiliaciones = alfaprd_processor.read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_ARPPROD_PAR_AFILIACIONES)
df_alfaprd_arpprod_par_trabajadores = alfaprd_processor.read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_ARPPROD_PAR_TRABAJADORES)
df_alfaprd_general_acu_personas_localizaciones = alfaprd_processor.read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_ACU_PERSONAS_LOCALIZACIONES)
df_alfaprd_general_cgr_departamanetos = alfaprd_processor.read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_CGR_DEPARTAMANETOS)
df_alfaprd_general_cgr_paises = alfaprd_processor.read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_CGR_PAISES)
df_alfaprd_general_cgr_ocupaciones = alfaprd_processor.read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_CGR_OCUPACIONES)
df_alfaprd_general_cgr_estados_civiles = alfaprd_processor.read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_CGR_ESTADOS_CIVILES)
df_alfaprd_general_acu_actividades_economica_ciiu = alfaprd_processor.read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_CGR_ACT_ECO_CIUU)
df_alfaprd_general_cgr_tipos_identificaciones  = alfaprd_processor.read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_CGR_TIPOS_ID)
df_general_cgr_estados = alfaprd_processor.read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_CGR_ESTADOS)
## Procesar datos básicos PN
df_alfprd_pn_datos_basicos = alfaprd_processor.process_personas_naturales_datos_basicos(
    df_alfaprd_general_acu_personas_naturales, df_alfaprd_general_cgr_tipos_identificaciones, 
    df_alfaprd_general_cgr_cuidades_departamentos, df_alfaprd_general_cgr_departamanetos,
    df_general_cgr_estados
)
df_alfprd_pn_demograficos = alfaprd_processor.process_personas_naturales_demograficos(
       df_alfaprd_general_acu_personas_naturales,
       df_alfaprd_general_cgr_tipos_identificaciones,
       df_alfaprd_general_cgr_cuidades_departamentos,
       df_alfaprd_general_cgr_departamanetos,
       df_alfaprd_general_cgr_paises,
       df_alfaprd_general_cgr_estados_civiles,
       df_alfaprd_general_acu_niveles_educativos,
       df_alfaprd_general_acu_actividades_economica_ciiu,
       df_alfaprd_general_cgr_ocupaciones,
       df_alfaprd_general_acu_profesiones,
       df_alfaprd_arpprod_par_afiliaciones,
       df_alfaprd_arpprod_par_trabajadores,
       df_alfaprd_general_acu_personas_juridicas
   )
# Contactabilidad PN
df_alfprd_pn_contactabilidad = alfaprd_processor.process_personas_naturales_contactabilidad(
       df_alfaprd_general_acu_personas_naturales,
       df_alfaprd_general_acu_personas_localizaciones,
       df_alfaprd_general_cgr_cuidades_departamentos,
       df_alfaprd_general_cgr_departamanetos,
       df_alfaprd_general_cgr_paises
)
# Datos básicos PJ
df_alfprd_pj_datos_basicos = alfaprd_processor.process_personas_juridicas_datos_basicos(
       df_alfaprd_general_acu_personas_juridicas,
       df_alfaprd_general_cgr_tipos_identificaciones,
       df_general_cgr_estados,
       df_alfaprd_general_acu_tipos_empresa
)
# Demográficos PJ
df_alfprd_pj_demograficos = alfaprd_processor.process_personas_juridicas_demograficos(
       df_alfaprd_general_acu_personas_juridicas,
       df_alfaprd_general_cgr_tipos_identificaciones,
       df_alfaprd_general_cgr_cuidades_departamentos,
       df_alfaprd_general_cgr_departamanetos,
       df_alfaprd_general_cgr_paises,
       df_alfaprd_general_acu_actividades_economica_ciiu
)
# Ubicación PJ
df_alfprd_pj_ubicacion = alfaprd_processor.process_personas_juridicas_ubicacion(
       df_alfaprd_general_acu_personas_juridicas,
       df_alfaprd_general_acu_personas_localizaciones,
       df_alfaprd_general_cgr_cuidades_departamentos,
       df_alfaprd_general_cgr_departamanetos,
       df_alfaprd_general_cgr_paises
)
# ===== CONSOLIDAR TODO =====
df_macro_alfaprd, summary = alfaprd_processor.consolidate_alfprd_data(
       df_alfprd_pn_datos_basicos,
       df_alfprd_pn_demograficos,
       df_alfprd_pn_contactabilidad,
       df_alfprd_pj_datos_basicos,
       df_alfprd_pj_demograficos,
       df_alfprd_pj_ubicacion
)
df_macro_alfaprd.printSchema()
datos_basicos_schema = DatosBasicosSchema(spark, glueContext, s3_handler_silver)
s3_prefix, s3_target_path = s3_handler_silver.generate_s3_paths('CLIENTES', 'DATOS_BASICOS', 'STG')
#datos_basicos_schema.create_empty_schema(s3_target_path)
## Procesar datos básicos PN
df_alfprd_pn_datos_basicos = alfaprd_processor.process_personas_naturales_datos_basicos(
    df_alfaprd_general_acu_personas_naturales, df_alfaprd_general_cgr_tipos_identificaciones, 
    df_alfaprd_general_cgr_cuidades_departamentos, df_alfaprd_general_cgr_departamanetos,
    df_general_cgr_estados
)
# Procesar datos básicos PJ
df_alfprd_pj_datos_basicos = alfaprd_processor.process_personas_juridicas_datos_basicos(
       df_alfaprd_general_acu_personas_juridicas,
       df_alfaprd_general_cgr_tipos_identificaciones,
       df_general_cgr_estados,
       df_alfaprd_general_acu_tipos_empresa
)
# =====================================================================
# PERSONAS NATURALES (PN) - Agregar columnas que solo tienen las PJ
# =====================================================================
df_processed_pn_extended = df_alfprd_pn_datos_basicos \
    .withColumn("razon_social", lit(None).cast("string")) \
    .withColumn("cod_tipo_empresa", lit(None).cast("string")) \
    .withColumn("tipo_empresa", lit(None).cast("string")) \
    .withColumn("TIPO_PERSONA", lit("NATURAL"))
# =====================================================================
# PERSONAS JURÍDICAS (PJ) - Agregar columnas que solo tienen las PN
# =====================================================================
df_processed_pj_extended = df_alfprd_pj_datos_basicos \
    .withColumn("PRIMER_APELLIDO", lit(None).cast("string")) \
    .withColumn("SEGUNDO_APELLIDO", lit(None).cast("string")) \
    .withColumn("PRIMER_NOMBRE", lit(None).cast("string")) \
    .withColumn("SEGUNDO_NOMBRE", lit(None).cast("string")) \
    .withColumn("FEC_EXPEDICION_DOCUMENTO", lit(None).cast("date")) \
    .withColumn("COD_CIUDAD_EXPEDICION_DOCU", lit(None).cast("string")) \
    .withColumn("ciudad_expedicion_docu", lit(None).cast("string")) \
    .withColumn("COD_DEPARTAMENTO", lit(None).cast("string")) \
    .withColumn("departamento_expe_docu", lit(None).cast("string")) \
    .withColumn("TIPO_PERSONA", lit("JURIDICA"))
df_datos_basicos_processed = df_processed_pn_extended.union(df_processed_pj_extended)
print(f"Total registros: {df_datos_basicos_processed.count():,}")
df_datos_basicos_processed.groupBy("TIPO_PERSONA").count().show()
df_datos_basicos_processed.printSchema()
alfaprd_datos_basicos = df_datos_basicos_processed.select(
    col("COD_TIPO_IDENTIFICACION").alias("cod_tipo_identificacion"),
    col("NID_NUMERO_IDENTIFICACION").cast("string").alias("numero_documento"),
    col("PRIMER_NOMBRE").alias("primer_nombre"),
    col("SEGUNDO_NOMBRE").alias("segundo_nombre"),
    col("PRIMER_APELLIDO").alias("primer_apellido"),
    col("SEGUNDO_APELLIDO").alias("segundo_apellido"),
    col("RAZON_SOCIAL").alias("nombre_completo"),
    col("FEC_EXPEDICION_DOCUMENTO").cast("date").alias("fecha_expedicion_documento"),
    col("COD_CIUDAD_EXPEDICION_DOCU").alias("ciudad_expedicion_documento"),
    col("FEC_REGISTRO").cast("date").alias("fecha_vinculacion"),
    col("COD_ESTADO").cast("int").alias("cod_estado"),
    col("COD_TIPO_EMPRESA").alias("cod_tipo_empresa"),
    col("NOMBRE").alias("tipo_empresa"),
    # ADD NULL VALUES 
    lit(None).cast("string").alias("cod_tipo_persona")
)
alfaprd_datos_basicos.printSchema()
summary = datos_basicos_schema.insert_data(
    df_new_data=alfaprd_datos_basicos,
    s3_path=s3_target_path,
    cod_fuente=COD_FUENTES.get("ALFAPRD",2),  # CARDIFF
    job_id=JOB_ID
)
datos_sociodemo_schema = DatosSociodemograficosSchema(spark, glueContext, s3_handler_silver)
s3_prefix, s3_target_path = s3_handler_silver.generate_s3_paths('CLIENTES', 'DATOS_SOCIODEMOGRAFICOS', 'STG')
#datos_sociodemo_schema.create_empty_schema(s3_target_path)
########## ALFPRD PN DATOS SOCIODEMOGRAFICOS #########
df_alfprd_pn_demograficos = alfaprd_processor.process_personas_naturales_demograficos(
       df_alfaprd_general_acu_personas_naturales,
       df_alfaprd_general_cgr_tipos_identificaciones,
       df_alfaprd_general_cgr_cuidades_departamentos,
       df_alfaprd_general_cgr_departamanetos,
       df_alfaprd_general_cgr_paises,
       df_alfaprd_general_cgr_estados_civiles,
       df_alfaprd_general_acu_niveles_educativos,
       df_alfaprd_general_acu_actividades_economica_ciiu,
       df_alfaprd_general_cgr_ocupaciones,
       df_alfaprd_general_acu_profesiones,
       df_alfaprd_arpprod_par_afiliaciones,
       df_alfaprd_arpprod_par_trabajadores,
       df_alfaprd_general_acu_personas_juridicas
   )
########## ALFPRD PJ DATOS SOCIODEMOGRAFICOS #########
df_alfprd_pj_demograficos = alfaprd_processor.process_personas_juridicas_demograficos(
       df_alfaprd_general_acu_personas_juridicas,
       df_alfaprd_general_cgr_tipos_identificaciones,
       df_alfaprd_general_cgr_cuidades_departamentos,
       df_alfaprd_general_cgr_departamanetos,
       df_alfaprd_general_cgr_paises,
       df_alfaprd_general_acu_actividades_economica_ciiu
)
# =====================================================================
# PERSONAS NATURALES (PN) - Agregar columnas que solo tienen las PJ
# =====================================================================
df_processed_pn_socio_extended = df_alfprd_pn_demograficos \
    .withColumn("fec_constitucion", lit(None).cast("date")) \
    .withColumn("cod_ciudad", lit(None).cast("string")) \
    .withColumn("TIPO_PERSONA", lit("NATURAL"))
# =====================================================================
# PERSONAS JURÍDICAS (PJ) - Agregar columnas que solo tienen las PN
# =====================================================================
df_processed_pj_socio_extended = df_alfprd_pj_demograficos \
    .withColumn("FEC_NACIMIENTO", lit(None).cast("date")) \
    .withColumn("COD_CIUDAD_NACIMIENTO", lit(None).cast("string")) \
    .withColumn("NACIONALIDAD", lit(None).cast("string")) \
    .withColumn("GENERO", lit(None).cast("string")) \
    .withColumn("COD_ESTADO_CIVIL", lit(None).cast("string")) \
    .withColumn("estado_civil", lit(None).cast("string")) \
    .withColumn("COD_NIVEL_EDUCATIVO", lit(None).cast("string")) \
    .withColumn("nivel_educativo", lit(None).cast("string")) \
    .withColumn("COD_PROFESION", lit(None).cast("string")) \
    .withColumn("profesion", lit(None).cast("string")) \
    .withColumn("COD_OCUPACION", lit(None).cast("string")) \
    .withColumn("ocupacion", lit(None).cast("string")) \
    .withColumn("cod_tipo_identificacion_emp", lit(None).cast("string")) \
    .withColumn("nid_numero_identificacion_tra", lit(None).cast("string")) \
    .withColumn("nombre_empresa", lit(None).cast("string")) \
    .withColumn("cod_cargo", lit(None).cast("string")) \
    .withColumn("fecha_ingreso_empresa", lit(None).cast("date")) \
    .withColumn("NID_AFILIACION", lit(None).cast("decimal")) \
    .withColumn("TIPO_PERSONA", lit("JURIDICA"))
# =====================================================================
# NORMALIZAR NOMBRES DE COLUMNAS ANTES DEL UNION
# =====================================================================

# Normalizar PN - renombrar columnas para que coincidan con PJ
df_processed_pn_socio_normalized = df_processed_pn_socio_extended \
    .withColumnRenamed("COD_CIUDAD_NACIMIENTO", "cod_ciudad_origen") \
    .withColumnRenamed("cod_ciudad", "cod_ciudad_constitucion")

# Normalizar PJ - renombrar columnas para que coincidan con PN  
df_processed_pj_socio_normalized = df_processed_pj_socio_extended \
    .withColumnRenamed("cod_ciudad", "cod_ciudad_constitucion") \
    .withColumnRenamed("COD_CIUDAD_NACIMIENTO", "cod_ciudad_origen")

# =====================================================================
# UNION FINAL SOCIODEMOGRÁFICOS
# =====================================================================
df_sociodemograficos_processed = df_processed_pn_socio_normalized.union(df_processed_pj_socio_normalized)
print(f"Total registros: {df_sociodemograficos_processed.count():,}")
df_sociodemograficos_processed.groupBy("TIPO_PERSONA").count().show()
df_sociodemograficos_processed.printSchema()
df_alfprd_sociodemograficos = df_sociodemograficos_processed.select(
    col("COD_TIPO_IDENTIFICACION").alias("cod_tipo_identificacion"),
    col("NID_NUMERO_IDENTIFICACION").cast("string").alias("numero_documento"),
    col("FEC_NACIMIENTO").cast("date").alias("fecha_nacimiento"),
    lit(None).cast("string").alias("edad"),
    col("cod_ciudad_origen").alias("cod_ciudad_nacimiento"),
    col("COD_DEPARTAMENTO").alias("cod_departamento_nacimiento"),
    lit(None).cast("string").alias("cod_pais_nacimiento"),
    col("nombre").alias("ciudad_nacimiento"),
    lit(None).cast("string").alias("departamento_nacimiento"),
    lit(None).cast("string").alias("pais_nacimiento"),
    col("NACIONALIDAD").alias("nacionalidad"),
    col("GENERO").alias("genero"),
    col("COD_ESTADO_CIVIL").alias("cod_estado_civil"),
    col("COD_NIVEL_EDUCATIVO").alias("cod_nivel_estudios"),
    col("nivel_educativo").alias("nivel_estudios"),
    col("COD_PROFESION").alias("cod_profesion"),
    col("profesion").alias("profesion"),
    col("COD_OCUPACION").alias("cod_ocupacion"),
    col("ocupacion").alias("ocupacion"),
    col("COD_ACTIVIDAD_ECONOMICA_CIIU").alias("cod_actividad_economica"),
    lit(None).cast("int").alias("numero_hijos"),
    lit(None).cast("string").alias("cod_tipo_sociedad"),
    col("cod_tipo_identificacion_emp").alias("tipo_id_empresa"),
    col("nid_numero_identificacion_tra").alias("nit_empresa"),
    col("NID_AFILIACION").cast("string").alias("afiliacion_id"),
    col("cod_cargo").alias("cod_cargo_empresa"),
    col("fecha_ingreso_empresa").cast("date").alias("fecha_ingreso_empresa")
)
s3_target_path
summary = datos_sociodemo_schema.insert_data(
    df_new_data=df_alfprd_sociodemograficos,
    s3_path=s3_target_path,
    cod_fuente=COD_FUENTES.get("ALFAPRD",2),  # CARDIFF
    job_id=JOB_ID
)
datos_contactabilidad_schema = DatosUbicacionContactabilidadSchema(spark, glueContext, s3_handler_silver)
s3_prefix, s3_target_path = s3_handler_silver.generate_s3_paths('CLIENTES', 'DATOS_UBICACION_Y_CONTACTABILIDAD', 'STG')
#datos_contactabilidad_schema.create_empty_schema(s3_target_path)
# Contactabilidad PN
df_alfprd_pn_contactabilidad = alfaprd_processor.process_personas_naturales_contactabilidad(
       df_alfaprd_general_acu_personas_naturales,
       df_alfaprd_general_acu_personas_localizaciones,
       df_alfaprd_general_cgr_cuidades_departamentos,
       df_alfaprd_general_cgr_departamanetos,
       df_alfaprd_general_cgr_paises
)
# Ubicación PJ
df_alfprd_pj_contactabilidad = alfaprd_processor.process_personas_juridicas_contactabilidad(
       df_alfaprd_general_acu_personas_juridicas,
       df_alfaprd_general_acu_personas_localizaciones,
       df_alfaprd_general_cgr_cuidades_departamentos,
       df_alfaprd_general_cgr_departamanetos,
       df_alfaprd_general_cgr_paises
)
df_contactabilidad_alfaprd = alfaprd_processor.consolidate_datos_contactabilidad_alfaprd_data(
    df_alfprd_pn_contactabilidad,
    df_alfprd_pj_contactabilidad
)
df_contactabilidad_alfaprd.printSchema()
df_contactabilidad_alfaprd_final = df_contactabilidad_alfaprd.select(
    col("COD_TIPO_IDENTIFICACION").alias("cod_tipo_identificacion"),
    col("NID_NUMERO_IDENTIFICACION").cast("string").alias("numero_documento"),
    col("e_mail").alias("correo_electronico"),
    col("tipo_ubicacion").alias("tipo_direccion"),
    col("direccion"),
    col("barrio_vereda").alias("barrio"),
    col("cod_ciudad"),
    col("cod_departamento"),
    col("cod_pais"),
    col("ciudad"),
    col("departamento"),
    col("pais"),
    lit(None).cast("string").alias("tipo_telefono"),
    col("indicativo"),
    col("numero_telefono"),
    col("extension_telefono").alias("extension")   
)
df_contactabilidad_alfaprd_final.printSchema()
s3_target_path
summary = datos_contactabilidad_schema.insert_data(
    df_new_data=df_contactabilidad_alfaprd_final,
    s3_path=s3_target_path,
    cod_fuente=COD_FUENTES.get("ALFAPRD",2),  # CARDIFF
    job_id=JOB_ID
)
#################### SOURCES #########################
##### PRINCIPAL TABLES #####
df_alfaprd_general_acu_personas_naturales = read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_ACU_PERSONAS_NATURALES)
df_alfaprd_general_acu_personas_juridicas = read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_ACU_PERSONAS_JURIDICAS)
df_alfaprd_general_acu_tipos_empresa = read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_ACU_TIPOS_EMPRESA)
df_alfaprd_general_cgr_cuidades_departamentos = read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_CGR_CUIDADES_DEPARTAMENTOS)
df_alfaprd_general_acu_niveles_educativos = read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_ACU_NIVELES_EDUCATIVOS)
df_alfaprd_general_acu_profesiones = read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_ACU_PROFESIONES)
df_alfaprd_arpprod_par_afiliaciones = read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_ARPPROD_PAR_AFILIACIONES)
df_alfaprd_arpprod_par_trabajadores = read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_ARPPROD_PAR_TRABAJADORES)
df_alfaprd_general_acu_personas_localizaciones = read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_ACU_PERSONAS_LOCALIZACIONES)
df_alfaprd_general_cgr_departamanetos = read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_CGR_DEPARTAMANETOS)
df_alfaprd_general_cgr_paises = read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_CGR_PAISES)
df_alfaprd_general_cgr_ocupaciones = read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_CGR_OCUPACIONES)
df_alfaprd_general_cgr_estados_civiles = read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_CGR_ESTADOS_CIVILES)
df_alfaprd_general_acu_actividades_economica_ciiu = read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_CGR_ACT_ECO_CIUU)
###### CATALOG TABLES ######
df_alfaprd_general_cgr_tipos_identificaciones  = read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_CGR_TIPOS_ID)
df_general_cgr_estados = read_full_parquet(s3_target_path=S3_PATH_ALFAPRD_GENERAL_CGR_ESTADOS)
# ================================================================================
# ALFPRD - PERSONAS NATURALES - DATOS BÁSICOS
# Query convertida a lógica Spark pura
# ================================================================================

print("📊 Procesando: Personas Naturales - Datos Básicos (ALFPRD)")

# Tabla principal: acu_personas_naturales 
df_personas_naturales = df_alfaprd_general_acu_personas_naturales

# LEFT JOIN 1: tipos_identificaciones
# ON PN.cod_tipo_identificacion = TD.cod_tipo_documento
df_result = df_personas_naturales.join(
    df_alfaprd_general_cgr_tipos_identificaciones,
    df_personas_naturales.COD_TIPO_IDENTIFICACION == df_alfaprd_general_cgr_tipos_identificaciones.COD_TIPO_DOCUMENTO,
    "left"
)

# LEFT JOIN 2: ciudades_departamentos  
# ON PN.cod_ciudad_expedicion_docu = CD.cod_ciudad AND PN.cod_departamento_expe_docu = CD.cod_departamento
df_result = df_result.join(
    df_alfaprd_general_cgr_cuidades_departamentos,
    (df_personas_naturales.COD_CIUDAD_EXPEDICION_DOCU == df_alfaprd_general_cgr_cuidades_departamentos.COD_CIUDAD) &
    (df_personas_naturales.COD_DEPARTAMENTO_EXPE_DOCU == df_alfaprd_general_cgr_cuidades_departamentos.COD_DEPARTAMENTO),
    "left"
)

# LEFT JOIN 3: departamentos
# ON CD.cod_departamento = DE.cod_departamento  
df_result = df_result.join(
    df_alfaprd_general_cgr_departamanetos,
    df_alfaprd_general_cgr_cuidades_departamentos.COD_DEPARTAMENTO == df_alfaprd_general_cgr_departamanetos.COD_DEPARTAMENTO,
    "left"
)

# LEFT JOIN 4: estados
# ON CE.cod_estado = PN.cod_estado
df_result = df_result.join(
    df_general_cgr_estados,
    df_personas_naturales.COD_ESTADO == df_general_cgr_estados.COD_ESTADO,
    "left"  
)

# SELECT final (exactamente como tu query)
df_alfprd_pn_datos_basicos = df_result.select(
    df_personas_naturales.COD_TIPO_IDENTIFICACION,
    df_alfaprd_general_cgr_tipos_identificaciones.NOMBRE.alias("nombre"),
    df_alfaprd_general_cgr_tipos_identificaciones.ABREVIATURA.alias("abreviatura"), 
    df_personas_naturales.NID_NUMERO_IDENTIFICACION,
    df_personas_naturales.PRIMER_APELLIDO,
    df_personas_naturales.SEGUNDO_APELLIDO,
    df_personas_naturales.PRIMER_NOMBRE,
    df_personas_naturales.SEGUNDO_NOMBRE,
    df_personas_naturales.FEC_EXPEDICION_DOCUMENTO,
    df_personas_naturales.COD_CIUDAD_EXPEDICION_DOCU,
    df_alfaprd_general_cgr_cuidades_departamentos.NOMBRE.alias("ciudad_expedicion_docu"),
    df_alfaprd_general_cgr_cuidades_departamentos.COD_DEPARTAMENTO,
    df_alfaprd_general_cgr_departamanetos.NOMBRE.alias("departamento_expe_docu"),
    df_personas_naturales.FEC_REGISTRO,
    df_personas_naturales.COD_ESTADO,
    df_general_cgr_estados.NOMBRE.alias("estado")
)

# Verificar resultado
count_result = df_alfprd_pn_datos_basicos.count()
print(f"Bases de Datos Original Conteo: {1105758:,} Fecha Conteo: 06-08-2025")
print(f"✅ Personas Naturales - Datos Básicos: {count_result:,} registros")

# Mostrar muestra
#print(" Muestra de datos:")
#df_alfprd_pn_datos_basicos.show(3, truncate=False)
# ================================================================================
# ALFPRD - PERSONAS NATURALES - DATOS DEMOGRÁFICOS (CORREGIDO JOIN AFI)
# ================================================================================

print("📊 Procesando: Personas Naturales - Datos Demográficos (ALFPRD)")

# PASO 1: Crear la subquery AFI (igual que antes)
df_afi_subquery = df_alfaprd_arpprod_par_afiliaciones.join(
    df_alfaprd_arpprod_par_trabajadores,
    df_alfaprd_arpprod_par_afiliaciones.NID_AFILIACION == df_alfaprd_arpprod_par_trabajadores.NID_AFILIACION,
    "inner"
).join(
    df_alfaprd_general_acu_personas_juridicas,
    (df_alfaprd_arpprod_par_afiliaciones.COD_TIPO_IDENTIFICACION == df_alfaprd_general_acu_personas_juridicas.COD_TIPO_IDENTIFICACION) &
    (df_alfaprd_arpprod_par_afiliaciones.NID_NUMERO_IDENTIFICACION == df_alfaprd_general_acu_personas_juridicas.NID_NUMERO_IDENTIFICACION),
    "inner"
).select(
    df_alfaprd_arpprod_par_afiliaciones.NID_AFILIACION,
    df_alfaprd_arpprod_par_trabajadores.COD_TIPO_IDENTIFICACION.alias("cod_tipo_identificacion_tra"),
    df_alfaprd_arpprod_par_trabajadores.NID_NUMERO_IDENTIFICACION.alias("nid_numero_identificacion_tra"),
    df_alfaprd_arpprod_par_afiliaciones.COD_TIPO_IDENTIFICACION.alias("cod_tipo_identificacion_emp"),
    df_alfaprd_arpprod_par_afiliaciones.NID_NUMERO_IDENTIFICACION.alias("tipo_identificacion_emp"),
    df_alfaprd_general_acu_personas_juridicas.RAZON_SOCIAL.alias("nombre_empresa"),
    df_alfaprd_arpprod_par_trabajadores.COD_OCUPACION.alias("cod_cargo"),
    df_alfaprd_arpprod_par_trabajadores.FEC_INGRESO_EMPRESA.alias("fecha_ingreso_empresa")
)

# PASO 2: Query principal con todos los JOINs (igual que antes hasta...)
df_result = df_alfaprd_general_acu_personas_naturales

# [Todos los JOINs anteriores iguales...]
df_result = df_result.join(
    df_alfaprd_general_cgr_tipos_identificaciones,
    df_alfaprd_general_acu_personas_naturales.COD_TIPO_IDENTIFICACION == df_alfaprd_general_cgr_tipos_identificaciones.COD_TIPO_DOCUMENTO,
    "left"
)

df_result = df_result.join(
    df_alfaprd_general_cgr_cuidades_departamentos,
    (df_alfaprd_general_acu_personas_naturales.COD_CIUDAD_NACIMIENTO == df_alfaprd_general_cgr_cuidades_departamentos.COD_CIUDAD) &
    (df_alfaprd_general_acu_personas_naturales.COD_DEPARTAMENTO_NACIMIENTO == df_alfaprd_general_cgr_cuidades_departamentos.COD_DEPARTAMENTO),
    "inner"
)

df_result = df_result.join(
    df_alfaprd_general_cgr_departamanetos,
    df_alfaprd_general_cgr_cuidades_departamentos.COD_DEPARTAMENTO == df_alfaprd_general_cgr_departamanetos.COD_DEPARTAMENTO,
    "inner"
)

df_result = df_result.join(
    df_alfaprd_general_cgr_paises,
    df_alfaprd_general_cgr_cuidades_departamentos.COD_PAIS == df_alfaprd_general_cgr_paises.COD_PAIS,
    "inner"
)

df_result = df_result.join(
    df_alfaprd_general_cgr_estados_civiles,
    df_alfaprd_general_acu_personas_naturales.COD_ESTADO_CIVIL == df_alfaprd_general_cgr_estados_civiles.COD_ESTADO_CIVIL,
    "inner"
)

df_result = df_result.join(
    df_alfaprd_general_acu_niveles_educativos,
    df_alfaprd_general_acu_personas_naturales.COD_NIVEL_EDUCATIVO == df_alfaprd_general_acu_niveles_educativos.COD_NIVEL_EDUCATIVO,
    "inner"
)

df_result = df_result.join(
    df_alfaprd_general_acu_actividades_economica_ciiu,
    df_alfaprd_general_acu_personas_naturales.COD_ACTIVIDAD_ECONOMICA_CIIU == df_alfaprd_general_acu_actividades_economica_ciiu.COD_ACTIVIDAD_ECONOMICA_CIIU,
    "inner"
)

df_result = df_result.join(
    df_alfaprd_general_cgr_ocupaciones,
    df_alfaprd_general_acu_personas_naturales.COD_OCUPACION == df_alfaprd_general_cgr_ocupaciones.COD_OCUPACION,
    "inner"
)

df_result = df_result.join(
    df_alfaprd_general_acu_profesiones,
    df_alfaprd_general_acu_personas_naturales.COD_PROFESION == df_alfaprd_general_acu_profesiones.COD_PROFESION,
    "inner"
)

df_result = df_result.join(
    df_afi_subquery,
    (df_alfaprd_general_acu_personas_naturales.COD_TIPO_IDENTIFICACION == df_afi_subquery.cod_tipo_identificacion_tra) &
    (df_alfaprd_general_acu_personas_naturales.NID_NUMERO_IDENTIFICACION == df_afi_subquery.nid_numero_identificacion_tra),
    "left"
)

# SELECT final - referenciar columnas directas del DataFrame AFI
df_alfprd_pn_demograficos = df_result.select(
    df_alfaprd_general_acu_personas_naturales.COD_TIPO_IDENTIFICACION,
    df_alfaprd_general_cgr_tipos_identificaciones.NOMBRE.alias("nombre"),
    df_alfaprd_general_cgr_tipos_identificaciones.ABREVIATURA.alias("abreviatura"),
    df_alfaprd_general_acu_personas_naturales.NID_NUMERO_IDENTIFICACION,
    df_alfaprd_general_acu_personas_naturales.FEC_NACIMIENTO,
    df_alfaprd_general_acu_personas_naturales.COD_CIUDAD_NACIMIENTO,
    df_alfaprd_general_cgr_cuidades_departamentos.NOMBRE.alias("ciudad"),
    df_alfaprd_general_cgr_cuidades_departamentos.COD_DEPARTAMENTO,
    df_alfaprd_general_cgr_departamanetos.NOMBRE.alias("departamento_nacimiento"),
    df_alfaprd_general_cgr_cuidades_departamentos.COD_PAIS.alias("cod_pais_nacimiento"),
    df_alfaprd_general_cgr_paises.NOMBRE.alias("pais_nacimiento"),
    df_alfaprd_general_acu_personas_naturales.NACIONALIDAD,
    df_alfaprd_general_acu_personas_naturales.GENERO,
    df_alfaprd_general_acu_personas_naturales.COD_ESTADO_CIVIL,
    df_alfaprd_general_cgr_estados_civiles.NOMBRE.alias("estado_civil"),
    df_alfaprd_general_acu_personas_naturales.COD_NIVEL_EDUCATIVO,
    df_alfaprd_general_acu_niveles_educativos.NOMBRE.alias("nivel_educativo"),
    df_alfaprd_general_acu_personas_naturales.COD_PROFESION,
    df_alfaprd_general_acu_profesiones.NOMBRE.alias("profesion"),
    df_alfaprd_general_acu_personas_naturales.COD_OCUPACION,
    df_alfaprd_general_cgr_ocupaciones.DESCRIPCION.alias("ocupacion"),
    df_alfaprd_general_acu_personas_naturales.COD_ACTIVIDAD_ECONOMICA_CIIU,
    df_alfaprd_general_acu_actividades_economica_ciiu.DESCRIPCION.alias("actividad_economica_ciiu"),
    df_afi_subquery.cod_tipo_identificacion_emp,
    df_afi_subquery.nid_numero_identificacion_tra,
    df_afi_subquery.nombre_empresa,
    df_afi_subquery.cod_cargo,
    df_afi_subquery.fecha_ingreso_empresa
)

print(f"Bases de Datos Original Conteo: {1243044:,} Fecha Conteo: 06-08-2025")
print(f"✅ Personas Naturales - Datos Demográficos: {df_alfprd_pn_demograficos.count():,} registros")
# ================================================================================
# ALFPRD - PERSONAS NATURALES - CONTACTABILIDAD
# Query convertida a lógica Spark pura
# ================================================================================

print("📊 Procesando: Personas Naturales - Contactabilidad (ALFPRD)")

# Tabla principal: acu_personas_naturales (CLI)
df_result = df_alfaprd_general_acu_personas_naturales

# LEFT JOIN 1: personas_localizaciones
# ON CLI.cod_tipo_identificacion = LOC.cod_tipo_identificacion AND CLI.nid_numero_identificacion = LOC.nid_numero_identificacion
df_result = df_result.join(
    df_alfaprd_general_acu_personas_localizaciones,
    (df_alfaprd_general_acu_personas_naturales.COD_TIPO_IDENTIFICACION == df_alfaprd_general_acu_personas_localizaciones.COD_TIPO_IDENTIFICACION) &
    (df_alfaprd_general_acu_personas_naturales.NID_NUMERO_IDENTIFICACION == df_alfaprd_general_acu_personas_localizaciones.NID_NUMERO_IDENTIFICACION),
    "left"
)

# INNER JOIN 2: ciudades_departamentos  
# ON LOC.cod_ciudad = CD.cod_ciudad AND LOC.cod_departamento = CD.cod_departamento
df_result = df_result.join(
    df_alfaprd_general_cgr_cuidades_departamentos,
    (df_alfaprd_general_acu_personas_localizaciones.COD_CIUDAD == df_alfaprd_general_cgr_cuidades_departamentos.COD_CIUDAD) &
    (df_alfaprd_general_acu_personas_localizaciones.COD_DEPARTAMENTO == df_alfaprd_general_cgr_cuidades_departamentos.COD_DEPARTAMENTO),
    "inner"
)

# INNER JOIN 3: departamentos
# ON CD.cod_departamento = DE.cod_departamento  
df_result = df_result.join(
    df_alfaprd_general_cgr_departamanetos,
    df_alfaprd_general_cgr_cuidades_departamentos.COD_DEPARTAMENTO == df_alfaprd_general_cgr_departamanetos.COD_DEPARTAMENTO,
    "inner"
)

# INNER JOIN 4: países
# ON PS.cod_pais = CD.cod_pais
df_result = df_result.join(
    df_alfaprd_general_cgr_paises,
    df_alfaprd_general_cgr_paises.COD_PAIS == df_alfaprd_general_cgr_cuidades_departamentos.COD_PAIS,
    "inner"
)

# SELECT con GROUP BY (usando dropDuplicates en lugar de GROUP BY)
df_alfprd_pn_contactabilidad = df_result.select(
    df_alfaprd_general_acu_personas_naturales.COD_TIPO_IDENTIFICACION,
    df_alfaprd_general_acu_personas_naturales.NID_NUMERO_IDENTIFICACION,
    df_alfaprd_general_acu_personas_localizaciones.E_MAIL.alias("e_mail"),
    df_alfaprd_general_acu_personas_localizaciones.COD_TIPO_LOCALIZACION.alias("cod_tipo_localizacion"),
    lit("").alias("tipo_ubicacion"),  # Campo vacío como en query original
    df_alfaprd_general_acu_personas_localizaciones.DIRECCION.alias("direccion"),
    df_alfaprd_general_acu_personas_localizaciones.COD_CIUDAD.alias("cod_ciudad"),
    df_alfaprd_general_cgr_cuidades_departamentos.NOMBRE.alias("ciudad"),
    df_alfaprd_general_cgr_cuidades_departamentos.COD_DEPARTAMENTO.alias("cod_departamento"),
    df_alfaprd_general_cgr_departamanetos.NOMBRE.alias("departamento"),
    df_alfaprd_general_cgr_cuidades_departamentos.COD_PAIS.alias("cod_pais"),
    df_alfaprd_general_cgr_paises.NOMBRE.alias("pais"),
    df_alfaprd_general_acu_personas_localizaciones.PREFIJO_CELULAR.alias("prefijo_celular"),
    df_alfaprd_general_acu_personas_localizaciones.NUMERO_CELULAR.alias("numero_celular"),
    df_alfaprd_general_acu_personas_localizaciones.INDICATIVO.alias("indicativo"),
    df_alfaprd_general_acu_personas_localizaciones.NUMERO_TELEFONO.alias("numero_telefono"),
    df_alfaprd_general_acu_personas_localizaciones.EXTENSION_TELEFONO.alias("extension_telefono")
).dropDuplicates()  # Equivalente a GROUP BY en Spark

# Verificar resultado
count_result = df_alfprd_pn_contactabilidad.count()
print(f"Bases de Datos Original Conteo: {1162155:,} Fecha Conteo: 06-08-2025")
print(f"✅ Personas Naturales - Contactabilidad: {count_result:,} registros")

# Mostrar muestra
#print("📋 Muestra de datos:")
#df_alfprd_pn_contactabilidad.show(3, truncate=False)
# ================================================================================
# ALFPRD - PERSONAS JURIDICAS - DATOS DEMOGRAFICOS
# Query convertida a logica Spark pura
# ================================================================================

print("📊 Procesando: Personas Juridicas - Datos Demograficos (ALFPRD)")

# Tabla principal: acu_personas_juridicas (PJ)
df_result = df_alfaprd_general_acu_personas_juridicas

# LEFT JOIN 1: tipos_identificaciones
# ON PJ.cod_tipo_identificacion = TD.cod_tipo_documento
df_result = df_result.join(
    df_alfaprd_general_cgr_tipos_identificaciones,
    df_alfaprd_general_acu_personas_juridicas.COD_TIPO_IDENTIFICACION == df_alfaprd_general_cgr_tipos_identificaciones.COD_TIPO_DOCUMENTO,
    "left"
)

# INNER JOIN 2: ciudades_departamentos
# ON PJ.cod_ciudad = CD.cod_ciudad AND PJ.cod_departamento = CD.cod_departamento
df_result = df_result.join(
    df_alfaprd_general_cgr_cuidades_departamentos,
    (df_alfaprd_general_acu_personas_juridicas.COD_CIUDAD == df_alfaprd_general_cgr_cuidades_departamentos.COD_CIUDAD) &
    (df_alfaprd_general_acu_personas_juridicas.COD_DEPARTAMENTO == df_alfaprd_general_cgr_cuidades_departamentos.COD_DEPARTAMENTO),
    "inner"
)

# INNER JOIN 3: departamentos
# ON CD.cod_departamento = DE.cod_departamento
df_result = df_result.join(
    df_alfaprd_general_cgr_departamanetos,
    df_alfaprd_general_cgr_cuidades_departamentos.COD_DEPARTAMENTO == df_alfaprd_general_cgr_departamanetos.COD_DEPARTAMENTO,
    "inner"
)

# INNER JOIN 4: paises
# ON PS.cod_pais = CD.cod_pais
df_result = df_result.join(
    df_alfaprd_general_cgr_paises,
    df_alfaprd_general_cgr_paises.COD_PAIS == df_alfaprd_general_cgr_cuidades_departamentos.COD_PAIS,
    "inner"
)

# INNER JOIN 5: actividades_economicas
# ON AE.cod_actividad_economica_ciiu = PJ.cod_actividad_economica_ciiu
df_result = df_result.join(
    df_alfaprd_general_acu_actividades_economica_ciiu,
    df_alfaprd_general_acu_actividades_economica_ciiu.COD_ACTIVIDAD_ECONOMICA_CIIU == df_alfaprd_general_acu_personas_juridicas.COD_ACTIVIDAD_ECONOMICA_CIIU,
    "inner"
)

# SELECT final (exactamente como tu query)
df_alfprd_pj_demograficos = df_result.select(
    df_alfaprd_general_acu_personas_juridicas.COD_TIPO_IDENTIFICACION,
    df_alfaprd_general_cgr_tipos_identificaciones.NOMBRE.alias("nombre"),
    df_alfaprd_general_cgr_tipos_identificaciones.ABREVIATURA.alias("abreviatura"),
    df_alfaprd_general_acu_personas_juridicas.NID_NUMERO_IDENTIFICACION,
    df_alfaprd_general_acu_personas_juridicas.FEC_CONSTITUCION.alias("fec_constitucion"),
    df_alfaprd_general_acu_personas_juridicas.COD_CIUDAD.alias("cod_ciudad"),
    df_alfaprd_general_cgr_cuidades_departamentos.NOMBRE.alias("ciudad"),
    df_alfaprd_general_cgr_cuidades_departamentos.COD_DEPARTAMENTO.alias("cod_departamento"),
    df_alfaprd_general_cgr_departamanetos.NOMBRE.alias("departamento_nacimiento"),
    df_alfaprd_general_cgr_cuidades_departamentos.COD_PAIS.alias("cod_pais_nacimiento"),
    df_alfaprd_general_cgr_paises.NOMBRE.alias("pais_nacimiento"),
    df_alfaprd_general_acu_personas_juridicas.COD_ACTIVIDAD_ECONOMICA_CIIU.alias("cod_actividad_economica_ciiu"),
    df_alfaprd_general_acu_actividades_economica_ciiu.DESCRIPCION.alias("actividad_economica_ciiu")
)

# Verificar resultado
count_result = df_alfprd_pj_demograficos.count()
print(f"Bases de Datos Original Conteo: {4847:,} Fecha Conteo: 06-08-2025")
print(f"✅ Personas Juridicas - Datos Demograficos: {count_result:,} registros")
# ================================================================================
# ALFPRD - PERSONAS JURIDICAS - DATOS BASICOS
# Query convertida a logica Spark pura
# ================================================================================

print("📊 Procesando: Personas Juridicas - Datos Basicos (ALFPRD)")

# Tabla principal: acu_personas_juridicas (PJ)
df_result = df_alfaprd_general_acu_personas_juridicas

# LEFT JOIN 1: tipos_identificaciones
# ON PJ.cod_tipo_identificacion = TD.cod_tipo_documento
df_result = df_result.join(
    df_alfaprd_general_cgr_tipos_identificaciones,
    df_alfaprd_general_acu_personas_juridicas.COD_TIPO_IDENTIFICACION == df_alfaprd_general_cgr_tipos_identificaciones.COD_TIPO_DOCUMENTO,
    "left"
)

# LEFT JOIN 2: estados
# ON ES.cod_estado = PJ.cod_estado
df_result = df_result.join(
    df_general_cgr_estados,
    df_general_cgr_estados.COD_ESTADO == df_alfaprd_general_acu_personas_juridicas.COD_ESTADO,
    "left"
)

# INNER JOIN 3: tipos_empresas
# ON TE.cod_tipo_empresa = PJ.cod_tipo_empresa
df_result = df_result.join(
    df_alfaprd_general_acu_tipos_empresa,
    df_alfaprd_general_acu_tipos_empresa.COD_TIPO_EMPRESA == df_alfaprd_general_acu_personas_juridicas.COD_TIPO_EMPRESA,
    "inner"
)

# SELECT final (exactamente como tu query)
df_alfprd_pj_datos_basicos = df_result.select(
    df_alfaprd_general_acu_personas_juridicas.COD_TIPO_IDENTIFICACION,
    df_alfaprd_general_cgr_tipos_identificaciones.NOMBRE.alias("nombre"),
    df_alfaprd_general_cgr_tipos_identificaciones.ABREVIATURA.alias("abreviatura"),
    df_alfaprd_general_acu_personas_juridicas.NID_NUMERO_IDENTIFICACION,
    df_alfaprd_general_acu_personas_juridicas.RAZON_SOCIAL.alias("razon_social"),
    df_alfaprd_general_acu_personas_juridicas.FEC_REGISTRO.alias("fec_registro"),
    df_alfaprd_general_acu_personas_juridicas.COD_ESTADO.alias("cod_estado"),
    df_general_cgr_estados.NOMBRE.alias("estado"),
    df_alfaprd_general_acu_personas_juridicas.COD_TIPO_EMPRESA.alias("cod_tipo_empresa"),
    df_alfaprd_general_acu_tipos_empresa.NOMBRE.alias("tipo_empresa")
)

# Verificar resultado
count_result = df_alfprd_pj_datos_basicos.count()
print(f"✅ Personas Juridicas - Datos Basicos: {count_result:,} registros")
print(f"Bases de Datos Original Conteo: {3452:,} Fecha Conteo: 06-08-2025")
# ================================================================================
# ALFPRD - PERSONAS JURIDICAS - DATOS UBICACION
# Query convertida a logica Spark pura
# ================================================================================

print("📊 Procesando: Personas Juridicas - Datos Ubicacion (ALFPRD)")

# Tabla principal: acu_personas_juridicas (PJ)
df_result = df_alfaprd_general_acu_personas_juridicas

# LEFT JOIN 1: personas_localizaciones
# ON PJ.cod_tipo_identificacion = LOC.cod_tipo_identificacion AND PJ.nid_numero_identificacion = LOC.nid_numero_identificacion
df_result = df_result.join(
    df_alfaprd_general_acu_personas_localizaciones,
    (df_alfaprd_general_acu_personas_juridicas.COD_TIPO_IDENTIFICACION == df_alfaprd_general_acu_personas_localizaciones.COD_TIPO_IDENTIFICACION) &
    (df_alfaprd_general_acu_personas_juridicas.NID_NUMERO_IDENTIFICACION == df_alfaprd_general_acu_personas_localizaciones.NID_NUMERO_IDENTIFICACION),
    "left"
)

# INNER JOIN 2: ciudades_departamentos
# ON LOC.cod_ciudad = CD.cod_ciudad AND LOC.cod_departamento = CD.cod_departamento
df_result = df_result.join(
    df_alfaprd_general_cgr_cuidades_departamentos,
    (df_alfaprd_general_acu_personas_localizaciones.COD_CIUDAD == df_alfaprd_general_cgr_cuidades_departamentos.COD_CIUDAD) &
    (df_alfaprd_general_acu_personas_localizaciones.COD_DEPARTAMENTO == df_alfaprd_general_cgr_cuidades_departamentos.COD_DEPARTAMENTO),
    "inner"
)

# INNER JOIN 3: departamentos
# ON CD.cod_departamento = DE.cod_departamento
df_result = df_result.join(
    df_alfaprd_general_cgr_departamanetos,
    df_alfaprd_general_cgr_cuidades_departamentos.COD_DEPARTAMENTO == df_alfaprd_general_cgr_departamanetos.COD_DEPARTAMENTO,
    "inner"
)

# INNER JOIN 4: paises
# ON PS.cod_pais = CD.cod_pais
df_result = df_result.join(
    df_alfaprd_general_cgr_paises,
    df_alfaprd_general_cgr_paises.COD_PAIS == df_alfaprd_general_cgr_cuidades_departamentos.COD_PAIS,
    "inner"
)

# SELECT final (exactamente como tu query)
df_alfprd_pj_ubicacion = df_result.select(
    df_alfaprd_general_acu_personas_juridicas.COD_TIPO_IDENTIFICACION,
    df_alfaprd_general_acu_personas_juridicas.NID_NUMERO_IDENTIFICACION,
    df_alfaprd_general_acu_personas_localizaciones.E_MAIL.alias("e_mail"),
    df_alfaprd_general_acu_personas_localizaciones.COD_TIPO_LOCALIZACION.alias("cod_tipo_localizacion"),
    lit("").alias("tipo_ubicacion"),  # Campo vacío como en el SQL
    df_alfaprd_general_acu_personas_localizaciones.DIRECCION.alias("direccion"),
    df_alfaprd_general_acu_personas_localizaciones.COD_CIUDAD.alias("cod_ciudad"),
    df_alfaprd_general_cgr_cuidades_departamentos.NOMBRE.alias("ciudad"),
    df_alfaprd_general_cgr_cuidades_departamentos.COD_DEPARTAMENTO.alias("cod_departamento"),
    df_alfaprd_general_cgr_departamanetos.NOMBRE.alias("departamento"),
    df_alfaprd_general_cgr_cuidades_departamentos.COD_PAIS.alias("cod_pais"),
    df_alfaprd_general_cgr_paises.NOMBRE.alias("pais"),
    df_alfaprd_general_acu_personas_localizaciones.PREFIJO_CELULAR.alias("prefijo_celular"),
    df_alfaprd_general_acu_personas_localizaciones.NUMERO_CELULAR.alias("numero_celular"),
    df_alfaprd_general_acu_personas_localizaciones.INDICATIVO.alias("indicativo"),
    df_alfaprd_general_acu_personas_localizaciones.NUMERO_TELEFONO.alias("numero_telefono"),
    df_alfaprd_general_acu_personas_localizaciones.EXTENSION_TELEFONO.alias("extension_telefono")
)

# GROUP BY (equivalente en Spark con distinct)
df_alfprd_pj_ubicacion = df_alfprd_pj_ubicacion.distinct()

# Verificar resultado
count_result = df_alfprd_pj_ubicacion.count()
print(f"✅ Personas Juridicas - Datos Ubicacion: {count_result:,} registros")
print(f"Bases de Datos Original Conteo: {5690:,} Fecha Conteo: 06-08-2025")
df_pn_demograficos_renamed = df_alfprd_pn_demograficos.select(
    *[col(c).alias(f"{c}_demografico") if c not in ["COD_TIPO_IDENTIFICACION", "NID_NUMERO_IDENTIFICACION"] 
      else col(c) for c in df_alfprd_pn_demograficos.columns]
)

# Renombrar columnas conflictivas en contactabilidad PN  
df_pn_contactabilidad_renamed = df_alfprd_pn_contactabilidad.select(
    *[col(c).alias(f"{c}_contacto") if c not in ["COD_TIPO_IDENTIFICACION", "NID_NUMERO_IDENTIFICACION"] 
      else col(c) for c in df_alfprd_pn_contactabilidad.columns]
)

df_alfaprd_pn_completo = df_alfprd_pn_datos_basicos \
    .join(
        df_pn_demograficos_renamed, 
        ["COD_TIPO_IDENTIFICACION", "NID_NUMERO_IDENTIFICACION"], 
        "left"
    ) \
    .join(
        df_pn_contactabilidad_renamed, 
        ["COD_TIPO_IDENTIFICACION", "NID_NUMERO_IDENTIFICACION"], 
        "left"
    )

# ===== CONSOLIDACIÓN PERSONAS JURÍDICAS =====
print("📋 Consolidando Personas Jurídicas...")

# Renombrar columnas conflictivas en demográficos PJ
df_pj_demograficos_renamed = df_alfprd_pj_demograficos.select(
    *[col(c).alias(f"{c}_demografico") if c not in ["COD_TIPO_IDENTIFICACION", "NID_NUMERO_IDENTIFICACION"] 
      else col(c) for c in df_alfprd_pj_demograficos.columns]
)

# Renombrar columnas conflictivas en ubicación PJ
df_pj_ubicacion_renamed = df_alfprd_pj_ubicacion.select(
    *[col(c).alias(f"{c}_ubicacion") if c not in ["COD_TIPO_IDENTIFICACION", "NID_NUMERO_IDENTIFICACION"] 
      else col(c) for c in df_alfprd_pj_ubicacion.columns]
)

df_alfaprd_pj_completo = df_alfprd_pj_datos_basicos \
    .join(
        df_pj_demograficos_renamed, 
        ["COD_TIPO_IDENTIFICACION", "NID_NUMERO_IDENTIFICACION"], 
        "left"
    ) \
    .join(
        df_pj_ubicacion_renamed, 
        ["COD_TIPO_IDENTIFICACION", "NID_NUMERO_IDENTIFICACION"], 
        "left"
    )

# ===== UNIÓN PN + PJ =====
print("🔄 Uniendo Personas Naturales + Personas Jurídicas...")
df_macro_alfaprd_temp = df_alfaprd_pn_completo.unionByName(
    df_alfaprd_pj_completo, 
    allowMissingColumns=True
)

# ===== AGREGAR CAMPO COD_FUENTE =====
print("📌 Agregando campo COD_FUENTE = 2...")
df_macro_alfaprd = df_macro_alfaprd_temp.withColumn("COD_FUENTE", lit(2))

# ===== VERIFICACIÓN FINAL =====
count_pn = df_alfaprd_pn_completo.count()
count_pj = df_alfaprd_pj_completo.count()
count_total = df_macro_alfaprd.count()

print(f"✅ Personas Naturales consolidadas: {count_pn:,} registros")
print(f"✅ Personas Jurídicas consolidadas: {count_pj:,} registros")
print(f"🎯 DF Macro ALFAPRD total: {count_total:,} registros")
# Verificar que la suma coincida
if count_total == (count_pn + count_pj):
    print("✅ Verificación exitosa: La suma de registros coincide")
else:
    print("⚠️ Advertencia: Revisar posibles duplicados o inconsistencias")
acsele_processor = AcseleStagingProcessor(
    spark_session=spark,
    glue_context=glueContext,
    bucket_bronze=BUCKET_BRONZE,
    bucket_silver=BUCKET_SILVER
)
tables = acsele_processor.load_acsele_tables()
 # Personas Naturales
df_pn_basicos = acsele_processor.process_personas_naturales_datos_basicos(tables)
df_pn_demograficos = acsele_processor.process_personas_naturales_datos_demograficos(tables)
df_pn_contactabilidad = acsele_processor.process_personas_naturales_contactabilidad(tables)
# Personas Jurídicas
df_pj_basicos = acsele_processor.process_personas_juridicas_datos_basicos(tables)
df_pj_demograficos = acsele_processor.process_personas_juridicas_datos_demograficos(tables)
df_pj_contactabilidad = acsele_processor.process_personas_juridicas_contactabilidad(tables)
df_datos_basicos_final = acsele_processor.consolidate_datos_basicos_acsele_data(df_pn_basicos, df_pj_basicos)
df_sociodemograficos_final = acsele_processor.consolidate_datos_sociodemograficos_acsele_data(df_pn_demograficos, df_pj_demograficos)
df_contactabilidad_final = acsele_processor.consolidate_datos_contactabilidad_acsele_data(df_pn_contactabilidad, df_pj_contactabilidad)
datos_basicos_schema = DatosBasicosSchema(spark, glueContext, s3_handler_silver)
s3_prefix, s3_target_path = s3_handler_silver.generate_s3_paths('CLIENTES', 'DATOS_BASICOS', 'STG')
#datos_basicos_schema.create_empty_schema(s3_target_path)
df_datos_basicos_final.printSchema()
df_datos_basicos_final_acsele = df_datos_basicos_final.withColumn("ciudad_expedicion_documento",col("ciudad_expedicion_documento").cast("string")) \
                            .withColumn("cod_tipo_persona",col("cod_tipo_persona").cast("string"))
df_datos_basicos_final_acsele.printSchema()
summary = datos_basicos_schema.insert_data(
    df_new_data=df_datos_basicos_final_acsele,
    s3_path=s3_target_path,
    cod_fuente=COD_FUENTES.get("ACSELE",1),  # CARDIFF
    job_id=JOB_ID
)
df_sociodemograficos_final.printSchema()
# Convertir todas las columnas void a string en un solo select
df_sociodemograficos_final_fixed = df_sociodemograficos_final.select([
    col(c).cast("string") if c in [
        "edad", "cod_pais_nacimiento", "pais_nacimiento", "nacionalidad", 
        "cod_profesion", "profesion", "ocupacion", "afiliacion_id", "cod_cargo_empresa"
    ] else col(c) 
    for c in df_sociodemograficos_final.columns
])
df_sociodemograficos_final_fixed.printSchema()
datos_sociodemo_schema = DatosSociodemograficosSchema(spark, glueContext, s3_handler_silver)
s3_prefix, s3_target_path = s3_handler_silver.generate_s3_paths('CLIENTES', 'DATOS_SOCIODEMOGRAFICOS', 'STG')
#datos_sociodemo_schema.create_empty_schema(s3_target_path)
s3_target_path
summary = datos_sociodemo_schema.insert_data(
    df_new_data=df_sociodemograficos_final_fixed,
    s3_path=s3_target_path,
    cod_fuente=COD_FUENTES.get("ACSELE",1),  # CARDIFF
    job_id=JOB_ID
)
df_contactabilidad_final.printSchema()
df_contactabilidad_final_fixed = df_contactabilidad_final.withColumn("tipo_telefono",col("tipo_telefono").cast("string")) \
                            .withColumn("extension",col("extension").cast("string"))
df_contactabilidad_final_fixed.printSchema()
datos_contactabilidad_schema = DatosUbicacionContactabilidadSchema(spark, glueContext, s3_handler_silver)
s3_prefix, s3_target_path = s3_handler_silver.generate_s3_paths('CLIENTES', 'DATOS_UBICACION_Y_CONTACTABILIDAD', 'STG')
#datos_contactabilidad_schema.create_empty_schema(s3_target_path)
summary = datos_contactabilidad_schema.insert_data(
    df_new_data=df_contactabilidad_final_fixed,
    s3_path=s3_target_path,
    cod_fuente=COD_FUENTES.get("ACSELE",1),  # CARDIFF
    job_id=JOB_ID
)
s3_1754537275713 = 's3://alfa-datalake-dev-silver/CLIENTES/DATOS_BASICOS/CLIENTES_DATOS_BASICOS_STG/2025/08/07/run-1754537275713-part-block-0-r-00000-snappy.parquet'
df_datos_basicos_1754537275713 = read_full_parquet(s3_1754537275713)
df_datos_basicos_1754537275713.count()
s3_1754537275713 = 's3://alfa-datalake-dev-silver/CLIENTES/DATOS_BASICOS/CLIENTES_DATOS_BASICOS_STG/2025/08/07/run-1754537275713-part-block-0-r-00000-snappy.parquet'
df_datos_basicos_1754537275713 = read_full_parquet(s3_1754537275713)
df_datos_basicos_1754537275713.count()
from s3_handler import S3Handler
s3_handler = S3Handler(BUCKET_SILVER, glueContext)
# Convert to Dynamic
df_dynamic = DynamicFrame.fromDF(df_alfprd_datos_basicos_schema, glueContext, "fuentes_clientes")
s3_handler = S3Handler(BUCKET_SILVER, glueContext)
s3_prefix, s3_target_path = s3_handler.generate_s3_paths('CLIENTES', 'DATOS_BASICOS', 'STG')
response = s3_handler.save_files_to_s3(df_dynamic, s3_target_path, format="parquet", mode="overwrite")
BUCKET_SILVER
s3_handler.save_schema(df_alfprd_datos_basicos_schema, "alfaprd_datos_basicos_schema.json")
job.commit()