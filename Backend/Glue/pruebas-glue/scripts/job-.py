
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import row_number, lit
from pyspark.sql.window import Window
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
# Origins
cardiff_hist_file = 's3://alfa-datalake-dev-langding/CARDIFF/DOM_CUSTOMERS_FULL.txt'
cardiff_delta_file = 's3://alfa-datalake-dev-langding/CARDIFF/DOM_CUSTOMERS_20240829.txt'
def show_properties(df):
    print("Properties")
    cols = len(df.columns)
    rows = df.count()
    print("Columns: ", cols)
    print("Rows: ", rows)
    df.printSchema()
df_cardiff_hist = spark.read.options(delimiter='|', header=True).csv(cardiff_hist_file)
show_properties(df_cardiff_hist)
from pyspark.sql.functions import row_number, lit
from pyspark.sql.window import Window
def read_delta_cardiff(schema, path_file):
    # 1. read dataframe
    df_cardiff_delta = spark.read \
                   .option("header", False) \
                   .schema(schema) \
                   .option("delimiter", "|") \
                   .csv(path_file)
    # 2. delete first row
    window = Window.orderBy(lit(1))
    df_cardiff_delta = df_cardiff_delta.withColumn("row_number", row_number().over(window)) \
                                        .filter("row_number > 1") \
                                        .drop("row_number")
    
    return df_cardiff_delta
# 1. Read columns
COLUMNS_CARDIFF = list(df_cardiff_hist.columns)
# 2. Structure Schema
schema = StructType([StructField(col, StringType(), True) for col in COLUMNS_CARDIFF])
df_cardiff_delta = read_delta_cardiff(schema, cardiff_delta_file)
show_properties(df_cardiff_delta)
df_joined = df_cardiff_hist.union(df_cardiff_delta)
#self._df_ola0_processed.union(self._df_ola1_processed)
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan, isnull, sum as spark_sum, count, round as spark_round

class SparkProcessorReport:
    """
    Clase para procesar DataFrames de Spark y generar reportes de análisis de datos.
    
    Esta clase analiza un DataFrame de Spark y genera reportes que incluyen:
    1. Conteo total de campos
    2. Análisis de campos numéricos con nivel de ceros
    3. Análisis de valores NULL en campos categóricos, numéricos, date y char
    
    Args:
        df (DataFrame): DataFrame de Spark a analizar
    """
    
    def __init__(self, df: DataFrame):
        self.df = df
        self.numeric_fields = ['CUSTOMER_NO', 'BIRTH_YEARS_OLD', 'ID_PIMS', 'ID_TMK']
        self.date_fields = ['CREATION_DATE', 'BIRTH_DATE', 'LAST_EXECUTION_DATE']
        self.char_fields = [
            'CUSTOMER_TYPE', 'BIRTH_STATE', 'BIRTH_STATE_NAME', 'BIRTH_CITY_CODE',
            'BIRTH_CITY_NAME', 'ADULT', 'MILITARY', 'SHARE_INFORMATION',
            'ADDRESS_STATE_CODE', 'ADDRESS_CITY_CODE', 'ADDRESS_TYPE', 'MARITAL_STATUS'
        ]
        self.categorical_fields = [col for col in self.df.columns 
                                 if col not in self.numeric_fields + self.date_fields + self.char_fields]
        
    def print_fields_summary(self) -> None:
        total_fields = len(self.df.columns)
        
        print("=" * 60)
        print("RESUMEN DE CAMPOS POR TIPO")
        print("=" * 60)
        print(f"Total de campos: {total_fields}")
        print(f"Campos numéricos: {len(self.numeric_fields)}")
        print(f"Campos fecha: {len(self.date_fields)}")
        print(f"Campos char: {len(self.char_fields)}")
        print(f"Campos categóricos: {len(self.categorical_fields)}")
        print()
        
        # print("CAMPOS NUMÉRICOS:")
        # for field in self.numeric_fields:
        #     if field in self.df.columns:
        #         print(f"   • {field}")
        # print()
        
        # print("CAMPOS FECHA:")
        # for field in self.date_fields:
        #     if field in self.df.columns:
        #         print(f"   • {field}")
        # print()
        
        # print("CAMPOS CHAR:")
        # for field in self.char_fields:
        #     if field in self.df.columns:
        #         print(f"   • {field}")
        # print()
        
        # print("CAMPOS CATEGÓRICOS:")
        # for field in self.categorical_fields:
        #     print(f"   • {field}")
    
    def print_numeric_zero_analysis(self) -> None:
        total_rows = self.df.count()
        
        print("=" * 60)
        print("ANÁLISIS DE CAMPOS NUMÉRICOS - NIVEL DE CEROS")
        print("=" * 60)
        
        for field in self.numeric_fields:
            if field in self.df.columns:
                zero_null_count = self.df.filter(
                    (col(field) == "0") | 
                    (col(field) == "") | 
                    col(field).isNull()
                ).count()
                
                percentage = round((zero_null_count * 100.0) / total_rows, 2) if total_rows > 0 else 0
                
                print(f"{field}: {zero_null_count} registros con cero/null de {total_rows} ({percentage}%)")
        
        print()
    
    def print_null_analysis(self) -> None:
        total_rows = self.df.count()
        
        print("=" * 80)
        print("ANÁLISIS DE VALORES NULL POR CAMPO")
        print("=" * 80)
        
        all_fields = [
            (self.numeric_fields, 'NUMERICO'),
            (self.date_fields, 'FECHA'),
            (self.char_fields, 'CHAR'),
            (self.categorical_fields, 'CATEGORICO')
        ]
        
        # Header
        print(f"{'CAMPO':<30}{'TIPO_CAMPO':<15}{'TOTAL_REGISTROS':<18}{'VALORES_NULL':<15}{'PORCENTAJE_NULL':<15}")
        print("-" * 93)
        
        all_field_data = []
        
        for field_list, field_type in all_fields:
            for field in field_list:
                if field in self.df.columns:
                    null_count = self.df.filter(
                        col(field).isNull() | 
                        (col(field) == "") |
                        (col(field) == "null")
                    ).count()
                    
                    percentage = round((null_count * 100.0) / total_rows, 2) if total_rows > 0 else 0
                    all_field_data.append((field, field_type, total_rows, null_count, percentage))
        
        # Sort by field name
        all_field_data.sort(key=lambda x: x[0])
        
        for field, field_type, total_reg, null_count, percentage in all_field_data:
            print(f"{field:<30}{field_type:<15}{total_reg:<18}{null_count:<15}{percentage:<15}")
        
        print()
    
    def print_statistics_by_type(self) -> None:
        total_rows = self.df.count()
        
        print("=" * 60)
        print("ESTADÍSTICAS POR TIPO DE CAMPO")
        print("=" * 60)
        
        field_types = [
            (self.numeric_fields, 'NUMÉRICO'),
            (self.date_fields, 'FECHA'),
            (self.char_fields, 'CHAR'),
            (self.categorical_fields, 'CATEGÓRICO')
        ]
        
        for field_list, field_type in field_types:
            total_nulls = 0
            valid_fields = [f for f in field_list if f in self.df.columns]
            
            for field in valid_fields:
                null_count = self.df.filter(
                    col(field).isNull() | 
                    (col(field) == "") |
                    (col(field) == "null")
                ).count()
                total_nulls += null_count
            
            avg_percentage = round((total_nulls * 100.0) / (total_rows * len(valid_fields)), 2) if valid_fields and total_rows > 0 else 0
            
            print(f"{field_type}: {len(valid_fields)} campos, {total_nulls} nulls totales, {avg_percentage}% promedio")
        
        print()
    
    def print_top_null_fields(self, top_n: int = 10) -> None:
        total_rows = self.df.count()
        
        print("=" * 60)
        print(f"TOP {top_n} CAMPOS CON MÁS VALORES NULL")
        print("=" * 60)
        
        all_fields = self.numeric_fields + self.date_fields + self.char_fields + self.categorical_fields
        field_null_data = []
        
        for field in all_fields:
            if field in self.df.columns:
                null_count = self.df.filter(
                    col(field).isNull() | 
                    (col(field) == "") |
                    (col(field) == "null")
                ).count()
                
                if null_count > 0:
                    percentage = round((null_count * 100.0) / total_rows, 2) if total_rows > 0 else 0
                    
                    # Determine field type
                    if field in self.numeric_fields:
                        field_type = 'NUMÉRICO'
                    elif field in self.date_fields:
                        field_type = 'FECHA'
                    elif field in self.char_fields:
                        field_type = 'CHAR'
                    else:
                        field_type = 'CATEGÓRICO'
                    
                    field_null_data.append((field, field_type, null_count, percentage))
        
        # Sort by null count descending and take top N
        top_fields = sorted(field_null_data, key=lambda x: x[2], reverse=True)[:top_n]
        
        if top_fields:
            for i, (field, field_type, null_count, percentage) in enumerate(top_fields, 1):
                print(f"{i:2d}. {field} ({field_type}): {null_count} nulls ({percentage}%)")
        else:
            print("No se encontraron campos con valores NULL")
        
        print()
    
    def print_complete_report(self) -> None:
        print("🔍 GENERANDO REPORTE COMPLETO DE ANÁLISIS DE DATOS")
        print("📊 DataFrame: df_joined")
        print(f"📈 Total de registros: {self.df.count()}")
        print()
        
        self.print_fields_summary()
        self.print_numeric_zero_analysis()
        self.print_null_analysis()
        self.print_statistics_by_type()
        self.print_top_null_fields()
        
        print("✅ REPORTE COMPLETO GENERADO")
# 6. REPORTE 5: Top 10 campos con más valores NULL
processor.print_top_null_fields(10)

# 7. REPORTE COMPLETO: Ejecuta todos los reportes anteriores
processor.print_complete_report()

# Ejemplos de uso específicos:

# Para ver solo los primeros 5 campos con más nulls:
processor.print_top_null_fields(5)

# Para ver los primeros 15 campos con más nulls:
processor.print_top_null_fields(15)
# 1. Inicializar la clase con tu DataFrame
processor = SparkProcessorReport(df_joined)

# 2. REPORTE 1: Resumen de campos por tipo
processor.print_fields_summary()
# 3. REPORTE 2: Análisis de campos numéricos con ceros
processor.print_numeric_zero_analysis()
# 4. REPORTE 3: Análisis completo de valores NULL
processor.print_null_analysis()
# 5. REPORTE 4: Estadísticas resumidas por tipo de campo
processor.print_statistics_by_type()
df_joined.count()
df_joined.show(5)
job.commit()