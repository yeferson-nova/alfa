
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
import pandas as pd
from pydantic_settings import BaseSettings
from pandas_profiling import ProfileReport
import boto3, datetime, json
from io import BytesIO
from commons.ExtractHelper import ExtractHelper
import warnings

s3_client = boto3.client("s3")

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
ExtractHelper = ExtractHelper(glueContext, spark, "-", "-")


spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
file = 's3://alfa-datalake-dev-langding/Clientes_Test/Datos_Basicos_PJ_V2.xlsx'
sheet = "Exportar Hoja de Trabajo"
with warnings.catch_warnings(record=True):
  warnings.simplefilter("always")
  pandas_df_SS_Datos_Basicos_PJ = pd.read_excel(file, sheet_name=sheet, engine="openpyxl")
pandas_df_SS_Datos_Basicos_PJ.dtypes
pandas_df_SS_Datos_Basicos_PJ.shape
resumen = pandas_df_SS_Datos_Basicos_PJ.describe()
df_resumen = pd.DataFrame(resumen)
df_resumen.iloc[:, :2]
df_resumen.iloc[:, [3, 4]]
df_resumen.iloc[:, [6, 7]]
pandas_df_SS_Datos_Basicos_PJ.describe()
nulos = pandas_df_SS_Datos_Basicos_PJ.isnull().sum()
nulos
# Filtrar solo las columnas numéricas
df_numericas = pandas_df_SS_Datos_Basicos_PJ.select_dtypes(include='number')

# Calcular y graficar la matriz de correlación
import seaborn as sns
import matplotlib.pyplot as plt

correlacion = df_numericas.corr()
correlacion
profile = ProfileReport(pandas_df_SS_Datos_Basicos_PJ, title="Profiling - SS - Datos Basicos PJ", explorative=True)
profile.to_file("/tmp/Profiling_SS_Datos_Basicos_PJ.html")
s3_client.upload_file('/tmp/Profiling_SS_Datos_Basicos_PJ.html', 'alfa-datalake-dev-langding', 'Clientes_Test/dataprofiling/Profiling_SS_Datos_Basicos_PJ.html')
file = 's3://alfa-datalake-dev-langding/Clientes_Test/Datos_Basicos_PN_V2.xlsx'
sheet = "Exportar Hoja de Trabajo"
with warnings.catch_warnings(record=True):
  warnings.simplefilter("always")
  pandas_df_SS_Datos_Basicos_PN = pd.read_excel(file, sheet_name=sheet, engine="openpyxl")
pandas_df_SS_Datos_Basicos_PN.dtypes
profile = ProfileReport(pandas_df_SS_Datos_Basicos_PN, title="Profiling - SS - Datos Basicos PN", explorative=True)
profile.to_file("/tmp/Profiling_SS_Datos_Basicos_PN.html")
s3_client.upload_file('/tmp/Profiling_SS_Datos_Basicos_PN.html', 'alfa-datalake-dev-langding', 'Clientes_Test/dataprofiling/Profiling_SS_Datos_Basicos_PN.html')
print("Generación Terminada")
file = 's3://alfa-datalake-dev-langding/Clientes_Test/Datos_Demograficos_PN_V2.xlsx'
sheet = "Exportar Hoja de Trabajo"
with warnings.catch_warnings(record=True):
  warnings.simplefilter("always")
  pandas_df_SS_Datos_Demograficos_PN = pd.read_excel(file, sheet_name=sheet, engine="openpyxl")
pandas_df_SS_Datos_Demograficos_PN.dtypes
profile = ProfileReport(pandas_df_SS_Datos_Demograficos_PN, title="Profiling - SS - Datos Demograficos PN - V2", explorative=True)
profile.to_file("/tmp/Profiling_SS_Datos_Demograficos_PN_V2.html")
s3_client.upload_file('/tmp/Profiling_SS_Datos_Demograficos_PN_V2.html', 'alfa-datalake-dev-langding', 'Clientes_Test/dataprofiling/Profiling_SS_Datos_Demograficos_PN_V2.html')
print("Generación Terminada")
file = 's3://alfa-datalake-dev-langding/Clientes_Test/Datos_Ubicacion_PN_V2.xlsx'
sheet = "Exportar Hoja de Trabajo"
with warnings.catch_warnings(record=True):
  warnings.simplefilter("always")
  pandas_df_SS_Datos_Ubicacion_PN = pd.read_excel(file, sheet_name=sheet, engine="openpyxl")
pandas_df_SS_Datos_Ubicacion_PN.dtypes
profile = ProfileReport(pandas_df_SS_Datos_Ubicacion_PN, title="Profiling - SS - Datos Ubicación PN", explorative=True)
profile.to_file("/tmp/Profiling_SS_Datos_Ubicacion_PN.html")
s3_client.upload_file('/tmp/Profiling_SS_Datos_Ubicacion_PN.html', 'alfa-datalake-dev-langding', 'Clientes_Test/dataprofiling/Profiling_SS_Datos_Ubicacion_PN.html')
print("Generación Terminada")
file = 's3://alfa-datalake-dev-langding/Clientes_Test/Datos_Demograficos_PJ.xlsx'
sheet = "Exportar Hoja de Trabajo"
with warnings.catch_warnings(record=True):
  warnings.simplefilter("always")
  pandas_df_SS_Datos_Demograficos_PJ = pd.read_excel(file, sheet_name=sheet, engine="openpyxl")
pandas_df_SS_Datos_Demograficos_PJ.dtypes
profile = ProfileReport(pandas_df_SS_Datos_Demograficos_PJ, title="Profiling - SS - Datos Ubicación PJ", explorative=True)
profile.to_file("/tmp/Profiling_SS_Datos_Demograficos_PJ.html")
s3_client.upload_file('/tmp/Profiling_SS_Datos_Demograficos_PJ.html', 'alfa-datalake-dev-langding', 'Clientes_Test/dataprofiling/Profiling_SS_Datos_Demograficos_PJ.html')
print("Generación Terminada")
file = 's3://alfa-datalake-dev-langding/Clientes_Test/Datos_Ubicacion_PJ.xlsx'
sheet = "Exportar Hoja de Trabajo"
with warnings.catch_warnings(record=True):
  warnings.simplefilter("always")
  pandas_df_SS_Datos_Ubicacion_PJ = pd.read_excel(file, sheet_name=sheet, engine="openpyxl")
pandas_df_SS_Datos_Ubicacion_PJ.dtypes
profile = ProfileReport(pandas_df_SS_Datos_Ubicacion_PJ, title="Profiling - SS - Datos Ubicación PJ", explorative=True)
profile.to_file("/tmp/Profiling_SS_Datos_Ubicacion_PJ.html")
s3_client.upload_file('/tmp/Profiling_SS_Datos_Ubicacion_PJ.html', 'alfa-datalake-dev-langding', 'Clientes_Test/dataprofiling/Profiling_SS_Datos_Ubicacion_PJ.html')
print("Generación Terminada")
file = 's3://alfa-datalake-dev-langding/Clientes_Test/Acsele_Datos_Basicos_PJ.xlsx'
sheet = "Exportar Hoja de Trabajo"
with warnings.catch_warnings(record=True):
  warnings.simplefilter("always")
  pandas_df_ACSELE_Datos_Basicos_PJ = pd.read_excel(file, sheet_name=sheet, engine="openpyxl")
pandas_df_ACSELE_Datos_Basicos_PJ.dtypes
profile = ProfileReport(pandas_df_ACSELE_Datos_Basicos_PJ, title="Profiling - ACSELE - Datos Basicos PJ", explorative=True)
profile.to_file("/tmp/Profiling_ACSELE_Datos_Basicos_PJ.html")
s3_client.upload_file('/tmp/Profiling_ACSELE_Datos_Basicos_PJ.html', 'alfa-datalake-dev-langding', 'Clientes_Test/dataprofiling/Profiling_ACSELE_Datos_Basicos_PJ.html')
print("Generación Terminada")
file = 's3://alfa-datalake-dev-langding/Clientes_Test/Acsele_Datos_Basicos_PN.xlsx'
sheet = "Exportar Hoja de Trabajo"
with warnings.catch_warnings(record=True):
  warnings.simplefilter("always")
  pandas_df_ACSELE_Datos_Basicos_PN = pd.read_excel(file, sheet_name=sheet, engine="openpyxl")
pandas_df_ACSELE_Datos_Basicos_PN.dtypes
profile = ProfileReport(pandas_df_ACSELE_Datos_Basicos_PN, title="Profiling - ACSELE - Datos Basicos PN", explorative=True)
profile.to_file("/tmp/Profiling_ACSELE_Datos_Basicos_PN.html")
s3_client.upload_file('/tmp/Profiling_ACSELE_Datos_Basicos_PN.html', 'alfa-datalake-dev-langding', 'Clientes_Test/dataprofiling/Profiling_ACSELE_Datos_Basicos_PN.html')
print("Generación Terminada")
file = 's3://alfa-datalake-dev-langding/Clientes_Test/Acsele_Datos_Basicos_AV.xlsx'
sheet = "Exportar Hoja de Trabajo"
with warnings.catch_warnings(record=True):
  warnings.simplefilter("always")
  pandas_df_ACSELE_Datos_Basicos_AV = pd.read_excel(file, sheet_name=sheet, engine="openpyxl")
pandas_df_ACSELE_Datos_Basicos_AV.dtypes
profile = ProfileReport(pandas_df_ACSELE_Datos_Basicos_AV, title="Profiling - ACSELE - Datos Basicos AV", explorative=True)
profile.to_file("/tmp/Profiling_ACSELE_Datos_Basicos_AV.html")
s3_client.upload_file('/tmp/Profiling_ACSELE_Datos_Basicos_AV.html', 'alfa-datalake-dev-langding', 'Clientes_Test/dataprofiling/Profiling_ACSELE_Datos_Basicos_AV.html')
print("Generación Terminada")
file = 's3://alfa-datalake-dev-langding/Clientes_Test/Cardiff_v1.xlsx'
sheet = "Exportar Hoja de Trabajo"
with warnings.catch_warnings(record=True):
  warnings.simplefilter("always")
  pandas_df_CARDIFR_Clientes = pd.read_excel(file, sheet_name=sheet, engine="openpyxl")
pandas_df_CARDIFR_Clientes.dtypes
profile = ProfileReport(pandas_df_CARDIFR_Clientes, title="Profiling - Cardiff - Full 30%", explorative=True)
profile.to_file("/tmp/Profiling_Cardiff_Clientes.html")
s3_client.upload_file('/tmp/Profiling_Cardiff_Clientes.html', 'alfa-datalake-dev-langding', 'Clientes_Test/dataprofiling/Profiling_Cardiff_Clientes.html')
print("Generación Terminada")
file = 's3://alfa-datalake-dev-langding/Clientes_Test/Datos_Basicos_Porvenir.xlsx'
sheet = "Exportar Hoja de Trabajo"
with warnings.catch_warnings(record=True):
  warnings.simplefilter("always")
  pandas_df_Porvenir_Clientes = pd.read_excel(file, sheet_name=sheet, engine="openpyxl")
pandas_df_Porvenir_Clientes.dtypes
profile = ProfileReport(pandas_df_Porvenir_Clientes, title="Profiling - Porvenir - Full 10%", explorative=True)
profile.to_file("/tmp/Profiling_Porvenir_Clientes.html")
s3_client.upload_file('/tmp/Profiling_Porvenir_Clientes.html', 'alfa-datalake-dev-langding', 'Clientes_Test/dataprofiling/Profiling_Porvenir_Clientes.html')
print("Generación Terminada")
file = 's3://alfa-datalake-dev-langding/Clientes_Test/ServiAlfa_V1.xlsx'
sheet = "Sheet1"
with warnings.catch_warnings(record=True):
  warnings.simplefilter("always")
  pandas_df_Servialfa_Clientes = pd.read_excel(file, sheet_name=sheet, engine="openpyxl")
pandas_df_Servialfa_Clientes.dtypes
profile = ProfileReport(pandas_df_Servialfa_Clientes, title="Profiling - ServiAlfa - Full", explorative=True)
profile.to_file("/tmp/Profiling_ServiAlfa_Clientes.html")
s3_client.upload_file('/tmp/Profiling_ServiAlfa_Clientes.html', 'alfa-datalake-dev-langding', 'Clientes_Test/dataprofiling/Profiling_ServiAlfa_Clientes.html')
print("Generación Terminada")
## Cliente - Porvenir - Producción
file = 's3://alfa-datalake-dev-langding/Clientes_Test/Datos_Basicos_Porvenir_Produccion.xlsx'
sheet = "Exportar Hoja de Trabajo"
with warnings.catch_warnings(record=True):
  warnings.simplefilter("always")
  pandas_df_Porvenir_Clientes_prd = pd.read_excel(file, sheet_name=sheet, engine="openpyxl")
pandas_df_Porvenir_Clientes_prd.dtypes
profile = ProfileReport(pandas_df_Porvenir_Clientes_prd, title="Profiling - Porvenir prd - Full 20%", explorative=True)
profile.to_file("/tmp/Profiling_Porvenir_Clientes_prd.html")
s3_client.upload_file('/tmp/Profiling_Porvenir_Clientes_prd.html', 'alfa-datalake-dev-langding', 'Clientes_Test/dataprofiling/Profiling_Porvenir_Clientes_prd.html')
print("Generación Terminada")
dyframe = glueContext.create_dynamic_frame.from_catalog(database='datalake-dev-bronce-catalog-db', table_name='alfaprd_general_acu_personas_naturales')
dyframe.printSchema()
dataFrame = dyframe.toDF()
pandas_df = dataFrame.select("COD_TIPO_IDENTIFICACION", "NID_NUMERO_IDENTIFICACION", "PRIMER_APELLIDO", "SEGUNDO_APELLIDO", "PRIMER_NOMBRE", "SEGUNDO_NOMBRE", 
                     "COD_CIUDAD_EXPEDICION_DOCU", "COD_ESTADO", "COD_CIUDAD_NACIMIENTO", "COD_DEPARTAMENTO_NACIMIENTO",
                     "NACIONALIDAD", "GENERO", "COD_ESTADO_CIVIL", "COD_NIVEL_EDUCATIVO", "COD_PROFESION", "COD_OCUPACION", "COD_ACTIVIDAD_ECONOMICA_CIIU").toPandas()
pandas_df
profile = ProfileReport(pandas_df, title="Profiling Report - acu_personas_naturales", explorative=True)
profile.to_file("/tmp/profiling_acu_personas_naturales.html")
s3 = boto3.client('s3')
s3.upload_file('/tmp/profiling_acu_personas_naturales.html', 'alfa-datalake-dev-assets', 'dataprofiling/profiling_reportacu_personas_naturales.html')

#Parámetros 
s3 = boto3.client('s3')
bucket_name = 'alfa-datalake-dev-langding'
file_key = 'Clientes_Test/Datos_Basicos_PJ_V2.xlsx'
# Obtén el objeto de S3 
obj = s3.get_object(Bucket=bucket_name, Key=file_key)
# Lee el contenido del archivo Excel 
pandas_df_Cliente_Unico = pd.read_excel(BytesIO(obj['Body'].read()))
dyframe_Prethirdparty = glueContext.create_dynamic_frame.from_catalog(database='datalake-dev-bronce-catalog-db', table_name='acsele_acsele_alfa_prethirdparty')
dyframe_Prethirdparty.printSchema()
dataFrame_Prethirdparty = dyframe_Prethirdparty.toDF()
pandas_df_Prethirdparty = dataFrame_Prethirdparty.toPandas()
pandas_df_Prethirdparty
profilePrethirdparty = ProfileReport(pandas_df, title="Profiling Report - Prethirdparty", explorative=True)
profilePrethirdparty.to_file("/tmp/profiling_Prethirdparty.html")
file = 's3://alfa-datalake-dev-langding/CARDIFF/DOM_CUSTOMERS_FULL.txt'
with warnings.catch_warnings(record=True):
  warnings.simplefilter("always")
  pandas_df_cardif = pd.read_csv(file, delimiter="|")


pandas_df_cardif = pandas_df_cardif.sample(frac = 0.5)
profile = ProfileReport(pandas_df_cardif, title="Profiling - Cardiff prd - Full 100%", explorative=True)
profile.to_file("/tmp/Profiling_Cardiff_Clientes_prd.html")
s3_client.upload_file('/tmp/Profiling_Cardiff_Clientes_prd.html', 'alfa-datalake-dev-langding', 'Clientes_Test/dataprofiling/Profiling_Cardiff_Clientes_prd.html')
print("Generación Terminada")
dyframe_Prethirdparty = glueContext.create_dynamic_frame.from_catalog(database='alfa-datalake-dev-silver', table_name='clientes_datos_ubicacion_y_contactabilidad_stg')
dyframe_Prethirdparty.printSchema()
dataFrame_Prethirdparty = dyframe_Prethirdparty.toDF()
dataFrame_ACSELE = dataFrame_Prethirdparty[dataFrame_Prethirdparty["cod_fuente_fk"] == 1]
dataFrame_ACSELE = dataFrame_ACSELE.drop('partition_0', 'partition_1', 'partition_2', 'job_cargue_id', 'fecha_cargue', 'cod_fuente_fk', 'customer_dubic_contact_id')
dataFrame_ACSELE
dataFrame_ACSELE_Muestra = dataFrame_ACSELE.sample(False, 0.01, seed=123)
dataFrame_ACSELE_Muestra.head()
dataFrame_ACSELE_Muestra.count()
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

def formatear_columnas(df: DataFrame, columnas: list, decimales: int = 2, default: str = "N/A") -> DataFrame:
    """
    Aplica formato numérico seguro a varias columnas en un DataFrame de PySpark.
    Si el valor es null, asigna un valor por defecto.
    
    :param df: DataFrame de PySpark.
    :param columnas: Lista de nombres de columnas a formatear.
    :param decimales: Número de decimales para el formato.
    :param default: Valor por defecto si es null.
    :return: DataFrame con las columnas formateadas como string.
    """
    for col in columnas:
        if col in df.columns:
            df = df.withColumn(
                col,
                F.when(F.col(col).isNotNull(), F.format_number(F.col(col), decimales))
                 .otherwise(F.lit(default))
            )
    return df


# Supongamos que tienes columnas numéricas: ["precio", "descuento", "impuesto"]
columnas_a_formatear = ["cod_ciudad", "cod_departamento", "cod_pais", "indicativo", "numero_telefono", "extension"]

df_formateado = formatear_columnas(dataFrame_ACSELE_Muestra, columnas_a_formatear, decimales=2, default="N/A")

df_formateado.show()

profile = ProfileReport(dataFrame_ACSELE_Muestra, title="Profiling Report - Ubucacion ACSELE", minimal=True, correlations=None, explorative=False)
profile.to_file("/tmp/profiling_ubicacion_acsele.html")
s3 = boto3.client('s3')
s3.upload_file('/tmp/profiling_ubicacion_acsele.html', 'alfa-datalake-dev-assets', 'dataprofiling/profiling_ubicacion_acsele.html')
# Convertir JSON a CSV
#stats = profile.get_description()  # Obtiene el diccionario con estadísticas
pd.DataFrame(profile.to_json()).to_csv("/tmp/profiling_ubicación_acsele.csv")
s3.upload_file('/tmp/profiling_ubicacion_acsele.csv', 'alfa-datalake-dev-assets', 'dataprofiling/profiling_ubicacion_acsele.csv')
json_data = profile.to_json()

# Normalizar
df = pd.json_normalize(json_data)
# Guardar como CSV
df.to_csv("/tmp/profiling_ubicación_acsele.csv", index=False)
s3.upload_file('/tmp/profiling_ubicacion_acsele.csv', 'alfa-datalake-dev-assets', 'dataprofiling/profiling_ubicacion_acsele.csv')

# Exportar estadísticas a JSON
profile.to_file("/tmp/reporte.json")




import json

# Cargar JSON anidado
with open("/tmp/reporte.json") as f:
    data = json.load(f)

# Normalizar
df = pd.json_normalize(data)

# Guardar como CSV
df.to_csv("/tmp/archivo.csv", index=False)

# Guardar como CSV
s3.upload_file('/tmp/archivo.csv', 'alfa-datalake-dev-assets', 'dataprofiling/profiling_ubicacion_acsele.csv')
job.commit()