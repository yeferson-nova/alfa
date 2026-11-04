
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from commons.ExtractHelper import ExtractHelper
import boto3
import datetime
from pyspark.sql import functions as F 

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","LEGACY")
job = Job(glueContext)
ExtractHelper = ExtractHelper(glueContext, spark, "-", "-")
DATE = "2024-11-27"
path = f"s3://alfa-datalake-dev-assets/support_phase_files/componente5/procces_ola0.xlsx"
sheet = "gold_phase"
df_tables = ExtractHelper.extract_excel_data(path, sheet)
sheet = "gold_tables"
df_tmp_tables = ExtractHelper.extract_excel_data(path, sheet)
year = DATE.split("-")[0]
month = DATE.split("-")[1]
day = DATE.split("-")[2]
def charge_tmp_tables(path, table)->None:
    """
    Function to extract data from parquets to df

    arg:
        path: The ubication of patquet files.
        tables: tables to extarct and create tmp views
    return:
        df: Return df with the data of parquet files.
    """
    fields = "*"
    table = table.upper()
    path = f"{path}{table}/"
    df = extract_parquet_data_gold(path, fields)
    df.createOrReplaceGlobalTempView(table)
    print(table)
    
def extract_parquet_data_gold(path, fields):
    """
    Function to extract data from parquets to df

    arg:
        path: The ubication of patquet files.
        fields: Fields that we need return by the function.
    return:
        df: Return df with the data of parquet files.
    """
    parquet_data = glueContext.create_dynamic_frame.from_options(
        format_options={},
        connection_type="s3",
        format="parquet",
        connection_options=
        {
            "paths": 
                [f"{path}{year}/{month}/{day}"],
            "recurse": True
        },
        transformation_ctx="AmazonS3"
    )
    df = parquet_data.toDF()
    return df
for row in df_tmp_tables.collect():
        table = row["Table"].upper()
        path_source =f"s3://alfa-datalake-dev-silver/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}_SILVER_"
        charge_tmp_tables(path_source, table)

from pyspark.sql import functions as F
from pyspark.sql import Window

class prueba:
    def __init__(self, glue_context, spark):
        self.spark = spark

    def create_urincterrdeudores(self):
        """
        This Function to create urincterrdeudores Table.
        """

        df_claim = self.spark.table("global_temp.CLAIM").alias("CLI")
        df_contextoperation = self.spark.table("global_temp.CONTEXTOPERATION").alias("CO")
        df_policydco = self.spark.table("global_temp.POLICYDCO").alias("PO")
        df_prepolicy = self.spark.table("global_temp.PREPOLICY").alias("PPOL")
        df_riskunitdco = self.spark.table("global_temp.RISKUNITDCO").alias("RI")
        df_agregatedpolicy = self.spark.table("global_temp.AGREGATEDPOLICY").alias("AGP")
        df_preproduct = self.spark.table("global_temp.PREPRODUCT").alias("PPRO")
        df_urvdeudores = self.spark.table("global_temp.URVDEUDORES").alias("URPOL")
        df_urincterrdeudores = self.spark.table("global_temp.URINCTERRDEUDORES").alias("URINC")
        df_cierresgenerales = self.spark.table("global_temp.CIERRESGENERALES").alias("T2")
        df_cierrevida = self.spark.table("global_temp.CIERREVIDA").alias("T3")



        result_df = (
            df_claim
            .join(df_contextoperation, (F.col("CO.ID") == F.col("CLI.OPERATIONPK")) & (F.col("CO.STATUS") == 2), "inner")
            .join(df_policydco, F.col("PO.OPERATIONPK") == F.col("CO.ID"), "inner")
            .join(df_prepolicy, F.col("PPOL.PK") == F.col("PO.DCOID"), "inner")
            .join(df_riskunitdco, F.col("RI.OPERATIONPK") == F.col("CO.ID"), "inner")
            .join(df_agregatedpolicy, F.col("AGP.AGREGATEDPOLICYID") == F.col("PO.AGREGATEDOBJECTID"), "inner")
            .join(df_preproduct, F.col("PPRO.STATIC") == F.col("AGP.PRODUCTID"), "inner")
            .join(df_urvdeudores, F.col("RI.DCOID") == F.col("URPOL.PK"), "left")
            .join(df_urincterrdeudores, F.col("RI.DCOID") == F.col("URINC.PK"), "left")
            .select(
                F.col("URPOL.FECHADESEMBOLSOINPUT")
            ).distinct()
        )
        
        return df
        
prueba = prueba(glueContext, spark)
df_detallada = prueba.create_urincterrdeudores()
from pyspark.sql import functions as F
from pyspark.sql import Window

class UrincterrdeudoresHelper:
    def __init__(self, glue_context, spark):
        self.spark = spark

    def create_urincterrdeudores(self):
        """
        This Function to create urincterrdeudores Table.
        """

        df_claim = self.spark.table("global_temp.CLAIM").alias("CLI")
        df_contextoperation = self.spark.table("global_temp.CONTEXTOPERATION").alias("CO")
        df_policydco = self.spark.table("global_temp.POLICYDCO").alias("PO")
        df_prepolicy = self.spark.table("global_temp.PREPOLICY").alias("PPOL")
        df_riskunitdco = self.spark.table("global_temp.RISKUNITDCO").alias("RI")
        df_agregatedpolicy = self.spark.table("global_temp.AGREGATEDPOLICY").alias("AGP")
        df_preproduct = self.spark.table("global_temp.PREPRODUCT").alias("PPRO")
        df_urvdeudores = self.spark.table("global_temp.URVDEUDORES").alias("URPOL")
        df_urincterrdeudores = self.spark.table("global_temp.URINCTERRDEUDORES").alias("URINC")
        df_cierresgenerales = self.spark.table("global_temp.CIERRESGENERALES").alias("T2")
        df_cierrevida = self.spark.table("global_temp.CIERREVIDA").alias("T3")



        result_df = (
            df_claim
            .join(df_contextoperation, (F.col("CO.ID") == F.col("CLI.OPERATIONPK")) & (F.col("CO.STATUS") == 2), "inner")
            .join(df_policydco, F.col("PO.OPERATIONPK") == F.col("CO.ID"), "inner")
            .join(df_prepolicy, F.col("PPOL.PK") == F.col("PO.DCOID"), "inner")
            .join(df_riskunitdco, F.col("RI.OPERATIONPK") == F.col("CO.ID"), "inner")
            .join(df_agregatedpolicy, F.col("AGP.AGREGATEDPOLICYID") == F.col("PO.AGREGATEDOBJECTID"), "inner")
            .join(df_preproduct, F.col("PPRO.STATIC") == F.col("AGP.PRODUCTID"), "inner")
            .join(df_urvdeudores, F.col("RI.DCOID") == F.col("URPOL.PK"), "left")
            .join(df_urincterrdeudores, F.col("RI.DCOID") == F.col("URINC.PK"), "left")
            .select(
                F.col("CLI.CLAIMNUMBER").alias("id_siniestro_fk"),
                F.concat(F.col("CLI.POLICYID").cast("string"), F.lit("-"), F.col("CLI.OPERATIONPK").cast("string")).alias("id_poliza_fk"),
                F.when(F.col("PPRO.RAMOVALUE") == '22.0',
                    F.when(F.col("URPOL.FECHADESEMBOLSOINPUT").isNotNull(), F.col("URPOL.FECHADESEMBOLSOINPUT").cast("date"))
                    .otherwise(F.col("URINC.FECHADESEMBOLSOINPUT").cast("date"))
                ).otherwise(None).alias("FECHA_DESEMBOLSO_CREDITO"),
                F.when(F.col("PPRO.RAMOVALUE") != '22.0', F.col("PO.INITIALDATE")).otherwise(None).alias("fecha_inicio_vigencia"),
                F.lit("1").alias("id_fuente_fk")
            ).distinct()
        )

       
        result_df = result_df.union(
            df_cierresgenerales.select(
                F.col("T2.numero_siniestro").alias("id_siniestro_fk"),
                F.concat(F.col("T2.simbolo"), F.lit("-"), F.col("T2.cons_poliza"), F.lit("-"), F.col("T2.sucursal")).alias("id_poliza_fk"),
                F.when(F.col("T2.ramo") == '22', F.col("T2.fecha_desembolso")).otherwise(None).alias("fecha_desembolso_credito"),
                F.when(F.col("T2.ramo") != '22', F.col("T2.vigencia_desde_poliza").cast("date")).otherwise(None).alias("fecha_inicio_vigencia"),
                F.lit("0").alias("id_fuente_fk")
            ).distinct()
        )

        
        result_df = result_df.union(
            df_cierrevida.select(
                F.col("T3.numero_siniestro").alias("id_siniestro_fk"),
                F.concat(F.col("T3.simbolo"), F.lit("-"), F.col("T3.cons_poliza"), F.lit("-"), F.col("T3.sucursal")).alias("id_poliza_fk"),
                F.when(F.col("T3.ramo") == '22', F.col("T3.fecha_desembolso")).otherwise(None).alias("fecha_desembolso_credito"),
                F.when(F.col("T3.ramo") != '22', F.col("T3.vigencia_desde_poliza").cast("date")).otherwise(None).alias("fecha_inicio_vigencia"),
                F.lit("0").alias("id_fuente_fk")
            ).distinct()
        )

        

        siniestros_count_df = result_df.groupBy("id_siniestro_fk").agg(F.countDistinct("id_poliza_fk").alias("polizas_count"))
        

        result_df_single_policy = result_df.join(siniestros_count_df, "id_siniestro_fk", "inner").filter(F.col("polizas_count") == 1)
        
        distinct_df_claim= result_df_single_policy.drop_duplicates()

        window_spec = Window.partitionBy("id_siniestro_fk", "id_poliza_fk").orderBy(
            F.greatest(
                F.coalesce(F.col("fecha_desembolso_credito"), F.lit("1900-01-01")),
                F.coalesce(F.col("fecha_inicio_vigencia"), F.lit("1900-01-01"))
            ).desc()
        )

        final_result_df = distinct_df_claim.withColumn(
            "row_num",
            F.row_number().over(window_spec)
        )

        df_filter = final_result_df.filter(F.col("row_num") == 1).drop("row_num")

   
        df = df_filter.withColumn(
            "id_poliza_detallada_pk",
            (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) + 1000000000).cast("int")
        )
        
        df = df.drop("polizas_count")

        return df
Urincterrdeudores_helper = UrincterrdeudoresHelper(glueContext, spark)
df_detallada = Urincterrdeudores_helper.create_urincterrdeudores()
df_detallada.filter((F.col("id_siniestro_fk") == "500001-2024-008280")).show()
df_cierresgenerales.filter(F.col("numero_siniestro") =='010121012889').show()
EstadoSiniestroHelperA = EstadoSiniestroHelperA(glueContext, spark)
df_estado= EstadoSiniestroHelperA.create_estado_siniestro() 
df_estado.filter((F.col("id_siniestro_fk") == "010121012889")).show()
df_test = spark.sql(
"""
SELECT *
FROM global_temp.tipo_transaccion""")
from pyspark.sql import functions as F 
def process_point(df, fuente_fk):
            return df.select(
                F.col("p.numero_siniestro").alias("id_siniestro_fk"),
                F.month("p.fecha_transaccion").alias("mes"),
                F.year("p.fecha_transaccion").alias("ano"),
                F.when(F.col("p.tipo_transaccion") == 'Constitución reserva', 'Avisado')
                .when((F.col("p.tipo_transaccion") == 'Pago') & (F.col("p.valor_transaccion") > 0), 'Pagado')
                .when(F.col("p.tipo_transaccion") == 'Liberación reserva', 'Objetado')
                .otherwise('En Reserva').alias("estado"),
                F.lit(fuente_fk).alias("id_fuente_fk")
                ##F.col("FECHA_OBJECION").alias("FECHA_OBJECION"),
            ).distinct()

 
df_cierrevida = spark.table("global_temp.cierrevida_comp5").alias("p")
df_cierresgenerales = spark.table("global_temp.cierresgenerales_comp5").alias("p")
df_cierrevida_processed = process_point(df_cierrevida, 0)
df_cierresgenerales_processed = process_point(df_cierresgenerales, 0)
df_cierrevida_processed.filter(F.col("numero_siniestro") == "180221018274").show()
df_cierresgenerales_processed.filter(F.col("numero_siniestro") == "180221018274").show()
df_test.printSchema()
import datetime
from pyspark.sql import functions as F,Window
path = f"s3://alfa-datalake-dev-assets/support_phase_files/DIM_CAUSA_SINIESTRO.xlsx"
sheet = "data"
df_causa_siniestro = ExtractHelper.extract_excel_data(path, sheet)
df_causa_siniestro.show(2)
df_causa_siniestro = df_causa_siniestro.drop("Unnamed: 2")
df_causa_siniestro.show(2)
path = f"s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-DIM_CAUSA_SINIESTRO/"
ExtractHelper.save_files(df_causa_siniestro,path)
df_homologaciones_dos_test = df_homologaciones_dos.filter((F.col("campo_homologacion") == "COD_CAUSA") & (F.col("tabla") == "cierresgenerales"))
df_homologaciones_dos_test.show(2)
drop_columns = ['tabla','campo_referencia','campo_homologacion']
df_homologaciones_dos_test = df_homologaciones_dos_test.drop(*drop_columns)
df_homologaciones_dos_test.show()
df_causa = df_homologaciones_dos_test.withColumnRenamed("valor_referencia","id_causa_pk").withColumnRenamed("valor_homologacion","causa_siniestro")
path = f"s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-DIM_CAUSA_SINIESTRO/"
ExtractHelper.save_files(df_causa,path)
s3_client = boto3.client("s3")
bucket_name = "alfa-datalake-dev-bronce"
BUCKET_NAME = "alfa-datalake-dev-bronce"
path
path_incre = f"s3://{BUCKET_NAME}/GEN/FILES/GEN-FILES-INCREMENTAL/"
path = f"{path_incre}"
df = ExtractHelper.extract_parquet_data(path, "*", Date)
fields_and_tables = {
    "claim":"claimid"
}
def obtain_field(table_name):
    if table_name in fields_and_tables:
        return fields_and_tables[table_name]
print(obtain_field("claim"))
data = [
    ("CLAIM", "1635559", "2024/11/12")
    ]
df = spark.createDataFrame(data, ["tabla", "max_id", "fecha_ejecucion"])
path_target = f"s3://{bucket_name}/GEN/FILES/GEN-FILES-"
path = f"{path_target}INCREMENTAL/"
ExtractHelper.save_files(df,path)
def get_last_date(bucket_name, path):
    prefix = f"{path}"
    reponse = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")

    if 'CommonPrefixes' in reponse:
        dates = []
        for prefixArr in reponse['CommonPrefixes']:
            date = prefixArr['Prefix'].replace(prefix, '').strip('/')
            dates.append(date)
        
        last_date = max(dates)
        return last_date
    else:
        return "Date not found"

path = 'GEN/FILES/GEN-FILES-INCREMENTAL/'
str_date = ''
for i in range(3):
    date_str = ''
    Date = get_last_date(bucket_name, path)
    date_str = date_str + Date + '/'
    str_date = str_date + Date + '-'
    if date_str != 'Date not found':
        path = path + date_str
path_incre = f"s3://{BUCKET_NAME}/GEN/FILES/GEN-FILES-INCREMENTAL/"
path = f"{path_incre}"
Date = str_date[:-1]
df = ExtractHelper.extract_parquet_data(path, "*", Date)
df.show()
import pyspark.sql.functions as F

tb = 'CLAIM'
df_filter = df.filter(df['tabla'] == tb)
max_date = df_filter.agg(F.max('fecha_ejecucion')).collect()[0][0]
id_max_date = df_filter.filter(df_filter['fecha_ejecucion'] == max_date).select('max_id').collect()[0][0]
print(id_max_date)
from pyspark.sql import functions as F
path_incre = f"s3://alfa-datalake-dev-bronce/GEN/FILES/GEN-FILES-INCREMENTAL/"
Date = "2024-11-20"
path = f"{path_incre}"
df = ExtractHelper.extract_parquet_data(path, "*", Date)
df.show()
max_value = int(df.agg(F.max("CLAIMID")).collect()[0][0])
max_value
ExtractHelper = ExtractHelper(glueContext, spark, "-", "-")
PolizaHelper = PolizaHelper(glueContext, spark)

Urincterrdeudores_helper = UrincterrdeudoresHelper(glueContext, spark)
TransformationHelper = TransformationHelper()
ClaimHelper = ClaimHelper(glueContext, spark)
def parse_schema(schema_str):
    fields = re.split(r',\s*(?=(?:[^()]*\([^()]*\))*[^()]*$)', schema_str)
    schema = []
    type_mapping = {
        "STRING": StringType,
        "INTEGER": IntegerType,
        "BOOLEAN": BooleanType,
        "FLOAT": FloatType,
        "DOUBLE": DoubleType
    }

    for field in fields:
        field = field.strip()
        if ":" not in field:
            print(f"Formato inválido en el esquema: {field}")
            continue
        field_name, field_type = map(str.strip, field.split(":", 1))
        field_type = field_type.upper()  

        try:
            if field_type.startswith("DECIMAL"):
                precision, scale = map(int, re.findall(r'\d+', field_type))
                schema.append(StructField(field_name, DecimalType(precision, scale), True))
            elif field_type in type_mapping:
                schema.append(StructField(field_name, type_mapping[field_type](), True))
            else:
                print(f"Tipo de dato desconocido para el campo {field_name}: {field_type}")
        except Exception as e:
            print(f"Error al procesar el esquema para el campo {field}: {e}")

    return StructType(schema)


def charge_tmp_tables(path, table)->None:
    """
    Function to extract data from parquets to df

    arg:
        path: The ubication of patquet files.
        tables: tables to extarct and create tmp views
    return:
        df: Return df with the data of parquet files.
    """
    fields = "*"
    table = table.upper()
    path = f"{path}{table}/"
    df = extract_parquet_data_gold(path, fields)
    df.createOrReplaceGlobalTempView(table)
    print(table)
    
def extract_parquet_data_gold(path, fields):
    """
    Function to extract data from parquets to df

    arg:
        path: The ubication of patquet files.
        fields: Fields that we need return by the function.
    return:
        df: Return df with the data of parquet files.
    """
    parquet_data = glueContext.create_dynamic_frame.from_options(
        format_options={},
        connection_type="s3",
        format="parquet",
        connection_options=
        {
            "paths": 
                [f"{path}2024/11/18"],
            "recurse": True
        },
        transformation_ctx="AmazonS3"
    )
    df = parquet_data.toDF()
    return df
path = f"s3://alfa-datalake-dev-assets/support_phase_files/componente5/procces_ola0.xlsx"
sheet = "gold_phase"
df_tables = ExtractHelper.extract_excel_data(path, sheet)
sheet = "gold_tables"
df_tmp_tables = ExtractHelper.extract_excel_data(path, sheet)
for row in df_tmp_tables.collect():
        table = row["Table"].upper()
        path_source =f"s3://alfa-datalake-dev-silver/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}_SILVER_"
        print(path_source+table)
        charge_tmp_tables(path_source, table)
        
df = spark.sql(
"""
WITH datos_sin_duplicados AS (
    SELECT DISTINCT
        cedula AS CC_NIT_ASEGURADO,
        fecha_nacimiento AS FECHA_NACIMIENTO_ASEGURADO,
        genero_asegurado AS GENERO_ASEGURADO
    FROM global_temp.asegurado
    UNION ALL
    SELECT DISTINCT
        cc_nit_asegurado AS CC_NIT_ASEGURADO,
        fecha_nacimiento_asegurado AS FECHA_NACIMIENTO_ASEGURADO,
        genero_ AS GENERO_ASEGURADO
    FROM global_temp.cierresgenerales_comp5
    UNION ALL
    SELECT DISTINCT
        cc_nit_asegurado AS CC_NIT_ASEGURADO,
        fecha_nacimiento_asegurado AS FECHA_NACIMIENTO_ASEGURADO,
        genero_ AS GENERO_ASEGURADO
    FROM global_temp.cierrevida_comp5
)
SELECT 
    row_number() over (order by CC_NIT_ASEGURADO) as id_asegurado_pk,
    CC_NIT_ASEGURADO as cedula,
    MAX(FECHA_NACIMIENTO_ASEGURADO) AS fecha_nacimiento,
    MAX(GENERO_ASEGURADO) as genero
FROM datos_sin_duplicados
GROUP BY CC_NIT_ASEGURADO
ORDER BY id_asegurado_pk
"""
)
df.show()
path = f"s3://alfa-datalake-dev-gold/GOLD/CARACT_SINIESTROS/GOLD-CARACT_SINIESTROS-{table}/"
response = ExtractHelper.save_files(df,path)
job.commit()