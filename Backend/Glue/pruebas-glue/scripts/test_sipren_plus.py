 
 
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
connection_oracle_options = {
"useConnectionProperties":"true",
"connectionName":"sipren_plus_test",
"dbtable": "RESERVAS.RSV_DATO_RESERVA"
}
 
# Leer DynamicFrame de base de datos RDS Oracle
df_oracle = glueContext.create_dynamic_frame.from_options(connection_type="oracle",
connection_options=connection_oracle_options)
 
df_oracle.show()
connection_oracle11_options = { "url": "jdbc:oracle:thin:@//dev-sipren-oracle-ee-rds.ccny821dt6zx.us-east-1.rds.amazonaws.com:1521/SIPREN", "dbtable": "CLIENTE_UNICO.VW_CONSULTA_CLIENTE", "user": "admin", "password": "qzmMHPJ,557|$"} 

df_oracle11 = glueContext.create_dynamic_frame.from_options(connection_type="oracle", connection_options=connection_oracle11_options)
print("Número de registros:", df_oracle11.count())
from commons.ExtractHelper import ExtractHelper
from commons.IncrementalCharge import IncrementalCharge
ExtractHelper = ExtractHelper(glueContext, spark, 'PLUS', 'alfa-datalake-dev-bronce')
IncrementalCharge = IncrementalCharge(glueContext, spark, 'PLUS', 'alfa-datalake-dev-bronce')
ExtractHelper.execute_data_incremental(table_name='VW_CONSULTA_CLIENTE',filter_field='NID_NUMERO_IDENTIFICACION',fields='*',table_base='VW_CONSULTA_CLIENTE',sql_script='SELECT * FROM CLIENTE_UNICO.VW_CONSULTA_CLIENTE',connectname='Jdbc_sipren_vpn',filter_value=0, delete_pk_field = 0, bd='PLUS')

my_tabla = self._glue_context.create_dynamic_frame.from_options(
        connection_type = "oracle",
        connection_options = connect_option,
            transformation_ctx = "MyTabla"
        )

data = [
        ("1001",  "1980-01-01", "M"),
        ("24615540", None, "F"),
        ("24615540", "1941-10-31", "F"),
        ("79898673", "1985-03-12", None),
        ("79898673", "1985-03-12", "M")
    ]
columns = ["cedula", "fecha_nacimiento", "genero"]
df = spark.createDataFrame(data, columns)
df_result = create_asegurado(df)

df_result.show()
results = [row.asDict() for row in df_result.collect()]
assert results == [
    {"cedula": "1001", "fecha_nacimiento": "1980-01-01", "genero": "M"},
    {"cedula": "24615540", "fecha_nacimiento": "1941-10-31", "genero": "F"},
    {"cedula": "24615540", "fecha_nacimiento": "1941-10-31", "genero": "F"},
    {"cedula": "79898673", "fecha_nacimiento": "1985-03-12", "genero": "M"},
    {"cedula": "79898673", "fecha_nacimiento": "1985-03-12", "genero": "M"}
]
from pyspark.sql import functions as F,Window
from pyspark.sql import Window
def create_asegurado(df):
    """
    This function help us to create a insurance data of the point source.
    """
    groupwindowaseg = Window.partitionBy(F.col("cedula"))
    default_value = 'No registra'
    regular_expresion =  r'[^0-9]'



    df = df.withColumn(
        "fecha_nacimiento",
        F.when(
            F.col("fecha_nacimiento").isNull(),  # Si es nulo
            F.first("fecha_nacimiento", ignorenulls=True).over(groupwindowaseg) # Tomar el primer valor válido en la ventana
        ).otherwise(
            F.col("fecha_nacimiento")  # Si ya tiene valor, lo mantiene
        )
    )

    df = df.withColumn(
        "cedula",
        F.when(
            F.col("cedula").isNull(),
            F.first(
                F.regexp_replace("cedula", regular_expresion, ''),
                ignorenulls=True
            ).over(groupwindowaseg)
        ).otherwise(
            F.when(
                (F.trim(F.regexp_replace("cedula", regular_expresion, '')) =="") |
                (F.regexp_replace("cedula", regular_expresion, '').isNull()) ,
                "No registra" 
            ).otherwise(
                F.regexp_replace("cedula", regular_expresion, '')
            )
        )     
    )
    df = df.withColumn("genero", F.col("genero").cast("string"))

    df = df.withColumn(
            "genero",
            F.first(
                F.when(F.col("genero").isin("M", "F"), F.col("genero")), 
                ignorenulls=True
            ).over(groupwindowaseg)
    )

    df = df.withColumn(
        "genero",
        F.when(
            F.col("genero").isNull() &
            (F.length(F.col("cedula")) <= 8), 
            F.when(
                (F.col("cedula").cast("int").between(10000, 19999999)) | 
                (F.col("cedula").cast("int").between(70000000, 79999999)), 
                "M"
            ).when(
                F.col("cedula").cast("int").between(20000000, 69999999), 
                "F"
            ).otherwise(default_value)
        ).otherwise(F.col("genero"))
    )

    df = df.withColumn(
        "genero",
        F.first(F.col("genero"), ignorenulls=True).over(groupwindowaseg)
    ) 
    df = df.withColumn(
        "genero",
        F.when(
            F.col("genero").isNull(),  # Si sigue siendo nulo
            F.lit(default_value)  # Asignar "No registra"
        ).otherwise(
            F.col("genero")  # Si tiene un valor, lo mantiene
        )
    )
    df = df.distinct()


    df = df.withColumn(
        "id_asegurado_pk",
        (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) + 1000000000).cast("int")
    )

    df = df.select(
        F.col("id_asegurado_pk").cast("int").alias("ID_ASEGURADO_PK"),
        F.col("cedula").cast("string").alias("CEDULA"),
        F.col("fecha_nacimiento").cast("date").alias("FECHA_NACIMIENTO"),
        F.col("genero").cast("string").alias("GENERO")
    )

    return df
job.commit()