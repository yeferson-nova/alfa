import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from commons.ExtractHelper import ExtractHelper
from commons.TransformationHelper import TransformationHelper
from pyspark.sql import functions as F
#from compt3.orchestrator.orchestrator_silver import orchestrator_silver
#from compt3.orchestrator.orchestrator_silver_dane import orchestrator_silver_dane
import traceback
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','DB_SCHEMA_BRONCE','DB_SCHEMA_SILVER','BUCKET_NAME_BRONCE','BUCKET_NAME_SILVER','DB_SOURCE','BUCKET_NAME_ASSETS'])

DB_SCHEMA_BRONCE = args['DB_SCHEMA_BRONCE']
DB_SCHEMA_BRONCE = args['DB_SCHEMA_BRONCE']
DB_SCHEMA_SILVER = args['DB_SCHEMA_SILVER']
BUCKET_NAME_BRONCE = args['BUCKET_NAME_BRONCE']
BUCKET_NAME_SILVER = args['BUCKET_NAME_SILVER']
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
DB_SOURCE = args['DB_SOURCE']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

ExtractHelper = ExtractHelper(glueContext, spark, "-", "-")
TransformationHelper = TransformationHelper()

path = f"s3://{BUCKET_NAME_ASSETS}/support_phase_files/jl/procces_ola0.xlsx"
sheet = "silver_tables"
df_tables = ExtractHelper.extract_excel_data(path, sheet)
path = f"s3://{BUCKET_NAME_ASSETS}/support_phase_files/homologaciones.xlsx"
sheet = "Homologaciones_vf"
df_homologaciones_dos = ExtractHelper.extract_excel_data(path, sheet)
path = f"s3://{BUCKET_NAME_ASSETS}/support_phase_files/Normalizacion_Datos.xlsx"
sheet = "Capitalizacion_datos"
df_capitalizacion = ExtractHelper.extract_excel_data(path, sheet)
sheet = "Casteo_datos"
df_casteodata = ExtractHelper.extract_excel_data(path, sheet)

tabla_to_proccess = df_homologaciones_dos.select("tabla").distinct().rdd.flatMap(lambda x:x).collect()

df_homologaciones_principal = df_homologaciones_dos.selectExpr(
"tabla as tabla",
"campo_referencia",
"valor_referencia",
"valor_homologacion",
"campo_homologacion"
)
Date = None
try:
    for row in df_tables.collect():
        table = row["Table"].upper()
        tabla = row["Table"].upper()
        if row["Status"] == "1":
            if row["DB"] == "ACSELE":
                if row["Logic"] == "-":
                    path_source = f"s3://{BUCKET_NAME_BRONCE}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-{row['SCHEMA'].upper()}-"
                    path = f"{path_source}{table}/"
                    df = ExtractHelper.extract_parquet_data(path, row["Fields"], Date)
                    if table.lower() in tabla_to_proccess:
                        df = TransformationHelper.homologacion_i(table, df_homologaciones_principal, df_homologaciones_principal, df, 'homologacion')
                    for capitalizacion in df_capitalizacion.collect():
                        if capitalizacion["TABLA"].upper() == row["Table"].upper():
                            df = TransformationHelper.data_normalice(capitalizacion["CAMPO"],df)
                    df = TransformationHelper.date_normalice(df)
                    for capitalizacion in df_capitalizacion.collect():
                        if capitalizacion["TABLA"].upper() == row["Table"].upper():
                            df = TransformationHelper.data_normalice(capitalizacion["CAMPO"],df)
                    if table.lower() in tabla_to_proccess:
                        df = TransformationHelper.homologacion_i(table, df_homologaciones_principal,df_homologaciones_principal, df, 'remediacion')
                    for rowcast in df_casteodata.collect():
                        if row['Table'].upper() == rowcast['TABLA'].upper():
                            df = TransformationHelper.data_cast(rowcast['CAMPO'].upper(),rowcast['NORMALIZACION'], df)
                    df = TransformationHelper.data_normalice_string(df, None)
                    path_target = f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_"
                    path = f"{path_target}{table}/"
                    ExtractHelper.save_files(df,path)
                else:
                    print(f"entro a esta tabla {table}")
                    path_source = f"s3://{BUCKET_NAME_BRONCE}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-{row['SCHEMA'].upper()}-"
                    path = f"{path_source}{table}/"
                    df = ExtractHelper.extract_parquet_data(path, row["Fields"], Date)
                    df.createTempView(table)
                    if table.upper() in ('REINCAPACIDADTOTPERMAN','REMUERTEVDEUDOR','REENFERMEDADESGVDEUDOR','REINCTERRDEUDORES','REAUXILIOFUNERARIOVDEUDOR','REVIDAAP'):
                        df_ubi = df.select("pk","DEPARTAMENTOINPUT","DEPARTAMENTOVALUE","CIUDADINPUT","CIUDADVALUE").distinct()
                        df_ubi = orchestrator_silver(args, glueContext, spark, table, df_ubi)
                        path_ubi = f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_UBICACION_ACSELE"
                        ExtractHelper.save_files(df_ubi, path_ubi)
                    df = spark.sql(row['Logic'])
                    if table.lower() in tabla_to_proccess:
                        df = TransformationHelper.homologacion_i(table, df_homologaciones_principal,df_homologaciones_principal, df, 'homologacion')
                    df = TransformationHelper.date_normalice(df)
                    for capitalizacion in df_capitalizacion.collect():
                        if capitalizacion["TABLA"].upper() == row["Table"].upper():
                            df = TransformationHelper.data_normalice(capitalizacion["CAMPO"],df)
                    if table.lower() in tabla_to_proccess:
                        df = TransformationHelper.homologacion_i(table, df_homologaciones_principal,df_homologaciones_principal, df, 'remediacion')
                    for rowcast in df_casteodata.collect():
                        if row['Table'].upper() == rowcast['TABLA'].upper():
                            df = TransformationHelper.data_cast(rowcast['CAMPO'].upper(),rowcast['NORMALIZACION'], df)
                    df = TransformationHelper.data_normalice_string(df, None)
                    if  "REINCAPACIDAD" in table:
                        df = TransformationHelper.validate_reincapacidad(df)
                    if "REMUERTE" in table:
                        df = TransformationHelper.validate_remuerte(df)
                    if "REENFERMEDADES" in table:
                        df = TransformationHelper.validate_reenfermedades(df)
                    if table.upper() in ('REINCAPACIDADTOTPERMAN','REMUERTEVDEUDOR','REENFERMEDADESGVDEUDOR','REAUXILIOFUNERARIOVDEUDOR'):
                        df = TransformationHelper.validate_claim_cause(df, table)    
                    path_target = f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_"
                    path = f"{path_target}{table}/"
                    ExtractHelper.save_files(df,path)
                       
            else:## es point
                path_source = f"s3://{BUCKET_NAME_BRONCE}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-{row['SCHEMA'].upper()}-"
                path = f"{path_source}{table}/"
                df = ExtractHelper.extract_parquet_data(path, row["Fields"], Date
                if table.lower() in ('cierresgenerales','cierrevida'):
                    df_ubi = df.select("numero_siniestro","lugar_ocurrencia").distinct()
                    df_ubi = orchestrator_silver(args, glueContext, spark, table, df_ubi)
                    path_ubi = f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_UBICACION_POINT"
                    ExtractHelper.save_files(df_ubi, path_ubi)
                if table.lower() in tabla_to_proccess:
                        df = TransformationHelper.homologacion_i(table, df_homologaciones_principal,df_homologaciones_principal, df, 'homologacion')
                for capitalizacion in df_capitalizacion.collect():
                    if capitalizacion["TABLA"].upper() == table:
                        df = TransformationHelper.data_normalice(capitalizacion["CAMPO"],df)
                if table.lower() in ('cierresgenerales','cierrevida'):
                    df = df.withColumnRenamed("vigencia_desde_poliza", "fecha_vigencia_desde_poliza")
                    df = TransformationHelper.date_normalice(df)
                    df = TransformationHelper.validate_claim_cause(df, table)
                if table.lower() in tabla_to_proccess:
                        df = TransformationHelper.homologacion_i(table, df_homologaciones_principal,df_homologaciones_principal, df, 'remediacion')
                capitalization_fields = []
                for i in df_capitalizacion.collect():
                    if i['TABLA'].upper() == tabla:
                        capitalization_fields.append(i['CAMPO'])
                for rowcast in df_casteodata.collect():
                    if table == rowcast['TABLA'].upper():
                        df = TransformationHelper.data_cast(rowcast['CAMPO'].upper(),rowcast['NORMALIZACION'], df)
                if table.lower() in ('cierresgenerales','cierrevida'):
                    df = df.withColumn(
                        "TIPO_TRANSACCION",
                        F.when(
                            (F.col("TIPO_TRANSACCION") == "Incremento / Decremento") & (F.col("valor_transaccion") > 0),
                            "Incremento reserva"
                        ).when(
                            (F.col("TIPO_TRANSACCION") == "Incremento / Decremento") & (F.col("valor_transaccion") >= 0),
                            "Disminucion reserva"
                        ).otherwise(F.col("TIPO_TRANSACCION"))
                    )
                    df = df.withColumnRenamed("fecha_vigencia_desde_poliza", "vigencia_desde_poliza")
                df = TransformationHelper.data_normalice_string(df, capitalization_fields)
                path_target = f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_"
                path = f"{path_target}{table}/"
                ExtractHelper.save_files(df,path)
    
        if row["Status"] == "2":
            if row["DB"] == "DANE":
                df = orchestrator_silver_dane(args, spark, glueContext, row["Table"])
                path_name = row["Table"].split("-")[1]
                path_target = f"s3://{BUCKET_NAME_SILVER}/ACSELE/TRANSFORMATION/ACSELE_SILVER_{path_name}/"
                path = f"{path_target}{table}/"
                ExtractHelper.save_files(df,path)
    
    job.commit()
except Exception as e:
    print(traceback.format_exc())
    raise e