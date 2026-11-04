"""Script that load bronce phase"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from commons.ExtractHelper import ExtractHelper
from commons.IncrementalCharge import IncrementalCharge
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import traceback
import boto3
from datetime import datetime

args = getResolvedOptions(
    sys.argv, 
        [
        'JOB_NAME',
        'DB_SCHEMA',
        'BUCKET_NAME',
        'BUCKET_NAME_ASSETS',
        'CONNECT_NAME',
        'FUNCTIONALITY',
        'PROJECT',
        'PROCCESS_FILE',
        'VAL_FILES',
        'OVERWRITE_TABLE'
        ]
    )
DB_SCHEMA = args['DB_SCHEMA']
BUCKET_NAME = args['BUCKET_NAME']
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
CONNECT_NAME = args['CONNECT_NAME']
FUNCTIONALITY = args['FUNCTIONALITY']
PROJECT = args['PROJECT']
PROCCESS_FILE = args['PROCCESS_FILE']
VAL_FILES = args['VAL_FILES']
OVERWRITE_TABLE = args['OVERWRITE_TABLE']
OVERWRITE_TABLE = OVERWRITE_TABLE.split(',')

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3_client = boto3.client("s3")
ExtractHelper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_NAME)
IncrementalCharge = IncrementalCharge(glueContext, spark, DB_SCHEMA, BUCKET_NAME)
path = f"s3://{BUCKET_NAME_ASSETS}/support_phase_files/{PROCCESS_FILE}.xlsx"
sheet = "bronce_tables"
df_tables = ExtractHelper.extract_excel_data(path, sheet)
sheet = "bronce_tmp_tables"
df_tmp_tables = ExtractHelper.extract_excel_data(path, sheet)
current_date = datetime.now()
day = current_date.day
month = current_date.month
year = current_date.year
bucket_date = f"{year}{month}{day}"
validation_files =  0 # dim 10
array_file_name = []
if PROJECT == 'ACSELE':
    schema_number = 2
    array_file_name = ['CIERREVIDA','CIERRESGENERALES']
if PROJECT == 'SIPREN':
    schema_number = 1
    array_file_name = ['BODEGA_PLUS']
    
def obtain_last_date(fecha = None):
    """
    This Function help us to obtein the last cut date.
    """
    if fecha is None:
        fecha = datetime.now()

    primer_dia_mes_actual = datetime(fecha.year, fecha.month, 1)

    ultimo_dia_mes_anterior = primer_dia_mes_actual - timedelta(days=1)

    return ultimo_dia_mes_anterior.year, ultimo_dia_mes_anterior.month

if FUNCTIONALITY == '1':
    try:
        bucket_name = BUCKET_NAME
        def get_last_date(bucket_name, path):
            prefix = f"{path}"
            reponse = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")
        
            if 'CommonPrefixes' in reponse:
                dates = []
                for prefixarr in reponse['CommonPrefixes']:
                    date = prefixarr['Prefix'].replace(prefix, '').strip('/')
                    dates.append(date)
                
                last_date = max(dates)
                return last_date
            else:
                return "Date not found"
        path = 'GEN/FILES/GEN-FILES-INCREMENTAL/'
        str_date = ''
        for i in range(3):
            date_str = ''
            Date = get_last_date(BUCKET_NAME, path)
            date_str = date_str + Date + '/'
            str_date = str_date + Date + '-'
            if date_str != 'Date not found':
                path = path + date_str
            
        path_incre = f"s3://{BUCKET_NAME}/GEN/FILES/GEN-FILES-"
        path = f"{path_incre}"
        df_incremental = IncrementalCharge.charge_tmp_tables(path, 'INCREMENTAL')
        validate_incre = True
        if df_incremental == None:
            validate_incre = False
        for row in df_tables.collect():
            tabla_p = row["Table"].upper()
            status = int(float(row["Status"]))
            if status == 1:
                print(f"Tabla a EJECUTAR : {tabla_p}")
                if validate_incre == True:
                    df_validate = df_incremental.filter(F.upper(F.col("tabla")) == row["Table"].upper())
                else:
                    filter_val = "0"
                if validate_incre == True:
                    if df_validate.count() > 0:
                        df_validate = df_validate.agg(F.max("max_id")).collect()
                        filter_val = df_validate[0][0]
                    else:
                        filter_val = "0"
                else:
                    filter_val = "0"
                if VAL_FILES == '1':
                    validation_files = 10
                if row["SCHEMA"] == "TIERRA":
                    validation_files = 1
                overwrite_val = 'No'
                if row["Table"].upper() in OVERWRITE_TABLE:
                    filter_val = 0
                    overwrite_val = 'Yes'    
                if row["SCHEMA"] != "FILES" and validation_files >= schema_number:
                    if row["Logic"]=="-":
                        ExtractHelper.execute_data_incremental(table_name=row["Table"],filter_field=row['Filter_field'],fields=row["Fields"],connectname=CONNECT_NAME,filter_value=filter_val,bd=row["DB"], overwrite = overwrite_val)
                    else:
                        ExtractHelper.execute_data_incremental(table_name=row["Table"],filter_field=row['Filter_field'],fields=row["Fields"],table_base=row["Base_table"],sql_script=row["Logic"],connectname=CONNECT_NAME,filter_value=filter_val, delete_pk_field = row["delete_pk"], bd=row["DB"], overwrite = overwrite_val)
                else:
                    table = row["Table"]
                    path = f"s3://{BUCKET_NAME_ASSETS}/FS_files/{bucket_date}/{table}/{table}.xlsx"#cambiar_por_landing
                    check_path = f"FS_files/{bucket_date}/{table}"
                    sheet = row["SHEET"]
                    print(f"la informacion de la siguiente tabla: {table}, {path}, {check_path}")
                    if ExtractHelper.check_folder(BUCKET_NAME_ASSETS, check_path):#cambiar_por_landing
                        df = ExtractHelper.extract_excel_data_point(path,sheet)
                        if table == "BODEGA_PLUS":
                            df = df.withColumnRenamed("DECODE(P.COD_MOVIMIENTO,4,'PAG","tipo_movimiento")
                            df = df.withColumnRenamed("DECODE(D.COD_CONCEPTO_PAGO_SIN","concepto_pago")
                            df = df.drop('FECHA SINIESTRO FINAL - COBERTURA IT')
                        path_target = f"s3://{BUCKET_NAME}/{row['DB'].upper()}/FILES/{row['DB'].upper()}-FILES-"
                        path = f"{path_target}{table}/"
                        if row["field_validation_file"] != "-":
                            year, month = obtain_last_date()
                            if df.count() > 0 and table.upper() in array_file_name:
                                validation_files = validation_files + 1
                                print("aumente")
                        if "vida" in table.lower():
                            df = df.filter((~F.col("VIGENCIA_DESDE_POLIZA").isNull()) | (F.length(F.col("VIGENCIA_DESDE_POLIZA")) > 0))
                            df = df.filter((F.length(F.col("tipo_transaccion")) < 3))
                        print(path)
                        ExtractHelper.save_files(df,path)
                    else:
                        print(f"No se tienen archivos para esta tabla {table}")
    except Exception as e:
        print(traceback.format_exc())
else:
    try:
        for row in df_tmp_tables.collect():
            table = row["TMP_TABLE"].upper()
            path_source =f"s3://{BUCKET_NAME}/ACSELE/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"
            print(path_source)
            IncrementalCharge.charge_tmp_tables(path_source, table, 'Gold')
        array_tables = IncrementalCharge.return_tables()
        data = []
        for key, value in array_tables.items():
            path = f"s3://{BUCKET_NAME}/ACSELE/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"
            try:
                query = IncrementalCharge.get_query(key)
                if query:
                    df = spark.sql(query)
                else:
                    df = IncrementalCharge.charge_tmp_tables(path, key)
                df = df.agg(F.max(value)).collect()
                filter_val = df[0][0]
                dato = (key, filter_val, '2024-12-01')
                data.append(dato)
            except Exception as e:
                print(f"Esta tabla {key}, no tiene datos: {e}")
        df_save = spark.createDataFrame(data, ["tabla", "max_id", "fecha_ejecucion"])
        df_save = df_save.withColumn(
            "max_id",
            F.col("max_id").cast("bigint")
        )
        ExtractHelper.save_files(df_save,f"s3://{BUCKET_NAME}/GEN/FILES/GEN-FILES-INCREMENTAL/")
    except Exception as e:
        print(traceback.format_exc())
        exit.status(1)
        raise e
