import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from commons.ExtractHelper import ExtractHelper
from commons.TransformationHelper import TransformationHelper
from commons.AseguradoHelper import AseguradoHelper
from pyspark.sql import functions as F
from pyspark.sql.functions import split, to_date, date_format, col, regexp_replace
from compt3.orchestrator.orchestrator_silver import orchestrator_silver
from compt3.orchestrator.orchestrator_silver_dane import orchestrator_silver_dane
from commons.IncrementalCharge import IncrementalCharge
import traceback
import boto3

import boto3
import json
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','DB_SCHEMA_BRONCE','DB_SCHEMA_SILVER','BUCKET_NAME_BRONCE','BUCKET_NAME_SILVER','DB_SOURCE','BUCKET_NAME_ASSETS','DATE','REPROCES','REPROCES_DATE','OVERWRITE_TABLE'])

DB_SCHEMA_BRONCE = args['DB_SCHEMA_BRONCE']
DB_SCHEMA_SILVER = args['DB_SCHEMA_SILVER']
BUCKET_NAME_BRONCE = args['BUCKET_NAME_BRONCE']
BUCKET_NAME_SILVER = args['BUCKET_NAME_SILVER']
BUCKET_NAME_ASSETS = args['BUCKET_NAME_ASSETS']
DB_SOURCE = args['DB_SOURCE']
REPROCES = args['REPROCES']
REPROCES_DATE = args['REPROCES_DATE']
OVERWRITE_TABLE = args['OVERWRITE_TABLE']
OVERWRITE_TABLE = OVERWRITE_TABLE.split(",")
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

ExtractHelper = ExtractHelper(glueContext, spark, "-", "-")
TransformationHelper = TransformationHelper()
AseguradoHelper = AseguradoHelper(glueContext, spark)
IncrementalCharge = IncrementalCharge(glueContext, spark, DB_SCHEMA_BRONCE, BUCKET_NAME_BRONCE)

def delete_s3_data(bucket_name, prefix):
    """
    This function help us on delete old data in overwrite tables(tables that need create since 0)
    """
    print(f"delete data {prefix}")
    try:
        s3 = boto3.resource("s3")
        bucket_s3 = s3.Bucket(bucket_name)
        bucket_s3.objects.filter(Prefix=prefix).delete()
        for o in bucket_s3.objects.filter(Prefix=prefix):
            print(f"Objeto encontrado {o.key}")
    except Exception as e:
        print(e)
    
try:
    ##OPEN PARAM in JSON##
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket = BUCKET_NAME_ASSETS, Key = 'support_phase_files/silver-procces.json')
    data = json.loads(obj['Body'].read().decode('utf-8'))
    ###
    for procces, info in data.items():
        if procces != '-': ## change here according to the process use. ejm: procces == ola0, remember change the condition
            path = f"s3://{BUCKET_NAME_ASSETS}/support_phase_files/{info['file_name']}"
            sheet = "silver_tables"
            df_tables = ExtractHelper.extract_excel_data(path, sheet)
            sheet = "tmp_silver"
            df_tables_tmp_silver = ExtractHelper.extract_excel_data(path, sheet)
            path = f"s3://{BUCKET_NAME_ASSETS}/support_phase_files/{info['hom_file_name']}"
            sheet = info['sheet']
            df_homologaciones_dos = ExtractHelper.extract_excel_data(path, sheet)
            path = f"s3://{BUCKET_NAME_ASSETS}/support_phase_files/{info['nom_file_name']}"
            sheet = "Capitalizacion_datos"
            df_capitalizacion = ExtractHelper.extract_excel_data(path, sheet)
            sheet = "Casteo_datos"
            df_casteodata = ExtractHelper.extract_excel_data(path, sheet)
            
            tabla_to_proccess = df_homologaciones_dos.select("tabla").distinct().rdd.flatMap(lambda x:x).collect()
            
            df_homologaciones_principal = df_homologaciones_dos.selectExpr(
            "tabla as tabla",
            "campo_referencia as campo_referencia",
            "valor_referencia",
            "valor_homologacion",
            "campo_homologacion"
            )
            validate_comp5 = 1
            if REPROCES == '1':
                Date = REPROCES_DATE
            else:
                Date = None
                        
            #df_tables_tmp_silver = None
            if df_tables_tmp_silver is not None:
                for row in df_tables_tmp_silver.collect():
                    table = row["Table"].upper()
                    if table in OVERWRITE_TABLE:
                        Date = '2100-01-01'
                    else: 
                        if REPROCES == '1':
                            Date = REPROCES_DATE
                        else:
                            Date = None
                    charge_type = row["Type"].upper()
                    if charge_type == 'No Incremental':
                        path = f"s3://{BUCKET_NAME_BRONCE}/ACSELE/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"
                        df = ExtractHelper.charge_tmp_tables(path,table,Date)
                        if df is None:
                            validate_comp5 = 1
                    else:
                        path = f"s3://{BUCKET_NAME_BRONCE}/ACSELE/ACSELE_ALFA/ACSELE-ACSELE_ALFA-"
                        IncrementalCharge.charge_tmp_tables(path, table)
                    
                        
                path = f"s3://{BUCKET_NAME_SILVER}/ACSELE/TRANSFORMATION/ACSELE_SILVER_"
                df_incremental = IncrementalCharge.charge_tmp_tables(path, "ASEGURADO")
        
        
            for row in df_tables.collect():
                table = row["Table"].upper()
                tabla = row["Table"].upper()
                
                if table in OVERWRITE_TABLE:
                    Date = '2100-01-01'
                else: 
                    if REPROCES == '1':
                        Date = REPROCES_DATE
                    else:
                        Date = None
                        
                status = int(float(row["Status"]))
                if status == 1:
                    if row["DB"] == "ACSELE":
                        if row["Logic"] == "-":
                            path_source = f"s3://{BUCKET_NAME_BRONCE}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-{row['SCHEMA'].upper()}-"
                            path = f"{path_source}{table}/"
                            df = ExtractHelper.extract_parquet_data(path, row["Fields"], Date)
                            if df != None:
                                if table.upper() in ('REINCAPACIDADTOTPERMAN','REMUERTEVDEUDOR','REENFERMEDADESGVDEUDOR','REINCTERRDEUDORES','REAUXILIOFUNERARIOVDEUDOR','REVIDAAP'):
                                    df_ubi = df.select("pk","DEPARTAMENTOINPUT","DEPARTAMENTOVALUE","CIUDADINPUT","CIUDADVALUE").distinct()
                                    df_ubi = orchestrator_silver(args, glueContext, spark, table, df_ubi)
                                    path_ubi = f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_UBICACION_ACSELE/"
                                    if table in OVERWRITE_TABLE:
                                        prefix = f"{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_UBICACION_ACSELE/2100/01/01/"
                                        delete_s3_data(BUCKET_NAME_SILVER, prefix)
                                        ExtractHelper.save_files(df_ubi, path_ubi,'Yes')
                                    else:
                                        ExtractHelper.save_files(df_ubi, path_ubi)
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
                                if table in OVERWRITE_TABLE:
                                    prefix = f"{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_{table}/2100/01/01/"
                                    delete_s3_data(BUCKET_NAME_SILVER, prefix)
                                    ExtractHelper.save_files(df,path,ov='Yes')
                                else:
                                    ExtractHelper.save_files(df,path)
                                
                            else:
                                print(f"Esta tabla no tiene datos para procesar {table}")
                        else:
                            path_source = f"s3://{BUCKET_NAME_BRONCE}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-{row['SCHEMA'].upper()}-"
                            path = f"{path_source}{table}/"
                            if table.upper() in OVERWRITE_TABLE:
                                Date = '2100-01-01'
                            else: 
                                if REPROCES == '1':
                                    Date = REPROCES_DATE
                                else:
                                    Date = None
                            df = ExtractHelper.extract_parquet_data(path, row["Fields"], Date)
                            if df != None:
                                if table.upper() in ('REINCAPACIDADTOTPERMAN','REMUERTEVDEUDOR','REENFERMEDADESGVDEUDOR','REINCTERRDEUDORES','REAUXILIOFUNERARIOVDEUDOR','REVIDAAP'):
                                    df_ubi = df.select("pk","DEPARTAMENTOINPUT","DEPARTAMENTOVALUE","CIUDADINPUT","CIUDADVALUE").distinct()
                                    df_ubi = orchestrator_silver(args, glueContext, spark, table, df_ubi)
                                    path_ubi = f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_UBICACION_ACSELE/"
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
                                if table.upper() in ('REMUERTEVDEUDOR','REINCAPACIDADTOTPERMAN','REENFERMEDADESGVDEUDOR','REAUXILIOFUNERARIOVDEUDOR'):
                                    df = TransformationHelper.validate_claim_cause(df, table)       
                                path_target = f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_"
                                path = f"{path_target}{table}/"
                                if table in OVERWRITE_TABLE:
                                    prefix = f"{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_{table}/2100/01/01/"
                                    delete_s3_data(BUCKET_NAME_SILVER, prefix)
                                    ExtractHelper.save_files(df,path,ov='Yes')
                                else:
                                    ExtractHelper.save_files(df,path)
                                
                            else:
                                print(f"Esta tabla no tiene datos para procesar {table}")
                    if row["DB"] == "POINT":
                        path_source = f"s3://{BUCKET_NAME_BRONCE}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-{row['SCHEMA'].upper()}-"
                        path = f"{path_source}{table}/"
                        df = ExtractHelper.extract_parquet_data(path, row["Fields"], Date)
                        
                        if df != None:
                            if table.lower() in ('cierresgenerales','cierrevida'):
                                df = df.filter(~F.col('ramo').contains('51'))
                                df = df.filter(~F.col('ramo').contains('50'))
                                df = df.filter(~F.col('ramo').contains('31'))
                                df = df.withColumnRenamed("vlrtrnalf","valor_transaccion")
                                df = TransformationHelper.validate_format_point(df)
                            if table.lower() in ('cierresgenerales','cierrevida'):
                                df_ubi = df.select("numero_siniestro","lugar_ocurrencia").distinct()
                                df_ubi = orchestrator_silver(args, glueContext, spark, table, df_ubi)
                                path_ubi = f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_UBICACION_POINT/"
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
                                        (F.col("TIPO_TRANSACCION") == "Incremento / Decremento") & (F.col("valor_transaccion") <= 0),
                                        "Disminucion reserva"
                                    ).otherwise(F.col("TIPO_TRANSACCION"))
                                )
                                df = df.withColumnRenamed("fecha_vigencia_hasta_poliza", "vigencia_hasta_poliza")
                            df = TransformationHelper.data_normalice_string(df, capitalization_fields)
                            if table.lower() in ('cierresgenerales','cierrevida'):
                                df = AseguradoHelper.create_asegurado_point(df)
                                df = TransformationHelper.date_normalice(df)
                                
                            path_target = f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_"
                            path = f"{path_target}{table}/"
                            if table in OVERWRITE_TABLE:
                                prefix = f"{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_{table}/2100/01/01/"
                                delete_s3_data(BUCKET_NAME_SILVER, prefix)
                                ExtractHelper.save_files(df,path,ov='Yes')
                            else:
                                ExtractHelper.save_files(df,path)
                        else:
                            print(f"Esta tabla no tiene datos para procesar {table}")
                    if row["DB"] == "GENERAL":
                        path_source = f"s3://{BUCKET_NAME_BRONCE}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-{row['SCHEMA'].upper()}-"
                        path = f"{path_source}{table}/"
                        df = ExtractHelper.extract_parquet_data(path, row["Fields"], Date)
                        if df != None:
                            path_target = f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_"
                            path = f"{path_target}{table}/"
                            if table in OVERWRITE_TABLE:
                                prefix = f"{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_{table}/2100/01/01/"
                                delete_s3_data(BUCKET_NAME_SILVER, prefix)
                                ExtractHelper.save_files(df,path,ov='Yes')
                            else:
                                ExtractHelper.save_files(df,path)
                        else:
                                print(f"Esta tabla en GENERAL no tiene datos para procesar {table}")
                    if row["DB"] == "SIPREN":
                        
                        if row["Logic"] == '-':
                            if row["SCHEMA"] == "FILES":
                                path_source = f"s3://{BUCKET_NAME_BRONCE}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-{row['SCHEMA'].upper()}-"
                            else:
                                path_source = f"s3://{BUCKET_NAME_BRONCE}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-{row['DB'].upper()}_ALFA-"
                            print(f"Entre a {table} conla fecha {Date}")
                            path = f"{path_source}{table}/"
                            df = ExtractHelper.extract_parquet_data(path, row["Fields"], Date)
                            
                            if df != None:
                                df_homologaciones_principal = TransformationHelper.trim_data(df_homologaciones_principal)
                                df = TransformationHelper.trim_data(df)
                                for capitalizacion in df_capitalizacion.collect():
                                    if capitalizacion["TABLA"].lower() == table.lower() and table.lower() == "rsv_comentario_reserva":
                                        df = TransformationHelper.data_normalice(capitalizacion["CAMPO"],df)
                                        df = df.withColumn( 
                                            capitalizacion["CAMPO"],
                                            F.concat(
                                                F.upper(F.substring(F.col(capitalizacion["CAMPO"]), 1, 1)),
                                                F.lower(F.substring(F.col(capitalizacion["CAMPO"]), 2, 1000))
                                            )
                                        )
                                        df = df.withColumn(
                                            "comentario_reserva",
                                            regexp_replace(F.col("comentario_reserva"), r'[^a-zA-Z0-9\s]','')
                                            )
                                        
                                        df = df.withColumn(
                                            "comentario_reserva",
                                            regexp_replace(F.col("comentario_reserva"), r'\b[a-zA-Z]\b','')
                                            )
                                        
                                        df = df.withColumn(
                                            "comentario_reserva",
                                            F.trim(F.col("comentario_reserva"))
                                            )
                                if tabla.upper() == 'SNR_CAUSA':
                                    df = df.filter(F.col('CLASE')=="CAUSA_SINIESTRO")
                                    df = df.drop("CLASE")                    
                                if table.lower() in tabla_to_proccess:
                                    df = TransformationHelper.homologacion_i(table, df_homologaciones_principal,df_homologaciones_principal, df, 'homologacion')
                                    df = TransformationHelper.homologacion_i(table, df_homologaciones_principal,df_homologaciones_principal, df, 'remediacion')
                                if table.upper() == 'BODEGA_PLUS':
                                    df = df.withColumn(
                                        "tipo_movimiento",
                                        F.when(
                                                (F.upper(F.col("tipo_movimiento")) == "RECOBRO") & (F.upper(F.col("concepto_pago")) == "AUXILIO FUNERARIO"),
                                                "Recobro"
                                            ).when(
                                                (F.upper(F.col("tipo_movimiento")) == "PAGO") & (F.upper(F.col("concepto_pago")) == "AUXILIO FUNERARIO"),
                                                "Perdida"
                                            ).otherwise(F.col("tipo_movimiento"))
                                        )
                                        
                                    columns = df.columns
                                    
                                    for col in columns:
                                        if "unnamed" in col:
                                            df = df.drop(col)
                                        if "#" in col:
                                            name = col.split("#")
                                            df = df.withColumnRenamed(col, name[0])
                                    columnas_clean = ["nid_siniestro", "periodo_point", "cod_movimiento", "poliza"]
                                    
                                    for columna in columnas_clean:
                                        df = df.withColumn(columna, 
                                           F.regexp_replace(F.col(columna), r"\.0+$", "")
                                        )
                                        
                                    for columna in df.columns:
                                        if "partition" in columna or "o_ocurrencia" in columna:
                                            df=df.drop(columna)
                                        
                                for rowcast in df_casteodata.collect():
                                    if row['Table'].upper() == rowcast['TABLA'].upper():
                                        df = TransformationHelper.data_cast(rowcast['CAMPO'].upper(),rowcast['NORMALIZACION'], df)
            
                                if table.lower() in ('spr_siniestros','spr_reserva','spr_pagos','spr_recuperaciones','ppr_polizas','spr_incapacidades_sol_audit','spr_siniestros_origenes_hist','vw_dsdafil_alfa'):
                                    df = TransformationHelper.date_normalice(df)
                                tables_dist = ['spr_conceptos_pagos_siniestros','cgr_movimientos_reserva','spr_causas_siniestros','cgr_origenes','spr_tipos_recuperaciones','vw_dsdafil_alfa','ben_solicitudes_v','SNR_ORIGEN','SNR_CAUSA','RSV_COBERTURA','RSV_TIPO_MOVIMIENTO','RSV_COBERTURA']
                                
                                tables_dist = [table.lower() for table in tables_dist]
                                
                                if table.lower() in tables_dist:
                                    df = df.distinct()
                                    
                                    
                                for capitalizacion in df_capitalizacion.collect():
                                    if capitalizacion["TABLA"].upper() == table:
                                        df = TransformationHelper.data_normalice(capitalizacion["CAMPO"],df)
                                        df = df.withColumn( 
                                            capitalizacion["CAMPO"],
                                            F.concat(
                                                F.upper(F.substring(F.col(capitalizacion["CAMPO"]), 1, 1)),
                                                F.lower(F.substring(F.col(capitalizacion["CAMPO"]), 2, 1000))
                                            )
                                        )
                                
                                df = TransformationHelper.date_normalice(df)
                                df = TransformationHelper.data_normalice_string(df, None)
                                path_target = f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_"
                                path = f"{path_target}{table}/"
                                if table in OVERWRITE_TABLE:
                                    prefix = f"{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_{table}/2100/01/01/"
                                    delete_s3_data(BUCKET_NAME_SILVER, prefix)
                                    ExtractHelper.save_files(df,path,ov='Yes')
                                    print("entre sipren 151")
                                else:
                                    ExtractHelper.save_files(df,path)
                            else:
                                print(f"no hay datos en {table}")    
                        else:
                            table_array = row["Tmp_tables"].split(",")
                            validation_int = len(table_array)
                            sum_int = 0
                            for tmp_table in table_array:
                                tmp_table = tmp_table.strip()
                                if tmp_table.upper() in OVERWRITE_TABLE:
                                    Date = '2100-01-01'
                                else: 
                                    if REPROCES == '1':
                                        Date = REPROCES_DATE
                                    else:
                                        Date = None
                                path_source = f"s3://{BUCKET_NAME_BRONCE}/{row['DB'].upper()}/{row['SCHEMA'].upper()}/{row['DB'].upper()}-{row['DB'].upper()}_ALFA-"
                                path = f"{path_source}{tmp_table}/"
                                df = ExtractHelper.extract_parquet_data(path, row["Fields"], Date)
                                try:
                                    df.createOrReplaceTempView(tmp_table)
                                    sum_int = sum_int + 1
                                except Exception as e:
                                    print(e)
                            
                            if validation_int == sum_int:
                                df = spark.sql(row["Logic"])                     
                                path_target = f"s3://{BUCKET_NAME_SILVER}/{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_"
                                path = f"{path_target}{table}/"
                                if table in OVERWRITE_TABLE:
                                    prefix = f"{row['DB'].upper()}/{DB_SCHEMA_SILVER}/{row['DB'].upper()}_SILVER_{table}/2100/01/01/"
                                    delete_s3_data(BUCKET_NAME_SILVER, prefix)
                                    ExtractHelper.save_files(df,path,ov='Yes')
                                else:
                                    ExtractHelper.save_files(df,path)
                                
                if row["Status"] == "2" and row["DB"] == "DANE":
                        df = orchestrator_silver_dane(args, spark, glueContext, row["Table"])
                        path_name = row["Table"].split("-")[1]
                        path_target = f"s3://{BUCKET_NAME_SILVER}/ACSELE/TRANSFORMATION/ACSELE_SILVER_{path_name}/"
                        path = f"{path_target}{table}/"
                        ExtractHelper.save_files(df,path)
        
            if validate_comp5 == 0:
                df_asegurado = AseguradoHelper.create_asegurado()
                df_asegurado = TransformationHelper.date_normalice(df_asegurado)
                df_asegurado_before = None
                try:
                    df_asegurado_before = spark.sql(f"SELECT * FROM global_temp.ASEGURADO")
                except Exception as e:
                    print(e)
                if df_asegurado_before is not None:
                    df_asegurado = df_asegurado.join(df_asegurado_before, on="cedula", how='left_anti')
                df_asegurado = TransformationHelper.date_normalice(df_asegurado)
                path = f"s3://{BUCKET_NAME_SILVER}/ACSELE/TRANSFORMATION/ACSELE_SILVER_ASEGURADO/"
                ExtractHelper.save_files(df_asegurado,path)  
            job.commit()
except Exception as e:
    print(traceback.format_exc())
    exit.status(1)
    raise e
