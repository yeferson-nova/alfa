# Glue Python Shell 3.0+ (Python 3.10)
import sys
import io
from datetime import date
import pandas as pd
import boto3
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
    sys.argv, 
        [
        'BUCKET_LANDING',
		'BUCKET_NAME_BRONCE',
        'BUCKET_FILE'
        ]
    )

session = boto3.session.Session()
AWS_REGION = session.region_name

BUCKET_LANDING = args['BUCKET_LANDING']
BUCKET_NAME_BRONCE = args['BUCKET_NAME_BRONCE']  
BUCKET_FILE = args['BUCKET_FILE']

DST_PREFIX   = "BRONCE/CALIDAD"
LOAD_DT      = date.today().isoformat()


print(f"Bucket Landing : {BUCKET_LANDING}")
print(f"Bucket Bronze  : {BUCKET_NAME_BRONCE}")
print(f"Archivo        : {BUCKET_FILE}")

# Hojas que vamos a procesar (segÃºn el Excel real)
SHEETS = {
    "tbl_activo":                   {"mode": "by_load_dt"},
    "tbl_dominio":                  {"mode": "by_load_dt"},
    "tbl_tipo_activo":              {"mode": "by_load_dt"},
    "tbl_atributo":                 {"mode": "by_load_dt"},
    "tbl_tipo_dato":                {"mode": "by_load_dt"},
    "tbl_tipo_estado":              {"mode": "by_load_dt"},
    "tbl_historico_versiones":      {"mode": "by_load_dt"},
    "tbl_seguridad_datos":          {"mode": "by_load_dt"},
    "tbl_ubicacion_fisica":         {"mode": "by_load_dt"},
    "tbl_gobierno_datos":           {"mode": "by_load_dt"},
    "tbl_regla":                    {"mode": "by_load_dt"},
    "tbl_umbral":                   {"mode": "by_load_dt"},
    "tbl_capa":                     {"mode": "by_load_dt"},
    "tbl_regla_atributo":           {"mode": "by_load_dt"},
    "tbl_lista_referencia":         {"mode": "by_load_dt"}
    
}

session = boto3.Session(region_name=AWS_REGION)
s3 = session.client("s3")

#Funcion que recibe que recibe ruta origen y ruta/nombre del archivo con el fin de extraer la data desde el excel
#Esta funciÃ³n estÃ¡ diseÃ±ada para leer un archivo Excel almacenado en S3 sin descargarlo a disco, cargÃ¡ndolo completamente en memoria mediante BytesIO.
def read_excel_from_s3(bucket, key):
    try:
        obj = s3.get_object(Bucket=bucket,Key=key)
        content = obj["Body"].read()
        return io.BytesIO(content)
    except Exception as e:
        print(f"Error leyendo archivo s3://{bucket}/{key}")
        raise e

def write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str):
    import pyarrow as pa
    import pyarrow.parquet as pq
    table = pa.Table.from_pandas(df, preserve_index=False)
    out = io.BytesIO()
    pq.write_table(table, out, compression="snappy")
    s3.put_object(Bucket=bucket, Key=key, Body=out.getvalue())

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower() for c in df.columns]
    return df

def process_calendario(df: pd.DataFrame):
    df = normalize_columns(df)
    # 'aÃ±o' puede venir como 'ano'
    if "ano" in df.columns and "aÃ±o" not in df.columns:
        df.rename(columns={"ano": "aÃ±o"}, inplace=True)

    # fecha: dd/mm/yyyy Ã³ serial Excel
    if "fecha" in df.columns:
        fecha = pd.to_datetime(df["fecha"], errors="coerce", dayfirst=True)
        mask_serial = fecha.isna() & df["fecha"].apply(lambda x: isinstance(x, (int, float)))
        if mask_serial.any():
            fecha2 = pd.to_datetime(df.loc[mask_serial, "fecha"], unit="D", origin="1899-12-30", errors="coerce")
            fecha.loc[mask_serial] = fecha2
        df["fecha"] = fecha.dt.date

    required = {"id","fecha","aÃ±o","mes","dia"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas en Calendario: {missing}")

    # Particionar por aÃ±o/mes
    for (ano, mes), gdf in df.groupby(["aÃ±o","mes"], dropna=False):
        key = f"{DST_PREFIX}/calendario/aÃ±o={int(ano)}/mes={int(mes)}/part-{LOAD_DT}.parquet"
        write_parquet_to_s3(gdf, BUCKET_NAME_BRONCE, key)
        print("Escrito:", f"s3://{BUCKET_NAME_BRONCE}/{key}")

def process_by_load_dt(df: pd.DataFrame, table_name: str):
    if df.empty:
        print(f"La hoja {table_name} está vacía")
        return
    
    df = normalize_columns(df)
    
    delete_prefix(BUCKET_NAME_BRONCE,f"{DST_PREFIX}/{table_name}/")
    
    key = f"{DST_PREFIX}/{table_name}/load_dt={LOAD_DT}/part-0001.parquet"
    
    try:
        write_parquet_to_s3(df, BUCKET_NAME_BRONCE, key)
    except Exception as e:
        print(f"Error escribiendo {table_name}: {e}")
    
    
    print("Escrito:", f"s3://{BUCKET_NAME_BRONCE}/{key}")

def delete_prefix(bucket, prefix):
    try:
        paginator = s3.get_paginator('list_objects_v2')
        deleted = 0
        for page in paginator.paginate(Bucket=bucket,Prefix=prefix):
            if 'Contents' not in page:
                continue

            objects = [{'Key': obj['Key']} for obj in page['Contents']]
            deleted += len(objects)
            s3.delete_objects(Bucket=bucket,Delete={'Objects': objects})
        print(f"Eliminados {deleted} objetos de s3://{bucket}/{prefix}")

    except Exception as e:
        print(f"Error eliminando {prefix}: {e}")
        raise

def main():

    bio = read_excel_from_s3(BUCKET_LANDING, BUCKET_FILE) # bio contiene el archivo desde S3 cargado en memoria
    xls = pd.ExcelFile(bio, engine="openpyxl") # obj: pandas.io.excel._base.ExcelFile

    for sheet, cfg in SHEETS.items():
        if sheet not in xls.sheet_names:
            print(f"Saltando hoja no encontrada: {sheet}")
            continue

        df = pd.read_excel(xls, sheet_name=sheet, engine="openpyxl")

        if sheet == "Calendario":
            process_calendario(df)
        else:
            process_by_load_dt(df, sheet.lower())

if __name__ == "__main__":
    main()