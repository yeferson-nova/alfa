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

AWS_REGION = boto3.Session().region_name

BUCKET_LANDING = args['BUCKET_LANDING']
BUCKET_NAME_BRONCE = args['BUCKET_NAME_BRONCE']  
BUCKET_FILE = args['BUCKET_FILE']

DST_PREFIX   = "BRONCE/CALIDAD"
LOAD_DT      = date.today().isoformat()

# Hojas que vamos a procesar (según el Excel real)
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

def read_excel_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read()
    return io.BytesIO(content)

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
    # 'año' puede venir como 'ano'
    if "ano" in df.columns and "año" not in df.columns:
        df.rename(columns={"ano": "año"}, inplace=True)

    # fecha: dd/mm/yyyy ó serial Excel
    if "fecha" in df.columns:
        fecha = pd.to_datetime(df["fecha"], errors="coerce", dayfirst=True)
        mask_serial = fecha.isna() & df["fecha"].apply(lambda x: isinstance(x, (int, float)))
        if mask_serial.any():
            fecha2 = pd.to_datetime(df.loc[mask_serial, "fecha"], unit="D", origin="1899-12-30", errors="coerce")
            fecha.loc[mask_serial] = fecha2
        df["fecha"] = fecha.dt.date

    required = {"id","fecha","año","mes","dia"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas en Calendario: {missing}")

    # Particionar por año/mes
    for (ano, mes), gdf in df.groupby(["año","mes"], dropna=False):
        key = f"{DST_PREFIX}/calendario/año={int(ano)}/mes={int(mes)}/part-{LOAD_DT}.parquet"
        write_parquet_to_s3(gdf, BUCKET_NAME_BRONCE, key)
        print("Escrito:", f"s3://{BUCKET_NAME_BRONCE}/{key}")

def process_by_load_dt(df: pd.DataFrame, table_name: str):
    df = normalize_columns(df)
    key = f"{DST_PREFIX}/{table_name}/load_dt={LOAD_DT}/part-0001.parquet"
    write_parquet_to_s3(df, BUCKET_NAME_BRONCE, key)
    print("Escrito:", f"s3://{BUCKET_NAME_BRONCE}/{key}")

def main():
    bio = read_excel_from_s3(BUCKET_LANDING, BUCKET_FILE)
    xls = pd.ExcelFile(bio, engine="openpyxl")

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