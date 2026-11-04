import sys
import io
from datetime import date
import pandas as pd
import boto3
from awsglue.utils import getResolvedOptions
import re

args = getResolvedOptions(
    sys.argv, 
        [
        'BUCKET_LANDING',
		'BUCKET_NAME_GOLD',
        'BUCKET_FILE'
        ]
    )

AWS_REGION = boto3.Session().region_name

BUCKET_LANDING = args['BUCKET_LANDING']
BUCKET_NAME_GOLD = args['BUCKET_NAME_GOLD']
BUCKET_FILE = args['BUCKET_FILE']

DST_PREFIX   = "GOLD/CALIDAD"
LOAD_DT      = date.today().isoformat()


SHEETS = {

    "fct_perfilamiento":    {"mode": "by_load_dt"}
}

session = boto3.Session(region_name=AWS_REGION)
s3 = session.client("s3")

def read_excel_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read()
    return io.BytesIO(content)


def write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str, schema: "pa.Schema" = None):
    import pyarrow as pa
    import pyarrow.parquet as pq
    table = pa.Table.from_pandas(df, preserve_index=False, schema=schema)
    out = io.BytesIO()
    pq.write_table(table, out, compression="snappy")
    s3.put_object(Bucket=bucket, Key=key, Body=out.getvalue())



def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:

    df.columns = [c.strip().lower() for c in df.columns]
    return df
    

ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$")  

def normalize_maximo_as_string(df: pd.DataFrame) -> pd.DataFrame:
    if "maximo" not in df.columns:
        return df

    s = df["maximo"]


    def _fix_bytes(x):
        if isinstance(x, (bytes, bytearray)):
            try:
                return x.decode("utf-8")
            except Exception:
                return repr(x)
        return x

    s = s.apply(_fix_bytes)

    def _to_str(x):
        if isinstance(x, pd.Timestamp):
            if x.time() == pd.Timestamp(0).time():
                return x.strftime("%Y-%m-%d")
            return x.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(x, (pd.Timestamp, pd.datetime, )):
            return pd.to_datetime(x).strftime("%Y-%m-%d")
        if isinstance(x, str):
            xs = x.strip()
            if ISO_DATE_RE.match(xs):
                try:
                    ts = pd.to_datetime(xs, errors="raise")
                    if ts.time() == pd.Timestamp(0).time():
                        return ts.strftime("%Y-%m-%d")
                    return ts.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    return xs
            return xs
        return str(x)

    s = s.apply(_to_str).astype("string")
    df["maximo"] = s
    return df
    

def coerce_object_columns(df: pd.DataFrame, numeric_like_cols: set = None, text_like_cols: set = None) -> pd.DataFrame:
    """
    - Convierte bytes -> str (utf-8, con fallback repr)
    - Para columnas en numeric_like_cols: a numérico (coerce NaN)
    - Para columnas en text_like_cols (o resto 'object'): a string estándar
    """
    numeric_like_cols = numeric_like_cols or set()
    text_like_cols = text_like_cols or set()

    for col in df.columns:
        if col not in df.columns:
            continue

        s = df[col]
        if s.dtype == 'object':
            def _fix_bytes(x):
                if isinstance(x, (bytes, bytearray)):
                    try:
                        return x.decode('utf-8')
                    except Exception:
                        return repr(x)
                return x
            s = s.apply(_fix_bytes)

        if col in numeric_like_cols:
            s = pd.to_numeric(s, errors="coerce")

        elif s.dtype == 'object' or col in text_like_cols:
            s = s.astype("string")

        df[col] = s

    return df




def process_calendario(df: pd.DataFrame):
    df = normalize_columns(df)

    if "ano" in df.columns and "año" not in df.columns:
        df.rename(columns={"ano": "año"}, inplace=True)

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

    df["año"] = pd.to_numeric(df["año"], errors="coerce").astype("Int64")
    df["mes"] = pd.to_numeric(df["mes"], errors="coerce").astype("Int64")
    df["dia"] = pd.to_numeric(df["dia"], errors="coerce").astype("Int64")

    df = coerce_object_columns(df)

    for (ano, mes), gdf in df.groupby(["año","mes"], dropna=False):
        ano_val = int(ano) if pd.notna(ano) else -1
        mes_val = int(mes) if pd.notna(mes) else -1
        key = f"{DST_PREFIX}/calendario/año={ano_val}/mes={mes_val}/part-{LOAD_DT}.parquet"
        write_parquet_to_s3(gdf, BUCKET_NAME_GOLD, key)
        print("Escrito:", f"s3://{BUCKET_NAME_GOLD}/{key}")



def process_by_load_dt(df: pd.DataFrame, table_name: str):
    df = normalize_columns(df)
    df = normalize_maximo_as_string(df) 
    numeric_like = set()
    text_like = set()

    if "maximo" in df.columns:
        numeric_like.add("maximo")
    if "minimo" in df.columns:
        numeric_like.add("minimo")

    for c in ("descripcion", "nombre", "codigo"):
        if c in df.columns:
            text_like.add(c)

    df = coerce_object_columns(df, numeric_like_cols=numeric_like, text_like_cols=text_like)

    key = f"{DST_PREFIX}/{table_name}/load_dt={LOAD_DT}/part-0001.parquet"
    write_parquet_to_s3(df, BUCKET_NAME_GOLD, key)
    print("Escrito:", f"s3://{BUCKET_NAME_GOLD}/{key}")

def main():
    bio = read_excel_from_s3(BUCKET_LANDING, BUCKET_FILE)
    xls = pd.ExcelFile(bio, engine="openpyxl")

    for sheet, cfg in SHEETS.items():
        if sheet not in xls.sheet_names:
            print(f"Saltando hoja no encontrada: {sheet}")
            continue

        df = pd.read_excel(xls, sheet_name=sheet, engine="openpyxl")
        mode = cfg["mode"]

        if sheet == "Calendario":
            process_calendario(df)
        else:
            process_by_load_dt(df, sheet.lower())

if __name__ == "__main__":
    main()