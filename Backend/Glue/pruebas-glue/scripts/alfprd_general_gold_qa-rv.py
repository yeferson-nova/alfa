import boto3
import pandas as pd
from datetime import datetime
import io
 
# Inicializar cliente S3
s3_client = boto3.client('s3')
 
# Listar todos los archivos .parquet recursivamente
def listar_parquets(bucket_name, prefix):
    parquet_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.parquet'):
                    parquet_files.append(obj['Key'])
    return parquet_files
 
# Leer y unir todos los parquet en un solo DataFrame
def leer_y_unir_parquets(bucket_name, files):
    dfs = []
    for key in files:
        print(f"Leyendo: {key}")
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        buffer = io.BytesIO(obj['Body'].read())
        try:
            df = pd.read_parquet(buffer)
            dfs.append(df)
        except Exception as e:
            print(f"Error leyendo {key}: {e}")
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()
 
# Guardar el análisis como archivo de texto plano con extensión .pdf
def guardar_analisis_como_pdf(df, output_path, nombre_tabla):
    with open(output_path, "w", encoding='utf-8') as f:
        f.write(f"Análisis de la tabla: {nombre_tabla}\n")
        f.write(f"Fecha: {datetime.today().strftime('%Y-%m-%d')}\n\n")
        f.write(f"Total de filas: {df.shape[0]}\n")
        f.write(f"Total de columnas: {df.shape[1]}\n\n")
        f.write("Cantidad de valores nulos por columna:\n")
        f.write(df.isnull().sum().to_string())
        f.write("\n\nResumen estadístico:\n")
        f.write(df.describe(include='all').transpose().to_string())
 
# Subir archivo a S3
def subir_pdf(bucket_name, s3_key, local_path):
    with open(local_path, 'rb') as f:
        s3_client.upload_fileobj(f, bucket_name, s3_key)
 
# Obtener nombre de tabla desde el prefix
def extraer_nombre_tabla(prefix):
    partes = prefix.strip('/').split('/')
    return partes[-1] if partes else 'tabla'
 
# Función principal
def main():
    bucket = 'alfa-datalake-dev-gold'
    general_prefix = 'GOLD/NIIF/RESERVA_MATEMATICA_GOLD/'
 
    parquet_files = listar_parquets(bucket, general_prefix)
    if not parquet_files:
        print("No se encontraron archivos Parquet.")
        return
 
    df_unido = leer_y_unir_parquets(bucket, parquet_files)
    if df_unido.empty:
        print("No se pudo unir ningún archivo.")
        return
 
    nombre_tabla = extraer_nombre_tabla(general_prefix)
    fecha = datetime.today().strftime('%Y-%m-%d')
    output_path = f'/tmp/analisis_{nombre_tabla}_{fecha}.pdf'
    guardar_analisis_como_pdf(df_unido, output_path, nombre_tabla)
 
    s3_destino = f'PDF_FILES/GOLD/{nombre_tabla}_{fecha}.pdf'
    subir_pdf(bucket, s3_destino, output_path)
    print(f"PDF subido correctamente a s3://{bucket}/{s3_destino}")
 
if __name__ == '__main__':
    main()