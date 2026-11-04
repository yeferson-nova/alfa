import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Table, TableStyle, SimpleDocTemplate, Paragraph, Spacer
from datetime import datetime
 
# Conexión con S3
s3_client = boto3.client('s3')
 
# Listar carpetas dentro de "PRV/"
def listar_carpetas(bucket_name, prefix):
    carpetas = set()
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    if 'CommonPrefixes' in response:
        carpetas = {obj['Prefix'] for obj in response['CommonPrefixes']}
    return sorted(carpetas)
 
# Listar archivos Parquet en una carpeta
def listar_archivos_parquet(bucket_name, prefix):
    archivos = []
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' in response:
        archivos = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
    return archivos
 
# Leer archivo Parquet desde S3
def leer_parquet_s3(bucket_name, file_key):
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    file_data = obj['Body'].read()
    parquet_file = BytesIO(file_data)
    table = pq.read_table(parquet_file)
    return table
 
# Fusionar múltiples archivos Parquet
def fusionar_archivos_parquet(bucket_name, archivos):
    tablas = []
    for archivo in archivos:
        print(f"Leyendo archivo: {archivo}")
        tabla = leer_parquet_s3(bucket_name, archivo)
        tablas.append(tabla)
    tabla_combinada = pa.concat_tables(tablas) if tablas else None
    return tabla_combinada
 
# Obtener el análisis del DataFrame combinado
def obtener_analisis(tabla):
    if tabla is None:
        return None
    df = tabla.to_pandas()
    total_filas = len(df)
    valores_nulos = df.isnull().sum()
    tipos_de_datos = {col: str(tabla.schema.field(col).type) for col in tabla.column_names}
    return {
        "Número de filas": total_filas,
        "Número de columnas": len(df.columns),
        "Columnas": list(df.columns),
        "Tipos de datos": tipos_de_datos,
        "Valores nulos por columna": valores_nulos.to_dict(),
        "Porcentaje de valores nulos": ((valores_nulos / total_filas) * 100).round(2).fillna(0).to_dict()
    }
 
# Generar el PDF con el análisis
def generar_pdf(analisis, output_filename, nombre_tabla):
    doc = SimpleDocTemplate(output_filename, pagesize=letter)
    elements = []
    styles = getSampleStyleSheet()
 
    title = Paragraph(f"<b>Análisis de Datos - {nombre_tabla}</b>", styles['Title'])
    elements.append(title)
    elements.append(Spacer(1, 12))
    elements.append(Paragraph(f"<b>Número de filas:</b> {analisis['Número de filas']}", styles['Normal']))
    elements.append(Paragraph(f"<b>Número de columnas:</b> {analisis['Número de columnas']}", styles['Normal']))
    elements.append(Spacer(1, 12))
 
    datos_tabla = [["Columna", "Tipo de dato", "Valores nulos", "% de valores nulos"]]
    for columna in analisis["Columnas"]:
        tipo_dato = analisis["Tipos de datos"].get(columna, "Desconocido")
        valores_nulos = analisis["Valores nulos por columna"].get(columna, 0)
        porcentaje_nulos = analisis["Porcentaje de valores nulos"].get(columna, 0)
        datos_tabla.append([columna, tipo_dato, valores_nulos, f"{porcentaje_nulos}%"])
 
    table = Table(datos_tabla, colWidths=[180, 100, 100, 100])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))
    elements.append(table)
    elements.append(Spacer(1, 12))
 
    doc.build(elements)
 
# Subir el PDF a S3
def subir_pdf_a_s3(bucket_name, file_key, local_path):
    s3_client.upload_file(local_path, bucket_name, file_key)
    print(f"PDF subido a s3://{bucket_name}/{file_key}")
 
# Obtener el nombre de la tabla desde la carpeta
def obtener_nombre_tabla(carpeta):
    return carpeta.strip('/').split('/')[-1]  # Última parte de la ruta
 
# Función principal
def main():
    bucket_name = 'alfa-datalake-dev-bronce'
    general_prefix = 'ALFAPRD/PRV/'  # Ruta base donde están las carpetas
 
    # Obtener todas las carpetas dentro de "PRV/"
    carpetas = listar_carpetas(bucket_name, general_prefix)
    if not carpetas:
        print("No se encontraron carpetas dentro de PRV.")
        return
 
    for carpeta in carpetas:
        nombre_tabla = obtener_nombre_tabla(carpeta)
        print(f"\nProcesando carpeta: {nombre_tabla}")
 
        # Listar archivos Parquet en la carpeta actual
        archivos_parquet = listar_archivos_parquet(bucket_name, carpeta)
        if not archivos_parquet:
            print(f"No se encontraron archivos Parquet en {carpeta}.")
            continue
 
        # Fusionar archivos Parquet
        tabla_combinada = fusionar_archivos_parquet(bucket_name, archivos_parquet)
 
        # Obtener análisis de la tabla combinada
        analisis = obtener_analisis(tabla_combinada)
        if analisis is None:
            print(f"No se pudo generar el análisis para {nombre_tabla}.")
            continue
 
        # Generar nombre del PDF
        fecha_actual = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'/tmp/analisis_{nombre_tabla}_{fecha_actual}.pdf'
 
        # Generar el PDF
        generar_pdf(analisis, output_filename, nombre_tabla)
 
        # Definir el destino del PDF en S3
        file_key_pdf = f'PDF_FILES/PRV_QA/{nombre_tabla}_{fecha_actual}.pdf'
 
        # Subir el PDF a S3
        subir_pdf_a_s3(bucket_name, file_key_pdf, output_filename)
 
        print(f"El archivo PDF se ha subido a: s3://{bucket_name}/{file_key_pdf}")
 
if __name__ == "__main__":
    main()