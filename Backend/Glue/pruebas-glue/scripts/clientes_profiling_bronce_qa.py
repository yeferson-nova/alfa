import sys
import time
import boto3
import pandas as pd
import numpy as np
from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.platypus import Table, TableStyle, SimpleDocTemplate, Paragraph
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from awsglue.utils import getResolvedOptions
 
# 📥 Parámetros
args = getResolvedOptions(sys.argv, [
    'ATHENA_DATABASE', 'ATHENA_OUTPUT_S3', 'S3_REPORT_FOLDER', 'TABLES'
])
 
athena_db = args['ATHENA_DATABASE']
athena_output = args['ATHENA_OUTPUT_S3']         # ej: s3://bucket/athena/results/
s3_report_folder = args['S3_REPORT_FOLDER']       # ej: s3://bucket/reportes/
table_list = [t.strip() for t in args['TABLES'].split(',')]  # Lista de tablas
 
athena = boto3.client('athena')
s3 = boto3.client('s3')
 
# 📌 Función auxiliar para ejecutar consulta y obtener DataFrame
def query_athena_table(database, table, output_location):
    query = f"SELECT * FROM {database}.{table}"
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location}
    )
    query_execution_id = response['QueryExecutionId']
 
    # Esperar que termine
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)
 
    if state != 'SUCCEEDED':
        raise Exception(f"La consulta de {table} falló: {state}")
 
    # Descargar el CSV
    bucket = output_location.replace("s3://", "").split("/")[0]
    prefix = "/".join(output_location.replace("s3://", "").split("/")[1:])
    result_key = f"{prefix}{query_execution_id}.csv"
 
    obj = s3.get_object(Bucket=bucket, Key=result_key)
    return pd.read_csv(obj['Body'])
 
# 📌 Función para análisis de calidad y generación de PDF
def generar_reporte_pdf(pdf, tabla, output_path_s3):
    total_campos = len(pdf.columns)
    total_registros = len(pdf)

    campos_numericos = pdf.select_dtypes(include=[np.number]).columns.tolist()
    campos_categoricos = pdf.select_dtypes(exclude=[np.number]).columns.tolist()

    # Ceros en numéricos
    reporte_ceros = []
    for col in campos_numericos:
        total_ceros = (pdf[col] == 0).sum()
        porcentaje = round((total_ceros / total_registros) * 100, 2)
        reporte_ceros.append([col, total_registros, total_ceros, porcentaje])

    # Nulos en todos
    reporte_nulos = []
    for col in pdf.columns:
        nulos = pdf[col].isnull().sum()
        porcentaje = round((nulos / total_registros) * 100, 2)
        tipo = 'NUMÉRICO' if col in campos_numericos else 'CATEGÓRICO'
        reporte_nulos.append([col, tipo, total_registros, nulos, porcentaje])

    # NUEVO: -1 en campos numéricos
    reporte_menos_uno = []
    for col in campos_numericos:
        total_menos_uno = (pdf[col] == -1).sum()
        porcentaje = round((total_menos_uno / total_registros) * 100, 2)
        if total_menos_uno > 0:
            reporte_menos_uno.append([col, total_registros, total_menos_uno, porcentaje])

    # NUEVO: "No Registra" en campos categóricos
    reporte_no_registra = []
    for col in campos_categoricos:
        total_no_registra = (pdf[col] == "No Registra").sum()
        porcentaje = round((total_no_registra / total_registros) * 100, 2)
        if total_no_registra > 0:
            reporte_no_registra.append([col, total_registros, total_no_registra, porcentaje])

    # Crear PDF
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []

    elements.append(Paragraph(f"REPORTE DE CALIDAD - TABLA: {tabla}", styles['Title']))
    elements.append(Paragraph(f"Total Campos: {total_campos} | Total Registros: {total_registros}", styles['Normal']))
    elements.append(Paragraph(f"Campos Numéricos: {len(campos_numericos)} | Campos Categóricos: {len(campos_categoricos)}", styles['Normal']))
    elements.append(Paragraph(" ", styles['Normal']))

    if reporte_ceros:
        elements.append(Paragraph("REPORTE CAMPOS NUMÉRICOS CON NIVEL DE CEROS", styles['Heading2']))
        data = [["CAMPO NUMÉRICO", "TOTAL REGISTROS", "REGISTROS CON 0", "% DE CEROS"]] + reporte_ceros
        table = Table(data, repeatRows=1)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
            ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ]))
        elements.append(table)
        elements.append(Paragraph(" ", styles['Normal']))

    if reporte_menos_uno:
        elements.append(Paragraph("REPORTE CAMPOS NUMÉRICOS CON VALORES -1", styles['Heading2']))
        data = [["CAMPO NUMÉRICO", "TOTAL REGISTROS", "REGISTROS CON -1", "% DE -1"]] + reporte_menos_uno
        table = Table(data, repeatRows=1)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
            ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ]))
        elements.append(table)
        elements.append(Paragraph(" ", styles['Normal']))

    if reporte_no_registra:
        elements.append(Paragraph("REPORTE CAMPOS CATEGÓRICOS CON 'No Registra'", styles['Heading2']))
        data = [["CAMPO CATEGÓRICO", "TOTAL REGISTROS", "'No Registra'", "%"]] + reporte_no_registra
        table = Table(data, repeatRows=1)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
            ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ]))
        elements.append(table)
        elements.append(Paragraph(" ", styles['Normal']))

    if reporte_nulos:
        elements.append(Paragraph("REPORTE DE NULOS EN CAMPOS", styles['Heading2']))
        data = [["CAMPO", "TIPO", "TOTAL REGISTROS", "VALORES NULOS", "% NULL"]] + reporte_nulos
        table = Table(data, repeatRows=1)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
            ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ]))
        elements.append(table)

    doc.build(elements)

    # Subir a S3
    bucket, key_prefix = s3_report_folder.replace("s3://", "").split("/", 1)
    report_key = f"{key_prefix.rstrip('/')}/reporte_{tabla}.pdf"
    s3.put_object(Bucket=bucket, Key=report_key, Body=buffer.getvalue(), ContentType='application/pdf')
    print(f"✅ PDF generado para {tabla} y guardado en s3://{bucket}/{report_key}")

 
# 🔁 Procesar cada tabla
for tabla in table_list:
    try:
        df = query_athena_table(athena_db, tabla, athena_output)
        generar_reporte_pdf(df, tabla, s3_report_folder)
    except Exception as e:
        print(f"❌ Error procesando {tabla}: {e}")
 