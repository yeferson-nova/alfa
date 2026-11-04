import sys
import time
import boto3
import pandas as pd
from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.platypus import Table, TableStyle, SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from awsglue.utils import getResolvedOptions 
# Parámetros esperados
args = getResolvedOptions(sys.argv, [
    'ATHENA_DATABASE',         
    'ATHENA_OUTPUT_S3',        
    'S3_REPORT_FOLDER',         
    'TABLES'                    
]) 
athena_db = args['ATHENA_DATABASE']
athena_output = args['ATHENA_OUTPUT_S3'].rstrip('/') + '/'  # para resultados de Athena
s3_report_folder = args['S3_REPORT_FOLDER'].rstrip('/') + '/'
table_list = [t.strip() for t in args['TABLES'].split(',')] 
athena = boto3.client('athena')
s3 = boto3.client('s3') 
def run_athena_query_and_get_df(query, database, output_location, timeout_secs=120):
    resp = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location}
    )
    qid = resp['QueryExecutionId']
    # Esperar a que termine
    poll_interval = 2
    waited = 0
    while waited < timeout_secs:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(poll_interval)
        waited += poll_interval
    if state != 'SUCCEEDED':
        reason = status['QueryExecution']['Status'].get('StateChangeReason', '')
        raise RuntimeError(f"Consulta falló ({state}): {reason}")
    # Descargar resultado
    output_bucket = output_location.replace("s3://", "").split("/", 1)[0]
    output_prefix = "/".join(output_location.replace("s3://", "").split("/", 1)[1:])
    result_key = f"{output_prefix}{qid}.csv" if output_prefix.endswith('/') else f"{output_prefix}/{qid}.csv"
    obj = s3.get_object(Bucket=output_bucket, Key=result_key)
    df = pd.read_csv(obj['Body'])
    return df 
def obtener_esquema_athena(database, table):
    # information_schema.columns contiene metadata de columnas
    # table_name en Athena suele ser minúscula; si no funciona, prueba lower/upper según tu caso
    query = f"""
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = '{database}' AND table_name = '{table}'
    ORDER BY ordinal_position
    """
    df = run_athena_query_and_get_df(query, database, athena_output)
    if df.empty:
        raise RuntimeError(f"No se encontró esquema para {table} en {database}")
    # Normalizar nombres
    df = df.rename(columns={
        'column_name': 'nombre_columna',
        'data_type': 'tipo_dato',
        'is_nullable': 'nullable'
    })[['nombre_columna', 'tipo_dato', 'nullable']]
    return df 
def generar_pdf_esquema(df_schema, tabla, s3_report_folder):
    total_columnas = len(df_schema) 
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, leftMargin=40, rightMargin=40, topMargin=40, bottomMargin=40)
    styles = getSampleStyleSheet()
    elements = [] 
    elements.append(Paragraph(f"ESTRUCTURA DE TABLA: {tabla}", styles['Title']))
    elements.append(Spacer(1, 6))
    elements.append(Paragraph(f"Número de columnas: {total_columnas}", styles['Normal']))
    elements.append(Spacer(1, 12))
    elements.append(Paragraph("DETALLE DE COLUMNAS", styles['Heading2'])) 
    data = [["Nombre de columna", "Tipo de dato", "Nullable"]] + df_schema[['nombre_columna', 'tipo_dato', 'nullable']].values.tolist()
    table = Table(data, repeatRows=1, hAlign='LEFT', colWidths=[150, 150, 80])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
    ]))
    elements.append(table) 
    doc.build(elements) 
    bucket, key_prefix = s3_report_folder.replace("s3://", "").split("/", 1)
    report_key = f"{key_prefix.rstrip('/')}/estructura_{tabla}.pdf"
    s3.put_object(Bucket=bucket, Key=report_key, Body=buffer.getvalue(), ContentType='application/pdf')
    print(f"✅ PDF generado para {tabla}: s3://{bucket}/{report_key}") 
def main():
    for tabla in table_list:
        try:
            print(f"\n--- Procesando tabla: {tabla} ---")
            schema_df = obtener_esquema_athena(athena_db, tabla)
            print(f"Columnas obtenidas para {tabla}: {len(schema_df)}")
            generar_pdf_esquema(schema_df, tabla, s3_report_folder)
        except Exception as e:
            print(f"❌ Error procesando {tabla}: {e}") 
if __name__ == "__main__":
    main()