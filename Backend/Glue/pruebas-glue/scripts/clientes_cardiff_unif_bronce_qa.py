import sys
import logging
import boto3
from io import BytesIO
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import DataFrame
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors 
# Configuración de logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__) 
# Parámetros
args = getResolvedOptions(sys.argv, [
    'S3_INITIAL_PATH',
    'S3_DELTA_PATH',
    'S3_OUTPUT_CSV',
    'S3_REPORT_PATH',
    'DELIMITER'
])
s3_initial = args['S3_INITIAL_PATH'].rstrip('/') + '/'
s3_delta = args['S3_DELTA_PATH'].rstrip('/') + '/'
s3_output_csv = args['S3_OUTPUT_CSV']
s3_report_path = args['S3_REPORT_PATH']
delimiter = args['DELIMITER'] 
# Contextos Spark / Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3 = boto3.client('s3') 
def parse_s3_uri(uri: str):
    if not uri.startswith("s3://"):
        raise ValueError(f"URI inválida: {uri}")
    path = uri[5:]
    parts = path.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key
  
def read_initial(s3_path: str, delimiter: str) -> DataFrame:
    # Lee los txt de initial con header e infiere esquema
    log.info(f"Lectura de initial desde {s3_path}")
    df = spark.read.option("header", "true") \
                   .option("sep", delimiter) \
                   .option("inferSchema", "true") \
                   .csv(s3_path)
    return df 
def read_delta(s3_path: str, schema, delimiter: str) -> DataFrame:
    """
    Lee cada archivo bajo s3_path, omite la primera línea de cada archivo (indicador),
    y arma un DataFrame con el esquema provisto.
    """
    log.info(f"Lectura de delta desde {s3_path}, omitiendo primera línea de cada archivo")
    # wholeTextFiles trae (ruta, contenido) por archivo
    files_rdd = sc.wholeTextFiles(s3_path)
    # Para cada archivo: separar en líneas, omitir la primera, emitir las restantes
    lines_rdd = files_rdd.flatMap(lambda pc: pc[1].splitlines()[1:])
    # Crear DataFrame desde las líneas filtradas como CSV con el esquema dado
    df = spark.read.option("header", "false") \
                   .option("sep", delimiter) \
                   .schema(schema) \
                   .csv(lines_rdd)
    return df
  
def move_single_csv_from_tmp(tmp_s3_path: str, dest_s3_path: str):
    tmp_bucket, tmp_prefix = parse_s3_uri(tmp_s3_path)
    dest_bucket, dest_key = parse_s3_uri(dest_s3_path) 
    objs = s3.list_objects_v2(Bucket=tmp_bucket, Prefix=tmp_prefix)
    if 'Contents' not in objs:
        raise RuntimeError(f"No se encontró output temporal en {tmp_s3_path}") 
    csv_candidates = [
        o['Key']
        for o in objs['Contents']
        if o['Key'].endswith(".csv") and "part-" in o['Key']
    ] 
    if not csv_candidates:
        raise RuntimeError("No se encontró el archivo part-*.csv generado.")
    if len(csv_candidates) > 1:
        log.warning(f"Se encontraron múltiples part-*.csv: {csv_candidates}. Se usará el primero ordenado.")
        csv_candidates.sort()
    source_key = csv_candidates[0] 
    copy_source = {'Bucket': tmp_bucket, 'Key': source_key}
    s3.copy_object(Bucket=dest_bucket, Key=dest_key, CopySource=copy_source)
    log.info(f"Copiado {source_key} a s3://{dest_bucket}/{dest_key}") 
    # Limpiar todo el tmp (incluye _SUCCESS, etc.)
    to_delete = [{'Key': o['Key']} for o in objs['Contents']]
    s3.delete_objects(Bucket=tmp_bucket, Delete={'Objects': to_delete})
    log.info(f"Temporal {tmp_prefix} limpiado en bucket {tmp_bucket}") 
def unify_and_write_csv(df: DataFrame, output_s3_path: str):
    # Escribir como CSV unificado (single file) en S3
    tmp_path = output_s3_path.rstrip("/") + ".tmp/"
    log.info(f"Escribiendo CSV unificado temporal en {tmp_path}")
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_path)
    log.info("Moviendo único CSV generado a destino final")
    move_single_csv_from_tmp(tmp_path, output_s3_path)
  
def schema_to_list(df: DataFrame):
    schema = df.schema
    rows = []
    for field in schema.fields:
        rows.append({
            'nombre_columna': field.name,
            'tipo_dato': field.dataType.simpleString(),
            'nullable': 'YES' if field.nullable else 'NO'
        })
    return rows 
def generar_pdf(schema_rows: list, total_registros: int, output_pdf_s3: str, base_name: str = "unificado"):
    styles = getSampleStyleSheet()
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        leftMargin=40,
        rightMargin=40,
        topMargin=40,
        bottomMargin=40
    )
    elements = []
    # Título
    elements.append(Paragraph(f"ESTRUCTURA DEL CSV UNIFICADO ({base_name})", styles['Title']))
    elements.append(Spacer(1, 6))
    elements.append(Paragraph(f"Número total de registros: {total_registros}", styles['Normal']))
    elements.append(Paragraph(f"Número de columnas: {len(schema_rows)}", styles['Normal']))
    elements.append(Spacer(1, 12))
    # Tabla de esquema
    elements.append(Paragraph("DETALLE DE COLUMNAS", styles['Heading2']))
    data = [["Nombre de columna", "Tipo de dato", "Nullable"]]
    for r in schema_rows:
        data.append([r['nombre_columna'], r['tipo_dato'], r['nullable']])
    table = Table(data, repeatRows=1, colWidths=[180, 180, 80])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
    ]))
    elements.append(table)
    doc.build(elements)
    # Subir a S3
    bucket, key = parse_s3_uri(output_pdf_s3)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue(), ContentType='application/pdf')
    log.info(f"✅ PDF subido a {output_pdf_s3}")
  
def main():
    try:
        # 1. Leer initial y obtener esquema
        df_initial = read_initial(s3_initial, delimiter)
        schema = df_initial.schema 
        # 2. Leer delta con esquema (omitiendo primer registro de cada archivo)
        df_delta = read_delta(s3_delta, schema, delimiter) 
        # 3. Unir
        df_union = df_initial.unionByName(df_delta) 
        # 4. Contar registros
        total = df_union.count()
        log.info(f"Total registros unificados: {total}") 
        # 5. Escribir CSV unificado
        unify_and_write_csv(df_union, s3_output_csv)
        log.info(f"CSV unificado escrito en: {s3_output_csv}") 
        # 6. Obtener estructura y generar PDF
        schema_rows = schema_to_list(df_union)
        generar_pdf(schema_rows, total, s3_report_path) 
    except Exception:
        log.exception("Fallo en la ejecución del job")
        raise
    finally:
        sc.stop()
        log.info("SparkContext detenido") 
if __name__ == "__main__":
    main()