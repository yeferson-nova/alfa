"""Extractor aislado PCR. Solo se conecta y escribe si se ejecuta main en Glue."""
import sys
import json
import gzip
import hashlib
import re
from urllib.parse import urlparse

def put_if_absent(client, **kwargs):

    def agregar_condicion(params, **event_kwargs):
        params.setdefault('headers', {})['If-None-Match'] = '*'
    evento = 'before-call.s3.PutObject'
    identificador = 'pcr-contrato-if-none-match'
    client.meta.events.register(evento, agregar_condicion, unique_id=identificador)
    try:
        return client.put_object(**kwargs)
    finally:
        client.meta.events.unregister(evento, unique_id=identificador)

def main():
    import boto3
    from botocore.exceptions import ClientError
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext
    from pyspark.sql import functions as F
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'PLAN_S3', 'PLAN_SHA256', 'TAREA', 'CONNECTION_NAME', 'ROOT_S3', 'LOTE_AMPLIACION', 'AUTORIZAR_EXTRACCION'])
    if args['AUTORIZAR_EXTRACCION'] != 'SI':
        raise ValueError('Extraccion no autorizada')
    if args['JOB_NAME'] != 'niif-dev-pcr-emisiones-ampliacion-b1-v1':
        raise ValueError('Job aislado diferente')
    session = boto3.Session(region_name='us-east-1')
    if session.client('sts').get_caller_identity()['Account'] != '533267035849':
        raise ValueError('Solo DEV')
    s3 = session.client('s3')
    prefix = 'pruebas/pcr/emisiones_generales_satelites/periodo=202606/'

    def split(uri, allowed_bucket=None):
        u = urlparse(uri)
        if u.scheme != 's3' or not u.netloc or u.query or u.fragment or (not u.path.lstrip('/').startswith(prefix)):
            raise ValueError('Ruta fuera del dominio aislado')
        if allowed_bucket and u.netloc != allowed_bucket:
            raise ValueError('Bucket diferente')
        return (u.netloc, u.path.lstrip('/'))

    def read_bytes(uri):
        b, k = split(uri, 'alfa-datalake-dev-assets')
        return s3.get_object(Bucket=b, Key=k)['Body'].read()

    def read_optional(uri):
        b, k = split(uri, 'alfa-datalake-dev-assets')
        try:
            return json.loads(s3.get_object(Bucket=b, Key=k)['Body'].read())
        except ClientError as exc:
            if exc.response['Error']['Code'] in ('NoSuchKey', '404', 'NotFound'):
                return None
            raise

    def put(uri, obj):
        b, k = split(uri, 'alfa-datalake-dev-assets')
        s3.put_object(Bucket=b, Key=k, Body=json.dumps(obj, ensure_ascii=False, indent=2).encode(), ContentType='application/json')
    plan_bytes = read_bytes(args['PLAN_S3'])
    if hashlib.sha256(plan_bytes).hexdigest() != args['PLAN_SHA256']:
        raise ValueError('Plan diferente al aprobado')
    plan = json.loads(plan_bytes)
    if plan.get('periodo_negocio') != '202606':
        raise ValueError('Periodo diferente')
    task = next((t for t in plan['tareas'] if t['tabla'] == args['TAREA']), None)
    if task is None:
        raise ValueError('Tarea no declarada')
    lote = int(args['LOTE_AMPLIACION'])
    if lote <= 0 or lote == 1793024990:
        raise ValueError('Se requiere lote nuevo, distinto de Vida')
    if task.get('where_adicional') is not None:
        raise ValueError('Este bloque no admite filtros adicionales')
    for identifier in [task['tabla'], task['llave']] + task['columnas']:
        if not re.fullmatch('[A-Z][A-Z0-9_]*', identifier):
            raise ValueError('Identificador SQL invalido')
    if task.get('esquema') != 'ACSELE_ALFA':
        raise ValueError('Esquema diferente')
    root = args['ROOT_S3'].rstrip('/') + '/'
    split(root, 'alfa-datalake-dev-assets')
    if '/ejecucion=' not in root:
        raise ValueError('ROOT debe ser exclusivo por ejecucion')
    code_hash = hashlib.sha256(open(__file__, 'rb').read()).hexdigest()
    contract = {'plan_sha256': args['PLAN_SHA256'], 'codigo_sha256': code_hash, 'lote_ampliacion': lote, 'connection_name': args['CONNECTION_NAME'], 'root': root, 'job': args['JOB_NAME'], 'periodo': '202606'}
    contract_uri = root + 'contrato.json'
    b, k = split(contract_uri, 'alfa-datalake-dev-assets')
    try:
        put_if_absent(s3, Bucket=b, Key=k, Body=json.dumps(contract, sort_keys=True).encode(), ContentType='application/json')
    except ClientError as exc:
        if exc.response['Error']['Code'] not in ('PreconditionFailed', '412'):
            raise
        if read_optional(contract_uri) != contract:
            raise ValueError('No reutilizar checkpoints de otro contrato')
    queue = read_bytes(task['cola_s3'])
    if hashlib.sha256(queue).hexdigest() != task['cola_sha256']:
        raise ValueError('Cola diferente')
    import csv
    import io
    with gzip.GzipFile(fileobj=io.BytesIO(queue)) as compressed:
        reader = csv.reader(io.TextIOWrapper(compressed, encoding='utf-8'))
        if next(reader) != [task['llave']]:
            raise ValueError('Cabecera de cola diferente')
        keys = []
        seen = set()
        for row in reader:
            if len(row) != 1 or not re.fullmatch('-?[0-9]+', row[0]):
                raise ValueError('Llave invalida')
            key = str(int(row[0]))
            if key in seen:
                raise ValueError('Llave duplicada')
            seen.add(key)
            keys.append(key)
    if len(keys) != task['total_llaves'] or not keys:
        raise ValueError('Cantidad de llaves diferente o vacia')
    gc = GlueContext(SparkContext.getOrCreate())
    spark = gc.spark_session
    spark.conf.set('spark.sql.caseSensitive', 'false')
    for setting in ('spark.sql.parquet.datetimeRebaseModeInRead', 'spark.sql.parquet.int96RebaseModeInRead', 'spark.sql.parquet.datetimeRebaseModeInWrite', 'spark.sql.parquet.int96RebaseModeInWrite'):
        spark.conf.set(setting, 'LEGACY')
    job = Job(gc)
    job.init(args['JOB_NAME'], args)
    credentials = gc.extract_jdbc_conf(args['CONNECTION_NAME'])
    url = credentials.get('fullUrl') or credentials.get('url')
    if not url or not url.startswith('jdbc:oracle:'):
        raise ValueError('Conexion no Oracle')
    props = {'user': credentials['user'], 'password': credentials['password'], 'driver': 'oracle.jdbc.OracleDriver', 'fetchsize': '10000', 'queryTimeout': '600', 'oracle.net.CONNECT_TIMEOUT': '30000', 'oracle.jdbc.ReadTimeout': '660000'}
    taskroot = root + task['tabla'].lower() + '/'
    rootkey = split(root, 'alfa-datalake-dev-assets')[1]
    output = 's3://alfa-datalake-dev-bronce/' + rootkey + task['tabla'].lower() + '/datos/'
    state = {'estado': 'RUNNING', 'tabla': task['tabla'], 'lote_ampliacion': lote, 'periodo_negocio': '202606', 'total_llaves': len(keys), 'grupos_confirmados': 0, 'total_registros': 0, 'ruta_dataset': output, 'certificado': False, 'cobertura': 'BLOQUE_INICIAL_NO_COMPLETO'}
    try:
        for group, start in enumerate(range(0, len(keys), 500)):
            subset = keys[start:start + 500]
            digest = hashlib.sha256(','.join(subset).encode()).hexdigest()
            commit_uri = taskroot + 'commits/grupo=' + str(group) + '.json'
            commit = read_optional(commit_uri)
            if commit is not None:
                if commit.get('llaves_sha256') != digest or commit.get('lote') != lote:
                    raise ValueError('Checkpoint incompatible')
                ob, ok = split(commit['ruta'], 'alfa-datalake-dev-bronce')
                present = s3.list_objects_v2(Bucket=ob, Prefix=ok, MaxKeys=1000)
                if not any((x['Key'].endswith('.parquet') for x in present.get('Contents', []))):
                    raise ValueError('Checkpoint sin Parquet; no se omite')
            else:
                import uuid
                path = output + 'grupo=' + str(group) + '/intento=' + uuid.uuid4().hex + '/'
                sql = '(SELECT ' + ','.join(task['columnas']) + ' FROM ACSELE_ALFA.' + task['tabla'] + ' WHERE ' + task['llave'] + ' IN (' + ','.join(subset) + ')) Q'
                frame = spark.read.jdbc(url=url, table=sql, properties=props)
                frame.withColumn('LOTE_CARGUE_ID', F.lit(lote).cast('bigint')).write.mode('errorifexists').option('compression', 'snappy').parquet(path)
                written = spark.read.parquet(path)
                rows = written.count()
                if written.filter(F.col('LOTE_CARGUE_ID') != lote).limit(1).count():
                    raise ValueError('Lote de salida diferente')
                commit = {'grupo': group, 'llaves_sha256': digest, 'lote': lote, 'filas': rows, 'ruta': path}
                put(commit_uri, commit)
            state['grupos_confirmados'] += 1
            state['total_registros'] += int(commit['filas'])
            put(taskroot + 'estado.json', state)
            print('GRUPO_CONFIRMADO', task['tabla'], group, commit['filas'], flush=True)
        commits = [read_optional(taskroot + 'commits/grupo=' + str(i) + '.json') for i in range(state['grupos_confirmados'])]
        put(taskroot + 'inventario_confirmado.json', {'tabla': task['tabla'], 'lote': lote, 'rutas_confirmadas': [c['ruta'] for c in commits], 'total_registros': state['total_registros'], 'advertencia': 'No leer prefijo completo: puede contener intentos no confirmados.'})
        state['estado'] = 'SUCCESS_BLOQUE_INICIAL'
        put(taskroot + 'estado.json', state)
        job.commit()
    except Exception as exc:
        state['estado'] = 'FAILED'
        state['error_tipo'] = type(exc).__name__
        put(taskroot + 'estado.json', state)
        raise
if __name__ == '__main__':
    main()
