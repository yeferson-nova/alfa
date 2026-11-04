"""Nucleo de checkpoint por segmentos. Biblioteca, no extractor ni desplegador.
No importa boto3. Recibe backend y callback del extractor. No consulta Oracle.
El plan debe estar persistido y acotado antes de ejecutarlo; no se descubren IDs aqui.
Datos y llaves se escriben en prefijos nuevos por intento; solo se consumen commits.
Un solo escritor por ejecucion: la orquestacion debe impedir concurrencia.
"""
import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
VERSION = 1

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode('utf-8')

def fingerprint(value):
    return hashlib.sha256(canonical(value)).hexdigest()

def now():
    return datetime.now(timezone.utc).isoformat()

def validate_plan(plan):
    required = {'execution_id', 'periodo', 'lote', 'config_hash', 'source_snapshot', 'segments'}
    if not required.issubset(plan):
        raise ValueError('Plan incompleto: ' + str(sorted(required - set(plan))))
    if not re.fullmatch('[a-f0-9]{32}', plan['execution_id']):
        raise ValueError('execution_id invalido')
    if not re.fullmatch('[0-9]{6}', plan['periodo']):
        raise ValueError('periodo invalido')
    from datetime import datetime
    if datetime.strptime(plan['periodo'], '%Y%m').strftime('%Y%m') != plan['periodo']:
        raise ValueError('periodo invalido')
    if not re.fullmatch('[a-f0-9]{64}', plan['config_hash']):
        raise ValueError('config_hash invalido')
    if not plan['source_snapshot']:
        raise ValueError('Se requiere identidad de fuente; no inventar SCN ni snapshot')
    if not isinstance(plan['lote'], int) or isinstance(plan['lote'], bool):
        raise ValueError('lote debe ser entero')
    segments = plan['segments']
    if not isinstance(segments, list) or not segments:
        raise ValueError('Plan sin segmentos explicitos')
    ids = set()
    for seg in segments:
        sid = seg.get('id', '')
        if not re.fullmatch('[A-Za-z0-9_-]{1,100}', sid) or sid in ids:
            raise ValueError('Segmento duplicado o invalido')
        ids.add(sid)
        if not seg.get('input_ref') or not seg.get('input_hash'):
            raise ValueError('Falta referencia persistida/hash de llaves del segmento')
        if not re.fullmatch('[a-f0-9]{64}', seg['input_hash']):
            raise ValueError('Hash de entrada invalido')
        if type(seg.get('max_rows')) is not int or seg['max_rows'] < 1:
            raise ValueError('Limite de filas por segmento obligatorio')
        if type(seg.get('max_bytes')) is not int or seg['max_bytes'] < 1:
            raise ValueError('Limite de bytes por segmento obligatorio')
    return fingerprint(plan)

class S3Store:
    """Adaptador de metadatos: s3 es un cliente suministrado por el caller.
    Los datos Parquet no pasan por este adaptador.
    """

    def __init__(self, s3, bucket):
        self.s3, self.bucket = (s3, bucket)

    def get(self, key):
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            code = getattr(exc, 'response', {}).get('Error', {}).get('Code')
            if code in ('NoSuchKey', '404'):
                return None
            raise
        return json.loads(response['Body'].read().decode('utf-8'))

    def create(self, key, value):
        args = dict(Bucket=self.bucket, Key=key, Body=canonical(value), ContentType='application/json')
        operation = self.s3.meta.service_model.operation_model('PutObject')
        if 'IfNoneMatch' in operation.input_shape.members:
            return self.s3.put_object(**args, IfNoneMatch='*')
        event = 'before-call.s3.PutObject'
        uid = 'checkpoint-if-none-match-' + uuid.uuid4().hex

        def add_condition(params, **kwargs):
            params.setdefault('headers', {})['If-None-Match'] = '*'
        self.s3.meta.events.register_first(event, add_condition, unique_id=uid)
        try:
            return self.s3.put_object(**args)
        finally:
            self.s3.meta.events.unregister(event, unique_id=uid)

class SegmentRunner:
    """No omite el segmento fallido. Omite solo commits validos.
    extract(segment, attempt_prefix) -> resultado de escrituras completas.
    verify(result) debe leer metadatos reales de salida, no limitarse a devolver True.
    No se permite consumir archivos huerfanos por listado recursivo del prefijo.
    """

    def __init__(self, store, root, plan):
        self.store = store
        self.plan = json.loads(canonical(plan))
        self.plan_hash = validate_plan(self.plan)
        self.root = root.rstrip('/') + '/'
        if not self.root.endswith('/' + plan['execution_id'] + '/'):
            raise ValueError('Prefijo no corresponde a execution_id')
        key = self.root + 'plan.json'
        existing = store.get(key)
        expected = {'version': VERSION, 'plan_hash': self.plan_hash, 'plan': self.plan}
        if existing is None:
            store.create(key, expected)
        elif existing != expected:
            raise ValueError('Plan/configuracion/lote/fuente cambiaron; no reanudar')

    def _commit_key(self, seg):
        return self.root + 'commits/' + seg['id'] + '.json'

    def _check(self, seg, result):
        if result.get('segment_id') != seg['id']:
            raise ValueError('Resultado de otro segmento')
        if result.get('input_hash') != seg['input_hash']:
            raise ValueError('Llaves de entrada cambiaron')
        if result.get('plan_hash') != self.plan_hash:
            raise ValueError('Resultado de otro plan')
        prefix = result.get('attempt_prefix', '')
        expected = self.root + 'attempts/' + seg['id'] + '/'
        if not prefix.startswith(expected) or not re.fullmatch('[a-f0-9]{32}/', prefix[len(expected):]):
            raise ValueError('Prefijo de intento fuera del segmento')
        for field in ('rows', 'bytes'):
            if type(result.get(field)) is not int or result[field] < 0:
                raise ValueError('Conteo no valido: ' + field)
        if result['rows'] > seg['max_rows'] or result['bytes'] > seg['max_bytes']:
            raise ValueError('Segmento excede limites: no confirmar ni truncar')
        for name in ('data_files', 'key_files'):
            files = result.get(name)
            if not isinstance(files, list) or len(files) != len(set(files)):
                raise ValueError('Manifest de archivos ausente o duplicado')
            if any((not isinstance(p, str) or not p.startswith(prefix) for p in files)):
                raise ValueError('Archivo fuera del intento')
        if result['rows'] and (not result['data_files']):
            raise ValueError('Filas positivas sin archivos')
        if result.get('keys_required') is not True:
            raise ValueError('Debe declararse el tratamiento de llaves explicitamente')
        if type(result.get('key_rows')) is not int or result['key_rows'] < 0:
            raise ValueError('Conteo de llaves invalido')
        if result['key_rows'] and (not result['key_files']):
            raise ValueError('Llaves positivas sin archivos')
        if result.get('writes_complete') is not True:
            raise ValueError('Escrituras no completadas')

    def run(self, extract, verify, fail_after=None):
        if fail_after is not None and (type(fail_after) is not int or fail_after < 1):
            raise ValueError('fail_after invalido')
        new_commits = 0
        for seg in self.plan['segments']:
            key = self._commit_key(seg)
            old = self.store.get(key)
            if old is not None:
                if old.get('version') != VERSION or old.get('plan_hash') != self.plan_hash:
                    raise ValueError('Commit de otro plan/version')
                self._check(seg, old['result'])
                verify(old['result'])
                continue
            # Los segmentos pendientes se procesan abajo en grupos paralelos.
            continue

        pending = []
        for seg in self.plan['segments']:
            if self.store.get(self._commit_key(seg)) is None:
                pending.append(seg)

        max_parallel = int(
            globals().get('config', {})
            .get('extension_pcr', {})
            .get('max_consultas_paralelas', 4)
        )
        if max_parallel < 1 or max_parallel > 4:
            raise ValueError('max_consultas_paralelas debe estar entre 1 y 4')

        from concurrent.futures import ThreadPoolExecutor, as_completed

        def ejecutar_segmento(seg):
            prefix = (
                self.root + 'attempts/' + seg['id'] + '/' +
                uuid.uuid4().hex + '/'
            )
            result = extract(
                json.loads(canonical(seg)),
                prefix,
                self.plan_hash
            )
            self._check(seg, result)
            verify(result)
            return seg, result

        with ThreadPoolExecutor(max_workers=max_parallel) as pool:
            futures = [
                pool.submit(ejecutar_segmento, seg)
                for seg in pending
            ]
            for future in as_completed(futures):
                seg, result = future.result()
                commit = {
                    'version': VERSION,
                    'plan_hash': self.plan_hash,
                    'result': result,
                    'committed_at': now()
                }
                self.store.create(self._commit_key(seg), commit)
                new_commits += 1
                if fail_after == new_commits:
                    raise RuntimeError(
                        'FALLO_CONTROLADO_DESPUES_DE_COMMIT'
                    )
        results = self.committed(verify)
        final = {'version': VERSION, 'plan_hash': self.plan_hash, 'segments': len(results), 'rows_sum': sum((x['rows'] for x in results)), 'key_rows_sum_before_distinct': sum((x['key_rows'] for x in results)), 'commits': [self._commit_key(s) for s in self.plan['segments']]}
        key = self.root + 'complete.json'
        existing = self.store.get(key)
        if existing is None:
            self.store.create(key, final)
        elif existing != final:
            raise ValueError('Cierre distinto del plan')
        return final

    def committed(self, verify):
        results = []
        for seg in self.plan['segments']:
            commit = self.store.get(self._commit_key(seg))
            if commit is None:
                raise ValueError('Faltan segmentos; no publicar dataset completo')
            if commit.get('plan_hash') != self.plan_hash:
                raise ValueError('Commit incompatible')
            self._check(seg, commit['result'])
            verify(commit['result'])
            results.append(commit['result'])
        return results

    def key_paths(self, verify):
        return [path for result in self.committed(verify) for path in result['key_files']]
PCR_CHECKPOINT_VERSION = 'a2d83da04575ab32ae1fd256fbfb81062576d356ea9abec0db3c2ffe4865e48a'

def pcr_recuperar_bloques(tabla_cfg, opciones, queries):
    _pcr_verified_once = set()
    from pyspark.sql import functions as F
    import hashlib, uuid
    if not config.get('extension_pcr', {}).get('checkpoint_bloques', False):
        raise ValueError('Checkpoint no habilitado')
    relative = normalizar_tabla_relativa(tabla_cfg)
    root = (
        'niif/pcr-vida/checkpoints/' +
        hashlib.sha256(relative.encode()).hexdigest()[:16] + '/' +
        pcr_id + '/'
    )
    client = boto3.client('s3')
    bucket = 'alfa-datalake-dev-assets'
    store = S3Store(client, bucket)
    columns = tabla_cfg['columnas']
    outputs = pcr_llaves_salida.get(relative, [])
    keycols = list(dict.fromkeys((c for group in outputs for c in group)))
    if not keycols:
        keycols = list(columns)
    segments = []
    for i, query in enumerate(queries):
        sid = str(i + 1).zfill(5)
        key = root + 'inputs/' + sid + '.json'
        value = {'query': query, 'tabla': relative}
        existing = store.get(key)
        if existing is None:
            store.create(key, value)
        elif existing != value:
            raise ValueError('Consulta cambio; no reanudar')
        segments.append({'id': sid, 'input_ref': key, 'input_hash': fingerprint(value), 'max_rows': 100000, 'max_bytes': 134217728})
    source_snapshot = fingerprint({
        'tipo': 'ORACLE_QUERY_PLAN',
        'tabla': relative,
        'periodo': str(periodo_negocio),
        'connection_name': connection_name,
        'queries': [
            fingerprint({
                'orden': i + 1,
                'query': query
            })
            for i, query in enumerate(queries)
        ]
    })

    plan = {
        'execution_id': pcr_id,
        'periodo': str(periodo_negocio),
        'lote': int(timestamp_ingesta),
        'config_hash': fingerprint({
            'config': config,
            'version': PCR_CHECKPOINT_VERSION
        }),
        'source_snapshot': source_snapshot,
        'segments': segments
    }
    runner = SegmentRunner(store, root, plan)

    def files(prefix):
        result = []
        size = 0
        for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
            for o in page.get('Contents', []):
                if o['Key'].endswith('.parquet'):
                    result.append(o['Key'])
                    size += o['Size']
        return (sorted(result), size)

    def extract(seg, prefix, ph):
        import time
        started = time.monotonic()
        value = store.get(seg['input_ref'])
        if fingerprint(value) != seg['input_hash']:
            raise ValueError('Entrada modificada')
        opt = dict(opciones)
        opt['sampleQuery'] = value['query']
        df = _vida_pivote_guardado(glueContext.create_dynamic_frame.from_options, connection_type='oracle', connection_options=opt).toDF()
        names = [c.upper() for c in df.columns]
        if len(names) != len(set(names)):
            raise ValueError('Columnas ambiguas')
        df = df.toDF(*names).select(*columns)
        uri = 's3://' + bucket + '/' + prefix + 'datos/'
        df.coalesce(1).write.mode('errorifexists').option('compression', 'snappy').parquet(uri)
        write_end = time.monotonic()
        saved = spark.read.parquet(uri)
        count = saved.count()
        count_end = time.monotonic()
        if count > seg['max_rows']:
            raise ValueError('Bloque supera limite de filas; no confirmar')
        keys = saved.select(*keycols).dropDuplicates()
        key_uri = 's3://' + bucket + '/' + prefix + 'llaves/'
        keys.coalesce(1).write.mode('errorifexists').parquet(key_uri)
        keycount = spark.read.parquet(key_uri).count()
        key_end = time.monotonic()
        data, nb = files(prefix + 'datos/')
        kf, kb = files(prefix + 'llaves/')
        result = {'segment_id': seg['id'], 'input_hash': seg['input_hash'], 'plan_hash': ph, 'attempt_prefix': prefix, 'rows': count, 'bytes': nb + kb, 'key_rows': keycount, 'keys_required': True, 'data_files': data, 'key_files': kf, 'writes_complete': True, 'tiempos': {'jdbc_y_escritura_datos_s': round(write_end - started, 3), 'conteo_parquet_datos_s': round(count_end - write_end, 3), 'llaves_escritura_y_conteo_s': round(key_end - count_end, 3)}}
        print('PCR_TIEMPOS_BLOQUE ' + json.dumps({'tabla': relative, 'segmento': seg['id'], 'filas': count, 'archivos_datos': len(data), 'archivos_llaves': len(kf), 'tiempos': result['tiempos']}), flush=True)
        return result

    def verify(result):
        import time
        signature = fingerprint(result)
        if signature in _pcr_verified_once:
            return
        start = time.monotonic()
        for key in result['data_files'] + result['key_files']:
            client.head_object(Bucket=bucket, Key=key)
        if spark.read.parquet(*['s3://' + bucket + '/' + k for k in result['data_files']]).count() != result['rows']:
            raise ValueError('Conteo datos distinto')
        if spark.read.parquet(*['s3://' + bucket + '/' + k for k in result['key_files']]).count() != result['key_rows']:
            raise ValueError('Conteo llaves distinto')
        _pcr_verified_once.add(signature)
        print('PCR_VERIFICACION_BLOQUE ' + json.dumps({'tabla': relative, 'segmento': result['segment_id'], 'segundos': round(time.monotonic() - start, 3)}), flush=True)
    flag = '--PCR_FALLO_CONTROLADO'
    fail = False
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 >= len(sys.argv) or sys.argv[i + 1] not in ('true', 'false'):
            raise ValueError('PCR_FALLO_CONTROLADO invalido')
        fail = sys.argv[i + 1] == 'true'
    runner.run(extract, verify, fail_after=1 if fail and relative == 'acsele_alfa/policydco' else None)
    committed = runner.committed(verify)
    paths = [k for result in committed for k in result['data_files']]
    parametros_extraccion.setdefault('checkpoint_bloques_pcr', {})[relative] = {'bloques': len(committed), 'filas': sum((r['rows'] for r in committed)), 'manifest': root + 'complete.json', 'advertencia': 'Pivote S3 identificado; Oracle sin SCN compartido. No acredita snapshot consistente entre intentos.'}
    return (
        spark.read
        .parquet(*[
            's3://' + bucket + '/' + key
            for key in paths
        ])
        .select(*columns)
        .dropDuplicates()
    )

def _vida_pivote_guardado(_original, *args, **kwargs):
    """Lectura JDBC real. No reemplaza CONTEXTOPERATION por Parquet."""
    return _original(*args, **kwargs)

import sys
import logging
import boto3
from urllib.parse import urlparse
from datetime import datetime
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, monotonically_increasing_id
from pyspark import StorageLevel

logger = logging.getLogger(__name__)

def ahora_dt():
    return datetime.now()

def ahora_str():
    return datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

def segundos_entre(inicio, fin):
    return int((fin - inicio).total_seconds())

def minutos(segundos):
    return None if segundos is None else round(float(segundos) / 60.0, 2)

def normalizar_texto(valor):
    return str(valor).lower().strip().replace(' ', '_')

def ensure_trailing_slash(path):
    return path if path.endswith('/') else path + '/'

def parse_s3_uri(ruta_s3):
    parsed = urlparse(ruta_s3)
    return (parsed.netloc, parsed.path.lstrip('/'))

def existe_s3(ruta_s3):
    bucket, key = parse_s3_uri(ruta_s3)
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') in ['404', 'NoSuchKey', 'NotFound']:
            return False
        raise

def existe_prefijo_s3(ruta_s3):
    bucket, prefix = parse_s3_uri(ruta_s3)
    s3 = boto3.client('s3')
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return 'Contents' in resp

def obtener_lotes_desde_ruta_llaves(ruta_llaves_base):
    if not ruta_llaves_base:
        return []
    bucket, prefix = parse_s3_uri(ruta_llaves_base)
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    lotes = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            match = re.search('lote_cargue_id=(\\d+)', key)
            if match:
                lotes.add(int(match.group(1)))
    return sorted(list(lotes))

def cargar_json_s3(ruta_s3):
    bucket, key = parse_s3_uri(ruta_s3)
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj['Body'].read().decode('utf-8'))

def guardar_json_s3(ruta_s3, contenido):
    bucket, key = parse_s3_uri(ruta_s3)
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(contenido, ensure_ascii=False, indent=2).encode('utf-8'), ContentType='application/json')

def pcr_identificador(valor):
    if not re.fullmatch('[A-Za-z][A-Za-z0-9_]*', str(valor)):
        raise ValueError('Identificador SQL no permitido: ' + str(valor))
    return str(valor)

def pcr_literal(valor):
    from decimal import Decimal
    if isinstance(valor, bool):
        raise ValueError('Booleano no permitido como llave')
    if isinstance(valor, (int, Decimal)):
        numero = Decimal(str(valor))
        if not numero.is_finite():
            raise ValueError('Llave numerica no finita')
        return format(numero, 'f')
    if isinstance(valor, str):
        return "'" + valor.replace("'", "''") + "'"
    raise ValueError('Tipo de llave no soportado: ' + type(valor).__name__)

def pcr_predicado(mapeo, filas):
    pares = [
        (
            pcr_identificador(x['origen']),
            pcr_identificador(x['destino'])
        )
        for x in mapeo
    ]

    if not pares:
        raise ValueError('Dependencia sin mapeo')

    if len(pares) == 1:
        origen, destino = pares[0]
        valores = {
            pcr_literal(fila[origen])
            for fila in filas
            if fila[origen] is not None and fila[origen] != ''
        }

        if not valores:
            return '1=0'

        if len(valores) > 1000:
            raise ValueError(
                'Bloque de llave simple supera 1000 valores Oracle'
            )

        return (
            destino
            + ' IN ('
            + ', '.join(sorted(valores))
            + ')'
        )

    condiciones = set()

    for fila in filas:
        valores = [fila[origen] for origen, destino in pares]

        if any(valor is None or valor == '' for valor in valores):
            continue

        condiciones.add(
            '('
            + ' AND '.join(
                destino + ' = ' + pcr_literal(valor)
                for (origen, destino), valor
                in zip(pares, valores)
            )
            + ')'
        )

    return (
        '(' + ' OR '.join(sorted(condiciones)) + ')'
        if condiciones
        else '1=0'
    )

def pcr_consulta(tabla_cfg, filtros_dependencia=None):
    columnas = tabla_cfg.get('columnas', [])
    if not columnas:
        raise ValueError('PCR requiere columnas explicitas')
    proyeccion = ', '.join((pcr_identificador(c) for c in columnas))
    tabla = pcr_identificador(tabla_cfg['schema']) + '.' + pcr_identificador(tabla_cfg['tabla'])
    filtros = []
    if tabla_cfg.get('where'):
        filtros.append('(' + tabla_cfg['where'] + ')')
    if filtros_dependencia is not None:
        filtros.append('(' + ' OR '.join(filtros_dependencia) + ')')
    consulta = 'SELECT ' + proyeccion + ' FROM ' + tabla
    if filtros:
        consulta += ' WHERE ' + ' AND '.join(filtros)
    limite = tabla_cfg.get('limite_muestra')
    if limite is not None:
        if tabla_cfg.get('dependencias_lectura'):
            raise ValueError('No limitar cada tabla hija: perderia dependencias')
        if not isinstance(limite, int) or not 1 <= limite <= 50:
            raise ValueError('Limite de pivote fuera de 1..50')
        orden = tabla_cfg.get('orden_muestra', [])
        if not orden:
            raise ValueError('La muestra requiere orden explicito')
        if not tabla_cfg.get('muestra_sin_orden', False):
            consulta += ' ORDER BY ' + ', '.join((pcr_identificador(c) for c in orden))
        consulta = 'SELECT * FROM (' + consulta + ') WHERE ROWNUM <= ' + str(limite)
    if len(consulta) > 200000:
        raise ValueError('Consulta demasiado grande para prueba; no se extrae tabla completa')
    return consulta


def pcr_indice_tablas():
    indice = {}

    for ciclo in config.get('ciclos', []):
        for segmento in ciclo.get('segmentos', []):
            for tabla_cfg in segmento.get('tablas', []):
                relativa = normalizar_tabla_relativa(tabla_cfg)
                indice[relativa] = tabla_cfg

    return indice


def pcr_condicion_relacion(dep, alias_padre, alias_hija):
    mapeo = dep.get('mapeo', [])

    if not mapeo:
        raise ValueError(
            'Dependencia sin mapeo: '
            + str(dep.get('tabla_origen'))
        )

    condiciones = []

    for relacion in mapeo:
        origen = pcr_identificador(relacion['origen'])
        destino = pcr_identificador(relacion['destino'])

        condiciones.append(
            alias_padre + '.' + origen
            + ' = '
            + alias_hija + '.' + destino
        )

    return ' AND '.join(condiciones)


def pcr_filtro_relacional(
    tabla_cfg,
    alias_actual,
    indice,
    nivel=0,
    visitadas=None
):
    if visitadas is None:
        visitadas = set()

    relativa = normalizar_tabla_relativa(tabla_cfg)

    if relativa in visitadas:
        raise ValueError(
            'Ciclo detectado en dependencias_lectura: ' + relativa
        )

    visitadas = set(visitadas)
    visitadas.add(relativa)

    filtros = []

    where = tabla_cfg.get('where')

    if where:
        filtros.append('(' + where + ')')

    dependencias = tabla_cfg.get('dependencias_lectura', [])

    if dependencias:
        exists_dependencias = []

        for posicion, dep in enumerate(dependencias):
            padre_relativa = dep['tabla_origen'].lower()

            if padre_relativa not in indice:
                raise ValueError(
                    'Tabla origen no configurada: ' + padre_relativa
                )

            padre_cfg = indice[padre_relativa]
            alias_padre = 'T' + str(nivel + 1) + '_' + str(posicion)

            tabla_padre = (
                pcr_identificador(padre_cfg['schema'])
                + '.'
                + pcr_identificador(padre_cfg['tabla'])
            )

            relacion = pcr_condicion_relacion(
                dep,
                alias_padre,
                alias_actual
            )

            filtro_padre = pcr_filtro_relacional(
                padre_cfg,
                alias_padre,
                indice,
                nivel + 1,
                visitadas
            )

            subconsulta = (
                'EXISTS ('
                'SELECT 1 FROM '
                + tabla_padre
                + ' '
                + alias_padre
                + ' WHERE '
                + relacion
            )

            if filtro_padre:
                subconsulta += ' AND (' + filtro_padre + ')'

            subconsulta += ')'
            exists_dependencias.append(subconsulta)

        filtros.append(
            '(' + ' OR '.join(exists_dependencias) + ')'
        )

    return ' AND '.join(filtros)


def pcr_consulta_relacional(tabla_cfg):
    columnas = tabla_cfg.get('columnas', [])

    if not columnas:
        raise ValueError(
            'PCR requiere columnas explicitas: '
            + normalizar_tabla_relativa(tabla_cfg)
        )

    indice = pcr_indice_tablas()
    alias = 'T0'

    proyeccion = ', '.join(
        alias + '.' + pcr_identificador(columna)
        for columna in columnas
    )

    tabla_oracle = (
        pcr_identificador(tabla_cfg['schema'])
        + '.'
        + pcr_identificador(tabla_cfg['tabla'])
    )

    filtro = pcr_filtro_relacional(
        tabla_cfg,
        alias,
        indice
    )

    query = (
        'SELECT DISTINCT '
        + proyeccion
        + ' FROM '
        + tabla_oracle
        + ' '
        + alias
    )

    if filtro:
        query += ' WHERE ' + filtro

    if len(query) > 200000:
        raise ValueError(
            'Consulta relacional supera 200000 caracteres: '
            + normalizar_tabla_relativa(tabla_cfg)
        )

    return query


def pcr_leer(tabla_cfg, opciones):
    query = pcr_consulta_relacional(tabla_cfg)

    logger.info(
        'PCR CONSULTA RELACIONAL %s: %s',
        normalizar_tabla_relativa(tabla_cfg),
        query
    )

    opciones_jdbc = dict(opciones)
    opciones_jdbc['sampleQuery'] = query

    dyf = glueContext.create_dynamic_frame.from_options(
        connection_type='oracle',
        connection_options=opciones_jdbc
    )

    columnas = tabla_cfg.get('columnas', [])

    return (
        dyf.toDF()
        .select(*columnas)
        .dropDuplicates()
    )

def pcr_registrar(tabla_cfg, df):
    from pyspark.sql import functions as F
    relativa = normalizar_tabla_relativa(tabla_cfg)
    if relativa in pcr_datasets:
        anterior = pcr_datasets[relativa]
        acumulado = anterior.unionByName(df).persist(StorageLevel.MEMORY_AND_DISK)
        acumulado.count()
        anterior.unpersist()
    else:
        acumulado = df.persist(StorageLevel.MEMORY_AND_DISK)
        acumulado.count()
    pcr_datasets[relativa] = acumulado
    salidas = pcr_llaves_salida.get(relativa, [])
    rutas = []
    for i, columnas in enumerate(salidas):
        ruta = pcr_ruta_llaves + relativa + '/mapeo_' + str(i) + '/'
        llaves = acumulado.select(*columnas).dropDuplicates()
        llaves.write.mode('overwrite').parquet(ruta)
        rutas.append({'columnas': columnas, 'ruta': ruta})
    parametros_extraccion.setdefault('dependencias_pcr', {})[relativa] = rutas
    return acumulado


def pcr_restaurar_tabla_confirmada(tabla_cfg):
    """Solo recuperacion PCR: valida commits y Bronze; no reescribe salidas."""
    relativa = normalizar_tabla_relativa(tabla_cfg)
    registros = [d for d in parametros_extraccion.get('datasets_extraidos', [])
                 if d.get('tabla_relativa', '').lower() == relativa and d.get('estado') == 'SUCCESS']
    if len(registros) != 1:
        raise ValueError('Registro confirmado ausente o ambiguo: ' + relativa)
    registro = registros[0]
    esperado = registro.get('total_registros')
    if type(esperado) is not int or esperado < 0:
        raise ValueError('Conteo confirmado invalido: ' + relativa)
    if registro.get('lote_cargue_id_dataset') != timestamp_ingesta:
        raise ValueError('Lote confirmado incompatible: ' + relativa)
    # pcr_leer reconstruye las consultas y valida plan, commits y archivos.
    opciones = {'connectionName': connection_name, 'useConnectionProperties': 'true',
                'dbtable': construir_dbtable(tabla_cfg['schema'], tabla_cfg['tabla'], usar_schema_en_dbtable),
                'oracle.net.CONNECT_TIMEOUT': '30000', 'oracle.jdbc.ReadTimeout': '30000',
                'fetchsize': fetchsize}
    # Exigir cierre antes de llamar al lector: nunca extraer nuevos bloques aqui.
    root = (
        'niif/pcr-vida/checkpoints/' +
        hashlib.sha256(relativa.encode()).hexdigest()[:16] + '/' +
        pcr_id + '/'
    )
    store = S3Store(boto3.client('s3'), 'alfa-datalake-dev-assets')
    plan = store.get(root + 'plan.json')
    cierre = store.get(root + 'complete.json')
    if plan is None or cierre is None:
        raise ValueError('Tabla SUCCESS sin plan/cierre de bloques: ' + relativa)
    for seg in plan['plan']['segments']:
        if store.get(root + 'commits/' + seg['id'] + '.json') is None:
            raise ValueError('Tabla SUCCESS con commit ausente: ' + relativa)
    df = pcr_leer(tabla_cfg, opciones).persist(StorageLevel.MEMORY_AND_DISK)
    if df.count() != esperado:
        raise ValueError('Conteo de commits distinto del runtime: ' + relativa)
    if esperado:
        ruta = s3_bronze_base + construir_ruta_catalogable(tabla_cfg, sistema_origen) + '/'
        if registro.get('ruta_dataset_origen') != ruta:
            raise ValueError('Ruta Bronze incompatible: ' + relativa)
        bronze = spark.read.parquet(ruta)
        from pyspark.sql import functions as F
        resumen = bronze.agg(F.count(F.lit(1)).alias('n'),
                            F.count('LOTE_CARGUE_ID').alias('nl'),
                            F.min('LOTE_CARGUE_ID').alias('lo'),
                            F.max('LOTE_CARGUE_ID').alias('hi')).first()
        if (resumen['n'] != esperado or resumen['nl'] != esperado or
                resumen['lo'] != timestamp_ingesta or resumen['hi'] != timestamp_ingesta):
            raise ValueError('Bronze incompleto o lote incompatible: ' + relativa)
    pcr_datasets[relativa] = df
    logger.info('PCR TABLA RESTAURADA SIN REESCRITURA: %s filas=%s', relativa, esperado)

def construir_dbtable(schema, tabla, usar_schema_en_dbtable=True):
    return f'{schema}.{tabla}' if usar_schema_en_dbtable else tabla

def construir_query_where(tabla_cfg, usar_schema_en_dbtable=True):
    dbtable = construir_dbtable(schema=tabla_cfg['schema'], tabla=tabla_cfg['tabla'], usar_schema_en_dbtable=usar_schema_en_dbtable)
    where = tabla_cfg.get('where')
    query = f'SELECT * FROM {dbtable}'
    return f'{query} WHERE {where}' if where else query

def normalizar_tabla_relativa(tabla_cfg):
    schema = tabla_cfg['schema']
    tabla = tabla_cfg['tabla']
    return tabla_cfg.get('tabla_relativa', f'{schema.lower()}/{tabla.lower()}').lower().strip('/')

def construir_ruta_catalogable(tabla_cfg, sistema_origen):
    sistema = normalizar_texto(sistema_origen)
    schema = normalizar_texto(tabla_cfg['schema'])
    tabla = normalizar_texto(tabla_cfg['tabla'])
    return f'{schema}/{sistema}_{schema}_{tabla}'

def construir_nombre_tabla_catalogo(tabla_cfg, sistema_origen):
    sistema = normalizar_texto(sistema_origen)
    schema = normalizar_texto(tabla_cfg['schema'])
    tabla = normalizar_texto(tabla_cfg['tabla'])
    return f'{sistema}_{schema}_{tabla}'

def puede_reutilizar_snapshot(tabla_cfg):
    reutilizacion = tabla_cfg.get('reutilizacion_compartida', {}) or {}
    return bool(reutilizacion.get('habilitada', False)) and (not bool(tabla_cfg.get('segmentacion_horizontal', {}).get('habilitada', False)))

def obtener_lote_snapshot_vigente(spark, ruta):
    if not existe_prefijo_s3(ruta):
        return (None, None)
    df = spark.read.parquet(ensure_trailing_slash(ruta))
    columnas = {c.lower(): c for c in df.columns}
    if 'lote_cargue_id' not in columnas:
        return (None, None)
    lote_col = columnas['lote_cargue_id']
    lote = df.selectExpr(f'max(cast(`{lote_col}` as bigint)) as lote_vigente').first()['lote_vigente']
    if lote is None:
        return (None, None)
    total = df.filter(df[lote_col].cast('bigint') == int(lote)).count()
    return (int(lote), int(total))

def upsert_dataset_extraido(parametros_extraccion, registro_dataset):
    tabla_relativa = registro_dataset['tabla_relativa'].lower()
    datasets = parametros_extraccion.get('datasets_extraidos', [])
    datasets = [d for d in datasets if d.get('tabla_relativa', '').lower() != tabla_relativa]
    datasets.append(registro_dataset)
    parametros_extraccion['datasets_extraidos'] = datasets

def crear_checkpoint_base(guardar_detalle_eventos=False):
    checkpoint = {'estado': 'IN_PROGRESS', 'ciclo_actual': None, 'segmento_actual': None, 'tabla_actual': None, 'ultimo_ciclo_ok': None, 'ultimo_segmento_ok': None, 'ultima_tabla_ok': None, 'tablas_exitosas': []}
    if guardar_detalle_eventos:
        checkpoint['detalle'] = []
    return checkpoint

def actualizar_checkpoint(parametros_extraccion, estado, guardar_detalle_eventos=False, ciclo=None, segmento=None, tabla=None, mensaje=None):
    checkpoint = parametros_extraccion.get('checkpoint_extraccion', crear_checkpoint_base(guardar_detalle_eventos))
    checkpoint['estado'] = estado
    if ciclo:
        checkpoint['ciclo_actual'] = {'orden': ciclo.get('orden'), 'nombre': ciclo.get('nombre')}
    if segmento:
        checkpoint['segmento_actual'] = {'orden': segmento.get('orden'), 'nombre': segmento.get('nombre'), 'forma': segmento.get('forma')}
    if tabla:
        checkpoint['tabla_actual'] = {'orden': tabla.get('orden'), 'schema': tabla.get('schema'), 'tabla': tabla.get('tabla'), 'tabla_relativa': normalizar_tabla_relativa(tabla)}
    if guardar_detalle_eventos and mensaje:
        checkpoint.setdefault('detalle', []).append({'fecha': ahora_str(), 'estado': estado, 'mensaje': mensaje})
    parametros_extraccion['checkpoint_extraccion'] = checkpoint

def registrar_ok_tabla(parametros_extraccion, tabla_cfg, guardar_detalle_eventos):
    checkpoint = parametros_extraccion.get('checkpoint_extraccion', crear_checkpoint_base(guardar_detalle_eventos))
    tabla_relativa = normalizar_tabla_relativa(tabla_cfg)
    checkpoint['ultima_tabla_ok'] = {'orden': tabla_cfg.get('orden'), 'schema': tabla_cfg.get('schema'), 'tabla': tabla_cfg.get('tabla'), 'tabla_relativa': tabla_relativa}
    tablas_exitosas = set(checkpoint.get('tablas_exitosas', []))
    tablas_exitosas.add(tabla_relativa)
    checkpoint['tablas_exitosas'] = sorted(list(tablas_exitosas))
    parametros_extraccion['checkpoint_extraccion'] = checkpoint

def registrar_ok_segmento(parametros_extraccion, segmento, guardar_detalle_eventos):
    checkpoint = parametros_extraccion.get('checkpoint_extraccion', crear_checkpoint_base(guardar_detalle_eventos))
    checkpoint['ultimo_segmento_ok'] = {'orden': segmento.get('orden'), 'nombre': segmento.get('nombre'), 'forma': segmento.get('forma')}
    parametros_extraccion['checkpoint_extraccion'] = checkpoint

def registrar_ok_ciclo(parametros_extraccion, ciclo, guardar_detalle_eventos):
    checkpoint = parametros_extraccion.get('checkpoint_extraccion', crear_checkpoint_base(guardar_detalle_eventos))
    checkpoint['ultimo_ciclo_ok'] = {'orden': ciclo.get('orden'), 'nombre': ciclo.get('nombre')}
    parametros_extraccion['checkpoint_extraccion'] = checkpoint

def obtener_tablas_exitosas(parametros_extraccion):
    tablas = set()
    for item in parametros_extraccion.get('datasets_extraidos', []):
        if item.get('estado') == 'SUCCESS' and item.get('tabla_relativa'):
            tablas.add(item['tabla_relativa'].lower())
    checkpoint = parametros_extraccion.get('checkpoint_extraccion', {})
    for tabla in checkpoint.get('tablas_exitosas', []):
        tablas.add(tabla.lower())
    return tablas

def construir_segmento_id(sistema_origen, periodo_negocio, tabla_cfg, segmentacion):
    segmento_id_cfg = segmentacion.get('segmento_id')
    if segmento_id_cfg:
        return normalizar_texto(segmento_id_cfg)
    periodo = str(periodo_negocio) if periodo_negocio is not None else 'sin_periodo'
    sistema = normalizar_texto(sistema_origen)
    schema = normalizar_texto(tabla_cfg['schema'])
    tabla = normalizar_texto(tabla_cfg['tabla'])
    return f'niif17_{periodo}_{sistema}_{schema}_{tabla}'

def construir_metadata_segmentacion(nombre_caso, sistema_origen, periodo_negocio, tabla_cfg, lote_actual):
    tabla_relativa = normalizar_tabla_relativa(tabla_cfg)
    segmentacion = tabla_cfg.get('segmentacion_horizontal', {}) or {}
    habilitada = bool(segmentacion.get('habilitada', False))
    segmento_id = construir_segmento_id(sistema_origen=sistema_origen, periodo_negocio=periodo_negocio, tabla_cfg=tabla_cfg, segmentacion=segmentacion)
    lotes_segmento = segmentacion.get('lotes_segmento') or [lote_actual]
    if lote_actual not in lotes_segmento:
        lotes_segmento.append(lote_actual)
    return {'habilitada': habilitada, 'segmento_id': segmento_id, 'id_negocio': segmentacion.get('id_negocio', nombre_caso), 'periodo_negocio': segmentacion.get('periodo_negocio', periodo_negocio), 'lote_actual': lote_actual, 'lotes_segmento': sorted(list(set(lotes_segmento))), 'llaves_segmentacion': segmentacion.get('llaves_segmentacion', []), 'modo_segmentacion': segmentacion.get('modo', 'NINGUNO'), 'requiere_preparacion_con_lotes_segmento': bool(segmentacion.get('requiere_preparacion_con_lotes_segmento', habilitada)), 'ruta_llaves_segmento_base': None, 'exclusion_aplicada': False, 'registros_excluidos_por_segmento': 0, 'observacion': 'Metadata de segmentación registrada.' if habilitada else None}

def construir_ruta_llaves_segmento_base(ruta_segmentos_base, segmento_id):
    segmento_id_norm = normalizar_texto(segmento_id)
    return f'{ensure_trailing_slash(ruta_segmentos_base)}{segmento_id_norm}/llaves/'

def construir_ruta_llaves_segmento(ruta_segmentos_base, segmento_id, lote_cargue_id):
    return f'{construir_ruta_llaves_segmento_base(ruta_segmentos_base, segmento_id)}lote_cargue_id={lote_cargue_id}/'

def resolver_llaves_reales(df, llaves_segmentacion, tabla_relativa):
    columnas_df = {c.upper(): c for c in df.columns}
    llaves_reales = []
    for llave in llaves_segmentacion:
        llave_upper = llave.upper()
        if llave_upper not in columnas_df:
            raise ValueError(f"La llave de segmentación '{llave}' no existe en {tabla_relativa}. Columnas disponibles: {df.columns}")
        llaves_reales.append(columnas_df[llave_upper])
    return llaves_reales

def aplicar_exclusion_llaves_segmento(spark, df, metadata_segmentacion, tabla_relativa, ruta_segmentos_base, excluir_llaves_segmento):
    metadata = {'exclusion_aplicada': False, 'ruta_llaves_segmento_base': None, 'registros_excluidos_por_segmento': 0}
    if not excluir_llaves_segmento:
        return (df, metadata)
    if not metadata_segmentacion.get('habilitada', False):
        return (df, metadata)
    modo = metadata_segmentacion.get('modo_segmentacion', 'NINGUNO').upper()
    if modo != 'EXCLUIR_EXTRAIDOS':
        return (df, metadata)
    llaves_segmentacion = metadata_segmentacion.get('llaves_segmentacion', [])
    if not llaves_segmentacion:
        raise ValueError(f'{tabla_relativa} tiene modo EXCLUIR_EXTRAIDOS pero no tiene llaves_segmentacion.')
    llaves_reales = resolver_llaves_reales(df=df, llaves_segmentacion=llaves_segmentacion, tabla_relativa=tabla_relativa)
    ruta_llaves_base = construir_ruta_llaves_segmento_base(ruta_segmentos_base, metadata_segmentacion['segmento_id'])
    metadata['ruta_llaves_segmento_base'] = ruta_llaves_base
    if not existe_prefijo_s3(ruta_llaves_base):
        logger.info(f'No existen llaves previas para {tabla_relativa}. No se aplica exclusión: {ruta_llaves_base}')
        return (df, metadata)
    logger.info(f'Leyendo llaves previas: {ruta_llaves_base}')
    df_llaves_previas = spark.read.parquet(ruta_llaves_base).select(*llaves_reales).dropDuplicates(llaves_reales)
    total_excluidos = df.join(df_llaves_previas, on=llaves_reales, how='inner').count()
    df_filtrado = df.join(df_llaves_previas, on=llaves_reales, how='left_anti')
    metadata['exclusion_aplicada'] = True
    metadata['registros_excluidos_por_segmento'] = total_excluidos
    logger.info(f'Exclusión aplicada para {tabla_relativa}. Registros excluidos: {total_excluidos}')
    return (df_filtrado, metadata)

def guardar_llaves_segmentacion_s3(df_brz, metadata_segmentacion, nombre_caso, sistema_origen, tabla_relativa, periodo_negocio, lote_cargue_id, ruta_segmentos_base):
    if not metadata_segmentacion.get('habilitada', False):
        return None
    llaves_segmentacion = metadata_segmentacion.get('llaves_segmentacion', [])
    if not llaves_segmentacion:
        return None
    llaves_reales = resolver_llaves_reales(df=df_brz, llaves_segmentacion=llaves_segmentacion, tabla_relativa=tabla_relativa)
    segmento_id = metadata_segmentacion['segmento_id']
    ruta_llaves_base = construir_ruta_llaves_segmento_base(ruta_segmentos_base, segmento_id)
    ruta_llaves = construir_ruta_llaves_segmento(ruta_segmentos_base, segmento_id, lote_cargue_id)
    metadata_segmentacion['ruta_llaves_segmento_base'] = ruta_llaves_base
    df_llaves = df_brz.select(*llaves_reales).dropDuplicates(llaves_reales).withColumn('segmento_id', lit(segmento_id)).withColumn('nombre_caso', lit(nombre_caso)).withColumn('sistema_origen', lit(sistema_origen)).withColumn('tabla_relativa', lit(tabla_relativa)).withColumn('periodo_negocio', lit(str(periodo_negocio))).withColumn('lote_cargue_id', lit(lote_cargue_id).cast('bigint')).withColumn('fecha_extraccion', lit(ahora_str()))
    columnas_salida = ['segmento_id', 'nombre_caso', 'sistema_origen', 'tabla_relativa', 'periodo_negocio', 'lote_cargue_id', 'fecha_extraccion'] + llaves_reales
    df_llaves = df_llaves.select(*columnas_salida)
    df_llaves.write.mode('overwrite').option('compression', 'snappy').parquet(ruta_llaves)
    total_llaves = df_llaves.count()
    return {'ruta_llaves_segmento_base': ruta_llaves_base, 'ruta_llaves_segmento': ruta_llaves, 'total_llaves_segmento': total_llaves}

def upsert_segmento_preparacion(parametros_extraccion, metadata_segmentacion, tabla_relativa, metadata_llaves_segmento=None):
    segmentos = parametros_extraccion.get('segmentos_preparacion', [])
    segmento_id = metadata_segmentacion['segmento_id']
    id_negocio = metadata_segmentacion.get('id_negocio')
    periodo_negocio = metadata_segmentacion.get('periodo_negocio')
    segmentos = [s for s in segmentos if not (s.get('segmento_id') == segmento_id and s.get('tabla_relativa') == tabla_relativa and (s.get('id_negocio') == id_negocio) and (str(s.get('periodo_negocio')) == str(periodo_negocio)))]
    ruta_llaves_segmento_base = metadata_llaves_segmento.get('ruta_llaves_segmento_base') if metadata_llaves_segmento else metadata_segmentacion.get('ruta_llaves_segmento_base')
    segmentos.append({'segmento_id': segmento_id, 'tabla_relativa': tabla_relativa, 'id_negocio': id_negocio, 'periodo_negocio': periodo_negocio, 'lote_actual': metadata_segmentacion.get('lote_actual'), 'lotes_segmento': metadata_segmentacion.get('lotes_segmento', []), 'llaves_segmentacion': metadata_segmentacion.get('llaves_segmentacion', []), 'requiere_preparacion_con_lotes_segmento': metadata_segmentacion.get('requiere_preparacion_con_lotes_segmento', False), 'ruta_llaves_segmento_base': ruta_llaves_segmento_base, 'ruta_llaves_segmento': metadata_llaves_segmento.get('ruta_llaves_segmento') if metadata_llaves_segmento else None, 'total_llaves_segmento': metadata_llaves_segmento.get('total_llaves_segmento') if metadata_llaves_segmento else None, 'exclusion_aplicada': metadata_segmentacion.get('exclusion_aplicada', False), 'registros_excluidos_por_segmento': metadata_segmentacion.get('registros_excluidos_por_segmento', 0)})
    parametros_extraccion['segmentos_preparacion'] = segmentos
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'RUTA_CONFIG_EXTRACCION'])
ruta_config_extraccion = args['RUTA_CONFIG_EXTRACCION']
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set('spark.sql.parquet.datetimeRebaseModeInWrite', 'LEGACY')
spark.conf.set('spark.sql.parquet.int96RebaseModeInWrite', 'LEGACY')
spark.conf.set('spark.sql.parquet.datetimeRebaseModeInRead', 'LEGACY')
spark.conf.set('spark.sql.parquet.int96RebaseModeInRead', 'LEGACY')
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
spark.conf.set('spark.sql.caseSensitive', 'false')
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
config = cargar_json_s3(ruta_config_extraccion)
nombre_caso = config['nombre_caso']
fuente = config['fuente']
sistema_origen = fuente.get('sistema')
if not sistema_origen:
    raise ValueError('El JSON debe contener fuente.sistema.')
connection_name = fuente['connection_name']
usar_schema_en_dbtable = bool(fuente.get('usar_schema_en_dbtable', True))
fetchsize = str(fuente.get('fetchsize', '10000'))
rutas = config['rutas']
s3_bronze_base = ensure_trailing_slash(rutas['s3_bronze_base'])
s3_silver_base = ensure_trailing_slash(rutas['s3_silver_base'])
ruta_parametros_extraccion = rutas['ruta_parametros_extraccion']
ruta_segmentos_base = rutas.get('ruta_segmentos_base')
if not ruta_segmentos_base:
    raise ValueError('El JSON debe contener rutas.ruta_segmentos_base.')
ruta_segmentos_base = ensure_trailing_slash(ruta_segmentos_base)
control = config.get('control_extraccion', {})
limite_unidades = control.get('limite_unidades_extraccion')
calcular_unidades = control.get('calcular_unidades_extraccion', True)
accion_si_supera_limite = control.get('accion_si_supera_limite', 'ADVERTIR').upper()
permitir_reanudacion = control.get('permitir_reanudacion', True)
usar_checkpoints = control.get('usar_checkpoints', True)
contar_registros = control.get('contar_registros', True)
persistir_dataframe = control.get('persistir_dataframe', True)
excluir_llaves_segmento = bool(control.get('excluir_llaves_segmento', False))
modo_ejecucion = control.get('modo_ejecucion', 'INICIAL').upper()
periodo_negocio = control.get('periodo_negocio') or config.get('periodo_negocio')
if not periodo_negocio:
    raise ValueError('El JSON debe contener periodo_negocio.')
if modo_ejecucion not in ['INICIAL', 'REPROCESO', 'RECARGUE_HISTORICO']:
    raise ValueError(f'modo_ejecucion no soportado: {modo_ejecucion}. Valores válidos: INICIAL, REPROCESO, RECARGUE_HISTORICO')
if accion_si_supera_limite not in ['ADVERTIR', 'FALLAR']:
    raise ValueError(f'accion_si_supera_limite no soportada: {accion_si_supera_limite}.')
salida_runtime = config.get('salida_runtime', {})
estructura_dataset = salida_runtime.get('estructura_dataset', {})
checkpoint_cfg = salida_runtime.get('checkpoint_extraccion', {})
agregar_nro_registro = estructura_dataset.get('agregar_nro_registro', True)
agregar_lote_cargue_id = estructura_dataset.get('agregar_lote_cargue_id', True)
modo_escritura = estructura_dataset.get('modo_escritura', 'overwrite').lower()
compresion = estructura_dataset.get('compresion', 'snappy')
guardar_detalle_eventos = checkpoint_cfg.get('guardar_detalle_eventos', False)
guardar_checkpoint_inicio_tabla = checkpoint_cfg.get('guardar_checkpoint_inicio_tabla', False)
saltar_tablas_exitosas = checkpoint_cfg.get('saltar_tablas_exitosas_en_reanudacion', True)
if modo_escritura not in ['overwrite', 'append']:
    raise ValueError('modo_escritura debe ser overwrite o append.')
pcr_activo = bool(config.get('extension_pcr', {}).get('habilitada', False))
pcr_datasets = {}
pcr_llaves_salida = {}
if pcr_activo:
    if boto3.client('sts').get_caller_identity()['Account'] != '533267035849':
        raise ValueError('Prueba PCR autorizada solo para DEV')
    if not config.get('extension_pcr', {}).get('modo_mensual', False):
        raise ValueError('PCR mensual requiere extension_pcr.modo_mensual=true')
    if not contar_registros or not persistir_dataframe:
        raise ValueError('PCR requiere conteo y persistencia')
    if s3_bronze_base != 's3://alfa-datalake-dev-bronce/niif/ACSELE/':
        raise ValueError('Ruta Bronze PCR Vida no autorizada: ' + s3_bronze_base)
    if ruta_parametros_extraccion != 's3://niif-dev-assets/pcr-vida/parametros_extraccion.json':
        raise ValueError('Runtime PCR Vida no autorizado: ' + ruta_parametros_extraccion)
    if config.get('extension_pcr', {}).get('runtime_unico', False):
        from urllib.parse import urlparse as _vida_urlparse
        _vida_runtime = _vida_urlparse(ruta_parametros_extraccion)
        _vida_bronze = _vida_urlparse(s3_bronze_base)
        _vida_silver = _vida_urlparse(s3_silver_base)
        if _vida_runtime.scheme != 's3' or _vida_runtime.netloc != 'alfa-datalake-dev-assets' or (not _vida_runtime.path.startswith('/pruebas/pcr/vida/ejecuciones/')) or (not _vida_runtime.path.endswith('/parametros_extraccion.json')):
            raise ValueError('Runtime unico fuera de la ruta aislada autorizada')
        _vida_parts = _vida_runtime.path.strip('/').split('/')
        if len(_vida_parts) != 6:
            raise ValueError('Estructura de runtime unica invalida')
        pcr_id = _vida_parts[4]
        if len(pcr_id) != 32 or any((c not in '0123456789abcdef' for c in pcr_id)):
            raise ValueError('Identificador de ejecucion invalido')
        _vida_base = '/pruebas/pcr/vida/ejecuciones/' + pcr_id + '/'
        if _vida_bronze.scheme != 's3' or _vida_bronze.netloc != 'alfa-datalake-dev-bronce' or _vida_bronze.path != _vida_base or (_vida_silver.scheme != 's3') or (_vida_silver.netloc != 'alfa-datalake-dev-silver') or (_vida_silver.path != _vida_base + 'base/'):
            raise ValueError('Bronze/Silver no corresponden al runtime unico')
        import boto3 as _vida_boto3
        from botocore.exceptions import ClientError as _vida_ClientError
        try:
            _vida_boto3.client('s3').head_object(Bucket=_vida_runtime.netloc, Key=_vida_runtime.path.lstrip('/'))
        except _vida_ClientError as _vida_exc:
            if _vida_exc.response['Error']['Code'] not in ('404', 'NoSuchKey', 'NotFound'):
                raise
        else:
            if not config.get('extension_pcr', {}).get('checkpoint_bloques', False):
                raise ValueError('Runtime ya existente; usar nueva ejecucion. Reanudacion pendiente')
            _previous = cargar_json_s3(ruta_parametros_extraccion)
            if _previous.get('estado_extraccion') not in ('FAILED', 'IN_PROGRESS'):
                raise ValueError('Solo recuperar ejecucion fallida/incompleta; no repetir SUCCESS')
            if _previous.get('config_checkpoint_hash') != fingerprint(config):
                raise ValueError('Configuracion cambio; no recuperar')
            _pcr_lote_anterior = int(_previous['lote_cargue_id'])
        logger.info('VIDA runtime unico respetado: %s', ruta_parametros_extraccion)
    else:
        pcr_id = __import__('uuid').uuid4().hex

    pcr_ruta_llaves = (
        's3://alfa-datalake-dev-bronce/niif/_control/'
        'pcr_vida/dependencias/' + pcr_id + '/'
    )
    vistos = set()
    for ciclo_p in sorted(config.get('ciclos', []), key=lambda x: x.get('orden', 0)):
        for segmento_p in sorted(ciclo_p.get('segmentos', []), key=lambda x: x.get('orden', 0)):
            for tabla_p in sorted(segmento_p.get('tablas', []), key=lambda x: x.get('orden', 0)):
                relativa_p = normalizar_tabla_relativa(tabla_p)
                segmentada_p = bool(
                    tabla_p.get(
                        'segmentacion_horizontal',
                        {}
                    ).get('habilitada', False)
                )

                if segmentada_p and tabla_p.get('tipo_tabla') != 'PIVOTE':
                    raise ValueError(
                        'PCR Vida solo permite segmentacion horizontal '
                        'en CONTEXTOPERATION'
                    )

                if puede_reutilizar_snapshot(tabla_p):
                    raise ValueError(
                        'PCR Vida mensual no reutiliza snapshots compartidos'
                    )
                if not tabla_p.get('columnas'):
                    raise ValueError('Tabla PCR sin columnas')
                for dep_p in tabla_p.get('dependencias_lectura', []):
                    padre_p = dep_p['tabla_origen'].lower()
                    if padre_p not in vistos:
                        raise ValueError('Orden de dependencia invalido: ' + padre_p)
                    llave_p = list(dict.fromkeys((x['origen'].upper() for x in dep_p['mapeo'])))
                    pcr_llaves_salida.setdefault(padre_p, [])
                    if llave_p not in pcr_llaves_salida[padre_p]:
                        pcr_llaves_salida[padre_p].append(llave_p)
                vistos.add(relativa_p)
ciclos = sorted(config.get('ciclos', []), key=lambda x: x.get('orden', 0))
if not ciclos:
    raise ValueError('El JSON de extracción no contiene ciclos.')
fecha_inicio_job_dt = ahora_dt()
timestamp_ingesta = globals().get('_pcr_lote_anterior', int(fecha_inicio_job_dt.timestamp()))
fecha_ejecucion = fecha_inicio_job_dt.strftime('%Y-%m-%dT%H:%M:%S')
reanudar_ejecucion = False
parametros_extraccion = None
_pcr_recuperacion_tablas = bool(pcr_activo and globals().get('_previous') is not None)
if _pcr_recuperacion_tablas:
    parametros_extraccion = json.loads(json.dumps(_previous))
    parametros_extraccion['estado_extraccion'] = 'IN_PROGRESS'
    parametros_extraccion['fecha_reanudacion_pcr'] = fecha_ejecucion
    parametros_extraccion.pop('error_extraccion', None)
    parametros_extraccion.pop('fecha_error_extraccion', None)
if permitir_reanudacion and existe_s3(ruta_parametros_extraccion):
    parametros_existentes = cargar_json_s3(ruta_parametros_extraccion)
    estado_anterior = parametros_existentes.get('estado_extraccion')
    if estado_anterior in ['FAILED', 'IN_PROGRESS']:
        parametros_extraccion = parametros_existentes
        timestamp_ingesta = parametros_extraccion['lote_cargue_id']
        reanudar_ejecucion = True
        parametros_extraccion['estado_extraccion'] = 'IN_PROGRESS'
        parametros_extraccion['fecha_reanudacion'] = fecha_ejecucion
if parametros_extraccion is None:
    parametros_extraccion = {'execution_id': f'bronze_{nombre_caso}_{timestamp_ingesta}', 'nombre_caso': nombre_caso, 'sistema_origen': sistema_origen, 'periodo_negocio': periodo_negocio, 'modo_ejecucion': modo_ejecucion, 'lote_cargue_id': timestamp_ingesta, 'fecha_ejecucion': fecha_ejecucion, 'estado_extraccion': 'IN_PROGRESS', 'ruta_origen_base': s3_bronze_base, 'ruta_destino_base': s3_silver_base, 'ruta_config_extraccion': ruta_config_extraccion, 'datasets_extraidos': [], 'segmentos_preparacion': [], 'checkpoint_extraccion': crear_checkpoint_base(guardar_detalle_eventos), 'reanudacion': {'habilitada': permitir_reanudacion, 'es_reanudacion': False}}
else:
    parametros_extraccion.setdefault('datasets_extraidos', [])
    parametros_extraccion.setdefault('segmentos_preparacion', [])
    parametros_extraccion.setdefault('checkpoint_extraccion', crear_checkpoint_base(guardar_detalle_eventos))
    parametros_extraccion['sistema_origen'] = sistema_origen
    parametros_extraccion['periodo_negocio'] = periodo_negocio
    parametros_extraccion['modo_ejecucion'] = modo_ejecucion
    parametros_extraccion['reanudacion'] = {'habilitada': permitir_reanudacion, 'es_reanudacion': reanudar_ejecucion, 'fecha_reanudacion': fecha_ejecucion if reanudar_ejecucion else None}
logger.info('=== INICIO JOB EXTRACCION ESTANDAR NIIF ===')
logger.info(f'Nombre caso            : {nombre_caso}')
logger.info(f'Sistema origen         : {sistema_origen}')
logger.info(f'Periodo negocio        : {periodo_negocio}')
logger.info(f'Modo ejecución         : {modo_ejecucion}')
logger.info(f'Excluir llaves         : {excluir_llaves_segmento}')
logger.info(f'Lote cargue id         : {timestamp_ingesta}')
logger.info(f'Ruta config            : {ruta_config_extraccion}')
logger.info(f'Ruta Bronze base       : {s3_bronze_base}')
logger.info(f'Ruta segmentos base    : {ruta_segmentos_base}')
logger.info(f'Es reanudación         : {reanudar_ejecucion}')
parametros_extraccion['config_checkpoint_hash'] = fingerprint(config)
parametros_extraccion['recuperacion_pcr'] = 'Restaura tablas SUCCESS validando commits y Bronze; procesa pendientes por bloques'
guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)
try:
    tablas_exitosas = obtener_tablas_exitosas(parametros_extraccion)
    for ciclo in ciclos:
        if usar_checkpoints:
            actualizar_checkpoint(parametros_extraccion, estado='CICLO_IN_PROGRESS', guardar_detalle_eventos=guardar_detalle_eventos, ciclo=ciclo, mensaje=f"Iniciando ciclo {ciclo.get('nombre')}")
        segmentos = sorted(ciclo.get('segmentos', []), key=lambda x: x.get('orden', 0))
        if not segmentos:
            raise ValueError(f"El ciclo {ciclo.get('nombre')} no contiene segmentos.")
        for segmento in segmentos:
            forma = segmento.get('forma', 'CASCADA').upper()
            if forma != 'CASCADA':
                logger.warning(f'Forma {forma} configurada, pero esta versión ejecuta todo en CASCADA.')
            if usar_checkpoints:
                actualizar_checkpoint(parametros_extraccion, estado='SEGMENTO_IN_PROGRESS', guardar_detalle_eventos=guardar_detalle_eventos, ciclo=ciclo, segmento=segmento, mensaje=f"Iniciando segmento {segmento.get('nombre')}")
            tablas = sorted(segmento.get('tablas', []), key=lambda x: x.get('orden', 0))
            if not tablas:
                raise ValueError(f"El segmento {segmento.get('nombre')} no contiene tablas.")
            for tabla_cfg in tablas:
                schema = tabla_cfg['schema']
                tabla = tabla_cfg['tabla']
                tabla_relativa = normalizar_tabla_relativa(tabla_cfg)
                dependencia = tabla_cfg.get('dependencia')
                if (_pcr_recuperacion_tablas and tabla_relativa in tablas_exitosas
                        and tabla_cfg.get('dependencias_lectura')):
                    pcr_restaurar_tabla_confirmada(tabla_cfg)
                    continue
                if reanudar_ejecucion and saltar_tablas_exitosas and (tabla_relativa in tablas_exitosas):
                    logger.info(f'Saltando tabla ya extraída: {tabla_relativa}')
                    continue
                logger.info(f'===== INICIANDO EXTRACCION {schema}.{tabla} =====')
                fecha_inicio_tabla_dt = ahora_dt()
                fecha_inicio_tabla = fecha_inicio_tabla_dt.strftime('%Y-%m-%dT%H:%M:%S')
                metadata_segmentacion = construir_metadata_segmentacion(nombre_caso=nombre_caso, sistema_origen=sistema_origen, periodo_negocio=periodo_negocio, tabla_cfg=tabla_cfg, lote_actual=timestamp_ingesta)
                if usar_checkpoints:
                    actualizar_checkpoint(parametros_extraccion, estado='TABLA_IN_PROGRESS', guardar_detalle_eventos=guardar_detalle_eventos, ciclo=ciclo, segmento=segmento, tabla=tabla_cfg, mensaje=f'Extrayendo {schema}.{tabla}')
                    if guardar_checkpoint_inicio_tabla:
                        guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)
                ruta_catalogable = construir_ruta_catalogable(tabla_cfg, sistema_origen)
                tabla_catalogo = construir_nombre_tabla_catalogo(tabla_cfg, sistema_origen)
                target_path = f'{s3_bronze_base}{ruta_catalogable}/'
                silver_target_path = f'{s3_silver_base}{ruta_catalogable}/'
                if puede_reutilizar_snapshot(tabla_cfg):
                    lote_reutilizado, total_reutilizado = obtener_lote_snapshot_vigente(spark, target_path)
                    if lote_reutilizado is not None:
                        logger.info(f'REUTILIZANDO SNAPSHOT {tabla_relativa}. lote={lote_reutilizado}, registros={total_reutilizado}')
                        registro_dataset = {'estado': 'SUCCESS', 'sistema_origen': sistema_origen.lower(), 'esquema': schema.lower(), 'tabla': tabla.lower(), 'tabla_relativa': tabla_relativa, 'tabla_catalogo': tabla_catalogo, 'ruta_catalogable': ruta_catalogable, 'ruta_dataset_origen': target_path, 'ruta_dataset_destino': silver_target_path, 'tipo_tabla': tabla_cfg.get('tipo_tabla'), 'segmentacion_horizontal': metadata_segmentacion, 'segmentacion_horizontal_habilitada': False, 'escritura_bronze': 'REUTILIZADA', 'modo_disponibilidad': 'REUTILIZADA_COMPARTIDA', 'lote_cargue_id_dataset': lote_reutilizado, 'lotes_preparacion_silver': [lote_reutilizado], 'datos_nuevos': False, 'datos_nuevos_para_flujo': False, 'requiere_validacion_tecnica': False, 'requiere_disponibilizacion_silver': False, 'requiere_preparacion_silver': True, 'total_registros': total_reutilizado, 'fecha_inicio_extraccion': ahora_str(), 'fecha_fin_extraccion': ahora_str(), 'observacion': 'Snapshot compartido reutilizado.'}
                        upsert_dataset_extraido(parametros_extraccion, registro_dataset)
                        parametros_extraccion.setdefault('lotes_usados_preparacion_silver', {})[tabla_relativa] = [lote_reutilizado]
                        if usar_checkpoints:
                            registrar_ok_tabla(parametros_extraccion, tabla_cfg, guardar_detalle_eventos)
                        guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)
                        tablas_exitosas.add(tabla_relativa)
                        continue
                dbtable_name = construir_dbtable(schema=schema, tabla=tabla, usar_schema_en_dbtable=usar_schema_en_dbtable)
                where = tabla_cfg.get('where')
                connection_options = {'connectionName': connection_name, 'useConnectionProperties': 'true', 'dbtable': dbtable_name, 'oracle.net.CONNECT_TIMEOUT': '30000', 'oracle.jdbc.ReadTimeout': '30000', 'fetchsize': fetchsize}
                fecha_inicio_lectura_dt = ahora_dt()
                if pcr_activo:
                    # Consulta relacional consolidada.
                    # La propagacion ocurre despues de la exclusion historica.
                    df = pcr_leer(tabla_cfg, connection_options)
                    df = df.dropDuplicates()
                else:
                    if where:
                        query = construir_query_where(tabla_cfg=tabla_cfg, usar_schema_en_dbtable=usar_schema_en_dbtable)
                        connection_options['sampleQuery'] = query
                        logger.info(f'Query: {query}')
                    fecha_inicio_lectura_dt = ahora_dt()
                    dyf = _vida_pivote_guardado(glueContext.create_dynamic_frame.from_options, connection_type='oracle', connection_options=connection_options)
                    df = dyf.toDF()
                metadata_exclusion = {'exclusion_aplicada': False, 'ruta_llaves_segmento_base': None, 'registros_excluidos_por_segmento': 0}
                df, metadata_exclusion = aplicar_exclusion_llaves_segmento(spark=spark, df=df, metadata_segmentacion=metadata_segmentacion, tabla_relativa=tabla_relativa, ruta_segmentos_base=ruta_segmentos_base, excluir_llaves_segmento=excluir_llaves_segmento)
                metadata_segmentacion['exclusion_aplicada'] = metadata_exclusion['exclusion_aplicada']
                metadata_segmentacion['ruta_llaves_segmento_base'] = metadata_exclusion['ruta_llaves_segmento_base']
                metadata_segmentacion['registros_excluidos_por_segmento'] = metadata_exclusion['registros_excluidos_por_segmento']

                # Las dependencias se resuelven mediante consultas
                # relacionales consolidadas directamente en Oracle.
                fecha_fin_lectura_dt = ahora_dt()
                duracion_lectura_segundos = segundos_entre(fecha_inicio_lectura_dt, fecha_fin_lectura_dt)
                if persistir_dataframe and contar_registros:
                    df = df.persist(StorageLevel.MEMORY_AND_DISK)
                total_columnas = len(df.columns)
                fecha_inicio_count_dt = ahora_dt()
                total_registros = df.count() if contar_registros else None
                fecha_fin_count_dt = ahora_dt()
                datos_nuevos = total_registros is None or total_registros > 0
                datos_nuevos_para_flujo = datos_nuevos
                motivo_sin_datos_nuevos = None
                if metadata_segmentacion.get('exclusion_aplicada') and total_registros == 0:
                    ruta_llaves_base = metadata_segmentacion.get('ruta_llaves_segmento_base')
                    lotes_previos_segmento = obtener_lotes_desde_ruta_llaves(ruta_llaves_base)
                    if lotes_previos_segmento:
                        metadata_segmentacion['lotes_segmento'] = lotes_previos_segmento
                    datos_nuevos = False
                    datos_nuevos_para_flujo = False
                    motivo_sin_datos_nuevos = 'EXCLUSION_DEJO_DATASET_SIN_REGISTROS_NUEVOS'
                    logger.info(f"{tabla_relativa} sin datos nuevos por exclusión. Lotes segmento vigentes: {metadata_segmentacion.get('lotes_segmento')}")
                if excluir_llaves_segmento and (not metadata_segmentacion.get('habilitada', False)):
                    datos_nuevos_para_flujo = False
                duracion_count_segundos = segundos_entre(fecha_inicio_count_dt, fecha_fin_count_dt)
                unidades_extraccion = total_registros * total_columnas if calcular_unidades and total_registros is not None else None
                supera_limite_unidades = False
                if limite_unidades is not None and unidades_extraccion is not None and (unidades_extraccion > int(limite_unidades)):
                    supera_limite_unidades = True
                    mensaje_limite = f'{schema}.{tabla} supera límite de unidades ({unidades_extraccion} > {limite_unidades}).'
                    if accion_si_supera_limite == 'FALLAR':
                        raise ValueError(f'{mensaje_limite} accion_si_supera_limite=FALLAR')
                    logger.warning(mensaje_limite)
                df_brz = df
                if agregar_nro_registro:
                    df_brz = df_brz.withColumn('NRO_REGISTRO', monotonically_increasing_id().cast('bigint'))
                if agregar_lote_cargue_id:
                    df_brz = df_brz.withColumn('LOTE_CARGUE_ID', lit(timestamp_ingesta).cast('bigint'))
                fecha_inicio_write_dt = ahora_dt()
                metadata_llaves_segmento = None
                escritura_bronze = 'SKIPPED'
                if total_registros is None or total_registros > 0:
                    modo_escritura_real = 'append' if metadata_segmentacion.get('exclusion_aplicada') else modo_escritura
                    logger.info(f'Escribiendo Bronze: {target_path} modo={modo_escritura_real}')
                    df_brz.write.mode(modo_escritura_real).option('compression', compresion).parquet(target_path)
                    escritura_bronze = 'SUCCESS'
                    if metadata_segmentacion['habilitada']:
                        metadata_llaves_segmento = guardar_llaves_segmentacion_s3(df_brz=df_brz, metadata_segmentacion=metadata_segmentacion, nombre_caso=nombre_caso, sistema_origen=sistema_origen, tabla_relativa=tabla_relativa, periodo_negocio=periodo_negocio, lote_cargue_id=timestamp_ingesta, ruta_segmentos_base=ruta_segmentos_base)
                else:
                    logger.info(f'{tabla_relativa} quedó con 0 registros después de exclusión. No se sobrescribe Bronze.')
                fecha_fin_write_dt = ahora_dt()
                duracion_write_segundos = segundos_entre(fecha_inicio_write_dt, fecha_fin_write_dt)
                if persistir_dataframe and contar_registros and (not pcr_activo):
                    df.unpersist()
                fecha_fin_tabla_dt = ahora_dt()
                fecha_fin_tabla = fecha_fin_tabla_dt.strftime('%Y-%m-%dT%H:%M:%S')
                duracion_tabla_segundos = segundos_entre(fecha_inicio_tabla_dt, fecha_fin_tabla_dt)
                dependencia_resumen = None
                if dependencia:
                    dependencia_resumen = {'tabla_origen': dependencia.get('tabla_origen'), 'llaves': dependencia.get('llaves', [])}
                registro_dataset = {'estado': 'SUCCESS', 'sistema_origen': sistema_origen.lower(), 'esquema': schema.lower(), 'tabla': tabla.lower(), 'tabla_relativa': tabla_relativa, 'tabla_catalogo': tabla_catalogo, 'ruta_catalogable': ruta_catalogable, 'ruta_dataset_origen': target_path, 'ruta_dataset_destino': silver_target_path, 'tipo_tabla': tabla_cfg.get('tipo_tabla'), 'ciclo_orden': ciclo.get('orden'), 'ciclo_nombre': ciclo.get('nombre'), 'segmento_orden': segmento.get('orden'), 'segmento_nombre': segmento.get('nombre'), 'forma': forma, 'tabla_orden': tabla_cfg.get('orden'), 'dependencia': dependencia_resumen, 'segmentacion_horizontal': metadata_segmentacion, 'segmentacion_horizontal_habilitada': metadata_segmentacion['habilitada'], 'segmento_id': metadata_segmentacion['segmento_id'], 'id_negocio': metadata_segmentacion.get('id_negocio'), 'periodo_negocio_segmento': metadata_segmentacion.get('periodo_negocio'), 'lotes_segmento': metadata_segmentacion['lotes_segmento'], 'llaves_segmentacion': metadata_segmentacion['llaves_segmentacion'], 'ruta_llaves_segmento': metadata_llaves_segmento.get('ruta_llaves_segmento') if metadata_llaves_segmento else None, 'total_llaves_segmento': metadata_llaves_segmento.get('total_llaves_segmento') if metadata_llaves_segmento else None, 'ruta_llaves_segmento_base': metadata_llaves_segmento.get('ruta_llaves_segmento_base') if metadata_llaves_segmento else metadata_segmentacion.get('ruta_llaves_segmento_base'), 'exclusion_aplicada': metadata_segmentacion.get('exclusion_aplicada', False), 'registros_excluidos_por_segmento': metadata_segmentacion.get('registros_excluidos_por_segmento', 0), 'escritura_bronze': escritura_bronze, 'modo_disponibilidad': 'EXTRAIDA', 'lote_cargue_id_dataset': timestamp_ingesta, 'lotes_preparacion_silver': metadata_segmentacion.get('lotes_segmento', []) if metadata_segmentacion.get('habilitada', False) else [timestamp_ingesta], 'datos_nuevos': datos_nuevos, 'datos_nuevos_para_flujo': datos_nuevos_para_flujo, 'motivo_sin_datos_nuevos': motivo_sin_datos_nuevos, 'requiere_validacion_tecnica': datos_nuevos, 'requiere_disponibilizacion_silver': datos_nuevos, 'requiere_preparacion_silver': True, 'modo_ejecucion': modo_ejecucion, 'modo_extraccion_tabla': modo_ejecucion, 'total_registros': total_registros, 'total_columnas': total_columnas, 'unidades_extraccion': unidades_extraccion, 'limite_unidades_extraccion': limite_unidades, 'supera_limite_unidades': supera_limite_unidades, 'accion_si_supera_limite': accion_si_supera_limite, 'fecha_inicio_extraccion': fecha_inicio_tabla, 'fecha_fin_extraccion': fecha_fin_tabla, 'duracion_segundos': duracion_tabla_segundos, 'duracion_lectura_segundos': duracion_lectura_segundos, 'duracion_count_segundos': duracion_count_segundos if contar_registros else None, 'duracion_write_segundos': duracion_write_segundos, 'dbtable_oracle': dbtable_name, 'usar_schema_en_dbtable': usar_schema_en_dbtable}
                upsert_dataset_extraido(parametros_extraccion, registro_dataset)
                parametros_extraccion.setdefault('lotes_usados_preparacion_silver', {})[tabla_relativa] = registro_dataset.get('lotes_preparacion_silver', [timestamp_ingesta])
                if metadata_segmentacion['habilitada']:
                    upsert_segmento_preparacion(parametros_extraccion=parametros_extraccion, metadata_segmentacion=metadata_segmentacion, tabla_relativa=tabla_relativa, metadata_llaves_segmento=metadata_llaves_segmento)
                if usar_checkpoints:
                    registrar_ok_tabla(parametros_extraccion, tabla_cfg, guardar_detalle_eventos)
                    actualizar_checkpoint(parametros_extraccion, estado='TABLA_SUCCESS', guardar_detalle_eventos=guardar_detalle_eventos, ciclo=ciclo, segmento=segmento, tabla=tabla_cfg, mensaje=f'OK {schema}.{tabla}')
                guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)
                tablas_exitosas.add(tabla_relativa)
                logger.info(f'===== OK {schema}.{tabla} =====')
            registrar_ok_segmento(parametros_extraccion, segmento, guardar_detalle_eventos)
            if usar_checkpoints:
                actualizar_checkpoint(parametros_extraccion, estado='SEGMENTO_SUCCESS', guardar_detalle_eventos=guardar_detalle_eventos, ciclo=ciclo, segmento=segmento, mensaje=f"OK segmento {segmento.get('nombre')}")
        registrar_ok_ciclo(parametros_extraccion, ciclo, guardar_detalle_eventos)
        if usar_checkpoints:
            actualizar_checkpoint(parametros_extraccion, estado='CICLO_SUCCESS', guardar_detalle_eventos=guardar_detalle_eventos, ciclo=ciclo, mensaje=f"OK ciclo {ciclo.get('nombre')}")
except Exception as e:
    logger.error('ERROR EN EXTRACCION ESTANDAR', exc_info=True)
    parametros_extraccion['estado_extraccion'] = 'FAILED'
    parametros_extraccion['error_extraccion'] = str(e)
    parametros_extraccion['fecha_error_extraccion'] = ahora_str()
    if usar_checkpoints:
        actualizar_checkpoint(parametros_extraccion, estado='FAILED', guardar_detalle_eventos=guardar_detalle_eventos, mensaje=str(e))
    guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)
    job.commit()
    raise
fecha_fin_job_dt = ahora_dt()
fecha_fin_extraccion = fecha_fin_job_dt.strftime('%Y-%m-%dT%H:%M:%S')
duracion_total_job = segundos_entre(fecha_inicio_job_dt, fecha_fin_job_dt)
datasets_extraidos = parametros_extraccion.get('datasets_extraidos', [])
total_registros_extraidos = 0
total_unidades_extraccion = 0
total_datasets_con_conteo = 0
datasets_con_datos_nuevos = []
datasets_sin_datos_nuevos = []
datasets_con_datos_nuevos_para_flujo = []
for dataset in datasets_extraidos:
    total_registros_dataset = dataset.get('total_registros')
    unidades_dataset = dataset.get('unidades_extraccion')
    if total_registros_dataset is not None:
        total_registros_extraidos += int(total_registros_dataset)
        total_datasets_con_conteo += 1
    if unidades_dataset is not None:
        total_unidades_extraccion += int(unidades_dataset)
    if dataset.get('datos_nuevos') is True:
        datasets_con_datos_nuevos.append(dataset.get('tabla_relativa'))
    else:
        datasets_sin_datos_nuevos.append(dataset.get('tabla_relativa'))
    if dataset.get('datos_nuevos_para_flujo') is True:
        datasets_con_datos_nuevos_para_flujo.append(dataset.get('tabla_relativa'))
hay_datos_nuevos = len(datasets_con_datos_nuevos_para_flujo) > 0
parametros_extraccion['estado_extraccion'] = 'SUCCESS'
parametros_extraccion['fecha_fin_extraccion'] = fecha_fin_extraccion
parametros_extraccion['hay_datos_nuevos'] = hay_datos_nuevos
parametros_extraccion['datasets_con_datos_nuevos'] = datasets_con_datos_nuevos
parametros_extraccion['datasets_sin_datos_nuevos'] = datasets_sin_datos_nuevos
parametros_extraccion['datasets_con_datos_nuevos_para_flujo'] = datasets_con_datos_nuevos_para_flujo
parametros_extraccion['estado_flujo_extraccion'] = 'SUCCESS_CON_DATOS_NUEVOS' if hay_datos_nuevos else 'SUCCESS_SIN_DATOS_NUEVOS'
parametros_extraccion['metricas_extraccion'] = {'fecha_inicio': parametros_extraccion.get('fecha_ejecucion'), 'fecha_fin': fecha_fin_extraccion, 'duracion_total_segundos': duracion_total_job, 'duracion_total_minutos': minutos(duracion_total_job), 'total_datasets': len(datasets_extraidos), 'total_datasets_con_conteo': total_datasets_con_conteo, 'total_registros_extraidos': total_registros_extraidos if contar_registros else None, 'total_unidades_extraccion': total_unidades_extraccion if calcular_unidades and contar_registros else None, 'contar_registros': contar_registros, 'persistir_dataframe': persistir_dataframe, 'modo_escritura': modo_escritura, 'compresion': compresion, 'modo_ejecucion': modo_ejecucion, 'sistema_origen': sistema_origen, 'periodo_negocio': periodo_negocio, 'excluir_llaves_segmento': excluir_llaves_segmento, 'hay_datos_nuevos': hay_datos_nuevos, 'total_datasets_con_datos_nuevos': len(datasets_con_datos_nuevos), 'total_datasets_sin_datos_nuevos': len(datasets_sin_datos_nuevos), 'total_datasets_con_datos_nuevos_para_flujo': len(datasets_con_datos_nuevos_para_flujo)}
if usar_checkpoints:
    actualizar_checkpoint(parametros_extraccion, estado='SUCCESS', guardar_detalle_eventos=guardar_detalle_eventos, mensaje='Extracción finalizada correctamente')
guardar_json_s3(ruta_parametros_extraccion, parametros_extraccion)
logger.info('=== FIN JOB EXTRACCION ESTANDAR NIIF ===')
logger.info(f'PARAMETROS_EXTRACCION guardado en: {ruta_parametros_extraccion}')
logger.info(f'Hay datos nuevos para flujo: {hay_datos_nuevos}')
logger.info(f"Estado flujo extracción: {parametros_extraccion['estado_flujo_extraccion']}")
logger.info(f'Duración total extracción segundos: {duracion_total_job}')
logger.info(f'Duración total extracción minutos : {minutos(duracion_total_job)}')
job.commit()
