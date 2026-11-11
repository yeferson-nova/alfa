import os
import json
import time
import socket
import logging
import warnings

from urllib.parse import unquote_plus

import boto3
import paramiko

# (opcional) silenciar warnings de cryptography por TripleDES
try:
    from cryptography.utils import CryptographyDeprecationWarning
    warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
except Exception:
    pass

# ---- Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---- Clientes AWS
s3 = boto3.client('s3')
secrets = boto3.client('secretsmanager')

# ---- Variables de entorno
HOST      = os.environ['SFTP_HOST']
PORT      = int(os.environ.get('SFTP_PORT', '22'))
BASE_DIR  = os.environ.get('SFTP_BASE_DIR', '/')
SECRET_ID = os.environ['CREDS_SECRET_ID']
ENABLE_SSH_RSA_FALLBACK = os.environ.get('ENABLE_SSH_RSA_FALLBACK', 'true').lower() == 'true'


def _mkdirs_sftp(sftp, remote_path: str):
    """
    Crea el árbol de directorios remoto si no existe. Tolerante a carrera/errores.
    """
    if not remote_path or remote_path == '/':
        return
    parts = [p for p in remote_path.strip('/').split('/') if p]
    path = ''
    for p in parts:
        path = (path + '/' + p) if path else '/' + p
        try:
            sftp.listdir(path)
        except IOError:
            logger.info(f"Creando dir remoto: {path}")
            try:
                sftp.mkdir(path)
            except Exception:
                # Otro worker pudo crearla entre listdir y mkdir: ignoramos
                pass


def _connect_sftp(host: str, port: int, username: str, password: str):
    """
    Conecta vía SSH y abre un canal SFTP usando SSHClient (acepta disabled_algorithms).
    Retorna (client, sftp).
    """
    # Verificación rápida de reachability TCP (5s)
    try:
        with socket.create_connection((host, port), timeout=5):
            logger.info(f"TCP reachability OK a {host}:{port}")
    except Exception as e:
        logger.error(f"No hay reachability a {host}:{port}: {e}")
        raise

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # timeouts más generosos para redes lentas
    connect_kwargs = dict(
        hostname=host,
        port=port,
        username=username,
        password=password,
        look_for_keys=False,
        allow_agent=False,
        timeout=10,          # connect timeout (socket)
        auth_timeout=20,     # autenticación
        banner_timeout=20,   # banner del server
    )

    if ENABLE_SSH_RSA_FALLBACK:
        # Si el server sólo publica 'ssh-rsa' (legacy), deshabilitamos rsa-sha2-*
        connect_kwargs["disabled_algorithms"] = {
            "pubkeys": ["rsa-sha2-512", "rsa-sha2-256"]
        }

    try:
        client.connect(**connect_kwargs)
    except paramiko.ssh_exception.SSHException as e:
        logger.warning(f"Fallo con disabled_algorithms, reintento simple: {e}")
        # Reintento sin 'disabled_algorithms'
        connect_kwargs.pop("disabled_algorithms", None)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(**connect_kwargs)

    # keepalive para conexiones largas
    try:
        client.get_transport().set_keepalive(30)
    except Exception:
        pass

    sftp = client.open_sftp()
    return client, sftp


def lambda_handler(event, context):
    start_ts = time.time()
    logger.info(f"Event recibido: {json.dumps(event)[:1000]}")

    # --- credenciales SFTP desde Secrets Manager
    try:
        sec = secrets.get_secret_value(SecretId=SECRET_ID)
        creds = json.loads(sec['SecretString'])
        username = creds['username']
        password = creds['password']
        logger.info("Secret cargado OK")
    except Exception as e:
        logger.exception(f"Error obteniendo secreto {SECRET_ID}: {e}")
        return {"status": "error", "stage": "secrets", "error": str(e)}

    # --- Conectar SFTP
    try:
        client, sftp = _connect_sftp(HOST, PORT, username, password)
        logger.info(f"Conectado SFTP a {HOST}:{PORT} como {username}")
    except Exception as e:
        logger.exception(f"Error conectando a SFTP: {e}")
        return {"status": "error", "stage": "connect", "error": str(e)}

    successes = 0
    failures = 0

    try:
        records = event.get('Records', [])
        if not records:
            logger.warning("No hay Records en el evento.")
            return {"status": "ok", "processed": 0}

        for idx, rec in enumerate(records, 1):
            try:
                if rec.get('eventSource') != 'aws:s3':
                    logger.info(f"[{idx}] Record no S3, se ignora")
                    continue

                bucket = rec['s3']['bucket']['name']
                key = unquote_plus(rec['s3']['object']['key'])
                logger.info(f"[{idx}] Procesando s3://{bucket}/{key}")

                # Saltar eventos que son carpetas (key termina en '/'). Al crear
                # una "carpeta" en S3 se genera un objeto de 0 bytes cuyo key
                # termina en '/', que no debe enviarse al SFTP.
                if key.endswith('/'):
                    logger.info(f"[{idx}] El key es una carpeta, se ignora: {key}")
                    continue

                # Normalizar y partir el key en segmentos
                rel = key.lstrip('/')
                parts = [p for p in rel.split('/') if p]

                if not parts:
                    logger.warning(f"[{idx}] Key relativa vacía tras procesamiento, se salta")
                    continue

                filename = parts[-1]

                # ------------------------------------------------------------------
                # Mapeo de carpetas S3 -> carpetas SFTP por caso de uso.
                #   export/primasArl/archivo.csv       -> {BASE_DIR}/primasArl/archivo.csv
                #   export/reservaMatematica/a/b.csv   -> {BASE_DIR}/reservaMatematica/a/b.csv
                #   export/archivo.csv                 -> {BASE_DIR}/archivo.csv
                #   archivo.csv                         -> {BASE_DIR}/archivo.csv
                # ------------------------------------------------------------------
                ROOT_PREFIX = os.environ.get('S3_ROOT_PREFIX', 'export')

                # Segmentos de carpeta (sin el nombre de archivo final)
                dir_parts = parts[:-1]

                # Quitar el prefijo raiz "export" si viene como primer segmento
                if dir_parts and dir_parts[0] == ROOT_PREFIX:
                    dir_parts = dir_parts[1:]

                base = BASE_DIR.strip('/')  # ej. "In" o "upload"
                remote_segments = ([base] if base else []) + dir_parts

                # Rutas ABSOLUTAS y consistentes (con '/' inicial) tanto para crear
                # el directorio como para escribir el archivo. Antes el mkdir usaba
                # ruta absoluta y el putfo relativa, lo que provocaba "No such file".
                remote_dir  = '/' + '/'.join(remote_segments) if remote_segments else '/'
                remote_path = (remote_dir.rstrip('/') + '/' + filename).replace('//', '/')

                logger.info(f"[{idx}] Caso de uso / subruta: {'/'.join(dir_parts) or '(raiz)'}")
                logger.info(f"[{idx}] Destino remoto final: {remote_path}")

                # Asegurar directorio
                _mkdirs_sftp(sftp, remote_dir)

                # Descargar desde S3 (streaming) y subir por SFTP
                obj = s3.get_object(Bucket=bucket, Key=key)
                body = obj['Body']  # StreamingBody
                size = obj.get('ContentLength')
                logger.info(f"[{idx}] Tamaño objeto: {size} bytes")

                transferred = 0

                def _cb(x, y):
                    nonlocal transferred
                    transferred = y
                    if y and size:
                        pct = int((y / size) * 100)
                        if pct in (10, 25, 50, 75, 100):
                            logger.info(f"[{idx}] Progreso {pct}% ({y}/{size})")

                sftp.putfo(body, remote_path, callback=_cb)

                try:
                    body.close()
                except Exception:
                    pass

                # Verificación simple
                try:
                    stat = sftp.stat(remote_path)
                    logger.info(f"[{idx}] Subido OK. Tamaño remoto: {stat.st_size}")
                except Exception as e:
                    logger.warning(f"[{idx}] Subido pero no pude stat remoto: {e}")

                successes += 1

            except Exception as e:
                failures += 1
                logger.exception(f"[{idx}] Error procesando objeto: {e}")

    finally:
        # Cierre ordenado
        try:
            sftp.close()
        except Exception:
            pass
        try:
            client.close()
        except Exception:
            pass

    elapsed = round(time.time() - start_ts, 3)
    logger.info(f"Terminado. Exitos: {successes}, Fallos: {failures}, t={elapsed}s")
    return {"status": "ok", "successes": successes, "failures": failures, "elapsed_s": elapsed}
