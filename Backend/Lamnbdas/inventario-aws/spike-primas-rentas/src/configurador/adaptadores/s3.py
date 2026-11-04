"""Adaptador de S3 para leer y escribir JSON."""
import json
from urllib.parse import urlparse

from ..errores import ConfiguracionError
from ..json_utils import decimal_default


def _separar(uri):
    partes = urlparse(uri)
    if partes.scheme != "s3":
        raise ConfiguracionError(f"URI S3 invalida: {uri}")
    return partes.netloc, partes.path.lstrip("/")


class AlmacenS3:
    def __init__(self, cliente):
        self._cliente = cliente

    def leer(self, uri):
        bucket, clave = _separar(uri)
        cuerpo = self._cliente.get_object(Bucket=bucket, Key=clave)["Body"].read()
        return json.loads(cuerpo.decode("utf-8"))

    def escribir(self, uri, contenido, metadatos):
        bucket, clave = _separar(uri)
        cuerpo = json.dumps(contenido, ensure_ascii=False, indent=2, default=decimal_default) + "\n"
        respuesta = self._cliente.put_object(
            Bucket=bucket,
            Key=clave,
            Body=cuerpo.encode("utf-8"),
            ContentType="application/json",
            Metadata=metadatos,
        )
        return {
            "uri": uri,
            "etag": respuesta.get("ETag", "").replace('"', ""),
            "versionId": respuesta.get("VersionId"),
        }
