"""Adaptador de S3 para leer y escribir JSON, y lectura del catálogo de mallas."""
import json
from urllib.parse import urlparse

from ...dominio import catalogo
from ...dominio.errores import ErrorConfiguracion
from ...dominio.serializacion import decimal_default
from .errores_aws import protegido


def separar_uri(uri):
    partes = urlparse(uri or "")
    if partes.scheme != "s3" or not partes.netloc or not partes.path.strip("/"):
        raise ErrorConfiguracion("URI de S3 inválida", codigo="CFG-CNF-009",
                                 detalle={"uri": uri})
    return partes.netloc, partes.path.lstrip("/")


class AlmacenS3:
    def __init__(self, cliente):
        self._cliente = cliente

    @protegido("s3.leer")
    def leer(self, uri):
        bucket, clave = separar_uri(uri)
        cuerpo = self._cliente.get_object(Bucket=bucket, Key=clave)["Body"].read()
        try:
            return json.loads(cuerpo.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ErrorConfiguracion("El archivo no es un JSON válido", codigo="CFG-CNF-009",
                                     detalle={"uri": uri}) from None

    @protegido("s3.escribir")
    def escribir(self, uri, contenido, metadatos):
        bucket, clave = separar_uri(uri)
        cuerpo = json.dumps(contenido, ensure_ascii=False, indent=2,
                            default=decimal_default) + "\n"
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


def cargar_catalogo(almacen, uri):
    """Lee y valida el catálogo de mallas (se hace una vez por contenedor)."""
    return catalogo.parsear(almacen.leer(uri))
