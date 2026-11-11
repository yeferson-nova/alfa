import json
from typing import Any, Mapping

from notificacion_certificacion.adaptadores.salida.errores_aws import traduciendo
from notificacion_certificacion.dominio.constantes import CODIFICACION
from notificacion_certificacion.dominio.errores import ErrorConfiguracion
from notificacion_certificacion.dominio.rutas import normalizar_clave


class RepositorioConfiguracionS3:
    def __init__(self, cliente: Any) -> None:
        self._cliente = cliente

    def cargar_json(self, bucket: str, clave: str) -> Mapping[str, Any]:
        clave_normalizada = normalizar_clave(bucket, clave)
        with traduciendo(f"S3 get_object {bucket}/{clave_normalizada}"):
            respuesta = self._cliente.get_object(Bucket=bucket, Key=clave_normalizada)
            contenido = respuesta["Body"].read().decode(CODIFICACION)
        return self._interpretar(contenido, f"{bucket}/{clave_normalizada}")

    def cargar_texto(self, bucket: str, clave: str) -> str:
        clave_normalizada = normalizar_clave(bucket, clave)
        with traduciendo(f"S3 get_object {bucket}/{clave_normalizada}"):
            respuesta = self._cliente.get_object(Bucket=bucket, Key=clave_normalizada)
            return respuesta["Body"].read().decode(CODIFICACION)

    @staticmethod
    def _interpretar(contenido: str, origen: str) -> Mapping[str, Any]:
        try:
            datos = json.loads(contenido)
        except json.JSONDecodeError as error:
            raise ErrorConfiguracion("NTF-CFG-001", f"JSON invalido en {origen}") from error
        if not isinstance(datos, dict):
            raise ErrorConfiguracion("NTF-CFG-001", f"El JSON debe ser un objeto en {origen}")
        return datos
