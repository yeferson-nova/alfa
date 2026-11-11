from notificacion_certificacion.domain.errores import ErrorConfiguracion

ESQUEMA_S3 = "s3://"


def ruta_salida_consultas(bucket: str, prefijo: str) -> str:
    return f"{ESQUEMA_S3}{bucket}/{prefijo}/"


def normalizar_clave(bucket: str, clave: str) -> str:
    texto = str(clave or "").strip()
    prefijo = f"{ESQUEMA_S3}{bucket}/"
    if texto.startswith(prefijo):
        texto = texto[len(prefijo):]
    if texto.startswith(ESQUEMA_S3):
        raise ErrorConfiguracion(
            "NTF-CFG-006",
            f"La ruta S3 no pertenece al bucket configurado. bucket={bucket}, clave={texto}",
        )
    return texto.lstrip("/")
