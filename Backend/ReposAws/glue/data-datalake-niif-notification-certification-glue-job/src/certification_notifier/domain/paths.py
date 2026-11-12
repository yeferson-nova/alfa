from certification_notifier.domain.errors import ConfigurationError

S3_SCHEME = "s3://"


def query_output_path(bucket: str, prefix: str) -> str:
    return f"{S3_SCHEME}{bucket}/{prefix}/"


def normalize_key(bucket: str, key: str) -> str:
    text = str(key or "").strip()
    prefix = f"{S3_SCHEME}{bucket}/"
    if text.startswith(prefix):
        text = text[len(prefix):]
    if text.startswith(S3_SCHEME):
        raise ConfigurationError(
            "NTF-CFG-006",
            f"La ruta S3 no pertenece al bucket configurado. bucket={bucket}, clave={text}",
        )
    return text.lstrip("/")
