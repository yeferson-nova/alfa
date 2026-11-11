import argparse
from typing import Any, NoReturn, Sequence

from notificacion_certificacion.domain.errores import ErrorEntrada
from notificacion_certificacion.domain.modelos import ParametrosEjecucion

VALORES_BANDERA = ("true", "false")

ARGUMENTOS_OBLIGATORIOS: tuple[tuple[str, str, type], ...] = (
    ("useCaseId", "caso_uso", str),
    ("certCode", "cert_code", str),
    ("emailType", "tipo_correo", str),
    ("CONFIG_BUCKET", "bucket_configuracion", str),
    ("CONFIG_KEY", "clave_caso", str),
    ("GLOBAL_CONFIG_KEY", "clave_global", str),
)

ARGUMENTOS_RETIRADOS: dict[str, str] = {
    "REDIRECT_BASE_URL": "notification.redirect_base_url",
    "SHORT_LINKS_TABLE": "notification.short_links_table",
    "SES_SENDER": "notification.ses_sender",
    "SES_CONFIGURATION_SET": "notification.ses_configuration_set",
    "INDICE_CERT_CODE": "dynamodb.cert_code_index",
    "ZONA_HORARIA": "timezone",
    "DIAS_VIGENCIA_ENLACE": "notification.link_validity_days",
    "LONGITUD_TOKEN_ENLACE": "notification.link_token_length",
    "SEGUNDOS_ENTRE_CONSULTAS": "notification.athena_poll_seconds",
    "SEGUNDOS_MAXIMOS_CONSULTA": "notification.athena_max_wait_seconds",
}

CAMPOS_TEXTO = (
    "caso_uso",
    "cert_code",
    "tipo_correo",
    "bucket_configuracion",
    "clave_caso",
    "clave_global",
)


class _Analizador(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise ErrorEntrada("NTF-ENT-001", message)


def _analizador() -> argparse.ArgumentParser:
    analizador = _Analizador(allow_abbrev=False, add_help=False)
    for nombre, destino, tipo in ARGUMENTOS_OBLIGATORIOS:
        analizador.add_argument(f"--{nombre}", dest=destino, type=tipo, required=True)
    analizador.add_argument(
        "--REGENERACION_SIN_DATOS_NUEVOS", dest="regeneracion", default=VALORES_BANDERA[1]
    )
    analizador.add_argument("--JOB_RUN_ID", dest="id_corrida", default=None)
    analizador.add_argument("--EJECUCION_ID", dest="id_ejecucion", default=None)
    return analizador


def _validar_textos(crudos: argparse.Namespace) -> None:
    for campo in CAMPOS_TEXTO:
        if not str(getattr(crudos, campo)).strip():
            raise ErrorEntrada("NTF-ENT-001", f"El argumento {campo} no puede estar vacio")


def _rechazar_retirados(argv: Sequence[str]) -> None:
    for argumento in argv:
        nombre = str(argumento).lstrip("-").split("=", 1)[0]
        if str(argumento).startswith("--") and nombre in ARGUMENTOS_RETIRADOS:
            raise ErrorEntrada(
                "NTF-ENT-004",
                f"El argumento {nombre} ya no se usa: el valor va en el JSON global ({ARGUMENTOS_RETIRADOS[nombre]})",
            )


def _leer_bandera(valor: str) -> bool:
    texto = valor.strip().lower()
    if texto not in VALORES_BANDERA:
        raise ErrorEntrada("NTF-ENT-001", "Marca de regeneracion invalida")
    return texto == VALORES_BANDERA[0]


def _opcional(valor: Any) -> str | None:
    return valor.strip() if isinstance(valor, str) and valor.strip() else None


def leer_parametros(argv: Sequence[str]) -> ParametrosEjecucion:
    _rechazar_retirados(argv)
    crudos, _ = _analizador().parse_known_args(list(argv))
    _validar_textos(crudos)
    return ParametrosEjecucion(
        caso_uso=crudos.caso_uso.strip(),
        cert_code=crudos.cert_code.strip(),
        tipo_correo=crudos.tipo_correo.strip().lower(),
        bucket_configuracion=crudos.bucket_configuracion.strip(),
        clave_caso=crudos.clave_caso.strip(),
        clave_global=crudos.clave_global.strip(),
        regeneracion_sin_datos_nuevos=_leer_bandera(crudos.regeneracion),
        id_ejecucion=_opcional(crudos.id_ejecucion),
        id_corrida=_opcional(crudos.id_corrida),
    )
