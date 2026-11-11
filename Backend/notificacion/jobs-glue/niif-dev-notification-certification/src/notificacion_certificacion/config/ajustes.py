import argparse
from typing import Any, NoReturn, Sequence

from notificacion_certificacion.dominio.constantes import LONGITUD_MAXIMA_TOKEN
from notificacion_certificacion.dominio.errores import ErrorEntrada
from notificacion_certificacion.dominio.modelos import ParametrosEjecucion

VALORES_BANDERA = ("true", "false")

ARGUMENTOS_OBLIGATORIOS: tuple[tuple[str, str, type], ...] = (
    ("useCaseId", "caso_uso", str),
    ("certCode", "cert_code", str),
    ("emailType", "tipo_correo", str),
    ("CONFIG_BUCKET", "bucket_configuracion", str),
    ("CONFIG_KEY", "clave_caso", str),
    ("GLOBAL_CONFIG_KEY", "clave_global", str),
    ("REDIRECT_BASE_URL", "url_redireccion", str),
    ("SHORT_LINKS_TABLE", "tabla_enlaces", str),
    ("SES_SENDER", "remitente", str),
    ("INDICE_CERT_CODE", "indice_cert_code", str),
    ("ZONA_HORARIA", "zona_horaria", str),
    ("DIAS_VIGENCIA_ENLACE", "dias_vigencia_enlace", int),
    ("LONGITUD_TOKEN_ENLACE", "longitud_token_enlace", int),
    ("SEGUNDOS_ENTRE_CONSULTAS", "segundos_entre_consultas", float),
    ("SEGUNDOS_MAXIMOS_CONSULTA", "segundos_maximos_consulta", float),
)

CAMPOS_TEXTO = (
    "caso_uso",
    "cert_code",
    "tipo_correo",
    "bucket_configuracion",
    "clave_caso",
    "clave_global",
    "url_redireccion",
    "tabla_enlaces",
    "remitente",
    "indice_cert_code",
    "zona_horaria",
)


class _Analizador(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise ErrorEntrada("NTF-ENT-001", message)


def _analizador() -> argparse.ArgumentParser:
    analizador = _Analizador(allow_abbrev=False, add_help=False)
    for nombre, destino, tipo in ARGUMENTOS_OBLIGATORIOS:
        analizador.add_argument(f"--{nombre}", dest=destino, type=tipo, required=True)
    analizador.add_argument("--SES_CONFIGURATION_SET", dest="conjunto_configuracion", default=None)
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


def _validar_numeros(crudos: argparse.Namespace) -> None:
    if crudos.dias_vigencia_enlace <= 0:
        raise ErrorEntrada("NTF-ENT-001", "DIAS_VIGENCIA_ENLACE debe ser mayor a cero")
    if not 0 < crudos.longitud_token_enlace <= LONGITUD_MAXIMA_TOKEN:
        raise ErrorEntrada("NTF-ENT-001", f"LONGITUD_TOKEN_ENLACE debe estar entre 1 y {LONGITUD_MAXIMA_TOKEN}")
    if crudos.segundos_entre_consultas <= 0:
        raise ErrorEntrada("NTF-ENT-001", "SEGUNDOS_ENTRE_CONSULTAS debe ser mayor a cero")
    if crudos.segundos_maximos_consulta < crudos.segundos_entre_consultas:
        raise ErrorEntrada("NTF-ENT-001", "SEGUNDOS_MAXIMOS_CONSULTA no puede ser menor al intervalo")


def _leer_bandera(valor: str) -> bool:
    texto = valor.strip().lower()
    if texto not in VALORES_BANDERA:
        raise ErrorEntrada("NTF-ENT-001", "Marca de regeneracion invalida")
    return texto == VALORES_BANDERA[0]


def _opcional(valor: Any) -> str | None:
    return valor.strip() if isinstance(valor, str) and valor.strip() else None


def leer_parametros(argv: Sequence[str]) -> ParametrosEjecucion:
    crudos, _ = _analizador().parse_known_args(list(argv))
    _validar_textos(crudos)
    _validar_numeros(crudos)
    return ParametrosEjecucion(
        caso_uso=crudos.caso_uso.strip(),
        cert_code=crudos.cert_code.strip(),
        tipo_correo=crudos.tipo_correo.strip().lower(),
        bucket_configuracion=crudos.bucket_configuracion.strip(),
        clave_caso=crudos.clave_caso.strip(),
        clave_global=crudos.clave_global.strip(),
        url_redireccion=crudos.url_redireccion.strip(),
        tabla_enlaces=crudos.tabla_enlaces.strip(),
        remitente=crudos.remitente.strip(),
        conjunto_configuracion=_opcional(crudos.conjunto_configuracion),
        indice_cert_code=crudos.indice_cert_code.strip(),
        zona_horaria=crudos.zona_horaria.strip(),
        dias_vigencia_enlace=crudos.dias_vigencia_enlace,
        longitud_token_enlace=crudos.longitud_token_enlace,
        segundos_entre_consultas=crudos.segundos_entre_consultas,
        segundos_maximos_consulta=crudos.segundos_maximos_consulta,
        regeneracion_sin_datos_nuevos=_leer_bandera(crudos.regeneracion),
        id_ejecucion=_opcional(crudos.id_ejecucion),
        id_corrida=_opcional(crudos.id_corrida),
    )
