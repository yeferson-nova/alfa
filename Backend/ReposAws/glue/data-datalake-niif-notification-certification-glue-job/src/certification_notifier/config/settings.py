import argparse
from typing import Any, NoReturn, Sequence
from uuid import uuid4

from certification_notifier.domain.constants import MAX_TOKEN_LENGTH
from certification_notifier.domain.errors import InputError
from certification_notifier.domain.models import RunParameters

FLAG_VALUES = ("true", "false")

REQUIRED_ARGUMENTS: tuple[tuple[str, str, type], ...] = (
    ("useCaseId", "use_case_id", str),
    ("certCode", "cert_code", str),
    ("emailType", "email_type", str),
    ("CONFIG_BUCKET", "config_bucket", str),
    ("CONFIG_KEY", "case_key", str),
    ("GLOBAL_CONFIG_KEY", "global_key", str),
    ("REDIRECT_BASE_URL", "redirect_url", str),
    ("SHORT_LINKS_TABLE", "links_table", str),
    ("SES_SENDER", "sender", str),
    ("INDICE_CERT_CODE", "cert_code_index", str),
    ("ZONA_HORARIA", "timezone_name", str),
    ("DIAS_VIGENCIA_ENLACE", "link_validity_days", int),
    ("LONGITUD_TOKEN_ENLACE", "link_token_length", int),
    ("SEGUNDOS_ENTRE_CONSULTAS", "poll_interval_seconds", float),
    ("SEGUNDOS_MAXIMOS_CONSULTA", "max_query_wait_seconds", float),
)

TEXT_FIELDS = (
    "use_case_id",
    "cert_code",
    "email_type",
    "config_bucket",
    "case_key",
    "global_key",
    "redirect_url",
    "links_table",
    "sender",
    "cert_code_index",
    "timezone_name",
)


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise InputError("NTF-ENT-001", message)


def _parser() -> argparse.ArgumentParser:
    parser = _Parser(allow_abbrev=False, add_help=False)
    for name, destination, kind in REQUIRED_ARGUMENTS:
        parser.add_argument(f"--{name}", dest=destination, type=kind, required=True)
    parser.add_argument("--SES_CONFIGURATION_SET", dest="configuration_set", default=None)
    parser.add_argument(
        "--REGENERACION_SIN_DATOS_NUEVOS", dest="regeneration", default=FLAG_VALUES[1]
    )
    parser.add_argument("--JOB_RUN_ID", dest="execution_id", default=None)
    return parser


def _validate_texts(raw_args: argparse.Namespace) -> None:
    for field_name in TEXT_FIELDS:
        if not str(getattr(raw_args, field_name)).strip():
            raise InputError("NTF-ENT-001", f"El argumento {field_name} no puede estar vacio")


def _validate_numbers(raw_args: argparse.Namespace) -> None:
    if raw_args.link_validity_days <= 0:
        raise InputError("NTF-ENT-001", "DIAS_VIGENCIA_ENLACE debe ser mayor a cero")
    if not 0 < raw_args.link_token_length <= MAX_TOKEN_LENGTH:
        raise InputError("NTF-ENT-001", f"LONGITUD_TOKEN_ENLACE debe estar entre 1 y {MAX_TOKEN_LENGTH}")
    if raw_args.poll_interval_seconds <= 0:
        raise InputError("NTF-ENT-001", "SEGUNDOS_ENTRE_CONSULTAS debe ser mayor a cero")
    if raw_args.max_query_wait_seconds < raw_args.poll_interval_seconds:
        raise InputError("NTF-ENT-001", "SEGUNDOS_MAXIMOS_CONSULTA no puede ser menor al intervalo")


def _read_flag(value: str) -> bool:
    text = value.strip().lower()
    if text not in FLAG_VALUES:
        raise InputError("NTF-ENT-001", "Marca de regeneracion invalida")
    return text == FLAG_VALUES[0]


def _optional(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def read_parameters(argv: Sequence[str]) -> RunParameters:
    raw_args, _ = _parser().parse_known_args(list(argv))
    _validate_texts(raw_args)
    _validate_numbers(raw_args)
    return RunParameters(
        use_case_id=raw_args.use_case_id.strip(),
        cert_code=raw_args.cert_code.strip(),
        email_type=raw_args.email_type.strip().lower(),
        config_bucket=raw_args.config_bucket.strip(),
        case_key=raw_args.case_key.strip(),
        global_key=raw_args.global_key.strip(),
        redirect_url=raw_args.redirect_url.strip(),
        links_table=raw_args.links_table.strip(),
        sender=raw_args.sender.strip(),
        configuration_set=_optional(raw_args.configuration_set),
        cert_code_index=raw_args.cert_code_index.strip(),
        timezone_name=raw_args.timezone_name.strip(),
        link_validity_days=raw_args.link_validity_days,
        link_token_length=raw_args.link_token_length,
        poll_interval_seconds=raw_args.poll_interval_seconds,
        max_query_wait_seconds=raw_args.max_query_wait_seconds,
        regeneration_without_new_data=_read_flag(raw_args.regeneration),
        execution_id=_optional(raw_args.execution_id) or uuid4().hex,
    )
