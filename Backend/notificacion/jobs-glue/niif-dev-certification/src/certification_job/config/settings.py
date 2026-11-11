import argparse
from typing import NoReturn, Sequence

from certification_job.domain.errors import InputError
from certification_job.domain.models import JobParameters
from certification_job.domain.periods import validate_period

REQUIRED_ARGUMENTS: tuple[tuple[str, str], ...] = (
    ("PERIOD", "period"),
    ("CONFIG_BUCKET", "config_bucket"),
    ("CONFIG_KEY", "case_key"),
    ("GLOBAL_CONFIG_KEY", "global_key"),
)
FLAG_VALUES = ("true", "false")
ORIGIN_SEPARATOR = ","


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise InputError("CER-ENT-001", message)


def _parser() -> argparse.ArgumentParser:
    parser = _Parser(allow_abbrev=False, add_help=False)
    for name, destination in REQUIRED_ARGUMENTS:
        parser.add_argument(f"--{name}", dest=destination, required=True)
    parser.add_argument("--REGENERACION_SIN_DATOS_NUEVOS", dest="regeneration", default=FLAG_VALUES[1])
    parser.add_argument("--EJECUCION_ID", dest="flow_execution_id", default=None)
    parser.add_argument("--EJECUCIONES_ORIGEN", dest="origin_execution_ids", default=None)
    parser.add_argument("--JOB_RUN_ID", dest="job_run_id", default=None)
    return parser


def _required_text(raw: argparse.Namespace, destination: str, name: str) -> str:
    value = str(getattr(raw, destination)).strip()
    if not value:
        raise InputError("CER-ENT-001", f"El argumento {name} no puede estar vacio")
    return value


def _flag(value: str) -> bool:
    text = value.strip().lower()
    if text not in FLAG_VALUES:
        raise InputError("CER-ENT-003", "REGENERACION_SIN_DATOS_NUEVOS debe ser true o false")
    return text == FLAG_VALUES[0]


def _optional(value: str | None) -> str | None:
    return value.strip() if value and value.strip() else None


def _origin_ids(value: str | None) -> tuple[str, ...]:
    if value is None or not value.strip():
        return ()
    ids = tuple(part.strip() for part in value.split(ORIGIN_SEPARATOR))
    if any(not part for part in ids) or len(set(ids)) != len(ids):
        raise InputError(
            "CER-ENT-004", "EJECUCIONES_ORIGEN debe ser una lista separada por comas, sin vacios ni repetidos"
        )
    return ids


def read_parameters(argv: Sequence[str]) -> JobParameters:
    raw, _ = _parser().parse_known_args(list(argv))
    texts = {destination: _required_text(raw, destination, name) for name, destination in REQUIRED_ARGUMENTS}
    return JobParameters(
        period=validate_period(texts["period"]),
        config_bucket=texts["config_bucket"],
        case_key=texts["case_key"],
        global_key=texts["global_key"],
        regeneration_without_new_data=_flag(raw.regeneration),
        flow_execution_id=_optional(raw.flow_execution_id),
        origin_execution_ids=_origin_ids(raw.origin_execution_ids),
        job_run_id=_optional(raw.job_run_id),
    )
