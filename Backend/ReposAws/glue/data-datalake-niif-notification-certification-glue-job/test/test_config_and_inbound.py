import json
import runpy
import sys
from pathlib import Path

import pytest

from fakes import InMemoryEventLogger, FixedClock
from certification_notifier.adapters.inbound import glue_job
from certification_notifier.adapters.outbound.json_logger import JsonEventLogger
from certification_notifier.adapters.outbound.clock import UuidTokenGenerator, SystemClock
from certification_notifier.application.notify_certification import JobResult
from certification_notifier.application.ports import LogEvent
from certification_notifier.config.settings import read_parameters
from certification_notifier.config.composition import Context, build_context
from certification_notifier.domain.errors import ConfigurationError, InputError, PlatformError
from builders import build_parameters

ARGUMENTS = {
    "useCaseId": "casoA",
    "certCode": "CERT1",
    "emailType": "Certification",
    "CONFIG_BUCKET": "bucket-config",
    "CONFIG_KEY": "casos/casoA.json",
    "GLOBAL_CONFIG_KEY": "global.json",
    "REDIRECT_BASE_URL": "https://redir.example/r",
    "SHORT_LINKS_TABLE": "tabla-enlaces",
    "SES_SENDER": "no-responder@example.com",
    "INDICE_CERT_CODE": "indice-cert",
    "ZONA_HORARIA": "America/Bogota",
    "DIAS_VIGENCIA_ENLACE": "7",
    "LONGITUD_TOKEN_ENLACE": "8",
    "SEGUNDOS_ENTRE_CONSULTAS": "1",
    "SEGUNDOS_MAXIMOS_CONSULTA": "60",
}


def _argv(**changes):
    values = {**ARGUMENTS, **changes}
    argv = []
    for name, value in values.items():
        if value is not None:
            argv.extend([f"--{name}", str(value)])
    return argv


def _without(*names):
    return [element for pair in _pairs(ARGUMENTS, names) for element in pair]


def _pairs(values, excluded):
    return [(f"--{name}", value) for name, value in values.items() if name not in excluded]


def test_read_parameters_builds_contract_with_normalized_values():
    parameters = read_parameters(_argv(**{"JOB_RUN_ID": "jr_1"}))

    assert parameters.use_case_id == "casoA"
    assert parameters.email_type == "certification"
    assert parameters.link_validity_days == 7
    assert parameters.link_token_length == 8
    assert parameters.poll_interval_seconds == 1.0
    assert parameters.max_query_wait_seconds == 60.0
    assert parameters.configuration_set is None
    assert parameters.regeneration_without_new_data is False
    assert parameters.execution_id == "jr_1"


def test_read_parameters_generates_id_if_glue_does_not_send_it():
    first = read_parameters(_argv())
    second = read_parameters(_argv())
    assert first.execution_id and first.execution_id != second.execution_id


def test_read_parameters_accepts_optionals_and_regeneration_flag():
    parameters = read_parameters(_argv(SES_CONFIGURATION_SET=" conjunto ", REGENERACION_SIN_DATOS_NUEVOS="TRUE"))
    assert parameters.configuration_set == "conjunto"
    assert parameters.regeneration_without_new_data is True


def test_read_parameters_ignores_glue_own_arguments():
    parameters = read_parameters(_argv() + ["--JOB_NAME", "job", "--enable-metrics", "true"])
    assert parameters.use_case_id == "casoA"


def test_read_parameters_rejects_argument_abbreviations():
    argv = [token for token in _argv() if token != "--useCaseId" and token != "casoA"] + ["--useCase", "casoA"]
    with pytest.raises(InputError):
        read_parameters(argv)


@pytest.mark.parametrize("missing", sorted(ARGUMENTS))
def test_read_parameters_requires_each_mandatory_argument(missing):
    with pytest.raises(InputError) as error:
        read_parameters(_without(missing))
    assert error.value.code == "NTF-ENT-001"


@pytest.mark.parametrize(
    "change",
    [
        {"useCaseId": "   "},
        {"SES_SENDER": " "},
        {"DIAS_VIGENCIA_ENLACE": "0"},
        {"DIAS_VIGENCIA_ENLACE": "x"},
        {"LONGITUD_TOKEN_ENLACE": "0"},
        {"LONGITUD_TOKEN_ENLACE": "33"},
        {"SEGUNDOS_ENTRE_CONSULTAS": "0"},
        {"SEGUNDOS_ENTRE_CONSULTAS": "abc"},
        {"SEGUNDOS_MAXIMOS_CONSULTA": "0.5"},
        {"REGENERACION_SIN_DATOS_NUEVOS": "quiza"},
    ],
)
def test_read_parameters_rejects_invalid_values(change):
    with pytest.raises(InputError) as error:
        read_parameters(_argv(**change))
    assert error.value.code == "NTF-ENT-001"


def test_logger_writes_one_json_line_per_event_with_context():
    lines: list[str] = []
    logger = JsonEventLogger(FixedClock(), "ejec-9", "componente", lines.append)

    logger.log(LogEvent.EMAIL_SENT, level="WARN", position=1, message_id="ñ")

    (line,) = lines
    data = json.loads(line)
    assert data == {
        "marcaTiempo": "2026-11-04T15:00:00+00:00",
        "nivel": "WARN",
        "evento": "NTF_CORREO_ENVIADO",
        "funcion": "componente",
        "ejecucion_id": "ejec-9",
        "position": 1,
        "message_id": "ñ",
    }
    assert "ñ" in line


def test_system_clock_returns_local_and_utc_time_with_zone():
    clock = SystemClock("America/Bogota")
    assert clock.local_now().utcoffset().total_seconds() == -5 * 3600
    assert clock.utc_now().utcoffset().total_seconds() == 0


def test_system_clock_rejects_unknown_zones():
    with pytest.raises(InputError) as error:
        SystemClock("Mundo/Imaginario")
    assert error.value.code == "NTF-ENT-003"


def test_token_generator_respects_length_and_does_not_repeat():
    generator = UuidTokenGenerator()
    tokens = {generator.generate(8) for _ in range(50)}
    assert len(tokens) == 50
    assert all(len(token) == 8 for token in tokens)


class _UseCase:
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error

    def run(self):
        if self.error is not None:
            raise self.error
        return self.result


def _factories(use_case, logger, received_arguments=None):
    def read(argv):
        if received_arguments is not None:
            received_arguments.append(list(argv))
        return build_parameters()

    def build(_parameters):
        return Context(use_case=use_case, logger=logger)

    return read, build


def test_run_returns_the_use_case_result():
    read, build = _factories(_UseCase(result=JobResult.SENT), InMemoryEventLogger())
    assert glue_job.run(["--x", "1"], read=read, build=build) == JobResult.SENT


def test_run_takes_command_line_arguments_by_default(monkeypatch):
    received: list[list[str]] = []
    monkeypatch.setattr(sys, "argv", ["job.py", "--a", "1"])
    read, build = _factories(_UseCase(result=JobResult.SENT), InMemoryEventLogger(), received)

    glue_job.run(read=read, build=build)

    assert received == [["--a", "1"]]


def test_run_logs_and_reraises_classified_errors():
    logger = InMemoryEventLogger()
    error = ConfigurationError("NTF-CFG-001", "falta clave")
    read, build = _factories(_UseCase(error=error), logger)

    with pytest.raises(ConfigurationError):
        glue_job.run([], read=read, build=build)

    event, level, context = logger.events[0]
    assert (event, level) == (LogEvent.JOB_ERROR, "ERROR")
    assert context["codigo"] == "NTF-CFG-001"
    assert context["dominio"] == "CONFIGURACION"


def test_run_classifies_and_reraises_unexpected_errors():
    logger = InMemoryEventLogger()
    read, build = _factories(_UseCase(error=RuntimeError("fallo inesperado")), logger)

    with pytest.raises(RuntimeError):
        glue_job.run([], read=read, build=build)

    _, _, context = logger.events[0]
    assert context["codigo"] == "NTF-PLA-999"
    assert context["mensaje"] == "fallo inesperado"


def test_run_does_not_catch_argument_read_errors():
    def read(_argv):
        raise InputError("NTF-ENT-001", "falta argumento")

    with pytest.raises(InputError):
        glue_job.run([], read=read, build=lambda parameters: None)


def test_build_context_wires_use_case_with_real_adapters():
    context = build_context(build_parameters(configuration_set="conjunto"))
    assert context.use_case is not None
    assert context.logger is not None


def test_build_context_rejects_invalid_timezone():
    with pytest.raises(InputError):
        build_context(build_parameters(timezone_name="Mundo/Imaginario"))


def test_job_script_calls_the_entry_point(monkeypatch):
    calls = []
    monkeypatch.setattr(glue_job, "run", lambda: calls.append("ejecutado"))
    script = Path(__file__).resolve().parents[1] / "scripts" / "notification_certification.py"

    runpy.run_path(str(script), run_name="__main__")

    assert calls == ["ejecutado"]


def test_job_script_runs_nothing_on_import(monkeypatch):
    calls = []
    monkeypatch.setattr(glue_job, "run", lambda: calls.append("ejecutado"))
    script = Path(__file__).resolve().parents[1] / "scripts" / "notification_certification.py"

    runpy.run_path(str(script), run_name="importado")

    assert calls == []


def test_platform_errors_can_use_unclassified_code():
    assert PlatformError("NTF-PLA-999", "x").as_dict()["codigo"] == "NTF-PLA-999"
