import json
import runpy
from pathlib import Path

import pytest

from builders import global_data, parameters
from certification_job.adapters.inbound import glue_job
from certification_job.adapters.outbound.s3_store import S3ObjectStore
from certification_job.config.composition import Context, build_context
from certification_job.config.settings import read_parameters
from certification_job.domain.errors import ConfigurationError, InputError
from certification_job.domain.models import CertificationResult, ExportedFile
from fakes import MemoryLog

ARGUMENTS = {
    "PERIOD": "202606",
    "CONFIG_BUCKET": "bucket-config",
    "CONFIG_KEY": "cases/caseA.json",
    "GLOBAL_CONFIG_KEY": "global.json",
}
SOURCE = Path(__file__).resolve().parents[1] / "src"


def _argv(**changes):
    values = {**ARGUMENTS, **changes}
    return [token for name, value in values.items() if value is not None for token in (f"--{name}", value)]


def test_read_parameters_builds_the_contract():
    params = read_parameters(_argv() + ["--JOB_NAME", "job", "--JOB_RUN_ID", " jr_1 ", "--enable-metrics", "true"])
    assert (params.period, params.config_bucket, params.case_key, params.global_key) == (
        "202606",
        "bucket-config",
        "cases/caseA.json",
        "global.json",
    )
    assert params.regeneration_without_new_data is False
    assert (params.flow_execution_id, params.origin_execution_ids, params.job_run_id) == (None, (), "jr_1")


def test_read_parameters_reads_the_flow_ids_and_the_regeneration_flag():
    params = read_parameters(
        _argv(EJECUCION_ID=" flujo-1 ", EJECUCIONES_ORIGEN="o-1, o-2", REGENERACION_SIN_DATOS_NUEVOS="TRUE")
    )
    assert (params.flow_execution_id, params.origin_execution_ids) == ("flujo-1", ("o-1", "o-2"))
    assert params.regeneration_without_new_data is True
    assert read_parameters(_argv(EJECUCIONES_ORIGEN=" ")).origin_execution_ids == ()


@pytest.mark.parametrize("missing", sorted(ARGUMENTS))
def test_read_parameters_requires_each_argument(missing):
    with pytest.raises(InputError) as error:
        read_parameters(_argv(**{missing: None}))
    assert error.value.code == "CER-ENT-001"


@pytest.mark.parametrize(
    "changes, code",
    [
        ({"CONFIG_KEY": " "}, "CER-ENT-001"),
        ({"PERIOD": "2026-06"}, "CER-ENT-002"),
        ({"REGENERACION_SIN_DATOS_NUEVOS": "quiza"}, "CER-ENT-003"),
        ({"EJECUCIONES_ORIGEN": "o-1,,o-2"}, "CER-ENT-004"),
        ({"EJECUCIONES_ORIGEN": "o-1,o-1"}, "CER-ENT-004"),
    ],
)
def test_read_parameters_rejects_invalid_values(changes, code):
    with pytest.raises(InputError) as error:
        read_parameters(_argv(**changes))
    assert error.value.code == code


def test_read_parameters_does_not_accept_abbreviations():
    argv = _argv(PERIOD=None) + ["--PER", "202606"]
    with pytest.raises(InputError):
        read_parameters(argv)


def _global_in_s3(monkeypatch, data):
    reads = []

    def load_json(_store, bucket, key):
        reads.append((bucket, key))
        return data

    monkeypatch.setattr(S3ObjectStore, "load_json", load_json)
    return reads


def test_build_context_reads_the_global_json_and_wires_the_use_case(monkeypatch):
    reads = _global_in_s3(monkeypatch, global_data())
    context = build_context(parameters())
    assert context.use_case is not None and context.log is not None
    assert reads == [("bucket-config", "global.json")]


def test_build_context_fails_on_an_invalid_global_json(monkeypatch):
    data = global_data()
    del data["certification"]["report_folders"]
    _global_in_s3(monkeypatch, data)
    with pytest.raises(ConfigurationError):
        build_context(parameters())


class _UseCase:
    def __init__(self, error=None):
        self.error = error
        self.files = [ExportedFile("k", "u", "f", 0, True), ExportedFile("k2", "u", "f2", 3, False)]

    def run(self):
        if self.error:
            raise self.error
        return CertificationResult("CERT", tuple(self.files))


def _context(error=None):
    return Context(use_case=_UseCase(error), log=MemoryLog())


def test_run_returns_the_result_and_logs_the_summary():
    context = _context()
    result = glue_job.run(["--x"], read=lambda argv: parameters(), build=lambda params: context)
    assert result.cert_code == "CERT"
    summary = context.log.fields(glue_job.LogEvent.JOB_SUMMARY)
    assert (summary["job_exitoso"], summary["archivos_generados"], summary["archivos_sin_datos"]) == (1, 2, 1)


def test_run_logs_and_reraises_classified_errors_with_the_summary():
    context = _context(ConfigurationError("CER-CFG-003", "sin carpeta"))
    with pytest.raises(ConfigurationError):
        glue_job.run([], read=lambda argv: parameters(), build=lambda params: context)
    error = context.log.fields(glue_job.LogEvent.JOB_ERROR)
    assert (error["codigo"], error["detalle"]) == ("CER-CFG-003", "sin carpeta")
    assert context.log.fields(glue_job.LogEvent.JOB_SUMMARY)["job_fallido"] == 1


def test_run_classifies_unexpected_errors():
    context = _context(KeyError("x"))
    with pytest.raises(KeyError):
        glue_job.run([], read=lambda argv: parameters(), build=lambda params: context)
    assert context.log.fields(glue_job.LogEvent.JOB_ERROR)["codigo"] == "CER-PLA-999"


def test_startup_errors_are_logged_as_json_with_the_trace_ids():
    lines: list[str] = []
    argv = ["--EJECUCION_ID", "flujo-1", "--JOB_RUN_ID", "jr_9", "--PERIOD", "mal"]
    with pytest.raises(InputError):
        glue_job.run(argv, write=lines.append)
    line = json.loads(lines[0])
    assert (line["evento"], line["nivel"], line["fase"]) == ("CER_ERROR_JOB", "ERROR", "arranque")
    assert (line["ejecucion_id"], line["request_id"]) == ("flujo-1", "jr_9")
    assert line["codigo"] == "CER-ENT-001"


def test_run_reads_sys_argv_by_default(monkeypatch):
    monkeypatch.setattr(glue_job.sys, "argv", ["script", "--PERIOD", "202606"])
    received = []
    glue_job.run(read=lambda argv: received.append(list(argv)) or parameters(), build=lambda params: _context())
    assert received == [["--PERIOD", "202606"]]


def test_default_writer_prints_the_line(capsys):
    glue_job._write("linea")
    assert capsys.readouterr().out == "linea\n"


def test_entry_script_calls_the_inbound_adapter(monkeypatch):
    calls = []
    monkeypatch.setattr(glue_job, "run", lambda: calls.append("run"))
    runpy.run_path(str(SOURCE / "niif_certification.py"), run_name="__main__")
    assert calls == ["run"]
