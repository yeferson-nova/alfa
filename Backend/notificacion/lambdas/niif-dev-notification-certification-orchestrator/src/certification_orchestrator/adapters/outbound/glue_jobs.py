from typing import Any, Mapping

from certification_orchestrator.adapters.outbound.aws_errors import translating


class GlueJobLauncher:
    def __init__(self, client: Any) -> None:
        self._client = client

    def start(self, job_name: str, arguments: Mapping[str, str]) -> str:
        with translating(f"Glue start_job_run {job_name}"):
            response = self._client.start_job_run(JobName=job_name, Arguments=dict(arguments))
        return str(response["JobRunId"])
