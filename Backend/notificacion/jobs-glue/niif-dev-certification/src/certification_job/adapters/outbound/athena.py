import math
from typing import Any, Callable

from certification_job.adapters.outbound.aws_errors import translating
from certification_job.domain.errors import CertificationError, PlatformError
from certification_job.domain.models import ProbeResult, QueryDetails, QueryExecution, WaitPolicy

SUCCEEDED = "SUCCEEDED"
FAILED_STATES = frozenset({"FAILED", "CANCELLED"})
PROBE_MAX_RESULTS = 2


class AthenaQueryRunner:
    def __init__(
        self,
        client: Any,
        sleep: Callable[[float], None],
        catalog: str,
        output_location: str,
        wait: WaitPolicy,
    ) -> None:
        self._client = client
        self._sleep = sleep
        self._catalog = catalog
        self._output_location = output_location
        self._poll_seconds = wait.poll_seconds
        self._polls = max(1, math.ceil(wait.max_wait_seconds / wait.poll_seconds))

    def run(self, sql: str, database: str) -> QueryExecution:
        with translating("Athena start_query_execution"):
            response = self._client.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={"Catalog": self._catalog, "Database": database},
                ResultConfiguration={"OutputLocation": self._output_location},
            )
        query_execution_id = response["QueryExecutionId"]
        return QueryExecution(query_execution_id, self._wait(query_execution_id))

    def describe(self, query_execution_id: str) -> QueryDetails:
        execution = self._execution(query_execution_id)
        return QueryDetails(
            state=execution["Status"]["State"],
            manifest_location=execution.get("Statistics", {}).get("DataManifestLocation"),
        )

    def probe(self, query_execution_id: str) -> ProbeResult:
        with translating("Athena get_query_results"):
            response = self._client.get_query_results(
                QueryExecutionId=query_execution_id, MaxResults=PROBE_MAX_RESULTS
            )
        result_set = response["ResultSet"]
        columns = result_set.get("ResultSetMetadata", {}).get("ColumnInfo", [])
        return ProbeResult(
            row_count=len(result_set.get("Rows", [])),
            has_more=bool(response.get("NextToken")),
            column_names=tuple(column["Name"] for column in columns),
        )

    def _execution(self, query_execution_id: str) -> dict[str, Any]:
        with translating("Athena get_query_execution"):
            return self._client.get_query_execution(QueryExecutionId=query_execution_id)["QueryExecution"]

    def _wait(self, query_execution_id: str) -> int | None:
        for _ in range(self._polls):
            execution = self._execution(query_execution_id)
            status = execution["Status"]
            if status["State"] == SUCCEEDED:
                return execution.get("Statistics", {}).get("DataScannedInBytes")
            if status["State"] in FAILED_STATES:
                raise PlatformError(
                    "CER-PLA-002",
                    f"Consulta {query_execution_id} {status['State']}: {status.get('StateChangeReason', '')}",
                )
            self._sleep(self._poll_seconds)
        outcome = self._cancel(query_execution_id)
        raise PlatformError(
            "CER-PLA-005",
            f"La consulta {query_execution_id} excedio el tiempo maximo de espera; {outcome}",
            recoverable=True,
        )

    def _cancel(self, query_execution_id: str) -> str:
        try:
            with translating("Athena stop_query_execution"):
                self._client.stop_query_execution(QueryExecutionId=query_execution_id)
        except CertificationError as error:
            return f"no se pudo cancelar ({error.code})"
        return "consulta cancelada en Athena"
