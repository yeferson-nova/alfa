import math
from typing import Any, Callable

from certification_notifier.adapters.outbound.aws_errors import translating
from certification_notifier.domain.errors import QueryFailedError, PlatformError
from certification_notifier.domain.models import QueryEnvironment, QueryResult

SUCCEEDED_STATE = "SUCCEEDED"
FAILED_STATES = frozenset({"FAILED", "CANCELLED"})


class AthenaQueryRunner:
    def __init__(
        self,
        client: Any,
        sleep: Callable[[float], None],
        poll_interval_seconds: float,
        max_wait_seconds: float,
    ) -> None:
        self._client = client
        self._sleep = sleep
        self._interval = poll_interval_seconds
        self._attempts = max(1, math.ceil(max_wait_seconds / poll_interval_seconds))

    def run(self, sql: str, database: str, query_environment: QueryEnvironment) -> QueryResult:
        identifier = self._start(sql, database, query_environment)
        self._wait_until_done(identifier)
        return self._read_result(identifier)

    def _start(self, sql: str, database: str, query_environment: QueryEnvironment) -> str:
        with translating("Athena start_query_execution"):
            response = self._client.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={"Catalog": query_environment.catalog, "Database": database},
                ResultConfiguration={"OutputLocation": query_environment.output_location},
            )
        return response["QueryExecutionId"]

    def _status(self, identifier: str) -> tuple[str, str]:
        with translating("Athena get_query_execution"):
            response = self._client.get_query_execution(QueryExecutionId=identifier)
        status = response["QueryExecution"]["Status"]
        return status["State"], status.get("StateChangeReason", "")

    def _wait_until_done(self, identifier: str) -> None:
        for _ in range(self._attempts):
            name, reason = self._status(identifier)
            if name == SUCCEEDED_STATE:
                return
            if name in FAILED_STATES:
                raise QueryFailedError("NTF-PLA-002", f"Consulta {identifier} {name}: {reason}")
            self._sleep(self._interval)
        raise PlatformError("NTF-PLA-005", f"La consulta {identifier} excedio el tiempo maximo de espera")

    @staticmethod
    def _values(row: dict[str, Any]) -> tuple[str, ...]:
        return tuple(column.get("VarCharValue", "") for column in row.get("Data", []))

    def _read_result(self, identifier: str) -> QueryResult:
        headers: tuple[str, ...] | None = None
        rows: list[tuple[str, ...]] = []
        token = None
        while True:
            arguments = {"QueryExecutionId": identifier}
            if token:
                arguments["NextToken"] = token
            with translating("Athena get_query_results"):
                response = self._client.get_query_results(**arguments)
            page = [self._values(row) for row in response["ResultSet"]["Rows"]]
            if headers is None:
                headers, page = (page[0] if page else ()), page[1:]
            rows.extend(page)
            token = response.get("NextToken")
            if not token:
                return QueryResult(headers=headers, rows=tuple(rows))
