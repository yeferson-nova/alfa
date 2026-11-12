from typing import Mapping, Sequence

from certification_notifier.application.ports import EventLogger, AnalyticsQueryRunner, LogEvent
from certification_notifier.domain.availability import extract_count
from certification_notifier.domain.errors import QueryFailedError
from certification_notifier.domain.email_html import table_block, query_error_paragraph, table_html
from certification_notifier.domain.models import Query, QueryEnvironment, QueryResult, Tables
from certification_notifier.domain.templating import substitute_vars
from certification_notifier.domain.wording import availability_notice
from certification_notifier.domain.tables import format_result, join_blocks
from certification_notifier.domain.texts import QUERY_ERROR_TEXT


class QueryService:
    def __init__(self, runner: AnalyticsQueryRunner, logger: EventLogger) -> None:
        self._runner = runner
        self._logger = logger

    def tables(
        self,
        queries: Sequence[Query],
        variables: Mapping[str, str],
        query_environment: QueryEnvironment,
    ) -> Tables:
        blocks = [self._block(query, variables, query_environment) for query in queries]
        return Tables(
            text=join_blocks(block.text for block in blocks),
            html="".join(block.html for block in blocks),
        )

    def availability(
        self,
        queries: Sequence[Query],
        variables: Mapping[str, str],
        query_environment: QueryEnvironment,
    ) -> str | None:
        if not queries:
            return None
        empty_members = [
            query.name
            for query in queries
            if self._count(query, variables, query_environment) == 0
        ]
        return availability_notice(str(variables["period"]), empty_members, len(queries))

    def _execute(
        self,
        query: Query,
        variables: Mapping[str, str],
        query_environment: QueryEnvironment,
    ) -> QueryResult:
        database = substitute_vars(query.database, variables, unescape=True)
        sql = substitute_vars(query.sql, variables, unescape=True)
        result = self._runner.run(sql, database, query_environment)
        self._logger.log(
            LogEvent.QUERY_EXECUTED, query=query.name, rows=len(result.rows)
        )
        return result

    def _block(
        self,
        query: Query,
        variables: Mapping[str, str],
        query_environment: QueryEnvironment,
    ) -> Tables:
        try:
            result = self._execute(query, variables, query_environment)
        except QueryFailedError as error:
            self._logger.log(
                LogEvent.QUERY_FAILED, level="ERROR", query=query.name, **error.as_dict()
            )
            return Tables(text=QUERY_ERROR_TEXT, html=table_block(query_error_paragraph()))
        return Tables(text=format_result(result), html=table_block(table_html(result)))

    def _count(
        self,
        query: Query,
        variables: Mapping[str, str],
        query_environment: QueryEnvironment,
    ) -> int:
        return extract_count(self._execute(query, variables, query_environment), query.name)
