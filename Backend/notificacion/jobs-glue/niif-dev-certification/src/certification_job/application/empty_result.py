from certification_job.application.ports import ObjectStore, QueryRunner
from certification_job.domain.csv_report import column_header_csv
from certification_job.domain.errors import DataError
from certification_job.domain.naming import split_s3_uri
from certification_job.domain.unload import UNLOAD_REJECTED, parse_unload, probe_query

SUCCEEDED = "SUCCEEDED"


class EmptyResultConfirmer:
    def __init__(self, queries: QueryRunner, objects: ObjectStore) -> None:
        self._queries = queries
        self._objects = objects

    def confirm(self, database: str, sql: str, query_execution_id: str, expected_uri: str) -> bytes:
        self._require_empty_manifest(query_execution_id)
        statement = parse_unload(sql)
        if statement.target != expected_uri:
            raise DataError(UNLOAD_REJECTED, "Destino UNLOAD distinto del prefijo buscado")
        probe = self._queries.run(probe_query(statement.inner_query), database)
        result = self._queries.probe(probe.query_execution_id)
        if result.row_count > 1 or result.has_more:
            raise DataError(UNLOAD_REJECTED, "La comprobacion encontro datos; no se crea un archivo vacio")
        if not result.column_names:
            raise DataError(UNLOAD_REJECTED, "Sin metadatos de columnas; no se genera el CSV")
        return column_header_csv(result.column_names)

    def _require_empty_manifest(self, query_execution_id: str) -> None:
        details = self._queries.describe(query_execution_id)
        if details.state != SUCCEEDED:
            raise DataError(UNLOAD_REJECTED, "No se admite un resultado sin datos de una consulta fallida")
        if not details.manifest_location:
            raise DataError(UNLOAD_REJECTED, "Sin manifiesto UNLOAD: no se puede confirmar el resultado vacio")
        bucket, key = split_s3_uri(details.manifest_location)
        if self._objects.read(bucket, key).strip():
            raise DataError(UNLOAD_REJECTED, "Manifiesto no vacio pero faltan archivos UNLOAD")
