from dataclasses import dataclass
from typing import Any

from certification_job.application.empty_result import EmptyResultConfirmer
from certification_job.application.ports import (
    Clock,
    ConfigRepository,
    EventLog,
    IdGenerator,
    LogEvent,
    NotificationRepository,
    ObjectStore,
    QueryRunner,
    Sleeper,
)
from certification_job.domain import csv_report, naming
from certification_job.domain.cert_code import build_cert_code
from certification_job.domain.configuration import read_case_config
from certification_job.domain.errors import ConfigurationError, PlatformError
from certification_job.domain.models import (
    CaseConfig,
    CertificationResult,
    DetailQuery,
    ExportedFile,
    GlobalConfig,
    JobParameters,
    NotificationKey,
)
from certification_job.domain.periods import period_range
from certification_job.domain.templating import render


@dataclass(frozen=True)
class Dependencies:
    configs: ConfigRepository
    queries: QueryRunner
    objects: ObjectStore
    notifications: NotificationRepository
    clock: Clock
    ids: IdGenerator
    sleep: Sleeper
    log: EventLog


@dataclass(frozen=True)
class _QueryContext:
    case: CaseConfig
    cert_code: str | None
    folder: str


class GenerateCertification:
    def __init__(self, parameters: JobParameters, global_config: GlobalConfig, dependencies: Dependencies) -> None:
        self._parameters = parameters
        self._global = global_config
        self._deps = dependencies
        self._confirmer = EmptyResultConfirmer(dependencies.queries, dependencies.objects)
        self.files: list[ExportedFile] = []

    def run(self) -> CertificationResult:
        log = self._deps.log
        log.bind(
            ejecucion_id=self._parameters.flow_execution_id,
            request_id=self._parameters.job_run_id,
            periodo_negocio=self._parameters.period,
        )
        log.record(
            LogEvent.JOB_START, regeneracion_sin_datos_nuevos=self._parameters.regeneration_without_new_data
        )
        if not self._parameters.flow_execution_id:
            log.record(LogEvent.FLOW_ID_MISSING, level="WARN")
        case = self._load_case()
        folder = naming.report_folder(self._global.certification.report_folders, case.use_case_id)
        context = _QueryContext(case, self._cert_code(case), folder)
        cert_code = context.cert_code
        for query in case.detail_queries:
            self.files.append(self._process(context, query))
        if cert_code:
            self._mark_pending(case.use_case_id, cert_code)
        log.record(LogEvent.JOB_END, archivos_generados=len(self.files))
        return CertificationResult(cert_code=cert_code, files=tuple(self.files))

    def _load_case(self) -> CaseConfig:
        parameters = self._parameters
        case = read_case_config(self._deps.configs.load_json(parameters.config_bucket, parameters.case_key))
        self._deps.log.bind(use_case_id=case.use_case_id)
        self._deps.log.record(
            LogEvent.CONFIG_LOADED, tipo_correo=case.email_type, consultas_detalle=len(case.detail_queries)
        )
        return case

    def _cert_code(self, case: CaseConfig) -> str | None:
        cert_code = build_cert_code(
            self._parameters.period,
            case.email_type,
            self._deps.clock.now_local(),
            self._parameters.regeneration_without_new_data,
            self._deps.ids.unique_suffix(),
        )
        self._deps.log.bind(cert_code=cert_code)
        self._deps.log.record(LogEvent.CERT_CODE_GENERATED, genera_cert_code=cert_code is not None)
        return cert_code

    def _variables(self, case: CaseConfig, execution_id: str) -> dict[str, str]:
        config = self._global
        return {
            "useCaseId": case.use_case_id,
            "period": self._parameters.period,
            "periodRange": period_range(self._parameters.period),
            "env": config.environment,
            "emailType": case.email_type,
            "execution_id": execution_id,
            "assets_bucket": config.assets_bucket,
            "report_bucket": config.reports_bucket,
            "unload_path": config.athena_unload_path,
        }

    def _database(self, case: CaseConfig, query: DetailQuery, variables: dict[str, str]) -> str:
        if query.database:
            return render(query.database, variables, f"database de {query.name}")
        if case.default_database:
            return case.default_database
        raise ConfigurationError(
            "CER-CFG-006", f"La consulta {query.name} no tiene database ni athena.defaultDatabase"
        )

    def _process(self, context: _QueryContext, query: DetailQuery) -> ExportedFile:
        execution_id = self._deps.ids.execution_id()
        variables = self._variables(context.case, execution_id)
        database = self._database(context.case, query, variables)
        sql = render(query.sql, variables, f"query de {query.name}")
        execution = self._deps.queries.run(sql, database)
        self._deps.log.record(
            LogEvent.QUERY_EXECUTED,
            consulta=query.name,
            id_consulta=execution.query_execution_id,
            bytes_escaneados=execution.bytes_scanned,
        )
        exported = self._export(context, query.name, execution_id, (database, sql, execution.query_execution_id))
        self._register(context, query.name, execution_id, exported)
        return exported

    def _unload_body(self, execution_id: str, athena: tuple[str, str, str]) -> tuple[bytes, bool]:
        config = self._global
        prefix = naming.unload_prefix(config.athena_unload_path, execution_id)
        listed = self._deps.objects.list_keys(config.assets_bucket, prefix)
        keys = sorted(key for key in listed if naming.is_unload_data_key(key))
        if keys:
            chunks = (self._deps.objects.read(config.assets_bucket, key) for key in keys)
            return csv_report.assemble(config.export.write_bom, b"", chunks), False
        self._deps.log.record(LogEvent.UNLOAD_EMPTY, id_consulta=athena[2])
        database, sql, query_execution_id = athena
        header = self._confirmer.confirm(database, sql, query_execution_id, naming.s3_uri(config.assets_bucket, prefix))
        self._deps.log.record(LogEvent.NO_DATA_CONFIRMED, id_consulta=query_execution_id)
        return csv_report.assemble(config.export.write_bom, header, ()), True

    def _export(
        self, context: _QueryContext, query_name: str, execution_id: str, athena: tuple[str, str, str]
    ) -> ExportedFile:
        config = self._global
        file_name = naming.report_file_name(self._parameters.period, query_name, self._deps.clock.now_local())
        body, no_data = self._unload_body(execution_id, athena)
        headers = csv_report.headers_for_group(context.case.header_specs, csv_report.query_group(file_name))
        data_rows = csv_report.count_data_rows(body, headers, config.export.field_delimiter)
        report_key = f"{context.folder}/{file_name}"
        final_body = csv_report.add_headers(body, headers, config.export.field_delimiter)
        self._deps.objects.write(config.reports_bucket, report_key, final_body, config.export.content_type)
        exported = ExportedFile(
            report_key=report_key,
            console_url=naming.console_url(config.console, config.reports_bucket, report_key),
            file_name=file_name,
            data_rows=data_rows,
            no_data=no_data or data_rows == 0,
        )
        self._deps.log.record(
            LogEvent.FILE_WRITTEN,
            consulta=query_name,
            archivo=naming.s3_uri(config.reports_bucket, report_key),
            filas_datos=data_rows,
            sin_datos=exported.no_data,
        )
        return exported

    def _item(
        self, context: _QueryContext, query_name: str, execution_id: str, exported: ExportedFile
    ) -> dict[str, Any]:
        parameters, settings = self._parameters, self._global.certification
        item: dict[str, Any] = {
            "useCaseId": context.case.use_case_id,
            "executionId": execution_id,
            "s3_url": exported.console_url,
            "status": settings.processed_status,
            "period": parameters.period,
            "emailType": context.case.email_type,
            "queryName": query_name,
            "userEmail": settings.default_user_email,
            "nameFile": exported.file_name,
            "creationDate": self._deps.clock.now_local().isoformat(),
            "configBucket": parameters.config_bucket,
            "configKey": parameters.case_key,
            "globalConfigKey": parameters.global_key,
            "regeneracionSinDatosNuevos": parameters.regeneration_without_new_data,
            "sinDatosPeriodo": exported.no_data,
        }
        if context.cert_code:
            item["certCode"] = context.cert_code
        if parameters.flow_execution_id:
            item["ejecucionId"] = parameters.flow_execution_id
        if parameters.origin_execution_ids:
            item["ejecucionesOrigen"] = list(parameters.origin_execution_ids)
        return item

    def _register(self, context: _QueryContext, query_name: str, execution_id: str, exported: ExportedFile) -> None:
        item = self._item(context, query_name, execution_id, exported)
        self._deps.notifications.put(self._global.notification_table, item)
        self._deps.log.record(
            LogEvent.ITEM_REGISTERED,
            consulta=query_name,
            execution_id_registro=execution_id,
            estado=item["status"],
            sin_datos=exported.no_data,
        )

    def _visible_keys(self, use_case_id: str, cert_code: str) -> list[NotificationKey]:
        config, policy = self._global, self._global.certification.index_visibility
        for attempt in range(1, policy.attempts + 1):
            keys = list(
                self._deps.notifications.find_by_cert_code(
                    config.notification_table, config.cert_code_index, cert_code, use_case_id
                )
            )
            if keys:
                return keys
            if attempt < policy.attempts:
                self._deps.log.record(
                    LogEvent.INDEX_NOT_VISIBLE, level="WARN", intento=attempt, intentos=policy.attempts
                )
                self._deps.sleep(policy.wait_seconds)
        raise PlatformError(
            "CER-PLA-006",
            f"El certCode no aparecio en el indice despues de {policy.attempts} intentos",
            recoverable=True,
        )

    def _mark_pending(self, use_case_id: str, cert_code: str) -> None:
        keys = self._visible_keys(use_case_id, cert_code)
        parameters, settings = self._parameters, self._global.certification
        values = {
            "status": settings.pending_status,
            "pendingDate": self._deps.clock.now_local().isoformat(),
            "configBucket": parameters.config_bucket,
            "configKey": parameters.case_key,
            "globalConfigKey": parameters.global_key,
        }
        self._deps.notifications.mark_pending(self._global.notification_table, keys, values)
        self._deps.log.record(LogEvent.PENDING_MARKED, registros=len(keys), estado=settings.pending_status)
