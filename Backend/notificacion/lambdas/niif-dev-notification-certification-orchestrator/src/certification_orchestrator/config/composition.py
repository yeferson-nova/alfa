import os
import uuid
from dataclasses import dataclass
from functools import lru_cache, partial
from typing import Any, Callable, Mapping

import boto3
from niif_observability import StructuredLogger

from certification_orchestrator.adapters.outbound.dynamodb_notifications import DynamoNotificationRepository
from certification_orchestrator.adapters.outbound.glue_jobs import GlueJobLauncher
from certification_orchestrator.adapters.outbound.s3_store import S3ObjectStore
from certification_orchestrator.adapters.outbound.ses_alerts import SesAlertSender
from certification_orchestrator.adapters.outbound.system import JsonEventLog, SystemClock
from certification_orchestrator.application.ports import Dependencies, EventLog
from certification_orchestrator.application.process_stream import ProcessStream
from certification_orchestrator.application.reprocess_pending import ReprocessPending
from certification_orchestrator.config.settings import read_reprocess_pointer

COMPONENT = "niif_orquestadora_certificacion"


@dataclass(frozen=True)
class Context:
    log: EventLog
    stream: ProcessStream
    reprocess: Callable[[], ReprocessPending]


@dataclass(frozen=True)
class _Clients:
    s3: Any
    dynamodb: Any
    glue: Any
    ses: Any


@lru_cache(maxsize=1)
def _clients() -> _Clients:
    session = boto3.session.Session()
    return _Clients(
        s3=session.client("s3"),
        dynamodb=session.client("dynamodb"),
        glue=session.client("glue"),
        ses=session.client("sesv2"),
    )


def _reprocessor(dependencies: Dependencies, token: str, environ: Mapping[str, str]) -> ReprocessPending:
    return ReprocessPending(dependencies, token, read_reprocess_pointer(environ))


def build_context(request_id: str | None, environ: Mapping[str, str] | None = None) -> Context:
    clients = _clients()
    clock = SystemClock()
    log = JsonEventLog(StructuredLogger(COMPONENT, partial(print, flush=True), clock.now_utc))
    log.bind(request_id=request_id)
    objects = S3ObjectStore(clients.s3)
    dependencies = Dependencies(
        configs=objects,
        notifications=DynamoNotificationRepository(clients.dynamodb),
        objects=objects,
        jobs=GlueJobLauncher(clients.glue),
        alerts=SesAlertSender(clients.ses),
        clock=clock,
        log=log,
    )
    token = request_id or uuid.uuid4().hex
    variables = os.environ if environ is None else environ
    return Context(
        log=log,
        stream=ProcessStream(dependencies, token),
        reprocess=partial(_reprocessor, dependencies, token, variables),
    )
