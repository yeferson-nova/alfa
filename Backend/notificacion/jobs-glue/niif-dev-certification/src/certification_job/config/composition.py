import time
from dataclasses import dataclass
from functools import partial

import boto3
from niif_observability import StructuredLogger

from certification_job.adapters.outbound.athena import AthenaQueryRunner
from certification_job.adapters.outbound.dynamodb_notifications import DynamoNotificationRepository
from certification_job.adapters.outbound.s3_store import S3ObjectStore
from certification_job.adapters.outbound.system import JsonEventLog, SystemClock, UuidGenerator
from certification_job.application.generate_certification import Dependencies, GenerateCertification
from certification_job.application.ports import EventLog
from certification_job.domain.configuration import read_global_config
from certification_job.domain.models import JobParameters
from certification_job.domain.naming import s3_uri

COMPONENT = "niif_certificacion"


@dataclass(frozen=True)
class Context:
    use_case: GenerateCertification
    log: EventLog


def build_context(parameters: JobParameters) -> Context:
    session = boto3.session.Session()
    objects = S3ObjectStore(session.client("s3"))
    config = read_global_config(objects.load_json(parameters.config_bucket, parameters.global_key))
    clock = SystemClock(config.timezone)
    log = JsonEventLog(StructuredLogger(COMPONENT, partial(print, flush=True), clock.now_utc))
    queries = AthenaQueryRunner(
        session.client("athena"),
        time.sleep,
        config.catalog,
        s3_uri(config.assets_bucket, f"{config.athena_output_path}/"),
        config.certification.athena_wait,
    )
    dependencies = Dependencies(
        configs=objects,
        queries=queries,
        objects=objects,
        notifications=DynamoNotificationRepository(session.client("dynamodb")),
        clock=clock,
        ids=UuidGenerator(),
        sleep=time.sleep,
        log=log,
    )
    return Context(use_case=GenerateCertification(parameters, config, dependencies), log=log)
