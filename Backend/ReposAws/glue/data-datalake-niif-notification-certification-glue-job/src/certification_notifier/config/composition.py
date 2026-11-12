import time
from dataclasses import dataclass
from functools import partial

import boto3

from certification_notifier.adapters.outbound.athena import AthenaQueryRunner
from certification_notifier.adapters.outbound.json_logger import JsonEventLogger
from certification_notifier.adapters.outbound.dynamo_composite import DynamoCompositeRepository
from certification_notifier.adapters.outbound.dynamo_links import DynamoLinkRepository
from certification_notifier.adapters.outbound.dynamo_notifications import DynamoNotificationRepository
from certification_notifier.adapters.outbound.clock import UuidTokenGenerator, SystemClock
from certification_notifier.adapters.outbound.s3_config import S3ConfigRepository
from certification_notifier.adapters.outbound.ses import SesNotifier
from certification_notifier.application.composite import CompositeService
from certification_notifier.application.queries import QueryService
from certification_notifier.application.links import LinkService
from certification_notifier.application.notify_certification import Dependencies, NotifyCertification
from certification_notifier.application.ports import EventLogger
from certification_notifier.domain.models import RunParameters, LinkPolicy

COMPONENT = "certification_notifier"


@dataclass(frozen=True)
class Context:
    use_case: NotifyCertification
    logger: EventLogger


def build_context(parameters: RunParameters) -> Context:
    session = boto3.session.Session()
    clock = SystemClock(parameters.timezone_name)
    logger = JsonEventLogger(clock, parameters.execution_id, COMPONENT, partial(print, flush=True))
    dynamodb = session.client("dynamodb")
    policy = LinkPolicy(
        url_base=parameters.redirect_url,
        table=parameters.links_table,
        validity_days=parameters.link_validity_days,
        token_length=parameters.link_token_length,
    )
    runner = AthenaQueryRunner(
        session.client("athena"),
        time.sleep,
        parameters.poll_interval_seconds,
        parameters.max_query_wait_seconds,
    )
    dependencies = Dependencies(
        config=S3ConfigRepository(session.client("s3")),
        notifications=DynamoNotificationRepository(dynamodb),
        notifier=SesNotifier(
            session.client("sesv2"), parameters.sender, parameters.configuration_set
        ),
        queries=QueryService(runner, logger),
        links=LinkService(
            DynamoLinkRepository(dynamodb),
            clock,
            UuidTokenGenerator(),
            logger,
            policy,
            parameters.use_case_id,
            parameters.cert_code,
        ),
        composite=CompositeService(DynamoCompositeRepository(dynamodb), clock, logger),
        clock=clock,
        logger=logger,
    )
    return Context(use_case=NotifyCertification(parameters, dependencies), logger=logger)
