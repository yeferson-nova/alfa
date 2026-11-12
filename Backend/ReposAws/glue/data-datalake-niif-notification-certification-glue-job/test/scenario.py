from dataclasses import dataclass
from typing import Any

from builders import (
    GLOBAL_CONFIG_DATA,
    DEFAULT_RESPONSES,
    build_parameters,
    build_record,
    copy_case_config,
)
from fakes import (
    InMemoryEventLogger,
    FakeQueryRunner,
    SequentialTokenGenerator,
    FakeNotifier,
    FixedClock,
    FakeCompositeRepository,
    FakeConfigRepository,
    FakeLinkRepository,
    FakeNotificationRepository,
)
from certification_notifier.application.composite import CompositeService
from certification_notifier.application.queries import QueryService
from certification_notifier.application.links import LinkService
from certification_notifier.application.notify_certification import Dependencies, NotifyCertification
from certification_notifier.domain.models import LinkPolicy


@dataclass
class Scenario:
    case_config: NotifyCertification
    config: FakeConfigRepository
    notifications: FakeNotificationRepository
    links: FakeLinkRepository
    runner: FakeQueryRunner
    notifier: FakeNotifier
    composite: FakeCompositeRepository
    logger: InMemoryEventLogger
    clock: FixedClock


def build_scenario(
    parameters: Any = None,
    raw_case_config: Any = None,
    records: Any = None,
    responses: Any = None,
    composite: Any = None,
    notifier: Any = None,
    templates: Any = None,
) -> Scenario:
    parameters = parameters or build_parameters()
    raw_case_config = raw_case_config if raw_case_config is not None else copy_case_config()
    documents = {
        (parameters.config_bucket, parameters.global_key): GLOBAL_CONFIG_DATA,
        (parameters.config_bucket, parameters.case_key): raw_case_config,
    }
    assets_bucket = GLOBAL_CONFIG_DATA["buckets"]["assets"]
    documents.update({(assets_bucket, key): text for key, text in (templates or {}).items()})
    config = FakeConfigRepository(documents)
    notifications = FakeNotificationRepository(
        records if records is not None else [build_record()]
    )
    links = FakeLinkRepository()
    runner = FakeQueryRunner(responses if responses is not None else dict(DEFAULT_RESPONSES))
    notifier = notifier or FakeNotifier()
    composite_repository = composite or FakeCompositeRepository()
    logger = InMemoryEventLogger()
    clock = FixedClock()
    policy = LinkPolicy(
        url_base=parameters.redirect_url,
        table=parameters.links_table,
        validity_days=parameters.link_validity_days,
        token_length=parameters.link_token_length,
    )
    dependencies = Dependencies(
        config=config,
        notifications=notifications,
        notifier=notifier,
        queries=QueryService(runner, logger),
        links=LinkService(
            links, clock, SequentialTokenGenerator(), logger, policy, parameters.use_case_id, parameters.cert_code
        ),
        composite=CompositeService(composite_repository, clock, logger),
        clock=clock,
        logger=logger,
    )
    return Scenario(
        case_config=NotifyCertification(parameters, dependencies),
        config=config,
        notifications=notifications,
        links=links,
        runner=runner,
        notifier=notifier,
        composite=composite_repository,
        logger=logger,
        clock=clock,
    )
