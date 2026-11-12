from contextlib import contextmanager
from typing import Iterator

from botocore.exceptions import BotoCoreError, ClientError

from certification_notifier.domain.errors import (
    ConcurrencyError,
    ConfigurationError,
    NotificationError,
    AccessDeniedError,
    PlatformError,
)

PERMISSION_CODES = frozenset({"AccessDenied", "AccessDeniedException", "UnrecognizedClientException"})
NOT_FOUND_CODES = frozenset(
    {"NoSuchKey", "NoSuchBucket", "ResourceNotFoundException", "NotFoundException"}
)
CONCURRENCY_CODES = frozenset({"ConditionalCheckFailedException", "TransactionCanceledException"})
THROTTLING_CODES = frozenset(
    {
        "ThrottlingException",
        "Throttling",
        "TooManyRequestsException",
        "ProvisionedThroughputExceededException",
        "RequestLimitExceeded",
        "LimitExceededException",
        "SlowDown",
    }
)


def translate(error: ClientError, context: str) -> NotificationError:
    code = error.response.get("Error", {}).get("Code", "")
    detail = f"{context}: {code}"
    if code in PERMISSION_CODES:
        return AccessDeniedError("NTF-PER-001", detail)
    if code in NOT_FOUND_CODES:
        return ConfigurationError("NTF-CFG-008", detail)
    if code in CONCURRENCY_CODES:
        return ConcurrencyError("NTF-CON-002", detail)
    if code in THROTTLING_CODES:
        return PlatformError("NTF-PLA-001", detail, recoverable=True)
    return PlatformError("NTF-PLA-004", detail)


@contextmanager
def translating(context: str) -> Iterator[None]:
    try:
        yield
    except ClientError as error:
        raise translate(error, context) from error
    except BotoCoreError as error:
        raise PlatformError("NTF-PLA-001", f"{context}: {error}", recoverable=True) from error
