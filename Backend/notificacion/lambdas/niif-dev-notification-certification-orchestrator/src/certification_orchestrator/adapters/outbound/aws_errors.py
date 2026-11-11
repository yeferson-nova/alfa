from contextlib import contextmanager
from typing import Iterator

from botocore.exceptions import BotoCoreError, ClientError

from certification_orchestrator.domain.errors import (
    AccessDeniedError,
    ConcurrencyError,
    ConfigurationError,
    OrchestratorError,
    PlatformError,
)

ACCESS_DENIED_CODES = frozenset({"AccessDenied", "AccessDeniedException", "UnrecognizedClientException", "403"})
NOT_FOUND_CODES = frozenset(
    {"NoSuchKey", "NoSuchBucket", "ResourceNotFoundException", "NotFoundException", "EntityNotFoundException"}
)
CONCURRENCY_CODES = frozenset({"TransactionConflictException", "TransactionInProgressException"})
THROTTLING_CODES = frozenset(
    {
        "ThrottlingException",
        "Throttling",
        "TooManyRequestsException",
        "ProvisionedThroughputExceededException",
        "RequestLimitExceeded",
        "LimitExceededException",
        "SlowDown",
        "ConcurrentRunsExceededException",
        "ResourceNumberLimitExceededException",
        "InternalServiceException",
        "InternalServerError",
    }
)


def error_code(error: ClientError) -> str:
    return str(error.response.get("Error", {}).get("Code", ""))


def translate(error: ClientError, operation: str) -> OrchestratorError:
    code = error_code(error)
    detail = f"{operation}: {code}"
    if code in ACCESS_DENIED_CODES:
        return AccessDeniedError("ORQ-PER-001", detail)
    if code in NOT_FOUND_CODES:
        return ConfigurationError("ORQ-CFG-004", detail)
    if code in CONCURRENCY_CODES:
        return ConcurrencyError("ORQ-CON-001", detail)
    if code in THROTTLING_CODES:
        return PlatformError("ORQ-PLA-001", detail, recoverable=True)
    return PlatformError("ORQ-PLA-004", detail)


@contextmanager
def translating(operation: str) -> Iterator[None]:
    try:
        yield
    except ClientError as error:
        raise translate(error, operation) from error
    except BotoCoreError as error:
        raise PlatformError("ORQ-PLA-001", f"{operation}: {error}", recoverable=True) from error
