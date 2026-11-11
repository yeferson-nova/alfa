from contextlib import contextmanager
from typing import Iterator

from botocore.exceptions import BotoCoreError, ClientError

from certification_job.domain.errors import (
    AccessDeniedError,
    CertificationError,
    ConfigurationError,
    PlatformError,
)

ACCESS_DENIED_CODES = frozenset({"AccessDenied", "AccessDeniedException", "UnrecognizedClientException"})
NOT_FOUND_CODES = frozenset({"NoSuchKey", "NoSuchBucket", "ResourceNotFoundException", "NotFoundException"})
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


def translate(error: ClientError, operation: str) -> CertificationError:
    code = error.response.get("Error", {}).get("Code", "")
    detail = f"{operation}: {code}"
    if code in ACCESS_DENIED_CODES:
        return AccessDeniedError("CER-PER-001", detail)
    if code in NOT_FOUND_CODES:
        return ConfigurationError("CER-CFG-007", detail)
    if code in THROTTLING_CODES:
        return PlatformError("CER-PLA-001", detail, recoverable=True)
    return PlatformError("CER-PLA-004", detail)


@contextmanager
def translating(operation: str) -> Iterator[None]:
    try:
        yield
    except ClientError as error:
        raise translate(error, operation) from error
    except BotoCoreError as error:
        raise PlatformError("CER-PLA-001", f"{operation}: {error}", recoverable=True) from error
