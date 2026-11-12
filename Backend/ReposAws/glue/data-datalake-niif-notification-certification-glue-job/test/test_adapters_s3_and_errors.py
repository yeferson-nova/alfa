import io
import json

import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from botocore.response import StreamingBody
from botocore.stub import Stubber

from certification_notifier.adapters.outbound.aws_errors import translate, translating
from certification_notifier.adapters.outbound.s3_config import S3ConfigRepository
from certification_notifier.domain.errors import (
    ConcurrencyError,
    ConfigurationError,
    AccessDeniedError,
    PlatformError,
)


def _body(text: str) -> StreamingBody:
    data = text.encode("utf-8")
    return StreamingBody(io.BytesIO(data), len(data))


def _repository():
    client = boto3.client("s3", region_name="us-east-1")
    return S3ConfigRepository(client), Stubber(client)


def test_load_json_reads_object_normalizing_key():
    repository, stub = _repository()
    stub.add_response(
        "get_object",
        {"Body": _body(json.dumps({"a": "ñ"}))},
        {"Bucket": "bucket", "Key": "ruta/archivo.json"},
    )
    with stub:
        assert repository.load_json("bucket", "s3://bucket/ruta/archivo.json") == {"a": "ñ"}


def test_load_json_with_invalid_content_fails_as_configuration():
    repository, stub = _repository()
    stub.add_response("get_object", {"Body": _body("{no es json")}, {"Bucket": "b", "Key": "k"})
    with stub, pytest.raises(ConfigurationError) as error:
        repository.load_json("b", "k")
    assert error.value.code == "NTF-CFG-001"


def test_load_json_requires_an_object_document():
    repository, stub = _repository()
    stub.add_response("get_object", {"Body": _body("[1, 2]")}, {"Bucket": "b", "Key": "k"})
    with stub, pytest.raises(ConfigurationError):
        repository.load_json("b", "k")


def test_load_json_translates_missing_object_to_configuration():
    repository, stub = _repository()
    stub.add_client_error("get_object", service_error_code="NoSuchKey")
    with stub, pytest.raises(ConfigurationError) as error:
        repository.load_json("b", "k")
    assert error.value.code == "NTF-CFG-008"


def test_load_json_rejects_other_bucket_paths_without_calling_aws():
    repository, stub = _repository()
    with stub, pytest.raises(ConfigurationError) as error:
        repository.load_json("b", "s3://otro/k")
    assert error.value.code == "NTF-CFG-006"


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "x"}}, "Operacion")


@pytest.mark.parametrize(
    ("code", "error_class", "error_code", "recoverable"),
    [
        ("AccessDenied", AccessDeniedError, "NTF-PER-001", False),
        ("AccessDeniedException", AccessDeniedError, "NTF-PER-001", False),
        ("NoSuchBucket", ConfigurationError, "NTF-CFG-008", False),
        ("ResourceNotFoundException", ConfigurationError, "NTF-CFG-008", False),
        ("ConditionalCheckFailedException", ConcurrencyError, "NTF-CON-002", False),
        ("TransactionCanceledException", ConcurrencyError, "NTF-CON-002", False),
        ("ThrottlingException", PlatformError, "NTF-PLA-001", True),
        ("TooManyRequestsException", PlatformError, "NTF-PLA-001", True),
        ("MessageRejected", PlatformError, "NTF-PLA-004", False),
    ],
)
def test_translate_classifies_aws_errors(code, error_class, error_code, recoverable):
    translated = translate(_client_error(code), "context")
    assert isinstance(translated, error_class)
    assert translated.code == error_code
    assert translated.recoverable is recoverable
    assert code in translated.message


def test_translate_without_error_code_is_platform():
    without_code = ClientError({}, "Operacion")
    assert translate(without_code, "ctx").code == "NTF-PLA-004"


def test_translating_turns_network_errors_into_recoverable_platform():
    with pytest.raises(PlatformError) as error:
        with translating("ctx"):
            raise EndpointConnectionError(endpoint_url="https://ejemplo.invalid")
    assert error.value.code == "NTF-PLA-001"
    assert error.value.recoverable is True


def test_translating_does_not_change_other_errors():
    with pytest.raises(ValueError):
        with translating("ctx"):
            raise ValueError("otro")


def test_load_text_reads_template_normalizing_key():
    repository, stubber = _repository()
    stubber.add_response(
        "get_object", {"Body": _body("<p>ñ $x</p>")}, {"Bucket": "bucket", "Key": "email-template/a.html"}
    )
    with stubber:
        assert repository.load_text("bucket", "s3://bucket/email-template/a.html") == "<p>ñ $x</p>"


def test_load_text_translates_missing_object_to_configuration():
    repository, stubber = _repository()
    stubber.add_client_error("get_object", service_error_code="NoSuchKey")
    with stubber, pytest.raises(ConfigurationError) as error:
        repository.load_text("b", "k")
    assert error.value.code == "NTF-CFG-008"
