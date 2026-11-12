import pytest

from builders import build_composite_record
from certification_notifier.domain.composite import (
    control_already_completed,
    composite_identifier,
    validate_member,
)
from certification_notifier.domain.errors import ConcurrencyError
from certification_notifier.domain.models import CompositeConfig


def test_composite_identifier_joins_group_and_period():
    config = CompositeConfig("t", "grupo", "_CONTROL", ("m1",))
    assert composite_identifier(config, "202610") == "grupo#202610"


def test_control_in_progress_is_not_completed():
    control = build_composite_record(status="NOTIFICATION_IN_PROGRESS")
    assert control_already_completed(control, "C1") is False


def test_control_completed_with_same_cert_code_is_recognized():
    control = build_composite_record(status="NOTIFICATION_COMPLETED", cert_code="C1")
    assert control_already_completed(control, "C1") is True


def test_control_completed_for_another_request_fails():
    control = build_composite_record(status="NOTIFICATION_COMPLETED", cert_code="OTRO")
    with pytest.raises(ConcurrencyError) as error:
        control_already_completed(control, "C1")
    assert error.value.code == "NTF-CON-001"


@pytest.mark.parametrize(
    "control",
    [
        None,
        build_composite_record(status="READY_TO_NOTIFY"),
        build_composite_record(status="NOTIFICATION_IN_PROGRESS", updated_at=""),
        build_composite_record(status="NOTIFICATION_IN_PROGRESS", updated_at=None),
    ],
)
def test_control_not_reserved_fails(control):
    with pytest.raises(ConcurrencyError) as error:
        control_already_completed(control, "C1")
    assert "reservado" in error.value.message


def test_ready_member_is_accepted():
    member = build_composite_record()
    assert validate_member(member, "m1", "202610") is member


@pytest.mark.parametrize(
    "member",
    [
        None,
        build_composite_record(status="CONSUMED"),
        build_composite_record(period="202609"),
    ],
)
def test_member_not_ready_fails(member):
    with pytest.raises(ConcurrencyError) as error:
        validate_member(member, "m1", "202610")
    assert "no listo" in error.value.message


@pytest.mark.parametrize("field_name", ["source_execution_id", "batch_id", "updated_at"])
def test_member_without_full_identity_fails(field_name):
    member = build_composite_record(**{field_name: None})
    with pytest.raises(ConcurrencyError) as error:
        validate_member(member, "m1", "202610")
    assert "identidad" in error.value.message
