from datetime import datetime

CERTIFICATION_EMAIL_TYPE = "cert"
NORMAL_FORMAT = "%Y%m%d-%H%M"
REGENERATION_FORMAT = "%Y%m%d-%H%M%S"


def build_cert_code(
    period: str,
    email_type: str,
    now: datetime,
    regeneration: bool,
    unique_suffix: str,
) -> str | None:
    if email_type.lower() != CERTIFICATION_EMAIL_TYPE:
        return None
    if regeneration:
        return f"{period}-{now.strftime(REGENERATION_FORMAT)}-{unique_suffix}"
    return f"{period}-{now.strftime(NORMAL_FORMAT)}"
