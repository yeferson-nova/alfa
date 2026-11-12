from typing import Any, Mapping

from certification_notifier.domain.constants import (
    DEFAULT_CATALOG,
    DEFAULT_SENT_STATUS,
    DEFAULT_CONTROL_MEMBER,
    SUMMARY_QUERY_TYPE,
    TOTAL_QUERY_TYPE,
)
from certification_notifier.domain.recipients import normalize_recipients
from certification_notifier.domain.errors import ConfigurationError
from certification_notifier.domain.models import (
    CaseConfig,
    CompositeConfig,
    GlobalConfig,
    Query,
)


def _exists(data: Any, path: tuple[str, ...]) -> bool:
    current = data
    for key in path:
        if not isinstance(current, Mapping) or key not in current:
            return False
        current = current[key]
    return True


def _find(data: Any, path: tuple[str, ...]) -> Any:
    if not _exists(data, path):
        raise ConfigurationError("NTF-CFG-001", f"Falta la clave de configuracion: {'.'.join(path)}")
    current = data
    for key in path:
        current = current[key]
    return current


def _text(data: Any, *path: str) -> str:
    value = _find(data, path)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError("NTF-CFG-001", f"La clave debe ser texto no vacio: {'.'.join(path)}")
    return value


def _optional_text(data: Any, default_value: str, *path: str) -> str:
    if not _exists(data, path):
        return default_value
    return _text(data, *path)


def _text_or_none(data: Any, *path: str) -> str | None:
    if not _exists(data, path):
        return None
    return _text(data, *path)


def _list(data: Any, path: tuple[str, ...], code: str) -> list[Any]:
    if not _exists(data, path):
        return []
    value = _find(data, path)
    if not isinstance(value, list):
        raise ConfigurationError(code, f"La clave debe ser una lista: {'.'.join(path)}")
    return value


def read_global_config(data: Any) -> GlobalConfig:
    return GlobalConfig(
        assets_bucket=_text(data, "buckets", "assets"),
        notifications_table=_text(data, "dynamodb", "notification_table"),
        catalog=_optional_text(data, DEFAULT_CATALOG, "catalog"),
        environment_name=_text(data, "environment"),
        athena_output_prefix=_text(data, "athena", "output_path"),
        sent_status=_optional_text(data, DEFAULT_SENT_STATUS, "certification", "sent_status"),
    )


def _query(raw: Any, path: str) -> Query:
    if not isinstance(raw, Mapping):
        raise ConfigurationError("NTF-CFG-001", f"La consulta debe ser un objeto: {path}")
    return Query(
        name=str(raw.get("queryName", "")),
        database=_text(raw, "database"),
        sql=_text(raw, "query"),
    )


def _queries(data: Any, kind: str) -> tuple[Query, ...]:
    raw_items = _list(data, ("queries", kind), "NTF-CFG-001")
    queries = (_query(raw, f"queries.{kind}") for raw in raw_items)
    return tuple(sorted(queries, key=lambda query: query.name))


def _presence_member(member: Any) -> Query:
    name = member.get("name") if isinstance(member, Mapping) else None
    if not isinstance(name, str) or not name.strip():
        raise ConfigurationError("NTF-CFG-005", "Nombre de miembro ausente en dataPresence")
    return Query(name=name, database=_text(member, "database"), sql=_text(member, "query"))


def _presence(email: Any) -> tuple[Query, ...]:
    members = _list(email, ("dataPresence",), "NTF-CFG-005")
    queries = tuple(_presence_member(member) for member in members)
    names = [query.name for query in queries]
    if len(set(names)) != len(names):
        raise ConfigurationError("NTF-CFG-005", "Nombre de miembro duplicado en dataPresence")
    return queries


def _composite_config(data: Any) -> CompositeConfig | None:
    raw = data.get("compositeNotification") if isinstance(data, Mapping) else None
    if not raw or not raw.get("enabled", False):
        return None
    members = tuple(_list(raw, ("memberIds",), "NTF-CFG-004"))
    control = _optional_text(raw, DEFAULT_CONTROL_MEMBER, "controlMemberId")
    if not members or len(set(members)) != len(members) or control in members:
        raise ConfigurationError("NTF-CFG-004", "Miembros del compuesto invalidos")
    return CompositeConfig(
        table=_text(raw, "tableName"),
        group_id=_text(raw, "groupId"),
        control_id=control,
        member_ids=members,
    )


def _string_vars(email: Any) -> dict[str, str]:
    if not _exists(email, ("stringVars",)):
        return {}
    raw_items = _find(email, ("stringVars",))
    if not isinstance(raw_items, Mapping):
        raise ConfigurationError("NTF-CFG-001", "email.stringVars debe ser un objeto")
    return {str(key): str(value) for key, value in raw_items.items()}


def read_case_config(data: Any, expected_use_case_id: str) -> CaseConfig:
    declared = data.get("useCaseId") if isinstance(data, Mapping) else None
    if declared != expected_use_case_id:
        raise ConfigurationError(
            "NTF-CFG-002",
            f"El useCaseId del JSON no coincide. JSON={declared} ARG={expected_use_case_id}",
        )
    email = _find(data, ("email",))
    return CaseConfig(
        use_case_id=declared,
        subject_template=_text(email, "subject"),
        string_vars=_string_vars(email),
        recipients=normalize_recipients(_list(email, ("destinations",), "NTF-CFG-003")),
        summary_queries=_queries(data, SUMMARY_QUERY_TYPE),
        total_queries=_queries(data, TOTAL_QUERY_TYPE),
        presence_queries=_presence(email),
        composite_config=_composite_config(data),
        template_path=_text_or_none(email, "templatePath"),
    )
