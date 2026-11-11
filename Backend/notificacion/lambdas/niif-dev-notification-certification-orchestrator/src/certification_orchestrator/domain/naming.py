from typing import Mapping

from certification_orchestrator.domain.errors import ConfigurationError, DataError

SEPARATOR = "/"
FORBIDDEN_FILE_NAMES = frozenset({"", ".", ".."})


def report_folder(folders: Mapping[str, str], use_case_id: str) -> str:
    if use_case_id not in folders:
        raise ConfigurationError(
            "ORQ-CFG-002", f"El caso {use_case_id} no tiene carpeta en certification.report_folders"
        )
    return folders[use_case_id].strip(SEPARATOR)


def valid_file_name(name_file: str | None) -> str:
    name = (name_file or "").strip()
    if name in FORBIDDEN_FILE_NAMES or SEPARATOR in name or "\\" in name:
        raise DataError("ORQ-DAT-003", "El registro no tiene un nameFile valido (vacio o con rutas)")
    return name


def join_key(*parts: str) -> str:
    return SEPARATOR.join(part.strip(SEPARATOR) for part in parts if part.strip(SEPARATOR))


def source_candidates(folder: str, use_case_id: str, name_file: str, use_historic: bool) -> tuple[str, ...]:
    keys = [join_key(folder, name_file)]
    if use_historic:
        keys.append(join_key(use_case_id, name_file))
    return tuple(dict.fromkeys(keys))


def transport_key(prefix: str, folder: str, name_file: str) -> str:
    return join_key(prefix, folder, name_file)


def rejected_key(folder: str, rejected_folder: str, name_file: str) -> str:
    return join_key(folder, rejected_folder, name_file)
