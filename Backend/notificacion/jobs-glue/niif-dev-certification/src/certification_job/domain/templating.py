import html
from string import Template
from typing import Mapping

from certification_job.domain.errors import ConfigurationError

UNRESOLVED_VARIABLE = "CER-CFG-004"


def render(text: str, variables: Mapping[str, str], label: str) -> str:
    try:
        return Template(html.unescape(text)).substitute(variables)
    except KeyError as error:
        raise ConfigurationError(
            UNRESOLVED_VARIABLE, f"{label}: la variable {error.args[0]} no tiene valor"
        ) from error
    except ValueError as error:
        raise ConfigurationError(UNRESOLVED_VARIABLE, f"{label}: marcador invalido ({error})") from error
