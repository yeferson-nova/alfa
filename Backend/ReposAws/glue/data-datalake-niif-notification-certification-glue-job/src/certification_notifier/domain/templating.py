import html
from string import Template
from typing import Any, Mapping

UNESCAPE_PASSES = 3


def clean_url(url: Any) -> str:
    text = str(url or "")
    for _ in range(UNESCAPE_PASSES):
        following = html.unescape(text)
        if following == text:
            break
        text = following
    return text.strip()


def substitute_vars(text: Any, variables: Mapping[str, Any], unescape: bool = False) -> str:
    value = str(text)
    if unescape:
        value = clean_url(value)
    return Template(value).safe_substitute(variables)
