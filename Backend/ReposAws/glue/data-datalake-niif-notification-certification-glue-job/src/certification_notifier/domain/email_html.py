import html
from string import Template
from typing import Mapping, Sequence

from certification_notifier.domain.constants import TONE_WARNING, TONE_INFO
from certification_notifier.domain.models import Notice, QueryResult
from certification_notifier.domain.texts import QUERY_ERROR_TEXT, NO_FILES_TEXT, NO_DATA_TEXT

NOTICE_MARKER = "$avisoHtml"
TEXT_STYLE = (
    "margin:0 0 8px 0; font-family:Arial, Helvetica, sans-serif; "
    "font-size:14px; line-height:20px; color:#1f2a27;"
)
TABLE_STYLE = (
    "border-collapse:collapse; border:1px solid #d9e1de; "
    "font-family:Arial, Helvetica, sans-serif; font-size:14px;"
)
HEADER_STYLE = (
    "background-color:#eef2f0; color:#5b6763; font-size:12px; font-weight:bold; "
    "text-transform:uppercase; text-align:left; padding:10px 12px; "
    "border:1px solid #d9e1de; white-space:nowrap;"
)
CELL_STYLE = (
    "color:#1f2a27; text-align:left; padding:10px 12px; "
    "border:1px solid #d9e1de; white-space:nowrap;"
)
LINK_STYLE = "color:#00875f; text-decoration:underline;"
NOTICE_CELL_STYLE = "padding:20px 24px 0 24px;"
NOTICE_COLORS = {
    TONE_INFO: ("#174ea6", "#e8f0fe", "#a8c3f5"),
    TONE_WARNING: ("#8a4b00", "#fff4e0", "#f1c27d"),
}


def paragraph(text: str) -> str:
    return f'<p style="{TEXT_STYLE}">{html.escape(text)}</p>'


def query_error_paragraph() -> str:
    return paragraph(QUERY_ERROR_TEXT)


def table_block(content: str) -> str:
    return f'<div style="margin:0 0 12px 0;">{content}</div>'


def _cell(value: str) -> str:
    return f'<td style="{CELL_STYLE}">{html.escape(value)}</td>'


def _row(headers: tuple[str, ...], row: tuple[str, ...]) -> str:
    cells = "".join(_cell(row[i] if i < len(row) else "") for i in range(len(headers)))
    return f"<tr>{cells}</tr>"


def table_html(result: QueryResult) -> str:
    if not result.rows:
        return paragraph(NO_DATA_TEXT)
    headers = "".join(
        f'<th style="{HEADER_STYLE}">{html.escape(header)}</th>' for header in result.headers
    )
    rows = "".join(_row(result.headers, row) for row in result.rows)
    return (
        '<table role="presentation" border="0" cellpadding="0" cellspacing="0" '
        f'style="{TABLE_STYLE}"><tr>{headers}</tr>{rows}</table>'
    )


def detail_html(files: Sequence[tuple[str, str]]) -> str:
    if not files:
        return paragraph(NO_FILES_TEXT)
    return "".join(
        f'<p style="{TEXT_STYLE} word-break:break-all;">'
        f'<a href="{html.escape(link)}" target="_blank" rel="noopener noreferrer" '
        f'style="{LINK_STYLE}">{html.escape(name)}</a></p>'
        for name, link in files
    )


def notice_box(notice: Notice) -> str:
    color, background, border = NOTICE_COLORS[notice.tone]
    lines = "<br>".join(html.escape(line) for line in notice.text.splitlines() if line.strip())
    return (
        '<div style="margin:0 0 8px 0; padding:12px 16px; '
        "font-family:Arial, Helvetica, sans-serif; font-size:14px; line-height:20px; "
        f"color:{color}; background-color:{background}; border:1px solid {border}; "
        f'border-radius:8px;">{lines}</div>'
    )


def notices_html(notices: Sequence[Notice]) -> str:
    if not notices:
        return ""
    boxes = "".join(notice_box(notice) for notice in notices)
    return f'<tr><td class="pad" style="{NOTICE_CELL_STYLE}">{boxes}</td></tr>'


def template_supports_notices(template: str, notices: Sequence[Notice]) -> bool:
    return not notices or NOTICE_MARKER in template


def html_fragments(
    summary: str,
    totals: str,
    files: Sequence[tuple[str, str]],
    certify_link: str,
    no_certify_link: str,
) -> dict[str, str]:
    return {
        "tableTotal": totals,
        "tableSummary": summary,
        "tableDetail": detail_html(files),
        "certificarLink": html.escape(certify_link),
        "nocertificarLink": html.escape(no_certify_link),
    }


def fill_template(
    template: str,
    variables: Mapping[str, str],
    fragments: Mapping[str, str],
    notices: Sequence[Notice],
) -> str:
    escaped = {key: html.escape(str(value)) for key, value in variables.items()}
    return Template(template).safe_substitute({**escaped, **fragments, "avisoHtml": notices_html(notices)})
