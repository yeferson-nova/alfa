import csv
import gzip
import io
from typing import Iterable, Sequence

from certification_job.domain.models import HeaderSpec

UTF8_BOM = b"\xef\xbb\xbf"
GZIP_MAGIC = b"\x1f\x8b"
ENCODING = "utf-8"
ENCODING_WITH_BOM = "utf-8-sig"
QUERY_TOKEN_SEPARATOR = "_"
FILE_NAME_SEPARATOR = "-"


def query_group(file_name: str) -> str | None:
    parts = file_name.split("/")[-1].split(FILE_NAME_SEPARATOR)
    if len(parts) < 2:
        return None
    return parts[1].split(QUERY_TOKEN_SEPARATOR)[0]


def headers_for_group(specs: Sequence[HeaderSpec], group: str | None) -> tuple[str, ...] | None:
    if not group:
        return None
    spec = next((candidate for candidate in specs if candidate.query_name == group), None)
    if spec is None or not spec.headers or spec.export_disabled:
        return None
    return spec.headers


def decompress_if_needed(chunk: bytes) -> bytes:
    return gzip.decompress(chunk) if chunk[:2] == GZIP_MAGIC else chunk


def assemble(write_bom: bool, prefix: bytes, chunks: Iterable[bytes]) -> bytes:
    buffer = io.BytesIO()
    if write_bom:
        buffer.write(UTF8_BOM)
    buffer.write(prefix)
    for chunk in chunks:
        buffer.write(decompress_if_needed(chunk))
    return buffer.getvalue()


def add_headers(body: bytes, headers: Sequence[str] | None, delimiter: str) -> bytes:
    if not headers:
        return body
    had_bom = body.startswith(UTF8_BOM)
    text = body.decode(ENCODING_WITH_BOM)
    header_line = delimiter.join(headers)
    if not text.strip():
        return _encode(header_line + "\n", had_bom)
    lines = text.splitlines()
    if lines and lines[0].strip() == header_line.strip():
        return body
    return _encode(header_line + "\n" + text, had_bom)


def _encode(text: str, with_bom: bool) -> bytes:
    encoded = text.encode(ENCODING)
    return UTF8_BOM + encoded if with_bom else encoded


def count_data_rows(body: bytes, headers: Sequence[str] | None, delimiter: str) -> int | None:
    if not headers:
        return None
    text = add_headers(body, headers, delimiter).decode(ENCODING_WITH_BOM, errors="replace")
    lines = [line for line in text.splitlines() if line.strip()]
    return max(len(lines) - 1, 0)


def column_header_csv(column_names: Sequence[str]) -> bytes:
    buffer = io.StringIO(newline="")
    csv.writer(buffer).writerow(list(column_names))
    return buffer.getvalue().encode(ENCODING)
