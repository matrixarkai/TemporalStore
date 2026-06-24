#!/usr/bin/env python3
"""MatrixArk resource parsing helpers.

The parser keeps raw bytes outside TemporalStore and converts resources into
small, cited, embedding-ready chunks. The shape intentionally mirrors the useful
parts of context-filesystem systems: stable refs, section hierarchy, L0-friendly
metadata, and bounded serving payloads.
"""

from __future__ import annotations

import csv
import hashlib
import html
import json
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any


Json = dict[str, Any]


class ResourceParserError(RuntimeError):
    pass


@dataclass(frozen=True)
class ParsedResourceChunk:
    chunk_hash: int
    source_ref: str
    text: str
    token_estimate: int
    metadata: Json

    def to_json(self) -> Json:
        return {
            "chunk_hash": self.chunk_hash,
            "source_ref": self.source_ref,
            "text": self.text,
            "token_estimate": self.token_estimate,
            "metadata": self.metadata,
        }


def stable_hash(value: str) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFF_FFFF_FFFF_FFFF


def infer_resource_type(raw_uri: str, resource_type: str | None = None) -> str:
    if resource_type:
        kind = resource_type.lower().lstrip(".")
    else:
        suffix = Path(raw_uri).suffix.lower().lstrip(".")
        kind = suffix or "txt"
    aliases = {
        "markdown": "md",
        "text": "txt",
        "htm": "html",
        "jpeg": "jpg",
        "jsonlines": "jsonl",
        "skill": "skill",
    }
    return aliases.get(kind, kind)


def token_estimate(text: str) -> int:
    return max(1, len(re.findall(r"[A-Za-z0-9_]+", text)))


def slugify(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower())
    return value.strip("-") or "section"


def compact_ws(text: str) -> str:
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def keywords_for_text(text: str, limit: int = 12) -> list[str]:
    stop = {
        "the", "and", "for", "that", "with", "from", "this", "will", "are", "was", "were", "has", "have",
        "should", "into", "when", "where", "what", "which", "your", "their", "about", "after", "before",
    }
    seen: set[str] = set()
    out: list[str] = []
    for token in re.findall(r"[A-Za-z][A-Za-z0-9_]{2,}", text.lower()):
        if token in stop or token in seen:
            continue
        seen.add(token)
        out.append(token)
        if len(out) >= limit:
            break
    return out


def parse_resource(
    raw_uri: str | Path,
    *,
    resource_type: str | None = None,
    text: str | None = None,
    max_chunk_chars: int = 1400,
    overlap_chars: int = 120,
    chunk_hash_base: int | None = None,
) -> list[ParsedResourceChunk]:
    """Parse supported resources into bounded serving chunks.

    Supported local/inline resource types: md, txt, pdf, html, csv, tsv, json,
    jsonl, docx, and skill. Unsupported binary files fail loudly instead of
    silently storing useless bytes.
    """
    raw_uri_text = str(raw_uri)
    kind = infer_resource_type(raw_uri_text, resource_type)
    units = _parse_units(raw_uri_text, kind, text)

    chunks: list[ParsedResourceChunk] = []
    for unit_index, unit in enumerate(units):
        unit_text = compact_ws(str(unit.get("text", "")))
        for split_index, piece in enumerate(_split_text(unit_text, max_chunk_chars, overlap_chars)):
            piece = compact_ws(piece)
            if not piece:
                continue
            metadata = {key: value for key, value in unit.items() if key != "text"}
            metadata.update(
                {
                    "resource_type": kind,
                    "chunk_index": len(chunks),
                    "unit_index": unit_index,
                    "split_index": split_index,
                    "char_count": len(piece),
                    "content_hash": content_hash(piece),
                    "keywords": keywords_for_text(piece),
                }
            )
            source_ref = _source_ref(raw_uri_text, metadata)
            metadata["citation"] = source_ref
            chunk_hash = (
                chunk_hash_base + len(chunks)
                if chunk_hash_base is not None
                else stable_hash(f"resource_chunk:{source_ref}:{metadata['content_hash']}")
            )
            chunks.append(
                ParsedResourceChunk(
                    chunk_hash=chunk_hash,
                    source_ref=source_ref,
                    text=piece,
                    token_estimate=token_estimate(piece),
                    metadata=metadata,
                )
            )
    return chunks


def _parse_units(raw_uri: str, kind: str, text: str | None) -> list[Json]:
    if text is None:
        path = Path(raw_uri)
        if not path.exists():
            raise ResourceParserError(f"resource path does not exist: {raw_uri}")
        if kind == "pdf":
            return _parse_pdf_units(path, raw_uri)
        if kind == "docx":
            return _parse_docx_units(path, raw_uri)
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            text = path.read_text(encoding="utf-8", errors="replace")
    if kind in {"md", "skill"}:
        return _markdown_units(text)
    if kind == "html":
        return _html_units(text, raw_uri)
    if kind in {"csv", "tsv"}:
        return _delimited_units(text, raw_uri, delimiter="\t" if kind == "tsv" else ",")
    if kind == "jsonl":
        return _jsonl_units(text, raw_uri)
    if kind == "json":
        return _json_units(text, raw_uri)
    if kind in {"txt", "log"}:
        return _paragraph_units(text, raw_uri)
    if kind in {"png", "jpg", "jpeg", "webp", "gif"}:
        return _binary_stub_units(raw_uri, kind)
    return _paragraph_units(text, raw_uri)


def _markdown_units(text: str) -> list[Json]:
    units: list[Json] = []
    heading_stack: list[tuple[int, str]] = []
    current_heading = "document"
    current_level = 0
    current_start_line = 1
    buffer: list[str] = []
    in_code = False

    def path_slugs() -> list[str]:
        return [slugify(name) for _, name in heading_stack] or ["document"]

    def flush(end_line: int) -> None:
        content = "\n".join(buffer).strip()
        if content:
            units.append(
                {
                    "text": content,
                    "heading": current_heading,
                    "heading_slug": slugify(current_heading),
                    "heading_level": current_level,
                    "heading_path": [name for _, name in heading_stack] or ["document"],
                    "heading_path_slugs": path_slugs(),
                    "line_start": current_start_line,
                    "line_end": max(current_start_line, end_line),
                    "unit_kind": "markdown_section",
                }
            )
        buffer.clear()

    for line_no, line in enumerate(text.splitlines(), start=1):
        if line.strip().startswith("```") or line.strip().startswith("~~~"):
            in_code = not in_code
            buffer.append(line.rstrip())
            continue
        match = None if in_code else re.match(r"^(#{1,6})\s+(.+?)\s*$", line)
        if match:
            flush(line_no - 1)
            current_level = len(match.group(1))
            current_heading = match.group(2).strip()
            heading_stack = [item for item in heading_stack if item[0] < current_level]
            heading_stack.append((current_level, current_heading))
            current_start_line = line_no
            buffer.append(line.strip())
        else:
            buffer.append(line.rstrip())
    flush(len(text.splitlines()) or 1)
    if not units and text.strip():
        units.append(
            {
                "text": text.strip(),
                "heading": "document",
                "heading_slug": "document",
                "heading_level": 0,
                "heading_path": ["document"],
                "heading_path_slugs": ["document"],
                "line_start": 1,
                "line_end": len(text.splitlines()) or 1,
                "unit_kind": "markdown_document",
            }
        )
    return units


def _paragraph_units(text: str, raw_uri: str) -> list[Json]:
    units: list[Json] = []
    cursor_line = 1
    for index, part in enumerate(re.split(r"\n\s*\n+", text)):
        paragraph = part.strip()
        line_count = max(1, part.count("\n") + 1)
        if paragraph:
            units.append(
                {
                    "text": paragraph,
                    "paragraph_index": index,
                    "line_start": cursor_line,
                    "line_end": cursor_line + line_count - 1,
                    "section": Path(raw_uri).name or "document",
                    "unit_kind": "paragraph",
                }
            )
        cursor_line += line_count
    if not units and text.strip():
        units.append({"text": text.strip(), "paragraph_index": 0, "section": Path(raw_uri).name or "document", "unit_kind": "paragraph"})
    return units


class _TextHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self.title = ""
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"p", "div", "section", "article", "br", "li", "h1", "h2", "h3", "h4", "h5", "h6"}:
            self.parts.append("\n")
        if tag == "title":
            self._in_title = True

    def handle_endtag(self, tag: str) -> None:
        if tag in {"p", "div", "section", "article", "li", "h1", "h2", "h3", "h4", "h5", "h6"}:
            self.parts.append("\n")
        if tag == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        value = html.unescape(data).strip()
        if not value:
            return
        if self._in_title:
            self.title = value
        self.parts.append(value + " ")


def _html_units(text: str, raw_uri: str) -> list[Json]:
    parser = _TextHTMLParser()
    parser.feed(text)
    plain = compact_ws("".join(parser.parts))
    units = _paragraph_units(plain, raw_uri)
    for unit in units:
        unit["unit_kind"] = "html_text"
        if parser.title:
            unit["title"] = parser.title
    return units


def _delimited_units(text: str, raw_uri: str, *, delimiter: str) -> list[Json]:
    rows = list(csv.DictReader(text.splitlines(), delimiter=delimiter))
    if not rows:
        return _paragraph_units(text, raw_uri)
    units = []
    for index, row in enumerate(rows):
        clean = {str(k): str(v) for k, v in row.items() if k is not None and str(v).strip()}
        rendered = "; ".join(f"{key}: {value}" for key, value in clean.items())
        if rendered:
            units.append({"text": rendered, "row_index": index, "columns": list(clean.keys()), "unit_kind": "table_row"})
    return units or _paragraph_units(text, raw_uri)


def _jsonl_units(text: str, raw_uri: str) -> list[Json]:
    units = []
    for index, line in enumerate(text.splitlines()):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
            rendered = json.dumps(obj, ensure_ascii=False, sort_keys=True)
            units.append({"text": rendered, "record_index": index, "unit_kind": "jsonl_record"})
        except json.JSONDecodeError:
            units.append({"text": line.strip(), "record_index": index, "unit_kind": "jsonl_text"})
    return units or _paragraph_units(text, raw_uri)


def _json_units(text: str, raw_uri: str) -> list[Json]:
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        return _paragraph_units(text, raw_uri)
    if isinstance(obj, list):
        return [
            {"text": json.dumps(item, ensure_ascii=False, sort_keys=True), "record_index": index, "unit_kind": "json_record"}
            for index, item in enumerate(obj)
        ]
    if isinstance(obj, dict):
        return [{"text": json.dumps(obj, ensure_ascii=False, sort_keys=True), "record_index": 0, "unit_kind": "json_document"}]
    return [{"text": str(obj), "record_index": 0, "unit_kind": "json_value"}]


def _binary_stub_units(raw_uri: str, kind: str) -> list[Json]:
    return [
        {
            "text": f"Binary {kind} resource at {raw_uri}. Use a VLM or OCR provider to extract visual content.",
            "unit_kind": "binary_stub",
            "needs_vlm": True,
        }
    ]


def _parse_pdf_units(path: Path, raw_uri: str) -> list[Json]:
    data = path.read_bytes()
    if not data.startswith(b"%PDF"):
        units = _paragraph_units(data.decode("utf-8", errors="replace"), raw_uri)
        for unit in units:
            unit["unit_kind"] = "pdf_text_fallback"
        return units
    errors: list[str] = []
    try:
        from pypdf import PdfReader  # type: ignore

        reader = PdfReader(str(path))
        units = []
        for index, page in enumerate(reader.pages):
            page_text = (page.extract_text() or "").strip()
            if page_text:
                units.append({"text": page_text, "page": index + 1, "unit_kind": "pdf_page"})
        if units:
            return units
    except Exception as exc:  # pragma: no cover - optional dependency path.
        errors.append(f"pypdf: {exc}")
    try:
        import pdfplumber  # type: ignore

        units = []
        with pdfplumber.open(str(path)) as pdf:
            for index, page in enumerate(pdf.pages):
                page_text = (page.extract_text() or "").strip()
                if page_text:
                    units.append({"text": page_text, "page": index + 1, "unit_kind": "pdf_page"})
        if units:
            return units
    except Exception as exc:  # pragma: no cover - optional dependency path.
        errors.append(f"pdfplumber: {exc}")
    raise ResourceParserError(
        "Unable to parse PDF resource. Install pypdf or pdfplumber, or provide extracted text. "
        + "; ".join(errors)
    )


def _parse_docx_units(path: Path, raw_uri: str) -> list[Json]:
    try:
        import docx  # type: ignore
    except Exception as exc:  # pragma: no cover - optional dependency path.
        raise ResourceParserError(f"Unable to parse DOCX resource. Install python-docx: {exc}")
    document = docx.Document(str(path))
    units = []
    current_heading = "document"
    buffer: list[str] = []

    def flush(index: int) -> None:
        content = "\n".join(buffer).strip()
        if content:
            units.append({"text": content, "heading": current_heading, "heading_slug": slugify(current_heading), "paragraph_index": index, "unit_kind": "docx_section"})
        buffer.clear()

    for index, paragraph in enumerate(document.paragraphs):
        style = (paragraph.style.name or "").lower() if paragraph.style else ""
        text = paragraph.text.strip()
        if not text:
            continue
        if style.startswith("heading"):
            flush(index)
            current_heading = text
            buffer.append(text)
        else:
            buffer.append(text)
    flush(len(document.paragraphs))
    return units or _paragraph_units("\n\n".join(p.text for p in document.paragraphs), raw_uri)


def _split_text(text: str, max_chunk_chars: int, overlap_chars: int) -> list[str]:
    text = compact_ws(text)
    if len(text) <= max_chunk_chars:
        return [text]
    pieces: list[str] = []
    start = 0
    while start < len(text):
        end = min(len(text), start + max_chunk_chars)
        if end < len(text):
            boundary = max(text.rfind("\n\n", start, end), text.rfind(". ", start, end), text.rfind("; ", start, end))
            if boundary > start + max_chunk_chars // 2:
                end = boundary + 1
        piece = text[start:end].strip()
        if piece:
            pieces.append(piece)
        if end >= len(text):
            break
        start = max(end - overlap_chars, start + 1)
    return pieces


def _source_ref(raw_uri: str, metadata: Json) -> str:
    if "page" in metadata:
        suffix = f"page={metadata['page']}"
    elif "heading_path_slugs" in metadata:
        suffix = "heading=" + "/".join(str(part) for part in metadata["heading_path_slugs"])
    elif "heading_slug" in metadata:
        suffix = f"heading={metadata['heading_slug']}"
    elif "row_index" in metadata:
        suffix = f"row={metadata['row_index']}"
    elif "record_index" in metadata:
        suffix = f"record={metadata['record_index']}"
    elif "paragraph_index" in metadata:
        suffix = f"paragraph={metadata['paragraph_index']}"
    else:
        suffix = f"chunk={metadata.get('chunk_index', 0)}"
    if metadata.get("split_index", 0):
        suffix += f"&part={metadata['split_index']}"
    return f"{raw_uri}#{suffix}"
