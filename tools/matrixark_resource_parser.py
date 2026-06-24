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
import os
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any


Json = dict[str, Any]

SUPPORTED_TEXT_TYPES = {"md", "txt", "log", "html", "csv", "tsv", "json", "jsonl", "skill"}
SUPPORTED_BINARY_TYPES = {"pdf", "docx", "png", "jpg", "jpeg", "webp", "gif"}
SUPPORTED_FILE_TYPES = SUPPORTED_TEXT_TYPES | SUPPORTED_BINARY_TYPES
SUPPORTED_DIRECTORY_TYPES = {"md", "txt", "pdf", "html", "csv", "tsv", "json", "jsonl", "docx", "skill"}
SKIP_DIRECTORY_NAMES = {".git", ".hg", ".svn", "node_modules", "__pycache__", ".venv", "venv", "target", "build", "dist"}
DEFAULT_MAX_FILE_BYTES = int(os.environ.get("MATRIXARK_RESOURCE_MAX_FILE_BYTES", str(20 * 1024 * 1024)))
DEFAULT_MAX_DIRECTORY_FILES = int(os.environ.get("MATRIXARK_RESOURCE_MAX_DIRECTORY_FILES", "256"))
DEFAULT_MAX_DIRECTORY_DEPTH = int(os.environ.get("MATRIXARK_RESOURCE_MAX_DIRECTORY_DEPTH", "8"))
DEFAULT_MAX_TOTAL_CHUNKS = int(os.environ.get("MATRIXARK_RESOURCE_MAX_TOTAL_CHUNKS", "2048"))
DEFAULT_MAX_INLINE_TEXT_CHARS = int(os.environ.get("MATRIXARK_RESOURCE_MAX_INLINE_TEXT_CHARS", str(5 * 1024 * 1024)))


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
    max_file_bytes: int = DEFAULT_MAX_FILE_BYTES,
    max_directory_files: int = DEFAULT_MAX_DIRECTORY_FILES,
    max_directory_depth: int = DEFAULT_MAX_DIRECTORY_DEPTH,
    max_total_chunks: int = DEFAULT_MAX_TOTAL_CHUNKS,
    max_inline_text_chars: int = DEFAULT_MAX_INLINE_TEXT_CHARS,
) -> list[ParsedResourceChunk]:
    """Parse supported resources into bounded serving chunks.

    Supported local/inline resource types: md, txt, pdf, html, csv, tsv, json,
    jsonl, docx, and skill. Unsupported binary files fail loudly instead of
    silently storing useless bytes.
    """
    if max_chunk_chars < 256:
        raise ResourceParserError("max_chunk_chars must be >= 256")
    if overlap_chars < 0 or overlap_chars >= max_chunk_chars:
        raise ResourceParserError("overlap_chars must be non-negative and smaller than max_chunk_chars")
    if max_total_chunks <= 0:
        raise ResourceParserError("max_total_chunks must be positive")
    if max_inline_text_chars <= 0:
        raise ResourceParserError("max_inline_text_chars must be positive")
    if text is not None and len(text) > max_inline_text_chars:
        raise ResourceParserError(f"inline resource text too large: {len(text)} chars, max is {max_inline_text_chars}")
    raw_uri_text = str(raw_uri)
    kind = infer_resource_type(raw_uri_text, resource_type)
    units = _parse_units(
        raw_uri_text,
        kind,
        text,
        max_file_bytes=max_file_bytes,
        max_directory_files=max_directory_files,
        max_directory_depth=max_directory_depth,
        max_inline_text_chars=max_inline_text_chars,
    )

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
            if len(chunks) >= max_total_chunks:
                raise ResourceParserError(f"resource produced more than max_total_chunks={max_total_chunks}")
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


def _parse_units(
    raw_uri: str,
    kind: str,
    text: str | None,
    *,
    max_file_bytes: int = DEFAULT_MAX_FILE_BYTES,
    max_directory_files: int = DEFAULT_MAX_DIRECTORY_FILES,
    max_directory_depth: int = DEFAULT_MAX_DIRECTORY_DEPTH,
    max_inline_text_chars: int = DEFAULT_MAX_INLINE_TEXT_CHARS,
) -> list[Json]:
    if text is None:
        path = Path(raw_uri)
        if not path.exists():
            raise ResourceParserError(f"resource path does not exist: {raw_uri}")
        if path.is_dir():
            return _parse_directory_units(path, max_file_bytes=max_file_bytes, max_directory_files=max_directory_files, max_directory_depth=max_directory_depth)
        if kind not in SUPPORTED_FILE_TYPES:
            raise ResourceParserError(f"unsupported resource type for local file: {kind}")
        _ensure_file_size(path, max_file_bytes)
        if kind == "pdf":
            return _parse_pdf_units(path, raw_uri)
        if kind == "docx":
            return _parse_docx_units(path, raw_uri)
        text = _read_text_file(path, max_file_bytes)
    if text is not None and len(text) > max_inline_text_chars:
        raise ResourceParserError(f"inline resource text too large: {len(text)} chars, max is {max_inline_text_chars}")
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


def _ensure_file_size(path: Path, max_file_bytes: int) -> None:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise ResourceParserError(f"cannot stat resource file {path}: {exc}") from exc
    if size > max_file_bytes:
        raise ResourceParserError(f"resource file too large: {path} has {size} bytes, max is {max_file_bytes}")


def _read_text_file(path: Path, max_file_bytes: int) -> str:
    _ensure_file_size(path, max_file_bytes)
    try:
        return path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8-sig", errors="replace")


def _markdown_units(text: str) -> list[Json]:
    document_metadata, text = _split_simple_front_matter(text)
    document_title = ""
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
                    "document_metadata": document_metadata,
                    "document_title": document_title or (heading_stack[0][1] if heading_stack else ""),
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
            if current_level == 1 and not document_title:
                document_title = current_heading
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
                "document_metadata": document_metadata,
                "document_title": document_title,
            }
        )
    return units


def _split_simple_front_matter(text: str) -> tuple[Json, str]:
    if not text.startswith("---"):
        return {}, text
    match = re.match(r"^---\s*\n(.*?)\n---\s*\n?(.*)$", text, flags=re.DOTALL)
    if not match:
        return {}, text
    metadata: Json = {}
    current_key = ""
    for raw_line in match.group(1).splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("-") and current_key:
            metadata.setdefault(current_key, [])
            if isinstance(metadata[current_key], list):
                metadata[current_key].append(stripped[1:].strip().strip('"').strip("'"))
            continue
        if ":" not in raw_line:
            continue
        key, value = raw_line.split(":", 1)
        current_key = key.strip()
        value = value.strip()
        if not value:
            metadata[current_key] = []
        elif value.startswith("[") and value.endswith("]"):
            metadata[current_key] = [item.strip().strip('"').strip("'") for item in value[1:-1].split(",") if item.strip()]
        else:
            metadata[current_key] = value.strip('"').strip("'")
    return metadata, match.group(2)


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


def _parse_directory_units(
    path: Path,
    *,
    max_file_bytes: int,
    max_directory_files: int,
    max_directory_depth: int,
) -> list[Json]:
    units: list[Json] = []
    parsed_files = 0
    skipped_files = 0
    for child in sorted(item for item in path.rglob("*") if item.is_file()):
        relative = child.relative_to(path)
        if child.is_symlink():
            skipped_files += 1
            continue
        if len(relative.parts) > max_directory_depth:
            skipped_files += 1
            continue
        if any(part.startswith(".") or part in SKIP_DIRECTORY_NAMES for part in relative.parts):
            skipped_files += 1
            continue
        child_kind = infer_resource_type(str(child))
        if child_kind not in SUPPORTED_DIRECTORY_TYPES:
            skipped_files += 1
            continue
        if parsed_files >= max_directory_files:
            raise ResourceParserError(f"directory resource has more than max_directory_files={max_directory_files}: {path}")
        try:
            child_units = _parse_units(str(child), child_kind, None, max_file_bytes=max_file_bytes, max_directory_files=max_directory_files, max_directory_depth=max_directory_depth)
        except ResourceParserError:
            skipped_files += 1
            continue
        parsed_files += 1
        relative_path = str(relative).replace("\\", "/")
        for unit in child_units:
            unit = dict(unit)
            unit["child_uri"] = str(child)
            unit["relative_path"] = relative_path
            unit["child_resource_type"] = child_kind
            unit["directory_root"] = str(path)
            units.append(unit)
    if not units:
        raise ResourceParserError(f"directory resource has no supported files: {path}")
    for unit in units:
        unit["directory_file_count"] = parsed_files
        unit["directory_skipped_files"] = skipped_files
    return units


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
    base_uri = str(metadata.get("child_uri") or raw_uri)
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
    return f"{base_uri}#{suffix}"
