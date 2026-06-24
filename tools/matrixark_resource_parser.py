#!/usr/bin/env python3
"""MatrixArk resource parsing helpers."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
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
        return resource_type.lower().lstrip(".")
    suffix = Path(raw_uri).suffix.lower().lstrip(".")
    return suffix or "txt"


def token_estimate(text: str) -> int:
    return max(1, len(re.findall(r"[A-Za-z0-9_]+", text)))


def slugify(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower())
    return value.strip("-") or "section"


def parse_resource(
    raw_uri: str | Path,
    *,
    resource_type: str | None = None,
    text: str | None = None,
    max_chunk_chars: int = 1400,
    overlap_chars: int = 120,
    chunk_hash_base: int | None = None,
) -> list[ParsedResourceChunk]:
    """Parse md/txt/pdf input into bounded serving chunks.

    Raw file bytes stay outside TemporalStore. The returned chunks carry stable
    source refs and compact metadata for ResourceChunk records.
    """
    raw_uri_text = str(raw_uri)
    kind = infer_resource_type(raw_uri_text, resource_type)
    if text is None:
        path = Path(raw_uri_text)
        if kind == "pdf":
            units = _parse_pdf_units(path, raw_uri_text)
        else:
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                text = path.read_text(encoding="utf-8", errors="replace")
            units = _text_units(text, kind, raw_uri_text)
    else:
        units = _text_units(text, kind, raw_uri_text)

    chunks: list[ParsedResourceChunk] = []
    for unit_index, unit in enumerate(units):
        for split_index, piece in enumerate(_split_text(unit["text"], max_chunk_chars, overlap_chars)):
            if not piece.strip():
                continue
            metadata = {key: value for key, value in unit.items() if key != "text"}
            metadata.update(
                {
                    "resource_type": kind,
                    "chunk_index": len(chunks),
                    "unit_index": unit_index,
                    "split_index": split_index,
                }
            )
            source_ref = _source_ref(raw_uri_text, metadata)
            chunk_hash = (
                chunk_hash_base + len(chunks)
                if chunk_hash_base is not None
                else stable_hash(f"resource_chunk:{source_ref}:{piece}")
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


def _text_units(text: str, kind: str, raw_uri: str) -> list[Json]:
    if kind in {"md", "markdown", "skill"}:
        return _markdown_units(text)
    return _paragraph_units(text, raw_uri)


def _markdown_units(text: str) -> list[Json]:
    units: list[Json] = []
    current_heading = "document"
    current_level = 0
    buffer: list[str] = []

    def flush() -> None:
        content = "\n".join(buffer).strip()
        if content:
            units.append(
                {
                    "text": content,
                    "heading": current_heading,
                    "heading_slug": slugify(current_heading),
                    "heading_level": current_level,
                }
            )
        buffer.clear()

    for line in text.splitlines():
        match = re.match(r"^(#{1,6})\s+(.+?)\s*$", line)
        if match:
            flush()
            current_level = len(match.group(1))
            current_heading = match.group(2).strip()
            buffer.append(line.strip())
        else:
            buffer.append(line.rstrip())
    flush()
    if not units and text.strip():
        units.append(
            {
                "text": text.strip(),
                "heading": "document",
                "heading_slug": "document",
                "heading_level": 0,
            }
        )
    return units


def _paragraph_units(text: str, raw_uri: str) -> list[Json]:
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n+", text) if part.strip()]
    if not paragraphs and text.strip():
        paragraphs = [text.strip()]
    return [
        {
            "text": paragraph,
            "paragraph_index": index,
            "section": Path(raw_uri).name or "document",
        }
        for index, paragraph in enumerate(paragraphs)
    ]


def _parse_pdf_units(path: Path, raw_uri: str) -> list[Json]:
    data = path.read_bytes()
    if not data.startswith(b"%PDF"):
        return _paragraph_units(data.decode("utf-8", errors="replace"), raw_uri)
    errors: list[str] = []
    try:
        from pypdf import PdfReader  # type: ignore

        reader = PdfReader(str(path))
        units = []
        for index, page in enumerate(reader.pages):
            page_text = (page.extract_text() or "").strip()
            if page_text:
                units.append({"text": page_text, "page": index + 1})
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
                    units.append({"text": page_text, "page": index + 1})
        if units:
            return units
    except Exception as exc:  # pragma: no cover - optional dependency path.
        errors.append(f"pdfplumber: {exc}")
    raise ResourceParserError(
        "Unable to parse PDF resource. Install pypdf or pdfplumber, or provide extracted text. "
        + "; ".join(errors)
    )


def _split_text(text: str, max_chunk_chars: int, overlap_chars: int) -> list[str]:
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    if len(text) <= max_chunk_chars:
        return [text]
    pieces: list[str] = []
    start = 0
    while start < len(text):
        end = min(len(text), start + max_chunk_chars)
        if end < len(text):
            boundary = max(text.rfind("\n\n", start, end), text.rfind(". ", start, end))
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
    elif "heading_slug" in metadata:
        suffix = f"heading={metadata['heading_slug']}"
    elif "paragraph_index" in metadata:
        suffix = f"paragraph={metadata['paragraph_index']}"
    else:
        suffix = f"chunk={metadata.get('chunk_index', 0)}"
    if metadata.get("split_index", 0):
        suffix += f"&part={metadata['split_index']}"
    return f"{raw_uri}#{suffix}"
