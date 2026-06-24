#!/usr/bin/env python3
"""MatrixArk skill parsing helpers.

Skills are structured prompt/tool resources: a manifest plus bounded sections
that can be retrieved into a ContextPack. The parser accepts simple SKILL.md
front matter, section lists, and skill bundle directories.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from tools.matrixark_resource_parser import ParsedResourceChunk, parse_resource, slugify, stable_hash, token_estimate
except ModuleNotFoundError:
    from matrixark_resource_parser import ParsedResourceChunk, parse_resource, slugify, stable_hash, token_estimate


Json = dict[str, Any]


@dataclass(frozen=True)
class ParsedSkill:
    skill_hash: int
    name: str
    description: str
    raw_uri: str
    text: str
    token_estimate: int
    metadata: Json
    chunks: list[ParsedResourceChunk]

    def to_json(self) -> Json:
        return {
            "skill_hash": self.skill_hash,
            "name": self.name,
            "description": self.description,
            "raw_uri": self.raw_uri,
            "text": self.text,
            "token_estimate": self.token_estimate,
            "metadata": self.metadata,
            "chunks": [chunk.to_json() for chunk in self.chunks],
        }


def parse_skill(
    raw_uri: str | Path,
    *,
    text: str | None = None,
    chunk_hash_base: int | None = None,
) -> ParsedSkill:
    raw_uri_text = str(raw_uri)
    bundle_files: list[str] = []
    if text is None:
        path = Path(raw_uri_text)
        if path.is_dir():
            bundle_files = sorted(str(item.relative_to(path)) for item in path.rglob("*") if item.is_file())[:200]
            skill_path = path / "SKILL.md"
            raw_uri_text = str(skill_path)
        else:
            skill_path = path
        text = skill_path.read_text(encoding="utf-8")
    front_matter, body = _split_front_matter(text)
    skill_text = body.strip() or text.strip()
    name = str(front_matter.get("name") or front_matter.get("id") or _first_heading(skill_text) or Path(raw_uri_text).stem or "skill")
    description = str(front_matter.get("description") or front_matter.get("summary") or _first_paragraph(skill_text) or name)
    triggers = _list_field(front_matter.get("triggers") or front_matter.get("activation") or front_matter.get("when")) or _extract_section_list(skill_text, "triggers") or _extract_section_list(skill_text, "when to use")
    allowed_tools = _list_field(front_matter.get("allowed_tools") or front_matter.get("tools") or front_matter.get("allowed-tools")) or _extract_section_list(skill_text, "allowed tools") or _extract_section_list(skill_text, "tools")
    owner_scope = str(front_matter.get("owner_scope") or front_matter.get("scope") or front_matter.get("visibility") or "user")
    examples = _list_field(front_matter.get("examples")) or _extract_section_list(skill_text, "examples")
    permissions = _list_field(front_matter.get("permissions") or front_matter.get("scopes")) or _extract_section_list(skill_text, "permissions")
    inputs = _list_field(front_matter.get("inputs") or front_matter.get("input_schema")) or _extract_section_list(skill_text, "inputs")
    outputs = _list_field(front_matter.get("outputs") or front_matter.get("output_schema")) or _extract_section_list(skill_text, "outputs")
    precedence = str(front_matter.get("precedence") or "normal")
    status = str(front_matter.get("status") or "active")
    version = str(front_matter.get("version") or "1")
    category = str(front_matter.get("category") or front_matter.get("domain") or "general")
    skill_hash = stable_hash(f"skill:{raw_uri_text}:{name}:{version}")
    chunks = parse_resource(
        raw_uri_text,
        resource_type="skill",
        text=skill_text,
        chunk_hash_base=chunk_hash_base,
    )
    metadata = {
        "resource_type": "skill",
        "front_matter": front_matter,
        "skill_name": name,
        "skill_slug": slugify(name),
        "skill_description": description,
        "triggers": _normalize_terms(triggers),
        "allowed_tools": _normalize_tools(allowed_tools),
        "owner_scope": owner_scope,
        "examples": examples[:8],
        "permissions": _normalize_terms(permissions),
        "inputs": inputs[:16],
        "outputs": outputs[:16],
        "precedence": precedence,
        "status": status,
        "version": version,
        "category": category,
        "bundle_files": bundle_files,
        "section_count": len(chunks),
    }
    return ParsedSkill(
        skill_hash=skill_hash,
        name=name,
        description=description,
        raw_uri=raw_uri_text,
        text=skill_text,
        token_estimate=token_estimate(skill_text),
        metadata=metadata,
        chunks=chunks,
    )


def _normalize_terms(values: list[str]) -> list[str]:
    out = []
    seen = set()
    for value in values:
        cleaned = re.sub(r"\s+", " ", str(value)).strip()
        if cleaned and cleaned.lower() not in seen:
            seen.add(cleaned.lower())
            out.append(cleaned)
    return out


def _normalize_tools(values: list[str]) -> list[str]:
    out = []
    seen = set()
    for value in values:
        cleaned = re.sub(r"[^A-Za-z0-9_.:-]+", "_", str(value).strip()).strip("_")
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)
    return out


def _list_field(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip().strip('"').strip("'") for item in value if str(item).strip()]
    return [part.strip().strip('"').strip("'") for part in re.split(r",|;", str(value)) if part.strip()]


def _extract_section_list(text: str, heading: str) -> list[str]:
    pattern = re.compile(rf"^##+\s+{re.escape(heading)}\s*$", re.IGNORECASE | re.MULTILINE)
    match = pattern.search(text)
    if not match:
        return []
    tail = text[match.end():]
    next_heading = re.search(r"^##+\s+", tail, flags=re.MULTILINE)
    section = tail[: next_heading.start()] if next_heading else tail
    values = []
    for line in section.splitlines():
        line = line.strip()
        if line.startswith(("-", "*")):
            values.append(line[1:].strip())
        elif line and not line.startswith("#") and not line.startswith("```"):
            values.append(line)
    return values


def _split_front_matter(text: str) -> tuple[Json, str]:
    if not text.startswith("---"):
        return {}, text
    match = re.match(r"^---\s*\n(.*?)\n---\s*\n?(.*)$", text, flags=re.DOTALL)
    if not match:
        return {}, text
    fields: Json = {}
    current_key = ""
    for raw_line in match.group(1).splitlines():
        if raw_line.startswith((" ", "\t")) and raw_line.strip().startswith("-") and current_key:
            fields.setdefault(current_key, [])
            if isinstance(fields[current_key], list):
                fields[current_key].append(raw_line.strip()[1:].strip().strip('"').strip("'"))
            continue
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("-") and current_key:
            fields.setdefault(current_key, [])
            if isinstance(fields[current_key], list):
                fields[current_key].append(stripped[1:].strip().strip('"').strip("'"))
            continue
        if ":" not in raw_line:
            continue
        key, value = raw_line.split(":", 1)
        current_key = key.strip()
        value = value.strip()
        if not value:
            fields[current_key] = []
        elif value.startswith("[") and value.endswith("]"):
            fields[current_key] = [item.strip().strip('"').strip("'") for item in value[1:-1].split(",") if item.strip()]
        elif value.lower() in {"true", "false"}:
            fields[current_key] = value.lower() == "true"
        else:
            fields[current_key] = value.strip('"').strip("'")
    return fields, match.group(2)


def _first_heading(text: str) -> str:
    for line in text.splitlines():
        match = re.match(r"^#{1,6}\s+(.+?)\s*$", line)
        if match:
            return match.group(1).strip()
    return ""


def _first_paragraph(text: str) -> str:
    for part in re.split(r"\n\s*\n+", text):
        part = re.sub(r"^#{1,6}\s+", "", part.strip())
        if part:
            return part[:240]
    return ""
