#!/usr/bin/env python3
"""MatrixArk skill parsing helpers.

Skills are treated as structured resources: a small manifest plus bounded
chunks that can be retrieved by agents. This mirrors the useful OpenViking
idea of tool/skill context while keeping TemporalStore as the serving store.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from tools.matrixark_resource_parser import ParsedResourceChunk, parse_resource, stable_hash, token_estimate
except ModuleNotFoundError:
    from matrixark_resource_parser import ParsedResourceChunk, parse_resource, stable_hash, token_estimate


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
    if text is None:
        path = Path(raw_uri_text)
        if path.is_dir():
            skill_path = path / "SKILL.md"
            raw_uri_text = str(skill_path)
        else:
            skill_path = path
        text = skill_path.read_text(encoding="utf-8")
    front_matter, body = _split_front_matter(text)
    name = str(front_matter.get("name") or _first_heading(body) or Path(raw_uri_text).stem or "skill")
    description = str(front_matter.get("description") or _first_paragraph(body) or name)
    skill_text = body.strip() or text.strip()
    triggers = _list_field(front_matter.get("triggers")) or _extract_section_list(skill_text, "triggers")
    allowed_tools = _list_field(front_matter.get("allowed_tools") or front_matter.get("tools")) or _extract_section_list(skill_text, "allowed tools")
    owner_scope = str(front_matter.get("owner_scope") or front_matter.get("scope") or "user")
    examples = _list_field(front_matter.get("examples")) or _extract_section_list(skill_text, "examples")
    precedence = str(front_matter.get("precedence") or "normal")
    status = str(front_matter.get("status") or "active")
    version = str(front_matter.get("version") or "1")
    skill_hash = stable_hash(f"skill:{raw_uri_text}:{name}")
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
        "skill_description": description,
        "triggers": triggers,
        "allowed_tools": allowed_tools,
        "owner_scope": owner_scope,
        "examples": examples[:8],
        "precedence": precedence,
        "status": status,
        "version": version,
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


def _list_field(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [part.strip() for part in re.split(r",|;", str(value)) if part.strip()]


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
        if line.startswith(('-', '*')):
            values.append(line[1:].strip())
        elif line and not line.startswith('#'):
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
    for line in match.group(1).splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("-") and current_key:
            fields.setdefault(current_key, [])
            if isinstance(fields[current_key], list):
                fields[current_key].append(stripped[1:].strip().strip('"'))
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        current_key = key.strip()
        value = value.strip()
        if not value:
            fields[current_key] = []
        elif value.startswith("[") and value.endswith("]"):
            fields[current_key] = [
                item.strip().strip('"').strip("'")
                for item in value[1:-1].split(",")
                if item.strip()
            ]
        else:
            fields[current_key] = value.strip('"')
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
