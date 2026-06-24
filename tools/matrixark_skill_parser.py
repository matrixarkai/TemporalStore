#!/usr/bin/env python3
"""MatrixArk skill parsing helpers.

Skills are structured prompt/tool resources: a manifest plus bounded sections
that can be retrieved into a ContextPack. The parser accepts simple SKILL.md
front matter, section lists, and skill bundle directories.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from tools.matrixark_resource_parser import ParsedResourceChunk, compact_ws, parse_resource, slugify, stable_hash, token_estimate
except ModuleNotFoundError:
    from matrixark_resource_parser import ParsedResourceChunk, compact_ws, parse_resource, slugify, stable_hash, token_estimate


Json = dict[str, Any]

VALID_SKILL_STATUS = {"active", "disabled", "deprecated"}
VALID_SKILL_PRECEDENCE = {"low", "normal", "high", "critical"}
VALID_OWNER_SCOPE = {"user", "team", "tenant", "account", "global"}
SKIP_BUNDLE_DIRS = {".git", ".hg", ".svn", "node_modules", "__pycache__", ".venv", "venv", "target", "build", "dist"}
MAX_BUNDLE_FILES = 200
DEFAULT_MAX_SKILL_TEXT_CHARS = int(os.environ.get("MATRIXARK_SKILL_MAX_TEXT_CHARS", str(2 * 1024 * 1024)))


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
    max_text_chars: int = DEFAULT_MAX_SKILL_TEXT_CHARS,
) -> ParsedSkill:
    if max_text_chars <= 0:
        raise ValueError("max_text_chars must be positive")
    raw_uri_text = str(raw_uri)
    bundle_files: list[str] = []
    bundle_manifest: Json = {}
    bundle_root = ""
    if text is None:
        path = Path(raw_uri_text)
        if path.is_dir():
            bundle_root = str(path)
            bundle_files = _bundle_file_list(path)
            bundle_manifest = _load_bundle_manifest(path)
            skill_path = _find_skill_entrypoint(path)
            raw_uri_text = str(skill_path)
        else:
            skill_path = path
        if skill_path.stat().st_size > max_text_chars:
            raise ValueError(f"skill file too large: {skill_path} has {skill_path.stat().st_size} bytes, max is {max_text_chars}")
        text = skill_path.read_text(encoding="utf-8-sig")
    if len(text) > max_text_chars:
        raise ValueError(f"skill text too large: {len(text)} chars, max is {max_text_chars}")
    front_matter, body = _split_front_matter(text)
    front_matter = {**bundle_manifest, **front_matter}
    skill_text = body.strip() or text.strip()
    nested_metadata = front_matter.get("metadata") if isinstance(front_matter.get("metadata"), dict) else {}
    name = str(front_matter.get("name") or front_matter.get("id") or _first_heading(skill_text) or Path(raw_uri_text).stem or "skill")
    description = str(
        front_matter.get("description")
        or front_matter.get("summary")
        or nested_metadata.get("short-description")
        or nested_metadata.get("description")
        or _first_paragraph(skill_text)
        or name
    )
    triggers = _list_field(front_matter.get("triggers") or front_matter.get("activation") or front_matter.get("when")) or _extract_section_list(skill_text, "triggers") or _extract_section_list(skill_text, "when to use")
    allowed_tools = _list_field(front_matter.get("allowed_tools") or front_matter.get("tools") or front_matter.get("allowed-tools")) or _extract_section_list(skill_text, "allowed tools") or _extract_section_list(skill_text, "tools")
    owner_scope = _validated_choice(str(front_matter.get("owner_scope") or front_matter.get("scope") or front_matter.get("visibility") or "user"), VALID_OWNER_SCOPE, "user")
    examples = _list_field(front_matter.get("examples")) or _extract_section_list(skill_text, "examples")
    permissions = _list_field(front_matter.get("permissions") or front_matter.get("scopes")) or _extract_section_list(skill_text, "permissions")
    inputs = _list_field(front_matter.get("inputs") or front_matter.get("input_schema")) or _extract_section_list(skill_text, "inputs")
    outputs = _list_field(front_matter.get("outputs") or front_matter.get("output_schema")) or _extract_section_list(skill_text, "outputs")
    precedence = _validated_choice(str(front_matter.get("precedence") or "normal"), VALID_SKILL_PRECEDENCE, "normal")
    status = _validated_choice(str(front_matter.get("status") or "active"), VALID_SKILL_STATUS, "active")
    version = str(front_matter.get("version") or "1")
    category = str(front_matter.get("category") or front_matter.get("domain") or "general")
    skill_hash = stable_hash(f"skill:{raw_uri_text}:{name}:{version}")
    chunks = parse_resource(
        raw_uri_text,
        resource_type="skill",
        text=skill_text,
        chunk_hash_base=chunk_hash_base,
        max_inline_text_chars=max_text_chars,
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
        "bundle_root": bundle_root,
        "bundle_manifest": bundle_manifest,
        "section_count": len(chunks),
    }
    metadata["embedding_text"] = skill_embedding_text(
        name=name,
        description=description,
        triggers=metadata["triggers"],
        allowed_tools=metadata["allowed_tools"],
        category=category,
        permissions=metadata["permissions"],
        skill_text=skill_text,
    )
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


def _validated_choice(value: str, allowed: set[str], default: str) -> str:
    normalized = value.strip().lower()
    return normalized if normalized in allowed else default


def _bundle_file_list(path: Path) -> list[str]:
    files = []
    for item in sorted(child for child in path.rglob("*") if child.is_file()):
        relative = item.relative_to(path)
        if item.is_symlink():
            continue
        if any(part.startswith(".") or part in SKIP_BUNDLE_DIRS for part in relative.parts):
            continue
        files.append(str(relative).replace("\\", "/"))
        if len(files) >= MAX_BUNDLE_FILES:
            break
    return files


def _find_skill_entrypoint(path: Path) -> Path:
    for candidate in ["SKILL.md", "skill.md", "README.md", "readme.md"]:
        skill_path = path / candidate
        if skill_path.exists():
            return skill_path
    raise FileNotFoundError(f"skill bundle has no SKILL.md or README.md: {path}")


def _load_bundle_manifest(path: Path) -> Json:
    for rel in ["skill.json", "manifest.json", ".codex-plugin/plugin.json"]:
        manifest_path = path / rel
        if not manifest_path.exists():
            continue
        try:
            data = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        normalized: Json = {}
        for key in ["name", "id", "description", "summary", "version", "status", "precedence", "category", "domain", "owner_scope", "scope", "visibility"]:
            if key in data:
                normalized[key] = data[key]
        if "triggers" in data:
            normalized["triggers"] = data["triggers"]
        if "tools" in data:
            normalized["tools"] = data["tools"]
        if "allowed_tools" in data:
            normalized["allowed_tools"] = data["allowed_tools"]
        normalized["manifest_uri"] = str(manifest_path)
        return normalized
    return {}


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


def skill_embedding_text(
    *,
    name: str,
    description: str,
    triggers: list[str],
    allowed_tools: list[str],
    category: str,
    permissions: list[str],
    skill_text: str,
) -> str:
    return compact_ws(
        "\n".join(
            [
                f"skill: {name}",
                f"description: {description}",
                "triggers: " + ", ".join(triggers[:12]),
                "allowed_tools: " + ", ".join(allowed_tools[:12]),
                "permissions: " + ", ".join(permissions[:12]),
                f"category: {category}",
                "instructions: " + skill_text[:1800],
            ]
        )
    )


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
        if raw_line.startswith((" ", "\t")) and ":" in raw_line and current_key:
            nested_key, nested_value = raw_line.strip().split(":", 1)
            if isinstance(fields.get(current_key), list) and not fields[current_key]:
                fields[current_key] = {}
            if isinstance(fields.get(current_key), dict):
                fields[current_key][nested_key.strip()] = nested_value.strip().strip('"').strip("'")
                continue
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
