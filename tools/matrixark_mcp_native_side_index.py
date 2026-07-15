#!/usr/bin/env python3
"""Native side-index helper functions for MatrixArk TemporalStore adapters."""

from __future__ import annotations

import json
from typing import Any

try:
    from tools.matrixark_mcp_identity import stable_hash
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import stable_hash


Json = dict[str, Any]


def context_index_lookup_key(storage_prefix: str, scope_key: str) -> str:
    scope_hash = stable_hash(scope_key) if scope_key else 0
    return f"{storage_prefix}:context_index_lookup:{scope_hash}"


def context_ref_locator_key(storage_prefix: str) -> str:
    return f"{storage_prefix}:context_ref_locator"


def context_placement_lookup_key(storage_prefix: str, scope_key: str) -> str:
    scope_hash = stable_hash(scope_key) if scope_key else 0
    return f"{storage_prefix}:context_placement_lookup:{scope_hash}"


def merge_ref_hashes(existing_value: str, new_refs: list[int]) -> list[int]:
    refs: list[int] = []
    seen: set[int] = set()
    if existing_value:
        try:
            decoded = json.loads(existing_value)
        except Exception:
            decoded = {}
        raw_refs = decoded.get("ref_hashes", []) if isinstance(decoded, dict) else []
        for value in raw_refs if isinstance(raw_refs, list) else []:
            try:
                ref_hash = int(value)
            except (TypeError, ValueError):
                continue
            if ref_hash and ref_hash not in seen:
                refs.append(ref_hash)
                seen.add(ref_hash)
    for ref_hash in new_refs:
        if ref_hash and ref_hash not in seen:
            refs.append(ref_hash)
            seen.add(ref_hash)
    return refs


def merge_ref_locations(existing_value: str, new_locations: list[Json]) -> list[Json]:
    locations: list[Json] = []
    seen: set[tuple[str, str]] = set()
    if existing_value:
        try:
            decoded = json.loads(existing_value)
        except Exception:
            decoded = {}
        raw_locations = decoded.get("locations", []) if isinstance(decoded, dict) else []
        for location in raw_locations if isinstance(raw_locations, list) else []:
            if not isinstance(location, dict):
                continue
            key = str(location.get("key") or "")
            field = str(location.get("field") or "")
            if not key or not field or (key, field) in seen:
                continue
            locations.append({"key": key, "field": field})
            seen.add((key, field))
    for location in new_locations:
        key = str(location.get("key") or "")
        field = str(location.get("field") or "")
        if not key or not field or (key, field) in seen:
            continue
        locations.append({"key": key, "field": field})
        seen.add((key, field))
    return locations


def merge_resource_versions(existing_value: str, new_versions: set[str]) -> list[str]:
    versions: set[str] = set()
    if existing_value:
        try:
            decoded = json.loads(existing_value)
        except Exception:
            decoded = {}
        raw_versions = decoded.get("resource_versions", []) if isinstance(decoded, dict) else []
        if isinstance(raw_versions, list):
            versions.update(str(value) for value in raw_versions if str(value))
    versions.update(str(value) for value in new_versions if str(value))
    return sorted(versions)
