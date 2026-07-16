#!/usr/bin/env python3
"""Native lookup helpers for TemporalStore MatrixArk adapters."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_core import Json, canonical_scope_key, json
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, canonical_scope_key, json


def native_index_ref_hashes(target: Any, *, scope: Json, secondary_index_groups: list[set[str]] | None) -> Json:
    self = target
    scope_key = canonical_scope_key(scope)
    groups = secondary_index_groups or []
    if not scope_key or not groups:
        return {"ref_hashes": set(), "postings_found": 0, "index_terms": [], "posting_buckets": [], "eligible": False, "reason": "missing_scope_or_filters"}
    batch_hget = getattr(self._client, "batch_hget", None)
    if not callable(batch_hget):
        return {"ref_hashes": set(), "postings_found": 0, "index_terms": [], "posting_buckets": [], "eligible": False, "reason": "backend_has_no_batch_hget"}
    index_terms = sorted({term for group in groups for term in group if term})
    entries = [{"key": self._context_index_lookup_key(scope_key), "field": term} for term in index_terms]
    try:
        rows = batch_hget(entries)
    except Exception as exc:
        return {"ref_hashes": set(), "postings_found": 0, "index_terms": index_terms, "posting_buckets": [], "eligible": False, "reason": f"index_lookup_failed:{exc}"}
    ref_hashes: set[int] = set()
    posting_buckets: set[int] = set()
    postings_found = 0
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        value = row.get("value")
        if not value:
            continue
        try:
            decoded = json.loads(str(value))
        except Exception:
            continue
        raw_refs = decoded.get("ref_hashes", []) if isinstance(decoded, dict) else []
        raw_buckets = decoded.get("posting_buckets", []) if isinstance(decoded, dict) else []
        if isinstance(raw_refs, list):
            postings_found += 1
            for value in raw_refs:
                try:
                    ref_hash = int(value)
                except (TypeError, ValueError):
                    continue
                if ref_hash:
                    ref_hashes.add(ref_hash)
        if isinstance(raw_buckets, list):
            for value in raw_buckets:
                try:
                    bucket = int(value)
                except (TypeError, ValueError):
                    continue
                if bucket:
                    posting_buckets.add(bucket)
    return {
        "ref_hashes": ref_hashes,
        "postings_found": postings_found,
        "index_terms": index_terms,
        "posting_buckets": sorted(posting_buckets),
        "eligible": bool(ref_hashes),
        "reason": "ok" if ref_hashes else "no_matching_postings",
    }


def native_locations_for_refs(target: Any, ref_hashes: set[int]) -> Json:
    self = target
    batch_hget = getattr(self._client, "batch_hget", None)
    if not callable(batch_hget) or not ref_hashes:
        return {"locations": [], "locator_rows": 0}
    entries = [{"key": self._context_ref_locator_key(), "field": str(ref_hash)} for ref_hash in sorted(ref_hashes)]
    try:
        rows = batch_hget(entries)
    except Exception:
        return {"locations": [], "locator_rows": 0}
    locations: list[Json] = []
    resource_versions: set[str] = set()
    seen: set[tuple[str, str]] = set()
    locator_rows = 0
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        value = row.get("value")
        if not value:
            continue
        try:
            decoded = json.loads(str(value))
        except Exception:
            continue
        raw_locations = decoded.get("locations", []) if isinstance(decoded, dict) else []
        raw_versions = decoded.get("resource_versions", []) if isinstance(decoded, dict) else []
        if isinstance(raw_versions, list):
            resource_versions.update(str(value) for value in raw_versions if str(value))
        if not isinstance(raw_locations, list):
            continue
        locator_rows += 1
        for location in raw_locations:
            if not isinstance(location, dict):
                continue
            key = str(location.get("key") or "")
            field = str(location.get("field") or "")
            if not key or not field or (key, field) in seen:
                continue
            locations.append({"key": key, "field": field})
            seen.add((key, field))
    return {"locations": locations, "locator_rows": locator_rows}


def load_records_from_locations(target: Any, locations: list[Json]) -> list[Json]:
    self = target
    batch_hget = getattr(self._client, "batch_hget", None)
    if not callable(batch_hget) or not locations:
        return []
    try:
        rows = batch_hget(locations)
    except Exception:
        return []
    records: list[Json] = []
    for item in rows if isinstance(rows, list) else []:
        if not isinstance(item, dict):
            continue
        payload = item.get("value", "")
        if not payload:
            continue
        try:
            decoded = json.loads(str(payload))
        except Exception:
            continue
        if isinstance(decoded, dict) and isinstance(decoded.get("record_bundle"), list):
            records.extend(row for row in decoded["record_bundle"] if isinstance(row, dict))
        elif isinstance(decoded, dict):
            records.append(decoded)
    return records

