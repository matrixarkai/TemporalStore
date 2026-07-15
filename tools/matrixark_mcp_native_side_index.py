#!/usr/bin/env python3
"""Native side-index helper functions for MatrixArk TemporalStore adapters."""

from __future__ import annotations

import json
from typing import Any, Callable

try:
    from tools.matrixark_mcp_identity import canonical_scope_key, stable_hash
    from tools.matrixark_mcp_indexing import (
        context_index_posting_bucket,
        context_index_ref_hashes,
        context_index_timestamp_key,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import canonical_scope_key, stable_hash
    from matrixark_mcp_indexing import (
        context_index_posting_bucket,
        context_index_ref_hashes,
        context_index_timestamp_key,
    )


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


def native_side_index_entries_for_bundles(
    *,
    storage_prefix: str,
    bundles: list[tuple[list[Json], str, str]],
    storage_route_for_bundle: Callable[[list[Json]], Json],
    read_hash_value: Callable[[str, str], str],
) -> list[Json]:
    """Build sidecar lookup rows so native retrieval can avoid broad scans."""
    lookup_updates: dict[tuple[str, str], Json] = {}
    locator_updates: dict[int, list[Json]] = {}
    placement_updates: dict[tuple[str, str], Json] = {}
    route_by_hash_field: dict[tuple[str, str], Json] = {}
    for bundle, record_key, record_id in bundles:
        location = {"key": record_key, "field": record_id}
        route = storage_route_for_bundle(bundle)
        for record in bundle:
            node_hash = record.get("node_hash")
            scope_key_for_placement = str(record.get("scope_key") or "")
            if not scope_key_for_placement:
                scope = record.get("scope") if isinstance(record.get("scope"), dict) else {}
                scope_key_for_placement = canonical_scope_key(scope) if scope else ""
            if scope_key_for_placement and node_hash is not None:
                try:
                    placement_node_hash = int(node_hash)
                except (TypeError, ValueError):
                    placement_node_hash = 0
                if placement_node_hash:
                    placement_key = (
                        context_placement_lookup_key(storage_prefix, scope_key_for_placement),
                        str(placement_node_hash),
                    )
                    placement_update = placement_updates.setdefault(
                        placement_key, {"locations": [], "resource_versions": set()}
                    )
                    placement_update["locations"].append(location)
                    resource_version = str(record.get("resource_version") or "")
                    if resource_version:
                        placement_update["resource_versions"].add(resource_version)
                    if route:
                        route_by_hash_field.setdefault(placement_key, route)
            for ref_hash in context_index_ref_hashes(record):
                locator_updates.setdefault(ref_hash, []).append(location)
                if route:
                    route_by_hash_field.setdefault(
                        (context_ref_locator_key(storage_prefix), str(ref_hash)), route
                    )
            if record.get("record_type") != "context_index":
                continue
            index_name = str(record.get("index_name") or "").strip()
            if not index_name:
                continue
            scope_key = str(record.get("scope_key") or "")
            if not scope_key:
                scope = record.get("scope") if isinstance(record.get("scope"), dict) else {}
                scope_key = canonical_scope_key(scope) if scope else ""
            ref_hashes = context_index_ref_hashes(record)
            if scope_key and ref_hashes:
                lookup_key = (context_index_lookup_key(storage_prefix, scope_key), index_name)
                update = lookup_updates.setdefault(lookup_key, {"ref_hashes": [], "posting_buckets": set()})
                update["ref_hashes"].extend(ref_hashes)
                update["posting_buckets"].add(
                    context_index_posting_bucket(context_index_timestamp_key(record))
                )
                if route:
                    route_by_hash_field.setdefault(lookup_key, route)

    entries: list[Json] = []
    for (key, field), update in lookup_updates.items():
        new_refs = update.get("ref_hashes", []) if isinstance(update, dict) else []
        new_buckets = update.get("posting_buckets", set()) if isinstance(update, dict) else set()
        existing_value = read_hash_value(key, field)
        merged_refs = merge_ref_hashes(existing_value, new_refs)
        existing_buckets: set[int] = set()
        if existing_value:
            try:
                decoded_existing = json.loads(existing_value)
            except Exception:
                decoded_existing = {}
            raw_buckets = decoded_existing.get("posting_buckets", []) if isinstance(decoded_existing, dict) else []
            if isinstance(raw_buckets, list):
                for value in raw_buckets:
                    try:
                        bucket = int(value)
                    except (TypeError, ValueError):
                        continue
                    if bucket:
                        existing_buckets.add(bucket)
        for value in new_buckets if isinstance(new_buckets, set) else set():
            try:
                bucket = int(value)
            except (TypeError, ValueError):
                continue
            if bucket:
                existing_buckets.add(bucket)
        entries.append(
            {
                "key": key,
                "field": field,
                "value": json.dumps(
                    {"ref_hashes": merged_refs, "posting_buckets": sorted(existing_buckets)},
                    separators=(",", ":"),
                ),
                "storage_route": route_by_hash_field.get((key, field), {}),
            }
        )
    locator_key = context_ref_locator_key(storage_prefix)
    for ref_hash, new_locations in locator_updates.items():
        field = str(ref_hash)
        merged_locations = merge_ref_locations(read_hash_value(locator_key, field), new_locations)
        entries.append(
            {
                "key": locator_key,
                "field": field,
                "value": json.dumps({"locations": merged_locations}, separators=(",", ":")),
                "storage_route": route_by_hash_field.get((locator_key, field), {}),
            }
        )
    for (key, field), update in placement_updates.items():
        new_locations = update.get("locations", []) if isinstance(update, dict) else []
        new_versions = update.get("resource_versions", set()) if isinstance(update, dict) else set()
        existing_value = read_hash_value(key, field)
        merged_locations = merge_ref_locations(existing_value, new_locations)
        merged_versions = merge_resource_versions(existing_value, new_versions if isinstance(new_versions, set) else set())
        entries.append(
            {
                "key": key,
                "field": field,
                "value": json.dumps(
                    {"locations": merged_locations, "resource_versions": merged_versions},
                    separators=(",", ":"),
                ),
                "storage_route": route_by_hash_field.get((key, field), {}),
            }
        )
    return entries
