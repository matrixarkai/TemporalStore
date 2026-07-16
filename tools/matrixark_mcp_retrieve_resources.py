#!/usr/bin/env python3
"""Resource metadata helpers for MatrixArk retrieval."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json, candidate_access_scope, scope_matches
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, candidate_access_scope, scope_matches


def latest_resource_metadata(records: list[Json], scope: Json) -> tuple[dict[int, str], dict[int, str]]:
    latest_resource_version_by_hash: dict[int, str] = {}
    resource_uri_by_hash: dict[int, str] = {}
    for manifest in reversed(records):
        if manifest.get("record_type") != "resource_manifest":
            continue
        if not scope_matches(candidate_access_scope(manifest), scope):
            continue
        try:
            resource_hash_key = int(manifest.get("resource_hash") or 0)
        except (TypeError, ValueError):
            resource_hash_key = 0
        raw_uri_key = str(manifest.get("raw_uri") or "")
        resource_version_key = str(manifest.get("resource_version") or "")
        if resource_hash_key:
            if raw_uri_key and resource_hash_key not in resource_uri_by_hash:
                resource_uri_by_hash[resource_hash_key] = raw_uri_key
            if resource_version_key and resource_hash_key not in latest_resource_version_by_hash:
                latest_resource_version_by_hash[resource_hash_key] = resource_version_key
    return latest_resource_version_by_hash, resource_uri_by_hash
