#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Resource and skill registry helpers for MatrixArk MCP adapters."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import (
        Json,
        MatrixArkError,
        candidate_access_scope,
        now_ms,
        optional_object,
        optional_string,
        optional_string_list,
        scope_matches,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        MatrixArkError,
        candidate_access_scope,
        now_ms,
        optional_object,
        optional_string,
        optional_string_list,
        scope_matches,
    )


def latest_skill_controls(adapter: object, records: list[Json] | None = None) -> dict[int, Json]:
    controls: dict[int, Json] = {}
    for record in reversed(records if records is not None else adapter.read_all()):
        if record.get("record_type") != "skill_registry_update":
            continue
        try:
            skill_hash = int(record.get("skill_hash"))
        except (TypeError, ValueError):
            continue
        if skill_hash not in controls:
            controls[skill_hash] = record
    return controls


def list_resources(adapter: object, args: Json) -> Json:
    scope = optional_object(args, "scope")
    limit = args.get("limit", 100)
    if not isinstance(limit, int) or limit <= 0:
        raise MatrixArkError("limit must be a positive integer")
    resource_type_filter = optional_string(args, "resource_type", "")
    resources: dict[int, Json] = {}
    for record in reversed(adapter.read_all()):
        if record.get("record_type") != "resource_manifest":
            continue
        if not scope_matches(candidate_access_scope(record), scope):
            continue
        if resource_type_filter and record.get("resource_type") != resource_type_filter:
            continue
        resource_hash = int(record.get("resource_hash") or 0)
        if resource_hash in resources:
            continue
        resources[resource_hash] = {
            "resource_hash": resource_hash,
            "raw_uri": record.get("raw_uri", ""),
            "requested_raw_uri": record.get("requested_raw_uri", record.get("raw_uri", "")),
            "resource_type": record.get("resource_type", ""),
            "resource_version": record.get("resource_version", ""),
            "content_hash": record.get("content_hash", ""),
            "chunk_count": record.get("chunk_count", 0),
            "original_chunk_count": record.get("original_chunk_count", record.get("chunk_count", 0)),
            "deduped_chunk_count": record.get("deduped_chunk_count", 0),
            "superseded_chunk_count": record.get("superseded_chunk_count", 0),
            "superseded_chunk_hashes": record.get("superseded_chunk_hashes", []),
            "raw_storage_policy": record.get("raw_storage_policy", "raw_uri_only"),
            "raw_storage_mode": record.get("raw_storage_mode", "local"),
            "upload_status": record.get("upload_status", "not_required"),
            "cloud_bucket": record.get("cloud_bucket", ""),
            "cloud_key": record.get("cloud_key", ""),
            "raw_bytes_stored": bool(record.get("raw_bytes_stored", False)),
            "parse_warnings": record.get("parse_warnings", []),
            "parse_warning_count": record.get("parse_warning_count", 0),
            "async_parent_summary_required": bool(record.get("async_parent_summary_required", False)),
            "access_scope": record.get("access_scope", candidate_access_scope(record)),
            "deployment_scope": record.get("deployment_scope", "local"),
            "import_task_hash": record.get("import_task_hash", 0),
            "token_estimate": record.get("token_estimate", 0),
            "node_hash": record.get("node_hash", 0),
            "node_path": record.get("node_path", []),
            "scope": candidate_access_scope(record),
            "updated_at_ms": record.get("updated_at_ms", 0),
        }
        if len(resources) >= limit:
            break
    return {"status": "ok", "resources": list(resources.values()), "count": len(resources)}


SKILL_SCOPE_LEVELS = ("global", "account", "tenant", "team", "user")


def skill_visible_in_scope(record: Json, scope: Json | None, owner_scope: str = "") -> bool:
    """Is a skill visible from `scope`, at the level its ``owner_scope`` declares?

    A skill is authored once and meant to apply broadly: a ``global`` runbook belongs to everyone,
    a ``user`` skill follows its author into every conversation they have. Matching the FULL access
    scope -- which includes the session the skill happened to be ingested in -- made every skill
    session-local, so a global skill was invisible from the next conversation and ``owner_scope``
    was recorded but never honoured.

    Each level therefore compares only the identity it names, and ignores everything narrower:
    ``global`` matches always, ``account``/``tenant``/``team`` match that identity, and ``user``
    matches the author. An unrecognised value falls back to ``user``, the safest of the five,
    because widening visibility by accident is the failure that leaks one customer's skill to
    another."""
    level = str(owner_scope or record.get("owner_scope") or "user").strip().lower()
    if level not in SKILL_SCOPE_LEVELS:
        level = "user"
    if level == "global":
        return True
    if not isinstance(scope, dict):
        return True
    access = candidate_access_scope(record)
    if not isinstance(access, dict):
        return True
    field = {"account": "account_id", "tenant": "tenant_id", "team": "team",
             "user": "user_id"}[level]
    wanted = str(scope.get(field) or "")
    holder = str(access.get(field) or "")
    if not wanted or not holder:
        # Nothing to compare at this level -- fall back to the strict check rather than widen.
        return scope_matches(access, scope)
    return wanted == holder


def list_skills(adapter: object, args: Json) -> Json:
    scope = optional_object(args, "scope")
    limit = args.get("limit", 100)
    if not isinstance(limit, int) or limit <= 0:
        raise MatrixArkError("limit must be a positive integer")
    include_disabled = bool(args.get("include_disabled", False))
    controls = latest_skill_controls(adapter)
    skills: dict[int, Json] = {}
    for record in reversed(adapter.read_all()):
        if record.get("record_type") != "skill_manifest":
            continue
        control_for_scope = latest_skill_controls(adapter).get(int(record.get("skill_hash") or 0), {})
        if not skill_visible_in_scope(
                record, scope,
                str(control_for_scope.get("owner_scope") or record.get("owner_scope") or "")):
            continue
        skill_hash = int(record.get("skill_hash") or 0)
        if skill_hash in skills:
            continue
        control = controls.get(skill_hash, {})
        status = str(control.get("status") or record.get("status") or "active")
        if status == "disabled" and not include_disabled:
            continue
        skills[skill_hash] = {
            "skill_hash": skill_hash,
            "name": record.get("name", ""),
            "description": record.get("description", ""),
            "raw_uri": record.get("raw_uri", ""),
            "requested_raw_uri": record.get("requested_raw_uri", record.get("raw_uri", "")),
            "raw_storage_policy": record.get("raw_storage_policy", "raw_uri_only"),
            "raw_storage_mode": record.get("raw_storage_mode", "local"),
            "upload_status": record.get("upload_status", "not_required"),
            "cloud_bucket": record.get("cloud_bucket", ""),
            "cloud_key": record.get("cloud_key", ""),
            "raw_bytes_stored": bool(record.get("raw_bytes_stored", False)),
            "owner_scope": control.get("owner_scope", record.get("owner_scope", "user")),
            "version": control.get("version", record.get("version", "1")),
            "status": status,
            "precedence": control.get("precedence", record.get("precedence", "normal")),
            "triggers": control.get("triggers", record.get("triggers", [])),
            "allowed_tools": control.get("allowed_tools", record.get("allowed_tools", [])),
            "examples": record.get("examples", record.get("metadata", {}).get("examples", [])),
            "permissions": record.get("permissions", record.get("metadata", {}).get("permissions", [])),
            "inputs": record.get("inputs", record.get("metadata", {}).get("inputs", [])),
            "outputs": record.get("outputs", record.get("metadata", {}).get("outputs", [])),
            "access_scope": record.get("access_scope", candidate_access_scope(record)),
            "deployment_scope": record.get("deployment_scope", "local"),
            "node_hash": record.get("node_hash", 0),
            "node_path": record.get("node_path", []),
            "scope": candidate_access_scope(record),
            "updated_at_ms": control.get("updated_at_ms", record.get("updated_at_ms", 0)),
        }
        if len(skills) >= limit:
            break
    return {"status": "ok", "skills": list(skills.values()), "count": len(skills)}


def update_skill(adapter: object, args: Json) -> Json:
    skill_hash = args.get("skill_hash")
    if not isinstance(skill_hash, int) or skill_hash <= 0:
        raise MatrixArkError("skill_hash must be a positive integer")
    status = optional_string(args, "status", "")
    if status and status not in {"active", "disabled"}:
        raise MatrixArkError("status must be active or disabled")
    precedence = optional_string(args, "precedence", "")
    if precedence and precedence not in {"low", "normal", "high", "critical"}:
        raise MatrixArkError("precedence must be low, normal, high, or critical")
    current = None
    for record in reversed(adapter.read_all()):
        if record.get("record_type") == "skill_manifest" and record.get("skill_hash") == skill_hash:
            current = record
            break
    if current is None:
        raise MatrixArkError("skill_hash not found")
    update = {
        "record_type": "skill_registry_update",
        "skill_hash": skill_hash,
        "status": status or current.get("status", "active"),
        "precedence": precedence or current.get("precedence", "normal"),
        "owner_scope": optional_string(args, "owner_scope", str(current.get("owner_scope") or "user")),
        "version": optional_string(args, "version", str(current.get("version") or "1")),
        "triggers": optional_string_list(args, "triggers", list(current.get("triggers", []))),
        "allowed_tools": optional_string_list(args, "allowed_tools", list(current.get("allowed_tools", []))),
        "scope": current.get("scope", {}),
        "node_hash": current.get("node_hash", 0),
        "node_path": current.get("node_path", []),
        "updated_at_ms": now_ms(),
    }
    adapter.append(update)
    return {"status": "updated", **update}
