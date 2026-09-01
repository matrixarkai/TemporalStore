# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_LocalAdapterDashboardMixin methods split from matrixark_mcp_local_adapter.MatrixArkLocalAdapter (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
    from tools.matrixark_mcp_core import _mcp_debug_log  # import * skips underscore names
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403
    from matrixark_mcp_core import _mcp_debug_log  # import * skips underscore names

try:  # names owned by the parent module
    from tools.matrixark_mcp_local_adapter import (
    Any,
    RESOURCE_IMPORT_IGNORE_DIRS,
    context_debug_records_enabled,
    latest_async_pipeline_rows,
    memory_layer_pressure_summary,
    thread_queue,
)
except ImportError:
    from matrixark_mcp_local_adapter import (
    Any,
    RESOURCE_IMPORT_IGNORE_DIRS,
    context_debug_records_enabled,
    latest_async_pipeline_rows,
    memory_layer_pressure_summary,
    thread_queue,
)


# Record types that carry an embedding vector, and the field naming what the vector belongs to.
# Must stay equal to `_EMBEDDING_OWNER_REFS` on the retrieve path: that is the list of things that
# get scored, and this is the list of things reported as encoded. A type in one and not the other
# means the portal's encoding panel disagrees with what retrieval actually searches.
EMBEDDING_OWNER_KEY_FIELDS = {
    "context_event": ("event", "event_id_hash"),
    "context_entity": ("entity", "entity_hash"),
    "context_summary": ("summary", "summary_hash"),
    "context_node": ("node", "node_hash"),
    "context_segment": ("segment", "segment_hash"),
    "context_compression_event": ("compression", "compression_id_hash"),
    "resource_chunk": ("resource_chunk", "chunk_hash"),
    "skill_section": ("skill_section", "section_hash"),
}


def embedding_owner_key(record: Json):
    """What this record's vector belongs to, or None when the record carries no vector.

    A legacy `context_embedding` row names its owner directly; an owner record IS the owner. Both
    resolve to the same key, which is what lets a log holding both count the vector once.
    """
    record_type = str(record.get("record_type") or "")
    if record_type == "context_embedding":
        ref_type = str(record.get("ref_type") or "")
        ref_hash = record.get("ref_hash")
        if ref_hash in (None, ""):
            # An old row with no owner named still counts -- as itself.
            return ("context_embedding", id(record))
        return (ref_type, ref_hash)
    owner = EMBEDDING_OWNER_KEY_FIELDS.get(record_type)
    if owner is None:
        return None
    vector = record.get("vector")
    meta = record.get("embedding_meta")
    if not (isinstance(vector, list) and vector) and not (isinstance(meta, dict) and meta):
        # No vector and no ride-along metadata: this record was never embedded, and counting it
        # would report a backlog that does not exist.
        return None
    ref_type, field = owner
    ref_hash = record.get(field)
    if ref_hash in (None, ""):
        return None
    return (ref_type, ref_hash)


class _LocalAdapterDashboardMixin:
    def latest_skill_controls(self, records: list[Json] | None = None) -> dict[int, Json]:
        controls: dict[int, Json] = {}
        for record in reversed(records if records is not None else self.read_all()):
            if record.get("record_type") != "skill_registry_update":
                continue
            try:
                skill_hash = int(record.get("skill_hash"))
            except (TypeError, ValueError):
                continue
            if skill_hash not in controls:
                controls[skill_hash] = record
        return controls

    def _dashboard_record_scope(self, record: Json) -> Json:
        scope = candidate_access_scope(record)
        envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else {}
        envelope_scope = envelope.get("scope", {}) if isinstance(envelope.get("scope"), dict) else {}
        access_scope = envelope_scope if record.get("record_type") == "context_event" and envelope_scope else candidate_access_scope(record)
        if isinstance(scope, dict) and isinstance(access_scope, dict):
            merged = {**scope, **access_scope}
            if scope.get("agent_name") and not merged.get("agent_name"):
                merged["agent_name"] = scope["agent_name"]
            explicit = scope.get("_explicit_scope_keys")
            if isinstance(explicit, list):
                merged["_explicit_scope_keys"] = explicit
            return merged
        return access_scope

    def _dashboard_message_rows(self, records: list[Json], scope: Json) -> list[Json]:
        rows: list[Json] = []
        debug_by_ref: dict[Any, Json] = {}
        for record in records:
            if record.get("record_type") != "context_debug_record" or record.get("ref_type") != "event":
                continue
            debug_by_ref[record.get("ref_hash")] = record.get("debug_payload", {}) if isinstance(record.get("debug_payload"), dict) else {}
        for record in records:
            if record.get("record_type") != "context_event":
                continue
            if not scope_matches(self._dashboard_record_scope(record), scope):
                continue
            debug_payload = debug_by_ref.get(record.get("event_id_hash"), {})
            envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else debug_payload.get("envelope", {})
            if not isinstance(envelope, dict):
                envelope = {}
            kind = str(envelope.get("kind") or record.get("source_kind") or "")
            if kind not in {"message", "feedback", "business_data"}:
                continue
            messages = envelope.get("messages", []) if isinstance(envelope.get("messages"), list) else []
            if not messages and kind == "message":
                messages = [{"role": "unknown", "content": record.get("text", "")}]
            extraction = debug_payload.get("internal_extraction", {}) if isinstance(debug_payload.get("internal_extraction"), dict) else {}
            for message in messages:
                if not isinstance(message, dict):
                    continue
                rows.append(
                    {
                        "row_type": "message",
                        "event_id_hash": record.get("event_id_hash", 0),
                        "kind": kind,
                        "role": message.get("role", ""),
                        "name": message.get("name", ""),
                        "content": message.get("content", ""),
                        "summary_text": record.get("summary_text", ""),
                        "classification": non_default_classification(extraction.get("classification", "")),
                        "event_type": extraction.get("event_type", ""),
                        "node_hash": record.get("node_hash", 0),
                        "node_path": record.get("node_path", []),
                        "scope": envelope.get("scope", scope_from_serving_record(record)),
                        "agent_name": envelope.get("scope", {}).get("agent_name", "") if isinstance(envelope.get("scope"), dict) else "",
                        "created_at_ms": message.get("created_at_ms") or envelope.get("ingestion_time_ms") or record.get("updated_at_ms", 0),
                    }
                )
        return rows

    def _dashboard_rows_for_table(self, records: list[Json], table: str, scope: Json) -> list[Json]:
        rows: list[Json] = []
        if table == "messages":
            return self._dashboard_message_rows(records, scope)
        node_scope_by_hash: dict[int, Json] = {}
        ref_scope_by_key: dict[tuple[str, Any], Json] = {}

        def remember_ref_scope(ref_type: str, ref_hash: Any, source_record: Json) -> None:
            if ref_hash in (None, ""):
                return
            source_scope = self._dashboard_record_scope(source_record)
            if source_scope:
                ref_scope_by_key.setdefault((ref_type, ref_hash), source_scope)

        for source_record in records:
            source_record_type = str(source_record.get("record_type") or "")
            if source_record_type == "context_event":
                remember_ref_scope("event", source_record.get("event_id_hash"), source_record)
            elif source_record_type == "context_entity":
                remember_ref_scope("entity", source_record.get("entity_hash"), source_record)
            elif source_record_type == "context_segment":
                remember_ref_scope("segment", source_record.get("segment_hash"), source_record)
            elif source_record_type == "context_summary":
                remember_ref_scope("summary", source_record.get("summary_hash") or source_record.get("node_hash"), source_record)
            elif source_record_type == "context_compression_event":
                remember_ref_scope("compression", source_record.get("compression_id_hash"), source_record)
            try:
                source_node_hash = int(source_record.get("node_hash") or 0)
            except (TypeError, ValueError):
                source_node_hash = 0
            if not source_node_hash or source_node_hash in node_scope_by_hash:
                continue
            source_scope = self._dashboard_record_scope(source_record)
            if source_scope:
                node_scope_by_hash[source_node_hash] = source_scope

        def dashboard_scope(record: Json) -> Json:
            record_scope = self._dashboard_record_scope(record)
            if (
                record_scope
                and scope.get("account_id")
                and not record_scope.get("account_id")
                and (record_scope.get("tenant_id") or record_scope.get("user_id") or record_scope.get("session_id"))
            ):
                record_scope = {**record_scope, "account_id": scope.get("account_id")}
            if record_scope:
                return record_scope
            if record.get("record_type") == "context_embedding":
                ref_scope = ref_scope_by_key.get((str(record.get("ref_type") or ""), record.get("ref_hash")))
                if ref_scope:
                    if (
                        scope.get("account_id")
                        and not ref_scope.get("account_id")
                        and (ref_scope.get("tenant_id") or ref_scope.get("user_id") or ref_scope.get("session_id"))
                    ):
                        ref_scope = {**ref_scope, "account_id": scope.get("account_id")}
                    return ref_scope
            try:
                node_hash = int(record.get("node_hash") or 0)
            except (TypeError, ValueError):
                node_hash = 0
            return node_scope_by_hash.get(node_hash, {}) if node_hash else {}

        for record in records:
            record_type = str(record.get("record_type") or "")
            record_dashboard_scope = dashboard_scope(record)
            if not scope_matches(record_dashboard_scope, scope):
                continue
            if table == "resources" and record_type in {"resource_import_task", "resource_manifest", "resource_chunk"}:
                rows.append(
                    {
                        "row_type": record_type,
                        "task_hash": record.get("task_hash", record.get("import_task_hash", 0)),
                        "resource_hash": record.get("resource_hash", 0),
                        "chunk_hash": record.get("chunk_hash", 0),
                        "status": record.get("status", ""),
                        "raw_uri": record.get("raw_uri", ""),
                        "requested_raw_uri": record.get("requested_raw_uri", ""),
                        "resource_type": record.get("resource_type", ""),
                        "resource_version": record.get("resource_version", ""),
                        "raw_uri_hash": record.get("raw_uri_hash", 0),
                        "source_locator": record.get("source_locator", record.get("metadata", {}).get("source_locator", "")),
                        "unit_kind": record.get("unit_kind", record.get("metadata", {}).get("unit_kind", "")),
                        "token_estimate": record.get("token_estimate", 0),
                        "chunk_count": record.get("chunk_count", 0),
                        "parse_warnings": record.get("parse_warnings", []),
                        "node_hash": record.get("node_hash", 0),
                        "node_path": record.get("node_path", []),
                        "scope": candidate_access_scope(record),
                        "updated_at_ms": record.get("updated_at_ms", record.get("created_at_ms", 0)),
                    }
                )
            elif table == "skills" and record_type in {"skill_manifest", "skill_registry", "skill_section"}:
                rows.append(
                    {
                        "row_type": record_type,
                        "skill_hash": record.get("skill_hash", 0),
                        "section_hash": record.get("section_hash", 0),
                        "name": record.get("name", record.get("skill_name", "")),
                        "heading": record.get("heading", ""),
                        "status": record.get("status", ""),
                        "version": record.get("version", ""),
                        "triggers": record.get("triggers", []),
                        "allowed_tools": record.get("allowed_tools", []),
                        "node_hash": record.get("node_hash", 0),
                        "node_path": record.get("node_path", []),
                        "scope": record_dashboard_scope,
                        "updated_at_ms": record.get("updated_at_ms", 0),
                    }
                )
            elif table == "events" and record_type == "context_event":
                rows.append(
                    {
                        "row_type": record_type,
                        "event_id_hash": record.get("event_id_hash", 0),
                        "text": record.get("text", ""),
                        "summary_text": record.get("summary_text", ""),
                        "classification": non_default_classification(record.get("internal_extraction", {}).get("classification", "")),
                        "event_type": record.get("event_type", record.get("internal_extraction", {}).get("event_type", "")),
                        "source_chunk_hash": record.get("source_chunk_hash", 0),
                        "resource_hash": record.get("resource_hash", 0),
                        "source_locator": record.get("source_locator", ""),
                        "node_hash": record.get("node_hash", 0),
                        "node_path": record.get("node_path", []),
                        "scope": record_dashboard_scope,
                        "memory_scope": record.get("memory_scope", ""),
                        "session_continuity": record.get("session_continuity", ""),
                        "extraction_phase": record.get("extraction_phase", ""),
                        "final_session_boundary": bool(record.get("final_session_boundary", False)),
                        "source_role": record.get("source_role", ""),
                        "source_roles": record.get("source_roles", []),
                        "source_role_counts": record.get("source_role_counts", {}),
                        "source_hook_types": record.get("source_hook_types", []),
                        "source_hook_type_counts": record.get("source_hook_type_counts", {}),
                        "source_codex_events": record.get("source_codex_events", []),
                        "source_codex_event_counts": record.get("source_codex_event_counts", {}),
                        "updated_at_ms": record.get("envelope", {}).get("ingestion_time_ms", record.get("updated_at_ms", 0)),
                    }
                )
            elif table == "entities" and record_type == "context_entity":
                rows.append(
                    {
                        "row_type": record_type,
                        "entity_hash": record.get("entity_hash", 0),
                        "entity_type": record.get("entity_type", ""),
                        "entity_name": record.get("entity_name", ""),
                        "value": record.get("state", record.get("value", record.get("text", ""))),
                        "status": record.get("status", ""),
                        "source_event_hash": record.get("source_event_hash", 0),
                        "source_chunk_hash": record.get("source_chunk_hash", 0),
                        "resource_hash": record.get("resource_hash", 0),
                        "source_locator": record.get("source_locator", ""),
                        "node_hash": record.get("node_hash", 0),
                        "node_path": record.get("node_path", []),
                        "scope": record_dashboard_scope,
                        "memory_scope": record.get("memory_scope", ""),
                        "session_continuity": record.get("session_continuity", ""),
                        "promoted_from_memory_scope": record.get("promoted_from_memory_scope", ""),
                        "profile_promotion_policy": record.get("profile_promotion_policy", ""),
                        "profile_promotion_blocker": record.get("profile_promotion_blocker", ""),
                        "source_session_ids": record.get("source_session_ids", [])[:8] if isinstance(record.get("source_session_ids"), list) else [],
                        "source_session_count": len(record.get("source_session_ids", [])) if isinstance(record.get("source_session_ids"), list) else 0,
                        "source_entity_count": len(record.get("source_entity_hashes", [])) if isinstance(record.get("source_entity_hashes"), list) else 0,
                        "source_roles": record.get("source_roles", []),
                        "source_role_counts": record.get("source_role_counts", {}),
                        "source_hook_types": record.get("source_hook_types", []),
                        "source_hook_type_counts": record.get("source_hook_type_counts", {}),
                        "source_codex_events": record.get("source_codex_events", []),
                        "source_codex_event_counts": record.get("source_codex_event_counts", {}),
                        "extraction_phase": record.get("extraction_phase", ""),
                        "final_session_boundary": bool(record.get("final_session_boundary", False)),
                        "updated_at_ms": record.get("updated_at_ms", 0),
                    }
                )
            elif table == "embeddings" and record_type == "context_embedding":
                row = {
                    "row_type": record_type,
                    "embedding_type": record.get("embedding_type", ""),
                    "ref_type": record.get("ref_type", ""),
                    "ref_hash": record.get("ref_hash", 0),
                    "node_hash": record.get("node_hash", 0),
                    "node_path": record.get("node_path", []),
                    "dim": record.get("dim", len(record_vector(record))),
                    "model": record.get("model", record.get("model_ref", "")),
                    "has_vector": bool(record_vector(record)),
                    "scope": record_dashboard_scope,
                    "memory_scope": record.get("memory_scope", ""),
                    "session_continuity": record.get("session_continuity", ""),
                    "promoted_from_memory_scope": record.get("promoted_from_memory_scope", ""),
                    "profile_promotion_policy": record.get("profile_promotion_policy", ""),
                    "extraction_phase": record.get("extraction_phase", ""),
                    "final_session_boundary": bool(record.get("final_session_boundary", False)),
                    "updated_at_ms": record.get("updated_at_ms", 0),
                }
                if context_debug_records_enabled():
                    row.update(
                        {
                            "source_roles": record.get("source_roles", []),
                            "source_role_counts": record.get("source_role_counts", {}),
                            "source_hook_types": record.get("source_hook_types", []),
                            "source_hook_type_counts": record.get("source_hook_type_counts", {}),
                            "source_codex_events": record.get("source_codex_events", []),
                            "source_codex_event_counts": record.get("source_codex_event_counts", {}),
                            "source_memory_scopes": record.get("source_memory_scopes", []),
                            "source_session_continuities": record.get("source_session_continuities", []),
                        }
                    )
                rows.append(row)
            elif table == "indexes" and record_type == "context_index":
                ref_hashes = record.get("ref_hashes", []) if isinstance(record.get("ref_hashes"), list) else []
                rows.append(
                    {
                        "row_type": record_type,
                        "index_hash": record.get("index_hash", 0),
                        "index_name": record.get("index_name", ""),
                        "data_model": record.get("data_model", ""),
                        "ref_type": record.get("ref_type", ""),
                        "ref_hash": record.get("ref_hash", 0),
                        "ref_hash_count": len(ref_hashes),
                        "batch_id_hash": record.get("batch_id_hash", 0),
                        "node_hash": record.get("node_hash", 0),
                        "node_path": record.get("node_path", []),
                        "scope": record_dashboard_scope,
                        "timestamp_key_ms": record.get("timestamp_key_ms", 0),
                        "updated_at_ms": record.get("updated_at_ms", record.get("timestamp_key_ms", 0)),
                    }
                )
            elif table == "summaries" and record_type == "context_summary":
                rows.append(
                    {
                        "row_type": record_type,
                        "summary_hash": record.get("summary_hash", 0),
                        "summary_type": record.get("summary_type", ""),
                        "summary_text": record.get("summary_text", ""),
                        "node_hash": record.get("node_hash", 0),
                        "node_path": record.get("node_path", []),
                        "scope": record_dashboard_scope,
                        # A profile entity keeps only the newest provenance inline and states its
                        # true total, so count the field when it is there rather than the window.
                        "source_event_count": int(record.get("source_event_count") or 0)
                        or (len(record.get("source_event_ids", [])) if isinstance(record.get("source_event_ids"), list) else 0),
                        "source_entity_count": len(record.get("source_entity_hashes", [])) if isinstance(record.get("source_entity_hashes"), list) else 0,
                        "source_segment_count": len(record.get("source_segment_hashes", [])) if isinstance(record.get("source_segment_hashes"), list) else 0,
                        "source_roles": record.get("source_roles", []),
                        "source_role_counts": record.get("source_role_counts", {}),
                        "source_hook_types": record.get("source_hook_types", []),
                        "source_hook_type_counts": record.get("source_hook_type_counts", {}),
                        "source_codex_events": record.get("source_codex_events", []),
                        "source_codex_event_counts": record.get("source_codex_event_counts", {}),
                        "extraction_phase": record.get("extraction_phase", ""),
                        "final_session_boundary": bool(record.get("final_session_boundary", False)),
                        "updated_at_ms": record.get("updated_at_ms", 0),
                    }
                )
            elif table == "context_packs" and record_type in {"context_pack_audit", "context_pack_telemetry"}:
                dropped_refs = record.get("dropped_refs", {})
                dropped_ref_bucket_counts = (
                    record.get("dropped_ref_bucket_counts")
                    if isinstance(record.get("dropped_ref_bucket_counts"), dict)
                    else {
                        key: value
                        for key, value in dropped_refs.items()
                        if isinstance(dropped_refs, dict)
                        and isinstance(value, int)
                        and key != "deadline_exceeded"
                        and value > 0
                    }
                )
                dropped_ref_count = (
                    len(dropped_refs.get("refs", []))
                    if record_type == "context_pack_audit"
                    and isinstance(dropped_refs, dict)
                    and isinstance(dropped_refs.get("refs"), list)
                    else record.get("dropped_ref_count", 0)
                )
                if not dropped_ref_count:
                    dropped_ref_count = sum(
                        int(value) for value in dropped_ref_bucket_counts.values() if isinstance(value, int)
                    )
                memory_layer_budget = record.get("memory_layer_budget")
                if not isinstance(memory_layer_budget, dict):
                    recall_policy = record.get("recall_policy", {}) if isinstance(record.get("recall_policy"), dict) else {}
                    memory_layer_budget = recall_policy.get("memory_layer_budget", {}) if isinstance(recall_policy.get("memory_layer_budget"), dict) else {}
                dropped_memory_layer_budget = record.get("dropped_memory_layer_budget")
                if not isinstance(dropped_memory_layer_budget, dict):
                    recall_policy = record.get("recall_policy", {}) if isinstance(record.get("recall_policy"), dict) else {}
                    dropped_memory_layer_budget = (
                        recall_policy.get("dropped_memory_layer_budget", {})
                        if isinstance(recall_policy.get("dropped_memory_layer_budget"), dict)
                        else {}
                    )
                memory_layer_pressure = record.get("memory_layer_pressure")
                if not isinstance(memory_layer_pressure, dict):
                    recall_policy = record.get("recall_policy", {}) if isinstance(record.get("recall_policy"), dict) else {}
                    memory_layer_pressure = (
                        recall_policy.get("memory_layer_pressure", {})
                        if isinstance(recall_policy.get("memory_layer_pressure"), dict)
                        else {}
                    )
                if not memory_layer_pressure:
                    memory_layer_pressure = memory_layer_pressure_summary(memory_layer_budget, dropped_memory_layer_budget)
                memory_selection_policy_budget = record.get("memory_selection_policy_budget")
                if not isinstance(memory_selection_policy_budget, dict):
                    recall_policy = record.get("recall_policy", {}) if isinstance(record.get("recall_policy"), dict) else {}
                    memory_selection_policy_budget = (
                        recall_policy.get("memory_selection_policy_budget_policy", {})
                        if isinstance(recall_policy.get("memory_selection_policy_budget_policy"), dict)
                        else {}
                    )
                memory_selection_policy_budget = serving_memory_selection_policy_budget(memory_selection_policy_budget)
                async_pipeline_readiness = record.get("async_pipeline_readiness")
                if not isinstance(async_pipeline_readiness, dict):
                    recall_policy = record.get("recall_policy", {}) if isinstance(record.get("recall_policy"), dict) else {}
                    async_pipeline_readiness = (
                        recall_policy.get("async_pipeline_readiness", {})
                        if isinstance(recall_policy.get("async_pipeline_readiness"), dict)
                        else {}
                    )
                session_identity = record.get("session_identity")
                if not isinstance(session_identity, dict):
                    recall_policy = record.get("recall_policy", {}) if isinstance(record.get("recall_policy"), dict) else {}
                    session_identity = recall_policy.get("session_identity", {}) if isinstance(recall_policy.get("session_identity"), dict) else {}
                retrieval_request_metadata = (
                    record.get("retrieval_request_metadata")
                    if isinstance(record.get("retrieval_request_metadata"), dict)
                    else {}
                )
                rows.append(
                    {
                        "row_type": record_type,
                        "context_pack_id": record.get("context_pack_id", ""),
                        "query": record.get("query", "") if record_type == "context_pack_audit" else f"hash:{record.get('query_hash', '')}",
                        "used_context_tokens": record.get("used_context_tokens", record.get("used_remote_context_tokens", 0)),
                        "used_local_context_tokens": record.get("used_local_context_tokens", 0),
                        "used_remote_context_tokens": record.get("used_remote_context_tokens", 0),
                        "remote_context_budget_tokens": record.get("remote_context_budget_tokens", 0),
                        "requested_max_context_tokens": record.get("requested_max_context_tokens", 0),
                        "selected_ref_count": len(record.get("selected_refs", [])) if record_type == "context_pack_audit" else record.get("selected_ref_count", 0),
                        "dropped_ref_count": dropped_ref_count,
                        "dropped_ref_bucket_counts": dropped_ref_bucket_counts,
                        "stale_dropped_refs": int(record.get("stale_dropped_refs") or dropped_ref_bucket_counts.get("stale", 0)),
                        "memory_layer_budget": memory_layer_budget,
                        "dropped_memory_layer_budget": dropped_memory_layer_budget,
                        "memory_layer_pressure": memory_layer_pressure,
                        "memory_selection_policy_budget": memory_selection_policy_budget,
                        "async_pipeline_readiness": async_pipeline_readiness,
                        "session_identity": session_identity,
                        "retrieval_request_metadata": retrieval_request_metadata,
                        "retrieval_source": retrieval_request_metadata.get("retrieval_source", retrieval_request_metadata.get("source", "")),
                        "codex_event": retrieval_request_metadata.get("codex_event", ""),
                        "hook_type": retrieval_request_metadata.get("hook_type", ""),
                        "lifecycle_stage": retrieval_request_metadata.get("lifecycle_stage", ""),
                        "quality_warnings": record.get("quality_warnings", []) if record_type == "context_pack_audit" else record.get("quality_warnings", {"count": record.get("quality_warning_count", 0)}),
                        "scope": candidate_access_scope(record),
                        "created_at_ms": record.get("created_at_ms", 0),
                    }
                )
            elif table == "summary_refresh" and record_type in {"context_batch_commit", "context_summary_dirty"}:
                if record_type == "context_batch_commit":
                    summary_refresh = record.get("summary_refresh", {})
                    if not isinstance(summary_refresh, dict):
                        summary_refresh = {}
                    profile_promotion_summary = record.get("profile_promotion_summary", [])
                    rows.append(
                        {
                            "row_type": record_type,
                            "commit_id_hash": record.get("commit_id_hash", 0),
                            "batch_id_hash": record.get("batch_id_hash", 0),
                            "node_hash": record.get("node_hash", 0),
                            "node_path": record.get("node_path", []),
                            "scope": candidate_access_scope(record),
                            "commit_reason": record.get("commit_reason", ""),
                            "trigger_policy": record.get("trigger_policy", ""),
                            "extraction_phase": record.get("extraction_phase", ""),
                            "final_session_boundary": bool(record.get("final_session_boundary", False)),
                            "summary_refresh_status": summary_refresh.get("status", ""),
                            "summary_dirty_hash_count": len(summary_refresh.get("dirty_hashes", []))
                            if isinstance(summary_refresh.get("dirty_hashes"), list)
                            else 0,
                            "session_dirty_hash_count": len(summary_refresh.get("session_dirty_hashes", []))
                            if isinstance(summary_refresh.get("session_dirty_hashes"), list)
                            else 0,
                            "profile_dirty_hash_count": len(summary_refresh.get("profile_dirty_hashes", []))
                            if isinstance(summary_refresh.get("profile_dirty_hashes"), list)
                            else 0,
                            "profile_summary_refresh_required": bool(summary_refresh.get("profile_summary_refresh_required", False)),
                            "profile_promotion_count": len(profile_promotion_summary)
                            if isinstance(profile_promotion_summary, list)
                            else 0,
                            "memory_layers_written": record.get("memory_layers_written", {}),
                            "source_roles": record.get("source_roles", []),
                            "source_role_counts": record.get("source_role_counts", {}),
                            "source_hook_types": record.get("source_hook_types", []),
                            "source_hook_type_counts": record.get("source_hook_type_counts", {}),
                            "source_codex_events": record.get("source_codex_events", []),
                            "source_codex_event_counts": record.get("source_codex_event_counts", {}),
                            "created_at_ms": record.get("created_at_ms", 0),
                        }
                    )
                else:
                    rows.append(
                        {
                            "row_type": record_type,
                            "dirty_node_hash": record.get("dirty_node_hash", record.get("node_hash", 0)),
                            "node_hash": record.get("node_hash", 0),
                            "node_path": record.get("node_path", []),
                            "scope": candidate_access_scope(record),
                            "dirty_reason": record.get("dirty_reason", ""),
                            "source_ref_type": record.get("source_ref_type", ""),
                            "source_batch_hash": record.get("source_batch_hash", 0),
                            "source_entity_hash": record.get("source_entity_hash", 0),
                            "source_event_hash": record.get("source_event_hash", 0),
                            "source_roles": record.get("source_roles", []),
                            "source_role_counts": record.get("source_role_counts", {}),
                            "source_hook_types": record.get("source_hook_types", []),
                            "source_hook_type_counts": record.get("source_hook_type_counts", {}),
                            "source_codex_events": record.get("source_codex_events", []),
                            "source_codex_event_counts": record.get("source_codex_event_counts", {}),
                            "source_memory_scopes": record.get("source_memory_scopes", []),
                            "source_session_continuities": record.get("source_session_continuities", []),
                            "source_extraction_phases": record.get("source_extraction_phases", []),
                            "updated_at_ms": record.get("updated_at_ms", record.get("created_at_ms", 0)),
                        }
                    )
            elif table == "async_pipeline" and record_type == "matrixark_async_pipeline_task":
                memory_layers_written = record.get("memory_layers_written", {})
                if not isinstance(memory_layers_written, dict):
                    memory_layers_written = {}
                completed_stages = record.get("completed_stages", [])
                if not isinstance(completed_stages, list):
                    completed_stages = []
                remaining_stages = record.get("remaining_stages", [])
                if not isinstance(remaining_stages, list):
                    remaining_stages = []
                source_roles = record.get("source_roles", [])
                if not isinstance(source_roles, list):
                    source_roles = []
                source_hook_types = record.get("source_hook_types", [])
                if not isinstance(source_hook_types, list):
                    source_hook_types = []
                source_codex_events = record.get("source_codex_events", [])
                if not isinstance(source_codex_events, list):
                    source_codex_events = []
                def layer_count(name: str) -> int:
                    try:
                        return int(memory_layers_written.get(name, 0) or 0)
                    except (TypeError, ValueError):
                        return 0
                source_memory_scopes = record.get("source_memory_scopes", [])
                if not isinstance(source_memory_scopes, list):
                    source_memory_scopes = []
                if not source_memory_scopes:
                    if layer_count("session_entities") > 0:
                        source_memory_scopes.append("session")
                    if layer_count("profile_entities") > 0:
                        source_memory_scopes.append("user_profile")
                source_session_continuities = record.get("source_session_continuities", [])
                if not isinstance(source_session_continuities, list):
                    source_session_continuities = []
                if not source_session_continuities:
                    if layer_count("same_session_entities") > 0:
                        source_session_continuities.append("same_session")
                    if layer_count("cross_session_entities") > 0:
                        source_session_continuities.append("cross_session")
                rows.append(
                    {
                        "row_type": record_type,
                        "task_hash": record.get("task_hash", 0),
                        "event_id_hash": record.get("event_id_hash", 0),
                        "commit_id_hash": record.get("commit_id_hash", 0),
                        "batch_id_hash": record.get("batch_id_hash", 0),
                        "node_hash": record.get("node_hash", 0),
                        "node_path": record.get("node_path", []),
                        "scope": candidate_access_scope(record),
                        "status": record.get("status", ""),
                        "stages": record.get("stages", []),
                        "completed_stages": completed_stages,
                        "remaining_stages": remaining_stages,
                        "summary_pending": "summary" in remaining_stages,
                        "compression_pending": "compression" in remaining_stages,
                        "embedding_pending": "embedding" in remaining_stages,
                        "trigger_policy": record.get("trigger_policy", ""),
                        "extraction_phase": record.get("extraction_phase", ""),
                        "final_session_boundary": bool(record.get("final_session_boundary", False)),
                        "source_roles": source_roles,
                        "source_role_counts": record.get("source_role_counts", {}),
                        "source_hook_types": source_hook_types,
                        "source_hook_type_counts": record.get("source_hook_type_counts", {}),
                        "source_codex_events": source_codex_events,
                        "source_codex_event_counts": record.get("source_codex_event_counts", {}),
                        "memory_layers_written": memory_layers_written,
                        "source_memory_scopes": source_memory_scopes,
                        "source_session_continuities": source_session_continuities,
                        "session_entities_written": layer_count("session_entities"),
                        "profile_entities_written": layer_count("profile_entities"),
                        "same_session_entities_written": layer_count("same_session_entities"),
                        "cross_session_entities_written": layer_count("cross_session_entities"),
                        "summary_refresh_status": record.get("summary_refresh_status", ""),
                        "summary_dirty_nodes": record.get("summary_dirty_nodes", 0),
                        "created_at_ms": record.get("created_at_ms", 0),
                        "updated_at_ms": record.get("updated_at_ms", record.get("created_at_ms", 0)),
                    }
                )
        if table == "async_pipeline":
            rows = latest_async_pipeline_rows(rows)
        if table == "resources":
            priority = {"resource_manifest": 0, "resource_chunk": 1, "resource_import_task": 2}
            rows.sort(
                key=lambda row: (
                    priority.get(str(row.get("row_type") or ""), 9),
                    -int(row.get("updated_at_ms") or row.get("created_at_ms") or 0),
                )
            )
        elif table == "indexes":
            data_model_priority = {
                "context_batch_commit": 0,
                "context_profile_entity": 1,
                "context_entity": 2,
                "context_summary": 3,
                "context_segment": 4,
                "context_event": 5,
            }
            rows.sort(
                key=lambda row: (
                    data_model_priority.get(str(row.get("data_model") or ""), 9),
                    -int(row.get("updated_at_ms") or row.get("created_at_ms") or row.get("timestamp_key_ms") or 0),
                )
            )
        else:
            rows.sort(key=lambda row: int(row.get("updated_at_ms") or row.get("created_at_ms") or 0), reverse=True)
        return rows

    @staticmethod
    def _embedding_is_pending(record: Json) -> bool:
        """Is this embedding record still a placeholder?

        Deliberately not `is_pending_async_candidate`: that is the RETRIEVAL predicate and returns
        False unless `ref_type == "event"`, because an event is the only shape it ranks. A backlog
        counted with it omits every pending embedding for a resource chunk or a skill section --
        most of what a bulk import produces -- and reads as smaller than it is, which is the one
        direction a backlog must never be wrong in.
        """
        metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}

        def field(name: str) -> str:
            return str(record.get(name) or metadata.get(name) or "").strip()

        return (
            field("status").lower() == "pending"
            or field("event_type").lower() == "pending_async"
            or field("classification").upper() == "PENDING_ASYNC_EXTRACTION"
            or field("extraction_phase").lower() == "pending_async"
            or field("extraction_status").lower() in {"pending", "async_pending"}
            or field("extraction_mode").lower() == "async_pending"
        )

    def embedding_status(self, args: Json) -> Json:
        """How much of this scope is encoded, and how much is still waiting.

        Counts, not rows. The dashboard can page the embeddings table, but a page is a sample and
        a sample presented as a total is worse than no number: "12 pending" out of a 200-row window
        onto 50,000 vectors is not a backlog, it is an artefact of the page size.

        `dimensions` is the one that catches a silent break. Vectors of different widths cannot be
        compared, so a store holding two dimensions is a store where some memories can never match
        a query -- which happens the moment somebody changes the embedding model without a
        backfill, and looks exactly like ordinary poor recall.
        """
        scope = optional_object(args, "scope")
        records = self.read_all()

        total = 0
        pending = 0
        encoded = 0
        without_vector = 0
        models: dict[str, int] = {}
        dimensions: dict[int, int] = {}
        oldest_pending_ms = 0
        newest_pending_ms = 0
        deferred_tasks = 0
        deferred_stages = 0

        # One entry per embedded thing, keyed by what it belongs to, so a log holding BOTH the
        # retired separate row and its owner counts the vector once rather than twice.
        seen_owners: set = set()

        for record in records:
            record_type = str(record.get("record_type") or "")
            if record_type == "matrixark_async_pipeline_task":
                if not scope_matches(candidate_access_scope(record), scope):
                    continue
                remaining = record.get("remaining_stages")
                remaining = remaining if isinstance(remaining, list) else []
                if remaining:
                    deferred_tasks += 1
                    deferred_stages += len(remaining)
                continue
            owner_key = embedding_owner_key(record)
            if owner_key is None:
                continue
            if not scope_matches(candidate_access_scope(record), scope):
                continue
            if owner_key in seen_owners:
                continue
            seen_owners.add(owner_key)

            total += 1
            vector = record.get("vector")
            has_vector = isinstance(vector, list) and bool(vector)
            if not has_vector:
                without_vector += 1

            if self._embedding_is_pending(record) or not has_vector:
                pending += 1
                try:
                    updated = int(record.get("updated_at_ms") or 0)
                except (TypeError, ValueError):
                    updated = 0
                if updated:
                    oldest_pending_ms = min(oldest_pending_ms or updated, updated)
                    newest_pending_ms = max(newest_pending_ms, updated)
            else:
                encoded += 1

            meta = record.get("embedding_meta")
            meta = meta if isinstance(meta, dict) else {}
            # The owner carries the retired row's fields under embedding_meta; a legacy separate row
            # carries them at the top level. Prefer whichever this record actually has.
            model = str(meta.get("model") or meta.get("model_ref")
                        or record.get("model") or record.get("model_ref") or "")
            if model:
                models[model] = models.get(model, 0) + 1
            try:
                dim = int(meta.get("dim") or record.get("dim")
                          or (len(vector) if has_vector else 0))
            except (TypeError, ValueError):
                dim = 0
            if dim:
                dimensions[dim] = dimensions.get(dim, 0) + 1

        return {
            "status": "ok",
            "scope": scope,
            "total": total,
            "encoded": encoded,
            "pending": pending,
            "without_vector": without_vector,
            "percent_encoded": round((encoded / total) * 100.0, 1) if total else 100.0,
            "models": [{"model": name, "count": count}
                       for name, count in sorted(models.items(), key=lambda kv: -kv[1])],
            "dimensions": [{"dim": dim, "count": count}
                           for dim, count in sorted(dimensions.items(), key=lambda kv: -kv[1])],
            "mixed_dimensions": len(dimensions) > 1,
            "oldest_pending_ms": oldest_pending_ms,
            "newest_pending_ms": newest_pending_ms,
            "deferred_tasks": deferred_tasks,
            "deferred_stages": deferred_stages,
            "record_count": len(records),
        }

    def ingestion_dashboard(self, args: Json) -> Json:
        scope = optional_object(args, "scope")
        table = optional_string(args, "table", "messages")
        allowed_tables = {
            "messages",
            "resources",
            "skills",
            "events",
            "entities",
            "embeddings",
            "indexes",
            "summaries",
            "context_packs",
            "summary_refresh",
            "async_pipeline",
        }
        if table not in allowed_tables:
            raise MatrixArkError(f"table must be one of {sorted(allowed_tables)}")
        page_size = args.get("page_size", 25)
        if not isinstance(page_size, int) or page_size <= 0 or page_size > 200:
            raise MatrixArkError("page_size must be an integer between 1 and 200")
        page_token = args.get("page_token", 0)
        if isinstance(page_token, str) and page_token.isdigit():
            page_token = int(page_token)
        if not isinstance(page_token, int) or page_token < 0:
            raise MatrixArkError("page_token must be a non-negative integer offset")
        records = self.read_all()
        totals = {name: len(self._dashboard_rows_for_table(records, name, scope)) for name in sorted(allowed_tables)}
        rows = self._dashboard_rows_for_table(records, table, scope)
        page = rows[page_token : page_token + page_size]
        next_page_token = page_token + page_size if page_token + page_size < len(rows) else None
        return {
            "status": "ok",
            "scope": scope,
            "table": table,
            "page_size": page_size,
            "page_token": page_token,
            "next_page_token": next_page_token,
            "total": len(rows),
            "totals": totals,
            "rows": page,
            "record_count": len(records),
        }

    # A response this size is a deliberate ceiling, not a guess: a skill or attachment can be
    # far larger than anything a caller wants in one JSON body, and the point of paging is that
    # they never have to find that out the hard way.
    RESOURCE_CONTENT_DEFAULT_CHUNKS = 32
    RESOURCE_CONTENT_MAX_CHARS = 200_000

    def get_resource_content(self, args: Json) -> Json:
        """Return one resource's (or skill's) stored text, in order, a page at a time.

        `list_skills` / `list_resources` return a POINTER -- raw_uri, cloud_key -- and metadata.
        The text is already stored, split across `resource_chunk` records at ingest, but nothing
        reassembled it, so "give me this skill's content" had no answer: a caller had to know the
        chunks existed and stitch them itself.

        Paged on purpose. An attachment can be far larger than anything that belongs in one JSON
        response, so this returns at most `chunk_limit` chunks and at most `max_chars` characters,
        and reports `next_chunk_offset` when there is more. A caller that wants everything loops;
        a caller that wants the first page pays for the first page.

        Ordering is `chunk_index` where present, and log order otherwise -- chunks written before
        the index existed have no other ordering key, and returning them in log order is what the
        ingest path produced.
        """
        resource_hash = args.get("resource_hash", args.get("skill_hash"))
        try:
            resource_hash = int(resource_hash)
        except (TypeError, ValueError):
            raise MatrixArkError("resource_hash (or skill_hash) must be an integer")
        if resource_hash <= 0:
            raise MatrixArkError("resource_hash (or skill_hash) must be a positive integer")
        scope = optional_object(args, "scope")
        offset = args.get("chunk_offset", 0)
        offset = int(offset) if isinstance(offset, int) and offset > 0 else 0
        limit = args.get("chunk_limit", self.RESOURCE_CONTENT_DEFAULT_CHUNKS)
        if not isinstance(limit, int) or limit <= 0:
            limit = self.RESOURCE_CONTENT_DEFAULT_CHUNKS
        max_chars = args.get("max_chars", self.RESOURCE_CONTENT_MAX_CHARS)
        if not isinstance(max_chars, int) or max_chars <= 0:
            max_chars = self.RESOURCE_CONTENT_MAX_CHARS

        found: list[Json] = []
        manifest: Json = {}
        for position, record in enumerate(self.read_all()):
            record_type = record.get("record_type")
            if record_type in {"resource_manifest", "skill_manifest"}:
                rid = record.get("resource_hash", record.get("skill_hash"))
                try:
                    rid = int(rid or 0)
                except (TypeError, ValueError):
                    continue
                if rid == resource_hash and scope_matches(candidate_access_scope(record), scope):
                    manifest = record
                continue
            # A skill's chunk text lives on its skill_section. Retrieval already skips
            # `resource_chunk` for skills (see the resource/skill scan in
            # matrixark_local_adapter_retrieve), so for a skill this view is the only reader the
            # duplicate chunk ever had -- reading the section instead lets ingest stop writing it.
            if record_type not in {"resource_chunk", "skill_section"}:
                continue
            try:
                if int(record.get("resource_hash") or 0) != resource_hash:
                    continue
            except (TypeError, ValueError):
                continue
            if not scope_matches(candidate_access_scope(record), scope):
                continue
            index = record.get("chunk_index")
            found.append({
                "order": (int(index) if isinstance(index, int) else position),
                "chunk_index": int(index) if isinstance(index, int) else None,
                # section_hash identifies a section the way chunk_hash identifies a chunk; the
                # dedupe below keys on whichever is present.
                "chunk_hash": record.get("chunk_hash", record.get("section_hash")),
                "source_ref": record.get("source_ref", record.get("source_locator", "")),
                "token_estimate": record.get("token_estimate", 0),
                "text": str(record.get("text") or ""),
            })
        # Same chunk_hash can be re-ingested; the later write wins, as everywhere else.
        deduped: dict[Any, Json] = {}
        for item in found:
            deduped[item.get("chunk_hash", item["order"])] = item
        ordered = sorted(deduped.values(), key=lambda c: c["order"])

        page: list[Json] = []
        chars = 0
        truncated_by_chars = False
        for item in ordered[offset:offset + limit]:
            text = item["text"]
            if chars + len(text) > max_chars:
                room = max(0, max_chars - chars)
                if room:
                    item = {**item, "text": text[:room], "truncated": True}
                    page.append(item)
                    chars += room
                truncated_by_chars = True
                break
            page.append(item)
            chars += len(text)

        served = offset + len(page)
        has_more = served < len(ordered) or truncated_by_chars
        return {
            "status": "ok",
            "resource_hash": resource_hash,
            "name": manifest.get("name", ""),
            "description": manifest.get("description", ""),
            "resource_type": manifest.get("resource_type", "skill" if manifest.get("skill_hash") else ""),
            "raw_uri": manifest.get("raw_uri", ""),
            "chunk_count": len(ordered),
            "chunk_offset": offset,
            "returned_chunks": len(page),
            "next_chunk_offset": served if has_more else None,
            "has_more": bool(has_more),
            "truncated_by_max_chars": truncated_by_chars,
            "chars": chars,
            "text": "".join(c["text"] for c in page),
            "chunks": [{k: v for k, v in c.items() if k != "order"} for c in page],
        }

    def list_resources(self, args: Json) -> Json:
        scope = optional_object(args, "scope")
        limit = args.get("limit", 100)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        resource_type_filter = optional_string(args, "resource_type", "")
        resources: dict[int, Json] = {}
        for record in reversed(self.read_all()):
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

    def list_skills(self, args: Json) -> Json:
        scope = optional_object(args, "scope")
        limit = args.get("limit", 100)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        include_disabled = bool(args.get("include_disabled", False))
        controls = self.latest_skill_controls()
        skills: dict[int, Json] = {}
        for record in reversed(self.read_all()):
            if record.get("record_type") != "skill_manifest":
                continue
            if not scope_matches(candidate_access_scope(record), scope):
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

    def update_skill(self, args: Json) -> Json:
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
        for record in reversed(self.read_all()):
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
        self.append(update)
        return {"status": "updated", **update}

    def _resource_import_pool_status(self) -> Json:
        return {
            "worker_count": self._resource_import_worker_count,
            "queue_max": self._resource_import_queue_max,
            "queue_depth": self._resource_import_queue.qsize(),
            "queue_remaining_capacity": max(0, self._resource_import_queue_max - self._resource_import_queue.qsize()),
            "bounded": True,
        }

    def _ensure_resource_import_workers(self) -> None:
        with self._resource_import_worker_lock:
            if self._resource_import_workers_started:
                return
            self._resource_import_stop.clear()
            for worker_index in range(self._resource_import_worker_count):
                thread = threading.Thread(
                    target=self._resource_import_worker_loop,
                    name=f"matrixark-resource-import-{worker_index}",
                    daemon=True,
                )
                thread.start()
                self._resource_import_threads.append(thread)
            self._resource_import_workers_started = True

    def _resource_import_worker_loop(self) -> None:
        while True:
            item = self._resource_import_queue.get()
            try:
                if item.get("_stop"):
                    return
                args = item.get("args", {})
                hook = item.get("hook")
                self._run_background_resource_import(args, hook if isinstance(hook, dict) else None)
            finally:
                self._resource_import_queue.task_done()

    def close(self, *, timeout_s: float = 5.0) -> None:
        """Drain async import work and stop background workers."""
        deadline = time.monotonic() + max(0.0, timeout_s)
        while getattr(self._resource_import_queue, "unfinished_tasks", 0) and time.monotonic() < deadline:
            time.sleep(0.01)
        self._resource_import_stop.set()
        with self._resource_import_worker_lock:
            if self._resource_import_workers_started:
                for _thread in self._resource_import_threads:
                    remaining = max(0.0, deadline - time.monotonic())
                    try:
                        self._resource_import_queue.put({"_stop": True}, timeout=remaining if remaining > 0 else 0.01)
                    except thread_queue.Full:
                        pass
                for thread in list(self._resource_import_threads):
                    thread.join(timeout=max(0.0, deadline - time.monotonic()))
                self._resource_import_threads = [thread for thread in self._resource_import_threads if thread.is_alive()]
                self._resource_import_workers_started = bool(self._resource_import_threads)

    def _enqueue_resource_import(self, *, args: Json, hook: Json | None, task_hash: int) -> Json:
        self._ensure_resource_import_workers()
        queue_before = self._resource_import_queue.qsize()
        try:
            self._resource_import_queue.put_nowait(
                {
                    "args": args,
                    "hook": hook,
                    "task_hash": task_hash,
                    "queued_at_ms": now_ms(),
                }
            )
        except thread_queue.Full:
            raise MatrixArkError(
                f"resource import queue is full; workers={self._resource_import_worker_count} max_queue={self._resource_import_queue_max}"
            )
        status = self._resource_import_pool_status()
        status["queue_depth_before_enqueue"] = queue_before
        self._observe_model_latency("resource_import_queue_wait", 0.0)
        metrics = getattr(self, "_matrixark_service_metrics", None)
        if metrics is not None:
            metrics.observe_resource_queue_depth(int(status.get("queue_depth") or 0))
        return status

    def _run_background_resource_import(self, args: Json, hook: Json | None) -> None:
        task_hash = args.get("_resource_import_task_hash", 0)
        try:
            self.ingest(args, hook=hook)
        except Exception as exc:  # pragma: no cover - background failure path is validated via records.
            scope = optional_object(args, "scope")
            metadata = optional_object(args, "metadata")
            envelope = normalize_envelope(args, default_kind="resource")
            deployment_scope = deployment_scope_from_args(args, envelope)
            sharing_scope = self.resource_sharing_scope(args, envelope, deployment_scope)
            node_hint = self.default_resource_node_path(args, envelope, deployment_scope=deployment_scope, sharing_scope=sharing_scope)
            node_path = [str(part) for part in node_hint if str(part)]
            try:
                self.append(
                    {
                        "record_type": "resource_import_task",
                        "task_hash": task_hash,
                        "status": "failed",
                        "kind": str(args.get("kind") or "resource"),
                        "raw_uri": str(args.get("raw_uri") or metadata.get("raw_uri") or "inline-resource"),
                        "resource_type": str(args.get("resource_type") or metadata.get("resource_type") or ""),
                        "error": str(exc),
                        "node_hash": stable_hash("/".join(node_path)),
                        "node_path": node_path,
                        "scope": dict(scope),
                        "updated_at_ms": now_ms(),
                    }
                )
            except Exception:
                _mcp_debug_log(f"resource import background failure could not be recorded: {exc}")

    def _resource_import_async_default_reason(self, args: Json, envelope: Json, raw_uri: str) -> str:
        if "wait" in args:
            return ""
        inline_text = "\n\n".join(str(message.get("content", "")) for message in envelope.get("messages", []))
        if len(inline_text) >= RESOURCE_ASYNC_DEFAULT_TEXT_CHARS:
            return f"inline_text_chars>={RESOURCE_ASYNC_DEFAULT_TEXT_CHARS}"
        try:
            path = Path(raw_uri)
            if not path.exists():
                return ""
            if path.is_file():
                size = path.stat().st_size
                if size >= RESOURCE_ASYNC_DEFAULT_BYTES:
                    return f"file_bytes>={RESOURCE_ASYNC_DEFAULT_BYTES}"
            elif path.is_dir():
                file_count = 0
                total_size = 0
                for child in path.rglob("*"):
                    if not child.is_file():
                        continue
                    if any(part in RESOURCE_IMPORT_IGNORE_DIRS for part in child.parts):
                        continue
                    file_count += 1
                    try:
                        total_size += child.stat().st_size
                    except OSError:
                        pass
                    if file_count >= RESOURCE_ASYNC_DEFAULT_PATH_COUNT:
                        return f"path_count>={RESOURCE_ASYNC_DEFAULT_PATH_COUNT}"
                    if total_size >= RESOURCE_ASYNC_DEFAULT_BYTES:
                        return f"directory_bytes>={RESOURCE_ASYNC_DEFAULT_BYTES}"
        except (OSError, ValueError):
            return ""
        return ""

