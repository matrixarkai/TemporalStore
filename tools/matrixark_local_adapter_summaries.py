# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_LocalAdapterSummariesMixin methods split from matrixark_mcp_local_adapter.MatrixArkLocalAdapter (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # names owned by the parent module
    from tools.matrixark_mcp_local_adapter import (
    Any,
    async_summary_progress_records,
    compression_context_index_records,
    compression_profile_layer_values,
    pending_dirty_node_records,
    source_event_lineage_summary,
)
except ImportError:
    from matrixark_mcp_local_adapter import (
    Any,
    async_summary_progress_records,
    compression_context_index_records,
    compression_profile_layer_values,
    pending_dirty_node_records,
    source_event_lineage_summary,
)


class _LocalAdapterSummariesMixin:
    def node_summary_source_records(
        self,
        *,
        records: list[Json],
        node_path: list[str],
        scope: Json,
        node_hash: int | None = None,
        max_events: int = 8,
        max_child_summaries: int = 8,
        max_entity_states: int = 6,
        max_operator_states: int = 4,
    ) -> tuple[list[Json], list[Json], list[Json], list[Json], Json]:
        prefix = node_path_tuple(node_path)
        target_node_hash = int(node_hash) if node_hash is not None else stable_hash("/".join(node_path))
        direct_child_hashes: set[int] = set()
        for record in records:
            if record.get("record_type") != "context_child_ref":
                continue
            if not scope_matches(candidate_access_scope(record), scope):
                continue
            try:
                parent_hash = int(record.get("parent_hash") or 0)
                child_hash = int(record.get("child_hash") or 0)
            except (TypeError, ValueError):
                continue
            if parent_hash == target_node_hash and child_hash:
                direct_child_hashes.add(child_hash)

        child_summaries: list[Json] = []
        entity_states: list[Json] = []
        operator_states: list[Json] = []
        same_node_events: list[Json] = []
        seen_summary_keys: set[tuple[int, str]] = set()
        seen_entity_hashes: set[int] = set()
        seen_operator_hashes: set[int] = set()
        summary_types = {"node_l0", "node_l1", "batch_l0", "session_l0", "resource_l0", "skill_l0"}
        operator_record_types = {"context_compression_event", "context_session_boundary"}
        for record in reversed(records):
            if not scope_matches(candidate_access_scope(record), scope):
                continue
            # Ephemeral (TTL) records are meant to vanish, not be summarized -- never fold a record
            # carrying a per-record expiry into any rollup, regardless of whether it has expired yet.
            if record.get("ephemeral") or record.get("expires_at_ms"):
                continue
            record_type = str(record.get("record_type") or "")
            try:
                record_node_hash = int(record.get("node_hash") or 0)
            except (TypeError, ValueError):
                record_node_hash = 0
            record_path = node_path_tuple(record.get("node_path", []))
            is_same_node = record_node_hash == target_node_hash or (bool(record_path) and record_path == prefix)
            is_direct_child = record_node_hash in direct_child_hashes or (
                bool(record_path) and starts_with_path(record_path, prefix) and len(record_path) == len(prefix) + 1
            )
            if record_type == "context_summary" and record.get("summary_type") in summary_types:
                if len(child_summaries) >= max_child_summaries or not is_direct_child:
                    continue
                key = (record_node_hash, str(record.get("summary_type", "")))
                if key in seen_summary_keys:
                    continue
                seen_summary_keys.add(key)
                child_summaries.append(record)
                continue
            if record_type == "context_entity" and (is_same_node or is_direct_child):
                try:
                    entity_hash = int(record.get("entity_hash") or 0)
                except (TypeError, ValueError):
                    entity_hash = 0
                if entity_hash and entity_hash in seen_entity_hashes:
                    continue
                if entity_hash:
                    seen_entity_hashes.add(entity_hash)
                entity_states.append(record)
                continue
            if record_type in operator_record_types and (is_same_node or is_direct_child):
                if len(operator_states) >= max_operator_states:
                    continue
                try:
                    operator_hash = int(
                        record.get("compression_id_hash")
                        or record.get("boundary_hash")
                        or record.get("ref_hash")
                        or 0
                    )
                except (TypeError, ValueError):
                    operator_hash = 0
                if operator_hash and operator_hash in seen_operator_hashes:
                    continue
                if operator_hash:
                    seen_operator_hashes.add(operator_hash)
                operator_states.append(record)
                continue
            if record_type == "context_event" and is_same_node and len(same_node_events) < max_events:
                same_node_events.append(record)

        # Parent nodes summarize direct child summaries plus compact state. They
        # do not recursively scan raw leaf events. Leaf or summary-missing nodes
        # can still use their own direct recent events as a fallback source.
        use_direct_events = not child_summaries
        events = same_node_events if use_direct_events else []
        codex_entity_priority = {
            "tool_evidence": 0,
            "codex_validation": 1,
            "codex_publish_outcome": 2,
            "assistant_decision": 3,
            "codex_code_change": 4,
            "codex_blocker": 5,
            "codex_next_action": 6,
        }

        def entity_state_sort_key(record: Json) -> tuple[int, int]:
            entity_type = str(record.get("entity_type") or "")
            profile_kind = str(record.get("profile_memory_kind") or "")
            try:
                updated_at_ms = int(record.get("updated_at_ms") or record.get("created_at_ms") or 0)
            except (TypeError, ValueError):
                updated_at_ms = 0
            return (
                codex_entity_priority.get(entity_type, 20 if profile_kind == "codex_outcome" else 40),
                -updated_at_ms,
            )

        selected_entity_states = sorted(entity_states, key=entity_state_sort_key)[:max_entity_states]
        policy = {
            "source_policy": "child_summaries_plus_state" if child_summaries else "direct_events_fallback",
            "raw_recursive_leaf_event_scan": False,
            "direct_child_count": len(direct_child_hashes),
            "used_direct_event_count": len(events),
            "used_child_summary_count": len(child_summaries),
            "used_entity_state_count": len(selected_entity_states),
            "used_operator_state_count": len(operator_states),
        }
        return (
            list(reversed(events[:max_events])),
            list(reversed(child_summaries[:max_child_summaries])),
            list(reversed(selected_entity_states)),
            list(reversed(operator_states[:max_operator_states])),
            policy,
        )

    def context_event_ingestion_time_ms(self, record: Json, debug_by_ref: dict[Any, Json] | None = None) -> int:
        event_hash = record.get("event_id_hash")
        debug_payload = (debug_by_ref or {}).get(event_hash, {}) if event_hash is not None else {}
        envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else debug_payload.get("envelope", {})
        if not isinstance(envelope, dict):
            envelope = {}
        for value in (envelope.get("ingestion_time_ms"), record.get("updated_at_ms"), record.get("created_at_ms")):
            try:
                timestamp = int(value)
            except (TypeError, ValueError):
                continue
            if timestamp > 0:
                return timestamp
        return 0

    def _write_time_compression_from_events(
        self,
        *,
        scope: Json,
        node_hash: int,
        node_path: list[str],
        selected: list[Json],
        event_times: dict[int, int],
        compressed_time_ms: int,
        summary: str = "",
        truncated: bool = False,
        mode: str = "manual",
        raw_event_ttl_after_compression_ms: int = TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS,
        summary_provider_meta: Json | None = None,
    ) -> Json:
        if not selected:
            raise MatrixArkError("no source events matched compression window")
        source_event_ids = [int(record["event_id_hash"]) for record in selected if record.get("event_id_hash") is not None]
        if not source_event_ids:
            raise MatrixArkError("source events need event_id_hash for compression")
        source_times = [event_times.get(event_id, 0) for event_id in source_event_ids if event_times.get(event_id, 0) > 0]
        source_start_ms = min(source_times) if source_times else compressed_time_ms
        source_end_ms = max(source_times) if source_times else compressed_time_ms
        if not summary:
            snippets = [summarize_text(str(record.get("text", "")), limit=180) for record in selected[:5]]
            suffix = " plus additional source events" if truncated else ""
            summary = (
                f"Temporal compression window [{source_start_ms}, {source_end_ms}] contains "
                f"{len(selected)} selected events{suffix}. " + " | ".join(snippets)
            )
        compression_id_hash = stable_hash(f"compress:{scope}:{node_hash}:{source_start_ms}:{source_end_ms}:{source_event_ids}")
        source_lineage = source_event_lineage_summary(selected)
        profile_lineage = compression_profile_layer_values(selected)
        record = {
            "record_type": "context_compression_event",
            "compression_id_hash": compression_id_hash,
            "node_hash": node_hash,
            "node_path": node_path,
            "scope": scope,
            "source_start_ms": source_start_ms,
            "source_end_ms": source_end_ms,
            "compressed_time_ms": compressed_time_ms,
            "summary_text": summarize_text(summary, limit=1200),
            "source_event_ids": source_event_ids,
            "source_event_count": len(source_event_ids),
            **source_lineage,
            **profile_lineage,
            "truncated_source_events": truncated,
            "operator": "TIME_COMPRESS",
            "compression_mode": mode,
            "summary_provider": summary_provider_meta
            or {
                "provider": "deterministic",
                "model": "",
                "fallback_used": False,
            },
            "compression_safety": {
                "source_event_ids_retained": bool(source_event_ids),
                "source_event_count": len(source_event_ids),
                "summary_non_empty": bool(summary.strip()),
                "raw_events_remain_replayable": True,
                "ttl_marker_only": True,
            },
            "retention_policy": {
                "raw_event_ttl_after_compression_ms": max(0, int(raw_event_ttl_after_compression_ms)),
                "evict_after_ms": compressed_time_ms + max(0, int(raw_event_ttl_after_compression_ms))
                if raw_event_ttl_after_compression_ms > 0
                else 0,
                "requires_no_recent_reinforcement": True,
            },
            "updated_at_ms": compressed_time_ms,
        }
        self.append(record)
        summary_vector = embedding_for_text(record["summary_text"])
        self.append(
            {
                "record_type": "context_embedding",
                "embedding_type": "compression_summary",
                "ref_type": "compression",
                "ref_hash": compression_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": len(summary_vector),
                "model": embedding_model_name(),
                "vector": summary_vector,
                "scope": scope,
                "operator": record.get("operator") or "TIME_COMPRESS",
                "source_roles": record.get("source_roles", []),
                "source_role_counts": record.get("source_role_counts", {}),
                "source_hook_types": record.get("source_hook_types", []),
                "source_hook_type_counts": record.get("source_hook_type_counts", {}),
                "source_codex_events": record.get("source_codex_events", []),
                "source_codex_event_counts": record.get("source_codex_event_counts", {}),
                "source_memory_selection_policies": record.get("source_memory_selection_policies", []),
                "source_memory_selection_policy_counts": record.get("source_memory_selection_policy_counts", {}),
                "source_memory_scopes": record.get("source_memory_scopes", []),
                "source_session_continuities": record.get("source_session_continuities", []),
                "source_extraction_phases": record.get("source_extraction_phases", []),
                "source_profile_memory_classes": record.get("source_profile_memory_classes", []),
                "source_profile_memory_kinds": record.get("source_profile_memory_kinds", []),
                "profile_memory_class": record.get("profile_memory_class", ""),
                "profile_memory_kind": record.get("profile_memory_kind", ""),
                "memory_scope": record.get("memory_scope", ""),
                "session_continuity": record.get("session_continuity", ""),
                "updated_at_ms": compressed_time_ms,
            }
        )
        self.append_many(compression_context_index_records(record))
        retention_records = []
        evict_after_ms = int(record["retention_policy"]["evict_after_ms"] or 0)
        for event_id in source_event_ids:
            retention_records.append(
                {
                    "record_type": "context_event_retention_marker",
                    "event_id_hash": event_id,
                    "compression_id_hash": compression_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": scope,
                    "retention_state": "compressed_retained",
                    "evict_after_ms": evict_after_ms,
                    "raw_events_remain_replayable": True,
                    "requires_no_recent_reinforcement": True,
                    "created_at_ms": compressed_time_ms,
                    "updated_at_ms": compressed_time_ms,
                }
            )
        if retention_records:
            self.append_many(retention_records)
        return record

    def auto_time_compress_node_events(
        self,
        *,
        records: list[Json],
        scope: Json,
        node_hash: int,
        node_path: list[str],
        compressed_time_ms: int,
        max_raw_events_per_node: int = TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE,
        max_source_events: int = TIME_COMPRESSION_WINDOW_EVENTS,
        min_source_events: int = TIME_COMPRESSION_MIN_EVENTS,
        max_windows: int = TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH,
        min_event_age_ms: int = TIME_COMPRESSION_MIN_EVENT_AGE_MS,
        raw_event_ttl_after_compression_ms: int = TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS,
    ) -> Json:
        max_raw_events_per_node = max(1, int(max_raw_events_per_node))
        max_source_events = max(1, int(max_source_events))
        min_source_events = max(1, int(min_source_events))
        max_windows = max(0, int(max_windows))
        if max_windows <= 0:
            return {"status": "disabled", "created_count": 0, "created": []}
        debug_by_ref = {
            record.get("ref_hash"): record.get("debug_payload", {})
            for record in records
            if record.get("record_type") == "context_debug_record" and record.get("ref_type") == "event"
        }
        compressed_source_ids: set[int] = set()
        reinforced_source_ids: set[int] = set()
        for record in records:
            if record.get("record_type") != "context_compression_event":
                if record.get("record_type") == "context_recall_reinforcement":
                    if int(record.get("node_hash") or 0) != node_hash:
                        continue
                    if not scope_matches(candidate_access_scope(record), scope):
                        continue
                    if int(record.get("protected_until_ms") or 0) < compressed_time_ms:
                        continue
                    try:
                        reinforced_source_ids.add(int(record.get("event_id_hash")))
                    except (TypeError, ValueError):
                        pass
                continue
            if int(record.get("node_hash") or 0) != node_hash:
                continue
            if not scope_matches(candidate_access_scope(record), scope):
                continue
            for event_id in record.get("source_event_ids", []) or []:
                try:
                    compressed_source_ids.add(int(event_id))
                except (TypeError, ValueError):
                    pass
        events: list[Json] = []
        event_times: dict[int, int] = {}
        event_scopes: dict[int, Json] = {}
        for record in records:
            if record.get("record_type") != "context_event":
                continue
            if int(record.get("node_hash") or 0) != node_hash:
                continue
            if not scope_matches(candidate_access_scope(record), scope):
                continue
            try:
                event_hash = int(record.get("event_id_hash"))
            except (TypeError, ValueError):
                continue
            event_time = self.context_event_ingestion_time_ms(record, debug_by_ref)
            if event_time <= 0:
                continue
            events.append(record)
            event_times[event_hash] = event_time
            event_scopes[event_hash] = candidate_access_scope(record)
        events.sort(key=lambda record: (event_times.get(int(record.get("event_id_hash") or 0), 0), int(record.get("event_id_hash") or 0)))
        if len(events) <= max_raw_events_per_node:
            return {
                "status": "skipped",
                "reason": "raw_event_count_within_threshold",
                "raw_event_count": len(events),
                "max_raw_events_per_node": max_raw_events_per_node,
                "created_count": 0,
                "created": [],
            }
        newest_raw_ids = {
            int(record.get("event_id_hash"))
            for record in events[-max_raw_events_per_node:]
            if record.get("event_id_hash") is not None
        }
        cold_cutoff_ms = compressed_time_ms - max(0, int(min_event_age_ms))
        old_uncompressed = [
            record
            for record in events
            if int(record.get("event_id_hash") or 0) not in newest_raw_ids
            and int(record.get("event_id_hash") or 0) not in compressed_source_ids
            and int(record.get("event_id_hash") or 0) not in reinforced_source_ids
            and (
                min_event_age_ms <= 0
                or event_times.get(int(record.get("event_id_hash") or 0), compressed_time_ms) <= cold_cutoff_ms
            )
        ]
        created: list[Json] = []
        for window_start in range(0, len(old_uncompressed), max_source_events):
            if len(created) >= max_windows:
                break
            window = old_uncompressed[window_start : window_start + max_source_events]
            if len(window) < min_source_events:
                continue
            first_hash = int(window[0].get("event_id_hash") or 0)
            compression_scope = event_scopes.get(first_hash, scope)
            source_ids = [int(record["event_id_hash"]) for record in window if record.get("event_id_hash") is not None]
            source_times = [event_times.get(event_id, 0) for event_id in source_ids if event_times.get(event_id, 0) > 0]
            summary_result = generate_time_compression_summary(
                node_path=node_path,
                source_start_ms=min(source_times) if source_times else compressed_time_ms,
                source_end_ms=max(source_times) if source_times else compressed_time_ms,
                event_texts=[str(record.get("text", "")) for record in window if record.get("text")],
                max_raw_events_per_node=max_raw_events_per_node,
            )
            created.append(
                self._write_time_compression_from_events(
                    scope=compression_scope,
                    node_hash=node_hash,
                    node_path=node_path,
                    selected=window,
                    event_times=event_times,
                    compressed_time_ms=compressed_time_ms,
                    summary=str(summary_result.get("summary", "")),
                    truncated=len(old_uncompressed) > len(source_ids),
                    mode="automatic",
                    raw_event_ttl_after_compression_ms=raw_event_ttl_after_compression_ms,
                    summary_provider_meta={
                        "provider": summary_result.get("provider", "deterministic"),
                        "model": summary_result.get("model", ""),
                        "fallback_used": bool(summary_result.get("fallback_used", False)),
                        "warning": summary_result.get("warning", ""),
                    },
                )
            )
        return {
            "status": "ok" if created else "skipped",
            "reason": "" if created else "no_uncompressed_old_window_met_minimum",
            "raw_event_count": len(events),
            "max_raw_events_per_node": max_raw_events_per_node,
            "min_event_age_ms": max(0, int(min_event_age_ms)),
            "cold_cutoff_ms": cold_cutoff_ms,
            "old_uncompressed_event_count": len(old_uncompressed),
            "reinforced_event_count": len(reinforced_source_ids),
            "created_count": len(created),
            "created": [
                {
                    "compression_id_hash": item.get("compression_id_hash"),
                    "source_start_ms": item.get("source_start_ms"),
                    "source_end_ms": item.get("source_end_ms"),
                    "source_event_count": item.get("source_event_count"),
                }
                for item in created
            ],
        }

    def node_summary_dirty_records(
        self,
        *,
        node_path: list[str],
        scope: Json,
        updated_at_ms: int,
        source_ref_type: str,
        source_hash_field: str,
        source_hash: int,
        dirty_reason: str = "new_event",
        propagate_depth: int | None = None,
        source_lineage: Json | None = None,
    ) -> tuple[list[int], list[Json]]:
        prefixes = node_prefixes(node_path)
        if propagate_depth is not None and propagate_depth >= 0:
            prefixes = prefixes[max(0, len(prefixes) - propagate_depth - 1) :]
        dirty_hashes: list[int] = []
        records: list[Json] = []
        lineage = source_event_lineage_summary([source_lineage]) if isinstance(source_lineage, dict) else {}
        for prefix in prefixes:
            node_hash = stable_hash("/".join(prefix))
            dirty_hash = stable_hash(
                f"summary_dirty:{node_hash}:{dirty_reason}:{source_ref_type}:{source_hash}:{updated_at_ms}"
            )
            dirty_hashes.append(dirty_hash)
            record = {
                "record_type": "context_summary_dirty",
                "dirty_hash": dirty_hash,
                "node_hash": node_hash,
                "node_path": prefix,
                "scope": scope,
                "status": "pending",
                "dirty_reason": dirty_reason,
                "source_ref_type": source_ref_type,
                source_hash_field: source_hash,
                "created_at_ms": updated_at_ms,
                "updated_at_ms": updated_at_ms,
            }
            for field in [
                "source_memory_scopes",
                "source_session_continuities",
                "source_extraction_phases",
                "source_profile_promotion_policies",
                "memory_scope",
                "session_continuity",
                "extraction_phase",
                "final_session_boundary",
            ]:
                value = lineage.get(field)
                if value not in (None, "", [], {}):
                    record[field] = value
            if ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS or ENABLE_SUMMARY_REFRESH_AUDIT or ENABLE_CONTEXT_DEBUG_RECORDS:
                record.update(
                    {
                        "depth": len(prefix),
                        "changed_ref_count": 1,
                        "propagate_depth": propagate_depth if propagate_depth is not None else len(node_path),
                    }
                )
                for field in [
                    "source_role",
                    "source_roles",
                    "source_role_counts",
                    "source_hook_types",
                    "source_hook_type_counts",
                    "source_codex_events",
                    "source_codex_event_counts",
                    "source_memory_selection_policies",
                    "source_memory_selection_policy_counts",
                    "source_profile_promotion_blockers",
                    "source_final_session_boundary_count",
                ]:
                    value = lineage.get(field)
                    if value not in (None, "", [], {}):
                        record[field] = value
            records.append(record)
        return dirty_hashes, records

    def mark_node_summary_dirty(
        self,
        *,
        node_path: list[str],
        scope: Json,
        updated_at_ms: int,
        source_ref_type: str,
        source_hash_field: str,
        source_hash: int,
        dirty_reason: str = "new_event",
        propagate_depth: int | None = None,
        source_lineage: Json | None = None,
    ) -> list[int]:
        dirty_hashes, records = self.node_summary_dirty_records(
            node_path=node_path,
            scope=scope,
            updated_at_ms=updated_at_ms,
            source_ref_type=source_ref_type,
            source_hash_field=source_hash_field,
            source_hash=source_hash,
            dirty_reason=dirty_reason,
            propagate_depth=propagate_depth,
            source_lineage=source_lineage,
        )
        self.append_many(records)
        return dirty_hashes

    def refresh_dirty_node_summaries(
        self,
        *,
        scope: Json,
        limit: int = 64,
        refreshed_at_ms: int | None = None,
        max_raw_events_per_node: int = TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE,
        compression_window_events: int = TIME_COMPRESSION_WINDOW_EVENTS,
        min_compression_events: int = TIME_COMPRESSION_MIN_EVENTS,
        max_compression_windows_per_node: int = TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH,
        min_compression_event_age_ms: int = TIME_COMPRESSION_MIN_EVENT_AGE_MS,
        raw_event_ttl_after_compression_ms: int = TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS,
        skip_dirty_reasons: set[str] | None = None,
        records: list[Json] | None = None,
    ) -> Json:
        refreshed_at_ms = refreshed_at_ms or now_ms()
        skip_dirty_reasons = skip_dirty_reasons or set()
        # `records` lets one caller's read serve a whole pass. Reading here is a full record-log
        # read, and on a native backend it holds the single shared proxy lane while it runs.
        records = self.read_all() if records is None else records
        skipped_dirty_reasons: Json = {}
        pending_by_node = pending_dirty_node_records(
            records=records,
            scope=scope,
            limit=limit,
            refreshed_at_ms=refreshed_at_ms,
            max_raw_events_per_node=max_raw_events_per_node,
            min_compression_event_age_ms=min_compression_event_age_ms,
            context_event_ingestion_time_ms=self.context_event_ingestion_time_ms,
            skip_dirty_reasons=skip_dirty_reasons,
            skipped_dirty_reasons=skipped_dirty_reasons,
        )
        refreshed = []
        # A pass is bounded by TIME as well as node count: each node costs source-record scans and
        # appends on the lanes foreground requests use, so an unbounded pass over a deep backlog
        # IS the foreground latency (measured: add p50 158s with the refresher churning vs 27.7s
        # without, same store). Leftover nodes are the next pass's work, not lost work.
        import os as _os
        import time as _time
        try:
            pass_budget_ms = int(_os.environ.get("MATRIXARK_SUMMARY_REFRESH_PASS_BUDGET_MS", "30000"))
        except (TypeError, ValueError):
            pass_budget_ms = 30000
        pass_deadline = (_time.monotonic() + pass_budget_ms / 1000.0) if pass_budget_ms > 0 else None
        pass_budget_exhausted = False
        selected_dirty = sorted(pending_by_node.values(), key=lambda item: int(item.get("updated_at_ms") or 0))[:limit]
        for dirty_index, dirty in enumerate(selected_dirty):
            if pass_deadline is not None and _time.monotonic() > pass_deadline:
                pass_budget_exhausted = True
                skipped_dirty_reasons["pass_budget_exhausted"] = (
                    int(skipped_dirty_reasons.get("pass_budget_exhausted") or 0)
                    + (len(selected_dirty) - dirty_index)
                )
                break
            node_path = [str(part) for part in dirty.get("node_path", [])]
            if not node_path:
                continue
            node_hash = int(dirty["node_hash"])
            events, child_summaries, entity_states, operator_states, summary_source_policy = self.node_summary_source_records(
                records=records,
                node_path=node_path,
                scope=dirty.get("scope", scope),
                node_hash=node_hash,
            )
            event_texts = [str(record.get("text", "")) for record in events if record.get("text")]
            child_summary_texts = [
                str(record.get("summary_text", ""))
                for record in child_summaries
                if record.get("summary_text")
            ]
            entity_state_texts = [
                summarize_text(
                    f"{record.get('entity_type', 'entity')} {record.get('entity_name', '')}: {record.get('state', '')}",
                    limit=240,
                )
                for record in entity_states
                if record.get("state")
            ]
            operator_state_texts = [
                summarize_text(
                    f"{record.get('operator', 'operator')}: {record.get('summary_text') or record.get('text') or ''}",
                    limit=260,
                )
                for record in operator_states
                if record.get("summary_text") or record.get("text")
            ]
            source_state_records = events + entity_states + child_summaries + operator_states
            source_text = " ".join(child_summary_texts + entity_state_texts + operator_state_texts + event_texts)
            if not source_text:
                source_text = " ".join(node_path)
            prefix_label = " / ".join(node_path)
            source_event_ids = [int(record["event_id_hash"]) for record in events if record.get("event_id_hash") is not None]
            source_summary_hashes = [
                int(record.get("summary_hash") or record.get("node_hash"))
                for record in child_summaries
                if record.get("summary_hash") is not None or record.get("node_hash") is not None
            ]
            source_entity_hashes = [
                int(record.get("entity_hash"))
                for record in entity_states
                if record.get("entity_hash") is not None
            ]
            source_entity_types = sorted(
                {
                    str(record.get("entity_type"))
                    for record in entity_states
                    if str(record.get("entity_type") or "").strip()
                }
            )
            source_roles = sorted(
                {
                    normalize_message_role(role)
                    for record in source_state_records
                    for role in (
                        record.get("source_roles")
                        if isinstance(record.get("source_roles"), list)
                        else [record.get("source_role")]
                    )
                    if normalize_message_role(role)
                }
            )
            source_role_counts: Json = {}
            for record in source_state_records:
                counts = record.get("source_role_counts") if isinstance(record.get("source_role_counts"), dict) else {}
                for role, count in counts.items():
                    role_name = normalize_message_role(role)
                    if not role_name:
                        continue
                    try:
                        source_role_counts[role_name] = int(source_role_counts.get(role_name, 0)) + max(0, int(count or 0))
                    except (TypeError, ValueError):
                        continue
                if not counts:
                    fallback_roles = (
                        record.get("source_roles")
                        if isinstance(record.get("source_roles"), list)
                        else [record.get("source_role")]
                    )
                    for role in fallback_roles:
                        role_name = normalize_message_role(role)
                        if role_name:
                            source_role_counts[role_name] = int(source_role_counts.get(role_name, 0)) + 1
            source_hook_types = sorted(
                {
                    str(hook_type).strip()
                    for record in source_state_records
                    for hook_type in (
                        record.get("source_hook_types")
                        if isinstance(record.get("source_hook_types"), list)
                        else [record.get("hook_type")]
                    )
                    if str(hook_type or "").strip()
                }
            )
            source_hook_type_counts: Json = {}
            for record in source_state_records:
                counts = record.get("source_hook_type_counts") if isinstance(record.get("source_hook_type_counts"), dict) else {}
                for hook_type, count in counts.items():
                    hook_name = str(hook_type or "").strip()
                    if not hook_name:
                        continue
                    try:
                        source_hook_type_counts[hook_name] = int(source_hook_type_counts.get(hook_name, 0)) + max(0, int(count or 0))
                    except (TypeError, ValueError):
                        continue
                if not counts:
                    fallback_hook_types = (
                        record.get("source_hook_types")
                        if isinstance(record.get("source_hook_types"), list)
                        else [record.get("hook_type")]
                    )
                    for hook_type in fallback_hook_types:
                        hook_name = str(hook_type or "").strip()
                        if hook_name:
                            source_hook_type_counts[hook_name] = int(source_hook_type_counts.get(hook_name, 0)) + 1
            source_codex_events = sorted(
                {
                    str(codex_event).strip()
                    for record in source_state_records
                    for codex_event in (
                        record.get("source_codex_events")
                        if isinstance(record.get("source_codex_events"), list)
                        else [record.get("codex_event")]
                    )
                    if str(codex_event or "").strip()
                }
            )
            source_codex_event_counts: Json = {}
            for record in source_state_records:
                counts = record.get("source_codex_event_counts") if isinstance(record.get("source_codex_event_counts"), dict) else {}
                for codex_event, count in counts.items():
                    event_name = str(codex_event or "").strip()
                    if not event_name:
                        continue
                    try:
                        source_codex_event_counts[event_name] = int(source_codex_event_counts.get(event_name, 0)) + max(0, int(count or 0))
                    except (TypeError, ValueError):
                        continue
                if not counts:
                    fallback_codex_events = (
                        record.get("source_codex_events")
                        if isinstance(record.get("source_codex_events"), list)
                        else [record.get("codex_event")]
                    )
                    for codex_event in fallback_codex_events:
                        event_name = str(codex_event or "").strip()
                        if event_name:
                            source_codex_event_counts[event_name] = int(source_codex_event_counts.get(event_name, 0)) + 1
            source_memory_selection_policy_counts: Json = {}
            for record in source_state_records:
                counts = (
                    record.get("source_memory_selection_policy_counts")
                    if isinstance(record.get("source_memory_selection_policy_counts"), dict)
                    else {}
                )
                for policy, count in counts.items():
                    policy_name = str(policy or "").strip()
                    if not policy_name:
                        continue
                    try:
                        source_memory_selection_policy_counts[policy_name] = int(
                            source_memory_selection_policy_counts.get(policy_name, 0)
                        ) + max(0, int(count or 0))
                    except (TypeError, ValueError):
                        continue
                if not counts:
                    values = (
                        record.get("source_memory_selection_policies")
                        if isinstance(record.get("source_memory_selection_policies"), list)
                        else []
                    )
                    for policy in values:
                        policy_name = str(policy or "").strip()
                        if policy_name:
                            source_memory_selection_policy_counts[policy_name] = int(
                                source_memory_selection_policy_counts.get(policy_name, 0)
                            ) + 1
            source_memory_selection_policies = sorted(source_memory_selection_policy_counts)
            def source_layer_values(list_field: str, fallback_field: str) -> list[str]:
                values: set[str] = set()
                for record in source_state_records:
                    raw_values = (
                        record.get(list_field)
                        if isinstance(record.get(list_field), list)
                        else []
                    )
                    for value in raw_values:
                        text = str(value or "").strip()
                        if text:
                            values.add(text)
                    fallback = str(record.get(fallback_field) or "").strip()
                    if fallback:
                        values.add(fallback)
                return sorted(values)

            source_memory_scopes = source_layer_values("source_memory_scopes", "memory_scope")
            source_session_continuities = source_layer_values("source_session_continuities", "session_continuity")
            source_extraction_phases = sorted(
                {
                    str(record.get("extraction_phase") or "").strip()
                    for record in source_state_records
                    if str(record.get("extraction_phase") or "").strip()
                }
            )
            source_final_session_boundary_count = sum(
                1
                for record in source_state_records
                if bool(record.get("final_session_boundary"))
            )
            source_profile_promotion_policies = sorted(
                {
                    str(value).strip()
                    for record in entity_states + child_summaries
                    for value in (
                        record.get("source_profile_promotion_policies")
                        if isinstance(record.get("source_profile_promotion_policies"), list)
                        else [record.get("profile_promotion_policy")]
                    )
                    if str(value or "").strip()
                }
            )
            source_profile_promotion_blockers = sorted(
                {
                    str(value).strip()
                    for record in entity_states + child_summaries
                    for value in (
                        record.get("source_profile_promotion_blockers")
                        if isinstance(record.get("source_profile_promotion_blockers"), list)
                        else [record.get("profile_promotion_blocker")]
                    )
                    if str(value or "").strip()
                }
            )

            def source_profile_layer_values(list_field: str, fallback_field: str) -> list[str]:
                values: set[str] = set()
                for record in entity_states + child_summaries:
                    raw_values = record.get(list_field) if isinstance(record.get(list_field), list) else []
                    for value in raw_values:
                        text_value = str(value or "").strip()
                        if text_value:
                            values.add(text_value)
                    fallback = str(record.get(fallback_field) or "").strip()
                    if fallback:
                        values.add(fallback)
                return sorted(values)

            source_profile_memory_classes = source_profile_layer_values("source_profile_memory_classes", "profile_memory_class")
            source_profile_memory_kinds = source_profile_layer_values("source_profile_memory_kinds", "profile_memory_kind")
            profile_memory_class = source_profile_memory_classes[0] if len(source_profile_memory_classes) == 1 else ("mixed" if source_profile_memory_classes else "")
            profile_memory_kind = (
                "codex_outcome"
                if "codex_outcome" in source_profile_memory_kinds
                else source_profile_memory_kinds[0]
                if len(source_profile_memory_kinds) == 1
                else "mixed"
                if source_profile_memory_kinds
                else ""
            )
            source_operator_hashes = [
                int(record.get("compression_id_hash") or record.get("boundary_hash") or record.get("ref_hash"))
                for record in operator_states
                if (
                    record.get("compression_id_hash") is not None
                    or record.get("boundary_hash") is not None
                    or record.get("ref_hash") is not None
                )
            ]
            l1_policy = node_l1_generation_policy(
                source_text=source_text,
                event_count=len(source_event_ids),
                child_summary_count=len(source_summary_hashes),
            )
            l1_policy = {**l1_policy, **summary_source_policy}
            l0_summary, l0_provider_meta = synthesize_context_node_summary(
                level="node_l0",
                node_path=node_path,
                source_text=source_text,
                fallback_text=f"{prefix_label} :: {source_text}",
                max_chars=220,
                policy=l1_policy,
            )
            summary_specs = [("node_l0", l0_summary, "node_l0", l0_provider_meta)]
            if l1_policy["generate_l1"]:
                l1_summary, l1_provider_meta = synthesize_context_node_summary(
                    level="node_l1",
                    node_path=node_path,
                    source_text=source_text,
                    fallback_text=(
                        f"Context node {prefix_label}. Rich overview: {source_text}. "
                        f"This node belongs to path {prefix_label} and should be used for tree-first retrieval before leaf event/entity recall."
                    ),
                    max_chars=1200,
                    policy=l1_policy,
                )
                summary_specs.append(("node_l1", l1_summary, "node_l1", l1_provider_meta))
            for level, summary_text, embedding_type, provider_meta in summary_specs:
                summary_hash = stable_hash(f"context_summary:{level}:{node_hash}")
                summary_policy = {**l1_policy, **provider_meta}
                profile_summary_current = (
                    "user_profile" in source_memory_scopes
                    or any(str(part).startswith("profile:") for part in node_path)
                )
                summary_record = {
                    "record_type": "context_summary",
                    "summary_type": level,
                    "summary_hash": summary_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "depth": len(node_path),
                    "summary_text": summary_text,
                    "source_event_ids": source_event_ids,
                    "source_summary_hashes": source_summary_hashes,
                    "source_entity_hashes": source_entity_hashes,
                    "source_entity_types": source_entity_types,
                    "source_roles": source_roles,
                    "source_role_counts": source_role_counts,
                    "source_hook_types": source_hook_types,
                    "source_hook_type_counts": source_hook_type_counts,
                    "source_codex_events": source_codex_events,
                    "source_codex_event_counts": source_codex_event_counts,
                    "source_memory_selection_policies": source_memory_selection_policies,
                    "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
                    "source_memory_scopes": source_memory_scopes,
                    "source_session_continuities": source_session_continuities,
                    "source_extraction_phases": source_extraction_phases,
                    "source_profile_promotion_policies": source_profile_promotion_policies,
                    "source_profile_promotion_blockers": source_profile_promotion_blockers,
                    "source_profile_memory_classes": source_profile_memory_classes,
                    "source_profile_memory_kinds": source_profile_memory_kinds,
                    "profile_memory_class": profile_memory_class,
                    "profile_memory_kind": profile_memory_kind,
                    "source_final_session_boundary_count": source_final_session_boundary_count,
                    "memory_scope": "user_profile" if "user_profile" in source_memory_scopes else ("session" if "session" in source_memory_scopes else ""),
                    "session_continuity": "cross_session" if "cross_session" in source_session_continuities else ("same_session" if "same_session" in source_session_continuities else ""),
                    "extraction_phase": "final" if "final" in source_extraction_phases else ("provisional" if "provisional" in source_extraction_phases else ""),
                    "final_session_boundary": source_final_session_boundary_count > 0,
                    "profile_summary_current": profile_summary_current,
                    "source_operator_hashes": source_operator_hashes,
                    "summary_generation_policy": summary_policy,
                    "dirty_hash": dirty.get("dirty_hash"),
                    "scope": dirty.get("scope", scope),
                    "updated_at_ms": refreshed_at_ms,
                }
                self.append(summary_record)
                # Lever 1. The events this summary rolls up no longer need postings of their own:
                # the summary carries its own, and those are the surviving recall path. The
                # tombstone names exactly the events covered, and the serving sweep drops their
                # postings.
                #
                # `index_compaction_tombstone` returns None when there is nothing to compact, so a
                # summary with no source events appends nothing and the log stays byte-identical to
                # what it was before this.
                try:
                    from tools.matrixark_index_growth_bound import (
                        index_compact_on_summary_enabled, index_compaction_tombstone)
                except ImportError:  # Direct script execution from tools/.
                    from matrixark_index_growth_bound import (
                        index_compact_on_summary_enabled, index_compaction_tombstone)
                summary_scope = summary_record.get("scope")
                if index_compact_on_summary_enabled(summary_scope):
                    compaction = index_compaction_tombstone(
                        source_event_ids=source_event_ids,
                        scope=summary_scope if isinstance(summary_scope, dict) else None,
                        summary_hash=summary_hash,
                    )
                    if compaction is not None:
                        self.append(compaction)
                for index_name in candidate_index_terms(summary_record, {}, {}):
                    self.append(
                        context_index_posting_record(
                            index_name=index_name,
                            data_model="context_summary",
                            ref_type="summary",
                            ref_hashes=[summary_hash],
                            node_hash=node_hash,
                            scope=dirty.get("scope", scope),
                            updated_at_ms=refreshed_at_ms,
                        )
                    )
                summary_vector = embedding_for_text(summary_text)
                self.append(
                    {
                        "record_type": "context_embedding",
                        "embedding_type": embedding_type,
                        "ref_type": "summary",
                        "ref_hash": summary_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "depth": len(node_path),
                        "dim": len(summary_vector),
                        "model": embedding_model_name(),
                        "vector": summary_vector,
                        "scope": dirty.get("scope", scope),
                        "summary_type": level,
                        "source_entity_types": source_entity_types,
                        "source_roles": source_roles,
                        "source_role_counts": source_role_counts,
                        "source_hook_types": source_hook_types,
                        "source_hook_type_counts": source_hook_type_counts,
                        "source_codex_events": source_codex_events,
                        "source_codex_event_counts": source_codex_event_counts,
                        "source_memory_selection_policies": source_memory_selection_policies,
                        "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
                        "source_memory_scopes": source_memory_scopes,
                        "source_session_continuities": source_session_continuities,
                        "source_extraction_phases": source_extraction_phases,
                        "source_profile_promotion_policies": source_profile_promotion_policies,
                        "source_profile_promotion_blockers": source_profile_promotion_blockers,
                        "source_profile_memory_classes": source_profile_memory_classes,
                        "source_profile_memory_kinds": source_profile_memory_kinds,
                        "profile_memory_class": profile_memory_class,
                        "profile_memory_kind": profile_memory_kind,
                        "memory_scope": summary_record["memory_scope"],
                        "session_continuity": summary_record["session_continuity"],
                        "extraction_phase": summary_record["extraction_phase"],
                        "final_session_boundary": summary_record["final_session_boundary"],
                        "profile_summary_current": summary_record["profile_summary_current"],
                        "updated_at_ms": refreshed_at_ms,
                    }
                )
            compression_refresh = self.auto_time_compress_node_events(
                records=records,
                scope=dirty.get("scope", scope),
                node_hash=node_hash,
                node_path=node_path,
                compressed_time_ms=refreshed_at_ms,
                max_raw_events_per_node=max_raw_events_per_node,
                max_source_events=compression_window_events,
                min_source_events=min_compression_events,
                max_windows=max_compression_windows_per_node,
                min_event_age_ms=min_compression_event_age_ms,
                raw_event_ttl_after_compression_ms=raw_event_ttl_after_compression_ms,
            )
            completion_marker = {
                "record_type": "context_summary_dirty",
                "dirty_hash": dirty.get("dirty_hash"),
                "node_hash": node_hash,
                "node_path": node_path,
                "scope": dirty.get("scope", scope),
                "dirty_reason": dirty.get("dirty_reason", ""),
                "source_ref_type": dirty.get("source_ref_type", ""),
                "changed_ref_count": dirty.get("changed_ref_count", 0),
                "empty_summary_seen": bool(dirty.get("empty_summary_seen", False)),
                "status": "completed",
                "created_at_ms": refreshed_at_ms,
                "updated_at_ms": refreshed_at_ms,
                "completed_at_ms": refreshed_at_ms,
            }
            self.append(completion_marker)
            if ENABLE_SUMMARY_REFRESH_AUDIT:
                self.append(
                    {
                        "record_type": "context_summary_refresh_audit",
                        "dirty_hash": dirty.get("dirty_hash"),
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "summary_version_hash": version_hash,
                        "source_event_ids": source_event_ids,
                        "source_summary_hashes": source_summary_hashes,
                        "source_event_count": len(source_event_ids),
                        "source_summary_count": len(source_summary_hashes),
                        "source_entity_count": len(source_entity_hashes),
                        "source_entity_types": source_entity_types,
                        "source_roles": source_roles,
                        "source_role_counts": source_role_counts,
                        "source_hook_types": source_hook_types,
                        "source_hook_type_counts": source_hook_type_counts,
                        "source_codex_events": source_codex_events,
                        "source_codex_event_counts": source_codex_event_counts,
                        "source_memory_selection_policies": source_memory_selection_policies,
                        "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
                        "source_memory_scopes": source_memory_scopes,
                        "source_session_continuities": source_session_continuities,
                        "source_extraction_phases": source_extraction_phases,
                        "source_profile_promotion_policies": source_profile_promotion_policies,
                        "source_profile_promotion_blockers": source_profile_promotion_blockers,
                        "source_final_session_boundary_count": source_final_session_boundary_count,
                        "generated_summary_types": [spec[0] for spec in summary_specs],
                        "summary_generation_policy": l1_policy,
                        "time_compression_policy": {
                            "automatic": True,
                            "max_raw_events_per_node": max_raw_events_per_node,
                            "compression_window_events": compression_window_events,
                            "min_compression_events": min_compression_events,
                            "max_compression_windows_per_node": max_compression_windows_per_node,
                            "min_compression_event_age_ms": min_compression_event_age_ms,
                            "raw_event_ttl_after_compression_ms": raw_event_ttl_after_compression_ms,
                        },
                        "time_compression": compression_refresh,
                        "status": "refreshed",
                        "worker": "matrixark-local-async-summary-worker",
                        "refreshed_at_ms": refreshed_at_ms,
                        "scope": dirty.get("scope", scope),
                    }
                )
            generated_summary_types = [spec[0] for spec in summary_specs]
            summary_progress_records = async_summary_progress_records(
                records=records,
                scope=dirty.get("scope", scope),
                source_event_ids=source_event_ids,
                source_entity_hashes=source_entity_hashes,
                dirty_hash=dirty.get("dirty_hash"),
                node_hash=node_hash,
                node_path=node_path,
                generated_summary_types=generated_summary_types,
                refreshed_at_ms=refreshed_at_ms,
                completed_followup_stages=["summary", "embedding", "compression"],
            )
            if summary_progress_records:
                self.append_many(summary_progress_records)
            refreshed.append(
                {
                    "dirty_hash": dirty.get("dirty_hash"),
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "source_event_count": len(source_event_ids),
                    "source_summary_count": len(source_summary_hashes),
                    "source_entity_count": len(source_entity_hashes),
                    "source_entity_types": source_entity_types,
                    "source_roles": source_roles,
                    "source_role_counts": source_role_counts,
                    "source_hook_types": source_hook_types,
                    "source_hook_type_counts": source_hook_type_counts,
                    "source_codex_events": source_codex_events,
                    "source_codex_event_counts": source_codex_event_counts,
                    "source_memory_selection_policies": source_memory_selection_policies,
                    "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
                    "source_memory_scopes": source_memory_scopes,
                    "source_session_continuities": source_session_continuities,
                    "source_extraction_phases": source_extraction_phases,
                    "source_profile_promotion_policies": source_profile_promotion_policies,
                    "source_profile_promotion_blockers": source_profile_promotion_blockers,
                    "source_final_session_boundary_count": source_final_session_boundary_count,
                    "source_operator_count": len(source_operator_hashes),
                    "generated_summary_types": generated_summary_types,
                    "summary_generation_policy": l1_policy,
                    "time_compression": compression_refresh,
                    "async_summary_progress_count": len(summary_progress_records),
                }
            )
        return {
            "status": "ok",
            "pass_budget_exhausted": pass_budget_exhausted,
            "refreshed_count": len(refreshed),
            "compression_created_count": sum(int(item.get("time_compression", {}).get("created_count", 0)) for item in refreshed),
            "skipped_dirty_count": sum(int(count or 0) for count in skipped_dirty_reasons.values()),
            "skipped_dirty_reasons": skipped_dirty_reasons,
            "refreshed": refreshed,
        }

    def append_node_summary_embeddings(
        self,
        *,
        node_path: list[str],
        source_text: str,
        scope: Json,
        updated_at_ms: int,
        source_hash_field: str,
        source_hash: int,
    ) -> Json:
        dirty_hashes = self.mark_node_summary_dirty(
            node_path=node_path,
            scope=scope,
            updated_at_ms=updated_at_ms,
            source_ref_type=source_hash_field.removeprefix("source_").removesuffix("_hash"),
            source_hash_field=source_hash_field,
            source_hash=source_hash,
            dirty_reason="new_event",
        )
        return {
            "status": "dirty_marked",
            "dirty_hashes": dirty_hashes,
            "refresh_result": None,
            "async_required": True,
        }

    def refresh_summaries(self, args: Json) -> Json:
        scope = optional_object(args, "scope")
        limit = args.get("limit", 64)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        refreshed_at_ms = args.get("refreshed_at_ms")
        if refreshed_at_ms is not None and not isinstance(refreshed_at_ms, int):
            raise MatrixArkError("refreshed_at_ms must be an integer")
        skip_dirty_reasons = {
            str(reason or "").strip()
            for reason in args.get("skip_dirty_reasons", [])
            if str(reason or "").strip()
        } if isinstance(args.get("skip_dirty_reasons"), list) else set()
        # One read for the pass. The embedding step below reuses it only when nothing was
        # written -- see there.
        pass_records = self.records_for_summary_refresh()
        result = self.refresh_dirty_node_summaries(
            records=pass_records,
            scope=scope,
            limit=limit,
            refreshed_at_ms=refreshed_at_ms,
            max_raw_events_per_node=integer_arg(args, "max_raw_events_per_node", TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE, minimum=1),
            compression_window_events=integer_arg(args, "compression_window_events", TIME_COMPRESSION_WINDOW_EVENTS, minimum=1),
            min_compression_events=integer_arg(args, "min_compression_events", TIME_COMPRESSION_MIN_EVENTS, minimum=1),
            max_compression_windows_per_node=integer_arg(
                args,
                "max_compression_windows_per_node",
                TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH,
                minimum=0,
            ),
            min_compression_event_age_ms=integer_arg(
                args,
                "min_compression_event_age_ms",
                TIME_COMPRESSION_MIN_EVENT_AGE_MS,
                minimum=0,
            ),
            raw_event_ttl_after_compression_ms=integer_arg(
                args,
                "raw_event_ttl_after_compression_ms",
                TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS,
                minimum=0,
            ),
            skip_dirty_reasons=skip_dirty_reasons,
        )
        ensure_embeddings_arg = args.get("ensure_embeddings", True)
        ensure_embeddings = (
            bool(ensure_embeddings_arg)
            if isinstance(ensure_embeddings_arg, bool)
            else str(ensure_embeddings_arg).strip().lower() not in {"0", "false", "no", "off"}
        )
        if ensure_embeddings:
            # Reuse the snapshot ONLY when the summary pass wrote nothing: then the store is
            # unchanged and a second read returns the same records. When it did write, this must
            # re-read, because it has to see the summaries just produced -- and a row read back
            # from the store differs from the one written (serving materialization strips the
            # scope it carries), so an in-memory reconstruction would not be the same input.
            try:
                wrote_nothing = int(result.get("refreshed_count") or 0) == 0
            except (TypeError, ValueError):
                wrote_nothing = False
            result["embedding_refresh"] = self.ensure_context_embeddings(
                records=pass_records if wrote_nothing else None,
                scope=scope,
                limit=integer_arg(args, "embedding_backfill_limit", max(1, limit * 8), minimum=1),
                updated_at_ms=refreshed_at_ms,
            )
            result["embedding_refresh_reused_pass_records"] = bool(wrote_nothing)
        return result

