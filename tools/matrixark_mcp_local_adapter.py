#!/usr/bin/env python3
"""Local MatrixArk adapter and in-memory serving backend."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

try:
    from tools.matrixark_mcp_core import (
        TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE,
        TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH,
        TIME_COMPRESSION_MIN_EVENTS,
        TIME_COMPRESSION_MIN_EVENT_AGE_MS,
        TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS,
        TIME_COMPRESSION_REINFORCEMENT_PROTECT_MS,
        TIME_COMPRESSION_WINDOW_EVENTS,
        Json,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE,
        TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH,
        TIME_COMPRESSION_MIN_EVENTS,
        TIME_COMPRESSION_MIN_EVENT_AGE_MS,
        TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS,
        TIME_COMPRESSION_REINFORCEMENT_PROTECT_MS,
        TIME_COMPRESSION_WINDOW_EVENTS,
        Json,
    )

try:
    from tools.matrixark_mcp_env import env_bool
    from tools.matrixark_mcp_metrics import MatrixArkServiceMetrics
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool
    from matrixark_mcp_metrics import MatrixArkServiceMetrics

try:
    from tools.matrixark_mcp_latest_values import compact_latest_value_records, latest_value_record_key
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_latest_values import compact_latest_value_records, latest_value_record_key

try:
    from tools import matrixark_mcp_session_policy as session_policy
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_session_policy as session_policy

try:
    from tools import matrixark_mcp_visibility as visibility_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_visibility as visibility_helpers

try:
    from tools import matrixark_mcp_deadline_pack as deadline_pack_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_deadline_pack as deadline_pack_helpers

try:
    from tools import matrixark_mcp_retrieval_records as retrieval_record_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieval_records as retrieval_record_helpers

try:
    from tools import matrixark_mcp_local_retrieve_runtime as local_retrieve_runtime
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_local_retrieve_runtime as local_retrieve_runtime

try:
    from tools import matrixark_mcp_local_cache as local_cache_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_local_cache as local_cache_helpers

try:
    from tools import matrixark_mcp_local_backend as local_backend_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_local_backend as local_backend_helpers

try:
    from tools import matrixark_mcp_local_idempotency as local_idempotency_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_local_idempotency as local_idempotency_helpers

try:
    from tools import matrixark_mcp_local_replay as local_replay_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_local_replay as local_replay_helpers

try:
    from tools import matrixark_mcp_local_read as local_read_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_local_read as local_read_helpers

try:
    from tools import matrixark_mcp_local_runtime as local_runtime_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_local_runtime as local_runtime_helpers

try:
    from tools.matrixark_mcp_native_pack_policy import native_context_pack_required_for_backend
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_native_pack_policy import native_context_pack_required_for_backend

try:
    from tools.matrixark_mcp_runtime_config import MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_runtime_config import MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK

try:
    from tools import matrixark_mcp_ingest_planning as ingest_planning_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_ingest_planning as ingest_planning_helpers

try:
    from tools import matrixark_mcp_local_ingest as local_ingest_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_local_ingest as local_ingest_helpers

try:
    from tools import matrixark_mcp_batch_extract_planning as batch_extract_planning_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_batch_extract_planning as batch_extract_planning_helpers

try:
    from tools import matrixark_mcp_local_batch_extract_runtime as local_batch_extract_runtime
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_local_batch_extract_runtime as local_batch_extract_runtime

try:
    from tools import matrixark_mcp_session_runtime as session_runtime
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_session_runtime as session_runtime

try:
    from tools import matrixark_mcp_context_nodes as context_node_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_context_nodes as context_node_helpers

try:
    from tools.matrixark_mcp_local_portal_imports import MatrixArkLocalPortalImportMixin
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_local_portal_imports import MatrixArkLocalPortalImportMixin

try:
    from tools import matrixark_mcp_summary_runtime as summary_runtime
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_summary_runtime as summary_runtime

try:
    from tools import matrixark_mcp_time_compression_runtime as time_compression_runtime
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_time_compression_runtime as time_compression_runtime

RETRIEVAL_HOT_RECORD_TYPES = retrieval_record_helpers.RETRIEVAL_HOT_RECORD_TYPES

LOCAL_READ_CACHE_COPY = env_bool("MATRIXARK_LOCAL_READ_CACHE_COPY", True)
DEFAULT_SESSION_IDLE_COMMIT_TIMEOUT_MS = 5 * 60 * 1000

@dataclass
class MatrixArkLocalAdapter(MatrixArkLocalPortalImportMixin):
    event_log: Path

    def __post_init__(self) -> None:
        self._init_local_runtime_state()

    def _init_local_runtime_state(self) -> None:
        local_runtime_helpers.init_local_runtime_state(self)

    def _write_batch_stack(self) -> list[list[Json]]:
        return local_runtime_helpers.write_batch_stack(self)

    def _current_write_batch(self) -> list[Json] | None:
        return local_runtime_helpers.current_write_batch(self)

    def _queue_batched_records(self, records: list[Json]) -> bool:
        return local_runtime_helpers.queue_batched_records(self, records)

    def write_batch(self, label: str = "hot_path"):
        return local_runtime_helpers.write_batch(self, label)

    def ensure_backend_ready(self, *, reason: str = "matrixark") -> Json:
        return local_backend_helpers.ensure_backend_ready(self, reason=reason)

    def backend_metrics(self) -> Json:
        return local_backend_helpers.backend_metrics(self)

    def _observe_model_latency(self, stage: str, elapsed_ms: float) -> None:
        local_backend_helpers.observe_model_latency(self, stage, elapsed_ms)

    def _update_read_cache_after_append(self, records: list[Json]) -> None:
        local_cache_helpers.update_read_cache_after_append(self, records)

    def append(self, record: Json) -> None:
        local_runtime_helpers.append(self, record)

    def append_many(self, records: list[Json]) -> None:
        local_runtime_helpers.append_many(self, records)

    def _update_latest_entity_cache(self, records: list[Json]) -> None:
        local_cache_helpers.update_latest_entity_cache(self, records)

    def _ensure_context_node_cache_loaded(self) -> None:
        local_cache_helpers.ensure_context_node_cache_loaded(self)

    def _ensure_latest_entity_cache_loaded(self) -> None:
        local_cache_helpers.ensure_latest_entity_cache_loaded(self)

    def append_audit(self, record: Json) -> None:
        self.append(record)

    def telemetry_record_for_context_pack(self, pack: Json, *, query: str, scope: Json, audit_mode: str) -> Json:
        return visibility_helpers.telemetry_record_for_context_pack(
            pack,
            query=query,
            scope=scope,
            audit_mode=audit_mode,
        )

    def append_context_pack_visibility(
        self,
        *,
        pack: Json,
        audit_record: Json,
        query: str,
        scope: Json,
        audit_mode: str,
        audit_sample_rate: float = 1.0,
    ) -> Json:
        return visibility_helpers.append_context_pack_visibility(
            self,
            pack=pack,
            audit_record=audit_record,
            query=query,
            scope=scope,
            audit_mode=audit_mode,
            audit_sample_rate=audit_sample_rate,
        )

    def flush_audits(self) -> None:
        return

    def find_idempotency_record(self, key_hash: int) -> Json | None:
        return local_idempotency_helpers.find_idempotency_record(self, key_hash)

    def append_idempotency_record(self, *, key_hash: int, tool_name: str, raw_key: str, identity: Json, response: Json) -> None:
        local_idempotency_helpers.append_idempotency_record(
            self,
            key_hash=key_hash,
            tool_name=tool_name,
            raw_key=raw_key,
            identity=identity,
            response=response,
        )

    def recent_records(self, limit: int = 128) -> list[Json]:
        return local_read_helpers.recent_records(self, limit, copy_slice=LOCAL_READ_CACHE_COPY)

    def read_all(self) -> list[Json]:
        return local_read_helpers.read_all(self)

    def retrieval_records(
        self,
        *,
        scope: Json,
        record_types: set[str] | None = None,
        secondary_index_groups: list[set[str]] | None = None,
        selected_node_hashes: set[int] | None = None,
        allow_broad_scan_fallback: bool | None = None,
    ) -> Json:
        """Return records eligible for retrieval hot-path scan/filter/pack.

        C++/Rust backends override this seam with native prefix scans and
        secondary-index prefiltering. The local adapter keeps the reference
        behavior by filtering the JSONL record log before Python scoring.
        """

        return local_read_helpers.retrieval_records(
            self,
            scope=scope,
            record_types=record_types,
            secondary_index_groups=secondary_index_groups,
            selected_node_hashes=selected_node_hashes,
            allow_broad_scan_fallback=allow_broad_scan_fallback,
            hot_record_types=RETRIEVAL_HOT_RECORD_TYPES,
        )

    def find_latest_entity(self, *, node_hash: int, entity_type: str, entity_name: str) -> Json | None:
        return local_cache_helpers.find_latest_entity(
            self,
            node_hash=node_hash,
            entity_type=entity_type,
            entity_name=entity_name,
        )

    def pending_session_events(self, scope: Json, *, limit: int | None = None) -> list[Json]:
        return local_cache_helpers.pending_session_events(self, scope, limit=limit)

    def append_session_buffer_event(self, *, envelope: Json, event_id_hash: int, node_hash: int, node_path: list[str], hook: Json | None) -> None:
        session_runtime.append_session_buffer_event(
            self,
            envelope=envelope,
            event_id_hash=event_id_hash,
            node_hash=node_hash,
            node_path=node_path,
            hook=hook,
        )

    def session_buffer_enabled(self, args: Json, *, kind: str = "message") -> bool:
        return session_policy.session_buffer_enabled(args, kind=kind)

    def auto_batch_extract_enabled(self, args: Json, *, kind: str = "message") -> bool:
        return session_policy.auto_batch_extract_enabled(args, kind=kind)

    def session_boundary_commit_requested(self, args: Json, *, hook: Json | None = None) -> bool:
        return session_policy.session_boundary_commit_requested(args, hook=hook)

    def default_session_node_path(self, scope: Json) -> list[str]:
        return session_policy.default_session_node_path(scope)

    def default_shared_context_node_path(self, scope: Json, *, kind: str, sharing_scope: str) -> list[str]:
        return session_policy.default_shared_context_node_path(scope, kind=kind, sharing_scope=sharing_scope)

    def resource_sharing_scope(self, args: Json, envelope: Json, deployment_scope: str) -> str:
        return session_policy.resource_sharing_scope(args, envelope, deployment_scope)

    def default_resource_node_path(self, args: Json, envelope: Json, *, deployment_scope: str, sharing_scope: str) -> list[str]:
        return session_policy.default_resource_node_path(
            args,
            envelope,
            deployment_scope=deployment_scope,
            sharing_scope=sharing_scope,
        )

    def ensure_context_node_path(self, *, node_path: list[str], scope: Json, updated_at_ms: int) -> Json:
        return context_node_helpers.ensure_context_node_path(
            self,
            node_path=node_path,
            scope=scope,
            updated_at_ms=updated_at_ms,
        )

    def session_commit(self, args: Json, *, hook: Json | None = None) -> Json:
        return session_runtime.session_commit(self, args, hook=hook)

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
        return summary_runtime.node_summary_source_records(
            records=records,
            node_path=node_path,
            scope=scope,
            node_hash=node_hash,
            max_events=max_events,
            max_child_summaries=max_child_summaries,
            max_entity_states=max_entity_states,
            max_operator_states=max_operator_states,
        )

    def context_event_ingestion_time_ms(self, record: Json, debug_by_ref: dict[Any, Json] | None = None) -> int:
        return time_compression_runtime.context_event_ingestion_time_ms(record, debug_by_ref)

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
        return time_compression_runtime.write_time_compression_from_events(
            append=self.append,
            append_many=self.append_many,
            scope=scope,
            node_hash=node_hash,
            node_path=node_path,
            selected=selected,
            event_times=event_times,
            compressed_time_ms=compressed_time_ms,
            summary=summary,
            truncated=truncated,
            mode=mode,
            raw_event_ttl_after_compression_ms=raw_event_ttl_after_compression_ms,
            summary_provider_meta=summary_provider_meta,
        )

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
        return time_compression_runtime.auto_time_compress_node_events(
            append=self.append,
            append_many=self.append_many,
            records=records,
            scope=scope,
            node_hash=node_hash,
            node_path=node_path,
            compressed_time_ms=compressed_time_ms,
            max_raw_events_per_node=max_raw_events_per_node,
            max_source_events=max_source_events,
            min_source_events=min_source_events,
            max_windows=max_windows,
            min_event_age_ms=min_event_age_ms,
            raw_event_ttl_after_compression_ms=raw_event_ttl_after_compression_ms,
        )

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
    ) -> tuple[list[int], list[Json]]:
        return summary_runtime.node_summary_dirty_records(
            node_path=node_path,
            scope=scope,
            updated_at_ms=updated_at_ms,
            source_ref_type=source_ref_type,
            source_hash_field=source_hash_field,
            source_hash=source_hash,
            dirty_reason=dirty_reason,
            propagate_depth=propagate_depth,
        )

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
    ) -> list[int]:
        return summary_runtime.mark_node_summary_dirty(
            append_many=self.append_many,
            node_path=node_path,
            scope=scope,
            updated_at_ms=updated_at_ms,
            source_ref_type=source_ref_type,
            source_hash_field=source_hash_field,
            source_hash=source_hash,
            dirty_reason=dirty_reason,
            propagate_depth=propagate_depth,
        )

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
    ) -> Json:
        return summary_runtime.refresh_dirty_node_summaries(
            self,
            scope=scope,
            limit=limit,
            refreshed_at_ms=refreshed_at_ms,
            max_raw_events_per_node=max_raw_events_per_node,
            compression_window_events=compression_window_events,
            min_compression_events=min_compression_events,
            max_compression_windows_per_node=max_compression_windows_per_node,
            min_compression_event_age_ms=min_compression_event_age_ms,
            raw_event_ttl_after_compression_ms=raw_event_ttl_after_compression_ms,
        )

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
        return summary_runtime.append_node_summary_embeddings(
            mark_node_summary_dirty=self.mark_node_summary_dirty,
            node_path=node_path,
            source_text=source_text,
            scope=scope,
            updated_at_ms=updated_at_ms,
            source_hash_field=source_hash_field,
            source_hash=source_hash,
        )

    def refresh_summaries(self, args: Json) -> Json:
        return summary_runtime.refresh_summaries(self, args)

    def ingest(self, args: Json, *, hook: Json | None = None) -> Json:
        ingest_start = ingest_planning_helpers.prepare_ingest_start(
            self,
            args,
            hook=hook,
            default_idle_commit_timeout_ms=DEFAULT_SESSION_IDLE_COMMIT_TIMEOUT_MS,
        )
        return local_ingest_helpers.ingest_after_start(self, args, ingest_start)

    def batch_extract(self, args: Json, *, hook: Json | None = None) -> Json:
        batch_start = batch_extract_planning_helpers.prepare_batch_extract_start(args, hook=hook)
        return local_batch_extract_runtime.batch_extract_after_start(self, args, batch_start)

    def write_time_compression(
        self,
        *,
        scope: Json,
        node_hash: int,
        node_path: list[str],
        source_start_ms: int,
        source_end_ms: int,
        compressed_time_ms: int,
        max_source_events: int = 32,
        min_confidence: float = 0.0,
        min_importance: float = 0.0,
        summary: str = "",
    ) -> Json:
        return time_compression_runtime.write_time_compression(
            append=self.append,
            records=self.read_all(),
            scope=scope,
            node_hash=node_hash,
            node_path=node_path,
            source_start_ms=source_start_ms,
            source_end_ms=source_end_ms,
            compressed_time_ms=compressed_time_ms,
            max_source_events=max_source_events,
            min_confidence=min_confidence,
            min_importance=min_importance,
            summary=summary,
        )

    def query_time_compressions(
        self, *, scope: Json, node_hashes: set[int], start_time_ms: int, end_time_ms: int, limit: int = 16
    ) -> list[Json]:
        return time_compression_runtime.query_time_compressions(
            records=self.read_all(),
            scope=scope,
            node_hashes=node_hashes,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            limit=limit,
        )

    def append_recall_reinforcement_markers(
        self,
        *,
        context_pack_id: str,
        selected_refs: list[Json],
        reinforced_at_ms: int,
        protect_ms: int = TIME_COMPRESSION_REINFORCEMENT_PROTECT_MS,
    ) -> Json:
        return time_compression_runtime.append_recall_reinforcement_markers(
            append_many=self.append_many,
            context_pack_id=context_pack_id,
            selected_refs=selected_refs,
            reinforced_at_ms=reinforced_at_ms,
            protect_ms=protect_ms,
        )

    def deadline_fallback_pack(
        self,
        *,
        query: str,
        scope: Json,
        question_type: str,
        max_context_tokens: int,
        local_budget: Json,
        deadline_ms: int,
        elapsed_ms: float,
        records: list[Json],
        reason: str,
        budget_source: str = "matrixark_default_max_context_tokens",
    ) -> Json:
        return deadline_pack_helpers.deadline_fallback_pack(
            self,
            query=query,
            scope=scope,
            question_type=question_type,
            max_context_tokens=max_context_tokens,
            local_budget=local_budget,
            deadline_ms=deadline_ms,
            elapsed_ms=elapsed_ms,
            records=records,
            reason=reason,
            budget_source=budget_source,
        )

    def supports_native_candidate_prefilter(self) -> bool:
        return False

    def supports_native_context_pack(self) -> bool:
        return False

    def native_context_pack_required(self) -> bool:
        backend_label = str(getattr(self, "_backend_label", lambda: "local")())
        return native_context_pack_required_for_backend(
            backend_label,
            require_flag=MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK,
        )

    def native_context_pack(self, request: Json) -> Json | None:
        """Return a backend-assembled ContextPack when the native backend supports it.

        Python remains responsible for MCP/auth/model glue and request shaping.
        C++/Rust backends should own scan, secondary-index filtering, scoring, and
        budget-aware pack assembly through this boundary when available.
        """
        return None

    def retrieve(self, args: Json) -> Json:
        return local_retrieve_runtime.retrieve(self, args)

    def feedback(self, args: Json, *, hook: Json | None = None) -> Json:
        args = {**args, "kind": "feedback"}
        return self.ingest(args, hook=hook)

    def replay(self, args: Json) -> Json:
        return local_replay_helpers.replay(self, args)
