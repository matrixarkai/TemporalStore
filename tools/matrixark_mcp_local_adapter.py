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
    from tools.matrixark_mcp_metrics import MatrixArkServiceMetrics
except ModuleNotFoundError:  # Direct script execution from tools/.
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
    from tools import matrixark_mcp_deadline_pack as deadline_pack_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_deadline_pack as deadline_pack_helpers

try:
    from tools import matrixark_mcp_local_retrieve_runtime as local_retrieve_runtime
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_local_retrieve_runtime as local_retrieve_runtime

try:
    from tools import matrixark_mcp_local_replay as local_replay_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_local_replay as local_replay_helpers

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
    from tools.matrixark_mcp_local_core_mixin import (
        MatrixArkLocalCoreMixin,
        RETRIEVAL_HOT_RECORD_TYPES,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_local_portal_imports import MatrixArkLocalPortalImportMixin
    from matrixark_mcp_local_core_mixin import (
        MatrixArkLocalCoreMixin,
        RETRIEVAL_HOT_RECORD_TYPES,
    )

try:
    from tools import matrixark_mcp_summary_runtime as summary_runtime
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_summary_runtime as summary_runtime

try:
    from tools import matrixark_mcp_time_compression_runtime as time_compression_runtime
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_time_compression_runtime as time_compression_runtime

DEFAULT_SESSION_IDLE_COMMIT_TIMEOUT_MS = 5 * 60 * 1000

@dataclass
class MatrixArkLocalAdapter(MatrixArkLocalCoreMixin, MatrixArkLocalPortalImportMixin):
    event_log: Path

    def __post_init__(self) -> None:
        self._init_local_runtime_state()

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
