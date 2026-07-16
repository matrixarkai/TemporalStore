#!/usr/bin/env python3
"""Local MatrixArk adapter and in-memory serving backend."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import *
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import *

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
    from tools import matrixark_mcp_dashboard as dashboard_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_dashboard as dashboard_helpers

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
    from tools import matrixark_mcp_resource_import_runtime as resource_import_runtime
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_resource_import_runtime as resource_import_runtime

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
    from tools import matrixark_mcp_session_runtime as session_runtime
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_session_runtime as session_runtime

try:
    from tools import matrixark_mcp_context_nodes as context_node_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_context_nodes as context_node_helpers

try:
    from tools import matrixark_mcp_registry as registry_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_registry as registry_helpers

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
class MatrixArkLocalAdapter:
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

    def latest_skill_controls(self, records: list[Json] | None = None) -> dict[int, Json]:
        return registry_helpers.latest_skill_controls(self, records)

    def _dashboard_record_scope(self, record: Json) -> Json:
        return dashboard_helpers.dashboard_record_scope(record)

    def _dashboard_message_rows(self, records: list[Json], scope: Json) -> list[Json]:
        return dashboard_helpers.dashboard_message_rows(records, scope)

    def _dashboard_rows_for_table(self, records: list[Json], table: str, scope: Json) -> list[Json]:
        return dashboard_helpers.dashboard_rows_for_table(records, table, scope)

    def ingestion_dashboard(self, args: Json) -> Json:
        return dashboard_helpers.ingestion_dashboard(self, args)

    def list_resources(self, args: Json) -> Json:
        return registry_helpers.list_resources(self, args)

    def list_skills(self, args: Json) -> Json:
        return registry_helpers.list_skills(self, args)

    def update_skill(self, args: Json) -> Json:
        return registry_helpers.update_skill(self, args)

    def _resource_import_pool_status(self) -> Json:
        return resource_import_runtime.resource_import_pool_status(self)

    def _ensure_resource_import_workers(self) -> None:
        resource_import_runtime.ensure_resource_import_workers(self)

    def _resource_import_worker_loop(self) -> None:
        resource_import_runtime.resource_import_worker_loop(self)

    def close(self, *, timeout_s: float = 5.0) -> None:
        """Drain async import work and stop background workers."""
        resource_import_runtime.close_resource_import_runtime(self, timeout_s=timeout_s)

    def _enqueue_resource_import(self, *, args: Json, hook: Json | None, task_hash: int) -> Json:
        return resource_import_runtime.enqueue_resource_import(self, args=args, hook=hook, task_hash=task_hash)

    def _run_background_resource_import(self, args: Json, hook: Json | None) -> None:
        resource_import_runtime.run_background_resource_import(self, args, hook)

    def _resource_import_async_default_reason(self, args: Json, envelope: Json, raw_uri: str) -> str:
        return resource_import_runtime.resource_import_async_default_reason(args, envelope, raw_uri)

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
        envelope = batch_start["envelope"]
        hook = batch_start["hook"]
        threshold = batch_start["threshold"]
        derive_from_existing_events = bool(batch_start["derive_from_existing_events"])
        source_event_ids = list(batch_start["source_event_ids"])
        if batch_start.get("deferred_result") is not None:
            return batch_start["deferred_result"]

        prior_records = [] if args.get("skip_prior_context") else self.read_all()
        prior_context = (
            {"level": "", "refs": [], "messages": [], "summaries": [], "char_count": 0, "limit": MAX_PRIOR_MESSAGES}
            if args.get("skip_prior_context")
            else collect_prior_context(envelope, prior_records)
        )
        extraction_started_perf = time.perf_counter()
        extraction = one_pass_memory_extraction(envelope, prior_context=prior_context)
        self._observe_model_latency("batch_extraction", (time.perf_counter() - extraction_started_perf) * 1000.0)
        batch_text = text_from_messages(envelope["messages"])
        batch_id_hash = stable_hash(
            f"batch:{batch_text}:{envelope['scope']}:{envelope['ingestion_time_ms']}"
        )
        node_hint = envelope["metadata"].get("node_path") or self.default_session_node_path(envelope["scope"])
        node_path = normalized_node_path(envelope, node_hint)
        node_hash = stable_hash("/".join(node_path))
        node_materialization = self.ensure_context_node_path(
            node_path=node_path,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
        )
        batch_summary = extraction["batch_summary"]

        event_hashes: list[int] = list(source_event_ids) if derive_from_existing_events else []
        records_to_append: list[Json] = []
        event_rows: list[tuple[int, Json, str, int]] = []
        segment_hash_by_position: dict[int, int] = {}
        segment_hashes_by_position: dict[int, list[int]] = {}
        for segment in extraction["segments"]:
            segment_hash = stable_hash(f"{batch_id_hash}:segment:{segment['topic']}:{segment['coordinate_tuples']}")
            for message_index in segment.get("message_indexes", []):
                if not isinstance(message_index, int):
                    continue
                segment_hashes_by_position.setdefault(message_index, []).append(segment_hash)
                segment_hash_by_position.setdefault(message_index, segment_hash)
        if not derive_from_existing_events:
            for index, message in enumerate(envelope["messages"]):
                event_text = f"{message['role']}: {message['content']}"
                event_id_hash = stable_hash(f"{batch_id_hash}:event:{index}:{event_text}")
                event_hashes.append(event_id_hash)
                event_rows.append((index, message, event_text, event_id_hash))
            event_vectors = embeddings_for_texts([event_text for _index, _message, event_text, _event_id_hash in event_rows])
            for (_index, message, event_text, event_id_hash), event_vector in zip(event_rows, event_vectors):
                records_to_append.append(
                    {
                        "record_type": "context_event",
                        "event_id_hash": event_id_hash,
                        "batch_id_hash": batch_id_hash,
                        "parent_segment_hash": segment_hash_by_position.get(_index),
                        "parent_segment_hashes": segment_hashes_by_position.get(_index, []),
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "text": event_text,
                        "summary_text": summarize_text(event_text),
                        "classification": extraction["classification"],
                        "event_type": extraction["event_type"],
                        "status": "observed",
                        "source_kind": envelope.get("kind", "message"),
                        "envelope": {
                            **envelope,
                            "messages": [message],
                        },
                        "internal_extraction": {
                            "mode": extraction["mode"],
                            "classification": extraction["classification"],
                            "event_type": extraction["event_type"],
                            "batch_id_hash": batch_id_hash,
                        },
                        "prior_context": prior_context,
                        "agent_hook": hook,
                        "storage_options": envelope.get("storage_options", {}),
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                records_to_append.append(
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "event_text",
                        "ref_type": "event",
                        "ref_hash": event_id_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dim": len(event_vector),
                        "model": embedding_model_name(),
                        "vector": event_vector,
                        "scope": envelope["scope"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )

        entity_hashes = []
        for entity in extraction["entities"]:
            entity_hash = stable_hash(
                f"{node_hash}:{entity['entity_type']}:{entity['entity_name']}"
            )
            previous_entity = self.find_latest_entity(
                node_hash=node_hash,
                entity_type=entity["entity_type"],
                entity_name=entity["entity_name"],
            )
            updated_entity = apply_entity_patches(previous_entity, entity)
            entity_hashes.append(entity_hash)
            records_to_append.append(
                {
                    "record_type": "context_entity",
                    "entity_hash": entity_hash,
                    "batch_id_hash": batch_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": envelope["scope"],
                    "entity_type": updated_entity["entity_type"],
                    "entity_name": updated_entity["entity_name"],
                    "state": updated_entity["state"],
                    "previous_state": updated_entity.get("previous_state", ""),
                    "confidence": updated_entity["confidence"],
                    "operator": updated_entity["operator"],
                    "source_refs": updated_entity["source_refs"],
                    "source_event_ids": source_event_ids,
                    "field_patches": updated_entity.get("field_patches", []),
                    "patch_results": updated_entity.get("patch_results", []),
                    "update_mode": updated_entity.get("update_mode", ""),
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            if updated_entity.get("patch_results"):
                records_to_append.append(
                    {
                        "record_type": "context_entity_update_audit",
                        "entity_hash": entity_hash,
                        "batch_id_hash": batch_id_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "entity_type": updated_entity["entity_type"],
                        "entity_name": updated_entity["entity_name"],
                        "previous_state": updated_entity.get("previous_state", ""),
                        "new_state": updated_entity["state"],
                        "patch_results": updated_entity.get("patch_results", []),
                        "llm_calls": 0,
                        "update_mode": "deterministic_eua",
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            entity_embedding_text = updated_entity["entity_type"] + " " + updated_entity["state"]
            entity_vector = embedding_for_text(entity_embedding_text)
            records_to_append.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": "entity_state",
                    "ref_type": "entity",
                    "ref_hash": entity_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(entity_vector),
                    "model": embedding_model_name(),
                    "vector": entity_vector,
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )

        segment_hashes = []
        for segment in extraction["segments"]:
            segment_hash = stable_hash(f"{batch_id_hash}:segment:{segment['topic']}:{segment['coordinate_tuples']}")
            segment_hashes.append(segment_hash)
            records_to_append.append(
                {
                    "record_type": "context_segment",
                    "segment_hash": segment_hash,
                    "batch_id_hash": batch_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": envelope["scope"],
                    "topic": segment["topic"],
                    "coordinate_tuples": segment["coordinate_tuples"],
                    "message_indexes": segment["message_indexes"],
                    "source_event_ids": [event_hashes[index] for index in segment["message_indexes"] if index < len(event_hashes)],
                    "saliency_score": segment["saliency_score"],
                    "summary_text": segment["summary_text"],
                    "text": segment["text"],
                    "non_contiguous": segment["non_contiguous"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            segment_embedding_text = segment["topic"] + " " + segment["summary_text"]
            segment_vector = embedding_for_text(segment_embedding_text)
            records_to_append.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": "segment_text",
                    "ref_type": "segment",
                    "ref_hash": segment_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(segment_vector),
                    "model": embedding_model_name(),
                    "vector": segment_vector,
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )

        summary_hash = stable_hash(f"batch_summary:{batch_id_hash}")
        records_to_append.append(
            {
                "record_type": "context_summary",
                "summary_type": "batch_l0",
                "summary_hash": summary_hash,
                "batch_id_hash": batch_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "summary_text": batch_summary,
                "source_entity_hashes": entity_hashes,
                "source_segment_hashes": segment_hashes,
                "source_event_ids": event_hashes,
                "scope": envelope["scope"],
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )
        summary_embedding_text = " ".join(node_path + [batch_summary])
        summary_vector = embedding_for_text(summary_embedding_text)
        records_to_append.append(
            {
                "record_type": "context_embedding",
                "embedding_type": "batch_l0",
                "ref_type": "summary",
                "ref_hash": summary_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": len(summary_vector),
                "model": embedding_model_name(),
                "vector": summary_vector,
                "scope": envelope["scope"],
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )
        secondary_index_budget = new_secondary_index_budget()
        batch_index_terms = take_secondary_index_terms(list(extraction["indexes"]), secondary_index_budget)
        for index_name in batch_index_terms:
            records_to_append.append(
                context_index_posting_record(
                    index_name=index_name,
                    capability="context_batch_commit",
                    batch_id_hash=batch_id_hash,
                    node_hash=node_hash,
                    scope=envelope["scope"],
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
            )
        records_to_append.append(
            {
                "record_type": "context_extraction_audit",
                "batch_id_hash": batch_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "schema": extraction["schema"],
                "message_count": extraction["message_count"],
                "token_count_estimate": extraction["token_count_estimate"],
                "outputs": {
                    "events": 0 if derive_from_existing_events else len(envelope["messages"]),
                    "source_events": len(event_hashes),
                    "entities": len(entity_hashes),
                    "segments": len(segment_hashes),
                    "summaries": 1,
                    "indexes": len(batch_index_terms),
                    **secondary_index_budget_summary(secondary_index_budget),
                },
                "mode": extraction["mode"],
                "derive_from_existing_events": derive_from_existing_events,
                "source_event_ids": event_hashes,
                "agent_hook": hook,
                "created_at_ms": now_ms(),
            }
        )
        dirty_hashes, dirty_records = self.node_summary_dirty_records(
            node_path=node_path,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
            source_ref_type="batch",
            source_hash_field="source_batch_hash",
            source_hash=batch_id_hash,
            dirty_reason="new_event",
        )
        records_to_append.extend(dirty_records)
        self.append_many(records_to_append)
        summary_refresh = {
            "status": "dirty_marked",
            "dirty_hashes": dirty_hashes,
            "refresh_result": None,
            "async_required": True,
            "write_path": "coalesced_with_batch_extract",
        }
        return {
            "status": "accepted",
            "mode": extraction["mode"],
            "segment_provider": extraction.get("segment_provider", {}),
            "classification": extraction["classification"],
            "batch_id_hash": batch_id_hash,
            "node_hash": node_hash,
            "storage_options": envelope.get("storage_options", {}),
            "storage_route": envelope.get("storage_route", {}),
            "embedding_model": embedding_model_name(),
            "embedding_execution_mode": embedding_execution_mode_name(),
            "embedding_fallback_used": embedding_fallback_used(),
            "message_count": extraction["message_count"],
            "token_count_estimate": extraction["token_count_estimate"],
            "events_written": 0 if derive_from_existing_events else len(envelope["messages"]),
            "source_event_count": len(event_hashes),
            "raw_events_duplicated": not derive_from_existing_events,
            "entities_written": len(entity_hashes),
            "segments_written": len(segment_hashes),
            "summary_hash": summary_hash,
            "summary_refresh": summary_refresh,
            "node_materialization": node_materialization,
            "indexes_written": len(batch_index_terms),
            **secondary_index_budget_summary(secondary_index_budget),
            "one_pass": True,
            "threshold_messages": threshold,
        }

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
        if MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK:
            return MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK in {"1", "true", "yes"}
        backend_label = str(getattr(self, "_backend_label", lambda: "local")())
        return backend_label != "local"

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
