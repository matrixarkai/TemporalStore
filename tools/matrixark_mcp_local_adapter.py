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
    from tools import matrixark_mcp_local_ingest as local_ingest_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_local_ingest as local_ingest_helpers

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
        entity_hash = stable_hash(f"{node_hash}:{entity_type}:{entity_name}")
        if entity_hash in self._latest_entity_by_hash:
            return self._latest_entity_by_hash[entity_hash]
        self._ensure_latest_entity_cache_loaded()
        return self._latest_entity_by_hash.get(entity_hash)

    def pending_session_events(self, scope: Json, *, limit: int | None = None) -> list[Json]:
        return local_cache_helpers.pending_session_events(self, scope, limit=limit)

    def append_session_buffer_event(self, *, envelope: Json, event_id_hash: int, node_hash: int, node_path: list[str], hook: Json | None) -> None:
        key = session_buffer_key(envelope)
        self.append(
            {
                "record_type": "session_buffer_event",
                "buffer_key_hash": stable_hash(":".join(key)),
                "buffer_key": list(key),
                "event_id_hash": event_id_hash,
                "node_hash": node_hash,
                "storage_options": envelope.get("storage_options", {}),
                "storage_route": envelope.get("storage_route", {}),
                "node_path": node_path,
                "scope": envelope["scope"],
                "status": "pending",
                "agent_hook": hook,
                "created_at_ms": envelope["ingestion_time_ms"],
            }
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
        dirty_hashes, records = self.node_summary_dirty_records(
            node_path=node_path,
            scope=scope,
            updated_at_ms=updated_at_ms,
            source_ref_type=source_ref_type,
            source_hash_field=source_hash_field,
            source_hash=source_hash,
            dirty_reason=dirty_reason,
            propagate_depth=propagate_depth,
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
    ) -> Json:
        refreshed_at_ms = refreshed_at_ms or now_ms()
        records = self.read_all()
        pending_by_node = summary_runtime.pending_dirty_node_records(
            records=records,
            scope=scope,
            limit=limit,
            refreshed_at_ms=refreshed_at_ms,
            max_raw_events_per_node=max_raw_events_per_node,
            min_compression_event_age_ms=min_compression_event_age_ms,
            context_event_ingestion_time_ms=self.context_event_ingestion_time_ms,
        )
        refreshed = []
        for dirty in sorted(pending_by_node.values(), key=lambda item: int(item.get("updated_at_ms") or 0))[:limit]:
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
            summary_refresh_records = summary_runtime.build_node_summary_refresh_records(
                node_path=node_path,
                node_hash=node_hash,
                scope=dirty.get("scope", scope),
                events=events,
                child_summaries=child_summaries,
                entity_states=entity_states,
                operator_states=operator_states,
                summary_source_policy=summary_source_policy,
                dirty_hash=dirty.get("dirty_hash"),
                refreshed_at_ms=refreshed_at_ms,
            )
            self.append_many(summary_refresh_records["records"])
            source_event_ids = summary_refresh_records["source_event_ids"]
            source_summary_hashes = summary_refresh_records["source_summary_hashes"]
            source_entity_hashes = summary_refresh_records["source_entity_hashes"]
            source_operator_hashes = summary_refresh_records["source_operator_hashes"]
            generated_summary_types = summary_refresh_records["generated_summary_types"]
            l1_policy = summary_refresh_records["summary_generation_policy"]
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
                "status": "completed",
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
                        "generated_summary_types": generated_summary_types,
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
            refreshed.append(
                {
                    "dirty_hash": dirty.get("dirty_hash"),
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "source_event_count": len(source_event_ids),
                    "source_summary_count": len(source_summary_hashes),
                    "source_entity_count": len(source_entity_hashes),
                    "source_operator_count": len(source_operator_hashes),
                    "generated_summary_types": generated_summary_types,
                    "summary_generation_policy": l1_policy,
                    "time_compression": compression_refresh,
                }
            )
        return {
            "status": "ok",
            "refreshed_count": len(refreshed),
            "compression_created_count": sum(int(item.get("time_compression", {}).get("created_count", 0)) for item in refreshed),
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
        envelope = normalize_envelope(args, default_kind="message")
        hook = validate_hook(hook)
        backend_readiness: Json | None = None
        if envelope["kind"] in {"resource", "skill"}:
            backend_readiness = self.ensure_backend_ready(reason=f"{envelope['kind']}_ingest")
        idle_commit_result: Json | None = None
        idle_commit_timeout_ms = args.get("idle_commit_timeout_ms", DEFAULT_SESSION_IDLE_COMMIT_TIMEOUT_MS)
        if idle_commit_timeout_ms is not None:
            if not isinstance(idle_commit_timeout_ms, int) or idle_commit_timeout_ms < 0:
                raise MatrixArkError("idle_commit_timeout_ms must be a non-negative integer")
        if (
            isinstance(idle_commit_timeout_ms, int)
            and idle_commit_timeout_ms > 0
            and self.auto_batch_extract_enabled(args, kind=envelope["kind"])
        ):
            idle_commit_result = self.session_commit(
                {
                    "scope": envelope["scope"],
                    "metadata": envelope["metadata"],
                    "threshold_messages": args.get("session_buffer_threshold", 20),
                    "force": False,
                    "idle_timeout_ms": idle_commit_timeout_ms,
                    "commit_reason": "idle_timeout",
                    "skip_prior_context": bool(args.get("skip_prior_context", False)),
                    "storage_options": envelope.get("storage_options", {}),
                },
                hook=hook,
            )
        lightweight_result = local_ingest_helpers.lightweight_async_accept(
            self,
            args,
            envelope=envelope,
            hook=hook,
            idle_commit_result=idle_commit_result,
        )
        if lightweight_result is not None:
            return lightweight_result
        prior_records = [] if args.get("skip_prior_context") else self.read_all()
        prior_context = (
            {"level": "", "refs": [], "messages": [], "summaries": [], "char_count": 0, "limit": MAX_PRIOR_MESSAGES}
            if args.get("skip_prior_context")
            else collect_prior_context(envelope, prior_records)
        )
        extraction_started_perf = time.perf_counter()
        extraction = compact_internal_extraction(
            envelope,
            prior_context=prior_context,
        )
        self._observe_model_latency("extraction", (time.perf_counter() - extraction_started_perf) * 1000.0)
        text = text_from_messages(envelope["messages"])
        event_id_hash = stable_hash(
            f"{envelope['kind']}:{text}:{envelope['scope']}:{envelope['ingestion_time_ms']}"
        )
        if envelope["kind"] in {"resource", "skill"}:
            early_deployment_scope = deployment_scope_from_args(args, envelope)
            early_sharing_scope = self.resource_sharing_scope(args, envelope, early_deployment_scope)
            node_hint = self.default_resource_node_path(args, envelope, deployment_scope=early_deployment_scope, sharing_scope=early_sharing_scope)
        else:
            early_deployment_scope = "local"
            early_sharing_scope = "private_user"
            node_hint = envelope["metadata"].get("node_path") or self.default_session_node_path(envelope["scope"])
        node_path = normalized_node_path(envelope, node_hint)
        node_hash = stable_hash("/".join(node_path))
        node_materialization = self.ensure_context_node_path(
            node_path=node_path,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
        )
        resource_chunk_hashes: list[int] = []
        resource_dirty_hashes: list[int] = []
        resource_parse_error = ""
        resource_import_task_hash = 0
        resource_import_task_status = "not_applicable"
        resource_import_wait = True
        resource_import_metrics: Json = {}
        resource_fact_event_hashes: list[int] = []
        resource_fact_entity_hashes: list[int] = []
        skill_hash = None
        if envelope["kind"] in {"resource", "skill"}:
            requested_raw_uri = str(envelope.get("raw_uri") or envelope["metadata"].get("raw_uri") or "inline-resource")
            resource_type = str(envelope.get("resource_type") or envelope["metadata"].get("resource_type") or "")
            async_default_reason = self._resource_import_async_default_reason(args, envelope, requested_raw_uri)
            resource_import_wait = bool(args.get("wait", not bool(async_default_reason)))
            resource_import_background = bool(args.get("_background_resource_import", False))
            deployment_scope = early_deployment_scope
            sharing_scope = early_sharing_scope
            access_scope = registry_access_scope(envelope["scope"], sharing_scope=sharing_scope)
            resource_record_scope = access_scope if sharing_scope in {"tenant_shared", "global_shared"} else envelope["scope"]
            provided_task_hash = args.get("_resource_import_task_hash")
            resource_import_task_hash = (
                int(provided_task_hash)
                if isinstance(provided_task_hash, int) and provided_task_hash > 0
                else stable_hash(f"resource_import_task:{envelope['kind']}:{requested_raw_uri}:{node_hash}:{envelope['ingestion_time_ms']}")
            )
            import_started_perf = time.perf_counter()
            raw_uri = requested_raw_uri
            raw_storage_policy = "raw_uri_only"
            storage_resolution: Json = {
                "storage_mode": resource_storage_mode_from_args(args, envelope, deployment_scope),
                "original_raw_uri": requested_raw_uri,
                "stored_raw_uri": requested_raw_uri,
                "parse_uri": requested_raw_uri,
                "parse_text": None,
                "raw_storage_policy": raw_storage_policy,
                "raw_bytes_stored": False,
                "upload_status": "not_started",
                "temp_paths": [],
            }
            if not resource_import_background:
                self.append(
                    {
                        "record_type": "resource_import_task",
                        "task_hash": resource_import_task_hash,
                        "status": "queued",
                        "kind": envelope["kind"],
                        "raw_uri": requested_raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "resource_type": resource_type,
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": raw_storage_policy,
                        "raw_bytes_stored": False,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "storage_options": envelope.get("storage_options", {}),
                        "wait": resource_import_wait,
                        "async_default_reason": async_default_reason,
                        "progress": {"stage": "queued", "percent": 0},
                        "created_at_ms": envelope["ingestion_time_ms"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            if not resource_import_wait:
                background_args = {
                    **args,
                    "wait": True,
                    "_background_resource_import": True,
                    "_resource_import_task_hash": resource_import_task_hash,
                }
                try:
                    queue_status = self._enqueue_resource_import(
                        args=background_args,
                        hook=hook,
                        task_hash=resource_import_task_hash,
                    )
                except MatrixArkError as exc:
                    self.append(
                        {
                            "record_type": "resource_import_task",
                            "task_hash": resource_import_task_hash,
                            "status": "failed",
                            "kind": envelope["kind"],
                            "raw_uri": requested_raw_uri,
                            "requested_raw_uri": requested_raw_uri,
                            "resource_type": resource_type,
                            "raw_storage_mode": storage_resolution["storage_mode"],
                            "raw_storage_policy": raw_storage_policy,
                            "raw_bytes_stored": False,
                        "error": str(exc),
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "storage_options": envelope.get("storage_options", {}),
                        "progress": {"stage": "failed", "percent": 100},
                        "updated_at_ms": now_ms(),
                    }
                )
                    raise
                return {
                    "status": "queued",
                    "event_id_hash": event_id_hash,
                    "node_hash": node_hash,
                    "resource_import_task": {
                        "task_hash": resource_import_task_hash,
                        "status": "queued",
                        "wait": False,
                        "background_started": True,
                        "raw_uri": requested_raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "resource_type": resource_type,
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": raw_storage_policy,
                        "raw_bytes_stored": False,
                        "worker_pool": queue_status,
                        "progress": {"stage": "queued", "percent": 0},
                        "async_default_reason": async_default_reason,
                    },
                    "node_materialization": node_materialization,
                }
            resource_import_task_status = "running"
            resource_text = "\n\n".join(str(message["content"]) for message in envelope["messages"])
            try:
                storage_resolution = resolve_raw_resource_for_ingest(
                    args,
                    envelope,
                    requested_raw_uri,
                    resource_type,
                    deployment_scope,
                    resource_text,
                )
            except MatrixArkError as exc:
                self.append(
                    {
                        "record_type": "resource_import_task",
                        "task_hash": resource_import_task_hash,
                        "status": "failed",
                        "kind": envelope["kind"],
                        "raw_uri": requested_raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "resource_type": resource_type,
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": storage_resolution["raw_storage_policy"],
                        "raw_bytes_stored": False,
                        "error": str(exc),
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "progress": {"stage": "failed", "percent": 100},
                        "updated_at_ms": now_ms(),
                    }
                )
                raise
            raw_uri = str(storage_resolution["stored_raw_uri"])
            parse_uri = str(storage_resolution.get("parse_uri") or raw_uri)
            parse_text = storage_resolution.get("parse_text")
            raw_storage_policy = str(storage_resolution.get("raw_storage_policy") or "raw_uri_only")
            self.append(
                {
                    "record_type": "resource_import_task",
                    "task_hash": resource_import_task_hash,
                    "status": "running",
                    "kind": envelope["kind"],
                    "raw_uri": raw_uri,
                    "requested_raw_uri": requested_raw_uri,
                    "resource_type": resource_type,
                    "raw_storage_mode": storage_resolution["storage_mode"],
                    "raw_storage_policy": raw_storage_policy,
                    "raw_bytes_stored": False,
                    "upload_status": storage_resolution.get("upload_status", "not_required"),
                    "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                    "cloud_key": storage_resolution.get("cloud_key", ""),
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": resource_record_scope,
                    "storage_options": envelope.get("storage_options", {}),
                    "progress": {"stage": "running", "percent": 10},
                    "updated_at_ms": now_ms(),
                }
            )
            try:
                if envelope["kind"] == "skill" or (resource_type or "").lower() == "skill":
                    parsed_skill = parse_skill(
                        parse_uri,
                        text=parse_text,
                        chunk_hash_base=args.get("chunk_hash_base") if isinstance(args.get("chunk_hash_base"), int) else None,
                    )
                    parsed_skill_chunks = rewrite_chunk_uris(parsed_skill.chunks, parse_uri=parse_uri, stored_raw_uri=raw_uri)
                    skill_hash = stable_hash(f"skill:{raw_uri}:{parsed_skill.name}:{parsed_skill.metadata.get('version', '1')}")
                    skill_serving_metadata = serving_resource_metadata(parsed_skill.metadata)
                    self.append(
                        {
                            "record_type": "skill_manifest",
                            "skill_hash": skill_hash,
                            "import_task_hash": resource_import_task_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "raw_uri": raw_uri,
                            "requested_raw_uri": requested_raw_uri,
                            "raw_storage_mode": storage_resolution["storage_mode"],
                            "raw_storage_policy": raw_storage_policy,
                            "upload_status": storage_resolution.get("upload_status", "not_required"),
                            "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                            "cloud_key": storage_resolution.get("cloud_key", ""),
                            "name": parsed_skill.name,
                            "description": parsed_skill.description,
                            "owner_scope": parsed_skill.metadata.get("owner_scope", "user"),
                            "version": parsed_skill.metadata.get("version", "1"),
                            "status": parsed_skill.metadata.get("status", "active"),
                            "precedence": parsed_skill.metadata.get("precedence", "normal"),
                            "triggers": parsed_skill.metadata.get("triggers", []),
                            "allowed_tools": parsed_skill.metadata.get("allowed_tools", []),
                            "examples": parsed_skill.metadata.get("examples", []),
                            "permissions": parsed_skill.metadata.get("permissions", []),
                            "inputs": parsed_skill.metadata.get("inputs", []),
                            "outputs": parsed_skill.metadata.get("outputs", []),
                            "access_scope": access_scope,
                            "deployment_scope": deployment_scope,
                            "text_preview": clip_context_text(parsed_skill.text),
                            "token_estimate": parsed_skill.token_estimate,
                            "metadata": skill_serving_metadata,
                            "scope": resource_record_scope,
                            "storage_options": envelope.get("storage_options", {}),
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    skill_debug_metadata = debug_resource_metadata(parsed_skill.metadata)
                    if skill_debug_metadata or parsed_skill.text:
                        self.append(
                            {
                                "record_type": "context_debug_record",
                                "debug_type": "skill_parse_detail",
                                "ref_type": "skill",
                                "ref_hash": skill_hash,
                                "skill_hash": skill_hash,
                                "import_task_hash": resource_import_task_hash,
                                "node_hash": node_hash,
                                "node_path": node_path,
                                "raw_uri": raw_uri,
                                "metadata_debug": skill_debug_metadata,
                                "text_preview": clip_context_text(parsed_skill.text),
                                "scope": resource_record_scope,
                                "updated_at_ms": envelope["ingestion_time_ms"],
                            }
                        )
                    self.append(
                        {
                            "record_type": "skill_registry",
                            "registry_hash": stable_hash(f"skill_registry:{skill_hash}:{deployment_scope}"),
                            "skill_hash": skill_hash,
                            "import_task_hash": resource_import_task_hash,
                            "raw_uri": raw_uri,
                            "requested_raw_uri": requested_raw_uri,
                            "raw_storage_mode": storage_resolution["storage_mode"],
                            "raw_storage_policy": raw_storage_policy,
                            "upload_status": storage_resolution.get("upload_status", "not_required"),
                            "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                            "cloud_key": storage_resolution.get("cloud_key", ""),
                            "name": parsed_skill.name,
                            "description": parsed_skill.description,
                            "owner_scope": parsed_skill.metadata.get("owner_scope", "user"),
                            "version": parsed_skill.metadata.get("version", "1"),
                            "status": parsed_skill.metadata.get("status", "active"),
                            "precedence": parsed_skill.metadata.get("precedence", "normal"),
                            "triggers": parsed_skill.metadata.get("triggers", []),
                            "allowed_tools": parsed_skill.metadata.get("allowed_tools", []),
                            "examples": parsed_skill.metadata.get("examples", []),
                            "permissions": parsed_skill.metadata.get("permissions", []),
                            "inputs": parsed_skill.metadata.get("inputs", []),
                            "outputs": parsed_skill.metadata.get("outputs", []),
                            "access_scope": access_scope,
                            "deployment_scope": deployment_scope,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    skill_vector = embedding_for_text(str(parsed_skill.metadata.get("embedding_text") or (parsed_skill.name + " " + parsed_skill.description)))
                    self.append(
                        {
                            "record_type": "context_embedding",
                            "embedding_type": "skill_summary",
                            "ref_type": "skill",
                            "ref_hash": skill_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "dim": len(skill_vector),
                            "model": embedding_model_name(),
                            "vector": skill_vector,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    parsed_chunks = parsed_skill_chunks
                else:
                    parsed_chunks = parse_resource(
                        parse_uri,
                        resource_type=resource_type or None,
                        text=parse_text,
                        chunk_hash_base=args.get("chunk_hash_base") if isinstance(args.get("chunk_hash_base"), int) else None,
                        resource_version=args.get("resource_version") if isinstance(args.get("resource_version"), str) else None,
                        supersedes_chunk_hashes=args.get("supersedes_chunk_hashes") if isinstance(args.get("supersedes_chunk_hashes"), dict) else None,
                    )
                    parsed_chunks = rewrite_chunk_uris(parsed_chunks, parse_uri=parse_uri, stored_raw_uri=raw_uri)
            except ResourceParserError as exc:
                resource_parse_error = str(exc)
                parsed_chunks = []
            finally:
                cleanup_temp_paths([str(path) for path in storage_resolution.get("temp_paths", []) if isinstance(path, str)])
            if not parsed_chunks:
                resource_import_task_status = "failed"
                self.append(
                    {
                        "record_type": "resource_import_task",
                        "task_hash": resource_import_task_hash,
                        "status": "failed",
                        "kind": envelope["kind"],
                        "raw_uri": raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "resource_type": resource_type,
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": raw_storage_policy,
                        "raw_bytes_stored": False,
                        "upload_status": storage_resolution.get("upload_status", "not_required"),
                        "error": resource_parse_error or "resource ingestion produced no chunks",
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "progress": {"stage": "failed", "percent": 100},
                        "updated_at_ms": now_ms(),
                    }
                )
                raise MatrixArkError(resource_parse_error or "resource ingestion produced no chunks")
            original_chunk_count = len(parsed_chunks)
            deduped_source_refs: list[str] = []
            seen_content_hashes: set[str] = set()
            unique_chunks = []
            for chunk in parsed_chunks:
                chunk_content_hash = str(chunk.metadata.get("content_hash") or content_hash(chunk.text))
                if chunk_content_hash in seen_content_hashes:
                    deduped_source_refs.append(chunk.source_ref)
                    continue
                seen_content_hashes.add(chunk_content_hash)
                unique_chunks.append(chunk)
            parsed_chunks = unique_chunks
            deduped_chunk_count = original_chunk_count - len(parsed_chunks)
            if not parsed_chunks:
                raise MatrixArkError("resource ingestion produced only duplicate chunks")
            resource_version_value = str(parsed_chunks[0].metadata.get("resource_version") or "")
            resource_content_hash = content_hash("\n".join(str(chunk.metadata.get("content_hash") or content_hash(chunk.text)) for chunk in parsed_chunks))
            superseded_chunk_count = sum(1 for chunk in parsed_chunks if chunk.metadata.get("supersedes_chunk_hash"))
            superseded_chunk_hashes = [
                int(chunk.metadata["supersedes_chunk_hash"])
                for chunk in parsed_chunks
                if isinstance(chunk.metadata.get("supersedes_chunk_hash"), int)
            ]
            parse_warnings = aggregate_parse_warnings_from_chunks(parsed_chunks)
            chunk_vectors = embeddings_for_texts([embedding_text_for_chunk(chunk) for chunk in parsed_chunks])
            index_write_count = 0
            index_candidate_count = 0
            index_dropped_by_cap_count = 0
            secondary_index_budget = new_secondary_index_budget()
            resource_kind = "skill" if skill_hash is not None else "resource"
            resource_l0_text = summarize_text(
                summarize_resource_chunks(parsed_chunks, raw_uri=raw_uri, resource_kind=resource_kind),
                limit=700,
            )
            resource_summary_hash = stable_hash(f"{resource_kind}_l0:{raw_uri}:{node_hash}")
            resource_summary_vector = embedding_for_text(" ".join(node_path + [resource_l0_text]))
            self.append(
                {
                    "record_type": "context_summary",
                    "summary_type": f"{resource_kind}_l0",
                    "summary_hash": resource_summary_hash,
                    "import_task_hash": resource_import_task_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "raw_uri": raw_uri,
                    "summary_text": resource_l0_text,
                    "source_chunk_hashes": [chunk.chunk_hash for chunk in parsed_chunks],
                    "scope": resource_record_scope,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            self.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": f"{resource_kind}_l0",
                    "ref_type": "summary",
                    "ref_hash": resource_summary_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(resource_summary_vector),
                    "model": embedding_model_name(),
                    "vector": resource_summary_vector,
                    "scope": resource_record_scope,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            resource_dirty_hashes = self.mark_node_summary_dirty(
                node_path=node_path,
                scope=envelope["scope"],
                updated_at_ms=envelope["ingestion_time_ms"],
                source_ref_type=f"{resource_kind}_summary",
                source_hash_field="source_summary_hash",
                source_hash=resource_summary_hash,
                dirty_reason=f"{resource_kind}_update",
            )
            raw_resource_indexes = ordered_unique(
                [
                    context_index_name("source_type", envelope["kind"]),
                    context_index_name("resource_type", resource_type or parsed_chunks[0].metadata.get("resource_type", "txt")),
                ]
                + (
                    [
                        context_index_name("skill_name", parsed_skill.name),
                    ]
                    + [context_index_name("skill_trigger", trigger) for trigger in parsed_skill.metadata.get("triggers", [])]
                    + [context_index_name("skill_tool", tool) for tool in parsed_skill.metadata.get("allowed_tools", [])]
                    if skill_hash is not None
                    else []
                )
            )
            index_candidate_count += len(raw_resource_indexes)
            resource_indexes = take_secondary_index_terms(raw_resource_indexes, secondary_index_budget)
            for index_name in resource_indexes:
                index_write_count += 1
                self.append(
                    context_index_posting_record(
                        index_name=index_name,
                        capability=f"{resource_kind}_summary",
                        ref_type="summary",
                        ref_hashes=[resource_summary_hash],
                        node_hash=node_hash,
                        scope=resource_record_scope,
                        updated_at_ms=envelope["ingestion_time_ms"],
                        storage_options=envelope.get("storage_options", {}),
                    )
                )
            resource_manifest_hash = stable_hash(f"resource_manifest:{raw_uri}:{node_hash}")
            raw_uri_hash = stable_hash(raw_uri)
            if envelope["kind"] == "resource":
                manifest_hash = resource_manifest_hash
                self.append(
                    {
                        "record_type": "resource_manifest",
                        "resource_hash": manifest_hash,
                        "import_task_hash": resource_import_task_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "raw_uri": raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                        "resource_version": resource_version_value,
                        "content_hash": resource_content_hash,
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": raw_storage_policy,
                        "raw_bytes_stored": False,
                        "upload_status": storage_resolution.get("upload_status", "not_required"),
                        "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                        "cloud_key": storage_resolution.get("cloud_key", ""),
                        "parse_warnings": parse_warnings[:100],
                        "parse_warning_count": len(parse_warnings),
                        "chunk_count": len(parsed_chunks),
                        "original_chunk_count": original_chunk_count,
                        "deduped_chunk_count": deduped_chunk_count,
                        "deduped_source_refs": deduped_source_refs[:50],
                        "superseded_chunk_count": superseded_chunk_count,
                        "superseded_chunk_hashes": superseded_chunk_hashes[:200],
                        "summary_dirty_hashes": resource_dirty_hashes,
                        "async_parent_summary_required": bool(resource_dirty_hashes),
                        "access_scope": access_scope,
                        "deployment_scope": deployment_scope,
                        "token_estimate": sum(chunk.token_estimate for chunk in parsed_chunks),
                        "scope": resource_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                self.append(
                    {
                        "record_type": "resource_registry",
                        "registry_hash": stable_hash(f"resource_registry:{raw_uri}:{node_hash}:{resource_version_value}:{deployment_scope}"),
                        "resource_hash": manifest_hash,
                        "import_task_hash": resource_import_task_hash,
                        "raw_uri": raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                        "resource_version": resource_version_value,
                        "content_hash": resource_content_hash,
                        "chunk_count": len(parsed_chunks),
                        "superseded_chunk_hashes": superseded_chunk_hashes[:200],
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": raw_storage_policy,
                        "upload_status": storage_resolution.get("upload_status", "not_required"),
                        "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                        "cloud_key": storage_resolution.get("cloud_key", ""),
                        "access_scope": access_scope,
                        "deployment_scope": deployment_scope,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            for chunk, vector in zip(parsed_chunks, chunk_vectors):
                resource_chunk_hashes.append(chunk.chunk_hash)
                source_locator = source_locator_from_ref(chunk.source_ref, raw_uri)
                chunk_metadata_source = {**chunk.metadata, "source_locator": source_locator}
                chunk_metadata = serving_resource_metadata(chunk_metadata_source)
                chunk_debug_metadata = debug_resource_metadata(chunk.metadata)
                if skill_hash is not None:
                    self.append(
                        {
                            "record_type": "skill_section",
                            "import_task_hash": resource_import_task_hash,
                            "skill_hash": skill_hash,
                            "section_hash": chunk.chunk_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "resource_hash": skill_hash,
                            "raw_uri_hash": raw_uri_hash,
                            "source_locator": source_locator,
                            "heading": chunk_metadata.get("heading", ""),
                            "text": chunk.text,
                            "token_estimate": chunk.token_estimate,
                            "metadata": chunk_metadata,
                            "access_scope": access_scope,
                            "deployment_scope": deployment_scope,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                self.append(
                    {
                        "record_type": "resource_chunk",
                        "import_task_hash": resource_import_task_hash,
                        "chunk_hash": chunk.chunk_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "resource_hash": resource_manifest_hash if skill_hash is None else skill_hash,
                        "raw_uri_hash": raw_uri_hash,
                        "resource_type": chunk_metadata.get("resource_type") or resource_type,
                        "source_locator": source_locator,
                        "text": chunk.text,
                        "token_estimate": chunk.token_estimate,
                        "metadata": chunk_metadata,
                        "access_scope": access_scope,
                        "deployment_scope": deployment_scope,
                        "scope": resource_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                if chunk_debug_metadata:
                    self.append(
                        {
                            "record_type": "context_debug_record",
                            "debug_type": "resource_chunk_parse_detail",
                            "ref_type": "skill_section" if skill_hash is not None else "resource_chunk",
                            "ref_hash": chunk.chunk_hash,
                            "chunk_hash": chunk.chunk_hash,
                            "import_task_hash": resource_import_task_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "resource_hash": resource_manifest_hash if skill_hash is None else skill_hash,
                            "raw_uri_hash": raw_uri_hash,
                            "raw_uri": raw_uri,
                            "source_locator": source_locator,
                            "source_ref": chunk.source_ref,
                            "metadata_debug": chunk_debug_metadata,
                            "text_preview": clip_context_text(chunk.text),
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                self.append(
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "resource_chunk",
                        "ref_type": "resource_chunk",
                        "ref_hash": chunk.chunk_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dim": len(vector),
                        "model": embedding_model_name(),
                        "vector": vector,
                        "scope": resource_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                if skill_hash is not None:
                    self.append(
                        {
                            "record_type": "context_embedding",
                            "embedding_type": "skill_section",
                            "ref_type": "skill_section",
                            "ref_hash": chunk.chunk_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "dim": len(vector),
                            "model": embedding_model_name(),
                            "vector": vector,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                raw_chunk_index_terms = (
                    [
                        context_index_name("source_type", "skill" if skill_hash is not None else "resource"),
                        context_index_name("resource_type", chunk_metadata.get("resource_type") or resource_type),
                    ]
                    + metadata_index_terms(chunk.metadata)
                    + (
                        [context_index_name("skill_name", parsed_skill.name)]
                        + [context_index_name("skill_trigger", trigger) for trigger in parsed_skill.metadata.get("triggers", [])]
                        + [context_index_name("skill_tool", tool) for tool in parsed_skill.metadata.get("allowed_tools", [])]
                        if skill_hash is not None and parsed_skill is not None
                        else []
                    )
                )
                index_candidate_count += len([term for term in raw_chunk_index_terms if term])
                chunk_index_terms = limited_index_terms(
                    raw_chunk_index_terms,
                    limit=MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
                )
                index_dropped_by_cap_count += max(0, len(ordered_unique([term for term in raw_chunk_index_terms if term])) - len(chunk_index_terms))
                chunk_index_terms = take_secondary_index_terms(chunk_index_terms, secondary_index_budget)
                for index_name in chunk_index_terms:
                    index_write_count += 1
                    self.append(
                        {
                            "record_type": "context_index",
                            "index_name": index_name,
                            "index_hash": stable_hash(f"{index_name}:{chunk.chunk_hash}"),
                            "ref_type": "skill_section" if skill_hash is not None else "resource_chunk",
                            "ref_hash": chunk.chunk_hash,
                            "chunk_hash": chunk.chunk_hash,
                            "resource_hash": resource_manifest_hash if skill_hash is None else skill_hash,
                            "source_locator": source_locator,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
            resource_fact_records: list[Json] = []
            fact_chunks = [chunk for chunk in parsed_chunks if skill_hash is None and should_extract_resource_fact(chunk.text, chunk.metadata)][:MAX_RESOURCE_FACT_CHUNKS]
            remaining_resource_fact_budget = max(0, MAX_RESOURCE_FACTS_PER_RESOURCE)
            for chunk in fact_chunks:
                if remaining_resource_fact_budget <= 0:
                    break
                source_locator = source_locator_from_ref(chunk.source_ref, raw_uri)
                chunk_metadata = serving_resource_metadata({**chunk.metadata, "source_locator": source_locator})
                for fact_extraction in extract_resource_facts(
                    chunk,
                    chunk_metadata=chunk_metadata,
                    envelope=envelope,
                    raw_uri=raw_uri,
                    resource_version=resource_version_value,
                )[:remaining_resource_fact_budget]:
                    remaining_resource_fact_budget -= 1
                    fact_event_type = str(fact_extraction["event_type"])
                    fact_entity_type = str(fact_extraction["entity_type"])
                    fact_value = str(fact_extraction.get("value", ""))
                    fact_event_hash = stable_hash(f"resource_fact:{chunk.chunk_hash}:{fact_event_type}:{resource_version_value}")
                    resource_fact_event_hashes.append(fact_event_hash)
                    fact_summary = summarize_text(f"{fact_event_type}: {fact_value}", limit=320)
                    resource_fact_records.append(
                        {
                            "record_type": "context_event",
                            "event_id_hash": fact_event_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "text": chunk.text,
                            "summary_text": fact_summary,
                            "classification": fact_extraction.get("classification", ""),
                            "event_type": fact_extraction.get("event_type", ""),
                            "entity_type": fact_extraction.get("entity_type", ""),
                            "status": fact_extraction.get("status", "observed"),
                            "source_kind": "resource_fact",
                            "envelope": {**envelope, "kind": "resource_fact"},
                            "internal_extraction": fact_extraction,
                            "source_chunk_hash": chunk.chunk_hash,
                            "resource_hash": resource_manifest_hash,
                            "source_locator": source_locator,
                            "resource_version": resource_version_value,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    fact_vector = embedding_for_text(fact_event_type + " " + fact_value + " " + chunk.text)
                    resource_fact_records.append(
                        {
                            "record_type": "context_embedding",
                            "embedding_type": "event_text",
                            "ref_type": "event",
                            "ref_hash": fact_event_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "dim": len(fact_vector),
                            "model": embedding_model_name(),
                            "vector": fact_vector,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    entity_name = str(fact_extraction.get("entity_name") or fact_entity_type)
                    entity_hash = stable_hash(f"{node_hash}:{fact_entity_type}:{entity_name}:{chunk.chunk_hash}")
                    resource_fact_entity_hashes.append(entity_hash)
                    entity_state = summarize_text(f"{fact_event_type}: {fact_value}. Source: {chunk.text}", limit=360)
                    resource_fact_records.append(
                        {
                            "record_type": "context_entity",
                            "entity_hash": entity_hash,
                            "batch_id_hash": resource_import_task_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "scope": resource_record_scope,
                            "entity_type": fact_entity_type,
                            "entity_name": entity_name,
                            "state": entity_state,
                            "confidence": fact_extraction.get("confidence", 0.78),
                            "operator": "LATEST",
                            "source_event_ids": [fact_event_hash],
                            "source_chunk_hash": chunk.chunk_hash,
                            "resource_hash": resource_manifest_hash,
                            "source_locator": source_locator,
                            "resource_version": resource_version_value,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    entity_vector = embedding_for_text(fact_entity_type + " " + entity_name + " " + entity_state)
                    resource_fact_records.append(
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
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    # Resource facts are ContextEvent/ContextEntity records with
                    # source_chunk refs. The resource chunk/index rows already provide
                    # secondary filtering, so avoid per-fact event index fanout here.
            if resource_fact_records:
                self.append_many(resource_fact_records)
            resource_import_metrics = {
                "duration_ms": round((time.perf_counter() - import_started_perf) * 1000.0, 3),
                "parser_chunk_count": original_chunk_count,
                "chunk_count": len(parsed_chunks),
                "dedupe_count": deduped_chunk_count,
                "embedding_count": len(chunk_vectors) + 1 + len(resource_fact_event_hashes) + len(resource_fact_entity_hashes),
                "resource_fact_count": len(resource_fact_event_hashes),
                "resource_entity_count": len(resource_fact_entity_hashes),
                "index_candidate_count": index_candidate_count,
                "index_write_count": index_write_count,
                "index_dropped_by_cap_count": index_dropped_by_cap_count,
                **secondary_index_budget_summary(secondary_index_budget),
                "index_cap_per_chunk": MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
                "index_cap_per_fact": MAX_INDEX_TERMS_PER_RESOURCE_FACT,
                "parse_warning_count": len(parse_warnings),
                "parse_warnings": parse_warnings[:100],
                "raw_storage_mode": storage_resolution["storage_mode"],
                "raw_storage_policy": raw_storage_policy,
                "raw_bytes_stored": False,
                "upload_status": storage_resolution.get("upload_status", "not_required"),
                "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                "cloud_key": storage_resolution.get("cloud_key", ""),
                "summary_dirty_count": len(resource_dirty_hashes),
            }
            resource_import_task_status = "completed"
            self.append(
                {
                    "record_type": "resource_import_task",
                    "task_hash": resource_import_task_hash,
                    "status": "completed",
                    "kind": envelope["kind"],
                    "raw_uri": raw_uri,
                    "requested_raw_uri": requested_raw_uri,
                    "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                    "resource_version": resource_version_value,
                    "content_hash": resource_content_hash,
                    "raw_storage_mode": storage_resolution["storage_mode"],
                    "raw_storage_policy": raw_storage_policy,
                    "raw_bytes_stored": False,
                    "upload_status": storage_resolution.get("upload_status", "not_required"),
                    "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                    "cloud_key": storage_resolution.get("cloud_key", ""),
                    "parse_warnings": parse_warnings[:100],
                    "parse_warning_count": len(parse_warnings),
                    "chunk_count": len(parsed_chunks),
                    "original_chunk_count": original_chunk_count,
                    "deduped_chunk_count": deduped_chunk_count,
                    "superseded_chunk_count": superseded_chunk_count,
                    "superseded_chunk_hashes": superseded_chunk_hashes[:200],
                    "resource_fact_count": len(resource_fact_event_hashes),
                    "resource_entity_count": len(resource_fact_entity_hashes),
                    "index_candidate_count": index_candidate_count,
                    "index_write_count": index_write_count,
                    "index_dropped_by_cap_count": index_dropped_by_cap_count,
                    **secondary_index_budget_summary(secondary_index_budget),
                    "index_cap_per_chunk": MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
                    "index_cap_per_fact": MAX_INDEX_TERMS_PER_RESOURCE_FACT,
                    "summary_dirty_hashes": resource_dirty_hashes,
                    "progress": {"stage": "completed", "percent": 100},
                    "metrics": resource_import_metrics,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": resource_record_scope,
                    "updated_at_ms": now_ms(),
                }
            )
            self.append(
                {
                    "record_type": "matrixark_metric",
                    "metric_name": "resource_import",
                    "task_hash": resource_import_task_hash,
                    "kind": envelope["kind"],
                    "raw_uri": raw_uri,
                    "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                    "metrics": resource_import_metrics,
                    "progress": {"stage": "completed", "percent": 100},
                    "scope": resource_record_scope,
                    "created_at_ms": now_ms(),
                }
            )
        hot_record_scope = resource_record_scope if envelope["kind"] in {"resource", "skill"} else envelope["scope"]
        summary_text = summarize_text(text)
        embedding_started_perf = time.perf_counter()
        event_embedding = embedding_for_text(text)
        self._observe_model_latency("embedding", (time.perf_counter() - embedding_started_perf) * 1000.0)
        with self.write_batch("message_ingest_hot_path"):
            session_key_parts = [str(part) for part in context_node_key(envelope)]
            if any(session_key_parts):
                session_summary_source = " ".join(
                    [item.get("text", "") for item in prior_context.get("summaries", [])[:2]]
                    + [item.get("text", "") for item in prior_context.get("messages", [])[:2]]
                    + [text]
                )
                session_summary_text = summarize_text(session_summary_source, limit=512)
                session_summary_hash = stable_hash("session:" + "/".join(session_key_parts))
                self.append(
                    {
                        "record_type": "context_summary",
                        "summary_type": "session_l0",
                        "summary_hash": session_summary_hash,
                        "summary_identity": "stable_per_session_node",
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "context_node_key": session_key_parts,
                        "summary_text": session_summary_text,
                        "source_event_hash": event_id_hash,
                        "scope": hot_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                self.append(
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "session_l0",
                        "ref_type": "summary",
                        "ref_hash": session_summary_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dim": len(embedding_for_text(session_summary_text)),
                        "model": embedding_model_name(),
                        "vector": embedding_for_text(session_summary_text),
                        "scope": hot_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            self.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": "event_text",
                    "ref_type": "event",
                    "ref_hash": event_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(event_embedding),
                    "model": embedding_model_name(),
                    "vector": event_embedding,
                    "scope": hot_record_scope,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            record = {
                "record_type": "context_event",
                "event_id_hash": event_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "text": text,
                "classification": extraction.get("classification", ""),
                "event_type": extraction.get("event_type", ""),
                "entity_type": extraction.get("entity_type", ""),
                "status": extraction.get("status", "observed"),
                "source_kind": envelope.get("kind", "message"),
                "envelope": envelope,
                "internal_extraction": extraction,
                "prior_context": prior_context,
                "agent_hook": hook,
                "storage_options": envelope.get("storage_options", {}),
            }
            self.append(record)
            event_index_terms = ordered_unique(
                extraction.get("indexes")
                or [
                    context_index_name("event_type", extraction.get("event_type") or infer_event_type(text)),
                    context_index_name("classification", non_default_classification(extraction.get("classification"))),
                    context_index_name("status", extraction.get("status") or "observed"),
                    context_index_name("source_type", envelope["kind"]),
                ]
            )
            event_index_records: list[Json] = []
            for index_name in event_index_terms:
                event_index_records.append(
                    {
                        "record_type": "context_index",
                        "index_name": index_name,
                        "capability": "context_event",
                        "ref_type": "event",
                        "ref_hashes": [event_id_hash],
                        "node_hash": node_hash,
                        "scope": envelope["scope"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            if event_index_records:
                self.append_many(event_index_records)
            if self.session_buffer_enabled(args, kind=envelope["kind"]):
                self.append_session_buffer_event(envelope=envelope, event_id_hash=event_id_hash, node_hash=node_hash, node_path=node_path, hook=hook)
            summary_refresh = self.append_node_summary_embeddings(
                node_path=node_path,
                source_text=text,
                scope=hot_record_scope,
                updated_at_ms=envelope["ingestion_time_ms"],
                source_hash_field="source_event_hash",
                source_hash=event_id_hash,
            )
        session_buffer_enabled = self.session_buffer_enabled(args, kind=envelope["kind"])
        pending_event_count = len(self.pending_session_events(envelope["scope"])) if session_buffer_enabled else 0
        auto_batch_result: Json | None = None
        auto_batch_extract = self.auto_batch_extract_enabled(args, kind=envelope["kind"])
        session_boundary_commit = self.session_boundary_commit_requested(args, hook=hook)
        session_buffer_threshold = args.get("session_buffer_threshold", 20)
        if not isinstance(session_buffer_threshold, int) or session_buffer_threshold <= 0:
            raise MatrixArkError("session_buffer_threshold must be a positive integer")
        if auto_batch_extract and (session_boundary_commit or pending_event_count >= session_buffer_threshold):
            auto_batch_result = self.session_commit(
                {
                    "scope": hot_record_scope,
                    "metadata": envelope["metadata"],
                    "threshold_messages": session_buffer_threshold,
                    "force": session_boundary_commit,
                    "max_messages": None if session_boundary_commit else session_buffer_threshold,
                    "commit_reason": "hook_boundary" if session_boundary_commit else "threshold",
                    "understanding_provider": args.get("understanding_provider"),
                    "extraction_provider": args.get("extraction_provider"),
                    "segment_provider": args.get("segment_provider"),
                    "segment_model": args.get("segment_model"),
                    "segment_model_path": args.get("segment_model_path"),
                    "segment_max_new_tokens": args.get("segment_max_new_tokens"),
                    "segment_provider_fallback": args.get("segment_provider_fallback"),
                    "skip_prior_context": bool(args.get("skip_prior_context", False)),
                    "storage_options": envelope.get("storage_options", {}),
                },
                hook=hook,
            )
        return {
            "status": "accepted",
            "event_id_hash": event_id_hash,
            "node_hash": record["node_hash"],
            "storage_options": envelope.get("storage_options", {}),
            "storage_route": envelope.get("storage_route", {}),
            "hook_captured": hook is not None,
            "embedding_model": embedding_model_name(),
            "embedding_execution_mode": embedding_execution_mode_name(),
            "embedding_fallback_used": embedding_fallback_used(),
            "extraction_mode": extraction["mode"],
            "classification": extraction.get("classification", "UNCLASSIFIED"),
            "prior_context": extraction.get("prior_context", ""),
            "prior_refs": extraction.get("prior_refs", []),
            "prior_message_count": extraction.get("prior_message_count", 0),
            "prior_summary_count": extraction.get("prior_summary_count", 0),
            "quality_warning": extraction.get("quality_warning", ""),
            "summary_refresh": summary_refresh,
            "resource_summary_refresh": {
                "status": "dirty_marked" if resource_dirty_hashes else "not_applicable",
                "dirty_hashes": resource_dirty_hashes,
                "refresh_result": None,
                "async_required": bool(resource_dirty_hashes),
            },
            "resource_import_task": {
                "task_hash": resource_import_task_hash,
                "status": resource_import_task_status,
                "wait": resource_import_wait,
                "metrics": resource_import_metrics,
                "raw_uri": raw_uri if resource_import_task_hash else "",
                "requested_raw_uri": requested_raw_uri if resource_import_task_hash else "",
                "raw_storage_mode": storage_resolution.get("storage_mode", "") if resource_import_task_hash else "",
                "raw_storage_policy": raw_storage_policy if resource_import_task_hash else "",
                "raw_bytes_stored": False if resource_import_task_hash else None,
                "upload_status": storage_resolution.get("upload_status", "") if resource_import_task_hash else "",
                "cloud_bucket": storage_resolution.get("cloud_bucket", "") if resource_import_task_hash else "",
                "cloud_key": storage_resolution.get("cloud_key", "") if resource_import_task_hash else "",
                "progress": {"stage": resource_import_task_status, "percent": 100 if resource_import_task_status == "completed" else 0},
            },
            "node_materialization": node_materialization,
            "resource_chunks": resource_chunk_hashes,
            "resource_chunk_count": len(resource_chunk_hashes),
            "resource_original_chunk_count": original_chunk_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_deduped_chunk_count": deduped_chunk_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_deduped_source_refs": deduped_source_refs[:20] if envelope["kind"] in {"resource", "skill"} else [],
            "resource_version": resource_version_value if envelope["kind"] in {"resource", "skill"} else "",
            "resource_content_hash": resource_content_hash if envelope["kind"] in {"resource", "skill"} else "",
            "resource_parse_warnings": parse_warnings if envelope["kind"] in {"resource", "skill"} else [],
            "resource_parse_warning_count": len(parse_warnings) if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_raw_uri": raw_uri if envelope["kind"] in {"resource", "skill"} else "",
            "resource_requested_raw_uri": requested_raw_uri if envelope["kind"] in {"resource", "skill"} else "",
            "resource_raw_storage_mode": storage_resolution.get("storage_mode", "") if envelope["kind"] in {"resource", "skill"} else "",
            "resource_raw_storage_policy": raw_storage_policy if envelope["kind"] in {"resource", "skill"} else "",
            "resource_raw_bytes_stored": False if envelope["kind"] in {"resource", "skill"} else None,
            "backend_readiness": backend_readiness or {},
            "resource_superseded_chunk_count": superseded_chunk_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_superseded_chunk_hashes": superseded_chunk_hashes if envelope["kind"] in {"resource", "skill"} else [],
            "resource_fact_events": resource_fact_event_hashes,
            "resource_fact_event_count": len(resource_fact_event_hashes),
            "resource_fact_entities": resource_fact_entity_hashes,
            "resource_fact_entity_count": len(resource_fact_entity_hashes),
            "resource_index_candidate_count": index_candidate_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_index_write_count": index_write_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_index_dropped_by_cap_count": index_dropped_by_cap_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_index_cap_per_chunk": MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
            "resource_index_cap_per_fact": MAX_INDEX_TERMS_PER_RESOURCE_FACT,
            "skill_hash": skill_hash,
            "session_buffer": {
                "enabled": session_buffer_enabled,
                "buffer_key": list(session_buffer_key(envelope)),
                "pending_event_count": pending_event_count,
                "threshold_messages": session_buffer_threshold,
                "auto_batch_extract": auto_batch_extract,
                "boundary_commit_requested": session_boundary_commit,
            },
            "idle_commit_result": idle_commit_result,
            "auto_batch_extract_result": auto_batch_result,
        }

    def batch_extract(self, args: Json, *, hook: Json | None = None) -> Json:
        envelope = normalize_envelope(args, default_kind="message")
        hook = validate_hook(hook)
        threshold = args.get("threshold_messages", 20)
        force = bool(args.get("force", False))
        derive_from_existing_events = bool(args.get("derive_from_existing_events", False))
        source_event_ids = [int(ref) for ref in args.get("source_event_ids", [])] if isinstance(args.get("source_event_ids", []), list) else []
        if not isinstance(threshold, int) or threshold <= 0:
            raise MatrixArkError("threshold_messages must be a positive integer")
        if len(envelope["messages"]) < threshold and not force:
            return {
                "status": "deferred",
                "message_count": len(envelope["messages"]),
                "threshold_messages": threshold,
                "reason": "logical batch below extraction threshold",
            }

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
        started_perf = time.perf_counter()
        query = require_string(args, "query")
        scope = optional_object(args, "scope")
        storage_options = normalize_storage_options(args)
        ranking = optional_object(args, "ranking")
        audit_mode = str(args.get("audit_mode") or os.environ.get("MATRIXARK_CONTEXT_AUDIT_MODE", "telemetry_only")).strip().lower()
        if audit_mode not in {"full", "telemetry_only", "off"}:
            raise MatrixArkError("audit_mode must be full, telemetry_only, or off")
        if "audit_sample_rate" in args:
            raw_audit_sample_rate = args.get("audit_sample_rate")
        elif audit_mode == "full":
            raw_audit_sample_rate = 1.0
        else:
            raw_audit_sample_rate = os.environ.get("MATRIXARK_CONTEXT_AUDIT_SAMPLE_RATE", 0.01)
        try:
            audit_sample_rate = clamp01(float(raw_audit_sample_rate))
        except (TypeError, ValueError):
            raise MatrixArkError("audit_sample_rate must be a number between 0 and 1")
        raw_deadline_ms = args.get("deadline_ms", ranking.get("deadline_ms", os.environ.get("MATRIXARK_RETRIEVAL_TIMEOUT_MS", 0)))
        try:
            deadline_ms = int(raw_deadline_ms or 0)
        except (TypeError, ValueError):
            raise MatrixArkError("deadline_ms must be an integer")

        def deadline_exceeded() -> bool:
            return deadline_ms > 0 and (time.perf_counter() - started_perf) * 1000.0 >= deadline_ms

        stage_names = ["query_understanding", "candidate_fetch", "node_traversal", "rerank_score", "pack", "audit"]
        explicit_stage_budgets = optional_object(args, "stage_budgets_ms") or optional_object(ranking, "stage_budgets_ms")
        if deadline_ms > 0:
            default_stage_budgets = {
                "query_understanding": max(25, int(deadline_ms * 0.15)),
                "candidate_fetch": max(25, int(deadline_ms * 0.20)),
                "node_traversal": max(25, int(deadline_ms * 0.15)),
                "rerank_score": max(25, int(deadline_ms * 0.30)),
                "pack": max(25, int(deadline_ms * 0.15)),
                "audit": max(10, int(deadline_ms * 0.05)),
            }
        else:
            default_stage_budgets = {
                "query_understanding": 500,
                "candidate_fetch": 750,
                "node_traversal": 500,
                "rerank_score": 1000,
                "pack": 500,
                "audit": 250,
            }
        stage_budgets_ms: dict[str, int] = {}
        for stage in stage_names:
            value = explicit_stage_budgets.get(stage, ranking.get(f"{stage}_budget_ms", default_stage_budgets[stage]))
            if not isinstance(value, int) or value < 0:
                raise MatrixArkError(f"stage budget for {stage} must be a non-negative integer")
            stage_budgets_ms[stage] = value
        stage_latencies_ms: dict[str, float] = {}
        stage_started_perf = time.perf_counter()

        def finish_retrieval_stage(stage: str, started: float) -> float:
            elapsed = round((time.perf_counter() - started) * 1000.0, 3)
            stage_latencies_ms[stage] = elapsed
            self._observe_model_latency(f"retrieval_{stage}", elapsed)
            return elapsed

        def stage_budget_snapshot() -> Json:
            stages = {
                stage: {
                    "budget_ms": stage_budgets_ms[stage],
                    "elapsed_ms": round(float(stage_latencies_ms.get(stage, 0.0)), 3),
                    "over_budget": bool(stage_budgets_ms[stage] > 0 and float(stage_latencies_ms.get(stage, 0.0)) > stage_budgets_ms[stage]),
                }
                for stage in stage_names
            }
            return {
                "enabled": True,
                "source": "explicit" if explicit_stage_budgets else ("deadline_derived" if deadline_ms > 0 else "defaults"),
                "stages": stages,
                "over_budget_stages": [stage for stage, row in stages.items() if row["over_budget"]],
            }

        question_type = str(args.get("question_type") or infer_query_type(query))
        retrieval_session_scope = str(args.get("session_scope") or ranking.get("session_scope") or "prefer").strip().lower()
        if retrieval_session_scope not in {"prefer", "only"}:
            raise MatrixArkError("session_scope must be prefer or only")
        retrieval_scope = {**scope, "_session_scope": retrieval_session_scope}
        secondary_index_filter_groups = infer_secondary_index_filter_groups(query, question_type)
        secondary_index_filter_mode = "any_group" if len(secondary_index_filter_groups) > 1 else "all_groups"
        secondary_index_dropped_count = 0
        secondary_index_matched_count = 0
        budget_source = "agent_provided_max_context_tokens" if "max_context_tokens" in args else "matrixark_default_max_context_tokens"
        max_context_tokens = args.get("max_context_tokens", DEFAULT_MAX_CONTEXT_TOKENS)
        if not isinstance(max_context_tokens, int) or max_context_tokens <= 0:
            raise MatrixArkError("max_context_tokens must be a positive integer")
        local_budget = local_context_budget(args)
        local_tokens = int(local_budget.get("token_estimate", 0))
        safety_margin_tokens = int(local_budget.get("safety_margin_tokens", 0))
        remote_context_budget_tokens = max(0, max_context_tokens - local_tokens - safety_margin_tokens)
        local_budget["remote_budget_tokens"] = remote_context_budget_tokens
        cross_session_policy = build_cross_session_policy(
            args,
            ranking,
            question_type=question_type,
            session_scope=retrieval_session_scope,
            remote_budget_tokens=remote_context_budget_tokens,
        )
        shared_context_policy = build_shared_context_policy(
            args,
            ranking,
            remote_budget_tokens=remote_context_budget_tokens,
        )
        query_terms = {term for term in tokens(query) if len(term) > 2}
        raw_reference_time_ms = args.get("reference_time_ms", now_ms())
        if not isinstance(raw_reference_time_ms, int):
            raise MatrixArkError("reference_time_ms must be an integer")
        reference_time_ms = raw_reference_time_ms
        query_plan = build_structured_query_plan(
            query,
            question_type=question_type,
            secondary_index_filter_groups=secondary_index_filter_groups,
            secondary_index_filter_mode=secondary_index_filter_mode,
            reference_time_ms=reference_time_ms,
        )
        debug_refs = bool(args.get("include_debug_refs") or ranking.get("include_debug_refs") or CONTEXT_PACK_DEBUG_REFS)
        pack_cache_enabled = (
            self._context_pack_cache_max_entries > 0
            and self._context_pack_cache_ttl_s > 0
            and python_hot_cache_allowed(backend_label=str(getattr(self, "_backend_label", lambda: "local")()))
        )
        pack_cache_key = (
            self._retrieval_records_cache_generation,
            canonical_scope_key(scope),
            query,
            question_type,
            retrieval_session_scope,
            max_context_tokens,
            int(local_budget.get("token_estimate", 0)),
            tuple(sorted(local_budget.get("text_hashes", set()))),
            json.dumps(ranking, sort_keys=True, separators=(",", ":")),
            bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False)),
        )
        if pack_cache_enabled:
            with self._context_pack_cache_lock:
                cached = self._context_pack_cache.get(pack_cache_key)
                if cached is not None:
                    cached_at, cached_pack = cached
                    if time.monotonic() - cached_at <= self._context_pack_cache_ttl_s:
                        pack = json.loads(json.dumps(cached_pack))
                        pack["context_pack_cache_hit"] = True
                        recall_policy = pack.get("recall_policy") if isinstance(pack.get("recall_policy"), dict) else {}
                        recall_policy["context_pack_cache"] = {"hit": True, "ttl_s": self._context_pack_cache_ttl_s}
                        pack["recall_policy"] = recall_policy
                        return compact_context_pack_for_serving(pack, include_debug=debug_refs)
                    self._context_pack_cache.pop(pack_cache_key, None)
        auxiliary_quota = integer_arg(ranking, "auxiliary_quota", 2, minimum=0)
        def annotate_session_continuity(candidate: Json, record: Json) -> Json:
            record_scope = candidate_access_scope(record)
            status = session_continuity_status(record_scope, retrieval_scope)
            boost = session_continuity_boost({**candidate, "session_continuity": status}, question_type)
            reason = (
                "same-session continuity"
                if status == "same_session"
                else "cross-session memory bridge"
                if status == "cross_session"
                else "session-neutral context"
            )
            return {
                **candidate,
                "session_continuity": status,
                "continuity_boost": round(boost, 6),
                "continuity_reason": reason,
                "question_type": question_type,
            }

        finish_retrieval_stage("query_understanding", stage_started_perf)
        native_pack = self.native_context_pack({
            "query": query,
            "scope": retrieval_scope,
            "question_type": question_type,
            "query_plan": query_plan,
            "secondary_index_groups": [sorted(group) for group in secondary_index_filter_groups],
            "secondary_index_filter_mode": secondary_index_filter_mode,
            "max_context_tokens": max_context_tokens,
            "local_budget": {
                "token_estimate": int(local_budget.get("token_estimate", 0)),
                "safety_margin_tokens": int(local_budget.get("safety_margin_tokens", 0)),
                "remote_budget_tokens": int(local_budget.get("remote_budget_tokens", max_context_tokens)),
            },
            "cross_session": cross_session_policy,
            "shared_context": shared_context_policy,
            "ranking": ranking,
            "deadline_ms": deadline_ms,
            "reference_time_ms": reference_time_ms,
            "include_superseded_resources": bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False)),
            "audit_mode": audit_mode,
        })
        if native_pack is not None:
            recall_policy = native_pack.get("recall_policy") if isinstance(native_pack.get("recall_policy"), dict) else {}
            recall_policy.setdefault("native_context_pack", {
                "enabled": True,
                "python_role": "mcp_auth_model_request_shaping_only",
                "backend_role": "scan_filter_score_pack",
            })
            recall_policy.setdefault("stage_latency_budgets", stage_budget_snapshot())
            native_pack["recall_policy"] = recall_policy
            native_pack.setdefault("context_pack_cache_hit", False)
            native_pack.setdefault("context_pack_assembly", "native_backend")
            native_pack.setdefault("remote_context_refs", native_pack.get("selected_refs", []))
            native_pack.setdefault("selected_ref_counts", selected_context_class_counts(native_pack.get("selected_refs", [])))
            selected_refs = native_pack.get("selected_refs", []) if isinstance(native_pack.get("selected_refs"), list) else []
            context_pack_id_text = str(native_pack.get("context_pack_id") or stable_hash(f"native:{query}:{selected_refs}:{now_ms()}"))
            native_pack["context_pack_id"] = context_pack_id_text
            if audit_mode == "full" and audit_sample_rate > 0 and (audit_sample_rate >= 1.0 or stable_hash(context_pack_id_text) % 10000 < int(audit_sample_rate * 10000)):
                self.append_audit(
                    compact_context_pack_audit_record({
                        "record_type": "context_pack_audit",
                        "context_pack_id": context_pack_id_text,
                        "query": query,
                        "scope": scope,
                        "summary_text": summarize_text(" ".join(str(item.get("text", "")) for item in selected_refs), limit=512),
                        "selected_refs": compact_refs_for_audit(selected_refs),
                        "local_context_refs": compact_local_context_refs(local_budget),
                        "context_sources_order": native_pack.get("context_sources_order", []),
                        "selected_ref_counts": native_pack.get("selected_ref_counts", {}),
                        "dropped_refs": native_pack.get("dropped_refs", {}),
                        "quality_warnings": native_pack.get("quality_warnings", []),
                        "question_type": question_type,
                        "packing_policy": native_pack.get("packing_policy", "native_backend"),
                        "recall_policy": recall_policy,
                        "stage_latency_budgets": recall_policy.get("stage_latency_budgets", {}),
                        "storage_options": storage_options,
                        "used_remote_context_tokens": native_pack.get("used_remote_context_tokens", native_pack.get("used_context_tokens", 0)),
                        "remote_context_budget_tokens": native_pack.get("remote_context_budget_tokens", max_context_tokens),
                        "requested_max_context_tokens": native_pack.get("requested_max_context_tokens", max_context_tokens),
                        "created_at_ms": now_ms(),
                    })
                )
            serving_selected_refs = compact_context_pack_refs(selected_refs, include_debug=debug_refs)
            native_pack["selected_refs"] = serving_selected_refs
            native_pack["remote_context_refs"] = serving_selected_refs
            native_pack["dropped_refs"] = compact_dropped_refs_for_context_pack(native_pack.get("dropped_refs", {}), include_debug=debug_refs)
            native_pack["context_pack_payload_policy"] = {
                "serving_refs": "compact" if not debug_refs else "debug_full",
                "hashes_and_matched_indexes": "audit_only" if not debug_refs else "included",
                "dropped_ref_details": "audit_only" if not debug_refs else "included",
                "enable_debug_refs_with": "include_debug_refs=true or MATRIXARK_CONTEXT_PACK_DEBUG_REFS=1",
            }
            return compact_context_pack_for_serving(native_pack, include_debug=debug_refs)
        if self.native_context_pack_required():
            raise MatrixArkError(
                "backend-native ContextPack assembly is required for TemporalStore serving, "
                "but this backend did not return matrixark_retrieve_context_pack. "
                "Python reference packing is disabled unless explicitly overridden for local debug."
            )
        embedding_started_perf = time.perf_counter()
        query_embedding = embedding_for_text(query)
        self._observe_model_latency("query_embedding", (time.perf_counter() - embedding_started_perf) * 1000.0)
        stage_started_perf = time.perf_counter()
        retrieval_record_result = self.retrieval_records(
            scope=retrieval_scope,
            secondary_index_groups=secondary_index_filter_groups,
        )
        records = retrieval_record_result["records"]
        retrieval_scan_stats = retrieval_record_result.get("scan_stats", {})

        def deadline_fallback(reason: str, fallback_records: list[Json] | None = None) -> Json:
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records if fallback_records is None else fallback_records,
                reason=reason,
                budget_source=budget_source,
            )
        skill_controls = self.latest_skill_controls(records)
        include_superseded_resources = bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False))
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
        finish_retrieval_stage("candidate_fetch", stage_started_perf)
        stage_started_perf = time.perf_counter()
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_record_load",
                budget_source=budget_source,
            )
        node_scores: dict[int, Json] = {}
        event_embedding_vectors: dict[int, list[float]] = {}
        entity_embedding_vectors: dict[int, list[float]] = {}
        segment_embedding_vectors: dict[int, list[float]] = {}
        compression_embedding_vectors: dict[int, list[float]] = {}
        resource_embedding_vectors: dict[int, list[float]] = {}
        skill_embedding_vectors: dict[int, list[float]] = {}
        index_terms_by_batch: dict[Any, list[str]] = {}
        index_terms_by_node: dict[Any, list[str]] = {}
        index_terms_by_ref: dict[Any, list[str]] = {}
        index_terms_by_node_for_prefilter: dict[int, list[str]] = {}
        node_summary_text_by_hash: dict[int, str] = {}
        for scan_index, record in enumerate(records, 1):
            if scan_index % 128 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_embedding_index_scan")
            record_type = record.get("record_type")
            if record_type == "context_index" and scope_matches(candidate_access_scope(record), retrieval_scope):
                index_name = str(record.get("index_name", ""))
                if index_name:
                    ref_hashes = context_index_ref_hashes(record)
                    if record.get("batch_id_hash") is not None:
                        index_terms_by_batch.setdefault(record.get("batch_id_hash"), []).append(index_name)
                    node_hash_for_index = record.get("node_hash")
                    try:
                        index_terms_by_node_for_prefilter.setdefault(int(node_hash_for_index), []).append(index_name)
                    except (TypeError, ValueError):
                        pass
                    if ref_hashes:
                        for ref_hash in ref_hashes:
                            index_terms_by_ref.setdefault(ref_hash, []).append(index_name)
                    else:
                        ref_hash = record.get("ref_hash") or record.get("chunk_hash") or record.get("section_hash") or record.get("skill_hash")
                        if ref_hash is not None:
                            index_terms_by_ref.setdefault(ref_hash, []).append(index_name)
                        else:
                            index_terms_by_node.setdefault(record.get("node_hash"), []).append(index_name)
            if record_type == "context_summary" and scope_matches(candidate_access_scope(record), scope):
                summary_type = str(record.get("summary_type", ""))
                if summary_type in {"node_l0", "node_l1", "batch_l0", "session_l0"}:
                    try:
                        node_hash_for_summary = int(record.get("node_hash"))
                    except (TypeError, ValueError):
                        continue
                    existing = node_summary_text_by_hash.get(node_hash_for_summary, "")
                    summary_text = str(record.get("summary_text", ""))
                    if len(summary_text) > len(existing):
                        node_summary_text_by_hash[node_hash_for_summary] = summary_text
        secondary_index_prefilter_node_hashes = {
            node_hash
            for node_hash, terms in index_terms_by_node_for_prefilter.items()
            if passes_secondary_index_filters(set(terms), secondary_index_filter_groups, mode=secondary_index_filter_mode)
        } if secondary_index_filter_groups else set()
        query_plan["secondary_index_prefilter"] = {
            "applied_before_l0_l1_traversal": True,
            "matched_node_count": len(secondary_index_prefilter_node_hashes),
            "fallback_when_no_index_matches": True,
            "strategy": "ContextIndex node hints boost L0/L1 traversal; leaf candidates still verify filters before embedding scoring",
        }
        for scan_index, record in enumerate(records, 1):
            if scan_index % 128 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_embedding_vector_scan")
            record_type = record.get("record_type")
            if record_type == "context_embedding" and not scope_matches(candidate_access_scope(record), scope):
                continue
            if record_type == "context_embedding" and record.get("embedding_type") in {"node_l0", "node_l1"}:
                dense_score = cosine(query_embedding, record.get("vector", []))
                node_hash = record["node_hash"]
                node_text = " ".join(record.get("node_path", [])) + " " + node_summary_text_by_hash.get(node_hash, "")
                sparse_score = sparse_lexical_score(query_terms, node_text)
                index_hint_boost = 0.08 if node_hash in secondary_index_prefilter_node_hashes else 0.0
                score = round(clamp01(0.72 * normalized_dense_score(dense_score) + 0.28 * sparse_score + index_hint_boost), 6)
                current = node_scores.get(node_hash)
                if current is None or score > current["score"]:
                    node_scores[node_hash] = {
                        "node_hash": node_hash,
                        "node_path": record.get("node_path", []),
                        "depth": record.get("depth", len(record.get("node_path", []))),
                        "score": score,
                        "dense_score": dense_score,
                        "sparse_score": sparse_score,
                        "embedding_type": record.get("embedding_type"),
                    }
            elif record_type == "context_embedding" and record.get("embedding_type") == "event_text":
                event_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "entity_state":
                entity_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "segment_text":
                segment_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "compression_summary":
                compression_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "resource_chunk":
                resource_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "skill_section":
                resource_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "skill_summary":
                skill_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
        for record in records:
            if record.get("record_type") != "context_node":
                continue
            try:
                node_hash = int(record.get("node_hash"))
            except (TypeError, ValueError):
                continue
            if node_hash not in secondary_index_prefilter_node_hashes or node_hash in node_scores:
                continue
            node_scores[node_hash] = {
                "node_hash": node_hash,
                "node_path": record.get("node_path", []),
                "depth": record.get("depth", len(record.get("node_path", []))),
                "score": 0.58,
                "dense_score": 0.0,
                "sparse_score": 0.0,
                "embedding_type": "secondary_index_hint",
            }
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_embedding_index_scan",
                budget_source=budget_source,
            )

        top_k_per_layer = integer_arg(ranking, "top_k_per_layer", DEFAULT_TOP_K_PER_LAYER, minimum=1)
        max_children_scored_per_parent = bounded_max_children_scored_per_parent(
            integer_arg(
                ranking,
                "max_children_scored_per_parent",
                DEFAULT_MAX_CHILDREN_SCORED_PER_PARENT,
                minimum=1,
            )
        )
        hard_max_children_scored_per_parent = max(1, HARD_MAX_CHILDREN_SCORED_PER_PARENT)
        max_candidates_per_node = integer_arg(ranking, "max_candidates_per_node", DEFAULT_MAX_CANDIDATES_PER_NODE, minimum=1)
        max_selected_refs = integer_arg(ranking, "max_selected_refs", DEFAULT_MAX_SELECTED_REFS, minimum=1)
        max_global_candidates = integer_arg(ranking, "max_global_candidates", DEFAULT_MAX_GLOBAL_CANDIDATES, minimum=1)
        min_similarity_score = float_arg(ranking, "min_similarity_score", DEFAULT_RETRIEVAL_MIN_SCORE, minimum=0.0, maximum=1.0)
        budget_fill_policy = str(ranking.get("budget_fill_policy", DEFAULT_BUDGET_FILL_POLICY) or DEFAULT_BUDGET_FILL_POLICY).strip().lower()
        if budget_fill_policy not in {"quality_first", "force_fill"}:
            raise MatrixArkError("budget_fill_policy must be quality_first or force_fill")
        max_raw_events_per_node = integer_arg(ranking, "max_raw_events_per_node", TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE, minimum=1)
        traversal = tree_first_traversal(
            node_scores,
            top_k_per_layer=top_k_per_layer,
            max_children_scored_per_parent=max_children_scored_per_parent,
        )
        finish_retrieval_stage("node_traversal", stage_started_perf)
        stage_started_perf = time.perf_counter()
        selected_paths = traversal["selected_paths"]
        selected_leaf_paths = traversal["leaf_paths"]
        selected_node_hashes = traversal["selected_node_hashes"]

        placement_record_result: Json = {}
        placement_candidate_records: list[Json] = []
        if selected_node_hashes and not traversal.get("fallback_to_flat"):
            placement_record_result = self.retrieval_records(
                scope=scope,
                secondary_index_groups=secondary_index_filter_groups,
                selected_node_hashes=selected_node_hashes,
                allow_broad_scan_fallback=False,
            )
            placement_candidate_records = placement_record_result.get("records", [])

            def record_identity(record: Json) -> tuple[str, Any]:
                record_type = str(record.get("record_type") or "")
                for field in (
                    "event_id_hash",
                    "entity_hash",
                    "segment_hash",
                    "compression_id_hash",
                    "summary_hash",
                    "chunk_hash",
                    "section_hash",
                    "skill_hash",
                    "resource_hash",
                    "batch_id_hash",
                ):
                    if record.get(field) is not None:
                        return (record_type, record.get(field))
                if record_type == "context_index":
                    return (
                        record_type,
                        (
                            record.get("index_name"),
                            record.get("node_hash"),
                            tuple(context_index_ref_hashes(record)),
                            record.get("timestamp_key_ms"),
                        ),
                    )
                return (record_type, stable_hash(json.dumps(record, sort_keys=True, separators=(",", ":"))))

            seen_record_identities = {record_identity(record) for record in records}
            for record in placement_candidate_records:
                identity = record_identity(record)
                if identity in seen_record_identities:
                    continue
                records.append(record)
                seen_record_identities.add(identity)

            for record in placement_candidate_records:
                record_type = record.get("record_type")
                if record_type == "context_index" and scope_matches(candidate_access_scope(record), scope):
                    index_name = str(record.get("index_name", ""))
                    if index_name:
                        ref_hashes = context_index_ref_hashes(record)
                        if record.get("batch_id_hash") is not None:
                            index_terms_by_batch.setdefault(record.get("batch_id_hash"), []).append(index_name)
                        node_hash_for_index = record.get("node_hash")
                        try:
                            index_terms_by_node_for_prefilter.setdefault(int(node_hash_for_index), []).append(index_name)
                        except (TypeError, ValueError):
                            pass
                        if ref_hashes:
                            for ref_hash in ref_hashes:
                                index_terms_by_ref.setdefault(ref_hash, []).append(index_name)
                        else:
                            ref_hash = record.get("ref_hash") or record.get("chunk_hash") or record.get("section_hash") or record.get("skill_hash")
                            if ref_hash is not None:
                                index_terms_by_ref.setdefault(ref_hash, []).append(index_name)
                            else:
                                index_terms_by_node.setdefault(record.get("node_hash"), []).append(index_name)
                elif record_type == "context_embedding" and scope_matches(candidate_access_scope(record), scope):
                    embedding_type = record.get("embedding_type")
                    if embedding_type == "event_text":
                        event_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
                    elif embedding_type == "entity_state":
                        entity_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
                    elif embedding_type == "segment_text":
                        segment_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
                    elif embedding_type == "compression_summary":
                        compression_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
                    elif embedding_type == "resource_chunk":
                        resource_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
                    elif embedding_type == "skill_section":
                        resource_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
                    elif embedding_type == "skill_summary":
                        skill_embedding_vectors[record["ref_hash"]] = record.get("vector", [])

        def selected_by_tree(record: Json) -> bool:
            if traversal.get("fallback_to_flat"):
                return True
            path = node_path_tuple(record.get("node_path", []))
            if path and path in selected_paths:
                return True
            if path and any(
                starts_with_path(path, leaf_path) or starts_with_path(leaf_path, path)
                for leaf_path in selected_leaf_paths
            ):
                return True
            try:
                return int(record.get("node_hash")) in selected_node_hashes
            except (TypeError, ValueError):
                return False

        if placement_candidate_records and not traversal.get("fallback_to_flat"):
            tree_candidate_records = [record for record in placement_candidate_records if selected_by_tree(record)]
            tree_prefilter_dropped_count = max(0, len(placement_candidate_records) - len(tree_candidate_records))
            retrieval_scan_stats = {
                **retrieval_scan_stats,
                "leaf_fetch": placement_record_result.get("scan_stats", {}),
                "leaf_fetch_record_count": len(placement_candidate_records),
                "leaf_fetch_strategy": "selected_node_placement",
            }
        else:
            tree_candidate_records = records if traversal.get("fallback_to_flat") else [record for record in records if selected_by_tree(record)]
            tree_prefilter_dropped_count = 0 if traversal.get("fallback_to_flat") else max(0, len(records) - len(tree_candidate_records))
        raw_event_ids_by_node: dict[Any, set[int]] = {}
        raw_event_time_window_dropped_count = 0
        events_by_node: dict[Any, list[Json]] = {}
        nodes_with_compression: set[Any] = set()
        for scan_index, record in enumerate(tree_candidate_records, 1):
            if scan_index % 128 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_tree_candidate_prefilter", records)
            if record.get("record_type") == "context_compression_event":
                node_key_for_compression: Any = record.get("node_hash")
                if node_key_for_compression is None:
                    node_key_for_compression = tuple(record.get("node_path", []))
                nodes_with_compression.add(node_key_for_compression)
                continue
            if record.get("record_type") != "context_event":
                continue
            if record.get("source_chunk_hash"):
                continue
            node_key: Any = record.get("node_hash")
            if node_key is None:
                node_key = tuple(record.get("node_path", []))
            events_by_node.setdefault(node_key, []).append(record)
        for node_key, node_events in events_by_node.items():
            if node_key not in nodes_with_compression:
                continue
            node_events.sort(
                key=lambda item: (
                    self.context_event_ingestion_time_ms(item),
                    int(item.get("event_id_hash") or 0),
                ),
                reverse=True,
            )
            admitted = {
                int(record.get("event_id_hash"))
                for record in node_events[:max_raw_events_per_node]
                if record.get("event_id_hash") is not None
            }
            raw_event_ids_by_node[node_key] = admitted
            raw_event_time_window_dropped_count += max(0, len(node_events) - len(admitted))
        candidate_count_by_node: dict[Any, int] = {}
        fanout_dropped_count = 0

        def admit_candidate_for_node(record: Json) -> bool:
            nonlocal fanout_dropped_count
            node_key: Any = record.get("node_hash")
            if node_key is None:
                node_key = tuple(record.get("node_path", []))
            count = candidate_count_by_node.get(node_key, 0)
            if count >= max_candidates_per_node:
                fanout_dropped_count += 1
                return False
            candidate_count_by_node[node_key] = count + 1
            return True

        layer_scores = sorted(
            traversal["trace"] or node_scores.values(),
            key=lambda item: (item.get("depth", 0), -float(item.get("score", 0.0)), item.get("node_hash", 0)),
        )
        primary_matches = []
        auxiliary_matches = []
        if question_type == "broad_exploration":
            for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
                if scan_index % 64 == 0 and deadline_exceeded():
                    return deadline_fallback("deadline_during_summary_scan", records)
                if record.get("record_type") != "context_summary":
                    continue
                if not access_scope_matches_before_scoring(record, retrieval_scope):
                    continue
                if not selected_by_tree(record):
                    continue
                summary_type = str(record.get("summary_type") or "")
                if summary_type not in {"node_l0", "node_l1", "resource_l0", "batch_l0", "session_l0"}:
                    continue
                index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
                if not passes_applicable_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                    secondary_index_dropped_count += 1
                    continue
                secondary_index_matched_count += 1
                if not admit_candidate_for_node(record):
                    continue
                text = str(record.get("summary_text", ""))
                if not text:
                    continue
                sparse_score = sparse_lexical_score(query_terms, text)
                keyword_score = len(query_terms.intersection(tokens(text)))
                embedding_score = cosine(query_embedding, embedding_for_text(" ".join(record.get("node_path", []) + [summary_type, text])))
                node_score = node_scores.get(record.get("node_hash"), {}).get("score", 0.0)
                origin_score = min(1.0, 0.06 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
                if origin_score <= 0:
                    continue
                primary_matches.append(
                    score_recall_candidate(
                        annotate_session_continuity({
                            "ref_type": "summary",
                            "ref_hash": record.get("summary_hash") or record.get("node_hash"),
                            "node_hash": record.get("node_hash"),
                            "node_path": record.get("node_path", []),
                            "origin_score": origin_score,
                            "keyword_score": keyword_score,
                            "sparse_score": sparse_score,
                            "embedding_score": embedding_score,
                            "node_score": node_score,
                            "matched_index_terms": sorted(index_terms),
                            "selection_reason": "selected by tree path and L0/L1 summary relevance",
                            "event_type": summary_type,
                            "context_class": "summary",
                            "summary_type": summary_type,
                            "access_decision": "allowed_by_registry_scope_before_scoring",
                            "access_scope": candidate_access_scope(record),
                            "scope": candidate_access_scope(record),
                            "updated_at_ms": record.get("updated_at_ms", now_ms()),
                            "text": clip_context_text(text),
                            "recall_path": "primary_summary",
                        }, record),
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
            if scan_index % 64 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_event_scan", records)
            if record.get("record_type") != "context_event":
                continue
            event_node_key: Any = record.get("node_hash")
            if event_node_key is None:
                event_node_key = tuple(record.get("node_path", []))
            if (
                not record.get("source_chunk_hash")
                and event_node_key in raw_event_ids_by_node
                and int(record.get("event_id_hash") or 0) not in raw_event_ids_by_node[event_node_key]
            ):
                continue
            envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else {}
            record_scope = candidate_access_scope(record)
            if not access_scope_matches_before_scoring(record, retrieval_scope):
                continue
            if not selected_by_tree(record):
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
            if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            if not admit_candidate_for_node(record):
                continue
            text = str(record.get("text", ""))
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            embedding_score = cosine(query_embedding, event_embedding_vectors.get(record["event_id_hash"], []))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            origin_score = hybrid_origin_score(query_terms, text, embedding_score, node_score)
            event_type = str(record.get("event_type") or record.get("classification") or "")
            candidate_metadata: Json = {}
            record_metadata = record.get("metadata")
            envelope_metadata = envelope.get("metadata")
            if isinstance(record_metadata, dict):
                candidate_metadata.update(record_metadata)
            if isinstance(envelope_metadata, dict):
                candidate_metadata.update(envelope_metadata)
            candidate = {
                "ref_type": "event",
                "ref_hash": record["event_id_hash"],
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "matched_index_terms": sorted(index_terms),
                "selection_reason": (
                    "selected by tree path, secondary indexes, and resource fact/event hybrid score"
                    if record.get("source_chunk_hash")
                    else "selected by tree path, secondary indexes, and event hybrid score"
                ),
                "event_type": event_type,
                "context_class": "resource_fact" if record.get("source_chunk_hash") else "event",
                "source_chunk_hash": record.get("source_chunk_hash"),
                "source_ref": record.get("source_ref", ""),
                "metadata": candidate_metadata,
                "scope": record_scope,
                "updated_at_ms": record.get("updated_at_ms") or envelope.get("ingestion_time_ms", now_ms()),
                "text": clip_context_text(text),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate(annotate_session_continuity({**candidate, "recall_path": "primary_hybrid"}, record), ranking, reference_time_ms=reference_time_ms))
            graph_text = " ".join(record.get("node_path", []) + sorted(index_terms) + [event_type, text])
            graph_score = sparse_lexical_score(query_terms, graph_text)
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **annotate_session_continuity(candidate, record),
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_event_scan",
                budget_source=budget_source,
            )
        for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
            if scan_index % 64 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_entity_scan", records)
            if record.get("record_type") != "context_entity":
                continue
            if not access_scope_matches_before_scoring(record, retrieval_scope):
                continue
            if not selected_by_tree(record):
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
            if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            if not admit_candidate_for_node(record):
                continue
            text = f"{record.get('entity_type', '')}: {record.get('entity_name', '')} = {record.get('state', '')}"
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            embedding_score = cosine(query_embedding, entity_embedding_vectors.get(record["entity_hash"], []))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            origin_score = min(1.0, 0.12 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
            candidate = {
                "ref_type": "entity",
                "ref_hash": record["entity_hash"],
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "matched_index_terms": sorted(index_terms),
                "selection_reason": (
                    "selected by tree path, secondary indexes, and resource entity state score"
                    if record.get("source_chunk_hash")
                    else "selected by tree path, secondary indexes, and entity state score"
                ),
                "entity_type": record.get("entity_type", ""),
                "entity_name": record.get("entity_name", ""),
                "context_class": "resource_entity_fact" if record.get("source_chunk_hash") else "entity",
                "source_chunk_hash": record.get("source_chunk_hash"),
                "source_ref": record.get("source_ref", ""),
                "metadata": record.get("metadata", {}),
                "scope": candidate_access_scope(record),
                "updated_at_ms": record.get("updated_at_ms", now_ms()),
                "text": clip_context_text(text),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate(annotate_session_continuity({**candidate, "recall_path": "primary_hybrid"}, record), ranking, reference_time_ms=reference_time_ms))
            graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + sorted(index_terms) + [text]))
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **annotate_session_continuity(candidate, record),
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_entity_scan",
                budget_source=budget_source,
            )
        for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
            if scan_index % 64 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_segment_scan", records)
            if record.get("record_type") != "context_segment":
                continue
            if not access_scope_matches_before_scoring(record, retrieval_scope):
                continue
            if not selected_by_tree(record):
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
            if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            if not admit_candidate_for_node(record):
                continue
            text = f"{record.get('topic', '')}: {record.get('summary_text', '')}"
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            embedding_score = cosine(query_embedding, segment_embedding_vectors.get(record["segment_hash"], []))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            saliency_score = float(record.get("saliency_score", 0.0))
            origin_score = min(
                1.0,
                0.1 + 0.75 * hybrid_origin_score(query_terms, text, embedding_score, node_score) + 0.15 * saliency_score,
            )
            candidate = {
                "ref_type": "segment",
                "ref_hash": record["segment_hash"],
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "matched_index_terms": sorted(index_terms),
                "selection_reason": "selected by tree path, secondary indexes, segment saliency, and segment hybrid score",
                "saliency_score": saliency_score,
                "topic": record.get("topic", ""),
                "coordinate_tuples": record.get("coordinate_tuples", []),
                "non_contiguous": record.get("non_contiguous", False),
                "scope": candidate_access_scope(record),
                "updated_at_ms": record.get("updated_at_ms", now_ms()),
                "text": clip_context_text(str(record.get("summary_text", ""))),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate(annotate_session_continuity({**candidate, "recall_path": "primary_hybrid"}, record), ranking, reference_time_ms=reference_time_ms))
            graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + sorted(index_terms) + [record.get("topic", ""), text]))
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **annotate_session_continuity(candidate, record),
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_segment_scan",
                budget_source=budget_source,
            )
        for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
            if scan_index % 64 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_resource_skill_scan", records)
            if record.get("record_type") not in {"resource_chunk", "skill_section"}:
                continue
            if not access_scope_matches_before_scoring(record, retrieval_scope):
                continue
            if not selected_by_tree(record):
                continue
            if record.get("record_type") == "resource_chunk" and record.get("resource_type") == "skill":
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
            if not passes_applicable_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            if not admit_candidate_for_node(record):
                continue
            if record.get("record_type") == "skill_section":
                ref_type = "skill_section"
                ref_hash = int(record.get("section_hash") or 0)
                parent_skill_hash = int(record.get("skill_hash") or 0)
                control = skill_controls.get(parent_skill_hash, {})
                if str(control.get("status") or "active") != "active":
                    continue
                resource_hash = parent_skill_hash
                raw_uri_value = str(record.get("raw_uri") or "")
                source_locator = str(record.get("source_locator") or "")
                citation = str(record.get("source_ref") or source_ref_from_locator(raw_uri_value, source_locator))
                resource_version_value = str(record.get("metadata", {}).get("resource_version") or record.get("resource_version") or "")
                version_state = "current"
                is_superseded_version = False
                text = f"skill section {record.get('heading', '')}: {record.get('text', '')}"
                embedding_score = cosine(query_embedding, resource_embedding_vectors.get(ref_hash, embedding_for_text(text)))
                business_type = "skill"
                metadata = {**record.get("metadata", {}), "skill_registry": control}
            else:
                ref_type = "resource_chunk"
                ref_hash = int(record.get("chunk_hash") or 0)
                metadata = record.get("metadata", {})
                resource_hash = int(record.get("resource_hash") or 0)
                raw_uri_value = str(record.get("raw_uri") or resource_uri_by_hash.get(resource_hash, ""))
                source_locator = str(record.get("source_locator") or metadata.get("source_locator") or "")
                citation = str(record.get("source_ref") or source_ref_from_locator(raw_uri_value, source_locator))
                resource_version_value = str(metadata.get("resource_version") or record.get("resource_version") or "")
                latest_version = latest_resource_version_by_hash.get(resource_hash, resource_version_value)
                is_superseded_version = bool(
                    resource_version_value
                    and latest_version
                    and resource_version_value != latest_version
                )
                if is_superseded_version and not include_superseded_resources:
                    secondary_index_dropped_count += 1
                    continue
                version_state = "historical" if is_superseded_version else "current"
                text = f"resource {source_locator}: {record.get('text', '')}"
                embedding_score = cosine(query_embedding, resource_embedding_vectors.get(ref_hash, embedding_for_text(text)))
                business_type = str(record.get("resource_type") or "resource")
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            node_score = node_scores.get(record.get("node_hash"), {}).get("score", 0.0)
            origin_score = min(1.0, 0.08 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
            if origin_score <= 0:
                continue
            primary_matches.append(
                score_recall_candidate(
                    annotate_session_continuity({
                        "ref_type": ref_type,
                        "ref_hash": ref_hash,
                        "node_hash": record.get("node_hash"),
                        "node_path": record.get("node_path", []),
                        "origin_score": origin_score,
                        "keyword_score": keyword_score,
                        "sparse_score": sparse_score,
                        "embedding_score": embedding_score,
                        "node_score": node_score,
                        "matched_index_terms": sorted(index_terms),
                        "selection_reason": (
                            "selected by tree path, secondary indexes, and resource/skill hybrid score"
                            if index_terms
                            else "selected by tree path and resource/skill hybrid score"
                        ),
                        "event_type": business_type,
                        "context_class": ref_type,
                        "resource_hash": resource_hash,
                        "source_locator": source_locator,
                        "resource_type": record.get("resource_type", ""),
                        "resource_version": resource_version_value,
                        "supersedes_chunk_hash": metadata.get("supersedes_chunk_hash"),
                        "version_state": version_state,
                        "stale_or_superseded": is_superseded_version,
                        "access_decision": "allowed_by_registry_scope_before_scoring",
                        "access_scope": candidate_access_scope(record),
                        "deployment_scope": record.get("deployment_scope", "local"),
                        "citation": citation,
                        "metadata": metadata,
                        "scope": candidate_access_scope(record),
                        "updated_at_ms": record.get("updated_at_ms", now_ms()),
                        "text": clip_context_text(text),
                        "recall_path": "primary_resource_skill",
                    }, record),
                    ranking,
                    reference_time_ms=reference_time_ms,
                )
            )

        for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
            if scan_index % 64 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_compression_scan", records)
            if record.get("record_type") != "context_compression_event":
                continue
            if not access_scope_matches_before_scoring(record, retrieval_scope):
                continue
            if not selected_by_tree(record):
                continue
            if not admit_candidate_for_node(record):
                continue
            text = f"TIME_COMPRESS: {summarize_text(str(record.get('summary_text', '')), limit=96)}"
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            compression_hash = int(record.get("compression_id_hash") or 0)
            embedding_score = cosine(query_embedding, compression_embedding_vectors.get(compression_hash, embedding_for_text(text)))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            origin_score = min(1.0, 0.08 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
            candidate = {
                "ref_type": "compression",
                "ref_hash": compression_hash,
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "event_type": "time_compress",
                "operator": "TIME_COMPRESS",
                "source_event_ids": record.get("source_event_ids", []),
                "source_start_ms": record.get("source_start_ms"),
                "source_end_ms": record.get("source_end_ms"),
                "scope": candidate_access_scope(record),
                "updated_at_ms": record.get("compressed_time_ms", record.get("updated_at_ms", now_ms())),
                "text": clip_context_text(text),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate(annotate_session_continuity({**candidate, "recall_path": "primary_time_compression"}, record), ranking, reference_time_ms=reference_time_ms))
            graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + [text, "time_compress"]))
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **annotate_session_continuity(candidate, record),
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_compression_scan",
                budget_source=budget_source,
            )
        finish_retrieval_stage("rerank_score", stage_started_perf)
        stage_started_perf = time.perf_counter()
        primary_matches.sort(key=lambda item: item["score"], reverse=True)
        auxiliary_matches.sort(key=lambda item: item["score"], reverse=True)
        selected_ref_cap = max(1, int(max_selected_refs or DEFAULT_MAX_SELECTED_REFS))
        rerank_candidate_limit = max(selected_ref_cap, max_global_candidates)
        first_stage_candidate_count = len(primary_matches) + len(auxiliary_matches)
        rerank_policy = {
            "enabled": True,
            "stage": "packing_rerank",
            "mode": "question_type_token_efficiency",
            "input_candidate_count": first_stage_candidate_count,
            "max_candidates": rerank_candidate_limit,
            "reranked_candidate_count": min(first_stage_candidate_count, rerank_candidate_limit),
            "question_type": question_type,
            "signals": [
                "weighted_recall_score",
                "question_type_ref_boost",
                "cross_session_rerank_boost",
                "token_efficiency",
                "multi_hop_node_diversity",
            ],
            "cross_session_rerank_enabled": True,
            "cross_session_signals": ["entity_state", "resource_fact_citation", "answer_event", "compression", "summary_demotion"],
            "fallback": "weighted_recall",
            "heavy_rerank_enabled": False,
            "min_similarity_score": min_similarity_score,
            "budget_fill_policy": budget_fill_policy,
        }
        selected, used_context_tokens, dropped_over_budget = select_token_budgeted_refs(
            primary_matches,
            auxiliary_matches,
            max_context_tokens=remote_context_budget_tokens,
            auxiliary_quota=auxiliary_quota,
            question_type=question_type,
            reserved_tokens=0,
            max_selected_refs=max_selected_refs,
            min_score=min_similarity_score,
            max_global_candidates=max_global_candidates,
            budget_fill_policy=budget_fill_policy,
            duplicate_text_hashes=local_budget["text_hashes"],
            deadline_exceeded=deadline_exceeded,
            deadline_reason="deadline_during_context_pack",
            cross_session_policy=cross_session_policy,
            shared_context_policy=shared_context_policy,
        )
        partial_context_pack = bool(dropped_over_budget.get("deadline_exceeded"))
        quality_warnings = []
        if partial_context_pack:
            quality_warnings.append(f"retrieval_deadline_exceeded:{dropped_over_budget.get('deadline_reason', 'deadline_during_context_pack')}")
        context_pack_id = stable_hash(f"{query}:{selected}:{now_ms()}")
        context_pack_id_text = str(context_pack_id)
        recall_reinforcement_enabled = bool(ranking.get("recall_reinforcement", True))
        if recall_reinforcement_enabled:
            reinforcement = self.append_recall_reinforcement_markers(
                context_pack_id=context_pack_id_text,
                selected_refs=selected,
                reinforced_at_ms=now_ms(),
            )
        else:
            reinforcement = {
                "reinforced_event_count": 0,
                "protect_ms": 0,
                "protected_until_ms": 0,
                "skipped": True,
                "reason": "disabled_for_read_only_scale_or_benchmark_run",
            }
        debug_refs = bool(args.get("include_debug_refs") or ranking.get("include_debug_refs") or CONTEXT_PACK_DEBUG_REFS)
        serving_selected = compact_context_pack_refs(selected, include_debug=debug_refs)
        serving_dropped = compact_dropped_refs_for_context_pack(dropped_over_budget, include_debug=debug_refs)
        pack_summary = summarize_text(
            " ".join(str(item.get("text", "")) for item in selected),
            limit=512,
        )
        selected_context_counts = selected_context_class_counts(selected)
        freshness_tolerance_ms = int(ranking.get("freshness_tolerance_ms", DEFAULT_TIME_DECAY_TOLERANCE_MS))
        half_life_ms = int(ranking.get("half_life_ms", DEFAULT_TIME_DECAY_HALFLIFE_MS))
        selected_time_scores = [float(item.get("time_score", 0.0)) for item in selected if "time_score" in item]
        selected_age_ms: list[int] = []
        for item in selected:
            try:
                selected_age_ms.append(max(0, int(reference_time_ms) - int(item.get("updated_at_ms") or reference_time_ms)))
            except (TypeError, ValueError):
                continue
        time_weighted_recall = {
            "enabled": True,
            "role": "ranking_prior_not_temporal_compression",
            "score_field": "time_score",
            "formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
            "freshness_tolerance_ms": freshness_tolerance_ms,
            "half_life_ms": half_life_ms,
            "selected_ref_count": len(selected),
            "avg_selected_time_score": round(sum(selected_time_scores) / len(selected_time_scores), 6) if selected_time_scores else 0.0,
            "min_selected_time_score": round(min(selected_time_scores), 6) if selected_time_scores else 0.0,
            "max_selected_age_ms": max(selected_age_ms) if selected_age_ms else 0,
            "recent_selected_ref_count": sum(1 for age_ms in selected_age_ms if age_ms <= freshness_tolerance_ms),
            "older_selected_ref_count": sum(1 for age_ms in selected_age_ms if age_ms > freshness_tolerance_ms),
        }
        pack = {
            "context_pack_id": str(context_pack_id),
            "context_sources_order": ["local_context", "matrixark_remote_context"],
            "local_context_refs": local_context_refs_for_pack(local_budget),
            "selected_refs": serving_selected,
            "remote_context_refs": serving_selected,
            "selected_ref_counts": selected_context_counts,
            "context_assembly_policy": {
                "access_scope_before_scoring": True,
                "skill_selection": "skill_section_only",
                "resource_selection": "resource_facts_entities_and_chunks_are_ranked_separately",
                "recall_reinforcement": "selected event refs and compression source ids receive protection markers before raw-event pruning",
            },
            "layer_scores": layer_scores[:24],
            "question_type": question_type,
            "packing_policy": f"question_type_aware:{question_type}",
            "query_embedding_model": embedding_model_name(),
            "embedding_execution_mode": embedding_execution_mode_name(),
            "embedding_fallback_used": embedding_fallback_used(),
            "recall_policy": {
                "query_plan": query_plan,
                "session_continuity": {
                    "mode": retrieval_session_scope,
                    "policy": "same-session continuity first; entity state bridges cross-session memory; cross-session evidence remains eligible under account/tenant/user scope",
                    "same_session_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "same_session"),
                    "cross_session_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "cross_session"),
                    "entity_bridge_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "cross_session" and item.get("ref_type") == "entity"),
                },
                "cross_session": dropped_over_budget.get("cross_session_policy", cross_session_policy),
                "shared_context": dropped_over_budget.get("shared_context_policy", shared_context_policy),
                "backend_retrieval_pushdown": retrieval_scan_stats,
                "ranking": {
                    "min_similarity_score": min_similarity_score,
                    "max_global_candidates": max_global_candidates,
                    "max_selected_refs": max_selected_refs,
                    "budget_fill_policy": budget_fill_policy,
                    "quality_first_budget_underfill_allowed": budget_fill_policy == "quality_first",
                },
                "tree_traversal": {
                    "enabled": True,
                    "summary_embeddings": ["node_l0", "node_l1"],
                    "top_k_per_layer": top_k_per_layer,
                    "max_children_scored_per_parent": max_children_scored_per_parent,
                    "hard_max_children_scored_per_parent": hard_max_children_scored_per_parent,
                    "children_scoring_policy": "score_all_children_up_to_hard_cap_then_split_node_layers",
                    "max_candidates_per_node": max_candidates_per_node,
                    "max_raw_events_per_node": max_raw_events_per_node,
                    "max_selected_refs": max_selected_refs,
                    "selected_node_count": len(selected_node_hashes),
                    "selected_path_count": len(selected_paths),
                    "selected_leaf_count": len(traversal.get("leaf_paths", [])),
                    "candidate_records_after_tree": len(tree_candidate_records),
                    "records_dropped_by_tree": tree_prefilter_dropped_count,
                    "records_dropped_by_node_fanout": fanout_dropped_count,
                    "raw_events_dropped_by_time_window": raw_event_time_window_dropped_count,
                    "cold_events_represented_by_compression": raw_event_time_window_dropped_count > 0,
                    "leaf_record_fetch_policy": "events/entities/resources/skills/compressions scanned only inside selected L0/L1 folders",
                    "fallback_to_flat": bool(traversal.get("fallback_to_flat")),
                    "fallback_reason": "missing_or_stale_summary_embeddings" if traversal.get("fallback_to_flat") else "",
                },
                "secondary_index_filter": {
                    "enabled": bool(secondary_index_filter_groups),
                    "required_groups": [sorted(group) for group in secondary_index_filter_groups],
                    "matched_candidate_count": secondary_index_matched_count,
                    "dropped_candidate_count": secondary_index_dropped_count,
                    "mode": "ANY group for multi-intent raw query, otherwise AND across groups; OR within each group",
                    "effective_mode": secondary_index_filter_mode,
                    "applied_before_embedding_scoring": True,
                    "fanout_cap_applied_before_embedding_scoring": True,
                },
                "rerank": rerank_policy,
                "primary_path": "tree-first hybrid dense semantic + sparse lexical after secondary-index prefilter",
                "auxiliary_path": "keyword graph inside selected tree after secondary-index prefilter",
                "time_decay": {
                    "freshness_tolerance_ms": freshness_tolerance_ms,
                    "half_life_ms": half_life_ms,
                },
                "time_weighted_recall": time_weighted_recall,
                "recall_reinforcement": reinforcement,
                "weights": {
                    "time": optional_object(ranking, "weights").get("time", DEFAULT_TIME_WEIGHT),
                    "business": optional_object(ranking, "weights").get("business", DEFAULT_BUSINESS_WEIGHT),
                },
                "auxiliary_quota": auxiliary_quota,
                "storage_options": storage_options,
                "hard_deadline": {
                    "deadline_ms": deadline_ms,
                    "elapsed_ms": round((time.perf_counter() - started_perf) * 1000.0, 3),
                    "partial_context_pack": partial_context_pack,
                    "fallback_reason": dropped_over_budget.get("deadline_reason", "") if partial_context_pack else "",
                },
            },
            "primary_candidate_count": len(primary_matches),
            "auxiliary_candidate_count": len(auxiliary_matches),
            "used_context_tokens": used_context_tokens,
            "used_remote_context_tokens": used_context_tokens,
            "used_local_context_tokens": local_tokens,
            "total_prompt_context_tokens": used_context_tokens + local_tokens,
            "remote_context_budget_tokens": remote_context_budget_tokens,
            "requested_max_context_tokens": max_context_tokens,
            "local_context_safety_margin_tokens": safety_margin_tokens,
            "budget_source": budget_source,
            "local_context_policy": {
                "mode": "shared_budget_dedupe",
                "local_context_count": len(local_budget["items"]),
                "local_context_tokens": local_tokens,
                "local_context_token_source": local_budget.get("token_source", "estimated_from_local_context"),
                "safety_margin_tokens": safety_margin_tokens,
                "safety_margin_source": local_budget.get("safety_margin_source", "matrixark_default_5_percent_capped"),
                "dedupe_remote_against_local": True,
                "remote_is_additive_only_within_remaining_budget": True,
            },
            "dropped_refs": serving_dropped,
            "quality_warnings": quality_warnings,
            "insufficient_context": not selected,
            "partial_context_pack": partial_context_pack,
            "context_pack_payload_policy": {
                "serving_refs": "compact" if not debug_refs else "debug_full",
                "hashes_and_matched_indexes": "audit_only" if not debug_refs else "included",
                "dropped_ref_details": "audit_only" if not debug_refs else "included",
                "enable_debug_refs_with": "include_debug_refs=true or MATRIXARK_CONTEXT_PACK_DEBUG_REFS=1",
            },
            "operational_visibility_policy": {
                "audit_mode": audit_mode,
                "audit_sample_rate": audit_sample_rate,
                "telemetry_record": audit_mode != "off",
                "rich_replay_audit": audit_mode == "full" and audit_sample_rate > 0,
                "rich_replay_audit_force_on_partial_or_warning": True,
            },
        }
        finish_retrieval_stage("pack", stage_started_perf)
        pack["recall_policy"]["stage_latency_budgets"] = stage_budget_snapshot()
        over_budget_stages = pack["recall_policy"]["stage_latency_budgets"].get("over_budget_stages", [])
        if over_budget_stages:
            quality_warnings.append("stage_budget_exceeded:" + ",".join(over_budget_stages))
            pack["quality_warnings"] = quality_warnings
        audit_started_perf = time.perf_counter()
        audit_record = {
            "record_type": "context_pack_audit",
            "context_pack_id": context_pack_id_text,
            "query": query,
            "scope": scope,
            "summary_text": pack_summary,
            "selected_refs": compact_refs_for_audit(selected),
            "local_context_refs": compact_local_context_refs(local_budget),
            "context_sources_order": pack["context_sources_order"],
            "selected_ref_counts": selected_context_counts,
            "context_assembly_policy": pack["context_assembly_policy"],
            "dropped_refs": dropped_over_budget,
            "quality_warnings": quality_warnings,
            "partial_context_pack": partial_context_pack,
            "layer_scores": layer_scores[:24],
            "tree_traversal": pack["recall_policy"]["tree_traversal"],
            "secondary_index_filter": pack["recall_policy"]["secondary_index_filter"],
            "question_type": question_type,
            "packing_policy": pack["packing_policy"],
            "rerank_policy": rerank_policy,
            "recall_policy": pack["recall_policy"],
            "stage_latency_budgets": pack["recall_policy"]["stage_latency_budgets"],
            "storage_options": storage_options,
            "local_context_policy": pack["local_context_policy"],
            "used_local_context_tokens": pack["used_local_context_tokens"],
            "used_remote_context_tokens": pack["used_remote_context_tokens"],
            "total_prompt_context_tokens": pack["total_prompt_context_tokens"],
            "remote_context_budget_tokens": pack["remote_context_budget_tokens"],
            "requested_max_context_tokens": pack["requested_max_context_tokens"],
            "local_context_safety_margin_tokens": pack["local_context_safety_margin_tokens"],
            "budget_source": pack["budget_source"],
            "operational_visibility_policy": pack["operational_visibility_policy"],
            "primary_candidate_count": len(primary_matches),
            "auxiliary_candidate_count": len(auxiliary_matches),
            "tree_candidate_records": len(tree_candidate_records),
            "tree_prefilter_dropped_count": tree_prefilter_dropped_count,
            "fanout_dropped_count": fanout_dropped_count,
            "max_candidates_per_node": max_candidates_per_node,
            "max_selected_refs": max_selected_refs,
            "created_at_ms": now_ms(),
        }
        visibility_decision = self.append_context_pack_visibility(
            pack=pack,
            audit_record=audit_record,
            query=query,
            scope=scope,
            audit_mode=audit_mode,
            audit_sample_rate=audit_sample_rate,
        )
        pack["operational_visibility_policy"] = visibility_decision
        if pack_cache_enabled and not pack.get("partial_context_pack"):
            cached_pack = json.loads(json.dumps(pack))
            cached_recall = cached_pack.get("recall_policy") if isinstance(cached_pack.get("recall_policy"), dict) else {}
            cached_recall["context_pack_cache"] = {"hit": False, "ttl_s": self._context_pack_cache_ttl_s}
            cached_pack["recall_policy"] = cached_recall
            with self._context_pack_cache_lock:
                if len(self._context_pack_cache) >= self._context_pack_cache_max_entries:
                    oldest_key = next(iter(self._context_pack_cache))
                    self._context_pack_cache.pop(oldest_key, None)
                self._context_pack_cache[pack_cache_key] = (time.monotonic(), cached_pack)
        finish_retrieval_stage("audit", audit_started_perf)
        placement = retrieval_scan_stats.get("native_selected_node_locations", {}) if isinstance(retrieval_scan_stats, dict) else {}
        candidate_cache_hit = bool(
            isinstance(retrieval_scan_stats, dict)
            and (
                retrieval_scan_stats.get("cache_hit")
                or retrieval_scan_stats.get("candidate_cache_hit")
                or retrieval_scan_stats.get("native_placement_candidate_cache_hit")
            )
        )
        index_postings_read = (
            int(retrieval_scan_stats.get("index_postings_read") or 0)
            if isinstance(retrieval_scan_stats, dict)
            else 0
        )
        if isinstance(retrieval_scan_stats, dict) and not index_postings_read:
            index_postings_read = int(
                retrieval_scan_stats.get("index_postings_touched")
                or retrieval_scan_stats.get("native_index_postings_found")
                or 0
            )
        pack["retrieval_metrics"] = {
            "query_plan_ms": round(float(stage_latencies_ms.get("query_understanding", 0.0)), 3),
            "node_traversal_ms": round(float(stage_latencies_ms.get("node_traversal", 0.0)), 3),
            "index_prefilter_ms": round(float(stage_latencies_ms.get("candidate_fetch", 0.0)), 3),
            "candidate_fetch_ms": round(float(stage_latencies_ms.get("candidate_fetch", 0.0)), 3),
            "score_ms": round(float(stage_latencies_ms.get("rerank_score", 0.0)), 3),
            "pack_ms": round(float(stage_latencies_ms.get("pack", 0.0)), 3),
            "audit_ms": round(float(stage_latencies_ms.get("audit", 0.0)), 3),
            "append_queue_wait_ms": 0.0,
            "append_engine_ms": 0.0,
            "selected_refs": len(selected),
            "dropped_refs": int(len(dropped_over_budget)),
            "scanned_records": int(retrieval_scan_stats.get("loaded_records") or retrieval_scan_stats.get("scanned_records") or len(records)) if isinstance(retrieval_scan_stats, dict) else len(records),
            "candidate_cache_hit": candidate_cache_hit,
            "cache_hit": candidate_cache_hit,
            "index_postings_read": index_postings_read,
            "index_postings_touched": index_postings_read,
            "placement_partitions_touched": len(placement.get("locations", []) or []) if isinstance(placement, dict) else 0,
            "native_pack_assembly": False,
            "python_pack_fallback": True,
            "raw_candidate_tables_returned": False,
            "source": "python_reference_pack",
        }
        if bool(args.get("include_retrieval_metrics")):
            pack["include_retrieval_metrics"] = True
        pack["recall_policy"]["stage_latency_budgets"] = stage_budget_snapshot()
        over_budget_stages = pack["recall_policy"]["stage_latency_budgets"].get("over_budget_stages", [])
        if over_budget_stages and not any(str(warning).startswith("stage_budget_exceeded:") for warning in quality_warnings):
            quality_warnings.append("stage_budget_exceeded:" + ",".join(over_budget_stages))
            pack["quality_warnings"] = quality_warnings
        if bool(args.get("debug_context_pack")) or bool(args.get("include_retrieval_debug")):
            return pack
        return compact_context_pack_for_serving(pack)

    def feedback(self, args: Json, *, hook: Json | None = None) -> Json:
        args = {**args, "kind": "feedback"}
        return self.ingest(args, hook=hook)

    def replay(self, args: Json) -> Json:
        return local_replay_helpers.replay(self, args)
