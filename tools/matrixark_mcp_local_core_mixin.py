#!/usr/bin/env python3
"""Core local MatrixArk adapter wrapper methods."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json
    from tools.matrixark_mcp_env import env_bool
    from tools import matrixark_mcp_local_backend as local_backend_helpers
    from tools import matrixark_mcp_local_cache as local_cache_helpers
    from tools import matrixark_mcp_local_idempotency as local_idempotency_helpers
    from tools import matrixark_mcp_local_read as local_read_helpers
    from tools import matrixark_mcp_local_runtime as local_runtime_helpers
    from tools import matrixark_mcp_retrieval_records as retrieval_record_helpers
    from tools import matrixark_mcp_visibility as visibility_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json
    from matrixark_mcp_env import env_bool
    import matrixark_mcp_local_backend as local_backend_helpers
    import matrixark_mcp_local_cache as local_cache_helpers
    import matrixark_mcp_local_idempotency as local_idempotency_helpers
    import matrixark_mcp_local_read as local_read_helpers
    import matrixark_mcp_local_runtime as local_runtime_helpers
    import matrixark_mcp_retrieval_records as retrieval_record_helpers
    import matrixark_mcp_visibility as visibility_helpers


LOCAL_READ_CACHE_COPY = env_bool("MATRIXARK_LOCAL_READ_CACHE_COPY", True)
RETRIEVAL_HOT_RECORD_TYPES = retrieval_record_helpers.RETRIEVAL_HOT_RECORD_TYPES


class MatrixArkLocalCoreMixin:
    """Local adapter plumbing for runtime, cache, reads, visibility, and idempotency."""

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

    def append_idempotency_record(
        self,
        *,
        key_hash: int,
        tool_name: str,
        raw_key: str,
        identity: Json,
        response: Json,
    ) -> None:
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
        """Return records eligible for retrieval hot-path scan/filter/pack."""

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
