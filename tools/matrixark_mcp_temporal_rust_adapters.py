#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Rust TemporalStore MatrixArk adapter implementations."""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        DIRECT_AUDIT_BUFFER_MAX_RECORDS,
        DIRECT_AUDIT_FLUSH_INTERVAL_MS,
        DIRECT_AUDIT_MODE,
        DIRECT_RECORD_LOG_SHARD_SIZE,
        DIRECT_WRITE_BACKOFF_MS,
        DIRECT_WRITE_RETRIES,
        DIRECT_WRITE_THROTTLE_MS,
        Json,
        MatrixArkError,
        compact_latest_context_state_records,
        native_candidate_prefilter_required,
    )
    from tools.matrixark_mcp_local_adapter import MatrixArkLocalAdapter
    from tools.matrixark_mcp_native_helpers import latency_quantile_from_bucket_map as _latency_quantile_from_bucket_map
    from tools.matrixark_mcp_rust_direct_client import MatrixArkRustCdylibClient
    from tools.matrixark_mcp_rust_proxy_client import MatrixArkRustCliClient, MatrixArkRustProxyClient
    from tools.matrixark_mcp_temporal_adapters import MatrixArkTemporalStoreDirectAdapter
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        DIRECT_AUDIT_BUFFER_MAX_RECORDS,
        DIRECT_AUDIT_FLUSH_INTERVAL_MS,
        DIRECT_AUDIT_MODE,
        DIRECT_RECORD_LOG_SHARD_SIZE,
        DIRECT_WRITE_BACKOFF_MS,
        DIRECT_WRITE_RETRIES,
        DIRECT_WRITE_THROTTLE_MS,
        Json,
        MatrixArkError,
        compact_latest_context_state_records,
        native_candidate_prefilter_required,
    )
    from matrixark_mcp_local_adapter import MatrixArkLocalAdapter
    from matrixark_mcp_native_helpers import latency_quantile_from_bucket_map as _latency_quantile_from_bucket_map
    from matrixark_mcp_rust_direct_client import MatrixArkRustCdylibClient
    from matrixark_mcp_rust_proxy_client import MatrixArkRustCliClient, MatrixArkRustProxyClient
    from matrixark_mcp_temporal_adapters import MatrixArkTemporalStoreDirectAdapter


class MatrixArkTemporalStoreRustAdapter(MatrixArkTemporalStoreDirectAdapter):
    """MatrixArk adapter backed by the Rust TemporalStore proxy or direct SDK."""

    def __init__(
        self,
        *,
        rust_cli: str = "",
        rust_proxy: str = "",
        metaserver: str,
        namespace: str,
        table: str,
        storage_prefix: str = "matrixark:mcp",
        request_timeout_ms: int = 20000,
        io_timeout_ms: int = 20000,
        sdk_mode: str = "proxy",
    ) -> None:
        MatrixArkLocalAdapter.__init__(self, Path("/tmp/matrixark-mcp-unused-rust.jsonl"))
        MatrixArkLocalAdapter._init_local_runtime_state(self)
        self._entity_cache_loaded = True
        self._context_node_cache_loaded = True
        self._metaserver = metaserver
        self._namespace = namespace
        self._table = table
        proxy_path = rust_proxy or rust_cli
        direct_lib = os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_DIRECT_LIB", "").strip()
        temporalstore_lib = os.environ.get("TEMPORALSTORE_LIB", os.environ.get("MATRIXARK_TEMPORALSTORE_LIB", "")).strip()
        self._rust_direct_cdylib_enabled = bool(
            sdk_mode in {"direct-sdk", "direct_sdk", "native-binding", "rust-direct"}
            and direct_lib
            and Path(direct_lib).exists()
        )
        if self._rust_direct_cdylib_enabled:
            self._client = MatrixArkRustCdylibClient(
                library_path=direct_lib,
                temporalstore_lib=temporalstore_lib,
                metaserver=metaserver,
                namespace=namespace,
                table=table,
                request_timeout_ms=request_timeout_ms,
                io_timeout_ms=io_timeout_ms,
            )
        else:
            self._client = MatrixArkRustProxyClient(
                proxy_path=proxy_path,
                metaserver=metaserver,
                namespace=namespace,
                table=table,
                request_timeout_ms=request_timeout_ms,
                io_timeout_ms=io_timeout_ms,
                sdk_mode=sdk_mode,
            )
        self._retrieve_client: Any | None = None
        self._summary_client: Any | None = None
        self._retrieve_client_lock = threading.RLock()
        self._summary_client_lock = threading.RLock()
        self._visibility_publish_lock = threading.RLock()
        self._visibility_publish_thread: threading.Thread | None = None
        self._visibility_publish_error: Exception | None = None
        self._dedicated_proxy_clients_enabled = os.environ.get(
            "MATRIXARK_RUST_PROXY_DEDICATED_CLIENTS",
            "1",
        ).strip().lower() in {"1", "true", "yes"}
        self._dedicated_pack_lanes_enabled = os.environ.get(
            "MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES",
            "1",
        ).strip().lower() in {"1", "true", "yes"}
        self._publish_visibility_after_flush = (
            os.environ.get("MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_ON_FLUSH")
            or os.environ.get("MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_AFTER_FLUSH")
            or "0"
        ).strip().lower() in {"1", "true", "yes"}
        self._track_pending_visibility_keys = (
            self._dedicated_proxy_clients_enabled or self._dedicated_pack_lanes_enabled
        )
        self._async_visibility_publish_after_flush = os.environ.get(
            "MATRIXARK_RUST_PROXY_ASYNC_VISIBILITY_PUBLISH_AFTER_FLUSH",
            "0",
        ).strip().lower() in {"1", "true", "yes"}
        self._rust_proxy_path = proxy_path
        self._rust_request_timeout_ms = request_timeout_ms
        self._rust_io_timeout_ms = io_timeout_ms
        self._rust_sdk_mode = sdk_mode
        self._metaserver = metaserver
        self._namespace = namespace
        self._table = table
        self._readiness_cache: Json | None = None
        self._readiness_lock = threading.RLock()
        self._storage_prefix = storage_prefix.rstrip(":")
        self._supported_storage_families = self._parse_supported_storage_families()
        self._record_hash_key = f"{self._storage_prefix}:records"
        self._index_key = f"{self._storage_prefix}:record_index"
        self._count_key = f"{self._storage_prefix}:record_count"
        self._matrixark_native_batch_append_available = True
        if self._rust_direct_cdylib_enabled:
            self._matrixark_append_write_path = "rust_direct_cdylib_matrixark_batch_append_records"
        else:
            self._matrixark_append_write_path = (
                "rust_direct_sdk_matrixark_batch_append_records"
                if sdk_mode in {"direct-sdk", "direct_sdk", "native-binding", "rust-direct"}
                else "rust_proxy_matrixark_batch_runtime_default"
            )
        self._matrixark_append_uses_per_record_hset = False
        self._matrixark_batch_append_uses_existing_batch_execute = True
        self._matrixark_batch_append_existing_batch_execute_source = "temporalstore_matrixark_batch_append_records"
        self._shard_size = DIRECT_RECORD_LOG_SHARD_SIZE
        self._index_cache: list[str] | None = None
        self._records_cache: list[Json] | None = None
        self._entry_count_cache: int | None = None
        self._legacy_index_mode = False
        self._records_lock = threading.RLock()
        self._audit_lock = threading.RLock()
        self._audit_buffer: list[Json] = []
        self._audit_flusher_started = False
        self._audit_flush_failures = 0
        if DIRECT_AUDIT_MODE not in {"buffered", "deferred", "drop", "sync"}:
            raise MatrixArkError("MATRIXARK_DIRECT_AUDIT_MODE must be buffered, deferred, drop, or sync")
        self._audit_mode = DIRECT_AUDIT_MODE
        self._audit_buffer_max_records = max(1, DIRECT_AUDIT_BUFFER_MAX_RECORDS)
        self._audit_flush_interval_s = max(0.05, DIRECT_AUDIT_FLUSH_INTERVAL_MS / 1000.0)
        self._write_retries = max(0, DIRECT_WRITE_RETRIES)
        self._write_backoff_s = max(0.0, DIRECT_WRITE_BACKOFF_MS / 1000.0)
        self._write_throttle_s = max(0.0, DIRECT_WRITE_THROTTLE_MS / 1000.0)
        self._backend_ready = False
        self._backend_ready_result = None
        self._backend_readiness_lock = threading.RLock()

    def _native_summary_client(self) -> Any:
        """Return a dedicated summary/audit lane when the backend transport needs one."""

        if (
            getattr(self, "_rust_direct_cdylib_enabled", False)
            or not getattr(self, "_dedicated_proxy_clients_enabled", False)
            or self._has_pending_visibility_keys()
            or self._visibility_publish_active()
        ):
            return self._client
        with self._summary_client_lock:
            if self._summary_client is None:
                self._summary_client = MatrixArkRustProxyClient(
                    proxy_path=self._rust_proxy_path,
                    metaserver=self._metaserver,
                    namespace=self._namespace,
                    table=self._table,
                    request_timeout_ms=self._rust_request_timeout_ms,
                    io_timeout_ms=self._rust_io_timeout_ms,
                    sdk_mode=self._rust_sdk_mode,
                )
            return self._summary_client

    def _append_client_for_records(self, records: list[Json]) -> Any:
        hot_record_types = {
            "context_event",
            "context_entity",
            "context_segment",
            "resource_chunk",
            "resource_manifest",
            "resource_registry",
            "skill_manifest",
            "skill_section",
            "skill_registry_update",
            "context_index",
        }
        summary_or_audit_types = {
            "context_child_ref",
            "context_embedding",
            "context_pack_audit",
            "context_summary",
            "context_summary_dirty",
            "context_summary_refresh_audit",
            "matrixark_audit_log",
        }
        record_types = {str(record.get("record_type") or "") for record in records if isinstance(record, dict)}
        if record_types and not (record_types & hot_record_types) and (record_types & summary_or_audit_types):
            return self._native_summary_client()
        return self._client

    def _native_retrieve_client(self) -> Any:
        """Return the native serving read client.

        Rust cdylib direct mode is in-process and does not need a proxy lane;
        the stdio proxy path keeps a dedicated read lane to avoid head-of-line
        blocking behind writes or audit flushes.
        """

        if (
            getattr(self, "_rust_direct_cdylib_enabled", False)
            or not getattr(self, "_dedicated_proxy_clients_enabled", False)
            or self._has_pending_visibility_keys()
            or self._visibility_publish_active()
        ):
            return self._client
        with self._retrieve_client_lock:
            if self._retrieve_client is None:
                self._retrieve_client = MatrixArkRustProxyClient(
                    proxy_path=self._rust_proxy_path,
                    metaserver=self._metaserver,
                    namespace=self._namespace,
                    table=self._table,
                    request_timeout_ms=self._rust_request_timeout_ms,
                    io_timeout_ms=self._rust_io_timeout_ms,
                    sdk_mode=self._rust_sdk_mode,
                )
                warmer = getattr(self._retrieve_client, "warm_lane_group", None)
                if callable(warmer):
                    warmer("pack")
            return self._retrieve_client

    def flush_direct_writes(self, timeout_s: float | None = None) -> None:
        super().flush_direct_writes(timeout_s=timeout_s)
        if not (
            getattr(self, "_publish_visibility_after_flush", False)
            or (
                getattr(self, "_async_visibility_publish_after_flush", False)
                and getattr(self, "_track_pending_visibility_keys", False)
            )
        ):
            return
        visibility_keys = self._consume_pending_visibility_keys()
        if not visibility_keys:
            visibility_keys = []
        if not getattr(self, "_publish_visibility_after_flush", False):
            self._start_async_visibility_publish(visibility_keys)
            return
        self._publish_visibility_keys(visibility_keys)

    def close(self, *, timeout_s: float = 5.0) -> None:
        try:
            self.flush_direct_writes(timeout_s=timeout_s)
        finally:
            for attr in ("_retrieve_client", "_summary_client", "_client"):
                client = getattr(self, attr, None)
                close = getattr(client, "close", None)
                if callable(close):
                    close()
            super_close = getattr(super(), "close", None)
            if callable(super_close):
                try:
                    super_close(timeout_s=timeout_s)
                except TypeError:
                    super_close()

    def _publish_visibility_keys(self, visibility_keys: list[str]) -> None:
        publisher = getattr(self._client, "matrixark_publish_visibility", None)
        if callable(publisher):
            publisher(visibility_keys=visibility_keys)

    def _should_publish_visibility_key(self, key: str) -> bool:
        key = str(key or "")
        return key == self._count_key or key.startswith(f"{self._record_hash_key}:")

    def _visibility_publish_active(self) -> bool:
        with self._visibility_publish_lock:
            thread = self._visibility_publish_thread
            return bool(thread is not None and thread.is_alive())

    def _start_async_visibility_publish(self, visibility_keys: list[str]) -> None:
        with self._visibility_publish_lock:
            thread = self._visibility_publish_thread
            if thread is not None and thread.is_alive():
                self._note_pending_visibility_keys(visibility_keys)
                return
            self._visibility_publish_error = None

            def publish() -> None:
                try:
                    self._publish_visibility_keys(visibility_keys)
                except Exception as exc:  # pragma: no cover - surfaced by read drain.
                    with self._visibility_publish_lock:
                        self._visibility_publish_error = exc

            thread = threading.Thread(target=publish, name="matrixark-rust-visibility-publisher", daemon=True)
            self._visibility_publish_thread = thread
            thread.start()

    def _drain_visibility_publish_for_read(self) -> None:
        while True:
            with self._visibility_publish_lock:
                thread = self._visibility_publish_thread
            if thread is not None:
                thread.join()
            with self._visibility_publish_lock:
                error = self._visibility_publish_error
                if error is not None:
                    self._visibility_publish_error = None
                    raise MatrixArkError(f"rust visibility publish failed before native read: {error}") from error
            visibility_keys = self._consume_pending_visibility_keys()
            if not visibility_keys:
                return
            self._publish_visibility_keys(visibility_keys)

    def supports_native_candidate_prefilter(self) -> bool:
        return True

    def supports_native_context_pack(self) -> bool:
        return True

    def native_context_pack(self, request: Json) -> Json | None:
        self._drain_visibility_publish_for_read()
        retriever = self._native_retrieve_client().matrixark_retrieve_context_pack
        try:
            response = retriever(
                count_key=self._count_key,
                record_hash_key=self._record_hash_key,
                shard_size=self._shard_size,
                request=request,
            )
        except Exception as exc:
            if self.native_context_pack_required():
                raise MatrixArkError(
                    f"backend-native ContextPack assembly failed for {self._backend_label()}: {exc}. "
                    "Python reference packing is disabled for TemporalStore serving unless explicitly overridden for local debug."
                ) from exc
            return None
        if not isinstance(response, dict) or not response.get("native_pack_assembly"):
            if self.native_context_pack_required():
                raise MatrixArkError(
                    f"backend-native ContextPack assembly returned an invalid response for {self._backend_label()}. "
                    "Python reference packing is disabled for TemporalStore serving unless explicitly overridden for local debug."
                )
            return None
        if isinstance(response.get("records"), list):
            raise MatrixArkError("native matrixark_retrieve_context_pack must return a finished ContextPack, not raw records")
        pack = response.get("context_pack")
        if not isinstance(pack, dict):
            return None
        pack.setdefault("context_pack_assembly", "native_backend")
        pack.setdefault("backend", self._backend_label())
        recall_policy = pack.get("recall_policy") if isinstance(pack.get("recall_policy"), dict) else {}
        contract = recall_policy.get("native_response_contract") if isinstance(recall_policy.get("native_response_contract"), dict) else {}
        contract.setdefault("raw_records_returned_to_python", False)
        contract.setdefault("python_hot_path_records", 0)
        contract.setdefault("python_role", "dispatch_request_receive_context_pack")
        contract.setdefault("backend_role", "scan_filter_score_pack")
        contract.setdefault("rust_proxy_dedicated_retrieve_lane", bool(getattr(self, "_dedicated_proxy_clients_enabled", False)))
        recall_policy["native_response_contract"] = contract
        pack["recall_policy"] = recall_policy
        return pack

    def _native_candidate_scan(
        self,
        *,
        scope: Json,
        record_types: set[str],
        secondary_index_groups: list[set[str]] | None,
        selected_node_hashes: set[int] | None,
    ) -> Json | None:
        try:
            self._drain_visibility_publish_for_read()
            response = self._native_retrieve_client().matrixark_scan_candidates(
                count_key=self._count_key,
                record_hash_key=self._record_hash_key,
                shard_size=self._shard_size,
                scope=scope,
                record_types=sorted(record_types),
                secondary_index_groups=[sorted(group) for group in (secondary_index_groups or [])],
                selected_node_hashes=sorted(int(item) for item in (selected_node_hashes or set())),
            )
        except Exception as exc:
            if native_candidate_prefilter_required(backend_label=self._backend_label()):
                raise MatrixArkError(
                    f"backend-native candidate prefilter failed for {self._backend_label()}: {exc}. "
                    "Python read_all scan/prefilter is disabled for TemporalStore serving unless explicitly overridden for local debug."
                ) from exc
            return None
        records = response.get("records") if isinstance(response, dict) else None
        if not isinstance(records, list):
            if native_candidate_prefilter_required(backend_label=self._backend_label()):
                raise MatrixArkError(
                    f"backend-native candidate prefilter returned an invalid response for {self._backend_label()}. "
                    "Python read_all scan/prefilter is disabled for TemporalStore serving unless explicitly overridden for local debug."
                )
            return None
        scan_stats = dict(response.get("scan_stats") or {})
        scan_stats.setdefault("backend", self._backend_label())
        scan_stats.setdefault("execution_mode", "native_temporalstore_candidate_prefilter")
        scan_stats.setdefault("backend_pushdown", True)
        scan_stats.setdefault("direct_backend_prefilter", True)
        scan_stats.setdefault("native_pushdown", True)
        scan_stats.setdefault("native_prefix_scan", True)
        scan_stats.setdefault("native_secondary_index_prefilter", bool(secondary_index_groups))
        scan_stats.setdefault("native_pack_assembly", False)
        scan_stats.setdefault("cache_hit", False)
        scan_stats.setdefault("record_types", sorted(record_types))
        scan_stats.setdefault("selected_node_hashes_supplied", len(selected_node_hashes or set()))
        scan_stats.setdefault("pack_assembly_location", "native_backend_candidate_scan")
        scan_stats.setdefault("rust_proxy_dedicated_retrieve_lane", True)
        latest_state_records = self._latest_context_state_records_for_candidate_scan(
            scope=scope,
            record_types=record_types,
            selected_node_hashes=selected_node_hashes,
        )
        if latest_state_records:
            records = list(records) + latest_state_records
        records = compact_latest_context_state_records(records)
        scan_stats["latest_summary_state_compaction"] = True
        scan_stats["latest_state_records_loaded"] = len(latest_state_records)
        return {"records": records, "scan_stats": scan_stats}

    def _backend_metaserver(self) -> str:
        return self._client.metaserver

    def _backend_label(self) -> str:
        return "temporalstore-rust"

    def _rust_storage_mode_label(self) -> str:
        sdk_mode = getattr(self._client, "sdk_mode", "")
        if sdk_mode == "direct_cdylib":
            return "rust-direct-cdylib"
        if sdk_mode == "direct_sdk":
            return "rust-direct-sdk-bridge"
        return "rust-proxy"

    def _backend_neutral_prometheus(self, snapshot: Json) -> str:
        backend = "rust"
        storage_mode = self._rust_storage_mode_label()
        buckets = snapshot.get("latency_buckets") if isinstance(snapshot.get("latency_buckets"), dict) else {}
        lines = [
            "# HELP matrixark_backend_qps MatrixArk storage backend command QPS.",
            "# TYPE matrixark_backend_qps gauge",
            f'matrixark_backend_qps{{backend="{backend}"}} {snapshot.get("qps", 0)}',
            "# HELP matrixark_backend_commands_total MatrixArk storage backend command count.",
            "# TYPE matrixark_backend_commands_total counter",
            f'matrixark_backend_commands_total{{backend="{backend}"}} {int(snapshot.get("commands_total") or 0)}',
            "# HELP matrixark_backend_errors_total MatrixArk storage backend command errors.",
            "# TYPE matrixark_backend_errors_total counter",
            f'matrixark_backend_errors_total{{backend="{backend}"}} {int(snapshot.get("commands_failed_total") or 0)}',
            "# HELP matrixark_backend_timeouts_total MatrixArk storage backend command timeouts.",
            "# TYPE matrixark_backend_timeouts_total counter",
            f'matrixark_backend_timeouts_total{{backend="{backend}"}} {int(snapshot.get("timeouts_total") or 0)}',
            "# HELP matrixark_backend_info MatrixArk storage backend identity and mode.",
            "# TYPE matrixark_backend_info gauge",
            f'matrixark_backend_info{{backend="{backend}",storage_mode="{storage_mode}"}} 1',
            "# HELP matrixark_backend_ready MatrixArk storage backend readiness, 1 for ready and 0 for not ready.",
            "# TYPE matrixark_backend_ready gauge",
            f'matrixark_backend_ready{{backend="{backend}",storage_mode="{storage_mode}",status="{"ready" if self._backend_ready else "unknown"}"}} {1 if self._backend_ready else 0}',
            "# HELP matrixark_backend_command_latency_ms MatrixArk storage backend command latency quantiles.",
            "# TYPE matrixark_backend_command_latency_ms gauge",
            f'matrixark_backend_command_latency_ms{{backend="{backend}",quantile="0.50"}} {round(_latency_quantile_from_bucket_map(buckets, int(snapshot.get("latency_ms_count") or 0), 0.50), 3)}',
            f'matrixark_backend_command_latency_ms{{backend="{backend}",quantile="0.95"}} {round(_latency_quantile_from_bucket_map(buckets, int(snapshot.get("latency_ms_count") or 0), 0.95), 3)}',
            f'matrixark_backend_command_latency_ms{{backend="{backend}",quantile="0.99"}} {round(_latency_quantile_from_bucket_map(buckets, int(snapshot.get("latency_ms_count") or 0), 0.99), 3)}',
            "# HELP matrixark_backend_command_latency_ms_bucket MatrixArk storage backend command latency buckets.",
            "# TYPE matrixark_backend_command_latency_ms_bucket counter",
        ]
        for bucket, count in buckets.items():
            lines.append(f'matrixark_backend_command_latency_ms_bucket{{backend="{backend}",le="{bucket}"}} {int(count)}')
        lines.extend(
            [
                "# HELP matrixark_backend_command_latency_ms_sum MatrixArk storage backend command latency sum in milliseconds.",
                "# TYPE matrixark_backend_command_latency_ms_sum counter",
                f'matrixark_backend_command_latency_ms_sum{{backend="{backend}"}} {snapshot.get("latency_ms_sum", 0)}',
                "# HELP matrixark_backend_command_latency_ms_count MatrixArk storage backend command latency sample count.",
                "# TYPE matrixark_backend_command_latency_ms_count counter",
                f'matrixark_backend_command_latency_ms_count{{backend="{backend}"}} {int(snapshot.get("latency_ms_count") or 0)}',
                "# HELP matrixark_backend_command_latency_max_ms MatrixArk storage backend maximum command latency in milliseconds.",
                "# TYPE matrixark_backend_command_latency_max_ms gauge",
                f'matrixark_backend_command_latency_max_ms{{backend="{backend}"}} {snapshot.get("latency_ms_max", 0)}',
                "# HELP matrixark_backend_records_written_total MatrixArk storage backend records written.",
                "# TYPE matrixark_backend_records_written_total counter",
                f'matrixark_backend_records_written_total{{backend="{backend}"}} {int(snapshot.get("records_written_total") or 0)}',
                "# HELP matrixark_backend_records_read_total MatrixArk storage backend records read.",
                "# TYPE matrixark_backend_records_read_total counter",
                f'matrixark_backend_records_read_total{{backend="{backend}"}} {int(snapshot.get("records_read_total") or 0)}',
                "# HELP matrixark_context_records_total MatrixArk context records currently cached by backend.",
                "# TYPE matrixark_context_records_total gauge",
                f'matrixark_context_records_total{{backend="{backend}"}} {int(snapshot.get("matrixark_context_records_total") or 0)}',
                "# HELP matrixark_backend_cached_clients MatrixArk storage backend cached clients.",
                "# TYPE matrixark_backend_cached_clients gauge",
                f'matrixark_backend_cached_clients{{backend="{backend}"}} {int(snapshot.get("clients_created_total") or 1)}',
                "# HELP matrixark_backend_audit_buffered_records MatrixArk buffered audit records awaiting flush.",
                "# TYPE matrixark_backend_audit_buffered_records gauge",
                f'matrixark_backend_audit_buffered_records{{backend="{backend}"}} {len(getattr(self, "_audit_buffer", []))}',
                "# HELP matrixark_backend_audit_flush_failures_total MatrixArk audit flush failure count.",
                "# TYPE matrixark_backend_audit_flush_failures_total counter",
                f'matrixark_backend_audit_flush_failures_total{{backend="{backend}"}} {int(getattr(self, "_audit_flush_failures", 0) or 0)}',
                "# HELP matrixark_backend_proxy_queue_wait_ms_total Total proxy lane queue wait time in milliseconds.",
                "# TYPE matrixark_backend_proxy_queue_wait_ms_total counter",
                f'matrixark_backend_proxy_queue_wait_ms_total{{backend="{backend}"}} {snapshot.get("proxy_queue_wait_ms_total", 0)}',
                "# HELP matrixark_backend_serialization_time_ms_total Total Rust proxy JSON serialization time in milliseconds.",
                "# TYPE matrixark_backend_serialization_time_ms_total counter",
                f'matrixark_backend_serialization_time_ms_total{{backend="{backend}"}} {snapshot.get("serialization_ms_total", 0)}',
                "# HELP matrixark_backend_rust_engine_time_ms_total Total Rust engine execution time in milliseconds.",
                "# TYPE matrixark_backend_rust_engine_time_ms_total counter",
                f'matrixark_backend_rust_engine_time_ms_total{{backend="{backend}"}} {snapshot.get("rust_engine_ms_total", 0)}',
                "# HELP matrixark_retrieve_scan_count_total Total records scanned by native MatrixArk retrieval.",
                "# TYPE matrixark_retrieve_scan_count_total counter",
                f'matrixark_retrieve_scan_count_total{{backend="{backend}"}} {int(snapshot.get("scan_count_total") or 0)}',
                "# HELP matrixark_retrieve_cache_hits_total Total native MatrixArk retrieval cache hits.",
                "# TYPE matrixark_retrieve_cache_hits_total counter",
                f'matrixark_retrieve_cache_hits_total{{backend="{backend}"}} {int(snapshot.get("cache_hits_total") or 0)}',
                "# HELP matrixark_context_pack_selected_refs_total Total refs selected by native ContextPack assembly.",
                "# TYPE matrixark_context_pack_selected_refs_total counter",
                f'matrixark_context_pack_selected_refs_total{{backend="{backend}"}} {int(snapshot.get("selected_refs_total") or 0)}',
                "# HELP matrixark_context_pack_dropped_refs_total Total refs dropped by native ContextPack assembly.",
                "# TYPE matrixark_context_pack_dropped_refs_total counter",
                f'matrixark_context_pack_dropped_refs_total{{backend="{backend}"}} {int(snapshot.get("dropped_refs_total") or 0)}',
            ]
        )
        return "\n".join(lines) + "\n"

    def backend_metrics(self) -> Json:
        health: Json
        readiness: Json
        try:
            health = self._client.health()
        except Exception as exc:
            health = {"ok": False, "error": str(exc)}
        try:
            readiness = self._client.readiness()
        except Exception as exc:
            readiness = {"ok": False, "error": str(exc)}
        rust_client_metrics = self._client.metrics_snapshot()
        rust_retrieve_metrics: Json | None = None
        rust_summary_metrics: Json | None = None
        if self._retrieve_client is not None:
            rust_retrieve_metrics = self._retrieve_client.metrics_snapshot()
        if self._summary_client is not None:
            rust_summary_metrics = self._summary_client.metrics_snapshot()
        try:
            prometheus = self._backend_neutral_prometheus(rust_client_metrics) + self._client.metrics_prometheus()
            if self._retrieve_client is not None:
                prometheus += self._retrieve_client.metrics_prometheus()
            if self._summary_client is not None:
                prometheus += self._summary_client.metrics_prometheus()
        except Exception as exc:
            prometheus = self._backend_neutral_prometheus(rust_client_metrics) + f"# matrixark_rust_proxy_metrics_error {json.dumps(str(exc))}\n"
        gateway_mode = str(rust_client_metrics.get("gateway_mode") or "rust_proxy")
        proxy_mode = str(rust_client_metrics.get("proxy_mode") or "rust_proxy_stdio")
        sdk_mode = str(rust_client_metrics.get("sdk_mode") or getattr(self._client, "sdk_mode", "proxy"))
        return {
            "backend": self._backend_label(),
            "metrics_format": "prometheus",
            "gateway_mode": gateway_mode,
            "sdk_mode": sdk_mode,
            "production_path": "rust_native_proxy" if not self._rust_direct_cdylib_enabled else "rust_c_api_bridge_diagnostic",
            "process_per_operation_enabled": False,
            "single_shot_mode": "debug_only",
            "direct_sdk_bridge": False,
            "native_c_api_bridge_diagnostic": bool(self._rust_direct_cdylib_enabled),
            "pure_embedded_direct_sdk": False,
            "capabilities": {
                "health_endpoint": True,
                "readiness_endpoint": True,
                "metrics_endpoint": True,
                "batch_append": True,
                "matrixark_batch_append_records": True,
                "matrixark_retrieve_context_pack": True,
                "compact_secondary_index_lookup": True,
                "placement_key_candidate_fetch": True,
                "context_pack_telemetry": True,
                "prefix_scan": True,
                "connection_pooling": True,
                "client_pooling": True,
                "dedicated_retrieve_lane": True,
                "backpressure": True,
                "graceful_shutdown": True,
                "timeout_handling": True,
                "structured_errors_compatible": True,
            },
            "health": health,
            "readiness": readiness,
            "prometheus": prometheus,
            "metrics": {
                "metaserver": self._metaserver,
                "namespace": self._namespace,
                "table": self._table,
                "storage_prefix": self._storage_prefix,
                "audit_mode": self._audit_mode,
                "audit_buffered_records": len(self._audit_buffer),
                "audit_flush_failures": self._audit_flush_failures,
                "rust_client": rust_client_metrics,
                "rust_write_client": rust_client_metrics,
                "rust_retrieve_client": rust_retrieve_metrics,
                "rust_summary_client": rust_summary_metrics,
                "rust_proxy_lanes": {
                    "write": not self._rust_direct_cdylib_enabled,
                    "retrieve": self._retrieve_client is not None,
                    "summary_audit": self._summary_client is not None,
                    "summary_audit_share_write_lane": False,
                    "retrieve_isolated_from_summary_audit": True,
                    "ingest_isolated_from_summary_audit": True,
                    "direct_cdylib_enabled": self._rust_direct_cdylib_enabled,
                    "transport": rust_client_metrics.get("transport", "rust_proxy_stdio"),
                },
            },
        }


class MatrixArkTemporalStoreRustDirectAdapter(MatrixArkTemporalStoreRustAdapter):
    """MatrixArk adapter backed by a long-lived Rust process using the Rust SDK directly.

    This is the Rust parity counterpart to the direct SDK adapter. The Python
    MCP process still owns protocol/model glue, while the Rust bridge owns the
    TemporalStore SDK client and native storage calls. It is intentionally
    explicit so benchmark reports can distinguish it from the production Rust
    proxy path.
    """

    def __init__(self, **kwargs: Any) -> None:
        kwargs["sdk_mode"] = "direct_sdk"
        super().__init__(**kwargs)

    def _backend_label(self) -> str:
        return "temporalstore-rust-direct"
