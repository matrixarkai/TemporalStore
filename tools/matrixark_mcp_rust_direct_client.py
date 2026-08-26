#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Rust direct cdylib client for MatrixArk TemporalStore adapters."""

from __future__ import annotations

import json
import time
from typing import Any

try:
    from tools.matrixark_mcp_core import Json, MatrixArkError
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, MatrixArkError


class MatrixArkRustCdylibClient:
    """In-process Rust direct SDK binding loaded through the Rust cdylib C ABI."""

    def __init__(
        self,
        *,
        library_path: str,
        temporalstore_lib: str = "",
        metaserver: str,
        namespace: str,
        table: str,
        request_timeout_ms: int,
        io_timeout_ms: int,
    ) -> None:
        import ctypes
        import json as _json

        if not library_path:
            raise MatrixArkError("MATRIXARK_TEMPORALSTORE_RUST_DIRECT_LIB is required for Rust direct cdylib mode")
        self.library_path = library_path
        self.metaserver = metaserver
        self.namespace = namespace
        self.table = table
        self.request_timeout_ms = request_timeout_ms
        self.io_timeout_ms = io_timeout_ms
        self.sdk_mode = "direct_cdylib"
        self._ctypes = ctypes
        load_mode = getattr(ctypes, "RTLD_GLOBAL", None)
        if temporalstore_lib:
            try:
                ctypes.CDLL(temporalstore_lib, mode=load_mode) if load_mode is not None else ctypes.CDLL(temporalstore_lib)
            except OSError:
                pass
        self._lib = ctypes.CDLL(library_path, mode=load_mode) if load_mode is not None else ctypes.CDLL(library_path)
        self._bind()
        self._handle = ctypes.c_void_p()
        self._commands_total = 0
        self._commands_failed_total = 0
        self._records_written_total = 0
        self._records_read_total = 0
        self._latency_samples_ms: list[float] = []
        options = {
            "metaserver_addr": metaserver,
            "namespace_name": namespace,
            "table_name": table,
            "request_timeout_ms": request_timeout_ms,
            "io_timeout_ms": io_timeout_ms,
            "connect_timeout_ms": min(request_timeout_ms, io_timeout_ms),
            "max_read_retries": 2,
            "max_write_retries": 1,
            "retry_backoff_ms": 2,
            "pin_primary": True,
        }
        error = ctypes.c_void_p()
        code = self._lib.temporalstore_rust_connect_json(
            _json.dumps(options, separators=(",", ":")).encode("utf-8"),
            ctypes.byref(self._handle),
            ctypes.byref(error),
        )
        self._check(code, error)

    def _bind(self) -> None:
        c = self._ctypes
        lib = self._lib
        lib.temporalstore_rust_free_string.argtypes = [c.c_void_p]
        lib.temporalstore_rust_free_string.restype = None
        lib.temporalstore_rust_connect_json.argtypes = [c.c_char_p, c.POINTER(c.c_void_p), c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_connect_json.restype = c.c_int
        lib.temporalstore_rust_close.argtypes = [c.c_void_p, c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_close.restype = c.c_int
        lib.temporalstore_rust_hset.argtypes = [c.c_void_p, c.c_char_p, c.c_char_p, c.c_char_p, c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_hset.restype = c.c_int
        lib.temporalstore_rust_hget.argtypes = [c.c_void_p, c.c_char_p, c.c_char_p, c.POINTER(c.c_void_p), c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_hget.restype = c.c_int
        lib.temporalstore_rust_hgetall_json.argtypes = [c.c_void_p, c.c_char_p, c.POINTER(c.c_void_p), c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_hgetall_json.restype = c.c_int
        lib.temporalstore_rust_matrixark_batch_append_records_json.argtypes = [c.c_void_p, c.c_char_p, c.c_char_p, c.c_char_p, c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_matrixark_batch_append_records_json.restype = c.c_int
        lib.temporalstore_rust_matrixark_scan_candidates_json.argtypes = [c.c_void_p, c.c_char_p, c.c_char_p, c.c_size_t, c.c_char_p, c.POINTER(c.c_void_p), c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_matrixark_scan_candidates_json.restype = c.c_int
        lib.temporalstore_rust_matrixark_retrieve_context_pack_json.argtypes = [c.c_void_p, c.c_char_p, c.c_char_p, c.c_size_t, c.c_char_p, c.POINTER(c.c_void_p), c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_matrixark_retrieve_context_pack_json.restype = c.c_int

    def _decode_owned(self, value: Any) -> str:
        try:
            return self._ctypes.cast(value, self._ctypes.c_char_p).value.decode("utf-8", errors="replace")
        finally:
            self._lib.temporalstore_rust_free_string(value)

    def _check(self, code: int, error: Any) -> None:
        if code == 0:
            return
        message = "unknown Rust TemporalStore direct binding error"
        if error:
            message = self._decode_owned(error)
        raise MatrixArkError(message)

    def _call(self, op: str, fn: Any, *, records_written: int = 0, records_read: int = 0) -> Any:
        started = time.perf_counter()
        self._commands_total += 1
        try:
            result = fn()
        except Exception:
            self._commands_failed_total += 1
            raise
        finally:
            self._latency_samples_ms.append((time.perf_counter() - started) * 1000.0)
            if len(self._latency_samples_ms) > 2048:
                self._latency_samples_ms = self._latency_samples_ms[-2048:]
        self._records_written_total += records_written
        self._records_read_total += records_read
        return result

    def close(self) -> None:
        if not getattr(self, "_handle", None):
            return
        error = self._ctypes.c_void_p()
        code = self._lib.temporalstore_rust_close(self._handle, self._ctypes.byref(error))
        self._handle = self._ctypes.c_void_p()
        self._check(code, error)

    def put_string(self, key: str, value: str) -> None:
        # MatrixArk direct serving should use batch append; keep this for compatibility through hset-style paths.
        self.hset(key, "", value)

    def get_string(self, key: str) -> str:
        return self.hget(key, "")

    def hset(self, key: str, field: str, value: str) -> None:
        def call() -> None:
            error = self._ctypes.c_void_p()
            code = self._lib.temporalstore_rust_hset(self._handle, key.encode(), field.encode(), value.encode(), self._ctypes.byref(error))
            self._check(code, error)
        self._call("hset", call, records_written=1)

    def hget(self, key: str, field: str) -> str:
        def call() -> str:
            out = self._ctypes.c_void_p()
            error = self._ctypes.c_void_p()
            code = self._lib.temporalstore_rust_hget(self._handle, key.encode(), field.encode(), self._ctypes.byref(out), self._ctypes.byref(error))
            self._check(code, error)
            return self._decode_owned(out)
        return self._call("hget", call, records_read=1)

    def hgetall(self, key: str) -> list[Json]:
        return list(self.scan_hash(key).get("records", []))

    def scan_hash(self, key: str) -> Json:
        def call() -> Json:
            out = self._ctypes.c_void_p()
            error = self._ctypes.c_void_p()
            code = self._lib.temporalstore_rust_hgetall_json(self._handle, key.encode(), self._ctypes.byref(out), self._ctypes.byref(error))
            self._check(code, error)
            return json.loads(self._decode_owned(out))
        result = self._call("scan_hash", call)
        self._records_read_total += int(result.get("count") or 0)
        return result

    def batch_hset(self, entries: list[Json]) -> None:
        self.matrixark_batch_append_records(entries)

    def matrixark_batch_append_records(
        self,
        entries: list[Json],
        *,
        count_key: str | None = None,
        count_value: str | None = None,
        append_options: Json | None = None,
    ) -> None:
        values = [{"key": str(entry.get("key") or ""), "field": str(entry.get("field") or ""), "value": str(entry.get("value") or "")} for entry in entries]
        payload = json.dumps(values, separators=(",", ":"), sort_keys=True).encode("utf-8")
        def call() -> None:
            error = self._ctypes.c_void_p()
            code = self._lib.temporalstore_rust_matrixark_batch_append_records_json(
                self._handle,
                payload,
                (count_key or "").encode("utf-8"),
                (count_value or "").encode("utf-8"),
                self._ctypes.byref(error),
            )
            self._check(code, error)
        self._call("matrixark_batch_append_records", call, records_written=len(values) + (1 if count_key else 0))

    def matrixark_append_records(
        self,
        entries: list[Json],
        *,
        count_key: str | None = None,
        count_value: str | None = None,
        append_options: Json | None = None,
    ) -> None:
        self.matrixark_batch_append_records(
            entries,
            count_key=count_key,
            count_value=count_value,
            append_options=append_options,
        )

    def matrixark_scan_candidates(self, *, count_key: str, record_hash_key: str, shard_size: int, scope: Json, record_types: list[str], secondary_index_groups: list[list[str]], selected_node_hashes: list[int], newest_by_type: Json | None = None) -> Json:
        request = json.dumps({"scope": scope, "record_types": record_types, "secondary_index_groups": secondary_index_groups, "selected_node_hashes": selected_node_hashes}, separators=(",", ":"), sort_keys=True).encode("utf-8")
        def call() -> Json:
            out = self._ctypes.c_void_p()
            error = self._ctypes.c_void_p()
            code = self._lib.temporalstore_rust_matrixark_scan_candidates_json(self._handle, count_key.encode(), record_hash_key.encode(), int(shard_size), request, self._ctypes.byref(out), self._ctypes.byref(error))
            self._check(code, error)
            return json.loads(self._decode_owned(out))
        return self._call("matrixark_scan_candidates", call)

    def matrixark_retrieve_context_pack(self, *, count_key: str, record_hash_key: str, shard_size: int, request: Json) -> Json:
        payload = json.dumps(request, separators=(",", ":"), sort_keys=True).encode("utf-8")
        def call() -> Json:
            out = self._ctypes.c_void_p()
            error = self._ctypes.c_void_p()
            code = self._lib.temporalstore_rust_matrixark_retrieve_context_pack_json(self._handle, count_key.encode(), record_hash_key.encode(), int(shard_size), payload, self._ctypes.byref(out), self._ctypes.byref(error))
            self._check(code, error)
            return json.loads(self._decode_owned(out))
        return self._call("matrixark_retrieve_context_pack", call)

    def health(self) -> Json:
        return {"ok": True, "status": "ok", "mode": "rust_direct_cdylib"}

    def readiness(self) -> Json:
        return {"ok": True, "status": "ready", "mode": "rust_direct_cdylib", "cached_clients": 1}

    def metrics_snapshot(self) -> Json:
        elapsed = max(0.001, sum(self._latency_samples_ms) / 1000.0) if self._latency_samples_ms else 1.0
        return {
            "gateway_mode": "rust_direct_cdylib",
            "proxy_mode": "none",
            "sdk_mode": "direct_cdylib",
            "transport": "in_process_cdylib_ctypes",
            "process_per_operation_enabled": False,
            "single_shot_mode": "debug_only_disabled_for_hot_path",
            "commands_total": self._commands_total,
            "commands_failed_total": self._commands_failed_total,
            "timeouts_total": 0,
            "qps": round(self._commands_total / elapsed, 6),
            "records_written_total": self._records_written_total,
            "records_read_total": self._records_read_total,
            "latency_ms_sum": round(sum(self._latency_samples_ms), 3),
            "latency_ms_count": len(self._latency_samples_ms),
            "latency_ms_max": round(max(self._latency_samples_ms) if self._latency_samples_ms else 0.0, 3),
            "p95_latency_ms": round(self._percentile(self._latency_samples_ms, 0.95), 3),
            "p99_latency_ms": round(self._percentile(self._latency_samples_ms, 0.99), 3),
            "matrixark_append_write_path": "rust_direct_cdylib_matrixark_batch_append_records",
            "matrixark_native_batch_append_available": True,
            "matrixark_batch_append_uses_existing_batch_execute": True,
            "matrixark_batch_append_existing_batch_execute_source": "temporalstore_rust_cdylib_to_temporalstore_matrixark_batch_append_records",
            "matrixark_append_uses_per_record_hset": False,
            "matrixark_append_uses_generic_batch_hset_fallback": False,
            "supports_batch_append": True,
            "supports_prefix_scan": True,
            "prefix_scan_path": "rust_direct_cdylib_hgetall_json",
            "supports_native_candidate_prefilter": True,
            "candidate_prefilter_path": "rust_direct_cdylib_matrixark_scan_candidates",
            "supports_native_pack_assembly": True,
            "native_pack_assembly_path": "rust_direct_cdylib_matrixark_retrieve_context_pack",
            "requires_c_sdk_hgetall_for_prefix_scan": False,
        }

    def metrics_prometheus(self) -> str:
        metrics = self.metrics_snapshot()
        return "\n".join([
            '# TYPE matrixark_rust_direct_cdylib_commands_total counter',
            f'matrixark_rust_direct_cdylib_commands_total {metrics["commands_total"]}',
            '# TYPE matrixark_rust_direct_cdylib_errors_total counter',
            f'matrixark_rust_direct_cdylib_errors_total {metrics["commands_failed_total"]}',
        ]) + "\n"

    @staticmethod
    def _percentile(values: list[float], ratio: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * ratio))))
        return ordered[index]

    def shutdown(self) -> None:
        self.close()
