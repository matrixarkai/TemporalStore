#!/usr/bin/env python3
"""Rust proxy client for MatrixArk TemporalStore adapters."""

from __future__ import annotations

import json
import queue
import subprocess
import threading
import time
from typing import Any

try:
    from tools.matrixark_mcp_core import Json, MatrixArkError
    from tools.matrixark_mcp_rust_proxy_coalesce import (
        append_options_signature,
        assign_coalesced_batch_hget_by_key,
        coalesced_batch_hget,
        coalesced_batch_hset,
        coalesced_matrixark_batch_append_records,
        drain_append_coalescer,
        drain_batch_hget_coalescer,
        drain_batch_hset_coalescer,
        max_count_value,
    )
    from tools.matrixark_mcp_rust_proxy_cache import (
        context_pack_response_cache_clear,
        context_pack_response_cache_get,
        context_pack_response_cache_key,
        context_pack_response_cache_put,
        context_pack_response_singleflight_enter,
        context_pack_response_singleflight_finish,
        context_pack_response_singleflight_wait,
        mark_context_pack_response_cache_hit,
        scan_hash_cache_get,
        scan_hash_cache_invalidate_keys,
        scan_hash_cache_put,
        string_cache_get,
        string_cache_key_allowed,
        string_cache_put,
    )
    from tools.matrixark_mcp_rust_proxy_config import initialize_rust_proxy_config
    from tools.matrixark_mcp_rust_proxy_lanes import build_lane_pools
    from tools.matrixark_mcp_rust_proxy_lane_select import (
        lane_group_for_op,
        pack_lane_sticky_index,
    )
    from tools.matrixark_mcp_rust_proxy_metrics_state import (
        initialize_rust_proxy_cache_state,
        initialize_rust_proxy_metrics_state,
    )
    from tools import matrixark_mcp_rust_proxy_metrics_snapshot as metrics_snapshot_helpers
    from tools.matrixark_mcp_rust_proxy_metrics_record import (
        count_context_record,
        nested_float,
        percentile,
    )
    from tools.matrixark_mcp_rust_proxy_process import (
        close_proxy_lanes,
        close_proxy_process,
        ensure_lane_process,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, MatrixArkError
    from matrixark_mcp_rust_proxy_coalesce import (
        append_options_signature,
        assign_coalesced_batch_hget_by_key,
        coalesced_batch_hget,
        coalesced_batch_hset,
        coalesced_matrixark_batch_append_records,
        drain_append_coalescer,
        drain_batch_hget_coalescer,
        drain_batch_hset_coalescer,
        max_count_value,
    )
    from matrixark_mcp_rust_proxy_cache import (
        context_pack_response_cache_clear,
        context_pack_response_cache_get,
        context_pack_response_cache_key,
        context_pack_response_cache_put,
        context_pack_response_singleflight_enter,
        context_pack_response_singleflight_finish,
        context_pack_response_singleflight_wait,
        mark_context_pack_response_cache_hit,
        scan_hash_cache_get,
        scan_hash_cache_invalidate_keys,
        scan_hash_cache_put,
        string_cache_get,
        string_cache_key_allowed,
        string_cache_put,
    )
    import matrixark_mcp_rust_proxy_metrics_snapshot as metrics_snapshot_helpers
    from matrixark_mcp_rust_proxy_metrics_record import (
        count_context_record,
        nested_float,
        percentile,
    )
    from matrixark_mcp_rust_proxy_config import initialize_rust_proxy_config
    from matrixark_mcp_rust_proxy_lanes import build_lane_pools
    from matrixark_mcp_rust_proxy_lane_select import (
        lane_group_for_op,
        pack_lane_sticky_index,
    )
    from matrixark_mcp_rust_proxy_metrics_state import (
        initialize_rust_proxy_cache_state,
        initialize_rust_proxy_metrics_state,
    )
    from matrixark_mcp_rust_proxy_process import (
        close_proxy_lanes,
        close_proxy_process,
        ensure_lane_process,
    )


class MatrixArkRustProxyClient:
    """Persistent Rust proxy boundary around the Rust TemporalStore SDK.

    The Rust binary owns SDK linkage and runs in JSON-lines ``--serve`` mode as
    a Rust proxy. MatrixArk production and benchmark paths should use this
    proxy or the Rust direct SDK path, never process-per-operation CLI calls.
    """

    def __init__(
        self,
        *,
        proxy_path: str = "",
        cli_path: str = "",
        metaserver: str,
        namespace: str,
        table: str,
        request_timeout_ms: int,
        io_timeout_ms: int,
        sdk_mode: str = "proxy",
    ) -> None:
        proxy_path = proxy_path or cli_path
        if not proxy_path:
            raise MatrixArkError("--rust-proxy or MATRIXARK_TEMPORALSTORE_RUST_PROXY is required for temporalstore-rust")
        self.cli_path = proxy_path
        self.proxy_path = proxy_path
        self.metaserver = metaserver
        self.namespace = namespace
        self.table = table
        self.request_timeout_ms = request_timeout_ms
        self.io_timeout_ms = io_timeout_ms
        self._legacy_lock = threading.Lock()
        self._legacy_semaphore = threading.BoundedSemaphore(1)
        initialize_rust_proxy_config(self, request_timeout_ms=request_timeout_ms)
        self._lanes = build_lane_pools(
            shared_process_mode=self._shared_process_mode,
            dedicated_pack_lanes_enabled=self._dedicated_pack_lanes_enabled,
            write_lane_count=self._write_lane_count,
            read_lane_count=self._read_lane_count,
            pack_lane_count=self._pack_lane_count,
            control_lane_count=self._control_lane_count,
        )
        self._lane_worker_counts = {name: len(lanes) for name, lanes in self._lanes.items()}
        self._lane_worker_counts["retrieve"] = self._lane_worker_counts.get("pack", 0)
        self._lane_cursors = {name: 0 for name in self._lanes}
        self._lane_select_lock = threading.Lock()
        initialize_rust_proxy_metrics_state(self)
        initialize_rust_proxy_cache_state(self)
        self._started_at = time.time()
        self._proc: subprocess.Popen[str] | None = None

    def close(self) -> None:
        close_proxy_lanes(self)

    @staticmethod
    def _close_proc(proc: subprocess.Popen[str]) -> None:
        close_proxy_process(proc)

    def _ensure_lane_proc(self, lane: Json) -> subprocess.Popen[str]:
        return ensure_lane_process(self, lane)

    def warm_lane_group(self, group: str) -> Json:
        lanes = self._lanes.get(group) or []
        started = 0
        for lane in lanes:
            lock: threading.Lock = lane["lock"]
            with lock:
                before = lane.get("proc")
                proc = self._ensure_lane_proc(lane)
                if before is None or before.poll() is not None:
                    started += 1
                if proc.poll() is not None:
                    raise MatrixArkError(f"Rust TemporalStore {group} proxy warmup failed with returncode {proc.returncode}")
        return {"ok": True, "group": group, "lanes": len(lanes), "started": started}

    def _lane_group_for_op(self, op: str) -> str:
        return lane_group_for_op(op)

    def _pack_lane_sticky_index(self, lanes: list[Json], kwargs: Json) -> int | None:
        return pack_lane_sticky_index(lanes, kwargs)

    def _choose_lane(self, op: str, kwargs: Json | None = None) -> tuple[str, Json]:
        group = self._lane_group_for_op(op)
        lanes = self._lanes.get(group) or self._lanes["control"]
        if op == "matrixark_retrieve_context_pack" and kwargs is not None:
            sticky_index = self._pack_lane_sticky_index(lanes, kwargs)
            if sticky_index is not None:
                return group, lanes[sticky_index]
        with self._lane_select_lock:
            index = self._lane_cursors.get(group, 0) % len(lanes)
            self._lane_cursors[group] = index + 1
        return group, lanes[index]

    def _read_json_line(self, proc: subprocess.Popen[str], op: str) -> Json:
        assert proc.stdout is not None
        deadline = time.monotonic() + max(2.0, self.request_timeout_ms / 1000.0 + 2.0)
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                stderr = proc.stderr.read() if proc.stderr else ""
                if op == "shutdown" and proc.returncode == 0:
                    return {"ok": True, "status": "shutdown"}
                raise MatrixArkError(f"Rust TemporalStore {op} process exited ({proc.returncode}): {stderr[-1000:]}")
            ready, _, _ = select.select([proc.stdout], [], [], 0.05)
            if not ready:
                continue
            line = proc.stdout.readline()
            if not line:
                continue
            if not line.strip().startswith("{"):
                continue
            try:
                return json.loads(line)
            except json.JSONDecodeError as exc:
                raise MatrixArkError(f"Rust TemporalStore {op} returned invalid JSON: {line[:200]!r}") from exc
        raise MatrixArkError(
            f"Rust TemporalStore {op} timed out waiting for response from {self.cli_path} "
            f"after {max(2.0, self.request_timeout_ms / 1000.0 + 2.0):.1f}s"
        )

    def _call_json(self, op: str, raise_on_error: bool = True, **kwargs: Any) -> Json:
        command = {
            "op": op,
            "metaserver": self.metaserver,
            "namespace": self.namespace,
            "table": self.table,
            "request_timeout_ms": self.request_timeout_ms,
            "io_timeout_ms": self.io_timeout_ms,
            **kwargs,
        }
        payload = json.dumps(command, separators=(",", ":")) + "\n"
        started = time.perf_counter()
        group, lane = self._choose_lane(op, kwargs)
        semaphore: threading.BoundedSemaphore = lane["semaphore"]
        wait_started = time.perf_counter()
        acquired = semaphore.acquire(timeout=self._backpressure_timeout_s)
        wait_ms = (time.perf_counter() - wait_started) * 1000.0
        if not acquired:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            self._record_call_metrics(op, kwargs, None, elapsed_ms, failed=True, backpressure=True, lane=group, wait_ms=wait_ms)
            raise MatrixArkError(
                f"Rust TemporalStore {op} rejected by {group} proxy lane backpressure after "
                f"{self._backpressure_timeout_s:.3f}s with "
                f"{self._lane_worker_counts.get(group, 1)} workers"
            )
        try:
            lock: threading.Lock = lane["lock"]
            with lock:
                proc = self._ensure_lane_proc(lane)
                assert proc.stdin is not None
                try:
                    proc.stdin.write(payload)
                    proc.stdin.flush()
                except BrokenPipeError as exc:
                    lane["proc"] = None
                    self._close_proc(proc)
                    raise MatrixArkError(f"Rust TemporalStore {op} pipe closed") from exc
                response = self._read_json_line(proc, op)
        except Exception:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            self._record_call_metrics(op, kwargs, None, elapsed_ms, failed=True, lane=group, wait_ms=wait_ms)
            raise
        finally:
            semaphore.release()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        if not response.get("ok"):
            self._record_call_metrics(op, kwargs, response, elapsed_ms, failed=True, lane=group, wait_ms=wait_ms)
            if not raise_on_error:
                return response
            raise MatrixArkError(f"Rust TemporalStore {op} failed: {response.get('error', 'unknown error')}")
        self._record_call_metrics(op, kwargs, response, elapsed_ms, failed=False, lane=group, wait_ms=wait_ms)
        return response

    def _record_call_metrics(
        self,
        op: str,
        kwargs: Json,
        response: Json | None,
        elapsed_ms: float,
        *,
        failed: bool,
        backpressure: bool = False,
        lane: str = "control",
        wait_ms: float = 0.0,
    ) -> None:
        with self._metrics_lock:
            self._commands_total += 1
            self._lane_commands_total[lane] = self._lane_commands_total.get(lane, 0) + 1
            self._lane_wait_ms_total[lane] = self._lane_wait_ms_total.get(lane, 0.0) + max(0.0, wait_ms)
            self._lane_wait_ms_max[lane] = max(self._lane_wait_ms_max.get(lane, 0.0), max(0.0, wait_ms))
            self._op_commands_total[op] = self._op_commands_total.get(op, 0) + 1
            self._op_latency_ms_total[op] = self._op_latency_ms_total.get(op, 0.0) + max(0.0, elapsed_ms)
            self._op_latency_ms_max[op] = max(self._op_latency_ms_max.get(op, 0.0), max(0.0, elapsed_ms))
            if failed:
                self._commands_failed_total += 1
                if "timed out" in str(response or "").lower() or elapsed_ms >= self.request_timeout_ms:
                    self._timeouts_total += 1
            if backpressure:
                self._backpressure_rejections_total += 1
            if response:
                serialization_ms = self._nested_float(
                    response,
                    "serialization_time_ms",
                    "serialization_ms",
                    "serialization_time",
                )
                engine_ms = self._nested_float(
                    response,
                    "rust_engine_time_ms",
                    "engine_ms",
                    "rust_engine_ms",
                )
                self._serialization_ms_total += serialization_ms
                self._serialization_ms_max = max(self._serialization_ms_max, serialization_ms)
                self._rust_engine_ms_total += engine_ms
                self._rust_engine_ms_max = max(self._rust_engine_ms_max, engine_ms)
                scan_count = int(
                    self._nested_float(
                        response,
                        "scan_count",
                        "scan_stats.scanned_records",
                        "context_pack.recall_policy.scan_stats.scanned_records",
                    )
                    or 0
                )
                self._scan_count_total += scan_count
                cache_hit = bool(response.get("cache_hit") or response.get("cache_hit_used"))
                if cache_hit:
                    self._cache_hits_total += 1
                elif op in {"matrixark_scan_candidates", "matrixark_retrieve_context_pack"}:
                    self._cache_misses_total += 1
                selected_count = int(
                    self._nested_float(
                        response,
                        "selected_ref_count",
                        "context_pack.selected_ref_count",
                    )
                    or 0
                )
                if not selected_count and isinstance(response.get("context_pack"), dict):
                    refs = response["context_pack"].get("selected_refs") or response["context_pack"].get("remote_context_refs") or []
                    if isinstance(refs, list):
                        selected_count = len(refs)
                self._selected_refs_total += selected_count
                dropped_count = int(
                    self._nested_float(
                        response,
                        "dropped_ref_count",
                        "context_pack.dropped_ref_count",
                    )
                    or 0
                )
                if not dropped_count and isinstance(response.get("context_pack"), dict):
                    dropped = response["context_pack"].get("dropped_refs")
                    if isinstance(dropped, dict):
                        reasons = dropped.get("reason_counts")
                        if isinstance(reasons, dict):
                            dropped_count = sum(int(value or 0) for value in reasons.values())
                self._dropped_refs_total += dropped_count
            self._last_latency_ms = elapsed_ms
            self._max_observed_latency_ms = max(self._max_observed_latency_ms, elapsed_ms)
            self._latency_samples_ms.append(elapsed_ms)
            if len(self._latency_samples_ms) > 2048:
                del self._latency_samples_ms[: len(self._latency_samples_ms) - 2048]
            lane_samples = self._lane_latency_samples_ms.setdefault(lane, [])
            lane_samples.append(elapsed_ms)
            if len(lane_samples) > 1024:
                del lane_samples[: len(lane_samples) - 1024]
            if response and response.get("ok"):
                count = int(response.get("count") or 0)
                if op in {"put_string", "hset"}:
                    self._records_written_total += 1
                    self._count_context_record(kwargs.get("value"))
                elif op in {"batch_hset", "matrixark_append_records", "matrixark_batch_append_records"}:
                    compact_entries = kwargs.get("entries_compact") or []
                    entries_for_key = kwargs.get("entries_for_key") or []
                    entries = kwargs.get("entries") or []
                    self._records_written_total += count or len(compact_entries) or len(entries_for_key) or len(entries)
                    for entry in entries:
                        if isinstance(entry, dict):
                            self._count_context_record(entry.get("value"))
                    for entry in compact_entries:
                        if isinstance(entry, (list, tuple)) and len(entry) >= 3:
                            self._count_context_record(entry[2])
                    for entry in entries_for_key:
                        if isinstance(entry, (list, tuple)) and len(entry) >= 2:
                            self._count_context_record(entry[1])
                elif op in {"get_string", "hget"}:
                    self._records_read_total += 1
                elif op in {"batch_hget", "hgetall", "scan_hash"}:
                    self._records_read_total += count
                elif op == "matrixark_publish_visibility":
                    visibility_keys = kwargs.get("visibility_keys") if isinstance(kwargs, dict) else []
                    key_count = len(visibility_keys) if isinstance(visibility_keys, list) else 0
                    index_bytes = int(
                        self._nested_float(
                            response,
                            "matrixark_visibility_index_bytes",
                            "extra.matrixark_visibility_index_bytes",
                            "count",
                        )
                        or 0
                    )
                    full_shard = bool(
                        response.get("matrixark_visibility_full_shard")
                        or (isinstance(response.get("extra"), dict) and response["extra"].get("matrixark_visibility_full_shard"))
                        or key_count == 0
                    )
                    self._publish_visibility_calls_total += 1
                    self._publish_visibility_keys_total += key_count
                    self._publish_visibility_full_shard_total += 1 if full_shard else 0
                    self._publish_visibility_index_bytes_total += index_bytes
                    self._publish_visibility_last_key_count = key_count
                    self._publish_visibility_last_index_bytes = index_bytes

    @staticmethod
    def _nested_float(payload: Json, *paths: str) -> float:
        return nested_float(payload, *paths)

    def _count_context_record(self, value: Any) -> None:
        count_context_record(self._context_record_counts, value)

    @staticmethod
    def _percentile(values: list[float], percentile_value: float) -> float:
        return percentile(values, percentile_value)

    def matrixark_retrieve_context_pack(
        self,
        *,
        count_key: str,
        record_hash_key: str,
        shard_size: int,
        request: Json,
    ) -> Json:
        return self._call_json(
            "matrixark_retrieve_context_pack",
            count_key=count_key,
            record_hash_key=record_hash_key,
            shard_size=shard_size,
            record_types=[
                "context_compression_event",
                "context_entity",
                "context_event",
                "context_index",
                "context_segment",
                "context_summary",
                "resource_chunk",
                "skill_section",
            ],
            return_index_records=False,
            scope=request.get("scope", {}),
            secondary_index_groups=request.get("secondary_index_groups", []),
            record=request,
        )

    def matrixark_publish_visibility(self, visibility_keys: list[str] | None = None) -> Json:
        return self._call_json("matrixark_publish_visibility", visibility_keys=visibility_keys or [])

    def metrics_snapshot(self) -> Json:
        return metrics_snapshot_helpers.metrics_snapshot(self)

    def _call(self, op: str, **kwargs: Any) -> str:
        response = self._call_json(op, **kwargs)
        return str(response.get("value", ""))

    def _string_cache_key_allowed(self, key: str) -> bool:
        return string_cache_key_allowed(self, key)

    def _string_cache_get(self, key: str) -> str | None:
        return string_cache_get(self, key)

    def _string_cache_put(self, key: str, value: str) -> None:
        string_cache_put(self, key, value)

    def _scan_hash_cache_get(self, key: str) -> Json | None:
        return scan_hash_cache_get(self, key)

    def _scan_hash_cache_put(self, key: str, response: Json) -> None:
        scan_hash_cache_put(self, key, response)

    def _scan_hash_cache_invalidate_keys(self, keys: Any) -> None:
        scan_hash_cache_invalidate_keys(self, keys)

    def _context_pack_response_cache_key(
        self,
        *,
        count_key: str,
        record_hash_key: str,
        shard_size: int,
        request: Json,
    ) -> str:
        return context_pack_response_cache_key(
            count_key=count_key,
            record_hash_key=record_hash_key,
            shard_size=shard_size,
            request=request,
        )

    def _mark_context_pack_response_cache_hit(self, response: Json) -> Json:
        return mark_context_pack_response_cache_hit(response)

    def _context_pack_response_cache_get(self, cache_key: str) -> Json | None:
        return context_pack_response_cache_get(self, cache_key)

    def _context_pack_response_cache_put(self, cache_key: str, response: Json) -> None:
        context_pack_response_cache_put(self, cache_key, response)

    def _context_pack_response_cache_clear(self) -> None:
        context_pack_response_cache_clear(self)

    def _context_pack_response_singleflight_enter(self, cache_key: str) -> tuple[Json, bool]:
        return context_pack_response_singleflight_enter(self, cache_key)

    def _context_pack_response_singleflight_finish(
        self,
        cache_key: str,
        inflight: Json,
        error: BaseException | None,
    ) -> None:
        context_pack_response_singleflight_finish(self, cache_key, inflight, error)

    def _context_pack_response_singleflight_wait(self, cache_key: str, inflight: Json) -> Json:
        return context_pack_response_singleflight_wait(self, cache_key, inflight)

    def put_string(self, key: str, value: str) -> None:
        self._call("put_string", key=key, value=value)
        self._string_cache_put(key, value)
        self._context_pack_response_cache_clear()

    def get_string(self, key: str) -> str:
        cached = self._string_cache_get(key)
        if cached is not None:
            return cached
        value = self._call("get_string", key=key)
        self._string_cache_put(key, value)
        return value

    def hset(self, key: str, field: str, value: str) -> None:
        self._call("hset", key=key, field=field, value=value)
        self._scan_hash_cache_invalidate_keys([key])
        self._context_pack_response_cache_clear()

    def hget(self, key: str, field: str) -> str:
        return self._call("hget", key=key, field=field)

    @staticmethod
    def _entries_for_single_key(compact_entries: list[list[str]]) -> tuple[str, list[list[str]]] | None:
        if not compact_entries:
            return None
        first_key = compact_entries[0][0]
        if not first_key:
            return None
        entries_for_key: list[list[str]] = []
        for entry in compact_entries:
            if len(entry) < 2 or entry[0] != first_key:
                return None
            value = entry[2] if len(entry) >= 3 else ""
            entries_for_key.append([entry[1], value])
        return first_key, entries_for_key

    def _call_hash_batch_json(
        self,
        op: str,
        compact_entries: list[list[str]],
        *,
        compact_read_response: bool = False,
    ) -> Json:
        same_key = self._entries_for_single_key(compact_entries)
        if same_key is not None:
            key, entries_for_key = same_key
            return self._call_json(
                op,
                key=key,
                entries_for_key=entries_for_key,
                compact_read_response=compact_read_response,
            )
        return self._call_json(op, entries_compact=compact_entries)

    @staticmethod
    def _batch_hget_records_from_response(compact_entries: list[list[str]], response: Json) -> list[Json]:
        records = response.get("records", [])
        if isinstance(records, list) and records:
            return records
        entries = response.get("entries", {})
        same_key = MatrixArkRustProxyClient._entries_for_single_key(compact_entries)
        if not isinstance(entries, dict) or same_key is None:
            return records if isinstance(records, list) else []
        key, _entries_for_key = same_key
        return [
            {
                "key": key,
                "field": str(entry[1]) if len(entry) >= 2 else "",
                "value": str(entries.get(str(entry[1]) if len(entry) >= 2 else "") or ""),
            }
            for entry in compact_entries
        ]

    def batch_hset(self, entries: list[Json]) -> None:
        if not entries:
            return
        compact_entries = [
            [str(entry.get("key") or ""), str(entry.get("field") or ""), str(entry.get("value") or "")]
            for entry in entries
            if isinstance(entry, dict)
        ]
        if (
            self._batch_hset_coalesce_enabled
            and self._shared_process_mode
            and len(compact_entries) >= self._batch_hset_coalesce_min_records
        ):
            self._coalesced_batch_hset(compact_entries)
            return
        self._call_hash_batch_json("batch_hset", compact_entries)
        self._scan_hash_cache_invalidate_keys(entry[0] for entry in compact_entries)
        self._context_pack_response_cache_clear()

    def _coalesced_batch_hset(self, compact_entries: list[list[str]]) -> None:
        coalesced_batch_hset(self, compact_entries)

    def _drain_batch_hset_coalescer(self) -> None:
        drain_batch_hset_coalescer(self)

    def matrixark_batch_append_records(
        self,
        entries: list[Json],
        *,
        count_key: str | None = None,
        count_value: str | None = None,
        append_options: Json | None = None,
    ) -> None:
        if not entries and not count_key:
            return
        compact_entries = [
            [str(entry.get("key") or ""), str(entry.get("field") or ""), str(entry.get("value") or "")]
            for entry in entries
            if isinstance(entry, dict)
        ]
        append_options = append_options or {}
        if (
            self._append_coalesce_enabled
            and self._shared_process_mode
            and len(compact_entries) >= self._append_coalesce_min_records
        ):
            self._coalesced_matrixark_batch_append_records(
                compact_entries,
                count_key=count_key or "",
                count_value=count_value or "",
                append_options=append_options,
            )
            return
        self._call_json(
            "matrixark_batch_append_records",
            entries_compact=compact_entries,
            key=count_key or "",
            value=count_value or "",
            append_options=append_options,
        )
        self._scan_hash_cache_invalidate_keys(entry[0] for entry in compact_entries)
        self._context_pack_response_cache_clear()
        if count_key:
            self._string_cache_put(count_key, count_value or "")

    @staticmethod
    def _append_options_signature(append_options: Json) -> str:
        return append_options_signature(append_options)

    @staticmethod
    def _max_count_value(values: list[str]) -> str:
        return max_count_value(values)

    def _coalesced_matrixark_batch_append_records(
        self,
        compact_entries: list[list[str]],
        *,
        count_key: str,
        count_value: str,
        append_options: Json,
    ) -> None:
        coalesced_matrixark_batch_append_records(
            self,
            compact_entries,
            count_key=count_key,
            count_value=count_value,
            append_options=append_options,
        )

    def _drain_append_coalescer(self) -> None:
        drain_append_coalescer(self)

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

    def matrixark_retrieve_context_pack(
        self,
        *,
        count_key: str,
        record_hash_key: str,
        shard_size: int,
        request: Json,
    ) -> Json:
        cache_key = self._context_pack_response_cache_key(
            count_key=count_key,
            record_hash_key=record_hash_key,
            shard_size=shard_size,
            request=request,
        )
        cached = self._context_pack_response_cache_get(cache_key)
        if cached is not None:
            return cached
        inflight, leader = self._context_pack_response_singleflight_enter(cache_key)
        if not leader:
            return self._context_pack_response_singleflight_wait(cache_key, inflight)
        error: BaseException | None = None
        try:
            response = self._call_json(
                "matrixark_retrieve_context_pack",
                count_key=count_key,
                record_hash_key=record_hash_key,
                shard_size=shard_size,
                record_types=[
                    "context_compression_event",
                    "context_entity",
                    "context_event",
                    "context_index",
                    "context_segment",
                    "context_summary",
                    "resource_chunk",
                    "skill_section",
                ],
                return_index_records=False,
                scope=request.get("scope", {}),
                secondary_index_groups=request.get("secondary_index_groups", []),
                record=request,
                top_level_response=True,
            )
            result = response
            value = response.get("value")
            if isinstance(value, str) and value:
                decoded = json.loads(value)
                if isinstance(decoded, dict):
                    result = decoded
            self._context_pack_response_cache_put(cache_key, result)
            return result
        except BaseException as exc:
            error = exc
            raise
        finally:
            self._context_pack_response_singleflight_finish(cache_key, inflight, error)

    def batch_hget(self, entries: list[Json]) -> list[Json]:
        if not entries:
            return []
        compact_entries = [
            [str(entry.get("key") or ""), str(entry.get("field") or ""), ""]
            for entry in entries
            if isinstance(entry, dict)
        ]
        if (
            self._batch_hget_coalesce_enabled
            and self._shared_process_mode
            and len(compact_entries) >= self._batch_hget_coalesce_min_records
        ):
            return self._coalesced_batch_hget(compact_entries)
        response = self._call_hash_batch_json(
            "batch_hget",
            compact_entries,
            compact_read_response=True,
        )
        return self._batch_hget_records_from_response(compact_entries, response)

    def _coalesced_batch_hget(self, compact_entries: list[list[str]]) -> list[Json]:
        return coalesced_batch_hget(self, compact_entries)

    def _drain_batch_hget_coalescer(self) -> None:
        drain_batch_hget_coalescer(self)

    @staticmethod
    def _assign_coalesced_batch_hget_by_key(pending: list[Json], rows: list[Json]) -> None:
        assign_coalesced_batch_hget_by_key(pending, rows)

    def scan_hash(self, key: str) -> Json:
        cached = self._scan_hash_cache_get(key)
        if cached is not None:
            return cached
        response = self._call_json("scan_hash", key=key)
        self._scan_hash_cache_put(key, response)
        return response

    def matrixark_scan_candidates(
        self,
        *,
        count_key: str,
        record_hash_key: str,
        shard_size: int,
        scope: Json,
        record_types: list[str],
        secondary_index_groups: list[list[str]],
        selected_node_hashes: list[int],
    ) -> Json:
        return self._call_json(
            "matrixark_scan_candidates",
            count_key=count_key,
            record_hash_key=record_hash_key,
            shard_size=shard_size,
            scope=scope,
            record_types=record_types,
            secondary_index_groups=secondary_index_groups,
            selected_node_hashes=selected_node_hashes,
        )

    def metrics_prometheus(self) -> str:
        return str(self._call_json("metrics_prometheus").get("prometheus", ""))

    def health(self) -> Json:
        return self._call_json("health")

    def readiness(self) -> Json:
        return self._call_json("readiness")

    def shutdown(self) -> None:
        try:
            self._call_json("shutdown")
        finally:
            self.close()


MatrixArkRustCliClient = MatrixArkRustProxyClient
