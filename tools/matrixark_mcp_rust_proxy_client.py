#!/usr/bin/env python3
"""Rust proxy client for MatrixArk TemporalStore adapters."""

from __future__ import annotations

import json
import os
import queue
import subprocess
import threading
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any

try:
    from tools.matrixark_mcp_core import Json, MatrixArkError
    from tools.matrixark_mcp_rust_proxy_lanes import build_lane_pools
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, MatrixArkError
    from matrixark_mcp_rust_proxy_lanes import build_lane_pools


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
        self._backpressure_timeout_s = max(
            0.05,
            int(
                os.environ.get(
                    "MATRIXARK_RUST_PROXY_BACKPRESSURE_TIMEOUT_MS",
                    os.environ.get("MATRIXARK_RUST_GATEWAY_BACKPRESSURE_TIMEOUT_MS", str(request_timeout_ms)),
                )
            )
            / 1000.0,
        )
        self._write_lane_count = max(1, int(os.environ.get("MATRIXARK_RUST_PROXY_WRITE_LANES", "4")))
        self._read_lane_count = max(1, int(os.environ.get("MATRIXARK_RUST_PROXY_READ_LANES", "4")))
        # Native ContextPack assembly should not over-provision proxy processes
        # by default: cold process startup used to leak into p95/p99 on small
        # retrieve runs. Match read lanes unless operators explicitly widen it.
        self._pack_lane_count = max(1, int(os.environ.get("MATRIXARK_RUST_PROXY_PACK_LANES", str(self._read_lane_count))))
        self._control_lane_count = max(1, int(os.environ.get("MATRIXARK_RUST_PROXY_CONTROL_LANES", "1")))
        self._shared_process_mode = os.environ.get("MATRIXARK_RUST_PROXY_SHARED_PROCESS", "1").strip().lower() not in {"0", "false", "no"}
        self._dedicated_pack_lanes_enabled = (
            os.environ.get("MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES", "1").strip().lower()
            not in {"0", "false", "no"}
        )
        self._batch_hset_coalesce_enabled = (
            os.environ.get("MATRIXARK_RUST_PROXY_BATCH_HSET_COALESCE", "1").strip().lower()
            not in {"0", "false", "no"}
        )
        self._batch_hset_coalesce_max_batches = max(
            1, int(os.environ.get("MATRIXARK_RUST_PROXY_BATCH_HSET_COALESCE_MAX_BATCHES", "32"))
        )
        self._batch_hset_coalesce_min_records = max(
            1, int(os.environ.get("MATRIXARK_RUST_PROXY_BATCH_HSET_COALESCE_MIN_RECORDS", "16"))
        )
        self._batch_hset_coalesce_wait_s = max(
            0.0,
            float(os.environ.get("MATRIXARK_RUST_PROXY_BATCH_HSET_COALESCE_WAIT_MS", "0")) / 1000.0,
        )
        self._batch_hget_coalesce_enabled = (
            os.environ.get("MATRIXARK_RUST_PROXY_BATCH_HGET_COALESCE", "1").strip().lower()
            not in {"0", "false", "no"}
        )
        self._batch_hget_coalesce_max_batches = max(
            1, int(os.environ.get("MATRIXARK_RUST_PROXY_BATCH_HGET_COALESCE_MAX_BATCHES", "32"))
        )
        self._batch_hget_coalesce_min_records = max(
            1, int(os.environ.get("MATRIXARK_RUST_PROXY_BATCH_HGET_COALESCE_MIN_RECORDS", "16"))
        )
        self._batch_hget_coalesce_wait_s = max(
            0.0,
            float(os.environ.get("MATRIXARK_RUST_PROXY_BATCH_HGET_COALESCE_WAIT_MS", "1.0")) / 1000.0,
        )
        self._append_coalesce_enabled = (
            os.environ.get("MATRIXARK_RUST_PROXY_APPEND_COALESCE", "1").strip().lower()
            not in {"0", "false", "no"}
        )
        self._append_coalesce_max_batches = max(
            1, int(os.environ.get("MATRIXARK_RUST_PROXY_APPEND_COALESCE_MAX_BATCHES", "32"))
        )
        self._append_coalesce_min_records = max(
            1, int(os.environ.get("MATRIXARK_RUST_PROXY_APPEND_COALESCE_MIN_RECORDS", "16"))
        )
        self._append_coalesce_wait_s = max(
            0.0,
            float(os.environ.get("MATRIXARK_RUST_PROXY_APPEND_COALESCE_WAIT_MS", "0.0")) / 1000.0,
        )
        self._string_cache_enabled = (
            os.environ.get("MATRIXARK_RUST_PROXY_STRING_CACHE", "1").strip().lower()
            not in {"0", "false", "no"}
        )
        self._scan_hash_cache_enabled = (
            os.environ.get("MATRIXARK_RUST_PROXY_SCAN_HASH_CACHE", "1").strip().lower()
            not in {"0", "false", "no"}
        )
        self._scan_hash_cache_max_entries = max(
            1, int(os.environ.get("MATRIXARK_RUST_PROXY_SCAN_HASH_CACHE_MAX_ENTRIES", "1024"))
        )
        self._context_pack_response_cache_enabled = (
            os.environ.get("MATRIXARK_RUST_PROXY_CONTEXT_PACK_CLIENT_CACHE", "1").strip().lower()
            not in {"0", "false", "no"}
        )
        self._context_pack_response_cache_max_entries = max(
            1, int(os.environ.get("MATRIXARK_RUST_PROXY_CONTEXT_PACK_CLIENT_CACHE_MAX_ENTRIES", "256"))
        )
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
        self._metrics_lock = threading.Lock()
        self._commands_total = 0
        self._commands_failed_total = 0
        self._records_written_total = 0
        self._records_read_total = 0
        self._backpressure_rejections_total = 0
        self._timeouts_total = 0
        self._last_latency_ms = 0.0
        self._max_observed_latency_ms = 0.0
        self._latency_samples_ms: list[float] = []
        self._lane_latency_samples_ms: dict[str, list[float]] = {lane: [] for lane in self._lane_worker_counts}
        self._lane_commands_total: dict[str, int] = {lane: 0 for lane in self._lane_worker_counts}
        self._lane_wait_ms_total: dict[str, float] = {lane: 0.0 for lane in self._lane_worker_counts}
        self._lane_wait_ms_max: dict[str, float] = {lane: 0.0 for lane in self._lane_worker_counts}
        self._op_commands_total: dict[str, int] = {}
        self._op_latency_ms_total: dict[str, float] = {}
        self._op_latency_ms_max: dict[str, float] = {}
        self._serialization_ms_total = 0.0
        self._serialization_ms_max = 0.0
        self._rust_engine_ms_total = 0.0
        self._rust_engine_ms_max = 0.0
        self._scan_count_total = 0
        self._cache_hits_total = 0
        self._cache_misses_total = 0
        self._selected_refs_total = 0
        self._dropped_refs_total = 0
        self._context_record_counts: dict[str, int] = {}
        self._publish_visibility_calls_total = 0
        self._publish_visibility_keys_total = 0
        self._publish_visibility_full_shard_total = 0
        self._publish_visibility_index_bytes_total = 0
        self._publish_visibility_last_key_count = 0
        self._publish_visibility_last_index_bytes = 0
        self._batch_hset_coalesce_lock = threading.Lock()
        self._batch_hset_coalesce_queue: list[Json] = []
        self._batch_hset_coalesce_active = False
        self._batch_hset_coalesced_batches_total = 0
        self._batch_hset_coalesced_calls_total = 0
        self._batch_hset_coalesced_records_total = 0
        self._batch_hset_coalesced_wait_ms_total = 0.0
        self._batch_hset_coalesced_wait_ms_max = 0.0
        self._batch_hget_coalesce_lock = threading.Lock()
        self._batch_hget_coalesce_queue: list[Json] = []
        self._batch_hget_coalesce_active = False
        self._batch_hget_coalesced_batches_total = 0
        self._batch_hget_coalesced_calls_total = 0
        self._batch_hget_coalesced_records_total = 0
        self._batch_hget_coalesced_wait_ms_total = 0.0
        self._batch_hget_coalesced_wait_ms_max = 0.0
        self._append_coalesce_lock = threading.Lock()
        self._append_coalesce_queue: list[Json] = []
        self._append_coalesce_active = False
        self._append_coalesced_batches_total = 0
        self._append_coalesced_calls_total = 0
        self._append_coalesced_records_total = 0
        self._append_coalesced_wait_ms_total = 0.0
        self._append_coalesced_wait_ms_max = 0.0
        self._string_cache_lock = threading.Lock()
        self._string_cache: dict[str, str] = {}
        self._string_cache_hits_total = 0
        self._string_cache_misses_total = 0
        self._string_cache_updates_total = 0
        self._scan_hash_cache_lock = threading.Lock()
        self._scan_hash_cache: OrderedDict[str, Json] = OrderedDict()
        self._scan_hash_cache_hits_total = 0
        self._scan_hash_cache_misses_total = 0
        self._scan_hash_cache_updates_total = 0
        self._scan_hash_cache_invalidations_total = 0
        self._context_pack_response_cache_lock = threading.Lock()
        self._context_pack_response_cache: OrderedDict[str, Json] = OrderedDict()
        self._context_pack_response_inflight: dict[str, Json] = {}
        self._context_pack_response_cache_hits_total = 0
        self._context_pack_response_cache_misses_total = 0
        self._context_pack_response_cache_updates_total = 0
        self._context_pack_response_cache_invalidations_total = 0
        self._context_pack_response_singleflight_waits_total = 0
        self._context_pack_response_singleflight_wait_ms_total = 0.0
        self._context_pack_response_singleflight_wait_ms_max = 0.0
        self._started_at = time.time()
        self._proc: subprocess.Popen[str] | None = None

    def close(self) -> None:
        seen: set[int] = set()
        for lanes in getattr(self, "_lanes", {}).values():
            for lane in lanes:
                proc = lane.get("proc")
                lane["proc"] = None
                if proc is None or id(proc) in seen:
                    continue
                seen.add(id(proc))
                self._close_proc(proc)
        proc = self._proc
        self._proc = None
        if proc is not None and id(proc) not in seen:
            self._close_proc(proc)

    @staticmethod
    def _close_proc(proc: subprocess.Popen[str]) -> None:
        if proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=2)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        for stream in (proc.stdin, proc.stdout, proc.stderr):
            try:
                if stream is not None:
                    stream.close()
            except Exception:
                pass

    def _ensure_lane_proc(self, lane: Json) -> subprocess.Popen[str]:
        proc = lane.get("proc")
        if proc is not None and proc.poll() is None:
            return proc
        if proc is not None:
            self._close_proc(proc)
        env = os.environ.copy()
        proxy_dir = str(Path(self.cli_path).resolve().parent)
        existing_ld_path = env.get("LD_LIBRARY_PATH", "")
        env["LD_LIBRARY_PATH"] = proxy_dir if not existing_ld_path else f"{proxy_dir}:{existing_ld_path}"
        lane["proc"] = subprocess.Popen(
            [self.cli_path, "--serve"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )
        return lane["proc"]

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
        if op in {
            "batch_hset",
            "matrixark_append_records",
            "matrixark_batch_append_records",
            "matrixark_batch_append_raw_ingestion_records",
            "hset",
            "put_string",
            "write_matrixark_record",
            "write_matrixark_records",
        }:
            return "write"
        if op in {"matrixark_retrieve_context_pack"}:
            return "pack"
        if op in {"batch_hget", "hgetall", "scan_hash", "hget", "get_string", "read_matrixark_record", "read_matrixark_records"}:
            return "read"
        return "control"

    def _pack_lane_sticky_index(self, lanes: list[Json], kwargs: Json) -> int | None:
        if not lanes or len(lanes) <= 1:
            return None
        request = kwargs.get("record")
        if isinstance(request, dict):
            query_id = request.get("query_id")
            if isinstance(query_id, int):
                return query_id % len(lanes)
            try:
                if query_id is not None:
                    return int(str(query_id)) % len(lanes)
            except (TypeError, ValueError):
                pass
        query = request.get("query") if isinstance(request, dict) else ""
        ranking = request.get("ranking") if isinstance(request, dict) else {}
        sticky_payload = {
            "count_key": kwargs.get("count_key"),
            "record_hash_key": kwargs.get("record_hash_key"),
            "scope": kwargs.get("scope"),
            "secondary_index_groups": kwargs.get("secondary_index_groups"),
            "query": query,
            "max_selected_refs": ranking.get("max_selected_refs") if isinstance(ranking, dict) else None,
        }
        try:
            encoded = json.dumps(sticky_payload, sort_keys=True, separators=(",", ":")).encode()
        except Exception:
            return None
        digest = hashlib.blake2b(encoded, digest_size=8).digest()
        return int.from_bytes(digest, "big") % len(lanes)

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
        for path in paths:
            current: Any = payload
            for part in path.split("."):
                if not isinstance(current, dict) or part not in current:
                    current = None
                    break
                current = current[part]
            if current is None:
                continue
            try:
                return float(current)
            except (TypeError, ValueError):
                continue
        return 0.0

    def _count_context_record(self, value: Any) -> None:
        if not isinstance(value, str) or not value.startswith("{"):
            return
        if '"record_type"' not in value:
            return
        try:
            payload = json.loads(value)
        except Exception:
            return
        record_type = str(payload.get("record_type") or "")
        if not record_type:
            return
        self._context_record_counts[record_type] = self._context_record_counts.get(record_type, 0) + 1

    @staticmethod
    def _percentile(values: list[float], percentile: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, math.ceil(percentile * len(ordered)) - 1))
        return ordered[index]

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
        with self._metrics_lock:
            elapsed_s = max(0.001, time.time() - self._started_at)
            samples = list(self._latency_samples_ms)
            context_counts = dict(sorted(self._context_record_counts.items()))
            lane_samples = {lane: list(values) for lane, values in self._lane_latency_samples_ms.items()}
            lane_metrics = {
                lane: {
                    "workers": self._lane_worker_counts.get(lane, 0),
                    "commands_total": self._lane_commands_total.get(lane, 0),
                    "wait_ms_total": round(self._lane_wait_ms_total.get(lane, 0.0), 3),
                    "wait_ms_max": round(self._lane_wait_ms_max.get(lane, 0.0), 3),
                    "queue_wait_ms_total": round(self._lane_wait_ms_total.get(lane, 0.0), 3),
                    "queue_wait_ms_max": round(self._lane_wait_ms_max.get(lane, 0.0), 3),
                    "p95_latency_ms": round(self._percentile(values, 0.95), 3),
                    "p99_latency_ms": round(self._percentile(values, 0.99), 3),
                }
                for lane, values in lane_samples.items()
            }
            op_metrics = {
                op: {
                    "commands_total": count,
                    "latency_ms_total": round(self._op_latency_ms_total.get(op, 0.0), 3),
                    "latency_ms_avg": round(self._op_latency_ms_total.get(op, 0.0) / max(1, count), 3),
                    "latency_ms_max": round(self._op_latency_ms_max.get(op, 0.0), 3),
                }
                for op, count in sorted(self._op_commands_total.items())
            }
            return {
                "gateway_mode": "rust_native_proxy",
                "sdk_mode": "rust_native_proxy",
                "transport": "stdio",
                "proxy_path": self.proxy_path,
                "cli_path": self.cli_path,
                "shared_process_mode": self._shared_process_mode,
                "max_inflight": sum(self._lane_worker_counts.get(group, 0) for group in ("write", "read", "pack", "control")),
                "lane_pool": {
                    "write": self._lane_worker_counts.get("write", 0),
                    "read": self._lane_worker_counts.get("read", 0),
                    "pack": self._lane_worker_counts.get("pack", 0),
                    "control": self._lane_worker_counts.get("control", 0),
                },
                "lanes": lane_metrics,
                "write_pool_size": self._lane_worker_counts.get("write", 0),
                "read_pool_size": self._lane_worker_counts.get("read", 0),
                "pack_pool_size": self._lane_worker_counts.get("pack", 0),
                "control_pool_size": self._lane_worker_counts.get("control", 0),
                "write_pool_enabled": self._lane_worker_counts.get("write", 0) > 1,
                "read_pool_enabled": self._lane_worker_counts.get("read", 0) > 1,
                "pack_pool_enabled": self._lane_worker_counts.get("pack", 0) > 1,
                "backpressure_timeout_ms": int(self._backpressure_timeout_s * 1000),
                "commands_total": self._commands_total,
                "commands_failed_total": self._commands_failed_total,
                "timeouts_total": self._timeouts_total,
                "qps": round(self._commands_total / elapsed_s, 6),
                "records_written_total": self._records_written_total,
                "records_read_total": self._records_read_total,
                "backpressure_rejections_total": self._backpressure_rejections_total,
                "proxy_queue_wait_ms_total": round(sum(self._lane_wait_ms_total.values()), 3),
                "proxy_queue_wait_ms_max": round(max(self._lane_wait_ms_max.values()) if self._lane_wait_ms_max else 0.0, 3),
                "serialization_ms_total": round(self._serialization_ms_total, 3),
                "serialization_ms_max": round(self._serialization_ms_max, 3),
                "rust_engine_ms_total": round(self._rust_engine_ms_total, 3),
                "rust_engine_ms_max": round(self._rust_engine_ms_max, 3),
                "scan_count_total": self._scan_count_total,
                "cache_hits_total": self._cache_hits_total,
                "cache_misses_total": self._cache_misses_total,
                "selected_refs_total": self._selected_refs_total,
                "dropped_refs_total": self._dropped_refs_total,
                "publish_visibility": {
                    "calls_total": self._publish_visibility_calls_total,
                    "keys_total": self._publish_visibility_keys_total,
                    "keys_avg": round(
                        self._publish_visibility_keys_total / max(1, self._publish_visibility_calls_total),
                        3,
                    ),
                    "full_shard_total": self._publish_visibility_full_shard_total,
                    "index_bytes_total": self._publish_visibility_index_bytes_total,
                    "index_bytes_avg": round(
                        self._publish_visibility_index_bytes_total / max(1, self._publish_visibility_calls_total),
                        3,
                    ),
                    "last_key_count": self._publish_visibility_last_key_count,
                    "last_index_bytes": self._publish_visibility_last_index_bytes,
                },
                "batch_hset_coalescing": {
                    "enabled": self._batch_hset_coalesce_enabled,
                    "max_batches": self._batch_hset_coalesce_max_batches,
                    "min_records": self._batch_hset_coalesce_min_records,
                    "wait_ms": round(self._batch_hset_coalesce_wait_s * 1000.0, 3),
                    "batches_total": self._batch_hset_coalesced_batches_total,
                    "calls_total": self._batch_hset_coalesced_calls_total,
                    "records_total": self._batch_hset_coalesced_records_total,
                    "wait_ms_total": round(self._batch_hset_coalesced_wait_ms_total, 3),
                    "wait_ms_max": round(self._batch_hset_coalesced_wait_ms_max, 3),
                },
                "batch_hget_coalescing": {
                    "enabled": self._batch_hget_coalesce_enabled,
                    "max_batches": self._batch_hget_coalesce_max_batches,
                    "min_records": self._batch_hget_coalesce_min_records,
                    "wait_ms": round(self._batch_hget_coalesce_wait_s * 1000.0, 3),
                    "batches_total": self._batch_hget_coalesced_batches_total,
                    "calls_total": self._batch_hget_coalesced_calls_total,
                    "records_total": self._batch_hget_coalesced_records_total,
                    "wait_ms_total": round(self._batch_hget_coalesced_wait_ms_total, 3),
                    "wait_ms_max": round(self._batch_hget_coalesced_wait_ms_max, 3),
                },
                "matrixark_append_coalescing": {
                    "enabled": self._append_coalesce_enabled,
                    "max_batches": self._append_coalesce_max_batches,
                    "min_records": self._append_coalesce_min_records,
                    "wait_ms": round(self._append_coalesce_wait_s * 1000.0, 3),
                    "batches_total": self._append_coalesced_batches_total,
                    "calls_total": self._append_coalesced_calls_total,
                    "records_total": self._append_coalesced_records_total,
                    "wait_ms_total": round(self._append_coalesced_wait_ms_total, 3),
                    "wait_ms_max": round(self._append_coalesced_wait_ms_max, 3),
                },
                "string_cache": {
                    "enabled": self._string_cache_enabled,
                    "entries": len(self._string_cache),
                    "hits_total": self._string_cache_hits_total,
                    "misses_total": self._string_cache_misses_total,
                    "updates_total": self._string_cache_updates_total,
                    "scope": "record_count_and_record_index_keys",
                },
                "scan_hash_cache": {
                    "enabled": self._scan_hash_cache_enabled,
                    "max_entries": self._scan_hash_cache_max_entries,
                    "entries": len(self._scan_hash_cache),
                    "hits_total": self._scan_hash_cache_hits_total,
                    "misses_total": self._scan_hash_cache_misses_total,
                    "updates_total": self._scan_hash_cache_updates_total,
                    "invalidations_total": self._scan_hash_cache_invalidations_total,
                    "scope": "hash_key_with_write_invalidation",
                },
                "context_pack_response_cache": {
                    "enabled": self._context_pack_response_cache_enabled,
                    "max_entries": self._context_pack_response_cache_max_entries,
                    "entries": len(self._context_pack_response_cache),
                    "hits_total": self._context_pack_response_cache_hits_total,
                    "misses_total": self._context_pack_response_cache_misses_total,
                    "updates_total": self._context_pack_response_cache_updates_total,
                    "invalidations_total": self._context_pack_response_cache_invalidations_total,
                    "singleflight_waits_total": self._context_pack_response_singleflight_waits_total,
                    "singleflight_wait_ms_total": round(self._context_pack_response_singleflight_wait_ms_total, 3),
                    "singleflight_wait_ms_max": round(self._context_pack_response_singleflight_wait_ms_max, 3),
                    "scope": "native_context_pack_request_envelope_with_write_invalidation",
                },
                "last_latency_ms": round(self._last_latency_ms, 3),
                "latency_ms_sum": round(sum(samples), 3),
                "latency_ms_count": len(samples),
                "latency_ms_max": round(max(samples) if samples else 0.0, 3),
                "latency_buckets": {str(int(bucket) if bucket != float("inf") else "+Inf"): sum(1 for value in samples if value <= bucket) for bucket in MatrixArkServiceMetrics.LATENCY_BUCKETS_MS},
                "p95_latency_ms": round(self._percentile(samples, 0.95), 3),
                "p99_latency_ms": round(self._percentile(samples, 0.99), 3),
                "max_observed_latency_ms": round(self._max_observed_latency_ms, 3),
                "matrixark_context_records_total": sum(context_counts.values()),
                "matrixark_context_records_by_type": context_counts,
                "op_metrics": op_metrics,
                "process_per_operation_enabled": False,
                "single_shot_mode": "debug_only",
                "native_proxy": True,
                "direct_sdk_bridge": False,
                "pure_embedded_direct_sdk": False,
                "supports_health": True,
                "supports_readiness": True,
                "supports_metrics": True,
                "supports_batch_append": True,
                "supports_matrixark_batch_append_records": True,
                "supports_matrixark_retrieve_context_pack": True,
                "supports_compact_secondary_index_lookup": True,
                "supports_placement_key_candidate_fetch": True,
                "supports_context_pack_telemetry": True,
                "supports_native_append_queue": True,
                "supports_coalesced_writes": True,
                "supports_coalesced_reads": True,
                "supports_coalesced_appends": True,
                "supports_placement_key_routing": True,
                "supports_prefix_scan": True,
                "supports_graceful_shutdown": True,
                "structured_errors": True,
                "matrixark_batch_append_wire_format": "entries_compact",
            }

    def _call(self, op: str, **kwargs: Any) -> str:
        response = self._call_json(op, **kwargs)
        return str(response.get("value", ""))

    def _string_cache_key_allowed(self, key: str) -> bool:
        return self._string_cache_enabled and str(key).endswith((":record_count", ":record_index"))

    def _string_cache_get(self, key: str) -> str | None:
        if not self._string_cache_key_allowed(key):
            return None
        with self._string_cache_lock:
            value = self._string_cache.get(key)
        with self._metrics_lock:
            if value is None:
                self._string_cache_misses_total += 1
            else:
                self._string_cache_hits_total += 1
        return value

    def _string_cache_put(self, key: str, value: str) -> None:
        if not self._string_cache_key_allowed(key):
            return
        with self._string_cache_lock:
            self._string_cache[key] = str(value)
        with self._metrics_lock:
            self._string_cache_updates_total += 1

    def _scan_hash_cache_get(self, key: str) -> Json | None:
        if not self._scan_hash_cache_enabled:
            return None
        with self._scan_hash_cache_lock:
            cached = self._scan_hash_cache.get(key)
            if cached is None:
                value = None
            else:
                self._scan_hash_cache.move_to_end(key)
                value = copy.deepcopy(cached)
        with self._metrics_lock:
            if value is None:
                self._scan_hash_cache_misses_total += 1
            else:
                self._scan_hash_cache_hits_total += 1
        return value

    def _scan_hash_cache_put(self, key: str, response: Json) -> None:
        if not self._scan_hash_cache_enabled:
            return
        with self._scan_hash_cache_lock:
            self._scan_hash_cache[key] = copy.deepcopy(response)
            self._scan_hash_cache.move_to_end(key)
            while len(self._scan_hash_cache) > self._scan_hash_cache_max_entries:
                self._scan_hash_cache.popitem(last=False)
        with self._metrics_lock:
            self._scan_hash_cache_updates_total += 1

    def _scan_hash_cache_invalidate_keys(self, keys: Iterable[str]) -> None:
        if not self._scan_hash_cache_enabled:
            return
        removed = 0
        with self._scan_hash_cache_lock:
            for key in set(str(item) for item in keys if str(item)):
                if self._scan_hash_cache.pop(key, None) is not None:
                    removed += 1
        if removed:
            with self._metrics_lock:
                self._scan_hash_cache_invalidations_total += removed

    def _context_pack_response_cache_key(
        self,
        *,
        count_key: str,
        record_hash_key: str,
        shard_size: int,
        request: Json,
    ) -> str:
        ranking = request.get("ranking") if isinstance(request, dict) else {}
        payload = {
            "count_key": count_key,
            "record_hash_key": record_hash_key,
            "shard_size": int(shard_size),
            "scope": request.get("scope", {}) if isinstance(request, dict) else {},
            "secondary_index_groups": request.get("secondary_index_groups", []) if isinstance(request, dict) else [],
            "query": request.get("query", "") if isinstance(request, dict) else "",
            "max_selected_refs": ranking.get("max_selected_refs") if isinstance(ranking, dict) else None,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
        return hashlib.blake2b(encoded, digest_size=16).hexdigest()

    def _mark_context_pack_response_cache_hit(self, response: Json) -> Json:
        cached = dict(response)
        cached["cache_hit"] = True
        cached["context_pack_response_cache_hit"] = True
        metrics = cached.get("retrieval_metrics")
        if isinstance(metrics, dict):
            metrics = dict(metrics)
            cached["retrieval_metrics"] = metrics
            metrics["cache_hit"] = True
            metrics["candidate_cache_hit"] = True
            metrics["context_pack_response_cache_hit"] = True
        pack = cached.get("context_pack")
        if isinstance(pack, dict):
            pack = dict(pack)
            cached["context_pack"] = pack
            pack_metrics = pack.get("retrieval_metrics")
            if isinstance(pack_metrics, dict):
                pack_metrics = dict(pack_metrics)
                pack["retrieval_metrics"] = pack_metrics
                pack_metrics["cache_hit"] = True
                pack_metrics["candidate_cache_hit"] = True
                pack_metrics["context_pack_response_cache_hit"] = True
        return cached

    def _context_pack_response_cache_get(self, cache_key: str) -> Json | None:
        if not self._context_pack_response_cache_enabled:
            return None
        with self._context_pack_response_cache_lock:
            cached = self._context_pack_response_cache.get(cache_key)
            if cached is not None:
                self._context_pack_response_cache.move_to_end(cache_key)
        with self._metrics_lock:
            if cached is None:
                self._context_pack_response_cache_misses_total += 1
            else:
                self._context_pack_response_cache_hits_total += 1
        if cached is None:
            return None
        return self._mark_context_pack_response_cache_hit(cached)

    def _context_pack_response_cache_put(self, cache_key: str, response: Json) -> None:
        if not self._context_pack_response_cache_enabled:
            return
        with self._context_pack_response_cache_lock:
            self._context_pack_response_cache[cache_key] = copy.deepcopy(response)
            self._context_pack_response_cache.move_to_end(cache_key)
            while len(self._context_pack_response_cache) > self._context_pack_response_cache_max_entries:
                self._context_pack_response_cache.popitem(last=False)
        with self._metrics_lock:
            self._context_pack_response_cache_updates_total += 1

    def _context_pack_response_cache_clear(self) -> None:
        if not self._context_pack_response_cache_enabled:
            return
        with self._context_pack_response_cache_lock:
            removed = len(self._context_pack_response_cache)
            self._context_pack_response_cache.clear()
        if removed:
            with self._metrics_lock:
                self._context_pack_response_cache_invalidations_total += removed

    def _context_pack_response_singleflight_enter(self, cache_key: str) -> tuple[Json, bool]:
        if not self._context_pack_response_cache_enabled:
            return {"event": threading.Event(), "error": None}, True
        with self._context_pack_response_cache_lock:
            inflight = self._context_pack_response_inflight.get(cache_key)
            if inflight is not None:
                return inflight, False
            inflight = {"event": threading.Event(), "error": None}
            self._context_pack_response_inflight[cache_key] = inflight
            return inflight, True

    def _context_pack_response_singleflight_finish(
        self,
        cache_key: str,
        inflight: Json,
        error: BaseException | None,
    ) -> None:
        if not self._context_pack_response_cache_enabled:
            return
        with self._context_pack_response_cache_lock:
            current = self._context_pack_response_inflight.get(cache_key)
            if current is inflight:
                self._context_pack_response_inflight.pop(cache_key, None)
            inflight["error"] = error
            event = inflight.get("event")
            if isinstance(event, threading.Event):
                event.set()

    def _context_pack_response_singleflight_wait(self, cache_key: str, inflight: Json) -> Json:
        event = inflight.get("event")
        if not isinstance(event, threading.Event):
            raise MatrixArkError("invalid ContextPack singleflight state")
        started = time.perf_counter()
        timeout_s = max(self._backpressure_timeout_s, self.request_timeout_ms / 1000.0 + 2.0)
        if not event.wait(timeout=timeout_s):
            raise MatrixArkError(f"Rust TemporalStore ContextPack singleflight timed out after {timeout_s:.1f}s")
        wait_ms = (time.perf_counter() - started) * 1000.0
        with self._metrics_lock:
            self._context_pack_response_singleflight_waits_total += 1
            self._context_pack_response_singleflight_wait_ms_total += wait_ms
            self._context_pack_response_singleflight_wait_ms_max = max(
                self._context_pack_response_singleflight_wait_ms_max,
                wait_ms,
            )
        error = inflight.get("error")
        if error:
            raise error
        cached = self._context_pack_response_cache_get(cache_key)
        if cached is not None:
            return cached
        raise MatrixArkError("ContextPack singleflight completed without cached response")

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
        event = threading.Event()
        request: Json = {
            "entries_compact": compact_entries,
            "event": event,
            "error": None,
        }
        became_leader = False
        queued_at = time.perf_counter()
        with self._batch_hset_coalesce_lock:
            self._batch_hset_coalesce_queue.append(request)
            if not self._batch_hset_coalesce_active:
                self._batch_hset_coalesce_active = True
                became_leader = True
        if became_leader:
            self._drain_batch_hset_coalescer()
        else:
            timeout_s = max(self._backpressure_timeout_s, self.request_timeout_ms / 1000.0 + 2.0)
            if not event.wait(timeout=timeout_s):
                raise MatrixArkError(f"Rust TemporalStore batch_hset coalescer timed out after {timeout_s:.1f}s")
        wait_ms = (time.perf_counter() - queued_at) * 1000.0
        with self._metrics_lock:
            self._batch_hset_coalesced_wait_ms_total += wait_ms
            self._batch_hset_coalesced_wait_ms_max = max(self._batch_hset_coalesced_wait_ms_max, wait_ms)
        error = request.get("error")
        if error:
            raise error

    def _drain_batch_hset_coalescer(self) -> None:
        try:
            if self._batch_hset_coalesce_wait_s > 0:
                time.sleep(self._batch_hset_coalesce_wait_s)
            while True:
                with self._batch_hset_coalesce_lock:
                    pending = self._batch_hset_coalesce_queue[: self._batch_hset_coalesce_max_batches]
                    del self._batch_hset_coalesce_queue[: len(pending)]
                if not pending:
                    with self._batch_hset_coalesce_lock:
                        if not self._batch_hset_coalesce_queue:
                            self._batch_hset_coalesce_active = False
                            return
                    continue
                merged: list[list[str]] = []
                for item in pending:
                    merged.extend(item.get("entries_compact") or [])
                error: BaseException | None = None
                try:
                    self._call_hash_batch_json("batch_hset", merged)
                except BaseException as exc:
                    error = exc
                if error is None:
                    self._scan_hash_cache_invalidate_keys(entry[0] for entry in merged)
                    self._context_pack_response_cache_clear()
                with self._metrics_lock:
                    self._batch_hset_coalesced_batches_total += 1
                    self._batch_hset_coalesced_calls_total += len(pending)
                    self._batch_hset_coalesced_records_total += len(merged)
                for item in pending:
                    item["error"] = error
                    item["event"].set()
                if error is not None:
                    with self._batch_hset_coalesce_lock:
                        remaining = self._batch_hset_coalesce_queue
                        self._batch_hset_coalesce_queue = []
                        self._batch_hset_coalesce_active = False
                    for item in remaining:
                        item["error"] = error
                        item["event"].set()
                    return
        except BaseException as exc:
            with self._batch_hset_coalesce_lock:
                remaining = self._batch_hset_coalesce_queue
                self._batch_hset_coalesce_queue = []
                self._batch_hset_coalesce_active = False
            for item in remaining:
                item["error"] = exc
                item["event"].set()
            raise

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
        try:
            return json.dumps(append_options or {}, sort_keys=True, separators=(",", ":"), default=str)
        except Exception:
            return str(sorted((append_options or {}).items())) if isinstance(append_options, dict) else str(append_options)

    @staticmethod
    def _max_count_value(values: list[str]) -> str:
        numeric: list[int] = []
        for value in values:
            try:
                numeric.append(int(str(value)))
            except (TypeError, ValueError):
                continue
        if numeric:
            return str(max(numeric))
        return values[-1] if values else ""

    def _coalesced_matrixark_batch_append_records(
        self,
        compact_entries: list[list[str]],
        *,
        count_key: str,
        count_value: str,
        append_options: Json,
    ) -> None:
        event = threading.Event()
        request: Json = {
            "entries_compact": compact_entries,
            "count_key": count_key,
            "count_value": count_value,
            "append_options": append_options,
            "append_options_signature": self._append_options_signature(append_options),
            "event": event,
            "error": None,
        }
        became_leader = False
        queued_at = time.perf_counter()
        with self._append_coalesce_lock:
            self._append_coalesce_queue.append(request)
            if not self._append_coalesce_active:
                self._append_coalesce_active = True
                became_leader = True
        if became_leader:
            self._drain_append_coalescer()
        else:
            timeout_s = max(self._backpressure_timeout_s, self.request_timeout_ms / 1000.0 + 2.0)
            if not event.wait(timeout=timeout_s):
                raise MatrixArkError(f"Rust TemporalStore matrixark append coalescer timed out after {timeout_s:.1f}s")
        wait_ms = (time.perf_counter() - queued_at) * 1000.0
        with self._metrics_lock:
            self._append_coalesced_wait_ms_total += wait_ms
            self._append_coalesced_wait_ms_max = max(self._append_coalesced_wait_ms_max, wait_ms)
        error = request.get("error")
        if error:
            raise error

    def _drain_append_coalescer(self) -> None:
        try:
            if self._append_coalesce_wait_s > 0:
                time.sleep(self._append_coalesce_wait_s)
            while True:
                with self._append_coalesce_lock:
                    pending = self._append_coalesce_queue[: self._append_coalesce_max_batches]
                    del self._append_coalesce_queue[: len(pending)]
                if not pending:
                    with self._append_coalesce_lock:
                        if not self._append_coalesce_queue:
                            self._append_coalesce_active = False
                            return
                    continue
                grouped: dict[tuple[str, str], list[Json]] = {}
                for item in pending:
                    signature = (str(item.get("count_key") or ""), str(item.get("append_options_signature") or ""))
                    grouped.setdefault(signature, []).append(item)
                for items in grouped.values():
                    merged: list[list[str]] = []
                    count_values: list[str] = []
                    append_options = items[0].get("append_options") or {}
                    count_key = str(items[0].get("count_key") or "")
                    for item in items:
                        merged.extend(item.get("entries_compact") or [])
                        value = str(item.get("count_value") or "")
                        if value:
                            count_values.append(value)
                    count_value = self._max_count_value(count_values)
                    error: BaseException | None = None
                    try:
                        self._call_json(
                            "matrixark_batch_append_records",
                            entries_compact=merged,
                            key=count_key,
                            value=count_value,
                            append_options=append_options,
                        )
                    except BaseException as exc:
                        error = exc
                    if error is None and count_key:
                        self._string_cache_put(count_key, count_value)
                    if error is None:
                        self._scan_hash_cache_invalidate_keys(entry[0] for entry in merged)
                        self._context_pack_response_cache_clear()
                    with self._metrics_lock:
                        self._append_coalesced_batches_total += 1
                        self._append_coalesced_calls_total += len(items)
                        self._append_coalesced_records_total += len(merged)
                    for item in items:
                        item["error"] = error
                        item["event"].set()
                    if error is not None:
                        with self._append_coalesce_lock:
                            remaining = self._append_coalesce_queue
                            self._append_coalesce_queue = []
                            self._append_coalesce_active = False
                        for item in remaining:
                            item["error"] = error
                            item["event"].set()
                        return
        except BaseException as exc:
            with self._append_coalesce_lock:
                remaining = self._append_coalesce_queue
                self._append_coalesce_queue = []
                self._append_coalesce_active = False
            for item in remaining:
                item["error"] = exc
                item["event"].set()
            raise

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
        event = threading.Event()
        request: Json = {
            "entries_compact": compact_entries,
            "event": event,
            "error": None,
            "records": None,
        }
        became_leader = False
        queued_at = time.perf_counter()
        with self._batch_hget_coalesce_lock:
            self._batch_hget_coalesce_queue.append(request)
            if not self._batch_hget_coalesce_active:
                self._batch_hget_coalesce_active = True
                became_leader = True
        if became_leader:
            self._drain_batch_hget_coalescer()
        else:
            timeout_s = max(self._backpressure_timeout_s, self.request_timeout_ms / 1000.0 + 2.0)
            if not event.wait(timeout=timeout_s):
                raise MatrixArkError(f"Rust TemporalStore batch_hget coalescer timed out after {timeout_s:.1f}s")
        wait_ms = (time.perf_counter() - queued_at) * 1000.0
        with self._metrics_lock:
            self._batch_hget_coalesced_wait_ms_total += wait_ms
            self._batch_hget_coalesced_wait_ms_max = max(self._batch_hget_coalesced_wait_ms_max, wait_ms)
        error = request.get("error")
        if error:
            raise error
        records = request.get("records")
        return records if isinstance(records, list) else []

    def _drain_batch_hget_coalescer(self) -> None:
        try:
            if self._batch_hget_coalesce_wait_s > 0:
                time.sleep(self._batch_hget_coalesce_wait_s)
            while True:
                with self._batch_hget_coalesce_lock:
                    pending = self._batch_hget_coalesce_queue[: self._batch_hget_coalesce_max_batches]
                    del self._batch_hget_coalesce_queue[: len(pending)]
                if not pending:
                    with self._batch_hget_coalesce_lock:
                        if not self._batch_hget_coalesce_queue:
                            self._batch_hget_coalesce_active = False
                            return
                    continue
                merged: list[list[str]] = []
                for item in pending:
                    merged.extend(item.get("entries_compact") or [])
                error: BaseException | None = None
                rows: list[Json] = []
                try:
                    response = self._call_hash_batch_json(
                        "batch_hget",
                        merged,
                        compact_read_response=True,
                    )
                    rows = self._batch_hget_records_from_response(merged, response)
                except BaseException as exc:
                    error = exc
                if error is None:
                    if len(rows) == len(merged):
                        cursor = 0
                        ordered = True
                        for item in pending:
                            item_records: list[Json] = []
                            for key, field, _ in item.get("entries_compact") or []:
                                row = rows[cursor] if cursor < len(rows) else {}
                                cursor += 1
                                if (
                                    not isinstance(row, dict)
                                    or str(row.get("key") or "") != key
                                    or str(row.get("field") or "") != field
                                ):
                                    ordered = False
                                    break
                                item_records.append(row)
                            if not ordered:
                                break
                            item["records"] = item_records
                        if not ordered:
                            self._assign_coalesced_batch_hget_by_key(pending, rows)
                    else:
                        self._assign_coalesced_batch_hget_by_key(pending, rows)
                with self._metrics_lock:
                    self._batch_hget_coalesced_batches_total += 1
                    self._batch_hget_coalesced_calls_total += len(pending)
                    self._batch_hget_coalesced_records_total += len(merged)
                for item in pending:
                    item["error"] = error
                    item["event"].set()
                if error is not None:
                    with self._batch_hget_coalesce_lock:
                        remaining = self._batch_hget_coalesce_queue
                        self._batch_hget_coalesce_queue = []
                        self._batch_hget_coalesce_active = False
                    for item in remaining:
                        item["error"] = error
                        item["event"].set()
                    return
        except BaseException as exc:
            with self._batch_hget_coalesce_lock:
                remaining = self._batch_hget_coalesce_queue
                self._batch_hget_coalesce_queue = []
                self._batch_hget_coalesce_active = False
            for item in remaining:
                item["error"] = exc
                item["event"].set()
            raise

    @staticmethod
    def _assign_coalesced_batch_hget_by_key(pending: list[Json], rows: list[Json]) -> None:
        records_by_entry: dict[tuple[str, str], deque[Json]] = defaultdict(deque)
        for row in rows:
            if not isinstance(row, dict):
                continue
            records_by_entry[(str(row.get("key") or ""), str(row.get("field") or ""))].append(row)
        for item in pending:
            item_records: list[Json] = []
            for key, field, _ in item.get("entries_compact") or []:
                bucket = records_by_entry.get((key, field))
                if bucket:
                    item_records.append(bucket.popleft())
                else:
                    item_records.append({"key": key, "field": field, "value": ""})
            item["records"] = item_records

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
