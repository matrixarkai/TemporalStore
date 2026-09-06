#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Rust proxy client for MatrixArk TemporalStore adapters."""

from __future__ import annotations

import json
import queue
import select
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
    from tools.matrixark_mcp_rust_proxy_cache_mixin import MatrixArkRustProxyCacheMixin
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
        record_call_metrics,
    )
    from tools.matrixark_mcp_rust_proxy_process import (
        close_proxy_lanes,
        close_proxy_process,
        ensure_lane_process,
        proxy_stderr_tail,
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
    from matrixark_mcp_rust_proxy_cache_mixin import MatrixArkRustProxyCacheMixin
    import matrixark_mcp_rust_proxy_metrics_snapshot as metrics_snapshot_helpers
    from matrixark_mcp_rust_proxy_metrics_record import (
        count_context_record,
        nested_float,
        percentile,
        record_call_metrics,
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
        proxy_stderr_tail,
    )


class MatrixArkRustProxyClient(MatrixArkRustProxyCacheMixin):
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

    def _read_json_line(
        self,
        proc: subprocess.Popen[str],
        op: str,
        lane: Json | None = None,
        expected_request_id: str | None = None,
    ) -> Json:
        assert proc.stdout is not None
        deadline = time.monotonic() + max(2.0, self.request_timeout_ms / 1000.0 + 2.0)
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                # The drain thread owns proc.stderr; reading it here would race it and, before
                # the drain existed, could block. Quote what the drain captured instead.
                stderr = proxy_stderr_tail(lane)
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
                parsed = json.loads(line)
            except json.JSONDecodeError as exc:
                raise MatrixArkError(f"Rust TemporalStore {op} returned invalid JSON: {line[:200]!r}") from exc
            # The proxy answers strictly in order on one stdout, so the late response of a
            # request some earlier caller abandoned (its own timeout) would otherwise be read
            # as THIS request's answer -- and every later reply shifts one back, silently
            # serving the wrong data. Discard responses tagged for a different request; an
            # untagged response (older proxy binary) is accepted unchanged.
            if expected_request_id is not None:
                stale_id = parsed.get("client_request_id")
                if stale_id is not None and stale_id != expected_request_id:
                    continue
            return parsed
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
                # Tag the request so the reader can discard the late responses of requests a
                # previous caller abandoned on this lane (see _read_json_line).
                request_id = f"{id(lane)}-{time.monotonic_ns()}"
                lane_command = dict(command)
                lane_command["client_request_id"] = request_id
                payload = json.dumps(lane_command, separators=(",", ":")) + "\n"
                try:
                    proc.stdin.write(payload)
                    proc.stdin.flush()
                except BrokenPipeError as exc:
                    lane["proc"] = None
                    returncode = proc.poll()
                    stderr = proxy_stderr_tail(lane)
                    self._close_proc(proc)
                    detail = f"Rust TemporalStore {op} pipe closed"
                    if returncode is not None:
                        detail += f" after process exit ({returncode})"
                    if stderr:
                        detail += f": {stderr[-1000:]}"
                    raise MatrixArkError(detail) from exc
                response = self._read_json_line(proc, op, lane, expected_request_id=request_id)
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
        record_call_metrics(
            self,
            op,
            kwargs,
            response,
            elapsed_ms,
            failed=failed,
            backpressure=backpressure,
            lane=lane,
            wait_ms=wait_ms,
        )

    @staticmethod
    def _nested_float(payload: Json, *paths: str) -> float:
        return nested_float(payload, *paths)

    def _count_context_record(self, value: Any) -> None:
        count_context_record(self._context_record_counts, value)

    @staticmethod
    def _percentile(values: list[float], percentile_value: float) -> float:
        return percentile(values, percentile_value)

    def matrixark_publish_visibility(self, visibility_keys: list[str] | None = None) -> Json:
        return self._call_json("matrixark_publish_visibility", visibility_keys=visibility_keys or [])

    def metrics_snapshot(self) -> Json:
        return metrics_snapshot_helpers.metrics_snapshot(self)

    def _call(self, op: str, **kwargs: Any) -> str:
        response = self._call_json(op, **kwargs)
        return str(response.get("value", ""))

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

    def resource_blob_put(self, tenant_hash: int, payload_base64: str) -> Json:
        return self._call_json(
            "matrixark_resource_blob_put", key=str(int(tenant_hash)), value=payload_base64
        )

    def resource_blob_fetch(self, uri: str, *, offset: int = 0, length: int = 0) -> Json:
        return self._call_json(
            "matrixark_resource_blob_fetch", key=str(uri), blob_offset=int(offset), blob_length=int(length)
        )

    def resource_blob_sweep(self, tenant_hash: int, referenced_hashes: list[str], min_age_ms: int) -> Json:
        return self._call_json(
            "matrixark_resource_blob_sweep",
            key=str(int(tenant_hash)),
            blob_referenced_hashes=[str(item) for item in referenced_hashes],
            blob_min_age_ms=int(min_age_ms),
        )


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
        compact_entries: list[list[str]] = []
        routed_entries: list[Json] = []
        has_routes = False
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            key = str(entry.get("key") or "")
            field = str(entry.get("field") or "")
            value = str(entry.get("value") or "")
            route = entry.get("storage_route")
            route_json = str(entry.get("route_json") or "")
            if not route_json and isinstance(route, dict):
                route_json = json.dumps(route, separators=(",", ":"), sort_keys=True)
            if route_json and route_json != "{}":
                has_routes = True
            compact_entries.append([key, field, value])
            routed_entries.append(
                {
                    "key": key,
                    "field": field,
                    "value": value,
                    "route_json": route_json or "{}",
                }
            )
        append_options = append_options or {}
        if (
            self._append_coalesce_enabled
            and self._shared_process_mode
            and not has_routes
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
            entries=None if not has_routes else routed_entries,
            entries_compact=compact_entries if not has_routes else None,
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
        # Include a caller-supplied durable append watermark in the hot
        # response-cache key so repeated identical queries stay in memory, but
        # callers that already observed a newer record_count avoid stale packs.
        record_count_watermark = str(
            request.get("record_count_watermark")
            or request.get("append_watermark")
            or request.get("resource_version_watermark")
            or ""
        )
        cache_key = self._context_pack_response_cache_key(
            count_key=count_key,
            record_hash_key=record_hash_key,
            shard_size=shard_size,
            request=request,
            record_count_watermark=record_count_watermark,
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
        record_ids: list[str] | None = None,
        return_index_records: bool = False,
        newest_by_type: Json | None = None,
    ) -> Json:
        extra: Json = {}
        if record_ids:
            extra["record_ids"] = [str(item) for item in record_ids]
        if return_index_records:
            extra["return_index_records"] = True
        # Cap the scan to the newest N locations of a named type. Sent only when asked for, so a
        # request that does not carry it is byte-identical to before.
        if newest_by_type:
            extra["newest_by_type"] = {
                str(record_type): int(limit) for record_type, limit in newest_by_type.items()
            }
        return self._call_json(
            "matrixark_scan_candidates",
            count_key=count_key,
            record_hash_key=record_hash_key,
            shard_size=shard_size,
            scope=scope,
            record_types=record_types,
            secondary_index_groups=secondary_index_groups,
            selected_node_hashes=selected_node_hashes,
            **extra,
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
