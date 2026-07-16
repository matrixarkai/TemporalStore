#!/usr/bin/env python3
"""Append and write helpers for TemporalStore-backed MatrixArk adapters."""

from __future__ import annotations

import json
import time
from typing import Any, Iterable

try:
    from tools.matrixark_mcp_core import (
        DIRECT_RECORD_BUNDLE_MAX_BYTES,
        Json,
        MatrixArkError,
        context_event_time_index_entries,
        context_event_time_index_field,
        context_event_time_index_key,
        context_event_time_index_payload,
        context_event_timestamp_ms,
        materialize_serving_record_batch,
        materialize_serving_records,
    )
    from tools import matrixark_mcp_temporal_append as temporal_append_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        DIRECT_RECORD_BUNDLE_MAX_BYTES,
        Json,
        MatrixArkError,
        context_event_time_index_entries,
        context_event_time_index_field,
        context_event_time_index_key,
        context_event_time_index_payload,
        context_event_timestamp_ms,
        materialize_serving_record_batch,
        materialize_serving_records,
    )
    import matrixark_mcp_temporal_append as temporal_append_helpers


class TemporalWriteSupportAdapterMixin:
    """Write-path helper methods for TemporalStore adapters."""

    def _parse_supported_storage_families(self) -> set[str]:
        raw = self._storage_family_env()
        families = {part.strip().lower().replace("-", "_") for part in raw.split(",") if part.strip()}
        return families or {"default", "local", "single_node", "shared_store"}

    def _storage_family_env(self) -> str:
        import os

        return os.environ.get("MATRIXARK_NATIVE_STORAGE_FAMILIES") or os.environ.get("MATRIXARK_SUPPORTED_STORAGE_FAMILIES") or "default,local,single_node,shared_store"

    def _validate_storage_routes_available(self, records: list[Json]) -> None:
        if not hasattr(self, "_supported_storage_families"):
            self._supported_storage_families = self._parse_supported_storage_families()
        requested: set[str] = set()
        for record in records:
            route = record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {}
            family = str(route.get("storage_family") or route.get("selected_storage_family") or "default").strip().lower().replace("-", "_")
            if family and family != "default":
                requested.add(family)
        if len(requested) > 1:
            raise MatrixArkError(f"one MatrixArk write batch cannot mix storage families: {sorted(requested)}")
        unsupported = requested - set(getattr(self, "_supported_storage_families", {"default"}))
        if unsupported:
            raise MatrixArkError(
                f"requested storage_family {sorted(unsupported)} is not configured for backend {self._backend_label()}; "
                f"configured families={sorted(getattr(self, '_supported_storage_families', []))}"
            )

    def append(self, record: Json) -> None:
        self._append_raw_ingestion_records([record])
        records = materialize_serving_records(record)
        if self._queue_batched_records(records):
            return
        self._append_many_materialized(records)

    def append_many(self, records: list[Json]) -> None:
        self._append_raw_ingestion_records(records)
        materialized = materialize_serving_record_batch(records)
        if self._queue_batched_records(materialized):
            return
        self._append_many_materialized(materialized)

    def _storage_route_for_bundle(self, bundle: list[Json]) -> Json:
        fallback: Json = {}
        for record in bundle:
            route = record.get("storage_route")
            if isinstance(route, dict) and route:
                if route.get("placement_key"):
                    return route
                if not fallback:
                    fallback = route
        return fallback

    def _native_append_options(self) -> Json:
        return {
            "append_path": "native_append_queue",
            "coalesce_writes": True,
            "route_by": "placement_key",
            "persist_from_storage_options": True,
            "hset_lowering": "forbidden_for_parity",
            "count_update": "same_batch",
            "audit_hot_path": "inline_counters_only",
            "full_context_pack_audit": "sample_or_enqueue_async_policy_enabled",
        }

    def _context_event_ingestion_time_ms(self, record: Json) -> int:
        return context_event_timestamp_ms(record)

    def _context_event_time_index_key(self, record: Json) -> str:
        return context_event_time_index_key(self._storage_prefix, record)

    def _context_event_time_index_field(self, record: Json) -> str:
        return context_event_time_index_field(record)

    def _context_event_time_index_payload(self, record: Json) -> str:
        """Compact timestamp-index payload."""
        return context_event_time_index_payload(record)

    def _context_event_time_index_entries(self, records: list[Json]) -> list[Json]:
        return context_event_time_index_entries(self._storage_prefix, records)

    def _append_client_for_records(self, records: list[Json]) -> Any:
        return self._client

    def _materialize_appended_records_locked(
        self,
        *,
        prior_entry_count: int,
        new_entry_count: int,
        records: list[Json],
    ) -> None:
        temporal_append_helpers.materialize_appended_records_locked(
            self,
            prior_entry_count=prior_entry_count,
            new_entry_count=new_entry_count,
            records=records,
        )

    def _append_many_materialized(self, records: list[Json], *, allow_queue: bool = True) -> None:
        temporal_append_helpers.append_many_materialized(self, records, allow_queue=allow_queue)

    def _note_pending_visibility_keys(self, keys: Iterable[str]) -> None:
        if not (
            getattr(self, "_publish_visibility_after_flush", False)
            or getattr(self, "_track_pending_visibility_keys", False)
        ):
            return
        pending = getattr(self, "_pending_visibility_keys", None)
        if pending is None:
            self._pending_visibility_keys = set()
            pending = self._pending_visibility_keys
        for key in keys:
            key = str(key or "")
            if key:
                pending.add(key)

    def _raw_ingestion_visibility_required_after_flush(self) -> bool:
        if not getattr(self, "_publish_visibility_after_flush", False):
            return False
        return bool(getattr(self, "_dedicated_proxy_clients_enabled", False))

    def _has_pending_visibility_keys(self) -> bool:
        pending = getattr(self, "_pending_visibility_keys", None)
        return bool(pending)

    def _consume_pending_visibility_keys(self) -> list[str]:
        pending = getattr(self, "_pending_visibility_keys", None)
        if not pending:
            return []
        keys = sorted(pending)
        pending.clear()
        return keys

    def _hset_with_backoff(self, key: str, field: str, value: str) -> None:
        self._write_with_backoff(lambda: self._client.hset(key, field, value), op="hset")
        if self._write_throttle_s > 0:
            time.sleep(self._write_throttle_s)

    def _hset_many_with_backoff(self, entries: list[Json]) -> None:
        if not entries:
            return
        batch_hset = getattr(self._client, "batch_hset", None)
        if callable(batch_hset):
            self._write_with_backoff(lambda: batch_hset(entries), op="batch_hset")
            if self._write_throttle_s > 0:
                time.sleep(self._write_throttle_s)
            return
        for entry in entries:
            self._hset_with_backoff(str(entry["key"]), str(entry["field"]), str(entry["value"]))

    def _put_string_with_backoff(self, key: str, value: str) -> None:
        self._write_with_backoff(lambda: self._client.put_string(key, value), op="put_string")
        if self._write_throttle_s > 0:
            time.sleep(self._write_throttle_s)

    def _write_with_backoff(self, fn: Any, *, op: str) -> None:
        attempt = 0
        while True:
            try:
                fn()
                return
            except Exception:
                if attempt >= self._write_retries:
                    raise
                sleep_s = self._write_backoff_s * (2**attempt)
                if sleep_s > 0:
                    time.sleep(sleep_s)
                attempt += 1

    def _record_bundles(self, records: list[Json]) -> list[list[Json]]:
        bundles: list[list[Json]] = []
        current: list[Json] = []
        current_bytes = 0
        max_bytes = max(8192, DIRECT_RECORD_BUNDLE_MAX_BYTES)
        for record in records:
            record_bytes = len(json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8"))
            if current and current_bytes + record_bytes > max_bytes:
                bundles.append(current)
                current = []
                current_bytes = 0
            current.append(record)
            current_bytes += record_bytes
        if current:
            bundles.append(current)
        return bundles
