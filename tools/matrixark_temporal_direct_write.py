# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_TemporalDirectWriteMixin methods split from matrixark_mcp_temporal_adapters.MatrixArkTemporalStoreDirectAdapter (mixin)."""
from __future__ import annotations

try:
    from tools.matrixark_mcp_env import env_bool
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool


try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:
    from tools.matrixark_mcp_local_adapter import (
        drop_vectors_for_opted_out_tenants,
        fold_embedding_records,
    )
except ImportError:
    from matrixark_mcp_local_adapter import (
        drop_vectors_for_opted_out_tenants,
        fold_embedding_records,
    )

try:
    from tools.matrixark_mcp_temporal_append import slim_persisted_storage_route
except ImportError:
    from matrixark_mcp_temporal_append import slim_persisted_storage_route

try:  # names owned by the parent module
    from tools.matrixark_mcp_temporal_adapters import (
    TEMPORAL_COMPRESSED_OLD_RECORD_TYPES,
    _mcp_debug_log,
    _records_with_matrixark_write_debug,
    matrixark_record_retention_filtered,
    queue,
    time,
)
except ImportError:
    from matrixark_mcp_temporal_adapters import (
    TEMPORAL_COMPRESSED_OLD_RECORD_TYPES,
    _mcp_debug_log,
    _records_with_matrixark_write_debug,
    matrixark_record_retention_filtered,
    queue,
    time,
)


class _TemporalDirectWriteMixin:
    def _direct_write_loop(self) -> None:
        while not self._direct_write_stop.is_set():
            try:
                first = self._direct_write_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            items = [first]
            max_batches = max(1, int(getattr(self, "_direct_write_queue_drain_max_batches", 64) or 64))
            while len(items) < max_batches:
                try:
                    items.append(self._direct_write_queue.get_nowait())
                except queue.Empty:
                    break
            try:
                flushed = self._flush_direct_write_items(items)
                self._direct_write_flushed_records += flushed
                self._direct_write_flushed_batches += len(items)
            except Exception as exc:
                self._direct_write_failures += 1
                _mcp_debug_log(f"matrixark direct write queue flush failed: {exc}")
            finally:
                for _item in items:
                    try:
                        self._direct_write_queue.task_done()
                    except Exception:
                        pass

    def _flush_direct_write_items(self, items: list[Any]) -> int:
        memory_records: list[Json] = []
        raw_ingestion_records: list[Json] = []
        flushed = 0
        flush_started_at_ms = now_ms()
        flush_batch_id = f"flush:{flush_started_at_ms}:{stable_hash(str(len(items)))}"
        for item in items:
            if isinstance(item, dict) and item.get("queue_mode") == "temporalstore":
                flushed += self._flush_direct_write_durable_field(str(item.get("field") or ""))
            elif isinstance(item, dict) and item.get("queue_mode") == "raw_ingestion":
                rows = item.get("records")
                if isinstance(rows, list):
                    raw_ingestion_records.extend(row for row in rows if isinstance(row, dict))
            elif isinstance(item, list):
                memory_records.extend(row for row in item if isinstance(row, dict))
            else:
                raise MatrixArkError("unknown direct write queue item")
        if raw_ingestion_records:
            raw_ingestion_records = _records_with_matrixark_write_debug(
                raw_ingestion_records,
                flush_started_at_ms=flush_started_at_ms,
                flush_batch_id=flush_batch_id,
                flush_item_count=len(items),
                flush_record_count=len(raw_ingestion_records),
            )
            self._append_raw_ingestion_records(raw_ingestion_records, allow_queue=False)
            flushed += len(raw_ingestion_records)
        if memory_records:
            self._append_many_materialized(memory_records, allow_queue=False)
            flushed += len(memory_records)
        return flushed

    def _flush_direct_write_item(self, item: Any) -> int:
        return self._flush_direct_write_items([item])

    def _load_direct_write_durable_payload(self, field: str) -> Json | None:
        if not field:
            return None
        raw = self._client.hget(self._direct_write_queue_key, field)
        if not raw:
            return None
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else None

    def _write_direct_write_durable_status(self, field: str, payload: Json, status: str, error: str | None = None) -> None:
        updated = dict(payload)
        updated["status"] = status
        updated["updated_at_ms"] = now_ms()
        updated["attempts"] = int(updated.get("attempts") or 0) + (1 if status in {"running", "failed", "dead"} else 0)
        if error:
            updated["error"] = error
        key = self._direct_write_queue_done_key if status == "done" else self._direct_write_queue_dead_key if status == "dead" else self._direct_write_queue_key
        self._hset_with_backoff(key, field, json.dumps(updated, separators=(",", ":")))
        if key != self._direct_write_queue_key:
            self._hset_with_backoff(self._direct_write_queue_key, field, json.dumps(updated, separators=(",", ":")))

    def _flush_direct_write_durable_field(self, field: str) -> int:
        payload = self._load_direct_write_durable_payload(field)
        if not payload:
            return 0
        status = str(payload.get("status") or "pending")
        if status == "done":
            return 0
        if status == "dead":
            return 0
        records = payload.get("records")
        if not isinstance(records, list):
            self._write_direct_write_durable_status(field, payload, "dead", "durable queue payload has no records list")
            self._direct_write_dead_letter_batches += 1
            return 0
        self._write_direct_write_durable_status(field, payload, "running")
        try:
            self._append_many_materialized(records, allow_queue=False)
        except Exception as exc:
            refreshed = self._load_direct_write_durable_payload(field) or payload
            self._write_direct_write_durable_status(field, refreshed, "failed", str(exc))
            raise
        refreshed = self._load_direct_write_durable_payload(field) or payload
        self._write_direct_write_durable_status(field, refreshed, "done")
        return len(records)

    def drain_durable_direct_write_queue(self, *, limit: int | None = None) -> Json:
        self._ensure_direct_write_queue_fields()
        if getattr(self, "_direct_write_queue_mode", "memory") != "temporalstore":
            return {"status": "skipped", "reason": "queue_mode_not_temporalstore"}
        scanner = getattr(self._client, "scan_hash", None)
        if not callable(scanner):
            return {"status": "skipped", "reason": "backend_has_no_scan_hash"}
        response = scanner(self._direct_write_queue_key)
        records = response.get("records") if isinstance(response, dict) else []
        fields: list[str] = []
        for row in records if isinstance(records, list) else []:
            if not isinstance(row, dict):
                continue
            field = str(row.get("field") or "")
            value = row.get("value")
            if not field or not isinstance(value, str):
                continue
            try:
                payload = json.loads(value)
            except Exception:
                continue
            if isinstance(payload, dict) and str(payload.get("status") or "pending") in {"pending", "failed", "running"}:
                fields.append(field)
            if limit is not None and len(fields) >= limit:
                break
        self._start_direct_write_worker()
        for field in fields:
            self._direct_write_queue.put({"queue_mode": "temporalstore", "field": field}, timeout=self._direct_write_queue_put_timeout_s)
        return {"status": "queued", "pending_batches": len(fields), "queue_key": self._direct_write_queue_key}

    def _direct_write_durable_pending_count(self) -> int:
        self._ensure_direct_write_queue_fields()
        scanner = getattr(getattr(self, "_client", None), "scan_hash", None)
        if not callable(scanner):
            return 0
        try:
            response = scanner(self._direct_write_queue_key)
        except Exception:
            return 0
        rows = response.get("records") if isinstance(response, dict) else []
        count = 0
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            value = row.get("value")
            if not isinstance(value, str):
                continue
            try:
                payload = json.loads(value)
            except Exception:
                continue
            if isinstance(payload, dict) and str(payload.get("status") or "pending") in {"pending", "failed", "running"}:
                count += 1
        return count

    def flush_direct_writes(self, timeout_s: float | None = None) -> None:
        self._ensure_direct_write_queue_fields()
        self._start_direct_write_worker()
        if getattr(self, "_direct_write_queue_mode", "memory") == "temporalstore":
            self.drain_durable_direct_write_queue()
        deadline = time.monotonic() + float(timeout_s if timeout_s is not None else 30.0)
        while self._direct_write_queue.unfinished_tasks:
            if time.monotonic() >= deadline:
                raise MatrixArkError("timed out waiting for direct TemporalStore write queue to drain")
            time.sleep(0.01)

    def _append_client_for_records(self, records: list[Json]) -> Any:
        return self._client

    def _materialize_appended_records_locked(
        self,
        *,
        prior_entry_count: int,
        new_entry_count: int,
        records: list[Json],
    ) -> None:
        """Refresh process-local materialized views after native latest-state writes.

        Some compact context records are written as latest-state HSet entries
        rather than append-log entries. Resource/skill list and retrieval paths
        still need those records visible in the adapter's parsed caches during
        the current process, without forcing the hot write path back through the
        legacy full record log.
        """
        if not records:
            return
        try:
            self._entry_count_cache = max(int(new_entry_count or 0), int(prior_entry_count or 0))
        except Exception:
            pass
        if getattr(self, "_records_cache", None) is not None:
            try:
                self._records_cache.extend(records)
                self._put_direct_record_cache(len(self._records_cache), self._records_cache)
            except Exception:
                pass
        try:
            self._prune_retrieval_candidate_cache(getattr(self, "_entry_count_cache", None) or int(new_entry_count or 0))
        except Exception:
            pass
        try:
            self._update_latest_entity_cache(records)
        except Exception:
            pass

    def _append_many_materialized(self, records: list[Json], *, allow_queue: bool = True) -> None:
        if not records:
            return
        # Embeddings fold onto their owners at this single backend append call site, exactly as
        # the pure-local JSONL adapter folds at its own append -- the fast direct-ingest path
        # never goes through append_many, so folding there alone let separate embedding rows
        # reach the engine. A drain re-entry (allow_queue=False on already-folded records) is a
        # no-op: nothing left to partition. The resolver is consulted only for an embedding with
        # no same-batch owner.
        records = drop_vectors_for_opted_out_tenants(records)
        records = fold_embedding_records(
            records, resolve_owner=getattr(self, "_resolve_embedding_owner", None)
        )
        if not records:
            return
        self._ensure_backend_metric_fields()
        records = compact_latest_context_state_records(records)
        self._append_disk_fallback_records(records)
        latest_state_entries, append_records_for_log = self._split_compacted_latest_context_state(records)
        self._validate_storage_routes_available(records)
        if latest_state_entries and not append_records_for_log:
            self._hset_many_with_backoff(latest_state_entries)
            self._materialize_appended_records_locked(
                prior_entry_count=getattr(self, "_entry_count_cache", None) or self._get_count(),
                new_entry_count=getattr(self, "_entry_count_cache", None) or self._get_count(),
                records=records,
            )
            return
        records_to_append = append_records_for_log
        if allow_queue and self._records_can_use_direct_write_queue(records_to_append):
            self._enqueue_direct_write(records)
            return
        started_perf = time.perf_counter()
        with self._records_lock:
            entry_count_cache = getattr(self, "_entry_count_cache", None)
            count = entry_count_cache if entry_count_cache is not None else self._get_count()
            if count <= 0 and self._index_cache is None:
                self._index_cache = self._get_index()
                self._legacy_index_mode = bool(self._index_cache)
            event_time_entries = self._context_event_time_index_entries(records_to_append)
            if self._legacy_index_mode:
                if self._index_cache is None:
                    self._index_cache = self._get_index()
                entries: list[Json] = []
                for record in records_to_append:
                    payload = json.dumps(record, sort_keys=True, separators=(",", ":"))
                    record_id = (
                        f"{len(self._index_cache):020d}:"
                        f"{record.get('record_type', 'record')}:"
                        f"{stable_hash(json.dumps(record, sort_keys=True))}"
                    )
                    route = record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {}
                    entries.append({"key": self._record_hash_key, "field": record_id, "value": payload, "storage_route": route})
                    self._index_cache.append(record_id)
                self._hset_many_with_backoff(latest_state_entries + event_time_entries + entries)
                self._put_string_with_backoff(self._index_key, json.dumps(self._index_cache, separators=(",", ":")))
                self._note_pending_visibility_keys(
                    [self._index_key]
                    + [str(entry.get("key") or "") for entry in latest_state_entries]
                    + [str(entry.get("key") or "") for entry in event_time_entries]
                    + [str(entry.get("key") or "") for entry in entries]
                )
                if self._records_cache is not None:
                    self._records_cache.extend(records)
                    self._put_direct_record_cache(len(self._records_cache), self._records_cache)
                self._update_latest_entity_cache(records)
                elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
                self._observe_append_engine(elapsed_ms)
                self._observe_backend_command(elapsed_ms, records_written=len(records))
                return

            sequence = count
            entries = []
            located_bundles: list[tuple[list[Json], str, str]] = []
            for bundle in self._record_bundles(records):
                record_key, record_id = self._record_location(sequence)
                payload_value: Json
                slim = [slim_persisted_storage_route(record) for record in bundle]
                payload_value = slim[0] if len(slim) == 1 else {"record_bundle": slim}
                payload = json.dumps(payload_value, sort_keys=True, separators=(",", ":"))
                entries.append({"key": record_key, "field": record_id, "value": payload, "storage_route": self._storage_route_for_bundle(bundle)})
                located_bundles.append((bundle, record_key, record_id))
                sequence += 1
            native_index_entries = self._native_side_index_entries_for_bundles(located_bundles)
            # Route the append through the type router, so a summary/audit-only bundle can ride the
            # dedicated summary client instead of the foreground write lane. This is the ONLY
            # append call site, and it ignored the router -- which made the router, the dedicated
            # client, and its gate dead code, and put every background summary append in front of
            # foreground ingests. Gate off (the default) the router returns self._client unchanged.
            append_client = self._append_client_for_records(records_to_append)
            append_records = getattr(append_client, "matrixark_batch_append_records", None)
            if callable(append_records):
                self._write_with_backoff(
                    lambda: self._matrixark_batch_append_records_with_options(
                        append_records,
                        event_time_entries + native_index_entries + entries,
                        count_key=self._count_key,
                        count_value=str(sequence),
                        append_options=self._native_append_options(),
                    ),
                    op="matrixark_batch_append_records",
                )
                if self._write_throttle_s > 0:
                    time.sleep(self._write_throttle_s)
            else:
                self._hset_many_with_backoff(event_time_entries + native_index_entries + entries)
                self._put_string_with_backoff(self._count_key, str(sequence))
            self._note_pending_visibility_keys(
                [self._count_key]
                + [str(entry.get("key") or "") for entry in latest_state_entries]
                + [str(entry.get("key") or "") for entry in event_time_entries]
                + [str(entry.get("key") or "") for entry in native_index_entries]
                + [str(entry.get("key") or "") for entry in entries]
            )
            self._entry_count_cache = sequence
            if self._records_cache is not None:
                self._records_cache.extend(records)
                self._put_direct_record_cache(self._entry_count_cache, self._records_cache)
            self._prune_retrieval_candidate_cache(sequence)
            self._update_latest_entity_cache(records)
            elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
            self._observe_append_engine(elapsed_ms)
            self._observe_backend_command(elapsed_ms, records_written=len(records))

    def _note_pending_visibility_keys(self, keys: Iterable[str]) -> None:
        if not getattr(self, "_publish_visibility_after_flush", False):
            return
        pending = getattr(self, "_pending_visibility_keys", None)
        if pending is None:
            self._pending_visibility_keys = set()
            pending = self._pending_visibility_keys
        for key in keys:
            key = str(key or "")
            should_publish = getattr(self, "_should_publish_visibility_key", None)
            if callable(should_publish) and not should_publish(key):
                continue
            if key:
                pending.add(key)

    def _raw_ingestion_visibility_required_after_flush(self) -> bool:
        if not getattr(self, "_publish_visibility_after_flush", False):
            return False
        return bool(getattr(self, "_dedicated_proxy_clients_enabled", False))

    def _consume_pending_visibility_keys(self) -> list[str]:
        pending = getattr(self, "_pending_visibility_keys", None)
        if not pending:
            return []
        keys = sorted(pending)
        pending.clear()
        return keys

    def append_audit(self, record: Json) -> None:
        if self._audit_mode == "drop":
            _mcp_debug_log("matrixark audit record dropped by MATRIXARK_DIRECT_AUDIT_MODE=drop")
            return
        if self._audit_mode == "sync":
            self.append(record)
            return
        with self._audit_lock:
            self._audit_buffer.append(record)
            if self._audit_mode == "buffered":
                self._ensure_audit_flusher_locked()
            max_pending = self._audit_buffer_max_records * 4
            if len(self._audit_buffer) > max_pending:
                dropped = len(self._audit_buffer) - max_pending
                self._audit_buffer = self._audit_buffer[-max_pending:]
                _mcp_debug_log(f"matrixark audit buffer dropped {dropped} oldest records after flush lag")

    def ensure_backend_ready(
        self,
        *,
        reason: str = "matrixark",
        probe: bool = True,
        timeout_ms: int | None = None,
    ) -> Json:
        with self._backend_readiness_lock:
            if self._backend_ready and self._backend_ready_result is not None:
                cached = dict(self._backend_ready_result)
                cached["cached"] = True
                cached["reason"] = reason
                return cached
            result = self._run_backend_readiness_gate(reason=reason, probe=probe, timeout_ms=timeout_ms)
            if result.get("status") == "ready":
                self._backend_ready = True
                self._backend_ready_result = dict(result)
            return result

    def _backend_metaserver(self) -> str:
        return str(getattr(self, "_metaserver", "") or getattr(getattr(self, "_client", None), "metaserver", ""))

    def _backend_label(self) -> str:
        return "temporalstore-direct"

    def _readiness_failure_result(
        self,
        *,
        reason: str,
        probe: bool,
        attempts: int,
        attempt_log: list[Json],
        error: str,
        checks: Json,
        metaserver: str,
        warmup_key: str,
        warmup_field: str,
    ) -> Json:
        return {
            "status": "topology_not_ready",
            "backend": self._backend_label(),
            "reason": reason,
            "probe": bool(probe),
            "attempts": attempts,
            "attempt_log": attempt_log,
            "error": error,
            "topology": {
                "metaserver": metaserver,
                "namespace": self._namespace,
                "table": self._table,
                "storage_prefix": self._storage_prefix,
                "warmup_key": warmup_key,
                "warmup_field": warmup_field,
            },
            "checks": checks,
        }

    def _run_backend_readiness_gate(
        self,
        *,
        reason: str,
        probe: bool = True,
        timeout_ms: int | None = None,
    ) -> Json:
        timeout = max(1, int(timeout_ms or BACKEND_READINESS_TIMEOUT_MS))
        timeout_s = max(0.1, timeout / 1000.0)
        backoff_s = max(0.01, BACKEND_READINESS_BACKOFF_MS / 1000.0)
        deadline = time.monotonic() + timeout_s
        attempts = 0
        metaserver = self._backend_metaserver()
        key = f"{self._storage_prefix}:readiness"
        field = f"{os.getpid()}:{int(time.time() * 1000)}:{stable_hash(reason)}"
        value = json.dumps({"reason": reason, "pid": os.getpid(), "created_at_ms": now_ms()}, sort_keys=True, separators=(",", ":"))
        attempt_log: list[Json] = []
        while True:
            attempts += 1
            checks: Json = {
                "mcp_process_started": True,
                "metaserver_reachable": {"ok": False, "address": metaserver, "error": "not checked"},
                "namespace_table_opened": False,
                "slot_coverage_verified_by_warmup_hset_hget": False,
            }
            if metaserver:
                meta_check = metaserver_reachable(metaserver)
                checks["metaserver_reachable"] = meta_check
                if not bool(meta_check.get("ok")):
                    last_error = f"metaserver unreachable: {meta_check.get('error', 'unknown')}"
                    attempt_log.append({"attempt": attempts, "ok": False, "retryable": True, "error": last_error, "checks": checks})
                    if time.monotonic() >= deadline:
                        return self._readiness_failure_result(
                            reason=reason,
                            probe=probe,
                            attempts=attempts,
                            attempt_log=attempt_log,
                            error=last_error,
                            checks=checks,
                            metaserver=metaserver,
                            warmup_key=key,
                            warmup_field=field,
                        )
                    time.sleep(min(backoff_s * attempts, 2.0))
                    continue
            try:
                checks["namespace_table_opened"] = True
                if probe:
                    self._client.hset(key, field, value)
                    readback = self._client.hget(key, field)
                    if readback != value:
                        raise MatrixArkError("readiness hget readback mismatch")
                    checks["slot_coverage_verified_by_warmup_hset_hget"] = True
                return {
                    "status": "ready",
                    "backend": self._backend_label(),
                    "reason": reason,
                    "probe": bool(probe),
                    "metaserver": metaserver,
                    "storage_prefix": self._storage_prefix,
                    "warmup_key": key,
                    "attempts": attempts,
                    "attempt_log": attempt_log,
                    "topology": {
                        "metaserver": metaserver,
                        "namespace": self._namespace,
                        "table": self._table,
                        "storage_prefix": self._storage_prefix,
                        "warmup_key": key,
                        "warmup_field": field,
                    },
                    "checks": checks,
                }
            except Exception as exc:
                last_error = str(exc)
                retryable = is_retryable_temporalstore_error(exc)
                attempt_log.append({"attempt": attempts, "ok": False, "retryable": retryable, "error": last_error, "checks": checks})
                if time.monotonic() >= deadline or not retryable:
                    return self._readiness_failure_result(
                        reason=reason,
                        probe=probe,
                        attempts=attempts,
                        attempt_log=attempt_log,
                        error=last_error,
                        checks=checks,
                        metaserver=metaserver,
                        warmup_key=key,
                        warmup_field=field,
                    )
                time.sleep(min(backoff_s * attempts, 2.0))

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

    def flush_audits(self) -> None:
        with self._audit_lock:
            if not self._audit_buffer:
                return
            records = self._audit_buffer
            self._audit_buffer = []
        try:
            self.append_many(records)
        except Exception as exc:
            with self._audit_lock:
                self._audit_flush_failures += 1
                remaining_capacity = max(0, self._audit_buffer_max_records * 2 - len(self._audit_buffer))
                if remaining_capacity:
                    self._audit_buffer = records[-remaining_capacity:] + self._audit_buffer
            _mcp_debug_log(f"matrixark audit flush failed: {exc}")

    def _ensure_audit_flusher_locked(self) -> None:
        if self._audit_flusher_started:
            return
        self._audit_flusher_started = True
        thread = threading.Thread(target=self._audit_flush_loop, name="matrixark-audit-flusher", daemon=True)
        thread.start()

    def _audit_flush_loop(self) -> None:
        while True:
            time.sleep(self._audit_flush_interval_s)
            try:
                self.flush_audits()
            except Exception as exc:
                _mcp_debug_log(f"matrixark audit flush loop failed: {exc}")

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

    def _async_context_warmup_storage_mode(self) -> str:
        for name in (
            "MATRIXARK_TEMPORALSTORE_STORAGE_MODE",
            "MATRIXARK_NATIVE_STORAGE_MODE",
            "MATRIXARK_STORAGE_MODE",
            "MATRIXARK_BENCHMARK_STORAGE_MODE",
        ):
            value = os.environ.get(name, "").strip().lower().replace("-", "_")
            if value:
                return value
        return "local"

    def _async_context_warmup_allowed(self) -> tuple[bool, str]:
        if not bool(getattr(self, "_async_context_warmup_enabled", False)):
            return False, "disabled"
        if env_bool("MATRIXARK_TEMPORALSTORE_ASYNC_CONTEXT_WARMUP_FORCE", False):
            return True, "env_force"
        mode = self._async_context_warmup_storage_mode()
        replication_mode = os.environ.get("MATRIXARK_BENCHMARK_REPLICATION_MODE", "").strip().lower().replace("-", "_")
        distributed_modes = {"distributed", "multi_node", "shared_store", "replicated", "replication", "raft"}
        if mode in distributed_modes or replication_mode in distributed_modes:
            return False, f"storage_mode_{mode}_replication_{replication_mode or 'unset'}"
        if mode in {"local", "single_node", "single", "standalone", "dev", "debug", "default"}:
            return True, f"storage_mode_{mode}"
        return False, f"storage_mode_{mode}"

    def start_async_context_memory_warmup(self, *, reason: str = "manual", max_records: int | None = None) -> Json:
        self._ensure_backend_metric_fields()
        allowed, gate_reason = self._async_context_warmup_allowed()
        if not allowed:
            status = {"status": "skipped", "reason": reason, "gate": gate_reason, "nonblocking": True}
            self._async_context_warmup_status = status
            return status
        with self._async_context_warmup_lock:
            if self._async_context_warmup_in_progress:
                return dict(self._async_context_warmup_status)
            self._async_context_warmup_in_progress = True
            self._async_context_warmup_started_total += 1
            status = {
                "status": "running",
                "reason": reason,
                "gate": gate_reason,
                "started_at_ms": now_ms(),
                "nonblocking": True,
                "source": "temporalstore_durable_record_log",
            }
            self._async_context_warmup_status = status
        thread = threading.Thread(
            target=self._async_context_memory_warmup_loop,
            kwargs={"reason": reason, "max_records": max_records},
            name="matrixark-context-memory-warmup",
            daemon=True,
        )
        thread.start()
        return dict(status)

    def _async_context_memory_warmup_loop(self, *, reason: str, max_records: int | None) -> None:
        started_perf = time.perf_counter()
        count = 0
        try:
            count = self._get_count()
            load_count = min(count, int(max_records)) if max_records is not None and int(max_records) > 0 else count
            raw_records = self._load_records_by_count(load_count) if load_count > 0 else []
            latest_state_records = self._load_latest_context_state_records()
            now = int(time.time() * 1000)
            skipped_old_compressed = 0
            skipped_retention = 0
            warm_records: list[Json] = []
            for record in list(raw_records) + list(latest_state_records):
                record_type = str(record.get("record_type") or "")
                if record_type in TEMPORAL_COMPRESSED_OLD_RECORD_TYPES:
                    skipped_old_compressed += 1
                    continue
                if matrixark_record_retention_filtered(record, now_ms=now):
                    skipped_retention += 1
                    continue
                warm_records.append(record)
            warm_records = compact_latest_context_state_records(warm_records)
            with self._records_lock:
                self._entry_count_cache = count
                self._records_cache = list(warm_records)
                self._put_direct_record_cache(count, self._records_cache)
            elapsed_ms = round((time.perf_counter() - started_perf) * 1000.0, 3)
            status = {
                "status": "completed",
                "reason": reason,
                "source": "temporalstore_durable_record_log",
                "count_key": getattr(self, "_count_key", ""),
                "record_hash_key": getattr(self, "_record_hash_key", ""),
                "target_count": count,
                "loaded_log_records": len(raw_records),
                "loaded_latest_state_records": len(latest_state_records),
                "warmed_records": len(warm_records),
                "skipped_old_compressed_records": skipped_old_compressed,
                "skipped_retention_records": skipped_retention,
                "records_cache_count": len(self._records_cache or []),
                "elapsed_ms": elapsed_ms,
                "nonblocking": True,
            }
            with self._async_context_warmup_lock:
                self._async_context_warmup_completed_total += 1
                self._async_context_warmup_status = status
        except Exception as exc:
            elapsed_ms = round((time.perf_counter() - started_perf) * 1000.0, 3)
            status = {
                "status": "failed",
                "reason": reason,
                "source": "temporalstore_durable_record_log",
                "target_count": count,
                "error": str(exc),
                "elapsed_ms": elapsed_ms,
                "nonblocking": True,
            }
            with self._async_context_warmup_lock:
                self._async_context_warmup_failed_total += 1
                self._async_context_warmup_status = status
            _mcp_debug_log(f"matrixark async context memory warmup failed: {exc}")
        finally:
            with self._async_context_warmup_lock:
                self._async_context_warmup_in_progress = False

