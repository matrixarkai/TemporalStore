# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_TemporalDirectRetrieveMixin methods split from matrixark_mcp_temporal_adapters.MatrixArkTemporalStoreDirectAdapter (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # package path
    from tools.matrixark_temporal_location_codec import compact_location_list, expand_location
except ImportError:
    from matrixark_temporal_location_codec import compact_location_list, expand_location

try:  # names owned by the parent module
    from tools.matrixark_mcp_temporal_adapters import (
    RETRIEVAL_HOT_RECORD_TYPES,
    _DIRECT_RETRIEVAL_CANDIDATE_CACHE,
    _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK,
    matrixark_record_retention_filtered,
    native_retrieve_fallback_allowed,
    time,
)
except ImportError:
    from matrixark_mcp_temporal_adapters import (
    RETRIEVAL_HOT_RECORD_TYPES,
    _DIRECT_RETRIEVAL_CANDIDATE_CACHE,
    _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK,
    matrixark_record_retention_filtered,
    native_retrieve_fallback_allowed,
    time,
)


class _TemporalDirectRetrieveMixin:
    def retrieve(self, args: Json) -> Json:
        self._ensure_backend_metric_fields()
        native_pack = self._try_native_context_pack(args)
        if native_pack is not None:
            return native_pack
        if native_retrieve_fallback_allowed(args):
            self._native_context_pack_fallback_active = True
            try:
                return super().retrieve(args)
            finally:
                self._native_context_pack_fallback_active = False
        return super().retrieve(args)

    def _native_locations_for_selected_nodes(self, *, scope: Json, selected_node_hashes: set[int]) -> Json:
        batch_hget = getattr(getattr(self, "_client", None), "batch_hget", None)
        scope_key = canonical_scope_key(scope)
        if not callable(batch_hget) or not scope_key or not selected_node_hashes:
            return {"locations": [], "locator_rows": 0, "eligible": False, "reason": "missing_scope_or_nodes"}
        entries = [
            {"key": self._context_placement_lookup_key(scope_key), "field": str(node_hash)}
            for node_hash in sorted(selected_node_hashes)
            if node_hash
        ]
        if not entries:
            return {"locations": [], "locator_rows": 0, "eligible": False, "reason": "empty_node_set"}
        try:
            rows = batch_hget(entries)
        except Exception as exc:
            return {"locations": [], "locator_rows": 0, "eligible": False, "reason": f"placement_lookup_failed:{exc}"}
        # A node's locations are held in bounded chunks so an append does not rewrite the whole
        # list. The head keeps the original field name and shape -- so this still works against a
        # store written before chunking -- and names how many overflow chunks follow it. Missing
        # them would drop locations silently, which reads as a memory that simply is not there.
        chunk_entries = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict) or not row.get("value"):
                continue
            try:
                decoded = json.loads(str(row.get("value")))
            except Exception:
                continue
            if not isinstance(decoded, dict):
                continue
            try:
                chunks = int(decoded.get("location_chunks") or 0)
            except (TypeError, ValueError):
                chunks = 0
            for index in range(1, chunks + 1):
                chunk_entries.append({"key": row.get("key"), "field": f"{row.get('field')}#{index}"})
        if chunk_entries:
            try:
                extra = batch_hget(chunk_entries)
            except Exception:
                extra = []
            if isinstance(extra, list):
                rows = list(rows) + extra
        locations: list[Json] = []
        resource_versions: set[str] = set()
        seen: set[tuple[str, str]] = set()
        locator_rows = 0
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            value = row.get("value")
            if not value:
                continue
            try:
                decoded = json.loads(str(value))
            except Exception:
                continue
            raw_locations = decoded.get("locations", []) if isinstance(decoded, dict) else []
            raw_versions = decoded.get("resource_versions", []) if isinstance(decoded, dict) else []
            if isinstance(raw_versions, list):
                resource_versions.update(str(value) for value in raw_versions if str(value))
            if not isinstance(raw_locations, list):
                continue
            locator_rows += 1
            scan_base = str(getattr(self, "_record_hash_key", "") or "")
            for location in raw_locations:
                expanded = expand_location(location, scan_base)
                if expanded is None:
                    continue
                key, field = expanded
                if (key, field) in seen:
                    continue
                locations.append({"key": key, "field": field})
                seen.add((key, field))
        return {
            "locations": locations,
            "locator_rows": locator_rows,
            "resource_version_watermark": "|".join(sorted(resource_versions)),
            "eligible": bool(locations),
            "reason": "ok" if locations else "no_matching_placement_rows",
        }

    def _filter_retrieval_candidates(
        self,
        records: list[Json],
        *,
        scope: Json,
        allowed_types: set[str],
        selected_nodes: set[int],
    ) -> tuple[list[Json], Json]:
        filtered: list[Json] = []
        dropped_type = 0
        dropped_scope = 0
        dropped_node = 0
        dropped_retention = 0
        now_ms = int(time.time() * 1000)
        for record in records:
            if matrixark_record_retention_filtered(record, now_ms=now_ms):
                dropped_retention += 1
                continue
            record_type = str(record.get("record_type") or "")
            if record_type not in allowed_types:
                dropped_type += 1
                continue
            if selected_nodes:
                try:
                    record_node_hash = int(record.get("node_hash"))
                except (TypeError, ValueError):
                    record_node_hash = None
                if record_node_hash is not None and record_node_hash not in selected_nodes:
                    dropped_node += 1
                    continue
            if record_type in {"context_embedding", "context_index", "context_summary", "resource_manifest", "skill_registry_update"}:
                if not scope_matches(candidate_access_scope(record), scope):
                    dropped_scope += 1
                    continue
            elif not access_scope_matches_before_scoring(record, scope):
                dropped_scope += 1
                continue
            filtered.append(record)
        return filtered, {
            "scanned": len(records),
            "returned": len(filtered),
            "dropped_type": dropped_type,
            "dropped_scope": dropped_scope,
            "dropped_node": dropped_node,
            "dropped_retention": dropped_retention,
        }

    def retrieval_records(
        self,
        *,
        scope: Json,
        record_types: set[str] | None = None,
        secondary_index_groups: list[set[str]] | None = None,
        selected_node_hashes: set[int] | None = None,
        allow_broad_scan_fallback: bool | None = None,
    ) -> Json:
        self._ensure_backend_metric_fields()
        count = self._entry_count_cache if self._entry_count_cache is not None else self._get_count()
        placement_result = self._native_locations_for_selected_nodes(scope=scope, selected_node_hashes=selected_node_hashes or set())
        resource_version_watermark = str(placement_result.get("resource_version_watermark") or "")
        cache_key = self._retrieval_candidate_cache_key(
            count=count,
            scope={**scope, "_resource_version_watermark": resource_version_watermark},
            record_types=record_types,
            secondary_index_groups=secondary_index_groups,
            selected_node_hashes=selected_node_hashes,
        )
        with _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK:
            cached = _DIRECT_RETRIEVAL_CANDIDATE_CACHE.get(cache_key)
            if cached is not None:
                result = dict(cached)
                result["records"] = list(cached.get("records", []))
                stats = dict(result.get("scan_stats", {}))
                stats["candidate_cache_hit"] = True
                stats["candidate_cache_scope"] = "process_global"
                result["scan_stats"] = stats
                return result

        allowed_types = record_types or RETRIEVAL_HOT_RECORD_TYPES
        selected_nodes = selected_node_hashes or set()
        native_candidates = self._native_candidate_scan(
            scope=scope,
            record_types=allowed_types,
            secondary_index_groups=secondary_index_groups,
            selected_node_hashes=selected_nodes,
        )
        if native_candidates is not None:
            return native_candidates
        if native_candidate_prefilter_required(backend_label=self._backend_label()) and not getattr(self, "_native_context_pack_fallback_active", False):
            raise MatrixArkError(
                f"backend-native candidate prefilter is required for {self._backend_label()}, "
                "but matrixark_scan_candidates did not return candidates. Python read_all scan/prefilter is disabled."
            )
        client_available = hasattr(self, "_client")
        broad_scan_allowed = (
            bool(allow_broad_scan_fallback)
            if allow_broad_scan_fallback is not None
            else (not client_available or not bool(selected_nodes or secondary_index_groups))
        )
        index_result = {"ref_hashes": set(), "postings_found": 0, "index_terms": [], "posting_buckets": [], "eligible": False, "reason": "skipped_for_placement_lookup"}
        fallback_reason = ""
        raw_records: list[Json] = []
        native_pushdown = False
        native_mode = ""
        placement_cache_result: Json = {"cache_hit": False, "cache_entries": 0, "loaded_records": 0}
        if bool(placement_result.get("eligible")):
            placement_cache_result = self._placement_candidate_records_from_cache_or_load(
                count=count,
                scope=scope,
                allowed_types=allowed_types,
                selected_nodes=selected_nodes,
                locations=placement_result.get("locations", []),
                resource_version_watermark=resource_version_watermark,
            )
            raw_records = placement_cache_result.get("records", [])
            native_pushdown = bool(raw_records)
            native_mode = "native_placement_prefetch"
            if not raw_records:
                fallback_reason = "native_placement_locations_empty"
        if not native_pushdown:
            index_result = self._native_index_ref_hashes(scope=scope, secondary_index_groups=secondary_index_groups)
        if not native_pushdown and bool(index_result.get("eligible")):
            location_result = self._native_locations_for_refs(index_result.get("ref_hashes", set()))
            raw_records = self._load_records_from_locations(location_result.get("locations", []))
            native_pushdown = bool(raw_records)
            native_mode = "native_secondary_index_prefilter"
            if not raw_records:
                fallback_reason = "native_index_locations_empty"
        else:
            location_result = {"locations": [], "locator_rows": 0}
            if not native_pushdown:
                fallback_reason = str(index_result.get("reason") or placement_result.get("reason") or "native_index_not_eligible")

        if native_pushdown:
            filtered, filter_stats = self._filter_retrieval_candidates(
                raw_records,
                scope=scope,
                allowed_types=allowed_types,
                selected_nodes=selected_nodes,
            )
            if not filtered:
                fallback_reason = "native_index_filtered_empty"
                native_pushdown = False

        broad_scan_used = False
        broad_scan_blocked = False
        if not native_pushdown and broad_scan_allowed:
            raw_records = self.read_all()
            broad_scan_used = True
            filtered, filter_stats = self._filter_retrieval_candidates(
                raw_records,
                scope=scope,
                allowed_types=allowed_types,
                selected_nodes=selected_nodes,
            )
            secondary_index_dropped = 0
            if secondary_index_groups:
                secondary_filtered = []
                for record in filtered:
                    if str(record.get("record_type") or "") == "resource_chunk":
                        terms = set(candidate_index_terms(record, {}, {}, {}))
                        if not any(terms.intersection(group) for group in secondary_index_groups):
                            secondary_index_dropped += 1
                            continue
                    secondary_filtered.append(record)
                filtered = secondary_filtered
                filter_stats["returned"] = len(filtered)
            filter_stats["secondary_index_dropped_candidate_count"] = secondary_index_dropped
        elif not native_pushdown:
            broad_scan_blocked = True
            raw_records = []
            filtered = []
            filter_stats = {
                "scanned": 0,
                "returned": 0,
                "dropped_type": 0,
                "dropped_scope": 0,
                "dropped_node": 0,
                "secondary_index_dropped_candidate_count": 0,
            }
        filter_stats.setdefault("secondary_index_dropped_candidate_count", 0)
        result = {
            "records": filtered,
            "count": count,
            "scan_stats": {
                "backend": self._backend_label(),
                "execution_mode": (
                    native_mode
                    if native_pushdown
                    else (
                        "direct_backend_hot_cache_prefilter"
                        if broad_scan_used and not client_available
                        else "broad_prefix_scan_fallback"
                        if broad_scan_used
                        else "native_prefilter_no_match_broad_scan_blocked"
                    )
                ),
                "backend_pushdown": True,
                "direct_backend_prefilter": True,
                "native_pushdown": native_pushdown,
                "native_prefix_scan": bool(native_pushdown),
                "native_secondary_index_prefilter": bool(native_pushdown and secondary_index_groups),
                "native_pack_assembly": False,
                "phase2_native_first": True,
                "native_placement_nodes": len(selected_nodes),
                "native_placement_locator_rows": placement_result.get("locator_rows", 0),
                "native_placement_locations": len(placement_result.get("locations", [])),
                "native_placement_candidate_cache_hit": bool(placement_cache_result.get("cache_hit")),
                "native_placement_candidate_cache_entries": int(placement_cache_result.get("cache_entries") or 0),
                "native_placement_loaded_records": int(placement_cache_result.get("loaded_records") or 0),
                "native_candidate_cache_key_shape": "scope_key+node_hash+record_type+append_watermark+resource_version_watermark",
                "native_candidate_cache_payload": "compact_struct",
                "native_resource_version_watermark": resource_version_watermark,
                "native_index_terms": index_result.get("index_terms", []),
                "native_index_posting_buckets": index_result.get("posting_buckets", []),
                "native_index_postings_found": index_result.get("postings_found", 0),
                "native_index_ref_hash_count": len(index_result.get("ref_hashes", set())),
                "native_locator_rows": location_result.get("locator_rows", 0),
                "native_locations": len(location_result.get("locations", [])),
                "fallback_reason": fallback_reason,
                "broad_scan_fallback_allowed": broad_scan_allowed,
                "broad_scan_used": broad_scan_used,
                "broad_scan_blocked": broad_scan_blocked,
                "broad_scan_policy": "explicit_fallback_or_debug_only",
                "candidate_cache_hit": False,
                "candidate_cache_scope": "process_global",
                "watermark_count": count,
                **filter_stats,
                "returned_records": filter_stats.get("returned", 0),
                "dropped_by_type": filter_stats.get("dropped_type", 0),
                "dropped_by_scope": filter_stats.get("dropped_scope", 0),
                "dropped_by_node": filter_stats.get("dropped_node", 0),
                "record_types": sorted(allowed_types),
                "pack_assembly_location": "python_reference_packer",
            },
        }
        with _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK:
            _DIRECT_RETRIEVAL_CANDIDATE_CACHE[cache_key] = {
                **result,
                "storage_prefix": self._storage_prefix,
                "records": list(filtered),
            }
            self._prune_retrieval_candidate_cache(count)
        return result

    def _load_records_by_count(self, count: int) -> list[Json]:
        # Same contract as _get_count: a backend that cannot answer right now (shard still
        # loading, timeout) must raise, never shrink the result. The count said the records
        # exist; silently dropping the ones a loading shard could not serve returned an
        # EMPTY-but-successful view of a populated store for the whole load window.
        records = []
        self._last_read_all_native_shard_scan = False
        scan_records = self._load_records_by_native_shard_scan(count)
        if scan_records is not None:
            self._last_read_all_native_shard_scan = True
            return scan_records
        batch_hget = getattr(self._client, "batch_hget", None)
        if callable(batch_hget):
            entries = []
            for sequence in range(count):
                record_key, record_id = self._record_location(sequence)
                entries.append({"key": record_key, "field": record_id})
            try:
                read_records = batch_hget(entries)
            except Exception as exc:
                if is_retryable_temporalstore_error(exc):
                    raise
                read_records = []
            for item in read_records:
                if not isinstance(item, dict):
                    continue
                payload = item.get("value", "")
                if not payload:
                    continue
                decoded = json.loads(str(payload))
                if isinstance(decoded, dict) and isinstance(decoded.get("record_bundle"), list):
                    records.extend(item for item in decoded["record_bundle"] if isinstance(item, dict))
                elif isinstance(decoded, dict):
                    records.append(decoded)
            if records or count == 0:
                return records
        for sequence in range(count):
            record_key, record_id = self._record_location(sequence)
            try:
                payload = self._client.hget(record_key, record_id)
            except Exception as exc:
                if is_retryable_temporalstore_error(exc):
                    raise
                continue
            if not payload:
                continue
            decoded = json.loads(payload)
            if isinstance(decoded, dict) and isinstance(decoded.get("record_bundle"), list):
                records.extend(item for item in decoded["record_bundle"] if isinstance(item, dict))
            elif isinstance(decoded, dict):
                records.append(decoded)
        return records

    def _load_records_by_native_shard_scan(self, count: int) -> list[Json] | None:
        scanner = getattr(getattr(self, "_client", None), "scan_hash", None)
        if not callable(scanner) or count <= 0:
            return None
        max_shard = (count - 1) // self._shard_size
        records_by_sequence: list[tuple[int, Json]] = []
        for shard in range(max_shard + 1):
            key = f"{self._record_hash_key}:{shard:06d}"
            try:
                response = scanner(key)
            except Exception as exc:
                if is_retryable_temporalstore_error(exc):
                    raise
                return None
            rows = response.get("records") if isinstance(response, dict) else None
            if not isinstance(rows, list):
                return None
            for row in rows:
                if not isinstance(row, dict):
                    continue
                field = str(row.get("field") or "")
                value = row.get("value")
                if not field or not isinstance(value, str):
                    continue
                try:
                    offset = int(field)
                    decoded = json.loads(value)
                except Exception:
                    continue
                sequence = shard * self._shard_size + offset
                if sequence >= count:
                    continue
                if isinstance(decoded, dict) and isinstance(decoded.get("record_bundle"), list):
                    for item in decoded["record_bundle"]:
                        if isinstance(item, dict):
                            records_by_sequence.append((sequence, item))
                elif isinstance(decoded, dict):
                    records_by_sequence.append((sequence, decoded))
        records_by_sequence.sort(key=lambda item: item[0])
        return [record for _, record in records_by_sequence]

    def _record_location(self, sequence: int) -> tuple[str, str]:
        shard = sequence // self._shard_size
        offset = sequence % self._shard_size
        return f"{self._record_hash_key}:{shard:06d}", f"{offset:020d}"

    def _load_records(self, index: list[str]) -> list[Json]:
        # Legacy index-mode read: same must-not-lie contract as _load_records_by_count.
        records = []
        for record_id in index:
            try:
                payload = self._client.hget(self._record_hash_key, record_id)
            except Exception as exc:
                if is_retryable_temporalstore_error(exc):
                    raise
                continue
            if not payload:
                continue
            records.append(json.loads(payload))
        return records

