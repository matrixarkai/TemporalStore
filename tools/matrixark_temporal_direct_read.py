# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_TemporalDirectReadMixin methods split from matrixark_mcp_temporal_adapters.MatrixArkTemporalStoreDirectAdapter (mixin)."""
from __future__ import annotations

try:
    from tools.matrixark_mcp_env import env_bool
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool


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
    _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE,
    _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK,
    _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES,
    _DIRECT_RECORD_CACHE,
    _DIRECT_RECORD_CACHE_LOCK,
    _DIRECT_RECORD_CACHE_MAX_PREFIXES,
    _DIRECT_RECORD_LOAD_LOCKS,
    _DIRECT_RETRIEVAL_CANDIDATE_CACHE,
    _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK,
    _DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES,
    _compact_native_selected_refs,
    _float_metric_or_default,
    _mcp_debug_log,
    _native_scope_with_hashes,
    auto_extraction_phase_budget_tokens,
    auto_memory_layer_budget_tokens,
    auto_memory_selection_policy_budget_tokens,
    auto_source_role_budget_tokens,
    compact_context_pack_for_serving,
    matrixark_record_retention_filtered,
    memory_layer_budget_question_reason,
    native_retrieve_fallback_allowed,
    pre_retrieval_idle_commit_flush,
    time,
)
except ImportError:
    from matrixark_mcp_temporal_adapters import (
    RETRIEVAL_HOT_RECORD_TYPES,
    _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE,
    _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK,
    _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES,
    _DIRECT_RECORD_CACHE,
    _DIRECT_RECORD_CACHE_LOCK,
    _DIRECT_RECORD_CACHE_MAX_PREFIXES,
    _DIRECT_RECORD_LOAD_LOCKS,
    _DIRECT_RETRIEVAL_CANDIDATE_CACHE,
    _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK,
    _DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES,
    _compact_native_selected_refs,
    _float_metric_or_default,
    _mcp_debug_log,
    _native_scope_with_hashes,
    auto_extraction_phase_budget_tokens,
    auto_memory_layer_budget_tokens,
    auto_memory_selection_policy_budget_tokens,
    auto_source_role_budget_tokens,
    compact_context_pack_for_serving,
    matrixark_record_retention_filtered,
    memory_layer_budget_question_reason,
    native_retrieve_fallback_allowed,
    pre_retrieval_idle_commit_flush,
    time,
)


class _TemporalDirectReadMixin:
    def read_all(self) -> list[Json]:
        self._recover_serving_from_disk_fallback_if_needed(reason="read_all")
        return self.read_all_without_disk_fallback_recovery()

    def read_all_without_disk_fallback_recovery(self) -> list[Json]:
        with self._records_lock:
            hot_cache_enabled = self.python_hot_cache_enabled()
            if hot_cache_enabled and self._records_cache is not None:
                self._records_cache = self._with_latest_context_state_records(self._records_cache)
                return list(self._records_cache)
            count = self._get_count()
            if count > 0:
                self._legacy_index_mode = False
                self._entry_count_cache = count
                if not hot_cache_enabled:
                    self._records_cache = None
                    self._drop_direct_record_cache()
                    return self._with_latest_context_state_records(self._load_records_by_count(count))
                cached = self._get_direct_record_cache(count)
                if cached is not None:
                    self._records_cache = self._with_latest_context_state_records(cached)
                    return list(self._records_cache)
                with self._direct_record_load_lock():
                    cached = self._get_direct_record_cache(count)
                    if cached is not None:
                        self._records_cache = self._with_latest_context_state_records(cached)
                        return list(self._records_cache)
                    self._records_cache = self._with_latest_context_state_records(self._load_records_by_count(count))
                    self._put_direct_record_cache(count, self._records_cache)
                    return list(self._records_cache)
            index = self._get_index()
            self._index_cache = index
            self._legacy_index_mode = bool(index)
            self._entry_count_cache = None
            records = self._with_latest_context_state_records(self._load_records(index))
            if hot_cache_enabled:
                self._records_cache = records
            else:
                self._records_cache = None
            return list(records)

    def retrieval_records(
        self,
        *,
        scope: Json,
        record_types: set[str] | None = None,
        secondary_index_groups: list[set[str]] | None = None,
        selected_node_hashes: set[int] | None = None,
    ) -> Json:
        """Return retrieval candidates with native scan/cache prefiltering.

        direct and Rust proxy/direct SDK expose native hash/prefix scan for
        debug candidate inspection. Normal TemporalStore retrieval should use
        matrixark_retrieve_context_pack so Python receives a finished ContextPack
        instead of materializing candidates or assembling the hot-path pack.
        """

        allowed_types = record_types or RETRIEVAL_HOT_RECORD_TYPES
        self._recover_serving_from_disk_fallback_if_needed(reason="retrieval_records")
        native_candidates = self._native_candidate_scan(
            scope=scope,
            record_types=allowed_types,
            secondary_index_groups=secondary_index_groups,
            selected_node_hashes=selected_node_hashes,
        )
        if native_candidates is not None:
            return native_candidates
        if native_candidate_prefilter_required(backend_label=self._backend_label()) and not getattr(self, "_native_context_pack_fallback_active", False):
            raise MatrixArkError(
                f"backend-native candidate prefilter is required for {self._backend_label()}, "
                "but matrixark_scan_candidates did not return candidates. Python read_all scan/prefilter is disabled."
            )

        raw_records = self.read_all()
        filtered: list[Json] = []
        scoped_records: list[Json] = []
        scanned = 0
        dropped_type = 0
        dropped_scope = 0
        dropped_retention = 0
        now_ms = int(time.time() * 1000)
        for record in raw_records:
            scanned += 1
            if matrixark_record_retention_filtered(record, now_ms=now_ms):
                dropped_retention += 1
                continue
            record_type = str(record.get("record_type") or "")
            if record_type not in allowed_types:
                dropped_type += 1
                continue
            if record_type in {"context_embedding", "context_index", "context_summary", "resource_manifest", "skill_registry_update"}:
                in_scope = scope_matches(candidate_access_scope(record), scope)
            else:
                in_scope = access_scope_matches_before_scoring(record, scope)
            if not in_scope:
                dropped_scope += 1
                continue
            scoped_records.append(record)

        secondary_index_dropped = 0
        secondary_index_matched = 0
        matched_node_hashes: set[int] = set()
        if secondary_index_groups:
            index_terms_by_batch: dict[Any, list[str]] = {}
            index_terms_by_node: dict[Any, list[str]] = {}
            index_terms_by_ref: dict[Any, list[str]] = {}
            index_terms_by_node_for_prefilter: dict[int, list[str]] = {}
            for record in scoped_records:
                if record.get("record_type") != "context_index":
                    continue
                index_name = str(record.get("index_name") or "")
                if not index_name:
                    continue
                index_terms_by_batch.setdefault(record.get("batch_id_hash"), []).append(index_name)
                ref_hashes = context_index_record_ref_hashes(record)
                for legacy_field in ("chunk_hash", "section_hash", "skill_hash"):
                    legacy_value = record.get(legacy_field)
                    if legacy_value is not None:
                        ref_hashes.append(legacy_value)
                ref_hashes = ordered_unique_any(ref_hashes)
                node_hashes_for_index = context_index_record_node_hashes(record)
                for node_hash_for_index in node_hashes_for_index:
                    try:
                        node_hash_int = int(node_hash_for_index)
                    except (TypeError, ValueError):
                        continue
                    index_terms_by_node_for_prefilter.setdefault(node_hash_int, []).append(index_name)
                    index_terms_by_node.setdefault(node_hash_int, []).append(index_name)
                if ref_hashes:
                    for ref_hash in ref_hashes:
                        index_terms_by_ref.setdefault(ref_hash, []).append(index_name)
                elif not node_hashes_for_index:
                    index_terms_by_node.setdefault(record.get("node_hash"), []).append(index_name)
            matched_node_hashes = {
                node_hash
                for node_hash, terms in index_terms_by_node_for_prefilter.items()
                if passes_secondary_index_filters(set(terms), secondary_index_groups, mode="any_group" if len(secondary_index_groups) > 1 else "all_groups")
            }
            filter_mode = "any_group" if len(secondary_index_groups) > 1 else "all_groups"
            for record in scoped_records:
                terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
                node_hash = record.get("node_hash")
                try:
                    node_matches = int(node_hash) in matched_node_hashes
                except (TypeError, ValueError):
                    node_matches = False
                if terms and not passes_applicable_secondary_index_filters(terms, secondary_index_groups, mode=filter_mode):
                    secondary_index_dropped += 1
                    continue
                if terms or node_matches:
                    secondary_index_matched += 1
                filtered.append(record)
        else:
            filtered = scoped_records

        if selected_node_hashes:
            narrowed: list[Json] = []
            selected = {int(item) for item in selected_node_hashes}
            for record in filtered:
                try:
                    node_hash = int(record.get("node_hash"))
                except (TypeError, ValueError):
                    narrowed.append(record)
                    continue
                if node_hash in selected or record.get("record_type") in {"context_index", "context_embedding"}:
                    narrowed.append(record)
            filtered = narrowed

        native_prefix_scan = bool(getattr(self, "_last_read_all_native_shard_scan", False))
        return {
            "records": filtered,
            "scan_stats": {
                "backend": self._backend_label(),
                "execution_mode": "native_temporalstore_shard_scan_prefilter" if native_prefix_scan else "direct_backend_hot_cache_prefilter",
                "backend_pushdown": True,
                "direct_backend_prefilter": True,
                "native_pushdown": native_prefix_scan,
                "native_prefix_scan": native_prefix_scan,
                "native_pack_assembly": False,
                "cache_hit": self._records_cache is not None,
                "record_types": sorted(allowed_types),
                "scanned_records": scanned,
                "returned_records": len(filtered),
                "dropped_by_type": dropped_type,
                "dropped_by_scope": dropped_scope,
                "dropped_by_retention": dropped_retention,
                "secondary_index_groups_supplied": len(secondary_index_groups or []),
                "secondary_index_matched_candidate_count": secondary_index_matched,
                "secondary_index_dropped_candidate_count": secondary_index_dropped,
                "secondary_index_matched_node_count": len(matched_node_hashes),
                "selected_node_hashes_supplied": len(selected_node_hashes or set()),
                "pack_assembly_location": "python_reference_packer",
                "next_native_gap": "conformance ContextPack assembly and scoring APIs",
            },
        }


    def supports_native_candidate_prefilter(self) -> bool:
        return callable(getattr(getattr(self, "_client", None), "matrixark_scan_candidates", None))

    def native_context_pack_required(self) -> bool:
        if getattr(self, "_native_context_pack_fallback_active", False):
            return False
        return super().native_context_pack_required()

    def supports_native_context_pack(self) -> bool:
        if getattr(self, "_native_context_pack_fallback_active", False):
            return False
        return callable(getattr(getattr(self, "_client", None), "matrixark_retrieve_context_pack", None))

    def native_context_pack(self, request: Json) -> Json | None:
        if getattr(self, "_native_context_pack_fallback_active", False):
            return None
        self._recover_serving_from_disk_fallback_if_needed(reason="native_context_pack")
        self.start_async_context_memory_warmup(reason="native_context_pack")
        retriever = getattr(getattr(self, "_client", None), "matrixark_retrieve_context_pack", None)
        if not callable(retriever):
            return None
        try:
            try:
                response = retriever(
                    count_key=self._count_key,
                    record_hash_key=self._record_hash_key,
                    shard_size=self._shard_size,
                    request=request,
                )
            except TypeError as exc:
                if not any(token in str(exc) for token in ("count_key", "record_hash_key", "shard_size")):
                    raise
                response = retriever(request)
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
            raise MatrixArkError(
                "native matrixark_retrieve_context_pack must return a finished ContextPack, not raw records"
            )
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
        scanner = getattr(getattr(self, "_client", None), "matrixark_scan_candidates", None)
        if not callable(scanner):
            return None
        try:
            response = scanner(
                count_key=self._count_key,
                record_hash_key=self._record_hash_key,
                shard_size=self._shard_size,
                scope=scope,
                record_types=sorted(record_types),
                secondary_index_groups=[sorted(group) for group in (secondary_index_groups or [])],
                selected_node_hashes=sorted(int(item) for item in (selected_node_hashes or set())),
            )
        except Exception as exc:
            if native_candidate_prefilter_required(backend_label=self._backend_label()) and not getattr(self, "_native_context_pack_fallback_active", False):
                raise MatrixArkError(
                    f"backend-native candidate prefilter failed for {self._backend_label()}: {exc}. "
                    "Python read_all scan/prefilter is disabled for TemporalStore serving unless explicitly overridden for local debug."
                ) from exc
            return None
        records = response.get("records") if isinstance(response, dict) else None
        if not isinstance(records, list):
            if native_candidate_prefilter_required(backend_label=self._backend_label()) and not getattr(self, "_native_context_pack_fallback_active", False):
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
        scan_stats.setdefault("pack_assembly_location", "python_reference_packer")
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

    def idle_commit_task_records(self, scope: Json) -> list[Json]:
        """Read only scheduled idle-commit tasks without broad Python materialization."""
        result = self._native_candidate_scan(
            scope=scope,
            record_types={"matrixark_async_pipeline_task"},
            secondary_index_groups=None,
            selected_node_hashes=None,
        )
        if not isinstance(result, dict):
            return []
        records = result.get("records")
        if not isinstance(records, list):
            return []
        found = [
            record
            for record in records
            if isinstance(record, dict)
            and record.get("record_type") == "matrixark_async_pipeline_task"
        ]
        # Tasks written since they gained a latest-state identity live in that hash, NOT the append
        # log the scan walks -- so the scan alone would stop seeing new tasks and the drain would
        # quietly never fire. Tasks written before it are still in the log. Both are returned, and
        # the drain's own last-write-wins fold over task_hash reconciles a task that appears in
        # both. A store that predates the change keeps working; a fresh one stops paying for the
        # log side entirely, because nothing writes there any more.
        try:
            latest_state = self._load_latest_context_state_records()
        except Exception:  # noqa: BLE001 - a missing latest-state view is not a reason to drop
            latest_state = []                     # the tasks the scan did find.
        found.extend(
            record
            for record in latest_state
            if isinstance(record, dict)
            and record.get("record_type") == "matrixark_async_pipeline_task"
        )
        return found

    def _direct_record_load_lock(self) -> threading.RLock:
        with _DIRECT_RECORD_CACHE_LOCK:
            lock = _DIRECT_RECORD_LOAD_LOCKS.get(self._storage_prefix)
            if lock is None:
                lock = threading.RLock()
                _DIRECT_RECORD_LOAD_LOCKS[self._storage_prefix] = lock
            return lock

    def _get_direct_record_cache(self, count: int) -> list[Json] | None:
        if not self.python_hot_cache_enabled():
            return None
        with _DIRECT_RECORD_CACHE_LOCK:
            cached = _DIRECT_RECORD_CACHE.get(self._storage_prefix)
            if cached is None:
                return None
            cached_count, records = cached
            if cached_count != count:
                return None
            return list(records)

    def _put_direct_record_cache(self, count: int, records: list[Json]) -> None:
        if not self.python_hot_cache_enabled():
            return
        with _DIRECT_RECORD_CACHE_LOCK:
            if len(_DIRECT_RECORD_CACHE) >= _DIRECT_RECORD_CACHE_MAX_PREFIXES and self._storage_prefix not in _DIRECT_RECORD_CACHE:
                oldest = next(iter(_DIRECT_RECORD_CACHE))
                _DIRECT_RECORD_CACHE.pop(oldest, None)
            _DIRECT_RECORD_CACHE[self._storage_prefix] = (count, list(records))

    def _drop_direct_record_cache(self) -> None:
        self._entry_count_cache = None
        self._records_cache = None
        self._index_cache = None
        with _DIRECT_RECORD_CACHE_LOCK:
            _DIRECT_RECORD_CACHE.pop(self._storage_prefix, None)
        with self._retrieval_candidate_cache_lock:
            self._retrieval_candidate_cache.clear()

    def _retrieval_candidate_cache_key(
        self,
        *,
        count: int,
        scope: Json,
        record_types: set[str] | None,
        secondary_index_groups: list[set[str]] | None,
        selected_node_hashes: set[int] | None,
    ) -> str:
        return json.dumps(
            {
                "count": count,
                "storage_prefix": self._storage_prefix,
                "scope": scope or {},
                "record_types": sorted(record_types or RETRIEVAL_HOT_RECORD_TYPES),
                "secondary_index_groups": [
                    sorted(group)
                    for group in (secondary_index_groups or [])
                ],
                "selected_node_hashes": sorted(selected_node_hashes or []),
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    def _prune_retrieval_candidate_cache(self, current_count: int) -> None:
        with _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK:
            stale_keys = [
                key
                for key, cached in _DIRECT_RETRIEVAL_CANDIDATE_CACHE.items()
                if cached.get("storage_prefix") == self._storage_prefix
                and int(cached.get("count") or -1) != int(current_count)
            ]
            for key in stale_keys:
                _DIRECT_RETRIEVAL_CANDIDATE_CACHE.pop(key, None)
            if len(_DIRECT_RETRIEVAL_CANDIDATE_CACHE) > _DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES:
                overflow = len(_DIRECT_RETRIEVAL_CANDIDATE_CACHE) - _DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES
                for key in list(_DIRECT_RETRIEVAL_CANDIDATE_CACHE)[:overflow]:
                    _DIRECT_RETRIEVAL_CANDIDATE_CACHE.pop(key, None)
        with _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK:
            stale_keys = [
                key
                for key in _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE
                if key.startswith(f"{self._storage_prefix}|")
                and f"|wm={int(current_count)}|" not in key
            ]
            for key in stale_keys:
                _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE.pop(key, None)
            if len(_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE) > _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES:
                overflow = len(_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE) - _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES
                for key in list(_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE)[:overflow]:
                    _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE.pop(key, None)

    def _placement_candidate_table_cache_key(
        self,
        *,
        count: int,
        scope_key: str,
        node_hash: int,
        record_type: str,
        resource_version_watermark: str = "",
    ) -> str:
        return (
            f"{self._storage_prefix}|wm={int(count)}|scope={stable_hash(scope_key)}|"
            f"node={int(node_hash)}|type={record_type}|rv={stable_hash(resource_version_watermark)}"
        )

    def _record_primary_hash(self, record: Json) -> int:
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
            "ref_hash",
        ):
            value = record.get(field)
            if value is not None:
                try:
                    return int(value)
                except (TypeError, ValueError):
                    break
        return stable_hash(json.dumps(record, sort_keys=True, separators=(",", ":")))

    def _placement_candidate_records_from_cache_or_load(
        self,
        *,
        count: int,
        scope: Json,
        allowed_types: set[str],
        selected_nodes: set[int],
        locations: list[Json],
        resource_version_watermark: str = "",
    ) -> Json:
        scope_key = canonical_scope_key(scope)
        if not scope_key or not selected_nodes or not allowed_types:
            return {"records": [], "cache_hit": False, "cache_entries": 0, "loaded_records": 0}

        keys = [
            self._placement_candidate_table_cache_key(
                count=count,
                scope_key=scope_key,
                node_hash=node_hash,
                record_type=record_type,
                resource_version_watermark=resource_version_watermark,
            )
            for node_hash in sorted(selected_nodes)
            for record_type in sorted(allowed_types)
        ]
        with _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK:
            cached_tables = [_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE.get(key) for key in keys]
            if keys and all(table is not None for table in cached_tables):
                compact_rows = [
                    row
                    for table in cached_tables
                    for row in (table or [])
                ]
                return {
                    "records": [dict(row[3]) for row in compact_rows],
                    "cache_hit": True,
                    "cache_entries": len(compact_rows),
                    "loaded_records": 0,
                    "resource_version_watermark": resource_version_watermark,
                }

        loaded_records = self._load_records_from_locations(locations)
        grouped: dict[str, list[tuple[str, int, int, Json]]] = {key: [] for key in keys}
        for record in loaded_records:
            record_type = str(record.get("record_type") or "")
            if record_type not in allowed_types:
                continue
            try:
                node_hash = int(record.get("node_hash"))
            except (TypeError, ValueError):
                continue
            if node_hash not in selected_nodes:
                continue
            key = self._placement_candidate_table_cache_key(
                count=count,
                scope_key=scope_key,
                node_hash=node_hash,
                record_type=record_type,
                resource_version_watermark=resource_version_watermark,
            )
            if key not in grouped:
                continue
            grouped[key].append((record_type, self._record_primary_hash(record), node_hash, dict(record)))

        with _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK:
            for key, compact_rows in grouped.items():
                _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE[key] = compact_rows
            if len(_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE) > _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES:
                overflow = len(_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE) - _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES
                for key in list(_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE)[:overflow]:
                    _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE.pop(key, None)

        compact_rows = [row for table in grouped.values() for row in table]
        return {
            "records": [dict(row[3]) for row in compact_rows],
            "cache_hit": False,
            "cache_entries": len(compact_rows),
            "loaded_records": len(loaded_records),
            "resource_version_watermark": resource_version_watermark,
        }

    def _native_index_ref_hashes(self, *, scope: Json, secondary_index_groups: list[set[str]] | None) -> Json:
        scope_key = canonical_scope_key(scope)
        groups = secondary_index_groups or []
        if not scope_key or not groups:
            return {"ref_hashes": set(), "postings_found": 0, "index_terms": [], "posting_buckets": [], "eligible": False, "reason": "missing_scope_or_filters"}
        batch_hget = getattr(self._client, "batch_hget", None)
        if not callable(batch_hget):
            return {"ref_hashes": set(), "postings_found": 0, "index_terms": [], "posting_buckets": [], "eligible": False, "reason": "backend_has_no_batch_hget"}
        index_terms = sorted({term for group in groups for term in group if term})
        entries = [{"key": self._context_index_lookup_key(scope_key), "field": term} for term in index_terms]
        try:
            rows = batch_hget(entries)
        except Exception as exc:
            return {"ref_hashes": set(), "postings_found": 0, "index_terms": index_terms, "posting_buckets": [], "eligible": False, "reason": f"index_lookup_failed:{exc}"}
        # A posting's ref set is held in bounded chunks so an append does not rewrite all of it.
        # The head names how many follow; missing them would silently narrow every search that
        # uses this term, which looks like a memory that was never stored.
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
                chunks = int(decoded.get("ref_chunks") or 0)
            except (TypeError, ValueError):
                chunks = 0
            for index in range(1, chunks + 1):
                chunk_entries.append({"key": row.get("key"), "field": f"{row.get('field')}#r{index}"})
        if chunk_entries:
            try:
                extra = batch_hget(chunk_entries)
            except Exception:
                extra = []
            if isinstance(extra, list):
                rows = list(rows) + extra
        ref_hashes: set[int] = set()
        posting_buckets: set[int] = set()
        postings_found = 0
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
            raw_refs = decoded.get("ref_hashes", []) if isinstance(decoded, dict) else []
            raw_buckets = decoded.get("posting_buckets", []) if isinstance(decoded, dict) else []
            if isinstance(raw_refs, list):
                postings_found += 1
                for value in raw_refs:
                    try:
                        ref_hash = int(value)
                    except (TypeError, ValueError):
                        continue
                    if ref_hash:
                        ref_hashes.add(ref_hash)
            if isinstance(raw_buckets, list):
                for value in raw_buckets:
                    try:
                        bucket = int(value)
                    except (TypeError, ValueError):
                        continue
                    if bucket:
                        posting_buckets.add(bucket)
        return {
            "ref_hashes": ref_hashes,
            "postings_found": postings_found,
            "index_terms": index_terms,
            "posting_buckets": sorted(posting_buckets),
            "eligible": bool(ref_hashes),
            "reason": "ok" if ref_hashes else "no_matching_postings",
        }

    def _native_locations_for_refs(self, ref_hashes: set[int]) -> Json:
        batch_hget = getattr(self._client, "batch_hget", None)
        if not callable(batch_hget) or not ref_hashes:
            return {"locations": [], "locator_rows": 0}
        entries = [{"key": self._context_ref_locator_key(), "field": str(ref_hash)} for ref_hash in sorted(ref_hashes)]
        try:
            rows = batch_hget(entries)
        except Exception:
            return {"locations": [], "locator_rows": 0}
        # A locator list longer than one chunk continues in sibling fields "{ref}#1", "{ref}#2".
        # The head names how many follow. Not reading them drops locations silently, which reads
        # as a memory that simply is not there -- so this follow-up is not an optimisation.
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
                if isinstance(extra, list):
                    rows = list(rows) + extra
            except Exception:
                pass
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
        return {"locations": locations, "locator_rows": locator_rows}

    def _load_records_from_locations(self, locations: list[Json]) -> list[Json]:
        batch_hget = getattr(self._client, "batch_hget", None)
        if not callable(batch_hget) or not locations:
            return []
        try:
            rows = batch_hget(locations)
        except Exception:
            return []
        records: list[Json] = []
        for item in rows if isinstance(rows, list) else []:
            if not isinstance(item, dict):
                continue
            payload = item.get("value", "")
            if not payload:
                continue
            try:
                decoded = json.loads(str(payload))
            except Exception:
                continue
            if isinstance(decoded, dict) and isinstance(decoded.get("record_bundle"), list):
                records.extend(row for row in decoded["record_bundle"] if isinstance(row, dict))
            elif isinstance(decoded, dict):
                records.append(decoded)
        return records

    def _native_context_pack_fallback_blocker(self, args: Json, *, reason: str) -> Json:
        scope = optional_object(args, "scope")
        query = str(args.get("query") or "")
        context_pack_id = str(stable_hash(f"native-blocked:{query}:{canonical_scope_key(scope)}:{now_ms()}"))
        pack: Json = {
            "context_pack_id": context_pack_id,
            "status": "timeout_partial",
            "native_context_pack": False,
            "context_pack_assembly": "native_context_pack_blocked",
            "query_embedding_model": embedding_model_name(),
            "embedding_execution_mode": embedding_execution_mode_name(),
            "embedding_fallback_used": embedding_fallback_used(),
            "remote_context_refs": [],
            "groups": [],
            "quality_warnings": [
                {
                    "code": "native_backend_contract_blocked",
                    "message": "Native matrixark_retrieve_context_pack was available but did not return a valid compact ContextPack; Python broad scan and hot-path pack fallback are disabled for production retrieval.",
                    "reason": reason,
                }
            ],
            "retrieval_metrics": {
                "backend": self._backend_label(),
                "native_api": "matrixark_retrieve_context_pack",
                "native_pack_assembly": False,
                "python_pack_fallback": False,
                "raw_candidate_tables_returned": False,
                "broad_scan_used": False,
                "broad_scan_blocked": True,
                "broad_scan_policy": "explicit_fallback_or_debug_only",
                "fallback_reason": reason,
                "selected_refs": 0,
                "dropped_refs": 0,
                "scanned_records": 0,
                "index_postings_read": 0,
                "placement_partitions_touched": 0,
                "candidate_cache_hit": False,
                "normal_path_stages": [
                    "query_understanding",
                    "scope_filter",
                    "l0_l1_node_traversal",
                    "compact_secondary_index_prefilter",
                    "placement_key_candidate_fetch",
                    "native_score_rerank_pack",
                ],
                "health_readiness_metrics": {
                    "health": True,
                    "readiness": True,
                    "metrics": True,
                },
            },
            "recall_policy": {
                "backend_retrieval_pushdown": {
                    "backend": self._backend_label(),
                    "execution_mode": "native_context_pack_blocked",
                    "python_materialized_records": 0,
                    "broad_scan_blocked": True,
                    "fallback_reason": reason,
                }
            },
        }
        if bool(args.get("include_retrieval_metrics")):
            pack["include_retrieval_metrics"] = True
        return pack

    def _try_native_context_pack(self, args: Json) -> Json | None:
        if env_bool("MATRIXARK_DISABLE_NATIVE_CONTEXT_PACK", False):
            return None
        if not self.supports_native_context_pack():
            return None
        scope = _native_scope_with_hashes(optional_object(args, "scope"))
        query = require_string(args, "query")
        ranking = optional_object(args, "ranking")
        if callable(getattr(self, "idle_commit_task_records", None)):
            pre_retrieval_idle_commit = pre_retrieval_idle_commit_flush(
                self,
                args,
                ranking,
                scope=scope,
            )
        else:
            pre_retrieval_idle_commit = {
                "enabled": True,
                "status": "unavailable",
                "reason": "native_idle_commit_task_reader_missing",
            }
        if pre_retrieval_idle_commit.get("status") in {"committed", "attempted"}:
            self._entry_count_cache = None
        scope_key = canonical_scope_key(scope)
        native_node_path = optional_object(args, "metadata").get("node_path")
        if not isinstance(native_node_path, list) or not native_node_path:
            native_node_path = self.default_session_node_path(scope)
        native_start_node_hash = stable_hash("/".join(str(part) for part in native_node_path))
        reference_time_ms = int(args.get("reference_time_ms", now_ms()) or now_ms())
        local_context = args.get("local_context", [])
        if not isinstance(local_context, list):
            local_context = []
        entry_count_cache = getattr(self, "_entry_count_cache", None)
        watermark_count = entry_count_cache if entry_count_cache is not None else self._get_count()
        max_context_tokens = int(args.get("max_context_tokens") or DEFAULT_MAX_CONTEXT_TOKENS)
        local_budget = local_context_budget(args)
        local_tokens = int(local_budget.get("token_estimate", 0))
        safety_margin_tokens = int(local_budget.get("safety_margin_tokens", 0))
        remote_context_budget_tokens = max(0, max_context_tokens - local_tokens - safety_margin_tokens)
        question_type = str(args.get("question_type") or infer_query_type(query))
        retrieval_session_scope = str(args.get("session_scope") or ranking.get("session_scope") or "prefer").strip().lower()
        if retrieval_session_scope not in {"prefer", "only"}:
            retrieval_session_scope = "prefer"
        cross_session_policy = build_cross_session_policy(
            args,
            ranking,
            question_type=question_type,
            session_scope=retrieval_session_scope,
            remote_budget_tokens=remote_context_budget_tokens,
        )
        source_role_budget_tokens = optional_object(args, "source_role_budget_tokens") or optional_object(ranking, "source_role_budget_tokens")
        source_role_budget_mode = "explicit" if source_role_budget_tokens else ""
        if not source_role_budget_tokens:
            source_role_budget_tokens, source_role_budget_mode = auto_source_role_budget_tokens(
                args,
                ranking,
                remote_budget_tokens=remote_context_budget_tokens,
                question_type=question_type,
            )
        memory_layer_budget_tokens = optional_object(args, "memory_layer_budget_tokens") or optional_object(ranking, "memory_layer_budget_tokens")
        memory_layer_budget_mode = "explicit" if memory_layer_budget_tokens else ""
        if not memory_layer_budget_tokens:
            memory_layer_budget_tokens, memory_layer_budget_mode = auto_memory_layer_budget_tokens(
                args,
                ranking,
                remote_budget_tokens=remote_context_budget_tokens,
                question_type=question_type,
            )
        memory_selection_policy_budget_tokens = (
            optional_object(args, "memory_selection_policy_budget_tokens")
            or optional_object(ranking, "memory_selection_policy_budget_tokens")
        )
        memory_selection_policy_budget_mode = "explicit" if memory_selection_policy_budget_tokens else ""
        if not memory_selection_policy_budget_tokens:
            memory_selection_policy_budget_tokens, memory_selection_policy_budget_mode = auto_memory_selection_policy_budget_tokens(
                args,
                ranking,
                remote_budget_tokens=remote_context_budget_tokens,
                question_type=question_type,
            )
        extraction_phase_budget_tokens = (
            optional_object(args, "extraction_phase_budget_tokens")
            or optional_object(ranking, "extraction_phase_budget_tokens")
        )
        extraction_phase_budget_mode = "explicit" if extraction_phase_budget_tokens else ""
        if not extraction_phase_budget_tokens:
            extraction_phase_budget_tokens, extraction_phase_budget_mode = auto_extraction_phase_budget_tokens(
                args,
                ranking,
                remote_budget_tokens=remote_context_budget_tokens,
                question_type=question_type,
            )
        resource_version_watermark = str(
            ranking.get("resource_version_watermark")
            or args.get("resource_version_watermark")
            or ""
        )
        skill_status_watermark = str(
            ranking.get("skill_status_watermark")
            or args.get("skill_status_watermark")
            or ""
        )
        request: Json = {
            "api_version": 1,
            "storage_prefix": getattr(self, "_storage_prefix", "matrixark:mcp"),
            "backend": self._backend_label(),
            "watermark_count": watermark_count,
            "append_watermark": watermark_count,
            "resource_version_watermark": resource_version_watermark,
            "skill_status_watermark": skill_status_watermark,
            "index_posting_watermark": watermark_count,
            "query": query,
            "question_type": question_type,
            "scope": scope,
            "session_scope": retrieval_session_scope,
            "scope_key": scope_key,
            "tenant_hash": int(scope.get("tenant_hash") or 0),
            "scope_hash": stable_hash(scope_key) if scope_key else 0,
            "start_node_hash": native_start_node_hash,
            "placement_node_hash": native_start_node_hash,
            "placement_key": f"context:{scope_key}:node={native_start_node_hash}",
            "native_start_node_path": [str(part) for part in native_node_path],
            "start_time_ms": 1,
            "end_time_ms": reference_time_ms,
            "as_of_ms": reference_time_ms,
            "max_selected_refs": int(ranking.get("max_selected_refs") or args.get("max_selected_refs") or 24),
            "min_score": float(ranking.get("min_score") or args.get("min_score") or 0.0),
            "decay_half_life_ms": int(ranking.get("half_life_ms") or 0),
            "max_depth": int(ranking.get("max_depth") or 4),
            "top_k_per_depth": int(ranking.get("top_k_per_layer") or ranking.get("top_k_per_depth") or 16),
            "max_children_scored_per_parent": int(ranking.get("max_children_scored_per_parent") or 256),
            "max_candidate_nodes": int(ranking.get("max_candidate_nodes") or 64),
            "shared_resource_max_refs": int(ranking.get("shared_resource_max_refs") or args.get("shared_resource_max_refs") or 4),
            "skill_max_refs": int(ranking.get("skill_max_refs") or args.get("skill_max_refs") or 4),
            "cross_session_max_refs": int(ranking.get("cross_session_max_refs") or args.get("cross_session_max_refs") or 4),
            "cross_session_rerank": bool(ranking.get("cross_session_rerank", True)),
            "cross_session": cross_session_policy,
            "source_role_budget_tokens": source_role_budget_tokens,
            "source_role_budget_mode": source_role_budget_mode or ("explicit" if source_role_budget_tokens else "disabled"),
            "memory_layer_budget_tokens": memory_layer_budget_tokens,
            "memory_layer_budget_mode": memory_layer_budget_mode or ("explicit" if memory_layer_budget_tokens else "disabled"),
            "memory_layer_budget_question_reason": memory_layer_budget_question_reason(question_type),
            "memory_selection_policy_budget_tokens": memory_selection_policy_budget_tokens,
            "memory_selection_policy_budget_mode": memory_selection_policy_budget_mode or (
                "explicit" if memory_selection_policy_budget_tokens else "disabled"
            ),
            "extraction_phase_budget_tokens": extraction_phase_budget_tokens,
            "extraction_phase_budget_mode": extraction_phase_budget_mode or (
                "explicit" if extraction_phase_budget_tokens else "disabled"
            ),
            "same_session_priority": bool(ranking.get("same_session_priority", True)),
            "leaf_only": bool(ranking.get("leaf_only", False)),
            "allow_broad_scan_fallback": bool(native_retrieve_fallback_allowed(args)),
            "ranking": ranking,
            "storage_options": optional_object(args, "storage_options"),
            "max_context_tokens": max_context_tokens,
            "local_context": local_context,
            "local_context_tokens": local_tokens,
            "local_context_safety_margin_tokens": safety_margin_tokens,
            "remote_context_budget_tokens": remote_context_budget_tokens,
            "reference_time_ms": reference_time_ms,
            "include_superseded": bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False)),
            "include_superseded_resources": bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False)),
            "debug_context_pack": bool(args.get("debug_context_pack") or args.get("include_retrieval_debug")),
            "include_retrieval_metrics": bool(args.get("include_retrieval_metrics")),
            "required_native_apis": [
                "health",
                "readiness",
                "metrics",
                "matrixark_batch_append_records",
                "matrixark_retrieve_context_pack",
                "compact_secondary_index_lookup",
                "placement_key_candidate_fetch",
            ],
            "normal_path_stages": [
                "query_understanding",
                "scope_filter",
                "l0_l1_node_traversal",
                "compact_secondary_index_prefilter",
                "placement_key_candidate_fetch",
                "native_score_rerank_pack",
            ],
            "normalization_requirements": {
                "scope_key": "canonical",
                "node_hash": "integer",
                "placement_key": "context:{scope_key}:node={node_hash}",
                "resource_visibility": "apply_scope_before_scoring",
                "skill_visibility": "apply_scope_before_scoring",
                "shared_resource_scope": "tenant_or_global_visible_before_scoring",
                "stale_superseded_state": "exclude_unless_include_superseded_resources",
            },
            "execution_plan_requirements": {
                "phase": "phase4_native_score_rerank_pack",
                "context_record_route": "context:{scope_key}:node={node_hash}",
                "traversal": "score_l0_l1_then_fetch_selected_node_partitions",
                "candidate_fetch": "selected_node_placement_partitions_only",
                "candidate_cache": "scope_key+node_hash+record_type+append_watermark+resource_version_watermark",
                "candidate_cache_payload": "compact_structs_not_json_strings",
                "secondary_index": "compact_postings_by_scope_index_time_bucket",
                "scoring": "native_embedding_similarity_temporal_decay_business_boost_same_session_boost",
                "quotas": "native_shared_resource_quota_cross_session_quota_current_session_priority",
                "rerank": "native_score_fusion_then_budget_aware_rerank",
                "token_budget_pack": "native_budget_pack_with_selected_refs_and_dropped_summary",
                "pack_assembly": "native_score_rank_budget_pack_selected_refs_dropped_summary",
                "python_role": "dispatcher_only_no_candidate_materialization_no_hot_path_pack",
                "write_path": "native_batch_append_records_append_queue_coalesced_persistence",
                "write_route": "placement_key_partition_route_before_persistence",
                "write_coalescing": "native_append_queue_coalesces_by_record_key_field",
                "durability": "storage_options_select_async_sync_shared_store_or_raft",
                "retrieval_hot_path_audit": "inline_counters_only_no_full_audit_blocking",
                "context_pack_audit": "sample_or_enqueue_async_policy_enabled",
                "full_replay_audit_default": "disabled",
                "broad_prefix_scan": "disabled_unless_explicit_debug_fallback",
                "fallback_telemetry_required": True,
                "health_readiness_metrics": "native_backend_must_expose_health_readiness_metrics",
                "normal_path": "query_understanding_scope_filter_l0_l1_traversal_compact_index_placement_fetch_native_score_rerank_pack",
            },
            "required_output": {
                "context_pack": True,
                "selected_refs": True,
                "dropped_summary": True,
                "drop_counters": [
                    "scope",
                    "placement",
                    "index_filter",
                    "stale",
                    "token_budget",
                    "score_threshold",
                    "source_role_budget",
                    "memory_layer_budget",
                    "memory_selection_policy_budget",
                    "extraction_phase_budget",
                ],
                "telemetry": True,
                "retrieval_metrics": bool(args.get("include_retrieval_metrics")),
                "placement_partitions_touched": True,
                "index_postings_read": True,
                "candidate_cache_hit": True,
                "candidate_cache_key_shape": True,
                "native_pack_assembly": True,
                "raw_candidate_tables": False,
                "python_pack_fallback": False,
                "broad_scan_used": True,
                "normal_path_stages": True,
                "health_readiness_metrics": True,
            },
        }
        started_perf = time.perf_counter()
        try:
            response = self.native_context_pack(request)
            if response is None:
                if not native_retrieve_fallback_allowed(args):
                    return self._native_context_pack_fallback_blocker(args, reason="native_context_pack_unavailable")
                return None
        except Exception as exc:
            _mcp_debug_log(f"matrixark native context pack failed: {exc}")
            if not native_retrieve_fallback_allowed(args):
                return self._native_context_pack_fallback_blocker(args, reason=f"native_context_pack_error:{exc}")
            return None
        try:
            pack = json.loads(response) if isinstance(response, str) else response
        except Exception as exc:
            _mcp_debug_log(f"matrixark native context pack returned invalid JSON: {exc}")
            if not native_retrieve_fallback_allowed(args):
                return self._native_context_pack_fallback_blocker(args, reason=f"native_context_pack_invalid_json:{exc}")
            return None
        if not isinstance(pack, dict):
            if not native_retrieve_fallback_allowed(args):
                return self._native_context_pack_fallback_blocker(args, reason="native_context_pack_not_object")
            return None
        native_envelope = dict(pack)
        if isinstance(pack.get("context_pack"), dict):
            inner_pack = dict(pack["context_pack"])
            if isinstance(native_envelope.get("scan_stats"), dict):
                recall_policy = inner_pack.get("recall_policy") if isinstance(inner_pack.get("recall_policy"), dict) else {}
                recall_policy.setdefault("scan_stats", native_envelope["scan_stats"])
                inner_pack["recall_policy"] = recall_policy
            if isinstance(native_envelope.get("retrieval_metrics"), dict) and not isinstance(inner_pack.get("retrieval_metrics"), dict):
                inner_pack["retrieval_metrics"] = native_envelope["retrieval_metrics"]
            if native_envelope.get("selected_ref_count") is not None:
                inner_pack.setdefault("selected_ref_count", native_envelope.get("selected_ref_count"))
            if native_envelope.get("dropped_ref_count") is not None:
                inner_pack.setdefault("dropped_ref_count", native_envelope.get("dropped_ref_count"))
            pack = inner_pack
        selected_refs = pack.get("selected_refs", [])
        groups = pack.get("groups", [])
        if not isinstance(selected_refs, list) and not isinstance(groups, (list, dict)):
            if not native_retrieve_fallback_allowed(args):
                return self._native_context_pack_fallback_blocker(args, reason="native_context_pack_missing_refs_or_groups")
            return None
        compact_dropped_refs = 0
        if isinstance(selected_refs, list) and selected_refs:
            compact_refs, compact_dropped_refs = _compact_native_selected_refs(selected_refs)
            if compact_refs and (compact_dropped_refs or len(compact_refs) != len(selected_refs)):
                pack["selected_refs"] = compact_refs
                pack["remote_context_refs"] = compact_refs
                selected_refs = compact_refs
            compact_token_total = 0
            for ref in selected_refs:
                if not isinstance(ref, dict):
                    continue
                try:
                    compact_token_total += int(ref.get("token_estimate") or 0)
                except (TypeError, ValueError):
                    compact_token_total += max(1, (len(str(ref.get("text") or "")) + 3) // 4)
            if compact_token_total > 0:
                pack["used_context_tokens"] = compact_token_total
                pack["used_remote_context_tokens"] = compact_token_total
        raw_candidate_tables = (
            pack.get("candidate_records")
            or pack.get("raw_candidate_records")
            or pack.get("candidate_tables")
            or pack.get("raw_candidate_tables")
        )
        if raw_candidate_tables:
            _mcp_debug_log("matrixark native context pack returned raw candidate tables")
            if not native_retrieve_fallback_allowed(args):
                blocker = self._native_context_pack_fallback_blocker(args, reason="native_context_pack_returned_raw_candidate_tables")
                blocker["retrieval_metrics"]["raw_candidate_tables_returned"] = True
                return blocker
            return None
        pack.setdefault("context_pack_id", str(stable_hash(f"native:{query}:{canonical_scope_key(scope)}:{now_ms()}")))
        pack.setdefault("context_pack_assembly", "native_direct")
        pack.setdefault("native_context_pack", True)
        pack.setdefault("query_embedding_model", embedding_model_name())
        pack.setdefault("embedding_execution_mode", embedding_execution_mode_name())
        pack.setdefault("embedding_fallback_used", embedding_fallback_used())
        if bool(args.get("include_retrieval_metrics")):
            pack["include_retrieval_metrics"] = True
        if selected_refs and "remote_context_refs" not in pack:
            pack["remote_context_refs"] = selected_refs
        if "recall_policy" not in pack:
            pack["recall_policy"] = {}
        if isinstance(pack["recall_policy"], dict):
            pack["recall_policy"]["pre_retrieval_idle_commit"] = pre_retrieval_idle_commit
            native_telemetry = pack.get("retrieval_metrics") if isinstance(pack.get("retrieval_metrics"), dict) else {}
            scan_stats = pack["recall_policy"].get("scan_stats") if isinstance(pack["recall_policy"].get("scan_stats"), dict) else {}
            if scan_stats:
                merged_native_telemetry = dict(scan_stats)
                merged_native_telemetry.update(native_telemetry)
                native_telemetry = merged_native_telemetry
            native_stage_metrics = native_telemetry.get("stages") if isinstance(native_telemetry.get("stages"), dict) else {}
            total_native_ms = round((time.perf_counter() - started_perf) * 1000.0, 3)
            selected_count = len(selected_refs) if isinstance(selected_refs, list) else 0
            pack_ms = float(native_telemetry.get("pack_ms") or native_stage_metrics.get("pack_ms") or 0.0)
            index_postings_read = int(
                native_telemetry.get("index_postings_read")
                or native_telemetry.get("index_postings_touched")
                or native_telemetry.get("native_index_postings_found")
                or 0
            )
            candidate_cache_hit = bool(
                native_telemetry.get("candidate_cache_hit", native_telemetry.get("cache_hit", False))
            )
            native_fallback_flags = native_telemetry.get("fallback_flags")
            if isinstance(native_fallback_flags, str):
                fallback_flags = [native_fallback_flags]
            elif isinstance(native_fallback_flags, list):
                fallback_flags = [str(flag) for flag in native_fallback_flags if str(flag)]
            else:
                fallback_flags = []
            retrieval_metrics = {
                "query_plan_ms": round(float(native_telemetry.get("query_plan_ms") or native_stage_metrics.get("query_plan_ms") or 0.0), 3),
                "node_traversal_ms": round(float(native_telemetry.get("node_traversal_ms") or native_stage_metrics.get("node_traversal_ms") or 0.0), 3),
                "index_prefilter_ms": round(float(native_telemetry.get("index_prefilter_ms") or native_stage_metrics.get("index_prefilter_ms") or 0.0), 3),
                "candidate_fetch_ms": round(float(native_telemetry.get("candidate_fetch_ms") or native_stage_metrics.get("candidate_fetch_ms") or 0.0), 3),
                "score_ms": round(float(native_telemetry.get("score_ms") or native_stage_metrics.get("score_ms") or 0.0), 3),
                "pack_ms": round(pack_ms, 3),
                "audit_ms": round(float(native_telemetry.get("audit_ms") or native_stage_metrics.get("audit_ms") or 0.0), 3),
                "append_queue_wait_ms": round(_float_metric_or_default(native_telemetry, "append_queue_wait_ms", self._append_queue_wait_ms_avg()), 3),
                "append_engine_ms": round(_float_metric_or_default(native_telemetry, "append_engine_ms", self._append_engine_ms_avg()), 3),
                "selected_refs": selected_count,
                "dropped_refs": int(native_telemetry.get("dropped_refs") or native_telemetry.get("dropped_ref_count") or 0) + compact_dropped_refs,
                "requested_max_context_tokens": int(
                    pack.get("requested_max_context_tokens")
                    or native_telemetry.get("requested_max_context_tokens")
                    or request.get("max_context_tokens")
                    or 0
                ),
                "used_local_context_tokens": int(
                    pack.get("used_local_context_tokens")
                    or native_telemetry.get("used_local_context_tokens")
                    or request.get("local_context_tokens")
                    or 0
                ),
                "used_remote_context_tokens": int(
                    pack.get("used_remote_context_tokens")
                    or native_telemetry.get("used_remote_context_tokens")
                    or pack.get("used_context_tokens")
                    or 0
                ),
                "total_prompt_context_tokens": int(
                    pack.get("total_prompt_context_tokens")
                    or native_telemetry.get("total_prompt_context_tokens")
                    or (
                        int(pack.get("used_remote_context_tokens") or pack.get("used_context_tokens") or 0)
                        + int(pack.get("used_local_context_tokens") or request.get("local_context_tokens") or 0)
                    )
                ),
                "remote_context_budget_tokens": int(
                    pack.get("remote_context_budget_tokens")
                    or native_telemetry.get("remote_context_budget_tokens")
                    or 0
                ),
                "local_context_safety_margin_tokens": int(
                    pack.get("local_context_safety_margin_tokens")
                    or native_telemetry.get("local_context_safety_margin_tokens")
                    or request.get("local_context_safety_margin_tokens")
                    or 0
                ),
                "local_context_count": int(native_telemetry.get("local_context_count") or 0),
                "remote_is_additive_only_within_remaining_budget": True,
                "scanned_records": int(native_telemetry.get("scanned_records") or 0),
                "candidate_cache_hit": candidate_cache_hit,
                "cache_hit": candidate_cache_hit,
                "placement_partitions_touched": int(native_telemetry.get("placement_partitions_touched") or 0),
                "placement_fetch_count": int(native_telemetry.get("placement_fetch_count") or 0),
                "index_postings_read": index_postings_read,
                "index_postings_touched": index_postings_read,
                "compact_index_bucket_used": bool(native_telemetry.get("compact_index_bucket_used", False)),
                "compact_index_bucket_count": int(native_telemetry.get("compact_index_bucket_count") or 0),
                "candidate_cache_key_shape": str(native_telemetry.get("candidate_cache_key_shape") or "scope_key+node_hash+record_type+append_watermark+resource_version_watermark"),
                "native_pack_assembly": True,
                "python_pack_fallback": False,
                "raw_candidate_tables_returned": False,
                "broad_scan_used": bool(native_telemetry.get("broad_scan_used", False)),
                "broad_scan_blocked": bool(native_telemetry.get("broad_scan_blocked", False)),
                "broad_scan_fallback_allowed": bool(native_telemetry.get("broad_scan_fallback_allowed", False)),
                "timeout_count": int(native_telemetry.get("timeout_count") or 0),
                "fallback_flags": fallback_flags,
                "broad_scan_policy": "explicit_fallback_or_debug_only",
                "fallback_reason": str(native_telemetry.get("fallback_reason") or ""),
                "normal_path_stages": list(request["normal_path_stages"]),
                "health_readiness_metrics": {
                    "health": True,
                    "readiness": True,
                    "metrics": True,
                },
                "native_context_pack_ms": total_native_ms,
                "source": "native_context_pack",
                "pre_retrieval_idle_commit": pre_retrieval_idle_commit,
            }
            native_candidate_class_counts = native_telemetry.get("candidate_class_counts")
            if isinstance(native_candidate_class_counts, dict):
                retrieval_metrics["candidate_class_counts"] = native_candidate_class_counts
            native_correctness = (
                native_telemetry.get("correctness_evidence")
                if isinstance(native_telemetry.get("correctness_evidence"), dict)
                else {}
            )
            if native_correctness:
                retrieval_metrics["correctness_evidence"] = {
                    "scope_filtering": bool(native_correctness.get("scope_filtering")),
                    "placement_filtering": bool(native_correctness.get("placement_filtering")),
                    "compact_secondary_index_prefilter": bool(
                        native_correctness.get("compact_secondary_index_prefilter")
                    ),
                    "stale_superseded_exclusion": bool(
                        native_correctness.get("stale_superseded_exclusion")
                    ),
                    "shared_resource_skill_quota": bool(
                        native_correctness.get("shared_resource_skill_quota")
                    ),
                    "cross_session_quota_rerank": bool(
                        native_correctness.get("cross_session_quota_rerank")
                    ),
                }
            native_drop_counters = native_telemetry.get("drop_counters") if isinstance(native_telemetry.get("drop_counters"), dict) else {}
            if not native_drop_counters:
                native_drop_counters = pack.get("drop_counters") if isinstance(pack.get("drop_counters"), dict) else {}
            if not native_drop_counters and isinstance(pack.get("dropped_refs"), dict):
                dropped = pack.get("dropped_refs", {})
                native_drop_counters = {
                    "scope": int(dropped.get("scope", 0) or dropped.get("access_denied", 0) or 0),
                    "placement": int(dropped.get("placement", 0) or dropped.get("placement_filter", 0) or 0),
                    "index_filter": int(dropped.get("index_filter", 0) or dropped.get("secondary_index_filter", 0) or 0),
                    "stale": int(dropped.get("stale", 0) or dropped.get("superseded", 0) or 0),
                    "token_budget": int(dropped.get("over_budget", 0) or dropped.get("max_selected_refs", 0) or 0),
                    "score_threshold": int(dropped.get("low_score", 0) or dropped.get("score_threshold", 0) or 0),
                }
            if compact_dropped_refs:
                native_drop_counters = dict(native_drop_counters or {})
                native_drop_counters["token_budget"] = int(native_drop_counters.get("token_budget") or 0) + compact_dropped_refs
            if native_drop_counters:
                retrieval_metrics["drop_counters"] = native_drop_counters
                if not int(retrieval_metrics.get("dropped_refs") or 0):
                    dropped_total = 0
                    for value in native_drop_counters.values():
                        try:
                            dropped_total += int(value or 0)
                        except (TypeError, ValueError):
                            continue
                    retrieval_metrics["dropped_refs"] = dropped_total
            pack["retrieval_metrics"] = retrieval_metrics
            pack["recall_policy"].setdefault(
                "backend_retrieval_pushdown",
                {
                    "backend": self._backend_label(),
                    "execution_mode": "native_context_pack",
                    "native_pack_assembly": True,
                    "watermark_count": request["watermark_count"],
                    "python_materialized_records": 0,
                },
            )
            pack["recall_policy"].setdefault(
                "stage_latency_budgets",
                {
                    "native_context_pack_ms": total_native_ms,
                    "metrics": retrieval_metrics,
                },
            )
        dropped_refs = pack.get("dropped_refs")
        if isinstance(dropped_refs, list):
            pack["dropped_refs"] = {"refs": dropped_refs, "native_summary": True}
        elif not isinstance(dropped_refs, dict):
            pack["dropped_refs"] = {"refs": [], "native_summary": True}
        audit_mode = str(
            args.get("audit_mode") or os.environ.get("MATRIXARK_CONTEXT_AUDIT_MODE", "telemetry_only")
        ).strip().lower()
        if audit_mode not in {"full", "telemetry_only", "off"}:
            audit_mode = "telemetry_only"
        try:
            audit_sample_rate = clamp01(float(args.get("audit_sample_rate", os.environ.get("MATRIXARK_CONTEXT_AUDIT_SAMPLE_RATE", 0.01))))
        except (TypeError, ValueError):
            audit_sample_rate = 0.01
        audit_record = {
            "record_type": "context_pack_audit",
            "context_pack_id": pack.get("context_pack_id", ""),
            "query": query,
            "scope": scope,
            "summary_text": summarize_text(" ".join(str(ref.get("text", "")) for ref in selected_refs if isinstance(ref, dict)), limit=512),
            "selected_refs": compact_refs_for_audit(selected_refs if isinstance(selected_refs, list) else []),
            "local_context_refs": [],
            "context_sources_order": pack.get("context_sources_order", ["local_context", "matrixark_remote_context"]),
            "selected_ref_counts": pack.get("selected_ref_counts", {}),
            "dropped_refs": pack.get("dropped_refs", {}),
            "quality_warnings": pack.get("quality_warnings", []),
            "partial_context_pack": bool(pack.get("partial_context_pack", False)),
            "question_type": pack.get("question_type", ""),
            "packing_policy": pack.get("packing_policy", "native_context_pack"),
            "recall_policy": pack.get("recall_policy", {}),
            "storage_options": optional_object(args, "storage_options"),
            "used_local_context_tokens": pack.get("used_local_context_tokens", 0),
            "used_remote_context_tokens": pack.get("used_remote_context_tokens", pack.get("used_context_tokens", 0)),
            "total_prompt_context_tokens": pack.get("total_prompt_context_tokens", pack.get("used_context_tokens", 0)),
            "remote_context_budget_tokens": pack.get("remote_context_budget_tokens", 0),
            "requested_max_context_tokens": pack.get("requested_max_context_tokens", args.get("max_context_tokens", 0)),
            "local_context_safety_margin_tokens": pack.get("local_context_safety_margin_tokens", 0),
            "budget_source": pack.get("budget_source", "native_context_pack"),
            "primary_candidate_count": pack.get("primary_candidate_count", 0),
            "auxiliary_candidate_count": pack.get("auxiliary_candidate_count", 0),
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
        if bool(args.get("debug_context_pack")) or bool(args.get("include_retrieval_debug")):
            return pack
        return compact_context_pack_for_serving(pack)

