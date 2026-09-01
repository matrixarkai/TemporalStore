# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_LocalAdapterRetrievalMixin methods split from matrixark_mcp_local_adapter.MatrixArkLocalAdapter (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # names owned by the parent module
    from tools.matrixark_mcp_local_adapter import (
    Any,
    RETRIEVAL_HOT_RECORD_TYPES,
    _drop_time_expired_records,
    context_source_lineage,
    source_event_lineage_summary,
)
except ImportError:
    from matrixark_mcp_local_adapter import (
    Any,
    RETRIEVAL_HOT_RECORD_TYPES,
    _drop_time_expired_records,
    context_source_lineage,
    source_event_lineage_summary,
)


def _idle_drain_min_interval_ms() -> int:
    """How long a quiet session may go unchecked for a due idle commit (default 1s, 0 disables)."""
    try:
        return max(0, int(os.environ.get("MATRIXARK_IDLE_DRAIN_MIN_INTERVAL_MS", "1000")))
    except (TypeError, ValueError):
        return 1000




# Fields the retrieval scan actually reads, measured with a probe that recorded every key access
# rather than chosen by inspection. `text`, `heading` and `source_locator` are absent because the
# scan never asks for them -- they exist to RETURN a hit, not to find one.
RETRIEVAL_SCAN_FIELDS = (
    "record_type", "node_path", "access_scope", "metadata", "scope", "scope_key", "envelope",
    "node_hash", "memory_scope", "session_continuity", "embedding_meta", "vector",
    "event_id_hash", "entity_hash", "segment_hash", "summary_hash", "compression_id_hash",
    "chunk_hash", "section_hash", "skill_hash", "ref_hash", "ref_hashes", "ref_type",
    "index_name", "batch_id_hash", "batch_id_hashes", "node_hashes", "updated_at_ms",
    "stale_or_superseded", "superseded_by_ref_hash", "superseded_by_entity_hash",
    "profile_shadowed_by_ref_hash", "expires_at", "tombstone_kind", "posting_part",
)

# OFF by default. A projected record cannot serve the resource and skill scans' lexical, keyword and
# origin terms, which read `text`; enabling this without hydrating those candidates changes ranking.
RETRIEVAL_SCAN_PROJECTION = os.environ.get(
    "MATRIXARK_RETRIEVAL_PROJECT_SCAN_FIELDS", "0"
).strip().lower() not in {"0", "false", "no", "off", ""}


def project_scan_record(record):
    """Keep only what the scan reads. Returns the record unchanged when projection is off."""
    if not RETRIEVAL_SCAN_PROJECTION:
        return record
    return {key: value for key, value in record.items() if key in RETRIEVAL_SCAN_FIELDS}


class _LocalAdapterRetrievalMixin:
    def reload_context_hot_state_from_disk(self, *, scope: Json | None = None) -> Json:
        """Rebuild process-local serving state from the durable JSONL record log."""

        records = self.read_all()
        if scope:
            warm_records = [
                record
                for record in records
                if (
                    not isinstance(record, dict)
                    or not record.get("scope")
                    or access_scope_matches_before_scoring(record, scope)
                    or scope_matches(candidate_access_scope(record), scope)
                )
            ]
        else:
            warm_records = records

        with self._session_buffer_cache_lock:
            self._context_event_by_hash = {}
            self._session_pending_event_ids_by_key = {}
            self._session_committed_event_ids_by_key = {}
        self._latest_entity_by_hash = {}
        self._context_node_hashes = set()
        self._context_child_ref_hashes = set()
        self._entity_cache_loaded = not bool(scope)
        self._context_node_cache_loaded = not bool(scope)
        self._update_latest_entity_cache(warm_records)
        with self._retrieval_records_cache_lock:
            self._retrieval_records_cache_generation += 1
            self._retrieval_records_cache.clear()
        with self._context_pack_cache_lock:
            self._context_pack_cache.clear()
        return {
            "status": "reloaded",
            "backend": getattr(self, "_backend_label", lambda: "local")(),
            "source": "disk_jsonl",
            "event_log": str(self.event_log),
            "records_scanned": len(records),
            "records_warmed": len(warm_records),
            "context_events_loaded": len(self._context_event_by_hash),
            "context_nodes_loaded": len(self._context_node_hashes),
            "context_child_refs_loaded": len(self._context_child_ref_hashes),
            "context_entities_loaded": len(self._latest_entity_by_hash),
            "scope_limited": bool(scope),
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
        """Return records eligible for retrieval hot-path scan/filter/pack.

        conformance backends override this seam with native prefix scans and
        secondary-index prefiltering. The local adapter keeps the reference
        behavior by filtering the JSONL record log before Python scoring.
        """

        allowed_types = record_types or RETRIEVAL_HOT_RECORD_TYPES
        scope_key = canonical_scope_key(scope)
        secondary_key = tuple(sorted(tuple(sorted(group)) for group in (secondary_index_groups or [])))
        selected_key = tuple(sorted(int(item) for item in (selected_node_hashes or set())))
        cache_key = (
            self._retrieval_records_cache_generation,
            scope_key,
            session_scope_mode(scope),
            tuple(sorted(allowed_types)),
            secondary_key,
            selected_key,
        )
        with self._retrieval_records_cache_lock:
            cached = self._retrieval_records_cache.get(cache_key)
            if cached is not None:
                scan_stats = dict(cached.get("scan_stats", {}))
                scan_stats["cache_hit"] = True
                # Re-check TTL on every cache hit so a record that expired since the cache was
                # built (with no intervening write) stops surfacing from retrieve.
                return {"records": _drop_time_expired_records(cached.get("records", [])), "scan_stats": scan_stats}
        raw_records = self.read_all()
        node_scope_by_hash: dict[int, Json] = {}
        embedding_scope_by_ref: dict[tuple[str, Any], Json] = {}
        ref_scope_by_key: dict[tuple[str, Any], Json] = {}

        def remember_ref_scope(ref_type: str, ref_hash: Any, source_record: Json) -> None:
            if ref_hash in (None, ""):
                return
            source_scope = candidate_access_scope(source_record)
            if source_scope:
                ref_scope_by_key.setdefault((ref_type, ref_hash), source_scope)

        for source_record in raw_records:
            source_record_type = str(source_record.get("record_type") or "")
            if source_record_type == "context_event":
                remember_ref_scope("event", source_record.get("event_id_hash"), source_record)
            elif source_record_type == "context_entity":
                remember_ref_scope("entity", source_record.get("entity_hash"), source_record)
            elif source_record_type == "context_segment":
                remember_ref_scope("segment", source_record.get("segment_hash"), source_record)
            elif source_record_type == "context_summary":
                remember_ref_scope("summary", source_record.get("summary_hash") or source_record.get("node_hash"), source_record)
            elif source_record_type == "context_compression_event":
                remember_ref_scope("compression", source_record.get("compression_id_hash"), source_record)
            if source_record.get("record_type") == "context_embedding" and source_record.get("ref_hash") not in (None, ""):
                embedding_scope = candidate_access_scope(source_record)
                if embedding_scope:
                    ref_type = str(source_record.get("ref_type") or "")
                    embedding_scope_by_ref[(ref_type, source_record.get("ref_hash"))] = embedding_scope
            try:
                source_node_hash = int(source_record.get("node_hash") or 0)
            except (TypeError, ValueError):
                source_node_hash = 0
            if not source_node_hash or source_node_hash in node_scope_by_hash:
                continue
            source_scope = candidate_access_scope(source_record)
            if source_scope:
                node_scope_by_hash[source_node_hash] = source_scope

        def scope_from_node_path(node_path: Any) -> Json:
            if not isinstance(node_path, list):
                return {}
            recovered_scope: Json = {}
            for part in node_path:
                value = str(part or "")
                if value.startswith("tenant:"):
                    recovered_scope["tenant_id"] = value.split(":", 1)[1]
                elif value.startswith("user:"):
                    recovered_scope["user_id"] = value.split(":", 1)[1]
                elif value.startswith("session:"):
                    recovered_scope["session_id"] = value.split(":", 1)[1]
            return {key: value for key, value in recovered_scope.items() if value}

        def recovered_record_scope(record: Json) -> Json:
            record_scope = candidate_access_scope(record)
            if record_scope:
                return record_scope
            # A folded owner carries the retired embedding record's fields under embedding_meta;
            # the access scope that used to be recovered from the separate record is there.
            meta = record.get("embedding_meta")
            if isinstance(meta, dict):
                record_scope = candidate_access_scope(meta)
                if record_scope:
                    return record_scope
            if record.get("record_type") == "context_embedding":
                ref_scope = ref_scope_by_key.get((str(record.get("ref_type") or ""), record.get("ref_hash")))
                if ref_scope:
                    return ref_scope
            ref_scope_fields = {
                "context_event": ("event", "event_id_hash"),
                "context_entity": ("entity", "entity_hash"),
                "context_segment": ("segment", "segment_hash"),
                "context_compression_event": ("compression", "compression_id_hash"),
                "context_summary": ("summary", "summary_hash"),
            }
            ref_scope_field = ref_scope_fields.get(str(record.get("record_type") or ""))
            if ref_scope_field is not None:
                ref_type, hash_field = ref_scope_field
                ref_hash = record.get(hash_field)
                if ref_hash in (None, "") and hash_field == "summary_hash":
                    ref_hash = record.get("node_hash")
                embedding_scope = embedding_scope_by_ref.get((ref_type, ref_hash))
                if embedding_scope:
                    return embedding_scope
            try:
                node_hash = int(record.get("node_hash") or 0)
            except (TypeError, ValueError):
                node_hash = 0
            if node_hash and node_hash in node_scope_by_hash:
                return node_scope_by_hash[node_hash]
            return scope_from_node_path(record.get("node_path", []))

        def recovered_scope_for_query(record: Json, query_scope: Json) -> Json:
            record_scope = recovered_record_scope(record)
            if (
                record_scope
                and query_scope.get("account_id")
                and not record_scope.get("account_id")
                and (record_scope.get("tenant_id") or record_scope.get("user_id") or record_scope.get("session_id"))
            ):
                record_scope = {**record_scope, "account_id": query_scope.get("account_id")}
            return record_scope

        def recovered_scope_matches(record: Json, query_scope: Json) -> bool:
            return scope_matches(recovered_scope_for_query(record, query_scope), query_scope)

        def profile_bridge_scope_matches(record: Json, query_scope: Json) -> bool:
            if not bool(query_scope.get("_allow_profile_bridge")):
                return False
            memory_scope = str(record.get("memory_scope") or "").strip().lower()
            session_continuity = str(record.get("session_continuity") or "").strip().lower()
            data_model = str(record.get("data_model") or "").strip().lower()
            embedding_type = str(record.get("embedding_type") or "").strip().lower()
            node_path = [str(part or "") for part in record.get("node_path", []) if str(part or "")]
            is_profile_record = (
                memory_scope in {"user_profile", "profile", "cross_session_profile"}
                or data_model == "context_profile_entity"
                or embedding_type == "profile_entity_state"
                or "profile:long_term_memory" in node_path
            )
            if not is_profile_record:
                return False
            if session_continuity and session_continuity != "cross_session" and memory_scope == "session":
                return False
            record_scope = recovered_scope_for_query(record, query_scope)
            if not record_scope and node_path:
                record_scope = scope_from_node_path(node_path)
                if query_scope.get("account_id") and not record_scope.get("account_id"):
                    record_scope = {**record_scope, "account_id": query_scope.get("account_id")}
            record_key_parts = parse_scope_key(str(record_scope.get("scope_key") or ""))
            for field in ["account_id", "account_hash", "tenant_id", "tenant_hash", "user_id", "user_hash"]:
                query_value = query_scope.get(field)
                record_value = record_scope.get(field)
                if query_value and record_value and query_value != record_value:
                    return False
            tenant_string_matched = bool(
                query_scope.get("tenant_id")
                and record_scope.get("tenant_id")
                and query_scope.get("tenant_id") == record_scope.get("tenant_id")
            )
            user_string_matched = bool(
                query_scope.get("user_id")
                and record_scope.get("user_id")
                and query_scope.get("user_id") == record_scope.get("user_id")
            )
            try:
                if (
                    query_scope.get("tenant_hash")
                    and not tenant_string_matched
                    and record_key_parts.get("t") != int(query_scope.get("tenant_hash"))
                ):
                    return False
                if (
                    query_scope.get("user_hash")
                    and not user_string_matched
                    and record_key_parts.get("u") != int(query_scope.get("user_hash"))
                ):
                    return False
            except (TypeError, ValueError):
                return False
            return bool(
                record_scope.get("tenant_id")
                or record_scope.get("tenant_hash")
                or record_key_parts.get("t")
            )

        def session_scope_allows_record(record: Json, query_scope: Json) -> bool:
            if session_scope_mode(query_scope) != "only":
                return True
            query_session = str(query_scope.get("session_id") or "").strip()
            if not query_session:
                return True
            record_type = str(record.get("record_type") or "")
            if not record_type.startswith("context_") and record_type != "matrixark_async_pipeline_task":
                return True
            if profile_bridge_scope_matches(record, query_scope):
                return True
            memory_scope = str(record.get("memory_scope") or "").strip().lower()
            session_continuity = str(record.get("session_continuity") or "").strip().lower()
            if memory_scope in {"user_profile", "profile", "cross_session_profile"} or session_continuity == "cross_session":
                return False
            record_scope = recovered_scope_for_query(record, query_scope)
            record_session = str(record_scope.get("session_id") or "").strip()
            if record_session and record_session != query_session:
                return False
            return True

        def profile_summary_path_matches(record: Json, query_scope: Json) -> bool:
            if record.get("record_type") != "context_summary":
                return False
            node_path = [str(part or "") for part in record.get("node_path", []) if str(part or "")]
            if "profile:long_term_memory" not in node_path:
                return False
            path_scope = scope_from_node_path(node_path)
            if query_scope.get("account_id") and not path_scope.get("account_id"):
                path_scope = {**path_scope, "account_id": query_scope.get("account_id")}
            return scope_matches(path_scope, query_scope)

        secondary_matched_index_count = 0
        secondary_embedding_matched_count = 0
        secondary_posting_ref_hashes: set[str] = set()
        secondary_posting_node_hashes: set[str] = set()
        secondary_posting_batch_hashes: set[str] = set()
        required_index_terms = {term for group in (secondary_index_groups or []) for term in group if term}
        if required_index_terms:
            for index_record in raw_records:
                if str(index_record.get("record_type") or "") != "context_index":
                    continue
                index_name = str(index_record.get("index_name") or "")
                if index_name not in required_index_terms:
                    continue
                if (
                    not recovered_scope_matches(index_record, scope)
                    and not profile_summary_path_matches(index_record, scope)
                    and not profile_bridge_scope_matches(index_record, scope)
                ):
                    continue
                secondary_matched_index_count += 1
                for ref_hash in context_index_record_ref_hashes(index_record):
                    if ref_hash is not None:
                        secondary_posting_ref_hashes.add(str(ref_hash))
                for node_hash in context_index_record_node_hashes(index_record):
                    if node_hash is not None:
                        secondary_posting_node_hashes.add(str(node_hash))
                batch_hashes = index_record.get("batch_id_hashes", [])
                if isinstance(batch_hashes, list):
                    for batch_hash in batch_hashes:
                        if batch_hash is not None:
                            secondary_posting_batch_hashes.add(str(batch_hash))
                batch_hash = index_record.get("batch_id_hash")
                if batch_hash is not None:
                    secondary_posting_batch_hashes.add(str(batch_hash))

            for embedding_record in raw_records:
                if str(embedding_record.get("record_type") or "") != "context_embedding":
                    continue
                embedding_type = embedding_record.get("embedding_type")
                ref_hash = embedding_record.get("ref_hash")
                if ref_hash in (None, ""):
                    continue
                if not recovered_scope_matches(embedding_record, scope) and not profile_bridge_scope_matches(embedding_record, scope):
                    continue
                synthetic_record: Json | None = None
                if embedding_type == "event_text" and embedding_record.get("ref_type") in {"event", None, ""}:
                    synthetic_record = {
                        **embedding_record,
                        "record_type": "context_event",
                        "event_id_hash": ref_hash,
                        "event_type": embedding_record.get("event_type", ""),
                        "classification": embedding_record.get("classification", ""),
                        "status": embedding_record.get("status", ""),
                        "source_type": embedding_record.get("source_type") or embedding_record.get("source_kind") or "message",
                    }
                elif embedding_type in {"entity_state", "profile_entity_state"} and embedding_record.get("ref_type") in {"entity", None, ""}:
                    synthetic_record = {
                        **embedding_record,
                        "record_type": "context_entity",
                        "entity_hash": ref_hash,
                        "entity_type": embedding_record.get("entity_type", ""),
                        "entity_name": embedding_record.get("entity_name", ""),
                    }
                elif embedding_type == "segment_text" and embedding_record.get("ref_type") in {"segment", None, ""}:
                    synthetic_record = {
                        **embedding_record,
                        "record_type": "context_segment",
                        "segment_hash": ref_hash,
                        "topic": embedding_record.get("topic", ""),
                    }
                elif embedding_type == "compression_summary" and embedding_record.get("ref_type") in {"compression", None, ""}:
                    synthetic_record = {
                        **embedding_record,
                        "record_type": "context_compression_event",
                        "compression_id_hash": ref_hash,
                        "operator": embedding_record.get("operator") or "TIME_COMPRESS",
                    }
                elif embedding_record.get("ref_type") == "summary" and str(embedding_type or "") in {
                    "node_l0",
                    "node_l1",
                    "batch_l0",
                    "session_l0",
                    "session_final",
                    "resource_l0",
                    "skill_l0",
                }:
                    synthetic_record = {
                        **embedding_record,
                        "record_type": "context_summary",
                        "summary_hash": ref_hash,
                        "summary_type": embedding_record.get("summary_type") or embedding_type,
                    }
                if synthetic_record is None:
                    continue
                synthetic_terms = candidate_index_terms(synthetic_record, {}, {})
                if not synthetic_terms.intersection(required_index_terms):
                    continue
                secondary_embedding_matched_count += 1
                secondary_posting_ref_hashes.add(str(ref_hash))
                node_hash = embedding_record.get("node_hash")
                if node_hash is not None:
                    secondary_posting_node_hashes.add(str(node_hash))

            # Since the fold-and-drop, a NEW log has no separate embedding records: the owner
            # itself carries the vector (and the ride-along embedding_meta). The owner is the
            # real record, so it is matched directly -- no synthetic reconstruction needed.
            _owner_ref_fields = {
                "context_event": "event_id_hash",
                "context_entity": "entity_hash",
                "context_summary": "summary_hash",
                "context_segment": "segment_hash",
                "context_compression_event": "compression_id_hash",
                "resource_chunk": "chunk_hash",
                "skill_section": "section_hash",
                "context_node": "node_hash",
            }
            for owner_record in raw_records:
                ref_field = _owner_ref_fields.get(str(owner_record.get("record_type") or ""))
                if ref_field is None:
                    continue
                if not owner_record.get("vector") and not owner_record.get("embedding_meta"):
                    continue
                ref_hash = owner_record.get(ref_field)
                if ref_hash in (None, ""):
                    continue
                if not recovered_scope_matches(owner_record, scope) and not profile_bridge_scope_matches(owner_record, scope):
                    continue
                owner_terms = candidate_index_terms(owner_record, {}, {})
                if not owner_terms.intersection(required_index_terms):
                    continue
                secondary_embedding_matched_count += 1
                secondary_posting_ref_hashes.add(str(ref_hash))
                node_hash = owner_record.get("node_hash")
                if node_hash is not None:
                    secondary_posting_node_hashes.add(str(node_hash))

        secondary_prefilter_enabled = bool(
            required_index_terms and (secondary_matched_index_count > 0 or secondary_embedding_matched_count > 0)
        )

        def record_matches_secondary_postings(record: Json) -> bool:
            if not secondary_prefilter_enabled:
                return True
            if profile_bridge_scope_matches(record, scope):
                return True
            if str(record.get("record_type") or "") == "context_index":
                return str(record.get("index_name") or "") in required_index_terms
            for field in ("node_hash", "parent_segment_hash"):
                value = record.get(field)
                if value is not None and str(value) in secondary_posting_node_hashes:
                    return True
            for field in (
                "ref_hash",
                "event_id_hash",
                "entity_hash",
                "summary_hash",
                "segment_hash",
                "compression_id_hash",
                "chunk_hash",
                "section_hash",
                "skill_hash",
            ):
                value = record.get(field)
                if value is not None and str(value) in secondary_posting_ref_hashes:
                    return True
            value = record.get("batch_id_hash")
            if value is not None and str(value) in secondary_posting_batch_hashes:
                return True
            refs = record.get("ref_hashes")
            if isinstance(refs, list) and any(str(ref) in secondary_posting_ref_hashes for ref in refs if ref is not None):
                return True
            return False

        filtered: list[Json] = []
        scanned = 0
        dropped_type = 0
        dropped_scope = 0
        dropped_node = 0
        dropped_secondary_index = 0
        selected_nodes = selected_node_hashes or set()
        for record in raw_records:
            scanned += 1
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
            if not record_matches_secondary_postings(record):
                dropped_secondary_index += 1
                continue
            if not session_scope_allows_record(record, scope):
                dropped_scope += 1
                continue
            if (
                record_type in {
                    "context_embedding",
                    "context_index",
                    "context_segment",
                    "context_summary",
                    "context_summary_dirty",
                    "resource_manifest",
                    "skill_registry_update",
                }
                or (
                    record_type == "context_event"
                    and (
                        str(record.get("event_type") or record.get("classification") or "").lower() == "pending_async"
                        or str(record.get("classification") or "").strip().upper() == "PENDING_ASYNC_EXTRACTION"
                        or str(record.get("extraction_phase") or "").strip().lower() == "pending_async"
                        or str(record.get("extraction_status") or "").strip().lower() in {"pending", "async_pending"}
                        or str(record.get("extraction_mode") or "").strip().lower() == "async_pending"
                    )
                )
            ):
                if (
                    not recovered_scope_matches(record, scope)
                    and not profile_summary_path_matches(record, scope)
                    and not profile_bridge_scope_matches(record, scope)
                ):
                    dropped_scope += 1
                    continue
            elif not access_scope_matches_before_scoring(record, scope):
                dropped_scope += 1
                continue
            filtered.append(record)
        result = {
            "records": [project_scan_record(record) for record in filtered],
            "scan_stats": {
                "backend": getattr(self, "_backend_label", lambda: "local")(),
                "execution_mode": "adapter_prefilter_cached",
                "native_pushdown": False,
                "broad_scan_fallback_allowed": True if allow_broad_scan_fallback is None else bool(allow_broad_scan_fallback),
                "broad_scan_used": not secondary_prefilter_enabled,
                "broad_scan_reason": "no_matching_secondary_index_postings" if required_index_terms and not secondary_prefilter_enabled else ("local_secondary_index_prefilter" if secondary_prefilter_enabled else "local_reference_adapter"),
                "record_types": sorted(allowed_types),
                "scanned_records": scanned,
                "returned_records": len(filtered),
                "dropped_by_type": dropped_type,
                "dropped_by_scope": dropped_scope,
                "dropped_by_node": dropped_node,
                "dropped_by_secondary_index": dropped_secondary_index,
                "secondary_index_groups_supplied": len(secondary_index_groups or []),
                "secondary_index_prefilter_enabled": secondary_prefilter_enabled,
                "secondary_index_matched_posting_count": secondary_matched_index_count,
                "secondary_embedding_matched_posting_count": secondary_embedding_matched_count,
                "secondary_index_posting_ref_hash_count": len(secondary_posting_ref_hashes),
                "secondary_index_posting_node_hash_count": len(secondary_posting_node_hashes),
                "secondary_index_posting_batch_hash_count": len(secondary_posting_batch_hashes),
                "index_postings_read": secondary_matched_index_count,
                "index_postings_touched": secondary_matched_index_count,
                "index_posting_ref_hash_count": len(secondary_posting_ref_hashes),
                "index_posting_node_hash_count": len(secondary_posting_node_hashes),
                "index_posting_batch_hash_count": len(secondary_posting_batch_hashes),
                "selected_node_hashes_supplied": len(selected_node_hashes or set()),
            },
        }
        with self._retrieval_records_cache_lock:
            self._retrieval_records_cache[cache_key] = result
        return {"records": _drop_time_expired_records(result["records"]), "scan_stats": result["scan_stats"]}

    def find_latest_entity(self, *, node_hash: int, entity_type: str, entity_name: str) -> Json | None:
        entity_hash = stable_hash(f"{node_hash}:{entity_type}:{entity_name}")
        if entity_hash in self._latest_entity_by_hash:
            return self._latest_entity_by_hash[entity_hash]
        self._ensure_latest_entity_cache_loaded()
        return self._latest_entity_by_hash.get(entity_hash)

    def pending_session_events(self, scope: Json, *, limit: int | None = None) -> list[Json]:
        key = session_buffer_key_from_scope(scope)
        if not hasattr(self, "_session_buffer_cache_lock"):
            self._session_buffer_cache_lock = threading.RLock()
        if not hasattr(self, "_context_event_by_hash"):
            self._context_event_by_hash = {}
        if not hasattr(self, "_session_pending_event_ids_by_key"):
            self._session_pending_event_ids_by_key = {}
        if not hasattr(self, "_session_committed_event_ids_by_key"):
            self._session_committed_event_ids_by_key = {}
        with self._session_buffer_cache_lock:
            if key in self._session_pending_event_ids_by_key:
                pending_ids = list(self._session_pending_event_ids_by_key.get(key, []))
                events = [self._context_event_by_hash[event_hash] for event_hash in pending_ids if event_hash in self._context_event_by_hash]
                return events[:limit] if limit is not None else events
        committed: set[int] = set()
        reader = getattr(self, "records_for_session_buffer", None)
        records = reader(scope) if callable(reader) else self.read_all()
        for record in records:
            if record.get("record_type") == "context_batch_commit" and session_buffer_key_from_scope(record.get("scope", {})) == key:
                for ref in record.get("source_event_ids", []):
                    try:
                        committed.add(int(ref))
                    except (TypeError, ValueError):
                        continue
        pending_ids: list[int] = []
        buffer_event_by_id: dict[int, Json] = {}
        for record in records:
            if record.get("record_type") != "session_buffer_event" or tuple(record.get("buffer_key", [])) != key:
                continue
            try:
                event_hash = int(record.get("event_id_hash"))
            except (TypeError, ValueError):
                continue
            buffer_event_by_id[event_hash] = record
            if event_hash not in committed:
                pending_ids.append(event_hash)
        event_by_id: dict[int, Json] = {}
        fallback_events: list[Json] = []
        for record in records:
            if record.get("record_type") != "context_event":
                continue
            try:
                event_hash = int(record.get("event_id_hash"))
            except (TypeError, ValueError):
                continue
            buffer_record = buffer_event_by_id.get(event_hash, {})
            if isinstance(buffer_record.get("envelope"), dict) or isinstance(buffer_record.get("agent_hook"), dict):
                enriched_record = dict(record)
                if isinstance(buffer_record.get("envelope"), dict) and "envelope" not in enriched_record:
                    enriched_record["envelope"] = buffer_record["envelope"]
                if isinstance(buffer_record.get("agent_hook"), dict) and "agent_hook" not in enriched_record:
                    enriched_record["agent_hook"] = buffer_record["agent_hook"]
                record = enriched_record
            event_by_id[event_hash] = record
            if not pending_ids and session_buffer_key(record.get("envelope", {})) == key and event_hash not in committed:
                fallback_events.append(record)
        events = [event_by_id[event_hash] for event_hash in pending_ids if event_hash in event_by_id]
        if not events:
            events = fallback_events
        with self._session_buffer_cache_lock:
            self._context_event_by_hash.update(event_by_id)
            self._session_committed_event_ids_by_key[key] = set(committed)
            cached_pending_ids: list[int] = []
            for record in events:
                try:
                    cached_pending_ids.append(int(record.get("event_id_hash")))
                except (TypeError, ValueError):
                    continue
            self._session_pending_event_ids_by_key[key] = cached_pending_ids
        if limit is not None:
            return events[:limit]
        return events

    def append_session_buffer_event(self, *, envelope: Json, event_id_hash: int, node_hash: int, node_path: list[str], hook: Json | None) -> None:
        key = session_buffer_key(envelope)
        source_lineage = source_event_lineage_summary([
            {
                "envelope": envelope,
                "agent_hook": hook,
            }
        ])
        context_lineage = context_source_lineage(envelope, hook)
        for lineage_key in [
            "source_memory_selection_policies",
            "source_memory_selection_policy_counts",
            "source_memory_selection_lossy_count",
            "source_memory_selection_complete_count",
            "source_memory_selection_dropped_text_chars",
            "source_memory_selection_dropped_line_count",
            "source_memory_selection_retained_text_ratio_avg",
            "source_memory_selection_retained_line_ratio_avg",
        ]:
            value = context_lineage.get(lineage_key)
            if value not in (None, "", [], {}):
                source_lineage[lineage_key] = value
        self.append(
            {
                "record_type": "session_buffer_event",
                "buffer_key_hash": stable_hash(":".join(key)),
                "buffer_key": list(key),
                "event_id_hash": event_id_hash,
                "node_hash": node_hash,
                "storage_options": envelope.get("storage_options", {}),
                "storage_route": envelope.get("storage_route", {}),
                "node_path": node_path,
                "scope": envelope["scope"],
                "status": "pending",
                # A resource/skill document is already in its chunk records and behind its
                # raw URI; embedding the full envelope here kept a third copy (1.06x source
                # on a 66.2 KB file). Only message CONTENT is bounded, and on a COPY --
                # the live envelope still feeds chunk parsing, and resource_text derives
                # from that same list, so trimming it in place would truncate the document.
                # Roles, metadata, hook_type and codex_event survive untouched: the commit
                # path reads those off the buffered envelope.
                "envelope": bounded_buffer_envelope(envelope),
                "agent_hook": hook,
                **source_lineage,
                "created_at_ms": envelope["ingestion_time_ms"],
            }
        )

    def _idle_commit_candidate_records(self, scope: Json) -> list[Json]:
        """The records the idle-commit drain needs: pipeline tasks, nothing else.

        Reading the whole log is fine on the JSONL backend, where `read_all()` walks an in-memory
        list. The native adapter overrides this with a typed scan, because there `read_all()` ships
        the entire record log over the proxy -- once per ingest -- to look at one record type.
        """
        return self.read_all()

    def drain_due_idle_session_commits(self, *, scope: Json, args: Json, hook: Json | None) -> Json:
        now = now_ms()
        # This runs on EVERY ingest and its only question is whether a scheduled idle deadline has
        # passed. Answering it costs a typed scan: measured on a 200-memory store, the scan the
        # drain issues was 53.7 ms of a 270 ms add, one of three scans that together were 61% of
        # the whole call. Re-asking it a few milliseconds after the last "no" cannot produce a
        # different answer -- an idle timeout is measured in seconds.
        #
        # So after a pass that finds nothing due, this session's key is quiet until the interval
        # elapses. A deadline that falls inside that window fires up to one interval late, against
        # an idle timeout orders of magnitude longer. A pass that DOES drain something sets no
        # gate, so a busy session keeps being checked every time.
        drain_key = session_buffer_key_from_scope(scope)
        gate = getattr(self, "_idle_drain_next_ms", None)
        if gate is None:
            gate = {}
            self._idle_drain_next_ms = gate
        if now < int(gate.get(drain_key, 0)):
            return {"status": "idle", "due_task_count": 0, "drained_task_count": 0, "drained": [],
                    "idle_drain_gated": True}
        records = self._idle_commit_candidate_records(scope)
        latest_status_by_task_hash: dict[int, str] = {}
        latest_order_by_task_hash: dict[int, int] = {}
        for index, record in enumerate(records):
            if record.get("record_type") != "matrixark_async_pipeline_task":
                continue
            try:
                task_hash = int(record.get("task_hash"))
            except (TypeError, ValueError):
                continue
            latest_status_by_task_hash[task_hash] = str(record.get("status") or "")
            latest_order_by_task_hash[task_hash] = index
        due_tasks: list[Json] = []
        scheduled_here = 0
        requested_key = session_buffer_key_from_scope(scope)
        for index, record in enumerate(records):
            if record.get("record_type") != "matrixark_async_pipeline_task":
                continue
            if record.get("status") != "idle_commit_scheduled":
                continue
            try:
                task_hash = int(record.get("task_hash"))
                deadline_ms = int(record.get("idle_commit_deadline_ms") or 0)
            except (TypeError, ValueError):
                continue
            if latest_order_by_task_hash.get(task_hash) != index:
                continue
            if latest_status_by_task_hash.get(task_hash) != "idle_commit_scheduled":
                continue
            task_scope = record.get("scope", {}) if isinstance(record.get("scope"), dict) else {}
            if session_buffer_key_from_scope(task_scope) != requested_key:
                continue
            scheduled_here += 1
            if deadline_ms > now:
                continue
            due_tasks.append(record)
        due_tasks.sort(
            key=lambda item: (
                int(item.get("idle_commit_deadline_ms") or 0),
                int(item.get("idle_commit_cutoff_ms") or 0),
                int(item.get("event_id_hash") or 0),
            )
        )
        drained: list[Json] = []
        for task in due_tasks[:8]:
            task_scope = task.get("scope", {}) if isinstance(task.get("scope"), dict) else scope
            task_storage_options = (
                task.get("requested_storage_options")
                if isinstance(task.get("requested_storage_options"), dict)
                else args.get("storage_options", {})
            )
            task_storage_options = dict(task_storage_options) if isinstance(task_storage_options, dict) else {}
            route_value = str(task_storage_options.get("route") or "").strip().lower().replace("-", "_")
            if route_value and route_value not in STORAGE_ROUTE_PRESETS:
                task_storage_options = {}
            result = self.session_commit(
                {
                    "scope": task_scope,
                    "metadata": args.get("metadata", {}),
                    "threshold_messages": task.get("threshold_messages", args.get("session_buffer_threshold", 20)),
                    "force": False,
                    "commit_before_ms": int(task.get("idle_commit_cutoff_ms") or 0),
                    "idle_timeout_ms": int(task.get("idle_commit_timeout_ms") or 0),
                    "commit_reason": "idle_timeout",
                    "understanding_provider": args.get("understanding_provider"),
                    "extraction_provider": args.get("extraction_provider"),
                    "segment_provider": args.get("segment_provider"),
                    "segment_model": args.get("segment_model"),
                    "segment_model_path": args.get("segment_model_path"),
                    "segment_max_new_tokens": args.get("segment_max_new_tokens"),
                    "segment_provider_fallback": args.get("segment_provider_fallback"),
                    "skip_prior_context": bool(args.get("skip_prior_context", False)),
                    "storage_options": task_storage_options,
                },
                hook=hook,
            )
            try:
                task_hash = int(task.get("task_hash"))
            except (TypeError, ValueError):
                task_hash = stable_hash(f"async_pipeline_idle_commit:{task.get('event_id_hash')}")
            status = "idle_commit_committed" if result.get("status") in {"accepted", "committed"} else "idle_commit_skipped"
            completion = {
                "record_type": "matrixark_async_pipeline_task",
                "task_hash": task_hash,
                "event_id_hash": task.get("event_id_hash"),
                "node_hash": task.get("node_hash"),
                "node_path": task.get("node_path", []),
                "scope": task_scope,
                "status": status,
                "stages": task.get("stages", ["extraction", "summary", "compression", "embedding"]),
                "completed_stages": ["extraction"] if status == "idle_commit_committed" else [],
                "remaining_stages": ["summary", "compression", "embedding"] if status == "idle_commit_committed" else task.get("stages", []),
                "reason": "session_buffer_idle_deadline_drained",
                "trigger_policy": "idle_timeout",
                "commit_result_status": result.get("status"),
                "commit_id_hash": result.get("commit_id_hash"),
                "batch_id_hash": result.get("batch_id_hash"),
                "idle_commit_deadline_ms": task.get("idle_commit_deadline_ms"),
                "idle_commit_cutoff_ms": task.get("idle_commit_cutoff_ms"),
                "updated_at_ms": now,
            }
            self.append(completion)
            drained.append(
                {
                    "task_hash": task_hash,
                    "event_id_hash": task.get("event_id_hash"),
                    "status": status,
                    "commit_result_status": result.get("status"),
                    "commit_id_hash": result.get("commit_id_hash"),
                    "batch_id_hash": result.get("batch_id_hash"),
                }
            )
        if scheduled_here == 0:
            # Nothing is even SCHEDULED for this session, so nothing can come due until something
            # schedules one -- and scheduling happens on this same path. Going quiet is free.
            #
            # Gating on "nothing DUE" instead was measurably worse where it counts: p50 fell but
            # p95 went 367 -> 820 ms, because a session with a pending deadline stopped being
            # checked, the due work piled up, and the add that finally drained paid for several
            # session commits at once. A pending task keeps being checked every time.
            gate[drain_key] = now + _idle_drain_min_interval_ms()
        else:
            gate.pop(drain_key, None)
        return {
            "status": "drained" if drained else "idle",
            "due_task_count": len(due_tasks),
            "drained_task_count": len(drained),
            "drained": drained,
        }

