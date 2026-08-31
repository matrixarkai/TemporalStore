"""_LocalAdapterIngestMixin methods split from matrixark_mcp_local_adapter.MatrixArkLocalAdapter (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

import os as _os
import re as _re
import warnings as _warnings

_PROFILE_SCOPE_WARNED: set = set()


def _segment_access_scope_enabled() -> bool:
    """Gate: write context_segment records WITH a tenant/user/session access_scope so a
    scored segment passes access_scope_matches_before_scoring instead of being dropped
    scope-less. Default ON; set MATRIXARK_SEGMENT_ACCESS_SCOPE=0 to restore prior behavior."""
    return str(_os.environ.get("MATRIXARK_SEGMENT_ACCESS_SCOPE", "1")).strip().lower() not in {"0", "false", "no", "off"}


def _value_entity_capture_enabled() -> bool:
    """Gate: mint a scoped, embedded fact entity for any assistant message stating an exact
    value (number+unit, count, hex hash, ALL_CAPS/dotted flag) that the topic segmenter and
    entity extractor skipped, with its state text NOT truncated before the value token.
    Default ON; set MATRIXARK_VALUE_ENTITY_CAPTURE=0 to restore prior behavior."""
    return str(_os.environ.get("MATRIXARK_VALUE_ENTITY_CAPTURE", "1")).strip().lower() not in {"0", "false", "no", "off"}


_VALUE_TOKEN_RE = _re.compile(
    r"(?:\b0x[0-9a-fA-F]{6,}\b)"
    r"|(?:\b[0-9a-f]{7,40}\b)"
    r"|(?:\b\d[\d,]*\.?\d*\s?"
    r"(?:%|ms|s|mb|gb|kb|tb|qps|tokens?|entries|records|shards|datanodes|microseconds|milliseconds|dimensions?|dim)\b)"
    r"|(?:\b[A-Z][A-Z0-9]{2,}(?:_[A-Z0-9]+)+\b)"
    r"|(?:\b\d[\d.,]*\d\b)",
    _re.IGNORECASE,
)


def _line_has_exact_value(text: str) -> bool:
    return bool(text) and bool(_VALUE_TOKEN_RE.search(text))


_VALUE_NAME_STOP = {
    "the", "a", "an", "is", "are", "was", "were", "to", "of", "and", "or", "for", "with",
    "on", "in", "at", "by", "now", "set", "we", "our", "it", "its", "that", "this", "yes",
    "no", "assistant", "user", "per", "about", "which", "while", "before", "after", "so",
}


def _value_entity_name(text: str) -> str:
    """Derive a short, stable entity name from the salient words of a value-bearing line."""
    words = _re.findall(r"[A-Za-z][A-Za-z0-9_.-]{2,}", text or "")
    salient = [w for w in words if w.lower() not in _VALUE_NAME_STOP][:6]
    return ("value_fact:" + "_".join(salient)).lower()[:96] if salient else "value_fact"


def warn_if_profile_scope_missing(scope) -> str:
    """Warn (once per identity) when a message-ingest scope lacks tenant_id/user_id.

    Profile promotion writes to `tenant:<id>/user:<id>/profile:long_term_memory`, so without
    both, extraction silently no-ops with blocker `profile_scope_missing` and long-term profile
    memory never populates. The Codex/Claude hooks always pass these; this guards direct
    HTTP/MCP/custom callers. Returns the warning string (also surfaced in the ingest result).
    """
    if not isinstance(scope, dict):
        return ""
    tid = str(scope.get("tenant_id") or "").strip()
    uid = str(scope.get("user_id") or "").strip()
    if tid and uid:
        return ""
    missing = " + ".join(k for k, v in (("tenant_id", tid), ("user_id", uid)) if not v)
    msg = (f"profile_scope_missing: ingest scope lacks {missing}; profile long-term memory will "
           "NOT be populated (entities will not promote to durable profile). Provide tenant_id + "
           "user_id in scope (the Codex/Claude hooks set these automatically).")
    dedup_key = (tid, uid)
    if dedup_key not in _PROFILE_SCOPE_WARNED:
        _PROFILE_SCOPE_WARNED.add(dedup_key)
        _warnings.warn(msg, stacklevel=2)
    return msg


# Ingest fields that come from the CALLER rather than from extraction -- the exact set
# MatrixArkLocalAdapter._stamp_ingest_fields puts on a `context_event`. Extraction cannot rebuild
# them, so a commit that rewrites an existing event row has to carry them over (see the use below).
# An explicit allowlist on purpose: the rewrite is meant to replace the extraction's own view of the
# event, and inheriting anything broader would let a stale earlier row overrule a fresh
# classification.
CALLER_SUPPLIED_EVENT_FIELDS = (
    "identity_key",
    "truth_class",
    "truth_rank",
    "expires_at",
    "expires_at_ms",
    "ephemeral",
)

try:  # names owned by the parent module
    from tools.matrixark_mcp_local_adapter import (
    assistant_profile_fact_lineage_text,
    async_pipeline_retrieval_readiness,
    auto_batch_extract_enabled,
    compact_context_embedding_record,
    compact_context_pack_for_serving,
    compression_context_index_records,
    context_event_type_for_message,
    context_source_lineage,
    deferred_idle_auto_batch_result,
    idle_commit_scheduled_task_record,
    memory_selection_policy_counts_for_message,
    memory_selection_retention_for_message,
    profile_promotion_decision,
    selected_ref_layer_budget,
    session_boundary_commit_requested,
    session_event_message_count,
    should_promote_session_entity_to_profile,
    source_event_lineage_summary,
    suppress_extracted_represented_pending_events,
    user_profile_fact_lineage_text,
)
except ImportError:
    from matrixark_mcp_local_adapter import (
    assistant_profile_fact_lineage_text,
    async_pipeline_retrieval_readiness,
    auto_batch_extract_enabled,
    compact_context_embedding_record,
    compact_context_pack_for_serving,
    compression_context_index_records,
    context_event_type_for_message,
    context_source_lineage,
    deferred_idle_auto_batch_result,
    idle_commit_scheduled_task_record,
    memory_selection_policy_counts_for_message,
    memory_selection_retention_for_message,
    profile_promotion_decision,
    selected_ref_layer_budget,
    session_boundary_commit_requested,
    session_event_message_count,
    should_promote_session_entity_to_profile,
    source_event_lineage_summary,
    suppress_extracted_represented_pending_events,
    user_profile_fact_lineage_text,
)


# How many provenance entries a profile entity keeps inline. The rest live in overflow records.
_PROFILE_PROVENANCE_INLINE = 16


def _profile_provenance_overflow(previous_profile, *, refs_all, events_all):
    """The provenance entries that just aged out of the inline window, or None.

    One small append per promotion instead of re-writing the whole history: what this returns is
    the DELTA between what the previous version already carried and what the new window keeps, so
    nothing is lost and nothing is written twice.
    """
    previous_refs = list((previous_profile or {}).get("source_refs", []) or [])
    previous_events = list((previous_profile or {}).get("source_event_ids", []) or [])
    kept_refs = set(str(value) for value in refs_all[-_PROFILE_PROVENANCE_INLINE:])
    kept_events = set(str(value) for value in events_all[-_PROFILE_PROVENANCE_INLINE:])
    already = set(str(value) for value in previous_refs) | set(str(value) for value in previous_events)
    aged_refs = [value for value in refs_all
                 if str(value) not in kept_refs and str(value) not in already]
    aged_events = [value for value in events_all
                   if str(value) not in kept_events and str(value) not in already]
    if not aged_refs and not aged_events:
        return None
    return {"source_refs": aged_refs, "source_event_ids": aged_events}


class _LocalAdapterIngestMixin:
    _MEMORY_UPSERT_ARG_KEYS = ("expires_at", "ttl_seconds", "retention_cutoff_ts", "identity_key", "truth_class")

    def ingest(self, args: Json, *, hook: Json | None = None) -> Json:
        """Public ingest entry. Fast-path is byte-identical to the core ingest; when any
        PurchaseMemory field (expires_at / ttl_seconds / retention_cutoff_ts / identity_key /
        truth_class) is present it layers per-record TTL stamping, a keyed-upsert truth-rank guard,
        and a scope-level retention-cutoff marker on top of the unchanged core ingest."""
        if not any(key in args for key in self._MEMORY_UPSERT_ARG_KEYS):
            return self._ingest_impl(args, hook=hook)
        envelope = normalize_envelope(args, default_kind="message")
        # Pin ingestion_time_ms so the core re-normalization inside _ingest_impl is deterministic
        # (event_id_hash derives from it); the caller's other fields already round-trip through args.
        args = {**args, "ingestion_time_ms": envelope["ingestion_time_ms"]}
        identity_key = str(envelope.get("identity_key") or "")
        self._push_ingest_stamp(envelope)
        try:
            result = self._ingest_impl(args, hook=hook)
        finally:
            self._pop_ingest_stamp()
        if isinstance(result, dict):
            if identity_key:
                result = self._apply_identity_upsert(result, identity_key=identity_key, envelope=envelope)
            if envelope.get("retention_cutoff_ms") is not None:
                self._write_retention_cutoff(result, envelope)
            if envelope.get("ephemeral"):
                # Reclaim space lazily; a no-op unless MATRIXARK_MEMORY_PURGE_THRESHOLD is set.
                self._maybe_sweep_expired()
        return result

    def _ingest_impl(self, args: Json, *, hook: Json | None = None) -> Json:
        envelope = normalize_envelope(args, default_kind="message")
        # Guard: a message-ingest scope without tenant_id/user_id silently disables profile
        # long-term memory. Warn (once per identity) so profile_scope_missing is never silent.
        self._profile_scope_warning = (
            warn_if_profile_scope_missing(envelope["scope"]) if envelope["kind"] == "message" else ""
        )
        hook = validate_hook(hook)
        source_lineage = context_source_lineage(envelope, hook)
        backend_readiness: Json | None = None
        if envelope["kind"] in {"resource", "skill"}:
            backend_readiness = self.ensure_backend_ready(reason=f"{envelope['kind']}_ingest")
        idle_commit_result: Json | None = None
        idle_commit_timeout_ms = args.get("idle_commit_timeout_ms")
        if idle_commit_timeout_ms is not None:
            if not isinstance(idle_commit_timeout_ms, int) or idle_commit_timeout_ms < 0:
                raise MatrixArkError("idle_commit_timeout_ms must be a non-negative integer")
            idle_commit_result = self.session_commit(
                {
                    "scope": envelope["scope"],
                    "metadata": envelope["metadata"],
                    "threshold_messages": args.get("session_buffer_threshold", 20),
                    "force": False,
                    "idle_timeout_ms": idle_commit_timeout_ms,
                    "commit_reason": "idle_timeout",
                    "skip_prior_context": bool(args.get("skip_prior_context", False)),
                    "storage_options": envelope.get("storage_options", {}),
                },
                hook=hook,
            )
        lightweight_async_accept = envelope["kind"] in {"message", "business_data", "feedback"} and (
            bool(args.get("async_processing", False)) or args.get("wait") is False
        )
        if lightweight_async_accept:
            due_idle_commit_result: Json | None = None
            if auto_batch_extract_enabled(args, kind=envelope["kind"]):
                due_idle_commit_result = self.drain_due_idle_session_commits(
                    scope=envelope["scope"],
                    args=args,
                    hook=hook,
                )
            text = text_from_messages(envelope["messages"])
            event_id_hash = stable_hash(
                f"{envelope['kind']}:{text}:{envelope['scope']}:{envelope['ingestion_time_ms']}"
            )
            node_hint = envelope["metadata"].get("node_path") or self.default_session_node_path(envelope["scope"])
            node_path = normalized_node_path(envelope, node_hint)
            node_hash = stable_hash("/".join(node_path))
            node_materialization = self.ensure_context_node_path(
                node_path=node_path,
                scope=envelope["scope"],
                updated_at_ms=envelope["ingestion_time_ms"],
            )
            source_memory_scopes, source_session_continuities = pending_extraction_memory_layer_intent(envelope["scope"])
            pending_event_type = "pending_async"
            if len(envelope["messages"]) == 1:
                pending_event_type = context_event_type_for_message(envelope["messages"][0], pending_event_type)
            pending_profile_memory_fields: Json = {}
            if (
                pending_event_type == "memory_feature"
                or profile_entity_type_for_memory_text(text) == "memory_feature_profile"
            ):
                pending_profile_memory_fields = {
                    "profile_memory_class": "memory_feature",
                    "profile_memory_kind": "memory_feature",
                    "source_profile_memory_classes": ["memory_feature"],
                    "source_profile_memory_kinds": ["memory_feature"],
                }
            pending_lineage = context_source_lineage(envelope, hook)
            pending_event_vector = embedding_for_text(text)
            with self.write_batch("message_ingest_sync_accept"):
                summary_dirty_hashes = self.mark_node_summary_dirty(
                    node_path=node_path,
                    scope=envelope["scope"],
                    updated_at_ms=envelope["ingestion_time_ms"],
                    source_ref_type="event",
                    source_hash_field="source_event_hash",
                    source_hash=event_id_hash,
                    dirty_reason="new_event",
                    source_lineage=source_lineage,
                )
                pending_event_record = {
                    "record_type": "context_event",
                    "event_id_hash": event_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "text": text,
                    "summary_text": summarize_text(text),
                    "classification": "PENDING_ASYNC_EXTRACTION",
                    "event_type": pending_event_type,
                    "batch_event_type": "pending_async",
                    "status": "pending",
                    "source_kind": envelope.get("kind", "message"),
                    "envelope": envelope,
                    "internal_extraction": {
                        "mode": "async_pending",
                        "classification": "PENDING_ASYNC_EXTRACTION",
                        "event_type": pending_event_type,
                        "batch_event_type": "pending_async",
                        "status": "pending",
                    },
                    "agent_hook": hook,
                    **source_lineage,
                    **pending_lineage,
                    "storage_options": envelope.get("storage_options", {}),
                    "async_processing": True,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "source_memory_scopes": source_memory_scopes,
                    "source_session_continuities": source_session_continuities,
                    **pending_profile_memory_fields,
                    "extraction_phase": "pending_async",
                    "final_session_boundary": False,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
                pending_memory_layer = candidate_memory_layer_name(pending_event_record)
                if pending_memory_layer:
                    pending_event_record["memory_layer"] = pending_memory_layer
                self._begin_append_coalescing()
                self.append(pending_event_record)
                pending_embedding_record = compact_context_embedding_record(
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "event_text",
                        "ref_type": "event",
                        "ref_hash": event_id_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dim": len(pending_event_vector),
                        "model": embedding_model_name(),
                        "vector": pending_event_vector,
                        "scope": envelope["scope"],
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "memory_layer": pending_memory_layer,
                        "event_type": pending_event_type,
                        "batch_event_type": "pending_async",
                        "classification": "PENDING_ASYNC_EXTRACTION",
                        "status": "pending",
                        "source_kind": envelope.get("kind", "message"),
                        **pending_lineage,
                        "source_memory_scopes": source_memory_scopes,
                        "source_session_continuities": source_session_continuities,
                        "source_extraction_phases": ["pending_async"],
                        **pending_profile_memory_fields,
                        "extraction_phase": "pending_async",
                        "final_session_boundary": False,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                pending_embedding_record["access_scope"] = envelope["scope"]
                self.append(pending_embedding_record)
                for index_name in candidate_index_terms(pending_event_record, {}, {}):
                    event_index = context_index_posting_record(
                        index_name=index_name,
                        data_model="context_event",
                        ref_type="event",
                        ref_hashes=[event_id_hash],
                        batch_id_hash=event_id_hash,
                        node_hash=node_hash,
                        scope=envelope["scope"],
                        updated_at_ms=envelope["ingestion_time_ms"],
                    )
                    event_index["access_scope"] = envelope["scope"]
                    event_index.pop("index_hash", None)
                    self.append(event_index)
                self.append_session_buffer_event(envelope=envelope, event_id_hash=event_id_hash, node_hash=node_hash, node_path=node_path, hook=hook)
                self.append(
                    {
                        "record_type": "matrixark_async_pipeline_task",
                        "task_hash": stable_hash(f"async_pipeline:{event_id_hash}"),
                        "event_id_hash": event_id_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": envelope["scope"],
                        "status": "pending",
                        "stages": ["extraction", "summary", "compression", "embedding"],
                        "reason": "sync_accept_async_processing",
                        **source_event_lineage_summary([source_lineage]),
                        "source_memory_scopes": source_memory_scopes,
                        "source_session_continuities": source_session_continuities,
                        **({"memory_layer": pending_memory_layer} if pending_memory_layer else {}),
                        **pending_profile_memory_fields,
                        "created_at_ms": envelope["ingestion_time_ms"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            self._flush_append_coalescing()
            pending_events = self.pending_session_events(envelope["scope"])
            pending_event_count = len(pending_events)
            pending_message_count = session_event_message_count(pending_events)
            auto_batch_result: Json | None = None
            auto_batch_extract = auto_batch_extract_enabled(args, kind=envelope["kind"])
            session_boundary_commit = session_boundary_commit_requested(args, hook=hook)
            session_buffer_threshold = args.get("session_buffer_threshold", 20)
            if not isinstance(session_buffer_threshold, int) or session_buffer_threshold <= 0:
                raise MatrixArkError("session_buffer_threshold must be a positive integer")
            threshold_ready = pending_event_count >= session_buffer_threshold or pending_message_count >= session_buffer_threshold
            immediate_idle_ready = bool(
                auto_batch_extract
                and not session_boundary_commit
                and not threshold_ready
                and idle_commit_timeout_ms == 0
                and pending_event_count > 0
            )
            idle_ready = bool(
                isinstance(idle_commit_result, dict)
                and idle_commit_result.get("status") in {"accepted", "committed"}
                and idle_commit_result.get("trigger_policy") == "idle_timeout"
            )
            if auto_batch_extract and (session_boundary_commit or threshold_ready):
                auto_batch_result = self.session_commit(
                    {
                        "scope": envelope["scope"],
                        "metadata": envelope["metadata"],
                        "threshold_messages": session_buffer_threshold,
                        "force": session_boundary_commit,
                        "max_messages": None if session_boundary_commit else session_buffer_threshold,
                        "commit_reason": "hook_boundary" if session_boundary_commit else "threshold",
                        "understanding_provider": args.get("understanding_provider"),
                        "extraction_provider": args.get("extraction_provider"),
                        "segment_provider": args.get("segment_provider"),
                        "segment_model": args.get("segment_model"),
                        "segment_model_path": args.get("segment_model_path"),
                        "segment_max_new_tokens": args.get("segment_max_new_tokens"),
                        "segment_provider_fallback": args.get("segment_provider_fallback"),
                        "skip_prior_context": bool(args.get("skip_prior_context", False)),
                        "storage_options": envelope.get("storage_options", {}),
                    },
                    hook=hook,
                )
            elif immediate_idle_ready:
                auto_batch_result = self.session_commit(
                    {
                        "scope": envelope["scope"],
                        "metadata": envelope["metadata"],
                        "threshold_messages": session_buffer_threshold,
                        "force": False,
                        "commit_before_ms": int(envelope["ingestion_time_ms"]),
                        "idle_timeout_ms": 0,
                        "commit_reason": "idle_timeout",
                        "understanding_provider": args.get("understanding_provider"),
                        "extraction_provider": args.get("extraction_provider"),
                        "segment_provider": args.get("segment_provider"),
                        "segment_model": args.get("segment_model"),
                        "segment_model_path": args.get("segment_model_path"),
                        "segment_max_new_tokens": args.get("segment_max_new_tokens"),
                        "segment_provider_fallback": args.get("segment_provider_fallback"),
                        "skip_prior_context": bool(args.get("skip_prior_context", False)),
                        "storage_options": envelope.get("storage_options", {}),
                    },
                    hook=hook,
                )
            elif auto_batch_extract and idle_ready and isinstance(idle_commit_result, dict):
                auto_batch_result = idle_commit_result
            elif auto_batch_extract:
                auto_batch_result = deferred_idle_auto_batch_result(
                    idle_commit_result=idle_commit_result,
                    pending_event_count=pending_event_count,
                    pending_message_count=pending_message_count,
                    threshold_messages=session_buffer_threshold,
                    idle_commit_timeout_ms=idle_commit_timeout_ms,
                )
            idle_commit_scheduled = bool(
                isinstance(auto_batch_result, dict)
                and auto_batch_result.get("status") == "deferred"
                and auto_batch_result.get("trigger_policy") == "idle_timeout"
                and auto_batch_result.get("idle_commit_scheduled")
            )
            if idle_commit_scheduled and idle_commit_timeout_ms is not None:
                self.append(
                    idle_commit_scheduled_task_record(
                        event_id_hash=event_id_hash,
                        node_hash=node_hash,
                        node_path=node_path,
                        scope=envelope["scope"],
                        storage_options=args.get("storage_options", {}) if isinstance(args.get("storage_options"), dict) else {},
                        ingestion_time_ms=int(envelope["ingestion_time_ms"]),
                        idle_commit_timeout_ms=int(idle_commit_timeout_ms),
                        pending_event_count=pending_event_count,
                        pending_message_count=pending_message_count,
                        threshold_messages=session_buffer_threshold,
                    )
                )
            return {
                "status": "accepted",
                "sync_write_mode": "lightweight_event",
                "async_processing": True,
                "async_pipeline_status": "pending",
                "event_id_hash": event_id_hash,
                "node_hash": node_hash,
                "storage_options": envelope.get("storage_options", {}),
                "storage_route": envelope.get("storage_route", {}),
                "hook_captured": hook is not None,
                "extraction_mode": "async_pending",
                "summary_refresh": {
                    "status": "dirty_marked",
                    "dirty_hashes": summary_dirty_hashes,
                    "async_required": True,
                },
                "node_materialization": node_materialization,
                "session_buffer": {
                    "buffer_key": list(session_buffer_key(envelope)),
                    "pending_event_count": pending_event_count,
                    "pending_message_count": pending_message_count,
                    "threshold_messages": session_buffer_threshold,
                    "threshold_ready": threshold_ready,
                    "session_boundary_commit": session_boundary_commit,
                    "idle_ready": bool(idle_ready or immediate_idle_ready),
                    "idle_commit_scheduled": idle_commit_scheduled,
                    "auto_batch_extract": auto_batch_extract,
                },
                "idle_commit_result": idle_commit_result,
                "due_idle_commit_result": due_idle_commit_result,
                "auto_batch_extract_result": auto_batch_result,
                "quality_warnings": ["async_processing_pending:extraction,summary,compression,embedding"],
            }
        prior_records = [] if args.get("skip_prior_context") else self.prior_context_records(envelope["scope"])
        prior_context = (
            {"level": "", "refs": [], "messages": [], "summaries": [], "char_count": 0, "limit": MAX_PRIOR_MESSAGES}
            if args.get("skip_prior_context")
            else collect_prior_context(envelope, prior_records)
        )
        extraction_started_perf = time.perf_counter()
        extraction = compact_internal_extraction(
            envelope,
            prior_context=prior_context,
        )
        self._observe_model_latency("extraction", (time.perf_counter() - extraction_started_perf) * 1000.0)
        text = text_from_messages(envelope["messages"])
        # A resource/skill document already lives in its chunk records and behind its raw URI, so
        # carrying it a THIRD time as event text is pure duplication -- measured at 1.05x source
        # on a 66.2 KB file. Bound the event text only.
        #
        # It must not be done by clipping envelope["messages"]: resource_text, and therefore every
        # chunk, is derived from that same list, so clipping there would truncate the DOCUMENT
        # instead of deduplicating its storage.
        #
        # This is the assignment the hot-path event actually reads. An identical assignment exists
        # earlier for a different branch, and bounding only that one has no effect here -- this
        # line overwrites it before the event record is built.
        text = bound_resource_event_text(
            envelope["kind"], text, str(envelope.get("metadata", {}).get("raw_uri") or "")
        )
        event_id_hash = stable_hash(
            f"{envelope['kind']}:{text}:{envelope['scope']}:{envelope['ingestion_time_ms']}"
        )
        if envelope["kind"] in {"resource", "skill"}:
            early_deployment_scope = deployment_scope_from_args(args, envelope)
            early_sharing_scope = self.resource_sharing_scope(args, envelope, early_deployment_scope)
            node_hint = self.default_resource_node_path(args, envelope, deployment_scope=early_deployment_scope, sharing_scope=early_sharing_scope)
        else:
            early_deployment_scope = "local"
            early_sharing_scope = "private_user"
            node_hint = envelope["metadata"].get("node_path") or self.default_session_node_path(envelope["scope"])
        node_path = normalized_node_path(envelope, node_hint)
        node_hash = stable_hash("/".join(node_path))
        node_materialization = self.ensure_context_node_path(
            node_path=node_path,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
        )
        resource_chunk_hashes: list[int] = []
        resource_dirty_hashes: list[int] = []
        resource_parse_error = ""
        resource_import_task_hash = 0
        resource_import_task_status = "not_applicable"
        resource_import_wait = True
        held_import_completion: list[Json] = []
        resource_import_metrics: Json = {}
        resource_fact_event_hashes: list[int] = []
        resource_fact_entity_hashes: list[int] = []
        skill_hash = None
        if envelope["kind"] in {"resource", "skill"}:
            requested_raw_uri = str(envelope.get("raw_uri") or envelope["metadata"].get("raw_uri") or "inline-resource")
            resource_type = str(envelope.get("resource_type") or envelope["metadata"].get("resource_type") or "")
            async_default_reason = self._resource_import_async_default_reason(args, envelope, requested_raw_uri)
            resource_import_wait = bool(args.get("wait", not bool(async_default_reason)))
            resource_import_background = bool(args.get("_background_resource_import", False))
            deployment_scope = early_deployment_scope
            sharing_scope = early_sharing_scope
            access_scope = registry_access_scope(envelope["scope"], sharing_scope=sharing_scope)
            resource_record_scope = access_scope if sharing_scope in {"tenant_shared", "global_shared"} else envelope["scope"]
            provided_task_hash = args.get("_resource_import_task_hash")
            resource_import_task_hash = (
                int(provided_task_hash)
                if isinstance(provided_task_hash, int) and provided_task_hash > 0
                else stable_hash(f"resource_import_task:{envelope['kind']}:{requested_raw_uri}:{node_hash}:{envelope['ingestion_time_ms']}")
            )
            import_started_perf = time.perf_counter()
            raw_uri = requested_raw_uri
            raw_storage_policy = "raw_uri_only"
            storage_resolution: Json = {
                "storage_mode": resource_storage_mode_from_args(args, envelope, deployment_scope),
                "original_raw_uri": requested_raw_uri,
                "stored_raw_uri": requested_raw_uri,
                "parse_uri": requested_raw_uri,
                "parse_text": None,
                "raw_storage_policy": raw_storage_policy,
                "raw_bytes_stored": False,
                "upload_status": "not_started",
                "temp_paths": [],
            }
            if not resource_import_background:
                self.append(
                    {
                        "record_type": "resource_import_task",
                        "task_hash": resource_import_task_hash,
                        "status": "queued",
                        "kind": envelope["kind"],
                        "raw_uri": requested_raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "resource_type": resource_type,
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": raw_storage_policy,
                        "raw_bytes_stored": False,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "storage_options": envelope.get("storage_options", {}),
                        "wait": resource_import_wait,
                        "async_default_reason": async_default_reason,
                        "progress": {"stage": "queued", "percent": 0},
                        "created_at_ms": envelope["ingestion_time_ms"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            if not resource_import_wait:
                background_args = {
                    **args,
                    "wait": True,
                    "_background_resource_import": True,
                    "_resource_import_task_hash": resource_import_task_hash,
                }
                try:
                    queue_status = self._enqueue_resource_import(
                        args=background_args,
                        hook=hook,
                        task_hash=resource_import_task_hash,
                    )
                except MatrixArkError as exc:
                    self.append(
                        {
                            "record_type": "resource_import_task",
                            "task_hash": resource_import_task_hash,
                            "status": "failed",
                            "kind": envelope["kind"],
                            "raw_uri": requested_raw_uri,
                            "requested_raw_uri": requested_raw_uri,
                            "resource_type": resource_type,
                            "raw_storage_mode": storage_resolution["storage_mode"],
                            "raw_storage_policy": raw_storage_policy,
                            "raw_bytes_stored": False,
                        "error": str(exc),
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "storage_options": envelope.get("storage_options", {}),
                        "progress": {"stage": "failed", "percent": 100},
                        "updated_at_ms": now_ms(),
                    }
                )
                    raise
                return {
                    "status": "queued",
                    "event_id_hash": event_id_hash,
                    "node_hash": node_hash,
                    "resource_import_task": {
                        "task_hash": resource_import_task_hash,
                        "status": "queued",
                        "wait": False,
                        "background_started": True,
                        "raw_uri": requested_raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "resource_type": resource_type,
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": raw_storage_policy,
                        "raw_bytes_stored": False,
                        "worker_pool": queue_status,
                        "progress": {"stage": "queued", "percent": 0},
                        "async_default_reason": async_default_reason,
                    },
                    "node_materialization": node_materialization,
                }
            resource_import_task_status = "running"
            resource_text = "\n\n".join(str(message["content"]) for message in envelope["messages"])
            try:
                storage_resolution = resolve_raw_resource_for_ingest(
                    # The engine blob tier rides the adapter's rust proxy client when there is
                    # one; the pure-local adapter has none and the key stays absent.
                    {**args, "_engine_blob_client": getattr(self, "_client", None)},
                    envelope,
                    requested_raw_uri,
                    resource_type,
                    deployment_scope,
                    resource_text,
                )
            except MatrixArkError as exc:
                self.append(
                    {
                        "record_type": "resource_import_task",
                        "task_hash": resource_import_task_hash,
                        "status": "failed",
                        "kind": envelope["kind"],
                        "raw_uri": requested_raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "resource_type": resource_type,
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": storage_resolution["raw_storage_policy"],
                        "raw_bytes_stored": False,
                        "error": str(exc),
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "progress": {"stage": "failed", "percent": 100},
                        "updated_at_ms": now_ms(),
                    }
                )
                raise
            raw_uri = str(storage_resolution["stored_raw_uri"])
            parse_uri = str(storage_resolution.get("parse_uri") or raw_uri)
            parse_text = storage_resolution.get("parse_text")
            raw_storage_policy = str(storage_resolution.get("raw_storage_policy") or "raw_uri_only")
            self.append(
                {
                    "record_type": "resource_import_task",
                    "task_hash": resource_import_task_hash,
                    "status": "running",
                    "kind": envelope["kind"],
                    "raw_uri": raw_uri,
                    "requested_raw_uri": requested_raw_uri,
                    "resource_type": resource_type,
                    "raw_storage_mode": storage_resolution["storage_mode"],
                    "raw_storage_policy": raw_storage_policy,
                    "raw_bytes_stored": False,
                    "upload_status": storage_resolution.get("upload_status", "not_required"),
                    "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                    "cloud_key": storage_resolution.get("cloud_key", ""),
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": resource_record_scope,
                    "storage_options": envelope.get("storage_options", {}),
                    "progress": {"stage": "running", "percent": 10},
                    "updated_at_ms": now_ms(),
                }
            )
            try:
                if envelope["kind"] == "skill" or (resource_type or "").lower() == "skill":
                    parsed_skill = parse_skill(
                        parse_uri,
                        text=parse_text,
                        chunk_hash_base=args.get("chunk_hash_base") if isinstance(args.get("chunk_hash_base"), int) else None,
                        max_text_chars=args.get("max_skill_bytes") if isinstance(args.get("max_skill_bytes"), int) else None,
                    )
                    parsed_skill_chunks = rewrite_chunk_uris(parsed_skill.chunks, parse_uri=parse_uri, stored_raw_uri=raw_uri)
                    skill_hash = stable_hash(f"skill:{raw_uri}:{parsed_skill.name}:{parsed_skill.metadata.get('version', '1')}")
                    skill_serving_metadata = serving_resource_metadata(parsed_skill.metadata)
                    self.append(
                        {
                            "record_type": "skill_manifest",
                            "skill_hash": skill_hash,
                            "import_task_hash": resource_import_task_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "raw_uri": raw_uri,
                            "requested_raw_uri": requested_raw_uri,
                            "raw_storage_mode": storage_resolution["storage_mode"],
                            "raw_storage_policy": raw_storage_policy,
                            "upload_status": storage_resolution.get("upload_status", "not_required"),
                            "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                            "cloud_key": storage_resolution.get("cloud_key", ""),
                            "name": parsed_skill.name,
                            "description": parsed_skill.description,
                            "owner_scope": parsed_skill.metadata.get("owner_scope", "user"),
                            "version": parsed_skill.metadata.get("version", "1"),
                            "status": parsed_skill.metadata.get("status", "active"),
                            "precedence": parsed_skill.metadata.get("precedence", "normal"),
                            "triggers": parsed_skill.metadata.get("triggers", []),
                            "allowed_tools": parsed_skill.metadata.get("allowed_tools", []),
                            "examples": parsed_skill.metadata.get("examples", []),
                            "permissions": parsed_skill.metadata.get("permissions", []),
                            "inputs": parsed_skill.metadata.get("inputs", []),
                            "outputs": parsed_skill.metadata.get("outputs", []),
                            "access_scope": access_scope,
                            "deployment_scope": deployment_scope,
                            "text_preview": clip_context_text(parsed_skill.text),
                            "token_estimate": parsed_skill.token_estimate,
                            "metadata": skill_serving_metadata,
                            "scope": resource_record_scope,
                            "storage_options": envelope.get("storage_options", {}),
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    skill_debug_metadata = debug_resource_metadata(parsed_skill.metadata)
                    if skill_debug_metadata or parsed_skill.text:
                        self.append(
                            {
                                "record_type": "context_debug_record",
                                "debug_type": "skill_parse_detail",
                                "ref_type": "skill",
                                "ref_hash": skill_hash,
                                "skill_hash": skill_hash,
                                "import_task_hash": resource_import_task_hash,
                                "node_hash": node_hash,
                                "node_path": node_path,
                                "raw_uri": raw_uri,
                                "metadata_debug": skill_debug_metadata,
                                "text_preview": clip_context_text(parsed_skill.text),
                                "scope": resource_record_scope,
                                "updated_at_ms": envelope["ingestion_time_ms"],
                            }
                        )
                    self.append(
                        {
                            "record_type": "skill_registry",
                            "registry_hash": stable_hash(f"skill_registry:{skill_hash}:{deployment_scope}"),
                            "skill_hash": skill_hash,
                            "import_task_hash": resource_import_task_hash,
                            "raw_uri": raw_uri,
                            "requested_raw_uri": requested_raw_uri,
                            "raw_storage_mode": storage_resolution["storage_mode"],
                            "raw_storage_policy": raw_storage_policy,
                            "upload_status": storage_resolution.get("upload_status", "not_required"),
                            "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                            "cloud_key": storage_resolution.get("cloud_key", ""),
                            "name": parsed_skill.name,
                            "description": parsed_skill.description,
                            "owner_scope": parsed_skill.metadata.get("owner_scope", "user"),
                            "version": parsed_skill.metadata.get("version", "1"),
                            "status": parsed_skill.metadata.get("status", "active"),
                            "precedence": parsed_skill.metadata.get("precedence", "normal"),
                            "triggers": parsed_skill.metadata.get("triggers", []),
                            "allowed_tools": parsed_skill.metadata.get("allowed_tools", []),
                            "examples": parsed_skill.metadata.get("examples", []),
                            "permissions": parsed_skill.metadata.get("permissions", []),
                            "inputs": parsed_skill.metadata.get("inputs", []),
                            "outputs": parsed_skill.metadata.get("outputs", []),
                            "access_scope": access_scope,
                            "deployment_scope": deployment_scope,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    skill_vector = embedding_for_text(str(parsed_skill.metadata.get("embedding_text") or (parsed_skill.name + " " + parsed_skill.description)))
                    self.append(
                        {
                            "record_type": "context_embedding",
                            "embedding_type": "skill_summary",
                            "ref_type": "skill",
                            "ref_hash": skill_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "dim": len(skill_vector),
                            "model": embedding_model_name(),
                            "vector": skill_vector,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    parsed_chunks = parsed_skill_chunks
                else:
                    parsed_chunks = parse_resource(
                        parse_uri,
                        resource_type=resource_type or None,
                        text=parse_text,
                        chunk_hash_base=args.get("chunk_hash_base") if isinstance(args.get("chunk_hash_base"), int) else None,
                        resource_version=args.get("resource_version") if isinstance(args.get("resource_version"), str) else None,
                        supersedes_chunk_hashes=args.get("supersedes_chunk_hashes") if isinstance(args.get("supersedes_chunk_hashes"), dict) else None,
                    )
                    parsed_chunks = rewrite_chunk_uris(parsed_chunks, parse_uri=parse_uri, stored_raw_uri=raw_uri)
            except ResourceParserError as exc:
                resource_parse_error = str(exc)
                parsed_chunks = []
            finally:
                cleanup_temp_paths([str(path) for path in storage_resolution.get("temp_paths", []) if isinstance(path, str)])
            if not parsed_chunks:
                resource_import_task_status = "failed"
                self.append(
                    {
                        "record_type": "resource_import_task",
                        "task_hash": resource_import_task_hash,
                        "status": "failed",
                        "kind": envelope["kind"],
                        "raw_uri": raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "resource_type": resource_type,
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": raw_storage_policy,
                        "raw_bytes_stored": False,
                        "upload_status": storage_resolution.get("upload_status", "not_required"),
                        "error": resource_parse_error or "resource ingestion produced no chunks",
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "progress": {"stage": "failed", "percent": 100},
                        "updated_at_ms": now_ms(),
                    }
                )
                raise MatrixArkError(resource_parse_error or "resource ingestion produced no chunks")
            original_chunk_count = len(parsed_chunks)
            deduped_source_refs: list[str] = []
            seen_content_hashes: set[str] = set()
            unique_chunks = []
            for chunk_index, chunk in enumerate(parsed_chunks):
                chunk_content_hash = str(chunk.metadata.get("content_hash") or content_hash(chunk.text))
                if chunk_content_hash in seen_content_hashes:
                    deduped_source_refs.append(chunk.source_ref)
                    continue
                seen_content_hashes.add(chunk_content_hash)
                unique_chunks.append(chunk)
            parsed_chunks = unique_chunks
            deduped_chunk_count = original_chunk_count - len(parsed_chunks)
            if not parsed_chunks:
                raise MatrixArkError("resource ingestion produced only duplicate chunks")
            resource_version_value = str(parsed_chunks[0].metadata.get("resource_version") or "")
            resource_content_hash = content_hash("\n".join(str(chunk.metadata.get("content_hash") or content_hash(chunk.text)) for chunk in parsed_chunks))
            superseded_chunk_count = sum(1 for chunk in parsed_chunks if chunk.metadata.get("supersedes_chunk_hash"))
            superseded_chunk_hashes = [
                int(chunk.metadata["supersedes_chunk_hash"])
                for chunk in parsed_chunks
                if isinstance(chunk.metadata.get("supersedes_chunk_hash"), int)
            ]
            parse_warnings = aggregate_parse_warnings_from_chunks(parsed_chunks)
            chunk_vectors = embeddings_for_texts([embedding_text_for_chunk(chunk) for chunk in parsed_chunks])
            index_write_count = 0
            index_candidate_count = 0
            index_dropped_by_cap_count = 0
            secondary_index_budget = new_secondary_index_budget()
            resource_kind = "skill" if skill_hash is not None else "resource"
            resource_l0_text = summarize_text(
                summarize_resource_chunks(parsed_chunks, raw_uri=raw_uri, resource_kind=resource_kind),
                limit=700,
            )
            resource_summary_hash = stable_hash(f"{resource_kind}_l0:{raw_uri}:{node_hash}")
            resource_summary_vector = embedding_for_text(" ".join(node_path + [resource_l0_text]))
            self.append(
                {
                    "record_type": "context_summary",
                    "summary_type": f"{resource_kind}_l0",
                    "summary_hash": resource_summary_hash,
                    "import_task_hash": resource_import_task_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "raw_uri": raw_uri,
                    "summary_text": resource_l0_text,
                    "source_chunk_hashes": [chunk.chunk_hash for chunk in parsed_chunks],
                    "scope": resource_record_scope,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            self.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": f"{resource_kind}_l0",
                    "ref_type": "summary",
                    "ref_hash": resource_summary_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(resource_summary_vector),
                    "model": embedding_model_name(),
                    "vector": resource_summary_vector,
                    "scope": resource_record_scope,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            resource_dirty_hashes = self.mark_node_summary_dirty(
                node_path=node_path,
                scope=envelope["scope"],
                updated_at_ms=envelope["ingestion_time_ms"],
                source_ref_type=f"{resource_kind}_summary",
                source_hash_field="source_summary_hash",
                source_hash=resource_summary_hash,
                dirty_reason=f"{resource_kind}_update",
            )
            raw_resource_indexes = ordered_unique(
                [
                    context_index_name("source_type", envelope["kind"]),
                    context_index_name("resource_type", resource_type or parsed_chunks[0].metadata.get("resource_type", "txt")),
                ]
                + (
                    [
                        context_index_name("skill_name", parsed_skill.name),
                    ]
                    + [context_index_name("skill_trigger", trigger) for trigger in parsed_skill.metadata.get("triggers", [])]
                    + [context_index_name("skill_tool", tool) for tool in parsed_skill.metadata.get("allowed_tools", [])]
                    if skill_hash is not None
                    else []
                )
            )
            index_candidate_count += len(raw_resource_indexes)
            resource_indexes = take_secondary_index_terms(raw_resource_indexes, secondary_index_budget)
            for index_name in resource_indexes:
                index_write_count += 1
                self.append(
                    context_index_posting_record(
                        index_name=index_name,
                        data_model=f"{resource_kind}_summary",
                        ref_type="summary",
                        ref_hashes=[resource_summary_hash],
                        node_hash=node_hash,
                        scope=resource_record_scope,
                        updated_at_ms=envelope["ingestion_time_ms"],
                        storage_options=envelope.get("storage_options", {}),
                    )
                )
            resource_manifest_hash = stable_hash(f"resource_manifest:{raw_uri}:{node_hash}")
            raw_uri_hash = stable_hash(raw_uri)
            if envelope["kind"] == "resource":
                manifest_hash = resource_manifest_hash
                self.append(
                    {
                        "record_type": "resource_manifest",
                        "resource_hash": manifest_hash,
                        "import_task_hash": resource_import_task_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "raw_uri": raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                        "resource_version": resource_version_value,
                        "content_hash": resource_content_hash,
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": raw_storage_policy,
                        "raw_bytes_stored": False,
                        "upload_status": storage_resolution.get("upload_status", "not_required"),
                        "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                        "cloud_key": storage_resolution.get("cloud_key", ""),
                        "parse_warnings": parse_warnings[:100],
                        "parse_warning_count": len(parse_warnings),
                        "chunk_count": len(parsed_chunks),
                        "original_chunk_count": original_chunk_count,
                        "deduped_chunk_count": deduped_chunk_count,
                        "deduped_source_refs": deduped_source_refs[:50],
                        "superseded_chunk_count": superseded_chunk_count,
                        "superseded_chunk_hashes": superseded_chunk_hashes[:200],
                        "summary_dirty_hashes": resource_dirty_hashes,
                        "async_parent_summary_required": bool(resource_dirty_hashes),
                        "access_scope": access_scope,
                        "deployment_scope": deployment_scope,
                        "token_estimate": sum(chunk.token_estimate for chunk in parsed_chunks),
                        "scope": resource_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                self.append(
                    {
                        "record_type": "resource_registry",
                        "registry_hash": stable_hash(f"resource_registry:{raw_uri}:{node_hash}:{resource_version_value}:{deployment_scope}"),
                        "resource_hash": manifest_hash,
                        "import_task_hash": resource_import_task_hash,
                        "raw_uri": raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                        "resource_version": resource_version_value,
                        "content_hash": resource_content_hash,
                        "chunk_count": len(parsed_chunks),
                        "superseded_chunk_hashes": superseded_chunk_hashes[:200],
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": raw_storage_policy,
                        "upload_status": storage_resolution.get("upload_status", "not_required"),
                        "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                        "cloud_key": storage_resolution.get("cloud_key", ""),
                        "access_scope": access_scope,
                        "deployment_scope": deployment_scope,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            # Attachments keep their manifest and raw URI -- listed, addressable and fetchable
            # on demand -- but skip chunk materialization. Measured on one 66.2 KB document the
            # chunks cost 7.05x the source: 60 resource_chunk records at 3.75x plus 76
            # embeddings at 2.93x. Those vectors were 32-dim deterministic ones; a 384-dim
            # encoder takes the same file toward 40x. For a file fetched rarely or never that is
            # the wrong trade, and there was no way to decline it -- raw_storage_policy was
            # written into records and read back for display, but nothing branched on its value.
            #
            # The manifest above is written either way, so the resource stays discoverable and
            # its chunk_count still reports what the file WOULD chunk into.
            materialize_chunks = resource_chunk_materialization_enabled(args, envelope)
            for chunk, vector in zip(parsed_chunks if materialize_chunks else [], chunk_vectors):
                resource_chunk_hashes.append(chunk.chunk_hash)
                source_locator = source_locator_from_ref(chunk.source_ref, raw_uri)
                chunk_metadata_source = {**chunk.metadata, "source_locator": source_locator}
                chunk_metadata = serving_resource_metadata(chunk_metadata_source)
                chunk_debug_metadata = debug_resource_metadata(chunk.metadata)
                if skill_hash is not None:
                    self.append(
                        {
                            "record_type": "skill_section",
                            "import_task_hash": resource_import_task_hash,
                            "skill_hash": skill_hash,
                            "section_hash": chunk.chunk_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "resource_hash": skill_hash,
                            "raw_uri_hash": raw_uri_hash,
                            "source_locator": source_locator,
                            "heading": chunk_metadata.get("heading", ""),
                            "text": chunk.text,
                            "token_estimate": chunk.token_estimate,
                            "metadata": chunk_metadata,
                            "access_scope": access_scope,
                            "deployment_scope": deployment_scope,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                self.append(
                    {
                        "record_type": "resource_chunk",
                        "import_task_hash": resource_import_task_hash,
                        # Explicit order. Reassembling content by log order works only while the
                        # log is read append-ordered and nothing in between re-orders or
                        # re-materializes; an index makes the order a property of the record
                        # instead of a property of how it happened to be read.
                        "chunk_index": chunk_index,
                        "chunk_hash": chunk.chunk_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "resource_hash": resource_manifest_hash if skill_hash is None else skill_hash,
                        "raw_uri_hash": raw_uri_hash,
                        "resource_type": chunk_metadata.get("resource_type") or resource_type,
                        "source_locator": source_locator,
                        "text": chunk.text,
                        "token_estimate": chunk.token_estimate,
                        "metadata": chunk_metadata,
                        "access_scope": access_scope,
                        "deployment_scope": deployment_scope,
                        "scope": resource_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                if chunk_debug_metadata:
                    self.append(
                        {
                            "record_type": "context_debug_record",
                            "debug_type": "resource_chunk_parse_detail",
                            "ref_type": "skill_section" if skill_hash is not None else "resource_chunk",
                            "ref_hash": chunk.chunk_hash,
                            "chunk_hash": chunk.chunk_hash,
                            "import_task_hash": resource_import_task_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "resource_hash": resource_manifest_hash if skill_hash is None else skill_hash,
                            "raw_uri_hash": raw_uri_hash,
                            "raw_uri": raw_uri,
                            "source_locator": source_locator,
                            "source_ref": chunk.source_ref,
                            "metadata_debug": chunk_debug_metadata,
                            "text_preview": clip_context_text(chunk.text),
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                self.append(
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "resource_chunk",
                        "ref_type": "resource_chunk",
                        "ref_hash": chunk.chunk_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dim": len(vector),
                        "model": embedding_model_name(),
                        "vector": vector,
                        "scope": resource_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                if skill_hash is not None:
                    self.append(
                        {
                            "record_type": "context_embedding",
                            "embedding_type": "skill_section",
                            "ref_type": "skill_section",
                            "ref_hash": chunk.chunk_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "dim": len(vector),
                            "model": embedding_model_name(),
                            "vector": vector,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                raw_chunk_index_terms = (
                    [
                        context_index_name("source_type", "skill" if skill_hash is not None else "resource"),
                        context_index_name("resource_type", chunk_metadata.get("resource_type") or resource_type),
                    ]
                    + metadata_index_terms(chunk.metadata)
                    + (
                        [context_index_name("skill_name", parsed_skill.name)]
                        + [context_index_name("skill_trigger", trigger) for trigger in parsed_skill.metadata.get("triggers", [])]
                        + [context_index_name("skill_tool", tool) for tool in parsed_skill.metadata.get("allowed_tools", [])]
                        if skill_hash is not None and parsed_skill is not None
                        else []
                    )
                )
                index_candidate_count += len([term for term in raw_chunk_index_terms if term])
                chunk_index_terms = limited_index_terms(
                    raw_chunk_index_terms,
                    limit=MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
                )
                index_dropped_by_cap_count += max(0, len(ordered_unique([term for term in raw_chunk_index_terms if term])) - len(chunk_index_terms))
                chunk_index_terms = take_secondary_index_terms(chunk_index_terms, secondary_index_budget)
                for index_name in chunk_index_terms:
                    index_write_count += 1
                    self.append(
                        {
                            "record_type": "context_index",
                            "index_name": index_name,
                            "index_hash": stable_hash(f"{index_name}:{chunk.chunk_hash}"),
                            "ref_type": "skill_section" if skill_hash is not None else "resource_chunk",
                            "ref_hash": chunk.chunk_hash,
                            "chunk_hash": chunk.chunk_hash,
                            "resource_hash": resource_manifest_hash if skill_hash is None else skill_hash,
                            "source_locator": source_locator,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
            resource_fact_records: list[Json] = []
            fact_chunks = [chunk for chunk in parsed_chunks if skill_hash is None and should_extract_resource_fact(chunk.text, chunk.metadata)][:MAX_RESOURCE_FACT_CHUNKS]
            remaining_resource_fact_budget = max(0, MAX_RESOURCE_FACTS_PER_RESOURCE)
            for chunk in fact_chunks:
                if remaining_resource_fact_budget <= 0:
                    break
                source_locator = source_locator_from_ref(chunk.source_ref, raw_uri)
                chunk_metadata = serving_resource_metadata({**chunk.metadata, "source_locator": source_locator})
                for fact_extraction in extract_resource_facts(
                    chunk,
                    chunk_metadata=chunk_metadata,
                    envelope=envelope,
                    raw_uri=raw_uri,
                    resource_version=resource_version_value,
                )[:remaining_resource_fact_budget]:
                    remaining_resource_fact_budget -= 1
                    fact_event_type = str(fact_extraction["event_type"])
                    fact_entity_type = str(fact_extraction["entity_type"])
                    fact_value = str(fact_extraction.get("value", ""))
                    fact_event_hash = stable_hash(f"resource_fact:{chunk.chunk_hash}:{fact_event_type}:{resource_version_value}")
                    resource_fact_event_hashes.append(fact_event_hash)
                    fact_summary = summarize_text(f"{fact_event_type}: {fact_value}", limit=320)
                    resource_fact_records.append(
                        {
                            "record_type": "context_event",
                            "event_id_hash": fact_event_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "text": chunk.text,
                            "summary_text": fact_summary,
                            "classification": fact_extraction.get("classification", ""),
                            "event_type": fact_extraction.get("event_type", ""),
                            "entity_type": fact_extraction.get("entity_type", ""),
                            "status": fact_extraction.get("status", "observed"),
                            "source_kind": "resource_fact",
                            "envelope": {**envelope, "kind": "resource_fact"},
                            "internal_extraction": fact_extraction,
                            "source_chunk_hash": chunk.chunk_hash,
                            "resource_hash": resource_manifest_hash,
                            "source_locator": source_locator,
                            "resource_version": resource_version_value,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    fact_vector = embedding_for_text(fact_event_type + " " + fact_value + " " + chunk.text)
                    resource_fact_records.append(
                        {
                            "record_type": "context_embedding",
                            "embedding_type": "event_text",
                            "ref_type": "event",
                            "ref_hash": fact_event_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "dim": len(fact_vector),
                            "model": embedding_model_name(),
                            "vector": fact_vector,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    entity_name = str(fact_extraction.get("entity_name") or fact_entity_type)
                    entity_hash = stable_hash(f"{node_hash}:{fact_entity_type}:{entity_name}:{chunk.chunk_hash}")
                    resource_fact_entity_hashes.append(entity_hash)
                    entity_state = summarize_text(f"{fact_event_type}: {fact_value}. Source: {chunk.text}", limit=360)
                    resource_fact_records.append(
                        {
                            "record_type": "context_entity",
                            "entity_hash": entity_hash,
                            "batch_id_hash": resource_import_task_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "scope": resource_record_scope,
                            "entity_type": fact_entity_type,
                            "entity_name": entity_name,
                            "state": entity_state,
                            "confidence": fact_extraction.get("confidence", 0.78),
                            "operator": "LATEST",
                            "source_event_ids": [fact_event_hash],
                            "source_chunk_hash": chunk.chunk_hash,
                            "resource_hash": resource_manifest_hash,
                            "source_locator": source_locator,
                            "resource_version": resource_version_value,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    entity_vector = embedding_for_text(fact_entity_type + " " + entity_name + " " + entity_state)
                    resource_fact_records.append(
                        {
                            "record_type": "context_embedding",
                            "embedding_type": "entity_state",
                            "ref_type": "entity",
                            "ref_hash": entity_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "dim": len(entity_vector),
                            "model": embedding_model_name(),
                            "vector": entity_vector,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    # Resource facts are ContextEvent/ContextEntity records with
                    # source_chunk refs. The resource chunk/index rows already provide
                    # secondary filtering, so avoid per-fact event index fanout here.
            if resource_fact_records:
                self.append_many(resource_fact_records)
            resource_import_metrics = {
                "duration_ms": round((time.perf_counter() - import_started_perf) * 1000.0, 3),
                "parser_chunk_count": original_chunk_count,
                "chunk_count": len(parsed_chunks),
                "dedupe_count": deduped_chunk_count,
                "embedding_count": len(chunk_vectors) + 1 + len(resource_fact_event_hashes) + len(resource_fact_entity_hashes),
                "resource_fact_count": len(resource_fact_event_hashes),
                "resource_entity_count": len(resource_fact_entity_hashes),
                "index_candidate_count": index_candidate_count,
                "index_write_count": index_write_count,
                "index_dropped_by_cap_count": index_dropped_by_cap_count,
                **secondary_index_budget_summary(secondary_index_budget),
                "index_cap_per_chunk": MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
                "index_cap_per_fact": MAX_INDEX_TERMS_PER_RESOURCE_FACT,
                "parse_warning_count": len(parse_warnings),
                "parse_warnings": parse_warnings[:100],
                "raw_storage_mode": storage_resolution["storage_mode"],
                "raw_storage_policy": raw_storage_policy,
                "raw_bytes_stored": False,
                "upload_status": storage_resolution.get("upload_status", "not_required"),
                "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                "cloud_key": storage_resolution.get("cloud_key", ""),
                "summary_dirty_count": len(resource_dirty_hashes),
            }
            resource_import_task_status = "completed"
            # On the background worker this call still has writes ahead of it -- the hot path
            # event batch and its index postings land after this point. Appending "completed"
            # here publishes the one signal a caller polls for while a fifth of the task's
            # records are still missing, and lets the caller tear the log down underneath the
            # worker. Hold the marker and append it once the call has finished writing.
            append_import_completion = (
                held_import_completion.append if resource_import_background else self.append
            )
            append_import_completion(
                {
                    "record_type": "resource_import_task",
                    "task_hash": resource_import_task_hash,
                    "status": "completed",
                    "kind": envelope["kind"],
                    "raw_uri": raw_uri,
                    "requested_raw_uri": requested_raw_uri,
                    "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                    "resource_version": resource_version_value,
                    "content_hash": resource_content_hash,
                    "raw_storage_mode": storage_resolution["storage_mode"],
                    "raw_storage_policy": raw_storage_policy,
                    "raw_bytes_stored": False,
                    "upload_status": storage_resolution.get("upload_status", "not_required"),
                    "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                    "cloud_key": storage_resolution.get("cloud_key", ""),
                    "parse_warnings": parse_warnings[:100],
                    "parse_warning_count": len(parse_warnings),
                    "chunk_count": len(parsed_chunks),
                    "original_chunk_count": original_chunk_count,
                    "deduped_chunk_count": deduped_chunk_count,
                    "superseded_chunk_count": superseded_chunk_count,
                    "superseded_chunk_hashes": superseded_chunk_hashes[:200],
                    "resource_fact_count": len(resource_fact_event_hashes),
                    "resource_entity_count": len(resource_fact_entity_hashes),
                    "index_candidate_count": index_candidate_count,
                    "index_write_count": index_write_count,
                    "index_dropped_by_cap_count": index_dropped_by_cap_count,
                    **secondary_index_budget_summary(secondary_index_budget),
                    "index_cap_per_chunk": MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
                    "index_cap_per_fact": MAX_INDEX_TERMS_PER_RESOURCE_FACT,
                    "summary_dirty_hashes": resource_dirty_hashes,
                    "progress": {"stage": "completed", "percent": 100},
                    "metrics": resource_import_metrics,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": resource_record_scope,
                    "updated_at_ms": now_ms(),
                }
            )
            self.append(
                {
                    "record_type": "matrixark_metric",
                    "metric_name": "resource_import",
                    "task_hash": resource_import_task_hash,
                    "kind": envelope["kind"],
                    "raw_uri": raw_uri,
                    "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                    "metrics": resource_import_metrics,
                    "progress": {"stage": "completed", "percent": 100},
                    "scope": resource_record_scope,
                    "created_at_ms": now_ms(),
                }
            )
        hot_record_scope = resource_record_scope if envelope["kind"] in {"resource", "skill"} else envelope["scope"]
        summary_text = summarize_text(text)
        embedding_started_perf = time.perf_counter()
        event_embedding = embedding_for_text(text)
        self._observe_model_latency("embedding", (time.perf_counter() - embedding_started_perf) * 1000.0)
        hot_messages = [message for message in envelope.get("messages", []) if isinstance(message, dict)]
        hot_event_type = (
            context_event_type_for_message(hot_messages[0], str(extraction.get("event_type") or ""))
            if len(hot_messages) == 1
            else str(extraction.get("event_type") or infer_event_type(text))
        )
        hot_profile_memory_fields: Json = {}
        if hot_event_type == "memory_feature" or profile_entity_type_for_memory_text(text) == "memory_feature_profile":
            hot_profile_memory_fields = {
                "profile_memory_class": "memory_feature",
                "profile_memory_kind": "memory_feature",
                "source_profile_memory_classes": ["memory_feature"],
                "source_profile_memory_kinds": ["memory_feature"],
            }
        elif hot_event_type in {"assistant_response", "assistant_decision", "tool_evidence"}:
            hot_profile_memory_fields = {
                "profile_memory_class": "codex_outcome",
                "profile_memory_kind": "codex_outcome",
                "source_profile_memory_classes": ["codex_outcome"],
                "source_profile_memory_kinds": ["codex_outcome"],
            }
        hot_event_memory_layer = candidate_memory_layer_name(
            {
                "record_type": "context_event",
                "ref_type": "event",
                "event_type": hot_event_type,
                **source_lineage,
                **hot_profile_memory_fields,
                "memory_scope": "session",
                "session_continuity": "same_session",
                "extraction_phase": "hot_path",
            }
        )
        with self.write_batch("message_ingest_hot_path"):
            session_key_parts = [str(part) for part in context_node_key(envelope)]
            # Ephemeral (TTL) ingests are excluded from rollup/summary generation: they are meant to
            # vanish, so their text must never be folded into a durable session summary.
            if any(session_key_parts) and not envelope.get("ephemeral"):
                session_summary_source = " ".join(
                    [item.get("text", "") for item in prior_context.get("summaries", [])[:2]]
                    + [item.get("text", "") for item in prior_context.get("messages", [])[:2]]
                    + [text]
                )
                session_summary_text = summarize_text(session_summary_source, limit=512)
                session_summary_hash = stable_hash("session:" + "/".join(session_key_parts))
                session_summary_record = {
                    "record_type": "context_summary",
                    "summary_type": "session_l0",
                    "summary_hash": session_summary_hash,
                    "summary_identity": "stable_per_session_node",
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "ref_type": "summary",
                    "context_node_key": session_key_parts,
                    "summary_text": session_summary_text,
                    "source_event_hash": event_id_hash,
                    **source_lineage,
                    **hot_profile_memory_fields,
                    "source_memory_scopes": source_lineage.get("source_memory_scopes", ["session"]),
                    "source_session_continuities": source_lineage.get("source_session_continuities", ["same_session"]),
                    "source_extraction_phases": source_lineage.get("source_extraction_phases", ["hot_path"]),
                    "extraction_phase": "hot_path",
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "scope": hot_record_scope,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
                session_summary_memory_layer = candidate_memory_layer_name(session_summary_record)
                if session_summary_memory_layer:
                    session_summary_record["memory_layer"] = session_summary_memory_layer
                self.append(session_summary_record)
                for index_name in candidate_index_terms(session_summary_record, {}, {}):
                    session_summary_index = context_index_posting_record(
                        index_name=index_name,
                        data_model="context_summary",
                        ref_type="summary",
                        ref_hashes=[session_summary_hash],
                        node_hash=node_hash,
                        scope=hot_record_scope,
                        updated_at_ms=envelope["ingestion_time_ms"],
                    )
                    session_summary_index["access_scope"] = hot_record_scope
                    self.append(session_summary_index)
                session_summary_vector = embedding_for_text(session_summary_text)
                self.append(
                    compact_context_embedding_record({
                        "record_type": "context_embedding",
                        "embedding_type": "session_l0",
                        "ref_type": "summary",
                        "ref_hash": session_summary_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dim": len(session_summary_vector),
                        "model": embedding_model_name(),
                        "vector": session_summary_vector,
                        "scope": hot_record_scope,
                        **source_lineage,
                        **hot_profile_memory_fields,
                        "source_memory_scopes": source_lineage.get("source_memory_scopes", ["session"]),
                        "source_session_continuities": source_lineage.get("source_session_continuities", ["same_session"]),
                        "source_extraction_phases": source_lineage.get("source_extraction_phases", ["hot_path"]),
                        "memory_layer": session_summary_memory_layer,
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "extraction_phase": "hot_path",
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    })
                )
            self.append(
                compact_context_embedding_record({
                    "record_type": "context_embedding",
                    "embedding_type": "event_text",
                    "ref_type": "event",
                    "ref_hash": event_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(event_embedding),
                    "model": embedding_model_name(),
                    "vector": event_embedding,
                    "scope": hot_record_scope,
                    "classification": extraction.get("classification", ""),
                    "event_type": hot_event_type,
                    "status": extraction.get("status", "observed"),
                    "source_kind": envelope.get("kind", "message"),
                    **source_lineage,
                    **hot_profile_memory_fields,
                    "source_memory_scopes": source_lineage.get("source_memory_scopes", ["session"]),
                    "source_session_continuities": source_lineage.get("source_session_continuities", ["same_session"]),
                    "source_extraction_phases": source_lineage.get("source_extraction_phases", ["hot_path"]),
                    "memory_layer": hot_event_memory_layer,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "extraction_phase": "hot_path",
                    "updated_at_ms": envelope["ingestion_time_ms"],
                })
            )
            record = {
                "record_type": "context_event",
                "event_id_hash": event_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "text": text,
                "classification": extraction.get("classification", ""),
                "event_type": hot_event_type,
                "entity_type": extraction.get("entity_type", ""),
                "status": extraction.get("status", "observed"),
                "source_kind": envelope.get("kind", "message"),
                "envelope": envelope,
                "internal_extraction": extraction,
                "prior_context": prior_context,
                "agent_hook": hook,
                **source_lineage,
                **hot_profile_memory_fields,
                "storage_options": envelope.get("storage_options", {}),
                "memory_scope": "session",
                "session_continuity": "same_session",
                "extraction_phase": "hot_path",
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
            hot_event_memory_layer = candidate_memory_layer_name(record)
            if hot_event_memory_layer:
                record["memory_layer"] = hot_event_memory_layer
            self.append(record)
            event_index_terms = ordered_unique(
                list(extraction.get("indexes") or [])
                + [
                    context_index_name("event_type", hot_event_type),
                    context_index_name("classification", non_default_classification(extraction.get("classification"))),
                    context_index_name("status", extraction.get("status") or "observed"),
                    context_index_name("source_type", envelope["kind"]),
                ]
                + sorted(candidate_index_terms(record, {}, {}))
            )
            event_index_records: list[Json] = []
            for index_name in event_index_terms:
                event_index_records.append(
                    {
                        "record_type": "context_index",
                        "index_name": index_name,
                        "data_model": "context_event",
                        "ref_type": "event",
                        "ref_hashes": [event_id_hash],
                        "node_hash": node_hash,
                        "scope": envelope["scope"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            if event_index_records:
                self.append_many(event_index_records)
            self.append_session_buffer_event(envelope=envelope, event_id_hash=event_id_hash, node_hash=node_hash, node_path=node_path, hook=hook)
            summary_refresh = self.append_node_summary_embeddings(
                node_path=node_path,
                source_text=text,
                scope=hot_record_scope,
                updated_at_ms=envelope["ingestion_time_ms"],
                source_hash_field="source_event_hash",
                source_hash=event_id_hash,
            )
        pending_events = self.pending_session_events(envelope["scope"])
        pending_event_count = len(pending_events)
        pending_message_count = session_event_message_count(pending_events)
        auto_batch_result: Json | None = None
        auto_batch_extract = auto_batch_extract_enabled(args, kind=envelope["kind"])
        due_idle_commit_result: Json | None = None
        if auto_batch_extract:
            due_idle_commit_result = self.drain_due_idle_session_commits(
                scope=envelope["scope"],
                args=args,
                hook=hook,
            )
            if due_idle_commit_result.get("drained_task_count", 0):
                pending_events = self.pending_session_events(envelope["scope"])
                pending_event_count = len(pending_events)
                pending_message_count = session_event_message_count(pending_events)
        session_boundary_commit = session_boundary_commit_requested(args, hook=hook)
        session_buffer_threshold = args.get("session_buffer_threshold", 20)
        if not isinstance(session_buffer_threshold, int) or session_buffer_threshold <= 0:
            raise MatrixArkError("session_buffer_threshold must be a positive integer")
        threshold_ready = pending_event_count >= session_buffer_threshold or pending_message_count >= session_buffer_threshold
        immediate_idle_ready = bool(
            auto_batch_extract
            and not session_boundary_commit
            and not threshold_ready
            and idle_commit_timeout_ms == 0
            and pending_event_count > 0
        )
        idle_ready = bool(
            isinstance(idle_commit_result, dict)
            and idle_commit_result.get("status") in {"accepted", "committed"}
            and idle_commit_result.get("trigger_policy") == "idle_timeout"
        )
        if auto_batch_extract and (session_boundary_commit or threshold_ready):
            auto_batch_result = self.session_commit(
                {
                    "scope": hot_record_scope,
                    "metadata": envelope["metadata"],
                    "threshold_messages": session_buffer_threshold,
                    "force": session_boundary_commit,
                    "max_messages": None if session_boundary_commit else session_buffer_threshold,
                    "commit_reason": "hook_boundary" if session_boundary_commit else "threshold",
                    "understanding_provider": args.get("understanding_provider"),
                    "extraction_provider": args.get("extraction_provider"),
                    "segment_provider": args.get("segment_provider"),
                    "segment_model": args.get("segment_model"),
                    "segment_model_path": args.get("segment_model_path"),
                    "segment_max_new_tokens": args.get("segment_max_new_tokens"),
                    "segment_provider_fallback": args.get("segment_provider_fallback"),
                    "skip_prior_context": bool(args.get("skip_prior_context", False)),
                    "storage_options": envelope.get("storage_options", {}),
                },
                hook=hook,
            )
        elif immediate_idle_ready:
            auto_batch_result = self.session_commit(
                {
                    "scope": hot_record_scope,
                    "metadata": envelope["metadata"],
                    "threshold_messages": session_buffer_threshold,
                    "force": False,
                    "commit_before_ms": int(envelope["ingestion_time_ms"]),
                    "idle_timeout_ms": 0,
                    "commit_reason": "idle_timeout",
                    "understanding_provider": args.get("understanding_provider"),
                    "extraction_provider": args.get("extraction_provider"),
                    "segment_provider": args.get("segment_provider"),
                    "segment_model": args.get("segment_model"),
                    "segment_model_path": args.get("segment_model_path"),
                    "segment_max_new_tokens": args.get("segment_max_new_tokens"),
                    "segment_provider_fallback": args.get("segment_provider_fallback"),
                    "skip_prior_context": bool(args.get("skip_prior_context", False)),
                    "storage_options": envelope.get("storage_options", {}),
                },
                hook=hook,
            )
        elif auto_batch_extract and idle_ready and isinstance(idle_commit_result, dict):
            auto_batch_result = idle_commit_result
        elif auto_batch_extract:
            auto_batch_result = deferred_idle_auto_batch_result(
                idle_commit_result=idle_commit_result,
                pending_event_count=pending_event_count,
                pending_message_count=pending_message_count,
                threshold_messages=session_buffer_threshold,
                idle_commit_timeout_ms=idle_commit_timeout_ms,
            )
        idle_commit_scheduled = bool(
            isinstance(auto_batch_result, dict)
            and auto_batch_result.get("status") == "deferred"
            and auto_batch_result.get("trigger_policy") == "idle_timeout"
            and auto_batch_result.get("idle_commit_scheduled")
        )
        if idle_commit_scheduled and idle_commit_timeout_ms is not None:
            self.append(
                idle_commit_scheduled_task_record(
                    event_id_hash=event_id_hash,
                    node_hash=record["node_hash"],
                    node_path=node_path,
                    scope=envelope["scope"],
                    storage_options=args.get("storage_options", {}) if isinstance(args.get("storage_options"), dict) else {},
                    ingestion_time_ms=int(envelope["ingestion_time_ms"]),
                    idle_commit_timeout_ms=int(idle_commit_timeout_ms),
                    pending_event_count=pending_event_count,
                    pending_message_count=pending_message_count,
                    threshold_messages=session_buffer_threshold,
                )
            )
        for held_record in held_import_completion:
            self.append(held_record)
        return {
            "status": "accepted",
            "event_id_hash": event_id_hash,
            "node_hash": record["node_hash"],
            "storage_options": envelope.get("storage_options", {}),
            "storage_route": envelope.get("storage_route", {}),
            "hook_captured": hook is not None,
            **({"profile_scope_warning": self._profile_scope_warning} if getattr(self, "_profile_scope_warning", "") else {}),
            "embedding_model": embedding_model_name(),
            "embedding_execution_mode": embedding_execution_mode_name(),
            "embedding_fallback_used": embedding_fallback_used(),
            "extraction_mode": extraction["mode"],
            "classification": extraction.get("classification", "UNCLASSIFIED"),
            "prior_context": extraction.get("prior_context", ""),
            "prior_refs": extraction.get("prior_refs", []),
            "prior_message_count": extraction.get("prior_message_count", 0),
            "prior_summary_count": extraction.get("prior_summary_count", 0),
            "quality_warning": extraction.get("quality_warning", ""),
            "summary_refresh": summary_refresh,
            "resource_summary_refresh": {
                "status": "dirty_marked" if resource_dirty_hashes else "not_applicable",
                "dirty_hashes": resource_dirty_hashes,
                "refresh_result": None,
                "async_required": bool(resource_dirty_hashes),
            },
            "resource_import_task": {
                "task_hash": resource_import_task_hash,
                "status": resource_import_task_status,
                "wait": resource_import_wait,
                "metrics": resource_import_metrics,
                "raw_uri": raw_uri if resource_import_task_hash else "",
                "requested_raw_uri": requested_raw_uri if resource_import_task_hash else "",
                "raw_storage_mode": storage_resolution.get("storage_mode", "") if resource_import_task_hash else "",
                "raw_storage_policy": raw_storage_policy if resource_import_task_hash else "",
                "raw_bytes_stored": False if resource_import_task_hash else None,
                "upload_status": storage_resolution.get("upload_status", "") if resource_import_task_hash else "",
                "cloud_bucket": storage_resolution.get("cloud_bucket", "") if resource_import_task_hash else "",
                "cloud_key": storage_resolution.get("cloud_key", "") if resource_import_task_hash else "",
                "progress": {"stage": resource_import_task_status, "percent": 100 if resource_import_task_status == "completed" else 0},
            },
            "node_materialization": node_materialization,
            "resource_chunks": resource_chunk_hashes,
            "resource_chunk_count": len(resource_chunk_hashes),
            "resource_original_chunk_count": original_chunk_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_deduped_chunk_count": deduped_chunk_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_deduped_source_refs": deduped_source_refs[:20] if envelope["kind"] in {"resource", "skill"} else [],
            "resource_version": resource_version_value if envelope["kind"] in {"resource", "skill"} else "",
            "resource_content_hash": resource_content_hash if envelope["kind"] in {"resource", "skill"} else "",
            "resource_parse_warnings": parse_warnings if envelope["kind"] in {"resource", "skill"} else [],
            "resource_parse_warning_count": len(parse_warnings) if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_raw_uri": raw_uri if envelope["kind"] in {"resource", "skill"} else "",
            "resource_requested_raw_uri": requested_raw_uri if envelope["kind"] in {"resource", "skill"} else "",
            "resource_raw_storage_mode": storage_resolution.get("storage_mode", "") if envelope["kind"] in {"resource", "skill"} else "",
            "resource_raw_storage_policy": raw_storage_policy if envelope["kind"] in {"resource", "skill"} else "",
            "resource_raw_bytes_stored": False if envelope["kind"] in {"resource", "skill"} else None,
            "backend_readiness": backend_readiness or {},
            "resource_superseded_chunk_count": superseded_chunk_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_superseded_chunk_hashes": superseded_chunk_hashes if envelope["kind"] in {"resource", "skill"} else [],
            "resource_fact_events": resource_fact_event_hashes,
            "resource_fact_event_count": len(resource_fact_event_hashes),
            "resource_fact_entities": resource_fact_entity_hashes,
            "resource_fact_entity_count": len(resource_fact_entity_hashes),
            "resource_index_candidate_count": index_candidate_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_index_write_count": index_write_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_index_dropped_by_cap_count": index_dropped_by_cap_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_index_cap_per_chunk": MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
            "resource_index_cap_per_fact": MAX_INDEX_TERMS_PER_RESOURCE_FACT,
            "skill_hash": skill_hash,
            "session_buffer": {
                "buffer_key": list(session_buffer_key(envelope)),
                "pending_event_count": pending_event_count,
                "pending_message_count": pending_message_count,
                "threshold_messages": session_buffer_threshold,
                "threshold_ready": threshold_ready,
                "session_boundary_commit": session_boundary_commit,
                "idle_ready": bool(idle_ready or immediate_idle_ready),
                "idle_commit_scheduled": idle_commit_scheduled,
                "auto_batch_extract": auto_batch_extract,
            },
            "idle_commit_result": idle_commit_result,
            "due_idle_commit_result": due_idle_commit_result,
            "auto_batch_extract_result": auto_batch_result,
        }


    def batch_extract(self, args: Json, *, hook: Json | None = None) -> Json:
        envelope = normalize_envelope(args, default_kind="message")
        extraction_context_messages = args.get("extraction_context_messages", [])
        if not isinstance(extraction_context_messages, list):
            extraction_context_messages = []
        extraction_context_event_ids = (
            [int(ref) for ref in args.get("extraction_context_event_ids", [])]
            if isinstance(args.get("extraction_context_event_ids", []), list)
            else []
        )
        extraction_envelope = envelope
        if extraction_context_messages:
            extraction_envelope = {
                **envelope,
                "messages": [
                    *envelope["messages"],
                    *[
                        dict(message)
                        for message in extraction_context_messages
                        if isinstance(message, dict)
                        and isinstance(message.get("role"), str)
                        and isinstance(message.get("content"), str)
                    ],
                ],
            }
        hook = validate_hook(hook)
        source_lineage = context_source_lineage(envelope, hook)
        threshold = args.get("threshold_messages", 20)
        force = bool(args.get("force", False))
        derive_from_existing_events = bool(args.get("derive_from_existing_events", False))
        source_event_ids = [int(ref) for ref in args.get("source_event_ids", [])] if isinstance(args.get("source_event_ids", []), list) else []
        source_event_records_arg = args.get("source_event_records", [])
        source_event_record_by_hash: dict[int, Json] = {}
        if isinstance(source_event_records_arg, list):
            for source_record in source_event_records_arg:
                if not isinstance(source_record, dict):
                    continue
                try:
                    source_event_record_by_hash[int(source_record.get("event_id_hash"))] = source_record
                except (TypeError, ValueError):
                    continue
        extraction_phase = str(args.get("extraction_phase") or "").strip().lower()
        if extraction_phase not in {"provisional", "final", "standalone"}:
            extraction_phase = "final" if force else "provisional"
        final_session_boundary = bool(args.get("final_session_boundary", extraction_phase == "final"))
        if not isinstance(threshold, int) or threshold <= 0:
            raise MatrixArkError("threshold_messages must be a positive integer")
        if len(envelope["messages"]) < threshold and not force:
            return {
                "status": "deferred",
                "message_count": len(envelope["messages"]),
                "threshold_messages": threshold,
                "reason": "logical batch below extraction threshold",
            }

        prior_records = [] if args.get("skip_prior_context") else self.prior_context_records(envelope["scope"])
        prior_context = (
            {"level": "", "refs": [], "messages": [], "summaries": [], "char_count": 0, "limit": MAX_PRIOR_MESSAGES}
            if args.get("skip_prior_context")
            else collect_prior_context(envelope, prior_records)
        )
        extraction_started_perf = time.perf_counter()
        extraction = one_pass_memory_extraction(extraction_envelope, prior_context=prior_context)
        self._observe_model_latency("batch_extraction", (time.perf_counter() - extraction_started_perf) * 1000.0)
        batch_text = text_from_messages(envelope["messages"])
        batch_id_hash = stable_hash(
            f"batch:{batch_text}:{envelope['scope']}:{envelope['ingestion_time_ms']}"
        )
        node_hint = envelope["metadata"].get("node_path") or self.default_session_node_path(envelope["scope"])
        node_path = normalized_node_path(envelope, node_hint)
        node_hash = stable_hash("/".join(node_path))
        node_materialization = self.ensure_context_node_path(
            node_path=node_path,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
        )
        batch_summary = extraction["batch_summary"]
        source_lineage_count = len(source_event_ids) if source_event_ids else len(envelope.get("messages", []))
        source_roles = sorted(
            {
                normalize_message_role(message.get("role"))
                for message in envelope.get("messages", [])
                if isinstance(message, dict) and normalize_message_role(message.get("role"))
            }
        )
        source_role_counts: Json = {}
        for message in envelope.get("messages", []):
            if not isinstance(message, dict):
                continue
            role = normalize_message_role(message.get("role"))
            if role:
                source_role_counts[role] = int(source_role_counts.get(role, 0)) + 1
        envelope_metadata = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
        metadata_role_counts = envelope_metadata.get("source_role_counts") if isinstance(envelope_metadata.get("source_role_counts"), dict) else {}
        if metadata_role_counts:
            source_role_counts = {}
            for role, count in metadata_role_counts.items():
                role_name = normalize_message_role(role)
                if not role_name:
                    continue
                try:
                    amount = max(0, int(count or 0))
                except (TypeError, ValueError):
                    continue
                if amount:
                    source_role_counts[role_name] = int(source_role_counts.get(role_name, 0)) + amount
            source_roles = sorted(source_role_counts)
        source_hook_type_values = [
            envelope.get("hook_type"),
            envelope_metadata.get("hook_type"),
            (hook or {}).get("hook_type") if isinstance(hook, dict) else "",
        ]
        if isinstance(envelope_metadata.get("source_hook_types"), list):
            source_hook_type_values.extend(envelope_metadata["source_hook_types"])
        source_hook_types = sorted({str(value).strip() for value in source_hook_type_values if str(value or "").strip()})
        source_hook_type_counts = {
            hook_type: source_lineage_count
            for hook_type in source_hook_types
            if hook_type
        }
        metadata_hook_type_counts = envelope_metadata.get("source_hook_type_counts") if isinstance(envelope_metadata.get("source_hook_type_counts"), dict) else {}
        if metadata_hook_type_counts:
            source_hook_type_counts = {}
            for hook_type, count in metadata_hook_type_counts.items():
                hook_name = str(hook_type or "").strip()
                if not hook_name:
                    continue
                try:
                    amount = max(0, int(count or 0))
                except (TypeError, ValueError):
                    continue
                if amount:
                    source_hook_type_counts[hook_name] = int(source_hook_type_counts.get(hook_name, 0)) + amount
        source_codex_event_values = [
            envelope.get("codex_event"),
            envelope_metadata.get("codex_event"),
            (hook or {}).get("codex_event") if isinstance(hook, dict) else "",
            (hook or {}).get("trigger") if isinstance(hook, dict) else "",
        ]
        if isinstance(envelope_metadata.get("source_codex_events"), list):
            source_codex_event_values.extend(envelope_metadata["source_codex_events"])
        source_codex_events = sorted({str(value).strip() for value in source_codex_event_values if str(value or "").strip()})
        source_codex_event_counts = {
            codex_event: source_lineage_count
            for codex_event in source_codex_events
            if codex_event
        }
        metadata_codex_event_counts = envelope_metadata.get("source_codex_event_counts") if isinstance(envelope_metadata.get("source_codex_event_counts"), dict) else {}
        if metadata_codex_event_counts:
            source_codex_event_counts = {}
            for codex_event, count in metadata_codex_event_counts.items():
                event_name = str(codex_event or "").strip()
                if not event_name:
                    continue
                try:
                    amount = max(0, int(count or 0))
                except (TypeError, ValueError):
                    continue
                if amount:
                    source_codex_event_counts[event_name] = int(source_codex_event_counts.get(event_name, 0)) + amount
        if not source_hook_types:
            source_hook_types = sorted(
                {
                    legacy_hook_type_from_codex_event(codex_event)
                    for codex_event in source_codex_events
                    if legacy_hook_type_from_codex_event(codex_event)
                }
            )
            source_hook_type_counts = {
                hook_type: source_lineage_count
                for hook_type in source_hook_types
                if hook_type
            }
        source_memory_selection_policy_counts: Json = {}
        metadata_selection_counts = (
            envelope_metadata.get("source_memory_selection_policy_counts")
            if isinstance(envelope_metadata.get("source_memory_selection_policy_counts"), dict)
            else {}
        )
        if metadata_selection_counts:
            for policy, count in metadata_selection_counts.items():
                policy_name = str(policy or "").strip()
                if not policy_name:
                    continue
                try:
                    amount = max(0, int(count or 0))
                except (TypeError, ValueError):
                    continue
                if amount:
                    source_memory_selection_policy_counts[policy_name] = int(
                        source_memory_selection_policy_counts.get(policy_name, 0)
                    ) + amount
        else:
            selection_values: list[str] = []
            if isinstance(envelope_metadata.get("source_memory_selection_policies"), list):
                selection_values.extend(envelope_metadata["source_memory_selection_policies"])
            selection = (
                envelope_metadata.get("codex_memory_selection")
                if isinstance(envelope_metadata.get("codex_memory_selection"), dict)
                else {}
            )
            if isinstance(selection.get("policies"), list):
                selection_values.extend(selection.get("policies", []))
            selection_policy = str(selection.get("policy") or "").strip()
            if selection_policy:
                selection_values.append(selection_policy)
            for policy_name in ordered_unique_any(selection_values):
                source_memory_selection_policy_counts[policy_name] = source_lineage_count
        assistant_text_parts = [
            str(message.get("content") or "")
            for message in envelope.get("messages", [])
            if isinstance(message, dict) and normalize_message_role(message.get("role")) == "assistant"
        ]
        user_text_parts = [
            str(message.get("content") or "")
            for message in envelope.get("messages", [])
            if isinstance(message, dict) and normalize_message_role(message.get("role")) == "user"
        ]
        assistant_lineage_text = "\n".join(assistant_text_parts) or envelope_metadata.get("text") or envelope.get("text")
        assistant_policies: list[str] = []
        assistant_feature_memory_only = feature_scope_excludes_outcome_evidence(assistant_lineage_text)
        if assistant_profile_fact_lineage_text(assistant_lineage_text):
            assistant_policies.append("selected_assistant_profile_fact")
        if (assistant_lineage_text or source_role_counts.get("assistant")) and not assistant_feature_memory_only:
            assistant_policies.append("selected_assistant_decision_outcome_only")
        if assistant_feature_memory_only and not assistant_policies:
            assistant_policies.append("selected_assistant_profile_fact")
        user_lineage_text = "\n".join(user_text_parts) or (envelope_metadata.get("text") if source_role_counts.get("user") else "")
        user_policies = ["selected_user_prompt"]
        if user_profile_fact_lineage_text(user_lineage_text):
            user_policies.append("selected_user_profile_fact")
        inferred_policy_by_role = {
            "assistant": assistant_policies,
            "tool": ["selected_tool_evidence_only"],
            "user": user_policies,
        }
        for role, count in source_role_counts.items():
            for policy_name in inferred_policy_by_role.get(role, []):
                if not policy_name or policy_name in source_memory_selection_policy_counts:
                    continue
                source_memory_selection_policy_counts[policy_name] = max(source_lineage_count, int(count or 0), 1)
        source_memory_selection_policies = sorted(source_memory_selection_policy_counts)
        source_profile_memory_classes = source_lineage.get("source_profile_memory_classes", [])
        if not isinstance(source_profile_memory_classes, list):
            source_profile_memory_classes = []
        source_profile_memory_kinds = source_lineage.get("source_profile_memory_kinds", [])
        if not isinstance(source_profile_memory_kinds, list):
            source_profile_memory_kinds = []
        source_memory_layers = source_lineage.get("source_memory_layers", [])
        if not isinstance(source_memory_layers, list):
            source_memory_layers = []
        source_memory_layer_counts = (
            source_lineage.get("source_memory_layer_counts")
            if isinstance(source_lineage.get("source_memory_layer_counts"), dict)
            else {}
        )
        source_memory_selection_retention: Json = {
            key: envelope_metadata.get(key)
            for key in [
                "source_memory_selection_lossy_count",
                "source_memory_selection_complete_count",
                "source_memory_selection_dropped_text_chars",
                "source_memory_selection_dropped_line_count",
                "source_memory_selection_retained_text_ratio_avg",
                "source_memory_selection_retained_line_ratio_avg",
            ]
            if envelope_metadata.get(key) not in (None, "", [], {})
        }
        selection = (
            envelope_metadata.get("codex_memory_selection")
            if isinstance(envelope_metadata.get("codex_memory_selection"), dict)
            else {}
        )
        if selection:
            if "source_memory_selection_lossy_count" not in source_memory_selection_retention:
                source_memory_selection_retention["source_memory_selection_lossy_count"] = (
                    1 if bool(selection.get("selection_lossy")) else 0
                )
            if "source_memory_selection_complete_count" not in source_memory_selection_retention:
                source_memory_selection_retention["source_memory_selection_complete_count"] = (
                    0 if bool(selection.get("selection_lossy")) else 1
                )
            for source_key, target_key in [
                ("dropped_text_chars", "source_memory_selection_dropped_text_chars"),
                ("dropped_line_count", "source_memory_selection_dropped_line_count"),
                ("retained_text_ratio", "source_memory_selection_retained_text_ratio_avg"),
                ("retained_line_ratio", "source_memory_selection_retained_line_ratio_avg"),
            ]:
                if target_key not in source_memory_selection_retention and selection.get(source_key) not in (None, ""):
                    source_memory_selection_retention[target_key] = selection.get(source_key)

        event_hashes: list[int] = list(source_event_ids) if derive_from_existing_events else []
        records_to_append: list[Json] = []
        event_records_to_append: list[Json] = []
        event_index_write_count = 0
        event_rows: list[tuple[int, Json, str, int]] = []
        event_records_by_hash: dict[int, Json] = {}
        segment_hash_by_position: dict[int, int] = {}
        segment_hashes_by_position: dict[int, list[int]] = {}
        for segment in extraction["segments"]:
            segment_hash = stable_hash(f"{batch_id_hash}:segment:{segment['topic']}:{segment['coordinate_tuples']}")
            for message_index in segment.get("message_indexes", []):
                if not isinstance(message_index, int):
                    continue
                segment_hashes_by_position.setdefault(message_index, []).append(segment_hash)
                segment_hash_by_position.setdefault(message_index, segment_hash)
        # Fields the CALLER supplied on the original ingest. When extraction commits it REWRITES the
        # `context_event` row for an id that already exists, building it from the extraction result
        # alone -- and latest-value compaction then serves that newer row. Anything the caller put on
        # the original row and the extractor does not reproduce is therefore silently dropped at read
        # time: an `identity_key` ingest became unfindable by keyed recall and stopped superseding,
        # and a `ttl_seconds` ingest became a record nothing would ever expire. They survived only
        # when extraction ran inside the ingest call, because `_stamp_ingest_fields` is scoped to it
        # -- so an in-process sync ingest looked correct while the same request through the gateway,
        # where finalize commits separately, lost them. Carry them across the rewrite.
        inherited_event_fields: dict[int, Json] = {}
        if derive_from_existing_events and source_event_ids:
            wanted_event_ids: set[int] = set()
            for source_event_id in source_event_ids:
                try:
                    wanted_event_ids.add(int(source_event_id))
                except (TypeError, ValueError):
                    continue
            # `prior_records` is the live view already loaded for prior context; only pay for a read
            # when the caller asked to skip that. Reading the LIVE view rather than the raw log is
            # deliberate -- a deleted row must not hand its identity key to its replacement.
            lookup_records = prior_records if prior_records else self.read_all()
            for prior_record in lookup_records:
                if str(prior_record.get("record_type") or "") != "context_event":
                    continue
                try:
                    prior_event_id = int(prior_record.get("event_id_hash"))
                except (TypeError, ValueError):
                    continue
                if prior_event_id not in wanted_event_ids:
                    continue
                carried = {
                    field: prior_record[field]
                    for field in CALLER_SUPPLIED_EVENT_FIELDS
                    if prior_record.get(field) is not None
                }
                if carried:
                    inherited_event_fields[prior_event_id] = carried
        if derive_from_existing_events:
            # The commit path re-emits one event PER MESSAGE, but the sync-accept path writes ONE
            # pending event for the whole envelope -- so a 10-message batch arrives here with a
            # single source_event_id. Skipping the messages past that id silently dropped nine of
            # ten: the re-emitted event reuses source_event_ids[0], and latest-value compaction
            # then replaced the full 359-char pending event with a 35-char one holding only the
            # first message. The text survived in the session summary, but events are what
            # retrieval returns, so those messages became unreachable.
            #
            # Messages beyond the supplied ids therefore get an id derived exactly as the
            # non-derived branch below does, which keeps the id stable for the same batch and
            # message position instead of discarding the message.
            for index, message in enumerate(envelope["messages"]):
                event_text = f"{message['role']}: {message['content']}"
                if index < len(source_event_ids):
                    event_id_hash = int(source_event_ids[index])
                else:
                    event_id_hash = stable_hash(f"{batch_id_hash}:event:{index}:{event_text}")
                    event_hashes.append(event_id_hash)
                event_rows.append((index, message, event_text, event_id_hash))
        else:
            for index, message in enumerate(envelope["messages"]):
                event_text = f"{message['role']}: {message['content']}"
                event_id_hash = stable_hash(f"{batch_id_hash}:event:{index}:{event_text}")
                event_hashes.append(event_id_hash)
                event_rows.append((index, message, event_text, event_id_hash))
        if event_rows:
            event_vectors = embeddings_for_texts([event_text for _index, _message, event_text, _event_id_hash in event_rows])
            for (_index, message, event_text, event_id_hash), event_vector in zip(event_rows, event_vectors):
                event_time_ms = int(envelope["ingestion_time_ms"])
                event_role = normalize_message_role(message.get("role"))
                original_event_role = str(message.get("original_role") or message.get("role") or "").strip().lower()
                event_type = context_event_type_for_message(message, str(extraction["event_type"] or ""))
                source_event_record = source_event_record_by_hash.get(int(event_id_hash), {})
                event_source_lineage = (
                    source_event_lineage_summary([source_event_record])
                    if source_event_record
                    else {}
                )
                event_hook_types = list(event_source_lineage.get("source_hook_types") or source_hook_types)
                event_hook_type_counts = dict(event_source_lineage.get("source_hook_type_counts") or {})
                if not event_hook_type_counts:
                    event_hook_type_counts = {hook_type: 1 for hook_type in event_hook_types if hook_type}
                event_codex_events = list(event_source_lineage.get("source_codex_events") or source_codex_events)
                event_codex_event_counts = dict(event_source_lineage.get("source_codex_event_counts") or {})
                if not event_codex_event_counts:
                    event_codex_event_counts = {codex_event: 1 for codex_event in event_codex_events if codex_event}
                event_profile_memory_classes = list(
                    event_source_lineage.get("source_profile_memory_classes") or source_profile_memory_classes
                )
                event_profile_memory_kinds = list(
                    event_source_lineage.get("source_profile_memory_kinds") or source_profile_memory_kinds
                )
                event_profile_memory_class = (
                    "codex_outcome"
                    if (
                        event_type in {"assistant_response", "assistant_decision", "tool_evidence", *CODEX_OUTCOME_ENTITY_TYPES}
                        or "codex_outcome" in {str(value or "").strip() for value in event_profile_memory_kinds}
                    )
                    else "memory_feature"
                    if (
                        event_type == "memory_feature"
                        or "memory_feature" in {str(value or "").strip() for value in event_profile_memory_classes}
                        or "memory_feature" in {str(value or "").strip() for value in event_profile_memory_kinds}
                    )
                    else ""
                )
                event_profile_memory_kind = (
                    "codex_outcome"
                    if event_profile_memory_class == "codex_outcome"
                    else "memory_feature"
                    if event_profile_memory_class == "memory_feature"
                    else ""
                )
                event_default_policy_counts = (
                    event_source_lineage.get("source_memory_selection_policy_counts")
                    if isinstance(event_source_lineage.get("source_memory_selection_policy_counts"), dict)
                    else source_memory_selection_policy_counts
                )
                event_memory_selection_policy_counts = memory_selection_policy_counts_for_message(
                    message,
                    default_counts=event_default_policy_counts,
                )
                event_memory_selection_policies = sorted(event_memory_selection_policy_counts)
                event_memory_selection_retention = memory_selection_retention_for_message(
                    message,
                    default_retention=source_memory_selection_retention,
                )
                event_record = {
                    "record_type": "context_event",
                    "event_id_hash": event_id_hash,
                    "event_time_ms": event_time_ms,
                    "event_time_key": f"{event_time_ms:020d}:{event_id_hash}",
                    "batch_id_hash": batch_id_hash,
                    "segment_hash": segment_hash_by_position.get(_index),
                    "parent_segment_hash": segment_hash_by_position.get(_index),
                    "parent_segment_hashes": segment_hashes_by_position.get(_index, []),
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": envelope["scope"],
                    "access_scope": envelope["scope"],
                    "text": event_text,
                    "summary_text": summarize_text(event_text),
                    "classification": extraction["classification"],
                    "event_type": event_type,
                    "batch_event_type": extraction["event_type"],
                    "status": "extraction_committed" if derive_from_existing_events else "observed",
                    "source_kind": envelope.get("kind", "message"),
                    "envelope": {
                        **envelope,
                        "messages": [message],
                    },
                    "internal_extraction": {
                        "mode": extraction["mode"],
                        "classification": extraction["classification"],
                        "event_type": event_type,
                        "batch_event_type": extraction["event_type"],
                        "batch_id_hash": batch_id_hash,
                    },
                    "prior_context": prior_context,
                    "agent_hook": hook,
                    **source_lineage,
                    "source_role": event_role,
                    "original_source_role": original_event_role,
                    "source_roles": [event_role] if event_role else [],
                    "source_role_counts": {event_role: 1} if event_role else {},
                    "source_hook_types": event_hook_types,
                    "source_hook_type_counts": event_hook_type_counts,
                    "source_codex_events": event_codex_events,
                    "source_codex_event_counts": event_codex_event_counts,
                    "source_memory_selection_policies": event_memory_selection_policies,
                    "source_memory_selection_policy_counts": event_memory_selection_policy_counts,
                    "profile_memory_class": event_profile_memory_class,
                    "profile_memory_kind": event_profile_memory_kind,
                    "source_profile_memory_classes": event_profile_memory_classes,
                    "source_profile_memory_kinds": event_profile_memory_kinds,
                    **event_memory_selection_retention,
                    "storage_options": envelope.get("storage_options", {}),
                    "updated_at_ms": envelope["ingestion_time_ms"],
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "extraction_phase": extraction_phase,
                    "final_session_boundary": final_session_boundary,
                    "extraction_context_event_ids": extraction_context_event_ids,
                    # Last, so the caller's own fields win over anything above that shares a name.
                    **inherited_event_fields.get(event_id_hash, {}),
                }
                event_memory_layer = candidate_memory_layer_name(event_record)
                if event_memory_layer:
                    event_record["memory_layer"] = event_memory_layer
                event_records_to_append.append(event_record)
                event_records_to_append.append(
                    compact_context_embedding_record({
                        "record_type": "context_embedding",
                        "embedding_type": "event_text",
                        "ref_type": "event",
                        "ref_hash": event_id_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dim": len(event_vector),
                        "model": embedding_model_name(),
                        "vector": event_vector,
                        "scope": envelope["scope"],
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "memory_layer": event_memory_layer,
                        "event_type": event_type,
                        "batch_event_type": extraction["event_type"],
                        "source_role": event_role,
                        "source_roles": [event_role] if event_role else [],
                        "source_role_counts": {event_role: 1} if event_role else {},
                        "source_hook_types": event_hook_types,
                        "source_hook_type_counts": event_hook_type_counts,
                        "source_codex_events": event_codex_events,
                        "source_codex_event_counts": event_codex_event_counts,
                        "source_memory_selection_policies": event_memory_selection_policies,
                        "source_memory_selection_policy_counts": event_memory_selection_policy_counts,
                        "profile_memory_class": event_profile_memory_class,
                        "profile_memory_kind": event_profile_memory_kind,
                        "source_profile_memory_classes": event_profile_memory_classes,
                        "source_profile_memory_kinds": event_profile_memory_kinds,
                        **event_memory_selection_retention,
                        "extraction_context_event_ids": extraction_context_event_ids,
                        "extraction_phase": extraction_phase,
                        "final_session_boundary": final_session_boundary,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    })
                )
        if event_records_to_append:
            event_records_by_hash = {
                int(event_record["event_id_hash"]): event_record
                for event_record in event_records_to_append
                if event_record.get("record_type") == "context_event" and event_record.get("event_id_hash") is not None
            }
            indexed_event_records: list[Json] = []
            for event_record in event_records_to_append:
                indexed_event_records.append(event_record)
                if event_record.get("record_type") != "context_event":
                    continue
                event_hash = event_record.get("event_id_hash")
                for index_name in candidate_index_terms(event_record, {}, {}):
                    event_index = context_index_posting_record(
                        index_name=index_name,
                        data_model="context_event",
                        ref_type="event",
                        ref_hashes=[event_hash],
                        batch_id_hash=batch_id_hash,
                        node_hash=node_hash,
                        scope=envelope["scope"],
                        updated_at_ms=envelope["ingestion_time_ms"],
                    )
                    event_index["access_scope"] = envelope["scope"]
                    event_index.pop("index_hash", None)
                    indexed_event_records.append(event_index)
                    event_index_write_count += 1
            event_records_to_append = indexed_event_records

        def source_lineage_for_event_ids(event_ids: list[int]) -> Json:
            scoped_events = [
                event_records_by_hash[int(event_id)]
                for event_id in event_ids
                if event_id is not None and int(event_id) in event_records_by_hash
            ]
            if scoped_events:
                return source_event_lineage_summary(scoped_events)
            return source_lineage

        def source_event_ids_for_entity(entity: Json) -> list[int]:
            refs = entity.get("source_refs") if isinstance(entity.get("source_refs"), list) else []
            resolved: list[int] = []
            source_event_id_set = {int(value) for value in source_event_ids if value is not None}
            for ref in refs:
                ref_text = str(ref or "").strip()
                if not ref_text:
                    continue
                try:
                    ref_value = int(ref_text)
                except (TypeError, ValueError):
                    continue
                if source_event_id_set and ref_value in source_event_id_set:
                    resolved.append(ref_value)
                    continue
                if not source_event_id_set and 0 <= ref_value < len(event_hashes):
                    resolved.append(int(event_hashes[ref_value]))
                    continue
                if source_event_id_set and 0 <= ref_value < len(source_event_ids):
                    resolved.append(int(source_event_ids[ref_value]))
            return ordered_unique_any(resolved) or list(source_event_ids or event_hashes)

        def memory_selection_policy_counts_for_entity(entity: Json, roles: list[str], role_counts: Json) -> Json:
            explicit_counts = (
                entity.get("source_memory_selection_policy_counts")
                if isinstance(entity.get("source_memory_selection_policy_counts"), dict)
                else {}
            )
            if explicit_counts:
                scoped: Json = {}
                for policy, count in explicit_counts.items():
                    policy_name = str(policy or "").strip()
                    if not policy_name:
                        continue
                    try:
                        amount = max(0, int(count or 0))
                    except (TypeError, ValueError):
                        continue
                    if amount:
                        scoped[policy_name] = int(scoped.get(policy_name, 0)) + amount
                if scoped:
                    return scoped
            allowed_by_role = {
                "assistant": {"selected_assistant_profile_fact", "selected_assistant_decision_outcome_only"},
                "tool": {"selected_tool_evidence_only"},
                "user": {"selected_user_prompt", "selected_user_profile_fact"},
            }
            allowed = {
                policy
                for role in roles
                for policy in allowed_by_role.get(normalize_message_role(role), set())
            }
            scoped = {
                policy: count
                for policy, count in source_memory_selection_policy_counts.items()
                if policy in allowed
            }
            if scoped:
                return dict(scoped)
            inferred: Json = {}
            for role in roles:
                role_name = normalize_message_role(role)
                for policy in sorted(allowed_by_role.get(role_name, set())):
                    inferred[policy] = max(1, int(role_counts.get(role_name, 0) or 0))
            return inferred or dict(source_memory_selection_policy_counts)

        profile_scope = {
            key: value
            for key, value in envelope["scope"].items()
            if key
            not in {
                "session_id",
                "session_hash",
                "scope_key",
                "tenant_hash",
                "user_hash",
                "_explicit_scope_keys",
            }
        }
        profile_scope["account_id"] = canonical_account_id(str(profile_scope.get("account_id") or ""))
        profile_scope["tenant_id"] = canonical_tenant_id(str(profile_scope.get("tenant_id") or ""))
        profile_scope["user_id"] = str(profile_scope.get("user_id") or "")
        profile_node_path: list[str] = []
        if profile_scope.get("tenant_id") and profile_scope.get("user_id"):
            profile_node_path = [
                f"tenant:{profile_scope.get('tenant_id')}",
                f"user:{profile_scope.get('user_id')}",
                "profile:long_term_memory",
            ]
            self.ensure_context_node_path(
                node_path=profile_node_path,
                scope=profile_scope,
                updated_at_ms=envelope["ingestion_time_ms"],
            )
        profile_node_hash = stable_hash("/".join(profile_node_path)) if profile_node_path else 0
        profile_promotion = profile_promotion_decision(profile_node_hash)
        profile_promotion_policy = str(profile_promotion["policy"])
        profile_promotion_importance_gate = bool(profile_promotion["importance_gate"])
        profile_promotion_scope_available = bool(profile_promotion["scope_available"])
        profile_promotion_blocker = str(profile_promotion["blocker"])
        source_session_id = str(envelope["scope"].get("session_id") or "")
        entity_hashes = []
        profile_entity_hashes = []
        entity_type_counts: Json = {}
        profile_promotion_summary: list[Json] = []
        profile_dirty_hashes: list[int] = []
        entity_index_write_count = 0
        summary_index_write_count = 0
        for entity in extraction["entities"]:
            entity_hash = stable_hash(
                f"{node_hash}:{entity['entity_type']}:{entity['entity_name']}"
            )
            previous_entity = self.find_latest_entity(
                node_hash=node_hash,
                entity_type=entity["entity_type"],
                entity_name=entity["entity_name"],
            )
            updated_entity = apply_entity_patches(previous_entity, entity)
            entity_source_roles = (
                ordered_unique_any([normalize_message_role(value) for value in entity.get("source_roles", []) if normalize_message_role(value)])
                if isinstance(entity.get("source_roles"), list)
                else source_roles
            )
            entity_source_role_counts: Json = {}
            if isinstance(entity.get("source_role_counts"), dict):
                for role, count in entity.get("source_role_counts", {}).items():
                    role_name = normalize_message_role(role)
                    if not role_name:
                        continue
                    try:
                        amount = max(0, int(count or 0))
                    except (TypeError, ValueError):
                        amount = 0
                    if amount > 0:
                        entity_source_role_counts[role_name] = amount
            if not entity_source_roles:
                entity_source_roles = source_roles
            if not entity_source_role_counts:
                entity_source_role_counts = source_role_counts
            entity_source_event_ids = source_event_ids_for_entity(entity)
            entity_source_lineage = source_lineage_for_event_ids(entity_source_event_ids)
            entity_source_hook_types = entity_source_lineage.get("source_hook_types", source_hook_types)
            entity_source_hook_type_counts = entity_source_lineage.get("source_hook_type_counts", source_hook_type_counts)
            entity_source_codex_events = entity_source_lineage.get("source_codex_events", source_codex_events)
            entity_source_codex_event_counts = entity_source_lineage.get("source_codex_event_counts", source_codex_event_counts)
            entity_source_memory_layers = entity_source_lineage.get("source_memory_layers", source_memory_layers)
            if not isinstance(entity_source_memory_layers, list):
                entity_source_memory_layers = source_memory_layers
            entity_source_memory_layer_counts = (
                entity_source_lineage.get("source_memory_layer_counts")
                if isinstance(entity_source_lineage.get("source_memory_layer_counts"), dict)
                else source_memory_layer_counts
            )
            entity_profile_memory_class = profile_memory_class_for_entity_type(updated_entity.get("entity_type"))
            entity_profile_memory_kind = profile_memory_kind_for_entity_type(updated_entity.get("entity_type"))
            entity_source_profile_memory_classes = ordered_unique_any(
                list(entity_source_lineage.get("source_profile_memory_classes", []))
                + list(source_profile_memory_classes)
                + ([entity_profile_memory_class] if entity_profile_memory_class else [])
            )
            entity_source_profile_memory_kinds = ordered_unique_any(
                list(entity_source_lineage.get("source_profile_memory_kinds", []))
                + list(source_profile_memory_kinds)
                + ([entity_profile_memory_kind] if entity_profile_memory_kind else [])
            )
            entity_source_memory_selection_policy_counts = memory_selection_policy_counts_for_entity(
                entity,
                entity_source_roles,
                entity_source_role_counts,
            )
            entity_source_memory_selection_policies = sorted(entity_source_memory_selection_policy_counts)
            entity_type_counts[updated_entity["entity_type"]] = int(entity_type_counts.get(updated_entity["entity_type"], 0)) + 1
            entity_hashes.append(entity_hash)
            session_entity_record = {
                "record_type": "context_entity",
                "entity_hash": entity_hash,
                "batch_id_hash": batch_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "scope": envelope["scope"],
                "access_scope": envelope["scope"],
                "entity_type": updated_entity["entity_type"],
                "entity_name": updated_entity["entity_name"],
                "profile_memory_class": entity_profile_memory_class,
                "profile_memory_kind": entity_profile_memory_kind,
                "state": updated_entity["state"],
                "previous_state": updated_entity.get("previous_state", ""),
                "confidence": updated_entity["confidence"],
                "operator": updated_entity["operator"],
                "source_refs": updated_entity["source_refs"],
                "source_event_ids": entity_source_event_ids,
                "source_roles": entity_source_roles,
                "source_role_counts": entity_source_role_counts,
                "source_hook_types": entity_source_hook_types,
                "source_hook_type_counts": entity_source_hook_type_counts,
                "source_codex_events": entity_source_codex_events,
                "source_codex_event_counts": entity_source_codex_event_counts,
                "source_memory_selection_policies": entity_source_memory_selection_policies,
                "source_memory_selection_policy_counts": entity_source_memory_selection_policy_counts,
                "source_memory_layers": entity_source_memory_layers,
                "source_memory_layer_counts": entity_source_memory_layer_counts,
                "source_profile_memory_classes": entity_source_profile_memory_classes,
                "source_profile_memory_kinds": entity_source_profile_memory_kinds,
                **source_memory_selection_retention,
                "extraction_context_event_ids": extraction_context_event_ids,
                "field_patches": updated_entity.get("field_patches", []),
                "patch_results": updated_entity.get("patch_results", []),
                "update_mode": updated_entity.get("update_mode", ""),
                "memory_scope": "session",
                "session_continuity": "same_session",
                "extraction_phase": extraction_phase,
                "final_session_boundary": final_session_boundary,
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
            session_entity_memory_layer = candidate_memory_layer_name(session_entity_record)
            if session_entity_memory_layer:
                session_entity_record["memory_layer"] = session_entity_memory_layer
            records_to_append.append(session_entity_record)
            for index_name in candidate_index_terms(session_entity_record, {}, {}):
                session_index = context_index_posting_record(
                    index_name=index_name,
                    data_model="context_entity",
                    ref_type="entity",
                    ref_hashes=[entity_hash],
                    batch_id_hash=batch_id_hash,
                    node_hash=node_hash,
                    scope=envelope["scope"],
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
                session_index["access_scope"] = envelope["scope"]
                session_index["memory_scope"] = session_entity_record["memory_scope"]
                session_index["session_continuity"] = session_entity_record["session_continuity"]
                session_index["profile_memory_class"] = session_entity_record.get("profile_memory_class", "")
                session_index["profile_memory_kind"] = session_entity_record.get("profile_memory_kind", "")
                session_index["extraction_phase"] = session_entity_record["extraction_phase"]
                session_index["final_session_boundary"] = session_entity_record["final_session_boundary"]
                session_index.pop("index_hash", None)
                records_to_append.append(session_index)
                entity_index_write_count += 1
            if updated_entity.get("patch_results"):
                records_to_append.append(
                    {
                        "record_type": "context_entity_update_audit",
                        "entity_hash": entity_hash,
                        "batch_id_hash": batch_id_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "entity_type": updated_entity["entity_type"],
                        "entity_name": updated_entity["entity_name"],
                        "previous_state": updated_entity.get("previous_state", ""),
                        "new_state": updated_entity["state"],
                        "patch_results": updated_entity.get("patch_results", []),
                        "llm_calls": 0,
                        "update_mode": "deterministic_eua",
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            entity_embedding_text = updated_entity["entity_type"] + " " + updated_entity["state"]
            entity_vector = embedding_for_text(entity_embedding_text)
            session_entity_embedding_record = compact_context_embedding_record({
                    "record_type": "context_embedding",
                    "embedding_type": "entity_state",
                    "ref_type": "entity",
                    "ref_hash": entity_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(entity_vector),
                    "model": embedding_model_name(),
                    "vector": entity_vector,
                    "scope": envelope["scope"],
                    "entity_type": updated_entity["entity_type"],
                    "entity_name": updated_entity["entity_name"],
                    "profile_memory_class": entity_profile_memory_class,
                    "profile_memory_kind": entity_profile_memory_kind,
                    "source_event_ids": entity_source_event_ids,
                    "source_roles": entity_source_roles,
                    "source_role_counts": entity_source_role_counts,
                    "source_hook_types": entity_source_hook_types,
                    "source_hook_type_counts": entity_source_hook_type_counts,
                    "source_codex_events": entity_source_codex_events,
                    "source_codex_event_counts": entity_source_codex_event_counts,
                    "source_memory_selection_policies": entity_source_memory_selection_policies,
                    "source_memory_selection_policy_counts": entity_source_memory_selection_policy_counts,
                    "source_memory_layers": entity_source_memory_layers,
                    "source_memory_layer_counts": entity_source_memory_layer_counts,
                    "source_profile_memory_classes": entity_source_profile_memory_classes,
                    "source_profile_memory_kinds": entity_source_profile_memory_kinds,
                    "memory_layer": session_entity_memory_layer,
                    **source_memory_selection_retention,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "extraction_phase": extraction_phase,
                    "final_session_boundary": final_session_boundary,
                    "extraction_context_event_ids": extraction_context_event_ids,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                })
            records_to_append.append(session_entity_embedding_record)
            if profile_node_hash and should_promote_session_entity_to_profile(updated_entity):
                profile_entity_hash = stable_hash(
                    f"{profile_node_hash}:{updated_entity['entity_type']}:{updated_entity['entity_name']}"
                )
                profile_shadow_fields: Json = {
                    "stale_or_superseded": True,
                    "version_state": "historical_superseded",
                    "current_state_policy": "historical_superseded_by_user_profile",
                    "profile_shadowed_by_ref_hash": profile_entity_hash,
                    "profile_shadowed_reason": "profile_entity_supersedes_session_entity",
                    "superseded_by_entity_hash": profile_entity_hash,
                    "profile_shadowed_at_ms": envelope["ingestion_time_ms"],
                }
                session_entity_record.update(profile_shadow_fields)
                session_entity_embedding_record.update(profile_shadow_fields)
                previous_profile_entity = self.find_latest_entity(
                    node_hash=profile_node_hash,
                    entity_type=updated_entity["entity_type"],
                    entity_name=updated_entity["entity_name"],
                )
                promoted_entity = apply_entity_patches(previous_profile_entity, updated_entity)
                previous_profile = previous_profile_entity or {}
                previous_profile_state = str(previous_profile.get("state") or "")
                promoted_state = str(promoted_entity.get("state") or "")
                cumulative_profile_entity_types = {
                    "assistant_decision",
                    "tool_evidence",
                    "memory_feature_profile",
                    *CODEX_OUTCOME_ENTITY_TYPES,
                }
                should_accumulate_profile_state = (
                    str(updated_entity.get("entity_type") or "") in cumulative_profile_entity_types
                )
                if (
                    should_accumulate_profile_state
                    and previous_profile_state
                    and previous_profile_state.lower() not in promoted_state.lower()
                ):
                    promoted_entity = {
                        **promoted_entity,
                        "state": summarize_text(previous_profile_state + " " + promoted_state, limit=320),
                        "previous_state": previous_profile_state,
                    }
                profile_source_session_ids = ordered_unique_any(
                    list(previous_profile.get("source_session_ids", []))
                    + ([source_session_id] if source_session_id else [])
                )
                profile_source_entity_hashes = ordered_unique_any(
                    list(previous_profile.get("source_entity_hashes", [])) + [entity_hash]
                )
                # A profile entity is re-written on every promotion, and these two lists carried its
                # whole history, so version k wrote k entries: O(list) bytes per add and O(list^2)
                # over the entity's life. Measured by walking the page segments over 300 ingests of
                # one subject: 261 versions of one profile, `source_event_ids` growing 1 -> 299 and
                # the record with it, 4 221 -> 16 687 bytes. `context_entity` was the largest record
                # type in the store at 15.6 KB per add, and most of it was this.
                #
                # The tail is kept, not dropped: `_profile_provenance_overflow` records carry every
                # id that ages out, one small append per promotion, so the full history stays in the
                # store and stops being re-written. What stays on the entity is the newest window
                # plus an EXACT count, because the count is what production actually reads --
                # nothing iterates a context_entity's `source_event_ids` for completeness (the
                # index-compaction tombstone builder, which would, has no production caller).
                profile_source_refs_all = ordered_unique_any(
                    list(previous_profile.get("source_refs", [])) + list(promoted_entity.get("source_refs", []))
                )
                profile_source_event_ids_all = ordered_unique_any(
                    list(previous_profile.get("source_event_ids", [])) + entity_source_event_ids
                )
                profile_source_refs = profile_source_refs_all[-_PROFILE_PROVENANCE_INLINE:]
                profile_source_event_ids = profile_source_event_ids_all[-_PROFILE_PROVENANCE_INLINE:]
                profile_source_ref_count = len(profile_source_refs_all)
                profile_source_event_id_count = len(profile_source_event_ids_all)
                profile_provenance_overflow = _profile_provenance_overflow(
                    previous_profile,
                    refs_all=profile_source_refs_all,
                    events_all=profile_source_event_ids_all,
                )
                profile_source_roles = ordered_unique_any(
                    list(previous_profile.get("source_roles", [])) + entity_source_roles
                )
                profile_source_role_counts: Json = dict(previous_profile.get("source_role_counts", {}))
                for role, count in entity_source_role_counts.items():
                    profile_source_role_counts[role] = int(profile_source_role_counts.get(role, 0)) + int(count)
                metadata_source_roles = (
                    envelope_metadata.get("source_roles")
                    if isinstance(envelope_metadata.get("source_roles"), list)
                    else []
                )
                metadata_has_llm_alias = any(
                    str(value or "").strip().lower()
                    in {"llm", "model", "assistant_response", "agent", "ai", "bot"}
                    for value in metadata_source_roles
                )
                if (
                    updated_entity.get("entity_type") == "assistant_decision"
                    and metadata_has_llm_alias
                    and int(source_role_counts.get("user", 0) or 0) > 0
                ):
                    profile_source_roles = ordered_unique_any(profile_source_roles + ["user"])
                    profile_source_role_counts["user"] = int(profile_source_role_counts.get("user", 0)) + int(
                        source_role_counts.get("user", 0) or 0
                    )
                profile_source_hook_types = ordered_unique_any(
                    list(previous_profile.get("source_hook_types", [])) + list(entity_source_hook_types)
                )
                profile_source_hook_type_counts: Json = dict(previous_profile.get("source_hook_type_counts", {}))
                for hook_type, count in entity_source_hook_type_counts.items():
                    profile_source_hook_type_counts[hook_type] = int(profile_source_hook_type_counts.get(hook_type, 0)) + int(count)
                profile_source_codex_events = ordered_unique_any(
                    list(previous_profile.get("source_codex_events", [])) + list(entity_source_codex_events)
                )
                profile_source_codex_event_counts: Json = dict(previous_profile.get("source_codex_event_counts", {}))
                for codex_event, count in entity_source_codex_event_counts.items():
                    profile_source_codex_event_counts[codex_event] = int(profile_source_codex_event_counts.get(codex_event, 0)) + int(count)
                profile_source_memory_selection_policies = ordered_unique_any(
                    list(previous_profile.get("source_memory_selection_policies", []))
                    + entity_source_memory_selection_policies
                )
                profile_source_memory_selection_policy_counts: Json = dict(
                    previous_profile.get("source_memory_selection_policy_counts", {})
                )
                for policy, count in entity_source_memory_selection_policy_counts.items():
                    profile_source_memory_selection_policy_counts[policy] = int(
                        profile_source_memory_selection_policy_counts.get(policy, 0)
                    ) + int(count)
                profile_source_memory_layers = ordered_unique_any(
                    list(previous_profile.get("source_memory_layers", []))
                    + list(entity_source_memory_layers)
                )
                profile_source_memory_layer_counts: Json = dict(
                    previous_profile.get("source_memory_layer_counts", {})
                    if isinstance(previous_profile.get("source_memory_layer_counts"), dict)
                    else {}
                )
                for layer, count in entity_source_memory_layer_counts.items():
                    layer_name = str(layer or "").strip()
                    if layer_name:
                        profile_source_memory_layer_counts[layer_name] = int(
                            profile_source_memory_layer_counts.get(layer_name, 0)
                        ) + int(count)
                if profile_promotion_scope_available:
                    profile_source_memory_selection_policies = ordered_unique_any(
                        profile_source_memory_selection_policies + ["selected_profile_current_state"]
                    )
                    profile_source_memory_selection_policy_counts["selected_profile_current_state"] = max(
                        1,
                        int(profile_source_memory_selection_policy_counts.get("selected_profile_current_state", 0) or 0),
                    )
                previous_profile_memory_classes = (
                    previous_profile.get("source_profile_memory_classes")
                    if isinstance(previous_profile.get("source_profile_memory_classes"), list)
                    else []
                )
                previous_profile_memory_kinds = (
                    previous_profile.get("source_profile_memory_kinds")
                    if isinstance(previous_profile.get("source_profile_memory_kinds"), list)
                    else []
                )
                profile_source_profile_memory_classes = ordered_unique_any(
                    list(previous_profile_memory_classes)
                    + list(entity_source_profile_memory_classes)
                    + ([profile_memory_class_for_entity_type(promoted_entity.get("entity_type"))] if promoted_entity.get("entity_type") else [])
                )
                profile_source_profile_memory_kinds = ordered_unique_any(
                    list(previous_profile_memory_kinds)
                    + list(entity_source_profile_memory_kinds)
                    + ([profile_memory_kind_for_entity_type(promoted_entity.get("entity_type"))] if promoted_entity.get("entity_type") else [])
                )
                profile_source_memory_selection_retention: Json = {}
                for key in [
                    "source_memory_selection_lossy_count",
                    "source_memory_selection_complete_count",
                    "source_memory_selection_dropped_text_chars",
                    "source_memory_selection_dropped_line_count",
                ]:
                    try:
                        profile_source_memory_selection_retention[key] = max(0, int(previous_profile.get(key) or 0)) + max(
                            0,
                            int(source_memory_selection_retention.get(key) or 0),
                        )
                    except (TypeError, ValueError):
                        pass
                for key in [
                    "source_memory_selection_retained_text_ratio_avg",
                    "source_memory_selection_retained_line_ratio_avg",
                ]:
                    try:
                        previous_ratio = float(previous_profile.get(key))
                    except (TypeError, ValueError):
                        previous_ratio = 1.0
                    try:
                        current_ratio = float(source_memory_selection_retention.get(key))
                    except (TypeError, ValueError):
                        current_ratio = 1.0
                    profile_source_memory_selection_retention[key] = round(min(previous_ratio, current_ratio), 6)
                previous_promotion_policy = str(previous_profile.get("profile_promotion_policy") or "").strip()
                previous_promotion_blocker = str(previous_profile.get("profile_promotion_blocker") or "").strip()
                profile_source_promotion_policies = ordered_unique_any(
                    list(previous_profile.get("source_profile_promotion_policies", []))
                    + ([previous_promotion_policy] if previous_promotion_policy else [])
                    + ([profile_promotion_policy] if profile_promotion_policy else [])
                )
                profile_source_promotion_blockers = ordered_unique_any(
                    list(previous_profile.get("source_profile_promotion_blockers", []))
                    + ([previous_promotion_blocker] if previous_promotion_blocker else [])
                    + ([profile_promotion_blocker] if profile_promotion_blocker else [])
                )
                profile_source_memory_scopes = ordered_unique_any(
                    list(previous_profile.get("source_memory_scopes", []))
                    + [previous_profile.get("memory_scope"), "session", "user_profile"]
                )
                profile_source_session_continuities = ordered_unique_any(
                    list(previous_profile.get("source_session_continuities", []))
                    + [previous_profile.get("session_continuity"), "same_session", "cross_session"]
                )
                previous_profile_revision = int(previous_profile.get("profile_revision") or 0)
                profile_revision = previous_profile_revision + 1
                previous_profile_updated_at_ms = int(previous_profile.get("updated_at_ms") or 0)
                profile_memory_class = profile_memory_class_for_entity_type(promoted_entity.get("entity_type"))
                profile_memory_kind = profile_memory_kind_for_entity_type(promoted_entity.get("entity_type"))
                profile_entity_hashes.append(profile_entity_hash)
                profile_promotion_summary.append(
                    {
                        "profile_entity_hash": profile_entity_hash,
                        "session_entity_hash": entity_hash,
                        "entity_type": promoted_entity["entity_type"],
                        "entity_name": promoted_entity["entity_name"],
                        "profile_memory_class": profile_memory_class,
                        "profile_memory_kind": profile_memory_kind,
                        "source_session_ids": profile_source_session_ids,
                        "source_entity_count": len(profile_source_entity_hashes),
                        "source_ref_count": profile_source_ref_count,
                        "source_event_count": profile_source_event_id_count,
                        "source_roles": profile_source_roles,
                        "source_role_counts": profile_source_role_counts,
                        "source_hook_types": profile_source_hook_types,
                        "source_hook_type_counts": profile_source_hook_type_counts,
                        "source_codex_events": profile_source_codex_events,
                        "source_codex_event_counts": profile_source_codex_event_counts,
                        "source_memory_selection_policies": profile_source_memory_selection_policies,
                        "source_memory_selection_policy_counts": profile_source_memory_selection_policy_counts,
                        "source_profile_memory_classes": profile_source_profile_memory_classes,
                        "source_profile_memory_kinds": profile_source_profile_memory_kinds,
                        **profile_source_memory_selection_retention,
                        "source_profile_promotion_policies": profile_source_promotion_policies,
                        "source_profile_promotion_blockers": profile_source_promotion_blockers,
                        "source_memory_scopes": profile_source_memory_scopes,
                        "source_session_continuities": profile_source_session_continuities,
                        "profile_revision": profile_revision,
                    }
                )
                profile_entity_record = {
                    "record_type": "context_entity",
                    "entity_hash": profile_entity_hash,
                    "batch_id_hash": batch_id_hash,
                    "node_hash": profile_node_hash,
                    "node_path": profile_node_path,
                    "scope": profile_scope,
                    "access_scope": profile_scope,
                    "entity_type": promoted_entity["entity_type"],
                    "entity_name": promoted_entity["entity_name"],
                    "profile_memory_class": profile_memory_class,
                    "profile_memory_kind": profile_memory_kind,
                    "state": promoted_entity["state"],
                    "previous_state": promoted_entity.get("previous_state", ""),
                    "confidence": promoted_entity["confidence"],
                    "operator": promoted_entity["operator"],
                    "source_refs": profile_source_refs,
                    "source_event_ids": profile_source_event_ids,
                    # The lists above are the newest window; these are the true totals. A reader
                    # that wants "how many events back this profile" must read the count, not the
                    # length of the window.
                    "source_ref_count": profile_source_ref_count,
                    "source_event_count": profile_source_event_id_count,
                    "source_provenance_windowed": True,
                    "source_session_ids": profile_source_session_ids,
                    "source_entity_hashes": profile_source_entity_hashes,
                    "source_roles": profile_source_roles,
                    "source_role_counts": profile_source_role_counts,
                    "source_hook_types": profile_source_hook_types,
                    "source_hook_type_counts": profile_source_hook_type_counts,
                    "source_codex_events": profile_source_codex_events,
                    "source_codex_event_counts": profile_source_codex_event_counts,
                    "source_memory_selection_policies": profile_source_memory_selection_policies,
                    "source_memory_selection_policy_counts": profile_source_memory_selection_policy_counts,
                    "source_memory_layers": profile_source_memory_layers,
                    "source_memory_layer_counts": profile_source_memory_layer_counts,
                    "source_profile_memory_classes": profile_source_profile_memory_classes,
                    "source_profile_memory_kinds": profile_source_profile_memory_kinds,
                    **profile_source_memory_selection_retention,
                    "source_profile_promotion_policies": profile_source_promotion_policies,
                    "source_profile_promotion_blockers": profile_source_promotion_blockers,
                    "source_memory_scopes": profile_source_memory_scopes,
                    "source_session_continuities": profile_source_session_continuities,
                    "source_batch_id_hash": batch_id_hash,
                    "extraction_context_event_ids": extraction_context_event_ids,
                    "field_patches": promoted_entity.get("field_patches", []),
                    "patch_results": promoted_entity.get("patch_results", []),
                    "update_mode": promoted_entity.get("update_mode", ""),
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "promoted_from_memory_scope": "session",
                    "profile_promotion_policy": profile_promotion_policy,
                    "profile_promotion_importance_gate": profile_promotion_importance_gate,
                    "profile_promotion_blocker": profile_promotion_blocker,
                    "profile_revision": profile_revision,
                    "profile_entity_current": True,
                    "supersedes_session_entity_hash": entity_hash,
                    "supersedes_session_entity_hashes": profile_source_entity_hashes,
                    "previous_profile_revision": previous_profile_revision,
                    "previous_profile_updated_at_ms": previous_profile_updated_at_ms,
                    "extraction_phase": extraction_phase,
                    "final_session_boundary": final_session_boundary,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
                profile_entity_memory_layer = candidate_memory_layer_name(profile_entity_record)
                if profile_entity_memory_layer:
                    profile_entity_record["memory_layer"] = profile_entity_memory_layer
                records_to_append.append(profile_entity_record)
                if profile_provenance_overflow:
                    # One small append carrying only what just aged out of the window. The history
                    # is preserved in the store; what stops happening is re-writing all of it on
                    # every promotion.
                    records_to_append.append(
                        {
                            "record_type": "context_entity_provenance",
                            "entity_hash": profile_entity_hash,
                            "node_hash": profile_node_hash,
                            "batch_id_hash": batch_id_hash,
                            "scope": profile_scope,
                            "access_scope": profile_scope,
                            "source_refs": profile_provenance_overflow["source_refs"],
                            "source_event_ids": profile_provenance_overflow["source_event_ids"],
                            "updated_at_ms": now_ms(),
                        }
                    )
                profile_entity_embedding_text = promoted_entity["entity_type"] + " " + promoted_entity["state"]
                profile_entity_vector = embedding_for_text(profile_entity_embedding_text)
                profile_entity_embedding_record = compact_context_embedding_record({
                        "record_type": "context_embedding",
                        "embedding_type": "profile_entity_state",
                        "ref_type": "entity",
                        "ref_hash": profile_entity_hash,
                        "node_hash": profile_node_hash,
                        "node_path": profile_node_path,
                        "dim": len(profile_entity_vector),
                        "model": embedding_model_name(),
                        "vector": profile_entity_vector,
                        "scope": profile_scope,
                        "entity_type": promoted_entity["entity_type"],
                        "entity_name": promoted_entity["entity_name"],
                        "profile_memory_class": profile_memory_class,
                        "profile_memory_kind": profile_memory_kind,
                        "source_event_ids": profile_source_event_ids,
                        "source_session_ids": profile_source_session_ids,
                        "source_entity_hashes": profile_source_entity_hashes,
                        "source_roles": profile_source_roles,
                        "source_role_counts": profile_source_role_counts,
                        "source_hook_types": profile_source_hook_types,
                        "source_hook_type_counts": profile_source_hook_type_counts,
                        "source_codex_events": profile_source_codex_events,
                        "source_codex_event_counts": profile_source_codex_event_counts,
                        "source_memory_selection_policies": profile_source_memory_selection_policies,
                        "source_memory_selection_policy_counts": profile_source_memory_selection_policy_counts,
                        "source_memory_layers": profile_source_memory_layers,
                        "source_memory_layer_counts": profile_source_memory_layer_counts,
                        "source_profile_memory_classes": profile_source_profile_memory_classes,
                        "source_profile_memory_kinds": profile_source_profile_memory_kinds,
                        "memory_layer": profile_entity_memory_layer,
                        **profile_source_memory_selection_retention,
                        "source_profile_promotion_policies": profile_source_promotion_policies,
                        "source_profile_promotion_blockers": profile_source_promotion_blockers,
                        "source_memory_scopes": profile_source_memory_scopes,
                        "source_session_continuities": profile_source_session_continuities,
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "promoted_from_memory_scope": "session",
                        "profile_promotion_policy": profile_promotion_policy,
                        "profile_promotion_importance_gate": profile_promotion_importance_gate,
                        "profile_promotion_blocker": profile_promotion_blocker,
                        "profile_revision": profile_revision,
                        "profile_entity_current": True,
                        "profile_source_session_count": len(profile_source_session_ids),
                        "profile_source_entity_count": len(profile_source_entity_hashes),
                        "profile_source_event_count": len(profile_source_event_ids),
                        "extraction_phase": extraction_phase,
                        "final_session_boundary": final_session_boundary,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    })
                for field in [
                    "source_event_ids",
                    "source_session_ids",
                    "source_entity_hashes",
                    "source_roles",
                    "source_role_counts",
                    "source_hook_types",
                    "source_hook_type_counts",
                    "source_codex_events",
                    "source_codex_event_counts",
                    "source_memory_selection_policies",
                    "source_memory_selection_policy_counts",
                    "source_profile_promotion_policies",
                    "source_profile_promotion_blockers",
                    "source_memory_scopes",
                    "source_session_continuities",
                    "supersedes_session_entity_hash",
                    "supersedes_session_entity_hashes",
                    "previous_profile_revision",
                    "previous_profile_updated_at_ms",
                    "extraction_context_event_ids",
                    "extraction_phase",
                    "final_session_boundary",
                    "profile_source_session_count",
                    "profile_source_entity_count",
                    "profile_source_event_count",
                ]:
                    profile_entity_embedding_record.pop(field, None)
                records_to_append.append(profile_entity_embedding_record)
                for index_name in candidate_index_terms(profile_entity_record, {}, {}):
                    profile_index = context_index_posting_record(
                        index_name=index_name,
                        data_model="context_profile_entity",
                        ref_type="entity",
                        ref_hashes=[profile_entity_hash],
                        batch_id_hash=batch_id_hash,
                        node_hash=profile_node_hash,
                        scope=profile_scope,
                        updated_at_ms=envelope["ingestion_time_ms"],
                    )
                    profile_index["access_scope"] = profile_scope
                    profile_index["memory_scope"] = profile_entity_record["memory_scope"]
                    profile_index["session_continuity"] = profile_entity_record["session_continuity"]
                    profile_index["profile_memory_class"] = profile_entity_record.get("profile_memory_class", "")
                    profile_index["profile_memory_kind"] = profile_entity_record.get("profile_memory_kind", "")
                    profile_index["profile_entity_current"] = profile_entity_record.get("profile_entity_current", False)
                    profile_index["profile_revision"] = profile_entity_record.get("profile_revision", 0)
                    profile_index["promoted_from_memory_scope"] = profile_entity_record.get("promoted_from_memory_scope", "")
                    profile_index["source_session_ids"] = profile_entity_record.get("source_session_ids", [])
                    profile_index["source_entity_hashes"] = profile_entity_record.get("source_entity_hashes", [])
                    profile_index["extraction_phase"] = profile_entity_record["extraction_phase"]
                    profile_index["final_session_boundary"] = profile_entity_record["final_session_boundary"]
                    profile_index.pop("index_hash", None)
                    records_to_append.append(profile_index)
                    entity_index_write_count += 1
                _profile_dirty_hashes, profile_dirty_records = self.node_summary_dirty_records(
                    node_path=profile_node_path,
                    scope=profile_scope,
                    updated_at_ms=envelope["ingestion_time_ms"],
                    source_ref_type="entity",
                    source_hash_field="source_entity_hash",
                    source_hash=profile_entity_hash,
                    dirty_reason="profile_entity_promoted",
                    propagate_depth=0,
                    source_lineage=profile_entity_record,
                )
                profile_dirty_hashes.extend(_profile_dirty_hashes)
                records_to_append.extend(profile_dirty_records)

        def segment_source_roles_and_type(segment: Json) -> tuple[list[str], Json, Json, str, list[str], list[str], str, str]:
            segment_roles: list[str] = []
            for message_index in segment.get("message_indexes", []):
                if not isinstance(message_index, int) or message_index < 0 or message_index >= len(envelope["messages"]):
                    continue
                role = normalize_message_role(envelope["messages"][message_index].get("role"))
                if role:
                    segment_roles.append(role)
            role_counts: Json = {}
            for role in segment_roles:
                role_counts[role] = int(role_counts.get(role, 0)) + 1
            ordered_roles = ordered_unique_any(segment_roles) or source_roles
            segment_policy_counts = memory_selection_policy_counts_for_entity({}, ordered_roles, role_counts or source_role_counts)
            policy_set = {str(policy or "").strip() for policy in segment_policy_counts if str(policy or "").strip()}
            topic_text = " ".join([str(segment.get("topic") or ""), str(segment.get("summary_text") or ""), str(segment.get("text") or "")]).lower()
            has_memory_feature = (
                "memory_feature" in {str(kind or "").strip().lower() for kind in source_profile_memory_kinds}
                or "memory_feature" in {str(cls or "").strip().lower() for cls in source_profile_memory_classes}
                or (
                    "selected_assistant_profile_fact" in policy_set
                    and "selected_assistant_decision_outcome_only" not in policy_set
                    and feature_scope_excludes_outcome_evidence(topic_text)
                )
            )
            has_tool_evidence = "tool" in ordered_roles or "selected_tool_evidence_only" in policy_set or "tool evidence" in topic_text
            has_assistant_outcome = (
                ("assistant" in ordered_roles and not has_memory_feature)
                or "selected_assistant_decision_outcome_only" in policy_set
                or "assistant decision" in topic_text
                or "assistant response" in topic_text
            )
            if has_memory_feature:
                event_type = "memory_feature"
            elif has_tool_evidence:
                event_type = "tool_evidence"
            elif has_assistant_outcome:
                event_type = "assistant_response"
            elif "user" in ordered_roles:
                event_type = "user_prompt"
            else:
                event_type = str(extraction.get("event_type") or "session")
            if has_memory_feature:
                profile_classes = ["memory_feature"]
                profile_kinds = ["memory_feature"]
                profile_class = "memory_feature"
                profile_kind = "memory_feature"
            elif has_tool_evidence or has_assistant_outcome:
                profile_classes = ["codex_outcome"]
                profile_kinds = ["codex_outcome"]
                profile_class = "codex_outcome"
                profile_kind = "codex_outcome"
            else:
                profile_classes = []
                profile_kinds = []
                profile_class = ""
                profile_kind = ""
            return ordered_roles, role_counts or source_role_counts, segment_policy_counts, event_type, profile_classes, profile_kinds, profile_class, profile_kind

        # --- Exact-value entity capture (general, gated) --------------------------------
        # Mint a scoped, embedded fact entity for any assistant message stating an exact
        # value (number+unit, count, hex hash, ALL_CAPS/dotted flag) that the topic segmenter
        # and entity extractor skipped, so the value token stays a selectable retrieval
        # candidate and its state text is NOT truncated before the value. Mirrors
        # session_entity_record; gated by MATRIXARK_VALUE_ENTITY_CAPTURE (default on).
        if _value_entity_capture_enabled():
            _seen_value_texts: set = set()
            for _vindex, _vmessage, _vevent_text, _vevent_hash in event_rows:
                if normalize_message_role(_vmessage.get("role")) != "assistant":
                    continue
                _vcontent = str(_vmessage.get("content") or "").strip()
                if not _line_has_exact_value(_vcontent):
                    continue
                _vnorm = _vcontent.lower()
                if _vnorm in _seen_value_texts:
                    continue
                # Mint a dedicated clean-state value entity even when the line is also folded
                # into another (often non-surfacing / truncated) extraction entity: the point
                # is to give exact-value facts their own selectable, untruncated candidate.
                _seen_value_texts.add(_vnorm)
                _value_state = f"assistant: {_vcontent}"
                _value_entity_hash = stable_hash(f"{batch_id_hash}:value_entity:{_vindex}:{_vevent_text}")
                _value_entity_record = {
                    "record_type": "context_entity",
                    "entity_hash": _value_entity_hash,
                    "batch_id_hash": batch_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": envelope["scope"],
                    "access_scope": envelope["scope"],
                    "entity_type": "exact_value_fact",
                    "entity_name": _value_entity_name(_vcontent),
                    "state": _value_state,
                    "previous_state": "",
                    "confidence": 1.0,
                    "operator": "observed",
                    "source_refs": [],
                    "source_event_ids": [_vevent_hash],
                    "source_roles": ["assistant"],
                    "source_role_counts": {"assistant": 1},
                    "extraction_context_event_ids": extraction_context_event_ids,
                    "field_patches": [],
                    "patch_results": [],
                    "update_mode": "value_capture",
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "extraction_phase": extraction_phase,
                    "final_session_boundary": final_session_boundary,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
                records_to_append.append(_value_entity_record)
                for _value_index_name in candidate_index_terms(_value_entity_record, {}, {}):
                    _value_index = context_index_posting_record(
                        index_name=_value_index_name,
                        data_model="context_entity",
                        ref_type="entity",
                        ref_hashes=[_value_entity_hash],
                        batch_id_hash=batch_id_hash,
                        node_hash=node_hash,
                        scope=envelope["scope"],
                        updated_at_ms=envelope["ingestion_time_ms"],
                    )
                    _value_index["access_scope"] = envelope["scope"]
                    _value_index["memory_scope"] = "session"
                    _value_index["session_continuity"] = "same_session"
                    _value_index.pop("index_hash", None)
                    records_to_append.append(_value_index)
                    entity_index_write_count += 1
                _value_vector = embedding_for_text("exact_value_fact " + _value_state)
                records_to_append.append(
                    compact_context_embedding_record({
                        "record_type": "context_embedding",
                        "embedding_type": "entity_state",
                        "ref_type": "entity",
                        "ref_hash": _value_entity_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dim": len(_value_vector),
                        "model": embedding_model_name(),
                        "vector": _value_vector,
                        "scope": envelope["scope"],
                        "source_event_ids": [_vevent_hash],
                        "source_roles": ["assistant"],
                        "source_role_counts": {"assistant": 1},
                    })
                )

        segment_hashes = []
        for segment in extraction["segments"]:
            segment_hash = stable_hash(f"{batch_id_hash}:segment:{segment['topic']}:{segment['coordinate_tuples']}")
            segment_hashes.append(segment_hash)
            (
                segment_source_roles,
                segment_source_role_counts,
                segment_source_memory_selection_policy_counts,
                segment_event_type,
                segment_profile_memory_classes,
                segment_profile_memory_kinds,
                segment_profile_memory_class,
                segment_profile_memory_kind,
            ) = segment_source_roles_and_type(segment)
            segment_source_memory_selection_policies = sorted(segment_source_memory_selection_policy_counts)
            segment_source_event_ids = [event_hashes[index] for index in segment["message_indexes"] if index < len(event_hashes)]
            segment_source_lineage = source_lineage_for_event_ids(segment_source_event_ids)
            segment_source_hook_types = segment_source_lineage.get("source_hook_types", source_hook_types)
            segment_source_hook_type_counts = segment_source_lineage.get("source_hook_type_counts", source_hook_type_counts)
            segment_source_codex_events = segment_source_lineage.get("source_codex_events", source_codex_events)
            segment_source_codex_event_counts = segment_source_lineage.get("source_codex_event_counts", source_codex_event_counts)
            segment_source_memory_layers = segment_source_lineage.get("source_memory_layers", source_memory_layers)
            if not isinstance(segment_source_memory_layers, list):
                segment_source_memory_layers = source_memory_layers
            segment_source_memory_layer_counts = (
                segment_source_lineage.get("source_memory_layer_counts")
                if isinstance(segment_source_lineage.get("source_memory_layer_counts"), dict)
                else source_memory_layer_counts
            )
            segment_record = {
                "record_type": "context_segment",
                "segment_hash": segment_hash,
                "batch_id_hash": batch_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "scope": envelope["scope"],
                **({"access_scope": envelope["scope"]} if _segment_access_scope_enabled() else {}),
                "topic": segment["topic"],
                "coordinate_tuples": segment["coordinate_tuples"],
                "message_indexes": segment["message_indexes"],
                "source_event_ids": segment_source_event_ids,
                "source_record_type": "context_event",
                "segment_origin": segment.get("segment_origin") or segment.get("detected_by") or "derived_from_events",
                "derived_from_context_events": bool(segment.get("derived_from_context_events", True)),
                "source_roles": segment_source_roles,
                "source_role_counts": segment_source_role_counts,
                "segment_source_roles": segment_source_roles,
                "segment_source_role_counts": segment_source_role_counts,
                "event_type": segment_event_type,
                "source_hook_types": segment_source_hook_types,
                "source_hook_type_counts": segment_source_hook_type_counts,
                "source_codex_events": segment_source_codex_events,
                "source_codex_event_counts": segment_source_codex_event_counts,
                "source_memory_selection_policies": segment_source_memory_selection_policies,
                "source_memory_selection_policy_counts": segment_source_memory_selection_policy_counts,
                "source_memory_layers": segment_source_memory_layers,
                "source_memory_layer_counts": segment_source_memory_layer_counts,
                **source_memory_selection_retention,
                "source_memory_scopes": ["session"],
                "source_session_continuities": ["same_session"],
                "source_extraction_phases": [extraction_phase],
                "source_profile_memory_classes": segment_profile_memory_classes,
                "source_profile_memory_kinds": segment_profile_memory_kinds,
                "profile_memory_class": segment_profile_memory_class,
                "profile_memory_kind": segment_profile_memory_kind,
                "saliency_score": segment["saliency_score"],
                "summary_text": segment["summary_text"],
                "text": segment["text"],
                "non_contiguous": segment["non_contiguous"],
                "memory_scope": "session",
                "session_continuity": "same_session",
                "extraction_phase": extraction_phase,
                "final_session_boundary": final_session_boundary,
                "extraction_context_event_ids": extraction_context_event_ids,
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
            segment_memory_layer = candidate_memory_layer_name(segment_record)
            if segment_memory_layer:
                segment_record["memory_layer"] = segment_memory_layer
            records_to_append.append(segment_record)
            for index_name in candidate_index_terms(segment_record, {}, {}):
                segment_index = context_index_posting_record(
                    index_name=index_name,
                    data_model="context_segment",
                    ref_type="segment",
                    ref_hashes=[segment_hash],
                    batch_id_hash=batch_id_hash,
                    node_hash=node_hash,
                    scope=envelope["scope"],
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
                segment_index["access_scope"] = envelope["scope"]
                segment_index["memory_scope"] = segment_record["memory_scope"]
                segment_index["session_continuity"] = segment_record["session_continuity"]
                segment_index["profile_memory_class"] = segment_record.get("profile_memory_class", "")
                segment_index["profile_memory_kind"] = segment_record.get("profile_memory_kind", "")
                segment_index["extraction_phase"] = segment_record["extraction_phase"]
                segment_index["final_session_boundary"] = segment_record["final_session_boundary"]
                segment_index.pop("index_hash", None)
                records_to_append.append(segment_index)
                entity_index_write_count += 1
            segment_embedding_text = segment["topic"] + " " + segment["summary_text"]
            segment_vector = embedding_for_text(segment_embedding_text)
            records_to_append.append(
                compact_context_embedding_record({
                    "record_type": "context_embedding",
                    "embedding_type": "segment_text",
                    "ref_type": "segment",
                    "ref_hash": segment_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(segment_vector),
                    "model": embedding_model_name(),
                    "vector": segment_vector,
                    "scope": envelope["scope"],
                    "topic": segment["topic"],
                    "source_event_ids": segment_source_event_ids,
                    "source_roles": segment_source_roles,
                    "source_role_counts": segment_source_role_counts,
                    "event_type": segment_event_type,
                    "source_hook_types": segment_source_hook_types,
                    "source_hook_type_counts": segment_source_hook_type_counts,
                    "source_codex_events": segment_source_codex_events,
                    "source_codex_event_counts": segment_source_codex_event_counts,
                    "source_memory_selection_policies": segment_source_memory_selection_policies,
                    "source_memory_selection_policy_counts": segment_source_memory_selection_policy_counts,
                    **source_memory_selection_retention,
                    "source_memory_scopes": ["session"],
                    "source_session_continuities": ["same_session"],
                    "source_extraction_phases": [extraction_phase],
                    "source_profile_memory_classes": segment_profile_memory_classes,
                    "source_profile_memory_kinds": segment_profile_memory_kinds,
                    "profile_memory_class": segment_profile_memory_class,
                    "profile_memory_kind": segment_profile_memory_kind,
                    "memory_layer": segment_memory_layer,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "extraction_phase": extraction_phase,
                    "final_session_boundary": final_session_boundary,
                    "extraction_context_event_ids": extraction_context_event_ids,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                })
            )

        batch_profile_memory_class_values = {
            str(value or "").strip().lower() for value in source_profile_memory_classes
        }
        batch_profile_memory_kind_values = {
            str(value or "").strip().lower() for value in source_profile_memory_kinds
        }
        batch_feature_only = bool(feature_scope_excludes_outcome_evidence(batch_text))
        batch_has_memory_feature = (
            "memory_feature" in batch_profile_memory_class_values
            or "memory_feature" in batch_profile_memory_kind_values
            or int(entity_type_counts.get("memory_feature_profile") or 0) > 0
            or batch_feature_only
        )
        batch_has_codex_outcome = (
            not batch_feature_only
            and (
                "codex_outcome" in batch_profile_memory_class_values
                or "codex_outcome" in batch_profile_memory_kind_values
                or any(normalize_message_role(role) in {"assistant", "tool"} for role in source_roles)
                or any(
                    str(policy or "").strip()
                    in {"selected_assistant_decision_outcome_only", "selected_tool_evidence_only"}
                    for policy in source_memory_selection_policies
                )
                or any(
                    str(event or "").strip()
                    in {
                        "PreviousAssistantBackfill",
                        "PreviousToolOutputBackfill",
                        "Stop",
                        "SubagentStop",
                        "PostCompact",
                        "PostToolUse",
                        "PreToolUse",
                        "PermissionRequest",
                    }
                    for event in source_codex_events
                )
            )
        )
        if batch_has_codex_outcome:
            batch_profile_memory_class = "codex_outcome"
            batch_profile_memory_kind = "codex_outcome"
        elif batch_has_memory_feature:
            batch_profile_memory_class = "memory_feature"
            batch_profile_memory_kind = "memory_feature"
        else:
            batch_profile_memory_class = ""
            batch_profile_memory_kind = ""
        batch_source_profile_memory_classes = ordered_unique_any(
            list(source_profile_memory_classes)
            + (["codex_outcome"] if batch_has_codex_outcome else [])
            + (["memory_feature"] if batch_has_memory_feature else [])
            + ([batch_profile_memory_class] if batch_profile_memory_class else [])
        )
        batch_source_profile_memory_kinds = ordered_unique_any(
            list(source_profile_memory_kinds)
            + (["codex_outcome"] if batch_has_codex_outcome else [])
            + (["memory_feature"] if batch_has_memory_feature else [])
            + ([batch_profile_memory_kind] if batch_profile_memory_kind else [])
        )

        summary_hash = stable_hash(f"batch_summary:{batch_id_hash}")
        batch_summary_record = {
            "record_type": "context_summary",
            "summary_type": "batch_l0",
            "summary_hash": summary_hash,
            "batch_id_hash": batch_id_hash,
            "node_hash": node_hash,
            "node_path": node_path,
            "summary_text": batch_summary,
            "source_entity_hashes": entity_hashes,
            "source_segment_hashes": segment_hashes,
            "source_event_ids": event_hashes,
            "source_roles": source_roles,
            "source_role_counts": source_role_counts,
            "source_hook_types": source_hook_types,
            "source_hook_type_counts": source_hook_type_counts,
            "source_codex_events": source_codex_events,
            "source_codex_event_counts": source_codex_event_counts,
            "source_memory_selection_policies": source_memory_selection_policies,
            "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
            "source_memory_layers": source_memory_layers,
            "source_memory_layer_counts": source_memory_layer_counts,
            "source_profile_memory_classes": batch_source_profile_memory_classes,
            "source_profile_memory_kinds": batch_source_profile_memory_kinds,
            "profile_memory_class": batch_profile_memory_class,
            "profile_memory_kind": batch_profile_memory_kind,
            **source_memory_selection_retention,
            "source_memory_scopes": ["session"],
            "source_session_continuities": ["same_session"],
            "source_extraction_phases": [extraction_phase],
            "memory_scope": "session",
            "session_continuity": "same_session",
            "scope": envelope["scope"],
            "extraction_phase": extraction_phase,
            "final_session_boundary": final_session_boundary,
            "extraction_context_event_ids": extraction_context_event_ids,
            "updated_at_ms": envelope["ingestion_time_ms"],
        }
        batch_summary_memory_layer = candidate_memory_layer_name(batch_summary_record)
        if batch_summary_memory_layer:
            batch_summary_record["memory_layer"] = batch_summary_memory_layer
        records_to_append.append(batch_summary_record)
        for index_name in candidate_index_terms(batch_summary_record, {}, {}):
            summary_index = context_index_posting_record(
                index_name=index_name,
                data_model="context_summary",
                ref_type="summary",
                ref_hashes=[summary_hash],
                batch_id_hash=batch_id_hash,
                node_hash=node_hash,
                scope=envelope["scope"],
                updated_at_ms=envelope["ingestion_time_ms"],
            )
            summary_index["access_scope"] = envelope["scope"]
            summary_index["memory_scope"] = batch_summary_record["memory_scope"]
            summary_index["session_continuity"] = batch_summary_record["session_continuity"]
            summary_index["profile_memory_class"] = batch_profile_memory_class
            summary_index["profile_memory_kind"] = batch_profile_memory_kind
            summary_index["extraction_phase"] = batch_summary_record["extraction_phase"]
            summary_index["final_session_boundary"] = batch_summary_record["final_session_boundary"]
            summary_index.pop("index_hash", None)
            records_to_append.append(summary_index)
            summary_index_write_count += 1
        summary_embedding_text = " ".join(node_path + [batch_summary])
        summary_vector = embedding_for_text(summary_embedding_text)
        records_to_append.append(
            compact_context_embedding_record({
                "record_type": "context_embedding",
                "embedding_type": "batch_l0",
                "ref_type": "summary",
                "ref_hash": summary_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": len(summary_vector),
                "model": embedding_model_name(),
                "vector": summary_vector,
                "scope": envelope["scope"],
                "source_event_count": len(event_rows),
                "source_segment_count": len(segment_hashes),
                "source_entity_count": len(entity_hashes),
                "source_entity_hashes": entity_hashes,
                "source_segment_hashes": segment_hashes,
                "source_event_ids": event_hashes,
                "source_roles": source_roles,
                "source_role_counts": source_role_counts,
                "source_hook_types": source_hook_types,
                "source_hook_type_counts": source_hook_type_counts,
                "source_codex_events": source_codex_events,
                "source_codex_event_counts": source_codex_event_counts,
                "source_memory_selection_policies": source_memory_selection_policies,
                "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
                "source_memory_layers": source_memory_layers,
                "source_memory_layer_counts": source_memory_layer_counts,
                "source_profile_memory_classes": batch_source_profile_memory_classes,
                "source_profile_memory_kinds": batch_source_profile_memory_kinds,
                "profile_memory_class": batch_profile_memory_class,
                "profile_memory_kind": batch_profile_memory_kind,
                "memory_layer": batch_summary_memory_layer,
                **source_memory_selection_retention,
                "source_memory_scopes": ["session"],
                "source_session_continuities": ["same_session"],
                "source_extraction_phases": [extraction_phase],
                "memory_scope": "session",
                "session_continuity": "same_session",
                "extraction_phase": extraction_phase,
                "final_session_boundary": final_session_boundary,
                "updated_at_ms": envelope["ingestion_time_ms"],
            })
        )
        secondary_index_budget = new_secondary_index_budget()
        batch_index_terms = take_secondary_index_terms(list(extraction["indexes"]), secondary_index_budget)
        for index_name in batch_index_terms:
            records_to_append.append(
                context_index_posting_record(
                    index_name=index_name,
                    data_model="context_batch_commit",
                    batch_id_hash=batch_id_hash,
                    node_hash=node_hash,
                    scope=envelope["scope"],
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
            )
        records_to_append.append(
            {
                "record_type": "context_extraction_audit",
                "batch_id_hash": batch_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "schema": extraction["schema"],
                "message_count": extraction["message_count"],
                "token_count_estimate": extraction["token_count_estimate"],
                "outputs": {
                    "events": len(event_rows),
                    "source_events": len(event_hashes),
                    "entities": len(entity_hashes),
                    "profile_entities": len(profile_entity_hashes),
                    "profile_promotion_summary": profile_promotion_summary[:16],
                    "profile_promotion_policy": profile_promotion_policy,
                    "profile_promotion_importance_gate": profile_promotion_importance_gate,
                    "profile_promotion_scope_available": profile_promotion_scope_available,
                    "profile_promotion_blocker": profile_promotion_blocker,
                    "entity_type_counts": entity_type_counts,
                    "source_role_counts": source_role_counts,
                    "source_hook_type_counts": source_hook_type_counts,
                    "source_codex_event_counts": source_codex_event_counts,
                    "source_memory_selection_policies": source_memory_selection_policies,
                    "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
                    **source_memory_selection_retention,
                    "segments": len(segment_hashes),
                    "summaries": 1,
                    "indexes": len(batch_index_terms) + event_index_write_count + entity_index_write_count + summary_index_write_count,
                    "event_indexes": event_index_write_count,
                    "entity_indexes": entity_index_write_count,
                    "summary_indexes": summary_index_write_count,
                    **secondary_index_budget_summary(secondary_index_budget),
                },
                "mode": extraction["mode"],
                "derive_from_existing_events": derive_from_existing_events,
                "source_event_ids": event_hashes,
                "source_roles": source_roles,
                "source_role_counts": source_role_counts,
                "source_hook_types": source_hook_types,
                "source_hook_type_counts": source_hook_type_counts,
                "source_codex_events": source_codex_events,
                "source_codex_event_counts": source_codex_event_counts,
                "source_memory_selection_policies": source_memory_selection_policies,
                "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
                "source_profile_memory_classes": source_profile_memory_classes,
                "source_profile_memory_kinds": source_profile_memory_kinds,
                **source_memory_selection_retention,
                "extraction_context_event_ids": extraction_context_event_ids,
                "extraction_phase": extraction_phase,
                "final_session_boundary": final_session_boundary,
                "agent_hook": hook,
                "created_at_ms": now_ms(),
            }
        )
        dirty_hashes, dirty_records = self.node_summary_dirty_records(
            node_path=node_path,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
            source_ref_type="batch",
            source_hash_field="source_batch_hash",
            source_hash=batch_id_hash,
            dirty_reason="new_event",
            source_lineage={
                "source_roles": source_roles,
                "source_role_counts": source_role_counts,
                "source_hook_types": source_hook_types,
                "source_hook_type_counts": source_hook_type_counts,
                "source_codex_events": source_codex_events,
                "source_codex_event_counts": source_codex_event_counts,
                "source_memory_selection_policies": source_memory_selection_policies,
                "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
                "source_profile_memory_classes": source_profile_memory_classes,
                "source_profile_memory_kinds": source_profile_memory_kinds,
                **source_memory_selection_retention,
                "source_memory_scopes": ["session"],
                "source_session_continuities": ["same_session"],
                "source_extraction_phases": [extraction_phase],
                "source_final_session_boundary_count": 1 if final_session_boundary else 0,
                "memory_scope": "session",
                "session_continuity": "same_session",
                "extraction_phase": extraction_phase,
                "final_session_boundary": final_session_boundary,
            },
        )
        records_to_append.extend(dirty_records)
        records_to_append.extend(event_records_to_append)
        self.append_many(records_to_append)
        summary_refresh = {
            "status": "dirty_marked",
            "dirty_hashes": ordered_unique_any(dirty_hashes + profile_dirty_hashes),
            "session_dirty_hashes": dirty_hashes,
            "profile_dirty_hashes": profile_dirty_hashes,
            "profile_summary_refresh_required": bool(profile_dirty_hashes),
            "refresh_result": None,
            "async_required": True,
            "write_path": "coalesced_with_batch_extract",
        }
        return {
            "status": "accepted",
            "mode": extraction["mode"],
            "segment_provider": extraction.get("segment_provider", {}),
            "classification": extraction["classification"],
            "batch_id_hash": batch_id_hash,
            "node_hash": node_hash,
            "storage_options": envelope.get("storage_options", {}),
            "storage_route": envelope.get("storage_route", {}),
            "embedding_model": embedding_model_name(),
            "embedding_execution_mode": embedding_execution_mode_name(),
            "embedding_fallback_used": embedding_fallback_used(),
            "message_count": extraction["message_count"],
            "token_count_estimate": extraction["token_count_estimate"],
            "events_written": len(event_rows),
            "source_event_count": len(event_hashes),
            "extraction_context_event_count": len(extraction_context_event_ids),
            "raw_events_duplicated": not derive_from_existing_events,
            "entities_written": len(entity_hashes),
            "profile_entities_written": len(profile_entity_hashes),
            "profile_promotion_policy": profile_promotion_policy,
            "profile_promotion_importance_gate": profile_promotion_importance_gate,
            "profile_promotion_scope_available": profile_promotion_scope_available,
            "profile_promotion_blocker": profile_promotion_blocker,
            "entity_type_counts": entity_type_counts,
            "source_role_counts": source_role_counts,
            "source_hook_type_counts": source_hook_type_counts,
            "source_codex_event_counts": source_codex_event_counts,
            "source_memory_selection_policies": source_memory_selection_policies,
            "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
            "source_profile_memory_classes": source_profile_memory_classes,
            "source_profile_memory_kinds": source_profile_memory_kinds,
            **source_memory_selection_retention,
            "profile_promotion_summary": profile_promotion_summary[:16],
            "segments_written": len(segment_hashes),
            "summary_hash": summary_hash,
            "summary_refresh": summary_refresh,
            "node_materialization": node_materialization,
            "indexes_written": len(batch_index_terms) + event_index_write_count + entity_index_write_count + summary_index_write_count,
            "event_indexes_written": event_index_write_count,
            "entity_indexes_written": entity_index_write_count,
            "summary_indexes_written": summary_index_write_count,
            **secondary_index_budget_summary(secondary_index_budget),
            "one_pass": True,
            "threshold_messages": threshold,
            "extraction_phase": extraction_phase,
            "final_session_boundary": final_session_boundary,
        }

    def write_time_compression(
        self,
        *,
        scope: Json,
        node_hash: int,
        node_path: list[str],
        source_start_ms: int,
        source_end_ms: int,
        compressed_time_ms: int,
        max_source_events: int = 32,
        min_confidence: float = 0.0,
        min_importance: float = 0.0,
        summary: str = "",
    ) -> Json:
        if source_start_ms > source_end_ms:
            raise MatrixArkError("source_start_ms must be <= source_end_ms")
        if max_source_events <= 0:
            raise MatrixArkError("max_source_events must be positive")
        records = self.read_all()
        debug_by_ref = {
            record.get("ref_hash"): record.get("debug_payload", {})
            for record in records
            if record.get("record_type") == "context_debug_record" and record.get("ref_type") == "event"
        }
        source_events = []
        event_times: dict[int, int] = {}
        event_scopes: dict[int, Json] = {}
        for record in records:
            if record.get("record_type") != "context_event":
                continue
            if int(record.get("node_hash") or 0) != node_hash:
                continue
            event_hash = int(record.get("event_id_hash") or 0)
            debug_payload = debug_by_ref.get(event_hash, {}) if event_hash else {}
            envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else debug_payload.get("envelope", {})
            if not isinstance(envelope, dict):
                envelope = {}
            event_scope = envelope.get("scope", scope_from_serving_record(record))
            if not scope_matches(event_scope, scope):
                continue
            event_time = int(envelope.get("ingestion_time_ms") or record.get("updated_at_ms") or 0)
            if event_time < source_start_ms or event_time > source_end_ms:
                continue
            extraction = debug_payload.get("internal_extraction", {}) if isinstance(debug_payload.get("internal_extraction"), dict) else {}
            confidence = float(extraction.get("confidence", record.get("confidence", 1.0)) or 1.0)
            metadata = envelope.get("metadata", {}) if isinstance(envelope.get("metadata"), dict) else {}
            importance = float(metadata.get("importance", record.get("importance", 1.0)) or 1.0)
            if confidence < min_confidence or importance < min_importance:
                continue
            source_events.append(record)
            event_times[event_hash] = event_time
            event_scopes[event_hash] = event_scope
        source_events.sort(key=lambda record: event_times.get(int(record.get("event_id_hash") or 0), 0))
        selected = source_events[:max_source_events]
        if not selected:
            raise MatrixArkError("no source events matched compression window")
        truncated = len(source_events) > len(selected)
        source_event_ids = [int(record["event_id_hash"]) for record in selected]
        compression_scope = event_scopes.get(int(selected[0].get("event_id_hash") or 0), scope)
        if not summary:
            snippets = [summarize_text(str(record.get("text", "")), limit=180) for record in selected[:5]]
            suffix = " plus additional source events" if truncated else ""
            summary = (
                f"Temporal compression window [{source_start_ms}, {source_end_ms}] contains "
                f"{len(selected)} selected events{suffix}. " + " | ".join(snippets)
            )
        compression_id_hash = stable_hash(f"compress:{scope}:{node_hash}:{source_start_ms}:{source_end_ms}:{source_event_ids}")
        source_lineage = source_event_lineage_summary(selected)
        record = {
            "record_type": "context_compression_event",
            "compression_id_hash": compression_id_hash,
            "node_hash": node_hash,
            "node_path": node_path,
            "scope": compression_scope,
            "source_start_ms": source_start_ms,
            "source_end_ms": source_end_ms,
            "compressed_time_ms": compressed_time_ms,
            "summary_text": summarize_text(summary, limit=1200),
            "source_event_ids": source_event_ids,
            "source_event_count": len(selected),
            **source_lineage,
            "truncated_source_events": truncated,
            "operator": "TIME_COMPRESS",
            "updated_at_ms": compressed_time_ms,
        }
        self.append(record)
        compression_vector = embedding_for_text(record["summary_text"])
        self.append(
            {
                "record_type": "context_embedding",
                "embedding_type": "compression_summary",
                "ref_type": "compression",
                "ref_hash": compression_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": len(compression_vector),
                "model": embedding_model_name(),
                "vector": compression_vector,
                "scope": compression_scope,
                "operator": record.get("operator") or "TIME_COMPRESS",
                "source_roles": record.get("source_roles", []),
                "source_role_counts": record.get("source_role_counts", {}),
                "source_hook_types": record.get("source_hook_types", []),
                "source_hook_type_counts": record.get("source_hook_type_counts", {}),
                "source_codex_events": record.get("source_codex_events", []),
                "source_codex_event_counts": record.get("source_codex_event_counts", {}),
                "source_memory_selection_policies": record.get("source_memory_selection_policies", []),
                "source_memory_selection_policy_counts": record.get("source_memory_selection_policy_counts", {}),
                "source_memory_scopes": record.get("source_memory_scopes", []),
                "source_session_continuities": record.get("source_session_continuities", []),
                "source_extraction_phases": record.get("source_extraction_phases", []),
                "memory_scope": record.get("memory_scope", ""),
                "session_continuity": record.get("session_continuity", ""),
                "updated_at_ms": compressed_time_ms,
            }
        )
        self.append_many(compression_context_index_records(record))
        return record

    def query_time_compressions(
        self, *, scope: Json, node_hashes: set[int], start_time_ms: int, end_time_ms: int, limit: int = 16
    ) -> list[Json]:
        matches = []
        for record in self.read_all():
            if record.get("record_type") != "context_compression_event":
                continue
            if node_hashes and int(record.get("node_hash") or 0) not in node_hashes:
                continue
            if not scope_matches(candidate_access_scope(record), scope):
                continue
            if int(record.get("source_end_ms") or 0) >= start_time_ms and int(record.get("source_start_ms") or 0) <= end_time_ms:
                matches.append(record)
        matches.sort(key=lambda record: (int(record.get("source_end_ms") or 0), int(record.get("compressed_time_ms") or 0)), reverse=True)
        return matches[:limit]

    def append_recall_reinforcement_markers(
        self,
        *,
        context_pack_id: str,
        selected_refs: list[Json],
        reinforced_at_ms: int,
        protect_ms: int = TIME_COMPRESSION_REINFORCEMENT_PROTECT_MS,
    ) -> Json:
        protect_ms = max(0, int(protect_ms))
        protected_until_ms = reinforced_at_ms + protect_ms if protect_ms else 0
        records: list[Json] = []
        seen: set[tuple[int, int]] = set()
        for ref in selected_refs:
            source_ids: list[int] = []
            if ref.get("ref_type") == "event" and ref.get("ref_hash") is not None:
                try:
                    source_ids.append(int(ref.get("ref_hash")))
                except (TypeError, ValueError):
                    pass
            for event_id in ref.get("source_event_ids", []) or []:
                try:
                    source_ids.append(int(event_id))
                except (TypeError, ValueError):
                    pass
            for event_id in source_ids:
                try:
                    node_hash = int(ref.get("node_hash") or 0)
                except (TypeError, ValueError):
                    node_hash = 0
                key = (event_id, node_hash)
                if key in seen:
                    continue
                seen.add(key)
                records.append(
                    {
                        "record_type": "context_recall_reinforcement",
                        "event_id_hash": event_id,
                        "node_hash": node_hash,
                        "node_path": ref.get("node_path", []),
                        "context_pack_id": context_pack_id,
                        "source_ref_type": ref.get("ref_type"),
                        "source_ref_hash": ref.get("ref_hash"),
                        "scope": ref.get("scope", {}),
                        "reinforced_at_ms": reinforced_at_ms,
                        "protected_until_ms": protected_until_ms,
                        "reason": "selected_in_context_pack",
                        "created_at_ms": reinforced_at_ms,
                        "updated_at_ms": reinforced_at_ms,
                    }
                )
        if records:
            self.append_many(records)
        return {
            "reinforced_event_count": len(records),
            "protect_ms": protect_ms,
            "protected_until_ms": protected_until_ms,
        }

    def deadline_fallback_pack(
        self,
        *,
        query: str,
        scope: Json,
        question_type: str,
        max_context_tokens: int,
        local_budget: Json,
        deadline_ms: int,
        elapsed_ms: float,
        records: list[Json],
        reason: str,
        budget_source: str = "matrixark_default_max_context_tokens",
        retrieval_scope: Json | None = None,
        source_role_budget_tokens: Json | None = None,
        source_role_budget_mode: str = "",
        memory_layer_budget_tokens: Json | None = None,
        memory_layer_budget_mode: str = "",
        memory_selection_policy_budget_tokens: Json | None = None,
        memory_selection_policy_budget_mode: str = "",
        extraction_phase_budget_tokens: Json | None = None,
        extraction_phase_budget_mode: str = "",
    ) -> Json:
        selected = []
        used_context_tokens = 0
        local_tokens = int(local_budget.get("token_estimate", 0))
        safety_margin_tokens = int(local_budget.get("safety_margin_tokens", 0))
        remote_budget = max(0, max_context_tokens - local_tokens - safety_margin_tokens)
        for record in reversed(records):
            record_type = record.get("record_type")
            metadata = record.get("metadata", {}) if isinstance(record.get("metadata"), dict) else {}
            record_scope = candidate_access_scope(record)
            if record_type not in {"context_summary", "context_entity", "context_event", "context_segment"}:
                continue
            if not scope_matches(record_scope, scope):
                continue
            if record_type == "context_summary":
                text = str(record.get("summary_text", ""))
                ref_type = "summary"
                ref_hash = record.get("summary_hash") or record.get("node_hash")
            elif record_type == "context_entity":
                text = f"{record.get('entity_type', '')}: {record.get('entity_name', '')} = {record.get('state', '')}"
                ref_type = "entity"
                ref_hash = record.get("entity_hash")
            elif record_type == "context_segment":
                text = f"{record.get('topic', '')}: {record.get('summary_text', '')}"
                ref_type = "segment"
                ref_hash = record.get("segment_hash")
            else:
                text = str(record.get("summary_text") or record.get("text") or "")
                ref_type = "event"
                ref_hash = record.get("event_id_hash")
            if not text or ref_hash is None:
                continue
            item_tokens = token_count(text)
            if used_context_tokens + item_tokens > remote_budget:
                continue
            ref = {
                "ref_type": ref_type,
                "ref_hash": ref_hash,
                "node_hash": record.get("node_hash"),
                "node_path": record.get("node_path", []),
                "score": 0.0,
                "recall_path": "deadline_fallback_recent_context",
                "updated_at_ms": record.get("updated_at_ms", record.get("envelope", {}).get("ingestion_time_ms", now_ms())),
                "text": clip_context_text(text),
                "token_estimate": item_tokens,
            }
            for field in [
                "memory_scope",
                "session_continuity",
                "extraction_phase",
                "event_type",
                "classification",
                "entity_type",
                "entity_name",
                "summary_type",
                "profile_current_state_representative",
                "current_state_policy",
                "current_state_source_session_count",
                "current_state_source_entity_count",
                "source_final_session_boundary_count",
            ]:
                value = record.get(field, metadata.get(field))
                if value not in (None, "", [], {}):
                    ref[field] = value
            if bool(record.get("final_session_boundary") or metadata.get("final_session_boundary")):
                ref["final_session_boundary"] = True
            for field in [
                "source_roles",
                "source_role_counts",
                "source_hook_types",
                "source_hook_type_counts",
                "source_codex_events",
                "source_codex_event_counts",
                "source_memory_scopes",
                "source_session_continuities",
                "source_extraction_phases",
                "source_memory_selection_policies",
                "source_memory_selection_policy_counts",
                "source_session_ids",
                "source_event_ids",
                "source_entity_hashes",
                "extraction_context_event_ids",
            ]:
                value = record.get(field, metadata.get(field))
                if isinstance(value, list) and value:
                    ref[field] = value[:16]
                elif isinstance(value, dict) and value:
                    ref[field] = {
                        str(key): int(count)
                        for key, count in value.items()
                        if str(key or "").strip() and isinstance(count, int) and count
                    }
            selected.append(ref)
            used_context_tokens += item_tokens
            if len(selected) >= 8:
                break
        fallback_dropped_over_budget: Json = {}
        selected, removed_pending_tokens = suppress_extracted_represented_pending_events(selected, fallback_dropped_over_budget)
        removed_pending_count = int(fallback_dropped_over_budget.get("pending_async_event_superseded_by_extracted_refs") or 0)
        if removed_pending_tokens:
            used_context_tokens = max(0, used_context_tokens - removed_pending_tokens)
        context_pack_id = str(stable_hash(f"deadline:{query}:{selected}:{now_ms()}"))
        serving_selected = compact_context_pack_refs(selected, include_debug=False)
        memory_layer_budget = selected_ref_layer_budget(selected)
        source_role_budget_policy = budget_control_policy_summary(
            selected_budget=memory_layer_budget,
            budget_tokens=source_role_budget_tokens,
            mode=source_role_budget_mode,
            remote_budget_tokens=remote_budget,
            bucket_name="by_source_role",
            semantics="independent_per_role_caps_under_global_remote_budget",
            normalize_keys=normalize_message_role,
        )
        memory_layer_budget_policy = budget_control_policy_summary(
            selected_budget=memory_layer_budget,
            budget_tokens=memory_layer_budget_tokens,
            mode=memory_layer_budget_mode,
            remote_budget_tokens=remote_budget,
            bucket_name="by_memory_layer",
            semantics="independent_per_layer_caps_under_global_remote_budget",
            question_type=question_type,
        )
        memory_selection_policy_budget_policy = budget_control_policy_summary(
            selected_budget=memory_layer_budget,
            budget_tokens=memory_selection_policy_budget_tokens,
            mode=memory_selection_policy_budget_mode,
            remote_budget_tokens=remote_budget,
            bucket_name="by_memory_selection_policy",
            semantics="independent_per_memory_selection_policy_caps_under_global_remote_budget",
        )
        extraction_phase_budget_policy = budget_control_policy_summary(
            selected_budget=memory_layer_budget,
            budget_tokens=extraction_phase_budget_tokens,
            mode=extraction_phase_budget_mode,
            remote_budget_tokens=remote_budget,
            bucket_name="by_extraction_phase",
            semantics="independent_per_extraction_phase_caps_under_global_remote_budget",
        )
        serving_memory_layer_budget_value = serving_memory_layer_budget(memory_layer_budget)
        async_readiness_scope = retrieval_scope if isinstance(retrieval_scope, dict) else {**scope, "_session_scope": "prefer"}
        async_pipeline_readiness = async_pipeline_retrieval_readiness(records, async_readiness_scope)
        quality_warnings = [
            f"retrieval_deadline_exceeded:{reason}",
            *async_pipeline_readiness.get("freshness_warnings", []),
        ]
        if removed_pending_count:
            quality_warnings.append(f"pending_async_event_superseded_by_extracted_refs:{removed_pending_count}")
        pack = {
            "context_pack_id": context_pack_id,
            "context_sources_order": ["local_context", "matrixark_remote_context"],
            "local_context_refs": local_context_refs_for_pack(local_budget),
            "selected_refs": serving_selected,
            "remote_context_refs": serving_selected,
            "selected_ref_counts": selected_context_class_counts(selected),
            "layer_scores": [],
            "question_type": question_type,
            "packing_policy": f"deadline_fallback:{question_type}",
            "query_embedding_model": embedding_model_name(),
            "embedding_execution_mode": embedding_execution_mode_name(),
            "embedding_fallback_used": embedding_fallback_used(),
            "recall_policy": {
                "deadline_ms": deadline_ms,
                "elapsed_ms": elapsed_ms,
                "partial_context_pack": True,
                "fallback_reason": reason,
                "memory_layer_budget": serving_memory_layer_budget_value,
                "source_role_budget": source_role_budget_policy,
                "source_role_budget_policy": source_role_budget_policy,
                "memory_layer_budget_policy": memory_layer_budget_policy,
                "memory_selection_policy_budget_policy": memory_selection_policy_budget_policy,
                "extraction_phase_budget_policy": extraction_phase_budget_policy,
                "async_pipeline_readiness": async_pipeline_readiness,
                "session_continuity": {
                    "mode": "fallback_recent_context",
                    "policy": "deadline fallback preserves same-session/cross-session/profile lineage while staying within the remaining remote budget",
                    "same_session_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "same_session"),
                    "cross_session_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "cross_session"),
                    "entity_bridge_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "cross_session" and item.get("ref_type") == "entity"),
                },
            },
            "primary_candidate_count": 0,
            "auxiliary_candidate_count": 0,
            "used_context_tokens": used_context_tokens,
            "used_remote_context_tokens": used_context_tokens,
            "used_local_context_tokens": local_tokens,
            "total_prompt_context_tokens": used_context_tokens + local_tokens,
                "remote_context_budget_tokens": remote_budget,
                "requested_max_context_tokens": max_context_tokens,
                "retrieval_metrics": {
                    "memory_layer_budget": serving_memory_layer_budget_value,
                    "source_role_budget": source_role_budget_policy,
                    "memory_layer_budget_policy": memory_layer_budget_policy,
            "memory_selection_policy_budget": memory_selection_policy_budget_policy,
            "extraction_phase_budget": extraction_phase_budget_policy,
            "async_pipeline_readiness": async_pipeline_readiness,
                    "requested_max_context_tokens": max_context_tokens,
                    "used_local_context_tokens": local_tokens,
                    "used_remote_context_tokens": used_context_tokens,
                    "total_prompt_context_tokens": used_context_tokens + local_tokens,
                    "remote_context_budget_tokens": remote_budget,
                    "partial_context_pack": True,
                    "fallback_reason": reason,
                    "source": "deadline_fallback_pack",
                },
                "local_context_safety_margin_tokens": safety_margin_tokens,
                "budget_source": budget_source,
            "local_context_policy": {
                "mode": "shared_budget_dedupe",
                "local_context_count": len(local_budget["items"]),
                "local_context_tokens": local_tokens,
                "local_context_token_source": local_budget.get("token_source", "estimated_from_local_context"),
                "safety_margin_tokens": safety_margin_tokens,
                "safety_margin_source": local_budget.get("safety_margin_source", "matrixark_default_5_percent_capped"),
                "dedupe_remote_against_local": True,
                "remote_is_additive_only_within_remaining_budget": True,
                },
                "dropped_refs": {},
                "quality_warnings": quality_warnings,
                "insufficient_context": not selected,
                "partial_context_pack": True,
        }
        if reason != "service_backpressure":
            self.append_audit(
                compact_context_pack_audit_record({
                    "record_type": "context_pack_audit",
                    "context_pack_id": context_pack_id,
                    "query": query,
                    "scope": scope,
                    "summary_text": summarize_text(" ".join(str(item.get("text", "")) for item in selected), limit=512),
                    "selected_refs": compact_refs_for_audit(selected),
                    "selected_ref_counts": selected_context_class_counts(selected),
                    "local_context_refs": compact_local_context_refs(local_budget),
                    "context_sources_order": pack["context_sources_order"],
                    "question_type": question_type,
                    "packing_policy": pack["packing_policy"],
                    "recall_policy": pack["recall_policy"],
                        "quality_warnings": pack["quality_warnings"],
                        "partial_context_pack": True,
                        "memory_layer_budget": memory_layer_budget,
                        "async_pipeline_readiness": async_pipeline_readiness,
                        "local_context_policy": pack["local_context_policy"],
                    "used_local_context_tokens": pack["used_local_context_tokens"],
                    "used_remote_context_tokens": pack["used_remote_context_tokens"],
                    "total_prompt_context_tokens": pack["total_prompt_context_tokens"],
                    "remote_context_budget_tokens": pack["remote_context_budget_tokens"],
                    "requested_max_context_tokens": pack["requested_max_context_tokens"],
                    "local_context_safety_margin_tokens": pack["local_context_safety_margin_tokens"],
                    "budget_source": pack["budget_source"],
                    "primary_candidate_count": 0,
                    "auxiliary_candidate_count": 0,
                    "created_at_ms": now_ms(),
                })
            )
        else:
            pack["operational_visibility_policy"] = {
                "audit_mode": "telemetry_only",
                "rich_replay_audit": False,
                "reason": "service_backpressure_uses_access_audit_only",
            }
        return compact_context_pack_for_serving(pack)

    def supports_native_candidate_prefilter(self) -> bool:
        return False

    def supports_native_context_pack(self) -> bool:
        return False

    def native_context_pack_required(self) -> bool:
        if MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK:
            return MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK in {"1", "true", "yes"}
        backend_label = str(getattr(self, "_backend_label", lambda: "local")())
        return backend_label != "local"

    def native_context_pack(self, request: Json) -> Json | None:
        """Return a backend-assembled ContextPack when the native backend supports it.

        Python remains responsible for MCP/auth/model glue and request shaping.
        conformance backends should own scan, secondary-index filtering, scoring, and
        budget-aware pack assembly through this boundary when available.
        """
        return None

