# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_LocalAdapterSessionCommitMixin methods split from matrixark_mcp_local_adapter.MatrixArkLocalAdapter (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # names owned by the parent module
    from tools.matrixark_mcp_local_adapter import (
    MEMORY_TOMBSTONE_RECORD_TYPE,
    compact_and_apply_tombstones,
    filter_live_memory_records,
    session_event_message_count,
    session_events_by_message_limit,
    source_event_lineage_summary,
    surviving_source_event_ids,
)
except ImportError:
    from matrixark_mcp_local_adapter import (
    MEMORY_TOMBSTONE_RECORD_TYPE,
    compact_and_apply_tombstones,
    filter_live_memory_records,
    session_event_message_count,
    session_events_by_message_limit,
    source_event_lineage_summary,
    surviving_source_event_ids,
)

try:  # gate flags only (runtime_config is a leaf; symbols do not star-propagate).
    from tools.matrixark_mcp_runtime_config import (
        SKILL_DISCOVERY_ENABLED,
        SKILL_DISCOVERY_MIN_SUPPORT,
        SKILL_DISCOVERY_MAX_SKILLS,
    )
except ImportError:
    from matrixark_mcp_runtime_config import (
        SKILL_DISCOVERY_ENABLED,
        SKILL_DISCOVERY_MIN_SUPPORT,
        SKILL_DISCOVERY_MAX_SKILLS,
    )


def _load_skill_discovery():
    """Lazy import: skill_discovery pulls matrixark_mcp_core, which would create an import
    cycle at adapter load time. Imported here only when discovery actually runs (gated)."""
    try:
        from tools import matrixark_skill_discovery as mod
    except ImportError:
        import matrixark_skill_discovery as mod
    return mod


class _LocalAdapterSessionCommitMixin:
    def _maybe_discover_skills(self, scope: Json, messages: list, *, final_session_boundary: bool) -> Json | None:
        """Mine reusable skills from the committed session and persist them (gated, safe).

        Runs only on the final session boundary and only when SKILL_DISCOVERY_ENABLED.
        Discovered skills are user-scoped and cross-session durable; deduped against
        already-defined local skills. Never raises through to the commit path.
        """
        if not SKILL_DISCOVERY_ENABLED or not final_session_boundary or not messages:
            return None
        try:
            _skill_discovery = _load_skill_discovery()
            session_id = ":".join(str(part) for part in session_buffer_key_from_scope(scope))
            events = _skill_discovery.events_from_leaned_messages(messages, session_id)
            local_records = [
                record for record in self.read_all()
                if record.get("record_type") in {"skill_manifest", "skill_registry"}
            ]
            return _skill_discovery.discover_capture_for_session(
                events, scope=scope, ingest_records=self.append_many,
                local_records=local_records, updated_at_ms=now_ms(),
                min_support=SKILL_DISCOVERY_MIN_SUPPORT, max_skills=SKILL_DISCOVERY_MAX_SKILLS,
                deployment_scope="user", sharing_scope="user",
            )
        except Exception as exc:  # discovery must never break a commit
            return {"discovered": 0, "captured": 0, "error": f"{type(exc).__name__}: {exc}"}

    def _commit_records_of_types(self, wanted: list[str]) -> list[Json]:
        """Records of exactly these types, tombstones applied.

        The commit path used `read_all()` -- a full store scan -- and then kept two record types
        out of it. Sampling a commit on a 250-memory store put 13 of 20 active samples inside that
        read and its compaction, at 893 ms per call, with the proxy idle: the cost was reading
        everything in order to look at almost nothing, and it grows with the store.

        The type index answers the same question directly. Tombstones are the catch and are why
        this is not a one-line swap: `_scan_records_of_types` returns the raw rows, so a commit
        belonging to a forgotten session would come back from the dead. The scan therefore asks
        for the tombstones too and applies them, which is what `read_all()` was doing for us.

        Falls back to the full read when the scan cannot answer -- `None` means "could not ask",
        which is not the same as "nothing of these types".
        """
        # The scan belongs to the TemporalStore-backed adapter; the plain local adapter commits
        # sessions too and does not have it. A missing scanner is the same answer as a scanner
        # that cannot answer -- do the full read -- so ask for it rather than assuming the mixin.
        scanner = getattr(self, "_scan_records_of_types", None)
        if not callable(scanner):
            return self.read_all()
        subset = scanner(list(wanted) + [MEMORY_TOMBSTONE_RECORD_TYPE])
        if subset is None:
            return self.read_all()
        return filter_live_memory_records(compact_and_apply_tombstones(list(subset)))

    def session_commit(self, args: Json, *, hook: Json | None = None) -> Json:
        scope = optional_object(args, "scope")
        threshold = args.get("threshold_messages", 20)
        if not isinstance(threshold, int) or threshold <= 0:
            raise MatrixArkError("threshold_messages must be a positive integer")
        force = bool(args.get("force", True))
        commit_reason = optional_string(args, "commit_reason") or ("manual_api" if force else "threshold")
        idle_timeout_ms = args.get("idle_timeout_ms")
        if idle_timeout_ms is not None and (not isinstance(idle_timeout_ms, int) or idle_timeout_ms < 0):
            raise MatrixArkError("idle_timeout_ms must be a non-negative integer")
        max_messages = args.get("max_messages")
        if max_messages is not None and (not isinstance(max_messages, int) or max_messages <= 0):
            raise MatrixArkError("max_messages must be a positive integer")
        pending_all_unfiltered = self.pending_session_events(scope)
        # Delete-before-extract forward guard (load-bearing): a pending source event can be deleted (or
        # its subject forgotten) BEFORE this async commit materializes its derivatives. The delete/forget
        # tombstone already kills the source ``context_event`` + its ``session_buffer_event``, but the
        # buffer/commit path reads the raw log, so without this filter the extractor would re-materialize
        # entities/summaries/segments with fresh ids appended AFTER the tombstone -- resurrecting the
        # deleted content (the order-aware ``apply_memory_tombstones`` only removes records that precede
        # the tombstone). We consult the durable, order-aware tombstone sweep and drop any pending event
        # whose source no longer survives, so NO derivative is ever produced for a deleted source. This
        # runs for EVERY commit trigger (manual/force, threshold, idle-timeout, session-boundary/finalize,
        # auto_batch_extract) because they all funnel through session_commit, and it is durable +
        # cross-process because the signal is read fresh from the JSONL log on each commit.
        # Use the RAW (un-compacted, tombstone-bearing) log: ``read_all()`` is already tombstone-filtered
        # so it would show no tombstones, and ``pending_session_events`` can serve the deleted event from
        # its in-memory buffer cache (populated at ingest, not invalidated by a later delete) -- so the
        # guard must consult the durable append history to detect the delete.
        # TODO(engine): the Rust engine-side batch-extraction/commit path (crates/**) needs the same
        # forward guard against delete-before-extract resurrection; this fix wires only the Python local
        # adapter. Not edited here per scope (crates are out of scope for this change).
        # Ask first whether there is anything to guard against. `surviving_source_event_ids`
        # returns None -- i.e. keep every pending event -- whenever the log carries no tombstone,
        # so on a log with none the raw read below is pure cost, once per commit, over the whole
        # store. A backend that can answer the question cheaply says so; the default answers it
        # by doing the read, exactly as before.
        # The backend answers from its tombstones alone when it can; the base implementation
        # is the probe-then-full-read this block used to inline.
        _surviving_event_ids = self.surviving_ids_for_pending_events(pending_all_unfiltered)
        if _surviving_event_ids is not None:
            pending_all_unfiltered = [
                record
                for record in pending_all_unfiltered
                if str(record.get("event_id_hash")) in _surviving_event_ids
            ]
        commit_before_ms = args.get("commit_before_ms")
        if commit_before_ms is None:
            commit_before_ms = args.get("idle_commit_cutoff_ms")
        if commit_before_ms is not None and (not isinstance(commit_before_ms, int) or commit_before_ms < 0):
            raise MatrixArkError("commit_before_ms must be a non-negative integer")

        def pending_event_time_ms(record: Json) -> int:
            try:
                return int(record.get("envelope", {}).get("ingestion_time_ms") or record.get("updated_at_ms") or 0)
            except (TypeError, ValueError):
                return 0

        if commit_before_ms:
            pending_all = [
                record
                for record in pending_all_unfiltered
                if pending_event_time_ms(record) <= commit_before_ms
            ]
        else:
            pending_all = pending_all_unfiltered
        pending_event_count = len(pending_all)
        pending_message_count = session_event_message_count(pending_all)
        pending_deferred_event_count = max(0, len(pending_all_unfiltered) - pending_event_count)
        pending_deferred_message_count = max(0, session_event_message_count(pending_all_unfiltered) - pending_message_count)
        idle_elapsed_ms = 0
        idle_ready = False
        if pending_all and idle_timeout_ms is not None:
            latest_event_time = max(
                pending_event_time_ms(record)
                for record in pending_all
            )
            idle_elapsed_ms = max(0, now_ms() - latest_event_time)
            idle_ready = idle_elapsed_ms >= idle_timeout_ms
        threshold_ready = pending_event_count >= threshold or pending_message_count >= threshold
        trigger_evidence: Json = {
            "pending_event_count": pending_event_count,
            "pending_message_count": pending_message_count,
            "threshold_messages": threshold,
            "threshold_ready": threshold_ready,
            "idle_timeout_ms": idle_timeout_ms,
            "idle_elapsed_ms": idle_elapsed_ms,
            "idle_ready": idle_ready,
            "force": force,
            "commit_reason": commit_reason,
            "commit_before_ms": commit_before_ms,
            "pending_deferred_event_count": pending_deferred_event_count,
            "pending_deferred_message_count": pending_deferred_message_count,
        }
        if not force and not threshold_ready and not idle_ready:
            return {
                "status": "deferred",
                "pending_event_count": pending_event_count,
                "pending_message_count": pending_message_count,
                "threshold_messages": threshold,
                "commit_reason": commit_reason,
                "idle_timeout_ms": idle_timeout_ms,
                "idle_elapsed_ms": idle_elapsed_ms,
                "commit_before_ms": commit_before_ms,
                "pending_deferred_event_count": pending_deferred_event_count,
                "pending_deferred_message_count": pending_deferred_message_count,
                "trigger_evidence": trigger_evidence,
                "reason": "session buffer below extraction threshold and idle timeout not reached",
            }
        trigger_policy = "force" if force else "idle_timeout" if idle_ready else "threshold"
        extraction_phase = "final" if force else "provisional"
        final_session_boundary = extraction_phase == "final"
        if max_messages is not None:
            commit_limit = max_messages
        elif force or idle_ready:
            commit_limit = None
        else:
            commit_limit = threshold
        pending = session_events_by_message_limit(pending_all, commit_limit)
        messages = []
        source_event_ids = []
        pending_source_roles: set[str] = set()
        pending_source_hook_types: set[str] = set()
        pending_source_codex_events: set[str] = set()
        for record in pending:
            event_envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else {}
            event_metadata = event_envelope.get("metadata", {}) if isinstance(event_envelope.get("metadata"), dict) else {}
            event_hook = record.get("agent_hook", {}) if isinstance(record.get("agent_hook"), dict) else {}
            for value in [event_envelope.get("hook_type"), event_metadata.get("hook_type"), event_hook.get("hook_type")]:
                if str(value or "").strip():
                    pending_source_hook_types.add(str(value).strip())
            for values in [event_metadata.get("source_hook_types")]:
                if isinstance(values, list):
                    pending_source_hook_types.update(str(value).strip() for value in values if str(value or "").strip())
            for value in [event_envelope.get("codex_event"), event_metadata.get("codex_event"), event_hook.get("codex_event"), event_hook.get("trigger")]:
                if str(value or "").strip():
                    pending_source_codex_events.add(str(value).strip())
            for values in [event_metadata.get("source_codex_events")]:
                if isinstance(values, list):
                    pending_source_codex_events.update(str(value).strip() for value in values if str(value or "").strip())
            record_messages = messages_from_event_record(record)
            if not record_messages:
                continue
            for message in record_messages:
                role = normalize_message_role(message.get("role"))
                if role:
                    pending_source_roles.add(role)
            for values in [event_metadata.get("source_roles")]:
                if isinstance(values, list):
                    pending_source_roles.update(normalize_message_role(value) for value in values if normalize_message_role(value))
            messages.extend(record_messages)
            source_event_ids.append(record["event_id_hash"])
        if not messages:
            if force and final_session_boundary:
                session_key = session_buffer_key_from_scope(scope)
                records = self._commit_records_of_types(
                    ["context_batch_commit", "context_session_boundary"]
                )
                prior_commits = [
                    record
                    for record in records
                    if record.get("record_type") == "context_batch_commit"
                    and session_buffer_key_from_scope(record.get("scope", {})) == session_key
                ]
                already_finalized = any(
                    (
                        record.get("record_type") == "context_session_boundary"
                        and record.get("final_session_boundary")
                        and session_buffer_key_from_scope(record.get("scope", {})) == session_key
                    )
                    or (
                        record.get("record_type") == "context_batch_commit"
                        and record.get("final_session_boundary")
                        and session_buffer_key_from_scope(record.get("scope", {})) == session_key
                    )
                    for record in records
                )
                if prior_commits and not already_finalized:
                    node_path = self.default_session_node_path(scope)
                    node_hash = stable_hash("/".join(node_path))
                    committed_event_ids: list[int] = []
                    for commit in prior_commits:
                        for event_id in commit.get("source_event_ids", []):
                            try:
                                committed_event_ids.append(int(event_id))
                            except (TypeError, ValueError):
                                continue
                    source_lineage = source_event_lineage_summary(prior_commits)
                    finalized_at_ms = now_ms()
                    unique_event_ids = ordered_unique_any(committed_event_ids)
                    boundary_hash = stable_hash(
                        f"session_boundary:{session_key}:{unique_event_ids}:final"
                    )
                    summary_hash = stable_hash(f"session_final_summary:{boundary_hash}")
                    summary_text = (
                        f"Session finalized after {len(prior_commits)} provisional commit(s) "
                        f"covering {len(unique_event_ids)} event(s)."
                    )
                    boundary_record: Json = {
                        "record_type": "context_session_boundary",
                        "boundary_hash": boundary_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": scope,
                        "summary_text": summary_text,
                        "status": "finalized",
                        "commit_reason": commit_reason,
                        "trigger_policy": trigger_policy,
                        "extraction_phase": "final",
                        "final_session_boundary": True,
                        "prior_commit_count": len(prior_commits),
                        "prior_committed_event_count": len(unique_event_ids),
                        "source_event_count": len(unique_event_ids),
                        **{
                            key: value
                            for key, value in source_lineage.items()
                            if key
                            in {
                                "source_roles",
                                "source_role_counts",
                                "source_hook_types",
                                "source_hook_type_counts",
                                "source_codex_events",
                                "source_codex_event_counts",
                                "source_memory_selection_policies",
                                "source_memory_selection_policy_counts",
                                "source_memory_selection_lossy_count",
                                "source_memory_selection_complete_count",
                                "source_memory_selection_dropped_text_chars",
                                "source_memory_selection_dropped_line_count",
                                "source_memory_selection_retained_text_ratio_avg",
                                "source_memory_selection_retained_line_ratio_avg",
                                "source_extraction_phases",
                            }
                            and value not in (None, "", [], {})
                        },
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "created_at_ms": finalized_at_ms,
                        "updated_at_ms": finalized_at_ms,
                    }
                    summary_record: Json = {
                        "record_type": "context_summary",
                        "summary_type": "session_final",
                        "summary_hash": summary_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "summary_text": summary_text,
                        "source_event_ids": unique_event_ids,
                        "source_boundary_hash": boundary_hash,
                        "source_commit_count": len(prior_commits),
                        "source_event_count": len(unique_event_ids),
                        **{
                            key: value
                            for key, value in source_lineage.items()
                            if key
                            in {
                                "source_roles",
                                "source_role_counts",
                                "source_hook_types",
                                "source_hook_type_counts",
                                "source_codex_events",
                                "source_codex_event_counts",
                                "source_memory_selection_policies",
                                "source_memory_selection_policy_counts",
                                "source_memory_selection_lossy_count",
                                "source_memory_selection_complete_count",
                                "source_memory_selection_dropped_text_chars",
                                "source_memory_selection_dropped_line_count",
                                "source_memory_selection_retained_text_ratio_avg",
                                "source_memory_selection_retained_line_ratio_avg",
                                "source_extraction_phases",
                            }
                            and value not in (None, "", [], {})
                        },
                        "scope": scope,
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "extraction_phase": "final",
                        "final_session_boundary": True,
                        "updated_at_ms": finalized_at_ms,
                    }
                    summary_index_names = [
                        *sorted(candidate_index_terms(summary_record, {}, {})),
                        "final_session_boundary:true",
                    ]
                    for phase in source_lineage.get("source_extraction_phases", []) or []:
                        phase_name = str(phase or "").strip().lower()
                        if phase_name:
                            summary_index_names.append(f"source_extraction_phase:{phase_name}")
                    summary_indexes = [
                        context_index_posting_record(
                            index_name=index_name,
                            data_model="context_summary",
                            ref_type="summary",
                            ref_hashes=[summary_hash],
                            batch_id_hash=boundary_hash,
                            node_hash=node_hash,
                            scope=scope,
                            updated_at_ms=finalized_at_ms,
                        )
                        for index_name in ordered_unique_any(summary_index_names)
                    ]
                    _dirty_hashes, dirty_records = self.node_summary_dirty_records(
                        node_path=node_path,
                        scope=scope,
                        updated_at_ms=finalized_at_ms,
                        source_ref_type="session_boundary",
                        source_hash_field="source_boundary_hash",
                        source_hash=boundary_hash,
                        dirty_reason="session_finalized",
                        propagate_depth=0,
                        source_lineage=boundary_record,
                    )
                    self.append_many([boundary_record, summary_record, *summary_indexes, *dirty_records])
                    return {
                        "status": "finalized",
                        "pending_event_count": pending_event_count,
                        "pending_message_count": pending_message_count,
                        "threshold_messages": threshold,
                        "commit_reason": commit_reason,
                        "trigger_policy": trigger_policy,
                        "idle_timeout_ms": idle_timeout_ms,
                        "idle_elapsed_ms": idle_elapsed_ms,
                        "extraction_phase": "final",
                        "final_session_boundary": True,
                        "boundary_hash": boundary_hash,
                        "summary_hash": summary_hash,
                        "prior_commit_count": len(prior_commits),
                        "prior_committed_event_count": len(unique_event_ids),
                        "memory_layers_written": {
                            "session_final_summary": 1,
                            "secondary_indexes": len(summary_indexes),
                            "summary_dirty_nodes": len(_dirty_hashes),
                            "summary_refresh_status": "dirty_marked",
                            "extraction_phase": "final",
                            "final_session_boundary": True,
                        },
                        "summary_refresh": {
                            "status": "dirty_marked",
                            "dirty_hashes": _dirty_hashes,
                            "dirty_reason": "session_finalized",
                        },
                        "trigger_evidence": {
                            **trigger_evidence,
                            "already_finalized": False,
                            "prior_commit_count": len(prior_commits),
                        },
                    }
                if already_finalized:
                    trigger_evidence = {
                        **trigger_evidence,
                        "already_finalized": True,
                        "prior_commit_count": len(prior_commits),
                    }
            return {
                "status": "empty",
                "pending_event_count": pending_event_count,
                "pending_message_count": pending_message_count,
                "threshold_messages": threshold,
                "commit_reason": commit_reason,
                "idle_timeout_ms": idle_timeout_ms,
                "idle_elapsed_ms": idle_elapsed_ms,
                "commit_before_ms": commit_before_ms,
                "pending_deferred_event_count": pending_deferred_event_count,
                "pending_deferred_message_count": pending_deferred_message_count,
                "trigger_evidence": trigger_evidence,
            }
        try:
            overlap_limit = int(args.get("extraction_context_overlap_messages", 2))
        except (TypeError, ValueError):
            overlap_limit = 2
        if force:
            overlap_limit = 0
        overlap_limit = max(0, overlap_limit)
        current_source_event_ids = {int(event_id) for event_id in source_event_ids}
        committed_event_ids: set[int] = set()
        session_key = session_buffer_key_from_scope(scope)
        records_for_overlap = (
            self._commit_records_of_types(["context_batch_commit", "context_event"])
            if overlap_limit
            else []
        )
        for record in records_for_overlap:
            if record.get("record_type") != "context_batch_commit" or session_buffer_key_from_scope(record.get("scope", {})) != session_key:
                continue
            for event_id in record.get("source_event_ids", []):
                try:
                    committed_event_ids.add(int(event_id))
                except (TypeError, ValueError):
                    continue
        overlap_records: list[Json] = []
        for record in records_for_overlap:
            if record.get("record_type") != "context_event":
                continue
            try:
                event_id = int(record.get("event_id_hash"))
            except (TypeError, ValueError):
                continue
            if event_id in current_source_event_ids or event_id not in committed_event_ids:
                continue
            overlap_records.append(record)
        overlap_records = overlap_records[-overlap_limit:]
        extraction_context_messages = [
            message
            for record in overlap_records
            for message in messages_from_event_record(record)
            if message
        ]
        extraction_context_event_ids = [
            int(record["event_id_hash"])
            for record in overlap_records
            if record.get("event_id_hash") is not None
        ]
        metadata = optional_object(args, "metadata")
        source_roles = sorted(pending_source_roles)
        if not pending_source_hook_types:
            for event_name in pending_source_codex_events:
                hook_name = legacy_hook_type_from_codex_event(event_name)
                if hook_name:
                    pending_source_hook_types.add(hook_name)
        source_hook_types = sorted(pending_source_hook_types)
        source_codex_events = sorted(pending_source_codex_events)
        source_lineage = source_event_lineage_summary(pending)
        source_role_counts = source_lineage.get("source_role_counts", {})
        source_hook_type_counts = source_lineage.get("source_hook_type_counts", {})
        source_codex_event_counts = source_lineage.get("source_codex_event_counts", {})
        source_memory_selection_policies = source_lineage.get("source_memory_selection_policies", [])
        source_memory_selection_policy_counts = source_lineage.get("source_memory_selection_policy_counts", {})
        source_profile_memory_classes = source_lineage.get("source_profile_memory_classes", [])
        source_profile_memory_kinds = source_lineage.get("source_profile_memory_kinds", [])
        source_memory_selection_retention: Json = {
            key: source_lineage.get(key)
            for key in [
                "source_memory_selection_lossy_count",
                "source_memory_selection_complete_count",
                "source_memory_selection_dropped_text_chars",
                "source_memory_selection_dropped_line_count",
                "source_memory_selection_retained_text_ratio_avg",
                "source_memory_selection_retained_line_ratio_avg",
            ]
            if source_lineage.get(key) not in (None, "", [], {})
        }
        if source_roles:
            metadata = {**metadata, "source_roles": source_roles, "source_role_counts": source_role_counts}
        if pending_source_hook_types:
            metadata = {**metadata, "source_hook_types": source_hook_types, "source_hook_type_counts": source_hook_type_counts}
            if "hook_type" not in metadata and len(pending_source_hook_types) == 1:
                metadata["hook_type"] = next(iter(pending_source_hook_types))
        if pending_source_codex_events:
            metadata = {**metadata, "source_codex_events": source_codex_events, "source_codex_event_counts": source_codex_event_counts}
            if "codex_event" not in metadata and len(pending_source_codex_events) == 1:
                metadata["codex_event"] = next(iter(pending_source_codex_events))
        if source_memory_selection_policies:
            metadata = {
                **metadata,
                "source_memory_selection_policies": source_memory_selection_policies,
                "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
                **source_memory_selection_retention,
            }
        if source_profile_memory_classes or source_profile_memory_kinds:
            metadata = {
                **metadata,
                "source_profile_memory_classes": source_profile_memory_classes,
                "source_profile_memory_kinds": source_profile_memory_kinds,
            }
        storage_options = normalize_storage_options(args, metadata)
        if "node_path" not in metadata:
            metadata = {**metadata, "node_path": self.default_session_node_path(scope)}
        batch_result = self.batch_extract(
            {
                "messages": messages,
                "scope": scope,
                "metadata": metadata,
                "storage_options": storage_options,
                "threshold_messages": threshold,
                "force": True,
                "derive_from_existing_events": True,
                "source_event_ids": source_event_ids,
                "source_event_records": pending,
                "extraction_context_messages": extraction_context_messages,
                "extraction_context_event_ids": extraction_context_event_ids,
                "extraction_phase": extraction_phase,
                "final_session_boundary": final_session_boundary,
                "understanding_provider": args.get("understanding_provider"),
                "extraction_provider": args.get("extraction_provider"),
                "segment_provider": args.get("segment_provider"),
                "segment_model": args.get("segment_model"),
                "segment_model_path": args.get("segment_model_path"),
                "segment_max_new_tokens": args.get("segment_max_new_tokens"),
                "segment_provider_fallback": args.get("segment_provider_fallback"),
                "skip_prior_context": bool(args.get("skip_prior_context", False)),
            },
            hook=hook,
        )
        commit_id_hash = stable_hash(f"commit:{scope}:{source_event_ids}:{now_ms()}")
        memory_layers_written = {
            "context_events": int(batch_result.get("events_written") or 0),
            "segments": int(batch_result.get("segments_written") or 0),
            "session_entities": int(batch_result.get("entities_written") or 0),
            "profile_entities": int(batch_result.get("profile_entities_written") or 0),
            "same_session_entities": int(batch_result.get("entities_written") or 0),
            "cross_session_entities": int(batch_result.get("profile_entities_written") or 0),
            "secondary_indexes": int(batch_result.get("indexes_written") or 0),
            "summary_dirty_nodes": len(batch_result.get("summary_refresh", {}).get("dirty_hashes", [])) if isinstance(batch_result.get("summary_refresh"), dict) else 0,
            "summary_refresh_status": batch_result.get("summary_refresh", {}).get("status") if isinstance(batch_result.get("summary_refresh"), dict) else None,
            "extraction_phase": extraction_phase,
            "final_session_boundary": final_session_boundary,
            "source_roles": source_roles,
            "source_hook_types": source_hook_types,
            "source_codex_events": source_codex_events,
            "source_memory_selection_policies": source_memory_selection_policies,
            **source_memory_selection_retention,
            "profile_promotion_policy": batch_result.get("profile_promotion_policy"),
            "profile_promotion_importance_gate": bool(batch_result.get("profile_promotion_importance_gate", False)),
            "profile_promotion_blocker": batch_result.get("profile_promotion_blocker"),
        }
        memory_layers_written = {
            key: value
            for key, value in memory_layers_written.items()
            if value not in (None, "", [], {})
        }
        committed_at_ms = now_ms()
        self.append(
            {
                "record_type": "context_batch_commit",
                "commit_id_hash": commit_id_hash,
                "batch_id_hash": batch_result.get("batch_id_hash"),
                "node_hash": batch_result.get("node_hash"),
                "node_path": metadata["node_path"],
                "source_event_ids": source_event_ids,
                "scope": scope,
                "message_count": len(messages),
                "threshold_messages": threshold,
                "commit_reason": commit_reason,
                "trigger_policy": trigger_policy,
                "extraction_phase": extraction_phase,
                "final_session_boundary": final_session_boundary,
                "pending_event_count_before_commit": pending_event_count,
                "pending_message_count_before_commit": pending_message_count,
                "pending_deferred_event_count": pending_deferred_event_count,
                "pending_deferred_message_count": pending_deferred_message_count,
                "commit_before_ms": commit_before_ms,
                "committed_event_count": len(source_event_ids),
                "extraction_context_event_ids": extraction_context_event_ids,
                "extraction_context_event_count": len(extraction_context_event_ids),
                "source_roles": source_roles,
                "source_role_counts": source_role_counts,
                "source_hook_types": source_hook_types,
                "source_hook_type_counts": source_hook_type_counts,
                "source_codex_events": source_codex_events,
                "source_codex_event_counts": source_codex_event_counts,
                "source_memory_selection_policies": source_memory_selection_policies,
                "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
                **source_memory_selection_retention,
                "profile_promotion_summary": batch_result.get("profile_promotion_summary", []),
                "profile_promotion_policy": batch_result.get("profile_promotion_policy"),
                "profile_promotion_importance_gate": bool(batch_result.get("profile_promotion_importance_gate", False)),
                "profile_promotion_blocker": batch_result.get("profile_promotion_blocker"),
                "summary_refresh": batch_result.get("summary_refresh", {}),
                "memory_layers_written": memory_layers_written,
                "idle_timeout_ms": idle_timeout_ms,
                "idle_elapsed_ms": idle_elapsed_ms,
                "trigger_evidence": trigger_evidence,
                "agent_hook": hook,
                "storage_options": storage_options,
                "storage_route": canonical_storage_route(storage_options),
                "created_at_ms": committed_at_ms,
            }
        )
        buffer_key = list(session_buffer_key_from_scope(scope))
        buffer_key_hash = stable_hash(":".join(buffer_key))
        progress_records: list[Json] = [
            {
                "record_type": "matrixark_async_pipeline_task",
                "task_hash": stable_hash(f"async_pipeline:{event_id}"),
                "event_id_hash": event_id,
                "commit_id_hash": commit_id_hash,
                "batch_id_hash": batch_result.get("batch_id_hash"),
                "scope": scope,
                "status": "extraction_committed",
                "stages": ["extraction", "summary", "compression", "embedding"],
                "completed_stages": ["extraction"],
                "remaining_stages": ["summary", "compression", "embedding"],
                "reason": "session_buffer_commit",
                "trigger_policy": trigger_policy,
                "extraction_phase": extraction_phase,
                "final_session_boundary": final_session_boundary,
                "pending_message_count_before_commit": pending_message_count,
                "source_roles": source_roles,
                "source_role_counts": source_role_counts,
                "source_hook_types": source_hook_types,
                "source_hook_type_counts": source_hook_type_counts,
                "source_codex_events": source_codex_events,
                "source_codex_event_counts": source_codex_event_counts,
                "source_memory_selection_policies": source_memory_selection_policies,
                "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
                **source_memory_selection_retention,
                "summary_refresh_status": memory_layers_written.get("summary_refresh_status"),
                "summary_dirty_nodes": memory_layers_written.get("summary_dirty_nodes", 0),
                "memory_layers_written": memory_layers_written,
                "updated_at_ms": committed_at_ms,
            }
            for event_id in source_event_ids
        ]
        for event_id in source_event_ids:
            progress_records.append(
                {
                    "record_type": "session_buffer_event",
                    "buffer_key_hash": buffer_key_hash,
                    "buffer_key": buffer_key,
                    "event_id_hash": event_id,
                    "commit_id_hash": commit_id_hash,
                    "batch_id_hash": batch_result.get("batch_id_hash"),
                    "scope": scope,
                    "status": "committed",
                    "reason": "session_buffer_commit",
                    "trigger_policy": trigger_policy,
                    "extraction_phase": extraction_phase,
                    "final_session_boundary": final_session_boundary,
                    "source_roles": source_roles,
                    "source_role_counts": source_role_counts,
                    "source_hook_types": source_hook_types,
                    "source_hook_type_counts": source_hook_type_counts,
                    "source_codex_events": source_codex_events,
                    "source_codex_event_counts": source_codex_event_counts,
                    "source_memory_selection_policies": source_memory_selection_policies,
                    "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
                    **source_memory_selection_retention,
                    "updated_at_ms": committed_at_ms,
                }
            )
        self.append_many(progress_records)
        skill_discovery_result = self._maybe_discover_skills(
            scope, messages, final_session_boundary=final_session_boundary
        )
        return {
            **batch_result,
            "status": "committed",
            "commit_id_hash": commit_id_hash,
            **({"skill_discovery": skill_discovery_result} if skill_discovery_result else {}),
            "storage_options": storage_options,
            "storage_route": canonical_storage_route(storage_options),
            "pending_event_count": pending_event_count,
            "pending_message_count": pending_message_count,
            "pending_deferred_event_count": pending_deferred_event_count,
            "pending_deferred_message_count": pending_deferred_message_count,
            "commit_before_ms": commit_before_ms,
            "committed_event_count": len(source_event_ids),
            "source_event_ids": source_event_ids,
            "extraction_context_event_ids": extraction_context_event_ids,
            "extraction_context_event_count": len(extraction_context_event_ids),
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
            "profile_promotion_summary": batch_result.get("profile_promotion_summary", []),
            "commit_reason": commit_reason,
            "trigger_policy": trigger_policy,
            "extraction_phase": extraction_phase,
            "final_session_boundary": final_session_boundary,
            "idle_timeout_ms": idle_timeout_ms,
            "idle_elapsed_ms": idle_elapsed_ms,
            "trigger_evidence": trigger_evidence,
            "memory_layers_written": memory_layers_written,
            "raw_events_duplicated": False,
        }

