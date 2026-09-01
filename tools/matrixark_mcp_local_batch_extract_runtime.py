#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Local MatrixArk batch extraction runtime."""

from __future__ import annotations

import time
import re
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        CODEX_OUTCOME_ENTITY_TYPES,
        EMBEDDING_LINEAGE_DEBUG_FIELDS,
        MAX_PRIOR_MESSAGES,
        Json,
        apply_entity_patches,
        candidate_index_terms,
        candidate_memory_layer_name,
        collect_prior_context,
        compact_hot_context_embedding_record,
        context_index_posting_record,
        embedding_execution_mode_name,
        embedding_fallback_used,
        embedding_for_text,
        embedding_model_name,
        embeddings_for_texts,
        new_secondary_index_budget,
        normalized_node_path,
        normalize_message_role,
        now_ms,
        one_pass_memory_extraction,
        ordered_unique_any,
        profile_memory_class_for_entity_type,
        profile_memory_kind_for_entity_type,
        secondary_index_budget_summary,
        stable_hash,
        summarize_text,
        take_secondary_index_terms,
        text_from_messages,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        CODEX_OUTCOME_ENTITY_TYPES,
        EMBEDDING_LINEAGE_DEBUG_FIELDS,
        MAX_PRIOR_MESSAGES,
        Json,
        apply_entity_patches,
        candidate_index_terms,
        candidate_memory_layer_name,
        collect_prior_context,
        compact_hot_context_embedding_record,
        context_index_posting_record,
        embedding_execution_mode_name,
        embedding_fallback_used,
        embedding_for_text,
        embedding_model_name,
        embeddings_for_texts,
        new_secondary_index_budget,
        normalized_node_path,
        normalize_message_role,
        now_ms,
        one_pass_memory_extraction,
        ordered_unique_any,
        profile_memory_class_for_entity_type,
        profile_memory_kind_for_entity_type,
        secondary_index_budget_summary,
        stable_hash,
        summarize_text,
        take_secondary_index_terms,
        text_from_messages,
    )


def _segments_enabled(scope: Any) -> bool:
    """Whether this tenant materialises context_segment rows. Fails OPEN, as the fold gate does."""
    try:
        from matrixark_index_growth_bound import extract_segments_enabled
    except Exception:  # pragma: no cover - policy module absent
        return True
    return bool(extract_segments_enabled(scope))


def compact_context_embedding_record(record: Json) -> Json:
    return compact_hot_context_embedding_record(record)


def attach_memory_layer(record: Json) -> Json:
    """Stamp the retrieval layer on newly materialized memory records."""
    if str(record.get("record_type") or "") not in {
        "context_event",
        "context_entity",
        "context_segment",
        "context_summary",
        "context_compression_event",
        "context_embedding",
    }:
        return record
    layer = candidate_memory_layer_name(record)
    if not layer or layer == "unknown":
        return record
    return {**record, "memory_layer": layer}


ASSISTANT_PROFILE_FACT_LINEAGE_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [
        r"\b(?:user|you)\b.{0,96}\b(?:prefer|prefers|preference|likes|wants|needs|asked|requires|required|always|never|avoid|remember)\b",
        r"\b(?:i(?:'ll| will)? remember|remembered|noted|got it|understood)\b.{0,140}\b(?:prefer|preference|want|need|always|never|avoid|profile|memory|workspace|repo|branch|reply|respond|format|style)\b",
        r"\b(?:i(?:'ll| will)|codex will|assistant will)\b.{0,64}\b(?:remember|keep|use|follow|prefer|avoid|not use|always use|make sure)\b",
        r"\b(?:standing instruction|standing preference|user profile|long[- ]term memor(?:y|ies)|saved preference|persistent instruction)\b",
        r"\b(?:call me|my name is|user(?:'s)? name is|user goes by|pronouns?|address (?:me|the user))\b",
        r"\b(?:reply|respond|answer|write|communication style|response style|answer style|preferred language|preferred format|timezone|time zone|locale)\b.{0,120}\b(?:concise|brief|detailed|bullets?|markdown|language|tone|style|format|timezone|locale)\b",
        r"\b(?:workspace|repo|repository|branch|remote|github|origin/main|main branch|ubuntu|wsl|linux|windows folder|worktree|build|deploy|deployment|rustraft|temporalstore|matrixark)\b.{0,140}\b(?:always|prefer|use|keep|must|should|avoid|never|don't|push|build|deploy)\b",
    ]
]


def assistant_profile_fact_lineage_text(text: Any) -> bool:
    compact = " ".join(str(text or "").split())
    return bool(compact and any(pattern.search(compact) for pattern in ASSISTANT_PROFILE_FACT_LINEAGE_PATTERNS))


def user_profile_fact_lineage_text(text: Any) -> bool:
    compact = " ".join(str(text or "").split())
    return bool(compact and any(pattern.search(compact) for pattern in ASSISTANT_PROFILE_FACT_LINEAGE_PATTERNS))


def batch_extract_after_start(self: Any, args: Json, batch_start: Json) -> Json:
    envelope = batch_start["envelope"]
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
    hook = batch_start["hook"]
    threshold = batch_start["threshold"]
    derive_from_existing_events = bool(batch_start["derive_from_existing_events"])
    source_event_ids = list(batch_start["source_event_ids"])
    extraction_phase = str(batch_start.get("extraction_phase") or "").strip().lower()
    if extraction_phase not in {"provisional", "final", "standalone"}:
        extraction_phase = "final" if bool(batch_start.get("force")) else "provisional"
    final_session_boundary = bool(batch_start.get("final_session_boundary", extraction_phase == "final"))
    if batch_start.get("deferred_result") is not None:
        return batch_start["deferred_result"]

    prior_records = [] if args.get("skip_prior_context") else self.read_all()
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
    profile_scope = {key: value for key, value in envelope["scope"].items() if key != "session_id"}
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
    profile_promotion_policy = "always_when_profile_scope_available"
    profile_promotion_importance_gate = False
    profile_promotion_blocker = "" if profile_node_hash else "profile_scope_missing"
    source_session_id = str(envelope["scope"].get("session_id") or "")
    envelope_metadata = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
    source_roles = sorted(
        {
            normalize_message_role(message.get("role"))
            for message in extraction_envelope.get("messages", [])
            if isinstance(message, dict) and normalize_message_role(message.get("role"))
        }
    )
    source_role_counts: Json = {}
    for message in extraction_envelope.get("messages", []):
        if not isinstance(message, dict):
            continue
        role = normalize_message_role(message.get("role"))
        if role:
            source_role_counts[role] = int(source_role_counts.get(role, 0)) + 1
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
    source_hook_type_values = []
    if isinstance(hook, dict):
        source_hook_type_values.append(hook.get("hook_type"))
    source_hook_type_values.append(envelope.get("hook_type"))
    source_hook_type_values.append(envelope_metadata.get("hook_type"))
    if isinstance(envelope_metadata.get("source_hook_types"), list):
        source_hook_type_values.extend(envelope_metadata["source_hook_types"])
    source_hook_types = sorted({str(value).strip() for value in source_hook_type_values if str(value or "").strip()})
    source_hook_type_counts = {hook_type: 1 for hook_type in source_hook_types}
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
        source_hook_types = sorted(source_hook_type_counts)
    source_codex_event_values = []
    if isinstance(hook, dict):
        source_codex_event_values.extend([hook.get("codex_event"), hook.get("trigger")])
    source_codex_event_values.append(envelope.get("codex_event"))
    source_codex_event_values.append(envelope_metadata.get("codex_event"))
    if isinstance(envelope_metadata.get("source_codex_events"), list):
        source_codex_event_values.extend(envelope_metadata["source_codex_events"])
    source_codex_events = sorted({str(value).strip() for value in source_codex_event_values if str(value or "").strip()})
    source_codex_event_counts = {codex_event: 1 for codex_event in source_codex_events}
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
        source_codex_events = sorted(source_codex_event_counts)
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
            source_memory_selection_policy_counts[policy_name] = 1
    assistant_text_parts = [
        str(message.get("content") or "")
        for message in extraction_envelope.get("messages", [])
        if isinstance(message, dict) and normalize_message_role(message.get("role")) == "assistant"
    ]
    user_text_parts = [
        str(message.get("content") or "")
        for message in extraction_envelope.get("messages", [])
        if isinstance(message, dict) and normalize_message_role(message.get("role")) == "user"
    ]
    assistant_lineage_text = "\n".join(assistant_text_parts) or envelope_metadata.get("text") or envelope.get("text")
    assistant_policies: list[str] = []
    if assistant_profile_fact_lineage_text(assistant_lineage_text):
        assistant_policies.append("selected_assistant_profile_fact")
    if assistant_lineage_text or source_role_counts.get("assistant"):
        assistant_policies.append("selected_assistant_decision_outcome_only")
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
            source_memory_selection_policy_counts[policy_name] = max(1, int(count or 0))
    source_memory_selection_policies = sorted(source_memory_selection_policy_counts)
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
    batch_summary = extraction["batch_summary"]

    event_hashes: list[int] = list(source_event_ids) if derive_from_existing_events else []
    records_to_append: list[Json] = []
    event_records_to_append: list[Json] = []
    event_rows: list[tuple[int, Json, str, int]] = []
    segment_hash_by_position: dict[int, int] = {}
    segment_hashes_by_position: dict[int, list[int]] = {}
    for segment in extraction["segments"]:
        segment_hash = stable_hash(f"{batch_id_hash}:segment:{segment['topic']}:{segment['coordinate_tuples']}")
        for message_index in segment.get("message_indexes", []):
            if not isinstance(message_index, int):
                continue
            segment_hashes_by_position.setdefault(message_index, []).append(segment_hash)
            segment_hash_by_position.setdefault(message_index, segment_hash)
    if derive_from_existing_events:
        for index, message in enumerate(envelope["messages"]):
            if index >= len(source_event_ids):
                continue
            event_text = f"{message['role']}: {message['content']}"
            event_rows.append((index, message, event_text, int(source_event_ids[index])))
    else:
        for index, message in enumerate(envelope["messages"]):
            event_text = f"{message['role']}: {message['content']}"
            event_id_hash = stable_hash(f"{batch_id_hash}:event:{index}:{event_text}")
            event_hashes.append(event_id_hash)
            event_rows.append((index, message, event_text, event_id_hash))
    if event_rows:
        event_vectors = embeddings_for_texts([event_text for _index, _message, event_text, _event_id_hash in event_rows])
        for (_index, message, event_text, event_id_hash), event_vector in zip(event_rows, event_vectors):
            event_records_to_append.append(
                {
                    "record_type": "context_event",
                    "event_id_hash": event_id_hash,
                    "batch_id_hash": batch_id_hash,
                    "parent_segment_hash": segment_hash_by_position.get(_index),
                    "parent_segment_hashes": segment_hashes_by_position.get(_index, []),
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "text": event_text,
                    "summary_text": summarize_text(event_text),
                    "classification": extraction["classification"],
                    "event_type": extraction["event_type"],
                    "status": "extraction_committed" if derive_from_existing_events else "observed",
                    "source_kind": envelope.get("kind", "message"),
                    "envelope": {
                        **envelope,
                        "messages": [message],
                    },
                    "internal_extraction": {
                        "mode": extraction["mode"],
                        "classification": extraction["classification"],
                        "event_type": extraction["event_type"],
                        "batch_id_hash": batch_id_hash,
                    },
                    "prior_context": prior_context,
                    "agent_hook": hook,
                    "storage_options": envelope.get("storage_options", {}),
                    "extraction_phase": extraction_phase,
                    "final_session_boundary": final_session_boundary,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "source_role": str(message.get("role") or ""),
                    "source_roles": [str(message.get("role") or "")] if str(message.get("role") or "") else [],
                    "source_role_counts": {str(message.get("role") or ""): 1} if str(message.get("role") or "") else {},
                    "source_hook_types": source_hook_types,
                    "source_hook_type_counts": source_hook_type_counts,
                    "source_codex_events": source_codex_events,
                    "source_codex_event_counts": source_codex_event_counts,
                    "source_memory_selection_policies": source_memory_selection_policies,
                    "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
                    "extraction_context_event_ids": extraction_context_event_ids,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
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
                    "event_type": extraction["event_type"],
                    "source_role": str(message.get("role") or ""),
                    "source_roles": [str(message.get("role") or "")] if str(message.get("role") or "") else [],
                    "source_role_counts": {str(message.get("role") or ""): 1} if str(message.get("role") or "") else {},
                    "source_hook_types": source_hook_types,
                    "source_hook_type_counts": source_hook_type_counts,
                    "source_codex_events": source_codex_events,
                    "source_codex_event_counts": source_codex_event_counts,
                    "source_memory_selection_policies": source_memory_selection_policies,
                    "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
                    **source_memory_selection_retention,
                    "extraction_context_event_ids": extraction_context_event_ids,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "extraction_phase": extraction_phase,
                    "final_session_boundary": final_session_boundary,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                })
            )

    entity_hashes = []
    profile_entity_hashes = []
    profile_promotion_summary: list[Json] = []
    profile_dirty_hashes: list[int] = []
    entity_index_write_count = 0
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
            ordered_unique_any(
                [
                    normalize_message_role(value)
                    for value in entity.get("source_roles", [])
                    if normalize_message_role(value)
                ]
            )
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
            "state": updated_entity["state"],
            "previous_state": updated_entity.get("previous_state", ""),
            "confidence": updated_entity["confidence"],
            "operator": updated_entity["operator"],
            "source_refs": updated_entity["source_refs"],
            "source_event_ids": source_event_ids,
            "source_roles": entity_source_roles,
            "source_role_counts": entity_source_role_counts,
            "source_hook_types": source_hook_types,
            "source_hook_type_counts": source_hook_type_counts,
            "source_codex_events": source_codex_events,
            "source_codex_event_counts": source_codex_event_counts,
            "source_memory_selection_policies": source_memory_selection_policies,
            "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
            "field_patches": updated_entity.get("field_patches", []),
            "patch_results": updated_entity.get("patch_results", []),
            "update_mode": updated_entity.get("update_mode", ""),
            "memory_scope": "session",
            "session_continuity": "same_session",
            "extraction_phase": extraction_phase,
            "final_session_boundary": final_session_boundary,
            "extraction_context_event_ids": extraction_context_event_ids,
            "updated_at_ms": envelope["ingestion_time_ms"],
        }
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
        records_to_append.append(
            compact_context_embedding_record({
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
                "source_event_ids": source_event_ids,
                "source_roles": entity_source_roles,
                "source_role_counts": entity_source_role_counts,
                "source_hook_types": source_hook_types,
                "source_hook_type_counts": source_hook_type_counts,
                "source_codex_events": source_codex_events,
                "source_codex_event_counts": source_codex_event_counts,
                "memory_scope": "session",
                "session_continuity": "same_session",
                "extraction_phase": extraction_phase,
                "final_session_boundary": final_session_boundary,
                "extraction_context_event_ids": extraction_context_event_ids,
                "updated_at_ms": envelope["ingestion_time_ms"],
            })
        )
        if profile_node_hash:
            profile_entity_hash = stable_hash(
                f"{profile_node_hash}:{updated_entity['entity_type']}:{updated_entity['entity_name']}"
            )
            previous_profile_entity = self.find_latest_entity(
                node_hash=profile_node_hash,
                entity_type=updated_entity["entity_type"],
                entity_name=updated_entity["entity_name"],
            )
            promoted_entity = apply_entity_patches(previous_profile_entity, updated_entity)
            previous_profile = previous_profile_entity or {}
            previous_profile_state = str(previous_profile.get("state") or "")
            promoted_state = str(promoted_entity.get("state") or "")
            cumulative_profile_entity_types = {"assistant_decision", "tool_evidence", *CODEX_OUTCOME_ENTITY_TYPES}
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
            profile_source_refs = ordered_unique_any(
                list(previous_profile.get("source_refs", [])) + list(promoted_entity.get("source_refs", []))
            )
            profile_memory_class = profile_memory_class_for_entity_type(promoted_entity.get("entity_type"))
            profile_memory_kind = profile_memory_kind_for_entity_type(promoted_entity.get("entity_type"))
            profile_source_event_ids = ordered_unique_any(
                list(previous_profile.get("source_event_ids", [])) + event_hashes
            )
            profile_source_roles = ordered_unique_any(
                list(previous_profile.get("source_roles", [])) + entity_source_roles
            )
            profile_source_role_counts: Json = dict(previous_profile.get("source_role_counts", {}))
            for role, count in entity_source_role_counts.items():
                profile_source_role_counts[role] = int(profile_source_role_counts.get(role, 0)) + int(count)
            profile_source_hook_types = ordered_unique_any(
                list(previous_profile.get("source_hook_types", [])) + source_hook_types
            )
            profile_source_hook_type_counts: Json = dict(previous_profile.get("source_hook_type_counts", {}))
            for hook_type, count in source_hook_type_counts.items():
                profile_source_hook_type_counts[hook_type] = int(profile_source_hook_type_counts.get(hook_type, 0)) + int(count)
            profile_source_codex_events = ordered_unique_any(
                list(previous_profile.get("source_codex_events", [])) + source_codex_events
            )
            profile_source_codex_event_counts: Json = dict(previous_profile.get("source_codex_event_counts", {}))
            for codex_event, count in source_codex_event_counts.items():
                profile_source_codex_event_counts[codex_event] = int(profile_source_codex_event_counts.get(codex_event, 0)) + int(count)
            profile_source_memory_selection_policies = ordered_unique_any(
                list(previous_profile.get("source_memory_selection_policies", []))
                + source_memory_selection_policies
            )
            profile_source_memory_selection_policy_counts: Json = dict(
                previous_profile.get("source_memory_selection_policy_counts", {})
            )
            for policy, count in source_memory_selection_policy_counts.items():
                profile_source_memory_selection_policy_counts[policy] = int(
                    profile_source_memory_selection_policy_counts.get(policy, 0)
                ) + int(count)
            if profile_node_hash:
                profile_source_memory_selection_policies = ordered_unique_any(
                    profile_source_memory_selection_policies + ["selected_profile_current_state"]
                )
                profile_source_memory_selection_policy_counts["selected_profile_current_state"] = max(
                    1,
                    int(profile_source_memory_selection_policy_counts.get("selected_profile_current_state", 0) or 0),
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
            profile_entity_hashes.append(profile_entity_hash)
            profile_promotion_summary.append(
                {
                    "source_entity_hash": entity_hash,
                    "profile_entity_hash": profile_entity_hash,
                    "entity_type": promoted_entity["entity_type"],
                    "entity_name": promoted_entity["entity_name"],
                    "source_session_ids": profile_source_session_ids,
                    "source_entity_count": len(profile_source_entity_hashes),
                    "source_event_count": len(profile_source_event_ids),
                    "profile_memory_class": profile_memory_class,
                    "profile_memory_kind": profile_memory_kind,
                    "source_memory_selection_policies": profile_source_memory_selection_policies,
                    "source_memory_selection_policy_counts": profile_source_memory_selection_policy_counts,
                    **profile_source_memory_selection_retention,
                    "source_profile_promotion_policies": profile_source_promotion_policies,
                    "source_profile_promotion_blockers": profile_source_promotion_blockers,
                    "source_memory_scopes": profile_source_memory_scopes,
                    "source_session_continuities": profile_source_session_continuities,
                    "profile_revision": profile_revision,
                    "policy": profile_promotion_policy,
                    "blocker": "",
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
                "profile_promotion_blocker": "",
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
            records_to_append.append(profile_entity_record)
            profile_entity_embedding_text = promoted_entity["entity_type"] + " " + promoted_entity["state"]
            profile_entity_vector = embedding_for_text(profile_entity_embedding_text)
            records_to_append.append(
                compact_context_embedding_record({
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
                    "profile_promotion_blocker": "",
                    "profile_revision": profile_revision,
                    "profile_entity_current": True,
                    "profile_source_session_count": len(profile_source_session_ids),
                    "profile_source_entity_count": len(profile_source_entity_hashes),
                    "profile_source_event_count": len(profile_source_event_ids),
                    "extraction_phase": extraction_phase,
                    "final_session_boundary": final_session_boundary,
                    "extraction_context_event_ids": extraction_context_event_ids,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                })
            )
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
            profile_dirty_lineage: Json = {
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "source_session_ids": profile_source_session_ids,
                "source_entity_hashes": profile_source_entity_hashes,
                "source_event_ids": profile_source_event_ids,
                "source_roles": profile_source_roles,
                "source_role_counts": profile_source_role_counts,
                "source_hook_types": profile_source_hook_types,
                "source_hook_type_counts": profile_source_hook_type_counts,
                "source_codex_events": profile_source_codex_events,
                "source_codex_event_counts": profile_source_codex_event_counts,
                "profile_memory_class": profile_memory_class,
                "profile_memory_kind": profile_memory_kind,
                "source_memory_selection_policies": profile_source_memory_selection_policies,
                "source_memory_selection_policy_counts": profile_source_memory_selection_policy_counts,
                **profile_source_memory_selection_retention,
                "source_profile_promotion_policies": profile_source_promotion_policies,
                "source_profile_promotion_blockers": profile_source_promotion_blockers,
                "source_memory_scopes": profile_source_memory_scopes,
                "source_session_continuities": profile_source_session_continuities,
                "profile_promotion_policy": profile_promotion_policy,
                "profile_promotion_blocker": "",
                "profile_revision": profile_revision,
                "profile_entity_current": True,
                "extraction_phase": extraction_phase,
                "final_session_boundary": final_session_boundary,
            }
            new_profile_dirty_hashes, profile_dirty_records = self.node_summary_dirty_records(
                node_path=profile_node_path,
                scope=profile_scope,
                updated_at_ms=envelope["ingestion_time_ms"],
                source_ref_type="entity",
                source_hash_field="source_entity_hash",
                source_hash=profile_entity_hash,
                dirty_reason="profile_entity_promoted",
                propagate_depth=0,
                source_lineage=profile_dirty_lineage,
            )
            profile_dirty_hashes.extend(int(item) for item in new_profile_dirty_hashes)
            records_to_append.extend(profile_dirty_records)

    segment_hashes = []
    # A segment restates its event, so a tenant can decline them. The list stays empty rather than
    # absent, which keeps every downstream source_segment_hashes field well-formed.
    for segment in (extraction["segments"] if _segments_enabled(envelope.get("scope")) else []):
        segment_hash = stable_hash(f"{batch_id_hash}:segment:{segment['topic']}:{segment['coordinate_tuples']}")
        segment_hashes.append(segment_hash)
        records_to_append.append(
            {
                "record_type": "context_segment",
                "segment_hash": segment_hash,
                "batch_id_hash": batch_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "scope": envelope["scope"],
                "topic": segment["topic"],
                "coordinate_tuples": segment["coordinate_tuples"],
                "message_indexes": segment["message_indexes"],
                "source_event_ids": [event_hashes[index] for index in segment["message_indexes"] if index < len(event_hashes)],
                "source_record_type": "context_event",
                "segment_origin": segment.get("segment_origin") or segment.get("detected_by") or "derived_from_events",
                "derived_from_context_events": bool(segment.get("derived_from_context_events", True)),
                "source_roles": source_roles,
                "source_role_counts": source_role_counts,
                "source_hook_types": source_hook_types,
                "source_hook_type_counts": source_hook_type_counts,
                "source_codex_events": source_codex_events,
                "source_codex_event_counts": source_codex_event_counts,
                "source_memory_selection_policies": source_memory_selection_policies,
                "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
                "source_memory_scopes": ["session"],
                "source_session_continuities": ["same_session"],
                "source_extraction_phases": [extraction_phase],
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
        )
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
                "source_event_ids": [event_hashes[index] for index in segment["message_indexes"] if index < len(event_hashes)],
                "source_roles": source_roles,
                "source_role_counts": source_role_counts,
                "source_hook_types": source_hook_types,
                "source_hook_type_counts": source_hook_type_counts,
                "source_codex_events": source_codex_events,
                "source_codex_event_counts": source_codex_event_counts,
                "source_memory_selection_policies": source_memory_selection_policies,
                "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
                **source_memory_selection_retention,
                "source_memory_scopes": ["session"],
                "source_session_continuities": ["same_session"],
                "source_extraction_phases": [extraction_phase],
                "memory_scope": "session",
                "session_continuity": "same_session",
                "extraction_phase": extraction_phase,
                "final_session_boundary": final_session_boundary,
                "extraction_context_event_ids": extraction_context_event_ids,
                "updated_at_ms": envelope["ingestion_time_ms"],
            })
        )

    summary_hash = stable_hash(f"batch_summary:{batch_id_hash}")
    records_to_append.append(
        {
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
            "source_memory_scopes": ["session"],
            "source_session_continuities": ["same_session"],
            "source_extraction_phases": [extraction_phase],
            "scope": envelope["scope"],
            "memory_scope": "session",
            "session_continuity": "same_session",
            "extraction_phase": extraction_phase,
            "final_session_boundary": final_session_boundary,
            "extraction_context_event_ids": extraction_context_event_ids,
            "updated_at_ms": envelope["ingestion_time_ms"],
        }
    )
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
            **source_memory_selection_retention,
            "source_memory_scopes": ["session"],
            "source_session_continuities": ["same_session"],
            "source_extraction_phases": [extraction_phase],
            "memory_scope": "session",
            "session_continuity": "same_session",
            "extraction_phase": extraction_phase,
            "final_session_boundary": final_session_boundary,
            "extraction_context_event_ids": extraction_context_event_ids,
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
                ref_type="batch",
                ref_hashes=[batch_id_hash],
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
                "profile_promotion_policy": profile_promotion_policy,
                "profile_promotion_importance_gate": profile_promotion_importance_gate,
                "profile_promotion_scope_available": bool(profile_node_hash),
                "profile_promotion_blocker": profile_promotion_blocker,
                "profile_promotion_summary": profile_promotion_summary[:16],
                "segments": len(segment_hashes),
                "summaries": 1,
                "indexes": len(batch_index_terms) + entity_index_write_count,
                "entity_indexes": entity_index_write_count,
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
            "source_memory_scopes": ["session"],
            "source_session_continuities": ["same_session"],
            "source_extraction_phases": [extraction_phase],
            "extraction_context_event_ids": extraction_context_event_ids,
            "extraction_phase": extraction_phase,
            "final_session_boundary": final_session_boundary,
            "agent_hook": hook,
            "created_at_ms": now_ms(),
        }
    )
    session_dirty_lineage: Json = {
        "memory_scope": "session",
        "session_continuity": "same_session",
        "source_event_ids": event_hashes,
        "source_entity_hashes": entity_hashes,
        "source_roles": source_roles,
        "source_role_counts": source_role_counts,
        "source_hook_types": source_hook_types,
        "source_hook_type_counts": source_hook_type_counts,
        "source_codex_events": source_codex_events,
        "source_codex_event_counts": source_codex_event_counts,
        "source_memory_selection_policies": source_memory_selection_policies,
        "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
        **source_memory_selection_retention,
        "source_memory_scopes": ["session"],
        "source_session_continuities": ["same_session"],
        "source_extraction_phases": [extraction_phase],
        "extraction_phase": extraction_phase,
        "final_session_boundary": final_session_boundary,
    }
    dirty_hashes, dirty_records = self.node_summary_dirty_records(
        node_path=node_path,
        scope=envelope["scope"],
        updated_at_ms=envelope["ingestion_time_ms"],
        source_ref_type="batch",
        source_hash_field="source_batch_hash",
        source_hash=batch_id_hash,
        dirty_reason="new_event",
        source_lineage=session_dirty_lineage,
    )
    all_dirty_hashes = ordered_unique_any(list(dirty_hashes) + profile_dirty_hashes)
    records_to_append.extend(dirty_records)
    records_to_append.extend(event_records_to_append)
    records_to_append = [attach_memory_layer(record) for record in records_to_append]
    self.append_many(records_to_append)
    summary_refresh = {
        "status": "dirty_marked",
        "dirty_hashes": all_dirty_hashes,
        "session_dirty_hashes": dirty_hashes,
        "profile_dirty_hashes": profile_dirty_hashes,
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
        "profile_promotion_scope_available": bool(profile_node_hash),
        "profile_promotion_blocker": profile_promotion_blocker,
        "profile_promotion_summary": profile_promotion_summary[:16],
        "segments_written": len(segment_hashes),
        "summary_hash": summary_hash,
        "summary_refresh": summary_refresh,
        "node_materialization": node_materialization,
        "indexes_written": len(batch_index_terms) + entity_index_write_count,
        "entity_indexes_written": entity_index_write_count,
        **secondary_index_budget_summary(secondary_index_budget),
        "one_pass": True,
        "threshold_messages": threshold,
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
    }
