#!/usr/bin/env python3
"""MatrixArk MCP server for LLM context ingestion and retrieval.

This is intentionally dependency-free. It implements the small JSON-RPC subset
needed by MCP clients over stdio, and keeps the storage boundary behind a local
adapter that can be replaced with TemporalStore RPC calls later.
"""

from __future__ import annotations

import argparse
import secrets
import hashlib
import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


Json = dict[str, Any]
MAX_PRIOR_MESSAGES = 8
MAX_PRIOR_CHARS = 4096
EMBEDDING_DIM = 32
DIRECT_RECORD_LOG_SHARD_SIZE = 256
MAX_CONTEXT_REF_CHARS = 4096
DEFAULT_TIME_DECAY_TOLERANCE_MS = 24 * 60 * 60 * 1000
DEFAULT_TIME_DECAY_HALFLIFE_MS = 7 * 24 * 60 * 60 * 1000
DEFAULT_TIME_WEIGHT = 0.18
DEFAULT_BUSINESS_WEIGHT = 0.22
DEFAULT_BUSINESS_TYPE_WEIGHTS: Json = {
    "confirmation": 1.0,
    "correction": 1.0,
    "approval_budget": 0.95,
    "approval": 0.95,
    "approval_state": 0.95,
    "budget": 0.9,
    "preference_update": 0.82,
    "plan_update": 0.78,
    "status_update": 0.76,
    "job_status": 0.76,
    "relationship": 0.74,
    "location": 0.7,
    "current_plan": 0.78,
    "family_profile": 0.72,
    "dialogue_batch": 0.45,
    "session": 0.45,
}
MATRIXARK_ADMIN_SCOPES = {"admin:account", "admin:tenant", "admin:api_key", "admin:sso", "admin:audit"}
MATRIXARK_CONTEXT_SCOPES = {
    "context:ingest",
    "context:retrieve",
    "context:feedback",
    "context:replay",
    "resource:ingest",
}
MATRIXARK_TOOL_SCOPES: dict[str, set[str]] = {
    "matrixark_ingest": {"context:ingest"},
    "matrixark_batch_extract": {"context:ingest"},
    "matrixark_session_commit": {"context:ingest"},
    "matrixark_retrieve": {"context:retrieve"},
    "matrixark_feedback": {"context:feedback"},
    "matrixark_replay": {"context:replay"},
    "matrixark_admin_create_account": {"admin:account"},
    "matrixark_admin_create_api_key": {"admin:api_key"},
    "matrixark_admin_rotate_api_key": {"admin:api_key"},
    "matrixark_admin_revoke_api_key": {"admin:api_key"},
    "matrixark_admin_map_sso_user": {"admin:sso"},
    "matrixark_admin_audit": {"admin:audit"},
}


def stable_hash(value: str) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFF_FFFF_FFFF_FFFF


def secret_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def make_api_key(prefix: str = "mk_test") -> str:
    return f"{prefix}_{secrets.token_urlsafe(32)}"


def now_ms() -> int:
    return int(time.time() * 1000)


def json_text(value: Json) -> Json:
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps(value, sort_keys=True),
            }
        ]
    }


class MatrixArkError(ValueError):
    pass


def require_string(data: Json, field: str) -> str:
    value = data.get(field)
    if not isinstance(value, str) or not value:
        raise MatrixArkError(f"{field} must be a non-empty string")
    return value


def require_messages(data: Json) -> list[Json]:
    messages = data.get("messages")
    if not isinstance(messages, list) or not messages:
        raise MatrixArkError("messages must be a non-empty list")
    for message in messages:
        if not isinstance(message, dict):
            raise MatrixArkError("messages entries must be objects")
        role = message.get("role")
        content = message.get("content")
        if role not in {"user", "assistant", "tool", "system"}:
            raise MatrixArkError("message role must be user, assistant, tool, or system")
        if not isinstance(content, str) or not content:
            raise MatrixArkError("message content must be a non-empty string")
    return messages


def optional_object(data: Json, field: str) -> Json:
    value = data.get(field, {})
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise MatrixArkError(f"{field} must be an object")
    return value


def optional_string(data: Json, field: str, default: str = "") -> str:
    value = data.get(field, default)
    if value is None:
        return default
    if not isinstance(value, str):
        raise MatrixArkError(f"{field} must be a string")
    return value


def optional_string_list(data: Json, field: str, default: list[str] | None = None) -> list[str]:
    value = data.get(field, default or [])
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise MatrixArkError(f"{field} must be a list of strings")
    return list(value)


def canonical_account_id(value: str) -> str:
    return value or "acct_dev"


def canonical_tenant_id(value: str) -> str:
    return value or "tenant_dev"


def identity_hashes(account_id: str, tenant_id: str, user_id: str = "", session_id: str = "") -> Json:
    tenant_hash = stable_hash(f"{account_id}:{tenant_id}")
    user_hash = stable_hash(f"{tenant_hash}:user:{user_id}") if user_id else 0
    session_hash = stable_hash(f"{tenant_hash}:session:{session_id}") if session_id else 0
    return {
        "tenant_hash": tenant_hash,
        "user_hash": user_hash,
        "session_hash": session_hash,
    }


def enrich_scope_with_identity(scope: Json, identity: Json) -> Json:
    account_id = str(identity["account_id"])
    tenant_id = str(identity["tenant_id"])
    user_id = str(scope.get("user_id") or identity.get("user_id") or "")
    session_id = str(scope.get("session_id") or identity.get("session_id") or "")
    hashes = identity_hashes(account_id, tenant_id, user_id, session_id)
    enriched = {
        **scope,
        "account_id": account_id,
        "tenant_id": tenant_id,
        "tenant_hash": hashes["tenant_hash"],
    }
    if user_id:
        enriched["user_hash"] = hashes["user_hash"]
    if session_id:
        enriched["session_hash"] = hashes["session_hash"]
    return enriched


def validate_hook(hook: Json | None) -> Json | None:
    if hook is None:
        return None
    if not isinstance(hook, dict):
        raise MatrixArkError("agent_hook must be an object")
    hook_type = require_string(hook, "hook_type")
    if hook_type not in {
        "before_llm",
        "after_llm",
        "tool_result",
        "resource_added",
        "feedback",
        "session_commit",
    }:
        raise MatrixArkError("agent_hook.hook_type is invalid")
    require_string(hook, "source")
    require_string(hook, "hook_id")
    if not isinstance(hook.get("observed_at_ms"), int):
        raise MatrixArkError("agent_hook.observed_at_ms must be an integer")
    if not isinstance(hook.get("auto_captured"), bool):
        raise MatrixArkError("agent_hook.auto_captured must be a boolean")
    if "idempotency_key" in hook and not isinstance(hook["idempotency_key"], str):
        raise MatrixArkError("agent_hook.idempotency_key must be a string")
    return hook


def has_confirmation_context(envelope: Json) -> bool:
    metadata = optional_object(envelope, "metadata")
    return bool(
        envelope.get("context_pack_id")
        or metadata.get("reply_to_context_pack_id")
        or envelope.get("accepted_refs")
        or envelope.get("rejected_refs")
    )


def explicit_context_pack_id(envelope: Json) -> str:
    metadata = optional_object(envelope, "metadata")
    value = envelope.get("context_pack_id") or metadata.get("reply_to_context_pack_id") or ""
    return str(value) if value else ""


def session_key(envelope: Json) -> tuple[Any, Any, Any]:
    scope = envelope.get("scope", {})
    return (
        scope.get("user_id", ""),
        scope.get("session_id", ""),
        scope.get("team", ""),
    )


def user_key(envelope: Json) -> tuple[Any, Any]:
    scope = envelope.get("scope", {})
    return (
        scope.get("user_id", ""),
        scope.get("team", ""),
    )


def context_node_key(envelope: Json) -> tuple[Any, Any, Any, Any]:
    scope = envelope.get("scope", {})
    return (
        scope.get("user_id", ""),
        scope.get("session_id", ""),
        scope.get("team", ""),
        scope.get("project", ""),
    )


def session_buffer_key_from_scope(scope: Json) -> tuple[str, str, str, str]:
    user_id = str(scope.get("user_id") or "")
    session_id = str(scope.get("session_id") or user_id or "")
    return (
        str(scope.get("account_id") or "acct_dev"),
        str(scope.get("tenant_id") or "tenant_dev"),
        user_id,
        session_id,
    )


def session_buffer_key(envelope: Json) -> tuple[str, str, str, str]:
    return session_buffer_key_from_scope(envelope.get("scope", {}))


def message_from_event_record(record: Json) -> Json | None:
    messages = record.get("envelope", {}).get("messages", [])
    if isinstance(messages, list) and messages:
        message = messages[0]
        if isinstance(message, dict) and "role" in message and "content" in message:
            return dict(message)
    text = str(record.get("text", ""))
    if ":" in text:
        role, content = text.split(":", 1)
        role = role.strip() or "user"
        return {"role": role if role in {"user", "assistant", "tool", "system"} else "user", "content": content.strip()}
    return None


def session_summary_for_events(level: str, event_records: list[Json], all_records: list[Json]) -> Json | None:
    if level != "session" or not event_records:
        return None
    target_key = context_node_key(event_records[0].get("envelope", {}))
    for record in reversed(all_records):
        if record.get("record_type") != "context_summary" or record.get("summary_type") != "session_l0":
            continue
        if tuple(record.get("context_node_key", [])) == target_key:
            return record
    return None


def collect_prior_context(envelope: Json, records: list[Json]) -> Json:
    pack_id = explicit_context_pack_id(envelope)
    if pack_id:
        for record in reversed(records):
            if record.get("record_type") == "context_pack_audit" and str(record.get("context_pack_id")) == pack_id:
                summary_text = str(record.get("summary_text", ""))
                refs = record.get("selected_refs", [])
                return {
                    "level": "explicit",
                    "refs": refs,
                    "messages": [],
                    "summaries": [
                        {
                            "ref_type": "context_pack",
                            "ref_hash": pack_id,
                            "text": summary_text[:MAX_PRIOR_CHARS],
                        }
                    ]
                    if summary_text
                    else [],
                    "char_count": min(len(summary_text), MAX_PRIOR_CHARS),
                    "limit": MAX_PRIOR_MESSAGES,
                }

    if has_confirmation_context(envelope):
        return {
            "level": "explicit",
            "refs": envelope.get("accepted_refs") or envelope.get("rejected_refs") or [],
            "messages": [],
            "summaries": [],
            "char_count": 0,
            "limit": MAX_PRIOR_MESSAGES,
        }

    selected_events = []
    key = session_key(envelope)
    if key[1]:
        for record in reversed(records):
            if record.get("record_type") != "context_event":
                continue
            prior_envelope = record.get("envelope", {})
            if session_key(prior_envelope) == key:
                selected_events.append(record)
                if len(selected_events) >= MAX_PRIOR_MESSAGES:
                    break
        if selected_events:
            return prior_context_payload("session", selected_events, records)

    selected_events = []
    fallback_key = user_key(envelope)
    if fallback_key[0]:
        for record in reversed(records):
            if record.get("record_type") != "context_event":
                continue
            prior_envelope = record.get("envelope", {})
            if user_key(prior_envelope) == fallback_key:
                selected_events.append(record)
                if len(selected_events) >= MAX_PRIOR_MESSAGES:
                    break
        if selected_events:
            return prior_context_payload("user", selected_events, records)

    return {"level": "", "refs": [], "messages": [], "summaries": [], "char_count": 0, "limit": MAX_PRIOR_MESSAGES}


def prior_context_payload(level: str, event_records: list[Json], all_records: list[Json]) -> Json:
    messages = []
    refs = []
    summaries = []
    char_count = 0
    session_summary = session_summary_for_events(level, event_records, all_records)
    if session_summary:
        text = str(session_summary.get("summary_text", ""))[:MAX_PRIOR_CHARS]
        char_count += len(text)
        summaries.append(
            {
                "ref_type": "session_summary",
                "ref_hash": session_summary.get("summary_hash"),
                "node_hash": session_summary.get("node_hash"),
                "text": text,
            }
        )
        refs.append(
            {
                "ref_type": "session_summary",
                "ref_hash": session_summary.get("summary_hash"),
                "node_hash": session_summary.get("node_hash"),
            }
        )

    event_hashes = {record.get("event_id_hash") for record in event_records}
    seen_summaries = set()
    for record in reversed(all_records):
        if record.get("record_type") != "context_summary":
            continue
        if record.get("source_event_hash") not in event_hashes:
            continue
        summary_hash = record.get("node_hash")
        if summary_hash in seen_summaries:
            continue
        remaining = MAX_PRIOR_CHARS - char_count
        if remaining <= 0:
            break
        text = str(record.get("summary_text", ""))[:remaining]
        char_count += len(text)
        seen_summaries.add(summary_hash)
        summaries.append(
            {
                "ref_type": "summary",
                "ref_hash": summary_hash,
                "node_hash": summary_hash,
                "text": text,
            }
        )
        refs.append({"ref_type": "summary", "ref_hash": summary_hash, "node_hash": summary_hash})

    for record in event_records:
        text = str(record.get("text", ""))
        remaining = MAX_PRIOR_CHARS - char_count
        if remaining <= 0:
            break
        clipped = text[:remaining]
        char_count += len(clipped)
        messages.append(
            {
                "ref_hash": record.get("event_id_hash"),
                "node_hash": record.get("node_hash"),
                "text": clipped,
            }
        )
        refs.append(
            {
                "ref_type": "event",
                "ref_hash": record.get("event_id_hash"),
                "node_hash": record.get("node_hash"),
            }
        )
    return {
        "level": level,
        "refs": refs,
        "messages": messages,
        "summaries": summaries,
        "char_count": char_count,
        "limit": MAX_PRIOR_MESSAGES,
    }


def compact_internal_extraction(envelope: Json, *, prior_context: Json) -> Json:
    """Rules-first internal extraction used by the local MCP MVP.

    Production MatrixArk can replace this with OSS/OpenAI/provider extraction,
    but callers still see the same Mem0-style envelope contract.
    """

    text = text_from_messages(envelope["messages"]).lower()
    if envelope["kind"] == "feedback":
        positive = any(term in text for term in ["yes", "confirmed", "approved", "correct", "looks good"])
        negative = any(term in text for term in ["no", "wrong", "incorrect", "reject", "not correct"])
        prior_level = prior_context.get("level", "")
        if not prior_level:
            return {
                "mode": "matrixark_internal",
                "classification": "AMBIGUOUS",
                "quality_warning": "short feedback lacks prior context",
                "prior_refs": [],
            }
        warning = ""
        if prior_level == "user":
            warning = "session_id missing; used user_id fallback for prior context"
        prior_refs = prior_context.get("refs", [])
        if positive:
            return {
                "mode": "matrixark_internal",
                "classification": "CONFIRMATION",
                "status": "accepted",
                "prior_context": prior_level,
                "prior_refs": prior_refs,
                "prior_message_count": len(prior_context.get("messages", [])),
                "prior_summary_count": len(prior_context.get("summaries", [])),
                "quality_warning": warning,
            }
        if negative:
            return {
                "mode": "matrixark_internal",
                "classification": "CORRECTION",
                "status": "rejected",
                "prior_context": prior_level,
                "prior_refs": prior_refs,
                "prior_message_count": len(prior_context.get("messages", [])),
                "prior_summary_count": len(prior_context.get("summaries", [])),
                "quality_warning": warning,
            }
        return {
            "mode": "matrixark_internal",
            "classification": "FEEDBACK",
            "status": "observed",
            "prior_context": prior_level,
            "prior_refs": prior_refs,
            "prior_message_count": len(prior_context.get("messages", [])),
            "prior_summary_count": len(prior_context.get("summaries", [])),
            "quality_warning": warning,
        }
    return {
        "mode": "matrixark_internal",
        "classification": "NEW_EVENT",
        "status": "observed",
        "prior_context": prior_context.get("level", ""),
        "prior_refs": prior_context.get("refs", []),
        "prior_message_count": len(prior_context.get("messages", [])),
        "prior_summary_count": len(prior_context.get("summaries", [])),
        "quality_warning": "",
    }


ONE_PASS_MEMORY_SCHEMA: Json = {
    "version": "matrixark-one-pass-memory-v1",
    "input": "logical session batch",
    "outputs": [
        "ContextEvent",
        "ContextEntity",
        "ContextSummary",
        "ContextIndex",
        "stale_blocker",
        "EntityPatch",
        "MemorySegment",
        "extraction_audit",
    ],
    "entity_types": [
        "preference",
        "relationship",
        "location",
        "job_status",
        "current_plan",
        "family_profile",
        "correction",
        "confirmation",
    ],
    "segmentation": {
        "phase_1": "semantic_saliency_filtering",
        "phase_2": "event_centric_partitioning",
        "output": "topic plus coordinate tuples over message indexes",
    },
}


def entity_patch(search: str, replace: str, *, field: str = "state") -> Json:
    return {
        "field": field,
        "patch": f"<< SEARCH\n{search}\n====\n{replace}\n>> REPLACE",
    }


def parse_entity_patch(patch_text: str) -> tuple[str, str] | None:
    match = re.search(
        r"<<\s*SEARCH\s*\n(?P<search>.*?)\n====\s*\n(?P<replace>.*?)\n>>\s*REPLACE",
        patch_text,
        flags=re.DOTALL,
    )
    if not match:
        return None
    return match.group("search").strip(), match.group("replace").strip()


def edit_distance(left: str, right: str) -> int:
    if left == right:
        return 0
    if not left:
        return len(right)
    if not right:
        return len(left)
    previous = list(range(len(right) + 1))
    for i, left_char in enumerate(left, start=1):
        current = [i]
        for j, right_char in enumerate(right, start=1):
            cost = 0 if left_char == right_char else 1
            current.append(
                min(
                    current[j - 1] + 1,
                    previous[j] + 1,
                    previous[j - 1] + cost,
                )
            )
        previous = current
    return previous[-1]


def best_span_by_edit_distance(text: str, search: str) -> tuple[int, int, float]:
    if not text or not search:
        return -1, -1, 1.0
    lower_text = text.lower()
    lower_search = search.lower()
    exact = lower_text.find(lower_search)
    if exact >= 0:
        return exact, exact + len(search), 0.0
    search_len = len(search)
    best = (-1, -1, 1.0)
    min_len = max(1, int(search_len * 0.7))
    max_len = min(len(text), max(search_len + 8, int(search_len * 1.3)))
    step = max(1, search_len // 8)
    for start in range(0, len(text), step):
        for span_len in range(min_len, max_len + 1, step):
            end = min(len(text), start + span_len)
            if end <= start:
                continue
            candidate = text[start:end]
            distance = edit_distance(candidate.lower(), lower_search)
            ratio = distance / max(len(candidate), len(search), 1)
            if ratio < best[2]:
                best = (start, end, ratio)
    return best


def apply_entity_patch(old_value: str, patch_text: str, *, max_distance_ratio: float = 0.45) -> Json:
    parsed = parse_entity_patch(patch_text)
    if not parsed:
        return {
            "updated": old_value,
            "applied": False,
            "reason": "invalid_patch",
            "distance_ratio": 1.0,
        }
    search, replace = parsed
    if not old_value:
        return {
            "updated": replace,
            "applied": True,
            "reason": "empty_old_value",
            "distance_ratio": 0.0,
        }
    start, end, ratio = best_span_by_edit_distance(old_value, search)
    if start < 0 or ratio > max_distance_ratio:
        return {
            "updated": replace,
            "applied": False,
            "reason": "no_close_span",
            "distance_ratio": round(ratio, 6),
        }
    return {
        "updated": old_value[:start] + replace + old_value[end:],
        "applied": True,
        "reason": "approximate_patch",
        "distance_ratio": round(ratio, 6),
        "span": [start, end],
    }


def apply_entity_patches(old_entity: Json | None, extracted_entity: Json) -> Json:
    state = str((old_entity or {}).get("state", ""))
    patches = extracted_entity.get("field_patches", [])
    patch_results = []
    updated_state = state or str(extracted_entity.get("state", ""))
    for patch in patches:
        if not isinstance(patch, dict) or patch.get("field", "state") != "state":
            continue
        result = apply_entity_patch(updated_state, str(patch.get("patch", "")))
        updated_state = str(result.get("updated", updated_state))
        patch_results.append(result)
    if not patches and not state:
        updated_state = str(extracted_entity.get("state", ""))
    elif not patches and state:
        new_state = str(extracted_entity.get("state", ""))
        if new_state and new_state.lower() not in state.lower():
            updated_state = summarize_text(state + " " + new_state, limit=320)
    return {
        **extracted_entity,
        "state": summarize_text(updated_state, limit=320),
        "previous_state": state,
        "patch_results": patch_results,
        "update_mode": "deterministic_eua" if patches else "merge_without_patch",
    }


def one_pass_memory_extraction(envelope: Json, *, prior_context: Json) -> Json:
    """Extract events, entities, summaries, and indexes from one batch pass.

    This mirrors the VikingMem one-pass idea: compile the desired memory outputs
    into one schema and process the input session once. The local MVP uses
    deterministic rules, while a production provider can replace this function
    with one GPT-4o-mini/OSS call that emits the same JSON shape.
    """

    messages = envelope["messages"]
    batch_text = text_from_messages(messages)
    batch_terms = tokens(batch_text)
    segments = intelligent_memory_segments(messages)
    entities = extract_batch_entities(messages, envelope)
    event_type = infer_event_type(batch_text)
    classification = "BATCH_MEMORY"
    if any(entity["entity_type"] == "confirmation" for entity in entities):
        classification = "CONFIRMATION"
    elif any(entity["entity_type"] == "correction" for entity in entities):
        classification = "CORRECTION"
    indexes = ordered_unique(
        [
            context_index_name("event_type", event_type),
            context_index_name("classification", classification),
            context_index_name("status", "observed"),
            context_index_name("source_type", envelope.get("kind", "message")),
        ]
        + [context_index_name("entity_type", entity["entity_type"]) for entity in entities]
        + [context_index_name("segment_topic", segment["topic"]) for segment in segments]
    )
    return {
        "mode": "matrixark_one_pass_schema",
        "schema": ONE_PASS_MEMORY_SCHEMA,
        "classification": classification,
        "status": "observed",
        "event_type": event_type,
        "entities": entities,
        "segments": segments,
        "indexes": indexes[:8],
        "batch_summary": summarize_text(batch_text, limit=700),
        "message_count": len(messages),
        "token_count_estimate": len(batch_terms),
        "prior_context": prior_context.get("level", ""),
        "prior_refs": prior_context.get("refs", []),
        "prior_message_count": len(prior_context.get("messages", [])),
        "prior_summary_count": len(prior_context.get("summaries", [])),
    }


def intelligent_memory_segments(messages: list[Json]) -> list[Json]:
    """Segment a batch into salient, event-centric memories.

    The production provider can emit the same coordinate tuples from one LLM
    call. The local implementation does deterministic semantic saliency and
    topic grouping, including non-contiguous segment consolidation.
    """

    salient: list[tuple[int, Json, str, str, float]] = []
    for index, message in enumerate(messages):
        text = str(message.get("content", ""))
        saliency = semantic_saliency_score(text)
        if saliency < 0.5:
            continue
        topic = infer_segment_topic(text)
        salient.append((index, message, text, topic, saliency))
    grouped: dict[str, list[tuple[int, Json, str, float]]] = {}
    for index, message, text, topic, saliency in salient:
        grouped.setdefault(topic, []).append((index, message, text, saliency))

    segments = []
    for topic, items in grouped.items():
        if not items:
            continue
        coordinate_tuples = contiguous_ranges([item[0] for item in items])
        segment_text = "\n".join(f"{index}: {text}" for index, _message, text, _score in items)
        avg_saliency = sum(item[3] for item in items) / len(items)
        segments.append(
            {
                "topic": topic,
                "coordinate_tuples": coordinate_tuples,
                "message_indexes": [item[0] for item in items],
                "saliency_score": round(avg_saliency, 6),
                "summary_text": summarize_text(segment_text, limit=420),
                "text": segment_text,
                "non_contiguous": len(coordinate_tuples) > 1,
            }
        )
    segments.sort(key=lambda item: (-item["saliency_score"], item["topic"]))
    return segments[:12]


def semantic_saliency_score(text: str) -> float:
    lower = text.lower().strip()
    if not lower:
        return 0.0
    filler = {
        "hi",
        "hello",
        "hey",
        "thanks",
        "thank you",
        "ok",
        "okay",
        "cool",
        "great",
        "sounds good",
    }
    compact = re.sub(r"[^a-z0-9 ]+", "", lower).strip()
    if compact in filler or len(compact) < 8:
        return 0.0
    score = 0.2
    if re.search(r"\b(recursion|base case|merge sort|algorithm|complexity|efficiency|dynamic programming|graph|game)\b", lower):
        score += 0.55
    if re.search(r"\b(prefer|favorite|approved|budget|plan|correction|instead|current|remember|important|moved|moving|located|location|live|lives|staying)\b", lower):
        score += 0.45
    if re.search(r"\b(is|means|because|therefore|warning|avoid|must|should|cannot|can)\b", lower):
        score += 0.2
    if len(tokens(text)) >= 8:
        score += 0.15
    return min(score, 1.0)


def infer_segment_topic(text: str) -> str:
    lower = text.lower()
    topic_keywords = [
        ("recursion", ["recursion", "recursive", "base case", "merge sort", "call stack"]),
        ("game_algorithm", ["game", "minimax", "alpha beta", "pathfinding", "npc"]),
        ("preference", ["prefer", "favorite", "likes", "loves"]),
        ("location", ["moved", "moving", "located", "location", "live", "lives", "staying"]),
        ("approval_budget", ["approved", "approval", "budget", "cost", "purchase"]),
        ("plan_status", ["plan", "current", "status", "going to", "will"]),
        ("correction", ["correction", "instead", "wrong", "changed", "updated"]),
    ]
    for topic, keywords in topic_keywords:
        if any(keyword in lower for keyword in keywords):
            return topic
    token_list = [token for token in tokens(text) if len(token) > 4]
    return token_list[0] if token_list else "general"


def contiguous_ranges(indexes: list[int]) -> list[list[int]]:
    if not indexes:
        return []
    ordered = sorted(set(indexes))
    ranges: list[list[int]] = []
    start = previous = ordered[0]
    for value in ordered[1:]:
        if value == previous + 1:
            previous = value
            continue
        ranges.append([start, previous])
        start = previous = value
    ranges.append([start, previous])
    return ranges


def infer_event_type(text: str) -> str:
    lower = text.lower()
    if any(term in lower for term in ["correct", "correction", "wrong", "instead", "updated", "changed"]):
        return "correction"
    if any(term in lower for term in ["yes", "confirmed", "approved", "looks good"]):
        return "confirmation"
    if any(term in lower for term in ["prefer", "favorite", "like", "love"]):
        return "preference_update"
    if any(term in lower for term in ["plan", "going to", "will ", "schedule"]):
        return "plan_update"
    if any(term in lower for term in ["work", "job", "role", "status", "position"]):
        return "status_update"
    return "dialogue_batch"


def extract_batch_entities(messages: list[Json], envelope: Json) -> list[Json]:
    entities: list[Json] = []
    text = text_from_messages(messages)
    lower = text.lower()
    source_event_ids = envelope.get("source_event_ids", [])
    source_refs = [str(ref) for ref in source_event_ids] if isinstance(source_event_ids, list) and source_event_ids else [str(index) for index, _ in enumerate(messages)]
    patterns = [
        ("preference", r"\b(?:prefer|prefers|favorite|likes?|loves?)\s+([^.;!?]{2,120})"),
        ("relationship", r"\b(?:friend|partner|mother|father|sister|brother|wife|husband|manager|teammate)\s+([^.;!?]{0,120})"),
        ("location", r"\b(?:live|lives|moved|moving|located|staying)\s+(?:in|to|at)?\s*([^.;!?]{2,120})"),
        ("job_status", r"\b(?:job|role|work|works|position|status)\s+(?:is|as|at|with)?\s*([^.;!?]{2,120})"),
        ("current_plan", r"\b(?:plan|plans|planning|going to|will)\s+([^.;!?]{2,140})"),
        ("family_profile", r"\b(?:family|child|children|son|daughter|pet|dog|cat)\s+([^.;!?]{0,120})"),
        ("correction", r"\b(?:correction|correct|wrong|instead|updated|changed)\s+([^.;!?]{2,140})"),
        ("approval_state", r"\b(?:approved|approval)\s+([^.;!?]{2,140})"),
        ("confirmation", r"\b(?:yes|confirmed|approved|correct|looks good)\b([^.;!?]{0,120})"),
    ]
    for entity_type, pattern in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            value = " ".join(match.group(1).split()).strip(" :-") if match.groups() else ""
            if entity_type == "confirmation" and not envelope.get("context_pack_id") and not lower.strip() in {
                "yes",
                "yes.",
                "correct",
                "correct.",
                "approved",
                "approved.",
            }:
                continue
            entity_name = canonical_entity_name(entity_type, value)
            field_patches = infer_entity_field_patches(entity_type, value, text)
            entities.append(
                {
                    "entity_type": entity_type,
                    "entity_name": entity_name or entity_type,
                    "state": summarize_text(value or text, limit=220),
                    "confidence": 0.82 if value else 0.66,
                    "source_refs": source_refs,
                    "operator": "LLM_MERGE" if entity_type not in {"confirmation", "correction"} else "LATEST",
                    "field_patches": field_patches,
                }
            )
    if not entities:
        entities.append(
            {
                "entity_type": "session",
                "entity_name": "session_memory",
                "state": summarize_text(text, limit=220),
                "confidence": 0.6,
                "source_refs": source_refs,
                "operator": "LLM_MERGE",
                "field_patches": [],
            }
        )
    return dedupe_entities(entities)


def infer_entity_field_patches(entity_type: str, value: str, text: str) -> list[Json]:
    patches: list[Json] = []
    correction = re.search(
        r"\b(?:correction|correct|wrong|updated|changed)[:\s]+([^.;!?]+?)\s+(?:instead\s+of|not)\s+([^.;!?]+)",
        text,
        flags=re.IGNORECASE,
    )
    if correction:
        replace = clean_patch_value(correction.group(1))
        search = clean_patch_value(correction.group(2))
        patches.append(entity_patch(search, replace))
    preference = re.search(
        r"\b(?:prefer|prefers|favorite|likes?|loves?)\s+([^.;!?]+?)\s+(?:now|instead\s+of|not)\s+([^.;!?]+)",
        text,
        flags=re.IGNORECASE,
    )
    if entity_type == "preference" and preference:
        replace = clean_patch_value(preference.group(1))
        search = clean_patch_value(preference.group(2))
        patches.append(entity_patch(search, replace))
    if entity_type in {"correction", "preference"} and not patches and value:
        patches.append(entity_patch("", summarize_text(value, limit=180)))
    return patches[:3]


def clean_patch_value(value: str) -> str:
    return summarize_text(" ".join(value.split()).strip(" ,;:-"), limit=180)


def canonical_entity_name(entity_type: str, value: str) -> str:
    if entity_type in {
        "preference",
        "location",
        "job_status",
        "current_plan",
        "family_profile",
        "correction",
        "confirmation",
    }:
        return entity_type
    return value[:80] if value else entity_type


def dedupe_entities(entities: list[Json]) -> list[Json]:
    seen = set()
    out = []
    for entity in entities:
        key = (entity.get("entity_type"), str(entity.get("entity_name", "")).lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(entity)
    return out[:12]


def ordered_unique(values: list[str]) -> list[str]:
    seen = set()
    out = []
    for value in values:
        value = value.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def normalized_index_value(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9_.:/-]+", "_", text)
    return text.strip("_")


def context_index_name(kind: str, value: Any) -> str:
    normalized = normalized_index_value(value)
    return f"{kind}:{normalized}" if normalized else ""


def normalize_envelope(args: Json, *, default_kind: str) -> Json:
    messages = require_messages(args)
    scope = optional_object(args, "scope")
    metadata = optional_object(args, "metadata")
    kind = args.get("kind", default_kind)
    if kind not in {"message", "feedback", "resource", "business_data"}:
        raise MatrixArkError("kind is invalid")
    envelope: Json = {
        "kind": kind,
        "messages": messages,
        "scope": scope,
        "metadata": metadata,
        "ingestion_time_ms": now_ms(),
    }
    for field in [
        "context_pack_id",
        "query_id_hash",
        "accepted_refs",
        "rejected_refs",
        "raw_uri",
        "resource_type",
        "source_event_ids",
    ]:
        if field in args:
            envelope[field] = args[field]
    return envelope


def text_from_messages(messages: list[Json]) -> str:
    return "\n".join(f"{item['role']}: {item['content']}" for item in messages)


def tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9_]+", text.lower())


def token_count(text: str) -> int:
    return len(tokens(text))


def clip_context_text(text: str, *, max_chars: int = MAX_CONTEXT_REF_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + " ...[truncated]"


def embedding_for_text(text: str) -> list[float]:
    vector = [0.0] * EMBEDDING_DIM
    for token in tokens(text):
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        index = digest[0] % EMBEDDING_DIM
        sign = 1.0 if digest[1] % 2 == 0 else -1.0
        vector[index] += sign
    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        return vector
    return [round(value / norm, 6) for value in vector]


def cosine(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    return round(sum(a * b for a, b in zip(left, right)), 6)


def clamp01(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        number = default
    return max(0.0, min(1.0, number))


def normalized_dense_score(value: float) -> float:
    return clamp01((value + 1.0) / 2.0)


def sparse_lexical_score(query_terms: set[str], text: str) -> float:
    if not query_terms:
        return 0.0
    matched = len(query_terms.intersection(tokens(text)))
    return clamp01(matched / max(len(query_terms), 1))


def infer_query_type(query: str) -> str:
    lower = query.lower()
    if re.search(r"\b(when|what date|which date|day|month|year|yesterday|tomorrow|last week|next week)\b", lower):
        return "date"
    if re.search(r"\b(current|currently|latest|now|still|today|valid|status|preference|prefer|likes|where does|where is)\b", lower):
        return "current_state"
    if re.search(r"\b(why|reason|because|feel|felt|emotion|happy|sad|angry|worried|excited)\b", lower):
        return "why_emotion"
    if re.search(r"\b(evidence|quote|exactly|what did .* say|conversation|dialogue|message)\b", lower):
        return "evidence"
    if re.search(r"\b(both|together|across|between|compare|combine|sessions|multi-hop|multi session|multi-session)\b", lower):
        return "multi_hop"
    return "fact"


def infer_secondary_index_filter_groups(query: str, question_type: str) -> list[set[str]]:
    lower = query.lower()
    groups: list[set[str]] = []

    def add_group(*terms: str) -> None:
        clean = {term for term in terms if term}
        if clean and clean not in groups:
            groups.append(clean)

    if re.search(r"\b(where|location|located|moved|moving|live|lives|city|home|staying)\b", lower):
        add_group(context_index_name("entity_type", "location"))
    if re.search(r"\b(prefer|preference|favorite|like|likes|love|loves)\b", lower):
        add_group(context_index_name("entity_type", "preference"), context_index_name("event_type", "preference_update"))
    if re.search(r"\b(friend|partner|mother|father|sister|brother|wife|husband|manager|teammate|relationship|family|child|children|son|daughter|pet)\b", lower):
        add_group(context_index_name("entity_type", "relationship"), context_index_name("entity_type", "family_profile"))
    if re.search(r"\b(job|role|work|works|position|status|company|employer)\b", lower):
        add_group(context_index_name("entity_type", "job_status"), context_index_name("event_type", "status_update"))
    if re.search(r"\b(plan|plans|planning|going to|schedule|next)\b", lower):
        add_group(context_index_name("entity_type", "current_plan"), context_index_name("event_type", "plan_update"))
    if re.search(r"\b(approval|approved|approve|confirmed|confirmation|budget|purchase|cost|gpu)\b", lower):
        add_group(
            context_index_name("event_type", "confirmation"),
            context_index_name("entity_type", "approval_state"),
            context_index_name("entity_type", "confirmation"),
            context_index_name("classification", "confirmation"),
            context_index_name("segment_topic", "approval_budget"),
        )
    if re.search(r"\b(correction|corrected|wrong|instead|updated|changed)\b", lower):
        add_group(
            context_index_name("event_type", "correction"),
            context_index_name("entity_type", "correction"),
            context_index_name("classification", "correction"),
            context_index_name("segment_topic", "correction"),
        )
    if question_type == "evidence":
        add_group(context_index_name("source_type", "message"), context_index_name("source_type", "feedback"))
    return groups


def candidate_index_terms(record: Json, index_terms_by_batch: dict[Any, list[str]], index_terms_by_node: dict[Any, list[str]]) -> set[str]:
    terms = set(index_terms_by_batch.get(record.get("batch_id_hash"), []))
    terms.update(index_terms_by_node.get(record.get("node_hash"), []))
    record_type = record.get("record_type")
    if record_type == "context_event":
        extraction = record.get("internal_extraction", {})
        envelope = record.get("envelope", {})
        terms.add(context_index_name("event_type", extraction.get("event_type")))
        terms.add(context_index_name("event_type", infer_event_type(str(record.get("text", "")))))
        terms.add(context_index_name("classification", extraction.get("classification")))
        terms.add(context_index_name("status", extraction.get("status") or "observed"))
        terms.add(context_index_name("source_type", envelope.get("kind") or "message"))
    elif record_type == "context_entity":
        terms.add(context_index_name("entity_type", record.get("entity_type")))
    elif record_type == "context_segment":
        terms.add(context_index_name("segment_topic", record.get("topic")))
    return {term for term in terms if term}


def passes_secondary_index_filters(candidate_terms: set[str], required_groups: list[set[str]]) -> bool:
    if not required_groups:
        return True
    return all(bool(candidate_terms.intersection(group)) for group in required_groups)



def hybrid_origin_score(query_terms: set[str], text: str, embedding_score: float, node_score: float) -> float:
    dense = normalized_dense_score(embedding_score)
    sparse = sparse_lexical_score(query_terms, text)
    node = normalized_dense_score(node_score)
    return round(clamp01(0.55 * dense + 0.35 * sparse + 0.10 * node), 6)


def time_decay_score(
    record_time_ms: Any,
    *,
    reference_time_ms: int,
    freshness_tolerance_ms: int,
    half_life_ms: int,
) -> float:
    try:
        event_time_ms = int(record_time_ms)
    except (TypeError, ValueError):
        return 0.5
    age_ms = max(0, reference_time_ms - event_time_ms)
    if age_ms <= freshness_tolerance_ms:
        return 1.0
    decay_age = age_ms - freshness_tolerance_ms
    half_life_ms = max(1, half_life_ms)
    # Fast initial decay, then slower long-tail decay for durable memories.
    return round(math.exp(-math.sqrt(decay_age / half_life_ms)), 6)


def business_instance_weight(*sources: Json) -> float | None:
    for source in sources:
        if not isinstance(source, dict):
            continue
        for field in ["business_weight", "business_score", "importance", "priority"]:
            if field in source:
                return clamp01(source.get(field))
    return None


def business_type_score(type_name: str, type_weights: Json) -> float:
    if not type_name:
        return 0.5
    normalized = type_name.lower()
    if normalized in type_weights:
        return clamp01(type_weights[normalized], 0.5)
    if "approval" in normalized or "budget" in normalized:
        return 0.9
    if "correction" in normalized or "confirmation" in normalized:
        return 1.0
    if "preference" in normalized or "plan" in normalized or "status" in normalized:
        return 0.75
    return 0.5


def business_score_for_candidate(candidate: Json, type_weights: Json) -> float:
    instance = business_instance_weight(candidate, candidate.get("metadata", {}), candidate.get("scope", {}))
    if instance is not None:
        return instance
    type_name = str(
        candidate.get("event_type")
        or candidate.get("entity_type")
        or candidate.get("topic")
        or candidate.get("ref_type")
        or ""
    )
    return business_type_score(type_name, type_weights)


def final_recall_score(origin_score: float, time_score: float, business_score: float, weights: Json) -> float:
    time_weight = clamp01(weights.get("time", DEFAULT_TIME_WEIGHT), DEFAULT_TIME_WEIGHT)
    business_weight = clamp01(weights.get("business", DEFAULT_BUSINESS_WEIGHT), DEFAULT_BUSINESS_WEIGHT)
    if time_weight + business_weight > 1.0:
        scale = 1.0 / (time_weight + business_weight)
        time_weight *= scale
        business_weight *= scale
    origin_weight = 1.0 - time_weight - business_weight
    return round(
        origin_weight * origin_score + time_weight * time_score + business_weight * business_score,
        6,
    )


def integer_arg(data: Json, field: str, default: int, *, minimum: int = 0) -> int:
    value = data.get(field, default)
    if not isinstance(value, int):
        raise MatrixArkError(f"{field} must be an integer")
    if value < minimum:
        raise MatrixArkError(f"{field} must be >= {minimum}")
    return value


def score_recall_candidate(candidate: Json, ranking: Json, *, reference_time_ms: int) -> Json:
    freshness_tolerance_ms = integer_arg(
        ranking,
        "freshness_tolerance_ms",
        DEFAULT_TIME_DECAY_TOLERANCE_MS,
        minimum=0,
    )
    half_life_ms = integer_arg(
        ranking,
        "half_life_ms",
        DEFAULT_TIME_DECAY_HALFLIFE_MS,
        minimum=1,
    )
    type_weights = {**DEFAULT_BUSINESS_TYPE_WEIGHTS, **optional_object(ranking, "business_type_weights")}
    weights = optional_object(ranking, "weights")
    origin_score = clamp01(candidate.get("origin_score"))
    s_time = time_decay_score(
        candidate.get("updated_at_ms"),
        reference_time_ms=reference_time_ms,
        freshness_tolerance_ms=freshness_tolerance_ms,
        half_life_ms=half_life_ms,
    )
    s_busi = business_score_for_candidate(candidate, type_weights)
    final_score = final_recall_score(origin_score, s_time, s_busi, weights)
    return {
        **candidate,
        "origin_score": origin_score,
        "time_score": s_time,
        "business_score": s_busi,
        "final_score": final_score,
        "score": final_score,
        "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    }


def numeric_field(record: Json, field: str = "value") -> float | None:
    for source in [record, record.get("metadata", {}), record.get("envelope", {}).get("metadata", {})]:
        if not isinstance(source, dict) or field not in source:
            continue
        try:
            return float(source[field])
        except (TypeError, ValueError):
            return None
    return None


def apply_statistical_operator(operator: str, records: list[Json], *, field: str = "value") -> float | int | None:
    values = [value for record in records if (value := numeric_field(record, field)) is not None]
    op = operator.upper()
    if op == "COUNT":
        return len(records)
    if not values:
        return None
    if op == "SUM":
        return round(sum(values), 6)
    if op == "AVG":
        return round(sum(values) / len(values), 6)
    if op == "MAX":
        return max(values)
    raise MatrixArkError(f"unsupported statistical operator: {operator}")


def latest_record(records: list[Json], *, time_field: str = "updated_at_ms") -> Json | None:
    if not records:
        return None
    return max(records, key=lambda record: int(record.get(time_field) or 0))


def merge_ranked_paths(primary: list[Json], auxiliary: list[Json], *, total_limit: int, auxiliary_quota: int) -> list[Json]:
    selected: list[Json] = []
    seen: set[tuple[str, Any]] = set()

    def take(items: list[Json], limit: int) -> None:
        for item in items:
            key = (str(item.get("ref_type", "")), item.get("ref_hash"))
            if key in seen:
                continue
            selected.append(item)
            seen.add(key)
            if len(selected) >= limit:
                return

    auxiliary_quota = max(0, min(auxiliary_quota, total_limit))
    primary_quota = max(0, total_limit - auxiliary_quota)
    take(primary, primary_quota)
    take(auxiliary, total_limit)
    if len(selected) < total_limit:
        take(primary, total_limit)
    return selected[:total_limit]


def question_type_ref_boost(candidate: Json, question_type: str) -> float:
    ref_type = str(candidate.get("ref_type", ""))
    text = str(candidate.get("text", "")).lower()
    event_type = str(candidate.get("event_type") or candidate.get("entity_type") or candidate.get("topic") or "").lower()
    if ref_type == "compression" and question_type in {"fact", "current_state", "multi_hop"}:
        source_count = len(candidate.get("source_event_ids", []) or [])
        return 0.32 if source_count >= 2 else 0.18
    if question_type == "current_state":
        if ref_type == "entity":
            return 0.28
        if "correction" in event_type or "confirmation" in event_type:
            return 0.16
        return 0.0
    if question_type == "evidence":
        return 0.22 if ref_type == "event" else 0.05 if ref_type == "segment" else 0.0
    if question_type == "date":
        return 0.18 if re.search(r"\b(20\d{2}|19\d{2}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", text) else 0.0
    if question_type == "multi_hop":
        return 0.14 if ref_type in {"entity", "segment"} else 0.04
    if question_type == "why_emotion":
        return 0.18 if re.search(r"\b(because|reason|felt|feel|happy|sad|angry|worried|excited|concerned)\b", text) else 0.0
    if question_type == "fact":
        return 0.14 if ref_type in {"entity", "event"} else 0.03
    return 0.0


def packing_sort_key(candidate: Json, question_type: str) -> tuple[float, float, float]:
    score = float(candidate.get("score", 0.0))
    boosted = clamp01(score + question_type_ref_boost(candidate, question_type))
    token_efficiency = boosted / max(1, token_count(str(candidate.get("text", ""))))
    return (boosted, token_efficiency, score)


def context_text_hashes(text: str) -> set[int]:
    compact = " ".join(str(text).split())
    variants = {compact[:512]}
    without_role = re.sub(r"^(user|assistant|tool|system):\s*", "", compact, flags=re.IGNORECASE)
    variants.add(without_role[:512])
    tokenized = tokens(compact)
    if tokenized:
        variants.add(" ".join(tokenized)[:512])
        if tokenized[0] in {"user", "assistant", "tool", "system"}:
            variants.add(" ".join(tokenized[1:])[:512])
    return {stable_hash(variant) for variant in variants if variant}


def local_context_budget(args: Json) -> Json:
    raw_items = args.get("local_context", [])
    if raw_items is None:
        raw_items = []
    if not isinstance(raw_items, list):
        raise MatrixArkError("local_context must be an array")
    items: list[Json] = []
    text_hashes: set[int] = set()
    token_total = 0
    for index, item in enumerate(raw_items):
        if isinstance(item, str):
            text = item
            source = f"local:{index}"
            ref_type = "local_context"
        elif isinstance(item, dict):
            text = str(item.get("text") or item.get("content") or "")
            source = str(item.get("source") or item.get("ref") or f"local:{index}")
            ref_type = str(item.get("ref_type") or "local_context")
        else:
            raise MatrixArkError("local_context items must be strings or objects")
        text = clip_context_text(text)
        if not text:
            continue
        item_tokens = token_count(text)
        token_total += item_tokens
        text_hashes.update(context_text_hashes(text))
        items.append(
            {
                "ref_type": ref_type,
                "source": source,
                "text": text,
                "token_estimate": item_tokens,
                "text_hash": stable_hash(text[:512]),
            }
        )
    explicit_tokens = args.get("local_context_tokens")
    if explicit_tokens is not None:
        if not isinstance(explicit_tokens, int) or explicit_tokens < 0:
            raise MatrixArkError("local_context_tokens must be a non-negative integer")
        token_total = max(token_total, explicit_tokens)
    return {
        "items": items,
        "token_estimate": token_total,
        "text_hashes": text_hashes,
    }


def diversify_for_question_type(candidates: list[Json], question_type: str, *, total_limit: int) -> list[Json]:
    if question_type != "multi_hop":
        return candidates[:total_limit]
    selected: list[Json] = []
    deferred: list[Json] = []
    seen_nodes: set[Any] = set()
    for candidate in candidates:
        node_hash = candidate.get("node_hash")
        if node_hash not in seen_nodes:
            selected.append(candidate)
            seen_nodes.add(node_hash)
        else:
            deferred.append(candidate)
        if len(selected) >= total_limit:
            return selected
    selected.extend(deferred)
    return selected[:total_limit]


def select_token_budgeted_refs(
    primary: list[Json],
    auxiliary: list[Json],
    *,
    max_context_tokens: int,
    auxiliary_quota: int,
    question_type: str = "fact",
    reserved_tokens: int = 0,
    duplicate_text_hashes: set[int] | None = None,
) -> tuple[list[Json], int, Json]:
    duplicate_text_hashes = duplicate_text_hashes or set()
    remote_budget = max(0, max_context_tokens - max(0, reserved_tokens))
    candidates = merge_ranked_paths(
        primary,
        auxiliary,
        total_limit=max(8, min(256, max_context_tokens)),
        auxiliary_quota=auxiliary_quota,
    )
    candidates.sort(key=lambda item: packing_sort_key(item, question_type), reverse=True)
    candidates = diversify_for_question_type(candidates, question_type, total_limit=max(8, min(256, max_context_tokens)))
    selected: list[Json] = []
    used_tokens = 0
    dropped: Json = {
        "over_budget": 0,
        "duplicate": 0,
        "low_score": 0,
        "stale": 0,
        "summary": 0,
        "raw_l2": 0,
        "estimated_tokens": {
            "over_budget": 0,
            "duplicate": 0,
            "low_score": 0,
            "stale": 0,
            "summary": 0,
            "raw_l2": 0,
        },
    }
    seen_text_hashes: set[int] = set()
    for candidate in candidates:
        ref_tokens = max(1, token_count(str(candidate.get("text", ""))))
        candidate_text_hashes = context_text_hashes(str(candidate.get("text", "")))
        if candidate_text_hashes.intersection(duplicate_text_hashes):
            dropped["duplicate"] += 1
            dropped["estimated_tokens"]["duplicate"] += ref_tokens
            continue
        text_hash = stable_hash(str(candidate.get("text", ""))[:512])
        if text_hash in seen_text_hashes:
            dropped["duplicate"] += 1
            dropped["estimated_tokens"]["duplicate"] += ref_tokens
            continue
        if float(candidate.get("score", 0.0)) < 0.04:
            dropped["low_score"] += 1
            dropped["estimated_tokens"]["low_score"] += ref_tokens
            continue
        if remote_budget <= 0 or (selected and used_tokens + ref_tokens > remote_budget):
            dropped["over_budget"] += 1
            dropped["estimated_tokens"]["over_budget"] += ref_tokens
            continue
        seen_text_hashes.add(text_hash)
        selected.append(
            {
                **candidate,
                "token_estimate": ref_tokens,
                "packing_score": round(packing_sort_key(candidate, question_type)[0], 6),
                "packing_policy": question_type,
            }
        )
        used_tokens += ref_tokens
        if used_tokens >= remote_budget:
            break
    if not selected and candidates and remote_budget > 0:
        first = next(
            (
                candidate
                for candidate in candidates
                if not context_text_hashes(str(candidate.get("text", ""))).intersection(duplicate_text_hashes)
            ),
            None,
        )
        if first is None:
            return selected, used_tokens, dropped
        clipped_words = tokens(str(first.get("text", "")))[:remote_budget]
        selected = [{**first, "text": " ".join(clipped_words), "token_estimate": len(clipped_words)}]
        used_tokens = len(clipped_words)
        dropped["over_budget"] = max(0, len(candidates) - 1)
    return selected, used_tokens, dropped


def compact_refs_for_audit(refs: list[Json], *, preview_chars: int = 160) -> list[Json]:
    compact: list[Json] = []
    keep_fields = [
        "ref_type",
        "ref_hash",
        "node_hash",
        "node_path",
        "scope",
        "recall_path",
        "score",
        "origin_score",
        "time_score",
        "business_score",
        "embedding_score",
        "sparse_score",
        "keyword_score",
        "token_estimate",
        "updated_at_ms",
        "operator",
        "source_start_ms",
        "source_end_ms",
        "source_event_ids",
    ]
    for ref in refs:
        item = {field: ref[field] for field in keep_fields if field in ref}
        text = str(ref.get("text", ""))
        if text:
            item["text_preview"] = clip_context_text(text, max_chars=preview_chars)
        compact.append(item)
    return compact


def summarize_text(text: str, *, limit: int = 220) -> str:
    compact = " ".join(text.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def normalized_node_path(envelope: Json, node_hint: list[Any]) -> list[str]:
    return [str(part) for part in node_hint if str(part)]


def node_prefixes(node_path: list[str]) -> list[list[str]]:
    return [node_path[: index + 1] for index in range(len(node_path))]


def node_path_tuple(node_path: Any) -> tuple[str, ...]:
    if not isinstance(node_path, list):
        return ()
    return tuple(str(part) for part in node_path if str(part))


def starts_with_path(path: tuple[str, ...], prefix: tuple[str, ...]) -> bool:
    return len(path) >= len(prefix) and path[: len(prefix)] == prefix


def top_scored_nodes(nodes: list[Json], limit: int) -> list[Json]:
    return sorted(
        nodes,
        key=lambda item: (-float(item.get("score", 0.0)), int(item.get("depth", 0)), str(item.get("node_path", []))),
    )[:limit]


def tree_first_traversal(
    node_scores: dict[int, Json],
    *,
    top_k_per_layer: int,
    max_children_scored_per_parent: int,
) -> Json:
    """Traverse ContextNode summaries layer by layer and return selected subtrees.

    The current Python runtime infers ContextNode children from node_path prefixes.
    C++ can later replace this with native ContextChildRef/list-children APIs while
    preserving the retrieval contract.
    """
    node_by_path: dict[tuple[str, ...], Json] = {}
    children_by_parent: dict[tuple[str, ...], list[Json]] = {}
    for node in node_scores.values():
        path = node_path_tuple(node.get("node_path", []))
        if not path:
            continue
        current = node_by_path.get(path)
        if current is None or float(node.get("score", 0.0)) > float(current.get("score", 0.0)):
            node_by_path[path] = node
    for path, node in node_by_path.items():
        parent = path[:-1]
        children_by_parent.setdefault(parent, []).append(node)

    roots = children_by_parent.get((), [])
    if not roots:
        return {
            "selected_node_hashes": set(),
            "selected_paths": set(),
            "leaf_paths": set(),
            "trace": [],
            "fallback_to_flat": True,
        }

    frontier = top_scored_nodes(roots[:max_children_scored_per_parent], top_k_per_layer)
    selected_paths: set[tuple[str, ...]] = set()
    selected_node_hashes: set[int] = set()
    leaf_paths: set[tuple[str, ...]] = set()
    trace: list[Json] = []

    while frontier:
        next_frontier: list[Json] = []
        for node in frontier:
            path = node_path_tuple(node.get("node_path", []))
            if not path:
                continue
            selected_paths.add(path)
            try:
                selected_node_hashes.add(int(node.get("node_hash")))
            except (TypeError, ValueError):
                pass
            children = children_by_parent.get(path, [])[:max_children_scored_per_parent]
            picked_children = top_scored_nodes(children, top_k_per_layer) if children else []
            trace.append(
                {
                    "node_hash": node.get("node_hash"),
                    "node_path": list(path),
                    "depth": node.get("depth", len(path)),
                    "score": node.get("score", 0.0),
                    "dense_score": node.get("dense_score", 0.0),
                    "sparse_score": node.get("sparse_score", 0.0),
                    "children_scored": len(children),
                    "children_selected": len(picked_children),
                    "selected": True,
                }
            )
            if picked_children:
                next_frontier.extend(picked_children)
            else:
                leaf_paths.add(path)
        frontier = next_frontier

    if not leaf_paths:
        leaf_paths = set(selected_paths)
    return {
        "selected_node_hashes": selected_node_hashes,
        "selected_paths": selected_paths,
        "leaf_paths": leaf_paths,
        "trace": trace,
        "fallback_to_flat": False,
    }


def scope_matches(record_scope: Json, query_scope: Json) -> bool:
    return not query_scope or all(record_scope.get(key) == value for key, value in query_scope.items())


@dataclass
class MatrixArkLocalAdapter:
    event_log: Path

    def __post_init__(self) -> None:
        self.event_log.parent.mkdir(parents=True, exist_ok=True)

    def append(self, record: Json) -> None:
        with self.event_log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")

    def read_all(self) -> list[Json]:
        if not self.event_log.exists():
            return []
        records = []
        with self.event_log.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def find_latest_entity(self, *, node_hash: int, entity_type: str, entity_name: str) -> Json | None:
        entity_hash = stable_hash(f"{node_hash}:{entity_type}:{entity_name}")
        for record in reversed(self.read_all()):
            if record.get("record_type") == "context_entity" and record.get("entity_hash") == entity_hash:
                return record
        return None

    def pending_session_events(self, scope: Json, *, limit: int | None = None) -> list[Json]:
        key = session_buffer_key_from_scope(scope)
        committed: set[int] = set()
        events: list[Json] = []
        for record in self.read_all():
            if record.get("record_type") == "context_batch_commit" and session_buffer_key_from_scope(record.get("scope", {})) == key:
                for ref in record.get("source_event_ids", []):
                    try:
                        committed.add(int(ref))
                    except (TypeError, ValueError):
                        continue
            elif record.get("record_type") == "context_event" and session_buffer_key(record.get("envelope", {})) == key:
                try:
                    event_hash = int(record.get("event_id_hash"))
                except (TypeError, ValueError):
                    continue
                if event_hash not in committed:
                    events.append(record)
        if limit is not None:
            return events[:limit]
        return events

    def append_session_buffer_event(self, *, envelope: Json, event_id_hash: int, node_hash: int, node_path: list[str], hook: Json | None) -> None:
        key = session_buffer_key(envelope)
        self.append(
            {
                "record_type": "session_buffer_event",
                "buffer_key_hash": stable_hash(":".join(key)),
                "buffer_key": list(key),
                "event_id_hash": event_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "scope": envelope["scope"],
                "status": "pending",
                "agent_hook": hook,
                "created_at_ms": envelope["ingestion_time_ms"],
            }
        )

    def default_session_node_path(self, scope: Json) -> list[str]:
        account_id = str(scope.get("account_id") or "acct_dev")
        tenant_id = str(scope.get("tenant_id") or "tenant_dev")
        user_id = str(scope.get("user_id") or "unknown_user")
        session_id = str(scope.get("session_id") or user_id or "default_session")
        return [
            f"account:{account_id}",
            f"tenant:{tenant_id}",
            f"principal:user:{user_id}",
            "collection:sessions",
            f"session:{session_id}",
        ]

    def session_commit(self, args: Json, *, hook: Json | None = None) -> Json:
        scope = optional_object(args, "scope")
        threshold = args.get("threshold_messages", 20)
        if not isinstance(threshold, int) or threshold <= 0:
            raise MatrixArkError("threshold_messages must be a positive integer")
        force = bool(args.get("force", True))
        max_messages = args.get("max_messages")
        if max_messages is not None and (not isinstance(max_messages, int) or max_messages <= 0):
            raise MatrixArkError("max_messages must be a positive integer")
        pending = self.pending_session_events(scope, limit=max_messages)
        if len(pending) < threshold and not force:
            return {
                "status": "deferred",
                "pending_event_count": len(pending),
                "threshold_messages": threshold,
                "reason": "session buffer below extraction threshold",
            }
        messages = []
        source_event_ids = []
        for record in pending:
            message = message_from_event_record(record)
            if not message:
                continue
            messages.append(message)
            source_event_ids.append(record["event_id_hash"])
        if not messages:
            return {"status": "empty", "pending_event_count": len(pending), "threshold_messages": threshold}
        metadata = optional_object(args, "metadata")
        if "node_path" not in metadata:
            metadata = {**metadata, "node_path": self.default_session_node_path(scope)}
        batch_result = self.batch_extract(
            {
                "messages": messages,
                "scope": scope,
                "metadata": metadata,
                "threshold_messages": threshold,
                "force": True,
                "derive_from_existing_events": True,
                "source_event_ids": source_event_ids,
                "skip_prior_context": bool(args.get("skip_prior_context", False)),
            },
            hook=hook,
        )
        commit_id_hash = stable_hash(f"commit:{scope}:{source_event_ids}:{now_ms()}")
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
                "agent_hook": hook,
                "created_at_ms": now_ms(),
            }
        )
        return {
            **batch_result,
            "status": "committed",
            "commit_id_hash": commit_id_hash,
            "pending_event_count": len(pending),
            "source_event_ids": source_event_ids,
            "raw_events_duplicated": False,
        }

    def node_summary_source_records(
        self,
        *,
        records: list[Json],
        node_path: list[str],
        scope: Json,
        max_events: int = 8,
        max_child_summaries: int = 8,
    ) -> tuple[list[Json], list[Json]]:
        prefix = node_path_tuple(node_path)
        child_summaries: list[Json] = []
        events: list[Json] = []
        seen_summary_keys: set[tuple[int, str]] = set()
        for record in reversed(records):
            if not scope_matches(record.get("scope", record.get("envelope", {}).get("scope", {})), scope):
                continue
            record_path = node_path_tuple(record.get("node_path", []))
            if not record_path or not starts_with_path(record_path, prefix):
                continue
            if record.get("record_type") == "context_summary" and record.get("summary_type") in {"node_l0", "node_l1", "batch_l0", "session_l0"}:
                if len(child_summaries) >= max_child_summaries:
                    continue
                try:
                    node_hash = int(record.get("node_hash"))
                except (TypeError, ValueError):
                    continue
                key = (node_hash, str(record.get("summary_type", "")))
                if key in seen_summary_keys:
                    continue
                if node_path_tuple(record.get("node_path", [])) == prefix:
                    continue
                seen_summary_keys.add(key)
                child_summaries.append(record)
            elif record.get("record_type") == "context_event":
                if len(events) >= max_events:
                    continue
                events.append(record)
        return list(reversed(events[:max_events])), list(reversed(child_summaries[:max_child_summaries]))

    def mark_node_summary_dirty(
        self,
        *,
        node_path: list[str],
        scope: Json,
        updated_at_ms: int,
        source_ref_type: str,
        source_hash_field: str,
        source_hash: int,
        dirty_reason: str = "new_event",
        propagate_depth: int | None = None,
    ) -> list[int]:
        prefixes = node_prefixes(node_path)
        if propagate_depth is not None and propagate_depth >= 0:
            prefixes = prefixes[max(0, len(prefixes) - propagate_depth - 1) :]
        dirty_hashes: list[int] = []
        for depth, prefix in enumerate(prefixes, start=1):
            node_hash = stable_hash("/".join(prefix))
            dirty_hash = stable_hash(
                f"summary_dirty:{node_hash}:{dirty_reason}:{source_ref_type}:{source_hash}:{updated_at_ms}"
            )
            dirty_hashes.append(dirty_hash)
            self.append(
                {
                    "record_type": "context_summary_dirty",
                    "dirty_hash": dirty_hash,
                    "node_hash": node_hash,
                    "node_path": prefix,
                    "depth": len(prefix),
                    "dirty_reason": dirty_reason,
                    "source_ref_type": source_ref_type,
                    source_hash_field: source_hash,
                    "changed_ref_count": 1,
                    "propagate_depth": propagate_depth if propagate_depth is not None else len(node_path),
                    "scope": scope,
                    "status": "pending",
                    "created_at_ms": updated_at_ms,
                    "updated_at_ms": updated_at_ms,
                }
            )
        return dirty_hashes

    def refresh_dirty_node_summaries(
        self,
        *,
        scope: Json,
        limit: int = 64,
        refreshed_at_ms: int | None = None,
    ) -> Json:
        refreshed_at_ms = refreshed_at_ms or now_ms()
        records = self.read_all()
        completed_dirty_hashes = {
            int(record.get("dirty_hash"))
            for record in records
            if record.get("record_type") == "context_summary_refresh_audit"
            and record.get("status") == "refreshed"
            and record.get("dirty_hash") is not None
        }
        pending_by_node: dict[int, Json] = {}
        for record in records:
            if record.get("record_type") != "context_summary_dirty":
                continue
            if not scope_matches(record.get("scope", {}), scope):
                continue
            try:
                dirty_hash = int(record.get("dirty_hash"))
                node_hash = int(record.get("node_hash"))
            except (TypeError, ValueError):
                continue
            if dirty_hash in completed_dirty_hashes:
                continue
            current = pending_by_node.get(node_hash)
            if current is None or int(record.get("updated_at_ms") or 0) >= int(current.get("updated_at_ms") or 0):
                pending_by_node[node_hash] = record
        refreshed = []
        for dirty in sorted(pending_by_node.values(), key=lambda item: int(item.get("updated_at_ms") or 0))[:limit]:
            node_path = [str(part) for part in dirty.get("node_path", [])]
            if not node_path:
                continue
            node_hash = int(dirty["node_hash"])
            events, child_summaries = self.node_summary_source_records(
                records=records,
                node_path=node_path,
                scope=dirty.get("scope", scope),
            )
            event_texts = [str(record.get("text", "")) for record in events if record.get("text")]
            child_summary_texts = [
                str(record.get("summary_text", ""))
                for record in child_summaries
                if record.get("summary_text")
            ]
            source_text = " ".join(child_summary_texts + event_texts)
            if not source_text:
                source_text = " ".join(node_path)
            prefix_label = " / ".join(node_path)
            l0_summary = summarize_text(f"{prefix_label} :: {source_text}", limit=220)
            l1_summary = summarize_text(
                f"Context node {prefix_label}. Overview: {source_text}. "
                f"This node belongs to path {prefix_label} and should be used for tree-first retrieval before leaf event/entity recall.",
                limit=1200,
            )
            source_event_ids = [int(record["event_id_hash"]) for record in events if record.get("event_id_hash") is not None]
            source_summary_hashes = [
                int(record.get("summary_hash") or record.get("node_hash"))
                for record in child_summaries
                if record.get("summary_hash") is not None or record.get("node_hash") is not None
            ]
            version_hash = stable_hash(
                f"summary_version:{node_hash}:{dirty.get('dirty_hash')}:{source_event_ids}:{source_summary_hashes}:{refreshed_at_ms}"
            )
            for level, summary_text, embedding_type in [
                ("node_l0", l0_summary, "node_l0"),
                ("node_l1", l1_summary, "node_l1"),
            ]:
                self.append(
                    {
                        "record_type": "context_summary",
                        "summary_type": level,
                        "summary_version_hash": version_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "depth": len(node_path),
                        "summary_text": summary_text,
                        "source_event_ids": source_event_ids,
                        "source_summary_hashes": source_summary_hashes,
                        "dirty_hash": dirty.get("dirty_hash"),
                        "scope": dirty.get("scope", scope),
                        "updated_at_ms": refreshed_at_ms,
                    }
                )
                self.append(
                    {
                        "record_type": "context_embedding",
                        "embedding_type": embedding_type,
                        "ref_type": "node",
                        "ref_hash": node_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "depth": len(node_path),
                        "dim": EMBEDDING_DIM,
                        "model": "matrixark-local-token-hash-v1",
                        "vector": embedding_for_text(summary_text),
                        "summary_version_hash": version_hash,
                        "dirty_hash": dirty.get("dirty_hash"),
                        "scope": dirty.get("scope", scope),
                        "updated_at_ms": refreshed_at_ms,
                    }
                )
            self.append(
                {
                    "record_type": "context_summary_refresh_audit",
                    "dirty_hash": dirty.get("dirty_hash"),
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "summary_version_hash": version_hash,
                    "source_event_ids": source_event_ids,
                    "source_summary_hashes": source_summary_hashes,
                    "status": "refreshed",
                    "worker": "matrixark-local-async-summary-worker",
                    "refreshed_at_ms": refreshed_at_ms,
                    "scope": dirty.get("scope", scope),
                }
            )
            refreshed.append(
                {
                    "dirty_hash": dirty.get("dirty_hash"),
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "summary_version_hash": version_hash,
                    "source_event_count": len(source_event_ids),
                    "source_summary_count": len(source_summary_hashes),
                }
            )
        return {
            "status": "ok",
            "refreshed_count": len(refreshed),
            "refreshed": refreshed,
        }

    def append_node_summary_embeddings(
        self,
        *,
        node_path: list[str],
        source_text: str,
        scope: Json,
        updated_at_ms: int,
        source_hash_field: str,
        source_hash: int,
    ) -> Json:
        dirty_hashes = self.mark_node_summary_dirty(
            node_path=node_path,
            scope=scope,
            updated_at_ms=updated_at_ms,
            source_ref_type=source_hash_field.removeprefix("source_").removesuffix("_hash"),
            source_hash_field=source_hash_field,
            source_hash=source_hash,
            dirty_reason="new_event",
        )
        refresh_result = self.refresh_dirty_node_summaries(
            scope=scope,
            refreshed_at_ms=updated_at_ms,
        )
        return {
            "dirty_hashes": dirty_hashes,
            "refresh_result": refresh_result,
        }

    def ingest(self, args: Json, *, hook: Json | None = None) -> Json:
        envelope = normalize_envelope(args, default_kind="message")
        hook = validate_hook(hook)
        prior_records = [] if args.get("skip_prior_context") else self.read_all()
        prior_context = (
            {"level": "", "refs": [], "messages": [], "summaries": [], "char_count": 0, "limit": MAX_PRIOR_MESSAGES}
            if args.get("skip_prior_context")
            else collect_prior_context(envelope, prior_records)
        )
        extraction = compact_internal_extraction(
            envelope,
            prior_context=prior_context,
        )
        text = text_from_messages(envelope["messages"])
        event_id_hash = stable_hash(
            f"{envelope['kind']}:{text}:{envelope['scope']}:{envelope['ingestion_time_ms']}"
        )
        node_hint = envelope["metadata"].get("node_path") or [
            envelope["scope"].get("team", "default_team"),
            envelope["scope"].get("project", "default_project"),
            envelope["kind"],
        ]
        node_path = normalized_node_path(envelope, node_hint)
        node_hash = stable_hash("/".join(node_path))
        summary_text = summarize_text(text)
        event_embedding = embedding_for_text(text)
        summary_embedding = embedding_for_text(" ".join(node_path + [summary_text]))
        session_key_parts = [str(part) for part in context_node_key(envelope)]
        if any(session_key_parts):
            session_summary_source = " ".join(
                [item.get("text", "") for item in prior_context.get("summaries", [])[:2]]
                + [item.get("text", "") for item in prior_context.get("messages", [])[:2]]
                + [text]
            )
            session_summary_text = summarize_text(session_summary_source, limit=512)
            session_summary_hash = stable_hash("session:" + "/".join(session_key_parts))
            self.append(
                {
                    "record_type": "context_summary",
                    "summary_type": "session_l0",
                    "summary_hash": session_summary_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "context_node_key": session_key_parts,
                    "summary_text": session_summary_text,
                    "source_event_hash": event_id_hash,
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            self.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": "session_l0",
                    "ref_type": "summary",
                    "ref_hash": session_summary_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": EMBEDDING_DIM,
                    "model": "matrixark-local-token-hash-v1",
                    "vector": embedding_for_text(session_summary_text),
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
        self.append(
            {
                "record_type": "context_embedding",
                "embedding_type": "event_text",
                "ref_type": "event",
                "ref_hash": event_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": EMBEDDING_DIM,
                "model": "matrixark-local-token-hash-v1",
                "vector": event_embedding,
                "scope": envelope["scope"],
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )
        record = {
            "record_type": "context_event",
            "event_id_hash": event_id_hash,
            "node_hash": node_hash,
            "node_path": node_path,
            "text": text,
            "summary_text": summary_text,
            "summary_embedding": summary_embedding,
            "envelope": envelope,
            "internal_extraction": extraction,
            "prior_context": prior_context,
            "agent_hook": hook,
        }
        self.append(record)
        self.append_session_buffer_event(envelope=envelope, event_id_hash=event_id_hash, node_hash=node_hash, node_path=node_path, hook=hook)
        summary_refresh = self.append_node_summary_embeddings(
            node_path=node_path,
            source_text=text,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
            source_hash_field="source_event_hash",
            source_hash=event_id_hash,
        )
        pending_event_count = len(self.pending_session_events(envelope["scope"]))
        auto_batch_result: Json | None = None
        auto_batch_extract = bool(args.get("auto_batch_extract", False))
        session_buffer_threshold = args.get("session_buffer_threshold", 20)
        if not isinstance(session_buffer_threshold, int) or session_buffer_threshold <= 0:
            raise MatrixArkError("session_buffer_threshold must be a positive integer")
        if auto_batch_extract and pending_event_count >= session_buffer_threshold:
            auto_batch_result = self.session_commit(
                {
                    "scope": envelope["scope"],
                    "metadata": envelope["metadata"],
                    "threshold_messages": session_buffer_threshold,
                    "force": False,
                    "skip_prior_context": bool(args.get("skip_prior_context", False)),
                },
                hook=hook,
            )
        return {
            "status": "accepted",
            "event_id_hash": event_id_hash,
            "node_hash": record["node_hash"],
            "hook_captured": hook is not None,
            "extraction_mode": extraction["mode"],
            "classification": extraction.get("classification", "UNCLASSIFIED"),
            "prior_context": extraction.get("prior_context", ""),
            "prior_refs": extraction.get("prior_refs", []),
            "prior_message_count": extraction.get("prior_message_count", 0),
            "prior_summary_count": extraction.get("prior_summary_count", 0),
            "quality_warning": extraction.get("quality_warning", ""),
            "summary_refresh": summary_refresh,
            "session_buffer": {
                "buffer_key": list(session_buffer_key(envelope)),
                "pending_event_count": pending_event_count,
                "threshold_messages": session_buffer_threshold,
                "auto_batch_extract": auto_batch_extract,
            },
            "auto_batch_extract_result": auto_batch_result,
        }

    def batch_extract(self, args: Json, *, hook: Json | None = None) -> Json:
        envelope = normalize_envelope(args, default_kind="message")
        hook = validate_hook(hook)
        threshold = args.get("threshold_messages", 20)
        force = bool(args.get("force", False))
        derive_from_existing_events = bool(args.get("derive_from_existing_events", False))
        source_event_ids = [int(ref) for ref in args.get("source_event_ids", [])] if isinstance(args.get("source_event_ids", []), list) else []
        if not isinstance(threshold, int) or threshold <= 0:
            raise MatrixArkError("threshold_messages must be a positive integer")
        if len(envelope["messages"]) < threshold and not force:
            return {
                "status": "deferred",
                "message_count": len(envelope["messages"]),
                "threshold_messages": threshold,
                "reason": "logical batch below extraction threshold",
            }

        prior_records = [] if args.get("skip_prior_context") else self.read_all()
        prior_context = (
            {"level": "", "refs": [], "messages": [], "summaries": [], "char_count": 0, "limit": MAX_PRIOR_MESSAGES}
            if args.get("skip_prior_context")
            else collect_prior_context(envelope, prior_records)
        )
        extraction = one_pass_memory_extraction(envelope, prior_context=prior_context)
        batch_text = text_from_messages(envelope["messages"])
        batch_id_hash = stable_hash(
            f"batch:{batch_text}:{envelope['scope']}:{envelope['ingestion_time_ms']}"
        )
        node_hint = envelope["metadata"].get("node_path") or [
            envelope["scope"].get("team", "default_team"),
            envelope["scope"].get("project", "default_project"),
            "session_batch",
        ]
        node_path = normalized_node_path(envelope, node_hint)
        node_hash = stable_hash("/".join(node_path))
        batch_summary = extraction["batch_summary"]

        event_hashes: list[int] = list(source_event_ids) if derive_from_existing_events else []
        if not derive_from_existing_events:
            for index, message in enumerate(envelope["messages"]):
                event_text = f"{message['role']}: {message['content']}"
                event_id_hash = stable_hash(f"{batch_id_hash}:event:{index}:{event_text}")
                event_hashes.append(event_id_hash)
                self.append(
                    {
                        "record_type": "context_event",
                        "event_id_hash": event_id_hash,
                        "batch_id_hash": batch_id_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "text": event_text,
                        "summary_text": summarize_text(event_text),
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
                    }
                )
                self.append(
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "event_text",
                        "ref_type": "event",
                        "ref_hash": event_id_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dim": EMBEDDING_DIM,
                        "model": "matrixark-local-token-hash-v1",
                        "vector": embedding_for_text(event_text),
                        "scope": envelope["scope"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )

        entity_hashes = []
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
            entity_hashes.append(entity_hash)
            self.append(
                {
                    "record_type": "context_entity",
                    "entity_hash": entity_hash,
                    "batch_id_hash": batch_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": envelope["scope"],
                    "entity_type": updated_entity["entity_type"],
                    "entity_name": updated_entity["entity_name"],
                    "state": updated_entity["state"],
                    "previous_state": updated_entity.get("previous_state", ""),
                    "confidence": updated_entity["confidence"],
                    "operator": updated_entity["operator"],
                    "source_refs": updated_entity["source_refs"],
                    "source_event_ids": source_event_ids,
                    "field_patches": updated_entity.get("field_patches", []),
                    "patch_results": updated_entity.get("patch_results", []),
                    "update_mode": updated_entity.get("update_mode", ""),
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            if updated_entity.get("patch_results"):
                self.append(
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
            self.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": "entity_state",
                    "ref_type": "entity",
                    "ref_hash": entity_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": EMBEDDING_DIM,
                    "model": "matrixark-local-token-hash-v1",
                    "vector": embedding_for_text(updated_entity["entity_type"] + " " + updated_entity["state"]),
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )

        segment_hashes = []
        for segment in extraction["segments"]:
            segment_hash = stable_hash(f"{batch_id_hash}:segment:{segment['topic']}:{segment['coordinate_tuples']}")
            segment_hashes.append(segment_hash)
            self.append(
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
                    "saliency_score": segment["saliency_score"],
                    "summary_text": segment["summary_text"],
                    "text": segment["text"],
                    "non_contiguous": segment["non_contiguous"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            self.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": "segment_text",
                    "ref_type": "segment",
                    "ref_hash": segment_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": EMBEDDING_DIM,
                    "model": "matrixark-local-token-hash-v1",
                    "vector": embedding_for_text(segment["topic"] + " " + segment["summary_text"]),
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )

        summary_hash = stable_hash(f"batch_summary:{batch_id_hash}")
        self.append(
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
                "scope": envelope["scope"],
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )
        self.append(
            {
                "record_type": "context_embedding",
                "embedding_type": "batch_l0",
                "ref_type": "summary",
                "ref_hash": summary_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": EMBEDDING_DIM,
                "model": "matrixark-local-token-hash-v1",
                "vector": embedding_for_text(" ".join(node_path + [batch_summary])),
                "scope": envelope["scope"],
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )
        for index_name in extraction["indexes"]:
            self.append(
                {
                    "record_type": "context_index",
                    "index_name": index_name,
                    "index_hash": stable_hash(f"{index_name}:{batch_id_hash}"),
                    "batch_id_hash": batch_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
        self.append(
            {
                "record_type": "context_extraction_audit",
                "batch_id_hash": batch_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "schema": extraction["schema"],
                "message_count": extraction["message_count"],
                "token_count_estimate": extraction["token_count_estimate"],
                "outputs": {
                    "events": 0 if derive_from_existing_events else len(envelope["messages"]),
                    "source_events": len(event_hashes),
                    "entities": len(entity_hashes),
                    "segments": len(segment_hashes),
                    "summaries": 1,
                    "indexes": len(extraction["indexes"]),
                },
                "mode": extraction["mode"],
                "derive_from_existing_events": derive_from_existing_events,
                "source_event_ids": event_hashes,
                "agent_hook": hook,
                "created_at_ms": now_ms(),
            }
        )
        summary_refresh = self.append_node_summary_embeddings(
            node_path=node_path,
            source_text=batch_summary,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
            source_hash_field="source_batch_hash",
            source_hash=batch_id_hash,
        )
        return {
            "status": "accepted",
            "mode": extraction["mode"],
            "classification": extraction["classification"],
            "batch_id_hash": batch_id_hash,
            "node_hash": node_hash,
            "message_count": extraction["message_count"],
            "token_count_estimate": extraction["token_count_estimate"],
            "events_written": 0 if derive_from_existing_events else len(envelope["messages"]),
            "source_event_count": len(event_hashes),
            "raw_events_duplicated": not derive_from_existing_events,
            "entities_written": len(entity_hashes),
            "segments_written": len(segment_hashes),
            "summary_hash": summary_hash,
            "summary_refresh": summary_refresh,
            "indexes_written": len(extraction["indexes"]),
            "one_pass": True,
            "threshold_messages": threshold,
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
        source_events = []
        for record in self.read_all():
            if record.get("record_type") != "context_event":
                continue
            if int(record.get("node_hash") or 0) != node_hash:
                continue
            event_scope = record.get("envelope", {}).get("scope", {})
            if not scope_matches(event_scope, scope):
                continue
            event_time = int(record.get("envelope", {}).get("ingestion_time_ms") or record.get("updated_at_ms") or 0)
            if event_time < source_start_ms or event_time > source_end_ms:
                continue
            extraction = record.get("internal_extraction", {})
            confidence = float(extraction.get("confidence", record.get("confidence", 1.0)) or 1.0)
            importance = float(record.get("envelope", {}).get("metadata", {}).get("importance", record.get("importance", 1.0)) or 1.0)
            if confidence < min_confidence or importance < min_importance:
                continue
            source_events.append(record)
        source_events.sort(key=lambda record: int(record.get("envelope", {}).get("ingestion_time_ms") or 0))
        selected = source_events[:max_source_events]
        if not selected:
            raise MatrixArkError("no source events matched compression window")
        truncated = len(source_events) > len(selected)
        source_event_ids = [int(record["event_id_hash"]) for record in selected]
        compression_scope = selected[0].get("envelope", {}).get("scope", scope)
        if not summary:
            snippets = [summarize_text(str(record.get("text", "")), limit=180) for record in selected[:5]]
            suffix = " plus additional source events" if truncated else ""
            summary = (
                f"Temporal compression window [{source_start_ms}, {source_end_ms}] contains "
                f"{len(selected)} selected events{suffix}. " + " | ".join(snippets)
            )
        compression_id_hash = stable_hash(f"compress:{scope}:{node_hash}:{source_start_ms}:{source_end_ms}:{source_event_ids}")
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
            "truncated_source_events": truncated,
            "operator": "TIME_COMPRESS",
            "updated_at_ms": compressed_time_ms,
        }
        self.append(record)
        self.append(
            {
                "record_type": "context_embedding",
                "embedding_type": "compression_summary",
                "ref_type": "compression",
                "ref_hash": compression_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": EMBEDDING_DIM,
                "model": "matrixark-local-token-hash-v1",
                "vector": embedding_for_text(record["summary_text"]),
                "scope": compression_scope,
                "updated_at_ms": compressed_time_ms,
            }
        )
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
            if not scope_matches(record.get("scope", {}), scope):
                continue
            if int(record.get("source_end_ms") or 0) >= start_time_ms and int(record.get("source_start_ms") or 0) <= end_time_ms:
                matches.append(record)
        matches.sort(key=lambda record: (int(record.get("source_end_ms") or 0), int(record.get("compressed_time_ms") or 0)), reverse=True)
        return matches[:limit]

    def retrieve(self, args: Json) -> Json:
        query = require_string(args, "query")
        scope = optional_object(args, "scope")
        ranking = optional_object(args, "ranking")
        question_type = str(args.get("question_type") or infer_query_type(query))
        secondary_index_filter_groups = infer_secondary_index_filter_groups(query, question_type)
        secondary_index_dropped_count = 0
        secondary_index_matched_count = 0
        max_context_tokens = args.get("max_context_tokens", 2048)
        if not isinstance(max_context_tokens, int) or max_context_tokens <= 0:
            raise MatrixArkError("max_context_tokens must be a positive integer")
        local_budget = local_context_budget(args)
        query_terms = {term for term in tokens(query) if len(term) > 2}
        query_embedding = embedding_for_text(query)
        raw_reference_time_ms = args.get("reference_time_ms", now_ms())
        if not isinstance(raw_reference_time_ms, int):
            raise MatrixArkError("reference_time_ms must be an integer")
        reference_time_ms = raw_reference_time_ms
        auxiliary_quota = integer_arg(ranking, "auxiliary_quota", 2, minimum=0)
        records = self.read_all()
        node_scores: dict[int, Json] = {}
        event_embedding_vectors: dict[int, list[float]] = {}
        entity_embedding_vectors: dict[int, list[float]] = {}
        segment_embedding_vectors: dict[int, list[float]] = {}
        compression_embedding_vectors: dict[int, list[float]] = {}
        index_terms_by_batch: dict[Any, list[str]] = {}
        index_terms_by_node: dict[Any, list[str]] = {}
        node_summary_text_by_hash: dict[int, str] = {}
        for record in records:
            record_type = record.get("record_type")
            if record_type == "context_index" and scope_matches(record.get("scope", {}), scope):
                index_name = str(record.get("index_name", ""))
                if index_name:
                    index_terms_by_batch.setdefault(record.get("batch_id_hash"), []).append(index_name)
                    index_terms_by_node.setdefault(record.get("node_hash"), []).append(index_name)
            if record_type == "context_summary" and scope_matches(record.get("scope", {}), scope):
                summary_type = str(record.get("summary_type", ""))
                if summary_type in {"node_l0", "node_l1", "batch_l0", "session_l0"}:
                    try:
                        node_hash_for_summary = int(record.get("node_hash"))
                    except (TypeError, ValueError):
                        continue
                    existing = node_summary_text_by_hash.get(node_hash_for_summary, "")
                    summary_text = str(record.get("summary_text", ""))
                    if len(summary_text) > len(existing):
                        node_summary_text_by_hash[node_hash_for_summary] = summary_text
        for record in records:
            record_type = record.get("record_type")
            if record_type == "context_embedding" and not scope_matches(record.get("scope", {}), scope):
                continue
            if record_type == "context_embedding" and record.get("embedding_type") in {"node_l0", "node_l1"}:
                dense_score = cosine(query_embedding, record.get("vector", []))
                node_hash = record["node_hash"]
                node_text = " ".join(record.get("node_path", [])) + " " + node_summary_text_by_hash.get(node_hash, "")
                sparse_score = sparse_lexical_score(query_terms, node_text)
                score = round(clamp01(0.72 * normalized_dense_score(dense_score) + 0.28 * sparse_score), 6)
                current = node_scores.get(node_hash)
                if current is None or score > current["score"]:
                    node_scores[node_hash] = {
                        "node_hash": node_hash,
                        "node_path": record.get("node_path", []),
                        "depth": record.get("depth", len(record.get("node_path", []))),
                        "score": score,
                        "dense_score": dense_score,
                        "sparse_score": sparse_score,
                        "embedding_type": record.get("embedding_type"),
                    }
            elif record_type == "context_embedding" and record.get("embedding_type") == "event_text":
                event_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "entity_state":
                entity_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "segment_text":
                segment_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "compression_summary":
                compression_embedding_vectors[record["ref_hash"]] = record.get("vector", [])

        top_k_per_layer = integer_arg(ranking, "top_k_per_layer", 8, minimum=1)
        max_children_scored_per_parent = integer_arg(ranking, "max_children_scored_per_parent", 10000, minimum=1)
        traversal = tree_first_traversal(
            node_scores,
            top_k_per_layer=top_k_per_layer,
            max_children_scored_per_parent=max_children_scored_per_parent,
        )
        selected_paths = traversal["selected_paths"]
        selected_leaf_paths = traversal["leaf_paths"]
        selected_node_hashes = traversal["selected_node_hashes"]

        def selected_by_tree(record: Json) -> bool:
            if traversal.get("fallback_to_flat"):
                return True
            path = node_path_tuple(record.get("node_path", []))
            if path and path in selected_paths:
                return True
            if path and any(
                starts_with_path(path, leaf_path) or starts_with_path(leaf_path, path)
                for leaf_path in selected_leaf_paths
            ):
                return True
            try:
                return int(record.get("node_hash")) in selected_node_hashes
            except (TypeError, ValueError):
                return False

        layer_scores = sorted(
            traversal["trace"] or node_scores.values(),
            key=lambda item: (item.get("depth", 0), -float(item.get("score", 0.0)), item.get("node_hash", 0)),
        )
        primary_matches = []
        auxiliary_matches = []
        for record in reversed(records):
            if record.get("record_type") != "context_event":
                continue
            envelope = record.get("envelope", {})
            record_scope = envelope.get("scope", {})
            if not scope_matches(record_scope, scope):
                continue
            if not selected_by_tree(record):
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node)
            if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            text = str(record.get("text", ""))
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            embedding_score = cosine(query_embedding, event_embedding_vectors.get(record["event_id_hash"], []))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            origin_score = hybrid_origin_score(query_terms, text, embedding_score, node_score)
            extraction = record.get("internal_extraction", {})
            event_type = str(extraction.get("event_type") or extraction.get("classification") or "")
            candidate = {
                "ref_type": "event",
                "ref_hash": record["event_id_hash"],
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "event_type": event_type,
                "metadata": envelope.get("metadata", {}),
                "scope": record_scope,
                "updated_at_ms": envelope.get("ingestion_time_ms", now_ms()),
                "text": clip_context_text(text),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate({**candidate, "recall_path": "primary_hybrid"}, ranking, reference_time_ms=reference_time_ms))
            graph_text = " ".join(record.get("node_path", []) + sorted(index_terms) + [event_type, text])
            graph_score = sparse_lexical_score(query_terms, graph_text)
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **candidate,
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        for record in reversed(records):
            if record.get("record_type") != "context_entity":
                continue
            if not scope_matches(record.get("scope", {}), scope):
                continue
            if not selected_by_tree(record):
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node)
            if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            text = f"{record.get('entity_type', '')}: {record.get('entity_name', '')} = {record.get('state', '')}"
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            embedding_score = cosine(query_embedding, entity_embedding_vectors.get(record["entity_hash"], []))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            origin_score = min(1.0, 0.12 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
            candidate = {
                "ref_type": "entity",
                "ref_hash": record["entity_hash"],
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "entity_type": record.get("entity_type", ""),
                "entity_name": record.get("entity_name", ""),
                "metadata": record.get("metadata", {}),
                "scope": record.get("scope", {}),
                "updated_at_ms": record.get("updated_at_ms", now_ms()),
                "text": clip_context_text(text),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate({**candidate, "recall_path": "primary_hybrid"}, ranking, reference_time_ms=reference_time_ms))
            graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + sorted(index_terms) + [text]))
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **candidate,
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        for record in reversed(records):
            if record.get("record_type") != "context_segment":
                continue
            if not scope_matches(record.get("scope", {}), scope):
                continue
            if not selected_by_tree(record):
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node)
            if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            text = f"{record.get('topic', '')}: {record.get('summary_text', '')}"
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            embedding_score = cosine(query_embedding, segment_embedding_vectors.get(record["segment_hash"], []))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            saliency_score = float(record.get("saliency_score", 0.0))
            origin_score = min(
                1.0,
                0.1 + 0.75 * hybrid_origin_score(query_terms, text, embedding_score, node_score) + 0.15 * saliency_score,
            )
            candidate = {
                "ref_type": "segment",
                "ref_hash": record["segment_hash"],
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "saliency_score": saliency_score,
                "topic": record.get("topic", ""),
                "coordinate_tuples": record.get("coordinate_tuples", []),
                "non_contiguous": record.get("non_contiguous", False),
                "scope": record.get("scope", {}),
                "updated_at_ms": record.get("updated_at_ms", now_ms()),
                "text": clip_context_text(str(record.get("summary_text", ""))),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate({**candidate, "recall_path": "primary_hybrid"}, ranking, reference_time_ms=reference_time_ms))
            graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + sorted(index_terms) + [record.get("topic", ""), text]))
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **candidate,
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        for record in reversed(records):
            if record.get("record_type") != "context_compression_event":
                continue
            if not scope_matches(record.get("scope", {}), scope):
                continue
            if not selected_by_tree(record):
                continue
            text = f"TIME_COMPRESS: {record.get('summary_text', '')}"
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            compression_hash = int(record.get("compression_id_hash") or 0)
            embedding_score = cosine(query_embedding, compression_embedding_vectors.get(compression_hash, embedding_for_text(text)))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            origin_score = min(1.0, 0.08 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
            candidate = {
                "ref_type": "compression",
                "ref_hash": compression_hash,
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "event_type": "time_compress",
                "operator": "TIME_COMPRESS",
                "source_event_ids": record.get("source_event_ids", []),
                "source_start_ms": record.get("source_start_ms"),
                "source_end_ms": record.get("source_end_ms"),
                "scope": record.get("scope", {}),
                "updated_at_ms": record.get("compressed_time_ms", record.get("updated_at_ms", now_ms())),
                "text": clip_context_text(text),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate({**candidate, "recall_path": "primary_time_compression"}, ranking, reference_time_ms=reference_time_ms))
            graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + [text, "time_compress"]))
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **candidate,
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        primary_matches.sort(key=lambda item: item["score"], reverse=True)
        auxiliary_matches.sort(key=lambda item: item["score"], reverse=True)
        selected, used_context_tokens, dropped_over_budget = select_token_budgeted_refs(
            primary_matches,
            auxiliary_matches,
            max_context_tokens=max_context_tokens,
            auxiliary_quota=auxiliary_quota,
            question_type=question_type,
            reserved_tokens=local_budget["token_estimate"],
            duplicate_text_hashes=local_budget["text_hashes"],
        )
        context_pack_id = stable_hash(f"{query}:{selected}:{now_ms()}")
        context_pack_id_text = str(context_pack_id)
        pack_summary = summarize_text(
            " ".join(str(item.get("text", "")) for item in selected),
            limit=512,
        )
        pack = {
            "context_pack_id": str(context_pack_id),
            "selected_refs": selected,
            "layer_scores": layer_scores[:24],
            "question_type": question_type,
            "packing_policy": f"question_type_aware:{question_type}",
            "query_embedding_model": "matrixark-local-token-hash-v1",
            "recall_policy": {
                "tree_traversal": {
                    "enabled": True,
                    "summary_embeddings": ["node_l0", "node_l1"],
                    "top_k_per_layer": top_k_per_layer,
                    "max_children_scored_per_parent": max_children_scored_per_parent,
                    "selected_node_count": len(selected_node_hashes),
                    "selected_path_count": len(selected_paths),
                    "selected_leaf_count": len(traversal.get("leaf_paths", [])),
                    "fallback_to_flat": bool(traversal.get("fallback_to_flat")),
                },
                "secondary_index_filter": {
                    "enabled": bool(secondary_index_filter_groups),
                    "required_groups": [sorted(group) for group in secondary_index_filter_groups],
                    "matched_candidate_count": secondary_index_matched_count,
                    "dropped_candidate_count": secondary_index_dropped_count,
                    "mode": "AND across groups, OR within each group",
                    "applied_before_embedding_scoring": True,
                },
                "primary_path": "tree-first hybrid dense semantic + sparse lexical after secondary-index prefilter",
                "auxiliary_path": "keyword graph inside selected tree after secondary-index prefilter",
                "time_decay": {
                    "freshness_tolerance_ms": ranking.get("freshness_tolerance_ms", DEFAULT_TIME_DECAY_TOLERANCE_MS),
                    "half_life_ms": ranking.get("half_life_ms", DEFAULT_TIME_DECAY_HALFLIFE_MS),
                },
                "weights": {
                    "time": optional_object(ranking, "weights").get("time", DEFAULT_TIME_WEIGHT),
                    "business": optional_object(ranking, "weights").get("business", DEFAULT_BUSINESS_WEIGHT),
                },
                "auxiliary_quota": auxiliary_quota,
            },
            "primary_candidate_count": len(primary_matches),
            "auxiliary_candidate_count": len(auxiliary_matches),
            "used_context_tokens": used_context_tokens,
            "used_remote_context_tokens": used_context_tokens,
            "used_local_context_tokens": local_budget["token_estimate"],
            "total_prompt_context_tokens": used_context_tokens + local_budget["token_estimate"],
            "remote_context_budget_tokens": max(0, max_context_tokens - local_budget["token_estimate"]),
            "local_context_policy": {
                "mode": "shared_budget_dedupe",
                "local_context_count": len(local_budget["items"]),
                "local_context_tokens": local_budget["token_estimate"],
                "dedupe_remote_against_local": True,
                "remote_is_additive_only_within_remaining_budget": True,
            },
            "dropped_refs": dropped_over_budget,
            "quality_warnings": [],
            "insufficient_context": not selected,
        }
        self.append(
            {
                "record_type": "context_pack_audit",
                "context_pack_id": context_pack_id_text,
                "query": query,
                "scope": scope,
                "summary_text": pack_summary,
                "selected_refs": compact_refs_for_audit(selected),
                "layer_scores": layer_scores[:24],
                "tree_traversal": pack["recall_policy"]["tree_traversal"],
                "secondary_index_filter": pack["recall_policy"]["secondary_index_filter"],
                "question_type": question_type,
                "packing_policy": pack["packing_policy"],
                "recall_policy": pack["recall_policy"],
                "local_context_policy": pack["local_context_policy"],
                "used_local_context_tokens": pack["used_local_context_tokens"],
                "used_remote_context_tokens": pack["used_remote_context_tokens"],
                "total_prompt_context_tokens": pack["total_prompt_context_tokens"],
                "remote_context_budget_tokens": pack["remote_context_budget_tokens"],
                "primary_candidate_count": len(primary_matches),
                "auxiliary_candidate_count": len(auxiliary_matches),
                "created_at_ms": now_ms(),
            }
        )
        return pack

    def feedback(self, args: Json, *, hook: Json | None = None) -> Json:
        args = {**args, "kind": "feedback"}
        return self.ingest(args, hook=hook)

    def replay(self, args: Json) -> Json:
        context_pack_id = require_string(args, "context_pack_id")
        return {
            "context_pack_id": context_pack_id,
            "events": self.read_all(),
        }


class MatrixArkTemporalStoreDirectAdapter(MatrixArkLocalAdapter):
    """MatrixArk storage adapter backed by the native C++ TemporalStore SDK.

    The MCP extraction, node/summary/event mapping, traversal scoring, feedback,
    and replay logic still live in this process. Only the record log boundary is
    replaced: every MatrixArk record is persisted as a TemporalStore hash field.
    New prefixes use a compact sharded append log: hash field = zero-padded
    sequence within a shard, hash key = records:<shard>, and a tiny string key
    stores the global record count. Older prefixes that still have a JSON
    record_index are read through the legacy path.
    """

    def __init__(
        self,
        *,
        metaserver: str,
        namespace: str,
        table: str,
        library_path: str = "",
        storage_prefix: str = "matrixark:mcp",
        request_timeout_ms: int = 20000,
        io_timeout_ms: int = 20000,
    ) -> None:
        super().__init__(Path("/tmp/matrixark-mcp-unused-direct.jsonl"))
        sdk_root = Path(__file__).resolve().parents[1] / "sdk" / "python"
        sys.path.insert(0, str(sdk_root))
        from temporalstore import Client, Options  # type: ignore

        options = Options(
            metaserver_addr=metaserver,
            namespace_name=namespace,
            table_name=table,
            request_timeout_ms=request_timeout_ms,
            io_timeout_ms=io_timeout_ms,
            max_read_retries=2,
            max_write_retries=1,
        )
        self._client = Client(options, library_path=library_path or None)
        self._storage_prefix = storage_prefix.rstrip(":")
        self._record_hash_key = f"{self._storage_prefix}:records"
        self._index_key = f"{self._storage_prefix}:record_index"
        self._count_key = f"{self._storage_prefix}:record_count"
        self._shard_size = DIRECT_RECORD_LOG_SHARD_SIZE
        self._index_cache: list[str] | None = None
        self._records_cache: list[Json] | None = None
        self._legacy_index_mode = False

    def __post_init__(self) -> None:
        # Direct adapter does not use the inherited JSONL path.
        return

    def _get_index(self) -> list[str]:
        try:
            raw = self._client.get_string(self._index_key)
        except Exception:
            return []
        if not raw:
            return []
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if not isinstance(value, list):
            return []
        return [str(item) for item in value]

    def _get_count(self) -> int:
        try:
            raw = self._client.get_string(self._count_key)
        except Exception:
            return 0
        if not raw:
            return 0
        try:
            value = int(raw)
        except ValueError:
            return 0
        return max(0, value)

    def append(self, record: Json) -> None:
        if self._records_cache is None:
            self.read_all()
        assert self._records_cache is not None
        payload = json.dumps(record, sort_keys=True, separators=(",", ":"))
        if self._legacy_index_mode:
            if self._index_cache is None:
                self._index_cache = self._get_index()
            record_id = (
                f"{len(self._index_cache):020d}:"
                f"{record.get('record_type', 'record')}:"
                f"{stable_hash(json.dumps(record, sort_keys=True))}"
            )
            self._client.hset(self._record_hash_key, record_id, payload)
            self._index_cache.append(record_id)
            self._client.put_string(self._index_key, json.dumps(self._index_cache, separators=(",", ":")))
            self._records_cache.append(record)
            return

        sequence = len(self._records_cache)
        record_key, record_id = self._record_location(sequence)
        self._client.hset(record_key, record_id, payload)
        self._client.put_string(self._count_key, str(sequence + 1))
        self._records_cache.append(record)

    def read_all(self) -> list[Json]:
        if self._records_cache is not None:
            return list(self._records_cache)
        count = self._get_count()
        if count > 0:
            self._legacy_index_mode = False
            self._records_cache = self._load_records_by_count(count)
            return list(self._records_cache)
        index = self._get_index()
        self._index_cache = index
        self._legacy_index_mode = bool(index)
        self._records_cache = self._load_records(index)
        return list(self._records_cache)

    def _load_records_by_count(self, count: int) -> list[Json]:
        records = []
        for sequence in range(count):
            record_key, record_id = self._record_location(sequence)
            try:
                payload = self._client.hget(record_key, record_id)
            except Exception:
                continue
            if not payload:
                continue
            records.append(json.loads(payload))
        return records

    def _record_location(self, sequence: int) -> tuple[str, str]:
        shard = sequence // self._shard_size
        offset = sequence % self._shard_size
        return f"{self._record_hash_key}:{shard:06d}", f"{offset:020d}"

    def _load_records(self, index: list[str]) -> list[Json]:
        records = []
        for record_id in index:
            try:
                payload = self._client.hget(self._record_hash_key, record_id)
            except Exception:
                continue
            if not payload:
                continue
            records.append(json.loads(payload))
        return records


class MatrixArkAccessManager:
    """Small MatrixArk product access layer over the same storage adapter.

    It is deliberately simple: API keys authenticate the calling app/service;
    account_id + tenant_id + user_id + session_id isolate context records.
    """

    def __init__(self, adapter: MatrixArkLocalAdapter, *, mode: str = "dev") -> None:
        if mode not in {"dev", "enforced"}:
            raise MatrixArkError("access mode must be dev or enforced")
        self.adapter = adapter
        self.mode = mode

    def authenticate(self, tool_name: str, args: Json) -> Json:
        api_key = optional_string(args, "api_key")
        scope = optional_object(args, "scope")
        required_scopes = MATRIXARK_TOOL_SCOPES.get(tool_name, set())
        if api_key:
            key_record = self.find_active_api_key(api_key)
            if not key_record:
                raise MatrixArkError("invalid or revoked MatrixArk API key")
            scopes = set(key_record.get("scopes", []))
            if not required_scopes.issubset(scopes):
                raise MatrixArkError(f"API key lacks required scope(s): {sorted(required_scopes)}")
            account_id = str(key_record["account_id"])
            tenant_id = str(key_record["tenant_id"])
            requested_account = str(scope.get("account_id", ""))
            requested_tenant = str(scope.get("tenant_id", ""))
            if requested_account and requested_account != account_id:
                raise MatrixArkError("scope.account_id does not match API key account")
            if requested_tenant and requested_tenant != tenant_id:
                raise MatrixArkError("scope.tenant_id does not match API key tenant")
            return {
                "mode": "api_key",
                "api_key_id": key_record["api_key_id"],
                "account_id": account_id,
                "tenant_id": tenant_id,
                "scopes": sorted(scopes),
                "role": key_record.get("role", "service"),
                "user_id": str(scope.get("user_id", "")),
                "session_id": str(scope.get("session_id", "")),
            }
        if self.mode == "enforced" and required_scopes:
            raise MatrixArkError("MatrixArk API key is required")
        account_id = canonical_account_id(str(scope.get("account_id", "")))
        tenant_id = canonical_tenant_id(str(scope.get("tenant_id", "")))
        return {
            "mode": "dev",
            "api_key_id": "dev",
            "account_id": account_id,
            "tenant_id": tenant_id,
            "scopes": sorted(MATRIXARK_CONTEXT_SCOPES | MATRIXARK_ADMIN_SCOPES),
            "role": "dev_admin",
            "user_id": str(scope.get("user_id", "")),
            "session_id": str(scope.get("session_id", "")),
        }

    def authorize_and_enrich(self, tool_name: str, args: Json) -> Json:
        identity = self.authenticate(tool_name, args)
        scope = optional_object(args, "scope")
        args["scope"] = enrich_scope_with_identity(scope, identity)
        args["_matrixark_auth"] = {
            "mode": identity["mode"],
            "api_key_id": identity["api_key_id"],
            "account_id": identity["account_id"],
            "tenant_id": identity["tenant_id"],
            "role": identity["role"],
        }
        return identity

    def find_active_api_key(self, api_key: str) -> Json | None:
        hashed = secret_hash(api_key)
        for record in reversed(self.adapter.read_all()):
            if record.get("record_type") != "matrixark_api_key":
                continue
            if record.get("api_key_hash") == hashed:
                return record if record.get("status") == "active" else None
        return None

    def latest_api_key_record(self, api_key_id: str) -> Json | None:
        for record in reversed(self.adapter.read_all()):
            if record.get("record_type") == "matrixark_api_key" and record.get("api_key_id") == api_key_id:
                return record
        return None

    def append_audit(self, action: str, identity: Json, *, status: str, details: Json | None = None) -> None:
        self.adapter.append(
            {
                "record_type": "matrixark_audit_log",
                "audit_id_hash": stable_hash(f"{action}:{identity.get('api_key_id')}:{now_ms()}"),
                "action": action,
                "status": status,
                "account_id": identity.get("account_id", ""),
                "tenant_id": identity.get("tenant_id", ""),
                "api_key_id": identity.get("api_key_id", ""),
                "role": identity.get("role", ""),
                "details": details or {},
                "created_at_ms": now_ms(),
            }
        )

    def create_account(self, args: Json, identity: Json) -> Json:
        account_id = canonical_account_id(optional_string(args, "account_id") or f"acct_{stable_hash(optional_string(args, 'account_name', 'account'))}")
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or "tenant_default")
        account_name = optional_string(args, "account_name", account_id)
        tenant_name = optional_string(args, "tenant_name", tenant_id)
        self.adapter.append(
            {
                "record_type": "matrixark_account",
                "account_id": account_id,
                "account_name": account_name,
                "status": "active",
                "created_by_api_key_id": identity.get("api_key_id", ""),
                "created_at_ms": now_ms(),
            }
        )
        self.adapter.append(
            {
                "record_type": "matrixark_tenant",
                "account_id": account_id,
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                **identity_hashes(account_id, tenant_id),
                "status": "active",
                "created_by_api_key_id": identity.get("api_key_id", ""),
                "created_at_ms": now_ms(),
            }
        )
        self.append_audit("admin.create_account", identity, status="ok", details={"account_id": account_id, "tenant_id": tenant_id})
        return {"status": "created", "account_id": account_id, "tenant_id": tenant_id}

    def create_api_key(self, args: Json, identity: Json) -> Json:
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        scopes = optional_string_list(args, "scopes", ["context:ingest", "context:retrieve", "context:feedback", "context:replay"])
        if not scopes:
            raise MatrixArkError("scopes must not be empty")
        role = optional_string(args, "role", "service")
        key_prefix = optional_string(args, "key_prefix", "mk_test")
        api_key = make_api_key(key_prefix)
        api_key_id = f"key_{stable_hash(api_key)}"
        record = {
            "record_type": "matrixark_api_key",
            "api_key_id": api_key_id,
            "api_key_hash": secret_hash(api_key),
            "account_id": account_id,
            "tenant_id": tenant_id,
            **identity_hashes(account_id, tenant_id),
            "scopes": sorted(set(scopes)),
            "role": role,
            "status": "active",
            "created_by_api_key_id": identity.get("api_key_id", ""),
            "created_at_ms": now_ms(),
        }
        self.adapter.append(record)
        self.append_audit("admin.create_api_key", identity, status="ok", details={"api_key_id": api_key_id, "account_id": account_id, "tenant_id": tenant_id})
        return {
            "status": "created",
            "api_key": api_key,
            "api_key_id": api_key_id,
            "account_id": account_id,
            "tenant_id": tenant_id,
            "scopes": record["scopes"],
            "warning": "Store api_key now. MatrixArk only stores its hash.",
        }

    def revoke_api_key(self, args: Json, identity: Json, *, action: str = "admin.revoke_api_key") -> Json:
        api_key_id = require_string(args, "api_key_id")
        record = self.latest_api_key_record(api_key_id)
        if not record or record.get("status") != "active":
            raise MatrixArkError("active api_key_id not found")
        revoked = {
            **record,
            "record_type": "matrixark_api_key",
            "status": "revoked",
            "revoked_by_api_key_id": identity.get("api_key_id", ""),
            "revoked_at_ms": now_ms(),
        }
        self.adapter.append(revoked)
        self.append_audit(action, identity, status="ok", details={"api_key_id": api_key_id})
        return {"status": "revoked", "api_key_id": api_key_id}

    def rotate_api_key(self, args: Json, identity: Json) -> Json:
        old_api_key_id = require_string(args, "api_key_id")
        old_record = self.latest_api_key_record(old_api_key_id)
        if old_record is None or old_record.get("status") != "active":
            raise MatrixArkError("active api_key_id not found")
        self.revoke_api_key({"api_key_id": old_api_key_id}, identity, action="admin.rotate_api_key.revoke_old")
        created = self.create_api_key(
            {
                "account_id": old_record["account_id"],
                "tenant_id": old_record["tenant_id"],
                "scopes": list(old_record.get("scopes", [])),
                "role": old_record.get("role", "service"),
                "key_prefix": optional_string(args, "key_prefix", "mk_test"),
            },
            identity,
        )
        self.append_audit("admin.rotate_api_key", identity, status="ok", details={"old_api_key_id": old_api_key_id, "new_api_key_id": created["api_key_id"]})
        return {"status": "rotated", "old_api_key_id": old_api_key_id, **created}

    def map_sso_user(self, args: Json, identity: Json) -> Json:
        provider = require_string(args, "provider")
        external_user_id = require_string(args, "external_user_id")
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        matrixark_user_id = optional_string(args, "matrixark_user_id") or f"mu_{stable_hash(f'{account_id}:{tenant_id}:{provider}:{external_user_id}')}"
        record = {
            "record_type": "matrixark_sso_user_mapping",
            "mapping_id_hash": stable_hash(f"{account_id}:{tenant_id}:{provider}:{external_user_id}"),
            "account_id": account_id,
            "tenant_id": tenant_id,
            "provider": provider,
            "external_user_id": external_user_id,
            "matrixark_user_id": matrixark_user_id,
            **identity_hashes(account_id, tenant_id, matrixark_user_id),
            "status": "active",
            "created_by_api_key_id": identity.get("api_key_id", ""),
            "created_at_ms": now_ms(),
        }
        self.adapter.append(record)
        self.append_audit("admin.map_sso_user", identity, status="ok", details={"provider": provider, "matrixark_user_id": matrixark_user_id})
        return {"status": "mapped", "matrixark_user_id": matrixark_user_id, "provider": provider, "external_user_id": external_user_id}

    def audit(self, args: Json, identity: Json) -> Json:
        limit = args.get("limit", 100)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        account_id = optional_string(args, "account_id", identity["account_id"])
        tenant_id = optional_string(args, "tenant_id", identity["tenant_id"])
        rows = [
            record
            for record in reversed(self.adapter.read_all())
            if record.get("record_type") == "matrixark_audit_log"
            and (not account_id or record.get("account_id") == account_id)
            and (not tenant_id or record.get("tenant_id") == tenant_id)
        ][:limit]
        return {"status": "ok", "audit_logs": rows, "count": len(rows)}


MESSAGE_SCHEMA: Json = {
    "type": "object",
    "description": "One chat/tool/system message. Only role and content are required.",
    "required": ["role", "content"],
    "properties": {
        "role": {"type": "string", "enum": ["user", "assistant", "tool", "system"]},
        "content": {"type": "string", "minLength": 1},
        "name": {"type": "string", "description": "Optional human/tool/agent name."},
        "created_at_ms": {
            "type": "integer",
            "description": "Optional source event time. MatrixArk still uses server ingestion time as the primary write key.",
        },
    },
    "additionalProperties": True,
}

SCOPE_SCHEMA: Json = {
    "type": "object",
    "description": "Optional memory scope. Send user_id or session_id at minimum; both together give the best user and thread grouping.",
    "properties": {
        "tenant_id": {"type": "string"},
        "user_id": {
            "type": "string",
            "description": "Optional user memory scope. Useful alone, and stronger when paired with session_id.",
        },
        "session_id": {
            "type": "string",
            "description": "Optional thread/run/session scope. Useful alone, and strongest when paired with user_id.",
        },
        "team": {"type": "string"},
        "project": {"type": "string"},
    },
    "additionalProperties": True,
}

METADATA_SCHEMA: Json = {
    "type": "object",
    "description": "Optional routing and evidence hints. MatrixArk can infer or fill these internally.",
    "properties": {
        "source": {"type": "string", "description": "Optional source name such as cursor, codex, tool, or resource parser."},
        "node_path": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Optional tree/path hint. MatrixArk may choose its own internal ContextNode path.",
        },
        "reply_to_message_id": {"type": "string", "description": "Optional message linkage for feedback."},
        "reply_to_context_pack_id": {
            "type": "string",
            "description": "Optional ContextPack linkage for confirmation/correction inference.",
        },
    },
    "additionalProperties": True,
}

AGENT_HOOK_SCHEMA: Json = {
    "type": "object",
    "description": "Optional auto-capture metadata from a host-agent hook.",
    "required": ["source", "hook_type", "hook_id", "observed_at_ms", "auto_captured"],
    "properties": {
        "source": {"type": "string", "minLength": 1},
        "hook_type": {
            "type": "string",
            "enum": [
                "before_llm",
                "after_llm",
                "tool_result",
                "resource_added",
                "feedback",
                "session_commit",
            ],
        },
        "hook_id": {"type": "string", "minLength": 1},
        "observed_at_ms": {"type": "integer"},
        "idempotency_key": {"type": "string"},
        "trigger": {"type": "string"},
        "auto_captured": {"type": "boolean"},
    },
    "additionalProperties": True,
}

API_KEY_SCHEMA: Json = {
    "type": "string",
    "description": "Optional MatrixArk API key. Required when access mode is enforced; dev mode allows omitted keys for local testing.",
}

ADMIN_ACCOUNT_PROPERTIES: Json = {
    "api_key": API_KEY_SCHEMA,
    "account_id": {"type": "string", "description": "MatrixArk account/customer id. Generated or defaulted when omitted in dev mode."},
    "account_name": {"type": "string"},
    "tenant_id": {"type": "string", "description": "MatrixArk tenant/workspace id. Defaults to tenant_default for account creation."},
    "tenant_name": {"type": "string"},
    "scope": SCOPE_SCHEMA,
}


TOOLS: list[Json] = [
    {
        "name": "matrixark_ingest",
        "description": "Ingest chat, business, tool, or resource context into MatrixArk.",
        "inputSchema": {
            "type": "object",
            "required": ["messages"],
            "properties": {
                "kind": {
                    "type": "string",
                    "enum": ["message", "feedback", "resource", "business_data"],
                    "default": "message",
                    "description": "Optional envelope kind. Defaults to message.",
                },
                "messages": {
                    "type": "array",
                    "minItems": 1,
                    "items": MESSAGE_SCHEMA,
                    "description": "Required. The only required top-level field for ingest.",
                },
                "scope": SCOPE_SCHEMA,
                "metadata": METADATA_SCHEMA,
                "agent_hook": AGENT_HOOK_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "raw_uri": {"type": "string", "description": "Optional resource URI/path when kind=resource."},
                "resource_type": {"type": "string", "description": "Optional resource type such as md, txt, pdf, url."},
                "auto_batch_extract": {
                    "type": "boolean",
                    "default": False,
                    "description": "If true, commit the same-session buffer once session_buffer_threshold pending events accumulate.",
                },
                "session_buffer_threshold": {
                    "type": "integer",
                    "default": 20,
                    "description": "Pending same-session raw event threshold for automatic one-pass batch extraction.",
                },
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_session_commit",
        "description": "Commit pending same-session raw ContextEvents into derived entities, segments, summaries, and indexes without duplicating raw events.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scope": SCOPE_SCHEMA,
                "metadata": METADATA_SCHEMA,
                "agent_hook": AGENT_HOOK_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "threshold_messages": {
                    "type": "integer",
                    "default": 20,
                    "description": "Minimum pending raw events unless force=true. Explicit session_commit defaults to force=true.",
                },
                "force": {
                    "type": "boolean",
                    "default": True,
                    "description": "Force commit even below threshold for session end/task complete hooks.",
                },
                "max_messages": {
                    "type": "integer",
                    "description": "Optional cap for how many pending raw events to commit in this batch.",
                },
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_retrieve",
        "description": "Retrieve a token-budgeted MatrixArk context pack for a raw query.",
        "inputSchema": {
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {"type": "string", "description": "Required raw user or agent query."},
                "scope": SCOPE_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "max_context_tokens": {
                    "type": "integer",
                    "default": 2048,
                    "description": "Optional shared prompt context budget for local plus MatrixArk remote context. Defaults to 2048.",
                },
                "local_context": {
                    "type": "array",
                    "description": "Optional local context already selected by Codex/Cursor, such as file snippets, open-buffer summaries, or tool output. MatrixArk dedupes remote refs against this and only fills the remaining budget.",
                    "items": {
                        "oneOf": [
                            {"type": "string"},
                            {
                                "type": "object",
                                "properties": {
                                    "text": {"type": "string"},
                                    "content": {"type": "string"},
                                    "source": {"type": "string"},
                                    "ref": {"type": "string"},
                                    "ref_type": {"type": "string"},
                                },
                                "additionalProperties": True,
                            },
                        ]
                    },
                },
                "local_context_tokens": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Optional token count for local context when the caller already counted it. MatrixArk reserves at least this many tokens before adding remote refs.",
                },
                "reference_time_ms": {
                    "type": "integer",
                    "description": "Optional retrieval clock for deterministic tests and replay.",
                },
                "ranking": {
                    "type": "object",
                    "description": "Optional multi-path recall config: time decay, business weights, and auxiliary quota.",
                    "properties": {
                        "weights": {
                            "type": "object",
                            "properties": {
                                "time": {"type": "number", "minimum": 0, "maximum": 1},
                                "business": {"type": "number", "minimum": 0, "maximum": 1},
                            },
                            "additionalProperties": True,
                        },
                        "freshness_tolerance_ms": {"type": "integer", "minimum": 0},
                        "half_life_ms": {"type": "integer", "minimum": 1},
                        "business_type_weights": {
                            "type": "object",
                            "additionalProperties": {"type": "number"},
                        },
                        "auxiliary_quota": {"type": "integer", "minimum": 0},
                    },
                    "additionalProperties": True,
                },
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_batch_extract",
        "description": "Run schema-driven one-pass memory extraction over a logical session batch.",
        "inputSchema": {
            "type": "object",
            "required": ["messages"],
            "properties": {
                "messages": {
                    "type": "array",
                    "minItems": 1,
                    "items": MESSAGE_SCHEMA,
                    "description": "Session/message batch. Best quality is usually >= 20 messages or explicit force/session_commit.",
                },
                "scope": SCOPE_SCHEMA,
                "metadata": METADATA_SCHEMA,
                "agent_hook": AGENT_HOOK_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "threshold_messages": {
                    "type": "integer",
                    "default": 20,
                    "description": "Default extraction threshold. Below this, extraction is deferred unless force=true.",
                },
                "force": {
                    "type": "boolean",
                    "default": False,
                    "description": "Force one-pass extraction even when the batch is below threshold.",
                },
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_feedback",
        "description": "Capture final answer feedback, confirmations, corrections, and accepted refs.",
        "inputSchema": {
            "type": "object",
            "required": ["messages"],
            "properties": {
                "messages": {
                    "type": "array",
                    "minItems": 1,
                    "items": MESSAGE_SCHEMA,
                    "description": "Required. Feedback text or final answer messages.",
                },
                "scope": SCOPE_SCHEMA,
                "metadata": METADATA_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "context_pack_id": {
                    "type": "string",
                    "description": "Optional but strongly recommended for confirmation/correction inference.",
                },
                "accepted_refs": {
                    "type": "array",
                    "description": "Optional refs the user/agent accepted from the prior ContextPack.",
                },
                "rejected_refs": {
                    "type": "array",
                    "description": "Optional refs the user/agent rejected from the prior ContextPack.",
                },
                "agent_hook": AGENT_HOOK_SCHEMA,
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_replay",
        "description": "Replay locally captured MatrixArk events for a context pack.",
        "inputSchema": {
            "type": "object",
            "required": ["context_pack_id"],
            "properties": {"context_pack_id": {"type": "string"}, "scope": SCOPE_SCHEMA, "api_key": API_KEY_SCHEMA},
        },
    },
    {
        "name": "matrixark_admin_create_account",
        "description": "Create a MatrixArk account and default tenant.",
        "inputSchema": {
            "type": "object",
            "properties": ADMIN_ACCOUNT_PROPERTIES,
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_create_api_key",
        "description": "Create a MatrixArk API key for an account/tenant. The raw key is returned once.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "scopes": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Allowed scopes such as context:ingest, context:retrieve, admin:api_key.",
                },
                "role": {"type": "string", "default": "service"},
                "key_prefix": {"type": "string", "default": "mk_test"},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_rotate_api_key",
        "description": "Revoke an active MatrixArk API key and create a replacement with the same scopes.",
        "inputSchema": {
            "type": "object",
            "required": ["api_key_id"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "api_key_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "key_prefix": {"type": "string", "default": "mk_test"},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_revoke_api_key",
        "description": "Revoke a MatrixArk API key.",
        "inputSchema": {
            "type": "object",
            "required": ["api_key_id"],
            "properties": {"api_key": API_KEY_SCHEMA, "api_key_id": {"type": "string"}, "scope": SCOPE_SCHEMA},
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_map_sso_user",
        "description": "Map an external Okta/Google/Azure AD user id to a MatrixArk user id.",
        "inputSchema": {
            "type": "object",
            "required": ["provider", "external_user_id"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "provider": {"type": "string", "description": "okta, google, azure_ad, or another IdP name."},
                "external_user_id": {"type": "string"},
                "matrixark_user_id": {"type": "string"},
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_audit",
        "description": "List MatrixArk access-management audit records.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "limit": {"type": "integer", "default": 100},
            },
            "additionalProperties": True,
        },
    },
]


class MatrixArkMcpServer:
    def __init__(self, adapter: MatrixArkLocalAdapter, *, line_json: bool = False, access_mode: str = "dev") -> None:
        self.adapter = adapter
        self.line_json = line_json
        self.access = MatrixArkAccessManager(adapter, mode=access_mode)

    def handle(self, request: Json) -> Json | None:
        method = request.get("method")
        request_id = request.get("id")
        try:
            if method == "initialize":
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "protocolVersion": "2025-06-18",
                        "serverInfo": {"name": "matrixark-context", "version": "0.1.0"},
                        "capabilities": {"tools": {}},
                    },
                }
            if method == "notifications/initialized":
                return None
            if method == "tools/list":
                return {"jsonrpc": "2.0", "id": request_id, "result": {"tools": TOOLS}}
            if method == "tools/call":
                params = request.get("params", {})
                name = params.get("name")
                args = params.get("arguments", {})
                if not isinstance(args, dict):
                    raise MatrixArkError("tool arguments must be an object")
                result = self.call_tool(name, args)
                return {"jsonrpc": "2.0", "id": request_id, "result": json_text(result)}
            raise MatrixArkError(f"unsupported method {method!r}")
        except Exception as exc:  # MCP errors should stay JSON-RPC shaped.
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32000, "message": str(exc)},
            }

    def call_tool(self, name: str, args: Json) -> Json:
        hook = args.pop("agent_hook", None)
        identity = self.access.authorize_and_enrich(name, args)
        if name == "matrixark_ingest":
            result = self.adapter.ingest(args, hook=hook)
            self.access.append_audit("context.ingest", identity, status="ok", details={"event_id_hash": result.get("event_id_hash")})
            return {**result, "access": args.get("_matrixark_auth", {})}
        if name == "matrixark_batch_extract":
            result = self.adapter.batch_extract(args, hook=hook)
            self.access.append_audit("context.batch_extract", identity, status="ok", details={"batch_id_hash": result.get("batch_id_hash")})
            return {**result, "access": args.get("_matrixark_auth", {})}
        if name == "matrixark_session_commit":
            result = self.adapter.session_commit(args, hook=hook)
            self.access.append_audit("context.session_commit", identity, status="ok", details={"commit_id_hash": result.get("commit_id_hash"), "batch_id_hash": result.get("batch_id_hash")})
            return {**result, "access": args.get("_matrixark_auth", {})}
        if name == "matrixark_retrieve":
            result = self.adapter.retrieve(args)
            self.access.append_audit("context.retrieve", identity, status="ok", details={"context_pack_id": result.get("context_pack_id")})
            return {**result, "access": args.get("_matrixark_auth", {})}
        if name == "matrixark_feedback":
            result = self.adapter.feedback(args, hook=hook)
            self.access.append_audit("context.feedback", identity, status="ok", details={"event_id_hash": result.get("event_id_hash")})
            return {**result, "access": args.get("_matrixark_auth", {})}
        if name == "matrixark_replay":
            result = self.adapter.replay(args)
            self.access.append_audit("context.replay", identity, status="ok", details={"context_pack_id": args.get("context_pack_id")})
            return {**result, "access": args.get("_matrixark_auth", {})}
        if name == "matrixark_admin_create_account":
            return self.access.create_account(args, identity)
        if name == "matrixark_admin_create_api_key":
            return self.access.create_api_key(args, identity)
        if name == "matrixark_admin_rotate_api_key":
            return self.access.rotate_api_key(args, identity)
        if name == "matrixark_admin_revoke_api_key":
            return self.access.revoke_api_key(args, identity)
        if name == "matrixark_admin_map_sso_user":
            return self.access.map_sso_user(args, identity)
        if name == "matrixark_admin_audit":
            return self.access.audit(args, identity)
        raise MatrixArkError(f"unsupported tool {name!r}")

    def read_message(self) -> Json | None:
        if self.line_json:
            line = sys.stdin.readline()
            if not line:
                return None
            line = line.strip()
            if not line:
                return {}
            return json.loads(line)

        first = sys.stdin.buffer.readline()
        if not first:
            return None
        if not first.strip():
            return {}
        if not first.lower().startswith(b"content-length:"):
            return json.loads(first.decode("utf-8"))

        length = int(first.split(b":", 1)[1].strip())
        while True:
            header = sys.stdin.buffer.readline()
            if header in {b"\r\n", b"\n", b""}:
                break
        body = sys.stdin.buffer.read(length)
        return json.loads(body.decode("utf-8"))

    def write_response(self, response: Json) -> None:
        payload = json.dumps(response, sort_keys=True)
        if self.line_json:
            sys.stdout.write(payload + "\n")
            sys.stdout.flush()
            return
        body = payload.encode("utf-8")
        sys.stdout.buffer.write(f"Content-Length: {len(body)}\r\n\r\n".encode("ascii"))
        sys.stdout.buffer.write(body)
        sys.stdout.buffer.flush()

    def serve(self) -> None:
        while True:
            request = self.read_message()
            if request is None:
                return
            if not request:
                continue
            response = self.handle(request)
            if response is not None:
                self.write_response(response)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backend",
        choices=["local", "temporalstore-direct"],
        default=os.environ.get("MATRIXARK_MCP_BACKEND", "local"),
        help="Storage backend. local uses JSONL; temporalstore-direct uses the native C++ TemporalStore SDK.",
    )
    parser.add_argument(
        "--event-log",
        type=Path,
        default=Path("/tmp/matrixark-mcp-events.jsonl"),
        help="JSONL event log used by the local adapter.",
    )
    parser.add_argument(
        "--line-json",
        action="store_true",
        help="Use newline-delimited JSON for simple shell debugging instead of MCP framing.",
    )
    parser.add_argument(
        "--access-mode",
        choices=["dev", "enforced"],
        default=os.environ.get("MATRIXARK_ACCESS_MODE", "dev"),
        help="dev allows omitted API keys for local testing; enforced requires scoped MatrixArk API keys.",
    )
    parser.add_argument(
        "--metaserver",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_METASERVER", "127.0.0.1:18000"),
        help="C++ TemporalStore metaserver address for --backend temporalstore-direct.",
    )
    parser.add_argument(
        "--namespace",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_NAMESPACE", "deploy_ns"),
        help="TemporalStore namespace for --backend temporalstore-direct.",
    )
    parser.add_argument(
        "--table",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_TABLE", "deploy_table"),
        help="TemporalStore table for --backend temporalstore-direct.",
    )
    parser.add_argument(
        "--temporalstore-lib",
        default=os.environ.get("TEMPORALSTORE_LIB", ""),
        help="Path to libbcache2.so for --backend temporalstore-direct.",
    )
    parser.add_argument(
        "--storage-prefix",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_PREFIX", "matrixark:mcp"),
        help="TemporalStore key prefix for MatrixArk records.",
    )
    parser.add_argument(
        "--request-timeout-ms",
        type=int,
        default=int(os.environ.get("MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS", "20000")),
        help="Per-request timeout for the native C++ TemporalStore SDK.",
    )
    parser.add_argument(
        "--io-timeout-ms",
        type=int,
        default=int(os.environ.get("MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS", "20000")),
        help="BRPC I/O timeout for the native C++ TemporalStore SDK.",
    )
    args = parser.parse_args()
    if args.backend == "temporalstore-direct":
        adapter = MatrixArkTemporalStoreDirectAdapter(
            metaserver=args.metaserver,
            namespace=args.namespace,
            table=args.table,
            library_path=args.temporalstore_lib,
            storage_prefix=args.storage_prefix,
            request_timeout_ms=args.request_timeout_ms,
            io_timeout_ms=args.io_timeout_ms,
        )
    else:
        adapter = MatrixArkLocalAdapter(args.event_log)
    MatrixArkMcpServer(adapter, line_json=args.line_json, access_mode=args.access_mode).serve()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
