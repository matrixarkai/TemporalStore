#!/usr/bin/env python3
"""Prior-context collection helpers for MatrixArk MCP ingestion."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_access_scope import scope_matches
    from tools.matrixark_mcp_envelope_keys import (
        context_node_key,
        explicit_context_pack_id,
        has_confirmation_context,
        session_key,
        user_key,
    )
    from tools.matrixark_mcp_identity import scope_from_serving_record
    from tools.matrixark_mcp_runtime_config import MAX_PRIOR_CHARS, MAX_PRIOR_MESSAGES
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_access_scope import scope_matches
    from matrixark_mcp_envelope_keys import (
        context_node_key,
        explicit_context_pack_id,
        has_confirmation_context,
        session_key,
        user_key,
    )
    from matrixark_mcp_identity import scope_from_serving_record
    from matrixark_mcp_runtime_config import MAX_PRIOR_CHARS, MAX_PRIOR_MESSAGES


Json = dict[str, Any]


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
    event_hashes = {record.get("event_id_hash") for record in event_records if record.get("event_id_hash") is not None}
    fallback_summary = None
    for record in reversed(all_records):
        if record.get("record_type") != "context_summary" or record.get("summary_type") != "session_l0":
            continue
        if target_key and tuple(record.get("context_node_key", [])) == target_key:
            return record
        if record.get("source_event_hash") in event_hashes and fallback_summary is None:
            fallback_summary = record
    return fallback_summary


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
            if session_key(prior_envelope) == key or scope_matches(scope_from_serving_record(record), envelope.get("scope", {})):
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
            if user_key(prior_envelope) == fallback_key or scope_matches(scope_from_serving_record(record), envelope.get("scope", {})):
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
        summary_hash = record.get("summary_hash") or record.get("node_hash")
        node_hash = record.get("node_hash")
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
                "node_hash": node_hash,
                "text": text,
            }
        )
        refs.append({"ref_type": "summary", "ref_hash": summary_hash, "node_hash": node_hash})

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

