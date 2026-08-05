"""Session / envelope-key / prior-context assembly helpers.

Split out of matrixark_mcp_core.py, re-exported at core end via the dual
relative/absolute pattern (after identity + compact re-exports, which supply
MatrixArkError / identity_hashes / require_string / optional_object). No
import-time cycle; same core module reused under both import paths.
"""
from typing import Any

try:  # package path
    from .matrixark_mcp_core import (
        Json,
        MAX_PRIOR_CHARS,
        MAX_PRIOR_MESSAGES,
        MatrixArkError,
        identity_hashes,
        optional_object,
        require_string,
        scope_from_serving_record,
        scope_matches,
    )
except ImportError:  # top-level path
    from matrixark_mcp_core import (
        Json,
        MAX_PRIOR_CHARS,
        MAX_PRIOR_MESSAGES,
        MatrixArkError,
        identity_hashes,
        optional_object,
        require_string,
        scope_from_serving_record,
        scope_matches,
    )

__all__ = ['enrich_scope_with_identity', 'validate_hook', 'adapter_ensure_backend_ready', 'has_confirmation_context', 'explicit_context_pack_id', 'session_key', 'user_key', 'context_node_key', 'session_buffer_key_from_scope', 'session_buffer_key', 'messages_from_event_record', 'message_from_event_record', 'session_summary_for_events', 'collect_prior_context', 'prior_context_payload']

def enrich_scope_with_identity(scope: Json, identity: Json) -> Json:
    account_id = str(identity["account_id"])
    tenant_id = str(identity["tenant_id"])
    user_id = str(scope.get("user_id") or identity.get("user_id") or "")
    session_id = str(scope.get("session_id") or identity.get("session_id") or "")
    hashes = identity_hashes(account_id, tenant_id, user_id, session_id)
    explicit_scope_keys = {str(key) for key in scope.keys()}
    if identity.get("mode") == "api_key":
        if user_id:
            explicit_scope_keys.add("user_id")
        if session_id:
            explicit_scope_keys.add("session_id")
    enriched = {
        **scope,
        "account_id": account_id,
        "tenant_id": tenant_id,
        "tenant_hash": hashes["tenant_hash"],
        "scope_key": hashes["scope_key"],
        "_explicit_scope_keys": sorted(explicit_scope_keys),
    }
    if identity.get("agent_name") and "agent_name" not in enriched:
        enriched["agent_name"] = identity["agent_name"]
    if user_id:
        enriched["user_id"] = user_id
        enriched["user_hash"] = hashes["user_hash"]
    if session_id:
        enriched["session_id"] = session_id
        enriched["session_hash"] = hashes["session_hash"]
    return enriched


def validate_hook(hook: Json | None) -> Json | None:
    if hook is None:
        return None
    if not isinstance(hook, dict):
        raise MatrixArkError("agent_hook must be an object")
    hook_type = str(hook.get("hook_type") or "").strip()
    if hook_type and hook_type not in {
        "before_llm",
        "after_llm",
        "tool_result",
        "resource_added",
        "feedback",
        "session_commit",
    }:
        raise MatrixArkError("agent_hook.hook_type is invalid")
    if "codex_event" in hook and not isinstance(hook["codex_event"], str):
        raise MatrixArkError("agent_hook.codex_event must be a string")
    if "trigger" in hook and not isinstance(hook["trigger"], str):
        raise MatrixArkError("agent_hook.trigger must be a string")
    require_string(hook, "source")
    require_string(hook, "hook_id")
    if not isinstance(hook.get("observed_at_ms"), int):
        raise MatrixArkError("agent_hook.observed_at_ms must be an integer")
    if not isinstance(hook.get("auto_captured"), bool):
        raise MatrixArkError("agent_hook.auto_captured must be a boolean")
    if "idempotency_key" in hook and not isinstance(hook["idempotency_key"], str):
        raise MatrixArkError("agent_hook.idempotency_key must be a string")
    return hook


def adapter_ensure_backend_ready(adapter: Any, *, reason: str = "manual", probe: bool = True, timeout_ms: int | None = None) -> Json:
    """Call adapter readiness across old/new adapter signatures."""
    try:
        return adapter.ensure_backend_ready(reason=reason, probe=probe, timeout_ms=timeout_ms)
    except TypeError as exc:
        text = str(exc)
        if "unexpected keyword argument" not in text or "probe" not in text:
            raise
        return adapter.ensure_backend_ready(reason=reason)

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
        str(scope.get("account_id") or "acct_local"),
        str(scope.get("tenant_id") or "tenant_local_agent"),
        user_id,
        session_id,
    )


def session_buffer_key(envelope: Json) -> tuple[str, str, str, str]:
    return session_buffer_key_from_scope(envelope.get("scope", {}))


def messages_from_event_record(record: Json) -> list[Json]:
    messages = record.get("envelope", {}).get("messages", [])
    if isinstance(messages, list) and messages:
        normalized = []
        for message in messages:
            if not (
                isinstance(message, dict)
                and "role" in message
                and "content" in message
                and str(message.get("content") or "")
            ):
                continue
            normalized_message = {
                "role": str(message.get("role") or "user"),
                "content": str(message.get("content") or ""),
            }
            metadata = message.get("metadata") if isinstance(message.get("metadata"), dict) else {}
            if metadata:
                normalized_message["metadata"] = dict(metadata)
            original_role = str(message.get("original_role") or "").strip()
            if original_role:
                normalized_message["original_role"] = original_role
            normalized.append(normalized_message)
        if normalized:
            return normalized
    text = str(record.get("text", ""))
    if ":" in text:
        role, content = text.split(":", 1)
        role = role.strip() or "user"
        return [{"role": role if role in {"user", "assistant", "tool", "system"} else "user", "content": content.strip()}]
    return []


def message_from_event_record(record: Json) -> Json | None:
    messages = messages_from_event_record(record)
    if messages:
        return messages[0]
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


