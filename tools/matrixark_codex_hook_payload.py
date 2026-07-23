#!/usr/bin/env python3
"""Shared Codex hook payload extraction for MatrixArk direct publishers."""

from __future__ import annotations

import json
import re
from typing import Any, Mapping

Json = dict[str, Any]

PROMPT_KEYS = (
    "prompt",
    "message",
    "text",
    "input",
    "input_prompt",
    "input-prompt",
    "inputMessages",
    "input_messages",
    "input-messages",
    "user_prompt",
    "user-prompt",
    "userPrompt",
)

SESSION_KEYS = ("session_id", "session-id", "sessionId", "conversation_id", "conversation-id", "conversationId")
THREAD_KEYS = ("thread_id", "thread-id", "threadId", "codex_thread_id", "codex-thread-id", "codexThreadId")
TURN_KEYS = ("turn_id", "turn-id", "turnId")
ENV_SESSION_KEYS = ("CODEX_SESSION_ID", "CODEX_CONVERSATION_ID", "MATRIXARK_HOOK_SESSION_ID")
ENV_THREAD_KEYS = ("CODEX_THREAD_ID", "CODEX_TASK_ID")
ENV_TURN_KEYS = ("CODEX_TURN_ID",)
NESTED_KEYS = ("hookInput", "hook_input", "payload", "event", "data", "params", "turn", "metadata")
FIELD_BOUNDARY_KEYS = PROMPT_KEYS + SESSION_KEYS + THREAD_KEYS + TURN_KEYS + (
    "type",
    "cwd",
    "client",
    "model",
    "hook_event_name",
    "hook-event-name",
    "transcript_path",
    "transcript-path",
    "last_assistant_message",
    "last-assistant-message",
)


def decode_payload(raw: bytes) -> Any:
    if len(raw) > 3 and raw[1] == 0 and raw[3] == 0:
        text = raw.decode("utf-16le", "replace")
    else:
        text = raw.decode("utf-8-sig", "replace")
    stripped = text.strip()
    if not stripped:
        return {}
    try:
        return json.loads(stripped)
    except Exception:
        return stripped


def _strip_loose_envelope(text: str) -> str:
    body = str(text or "").strip().lstrip("\ufeff").strip()
    if body.startswith("--"):
        body = body[2:].strip()
    for _ in range(3):
        changed = False
        if body.startswith("{") and body.endswith("}"):
            body = body[1:-1].strip()
            changed = True
        if body.startswith('"') and body.endswith('"'):
            body = body[1:-1].strip()
            changed = True
        if not changed:
            break
    return body.replace('\\"', '"')


def _find_loose_key(body: str, key: str) -> re.Match[str] | None:
    return re.search(r'(?:^|[,{]\s*)"?(' + re.escape(key) + r')"?\s*:', body)


def loose_field(text: Any, keys: tuple[str, ...] | list[str]) -> str:
    if not isinstance(text, str) or not text.strip():
        return ""
    body = _strip_loose_envelope(text)
    boundary = "|".join(re.escape(key) for key in FIELD_BOUNDARY_KEYS)
    for key in keys:
        match = _find_loose_key(body, key)
        if not match:
            continue
        value_start = match.end()
        while value_start < len(body) and body[value_start].isspace():
            value_start += 1
        if value_start < len(body) and body[value_start] == "[":
            depth = 0
            for pos in range(value_start, len(body)):
                char = body[pos]
                if char == "[":
                    depth += 1
                elif char == "]":
                    depth -= 1
                    if depth == 0:
                        return body[value_start + 1 : pos].strip()
            return body[value_start + 1 :].strip().rstrip('}"').strip()
        next_match = re.search(r',\s*"?(?:' + boundary + r')"?\s*:', body[value_start:])
        value_end = value_start + next_match.start() if next_match else len(body)
        return body[value_start:value_end].strip().strip(",").strip().strip('}"').strip()
    return ""


def loose_fields(text: Any) -> Json:
    if not isinstance(text, str) or not text.strip():
        return {}
    fields: Json = {}
    for key in FIELD_BOUNDARY_KEYS:
        value = loose_field(text, (key,))
        if value:
            fields[key] = value
    return fields


def _dict_field(source: Any, keys: tuple[str, ...] | list[str]) -> str:
    if not isinstance(source, dict):
        return ""
    for key in keys:
        value = source.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _env_field(env: Mapping[str, str] | None, keys: tuple[str, ...] | list[str]) -> str:
    if not env:
        return ""
    for key in keys:
        value = env.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _sources(payload: Any) -> list[Any]:
    found = [payload]
    if isinstance(payload, dict):
        for key in NESTED_KEYS:
            value = payload.get(key)
            if isinstance(value, (dict, str)):
                found.extend(_sources(value))
    return found


def prompt_from_input_messages(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        return ""
    matches = re.findall(r"<input>(.*?)</input>", value, re.DOTALL)
    if matches:
        return matches[-1].strip()
    stripped = value.strip().strip("[]").strip()
    if not stripped:
        return ""
    try:
        parsed = json.loads(value if value.lstrip().startswith("[") else f"[{value}]")
        if isinstance(parsed, list):
            parts = [
                str(part).strip()
                for part in parsed
                if isinstance(part, (str, int, float)) and str(part).strip()
            ]
            if parts:
                return parts[-1]
    except Exception:
        pass
    if "<codex_delegation>" in stripped:
        parts = [part.strip() for part in re.split(r",\s*(?=<codex_delegation>)", stripped) if part.strip()]
        return parts[-1] if parts else stripped
    line_parts = [part.strip() for part in re.split(r"(?:\r?\n|\\n)\s*,", stripped) if part.strip()]
    if len(line_parts) > 1:
        return line_parts[-1]
    return stripped


def extract_prompt(payload: Any, *, event: str = "UserPromptSubmit") -> str:
    for source in _sources(payload):
        if isinstance(source, str):
            input_messages = loose_field(source, ("input-messages", "input_messages", "inputMessages"))
            if input_messages:
                return prompt_from_input_messages(input_messages)
            for key in PROMPT_KEYS:
                value = loose_field(source, (key,))
                if value:
                    if key in {"input-messages", "input_messages", "inputMessages"}:
                        return prompt_from_input_messages(value)
                    return value
        elif isinstance(source, dict):
            input_messages = _dict_field(source, ("input-messages", "input_messages", "inputMessages"))
            if input_messages:
                return prompt_from_input_messages(input_messages)
            value = _dict_field(source, PROMPT_KEYS)
            if value:
                return value
    return ""


def extract_identity(payload: Any, *, env: Mapping[str, str] | None = None) -> Json:
    loose = loose_fields(payload) if isinstance(payload, str) else {}
    session_id = ""
    thread_id = ""
    turn_id = ""
    source_name = ""
    for source in _sources(payload):
        session_id = session_id or _dict_field(source, SESSION_KEYS) or _dict_field(loose, SESSION_KEYS)
        thread_id = thread_id or _dict_field(source, THREAD_KEYS) or _dict_field(loose, THREAD_KEYS)
        turn_id = turn_id or _dict_field(source, TURN_KEYS) or _dict_field(loose, TURN_KEYS)
        if session_id or thread_id:
            source_name = "payload_field"
            break
    if not (session_id or thread_id):
        session_id = _env_field(env, ENV_SESSION_KEYS)
        thread_id = _env_field(env, ENV_THREAD_KEYS)
        if session_id or thread_id:
            source_name = "env"
    if not turn_id:
        turn_id = _env_field(env, ENV_TURN_KEYS)
    canonical = session_id or thread_id
    if canonical and not canonical.startswith("codex:"):
        canonical = f"codex:{canonical}"
    return {
        "session_id": canonical,
        "session_id_source": source_name,
        "thread_id": thread_id,
        "turn_id": turn_id,
        "conversation_id": session_id,
    }
