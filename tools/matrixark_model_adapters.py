#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Client-side message-format adapters: normalize provider-shaped model SDK
data into MatrixArk's canonical ``messages`` list.

Customers usually already hold the object their model SDK produced or consumed
— an OpenAI chat-completions request/response, or an Anthropic messages
request/response. Rather than force them to hand-transcribe those into
MatrixArk's ``[{"role", "content"}, ...]`` shape before calling ``add()`` /
``ingest_text()``, these pure functions do the normalization:

  * ``from_openai(obj)``     — OpenAI chat request / response / message / list.
  * ``from_anthropic(obj)``  — Anthropic messages request / response / list.
  * ``to_messages(obj, *, provider="auto")`` — sniff the shape and dispatch.

Design constraints:

  * **Stdlib only.** We do NOT import the ``openai`` or ``anthropic`` SDKs.
    Inputs are accepted as plain ``dict``s OR as duck-typed objects exposing the
    same fields via attribute access (so a real SDK response object works too,
    without us depending on the package).
  * **Pure + total.** Empty / ``None`` input returns ``[]`` (never raises).
    ``ValueError`` is raised only for clearly-malformed input (e.g. a message
    whose ``content`` is a number, or a non-mapping/non-list top-level object).

Output roles are constrained to MatrixArk's set: ``user`` / ``assistant`` /
``tool`` / ``system``. Provider role names are mapped onto that set
(``function`` -> ``tool``; an Anthropic ``user`` turn carrying ``tool_result``
blocks -> ``tool``). Content that is a list of typed parts/blocks is flattened
to a single text string; turns whose flattened content is empty are skipped.
"""
from __future__ import annotations

from typing import Any, Optional

Json = dict[str, Any]

__all__ = ["from_openai", "from_anthropic", "to_messages", "VALID_ROLES"]

# MatrixArk's canonical role set. Everything a provider emits maps into this.
VALID_ROLES = ("user", "assistant", "tool", "system")

# OpenAI role -> MatrixArk role. Anything not listed passes through unchanged if
# it is already a valid role, else falls back to "user".
_OPENAI_ROLE_MAP = {
    "system": "system",
    "user": "user",
    "assistant": "assistant",
    "tool": "tool",
    "function": "tool",   # legacy OpenAI function-call role -> our tool role
    "developer": "system",  # newer OpenAI "developer" role behaves like system
}


# --------------------------------------------------------------------------- #
# duck-typed field access (dict OR object with attributes)
# --------------------------------------------------------------------------- #
def _get(obj: Any, key: str, default: Any = None) -> Any:
    """Read ``key`` from a mapping (``.get``) or an object (``getattr``).

    Supports the two shapes a customer might pass: a plain ``dict`` decoded from
    JSON, or an SDK model object (e.g. ``ChatCompletion``) exposing the same
    fields as attributes. Returns ``default`` when absent."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    getter = getattr(obj, "get", None)
    if callable(getter):
        try:
            return getter(key, default)
        except TypeError:
            pass
    return getattr(obj, key, default)


def _is_mapping_like(obj: Any) -> bool:
    """True when ``obj`` is a dict or a non-string/list object we can read
    fields off of (i.e. a candidate message/envelope, not a scalar)."""
    if isinstance(obj, dict):
        return True
    if obj is None or isinstance(obj, (str, bytes, list, tuple, int, float, bool)):
        return False
    return True


# --------------------------------------------------------------------------- #
# content flattening
# --------------------------------------------------------------------------- #
def _flatten_content(content: Any) -> str:
    """Flatten a message's ``content`` into a plain string.

    Accepts:
      * ``None`` -> ``""`` (empty; caller skips the turn).
      * ``str`` -> returned as-is.
      * ``list`` of parts/blocks -> the ``text`` of each text-bearing part,
        joined with newlines. Recognizes both the OpenAI part shape
        (``{"type": "text", "text": "..."}``) and the Anthropic block shape
        (same keys). ``tool_result`` blocks contribute their nested text/content;
        ``tool_use``/``function`` call blocks contribute a compact description so
        the turn is not silently dropped. Non-text parts (images) are skipped.
      * a single part/block dict -> flattened as a one-element list.

    Raises ``ValueError`` for a content that is a scalar we cannot interpret as
    text (e.g. an int / float / bool)."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, (int, float, bool)):
        raise ValueError(f"message content must be str or list, got {type(content).__name__}")
    if _is_mapping_like(content):
        return _flatten_content([content])
    if isinstance(content, (list, tuple)):
        pieces: list[str] = []
        for part in content:
            piece = _flatten_part(part)
            if piece:
                pieces.append(piece)
        return "\n".join(pieces)
    raise ValueError(f"unsupported message content type: {type(content).__name__}")


def _flatten_part(part: Any) -> str:
    """Flatten one content part/block into text (``""`` if it carries none)."""
    if part is None:
        return ""
    if isinstance(part, str):
        return part
    if not _is_mapping_like(part):
        raise ValueError(f"content part must be str or object, got {type(part).__name__}")

    ptype = _get(part, "type")

    # Plain text part/block: {"type": "text", "text": "..."}.
    text = _get(part, "text")
    if isinstance(text, str) and (ptype in (None, "text", "input_text", "output_text") or ptype is None):
        return text

    # Anthropic tool_result block: content may be a string or a list of blocks.
    if ptype == "tool_result":
        inner = _get(part, "content")
        return _flatten_content(inner) if inner is not None else ""

    # Anthropic tool_use / OpenAI function/tool call: describe it so the turn is
    # not dropped. Prefer a text field if one is present.
    if ptype in ("tool_use", "function", "function_call", "tool_call"):
        name = _get(part, "name")
        if isinstance(text, str) and text:
            return text
        return f"[tool_use: {name}]" if name else ""

    # Fallback: if there is any string ``text`` on an unknown part, use it.
    if isinstance(text, str):
        return text
    return ""


def _map_openai_role(role: Any) -> str:
    """Map an OpenAI role onto MatrixArk's set (default ``user``)."""
    if not isinstance(role, str) or not role:
        return "user"
    mapped = _OPENAI_ROLE_MAP.get(role.lower())
    if mapped:
        return mapped
    return role.lower() if role.lower() in VALID_ROLES else "user"


def _emit(messages: list[Json], role: str, content: Any) -> None:
    """Flatten ``content`` and append ``{"role", "content"}`` unless the
    flattened text is empty (skip empty turns)."""
    text = _flatten_content(content)
    if text == "":
        return
    messages.append({"role": role, "content": text})


# --------------------------------------------------------------------------- #
# OpenAI
# --------------------------------------------------------------------------- #
def _openai_message_to_entry(msg: Any) -> Optional[Json]:
    """Normalize one OpenAI message dict/object into ``{"role", "content"}`` or
    ``None`` when it flattens to empty."""
    if not _is_mapping_like(msg):
        raise ValueError(f"OpenAI message must be an object, got {type(msg).__name__}")
    role = _map_openai_role(_get(msg, "role"))
    text = _flatten_content(_get(msg, "content"))
    if text == "":
        return None
    return {"role": role, "content": text}


def from_openai(obj: Any) -> list[Json]:
    """Normalize OpenAI-shaped data into MatrixArk ``messages``.

    Accepts any of:
      * a **chat-completions request**: ``{"messages": [{role, content}, ...]}``
      * a **chat-completions response**:
        ``{"choices": [{"message": {"role": "assistant", "content": ...}}]}``
      * a **single message**: ``{"role": ..., "content": ...}``
      * a **raw list** of messages: ``[{role, content}, ...]``

    Content that is a list of parts (``[{"type": "text", "text": ...}]``) is
    flattened to a string; the ``function`` role maps to ``tool``. Empty / None
    input -> ``[]``. Empty turns are skipped. Raises ``ValueError`` only on a
    clearly-malformed shape."""
    if obj is None:
        return []

    # A raw list: either a list of messages, or (rarely) a list of choices.
    if isinstance(obj, (list, tuple)):
        out: list[Json] = []
        for item in obj:
            entry = _openai_message_to_entry(item)
            if entry is not None:
                out.append(entry)
        return out

    if isinstance(obj, str):
        # Bare string -> a single user turn (mirrors mem0's convenience).
        return [{"role": "user", "content": obj}] if obj else []

    if not _is_mapping_like(obj):
        raise ValueError(f"from_openai expects a dict/list/str, got {type(obj).__name__}")

    # Response shape: {"choices": [{"message": {...}}, ...]}.
    choices = _get(obj, "choices")
    if choices is not None:
        if not isinstance(choices, (list, tuple)):
            raise ValueError("OpenAI 'choices' must be a list")
        out = []
        for choice in choices:
            msg = _get(choice, "message")
            if msg is None:
                # Streaming/delta responses put the payload under 'delta'.
                msg = _get(choice, "delta")
            if msg is None:
                continue
            entry = _openai_message_to_entry(msg)
            if entry is not None:
                out.append(entry)
        return out

    # Request shape: {"messages": [...]}.
    msgs = _get(obj, "messages")
    if msgs is not None:
        if not isinstance(msgs, (list, tuple)):
            raise ValueError("OpenAI 'messages' must be a list")
        out = []
        for msg in msgs:
            entry = _openai_message_to_entry(msg)
            if entry is not None:
                out.append(entry)
        return out

    # Single message: {"role", "content"}.
    if _get(obj, "role") is not None or _get(obj, "content") is not None:
        entry = _openai_message_to_entry(obj)
        return [entry] if entry is not None else []

    raise ValueError(
        "unrecognized OpenAI shape: expected 'messages', 'choices', or a "
        "single {role, content} message")


# --------------------------------------------------------------------------- #
# Anthropic
# --------------------------------------------------------------------------- #
def _anthropic_role(role: Any, content: Any) -> str:
    """Resolve an Anthropic turn's MatrixArk role. Anthropic uses only
    ``user``/``assistant``; a ``user`` turn whose content is entirely
    ``tool_result`` blocks represents a tool response -> role ``tool``."""
    r = role.lower() if isinstance(role, str) and role else "user"
    if r == "user" and _all_tool_result(content):
        return "tool"
    if r in VALID_ROLES:
        return r
    return "user"


def _all_tool_result(content: Any) -> bool:
    """True when ``content`` is a non-empty list of blocks, all ``tool_result``."""
    if not isinstance(content, (list, tuple)) or not content:
        return False
    saw = False
    for block in content:
        if not _is_mapping_like(block):
            return False
        if _get(block, "type") != "tool_result":
            return False
        saw = True
    return saw


def _anthropic_message_to_entry(msg: Any) -> Optional[Json]:
    """Normalize one Anthropic message dict/object into ``{"role", "content"}``
    or ``None`` when empty."""
    if not _is_mapping_like(msg):
        raise ValueError(f"Anthropic message must be an object, got {type(msg).__name__}")
    content = _get(msg, "content")
    role = _anthropic_role(_get(msg, "role"), content)
    text = _flatten_content(content)
    if text == "":
        return None
    return {"role": role, "content": text}


def from_anthropic(obj: Any) -> list[Json]:
    """Normalize Anthropic-shaped data into MatrixArk ``messages``.

    Accepts any of:
      * a **messages request**: ``{"messages": [{role, content}, ...]}`` where
        ``content`` is a string OR a list of blocks
        (``[{"type": "text", "text": ...}]``).
      * a **Messages response**:
        ``{"role": "assistant", "content": [{"type": "text", "text": ...}], ...}``
      * a **raw list** of messages.

    Text blocks are flattened; a ``user`` turn made of ``tool_result`` blocks
    maps to role ``tool``. Empty / None -> ``[]``. Empty turns skipped. Raises
    ``ValueError`` only on a clearly-malformed shape."""
    if obj is None:
        return []

    if isinstance(obj, (list, tuple)):
        out: list[Json] = []
        for item in obj:
            entry = _anthropic_message_to_entry(item)
            if entry is not None:
                out.append(entry)
        return out

    if isinstance(obj, str):
        return [{"role": "user", "content": obj}] if obj else []

    if not _is_mapping_like(obj):
        raise ValueError(f"from_anthropic expects a dict/list/str, got {type(obj).__name__}")

    # Request shape: {"messages": [...]}.
    msgs = _get(obj, "messages")
    if msgs is not None:
        if not isinstance(msgs, (list, tuple)):
            raise ValueError("Anthropic 'messages' must be a list")
        out = []
        for msg in msgs:
            entry = _anthropic_message_to_entry(msg)
            if entry is not None:
                out.append(entry)
        return out

    # Response / single-message shape: a top-level {"role", "content"}.
    if _get(obj, "content") is not None or _get(obj, "role") is not None:
        entry = _anthropic_message_to_entry(obj)
        return [entry] if entry is not None else []

    raise ValueError(
        "unrecognized Anthropic shape: expected 'messages' or a top-level "
        "{role, content} with text blocks")


# --------------------------------------------------------------------------- #
# provider sniffing
# --------------------------------------------------------------------------- #
def _looks_like_openai(obj: Any) -> bool:
    """OpenAI-specific: a ``choices`` list (chat-completions response)."""
    return _is_mapping_like(obj) and _get(obj, "choices") is not None


def _looks_like_anthropic(obj: Any) -> bool:
    """Anthropic-specific: a top-level ``content`` that is a list of typed
    blocks (Messages response), or Anthropic-only response markers."""
    if not _is_mapping_like(obj):
        return False
    if _get(obj, "type") == "message" and _get(obj, "role") == "assistant":
        return True
    content = _get(obj, "content")
    if isinstance(content, (list, tuple)) and content:
        first = content[0]
        if _is_mapping_like(first) and _get(first, "type") is not None:
            return True
    # A messages list whose turns use block-list content is Anthropic-shaped.
    msgs = _get(obj, "messages")
    if isinstance(msgs, (list, tuple)):
        for m in msgs:
            c = _get(m, "content")
            if isinstance(c, (list, tuple)) and c and _is_mapping_like(c[0]) \
                    and _get(c[0], "type") in ("text", "tool_result", "tool_use", "image"):
                return True
    return False


def to_messages(obj: Any, *, provider: str = "auto") -> list[Json]:
    """Normalize provider-shaped ``obj`` into MatrixArk ``messages``.

    ``provider``:
      * ``"openai"`` / ``"anthropic"`` — force that adapter.
      * ``"matrixark"`` / ``"messages"`` / ``"passthrough"`` — treat ``obj`` as
        already-normalized (validate + flatten, no provider mapping).
      * ``"auto"`` (default) — sniff the shape: a ``choices`` list -> OpenAI;
        content-block lists / Anthropic response markers -> Anthropic; otherwise
        fall back to the OpenAI reader (it already handles plain
        ``{"messages": [{role, content}]}`` requests, single messages, lists and
        bare strings, which is also MatrixArk's native shape).

    Empty / None -> ``[]``. Raises ``ValueError`` on an unknown ``provider`` or a
    clearly-malformed ``obj``."""
    if obj is None:
        return []
    p = (provider or "auto").lower()

    if p == "openai":
        return from_openai(obj)
    if p == "anthropic":
        return from_anthropic(obj)
    if p in ("matrixark", "messages", "passthrough", "native"):
        return _passthrough(obj)
    if p != "auto":
        raise ValueError(
            f"unknown provider {provider!r} (use 'auto', 'openai', 'anthropic', "
            f"or 'matrixark')")

    # auto-sniff
    if _looks_like_openai(obj):
        return from_openai(obj)
    if _looks_like_anthropic(obj):
        return from_anthropic(obj)
    # Default reader: OpenAI handles the MatrixArk-native shapes too.
    return from_openai(obj)


def _passthrough(obj: Any) -> list[Json]:
    """Treat ``obj`` as already-normalized MatrixArk messages. Accepts a list of
    ``{role, content}``, a ``{"messages": [...]}`` envelope, a single message, or
    a bare string. Flattens list-content and validates roles, but applies no
    provider role remapping beyond coercion into the valid set."""
    if obj is None:
        return []
    if isinstance(obj, str):
        return [{"role": "user", "content": obj}] if obj else []
    if isinstance(obj, dict) and _get(obj, "messages") is not None:
        obj = _get(obj, "messages")
    if _is_mapping_like(obj) and not isinstance(obj, (list, tuple)):
        obj = [obj]
    if not isinstance(obj, (list, tuple)):
        raise ValueError(f"passthrough expects messages, got {type(obj).__name__}")
    out: list[Json] = []
    for msg in obj:
        if not _is_mapping_like(msg):
            raise ValueError(f"message must be an object, got {type(msg).__name__}")
        role = _get(msg, "role")
        role = role.lower() if isinstance(role, str) and role.lower() in VALID_ROLES else "user"
        text = _flatten_content(_get(msg, "content"))
        if text == "":
            continue
        out.append({"role": role, "content": text})
    return out
