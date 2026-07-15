#!/usr/bin/env python3
"""Small text and token helpers shared by MatrixArk MCP modules."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_scoring import tokens
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_scoring import tokens


Json = dict[str, Any]

MAX_CONTEXT_REF_CHARS = 4096


def text_from_messages(messages: list[Json]) -> str:
    return "\n".join(f"{item['role']}: {item['content']}" for item in messages)


def token_count(text: str) -> int:
    return len(tokens(text))


def clip_context_text(text: str, *, max_chars: int = MAX_CONTEXT_REF_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + " ...[truncated]"
