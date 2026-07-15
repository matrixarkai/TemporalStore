#!/usr/bin/env python3
"""Summary and temporal-compression helpers for MatrixArk MCP."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

try:
    from tools.matrixark_mcp_errors import MatrixArkError
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import MatrixArkError


Json = dict[str, Any]


TIME_COMPRESSION_SUMMARY_PROVIDER = os.environ.get("MATRIXARK_TIME_COMPRESSION_SUMMARY_PROVIDER", "deterministic").strip().lower()
TIME_COMPRESSION_SUMMARY_MODEL = os.environ.get("MATRIXARK_TIME_COMPRESSION_SUMMARY_MODEL", os.environ.get("OPENAI_MODEL", "gpt-4o-mini"))
TIME_COMPRESSION_SUMMARY_BASE_URL = os.environ.get("MATRIXARK_TIME_COMPRESSION_SUMMARY_BASE_URL", os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")).rstrip("/")
TIME_COMPRESSION_SUMMARY_API_KEY_ENV = os.environ.get("MATRIXARK_TIME_COMPRESSION_SUMMARY_API_KEY_ENV", "OPENAI_API_KEY")
TIME_COMPRESSION_SUMMARY_TIMEOUT_SEC = float(os.environ.get("MATRIXARK_TIME_COMPRESSION_SUMMARY_TIMEOUT_SEC", "30"))
TIME_COMPRESSION_REQUIRE_LLM_SUMMARY = os.environ.get("MATRIXARK_REQUIRE_LLM_TIME_COMPRESSION", "").strip().lower() in {"1", "true", "yes"}


def summarize_text(text: str, *, limit: int = 220) -> str:
    compact = " ".join(text.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def deterministic_time_compression_summary(
    *,
    node_path: list[str],
    source_start_ms: int,
    source_end_ms: int,
    event_texts: list[str],
    max_raw_events_per_node: int,
) -> str:
    snippets = [summarize_text(text, limit=180) for text in event_texts[:5]]
    return (
        f"Temporal compression window [{source_start_ms}, {source_end_ms}] under "
        f"{' / '.join(node_path)} contains {len(event_texts)} source events. "
        f"Normal retrieval should score this synthesis plus the newest {max_raw_events_per_node} raw events. "
        + " | ".join(snippets)
    )


def time_compression_summary_provider_name() -> str:
    provider = TIME_COMPRESSION_SUMMARY_PROVIDER.replace("-", "_")
    if provider in {"", "local", "rules"}:
        return "deterministic"
    return provider


def generate_time_compression_summary(
    *,
    node_path: list[str],
    source_start_ms: int,
    source_end_ms: int,
    event_texts: list[str],
    max_raw_events_per_node: int,
) -> Json:
    fallback = deterministic_time_compression_summary(
        node_path=node_path,
        source_start_ms=source_start_ms,
        source_end_ms=source_end_ms,
        event_texts=event_texts,
        max_raw_events_per_node=max_raw_events_per_node,
    )
    provider = time_compression_summary_provider_name()
    if provider == "deterministic":
        return {
            "summary": fallback,
            "provider": "deterministic",
            "model": "",
            "fallback_used": False,
        }
    if provider not in {"openai", "openai_compatible", "openai_compatible_llm"}:
        if TIME_COMPRESSION_REQUIRE_LLM_SUMMARY:
            raise MatrixArkError(f"unsupported TIME_COMPRESS summary provider: {provider}")
        return {
            "summary": fallback,
            "provider": provider,
            "model": TIME_COMPRESSION_SUMMARY_MODEL,
            "fallback_used": True,
            "warning": "unsupported_time_compression_summary_provider",
        }
    api_key = os.environ.get(TIME_COMPRESSION_SUMMARY_API_KEY_ENV, "")
    if not api_key:
        if TIME_COMPRESSION_REQUIRE_LLM_SUMMARY:
            raise MatrixArkError(f"{TIME_COMPRESSION_SUMMARY_API_KEY_ENV} is required for TIME_COMPRESS summaries")
        return {
            "summary": fallback,
            "provider": provider,
            "model": TIME_COMPRESSION_SUMMARY_MODEL,
            "fallback_used": True,
            "warning": "missing_time_compression_summary_api_key",
        }
    prompt = (
        "Summarize old LLM context events into a compact replayable memory. "
        "Preserve decisions, entities, dates, constraints, and stale/current status. "
        "Do not invent facts. Return only the summary.\n\n"
        f"Node path: {' / '.join(node_path)}\n"
        f"Source time window: {source_start_ms}..{source_end_ms}\n"
        f"Newest raw events kept outside this summary: {max_raw_events_per_node}\n"
        "Source events:\n"
        + "\n".join(f"- {summarize_text(text, limit=400)}" for text in event_texts[:32])
    )
    payload = {
        "model": TIME_COMPRESSION_SUMMARY_MODEL,
        "messages": [
            {"role": "system", "content": "You write concise, factual memory compression summaries for an LLM context system."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
        "max_tokens": 512,
    }
    request = urllib.request.Request(
        f"{TIME_COMPRESSION_SUMMARY_BASE_URL}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=TIME_COMPRESSION_SUMMARY_TIMEOUT_SEC) as response:
            data = json.loads(response.read().decode("utf-8"))
        summary = str(data.get("choices", [{}])[0].get("message", {}).get("content", "")).strip()
        if not summary:
            raise MatrixArkError("TIME_COMPRESS summary provider returned empty content")
        return {
            "summary": summarize_text(summary, limit=1200),
            "provider": provider,
            "model": TIME_COMPRESSION_SUMMARY_MODEL,
            "fallback_used": False,
        }
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, MatrixArkError, OSError, json.JSONDecodeError) as exc:
        if TIME_COMPRESSION_REQUIRE_LLM_SUMMARY:
            raise MatrixArkError(f"TIME_COMPRESS summary provider failed: {exc}") from exc
        return {
            "summary": fallback,
            "provider": provider,
            "model": TIME_COMPRESSION_SUMMARY_MODEL,
            "fallback_used": True,
            "warning": str(exc),
        }


def estimated_context_tokens(text: str) -> int:
    """Cheap token estimate used for summary policy decisions."""
    compact = " ".join(str(text).split())
    if not compact:
        return 0
    return max(1, (len(compact) + 3) // 4)


def node_l1_generation_policy(
    *,
    source_text: str,
    event_count: int,
    child_summary_count: int,
) -> Json:
    """Decide when a node needs a richer L1 overview.

    L0 is mandatory for traversal. L1 is useful once a node has enough local
    content or child summaries that a short abstract would lose routing detail.
    """
    token_estimate = estimated_context_tokens(source_text)
    base = {
        "token_estimate": token_estimate,
        "event_count": event_count,
        "child_summary_count": child_summary_count,
    }
    if child_summary_count > 0:
        return {**base, "generate_l1": True, "reason": "has_child_summaries"}
    if event_count >= 3:
        return {**base, "generate_l1": True, "reason": "event_count_threshold"}
    if token_estimate >= 180:
        return {**base, "generate_l1": True, "reason": "token_threshold"}
    return {**base, "generate_l1": False, "reason": "l0_sufficient"}


