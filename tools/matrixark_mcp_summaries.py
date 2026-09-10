#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Summary and temporal-compression helpers for MatrixArk MCP."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_env import env_bool
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool


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
TIME_COMPRESSION_REQUIRE_LLM_SUMMARY = env_bool("MATRIXARK_REQUIRE_LLM_TIME_COMPRESSION", False)


SUMMARY_LLM_PROVIDER = os.environ.get(
    "MATRIXARK_SUMMARY_PROVIDER",
    os.environ.get("MATRIXARK_UNDERSTANDING_PROVIDER", os.environ.get("MATRIXARK_EXTRACTION_PROVIDER", "deterministic")),
).strip().lower().replace("-", "_")
# The same chain matrixark_mcp_core resolves for EXTRACTION_LLM_MODEL, ending in the same
# literal. This module imports nothing from the project on purpose, so the chain is written
# out rather than shared -- and the last step used to say "gpt-4o-mini" here while mcp_core
# said "qwen2.5:1.5b". Both modules send `model=SUMMARY_LLM_MODEL` to the endpoint, so a
# deployment that chose a provider and named no model asked for a different model depending
# on which of them did the summarising.
# The summary IS the extraction model; see the note beside the same constant in
# matrixark_mcp_core. Spelled out rather than imported because these two modules deliberately do not
# depend on each other, and a test pins that they still resolve to the same thing.
SUMMARY_LLM_MODEL = os.environ.get("MATRIXARK_EXTRACTION_MODEL", os.environ.get("OPENAI_MODEL", "qwen2.5:1.5b"))
SUMMARY_LLM_MAX_TOKENS = int(os.environ.get("MATRIXARK_SUMMARY_MAX_TOKENS", "900"))


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import summarize_text
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import summarize_text


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import deterministic_time_compression_summary
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import deterministic_time_compression_summary


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import time_compression_summary_provider_name
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import time_compression_summary_provider_name


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


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import estimated_context_tokens
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import estimated_context_tokens


# Not defined here: the implementation lives in matrixark_mcp_core_node_tree and this module carried an
# identical second copy of each. Every caller importing these names from here is
# unaffected -- it is the same code, and the free names each body reads are bound the
# same way in both modules, which is what makes re-exporting a no-op rather than a
# swap.
try:
    from tools.matrixark_mcp_core_node_tree import (
        node_l1_generation_policy,
    )
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core_node_tree import (
        node_l1_generation_policy,
    )


def _require_oss_understanding() -> bool:
    try:
        from tools.matrixark_mcp_oss_understanding import require_oss_understanding
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_oss_understanding import require_oss_understanding
    return require_oss_understanding()


def summary_provider() -> str:
    provider = os.environ.get("MATRIXARK_SUMMARY_PROVIDER", SUMMARY_LLM_PROVIDER).strip().lower().replace("-", "_")
    if provider in {"oss", "open_source", "local_llm", "oss_llm"}:
        return "openai_compatible"
    if provider in {"", "deterministic", "rules", "local"} and _require_oss_understanding():
        raise MatrixArkError("deterministic L0/L1 summary generation is disabled because MATRIXARK_REQUIRE_OSS_UNDERSTANDING=1")
    return provider or "deterministic"


def synthesize_context_node_summary(
    *,
    level: str,
    node_path: list[str],
    source_text: str,
    fallback_text: str,
    max_chars: int,
    policy: Json,
) -> tuple[str, Json]:
    provider = summary_provider()
    fallback_summary = summarize_text(fallback_text, limit=max_chars)
    provider_meta: Json = {
        "provider": provider,
        "model": SUMMARY_LLM_MODEL if provider in {"openai", "openai_compatible", "openai_compatible_llm"} else "",
        "fallback_used": False,
        "summary_level": level,
    }
    if provider in {"openai", "openai_compatible", "openai_compatible_llm"}:
        system = (
            "You generate MatrixArk ContextNode traversal summaries. Return JSON only. "
            "The summary must be faithful to the supplied child summaries, entity state, operator state, and recent events. "
            "For node_l0 return a compact routing abstract. For node_l1 return a richer semantic synthesis for tree-first retrieval. "
            "Do not invent facts. Prefer current state and resolved contradictions."
        )
        user = json.dumps(
            {
                "summary_level": level,
                "node_path": node_path,
                "generation_policy": policy,
                "source_text": summarize_text(source_text, limit=5000),
                "required_json_shape": {"summary_text": "string"},
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        try:
            json_call = globals().get("openai_compatible_json_call")
            if json_call is None:
                try:
                    from tools.matrixark_mcp_extraction_provider import openai_compatible_json_call as json_call
                except ModuleNotFoundError:  # Direct script execution from tools/.
                    from matrixark_mcp_extraction_provider import openai_compatible_json_call as json_call
            result = json_call(
                system=system,
                user=user,
                model=SUMMARY_LLM_MODEL,
                max_tokens=SUMMARY_LLM_MAX_TOKENS,
            )
            summary_text = summarize_text(str(result.get("summary_text") or result.get("summary") or ""), limit=max_chars)
            if not summary_text:
                raise MatrixArkError("summary provider returned empty summary_text")
            provider_meta["execution_mode"] = "llm_json"
            return summary_text, provider_meta
        except MatrixArkError:
            if _require_oss_understanding():
                raise
            provider_meta["fallback_used"] = True
            provider_meta["execution_mode"] = "deterministic_fallback"
            return fallback_summary, provider_meta
    if _require_oss_understanding():
        raise MatrixArkError(f"unsupported OSS summary provider: {provider}")
    provider_meta["fallback_used"] = True
    provider_meta["execution_mode"] = "deterministic_fallback"
    return fallback_summary, provider_meta

