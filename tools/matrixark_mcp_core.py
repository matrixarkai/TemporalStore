#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""MatrixArk MCP server for LLM context ingestion and retrieval.

This is intentionally dependency-free. It implements the small JSON-RPC subset
needed by MCP clients over stdio, and keeps the storage boundary behind a local
adapter that can be replaced with TemporalStore RPC calls later.
"""

from __future__ import annotations

try:
    from tools.matrixark_mcp_env import env_bool
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool

import base64 as _base64
import struct as _struct
from struct import error as struct_error

import argparse
import secrets
import select
import shutil
import socket
import subprocess
import functools
import hashlib
import json
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
import urllib.error
import urllib.request
import math
import os
import re
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

try:
    from tools.matrixark_resource_parser import ResourceParserError, content_hash, embedding_text_for_chunk, normalize_parse_warnings, parse_resource, summarize_resource_chunks
    from tools.matrixark_skill_parser import parse_skill
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_resource_parser import ResourceParserError, content_hash, embedding_text_for_chunk, normalize_parse_warnings, parse_resource, summarize_resource_chunks
    from matrixark_skill_parser import parse_skill


Json = dict[str, Any]


def pending_extraction_memory_layer_intent(scope: Json) -> tuple[list[str], list[str]]:
    has_profile_scope = bool(str(scope.get("user_id") or "").strip())
    if has_profile_scope:
        return ["session", "user_profile"], ["same_session", "cross_session"]
    return ["session"], ["same_session"]


def _mcp_debug_log(message: str) -> None:
    path = os.environ.get("MATRIXARK_MCP_DEBUG_LOG")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(f"{time.time():.3f} {message}\n")
    except Exception:
        pass
MAX_PRIOR_MESSAGES = 8
MAX_PRIOR_CHARS = 4096
EMBEDDING_DIM = 32
DIRECT_RECORD_LOG_SHARD_SIZE = 256
DIRECT_RECORD_BUNDLE_MAX_BYTES = int(os.environ.get("MATRIXARK_DIRECT_RECORD_BUNDLE_MAX_BYTES", "65536"))
DIRECT_RECORD_HOT_CACHE_MAX_RECORDS = int(os.environ.get("MATRIXARK_DIRECT_RECORD_HOT_CACHE_MAX_RECORDS", "20000"))
DIRECT_WRITE_RETRIES = int(os.environ.get("MATRIXARK_DIRECT_WRITE_RETRIES", "3"))
DIRECT_WRITE_BACKOFF_MS = int(os.environ.get("MATRIXARK_DIRECT_WRITE_BACKOFF_MS", "25"))
DIRECT_WRITE_THROTTLE_MS = int(os.environ.get("MATRIXARK_DIRECT_WRITE_THROTTLE_MS", "0"))
DIRECT_AUDIT_MODE = os.environ.get("MATRIXARK_DIRECT_AUDIT_MODE", "buffered").strip().lower()
DIRECT_AUDIT_BUFFER_MAX_RECORDS = int(os.environ.get("MATRIXARK_DIRECT_AUDIT_BUFFER_MAX_RECORDS", "128"))
DIRECT_AUDIT_FLUSH_INTERVAL_MS = int(os.environ.get("MATRIXARK_DIRECT_AUDIT_FLUSH_INTERVAL_MS", "1000"))
CONTEXT_TELEMETRY_WRITE_MODE = os.environ.get("MATRIXARK_CONTEXT_TELEMETRY_WRITE_MODE", "inline").strip().lower()
# These four, and four more below, are defined here AND in matrixark_mcp_runtime_config. See the
# single-source note beside DEFAULT_MAX_CONTEXT_TOKENS further down: that constant was read from
# the same variable in both modules with different fallbacks, and an operator who set nothing got
# one answer through core paths and another through runtime-config paths. Same shape, same file --
# so take the value from there rather than writing a second literal that can drift the same way.
SUMMARY_REFRESH_INTERVAL_MS = int(os.environ.get("MATRIXARK_SUMMARY_REFRESH_INTERVAL_MS", "1000"))
SUMMARY_REFRESH_LIMIT = int(os.environ.get("MATRIXARK_SUMMARY_REFRESH_LIMIT", "64"))
# Largest share of wall-clock the background summary refresher may occupy. A refresh pass
# costs O(store) -- it reads the whole record log and writes the refreshed summaries back
# through the same proxy lane the request path uses -- so at a fixed interval a pass that
# grows past that interval turns the loop into a permanent occupant of the lane. See
# MatrixArkMcpServer._next_summary_refresh_delay_s.
SUMMARY_REFRESH_MAX_DUTY = float(os.environ.get("MATRIXARK_SUMMARY_REFRESH_MAX_DUTY", "0.2"))
SUMMARY_REFRESH_MAX_BACKOFF_MS = int(os.environ.get("MATRIXARK_SUMMARY_REFRESH_MAX_BACKOFF_MS", "300000"))
BACKEND_READINESS_TIMEOUT_MS = int(os.environ.get("MATRIXARK_BACKEND_READINESS_TIMEOUT_MS", "30000"))
BACKEND_READINESS_BACKOFF_MS = int(os.environ.get("MATRIXARK_BACKEND_READINESS_BACKOFF_MS", "200"))
MATRIXARK_MCP_PROFILE = os.environ.get("MATRIXARK_MCP_PROFILE", "dev").strip().lower()
# MATRIXARK_ALLOW_LOCAL_BACKEND comes from matrixark_mcp_runtime_config, above.
MATRIXARK_REQUIRE_BACKEND_READY = os.environ.get("MATRIXARK_REQUIRE_BACKEND_READY", "").strip().lower()
MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = os.environ.get("MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK", "").strip().lower()
MATRIXARK_ALLOW_PYTHON_HOT_CACHE = os.environ.get("MATRIXARK_ALLOW_PYTHON_HOT_CACHE", "").strip().lower()
MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER = os.environ.get("MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER", "").strip().lower()
BACKEND_READINESS_CONNECT_TIMEOUT_MS = int(os.environ.get("MATRIXARK_BACKEND_READINESS_CONNECT_TIMEOUT_MS", "1000"))

# Single source of truth: reuse the runtime-config default so core and
# matrixark_mcp_runtime_config agree out-of-the-box (both were previously
# reading the same env var but with different fallback literals -- 128000 here
# vs 500000 in runtime_config -- so an operator who set nothing got a 128k
# ceiling from core paths and a 500k ceiling from runtime-config paths). Both
# still honour MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS when it is set.
try:
    from tools.matrixark_mcp_runtime_config import (
        AUDIT_DEBUG_PAYLOAD,
        CONTEXT_PACK_DEBUG_REFS,
        ENABLE_CONTEXT_DEBUG_RECORDS,
        ENABLE_CONTEXT_REPLAY,
        ENABLE_LLM_MERGE_OPERATOR,
        ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS,
        ENABLE_SUMMARY_REFRESH_AUDIT,
        MATRIXARK_ALLOW_LOCAL_BACKEND,
        live_float,
        live_int,
        DEFAULT_MAX_CONTEXT_TOKENS as _RUNTIME_DEFAULT_MAX_CONTEXT_TOKENS,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_runtime_config import (
        AUDIT_DEBUG_PAYLOAD,
        CONTEXT_PACK_DEBUG_REFS,
        ENABLE_CONTEXT_DEBUG_RECORDS,
        ENABLE_CONTEXT_REPLAY,
        ENABLE_LLM_MERGE_OPERATOR,
        ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS,
        ENABLE_SUMMARY_REFRESH_AUDIT,
        MATRIXARK_ALLOW_LOCAL_BACKEND,
        live_float,
        live_int,
        DEFAULT_MAX_CONTEXT_TOKENS as _RUNTIME_DEFAULT_MAX_CONTEXT_TOKENS,
    )
DEFAULT_MAX_CONTEXT_TOKENS = _RUNTIME_DEFAULT_MAX_CONTEXT_TOKENS
CONTEXT_PACK_DEBUG_REFS = env_bool("MATRIXARK_CONTEXT_PACK_DEBUG_REFS", False)
AUDIT_DEBUG_PAYLOAD = env_bool("MATRIXARK_AUDIT_DEBUG_PAYLOAD", False)

MAX_SECONDARY_INDEX_TERMS_PER_RECORD = int(os.environ.get("MATRIXARK_MAX_SECONDARY_INDEX_TERMS_PER_RECORD", "10"))
SECONDARY_INDEX_POSTING_BUCKET_MS = int(os.environ.get("MATRIXARK_SECONDARY_INDEX_POSTING_BUCKET_MS", "60000"))
MAX_METADATA_KEYWORD_INDEXES_PER_CHUNK = int(os.environ.get("MATRIXARK_MAX_METADATA_KEYWORD_INDEXES_PER_CHUNK", "6"))
MAX_INDEX_TERMS_PER_RESOURCE_CHUNK = int(os.environ.get("MATRIXARK_MAX_INDEX_TERMS_PER_RESOURCE_CHUNK", str(MAX_SECONDARY_INDEX_TERMS_PER_RECORD)))
MAX_INDEX_TERMS_PER_RESOURCE_FACT = int(os.environ.get("MATRIXARK_MAX_INDEX_TERMS_PER_RESOURCE_FACT", str(MAX_SECONDARY_INDEX_TERMS_PER_RECORD)))
MAX_SECONDARY_INDEX_RECORDS_PER_OPERATION = int(os.environ.get("MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_OPERATION", "128"))
MAX_SECONDARY_INDEX_REFS_PER_POSTING = int(os.environ.get("MATRIXARK_MAX_SECONDARY_INDEX_REFS_PER_POSTING", "512"))
SECONDARY_INDEX_TIME_BUCKET_MS = int(os.environ.get("MATRIXARK_SECONDARY_INDEX_TIME_BUCKET_MS", "60000"))
DEFAULT_MAX_CHILDREN_SCORED_PER_PARENT = int(os.environ.get("MATRIXARK_MAX_CHILDREN_SCORED_PER_PARENT", "100000"))
HARD_MAX_CHILDREN_SCORED_PER_PARENT = int(os.environ.get("MATRIXARK_HARD_MAX_CHILDREN_SCORED_PER_PARENT", "100000"))
SECONDARY_INDEX_PRIORITY_PREFIXES = (
    "source_type:",
    "resource_type:",
    "unit_kind:",
    "entity_type:",
    "event_type:",
    "classification:",
    "status:",
    "memory_scope:",
    "session_continuity:",
    "extraction_phase:",
    "memory_selection_policy:",
    "memory_selection_quality:",
    "profile_promotion_policy:",
    "skill_name:",
    "skill_trigger:",
    "skill_tool:",
    "relative_path:",
    "heading_slug:",
    "segment_topic:",
    "keyword:",
)
# ------------------------------------------------------------------------------------------------
# Secondary-index dimension pruning (Lever 2).
#
# One message turn emits ~87 context_index postings across ~21 dimensions, but the majority index on
# CONSTANT internal routing/policy metadata (every record carries the same value) that is not useful
# for semantic retrieval -- they dominate the posting COUNT without adding recall. Dropping these
# non-semantic dimensions from posting materialization cuts the posting count ~80% while keeping every
# semantic dimension used to recall facts (entity_type, entity_name, segment_topic, event_type,
# classification, status, source_role, source_type, keyword, and skill/resource/topology dims).
#
# On. `test_intern_record_metadata` sets this constant to False to isolate the other lever on a
# metadata-heavy corpus, which is the only thing that ever selected the other position. Recall is
# validated by test_prune_internal_index_dimensions (must stay 5/5); any dimension whose removal
# drops recall must be taken OFF this list and kept.
PRUNE_INTERNAL_INDEX_DIMENSIONS = True
INTERNAL_INDEX_DIMENSIONS = frozenset({
    "memory_selection_policy",
    "memory_scope",
    "source_memory_scope",
    "memory_layer",
    "source_memory_layer",
    "session_continuity",
    "source_session_continuity",
    "extraction_phase",
    "profile_promotion_policy",
    "profile_memory_class",
    "profile_memory_kind",
    "profile_entity_current",
})


def prune_internal_index_terms(terms):
    """Drop non-semantic internal-metadata index dimensions from a term collection when the flag is ON.

    Terms are ``"<dimension>:<value>"``; the dimension is the substring before the first ``:``. Returns
    a set of the retained terms. No-op (returns the terms as a set) when the flag is OFF."""
    if not PRUNE_INTERNAL_INDEX_DIMENSIONS:
        return set(terms)
    kept = set()
    for term in terms:
        if not term:
            continue
        dimension = term.split(":", 1)[0]
        if dimension in INTERNAL_INDEX_DIMENSIONS:
            continue
        kept.add(term)
    return kept


MAX_RESOURCE_FACT_CHUNKS = int(os.environ.get("MATRIXARK_MAX_RESOURCE_FACT_CHUNKS", "8"))
MAX_RESOURCE_FACTS_PER_RESOURCE = int(os.environ.get("MATRIXARK_MAX_RESOURCE_FACTS_PER_RESOURCE", "8"))
MAX_RESOURCE_FACTS_PER_CHUNK = int(os.environ.get("MATRIXARK_MAX_RESOURCE_FACTS_PER_CHUNK", "2"))
ENABLE_GENERIC_RESOURCE_FACTS = env_bool("MATRIXARK_ENABLE_GENERIC_RESOURCE_FACTS", False)
RESOURCE_ASYNC_DEFAULT_BYTES = int(os.environ.get("MATRIXARK_RESOURCE_ASYNC_DEFAULT_BYTES", str(2 * 1024 * 1024)))
RESOURCE_ASYNC_DEFAULT_TEXT_CHARS = int(os.environ.get("MATRIXARK_RESOURCE_ASYNC_DEFAULT_TEXT_CHARS", "200000"))
RESOURCE_ASYNC_DEFAULT_PATH_COUNT = int(os.environ.get("MATRIXARK_RESOURCE_ASYNC_DEFAULT_PATH_COUNT", "32"))
MAX_CONTEXT_REF_CHARS = 4096
DEFAULT_TIME_DECAY_TOLERANCE_MS = 24 * 60 * 60 * 1000
DEFAULT_TIME_DECAY_HALFLIFE_MS = 7 * 24 * 60 * 60 * 1000
DEFAULT_TIME_WEIGHT = 0.18
DEFAULT_BUSINESS_WEIGHT = 0.22
DEFAULT_RETRIEVAL_MIN_SCORE = float(os.environ.get("MATRIXARK_RETRIEVAL_MIN_SCORE", "0.20"))
DEFAULT_TOP_K_PER_LAYER = int(os.environ.get("MATRIXARK_TOP_K_PER_LAYER", "8"))
DEFAULT_MAX_CANDIDATES_PER_NODE = int(os.environ.get("MATRIXARK_MAX_CANDIDATES_PER_NODE", "1024"))
DEFAULT_MAX_GLOBAL_CANDIDATES = int(os.environ.get("MATRIXARK_MAX_GLOBAL_CANDIDATES", "512"))
DEFAULT_MAX_SELECTED_REFS = int(os.environ.get("MATRIXARK_MAX_SELECTED_REFS", "64"))
DEFAULT_BUDGET_FILL_POLICY = os.environ.get("MATRIXARK_BUDGET_FILL_POLICY", "quality_first").strip().lower()
# Near-duplicate suppression threshold (see matrixark_mcp_runtime_config for the
# canonical definition). Reused here so the ref-selection path shares one source
# of truth for the knob.
DEFAULT_NEAR_DUPLICATE_OVERLAP_THRESHOLD = float(
    os.environ.get("MATRIXARK_NEAR_DUPLICATE_OVERLAP_THRESHOLD", "0.85")
)
DEFAULT_CROSS_SESSION_BUDGET_RATIO = 0.12  # build default; live_float reads the environment
DEFAULT_CROSS_SESSION_CURRENT_STATE_BUDGET_RATIO = float(os.environ.get("MATRIXARK_CROSS_SESSION_CURRENT_STATE_BUDGET_RATIO", "0.20"))
DEFAULT_CROSS_SESSION_MULTI_HOP_BUDGET_RATIO = float(os.environ.get("MATRIXARK_CROSS_SESSION_MULTI_HOP_BUDGET_RATIO", "0.20"))
DEFAULT_CROSS_SESSION_BROAD_BUDGET_RATIO = float(os.environ.get("MATRIXARK_CROSS_SESSION_BROAD_BUDGET_RATIO", "0.15"))
DEFAULT_CROSS_SESSION_PROFILE_BUDGET_RATIO = 0.30  # build default; live_float reads the environment
DEFAULT_CROSS_SESSION_MAX_BUDGET_TOKENS = 262144  # build default; live_int reads the environment
DEFAULT_CROSS_SESSION_PROFILE_MAX_BUDGET_TOKENS = 327680  # build default; live_int reads the environment
DEFAULT_CROSS_SESSION_MAX_SESSIONS = int(os.environ.get("MATRIXARK_CROSS_SESSION_MAX_SESSIONS", "3"))
DEFAULT_CROSS_SESSION_MAX_CANDIDATES = int(os.environ.get("MATRIXARK_CROSS_SESSION_MAX_CANDIDATES", "24"))
DEFAULT_CROSS_SESSION_MIN_ENTITY_BRIDGE_REFS = int(os.environ.get("MATRIXARK_CROSS_SESSION_MIN_ENTITY_BRIDGE_REFS", "2"))
DEFAULT_CROSS_SESSION_PARALLELISM = int(os.environ.get("MATRIXARK_CROSS_SESSION_PARALLELISM", "4"))
DEFAULT_CROSS_SESSION_MIN_BUDGET_TOKENS = int(os.environ.get("MATRIXARK_CROSS_SESSION_MIN_BUDGET_TOKENS", "256"))
DEFAULT_CROSS_SESSION_MIN_SCORE = float(os.environ.get("MATRIXARK_CROSS_SESSION_MIN_SCORE", "0.20"))
DEFAULT_CROSS_SESSION_RAW_EVIDENCE_MIN_SCORE = float(os.environ.get("MATRIXARK_CROSS_SESSION_RAW_EVIDENCE_MIN_SCORE", "0.45"))
DEFAULT_CROSS_SESSION_MAX_BUDGET_RATIO = 0.50  # the guard, not the setting: the share below is what decides
DEFAULT_CROSS_SESSION_PROFILE_MAX_BUDGET_RATIO = 0.60  # the guard, not the setting: the share below is what decides
DEFAULT_CROSS_SESSION_PROFILE_MAX_SESSIONS = int(os.environ.get("MATRIXARK_CROSS_SESSION_PROFILE_MAX_SESSIONS", "6"))
DEFAULT_CROSS_SESSION_PROFILE_MAX_CANDIDATES = int(os.environ.get("MATRIXARK_CROSS_SESSION_PROFILE_MAX_CANDIDATES", "48"))
DEFAULT_CROSS_SESSION_PROFILE_MIN_ENTITY_BRIDGE_REFS = int(os.environ.get("MATRIXARK_CROSS_SESSION_PROFILE_MIN_ENTITY_BRIDGE_REFS", "3"))
DEFAULT_CROSS_SESSION_PREFERRED_REF_TYPES = tuple(
    item.strip()
    for item in os.environ.get("MATRIXARK_CROSS_SESSION_PREFERRED_REF_TYPES", "entity,summary,compression").split(",")
    if item.strip()
)
DEFAULT_SHARED_RESOURCE_BUDGET_RATIO = 0.25  # build default; live_float reads the environment
DEFAULT_SHARED_RESOURCE_MAX_BUDGET_RATIO = 0.50  # the guard, not the setting: the share below is what decides
DEFAULT_SHARED_RESOURCE_MAX_BUDGET_TOKENS = 262144  # build default; live_int reads the environment
DEFAULT_SHARED_SKILL_BUDGET_RATIO = 0.10  # build default; live_float reads the environment
DEFAULT_SHARED_SKILL_MAX_BUDGET_RATIO = 0.50  # the guard, not the setting: the share below is what decides
DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS = 262144  # build default; live_int reads the environment
DEFAULT_SHARED_CONTEXT_MIN_SCORE = float(os.environ.get("MATRIXARK_SHARED_CONTEXT_MIN_SCORE", "0.20"))
TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE", "256"))
TIME_COMPRESSION_WINDOW_EVENTS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_WINDOW_EVENTS", "64"))
TIME_COMPRESSION_MIN_EVENTS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MIN_EVENTS", "8"))
TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH", "4"))
TIME_COMPRESSION_MIN_EVENT_AGE_MS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MIN_EVENT_AGE_MS", "0"))
TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS", str(30 * 24 * 60 * 60 * 1000)))
TIME_COMPRESSION_REINFORCEMENT_PROTECT_MS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_REINFORCEMENT_PROTECT_MS", str(30 * 24 * 60 * 60 * 1000)))
TIME_COMPRESSION_SUMMARY_PROVIDER = os.environ.get("MATRIXARK_TIME_COMPRESSION_SUMMARY_PROVIDER", "deterministic").strip().lower()
TIME_COMPRESSION_SUMMARY_MODEL = os.environ.get("MATRIXARK_TIME_COMPRESSION_SUMMARY_MODEL", os.environ.get("OPENAI_MODEL", "gpt-4o-mini"))
TIME_COMPRESSION_SUMMARY_BASE_URL = os.environ.get("MATRIXARK_TIME_COMPRESSION_SUMMARY_BASE_URL", os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")).rstrip("/")
TIME_COMPRESSION_SUMMARY_API_KEY_ENV = os.environ.get("MATRIXARK_TIME_COMPRESSION_SUMMARY_API_KEY_ENV", "OPENAI_API_KEY")
TIME_COMPRESSION_SUMMARY_TIMEOUT_SEC = float(os.environ.get("MATRIXARK_TIME_COMPRESSION_SUMMARY_TIMEOUT_SEC", "30"))
TIME_COMPRESSION_REQUIRE_LLM_SUMMARY = env_bool("MATRIXARK_REQUIRE_LLM_TIME_COMPRESSION", False)
EXTRACTION_LLM_MODEL = os.environ.get("MATRIXARK_EXTRACTION_MODEL", os.environ.get("OPENAI_MODEL", "qwen2.5:1.5b"))
EXTRACTION_LLM_BASE_URL = os.environ.get("MATRIXARK_EXTRACTION_BASE_URL", os.environ.get("OPENAI_BASE_URL", "http://127.0.0.1:8000/v1")).rstrip("/")
EXTRACTION_LLM_API_KEY_ENV = os.environ.get("MATRIXARK_EXTRACTION_API_KEY_ENV", "OPENAI_API_KEY")
EXTRACTION_LLM_TIMEOUT_SEC = float(os.environ.get("MATRIXARK_EXTRACTION_TIMEOUT_SEC", "30"))
EXTRACTION_LLM_MAX_TOKENS = int(os.environ.get("MATRIXARK_EXTRACTION_MAX_TOKENS", "1200"))
# Anthropic / Claude GENERATION provider (extraction + summary; Anthropic has no embeddings
# API, so this is the generation side only). Mirrors the OpenAI-compatible extraction config
# above but targets the Anthropic Messages API (POST /v1/messages; x-api-key + anthropic-version
# headers; response text in .content[0].text). Default-off: only selected when the request's
# understanding_provider resolves to anthropic/claude/anthropic_messages.
ANTHROPIC_LLM_MODEL = os.environ.get("MATRIXARK_ANTHROPIC_MODEL", "claude-sonnet-5")
ANTHROPIC_API_BASE = os.environ.get("MATRIXARK_ANTHROPIC_API_BASE", "https://api.anthropic.com").rstrip("/")
ANTHROPIC_LLM_API_KEY_ENV = os.environ.get("MATRIXARK_EXTRACTION_API_KEY_ENV", "ANTHROPIC_API_KEY")
ANTHROPIC_API_VERSION = os.environ.get("MATRIXARK_ANTHROPIC_VERSION", "2023-06-01")
ANTHROPIC_LLM_TIMEOUT_SEC = float(os.environ.get("MATRIXARK_ANTHROPIC_TIMEOUT_SEC", os.environ.get("MATRIXARK_EXTRACTION_TIMEOUT_SEC", "30")))
ANTHROPIC_LLM_MAX_TOKENS = int(os.environ.get("MATRIXARK_ANTHROPIC_MAX_TOKENS", os.environ.get("MATRIXARK_EXTRACTION_MAX_TOKENS", "1200")))
SUMMARY_LLM_PROVIDER = os.environ.get(
    "MATRIXARK_SUMMARY_PROVIDER",
    os.environ.get("MATRIXARK_UNDERSTANDING_PROVIDER", os.environ.get("MATRIXARK_EXTRACTION_PROVIDER", "deterministic")),
).strip().lower().replace("-", "_")
# The summary IS the extraction model. It used to be `get(MATRIXARK_SUMMARY_MODEL, ...)`, a
# second name for a call made against the extraction endpoint with the extraction key -- so the two
# could name models that endpoint does not both serve, and the portal offered no way to see that.
SUMMARY_LLM_MODEL = EXTRACTION_LLM_MODEL
SUMMARY_LLM_MAX_TOKENS = int(os.environ.get("MATRIXARK_SUMMARY_MAX_TOKENS", "900"))
# ENABLE_LLM_MERGE_OPERATOR comes from matrixark_mcp_runtime_config, above.
DEFAULT_ENTITY_MERGE_OPERATOR = os.environ.get("MATRIXARK_ENTITY_MERGE_OPERATOR", "EUA_MERGE").strip().upper() or "EUA_MERGE"
_OSS_SEGMENT_MODEL_CACHE: dict[str, Any] = {}
_OSS_EMBEDDING_MODEL_CACHE: dict[str, Any] = {}
_OSS_UNDERSTANDING_PROTOTYPE_CACHE: dict[str, dict[str, list[float]]] = {}
_EMBEDDING_VECTOR_CACHE: dict[tuple[str, str], list[float]] = {}
_EMBEDDING_VECTOR_CACHE_LOCK = threading.RLock()
_EMBEDDING_FALLBACK_USED = False
_DIRECT_RECORD_CACHE: dict[str, tuple[int, list[Json]]] = {}
_DIRECT_RECORD_CACHE_LOCK = threading.RLock()
_DIRECT_RECORD_CACHE_MAX_PREFIXES = 64
_DIRECT_RECORD_LOAD_LOCKS: dict[str, threading.RLock] = {}
_DIRECT_RETRIEVAL_CANDIDATE_CACHE: dict[str, Json] = {}
_DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK = threading.RLock()
_DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES = int(os.environ.get("MATRIXARK_DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES", "256"))
_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE: dict[str, list[tuple[str, int, int, Json]]] = {}
_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK = threading.RLock()
_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES = int(os.environ.get("MATRIXARK_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES", "1024"))


def matrixark_production_profile_enabled() -> bool:
    return MATRIXARK_MCP_PROFILE in {"prod", "production", "benchmark", "bench", "parity"}


def python_hot_cache_allowed(*, backend_label: str = "") -> bool:
    """Whether the record cache may serve a read. Native backends are excluded BY POLICY.

    The cache itself is safe on any backend: it is validated by the record COUNT read fresh from
    the store on every call, and the record log is append-only -- a delete is a tombstone append --
    so any write anywhere moves the count and the entry misses. The mutable half, the
    latest-context-state store, is never cached; it is re-read and re-folded even on a hit.

    It is nonetheless off for the native backends on purpose, matching
    `native_candidate_prefilter_required`: TemporalStore serving is meant to read through native
    pushdown rather than a Python-side cache. `test_python_hot_cache_default_policy` pins that.

    Turning it on is a measured, one-variable win if a deployment wants it --
    MATRIXARK_ALLOW_PYTHON_HOT_CACHE=1. Over the mem0 surface, 4 users x a 10-turn conversation:
    get_all 1491->326 ms, get 1677->402 ms, update 5050->1218 ms, users 1512->475 ms,
    batch_update 7782->3144 ms, search 710->510 ms, with all 15 APIs still correct on both arms.
    """
    if MATRIXARK_ALLOW_PYTHON_HOT_CACHE:
        return MATRIXARK_ALLOW_PYTHON_HOT_CACHE in {"1", "true", "yes"}
    return backend_label == "local"


def native_candidate_prefilter_required(*, backend_label: str = "") -> bool:
    if MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER:
        return MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER in {"1", "true", "yes"}
    return backend_label != "local"

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
    "skill": 0.84,
    "resource": 0.68,
    "dialogue_batch": 0.45,
    "session": 0.45,
}
MATRIXARK_ADMIN_SCOPES = {"admin:account", "admin:tenant", "admin:user", "admin:api_key", "admin:sso", "admin:audit", "portal:read"}
MATRIXARK_CONTEXT_SCOPES = {
    "context:ingest",
    "context:retrieve",
    "context:forget",
    "context:feedback",
    "context:replay",
    "resource:ingest",
    "resource:read",
    "resource:manage",
    "skill:read",
    "skill:manage",
}
MATRIXARK_ALL_SCOPES = MATRIXARK_CONTEXT_SCOPES | MATRIXARK_ADMIN_SCOPES
MATRIXARK_TOOL_SCOPES: dict[str, set[str]] = {
    "matrixark_ingest": {"context:ingest"},
    "matrixark_batch_extract": {"context:ingest"},
    "matrixark_session_commit": {"context:ingest"},
    "matrixark_refresh_summaries": {"context:ingest"},
    "matrixark_retrieve": {"context:retrieve"},
    "matrixark_embedding_status": {"context:retrieve"},
    "matrixark_forget": {"context:forget"},
    "matrixark_delete": {"context:forget"},
    "matrixark_reset": {"context:forget"},
    "matrixark_get_all": {"context:retrieve"},
    "matrixark_list_users": {"context:retrieve"},
    "matrixark_get_resource_content": {"context:retrieve"},
    "matrixark_get_memory": {"context:retrieve"},
    "matrixark_get_memory_by_key": {"context:retrieve"},
    "matrixark_update_memory": {"context:ingest"},
    "matrixark_memory_history": {"context:retrieve"},
    # A write about a memory, so it gates like a write rather than like a read.
    "matrixark_memory_feedback": {"context:ingest"},
    "matrixark_ingestion_dashboard": {"context:replay"},
    "matrixark_management_portal": {"portal:read"},
    "matrixark_auth_sso_login": set(),
    "matrixark_list_resources": {"resource:read"},
    "matrixark_list_skills": {"skill:read"},
    "matrixark_update_skill": {"skill:manage"},
    "matrixark_feedback": {"context:feedback"},
    "matrixark_replay": {"context:replay"},
    "matrixark_admin_create_account": {"admin:account"},
    "matrixark_admin_update_account": {"admin:account"},
    "matrixark_admin_list_accounts": {"admin:account"},
    "matrixark_admin_create_user": {"admin:user"},
    "matrixark_admin_update_user": {"admin:user"},
    "matrixark_admin_list_users": {"admin:user"},
    "matrixark_admin_create_api_key": {"admin:api_key"},
    "matrixark_admin_apply_api_key": {"admin:api_key", "admin:account", "admin:user"},
    "matrixark_admin_list_api_keys": {"admin:api_key"},
    "matrixark_admin_rotate_api_key": {"admin:api_key"},
    "matrixark_admin_revoke_api_key": {"admin:api_key"},
    "matrixark_admin_map_sso_user": {"admin:sso"},
    "matrixark_admin_audit": {"admin:audit"},
    "matrixark_backend_ready": set(),
    "matrixark_backend_metrics": set(),
}

MATRIXARK_ROLE_SCOPE_LIMITS: dict[str, set[str] | None] = {
    "owner": None,
    "admin": None,
    "operator": {
        "portal:read",
        "admin:audit",
        "context:ingest",
        "context:retrieve",
        "context:forget",
        "context:feedback",
        "context:replay",
        "resource:ingest",
        "resource:read",
        "resource:manage",
        "skill:read",
        "skill:manage",
    },
    "developer": {
        "portal:read",
        "context:ingest",
        "context:retrieve",
        "context:feedback",
        "context:replay",
        "resource:ingest",
        "resource:read",
        "skill:read",
    },
    "viewer": {"portal:read", "context:retrieve", "context:replay", "resource:read", "skill:read"},
    # Scoped service keys are capability-limited by their explicit scopes and
    # optional user/session allow-lists. They may be used by Codex, Claude,
    # Cursor, CI, or backend agents without forcing a human role name.
    "service": None,
    "local_agent": None,
    "dev_admin": None,
}



def scope_from_serving_record(record: Json) -> Json:
    scope = record.get("scope")
    if isinstance(scope, dict) and scope:
        return scope
    scope_key = str(record.get("scope_key") or "")
    return {"scope_key": scope_key} if scope_key else {}


def node_id_ref(node_hash: int) -> Json:
    return {"node_hash": node_hash, "node_id": node_hash}


def compact_model_slug(model_name: str) -> str:
    cleaned = str(model_name or "").replace("\\", "/").strip().strip("/")
    if not cleaned:
        return "model"
    parts = [part for part in cleaned.split("/") if part]
    tail = "/".join(parts[-2:]) if len(parts) >= 2 else parts[0]
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", tail).strip("_").lower()
    return (slug or "model")[:40]


def embedding_model_ref_for_name(model_name: str) -> str:
    slug = compact_model_slug(model_name)
    suffix = stable_hash(f"embedding_model:{model_name}") % 10000
    return f"emb:{slug}:{suffix:04d}"


def same_embedding_model(left: str, right: str) -> bool:
    """Whether two names refer to one encoder.

    A repository prefix is not part of the identity: `sentence-transformers/all-MiniLM-L6-v2` and
    `all-MiniLM-L6-v2` are one model, and both forms occur in real configuration -- a store written
    under one and queried under the other must not read as an encoder change. Getting this wrong is
    worse than not checking at all, because it declines every stored vector over a rename that
    changed nothing.

    Compared on the final path segment, which is the model name; the prefix is where it was fetched
    from. Case is significant -- these are identifiers, not prose.
    """
    left = str(left or "").strip().rstrip("/")
    right = str(right or "").strip().rstrip("/")
    if not left or not right:
        return False
    if left == right:
        return True
    return left.rsplit("/", 1)[-1] == right.rsplit("/", 1)[-1]


def embedding_model_conflicts(stored_model: str, active_model: str) -> bool:
    """Whether a stored vector was written by a different encoder than the one asking.

    Mirrors `context_embedding_model_conflicts` in the engine, and must keep mirroring it: the two
    decide the same question on two paths over the same store, and a store where one path scores a
    vector the other declines ranks differently depending on which binary served the request.

    Width is not the signal. Any two models truncated to a common width look identical, so a swap
    produces no length mismatch and no error; the recorded model name is the only thing separating
    them.

    An empty name on either side means UNKNOWN and never conflicts. A stored blank predates the
    field being written, and an active blank means nothing named an encoder -- treating either as a
    conflict would decline every vector in every older store and take retrieval dark, which is the
    outcome this guard exists to prevent.
    """
    stored = str(stored_model or "").strip()
    active = str(active_model or "").strip()
    if not stored or not active:
        return False
    return not same_embedding_model(stored, active)


def context_model_registry_record(model_name: str, *, model_kind: str = "embedding", updated_at_ms: int | None = None) -> Json:
    model_name = str(model_name or "").strip()
    model_hash = stable_hash(f"{model_kind}_model:{model_name}")
    return {
        "record_type": "context_model_registry",
        "model_kind": model_kind,
        "model_ref": embedding_model_ref_for_name(model_name) if model_kind == "embedding" else f"{model_kind}:{compact_model_slug(model_name)}:{model_hash % 10000:04d}",
        "model_name": model_name,
        "model_hash": model_hash,
        "provider": os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic") if model_kind == "embedding" else "",
        "execution_mode": embedding_execution_mode_name() if model_kind == "embedding" else "",
        "updated_at_ms": int(updated_at_ms or now_ms()),
    }


def context_model_registry_records(records: list[Json]) -> list[Json]:
    models: dict[str, int] = {}
    for record in records:
        if str(record.get("record_type") or "") != "context_embedding":
            continue
        model_name = str(record.get("model") or "").strip()
        if not model_name:
            continue
        updated_at_ms = record.get("updated_at_ms") or record.get("created_at_ms") or now_ms()
        try:
            timestamp = int(updated_at_ms)
        except (TypeError, ValueError):
            timestamp = now_ms()
        models[model_name] = max(models.get(model_name, 0), timestamp)
    return [context_model_registry_record(model_name, updated_at_ms=timestamp) for model_name, timestamp in sorted(models.items())]


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

    This mirrors a single-pass extraction idea: compile the desired memory outputs
    into one schema and process the input session once. The local MVP uses
    deterministic rules, while a production provider can replace this function
    with one GPT-4o-mini/OSS call that emits the same JSON shape.
    """

    provider = understanding_provider(envelope)
    # A provider that fails falls back to the deterministic rules below -- which is the right
    # behaviour, but the result used to still advertise `understanding_provider: <the provider>`.
    # That is actively misleading: an LLM call that times out (the client timeout is 30s by
    # default, and a 7B on CPU needs minutes) produced rule-based entities while the payload
    # claimed the LLM had run, so anyone measuring extraction quality would silently measure the
    # deterministic path instead. The failure is recorded and the label corrected below.
    provider_failure = ""
    if provider in {"openai", "openai_compatible", "openai_compatible_llm"}:
        try:
            return openai_compatible_one_pass_memory_extraction(envelope, prior_context=prior_context)
        except MatrixArkError as exc:
            if require_oss_understanding():
                raise
            provider_failure = str(exc)
    if provider in {"anthropic", "claude", "anthropic_messages"}:
        try:
            return anthropic_one_pass_memory_extraction(envelope, prior_context=prior_context)
        except MatrixArkError as exc:
            if require_oss_understanding():
                raise
            provider_failure = str(exc)
    messages = envelope["messages"]
    batch_text = text_from_messages(messages)
    batch_terms = tokens(batch_text)
    segments, segment_provider_meta = detect_memory_segments(messages, envelope)
    if not segments and batch_text.strip():
        segments = [
            {
                "topic": infer_segment_topic(batch_text),
                "coordinate_tuples": [[0, max(0, len(messages) - 1)]],
                "message_indexes": list(range(len(messages))),
                "saliency_score": round(max(0.5, semantic_saliency_score(batch_text)), 6),
                "summary_text": summarize_text(batch_text, limit=420),
                "text": batch_text,
                "non_contiguous": False,
                "segment_origin": "fallback_derived_from_events",
                "derived_from_context_events": True,
            }
        ]
        segment_provider_meta = {
            **segment_provider_meta,
            "fallback_used": True,
            "fallback_reason": "empty_segment_output",
            "segment_count": len(segments),
        }
    if provider == "oss_encoder":
        entities = oss_encoder_extract_batch_entities(messages, envelope)
        event_type = oss_encoder_event_type(batch_text)
    else:
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
        "mode": "matrixark_one_pass_schema_oss_encoder" if provider == "oss_encoder" else "matrixark_one_pass_schema",
        "understanding_provider": "deterministic" if provider_failure else provider,
        "understanding_provider_requested": provider if provider_failure else "",
        "understanding_provider_error": provider_failure,
        "schema": ONE_PASS_MEMORY_SCHEMA,
        "classification": classification,
        "status": "observed",
        "event_type": event_type,
        "entities": entities,
        "segments": segments,
        "segment_provider": segment_provider_meta,
        "indexes": indexes[:8],
        "batch_summary": summarize_text(batch_text, limit=700),
        "message_count": len(messages),
        "token_count_estimate": len(batch_terms),
        "prior_context": prior_context.get("level", ""),
        "prior_refs": prior_context.get("refs", []),
        "prior_message_count": len(prior_context.get("messages", [])),
        "prior_summary_count": len(prior_context.get("summaries", [])),
    }



def detect_memory_segments(messages: list[Json], envelope: Json | None = None) -> tuple[list[Json], Json]:
    envelope = envelope or {}
    provider = str(envelope.get("segment_provider") or os.getenv("MATRIXARK_SEGMENT_PROVIDER", "deterministic")).strip().lower()
    if provider in {"oss_encoder", "oss-encoder", "embedding"}:
        segments = oss_encoder_memory_segments(messages)
        return segments, {
            "provider": "oss_encoder",
            "execution_mode": "oss_embedding_model",
            "model": embedding_model_name(),
            "fallback_used": False,
            "segment_count": len(segments),
        }
    if provider in {"", "deterministic", "rules", "local"}:
        if require_oss_understanding():
            raise MatrixArkError("deterministic segmentation is disabled because MATRIXARK_REQUIRE_OSS_UNDERSTANDING=1")
        return intelligent_memory_segments(messages), {
            "provider": "deterministic",
            "execution_mode": "rules",
            "model": "matrixark-local-segmentation-v1",
            "fallback_used": False,
        }

    fallback_enabled = bool(envelope.get("segment_provider_fallback", False)) or provider in {"oss-fallback", "oss_with_fallback"} or os.getenv("MATRIXARK_SEGMENT_PROVIDER_FALLBACK", "").lower() in {"1", "true", "yes"}
    if provider in {"oss", "oss-fallback", "oss_with_fallback"}:
        model = str(envelope.get("segment_model") or os.getenv("MATRIXARK_SEGMENT_MODEL", "Qwen/Qwen2.5-0.5B-Instruct"))
        model_path = str(envelope.get("segment_model_path") or os.getenv("MATRIXARK_SEGMENT_MODEL_PATH", ""))
        max_new_tokens = int(envelope.get("segment_max_new_tokens") or os.getenv("MATRIXARK_SEGMENT_MAX_NEW_TOKENS", "512"))
        try:
            raw = oss_model_memory_segments(
                messages,
                model=model,
                model_path=model_path,
                max_new_tokens=max_new_tokens,
                local_only=fallback_enabled,
            )
            segments = normalize_model_segments(raw, messages)
            return segments, {
                "provider": "oss",
                "execution_mode": "oss_model",
                "model": model_path or model,
                "fallback_used": False,
                "segment_count": len(segments),
            }
        except Exception as exc:  # pragma: no cover - optional local model stack.
            if not fallback_enabled:
                raise MatrixArkError(f"OSS segment provider failed: {exc}") from exc
            segments = intelligent_memory_segments(messages)
            return segments, {
                "provider": "oss",
                "execution_mode": "rules_fallback",
                "model": model_path or model,
                "fallback_used": True,
                "fallback_reason": str(exc),
                "segment_count": len(segments),
            }
    raise MatrixArkError("segment_provider must be deterministic, oss, or oss-fallback")


def build_segment_prompt(messages: list[Json]) -> str:
    indexed = "\n".join(f"{index}. {message.get('role', 'user')}: {message.get('content', '')}" for index, message in enumerate(messages))
    return (
        "You are MatrixArk's memory segmentation extractor. Identify high-saliency memory segments from the indexed conversation. "
        "Prune greetings, acknowledgements, and filler. Merge semantically related non-contiguous messages into the same segment. "
        "Return only valid JSON with this shape: "
        '{"segments":[{"topic":"short_snake_case","coordinate_tuples":[[start,end]],"message_indexes":[0],"saliency_score":0.0,"summary_text":"short summary"}]} '
        "Indexes are zero-based and coordinate end is inclusive. Do not include messages that are only filler.\n\n"
        f"Conversation:\n{indexed}\n\nJSON:"
    )


def parse_first_json_object(text: str) -> Json:
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            value, _end = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    raise MatrixArkError("model response did not contain a JSON object")


def oss_model_memory_segments(messages: list[Json], *, model: str, model_path: str = "", max_new_tokens: int = 512, local_only: bool = False) -> Json:
    try:
        import torch  # type: ignore
        from transformers import AutoModelForCausalLM, AutoTokenizer  # type: ignore
    except Exception as exc:  # pragma: no cover - depends on optional OSS stack.
        raise MatrixArkError("torch and transformers are required for segment_provider=oss") from exc

    target = model_path or model
    cache_key = f"{target}:{max_new_tokens}"
    cached = _OSS_SEGMENT_MODEL_CACHE.get(cache_key)
    if cached is None:
        local_only = bool(local_only) or bool(model_path) or os.getenv("MATRIXARK_SEGMENT_MODEL_LOCAL_ONLY", "").lower() in {"1", "true", "yes"}
        tokenizer = AutoTokenizer.from_pretrained(target, local_files_only=local_only)
        model_obj = AutoModelForCausalLM.from_pretrained(target, local_files_only=local_only)
        device = "cuda" if torch.cuda.is_available() else "cpu"
        model_obj.to(device)
        model_obj.eval()
        cached = {"tokenizer": tokenizer, "model": model_obj, "device": device}
        _OSS_SEGMENT_MODEL_CACHE[cache_key] = cached
    tokenizer = cached["tokenizer"]
    model_obj = cached["model"]
    device = cached["device"]
    prompt = build_segment_prompt(messages)
    if getattr(tokenizer, "chat_template", None):
        chat = [
            {"role": "system", "content": "Return only JSON. No markdown."},
            {"role": "user", "content": prompt},
        ]
        encoded = tokenizer.apply_chat_template(chat, add_generation_prompt=True, return_tensors="pt")
        # transformers >= 5 returns a BatchEncoding (input_ids + attention_mask) here, where older
        # versions returned a bare tensor. Passing the mapping positionally to generate() made it
        # read `.shape` off a dict-like and raise a bare AttributeError, so the OSS extractor could
        # not run at all on a current transformers. Accept both shapes.
        if hasattr(encoded, "keys"):
            inputs = {key: value.to(device) for key, value in dict(encoded).items()}
            prompt_length = inputs["input_ids"].shape[-1]
            outputs = model_obj.generate(**inputs, max_new_tokens=max_new_tokens, do_sample=False)
        else:
            input_ids = encoded.to(device)
            prompt_length = input_ids.shape[-1]
            outputs = model_obj.generate(input_ids, max_new_tokens=max_new_tokens, do_sample=False)
        generated = outputs[0][prompt_length:]
        response = tokenizer.decode(generated, skip_special_tokens=True)
    else:
        inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=4096)
        inputs = {key: value.to(device) for key, value in inputs.items()}
        outputs = model_obj.generate(**inputs, max_new_tokens=max_new_tokens, do_sample=False)
        generated = outputs[0][inputs["input_ids"].shape[-1]:]
        response = tokenizer.decode(generated, skip_special_tokens=True)
    return parse_first_json_object(response)


def normalize_model_segments(raw: Any, messages: list[Json]) -> list[Json]:
    if isinstance(raw, list):
        raw_segments = raw
    elif isinstance(raw, dict) and isinstance(raw.get("segments"), list):
        raw_segments = raw["segments"]
    else:
        raise MatrixArkError("OSS segment provider must return {segments:[...]}")
    max_index = len(messages) - 1
    normalized: list[Json] = []
    for raw_segment in raw_segments[:12]:
        if not isinstance(raw_segment, dict):
            continue
        topic = re.sub(r"[^a-z0-9_]+", "_", str(raw_segment.get("topic") or "model_segment").lower()).strip("_") or "model_segment"
        coordinate_tuples = normalize_coordinate_tuples(raw_segment.get("coordinate_tuples"), max_index)
        message_indexes = normalize_message_indexes(raw_segment.get("message_indexes"), coordinate_tuples, max_index)
        if not message_indexes:
            continue
        if not coordinate_tuples:
            coordinate_tuples = contiguous_ranges(message_indexes)
        segment_text = "\n".join(f"{index}: {messages[index].get('content', '')}" for index in message_indexes)
        saliency = raw_segment.get("saliency_score", 0.85)
        try:
            saliency_score = max(0.0, min(1.0, float(saliency)))
        except (TypeError, ValueError):
            saliency_score = 0.85
        summary_text = str(raw_segment.get("summary_text") or summarize_text(segment_text, limit=420))
        normalized.append(
            {
                "topic": topic,
                "coordinate_tuples": coordinate_tuples,
                "message_indexes": message_indexes,
                "saliency_score": round(saliency_score, 6),
                "summary_text": summarize_text(summary_text, limit=420),
                "text": segment_text,
                "non_contiguous": len(coordinate_tuples) > 1,
                "detected_by": "oss_model",
            }
        )
    normalized.sort(key=lambda item: (-item["saliency_score"], item["topic"]))
    return normalized


def normalize_coordinate_tuples(value: Any, max_index: int) -> list[list[int]]:
    ranges: list[list[int]] = []
    if not isinstance(value, list):
        return ranges
    for item in value:
        if not isinstance(item, list) or len(item) != 2:
            continue
        try:
            start = int(item[0])
            end = int(item[1])
        except (TypeError, ValueError):
            continue
        start = max(0, min(max_index, start))
        end = max(0, min(max_index, end))
        if end < start:
            start, end = end, start
        ranges.append([start, end])
    return ranges


def normalize_message_indexes(value: Any, coordinate_tuples: list[list[int]], max_index: int) -> list[int]:
    indexes: set[int] = set()
    if isinstance(value, list):
        for item in value:
            try:
                index = int(item)
            except (TypeError, ValueError):
                continue
            if 0 <= index <= max_index:
                indexes.add(index)
    for start, end in coordinate_tuples:
        indexes.update(range(start, end + 1))
    return sorted(indexes)

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
    if re.search(r"\b(prefer|favorite|approved|budget|plan|correction|instead|current|remember|important|moved|moving|located|location|live|lives|staying|deadline|owner|owns|reviewer|checklist|decision|decided|require|requires|required|incident|runbook|alert|outage|rollback|metric|latency|p95|p99|sla|policy|risk|blocked|blocker)\b", lower):
        score += 0.45
    if re.search(r"\b(is|means|because|therefore|warning|avoid|must|should|cannot|can|require|requires|required|blocked|blocker)\b", lower):
        score += 0.2
    if re.search(r"\b(\d{2,}|monday|tuesday|wednesday|thursday|friday|saturday|sunday|january|february|march|april|may|june|july|august|september|october|november|december)\b", lower):
        score += 0.1
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
        ("incident_runbook", ["incident", "runbook", "alert", "outage", "rollback", "postmortem"]),
        ("task_decision", ["decision", "decided", "owner", "owns", "deadline", "checklist", "reviewer", "require", "requires", "required"]),
        ("metric_sla", ["metric", "latency", "p95", "p99", "qps", "sla", "error rate"]),
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


UNDERSTANDING_LABELS: dict[str, str] = {
    "confirmation": "confirmation approval accepted answer yes correct looks good",
    "correction": "correction wrong changed updated instead stale fact",
    "preference_update": "user preference likes prefers favorite language tool choice",
    "plan_update": "future plan schedule going to next step planned trip",
    "status_update": "job role work status position current responsibility",
    "approval": "business approval purchase approval budget approval confirmed cost",
    "location": "current location city moved to lives in staying at",
    "relationship": "relationship manager sister brother teammate family person",
    "family_profile": "family profile pet dog cat child sibling household fact",
    "identity_profile": "user identity name nickname pronouns how to address user",
    "communication_profile": "user communication style language locale response format tone",
    "workspace_profile": "workspace repo branch remote build deployment operating system local folder constraint",
    "current_plan": "current plan goal user request requirement upcoming action task to complete next milestone",
    "session": "general conversation memory useful session fact",
}

QUERY_TYPE_LABELS: dict[str, str] = {
    "profile_memory": "question asks user profile long term memory profile entities profile summaries cross session memories across sessions feature parity features only no testing monitoring debugging evidence",
    "benchmark_quality": "question asks benchmark quality hit rate latency p50 p99 throughput locomo longmemeval memory evaluation",
    "date": "question asks when date before after yesterday tomorrow week month year",
    "current_state": "question asks current latest now still status preference location role valid state active goal task request requirement",
    "why_emotion": "question asks why reason feeling emotion because",
    "evidence": "question asks quote exact message evidence what did someone say",
    "procedure": "question asks procedure steps troubleshoot debug rollback runbook checklist how to fix",
    "broad_exploration": "question asks overview summarize broad exploration topics inventory what is known",
    "multi_hop": "question requires combining multiple sessions people facts cross conversation reasoning",
    "fact": "question asks a direct factual answer",
}

PROFILE_MEMORY_QUERY_RE = re.compile(
    r"\b(user profile|profile memory|long[- ]term memor(?:y|ies)|cross[- ]session memor(?:y|ies)|profile entit(?:y|ies)|profile summar(?:y|ies)|identity profile|communication profile|workspace profile|mem0|memory feature parity|feature parity|feature[- ]focused memor(?:y|ies)|feature[- ]focused|features? only|features? referring to|focuns on features?|focus(?:ed)? on features?|functionality only|memory functionalit(?:y|ies)|memory algorithms?|memory algos?|no testing|no teseting|no monitoring|no debugging|no evidence|no evident|session memory|remember about me|remember about|what should (?:i|you|we) remember|standing instructions?|standing preferences?|persistent instructions?|saved preferences?|know about (?:me|my|the user)|what (?:have|did) i (?:tell|told) you|what (?:are|were|do|did) my preferences|what do i prefer|do i prefer|my preferences|my .*?(?:policy|policies|instruction|instructions|preference|preferences)|told you before|from previous sessions?|across sessions?|across conversations?|between conversations?|how should (?:you|codex) (?:address|reply|respond|answer)|what (?:is|are) my (?:name|nickname|pronouns?|preferred language|preferred format|communication style|response style|workspace rules?|repo rules?|repository rules?|branch rules?|build rules?|deployment rules?)|what (?:workspace|repo|repository|branch|build|deployment|github|remote) rules? (?:do|should) (?:you|codex) remember|what (?:workflow|workflows|rules?|instructions?|preferences?) (?:do|should) (?:you|codex) follow)\b"
)

PROFILE_MEMORY_STANDING_RULE_QUERY_RE = re.compile(
    r"\b(?:which|what|where|should|must|need)\b.{0,80}\b(?:repo|repository|folder|workspace|worktree|ubuntu|wsl|linux|windows|branch|remote|github|main branch|build|deploy|deployment|push)\b.{0,80}\b(?:use|work|build|push|commit|rebase|download|clone|store|keep|follow|prefer)\b"
    r"|\b(?:use|work|build|push|commit|rebase|download|clone|store|keep|follow|prefer)\b.{0,80}\b(?:repo|repository|folder|workspace|worktree|ubuntu|wsl|linux|windows|branch|remote|github|main branch|build|deploy|deployment|push)\b.{0,80}\b(?:which|what|where|should|must|need)\b"
    r"|\b(?:which|what|where|should|must|need)\b.{0,80}\b(?:use|work|build|push|commit|rebase|download|clone|store|keep|follow|prefer)\b.{0,80}\b(?:repo|repository|folder|workspace|worktree|ubuntu|wsl|linux|windows|branch|remote|github|main branch|temporalstore|rustraft|matrixark)\b"
    r"|\b(?:what|which|how)\b.{0,80}\b(?:always|default|standing|persistent)\b.{0,80}\b(?:do|behav(?:e|ior)|rules?|instructions?|preferences?|follow|remember)\b"
    r"|\b(?:what|which|how)\b.{0,80}\b(?:do|behav(?:e|ior)|follow|remember)\b.{0,80}\b(?:always|by default|default|standing|persistent)\b"
    r"|\b(?:always|default|standing|persistent)\b.{0,80}\b(?:rules?|instructions?|preferences?|behaviou?r|workflow|workflows)\b"
)

FEATURE_MEMORY_QUERY_RE = re.compile(
    r"\b(?:mem0|feature parity|feature[- ]focused|features? only|features? referring to|focuns on features?|focus(?:ed)? on features?|functionalit(?:y|ies)|algorithms?|algos?|memory feature|session memory|profile memory|cross[- ]session memory|long[- ]term memory|live ingestion|memory ingestion|batch extraction|threshold|idle batch|profile promotion|retrieval budgets?|memory retrieval|secondary indexes?|context events?|context entit(?:y|ies)|context summaries?|contextpacks?)\b"
)
ACTIVE_MEMORY_GOAL_QUERY_RE = re.compile(
    r"\b(?:active|current|latest|next|ongoing|standing|persistent)\b.{0,80}\b(?:memory|retrieval|extraction|ingestion|context)\b.{0,80}\b(?:goal|focus|priority|work|feature|functionality|implementation|direction|instruction|preference)\b"
    r"|\b(?:what|which|how)\b.{0,80}\b(?:should|must|need|keep|continue|focus|prioriti[sz]e|work on)\b.{0,80}\b(?:memory|retrieval|extraction|ingestion|context)\b.{0,80}\b(?:goal|focus|priority|feature|functionality|implementation|direction)\b"
    r"|\b(?:memory|retrieval|extraction|ingestion|context)\b.{0,80}\b(?:goal|focus|priority|feature|functionality|implementation|direction)\b.{0,80}\b(?:active|current|latest|next|ongoing|standing|persistent)\b"
)

FEATURE_SCOPE_EXCLUSION_RE = re.compile(
    r"\b(?:no|not|skip|without|exclude|excluding|ignore|omit)\s+"
    r"(?:testing|teseting|tests?|monitoring|debugging|debug|evidence|evident|validation|benchmarks?)\b"
)
FEATURE_SCOPE_EXCLUDED_DIMENSION_RE = re.compile(
    r"\b(?:testing|teseting|tests?|monitoring|debugging|debug|evidence|evident|validation|benchmarks?)\b"
)
FEATURE_SCOPE_ONLY_RE = re.compile(
    r"\b(?:just|only|pure(?:ly)?|focus(?:ed)? on|prioriti[sz]e)\s+"
    r"(?:feature parity|features?|functionalit(?:y|ies)|implementation|algorithms?|algos?)\b"
    r"|\b(?:feature parity|features?|functionalit(?:y|ies)|implementation|algorithms?|algos?)\s+"
    r"(?:only|focus|focused|first|parity)\b"
    r"|\b(?:feature work only|features? only|functionality only|implementation only|code changes only)\b"
)

CODEX_OUTCOME_QUERY_RE = re.compile(r"\b(?:codex|assistant|agent)\b.{0,80}\b(?:implement(?:ed)?|fixed|changed|updated|configured|enabled|disabled|installed|migrated|recovered|restored|cleaned|validated|verified|push(?:ed)?|publish(?:ed)?|deploy(?:ed)?|release(?:d)?|merge(?:d)?|commit(?:ted)?|rebase(?:d)?|failed|blocked|blocker|done|outcome|decision|decided|next action)\b|\bwhat (?:was|were|did)\b.{0,80}\b(?:implement(?:ed)?|fixed|changed|updated|configured|enabled|disabled|installed|migrated|recovered|restored|cleaned|validated|verified|push(?:ed)?|publish(?:ed)?|deploy(?:ed)?|release(?:d)?|merge(?:d)?|commit(?:ted)?|failed|blocked|done)\b|\bwhat did (?:you|we)\b.{0,80}\b(?:implement|fix|change|update|configure|enable|disable|install|migrate|recover|restore|clean|validate|verify|push|publish|deploy|release|merge|commit|rebase|fail|block|decide|do)\b|\b(?:show|find|retrieve|summari[sz]e)\b.{0,80}\b(?:assistant decision|tool evidence|validation evidence|pushed commit|blocked work|failed validation|validation result|test result|tests? passed|pushed commit|deployment result|deploy(?:ed)? result|install(?:ed)? result|configured result|configuration result|recovery result|migration result|merge result|publish(?:ed)? result|release result|outcome facts?)\b|\b(?:what|which|show|find|retrieve|summari[sz]e)\b.{0,80}\b(?:tests? passed|validation (?:passed|result|evidence)|pushed commit|commit (?:was )?pushed|push result|rebase result|deploy(?:ed)? result|deployment result|install(?:ed)? result|configured result|configuration result|recovery result|migration result|merge result|publish(?:ed)? result|release result)\b|\bwhat (?:failed|was blocked|blocked)\b.{0,80}\b(?:memory work|work|validation|commit|push|deploy|deployment|install|configuration|migration|recovery|merge|publish|release|tool|codex|temporalstore)\b")

QUERY_INDEX_LABELS: dict[str, str] = {
    "entity_type:location": "location city moved lives staying where user is",
    "entity_type:preference": "preference prefer favorite likes language tool choice",
    "event_type:preference_update": "preference update changed choice likes prefers",
    "entity_type:relationship": "relationship manager sister brother teammate family person",
    "entity_type:family_profile": "family pet dog cat child household",
    "entity_type:job_status": "job role work status position responsibility",
    "entity_type:identity_profile": "identity name nickname pronouns call me address user",
    "entity_type:communication_profile": "communication style language locale concise detailed bullet format tone",
    "entity_type:workspace_profile": "workspace repo branch remote main github ubuntu wsl build deployment folder rust native temporalstore",
    "entity_type:memory_feature_profile": "memory feature parity mem0 extraction retrieval profile session threshold idle batch live ingestion profile promotion retrieval budget secondary index context event context entity context summary feature focused features only no testing monitoring debugging evidence",
    "event_type:status_update": "job status role work update",
    "entity_type:current_plan": "plan current plan goal user request requirement upcoming task schedule next milestone",
    "event_type:plan_update": "plan update going to schedule will next",
    "event_type:confirmation": "confirmation approved accepted yes correct confirmed",
    "entity_type:approval_state": "approval budget purchase cost approved",
    "entity_type:confirmation": "confirmation approved correct accepted",
    "classification:confirmation": "confirmation approved accepted yes correct",
    "segment_topic:approval_budget": "approval budget purchase GPU cost finance",
    "event_type:correction": "correction wrong changed updated instead stale",
    "entity_type:correction": "correction wrong changed updated instead",
    "classification:correction": "correction wrong changed update",
    "segment_topic:correction": "correction updated stale changed",
    "entity_type:assistant_decision": "assistant decision final answer done implemented chose decided next action",
    "entity_type:codex_next_action": "codex next action follow up plan todo upcoming work",
    "entity_type:codex_blocker": "codex blocker blocked failed error missing rejected fatal risk",
    "entity_type:codex_validation": "codex validation tests passed py_compile cargo check pytest verified",
    "entity_type:codex_publish_outcome": "codex outcome commit pushed origin main remote branch publication",
    "entity_type:codex_code_change": "codex changed implemented fixed added removed updated code change",
    "entity_type:codex_benchmark_result": "codex benchmark p50 p99 throughput latency workload result",
    "event_type:assistant_response": "assistant response final answer outcome done implemented fixed decision",
    "event_type:user_prompt": "user prompt request asks asked requirement instruction",
    "entity_type:tool_evidence": "tool evidence tests passed failed exit code commit push rebase validation benchmark blocker",
    "event_type:tool_evidence": "tool event evidence tests passed failed exit code commit push rebase validation benchmark blocker",
    "memory_scope:user_profile": "user profile long term memory profile entity profile summary durable cross session user state",
    "memory_scope:session": "same active session local conversation memory session scoped context",
    "session_continuity:cross_session": "cross session previous sessions other conversations across tasks persistent memory bridge",
    "session_continuity:same_session": "same session current conversation active turn local context",
    "profile_entity_current:true": "current profile entity latest durable user state standing preference instruction",
    "profile_summary_current:true": "current profile summary latest durable long term memory profile overview",
    "profile_memory_class:memory_feature": "memory feature profile feature parity extraction retrieval budget live ingestion profile promotion secondary index context event context entity context summary",
    "profile_memory_kind:memory_feature": "memory feature durable preference feature focused features only no testing no monitoring no debugging no evidence no evident live ingestion profile promotion retrieval budget",
    "memory_selection_policy:selected_profile_current_state": "selected profile current state standing instruction durable preference",
    "memory_selection_policy:selected_user_prompt": "selected user prompt explicit request instruction preference",
    "memory_selection_policy:selected_user_profile_fact": "selected user profile fact standing preference durable instruction",
    "memory_selection_policy:selected_assistant_profile_fact": "selected assistant stated user profile preference standing instruction durable memory",
    "memory_selection_policy:selected_assistant_decision_outcome_only": "selected assistant decision outcome final answer implemented changed pushed",
    "memory_selection_policy:selected_tool_evidence_only": "selected tool evidence validation test result commit push benchmark",
    "source_role:user": "user prompt explicit instruction preference request",
    "source_role:assistant": "assistant decision final answer outcome implementation",
    "source_role:tool": "tool evidence command output validation benchmark",
    "source_type:message": "raw message dialogue evidence",
    "source_type:feedback": "feedback accepted rejected final answer",
    "source_type:resource": "resource document file pdf markdown text csv table runbook policy docs",
    "source_type:skill": "skill tool instruction playbook procedure capability",
    "source_type:resource_fact": "extracted fact from resource decision owner cost deadline policy approval risk procedure api",
    "resource_type:pdf": "pdf document page file",
    "resource_type:md": "markdown md readme documentation runbook",
    "resource_type:txt": "text txt note plain document",
    "resource_type:csv": "csv table rows spreadsheet",
    "resource_type:tsv": "tsv table rows spreadsheet",
    "resource_type:xlsx": "excel xlsx spreadsheet workbook sheet table",
    "resource_type:html": "html web page documentation",
    "resource_type:docx": "word docx document",
    "resource_type:pptx": "powerpoint pptx slide deck presentation",
    "unit_kind:paragraph": "paragraph text passage",
    "unit_kind:heading": "heading section title",
    "unit_kind:table_row_group": "table rows row group csv spreadsheet",
    "unit_kind:page": "page pdf page",
    "unit_kind:slide": "slide presentation deck",
    "unit_kind:code_symbol": "code function class symbol",
    "skill_trigger:context_pack_replay": "context pack replay audit inspect selected refs",
    "skill_tool:matrixark_replay": "matrixark replay tool context replay",
    "skill_tool:matrixark_audit": "matrixark audit tool context audit",
}


def require_oss_understanding() -> bool:
    return os.getenv("MATRIXARK_REQUIRE_OSS_UNDERSTANDING", "").strip().lower() in {"1", "true", "yes"}


def understanding_provider(envelope: Json | None = None) -> str:
    provider = ""
    if envelope:
        provider = str(envelope.get("understanding_provider") or envelope.get("extraction_provider") or "")
    provider = provider or os.getenv("MATRIXARK_UNDERSTANDING_PROVIDER", os.getenv("MATRIXARK_EXTRACTION_PROVIDER", "deterministic"))
    provider = provider.strip().lower().replace("-", "_")
    if provider in {"oss", "open_source", "embedding", "oss_embedding"}:
        return "oss_encoder"
    if provider in {"", "deterministic", "rules", "local"} and require_oss_understanding():
        raise MatrixArkError("deterministic extraction/query understanding is disabled because MATRIXARK_REQUIRE_OSS_UNDERSTANDING=1")
    return provider or "deterministic"


def prototype_vectors(labels: dict[str, str]) -> dict[str, list[float]]:
    cache_key = json.dumps(labels, sort_keys=True) + "|" + embedding_model_name()
    cached = _OSS_UNDERSTANDING_PROTOTYPE_CACHE.get(cache_key)
    if cached is not None:
        return cached
    vectors = {label: embedding_for_text(description) for label, description in labels.items()}
    _OSS_UNDERSTANDING_PROTOTYPE_CACHE[cache_key] = vectors
    return vectors


def oss_encoder_rank_labels(text: str, labels: dict[str, str], *, limit: int = 5) -> list[Json]:
    query_vector = embedding_for_text(text)
    ranked = [
        {
            "label": label,
            "score": round(normalized_dense_score(cosine(query_vector, vector)), 6),
            "description": labels[label],
        }
        for label, vector in prototype_vectors(labels).items()
    ]
    ranked.sort(key=lambda item: item["score"], reverse=True)
    return ranked[:limit]


def oss_encoder_event_type(text: str) -> str:
    ranked = oss_encoder_rank_labels(text, UNDERSTANDING_LABELS, limit=1)
    label = str(ranked[0]["label"]) if ranked else "session"
    if label == "approval":
        return "confirmation"
    if label == "location":
        return "status_update"
    if label in {"relationship", "family_profile"}:
        return "dialogue_batch"
    return label


def oss_encoder_compact_extraction(envelope: Json, *, prior_context: Json) -> Json:
    text = text_from_messages(envelope["messages"])
    ranked = oss_encoder_rank_labels(text, UNDERSTANDING_LABELS, limit=5)
    top = str(ranked[0]["label"]) if ranked else "session"
    classification = "NEW_EVENT"
    status = "observed"
    if envelope["kind"] == "feedback":
        if not prior_context.get("level"):
            classification = "AMBIGUOUS"
        elif top in {"confirmation", "approval"}:
            classification = "CONFIRMATION"
            status = "accepted"
        elif top == "correction":
            classification = "CORRECTION"
            status = "rejected"
        else:
            classification = "FEEDBACK"
    return {
        "mode": "matrixark_internal_oss_encoder",
        "understanding_provider": "oss_encoder",
        "classification": classification,
        "status": status,
        "event_type": oss_encoder_event_type(text),
        "label_scores": ranked,
        "prior_context": prior_context.get("level", ""),
        "prior_refs": prior_context.get("refs", []),
        "prior_message_count": len(prior_context.get("messages", [])),
        "prior_summary_count": len(prior_context.get("summaries", [])),
        "quality_warning": "" if classification != "AMBIGUOUS" else "short feedback lacks prior context",
    }


def oss_encoder_extract_batch_entities(messages: list[Json], envelope: Json) -> list[Json]:
    text = text_from_messages(messages)
    ranked = oss_encoder_rank_labels(text, UNDERSTANDING_LABELS, limit=8)
    source_event_ids = envelope.get("source_event_ids", [])
    source_refs = [str(ref) for ref in source_event_ids] if isinstance(source_event_ids, list) and source_event_ids else [str(index) for index, _ in enumerate(messages)]
    entities: list[Json] = []
    for item in ranked:
        label = str(item["label"])
        if label == "approval":
            entity_type = "approval_state"
        elif label == "status_update":
            entity_type = "job_status"
        elif label == "plan_update":
            entity_type = "current_plan"
        elif label == "preference_update":
            entity_type = "preference"
        else:
            entity_type = label
        if float(item["score"]) < 0.42 and entity_type != "session":
            continue
        state = summarize_text(f"{entity_type}: {text}", limit=220)
        entities.append(
            {
                "entity_type": entity_type,
                "entity_name": canonical_entity_name(entity_type, state) or entity_type,
                "state": state,
                "confidence": round(float(item["score"]), 6),
                "source_refs": source_refs,
                "operator": normalize_entity_operator(None, entity_type),
                "field_patches": [entity_patch("", state)] if entity_type != "session" else [],
                "extracted_by": "oss_encoder",
            }
        )
    if not entities:
        entities.append(
            {
                "entity_type": "session",
                "entity_name": "session_memory",
                "state": summarize_text(text, limit=220),
                "confidence": 0.5,
                "source_refs": source_refs,
                "operator": normalize_entity_operator(None, "session"),
                "field_patches": [],
                "extracted_by": "oss_encoder",
            }
        )
    return dedupe_entities(entities)


def oss_encoder_memory_segments(messages: list[Json]) -> list[Json]:
    labeled: dict[str, list[tuple[int, Json, float]]] = {}
    for index, message in enumerate(messages):
        text = str(message.get("content", ""))
        if not text.strip():
            continue
        ranked = oss_encoder_rank_labels(text, UNDERSTANDING_LABELS, limit=1)
        label = str(ranked[0]["label"]) if ranked else "session"
        score = float(ranked[0]["score"]) if ranked else 0.5
        labeled.setdefault(label, []).append((index, message, score))
    segments = []
    for label, items in labeled.items():
        indexes = [index for index, _message, _score in items]
        ranges = contiguous_ranges(indexes)
        segment_text = "\n".join(f"{index}: {message.get('content', '')}" for index, message, _score in items)
        segments.append(
            {
                "topic": label,
                "coordinate_tuples": ranges,
                "message_indexes": indexes,
                "saliency_score": round(sum(score for _index, _message, score in items) / max(len(items), 1), 6),
                "summary_text": summarize_text(segment_text, limit=420),
                "text": segment_text,
                "non_contiguous": len(ranges) > 1,
                "detected_by": "oss_encoder",
            }
        )
    segments.sort(key=lambda item: (-item["saliency_score"], item["topic"]))
    return segments[:12]


def extract_batch_entities(messages: list[Json], envelope: Json) -> list[Json]:
    entities: list[Json] = []
    text = text_from_messages(messages)
    lower = text.lower()
    feature_scope_memory_only = feature_scope_excludes_outcome_evidence(text)
    source_event_ids = envelope.get("source_event_ids", [])
    source_refs = [str(ref) for ref in source_event_ids] if isinstance(source_event_ids, list) and source_event_ids else [str(index) for index, _ in enumerate(messages)]

    def source_ref_for_message_index(index: int) -> str:
        if isinstance(source_event_ids, list) and index < len(source_event_ids):
            return str(source_event_ids[index])
        return str(index)

    def source_refs_for_role(role_name: str) -> list[str]:
        refs: list[str] = []
        for index, item in enumerate(messages):
            if normalized_extraction_message_role(item.get("role")) != role_name:
                continue
            if not str(item.get("content") or "").strip():
                continue
            if isinstance(source_event_ids, list) and index < len(source_event_ids):
                refs.append(str(source_event_ids[index]))
            else:
                refs.append(str(index))
        return refs or source_refs

    def source_lineage_for_match(snippet: str) -> Json:
        needle = " ".join(str(snippet or "").lower().split()).strip()
        refs: list[str] = []
        role_counts: Json = {}
        for index, item in enumerate(messages):
            content = str(item.get("content") or "")
            if not content.strip():
                continue
            haystack = " ".join(content.lower().split())
            if needle and needle not in haystack:
                continue
            role = normalized_extraction_message_role(item.get("role"))
            if not role:
                continue
            if isinstance(source_event_ids, list) and index < len(source_event_ids):
                refs.append(str(source_event_ids[index]))
            else:
                refs.append(str(index))
            role_counts[role] = int(role_counts.get(role, 0)) + 1
        if not refs:
            for index, item in enumerate(messages):
                if not str(item.get("content") or "").strip():
                    continue
                role = normalized_extraction_message_role(item.get("role"))
                if not role:
                    continue
                if isinstance(source_event_ids, list) and index < len(source_event_ids):
                    refs.append(str(source_event_ids[index]))
                else:
                    refs.append(str(index))
                role_counts[role] = int(role_counts.get(role, 0)) + 1
        return {
            "source_refs": ordered_unique(refs) or source_refs,
            "source_roles": sorted(role_counts),
            "source_role_counts": role_counts,
        }

    def assistant_profile_fact_only_lineage(lineage: Json) -> bool:
        roles = set(lineage.get("source_roles") or [])
        if roles != {"assistant"}:
            return False
        lineage_refs = {str(ref) for ref in lineage.get("source_refs") or []}
        for index, item in assistant_message_items:
            ref = source_ref_for_message_index(index)
            if lineage_refs and ref not in lineage_refs:
                continue
            if assistant_profile_fact_memory_text(str(item.get("content") or "")):
                return True
        return False

    user_messages = [
        item
        for item in messages
        if normalized_extraction_message_role(item.get("role")) == "user"
        and str(item.get("content") or "").strip()
    ]
    user_text = text_from_messages(user_messages) if user_messages else ""
    user_profile_entity_type = profile_entity_type_for_memory_text(user_text)
    if user_profile_entity_type == "memory_feature_profile":
        state = summarize_text(f"memory feature policy: {user_text}", limit=220)
        entities.append(
            {
                "entity_type": user_profile_entity_type,
                "entity_name": user_profile_entity_type,
                "state": state,
                "confidence": 0.86,
                "source_refs": source_refs_for_role("user"),
                "source_roles": ["user"],
                "source_role_counts": {"user": len(user_messages) or 1},
                "operator": normalize_entity_operator(None, user_profile_entity_type),
                "field_patches": [entity_patch("", summarize_text(state, limit=180))],
            }
        )
    if user_text:
        user_directive_patterns = [
            ("current_plan", r"\bgoal\s*:\s*([^.;!?\n]{4,220})"),
            ("current_plan", r"\b(?:please\s+)?(?:implement|fix|add|remove|replace|move)\s+([^.;!?\n]{4,180})"),
            ("preference", r"\b(?:remember(?:\s+that)?|please\s+always|always|keep|use|prefer|make\s+sure(?:\s+to)?)\b[:\s]+([^.;!?\n]{4,180})"),
            ("preference", r"\b(?:do\s+not|don't|never|avoid|stop)\s+([^.;!?\n]{4,180})"),
            ("current_plan", r"\b(?:we\s+should|should|need\s+to|must|have\s+to|let's|lets|please)\s+([^.;!?\n]{4,180})"),
        ]
        for entity_type, pattern in user_directive_patterns:
            for match in re.finditer(pattern, user_text, re.IGNORECASE):
                directive = clean_patch_value(match.group(0))
                if not directive:
                    continue
                directive_entity_type = entity_type if entity_type == "current_plan" else profile_entity_type_for_memory_text(directive) or entity_type
                if directive_entity_type.endswith("_profile"):
                    prefix = "user profile"
                else:
                    prefix = "user directive" if directive_entity_type == "preference" else "user plan"
                state = summarize_text(f"{prefix}: {directive}", limit=220)
                entities.append(
                    {
                        "entity_type": directive_entity_type,
                        "entity_name": summarize_text(f"{directive_entity_type}:{directive}", limit=96),
                        "state": state,
                        "confidence": 0.86,
                        "source_refs": source_refs_for_role("user"),
                        "source_roles": ["user"],
                        "source_role_counts": {"user": 1},
                        "operator": normalize_entity_operator(None, directive_entity_type),
                        "field_patches": [entity_patch("", summarize_text(state, limit=180))],
                    }
                )
    tool_message_items = [
        (index, item)
        for index, item in enumerate(messages)
        if normalized_extraction_message_role(item.get("role")) == "tool"
        and str(item.get("content") or "").strip()
    ]
    tool_messages = [item for _index, item in tool_message_items]
    tool_text = text_from_messages(tool_messages) if tool_messages else ""
    tool_profile_entity_type = profile_entity_type_for_memory_text(tool_text)
    if tool_profile_entity_type == "memory_feature_profile":
        state = summarize_text(f"tool memory feature result: {tool_evidence_memory_text(tool_text)}", limit=220)
        entities.append(
            {
                "entity_type": tool_profile_entity_type,
                "entity_name": tool_profile_entity_type,
                "state": state,
                "confidence": 0.84,
                "source_refs": source_refs_for_role("tool"),
                "source_roles": ["tool"],
                "source_role_counts": {"tool": len(tool_messages)},
                "operator": normalize_entity_operator(None, tool_profile_entity_type),
                "field_patches": [entity_patch("", summarize_text(state, limit=180))],
            }
        )
    if tool_text and not feature_scope_memory_only:
        tool_refs = source_refs_for_role("tool")
        evidence_state = summarize_text(tool_evidence_memory_text(tool_text), limit=220)
        entities.append(
            {
                "entity_type": "tool_evidence",
                "entity_name": "tool_evidence",
                "state": evidence_state,
                "confidence": 0.86,
                "source_refs": tool_refs,
                "source_roles": ["tool"],
                "source_role_counts": {"tool": len(tool_messages)},
                "operator": normalize_entity_operator(None, "tool_evidence"),
                "field_patches": [entity_patch("", summarize_text(evidence_state, limit=180))],
            }
        )
        for message_index, message in tool_message_items:
            entities.extend(
                codex_outcome_fact_entities(
                    str(message.get("content") or ""),
                    role_name="tool",
                    source_refs=[source_ref_for_message_index(message_index)],
                    source_count=1,
                )
            )
    assistant_message_items = [
        (index, item)
        for index, item in enumerate(messages)
        if normalized_extraction_message_role(item.get("role")) == "assistant"
        and str(item.get("content") or "").strip()
    ]
    assistant_messages = [item for _index, item in assistant_message_items]
    assistant_text = text_from_messages(assistant_messages) if assistant_messages else ""
    assistant_refs = source_refs_for_role("assistant") if assistant_text else []
    if assistant_text:
        assistant_profile_entity_type = profile_entity_type_for_memory_text(assistant_text)
        if assistant_profile_entity_type == "memory_feature_profile":
            state = summarize_text(f"assistant memory feature policy: {assistant_text}", limit=220)
            entities.append(
                {
                    "entity_type": assistant_profile_entity_type,
                    "entity_name": assistant_profile_entity_type,
                    "state": state,
                    "confidence": 0.84,
                    "source_refs": assistant_refs,
                    "source_roles": ["assistant"],
                    "source_role_counts": {"assistant": len(assistant_messages)},
                    "operator": normalize_entity_operator(None, assistant_profile_entity_type),
                    "field_patches": [entity_patch("", summarize_text(state, limit=180))],
                }
            )
        assistant_profile_fact_patterns = [
            r"\b(?:i(?:'ll| will)?|codex will|assistant will)\s+(?:remember|keep|use|follow|prefer|avoid|stop using|not use|always use|make sure)\b[:\s]+([^.;!?\n]{4,220})",
            r"\b(?:noted|got it|understood|i(?:'ll| will)? remember|remembered)\b[:\s]+(?:that\s+)?([^.;!?\n]{4,220})",
            r"\b(?:i(?:'ll| will) keep|i(?:'ll| will) use|i(?:'ll| will) avoid|i(?:'ll| will) make sure)\s+([^.;!?\n]{4,220})",
        ]
        seen_assistant_profile_facts: set[str] = set()
        for pattern in assistant_profile_fact_patterns:
            for match in re.finditer(pattern, assistant_text, re.IGNORECASE):
                fact_text = clean_patch_value(match.group(1) if match.groups() else match.group(0))
                if not fact_text:
                    continue
                fact_key = re.sub(r"\s+", " ", fact_text.lower()).strip(" .,:;-")
                if any(fact_key in seen or seen in fact_key for seen in seen_assistant_profile_facts):
                    continue
                seen_assistant_profile_facts.add(fact_key)
                fact_entity_type = profile_entity_type_for_memory_text(fact_text) or "preference"
                state = summarize_text(f"assistant profile fact: {fact_text}", limit=220)
                entities.append(
                    {
                        "entity_type": fact_entity_type,
                        "entity_name": summarize_text(f"{fact_entity_type}:{fact_text}", limit=96),
                        "state": state,
                        "confidence": 0.84,
                        "source_refs": assistant_refs,
                        "source_roles": ["assistant"],
                        "source_role_counts": {"assistant": len(assistant_messages)},
                        "operator": normalize_entity_operator(None, fact_entity_type),
                        "field_patches": [entity_patch("", summarize_text(state, limit=180))],
                    }
                )
    if assistant_text and re.search(
        r"\b(?:decision|decided|done|implemented|fixed|committed|pushed|published|deployed|released|merged|rebased|configured|enabled|disabled|installed|migrated|recovered|restored|cleaned|will|next|choose|chose|use|keep|remove|blocked|blocker|failed|failure|error|rejected|updated|changed|validated|validation|verified|promoted|indexed|budgeted|batched|flushed|profile|cross[- ]session|memory|gap|risk|warning)\b",
        assistant_text,
        re.IGNORECASE,
    ):
        if not feature_scope_memory_only:
            decision_state = summarize_text(assistant_decision_memory_text(assistant_text), limit=220)
            entities.append(
                {
                    "entity_type": "assistant_decision",
                    "entity_name": "assistant_decision",
                    "state": decision_state,
                    "confidence": 0.82,
                    "source_refs": assistant_refs,
                    "source_roles": ["assistant"],
                    "source_role_counts": {"assistant": len(assistant_messages)},
                    "operator": normalize_entity_operator(None, "assistant_decision"),
                    "field_patches": [entity_patch("", summarize_text(decision_state, limit=180))],
                }
            )
            for message_index, message in assistant_message_items:
                entities.extend(
                    codex_outcome_fact_entities(
                        str(message.get("content") or ""),
                        role_name="assistant",
                        source_refs=[source_ref_for_message_index(message_index)],
                        source_count=1,
                    )
                )
    patterns = [
        ("preference", r"\b(?:prefer|prefers|favorite|likes?|loves?)\s+([^.;!?]{2,120})"),
        ("preference", r"\b(?:you|user)\s+(?:always|usually|prefer(?:s)?|like(?:s)?|want(?:s)?|need(?:s)?)\s+([^.;!?]{2,140})"),
        ("preference", r"\b(?:you|user)\s+(?:never|avoid(?:s)?|do(?:es)?\s+not|don't|doesn't|cannot|can't|should\s+not|must\s+not)\s+([^.;!?]{2,140})"),
        ("preference", r"\b(?:i(?:'ll| will)?\s+remember|remembered|noted|got it)[:\s]+(?:that\s+)?(?:you|user)\s+([^.;!?]{2,160})"),
        ("preference", r"\b(?:standing instruction|standing preference|saved preference|persistent instruction)[:\s]+([^.;!?]{2,180})"),
        ("relationship", r"\b(?:friend|partner|mother|father|sister|brother|wife|husband|manager|teammate)\s+([^.;!?]{0,120})"),
        # The old pattern ran to the end of the sentence, so "I live in Seattle and prefer
        # metric units" stored the location as "Seattle and prefer metric units" -- wrong, and
        # it then polluted the profile summary text that carries cross-session memory. Stop at
        # the clause boundary instead.
        ("location", r"\b(?:live|lives|moved|moving|located|staying)\s+(?:in|to|at)?\s*"
                     r"([^.;!?]{2,120}?)"
                     r"(?=\s+(?:and|but|so|then|because|for|while|which|since|although|though|"
                     r"however|before|after|when|during|with|to)\b|[.;!?]|$)"),
        ("job_status", r"\b(?:job|role|work|works|position|status)\s+(?:is|as|at|with)?\s*([^.;!?]{2,120})"),
        ("current_plan", r"\b(?:plan|plans|planning|going to|will)\s+([^.;!?]{2,140})"),
        ("current_plan", r"\b(?:you|user)\s+(?:asked|requested|required|requires|need(?:s)?|want(?:s)?)\s+(?:me\s+|codex\s+|us\s+|to\s+)?([^.;!?]{2,160})"),
        ("family_profile", r"\b(?:family|child|children|son|daughter|pet|dog|cat)\s+([^.;!?]{0,120})"),
        ("identity_profile", r"\b(?:call me|my name is|i am called|i'm called)\s+([^.;!?]{2,80})"),
        ("identity_profile", r"\b(?:user(?:'s)? name is|user goes by|user prefers to be called)\s+([^.;!?]{2,80})"),
        ("identity_profile", r"\b(?:my pronouns are|user(?:'s)? pronouns are)\s+([^.;!?]{2,80})"),
        ("communication_profile", r"\b(?:reply|respond|answer|write)\s+(?:to\s+me\s+)?(?:in|with|using)\s+([^.;!?]{2,140})"),
        ("communication_profile", r"\b(?:use|prefer|likes?|wants?)\s+([^.;!?]{2,120}?\b(?:tone|style|format|bullets?|bullet points?|markdown|language|locale|timezone|time zone|concise|detailed|brief))"),
        ("communication_profile", r"\b(?:communication style|response style|answer style|writing style|preferred language|preferred format|timezone|time zone|locale)[:\s]+([^.;!?]{2,160})"),
        ("workspace_profile", r"\b(?:always|please|must|should|use|keep|prefer)\s+([^.;!?]{2,180}?\b(?:ubuntu|wsl|linux|repo|repository|workspace|worktree|folder|branch|main|remote|github|rustraft|temporalstore|matrixark|build|deploy|deployment))"),
        ("workspace_profile", r"\b(?:do not|don't|never|avoid|stop)\s+([^.;!?]{2,180}?\b(?:windows|folder|repo|repository|worktree|branch|remote|build|deploy|deployment))"),
        ("workspace_profile", r"\b(?:workspace|repo|repository|branch|remote|github|build|deployment|deploy|ubuntu|wsl|linux|rustraft|temporalstore|matrixark)[:\s]+([^.;!?]{2,180})"),
        ("correction", r"\b(?:correction|correct|wrong|instead|updated|changed)\s+([^.;!?]{2,140})"),
        ("approval_state", r"\b(?:approved|approval)\s+([^.;!?]{2,140})"),
        ("confirmation", r"\b(?:yes|confirmed|approved|correct|looks good)\b([^.;!?]{0,120})"),
    ]
    profile_pattern_messages = [
        item
        for item in messages
        if normalized_extraction_message_role(item.get("role")) in {"user", "assistant"}
        and str(item.get("content") or "").strip()
    ]
    profile_pattern_text = text_from_messages(profile_pattern_messages)
    profile_pattern_lower = profile_pattern_text.lower()
    for entity_type, pattern in patterns:
        if not profile_pattern_text:
            break
        for match in re.finditer(pattern, profile_pattern_text, re.IGNORECASE):
            value = " ".join(match.group(1).split()).strip(" :-") if match.groups() else ""
            lineage = source_lineage_for_match(match.group(0))
            if entity_type == "confirmation" and not envelope.get("context_pack_id") and not profile_pattern_lower.strip() in {
                "yes",
                "yes.",
                "correct",
                "correct.",
                "approved",
                "approved.",
            }:
                continue
            if feature_scope_memory_only and entity_type in {"relationship", "location", "job_status", "family_profile"}:
                continue
            if assistant_profile_fact_only_lineage(lineage) and entity_type in {
                "relationship",
                "location",
                "job_status",
                "family_profile",
                "current_plan",
                "confirmation",
                "approval_state",
                "correction",
            }:
                continue
            entity_name = canonical_entity_name(entity_type, value)
            field_patches = infer_entity_field_patches(entity_type, value, profile_pattern_text)
            entities.append(
                {
                    "entity_type": entity_type,
                    "entity_name": entity_name or entity_type,
                    "state": summarize_text(value or profile_pattern_text, limit=220),
                    "confidence": 0.82 if value else 0.66,
                    "source_refs": lineage["source_refs"],
                    "source_roles": lineage["source_roles"],
                    "source_role_counts": lineage["source_role_counts"],
                    "operator": normalize_entity_operator(None, entity_type),
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
                "operator": normalize_entity_operator(None, "session"),
                "field_patches": [],
            }
        )
    return dedupe_entities(entities)


_PATCH_CORRECTION_RE = re.compile(
    r"\b(?:correction|correct|wrong|updated|changed)[:\s]+([^.;!?]+?)\s+(?:instead\s+of|not)\s+([^.;!?]+)",
    flags=re.IGNORECASE,
)
_PATCH_PREFERENCE_RE = re.compile(
    r"\b(?:prefer|prefers|favorite|likes?|loves?)\s+([^.;!?]+?)\s+(?:now|instead\s+of|not)\s+([^.;!?]+)",
    flags=re.IGNORECASE,
)
_PATCH_NEGATIVE_RE = re.compile(
    r"\b(?:never|avoid(?:s)?|do(?:es)?\s+not|don't|doesn't|cannot|can't|should\s+not|must\s+not)\s+([^.;!?]+)",
    flags=re.IGNORECASE,
)


@functools.lru_cache(maxsize=4)
def _whole_text_patch_scans(text: str):
    """The three scans that read the WHOLE document, done once per document.

    `infer_entity_field_patches` is called once per extracted entity, and each call used to run
    these three searches across the entire text. The entity count grows with the document, so the
    work grew as its square: a 256 KB markdown ingest spent ~33 seconds here, against ~1.2 s for a
    JSON document of the same size that produced far fewer entities.

    None of the three depend on the entity -- only on the text. Which patch is *kept* still depends
    on `entity_type`, and that check is unchanged; this only stops recomputing the same three
    answers. The cache is keyed on the text itself and holds four entries, which is enough for one
    ingest to reuse its own document while never pinning more than a handful.
    """
    return (
        _PATCH_CORRECTION_RE.search(text),
        _PATCH_PREFERENCE_RE.search(text),
        _PATCH_NEGATIVE_RE.search(text),
    )


def infer_entity_field_patches(entity_type: str, value: str, text: str) -> list[Json]:
    patches: list[Json] = []
    correction, preference, negative_preference = _whole_text_patch_scans(text)
    if correction:
        replace = clean_patch_value(correction.group(1))
        search = clean_patch_value(correction.group(2))
        patches.append(entity_patch(search, replace))
    if entity_type == "preference" and preference:
        replace = clean_patch_value(preference.group(1))
        search = clean_patch_value(preference.group(2))
        patches.append(entity_patch(search, replace))
    if entity_type == "preference" and negative_preference and not patches:
        patches.append(entity_patch("", "avoid " + clean_patch_value(negative_preference.group(1))))
    evolving_entity_types = {
        "preference",
        "location",
        "job_status",
        "current_plan",
        "family_profile",
        "identity_profile",
        "communication_profile",
        "memory_feature_profile",
        "workspace_profile",
        "relationship",
        "approval_state",
        "correction",
        "confirmation",
        "assistant_decision",
        "tool_evidence",
        *CODEX_OUTCOME_ENTITY_TYPES,
    }
    if entity_type in evolving_entity_types and not patches and value:
        patches.append(entity_patch("", summarize_text(value, limit=180)))
    return patches[:3]


def clean_patch_value(value: str) -> str:
    return summarize_text(" ".join(value.split()).strip(" ,;:-"), limit=180)


def canonical_entity_name(entity_type: str, value: str) -> str:
    compact_value = " ".join(str(value or "").split()).strip(" ,;:-")
    if entity_type in {
        "preference",
        "location",
        "job_status",
        "current_plan",
        "family_profile",
        "identity_profile",
        "communication_profile",
        "memory_feature_profile",
        "workspace_profile",
        "correction",
        "confirmation",
        "assistant_decision",
        "tool_evidence",
        *CODEX_OUTCOME_ENTITY_TYPES,
    }:
        return entity_type
    if entity_type == "approval_state":
        subject = compact_value
        subject_patterns = [
            r"^(?:the\s+)?(.+?)\s+(?:is|was|are|were|has been|have been)\s+(?:approved|required|missing|blocked|ready|done|complete|needed)\b",
            r"^(?:the\s+)?(.+?)\s+as\s+(?:a\s+|an\s+|the\s+)?(?:blocker|requirement|approval|decision|status)\b",
            r"^(?:the\s+)?(.+?)\s+after\b",
            r"^(?:the\s+)?(.+?)\s+before\b",
            r"^(?:the\s+)?(.+?)\s+because\b",
            r"^(?:the\s+)?(.+?)\s+as\b",
        ]
        for pattern in subject_patterns:
            match = re.search(pattern, subject, flags=re.IGNORECASE)
            if match:
                subject = match.group(1)
                break
        subject = re.sub(r"^(?:the|a|an)\s+", "", subject, flags=re.IGNORECASE).strip(" ,;:-")
        return summarize_text(subject or compact_value, limit=80) if (subject or compact_value) else entity_type
    return compact_value[:80] if compact_value else entity_type


def entity_retention_priority(entity: Json) -> int:
    entity_type = str(entity.get("entity_type") or "").strip().lower()
    entity_name = str(entity.get("entity_name") or "").strip().lower()
    source_roles = {
        normalized_extraction_message_role(role)
        for role in entity.get("source_roles", [])
        if normalized_extraction_message_role(role)
    }
    if entity_type in {
        "identity_profile",
        "communication_profile",
        "workspace_profile",
        "memory_feature_profile",
        "preference",
        "approval_state",
        "correction",
    }:
        return 0
    if "user" in source_roles or entity_type in {"current_plan", "confirmation"}:
        return 1
    if entity_type in CODEX_OUTCOME_ENTITY_TYPES:
        return 2
    if entity_type in {"assistant_decision", "tool_evidence"} and ":" in entity_name:
        return 2
    if entity_type in {"assistant_decision", "tool_evidence"}:
        return 3
    return 4


DIRECTIVE_STATE_PREFIXES = ("user directive:", "user plan:", "user profile:")


def directive_free_state(entity: Json) -> str:
    """The entity's state with the directive prefix removed, for comparison only."""
    text = " ".join(str(entity.get("state") or "").lower().split())
    for prefix in DIRECTIVE_STATE_PREFIXES:
        if text.startswith(prefix):
            return text[len(prefix):].strip()
    return text


def drop_directive_duplicates(entities: list[Json]) -> list[Json]:
    """Drop a directive entity when a plain entity already carries the same fact.

    Two independent extractors fire on one clause: the directive scan turns "prefer metric
    units" into state "user directive: prefer metric units" named "preference:prefer metric
    units", and the pattern table turns the same words into state "metric units" named
    "preference". Their names differ, so the name-keyed dedupe above never saw them as one and
    the preference was stored twice -- then twice again once it promoted to the profile node.

    The PLAIN entity is the one kept, never the directive. Its name is the bare entity type,
    which is the stable identity cross-session supersession matches on; the directive's name
    embeds its own text, so it cannot supersede a later correction and a profile would keep
    both revisions forever. Removing the plain one instead costs the verb ("prefer") and
    breaks lineage -- test_profile_preference_correction_supersedes_stale_cross_session_state
    catches exactly that.

    Only directive-prefixed entities are ever removed, and only when a same-type plain entity's
    text is literally contained in theirs, so genuinely distinct entities cannot collapse on a
    substring: project "Aurora" cannot absorb "Aurora v2", neither being a directive.
    """
    plain = [
        (entity.get("entity_type"), directive_free_state(entity))
        for entity in entities
        if not str(entity.get("state") or "").lower().startswith(DIRECTIVE_STATE_PREFIXES)
    ]
    plain = [(kind, text) for kind, text in plain if len(text) >= 4]
    if not plain:
        return entities

    def superseded_by_plain(entity: Json) -> bool:
        if not str(entity.get("state") or "").lower().startswith(DIRECTIVE_STATE_PREFIXES):
            return False
        text = directive_free_state(entity)
        return any(kind == entity.get("entity_type") and other != text and other in text
                   for kind, other in plain)

    return [entity for entity in entities if not superseded_by_plain(entity)]


def dedupe_entities(entities: list[Json]) -> list[Json]:
    seen = set()
    positions: dict[tuple[Any, str], int] = {}
    out = []
    for entity in entities:
        key = (entity.get("entity_type"), str(entity.get("entity_name", "")).lower())
        if key in seen:
            existing = out[positions[key]]
            if entity_retention_priority(entity) < entity_retention_priority(existing):
                out[positions[key]] = entity
                continue
            if entity.get("entity_type") == "tool_evidence" and existing.get("state"):
                continue
            if entity.get("entity_name") == entity.get("entity_type"):
                out[positions[key]] = entity
            continue
        seen.add(key)
        positions[key] = len(out)
        out.append(entity)
    out = drop_directive_duplicates(out)
    ranked = sorted(enumerate(out), key=lambda item: (entity_retention_priority(item[1]), item[0]))
    kept_indexes = {index for index, _entity in ranked[:20]}
    return [entity for index, entity in enumerate(out) if index in kept_indexes]


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


RAW_BYTE_METADATA_FIELDS = {"raw_bytes", "file_bytes", "bytes", "binary", "payload_bytes", "data_url", "base64"}
SERVING_RESOURCE_METADATA_FIELDS = {
    # `resource_type` is not carried: it restates what the record already says it is.
    # candidate_index_terms derives both `resource_type:` and `source_type:` from the
    # record_type -- a skill_section is a skill -- and dropping this copy loses no terms,
    # checked against the derivation. 63.7 KB per 1 MB skill, identical on every row.
    #
    # `unit_kind` and `heading_slug` are NOT droppable with it: the same check shows each is
    # the only source of its own term. `resource_version` produces no term, but the retrieve
    # path compares it against the manifest's latest to decide version_state, and a manifest
    # holding only the latest cannot supply a record's own ingested version.
    "resource_version",
    "unit_kind",
    "relative_path",
    "heading",
    "heading_slug",
    "heading_path",
    # `source_locator` is deliberately absent: every chunk record carries it at the TOP level
    # (both resource_chunk_record and skill_section_record set it unconditionally), and storing
    # it here as well cost 290.8 KB per 1 MB skill -- 3.7% of the ingest -- byte-identical to the
    # copy beside it. Readers of the metadata copy are all fallbacks of the form
    # `record.get("source_locator") or metadata.get("source_locator")`, which cannot fire while
    # the top-level field is present.
    #
    # `heading` stays: skill_section_record sets it at the top level but resource_chunk_record
    # does not, so for a resource chunk this is the only copy.
    # `content_hash` is not carried: it is a hash of text the same record already stores,
    # and every reader recomputes it when absent -- `metadata.get("content_hash") or
    # content_hash(chunk.text)` in ingest and in resource IO, with the remaining uses
    # conditional on its presence. 88.2 KB per 1 MB skill to restate what the row derives
    # from itself. The dashboard and registry read the TOP-level field, not this one.
    "token_estimate",
    "row_start",
    "row_end",
    "record_start",
    "record_end",
    "page",
    "page_section",
    "slide_number",
    "sheet_name",
    "row_count",
    "supersedes_chunk_hash",
}
DEBUG_RESOURCE_METADATA_FIELDS = {
    "embedding_text",
    "parse_warnings",
    "parser_name",
    "parser_version",
    "parse_warning_count",
    "columns",
    "links",
    "tables",
    "front_matter",
}


def sanitize_resource_metadata(metadata: Json) -> Json:
    sanitized = {
        key: value
        for key, value in metadata.items()
        if key not in RAW_BYTE_METADATA_FIELDS
    }
    sanitized["parse_warnings"] = normalize_parse_warnings(sanitized)
    sanitized["raw_storage_policy"] = str(sanitized.get("raw_storage_policy") or "raw_uri_only")
    sanitized["raw_bytes_stored"] = False
    return sanitized


def serving_resource_metadata(metadata: Json) -> Json:
    sanitized = sanitize_resource_metadata(metadata)
    serving = {
        key: sanitized[key]
        for key in SERVING_RESOURCE_METADATA_FIELDS
        if key in sanitized and sanitized[key] not in (None, "", [], {})
    }
    # `raw_storage_policy` is not carried per chunk: it is a document fact, identical on every
    # chunk, and no reader takes it from a stored chunk. The dashboard reads the TOP-level
    # field on manifest rows, ingest reads the live storage_resolution, and resource IO reads
    # the ENVELOPE metadata while deciding where raw bytes go. 93.1 KB per 1 MB skill.
    #
    # `resource_version` stays, though it is the same shape: the retrieve path reads it from a
    # stored record to decide version_state, falling back to a top-level field sections do not
    # carry, so dropping it would make every chunk look current.
    # `raw_bytes_stored` is a per-document fact and a constant False on every chunk of a
    # document, 27 B a row -- 66.2 KB per 1 MB skill. It is not carried here because
    # nothing reads it from a chunk: every mention inside a metadata dict is an
    # ASSIGNMENT, and every read takes the top-level field with a False default, which
    # the manifest record supplies.
    parse_warnings = normalize_parse_warnings(sanitized)
    if parse_warnings:
        serving["parse_warning_count"] = len(parse_warnings)
        serving["has_parse_warnings"] = True
    return serving


def debug_resource_metadata(metadata: Json) -> Json:
    sanitized = sanitize_resource_metadata(metadata)
    debug = {
        key: sanitized[key]
        for key in sorted(DEBUG_RESOURCE_METADATA_FIELDS)
        if key in sanitized and sanitized[key] not in (None, "", [], {})
    }
    parse_warnings = normalize_parse_warnings(sanitized)
    if parse_warnings:
        debug["parse_warnings"] = parse_warnings
        debug["parse_warning_count"] = len(parse_warnings)
    embedding_text = str(sanitized.get("embedding_text") or "")
    if embedding_text:
        debug["embedding_text_hash"] = stable_hash(embedding_text)
        debug["embedding_text_preview"] = summarize_text(embedding_text, limit=320)
    return debug


def source_locator_from_ref(source_ref: str, raw_uri: str) -> str:
    source_ref = str(source_ref or "")
    raw_uri = str(raw_uri or "")
    if not source_ref:
        return ""
    if raw_uri and source_ref == raw_uri:
        return ""
    if raw_uri and source_ref.startswith(raw_uri + "#"):
        return source_ref.partition("#")[2]
    if "#" in source_ref:
        return source_ref.partition("#")[2]
    return source_ref


def source_ref_from_locator(raw_uri: str, source_locator: str) -> str:
    raw_uri = str(raw_uri or "")
    source_locator = str(source_locator or "")
    if not source_locator:
        return raw_uri
    if source_locator.startswith(("file:", "s3://", "http://", "https://", "/")):
        return source_locator
    return f"{raw_uri}#{source_locator}" if raw_uri else source_locator


def registry_access_scope(scope: Json, *, sharing_scope: str = "private_user") -> Json:
    sharing_scope = str(sharing_scope or "private_user").strip().lower()
    access = {
        "account_id": str(scope.get("account_id") or ""),
        "tenant_id": str(scope.get("tenant_id") or ""),
        "team": str(scope.get("team") or ""),
        "user_id": str(scope.get("user_id") or ""),
        "session_id": str(scope.get("session_id") or ""),
        "account_hash": scope.get("account_hash", 0),
        "tenant_hash": scope.get("tenant_hash", 0),
        "user_hash": scope.get("user_hash", 0),
        "session_hash": scope.get("session_hash", 0),
        "scope_key": scope.get("scope_key", ""),
        "sharing_scope": sharing_scope,
    }
    if sharing_scope == "tenant_shared":
        access["user_id"] = ""
        access["session_id"] = ""
        access["user_hash"] = 0
        access["session_hash"] = 0
        tenant_hash = int(access.get("tenant_hash") or 0)
        access["scope_key"] = scope_key_from_hashes(tenant_hash) if tenant_hash else ""
    elif sharing_scope == "global_shared":
        access["tenant_id"] = ""
        access["team"] = ""
        access["user_id"] = ""
        access["session_id"] = ""
        access["tenant_hash"] = 0
        access["user_hash"] = 0
        access["session_hash"] = 0
        access["scope_key"] = "global|shared|"
    return access


# Characters an index term may keep beyond ASCII: CJK ideographs, kana and hangul.
#
# Without them `[^a-z0-9_.:/-]` turned every CJK character into "_", so a pure-Chinese value
# normalized to "" and `context_index_name` returned "" -- no term at all. The resource parser
# indexes Chinese as overlapping character bigrams precisely because Chinese has no spaces to
# split on, and every one of those bigrams was being discarded here.
#
# It cost twice over. The Chinese half of a CN/EN corpus had NO keyword index, and because
# `keywords_for_text` interleaves bigrams with Latin runs so English filler cannot exhaust the
# quota, the discarded bigrams still consumed their slots: measured on a CN/EN corpus, only
# 42.9-51.1% of emitted keywords survived to become terms, so a cap of 12 bought 6 usable terms
# and a cap of 200 bought 86.
# `normalized_index_value` and `context_index_name` are not defined here. They were, and this copy
# was the one that kept CJK and other non-ASCII characters; matrixark_mcp_indexing had a plainer
# copy that replaced everything outside [a-z0-9_.:/-] with "_". Because the result is then stripped
# of "_", and `context_index_name` returns "" for an empty value, a term written only in Chinese,
# Japanese or Korean normalised to the EMPTY STRING through that copy:
#
#     written here                keyword:内存
#     normalised there            ''
#
# Both are live and they sit on opposite sides of the same lookup: matrixark_mcp_core_query_analysis
# and matrixark_mcp_core_codex_outcome reach this module, while matrixark_mcp_query reaches
# matrixark_mcp_indexing. A term indexed under one could not be found by a query normalised through
# the other -- not ranked lower, not found at all. `café` lost its accent the same way.
#
# The character class moved to matrixark_mcp_indexing, which owns index naming and imports nothing
# from here, and this module re-exports it, so there is one normaliser and a change to it cannot
# reach the writer and miss the reader again.
try:
    from tools.matrixark_mcp_indexing import context_index_name, normalized_index_value
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_indexing import context_index_name, normalized_index_value


def profile_memory_class_for_entity_type(entity_type: Any) -> str:
    normalized = normalized_index_value(entity_type)
    if normalized in {"identity_profile"}:
        return "identity"
    if normalized in {"communication_profile"}:
        return "communication"
    if normalized in {"workspace_profile"}:
        return "workspace"
    if normalized in {"memory_feature_profile"}:
        return "memory_feature"
    if normalized in {"preference", "approval_state", "confirmation", "correction"}:
        return "preference"
    if normalized in {"relationship", "family_profile", "job_status", "location"}:
        return "personal_context"
    if normalized in {"current_plan"}:
        return "task_context"
    if is_codex_outcome_entity_type(normalized):
        return "codex_outcome"
    if normalized in {"resource_fact"}:
        return "resource_context"
    return "general"


def profile_memory_kind_for_entity_type(entity_type: Any) -> str:
    normalized = normalized_index_value(entity_type)
    if is_codex_outcome_entity_type(normalized):
        return "codex_outcome"
    if normalized.startswith("resource_") or normalized in {"resource_fact"}:
        return "resource_context"
    if normalized in {"current_plan"}:
        return "task_context"
    if normalized in {"memory_feature_profile"}:
        return "memory_feature"
    return "durable_profile"


def non_default_classification(value: Any) -> str:
    classification = str(value or "").strip().upper()
    return "" if classification in {"", "NEW_EVENT"} else classification


def benchmark_quality_index_terms(*values: Any) -> list[str]:
    try:
        from tools.matrixark_mcp_indexing import benchmark_quality_index_terms as shared_benchmark_quality_index_terms
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_indexing import benchmark_quality_index_terms as shared_benchmark_quality_index_terms
    return shared_benchmark_quality_index_terms(*values)


def metadata_index_terms(metadata: Json, *, keyword_limit: int = MAX_METADATA_KEYWORD_INDEXES_PER_CHUNK) -> list[str]:
    terms: list[str] = []
    for field in ["unit_kind", "heading_slug", "relative_path"]:
        terms.append(context_index_name(field, metadata.get(field)))
    for keyword in metadata.get("keywords", [])[: max(0, keyword_limit)]:
        terms.append(context_index_name("keyword", keyword))
    return ordered_unique(terms)


# Every priority prefix is exactly "<kind>:", and every index term is exactly
# f"{kind}:{value}", so priority is decided by the term's kind alone. Built once here rather
# than rediscovered by a 20-way startswith scan on each of the ~36,000 terms a document emits.
_SECONDARY_INDEX_PRIORITY_BY_KIND = {
    prefix[:-1]: index
    for index, prefix in enumerate(SECONDARY_INDEX_PRIORITY_PREFIXES)
}
_SECONDARY_INDEX_PRIORITY_DEFAULT = len(SECONDARY_INDEX_PRIORITY_PREFIXES)


def secondary_index_priority(term: str) -> int:
    kind, separator, _ = term.partition(":")
    if not separator:
        return _SECONDARY_INDEX_PRIORITY_DEFAULT
    return _SECONDARY_INDEX_PRIORITY_BY_KIND.get(kind, _SECONDARY_INDEX_PRIORITY_DEFAULT)


def limited_index_terms(terms: list[str], *, limit: int) -> list[str]:
    unique_terms = ordered_unique([term for term in terms if term])
    capped_limit = max(0, int(limit))
    return [
        term
        for _, term in sorted(
            enumerate(unique_terms),
            key=lambda item: (secondary_index_priority(item[1]), item[0]),
        )
    ][:capped_limit]


def new_secondary_index_budget(limit: int | None = None) -> Json:
    configured_limit = MAX_SECONDARY_INDEX_RECORDS_PER_OPERATION if limit is None else int(limit)
    return {"limit": max(0, configured_limit), "emitted": 0, "dropped": 0}


def take_secondary_index_terms(terms: list[str], budget: Json) -> list[str]:
    unique_terms = ordered_unique([term for term in terms if term])
    limit = max(0, int(budget.get("limit", 0)))
    emitted = max(0, int(budget.get("emitted", 0)))
    remaining = max(0, limit - emitted)
    selected = unique_terms[:remaining]
    budget["emitted"] = emitted + len(selected)
    budget["dropped"] = max(0, int(budget.get("dropped", 0))) + max(0, len(unique_terms) - len(selected))
    return selected


def secondary_index_budget_summary(budget: Json) -> Json:
    return {
        "index_total_cap": max(0, int(budget.get("limit", 0))),
        "index_emitted_count": max(0, int(budget.get("emitted", 0))),
        "index_dropped_by_total_cap_count": max(0, int(budget.get("dropped", 0))),
    }


def context_index_posting_record(
    *,
    index_name: str,
    data_model: str,
    ref_type: str | None = None,
    ref_hashes: list[Any] | None = None,
    batch_id_hash: Any = None,
    node_hash: Any = None,
    scope: Json | None = None,
    updated_at_ms: Any = None,
    source_ref: str | None = None,
    storage_options: Json | None = None,
) -> Json:
    """Build a compact TemporalStore-style secondary-index posting row.

    ContextIndex is an index data model, not an event/chunk/entity clone. New
    writes store the indexed data model and a timestamp key plus a compact
    posting list of referenced hashes. Readers still accept older ref_hash rows.
    """
    refs: list[Any] = []
    seen_refs: set[str] = set()
    for ref in ref_hashes or []:
        if ref is None:
            continue
        key = str(ref)
        if key in seen_refs:
            continue
        seen_refs.add(key)
        refs.append(ref)
    timestamp_key_ms = int(updated_at_ms or now_ms())
    identity = {
        "index_name": index_name,
        "data_model": data_model,
        "timestamp_key_ms": timestamp_key_ms,
        "batch_id_hash": batch_id_hash,
        "node_hash": node_hash,
        "ref_hashes": refs,
    }
    record: Json = {
        "record_type": "context_index",
        "index_name": index_name,
        "index_hash": stable_hash(json.dumps(identity, sort_keys=True, separators=(",", ":"))),
        "data_model": data_model,
        "timestamp_key_ms": timestamp_key_ms,
        "ref_hashes": refs,
        "updated_at_ms": timestamp_key_ms,
    }
    if len(refs) == 1:
        # Compatibility for native readers that still consume single-ref postings.
        record["ref_hash"] = refs[0]
    if ref_type:
        record["ref_type"] = ref_type
    if batch_id_hash is not None:
        record["batch_id_hash"] = batch_id_hash
    if node_hash is not None:
        record["node_hash"] = node_hash
    if scope:
        record["scope"] = scope
    if source_ref:
        record["source_ref"] = source_ref
    if storage_options:
        record["storage_options"] = storage_options
    return record


def context_index_record_ref_hashes(record: Json) -> list[Any]:
    refs = record.get("ref_hashes")
    if isinstance(refs, list):
        return [ref for ref in refs if ref is not None]
    legacy = record.get("ref_hash")
    return [legacy] if legacy is not None else []


def context_index_record_node_hashes(record: Json) -> list[Any]:
    node_hashes = record.get("node_hashes")
    if isinstance(node_hashes, list) and node_hashes:
        return [node_hash for node_hash in node_hashes if node_hash is not None]
    node_hash = record.get("node_hash")
    return [node_hash] if node_hash is not None else []


def ordered_unique_any(values: list[Any]) -> list[Any]:
    output: list[Any] = []
    seen: set[str] = set()
    for value in values:
        if value is None:
            continue
        key = str(value)
        if key in seen:
            continue
        seen.add(key)
        output.append(value)
    return output


def context_index_time_bucket(timestamp_ms: Any) -> int:
    try:
        timestamp = int(timestamp_ms)
    except (TypeError, ValueError):
        timestamp = now_ms()
    bucket_ms = max(1, int(SECONDARY_INDEX_TIME_BUCKET_MS))
    return (timestamp // bucket_ms) * bucket_ms


def _chunked_refs(refs: list[Any], *, limit: int) -> list[list[Any]]:
    cap = max(1, int(limit))
    return [refs[index:index + cap] for index in range(0, len(refs), cap)] or [[]]


#: This module carries its own copy of the posting fold, and it groups by data_model where the
#: copy in matrixark_mcp_indexing groups by capability -- different keys, different policy string.
#: Compaction resolves THIS one. The skip-when-already-folded helper is shared with the other copy
#: rather than written twice, because two copies of this fold have already drifted once.
_CORE_POSTING_POLICY = "bucketed_by_scope_data_model_index_time"
_ALREADY_FOLDED_POSTINGS = None


def _core_posting_bucket_key(record: Json):
    index_name = str(record.get("index_name") or "")
    data_model = str(record.get("data_model") or "")
    if not index_name or not data_model:
        return None
    return (
        str(record.get("scope_key") or ""),
        data_model,
        index_name,
        str(record.get("ref_type") or ""),
        context_index_time_bucket(record.get("timestamp_key_ms") or record.get("updated_at_ms")),
    )


def _core_already_folded_postings(records: list[Json]):
    """Resolved once: compaction calls this for every read, and an import per call was already
    measured as 82.9% of what keying a record costs."""
    global _ALREADY_FOLDED_POSTINGS
    helper = _ALREADY_FOLDED_POSTINGS
    if helper is None:
        try:
            from tools.matrixark_mcp_indexing import already_folded_postings as helper
        except ImportError:  # Direct script execution from tools/.
            from matrixark_mcp_indexing import already_folded_postings as helper
        _ALREADY_FOLDED_POSTINGS = helper
    return helper(records, _CORE_POSTING_POLICY, _core_posting_bucket_key, ("index_hash",))


def compact_context_index_postings(records: list[Json]) -> list[Json]:
    """Group ContextIndex writes into Feature-style timestamped posting rows.

    ContextIndex is an index data model. It should not produce one row per
    indexed object when many objects share the same term, model, scope, node, and
    timestamp bucket. Grouping keeps index rows bounded by terms and buckets,
    while ref_hashes carries the posting list.
    """
    unchanged = _core_already_folded_postings(records)
    if unchanged is not None:
        return unchanged
    scalar_lineage_fields = [
        "memory_scope",
        "session_continuity",
        "profile_memory_class",
        "profile_memory_kind",
        "profile_entity_current",
        "profile_revision",
        "promoted_from_memory_scope",
        "extraction_phase",
        "final_session_boundary",
    ]
    list_lineage_fields = [
        "source_session_ids",
        "source_entity_hashes",
        "source_memory_scopes",
        "source_session_continuities",
        "source_profile_memory_classes",
        "source_profile_memory_kinds",
    ]
    grouped: dict[tuple[Any, ...], Json] = {}
    grouped_scalar_values: dict[tuple[Any, ...], dict[str, set[str]]] = {}
    grouped_list_values: dict[tuple[Any, ...], dict[str, list[Any]]] = {}
    passthrough: list[Json] = []
    order: list[tuple[Any, ...]] = []
    for record in records:
        if str(record.get("record_type") or "") != "context_index":
            passthrough.append(record)
            continue
        index_name = str(record.get("index_name") or "")
        data_model = str(record.get("data_model") or "")
        if not index_name or not data_model:
            passthrough.append(record)
            continue
        bucket_ms = context_index_time_bucket(record.get("timestamp_key_ms") or record.get("updated_at_ms"))
        key = (
            str(record.get("scope_key") or ""),
            data_model,
            index_name,
            str(record.get("ref_type") or ""),
            bucket_ms,
        )
        if key not in grouped:
            grouped[key] = {
                "record_type": "context_index",
                "index_name": index_name,
                "data_model": data_model,
                "timestamp_key_ms": bucket_ms,
                "updated_at_ms": bucket_ms,
                "ref_hashes": [],
                "node_hashes": [],
                "batch_id_hashes": [],
                "posting_count": 0,
                "posting_policy": "bucketed_by_scope_data_model_index_time",
            }
            for field in ("scope_key", "ref_type", "storage_route"):
                value = record.get(field)
                if value not in (None, "", [], {}):
                    grouped[key][field] = value
            grouped_scalar_values[key] = {field: set() for field in scalar_lineage_fields}
            grouped_list_values[key] = {field: [] for field in list_lineage_fields}
            order.append(key)
        posting = grouped[key]
        for field in scalar_lineage_fields:
            value = record.get(field)
            if value not in (None, "", [], {}):
                grouped_scalar_values[key][field].add(str(value))
        for field in list_lineage_fields:
            values = record.get(field)
            if not isinstance(values, list):
                value = record.get(field)
                values = [value] if value not in (None, "", [], {}) else []
            for value in values:
                if value not in (None, "", [], {}) and str(value) not in {str(item) for item in grouped_list_values[key][field]}:
                    grouped_list_values[key][field].append(value)
        node_hash = record.get("node_hash")
        if node_hash is not None and str(node_hash) not in {str(item) for item in posting.get("node_hashes", [])}:
            posting["node_hashes"].append(node_hash)
        batch_id_hash = record.get("batch_id_hash")
        if batch_id_hash is not None and str(batch_id_hash) not in {str(item) for item in posting.get("batch_id_hashes", [])}:
            posting["batch_id_hashes"].append(batch_id_hash)
        existing = {str(ref) for ref in posting.get("ref_hashes", [])}
        for ref in context_index_record_ref_hashes(record):
            if ref is None or str(ref) in existing:
                continue
            posting["ref_hashes"].append(ref)
            existing.add(str(ref))
        if record.get("source_ref") and not posting.get("sample_source_ref"):
            posting["sample_source_ref"] = record.get("source_ref")
        try:
            posting["posting_count"] += max(1, int(record.get("posting_count") or len(context_index_record_ref_hashes(record)) or 1))
        except (TypeError, ValueError):
            posting["posting_count"] += 1
    compacted_indexes: list[Json] = []
    for key in order:
        base = grouped[key]
        for field, values in grouped_scalar_values.get(key, {}).items():
            if len(values) == 1:
                raw_value = next(iter(values))
                if raw_value == "True":
                    base[field] = True
                elif raw_value == "False":
                    base[field] = False
                elif field == "profile_revision":
                    try:
                        base[field] = int(raw_value)
                    except (TypeError, ValueError):
                        base[field] = raw_value
                else:
                    base[field] = raw_value
        for field, values in grouped_list_values.get(key, {}).items():
            if values:
                base[field] = values
        refs = []
        seen_ref_keys: set[str] = set()
        for ref in base.get("ref_hashes", []):
            ref_key = str(ref)
            if ref_key in seen_ref_keys:
                continue
            seen_ref_keys.add(ref_key)
            refs.append(ref)
        for part, ref_chunk in enumerate(_chunked_refs(refs, limit=MAX_SECONDARY_INDEX_REFS_PER_POSTING)):
            record = dict(base)
            record["ref_hashes"] = ref_chunk
            record["posting_part"] = part
            # `ref_hashes` is the one place a posting names what it points at. The singular
            # `ref_hash` restated it on every single-ref row, and `chunk_hash` restated it again;
            # the serving accessor reads neither when the list is present, and `index_hash` is
            # derived from the list, so both are dropped rather than written three ways. Older
            # rows still resolve -- the fallbacks that read them are unchanged.
            record.pop("ref_hash", None)
            record.pop("chunk_hash", None)
            if len(record.get("node_hashes", [])) == 1:
                record["node_hash"] = record["node_hashes"][0]
            else:
                record.pop("node_hash", None)
            if len(record.get("batch_id_hashes", [])) == 1:
                record["batch_id_hash"] = record["batch_id_hashes"][0]
            else:
                record.pop("batch_id_hash", None)
            if not record.get("node_hashes"):
                record.pop("node_hashes", None)
            if not record.get("batch_id_hashes"):
                record.pop("batch_id_hashes", None)
            identity = {
                "scope_key": record.get("scope_key"),
                "index_name": record.get("index_name"),
                "data_model": record.get("data_model"),
                "timestamp_key_ms": record.get("timestamp_key_ms"),
                "node_hashes": record.get("node_hashes") or ([record.get("node_hash")] if record.get("node_hash") is not None else []),
                "batch_id_hashes": record.get("batch_id_hashes") or ([record.get("batch_id_hash")] if record.get("batch_id_hash") is not None else []),
                "ref_type": record.get("ref_type"),
                "posting_part": part,
                "ref_hashes": ref_chunk,
            }
            record["index_hash"] = stable_hash(json.dumps(identity, sort_keys=True, separators=(",", ":")))
            compacted_indexes.append(record)
    return passthrough + compacted_indexes


RESOURCE_FACT_KEYWORDS = re.compile(
    r"\b(decision|decided|owner|owns|deadline|due|cost|budget|approval|approved|risk|policy|must|should|required|requires|api|endpoint|contract|runbook|rollback|incident|troubleshoot|alert|sla|p95|p99|procedure|checklist)\b",
    flags=re.IGNORECASE,
)

RESOURCE_FACT_SCHEMAS: list[Json] = [
    {
        "fact_type": "resource_decision",
        "entity_type": "resource_decision",
        "entity_prefix": "decision",
        "keywords": ["decision", "decided", "approved", "rejected", "selected"],
    },
    {
        "fact_type": "resource_owner",
        "entity_type": "resource_owner",
        "entity_prefix": "owner",
        "keywords": ["owner", "owns", "reviewer", "assignee", "responsible"],
    },
    {
        "fact_type": "resource_cost",
        "entity_type": "resource_cost",
        "entity_prefix": "cost",
        "keywords": ["cost", "budget", "amount", "price", "spend", "$"],
    },
    {
        "fact_type": "resource_deadline",
        "entity_type": "resource_deadline",
        "entity_prefix": "deadline",
        "keywords": ["deadline", "due", "by monday", "by tuesday", "by wednesday", "by thursday", "by friday", "by saturday", "by sunday"],
    },
    {
        "fact_type": "resource_api_contract",
        "entity_type": "resource_api_contract",
        "entity_prefix": "api",
        "keywords": ["api", "endpoint", "contract", "schema", "request", "response", "http", "grpc"],
    },
    {
        "fact_type": "resource_troubleshooting_step",
        "entity_type": "resource_troubleshooting",
        "entity_prefix": "troubleshooting",
        "keywords": ["troubleshoot", "debug", "incident", "alert", "rollback", "runbook", "remediation", "mitigation"],
    },
    {
        "fact_type": "resource_policy",
        "entity_type": "resource_policy",
        "entity_prefix": "policy",
        "keywords": ["policy", "must", "should", "required", "requires", "cannot", "allowed"],
    },
    {
        "fact_type": "resource_approval",
        "entity_type": "resource_approval",
        "entity_prefix": "approval",
        "keywords": ["approval", "approved", "approve", "signoff", "confirmed"],
    },
    {
        "fact_type": "resource_risk",
        "entity_type": "resource_risk",
        "entity_prefix": "risk",
        "keywords": ["risk", "blocker", "blocked", "failure", "unsafe", "degraded"],
    },
    {
        "fact_type": "resource_procedure",
        "entity_type": "resource_procedure",
        "entity_prefix": "procedure",
        "keywords": ["procedure", "step", "checklist", "first", "then", "verify", "confirm"],
    },
]


def should_extract_resource_fact(text: str, metadata: Json) -> bool:
    if RESOURCE_FACT_KEYWORDS.search(text):
        return True
    unit_kind = str(metadata.get("unit_kind", ""))
    return unit_kind in {"table_row", "table_row_group", "xlsx_row", "xlsx_row_group", "json_document", "json_record", "json_record_group"}


def matched_resource_fact_schemas(text: str, metadata: Json) -> list[Json]:
    lower = text.lower()
    matches = [
        schema
        for schema in RESOURCE_FACT_SCHEMAS
        if any(keyword in lower for keyword in schema["keywords"])
    ]
    if matches:
        return matches[: max(0, MAX_RESOURCE_FACTS_PER_CHUNK)]
    if ENABLE_GENERIC_RESOURCE_FACTS and should_extract_resource_fact(text, metadata):
        return [{"fact_type": "resource_fact", "entity_type": "resource_fact", "entity_prefix": "fact", "keywords": []}]
    return []


def extract_resource_fact_value(text: str, fact_type: str) -> str:
    patterns = {
        "resource_owner": r"\b(?:owner|owns|reviewer|assignee|responsible)\s*(?:is|:|=)?\s*([^.;\n]{2,120})",
        "resource_deadline": r"\b(?:deadline|due)\s*(?:is|:|=|by)?\s*([^.;\n]{2,120})",
        "resource_cost": r"\b(?:cost|budget|amount|price|spend)\s*(?:is|:|=)?\s*([^.;\n]{2,120})",
        "resource_api_contract": r"\b(?:api|endpoint|contract|schema)\s*(?:is|:|=)?\s*([^.;\n]{2,160})",
        "resource_approval": r"\b(?:approval|approved|approve|signoff|confirmed)\s*(?:is|:|=)?\s*([^.;\n]{0,140})",
        "resource_risk": r"\b(?:risk|blocker|blocked|failure)\s*(?:is|:|=)?\s*([^.;\n]{2,160})",
        "resource_decision": r"\b(?:decision|decided)\s*(?:is|:|=)?\s*([^.;\n]{2,180})",
        "resource_policy": r"\b(?:policy|must|should|required|requires)\s*(?:is|:|=)?\s*([^.;\n]{2,180})",
        "resource_troubleshooting_step": r"\b(?:troubleshoot|debug|incident|alert|rollback|runbook|remediation|mitigation)\s*(?:is|:|=)?\s*([^.;\n]{2,180})",
        "resource_procedure": r"\b(?:procedure|step|checklist|verify|confirm)\s*(?:is|:|=)?\s*([^.;\n]{2,180})",
    }
    pattern = patterns.get(fact_type, "")
    if pattern:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return summarize_text(match.group(1).strip(" :-"), limit=180)
    return summarize_text(text, limit=220)


def resource_fact_entity_name(schema: Json, value: str, chunk_metadata: Json, raw_uri: str) -> str:
    prefix = str(schema.get("entity_prefix") or schema.get("entity_type") or "fact")
    semantic_value = summarize_text(str(value or "").strip(), limit=80).strip()
    if semantic_value:
        return f"{prefix}:{semantic_value}"
    heading = str(chunk_metadata.get("heading") or chunk_metadata.get("heading_slug") or "").strip()
    if heading:
        return f"{prefix}:{summarize_text(heading, limit=80)}"
    return prefix


def resource_extraction_mode(envelope: Json) -> str:
    provider = understanding_provider(envelope)
    if provider == "oss_encoder":
        return "matrixark_resource_schema_oss_encoder"
    if provider in {"openai", "openai_compatible", "openai-compatible"}:
        return "matrixark_resource_schema_openai_compatible"
    return "matrixark_resource_schema"


def extract_resource_facts(chunk: Any, *, chunk_metadata: Json, envelope: Json, raw_uri: str, resource_version: str) -> list[Json]:
    """Extract cited resource facts through the same provider-shaped contract as messages.

    The local implementation is deterministic for CI. OSS/OpenAI-compatible
    providers should emit the same fields so storage, indexes, and replay stay
    unchanged.
    """
    mode = resource_extraction_mode(envelope)
    provider = understanding_provider(envelope)
    if provider in {"openai", "openai_compatible", "openai_compatible_llm"}:
        try:
            model_facts = openai_compatible_resource_facts(
                chunk,
                chunk_metadata=chunk_metadata,
                envelope=envelope,
                raw_uri=raw_uri,
                resource_version=resource_version,
            )
            if model_facts or require_oss_understanding():
                return model_facts
        except MatrixArkError:
            if require_oss_understanding():
                raise
    facts: list[Json] = []
    for fact_schema in matched_resource_fact_schemas(chunk.text, chunk.metadata):
        fact_event_type = str(fact_schema["fact_type"])
        fact_entity_type = str(fact_schema["entity_type"])
        fact_value = extract_resource_fact_value(chunk.text, fact_event_type)
        facts.append(
            {
                "mode": mode,
                "classification": "RESOURCE_FACT",
                "event_type": fact_event_type,
                "entity_type": fact_entity_type,
                "status": "observed",
                "value": fact_value,
                "entity_name": resource_fact_entity_name(fact_schema, fact_value, chunk_metadata, raw_uri),
                "confidence": 0.82 if fact_event_type != "resource_fact" else 0.68,
                "source_chunk_hash": chunk.chunk_hash,
                "source_ref": chunk.source_ref,
                "resource_version": resource_version,
                "extraction_provider": understanding_provider(envelope),
            }
        )
    return facts


def resolve_ingest_messages(args: Json, kind: str) -> list[Json]:
    """Resolve the messages list for an ingest envelope.

    Standard kinds (message/feedback/business_data) always require a non-empty
    ``messages`` list, exactly as before. For ``resource``/``skill`` ingest we
    additionally accept a single-call form with EITHER inline text
    (``text`` or ``resource_text``) OR a local-file/URI ``raw_uri``, synthesizing
    a messages list so callers no longer need to wrap a file path or a block of
    text in a dummy ``messages`` list. The synthesized text reaches the resource
    runtime as ``resource_text`` (derived from ``messages``); for a real file/URI
    source the placeholder is ignored because the resolver parses the file.
    """
    messages = args.get("messages")
    if isinstance(messages, list) and messages:
        return require_messages(args)
    if kind in {"resource", "skill"}:
        for field in ("text", "resource_text"):
            value = args.get(field)
            if isinstance(value, str) and value:
                return [{"role": "user", "content": value}]
        raw_uri = args.get("raw_uri")
        if isinstance(raw_uri, str) and raw_uri and raw_uri != "inline-resource":
            return [{"role": "user", "content": f"resource:{raw_uri}"}]
        raise MatrixArkError("resource/skill ingest needs one of: messages, text, or raw_uri")
    return require_messages(args)


# --------------------------------------------------------------------------------------------- #
# PurchaseMemory primitives: per-record TTL / retention-cutoff + keyed-upsert truth-rank.
# These are all OPTIONAL ingest fields; an envelope without them is byte-identical to before.
# --------------------------------------------------------------------------------------------- #
DEFAULT_TRUTH_RANKS: dict[str, int] = {"asserted": 3, "reported": 2, "inferred": 1}


def resolve_truth_rank(truth_class: Any) -> int:
    """Map a ``truth_class`` string onto an integer rank. Unknown / empty -> 0.

    Default table: asserted=3, reported=2, inferred=1. Overridable at runtime via the
    ``MATRIXARK_TRUTH_RANK`` env var (a JSON object of ``{class: rank}``); malformed JSON is
    ignored so a bad override never breaks ingest."""
    if truth_class in (None, ""):
        return 0
    table = dict(DEFAULT_TRUTH_RANKS)
    override = os.environ.get("MATRIXARK_TRUTH_RANK")
    if override:
        try:
            parsed = json.loads(override)
            if isinstance(parsed, dict):
                for key, value in parsed.items():
                    try:
                        table[str(key).strip().lower()] = int(value)
                    except (TypeError, ValueError):
                        continue
        except (ValueError, TypeError):
            pass
    return int(table.get(str(truth_class).strip().lower(), 0))


def _coerce_epoch_seconds(value: Any) -> float | None:
    """Best-effort coercion of an absolute unix-seconds timestamp (int/float/numeric string).

    Returns ``None`` when the value is absent or not numeric (so an unparseable field is simply
    ignored rather than breaking ingest)."""
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def apply_memory_envelope_fields(args: Json, envelope: Json) -> Json:
    """Normalize the optional PurchaseMemory ingest fields onto ``envelope`` (in place).

    Accepts (all optional):
      * ``expires_at`` -- absolute unix seconds (int/float). Wins over ``ttl_seconds``.
      * ``ttl_seconds`` -- relative seconds; expires_at = occurred_at (ingestion_time) + ttl.
      * ``retention_cutoff_ts`` -- scope-level absolute unix seconds; records whose occurrence
        time < cutoff are hidden/purged.
      * ``identity_key`` -- logical identity of a fact (keyed-upsert).
      * ``truth_class`` -- confidence class mapped to an integer ``truth_rank``.

    Stamps ``expires_at`` / ``expires_at_ms`` / ``ephemeral`` when a TTL applies, plus
    ``retention_cutoff_ts`` / ``retention_cutoff_ms`` / ``identity_key`` / ``truth_class`` /
    ``truth_rank`` when supplied. ``ingestion_time_ms`` must already be set on ``envelope``."""
    ingestion_time_ms = int(envelope.get("ingestion_time_ms") or 0)
    expires_at = _coerce_epoch_seconds(args.get("expires_at"))
    ttl_seconds = None
    raw_ttl = args.get("ttl_seconds")
    if raw_ttl not in (None, ""):
        try:
            ttl_seconds = float(raw_ttl)
        except (TypeError, ValueError):
            ttl_seconds = None
    # expires_at wins over ttl_seconds when both are present.
    if expires_at is None and ttl_seconds is not None and ingestion_time_ms > 0:
        expires_at = ingestion_time_ms / 1000.0 + ttl_seconds
    if expires_at is not None:
        envelope["expires_at"] = float(expires_at)
        envelope["expires_at_ms"] = int(round(expires_at * 1000.0))
        envelope["ephemeral"] = True
    if ttl_seconds is not None:
        envelope["ttl_seconds"] = float(ttl_seconds)
    cutoff = _coerce_epoch_seconds(args.get("retention_cutoff_ts"))
    if cutoff is not None:
        envelope["retention_cutoff_ts"] = float(cutoff)
        envelope["retention_cutoff_ms"] = int(round(cutoff * 1000.0))
    identity_key = args.get("identity_key")
    if isinstance(identity_key, str) and identity_key.strip():
        envelope["identity_key"] = identity_key.strip()
    truth_class = args.get("truth_class")
    if isinstance(truth_class, str) and truth_class.strip():
        envelope["truth_class"] = truth_class.strip()
        envelope["truth_rank"] = resolve_truth_rank(truth_class)
    elif "identity_key" in envelope:
        # A keyed write with no explicit class defaults to rank 0 (unknown).
        envelope["truth_rank"] = int(envelope.get("truth_rank") or 0)
    return envelope


def normalize_envelope(args: Json, *, default_kind: str) -> Json:
    kind = args.get("kind", default_kind)
    if kind not in {"message", "feedback", "resource", "skill", "business_data"}:
        raise MatrixArkError("kind is invalid")
    messages = resolve_ingest_messages(args, kind)
    scope = optional_object(args, "scope")
    # Fold mem0-style top-level identity kwargs (user_id / agent_id / run_id)
    # into the canonical scope BEFORE anything reads it. Canonical scope fields
    # win over aliases; byte-identical for callers that pass a full scope.
    scope = fold_mem0_scope_aliases(args, scope)
    metadata = optional_object(args, "metadata")
    ingestion_time_ms = args.get("ingestion_time_ms", metadata.get("ingestion_time_ms"))
    if ingestion_time_ms in (None, ""):
        ingestion_time_ms = now_ms()
    try:
        ingestion_time_ms = int(ingestion_time_ms)
    except (TypeError, ValueError) as exc:
        raise MatrixArkError("ingestion_time_ms must be an integer timestamp in milliseconds") from exc
    if ingestion_time_ms <= 0:
        raise MatrixArkError("ingestion_time_ms must be positive")
    envelope: Json = {
        "kind": kind,
        "messages": messages,
        "scope": scope,
        "metadata": metadata,
        "ingestion_time_ms": ingestion_time_ms,
        "storage_options": normalize_storage_options(args, metadata),
    }
    envelope["storage_route"] = canonical_storage_route(envelope.get("storage_options", {}))
    for field in [
        "context_pack_id",
        "query_id_hash",
        "accepted_refs",
        "rejected_refs",
        "raw_uri",
        "resource_type",
        "source_event_ids",
        "segment_provider",
        "segment_model",
        "segment_model_path",
        "segment_max_new_tokens",
        "segment_provider_fallback",
        "understanding_provider",
        "extraction_provider",
    ]:
        if field in args:
            envelope[field] = args[field]
    apply_memory_envelope_fields(args, envelope)
    return envelope


STORAGE_ROUTE_PRESETS: dict[str, Json] = {
    "shared_store_async": {
        "storage_family": "shared_store",
        "write_mode": "async",
        "storage_mode": "shared_store",
        "replication_mode": "shared_store",
        "oplog_mode": "async",
        "raft_mode": False,
    },
    "shared_store_sync": {
        "storage_family": "shared_store",
        "write_mode": "sync",
        "storage_mode": "shared_store",
        "replication_mode": "shared_store",
        "oplog_mode": "sync",
        "raft_mode": False,
    },
    "raft_async": {
        "storage_family": "raft",
        "write_mode": "async",
        "storage_mode": "raft",
        "replication_mode": "raft",
        "oplog_mode": "async",
        "raft_mode": True,
    },
    "raft_sync": {
        "storage_family": "raft",
        "write_mode": "sync",
        "storage_mode": "raft",
        "replication_mode": "raft",
        "oplog_mode": "sync",
        "raft_mode": True,
    },
}


def entity_ref_text(record: Json) -> str:
    """The one line an entity ref shows in a context pack.

    Rendered as ``f"{entity_type}: {entity_name} = {state}"`` at five call sites, which spelled the
    type twice whenever the name already carried it -- and it usually does. Measured over 448 entity
    refs in the live log: 195 had ``entity_name == entity_type`` ("assistant_decision:
    assistant_decision = ...") and another 186 had the name prefixed with the type
    ("codex_validation: codex_validation:ran_159_tests = ..."). 381 of 448.

    Dropping the repeat is 8.8% off the rendered length of those refs, about 1,827 tokens across
    that sample -- and every one of them is a token the model pays for on the turn the pack is
    injected into.

    A name unrelated to the type still shows both: that pairing is information.
    """
    entity_type = str(record.get("entity_type", "") or "")
    entity_name = str(record.get("entity_name", "") or "")
    state = str(record.get("state", "") or "")
    if entity_name and entity_type:
        if entity_name == entity_type or entity_name.startswith(entity_type + ":"):
            return f"{entity_name} = {state}"
    return f"{entity_type}: {entity_name} = {state}"


def canonical_storage_route(storage_options: Json | None) -> Json:
    options = storage_options if isinstance(storage_options, dict) else {}
    storage_mode = str(options.get("storage_mode") or "default")
    replication_mode = str(options.get("replication_mode") or "default")
    oplog_mode = str(options.get("oplog_mode") or "default")
    raft_mode = bool(options.get("raft_mode", False))
    requested_family = str(options.get("storage_family") or options.get("family") or "default")
    # `durability` is the shorthand for `write_mode`, and this resolver used to ignore it while
    # matrixark_mcp_storage_options.canonical_storage_route honoured it. Both are live -- this one
    # serves the request path (matrixark_mcp_requests, matrixark_mcp_retrieve_request,
    # matrixark_mcp_session_runtime all import it from here), the other serves
    # matrixark_mcp_serving_records and matrixark_mcp_temporal_append -- so `durability: "sync"`
    # was routed sync through one and async through the other, with the opposite write_ack_policy
    # and durability_result. Nothing sets it today, which is why nothing had failed; the schema has
    # advertised it since the storage options were completed, so a caller may start any time.
    requested_durability = str(options.get("durability") or "default")
    requested_write_mode = str(options.get("write_mode") or "default")
    if requested_write_mode == "default" and requested_durability in {"async", "sync"}:
        requested_write_mode = requested_durability
    write_mode = requested_write_mode if requested_write_mode in {"async", "sync"} else oplog_mode
    if write_mode not in {"async", "sync"}:
        write_mode = "async"
    if requested_family == "raft" or storage_mode == "raft" or replication_mode == "raft" or raft_mode:
        route = f"raft_{write_mode}"
        backend_family = "raft"
        storage_mode = "raft" if storage_mode == "default" else storage_mode
        replication_mode = "raft" if replication_mode == "default" else replication_mode
        raft_mode = True
    elif requested_family == "shared_store" or storage_mode == "shared_store" or replication_mode == "shared_store":
        route = f"shared_store_{write_mode}"
        backend_family = "shared_store"
        storage_mode = "shared_store" if storage_mode == "default" else storage_mode
        replication_mode = "shared_store" if replication_mode == "default" else replication_mode
    else:
        route = f"{storage_mode}_{write_mode}" if storage_mode != "default" else "default"
        backend_family = storage_mode
    oplog_mode = write_mode if oplog_mode == "default" else oplog_mode
    background_write = bool(options.get("background_write", write_mode == "async"))
    return {
        "route": route,
        "route_key": route,
        "backend_family": backend_family,
        "storage_family": backend_family,
        "write_mode": write_mode,
        "storage_mode": storage_mode,
        "replication_mode": replication_mode,
        "oplog_mode": oplog_mode,
        "raft_mode": raft_mode,
        "consistency": str(options.get("consistency") or "default"),
        "sync_write": write_mode == "sync",
        "async_write": write_mode == "async",
        "background_write": background_write,
        "write_ack_policy": "ack_after_durable_commit" if write_mode == "sync" else "ack_after_memory_append",
        "native_backend_decides_route": True,
        "selected_storage_family": backend_family,
        "selected_write_mode": write_mode,
        "durability_result": "durable_before_ack" if write_mode == "sync" else "accepted_for_async_durability",
    }


def normalize_storage_options(args: Json, metadata: Json | None = None) -> Json:
    metadata = metadata if isinstance(metadata, dict) else optional_object(args, "metadata")
    raw_options = args.get("storage_options")
    options = dict(raw_options) if isinstance(raw_options, dict) else {}
    metadata_options = metadata.get("storage_options") if isinstance(metadata, dict) else None
    if isinstance(metadata_options, dict):
        options = {**metadata_options, **options}
    aliases = {
        "temporalstore_storage_mode": "storage_mode",
        "temporalstore_oplog_mode": "oplog_mode",
        "temporalstore_replication_mode": "replication_mode",
        "temporalstore_raft_mode": "raft_mode",
        "temporalstore_consistency": "consistency",
        "temporalstore_route": "route",
        "temporalstore_storage_family": "storage_family",
        "temporalstore_write_mode": "write_mode",
        "temporalstore_background_write": "background_write",
    }
    for source, target in aliases.items():
        if source in args:
            options[target] = args[source]
        if isinstance(metadata, dict) and source in metadata:
            options.setdefault(target, metadata[source])
    if not options:
        return {}

    allowed = {
        "storage_mode": {"default", "local", "single_node", "multi_node", "shared_store", "raft"},
        "oplog_mode": {"default", "async", "sync"},
        "replication_mode": {"default", "none", "shared_store", "raft"},
        "consistency": {"default", "eventual", "read_your_writes", "linearizable"},
        "route": set(STORAGE_ROUTE_PRESETS),
        "storage_family": {"default", "shared_store", "raft"},
        "family": {"default", "shared_store", "raft"},
        "write_mode": {"default", "async", "sync"},
    }
    route_value = options.get("route")
    if route_value is not None:
        if not isinstance(route_value, str):
            raise MatrixArkError("storage_options.route must be a string")
        route_key = route_value.strip().lower().replace("-", "_")
        if route_key not in STORAGE_ROUTE_PRESETS:
            raise MatrixArkError(f"storage_options.route must be one of {sorted(STORAGE_ROUTE_PRESETS)}")
        options = {**STORAGE_ROUTE_PRESETS[route_key], **options, "route": route_key}

    normalized: Json = {}
    for key, value in options.items():
        if key in {"raft_mode", "background_write"}:
            if not isinstance(value, bool):
                raise MatrixArkError(f"storage_options.{key} must be a boolean")
            normalized[key] = value
            continue
        if key not in allowed:
            normalized[key] = value
            continue
        if not isinstance(value, str):
            raise MatrixArkError(f"storage_options.{key} must be a string")
        compact = value.strip().lower().replace("-", "_")
        if compact not in allowed[key]:
            raise MatrixArkError(f"storage_options.{key} must be one of {sorted(allowed[key])}")
        normalized[key] = compact
    storage_family = normalized.get("storage_family") or normalized.get("family")
    explicit_modes = {
        str(value)
        for value in (normalized.get("storage_mode"), normalized.get("replication_mode"), storage_family)
        if value in {"shared_store", "raft"}
    }
    if len(explicit_modes) > 1:
        raise MatrixArkError("storage_options must route to exactly one storage_family; do not mix raft and shared_store in one request")
    if storage_family == "raft":
        normalized.setdefault("replication_mode", "raft")
        normalized.setdefault("storage_mode", "raft")
        normalized["raft_mode"] = True
    elif storage_family == "shared_store":
        normalized.setdefault("replication_mode", "shared_store")
        normalized.setdefault("storage_mode", "shared_store")
        normalized["raft_mode"] = False
    if normalized.get("write_mode") in {"async", "sync"}:
        normalized["oplog_mode"] = normalized["write_mode"]
    if normalized.get("oplog_mode") == "sync" and normalized.get("background_write") is True:
        raise MatrixArkError("storage_options.background_write cannot be true when write_mode/oplog_mode is sync")
    if normalized.get("raft_mode") is True:
        normalized.setdefault("replication_mode", "raft")
        normalized.setdefault("storage_mode", "raft")
    route = canonical_storage_route(normalized)
    normalized.update(
        {
            key: value
            for key, value in route.items()
            if key
            in {
                "route",
                "route_key",
                "backend_family",
                "storage_family",
                "write_mode",
                "sync_write",
                "async_write",
                "background_write",
                "write_ack_policy",
                "native_backend_decides_route",
                "selected_storage_family",
                "selected_write_mode",
                "durability_result",
            }
        }
    )
    normalized["request_level"] = True
    return normalized


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


# API embedding providers (OpenAI / Voyage / any OpenAI-compatible endpoint) are defined in the
# embeddings module; import them so this canonical core embedding path also honors API keys in
# production. Acyclic: matrixark_mcp_embeddings imports only leaf helpers (errors, scoring), never core.
try:  # package path
    from tools.matrixark_mcp_embeddings import (
        _API_EMBEDDING_PROVIDERS,
        api_embedding_for_text,
        api_embedding_for_texts,
    )
except ImportError:  # top-level path (direct tools/ execution)
    from matrixark_mcp_embeddings import (
        _API_EMBEDDING_PROVIDERS,
        api_embedding_for_text,
        api_embedding_for_texts,
    )


# These delegate rather than duplicate. This module used to carry its own copies of
# embedding_for_text and embeddings_for_texts, with their own _EMBEDDING_VECTOR_CACHE, and because
# the serving modules pull this module in with `import *`, the copies here were the ones that
# actually ran. Improvements made to matrixark_mcp_embeddings therefore did nothing in production:
# the LRU eviction that replaced a clear-the-whole-cache policy landed in the copy nobody called,
# and adding the query/passage role there produced a TypeError at the call site, which is how the
# duplication was found at all.
#
# One implementation, imported here, so a fix applies once.
try:  # package path
    from tools.matrixark_mcp_embeddings import (  # noqa: F401
        embedding_for_text,
        embeddings_for_texts,
    )
except ImportError:  # top-level path (direct tools/ execution)
    from matrixark_mcp_embeddings import (  # noqa: F401
        embedding_for_text,
        embeddings_for_texts,
    )

def _env_int(name: str, default: int) -> int:
    """Read an integer env var, treating empty or unparseable as unset.

    These are read at import time, so a bare `FOO=` in a shell profile or a compose
    file would otherwise raise ValueError and take the whole module down rather than
    fall back to the default.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except (ValueError, AttributeError):
        return default


try:  # optional: the ingest path must still run where numpy is absent
    import numpy as _NUMPY
except ImportError:  # pragma: no cover - exercised only on installs without numpy
    _NUMPY = None

EMBEDDING_VECTOR_DECIMALS = _env_int("MATRIXARK_EMBEDDING_VECTOR_DECIMALS", 6)
# Default 10000. A uniform scale multiplies every stored vector by the same constant, so
# no pair of vectors can change places -- and with cosine() normalising both sides the
# score itself is unchanged too. Measured over 500 real chunks, six EN/CN queries,
# e5-large at 512 dims, against the float ranking: top-1 6/6, exact top-10 order 6/6,
# overlap 10.0/10, under BOTH the normalised scorer and the bare dot it replaced.
# 10000 is the smallest factor that is still EXACT here: it resolves every margin that
# separates near-neighbours, while 100000 carries an extra digit per element that never
# changes an ordering. Measured bytes per 512-dim vector: float 5,328, scale=1e5 3,247,
# scale=1e4 2,736, int8 2,110 -- and of those only 1e4 and 1e5 reproduce the float
# top-10 exactly (6/6); int8 manages 1/6. Set to 0 to store rounded floats.
EMBEDDING_VECTOR_SCALE = _env_int("MATRIXARK_EMBEDDING_VECTOR_SCALE", 10000)
# Default OFF, and it stays off for retrieval -- measured, not assumed.
#
# Over 500 real chunks with six EN/CN queries (e5-large @512), scored against the FLOAT
# ranking, int8 gets top-1 right 4/6 and reproduces the exact top-10 order 0/6, overlap
# 9.5/10. Through the bare dot product this path used to use it was far worse -- 0/6 and
# 0.5/10 -- so normalising the scorer took int8 from unusable to merely wrong, and did
# not make it correct. A reconstruction cosine near 0.9999 does not contradict this: the
# margins between competing near-neighbours are smaller than that error.
#
# The reason is structural. int8 divides each vector by its OWN peak, so two stored
# vectors are scaled by different factors and can change places against one query.
# EMBEDDING_VECTOR_SCALE is uniform and therefore exact, which is why it is the default
# and this is not.
#
# int8 remains legitimate where ranking does not matter -- bulk archival, or a coarse
# prefilter re-scored at full precision.
EMBEDDING_VECTOR_INT8 = os.environ.get("MATRIXARK_EMBEDDING_VECTOR_INT8", "0") not in {"0", "false", "False", ""}


def _int8_scale(dims: int) -> float:
    """The int8 factor for a unit vector of this width -- the SAME for every vector.

    A per-vector factor (dividing by that vector's own peak) is what made int8 reorder: two
    stored vectors scaled differently can swap places against one query, which no amount of
    precision repairs. This depends only on the width, so it is uniform across vectors and
    still computable one vector at a time, which a corpus-derived factor is not.

    Unit vectors in d dimensions have elements around 1/sqrt(d); 8/sqrt(d) covers that
    distribution and its tail, measured at 0.000% clipping on real e5-large vectors.
    """
    if dims <= 0:
        return 127.0
    return 127.0 * math.sqrt(dims) / 8.0


# base64 of int16 little-endian, instead of JSON decimal digits. A 512-dim vector at
# scale=1e4 is 2,745 bytes as JSON text and 1,368 as base64 int16 -- the same integers, half
# the bytes, because JSON spends a character per digit on numbers no human reads.
#
# int16 fits by construction: at scale=1e4 the largest value a UNIT vector can produce is
# 10,000 (all mass on one axis), against a signed range of 32,767. Measured over 500 real
# vectors the range was -1,649..3,036.
#
# Default ON. Measured on a 1 MB skill: vectors 2,727 -> 1,374 bytes, the whole ingest
# 12.6 MB -> 9.2 MB, amplification 11.8x -> 8.6x, 12.6 GB -> 9.2 GB per thousand documents.
# The footprint stops being vector-dominated: 59.5% -> 44.6%.
#
# This was gated off until every reader went through decode_stored_vector, because the failure
# mode is silent rather than loud: a reader that expects a list and receives a string does not
# raise, it reads the vector as absent, and the node simply stops being scored. Twenty-four
# extraction sites and five isinstance(..., list) checks -- one on the context-node path, one
# skipping context_node embeddings outright -- had to be fixed first.
#
# Set MATRIXARK_EMBEDDING_VECTOR_BASE64=0 to write lists again. Reads accept both forms either
# way, so a store written under one setting serves under the other.
EMBEDDING_VECTOR_BASE64 = os.environ.get(
    "MATRIXARK_EMBEDDING_VECTOR_BASE64", "1"
).strip().lower() not in {"0", "false", "no", "off", ""}

_VECTOR_BASE64_PREFIX = "i16:"
_VECTOR_INT8_PREFIX = "i8:"


def encode_stored_vector(values: list) -> Any:
    """The stored form of an already-compacted vector: a list, or a tagged base64 string.

    Width follows the values. int8 vectors fit in a byte, and packing them as int16 would waste
    half of every element -- the encoding and the container have to agree or the smaller encoding
    buys nothing. The tag records which was used, so a store holding both serves both.
    """
    if not EMBEDDING_VECTOR_BASE64 or not values:
        return values
    try:
        ints = [int(v) for v in values]
    except (ValueError, TypeError):
        return values          # a float encoding cannot use this container
    try:
        if all(-128 <= v <= 127 for v in ints):
            return _VECTOR_INT8_PREFIX + _base64.b64encode(
                _struct.pack("<%db" % len(ints), *ints)).decode("ascii")
        packed = _struct.pack("<%dh" % len(ints), *ints)
    except (struct_error, ValueError, TypeError):
        # Outside int16 as well -- a scale too large for this container. Store the list rather
        # than lose precision silently.
        return values
    return _VECTOR_BASE64_PREFIX + _base64.b64encode(packed).decode("ascii")


def decode_stored_vector(value: Any) -> list:
    """Read a stored vector in either form.

    Dual read is the whole point: a store written before this existed holds lists, and one
    written with it holds tagged strings. Both must serve, and a reader that silently treated
    the string form as "no vector" would stop scoring those nodes with nothing to notice.
    """
    if isinstance(value, str):
        if value.startswith(_VECTOR_INT8_PREFIX):
            blob = _base64.b64decode(value[len(_VECTOR_INT8_PREFIX):])
            return list(_struct.unpack("<%db" % len(blob), blob))
        if value.startswith(_VECTOR_BASE64_PREFIX):
            blob = _base64.b64decode(value[len(_VECTOR_BASE64_PREFIX):])
            return list(_struct.unpack("<%dh" % (len(blob) // 2), blob))
        return []
    if isinstance(value, list):
        return value
    return []


def record_vector(record: Json) -> list:
    """The vector on a record, in either stored form."""
    return decode_stored_vector((record or {}).get("vector"))


def compact_embedding_vector(vector: list[float]) -> list[float]:
    """Drop float digits the encoder never produced.

    Embedding records are the bulk of what an ingest writes - 85% of the bytes for a
    markdown skill - and a 384-value vector serializes to about 8.2 KB at full float
    repr. The models emit f32, which carries roughly seven decimal digits, so the rest
    is noise being paid for.

    Every consumer of a stored vector scores it with cosine, and cosine ignores a
    uniform scale - cos(a, k*b) == cos(a, b) for k > 0 - so the values can be rescaled
    freely as long as one vector is scaled by one factor. That is what lets both the
    integer modes below store whole numbers and keep the score identical.

    Measured on 40,000 embedding records, only the encoding differing:

        round(6) floats   4,207 B/record   3,302 B resident   6,765 B disk
        scale=100000      2,666 B/record   3,193 B resident   4,965 B disk
        int8              1,861 B/record   3,126 B resident   3,688 B disk

    Note what that says: shrinking a vector is a DISK win, not a memory one. Resident
    memory is per-record bookkeeping and barely moves with payload size (-5% across a
    56% smaller record). Reduce record COUNT to reduce memory.

    A cosine number close to 1 is NOT sufficient evidence here. int8 scores 0.99992
    against the original vector, which looks harmless, but the margins between competing
    near-neighbours in a real corpus are smaller than that error. Measured over 500
    candidate chunks from one document with six CN/EN queries, against the float
    ranking:

        scale=100000   top-1 correct 6/6   exact top-10 order 6/6   overlap 10.0/10
        int8           top-1 correct 1/6   exact top-10 order 0/6   overlap  4.8/10

    int8 loses half the top-10. An earlier six-vector probe showed ranking preserved
    and was simply too small to reorder anything. Prefer scale=100000, which is
    exact on this test and still 67.9% of the float size.

    Modes, in precedence order:
      MATRIXARK_EMBEDDING_VECTOR_INT8=1   each vector scaled by its own max magnitude
                                          into [-127, 127]. Smallest, but it CHANGES
                                          RANKING on a realistic candidate set and is
                                          not recommended for retrieval - see below.
      MATRIXARK_EMBEDDING_VECTOR_SCALE=N  integers scaled by N. At 1e5 the worst
                                          cosine is 1.000000000.
      MATRIXARK_EMBEDDING_VECTOR_DECIMALS rounded floats, default 6.
    """
    # 1,690,624 element operations per 1 MB skill -- one per dimension of every chunk vector,
    # and 44% of what record emission costs. The arithmetic is trivial; the Python loop is not,
    # so hand it to numpy where numpy exists. Byte-identical either way: both round half to even,
    # which is why the fallback below is a fallback and not a second behaviour.
    if _NUMPY is not None and vector:
        values = _NUMPY.asarray(vector, dtype=_NUMPY.float64)
        if EMBEDDING_VECTOR_INT8:
            scale = _int8_scale(len(vector))
            return (
                _NUMPY.clip(_NUMPY.rint(values * scale), -127, 127)
                .astype(_NUMPY.int64)
                .tolist()
            )
        if EMBEDDING_VECTOR_SCALE > 0:
            return _NUMPY.rint(values * EMBEDDING_VECTOR_SCALE).astype(_NUMPY.int64).tolist()
        if EMBEDDING_VECTOR_DECIMALS <= 0:
            return vector
        return _NUMPY.round(values, EMBEDDING_VECTOR_DECIMALS).tolist()
    if EMBEDDING_VECTOR_INT8:
        scale = _int8_scale(len(vector))
        return [max(-127, min(127, int(round(value * scale)))) for value in vector]
    if EMBEDDING_VECTOR_SCALE > 0:
        return [int(round(value * EMBEDDING_VECTOR_SCALE)) for value in vector]
    if EMBEDDING_VECTOR_DECIMALS <= 0:
        return vector
    return [round(value, EMBEDDING_VECTOR_DECIMALS) for value in vector]


def embedding_model_name() -> str:
    from matrixark_mcp_embeddings import embedding_provider_name

    provider = embedding_provider_name()
    if provider in {"oss", "open_source", "sentence_transformers", "sentence-transformers"}:
        return os.environ.get("MATRIXARK_EMBEDDING_MODEL_PATH") or os.environ.get(
            "MATRIXARK_EMBEDDING_MODEL",
            "sentence-transformers/all-MiniLM-L6-v2",
        )
    if provider in _API_EMBEDDING_PROVIDERS:
        default_model = "voyage-3" if provider == "voyage" else "text-embedding-3-large"
        return os.environ.get("MATRIXARK_EMBEDDING_MODEL", default_model)
    return "matrixark-local-token-hash-v1"


def embedding_execution_mode_name() -> str:
    from matrixark_mcp_embeddings import embedding_provider_name

    provider = embedding_provider_name()
    if embedding_fallback_used():
        return "local_hash_embedding_fallback"
    if provider in {"oss", "open_source", "sentence_transformers", "sentence-transformers"}:
        return "oss_embedding_model"
    if provider in _API_EMBEDDING_PROVIDERS:
        return "voyage_embedding_api" if provider == "voyage" else "openai_embedding_api"
    if provider == "hash":
        return "hashing-local"
    return "deterministic-token-hash"


def embedding_fallback_used() -> bool:
    return _EMBEDDING_FALLBACK_USED


def oss_embedding_for_text(text: str) -> list[float]:
    global _EMBEDDING_FALLBACK_USED
    model_ref = os.environ.get("MATRIXARK_EMBEDDING_MODEL_PATH") or os.environ.get(
        "MATRIXARK_EMBEDDING_MODEL",
        "sentence-transformers/all-MiniLM-L6-v2",
    )
    try:
        encoder = _OSS_EMBEDDING_MODEL_CACHE.get(model_ref)
        if encoder is None:
            from sentence_transformers import SentenceTransformer  # type: ignore

            encoder = SentenceTransformer(model_ref)
            _OSS_EMBEDDING_MODEL_CACHE[model_ref] = encoder
        vector = encoder.encode([text], normalize_embeddings=True, show_progress_bar=False)[0]
        return [round(float(value), 6) for value in vector]
    except Exception as exc:  # pragma: no cover - depends on optional local model packages.
        if env_bool("MATRIXARK_REQUIRE_OSS_EMBEDDINGS", False):
            raise MatrixArkError(f"OSS embedding model is required but unavailable: {model_ref}: {exc}") from exc
        _EMBEDDING_FALLBACK_USED = True
        previous = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER")
        try:
            os.environ["MATRIXARK_EMBEDDING_PROVIDER"] = "deterministic"
            return embedding_for_text(text)
        finally:
            if previous is None:
                os.environ.pop("MATRIXARK_EMBEDDING_PROVIDER", None)
            else:
                os.environ["MATRIXARK_EMBEDDING_PROVIDER"] = previous


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


def candidate_index_terms(
    record: Json,
    index_terms_by_batch: dict[Any, list[str]],
    index_terms_by_node: dict[Any, list[str]],
    index_terms_by_ref: dict[Any, list[str]] | None = None,
) -> set[str]:
    terms: set[str] = set()
    index_terms_by_ref = index_terms_by_ref or {}
    record_type = record.get("record_type")

    def add_direct_layer_terms() -> None:
        for field, prefix in [
            ("memory_scope", "memory_scope"),
            ("session_continuity", "session_continuity"),
            ("extraction_phase", "extraction_phase"),
            ("version_state", "version_state"),
            ("current_state_policy", "current_state_policy"),
            ("profile_shadowed_reason", "profile_shadowed_reason"),
        ]:
            value = record.get(field)
            if value not in (None, "", [], {}):
                terms.add(context_index_name(prefix, value))
        if bool(record.get("stale_or_superseded") or record.get("stale")):
            terms.add(context_index_name("stale_or_superseded", "true"))
        if record.get("superseded_by_ref_hash") not in (None, "", [], {}):
            terms.add(context_index_name("superseded", "true"))
        if record.get("superseded_by_entity_hash") not in (None, "", [], {}):
            terms.add(context_index_name("superseded", "true"))
        if record.get("profile_shadowed_by_ref_hash") not in (None, "", [], {}):
            terms.add(context_index_name("profile_shadowed", "true"))
        memory_layer = candidate_memory_layer_name(record)
        if memory_layer:
            terms.add(context_index_name("memory_layer", memory_layer))

    def add_source_lineage_terms() -> None:
        role_values: set[str] = set()
        scalar_role = normalize_message_role(record.get("source_role"))
        if scalar_role:
            role_values.add(scalar_role)
        if isinstance(record.get("source_roles"), list):
            role_values.update(
                normalize_message_role(role)
                for role in record.get("source_roles", [])[:16]
                if normalize_message_role(role)
            )
        if isinstance(record.get("source_role_counts"), dict):
            for role, count in list(record.get("source_role_counts", {}).items())[:16]:
                try:
                    if int(count or 0) <= 0:
                        continue
                except (TypeError, ValueError):
                    continue
                role_name = normalize_message_role(role)
                if role_name:
                    role_values.add(role_name)
        for role in sorted(role_values):
            terms.add(context_index_name("source_role", role))
        for field, prefix in [
            ("source_hook_types", "hook_type"),
            ("source_codex_events", "codex_event"),
            ("source_memory_selection_policies", "memory_selection_policy"),
            ("source_memory_layers", "source_memory_layer"),
            ("source_memory_scopes", "source_memory_scope"),
            ("source_session_continuities", "source_session_continuity"),
            ("source_extraction_phases", "extraction_phase"),
            ("source_profile_promotion_policies", "profile_promotion_policy"),
            ("source_profile_promotion_blockers", "profile_promotion_blocker"),
        ]:
            for value in record.get(field, [])[:16] if isinstance(record.get(field), list) else []:
                terms.add(context_index_name(prefix, value))
        for field, prefix in [
            ("source_hook_type_counts", "hook_type"),
            ("source_codex_event_counts", "codex_event"),
            ("source_memory_selection_policy_counts", "memory_selection_policy"),
            ("source_memory_layer_counts", "source_memory_layer"),
        ]:
            counts = record.get(field)
            if not isinstance(counts, dict):
                continue
            for value, count in list(counts.items())[:16]:
                try:
                    if int(count or 0) <= 0:
                        continue
                except (TypeError, ValueError):
                    continue
                terms.add(context_index_name(prefix, value))
        try:
            lossy_count = int(record.get("source_memory_selection_lossy_count") or 0)
        except (TypeError, ValueError):
            lossy_count = 0
        try:
            complete_count = int(record.get("source_memory_selection_complete_count") or 0)
        except (TypeError, ValueError):
            complete_count = 0
        if lossy_count > 0:
            terms.add(context_index_name("memory_selection_quality", "lossy"))
        if complete_count > 0:
            terms.add(context_index_name("memory_selection_quality", "complete"))
        if lossy_count > 0 and complete_count > 0:
            terms.add(context_index_name("memory_selection_quality", "mixed"))

    if record_type == "context_event":
        terms.update(index_terms_by_batch.get(record.get("batch_id_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        terms.add(context_index_name("event_type", record.get("event_type")))
        if record.get("profile_memory_class") not in (None, "", [], {}):
            terms.add(context_index_name("profile_memory_class", record.get("profile_memory_class")))
        if record.get("profile_memory_kind") not in (None, "", [], {}):
            terms.add(context_index_name("profile_memory_kind", record.get("profile_memory_kind")))
        for profile_class in record.get("source_profile_memory_classes", [])[:8] if isinstance(record.get("source_profile_memory_classes"), list) else []:
            terms.add(context_index_name("profile_memory_class", profile_class))
        for profile_kind in record.get("source_profile_memory_kinds", [])[:8] if isinstance(record.get("source_profile_memory_kinds"), list) else []:
            terms.add(context_index_name("profile_memory_kind", profile_kind))
        if not require_oss_understanding() and not record.get("event_type"):
            terms.add(context_index_name("event_type", infer_event_type(str(record.get("text", "")))))
        classification = non_default_classification(record.get("classification"))
        if classification:
            terms.add(context_index_name("classification", classification))
        terms.add(context_index_name("status", record.get("status") or "observed"))
        terms.add(context_index_name("source_type", record.get("source_type") or "message"))
        terms.update(benchmark_quality_index_terms(record.get("text"), record.get("summary_text"), record.get("event_type"), record.get("metadata")))
        add_source_lineage_terms()
        add_direct_layer_terms()
    elif record_type == "context_entity":
        terms.add(context_index_name("entity_type", record.get("entity_type")))
        if record.get("profile_memory_class") not in (None, "", [], {}):
            terms.add(context_index_name("profile_memory_class", record.get("profile_memory_class")))
        if record.get("profile_memory_kind") not in (None, "", [], {}):
            terms.add(context_index_name("profile_memory_kind", record.get("profile_memory_kind")))
        for profile_class in record.get("source_profile_memory_classes", [])[:8] if isinstance(record.get("source_profile_memory_classes"), list) else []:
            terms.add(context_index_name("profile_memory_class", profile_class))
        for profile_kind in record.get("source_profile_memory_kinds", [])[:8] if isinstance(record.get("source_profile_memory_kinds"), list) else []:
            terms.add(context_index_name("profile_memory_kind", profile_kind))
        entity_name = record.get("entity_name")
        if entity_name not in (None, "", [], {}):
            terms.add(context_index_name("entity_name", normalized_index_value(entity_name).replace(":", "_")))
        if bool(record.get("profile_entity_current")):
            terms.add(context_index_name("profile_entity_current", "true"))
        terms.update(codex_outcome_fact_index_terms(record.get("entity_name"), record.get("entity_type"), record.get("state"), record.get("text")))
        terms.update(benchmark_quality_index_terms(record.get("entity_name"), record.get("entity_type"), record.get("state"), record.get("text")))
        add_direct_layer_terms()
        add_source_lineage_terms()
    elif record_type == "context_summary":
        terms.add(context_index_name("summary_type", record.get("summary_type")))
        if record.get("profile_memory_class") not in (None, "", [], {}):
            terms.add(context_index_name("profile_memory_class", record.get("profile_memory_class")))
        if record.get("profile_memory_kind") not in (None, "", [], {}):
            terms.add(context_index_name("profile_memory_kind", record.get("profile_memory_kind")))
        for profile_class in record.get("source_profile_memory_classes", [])[:8] if isinstance(record.get("source_profile_memory_classes"), list) else []:
            terms.add(context_index_name("profile_memory_class", profile_class))
        for profile_kind in record.get("source_profile_memory_kinds", [])[:8] if isinstance(record.get("source_profile_memory_kinds"), list) else []:
            terms.add(context_index_name("profile_memory_kind", profile_kind))
        if bool(record.get("profile_summary_current")):
            terms.add(context_index_name("profile_summary_current", "true"))
        for entity_type in record.get("source_entity_types", [])[:16]:
            terms.add(context_index_name("entity_type", entity_type))
        terms.update(benchmark_quality_index_terms(record.get("summary_type"), record.get("summary_text"), record.get("text")))
        add_direct_layer_terms()
        add_source_lineage_terms()
    elif record_type == "context_compression_event":
        terms.update(index_terms_by_ref.get(record.get("compression_id_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        terms.add(context_index_name("context_class", "compression"))
        terms.add(context_index_name("operator", record.get("operator") or "TIME_COMPRESS"))
        terms.update(benchmark_quality_index_terms(record.get("summary_text"), record.get("text")))
        add_source_lineage_terms()
        add_direct_layer_terms()
    elif record_type == "context_segment":
        terms.add(context_index_name("segment_topic", record.get("topic")))
        if record.get("event_type") not in (None, "", [], {}):
            terms.add(context_index_name("event_type", record.get("event_type")))
        if record.get("profile_memory_class") not in (None, "", [], {}):
            terms.add(context_index_name("profile_memory_class", record.get("profile_memory_class")))
        if record.get("profile_memory_kind") not in (None, "", [], {}):
            terms.add(context_index_name("profile_memory_kind", record.get("profile_memory_kind")))
        for profile_class in record.get("source_profile_memory_classes", [])[:8] if isinstance(record.get("source_profile_memory_classes"), list) else []:
            terms.add(context_index_name("profile_memory_class", profile_class))
        for profile_kind in record.get("source_profile_memory_kinds", [])[:8] if isinstance(record.get("source_profile_memory_kinds"), list) else []:
            terms.add(context_index_name("profile_memory_kind", profile_kind))
        terms.update(codex_outcome_fact_index_terms(record.get("topic"), record.get("text"), record.get("summary_text"), record.get("event_type")))
        terms.update(benchmark_quality_index_terms(record.get("topic"), record.get("text"), record.get("summary_text")))
        add_direct_layer_terms()
        add_source_lineage_terms()
    elif record_type == "resource_chunk":
        terms.update(index_terms_by_ref.get(record.get("chunk_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        terms.add(context_index_name("source_type", "resource"))
        terms.add(context_index_name("resource_type", record.get("resource_type")))
        terms.update(metadata_index_terms(record.get("metadata", {})))
    elif record_type == "skill_manifest":
        terms.update(index_terms_by_ref.get(record.get("skill_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        terms.add(context_index_name("source_type", "skill"))
        terms.add(context_index_name("resource_type", "skill"))
        terms.add(context_index_name("skill_name", record.get("name")))
        for trigger in record.get("triggers", [])[:8]:
            terms.add(context_index_name("skill_trigger", trigger))
        for tool in record.get("allowed_tools", [])[:8]:
            terms.add(context_index_name("skill_tool", tool))
    elif record_type == "skill_section":
        terms.update(index_terms_by_ref.get(record.get("section_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        terms.add(context_index_name("source_type", "skill"))
        terms.add(context_index_name("resource_type", "skill"))
        terms.update(metadata_index_terms(record.get("metadata", {})))
    return prune_internal_index_terms(term for term in terms if term)


def ordered_normalized_role_list(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    roles: list[str] = []
    seen: set[str] = set()
    for item in raw:
        role = normalize_message_role(item)
        if role and role not in seen:
            roles.append(role)
            seen.add(role)
    return roles


def normalize_memory_layer_budget_role_fields(memory_layer_budget: Any) -> Json:
    if not isinstance(memory_layer_budget, dict):
        return {}
    normalized = dict(memory_layer_budget)
    for field in ["source_message_counts_by_role"]:
        role_counts = normalized_role_int_map(normalized.get(field))
        if role_counts:
            normalized[field] = role_counts
    role_buckets = normalized.get("by_source_role")
    if isinstance(role_buckets, dict):
        normalized_buckets: Json = {}
        for role, bucket in role_buckets.items():
            role_name = normalize_message_role(role)
            if not role_name:
                continue
            if not isinstance(bucket, dict):
                continue
            target = normalized_buckets.setdefault(role_name, {})
            for metric in ["refs", "tokens", "selected_refs", "selected_tokens", "dropped_refs", "dropped_tokens"]:
                try:
                    amount = int(bucket.get(metric) or 0)
                except (TypeError, ValueError):
                    amount = 0
                if amount:
                    target[metric] = int(target.get(metric, 0)) + amount
        if normalized_buckets:
            normalized["by_source_role"] = normalized_buckets
    return normalized


def serving_memory_selection_policy_budget(policy: Any) -> Json:
    if not isinstance(policy, dict):
        return {}
    compact: Json = {}
    if "enabled" in policy:
        compact["enabled"] = bool(policy.get("enabled"))
    for field in [
        "mode",
        "budget_semantics",
        "independent_caps",
        "global_remote_budget_enforced",
    ]:
        value = policy.get(field)
        if value not in (None, "", [], {}):
            compact[field] = value
    try:
        remote_budget = int(policy.get("remote_budget_tokens") or 0)
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget > 0:
        compact["remote_budget_tokens"] = remote_budget
    for field in ["budget_tokens", "selected_tokens_by_policy", "selected_ref_count_by_policy"]:
        values = policy.get(field)
        if not isinstance(values, dict):
            continue
        normalized: Json = {}
        for key, raw in values.items():
            label = str(key or "").strip()
            if not label:
                continue
            try:
                amount = int(raw or 0)
            except (TypeError, ValueError):
                continue
            if amount > 0:
                normalized[label] = amount
        if normalized:
            compact[field] = normalized
    return {key: value for key, value in compact.items() if value not in (None, "", [], {})}


def serving_memory_layer_budget(memory_layer_budget: Any, *, include_debug: bool = False) -> Json:
    normalized = normalize_memory_layer_budget_role_fields(memory_layer_budget)
    if include_debug:
        return normalized
    for field in [
        "by_source_role",
        "by_hook_type",
        "by_codex_event",
        "by_memory_selection_policy",
        "source_message_counts_by_role",
        "source_hook_counts_by_type",
        "source_codex_event_counts_by_event",
    ]:
        normalized.pop(field, None)
    try:
        from tools.matrixark_mcp_context_pack import strip_default_debug_lineage_fields
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_context_pack import strip_default_debug_lineage_fields
    normalized = strip_default_debug_lineage_fields(normalized)
    for field in list(normalized):
        if isinstance(normalized.get(field), dict) and not normalized[field]:
            normalized.pop(field, None)
    return normalized


def serving_memory_layer_pressure(memory_layer_pressure: Any, *, include_debug: bool = False) -> Json:
    if not isinstance(memory_layer_pressure, dict):
        return {}
    compact = dict(memory_layer_pressure)
    if include_debug:
        return compact
    lineage_dimensions = {
        "by_source_role",
        "by_hook_type",
        "by_codex_event",
        "by_memory_selection_policy",
        "source_message_counts_by_role",
        "source_hook_counts_by_type",
        "source_codex_event_counts_by_event",
    }
    for list_field in ["pressure_dimensions", "dropped_dimensions"]:
        values = compact.get(list_field)
        if isinstance(values, list):
            compact[list_field] = [value for value in values if str(value) not in lineage_dimensions]
    by_dimension = compact.get("by_dimension")
    if isinstance(by_dimension, dict):
        compact["by_dimension"] = {
            str(key): value for key, value in by_dimension.items() if str(key) not in lineage_dimensions
        }
    for field in [
        "assistant_memory_pressure",
        "user_memory_pressure",
        "tool_memory_pressure",
        "assistant_source_message_pressure",
        "user_source_message_pressure",
        "tool_source_message_pressure",
        "hook_boundary_source_pressure",
        "after_llm_source_pressure",
        "tool_result_source_pressure",
        "stop_event_source_pressure",
        "post_tool_use_source_pressure",
    ]:
        compact.pop(field, None)
    try:
        from tools.matrixark_mcp_context_pack import strip_default_debug_lineage_fields
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_context_pack import strip_default_debug_lineage_fields
    compact = strip_default_debug_lineage_fields(compact)
    by_dimension = compact.get("by_dimension")
    if isinstance(by_dimension, dict):
        compact["by_dimension"] = {
            str(key): value
            for key, value in by_dimension.items()
            if not (isinstance(value, dict) and not value)
        }
        valid_dimensions = set(compact["by_dimension"])
        for list_field in ["pressure_dimensions", "dropped_dimensions"]:
            values = compact.get(list_field)
            if isinstance(values, list):
                compact[list_field] = [
                    value
                    for value in values
                    if not (str(value).startswith("by_") and str(value) not in valid_dimensions)
                ]
        if not compact["by_dimension"]:
            compact.pop("by_dimension", None)
    return compact


def compact_context_pack_policy(policy: Any) -> Json:
    if not isinstance(policy, dict):
        return {}
    keep_fields = [
        "enabled",
        "mode",
        "decision",
        "strategy",
        "quality_gate",
        "resource_mode",
        "skill_mode",
    ]
    compact: Json = {
        field: policy.get(field)
        for field in keep_fields
        if policy.get(field) not in (None, "", [], {})
    }
    is_layer_policy = (
        isinstance(policy.get("selected_tokens_by_layer"), dict)
        or isinstance(policy.get("selected_ref_count_by_layer"), dict)
        or isinstance(policy.get("selected_tokens_by_policy"), dict)
        or isinstance(policy.get("selected_ref_count_by_policy"), dict)
        or isinstance(policy.get("selected_tokens_by_phase"), dict)
        or isinstance(policy.get("selected_ref_count_by_phase"), dict)
    )
    for field in [
        "selected_ref_count",
        "selected_session_count",
        "resource_selected_ref_count",
        "skill_selected_ref_count",
        "entity_bridge_selected_ref_count",
        "selected_tokens",
        "resource_selected_tokens",
        "skill_selected_tokens",
    ]:
        value = policy.get(field)
        if isinstance(value, int) and value > 0:
            compact[field] = value
    for field in [
        "budget_tokens",
        "selected_tokens_by_role",
        "selected_ref_count_by_role",
    ]:
        value = policy.get(field)
        if isinstance(value, dict) and value:
            if field == "budget_tokens" and is_layer_policy:
                compact[field] = {
                    str(key): int(amount)
                    for key, amount in value.items()
                    if str(key or "").strip() and isinstance(amount, int) and amount
                }
            else:
                compact[field] = normalized_role_int_map(value)
    for field in [
        "selected_tokens_by_layer",
        "selected_ref_count_by_layer",
    ]:
        value = policy.get(field)
        if isinstance(value, dict) and value:
            compact[field] = {
                str(key): int(amount)
                for key, amount in value.items()
                if str(key or "").strip() and isinstance(amount, int) and amount
            }
    for field in [
        "selected_tokens_by_policy",
        "selected_ref_count_by_policy",
        "selected_tokens_by_phase",
        "selected_ref_count_by_phase",
    ]:
        value = policy.get(field)
        if isinstance(value, dict) and value:
            compact[field] = {
                str(key): int(amount)
                for key, amount in value.items()
                if str(key or "").strip() and isinstance(amount, int)
            }
    return compact


def compact_dropped_refs_for_context_pack(dropped: Json, *, include_debug: bool = False) -> Json:
    if include_debug or not isinstance(dropped, dict):
        return dropped
    compact: Json = {
        key: value
        for key, value in dropped.items()
        if isinstance(value, int) and value
    }
    estimated = dropped.get("estimated_tokens")
    if isinstance(estimated, dict):
        compact_estimated = {key: value for key, value in estimated.items() if isinstance(value, int) and value}
        if compact_estimated:
            compact["estimated_tokens"] = compact_estimated
    for field in [
        "deadline_exceeded",
        "deadline_reason",
        "min_score",
        "budget_fill_policy",
    ]:
        value = dropped.get(field)
        if value not in (None, "", [], {}):
            compact[field] = value
    for field in [
        "cross_session_policy",
        "shared_context_policy",
        "source_role_budget_policy",
        "memory_layer_budget_policy",
        "memory_selection_policy_budget_policy",
        "extraction_phase_budget_policy",
    ]:
        value = compact_context_pack_policy(dropped.get(field))
        if value:
            compact[field] = value
    if dropped.get("refs"):
        compact["dropped_ref_detail_available_in_audit"] = True
        compact["dropped_ref_count"] = len(dropped.get("refs") or [])
    return compact


def memory_hierarchy_contract_from_recall_policy(recall_policy: Json) -> Json:
    if not isinstance(recall_policy, dict):
        return {}
    continuity = recall_policy.get("session_continuity") if isinstance(recall_policy.get("session_continuity"), dict) else {}
    cross_session = recall_policy.get("cross_session") if isinstance(recall_policy.get("cross_session"), dict) else {}
    shared_context = recall_policy.get("shared_context") if isinstance(recall_policy.get("shared_context"), dict) else {}
    source_role_budget = recall_policy.get("source_role_budget") if isinstance(recall_policy.get("source_role_budget"), dict) else {}
    memory_layer_budget = recall_policy.get("memory_layer_budget_policy") if isinstance(recall_policy.get("memory_layer_budget_policy"), dict) else {}
    memory_selection_policy_budget = (
        recall_policy.get("memory_selection_policy_budget_policy")
        if isinstance(recall_policy.get("memory_selection_policy_budget_policy"), dict)
        else {}
    )
    extraction_phase_budget = (
        recall_policy.get("extraction_phase_budget_policy")
        if isinstance(recall_policy.get("extraction_phase_budget_policy"), dict)
        else {}
    )
    return {
        "models": {
            "session_entity": {
                "record_type": "context_entity",
                "memory_scope": "session",
                "session_continuity": "same_session",
            },
            "profile_entity": {
                "record_type": "context_entity",
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "node_path_suffix": "profile:long_term_memory",
            },
            "profile_index": {
                "record_type": "context_index",
                "data_model": "context_profile_entity",
            },
        },
        "retrieval_strategy": continuity.get(
            "policy",
            "same-session continuity first; entity state bridges cross-session memory; cross-session evidence remains bounded",
        ),
        "session_scope_mode": continuity.get("mode"),
        "cross_session_enabled": cross_session.get("enabled"),
        "cross_session_budget_tokens": cross_session.get("budget_tokens"),
        "cross_session_remote_budget_tokens": cross_session.get("remote_budget_tokens"),
        "cross_session_computed_budget_tokens": cross_session.get("computed_budget_tokens"),
        "cross_session_budget_floor_tokens": cross_session.get("budget_floor_tokens"),
        "cross_session_budget_floor_applied": cross_session.get("budget_floor_applied"),
        "cross_session_budget_floor_status": cross_session.get("budget_floor_status"),
        "cross_session_max_sessions": cross_session.get("max_sessions"),
        "cross_session_max_candidates": cross_session.get("max_candidates"),
        "shared_context_enabled": shared_context.get("enabled"),
        "source_role_budget_enabled": source_role_budget.get("enabled"),
        "source_role_budget_mode": source_role_budget.get("mode"),
        "source_role_budget_semantics": source_role_budget.get("budget_semantics"),
        "source_role_independent_caps": source_role_budget.get("independent_caps"),
        "source_role_global_remote_budget_enforced": source_role_budget.get("global_remote_budget_enforced"),
        "source_role_remote_budget_tokens": source_role_budget.get("remote_budget_tokens"),
        "source_role_budget_tokens": normalized_role_int_map(source_role_budget.get("budget_tokens")),
        "source_role_selected_tokens_by_role": normalized_role_int_map(source_role_budget.get("selected_tokens_by_role")),
        "source_role_selected_ref_count_by_role": normalized_role_int_map(source_role_budget.get("selected_ref_count_by_role")),
        "memory_layer_budget_enabled": memory_layer_budget.get("enabled"),
        "memory_layer_budget_mode": memory_layer_budget.get("mode"),
        "memory_layer_budget_question_type": memory_layer_budget.get("question_type"),
        "memory_layer_budget_question_reason": memory_layer_budget.get("question_budget_reason"),
        "memory_layer_budget_semantics": memory_layer_budget.get("budget_semantics"),
        "memory_layer_independent_caps": memory_layer_budget.get("independent_caps"),
        "memory_layer_global_remote_budget_enforced": memory_layer_budget.get("global_remote_budget_enforced"),
        "memory_layer_remote_budget_tokens": memory_layer_budget.get("remote_budget_tokens"),
        "memory_layer_budget_tokens": normalized_string_int_map(memory_layer_budget.get("budget_tokens")),
        "memory_layer_selected_tokens_by_layer": normalized_string_int_map(memory_layer_budget.get("selected_tokens_by_layer")),
        "memory_layer_selected_ref_count_by_layer": normalized_string_int_map(memory_layer_budget.get("selected_ref_count_by_layer")),
        "memory_selection_policy_budget_enabled": memory_selection_policy_budget.get("enabled"),
        "memory_selection_policy_budget_mode": memory_selection_policy_budget.get("mode"),
        "memory_selection_policy_budget_semantics": memory_selection_policy_budget.get("budget_semantics"),
        "memory_selection_policy_independent_caps": memory_selection_policy_budget.get("independent_caps"),
        "memory_selection_policy_global_remote_budget_enforced": memory_selection_policy_budget.get("global_remote_budget_enforced"),
        "memory_selection_policy_remote_budget_tokens": memory_selection_policy_budget.get("remote_budget_tokens"),
        "memory_selection_policy_budget_tokens": normalized_string_int_map(memory_selection_policy_budget.get("budget_tokens")),
        "memory_selection_policy_selected_tokens_by_policy": normalized_string_int_map(memory_selection_policy_budget.get("selected_tokens_by_policy")),
        "memory_selection_policy_selected_ref_count_by_policy": normalized_string_int_map(memory_selection_policy_budget.get("selected_ref_count_by_policy")),
        "extraction_phase_budget_enabled": extraction_phase_budget.get("enabled"),
        "extraction_phase_budget_mode": extraction_phase_budget.get("mode"),
        "extraction_phase_budget_semantics": extraction_phase_budget.get("budget_semantics"),
        "extraction_phase_independent_caps": extraction_phase_budget.get("independent_caps"),
        "extraction_phase_global_remote_budget_enforced": extraction_phase_budget.get("global_remote_budget_enforced"),
        "extraction_phase_remote_budget_tokens": extraction_phase_budget.get("remote_budget_tokens"),
        "extraction_phase_budget_tokens": normalized_string_int_map(extraction_phase_budget.get("budget_tokens")),
        "extraction_phase_selected_tokens_by_phase": normalized_string_int_map(extraction_phase_budget.get("selected_tokens_by_phase")),
        "extraction_phase_selected_ref_count_by_phase": normalized_string_int_map(extraction_phase_budget.get("selected_ref_count_by_phase")),
        "selected_ref_flow": [
            "local_context_budget",
            "same_session_memory",
            "profile_entity_bridge",
            "bounded_cross_session_evidence",
            "source_role_budget_gate",
            "memory_layer_budget_gate",
            "memory_selection_policy_budget_gate",
            "extraction_phase_budget_gate",
            "summary_or_compression",
            "shared_resource_or_skill",
        ],
    }


def compact_recall_policy_for_audit(recall_policy: Json, *, include_debug: bool = False) -> Json:
    if not isinstance(recall_policy, dict):
        return {}
    tree = recall_policy.get("tree_traversal") if isinstance(recall_policy.get("tree_traversal"), dict) else {}
    secondary = recall_policy.get("secondary_index_filter") if isinstance(recall_policy.get("secondary_index_filter"), dict) else {}
    rerank = recall_policy.get("rerank") if isinstance(recall_policy.get("rerank"), dict) else {}
    hard_deadline = recall_policy.get("hard_deadline") if isinstance(recall_policy.get("hard_deadline"), dict) else {}
    session = recall_policy.get("session_continuity") if isinstance(recall_policy.get("session_continuity"), dict) else {}
    storage_options = recall_policy.get("storage_options") if isinstance(recall_policy.get("storage_options"), dict) else {}
    compact: Json = {}
    query_plan = recall_policy.get("query_plan")
    if isinstance(query_plan, dict):
        compact["query_plan"] = {
            field: query_plan[field]
            for field in ["query_type", "temporal_window", "secondary_filters"]
            if query_plan.get(field) not in (None, "", [], {})
        }
    if tree:
        compact["tree"] = {
            field: tree.get(field)
            for field in [
                "enabled",
                "fallback_to_flat",
                "selected_node_count",
                "selected_leaf_count",
                "candidate_records_after_tree",
                "records_dropped_by_tree",
                "max_candidates_per_node",
            ]
            if tree.get(field) not in (None, "", [], {})
        }
    if secondary:
        compact["secondary_index"] = {
            field: secondary.get(field)
            for field in ["enabled", "matched_candidate_count", "dropped_candidate_count", "effective_mode"]
            if secondary.get(field) not in (None, "", [], {})
        }
    if rerank:
        compact["rerank"] = {
            field: rerank.get(field)
            for field in ["enabled", "mode", "reranked_candidate_count", "min_similarity_score", "budget_fill_policy"]
            if rerank.get(field) not in (None, "", [], {})
        }
    if session:
        compact["session_continuity"] = {
            field: session.get(field)
            for field in ["mode", "same_session_selected_ref_count", "cross_session_selected_ref_count", "entity_bridge_selected_ref_count"]
            if session.get(field) not in (None, "", [], {})
        }
    memory_layer_budget = recall_policy.get("memory_layer_budget")
    if isinstance(memory_layer_budget, dict):
        compact["memory_layer_budget"] = serving_memory_layer_budget(memory_layer_budget, include_debug=include_debug)
    dropped_memory_layer_budget = recall_policy.get("dropped_memory_layer_budget")
    if isinstance(dropped_memory_layer_budget, dict):
        compact["dropped_memory_layer_budget"] = serving_memory_layer_budget(dropped_memory_layer_budget, include_debug=include_debug)
    memory_layer_pressure = recall_policy.get("memory_layer_pressure")
    if isinstance(memory_layer_pressure, dict):
        compact["memory_layer_pressure"] = serving_memory_layer_pressure(memory_layer_pressure, include_debug=include_debug)
    memory_selection_policy_budget = recall_policy.get("memory_selection_policy_budget_policy")
    if isinstance(memory_selection_policy_budget, dict):
        compact_policy = serving_memory_selection_policy_budget(memory_selection_policy_budget)
        if compact_policy:
            compact["memory_selection_policy_budget"] = compact_policy
    async_pipeline_readiness = recall_policy.get("async_pipeline_readiness")
    if isinstance(async_pipeline_readiness, dict):
        compact["async_pipeline_readiness"] = async_pipeline_readiness
    if storage_options:
        compact["storage_route"] = {
            field: storage_options.get(field)
            for field in ["route", "storage_family", "write_mode", "durability_result", "background_write"]
            if storage_options.get(field) not in (None, "", [], {})
        }
    if hard_deadline:
        compact["deadline"] = {
            field: hard_deadline.get(field)
            for field in ["deadline_ms", "elapsed_ms", "partial_context_pack", "fallback_reason"]
            if hard_deadline.get(field) not in (None, "", [], {})
        }
    return compact


def compact_context_pack_audit_record(record: Json, *, include_debug: bool = False) -> Json:
    if include_debug or AUDIT_DEBUG_PAYLOAD:
        return record
    compact: Json = {
        "record_type": record.get("record_type", "context_pack_audit"),
        "context_pack_id": record.get("context_pack_id", ""),
        "query": record.get("query", ""),
        "summary_text": record.get("summary_text", ""),
        "selected_refs": compact_context_pack_refs(record.get("selected_refs", []), include_debug=False),
        "selected_ref_counts": record.get("selected_ref_counts", {}),
        "dropped_refs": compact_dropped_refs_for_context_pack(record.get("dropped_refs", {}), include_debug=False),
        "quality_warnings": record.get("quality_warnings", []),
        "partial_context_pack": bool(record.get("partial_context_pack", False)),
        "question_type": record.get("question_type", ""),
        "packing_policy": record.get("packing_policy", ""),
        "used_local_context_tokens": record.get("used_local_context_tokens", 0),
        "used_remote_context_tokens": record.get("used_remote_context_tokens", 0),
        "total_prompt_context_tokens": record.get("total_prompt_context_tokens", 0),
        "remote_context_budget_tokens": record.get("remote_context_budget_tokens", 0),
        "requested_max_context_tokens": record.get("requested_max_context_tokens", 0),
        "primary_candidate_count": record.get("primary_candidate_count", 0),
        "auxiliary_candidate_count": record.get("auxiliary_candidate_count", 0),
        "tree_prefilter_dropped_count": record.get("tree_prefilter_dropped_count", 0),
        "fanout_dropped_count": record.get("fanout_dropped_count", 0),
        "max_candidates_per_node": record.get("max_candidates_per_node", 0),
        "max_selected_refs": record.get("max_selected_refs", 0),
        "created_at_ms": record.get("created_at_ms"),
        "payload_policy": {
            "mode": "compact_audit",
            "verbose_with": "MATRIXARK_AUDIT_DEBUG_PAYLOAD=1 or replay include_debug_records=true",
        },
    }
    recall_summary = compact_recall_policy_for_audit(record.get("recall_policy", {}))
    if recall_summary:
        compact["recall_policy_summary"] = recall_summary
    memory_hierarchy = memory_hierarchy_contract_from_recall_policy(record.get("recall_policy", {}))
    memory_layer_budget = record.get("memory_layer_budget")
    if not isinstance(memory_layer_budget, dict):
        recall_policy = record.get("recall_policy") if isinstance(record.get("recall_policy"), dict) else {}
        memory_layer_budget = recall_policy.get("memory_layer_budget")
    if isinstance(memory_layer_budget, dict):
        compact["memory_layer_budget"] = serving_memory_layer_budget(memory_layer_budget)
    dropped_memory_layer_budget = record.get("dropped_memory_layer_budget")
    if not isinstance(dropped_memory_layer_budget, dict):
        recall_policy = record.get("recall_policy") if isinstance(record.get("recall_policy"), dict) else {}
        dropped_memory_layer_budget = recall_policy.get("dropped_memory_layer_budget")
    if isinstance(dropped_memory_layer_budget, dict):
        compact["dropped_memory_layer_budget"] = serving_memory_layer_budget(dropped_memory_layer_budget)
    memory_layer_pressure = record.get("memory_layer_pressure")
    if not isinstance(memory_layer_pressure, dict):
        recall_policy = record.get("recall_policy") if isinstance(record.get("recall_policy"), dict) else {}
        memory_layer_pressure = recall_policy.get("memory_layer_pressure")
    if isinstance(memory_layer_pressure, dict):
        compact["memory_layer_pressure"] = serving_memory_layer_pressure(memory_layer_pressure)
    async_pipeline_readiness = record.get("async_pipeline_readiness")
    if not isinstance(async_pipeline_readiness, dict):
        recall_policy = record.get("recall_policy") if isinstance(record.get("recall_policy"), dict) else {}
        async_pipeline_readiness = recall_policy.get("async_pipeline_readiness")
    if isinstance(async_pipeline_readiness, dict):
        compact["async_pipeline_readiness"] = async_pipeline_readiness
    local_policy = record.get("local_context_policy")
    if isinstance(local_policy, dict):
        compact["local_context_policy"] = {
            field: local_policy.get(field)
            for field in ["local_context_count", "local_context_tokens", "safety_margin_tokens", "remote_is_additive_only_within_remaining_budget"]
            if local_policy.get(field) not in (None, "", [], {})
        }
    visibility = record.get("operational_visibility_policy")
    if isinstance(visibility, dict):
        compact["operational_visibility_policy"] = {
            field: visibility.get(field)
            for field in ["audit_mode", "audit_sample_rate", "rich_replay_audit", "telemetry_record"]
            if visibility.get(field) not in (None, "", [], {})
        }
    recall_policy = record.get("recall_policy", {})
    if isinstance(recall_policy, dict):
        pushdown = recall_policy.get("backend_retrieval_pushdown", {})
        if isinstance(pushdown, dict):
            compact_pushdown = {
                field: pushdown.get(field)
                for field in [
                    "execution_mode",
                    "loaded_records",
                    "scanned_records",
                    "dropped_by_type",
                    "index_postings_read",
                    "placement_partitions_touched",
                    "source",
                ]
                if pushdown.get(field) not in (None, "", [], {})
            }
            if compact_pushdown:
                compact["backend_retrieval_pushdown"] = compact_pushdown
    compact = {key: value for key, value in compact.items() if value not in (None, "", [], {})}
    try:
        from tools.matrixark_mcp_context_pack import strip_default_debug_lineage_fields
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_context_pack import strip_default_debug_lineage_fields
    return strip_default_debug_lineage_fields(compact)


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
        "selection_reason",
        "matched_index_terms",
        "resource_hash",
        "raw_uri_hash",
        "source_locator",
        "resource_type",
        "resource_version",
        "context_class",
        "source_chunk_hash",
        "access_decision",
        "access_scope",
        "sharing_scope",
        "deployment_scope",
        "version_state",
        "stale_or_superseded",
        "citation",
        "operator",
        "source_start_ms",
        "source_end_ms",
        "source_event_ids",
        "summary_type",
        "session_continuity",
        "continuity_boost",
        "continuity_reason",
    ]
    metadata_keep_fields = [
        "unit_kind",
        "heading",
        "heading_slug",
        "heading_path",
        "relative_path",
        "keywords",
        "source_locator",
        "resource_version",
        "row_start",
        "row_end",
        "record_start",
        "record_end",
        "page",
        "page_section",
        "slide_number",
        "sheet_name",
        "row_count",
        "supersedes_chunk_hash",
        "parse_warnings",
    ]
    for ref in refs:
        item = {field: ref[field] for field in keep_fields if field in ref}
        metadata = ref.get("metadata", {})
        if isinstance(metadata, dict):
            compact_metadata = {field: metadata[field] for field in metadata_keep_fields if field in metadata}
            if compact_metadata:
                item["metadata"] = compact_metadata
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
    anthropic_summary = provider in {"anthropic", "claude", "anthropic_messages"}
    if provider not in {"openai", "openai_compatible", "openai_compatible_llm", "anthropic", "claude", "anthropic_messages"}:
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
    if anthropic_summary:
        payload = {
            "model": TIME_COMPRESSION_SUMMARY_MODEL,
            "max_tokens": 512,
            "system": "You write concise, factual memory compression summaries for an LLM context system.",
            "messages": [
                {"role": "user", "content": prompt},
            ],
        }
        request = urllib.request.Request(
            f"{ANTHROPIC_API_BASE}/v1/messages",
            data=json.dumps(payload).encode("utf-8"),
            headers={"x-api-key": api_key, "anthropic-version": ANTHROPIC_API_VERSION, "Content-Type": "application/json"},
            method="POST",
        )
    else:
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
        if anthropic_summary:
            blocks = data.get("content", [])
            summary = "".join(
                str(block.get("text", ""))
                for block in blocks
                if isinstance(block, dict) and block.get("type", "text") == "text"
            ).strip() if isinstance(blocks, list) else ""
        else:
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



def summary_provider() -> str:
    provider = os.environ.get("MATRIXARK_SUMMARY_PROVIDER", SUMMARY_LLM_PROVIDER).strip().lower().replace("-", "_")
    if provider in {"oss", "open_source", "local_llm", "oss_llm"}:
        return "openai_compatible"
    if provider in {"", "deterministic", "rules", "local"} and require_oss_understanding():
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
            result = openai_compatible_json_call(
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
            if require_oss_understanding():
                raise
            provider_meta["fallback_used"] = True
            provider_meta["execution_mode"] = "deterministic_fallback"
            return fallback_summary, provider_meta
    if require_oss_understanding():
        raise MatrixArkError(f"unsupported OSS summary provider: {provider}")
    provider_meta["fallback_used"] = True
    provider_meta["execution_mode"] = "deterministic_fallback"
    return fallback_summary, provider_meta

def scope_matches(record_scope: Json, query_scope: Json) -> bool:
    if not query_scope:
        return True
    sharing_scope = str(record_scope.get("sharing_scope") or "").strip().lower()
    if sharing_scope == "global_shared":
        return True
    if sharing_scope == "tenant_shared":
        for field in ["account_id", "account_hash", "tenant_id", "tenant_hash"]:
            query_value = query_scope.get(field)
            record_value = record_scope.get(field)
            if query_value and record_value and query_value != record_value:
                return False
        return True
    explicit_keys = set(query_scope.get("_explicit_scope_keys", []))
    record_scope_key = str(record_scope.get("scope_key") or "")
    if record_scope_key:
        if not scope_key_matches_query(record_scope_key, query_scope, explicit_keys):
            return False
        if set(record_scope.keys()).issubset({"scope_key"}):
            return True
    for key, value in query_scope.items():
        if str(key).startswith("_"):
            continue
        if key == "scope_key":
            continue
        if key == "agent_name" and key not in record_scope:
            continue
        if key in {"team", "project"} and key not in record_scope and record_scope_key:
            continue
        if key in {"agent_name", "team", "project"} and key not in explicit_keys:
            continue
        if key in {"account_id", "tenant_id", "account_hash", "tenant_hash"} and record_scope_key and key not in record_scope:
            continue
        if key in {"user_id", "user_hash"} and "user_id" not in explicit_keys:
            continue
        if key in {"session_id", "session_hash"}:
            if "session_id" not in explicit_keys or session_scope_mode(query_scope) == "prefer":
                continue
        if record_scope.get(key) != value:
            return False
    return True


def candidate_access_scope(record: Json) -> Json:
    access_scope = record.get("access_scope")
    if isinstance(access_scope, dict) and access_scope:
        return access_scope
    metadata = record.get("metadata")
    if isinstance(metadata, dict) and isinstance(metadata.get("access_scope"), dict):
        return metadata["access_scope"]
    serving_scope = scope_from_serving_record(record)
    if serving_scope:
        return serving_scope
    node_path = record.get("node_path")
    if isinstance(node_path, list):
        recovered_scope: Json = {}
        for part in node_path:
            value = str(part or "")
            if value.startswith("tenant:"):
                recovered_scope["tenant_id"] = value.split(":", 1)[1]
            elif value.startswith("user:"):
                recovered_scope["user_id"] = value.split(":", 1)[1]
            elif value.startswith("session:"):
                recovered_scope["session_id"] = value.split(":", 1)[1]
        if recovered_scope:
            return recovered_scope
    envelope = record.get("envelope", {})
    if isinstance(envelope, dict):
        return envelope.get("scope", {})
    return {}


def access_scope_matches_before_scoring(record: Json, query_scope: Json) -> bool:
    """Gate candidate eligibility before semantic scoring."""
    record_scope = candidate_access_scope(record)
    sharing_scope = str(record_scope.get("sharing_scope") or record.get("sharing_scope") or "").strip().lower()
    if sharing_scope == "global_shared":
        return True
    if sharing_scope == "tenant_shared":
        for field in ["account_id", "account_hash", "tenant_id", "tenant_hash"]:
            query_value = query_scope.get(field)
            record_value = record_scope.get(field)
            if query_value and record_value and query_value != record_value:
                return False
        return True
    if (
        str(record.get("memory_scope") or "") == "user_profile"
        and str(record.get("session_continuity") or "") == "cross_session"
    ):
        for field in ["account_id", "account_hash", "tenant_id", "tenant_hash", "user_id", "user_hash"]:
            query_value = query_scope.get(field)
            record_value = record_scope.get(field)
            if query_value and record_value and query_value != record_value:
                return False
        return True
    return scope_matches(record_scope, query_scope)


def session_continuity_status(record_scope: Json, query_scope: Json) -> str:
    query_session = str(query_scope.get("session_id") or "")
    if not query_session:
        return "unscoped"
    record_session = str(record_scope.get("session_id") or "")
    if record_session == query_session:
        return "same_session"
    record_key = str(record_scope.get("scope_key") or "")
    query_session_hash = int(query_scope.get("session_hash") or 0)
    if record_key and query_session_hash and parse_scope_key(record_key).get("s") == query_session_hash:
        return "same_session"
    if record_session or record_key:
        return "cross_session"
    return "unscoped"


def session_continuity_boost(candidate: Json, question_type: str) -> float:
    status = str(candidate.get("session_continuity") or "")
    ref_type = str(candidate.get("ref_type") or "")
    context_class = str(candidate.get("context_class") or ref_type)
    memory_scope = str(candidate.get("memory_scope") or "").strip().lower()
    if status == "same_session":
        if ref_type in {"event", "segment"}:
            return 0.16
        if ref_type == "summary":
            return 0.12
        if ref_type == "entity":
            return 0.10
        return 0.08
    if status == "cross_session":
        if question_type == "profile_memory" and memory_scope == "user_profile":
            if ref_type == "entity":
                return 0.16
            if ref_type in {"summary", "compression"}:
                return 0.14
            if ref_type == "segment":
                return 0.08
        if ref_type == "entity" or context_class in {"resource_entity_fact", "resource_fact"}:
            return 0.11
        if question_type in {"multi_hop", "current_state"} and ref_type in {"event", "segment", "compression"}:
            return 0.06
    return 0.0


def cross_session_rerank_adjustment(candidate: Json, question_type: str) -> float:
    if str(candidate.get("session_continuity") or "") != "cross_session":
        return 0.0
    ref_type = str(candidate.get("ref_type") or "")
    context_class = str(candidate.get("context_class") or ref_type)
    memory_scope = str(candidate.get("memory_scope") or "").strip().lower()
    profile_memory_kind = str(candidate.get("profile_memory_kind") or "").strip().lower()
    is_feature_profile_memory = candidate_is_feature_profile_memory(candidate)
    has_citation = bool(candidate.get("source_ref") or candidate.get("citation") or candidate.get("source_chunk_hash"))
    if question_type == "profile_memory":
        if memory_scope == "user_profile" or profile_memory_kind == "durable_profile" or is_feature_profile_memory:
            if ref_type == "entity":
                return 0.16
            if ref_type in {"summary", "compression"}:
                return 0.12
            if ref_type == "segment":
                return 0.06
        if profile_memory_kind == "codex_outcome" and ref_type in {"entity", "summary", "compression"}:
            return 0.10
    if ref_type == "entity":
        profile_boost = 0.04 if memory_scope == "user_profile" else 0.0
        base_boost = 0.10 if question_type in {"current_state", "latest", "multi_hop"} else 0.06
        return base_boost + profile_boost
    if context_class in {"resource_fact", "resource_entity_fact"}:
        return 0.06 if has_citation else 0.04
    if ref_type == "resource_chunk" and has_citation:
        return 0.04
    if ref_type in {"event", "segment"} and question_type in {"multi_hop", "why_emotion", "fact", "evidence"}:
        return 0.01
    if ref_type == "compression":
        return 0.05
    if ref_type == "summary":
        return 0.05 if question_type in {"broad_exploration", "profile_memory"} else 0.02
    return 0.0


# Re-export identity/scope helpers split into matrixark_mcp_core_identity.py
try:  # package import path (tools.matrixark_mcp_core)
    from .matrixark_mcp_core_identity import *  # noqa: E402,F401,F403
except ImportError:  # top-level import path (matrixark_mcp_core)
    from matrixark_mcp_core_identity import *  # noqa: E402,F401,F403

# Re-export compact/context-record helpers split into matrixark_mcp_core_compact.py
try:  # package path
    from .matrixark_mcp_core_compact import *  # noqa: E402,F401,F403
except ImportError:  # top-level path
    from matrixark_mcp_core_compact import *  # noqa: E402,F401,F403

# Re-export session/prior-context helpers split into matrixark_mcp_core_session.py
try:  # package path
    from .matrixark_mcp_core_session import *  # noqa: E402,F401,F403
except ImportError:  # top-level path
    from matrixark_mcp_core_session import *  # noqa: E402,F401,F403

# Re-export helpers split into matrixark_mcp_core_extraction.py
try:  # package path
    from .matrixark_mcp_core_extraction import *  # noqa: E402,F401,F403
except ImportError:  # top-level path
    from matrixark_mcp_core_extraction import *  # noqa: E402,F401,F403

# Re-export helpers split into matrixark_mcp_core_codex_outcome.py
try:  # package path
    from .matrixark_mcp_core_codex_outcome import *  # noqa: E402,F401,F403
except ImportError:  # top-level path
    from matrixark_mcp_core_codex_outcome import *  # noqa: E402,F401,F403

# Re-export helpers split into matrixark_mcp_core_resource_io.py
try:  # package path
    from .matrixark_mcp_core_resource_io import *  # noqa: E402,F401,F403
except ImportError:  # top-level path
    from matrixark_mcp_core_resource_io import *  # noqa: E402,F401,F403

# Re-export helpers split into matrixark_mcp_core_query_analysis.py
try:  # package path
    from .matrixark_mcp_core_query_analysis import *  # noqa: E402,F401,F403
except ImportError:  # top-level path
    from matrixark_mcp_core_query_analysis import *  # noqa: E402,F401,F403

# Re-export helpers split into matrixark_mcp_core_scoring.py
try:  # package path
    from .matrixark_mcp_core_scoring import *  # noqa: E402,F401,F403
except ImportError:  # top-level path
    from matrixark_mcp_core_scoring import *  # noqa: E402,F401,F403

# Re-export helpers split into matrixark_mcp_core_candidate_policy.py
try:  # package path
    from .matrixark_mcp_core_candidate_policy import *  # noqa: E402,F401,F403
except ImportError:  # top-level path
    from matrixark_mcp_core_candidate_policy import *  # noqa: E402,F401,F403

# Re-export helpers split into matrixark_mcp_core_packing.py
try:  # package path
    from .matrixark_mcp_core_packing import *  # noqa: E402,F401,F403
except ImportError:  # top-level path
    from matrixark_mcp_core_packing import *  # noqa: E402,F401,F403

# Re-export helpers split into matrixark_mcp_core_ref_selection.py
try:  # package path
    from .matrixark_mcp_core_ref_selection import *  # noqa: E402,F401,F403
except ImportError:  # top-level path
    from matrixark_mcp_core_ref_selection import *  # noqa: E402,F401,F403

# Re-export helpers split into matrixark_mcp_core_context_pack.py
try:  # package path
    from .matrixark_mcp_core_context_pack import *  # noqa: E402,F401,F403
except ImportError:  # top-level path
    from matrixark_mcp_core_context_pack import *  # noqa: E402,F401,F403

# Re-export helpers split into matrixark_mcp_core_node_tree.py
try:  # package path
    from .matrixark_mcp_core_node_tree import *  # noqa: E402,F401,F403
except ImportError:  # top-level path
    from matrixark_mcp_core_node_tree import *  # noqa: E402,F401,F403
