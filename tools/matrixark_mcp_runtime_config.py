#!/usr/bin/env python3
"""Runtime configuration constants for MatrixArk MCP."""

from __future__ import annotations

import os


MAX_PRIOR_MESSAGES = 8
MAX_PRIOR_CHARS = 4096

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
ENABLE_CONTEXT_DEBUG_RECORDS = os.environ.get("MATRIXARK_CONTEXT_DEBUG_RECORDS", "0").strip().lower() in {"1", "true", "yes"}
ENABLE_CONTEXT_REPLAY = os.environ.get("MATRIXARK_ENABLE_REPLAY", "0").strip().lower() in {"1", "true", "yes"}
ENABLE_SUMMARY_REFRESH_AUDIT = os.environ.get("MATRIXARK_SUMMARY_REFRESH_AUDIT", "0").strip().lower() in {"1", "true", "yes"}
ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS = os.environ.get("MATRIXARK_SUMMARY_DIRTY_DEBUG_FIELDS", "0").strip().lower() in {"1", "true", "yes"}
SUMMARY_REFRESH_INTERVAL_MS = int(os.environ.get("MATRIXARK_SUMMARY_REFRESH_INTERVAL_MS", "1000"))
SUMMARY_REFRESH_LIMIT = int(os.environ.get("MATRIXARK_SUMMARY_REFRESH_LIMIT", "64"))

BACKEND_READINESS_TIMEOUT_MS = int(os.environ.get("MATRIXARK_BACKEND_READINESS_TIMEOUT_MS", "30000"))
BACKEND_READINESS_BACKOFF_MS = int(os.environ.get("MATRIXARK_BACKEND_READINESS_BACKOFF_MS", "200"))
BACKEND_READINESS_CONNECT_TIMEOUT_MS = int(os.environ.get("MATRIXARK_BACKEND_READINESS_CONNECT_TIMEOUT_MS", "1000"))

MATRIXARK_MCP_PROFILE = os.environ.get("MATRIXARK_MCP_PROFILE", "dev").strip().lower()
MATRIXARK_ALLOW_LOCAL_BACKEND = os.environ.get("MATRIXARK_ALLOW_LOCAL_BACKEND", "0").strip().lower() in {"1", "true", "yes"}
MATRIXARK_REQUIRE_BACKEND_READY = os.environ.get("MATRIXARK_REQUIRE_BACKEND_READY", "").strip().lower()
MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = os.environ.get("MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK", "").strip().lower()
MATRIXARK_ALLOW_PYTHON_HOT_CACHE = os.environ.get("MATRIXARK_ALLOW_PYTHON_HOT_CACHE", "").strip().lower()
MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER = os.environ.get("MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER", "").strip().lower()


def matrixark_production_profile_enabled() -> bool:
    return MATRIXARK_MCP_PROFILE in {"prod", "production", "benchmark", "bench", "parity"}


def python_hot_cache_allowed(*, backend_label: str = "") -> bool:
    if MATRIXARK_ALLOW_PYTHON_HOT_CACHE:
        return MATRIXARK_ALLOW_PYTHON_HOT_CACHE in {"1", "true", "yes"}
    return backend_label == "local"


def native_candidate_prefilter_required(*, backend_label: str = "") -> bool:
    if MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER:
        return MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER in {"1", "true", "yes"}
    return backend_label != "local"

DEFAULT_MAX_CONTEXT_TOKENS = int(os.environ.get("MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS", "128000"))

# Context-source policy: which context the agent's prompt is assembled from.
#   auto (default)   -> synthetic/debug requests (retrieve arg synthetic=true) use the
#                       remote TemporalStore pack ONLY; real Codex/Claude sessions keep
#                       local + remote (legacy local-first-fill-remaining behavior).
#   remote_only      -> global flip: EVERY request drops the local reservation and uses
#                       the remote pack only. Set this once TemporalStore recall is proven
#                       good enough to replace local replay for real sessions too.
#   local_and_remote -> force legacy local-first for every request (disables the flip).
# Per-request override: pass "context_source_mode" in the retrieve args.
DEFAULT_CONTEXT_SOURCE_MODE = os.environ.get("MATRIXARK_CONTEXT_SOURCE_MODE", "auto").strip().lower()


def resolve_context_source_mode(args: dict | None, *, default_mode: str | None = None) -> str:
    """Resolve a retrieve request to 'remote_only' or 'local_and_remote'.

    remote_only reserves zero local-context budget so the remote (TemporalStore) pack
    receives the whole token window and no local refs are injected. 'auto' keys off the
    request's synthetic flag: synthetic/debug -> remote_only, real sessions -> local+remote.
    An explicit per-request "context_source_mode" wins over the env default.
    """
    args = args if isinstance(args, dict) else {}
    raw = args.get("context_source_mode")
    mode = str(raw or default_mode or DEFAULT_CONTEXT_SOURCE_MODE or "auto").strip().lower()
    if mode in {"remote_only", "local_and_remote"}:
        return mode
    # auto (or any unrecognized value): decide from the synthetic flag.
    return "remote_only" if bool(args.get("synthetic")) else "local_and_remote"


# Remote-only safety floor: if a remote_only retrieve produces a pack below this many
# tokens (cold start, or a sparse-topic retrieval miss like the q_storage empty-pack case),
# fall back to the request's local context for that turn so the agent is never left blind.
# Makes the eventual global flip safe by construction. 0 disables the fallback.
DEFAULT_REMOTE_ONLY_LOCAL_FALLBACK_FLOOR_TOKENS = int(
    os.environ.get("MATRIXARK_REMOTE_ONLY_LOCAL_FALLBACK_FLOOR_TOKENS", "256")
)


def apply_remote_only_local_fallback(
    local_budget: dict,
    used_remote_tokens: int,
    *,
    floor_tokens: int | None = None,
) -> bool:
    """Re-admit local context when a remote_only pack came back too sparse.

    Only fires when the request was resolved to remote_only (local was set aside, not
    absent). Restores the stashed local items + token estimate so the downstream local-ref
    assembly includes them, and flips the recorded mode to 'remote_only_local_fallback' for
    telemetry. Returns True when the fallback was applied. Real local+remote requests and
    remote_only requests with an adequate remote pack are untouched.
    """
    if not isinstance(local_budget, dict):
        return False
    if local_budget.get("context_source_mode") != "remote_only":
        return False
    floor = DEFAULT_REMOTE_ONLY_LOCAL_FALLBACK_FLOOR_TOKENS if floor_tokens is None else int(floor_tokens)
    if floor <= 0:
        return False
    fallback_items = local_budget.get("_remote_only_fallback_items") or []
    if int(used_remote_tokens or 0) >= floor or not fallback_items:
        return False
    local_budget["items"] = list(fallback_items)
    local_budget["token_estimate"] = int(local_budget.get("observed_local_token_estimate", 0))
    local_budget["context_source_mode"] = "remote_only_local_fallback"
    return True
CONTEXT_PACK_DEBUG_REFS = os.environ.get("MATRIXARK_CONTEXT_PACK_DEBUG_REFS", "0").strip().lower() in {"1", "true", "yes"}
AUDIT_DEBUG_PAYLOAD = os.environ.get("MATRIXARK_AUDIT_DEBUG_PAYLOAD", "0").strip().lower() in {"1", "true", "yes"}

DEFAULT_MAX_CHILDREN_SCORED_PER_PARENT = int(os.environ.get("MATRIXARK_MAX_CHILDREN_SCORED_PER_PARENT", "100000"))
HARD_MAX_CHILDREN_SCORED_PER_PARENT = int(os.environ.get("MATRIXARK_HARD_MAX_CHILDREN_SCORED_PER_PARENT", "100000"))
RESOURCE_ASYNC_DEFAULT_BYTES = int(os.environ.get("MATRIXARK_RESOURCE_ASYNC_DEFAULT_BYTES", str(2 * 1024 * 1024)))
RESOURCE_ASYNC_DEFAULT_TEXT_CHARS = int(os.environ.get("MATRIXARK_RESOURCE_ASYNC_DEFAULT_TEXT_CHARS", "200000"))
RESOURCE_ASYNC_DEFAULT_PATH_COUNT = int(os.environ.get("MATRIXARK_RESOURCE_ASYNC_DEFAULT_PATH_COUNT", "32"))

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

DEFAULT_CROSS_SESSION_BUDGET_RATIO = float(os.environ.get("MATRIXARK_CROSS_SESSION_BUDGET_RATIO", "0.12"))
DEFAULT_CROSS_SESSION_CURRENT_STATE_BUDGET_RATIO = float(os.environ.get("MATRIXARK_CROSS_SESSION_CURRENT_STATE_BUDGET_RATIO", "0.20"))
DEFAULT_CROSS_SESSION_MULTI_HOP_BUDGET_RATIO = float(os.environ.get("MATRIXARK_CROSS_SESSION_MULTI_HOP_BUDGET_RATIO", "0.20"))
DEFAULT_CROSS_SESSION_BROAD_BUDGET_RATIO = float(os.environ.get("MATRIXARK_CROSS_SESSION_BROAD_BUDGET_RATIO", "0.15"))
DEFAULT_CROSS_SESSION_PROFILE_BUDGET_RATIO = float(os.environ.get("MATRIXARK_CROSS_SESSION_PROFILE_BUDGET_RATIO", "0.30"))
DEFAULT_CROSS_SESSION_MAX_BUDGET_TOKENS = int(os.environ.get("MATRIXARK_CROSS_SESSION_MAX_BUDGET_TOKENS", "8192"))
DEFAULT_CROSS_SESSION_PROFILE_MAX_BUDGET_TOKENS = int(
    os.environ.get("MATRIXARK_CROSS_SESSION_PROFILE_MAX_BUDGET_TOKENS", "16384")
)
DEFAULT_CROSS_SESSION_MAX_SESSIONS = int(os.environ.get("MATRIXARK_CROSS_SESSION_MAX_SESSIONS", "3"))
DEFAULT_CROSS_SESSION_MAX_CANDIDATES = int(os.environ.get("MATRIXARK_CROSS_SESSION_MAX_CANDIDATES", "24"))
DEFAULT_CROSS_SESSION_MIN_ENTITY_BRIDGE_REFS = int(os.environ.get("MATRIXARK_CROSS_SESSION_MIN_ENTITY_BRIDGE_REFS", "2"))
DEFAULT_CROSS_SESSION_PARALLELISM = int(os.environ.get("MATRIXARK_CROSS_SESSION_PARALLELISM", "4"))
DEFAULT_CROSS_SESSION_MIN_BUDGET_TOKENS = int(os.environ.get("MATRIXARK_CROSS_SESSION_MIN_BUDGET_TOKENS", "256"))
DEFAULT_CROSS_SESSION_MIN_SCORE = float(os.environ.get("MATRIXARK_CROSS_SESSION_MIN_SCORE", "0.20"))
DEFAULT_CROSS_SESSION_RAW_EVIDENCE_MIN_SCORE = float(os.environ.get("MATRIXARK_CROSS_SESSION_RAW_EVIDENCE_MIN_SCORE", "0.45"))
DEFAULT_CROSS_SESSION_MAX_BUDGET_RATIO = float(os.environ.get("MATRIXARK_CROSS_SESSION_MAX_BUDGET_RATIO", "0.20"))
DEFAULT_CROSS_SESSION_PROFILE_MAX_BUDGET_RATIO = float(os.environ.get("MATRIXARK_CROSS_SESSION_PROFILE_MAX_BUDGET_RATIO", "0.35"))
DEFAULT_CROSS_SESSION_PROFILE_MAX_SESSIONS = int(os.environ.get("MATRIXARK_CROSS_SESSION_PROFILE_MAX_SESSIONS", "6"))
DEFAULT_CROSS_SESSION_PROFILE_MAX_CANDIDATES = int(os.environ.get("MATRIXARK_CROSS_SESSION_PROFILE_MAX_CANDIDATES", "48"))
DEFAULT_CROSS_SESSION_PROFILE_MIN_ENTITY_BRIDGE_REFS = int(os.environ.get("MATRIXARK_CROSS_SESSION_PROFILE_MIN_ENTITY_BRIDGE_REFS", "3"))
DEFAULT_CROSS_SESSION_PREFERRED_REF_TYPES = tuple(
    item.strip()
    for item in os.environ.get("MATRIXARK_CROSS_SESSION_PREFERRED_REF_TYPES", "entity,summary,compression").split(",")
    if item.strip()
)

DEFAULT_SHARED_RESOURCE_BUDGET_RATIO = float(os.environ.get("MATRIXARK_SHARED_RESOURCE_BUDGET_RATIO", "0.25"))
DEFAULT_SHARED_RESOURCE_MAX_BUDGET_RATIO = float(os.environ.get("MATRIXARK_SHARED_RESOURCE_MAX_BUDGET_RATIO", "0.25"))
DEFAULT_SHARED_RESOURCE_MAX_BUDGET_TOKENS = int(os.environ.get("MATRIXARK_SHARED_RESOURCE_MAX_BUDGET_TOKENS", "16384"))
DEFAULT_SHARED_SKILL_BUDGET_RATIO = float(os.environ.get("MATRIXARK_SHARED_SKILL_BUDGET_RATIO", "0.10"))
DEFAULT_SHARED_SKILL_MAX_BUDGET_RATIO = float(os.environ.get("MATRIXARK_SHARED_SKILL_MAX_BUDGET_RATIO", "0.10"))
DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS = int(os.environ.get("MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS", "8192"))
DEFAULT_SHARED_CONTEXT_MIN_SCORE = float(os.environ.get("MATRIXARK_SHARED_CONTEXT_MIN_SCORE", "0.20"))

TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE", "256"))
TIME_COMPRESSION_WINDOW_EVENTS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_WINDOW_EVENTS", "64"))
TIME_COMPRESSION_MIN_EVENTS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MIN_EVENTS", "8"))
TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH", "4"))
TIME_COMPRESSION_MIN_EVENT_AGE_MS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MIN_EVENT_AGE_MS", "0"))
TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS", str(30 * 24 * 60 * 60 * 1000)))
TIME_COMPRESSION_REINFORCEMENT_PROTECT_MS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_REINFORCEMENT_PROTECT_MS", str(30 * 24 * 60 * 60 * 1000)))

ENABLE_LLM_MERGE_OPERATOR = os.environ.get("MATRIXARK_ENABLE_LLM_MERGE_OPERATOR", "").strip().lower() in {"1", "true", "yes"}
DEFAULT_ENTITY_MERGE_OPERATOR = os.environ.get("MATRIXARK_ENTITY_MERGE_OPERATOR", "EUA_MERGE").strip().upper() or "EUA_MERGE"

DEFAULT_BUSINESS_TYPE_WEIGHTS: dict[str, float] = {
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
