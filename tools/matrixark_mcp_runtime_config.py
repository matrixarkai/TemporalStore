#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Runtime configuration constants for MatrixArk MCP."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_env import env_bool
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool


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
ENABLE_CONTEXT_DEBUG_RECORDS = env_bool("MATRIXARK_CONTEXT_DEBUG_RECORDS", False)
ENABLE_CONTEXT_REPLAY = env_bool("MATRIXARK_ENABLE_REPLAY", False)
ENABLE_SUMMARY_REFRESH_AUDIT = env_bool("MATRIXARK_SUMMARY_REFRESH_AUDIT", False)
ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS = env_bool("MATRIXARK_SUMMARY_DIRTY_DEBUG_FIELDS", False)
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
BACKEND_READINESS_CONNECT_TIMEOUT_MS = int(os.environ.get("MATRIXARK_BACKEND_READINESS_CONNECT_TIMEOUT_MS", "1000"))

MATRIXARK_MCP_PROFILE = os.environ.get("MATRIXARK_MCP_PROFILE", "dev").strip().lower()
MATRIXARK_ALLOW_LOCAL_BACKEND = env_bool("MATRIXARK_ALLOW_LOCAL_BACKEND", False)
MATRIXARK_REQUIRE_BACKEND_READY = os.environ.get("MATRIXARK_REQUIRE_BACKEND_READY", "").strip().lower()
MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = os.environ.get("MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK", "").strip().lower()
MATRIXARK_ALLOW_PYTHON_HOT_CACHE = os.environ.get("MATRIXARK_ALLOW_PYTHON_HOT_CACHE", "").strip().lower()
MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER = os.environ.get("MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER", "").strip().lower()


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

DEFAULT_MAX_CONTEXT_TOKENS = int(os.environ.get("MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS", "500000"))

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

# Mode-dependent quota (opt-in; default OFF preserves legacy per-question-type ratios + tests).
# When enabled, the cross-session budget ratio depends on context_source_mode:
#   local_and_remote (augment): the agent's local context already carries the current session,
#       so route the memory budget to cross-session + long-term profile (current-session quota
#       -> ~0; same-session refs are deduped against the local context in the packer).
#   remote_only: remote must also reconstruct the working context, so cross-session takes the
#       minority (current-session reconstruction fills the majority of the remote budget).
# Flip on with MATRIXARK_MODE_DEPENDENT_QUOTA=1 once the three-arm study validates it.
MODE_DEPENDENT_QUOTA_ENABLED = env_bool("MATRIXARK_MODE_DEPENDENT_QUOTA", False)
DEFAULT_AUGMENT_CROSS_SESSION_BUDGET_RATIO = float(os.environ.get("MATRIXARK_AUGMENT_CROSS_SESSION_BUDGET_RATIO", "0.60"))
DEFAULT_REMOTE_ONLY_CROSS_SESSION_BUDGET_RATIO = float(os.environ.get("MATRIXARK_REMOTE_ONLY_CROSS_SESSION_BUDGET_RATIO", "0.30"))


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
    os.environ.get("MATRIXARK_REMOTE_ONLY_LOCAL_FALLBACK_FLOOR_TOKENS", "2048")
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
CONTEXT_PACK_DEBUG_REFS = env_bool("MATRIXARK_CONTEXT_PACK_DEBUG_REFS", False)
AUDIT_DEBUG_PAYLOAD = env_bool("MATRIXARK_AUDIT_DEBUG_PAYLOAD", False)

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
# Near-duplicate suppression in ref selection. A candidate whose token set has a
# Jaccard similarity >= this ratio with an already-selected (higher-ranked) ref
# is dropped before packing, so the richer default packs (top_k 24, cross-session
# cands 200) do not spend budget on repetitive/near-identical refs. Jaccard (not
# containment) is used so distinct refs that merely share a common prefix are
# kept. 1.0 == only token-set-identical collapses; <= 0.0 disables entirely.
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
def live_int(variable: str, fallback: int) -> int:
    """The value in force NOW, not the one bound when this module was imported.

    A budget ceiling is a decision about the shape of the next pack, and the next pack is when an
    operator means it -- not the next restart. Every value it feeds is already resolved per request,
    so there is no cached state for a late change to contradict.

    Anything unparseable, or a nonsensical zero or negative, falls back to the constant: a ceiling
    that resolved to nothing would cut its section to zero, which is a worse failure than ignoring a
    bad value. That is the same choice `explicit_int` makes for the retrieval budgets.
    """
    raw = os.environ.get(variable)
    if raw is None:
        return fallback
    try:
        parsed = int(str(raw).strip())
    except (TypeError, ValueError):
        return fallback
    return parsed if parsed > 0 else fallback


DEFAULT_HOOK_MAX_CONTEXT_TOKENS = 128000  # build default; the resolver below reads the env


def hook_max_context_tokens() -> int:
    """The budget an AGENT HOOK asks for, which is not the one an API caller gets.

    `DEFAULT_MAX_CONTEXT_TOKENS` above is 500,000 and is what the packer, the schema documentation
    and the portal's budget panel all quote. The hooks never used it: they read
    `MATRIXARK_HOOK_MAX_CONTEXT_TOKENS` first, fall back to `MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS`,
    and fall back again to a number of their own -- and the installation manual sets the first one
    to 10,000. So on a deployment installed by the book the agent path runs a budget fifty times
    smaller than every surface reporting it.

    Both hooks carried this expression inline, which is why nothing could report it: a duplicated
    literal in two scripts is not something a panel can ask.
    """
    for variable in ("MATRIXARK_HOOK_MAX_CONTEXT_TOKENS", "MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS"):
        raw = os.environ.get(variable)
        if raw is None:
            continue
        try:
            parsed = int(str(raw).strip())
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            return parsed
    return DEFAULT_HOOK_MAX_CONTEXT_TOKENS


def live_float(variable: str, fallback: float) -> float:
    """A section's share of the budget, read per pack. The float counterpart of `live_int`.

    A value outside 0.0-1.0 is not a share of anything, so it falls back to the build default
    rather than clamping: a number the operator has to correct is better than one silently
    reinterpreted as something they did not ask for.

    Zero IS accepted, unlike `live_int` above. A ceiling of zero would cut its section to nothing
    by accident; a share of zero says "this section gets none of the pack", which is a decision an
    operator can mean and cannot express any other way.
    """
    raw = os.environ.get(variable)
    if raw is None:
        return fallback
    try:
        parsed = float(str(raw).strip())
    except (TypeError, ValueError):
        return fallback
    return parsed if 0.0 <= parsed <= 1.0 else fallback


DEFAULT_SHARED_RESOURCE_MAX_BUDGET_TOKENS = 262144  # build default; live_int reads the environment
DEFAULT_SHARED_SKILL_BUDGET_RATIO = 0.10  # build default; live_float reads the environment
DEFAULT_SHARED_SKILL_MAX_BUDGET_RATIO = 0.50  # the guard, not the setting: the share below is what decides
DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS = 262144  # build default; live_int reads the environment
DEFAULT_SHARED_CONTEXT_MIN_SCORE = float(os.environ.get("MATRIXARK_SHARED_CONTEXT_MIN_SCORE", "0.20"))

# Conditional follow-up query rewriting: rewrite the RETRIEVAL query (ranking only) from
# recent session turns so anaphora ("that"/"the ones") carries its referent terms. It does
# NOT change the pack fed to the model, so it adds ZERO model tokens. Ships OFF.
QUERY_REWRITE_ENABLED = env_bool("MATRIXARK_QUERY_REWRITE", False)
QUERY_REWRITE_WINDOW = int(os.environ.get("MATRIXARK_QUERY_REWRITE_WINDOW", "3"))

# Precision-expand: for exact-fact queries, expand matched segments/summaries to their source
# raw events (recovers exact hashes/numbers/commands that summaries drop). Ships OFF. Adds tokens
# (raw > summary), so intended for remote-only exact-fact queries where accuracy is the priority.
PACK_PRECISION_EXPAND_ENABLED = env_bool("MATRIXARK_PACK_PRECISION_EXPAND", False)
PACK_PRECISION_EXPAND_MAX_EVENTS = int(os.environ.get("MATRIXARK_PACK_PRECISION_EXPAND_MAX_EVENTS", "12"))
PACK_PRECISION_EXPAND_QUESTION_TYPES = {"fact", "multi_hop", "evidence", "benchmark_quality", "date"}

# Auto skill discovery on session commit (mine reusable tool-procedures -> skill records).
# Ships OFF; enable per-deployment. Runs only on final session boundaries.
SKILL_DISCOVERY_ENABLED = env_bool("MATRIXARK_SKILL_DISCOVERY", False)
SKILL_DISCOVERY_MIN_SUPPORT = int(os.environ.get("MATRIXARK_SKILL_DISCOVERY_MIN_SUPPORT", "2"))
SKILL_DISCOVERY_MAX_SKILLS = int(os.environ.get("MATRIXARK_SKILL_DISCOVERY_MAX_SKILLS", "8"))

TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE", "256"))
TIME_COMPRESSION_WINDOW_EVENTS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_WINDOW_EVENTS", "64"))
TIME_COMPRESSION_MIN_EVENTS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MIN_EVENTS", "8"))
TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH", "4"))
TIME_COMPRESSION_MIN_EVENT_AGE_MS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MIN_EVENT_AGE_MS", "0"))
TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS", str(30 * 24 * 60 * 60 * 1000)))
TIME_COMPRESSION_REINFORCEMENT_PROTECT_MS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_REINFORCEMENT_PROTECT_MS", str(30 * 24 * 60 * 60 * 1000)))

ENABLE_LLM_MERGE_OPERATOR = env_bool("MATRIXARK_ENABLE_LLM_MERGE_OPERATOR", False)
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
