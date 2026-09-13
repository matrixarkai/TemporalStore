#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The environment-variable surface only shrinks, and says what holds each part of it.

Production Python under `tools/` reads several hundred distinct `TS_*` / `MATRIXARK_*` /
`TEMPORALSTORE_*` variables. Nothing counted them, so nothing noticed the number growing, and
"there are too many flags" had no denominator to argue with.

This is a CEILING, not a floor: the count may fall freely and a rise fails. That is the opposite
of the vacuity floors next door, which exist to catch a scan that stopped matching and must
therefore be set from what they are FOR -- see the note in
`test_a_portal_bool_accepts_the_words_a_bool_is_written_with`. A ceiling can be pinned to a
measurement precisely because crossing it is the event worth failing on.

## The classification, which is the useful half

A bare count invites the wrong cut. Every flag here falls in one of these, and only the last is a
candidate for removal:

* **selected** -- the portal offers it, `matrixark_load_config.ENV_MAP` maps it, a test sets it,
  or a config file, script, deploy profile, workflow or document names it. Something can choose
  its value, so it is a switch.
* **instructed** -- a sentence in production prose tells a reader to set it: a portal help text, a
  docstring saying what a value does, an error message naming the words it accepts. Nothing in
  the repository SETS `MATRIXARK_RESOURCE_EVENT_TEXT_CHARS`, and its docstring says *"Set ...=0 to
  store the full text"*. Being told to set it is being able to set it.
* **a harness's own CLI** -- a benchmark, report generator or sweep reading its own
  `MATRIXARK_<TOOL>_*` namespace, or an `argparse` default. Nothing sets
  `MATRIXARK_BACKFILL_BENCH_RECORDS` because you set it when you RUN the benchmark.
* **deployment identity** -- an endpoint, credential variable, bucket, model, region, namespace or
  library path. *"Nothing sets it here"* is not the claim *"no deployment needs it"*, and writing
  one down hard-codes where a deployment points.
* **a legacy spelling** -- the second or third link of an alias chain, kept so an older
  configuration keeps working.
* **candidate** -- none of the above. A flag no one can be shown to set or be told to set is not
  a switch, it is a branch, and its live side can be made unconditional. That is the cut
  matrixarkai#1540 took 57 of.

The classifications are computed, not listed, so they cannot go stale -- and each is asserted to
hold a plausible share, because a rule matching nearly everything is measuring the population
rather than the property. Four of these were written too loosely first and did exactly that.
"""
from __future__ import annotations

import ast
import io
import os
import re
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)

_NAME = re.compile(r"(?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+")
_FLAG = re.compile(r"^(?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+$")
_READ = re.compile(
    r'os\.(?:environ\.get|getenv)\(\s*["\']((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)["\']'
    r'|os\.environ\[\s*["\']((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)["\']'
    r'|\b\w*[Ee][Nn][Vv]\w*\(\s*["\']((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)["\']')

#: Telling a reader to give a flag a value, within reach of the name itself. Matching anywhere in
#: a long string held 478 of 520 -- a rule that holds nearly everything is not a rule.
_INSTRUCTION = re.compile(
    r"\bset\b|\bmust be\b|\bexport\b|\benable[sd]?\b|\bdisable[sd]?\b|\bturn\b|\bto store\b"
    r"|\bapply\b|\bconfigurable\b|\boverride\b|\bdefaults? to\b|=", re.IGNORECASE)
_INSTRUCTION_REACH = 90

#: A module whose env reads ARE its command line rather than its configuration.
#: Where a deployment points, who it authenticates as, what it loads.
_IDENTITY = re.compile(
    r"(API_KEY|_KEY_ENV|BASE_URL|_URL$|_URI$|ENDPOINT|PROVIDER|_MODEL$|_MODEL_|BUCKET|PREFIX"
    r"|HOST|_PORT$|_PATH$|_DIR$|_DB$|_CLIENT_ID$|COMMAND|TOKEN|SECRET|CREDENTIAL|REGION"
    r"|ACCOUNT|TENANT|NAMESPACE|_ADDR$|METASERVER|_FILE$|_LOG$|_LIB$"
    # Found by reading the last twenty-nine candidates one at a time: nine of them were identity
    # this pattern did not know the spelling of. MATRIXARK_REPO falls back to
    # "/opt/github-services/TemporalStore" and MATRIXARK_WSL_MOUNT to "/mnt" -- writing either down
    # hard-codes one machine's layout. STORAGE_FAMILY, STORAGE_MODE and REPLICATION_MODE say what
    # the store IS on this deployment, which is the same kind of fact as where it lives.
    r"|_REPO$|_MOUNT$|_BIN$|_SCOPE$|_HTTP$|STORAGE_FAMILY|STORAGE_FAMILIES|STORAGE_MODE"
    r"|REPLICATION_MODE)")

#: The ceiling. Lower it when you cut; a rise is the failure this file exists for.
#: 520 when this was written, 484 now that matrixarkai#1540 has landed -- it folded 57 reads of
#: flags nothing sets, and 36 of those were the last read of their variable. Banked here in the
#: same breath, because a ratchet that does not bank a reduction is the reduction nobody can see
#: was made, and the check below refuses a ceiling left drifting above the truth.
MAXIMUM_FLAGS_READ = 536


#: Candidates that have been read one at a time, with what was found. **Not a skip list**: the
#: point of recording them is that `candidate` then counts the flags NOBODY HAS LOOKED AT, which is
#: the only number worth working down. A flag leaves this dict by being retired, not by being
#: forgotten.
#:
#: Writing this here is only safe because `_selected()` skips `_SELF`. Without that exclusion every
#: name below would classify itself as named-by-a-test, and the candidate count would fall by the
#: size of the register -- which is what happened on the first attempt, 94 to 70, for no reason at
#: all.
#:
#: What reading them one at a time actually settles: **none of these is a branch nothing selects.**
#: Each is a deadline, a bound or a coalescer parameter whose off-position or wider setting is the
#: thing an operator reaches for when a box is slow or a pack is wrong. Two looked inert to a scan
#: that asks which `if` tests the module constant and are not -- `LOCAL_JSONL_ENABLED` flows into
#: `self._local_jsonl_enabled` before anything branches, and `HOOK_TRACE_APPEND_TIMEOUT_MS` is
#: handed to `_run_best_effort_with_timeout` rather than tested. **A flag whose constant is never
#: the subject of an `if` is not thereby dead**, and a sweep that assumes otherwise will cut a
#: timeout.
EXAMINED = {
    "MATRIXARK_FEEDBACK_TIMEOUT_MS":
        "the MCP server's admission control: the per-tool deadlines and concurrency caps an operator turns when a deployment is overloaded. MATRIXARK_MAX_CONCURRENT_RETRIEVE even defaults to max(4, min(8, cpu_count)), which is a value that wants overriding on a box the formula guesses wrong about",
    "MATRIXARK_REPLAY_TIMEOUT_MS":
        "the MCP server's admission control: the per-tool deadlines and concurrency caps an operator turns when a deployment is overloaded. MATRIXARK_MAX_CONCURRENT_RETRIEVE even defaults to max(4, min(8, cpu_count)), which is a value that wants overriding on a box the formula guesses wrong about",
    "MATRIXARK_ADMIN_TIMEOUT_MS":
        "the MCP server's admission control: the per-tool deadlines and concurrency caps an operator turns when a deployment is overloaded. MATRIXARK_MAX_CONCURRENT_RETRIEVE even defaults to max(4, min(8, cpu_count)), which is a value that wants overriding on a box the formula guesses wrong about",
    "MATRIXARK_MAX_CONCURRENT_INGEST":
        "the MCP server's admission control: the per-tool deadlines and concurrency caps an operator turns when a deployment is overloaded. MATRIXARK_MAX_CONCURRENT_RETRIEVE even defaults to max(4, min(8, cpu_count)), which is a value that wants overriding on a box the formula guesses wrong about",
    "MATRIXARK_MAX_CONCURRENT_RETRIEVE":
        "the MCP server's admission control: the per-tool deadlines and concurrency caps an operator turns when a deployment is overloaded. MATRIXARK_MAX_CONCURRENT_RETRIEVE even defaults to max(4, min(8, cpu_count)), which is a value that wants overriding on a box the formula guesses wrong about",
    "MATRIXARK_MAX_CONCURRENT_FEEDBACK":
        "the MCP server's admission control: the per-tool deadlines and concurrency caps an operator turns when a deployment is overloaded. MATRIXARK_MAX_CONCURRENT_RETRIEVE even defaults to max(4, min(8, cpu_count)), which is a value that wants overriding on a box the formula guesses wrong about",
    "MATRIXARK_MAX_CONCURRENT_REPLAY":
        "the MCP server's admission control: the per-tool deadlines and concurrency caps an operator turns when a deployment is overloaded. MATRIXARK_MAX_CONCURRENT_RETRIEVE even defaults to max(4, min(8, cpu_count)), which is a value that wants overriding on a box the formula guesses wrong about",
    "MATRIXARK_MAX_CONCURRENT_ADMIN":
        "the MCP server's admission control: the per-tool deadlines and concurrency caps an operator turns when a deployment is overloaded. MATRIXARK_MAX_CONCURRENT_RETRIEVE even defaults to max(4, min(8, cpu_count)), which is a value that wants overriding on a box the formula guesses wrong about",
    "MATRIXARK_BACKPRESSURE_TIMEOUT_MS":
        "the MCP server's admission control: the per-tool deadlines and concurrency caps an operator turns when a deployment is overloaded. MATRIXARK_MAX_CONCURRENT_RETRIEVE even defaults to max(4, min(8, cpu_count)), which is a value that wants overriding on a box the formula guesses wrong about",
    "MATRIXARK_RETRIEVE_SHED_COOLDOWN_MS":
        "the MCP server's admission control: the per-tool deadlines and concurrency caps an operator turns when a deployment is overloaded. MATRIXARK_MAX_CONCURRENT_RETRIEVE even defaults to max(4, min(8, cpu_count)), which is a value that wants overriding on a box the formula guesses wrong about",
    "MATRIXARK_AUDIT_WORKERS":
        "the MCP server's admission control: the per-tool deadlines and concurrency caps an operator turns when a deployment is overloaded. MATRIXARK_MAX_CONCURRENT_RETRIEVE even defaults to max(4, min(8, cpu_count)), which is a value that wants overriding on a box the formula guesses wrong about",
    "MATRIXARK_CROSS_SESSION_CURRENT_STATE_BUDGET_RATIO":
        "cross-session retrieval tuning in matrixark_mcp_core: the ratios and minimums that decide how much of a pack comes from other sessions. Changing what a pack contains is the reason a deployment reaches for a knob at all",
    "MATRIXARK_CROSS_SESSION_MULTI_HOP_BUDGET_RATIO":
        "cross-session retrieval tuning in matrixark_mcp_core: the ratios and minimums that decide how much of a pack comes from other sessions. Changing what a pack contains is the reason a deployment reaches for a knob at all",
    "MATRIXARK_CROSS_SESSION_BROAD_BUDGET_RATIO":
        "cross-session retrieval tuning in matrixark_mcp_core: the ratios and minimums that decide how much of a pack comes from other sessions. Changing what a pack contains is the reason a deployment reaches for a knob at all",
    "MATRIXARK_CROSS_SESSION_MIN_ENTITY_BRIDGE_REFS":
        "cross-session retrieval tuning in matrixark_mcp_core: the ratios and minimums that decide how much of a pack comes from other sessions. Changing what a pack contains is the reason a deployment reaches for a knob at all",
    "MATRIXARK_CROSS_SESSION_RAW_EVIDENCE_MIN_SCORE":
        "cross-session retrieval tuning in matrixark_mcp_core: the ratios and minimums that decide how much of a pack comes from other sessions. Changing what a pack contains is the reason a deployment reaches for a knob at all",
    "MATRIXARK_CROSS_SESSION_PROFILE_MIN_ENTITY_BRIDGE_REFS":
        "cross-session retrieval tuning in matrixark_mcp_core: the ratios and minimums that decide how much of a pack comes from other sessions. Changing what a pack contains is the reason a deployment reaches for a knob at all",
    "MATRIXARK_CROSS_SESSION_PREFERRED_REF_TYPES":
        "cross-session retrieval tuning in matrixark_mcp_core: the ratios and minimums that decide how much of a pack comes from other sessions. Changing what a pack contains is the reason a deployment reaches for a knob at all",
    "MATRIXARK_HARD_MAX_CHILDREN_SCORED_PER_PARENT":
        "cross-session retrieval tuning in matrixark_mcp_core: the ratios and minimums that decide how much of a pack comes from other sessions. Changing what a pack contains is the reason a deployment reaches for a knob at all",
    "MATRIXARK_MAX_INDEX_TERMS_PER_RESOURCE_FACT":
        "an index-width bound; its neighbours in the same family are offered on the portal and this one bounds what they produce",
    "MATRIXARK_EMBEDDING_VECTOR_DECIMALS":
        "how many decimal places a stored vector keeps, which is a size-against-precision trade a deployment makes once and lives with",
    "MATRIXARK_ALLOW_PYTHON_RETRIEVAL_FALLBACK":
        "lets Python leave the native serving path; off is the default and on is what an operator reaches for when the native path refuses a request",
    "MATRIXARK_DIRECT_WRITE_QUEUE_ALLOW_SYNC_CONTEXT":
        "widens the direct-write queue to synchronous context writes, read inline at its branch",
    "MATRIXARK_DISABLE_NATIVE_CONTEXT_PACK":
        "the off switch for native packing, read inline at the branch it guards",
    "MATRIXARK_ENABLE_GENERIC_RESOURCE_FACTS":
        "gates a live branch in matrixark_mcp_core that emits generic resource facts",
    "MATRIXARK_FORCE_GENERIC_BATCH_HSET_FALLBACK":
        "a force switch for the generic batch path, which is what it is for: something to set when the specific path is failing and nothing in a repository would ever set it",
    "MATRIXARK_HOOK_TOOL_RESULT_RAW":
        "three live branches in the codex hook decide what a tool result carries",
    "MATRIXARK_HOOK_TOOL_RESULT_SERVING":
        "routes tool results to the serving scope",
    "MATRIXARK_HOOK_TOOL_RESULT_ROLLOUT_BACKFILL":
        "routes tool results into rollout backfill",
    "MATRIXARK_HOOK_TRACE_APPEND_TIMEOUT_MS":
        "handed to _run_best_effort_with_timeout rather than branched on: the deadline an operator raises when a slow box makes the hook give up on its trace append",
    "MATRIXARK_HOOK_RETRIEVE_TIMEOUT_MS":
        "the hook's retrieve deadline, the first thing to raise when retrieval is slow",
    "MATRIXARK_HOOK_TOOL_CALL_TIMEOUT_MS":
        "the hook's per-tool-call deadline, raised when a tool the hook shells out to is slow",
    "MATRIXARK_CODEX_HOOK_CAPTURE_RAW_PAYLOAD":
        "keeps the raw payload for diagnosis, which is a thing turned on while investigating",
    "MATRIXARK_LOCAL_JSONL_ENABLED":
        "default ON; the constant flows into self._local_jsonl_enabled and THAT is what branches, so a scan asking which if tests the constant reports it as inert and is wrong",
    "MATRIXARK_LOCAL_JSONL_INCLUDE_BULKY_FIELDS":
        "what the JSONL mirror keeps per record, which is the size-against-detail trade for it",
    "MATRIXARK_LOCAL_JSONL_RETENTION_AGE_MS":
        "how long the JSONL mirror keeps a record",
    "MATRIXARK_NATIVE_SIDE_INDEX_ASSUME_FRESH":
        "skips a freshness check on the native side index, read inline at its branch",
    "MATRIXARK_PRIOR_CONTEXT_PROBE_WINDOW":
        "a window size whose own docstring says 0 disables the probe entirely -- a described off position, which is a switch however it is spelled",
    "MATRIXARK_PRIOR_CONTEXT_EVENT_WINDOW":
        "the companion window for prior-context events",
    "MATRIXARK_REQUIRE_LLM_TIME_COMPRESSION":
        "six live branches in matrixark_mcp_core gate whether a model must produce the summary",
    "MATRIXARK_RUST_PROXY_STARTUP_WARMUP_FULL_SCAN":
        "default ON; the daemon reads it to decide whether the startup warmup scans everything",
    "MATRIXARK_RUST_PROXY_STARTUP_WARMUP_MAX_SELECTED_REFS":
        "how many refs the startup warmup selects, a bound on what a cold proxy pulls in",
    "MATRIXARK_RUST_PROXY_STARTUP_WARMUP_QUERY":
        "the query the startup warmup sends, which decides what a cold proxy pulls into cache",
    "MATRIXARK_RUST_PROXY_STARTUP_WARMUP_TIMEOUT_MS":
        "the startup warmup deadline, the thing to raise on a slow box",
    "MATRIXARK_SLIM_IDEMPOTENCY_RESPONSE":
        "default ON, off stores the full response; its docstring describes BOTH positions, which is the shape the instruction rule above now recognises",
    "MATRIXARK_SUMMARY_DIRTY_DEBUG_FIELDS":
        "four live branches; a debug field switch",
    "MATRIXARK_SUMMARY_REFRESH_AUDIT":
        "six live branches; the switch that decides whether a summary refresh is audited",
    "MATRIXARK_TEMPORALSTORE_ASYNC_CONTEXT_WARMUP":
        "read inline at the branch it guards, where the async context warmup is chosen",
    "MATRIXARK_TEMPORALSTORE_ASYNC_CONTEXT_WARMUP_FORCE":
        "the force half of the warmup pair, read inline",
    "MATRIXARK_CONTEXT_PACK_CACHE_MAX_ENTRIES":
        "how many packs the local adapter keeps; the bound a deployment lowers when memory is tight",
    "MATRIXARK_CONTEXT_PACK_CACHE_TTL_S":
        "how long a cached pack stays valid, which is the freshness-against-cost trade for it",
    "MATRIXARK_DIRECT_WRITE_QUEUE_MAX_RECORDS":
        "the direct-write queue's capacity; read once through direct_write_queue_limits so the bound has one home rather than two",
    "MATRIXARK_DIRECT_WRITE_QUEUE_PUT_TIMEOUT_MS":
        "how long a writer waits for room in that queue before giving up",
    "MATRIXARK_DIRECT_WRITE_QUEUE_DRAIN_MAX_BATCHES":
        "how many batches one drain pass takes, which bounds how long it holds the lane",
    "MATRIXARK_IDLE_DRAIN_MIN_INTERVAL_MS":
        "the floor between idle drains; raising it is what an operator does when the drain is competing with request work",
    "MATRIXARK_AUGMENT_CROSS_SESSION_BUDGET_RATIO":
        "the share of a pack an augmenting cross-session query may take",
    "MATRIXARK_REMOTE_ONLY_CROSS_SESSION_BUDGET_RATIO":
        "the same share for a remote-only deployment, which has a different cost per candidate",
    "MATRIXARK_PACK_PRECISION_EXPAND_MAX_EVENTS":
        "how many events a precision question may expand to, bounding the widest pack it can ask for",
    "MATRIXARK_QUERY_REWRITE_WINDOW":
        "how many recent turns the follow-up rewrite reads, so a question saying 'that' carries its subject; it does nothing unless the rewrite itself is on",
    "MATRIXARK_RESOURCE_MAX_CHUNK_CHARS":
        "the character ceiling on a resource chunk, computed from the token ceiling when unset",
    "MATRIXARK_RESOURCE_OVERLAP_CHARS":
        "how much two adjacent chunks share; another setting's portal help names this one as applying alongside it, so it is documented to an operator through its neighbour",
    "MATRIXARK_CONTEXT_INDEX_POSTINGS":
        "selects how context index postings are written; a mode with more than two positions, read as a lowercase word rather than a boolean",
    "MATRIXARK_PRE_RETRIEVAL_SUMMARY_REFRESH":
        "default OFF; turning it on refreshes summaries before a retrieve, which is the trade between a fresher pack and a slower one",
    "MATRIXARK_RESOURCE_STORAGE_POLICY":
        "which storage policy a resource takes when the request names none",
    "MATRIXARK_SKILL_RESERVED_REFS":
        "pack slots held for skills; it is a tenant knob and the v1 gateway surfaces its default, so a deployment sets it per tenant rather than per process",
    "MATRIXARK_BENCHMARK_REPLICATION_MODE":
        "the replication mode a benchmark run asks for; set when the benchmark is invoked, which is the harness rule wearing a name the harness pattern does not match",
    "MATRIXARK_REQUIRE_RETRIEVAL_MEMORY_COVERAGE":
        "a report gate: the workflow report reads it to decide whether missing coverage fails the run, and a gate exists to be turned on for a run",
}

def _readers_all_unreachable(reads):
    """Flags every reader of which sits in a module production cannot reach.

    A flag is only a control if something can turn it. These nineteen gate code no request arrives
    at, so turning them changes nothing that runs -- which makes them the one group on this page
    that could be retired without removing a capability from anybody.

    Not a removal list. Seventeen of the nineteen are in `matrixark_mcp_rust_proxy_config`, and
    `test_a_module_only_tests_reach_is_not_live` records that module as unwired rather than
    abandoned: the waiter fix for mx#1073 landed in it, so it is maintained code whose flags are
    its tuning surface. Retiring them is a decision about whether that path is coming back, and
    the point of naming the group is that the decision is now one decision rather than nineteen.

    Computed from the reachability guard next door rather than restated, so a module that becomes
    reachable takes its flags out of this group on the same day.
    """
    try:
        import test_a_module_only_tests_reach_is_not_live as reachability
    except Exception:  # pragma: no cover - the guard is absent
        return set()
    try:
        _library, reached = reachability.reachable_from_production()
    except Exception:  # pragma: no cover - a broken scan must not reclassify the page
        return set()
    if not reached:
        return set()
    out = set()
    for name, modules in reads.items():
        stems = {m[:-3] if m.endswith(".py") else m for m in modules}
        if stems and not (stems & reached):
            out.add(name)
    return out


#: Module prefixes and markers that make a file a TOOL rather than the product: a benchmark, a
#: conformance gate, a report builder, a sweep. It replaces a narrower rule that read the same thing and
#: caught 8 of these 77 -- not because it looked in the wrong place, but because `selected` and
#: `instructed` are tested first, and a benchmark's flags are named by tests and described in prose
#: like any others. That group went to zero once this one existed, and a group that has quietly
#: emptied is a classification that has stopped saying anything, so it is gone.
_TOOLING_PREFIXES = (
    "run_", "validate_", "generate_", "probe_", "summarize_", "compare_", "convert_",
    "check_", "resolve_", "download_", "analyze_", "mock_", "build_", "redis_",
)
_TOOLING_MARKERS = ("benchmark", "microbench", "_bench", "_harness", "_report", "sweep", "soak")


def _is_tooling(module):
    stem = module[:-3] if module.endswith(".py") else module
    return stem.startswith(_TOOLING_PREFIXES) or any(m in stem for m in _TOOLING_MARKERS)


#: The configurable surface's ceiling, and the only number on this page with a TARGET under it.
#:
#: 99, and it must stay under 100. That is not a round number chosen after the fact: the surface
#: was 103 when it was first measured, and it reached 99 by two changes that are recorded in the
#: commits -- two switches folded whose only off-state effect was to write a duplicate back, and a
#: rule of this file corrected to stop counting a container's environment block as configuration
#: when it already excluded a script's exports for the same reason.
#:
#: WHY A SEPARATE CEILING FROM MAXIMUM_FLAGS_READ. That one bounds what production Python reads,
#: which moves when a benchmark gains a knob. This one bounds what an OPERATOR is offered, and it
#: is the number that answers "how many knobs does this thing have". They move independently and a
#: single ceiling would hide one behind the other.
MAXIMUM_CONFIGURABLE = 128

#: Flags a deployment can set that decide whether a code path RUNS -- the number "how many features
#: can this thing be asked to turn off" is asking for, and the one the under-a-hundred target is
#: asserted of. 43 by the old scan; 50 once a flag named to a helper counts as read. The seven it
#: gained are real: MATRIXARK_REQUIRE_OSS_EMBEDDINGS decides whether a failed encoder raises or
#: silently writes hash vectors, and the six budget ceilings each decide whether a section of a
#: pack is cut at all.
#:
#: A CEILING ALONE DOES NOT PROTECT THIS ONE. Blinding the gating scan makes the number FALL, and a
#: ratchet that only bounds from above reads a fall as progress -- verified by mutation: stopping
#: `_flag_read_in` from following helpers took it to 43 and every test still passed. So the scan is
#: asserted as well as the count, in `test_the_helper_derivation_finds_the_readers_it_is_for`.
MAXIMUM_GATING_CONFIGURABLE = 54

#: Scan results that cost a tree walk, computed once per process.
_CACHE: dict = {}


def _engine_reads():
    """Flag names the RUST engine mentions, cached. The half of the product this file cannot see.

    Everything else on this page scans production PYTHON, which is the right scope for a count of
    what Python reads and the WRONG scope for the claim "no product module reads this". The engine
    is production too, and `TS_SERVER_WORKER_THREADS` is the proof: `server.rs` reads it,
    `config/temporalstore.toml` sets it, and the only Python that names it is an inventory builder
    -- so the tooling rule called a live server control a benchmark's command line.

    Four of the seventy-seven were wrong this way. A rule that reads one language and concludes
    about the product is measuring what it can see.
    """
    if "engine" not in _CACHE:
        names = set()
        for rel in _tracked("crates/*", "*.rs"):
            if rel.endswith(".rs"):
                names |= set(_NAME.findall(_text(rel)))
        _CACHE["engine"] = names
    return _CACHE["engine"]


def _read_only_by_tooling(reads):
    """Flags no PRODUCT module reads -- only a benchmark, a gate or a report builder.

    A flag nothing in the product reads cannot configure the product, whatever else names it. That
    is a sharper thing to know than "a test names it" or "its reader addresses an operator", both
    of which are true of most of these and neither of which tells you it is a tool's own command
    line spelled as an environment variable.

    Ordered ahead of `selected` for the same reason `readers all unreachable` is: those two rules
    describe how a flag is DOCUMENTED, and this one describes whether the product can see it at
    all.

    73 of 464 when this was written -- 21 in the context backfill benchmark alone, 17 in the
    dual-write ingestion benchmark, 15 in the locomo ingest harness. It was 77 until the Rust
    engine was consulted; see `_engine_reads` for the four it was wrong about.
    """
    engine = _engine_reads()
    out = set()
    for name, modules in reads.items():
        if name in engine:
            # The engine is production. See `_engine_reads`.
            continue
        if modules and all(_is_tooling(m) for m in modules):
            out.add(name)
    return out


#: `Setting` in matrixark_gateway_config has the same shape as `Knob` -- a constructor storing the
#: variable name on the instance, read back with `os.environ.get(setting.env)` -- but the portal
#: reading its own rows to show an operator the CURRENT value is not the product consulting a
#: control. `_SELF` below discounts this file's own mentions for the same reason.
_REGISTRIES_THAT_DISPLAY = frozenset(("Setting",))


def _tracked(*globs):
    return subprocess.run(["git", "ls-files", *globs], cwd=REPO,
                          capture_output=True, text=True).stdout.split()


def _text(rel):
    """Read a tracked file, once per process.

    Every rule on this page reads the tree and several read it per FLAG: this was called 11,200
    times in one run, against about three hundred distinct paths. The content cannot change while
    the process lives, so the reads after the first are pure cost -- and the cost was not academic.
    Adding the tooling, engine, settable, configurable and gating scans took this file from 21
    seconds to 68, and the ratchet job it runs in was cancelled twice at its 45-minute limit.
    """
    cached = _CACHE.setdefault("text", {})
    if rel not in cached:
        try:
            with io.open(os.path.join(REPO, rel), encoding="utf-8", errors="replace") as handle:
                cached[rel] = handle.read()
        except OSError:
            cached[rel] = ""
    return cached[rel]


def _tree(rel):
    """Parse a tracked file, once per process.

    `_text` above memoises the READ for the same reason this memoises the PARSE: the helper
    derivation, the read scan and the gating scan each walk every production module, and parsing
    three hundred files three times took this file from 20 seconds to 36. The five scans added
    before this one cost 21 seconds to 68 and had the ratchet job cancelled twice at its 45-minute
    limit, which is the whole reason the caching exists.
    """
    cached = _CACHE.setdefault("tree", {})
    if rel not in cached:
        try:
            cached[rel] = ast.parse(_text(rel))
        except SyntaxError:  # pragma: no cover - an unparseable module is not this file's problem
            cached[rel] = None
    return cached[rel]


def _environ_aliases(tree):
    """Local names bound to `os.environ` in this module.

    `GatewayConfig.from_env` opens with `env = os.environ` and every read below it is
    `env.get("MATRIXARK_RL_INGEST_RPS")`. Nothing in that line says `os.environ`, so a scan keyed on
    the spelling sees no read at all -- 14 rate-limit and timeout controls, plus
    MATRIXARK_REQUIRE_AUTH and MATRIXARK_AUTH_ENFORCED, were invisible for exactly that reason.

    The alias is taken per module and used for the whole module rather than per scope. That is
    deliberately loose: a name bound to `os.environ` anywhere and used as `name.get("MATRIXARK_X")`
    elsewhere is a read either way, and the only cost of the looseness is finding a read that a
    narrower scope rule would attribute to a different function -- which changes no count here.
    """
    aliases = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and ast.unparse(node.value) in ("os.environ", "environ"):
                aliases.add(target.id)
    return aliases


def _env_key_expression(node, aliases=()):
    """The expression this node uses as an environment-variable KEY, if it uses one."""
    if isinstance(node, ast.Call):
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr in ("get", "getenv") and node.args:
            source = ast.unparse(func.value)
            if "environ" in source or source == "os" or source in aliases:
                return node.args[0]
    if isinstance(node, ast.Subscript):
        source = ast.unparse(node.value)
        if source.endswith("environ") or source in aliases:
            return node.slice
    return None


def _env_key_helpers():
    """Functions that read the environment using a PARAMETER as the key, and the argument index.

    WHY THIS EXISTS. `_READ` and `_flag_of` above find a flag only where the NAME is written at the
    read: `os.environ.get("MATRIXARK_X")`. This tree mostly does not do that. It reads through
    helpers -- `live_int("MATRIXARK_X", 8)`, `_env_bool("MATRIXARK_X")`,
    `require_model_embeddings("MATRIXARK_X")` -- where the name is an argument and the read is one
    call away, or the read is written against a LOCAL ALIAS of os.environ (`_environ_aliases`).
    Every one of those was invisible, and the surface this file reports was 70 flags short, the
    configurable count 31 short and the gating count 15 short -- and the number the target is
    asserted of, flags a deployment can set that decide whether a path runs, was 43 when it is 54.

    The helpers are DERIVED, not listed. A list is the same defect one level up: correct the day it
    is written and silently wrong for the next helper somebody adds. This asks the code which
    functions read `os.environ[p]` for a parameter `p`, and finds 23 where a careful hand-list
    found 6.

    THE LOOP IS FOR ORDER, NOT FOR DEPTH, and it was measured rather than assumed. A helper that
    only reads through ANOTHER helper is found in the same pass whenever the inner one happens to
    be visited first, so on this tree one pass already finds all 23 and the second round adds
    nothing. The loop is still here because that is an accident of iteration order -- move a
    definition and a single pass starts missing it -- and the termination check costs one extra
    pass over an in-memory list. Do not read the loop as evidence that a second level exists
    today; it does not.

    REGISTRIES are the second shape. `Knob("recall_reinforcement", "bool",
    "MATRIXARK_RECALL_REINFORCEMENT", ...)` stores the name on the instance and
    matrixark_tenant_policy resolves it with `os.environ.get(knob.env)`, so the constructor call IS
    the read site. `Setting` in matrixark_gateway_config has the identical shape and is deliberately
    EXCLUDED: the portal reading its own rows to show an operator their current value is not the
    product consulting a control, and it is the same self-mention `_SELF` below discounts.
    """
    if "env_key_helpers" in _CACHE:
        return _CACHE["env_key_helpers"]
    trees = {rel: _tree(rel) for rel in _production_modules()}
    trees = {rel: tree for rel, tree in trees.items() if tree is not None}
    functions = []
    for tree in trees.values():
        aliases = _environ_aliases(tree)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                params = [a.arg for a in node.args.args] + [a.arg for a in node.args.kwonlyargs]
                functions.append((node.name, params, node, aliases))

    helpers = {}
    for _round in range(8):
        grew = False
        for name, params, node, aliases in functions:
            for sub in ast.walk(node):
                key = _env_key_expression(sub, aliases)
                if key is None and isinstance(sub, ast.Call):
                    callee = getattr(sub.func, "id", "") or getattr(sub.func, "attr", "")
                    index = helpers.get(callee)
                    if index is not None and len(sub.args) > index:
                        key = sub.args[index]
                if isinstance(key, ast.Name) and key.id in params:
                    index = params.index(key.id)
                    if helpers.get(name) != index:
                        helpers[name] = index
                        grew = True
        if not grew:
            break

    read_attributes = set()
    for tree in trees.values():
        aliases = _environ_aliases(tree)
        for node in ast.walk(tree):
            key = _env_key_expression(node, aliases)
            if isinstance(key, ast.Attribute):
                read_attributes.add(key.attr)
    registries = {}
    for tree in trees.values():
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef) or node.name in _REGISTRIES_THAT_DISPLAY:
                continue
            for member in node.body:
                if not (isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef))
                        and member.name == "__init__"):
                    continue
                params = [a.arg for a in member.args.args]
                for statement in ast.walk(member):
                    if not (isinstance(statement, ast.Assign) and len(statement.targets) == 1):
                        continue
                    target = statement.targets[0]
                    if (isinstance(target, ast.Attribute) and target.attr in read_attributes
                            and isinstance(statement.value, ast.Name)
                            and statement.value.id in params):
                        registries[node.name] = params.index(statement.value.id) - 1
    both = dict(helpers)
    both.update(registries)
    _CACHE["env_key_helpers"] = (helpers, registries, both)
    return _CACHE["env_key_helpers"]


def _flag_through_helper(call):
    """The flag a call reads by NAMING it to a helper, as against writing the read out."""
    if not isinstance(call, ast.Call):
        return None
    _helpers, _registries, both = _env_key_helpers()
    callee = getattr(call.func, "id", "") or getattr(call.func, "attr", "")
    index = both.get(callee)
    if index is None or len(call.args) <= index:
        return None
    argument = call.args[index]
    if not (isinstance(argument, ast.Constant) and isinstance(argument.value, str)):
        return None
    return argument.value if _FLAG.match(argument.value) else None


def _flag_of(call):
    if not isinstance(call, ast.Call):
        return None
    func = call.func
    if isinstance(func, ast.Attribute) and func.attr in ("get", "getenv"):
        source = ast.unparse(func.value)
        if "environ" not in source and source != "os":
            return None
    elif not (isinstance(func, ast.Name) and "env" in func.id.lower()):
        return None
    if not call.args or not isinstance(call.args[0], ast.Constant):
        return None
    name = call.args[0].value
    return name if isinstance(name, str) and _FLAG.match(name) else None


def _production_modules():
    """Python this product SHIPS, which is not the same as `tools/`.

    This read `tools/*.py` alone. sdk/python/temporalstore is the client library the product
    ships -- matrixark_http's native reader imports temporalstore.client to open a store -- and it
    reads three variables of its own: TS_REDIS_HOST, TS_REDIS_PORT and TS_TENANT. None was counted,
    so the surface this file reports was three short.

    The same mistake the engine scan corrected one language along: a rule that reads one directory
    and concludes about the product is measuring what it was pointed at. `examples/` stays out --
    a sample is not shipped behaviour.
    """
    modules = [rel for rel in _tracked("tools/*.py")
               if not os.path.basename(rel).startswith("test_")]
    modules += [rel for rel in _tracked("sdk/python/temporalstore/*.py")
                if not os.path.basename(rel).startswith("test_")]
    return modules


def read_by_production():
    """flag -> {module basenames that read it}."""
    found = {}
    for rel in _production_modules():
        base = os.path.basename(rel)
        for match in _READ.finditer(_text(rel)):
            name = match.group(1) or match.group(2) or match.group(3)
            found.setdefault(name, set()).add(base)
        # The reads the regex cannot see, because the NAME is an argument and the read is one call
        # away. See `_env_key_helpers`: 35 flags, every one of them a real control.
        tree = _tree(rel)
        if tree is None:
            continue
        aliases = _environ_aliases(tree)
        for node in ast.walk(tree):
            name = _flag_through_helper(node)
            if name:
                found.setdefault(name, set()).add(base)
                continue
            key = _env_key_expression(node, aliases)
            if isinstance(key, ast.Constant) and isinstance(key.value, str) \
                    and _FLAG.match(key.value):
                found.setdefault(key.value, set()).add(base)
    return found


#: This file, relative to the repository. It NAMES flags in its own prose -- it has to, because a
#: rule is unreadable without the example that made it -- and its own mention scan reads every
#: tracked `tools/test_*.py`. So the moment it was committed it credited
#: `MATRIXARK_BACKFILL_BENCH_RECORDS` and `MATRIXARK_RESOURCE_EVENT_TEXT_CHARS`, the two examples
#: below, with being selected by a test. Both were then classified `selected` for no reason except
#: that this file explains them.
#:
#: The fourth instance of a guard feeding on its own list in this tree, after mx#910,
#: `test_no_module_is_orphaned_quietly._SELF`, and the reachability guard that listed unreachable
#: modules and thereby reached them. It is worth stating as a rule rather than a fix: **a file that
#: decides about names must not count its own mention of them**, and the way that shows up is a
#: category getting quietly larger the better the prose gets.
#:
#: It also settles a thing that looked tempting: recording examined flags in a dict HERE, so the
#: candidate count becomes the number nobody has read. Every name written into that dict would
#: classify itself as selected. The record belongs somewhere this scan does not read.
_SELF = os.path.join("tools", os.path.basename(__file__))


#: What a DEPLOYMENT'S OWN ARTEFACTS carry: the portal's writable knobs, the config loader's map,
#: a config file, a container definition. Deliberately NOT `scripts/*` -- a repo script exporting a
#: variable before it launches something is the launcher SUPPLYING a value, not an operator
#: configuring one, and 61 flags are settable only that way.
def _portal_offers():
    """Flags the portal actually OFFERS: the `env` argument of each `Setting(...)`.

    Read out of the syntax rather than grepped, because matrixark_gateway_config READS flags of its
    own and names others in prose, and crediting those to the portal is the same mistake `_SELF`
    below exists to stop -- a file that decides about names counting its own mention of them.
    Grepping the file gives 116 configurable; parsing the calls gives 105, and the eleven in the
    difference are variables that file reads rather than knobs it offers.

    121 of the calls pass `env` positionally and none passes it by keyword, so reading args[2] sees
    every one of them. That is asserted below rather than assumed, because a single keyword call
    would be invisible here and would look exactly like a knob the portal does not offer.
    """
    if "portal_offers" in _CACHE:
        return _CACHE["portal_offers"]
    offers = _CACHE.setdefault("portal_offers", set())
    try:
        tree = ast.parse(_text("tools/matrixark_gateway_config.py"))
    except SyntaxError:  # pragma: no cover - a broken portal must not widen the surface
        return offers
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and getattr(node.func, "id", "") == "Setting"):
            continue
        if len(node.args) >= 3 and isinstance(node.args[2], ast.Constant):
            # Three settings carry env="": the registry's way of saying "dynamic", where `_env_name`
            # routes the value to whichever variable the selected provider reads. The empty string
            # is not a variable, and putting it in a set of variable names is a placeholder waiting
            # for the first caller who unions this set with anything.
            if isinstance(node.args[2].value, str) and node.args[2].value:
                offers.add(node.args[2].value)
    return offers


def _loader_maps():
    """Env vars `matrixark_load_config.ENV_MAP` maps a config key ONTO, read out of the syntax.

    Grepping that module credits it with MATRIXARK_CONFIG_FILE, which it READS to find the file --
    the bootstrap variable, not a mapped one. Same mistake as grepping the portal, and the same
    fix: read the mechanism, not the text around it.
    """
    if "loader_maps" in _CACHE:
        return _CACHE["loader_maps"]
    out = _CACHE.setdefault("loader_maps", set())
    try:
        tree = ast.parse(_text("tools/matrixark_load_config.py"))
    except SyntaxError:  # pragma: no cover
        return out
    for node in ast.walk(tree):
        if isinstance(node, ast.AnnAssign):
            target, value = node.target, node.value
        elif isinstance(node, ast.Assign) and len(node.targets) == 1:
            target, value = node.targets[0], node.value
        else:
            continue
        if getattr(target, "id", "") != "ENV_MAP" or not isinstance(value, ast.Dict):
            continue
        for item in value.values:
            if isinstance(item, ast.Constant) and isinstance(item.value, str):
                out.add(item.value)
    return out


#: Genuine controls whose ONLY surface is a container's environment block -- no portal Setting, no
#: ENV_MAP key. Named one at a time, because each is a documentation hole as much as a control: an
#: operator cannot discover it without reading a compose file.
#:
#: A container's `environment:` block is the LAUNCHER supplying a value, which is the same argument
#: that keeps `scripts/*` out of this number. Counting scripts as launchers and containers as
#: configuration was an inconsistency in this rule, and it was worth four flags:
#: MATRIXARK_RUST_SERVICE_PROXY_ADDR ("proxy:17100"), TEMPORALSTORE_HF_READER_HOST,
#: TEMPORALSTORE_HF_READER_PORT and TEMPORALSTORE_READER_MODEL -- a bind address, a host, a port
#: and a model name, which are where the process points and what it loads.
#:
#: Judged by READING them, not by `_IDENTITY`. That pattern is calibrated for its own group and
#: over-matches here: it calls MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS identity on `TOKEN` and
#: MATRIXARK_REQUIRE_MODEL_SUMMARIES identity on `_MODEL_`, and using it would drop seven working
#: knobs -- a smaller number bought with a miscategorisation.
CONTAINER_ONLY_CONTROLS = frozenset((
    "MATRIXARK_ACCESS_MODE",
))


def _default_argument_index():
    """For each reader helper, WHICH argument is the default -- taken from the signature.

    Not "the one after the name". `num(env_name, key, cast)` in matrixark_v1_gateway has a CONFIG
    KEY in that slot, and reading it as a default reported eight rate-limit settings as disagreeing
    with the portal when they agree exactly. The parameter is named, so ask the parameter.
    """
    if "default_index" in _CACHE:
        return _CACHE["default_index"]
    helpers, registries, _both = _env_key_helpers()
    out = {}
    for rel in _production_modules():
        tree = _tree(rel)
        if tree is None:
            continue
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in helpers:
                for position, argument in enumerate(node.args.args):
                    if argument.arg.lower().startswith(("default", "fallback")):
                        out[node.name] = position
                        break
            if isinstance(node, ast.ClassDef) and node.name in registries:
                for member in node.body:
                    if not (isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef))
                            and member.name == "__init__"):
                        continue
                    for position, argument in enumerate(member.args.args[1:]):
                        if argument.arg.lower().startswith("default"):
                            out[node.name] = position
                            break
    _CACHE["default_index"] = out
    return out


def _literal(node):
    """The value of a literal expression, or None. Negative numbers are literals too."""
    if isinstance(node, ast.Constant):
        return node.value if not isinstance(node.value, str) or node.value.strip() else None
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub) \
            and isinstance(node.operand, ast.Constant):
        return -node.operand.value
    return None


def portal_declared_defaults():
    """The default the portal SHOWS an operator, per variable. Blank ones are absent, not zero."""
    if "portal_defaults" in _CACHE:
        return _CACHE["portal_defaults"]
    out = {}
    tree = _tree("tools/matrixark_gateway_config.py")
    if tree is not None:
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and getattr(node.func, "id", "") == "Setting"
                    and len(node.args) >= 6):
                continue
            env, default = node.args[2], node.args[5]
            if isinstance(env, ast.Constant) and isinstance(default, ast.Constant) \
                    and isinstance(env.value, str) and env.value \
                    and str(default.value).strip() != "":
                out[env.value] = str(default.value)
    _CACHE["portal_defaults"] = out
    return out


def code_fallbacks():
    """What the PRODUCT falls back to when the variable is unset: variable -> {(value, module)}.

    Two shapes, because the tree writes defaults two ways: named to a helper,
    `live_int("MATRIXARK_TOP_K_PER_LAYER", 8)`, and written after an `or`,
    `int(os.environ.get("X", "").strip() or "8")`. Tooling is excluded -- a benchmark's own default
    is its business and it is not what the portal is describing.
    """
    if "code_fallbacks" in _CACHE:
        return _CACHE["code_fallbacks"]
    _helpers, _registries, both = _env_key_helpers()
    default_index = _default_argument_index()
    out = {}
    for rel in _production_modules():
        base = os.path.basename(rel)
        if _is_tooling(base):
            continue
        tree = _tree(rel)
        if tree is None:
            continue
        aliases = _environ_aliases(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                callee = getattr(node.func, "id", "") or getattr(node.func, "attr", "")
                key_index = both.get(callee)
                position = default_index.get(callee)
                if key_index is not None and position is not None \
                        and len(node.args) > max(key_index, position):
                    name = node.args[key_index]
                    if isinstance(name, ast.Constant) and isinstance(name.value, str) \
                            and _FLAG.match(name.value):
                        value = _literal(node.args[position])
                        if value is not None:
                            out.setdefault(name.value, set()).add((str(value), base))
                continue
            if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
                name = None
                for sub in ast.walk(node.value):
                    key = _env_key_expression(sub, aliases)
                    if isinstance(key, ast.Constant) and isinstance(key.value, str) \
                            and _FLAG.match(key.value):
                        name = key.value
                        break
                if name is None:
                    continue
                for sub in ast.walk(node.value):
                    if isinstance(sub, ast.BoolOp) and isinstance(sub.op, ast.Or):
                        tail = _literal(sub.values[-1])
                        if tail is not None:
                            out.setdefault(name, set()).add((str(tail), base))
    _CACHE["code_fallbacks"] = out
    return out


def _values_agree(declared, used):
    """Whether two defaults mean the same thing, across the type the portal renders them in."""
    declared, used = str(declared).strip().lower(), str(used).strip().lower()
    if declared == used:
        return True
    try:
        return float(declared) == float(used)
    except ValueError:
        pass
    truthy, falsy = {"1", "true", "yes", "on"}, {"0", "false", "no", "off"}
    return (declared in truthy and used in truthy) or (declared in falsy and used in falsy)


def deployment_configurable(reads):
    """The configurable surface: what the portal offers or the config loader maps.

    Every part of it is read from a MECHANISM rather than grepped for flag-shaped words: the `env`
    argument of each `Setting(...)`, the values of `ENV_MAP`, and the environment blocks of the
    containers that are not benchmarks. Grepping the same files gives 105, and the two in the
    difference are a variable the loader READS to find its file and a variable only a benchmark's
    compose file sets.

    `deployment_settable` above counts anything a shipping file writes, scripts included, and gets
    166. Reading those 61 script-only flags one at a time is what produced this narrower rule:
    almost all of them are identity and wiring -- MATRIXARK_ACCOUNT_ID, MATRIXARK_API_KEY,
    MATRIXARK_TENANT_ID, MATRIXARK_USER_ID, the metaserver, the namespace, the table, the prefix,
    the paths to the Rust CLI, proxy and hook roots. Writing any of them down hard-codes where one
    deployment points and who it authenticates as. They are not knobs anybody turns; they are how
    the process is told what it is.

    The portal half is read out of the SYNTAX -- the `env` argument of each `Setting(...)` -- not
    grepped, because that file reads flags of its own; see `_portal_offers`.

    22 of the 61 match `_IDENTITY` already. The other 39 are identity spellings that pattern does
    not know -- `_STORE_BASE`, `_HOOK_ROOT`, `_SOCKET`, MATRIXARK_HOME, MATRIXARK_TEAM, TS_ROOT --
    which is the same thing the note above `_IDENTITY` records happening once before. Widening it
    is a separate change: it emptied `legacy spelling` last time it was widened, and a group that
    empties for the wrong reason is how a classification stops saying anything.

    So there are three numbers on this page and they answer three questions:

        464   what production Python reads          has the surface grown?
        166   what any shipping file writes         what can be set at all?
        105   what a deployment's own artefacts     how much is there to CONFIGURE?
              carry
    """
    return {name for name in reads if name in (_portal_offers() | _loader_maps())} | CONTAINER_ONLY_CONTROLS


#: The files a DEPLOYMENT is configured from: what the portal offers, what the config loader maps,
#: and what a config file, a script, a deploy profile, a container definition or a CI workflow
#: actually writes. Deliberately NOT `docs/*` and NOT `tools/test_*.py` -- a doc telling an operator
#: to export something is an affordance (that is what `instructed` is for) and a test setting a
#: variable proves only that a test can set it.
_SETTING_GLOBS = ("config/*", "scripts/*", "*.sh", "tools/*.sh", "docker/*", ".github/*")


def _flag_read_in(node):
    """The flag an expression reads, if it reads one."""
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call):
            through = _flag_through_helper(sub)
            if through:
                return through
            func = sub.func
            named = ((isinstance(func, ast.Attribute) and func.attr in ("get", "getenv"))
                     or (isinstance(func, ast.Name) and "env" in func.id.lower()))
            if named and sub.args and isinstance(sub.args[0], ast.Constant):
                value = sub.args[0].value
                if isinstance(value, str) and _FLAG.match(value):
                    return value
        if isinstance(sub, ast.Subscript) and isinstance(sub.slice, ast.Constant):
            value = sub.slice.value
            if isinstance(value, str) and _FLAG.match(value):
                return value
    return None


def path_gating(reads):
    """Flags that GATE a code path, as against flags that supply a VALUE.

    A timeout, a limit, a budget and a model name are dials: production reads them and uses the
    number. A flag that is the subject of an `if` is a toggle -- it decides whether a path runs at
    all. Both are "flags"; only one keeps a live path alive, and a count that mixes them cannot say
    how many features this thing can be asked to turn off.

    176 of 464 when this was written: 45 a deployment can set, 17 only a benchmark reads, and 114
    gating a live path with nothing shipped able to set them -- a branch permanently on one side.
    Those 114 are not a cut list: 64 are flipped by a test, so both arms are exercised; 19 are
    deployment identity; 10 have production prose telling an operator to set them.

    THE SCAN IS CROSS-MODULE, and the first version was not. A flag is usually read into a constant
    in one module and branched on in another -- matrixark_mcp_runtime_config binds it,
    matrixark_mcp_core tests it -- so a per-module scan links neither to the other. Reading one
    module at a time gave 131 and reading them all gives 176: a third of them missed, silently,
    which is the failure a detector of this shape always has.
    """
    trees = {rel: _tree(rel) for rel in _production_modules()}
    trees = {rel: tree for rel, tree in trees.items() if tree is not None}
    bound = {}
    for tree in trees.values():
        for node in ast.walk(tree):
            target = None
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                target = getattr(node.targets[0], "id", None)
            elif isinstance(node, ast.AnnAssign):
                target = getattr(node.target, "id", None)
            if target and node.value is not None:
                flag = _flag_read_in(node.value)
                if flag:
                    bound.setdefault(target, set()).add(flag)
    gating = set()
    for tree in trees.values():
        for node in ast.walk(tree):
            tests = []
            if isinstance(node, (ast.If, ast.IfExp, ast.Assert, ast.While)):
                tests.append(node.test)
            elif isinstance(node, ast.comprehension):
                tests.extend(node.ifs)
            for test in tests:
                flag = _flag_read_in(test)
                if flag:
                    gating.add(flag)
                for sub in ast.walk(test):
                    key = getattr(sub, "id", None) or getattr(sub, "attr", None)
                    if key in bound:
                        gating |= bound[key]
    return {name for name in reads if name in gating}


def deployment_settable(reads):
    """The flags a deployment can actually set, which is not the same number as the surface.

    `selected` answers "can anything choose this value at all", and a test counts, because a flag
    a test sets is a flag with a live branch on both sides. That is the right question for
    "is this a switch or a dead branch" and the wrong one for "how much is there to configure":
    245 flags are selected and 183 of them are selected BY A TEST.

    So this asks the narrower thing. 166 of 464 when this was written -- 111 mapped by the portal
    or the config loader, 55 more written by a config file, script, deploy profile, container or
    CI workflow. The other 298 are read by production and set by nothing that ships: named in a
    test, described in prose, spelled on a benchmark's command line, or pointing at where a
    deployment lives.

    This is a report, not a group: a flag here is still classified by the rules below, and nothing
    is removed by counting it.
    """
    names = set(_NAME.findall(_text("tools/matrixark_gateway_config.py")))
    names |= set(_NAME.findall(_text("tools/matrixark_load_config.py")))
    for rel in _tracked(*_SETTING_GLOBS):
        if rel == _SELF:
            continue
        names |= set(_NAME.findall(_text(rel)))
    return {name for name in reads if name in names}


#: Where a flag counts as SELECTED: chosen by a test, a shipped config, a launcher, a workflow or a
#: document, plus the two modules that exist to offer and map settings. Written once because the
#: control below has to ask about the same set -- it used to list the globs and leave the two
#: modules out, so a flag selected because the PORTAL names it looked, to that control, like a flag
#: selected only because this file names it.
_SELECTION_SOURCES = ("tools/test_*.py", "config/*", "scripts/*", "*.sh", "tools/*.sh",
                      "docker/*", ".github/*", "docs/*")
_SELECTION_MODULES = ("tools/matrixark_gateway_config.py", "tools/matrixark_load_config.py")


def _selecting_files():
    """Every tracked file whose mention of a flag makes it `selected`, except this one."""
    return [rel for rel in _tracked(*_SELECTION_SOURCES) if rel != _SELF] + list(_SELECTION_MODULES)


def _selected():
    names = set()
    for rel in _selecting_files():
        names |= set(_NAME.findall(_text(rel)))
    return names


def _instructed():
    names = set()
    for rel in _production_modules():
        body = _text(rel)
        try:
            tree = ast.parse(body)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            prose = None
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                prose = node.value
            if not prose or len(prose) < 24:
                continue
            for name in set(_NAME.findall(prose)):
                at = prose.index(name)
                window = prose[max(0, at - _INSTRUCTION_REACH):at + len(name) + _INSTRUCTION_REACH]
                if _INSTRUCTION.search(window.replace(name, " ")):
                    names.add(name)
        # An argparse default reading a variable is that script's command line.
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "add_argument"):
                continue
            for keyword in node.keywords:
                if keyword.arg != "default":
                    continue
                for call in ast.walk(keyword.value):
                    flag = _flag_of(call)
                    if flag:
                        names.add(flag)
    return names


def _legacy_spellings():
    """Every flag that appears only as a LATER link of an alias chain."""
    first, later = set(), set()
    for rel in _production_modules():
        try:
            tree = ast.parse(_text(rel))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
                links = []
                for value in node.values:
                    found = [f for f in (_flag_of(c) for c in ast.walk(value)) if f]
                    links.append(found[0] if found else None)
                real = [n for n in links if n]
                if len(real) >= 2:
                    first.add(real[0])
                    later |= set(real[1:])
            name = _flag_of(node)
            if name and len(getattr(node, "args", [])) > 1:
                inner = [f for f in (_flag_of(c) for c in ast.walk(node.args[1])) if f]
                if inner:
                    first.add(name)
                    later |= set(inner)
    return later - first


def classify():
    """The whole page's classification, computed once per process.

    Seven checks call this and each used to redo every rule -- the read scan, the four name scans,
    the reachability walk and the engine walk. Nothing it reads can change while the process lives.
    """
    if "classify" in _CACHE:
        return _CACHE["classify"]
    reads = read_by_production()
    selected = _selected()
    instructed = _instructed()
    legacy = _legacy_spellings()
    tooling = _read_only_by_tooling(reads)
    out = {"tooling only": set(),
           "selected": set(), "instructed": set(),
           "deployment identity": set(), "legacy spelling": set(),
           "readers all unreachable": set(),
           "read one at a time": set(), "candidate": set()}
    unreachable = _readers_all_unreachable(reads)
    for name in reads:
        if name in unreachable:
            # First, because it is the sharpest thing true of these. A flag whose every reader is
            # unreachable is not held by a test naming it or by prose describing it -- nothing can
            # turn it at all.
            out["readers all unreachable"].add(name)
        elif name in tooling:
            # Second: the product cannot see these either, but a benchmark can, so they are a
            # tool's command line rather than a control nothing can reach.
            out["tooling only"].add(name)
        elif name in selected:
            out["selected"].add(name)
        elif name in instructed:
            out["instructed"].add(name)
        elif name in legacy:
            # Before the identity test on purpose. Widening _IDENTITY to know STORAGE_FAMILY and
            # REPLICATION_MODE emptied this group, because those names are BOTH identity and the
            # later link of an alias chain -- and "kept so an older configuration keeps working"
            # is the more specific thing to know about them. A group that quietly goes to zero is
            # a classification that has stopped saying anything.
            out["legacy spelling"].add(name)
        elif _IDENTITY.search(name):
            out["deployment identity"].add(name)
        elif name in EXAMINED:
            out["read one at a time"].add(name)
        else:
            out["candidate"].add(name)
    _CACHE["classify"] = (reads, out)
    return _CACHE["classify"]


class TheFlagSurfaceOnlyShrinksTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.reads, cls.groups = classify()

    def test_the_scan_reads_the_tree(self) -> None:
        """A floor on the SCAN, set from what it is for: a reader that stopped matching finds
        approximately nothing, and this whole file would then report a shrinking surface."""
        self.assertGreater(
            len(self.reads), 100,
            "only %d flags found in production python; the read scan has stopped matching, and a "
            "count that falls because the scan broke is the one failure this file cannot see"
            % len(self.reads))

    def test_the_surface_has_not_grown(self) -> None:
        self.assertLessEqual(
            len(self.reads), MAXIMUM_FLAGS_READ,
            "production Python now reads %d distinct environment variables, above the recorded "
            "%d. Either retire one, or raise the ceiling deliberately and say what the new flag "
            "is for -- the point of this number is that nobody could see it moving."
            % (len(self.reads), MAXIMUM_FLAGS_READ))

    def test_the_ceiling_is_not_far_above_the_truth(self) -> None:
        """A ceiling left far above the count stops being a ratchet without saying so."""
        self.assertGreaterEqual(
            len(self.reads), MAXIMUM_FLAGS_READ - 40,
            "the surface is %d and the ceiling is %d. Lower it: a ratchet that banks a reduction "
            "is what makes the next one visible." % (len(self.reads), MAXIMUM_FLAGS_READ))

    def test_every_group_holds_a_plausible_share(self) -> None:
        """The trap that made four earlier versions of these rules useless.

        A rule matching nearly everything is measuring the population, not the property -- and it
        makes the candidate list look empty when it is not. "A test names it" matched the ENV VAR
        and held 100; it means the portal KEY and holds 67. "The reader addresses an operator"
        matched any read and held 121; it means the comment above the read and holds 2.
        """
        total = len(self.reads)
        for name, names in self.groups.items():
            with self.subTest(group=name):
                self.assertLess(
                    len(names), total * 4 // 5,
                    "%r holds %d of %d flags. Check it is asking the question the tree asks "
                    "rather than a looser one." % (name, len(names), total))

    def test_every_flag_lands_in_exactly_one_group(self) -> None:
        counted = sum(len(v) for v in self.groups.values())
        self.assertEqual(
            len(self.reads), counted,
            "the groups hold %d of %d flags, so the classification is not a partition and the "
            "candidate count below cannot be read" % (counted, len(self.reads)))

    def test_this_file_does_not_credit_its_own_examples(self) -> None:
        """The rule a guard that lists names has to follow, checked rather than remembered.

        This file names flags in its prose because a rule is unreadable without the example that
        made it. Its own mention scan reads every tracked `tools/test_*.py`, so without the
        exclusion those examples classify themselves as selected -- which is how a category grows
        because somebody improved a comment.
        """
        own = set(_NAME.findall(_text(_SELF)))
        self.assertTrue(
            own, "this file names no flag at all, so either the prose lost its examples or the "
                 "scan stopped reading -- and the exclusion below is then hiding nothing")
        selected = _selected()
        named_elsewhere = set()
        for rel in _selecting_files():
            named_elsewhere |= set(_NAME.findall(_text(rel)))

        # ASKED THE OTHER WAY ROUND, because the first way could not fail without lying. It took
        # every flag this file names that IS selected and demanded something else name it -- but
        # `_selected` reads matrixark_gateway_config and matrixark_load_config too, and the check
        # did not, so a flag the PORTAL selects tripped it the moment this file mentioned it, with
        # a message saying the only thing naming it is this file while three modules named it.
        # Widening the check to the same set removes the false accusation and leaves the assertion
        # unable to fail at all: every selected flag is, by definition, named by a selecting file.
        #
        # The property wanted is the one the comment at the top of this file states -- that writing
        # a flag's name in this file's prose does not make it `selected`. So: a flag only THIS file
        # names must not be selected. 74 of the 99 this file names qualify, which is what makes it
        # worth asserting rather than assuming.
        only_here = sorted(set(own) - named_elsewhere)
        self.assertGreater(
            len(only_here), 20,
            "only %d of the flags this file names are named nowhere else that selects. Near zero "
            "means the mention scan or the selecting set has stopped matching, and the assertion "
            "below then holds over almost nothing." % len(only_here))
        leaked = sorted(set(only_here) & selected)
        self.assertEqual(
            [], leaked,
            "these flags are named only in this file's own prose and are classified selected "
            "anyway, so the register of what is documented where now describes this file rather "
            "than the tree: %s" % ", ".join(leaked))

    def test_every_examined_flag_says_what_was_found(self) -> None:
        """A recorded flag with no finding beside it is a skip list wearing a register's name."""
        thin = sorted(name for name, note in EXAMINED.items() if len(note.strip()) < 40)
        self.assertEqual([], thin, "recorded as read with nothing recorded: %s" % thin)

    def test_every_examined_flag_is_still_read(self) -> None:
        """One that stopped being read should leave rather than sit here describing nothing."""
        gone = sorted(name for name in EXAMINED if name not in self.reads)
        self.assertEqual(
            [], gone,
            "these are recorded as read one at a time, but production Python no longer reads "
            "them: %s" % gone)

    def test_the_register_does_not_classify_itself(self) -> None:
        """The trap this register walked into once, kept shut.

        Every name in EXAMINED is written in this file. If `_selected()` ever stops skipping
        `_SELF`, each would count as named-by-a-test and land in `selected` instead -- the register
        would shrink the candidate list by existing, which is the opposite of what it is for.
        """
        selected = _selected()
        leaked = sorted(name for name in EXAMINED if name in selected)
        for name in leaked:
            with self.subTest(flag=name):
                elsewhere = any(
                    name in _text(rel)
                    for rel in _tracked("tools/test_*.py", "config/*", "scripts/*", "*.sh",
                                        "tools/*.sh", "docker/*", ".github/*", "docs/*")
                    if rel != _SELF)
                self.assertTrue(
                    elsewhere,
                    "%s is in this register and reads as selected, and nothing but this file "
                    "names it -- the register is classifying itself." % name)

    def test_the_unreachable_group_is_computed_not_listed(self) -> None:
        """It comes from the reachability guard, so it cannot go stale on its own.

        A hand-written list here would keep naming flags after their module was wired up, and a
        group that describes a tree which has moved is worse than no group.
        """
        _reads, groups = classify()
        unreachable = groups["readers all unreachable"]
        import test_a_module_only_tests_reach_is_not_live as reachability
        library, reached = reachability.reachable_from_production()
        # The vacuity guard is on the SCAN, not on the group.
        #
        # It used to require the group to be non-empty, with the reasoning that an empty one is
        # what a broken reachability scan looks like. That was true when it was written and is
        # not any more: this group held nineteen flags, all of them in the unwired proxy config,
        # and folding those to the values they already produced emptied it -- which is the
        # outcome naming the group was FOR. An assertion that cannot tell "the scan broke" from
        # "we retired them all" fails on the success it was built to enable.
        #
        # So it asks the scan instead. A scan that has stopped matching reaches approximately
        # nothing, and that is the failure this file cannot otherwise see. The group is computed
        # rather than listed, so it refills the day a module stops being reachable.
        self.assertGreater(
            len(reached), len(library) // 2,
            "the reachability scan reaches %d of %d library modules. Below half, believe the scan "
            "is broken before believing the tree changed shape."
            % (len(reached), len(library)))
        for name in sorted(unreachable):
            with self.subTest(flag=name):
                stems = {m[:-3] if m.endswith(".py") else m for m in self.reads[name]}
                self.assertFalse(
                    stems & reached,
                    "%s is in the unreachable group and %s is reachable" % (name, stems & reached))

    def test_no_tooling_flag_is_read_by_the_engine(self) -> None:
        """The product is not only Python, and this page can only see Python.

        `TS_SERVER_WORKER_THREADS` is read by crates/temporalstore-rust/src/bin/server.rs and set
        in config/temporalstore.toml. The only PYTHON that names it is an inventory builder, so
        the tooling rule called a live server control a benchmark's command line. Four of the
        seventy-seven were wrong that way.
        """
        engine = _engine_reads()
        self.assertGreater(
            len(engine), 100,
            "only %d flag names found in the Rust tree. Below this the engine scan has stopped "
            "matching, and the exclusion it feeds silently stops excluding -- which puts live "
            "server controls back into the tooling group without anything saying so." % len(engine))
        _reads, groups = classify()
        for name in sorted(groups["tooling only"]):
            with self.subTest(flag=name):
                self.assertNotIn(
                    name, engine,
                    "%s is called tooling-only and the engine reads it" % name)

    def test_the_product_surface_is_reported_apart_from_the_tools(self) -> None:
        """The number a reduction target is about is the PRODUCT's, and it is not the total.

        A flag only a benchmark reads is that benchmark's command line spelled as an environment
        variable. Counting it alongside the controls a deployment sets makes the surface look
        larger than the thing anybody configures.
        """
        _reads, groups = classify()
        tooling = groups["tooling only"]
        self.assertTrue(
            tooling,
            "no flag is read only by tooling. That would be a surprise in a tree with this many "
            "benchmarks, and it is also what a broken module-role test says.")
        product = len(self.reads) - len(tooling)
        self.assertGreater(
            product, len(tooling),
            "more flags belong to tools than to the product (%d against %d), which would mean the "
            "role test has started matching product modules" % (len(tooling), product))
        for name in sorted(tooling):
            with self.subTest(flag=name):
                self.assertFalse(
                    [m for m in self.reads[name] if not _is_tooling(m)],
                    "%s is in the tooling group and a product module reads it" % name)

    def test_the_toggles_are_reported_apart_from_the_dials(self) -> None:
        """How many features can be asked to turn off, which is not how many flags there are.

        176 of the 464 are the subject of an `if`; the other 288 supply a value. Of the 99 a
        deployment can configure, 45 gate a path and 54 set a number.
        """
        gating = path_gating(self.reads)
        self.assertTrue(
            gating,
            "no flag gates a code path, which is not credible in this tree and is what a scan that "
            "has stopped matching `if` tests says")
        self.assertLess(
            len(gating), len(self.reads),
            "every flag read gates a path, so the rule is matching any mention rather than a test")
        # The cross-module half, checked by a NAMED case rather than by a count.
        #
        # A count cannot check this. The per-module version of this scan finds 138 of the 176 and
        # 35 of the 45 configurable ones -- enough to satisfy any floor loose enough to be stable,
        # which is how it passed the first guard written for it.
        #
        # MATRIXARK_ALLOW_LOCAL_BACKEND spans three modules and is the case this scan exists to
        # catch: matrixark_mcp_runtime_config binds it with `env_bool`, matrixark_mcp_core
        # re-exports the constant, and matrixark_mcp_backends tests it in
        # `validate_mcp_backend_policy`. Nothing reads the environment where the `if` is, so a scan
        # that does not follow the constant across files sees a permission with no toggle at all.
        self.assertIn(
            "MATRIXARK_ALLOW_LOCAL_BACKEND", gating,
            "the cross-module link from a constant to its `if` is broken: this flag is bound in "
            "matrixark_mcp_runtime_config and tested in matrixark_mcp_backends, three modules "
            "apart, and a per-module scan cannot see it")

    def test_the_configurable_surface_stays_under_a_hundred(self) -> None:
        """The ratchet on the number an operator's question is about.

        Every other check here reports; this one holds. A surface that is measured but not bounded
        drifts back, one Setting at a time, and each addition looks reasonable on its own -- which
        is how it reached 520 the first time anybody counted.

        THE UNDER-A-HUNDRED TARGET MOVED ONTO A DIFFERENT NUMBER, AND NOT BECAUSE IT WAS MISSED.
        It used to be asserted of `configurable`, which measured 97. Then `_env_key_helpers` and
        `_environ_aliases` taught the scan to follow a flag name into the helper that reads it and
        through a local alias of os.environ, and `configurable` turned out to be 128: it had never
        been under a hundred, it had been under-COUNTED by 31. Those controls were invisible
        because `live_int("MATRIXARK_X", 8)` writes the name as an argument and
        `env = os.environ; env.get("MATRIXARK_X")` never writes `os.environ` at the read.

        Under a hundred is now asserted of the number the phrase always described -- flags a
        deployment can set that decide whether a path RUNS, which is 54 -- and `configurable` keeps
        its own ceiling, which may only come down. Both are asserted, so nothing that was bounded
        before is unbounded now.

        The 128 are not reducible by the lever that produced the last cut: `configurable and every
        reader unreachable` is EMPTY, and so is `configurable and read only by tooling`. Every one
        of the twenty-eight above a hundred is a live control somebody would have to decide to
        remove, which is a product decision and not a tidy-up. Saying that is the point of leaving
        the number visible rather than quietly re-scoping it.
        """
        configurable = deployment_configurable(self.reads)
        self.assertLessEqual(
            len(configurable), MAXIMUM_CONFIGURABLE,
            "a deployment can now configure %d flags, above the recorded %d. Retire one, or raise "
            "the ceiling deliberately and say what the new knob is for."
            % (len(configurable), MAXIMUM_CONFIGURABLE))
        gating = path_gating(self.reads)
        keeping_a_path = configurable & gating
        self.assertLessEqual(
            len(keeping_a_path), MAXIMUM_GATING_CONFIGURABLE,
            "%d flags both can be set by a deployment and decide whether a path runs, above the "
            "recorded %d" % (len(keeping_a_path), MAXIMUM_GATING_CONFIGURABLE))
        self.assertLess(
            MAXIMUM_GATING_CONFIGURABLE, 100,
            "the ceiling itself has been raised to %d. Under a hundred is the target; moving the "
            "ceiling through it is not the same as meeting it." % MAXIMUM_CONFIGURABLE)
        self.assertGreaterEqual(
            len(configurable), MAXIMUM_CONFIGURABLE - 25,
            "the surface is %d and the ceiling is %d. Lower it: a ratchet that does not bank a "
            "reduction is the reduction nobody can see was made."
            % (len(configurable), MAXIMUM_CONFIGURABLE))

    def test_every_container_only_control_is_still_one(self) -> None:
        """Each name in CONTAINER_ONLY_CONTROLS must still have no other surface.

        The moment one gains a portal Setting or an ENV_MAP key it is configurable by the rule and
        the entry is dead weight -- and dead entries are how a hand-written list stops describing
        the tree. The check fails in both directions.
        """
        offered = _portal_offers() | _loader_maps()
        for name in sorted(CONTAINER_ONLY_CONTROLS):
            with self.subTest(flag=name):
                self.assertIn(name, self.reads,
                              "%s is recorded as a container-only control and production no "
                              "longer reads it" % name)
                self.assertNotIn(
                    name, offered,
                    "%s now has a portal Setting or an ENV_MAP key, so it is configurable by the "
                    "rule -- drop the entry rather than counting it twice" % name)

    def test_the_configurable_surface_is_the_narrowest_honest_number(self) -> None:
        """105 of 464, and it is a strict subset of the 166 rather than a different measurement.

        The 61 in the difference are settable only because a repo script exports them before
        launching something, and reading them one at a time is what made this rule: they are
        identity and wiring -- the account, the tenant, the API key, the metaserver, the namespace,
        the table, the path to the Rust CLI. Not knobs anybody turns.
        """
        # The vacuity guard is on the PARSE, not on the result. A `Setting` class that gets
        # renamed or wrapped makes `_portal_offers` return nothing, and the configurable number
        # then falls silently -- which is the one way this report can be wrong in the direction
        # that looks like progress. Asserting the SET is non-empty does not catch it: the loader
        # and the config files alone still leave 60-odd flags.
        mapped = _loader_maps()
        self.assertGreater(
            len(mapped), 60,
            "only %d ENV_MAP entries were parsed out of matrixark_load_config. Below this the dict "
            "has been renamed or built at runtime and the configurable surface is being "
            "under-reported -- the same failure as the portal parse below, one file along."
            % len(mapped))
        offers = _portal_offers()
        self.assertGreater(
            len(offers), 50,
            "only %d portal Settings were parsed out of matrixark_gateway_config. Below this the "
            "Setting call has been renamed or wrapped and the configurable surface is being "
            "under-reported, which is the failure that looks like a reduction." % len(offers))
        configurable = deployment_configurable(self.reads)
        settable = deployment_settable(self.reads)
        self.assertTrue(configurable, "nothing is configurable, which is what a broken scan says")
        self.assertTrue(
            configurable <= settable,
            "the configurable surface is not a subset of the settable one, so the two rules "
            "disagree about what a shipping file is: %s"
            % ", ".join(sorted(configurable - settable)))
        self.assertLess(
            len(configurable), len(settable),
            "every settable flag is also configurable, which would mean scripts/* has stopped "
            "being excluded and the narrower number has stopped being narrower")

    def test_the_deployment_settable_surface_is_reported(self) -> None:
        """The number an operator's question is about, which is not the number at the top.

        "How many knobs does this thing have" is answered by what a deployment can set, and that
        is 166 of the 464 read. `selected` deliberately counts a test as a chooser, because a test
        setting a flag proves the branch is live on both sides -- a good answer to "is this dead"
        and a misleading one to "how much is there to configure", since 183 of the 245 selected
        flags are selected by a test.
        """
        settable = deployment_settable(self.reads)
        self.assertTrue(
            settable,
            "no flag is set by the portal, the loader, a config file, a script, a profile, a "
            "container or a workflow. That is not credible in this tree and is what a broken "
            "tracked-file scan says, so check that before believing it.")
        self.assertLess(
            len(settable), len(self.reads),
            "every flag read is one a deployment sets, which would mean this is measuring the "
            "population rather than the property")
        # The half of the claim a count cannot make: each one really is named where it says.
        where = set(_NAME.findall(_text("tools/matrixark_gateway_config.py")))
        where |= set(_NAME.findall(_text("tools/matrixark_load_config.py")))
        for rel in _tracked(*_SETTING_GLOBS):
            if rel != _SELF:
                where |= set(_NAME.findall(_text(rel)))
        for name in sorted(settable):
            with self.subTest(flag=name):
                self.assertIn(name, where, "%s is counted settable and no shipping file names it"
                              % name)

    def test_every_portal_setting_names_its_variable_positionally(self) -> None:
        """`_portal_offers` reads the third positional argument, so a keyword `env=` is invisible.

        A setting the portal offers but this scan cannot see is counted as unconfigurable, which
        moves the number the goal is about in the flattering direction for no reason but a call
        style. The empty `env` is separately excluded there: three settings use it to mean "the
        variable depends on the provider selected", and the empty string is not a variable name.
        """
        tree = ast.parse(_text("tools/matrixark_gateway_config.py"))
        calls = [n for n in ast.walk(tree)
                 if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "Setting"]
        self.assertGreater(len(calls), 100,
                           "the portal registry stopped parsing as Setting(...) calls, which would "
                           "empty this scan silently")
        by_keyword = [n for n in calls if any(k.arg == "env" for k in n.keywords)]
        self.assertEqual(
            [], [n.lineno for n in by_keyword],
            "a Setting names its variable with a keyword, which _portal_offers cannot see")
        without = [n.lineno for n in calls if len(n.args) < 3]
        self.assertEqual([], without, "a Setting call has no third positional argument")
        dynamic = [n.lineno for n in calls
                   if isinstance(n.args[2], ast.Constant) and n.args[2].value == ""]
        self.assertEqual(
            3, len(dynamic),
            "the number of provider-routed settings changed (lines %s). That is fine, but it is "
            "the thing the empty-string exclusion was measured against" % (dynamic,))
        self.assertNotIn("", _portal_offers(),
                         "the empty env is back in the offered set")

    def test_the_helper_derivation_finds_the_readers_it_is_for(self) -> None:
        """The vacuity guard belongs on the DERIVATION, not on the flags it adds.

        A floor on "how many extra flags did following helpers find" fails the day somebody retires
        a helper, which is exactly the success it was meant to enable. So this asserts the scan
        instead: that the fixpoint still finds the reader functions, that it still reaches the ones
        a single pass cannot, and that the portal's own registry is still excluded.
        """
        helpers, registries, both = _env_key_helpers()
        self.assertGreater(
            len(helpers), 10,
            "the helper derivation found %d functions reading os.environ through a parameter. It "
            "found 23 when written, and near zero means the walk stopped matching -- which makes "
            "every count on this page silently smaller." % len(helpers))
        for name in ("live_int", "live_float", "_env_bool", "_env_int"):
            with self.subTest(helper=name):
                self.assertIn(name, helpers,
                              "%s reads os.environ with its first argument and the derivation no "
                              "longer sees it" % name)
        self.assertIn(
            "require_model_embeddings", helpers,
            "require_model_embeddings decides whether a failed encoder raises or silently writes "
            "hash vectors, and it takes the variable name as its argument. Losing it drops the "
            "flag that guards the store against 32-dimension data.")
        self.assertIn(
            "Knob", registries,
            "matrixark_tenant_policy resolves every Knob with os.environ.get(knob.env), so the "
            "constructor call is the read site. Losing it drops 21 tenant controls from the "
            "surface.")
        self.assertNotIn(
            "Setting", both,
            "the portal's own registry is counted. matrixark_gateway_config reads each row's "
            "variable to show an operator its CURRENT value, which is the self-mention _SELF "
            "discounts, and counting it adds every portal row to the surface a second time.")
        written_out = set()
        for rel in _production_modules():
            for match in _READ.finditer(_text(rel)):
                written_out.add(match.group(1) or match.group(2) or match.group(3))
        only_through_a_helper = set(self.reads) - written_out
        self.assertTrue(
            only_through_a_helper,
            "no flag in the surface is reached ONLY by naming it to a helper. That is the whole "
            "point of the derivation, and an empty answer means it contributes nothing -- which is "
            "what a broken fixpoint looks like from the outside.")
        # NAMED, not counted. Every count here is a ratchet bounded from ABOVE, so a scan that goes
        # blind produces a SMALLER number and reads as progress -- verified twice by mutation:
        # stopping `_flag_read_in` from following helpers took the gating count 54 -> 43, and
        # dropping the alias rule took the surface 535 -> 521, and both passed every test. A count
        # cannot catch that. A control that can only be seen through the mechanism can.
        self.assertIn(
            "MATRIXARK_REQUIRE_OSS_EMBEDDINGS", path_gating(self.reads),
            "this flag is read as `require_model_embeddings(\"MATRIXARK_REQUIRE_OSS_EMBEDDINGS\")` "
            "and nowhere else, and it decides whether a failed encoder RAISES or silently writes "
            "32-dimension hash vectors into the store. If it is not gating, `_flag_read_in` has "
            "stopped following a name into the helper that reads it.")
        self.assertIn(
            "MATRIXARK_RL_INGEST_RPS", self.reads,
            "this flag is read by `num(\"MATRIXARK_RL_INGEST_RPS\", ...)` inside "
            "GatewayConfig.from_env, whose body does `env = os.environ` and then `env.get(...)`. "
            "`num` is only a known reader BECAUSE of the alias rule, so losing this means "
            "`_environ_aliases` stopped working and fourteen rate-limit and timeout controls went "
            "back to invisible.")
        self.assertLessEqual(
            len(only_through_a_helper), len(self.reads) // 2,
            "%d of %d flags are reached only through a helper. More than half would mean the "
            "derivation has started matching functions that do not read the environment, which "
            "inflates every count on this page."
            % (len(only_through_a_helper), len(self.reads)))

    def test_the_portal_shows_the_default_the_code_actually_uses(self) -> None:
        """A default an operator reads on the page must be the one they get by setting nothing.

        The Rust half of this question is guarded -- test_engine_settings_offer_the_engine_default
        compares the portal to `storage_config.rs`, after mx#959 found budgets advertised 10x to
        156x larger than any deployment gets. The PYTHON half was not, and it is the same failure:
        the display and the code that consumes it each correct in isolation, with nothing comparing
        them.

        It is only askable now because the defaults are only VISIBLE now. Most are written as an
        argument -- `live_int("MATRIXARK_TOP_K_PER_LAYER", 8)` -- so a scan keyed on
        `os.environ.get("NAME", "8")` saw neither the flag nor its default.

        Clean when written: 47 comparable, zero disagreeing, and 13 of the 47 comparable ONLY
        because the default is read out of a helper argument. The rest of the portal's 105 declared
        defaults are engine variables whose fallback lives in Rust, which is the other guard's
        subject, or have no literal fallback in Python at all.

        Both scans are read from a MECHANISM. `num(env_name, key, cast)` has a config KEY where
        most helpers have a default, so the argument is found from the parameter's NAME rather than
        assumed to follow the variable -- assuming it reported eight rate-limit settings as
        disagreeing when they agree exactly.
        """
        declared = portal_declared_defaults()
        used = code_fallbacks()
        comparable = sorted(set(declared) & set(used))
        self.assertGreater(
            len(comparable), 30,
            "only %d portal settings could be compared against a code fallback. It was 47; near "
            "zero means one of the two scans stopped matching, and an agreement nobody can check "
            "is not an agreement." % len(comparable))
        # NAMED, because the count cannot protect this. Thirteen of the 47 are comparable ONLY
        # because the default is read out of a helper ARGUMENT, and a scan that loses that path
        # reports a smaller comparable set with no disagreements -- which looks like success.
        self.assertIn(
            "MATRIXARK_HOOK_FAIL_OPEN", comparable,
            "read as `_env_bool(\"MATRIXARK_HOOK_FAIL_OPEN\", True)` and nowhere written out. It "
            "decides whether a failing hook blocks the turn, so the portal showing a default it "
            "does not have is the difference between a hook that fails open and one that does not.")
        # The registry half is asserted by MECHANISM rather than by naming one of its variables.
        # Naming one here would make this file the only TEST that names it, which classifies it
        # `selected` on the strength of this file alone -- the self-feeding
        # `test_the_scan_does_not_feed_on_this_file` exists to catch, and it caught me doing it.
        _helpers, registries, both = _env_key_helpers()
        positions = _default_argument_index()
        from_a_registry = set()
        for rel in _production_modules():
            tree = _tree(rel)
            if tree is None:
                continue
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                callee = getattr(node.func, "id", "") or getattr(node.func, "attr", "")
                if callee not in registries:
                    continue
                key, position = both.get(callee), positions.get(callee)
                if key is None or position is None or len(node.args) <= max(key, position):
                    continue
                argument = node.args[key]
                if isinstance(argument, ast.Constant) and isinstance(argument.value, str)                         and _FLAG.match(argument.value)                         and _literal(node.args[position]) is not None:
                    from_a_registry.add(argument.value)
        self.assertTrue(
            from_a_registry & set(comparable),
            "no comparable default comes from a registry constructor. A Knob declares its variable "
            "and its default as constructor arguments and the value is resolved later with "
            "os.environ.get(knob.env), so losing that path drops the whole tenant-policy family "
            "out of this comparison while leaving it looking clean.")
        disagreeing = ["%s: the portal shows %s, %s falls back to %s" % (name, declared[name],
                                                                        module, value)
                       for name in comparable for value, module in sorted(used[name])
                       if not _values_agree(declared[name], value)]
        self.assertEqual([], disagreeing, "; ".join(disagreeing))

    def test_the_candidates_are_reported(self) -> None:
        """Not an assertion about how many: a record of what is left, printed where it is read.

        Every candidate is a flag no one can be shown to set and no sentence tells anyone to set.
        Cutting one still needs the suites to be run -- `test_matrixark_knobs_apply_live` refused
        two by name for being wired to what gets stored, which no rule here can see.
        """
        candidates = sorted(self.groups["candidate"])
        self.assertIsInstance(candidates, list)
        if candidates:
            print("\n  %d flags nothing selects and no sentence instructs:" % len(candidates))
            for name in candidates[:40]:
                print("     %-56s %s" % (name, ", ".join(sorted(self.reads[name]))[:60]))


if __name__ == "__main__":
    unittest.main()
