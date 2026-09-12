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
MAXIMUM_FLAGS_READ = 464


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


def _read_only_by_tooling(reads):
    """Flags no PRODUCT module reads -- only a benchmark, a gate or a report builder.

    A flag nothing in the product reads cannot configure the product, whatever else names it. That
    is a sharper thing to know than "a test names it" or "its reader addresses an operator", both
    of which are true of most of these and neither of which tells you it is a tool's own command
    line spelled as an environment variable.

    Ordered ahead of `selected` for the same reason `readers all unreachable` is: those two rules
    describe how a flag is DOCUMENTED, and this one describes whether the product can see it at
    all.

    77 of 464 when this was written -- 21 in the context backfill benchmark alone, 17 in the
    dual-write ingestion benchmark, 15 in the locomo ingest harness.
    """
    out = set()
    for name, modules in reads.items():
        if modules and all(_is_tooling(m) for m in modules):
            out.add(name)
    return out


def _tracked(*globs):
    return subprocess.run(["git", "ls-files", *globs], cwd=REPO,
                          capture_output=True, text=True).stdout.split()


def _text(rel):
    try:
        with io.open(os.path.join(REPO, rel), encoding="utf-8", errors="replace") as handle:
            return handle.read()
    except OSError:
        return ""


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
    return [rel for rel in _tracked("tools/*.py")
            if not os.path.basename(rel).startswith("test_")]


def read_by_production():
    """flag -> {module basenames that read it}."""
    found = {}
    for rel in _production_modules():
        base = os.path.basename(rel)
        for match in _READ.finditer(_text(rel)):
            name = match.group(1) or match.group(2) or match.group(3)
            found.setdefault(name, set()).add(base)
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


def _selected():
    names = set()
    for rel in _tracked("tools/test_*.py", "config/*", "scripts/*", "*.sh", "tools/*.sh",
                        "docker/*", ".github/*", "docs/*"):
        if rel == _SELF:
            continue
        names |= set(_NAME.findall(_text(rel)))
    names |= set(_NAME.findall(_text("tools/matrixark_gateway_config.py")))
    names |= set(_NAME.findall(_text("tools/matrixark_load_config.py")))
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
    return reads, out


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
        leaked = sorted(own & selected & set(self.reads))
        for name in leaked:
            with self.subTest(flag=name):
                elsewhere = any(
                    name in _text(rel)
                    for rel in _tracked("tools/test_*.py", "config/*", "scripts/*", "*.sh",
                                        "tools/*.sh", "docker/*", ".github/*", "docs/*")
                    if rel != _SELF)
                self.assertTrue(
                    elsewhere,
                    "%s is classified as selected and the only thing naming it is this file. "
                    "The exclusion is not working." % name)

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
