#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Per-tenant memory policy: what each tenant stores and how big its index may get.

Until now every memory knob was a process-wide environment variable, which is wrong for a
multi-tenant store: one tenant wanting segments, or a bigger index budget, forced that choice on
everyone sharing the process. This module resolves each knob **per tenant**, in this order:

    tenant override  ->  environment variable  ->  built-in default

so an unconfigured deployment behaves exactly as it did before, and a tenant that needs something
different says so without touching anyone else.

## Where overrides come from

1. A JSON policy file at ``MATRIXARK_TENANT_POLICY_PATH``::

       {
         "defaults":  {"extract_segments": false, "max_secondary_index_records_per_scope": 256},
         "tenants": {
           "acme":      {"extract_segments": true, "max_secondary_index_records_per_scope": 4096},
           "starter-co": {"compact_index_on_summary": true, "max_secondary_index_records_per_scope": 64}
         }
       }

   Re-read automatically when the file's mtime changes, so policy edits do not need a restart.

2. ``matrixark_tenant_policy`` records in the store itself (durable, survives restart, and lets a
   control-plane API set policy at runtime): ``{"record_type": "matrixark_tenant_policy",
   "tenant_id": "acme", "policy": {...}}``. Registered through :func:`register_tenant_policy_records`.
   A record overrides the file for that tenant, since it is the more recent statement of intent.

## Tenant identity

Any of these resolves: a scope dict (``tenant_id`` preferred, else ``tenant_hash``), a ``scope_key``
string (``t=<hash>;u=<hash>``), or a bare tenant id. A scope with no resolvable tenant falls back to
the global answer -- never to another tenant's policy.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from typing import Any

Json = dict[str, Any]

LOGGER = logging.getLogger("matrixark.tenant_policy")

TENANT_POLICY_RECORD_TYPE = "matrixark_tenant_policy"
USER_POLICY_RECORD_TYPE = "matrixark_user_policy"


class Knob:
    """One tunable: its type, its env var, and the value used when nobody configured it.

    ``aliases`` / ``env_aliases`` keep an earlier name working after a rename, so an existing
    deployment's config file or environment is not silently ignored."""

    __slots__ = ("name", "kind", "env", "default", "description", "aliases", "env_aliases",
                 "choices", "layer")

    def __init__(self, name: str, kind: str, env: str, default: Any, description: str,
                 aliases: tuple = (), env_aliases: tuple = (), choices: tuple = (),
                 layer: str = "write") -> None:
        # Which layer is allowed to set this. See READ_PATH_KNOBS below for what "read" means and
        # why the default is the restrictive one.
        if layer not in ("read", "write"):
            raise ValueError(f"knob layer must be read or write, got {layer!r}")
        self.layer = layer
        self.choices = frozenset(choices)
        self.name = name
        self.kind = kind
        self.env = env
        self.default = default
        self.description = description
        self.aliases = aliases
        self.env_aliases = env_aliases

    def coerce(self, value: Any) -> Any:
        if self.kind == "choice":
            text = str(value).strip().lower()
            if text not in self.choices:
                raise ValueError(f"{self.name} must be one of {sorted(self.choices)}")
            return text
        if self.kind == "bool":
            if isinstance(value, bool):
                return value
            return str(value).strip().lower() not in {"0", "false", "no", "off", ""}
        if self.kind == "int":
            return max(0, int(str(value).strip()))
        return value


def _registry(*knobs: Knob) -> dict[str, Knob]:
    """The knob registry, refusing a name defined twice.

    This was a dict comprehension, which keeps whichever definition came last. Three knobs were
    defined twice and their earlier definitions were dead -- a different default, sitting in the
    file, that nothing read. Raising here turns that into a startup failure instead of a number
    nobody can trust.
    """
    out: dict[str, Knob] = {}
    for knob in knobs:
        if knob.name in out:
            raise ValueError(
                f"knob {knob.name!r} is defined twice; the later definition would silently win"
            )
        out[knob.name] = knob
    return out


KNOBS: dict[str, Knob] = _registry(
        Knob("extract_segments", "bool", "MATRIXARK_EXTRACT_SEGMENTS", False,
             "Materialize context_segment rows. Off by default: a segment restates its event."),
        Knob("compact_index_on_summary", "bool", "MATRIXARK_INDEX_COMPACT_ON_SUMMARY", True,
             "Drop an event's per-event index postings once a summary covers it (lossless)."),
        Knob("max_secondary_index_records_per_session", "int",
             "MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_SESSION", 128,
             "Per-session posting budget; 0 = unlimited. Measured with dedup on: 128 holds recall "
             "5/5 (177 postings at 40 turns), 96 and below lose a fact.",
             aliases=("max_secondary_index_records_per_scope",),
             env_aliases=("MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_SCOPE",)),
        Knob("max_secondary_index_records_per_tenant", "int",
             "MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_TENANT", 1024,
             "Per-tenant posting budget across all that tenant's sessions; 0 = unlimited. There is "
             "no store-wide total on purpose: a global budget would let one tenant evict another.",
             aliases=("secondary_index_hard_ceiling",),
             env_aliases=("MATRIXARK_SECONDARY_INDEX_HARD_CEILING",)),
        Knob("summary_levels", "choice", "MATRIXARK_SUMMARY_LEVELS", "auto",
             "Which node summaries to generate, explicitly: 'auto' keeps today's content-driven "
             "rule (L0 always, L1 when the node has child summaries / >=3 events / >=180 tokens), "
             "'l0' never generates L1, 'l0_l1' always generates both regardless of the rule, 'none' "
             "generates neither. NOTE 'none' also disables index compaction, which only fires once "
             "an event has been rolled up -- so per-event postings then live forever.",
             choices=("auto", "l0", "l0_l1", "none")),
        Knob("audit_payload_retain_per_scope", "int", "MATRIXARK_AUDIT_PAYLOAD_RETAIN_PER_SCOPE", 20,
             "How many audit / batch-commit records per scope keep their diagnostic payload "
             "(outputs, schema, profile_promotion_summary, memory_layers_written, summary_refresh, "
             "trigger_evidence). Older rows keep identity, scope, status and timing -- which is all "
             "the ingest and retrieval paths read -- and are marked payload_slimmed. 0 = keep every "
             "payload forever. Measured: these payloads are ~11% of resident memory."),
        Knob("summarize_aggregation_only_nodes", "bool", "MATRIXARK_SUMMARIZE_AGGREGATION_ONLY_NODES", True,
             "Summarize nodes that hold no events of their own (tenant / user / profile). ON by "
             "default, i.e. the skip is opt-in, because the first framing of this lever was wrong: "
             "the argument was that the traversal visits these nodes unconditionally, so a summary "
             "there cannot help it prune. True, but retrieval does not only use summaries to prune "
             "-- it INJECTS them as their own memory layer, and the profile node's summary is what "
             "carries cross-session memory into a fresh session pack. Worse, node_l1 is only ever "
             "generated where child summaries exist, which is exactly these nodes, so skipping them "
             "removed every L1 in the store (caught by three profile/L1 tests). Turn it off per "
             "tenant to trade that layer for the memory: measured ~5 summaries per 3 sessions."),
        Knob("pack_drop_redundant_items", "bool", "MATRIXARK_PACK_DROP_REDUNDANT_ITEMS", True,
             "Drop a pack item whose content another item in the SAME pack already carries. An "
             "entity is a projection of the event it was extracted from, so a pack routinely ships "
             "both -- 'user: I live in Kyoto and my favorite drink is matcha.' alongside "
             "'preference: preference = drink is matcha'. The reader pays tokens twice for one "
             "fact. The longer item is kept, because it is the one that carries the surrounding "
             "context; only the projection is dropped, and only when its value is literally "
             "contained in the kept item."),
        Knob("recall_reinforcement", "bool", "MATRIXARK_RECALL_REINFORCEMENT", True,
             "Write a protection marker for every ref a retrieval selected, so recently recalled "
             "memory survives raw-event pruning. It is genuinely useful -- recall is the signal "
             "that something matters -- but it makes RETRIEVAL a writer: measured, 5 searches "
             "produced 572 marker records, about 114 writes per query, and that write "
             "amplification shows up in both search latency and storage (124.9 KB/message). A "
             "tenant that prunes rarely, or values read latency over pruning accuracy, can turn it "
             "off; a request can still override per call via ranking.recall_reinforcement."),
        Knob("skill_chunks_per_skill", "int", "MATRIXARK_SKILL_CHUNKS_PER_SKILL", 3,
             "Sections returned from ONE skill, chosen by how well each section matches the query "
             "rather than by document order. A skill is chunked one section per heading, so a "
             "40-chapter handbook is 41 independently embedded sections: returning all of them put "
             "1385 tokens of operations procedure into a pack asking about a favourite drink. "
             "Returning the best 3 costs about 60 tokens and is more accurate, because the three "
             "relevant chapters beat forty of which thirty-seven are noise. 0 returns every "
             "section (the old behaviour)."),
        Knob("skill_description_always", "bool", "MATRIXARK_SKILL_DESCRIPTION_ALWAYS", True,
             "Include each visible skill's name + description in every pack, even when no section "
             "matches. This is what makes many skills affordable: a description is ~5-30 tokens, "
             "so the model learns a skill EXISTS for a few tokens and its content is fetched only "
             "when a chunk actually matches. Progressive disclosure, the same shape agent runtimes "
             "use for their own skill listings."),
        Knob("skill_reserved_refs", "int", "MATRIXARK_SKILL_RESERVED_REFS", 3,
             "Pack slots held for SKILLS, admitted by policy rather than by similarity score. A "
             "skill is instruction, not recollection: an incident runbook does not read like the "
             "question 'how do I handle an incident', so on score alone it loses to any event that "
             "happens to share wording -- measured, no skill reached a pack at all while both "
             "session and profile memory did. Reserving slots is the same shape as guaranteeing "
             "the current session: the thing that must be present is admitted, and ranking decides "
             "the rest. 0 disables the reservation and returns skills to competing on score."),
        Knob("traverse_sibling_sessions", "bool", "MATRIXARK_TRAVERSE_SIBLING_SESSIONS", True,
             "Descend into other sessions' subtrees during retrieval. A pack is built from the "
             "CURRENT session (matched by session_id, and admitted unconditionally) plus the "
             "durable PROFILE, which is the aggregate that carries cross-session memory. Measured "
             "over a 12-session store, every pack item came from one of those two and none from a "
             "sibling session's events -- so scoring the siblings is a cosine per summary per query "
             "whose result is discarded, and it grows with every conversation the user has ever "
             "had. Left ON by default because turning it off changes what CAN be reached, not just "
             "what costs; verify recall on a real corpus before switching it off for a tenant."),
        Knob("top_k_per_layer", "int", "MATRIXARK_TOP_K_PER_LAYER", 240,
             "How many child nodes each PARENT contributes to the next traversal frontier (the "
             "name says layer, the code applies it per parent). Raised 24 -> 240: at 24 a user "
             "with more sessions than that had the surplus discarded by node-summary score, and "
             "the session most likely to lose is the newest one. The current session no longer "
             "depends on this -- it is admitted unconditionally -- so this now governs only how "
             "much of the cross-session history is explored."),
        Knob("max_candidates_per_node", "int", "MATRIXARK_MAX_CANDIDATES_PER_NODE", 10240,
             "Candidate refs (events + entities) admitted from a single node. Never applied to "
             "child nodes -- a session holding 40 events returns them all; this is the ceiling on "
             "how deep one node can go."),
        Knob("max_global_candidates", "int", "MATRIXARK_MAX_GLOBAL_CANDIDATES", 20480,
             "Total candidates gathered across the whole traversal before scoring."),
        Knob("max_selected_refs", "int", "MATRIXARK_MAX_SELECTED_REFS", 10000,
             "Refs allowed into the pack after scoring. The token budget is the limit that "
             "actually binds in practice; this is the backstop."),
        Knob("cross_session_profile_max_candidates", "int",
             "MATRIXARK_CROSS_SESSION_PROFILE_MAX_CANDIDATES", 2000,
             "Candidates drawn from the durable profile -- the aggregation that carries "
             "cross-session memory into a fresh session, rather than scanning every past session."),
        Knob("share_serving_values", "bool", "MATRIXARK_SHARE_SERVING_VALUES", False,
             "Point every served record at one shared object per distinct storage_route / "
             "storage_options / scope / access_scope value. OFF by default: it saves real memory "
             "(188 route objects for 3 values on a 3-session store) but it measurably perturbs the "
             "store -- an HTTP module that fails 0/20 runs on main fails 7/20 with this on, via "
             "background writes landing after test teardown, and that reproduced with the sweep "
             "written copy-on-write as well as in place, so the trigger is not value mutation (a "
             "tripwire dict recorded zero in-place writes to a shared value). Until that is "
             "explained, the sharing on the INTERNED path -- which is on by default and verified "
             "clean over the same 12 runs -- is the part that ships. Turn this on per tenant only "
             "with the same background-write check."),
        Knob("return_all_candidates", "bool", "MATRIXARK_RETURN_ALL_CANDIDATES", False,
             "Always return every eligible candidate: skip the secondary-index prefilter and skip "
             "scoring entirely, letting the token budget be the only limit. For a tenant whose whole "
             "memory fits the budget, ranking can only lose facts it did not need to lose."),
        Knob("return_all_candidate_threshold", "int", "MATRIXARK_RETURN_ALL_CANDIDATE_THRESHOLD", 0,
             "Return every candidate when a broad scan yields at most this many event/entity "
             "candidates; above it, fall back to the indexed + scored path. 0 = off. This is the "
             "self-tuning form of return_all_candidates: small stores pay nothing for ranking, "
             "large ones keep it."),
        Knob("node_path_embeddings", "bool", "MATRIXARK_NODE_PATH_EMBEDDINGS", True,
             "Embed a context_node's PATH when the node is created. ON by default, and turning it "
             "off is a correctness risk, not just a memory trade.\n\n"
             "A path is not content, so embedding it looked like pure waste -- it puts the "
             "tenant/user/session spine into the vector of every node. But it is the only embedding "
             "a node is guaranteed to have AT CREATION TIME. A node's other embedding comes from its "
             "SUMMARY, which is written later by refresh_summaries, so switching this off leaves a "
             "window in which a node cannot be scored at all. Retrieval then selects no node, the "
             "traversal has nothing to descend, and the pack falls back to profile entities alone.\n\n"
             "Measured: with this off, a 40-event store returned 1 pack item instead of 38 in 8 of "
             "10 retrieves on a loaded machine (0 of 10 on the unmodified code). One retrieve per "
             "fresh process is what exposes it -- 25 retrieves inside one process all succeed, "
             "because the first one warms the state the rest rely on. Load widens the window; it "
             "does not create it.\n\n"
             "Turn it off only for a tenant whose nodes are always summarised before their first "
             "retrieve, and verify with a cold-start check, not a warm one."),
        Knob("embed_node_path_prefix", "bool", "MATRIXARK_EMBED_NODE_PATH_PREFIX", False,
             "Include the node path in the text that node summaries are EMBEDDED from. Off by "
             "default: the path is near-identical across siblings, so it injects a shared component "
             "into every node vector -- measured, two nodes sharing nothing went from cos 0.0000 to "
             "0.3636 once prefixed, and the right-node margin shrank in both queries tested. The "
             "stored summary_text keeps the prefix; only the embedded text drops it."),
        Knob("generate_l1_summaries", "bool", "MATRIXARK_GENERATE_L1_SUMMARIES", True,
             "Generate the richer node_l1 overview alongside the mandatory node_l0. L0 is what "
             "traversal needs; L1 adds routing detail for nodes with a lot under them. Node "
             "summaries are bounded by tree size (3 + one per session), so this is a small, safe "
             "knob -- the linear-growing summary is batch_l0, one per ingest batch."),
        Knob("write_secondary_index", "bool", "MATRIXARK_WRITE_SECONDARY_INDEX", True,
             "Store context_index postings at all. A tenant whose corpus is small enough to scan "
             "can turn the index off entirely and pay nothing to maintain it."),
        Knob("dedupe_index_postings", "bool", "MATRIXARK_DEDUPE_INDEX_POSTINGS", True,
             "Collapse repeated postings for the same (scope, term, refs). Lossless."),
        Knob("store_event_summary_text", "bool", "MATRIXARK_STORE_EVENT_SUMMARY_TEXT", False,
             "Store a context_event's summary_text. Off by default: it is a whitespace-collapsed "
             "truncation of text (no LLM), so it duplicates text for any event under the limit, and "
             "every reader already falls back to text."),
        Knob("max_summary_text_chars", "int", "MATRIXARK_MAX_SUMMARY_TEXT_CHARS", 0,
             "Cap a context_summary's summary_text; 0 = the built-in budget (220 for L0, 1200 for "
             "L1). Measured: this is where noisy tool output actually persists -- an event drops it, "
             "the node summary keeps up to 1200 chars of it per row."),
        Knob("max_event_text_chars", "int", "MATRIXARK_MAX_EVENT_TEXT_CHARS", 0,
             "Clip each message's content to this many characters BEFORE extraction; 0 = unlimited. "
             "Bounds what a noisy tool-output turn can cost across the event, its extraction, its "
             "embedding and its index postings -- all of which read the clipped text."),
        Knob("generate_embeddings", "bool", "MATRIXARK_GENERATE_EMBEDDINGS", True,
             "Store context_embedding vectors. A small tenant can turn these off and retrieve "
             "through the secondary index instead -- embeddings are ~13% of resident memory."),
        Knob("collapse_pipeline_task_rows", "bool", "MATRIXARK_COLLAPSE_PIPELINE_TASK_ROWS", True,
             "Collapse re-stamped async pipeline task rows to the newest per (task, status)."),
        Knob("slim_terminal_pipeline_tasks", "bool", "MATRIXARK_SLIM_TERMINAL_PIPELINE_TASKS", False,
             "Age out finished pipeline tasks' dashboard-only payload."),
)


# Knobs a USER may override for themselves. Everything else is tenant-level.
#
# The store is shared inside a tenant. A knob that changes what gets WRITTEN, set differently by
# two users, leaves one store holding records of two shapes -- and unlike a setting, that does not
# go back when the setting does. It is the same failure as two writers disagreeing about the
# encoder: nobody notices until retrieval is quietly worse for everyone.
#
# These are the ones that decide how ONE request's results are selected and packed. They touch no
# stored record, so two users may hold different values without either of them affecting the other.
#
# `recall_reinforcement` is deliberately NOT here, and it is the reason this list is written by
# reading each knob rather than by pattern-matching the name: it reads like a retrieval knob and it
# is a writer -- measured, five searches produced 572 protection markers, about 114 writes per
# query. Per-user divergence there changes what survives pruning for the whole tenant.
READ_PATH_KNOBS = (
    "top_k_per_layer",
    "max_global_candidates",
    "max_selected_refs",
    "max_candidates_per_node",
    "cross_session_profile_max_candidates",
    "traverse_sibling_sessions",
    "pack_drop_redundant_items",
    "skill_chunks_per_skill",
    "skill_description_always",
    "skill_reserved_refs",
    "return_all_candidates",
    "return_all_candidate_threshold",
)

for _name in READ_PATH_KNOBS:
    if _name not in KNOBS:
        # A rename that leaves this list behind would silently drop a knob back to tenant-only.
        raise ValueError(f"READ_PATH_KNOBS names {_name!r}, which is not a knob")
    KNOBS[_name].layer = "read"
del _name


def tenant_hash_of(tenant_id: str, account_id: str = "") -> str:
    """The ``t=`` hash a scope_key carries for `tenant_id`, so policy keyed by the human id also
    resolves for records that were reduced to hashes. Mirrors
    ``matrixark_mcp_identity.identity_hashes``; returns "" when identity helpers are unavailable."""
    try:
        try:
            from tools.matrixark_mcp_identity import canonical_account_id, canonical_tenant_id, stable_hash
        except ModuleNotFoundError:
            from matrixark_mcp_identity import canonical_account_id, canonical_tenant_id, stable_hash
    except Exception:  # identity layer not importable (pure-policy unit tests)
        return ""
    account = canonical_account_id(str(account_id or ""))
    tenant = canonical_tenant_id(str(tenant_id or ""))
    return str(stable_hash(f"{account}:{tenant}"))


def _tenant_aliases(tenant_id: str) -> list[str]:
    """Every identity a policy for `tenant_id` should answer to: the id itself and its scope hash."""
    aliases = [str(tenant_id)]
    hashed = tenant_hash_of(tenant_id)
    if hashed and hashed not in aliases:
        aliases.append(hashed)
    return aliases


_KNOBS_BY_ALIAS: dict[str, Knob] = {
    alias: knob for knob in KNOBS.values() for alias in knob.aliases
}

_LOCK = threading.RLock()
_FILE_CACHE: dict[str, Any] = {"path": None, "mtime_ns": -1, "defaults": {}, "tenants": {},
                               "users": {}}
_RECORD_POLICIES: dict[str, Json] = {}
# Per-user overrides, keyed by (tenant, user) -- never by the user id alone. "alice" in two tenants
# is two people, and a policy keyed on the bare id would serve one customer's settings to another.
_USER_POLICIES: dict[str, Json] = {}


def _validated(policy: Any, *, source: str, tenant: str) -> Json:
    """Keep the knobs we know, coerced to their declared type; drop and log anything else."""
    if not isinstance(policy, dict):
        LOGGER.warning("tenant_policy_ignored source=%s tenant=%s reason=not_an_object", source, tenant)
        return {}
    clean: Json = {}
    for key, value in policy.items():
        knob = KNOBS.get(str(key)) or _KNOBS_BY_ALIAS.get(str(key))
        if knob is None:
            LOGGER.warning("tenant_policy_unknown_knob source=%s tenant=%s knob=%s", source, tenant, key)
            continue
        try:
            clean[knob.name] = knob.coerce(value)
        except (TypeError, ValueError):
            LOGGER.warning("tenant_policy_bad_value source=%s tenant=%s knob=%s value=%r",
                           source, tenant, key, value)
    return clean


def _load_file_policies() -> tuple[Json, dict[str, Json]]:
    """File defaults and per-tenant overrides. `_load_file_users` reads the same file's users."""
    path = os.environ.get("MATRIXARK_TENANT_POLICY_PATH", "").strip()
    if not path:
        return {}, {}
    try:
        stat = os.stat(path)
        # (mtime, size, inode): a rewrite inside one filesystem timestamp tick is invisible to mtime
        # alone, which would serve a stale policy indefinitely.
        mtime_ns = (stat.st_mtime_ns, stat.st_size, stat.st_ino)
    except OSError:
        if _FILE_CACHE["path"] == path:
            LOGGER.warning("tenant_policy_file_unreadable path=%s (keeping last good policy)", path)
            return _FILE_CACHE["defaults"], _FILE_CACHE["tenants"]
        LOGGER.warning("tenant_policy_file_missing path=%s", path)
        return {}, {}
    with _LOCK:
        if _FILE_CACHE["path"] == path and _FILE_CACHE["mtime_ns"] == mtime_ns:
            return _FILE_CACHE["defaults"], _FILE_CACHE["tenants"]
        try:
            with open(path, encoding="utf-8") as handle:
                raw = json.load(handle)
        except (OSError, ValueError) as error:
            # A broken edit must not silently revert everyone to defaults mid-flight.
            LOGGER.warning("tenant_policy_file_invalid path=%s error=%s (keeping last good policy)",
                           path, error)
            return _FILE_CACHE["defaults"], _FILE_CACHE["tenants"]
        defaults = _validated(raw.get("defaults", {}), source=path, tenant="*")
        tenants: dict[str, Json] = {}
        for tenant, policy in (raw.get("tenants", {}) or {}).items():
            clean = _validated(policy, source=path, tenant=str(tenant))
            for alias in _tenant_aliases(str(tenant)):
                tenants[alias] = clean
        # Per-user overrides live in the same file, under `users`, keyed tenant then user. Read
        # through the same validation, so a write-path knob written here by hand is refused exactly
        # as it is through the API.
        users: dict[str, Json] = {}
        for tenant, by_user in (raw.get("users", {}) or {}).items():
            if not isinstance(by_user, dict):
                LOGGER.warning("user_policy_ignored source=%s tenant=%s reason=not_an_object",
                               path, tenant)
                continue
            for user, policy in by_user.items():
                clean_user = _validated_user(policy, source=path, tenant=str(tenant),
                                             user=str(user))
                for alias in _user_aliases(str(tenant), str(user)):
                    users[alias] = clean_user
        _FILE_CACHE.update({"path": path, "mtime_ns": mtime_ns, "defaults": defaults,
                            "tenants": tenants, "users": users})
        LOGGER.info("tenant_policy_loaded path=%s tenants=%d users=%d",
                    path, len(tenants), len(users))
        return defaults, tenants


def register_tenant_policy_records(records: list[Json]) -> int:
    """Absorb ``matrixark_tenant_policy`` rows from the store (later rows win). Returns the count."""
    found = 0
    for record in records or ():
        if str(record.get("record_type") or "") != TENANT_POLICY_RECORD_TYPE:
            continue
        tenant = str(record.get("tenant_id") or record.get("tenant_hash") or "").strip()
        if not tenant:
            continue
        clean = _validated(record.get("policy"), source="store", tenant=tenant)
        with _LOCK:
            for alias in _tenant_aliases(tenant):
                _RECORD_POLICIES[alias] = clean
        found += 1
    return found


def set_tenant_policy(tenant_id: str, policy: Json, *, merge: bool = True) -> Json:
    """Change one tenant's policy WHILE THE SERVICE IS RUNNING; returns the tenant's new override set.

    Takes effect on the next resolution -- no restart, no reconnect, and no effect on any other
    tenant. Callers that want it to survive a restart append the returned policy to the store as a
    ``matrixark_tenant_policy`` record (``register_tenant_policy_records`` reads it back on load);
    the in-memory update here is what makes the change immediate.

    ``merge=False`` replaces the tenant's overrides outright instead of layering onto them."""
    tenant = str(tenant_id or "").strip()
    if not tenant:
        raise ValueError("set_tenant_policy requires a tenant id")
    clean = _validated(policy, source="runtime", tenant=tenant)
    with _LOCK:
        if merge:
            current = dict(_RECORD_POLICIES.get(tenant, {}))
            current.update(clean)
            merged = current
        else:
            merged = clean
        # Registered under the human id AND its scope hash: a served record usually carries only the
        # hash, so a policy that answered to just the id would silently miss every such record.
        for alias in _tenant_aliases(tenant):
            _RECORD_POLICIES[alias] = merged
        result = dict(merged)
    LOGGER.info("tenant_policy_set tenant=%s knobs=%s merge=%s", tenant, sorted(clean), merge)
    return result


def tenant_policy_record(tenant_id: str, policy: Json) -> Json:
    """The durable record form of a policy change, for appending to the store."""
    return {
        "record_type": TENANT_POLICY_RECORD_TYPE,
        "tenant_id": str(tenant_id),
        "policy": _validated(policy, source="runtime", tenant=str(tenant_id)),
    }


def clear_tenant_policy_cache() -> None:
    """Drop every cached policy (tests, and after a control-plane rewrite)."""
    with _LOCK:
        _FILE_CACHE.update({"path": None, "mtime_ns": -1, "defaults": {}, "tenants": {},
                            "users": {}})
        _RECORD_POLICIES.clear()
        # The per-user layer clears with it: a test or a control-plane rewrite that reset the
        # tenant policy and left user overrides standing would resolve from a half-cleared state.
        _USER_POLICIES.clear()


def tenant_of(scope: Any) -> str:
    """Best-effort tenant identity from a scope dict, a scope_key, or a bare id ("" if none)."""
    if scope in (None, "", {}):
        return ""
    if isinstance(scope, dict):
        for field in ("tenant_id", "tenant", "tenant_hash"):
            value = scope.get(field)
            if value not in (None, "", 0):
                return str(value)
        return tenant_of(scope.get("scope_key") or "")
    text = str(scope)
    if "=" in text:  # scope_key form: t=<hash>|u=<hash>|s=<hash>
        for part in text.replace("|", ";").replace(",", ";").split(";"):
            key, _, value = part.partition("=")
            if key.strip() in {"t", "tenant", "tenant_hash"} and value.strip():
                return value.strip()
        return ""
    return text.strip()


def user_of(scope: Any) -> str:
    """Best-effort user identity from a scope dict or a scope_key ("" if none).

    Mirrors `tenant_of`. A scope_key reduces identities to hashes, so the `u=` part is read the same
    way the `t=` part is -- a policy set from the portal by human id must still answer for a served
    record that carries only hashes.
    """
    if scope in (None, "", {}):
        return ""
    if isinstance(scope, dict):
        for field in ("user_id", "user", "user_hash"):
            value = scope.get(field)
            if value not in (None, "", 0):
                return str(value)
        return user_of(scope.get("scope_key") or "")
    text = str(scope)
    if "=" in text:  # scope_key form: t=<hash>|u=<hash>|s=<hash>
        for part in text.replace("|", ";").replace(",", ";").split(";"):
            key, _, value = part.partition("=")
            if key.strip() in {"u", "user", "user_hash"} and value.strip():
                return value.strip()
    return ""


def user_key(tenant_id: str, user_id: str) -> str:
    """The key a per-user policy is stored under.

    Both halves, always. A per-user policy keyed on the user id alone would hand one tenant's
    settings to a same-named user in another -- the identities are only unique together.
    """
    return "%s\x1f%s" % (str(tenant_id or "").strip(), str(user_id or "").strip())


def _user_aliases(tenant_id: str, user_id: str) -> list[str]:
    """Every key a per-user policy should answer to: each tenant alias paired with each user form."""
    users = [str(user_id or "").strip()]
    try:
        from matrixark_mcp_identity import identity_hashes  # type: ignore

        hashed = identity_hashes({"tenant_id": tenant_id, "user_id": user_id}) or {}
        candidate = str(hashed.get("user_hash") or "").strip()
        if candidate and candidate not in users:
            users.append(candidate)
    except Exception:
        # Identity helpers are optional here, exactly as they are for the tenant aliases.
        pass
    return [user_key(tenant_alias, user)
            for tenant_alias in _tenant_aliases(tenant_id)
            for user in users if user]


def _validated_user(policy: Any, *, source: str, tenant: str, user: str) -> Json:
    """Validate a per-user override, refusing anything that is not read-path.

    A refusal is logged rather than raised: one bad key in a submitted set must not discard the
    rest, and the caller is told which were kept.
    """
    clean = _validated(policy, source=source, tenant=tenant)
    kept: Json = {}
    for name, value in clean.items():
        if KNOBS[name].layer != "read":
            LOGGER.warning(
                "user_policy_refused source=%s tenant=%s user=%s knob=%s reason=write_path",
                source, tenant, user, name)
            continue
        kept[name] = value
    return kept


def user_policy(scope: Any = None) -> Json:
    """The per-user overrides in force for `scope` (file < store record), empty when there are none.

    Same precedence as the tenant layer, so one mental model covers both: what is on disk is the
    starting point and what was set at runtime wins.
    """
    tenant = tenant_of(scope)
    user = user_of(scope)
    if not tenant or not user:
        return {}
    _load_file_policies()  # refreshes the cache, including its users map
    merged: Json = {}
    with _LOCK:
        file_users = _FILE_CACHE.get("users") or {}
        for key in _user_aliases(tenant, user):
            if key in file_users:
                merged.update(file_users[key])
                break
        for key in _user_aliases(tenant, user):
            found = _USER_POLICIES.get(key)
            if found:
                merged.update(found)
                break
    return merged


def set_user_policy(tenant_id: str, user_id: str, policy: Json, *, merge: bool = True) -> Json:
    """Change one user's overrides WHILE THE SERVICE IS RUNNING; returns their new override set.

    Takes effect on the next resolution -- no restart, and no effect on any other user or on the
    tenant's own policy. Write-path knobs are dropped with a warning rather than accepted: they
    decide what goes into a store this user shares with everyone else in the tenant.

    Callers that want it to survive a restart append the returned policy to the store as a
    ``matrixark_user_policy`` record; the in-memory update here is what makes it immediate.
    """
    tenant = str(tenant_id or "").strip()
    user = str(user_id or "").strip()
    if not tenant:
        raise ValueError("set_user_policy requires a tenant id")
    if not user:
        raise ValueError("set_user_policy requires a user id")
    clean = _validated_user(policy, source="runtime", tenant=tenant, user=user)
    with _LOCK:
        if merge:
            current = dict(_USER_POLICIES.get(user_key(tenant, user), {}))
            current.update(clean)
            merged = current
        else:
            merged = clean
        for alias in _user_aliases(tenant, user):
            _USER_POLICIES[alias] = merged
        result = dict(merged)
    LOGGER.info("user_policy_set tenant=%s user=%s knobs=%s merge=%s",
                tenant, user, sorted(clean), merge)
    return result


def user_policy_record(tenant_id: str, user_id: str, policy: Json) -> Json:
    """The durable record form of a per-user change, for appending to the store."""
    return {
        "record_type": USER_POLICY_RECORD_TYPE,
        "tenant_id": str(tenant_id),
        "user_id": str(user_id),
        "policy": _validated_user(policy, source="runtime",
                                  tenant=str(tenant_id), user=str(user_id)),
    }


def register_user_policy_records(records: list[Json]) -> int:
    """Absorb ``matrixark_user_policy`` rows from the store (later rows win). Returns the count."""
    found = 0
    for record in records or ():
        if str(record.get("record_type") or "") != USER_POLICY_RECORD_TYPE:
            continue
        tenant = str(record.get("tenant_id") or record.get("tenant_hash") or "").strip()
        user = str(record.get("user_id") or record.get("user_hash") or "").strip()
        if not tenant or not user:
            # Half an identity is not an identity; a record missing either half would otherwise
            # land under a key that some other user resolves to.
            continue
        clean = _validated_user(record.get("policy"), source="store", tenant=tenant, user=user)
        with _LOCK:
            for alias in _user_aliases(tenant, user):
                _USER_POLICIES[alias] = clean
        found += 1
    return found


def policy_file_path() -> str:
    """Where policy is persisted, or "" when nothing is configured to persist to."""
    return os.environ.get("MATRIXARK_TENANT_POLICY_PATH", "").strip()


def persist_user_policy(tenant_id: str, user_id: str, policy: Json, *,
                        merge: bool = True) -> bool:
    """Write one user's overrides into the policy file. Returns False when there is no file.

    Written through a temporary file and renamed into place: a policy file half-rewritten when a
    process dies is one the loader would keep serving as "last good" without knowing it is a
    fragment.

    The caller is told when nothing was persisted rather than being allowed to assume it was. A
    change that applies now and vanishes on the next restart is the kind of thing a customer only
    discovers when it matters.
    """
    path = policy_file_path()
    if not path:
        return False
    tenant = str(tenant_id or "").strip()
    user = str(user_id or "").strip()
    if not tenant or not user:
        raise ValueError("persist_user_policy requires both a tenant id and a user id")
    clean = _validated_user(policy, source="portal", tenant=tenant, user=user)
    with _LOCK:
        try:
            with open(path, encoding="utf-8") as handle:
                document = json.load(handle)
            if not isinstance(document, dict):
                document = {}
        except (OSError, ValueError):
            # A file that is missing or unreadable is started fresh; a file that is CORRUPT is not
            # silently replaced -- the loader is still serving its last good copy and overwriting it
            # here would destroy the only record of what was configured.
            if os.path.exists(path):
                LOGGER.warning("policy_file_unparsable path=%s (refusing to overwrite)", path)
                return False
            document = {}
        users = document.setdefault("users", {})
        if not isinstance(users, dict):
            LOGGER.warning("policy_file_users_not_an_object path=%s (refusing to overwrite)", path)
            return False
        by_user = users.setdefault(tenant, {})
        if not isinstance(by_user, dict):
            by_user = {}
            users[tenant] = by_user
        if merge and isinstance(by_user.get(user), dict):
            merged = dict(by_user[user])
            merged.update(clean)
        else:
            merged = clean
        by_user[user] = merged
        temporary = path + ".tmp"
        with open(temporary, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(document, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _FILE_CACHE["mtime_ns"] = -1  # force the next read to pick this up
    LOGGER.info("user_policy_persisted path=%s tenant=%s user=%s knobs=%s",
                path, tenant, user, sorted(clean))
    return True


def clear_user_policy_cache() -> None:
    """Drop every cached per-user override (tests, and after a control-plane rewrite)."""
    with _LOCK:
        _USER_POLICIES.clear()


def tenant_scope_from_node_path(node_path: Any) -> Json:
    """Recover a tenant identity from a context node path (``["tenant:acme", "user:u1", ...]``).

    A background summary refresh can run with no scope argument, against a dirty marker whose own
    scope was stripped by serving materialization -- but the node path always names the tenant, so
    per-tenant policy has a last-resort identity instead of failing open."""
    if not isinstance(node_path, (list, tuple)):
        return {}
    for part in node_path:
        text = str(part or "")
        if text.startswith("tenant:") and len(text) > 7:
            return {"tenant_id": text[7:]}
    return {}


def tenant_policy(scope: Any = None) -> Json:
    """The merged override set for `scope`'s tenant (file defaults < file tenant < store record)."""
    file_defaults, file_tenants = _load_file_policies()
    tenant = tenant_of(scope)
    merged: Json = dict(file_defaults)
    if tenant:
        merged.update(file_tenants.get(tenant, {}))
        with _LOCK:
            merged.update(_RECORD_POLICIES.get(tenant, {}))
    return merged


def resolve(name: str, scope: Any = None) -> Any:
    """Resolve one knob for `scope`: user -> tenant -> env var -> built-in default.

    The user layer only ever holds read-path knobs (see READ_PATH_KNOBS), so a user override can
    change how their own results are selected and can never change what is written into the store
    their tenant shares.
    """
    knob = KNOBS.get(name) or _KNOBS_BY_ALIAS.get(name)
    if knob is None:
        raise KeyError(f"unknown tenant policy knob: {name}")
    if knob.layer == "read":
        mine = user_policy(scope)
        if knob.name in mine:
            return mine[knob.name]
    policy = tenant_policy(scope)
    if knob.name in policy:
        return policy[knob.name]
    raw = os.environ.get(knob.env)
    if raw is None or str(raw).strip() == "":
        for legacy in knob.env_aliases:
            candidate = os.environ.get(legacy)
            if candidate is not None and str(candidate).strip() != "":
                raw = candidate
                break
    if raw is not None and str(raw).strip() != "":
        try:
            return knob.coerce(raw)
        except (TypeError, ValueError):
            LOGGER.warning("tenant_policy_bad_env knob=%s value=%r", name, raw)
    return knob.default


def describe_effective_policy(scope: Any = None) -> Json:
    """Every knob's effective value and where it came from -- for support and for the dashboard."""
    policy = tenant_policy(scope)
    mine = user_policy(scope)
    out: Json = {"tenant": tenant_of(scope), "user": user_of(scope), "knobs": {}}
    for name, knob in KNOBS.items():
        if knob.layer == "read" and name in mine:
            source = "user"
        elif name in policy:
            source = "tenant"
        elif os.environ.get(knob.env, "").strip() != "" or any(
            os.environ.get(legacy, "").strip() != "" for legacy in knob.env_aliases
        ):
            source = "env"
        else:
            source = "default"
        out["knobs"][name] = {"value": resolve(name, scope), "source": source, "env": knob.env,
                              # What a customer needs to know before trying to set it: a write-path
                              # knob is tenant-level because the store is shared.
                              "layer": knob.layer,
                              "settable_per_user": knob.layer == "read"}
    return out


# One registry, whichever name imported this module first.
#
# This file is reachable as ``matrixark_tenant_policy`` (running from tools/) and as
# ``tools.matrixark_tenant_policy`` (imported as part of the package). Python treats those as two
# modules and gives each its own ``_RECORD_POLICIES`` and ``_FILE_CACHE``, so a policy set through
# one name is invisible to a reader that arrived through the other -- the cap silently does not
# apply, and nothing says so.
#
# Aliasing both names to the module object that loaded first means there is one registry however
# it was reached. ``setdefault`` so the first loader wins and a second import finds it rather than
# re-executing this file.
import sys as _sys

_sys.modules.setdefault("matrixark_tenant_policy", _sys.modules[__name__])
_sys.modules.setdefault("tools.matrixark_tenant_policy", _sys.modules[__name__])
