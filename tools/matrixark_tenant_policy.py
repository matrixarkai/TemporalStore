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


class Knob:
    """One tunable: its type, its env var, and the value used when nobody configured it.

    ``aliases`` / ``env_aliases`` keep an earlier name working after a rename, so an existing
    deployment's config file or environment is not silently ignored."""

    __slots__ = ("name", "kind", "env", "default", "description", "aliases", "env_aliases")

    def __init__(self, name: str, kind: str, env: str, default: Any, description: str,
                 aliases: tuple = (), env_aliases: tuple = ()) -> None:
        self.name = name
        self.kind = kind
        self.env = env
        self.default = default
        self.description = description
        self.aliases = aliases
        self.env_aliases = env_aliases

    def coerce(self, value: Any) -> Any:
        if self.kind == "bool":
            if isinstance(value, bool):
                return value
            return str(value).strip().lower() not in {"0", "false", "no", "off", ""}
        if self.kind == "int":
            return max(0, int(str(value).strip()))
        return value


KNOBS: dict[str, Knob] = {
    knob.name: knob
    for knob in (
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
}


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
_FILE_CACHE: dict[str, Any] = {"path": None, "mtime_ns": -1, "defaults": {}, "tenants": {}}
_RECORD_POLICIES: dict[str, Json] = {}


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
        _FILE_CACHE.update({"path": path, "mtime_ns": mtime_ns, "defaults": defaults, "tenants": tenants})
        LOGGER.info("tenant_policy_loaded path=%s tenants=%d", path, len(tenants))
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
        _FILE_CACHE.update({"path": None, "mtime_ns": -1, "defaults": {}, "tenants": {}})
        _RECORD_POLICIES.clear()


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
    """Resolve one knob for `scope`'s tenant: tenant override -> env var -> built-in default."""
    knob = KNOBS.get(name) or _KNOBS_BY_ALIAS.get(name)
    if knob is None:
        raise KeyError(f"unknown tenant policy knob: {name}")
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
    out: Json = {"tenant": tenant_of(scope), "knobs": {}}
    for name, knob in KNOBS.items():
        if name in policy:
            source = "tenant"
        elif os.environ.get(knob.env, "").strip() != "" or any(
            os.environ.get(legacy, "").strip() != "" for legacy in knob.env_aliases
        ):
            source = "env"
        else:
            source = "default"
        out["knobs"][name] = {"value": resolve(name, scope), "source": source, "env": knob.env}
    return out
