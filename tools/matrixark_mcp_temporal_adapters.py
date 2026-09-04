#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""TemporalStore-backed MatrixArk adapters for and Rust backends."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_env import env_bool
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool


from matrixark_mcp_core import record_vector
import collections
import queue
import socket
import threading
import time
from pathlib import PurePosixPath

try:  # the proxy stderr drain is shared with the standalone proxy client
    from tools.matrixark_mcp_rust_proxy_process import (
        PROXY_STDERR_TAIL_LINES,
        _drain_proxy_stderr,
        proxy_stderr_tail,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_rust_proxy_process import (
        PROXY_STDERR_TAIL_LINES,
        _drain_proxy_stderr,
        proxy_stderr_tail,
    )

try:
    from tools.matrixark_mcp_core import *
    from tools.matrixark_mcp_core import (
        _DIRECT_RECORD_CACHE,
        _DIRECT_RECORD_CACHE_LOCK,
        _DIRECT_RECORD_CACHE_MAX_PREFIXES,
        _DIRECT_RECORD_LOAD_LOCKS,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES,
        _mcp_debug_log,
        compact_context_pack_for_serving_flat as compact_context_pack_for_serving,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import *
    from matrixark_mcp_core import (
        _DIRECT_RECORD_CACHE,
        _DIRECT_RECORD_CACHE_LOCK,
        _DIRECT_RECORD_CACHE_MAX_PREFIXES,
        _DIRECT_RECORD_LOAD_LOCKS,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES,
        _mcp_debug_log,
        compact_context_pack_for_serving_flat as compact_context_pack_for_serving,
    )

try:
    from tools.matrixark_mcp_local_adapter import MatrixArkLocalAdapter
    from tools.matrixark_mcp_local_adapter import RETRIEVAL_HOT_RECORD_TYPES
    from tools.matrixark_mcp_local_adapter import materialize_serving_record_batch
    from tools.matrixark_mcp_local_adapter import (
        _MEMORY_DERIVATIVE_RECORD_TYPES as MEMORY_DERIVATIVE_RECORD_TYPES,
        _record_provenance_source_ids as record_provenance_source_ids,
        _record_derivative_identity_ids as record_derivative_identity_ids,
    )
    from tools.matrixark_mcp_local_adapter import (
        auto_extraction_phase_budget_tokens,
        auto_memory_layer_budget_tokens,
        auto_memory_selection_policy_budget_tokens,
        auto_source_role_budget_tokens,
        memory_layer_budget_question_reason,
    )
    from tools.matrixark_mcp_metrics import MatrixArkServiceMetrics
    from tools.matrixark_mcp_raw_ingestion import normalize_raw_ingestion_record
    from tools.matrixark_mcp_retrieve_request import pre_retrieval_idle_commit_flush
    from tools.matrixark_mcp_retrieval import native_retrieve_fallback_allowed
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_local_adapter import MatrixArkLocalAdapter
    from matrixark_mcp_local_adapter import RETRIEVAL_HOT_RECORD_TYPES
    from matrixark_mcp_local_adapter import materialize_serving_record_batch
    from matrixark_mcp_local_adapter import (
        _MEMORY_DERIVATIVE_RECORD_TYPES as MEMORY_DERIVATIVE_RECORD_TYPES,
        _record_provenance_source_ids as record_provenance_source_ids,
        _record_derivative_identity_ids as record_derivative_identity_ids,
    )
    from matrixark_mcp_local_adapter import (
        auto_extraction_phase_budget_tokens,
        auto_memory_layer_budget_tokens,
        auto_memory_selection_policy_budget_tokens,
        auto_source_role_budget_tokens,
        memory_layer_budget_question_reason,
    )
    from matrixark_mcp_metrics import MatrixArkServiceMetrics
    from matrixark_mcp_raw_ingestion import normalize_raw_ingestion_record
    from matrixark_mcp_retrieve_request import pre_retrieval_idle_commit_flush
    from matrixark_mcp_retrieval import native_retrieve_fallback_allowed


def _latency_quantile_from_cumulative_buckets(buckets: list[int], bucket_bounds: tuple[float, ...], total: int, quantile: float) -> float:
    if total <= 0:
        return 0.0
    target = max(1, math.ceil(total * quantile))
    previous_bound = 0.0
    for count, bound in zip(buckets, bucket_bounds):
        if int(count) >= target:
            return previous_bound if bound == float("inf") else float(bound)
        if bound != float("inf"):
            previous_bound = float(bound)
    return previous_bound


def _latency_quantile_from_bucket_map(buckets: dict[str, Any], total: int, quantile: float) -> float:
    if total <= 0:
        return 0.0
    parsed: list[tuple[float, int]] = []
    for key, value in buckets.items():
        bound = float("inf") if str(key) == "+Inf" else float(key)
        parsed.append((bound, int(value or 0)))
    parsed.sort(key=lambda item: item[0])
    target = max(1, math.ceil(total * quantile))
    previous = 0.0
    for bound, count in parsed:
        if count >= target:
            return previous if bound == float("inf") else bound
        if bound != float("inf"):
            previous = bound
    return previous


def _float_metric_or_default(metrics: dict[str, Any], name: str, default: float = 0.0) -> float:
    if name not in metrics or metrics.get(name) is None:
        return float(default)
    try:
        return float(metrics.get(name))
    except (TypeError, ValueError):
        return float(default)


def _records_with_matrixark_write_debug(records: list[Json], **fields: Any) -> list[Json]:
    """Copy records and attach raw-write lifecycle fields for hook diagnostics."""

    if not records:
        return []
    cleaned = {key: value for key, value in fields.items() if value is not None}
    out: list[Json] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        copied = dict(record)
        existing = copied.get("matrixark_write_debug")
        debug = dict(existing) if isinstance(existing, dict) else {}
        debug.update(cleaned)
        copied["matrixark_write_debug"] = debug
        out.append(copied)
    return out


def matrixark_record_retention_filtered(record: Json, *, now_ms: int | None = None) -> bool:
    if not isinstance(record, dict):
        return False
    if bool(record.get("synthetic")):
        return True
    if str(record.get("retention_class") or "").lower() in {"debug", "probe"}:
        return True
    try:
        expires_at_ms = int(record.get("expires_at_ms") or 0)
    except (TypeError, ValueError):
        expires_at_ms = 0
    if expires_at_ms > 0:
        current_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        if expires_at_ms <= current_ms:
            return True
    try:
        deleted_at_ms = int(record.get("deleted_at_ms") or 0)
    except (TypeError, ValueError):
        deleted_at_ms = 0
    return deleted_at_ms > 0


TEMPORAL_COMPRESSED_OLD_RECORD_TYPES = {
    "context_compression_event",
    "context_temporal_compression",
}


def _durable_recovery_record_identity(record: Json) -> tuple[Any, ...]:
    record_type = str(record.get("record_type") or "")
    for field in (
        "event_id_hash",
        "entity_hash",
        "segment_hash",
        "summary_hash",
        "node_hash",
        "chunk_hash",
        "section_hash",
        "task_hash",
        "batch_id_hash",
        "ref_hash",
        "context_event_key",
    ):
        value = record.get(field)
        if value not in (None, "", [], {}):
            return (record_type, field, value)
    payload = json.dumps(record, sort_keys=True, separators=(",", ":"))
    return (record_type, "payload_hash", stable_hash(payload))


def _native_scope_with_hashes(scope: Json) -> Json:
    if not isinstance(scope, dict):
        return {}
    if int(scope.get("tenant_hash") or 0) and canonical_scope_key(scope):
        return dict(scope)
    defaults = local_identity_defaults({}, scope)
    account_id = str(scope.get("account_id") or defaults.get("account_id") or "acct_local")
    tenant_id = str(scope.get("tenant_id") or defaults.get("tenant_id") or "tenant_local_agent")
    user_id = str(scope.get("user_id") or defaults.get("user_id") or "")
    session_id = str(scope.get("session_id") or defaults.get("session_id") or "")
    agent_id = str(scope.get("agent_id") or "")
    hashes = identity_hashes(account_id, tenant_id, user_id, session_id, agent_id)
    explicit_scope_keys = {str(key) for key in scope.get("_explicit_scope_keys", []) if isinstance(key, str)}
    explicit_scope_keys.update(str(key) for key in scope.keys())
    enriched = {
        **scope,
        "account_id": account_id,
        "tenant_id": tenant_id,
        "tenant_hash": hashes["tenant_hash"],
        "scope_key": hashes["scope_key"],
        "_explicit_scope_keys": sorted(explicit_scope_keys),
    }
    if user_id:
        enriched["user_id"] = user_id
        enriched["user_hash"] = hashes["user_hash"]
    if session_id:
        enriched["session_id"] = session_id
        enriched["session_hash"] = hashes["session_hash"]
    if agent_id:
        enriched["agent_id"] = agent_id
        enriched["agent_hash"] = hashes["agent_hash"]
    return enriched


def _selected_ref_class(ref: Json) -> str:
    raw = str(ref.get("context_class") or ref.get("ref_type") or ref.get("type") or "").lower()
    if "entity" in raw:
        return "entity"
    if "segment" in raw:
        return "segment"
    if "summary" in raw:
        return "summary"
    if "resource" in raw or "chunk" in raw:
        return "resource"
    if "skill" in raw:
        return "skill"
    if "event" in raw:
        return "event"
    return raw or "ref"


def _selected_ref_stable_key(ref: Json) -> str:
    ref_class = _selected_ref_class(ref)
    stable_id = (
        ref.get("source_ref")
        or ref.get("context_event_key")
        or ref.get("summary_key")
        or ref.get("entity_name")
        or ref.get("resource_id")
        or ref.get("skill_id")
        or ref.get("ref_hash")
        or ref.get("event_id_hash")
        or ref.get("entity_hash")
        or ref.get("chunk_hash")
    )
    if stable_id is not None:
        return f"{ref_class}:{stable_id}"
    text = str(ref.get("text") or ref.get("summary_text") or ref.get("state") or "")
    return f"{ref_class}:text:{stable_hash(text)}"


def _compact_native_selected_refs(selected_refs: list[Json], *, max_total: int = 4, max_text_chars: int = 480) -> tuple[list[Json], int]:
    """Deduplicate and cap already-selected native refs without Python scans."""

    if not selected_refs:
        return [], 0
    per_class_limit = {
        "entity": 1,
        "event": 1,
        "segment": 1,
        "summary": 1,
        "resource": 1,
        "skill": 1,
        "ref": 1,
    }
    selected: list[Json] = []
    seen: set[str] = set()
    class_counts: dict[str, int] = {}
    dropped = 0
    for ref in selected_refs:
        if not isinstance(ref, dict):
            dropped += 1
            continue
        ref_class = _selected_ref_class(ref)
        key = _selected_ref_stable_key(ref)
        limit = per_class_limit.get(ref_class, 1)
        if key in seen or class_counts.get(ref_class, 0) >= limit or len(selected) >= max_total:
            dropped += 1
            continue
        normalized = dict(ref)
        normalized.setdefault("context_class", ref_class)
        text = normalized.get("text")
        if isinstance(text, str) and len(text) > max_text_chars:
            normalized["text"] = text[: max(0, max_text_chars - 1)].rstrip() + "..."
            normalized["token_estimate"] = max(1, (len(str(normalized["text"])) + 3) // 4)
        selected.append(normalized)
        seen.add(key)
        class_counts[ref_class] = class_counts.get(ref_class, 0) + 1
    return selected, dropped



def _latency_quantile_from_cumulative_buckets(buckets: list[int], bucket_bounds: tuple[float, ...], total: int, quantile: float) -> float:
    if total <= 0:
        return 0.0
    target = max(1, math.ceil(total * quantile))
    previous_bound = 0.0
    for count, bound in zip(buckets, bucket_bounds):
        if int(count) >= target:
            return previous_bound if bound == float("inf") else float(bound)
        if bound != float("inf"):
            previous_bound = float(bound)
    return previous_bound


def _latency_quantile_from_bucket_map(buckets: dict[str, Any], total: int, quantile: float) -> float:
    if total <= 0:
        return 0.0
    parsed: list[tuple[float, int]] = []
    for key, value in buckets.items():
        bound = float("inf") if str(key) == "+Inf" else float(key)
        parsed.append((bound, int(value or 0)))
    parsed.sort(key=lambda item: item[0])
    target = max(1, math.ceil(total * quantile))
    previous = 0.0
    for bound, count in parsed:
        if count >= target:
            return previous if bound == float("inf") else bound
        if bound != float("inf"):
            previous = bound
    return previous

try:  # mixin
    from tools.matrixark_temporal_direct_backend import _TemporalDirectBackendMixin
except ImportError:
    from matrixark_temporal_direct_backend import _TemporalDirectBackendMixin

try:  # mixin
    from tools.matrixark_temporal_direct_write import _TemporalDirectWriteMixin
except ImportError:
    from matrixark_temporal_direct_write import _TemporalDirectWriteMixin

try:  # mixin
    from tools.matrixark_temporal_direct_read import _TemporalDirectReadMixin
except ImportError:
    from matrixark_temporal_direct_read import _TemporalDirectReadMixin

try:  # mixin
    from tools.matrixark_temporal_direct_retrieve import _TemporalDirectRetrieveMixin
except ImportError:
    from matrixark_temporal_direct_retrieve import _TemporalDirectRetrieveMixin

def matrixark_storage_mode_label(adapter: Any) -> str:
    """The storage mode a native adapter is actually running, for metrics/diagnostics."""
    if getattr(adapter, "_matrixark_proxy_mode", False):
        return "temporalstore-native-proxy"
    return "temporalstore-native"


# Scope fields that are DERIVED from an identity rather than naming one. When re-scoping a
# request from the caller to some other subject, these have to go: they are already computed for
# the caller, and leaving them in means the subject filter silently resolves back to the caller.
# That is not theoretical -- it made `users()` report the caller's memory count for every user and
# never drop a user whose memories had all been forgotten.
_SUBJECT_RESCOPE_DROP = frozenset({
    "session_id", "agent_id", "user_hash", "session_hash", "scope_key", "_explicit_scope_keys",
})



def _prior_context_probe_window() -> int:
    """How many newest events the cheap first pass fetches. 0 disables the probe entirely."""
    import os as _os

    raw = _os.environ.get("MATRIXARK_PRIOR_CONTEXT_PROBE_WINDOW", "").strip()
    if not raw:
        return 32
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 32


def _prior_context_probe_is_enough(records: list, scope: object) -> bool:
    """Does this small subset already hold every event the consumer would select?

    The consumer stops at `MAX_PRIOR_MESSAGES` matches walking newest-first, so a subset holding
    that many matches yields exactly the same selection. Counts scope matches only -- the consumer
    also accepts a session-key match, so this undercounts, which escalates rather than truncates.
    """
    if not isinstance(scope, dict) or not scope:
        return False
    try:
        from tools.matrixark_mcp_core import (
            MAX_PRIOR_MESSAGES,
            scope_from_serving_record,
            scope_matches,
        )
    except (ModuleNotFoundError, ImportError):
        try:
            from matrixark_mcp_core import (
                MAX_PRIOR_MESSAGES,
                scope_from_serving_record,
                scope_matches,
            )
        except (ModuleNotFoundError, ImportError):
            return False
    matched = 0
    for record in records:
        if not isinstance(record, dict) or record.get("record_type") != "context_event":
            continue
        try:
            if scope_matches(scope_from_serving_record(record), scope):
                matched += 1
                if matched >= MAX_PRIOR_MESSAGES:
                    return True
        except Exception:  # noqa: BLE001 - an unanswerable match escalates.
            return False
    return False


def _prior_context_event_window() -> int:
    """How many of a subject's newest events prior context fetches. 0 disables the cap.

    Default 256 against a consumer that uses 8: wide enough that a subject interleaving many
    sessions still finds its eight, narrow enough that the fetch stops growing with a subject's
    whole history.
    """
    import os as _os

    raw = _os.environ.get("MATRIXARK_PRIOR_CONTEXT_EVENT_WINDOW", "").strip()
    if not raw:
        return 256
    try:
        value = int(raw)
    except ValueError:
        return 256
    return max(value, 0)



def _shadow_compare_enabled() -> bool:
    """One switch for the shadow comparison, whichever read is running it.

    There were six of these, one per operation, and they were six copies of the same two lines.
    Nothing used the granularity: no test set any of them, nothing outside this module mentioned
    them, and none was offered on the portal. One name is easier to find, and easier to turn off
    again after a diagnosis.
    """
    import os as _os
    return _os.environ.get("MATRIXARK_SHADOW_COMPARE", "").strip() not in {
        "", "0", "false", "no", "off"
    }


def _shadow_log_path(operation: str) -> str:
    """Where the comparison writes. One override; the default still names the operation.

    The six per-operation defaults are kept deliberately -- comparing two reads is easier when
    their logs are separate files -- so what collapsed is the six ways to override the path, not
    the paths themselves.
    """
    import os as _os
    return _os.environ.get("MATRIXARK_SHADOW_LOG", "/tmp/matrixark_%s_shadow.log" % operation)


def _note_full_read_fallback(where: str, reason: BaseException) -> None:
    """Say which scoped scan gave up and why, when the answer is to read the whole store.

    Each of these replaces a scan of the records a request needs with a read of every record
    there is, on every turn it happens. Silent, that is invisible from outside: a scan path that
    has stopped working looks exactly like one that was never taken, and the cost shows up only as
    a store that got slower.

    Written only when MATRIXARK_MCP_DEBUG_LOG names a file, so the normal path costs one lookup --
    against a fallback that is about to read everything. Never raises: a channel that reports a
    problem must not become one.
    """
    try:
        import os as _os

        debug_path = _os.environ.get("MATRIXARK_MCP_DEBUG_LOG")
        if not debug_path:
            return
        with open(debug_path, "a", encoding="utf-8") as handle:
            handle.write("%s fell back to the full read: %s: %s%s"
                         % (where, reason.__class__.__name__, reason, chr(10)))
    except OSError:
        pass


def disk_fallback_store_path() -> str:
    """Where a direct adapter falls back to on disk, or "" when no fallback is configured.

    Two adapters set this in their constructors and a third restores it for an instance that
    predates the attribute; all three spelled out the same read, including the strip. The strip is
    the part worth having in one place: a path with a stray newline from a shell export is not the
    same string as the path, and two of the three would have had to remember to remove it.

    Read per call rather than captured at import, so a deployment that sets the variable after this
    module loads still gets it.
    """
    return os.environ.get("MATRIXARK_TEMPORALSTORE_LOCAL_STORE", "").strip()


class MatrixArkTemporalStoreDirectAdapter(MatrixArkLocalAdapter, _TemporalDirectBackendMixin, _TemporalDirectWriteMixin, _TemporalDirectReadMixin, _TemporalDirectRetrieveMixin):
    # The base class precedes the mixins in the MRO, so without this the buffered audit
    # implementation the __init__ prepares for (MATRIXARK_DIRECT_AUDIT_MODE, buffer, flusher)
    # is shadowed by the base's durable-append-per-record version.
    append_audit = _TemporalDirectWriteMixin.append_audit
    """MatrixArk adapter backed by TemporalStore proxy or direct SDK.

    Python stays as API/auth/model orchestration. Production retrieval should
    call native proxy/direct APIs for append, prefix scan, secondary-index
    prefiltering, scoring, and ContextPack assembly. Direct SDK remains the
    embedded/local path; MATRIXARK_TEMPORALSTORE_NATIVE_PROXY_ENDPOINT selects the
    proxy boundary.
    """

    def read_all(self) -> list[Json]:
        """Read through the native record log, not MatrixArkLocalAdapter's JSONL log.

        `MatrixArkLocalAdapter` precedes the direct mixins in this class's MRO and also
        defines `read_all`, so its JSONL implementation won here -- and on a native backend
        there is no JSONL log, so it returned ZERO records. Every reader built on read_all
        (get / get_all / update / history / keyed recall) therefore read empty while the
        records sat durably in the record log; the same inherited method is correct on the
        JSONL backend, which is why only the native backends looked broken.

        Only `read_all` collided: `read_all_without_disk_fallback_recovery` is defined solely
        on `_TemporalDirectReadMixin` and already resolved correctly, so delegating to it
        reproduces the direct read exactly without reordering bases (which would silently
        move every other method both classes define).

        The live-view post-processing MUST be kept: `MatrixArkLocalAdapter.read_all` does not
        just read, it filters the log down to live records. Delegating to the raw direct read
        alone silently drops that -- forget/delete tombstones stop being honoured (the subject's
        records stay visible after a forget) and a TTL record never expires, because expiry is
        enforced on read and is never cached.

        The body is deliberately identical to `MatrixArkLocalAdapter.read_all`; only the
        `_read_all_compacted` beneath it differs per backend.
        """
        try:
            from tools.matrixark_mcp_local_adapter import (
                compact_and_apply_tombstones,
                filter_live_memory_records,
            )
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_local_adapter import (
                compact_and_apply_tombstones,
                filter_live_memory_records,
            )
        records = self._read_all_compacted()
        # All three serving stages, in the order every other read path uses. This ran only the
        # last of them: expiry and retention were applied, compaction and tombstones were not. A
        # forget wrote its tombstone, reported an accurate removed_count, and then served every
        # one of those records straight back on the next read.
        return filter_live_memory_records(compact_and_apply_tombstones(records))

    def _read_all_compacted(self) -> list[Json]:
        """The compacted, tombstone-swept view -- WITHOUT the expiry/retention filter.

        The seam between "compacted" and "live" exists because some callers need to see records
        the live view hides: `sweep_expired_memories` has to find the expired rows in order to
        tombstone them, and would reclaim nothing if handed a view they had already been filtered
        out of. Overriding it here rather than only `read_all` is what makes the three memory
        paths built on it work on a native backend -- keyed upsert (`_apply_identity_upsert`),
        the retention-cutoff scope resolver, and the expiry sweep. All three called the inherited
        JSONL implementation, which returns `[]` as soon as `_local_jsonl_enabled` is False, so on
        every native backend keyed upsert silently never ran: a second ingest under the same
        `identity_key` found no existing record to supersede and both values stayed live.

        The two extra serving-pipeline stages are applied HERE rather than inside
        `_with_latest_context_state_records`, which the retrieval hot path also goes through:

          * `compact_latest_value_records` collapses records sharing a latest-value key. For a
            `context_event` that key is the `event_id_hash`, and ingest persists an event row
            twice -- once on the hot path (`extraction_phase: hot_path`) and again when
            extraction commits (`final`). Without it both rows serve and `get_all` reports one
            memory as two.
          * `apply_memory_tombstones` is what makes forget / delete / reset remove anything.
            Without it a forget writes its tombstone, reports an accurate `removed_count`, and
            then serves every one of those records right back.

        `compact_and_apply_tombstones` composes all three in the one order correct for both
        orphan sweeping and supersede (see its docstring). Both added stages fast-path a log
        with no duplicate keys / no tombstone, and all three are idempotent -- which matters,
        because the result feeds a cache this method is called against repeatedly.
        """
        try:
            from tools.matrixark_mcp_local_adapter import compact_and_apply_tombstones
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_local_adapter import compact_and_apply_tombstones
        self._recover_serving_from_disk_fallback_if_needed(reason="read_all")
        return compact_and_apply_tombstones(self.read_all_without_disk_fallback_recovery())

    def _read_raw_records(self) -> list[Json]:
        """The native record log in append order -- NOT compacted, NOT tombstone-filtered.

        Same MRO collision as `read_all`: `MatrixArkLocalAdapter._read_raw_records` reads the
        JSONL shards and returns `[]` the moment `_local_jsonl_enabled` is False, which is
        exactly what every native backend sets. `history` is built on this method -- it is
        deliberately the RAW log, because the change history of a memory is the tombstones and
        superseded rows that the live view exists to hide -- so on a native backend history
        reported an empty log for a memory that plainly had one.

        The delete-before-extract guard in `session_commit` also reads through here. It asks
        whether a pending event survived the tombstone sweep and treats a tombstone-free log as
        "keep everything", so an empty result made it silently inert on native backends rather
        than wrong; with a real log behind it the guard now does its job there too.

        Deliberately does NOT fold in `_load_latest_context_state_records()`: that store holds
        the compacted latest state, which is the opposite of an append history, and none of it
        is a `context_event` or a tombstone.

        Deliberately does NOT take `_records_lock` either. That lock guards the serving-record
        cache, and this method reads nothing from it -- but taking it would put a thread inside
        `_records_lock` while it waits on the proxy client's lane semaphore, while the serving
        read holds a lane slot and waits on `_records_lock`. Two threads, opposite order: a real
        inversion, and nothing here needs the lock, since each call reads a fresh count and its
        records straight from the backend.

        To be precise about what was and was not observed: dropping this lock did NOT resolve the
        gateway wedge it was first suspected of causing. That wedge was the background summary
        refresher holding the single shared proxy lane (see `next_summary_refresh_delay_s`). The
        inversion above is a hazard on the code's own terms, not a diagnosis of that incident.
        """
        try:
            from tools.matrixark_mcp_latest_context_state import expand_record_bundles
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_latest_context_state import expand_record_bundles
        self._recover_serving_from_disk_fallback_if_needed(reason="read_raw_records")
        count = self._get_count()
        if count > 0:
            records = self._load_records_by_count(count)
        else:
            records = self._load_records(self._get_index())
        return expand_record_bundles(records)

    def _idempotency_index_key(self) -> str:
        return f"{self._storage_prefix}:idempotency_index"

    def _idempotency_index_ready_key(self) -> str:
        return f"{self._storage_prefix}:idempotency_index_ready"

    @staticmethod
    def _idempotency_index_field(key_hash: int) -> str:
        # Fixed width, so a field is never a prefix of another and ordering stays stable.
        return f"{int(key_hash):020d}"

    def _ensure_idempotency_index(self) -> None:
        """Make the keyed index cover what the log already holds, exactly once per store.

        Without this a miss could not be trusted. A store written before the index existed has
        idempotency records in the log and nothing in the index, so "absent from the index" would
        not mean "absent from the store", and a replay would be missed -- the one thing the
        idempotency record exists to prevent. Backfilling once and persisting a marker makes a
        miss authoritative from then on, and keeps a restart or a second worker from repeating it.
        """
        if getattr(self, "_idempotency_index_built", False):
            return
        try:
            if self._client.get_string(self._idempotency_index_ready_key()):
                self._idempotency_index_built = True
                return
        except Exception:  # noqa: BLE001 - an unreadable marker just means "build it".
            pass
        import json as _json

        entries: list[Json] = []
        index_key = self._idempotency_index_key()
        for record in self.read_all():
            if not isinstance(record, dict) or record.get("record_type") != "matrixark_idempotency":
                continue
            key_hash = record.get("key_hash")
            if key_hash is None:
                continue
            entries.append({
                "key": index_key,
                "field": self._idempotency_index_field(int(key_hash)),
                "value": _json.dumps(record, separators=(",", ":"), default=str),
            })
        if entries:
            self._client.batch_hset(entries)
        self._client.put_string(self._idempotency_index_ready_key(), "1")
        self._idempotency_index_built = True

    def find_idempotency_record(self, key_hash: int) -> Json | None:
        """Point-lookup, instead of walking the whole record log to answer one key.

        `MatrixArkLocalAdapter.find_idempotency_record` scans `reversed(self.read_all())`. On the
        JSONL backend that walks an in-memory list; on a native backend `read_all()` is a full
        record-log read shipped over the proxy and re-run through the serving pipeline. The request
        policy asks this twice per tool call -- once to replay, once before storing -- and an ingest
        is two tool calls, so ONE ingest paid four full-store reads purely to answer "have I seen
        this key". That is most of why ingest latency climbed with the number of records held.

        The log stays the source of truth: `append_idempotency_record` still appends the record and
        additionally writes the keyed entry, so durability, replay and history are unchanged.
        """
        self._ensure_idempotency_index()
        import json as _json
        import time as _time

        # A failed point-read must NOT fall back to the scanning lookup. The scan is a full-store
        # read, this lookup runs first on EVERY tool call, and the point-read only fails when the
        # lanes are already starved -- so the fallback launched store-wide reads at the exact
        # moment the system could least afford them, starving the lanes further and making the
        # next point-read fail too. Caught live: two of three request threads inside that scan
        # while metrics timed out at 120s. Retry the cheap read, then let the failure propagate as
        # the retryable error it is; answering "never seen" here instead would double-apply the
        # request, and the scan would not have answered under these conditions either.
        raw = None
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                raw = self._client.hget(
                    self._idempotency_index_key(), self._idempotency_index_field(key_hash)
                )
                last_error = None
                break
            except Exception as exc:  # noqa: BLE001 - retried, then propagated below.
                last_error = exc
                if attempt < 2:
                    _time.sleep(0.2 * (attempt + 1))
        if last_error is not None:
            raise last_error
        if not raw:
            return None
        try:
            record = _json.loads(raw)
        except (TypeError, ValueError):
            # The index held bytes the log never produced: corruption, not load. The log is the
            # source of truth, and this path is rare and not load-correlated, so the scan is safe.
            return self.find_idempotency_record_in_log(key_hash)
        return record if isinstance(record, dict) else None

    def append_idempotency_record(self, *, key_hash: int, tool_name: str, raw_key: str, identity: Json, response: Json) -> None:
        """Append the record to the log AND index it, so the next lookup is a point read.

        The indexed value is built with the same builder the log record uses rather than read
        back out of the log, because reading it back would be the very full-store scan this
        exists to remove.
        """
        super().append_idempotency_record(
            key_hash=key_hash, tool_name=tool_name, raw_key=raw_key, identity=identity, response=response,
        )
        try:
            from tools.matrixark_mcp_local_idempotency import build_idempotency_record
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_local_idempotency import build_idempotency_record
        import json as _json

        self._ensure_idempotency_index()
        record = build_idempotency_record(
            key_hash=key_hash, tool_name=tool_name, raw_key=raw_key, identity=identity, response=response,
        )
        try:
            self._client.hset(
                self._idempotency_index_key(),
                self._idempotency_index_field(key_hash),
                _json.dumps(record, separators=(",", ":"), default=str),
            )
        except Exception:  # noqa: BLE001 - the log already holds it; the index rebuilds from there.
            pass

    def find_idempotency_record_in_log(self, key_hash: int) -> Json | None:
        """The scanning lookup, kept addressable as the fallback and for tests."""
        return super().find_idempotency_record(key_hash)

    def _node_embedding_index_key(self) -> str:
        return f"{self._storage_prefix}:node_embedding_index"

    def _node_embedding_index_ready_key(self) -> str:
        return f"{self._storage_prefix}:node_embedding_index_ready"

    @staticmethod
    def _node_embedding_index_field(node_hash: int, model_ref: str) -> str:
        return f"{model_ref}:{int(node_hash):020d}"

    def _ensure_node_embedding_index(self) -> None:
        """Backfill the keyed index from the log once per store, then trust it.

        Same reasoning as the idempotency index: a store written before this index existed has
        node embeddings in the log and nothing in the index, so a miss would not mean "absent"
        and every node would be re-embedded. The marker is persisted so a restart or a second
        worker does not repeat the backfill -- which matters here because the native adapters
        deliberately start their context-node caches EMPTY and never load them from the store.
        """
        if getattr(self, "_node_embedding_index_built", False):
            return
        try:
            if self._client.get_string(self._node_embedding_index_ready_key()):
                self._node_embedding_index_built = True
                return
        except Exception:  # noqa: BLE001 - an unreadable marker just means "build it".
            pass
        entries: list[Json] = []
        index_key = self._node_embedding_index_key()
        for record in self.read_all():
            if (
                not isinstance(record, dict)
                or record.get("record_type") != "context_embedding"
                or record.get("ref_type") != "node"
                or record.get("embedding_type") != "context_node"
                or record.get("ref_hash") is None
                or not record_vector(record)
                or not record.get("vector")
            ):
                continue
            try:
                ref_hash = int(record.get("ref_hash"))
            except (TypeError, ValueError):
                continue
            entries.append({
                "key": index_key,
                "field": self._node_embedding_index_field(ref_hash, str(record.get("model_ref") or "")),
                "value": "1",
            })
        if entries:
            self._client.batch_hset(entries)
        self._client.put_string(self._node_embedding_index_ready_key(), "1")
        self._node_embedding_index_built = True

    def _existing_node_embedding_refs(self, current_model_ref: str) -> set[int]:
        """One small keyed scan instead of a full record-log read.

        The inherited implementation walks `read_all()` to collect the node hashes that already
        have an embedding. On a native backend that is a full record-log read over the proxy, and
        `ensure_context_node_path` runs THREE times per ingest, so it was the largest remaining
        O(store) cost on the ingest path. This index holds one tiny entry per embedded node rather
        than every record in the store.
        """
        self._ensure_node_embedding_index()
        scanner = getattr(self._client, "scan_hash", None)
        if not callable(scanner):
            return super()._existing_node_embedding_refs(current_model_ref)
        try:
            response = scanner(self._node_embedding_index_key())
        except Exception:  # noqa: BLE001 - re-embedding is worse than one slow read.
            return super()._existing_node_embedding_refs(current_model_ref)
        rows = response.get("records") if isinstance(response, dict) else []
        prefix = f"{current_model_ref}:"
        refs: set[int] = set()
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            field = str(row.get("field") or "")
            if not field.startswith(prefix):
                continue
            try:
                refs.add(int(field[len(prefix):]))
            except (TypeError, ValueError):
                continue
        return refs

    def _record_node_embedding_ref(self, node_hash: int, current_model_ref: str) -> None:
        self._ensure_node_embedding_index()
        try:
            self._client.hset(
                self._node_embedding_index_key(),
                self._node_embedding_index_field(node_hash, current_model_ref),
                "1",
            )
        except Exception:  # noqa: BLE001 - the log still has it; the index rebuilds from there.
            pass

    def refresh_summaries(self, args: Json) -> Json:
        """Skip the cross-scope background pass when the store has not changed since the last one.

        The background refresher calls this with an EMPTY scope on a timer. The first thing the
        pass does is `read_all()` -- a full record-log read -- and on a native backend that read
        goes over the proxy and holds the single shared lane for its whole duration. Measured on a
        99 MB store that is minutes, during which every request queues on the lane and is rejected
        on backpressure, with NO client load and nothing to refresh. Spacing the passes out cannot
        help: the cost is inside one pass, not between them.

        Dirty state only changes when records are appended, and the record count is a single point
        read. So if the count is exactly what it was before the last pass AND that pass found
        nothing to refresh, this pass would read the whole store to reach the same conclusion --
        skip it. The `refreshed_count == 0` half matters: a pass that hit its node limit left work
        behind, and must be allowed to run again even though nothing new was written.

        Deliberately scoped to the empty-scope background caller. The pre-retrieval refresh passes
        a real scope and is on the request path already, so it is left exactly as it was.
        """
        scope = args.get("scope") if isinstance(args, dict) else None
        if scope:
            return super().refresh_summaries(args)
        token = None
        try:
            token = int(self._get_count())
        except Exception:  # noqa: BLE001 - no cheap token available, just do the pass.
            token = None
        last_token, last_refreshed = self._load_summary_pass_state()
        if token is not None and token == last_token and last_refreshed == 0:
            return {"refreshed_count": 0, "status": "unchanged", "skipped": True}
        result = super().refresh_summaries(args)
        # Record the count as it was BEFORE the pass: a pass that refreshes something appends, so
        # the count moves and the next pass runs regardless.
        try:
            refreshed = int((result or {}).get("refreshed_count") or 0)
        except (TypeError, ValueError):
            refreshed = None
        # A pass stopped by its wall-clock budget left work behind even when it refreshed nothing,
        # and the unchanged-count skip must not park that backlog forever: record it as "did work"
        # so the next pass runs.
        if (result or {}).get("pass_budget_exhausted") and refreshed == 0:
            refreshed = 1
        self._store_summary_pass_state(token, refreshed)
        return result

    def _invalidate_summary_pass_state(self) -> None:
        """Force the next background refresh pass to actually run.

        The skip in `refresh_summaries` rests on the record COUNT: dirty state only changes when
        records are appended, so an unchanged count after a pass that found nothing means the next
        pass would reach the same conclusion. A purge breaks that -- it removes and rewrites
        records in place without moving the count -- and the node whose summary and index postings
        it just removed is exactly the node that now needs rebuilding. Left alone, an updated
        memory stayed in `get_all` and never became retrievable again.
        """
        self._summary_pass_state = (None, None)
        try:
            self._client.put_string(self._summary_pass_state_key(), "")
        except Exception:  # noqa: BLE001 - the in-process copy already forces a pass here.
            pass

    def _summary_pass_state_key(self) -> str:
        return f"{self._storage_prefix}:summary_pass_state"

    def _load_summary_pass_state(self) -> tuple[int | None, int | None]:
        """The record count and outcome of the last completed background pass.

        Held in the store, not just on the instance, so a RESTART does not force one full-store
        pass. That pass is minutes long on a large store and holds the single shared proxy lane
        for its whole duration, so an in-memory-only token turns every restart into a window
        where the gateway answers nothing -- which is exactly what it looked like before this
        was persisted.
        """
        cached = getattr(self, "_summary_pass_state", None)
        if cached is not None:
            return cached
        token: int | None = None
        refreshed: int | None = None
        try:
            raw = self._client.get_string(self._summary_pass_state_key())
        except Exception:  # noqa: BLE001 - unreadable state just means "run the pass".
            raw = ""
        if raw and ":" in raw:
            left, _, right = raw.partition(":")
            try:
                token, refreshed = int(left), int(right)
            except (TypeError, ValueError):
                token, refreshed = None, None
        self._summary_pass_state = (token, refreshed)
        return self._summary_pass_state

    def _store_summary_pass_state(self, token: int | None, refreshed: int | None) -> None:
        self._summary_pass_state = (token, refreshed)
        if token is None or refreshed is None:
            return
        try:
            self._client.put_string(self._summary_pass_state_key(), f"{token}:{refreshed}")
        except Exception:  # noqa: BLE001 - the instance copy still short-circuits this process.
            pass

    def backend_metrics(self) -> Json:
        """Describe the backend this adapter actually uses.

        Without this the direct adapter inherits `MatrixArkLocalAdapter.backend_metrics`, which
        hardcodes `mode: "local-jsonl"` and reports `event_log` -- and on a native backend that
        path is the `-unused-` sentinel file the adapter never writes to. So a native deployment
        asking for backend metrics was told it was running the JSONL backend, and pointed at a
        file that does not exist. The subclass used by `temporalstore-rust` already overrides
        this; `temporalstore-direct` is the backend that was left reporting the wrong engine.

        Kept cheap on purpose: a metrics call must not become a full-store read, so the only
        store access is the record count, which is a single point read.
        """
        metrics: Json = {
            "mode": matrixark_storage_mode_label(self),
            "storage_prefix": getattr(self, "_storage_prefix", ""),
            "namespace": getattr(self, "_namespace", ""),
            "table": getattr(self, "_table", ""),
        }
        try:
            metrics["record_count"] = int(self._get_count())
        except Exception:  # noqa: BLE001 - metrics must never be the thing that fails.
            metrics["record_count"] = None
        for name in ("health", "readiness", "metrics_snapshot"):
            probe = getattr(self._client, name, None)
            if not callable(probe):
                continue
            try:
                metrics[name] = probe()
            except Exception as exc:  # noqa: BLE001
                metrics[name] = {"ok": False, "error": str(exc)}
        return {
            "backend": self._backend_label(),
            "metrics_format": "json",
            "metrics": metrics,
        }

    def _subject_index_key(self) -> str:
        return f"{self._storage_prefix}:memory_subject_index"

    def _subject_index_ready_key(self) -> str:
        return f"{self._storage_prefix}:memory_subject_index_ready"

    def _ensure_subject_index(self) -> None:
        """Backfill the subject index from the log once per store, behind a persisted marker.

        Same shape as the idempotency and node-embedding indexes: without the backfill a store
        written before this existed would list no subjects at all, and without persisting the
        marker every restart would re-read the whole log to rebuild it.
        """
        if getattr(self, "_subject_index_built", False):
            return
        try:
            if self._client.get_string(self._subject_index_ready_key()):
                self._subject_index_built = True
                return
        except Exception:  # noqa: BLE001 - an unreadable marker just means "build it".
            pass
        entries: list[Json] = []
        index_key = self._subject_index_key()
        for record in self.read_all():
            for kind, name in self.memory_subjects_in_record(record):
                entries.append({"key": index_key, "field": f"{kind}:{name}", "value": "1"})
        if entries:
            self._client.batch_hset(entries)
        self._client.put_string(self._subject_index_ready_key(), "1")
        self._subject_index_built = True

    def _index_memory_subjects(self, records: list[Json]) -> None:
        """Note any NEW subject these records introduce.

        Called on the append path, so it has to stay close to free. The set of subjects is tiny
        and changes almost never, so an in-process set of the ones already written means a steady
        stream of writes for known subjects costs nothing, and only a genuinely new user / agent
        / run pays a single hset.
        """
        known = getattr(self, "_subject_index_seen", None)
        if known is None:
            known = self._subject_index_seen = set()
        fresh = []
        for record in records:
            for subject in self.memory_subjects_in_record(record):
                if subject not in known:
                    known.add(subject)
                    fresh.append(subject)
        if not fresh:
            return
        self._ensure_subject_index()
        index_key = self._subject_index_key()
        try:
            self._client.batch_hset([
                {"key": index_key, "field": f"{kind}:{name}", "value": "1"} for kind, name in fresh
            ])
        except Exception:  # noqa: BLE001 - the log still holds the truth; the index rebuilds.
            for subject in fresh:
                known.discard(subject)

    @staticmethod
    def _subject_scope(base_scope: Json, user_id: str) -> Json:
        """The caller's scope re-pointed at another subject, with that subject's OWN hashes.

        A request scope carries identity fields DERIVED from the caller -- `user_hash`,
        `session_hash`, `scope_key` -- and `get_all` filters on the hashes, never on `user_id`.
        So swapping only `user_id` leaves the caller's `user_hash` in place and the lookup
        resolves straight back to the caller; dropping the hashes instead yields `user_hash == 0`,
        which reads as "no subject filter" and returns the whole tenant. Both were observed:
        every user reported the same memory count, and a user whose memories had all been
        forgotten never dropped out of `users()`.

        The hashes have to be recomputed for the subject, by the same function the ingest path
        uses, so they match what is actually stored.
        """
        try:
            from tools.matrixark_mcp_core_identity import identity_hashes
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_core_identity import identity_hashes
        account_id = str(base_scope.get("account_id") or "")
        tenant_id = str(base_scope.get("tenant_id") or "")
        scope = {k: v for k, v in base_scope.items() if k not in _SUBJECT_RESCOPE_DROP}
        scope["user_id"] = user_id
        scope.update(identity_hashes(account_id, tenant_id, user_id=user_id))
        return scope

    def list_memory_subjects(self, args: Json) -> Json:
        """The keyed subject index, instead of reading the whole log to collect scopes.

        The inherited implementation walks `read_all()`. On a native backend that is a full
        record-log read over the proxy, and `users()` is exactly the kind of call an operator
        loops over, so it must not be O(store).

        The index is add-only, so it can name a subject whose memories have since all been
        forgotten or expired. mem0's `users()` means "who has memories", so the live view decides:
        the count comes from a scoped `get_all`, and a subject with none is dropped. That keeps
        the expensive part proportional to the number of SUBJECTS, not to the size of the store.
        """
        self._ensure_subject_index()
        limit = args.get("limit") if isinstance(args, dict) else None
        limit = int(limit) if isinstance(limit, int) and limit > 0 else 0
        scanner = getattr(self._client, "scan_hash", None)
        if not callable(scanner):
            return super().list_memory_subjects(args)
        try:
            response = scanner(self._subject_index_key())
        except Exception:  # noqa: BLE001
            return super().list_memory_subjects(args)
        rows = response.get("records") if isinstance(response, dict) else []
        subjects: list[tuple[str, str]] = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            field = str(row.get("field") or "")
            kind, _, name = field.partition(":")
            if kind and name:
                subjects.append((kind, name))
        # The subject's memories have to be counted in the CALLER's scope. A bare
        # {"user_id": name} carries no account/tenant, so the subject filter resolves to nothing
        # and get_all returns the whole tenant -- every user then reports the same count and a
        # user whose memories were all forgotten never drops out of the list.
        base_scope = dict(args.get("scope") or {}) if isinstance(args, dict) else {}
        ordered = sorted(set(subjects))
        counts = self._subject_counts_in_one_pass(
            base_scope, [name for kind, name in ordered if kind == "user"]
        )
        results: list[Json] = []
        for kind, name in ordered:
            if kind != "user":
                # Only a user is addressable by get_all here, so agents/runs are reported without
                # a live count rather than with a wrong one.
                results.append({"type": kind, "name": name})
                continue
            if counts is not None:
                live = counts.get(name, 0)
                if not live:
                    continue
                results.append({"type": kind, "name": name, "memory_count": live})
                if limit and len(results) >= limit:
                    break
                continue
            scope = self._subject_scope(base_scope, name)
            try:
                listed = self.get_all({"scope": scope})
            except Exception:  # noqa: BLE001
                results.append({"type": kind, "name": name})
                continue
            memories = listed.get("memories") or listed.get("items") or listed.get("results") or []
            if not memories:
                continue
            results.append({"type": kind, "name": name, "memory_count": len(memories)})
            if limit and len(results) >= limit:
                break
        return {"results": results, "count": len(results)}

    def _subject_counts_in_one_pass(self, base_scope: Json, names: list[str]) -> dict[str, int] | None:
        """Live `context_event` count per named user, from ONE read of the record log.

        Replaces one scoped `get_all` per subject. `get_all` filters in Python after reading the
        whole log, so per-subject reads cost O(subjects x store): 21 full reads of ~450ms for a
        single `users()` call over 20 subjects, measured.

        The predicate is `get_all`'s, unchanged -- a record belongs to a subject when the tenant
        and user hashes on its own access scope match the ones the same resolver derives for that
        subject's scope -- so this is a cheaper way to compute the same answer, not a different
        answer.

        Returns None, never an empty map, when the subjects cannot be resolved to distinct user
        hashes. The caller then falls back to the per-subject reads: being slow is recoverable,
        reporting that nobody has memories is not.
        """
        try:
            from tools.matrixark_mcp_local_adapter import _record_scope_hashes
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_local_adapter import _record_scope_hashes
        try:
            tenant_by_user: dict[int, int] = {}
            name_by_user: dict[int, str] = {}
            for name in names:
                tenant_hash, user_hash = self._resolve_subject_hashes(
                    self._subject_scope(base_scope, name)
                )
                if not user_hash or user_hash in name_by_user:
                    # Unresolvable, or two names landing on one hash: neither can be counted
                    # apart here, and guessing would attribute one subject's memories to another.
                    return None
                name_by_user[user_hash] = name
                tenant_by_user[user_hash] = tenant_hash
            if not name_by_user:
                return {}
            counts: dict[str, int] = {name: 0 for name in names}

            def count_in(records: list[Json], only_name: str | None = None) -> None:
                for record in records:
                    if not isinstance(record, dict):
                        continue
                    if str(record.get("record_type") or "") != "context_event":
                        continue
                    record_tenant, record_user = _record_scope_hashes(record)
                    name = name_by_user.get(record_user)
                    if name is None or (only_name is not None and name != only_name):
                        continue
                    subject_tenant = tenant_by_user.get(record_user) or 0
                    if subject_tenant and record_tenant != subject_tenant:
                        continue
                    counts[name] += 1

            # Two conditions, and both matter. Every subject must resolve to a non-zero tenant
            # AND user hash, which is what a pinned view needs to engage; and this adapter must
            # actually have the engine scan API, because without it a "pinned" view degrades to a
            # full read and per-subject counting becomes one full read PER SUBJECT -- strictly
            # worse than the single pass it replaced.
            scanner_available = callable(
                getattr(getattr(self, "_client", None), "matrixark_scan_candidates", None)
            )
            pinned = scanner_available and all(
                tenant_by_user.get(user_hash) and user_hash
                for user_hash in name_by_user
            )
            if pinned:
                for name in names:
                    count_in(
                        self.records_for_get_all(self._subject_scope(base_scope, name)),
                        only_name=name,
                    )
                return counts
            count_in(self.read_all())
            return counts
        except Exception:  # noqa: BLE001 - fall back to the per-subject reads rather than answer wrong.
            return None

    def memory_tombstones_may_exist(self) -> bool:
        """Answer from a type-filtered scan, so the guard does not read the whole log to find out.

        The inherited implementation reads every raw record. On a native backend that is a full
        record-log read on EVERY commit -- measured at up to 2392 records per commit -- and its
        entire output is one boolean, almost always False. The engine can filter by record type
        during its own walk, so only tombstone rows cross the boundary: normally none at all.

        Deliberately scope-free: the guard is store-wide, and a tombstone in any scope is a reason
        to do the full read. Bundled appends are handled engine-side, which matters because a
        tombstone can be stored inside a bundle whose wrapper carries no record_type.

        Returns True on ANY failure or missing support. A false negative here would skip the guard
        and let a deleted event be re-materialised by extraction; a false positive only costs the
        read the guard used to do unconditionally.
        """
        # No scanner at all is not the same as a scan that failed: without the capability the
        # base implementation answers accurately from the raw log (what would run anyway), while
        # a FAILED scan means the engine could not answer and the only safe report is True.
        if not callable(getattr(self._client, "matrixark_scan_candidates", None)):
            return super().memory_tombstones_may_exist()
        # One tombstone is enough to answer yes, and this runs on every add.
        if self._memory_tombstone_probe():
            return True
        # An empty capped answer is NOT conclusive: a location the index still lists but that no
        # longer resolves is skipped by the fetch, so a store with tombstones could probe empty.
        # A false negative here skips tombstone filtering and lets deleted memories come back, so
        # confirm with the full read -- which is cheap exactly when it runs, because a store with
        # no tombstones has nothing to read.
        records = self._memory_tombstone_records()
        if records is None:
            return True
        return bool(records)

    def _scan_records_of_types(
        self,
        record_types: list[str],
        record_ids: list[str] | None = None,
        scope: Json | None = None,
        newest_by_type: Json | None = None,
    ) -> list[Json] | None:
        """Records of exactly these types, in append order, from one filtered scan. None = could
        not ask.

        None and [] mean different things and the callers depend on it: [] is an authoritative
        "nothing of these types", None sends the caller to the expensive, correct path. Bundled
        appends are expanded engine-side, so a record stored inside a bundle whose wrapper carries
        no record_type is still returned.
        """
        # An adapter with no client at all (partial construction, stubs) is the same answer as
        # a client without the scan: the question cannot be asked here.
        scanner = getattr(getattr(self, "_client", None), "matrixark_scan_candidates", None)
        if not callable(scanner):
            return None
        try:
            response = scanner(
                count_key=self._count_key,
                record_hash_key=self._record_hash_key,
                shard_size=self._shard_size,
                scope=dict(scope) if scope else {},
                record_types=list(record_types),
                secondary_index_groups=[],
                selected_node_hashes=[],
                # The scan's serving default drops embedding/index rows AFTER filtering, which
                # silently overrides an explicit request for those types. This helper's contract
                # is "records of exactly these types": for requests without them the type filter
                # already excluded them and the flag is a no-op.
                return_index_records=True,
                **({"record_ids": [str(item) for item in record_ids]} if record_ids else {}),
                **({"newest_by_type": dict(newest_by_type)} if newest_by_type else {}),
            )
        except TypeError:
            # A client whose signature lacks one of the newer parameters. Retrying with fewer
            # kwargs is generally WRONG -- a scan without return_index_records silently eats the
            # embedding rows a caller asked for -- so the full read is the fallback.
            #
            # Except for one: the cap is the ONE kwarg safe to drop, because dropping it widens
            # the answer rather than narrowing it. Without it the scan returns every record of
            # these types, which is exactly what this call did before the cap existed, and the
            # consumer filters the same way either way. Falling all the way back to a full-store
            # read would be the opposite of what asking for a cap was for.
            if newest_by_type:
                try:
                    return self._scan_records_of_types(
                        record_types, record_ids=record_ids, scope=scope, newest_by_type=None
                    )
                except Exception as fallback_reason:  # noqa: BLE001 - the caller reads everything.
                    _note_full_read_fallback("_scan_records_of_types", fallback_reason)
                    return None
            return None
        except Exception:  # noqa: BLE001 - an unanswered question means "do the full read".
            return None
        if not isinstance(response, dict):
            return None
        records = response.get("records")
        if not isinstance(records, list):
            return None
        wanted = set(record_types)
        return [
            record for record in records
            if isinstance(record, dict) and str(record.get("record_type") or "") in wanted
        ]

    def _memory_tombstone_records(self) -> list[Json] | None:
        """Every memory tombstone in the store. None = could not ask (see _scan_records_of_types)."""
        try:
            from tools.matrixark_mcp_local_adapter import MEMORY_TOMBSTONE_RECORD_TYPE
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_local_adapter import MEMORY_TOMBSTONE_RECORD_TYPE
        return self._scan_records_of_types([MEMORY_TOMBSTONE_RECORD_TYPE])

    def _memory_tombstone_probe(self) -> list[Json] | None:
        """At most one memory tombstone, to answer whether any exist.

        Separate from `_memory_tombstone_records` on purpose: the consumer that tests each pending
        event against the tombstones genuinely needs all of them, while the guard only needs to
        know whether the set is non-empty. Reading the whole set to answer that made a per-add
        guard cost grow with every delete the store had ever seen.
        """
        try:
            from tools.matrixark_mcp_local_adapter import MEMORY_TOMBSTONE_RECORD_TYPE
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_local_adapter import MEMORY_TOMBSTONE_RECORD_TYPE
        return self._scan_records_of_types(
            [MEMORY_TOMBSTONE_RECORD_TYPE],
            newest_by_type={MEMORY_TOMBSTONE_RECORD_TYPE: 1},
        )

    # Every record type the refresh pass's consumers filter on, enumerated from the consumers
    # themselves. Deliberately absent: context_index -- the store's largest class, whose only
    # serving-chain effect (posting compaction) rewrites index rows nothing here reads -- and the
    # audit/telemetry/idempotency families.
    _SUMMARY_REFRESH_RECORD_TYPES = (
        "context_event",
        "context_summary",
        "context_summary_dirty",
        "context_summary_refresh_audit",
        "context_debug_record",
        "context_child_ref",
        "context_entity",
        "context_segment",
        "context_node",
        "context_embedding",
        "context_compression_event",
        "context_session_boundary",
    )

    def records_for_get_all(self, scope: Json) -> list[Json]:
        """A pinned subject's live view from a scoped typed scan, not a full-store read.

        Engages only when the scope resolves to non-zero tenant AND user hashes -- the engine's
        scope index answers exactly that shape, and anything broader serves the full read.
        Tombstones and retention markers carry no scope_key, so the scan's scopeless bucket
        carries them into the subset and the same serving chain as the full read applies.
        """
        import os as _os

        try:
            tenant_hash, user_hash = self._resolve_subject_hashes(scope or {})
        except Exception as fallback_reason:  # noqa: BLE001
            _note_full_read_fallback("records_for_get_all", fallback_reason)
            return self.read_all()
        if not tenant_hash or not user_hash:
            return self.read_all()
        try:
            from tools.matrixark_mcp_local_adapter import (
                MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
                MEMORY_TOMBSTONE_RECORD_TYPE,
                compact_and_apply_tombstones,
                filter_live_memory_records,
            )
            from tools.matrixark_mcp_serving_records import compact_latest_context_state_records
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_local_adapter import (
                MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
                MEMORY_TOMBSTONE_RECORD_TYPE,
                compact_and_apply_tombstones,
                filter_live_memory_records,
            )
            from matrixark_mcp_serving_records import compact_latest_context_state_records

        shadow_on = _shadow_compare_enabled()
        count_before = None
        if shadow_on:
            try:
                count_before = int(self._get_count())
            except Exception:  # noqa: BLE001
                count_before = None

        engine_scope: Json = {
            "tenant_hash": tenant_hash,
            "user_hash": user_hash,
            "_explicit_scope_keys": ["tenant_id", "user_id"],
        }
        if isinstance(scope, dict) and scope.get("user_id"):
            engine_scope["user_id"] = str(scope.get("user_id"))
        subset = self._scan_records_of_types(
            [
                "context_event",
                MEMORY_TOMBSTONE_RECORD_TYPE,
                MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
            ],
            scope=engine_scope,
        )
        if subset is None:
            return self.read_all()
        try:
            latest_state = self._load_latest_context_state_records()
        except Exception as fallback_reason:  # noqa: BLE001 - the full read is the fallback.
            _note_full_read_fallback("records_for_get_all", fallback_reason)
            return self.read_all()
        folded = compact_latest_context_state_records(list(subset) + list(latest_state))
        live_subset = filter_live_memory_records(compact_and_apply_tombstones(folded))

        if shadow_on:
            full = self.read_all()
            count_after = None
            try:
                count_after = int(self._get_count())
            except Exception:  # noqa: BLE001
                count_after = None
            log_path = _shadow_log_path("getall")
            if count_before is None or count_after is None or count_before != count_after:
                try:
                    with open(log_path, "a", encoding="utf-8") as log:
                        log.write("SKIPPED racy window count %r -> %r latest %r -> %r\n"
                                  % (count_before, count_after, latest_before, latest_after))
                except OSError:
                    pass
                return full

            def project(records: list[Json]) -> list[tuple]:
                out = []
                for record in records:
                    if str(record.get("record_type") or "") != "context_event":
                        continue
                    rec_tenant, rec_user = 0, 0
                    try:
                        from tools.matrixark_mcp_local_adapter import _record_scope_hashes
                    except ModuleNotFoundError:
                        from matrixark_mcp_local_adapter import _record_scope_hashes
                    rec_tenant, rec_user = _record_scope_hashes(record)
                    if rec_tenant != tenant_hash or rec_user != user_hash:
                        continue
                    out.append((str(record.get("event_id_hash") or ""),
                                int(record.get("updated_at_ms") or 0)))
                return out

            expected, got = project(full), project(live_subset)
            if expected != got:
                try:
                    with open(log_path, "a", encoding="utf-8") as log:
                        log.write("MISMATCH full=%d subset=%d only_full=%r only_subset=%r\n" % (
                            len(expected), len(got),
                            [row for row in expected if row not in set(got)][:4],
                            [row for row in got if row not in set(expected)][:4],
                        ))
                except OSError:
                    pass
            else:
                try:
                    with open(log_path, "a", encoding="utf-8") as log:
                        log.write("CLEAN %d records\n" % len(got))
                except OSError:
                    pass
            return full
        return live_subset

    def records_for_session_buffer(self, scope: Json) -> list[Json]:
        """The session-buffer cache-miss view from a pinned typed scan, not a full read.

        The miss consumer filters by buffer key (tenant, user, session) and record type, so a
        scope-pinned superset of the live view is sufficient. Engages only when the scope
        resolves to non-zero tenant AND user hashes; anything broader serves the full read. The
        subset runs the SAME serving chain as the full read (latest-state fold, tombstone sweep,
        expiry filter), so committed/tombstoned/expired records drop out identically.
        """
        import os as _os

        try:
            tenant_hash, user_hash = self._resolve_subject_hashes(scope or {})
        except Exception as fallback_reason:  # noqa: BLE001
            _note_full_read_fallback("records_for_session_buffer", fallback_reason)
            return self.read_all()
        if not tenant_hash or not user_hash:
            return self.read_all()
        try:
            from tools.matrixark_mcp_local_adapter import (
                MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
                MEMORY_TOMBSTONE_RECORD_TYPE,
                compact_and_apply_tombstones,
                filter_live_memory_records,
            )
            from tools.matrixark_mcp_serving_records import compact_latest_context_state_records
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_local_adapter import (
                MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
                MEMORY_TOMBSTONE_RECORD_TYPE,
                compact_and_apply_tombstones,
                filter_live_memory_records,
            )
            from matrixark_mcp_serving_records import compact_latest_context_state_records

        shadow_on = _shadow_compare_enabled()
        engine_scope: Json = {
            "tenant_hash": tenant_hash,
            "user_hash": user_hash,
            "_explicit_scope_keys": ["tenant_id", "user_id"],
        }
        if isinstance(scope, dict) and scope.get("user_id"):
            engine_scope["user_id"] = str(scope.get("user_id"))
        subset = self._scan_records_of_types(
            [
                "context_event",
                "session_buffer_event",
                "context_batch_commit",
                MEMORY_TOMBSTONE_RECORD_TYPE,
                MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
            ],
            scope=engine_scope,
        )
        if subset is None:
            return self.read_all()
        try:
            latest_state = self._load_latest_context_state_records()
        except Exception as fallback_reason:  # noqa: BLE001 - full read is the fallback.
            _note_full_read_fallback("records_for_session_buffer", fallback_reason)
            return self.read_all()
        folded = compact_latest_context_state_records(list(subset) + list(latest_state))
        live_subset = filter_live_memory_records(compact_and_apply_tombstones(folded))

        if shadow_on:
            full = self.read_all()
            log_path = _shadow_log_path("sessbuf")

            def project(records: list[Json]) -> tuple:
                events, buffers, commits = [], [], []
                for record in records:
                    rtype = str(record.get("record_type") or "")
                    if rtype == "context_event":
                        events.append(str(record.get("event_id_hash")))
                    elif rtype == "session_buffer_event":
                        buffers.append(str(record.get("event_id_hash")))
                    elif rtype == "context_batch_commit":
                        commits.append(str(record.get("batch_id_hash") or record.get("commit_hash") or ""))
                return (sorted(events), sorted(buffers), sorted(commits))

            def scoped(records: list[Json]) -> list[Json]:
                try:
                    from tools.matrixark_mcp_local_adapter import _record_scope_hashes
                except ModuleNotFoundError:
                    from matrixark_mcp_local_adapter import _record_scope_hashes
                keep = []
                for record in records:
                    rec_tenant, rec_user = _record_scope_hashes(record)
                    if (not rec_tenant or rec_tenant == tenant_hash) and (not rec_user or rec_user == user_hash):
                        keep.append(record)
                return keep

            expected, got = project(scoped(full)), project(scoped(live_subset))
            try:
                with open(log_path, "a", encoding="utf-8") as log:
                    if expected != got:
                        log.write("MISMATCH %r vs %r\n" % (
                            tuple(len(part) for part in expected),
                            tuple(len(part) for part in got)))
                    else:
                        log.write("CLEAN events=%d buffers=%d commits=%d\n"
                                  % tuple(len(part) for part in expected))
            except OSError:
                pass
            return full
        return live_subset

    def records_for_summary_refresh(self) -> list[Json]:
        """The refresh pass's view from a typed scan, through the SAME serving chain as read_all.

        Chain parity is the whole correctness story: the full read is
        fold(raw + latest-state) -> compact_and_apply_tombstones -> filter_live, and this applies
        exactly that to the subset. The fold's index-posting step is a no-op here because index
        rows are excluded by design and nothing this pass reads consumes them.

        Any failure serves the full read. With MATRIXARK_SHADOW_COMPARE=1 both views are
        computed and compared -- skipping the comparison when the store moved between the two
        reads, because a foreground write landing in the window is a race, not a divergence --
        and the FULL view is served.
        """
        import os as _os

        try:
            from tools.matrixark_mcp_local_adapter import (
                MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
                MEMORY_TOMBSTONE_RECORD_TYPE,
                compact_and_apply_tombstones,
                filter_live_memory_records,
            )
            from tools.matrixark_mcp_serving_records import compact_latest_context_state_records
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_local_adapter import (
                MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
                MEMORY_TOMBSTONE_RECORD_TYPE,
                compact_and_apply_tombstones,
                filter_live_memory_records,
            )
            from matrixark_mcp_serving_records import compact_latest_context_state_records

        shadow_on = _shadow_compare_enabled()
        count_before = None
        if shadow_on:
            try:
                count_before = int(self._get_count())
            except Exception:  # noqa: BLE001
                count_before = None

        wanted = list(self._SUMMARY_REFRESH_RECORD_TYPES) + [
            MEMORY_TOMBSTONE_RECORD_TYPE,
            MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
        ]
        subset = self._scan_records_of_types(wanted)
        if subset is None:
            return self.read_all()
        try:
            latest_state = self._load_latest_context_state_records()
        except Exception as fallback_reason:  # noqa: BLE001 - full read is the fallback.
            _note_full_read_fallback("records_for_summary_refresh", fallback_reason)
            return self.read_all()
        folded = compact_latest_context_state_records(list(subset) + list(latest_state))
        live_subset = filter_live_memory_records(compact_and_apply_tombstones(folded))

        if shadow_on:
            full = self.read_all()
            count_after = None
            try:
                count_after = int(self._get_count())
            except Exception:  # noqa: BLE001
                count_after = None
            log_path = _shadow_log_path("refresh")
            if count_before is None or count_after is None or count_before != count_after:
                try:
                    with open(log_path, "a", encoding="utf-8") as log:
                        log.write("SKIPPED racy window count %r -> %r\n"
                                  % (count_before, count_after))
                except OSError:
                    pass
                return full

            kept = set(wanted)

            def project(records: list[Json]) -> list[tuple]:
                out = []
                for record in records:
                    record_type = str(record.get("record_type") or "")
                    if record_type not in kept:
                        continue
                    identity = ""
                    for field in ("event_id_hash", "summary_hash", "entity_hash", "dirty_hash",
                                  "segment_hash", "node_hash", "ref_hash", "child_hash",
                                  "compression_id_hash", "boundary_hash"):
                        value = record.get(field)
                        if value not in (None, ""):
                            identity = "%s=%s" % (field, value)
                            break
                    out.append((record_type, identity, int(record.get("updated_at_ms") or 0)))
                return out

            expected, got = project(full), project(live_subset)
            if expected != got:
                try:
                    with open(log_path, "a", encoding="utf-8") as log:
                        log.write("MISMATCH full=%d subset=%d only_full=%r only_subset=%r\n" % (
                            len(expected), len(got),
                            [row for row in expected if row not in set(got)][:4],
                            [row for row in got if row not in set(expected)][:4],
                        ))
                except OSError:
                    pass
            else:
                try:
                    with open(log_path, "a", encoding="utf-8") as log:
                        log.write("CLEAN %d records\n" % len(got))
                except OSError:
                    pass
            return full
        return live_subset

    def records_for_delete(self, memory_id: str) -> list[Json]:
        """The id's live records from the same two-round id-scoped fetch get(id) uses.

        Wider type set than get(id): index postings are included because the closure must see
        multi-source derivatives to demote them rather than remove them. Gated on the locator
        coverage marker; without it the full read stands, since a derivative the locator never
        saw would silently keep pointing at a deleted source.
        """
        import os as _os

        if not self._locator_covers_pointed_ids():
            return self.read_all()
        try:
            from tools.matrixark_mcp_local_adapter import (
                MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
                MEMORY_TOMBSTONE_RECORD_TYPE,
                compact_and_apply_tombstones,
                filter_live_memory_records,
            )
            from tools.matrixark_mcp_serving_records import compact_latest_context_state_records
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_local_adapter import (
                MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
                MEMORY_TOMBSTONE_RECORD_TYPE,
                compact_and_apply_tombstones,
                filter_live_memory_records,
            )
            from matrixark_mcp_serving_records import compact_latest_context_state_records

        kept_types = [
            "context_event",
            "context_entity",
            "context_summary",
            "context_summary_dirty",
            "context_segment",
            "context_index",
            MEMORY_TOMBSTONE_RECORD_TYPE,
            MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
        ]
        linked = self._scan_records_of_types(kept_types, record_ids=[str(memory_id)])
        if linked is None:
            return self.read_all()
        identity_ids = {str(memory_id)}
        for record in linked:
            for field in ("entity_hash", "summary_hash", "segment_hash", "event_id_hash"):
                value = record.get(field)
                if value not in (None, "", 0):
                    identity_ids.add(str(value))
        subset = (
            linked
            if len(identity_ids) <= 1
            else self._scan_records_of_types(kept_types, record_ids=sorted(identity_ids))
        )
        if subset is None:
            return self.read_all()
        try:
            latest_state = self._load_latest_context_state_records()
        except Exception as fallback_reason:  # noqa: BLE001 - full read is the fallback.
            _note_full_read_fallback("records_for_delete", fallback_reason)
            return self.read_all()
        folded = compact_latest_context_state_records(list(subset) + list(latest_state))
        live_subset = filter_live_memory_records(compact_and_apply_tombstones(folded))

        if _shadow_compare_enabled():
            full = self.read_all()
            log_path = _shadow_log_path("delete")

            def decisions(records: list[Json]) -> tuple:
                """What delete actually decides from these records: is this a source event, and
                which derivatives are single-source (removed) versus multi-source (demoted)."""
                try:
                    from tools.matrixark_mcp_local_adapter import (
                        _record_derivative_identity_ids,
                        _record_provenance_source_ids,
                        _safe_int,
                    )
                except ModuleNotFoundError:
                    from matrixark_mcp_local_adapter import (
                        _record_derivative_identity_ids,
                        _record_provenance_source_ids,
                        _safe_int,
                    )
                mid_int = _safe_int(str(memory_id))
                is_source = any(
                    str(r.get("record_type") or "") == "context_event"
                    and str(r.get("event_id_hash")) == str(memory_id)
                    for r in records
                )
                single, multi = set(), set()
                for record in records:
                    provenance = _record_provenance_source_ids(record)
                    if provenance is None or mid_int not in provenance:
                        continue
                    ids = _record_derivative_identity_ids(record)
                    if provenance == {mid_int}:
                        single |= set(ids)
                    else:
                        multi |= set(ids)
                return (is_source, tuple(sorted(map(str, single))), tuple(sorted(map(str, multi))))

            expected, got = decisions(full), decisions(live_subset)
            try:
                with open(log_path, "a", encoding="utf-8") as log:
                    if expected != got:
                        log.write("MISMATCH id=%s full=%r subset=%r\n" % (memory_id, expected, got))
                    else:
                        log.write("CLEAN id=%s source=%s single=%d multi=%d\n"
                                  % (memory_id, expected[0], len(expected[1]), len(expected[2])))
            except OSError:
                pass
            return full
        return live_subset

    def _locator_covers_pointed_ids(self) -> bool:
        """True once the store's locator has indexed pointed ids since its FIRST append.

        Sticky per process: the marker is stamped at store birth and never unset, so one positive
        read is authoritative for the store's lifetime.
        """
        if getattr(self, "_locator_pointed_marker", False):
            return True
        try:
            rows = self._client.scan_hash(
                f"{self._storage_prefix}:context_ref_locator_meta").get("records") or []
        except Exception:  # noqa: BLE001
            return False
        covered = any(
            str(row.get("field")) == "provenance_from_start" and str(row.get("value")).strip() == "1"
            for row in rows if isinstance(row, dict)
        )
        if covered:
            self._locator_pointed_marker = True
        return covered

    def records_for_get_memory(self, memory_id: str) -> list[Json]:
        """One id's live view from an id-scoped scan, on stores whose locator can answer it.

        Engages ONLY when the provenance_from_start marker attests that pointed-id indexing was
        active for every record this store ever held -- on any other store the locator cannot
        enumerate the id's derivatives, and the id-scoped read would silently drop them (the
        failure the shadow harness caught on the first attempt). The subset runs the SAME chain
        as the full read -- latest-state fold, tombstone sweep, expiry filter -- so a deleted
        memory still answers {found: false}.
        """
        import os as _os

        if not self._locator_covers_pointed_ids():
            return self.read_all()
        try:
            from tools.matrixark_mcp_local_adapter import (
                MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
                MEMORY_TOMBSTONE_RECORD_TYPE,
                compact_and_apply_tombstones,
                filter_live_memory_records,
            )
            from tools.matrixark_mcp_serving_records import compact_latest_context_state_records
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_local_adapter import (
                MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
                MEMORY_TOMBSTONE_RECORD_TYPE,
                compact_and_apply_tombstones,
                filter_live_memory_records,
            )
            from matrixark_mcp_serving_records import compact_latest_context_state_records

        kept_types = [
            "context_event",
            "context_entity",
            "context_summary",
            "context_summary_dirty",
            "context_segment",
            MEMORY_TOMBSTONE_RECORD_TYPE,
            MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
        ]
        # Round 1 discovers WHICH derivative identities this memory is involved with.
        linked = self._scan_records_of_types(kept_types, record_ids=[str(memory_id)])
        if linked is None:
            return self.read_all()
        # Round 2 fetches every VERSION of those identities. Derivatives are last-writer-wins by
        # identity, so a subset holding only the versions that still LINK to this id would let a
        # superseded copy win compaction and resurface -- the shadow caught exactly that. One
        # scan (not a union of two) so the records come back in append order, which is what
        # last-writer-wins needs to pick the same winner the full read picks.
        identity_ids = {str(memory_id)}
        for record in linked:
            for field in ("entity_hash", "summary_hash", "segment_hash", "event_id_hash"):
                value = record.get(field)
                if value not in (None, "", 0):
                    identity_ids.add(str(value))
        subset = (
            linked
            if len(identity_ids) <= 1
            else self._scan_records_of_types(kept_types, record_ids=sorted(identity_ids))
        )
        if subset is None:
            return self.read_all()
        try:
            latest_state = self._load_latest_context_state_records()
        except Exception as fallback_reason:  # noqa: BLE001 - full read is the fallback.
            _note_full_read_fallback("records_for_get_memory", fallback_reason)
            return self.read_all()
        folded = compact_latest_context_state_records(list(subset) + list(latest_state))
        live_subset = filter_live_memory_records(compact_and_apply_tombstones(folded))

        if _shadow_compare_enabled():
            full = self.read_all()
            log_path = _shadow_log_path("getmem")

            def project(records: list[Json]) -> tuple:
                try:
                    from tools.matrixark_mcp_local_adapter import (
                        _record_provenance_source_ids,
                        _safe_int,
                    )
                except ModuleNotFoundError:
                    from matrixark_mcp_local_adapter import (
                        _record_provenance_source_ids,
                        _safe_int,
                    )
                mid_int = _safe_int(str(memory_id))
                event_seen = None
                derived = []
                for record in records:
                    rtype = str(record.get("record_type") or "")
                    if rtype == "context_event" and str(record.get("event_id_hash")) == str(memory_id):
                        event_seen = str(record.get("event_id_hash"))
                        continue
                    if mid_int is not None:
                        prov = _record_provenance_source_ids(record)
                        if prov is not None and mid_int in prov:
                            derived.append((rtype, str(record.get("entity_hash")),
                                            str(record.get("summary_hash"))))
                return (event_seen, sorted(derived))

            expected, got = project(full), project(live_subset)
            try:
                with open(log_path, "a", encoding="utf-8") as log:
                    if expected != got:
                        log.write("MISMATCH id=%s full=%r subset=%r\n"
                                  % (memory_id, expected, got))
                    else:
                        log.write("CLEAN id=%s derived=%d\n" % (memory_id, len(expected[1])))
            except OSError:
                pass
            return full
        return live_subset

    def raw_records_for_history(self, memory_id: str | None = None) -> list[Json]:
        """History's records from a three-type scan instead of a raw read of the whole log.

        The raw read hauls the entire store -- summaries, embeddings, index postings -- to answer
        one id, and it grows with the store. The scan returns the three types history reports, in
        append order, which is the order history walks. Duplicate event rows need no special care:
        history collapses them to one "ingested" entry either way. Any failure falls back to the
        raw read.
        """
        try:
            from tools.matrixark_mcp_local_adapter import MEMORY_TOMBSTONE_RECORD_TYPE
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_local_adapter import MEMORY_TOMBSTONE_RECORD_TYPE
        subset = self._scan_records_of_types(
            [
                "context_event",
                MEMORY_TOMBSTONE_RECORD_TYPE,
                self.MEMORY_FEEDBACK_RECORD_TYPE,
            ],
            record_ids=[str(memory_id)] if memory_id else None,
        )
        if subset is None:
            return super().raw_records_for_history(memory_id)
        return subset

    def prior_context_records(self, scope: Json | None = None) -> list[Json]:
        """The prior-context view from a five-type scan instead of a full-store read.

        `collect_prior_context` and the caller-supplied-fields carry-over consume three record
        types; tombstones and retention-cutoff markers are fetched alongside so the LIVE-view
        semantics reproduce on the subset via the SAME functions the full read applies --
        `compact_and_apply_tombstones` then `filter_live_memory_records`. The scan returns append
        order, which is what the order-aware sweep and the newest-first consumers need.

        Any failure falls back to the full read. With MATRIXARK_SHADOW_COMPARE=1
        both views are computed, projected to what the consumers can see, compared, and the FULL
        view is served -- the scan path earns trust shadowed over live traffic first.
        """
        import os as _os

        try:
            from tools.matrixark_mcp_local_adapter import (
                MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
                MEMORY_TOMBSTONE_RECORD_TYPE,
                compact_and_apply_tombstones,
                filter_live_memory_records,
            )
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_local_adapter import (
                MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
                MEMORY_TOMBSTONE_RECORD_TYPE,
                compact_and_apply_tombstones,
                filter_live_memory_records,
            )
        pinned_hashes: tuple[int, int] | None = None
        engine_scope: Json = {}
        if isinstance(scope, dict) and scope:
            try:
                tenant_hash, user_hash = self._resolve_subject_hashes(scope)
            except Exception:  # noqa: BLE001
                tenant_hash, user_hash = 0, 0
            if tenant_hash and user_hash:
                pinned_hashes = (tenant_hash, user_hash)
                engine_scope = {
                    "tenant_hash": tenant_hash,
                    "user_hash": user_hash,
                    "_explicit_scope_keys": ["tenant_id", "user_id"],
                }
                if scope.get("user_id"):
                    engine_scope["user_id"] = str(scope.get("user_id"))
        # `collect_prior_context` walks these newest-first and STOPS at MAX_PRIOR_MESSAGES, so
        # every event older than that window is fetched, decoded and discarded. Measured on a
        # subject with 125 memories: 436 records and 2.65 MB fetched, 184 ms, to select eight.
        #
        # The cap is on `context_event` ONLY. Tombstones and retention cutoffs are what make the
        # subset reproduce LIVE-view semantics, and summaries are what the payload is built from --
        # capping those would drop deleted memories back into view. The window is far wider than
        # the eight actually consumed because the consumer filters by session and scope after the
        # fetch, and it counts only matches; the margin is what keeps a busy multi-session subject
        # seeing the same eight it saw before.
        newest_events = _prior_context_event_window()
        wanted_types = [
            "context_event",
            "context_summary",
            "context_pack_audit",
            MEMORY_TOMBSTONE_RECORD_TYPE,
            MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
        ]
        engine_scope_arg = engine_scope if pinned_hashes else None

        # `collect_prior_context` walks these newest-first and STOPS at the eighth event that
        # matches the request's session or scope. The window exists for the subject that
        # interleaves sessions, where the eight matches are scattered among many events -- but on
        # the common subject the eight newest events ARE the eight matches, and fetching 256 to
        # hand over a list the consumer abandons after eight is the waste. Measured at steady
        # state on a 300-memory subject: 384 records and 1.32 MB fetched per add, 89 ms, of which
        # 256 events were 901 KB.
        #
        # So probe with a small window first and escalate only when it cannot satisfy the
        # consumer. The test is deliberately STRICTER than the consumer's: it counts only events
        # matching by scope, while the consumer also accepts a session-key match. Undercounting
        # escalates and costs a second fetch; it can never serve fewer matches than the consumer
        # would have found.
        probe_events = _prior_context_probe_window()
        subset = None
        if probe_events and newest_events and probe_events < newest_events:
            probe = self._scan_records_of_types(
                wanted_types,
                scope=engine_scope_arg,
                newest_by_type={"context_event": probe_events},
            )
            if probe is not None and _prior_context_probe_is_enough(probe, scope):
                subset = probe
        if subset is None:
            subset = self._scan_records_of_types(
                wanted_types,
                scope=engine_scope_arg,
                newest_by_type=({"context_event": newest_events} if newest_events else None),
            )
        if subset is None:
            return self.read_all()
        # Summaries and other compact records can live in the latest-state HASH rather than the
        # append log, and the scan walks only the log -- the shadow compare caught exactly this:
        # the subset was missing every refresher-written context_summary. Fold the latest-state
        # records in, filtered to the same types, in the same position the full read gives them
        # (after the log records).
        kept_types = {
            "context_event", "context_summary", "context_pack_audit",
            MEMORY_TOMBSTONE_RECORD_TYPE, MEMORY_RETENTION_CUTOFF_RECORD_TYPE,
        }
        try:
            latest_state = self._load_latest_context_state_records()
        except Exception as fallback_reason:  # noqa: BLE001 - full read is the fallback.
            _note_full_read_fallback("prior_context_records", fallback_reason)
            return self.read_all()
        try:
            from tools.matrixark_mcp_serving_records import compact_latest_context_state_records
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_serving_records import compact_latest_context_state_records
        # Fold latest-state the way the full read does rather than appending it after the log
        # records: prior-context consumers are order-aware, and this is the assembly that already
        # earned a clean shadow on get_all.
        folded_prior = compact_latest_context_state_records(list(subset) + [
            record for record in latest_state
            if isinstance(record, dict) and str(record.get("record_type") or "") in kept_types
        ])
        live_subset = filter_live_memory_records(compact_and_apply_tombstones(folded_prior))

        if _shadow_compare_enabled():
            # prior-context shadow: skip a racy window. The two views are computed in sequence,
            # so a write landing between them differs in length with every compared position
            # equal -- a race, not a divergence. Read the count around the pair and skip when it
            # moved; a skipped window is never counted as clean.
            def _race_marks() -> tuple:
                try:
                    count = int(self._get_count())
                except Exception:  # noqa: BLE001
                    return (None, None)
                try:
                    latest = len(self._load_latest_context_state_records())
                except Exception:  # noqa: BLE001
                    return (count, None)
                return (count, latest)

            count_before, latest_before = _race_marks()
            full = self.read_all()
            count_after, latest_after = _race_marks()
            if (count_before is None or count_after is None
                    or latest_before is None or latest_after is None
                    or count_before != count_after or latest_before != latest_after):
                try:
                    path = _shadow_log_path("prior_context")
                    with open(path, "a", encoding="utf-8") as log:
                        log.write("SKIPPED racy window count %r -> %r\n"
                                  % (count_before, count_after))
                except OSError:
                    pass
                return full

            def project(records: list[Json]) -> list[tuple]:
                keep = {"context_event", "context_summary", "context_pack_audit"}
                try:
                    from tools.matrixark_mcp_local_adapter import _record_scope_hashes
                except ModuleNotFoundError:
                    from matrixark_mcp_local_adapter import _record_scope_hashes
                out = []
                for record in records:
                    record_type = str(record.get("record_type") or "")
                    if record_type not in keep:
                        continue
                    if pinned_hashes is not None:
                        rec_tenant, rec_user = _record_scope_hashes(record)
                        if (rec_tenant and rec_tenant != pinned_hashes[0]) or (
                            rec_user and rec_user != pinned_hashes[1]
                        ):
                            continue
                    out.append((
                        record_type,
                        str(record.get("event_id_hash") or record.get("summary_hash")
                            or record.get("context_pack_id") or ""),
                        int(record.get("updated_at_ms") or 0),
                    ))
                return out

            expected, got = project(full), project(live_subset)
            if expected != got:
                try:
                    path = _shadow_log_path("prior_context")
                    with open(path, "a", encoding="utf-8") as log:
                        first_div = next((i for i, (a, b) in enumerate(zip(expected, got)) if a != b), -1)
                        log.write("MISMATCH n=%d div@%d full=%r subset=%r ctx_full=%r ctx_sub=%r\n" % (
                            len(expected), first_div,
                            expected[first_div] if 0 <= first_div < len(expected) else None,
                            got[first_div] if 0 <= first_div < len(got) else None,
                            expected[max(0, first_div - 1):first_div + 2],
                            got[max(0, first_div - 1):first_div + 2],
                        ))
                except OSError:
                    pass
            return full
        return live_subset

    def surviving_ids_for_pending_events(self, pending: list[Json]) -> set[str] | None:
        """Decide the guard from the tombstones alone, without reading the whole raw log.

        Per pending event, against each scanned tombstone that `_tombstone_kills_record` says
        matches it:

        * `delete` kind: the id is unique to one ingest and the tombstone was written to remove
          it, so the tombstone postdates the event by construction -- matching alone kills it.
        * `forget`/`reset` kind: only records PRECEDING the tombstone die, and a re-ingest after a
          forget must survive. Order comes from timestamps; a strict comparison decides, and a tie
          or a missing timestamp makes the event AMBIGUOUS -- the whole commit then takes the full
          raw read, because guessing either way is a real bug (resurrected deleted content one
          way, silently unextracted memory the other).

        Returns None for "keep everything" so the caller's contract is unchanged.
        """
        if not pending:
            return None
        tombstones = self._memory_tombstone_records()
        if tombstones is None:
            return super().surviving_ids_for_pending_events(pending)
        if not tombstones:
            return None
        try:
            from tools.matrixark_mcp_local_adapter import _tombstone_kills_record
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_local_adapter import _tombstone_kills_record

        def event_time_ms(record: Json) -> int:
            # The same resolution order the commit path uses for pending events.
            try:
                return int(
                    (record.get("envelope") or {}).get("ingestion_time_ms")
                    or record.get("updated_at_ms")
                    or record.get("timestamp_key_ms")
                    or 0
                )
            except (TypeError, ValueError):
                return 0

        surviving: set[str] = set()
        for event in pending:
            if not isinstance(event, dict):
                continue
            event_id = str(event.get("event_id_hash") or "")
            if not event_id:
                continue
            killed = False
            for tombstone in tombstones:
                if not _tombstone_kills_record(tombstone, event):
                    continue
                kind = str(tombstone.get("tombstone_kind") or "")
                if kind == "delete":
                    killed = True
                    break
                event_ms = event_time_ms(event)
                try:
                    tombstone_ms = int(tombstone.get("created_at_ms") or 0)
                except (TypeError, ValueError):
                    tombstone_ms = 0
                if not event_ms or not tombstone_ms or event_ms == tombstone_ms:
                    # Cannot order them: ambiguous, so this commit takes the full, correct read.
                    return super().surviving_ids_for_pending_events(pending)
                if tombstone_ms > event_ms:
                    killed = True
                    break
            if not killed:
                surviving.add(event_id)
        return surviving

    def _purge_scope_in_engine(self, scope: Json) -> Json:
        """Ask the engine to physically remove every record matching `scope`.

        Shared by forget (one subject) and reset (the whole tenant). The engine refuses an
        under-specified scope -- it requires a non-zero tenant_hash or an explicit subject
        dimension -- so an empty scope cannot be turned into "delete everything" by accident.
        """
        purger = getattr(self._client, "matrixark_forget_scope", None)
        if not callable(purger):
            return {"ok": False, "error": "engine does not expose matrixark_forget_scope"}
        try:
            purged = purger(
                count_key=self._count_key,
                record_hash_key=self._record_hash_key,
                shard_size=self._shard_size,
                scope=scope,
            )
        except Exception as exc:  # noqa: BLE001 - the tombstone stands; report the engine half.
            return {"ok": False, "error": str(exc)}
        self._invalidate_summary_pass_state()
        self._drop_direct_record_cache()
        return {
            "ok": True,
            "records_removed": purged.get("matrixark_forget_records_removed"),
            "fields_deleted": purged.get("matrixark_forget_fields_deleted"),
            "fields_rewritten": purged.get("matrixark_forget_fields_rewritten"),
            "shards_scanned": purged.get("matrixark_forget_shards_scanned"),
        }

    def _purge_record_ids_in_engine(self, ids: list[str]) -> Json | None:
        """Remove the named records in the engine. Returns None when there is nothing to remove.

        Shared by delete and update. Both address a set of records the caller has already decided
        on -- an empty set means remove nothing, never "remove everything".
        """
        ids = [str(item) for item in (ids or []) if str(item)]
        if not ids:
            return None
        purger = getattr(self._client, "matrixark_delete_records", None)
        if not callable(purger):
            return {"ok": False, "error": "engine does not expose matrixark_delete_records"}
        try:
            purged = purger(
                count_key=self._count_key,
                record_hash_key=self._record_hash_key,
                shard_size=self._shard_size,
                record_ids=ids,
            )
        except Exception as exc:  # noqa: BLE001 - the tombstone stands; report the engine half.
            return {"ok": False, "error": str(exc)}
        self._drop_direct_record_cache()
        return {
            "ok": True,
            "records_removed": purged.get("matrixark_delete_records_removed"),
            "fields_deleted": purged.get("matrixark_delete_fields_deleted"),
            "fields_rewritten": purged.get("matrixark_delete_fields_rewritten"),
            "ids_requested": purged.get("matrixark_delete_ids_requested"),
            # How much the purge actually READ, not just what it removed. Without this the
            # caller cannot tell a cheap purge from an expensive one: measured on one subject
            # with 600 memories, a single update decodes ~11,400 records to remove five.
            "records_scanned": purged.get("matrixark_delete_records_scanned"),
            # ...and how that read decomposed. records_scanned mixes two costs with two different
            # fixes: how many fields the purge opened, and how many records were sitting in each.
            # fields_without_match separates them further -- a field opened and then discarded was
            # pointed at by the located set but held none of the ids.
            "fields_visited": purged.get("matrixark_delete_fields_visited"),
            "fields_without_match": purged.get("matrixark_delete_fields_without_match"),
            # Of those, the ones still holding a record that POINTS AT one of the ids. Deletion
            # reaches a derivative through that link, so such a location is correctly filed and
            # cannot be dropped; what is left over is the part that relates to the ids not at all.
            "fields_pointed_only": purged.get("matrixark_delete_fields_pointed_only"),
        }

    def update_memory(self, args: Json, hook: Json | None = None) -> Json:
        """Remove the superseded version from the ENGINE as well as from the serving view.

        An update is a supersede: the new text is ingested and the old id tombstoned. `get_all`
        honours that immediately. `/v1/retrieve` is assembled inside the engine, which has never
        heard of a tombstone, so the OLD text kept being served -- and because it outranked the new
        one, a search after a successful update returned the stale value and not the new value at
        all:

            after update:  get_all = the new text
                           retrieve = the OLD text, and the new text nowhere in the results

        That is worse than a failed update, because the caller is told it succeeded. The inherited
        implementation already computes exactly which records the old version covered, and its own
        comment says the sweep exists "so the old text can't leak via retrieval after the update" --
        the engine simply never learned about it.
        """
        result = super().update_memory(args, hook)
        # `finalize` is honoured by the DISPATCH layer, which runs session_commit as a second tool
        # call; `adapter.ingest()` ignores it. update re-ingests through the adapter directly, so
        # on a native backend the replacement stayed at `extraction_phase: hot_path` /
        # `status: observed` forever -- read straight out of the engine it had no node_path, while
        # an ordinary ingest in the same scope reached final/extraction_committed. Retrieval there
        # serves committed content, so the updated memory was in `get_all` and in no search, while
        # the value it replaced had been committed and was still being served.
        #
        # Only the native path needs this. On the JSONL backend the re-ingest is already
        # retrievable and committing there BREAKS it -- doing this in the shared implementation
        # turned `test_update_supersede_retrieve_returns_new` into an empty context pack.
        reingest_scope = result.get("reingest_scope")
        if isinstance(reingest_scope, dict) and reingest_scope:
            try:
                self.session_commit({"scope": reingest_scope, "force": True,
                                     "commit_reason": "update_supersede"}, hook=hook)
            except Exception as exc:  # noqa: BLE001 - durably stored; the idle commit still closes it.
                result["commit_error"] = str(exc)
        purged = self._purge_record_ids_in_engine(result.get("closure_ref_ids") or [])
        if purged is not None:
            result["engine_purge"] = purged
        return result

    def delete_memory(self, args: Json, hook: Json | None = None) -> Json:
        """Delete the memory in the ENGINE as well as in the serving view.

        Same hole forget and reset had: the tombstone is honoured by every read that goes through
        the Python serving pipeline, so `get_all` drops immediately, but `/v1/retrieve` is assembled
        inside the engine and the engine has never heard of a tombstone. Measured before this: after
        a delete, get_all went 2 -> 1 while retrieve still served the deleted memory.

        The identity set comes from the inherited implementation rather than being re-derived here.
        Which records a delete covers is genuinely subtle -- the addressed event, its single-source
        derivatives, and the embeddings/postings pointing at any of them, while MULTI-source
        derivatives are demoted rather than removed -- and deciding it twice, in two languages, is
        how the two copies drift.
        """
        result = super().delete_memory(args, hook)
        purged = self._purge_record_ids_in_engine(result.get("closure_ref_ids") or [])
        if purged is not None:
            result["engine_purge"] = purged
        return result

    def reset(self, args: Json, hook: Json | None = None) -> Json:
        """Wipe the tenant in the ENGINE as well as in the serving view.

        Same defect as forget, same cause: the inherited implementation writes a tombstone that
        only the Python serving pipeline honours, so `get_all` returned 0 while `/v1/retrieve` --
        assembled inside the engine -- went on serving the tenant's memories verbatim. Measured
        before this: get_all=0 and the reset canary still retrievable.

        Scoped to the caller's tenant, never wider. The engine's own guard would refuse an
        under-specified scope anyway, but the tenant hash is resolved here rather than trusting
        whatever the request carried.
        """
        result = super().reset(args, hook)
        scope = dict(optional_object(args, "scope"))
        tenant_hash, _ = self._resolve_subject_hashes(scope)
        if not tenant_hash:
            return result
        # Tenant-wide on purpose: drop the user/session dimensions so this matches every record in
        # the tenant, which is what reset means -- and keep the tenant hash, which is what stops it
        # matching anything else.
        engine_scope = {k: v for k, v in scope.items()
                        if k not in ("user_id", "session_id", "agent_id", "user_hash",
                                     "session_hash", "scope_key", "_explicit_scope_keys")}
        engine_scope["tenant_hash"] = tenant_hash
        result["engine_purge"] = self._purge_scope_in_engine(engine_scope)
        return result

    def forget(self, args: Json, hook: Json | None = None) -> Json:
        """Forget the subject in the ENGINE as well as in the serving view.

        The inherited implementation writes a durable tombstone, and every read that goes through
        the Python serving pipeline honours it -- `get_all` returns 0 immediately. `/v1/retrieve`
        does not go through that pipeline: on a native backend the engine assembles the context
        pack itself, and the engine has no idea the tombstone exists. So a forgotten memory kept
        coming back from retrieve, verbatim, while every check that asked `/v1/memories` reported
        a clean wipe:

            before forget:  get_all=2, retrieve contains the subject's secret = True
            forget:         http 200, removed_count 54
            after forget:   get_all=0, retrieve contains the secret = STILL True

        Deleting data has to mean deleting it on every read path. The engine has had
        `matrixark_forget_scope` all along -- it removes the subject's records, refuses an
        under-specified scope that would match everything, commits one durable batch, and clears
        its scan caches so a later retrieve cannot re-serve from cache -- and nothing called it.

        The tombstone is still written first, and deliberately: it is the durable, auditable record
        of the forget, it makes the serving view correct even if the engine call fails, and its
        order-aware semantics are what let a subject be re-ingested afterwards. The engine purge is
        what makes the deletion real.
        """
        result = super().forget(args, hook)
        try:
            from tools.matrixark_mcp_core_identity import identity_hashes
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_core_identity import identity_hashes
        scope = dict(optional_object(args, "scope"))
        # The engine matches records by scope, so it needs the SUBJECT's own hashes rather than
        # whatever the caller's request happened to carry.
        user_id = str(scope.get("user_id") or "")
        if user_id:
            scope.update(identity_hashes(str(scope.get("account_id") or ""),
                                         str(scope.get("tenant_id") or ""), user_id=user_id))
        result["engine_purge"] = self._purge_scope_in_engine(scope)
        return result

    def _idle_commit_candidate_records(self, scope: Json) -> list[Json]:
        """Ask the engine for the pipeline tasks instead of reading the whole log.

        Safe to narrow because the drain uses these records for nothing else: both of its loops
        skip anything that is not a `matrixark_async_pipeline_task`.

        Order is what makes it equivalent, and it survives. The drain decides last-write-wins from
        list position, and the scan's `compact_latest_context_state_records` keys only
        `context_summary`, `context_model_registry` and some `context_embedding` rows -- a pipeline
        task gets no key, so it passes through untouched -- and the function re-sorts by the
        original index, so append order is preserved either way.

        A scope is required. `idle_commit_task_records({})` degenerates to a cross-scope full-store
        scan, which is the cost this exists to avoid; without one, fall back to the inherited read.
        """
        if not scope:
            return super()._idle_commit_candidate_records(scope)
        scanner = getattr(self, "idle_commit_task_records", None)
        if not callable(scanner):
            return super()._idle_commit_candidate_records(scope)
        try:
            return scanner(scope)
        except Exception:  # noqa: BLE001 - a scan failure must not stop the drain.
            return super()._idle_commit_candidate_records(scope)

    def read_all(self) -> list[Json]:
        """Read through the native record log, not MatrixArkLocalAdapter's JSONL log.

        `MatrixArkLocalAdapter` precedes the direct mixins in this class's MRO and also
        defines `read_all`, so its JSONL implementation won here -- and on a native backend
        there is no JSONL log, so it returned ZERO records. Every reader built on read_all
        (get / get_all / update / history / keyed recall) therefore read empty while the
        records sat durably in the record log; the same inherited method is correct on the
        JSONL backend, which is why only the native backends looked broken.

        Only `read_all` collided: `read_all_without_disk_fallback_recovery` is defined solely
        on `_TemporalDirectReadMixin` and already resolved correctly, so delegating to it
        reproduces the direct read exactly without reordering bases (which would silently
        move every other method both classes define).

        The live-view post-processing MUST be kept: `MatrixArkLocalAdapter.read_all` does not
        just read, it replays persisted tenant policies and then filters the log down to live
        records. Delegating to the raw direct read alone silently drops both -- forget/delete
        tombstones stop being honoured (the subject's records stay visible after a forget) and
        a TTL record never expires, because expiry is enforced on read and is never cached.
        """
        try:
            from tools.matrixark_mcp_local_adapter import (
                compact_and_apply_tombstones,
                filter_live_memory_records,
            )
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_local_adapter import (
                compact_and_apply_tombstones,
                filter_live_memory_records,
            )
        self._recover_serving_from_disk_fallback_if_needed(reason="read_all")
        records = self.read_all_without_disk_fallback_recovery()
        self._register_persisted_tenant_policies(records)
        # All three serving stages, in the order every other read path uses. This ran only the
        # last of them: expiry and retention were applied, compaction and tombstones were not. A
        # forget wrote its tombstone, reported an accurate removed_count, and then served every
        # one of those records straight back on the next read.
        return filter_live_memory_records(compact_and_apply_tombstones(records))

    def _register_persisted_tenant_policies(self, records: list[Json]) -> None:
        """Absorb tenant and user policy rows from the store, so a policy written by another
        process -- or by an earlier run -- applies to this reader.

        This was called here but never defined, so every read through this adapter raised
        AttributeError. Five tests in test_mem0_native_read_path failed on it, each on its first
        `read_all`.

        The functions it needs already existed and had no caller: a policy row could be written and
        would be read back as an ordinary record, but nothing turned it into an active policy. So
        the call was right and only the wiring was missing.

        Best effort by design: a malformed policy row in the store must not make the store
        unreadable. It is skipped, and the records are still served.
        """
        if not records:
            return
        try:
            from tools import matrixark_tenant_policy as tenant_policy
        except ModuleNotFoundError:  # Direct script execution from tools/.
            import matrixark_tenant_policy as tenant_policy
        try:
            tenant_policy.register_tenant_policy_records(records)
            tenant_policy.register_user_policy_records(records)
        except (TypeError, ValueError, KeyError, AttributeError):
            return

    def append(self, record: Json) -> None:
        self.append_many([record])

    def append_many(self, records: list[Json]) -> None:
        # TODO(engine): record-metadata interning (see encode_interned_records in
        # matrixark_mcp_local_adapter) is intentionally NOT applied on the backend write path. Unlike
        # the pure-local JSONL log -- where the codec sits entirely at the Python (de)serialization
        # boundary and every read choke point re-expands -- the native backend consumes storage_route /
        # placement_key for real routing and placement decisions, so replacing those with interned
        # tokens here would hide routing metadata from the engine, and the inverse expansion would have
        # to live inside the native layer (out of scope; do not touch crates). Lever 2 (index-dimension
        # pruning) DOES apply to the backend because it prunes shared candidate_index_terms upstream of
        # this writer, so fewer postings are materialized into the engine as well.
        #
        # Persist serving records to the durable TemporalStore backend.
        #
        # MatrixArkLocalAdapter.append/append_many only mirror records into a
        # local JSONL event log, which backend adapters disable (their event_log
        # is the "-unused-" sentinel, so _local_jsonl_enabled is False). Because
        # MatrixArkLocalAdapter is FIRST in this class's MRO, those no-op writers
        # otherwise win and every serving record produced by the MCP ingest /
        # session-commit path is silently dropped (backend records_written == 0),
        # leaving retrieval permanently empty. Route through the canonical
        # _append_many_materialized backend writer instead so records actually
        # land in the store and become retrievable on later turns. The pure-local
        # JSONL adapter is unaffected (it keeps _local_jsonl_enabled and is not
        # this class); the fast direct-ingest path calls _append_many_materialized
        # itself (never append), so there is no double write. There is no switch off: what the
        # off position restored is the state described below, where an ingest carrying a
        # ttl_seconds stored a record nothing would ever expire.
        #
        # Per-ingestion stamping runs HERE, in the same order as
        # MatrixArkLocalAdapter.append_many, because it was being skipped entirely on every
        # native backend. `_stamp_ingest_fields` is what puts `expires_at_ms`/`ephemeral` and
        # `identity_key`/`truth_class`/`truth_rank` onto the records; without it an
        # `ttl_seconds` ingest stored a record that nothing would ever expire, an
        # `identity_key` ingest stored a record keyed recall could not find (404) and
        # keyed-upsert never superseded, and a tenant that switched a record kind off still
        # had it written. Policy runs on the RAW records, before serving materialization
        # strips the scope a context_embedding row carries -- afterwards there is no tenant
        # left to attribute it to.
        #
        # `_apply_serving_dedup` is deliberately NOT applied here: its summary-dirty
        # coalescing calls read_all(), which on a native backend is a full record-log read on
        # every append batch. It only removes redundant pending markers -- a size
        # optimization, not correctness -- and paying an O(store) read per ingest to get it
        # is the wrong trade on this path.
        # The raw half of the dual write. _TemporalDirectBackendMixin.append_many performs it,
        # but this override wins the MRO and did not, so on the default backend the raw records
        # were written NOWHERE: the only other caller, _flush_direct_write_items, runs on the
        # queue path, and queuing is off by default. Gated rather than simply restored, because
        # this adapter is the default backend and _append_raw_ingestion_records has no gate of
        # its own -- calling it unconditionally would add a second store write to every ingest.
        # Default off keeps today's behaviour byte for byte; turning it on is one flag.
        if bool(getattr(self, "_direct_raw_ingestion_enabled", False)):
            self._append_raw_ingestion_records(records)
        materialized = self._stamp_ingest_fields(materialize_serving_record_batch(records))
        if not materialized:
            return
        # Index subjects from the RAW records: serving materialization strips the scope off some
        # rows, and a subject with no scope left cannot be attributed to anyone.
        self._index_memory_subjects(records)
        if self._queue_batched_records(materialized):
            return
        append_backend = getattr(self, "_append_many_materialized", None)
        route_to_backend = (
            callable(append_backend)
            and not getattr(self, "_local_jsonl_enabled", False)
        )
        if route_to_backend:
            append_backend(materialized, allow_queue=False)
            self._update_latest_entity_cache(materialized)
            self._maintain_event_membership_after_append(materialized)
            return
        super().append_many(records)

    # ------------------------------------------------------------------------------------------------
    # Event-membership index -- durable engine backing (``{prefix}:event_members`` hash).
    # field = event_id_hash, value = json(sorted member identity hashes). hset on append (write-through
    # the batch's additions), hget on delete (O(1) member lookup, no rescan), soft-clear on delete.
    # This overrides the LOCAL adapter's in-memory-only seam so a delete on the engine never scans; the
    # in-memory index + scan fallback remain as correctness backstops. Best-effort: a backend hiccup
    # never breaks the write/delete path (the in-memory rebuild-from-live-view still yields a complete
    # member set). A native ``event_members`` secondary index in the engine is future crates work.
    # ------------------------------------------------------------------------------------------------
    def _event_members_hash_key(self) -> str:
        prefix = str(getattr(self, "_storage_prefix", "matrixark:mcp")).rstrip(":")
        return f"{prefix}:event_members"

    def _lookup_persisted_event_members(self, event_id: str) -> set[str] | None:
        try:
            raw = self._client.hget(self._event_members_hash_key(), str(event_id))
        except Exception:  # noqa: BLE001 - never let a backend read break delete; fall back to in-memory
            return None
        if not raw:
            return None
        try:
            values = json.loads(raw)
        except (ValueError, TypeError):
            return None
        if not isinstance(values, list):
            return None
        return {str(v) for v in values}

    def _lookup_persisted_event_members_many(self, event_ids: list[str]) -> dict[str, set[str]]:
        """Read many membership entries in one round trip.

        Every entry lives under the same hash key, so a single ``batch_hget`` replaces one read
        per event. An id the batch response does not answer for is read singly rather than
        treated as absent -- treating it as absent would let a partial response overwrite a
        persisted member set with a SMALLER one, and membership is cumulative across the async
        pipeline. Absent stays absent (omitted from the result), matching the single-key
        contract that the caller reads with ``or set()``.
        """
        found: dict[str, set[str]] = {}
        wanted = [str(event_id) for event_id in event_ids]
        if not wanted:
            return found
        key = self._event_members_hash_key()
        entries = [{"key": key, "field": event_id} for event_id in wanted]
        try:
            rows = self._client.batch_hget(entries)
        except Exception:  # noqa: BLE001 - never let a backend read break the write path
            rows = []
        raw_by_field: dict[str, str] = {}
        for index, row in enumerate(rows if isinstance(rows, list) else []):
            field = ""
            raw = ""
            if isinstance(row, dict):
                field = str(row.get("field") or "")
                raw = str(row.get("value") or "")
            elif isinstance(row, str):
                raw = row
            if not field and index < len(entries):
                field = str(entries[index]["field"])
            if field:
                raw_by_field[field] = raw
        for event_id in wanted:
            if event_id in raw_by_field:
                members = self._decode_event_members(raw_by_field[event_id])
            else:
                # Not answered by the batch: read it singly rather than assume absent.
                members = self._lookup_persisted_event_members(event_id)
            if members is not None:
                found[event_id] = members
        return found

    @staticmethod
    def _decode_event_members(raw: str) -> set[str] | None:
        if not raw:
            return None
        try:
            values = json.loads(raw)
        except (ValueError, TypeError):
            return None
        if not isinstance(values, list):
            return None
        return {str(v) for v in values}

    def _persist_event_members(self, event_id: str, member_ids: set[str]) -> None:
        try:
            self._hset_with_backoff(
                self._event_members_hash_key(),
                str(event_id),
                json.dumps(sorted(str(m) for m in member_ids), separators=(",", ":")),
            )
        except Exception:  # noqa: BLE001 - best-effort; in-memory index remains authoritative
            return None

    def _forget_persisted_event_members(self, event_id: str) -> None:
        # No hdel primitive on the client; an empty value reads back as "absent" (see _lookup).
        try:
            self._hset_with_backoff(self._event_members_hash_key(), str(event_id), "")
        except Exception:  # noqa: BLE001
            return None

    def _maintain_event_membership_after_append(self, records: list[Json]) -> None:
        # Keep the in-memory index coherent (base behavior) AND write-through the durable hash so a
        # later delete on a fresh process still gets O(1) membership. Additions from THIS batch are
        # unioned into any existing persisted set (membership is cumulative across the async pipeline:
        # the event lands first, its derivatives + embeddings/postings arrive at extraction/commit).
        super()._maintain_event_membership_after_append(records)
        if not records:
            return
        additions: dict[str, set[str]] = {}
        for record in records:
            record_type = str(record.get("record_type") or "")
            if record_type == "context_event":
                event_hash = record.get("event_id_hash")
                if event_hash not in (None, ""):
                    additions.setdefault(str(event_hash), set()).add(str(event_hash))
                continue
            if record_type in MEMORY_DERIVATIVE_RECORD_TYPES:
                provenance = record_provenance_source_ids(record)
                if not provenance:
                    continue
                identity_ids = record_derivative_identity_ids(record)
                if not identity_ids:
                    continue
                for source_id in provenance:
                    additions.setdefault(str(source_id), set()).update(identity_ids)
        existing_by_event = self._lookup_persisted_event_members_many(list(additions))
        pending: list[tuple[str, set[str]]] = []
        for event_id, new_members in additions.items():
            existing = existing_by_event.get(str(event_id)) or set()
            merged = existing | new_members
            if merged != existing:
                pending.append((str(event_id), merged))
        if not pending:
            return
        entries = [
            {
                "key": self._event_members_hash_key(),
                "field": event_id,
                "value": json.dumps(sorted(str(m) for m in merged), separators=(",", ":")),
            }
            for event_id, merged in pending
        ]
        try:
            self._client.batch_hset(entries)
        except Exception:  # noqa: BLE001 - best-effort, same as the single-key write it replaces
            for event_id, merged in pending:
                self._persist_event_members(event_id, merged)

    def __init__(
        self,
        *,
        metaserver: str,
        namespace: str,
        table: str,
        library_path: str = "",
        storage_prefix: str = "matrixark:mcp",
        request_timeout_ms: int = 20000,
        io_timeout_ms: int = 20000,
    ) -> None:
        super().__init__(Path("/tmp/matrixark-mcp-unused-direct.jsonl"))
        MatrixArkLocalAdapter._init_local_runtime_state(self)
        self._entity_cache_loaded = True
        self._context_node_cache_loaded = True
        sdk_root = Path(__file__).resolve().parents[1] / "sdk" / "python"
        sys.path.insert(0, str(sdk_root))
        from temporalstore import Client, Options, ProxyClient, ProxyOptions  # type: ignore

        proxy_endpoint = os.environ.get("MATRIXARK_TEMPORALSTORE_NATIVE_PROXY_ENDPOINT", "").strip()
        force_generic_batch_fallback = os.environ.get("MATRIXARK_FORCE_GENERIC_BATCH_HSET_FALLBACK", "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self._proxy_endpoint = proxy_endpoint
        if proxy_endpoint:
            proxy_options = ProxyOptions(
                endpoint=proxy_endpoint,
                namespace_name=namespace,
                table_name=table,
                timeout_seconds=max(1.0, request_timeout_ms / 1000.0),
            )
            self._client = ProxyClient(proxy_options)
            self._matrixark_native_batch_append_available = True
            self._matrixark_append_write_path = "native_proxy_matrixark_batch_append_records"
        else:
            options = Options(
                metaserver_addr=metaserver,
                namespace_name=namespace,
                table_name=table,
                request_timeout_ms=request_timeout_ms,
                io_timeout_ms=io_timeout_ms,
                max_read_retries=2,
                max_write_retries=1,
            )
            self._client = Client(options, library_path=library_path or None)
            self._matrixark_native_batch_append_available = bool(
                getattr(getattr(self._client, "_native", None), "has_matrixark_batch_append_records", False)
            )
            self._matrixark_append_write_path = (
                "native_direct_existing_batch_execute_raw_batch_mset"
                if self._matrixark_native_batch_append_available
                else "fallback_python_batch_hset_loop"
            )
        if force_generic_batch_fallback:
            self._matrixark_native_batch_append_available = False
            self._matrixark_append_write_path = "forced_fallback_python_batch_hset_loop"
        self._matrixark_append_uses_per_record_hset = not self._matrixark_native_batch_append_available
        self._matrixark_batch_append_uses_existing_batch_execute = bool(
            self._matrixark_native_batch_append_available
            and self._matrixark_append_write_path == "native_direct_existing_batch_execute_raw_batch_mset"
        )
        self._matrixark_proxy_mode = bool(proxy_endpoint)
        # has a native CONTEXT extension (WRITE_EVENT / WRITE_EXTRACTED_EVENT)
        # but the generic JSON record-log adapter still persists through the
        # MatrixArk batch hash API. Keep this explicit in metrics/reports so the
        # deeper append-queue optimization is not confused with the API boundary.
        self._matrixark_context_extension_append_selected = False
        self._metaserver = metaserver
        self._namespace = namespace
        self._table = table
        self._readiness_cache: Json | None = None
        self._readiness_lock = threading.RLock()
        self._storage_prefix = storage_prefix.rstrip(":")
        self._supported_storage_families = self._parse_supported_storage_families()
        self._record_hash_key = f"{self._storage_prefix}:records"
        self._index_key = f"{self._storage_prefix}:record_index"
        self._count_key = f"{self._storage_prefix}:record_count"
        configured_raw_prefix = os.environ.get("MATRIXARK_DIRECT_RAW_STORAGE_PREFIX", "").strip().rstrip(":")
        self._raw_ingestion_prefix = configured_raw_prefix or f"{self._storage_prefix}:raw_ingestion"
        self._raw_record_hash_key = f"{self._raw_ingestion_prefix}:records"
        self._raw_count_key = f"{self._raw_ingestion_prefix}:record_count"
        self._raw_storage_backend = self._normalize_raw_storage_backend(
            os.environ.get("MATRIXARK_RAW_INGESTION_BACKEND", "temporalstore")
        )
        self._raw_entry_count_cache: int | None = None
        self._shard_size = DIRECT_RECORD_LOG_SHARD_SIZE
        self._index_cache: list[str] | None = None
        self._records_cache: list[Json] | None = None
        self._retrieval_candidate_cache: dict[str, Json] = {}
        self._retrieval_candidate_cache_lock = threading.RLock()
        self._entry_count_cache: int | None = None
        self._legacy_index_mode = False
        self._pending_visibility_keys: set[str] = set()
        self._records_lock = threading.RLock()
        self._audit_lock = threading.RLock()
        self._audit_buffer: list[Json] = []
        self._audit_flusher_started = False
        self._audit_flush_failures = 0
        if DIRECT_AUDIT_MODE not in {"buffered", "deferred", "drop", "sync"}:
            raise MatrixArkError("MATRIXARK_DIRECT_AUDIT_MODE must be buffered, deferred, drop, or sync")
        self._audit_mode = DIRECT_AUDIT_MODE
        self._audit_buffer_max_records = max(1, DIRECT_AUDIT_BUFFER_MAX_RECORDS)
        self._audit_flush_interval_s = max(0.05, DIRECT_AUDIT_FLUSH_INTERVAL_MS / 1000.0)
        self._write_retries = max(0, DIRECT_WRITE_RETRIES)
        self._write_backoff_s = max(0.0, DIRECT_WRITE_BACKOFF_MS / 1000.0)
        self._write_throttle_s = max(0.0, DIRECT_WRITE_THROTTLE_MS / 1000.0)
        self._direct_write_queue_enabled = env_bool("MATRIXARK_DIRECT_WRITE_QUEUE", False)
        self._direct_write_queue_max_records = max(1, int(os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_MAX_RECORDS", "10000")))
        self._direct_write_queue_put_timeout_s = max(0.01, int(os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_PUT_TIMEOUT_MS", "1000")) / 1000.0)
        self._direct_write_queue_mode = os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_MODE", "memory").strip().lower() or "memory"
        if self._direct_write_queue_mode not in {"memory", "temporalstore"}:
            raise MatrixArkError("MATRIXARK_DIRECT_WRITE_QUEUE_MODE must be memory or temporalstore")
        self._direct_write_queue_drain_max_batches = max(1, int(os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_DRAIN_MAX_BATCHES", "64")))
        self._direct_write_queue_allow_sync_context = os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_ALLOW_SYNC_CONTEXT", "0").strip().lower() in {"1", "true", "yes"}
        self._direct_write_queue_autostart = True
        self._native_side_index_assume_fresh = os.environ.get("MATRIXARK_NATIVE_SIDE_INDEX_ASSUME_FRESH", "0").strip().lower() in {"1", "true", "yes"}
        self._direct_raw_ingestion_queue_enabled = os.environ.get("MATRIXARK_DIRECT_RAW_INGESTION_QUEUE", "0").strip().lower() in {"1", "true", "yes"}
        self._direct_raw_ingestion_enabled = os.environ.get("MATRIXARK_DIRECT_RAW_INGESTION", "0").strip().lower() in {"1", "true", "yes"}
        self._direct_write_queue_key = f"{self._storage_prefix}:direct_write_queue"
        self._direct_write_queue_done_key = f"{self._storage_prefix}:direct_write_queue_done"
        self._direct_write_queue_dead_key = f"{self._storage_prefix}:direct_write_queue_dead"
        self._direct_write_queue: queue.Queue[Any] = queue.Queue(maxsize=self._direct_write_queue_max_records)
        self._direct_write_worker_started = False
        self._direct_write_worker_lock = threading.RLock()
        self._direct_write_stop = threading.Event()
        self._direct_write_failures = 0
        self._direct_write_enqueued_records = 0
        self._direct_write_flushed_records = 0
        self._direct_write_enqueued_batches = 0
        self._direct_write_flushed_batches = 0
        self._direct_write_dead_letter_batches = 0
        self._backend_ready = False
        self._backend_ready_result: Json | None = None
        self._backend_readiness_lock = threading.RLock()
        self._metrics_lock = threading.RLock()
        self._metrics_started_at_ms = now_ms()
        self._commands_total = 0
        self._errors_total = 0
        self._timeouts_total = 0
        self._latency_sum_ms = 0.0
        self._latency_max_ms = 0.0
        self._latency_buckets = [0 for _ in MatrixArkServiceMetrics.LATENCY_BUCKETS_MS]
        self._records_written_total = 0
        self._records_read_total = 0
        self._append_queue_wait_ms_total = 0.0
        self._append_queue_wait_count = 0
        self._append_engine_ms_total = 0.0
        self._append_engine_count = 0
        self._disk_fallback_adapter: MatrixArkLocalAdapter | None = None
        self._disk_fallback_path = disk_fallback_store_path()
        self._disk_fallback_enabled = bool(self._disk_fallback_path)
        self._disk_fallback_recovery_enabled = bool(self._disk_fallback_path)
        self._disk_fallback_recovery_attempted = False
        self._disk_fallback_recovery_in_progress = False
        self._disk_fallback_recovery_status: Json = {"status": "not_attempted"}
        self._async_context_warmup_enabled = os.environ.get(
            "MATRIXARK_TEMPORALSTORE_ASYNC_CONTEXT_WARMUP",
            "1",
        ).strip().lower() not in {"0", "false", "no", "off"}
        self._async_context_warmup_lock = threading.RLock()
        self._async_context_warmup_in_progress = False
        self._async_context_warmup_started_total = 0
        self._async_context_warmup_completed_total = 0
        self._async_context_warmup_failed_total = 0
        self._async_context_warmup_status: Json = {"status": "not_started"}

    def __post_init__(self) -> None:
        # Direct adapter does not use the inherited JSONL path.
        return

    def _parse_supported_storage_families(self) -> set[str]:
        raw = os.environ.get("MATRIXARK_NATIVE_STORAGE_FAMILIES") or os.environ.get("MATRIXARK_SUPPORTED_STORAGE_FAMILIES") or "default,local,single_node,shared_store"
        families = {part.strip().lower().replace("-", "_") for part in raw.split(",") if part.strip()}
        return families or {"default", "local", "single_node", "shared_store"}

    def _validate_storage_routes_available(self, records: list[Json]) -> None:
        if not hasattr(self, "_supported_storage_families"):
            self._supported_storage_families = self._parse_supported_storage_families()
        requested: set[str] = set()
        for record in records:
            route = record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {}
            family = str(route.get("storage_family") or route.get("selected_storage_family") or "default").strip().lower().replace("-", "_")
            if family and family != "default":
                requested.add(family)
        if len(requested) > 1:
            raise MatrixArkError(f"one MatrixArk write batch cannot mix storage families: {sorted(requested)}")
        unsupported = requested - set(getattr(self, "_supported_storage_families", {"default"}))
        if unsupported:
            raise MatrixArkError(
                f"requested storage_family {sorted(unsupported)} is not configured for backend {self._backend_label()}; "
                f"configured families={sorted(getattr(self, '_supported_storage_families', []))}"
            )

    def _backend_label(self) -> str:
        return "temporalstore-native-proxy" if getattr(self, "_matrixark_proxy_mode", False) else "temporalstore-native"

    def python_hot_cache_enabled(self) -> bool:
        return python_hot_cache_allowed(backend_label=self._backend_label())

    def _ensure_backend_metric_fields(self) -> None:
        if not hasattr(self, "_metrics_lock"):
            self._metrics_lock = threading.RLock()
        if not hasattr(self, "_metrics_started_at_ms"):
            self._metrics_started_at_ms = now_ms()
        if not hasattr(self, "_commands_total"):
            self._commands_total = 0
        if not hasattr(self, "_errors_total"):
            self._errors_total = 0
        if not hasattr(self, "_timeouts_total"):
            self._timeouts_total = 0
        if not hasattr(self, "_latency_sum_ms"):
            self._latency_sum_ms = 0.0
        if not hasattr(self, "_latency_max_ms"):
            self._latency_max_ms = 0.0
        if not hasattr(self, "_latency_buckets"):
            self._latency_buckets = [0 for _ in MatrixArkServiceMetrics.LATENCY_BUCKETS_MS]
        if not hasattr(self, "_records_written_total"):
            self._records_written_total = 0
        if not hasattr(self, "_records_read_total"):
            self._records_read_total = 0
        if not hasattr(self, "_append_queue_wait_ms_total"):
            self._append_queue_wait_ms_total = 0.0
        if not hasattr(self, "_append_queue_wait_count"):
            self._append_queue_wait_count = 0
        if not hasattr(self, "_append_engine_ms_total"):
            self._append_engine_ms_total = 0.0
        if not hasattr(self, "_append_engine_count"):
            self._append_engine_count = 0
        if not hasattr(self, "_backend_ready"):
            self._backend_ready = False
        if not hasattr(self, "_storage_prefix"):
            self._storage_prefix = "matrixark:mcp"
        if not hasattr(self, "_record_hash_key"):
            self._record_hash_key = f"{self._storage_prefix}:records"
        if not hasattr(self, "_index_key"):
            self._index_key = f"{self._storage_prefix}:record_index"
        if not hasattr(self, "_count_key"):
            self._count_key = f"{self._storage_prefix}:record_count"
        if not hasattr(self, "_shard_size"):
            self._shard_size = DIRECT_RECORD_LOG_SHARD_SIZE
        if not hasattr(self, "_records_cache"):
            self._records_cache = None
        if not hasattr(self, "_index_cache"):
            self._index_cache = None
        if not hasattr(self, "_entry_count_cache"):
            self._entry_count_cache = None
        if not hasattr(self, "_legacy_index_mode"):
            self._legacy_index_mode = False
        if not hasattr(self, "_records_lock"):
            self._records_lock = threading.RLock()
        if not hasattr(self, "_audit_lock"):
            self._audit_lock = threading.RLock()
        if not hasattr(self, "_retrieval_candidate_cache"):
            self._retrieval_candidate_cache = {}
        if not hasattr(self, "_retrieval_candidate_cache_lock"):
            self._retrieval_candidate_cache_lock = threading.RLock()
        if not hasattr(self, "_audit_buffer"):
            self._audit_buffer = []
        if not hasattr(self, "_audit_buffer_max_records"):
            self._audit_buffer_max_records = max(1, DIRECT_AUDIT_BUFFER_MAX_RECORDS)
        if not hasattr(self, "_audit_mode"):
            self._audit_mode = DIRECT_AUDIT_MODE
        if not hasattr(self, "_audit_flusher_started"):
            self._audit_flusher_started = False
        if not hasattr(self, "_audit_flush_failures"):
            self._audit_flush_failures = 0
        if not hasattr(self, "_write_retries"):
            self._write_retries = max(0, DIRECT_WRITE_RETRIES)
        if not hasattr(self, "_write_backoff_s"):
            self._write_backoff_s = max(0.0, DIRECT_WRITE_BACKOFF_MS / 1000.0)
        if not hasattr(self, "_write_throttle_s"):
            self._write_throttle_s = max(0.0, DIRECT_WRITE_THROTTLE_MS / 1000.0)
        if not hasattr(self, "_pending_visibility_keys"):
            self._pending_visibility_keys = set()
        if not hasattr(self, "_retrieval_records_cache_lock"):
            self._retrieval_records_cache_lock = threading.RLock()
        if not hasattr(self, "_retrieval_records_cache_generation"):
            self._retrieval_records_cache_generation = 0
        if not hasattr(self, "_retrieval_records_cache"):
            self._retrieval_records_cache = {}
        if not hasattr(self, "_context_pack_cache_lock"):
            self._context_pack_cache_lock = threading.RLock()
        if not hasattr(self, "_context_pack_cache"):
            self._context_pack_cache = {}
        if not hasattr(self, "_context_pack_cache_max_entries"):
            self._context_pack_cache_max_entries = max(0, int(os.environ.get("MATRIXARK_CONTEXT_PACK_CACHE_MAX_ENTRIES", "256")))
        if not hasattr(self, "_context_pack_cache_ttl_s"):
            self._context_pack_cache_ttl_s = max(0.0, float(os.environ.get("MATRIXARK_CONTEXT_PACK_CACHE_TTL_S", "30")))
        if not hasattr(self, "_disk_fallback_adapter"):
            self._disk_fallback_adapter = None
        if not hasattr(self, "_disk_fallback_path"):
            self._disk_fallback_path = disk_fallback_store_path()
        if not hasattr(self, "_disk_fallback_enabled"):
            self._disk_fallback_enabled = bool(self._disk_fallback_path)
        if not hasattr(self, "_disk_fallback_recovery_enabled"):
            self._disk_fallback_recovery_enabled = bool(self._disk_fallback_path)
        if not hasattr(self, "_disk_fallback_recovery_attempted"):
            self._disk_fallback_recovery_attempted = False
        if not hasattr(self, "_disk_fallback_recovery_in_progress"):
            self._disk_fallback_recovery_in_progress = False
        if not hasattr(self, "_disk_fallback_recovery_status"):
            self._disk_fallback_recovery_status = {"status": "not_attempted"}
        self._ensure_direct_write_queue_fields()


    def _direct_write_durable_field(self, payload: Json) -> str:
        digest = stable_hash(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return f"{int(payload.get('created_at_ms') or now_ms()):020d}:{digest}"

    def _enqueue_direct_write_durable(self, records: list[Json]) -> str:
        payload = self._direct_write_durable_payload(list(records))
        field = self._direct_write_durable_field(payload)
        self._hset_with_backoff(self._direct_write_queue_key, field, json.dumps(payload, separators=(",", ":")))
        return field

    def _enqueue_direct_write_item(self, item: Any, record_count: int) -> None:
        self._ensure_direct_write_queue_fields()
        if bool(getattr(self, "_direct_write_queue_autostart", True)):
            self._start_direct_write_worker()
        wait_started_perf = time.perf_counter()
        try:
            self._direct_write_queue.put(item, timeout=self._direct_write_queue_put_timeout_s)
        except queue.Full as exc:
            self._observe_append_queue_wait((time.perf_counter() - wait_started_perf) * 1000.0)
            if isinstance(item, dict) and item.get("queue_mode") == "temporalstore":
                _mcp_debug_log("matrixark durable direct write queue accepted batch but local worker queue is full; batch will be recovered by drain")
                self._direct_write_enqueued_records += record_count
                self._direct_write_enqueued_batches += 1
                return
            raise MatrixArkError("direct TemporalStore write queue is full") from exc
        self._observe_append_queue_wait((time.perf_counter() - wait_started_perf) * 1000.0)
        self._direct_write_enqueued_records += record_count
        self._direct_write_enqueued_batches += 1

    def _enqueue_direct_write(self, records: list[Json]) -> None:
        item: Any = list(records)
        if getattr(self, "_direct_write_queue_mode", "memory") == "temporalstore":
            item = {"queue_mode": "temporalstore", "field": self._enqueue_direct_write_durable(records)}
        self._enqueue_direct_write_item(item, len(records))






class MatrixArkRustCdylibClient:
    """In-process Rust direct SDK binding loaded through the Rust cdylib C ABI."""

    def __init__(
        self,
        *,
        library_path: str,
        temporalstore_lib: str = "",
        metaserver: str,
        namespace: str,
        table: str,
        request_timeout_ms: int,
        io_timeout_ms: int,
    ) -> None:
        import ctypes
        import json as _json

        if not library_path:
            raise MatrixArkError("MATRIXARK_TEMPORALSTORE_RUST_DIRECT_LIB is required for Rust direct cdylib mode")
        self.library_path = library_path
        self.metaserver = metaserver
        self.namespace = namespace
        self.table = table
        self.request_timeout_ms = request_timeout_ms
        self.io_timeout_ms = io_timeout_ms
        self.sdk_mode = "direct_cdylib"
        self._ctypes = ctypes
        load_mode = getattr(ctypes, "RTLD_GLOBAL", None)
        if temporalstore_lib:
            try:
                ctypes.CDLL(temporalstore_lib, mode=load_mode) if load_mode is not None else ctypes.CDLL(temporalstore_lib)
            except OSError:
                pass
        self._lib = ctypes.CDLL(library_path, mode=load_mode) if load_mode is not None else ctypes.CDLL(library_path)
        self._bind()
        self._handle = ctypes.c_void_p()
        self._commands_total = 0
        self._commands_failed_total = 0
        self._records_written_total = 0
        self._records_read_total = 0
        self._latency_samples_ms: list[float] = []
        options = {
            "metaserver_addr": metaserver,
            "namespace_name": namespace,
            "table_name": table,
            "request_timeout_ms": request_timeout_ms,
            "io_timeout_ms": io_timeout_ms,
            "connect_timeout_ms": min(request_timeout_ms, io_timeout_ms),
            "max_read_retries": 2,
            "max_write_retries": 1,
            "retry_backoff_ms": 2,
            "pin_primary": True,
        }
        error = ctypes.c_void_p()
        code = self._lib.temporalstore_rust_connect_json(
            _json.dumps(options, separators=(",", ":")).encode("utf-8"),
            ctypes.byref(self._handle),
            ctypes.byref(error),
        )
        self._check(code, error)

    def _bind(self) -> None:
        c = self._ctypes
        lib = self._lib
        lib.temporalstore_rust_free_string.argtypes = [c.c_void_p]
        lib.temporalstore_rust_free_string.restype = None
        lib.temporalstore_rust_connect_json.argtypes = [c.c_char_p, c.POINTER(c.c_void_p), c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_connect_json.restype = c.c_int
        lib.temporalstore_rust_close.argtypes = [c.c_void_p, c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_close.restype = c.c_int
        lib.temporalstore_rust_hset.argtypes = [c.c_void_p, c.c_char_p, c.c_char_p, c.c_char_p, c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_hset.restype = c.c_int
        lib.temporalstore_rust_hget.argtypes = [c.c_void_p, c.c_char_p, c.c_char_p, c.POINTER(c.c_void_p), c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_hget.restype = c.c_int
        lib.temporalstore_rust_hgetall_json.argtypes = [c.c_void_p, c.c_char_p, c.POINTER(c.c_void_p), c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_hgetall_json.restype = c.c_int
        lib.temporalstore_rust_matrixark_batch_append_records_json.argtypes = [c.c_void_p, c.c_char_p, c.c_char_p, c.c_char_p, c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_matrixark_batch_append_records_json.restype = c.c_int
        lib.temporalstore_rust_matrixark_scan_candidates_json.argtypes = [c.c_void_p, c.c_char_p, c.c_char_p, c.c_size_t, c.c_char_p, c.POINTER(c.c_void_p), c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_matrixark_scan_candidates_json.restype = c.c_int
        lib.temporalstore_rust_matrixark_retrieve_context_pack_json.argtypes = [c.c_void_p, c.c_char_p, c.c_char_p, c.c_size_t, c.c_char_p, c.POINTER(c.c_void_p), c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_matrixark_retrieve_context_pack_json.restype = c.c_int

    def _decode_owned(self, value: Any) -> str:
        try:
            return self._ctypes.cast(value, self._ctypes.c_char_p).value.decode("utf-8", errors="replace")
        finally:
            self._lib.temporalstore_rust_free_string(value)

    def _check(self, code: int, error: Any) -> None:
        if code == 0:
            return
        message = "unknown Rust TemporalStore direct binding error"
        if error:
            message = self._decode_owned(error)
        raise MatrixArkError(message)

    def _call(self, op: str, fn: Any, *, records_written: int = 0, records_read: int = 0) -> Any:
        started = time.perf_counter()
        self._commands_total += 1
        try:
            result = fn()
        except Exception:
            self._commands_failed_total += 1
            raise
        finally:
            self._latency_samples_ms.append((time.perf_counter() - started) * 1000.0)
            if len(self._latency_samples_ms) > 2048:
                self._latency_samples_ms = self._latency_samples_ms[-2048:]
        self._records_written_total += records_written
        self._records_read_total += records_read
        return result

    def close(self) -> None:
        if not getattr(self, "_handle", None):
            return
        error = self._ctypes.c_void_p()
        code = self._lib.temporalstore_rust_close(self._handle, self._ctypes.byref(error))
        self._handle = self._ctypes.c_void_p()
        self._check(code, error)

    def put_string(self, key: str, value: str) -> None:
        # MatrixArk direct serving should use batch append; keep this for compatibility through hset-style paths.
        self.hset(key, "", value)

    def get_string(self, key: str) -> str:
        return self.hget(key, "")

    def hset(self, key: str, field: str, value: str) -> None:
        def call() -> None:
            error = self._ctypes.c_void_p()
            code = self._lib.temporalstore_rust_hset(self._handle, key.encode(), field.encode(), value.encode(), self._ctypes.byref(error))
            self._check(code, error)
        self._call("hset", call, records_written=1)

    def hget(self, key: str, field: str) -> str:
        def call() -> str:
            out = self._ctypes.c_void_p()
            error = self._ctypes.c_void_p()
            code = self._lib.temporalstore_rust_hget(self._handle, key.encode(), field.encode(), self._ctypes.byref(out), self._ctypes.byref(error))
            self._check(code, error)
            return self._decode_owned(out)
        return self._call("hget", call, records_read=1)

    def hgetall(self, key: str) -> list[Json]:
        return list(self.scan_hash(key).get("records", []))

    def scan_hash(self, key: str) -> Json:
        def call() -> Json:
            out = self._ctypes.c_void_p()
            error = self._ctypes.c_void_p()
            code = self._lib.temporalstore_rust_hgetall_json(self._handle, key.encode(), self._ctypes.byref(out), self._ctypes.byref(error))
            self._check(code, error)
            return json.loads(self._decode_owned(out))
        result = self._call("scan_hash", call)
        self._records_read_total += int(result.get("count") or 0)
        return result

    def batch_hset(self, entries: list[Json]) -> None:
        self.matrixark_batch_append_records(entries)

    def matrixark_batch_append_records(
        self,
        entries: list[Json],
        *,
        count_key: str | None = None,
        count_value: str | None = None,
        append_options: Json | None = None,
    ) -> None:
        values = [{"key": str(entry.get("key") or ""), "field": str(entry.get("field") or ""), "value": str(entry.get("value") or "")} for entry in entries]
        payload = json.dumps(values, separators=(",", ":"), sort_keys=True).encode("utf-8")
        def call() -> None:
            error = self._ctypes.c_void_p()
            code = self._lib.temporalstore_rust_matrixark_batch_append_records_json(
                self._handle,
                payload,
                (count_key or "").encode("utf-8"),
                (count_value or "").encode("utf-8"),
                self._ctypes.byref(error),
            )
            self._check(code, error)
        self._call("matrixark_batch_append_records", call, records_written=len(values) + (1 if count_key else 0))

    def matrixark_append_records(
        self,
        entries: list[Json],
        *,
        count_key: str | None = None,
        count_value: str | None = None,
        append_options: Json | None = None,
    ) -> None:
        self.matrixark_batch_append_records(
            entries,
            count_key=count_key,
            count_value=count_value,
            append_options=append_options,
        )

    def matrixark_scan_candidates(self, *, count_key: str, record_hash_key: str, shard_size: int, scope: Json, record_types: list[str], secondary_index_groups: list[list[str]], selected_node_hashes: list[int], record_ids: list[str] | None = None, return_index_records: bool = False, newest_by_type: Json | None = None) -> Json:
        payload: Json = {"scope": scope, "record_types": record_types, "secondary_index_groups": secondary_index_groups, "selected_node_hashes": selected_node_hashes}
        if record_ids:
            payload["record_ids"] = [str(item) for item in record_ids]
        if return_index_records:
            payload["return_index_records"] = True
        if newest_by_type:
            payload["newest_by_type"] = {str(k): int(v) for k, v in newest_by_type.items()}
        request = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        def call() -> Json:
            out = self._ctypes.c_void_p()
            error = self._ctypes.c_void_p()
            code = self._lib.temporalstore_rust_matrixark_scan_candidates_json(self._handle, count_key.encode(), record_hash_key.encode(), int(shard_size), request, self._ctypes.byref(out), self._ctypes.byref(error))
            self._check(code, error)
            return json.loads(self._decode_owned(out))
        return self._call("matrixark_scan_candidates", call)

    def matrixark_retrieve_context_pack(self, *, count_key: str, record_hash_key: str, shard_size: int, request: Json) -> Json:
        payload = json.dumps(request, separators=(",", ":"), sort_keys=True).encode("utf-8")
        def call() -> Json:
            out = self._ctypes.c_void_p()
            error = self._ctypes.c_void_p()
            code = self._lib.temporalstore_rust_matrixark_retrieve_context_pack_json(self._handle, count_key.encode(), record_hash_key.encode(), int(shard_size), payload, self._ctypes.byref(out), self._ctypes.byref(error))
            self._check(code, error)
            return json.loads(self._decode_owned(out))
        return self._call("matrixark_retrieve_context_pack", call)

    def health(self) -> Json:
        return {"ok": True, "status": "ok", "mode": "rust_direct_cdylib"}

    def readiness(self) -> Json:
        return {"ok": True, "status": "ready", "mode": "rust_direct_cdylib", "cached_clients": 1}

    def metrics_snapshot(self) -> Json:
        elapsed = max(0.001, sum(self._latency_samples_ms) / 1000.0) if self._latency_samples_ms else 1.0
        return {
            "gateway_mode": "rust_direct_cdylib",
            "proxy_mode": "none",
            "sdk_mode": "direct_cdylib",
            "transport": "in_process_cdylib_ctypes",
            "process_per_operation_enabled": False,
            "single_shot_mode": "debug_only_disabled_for_hot_path",
            "commands_total": self._commands_total,
            "commands_failed_total": self._commands_failed_total,
            "timeouts_total": 0,
            "qps": round(self._commands_total / elapsed, 6),
            "records_written_total": self._records_written_total,
            "records_read_total": self._records_read_total,
            "latency_ms_sum": round(sum(self._latency_samples_ms), 3),
            "latency_ms_count": len(self._latency_samples_ms),
            "latency_ms_max": round(max(self._latency_samples_ms) if self._latency_samples_ms else 0.0, 3),
            "p95_latency_ms": round(self._percentile(self._latency_samples_ms, 0.95), 3),
            "p99_latency_ms": round(self._percentile(self._latency_samples_ms, 0.99), 3),
            "matrixark_append_blob_parity_total": 0,
            "matrixark_append_hset_count_lowering_total": 0,
            "matrixark_append_hot_path": "native_c_api_bridge_diagnostic",
            "matrixark_append_write_path": "rust_direct_cdylib_matrixark_batch_append_records",
            "matrixark_native_batch_append_available": True,
            "matrixark_batch_append_uses_existing_batch_execute": True,
            "matrixark_batch_append_existing_batch_execute_source": "temporalstore_rust_cdylib_to_temporalstore_matrixark_batch_append_records",
            "matrixark_append_uses_per_record_hset": False,
            "matrixark_append_uses_generic_batch_hset_fallback": False,
            "supports_batch_append": True,
            "supports_prefix_scan": True,
            "prefix_scan_path": "rust_direct_cdylib_hgetall_json",
            "supports_native_candidate_prefilter": True,
            "candidate_prefilter_path": "rust_direct_cdylib_matrixark_scan_candidates",
            "supports_native_pack_assembly": True,
            "native_pack_assembly_path": "rust_direct_cdylib_matrixark_retrieve_context_pack",
            "requires_c_sdk_hgetall_for_prefix_scan": False,
        }

    def metrics_prometheus(self) -> str:
        metrics = self.metrics_snapshot()
        return "\n".join([
            '# TYPE matrixark_rust_direct_cdylib_commands_total counter',
            f'matrixark_rust_direct_cdylib_commands_total {metrics["commands_total"]}',
            '# TYPE matrixark_rust_direct_cdylib_errors_total counter',
            f'matrixark_rust_direct_cdylib_errors_total {metrics["commands_failed_total"]}',
        ]) + "\n"

    @staticmethod
    def _percentile(values: list[float], ratio: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * ratio))))
        return ordered[index]

    def shutdown(self) -> None:
        self.close()

class MatrixArkRustProxyClient:
    """Persistent Rust proxy boundary around the Rust TemporalStore SDK.

    The Rust binary owns SDK linkage and runs in JSON-lines ``--serve`` mode as
    a Rust proxy. MatrixArk production and benchmark paths should use this
    proxy or the Rust direct SDK path, never process-per-operation CLI calls.
    """

    def __init__(
        self,
        *,
        proxy_path: str = "",
        cli_path: str = "",
        metaserver: str,
        namespace: str,
        table: str,
        request_timeout_ms: int,
        io_timeout_ms: int,
        sdk_mode: str = "proxy",
    ) -> None:
        proxy_path = proxy_path or cli_path
        if not proxy_path:
            raise MatrixArkError("--rust-proxy or MATRIXARK_TEMPORALSTORE_RUST_PROXY is required for temporalstore-rust")
        self.cli_path = proxy_path
        self.proxy_path = proxy_path
        self.metaserver = metaserver
        self.namespace = namespace
        self.table = table
        self.request_timeout_ms = request_timeout_ms
        self.io_timeout_ms = io_timeout_ms
        self._legacy_lock = threading.Lock()
        self._legacy_semaphore = threading.BoundedSemaphore(1)
        self._backpressure_timeout_s = max(
            0.05,
            int(
                os.environ.get(
                    "MATRIXARK_RUST_PROXY_BACKPRESSURE_TIMEOUT_MS",
                    os.environ.get("MATRIXARK_RUST_GATEWAY_BACKPRESSURE_TIMEOUT_MS", str(request_timeout_ms)),
                )
            )
            / 1000.0,
        )
        self._write_lane_count = max(1, int(os.environ.get("MATRIXARK_RUST_PROXY_WRITE_LANES", "4")))
        self._read_lane_count = max(1, int(os.environ.get("MATRIXARK_RUST_PROXY_READ_LANES", "4")))
        self._pack_lane_count = max(1, int(os.environ.get("MATRIXARK_RUST_PROXY_PACK_LANES", "8")))
        self._control_lane_count = max(1, int(os.environ.get("MATRIXARK_RUST_PROXY_CONTROL_LANES", "1")))
        self._shared_process_mode = env_bool("MATRIXARK_RUST_PROXY_SHARED_PROCESS", True)
        self._proxy_socket = os.environ.get("MATRIXARK_RUST_PROXY_SOCKET", "").strip()
        self._dedicated_pack_lanes_enabled = (
            env_bool("MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES", False)
        )
        if self._shared_process_mode:
            # The local Rust TemporalEngine is embedded in the proxy process. A
            # multi-process write lane pool can hide writes from reads until
            # there is a real shared server/proxy behind it, so writes/control
            # stay on one process. Retrieve-pack is read-mostly after ingest
            # and may use a warm process pool to avoid stdin/stdout head-of-line
            # blocking in scale tests and production proxy mode.
            shared_lanes = self._make_lanes(1)
            pack_lanes = self._make_lanes(self._pack_lane_count) if self._dedicated_pack_lanes_enabled else shared_lanes
            self._lanes = {
                "write": shared_lanes,
                "read": shared_lanes,
                "pack": pack_lanes,
                "control": shared_lanes,
            }
        else:
            self._lanes = {
                "write": self._make_lanes(self._write_lane_count),
                "read": self._make_lanes(self._read_lane_count),
                "pack": self._make_lanes(self._pack_lane_count),
                "control": self._make_lanes(self._control_lane_count),
            }
        self._lane_worker_counts = {name: len(lanes) for name, lanes in self._lanes.items()}
        self._lane_worker_counts["retrieve"] = self._lane_worker_counts.get("pack", 0)
        self._lane_cursors = {name: 0 for name in self._lanes}
        self._lane_select_lock = threading.Lock()
        self._metrics_lock = threading.Lock()
        self._commands_total = 0
        self._commands_failed_total = 0
        self._records_written_total = 0
        self._records_read_total = 0
        self._backpressure_rejections_total = 0
        self._timeouts_total = 0
        self._last_latency_ms = 0.0
        self._max_observed_latency_ms = 0.0
        self._latency_samples_ms: list[float] = []
        self._lane_latency_samples_ms: dict[str, list[float]] = {lane: [] for lane in self._lane_worker_counts}
        self._lane_commands_total: dict[str, int] = {lane: 0 for lane in self._lane_worker_counts}
        self._lane_wait_ms_total: dict[str, float] = {lane: 0.0 for lane in self._lane_worker_counts}
        self._lane_wait_ms_max: dict[str, float] = {lane: 0.0 for lane in self._lane_worker_counts}
        self._op_commands_total: dict[str, int] = {}
        self._op_latency_ms_total: dict[str, float] = {}
        self._op_latency_ms_max: dict[str, float] = {}
        self._serialization_ms_total = 0.0
        self._serialization_ms_max = 0.0
        self._rust_engine_ms_total = 0.0
        self._rust_engine_ms_max = 0.0
        self._scan_count_total = 0
        self._cache_hits_total = 0
        self._cache_misses_total = 0
        self._selected_refs_total = 0
        self._dropped_refs_total = 0
        self._matrixark_append_blob_parity_total = 0
        self._matrixark_append_hset_count_lowering_total = 0
        self._memory_layer_budget_totals: dict[str, Any] = {
            "by_memory_scope": {},
            "by_session_continuity": {},
            "by_extraction_phase": {},
            "by_ref_type": {},
            "by_entity_type": {},
            "by_source_role": {},
            "by_hook_type": {},
            "by_codex_event": {},
            "source_message_counts_by_role": {},
            "source_hook_counts_by_type": {},
            "source_codex_event_counts_by_event": {},
            "final_session_boundary_ref_count": 0,
            "provisional_ref_count": 0,
            "final_ref_count": 0,
            "total_selected_refs": 0,
            "total_selected_tokens": 0,
        }
        self._context_record_counts: dict[str, int] = {}
        self._publish_visibility_calls_total = 0
        self._publish_visibility_keys_total = 0
        self._publish_visibility_full_shard_total = 0
        self._publish_visibility_index_bytes_total = 0
        self._publish_visibility_last_key_count = 0
        self._publish_visibility_last_index_bytes = 0
        self._started_at = time.time()
        self._proc: subprocess.Popen[str] | None = None

    @staticmethod
    def _make_lanes(count: int) -> list[Json]:
        return [
            {
                "proc": None,
                "lock": threading.Lock(),
                "semaphore": threading.BoundedSemaphore(1),
            }
            for _ in range(count)
        ]

    def close(self) -> None:
        seen: set[int] = set()
        for lanes in getattr(self, "_lanes", {}).values():
            for lane in lanes:
                proc = lane.get("proc")
                lane["proc"] = None
                if proc is None or id(proc) in seen:
                    continue
                seen.add(id(proc))
                self._close_proc(proc)
        proc = self._proc
        self._proc = None
        if proc is not None and id(proc) not in seen:
            self._close_proc(proc)

    @staticmethod
    def _close_proc(proc: subprocess.Popen[str]) -> None:
        if proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=2)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        for stream in (proc.stdin, proc.stdout, proc.stderr):
            try:
                if stream is not None:
                    stream.close()
            except Exception:
                pass

    @staticmethod
    def _library_parent(path: str) -> str:
        if path.startswith("/"):
            return str(PurePosixPath(path).parent)
        return str(Path(path).resolve().parent)

    def _rust_proxy_library_search_path(self, env: Json) -> str:
        paths: list[str] = [self._library_parent(self.cli_path)]
        temporalstore_lib = str(env.get("TEMPORALSTORE_LIB") or "").strip()
        if temporalstore_lib:
            lib_dir = self._library_parent(temporalstore_lib)
            if lib_dir not in paths:
                paths.append(lib_dir)
        existing_ld_path = str(env.get("LD_LIBRARY_PATH") or "").strip()
        for item in existing_ld_path.split(":"):
            if item and item not in paths:
                paths.append(item)
        return ":".join(paths)

    def _ensure_lane_proc(self, lane: Json) -> subprocess.Popen[str]:
        proc = lane.get("proc")
        if proc is not None and proc.poll() is None:
            return proc
        if proc is not None:
            self._close_proc(proc)
        env = os.environ.copy()
        env["LD_LIBRARY_PATH"] = self._rust_proxy_library_search_path(env)
        if env.get("MATRIXARK_RUST_PROXY_NATIVE_MATRIXARK_C_API_COMPAT", "0").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }:
            env.setdefault("TEMPORALSTORE_RUST_ALLOW_NATIVE_MATRIXARK_C_API", "1")
        lane["proc"] = subprocess.Popen(
            [self.cli_path, "--serve"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )
        # Drain stderr from the moment the proxy starts. Nothing read this pipe outside the
        # error paths, which only run once the process has exited or its stdin has broken -- so
        # a proxy that logged past the 64KB pipe buffer blocked forever inside its next stderr
        # write. It stopped answering, sat near-idle because it was blocked rather than working,
        # held its lane, and never recovered; downstream that looked like one request hanging to
        # its timeout and every later one rejected on the lane. The busier the proxy, the sooner
        # it happened. Bounded deque, daemon thread: the error paths still quote the tail.
        sink: collections.deque = collections.deque(maxlen=PROXY_STDERR_TAIL_LINES)
        lane["stderr_tail"] = sink
        drain = threading.Thread(
            target=_drain_proxy_stderr,
            args=(lane["proc"], sink),
            name="matrixark-proxy-stderr-drain",
            daemon=True,
        )
        lane["stderr_drain"] = drain
        drain.start()
        return lane["proc"]

    def _lane_group_for_op(self, op: str) -> str:
        if op in {
            "batch_hset",
            "matrixark_append_records",
            "matrixark_batch_append_records",
            "matrixark_batch_append_raw_ingestion_records",
            "hset",
            "put_string",
            "write_matrixark_record",
            "write_matrixark_records",
        }:
            return "write"
        if op in {"matrixark_retrieve_context_pack"}:
            return "pack"
        if op in {"batch_hget", "hgetall", "scan_hash", "hget", "get_string", "read_matrixark_record", "read_matrixark_records"}:
            return "read"
        return "control"

    def _choose_lane(self, op: str) -> tuple[str, Json]:
        group = self._lane_group_for_op(op)
        lanes = self._lanes.get(group) or self._lanes["control"]
        with self._lane_select_lock:
            index = self._lane_cursors.get(group, 0) % len(lanes)
            self._lane_cursors[group] = index + 1
        return group, lanes[index]

    def _read_json_line(
        self,
        proc: subprocess.Popen[str],
        op: str,
        lane: Json | None = None,
        expected_request_id: str | None = None,
    ) -> Json:
        assert proc.stdout is not None
        deadline = time.monotonic() + max(2.0, self.request_timeout_ms / 1000.0 + 2.0)
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                # The drain thread owns proc.stderr; read what it captured, never the pipe.
                stderr = proxy_stderr_tail(lane)
                if op == "shutdown" and proc.returncode == 0:
                    return {"ok": True, "status": "shutdown"}
                raise MatrixArkError(f"Rust TemporalStore {op} process exited ({proc.returncode}): {stderr[-1000:]}")
            ready, _, _ = select.select([proc.stdout], [], [], 0.05)
            if not ready:
                continue
            line = proc.stdout.readline()
            if not line:
                continue
            if not line.strip().startswith("{"):
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError as exc:
                raise MatrixArkError(f"Rust TemporalStore {op} returned invalid JSON: {line[:200]!r}") from exc
            # The proxy answers strictly in order on one stdout. A request abandoned by ITS OWN
            # timeout still gets its response line later -- and without correlation the next
            # caller on this lane would read that stale line as its answer, shifting every
            # later reply one back and serving the wrong data (one scope's scan was observed
            # answered with another scope's records). Discard any response tagged for a
            # different request; a response with no tag (older proxy binary) is accepted
            # unchanged.
            if expected_request_id is not None:
                stale_id = parsed.get("client_request_id")
                if stale_id is not None and stale_id != expected_request_id:
                    continue
            return parsed
        raise MatrixArkError(
            f"Rust TemporalStore {op} timed out waiting for response from {self.cli_path} "
            f"after {max(2.0, self.request_timeout_ms / 1000.0 + 2.0):.1f}s"
        )

    def _call_json(self, op: str, raise_on_error: bool = True, **kwargs: Any) -> Json:
        command = {
            "op": op,
            "metaserver": self.metaserver,
            "namespace": self.namespace,
            "table": self.table,
            "request_timeout_ms": self.request_timeout_ms,
            "io_timeout_ms": self.io_timeout_ms,
            **kwargs,
        }
        payload = json.dumps(command, separators=(",", ":")) + "\n"
        started = time.perf_counter()
        if self._proxy_socket:
            try:
                response = self._call_socket_json(op, payload)
            except Exception:
                elapsed_ms = (time.perf_counter() - started) * 1000.0
                self._record_call_metrics(op, kwargs, None, elapsed_ms, failed=True, lane="daemon", wait_ms=0.0)
                raise
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            if not response.get("ok"):
                self._record_call_metrics(op, kwargs, response, elapsed_ms, failed=True, lane="daemon", wait_ms=0.0)
                if not raise_on_error:
                    return response
                raise MatrixArkError(f"Rust TemporalStore {op} failed: {response.get('error', 'unknown error')}")
            self._record_call_metrics(op, kwargs, response, elapsed_ms, failed=False, lane="daemon", wait_ms=0.0)
            return response
        group, lane = self._choose_lane(op)
        semaphore: threading.BoundedSemaphore = lane["semaphore"]
        wait_started = time.perf_counter()
        acquired = semaphore.acquire(timeout=self._backpressure_timeout_s)
        wait_ms = (time.perf_counter() - wait_started) * 1000.0
        if not acquired:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            self._record_call_metrics(op, kwargs, None, elapsed_ms, failed=True, backpressure=True, lane=group, wait_ms=wait_ms)
            raise MatrixArkError(
                f"Rust TemporalStore {op} rejected by {group} proxy lane backpressure after "
                f"{self._backpressure_timeout_s:.3f}s with "
                f"{self._lane_worker_counts.get(group, 1)} workers"
            )
        try:
            lock: threading.Lock = lane["lock"]
            with lock:
                proc = self._ensure_lane_proc(lane)
                assert proc.stdin is not None
                # Tag the request so the reader can discard the late responses of requests a
                # previous caller abandoned on this lane (see _read_json_line). Tagged into the
                # LANE payload only: the daemon-socket path opens a connection per request and
                # cannot desync.
                request_id = f"{id(lane)}-{time.monotonic_ns()}"
                lane_command = dict(command)
                lane_command["client_request_id"] = request_id
                payload = json.dumps(lane_command, separators=(",", ":")) + "\n"
                try:
                    proc.stdin.write(payload)
                    proc.stdin.flush()
                except BrokenPipeError as exc:
                    lane["proc"] = None
                    returncode = proc.poll()
                    stderr = proxy_stderr_tail(lane)
                    self._close_proc(proc)
                    detail = f"Rust TemporalStore {op} pipe closed"
                    if returncode is not None:
                        detail += f" after process exit ({returncode})"
                    if stderr:
                        detail += f": {stderr[-1000:]}"
                    raise MatrixArkError(detail) from exc
                response = self._read_json_line(proc, op, lane, expected_request_id=request_id)
        except Exception:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            self._record_call_metrics(op, kwargs, None, elapsed_ms, failed=True, lane=group, wait_ms=wait_ms)
            raise
        finally:
            semaphore.release()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        if not response.get("ok"):
            self._record_call_metrics(op, kwargs, response, elapsed_ms, failed=True, lane=group, wait_ms=wait_ms)
            if not raise_on_error:
                return response
            raise MatrixArkError(f"Rust TemporalStore {op} failed: {response.get('error', 'unknown error')}")
        self._record_call_metrics(op, kwargs, response, elapsed_ms, failed=False, lane=group, wait_ms=wait_ms)
        return response

    # Connecting to a live daemon socket returns immediately, so the connect keeps a
    # short ceiling: a stale socket file should fail fast rather than spend the call's
    # whole budget. Reads are a different question -- see `_call_socket_json`.
    SOCKET_CONNECT_TIMEOUT_CEILING_S = 2.0

    def _call_socket_json(self, op: str, payload: str) -> Json:
        # The read timeout tracks what is LEFT of the deadline. It used to be a constant
        # `min(2.0, ...)`, which no `--request-timeout-ms` could raise, so any response
        # slower than two seconds became a timeout -- and since `_call_json` re-raises
        # instead of falling back to the lane path, the call simply failed. A ContextPack
        # over a large store takes longer than that, so hook retrieval failed open with
        # no context and nothing in the logs but "TimeoutError: timed out".
        budget_s = max(2.0, self.request_timeout_ms / 1000.0 + 2.0)
        deadline = time.monotonic() + budget_s
        connect_timeout_s = min(
            self.SOCKET_CONNECT_TIMEOUT_CEILING_S,
            max(0.1, self.request_timeout_ms / 1000.0),
        )
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(connect_timeout_s)
            client.connect(self._proxy_socket)
            client.sendall(payload.encode("utf-8"))
            stream = client.makefile("rb")
            while True:
                remaining_s = deadline - time.monotonic()
                if remaining_s <= 0:
                    break
                # Re-armed per read so a partial answer cannot extend the total budget.
                client.settimeout(remaining_s)
                try:
                    line = stream.readline()
                except socket.timeout:
                    break
                if not line:
                    break
                if not line.strip().startswith(b"{"):
                    continue
                try:
                    return json.loads(line.decode("utf-8"))
                except json.JSONDecodeError as exc:
                    raise MatrixArkError(f"Rust TemporalStore {op} daemon returned invalid JSON: {line[:200]!r}") from exc
        raise MatrixArkError(
            f"Rust TemporalStore {op} daemon timed out waiting for response from "
            f"{self._proxy_socket} after {budget_s:.1f}s"
        )

    def _record_call_metrics(
        self,
        op: str,
        kwargs: Json,
        response: Json | None,
        elapsed_ms: float,
        *,
        failed: bool,
        backpressure: bool = False,
        lane: str = "control",
        wait_ms: float = 0.0,
    ) -> None:
        with self._metrics_lock:
            self._commands_total += 1
            self._lane_commands_total[lane] = self._lane_commands_total.get(lane, 0) + 1
            self._lane_wait_ms_total[lane] = self._lane_wait_ms_total.get(lane, 0.0) + max(0.0, wait_ms)
            self._lane_wait_ms_max[lane] = max(self._lane_wait_ms_max.get(lane, 0.0), max(0.0, wait_ms))
            self._op_commands_total[op] = self._op_commands_total.get(op, 0) + 1
            self._op_latency_ms_total[op] = self._op_latency_ms_total.get(op, 0.0) + max(0.0, elapsed_ms)
            self._op_latency_ms_max[op] = max(self._op_latency_ms_max.get(op, 0.0), max(0.0, elapsed_ms))
            if failed:
                self._commands_failed_total += 1
                if "timed out" in str(response or "").lower() or elapsed_ms >= self.request_timeout_ms:
                    self._timeouts_total += 1
            if backpressure:
                self._backpressure_rejections_total += 1
            if response:
                serialization_ms = self._nested_float(
                    response,
                    "serialization_time_ms",
                    "serialization_ms",
                    "serialization_time",
                )
                engine_ms = self._nested_float(
                    response,
                    "rust_engine_time_ms",
                    "engine_ms",
                    "rust_engine_ms",
                )
                self._serialization_ms_total += serialization_ms
                self._serialization_ms_max = max(self._serialization_ms_max, serialization_ms)
                self._rust_engine_ms_total += engine_ms
                self._rust_engine_ms_max = max(self._rust_engine_ms_max, engine_ms)
                self._merge_memory_layer_budget(response)
                scan_count = int(
                    self._nested_float(
                        response,
                        "scan_count",
                        "scan_stats.scanned_records",
                        "context_pack.recall_policy.scan_stats.scanned_records",
                    )
                    or 0
                )
                self._scan_count_total += scan_count
                cache_hit = bool(response.get("cache_hit") or response.get("cache_hit_used"))
                if cache_hit:
                    self._cache_hits_total += 1
                elif op in {"matrixark_scan_candidates", "matrixark_retrieve_context_pack"}:
                    self._cache_misses_total += 1
                selected_count = int(
                    self._nested_float(
                        response,
                        "selected_ref_count",
                        "context_pack.selected_ref_count",
                    )
                    or 0
                )
                if not selected_count and isinstance(response.get("context_pack"), dict):
                    refs = response["context_pack"].get("selected_refs") or response["context_pack"].get("remote_context_refs") or []
                    if isinstance(refs, list):
                        selected_count = len(refs)
                self._selected_refs_total += selected_count
                dropped_count = int(
                    self._nested_float(
                        response,
                        "dropped_ref_count",
                        "context_pack.dropped_ref_count",
                    )
                    or 0
                )
                if not dropped_count and isinstance(response.get("context_pack"), dict):
                    dropped = response["context_pack"].get("dropped_refs")
                    if isinstance(dropped, dict):
                        reasons = dropped.get("reason_counts")
                        if isinstance(reasons, dict):
                            dropped_count = sum(int(value or 0) for value in reasons.values())
                self._dropped_refs_total += dropped_count
                if op in {"matrixark_append_records", "matrixark_batch_append_records"}:
                    if bool(response.get("append_blob_parity")):
                        self._matrixark_append_blob_parity_total += 1
                    if str(response.get("batch_lowering") or "") == "rust_proxy_hset_count_lowering":
                        self._matrixark_append_hset_count_lowering_total += 1
            self._last_latency_ms = elapsed_ms
            self._max_observed_latency_ms = max(self._max_observed_latency_ms, elapsed_ms)
            self._latency_samples_ms.append(elapsed_ms)
            if len(self._latency_samples_ms) > 2048:
                del self._latency_samples_ms[: len(self._latency_samples_ms) - 2048]
            lane_samples = self._lane_latency_samples_ms.setdefault(lane, [])
            lane_samples.append(elapsed_ms)
            if len(lane_samples) > 1024:
                del lane_samples[: len(lane_samples) - 1024]
            if response and response.get("ok"):
                count = int(response.get("count") or 0)
                if op in {"put_string", "hset"}:
                    self._records_written_total += 1
                    self._count_context_record(kwargs.get("value"))
                elif op in {"batch_hset", "matrixark_append_records", "matrixark_batch_append_records"}:
                    compact_entries = kwargs.get("entries_compact") or []
                    entries = kwargs.get("entries") or []
                    self._records_written_total += count or len(compact_entries) or len(entries)
                    for entry in entries:
                        if isinstance(entry, dict):
                            self._count_context_record(entry.get("value"))
                    for entry in compact_entries:
                        if isinstance(entry, (list, tuple)) and len(entry) >= 3:
                            self._count_context_record(entry[2])
                elif op in {"get_string", "hget"}:
                    self._records_read_total += 1
                elif op in {"batch_hget", "hgetall", "scan_hash"}:
                    self._records_read_total += count
                elif op == "matrixark_publish_visibility":
                    visibility_keys = kwargs.get("visibility_keys") if isinstance(kwargs, dict) else []
                    key_count = len(visibility_keys) if isinstance(visibility_keys, list) else 0
                    index_bytes = int(
                        self._nested_float(
                            response,
                            "matrixark_visibility_index_bytes",
                            "extra.matrixark_visibility_index_bytes",
                            "count",
                        )
                        or 0
                    )
                    full_shard = bool(
                        response.get("matrixark_visibility_full_shard")
                        or (isinstance(response.get("extra"), dict) and response["extra"].get("matrixark_visibility_full_shard"))
                        or key_count == 0
                    )
                    self._publish_visibility_calls_total += 1
                    self._publish_visibility_keys_total += key_count
                    self._publish_visibility_full_shard_total += 1 if full_shard else 0
                    self._publish_visibility_index_bytes_total += index_bytes
                    self._publish_visibility_last_key_count = key_count
                    self._publish_visibility_last_index_bytes = index_bytes

    @staticmethod
    def _nested_float(payload: Json, *paths: str) -> float:
        for path in paths:
            current: Any = payload
            for part in path.split("."):
                if not isinstance(current, dict) or part not in current:
                    current = None
                    break
                current = current[part]
            if current is None:
                continue
            try:
                return float(current)
            except (TypeError, ValueError):
                continue
        return 0.0

    @staticmethod
    def _nested_dict(payload: Json, *paths: str) -> Json:
        for path in paths:
            current: Any = payload
            for part in path.split("."):
                if not isinstance(current, dict) or part not in current:
                    current = None
                    break
                current = current[part]
            if isinstance(current, dict):
                return current
        return {}

    @staticmethod
    def _add_bucket_totals(target_bucket: Json, source_bucket: Any) -> None:
        if not isinstance(source_bucket, dict):
            return
        for key, value in source_bucket.items():
            if not isinstance(value, dict):
                continue
            bucket = target_bucket.setdefault(str(key), {"refs": 0, "tokens": 0})
            bucket["refs"] = int(bucket.get("refs") or 0) + int(value.get("refs") or 0)
            bucket["tokens"] = int(bucket.get("tokens") or 0) + int(value.get("tokens") or 0)

    @staticmethod
    def _add_count_totals(target_bucket: Json, source_bucket: Any) -> None:
        if not isinstance(source_bucket, dict):
            return
        for key, value in source_bucket.items():
            try:
                count = int(value or 0)
            except (TypeError, ValueError):
                continue
            if count:
                target_bucket[str(key)] = int(target_bucket.get(str(key)) or 0) + count

    def _merge_memory_layer_budget(self, response: Json) -> None:
        budget = self._nested_dict(
            response,
            "retrieval_metrics.memory_layer_budget",
            "context_pack.retrieval_metrics.memory_layer_budget",
            "context_pack.recall_policy.memory_layer_budget",
        )
        if not budget:
            return
        totals = self._memory_layer_budget_totals
        for bucket_name in [
            "by_memory_scope",
            "by_session_continuity",
            "by_extraction_phase",
            "by_ref_type",
            "by_entity_type",
            "by_source_role",
            "by_hook_type",
            "by_codex_event",
        ]:
            self._add_bucket_totals(totals.setdefault(bucket_name, {}), budget.get(bucket_name))
        for bucket_name in [
            "source_message_counts_by_role",
            "source_hook_counts_by_type",
            "source_codex_event_counts_by_event",
        ]:
            self._add_count_totals(totals.setdefault(bucket_name, {}), budget.get(bucket_name))
        for counter_name in [
            "final_session_boundary_ref_count",
            "provisional_ref_count",
            "final_ref_count",
            "total_selected_refs",
            "total_selected_tokens",
        ]:
            totals[counter_name] = int(totals.get(counter_name) or 0) + int(budget.get(counter_name) or 0)

    def _count_context_record(self, value: Any) -> None:
        if not isinstance(value, str) or not value.startswith("{"):
            return
        try:
            payload = json.loads(value)
        except Exception:
            return
        record_type = str(payload.get("record_type") or "")
        if not record_type:
            return
        self._context_record_counts[record_type] = self._context_record_counts.get(record_type, 0) + 1

    @staticmethod
    def _percentile(values: list[float], percentile: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, math.ceil(percentile * len(ordered)) - 1))
        return ordered[index]

    def matrixark_retrieve_context_pack(
        self,
        *,
        count_key: str,
        record_hash_key: str,
        shard_size: int,
        request: Json,
    ) -> Json:
        return self._call_json(
            "matrixark_retrieve_context_pack",
            count_key=count_key,
            record_hash_key=record_hash_key,
            shard_size=shard_size,
            record_types=[
                "context_compression_event",
                "context_entity",
                "context_event",
                "context_index",
                "context_segment",
                "context_summary",
                "resource_chunk",
                "skill_section",
            ],
            return_index_records=False,
            scope=request.get("scope", {}),
            secondary_index_groups=request.get("secondary_index_groups", []),
            record=request,
        )

    def matrixark_publish_visibility(self, visibility_keys: list[str] | None = None) -> Json:
        return self._call_json("matrixark_publish_visibility", visibility_keys=visibility_keys or [])

    def metrics_snapshot(self) -> Json:
        with self._metrics_lock:
            elapsed_s = max(0.001, time.time() - self._started_at)
            samples = list(self._latency_samples_ms)
            context_counts = dict(sorted(self._context_record_counts.items()))
            memory_layer_budget_totals = {
                key: (dict(value) if isinstance(value, dict) else value)
                for key, value in self._memory_layer_budget_totals.items()
            }
            lane_samples = {lane: list(values) for lane, values in self._lane_latency_samples_ms.items()}
            lane_metrics = {
                lane: {
                    "workers": self._lane_worker_counts.get(lane, 0),
                    "commands_total": self._lane_commands_total.get(lane, 0),
                    "wait_ms_total": round(self._lane_wait_ms_total.get(lane, 0.0), 3),
                    "wait_ms_max": round(self._lane_wait_ms_max.get(lane, 0.0), 3),
                    "queue_wait_ms_total": round(self._lane_wait_ms_total.get(lane, 0.0), 3),
                    "queue_wait_ms_max": round(self._lane_wait_ms_max.get(lane, 0.0), 3),
                    "p95_latency_ms": round(self._percentile(values, 0.95), 3),
                    "p99_latency_ms": round(self._percentile(values, 0.99), 3),
                }
                for lane, values in lane_samples.items()
            }
            op_metrics = {
                op: {
                    "commands_total": count,
                    "latency_ms_total": round(self._op_latency_ms_total.get(op, 0.0), 3),
                    "latency_ms_avg": round(self._op_latency_ms_total.get(op, 0.0) / max(1, count), 3),
                    "latency_ms_max": round(self._op_latency_ms_max.get(op, 0.0), 3),
                }
                for op, count in sorted(self._op_commands_total.items())
            }
            return {
                "gateway_mode": "rust_native_proxy",
                "sdk_mode": "rust_native_proxy",
                "transport": "stdio",
                "proxy_path": self.proxy_path,
                "cli_path": self.cli_path,
                "shared_process_mode": self._shared_process_mode,
                "max_inflight": sum(self._lane_worker_counts.get(group, 0) for group in ("write", "read", "pack", "control")),
                "lane_pool": {
                    "write": self._lane_worker_counts.get("write", 0),
                    "read": self._lane_worker_counts.get("read", 0),
                    "pack": self._lane_worker_counts.get("pack", 0),
                    "control": self._lane_worker_counts.get("control", 0),
                },
                "lanes": lane_metrics,
                "lane_metrics": lane_metrics,
                "lane_worker_counts": dict(self._lane_worker_counts),
                "write_pool_size": self._lane_worker_counts.get("write", 0),
                "read_pool_size": self._lane_worker_counts.get("read", 0),
                "pack_pool_size": self._lane_worker_counts.get("pack", 0),
                "control_pool_size": self._lane_worker_counts.get("control", 0),
                "write_pool_enabled": self._lane_worker_counts.get("write", 0) > 1,
                "read_pool_enabled": self._lane_worker_counts.get("read", 0) > 1,
                "pack_pool_enabled": self._lane_worker_counts.get("pack", 0) > 1,
                "backpressure_timeout_ms": int(self._backpressure_timeout_s * 1000),
                "commands_total": self._commands_total,
                "commands_failed_total": self._commands_failed_total,
                "timeouts_total": self._timeouts_total,
                "qps": round(self._commands_total / elapsed_s, 6),
                "records_written_total": self._records_written_total,
                "records_read_total": self._records_read_total,
                "backpressure_rejections_total": self._backpressure_rejections_total,
                "proxy_queue_wait_ms_total": round(sum(self._lane_wait_ms_total.values()), 3),
                "proxy_queue_wait_ms_max": round(max(self._lane_wait_ms_max.values()) if self._lane_wait_ms_max else 0.0, 3),
                "serialization_ms_total": round(self._serialization_ms_total, 3),
                "serialization_ms_max": round(self._serialization_ms_max, 3),
                "rust_engine_ms_total": round(self._rust_engine_ms_total, 3),
                "rust_engine_ms_max": round(self._rust_engine_ms_max, 3),
                "scan_count_total": self._scan_count_total,
                "cache_hits_total": self._cache_hits_total,
                "cache_misses_total": self._cache_misses_total,
                "selected_refs_total": self._selected_refs_total,
                "dropped_refs_total": self._dropped_refs_total,
                "matrixark_append_blob_parity_total": self._matrixark_append_blob_parity_total,
                "matrixark_append_hset_count_lowering_total": self._matrixark_append_hset_count_lowering_total,
                "matrixark_append_hot_path": (
                    "append_blob"
                    if self._matrixark_append_blob_parity_total > 0
                    and self._matrixark_append_hset_count_lowering_total == 0
                    else "hset_count_lowering"
                    if self._matrixark_append_hset_count_lowering_total > 0
                    else "unknown"
                ),
                "memory_layer_budget_totals": memory_layer_budget_totals,
                "publish_visibility": {
                    "calls_total": self._publish_visibility_calls_total,
                    "keys_total": self._publish_visibility_keys_total,
                    "keys_avg": round(
                        self._publish_visibility_keys_total / max(1, self._publish_visibility_calls_total),
                        3,
                    ),
                    "full_shard_total": self._publish_visibility_full_shard_total,
                    "index_bytes_total": self._publish_visibility_index_bytes_total,
                    "index_bytes_avg": round(
                        self._publish_visibility_index_bytes_total / max(1, self._publish_visibility_calls_total),
                        3,
                    ),
                    "last_key_count": self._publish_visibility_last_key_count,
                    "last_index_bytes": self._publish_visibility_last_index_bytes,
                },
                "last_latency_ms": round(self._last_latency_ms, 3),
                "latency_ms_sum": round(sum(samples), 3),
                "latency_ms_count": len(samples),
                "latency_ms_max": round(max(samples) if samples else 0.0, 3),
                "latency_buckets": {str(int(bucket) if bucket != float("inf") else "+Inf"): sum(1 for value in samples if value <= bucket) for bucket in MatrixArkServiceMetrics.LATENCY_BUCKETS_MS},
                "p95_latency_ms": round(self._percentile(samples, 0.95), 3),
                "p99_latency_ms": round(self._percentile(samples, 0.99), 3),
                "max_observed_latency_ms": round(self._max_observed_latency_ms, 3),
                "matrixark_context_records_total": sum(context_counts.values()),
                "matrixark_context_records_by_type": context_counts,
                "op_metrics": op_metrics,
                "process_per_operation_enabled": False,
                "single_shot_mode": "debug_only",
                "native_proxy": True,
                "direct_sdk_bridge": False,
                "pure_embedded_direct_sdk": False,
                "supports_health": True,
                "supports_readiness": True,
                "supports_metrics": True,
                "supports_batch_append": True,
                "supports_matrixark_batch_append_records": True,
                "supports_matrixark_retrieve_context_pack": True,
                "supports_compact_secondary_index_lookup": True,
                "supports_placement_key_candidate_fetch": True,
                "supports_context_pack_telemetry": True,
                "supports_native_append_queue": True,
                "supports_coalesced_writes": True,
                "supports_placement_key_routing": True,
                "supports_prefix_scan": True,
                "supports_graceful_shutdown": True,
                "structured_errors": True,
                "matrixark_batch_append_wire_format": "entries_compact",
            }

    def _call(self, op: str, **kwargs: Any) -> str:
        response = self._call_json(op, **kwargs)
        return str(response.get("value", ""))

    def put_string(self, key: str, value: str) -> None:
        self._call("put_string", key=key, value=value)

    def get_string(self, key: str) -> str:
        return self._call("get_string", key=key)

    def hset(self, key: str, field: str, value: str) -> None:
        self._call("hset", key=key, field=field, value=value)

    def hget(self, key: str, field: str) -> str:
        return self._call("hget", key=key, field=field)

    def resource_blob_put(self, tenant_hash: int, payload_base64: str) -> Json:
        return self._call_json(
            "matrixark_resource_blob_put", key=str(int(tenant_hash)), value=payload_base64
        )

    def resource_blob_fetch(self, uri: str, *, offset: int = 0, length: int = 0) -> Json:
        return self._call_json(
            "matrixark_resource_blob_fetch", key=str(uri), blob_offset=int(offset), blob_length=int(length)
        )

    def resource_blob_sweep(self, tenant_hash: int, referenced_hashes: list[str], min_age_ms: int) -> Json:
        return self._call_json(
            "matrixark_resource_blob_sweep",
            key=str(int(tenant_hash)),
            blob_referenced_hashes=[str(item) for item in referenced_hashes],
            blob_min_age_ms=int(min_age_ms),
        )

    def batch_hset(self, entries: list[Json]) -> None:
        if not entries:
            return
        compact_entries = [
            [str(entry.get("key") or ""), str(entry.get("field") or ""), str(entry.get("value") or "")]
            for entry in entries
            if isinstance(entry, dict)
        ]
        self._call_json("batch_hset", entries_compact=compact_entries)

    def matrixark_batch_append_records(
        self,
        entries: list[Json],
        *,
        count_key: str | None = None,
        count_value: str | None = None,
        append_options: Json | None = None,
    ) -> None:
        if not entries and not count_key:
            return
        compact_entries: list[list[str]] = []
        routed_entries: list[Json] = []
        has_routes = False
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            key = str(entry.get("key") or "")
            field = str(entry.get("field") or "")
            value = str(entry.get("value") or "")
            route = entry.get("storage_route")
            route_json = str(entry.get("route_json") or "")
            if not route_json and isinstance(route, dict):
                route_json = json.dumps(route, separators=(",", ":"), sort_keys=True)
            if route_json and route_json != "{}":
                has_routes = True
            compact_entries.append([key, field, value])
            routed_entries.append(
                {
                    "key": key,
                    "field": field,
                    "value": value,
                    "route_json": route_json or "{}",
                }
            )
        self._call_json(
            "matrixark_batch_append_records",
            entries=None if not has_routes else routed_entries,
            entries_compact=compact_entries if not has_routes else None,
            key=count_key or "",
            value=count_value or "",
            append_options=append_options or {},
        )

    def matrixark_append_records(
        self,
        entries: list[Json],
        *,
        count_key: str | None = None,
        count_value: str | None = None,
        append_options: Json | None = None,
    ) -> None:
        self.matrixark_batch_append_records(
            entries,
            count_key=count_key,
            count_value=count_value,
            append_options=append_options,
        )

    def matrixark_retrieve_context_pack(
        self,
        *,
        count_key: str,
        record_hash_key: str,
        shard_size: int,
        request: Json,
    ) -> Json:
        response = self._call_json(
            "matrixark_retrieve_context_pack",
            count_key=count_key,
            record_hash_key=record_hash_key,
            shard_size=shard_size,
            record_types=[
                "context_compression_event",
                "context_entity",
                "context_event",
                "context_index",
                "context_segment",
                "context_summary",
                "resource_chunk",
                "skill_section",
            ],
            return_index_records=False,
            scope=request.get("scope", {}),
            secondary_index_groups=request.get("secondary_index_groups", []),
            record=request,
            top_level_response=True,
        )
        value = response.get("value")
        if isinstance(value, str) and value:
            decoded = json.loads(value)
            if isinstance(decoded, dict):
                return decoded
        return response

    def batch_hget(self, entries: list[Json]) -> list[Json]:
        if not entries:
            return []
        compact_entries = [
            [str(entry.get("key") or ""), str(entry.get("field") or ""), ""]
            for entry in entries
            if isinstance(entry, dict)
        ]
        response = self._call_json("batch_hget", entries_compact=compact_entries)
        records = response.get("records", [])
        return records if isinstance(records, list) else []

    def scan_hash(self, key: str) -> Json:
        return self._call_json("scan_hash", key=key)

    def matrixark_delete_records(
        self,
        *,
        count_key: str,
        record_hash_key: str,
        shard_size: int,
        record_ids: list[str],
    ) -> Json:
        """Remove the named records in the engine.

        A deliberately dumb primitive: the caller decides what a delete covers, the engine removes
        what matches. An empty id list removes nothing.
        """
        return self._call_json(
            "matrixark_delete_records",
            count_key=count_key,
            record_hash_key=record_hash_key,
            shard_size=shard_size,
            record_ids=[str(item) for item in record_ids],
        )

    def matrixark_forget_scope(
        self,
        *,
        count_key: str,
        record_hash_key: str,
        shard_size: int,
        scope: Json,
    ) -> Json:
        """Physically remove a subject's records in the engine.

        The engine refuses an under-specified scope, so this cannot be used to wipe a store by
        accident; it rewrites each hash field to its survivors, commits one durable batch, and
        clears the engine's scan/hgetall caches.
        """
        return self._call_json(
            "matrixark_forget_scope",
            count_key=count_key,
            record_hash_key=record_hash_key,
            shard_size=shard_size,
            scope=scope,
        )

    def matrixark_scan_candidates(
        self,
        *,
        count_key: str,
        record_hash_key: str,
        shard_size: int,
        scope: Json,
        record_types: list[str],
        secondary_index_groups: list[list[str]],
        selected_node_hashes: list[int],
        record_ids: list[str] | None = None,
        return_index_records: bool = False,
        newest_by_type: Json | None = None,
    ) -> Json:
        extra: Json = {}
        if record_ids:
            extra["record_ids"] = [str(item) for item in record_ids]
        if return_index_records:
            extra["return_index_records"] = True
        if newest_by_type:
            extra["newest_by_type"] = {
                str(record_type): int(limit) for record_type, limit in newest_by_type.items()
            }
        return self._call_json(
            "matrixark_scan_candidates",
            count_key=count_key,
            record_hash_key=record_hash_key,
            shard_size=shard_size,
            scope=scope,
            record_types=record_types,
            secondary_index_groups=secondary_index_groups,
            selected_node_hashes=selected_node_hashes,
            **extra,
        )

    def metrics_prometheus(self) -> str:
        return str(self._call_json("metrics_prometheus").get("prometheus", ""))

    def health(self) -> Json:
        return self._call_json("health")

    def readiness(self) -> Json:
        return self._call_json("readiness")

    def shutdown(self) -> None:
        try:
            self._call_json("shutdown")
        finally:
            self.close()


MatrixArkRustCliClient = MatrixArkRustProxyClient


class MatrixArkTemporalStoreRustAdapter(MatrixArkTemporalStoreDirectAdapter):
    """MatrixArk adapter backed by the Rust TemporalStore proxy or direct SDK."""

    def __init__(
        self,
        *,
        rust_cli: str = "",
        rust_proxy: str = "",
        metaserver: str,
        namespace: str,
        table: str,
        storage_prefix: str = "matrixark:mcp",
        request_timeout_ms: int = 20000,
        io_timeout_ms: int = 20000,
        sdk_mode: str = "proxy",
    ) -> None:
        MatrixArkLocalAdapter.__init__(self, Path("/tmp/matrixark-mcp-unused-rust.jsonl"))
        MatrixArkLocalAdapter._init_local_runtime_state(self)
        self._entity_cache_loaded = True
        self._context_node_cache_loaded = True
        self._retrieval_candidate_cache: dict[str, Json] = {}
        self._retrieval_candidate_cache_lock = threading.RLock()
        self._disk_fallback_adapter: MatrixArkLocalAdapter | None = None
        self._disk_fallback_path = disk_fallback_store_path()
        self._disk_fallback_enabled = bool(self._disk_fallback_path)
        self._disk_fallback_write_failures = 0
        self._metaserver = metaserver
        self._namespace = namespace
        self._table = table
        proxy_path = rust_proxy or rust_cli
        direct_lib = os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_DIRECT_LIB", "").strip()
        temporalstore_lib = os.environ.get("TEMPORALSTORE_LIB", os.environ.get("MATRIXARK_TEMPORALSTORE_LIB", "")).strip()
        self._rust_direct_cdylib_enabled = bool(
            sdk_mode in {"direct-sdk", "direct_sdk", "native-binding", "rust-direct"}
            and direct_lib
            and Path(direct_lib).exists()
        )
        if self._rust_direct_cdylib_enabled:
            self._client = MatrixArkRustCdylibClient(
                library_path=direct_lib,
                temporalstore_lib=temporalstore_lib,
                metaserver=metaserver,
                namespace=namespace,
                table=table,
                request_timeout_ms=request_timeout_ms,
                io_timeout_ms=io_timeout_ms,
            )
        else:
            self._client = MatrixArkRustProxyClient(
                proxy_path=proxy_path,
                metaserver=metaserver,
                namespace=namespace,
                table=table,
                request_timeout_ms=request_timeout_ms,
                io_timeout_ms=io_timeout_ms,
                sdk_mode=sdk_mode,
            )
        self._retrieve_client: Any | None = None
        self._summary_client: Any | None = None
        self._retrieve_client_lock = threading.RLock()
        self._summary_client_lock = threading.RLock()
        self._dedicated_proxy_clients_enabled = os.environ.get(
            "MATRIXARK_RUST_PROXY_DEDICATED_CLIENTS",
            "0",
        ).strip().lower() in {"1", "true", "yes"}
        self._dedicated_pack_lanes_enabled = os.environ.get(
            "MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES",
            "0",
        ).strip().lower() in {"1", "true", "yes"}
        self._publish_visibility_after_flush = (
            os.environ.get("MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_AFTER_FLUSH")
            or os.environ.get("MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_ON_FLUSH")
            or "0"
        ).strip().lower() in {"1", "true", "yes"}
        self._rust_proxy_path = proxy_path
        self._rust_request_timeout_ms = request_timeout_ms
        self._rust_io_timeout_ms = io_timeout_ms
        self._rust_sdk_mode = sdk_mode
        self._metaserver = metaserver
        self._namespace = namespace
        self._table = table
        self._readiness_cache: Json | None = None
        self._readiness_lock = threading.RLock()
        self._storage_prefix = storage_prefix.rstrip(":")
        self._supported_storage_families = self._parse_supported_storage_families()
        self._record_hash_key = f"{self._storage_prefix}:records"
        self._index_key = f"{self._storage_prefix}:record_index"
        self._count_key = f"{self._storage_prefix}:record_count"
        self._matrixark_native_batch_append_available = True
        if self._rust_direct_cdylib_enabled:
            self._matrixark_append_write_path = "rust_direct_cdylib_matrixark_batch_append_records"
        else:
            self._matrixark_append_write_path = (
                "rust_direct_sdk_matrixark_batch_append_records"
                if sdk_mode in {"direct-sdk", "direct_sdk", "native-binding", "rust-direct"}
                else "rust_proxy_matrixark_batch_runtime_default"
            )
        self._matrixark_append_uses_per_record_hset = False
        self._matrixark_batch_append_uses_existing_batch_execute = True
        self._matrixark_batch_append_existing_batch_execute_source = "temporalstore_matrixark_batch_append_records"
        self._shard_size = DIRECT_RECORD_LOG_SHARD_SIZE
        self._index_cache: list[str] | None = None
        self._records_cache: list[Json] | None = None
        self._entry_count_cache: int | None = None
        self._legacy_index_mode = False
        self._records_lock = threading.RLock()
        self._audit_lock = threading.RLock()
        self._audit_buffer: list[Json] = []
        self._audit_flusher_started = False
        self._audit_flush_failures = 0
        if DIRECT_AUDIT_MODE not in {"buffered", "deferred", "drop", "sync"}:
            raise MatrixArkError("MATRIXARK_DIRECT_AUDIT_MODE must be buffered, deferred, drop, or sync")
        self._audit_mode = DIRECT_AUDIT_MODE
        self._audit_buffer_max_records = max(1, DIRECT_AUDIT_BUFFER_MAX_RECORDS)
        self._audit_flush_interval_s = max(0.05, DIRECT_AUDIT_FLUSH_INTERVAL_MS / 1000.0)
        self._write_retries = max(0, DIRECT_WRITE_RETRIES)
        self._write_backoff_s = max(0.0, DIRECT_WRITE_BACKOFF_MS / 1000.0)
        self._write_throttle_s = max(0.0, DIRECT_WRITE_THROTTLE_MS / 1000.0)
        self._backend_ready = False
        self._backend_ready_result = None
        self._backend_readiness_lock = threading.RLock()

    def _native_summary_client(self) -> Any:
        """Return a dedicated summary/audit lane when the backend transport needs one."""

        if getattr(self, "_rust_direct_cdylib_enabled", False) or not getattr(self, "_dedicated_proxy_clients_enabled", False):
            return self._client
        with self._summary_client_lock:
            if self._summary_client is None:
                self._summary_client = MatrixArkRustProxyClient(
                    proxy_path=self._rust_proxy_path,
                    metaserver=self._metaserver,
                    namespace=self._namespace,
                    table=self._table,
                    request_timeout_ms=self._rust_request_timeout_ms,
                    io_timeout_ms=self._rust_io_timeout_ms,
                    sdk_mode=self._rust_sdk_mode,
                )
            return self._summary_client

    def _append_client_for_records(self, records: list[Json]) -> Any:
        hot_record_types = {
            "context_event",
            "context_entity",
            "context_segment",
            "resource_chunk",
            "resource_manifest",
            "resource_registry",
            "skill_manifest",
            "skill_section",
            "skill_registry_update",
            "context_index",
        }
        summary_or_audit_types = {
            "context_child_ref",
            "context_embedding",
            "context_pack_audit",
            "context_summary",
            "context_summary_dirty",
            "context_summary_refresh_audit",
            "matrixark_audit_log",
        }
        record_types = {str(record.get("record_type") or "") for record in records if isinstance(record, dict)}
        if record_types and not (record_types & hot_record_types) and (record_types & summary_or_audit_types):
            return self._native_summary_client()
        return self._client

    def _native_retrieve_client(self) -> Any:
        """Return the native serving read client.

        Rust cdylib direct mode is in-process and does not need a proxy lane;
        the stdio proxy path keeps a dedicated read lane to avoid head-of-line
        blocking behind writes or audit flushes.
        """

        if getattr(self, "_rust_direct_cdylib_enabled", False) or not getattr(self, "_dedicated_proxy_clients_enabled", False):
            return self._client
        with self._retrieve_client_lock:
            if self._retrieve_client is None:
                self._retrieve_client = MatrixArkRustProxyClient(
                    proxy_path=self._rust_proxy_path,
                    metaserver=self._metaserver,
                    namespace=self._namespace,
                    table=self._table,
                    request_timeout_ms=self._rust_request_timeout_ms,
                    io_timeout_ms=self._rust_io_timeout_ms,
                    sdk_mode=self._rust_sdk_mode,
                )
            return self._retrieve_client

    def _raw_ingestion_visibility_required_after_flush(self) -> bool:
        return bool(getattr(self, "_publish_visibility_after_flush", False))

    def flush_direct_writes(self, timeout_s: float | None = None) -> None:
        super().flush_direct_writes(timeout_s=timeout_s)
        if not getattr(self, "_publish_visibility_after_flush", False):
            return
        visibility_keys = self._consume_pending_visibility_keys()
        publisher = getattr(self._client, "matrixark_publish_visibility", None)
        if not callable(publisher):
            return
        if not visibility_keys:
            publisher(visibility_keys=[])
            return
        for keys in self._visibility_key_groups_by_partition(visibility_keys):
            publisher(visibility_keys=keys)

    def _visibility_key_groups_by_partition(self, visibility_keys: list[str]) -> list[list[str]]:
        groups: dict[str, list[str]] = {}
        order: list[str] = []
        for key in visibility_keys:
            partition = self._visibility_key_partition(str(key or ""))
            if partition not in groups:
                groups[partition] = []
                order.append(partition)
            groups[partition].append(str(key or ""))
        return [groups[partition] for partition in order if groups[partition]]

    def _visibility_key_partition(self, key: str) -> str:
        key = str(key or "").strip()
        for marker in (
            ":records",
            ":record_count",
            ":record_index",
            ":event_time",
            ":readiness",
            ":direct_write_queue",
            ":context_event_by_ingestion_time",
            ":context_latest_state",
            ":context_ref_locator",
            ":context_index_lookup",
            ":context_placement_lookup",
        ):
            if marker in key:
                prefix, _ = key.split(marker, 1)
                if prefix:
                    return prefix
        return key

    def _should_publish_visibility_key(self, key: str) -> bool:
        key = str(key or "")
        raw_count_key = str(getattr(self, "_raw_count_key", "") or "")
        raw_record_hash_key = str(getattr(self, "_raw_record_hash_key", "") or "")
        return (
            key == self._count_key
            or key.startswith(f"{self._record_hash_key}:")
            or (raw_count_key and key == raw_count_key)
            or (raw_record_hash_key and key.startswith(f"{raw_record_hash_key}:"))
        )

    def close(self, *, timeout_s: float = 5.0) -> None:
        try:
            self.flush_direct_writes(timeout_s=timeout_s)
        finally:
            for attr in ("_retrieve_client", "_summary_client", "_client"):
                client = getattr(self, attr, None)
                close = getattr(client, "close", None)
                if callable(close):
                    close()
            super_close = getattr(super(), "close", None)
            if callable(super_close):
                try:
                    super_close(timeout_s=timeout_s)
                except TypeError:
                    super_close()

    def supports_native_candidate_prefilter(self) -> bool:
        return True

    def supports_native_context_pack(self) -> bool:
        return True

    def _recovery_status_snapshot(self, native_metrics: Json | None = None) -> Json:
        snapshot = super()._recovery_status_snapshot(native_metrics=native_metrics)
        native_metrics = native_metrics or {}
        shared_read_throughs = int(
            native_metrics.get("shared_store_read_throughs")
            or native_metrics.get("shared_store_read_through_count")
            or native_metrics.get("read_through_count")
            or 0
        )
        page_reads = int(native_metrics.get("page_store_reads") or native_metrics.get("page_reads") or 0)
        cache_warmups = int(native_metrics.get("cache_warmup_page_refs") or native_metrics.get("cache_warmup_warmed_page_refs") or 0)
        replicated_recovery = bool(
            shared_read_throughs
            or page_reads
            or cache_warmups
            or str(native_metrics.get("storage_family") or "").strip().lower() in {"shared_store", "raft"}
        )
        snapshot.update(
            {
                "status": "native_replicated_storage_ready" if replicated_recovery else snapshot.get("status", "unknown"),
                "recovery_source": "rust_replicated_page_store_read_through",
                "read_through_cache_warmup": bool(shared_read_throughs or cache_warmups),
                "replicated_storage_recovery": replicated_recovery,
                "shared_store_read_throughs": shared_read_throughs,
                "page_store_reads": page_reads,
                "cache_warmup_page_refs": cache_warmups,
                "cache_hits_total": int(native_metrics.get("cache_hits_total") or 0),
                "cache_misses_total": int(native_metrics.get("cache_misses_total") or 0),
            }
        )
        return snapshot

    def native_context_pack(self, request: Json) -> Json | None:
        self._recover_serving_from_disk_fallback_if_needed(reason="native_context_pack")
        retriever = self._native_retrieve_client().matrixark_retrieve_context_pack
        try:
            response = retriever(
                count_key=self._count_key,
                record_hash_key=self._record_hash_key,
                shard_size=self._shard_size,
                request=request,
            )
        except Exception as exc:
            if self.native_context_pack_required():
                raise MatrixArkError(
                    f"backend-native ContextPack assembly failed for {self._backend_label()}: {exc}. "
                    "Python reference packing is disabled for TemporalStore serving unless explicitly overridden for local debug."
                ) from exc
            return None
        if not isinstance(response, dict) or not response.get("native_pack_assembly"):
            if self.native_context_pack_required():
                raise MatrixArkError(
                    f"backend-native ContextPack assembly returned an invalid response for {self._backend_label()}. "
                    "Python reference packing is disabled for TemporalStore serving unless explicitly overridden for local debug."
                )
            return None
        if isinstance(response.get("records"), list):
            raise MatrixArkError("native matrixark_retrieve_context_pack must return a finished ContextPack, not raw records")
        pack = response.get("context_pack")
        if not isinstance(pack, dict):
            return None
        pack.setdefault("context_pack_assembly", "native_backend")
        pack.setdefault("backend", self._backend_label())
        recall_policy = pack.get("recall_policy") if isinstance(pack.get("recall_policy"), dict) else {}
        contract = recall_policy.get("native_response_contract") if isinstance(recall_policy.get("native_response_contract"), dict) else {}
        contract.setdefault("raw_records_returned_to_python", False)
        contract.setdefault("python_hot_path_records", 0)
        contract.setdefault("python_role", "dispatch_request_receive_context_pack")
        contract.setdefault("backend_role", "scan_filter_score_pack")
        contract.setdefault("rust_proxy_dedicated_retrieve_lane", bool(getattr(self, "_dedicated_proxy_clients_enabled", False)))
        recall_policy["native_response_contract"] = contract
        pack["recall_policy"] = recall_policy
        return pack

    def _native_candidate_scan(
        self,
        *,
        scope: Json,
        record_types: set[str],
        secondary_index_groups: list[set[str]] | None,
        selected_node_hashes: set[int] | None,
    ) -> Json | None:
        try:
            response = self._native_retrieve_client().matrixark_scan_candidates(
                count_key=self._count_key,
                record_hash_key=self._record_hash_key,
                shard_size=self._shard_size,
                scope=scope,
                record_types=sorted(record_types),
                secondary_index_groups=[sorted(group) for group in (secondary_index_groups or [])],
                selected_node_hashes=sorted(int(item) for item in (selected_node_hashes or set())),
            )
        except Exception as exc:
            if native_candidate_prefilter_required(backend_label=self._backend_label()):
                raise MatrixArkError(
                    f"backend-native candidate prefilter failed for {self._backend_label()}: {exc}. "
                    "Python read_all scan/prefilter is disabled for TemporalStore serving unless explicitly overridden for local debug."
                ) from exc
            return None
        records = response.get("records") if isinstance(response, dict) else None
        if not isinstance(records, list):
            if native_candidate_prefilter_required(backend_label=self._backend_label()):
                raise MatrixArkError(
                    f"backend-native candidate prefilter returned an invalid response for {self._backend_label()}. "
                    "Python read_all scan/prefilter is disabled for TemporalStore serving unless explicitly overridden for local debug."
                )
            return None
        scan_stats = dict(response.get("scan_stats") or {})
        scan_stats.setdefault("backend", self._backend_label())
        scan_stats.setdefault("execution_mode", "native_temporalstore_candidate_prefilter")
        scan_stats.setdefault("backend_pushdown", True)
        scan_stats.setdefault("direct_backend_prefilter", True)
        scan_stats.setdefault("native_pushdown", True)
        scan_stats.setdefault("native_prefix_scan", True)
        scan_stats.setdefault("native_secondary_index_prefilter", bool(secondary_index_groups))
        scan_stats.setdefault("native_pack_assembly", False)
        scan_stats.setdefault("cache_hit", False)
        scan_stats.setdefault("record_types", sorted(record_types))
        scan_stats.setdefault("selected_node_hashes_supplied", len(selected_node_hashes or set()))
        scan_stats.setdefault("pack_assembly_location", "native_backend_candidate_scan")
        scan_stats.setdefault("rust_proxy_dedicated_retrieve_lane", True)
        latest_state_records = self._latest_context_state_records_for_candidate_scan(
            scope=scope,
            record_types=record_types,
            selected_node_hashes=selected_node_hashes,
        )
        if latest_state_records:
            records = list(records) + latest_state_records
        records = compact_latest_context_state_records(records)
        scan_stats["latest_summary_state_compaction"] = True
        scan_stats["latest_state_records_loaded"] = len(latest_state_records)
        return {"records": records, "scan_stats": scan_stats}

    def _backend_metaserver(self) -> str:
        return self._client.metaserver

    def _backend_label(self) -> str:
        return "temporalstore-rust"

    def _rust_storage_mode_label(self) -> str:
        sdk_mode = getattr(self._client, "sdk_mode", "")
        if sdk_mode == "direct_cdylib":
            return "rust-direct-cdylib"
        if sdk_mode == "direct_sdk":
            return "rust-direct-sdk-bridge"
        return "rust-proxy"

    def _backend_neutral_prometheus(self, snapshot: Json) -> str:
        backend = "rust"
        storage_mode = self._rust_storage_mode_label()
        buckets = snapshot.get("latency_buckets") if isinstance(snapshot.get("latency_buckets"), dict) else {}
        lines = [
            "# HELP matrixark_backend_qps MatrixArk storage backend command QPS.",
            "# TYPE matrixark_backend_qps gauge",
            f'matrixark_backend_qps{{backend="{backend}"}} {snapshot.get("qps", 0)}',
            "# HELP matrixark_backend_commands_total MatrixArk storage backend command count.",
            "# TYPE matrixark_backend_commands_total counter",
            f'matrixark_backend_commands_total{{backend="{backend}"}} {int(snapshot.get("commands_total") or 0)}',
            "# HELP matrixark_backend_errors_total MatrixArk storage backend command errors.",
            "# TYPE matrixark_backend_errors_total counter",
            f'matrixark_backend_errors_total{{backend="{backend}"}} {int(snapshot.get("commands_failed_total") or 0)}',
            "# HELP matrixark_backend_timeouts_total MatrixArk storage backend command timeouts.",
            "# TYPE matrixark_backend_timeouts_total counter",
            f'matrixark_backend_timeouts_total{{backend="{backend}"}} {int(snapshot.get("timeouts_total") or 0)}',
            "# HELP matrixark_backend_info MatrixArk storage backend identity and mode.",
            "# TYPE matrixark_backend_info gauge",
            f'matrixark_backend_info{{backend="{backend}",storage_mode="{storage_mode}"}} 1',
            "# HELP matrixark_backend_ready MatrixArk storage backend readiness, 1 for ready and 0 for not ready.",
            "# TYPE matrixark_backend_ready gauge",
            f'matrixark_backend_ready{{backend="{backend}",storage_mode="{storage_mode}",status="{"ready" if self._backend_ready else "unknown"}"}} {1 if self._backend_ready else 0}',
            "# HELP matrixark_backend_command_latency_ms MatrixArk storage backend command latency quantiles.",
            "# TYPE matrixark_backend_command_latency_ms gauge",
            f'matrixark_backend_command_latency_ms{{backend="{backend}",quantile="0.50"}} {round(_latency_quantile_from_bucket_map(buckets, int(snapshot.get("latency_ms_count") or 0), 0.50), 3)}',
            f'matrixark_backend_command_latency_ms{{backend="{backend}",quantile="0.95"}} {round(_latency_quantile_from_bucket_map(buckets, int(snapshot.get("latency_ms_count") or 0), 0.95), 3)}',
            f'matrixark_backend_command_latency_ms{{backend="{backend}",quantile="0.99"}} {round(_latency_quantile_from_bucket_map(buckets, int(snapshot.get("latency_ms_count") or 0), 0.99), 3)}',
            "# HELP matrixark_backend_command_latency_ms_bucket MatrixArk storage backend command latency buckets.",
            "# TYPE matrixark_backend_command_latency_ms_bucket counter",
        ]
        for bucket, count in buckets.items():
            lines.append(f'matrixark_backend_command_latency_ms_bucket{{backend="{backend}",le="{bucket}"}} {int(count)}')
        lines.extend(
            [
                "# HELP matrixark_backend_command_latency_ms_sum MatrixArk storage backend command latency sum in milliseconds.",
                "# TYPE matrixark_backend_command_latency_ms_sum counter",
                f'matrixark_backend_command_latency_ms_sum{{backend="{backend}"}} {snapshot.get("latency_ms_sum", 0)}',
                "# HELP matrixark_backend_command_latency_ms_count MatrixArk storage backend command latency sample count.",
                "# TYPE matrixark_backend_command_latency_ms_count counter",
                f'matrixark_backend_command_latency_ms_count{{backend="{backend}"}} {int(snapshot.get("latency_ms_count") or 0)}',
                "# HELP matrixark_backend_command_latency_max_ms MatrixArk storage backend maximum command latency in milliseconds.",
                "# TYPE matrixark_backend_command_latency_max_ms gauge",
                f'matrixark_backend_command_latency_max_ms{{backend="{backend}"}} {snapshot.get("latency_ms_max", 0)}',
                "# HELP matrixark_backend_records_written_total MatrixArk storage backend records written.",
                "# TYPE matrixark_backend_records_written_total counter",
                f'matrixark_backend_records_written_total{{backend="{backend}"}} {int(snapshot.get("records_written_total") or 0)}',
                "# HELP matrixark_backend_records_read_total MatrixArk storage backend records read.",
                "# TYPE matrixark_backend_records_read_total counter",
                f'matrixark_backend_records_read_total{{backend="{backend}"}} {int(snapshot.get("records_read_total") or 0)}',
                "# HELP matrixark_context_records_total MatrixArk context records currently cached by backend.",
                "# TYPE matrixark_context_records_total gauge",
                f'matrixark_context_records_total{{backend="{backend}"}} {int(snapshot.get("matrixark_context_records_total") or 0)}',
                "# HELP matrixark_backend_cached_clients MatrixArk storage backend cached clients.",
                "# TYPE matrixark_backend_cached_clients gauge",
                f'matrixark_backend_cached_clients{{backend="{backend}"}} {int(snapshot.get("clients_created_total") or 1)}',
                "# HELP matrixark_backend_audit_buffered_records MatrixArk buffered audit records awaiting flush.",
                "# TYPE matrixark_backend_audit_buffered_records gauge",
                f'matrixark_backend_audit_buffered_records{{backend="{backend}"}} {len(getattr(self, "_audit_buffer", []))}',
                "# HELP matrixark_backend_audit_flush_failures_total MatrixArk audit flush failure count.",
                "# TYPE matrixark_backend_audit_flush_failures_total counter",
                f'matrixark_backend_audit_flush_failures_total{{backend="{backend}"}} {int(getattr(self, "_audit_flush_failures", 0) or 0)}',
                "# HELP matrixark_backend_proxy_queue_wait_ms_total Total proxy lane queue wait time in milliseconds.",
                "# TYPE matrixark_backend_proxy_queue_wait_ms_total counter",
                f'matrixark_backend_proxy_queue_wait_ms_total{{backend="{backend}"}} {snapshot.get("proxy_queue_wait_ms_total", 0)}',
                "# HELP matrixark_backend_serialization_time_ms_total Total Rust proxy JSON serialization time in milliseconds.",
                "# TYPE matrixark_backend_serialization_time_ms_total counter",
                f'matrixark_backend_serialization_time_ms_total{{backend="{backend}"}} {snapshot.get("serialization_ms_total", 0)}',
                "# HELP matrixark_backend_rust_engine_time_ms_total Total Rust engine execution time in milliseconds.",
                "# TYPE matrixark_backend_rust_engine_time_ms_total counter",
                f'matrixark_backend_rust_engine_time_ms_total{{backend="{backend}"}} {snapshot.get("rust_engine_ms_total", 0)}',
                "# HELP matrixark_retrieve_scan_count_total Total records scanned by native MatrixArk retrieval.",
                "# TYPE matrixark_retrieve_scan_count_total counter",
                f'matrixark_retrieve_scan_count_total{{backend="{backend}"}} {int(snapshot.get("scan_count_total") or 0)}',
                "# HELP matrixark_retrieve_cache_hits_total Total native MatrixArk retrieval cache hits.",
                "# TYPE matrixark_retrieve_cache_hits_total counter",
                f'matrixark_retrieve_cache_hits_total{{backend="{backend}"}} {int(snapshot.get("cache_hits_total") or 0)}',
                "# HELP matrixark_context_pack_selected_refs_total Total refs selected by native ContextPack assembly.",
                "# TYPE matrixark_context_pack_selected_refs_total counter",
                f'matrixark_context_pack_selected_refs_total{{backend="{backend}"}} {int(snapshot.get("selected_refs_total") or 0)}',
                "# HELP matrixark_context_pack_dropped_refs_total Total refs dropped by native ContextPack assembly.",
                "# TYPE matrixark_context_pack_dropped_refs_total counter",
                f'matrixark_context_pack_dropped_refs_total{{backend="{backend}"}} {int(snapshot.get("dropped_refs_total") or 0)}',
                "# HELP matrixark_append_blob_parity_total MatrixArk append commands using append-blob parity semantics.",
                "# TYPE matrixark_append_blob_parity_total counter",
                f'matrixark_append_blob_parity_total{{backend="{backend}"}} {int(snapshot.get("matrixark_append_blob_parity_total") or 0)}',
                "# HELP matrixark_append_hset_count_lowering_total MatrixArk append commands lowered to hset plus count updates.",
                "# TYPE matrixark_append_hset_count_lowering_total counter",
                f'matrixark_append_hset_count_lowering_total{{backend="{backend}"}} {int(snapshot.get("matrixark_append_hset_count_lowering_total") or 0)}',
            ]
        )
        return "\n".join(lines) + "\n"

    def backend_metrics(self) -> Json:
        health: Json
        readiness: Json
        try:
            health = self._client.health()
        except Exception as exc:
            health = {"ok": False, "error": str(exc)}
        try:
            readiness = self._client.readiness()
        except Exception as exc:
            readiness = {"ok": False, "error": str(exc)}
        rust_client_metrics = self._client.metrics_snapshot()
        rust_retrieve_metrics: Json | None = None
        rust_summary_metrics: Json | None = None
        if self._retrieve_client is not None:
            rust_retrieve_metrics = self._retrieve_client.metrics_snapshot()
        if self._summary_client is not None:
            rust_summary_metrics = self._summary_client.metrics_snapshot()
        try:
            prometheus = self._backend_neutral_prometheus(rust_client_metrics) + self._client.metrics_prometheus()
            if self._retrieve_client is not None:
                prometheus += self._retrieve_client.metrics_prometheus()
            if self._summary_client is not None:
                prometheus += self._summary_client.metrics_prometheus()
        except Exception as exc:
            prometheus = self._backend_neutral_prometheus(rust_client_metrics) + f"# matrixark_rust_proxy_metrics_error {json.dumps(str(exc))}\n"
        gateway_mode = str(rust_client_metrics.get("gateway_mode") or "rust_proxy")
        proxy_mode = str(rust_client_metrics.get("proxy_mode") or "rust_proxy_stdio")
        sdk_mode = str(rust_client_metrics.get("sdk_mode") or getattr(self._client, "sdk_mode", "proxy"))
        return {
            "backend": self._backend_label(),
            "metrics_format": "prometheus",
            "gateway_mode": gateway_mode,
            "sdk_mode": sdk_mode,
            "production_path": "rust_native_proxy" if not self._rust_direct_cdylib_enabled else "rust_c_api_bridge_diagnostic",
            "process_per_operation_enabled": False,
            "single_shot_mode": "debug_only",
            "direct_sdk_bridge": False,
            "native_c_api_bridge_diagnostic": bool(self._rust_direct_cdylib_enabled),
            "pure_embedded_direct_sdk": False,
            "capabilities": {
                "health_endpoint": True,
                "readiness_endpoint": True,
                "metrics_endpoint": True,
                "batch_append": True,
                "matrixark_batch_append_records": True,
                "matrixark_retrieve_context_pack": True,
                "compact_secondary_index_lookup": True,
                "placement_key_candidate_fetch": True,
                "context_pack_telemetry": True,
                "prefix_scan": True,
                "connection_pooling": True,
                "client_pooling": True,
                "dedicated_retrieve_lane": True,
                "backpressure": True,
                "graceful_shutdown": True,
                "timeout_handling": True,
                "structured_errors_compatible": True,
            },
            "health": health,
            "readiness": readiness,
            "prometheus": prometheus,
            "metrics": {
                "metaserver": self._metaserver,
                "namespace": self._namespace,
                "table": self._table,
                "storage_prefix": self._storage_prefix,
                "audit_mode": self._audit_mode,
                "audit_buffered_records": len(self._audit_buffer),
                "audit_flush_failures": self._audit_flush_failures,
                "rust_client": rust_client_metrics,
                "rust_write_client": rust_client_metrics,
                "rust_retrieve_client": rust_retrieve_metrics,
                "rust_summary_client": rust_summary_metrics,
                "recovery_status": self._recovery_status_snapshot(native_metrics=rust_client_metrics),
                "cache_state": self._cache_state_snapshot(),
                "rust_proxy_lanes": {
                    "write": not self._rust_direct_cdylib_enabled,
                    "retrieve": self._retrieve_client is not None,
                    "summary_audit": self._summary_client is not None,
                    "summary_audit_share_write_lane": False,
                    "retrieve_isolated_from_summary_audit": True,
                    "ingest_isolated_from_summary_audit": True,
                    "direct_cdylib_enabled": self._rust_direct_cdylib_enabled,
                    "transport": rust_client_metrics.get("transport", "rust_proxy_stdio"),
                },
            },
        }


class MatrixArkTemporalStoreRustDirectAdapter(MatrixArkTemporalStoreRustAdapter):
    """MatrixArk adapter backed by a long-lived Rust process using the Rust SDK directly.

    This is the Rust parity counterpart to the direct SDK adapter. The Python
    MCP process still owns protocol/model glue, while the Rust bridge owns the
    TemporalStore SDK client and native storage calls. It is intentionally
    explicit so benchmark reports can distinguish it from the production Rust
    proxy path.
    """

    def __init__(self, **kwargs: Any) -> None:
        kwargs["sdk_mode"] = "direct_sdk"
        super().__init__(**kwargs)

    def _backend_label(self) -> str:
        return "temporalstore-rust-direct"



