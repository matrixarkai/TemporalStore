#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Every memory API must reach TemporalStore on a native backend, not the local JSONL log.

`MatrixArkLocalAdapter` precedes the direct mixins in every native adapter's MRO and defines
JSONL-only readers and writers. Each of those returns `[]`, or writes nothing, the moment
`_local_jsonl_enabled` is False -- which is exactly what a native backend sets. So a method can
look implemented, resolve happily, and silently do nothing on the backends that actually ship.
That is not hypothetical: it is how `read_all`, `_read_all_compacted`, `_read_raw_records` and
`append_many` all came to be broken at once, which made forget serve deleted memories back,
history report an empty log, and keyed upsert never supersede.

Resolving to a `matrixark_local_adapter_*` module is NOT the defect -- most of those are
backend-agnostic orchestration that reads through `read_all()` and writes through `append_many()`,
both overridden natively. The defect is a method whose own code reaches for the local log.

This checks every native adapter a deployment can select, because auditing one of them checks a
backend nobody may be running: `temporalstore-direct` builds MatrixArkTemporalStoreDirectAdapter
and `temporalstore-rust` builds MatrixArkTemporalStoreRustAdapter (see matrixark_mcp_backends).
"""
from __future__ import annotations

import ast
import inspect
import textwrap
import unittest

try:
    from tools import matrixark_mcp_temporal_adapters as adapters
except ImportError:  # run from tools/ dir
    import matrixark_mcp_temporal_adapters as adapters


NATIVE_ADAPTERS = [
    ("temporalstore-direct", adapters.MatrixArkTemporalStoreDirectAdapter),
    ("temporalstore-rust", adapters.MatrixArkTemporalStoreRustAdapter),
    ("rust-direct", adapters.MatrixArkTemporalStoreRustDirectAdapter),
]

# Every adapter method the memory/mem0 surface dispatches to, plus the seams beneath them.
MEMORY_PATH_METHODS = [
    "ingest", "session_commit", "retrieve", "batch_extract",
    "get_memory", "get_all", "history", "update_memory", "delete_memory",
    "forget", "reset", "get_memory_by_identity_key", "feedback",
    "refresh_summaries", "replay", "backend_metrics",
    "get_tenant_policy", "set_tenant_policy",
    "read_all", "_read_all_compacted", "_read_raw_records",
    "read_all_without_disk_fallback_recovery", "_with_latest_context_state_records",
    "recent_records", "append", "append_many", "_append_many_materialized",
    "_apply_identity_upsert", "_write_retention_cutoff", "sweep_expired_memories",
    "find_idempotency_record", "append_idempotency_record",
    "ensure_context_node_path", "_existing_node_embedding_refs",
    "refresh_dirty_node_summaries", "drain_due_idle_session_commits",
]

# Source markers that mean "this reaches the local JSONL log itself".
LOCAL_LOG_MARKERS = ("_local_jsonl_enabled", "_shard_path", "_read_shard", "_write_shard",
                     ".jsonl", "_jsonl_", "_log_path")

# `append_many` names `_local_jsonl_enabled`, but as the GUARD that routes the write to the native
# backend: `route_to_backend = callable(append_backend) and not _local_jsonl_enabled and ...`.
# Reading the flag to choose the native path is the opposite of falling back to the local log.
ALLOWED = {"append_many"}


_CODE_CACHE: dict[int, str] = {}


def _code_only(fn) -> str:
    """The method's source with docstrings stripped.

    The native overrides EXPLAIN this trap in their docstrings, so scanning raw source reports
    them as reaching for the local log -- false positives that would bury a real one.

    Memoized on the function object: the three adapters inherit most of these methods, so the
    same source would otherwise be parsed and unparsed three times, and some of these functions
    are very large.
    """
    cached = _CODE_CACHE.get(id(fn))
    if cached is not None:
        return cached
    _CODE_CACHE[id(fn)] = result = _parse_code_only(fn)
    return result


def _parse_code_only(fn) -> str:
    try:
        src = textwrap.dedent(inspect.getsource(fn))
    except (OSError, TypeError):
        return ""
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return src
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if not isinstance(body, list) or not body:
            continue
        first = body[0]
        if (isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)):
            body.pop(0)
            if not body:
                body.append(ast.Pass())
    try:
        return ast.unparse(tree)
    except Exception:  # noqa: BLE001
        return src


class NativeBackendsReachTemporalStoreTest(unittest.TestCase):
    def test_no_memory_path_method_reaches_the_local_jsonl_log(self) -> None:
        offenders: list[str] = []
        for label, cls in NATIVE_ADAPTERS:
            for name in MEMORY_PATH_METHODS:
                if name in ALLOWED:
                    continue
                fn = getattr(cls, name, None)
                if fn is None:
                    continue
                hits = sorted({m for m in LOCAL_LOG_MARKERS if m in _code_only(fn)})
                if hits:
                    offenders.append(
                        f"{label}.{name} (from {getattr(fn, '__module__', '?')}) touches {hits}"
                    )
        self.assertEqual([], offenders, "\n".join(["memory-path methods on a local log:"] + offenders))

    def test_backend_metrics_does_not_claim_the_jsonl_backend(self) -> None:
        """A native deployment asking for backend metrics must not be told it is running JSONL.

        The direct adapter used to inherit the JSONL implementation, which hardcodes
        `mode: "local-jsonl"` and reports an `event_log` path that on a native backend is the
        `-unused-` sentinel the adapter never writes to.
        """
        for label, cls in NATIVE_ADAPTERS:
            src = _code_only(cls.backend_metrics)
            self.assertNotIn("local-jsonl", src, f"{label}.backend_metrics claims the JSONL backend")

    def test_every_dispatched_memory_method_exists(self) -> None:
        """A missing method would fall through to whatever the MRO happens to offer."""
        for label, cls in NATIVE_ADAPTERS:
            for name in ("ingest", "session_commit", "retrieve", "get_memory", "get_all",
                         "history", "update_memory", "delete_memory", "forget", "reset",
                         "get_memory_by_identity_key", "refresh_summaries", "backend_metrics"):
                self.assertTrue(callable(getattr(cls, name, None)), f"{label} is missing {name}")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
