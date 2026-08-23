#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The idle-commit drain reads only pipeline tasks, not the whole record log.

`drain_due_idle_session_commits` runs once per ingest and opened with `read_all()` -- on a native
backend, the entire record log shipped over the proxy -- while using the result for nothing but
`matrixark_async_pipeline_task` records.

Two things make the narrower read equivalent, and both are pinned here.

ORDER. The drain decides last-write-wins from list position. The native scan ends in
`compact_latest_context_state_records`, which keys only `context_summary`,
`context_model_registry` and some `context_embedding` rows -- a pipeline task gets no key, so it
passes through untouched -- and that function re-sorts by the original index, so append order
survives regardless.

SCOPE. `idle_commit_task_records({})` degenerates to a cross-scope full-store scan, which is the
very cost this avoids, so an empty scope must fall back rather than "optimise" into the worst case.
"""
from __future__ import annotations

import unittest

try:
    from tools import matrixark_mcp_temporal_adapters as adapters
    from tools.matrixark_mcp_core_compact import (
        compact_latest_context_state_records,
        latest_context_state_key,
    )
except ImportError:  # run from tools/ dir
    import matrixark_mcp_temporal_adapters as adapters
    from matrixark_mcp_core_compact import (
        compact_latest_context_state_records,
        latest_context_state_key,
    )

TASK = "matrixark_async_pipeline_task"


def _task(task_hash, status, order):
    return {"record_type": TASK, "task_hash": task_hash, "status": status, "n": order}


class OrderSurvivesCompactionTest(unittest.TestCase):
    """The equivalence argument, checked against the real compaction rather than assumed."""

    def test_a_pipeline_task_gets_no_latest_state_key(self) -> None:
        self.assertIsNone(latest_context_state_key(_task(1, "scheduled", 0)))

    def test_compaction_keeps_every_task_in_append_order(self) -> None:
        records = [_task(1, "scheduled", 0), _task(2, "scheduled", 1),
                   _task(1, "idle_commit_scheduled", 2), _task(1, "completed", 3)]
        out = compact_latest_context_state_records(list(records))
        self.assertEqual([0, 1, 2, 3], [r["n"] for r in out],
                         "the drain reads last-write-wins from position; order must survive")
        self.assertEqual(4, len([r for r in out if r.get("record_type") == TASK]))


class _Stub(adapters.MatrixArkTemporalStoreDirectAdapter):
    """A real subclass, so the override's `super()` fallback resolves through the actual MRO.

    Built with `object.__new__` -- the real __init__ would dial a metaserver and spawn a CLI.
    """

    def read_all(self):
        self.read_all_calls += 1
        return [{"record_type": "context_event"}, _task(9, "scheduled", 0)]

    def idle_commit_task_records(self, scope):
        self.scan_calls.append(scope)
        if self.raises:
            raise RuntimeError("scan unavailable")
        return self.tasks


def _Adapter(tasks=None, raises=False):
    a = object.__new__(_Stub)
    a.tasks = tasks or []
    a.raises = raises
    a.read_all_calls = 0
    a.scan_calls = []
    return a


def _bind(adapter):
    """Call the native override, bound so `super()` works."""
    return adapters.MatrixArkTemporalStoreDirectAdapter._idle_commit_candidate_records


class IdleCommitCandidateRecordsTest(unittest.TestCase):
    def test_a_real_scope_uses_the_typed_scan(self) -> None:
        a = _Adapter(tasks=[_task(1, "scheduled", 0)])
        out = _bind(a)(a, {"user_id": "alice", "tenant_hash": 7})
        self.assertEqual(1, len(out))
        self.assertEqual(0, a.read_all_calls, "the whole log must not be read")
        self.assertEqual([{"user_id": "alice", "tenant_hash": 7}], a.scan_calls)

    def test_an_empty_scope_falls_back_instead_of_scanning_everything(self) -> None:
        """A cross-scope task scan IS a full-store scan -- the exact cost this avoids."""
        a = _Adapter()
        out = _bind(a)(a, {})
        self.assertEqual([], a.scan_calls, "must not issue a cross-scope scan")
        self.assertEqual(1, a.read_all_calls)
        self.assertEqual(2, len(out))

    def test_a_failing_scan_falls_back_rather_than_stopping_the_drain(self) -> None:
        a = _Adapter(raises=True)
        out = _bind(a)(a, {"user_id": "alice"})
        self.assertEqual(1, a.read_all_calls)
        self.assertEqual(2, len(out))

    def test_the_default_reads_the_log(self) -> None:
        """The JSONL backend keeps the old behaviour; read_all there is an in-memory walk."""
        try:
            from tools.matrixark_local_adapter_retrieval import _LocalAdapterRetrievalMixin as M
        except ImportError:
            from matrixark_local_adapter_retrieval import _LocalAdapterRetrievalMixin as M
        a = _Adapter()
        out = M._idle_commit_candidate_records(a, {"user_id": "alice"})
        self.assertEqual(1, a.read_all_calls)
        self.assertEqual([], a.scan_calls)
        self.assertEqual(2, len(out))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
