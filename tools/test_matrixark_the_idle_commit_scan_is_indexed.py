# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`pre_retrieval_idle_commit_flush` runs before every retrieve to find scheduled idle-commit tasks.

It prefers `idle_commit_task_records` on the target and falls back to `read_all()`. The native
reader has offered that method since it was written; the local adapter did not, so the fallback ran
and scanned the entire live view TWICE per query to find rows that number in the handful. Profiled
on a 1 MB corpus at 8,548 records, that flush was 68% of a settled retrieve.

These tests pin the index against the scan it replaces, in the three states where an index can go
wrong: built cold, folded forward after an append, and rebuilt after the cache it describes is
dropped. Task rows are APPENDED rather than hoped for -- the corpora these tests can build carry
none of their own, and an index compared against an empty scan proves nothing. That is not
hypothetical: it hid a real defect in the first version of this change, where the fold-forward sat
inside another index's guard and never ran.
"""
import json
import shutil
import tempfile
import unittest
from pathlib import Path

try:  # package path
    from tools import matrixark_mcp_local_adapter as adapter_module
    from tools.matrixark_mcp_local_adapter import MatrixArkLocalAdapter
except ImportError:
    import matrixark_mcp_local_adapter as adapter_module
    from matrixark_mcp_local_adapter import MatrixArkLocalAdapter

SCOPE = {"tenant_id": "acme", "user_id": "dana", "session_id": "skills"}
TASK_TYPE = "matrixark_async_pipeline_task"


def task_row(index: int) -> dict:
    return {
        "record_type": TASK_TYPE,
        "task_hash": 900000 + index,
        "scheduled_task_hash": 900000 + index,
        "status": "idle_commit_scheduled",
        "scope": dict(SCOPE),
        "access_scope": dict(SCOPE),
    }


def canonical(rows) -> str:
    return json.dumps(rows, sort_keys=True, default=str)


class IdleCommitScanIsIndexedTest(unittest.TestCase):

    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix="idle_index_"))
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.log = self.root / "events.jsonl"
        writer = MatrixArkLocalAdapter(self.log)
        for index in range(2):
            writer.ingest({
                "kind": "skill", "scope": SCOPE,
                "text": "# Runbook %d\n\nDrain the queue for case %d.\n" % (index, index),
                "metadata": {"raw_uri": "file:///s/r-%d.md" % index, "title": "r-%d" % index},
            })
        for index in range(4):
            writer.append(task_row(index))
        writer.close(timeout_s=600)

    def scan(self, adapter) -> list:
        """What the flush used to do: walk everything, keep one type."""
        return [record for record in adapter.read_all()
                if isinstance(record, dict)
                and str(record.get("record_type") or "") == TASK_TYPE]

    def test_the_hook_exists_at_all(self):
        adapter = MatrixArkLocalAdapter(self.log)
        self.assertTrue(callable(getattr(adapter, "idle_commit_task_records", None)),
                        "without this method the flush falls back to scanning the whole store")

    def test_it_returns_what_the_scan_returned(self):
        adapter = MatrixArkLocalAdapter(self.log)
        scanned = self.scan(adapter)
        # The control. An index compared against an empty scan is equal for the wrong reason.
        self.assertGreater(len(scanned), 0,
                           "no task rows in the fixture, so the comparison below is vacuous")
        self.assertEqual(canonical(adapter.idle_commit_task_records(SCOPE)), canonical(scanned))

    def test_it_is_folded_forward_on_append(self):
        adapter = MatrixArkLocalAdapter(self.log)
        before = adapter.idle_commit_task_records(SCOPE)   # build it, so the append must maintain it
        adapter.append(task_row(99))
        scanned = self.scan(adapter)
        self.assertGreater(len(scanned), len(before),
                           "the append added no row, so the fold-forward is untested")
        self.assertEqual(canonical(adapter.idle_commit_task_records(SCOPE)), canonical(scanned),
                         "the index went stale against the cache after an append")

    def test_a_fresh_reader_rebuilds_it(self):
        adapter_module._LOCAL_READ_CACHE.clear()
        fresh = MatrixArkLocalAdapter(self.log)
        scanned = self.scan(fresh)
        self.assertGreater(len(scanned), 0)
        self.assertEqual(canonical(fresh.idle_commit_task_records(SCOPE)), canonical(scanned))

    def test_the_pack_is_unchanged(self):
        """The answer must not move. A faster retrieve that returns something else is not a saving."""
        adapter = MatrixArkLocalAdapter(self.log)
        query = {"scope": SCOPE, "query": "drain the queue", "limit": 8}
        for _ in range(2):
            adapter.retrieve(dict(query))
        with_hook = adapter.retrieve(dict(query))

        # Take the hook away so the flush uses the old fallback, on the SAME adapter and store.
        hidden = type(adapter).idle_commit_task_records
        try:
            delattr(type(adapter), "idle_commit_task_records")
            self.assertFalse(callable(getattr(adapter, "idle_commit_task_records", None)),
                             "the hook is still reachable, so both sides are the same path")
            for _ in range(2):
                adapter.retrieve(dict(query))
            without_hook = adapter.retrieve(dict(query))
        finally:
            type(adapter).idle_commit_task_records = hidden

        self.assertEqual(canonical((with_hook or {}).get("selected_refs")),
                         canonical((without_hook or {}).get("selected_refs")))


if __name__ == "__main__":
    unittest.main()
