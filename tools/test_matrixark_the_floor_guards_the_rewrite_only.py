# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`MATRIXARK_LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS` is meant to bound how often the snapshot is
rewritten. It was checked at the top of the writer, so it gated two very different writes with one
test: the O(corpus) base rewrite, and the cheap tail append that keeps the head's signature current
after a write.

Gating the second is what made the knob unusable. A snapshot whose head no longer matches the log is
ignored, so a restart re-derives -- and four tests assert exactly that it should not have to. Setting
the floor at all therefore broke them, which is why the default is 0 and why the comment above the
constant says a floor "cost correctness".

These tests pin the split: with a floor set, a write still refreshes the head promptly, and only the
full rewrite is skipped.
"""
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


class FloorGuardsTheRewriteOnlyTest(unittest.TestCase):

    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix="write_floor_"))
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.log = self.root / "events.jsonl"
        # The floor is a PROCESS-GLOBAL read at call time. Restore it, or every later test in the
        # suite runs under whatever this one left behind.
        self.original_floor = adapter_module.LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS
        self.addCleanup(setattr, adapter_module,
                        "LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS", self.original_floor)

    def base_mtime(self) -> int:
        try:
            return (self.root / "events.jsonl.read-cache.json").stat().st_mtime_ns
        except FileNotFoundError:
            return 0

    def seeded(self) -> MatrixArkLocalAdapter:
        """An adapter with a base snapshot already on disk."""
        writer = MatrixArkLocalAdapter(self.log)
        for index in range(6):
            writer.append({"record_type": "context_event", "event_id": "seed-%d" % index,
                           "content": "seed %d" % index})
        writer.read_all()
        self.assertGreater(self.base_mtime(), 0, "no base snapshot was written to throttle")
        return writer

    def test_a_write_still_refreshes_the_head_under_a_floor(self):
        adapter = self.seeded()
        adapter_module.LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS = 600000.0
        adapter.append({"record_type": "context_event", "event_id": "after-floor",
                        "content": "written while the floor is set"})
        # The promptness guarantee: a restart must still be served from the snapshot rather than
        # re-deriving from the log, which is only true if the write refreshed the head.
        adapter_module._LOCAL_READ_CACHE.clear()
        restarted = MatrixArkLocalAdapter(self.log)
        records = restarted.read_all()
        self.assertEqual(restarted._read_cache_source, "durable",
                         "a floor blocked the tail write, so the snapshot went stale")
        self.assertIn("after-floor", {r.get("event_id") for r in records},
                      "the record written under the floor is missing from the served view")

    def test_the_full_rewrite_is_the_thing_the_floor_skips(self):
        adapter = self.seeded()
        adapter_module.LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS = 600000.0
        before = self.base_mtime()
        # Force the path that rewrites the base: a caller that cannot vouch for the list passes
        # epoch=None, which is what makes the tail path unavailable.
        adapter._write_durable_read_cache(
            list(adapter.read_all()), adapter._jsonl_cache_signature_detail(), epoch=None)
        self.assertEqual(self.base_mtime(), before,
                         "the base was rewritten despite the floor")

    def test_without_a_floor_the_same_call_does_rewrite(self):
        # Positive control. Without this, the test above passes for any reason at all -- including
        # the rewrite never being reachable in this fixture.
        adapter = self.seeded()
        adapter_module.LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS = 0.0
        before = self.base_mtime()
        adapter.append({"record_type": "context_event", "event_id": "extra",
                        "content": "one more"})
        adapter._write_durable_read_cache(
            list(adapter.read_all()), adapter._jsonl_cache_signature_detail(), epoch=None)
        self.assertNotEqual(self.base_mtime(), before,
                            "the base was not rewritten even with no floor, so the test above "
                            "proves nothing")

    def delta_mtime(self) -> int:
        try:
            return (self.root / ".events.jsonl.read-cache-delta.jsonl").stat().st_mtime_ns
        except FileNotFoundError:
            return 0

    def test_a_forced_write_ignores_the_floor(self):
        # The rebuild-from-log path forces, so a store always ends up with a snapshot even under an
        # aggressive floor. Assert only that SOMETHING was persisted -- which of the two files the
        # writer picks is its own decision, and pinning that here would make this test fail for
        # reasons that have nothing to do with the floor.
        adapter = self.seeded()
        adapter_module.LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS = 600000.0
        adapter.append({"record_type": "context_event", "event_id": "forced",
                        "content": "forced"})
        before = (self.base_mtime(), self.delta_mtime())
        adapter._write_durable_read_cache(
            list(adapter.read_all()), adapter._jsonl_cache_signature_detail(),
            force=True, epoch=None)
        self.assertNotEqual((self.base_mtime(), self.delta_mtime()), before,
                            "force=True wrote nothing at all, so the floor blocked it")


if __name__ == "__main__":
    unittest.main()
