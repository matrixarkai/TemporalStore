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

    def base_state(self) -> tuple:
        """What the base snapshot HOLDS, not when it was touched.

        An mtime comparison looked obvious and was not deterministic: two writes can land inside
        one filesystem timestamp tick, so "the base was rewritten" and "the base was not rewritten"
        are indistinguishable whenever the test runs fast enough. That made this file fail about
        one run in six, and worse once the fixture got quicker.

        Content settles it. A rewrite from a shorter list changes the record count, and a rewrite
        from the same list still changes nothing -- which is why the tests below write a list with
        a row REMOVED, the case compaction produces and the floor exists to bound.
        """
        path = self.root / "events.jsonl.read-cache.json"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, ValueError):
            return (0, 0)
        records = payload.get("records")
        return (path.stat().st_size, len(records) if isinstance(records, list) else -1)

    def seeded(self) -> MatrixArkLocalAdapter:
        """An adapter with a base snapshot already on disk."""
        writer = MatrixArkLocalAdapter(self.log)
        for index in range(6):
            writer.append({"record_type": "context_event", "event_id": "seed-%d" % index,
                           "content": "seed %d" % index})
        writer.read_all()
        self.assertGreater(self.base_state()[0], 0, "no base snapshot was written to throttle")
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

    def shortened(self, adapter) -> list:
        """The record list with one row dropped from the middle.

        Passing `epoch=None` alone does NOT force the base rewrite: the writer still has a
        disk-based contiguity fallback, so it can decide the persisted file is a prefix of this
        list and append the tail instead. Which path it picked then depended on bookkeeping this
        fixture does not control, and the control below failed about one run in six.

        A SHORTER list settles it. Both contiguity routes require `appended > 0`, and a removal
        makes it negative -- which is exactly what compaction does in production, and exactly the
        case the floor exists to bound.
        """
        records = list(adapter.read_all())
        self.assertGreater(len(records), 2, "too few records to drop one from the middle")
        return records[:1] + records[2:]

    def test_the_full_rewrite_is_the_thing_the_floor_skips(self):
        adapter = self.seeded()
        adapter_module.LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS = 600000.0
        shorter = self.shortened(adapter)
        before = self.base_state()
        adapter._write_durable_read_cache(
            shorter, adapter._jsonl_cache_signature_detail(), epoch=None)
        self.assertEqual(self.base_state(), before,
                         "the base was rewritten despite the floor")

    def test_without_a_floor_the_same_call_does_rewrite(self):
        # Positive control for the test above, which would otherwise pass for any reason at all --
        # including the rewrite never being reachable in this fixture. Same call, same shortened
        # list, floor removed.
        adapter = self.seeded()
        shorter = self.shortened(adapter)
        adapter_module.LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS = 0.0
        before = self.base_state()
        adapter._write_durable_read_cache(
            shorter, adapter._jsonl_cache_signature_detail(), epoch=None)
        self.assertNotEqual(self.base_state(), before,
                            "the base was not rewritten even with no floor, so the test above "
                            "proves nothing")

    def delta_size(self) -> int:
        # Size, not mtime, for the same reason as base_state: two writes can share one timestamp
        # tick and then "written" and "not written" look identical.
        try:
            return (self.root / ".events.jsonl.read-cache-delta.jsonl").stat().st_size
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
        before = (self.base_state(), self.delta_size())
        adapter._write_durable_read_cache(
            list(adapter.read_all()), adapter._jsonl_cache_signature_detail(),
            force=True, epoch=None)
        self.assertNotEqual((self.base_state(), self.delta_size()), before,
                            "force=True wrote nothing at all, so the floor blocked it")


if __name__ == "__main__":
    unittest.main()
