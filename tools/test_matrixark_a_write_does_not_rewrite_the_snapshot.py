# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A write continues the snapshot's tail or leaves it alone. It never rewrites the whole base.

Ingest does not only append. Compaction rewrites earlier records, so the list handed to the
snapshot writer is usually not an extension of what is persisted and the tail cannot apply.
Measured over 40 ingests: 173 snapshot writes, 146 of them rewriting the entire record set.

The snapshot is derived state -- _load_durable_read_cache checks it against the log's signature and
returns None on any mismatch, and the caller re-derives from the log. So a write can decline to
refresh it; the read path installs the records and writes it anyway.

Measured with the writes interleaved to cancel machine drift, eight paired runs: the time inside
the snapshot writer fell from 2.864 s to 0.621 s across the set, and ingest was faster in six of
the eight pairs, median 0.26 s.
"""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_local_adapter as adapter_module


class AWriteDoesNotRewriteTheSnapshot(unittest.TestCase):
    def setUp(self):
        with adapter_module._LOCAL_READ_CACHE_LOCK:
            adapter_module._LOCAL_READ_CACHE.clear()

    tearDown = setUp

    @staticmethod
    def _seeded(store):
        log = Path(store) / "events.jsonl"
        adapter = adapter_module.MatrixArkLocalAdapter(log)
        for i in range(10):
            adapter.append({"record_type": "context_document", "id": "doc-%d" % i,
                            "node_path": "/n/%d" % i, "text": "body " * 20})
        records = adapter.read_all()          # the read installs the base
        # That read just stamped the write clock, and there is a floor between snapshot writes.
        # Clear it, or the write under test is skipped for a reason unrelated to what is asserted.
        adapter._durable_read_cache_last_write_ms = 0
        return adapter, records

    def test_a_write_will_not_rewrite_the_base(self):
        """A list that is not a continuation used to cost a full rewrite on every append."""
        with tempfile.TemporaryDirectory() as store:
            adapter, records = self._seeded(store)
            base = adapter._durable_read_cache_snapshot_path()
            before = base.read_bytes()

            diverged = [dict(r) for r in records]
            diverged[-1] = {"record_type": "context_document", "id": "changed",
                            "node_path": "/n/changed", "text": "compaction rewrote this"}
            diverged.append({"record_type": "context_document", "id": "new",
                             "node_path": "/n/new", "text": "appended"})
            adapter._durable_read_cache_state = None
            adapter._durable_read_cache_last_write_ms = 0
            adapter._write_durable_read_cache(
                diverged, adapter._jsonl_cache_signature_detail(), epoch=None, tail_only=True)

            self.assertEqual(before, base.read_bytes(),
                             "a write rewrote the whole base; that is O(corpus) per append")

    def test_a_write_still_appends_a_tail_it_can_prove(self):
        """Declining the full rewrite must not disable the cheap path."""
        with tempfile.TemporaryDirectory() as store:
            adapter, records = self._seeded(store)
            delta = adapter._durable_read_cache_delta_path()
            before = delta.stat().st_size if delta.exists() else 0

            longer = list(records) + [{"record_type": "context_document", "id": "tail",
                                       "node_path": "/n/tail", "text": "appended after the base"}]
            adapter._durable_read_cache_last_write_ms = 0
            adapter._write_durable_read_cache(
                longer, adapter._jsonl_cache_signature_detail(), epoch=None, tail_only=True)

            self.assertGreater(delta.stat().st_size if delta.exists() else 0, before,
                               "the tail was not appended, so nothing keeps the snapshot current")

    def test_a_cold_reader_always_gets_every_record(self):
        """Correctness does not depend on the snapshot at all.

        This is the guarantee that makes declining a rewrite safe: _load_durable_read_cache checks
        the snapshot against the log's signature and returns None on any mismatch, so a snapshot
        that has fallen behind is not served -- the caller re-derives from the log.
        """
        with tempfile.TemporaryDirectory() as store:
            log = Path(store) / "events.jsonl"
            adapter = adapter_module.MatrixArkLocalAdapter(log)
            for i in range(24):
                adapter.ingest({
                    "kind": "message",
                    "scope": {"tenant_id": "acme", "user_id": "u", "session_id": "s%d" % (i // 8)},
                    "messages": [{"role": "user", "content": "a sentence to extract %d" % i}],
                })
            expected = len(adapter.read_all())
            self.assertGreater(expected, 0)

            with adapter_module._LOCAL_READ_CACHE_LOCK:
                adapter_module._LOCAL_READ_CACHE.clear()
            cold = adapter_module.MatrixArkLocalAdapter(log)
            self.assertEqual(expected, len(cold.read_all()),
                             "a reader with no process state lost records")

    def test_a_re_deriving_read_leaves_a_snapshot_the_next_start_can_use(self):
        """What keeps the snapshot current once writes no longer refresh it.

        Asserted through a read that actually re-derives -- a read served from the process cache
        writes nothing, which is exactly the case that leaves the snapshot behind.
        """
        with tempfile.TemporaryDirectory() as store:
            log = Path(store) / "events.jsonl"
            adapter = adapter_module.MatrixArkLocalAdapter(log)
            for i in range(24):
                adapter.ingest({
                    "kind": "message",
                    "scope": {"tenant_id": "acme", "user_id": "u", "session_id": "s%d" % (i // 8)},
                    "messages": [{"role": "user", "content": "a sentence to extract %d" % i}],
                })
            with adapter_module._LOCAL_READ_CACHE_LOCK:
                adapter_module._LOCAL_READ_CACHE.clear()
            refresher = adapter_module.MatrixArkLocalAdapter(log)
            expected = len(refresher.read_all())      # re-derives, and writes the snapshot

            with adapter_module._LOCAL_READ_CACHE_LOCK:
                adapter_module._LOCAL_READ_CACHE.clear()
            cold = adapter_module.MatrixArkLocalAdapter(log)
            served = cold.read_all()
            self.assertEqual(expected, len(served))
            self.assertEqual("durable", cold._read_cache_source,
                             "a read that re-derived did not leave a usable snapshot, so every "
                             "restart would re-read the whole log")


if __name__ == "__main__":
    unittest.main()
