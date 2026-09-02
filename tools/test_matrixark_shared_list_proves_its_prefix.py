# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The read-cache snapshot can continue its tail for a list it did not build.

Several adapters serve one event log, and the list handed to the snapshot writer is usually the
process-wide one they share. No instance can vouch for that list from its own bookkeeping, so the
writer received `epoch=None` and rewrote the entire base -- measured at 290 of 291 writes with two
adapters on one log, re-encoding the whole corpus on every read.

The head now names the last record it persisted, which lets any holder of a longer list prove the
file is a prefix of it and append only the tail.

Two things are asserted here, because the second is what makes the first safe: the rewrites stop,
AND a snapshot whose recorded record does not match is refused rather than served.
"""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_local_adapter as adapter_module


def _ingest(store, n_adapters, batches=12, per_batch=10):
    """Append through several adapters over one log, reading between batches."""
    log = Path(store) / "events.jsonl"
    adapters = [adapter_module.MatrixArkLocalAdapter(log) for _ in range(n_adapters)]
    full_rewrites = {"n": 0}
    real_dump = json.dump

    def counting_dump(obj, handle, **kwargs):
        # only the base carries "records"; the head does not
        if isinstance(obj, dict) and "records" in obj:
            full_rewrites["n"] += 1
        return real_dump(obj, handle, **kwargs)

    adapter_module.json.dump = counting_dump
    try:
        for batch in range(batches):
            current = adapters[batch % len(adapters)]
            for i in range(per_batch):
                index = batch * per_batch + i
                current.append({
                    "record_type": "context_document",
                    "id": "doc-%d" % index,
                    "node_path": "/n/%d" % index,
                    "text": "some body text " * 20,
                })
            current.read_all()
    finally:
        adapter_module.json.dump = real_dump
    return log, full_rewrites["n"], batches * per_batch


def _cold_read(log):
    """A reader with no process-local state, as a restart would have."""
    with adapter_module._LOCAL_READ_CACHE_LOCK:
        adapter_module._LOCAL_READ_CACHE.clear()
    fresh = adapter_module.MatrixArkLocalAdapter(log)
    return fresh, fresh.read_all()


class ASharedListCanProveItsPrefix(unittest.TestCase):
    def test_one_adapter_still_appends_its_tail(self):
        """The path that already worked must keep working."""
        with tempfile.TemporaryDirectory() as store:
            _log, rewrites, total = _ingest(store, 1)
            self.assertLessEqual(rewrites, 2,
                                 "a single adapter should write the base once, then tails")
            self.assertGreater(total, 0)

    def test_several_adapters_stop_rewriting_the_whole_base(self):
        with tempfile.TemporaryDirectory() as store:
            _log, rewrites, _total = _ingest(store, 2)
            self.assertLessEqual(rewrites, 2,
                                 "the base was rewritten %d times; a shared list must be able to "
                                 "prove its prefix from the head instead" % rewrites)

    def test_a_cold_reader_is_served_from_the_snapshot_and_gets_everything(self):
        """The point of the snapshot. It was being discarded: its signature described a shorter
        log than the one on disk, so every restart re-derived from the log instead."""
        with tempfile.TemporaryDirectory() as store:
            log, _rewrites, total = _ingest(store, 2)
            fresh, records = _cold_read(log)
            self.assertEqual("durable", fresh._read_cache_source,
                             "the snapshot was not used, so it is not keeping up with the log")
            ids = sorted(int(r["id"].split("-")[1]) for r in records
                         if str(r.get("id", "")).startswith("doc-"))
            self.assertEqual(list(range(total)), ids, "the snapshot lost or reordered records")

    def test_a_snapshot_that_does_not_match_its_recorded_record_is_refused(self):
        """The safety net. A tail appended onto the wrong base would still satisfy the counts, so
        a wrong snapshot must be refused and re-derived rather than served."""
        with tempfile.TemporaryDirectory() as store:
            log, _rewrites, total = _ingest(store, 2)
            fresh = adapter_module.MatrixArkLocalAdapter(log)
            head_path = fresh._durable_read_cache_head_path()
            head = json.loads(head_path.read_text(encoding="utf-8"))
            self.assertTrue(head.get("tail_fingerprint"),
                            "the head must name the last record it persisted")
            head["tail_fingerprint"] = "0" * 32
            head_path.write_text(json.dumps(head, separators=(",", ":")) + "\n", encoding="utf-8")

            reader, records = _cold_read(log)
            self.assertNotEqual("durable", reader._read_cache_source,
                                "a snapshot naming a record it does not hold was served anyway")
            ids = sorted(int(r["id"].split("-")[1]) for r in records
                         if str(r.get("id", "")).startswith("doc-"))
            self.assertEqual(list(range(total)), ids,
                             "falling back to the log must still serve every record")


if __name__ == "__main__":
    unittest.main()
