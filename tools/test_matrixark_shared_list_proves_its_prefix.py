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
    # These switches are read into module constants at import, so setting the environment here
    # would not reach them. The whole suite shares one process and other files set these, which is
    # why this passed alone and failed in CI -- pin them for the test and restore afterwards.
    _PINNED = {
        "LOCAL_DURABLE_READ_CACHE_ENABLED": True,
        "LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS": 0.0,
        "LOCAL_JSONL_ENABLED": True,
    }

    def setUp(self):
        self._saved = {}
        for name, value in self._PINNED.items():
            self._saved[name] = getattr(adapter_module, name)
            setattr(adapter_module, name, value)
        # a delta cap below the corpus would force a base rewrite for reasons unrelated to this
        self._saved["LOCAL_DURABLE_READ_CACHE_MAX_DELTA"] =             adapter_module.LOCAL_DURABLE_READ_CACHE_MAX_DELTA
        adapter_module.LOCAL_DURABLE_READ_CACHE_MAX_DELTA = max(
            2000, adapter_module.LOCAL_DURABLE_READ_CACHE_MAX_DELTA)
        with adapter_module._LOCAL_READ_CACHE_LOCK:
            adapter_module._LOCAL_READ_CACHE.clear()

    def tearDown(self):
        for name, value in self._saved.items():
            setattr(adapter_module, name, value)
        with adapter_module._LOCAL_READ_CACHE_LOCK:
            adapter_module._LOCAL_READ_CACHE.clear()

    def _seeded(self, store):
        """An adapter whose snapshot base is already on disk, with the list that produced it."""
        log = Path(store) / "events.jsonl"
        adapter = adapter_module.MatrixArkLocalAdapter(log)
        for i in range(10):
            adapter.append({"record_type": "context_document", "id": "doc-%d" % i,
                            "node_path": "/n/%d" % i, "text": "some body text " * 20})
        records = adapter.read_all()          # installs the base
        return adapter, records

    def test_a_list_it_did_not_build_can_still_append_its_tail(self):
        """The fix, asserted on one write rather than on a count over many.

        epoch=None is how the process-wide list arrives -- shared between every adapter over this
        log, so no instance can vouch for it from its own bookkeeping. Before the head recorded a
        fingerprint, that meant rewriting the whole base. Counting rewrites across a whole ingest
        turned out to depend on process state other tests leave behind, so this asserts the file
        effect of a single call instead.
        """
        with tempfile.TemporaryDirectory() as store:
            adapter, records = self._seeded(store)
            base = adapter._durable_read_cache_snapshot_path()
            delta = adapter._durable_read_cache_delta_path()
            base_before = base.stat().st_size
            delta_before = delta.stat().st_size if delta.exists() else 0

            longer = list(records) + [{"record_type": "context_document", "id": "doc-extra",
                                       "node_path": "/n/extra", "text": "appended after the base"}]
            adapter._write_durable_read_cache(
                longer, adapter._jsonl_cache_signature_detail(), epoch=None)

            self.assertEqual(base_before, base.stat().st_size,
                             "the whole base was rewritten for a tail of one record")
            self.assertGreater(delta.stat().st_size if delta.exists() else 0, delta_before,
                               "the tail was not appended")

    def test_a_list_that_is_not_a_continuation_rewrites_the_base(self):
        """The other half. The fingerprint has to REFUSE as well as permit, or it proves nothing."""
        with tempfile.TemporaryDirectory() as store:
            adapter, records = self._seeded(store)
            base = adapter._durable_read_cache_snapshot_path()
            base_before = base.read_bytes()

            # same length as the persisted prefix plus one, but a different record at the position
            # the head named -- so this list is NOT a continuation of what is on disk
            diverged = [dict(r) for r in records]
            diverged[-1] = {"record_type": "context_document", "id": "doc-different",
                            "node_path": "/n/different", "text": "not what the head recorded"}
            diverged.append({"record_type": "context_document", "id": "doc-extra2",
                             "node_path": "/n/extra2", "text": "tail"})
            adapter._durable_read_cache_state = None      # force the fingerprint path
            adapter._write_durable_read_cache(
                diverged, adapter._jsonl_cache_signature_detail(), epoch=None)

            self.assertNotEqual(base_before, base.read_bytes(),
                                "a list that is not a continuation was appended as a tail, which "
                                "would leave the snapshot missing records")

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
