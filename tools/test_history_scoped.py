#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""History walks a three-type scan, and its output is unchanged.

The raw read hauled the whole store to answer one memory id. What history actually reports:
the id's ingest, its supersede/delete tombstones, its creation-by-supersede link, and its
feedback ratings -- three record types. The test drives `history()` itself through both the
subset path and the raw fallback and requires identical output, because the seam's contract is
output equivalence, not input similarity.
"""
import unittest

try:
    from tools import matrixark_mcp_local_adapter as local_mod
    from tools import matrixark_mcp_temporal_adapters as adapters
except ImportError:  # run from tools/ dir
    import matrixark_mcp_local_adapter as local_mod
    import matrixark_mcp_temporal_adapters as adapters

TOMBSTONE = local_mod.MEMORY_TOMBSTONE_RECORD_TYPE


def _story():
    """A memory's full life: ingested twice (hot+committed), rated, superseded, deleted."""
    return [
        {"record_type": "context_event", "event_id_hash": "77", "text": "pending",
         "extraction_phase": "hot_path", "updated_at_ms": 100},
        {"record_type": "context_summary", "summary_hash": "s1", "summary_text": "noise",
         "updated_at_ms": 110},
        {"record_type": "context_event", "event_id_hash": "77", "text": "committed",
         "extraction_phase": "final", "updated_at_ms": 150},
        {"record_type": local_mod.MatrixArkLocalAdapter.MEMORY_FEEDBACK_RECORD_TYPE,
         "target_memory_id": "77", "feedback": "POSITIVE", "created_at_ms": 200},
        {"record_type": TOMBSTONE, "tombstone_kind": "delete", "tombstone_reason": "supersede",
         "target_memory_id": "77", "superseded_by": "88", "created_at_ms": 300},
        {"record_type": "context_event", "event_id_hash": "88", "text": "the new version",
         "updated_at_ms": 300},
        {"record_type": TOMBSTONE, "tombstone_kind": "delete", "target_memory_id": "88",
         "created_at_ms": 400},
        {"record_type": "context_embedding", "ref_hash": 77, "updated_at_ms": 410},
    ]


class _NativeAdapter(adapters.MatrixArkTemporalStoreDirectAdapter):
    def __init__(self, records, *, scan_available=True):
        self._records = records
        self._scan_available = scan_available
        self.scan_calls = 0
        self.raw_reads = 0

    def _scan_records_of_types(self, record_types, record_ids=None):
        self.scan_calls += 1
        self.last_record_ids = list(record_ids or [])
        if not self._scan_available:
            return None
        wanted = set(record_types)
        subset = [r for r in self._records if str(r.get("record_type") or "") in wanted]
        if record_ids:
            ids = {str(i) for i in record_ids}
            def linked(r):
                own = {str(r.get(f)) for f in ("event_id_hash", "target_memory_id", "superseded_by")
                       if r.get(f) is not None}
                return bool(own & ids)
            subset = [r for r in subset if linked(r)]
        return subset

    def _read_raw_records(self):
        self.raw_reads += 1
        return list(self._records)


class HistoryScopedTests(unittest.TestCase):
    def _events(self, adapter, memory_id):
        return [(e["event"], e.get("superseded_by") or e.get("supersedes_memory_id")
                 or e.get("feedback"))
                for e in adapter.history({"memory_id": memory_id})["history"]]

    def test_the_subset_answers_without_a_raw_read(self):
        adapter = _NativeAdapter(_story())
        events = self._events(adapter, "77")
        self.assertEqual(0, adapter.raw_reads)
        self.assertEqual(["77"], adapter.last_record_ids,
                         "the memory id must reach the scan as its scoping hint")
        self.assertEqual([("ingested", None), ("feedback", "POSITIVE"),
                          ("superseded", "88")], events)

    def test_the_subset_output_equals_the_raw_output(self):
        """The contract: history() itself, both paths, identical answers -- for every id."""
        for memory_id in ("77", "88"):
            scan = _NativeAdapter(_story())
            raw = _NativeAdapter(_story(), scan_available=False)
            self.assertEqual(
                raw.history({"memory_id": memory_id}),
                scan.history({"memory_id": memory_id}),
                "path divergence for id %s" % memory_id,
            )

    def test_a_superseding_memory_reports_its_creation_link(self):
        adapter = _NativeAdapter(_story())
        events = self._events(adapter, "88")
        self.assertEqual([("created", "77"), ("ingested", None), ("deleted", None)], events)

    def test_an_unavailable_scan_falls_back_to_the_raw_read(self):
        adapter = _NativeAdapter(_story(), scan_available=False)
        events = self._events(adapter, "77")
        self.assertEqual(1, adapter.raw_reads)
        self.assertEqual([("ingested", None), ("feedback", "POSITIVE"),
                          ("superseded", "88")], events)

    def test_duplicate_event_rows_still_collapse_to_one_ingest(self):
        adapter = _NativeAdapter(_story())
        events = [e for e, _ in self._events(adapter, "77")]
        self.assertEqual(1, events.count("ingested"))


if __name__ == "__main__":
    unittest.main()
