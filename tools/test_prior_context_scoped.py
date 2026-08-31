#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Prior-context collection reads a five-type scan, not the whole store -- and stays equivalent.

The read at the top of `batch_extract` was the last unconditional O(store) read on the ingest
path. Its consumers touch three record types; the subset fetch adds tombstones and retention
markers so the live-view semantics reproduce via the SAME functions the full read applies.

What a wrong implementation gets silently wrong, pinned here:

* records living only in the latest-state HASH (refresher-written summaries) are not in the
  append log, and a scan-only subset misses them -- the live shadow-compare caught exactly that;
* a deleted event must not reappear in prior context (the sweep must run on the subset);
* a scan failure must serve the full read, never an empty prior context.
"""
import unittest

try:
    from tools import matrixark_mcp_local_adapter as local_mod
    from tools import matrixark_mcp_temporal_adapters as adapters
except ImportError:  # run from tools/ dir
    import matrixark_mcp_local_adapter as local_mod
    import matrixark_mcp_temporal_adapters as adapters

SCOPE_KEY = "t=11|u=22|s=33|"


def _event(event_id, *, at_ms, text="hello"):
    return {"record_type": "context_event", "event_id_hash": event_id, "text": text,
            "scope_key": SCOPE_KEY,
            "access_scope": {"tenant_hash": 11, "user_hash": 22, "scope_key": SCOPE_KEY},
            "updated_at_ms": at_ms, "timestamp_key_ms": at_ms}


def _summary(summary_hash, *, at_ms):
    return {"record_type": "context_summary", "summary_hash": summary_hash,
            "summary_type": "session_l0", "summary_text": "s", "scope_key": SCOPE_KEY,
            "updated_at_ms": at_ms}


def _delete_tombstone(target_id, *, at_ms):
    return {"record_type": local_mod.MEMORY_TOMBSTONE_RECORD_TYPE, "tombstone_kind": "delete",
            "target_memory_id": str(target_id), "created_at_ms": at_ms}


class _Adapter(adapters.MatrixArkTemporalStoreDirectAdapter):
    def __init__(self, scanned, *, latest_state=None, full=None):
        self.scanned = scanned
        self.latest_state = latest_state or []
        self.full = full or []
        self.full_reads = 0

    def _scan_records_of_types(self, record_types, record_ids=None, scope=None,
                               newest_by_type=None):
        # Prior context caps the event fetch; the stub records the cap rather than
        # refusing it, so a signature change cannot silently turn into a full read.
        self.newest_by_type = newest_by_type
        if self.scanned is None:
            return None
        wanted = set(record_types)
        return [r for r in self.scanned if str(r.get("record_type") or "") in wanted]

    def _load_latest_context_state_records(self):
        return list(self.latest_state)

    def read_all(self):
        self.full_reads += 1
        return list(self.full)


class PriorContextScopedTests(unittest.TestCase):
    def test_the_subset_serves_without_a_full_read(self):
        adapter = _Adapter([_event("1", at_ms=100), _summary("s1", at_ms=150)])
        records = adapter.prior_context_records()
        self.assertEqual(0, adapter.full_reads)
        self.assertEqual({"1"}, {r.get("event_id_hash") for r in records
                                 if r.get("record_type") == "context_event"})

    def test_latest_state_records_are_folded_in(self):
        """Refresher-written summaries live in the latest-state hash, not the append log."""
        adapter = _Adapter([_event("1", at_ms=100)],
                           latest_state=[_summary("s9", at_ms=200)])
        records = adapter.prior_context_records()
        self.assertIn("s9", {r.get("summary_hash") for r in records
                             if r.get("record_type") == "context_summary"})

    def test_latest_state_records_of_other_types_are_not_dragged_in(self):
        adapter = _Adapter([_event("1", at_ms=100)],
                           latest_state=[{"record_type": "context_embedding", "ref_hash": 7}])
        records = adapter.prior_context_records()
        self.assertEqual([], [r for r in records if r.get("record_type") == "context_embedding"])

    def test_a_deleted_event_does_not_reappear_in_prior_context(self):
        """The tombstone sweep must run on the subset, or extraction sees deleted content."""
        adapter = _Adapter([_event("1", at_ms=100), _event("2", at_ms=110),
                            _delete_tombstone("1", at_ms=200)])
        records = adapter.prior_context_records()
        ids = {r.get("event_id_hash") for r in records if r.get("record_type") == "context_event"}
        self.assertEqual({"2"}, ids)

    def test_duplicate_event_rows_collapse_to_one(self):
        """Ingest persists an event twice (hot path + committed); both serving would double it."""
        adapter = _Adapter([_event("1", at_ms=100, text="pending"),
                            _event("1", at_ms=150, text="committed")])
        events = [r for r in adapter.prior_context_records()
                  if r.get("record_type") == "context_event"]
        self.assertEqual(1, len(events))
        self.assertEqual("committed", events[0]["text"])

    def test_an_unanswerable_scan_serves_the_full_read(self):
        adapter = _Adapter(None, full=[_event("7", at_ms=100)])
        records = adapter.prior_context_records()
        self.assertEqual(1, adapter.full_reads)
        self.assertEqual("7", records[0]["event_id_hash"])

    def test_a_failing_latest_state_load_serves_the_full_read(self):
        adapter = _Adapter([_event("1", at_ms=100)], full=[_event("7", at_ms=100)])

        def _boom():
            raise RuntimeError("latest-state hash unreachable")

        adapter._load_latest_context_state_records = _boom
        records = adapter.prior_context_records()
        self.assertEqual(1, adapter.full_reads)
        self.assertEqual("7", records[0]["event_id_hash"])


class BaseSeamTests(unittest.TestCase):
    def test_the_base_implementation_is_the_full_read(self):
        adapter = object.__new__(local_mod.MatrixArkLocalAdapter)
        adapter.read_all = lambda: [{"record_type": "context_event", "event_id_hash": "1"}]
        self.assertEqual("1", adapter.prior_context_records()[0]["event_id_hash"])


if __name__ == "__main__":
    unittest.main()
