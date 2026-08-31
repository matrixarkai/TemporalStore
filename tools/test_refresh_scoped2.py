#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The refresh pass's typed subset runs the SAME serving chain as the full read.

The first attempt at this seam was stopped by its own shadow harness; this version earned zero
mismatches on live traffic by fixing three things these tests pin:

* chain parity -- the subset goes through the latest-state fold BEFORE the tombstone sweep and
  expiry filter, in the full read's order (skipping the fold silently dropped profile-shadowing
  and copy-collapse behaviour);
* the scan must not let the serving default eat requested types -- without return_index_records
  the scan drops embedding rows AFTER filtering, which showed as 78 embeddings missing from every
  compared pass, one direction, every time;
* signature drift between the two proxy-client classes fails SAFE: a TypeError sends the caller
  to the full read, never to a retry whose scan silently lacks the rows it asked for.
"""
import unittest

try:
    from tools import matrixark_mcp_local_adapter as local_mod
    from tools import matrixark_mcp_temporal_adapters as adapters
except ImportError:  # run from tools/ dir
    import matrixark_mcp_local_adapter as local_mod
    import matrixark_mcp_temporal_adapters as adapters


class _Adapter(adapters.MatrixArkTemporalStoreDirectAdapter):
    def __init__(self, scanned, *, latest_state=None, full=None):
        self.scanned = scanned
        self.latest_state = latest_state or []
        self.full = full or []
        self.full_reads = 0
        self.scan_kwargs = None

    def _scan_records_of_types(self, record_types, record_ids=None, scope=None,
                               newest_by_type=None):
        # Prior context caps the event fetch; the stub records the cap rather than
        # refusing it, so a signature change cannot silently turn into a full read.
        self.newest_by_type = newest_by_type
        self.scan_kwargs = {"record_types": list(record_types), "record_ids": record_ids}
        if self.scanned is None:
            return None
        wanted = set(record_types)
        return [r for r in self.scanned if str(r.get("record_type") or "") in wanted]

    def _load_latest_context_state_records(self):
        return list(self.latest_state)

    def read_all(self):
        self.full_reads += 1
        return list(self.full)

    def _get_count(self):
        return 7


def _event(event_id, at_ms):
    return {"record_type": "context_event", "event_id_hash": event_id, "text": "e%s" % event_id,
            "updated_at_ms": at_ms}


class RefreshSubsetTests(unittest.TestCase):
    def test_the_subset_serves_without_a_full_read(self):
        adapter = _Adapter([_event("1", 100)])
        records = adapter.records_for_summary_refresh()
        self.assertEqual(0, adapter.full_reads)
        self.assertEqual(["1"], [r["event_id_hash"] for r in records])

    def test_embeddings_are_requested_and_survive(self):
        """The class of row the serving default used to eat."""
        adapter = _Adapter([
            _event("1", 100),
            {"record_type": "context_embedding", "embedding_type": "node_l0",
             "ref_type": "node", "ref_hash": 9, "node_hash": 9, "updated_at_ms": 150},
        ])
        records = adapter.records_for_summary_refresh()
        self.assertIn("context_embedding", adapter.scan_kwargs["record_types"])
        self.assertEqual(1, len([r for r in records
                                 if r.get("record_type") == "context_embedding"]))

    def test_a_deleted_event_does_not_reach_the_pass(self):
        adapter = _Adapter([
            _event("1", 100),
            {"record_type": local_mod.MEMORY_TOMBSTONE_RECORD_TYPE, "tombstone_kind": "delete",
             "target_memory_id": "1", "created_at_ms": 200},
        ])
        records = adapter.records_for_summary_refresh()
        self.assertEqual([], [r for r in records if r.get("record_type") == "context_event"])

    def test_latest_state_rows_are_folded_in(self):
        adapter = _Adapter([_event("1", 100)],
                           latest_state=[{"record_type": "context_summary", "summary_hash": "s9",
                                          "summary_text": "from the hash", "updated_at_ms": 150}])
        records = adapter.records_for_summary_refresh()
        self.assertIn("s9", {r.get("summary_hash") for r in records
                             if r.get("record_type") == "context_summary"})

    def test_an_unanswerable_scan_serves_the_full_read(self):
        adapter = _Adapter(None, full=[_event("7", 100)])
        records = adapter.records_for_summary_refresh()
        self.assertEqual(1, adapter.full_reads)
        self.assertEqual("7", records[0]["event_id_hash"])

    def test_a_failing_latest_state_load_serves_the_full_read(self):
        adapter = _Adapter([_event("1", 100)], full=[_event("7", 100)])

        def _boom():
            raise RuntimeError("latest-state hash unreachable")

        adapter._load_latest_context_state_records = _boom
        records = adapter.records_for_summary_refresh()
        self.assertEqual(1, adapter.full_reads)
        self.assertEqual("7", records[0]["event_id_hash"])

    def test_index_postings_are_not_requested(self):
        """The store's largest class stays out of the pass on purpose."""
        adapter = _Adapter([_event("1", 100)])
        adapter.records_for_summary_refresh()
        self.assertNotIn("context_index", adapter.scan_kwargs["record_types"])


class TypeErrorFailsSafeTests(unittest.TestCase):
    def test_signature_drift_sends_the_caller_to_its_fallback(self):
        """A client missing a newer kwarg must yield None, never a degraded scan."""
        adapter = object.__new__(adapters.MatrixArkTemporalStoreDirectAdapter)
        adapter._count_key = "k"
        adapter._record_hash_key = "r"
        adapter._shard_size = 1024

        class _OldClient:
            def matrixark_scan_candidates(self, *, count_key, record_hash_key, shard_size,
                                          scope, record_types, secondary_index_groups,
                                          selected_node_hashes):
                raise AssertionError("must not be reached with newer kwargs")

        adapter._client = _OldClient()
        self.assertIsNone(adapter._scan_records_of_types(["context_event"]))


if __name__ == "__main__":
    unittest.main()
