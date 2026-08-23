#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`users()` counts every subject in one pass over the record log.

The listing used to ask a scoped `get_all` per subject, and `get_all` on a native backend reads the
whole log into Python before filtering, so the call cost O(subjects x store) -- 21 full reads for
one `users()` over 20 subjects, each around 450ms. These tests pin the cheaper shape AND the answer
it has to keep producing: same counts, same liveness, one read.

The read count is asserted directly, because that is the property that regresses silently -- a
correct answer computed the expensive way looks identical from outside.
"""
import unittest

try:
    from tools import matrixark_mcp_temporal_adapters as adapters
    from tools import matrixark_mcp_local_adapter as local_mod
except ImportError:  # run from tools/ dir
    import matrixark_mcp_temporal_adapters as adapters
    import matrixark_mcp_local_adapter as local_mod


class _CountingAdapter:
    """A direct adapter whose record log is a list, counting how often it is read.

    `object.__new__` skips __init__ (which would dial a metaserver and spawn a CLI); only what
    these paths touch is stubbed.
    """

    @staticmethod
    def build(records):
        adapter = object.__new__(adapters.MatrixArkTemporalStoreDirectAdapter)
        adapter._storage_prefix = "matrixark:mcp"
        adapter._local_jsonl_enabled = False
        adapter._native_log = list(records)
        adapter.reads = 0

        def _read_all():
            adapter.reads += 1
            return list(adapter._native_log)

        adapter.read_all = _read_all
        return adapter


def _hashes_for(adapter, name):
    """The tenant/user hashes the product itself derives for a subject name."""
    return adapter._resolve_subject_hashes(adapter._subject_scope({}, name))


def _event(adapter, name, event_id, text):
    """A `context_event` row scoped to `name`, carrying the hashes the reader matches on."""
    tenant_hash, user_hash = _hashes_for(adapter, name)
    scope_key = f"t={tenant_hash}|u={user_hash}|s=1|"
    return {
        "record_type": "context_event",
        "event_id_hash": event_id,
        "text": text,
        "scope_key": scope_key,
        "access_scope": {"tenant_hash": tenant_hash, "user_hash": user_hash,
                         "scope_key": scope_key},
        "extraction_phase": "final",
        "status": "extraction_committed",
        "updated_at_ms": 1000 + event_id,
        "timestamp_key_ms": 1000 + event_id,
    }


class SubjectCountsInOnePassTests(unittest.TestCase):
    def _adapter_with(self, per_user):
        adapter = _CountingAdapter.build([])
        log = []
        event_id = 1
        for name, how_many in per_user.items():
            for _ in range(how_many):
                log.append(_event(adapter, name, event_id, f"{name} memory {event_id}"))
                event_id += 1
        adapter._native_log = log
        return adapter

    def test_counts_each_subject_from_the_records_own_scope(self):
        adapter = self._adapter_with({"alice": 3, "bob": 1})
        counts = adapter._subject_counts_in_one_pass({}, ["alice", "bob"])
        self.assertEqual({"alice": 3, "bob": 1}, counts)

    def test_the_log_is_read_once_regardless_of_how_many_subjects(self):
        """The whole point: cost stops scaling with the number of subjects."""
        adapter = self._adapter_with({"u%d" % i: 2 for i in range(12)})
        adapter.reads = 0
        counts = adapter._subject_counts_in_one_pass({}, ["u%d" % i for i in range(12)])
        self.assertEqual(1, adapter.reads, "twelve subjects must still cost one read")
        self.assertEqual({"u%d" % i: 2 for i in range(12)}, counts)

    def test_a_subject_with_no_records_counts_zero_rather_than_being_missed(self):
        adapter = self._adapter_with({"alice": 2})
        counts = adapter._subject_counts_in_one_pass({}, ["alice", "never_ingested"])
        self.assertEqual({"alice": 2, "never_ingested": 0}, counts)

    def test_another_subjects_records_are_not_attributed_to_this_one(self):
        adapter = self._adapter_with({"alice": 2, "bob": 5})
        counts = adapter._subject_counts_in_one_pass({}, ["alice"])
        self.assertEqual({"alice": 2}, counts, "bob's five must not land on alice")

    def test_an_unresolvable_subject_returns_none_so_the_caller_falls_back(self):
        """None, not {} -- an empty map would report that nobody has memories, and `users()`
        would answer with an empty list instead of falling back to the per-subject reads."""
        adapter = self._adapter_with({"alice": 2})
        adapter._resolve_subject_hashes = lambda scope: (0, 0)
        self.assertIsNone(adapter._subject_counts_in_one_pass({}, ["alice"]))

    def test_two_names_on_one_hash_return_none_rather_than_a_wrong_split(self):
        adapter = self._adapter_with({"alice": 2, "bob": 1})
        adapter._resolve_subject_hashes = lambda scope: (7, 99)
        self.assertIsNone(adapter._subject_counts_in_one_pass({}, ["alice", "bob"]))

    def test_a_read_failure_returns_none_rather_than_zero_counts(self):
        adapter = self._adapter_with({"alice": 2})

        def _boom():
            raise RuntimeError("record log unavailable")

        adapter.read_all = _boom
        self.assertIsNone(adapter._subject_counts_in_one_pass({}, ["alice"]))

    def test_only_context_events_are_counted(self):
        """`get_all` counts memories, not the summaries and postings derived from them."""
        adapter = self._adapter_with({"alice": 1})
        tenant_hash, user_hash = _hashes_for(adapter, "alice")
        scope_key = f"t={tenant_hash}|u={user_hash}|s=1|"
        for record_type in ("context_summary", "context_embedding", "context_index"):
            adapter._native_log.append({
                "record_type": record_type,
                "scope_key": scope_key,
                "access_scope": {"tenant_hash": tenant_hash, "user_hash": user_hash,
                                 "scope_key": scope_key},
            })
        self.assertEqual({"alice": 1}, adapter._subject_counts_in_one_pass({}, ["alice"]))


class ListMemorySubjectsTests(unittest.TestCase):
    """End to end through `list_memory_subjects`, with the subject index stubbed."""

    def _adapter(self, per_user, index_names):
        adapter = _CountingAdapter.build([])
        log, event_id = [], 1
        for name, how_many in per_user.items():
            for _ in range(how_many):
                log.append(_event(adapter, name, event_id, f"{name} memory {event_id}"))
                event_id += 1
        adapter._native_log = log
        adapter._ensure_subject_index = lambda: None
        adapter._subject_index_key = lambda: "matrixark:mcp:subject_index"

        class _Client:
            @staticmethod
            def scan_hash(_key):
                return {"records": [{"field": "user:%s" % n} for n in index_names]}

        adapter._client = _Client()
        return adapter

    def test_listing_costs_one_read_and_reports_each_subjects_count(self):
        adapter = self._adapter({"alice": 3, "bob": 1}, ["alice", "bob"])
        adapter.reads = 0
        listed = adapter.list_memory_subjects({})
        self.assertEqual(1, adapter.reads)
        self.assertEqual(
            [{"type": "user", "name": "alice", "memory_count": 3},
             {"type": "user", "name": "bob", "memory_count": 1}],
            listed["results"],
        )

    def test_a_subject_the_index_still_names_but_holds_nothing_is_dropped(self):
        """The index is add-only, so it outlives the memories; `users()` answers who HAS memories."""
        adapter = self._adapter({"alice": 2}, ["alice", "forgotten_user"])
        listed = adapter.list_memory_subjects({})
        self.assertEqual(["alice"], [row["name"] for row in listed["results"]])
        self.assertEqual(1, listed["count"])


if __name__ == "__main__":
    unittest.main()
