#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A summary refresh pass reads the record log once when it can, twice only when it must.

The pass used to read the whole log twice back to back -- once to find dirty nodes, once to find
rows needing embeddings. Instrumented over three ingests the refresher was the largest reader in
the process, and on a native backend each read holds the single shared proxy lane for its whole
duration, which is the resource request traffic contends for.

Reuse is only sound when the summary half wrote NOTHING: then the store is unchanged and the second
read returns the same records. These tests pin both directions, because the failure mode of getting
it wrong is silent -- embeddings computed against a stale view would simply be missing, and the next
pass would paper over it.
"""
import unittest

try:
    from tools import matrixark_mcp_local_adapter as local_mod
except ImportError:  # run from tools/ dir
    import matrixark_mcp_local_adapter as local_mod


class _Adapter(local_mod.MatrixArkLocalAdapter):
    """Counts reads and records what each half of the pass was handed."""

    def __init__(self, refreshed_count):
        self.reads = 0
        self.refreshed_count = refreshed_count
        self.summary_records = "unset"
        self.embedding_records = "unset"
        self._log = [{"record_type": "context_event", "event_id_hash": 1, "text": "hello"}]

    def read_all(self):
        self.reads += 1
        return list(self._log)

    def refresh_dirty_node_summaries(self, *, records=None, **kwargs):
        self.summary_records = records
        if records is None:
            self.read_all()
        return {"status": "ok", "refreshed_count": self.refreshed_count, "refreshed": []}

    def ensure_context_embeddings(self, *, records=None, **kwargs):
        self.embedding_records = records
        if records is None:
            self.read_all()
        return {"status": "ok"}


class RefreshPassReadTests(unittest.TestCase):
    def test_a_pass_that_refreshes_nothing_reads_once(self):
        adapter = _Adapter(refreshed_count=0)
        result = adapter.refresh_summaries({"scope": {}})
        self.assertEqual(1, adapter.reads)
        self.assertTrue(result["embedding_refresh_reused_pass_records"])

    def test_both_halves_get_the_same_snapshot_when_nothing_was_written(self):
        adapter = _Adapter(refreshed_count=0)
        adapter.refresh_summaries({"scope": {}})
        self.assertIsNotNone(adapter.summary_records)
        self.assertIs(adapter.summary_records, adapter.embedding_records)

    def test_a_pass_that_wrote_something_reads_again_for_the_embeddings(self):
        """The embedding half has to SEE the summaries the pass just wrote."""
        adapter = _Adapter(refreshed_count=3)
        result = adapter.refresh_summaries({"scope": {}})
        self.assertEqual(2, adapter.reads)
        self.assertIsNone(adapter.embedding_records,
                          "a written-to store must be re-read, not reused")
        self.assertFalse(result["embedding_refresh_reused_pass_records"])

    def test_the_summary_half_never_reads_for_itself(self):
        """Its read is hoisted into the pass; it is the same read, taken one level up."""
        for refreshed in (0, 5):
            with self.subTest(refreshed=refreshed):
                adapter = _Adapter(refreshed_count=refreshed)
                adapter.refresh_summaries({"scope": {}})
                self.assertIsNotNone(adapter.summary_records)

    def test_an_unreadable_refreshed_count_re_reads_rather_than_reusing(self):
        """Conservative: if the pass cannot say what it wrote, assume it wrote."""
        adapter = _Adapter(refreshed_count=None)

        def _refresh(*, records=None, **kwargs):
            adapter.summary_records = records
            return {"status": "ok", "refreshed_count": "not a number"}

        adapter.refresh_dirty_node_summaries = _refresh
        result = adapter.refresh_summaries({"scope": {}})
        self.assertEqual(2, adapter.reads)
        self.assertFalse(result["embedding_refresh_reused_pass_records"])

    def test_skipping_embeddings_still_only_reads_once(self):
        adapter = _Adapter(refreshed_count=7)
        adapter.refresh_summaries({"scope": {}, "ensure_embeddings": False})
        self.assertEqual(1, adapter.reads)
        self.assertEqual("unset", adapter.embedding_records)


if __name__ == "__main__":
    unittest.main()
