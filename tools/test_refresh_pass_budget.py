#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A summary-refresh pass stops at its wall-clock budget, and a stopped pass cannot be wedged.

The node limit bounds how MANY nodes a pass refreshes; nothing bounded how LONG it ran, and on a
backlogged store the pass duration is the foreground latency (measured: add p50 158s with the
refresher churning vs 27.7s without, same store).

The wedge case is the one worth the most care: the native refresher skips a pass when the record
count is unchanged AND the last pass refreshed nothing. A budget that expired before the first
node would store refreshed == 0, and with no new writes the backlog would never drain. A
budget-stopped pass must therefore record "left work behind".
"""
import os
import unittest

try:
    from tools import matrixark_mcp_local_adapter as local_mod
    from tools import matrixark_mcp_temporal_adapters as adapters
except ImportError:  # run from tools/ dir
    import matrixark_mcp_local_adapter as local_mod
    import matrixark_mcp_temporal_adapters as adapters


def _dirty(node, at_ms):
    # The selector requires an int dirty_hash and node_hash; a marker without either is dropped.
    return {"record_type": "context_summary_dirty", "dirty_hash": at_ms, "node_path": [node],
            "node_hash": abs(hash(node)) % (10 ** 9), "updated_at_ms": at_ms, "status": "pending"}


class _Adapter(local_mod.MatrixArkLocalAdapter):
    """Drives refresh_dirty_node_summaries with stubbed node work of controllable duration."""

    def __init__(self, dirty_nodes, *, node_seconds=0.0):
        self._dirty_nodes = dirty_nodes
        self._node_seconds = node_seconds
        self.refreshed_nodes = []
        self._log = list(dirty_nodes)

    def read_all(self):
        return list(self._log)

    def append(self, record):
        self._log.append(record)

    def append_many(self, records):
        self._log.extend(records)

    def append_node_summary_embeddings(self, **kwargs):
        return None

    def context_event_ingestion_time_ms(self, record):
        return int(record.get("updated_at_ms") or 0)

    def node_summary_source_records(self, *, records, node_path, scope, node_hash):
        import time
        if self._node_seconds:
            time.sleep(self._node_seconds)
        self.refreshed_nodes.append(node_path)
        # events, child summaries, entity states, operator states, policy (a MAPPING)
        return ([], [], [], [], {})


class PassBudgetTests(unittest.TestCase):
    def setUp(self):
        self._saved = os.environ.get("MATRIXARK_SUMMARY_REFRESH_PASS_BUDGET_MS")

    def tearDown(self):
        if self._saved is None:
            os.environ.pop("MATRIXARK_SUMMARY_REFRESH_PASS_BUDGET_MS", None)
        else:
            os.environ["MATRIXARK_SUMMARY_REFRESH_PASS_BUDGET_MS"] = self._saved

    def _run(self, adapter):
        return adapter.refresh_dirty_node_summaries(scope={}, limit=64)

    def test_an_exhausted_budget_stops_the_pass_and_reports_the_leftovers(self):
        os.environ["MATRIXARK_SUMMARY_REFRESH_PASS_BUDGET_MS"] = "150"
        adapter = _Adapter([_dirty("n%d" % i, 1000 + i) for i in range(8)], node_seconds=0.1)
        result = self._run(adapter)
        self.assertTrue(result["pass_budget_exhausted"])
        self.assertLess(len(adapter.refreshed_nodes), 8, "the pass must stop early")
        left = result["skipped_dirty_reasons"].get("pass_budget_exhausted", 0)
        self.assertEqual(8, len(adapter.refreshed_nodes) + left,
                         "every node is either refreshed or reported left behind")

    def test_a_generous_budget_changes_nothing(self):
        os.environ["MATRIXARK_SUMMARY_REFRESH_PASS_BUDGET_MS"] = "60000"
        adapter = _Adapter([_dirty("n%d" % i, 1000 + i) for i in range(4)])
        result = self._run(adapter)
        self.assertFalse(result["pass_budget_exhausted"])
        self.assertEqual(4, len(adapter.refreshed_nodes))

    def test_zero_budget_means_unbounded(self):
        os.environ["MATRIXARK_SUMMARY_REFRESH_PASS_BUDGET_MS"] = "0"
        adapter = _Adapter([_dirty("n%d" % i, 1000 + i) for i in range(4)], node_seconds=0.01)
        result = self._run(adapter)
        self.assertFalse(result["pass_budget_exhausted"])
        self.assertEqual(4, len(adapter.refreshed_nodes))


class SkipWedgeTests(unittest.TestCase):
    """The native unchanged-count skip must treat a budget-stopped pass as work left behind."""

    def _wrapper(self, result, *, token=42):
        adapter = object.__new__(adapters.MatrixArkTemporalStoreDirectAdapter)
        adapter.stored = None
        adapter._get_count = lambda: token
        adapter._load_summary_pass_state = lambda: (None, None)
        adapter._store_summary_pass_state = (
            lambda tok, refreshed: setattr(adapter, "stored", (tok, refreshed)))
        # Route the super() call to a stub returning `result`.
        parent = local_mod.MatrixArkLocalAdapter
        original = parent.refresh_summaries
        parent.refresh_summaries = lambda self, args: result
        try:
            adapter.refresh_summaries({"scope": {}})
        finally:
            parent.refresh_summaries = original
        return adapter.stored

    def test_a_budget_stopped_pass_with_zero_refreshed_records_work_left(self):
        stored = self._wrapper({"refreshed_count": 0, "pass_budget_exhausted": True})
        self.assertEqual((42, 1), stored,
                         "refreshed=0 here would let the unchanged-count skip park the backlog")

    def test_a_clean_empty_pass_still_records_zero(self):
        stored = self._wrapper({"refreshed_count": 0, "pass_budget_exhausted": False})
        self.assertEqual((42, 0), stored)

    def test_a_productive_pass_records_its_count(self):
        stored = self._wrapper({"refreshed_count": 5, "pass_budget_exhausted": True})
        self.assertEqual((42, 5), stored)


if __name__ == "__main__":
    unittest.main()
