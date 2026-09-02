# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The scan drops the two routing fields it never reads, and nothing else.

This replaces an allowlist. The pack is assembled from these same rows, so a field the list forgets
is a field the answer cannot print -- which shipped once: retrieval returned "text": "" for every
hit while every ranking number stayed green, because ranking does not read the dropped fields.

A denylist inverts the failure. A field nobody thought of is kept, so the cost of an omission is a
smaller saving rather than an empty answer. That matters more than it sounds: on this corpus a
correct allowlist and this denylist save nearly the same, because the bytes are concentrated in two
fields rather than spread across the ones a list would forget.
"""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_local_adapter as adapter_module
import matrixark_local_adapter_retrieval as retrieval

PROJECTION = "MATRIXARK_RETRIEVAL_PROJECT_SCAN_FIELDS"
PROFILE = "MATRIXARK_ONEBOX_EMBEDDING_FIRST"

FACTS = [
    "I work on the ingestion pipeline and own the retrieval budget code.",
    "My name is Dana and I lead the storage team in Berlin.",
    "The deploy runbook lives in docs/INSTALL.md and needs the staging key.",
]


class TheScanDropsOnlyWhatIsUnused(unittest.TestCase):
    def setUp(self):
        self._saved = {k: os.environ.get(k) for k in (PROJECTION, PROFILE)}

    def tearDown(self):
        for key, value in self._saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    @staticmethod
    def _corpus(store):
        adapter = adapter_module.MatrixArkLocalAdapter(Path(store) / "events.jsonl")
        scope = {"tenant_id": "acme", "user_id": "dana", "session_id": "s0"}
        for i in range(12):
            adapter.ingest({
                "kind": "message",
                "scope": dict(scope, session_id="s%d" % (i // 4)),
                "messages": [{"role": ["user", "assistant", "tool"][i % 3],
                              "content": "%s (turn %d)" % (FACTS[i % len(FACTS)], i)}],
            })
        return adapter, scope

    def test_it_drops_the_routing_fields_and_keeps_everything_else(self):
        record = {"record_type": "context_event", "node_hash": 1, "vector": [0.5, 0.5],
                  "text": "the answer prints this", "heading": "H",
                  "storage_route": {"tier": "hot"}, "storage_options": {"replicas": 3},
                  "a_field_nobody_listed": "kept, because the list names what goes, not what stays"}
        projected = retrieval.project_scan_record(record, True)
        for gone in ("storage_route", "storage_options"):
            self.assertNotIn(gone, projected)
        for kept in ("record_type", "node_hash", "vector", "text", "heading",
                     "a_field_nobody_listed"):
            self.assertIn(kept, projected,
                          "%s was dropped; only the named fields may go" % kept)

    def test_an_unlisted_field_survives(self):
        """The property an allowlist cannot have, stated on its own so it cannot be lost."""
        projected = retrieval.project_scan_record({"invented_later": "x"}, True)
        self.assertIn("invented_later", projected)

    def test_the_answer_is_unchanged_with_the_projection_on(self):
        """Asserted on the ANSWER, not on the ranking.

        Ranking cannot see this: it does not read the dropped fields, so hit@1 stays flat whether
        the reply carries its text or comes back empty.
        """
        os.environ[PROFILE] = "1"
        with tempfile.TemporaryDirectory() as store:
            adapter, scope = self._corpus(store)
            queries = ["what do I work on", "who am I", "where is the deploy runbook"]

            def answers(enabled):
                os.environ[PROJECTION] = "1" if enabled else "0"
                return [json.dumps(adapter.retrieve({"query": q, "scope": scope}),
                                   sort_keys=True, default=str) for q in queries]

            off, on = answers(False), answers(True)
            self.assertEqual(off, on, "the projection changed what retrieval returned")
            self.assertTrue(any(len(a) > 2 for a in off), "no query returned anything to compare")

    def test_the_dropped_fields_are_the_ones_holding_the_bytes(self):
        """Why these two and not others -- if they stop being the bulk, the list should be revisited."""
        with tempfile.TemporaryDirectory() as store:
            adapter, _scope = self._corpus(store)
            records = adapter.read_all()
            total = sum(len(json.dumps(r, separators=(",", ":"), default=str)) for r in records)
            dropped = sum(
                len(json.dumps(r.get(f), separators=(",", ":"), default=str))
                for r in records for f in retrieval.RETRIEVAL_SCAN_DROPPED_FIELDS if f in r)
            self.assertGreater(total, 0)
            self.assertGreater(100.0 * dropped / total, 10.0,
                               "the dropped fields hold only %.1f%% of the scan; this projection "
                               "is no longer paying for itself" % (100.0 * dropped / total))


if __name__ == "__main__":
    unittest.main()
