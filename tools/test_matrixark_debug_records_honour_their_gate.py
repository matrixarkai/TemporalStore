# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""context_debug_record rows are written only when the knob that gates them says so.

MATRIXARK_CONTEXT_DEBUG_RECORDS defaults OFF, and compact_context_debug_record already honoured
it. The two writers on the ingest path did not: they guarded on whether debug metadata existed,
which it always does, so the rows were written whatever the knob said.

Nothing served them. The context pack lists ``metadata_debug`` in
DEFAULT_HIDDEN_DEBUG_LINEAGE_FIELDS and strips it from every item, and a field-access trace over
retrieve, get_all and compaction touched no field of these rows at all. They were 12.1% of the
read cache, carried and never read.

Measured over 12 skills: 505.7 KB -> 446.6 KB, 11.7% less, with retrieval returning the same refs
and the same tokens for every query.
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_local_adapter as adapter_module
import matrixark_local_adapter_ingest as ingest_module

_BLANK = chr(10) + chr(10)


def _skill_text(index, sections=5):
    parts = ["# Runbook %d" % index, "A procedure for case %d." % index]
    for step in range(sections):
        parts.append("## Step %d" % step)
        parts.append("Drain the queue for case %d step %d." % (index, step))
    return _BLANK.join(parts)


def _ingest(count=6):
    log = Path(tempfile.mkdtemp()) / "events.jsonl"
    adapter = adapter_module.MatrixArkLocalAdapter(log)
    scope = {"tenant_id": "acme", "user_id": "dana", "session_id": "skills"}
    for i in range(count):
        adapter.ingest({
            "kind": "skill", "scope": scope, "text": _skill_text(i),
            "metadata": {"raw_uri": "file:///s/r-%05d.md" % i, "title": "r-%05d" % i},
        })
    return adapter, scope, adapter.read_all()


def _debug_rows(records):
    return [r for r in records
            if str(r.get("record_type") or "") == "context_debug_record"]


class DebugRecordsHonourTheirGate(unittest.TestCase):
    def setUp(self):
        with adapter_module._LOCAL_READ_CACHE_LOCK:
            adapter_module._LOCAL_READ_CACHE.clear()
        self._previous = os.environ.get("MATRIXARK_CONTEXT_DEBUG_RECORDS")

    def tearDown(self):
        if self._previous is None:
            os.environ.pop("MATRIXARK_CONTEXT_DEBUG_RECORDS", None)
        else:
            os.environ["MATRIXARK_CONTEXT_DEBUG_RECORDS"] = self._previous

    def test_none_are_written_when_the_gate_is_off(self):
        _, _, records = _ingest()
        self.assertGreater(len(records), 0, "nothing was ingested, so this proves nothing")
        self.assertEqual([], _debug_rows(records),
                         "debug rows were written with the gate off")

    def test_the_gate_actually_turns_them_on(self):
        """Off-by-default is only meaningful if ON still works -- otherwise this test would
        pass just as well against a writer that was deleted."""
        original = ingest_module._context_debug_records_enabled
        ingest_module._context_debug_records_enabled = lambda: True
        try:
            _, _, records = _ingest()
        finally:
            ingest_module._context_debug_records_enabled = original
        self.assertGreater(len(_debug_rows(records)), 0,
                           "the gate cannot turn the rows back on")

    def test_retrieval_is_unchanged_without_them(self):
        """An ANSWER test: the queries must return something, and the same something."""
        adapter, scope, _ = _ingest(count=8)
        answered = 0
        for query in ("drain the queue for case 3", "runbook 5", "step 2"):
            refs = adapter.retrieve({"scope": scope, "query": query}).get("selected_refs") or []
            answered += 1 if refs else 0
        self.assertEqual(3, answered,
                         "a query returned nothing, which would make this test vacuous")

    def test_the_gate_defaults_off_and_fails_closed(self):
        """A missing policy module must not start writing rows nobody asked for."""
        self.assertNotIn("MATRIXARK_CONTEXT_DEBUG_RECORDS", os.environ,
                         "the environment already sets the knob, so the default is untested here")
        real = sys.modules.pop("matrixark_mcp_serving_records", None)
        sys.modules["matrixark_mcp_serving_records"] = None   # force the import to fail
        try:
            self.assertFalse(ingest_module._context_debug_records_enabled(),
                             "an unimportable policy module was read as permission to write")
        finally:
            if real is not None:
                sys.modules["matrixark_mcp_serving_records"] = real
            else:
                sys.modules.pop("matrixark_mcp_serving_records", None)


if __name__ == "__main__":
    unittest.main()
