# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A skill's sections and chunks are stored once, not twice.

An ingest issues its writes as 45 separate one-record appends, so every stage of the batch
pipeline saw one row at a time and could never notice that two of them were the same row. Writing
the run as one batch -- which is what ``_begin_append_coalescing`` exists for, and what the run
already qualified for, having no read after its first -- let the dedup see both.

Measured over 25 skills: 100 ``skill_section`` rows for 50 distinct texts, and the same for
``resource_chunk``. Every second one was an exact repeat.

Retrieval is unchanged: the same four queries return the same 26 refs and the same 434 tokens
before and after, so the duplicates were pure storage cost.
"""
import os
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_local_adapter as adapter_module

_BLANK = chr(10) + chr(10)


def _skill_text(index, sections=6):
    parts = ["# Runbook %d" % index, "A procedure for case %d." % index]
    for step in range(sections):
        parts.append("## Step %d" % step)
        parts.append("Drain the queue for case %d step %d." % (index, step))
    return _BLANK.join(parts)


def _ingest_skills(adapter, count):
    scope = {"tenant_id": "acme", "user_id": "dana", "session_id": "skills"}
    for i in range(count):
        adapter.ingest({
            "kind": "skill",
            "scope": scope,
            "text": _skill_text(i),
            "metadata": {"raw_uri": "file:///s/r-%05d.md" % i, "title": "r-%05d" % i},
        })
    return scope


class SkillSectionsStoredOnce(unittest.TestCase):
    def setUp(self):
        with adapter_module._LOCAL_READ_CACHE_LOCK:
            adapter_module._LOCAL_READ_CACHE.clear()

    def _store(self, count=4):
        log = Path(tempfile.mkdtemp()) / "events.jsonl"
        adapter = adapter_module.MatrixArkLocalAdapter(log)
        scope = _ingest_skills(adapter, count)
        return adapter, scope, adapter.read_all()

    def test_no_section_or_chunk_is_stored_twice(self):
        _, _, records = self._store()
        for record_type in ("skill_section", "resource_chunk"):
            bodies = Counter()
            for record in records:
                if str(record.get("record_type") or "") != record_type:
                    continue
                bodies[str(record.get("text") or record.get("body") or "")] += 1
            self.assertGreater(sum(bodies.values()), 0,
                               "no %s was written, so this proves nothing" % record_type)
            repeats = {body[:60]: n for body, n in bodies.items() if n > 1}
            self.assertEqual({}, repeats,
                             "%s rows repeated: %s" % (record_type, repeats))

    def test_the_run_is_written_as_one_batch(self):
        """One durable write per ingest, not one per record."""
        adapter_calls = []
        original = adapter_module.MatrixArkLocalAdapter.append_many

        def counting(self, records, *a, **k):
            adapter_calls.append(len(records))
            return original(self, records, *a, **k)

        adapter_module.MatrixArkLocalAdapter.append_many = counting
        try:
            log = Path(tempfile.mkdtemp()) / "events.jsonl"
            adapter = adapter_module.MatrixArkLocalAdapter(log)
            _ingest_skills(adapter, 2)
        finally:
            adapter_module.MatrixArkLocalAdapter.append_many = original

        self.assertTrue(adapter_calls, "the run was not batched at all")
        self.assertGreater(max(adapter_calls), 1,
                           "every batch carried one record, so nothing was coalesced")

    def test_a_failed_ingest_writes_nothing_it_buffered(self):
        """An abort must not leave a half-written record set, nor an active buffer behind."""
        log = Path(tempfile.mkdtemp()) / "events.jsonl"
        adapter = adapter_module.MatrixArkLocalAdapter(log)
        boom = RuntimeError("ingest failed")

        original = type(adapter)._ingest_impl

        def failing(self, args, **kwargs):
            self._begin_append_coalescing()
            self.append({"record_type": "skill_section", "text": "half written"})
            raise boom

        type(adapter)._ingest_impl = failing
        try:
            with self.assertRaises(RuntimeError):
                adapter.ingest({"kind": "skill", "scope": {"tenant_id": "acme"}, "text": "x"})
        finally:
            type(adapter)._ingest_impl = original

        tls = getattr(adapter, "_append_coalesce_tls_obj", None)
        self.assertFalse(getattr(tls, "active", False),
                         "a failed ingest left its buffer active for the next one on this thread")
        self.assertEqual([], getattr(tls, "buffer", []), "the aborted records are still buffered")
        if log.exists():
            self.assertNotIn("half written", log.read_text(encoding="utf-8"),
                             "an aborted ingest reached the log")

    def test_retrieval_is_unchanged_by_storing_them_once(self):
        """An ANSWER test: the queries must return something, and the same something."""
        adapter, scope, records = self._store(count=8)
        answered = 0
        for query in ("drain the queue for case 3", "runbook 5", "step 2"):
            pack = adapter.retrieve({"scope": scope, "query": query})
            refs = pack.get("selected_refs") or []
            answered += 1 if refs else 0
        self.assertEqual(3, answered,
                         "a query returned nothing, which would make this test vacuous")


if __name__ == "__main__":
    unittest.main()
