#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""mem0 `feedback`: rate an existing memory, without the rating becoming a memory.

There was already a `matrixark_feedback` tool, but it means something else -- it ingests feedback
TEXT as a new memory. mem0's `feedback` attaches a rating to an EXISTING memory, and nothing did
that.

The two things worth pinning are what it must NOT do. A rating stored as a `context_event` would
show up in `get_all` and compete for retrieval with real memories, which is worse than not storing
it. And a rating stored somewhere nothing reads would be a feature only on paper -- so it has to
come back out of `history`.
"""
import unittest

try:
    from tools import matrixark_mcp_local_adapter as adapter_mod
    from tools import matrixark_v1_gateway as gateway
except ImportError:  # run from tools/ dir
    import matrixark_mcp_local_adapter as adapter_mod
    import matrixark_v1_gateway as gateway

SCOPE_KEY = "t=11|u=22|s=33|"


class _Adapter(adapter_mod.MatrixArkLocalAdapter):
    """An adapter whose log is a list, so the record this writes can be inspected directly."""

    def __init__(self, memories):
        self.appended = []
        self._log = list(memories)

    def read_all(self):
        return list(self._log) + list(self.appended)

    def _read_raw_records(self):
        return list(self._log) + list(self.appended)

    def append(self, record):
        self.appended.append(record)

    def _resolve_subject_hashes(self, scope):
        return (int(scope.get("tenant_hash") or 0), int(scope.get("user_hash") or 0))


def _memory(memory_id="777"):
    return {"record_type": "context_event", "event_id_hash": memory_id,
            "text": "The escalation contact is Dana.", "scope_key": SCOPE_KEY,
            "access_scope": {"tenant_hash": 11, "user_hash": 22, "scope_key": SCOPE_KEY},
            "updated_at_ms": 1000}


class MemoryFeedbackTests(unittest.TestCase):
    def test_a_rating_is_recorded_against_the_memory(self):
        adapter = _Adapter([_memory()])
        out = adapter.memory_feedback({"memory_id": "777", "feedback": "POSITIVE"})
        self.assertTrue(out["recorded"])
        self.assertEqual(1, len(adapter.appended))
        self.assertEqual("777", adapter.appended[0]["target_memory_id"])
        self.assertEqual("POSITIVE", adapter.appended[0]["feedback"])

    def test_the_rating_is_not_stored_as_a_memory(self):
        """A context_event here would surface in get_all and compete for retrieval."""
        adapter = _Adapter([_memory()])
        adapter.memory_feedback({"memory_id": "777", "feedback": "NEGATIVE"})
        self.assertNotEqual("context_event", adapter.appended[0]["record_type"])
        self.assertEqual(adapter.MEMORY_FEEDBACK_RECORD_TYPE, adapter.appended[0]["record_type"])

    def test_history_reports_it_beside_the_ingest(self):
        adapter = _Adapter([_memory()])
        adapter.memory_feedback({"memory_id": "777", "feedback": "POSITIVE", "feedback_reason": "spot on"})
        events = adapter.history({"memory_id": "777"})["history"]
        self.assertEqual(["ingested", "feedback"], [e["event"] for e in events])
        self.assertEqual("POSITIVE", events[1]["feedback"])
        self.assertEqual("spot on", events[1]["feedback_reason"])

    def test_a_rating_for_another_memory_is_not_reported(self):
        adapter = _Adapter([_memory("777"), _memory("888")])
        adapter.memory_feedback({"memory_id": "888", "feedback": "NEGATIVE"})
        events = adapter.history({"memory_id": "777"})["history"]
        self.assertEqual(["ingested"], [e["event"] for e in events])

    def test_the_reason_is_optional(self):
        adapter = _Adapter([_memory()])
        adapter.memory_feedback({"memory_id": "777", "feedback": "POSITIVE"})
        self.assertNotIn("feedback_reason", adapter.appended[0])

    def test_a_rating_outside_the_vocabulary_is_refused(self):
        """Refused, not stored: a rating nobody can interpret reads as feedback that was understood."""
        adapter = _Adapter([_memory()])
        with self.assertRaises(adapter_mod.MatrixArkInvalidRequestError):
            adapter.memory_feedback({"memory_id": "777", "feedback": "AMAZING"})
        self.assertEqual([], adapter.appended)

    def test_the_rating_is_case_insensitive(self):
        adapter = _Adapter([_memory()])
        adapter.memory_feedback({"memory_id": "777", "feedback": "positive"})
        self.assertEqual("POSITIVE", adapter.appended[0]["feedback"])

    def test_a_missing_rating_is_refused(self):
        adapter = _Adapter([_memory()])
        with self.assertRaises(adapter_mod.MatrixArkInvalidRequestError):
            adapter.memory_feedback({"memory_id": "777"})

    def test_a_missing_memory_id_is_refused(self):
        adapter = _Adapter([_memory()])
        with self.assertRaises(adapter_mod.MatrixArkInvalidRequestError):
            adapter.memory_feedback({"feedback": "POSITIVE"})

    def test_an_unknown_memory_raises_not_found(self):
        adapter = _Adapter([_memory()])
        with self.assertRaises(adapter_mod.MatrixArkNotFoundError):
            adapter.memory_feedback({"memory_id": "does-not-exist", "feedback": "POSITIVE"})
        self.assertEqual([], adapter.appended)

    def test_another_tenants_memory_cannot_be_rated(self):
        adapter = _Adapter([_memory()])
        with self.assertRaises(adapter_mod.MatrixArkError):
            adapter.memory_feedback({"memory_id": "777", "feedback": "POSITIVE",
                                     "scope": {"tenant_hash": 99, "user_hash": 22}})
        self.assertEqual([], adapter.appended)


class FeedbackStatusTests(unittest.TestCase):
    def test_a_bad_rating_maps_to_400(self):
        self.assertEqual(400, gateway._classify_backend_error(
            adapter_mod.MatrixArkInvalidRequestError("feedback must be one of ...")))

    def test_an_unknown_memory_maps_to_404(self):
        self.assertEqual(404, gateway._classify_backend_error(
            adapter_mod.MatrixArkNotFoundError("feedback target memory not found")))

    def test_an_ordinary_failure_still_maps_to_500(self):
        self.assertEqual(500, gateway._classify_backend_error(
            adapter_mod.MatrixArkError("the backend fell over")))


if __name__ == "__main__":
    unittest.main()
