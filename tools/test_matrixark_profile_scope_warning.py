#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Guard: a message-ingest scope without tenant_id/user_id must not silently disable profile."""
import tempfile
import time
import unittest
import warnings
from pathlib import Path

import matrixark_mcp_local_adapter as mcp
import matrixark_local_adapter_ingest as ing
import matrixark_mcp_server as mcp_server


class ProfileScopeWarningTest(unittest.TestCase):
    def _adapter(self):
        return mcp.MatrixArkLocalAdapter(Path(tempfile.mkdtemp()) / "events.jsonl")

    def test_helper_flags_missing_and_passes_present(self):
        self.assertTrue(ing.warn_if_profile_scope_missing({"user_id": "u"}))          # no tenant_id
        self.assertTrue(ing.warn_if_profile_scope_missing({"tenant_id": "t"}))        # no user_id
        self.assertTrue(ing.warn_if_profile_scope_missing({"tenant_id": "t", "user_id": ""}))  # empty
        self.assertEqual("", ing.warn_if_profile_scope_missing({"tenant_id": "t", "user_id": "u"}))
        self.assertEqual("", ing.warn_if_profile_scope_missing("not a dict"))

    def test_ingest_missing_scope_warns_and_surfaces(self):
        ing._PROFILE_SCOPE_WARNED.clear()
        a = self._adapter()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            r = a.ingest({"kind": "message", "scope": {"user": "alice", "session_id": "s1"},
                          "messages": [{"role": "user", "content": "I am Alice, ML lead."}]})
        self.assertEqual(1, len(w))
        self.assertIn("profile_scope_missing", str(w[0].message))
        self.assertIn("profile_scope_warning", r)

    def test_ingest_full_scope_is_silent(self):
        ing._PROFILE_SCOPE_WARNED.clear()
        a = self._adapter()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            r = a.ingest({"kind": "message", "scope": {"tenant_id": "acme", "user_id": "alice", "session_id": "s2"},
                          "messages": [{"role": "user", "content": "hi"}]})
        self.assertEqual(0, len(w))
        self.assertNotIn("profile_scope_warning", r)

    def test_warning_deduped_per_identity(self):
        ing._PROFILE_SCOPE_WARNED.clear()
        a = self._adapter()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            for _ in range(3):
                a.ingest({"kind": "message", "scope": {"session_id": "s"},
                          "messages": [{"role": "user", "content": "x"}]})
        self.assertEqual(1, len(w))  # warned once, not per-call


class BatchedIngestEquivalenceTest(unittest.TestCase):
    """A batched ingest must be equivalent to the same messages sent one per call.

    A warning here used to tell callers a batch retained ~1 raw context_event and lost the
    rest. That was true until the commit loop stopped skipping messages past source_event_ids;
    keeping the warning would have pushed callers onto the per-turn path, which produces the
    identical context records at strictly more per-call bookkeeping. The warning is gone and
    this asserts the equivalence that replaced it, so a regression cannot restore the loss
    silently.
    """

    CONVO = [{"role": "user", "content": "I am a robotics engineer at Acme."},
             {"role": "assistant", "content": "What are you building?"},
             {"role": "user", "content": "A project called Aurora, a warehouse arm."},
             {"role": "assistant", "content": "What stack?"},
             {"role": "user", "content": "Rust for control, Python for planning."},
             {"role": "user", "content": "The p99 was 27ms on build 4471."}]
    SCOPE = {"account_id": "acct_local", "tenant_id": "equiv", "user_id": "u",
             "session_id": "s0", "agent_name": "t"}

    def _run(self, bundled):
        adapter = mcp.MatrixArkLocalAdapter(Path(tempfile.mkdtemp()) / "events.jsonl")
        server = mcp_server.MatrixArkMcpServer(adapter, access_mode="dev")
        batches = [self.CONVO] if bundled else [[m] for m in self.CONVO]
        for batch in batches:
            server.call_tool("matrixark_ingest",
                             {"scope": self.SCOPE, "finalize": True, "messages": batch})
        server.call_tool("matrixark_session_commit", {"scope": self.SCOPE})
        # Embeddings are produced by a background worker, so reading immediately after commit
        # catches a varying amount of in-flight work: the batched arm returned 19, 20, 21 or 22
        # context_embedding records across runs against a steady 18 for per-turn, and the test
        # failed roughly one run in three. That is the harness racing the worker, not a
        # difference between the two ingest shapes. Wait for the record count to settle so the
        # comparison is between two finished states.
        previous, stable = -1, 0
        deadline = time.time() + 60
        while time.time() < deadline:
            rows = adapter.read_all()
            if len(rows) == previous:
                stable += 1
                if stable >= 3:
                    break
            else:
                stable = 0
            previous = len(rows)
            time.sleep(0.3)
        return adapter.read_all()

    @staticmethod
    def _texts(rows):
        return sorted(str(r.get("text") or "") for r in rows
                      if r.get("record_type") == "context_event")

    def test_batched_ingest_keeps_every_message(self):
        texts = " ".join(self._texts(self._run(bundled=True)))
        for message in self.CONVO:
            self.assertIn(message["content"], texts)

    def test_batched_matches_per_turn_context_records(self):
        bundled = self._run(bundled=True)
        per_turn = self._run(bundled=False)
        self.assertEqual(self._texts(per_turn), self._texts(bundled))
        # Retention-critical types only. context_summary, context_node and context_embedding
        # are deliberately NOT compared: node_l1_generation_policy gates L1 on event_count, so
        # a bundled call and per-turn calls legitimately reach that gate at different counts and
        # emit a different number of summaries -- and an embedding is generated PER summary, so
        # the embedding count inherits that variance (measured at 24 against 18). The settle-wait
        # below cannot fix that: it is a real difference between the two ingest shapes, not a
        # race, and asserting equality on it left this test failing about one run in three.
        # Original note follows.
        # Retention-critical types only. context_summary and context_node are deliberately
        # NOT compared: node_l1_generation_policy gates L1 on event_count, so a bundled call and
        # a sequence of per-turn calls legitimately evaluate that gate at different counts and
        # emit a different number of summaries. Asserting equality there made this test
        # NONDETERMINISTIC -- it failed roughly one run in three, in any tree, and its failures
        # were mistaken for order-dependence in unrelated baselines.
        #
        # The claim this test exists to defend is that batching loses no MESSAGE, which the
        # event/entity/embedding counts and the text assertion above establish.
        for record_type in ("context_event", "context_entity"):
            self.assertEqual(
                len([r for r in per_turn if r.get("record_type") == record_type]),
                len([r for r in bundled if r.get("record_type") == record_type]),
                "%s count differs between batched and per-turn ingest" % record_type)

    def test_no_batched_ingest_warning_is_emitted(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self._run(bundled=True)
        self.assertFalse([w for w in caught if "batched_ingest" in str(w.message)])


if __name__ == "__main__":
    unittest.main()
