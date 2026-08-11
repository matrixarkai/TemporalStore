#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Tests for conditional follow-up query rewriting."""
import unittest

import matrixark_query_rewrite as R


class FollowupDetectionTest(unittest.TestCase):
    def test_anaphora_is_followup(self):
        self.assertTrue(R.is_followup_query("How are tool events handled during that?"))
        self.assertTrue(R.is_followup_query("Why did that matter for token savings?"))
        self.assertTrue(R.is_followup_query("What was the fix for the ones being skipped?"))

    def test_continuation_is_followup(self):
        self.assertTrue(R.is_followup_query("And how does the session buffer fit into it?"))
        self.assertTrue(R.is_followup_query("So why is that slow?"))

    def test_thread_verb_is_followup(self):
        self.assertTrue(R.is_followup_query("Summarize the whole decision for me."))
        self.assertTrue(R.is_followup_query("recap that"))

    def test_standalone_is_not_followup(self):
        self.assertFalse(R.is_followup_query("What did the 3-arm token and quality benchmark show?"))
        self.assertFalse(R.is_followup_query("How should Windows users install TemporalStore with Docker?"))
        self.assertFalse(R.is_followup_query("Explain the storage backend resolution order."))

    def test_empty(self):
        self.assertFalse(R.is_followup_query(""))
        self.assertFalse(R.is_followup_query("   "))


class RewriteTest(unittest.TestCase):
    def test_folds_prior_turns(self):
        rq = R.rewrite_query("during that?", ["What did we decide about hooks?", "How are tool events handled?"], window=2)
        self.assertIn("hooks", rq)
        self.assertIn("tool events", rq)
        self.assertTrue(rq.endswith("during that?"))

    def test_window_limits_prior(self):
        rq = R.rewrite_query("q", ["a", "b", "c", "d"], window=2)
        self.assertTrue(rq.startswith("c d"))

    def test_no_prior_returns_query(self):
        self.assertEqual("q", R.rewrite_query("q", []))


class ConditionalTest(unittest.TestCase):
    priors = ["What did we decide about hooks?", "How are tool events handled?"]

    def test_standalone_not_rewritten(self):
        q = "What did the benchmark show?"
        out, rewritten, reason = R.conditional_retrieval_query(q, self.priors)
        self.assertEqual(q, out)
        self.assertFalse(rewritten)
        self.assertEqual("standalone", reason)

    def test_followup_rewritten(self):
        out, rewritten, reason = R.conditional_retrieval_query("why did that matter?", self.priors)
        self.assertTrue(rewritten)
        self.assertEqual("followup_rewritten", reason)
        self.assertIn("hooks", out)
        self.assertTrue(out.endswith("why did that matter?"))

    def test_followup_without_prior_not_rewritten(self):
        out, rewritten, reason = R.conditional_retrieval_query("why did that matter?", [])
        self.assertFalse(rewritten)
        self.assertEqual("no_prior_context", reason)

    def test_disabled(self):
        out, rewritten, reason = R.conditional_retrieval_query("why did that matter?", self.priors, enabled=False)
        self.assertFalse(rewritten)
        self.assertEqual("disabled", reason)


if __name__ == "__main__":
    unittest.main()
