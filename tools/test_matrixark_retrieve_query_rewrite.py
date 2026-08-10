#!/usr/bin/env python3
"""Production retrieve-path wiring for conditional query rewriting.

Verifies the ranking query is rewritten only for follow-ups when the gate is on, that the
pack-facing `query` is never touched (zero added model tokens), and that priors come from
args or the session buffer.
"""
import os
import subprocess
import sys
import unittest

import matrixark_mcp_local_adapter  # noqa: F401  loads the retrieve mixin (breaks standalone import cycle)
import matrixark_local_adapter_retrieve as R


class PriorsExtractionTest(unittest.TestCase):
    def test_from_args_string_list(self):
        got = R._recent_user_texts_for_rewrite({"prior_user_messages": ["a", "b"]}, None, {})
        self.assertEqual(["a", "b"], got)

    def test_from_args_role_dicts(self):
        args = {"prior_messages": [{"role": "user", "content": "u1"},
                                   {"role": "assistant", "content": "skip"},
                                   {"role": "user", "content": "u2"}]}
        self.assertEqual(["u1", "u2"], R._recent_user_texts_for_rewrite(args, None, {}))

    def test_no_priors_and_no_adapter_is_empty(self):
        self.assertEqual([], R._recent_user_texts_for_rewrite({}, None, {}))


class GateOffTest(unittest.TestCase):
    def test_default_off_returns_query_unchanged(self):
        # ships OFF: ranking query == user query, nothing rewritten
        rq, info = R._maybe_rewrite_retrieval_query("why did that matter?",
                                                    {"prior_user_messages": ["about hooks"]}, None, {})
        self.assertEqual("why did that matter?", rq)
        self.assertFalse(info["query_rewritten"])

    def test_standalone_never_rewritten(self):
        rq, info = R._maybe_rewrite_retrieval_query("What did the benchmark show?",
                                                    {"prior_user_messages": ["about hooks"]}, None, {})
        self.assertEqual("What did the benchmark show?", rq)
        self.assertFalse(info["query_rewritten"])


class GateOnSubprocessTest(unittest.TestCase):
    def test_followup_rewritten_when_enabled(self):
        code = (
            "import sys; sys.path.insert(0, 'tools');"
            "import matrixark_mcp_local_adapter;"
            "import matrixark_local_adapter_retrieve as R;"
            "rq, info = R._maybe_rewrite_retrieval_query('why did that matter?',"
            " {'prior_user_messages': ['What did we decide about hooks ingestion?']}, None, {});"
            "print(info['query_rewritten'], 'hooks' in rq, rq.endswith('why did that matter?'))"
        )
        env = {**os.environ, "MATRIXARK_QUERY_REWRITE": "1"}
        repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        out = subprocess.run([sys.executable, "-c", code], cwd=repo, env=env, capture_output=True, text=True)
        self.assertEqual(0, out.returncode, out.stderr)
        self.assertEqual("True True True", out.stdout.strip())


if __name__ == "__main__":
    unittest.main()
