#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A retrieve that raises is recorded, not fatal.

Retrieve used to be left unwrapped on the reasoning that a failed read IS the failure. That held
only while there was nothing better to serve. Now the hook keeps the last pack that loaded, so a
raise that kills main() throws away a usable answer -- and, worse, makes a LOUD policy error
(native ContextPack required but not produced) end in exactly the same `{}` as a silent one.

These tests pin the two halves that matter: the turn survives, and the reason is not swallowed.
"""
import unittest

import matrixark_agent_hook as hook


class RetrieveFailureIsSurvivableTest(unittest.TestCase):
    def setUp(self):
        self._real_call_tool = hook.call_tool

    def tearDown(self):
        hook.call_tool = self._real_call_tool

    def test_a_raising_retrieve_returns_empty_and_is_recorded(self):
        def boom(server, tool, tool_args):
            raise RuntimeError(
                "backend-native ContextPack assembly is required for TemporalStore serving"
            )

        hook.call_tool = boom
        failures: list = []
        result = hook.call_retrieve_tool(None, "matrixark_retrieve", {}, failures=failures)

        self.assertEqual(result, {}, "a failed retrieve must return {} so the turn can continue")
        self.assertEqual(len(failures), 1, "the failure must be recorded, not swallowed")
        self.assertEqual(failures[0]["tool"], "matrixark_retrieve")
        self.assertIn(
            "ContextPack assembly is required",
            failures[0]["error"],
            "the recorded error must carry the REASON -- a policy error is the case an operator "
            "most needs told rather than left to infer",
        )
        self.assertIn("RuntimeError", failures[0]["error"], "the error type must survive too")

    def test_a_successful_retrieve_passes_straight_through(self):
        """The control: without this, returning {} unconditionally would pass the test above."""
        pack = {"context_pack_id": "rust-native-123-4", "groups": [{"items": [{"text": "hi"}]}]}
        hook.call_tool = lambda server, tool, tool_args: pack
        failures: list = []

        self.assertEqual(
            hook.call_retrieve_tool(None, "matrixark_retrieve", {}, failures=failures), pack
        )
        self.assertEqual(failures, [], "a retrieve that worked must record no failure")

    def test_the_recorded_error_is_bounded(self):
        """An adapter error can carry a whole request; the report must not become the payload."""
        hook.call_tool = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x" * 5000))
        failures: list = []
        hook.call_retrieve_tool(None, "matrixark_retrieve", {}, failures=failures)
        self.assertLessEqual(len(failures[0]["error"]), 500)


if __name__ == "__main__":
    unittest.main()
