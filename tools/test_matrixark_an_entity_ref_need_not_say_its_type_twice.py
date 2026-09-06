#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""An entity ref in a context pack said its type twice.

Five call sites rendered a ref as ``f"{entity_type}: {entity_name} = {state}"``, and the name
usually already carries the type. From a live injected pack:

    tool_evidence: tool_evidence = tool: Exit code: 0; Ran 159 tests
    codex_validation: codex_validation:ran_159_tests = tool validation: Ran 159 tests

Measured over 448 entity refs in the log, 195 had ``entity_name == entity_type`` and another 186
had the name prefixed with it -- 381 of 448. Dropping the repeat is 8.8% off their rendered length,
about 1,827 tokens, and each of those is a token the model pays for on the turn the pack lands in.

Only an EXACT match or a ``type + ":"`` prefix counts. ``session`` / ``session_memory`` keeps both,
because that pairing is information rather than a repeat.
"""
import ast
import os
import unittest

try:
    from tools.matrixark_mcp_core import entity_ref_text
except ImportError:  # run from tools/
    from matrixark_mcp_core import entity_ref_text

HERE = os.path.dirname(os.path.abspath(__file__))


class EntityRefTextTests(unittest.TestCase):
    def test_a_name_equal_to_the_type_is_not_repeated(self):
        self.assertEqual(
            "assistant_decision = chose the batch path",
            entity_ref_text({"entity_type": "assistant_decision",
                             "entity_name": "assistant_decision",
                             "state": "chose the batch path"}))

    def test_a_name_prefixed_by_the_type_is_not_repeated(self):
        self.assertEqual(
            "codex_validation:ran_159_tests = tool validation: Ran 159 tests",
            entity_ref_text({"entity_type": "codex_validation",
                             "entity_name": "codex_validation:ran_159_tests",
                             "state": "tool validation: Ran 159 tests"}))

    def test_an_unrelated_name_still_shows_both(self):
        """The pairing is information when the name does not carry the type."""
        self.assertEqual(
            "session: session_memory = active",
            entity_ref_text({"entity_type": "session", "entity_name": "session_memory",
                             "state": "active"}))

    def test_an_underscore_is_not_a_prefix_match(self):
        """Only `type + ':'` counts -- `session_memory` is a different name, not `session` again."""
        out = entity_ref_text({"entity_type": "session", "entity_name": "session_memory",
                               "state": "x"})
        self.assertTrue(out.startswith("session: "))

    def test_nothing_is_lost(self):
        """Whatever the shape, the name and the state must both survive."""
        for record in (
            {"entity_type": "a", "entity_name": "a", "state": "s"},
            {"entity_type": "a", "entity_name": "a:b", "state": "s"},
            {"entity_type": "a", "entity_name": "zzz", "state": "s"},
        ):
            out = entity_ref_text(record)
            self.assertIn(record["entity_name"], out)
            self.assertIn(record["state"], out)

    def test_missing_fields_do_not_raise(self):
        # Unchanged from the original rendering, empty parts and all -- this pins that the
        # helper did not quietly alter the degenerate case while fixing the repeat.
        self.assertEqual(":  = ", entity_ref_text({}))
        self.assertEqual("a:  = ", entity_ref_text({"entity_type": "a"}))
        self.assertEqual(": b = ", entity_ref_text({"entity_name": "b"}))

    def test_every_call_site_uses_the_helper(self):
        """The five sites were identical copies; a sixth must not reappear.

        Counted as ast.Call nodes rather than by grepping, so a mention in a comment or a docstring
        cannot make this pass.
        """
        inline = 0
        calls = 0
        for name in ("matrixark_local_adapter_retrieve.py", "matrixark_local_adapter_ingest.py"):
            path = os.path.join(HERE, name)
            source = open(path, encoding="utf-8").read()
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    fn = node.func
                    if isinstance(fn, ast.Name) and fn.id == "entity_ref_text":
                        calls += 1
                # an f-string still spelling the old rendering
                if isinstance(node, ast.JoinedStr):
                    rendered = "".join(
                        part.value for part in node.values
                        if isinstance(part, ast.Constant) and isinstance(part.value, str))
                    if rendered.startswith(": ") and " = " in rendered:
                        inline += 1
        self.assertEqual(5, calls, "expected all five sites to call the helper")
        self.assertEqual(0, inline, "an inline copy of the old rendering is still there")


if __name__ == "__main__":
    unittest.main()
