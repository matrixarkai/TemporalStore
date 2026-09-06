#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The last-good-pack fallback is for a turn the store did not answer.

Emitting ``{}`` when the store could not answer tells the agent it has no history -- wrong, and
silent -- so a turn that cannot build a pack serves the previous one, labelled in band. The
condition it fired on was "nothing was rendered", and that is a different question.

A **heartbeat** turn renders nothing on purpose: the retrieved pack contains only the hook's own
heartbeat line, which is filtered out, so nothing is left to show. There was an answer; it had
nothing in it. Serving the previous pack there injects stale context into a turn that deliberately
has none -- the opposite of the fallback's reason for existing.

The rule now lives in the module both hooks already share, and both ask it. A fallback added to one
entry point and not the other is a shape this codebase has been bitten by before: Codex runs a
separate entry point from Claude, which is why the fallback had to be added twice in the first
place.
"""
from __future__ import annotations

import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

try:
    from tools import matrixark_hook_pack_cache as pack_cache  # type: ignore
except ImportError:
    import matrixark_hook_pack_cache as pack_cache  # type: ignore


class TheQuestionIsWhetherTheStoreAnsweredTest(unittest.TestCase):
    """Not whether anything was rendered. Those differ exactly where this went wrong."""

    def test_a_pack_that_rendered_to_nothing_still_counts_as_an_answer(self) -> None:
        """The heartbeat case: a pack came back and its only line was filtered out."""
        self.assertTrue(pack_cache.store_answered(
            {"pack_id": "pack-heartbeat-only", "context": "user: hook heartbeat"}))

    def test_a_pack_with_refs_counts(self) -> None:
        self.assertTrue(pack_cache.store_answered({"refs": [{"ref": "x"}]}))
        self.assertTrue(pack_cache.store_answered({"context_pack_id": "pack-1"}))

    def test_no_answer_is_no_answer(self) -> None:
        for retrieve in ({}, None, "", {"pack_id": ""}, {"context": ""}):
            with self.subTest(retrieve=retrieve):
                self.assertFalse(pack_cache.store_answered(retrieve))

    def test_a_timed_out_tool_call_is_not_an_answer(self) -> None:
        """The tell the hooks already use: a pack that arrived after the deadline is not a pack."""
        self.assertFalse(pack_cache.store_answered(
            {"pack_id": "pack-1", "context": "something", "_hook_tool_timeout": True}))

    def test_a_shape_it_does_not_understand_is_not_an_answer(self) -> None:
        """Defaulting to "answered" would silently disable the fallback; defaulting to "not
        answered" only serves a stale pack, which is the smaller error and the one this whole
        mechanism already chose."""
        self.assertFalse(pack_cache.store_answered(["refs"]))
        self.assertFalse(pack_cache.store_answered(42))


class BothHooksAskItTest(unittest.TestCase):
    """Codex runs a separate entry point from Claude. The fallback had to be added twice; the
    condition on it has to be right twice."""

    def _source(self, filename: str) -> str:
        with open(os.path.join(TOOLS, filename), encoding="utf-8") as handle:
            return handle.read()

    def test_the_codex_hook_asks(self) -> None:
        self.assertIn("store_answered(retrieve)", self._source("matrixark_codex_hook.py"))

    def test_the_claude_hook_asks(self) -> None:
        self.assertIn("store_answered(retrieve)", self._source("matrixark_agent_hook.py"))

    def test_neither_decides_it_alone(self) -> None:
        """One rule. A second copy is how the two entry points drift, which is the reason this
        module exists at all."""
        for filename in ("matrixark_codex_hook.py", "matrixark_agent_hook.py"):
            with self.subTest(hook=filename):
                self.assertNotIn("def store_answered", self._source(filename))


class WhereEachSideIsCoveredTest(unittest.TestCase):
    """Said out loud, because the asymmetry is the reason this was visible on one side only.

    The Codex hook builds its output in a function a test can call, so the heartbeat case has a real
    end-to-end test there -- it is the test that went red when the fallback landed. The Claude hook
    builds the same output inside ``main()``, which reads stdin, parses arguments and calls the
    backend; there is no seam to drive, so the same defect on that side showed nothing. That is the
    whole reason the rule is asserted directly and both call sites are pinned, rather than trusting
    one end-to-end test to cover a decision made in two places.
    """

    def test_the_codex_side_has_the_end_to_end_case(self) -> None:
        with open(os.path.join(TOOLS, "test_codex_hook_output_part2.py"), encoding="utf-8") as f:
            self.assertIn("test_heartbeat_only_rendered_context_does_not_emit_additional_context",
                          f.read())

    def test_the_claude_side_fallback_is_still_inside_main(self) -> None:
        """If it ever gains a seam, this fails -- and the end-to-end test that could not be written
        becomes writable. That is a prompt, not a rule against refactoring."""
        with open(os.path.join(TOOLS, "matrixark_agent_hook.py"), encoding="utf-8") as f:
            lines = f.read().splitlines()
        fallback = next(i for i, line in enumerate(lines)
                        if "store_answered(retrieve)" in line)
        enclosing = [line for line in lines[:fallback] if line.startswith("def ")][-1]
        self.assertEqual("def main() -> int:", enclosing,
                         "the Claude-side fallback moved out of main(); an end-to-end heartbeat "
                         "test is now possible and should replace this note")


if __name__ == "__main__":
    unittest.main()
