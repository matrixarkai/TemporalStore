#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A list that failed to load is not a scope with nothing in it.

Explore loads two lists side by side from one function: the subjects holding memories, and the
memories themselves. When the memory list failed it wrote "Not loaded." and named the cause. When
the subject list failed it ran ``$("users").innerHTML = ""`` -- it blanked the panel and said
nothing at all.

That is worse than an unhelpful message. An empty scope on this page has WORDS ("No subjects hold
memories in this scope yet"), so a blank panel was a third state with nothing in it, and the two
fetches fail independently: with the memory list succeeding the screen contradicted itself, showing
memories listed and no subject holding any.

`innerHTML = ""` and `innerHTML = "<div class=empty>…</div>"` are the same shape of statement, so a
diff cannot tell them apart. The shipped function is run and the DOM is read.
"""
from __future__ import annotations

import io
import os
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
sys.path.insert(0, TOOLS)

HARNESS = os.path.join(PORTAL, "browse_failure_harness.js")
PAGE = os.path.join(PORTAL, "explore_portal.html")


class ThePageSaysWhichHappenedTest(unittest.TestCase):

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def run_mode(self, mode: str) -> str:
        out = subprocess.run(["node", HARNESS, PAGE, mode],
                             capture_output=True, text=True, timeout=600)
        return out.stdout + out.stderr

    def test_a_failed_subject_list_says_so(self) -> None:
        self.assertIn("all ok", self.run_mode("users"), self.run_mode("users"))

    def test_a_failed_memory_list_still_does(self) -> None:
        """The half that was already right; it is the model for the other."""
        self.assertIn("all ok", self.run_mode("memories"))

    def test_both_failing_is_still_explained(self) -> None:
        self.assertIn("all ok", self.run_mode("both"))

    def test_nothing_claims_a_failure_when_both_work(self) -> None:
        """The floor. A panel that always says "Not loaded." would pass every test above."""
        self.assertIn("all ok", self.run_mode("none"))

    def test_an_empty_scope_still_reads_as_empty(self) -> None:
        """The other half of that floor, and the one the other modes cannot reach: they all
        return a row, so a SUCCESSFUL call with nothing in it never gets rendered. Turning the
        empty-state message into "Not loaded." survived until this mode existed."""
        self.assertIn("all ok", self.run_mode("empty"), self.run_mode("empty"))


class TheTwoPanelsAgreeOnHowToFailTest(unittest.TestCase):
    """They are loaded by one function and read side by side, so one going quiet while the other
    explains itself is the inconsistency, not just the silence."""

    @staticmethod
    def _browse() -> str:
        with io.open(PAGE, encoding="utf-8") as handle:
            text = handle.read()
        start = text.find("function loadBrowse() {")
        if start < 0:
            raise AssertionError("loadBrowse is not on the page")
        depth = 0
        for index in range(text.index("{", start), len(text)):
            if text[index] == "{":
                depth += 1
            elif text[index] == "}":
                depth -= 1
                if depth == 0:
                    return text[start:index + 1]
        raise AssertionError("loadBrowse is not closed")

    def test_neither_failure_path_blanks_its_panel(self) -> None:
        body = self._browse()
        self.assertNotIn('innerHTML = ""', body,
                         "a failure path blanks a panel, which reads as an empty result")

    def test_both_failure_paths_name_the_cause(self) -> None:
        """Counted per .catch, not over the whole function: loadBrowse also CLEARS the message
        line on entry, so a total of three mentions is one clear and two failures -- and counting
        mentions would have passed with both of them in the same handler."""
        import re

        # Comments stripped first: the assertion is about what the handler DOES, and a window
        # measured over commentary says nothing about the code in it. Mine filled 400 characters.
        body = re.sub(r"/\*.*?\*/", "", self._browse(), flags=re.S)
        blocks = body.split(".catch(")[1:]
        self.assertEqual(2, len(blocks), "expected one failure handler per list")
        for index, block in enumerate(blocks):
            self.assertIn("browseMsg", block[:300],
                          "failure handler %d does not name the cause" % index)

    def test_the_reader_found_the_function(self) -> None:
        # The floor: the two assertions above are absence checks over this text.
        body = self._browse()
        self.assertGreater(len(body), 600)
        self.assertIn("/v1/users", body)
        self.assertIn("/v1/memories", body)


if __name__ == "__main__":
    unittest.main()
