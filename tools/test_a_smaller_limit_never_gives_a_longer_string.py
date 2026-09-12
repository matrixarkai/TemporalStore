#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A truncating helper must not GROW as its limit falls.

`summarize_text` cut at `limit - 3` to leave room for an ellipsis. Below a limit of three that
subtraction is negative, and a negative slice keeps everything but the last few characters instead
of keeping none:

    summarize_text("abcdefghij", limit=0)  ->  "abcdefg..."     ten characters for a limit of zero
    summarize_text("abcdefghij", limit=2)  ->  "abcdefghi..."   and it GROWS as the limit falls

It was not reachable: every call site passes a literal of 96 or more, and the one caller-supplied
budget is floored before it arrives. So the fix changed no output any path produces, and that is
exactly why a test is worth more than the fix -- nothing else in the tree would have noticed the
behaviour coming back.

WHY THE PROPERTY AND NOT THE CASE. Pinning `summarize_text("abcdefghij", limit=0) == "..."` passes
for any implementation that special-cases zero. The thing that was wrong is monotonicity: a
smaller budget must never buy a longer string. That is checked across every limit from 0 upward,
for both this helper and `preview_text` in matrixark_mcp_resources -- a second copy of the same
function, which already had the guard. The extracted copy got the fix and the original never did,
so the pair is checked together and neither can regress alone.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:  # package path
    from tools.matrixark_mcp_core import summarize_text
    from tools.matrixark_mcp_resources import preview_text
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import summarize_text
    from matrixark_mcp_resources import preview_text

HELPERS = (("summarize_text", summarize_text), ("preview_text", preview_text))

SAMPLES = (
    "abcdefghij",
    "The deployment was blocked by a failing migration and the owner rolled it back.",
    "one two three four five six seven eight nine ten eleven twelve thirteen",
    "a" * 300,
    "   leading and trailing   ",
    "",
)


class ASmallerLimitNeverGivesALongerStringTest(unittest.TestCase):

    def test_the_output_never_grows_as_the_limit_falls(self) -> None:
        for name, helper in HELPERS:
            for text in SAMPLES:
                previous = None
                for limit in range(0, 40):
                    out = helper(text, limit=limit)
                    if previous is not None:
                        with self.subTest(helper=name, text=text[:20], limit=limit):
                            self.assertLessEqual(
                                len(previous), len(out),
                                "%s(limit=%d) is %d characters and (limit=%d) is %d -- a smaller "
                                "budget bought a longer string, which is the negative-slice bug"
                                % (name, limit - 1, len(previous), limit, len(out)))
                    previous = out

    def test_a_limit_below_the_ellipsis_yields_only_the_ellipsis(self) -> None:
        """The specific shape of the old bug, kept as well as the property.

        The property above would also hold for an implementation that returned the whole string at
        every small limit. It must not: a caller asking for two characters has asked for fewer
        than the ellipsis costs, and the honest answer is the ellipsis, not the input.
        """
        for name, helper in HELPERS:
            for limit in (0, 1, 2, 3):
                with self.subTest(helper=name, limit=limit):
                    self.assertEqual(
                        "...", helper("abcdefghij", limit=limit),
                        "%s(limit=%d) should be the bare ellipsis" % (name, limit))

    def test_a_limit_that_fits_returns_the_text_unchanged(self) -> None:
        """The positive control. Both checks above pass for a helper that returns "..." always."""
        for name, helper in HELPERS:
            with self.subTest(helper=name):
                self.assertEqual("abcdefghij", helper("abcdefghij", limit=220))
                self.assertEqual("a b c", helper("a  b\t\tc", limit=220),
                                 "%s should collapse whitespace runs" % name)


if __name__ == "__main__":
    unittest.main()
