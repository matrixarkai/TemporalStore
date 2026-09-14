#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A truncating helper must not GROW as its limit falls.

`summarize_text` cut at `limit - 3` to leave room for an ellipsis. Below a limit of three that
subtraction is negative, and a negative slice keeps everything but the last few characters instead
of keeping none:

    summarize_text("abcdefghij", limit=0)  ->  "abcdefg..."     ten characters for a limit of zero
    summarize_text("abcdefghij", limit=2)  ->  "abcdefghi..."   and it GROWS as the limit falls

It was not reachable, and this file used to say why in a way that was not true: "every call site
passes a literal of 96 or more, and the one caller-supplied budget is floored before it arrives."
Measured over every non-test module and every call site. The figures below are the ones
`TheSmallestLimitAnyCallSitePasses` asserts, so they cannot rot the way the sentence they
replace did:

    smallest literal limit        80, at three sites in matrixark_mcp_core
    calls with a non-literal      2, both `limit=max_chars` in synthesize_context_node_summary
    what max_chars is bound to    220 or 1200, literals at all four of its call sites

There is no floor and no caller-supplied budget: `max_chars` is a parameter that only ever receives
a literal. The conclusion survives -- 80 is still far above the 3 below which the slice inverts --
but it rests on the literals themselves and on nothing that would hold a smaller one back. That is
why `test_the_smallest_limit_any_call_site_passes` below now asserts the 80 IN BOTH DIRECTIONS,
rather than leaving the number in prose where it had already rotted once.

So the fix changed no output any path produces, and that is exactly why a test is worth more than
the fix -- nothing else in the tree would have noticed the behaviour coming back.

WHY THE PROPERTY AND NOT THE CASE. Pinning `summarize_text("abcdefghij", limit=0) == "..."` passes
for any implementation that special-cases zero. The thing that was wrong is monotonicity: a
smaller budget must never buy a longer string. That is checked across every limit from 0 upward,
for both this helper and `preview_text` in matrixark_mcp_resources -- a second copy of the same
function, which already had the guard. The extracted copy got the fix and the original never did,
so the pair is checked together and neither can regress alone.
"""
from __future__ import annotations

import ast
import collections
import functools
import os
import pathlib
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

TOOLS = pathlib.Path(__file__).resolve().parent

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


#: The smallest literal `limit` any non-test call site passes. Recorded because the
#: sentence this file used to carry -- "every call site passes a literal of 96 or more" -- was
#: wrong by three sites and nothing could tell. A number in prose rots silently; this one cannot.
SMALLEST_LITERAL_LIMIT = 80
#: Which modules hold a call at that limit, and how many each. Counted by module rather than
#: pinned by line number: a line number breaks on any unrelated edit above it, and a guard that
#: cries wolf gets weakened rather than re-read.
SMALLEST_LIMIT_SITES = {"matrixark_mcp_core.py": 3}
#: The only calls whose `limit` is not a literal. Both pass the `max_chars` parameter of
#: `synthesize_context_node_summary`, which every one of its own call sites binds to a literal.
NON_LITERAL_LIMIT_SITES = {
    ("matrixark_mcp_core.py", "max_chars"),
}


@functools.lru_cache(maxsize=1)
def _summarize_text_limits():
    """(modules scanned, literal limits, non-literal limit expressions) over non-test modules.

    Read from the SOURCE rather than from a running call, because the claim being pinned is about
    what the call sites are written to pass, which no single execution reveals.

    Cached: four tests below ask the same question, and parsing 286 modules once per test is four
    scans of the tree for one answer that cannot change inside a run.
    """
    literals, non_literal = [], set()
    scanned = 0
    for path in sorted(TOOLS.glob("*.py")):
        if path.name.startswith("test_"):
            continue
        scanned += 1
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        except SyntaxError:  # pragma: no cover
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if (getattr(node.func, "id", "") or getattr(node.func, "attr", "")) != "summarize_text":
                continue
            for keyword in node.keywords:
                if keyword.arg != "limit":
                    continue
                if isinstance(keyword.value, ast.Constant) and isinstance(
                        keyword.value.value, int):
                    literals.append((path.name, node.lineno, keyword.value.value))
                else:
                    non_literal.add((path.name, ast.unparse(keyword.value)))
    return scanned, tuple(literals), frozenset(non_literal)


class TheSmallestLimitAnyCallSitePasses(unittest.TestCase):
    """The recorded fact, asserted so it fails in BOTH directions.

    A table that only fails when a NEW case appears is half a guard: it goes on passing once the
    recorded cases are gone, and then it is decoration. This one fails if a call site starts
    passing something smaller than 80, and equally if the three sites that make 80 the answer
    disappear -- at which point the number in the prose above is stale again and must be re-read.
    """

    def test_the_scan_finds_call_sites_at_all(self):
        """A floor. A renamed helper would make every assertion below vacuously true."""
        scanned, literals, _non_literal = _summarize_text_limits()
        self.assertGreater(scanned, 100,
                           "the scan covered almost no modules -- it is proving nothing")
        self.assertGreater(
            len(literals), 50,
            "only %d summarize_text calls with a literal limit were found; the scan has stopped "
            "matching and this file is asserting nothing" % len(literals))

    def test_no_call_site_passes_a_smaller_limit(self):
        _scanned, literals, _non_literal = _summarize_text_limits()
        smaller = [(name, line, value) for name, line, value in literals
                   if value < SMALLEST_LITERAL_LIMIT]
        self.assertEqual(
            [], smaller,
            "a call site now passes a limit below the recorded %d: %r. The negative-slice guard "
            "bites below 3, so this is not yet a defect -- but the margin stated in the comment on "
            "summarize_text is no longer what it says, and it has to be re-measured rather than "
            "left to rot the way the previous sentence did." % (SMALLEST_LITERAL_LIMIT, smaller))

    def test_the_recorded_smallest_sites_are_still_the_smallest(self):
        """The other direction: the recorded fact must fail when it is RESOLVED, not only broken."""
        _scanned, literals, _non_literal = _summarize_text_limits()
        at_minimum = collections.Counter(
            name for name, _line, value in literals if value == SMALLEST_LITERAL_LIMIT)
        self.assertEqual(
            SMALLEST_LIMIT_SITES, dict(at_minimum),
            "the call sites that make %d the smallest limit have moved or gone. Re-measure and "
            "update both this table and the comment on summarize_text -- a recorded number nobody "
            "re-reads is exactly how the sentence this file replaced became wrong."
            % SMALLEST_LITERAL_LIMIT)

    def test_the_only_non_literal_limits_are_the_recorded_ones(self):
        """`max_chars` is a parameter, not a floor. The prose used to call it a floored budget."""
        _scanned, _literals, non_literal = _summarize_text_limits()
        self.assertEqual(
            NON_LITERAL_LIMIT_SITES, set(non_literal),
            "the set of summarize_text calls with a computed limit has changed. A computed limit "
            "is the only way a value below 3 could reach this helper, so each one has to be read "
            "before the reachability claim above can stand.")

    def test_max_chars_is_only_ever_bound_to_a_literal(self):
        """The half of the old sentence that named a floor which does not exist."""
        bound = []
        for path in sorted(TOOLS.glob("*.py")):
            if path.name.startswith("test_"):
                continue
            try:
                tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
            except SyntaxError:  # pragma: no cover
                continue
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                name = getattr(node.func, "id", "") or getattr(node.func, "attr", "")
                if name != "synthesize_context_node_summary":
                    continue
                for keyword in node.keywords:
                    if keyword.arg == "max_chars":
                        bound.append((path.name, node.lineno, ast.unparse(keyword.value)))
        self.assertGreater(
            len(bound), 0,
            "no synthesize_context_node_summary call sites were found; the scan has stopped "
            "matching")
        computed = [site for site in bound if not site[2].isdigit()]
        self.assertEqual(
            [], computed,
            "max_chars now receives a computed value at %r. It reaches summarize_text as `limit` "
            "with nothing in between, so this is the path along which a value below 3 could "
            "arrive." % (computed,))


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
