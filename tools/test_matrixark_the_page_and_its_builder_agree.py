#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Every function the page and its builder both define is the same function.

`tools/portal/build_portal_pages.py` carries page templates; `tools/portal/setup_portal.html` is
the page a browser runs. A number of JavaScript functions exist in both, byte for byte.

Every harness in this repository runs the PAGE's copy, because that is the one that ships. So an
edit made to only one of the two passes every test there is, and the next generated page quietly
carries the old behaviour. Nothing checked that, and three separate changes have now had to add a
bespoke guard for the one function each of them touched -- `controlHtml`, then `sparkline` and
`renderTrend`, then `fieldHtml`. A written list of function names would need a fourth.

So the pairs are DERIVED: whatever the two files both define, they must define identically.
"""
from __future__ import annotations

import io
import os
import re
import unittest

PORTAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portal")
PAGE = os.path.join(PORTAL, "setup_portal.html")
BUILDER = os.path.join(PORTAL, "build_portal_pages.py")

_DEF = re.compile(r"\bfunction\s+([A-Za-z_$][\w$]*)\s*\(")


def _functions(text: str) -> dict:
    """Every `function name(...) { ... }` in `text`, by name, as a LIST of bodies.

    A list rather than one body, because the builder holds templates for several pages and the
    same function name legitimately appears once per template. The first version of this file
    DROPPED any name defined more than once -- "picking one of several would be inventing the
    question" -- which silently excused exactly those names from the check. `renderSummary` is
    defined twice in the builder, so it was skipped, and a mutation that changed only the
    builder's copy of it survived this guard.

    Dropping them was also invisible: nothing said which names had been skipped, so the guard
    read as covering everything it had parsed.
    """
    found: dict = {}
    for match in _DEF.finditer(text):
        name = match.group(1)
        try:
            start = text.index("{", match.end() - 1)
        except ValueError:  # pragma: no cover - a function with no body
            continue
        depth = 0
        for index in range(start, len(text)):
            if text[index] == "{":
                depth += 1
            elif text[index] == "}":
                depth -= 1
                if depth == 0:
                    found.setdefault(name, []).append(text[match.start():index + 1])
                    break
    return found


def _read(path: str) -> dict:
    with io.open(path, encoding="utf-8") as handle:
        return _functions(handle.read())


def _shared() -> dict:
    """name -> (the page's single definition, every definition the builder holds of that name)."""
    page, builder = _read(PAGE), _read(BUILDER)
    return {name: (page[name], builder[name])
            for name in sorted(set(page) & set(builder))}


class ThePageAndItsBuilderAgreeTest(unittest.TestCase):

    def test_they_share_a_meaningful_number_of_functions(self) -> None:
        # The floor. Every assertion below is over this set, and a reader that finds nothing
        # passes them all: an empty intersection has no disagreement in it.
        shared = _shared()
        self.assertGreater(len(shared), 5, sorted(shared))
        for name in ("controlHtml", "fieldHtml", "renderSummary"):
            self.assertIn(name, shared)

    def test_every_shared_function_is_identical(self) -> None:
        """The page's copy must be one of the builder's copies of that name.

        Not "the builder's copy", because the builder holds a template per page and the same
        name appears once per template. What must hold is that the page a browser runs is one
        the builder can still produce.
        """
        differing = sorted(name for name, (page_bodies, builder_bodies) in _shared().items()
                           if not set(page_bodies) & set(builder_bodies))
        self.assertEqual([], differing,
                         "%d function(s) differ between the shipped page and the builder that "
                         "writes pages; the harnesses only exercise the page" % len(differing))

    def test_a_name_the_builder_defines_twice_is_still_checked(self) -> None:
        """The gap this file used to have. Names defined more than once were dropped, silently,
        so the guard read as covering everything it had parsed while excusing exactly those."""
        builder = _read(BUILDER)
        repeated = sorted(name for name, bodies in builder.items() if len(bodies) > 1)
        self.assertTrue(repeated, "the builder no longer repeats any name; this test is moot")
        covered = [name for name in repeated if name in _shared()]
        self.assertTrue(covered,
                        "every repeated name is being skipped again: %s" % repeated[:5])


class TheReaderWorksTest(unittest.TestCase):
    """The assertion above is an equality against an empty list, which is also what a reader that
    parses nothing produces."""

    def test_it_finds_a_function_and_its_whole_body(self) -> None:
        found = _functions("function a(x) { if (x) { return 1; } return 2; }\n")
        self.assertEqual(["a"], sorted(found))
        self.assertEqual(1, len(found["a"]))
        self.assertTrue(found["a"][0].endswith("return 2; }"), found["a"])

    def test_it_reports_a_difference_when_there_is_one(self) -> None:
        left = _functions("function a() { return 1; }")
        right = _functions("function a() { return 2; }")
        self.assertFalse(set(left["a"]) & set(right["a"]))

    def test_a_name_defined_twice_keeps_both(self) -> None:
        found = _functions("function a() { return 1; }\nfunction a() { return 2; }")
        self.assertIn("a", found)
        self.assertEqual(2, len(found["a"]))

    def test_matching_one_of_several_is_enough(self) -> None:
        """A page whose copy equals the SECOND of the builder's definitions agrees with it."""
        page = _functions("function a() { return 2; }")
        builder = _functions("function a() { return 1; }\nfunction a() { return 2; }")
        self.assertTrue(set(page["a"]) & set(builder["a"]))

    def test_the_real_files_parse_into_something(self) -> None:
        for path in (PAGE, BUILDER):
            self.assertGreater(len(_read(path)), 5, os.path.basename(path))


if __name__ == "__main__":
    unittest.main()
