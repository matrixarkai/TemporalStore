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
    """Every `function name(...) { ... }` in `text`, by name, brace-matched.

    A name defined more than once is dropped rather than guessed at: this file is about whether
    two definitions agree, and picking one of several would be inventing the question.
    """
    found: dict = {}
    duplicates = set()
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
                    body = text[match.start():index + 1]
                    if name in found and found[name] != body:
                        duplicates.add(name)
                    found[name] = body
                    break
    for name in duplicates:
        found.pop(name, None)
    return found


def _read(path: str) -> dict:
    with io.open(path, encoding="utf-8") as handle:
        return _functions(handle.read())


def _shared() -> dict:
    page, builder = _read(PAGE), _read(BUILDER)
    return {name: (page[name], builder[name]) for name in sorted(set(page) & set(builder))}


class ThePageAndItsBuilderAgreeTest(unittest.TestCase):

    def test_they_share_a_meaningful_number_of_functions(self) -> None:
        # The floor. Every assertion below is over this set, and a reader that finds nothing
        # passes them all: an empty intersection has no disagreement in it.
        shared = _shared()
        self.assertGreater(len(shared), 5, sorted(shared))
        for name in ("controlHtml", "fieldHtml"):
            self.assertIn(name, shared)

    def test_every_shared_function_is_identical(self) -> None:
        differing = sorted(name for name, (page, builder) in _shared().items()
                           if page != builder)
        self.assertEqual([], differing,
                         "%d function(s) differ between the shipped page and the builder that "
                         "writes pages; the harnesses only exercise the page" % len(differing))


class TheReaderWorksTest(unittest.TestCase):
    """The assertion above is an equality against an empty list, which is also what a reader that
    parses nothing produces."""

    def test_it_finds_a_function_and_its_whole_body(self) -> None:
        found = _functions("function a(x) { if (x) { return 1; } return 2; }\n")
        self.assertEqual(["a"], sorted(found))
        self.assertTrue(found["a"].endswith("return 2; }"), found["a"])

    def test_it_reports_a_difference_when_there_is_one(self) -> None:
        left = _functions("function a() { return 1; }")
        right = _functions("function a() { return 2; }")
        self.assertNotEqual(left["a"], right["a"])

    def test_a_name_defined_twice_is_left_out_rather_than_guessed(self) -> None:
        found = _functions("function a() { return 1; }\nfunction a() { return 2; }")
        self.assertNotIn("a", found)

    def test_the_real_files_parse_into_something(self) -> None:
        for path in (PAGE, BUILDER):
            self.assertGreater(len(_read(path)), 5, os.path.basename(path))


if __name__ == "__main__":
    unittest.main()
