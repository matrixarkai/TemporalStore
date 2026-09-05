#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Every message severity the portal can ask for has a rule to draw it.

Each page defines ``.msg.err``, ``.msg.ok`` and ``.msg.info``. None defined ``.msg.warn`` -- and
five places across three panels asked for it:

* the key portal's *"Nothing is being recorded ... an empty list below is not evidence that nothing
  happened"*, which is the whole point of the audit panel;
* Explore's *"Type <op> in the confirm box to run this"*, twice -- the gate in front of a
  destructive operation;
* Setup's *"Type a model name first"* and *"That field is not loaded"*.

``say()`` renders ``class="msg " + cls``, so those matched only the base ``.msg`` rule and were
drawn exactly like a neutral note. The severity existed in the code and not in the stylesheet.

The check is derived from the calls rather than from a list of severities: the failure was a
severity being used that nobody had styled, so a list would have been written from the stylesheet
and agreed with it. Every literal severity passed to ``say()`` or ``showMsg()`` on a page must have
a rule on that page.
"""
from __future__ import annotations

import io
import os
import re
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")

# Severities are the third argument: say(el, text, "warn") / showMsg(id, "err", text).
# Parentheses are allowed inside. The first argument is almost always `$("someId")`, so a class
# that excluded them matched none of the real calls -- which read as "only one page ever warns",
# and would have quietly narrowed this sweep to the single page that happens to use the markup form.
SAY = re.compile(r'\bsay\([^;\n]*?,\s*"(\w+)"\s*\)')
SHOW = re.compile(r'\bshowMsg\(\s*"[^"]*"\s*,\s*"(\w+)"')
MARKUP = re.compile(r'class="msg (\w+)"')


def pages() -> dict:
    return {name: io.open(os.path.join(PORTAL, name), encoding="utf-8").read()
            for name in sorted(os.listdir(PORTAL)) if name.endswith(".html")}


def stylesheet(text: str) -> str:
    return "\n".join(re.findall(r"<style>([\s\S]*?)</style>", text))


def severities_used(text: str) -> set:
    """Every severity this page asks for, by literal.

    A severity passed as a variable cannot be resolved without running the page; the literals are
    the ones a stylesheet can be checked against, and they are where this went wrong.
    """
    found = set(SAY.findall(text)) | set(SHOW.findall(text)) | set(MARKUP.findall(text))
    # say(el, "") clears the box, and the helper substitutes "info" for a missing class.
    return {s for s in found if s and s != "show"}


class EverySeverityHasARuleTest(unittest.TestCase):

    def setUp(self) -> None:
        self.pages = pages()
        self.assertGreaterEqual(len(self.pages), 7)

    def test_the_scan_finds_severities(self) -> None:
        """A pattern that stopped matching would report every page as consistent."""
        total = sum(len(severities_used(text)) for text in self.pages.values())
        self.assertGreaterEqual(total, 10, total)

    def test_warn_is_one_of_them(self) -> None:
        """Named because it is the one that was missing: a future edit that stopped asking for it
        would satisfy the sweep by shrinking what is swept."""
        asking = sorted(name for name, text in self.pages.items()
                        if "warn" in severities_used(text))
        self.assertGreaterEqual(len(asking), 3, asking)

    def test_every_severity_asked_for_is_defined_on_that_page(self) -> None:
        missing = {}
        for name, text in self.pages.items():
            css = stylesheet(text)
            absent = sorted(s for s in severities_used(text) if (".msg.%s{" % s) not in css)
            if absent:
                missing[name] = absent
        self.assertEqual({}, missing,
                         "severities asked for and never styled, so they draw as a neutral note: "
                         "%r" % missing)

    def test_the_warning_colour_is_the_warning_colour(self) -> None:
        """Defined in terms of the variables the rest of the portal warns with, so a theme change
        moves it too -- and so it is visibly not the error colour."""
        for name, text in self.pages.items():
            css = stylesheet(text)
            rule = re.search(r"\.msg\.warn\{([^}]*)\}", css)
            with self.subTest(page=name):
                self.assertIsNotNone(rule, "no .msg.warn rule")
                self.assertIn("--warn", rule.group(1))
                self.assertNotIn("--crit", rule.group(1))


if __name__ == "__main__":
    unittest.main()
