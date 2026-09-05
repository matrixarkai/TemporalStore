#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Every panel has the shared stylesheet, not the version it was given once.

Five pages are generated and get ``NAV_CSS`` as part of their stylesheet every time. The other two
are hand-maintained, and ``inject()`` adds the shared nav to them. That function has two paths: the
first time, with no nav present, it inserts the markup, the JS *and* the CSS; every run after that
takes the refresh path, which replaced the markup and the JS and never touched the CSS.

So their copy was frozen at whatever ``NAV_CSS`` said on the day the nav was first added.
``NAV_CSS`` defines twelve class names. The key portal had two of them and the ingestion page four
-- everything added since was missing from both, including the whole live status strip
(``.livestrip``, ``.live-seg``, ``.live-dot``, ``.stale``, ``.busy``, ``.down``), the status chip,
the subhead, and the ``.sr-only`` rule whose entire job is keeping a screen-reader label off the
screen.

The visible half: the status strip whose behaviour two recent changes went to some trouble to get
right rendered completely unstyled on two of the seven panels, and a label meant to be invisible
would have been drawn in the middle of the page.

The check is derived from ``NAV_CSS`` rather than listing what to look for, because the failure was
precisely that something new was added to it and did not travel. A list would have been written
against what existed at the time and gone stale the same way.
"""
from __future__ import annotations

import io
import os
import re
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
BUILDER = os.path.join(PORTAL, "build_portal_pages.py")


def read(path: str) -> str:
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def shared_classes() -> set:
    """The class names the shared block defines, read out of the generator."""
    source = read(BUILDER)
    match = re.search(r'^NAV_CSS\s*=\s*"""', source, re.M)
    assert match, "NAV_CSS is no longer a plain triple-quoted constant"
    block = source[match.end():source.index('"""', match.end())]
    return set(re.findall(r"\.([A-Za-z][\w-]*)\s*[{,:]", block))


def stylesheet(text: str) -> str:
    return "\n".join(re.findall(r"<style>([\s\S]*?)</style>", text))


def pages() -> dict:
    return {name: read(os.path.join(PORTAL, name))
            for name in sorted(os.listdir(PORTAL)) if name.endswith(".html")}


class EveryPanelHasTheSharedStylesheetTest(unittest.TestCase):

    def setUp(self) -> None:
        self.shared = shared_classes()
        self.pages = pages()

    def test_the_shared_block_defines_something(self) -> None:
        """Derived, so a constant that stopped parsing would leave every check below vacuous."""
        self.assertGreaterEqual(len(self.shared), 8, sorted(self.shared))

    def test_it_covers_the_live_strip(self) -> None:
        """The part that was missing, named so a future edit cannot quietly drop it from the
        shared block and satisfy the sweep by shrinking what is being swept."""
        for name in ("livestrip", "live-seg", "live-dot"):
            self.assertIn(name, self.shared)

    def test_every_page_defines_every_shared_class(self) -> None:
        missing = {}
        for name, text in self.pages.items():
            css = stylesheet(text)
            absent = sorted(cls for cls in self.shared if ("." + cls) not in css)
            if absent:
                missing[name] = absent
        self.assertEqual({}, missing,
                         "panels whose stylesheet is behind the shared one: %r" % missing)

    def test_both_hand_maintained_pages_are_in_the_sweep(self) -> None:
        """They are the two the refresh path skipped, so a sweep that stopped seeing them would
        pass while they went stale again."""
        for name in ("api_key_portal.html", "ingestion_portal.html"):
            self.assertIn(name, self.pages)


class TheBlockCanBeFoundAgainTest(unittest.TestCase):
    """It could not be, which is why it was written once and never updated."""

    def test_the_hand_maintained_pages_carry_the_markers(self) -> None:
        source = read(BUILDER)
        start = re.search(r'NAV_CSS_START\s*=\s*"([^"]+)"', source).group(1)
        end = re.search(r'NAV_CSS_END\s*=\s*"([^"]+)"', source).group(1)
        for name in ("api_key_portal.html", "ingestion_portal.html"):
            with self.subTest(page=name):
                text = read(os.path.join(PORTAL, name))
                self.assertIn(start, text)
                self.assertIn(end, text)

    def test_the_refresh_path_writes_the_css(self) -> None:
        """The whole defect in one line: the refresh path handled the markup and the scripts and
        left the stylesheet alone."""
        source = read(BUILDER)
        refresh = source[source.index('if "portalnav" in text:'):]
        refresh = refresh[:refresh.index("print(\"nav refreshed")]
        self.assertIn("_with_nav_css", refresh)


if __name__ == "__main__":
    unittest.main()
