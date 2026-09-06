#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The pages that ship are byte for byte what the builder produces.

`test_matrixark_the_page_and_its_builder_agree` compares FUNCTIONS the page and the builder both
define, which is the check that caught a mutation of `renderSummary`. It cannot see anything
outside a function: a changed HTML body, a rule added to the stylesheet, a nav that moved. The
builder is cheap to run, so this runs it and diffs.

It runs in a COPY. `build_portal_pages.py` writes its output in place and edits two more pages
that it does not generate, so running it against the working tree to find out whether the working
tree is current would be the mechanism destroying the evidence -- a settings test in this
repository has twice now written to a live configuration for the same shape of reason.

**Determinism is the floor.** A builder that embeds a timestamp would fail this on every run and
teach everyone to ignore it, so the first assertion is that two builds of the same source agree
with each other. Only then does a disagreement with the shipped page mean the page is stale.
"""
from __future__ import annotations

import filecmp
import io
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")


def _build(into: str) -> str:
    """Copy the portal directory into `into` and run the builder there. Returns the copy."""
    work = os.path.join(into, "portal")
    shutil.copytree(PORTAL, work)
    result = subprocess.run([sys.executable, "build_portal_pages.py"],
                            cwd=work, capture_output=True, text=True, timeout=900)
    if result.returncode != 0:
        raise AssertionError("the builder failed:\n%s" % (result.stderr or result.stdout)[-800:])
    return work


def _pages() -> list:
    return sorted(name for name in os.listdir(PORTAL) if name.endswith("_portal.html"))


class TheBuilderIsDeterministicTest(unittest.TestCase):
    """The floor. Without it a stale-page failure and a timestamp are the same red."""

    def test_two_builds_of_one_source_agree(self) -> None:
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            one, two = _build(first), _build(second)
            differing = [name for name in _pages()
                         if not filecmp.cmp(os.path.join(one, name),
                                            os.path.join(two, name), shallow=False)]
            self.assertEqual([], differing,
                             "the builder is not deterministic, so a diff against it means "
                             "nothing: %s" % differing)

    def test_it_produced_the_pages_at_all(self) -> None:
        # Otherwise every comparison below is over an empty list.
        with tempfile.TemporaryDirectory() as work:
            built = _build(work)
            names = [n for n in os.listdir(built) if n.endswith("_portal.html")]
            self.assertGreater(len(names), 4, names)
            for name in names:
                self.assertGreater(os.path.getsize(os.path.join(built, name)), 2000, name)


class TheShippedPagesAreCurrentTest(unittest.TestCase):

    def test_every_page_matches_a_fresh_build(self) -> None:
        with tempfile.TemporaryDirectory() as work:
            built = _build(work)
            stale = [name for name in _pages()
                     if not filecmp.cmp(os.path.join(PORTAL, name),
                                        os.path.join(built, name), shallow=False)]
            self.assertEqual([], stale,
                             "%d shipped page(s) differ from what the builder produces; a "
                             "browser is running something the builder would not write: %s"
                             % (len(stale), stale))

    #: Generated whole. A hand edit anywhere in these is caught.
    GENERATED = ("overview_portal.html", "api_portal.html", "explore_portal.html",
                 "setup_portal.html", "catalog_portal.html")
    #: Only nav-injected. The builder starts FROM these, so it owns part of them and no more.
    INJECTED = ("ingestion_portal.html", "api_key_portal.html")

    def test_the_parts_the_builder_owns_on_an_injected_page_are_current(self) -> None:
        """Bounded on purpose, and the bound is the interesting half.

        The builder does not generate these two: it reads the page and rewrites the nav, its
        stylesheet and the tabs helper. So a hand edit to the BODY survives into the build -- both
        sides carry it and the comparison passes. Asserting these pages "match a fresh build"
        without saying that would be a test that passes whatever anyone does to them.

        What it does own it restores on every run, and the control below proves that is not an
        empty claim.
        """
        with tempfile.TemporaryDirectory() as work:
            built = _build(work)
            for name in self.INJECTED:
                self.assertTrue(os.path.exists(os.path.join(built, name)), name)
                self.assertTrue(filecmp.cmp(os.path.join(PORTAL, name),
                                            os.path.join(built, name), shallow=False), name)

    def test_the_builder_really_does_rewrite_the_nav_it_injects(self) -> None:
        """The control. Alter the nav in a copy, build there, and it comes back -- so the
        assertion above covers the nav rather than being satisfied by the builder leaving the
        file alone."""
        with tempfile.TemporaryDirectory() as work:
            copy = os.path.join(work, "portal")
            shutil.copytree(PORTAL, copy)
            target = os.path.join(copy, "ingestion_portal.html")
            with io.open(target, encoding="utf-8") as handle:
                original = handle.read()
            start = original.find('<nav class="portalnav">')
            self.assertGreater(start, 0, "no nav on the injected page")
            end = original.find("</nav>", start)
            altered = original[:start] + original[start:end].replace("Setup", "Setupp", 1)                 + original[end:]
            self.assertNotEqual(original, altered, "nothing in the nav to alter")
            with io.open(target, "w", encoding="utf-8") as handle:
                handle.write(altered)
            result = subprocess.run([sys.executable, "build_portal_pages.py"],
                                    cwd=copy, capture_output=True, text=True, timeout=900)
            self.assertEqual(0, result.returncode, result.stderr)
            with io.open(target, encoding="utf-8") as handle:
                self.assertEqual(original, handle.read(),
                                 "the builder left an altered nav in place, so the comparison "
                                 "above proves nothing about it")

    def test_a_body_edit_on_an_injected_page_is_NOT_claimed_to_be_covered(self) -> None:
        """Stated as a limit rather than left for someone to discover. The two lists together are
        every shipped page, so nothing is silently outside both."""
        shipped = {n for n in os.listdir(PORTAL) if n.endswith("_portal.html")}
        self.assertEqual(shipped, set(self.GENERATED) | set(self.INJECTED))
        self.assertTrue(set(self.GENERATED).isdisjoint(self.INJECTED))


class ItNeverTouchesTheWorkingTreeTest(unittest.TestCase):
    """The builder writes in place, so running it to check the tree would rewrite the tree."""

    def test_the_shipped_pages_are_unchanged_by_this_file(self) -> None:
        before = {name: os.path.getmtime(os.path.join(PORTAL, name)) for name in _pages()}
        with tempfile.TemporaryDirectory() as work:
            _build(work)
        after = {name: os.path.getmtime(os.path.join(PORTAL, name)) for name in _pages()}
        self.assertEqual(before, after, "the builder ran against the working tree")


if __name__ == "__main__":
    unittest.main()
