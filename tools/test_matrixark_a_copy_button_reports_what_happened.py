#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A copy button says whether the copy happened.

Seven call sites across four portal pages wrote to the clipboard and then announced success
without looking:

  * Setup's four buttons, and Explore's and Overview's message lines, ran
    ``if (navigator.clipboard) { writeText(text); }`` and then said "copied" *unconditionally*. On
    an http:// origin -- which a self-hosted portal often is -- ``navigator.clipboard`` does not
    exist, so nothing was copied and the page said it was.
  * The API page set "hide curl — copied" in the same tick as the write, before the promise it
    ignored had settled.
  * Ingestion alone waited for the promise, but had no rejection branch, so a refused copy left
    the label untouched and said nothing at all.

``writeText`` returns a promise that rejects on a denied permission or an unfocused document.
Claiming a copy that did not happen is worse than saying nothing: the reader stops trying and
leaves with an empty clipboard.

One helper in the shared nav script now resolves true or false and each caller reports what it is
handed. The behaviour is exercised by ``copy_helper_harness.js``, which runs the helper as a
browser would across four clipboards: one that accepts, one that refuses, one that is absent, and
one that throws on property access.

The source checks below state their own extent -- how many pages were scanned and how many write
sites were found -- because a pattern that stopped matching would otherwise report success over
nothing.
"""
from __future__ import annotations

import glob
import io
import os
import re
import shutil
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")

# Every page carries the shared helper, so each contributes one write site; the key portal has its
# own copier for the one-time secret as well. A floor, not an equality: adding a page is fine.
MIN_PAGES = 7
MIN_WRITE_SITES = 7

WRITE = "navigator.clipboard.writeText("


def pages() -> list:
    return sorted(glob.glob(os.path.join(PORTAL, "*.html")))


def text_of(path: str) -> str:
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


class TheCopyHelperBehavesTest(unittest.TestCase):

    @unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
    def test_the_helper_reports_every_outcome_on_every_page(self) -> None:
        proc = subprocess.run(
            ["node", os.path.join(PORTAL, "copy_helper_harness.js")] + pages(),
            capture_output=True, text=True, timeout=180)
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
        self.assertIn("every page given carries the helper", proc.stdout)


class NoCopySiteClaimsWhatItDidNotDoTest(unittest.TestCase):

    def test_the_scan_covers_the_pages_it_claims_to(self) -> None:
        self.assertGreaterEqual(len(pages()), MIN_PAGES,
                                "found %d portal pages, expected at least %d"
                                % (len(pages()), MIN_PAGES))

    def test_every_clipboard_write_looks_at_its_result(self) -> None:
        """The fire-and-forget shape is exactly a write whose promise nobody reads."""
        found = 0
        for path in pages():
            source = text_of(path)
            for match in re.finditer(re.escape(WRITE), source):
                found += 1
                tail = source[match.end():match.end() + 240]
                self.assertIn(".then(", tail,
                              "%s: a clipboard write at offset %d ignores the promise it returns, "
                              "so whatever it reports is a guess"
                              % (os.path.basename(path), match.start()))
        self.assertGreaterEqual(found, MIN_WRITE_SITES,
                                "found only %d clipboard writes; the pattern has stopped matching "
                                "and this check is passing over nothing" % found)

    def test_no_page_still_carries_the_unconditional_shape(self) -> None:
        """`if (navigator.clipboard) { writeText(x); }` followed by an unconditional claim."""
        for path in pages():
            self.assertNotIn("if (navigator.clipboard) { navigator.clipboard.writeText(",
                             text_of(path),
                             "%s writes to the clipboard without reading the result"
                             % os.path.basename(path))

    def test_every_caller_branches_on_the_answer(self) -> None:
        """A caller that takes the boolean and ignores it is the same bug in a new shape."""
        callers = 0
        for path in pages():
            source = text_of(path)
            for match in re.finditer(r"__matrixarkCopyText\(", source):
                # The definition itself is not a call site.
                if source[max(0, match.start() - 30):match.start()].rstrip().endswith("="):
                    continue
                callers += 1
                tail = source[match.end():match.end() + 400]
                self.assertRegex(tail, r"\.then\(function \(ok\)",
                                 "%s: a copy at offset %d does not take the outcome"
                                 % (os.path.basename(path), match.start()))
                self.assertIn("ok ?", tail,
                              "%s: a copy at offset %d takes the outcome and never branches on it"
                              % (os.path.basename(path), match.start()))
        self.assertGreaterEqual(callers, MIN_WRITE_SITES,
                                "found only %d callers of the shared copier" % callers)


class TheHelperIsReachableFromEveryPageTest(unittest.TestCase):
    """A page that calls the helper without carrying it throws inside a click handler, which is
    invisible until somebody presses the button."""

    def test_a_page_that_calls_the_copier_also_defines_it(self) -> None:
        for path in pages():
            source = text_of(path)
            if "__matrixarkCopyText(" not in source:
                continue
            self.assertIn("window.__matrixarkCopyText = function", source,
                          "%s calls the shared copier but the definition is not on the page"
                          % os.path.basename(path))


if __name__ == "__main__":
    unittest.main()
