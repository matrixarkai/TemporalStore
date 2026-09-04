#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A portal page in a background tab lets go of its live connection, and keeps letting go.

The status strip has carried the rule in as many words -- *"A hidden tab holds a connection open
for nobody. Drop it, reconnect on return."* -- and it did abort the request when the tab was
hidden. What it never did was stop the reconnect. Aborting rejects the fetch, the ``catch``
schedules ``setTimeout(open, backoff)``, and about a second later the page opened the stream again
with the tab still hidden. The abort is on one line; the line that undoes it is thirty away in a
different function, which is why reading the source says the rule holds.

Two pages had less than that. Overview and Setup claim ``window.__matrixarkLive``, so the strip's
script returns before it ever registers the rule, and their own ``liveStream`` registered only
``pagehide`` -- no drop at all, on the two pages a customer is most likely to leave open in a
background tab.

Whether a connection comes back is a question about a timer inside a rejected promise, so it is
run, not read. The harness gives the page a fake clock and a fake fetch that never settles -- which
is what a live stream looks like -- hides the tab, and counts the connections opened afterwards.

The flush in that harness is load-bearing. The reconnect is scheduled at the end of the abort's
microtask chain, so a scan for due timers that runs first finds none and reports a page that let go
of its connection. That is a false pass on the one behaviour this file exists to check, and it is
what a single ``await Promise.resolve()`` produced.
"""
from __future__ import annotations

import io
import os
import re
import shutil
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "hidden_tab_harness.js")
BUILDER = os.path.join(PORTAL, "build_portal_pages.py")


def read(path: str) -> str:
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def pages() -> dict:
    return {name: read(os.path.join(PORTAL, name))
            for name in sorted(os.listdir(PORTAL)) if name.endswith(".html")}


def by_stream() -> tuple:
    """Which pages run their own connection, and which let the strip run one.

    Read off the page rather than listed: a page opts out of the strip's connection by claiming
    ``__matrixarkLive``, and that claim is the thing that decides which code has to obey the rule.
    """
    own, strip = [], []
    for name, text in pages().items():
        (own if '__matrixarkLive = "page"' in text else strip).append(name)
    return own, strip


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class AHiddenTabLetsGoTest(unittest.TestCase):

    def _run(self, page_name, mode):
        return subprocess.run(["node", HARNESS, os.path.join(PORTAL, page_name), mode],
                              capture_output=True, text=True, timeout=300)

    def setUp(self) -> None:
        self.own, self.strip = by_stream()

    def test_both_kinds_of_page_exist(self) -> None:
        """The split is the point. With everything on one side, half these checks say nothing."""
        self.assertGreaterEqual(len(self.own), 2, self.own)
        self.assertGreaterEqual(len(self.strip), 4, self.strip)

    def test_the_strip_lets_go_and_stays_gone(self) -> None:
        for name in self.strip:
            with self.subTest(page=name):
                proc = self._run(name, "strip")
                self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
                self.assertIn("ok   hiding the tab aborts the connection", proc.stdout)
                self.assertIn("ok   and it stays closed while the tab is hidden", proc.stdout)

    def test_a_page_running_its_own_stream_does_the_same(self) -> None:
        """The strip stands down for these two, so the rule has to live in their stream as well --
        and these are the pages most likely to be sitting in a background tab."""
        for name in self.own:
            with self.subTest(page=name):
                proc = self._run(name, "page")
                self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
                self.assertIn("ok   the page defines its own stream", proc.stdout)
                self.assertIn("ok   hiding the tab aborts the connection", proc.stdout)
                self.assertIn("ok   and it stays closed while the tab is hidden", proc.stdout)

    def test_coming_back_reconnects(self) -> None:
        """Dropping it is only half. A page that never reconnects is a page whose live panels
        quietly stop being live, which is worse than the connection it saved."""
        for name in self.strip:
            with self.subTest(page=name, mode="strip"):
                self.assertIn("ok   coming back opens it again", self._run(name, "strip").stdout)
        for name in self.own:
            with self.subTest(page=name, mode="page"):
                self.assertIn("ok   coming back opens it again", self._run(name, "page").stdout)


class TheHarnessCanSeeAReconnectTest(unittest.TestCase):
    """The check above passes trivially on a harness that looks for the reconnect too early."""

    def test_it_waits_for_the_microtask_queue(self) -> None:
        source = read(HARNESS)
        self.assertIn("realSetImmediate", source,
                      "the harness flushes with promise ticks, which is not enough to see a "
                      "reconnect scheduled from a rejection handler")
        self.assertIn("await this.flush()", source)

    def test_it_hands_the_page_a_key(self) -> None:
        """The strip gives up quietly without one, so a stub with no key produces a page that
        never connects -- and every check about letting go passes for the wrong reason."""
        self.assertIn("value: \"k-test\"", read(HARNESS))


class TheStreamIsWrittenOnceTest(unittest.TestCase):

    def test_the_builder_holds_one_copy(self) -> None:
        """It held two, byte for byte. Two copies of a behaviour that looks identical is how they
        stop being identical -- and this one had a rule to keep."""
        self.assertEqual(1, read(BUILDER).count("function liveStream(options)"))

    def test_both_pages_still_get_it(self) -> None:
        own, _strip = by_stream()
        for name in own:
            with self.subTest(page=name):
                self.assertEqual(1, pages()[name].count("function liveStream(options)"))


if __name__ == "__main__":
    unittest.main()
