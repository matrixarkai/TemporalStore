#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The stream readers read the label the gateway writes.

The gateway labels everything it puts on the stream: ``event: status`` for a frame, ``event: bye``
before it closes one that has reached its maximum age -- ten minutes, so that an abandoned tab does
not hold a connection for the life of the worker. Both page-side readers looked for a ``data:``
line and nothing else.

A goodbye carries a data line of its own. So ``{"reason": "stream_max_age"}`` was handed to the
frame handler as though it were the deployment's state, and a page rendering from an object with
none of the fields blanks what it draws: the traffic table, the failures panel, the imports, the
counts, and every segment of the strip. The close was then reported through the backoff path, which
the pages show as **down**. Both happen on every open page every ten minutes, on every deployment.

The strip's reader is the worse of the two -- it did not look at the event line at all, so *any*
event the gateway ever adds would be rendered as state on the five pages that read through it.
Running it against an unknown event shows the strip drawing ``99 warnings`` from one.

None of this is provable from source. A reader that ignores the label reads exactly like one that
honours it until something is put through it, which is what ``stream_label_harness.js`` does.

The floor on the reconnect is the other half. Reconnecting at once is right for a stream that ran
its ten minutes; for one that says goodbye immediately it is a hot loop, so the label decides
whether to report a fault and the clock decides whether to believe it.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

PORTAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portal")
HARNESS = os.path.join(PORTAL, "stream_label_harness.js")
GATEWAY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "matrixark_v1_gateway.py")

# The two pages that run their own stream; the other five read through the strip.
OWN_STREAM = ("setup_portal.html", "overview_portal.html")


def _pages():
    return sorted(name for name in os.listdir(PORTAL) if name.endswith("_portal.html"))


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class EveryPageReadsTheLabelTest(unittest.TestCase):

    def _run(self, page):
        return subprocess.run(["node", HARNESS, os.path.join(PORTAL, page)],
                              capture_output=True, text=True, timeout=300)

    def test_there_are_pages_to_check(self) -> None:
        """A sweep over an empty list would pass while checking nothing."""
        self.assertGreaterEqual(len(_pages()), 7, _pages())

    def test_every_page_passes(self) -> None:
        for page in _pages():
            with self.subTest(page=page):
                proc = self._run(page)
                self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_the_goodbye_is_not_rendered_as_state_anywhere(self) -> None:
        for page in _pages():
            with self.subTest(page=page):
                self.assertIn("ok   the strip did not render the goodbye over it",
                              self._run(page).stdout)

    def test_a_planned_close_is_not_shown_as_trouble_anywhere(self) -> None:
        for page in _pages():
            with self.subTest(page=page):
                self.assertIn("ok   a planned close leaves the strip's dot alone",
                              self._run(page).stdout)

    def test_a_real_disconnection_is_still_shown(self) -> None:
        """The point of reading the label is to tell the two apart, not to stop reporting."""
        for page in _pages():
            with self.subTest(page=page):
                self.assertIn("ok   a close with no goodbye still marks the strip stale",
                              self._run(page).stdout)

    def test_the_two_pages_with_their_own_reader_are_checked(self) -> None:
        """The harness skips those checks where there is no such reader, and a skip that spread to
        every page would empty this file without failing it."""
        for page in _pages():
            out = self._run(page).stdout
            with self.subTest(page=page):
                if page in OWN_STREAM:
                    self.assertIn("ok   a goodbye does not reach the frame handler", out)
                    self.assertIn("ok   a planned close reconnects", out)
                else:
                    self.assertIn("skip liveStream is not on this page", out)


class TheGatewayAndTheReadersAgreeTest(unittest.TestCase):
    """The two halves of one contract, asserted against each other.

    The readers now act on the names `status` and `bye`. If the gateway ever emits a third, the
    readers will ignore it -- which is the safe direction, and much better than rendering it -- but
    this should be a decision somebody makes rather than one that happens quietly.
    """

    def _emitted_labels(self):
        with open(GATEWAY, encoding="utf-8") as handle:
            source = handle.read()
        return set(re.findall(r'b"event: ([a-z_]+)', source))

    def test_the_gateway_emits_labels_at_all(self) -> None:
        """An empty set would make the comparison below vacuous."""
        self.assertTrue(self._emitted_labels())

    def test_the_readers_handle_every_label_the_gateway_emits(self) -> None:
        emitted = self._emitted_labels()
        with open(os.path.join(PORTAL, "setup_portal.html"), encoding="utf-8") as handle:
            page = handle.read()
        unhandled = sorted(name for name in emitted if '"%s"' % name not in page)
        self.assertEqual([], unhandled,
                         "the gateway labels these and no reader mentions them: %r" % unhandled)


if __name__ == "__main__":
    unittest.main()
