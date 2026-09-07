#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The page keeps a record of what it asked the gateway, and what came back.

Every panel on this portal is drawn from a call, and the only evidence a call was made was whether
the panel filled in. When something is wrong the first question is *which request failed and what
did it answer*, and the only way to find out was the browser's own developer tools -- a fine answer
for whoever wrote the page, and no answer at all for an operator looking at a deployment they did
not build.

So the last sixty calls are kept and can be shown: method, path, status, how long, and when.

**The response is handed back untouched.** That is the property this whole thing lives or dies on:
a recorder that read the body to pull an incident token out of it would hand the caller an empty
one, and every panel on the portal would break in exactly the way this exists to help diagnose. It
is checked by reading a body *after* the recorder has seen it.

The three outcomes are kept apart, because they are three different problems. A 200 is fine, a 403
is the deployment refusing, and a status of 0 with the browser's own message is a request that
never got an answer -- which is a network or a CORS question, not a gateway one.
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "trace_harness.js")


def pages() -> list:
    return sorted(f for f in os.listdir(PORTAL) if f.endswith("_portal.html"))


def read(name: str) -> str:
    with io.open(os.path.join(PORTAL, name), encoding="utf-8") as handle:
        return handle.read()


class TheRecorderTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            raise unittest.SkipTest("node is not available")
        out = subprocess.run(["node", HARNESS, os.path.join(PORTAL, "overview_portal.html")],
                             capture_output=True, text=True, timeout=300)
        if out.returncode != 0:
            raise AssertionError(out.stderr[-900:])
        cls.result = json.loads(out.stdout)

    def test_the_body_is_still_there_after_it_is_recorded(self) -> None:
        """The property everything else depends on. A recorder that consumed the body would break
        every panel on the portal, and it would break them while looking like a debugging aid."""
        self.assertTrue(self.result["bodyWasNotConsumedByTheRecorder"])
        self.assertEqual('{"fine":true}', self.result["bodyAfterRecording"])

    def test_the_caller_still_sees_the_failure(self) -> None:
        """A recorder that swallowed a rejection would leave every catch on the portal unreached,
        which is worse than not recording at all."""
        self.assertEqual("Failed to fetch", self.result["refusedError"])

    def test_the_three_outcomes_are_kept_apart(self) -> None:
        self.assertEqual([200, 403, 0], self.result["statuses"])
        self.assertEqual(["/ok", "/refused", "/gone"], self.result["urls"])

    def test_a_request_that_never_answered_says_what_the_browser_said(self) -> None:
        """A status of 0 is not a status. Without the message it is indistinguishable from any
        other call that did not complete, and the message is what separates blocked from refused.
        """
        self.assertEqual("Failed to fetch", self.result["detailOnTheOneThatNeverAnswered"])

    def test_it_is_bounded(self) -> None:
        """A portal tab is left open for days, so an unbounded list of every request is a leak that
        only shows up on the busiest deployment."""
        self.assertEqual(60, self.result["bounded"])

    def test_every_entry_can_be_placed_in_time(self) -> None:
        self.assertTrue(self.result["everyEntryHasATime"])
        self.assertTrue(self.result["everyEntryHasADuration"])

    def test_it_actually_wrapped_fetch(self) -> None:
        """The positive control: every assertion above is satisfied by a recorder that records
        nothing, if the harness's own fetch answers the same way."""
        self.assertTrue(self.result["wrapped"])
        self.assertGreater(self.result["listenerFired"], 0)


class EveryPageCarriesItTest(unittest.TestCase):
    """Which call failed is asked on whichever page went wrong, so it is on all of them -- the two
    the builder only injects a nav into included."""

    def test_each_page_has_the_recorder_the_button_and_the_panel(self) -> None:
        for name in pages():
            text = read(name)
            with self.subTest(page=name):
                self.assertEqual(1, text.count("window.__matrixarkTrace = "))
                self.assertEqual(1, text.count('id="liveCalls"'))
                self.assertEqual(1, text.count('id="callLog"'))

    def test_the_panel_starts_folded_away(self) -> None:
        """It is a debugging surface, not part of what the page is for."""
        for name in pages():
            with self.subTest(page=name):
                self.assertIn('<div class="calllog" id="callLog" hidden>', read(name))

    def test_there_are_pages_to_check(self) -> None:
        self.assertGreaterEqual(len(pages()), 5, "found %s" % pages())


if __name__ == "__main__":
    unittest.main()
