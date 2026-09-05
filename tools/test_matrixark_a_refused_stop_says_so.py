#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Stopping an import that the gateway refuses to stop says so.

``cancelJob`` posted to the cancel route and then, whatever came back, reloaded the job list::

    fetch(...).then(loadJobs).catch(...)

``fetch`` only rejects on a network failure. A 403 for want of a scope, a 409 because the job had
already finished, a 404 for an id that is not there -- all of those *resolve*, so the list reloaded
and nothing was said. The customer clicked Stop, watched the table refresh, saw the import still
running, and concluded the cancel was slow.

``retryJob`` sits directly above it, for the sibling action on the same page, and it checks
``res.ok`` and reports through the shared helpers. So this was not the page's habit -- it was one
function out of step with its own neighbour, which is why it lasted: everything around it is right.

On a refusal the list is deliberately *not* reloaded. ``loadJobs`` clears the message area, so
refreshing would replace the explanation with the same table the reader was already looking at --
which is the original bug wearing an error handler.

None of that is visible in source; the code reads as a completed action with a refresh at the end.
So the page's own ``cancelJob`` is run against canned responses and asked what it did.
"""
from __future__ import annotations

import io
import os
import shutil
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
PAGE = os.path.join(PORTAL, "ingestion_portal.html")
HARNESS = os.path.join(PORTAL, "stop_refusal_harness.js")


def page() -> str:
    with io.open(PAGE, encoding="utf-8") as handle:
        return handle.read()


class ItChecksTheAnswerTest(unittest.TestCase):

    def test_the_cancel_handler_looks_at_the_status(self) -> None:
        """The single line that decides whether any of the behaviour below is possible."""
        start = page().index("function cancelJob(")
        body = page()[start:page().index("\n  }", start)]
        self.assertIn("res.ok", body,
                      "the cancel handler does not look at whether the call succeeded")

    def test_it_reports_through_the_same_helpers_as_its_neighbour(self) -> None:
        """retryJob does this for the sibling action. Two ways of explaining the same refusal on
        one page is how they drift."""
        start = page().index("function cancelJob(")
        body = page()[start:page().index("\n  }", start)]
        self.assertIn("__matrixarkWhy", body)
        self.assertIn("failure(res.status)", body)


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class WhatItActuallyDoesTest(unittest.TestCase):

    def _run(self):
        return subprocess.run(["node", HARNESS, PAGE], capture_output=True, text=True, timeout=300)

    def test_the_harness_passes(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_a_refused_stop_is_reported(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   a refused stop is reported", out)
        self.assertIn("ok   and reported as a failure", out)
        self.assertIn("ok   and says what the gateway said", out)

    def test_the_explanation_is_not_reloaded_over(self) -> None:
        """Reporting it and then refreshing the table on top would be the same silence, arrived at
        by a longer route."""
        self.assertIn("ok   and does not reload the table over the explanation", self._run().stdout)

    def test_a_refusal_with_no_body_still_gets_a_sentence(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   a refusal with no body still gets a sentence", out)
        self.assertIn("ok   and not a bare number", out)

    def test_a_stop_that_worked_still_refreshes(self) -> None:
        """The check above is satisfied by a handler that never reloads at all, which would leave
        a stopped import looking like it is still running."""
        out = self._run().stdout
        self.assertIn("ok   a stop that worked refreshes the list", out)
        self.assertIn("ok   and does not cry failure", out)

    def test_an_unreachable_gateway_is_still_reported(self) -> None:
        self.assertIn("ok   a gateway that cannot be reached is reported", self._run().stdout)


if __name__ == "__main__":
    unittest.main()
