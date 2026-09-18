#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
""""Nothing matched" has a third cause, and the page used to send you away from it.

The Explore page's empty-result message named two: the stored vectors were made by a different
encoder, or the provider is still deterministic. Its own comment said why the list matters --
*naming only one sends the reader to check a setting that is already right.*

There is a third. Under load this gateway stops retrieving and answers with an empty pack in about
100 ms, carrying ``service_backpressure`` in ``warnings``. The page rendered that warning, as the
raw string the backend wrote, underneath a full sentence confidently blaming the embedding
provider. A reader follows the sentence, goes to Setup, finds the provider correctly configured,
and has learned nothing -- worse than silence, because now the page looks broken too.

Shedding is named first, because it is the only one of the three that is not about this store at
all: nothing was searched, so nothing about the encoder or the vectors explains it.

The sentence was lifted out of the fetch callback to get here. Inside ``ask()``'s ``.then()`` it
was reachable only from a browser talking to a gateway that happened to be shedding, which is to
say never, in a test.
"""
from __future__ import annotations

import os
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "explore_empty_harness.js")
PAGE = os.path.join(PORTAL, "explore_portal.html")


class TheEmptyResultNamesTheRightCauseTest(unittest.TestCase):
    """Driven through the shipped page, not read out of it.

    The guard that already covers this area reads the generator's source for a sentence. That
    catches a sentence being deleted and nothing else -- not a condition inverted, not a branch
    that can never be reached, not a message rendered for the wrong shape.
    """

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_the_shipped_page_names_each_cause_correctly(self) -> None:
        out = subprocess.run(["node", HARNESS, PAGE],
                             capture_output=True, text=True, timeout=300)
        self.assertEqual(0, out.returncode, (out.stdout + out.stderr)[-3000:])

    def test_the_harness_checks_more_than_a_couple_of_things(self) -> None:
        """The vacuity guard. A harness that stopped extracting would report `all passed` over an
        empty list of assertions, and the test above would be satisfied by it."""
        out = subprocess.run(["node", HARNESS, PAGE],
                             capture_output=True, text=True, timeout=300)
        passed = [line for line in out.stdout.splitlines() if line.startswith("ok ")]
        self.assertGreaterEqual(len(passed), 10,
                                "the harness ran %d assertions; it has probably stopped "
                                "extracting" % len(passed))


if __name__ == "__main__":
    unittest.main()
