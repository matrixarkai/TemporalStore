#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The setup page reads the encoding backlog once, not twice.

`onFrame` already renders the backlog the stream carries. A timer also fetched
`/v1/admin/embeddings`, and that endpoint walks the record log on the backend -- two reads of one
number, per viewer, every five seconds while anything was draining. The stream is the fresher of
the two: it refreshes at 4s while pending and 30s when idle, against the poll's 5 and 60.

Run rather than read. Whether the timer fetches depends on a closure variable set by a callback the
stream invokes when it connects, which no amount of grepping settles.

The harness runs the page twice -- with a stream that connects and one that fails -- because
"does not poll while live" is satisfied by a page that never polls at all, and that page is broken
for anyone whose proxy buffers server-sent events.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "encoding_poll_harness.js")
PAGE = os.path.join(PORTAL, "setup_portal.html")


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class TheBacklogIsReadOnceTest(unittest.TestCase):

    def _run(self):
        return subprocess.run(["node", HARNESS, PAGE], capture_output=True, text=True, timeout=180)

    def test_the_page_does_not_read_it_twice(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode,
                         "the setup page reads the encoding backlog twice:"
                         + chr(10) + proc.stdout + proc.stderr)

    def test_the_fallback_still_works(self) -> None:
        """A page whose stream is blocked must still get its numbers from somewhere."""
        out = self._run().stdout
        self.assertIn("ok   with no stream, the timer reads the backlog", out, out)

    def test_the_live_case_is_not_vacuous(self) -> None:
        """If the stream never opened, 'it did not poll while live' would prove nothing."""
        out = self._run().stdout
        self.assertIn("ok   the page opened the stream", out, out)
        self.assertIn("ok   the page set an interval to fall back on", out, out)


if __name__ == "__main__":
    unittest.main()
