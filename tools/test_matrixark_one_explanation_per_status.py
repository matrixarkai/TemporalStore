#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Every panel explains a gateway status the same way, and the upload page explains 413.

Three pages mapped HTTP statuses to sentences and each carried a different subset:

===== ======== ========= ======
code  explore  ingestion setup
===== ======== ========= ======
401   yes      yes       yes
403   yes      yes*      yes*
409   --       yes       --
413   yes      **no**    yes
429   yes      yes       yes
502   yes      --        --
504   yes      --        yes
===== ======== ========= ======

The 413 row is the one that cost something. Ingestion is the page that uploads files, and it was
the page with no sentence for the refusal an upload gets when it is too big -- so a customer read
*"The gateway answered 413."* on the one panel where the cause is obvious to everybody except them.
The other four pages mapped nothing at all, so a 502 from a gateway that could not reach its storage
was a bare number there too.

The map now sits beside ``__matrixarkWhy`` and ``__matrixarkCopyText``, and each page keeps only
what is genuinely its own: a *file* rather than a request where a file is being sent, and the import
conflict only ingestion can produce.

Setup's 403 loses its hardcoded *"Issue one with admin:api_key"*. That is not a downgrade: the edge
sends ``required`` on a scope refusal and ``__matrixarkWhy`` appends it, so the scope is named by
the process that refused rather than guessed by the page -- which is what makes it right when the
answer is a scope the page did not predict.

A map looks fine in source whatever it omits, so the pages' own helpers are executed and asked.
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
HARNESS = os.path.join(PORTAL, "status_message_harness.js")

# What the edge answers with. A page leaving any of these as a bare number is a page that made a
# reader guess at something the gateway already knew.
EXPLAINED = (401, 403, 413, 429, 502, 503, 504)


def pages() -> dict:
    return {name: io.open(os.path.join(PORTAL, name), encoding="utf-8").read()
            for name in sorted(os.listdir(PORTAL)) if name.endswith(".html")}


class TheMapIsSharedTest(unittest.TestCase):

    def setUp(self) -> None:
        self.pages = pages()
        self.assertGreaterEqual(len(self.pages), 7, "not every panel is being checked")

    def test_every_panel_carries_it(self) -> None:
        without = sorted(name for name, text in self.pages.items()
                         if "__matrixarkFailure = function" not in text)
        self.assertEqual([], without, "panels with no status map: %r" % without)

    def test_no_panel_keeps_a_second_one(self) -> None:
        """Two maps is how they came to disagree. The shared helper is the only place a status
        literal belongs; a page's own failure() may only choose overrides."""
        extra = {}
        for name, text in self.pages.items():
            own = re.search(r"function failure\(status\) \{[\s\S]*?\n  \}", text)
            if own and re.search(r"if \(status === \d", own.group(0)):
                extra[name] = own.group(0)[:80]
        self.assertEqual({}, extra, "panels still holding their own map: %r" % sorted(extra))


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class TheAnswersAreRunNotReadTest(unittest.TestCase):

    def _run(self):
        return subprocess.run(
            ["node", HARNESS] + [os.path.join(PORTAL, n) for n in sorted(pages())],
            capture_output=True, text=True, timeout=300)

    def test_the_harness_passes(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_no_panel_leaves_a_status_as_a_bare_number(self) -> None:
        out = self._run().stdout
        for name in sorted(pages()):
            short = name.replace("_portal.html", "")
            with self.subTest(page=short):
                self.assertIn("ok   %s: every status the edge answers with has a sentence" % short,
                              out)

    def test_the_upload_page_explains_an_oversized_upload(self) -> None:
        """The gap that cost something: the page sending files had no sentence for the refusal a
        file gets for being too big."""
        self.assertIn("ok   ingestion: an oversized upload is described as a file, not a request",
                      self._run().stdout)

    def test_a_page_keeps_what_only_it_knows(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   ingestion: a running import is still its own answer", out)
        self.assertIn("ok   explore: an oversized upload is described as a file, not a request", out)

    def test_setup_gained_the_status_it_was_missing(self) -> None:
        self.assertIn("ok   setup: a gateway that cannot reach storage is explained here too",
                      self._run().stdout)

    def test_an_unfamiliar_status_still_names_the_number(self) -> None:
        """Better than a wrong sentence: a reader can search for the number."""
        out = self._run().stdout
        self.assertIn("ok   setup: an unfamiliar status still names the number", out)

    def test_an_override_applies_only_where_it_was_given(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   setup: an override wins where a page knows better", out)
        self.assertIn("ok   setup: an override for another status does not leak", out)


if __name__ == "__main__":
    unittest.main()
