#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Both destructive actions on the key portal ask first.

Revoke always asked: *"Revoke <id>? Requests using it will be rejected."* Rotate sat directly
beside it in the same table row and asked nothing -- while doing the same thing to the key you
were using, plus issuing a replacement. Worse, of the two it is the one that looks safe: Revoke
carries `mini danger`, Rotate `mini secondary`, so the unguarded button is the calmer-looking one.

There is no undo. The old secret is stored as a hash and the new one is shown once, so a
mis-click means every client holding that key starts failing and the replacement is on screen
exactly once.

The wording says what happens rather than naming the operation, because "rotate" is precisely the
word whose consequences the person clicking has not thought through.

Two of these can only be seen by running the page: that the dialog appears at all, and that
answering *no* leaves the key alone. A guard that asks and then proceeds regardless is the same
button it was before with an extra click, and it reads identically in source.
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
PAGE = os.path.join(PORTAL, "api_key_portal.html")
HARNESS = os.path.join(PORTAL, "key_copy_harness.js")

# Everything on this page that cannot be undone. Adding a third without a confirmation should
# fail here rather than in somebody's deployment.
DESTRUCTIVE = ("revokeKey", "rotateKey")


def page_source() -> str:
    with io.open(PAGE, encoding="utf-8") as handle:
        return handle.read()


def body_of(source: str, function: str) -> str:
    """The text of one top-level function, up to the next one at the same indent."""
    start = source.index("function %s(" % function)
    following = re.search(r"\n  function \w+\(", source[start + 1:])
    end = start + 1 + following.start() if following else len(source)
    return source[start:end]


class BothDestructiveActionsAskTest(unittest.TestCase):

    def test_each_one_confirms_before_it_acts(self) -> None:
        source = page_source()
        for function in DESTRUCTIVE:
            body = body_of(source, function)
            self.assertIn("window.confirm(", body,
                          "%s cannot be undone and does not ask" % function)

    def test_the_question_says_what_happens_not_just_the_verb(self) -> None:
        """"Rotate this key?" is the question somebody clicks through. What it costs is that the
        key they are using stops working."""
        body = body_of(page_source(), "rotateKey")
        self.assertIn("stops working", body, body[:200])

    def test_the_guard_runs_before_the_request(self) -> None:
        """A confirmation after the call is decoration."""
        body = body_of(page_source(), "rotateKey")
        self.assertLess(body.index("window.confirm("), body.index("apiFetch("),
                        "the request is issued before the question is asked")


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class TheDialogIsObeyedTest(unittest.TestCase):
    """Runs the page. Whether a dialog is obeyed is behaviour, not text."""

    def _run(self):
        return subprocess.run(["node", HARNESS, PAGE], capture_output=True, text=True, timeout=180)

    def test_the_page_runs_clean(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_rotating_asks(self) -> None:
        self.assertIn("ok   G rotating asks first", self._run().stdout)

    def test_declining_rotates_nothing(self) -> None:
        self.assertIn("ok   G answering no rotates nothing", self._run().stdout)

    def test_accepting_still_rotates(self) -> None:
        """Otherwise a rotate button that never worked would satisfy the check above."""
        self.assertIn("ok   G answering yes still rotates", self._run().stdout)


if __name__ == "__main__":
    unittest.main()
