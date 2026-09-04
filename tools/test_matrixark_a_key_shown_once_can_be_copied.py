#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The key you are shown once can be copied, and a copy that failed says so.

A new API key is returned exactly once and stored only as a hash. The page said so plainly and
then left the customer to select twenty-four characters of base62 out of a dark code block by
hand -- while offering a copy button for the curl command, which they could rebuild from the form
above it in ten seconds. Rotation mints a replacement on the same terms and had the same gap. Miss
the selection, or navigate away, and the only recovery is to rotate again and update whatever was
going to use it.

The copy button that did exist reported success unconditionally. It wrote "Copied" whether or not
`navigator.clipboard` was there -- it is absent on an http:// origin, which a self-hosted portal
often is -- and it never looked at the promise `writeText` returns, so a rejected write also read
as "Copied". For a curl command that is a small annoyance. For a key shown once it is the entire
loss, and it is silent: the customer has been told the copy worked.

So one copier, used by all three sites, that reports what actually happened.

Two properties are worth pinning because neither is visible in the source.

**Nothing is offered when there is no key.** The server can answer without one, and the page
renders "(not returned)" for that. A copy button there would put that string on the clipboard and
look like it had worked.

**The secret is copied alone.** It is rendered beside the key id and above the warning; a copier
that swept up its surroundings would produce a clipboard that fails when pasted into a config
file, later and somewhere else.

The mutations below turn each guard off in the page source and require the harness to notice. A
guard whose removal changes no output is not being tested by anything.
"""
from __future__ import annotations

import io
import os
import shutil
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
PAGE = os.path.join(PORTAL, "api_key_portal.html")
HARNESS = os.path.join(PORTAL, "key_copy_harness.js")


def page_source() -> str:
    with io.open(PAGE, encoding="utf-8") as handle:
        return handle.read()


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class TheKeyShownOnceCanBeCopiedTest(unittest.TestCase):
    """Runs the page's own JS. Reading the source cannot tell these cases apart."""

    def _run(self, *mutation):
        return subprocess.run(["node", HARNESS, PAGE] + list(mutation),
                              capture_output=True, text=True, timeout=180)

    def test_every_copy_path_behaves(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
        self.assertIn("PASS", proc.stdout, proc.stdout)

    def test_the_created_key_is_offered_a_copy_button(self) -> None:
        self.assertIn("ok   A a copy button is offered", self._run().stdout)

    def test_the_secret_reaches_the_clipboard_alone(self) -> None:
        self.assertIn("ok   A clicking it copies the secret and nothing else", self._run().stdout)

    def test_nothing_is_offered_when_no_key_came_back(self) -> None:
        self.assertIn("ok   B no copy button is offered when there is no key", self._run().stdout)

    def test_a_refused_copy_is_not_reported_as_a_copy(self) -> None:
        self.assertIn("ok   C a refused copy is reported as failed, not as copied",
                      self._run().stdout)

    def test_an_absent_clipboard_api_is_reported_rather_than_ignored(self) -> None:
        self.assertIn("ok   D with no clipboard API the button says so rather than quietly "
                      "doing nothing", self._run().stdout)

    def test_rotation_copies_the_replacement_not_the_key_it_replaced(self) -> None:
        self.assertIn("ok   E it copies the NEW key, not the one it replaced", self._run().stdout)

    def test_the_curl_button_shares_the_honest_copier(self) -> None:
        self.assertIn("ok   F a refused curl copy is reported as failed too", self._run().stdout)


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class TheGuardsAreLoadBearingTest(unittest.TestCase):
    """Turn each guard off and require the harness to go red, and to go red in the right place.

    A mutation that changes nothing means the guard is inert; a mutation that reddens everything
    means the harness is asserting something coarser than the guard.
    """

    def _run(self, mutation):
        proc = subprocess.run(["node", HARNESS, PAGE, mutation],
                              capture_output=True, text=True, timeout=180)
        self.assertNotEqual(0, proc.returncode,
                            "the mutation changed no observable behaviour:\n" + proc.stdout)
        return proc.stdout

    def test_offering_a_button_with_no_key_is_caught(self) -> None:
        out = self._run("nogate")
        self.assertIn("FAIL B no copy button is offered when there is no key", out, out)
        self.assertNotIn("FAIL A", out, "the no-key gate should not affect the happy path:\n" + out)

    def test_claiming_a_copy_that_failed_is_caught(self) -> None:
        out = self._run("oldcopy")
        for case in ("FAIL C a refused copy", "FAIL D with no clipboard API",
                     "FAIL F a refused curl copy"):
            self.assertIn(case, out, out)
        self.assertNotIn("FAIL A", out,
                         "a successful copy should still be reported as one:\n" + out)


class EveryClipboardWriteGoesThroughTheOneCopierTest(unittest.TestCase):
    """A second copier would be free to lie again, so there is only allowed to be one.

    This scans source, so it states its own extent: the assertion is over the whole page, and it
    names the count it found rather than checking that some particular line still exists.
    """

    def test_the_page_writes_to_the_clipboard_in_exactly_one_place(self) -> None:
        source = page_source()
        self.assertEqual(1, source.count("clipboard.writeText("),
                         "every copy on this page must go through copyText, which reports what "
                         "happened; a second write site is free to claim a copy it did not make")

    def test_that_one_place_is_inside_the_copier(self) -> None:
        source = page_source()
        start = source.index("function copyText(")
        end = source.index("// ---- create", start)
        write = source.index("clipboard.writeText(")
        self.assertTrue(start < write < end,
                        "the only clipboard write is outside copyText, so its result is not "
                        "reported to anyone")

    def test_the_copier_reports_both_outcomes(self) -> None:
        """The failure branch is the one that was missing; assert it is present and distinct."""
        source = page_source()
        start = source.index("function copyText(")
        end = source.index("// ---- create", start)
        body = source[start:end]
        self.assertIn("Copy failed", body)
        self.assertIn("settled(false)", body,
                      "nothing drives the failure branch, so it cannot be reached")


if __name__ == "__main__":
    unittest.main()
