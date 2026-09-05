#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A deployment that answers without a key gets a live portal.

The status strip refused to connect until somebody typed one::

    var k = key();
    if (!k) { setTimeout(open, 2000); return; }   /* inert without a key, like every page */

On the developer default -- ``MATRIXARK_REQUIRE_AUTH`` unset -- the gateway answers every admin
route anonymously. Measured against the app rather than assumed: ``/v1/admin/overview``, ``/config``,
``/scopes``, ``/api_key_usage``, ``/models`` and ``/deployment`` all return 200 with no
``Authorization`` header, and 401 once auth is on.

So on the deployment somebody is most likely to be looking at first, the strip stayed dark on all
seven panels -- and Overview and Setup, which run a connection of their own, stayed dark twice over
-- because the page would not ask a question the gateway was willing to answer.

Both halves have to hold and they pull against each other: ask when there is no key, and still watch
for a key on a deployment that wants one rather than backing off to thirty seconds. So the strip is
run against a gateway that answers and a gateway that refuses.

``startLive`` on the two pages with their own stream loses its key gate as well. It is reached from
``load()``'s success path, so by the time it runs the deployment has already answered -- the gate
meant a panel that had just loaded anonymously refused to keep itself live.
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
HARNESS = os.path.join(PORTAL, "open_deployment_harness.js")
HIDDEN = os.path.join(PORTAL, "hidden_tab_harness.js")


def pages() -> dict:
    return {name: io.open(os.path.join(PORTAL, name), encoding="utf-8").read()
            for name in sorted(os.listdir(PORTAL)) if name.endswith(".html")}


class NoPageRefusesToAskTest(unittest.TestCase):

    def test_the_old_refusal_is_gone_everywhere(self) -> None:
        """One comment, on seven pages, saying the thing that was wrong."""
        left = sorted(name for name, text in pages().items()
                      if "inert without a key, like every page" in text)
        self.assertEqual([], left, left)

    def test_the_page_streams_do_not_gate_on_the_key_box_either(self) -> None:
        """startLive runs only after load() succeeded, so a key check there refuses on behalf of a
        gateway that has already answered."""
        gated = []
        for name, text in pages().items():
            match = re.search(r"function startLive\(\) \{[\s\S]{0,200}", text)
            if match and '$("key").value.trim()' in match.group(0):
                gated.append(name)
        self.assertEqual([], gated, "panels still refusing to go live without a key: %r" % gated)

    def test_the_pages_that_run_their_own_stream_are_covered(self) -> None:
        """Without this the check above passes on a portal where nothing defines startLive."""
        own = [name for name, text in pages().items() if "function startLive()" in text]
        self.assertGreaterEqual(len(own), 2, own)


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class TheStripAsksFirstTest(unittest.TestCase):

    def _run(self, page_name):
        return subprocess.run(["node", HARNESS, os.path.join(PORTAL, page_name)],
                              capture_output=True, text=True, timeout=300)

    def test_every_panel_connects_without_a_key(self) -> None:
        for name in sorted(pages()):
            with self.subTest(page=name):
                proc = self._run(name)
                self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
                self.assertIn("ok   with no key it still asks", proc.stdout)

    def test_it_does_not_invent_a_credential(self) -> None:
        for name in sorted(pages()):
            with self.subTest(page=name):
                self.assertIn("ok   and sends no credential it does not have", self._run(name).stdout)

    def test_a_deployment_that_wants_a_key_is_still_watched_for_one(self) -> None:
        """The half that pulls the other way. Backing off to thirty seconds here would mean a key
        typed into the box takes half a minute to do anything."""
        for name in sorted(pages()):
            with self.subTest(page=name):
                out = self._run(name).stdout
                self.assertIn("ok   a deployment that refuses is asked once", out)
                self.assertIn("ok   and is then watched for a key, not backed off from", out)

    def test_a_key_is_still_sent_when_there_is_one(self) -> None:
        for name in sorted(pages()):
            with self.subTest(page=name):
                self.assertIn("ok   a key is still sent when there is one", self._run(name).stdout)


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class TheHiddenTabRuleSurvivesTest(unittest.TestCase):
    """This change rewrites the same function that lets go of a hidden tab's connection."""

    def _run(self, page_name, mode):
        return subprocess.run(["node", HIDDEN, os.path.join(PORTAL, page_name), mode],
                              capture_output=True, text=True, timeout=300)

    def test_the_strip_still_lets_go(self) -> None:
        for name, text in sorted(pages().items()):
            if '__matrixarkLive = "page"' in text:
                continue
            with self.subTest(page=name):
                proc = self._run(name, "strip")
                self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_the_page_streams_still_let_go(self) -> None:
        for name, text in sorted(pages().items()):
            if '__matrixarkLive = "page"' not in text:
                continue
            with self.subTest(page=name):
                proc = self._run(name, "page")
                self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)


if __name__ == "__main__":
    unittest.main()
