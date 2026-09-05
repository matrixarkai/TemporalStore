#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The ingestion panel notices the configuration changing underneath it.

That panel renders what an import will actually use: the embedding provider and model, the encoder
endpoint, whether a failed encoder call errors or falls back to hash vectors, the extraction
provider, and whether the extraction key is set. Every one of those is set on the Setup page.

It read them once, on load, and never again -- so a provider changed on Setup left this panel
describing the old deployment for as long as the tab stayed open, under a header reading
"checked 14:32:01": true about when it looked, false about what it is showing.

Overview and Setup already watch ``config_changed_at`` on the live frame for exactly this. Ingestion
subscribed to no frames at all, so it had nothing to watch with.

The registration goes through ``__matrixarkFrameQueue`` rather than calling the register function:
this script runs before the nav block defines that function, and a direct call registers nothing.
The catalog page lost its own watcher to that and carries the comment.

Whether a watcher fires is behaviour, and every way it goes wrong is silent -- registering against
nothing, firing on the first frame, firing on every frame, firing for a tab nobody is looking at --
so the page's own registration is executed and the callback it hands over is driven with frames.
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
PAGE = os.path.join(PORTAL, "ingestion_portal.html")
HARNESS = os.path.join(PORTAL, "config_watch_harness.js")


def page() -> str:
    with io.open(PAGE, encoding="utf-8") as handle:
        return handle.read()


class ThePanelSubscribesTest(unittest.TestCase):

    def test_it_registers_a_frame_watcher(self) -> None:
        self.assertIn("__matrixarkFrameQueue", page())

    def test_through_the_queue_rather_than_the_register_function(self) -> None:
        """This script runs before the nav block defines the register function. Calling it
        directly registers nothing at all, and nothing says so -- the catalog page lost a watcher
        that way."""
        self.assertIn("(window.__matrixarkFrameQueue = window.__matrixarkFrameQueue || []).push",
                      page())

    def test_it_watches_the_field_that_marks_a_configuration_write(self) -> None:
        self.assertIn("config_changed_at", page())

    def test_the_panel_it_refreshes_is_the_one_showing_configuration(self) -> None:
        """Named so this cannot drift into watching for a change and refreshing something else."""
        source = page()
        watcher = source[source.index("var lastConfigAt = null;"):]
        watcher = watcher[:watcher.index("});") + 3]
        self.assertIn("loadConfig(", watcher)


class TheStampSaysWhichItWasTest(unittest.TestCase):

    def test_a_re_read_is_distinguishable_from_a_look(self) -> None:
        """"checked" is about when the panel looked. After somebody else changes the deployment it
        is showing a different table, and saying only when it looked is the quiet half of that."""
        self.assertIn("changed elsewhere", page())

    def test_the_ordinary_load_still_says_checked(self) -> None:
        self.assertIn('"checked "', page())


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class WhatTheWatcherActuallyDoesTest(unittest.TestCase):

    def _run(self):
        return subprocess.run(["node", HARNESS, PAGE], capture_output=True, text=True, timeout=300)

    def test_the_harness_passes(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_the_first_frame_is_not_treated_as_a_change(self) -> None:
        """On load the configuration has just been read; re-reading on the first sighting would be
        a request for nothing, on every page load."""
        out = self._run().stdout
        self.assertIn("ok   the first frame does not trigger a re-read", out)
        self.assertIn("ok   an unchanged value does not trigger one either", out)

    def test_a_change_is(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   a changed value re-reads the configuration", out)
        self.assertIn("ok   and the re-read is marked as caused by a change elsewhere", out)

    def test_and_only_once_per_change(self) -> None:
        """The frame repeats while nothing moves; re-reading on each would be a request every few
        seconds for a table nobody asked to refresh."""
        self.assertIn("ok   the same value again does not re-read", self._run().stdout)

    def test_a_hidden_tab_is_left_alone_but_not_forgotten(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   a hidden tab does not re-read", out)
        self.assertIn("ok   and it re-reads once the tab is looked at again", out)

    def test_it_does_not_ask_without_a_key(self) -> None:
        self.assertIn("ok   with no key it does not ask", self._run().stdout)


if __name__ == "__main__":
    unittest.main()
