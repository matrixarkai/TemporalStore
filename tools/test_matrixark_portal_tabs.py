#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The portal's tabs, driven rather than read.

Tabs are the one part of a page whose source says least about it. Markup declaring `role="tab"` on
a button wired to nothing is indistinguishable, by inspection, from markup whose tabs work -- the
overrides panel on the key portal shipped broken three separate times on exactly that, each time
loading clean and failing on click. So the tab code is executed here against the tabs and panes
read out of the real generated page.

Two properties beyond "it switches":

* **Nothing was lost.** The setup page had twelve sections in one column before they were grouped
  into four panes. A regrouping that silently drops one is the obvious way this goes wrong, and it
  would look fine on screen -- the remaining tabs still work.
* **One implementation.** The explore page had tabs first, with its switching code inline. This
  runs the same harness against both pages, so a helper that works on one and not the other fails
  here rather than in front of someone.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "tabs_harness.js")

TABBED = ["setup_portal.html", "explore_portal.html", "api_key_portal.html"]

# Every heading the setup page carried before it was grouped into panes.
SETUP_HEADINGS = [
    "Access", "Start from a provider", "Models", "Retrieval settings", "Recent changes",
    "Move this configuration", "Encoding", "Endpoint test", "Traffic",
    "Where this deployment lives", "Launch a deployment", "Grafana",
]


def page(name: str) -> str:
    with open(os.path.join(PORTAL, name), encoding="utf-8") as handle:
        return handle.read()


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class TheTabsWorkTest(unittest.TestCase):

    def _run(self, name: str):
        return subprocess.run(["node", HARNESS, os.path.join(PORTAL, name)],
                              capture_output=True, text=True, timeout=180)

    def test_every_tabbed_page_passes(self) -> None:
        for name in TABBED:
            with self.subTest(page=name):
                proc = self._run(name)
                self.assertEqual(0, proc.returncode,
                                 "%s: %s%s" % (name, proc.stdout, proc.stderr))

    def test_the_keyboard_is_actually_exercised(self) -> None:
        """A tablist answering only to the mouse is the state this change was made to leave."""
        out = self._run("setup_portal.html").stdout
        for what in ("ArrowRight moves to the next tab",
                     "ArrowLeft from the first tab wraps to the last",
                     "Home jumps to the first tab"):
            self.assertIn("ok   " + what, out, out)

    def test_a_meaningful_number_of_checks_run(self) -> None:
        out = self._run("setup_portal.html").stdout
        checked = sum(1 for line in out.splitlines() if line.startswith("ok "))
        self.assertGreaterEqual(checked, 20, "only %d checks ran, so passing says little" % checked)


class NothingWasLostInTheRegroupingTest(unittest.TestCase):

    def setUp(self) -> None:
        self.text = page("setup_portal.html")

    def test_every_section_survived(self) -> None:
        for heading in SETUP_HEADINGS:
            self.assertIn(heading, self.text,
                          "the setup page no longer mentions %r, so grouping into tabs dropped it"
                          % heading)

    def test_every_section_is_inside_a_pane(self) -> None:
        """A section left outside the panes is visible on every tab, which is not a tab at all."""
        outside = self.text.split('<section class="pane"', 1)[0]
        self.assertNotIn("<h2>", outside,
                         "a section sits above the first pane and will show on every tab")

    def test_the_two_deployment_sections_are_told_apart(self) -> None:
        """Both were called Deployment: one is read-only, one composes a launch."""
        self.assertIn("Where this deployment lives", self.text)
        self.assertIn("Launch a deployment", self.text)
        self.assertNotIn("<h2>Deployment</h2>", self.text,
                         "two sections still carry the same heading")


class OnlyPagesWithTabsCarryTheHelperTest(unittest.TestCase):
    """A page without tabs should be untouched by this, and stay that way."""

    def test_the_helper_travels_with_the_tablist(self) -> None:
        for name in os.listdir(PORTAL):
            if not name.endswith(".html"):
                continue
            with self.subTest(page=name):
                text = page(name)
                has_tabs = 'role="tablist"' in text
                has_helper = "window.wireTabs" in text
                self.assertEqual(has_tabs, has_helper,
                                 "%s: tablist=%s but helper=%s" % (name, has_tabs, has_helper))

    def test_there_is_exactly_one_implementation(self) -> None:
        """Two copies of a switcher is how two pages that look alike stop behaving alike."""
        for name in TABBED:
            text = page(name)
            self.assertEqual(1, text.count("window.wireTabs = function"),
                             "%s carries more than one tab implementation" % name)
            self.assertNotIn('Array.prototype.forEach.call(document.querySelectorAll(".tabs button")',
                             text, "%s still has its own inline copy of the switcher" % name)


class NoSentencePointsAcrossATabTest(unittest.TestCase):
    """Grouping a page into panes invalidates every "above" and "below" that crosses one.

    Targeted rather than general: these four were found by mapping each line to its pane, and a
    rule broad enough to catch any directional word would flag every legitimate "the table below"
    within a single pane. What this stops is these four coming back.
    """

    def setUp(self) -> None:
        self.text = page("setup_portal.html")

    def test_the_known_crossings_stay_fixed(self) -> None:
        for phrase, why in [
            ("not editable above", "the storage fields are on the Settings tab"),
            ("entered above", "secret values are on the Settings tab"),
            ("key above", "the admin key field is on the Access tab"),
            ("configuration below", "the save bar is on the Settings tab"),
        ]:
            self.assertNotIn(phrase, self.text, "%r points across a tab: %s" % (phrase, why))

    def test_a_write_into_another_pane_takes_you_there(self) -> None:
        """Picking a model sets a field on the Settings tab; without this it lands unseen."""
        self.assertIn('showTab("settings")', self.text,
                      "the model picker writes a settings field from another tab and never "
                      "shows it, so the change and its save bar stay invisible")


class TheHandMaintainedPagesGetTheHelperTest(unittest.TestCase):
    """The key portal is not generated, so the builder injects the helper the way it injects nav.

    Two things can go wrong with an injector that runs on every build: it can stack, defining the
    helper once per build, or it can reach a page that has no tabs.
    """

    def test_the_key_portal_has_exactly_one_copy(self) -> None:
        self.assertEqual(1, page("api_key_portal.html").count("window.wireTabs = function"),
                         "the builder stacked the helper, so it is defined more than once")

    def test_a_page_without_tabs_is_left_alone(self) -> None:
        text = page("ingestion_portal.html")
        self.assertNotIn('role="tablist"', text, "this test is about a page that has no tabs")
        self.assertNotIn("wireTabs", text,
                         "the helper was injected into a page with nothing to switch")

    def test_the_helper_is_defined_before_it_is_called(self) -> None:
        """Scripts run in document order. Appended last, the helper is undefined when called."""
        for name in TABBED:
            with self.subTest(page=name):
                text = page(name)
                defined = text.index("window.wireTabs = function")
                called = text.index("window.wireTabs(")
                self.assertLess(defined, called,
                                "%s calls the tab helper before the block defining it, which "
                                "throws on load and stops the rest of that script" % name)

    def test_the_connection_step_is_not_behind_a_tab(self) -> None:
        """Every pane needs a key first. A step you must take before any tab works is not a tab."""
        text = page("api_key_portal.html")
        above = text.split('<div class="tabs"', 1)[0]
        self.assertIn("Connection", above,
                      "the connection panel moved behind a tab, so the other tabs look broken "
                      "until you find it")


if __name__ == "__main__":
    unittest.main()
