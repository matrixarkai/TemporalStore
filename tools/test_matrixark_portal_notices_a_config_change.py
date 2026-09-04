#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""An open portal notices a configuration change made somewhere else.

The overview page re-read its checklist on a blind sixty-second timer. Someone changing a setting
in another tab -- or a colleague changing one from another machine -- left every other open portal
telling people the old answer for up to a minute, including the rows that exist precisely to say
what is misconfigured.

The stored configuration already recorded when it was last written, and the metrics endpoint
published it. The frame did not carry it, so the page had nothing to react to and no choice but to
poll.

Two halves that fail differently. Whether the GATEWAY puts the timestamp on the frame is Python.
Whether the PAGE re-reads on a change, and only on a change, can only be seen by running it -- a
page that re-read on every frame would look almost identical in source and would cost three backend
listings every two seconds.
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")


def _helpers():
    """Deferred: importing a test module from a test module reorders `unittest discover`."""
    from test_matrixark_v1_gateway import _cfg, _FakeServer
    return _cfg, _FakeServer


class TheFrameCarriesWhenConfigChangedTest(unittest.TestCase):

    def setUp(self) -> None:
        self.cfg, self.FakeServer = _helpers()
        gw._reset_live_cache()
        self.addCleanup(gw._reset_live_cache)

    def _frame(self):
        return asyncio.run(gw._event_frame(self.FakeServer(), self.cfg(),
                                           "k", "acme", None, None))

    def test_the_field_is_on_the_frame(self) -> None:
        self.assertIn("config_changed_at", self._frame())

    def test_it_reports_what_the_stored_configuration_says(self) -> None:
        """Isolated, and the isolation is asserted: this box has a real config file with real
        deployment keys in it, and a test that wrote there once already cost an afternoon."""
        import matrixark_gateway_config as cfgmod
        directory = tempfile.mkdtemp()
        before = os.environ.get("MATRIXARK_RUNTIME_CONFIG_FILE")
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = os.path.join(directory, "runtime.json")
        self.addCleanup(shutil.rmtree, directory, True)
        if before is None:
            self.addCleanup(os.environ.pop, "MATRIXARK_RUNTIME_CONFIG_FILE", None)
        else:
            self.addCleanup(os.environ.__setitem__, "MATRIXARK_RUNTIME_CONFIG_FILE", before)

        self.assertEqual(os.path.join(directory, "runtime.json"), cfgmod.config_path(),
                         "the config path did not move, so this test would write the real one")
        with open(cfgmod.config_path(), "w", encoding="utf-8") as handle:
            json.dump({"updated_at": 1730000000.5, "values": {}}, handle)
        gw._reset_live_cache()
        self.assertEqual(1730000000.5, self._frame()["config_changed_at"])

    def test_it_is_shared_rather_than_built_per_viewer(self) -> None:
        """Deployment-wide: one write, the same answer for everyone watching."""
        gw._reset_live_cache()
        parts = gw._shared_live_parts()
        self.assertIn("config_changed_at", parts)

    def test_it_carries_no_values(self) -> None:
        """The fact and the time. The settings stay behind the admin-gated read."""
        value = self._frame()["config_changed_at"]
        self.assertTrue(value is None or isinstance(value, float), repr(value))


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class ThePageReactsOnlyToAChangeTest(unittest.TestCase):
    """Re-reading costs three backend listings, so "reacts to a change" and "reacts to every
    frame" are very different behaviours that look nearly identical in source."""

    def _run(self):
        return subprocess.run(
            ["node", os.path.join(PORTAL, "config_change_harness.js"),
             os.path.join(PORTAL, "overview_portal.html")],
            capture_output=True, text=True, timeout=180)

    def test_it_re_reads_when_the_configuration_changes(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_an_unchanged_configuration_costs_nothing(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   an unchanged configuration does not cause a re-read", out, out)

    def test_the_first_frame_is_not_treated_as_a_change(self) -> None:
        """On load the checklist has just been read; reacting to the first sighting re-reads it
        immediately for nothing."""
        out = self._run().stdout
        self.assertIn("ok   an unchanged configuration does not cause a re-read", out, out)
        self.assertIn("ok   the page read its checklist on load", out, out)

    def test_a_frame_without_the_field_changes_nothing(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   a frame without the field changes nothing", out, out)


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class TheSetupPageDoesNotClobberEditsTest(unittest.TestCase):
    """The Setup page holds an editable form, so reacting to someone else's change is not simply
    "reload".

    With nothing unsaved, taking their change is right -- the form is showing stale values and
    there is nothing to lose. With unsaved edits, reloading would discard what the person here has
    typed, which is the case two operators on one deployment actually hit. Both paths run through
    the same handler and differ by one condition, which is the kind of thing that reads correct and
    behaves wrong.
    """

    def _run(self):
        return subprocess.run(
            ["node", os.path.join(PORTAL, "setup_config_harness.js"),
             os.path.join(PORTAL, "setup_portal.html")],
            capture_output=True, text=True, timeout=180)

    def test_both_paths_behave(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_with_nothing_unsaved_it_takes_the_change(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   with nothing unsaved, a change elsewhere is taken", out, out)

    def test_with_unsaved_edits_it_does_not_reload_over_them(self) -> None:
        """The one that loses work if it goes wrong."""
        out = self._run().stdout
        self.assertIn("ok   with unsaved edits, the page does NOT reload over them", out, out)

    def test_it_warns_that_saving_would_overwrite_the_other_change(self) -> None:
        """Knowing after you pressed save is not knowing."""
        out = self._run().stdout
        self.assertIn("ok   it warns that saving would overwrite theirs", out, out)

    def test_the_edit_path_is_actually_exercised(self) -> None:
        """If the form never recorded an edit, the check above would pass with nothing unsaved."""
        out = self._run().stdout
        self.assertIn("ok   the settings form records edits", out, out)


if __name__ == "__main__":
    unittest.main()
