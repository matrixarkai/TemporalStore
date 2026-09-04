#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A setting that has been written and is not in effect says so.

The portal badges "needs restart" on every setting whose ``applies`` is ``restart``. That is a
property of the setting and it is true before anybody touches anything — advice, not status. What
nothing said was that a setting has *been* changed and this process is still running the old value.

``update()`` reports ``restart_required`` in its own response, so the person who made the change is
told once, at the moment they make it. Anybody arriving afterwards — the next operator, the same
one on Monday — sees a page describing a configuration the deployment is not running. Settings
frozen at import keep their startup value indefinitely, and nothing on any screen says so.

The value a restart-scoped setting actually took is knowable at exactly one moment: ``apply_boot``,
after the stored file has been folded into the environment. Recording it there turns "is this
pending?" into a comparison rather than a guess.

**Absence of a record means unknown, and unknown reports False.** A process that never called
``apply_boot`` has nothing to compare against, and answering "pending" there would put an alarm on
every deployment that merely imports this module.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest

import matrixark_gateway_config as cfg

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")

RESTART_KEY = "extraction.base_url"
LIVE_KEY = next(s.key for s in cfg.SETTINGS if s.applies == "live")


class APendingSettingIsReportedTest(unittest.TestCase):

    def setUp(self) -> None:
        self._saved = dict(os.environ)
        self.addCleanup(lambda: (os.environ.clear(), os.environ.update(self._saved)))
        self._boot = dict(cfg._BOOT_EFFECTIVE)
        self.addCleanup(lambda: (cfg._BOOT_EFFECTIVE.clear(),
                                 cfg._BOOT_EFFECTIVE.update(self._boot)))
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = os.path.join(tmp.name, "runtime.json")

    @staticmethod
    def _pending():
        return cfg.snapshot()["pending_restart"]

    def test_the_setting_this_is_about_is_still_restart_scoped(self) -> None:
        """If it became live, every check below would pass while testing nothing."""
        self.assertEqual("restart", cfg.SETTINGS_BY_KEY[RESTART_KEY].applies)


    def _change_it(self):
        """Write something that differs from whatever is in effect right now.

        A fixed string is not enough: another suite writes the same value into this same setting,
        and under discovery its environment outlives it, so the write can land as a no-op and the
        assertions below then describe a change that never happened.
        """
        field = next(f for group in cfg.snapshot()["groups"].values() for f in group
                     if f["key"] == RESTART_KEY)
        changed = (field["value"] or "https://example.invalid") + "/changed-by-this-test"
        self.assertNotIn(RESTART_KEY, cfg.pending_restart_keys(),
                         "something was already pending before this test wrote anything")
        cfg.update({RESTART_KEY: changed})
        return changed

    def test_a_fresh_boot_has_nothing_waiting(self) -> None:
        cfg.apply_boot()
        self.assertEqual([], self._pending())

    def test_a_write_to_a_restart_setting_is_reported_as_waiting(self) -> None:
        cfg.apply_boot()
        self._change_it()
        self.assertIn(RESTART_KEY, self._pending())

    def test_the_field_itself_carries_it(self) -> None:
        """The page renders per field, so the list alone would not reach the reader."""
        cfg.apply_boot()
        self._change_it()
        field = next(f for group in cfg.snapshot()["groups"].values() for f in group
                     if f["key"] == RESTART_KEY)
        self.assertTrue(field["pending_restart"])
        self.assertEqual("restart", field["applies"],
                         "the two are different facts and both have to survive")

    def test_a_restart_clears_it(self) -> None:
        cfg.apply_boot()
        self._change_it()
        self.assertIn(RESTART_KEY, self._pending())
        cfg.apply_boot()                       # what a restart does
        self.assertEqual([], self._pending(),
                         "it still reads as waiting after the restart that applied it")

    def test_a_live_setting_is_never_waiting(self) -> None:
        cfg.apply_boot()
        cfg.update({LIVE_KEY: cfg.SETTINGS_BY_KEY[LIVE_KEY].default or "1"})
        self.assertEqual([], self._pending())

    def test_without_a_boot_record_nothing_is_claimed(self) -> None:
        """A tool that imports this module has no idea what the serving process started with, and
        must not answer as though it did."""
        cfg._BOOT_EFFECTIVE.clear()
        cfg.update({RESTART_KEY: "https://example.invalid/whatever"})
        self.assertEqual([], self._pending())


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class ThePageShowsTheStateNotJustTheAdviceTest(unittest.TestCase):
    """Whether the two badges are mutually exclusive is behaviour: "needs restart" describes the
    kind of setting, "changed — restart to apply" describes this deployment now, and showing both
    says it twice while burying the half about today."""

    def _run(self):
        return subprocess.run(
            ["node", os.path.join(PORTAL, "setting_badge_harness.js"),
             os.path.join(PORTAL, "setup_portal.html")],
            capture_output=True, text=True, timeout=180)

    def test_the_badges_render_as_intended(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_a_written_setting_shows_the_state(self) -> None:
        self.assertIn("ok   a written one says it is waiting", self._run().stdout)

    def test_it_replaces_the_advice_rather_than_joining_it(self) -> None:
        self.assertIn("ok   and drops the advice, rather than showing both", self._run().stdout)

    def test_an_untouched_setting_keeps_the_advice(self) -> None:
        self.assertIn("ok   an untouched restart setting carries the advice", self._run().stdout)


if __name__ == "__main__":
    unittest.main()
