#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The strip says how many settings are waiting for a restart.

The per-field badge lands on the Setup page — where somebody goes *to change* a setting, not where
they go afterwards and not where they go for anything else. A deployment running configuration it
has been told to stop running is a fact about the whole deployment, and the strip is the one
element on every page.

Cheap deliberately. `snapshot()` builds the whole catalogue, and the frame already spends most of
itself taking one integer out of `_model_config_snapshot()`; a second document build every tick
would cost more than the thing it reports. `pending_restart_keys()` walks the settings list
comparing strings, and that it does *not* go through `snapshot()` is asserted rather than assumed
— by making `snapshot()` raise and requiring the answer anyway.

Nothing is drawn when nothing is waiting. A strip reading "0 awaiting restart" on every healthy
deployment forever is a place the reader learns to skip, and the real number would appear there.

The rendering is exercised by feeding the strip a frame, because `render()` is wrapped by its
caller in a catch that ignores: a segment reaching for a helper that block does not define throws,
is swallowed, and looks exactly like a healthy deployment with nothing waiting.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock

import matrixark_gateway_config as cfg
import matrixark_v1_gateway as gw

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "waiting_segment_harness.js")

RESTART_KEY = "extraction.base_url"


class TheCheapAnswerIsTheSameAnswerTest(unittest.TestCase):

    def setUp(self) -> None:
        self._saved = dict(os.environ)
        self.addCleanup(lambda: (os.environ.clear(), os.environ.update(self._saved)))
        self._boot = dict(cfg._BOOT_EFFECTIVE)
        self.addCleanup(lambda: (cfg._BOOT_EFFECTIVE.clear(),
                                 cfg._BOOT_EFFECTIVE.update(self._boot)))
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = os.path.join(tmp.name, "runtime.json")
        cfg.apply_boot()

    def test_it_agrees_with_the_catalogue_before_any_write(self) -> None:
        self.assertEqual(cfg.snapshot()["pending_restart"], cfg.pending_restart_keys())

    def test_it_agrees_with_the_catalogue_after_one(self) -> None:
        cfg.update({RESTART_KEY: "https://api.deepseek.com/v1"})
        self.assertEqual([RESTART_KEY], cfg.pending_restart_keys())
        self.assertEqual(cfg.snapshot()["pending_restart"], cfg.pending_restart_keys())

    def test_it_does_not_go_through_the_expensive_path(self) -> None:
        """The whole reason it exists. Asserted by breaking the expensive path and requiring the
        answer anyway, rather than by timing it."""
        cfg.update({RESTART_KEY: "https://api.deepseek.com/v1"})
        with mock.patch.object(cfg, "snapshot",
                               side_effect=AssertionError("built the catalogue")):
            self.assertEqual([RESTART_KEY], cfg.pending_restart_keys())

    def test_without_a_boot_record_it_claims_nothing(self) -> None:
        cfg._BOOT_EFFECTIVE.clear()
        cfg.update({RESTART_KEY: "https://api.deepseek.com/v1"})
        self.assertEqual([], cfg.pending_restart_keys())


class TheFrameCarriesItTest(unittest.TestCase):

    def setUp(self) -> None:
        self._saved = dict(os.environ)
        self.addCleanup(lambda: (os.environ.clear(), os.environ.update(self._saved)))
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = os.path.join(tmp.name, "runtime.json")
        # The shared part is cached for a tick; start from nothing so this reads a fresh build.
        gw._reset_live_cache()
        self.addCleanup(gw._reset_live_cache)

    def test_the_shared_part_reports_a_count(self) -> None:
        parts = gw._shared_live_parts()
        self.assertIn("settings_waiting", parts)
        self.assertIsInstance(parts["settings_waiting"], int)

    def test_a_broken_registry_does_not_break_the_frame(self) -> None:
        """Every other part of this frame is wrapped the same way: a strip that stops arriving
        because one number could not be worked out is worse than a strip missing that number."""
        gw._reset_live_cache()
        with mock.patch.object(gw._gwconfig, "pending_restart_keys",
                               side_effect=OSError("unreadable")):
            parts = gw._shared_live_parts()
        self.assertEqual(0, parts["settings_waiting"])
        self.assertIn("traffic", parts, "the rest of the frame was lost with it")


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class TheStripDrawsItTest(unittest.TestCase):
    """render() is wrapped by its caller in a catch that ignores, so a throwing segment is silent
    and indistinguishable from a deployment with nothing waiting."""

    def _run(self, page):
        return subprocess.run(["node", HARNESS, os.path.join(PORTAL, page)],
                              capture_output=True, text=True, timeout=180)

    def test_it_works_on_a_generated_page(self) -> None:
        proc = self._run("overview_portal.html")
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_it_works_on_a_hand_maintained_page(self) -> None:
        """Those two get the strip injected rather than generated with it."""
        proc = self._run("api_key_portal.html")
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_nothing_waiting_takes_no_space(self) -> None:
        self.assertIn("ok   nothing waiting takes no space", self._run("setup_portal.html").stdout)

    def test_an_older_gateway_claims_nothing(self) -> None:
        """A frame without the field is not a frame reporting zero."""
        self.assertIn("ok   a frame from an older gateway claims nothing",
                      self._run("setup_portal.html").stdout)

    def test_one_reads_as_one(self) -> None:
        self.assertIn("ok   exactly one reads as singular", self._run("setup_portal.html").stdout)

    def test_the_rest_of_the_strip_survives_it(self) -> None:
        self.assertIn("ok   the other segments still render",
                      self._run("setup_portal.html").stdout)


if __name__ == "__main__":
    unittest.main()
