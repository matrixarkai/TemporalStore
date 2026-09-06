#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A stored configuration that is not being applied says so.

``load()`` returns the same empty document for four different situations: no file, a file it
cannot read, a file that is not JSON, and JSON that is not the right shape. That is deliberate and
right -- the docstring says so -- because a deployment must start whatever state that file is in.

But only one of the four is normal. In the other three every value in the file is being ignored and
the deployment is running built-in defaults, and on the screen that is indistinguishable from a
deployment nobody has configured yet. The settings page shows the resolved values either way, so an
operator whose file was truncated mid-write reads a page full of defaults as their configuration.

``config_file_status()`` separates them. It is read on its own rather than reported by ``load()``,
so the hot path keeps its one job and its promise never to raise.
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import tempfile
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402


class Case(unittest.TestCase):
    """Each test gets its own directory and the real config file is never in reach."""

    def setUp(self) -> None:
        self._environ = dict(os.environ)
        self.addCleanup(self._restore)
        self._work = tempfile.TemporaryDirectory(prefix="matrixark-status-")
        self.addCleanup(self._work.cleanup)
        self.path = os.path.join(self._work.name, "runtime_config.json")
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = self.path

    def _restore(self) -> None:
        os.environ.clear()
        os.environ.update(self._environ)

    def write(self, text: str) -> None:
        with io.open(self.path, "w", encoding="utf-8") as handle:
            handle.write(text)


class TheFourStatesAreToldApartTest(Case):

    def test_no_file_is_normal_and_nothing_is_being_ignored(self) -> None:
        status = cfg.config_file_status()
        self.assertEqual("absent", status["state"])
        self.assertTrue(status["applied"],
                        "a fresh deployment must not be reported as having a problem")

    def test_a_good_file_is_applied(self) -> None:
        cfg.update({"retrieval.min_score": "0.35"}, actor="test")
        status = cfg.config_file_status()
        self.assertEqual("ok", status["state"])
        self.assertTrue(status["applied"])
        self.assertEqual(1, status["settings"])

    def test_a_truncated_file_is_not(self) -> None:
        self.write('{"values": {"retrieval.min_score": "0.35"')
        status = cfg.config_file_status()
        self.assertEqual("unparsable", status["state"])
        self.assertFalse(status["applied"])
        self.assertTrue(status["detail"], "an operator cannot repair what is not described")

    def test_json_of_the_wrong_shape_is_not(self) -> None:
        self.write('["not", "an", "object"]')
        status = cfg.config_file_status()
        self.assertEqual("wrong_shape", status["state"])
        self.assertFalse(status["applied"])

    def test_nor_is_a_values_key_that_is_not_an_object(self) -> None:
        """The shape `load()` silently repairs: it replaces a non-dict `values` with {}, so every
        stored setting disappears while the document still parses."""
        self.write('{"values": ["a", "b"]}')
        status = cfg.config_file_status()
        self.assertEqual("wrong_shape", status["state"])
        self.assertFalse(status["applied"])

    def test_a_file_that_cannot_be_read_is_not(self) -> None:
        """A directory where the file should be. Deliberately NOT chmod 000: these tests run as
        root often enough that a permission bit proves nothing -- root reads a 0000 file, and the
        case would pass while testing the branch above it instead."""
        os.mkdir(self.path)
        status = cfg.config_file_status()
        self.assertEqual("unreadable", status["state"])
        self.assertFalse(status["applied"])
        self.assertIn("directory", (status.get("detail") or "").lower())

    def test_every_state_names_the_file(self) -> None:
        """Whatever went wrong, the operator has to be told where."""
        self.write("{}")
        for setup in (lambda: None, lambda: self.write("nonsense")):
            setup()
            self.assertEqual(self.path, cfg.config_file_status()["path"])


class TheDistinctionIsReachableFromTheScreenTest(Case):

    def test_the_snapshot_carries_it(self) -> None:
        self.write("nonsense")
        status = cfg.snapshot()["config_file_status"]
        self.assertEqual("unparsable", status["state"])
        self.assertFalse(status["applied"])

    def test_and_a_healthy_deployment_reports_applied(self) -> None:
        cfg.update({"retrieval.min_score": "0.35"}, actor="test")
        self.assertTrue(cfg.snapshot()["config_file_status"]["applied"])

    def test_the_settings_look_identical_either_way(self) -> None:
        """The premise. If a corrupt file produced a visibly different settings document there
        would be nothing to report, and this whole file would be decoration."""
        self.write("nonsense")
        broken = cfg.snapshot()
        os.unlink(self.path)
        absent = cfg.snapshot()
        self.assertEqual(json.dumps(broken["groups"], sort_keys=True),
                         json.dumps(absent["groups"], sort_keys=True),
                         "a corrupt file already renders differently; check what changed")


class ThePageSaysSoTest(unittest.TestCase):

    HARNESS = os.path.join(PORTAL, "config_not_applied_harness.js")
    PAGE = os.path.join(PORTAL, "setup_portal.html")

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_the_shipped_page_draws_the_banner(self) -> None:
        out = subprocess.run(["node", self.HARNESS, self.PAGE],
                             capture_output=True, text=True, timeout=600)
        self.assertIn("all ok", out.stdout + out.stderr)


if __name__ == "__main__":
    unittest.main()
