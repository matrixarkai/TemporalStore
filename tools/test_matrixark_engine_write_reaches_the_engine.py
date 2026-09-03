#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A portal write of an engine knob reaches the environment the engine reads.

The live/restart label is checked structurally elsewhere: the accessors read per call, nothing
caches, so the knob CAN take effect without a restart. That is a statement about the engine, not
about the portal, and it holds equally well if `update` never writes the variable at all.

This tests the other half, which is the half a customer experiences: write a setting through the
same entry point the Setup page uses, then read the process environment the engine reads from. Both
sides are needed -- a knob read per call from a variable nobody sets is exactly as dead as one
captured at import.

Scoped to the engine group because that is the group whose variables belong to another runtime; the
Python-side knobs are covered by the behavioural tests next door, which assert what gets stored.
"""
from __future__ import annotations

import os
import shutil
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_config as cfgmod  # noqa: E402


def _isolate_config(case) -> None:
    """Point this test at a throwaway config file, and prove it landed there.

    `cfgmod.config_path()` resolves to ~/.matrixark/runtime_config.json unless
    MATRIXARK_RUNTIME_CONFIG_FILE says otherwise, and `update()` PERSISTS. Without this a test that
    writes a setting rewrites the config of whatever deployment shares that home directory -- which
    is exactly what happened when this file was first written: sixteen engine keys with probe
    values landed in a live config on a shared machine.

    The assertion is the point. Setting the variable is easy to do and easy to get wrong; checking
    that `config_path()` actually moved is what makes the isolation real rather than intended.
    """
    import tempfile

    directory = tempfile.mkdtemp(prefix="matrixark-config-test-")
    path = os.path.join(directory, "runtime_config.json")
    case._saved_config = os.environ.get("MATRIXARK_RUNTIME_CONFIG_FILE")
    os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = path
    resolved = cfgmod.config_path()
    if resolved != path:
        raise AssertionError(
            "config isolation failed: config_path() is %r, not the temporary file %r. Refusing to "
            "run, because this test writes settings and would rewrite a real deployment's config."
            % (resolved, path))
    home = os.path.join(os.path.expanduser("~"), ".matrixark")
    if resolved.startswith(home):
        raise AssertionError("config isolation resolved inside %s" % home)

    def restore():
        if case._saved_config is None:
            os.environ.pop("MATRIXARK_RUNTIME_CONFIG_FILE", None)
        else:
            os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = case._saved_config
        shutil.rmtree(directory, ignore_errors=True)

    case.addCleanup(restore)


class APortalWriteReachesTheEngineTest(unittest.TestCase):

    def setUp(self) -> None:
        _isolate_config(self)
        self.engine = [s for s in cfgmod.SETTINGS if s.env and s.env.startswith("TS_")]
        self.assertGreaterEqual(len(self.engine), 10,
                                "almost no engine settings are offered, so this proves little")
        self._saved = dict(os.environ)
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        os.environ.clear()
        os.environ.update(self._saved)

    def _write(self, setting, value: str) -> None:
        """Through `update`, the same entry point the Setup page posts to."""
        cfgmod.update({setting.key: value}, actor="test")

    def test_every_engine_setting_lands_in_the_environment(self) -> None:
        landed, missed = 0, []
        for setting in self.engine:
            probe = "0" if setting.kind == "bool" else "4096"
            os.environ.pop(setting.env, None)
            try:
                self._write(setting, probe)
            except Exception as exc:  # pragma: no cover - a rejected write is reported, not raised
                missed.append("%s (%s) rejected the write: %s" % (setting.key, setting.env, exc))
                continue
            actual = os.environ.get(setting.env)
            if actual != probe:
                missed.append("%s (%s): wrote %r, environment holds %r"
                              % (setting.key, setting.env, probe, actual))
            else:
                landed += 1
        self.assertEqual(
            [], missed,
            "a portal write of these did not reach the variable the engine reads, so the setting "
            "does nothing however it is labelled: %s" % "; ".join(missed))
        self.assertGreaterEqual(landed, 10,
                                "only %d engine writes were verified" % landed)

    def test_clearing_a_setting_removes_the_variable(self) -> None:
        """Otherwise a customer cannot get back to the engine's own default from the page."""
        setting = next(s for s in self.engine if s.kind == "int")
        self._write(setting, "4096")
        self.assertEqual("4096", os.environ.get(setting.env))
        self._write(setting, "")
        self.assertIsNone(
            os.environ.get(setting.env),
            "%s stayed in the environment after being cleared, so the engine keeps using the last "
            "value instead of falling back to its own default" % setting.env)

    def test_the_write_reports_it_is_in_effect(self) -> None:
        """The page tells the customer whether the change is live; that claim is checked here."""
        setting = next(s for s in self.engine if s.kind == "bool")
        result = cfgmod.update({setting.key: "0"}, actor="test")
        applied = {row["key"]: row for row in (result or {}).get("applied", [])}
        self.assertIn(setting.key, applied, "the write was not reported as applied at all")
        self.assertTrue(
            applied[setting.key].get("in_effect"),
            "%s is labelled %r, so the page tells the customer the change is already live"
            % (setting.key, setting.applies))


if __name__ == "__main__":
    unittest.main()
