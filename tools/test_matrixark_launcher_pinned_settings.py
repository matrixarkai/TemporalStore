#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A setting the launcher pins says so, including after someone changes it.

`apply_boot` does not overwrite a variable the operator's launcher set: a boot value keeps
precedence over the stored file. That is deliberate. It is also invisible, and invisible at the
worst moment -- a change made on the page applies immediately, and is then dropped at the next
restart when the launcher's value is used again.

The existing `source` field cannot carry this. It reports "environment" while the launcher's value
is live and flips to "portal" the instant a customer overrides it, so the one signal that something
is unusual about this field disappears exactly when the customer has done the thing that will be
undone.

Two of the engine settings are pinned this way by `deploy_profile_common.sh`, which does
`export TS_VECTOR_SCALED="${TS_VECTOR_SCALED:-1}"` -- so any deployment launched through it has
that variable in its boot environment.
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


class ALauncherPinnedSettingSaysSoTest(unittest.TestCase):

    def setUp(self) -> None:
        _isolate_config(self)
        self.setting = next(s for s in cfgmod.SETTINGS if s.env == "TS_VECTOR_SCALED")
        self._boot = dict(cfgmod._BOOT_ENV)
        self._env = dict(os.environ)
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        cfgmod._BOOT_ENV.clear()
        cfgmod._BOOT_ENV.update(self._boot)
        os.environ.clear()
        os.environ.update(self._env)

    def _entry(self, env_name: str):
        snapshot = cfgmod.snapshot()
        for items in snapshot["groups"].values():
            for item in items:
                if item.get("env") == env_name:
                    return item
        return None

    def test_a_pinned_setting_is_flagged(self) -> None:
        cfgmod._BOOT_ENV["TS_VECTOR_SCALED"] = "1"
        os.environ["TS_VECTOR_SCALED"] = "1"
        entry = self._entry("TS_VECTOR_SCALED")
        self.assertIsNotNone(entry, "the setting is not offered at all")
        self.assertTrue(entry["boot_pinned"],
                        "the launcher set this variable and the page does not say so")

    def test_the_flag_survives_a_customer_override(self) -> None:
        """The case the source badge cannot cover."""
        cfgmod._BOOT_ENV["TS_VECTOR_SCALED"] = "1"
        os.environ["TS_VECTOR_SCALED"] = "0"          # the customer changed it on the page
        entry = self._entry("TS_VECTOR_SCALED")
        self.assertEqual("environment", entry["source"],
                         "sanity: an overridden boot value still reports its origin")
        self.assertTrue(
            entry["boot_pinned"],
            "after a customer overrides a launcher-set value the warning disappeared, which is "
            "precisely when they need it: their change is dropped at the next restart")

    def test_an_unpinned_setting_is_not_flagged(self) -> None:
        cfgmod._BOOT_ENV.pop("TS_VECTOR_INT8", None)
        os.environ.pop("TS_VECTOR_INT8", None)
        entry = self._entry("TS_VECTOR_INT8")
        self.assertFalse(entry["boot_pinned"],
                         "a setting the launcher does not set was flagged, which would train a "
                         "customer to ignore the badge")

    def test_the_flag_is_present_on_every_setting(self) -> None:
        """A missing key reads as false in the page's template, so absence must not be possible."""
        missing = []
        for items in cfgmod.snapshot()["groups"].values():
            for item in items:
                if "boot_pinned" not in item:
                    missing.append(item.get("key"))
        self.assertEqual([], missing,
                         "these entries carry no boot_pinned field: %s" % ", ".join(missing[:8]))


if __name__ == "__main__":
    unittest.main()
