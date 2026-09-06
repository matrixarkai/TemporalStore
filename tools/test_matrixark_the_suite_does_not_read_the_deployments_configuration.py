# SPDX-License-Identifier: Apache-2.0
"""The suite must not read the configuration of the machine it runs on.

`matrixark_gateway_config.config_path()` falls back to ~/.matrixark/runtime_config.json. On a
configured box that file holds real values -- 115 of them on the deployment this was found on,
including `audit.mode` and `behaviour.top_k_per_layer`. `apply_boot()` seeds them into the process
environment when the gateway app is constructed. It is right to do so and it declines, correctly,
to overwrite anything already present at boot; but a test sets its variables long after boot, so
the stored value wins and the assertion is made against the machine rather than against the change.

CI has no such file, so the suite was green there and red on any real deployment -- four failures
that looked like a regression on main and were not. A gate whose answer depends on the box it runs
on is not reporting on the change.

Two separate holes produced those four, and each is guarded here:

  * the runtime config file was read at all -- the ratchet now runs the suite against a path that
    does not exist, and the three suites that manipulate these variables pin their own;
  * the portal suite's environment isolation named four PREFIXES by hand. The registry declares
    two, MATRIXARK_ for 98 settings and TS_ for 19, and TS_ was not among the four -- so nineteen
    settings were never isolated at all. It is derived from the registry now.
"""

import io
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_config as cfgmod  # noqa: E402


class TheRatchetRunsAgainstNoStoredConfiguration(unittest.TestCase):

    def _captured_env(self) -> dict:
        """Run suite_result() with the subprocess replaced, and return the env it would use."""
        import run_python_suite_ratchet as ratchet

        seen = {}

        class _Result(object):
            stderr = ""
            stdout = "Ran 0 tests in 0.0s\n"

        def fake_run(*args, **kwargs):
            seen.update(kwargs.get("env") or {})
            seen["__passed_env__"] = "env" in kwargs
            return _Result()

        real = ratchet.subprocess.run
        ratchet.subprocess.run = fake_run
        try:
            ratchet.suite_result()
        finally:
            ratchet.subprocess.run = real
        return seen

    def test_it_passes_an_environment_at_all(self) -> None:
        # Without this the next test passes for the wrong reason: an inherited environment has
        # no key to look at, and `.get` on it returns None just as a pinned-but-wrong one would.
        self.assertTrue(self._captured_env().get("__passed_env__"),
                        "the suite subprocess inherits the environment unchanged")

    def test_the_config_file_is_pinned_somewhere_that_does_not_exist(self) -> None:
        pinned = self._captured_env().get("MATRIXARK_RUNTIME_CONFIG_FILE")
        self.assertTrue(pinned, "the suite runs against whatever config the box has")
        self.assertFalse(os.path.exists(pinned),
                         "the suite would read a real file at %s" % pinned)

    def test_it_is_not_the_deployment_path(self) -> None:
        pinned = self._captured_env().get("MATRIXARK_RUNTIME_CONFIG_FILE") or ""
        home = os.path.join(os.path.expanduser("~"), ".matrixark")
        self.assertFalse(os.path.abspath(pinned).startswith(os.path.abspath(home)),
                         "pinned inside the deployment's own directory")


class ThePortalIsolationIsDerivedFromTheRegistry(unittest.TestCase):

    @staticmethod
    def _declared() -> set:
        return {setting.env for setting in cfgmod.SETTINGS if setting.env}

    def test_the_registry_declares_more_than_one_prefix(self) -> None:
        # The floor that makes the rest meaningful. If every setting shared one prefix, a written
        # list would be as good as a derived one and none of this would matter.
        prefixes = {name.split("_", 1)[0] for name in self._declared()}
        self.assertGreater(len(prefixes), 1, sorted(prefixes))
        self.assertGreater(len(self._declared()), 100, len(self._declared()))

    def test_no_declared_variable_survives_the_portal_setup(self) -> None:
        import test_matrixark_gateway_portal as portal

        declared = self._declared()
        saved = dict(os.environ)
        try:
            for name in declared:
                os.environ[name] = "sentinel-from-the-machine"
            case = portal.ConfigExportTest("test_export_needs_a_key")
            case.setUp()
            try:
                survivors = sorted(n for n in declared
                                   if os.environ.get(n) == "sentinel-from-the-machine")
            finally:
                case.tearDown()
            self.assertEqual([], survivors,
                             "%d declared variables survived the isolation" % len(survivors))
        finally:
            os.environ.clear()
            os.environ.update(saved)

    def test_the_sentinel_check_can_see_a_survivor(self) -> None:
        # The assertion above is an equality against the empty list, which is exactly what a
        # detector that never looks at anything also returns. Prove it finds one when one is there.
        declared = self._declared()
        saved = dict(os.environ)
        try:
            for name in declared:
                os.environ[name] = "sentinel-from-the-machine"
            # A prefix list of the kind that was there before: it cannot reach the TS_ names.
            for name in list(os.environ):
                if name.startswith(("MATRIXARK_", "DEEPSEEK_", "OPENAI_", "LOCAL_ENCODER_")):
                    del os.environ[name]
            survivors = sorted(n for n in declared
                               if os.environ.get(n) == "sentinel-from-the-machine")
            self.assertTrue(survivors,
                            "a hand-written prefix list left nothing behind, so this test "
                            "proves nothing about the derived one")
            self.assertTrue(all(n.startswith("TS_") for n in survivors), survivors)
        finally:
            os.environ.clear()
            os.environ.update(saved)


class TheSuitesThatSetTheseVariablesPinTheirOwn(unittest.TestCase):
    """The ratchet covers CI. `python3 -m unittest <module>` is how anyone reads a failure, and
    it has to give the same answer on a configured box as on a bare one."""

    MODULES = (
        "test_matrixark_gateway_portal.py",
        "test_matrixark_user_policy.py",
        "test_matrixark_the_audit_log_can_be_read.py",
    )

    def test_each_one_pins_the_runtime_config_file(self) -> None:
        here = os.path.dirname(os.path.abspath(__file__))
        for name in self.MODULES:
            with io.open(os.path.join(here, name), encoding="utf-8") as handle:
                source = handle.read()
            self.assertIn("MATRIXARK_RUNTIME_CONFIG_FILE", source, name)
            self.assertIn("tempfile", source, name)


class OneSetUpLeavesTheProcessAsItFoundIt(unittest.TestCase):
    """Measured before the fix: 109 variables gained from a single setUp/tearDown.

    The saved-environment snapshot was taken after make_v1_app() had already called apply_boot(),
    so the snapshot contained the seeded values and tearDown wrote them back into the process.
    Every later test in that run then inherited a deployment's configuration from a suite that
    had been careful to clear it.

    A stored document is planted here so the check discriminates on a bare runner too: with no
    file to seed from there is nothing to leak and the test would pass without proving anything.
    """

    WATCHED = ("MATRIXARK_", "TS_")

    def _planted(self) -> str:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = os.path.join(directory.name, "runtime.json")
        with io.open(path, "w", encoding="utf-8") as handle:
            json.dump({"values": {"audit.mode": "async",
                                  "behaviour.top_k_per_layer": "8",
                                  "storage_engine.cold_scan_no_cache_fill": "1"}}, handle)
        return path

    def _watched(self) -> set:
        return {name for name in os.environ if name.startswith(self.WATCHED)}

    def test_the_plant_would_seed_something(self) -> None:
        """The control. If apply_boot cannot seed from this document, the next test is empty."""
        saved = dict(os.environ)
        try:
            os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = self._planted()
            cfgmod._BOOT_ENV.clear()
            for name in list(os.environ):
                if name.startswith(self.WATCHED) and name != "MATRIXARK_RUNTIME_CONFIG_FILE":
                    del os.environ[name]
            self.assertGreaterEqual(len(cfgmod.apply_boot()), 3)
        finally:
            os.environ.clear()
            os.environ.update(saved)
            cfgmod._BOOT_ENV.clear()
            cfgmod._BOOT_ENV.update(saved)

    def test_one_setup_and_teardown_gains_nothing(self) -> None:
        import test_matrixark_gateway_portal as portal

        saved = dict(os.environ)
        try:
            os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = self._planted()
            before = self._watched()
            case = portal.ConfigExportTest("test_export_needs_a_key")
            case.setUp()
            case.tearDown()
            gained = sorted(self._watched() - before)
            self.assertEqual([], gained,
                             "%d variables leaked into the process" % len(gained))
        finally:
            os.environ.clear()
            os.environ.update(saved)


if __name__ == "__main__":
    unittest.main()
