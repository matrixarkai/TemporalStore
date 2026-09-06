#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Exporting a configuration and importing it hands the clone the same settings.

`export_settings()` produces a patch body meant to be POSTed at another deployment. Nothing tested
the trip: export, import into a fresh config, and compare. These do, for every setting at once.

**What this does NOT establish.** It is tempting to say this catches a declared default that
disagrees with the build -- the defect several earlier fixes were about, since
`include_defaults=True` writes each declared default into the target as an explicit value. It does
not, and a mutation proved it: disabling `_apply_build_defaults` leaves these tests green, because
source and target compute effective values the same way, so a wrong default is wrong *identically*
on both sides. Whether a declared default matches the build is
`test_matrixark_the_portal_declares_the_budget_the_build_runs`'s question, and it stays there.

What these do catch, each shown by a mutation that reddens them: a secret exported instead of
omitted, an export that drops the settings nobody set, and a config path that stops honouring its
override.

**Isolation matters here.** These functions read and WRITE the runtime configuration file, and a
settings test in this repository has twice now written to a live one -- the second time because a
mutation disabled `config_path()`'s override and the suite was then run against it, which sent
every write to the real file. Every case points `MATRIXARK_RUNTIME_CONFIG_FILE` at a
`TemporaryDirectory` and asserts it took effect; that assertion is the one worth keeping, and
mutating the mechanism it guards is not a test worth running against a live box.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402

PATH_VARIABLE = "MATRIXARK_RUNTIME_CONFIG_FILE"


class Case(unittest.TestCase):
    """Each test gets its own directory, the real config file is never in reach, and the whole
    environment is put back afterwards.

    `update()` says it "applies to the live process environment unconditionally" -- a portal write
    is meant to take effect without a restart, so it sets `os.environ` as well as the file. In a
    test process that makes every `update()` here a change to the configuration every LATER test
    in the same process runs under. The first version of this suite restored only the config path
    and turned 229 sibling tests red in the ratchet: it did not fail, it reconfigured everything
    that came after it.

    So the whole environment is snapshotted and restored, not the one variable this suite sets.
    """

    def setUp(self) -> None:
        self._environ = dict(os.environ)
        self._work = tempfile.TemporaryDirectory(prefix="matrixark-clone-")
        self.addCleanup(self._work.cleanup)
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        os.environ.clear()
        os.environ.update(self._environ)

    def at(self, name: str) -> str:
        """Point the module at a config file of this name, and return the path."""
        path = os.path.join(self._work.name, name)
        os.environ[PATH_VARIABLE] = path
        return path

    def effective(self) -> dict:
        """What the deployment this file describes is actually running, per setting."""
        stored = {k: str(v) for k, v in (cfg.load().get("values") or {}).items()}
        return {s.key: cfg._effective(s, stored)[0] for s in cfg.SETTINGS}


class AConfiguredDeploymentClonesExactlyTest(Case):

    def test_what_was_set_comes_back_with_the_value_it_went_out_with(self) -> None:
        self.at("source.json")
        cfg.apply_preset("voyage", actor="test")
        cfg.update({"skills.shared_skill_budget_ratio": "0.30",
                    "retrieval.default_max_context_tokens": "250000"}, actor="test")
        sent = cfg.export_settings()["settings"]

        self.at("target.json")
        cfg.update(dict(sent), actor="test")
        back = cfg.export_settings()["settings"]

        self.assertEqual(sent, back)

    def test_the_export_carried_something(self) -> None:
        """The floor: two empty documents compare equal, and would say nothing at all."""
        self.at("source.json")
        cfg.apply_preset("voyage", actor="test")
        sent = cfg.export_settings()["settings"]
        self.assertGreaterEqual(len(sent), 3, "the export carried %d settings" % len(sent))


class ADefaultExportCarriesEverySettingTest(Case):
    """Exporting WITH defaults writes every declared default into the target as an explicit value.

    That the two sides then AGREE is what is checked here, and it is a weaker claim than it looks:
    a declared default that disagrees with the build is wrong identically on both, so this passes.
    It catches the trip losing or altering a value, not the value being wrong to begin with.
    """

    def test_the_clone_runs_the_same_values(self) -> None:
        self.at("source.json")           # a deployment that has configured nothing
        source = self.effective()
        exported = cfg.export_settings(include_defaults=True)["settings"]

        self.at("target.json")           # a fresh one, handed the export
        cfg.update(dict(exported), actor="test")
        target = self.effective()

        differ = {key: (source[key], target.get(key))
                  for key in source if source[key] != target.get(key)}
        self.assertEqual({}, differ,
                         "these run a different value on the clone: %s" % differ)

    def test_it_compared_every_setting(self) -> None:
        """The floor: an empty comparison passes. Whatever the registry holds must be in it."""
        self.at("source.json")
        self.assertEqual(len(cfg.SETTINGS), len(self.effective()))
        self.assertGreater(len(cfg.SETTINGS), 50)

    def test_the_default_export_carries_more_than_the_sparse_one(self) -> None:
        """`include_defaults` must actually include them, or the risky path is untested.

        Stated as a superset rather than a margin: `_effective` counts an environment variable as
        a set value, and a machine that already exports a hundred of them makes the "sparse"
        export anything but. The property that matters holds on both -- everything the sparse
        export carries is in the full one, and the full one carries more.
        """
        self.at("source.json")
        sparse = cfg.export_settings()["settings"]
        full = cfg.export_settings(include_defaults=True)["settings"]
        self.assertTrue(set(full) >= set(sparse),
                        "the full export dropped %s" % sorted(set(sparse) - set(full)))
        self.assertGreater(len(full), len(sparse))


class ASecretIsNeverExportedTest(Case):
    """A blank would be a WRITE that clears the target's working key, so an import that applied
    everything would break the deployment it was meant to configure."""

    def test_a_stored_secret_is_omitted_and_named(self) -> None:
        secrets = [s for s in cfg.SETTINGS if s.secret]
        if not secrets:
            self.skipTest("no secret settings in this build")
        self.at("source.json")
        cfg.update({secrets[0].key: "a-real-looking-value"}, actor="test")
        exported = cfg.export_settings(include_defaults=True)
        self.assertNotIn(secrets[0].key, exported["settings"])
        self.assertIn(secrets[0].key, exported["secrets_omitted"])

    def test_the_value_appears_nowhere_in_the_document(self) -> None:
        secrets = [s for s in cfg.SETTINGS if s.secret]
        if not secrets:
            self.skipTest("no secret settings in this build")
        self.at("source.json")
        cfg.update({secrets[0].key: "a-real-looking-value"}, actor="test")
        self.assertNotIn("a-real-looking-value",
                         repr(cfg.export_settings(include_defaults=True)))


class TheTestsTouchNoRealConfigTest(Case):
    """The hazard this suite is written around, asserted rather than assumed."""

    def test_the_path_points_inside_the_temporary_directory(self) -> None:
        path = self.at("source.json")
        self.assertEqual(path, cfg.config_path())
        self.assertTrue(cfg.config_path().startswith(self._work.name))

    def test_writing_creates_the_file_there_and_nowhere_else(self) -> None:
        path = self.at("source.json")
        cfg.update({"skills.shared_skill_budget_ratio": "0.30"}, actor="test")
        self.assertTrue(os.path.isfile(path))
        self.assertEqual([os.path.basename(path)],
                         [n for n in os.listdir(self._work.name) if n.endswith(".json")])


class TheSuitePutsTheEnvironmentBackTest(Case):
    """The hazard this suite is written around, asserted rather than assumed.

    A test that leaves `os.environ` changed does not fail -- it changes the answer for every test
    that runs after it, in a different file, and the failure is reported against them.
    """

    def test_an_update_here_does_not_outlive_the_test(self) -> None:
        key = "skills.shared_skill_budget_ratio"
        variable = cfg.SETTINGS_BY_KEY[key].env
        before = os.environ.get(variable)
        self.at("source.json")
        cfg.update({key: "0.42"}, actor="test")
        self.assertEqual("0.42", os.environ.get(variable),
                         "update() is supposed to apply to the process environment")
        # The cleanup this class installs is what puts it back; run it here to prove it does.
        self._restore()
        self.assertEqual(before, os.environ.get(variable))

    def test_the_snapshot_covers_every_variable_that_was_there(self) -> None:
        """The floor: restoring a single name would pass the test above and still leak the other
        hundred a preset writes.

        Counting what the machine happens to export is not that floor. The first version asserted
        `len(self._environ) > 5`, which passes on any developer box and fails under `env -i`,
        where there are three variables -- it measured the environment rather than the snapshot,
        so it was strongest exactly where the environment was richest and the leak least likely
        to matter. Plant the names and the count is of the mechanism.
        """
        planted = {"MATRIXARK_SNAPSHOT_PROBE_%d" % index: str(index) for index in range(6)}
        saved = dict(os.environ)
        try:
            os.environ.update(planted)
            # A second instance, set up while the planted names are present: the snapshot under
            # test is the one `setUp` takes, and this test's own was taken before they existed.
            probe = type(self)(self._testMethodName)
            probe.setUp()
            try:
                missing = sorted(name for name in planted if name not in probe._environ)
                self.assertEqual([], missing,
                                 "the snapshot missed %d of the names that were set" % len(missing))
                self.assertEqual(planted, {name: probe._environ[name] for name in planted})
                # And it is a copy. A snapshot that aliased os.environ would satisfy everything
                # above and restore nothing, because it would change with the thing it records.
                os.environ["MATRIXARK_SNAPSHOT_PROBE_0"] = "changed-after-the-snapshot"
                self.assertEqual("0", probe._environ["MATRIXARK_SNAPSHOT_PROBE_0"])
                # A name that did not exist when the snapshot was taken. Putting the snapshot
                # back is only half of restoring: without clearing first, everything the test
                # ADDED survives, and a preset write adds about a hundred.
                os.environ["MATRIXARK_SNAPSHOT_PROBE_ADDED"] = "added-during-the-test"
            finally:
                probe.doCleanups()
            # doCleanups ran _restore, which is what puts the changed name back.
            self.assertEqual("0", os.environ.get("MATRIXARK_SNAPSHOT_PROBE_0"))
            self.assertIsNone(os.environ.get("MATRIXARK_SNAPSHOT_PROBE_ADDED"),
                              "the snapshot went back but what the test added stayed")
        finally:
            os.environ.clear()
            os.environ.update(saved)


if __name__ == "__main__":
    unittest.main()
