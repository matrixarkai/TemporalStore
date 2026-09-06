#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The configuration log records what changed, and only what changed.

Its own comment says why it exists: *"Who changed the embedding model, and when" is a real support
question, and until now the file recorded only the latest state -- which answers neither.*

Every write appended a full entry regardless of whether anything moved. A caller re-POSTing the
values it already holds -- a periodic reconciler, a form saved without an edit -- produced an entry
whose ``from`` equalled its ``to``, and the log is capped, so those entries evicted the ones that
answered the question.

Measured on the deployment this was found on:

===========================================  =====
history entries retained                        50
entries where nothing changed at all            45
individual changes recorded                    269
...with ``from`` equal to ``to``               254
...that changed something                       15
window still covered                     10.9 hours
===========================================  =====

**"Changed" means the FILE changed, not the effective value.** Storing a value identical to the
build default moves nothing an operator can observe today and is still a real change: it pins the
setting, so a later build that improves that default will not reach this deployment. That write is
recorded. Comparing effective values instead would have dropped exactly the entry explaining how a
deployment came to be pinned.

**A secret is exempt.** Its value is never compared, so re-setting one cannot be told from rotating
it, and the fact of the write is the useful half.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402

KEY = "retrieval.min_score"


class Case(unittest.TestCase):

    def setUp(self) -> None:
        self._environ = dict(os.environ)
        self.addCleanup(self._restore)
        self._work = tempfile.TemporaryDirectory(prefix="matrixark-log-")
        self.addCleanup(self._work.cleanup)
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = os.path.join(self._work.name, "cfg.json")

    def _restore(self) -> None:
        os.environ.clear()
        os.environ.update(self._environ)

    def entries(self) -> list:
        return cfg.history(limit=50)


class ARepeatedWriteIsNotAChangeTest(Case):

    def test_the_first_write_is_recorded(self) -> None:
        cfg.update({KEY: "0.44"}, actor="test")
        self.assertEqual(1, len(self.entries()))

    def test_writing_the_same_value_again_is_not(self) -> None:
        cfg.update({KEY: "0.44"}, actor="test")
        for _ in range(5):
            cfg.update({KEY: "0.44"}, actor="test")
        self.assertEqual(1, len(self.entries()),
                         "a caller re-sending what it already stored fills the log")

    def test_a_different_value_is_recorded_again(self) -> None:
        """The floor. A log that recorded nothing would pass the test above."""
        cfg.update({KEY: "0.44"}, actor="test")
        cfg.update({KEY: "0.55"}, actor="test")
        self.assertEqual(2, len(self.entries()))

    def test_one_no_op_among_real_changes_is_dropped_alone(self) -> None:
        """The entry survives; the change that changed nothing does not appear in it."""
        # Taken from the registry rather than written here: a name invented for a test is a name
        # that can stop existing, and update() refuses an unknown key outright.
        other = next(s.key for s in cfg.SETTINGS
                     if s.kind == "int" and not s.secret and s.key != KEY)
        cfg.update({KEY: "0.44", other: "1000"}, actor="test")
        cfg.update({KEY: "0.44", other: "2000"}, actor="test")
        latest = self.entries()[0]
        self.assertEqual([other], [c["key"] for c in latest["changes"]],
                         "the unchanged setting was recorded beside the changed one")

    def test_nothing_at_all_appends_no_entry(self) -> None:
        cfg.update({KEY: "0.44"}, actor="test")
        before = len(self.entries())
        cfg.update({KEY: "0.44"}, actor="test")
        self.assertEqual(before, len(self.entries()))


class TheFileIsWhatCountsAsChangedTest(Case):
    """Not the effective value: those differ exactly where it matters."""

    def test_storing_the_build_default_is_recorded(self) -> None:
        default = cfg.SETTINGS_BY_KEY[KEY].default
        cfg.update({KEY: default}, actor="test")
        self.assertEqual(1, len(self.entries()),
                         "pinning a setting to the default left no trace, and a pin is what "
                         "stops a later build reaching this deployment")

    def test_and_the_effective_value_did_not_move(self) -> None:
        """The premise of the test above. If the effective value changed too, it would not be
        showing that the file is the thing being compared."""
        default = cfg.SETTINGS_BY_KEY[KEY].default
        before, _source = cfg._effective(cfg.SETTINGS_BY_KEY[KEY], {})
        cfg.update({KEY: default}, actor="test")
        after, _source = cfg._effective(cfg.SETTINGS_BY_KEY[KEY], {KEY: default})
        self.assertEqual(before, after)

    def test_storing_it_twice_records_once(self) -> None:
        default = cfg.SETTINGS_BY_KEY[KEY].default
        cfg.update({KEY: default}, actor="test")
        cfg.update({KEY: default}, actor="test")
        self.assertEqual(1, len(self.entries()))

    def test_a_reset_that_removes_something_is_recorded(self) -> None:
        cfg.update({KEY: "0.44"}, actor="test")
        cfg.update({KEY: None}, actor="test")
        self.assertEqual(2, len(self.entries()))

    def test_a_reset_of_something_unstored_is_not(self) -> None:
        cfg.update({KEY: None}, actor="test")
        self.assertEqual([], self.entries())


class ASecretIsAlwaysRecordedTest(Case):

    def _secret(self) -> str:
        secrets = [s.key for s in cfg.SETTINGS if s.secret]
        if not secrets:
            self.skipTest("no secret settings in this build")
        return secrets[0]

    def test_setting_the_same_secret_twice_records_twice(self) -> None:
        key = self._secret()
        cfg.update({key: "sk-same"}, actor="test")
        cfg.update({key: "sk-same"}, actor="test")
        self.assertEqual(2, len(self.entries()),
                         "a secret's value is never compared, so a rotation cannot be told from "
                         "a repeat and the write itself is the record")

    def test_and_the_value_is_still_never_written_down(self) -> None:
        import json

        key = self._secret()
        cfg.update({key: "sk-do-not-log-me"}, actor="test")
        self.assertNotIn("sk-do-not-log-me", json.dumps(self.entries()))


if __name__ == "__main__":
    unittest.main()
