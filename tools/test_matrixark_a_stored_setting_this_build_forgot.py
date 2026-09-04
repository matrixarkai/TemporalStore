#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A stored value this build has no setting for is named.

The configuration file outlives the process that wrote it -- that is its purpose -- and it outlives
the build too. When a setting is renamed or dropped, the value stays in the file and stops meaning
anything, and until now nothing said so:

    the stale key is mentioned anywhere in the snapshot: False
    apply_boot seeded:                                   ['MATRIXARK_EXTRACTION_MODEL']
    export carries it:                                   False
    writing it is refused: unknown setting(s): extraction.retired_knob

So an operator who set it, upgraded, and came back to the page saw a deployment configured the way
they remembered, running the default for that behaviour instead. The single place it surfaced was a
write attempt, which nobody makes for a setting they have already set.

Reported, never removed. Deleting a customer's stored values because this build does not recognise
them is the wrong answer to a downgrade, a partial rollout, or a key this build reads under another
name -- so the page says what they are and where they live, and leaves them alone.
"""
from __future__ import annotations

import io
import json
import os
import shutil
import subprocess
import tempfile
import unittest

import matrixark_gateway_config as cfg

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "stale_settings_harness.js")
PAGE = os.path.join(PORTAL, "setup_portal.html")


class TheSnapshotNamesThemTest(unittest.TestCase):

    def setUp(self) -> None:
        self._saved = dict(os.environ)
        self.addCleanup(lambda: (os.environ.clear(), os.environ.update(self._saved)))
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = os.path.join(tmp.name, "runtime.json")
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = self.path

    def _store(self, values) -> None:
        with io.open(self.path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps({"values": values, "updated_at": 1}))

    def test_a_key_this_build_does_not_have_is_reported(self) -> None:
        self._store({"extraction.model": "m", "extraction.retired_knob": "set-long-ago"})
        self.assertEqual(["extraction.retired_knob"], cfg.snapshot()["unknown_stored"])

    def test_a_file_of_known_keys_reports_nothing(self) -> None:
        """Otherwise it is a permanent notice, and a permanent notice is one people stop reading."""
        self._store({"extraction.model": "m"})
        self.assertEqual([], cfg.snapshot()["unknown_stored"])

    def test_the_known_key_is_still_a_setting(self) -> None:
        """If extraction.model ever stopped being one, the check above would pass by accident."""
        self.assertIn("extraction.model", cfg.SETTINGS_BY_KEY)

    def test_writing_one_is_still_refused(self) -> None:
        """Naming them is not permitting them: an unknown key is still not writable."""
        self._store({"extraction.retired_knob": "set-long-ago"})
        with self.assertRaises(cfg.UnknownSetting):
            cfg.update({"extraction.retired_knob": "x"})

    def test_they_are_left_in_the_file(self) -> None:
        """A build that does not recognise a value has not established that it is rubbish."""
        self._store({"extraction.model": "m", "extraction.retired_knob": "set-long-ago"})
        cfg.update({"extraction.model": "changed"})
        with io.open(self.path, encoding="utf-8") as handle:
            self.assertIn("extraction.retired_knob", json.load(handle)["values"])


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class ThePageSaysSoTest(unittest.TestCase):

    def _run(self, *extra):
        return subprocess.run(["node", HARNESS, PAGE] + list(extra),
                              capture_output=True, text=True, timeout=180)

    def test_the_page_renders_them(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_it_says_they_do_nothing_and_where_they_are(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   it says they do nothing", out)
        self.assertIn("ok   it says where they are", out)

    def test_it_is_silent_when_there_is_nothing_to_say(self) -> None:
        self.assertIn("ok   nothing is said when nothing is stale", self._run("--none").stdout)

    def test_the_existing_warnings_survive_it(self) -> None:
        """It shares the warnings area, so an addition could have replaced what was there."""
        self.assertIn("ok   the model warning still renders", self._run().stdout)


if __name__ == "__main__":
    unittest.main()
