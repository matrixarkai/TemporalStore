#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""An import names the secrets it could not bring.

``export_settings`` omits secret values on purpose, and says why: a blank would be a *write* that
clears the target's working key, so an export that "just applied everything" would break the
deployment it was meant to configure. It returns ``secrets_omitted``, and the export button shows
that list.

To the person exporting. The downloaded file does not carry it — the export writes
``JSON.stringify({settings: d.settings})`` and drops the rest — and the import answered *"Applied
47 settings."* and stopped there. So whoever configures the target, who is usually not whoever made
the file, is told the configuration transferred.

It did, apart from the credentials. An absent key looks exactly like a present-but-rejected one:
both fall back to the deterministic path at ingest time with no error the caller ever sees.

The import now names the secrets unset **on this deployment**, rather than echoing whatever the
file declared. That is the question the operator has to act on, and it answers for a hand-written
file too. The snapshot never carries a secret value — it reports ``kind: "secret"`` and a
``configured`` boolean — so that is what is read.

The message depends on a reload finishing and a second message replacing the first, which is
behaviour rather than text: the harness applies a configuration against a deployment with one
secret set and one not, and reads what the page ended up saying.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
PAGE = os.path.join(PORTAL, "setup_portal.html")
HARNESS = os.path.join(PORTAL, "import_secrets_harness.js")


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class AnImportSaysWhatItCouldNotBringTest(unittest.TestCase):

    def _run(self):
        return subprocess.run(["node", HARNESS, PAGE], capture_output=True, text=True, timeout=180)

    def test_the_import_path_runs_clean(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_it_still_reports_what_it_applied(self) -> None:
        """The new sentence must not cost the old one: how many settings landed, and whether any
        of them wait on a restart, is still the first thing an operator needs."""
        out = self._run().stdout
        self.assertIn("ok   it still reports what it applied", out)
        self.assertIn("ok   it says the restart is needed", out)

    def test_it_names_a_secret_this_deployment_lacks(self) -> None:
        self.assertIn("ok   it names the secret this deployment does not have", self._run().stdout)

    def test_it_does_not_name_one_that_is_already_set(self) -> None:
        """Otherwise it is a fixed warning rather than a report, and the reader learns to skip it."""
        self.assertIn("ok   it does not name the one that is set", self._run().stdout)

    def test_it_says_why_the_file_could_not_carry_it(self) -> None:
        """"extraction.api_key is not set" reads like something went wrong. It did not: an export
        never carries secret values, and saying so is the difference between a fault and a step."""
        self.assertIn("ok   it says why the file could not carry it", self._run().stdout)


if __name__ == "__main__":
    unittest.main()
