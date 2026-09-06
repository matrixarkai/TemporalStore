#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A stored setting can be cleared even when it holds the build's own default.

Reset DROPS the stored entry -- the page's own comment says so: "null tells the server to DROP the
stored override, not to store an empty string". So it is worth offering whenever the file holds an
entry.

The page decided instead from `value !== default`, which is a different question. A setting stored
at the value the build already uses got no reset control, and that is the only control on the page
that removes such an entry. The one action that clears a redundant pin was missing from every field
that had one.

It is not hypothetical arithmetic. The deployment this was found on stores **115** settings, of
which **109** hold exactly the build default and **6** change anything. None of the 109 could be
cleared from this page.

They are not harmless. A stored entry stops following the build: if a later release improves that
default, this deployment keeps the old number and nothing on the screen says why. So the field now
says "same as the default" as well as offering the reset.

`stored` comes from the snapshot, which knows whether the file holds the key. The page was
inferring it from the value, and the value cannot answer it.
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

    def setUp(self) -> None:
        self._environ = dict(os.environ)
        self.addCleanup(self._restore)
        self._work = tempfile.TemporaryDirectory(prefix="matrixark-pin-")
        self.addCleanup(self._work.cleanup)
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = os.path.join(self._work.name, "cfg.json")

    def _restore(self) -> None:
        os.environ.clear()
        os.environ.update(self._environ)

    def fields(self) -> dict:
        snapshot = cfg.snapshot()
        groups = snapshot["groups"]
        rows = ([f for g in groups for f in g["fields"]] if isinstance(groups, list)
                else [f for fs in groups.values() for f in fs])
        return {f["key"]: f for f in rows}


class TheSnapshotSaysWhatIsStoredTest(Case):

    KEY = "retrieval.min_score"

    def test_a_setting_nobody_stored_is_not_stored(self) -> None:
        self.assertFalse(self.fields()[self.KEY]["stored"])

    def test_a_setting_stored_at_the_default_is_stored(self) -> None:
        """The case the value cannot answer: identical to the default, and in the file."""
        default = cfg.SETTINGS_BY_KEY[self.KEY].default
        cfg.update({self.KEY: default}, actor="test")
        field = self.fields()[self.KEY]
        self.assertTrue(field["stored"])
        self.assertEqual(field["value"], field["default"],
                         "the value differs, so this test is not covering the case it claims")

    def test_a_setting_stored_at_something_else_is_stored(self) -> None:
        cfg.update({self.KEY: "0.44"}, actor="test")
        self.assertTrue(self.fields()[self.KEY]["stored"])

    def test_clearing_it_makes_it_unstored_again(self) -> None:
        cfg.update({self.KEY: "0.44"}, actor="test")
        cfg.update({self.KEY: None}, actor="test")
        self.assertFalse(self.fields()[self.KEY]["stored"],
                         "reset left the entry in the file")

    def test_every_field_carries_the_key(self) -> None:
        """Absent would make the page's `f.stored` silently false for everything."""
        for key, field in self.fields().items():
            self.assertIn("stored", field, key)


class ThePageOffersTheResetTest(unittest.TestCase):
    """The shipped `fieldHtml`, run. A control hidden by a condition and a control that is not
    there look identical in a diff."""

    SCRIPT = """
const fs = require("fs");
const page = fs.readFileSync(process.argv[1], "utf8");
const start = page.indexOf("function fieldHtml(f) {");
let depth = 0, end = -1;
for (let i = page.indexOf("{", start); i < page.length; i++) {
  if (page[i] === "{") depth++;
  else if (page[i] === "}") { depth--; if (depth === 0) { end = i + 1; break; } }
}
const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const scope = { esc, fieldId: (k) => "f_" + k, byteHint: () => "", controlHtml: () => "<input>" };
const names = Object.keys(scope);
const fieldHtml = new Function(...names,
  page.slice(start, end) + "; return fieldHtml;")(...names.map((k) => scope[k]));
process.stdout.write(fieldHtml(JSON.parse(process.argv[2])));
"""

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def render(self, **extra) -> str:
        field = {"key": "retrieval.min_score", "env": "MATRIXARK_RETRIEVAL_MIN_SCORE",
                 "label": "Min score", "help": "h", "default": "0.20", "applies": "live",
                 "essential": False, "secret": False, "configured": False, "overridable_by": [],
                 "boot_pinned": False, "pending_restart": False, "read_by_nothing": False,
                 "overridden_by": None, "value": "0.20", "source": "default", "stored": False}
        field.update(extra)
        out = subprocess.run(["node", "-e", self.SCRIPT,
                              os.path.join(PORTAL, "setup_portal.html"), json.dumps(field)],
                             capture_output=True, text=True, timeout=300)
        if out.returncode != 0:
            raise AssertionError(out.stderr)
        return out.stdout

    def test_a_setting_stored_at_the_default_can_be_reset(self) -> None:
        html = self.render(stored=True, source="portal")
        self.assertIn("data-reset=", html,
                      "the only control that clears a stored entry is missing from the fields "
                      "that have one")

    def test_and_says_it_is_the_same_as_the_default(self) -> None:
        self.assertIn("same as the default", self.render(stored=True, source="portal"))

    def test_a_setting_nobody_stored_offers_no_reset(self) -> None:
        """The floor. Always offering it would pass the first test and mean nothing."""
        html = self.render(stored=False, source="default")
        self.assertNotIn("data-reset=", html)
        self.assertNotIn("same as the default", html)

    def test_a_stored_setting_that_differs_is_unchanged(self) -> None:
        html = self.render(stored=True, source="portal", value="0.44")
        self.assertIn("data-reset=", html)
        self.assertNotIn("same as the default", html,
                         "a value that differs is not the same as the default")

    def test_an_environment_value_still_offers_reset(self) -> None:
        """It did before this change; a fix that took that away would be a regression."""
        self.assertIn("data-reset=",
                      self.render(stored=False, source="environment", value="0.55"))

    def test_a_secret_still_offers_none(self) -> None:
        self.assertNotIn("data-reset=",
                         self.render(stored=True, source="portal", secret=True, kind="secret"))


if __name__ == "__main__":
    unittest.main()
