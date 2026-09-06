#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A variable the portal does not offer, winning over a field it does.

`matrixark_mcp_core` reads ``MATRIXARK_ANTHROPIC_TIMEOUT_SEC`` first and falls back to
``MATRIXARK_EXTRACTION_TIMEOUT_SEC``. The portal offers the second and not the first, so on an
Anthropic deployment with the override set the page showed **30** while the extraction path was
using **5**, with no badge, and editing the field changed nothing for that provider.

The setting's own help says the override exists. That is not the same as the screen saying it is
in force right now: help describes what can happen, and the field showed a number as though it
were the one in use.

**The set is derived here, not trusted.** `_UNOFFERED_OVERRIDES` is written in the registry because
the portal needs it per request and deriving it means parsing the tree. This file derives the same
thing from the source and fails in either direction, so the written map cannot drift from what the
code does.

Two shapes are excluded, and each exclusion is asserted rather than assumed:

* a lower-case first argument is a REQUEST argument, not a variable -- `deadline_ms` and
  `audit_sample_rate` override their settings per call, which is the documented per-request
  override and not something a deployment-wide badge should claim;
* a first argument that is ITSELF an offered setting is not unoffered -- `MATRIXARK_SUMMARY_PROVIDER`
  wins over ``MATRIXARK_UNDERSTANDING_PROVIDER`` and the portal offers both, which is the
  documented "blank follows extraction" behaviour.
"""
from __future__ import annotations

import ast
import io
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402


def _string(node) -> str:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else ""


def _precedence_pairs() -> list:
    """(winner, loser, module, line) for every `X.get(A, Y.get(B, ...))` in production code."""
    pairs = []
    for name in sorted(os.listdir(TOOLS)):
        if not name.endswith(".py") or name.startswith("test_"):
            continue
        try:
            with io.open(os.path.join(TOOLS, name), encoding="utf-8") as handle:
                tree = ast.parse(handle.read())
        except (OSError, SyntaxError):
            continue
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and getattr(node.func, "attr", "") == "get"):
                continue
            if len(node.args) < 2:
                continue
            winner, inner = _string(node.args[0]), node.args[1]
            if not winner:
                continue
            if not (isinstance(inner, ast.Call) and getattr(inner.func, "attr", "") == "get"
                    and inner.args):
                continue
            loser = _string(inner.args[0])
            if loser:
                pairs.append((winner, loser, name, node.lineno))
    return pairs


def _derived_overrides() -> set:
    declared = {s.env for s in cfg.SETTINGS if s.env}
    found = set()
    for winner, loser, _module, _line in _precedence_pairs():
        if loser not in declared:
            continue
        if not winner.isupper():
            continue          # a request argument, not a variable
        if winner in declared:
            continue          # offered on its own account
        found.add(winner)
    return found


class TheWrittenMapMatchesTheCodeTest(unittest.TestCase):

    @staticmethod
    def _written() -> set:
        return {entry[0] for entry in cfg._UNOFFERED_OVERRIDES.values()}

    def test_the_deriver_found_something_to_compare(self) -> None:
        # The floor: both assertions below are set equality, and two empty sets are equal.
        self.assertGreater(len(_precedence_pairs()), 3, "the source scan found almost nothing")
        self.assertTrue(_derived_overrides())

    def test_nothing_overrides_a_field_without_being_listed(self) -> None:
        missing = sorted(_derived_overrides() - self._written())
        self.assertEqual([], missing,
                         "%s wins over an offered setting and the portal says nothing" % missing)

    def test_nothing_is_listed_that_does_not_override(self) -> None:
        stale = sorted(self._written() - _derived_overrides())
        self.assertEqual([], stale, "%s no longer overrides anything" % stale)

    def test_each_entry_names_a_real_setting(self) -> None:
        for key, (_env, depends_on, _when) in cfg._UNOFFERED_OVERRIDES.items():
            self.assertIn(key, cfg.SETTINGS_BY_KEY)
            self.assertIn(depends_on, cfg.SETTINGS_BY_KEY)


class TheExclusionsAreRealTest(unittest.TestCase):
    """Each exclusion hides a genuine precedence pair, so each has to be justified rather than
    quietly convenient."""

    def test_a_request_argument_is_excluded_and_exists(self) -> None:
        lower = {w for w, l, _m, _n in _precedence_pairs()
                 if not w.isupper() and l in {s.env for s in cfg.SETTINGS if s.env}}
        self.assertTrue(lower, "no lower-case pair found; the exclusion may be excusing nothing")
        self.assertEqual(set(), lower & _derived_overrides())

    def test_an_offered_winner_is_excluded_and_exists(self) -> None:
        declared = {s.env for s in cfg.SETTINGS if s.env}
        offered = {w for w, l, _m, _n in _precedence_pairs()
                   if w.isupper() and w in declared and l in declared}
        self.assertTrue(offered, "no offered-winner pair found")
        self.assertEqual(set(), offered & _derived_overrides())


class ItFiresOnlyWhenItAppliesTest(unittest.TestCase):

    def setUp(self) -> None:
        self._environ = dict(os.environ)
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        os.environ.clear()
        os.environ.update(self._environ)

    @staticmethod
    def _override(key: str) -> dict:
        return cfg.unoffered_override(key, {})

    def test_it_fires_when_the_variable_is_set_and_the_provider_matches(self) -> None:
        os.environ["MATRIXARK_ANTHROPIC_TIMEOUT_SEC"] = "5"
        os.environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = "anthropic"
        found = self._override("extraction.timeout_sec")
        self.assertIsNotNone(found)
        self.assertEqual("MATRIXARK_ANTHROPIC_TIMEOUT_SEC", found["env"])
        self.assertEqual("5", found["value"])

    def test_it_does_not_fire_on_a_provider_the_override_never_reaches(self) -> None:
        os.environ["MATRIXARK_ANTHROPIC_TIMEOUT_SEC"] = "5"
        os.environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = "openai_compatible"
        self.assertIsNone(self._override("extraction.timeout_sec"),
                          "a variable that overrides nothing here was reported as winning")

    def test_it_does_not_fire_when_the_variable_is_unset(self) -> None:
        os.environ.pop("MATRIXARK_ANTHROPIC_TIMEOUT_SEC", None)
        os.environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = "anthropic"
        self.assertIsNone(self._override("extraction.timeout_sec"))

    def test_an_empty_variable_is_not_an_override(self) -> None:
        os.environ["MATRIXARK_ANTHROPIC_TIMEOUT_SEC"] = "   "
        os.environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = "anthropic"
        self.assertIsNone(self._override("extraction.timeout_sec"))

    def test_an_unrelated_setting_is_never_overridden(self) -> None:
        os.environ["MATRIXARK_ANTHROPIC_TIMEOUT_SEC"] = "5"
        os.environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = "anthropic"
        self.assertIsNone(self._override("retrieval.min_score"))


class TheScreenIsToldTest(unittest.TestCase):
    """Reporting it from a helper nobody calls is the defect this file is about, one level up."""

    def setUp(self) -> None:
        self._environ = dict(os.environ)
        self.addCleanup(self._restore)
        import tempfile
        self._work = tempfile.TemporaryDirectory(prefix="matrixark-override-")
        self.addCleanup(self._work.cleanup)
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = os.path.join(self._work.name, "cfg.json")

    def _restore(self) -> None:
        os.environ.clear()
        os.environ.update(self._environ)

    def _fields(self) -> dict:
        snapshot = cfg.snapshot()
        groups = snapshot["groups"]
        rows = ([f for g in groups for f in g["fields"]] if isinstance(groups, list)
                else [f for fs in groups.values() for f in fs])
        return {f["key"]: f for f in rows}

    def test_the_snapshot_carries_the_override(self) -> None:
        os.environ["MATRIXARK_ANTHROPIC_TIMEOUT_SEC"] = "5"
        os.environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = "anthropic"
        field = self._fields()["extraction.timeout_sec"]
        self.assertIsNotNone(field["overridden_by"],
                             "the page is handed no sign that this field is not deciding")
        self.assertEqual("MATRIXARK_ANTHROPIC_TIMEOUT_SEC", field["overridden_by"]["env"])

    def test_and_the_value_shown_is_the_one_being_overridden(self) -> None:
        """The point of the badge: the number in the box is NOT what the deployment uses."""
        os.environ["MATRIXARK_ANTHROPIC_TIMEOUT_SEC"] = "5"
        os.environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = "anthropic"
        field = self._fields()["extraction.timeout_sec"]
        self.assertNotEqual(field["value"], field["overridden_by"]["value"],
                            "the two agree, so this deployment cannot demonstrate the problem")

    def test_every_field_carries_the_key(self) -> None:
        """Absent would make the page's `if (f.overridden_by)` silently false everywhere."""
        for key, field in self._fields().items():
            self.assertIn("overridden_by", field, key)

    def test_nothing_is_marked_when_no_override_is_set(self) -> None:
        os.environ.pop("MATRIXARK_ANTHROPIC_TIMEOUT_SEC", None)
        os.environ.pop("MATRIXARK_ANTHROPIC_MAX_TOKENS", None)
        marked = [k for k, f in self._fields().items() if f["overridden_by"]]
        self.assertEqual([], marked)


class ThePageDrawsItTest(unittest.TestCase):
    """The shipped `fieldHtml`, run. A badge added to the builder alone renders nowhere, and a
    badge that draws for every field says nothing."""

    def setUp(self) -> None:
        import subprocess
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    @staticmethod
    def _render(overridden) -> str:
        import json
        import subprocess
        script = """
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
        field = {"key": "extraction.timeout_sec", "env": "MATRIXARK_EXTRACTION_TIMEOUT_SEC",
                 "label": "Extraction timeout", "help": "h", "value": "30", "default": "30",
                 "source": "default", "applies": "restart", "essential": False, "secret": False,
                 "configured": False, "overridable_by": [], "boot_pinned": False,
                 "pending_restart": False, "read_by_nothing": False,
                 "overridden_by": overridden}
        page = os.path.join(TOOLS, "portal", "setup_portal.html")
        out = subprocess.run(["node", "-e", script, page, json.dumps(field)],
                             capture_output=True, text=True, timeout=300)
        if out.returncode != 0:
            raise AssertionError(out.stderr)
        return out.stdout

    def test_an_overridden_field_names_the_variable_and_its_value(self) -> None:
        html = self._render({"env": "MATRIXARK_ANTHROPIC_TIMEOUT_SEC", "value": "5",
                             "depends_on": "extraction.provider",
                             "when": "extraction.provider is anthropic"})
        self.assertIn("MATRIXARK_ANTHROPIC_TIMEOUT_SEC", html)
        self.assertIn("5", html)
        self.assertIn("wins", html)
        self.assertIn("extraction.provider is anthropic", html)

    def test_an_ordinary_field_carries_no_such_badge(self) -> None:
        self.assertNotIn("wins</span>", self._render(None))


if __name__ == "__main__":
    unittest.main()
