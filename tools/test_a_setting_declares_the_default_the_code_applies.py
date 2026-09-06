# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A setting declares the default the code actually applies, and says when there are two.

The portal's default is not decoration. `export_settings(include_defaults=True)` writes it to the
target as an EXPLICIT value, so a number here that the build does not use reconfigures a clone --
which is how a set of frozen budgets once came out of a clone up to 156x larger than the source.

So: for every setting whose variable is read somewhere with a default this can read off the source,
the declared default must be that one.

AND WHERE TWO READ SITES DISAGREE, the help must say so. `MATRIXARK_RETRIEVAL_TIMEOUT_MS` is read
twice: `matrixark_mcp_retrieve_planning` treats unset as 0, no deadline, which is what the page
describes -- and `matrixark_mcp_server` treats unset as 30000ms and uses it as the ABORT CEILING
for the matrixark_retrieve tool. Below that ceiling the server discards a ContextPack it had
already computed and returns an empty deadline_fallback_pack; the comment at that read site says so
in as many words. An operator lowering the deadline on this page was also lowering that ceiling,
and nothing on the page mentioned it.

The rule for that case is deliberately concrete rather than "the help should explain": the help
must contain the OTHER default as text. A prose requirement cannot be checked; a number can.
"""
from __future__ import annotations

import ast
import importlib
import os
import subprocess
import sys
import unittest
from typing import Dict, Optional, Set, Tuple

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tools"))

TRUE = {"1", "true", "yes", "on"}
FALSE = {"0", "false", "no", "off"}
READERS = {"env_bool", "env_int", "env_float", "get", "getenv"}

#: 160 when this was written. A floor, so a parser that stops recognising reads fails here rather
#: than passing with nothing to compare.
EXPECTED_CHECKABLE_FLOOR = 120


def _tracked() -> list:
    return subprocess.run(["git", "ls-files", "tools/*.py"], cwd=REPO,
                          capture_output=True, text=True).stdout.split()


def _default_at(node, numeric: Dict[int, str]) -> Optional[Tuple[str, str]]:
    fn = node.func
    name = fn.id if isinstance(fn, ast.Name) else (fn.attr if isinstance(fn, ast.Attribute)
                                                   else None)
    if name not in READERS or not node.args:
        return None
    raw = node.args[1].value if len(node.args) > 1 and isinstance(node.args[1], ast.Constant) \
        else None
    if name == "env_bool" and isinstance(raw, bool):
        return "bool", "1" if raw else "0"
    if name == "env_int" and isinstance(raw, int) and not isinstance(raw, bool):
        return "int", str(raw)
    if name == "env_float" and isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return "float", str(raw)
    if isinstance(raw, str):
        wrapped = numeric.get(id(node))
        try:
            if wrapped == "int":
                return "int", str(int(float(raw)))
            if wrapped == "float":
                return "float", str(float(raw))
        except ValueError:
            return None
        low = raw.strip().lower()
        if low in TRUE or low in FALSE:
            return "bool", "1" if low in TRUE else "0"
    if isinstance(raw, int) and not isinstance(raw, bool) and name in {"get", "getenv"}:
        return "int", str(raw)
    return None


def defaults_in_the_source() -> Dict[str, Set[Tuple[str, str]]]:
    """env var -> every (kind, default) a production read site declares for it."""
    found: Dict[str, Set[Tuple[str, str]]] = {}
    for rel in _tracked():
        if os.path.basename(rel).startswith("test_"):
            continue
        try:
            with open(os.path.join(REPO, rel), encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except (OSError, SyntaxError):
            continue
        numeric: Dict[int, str] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                    and node.func.id in {"int", "float"}:
                for arg in node.args:
                    for inner in ast.walk(arg):
                        if isinstance(inner, ast.Call):
                            numeric[id(inner)] = node.func.id
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            first = node.args[0]
            if not (isinstance(first, ast.Constant) and isinstance(first.value, str)
                    and first.value.startswith(("MATRIXARK_", "TS_"))):
                continue
            pair = _default_at(node, numeric)
            if pair is not None:
                found.setdefault(first.value, set()).add(pair)
    return found


def _same(declared: str, kind: str, actual: str) -> bool:
    if kind in ("int", "float"):
        try:
            return float(declared) == float(actual)
        except (TypeError, ValueError):
            return False
    return str(declared).strip().lower() in (TRUE if actual == "1" else FALSE)


class ASettingDeclaresTheDefaultTheCodeAppliesTest(unittest.TestCase):

    def setUp(self) -> None:
        self.config = importlib.import_module("matrixark_gateway_config")
        self.source = defaults_in_the_source()

    def _checkable(self):
        for setting in self.config.SETTINGS:
            if not setting.env:
                continue
            pairs = self.source.get(setting.env)
            if not pairs:
                continue
            yield setting, pairs

    def test_there_is_enough_to_compare(self) -> None:
        """A comparison over nothing passes hardest, so assert the extent first."""
        count = sum(1 for _ in self._checkable())
        self.assertGreaterEqual(
            count, EXPECTED_CHECKABLE_FLOOR,
            "only %d settings have a default this can read from the source, below the floor of "
            "%d -- the reader stopped recognising read sites, so the rules below compare almost "
            "nothing" % (count, EXPECTED_CHECKABLE_FLOOR))

    def test_a_single_default_is_the_one_declared(self) -> None:
        wrong = []
        for setting, pairs in self._checkable():
            if len(pairs) != 1:
                continue
            kind, actual = next(iter(pairs))
            if not _same(setting.default, kind, actual):
                wrong.append("%s declares %r, %s applies %r"
                             % (setting.key, setting.default, setting.env, actual))
        self.assertEqual(
            [], wrong,
            "a setting declares a default the code does not apply. Cloning a deployment writes "
            "this value explicitly, so the clone gets the number on the page rather than the one "
            "the build uses:\n  " + "\n  ".join(wrong))

    def test_two_disagreeing_defaults_are_named_in_the_help(self) -> None:
        silent = []
        for setting, pairs in self._checkable():
            values = {actual for _kind, actual in pairs}
            if len(values) < 2:
                continue
            missing = [v for v in values
                       if not _same(setting.default, "int", v) and v not in (setting.help or "")]
            if missing:
                silent.append("%s reads %s with defaults %s; the help mentions %r and not %s"
                              % (setting.key, setting.env, sorted(values), setting.default,
                                 missing))
        self.assertEqual(
            [], silent,
            "a variable is read with two different defaults and the page names only one. An "
            "operator setting the value on this page changes both, and the page says so for "
            "neither:\n  " + "\n  ".join(silent))

    def test_the_reader_finds_a_known_disagreement(self) -> None:
        """Mechanism control: the two-default case must be one this actually detects."""
        pairs = self.source.get("MATRIXARK_RETRIEVAL_TIMEOUT_MS") or set()
        values = {actual for _kind, actual in pairs}
        self.assertIn(
            "30000", values,
            "the reader no longer sees matrixark_mcp_server reading "
            "MATRIXARK_RETRIEVAL_TIMEOUT_MS with a default of 30000, so the rule above has "
            "nothing to catch and would pass on a page that hid it")
        self.assertGreaterEqual(
            len(values), 2,
            "MATRIXARK_RETRIEVAL_TIMEOUT_MS now has one default everywhere. If the two read "
            "sites were reconciled, say so here and drop this control")


if __name__ == "__main__":
    unittest.main()
