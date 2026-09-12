#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A percentile call passes the unit its implementation takes.

There are thirteen `percentile` / `_percentile` implementations under `tools/`, four different
index formulas between them, and **two incompatible units**: five take a ratio in 0..1 and seven
take a number in 0..100. Nothing named which was which, and nothing checked that a caller matched.

The failure is silent and total. Hand 95 to an implementation that wants 0.95 and the index clamps
to the end of the list, so `p50`, `p95` and `p99` all return the MAXIMUM -- three identical numbers
reported under three different names, on the surfaces used to decide whether a change made things
faster. Hand 0.95 to one that wants 95 and you get the second-smallest sample.

**Every pairing in the tree is correct today**, including the four modules that import the function
rather than defining it. That is the condition this file exists for, not an argument against it:
`test_there_is_one_cosine` records the same situation -- *"vectors stored today are unit length, so
the two answered the same and nothing had gone wrong. That is what let one copy be fixed while the
other was not."*

## Why the unit is measured by RUNNING each one

The first version of this read the source for `/ 100` and got `redis_scale_load` wrong: it takes
0..100 and says so through `statistics.quantiles(values, n=100, method="inclusive")[pct - 1]`,
where the 100 is an argument rather than a divisor. A classifier that reads the arithmetic will
keep meeting spellings it does not know. Calling each implementation on `range(1, 101)` and asking
which argument returns something near 95 cannot be fooled that way, and needs no list of shapes.
"""
from __future__ import annotations

import ast
import io
import math
import os
import re
import statistics
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)

#: A sorted run where the answer is unmistakable: nearest-rank p95 is 95, linear p95 is 95.05, and
#: the maximum -- what a unit mismatch returns -- is 100.
PROBE = list(range(1, 101))
NEAR_P95 = (93.0, 97.0)

#: Names that hold a percentile. Both spellings occur.
NAMES = ("percentile", "_percentile")

#: Floors, set from what they are FOR. A scan that stopped finding implementations, or stopped
#: resolving call sites, would report a clean tree; neither number tracks how many there happen to
#: be today.
EXPECTED_IMPLEMENTATION_FLOOR = 6
EXPECTED_CALL_FLOOR = 10

#: Implementations that cannot be exercised in isolation, with the reason. Asserted exactly, so one
#: that becomes callable fails here rather than sitting on a list forever.
UNCALLABLE = {
    "matrixark_mcp_rust_proxy_client": "delegates to the module-level percentile in "
                                       "matrixark_mcp_rust_proxy_metrics_record, which is checked "
                                       "on its own",
}

#: Modules whose percentile call this cannot resolve statically, because the method arrives from a
#: mixin. Asserted exactly: a new one fails rather than being skipped in silence.
UNRESOLVED_CALLERS = ("matrixark_mcp_rust_proxy_metrics_snapshot",)

#: Where that mixin's percentile ends up, checked by calling it like any other.
UNRESOLVED_SUPPLIER = "matrixark_mcp_rust_proxy_metrics_record"

_CALL = re.compile(r"\b(_?percentile)\s*\(\s*[^,()]+,\s*([0-9]+(?:\.[0-9]+)?)\s*\)")


def _tracked():
    return subprocess.run(["git", "ls-files", "tools/*.py"], cwd=REPO,
                          capture_output=True, text=True).stdout.split()


def _text(rel):
    with io.open(os.path.join(REPO, rel), encoding="utf-8", errors="replace") as handle:
        return handle.read()


def _unit_of(source, name):
    """'ratio' if it wants 0.95, 'percent' if it wants 95, None if it cannot be told."""
    namespace = {"math": math, "statistics": statistics, "sorted": sorted, "min": min,
                 "max": max, "int": int, "float": float, "round": round, "len": len, "list": list}
    try:
        exec(compile(source, "<percentile>", "exec"), namespace)  # noqa: S102
        function = namespace[name]
    except Exception:
        return None
    answers = {}
    for label, argument in (("percent", 95), ("ratio", 0.95)):
        try:
            value = function(list(PROBE), argument)
        except Exception:
            continue
        if isinstance(value, (int, float)):
            answers[label] = NEAR_P95[0] <= float(value) <= NEAR_P95[1]
    if answers.get("percent") and not answers.get("ratio"):
        return "percent"
    if answers.get("ratio") and not answers.get("percent"):
        return "ratio"
    return None


def implementations():
    """{module stem: unit} for every percentile whose unit can be established by calling it."""
    found, uncallable = {}, set()
    for rel in _tracked():
        stem = os.path.basename(rel)[:-3]
        if stem.startswith("test_"):
            continue
        body = _text(rel)
        try:
            tree = ast.parse(body)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if node.name not in NAMES:
                continue
            source = ast.unparse(node)
            if source.lstrip().startswith("@"):          # a staticmethod decorator, dropped
                source = "\n".join(source.splitlines()[1:])
            unit = _unit_of(source, node.name)
            if unit is None:
                uncallable.add(stem)
            else:
                found.setdefault(stem, unit)
    return found, uncallable


def _resolved_source(stem, body):
    """Which module's percentile a file uses: its own, or the one it imports."""
    try:
        tree = ast.parse(body)
    except SyntaxError:
        return None
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in NAMES:
            return stem
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module \
                and any(alias.name in NAMES for alias in node.names):
            return node.module.rsplit(".", 1)[-1]
    return None


def call_sites():
    """([(stem, line, literal, source stem)], [(stem, line, literal)]) -- resolved and not.

    Returning the second list is the point. A call whose implementation cannot be found is a call
    this file does not check, and dropping those quietly is how a sweep reports a clean tree while
    covering less of it than anyone thinks.
    """
    resolved, unresolved = [], []
    for rel in _tracked():
        stem = os.path.basename(rel)[:-3]
        if stem.startswith("test_"):
            continue
        body = _text(rel)
        source = _resolved_source(stem, body)
        for number, line in enumerate(body.splitlines(), 1):
            match = _CALL.search(line)
            if not match:
                continue
            if line.lstrip().startswith("def ") or "def %s(" % match.group(1) in line:
                continue
            literal = float(match.group(2))
            if source is None:
                unresolved.append((stem, number, literal))
            else:
                resolved.append((stem, number, literal, source))
    return resolved, unresolved


class APercentileCallMatchesItsImplementationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.units, cls.uncallable = implementations()
        cls.calls, cls.unresolved = call_sites()

    def test_the_scan_finds_the_implementations(self) -> None:
        self.assertGreaterEqual(
            len(self.units), EXPECTED_IMPLEMENTATION_FLOOR,
            "only %d percentile implementations could be exercised, so the check below is about "
            "almost nothing" % len(self.units))

    def test_the_scan_finds_the_calls(self) -> None:
        self.assertGreaterEqual(
            len(self.calls), EXPECTED_CALL_FLOOR,
            "only %d percentile calls with a literal were found; the call pattern has probably "
            "changed and this file now checks nothing" % len(self.calls))

    def test_both_units_are_still_present(self) -> None:
        """The reason this file exists. If one unit disappeared the tree got safer, and saying so
        beats leaving a check nobody can fail."""
        seen = set(self.units.values())
        self.assertEqual(
            {"ratio", "percent"}, seen,
            "the tree used to hold both a 0..1 and a 0..100 percentile, and now holds %r. If they "
            "have been consolidated, this file has done its job and can go." % (sorted(seen),))

    def test_only_the_recorded_one_cannot_be_exercised(self) -> None:
        self.assertEqual(
            sorted(UNCALLABLE), sorted(self.uncallable),
            "an implementation stopped being callable in isolation, or started. One that cannot "
            "be called is one whose unit nothing here can check.")

    def test_every_call_passes_the_unit_its_implementation_takes(self) -> None:
        wrong = []
        for stem, line, literal, source in self.calls:
            unit = self.units.get(source)
            if unit is None:
                continue
            if unit == "ratio" and literal > 1.0:
                wrong.append("%s:%d passes %g to the 0..1 percentile in %s -- the index clamps to "
                             "the end, so this reports the MAXIMUM" % (stem, line, literal, source))
            elif unit == "percent" and literal <= 1.0:
                wrong.append("%s:%d passes %g to the 0..100 percentile in %s -- that is the "
                             "bottom of the list, not a percentile" % (stem, line, literal, source))
        self.assertEqual([], wrong, "\n  ".join([""] + wrong))

    def test_the_calls_this_cannot_resolve_are_named(self) -> None:
        """A call whose implementation this cannot find is a call it does not check.

        Four of the fifty-three are `self._percentile(...)` in
        `matrixark_mcp_rust_proxy_metrics_snapshot`, which defines no percentile and imports none:
        the method arrives from the mixin in `matrixark_mcp_rust_proxy_client`, which delegates in
        turn to the module-level one in `matrixark_mcp_rust_proxy_metrics_record`. Following a
        mixin statically is guesswork; naming the four is not, and the check below still holds them
        to the unit that supplier takes.
        """
        self.assertEqual(
            sorted(UNRESOLVED_CALLERS), sorted({stem for stem, _l, _v in self.unresolved}),
            "a percentile call appeared whose implementation this file cannot resolve, or one "
            "stopped being unresolvable. Either way the coverage moved without saying so.")

    def test_the_mixin_callers_pass_a_ratio(self) -> None:
        """Those four reach `matrixark_mcp_rust_proxy_metrics_record.percentile`, which takes 0..1
        -- established by calling it, like every other implementation here."""
        self.assertEqual(
            "ratio", self.units.get(UNRESOLVED_SUPPLIER),
            "%s no longer takes a ratio, so the calls below are checked against the wrong unit"
            % UNRESOLVED_SUPPLIER)
        wrong = ["%s:%d passes %g to a 0..1 percentile -- that reports the MAXIMUM"
                 % (stem, line, literal)
                 for stem, line, literal in self.unresolved if literal > 1.0]
        self.assertEqual([], wrong, chr(10).join([""] + wrong))



if __name__ == "__main__":
    unittest.main()
